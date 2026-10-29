//! The node loop. It watches the `workloads` and `leases` caches and writes
//! this node to `nodes`. For every workload whose state is `running` it waits
//! until the lease looks free and the placement policy lets it, then tries to
//! claim it. It runs a workload only while it holds the lease.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use bytes::Bytes;
use felix_client::{CacheChange, CacheCondition, CacheWatchItem, ClusterCacheWatch};
use felixd_core::lease::{LeaseTiming, MIN_TTL};
use felixd_core::node::NodeInfo;
use felixd_core::placement::{FirstClaimWins, Placement};
use felixd_core::spec::{WorkloadSpec, WorkloadState};
use felixd_core::{names, output, snapshot};
use felixd_runtime::Runtime;
use tokio::sync::futures::Notified;
use tokio::sync::{Notify, oneshot, watch};
use tokio::task::JoinHandle;

use crate::faults;
use crate::felix::Felix;
use crate::lease::{self, Fence, Held};
use crate::runner::{Modules, Outcome, run_workload};
use crate::settings::Settings;

const RETRY_DELAY: Duration = Duration::from_secs(1);

/// How long shutdown waits for workloads to release their leases.
const RELEASE_WITHIN: Duration = Duration::from_secs(2);

/// Who this node is and how it holds and claims leases.
#[derive(Clone, Debug)]
pub struct AgentConfig {
    /// This node's name in the leases it writes and in `nodes`. Each running
    /// agent needs its own.
    pub node_id: String,
    pub lease: LeaseTiming,
    pub placement: FirstClaimWins,
}

struct Node {
    felix: Felix,
    modules: Modules,
    node_id: String,
    timing: LeaseTiming,
    max_workloads: usize,
    placement: Box<dyn Placement>,
    /// Workloads this node holds or is claiming.
    held: AtomicUsize,
    /// Notified when a workload ends here, so a full node looks again.
    room: Notify,
    random: AtomicU64,
}

/// What the leases watch last said about one workload's lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Seen {
    Unknown,
    /// Deleted.
    Free,
    /// Written, expiring at this broker time (0 is never).
    Held {
        expires_at_millis: u64,
    },
}

/// A workload this agent is trying to claim or running.
struct Task {
    stop: watch::Sender<bool>,
    handle: JoinHandle<()>,
    spec: WorkloadSpec,
}

/// Runs until `shutdown` completes, then releases the leases it holds.
pub async fn run(
    settings: &Settings,
    config: AgentConfig,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    anyhow::ensure!(
        config.lease.ttl >= MIN_TTL,
        "the lease TTL must be at least {} ms",
        MIN_TTL.as_millis()
    );
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
        ^ (u64::from(std::process::id()) << 32);
    let node = Arc::new(Node {
        felix: Felix::connect(settings).await?,
        modules: Modules::new(Runtime::new().map_err(|e| anyhow::anyhow!("{e}"))?),
        node_id: config.node_id,
        timing: config.lease,
        max_workloads: config.placement.max_workloads,
        placement: Box::new(config.placement),
        held: AtomicUsize::new(0),
        room: Notify::new(),
        random: AtomicU64::new(seed | 1),
    });
    tracing::info!(node = %node.node_id, "agent started");
    let presence = tokio::spawn(announce(node.clone()));
    let mut tasks: HashMap<String, Task> = HashMap::new();
    let mut seen: HashMap<String, watch::Sender<Seen>> = HashMap::new();
    tokio::pin!(shutdown);
    // The caches may not exist yet on a new cluster; `felixd run` creates them.
    let opened = async {
        let leases = reopen(&node, names::LEASES, Duration::ZERO).await;
        (
            leases,
            reopen(&node, names::WORKLOADS, Duration::ZERO).await,
        )
    };
    let Some((mut leases, mut watch)) = unless(shutdown.as_mut(), opened).await else {
        return shut_down(&node, presence, tasks).await;
    };
    // Set while a new watch is redelivering the current workloads.
    let mut redelivery: Option<Redelivery> = None;
    loop {
        let item = tokio::select! {
            // A lease change waiting to be read comes before a workload, so a
            // new task starts from the lease as it is.
            biased;
            () = &mut shutdown => return shut_down(&node, presence, tasks).await,
            item = leases.recv() => {
                match item {
                    Some(CacheWatchItem::Change(change)) => {
                        if change.value.is_some() || !faults::ignore_lease_delete() {
                            note_lease(&mut seen, change);
                        }
                    }
                    // A new watch redelivers every live lease.
                    _ => match unless(shutdown.as_mut(), reopen(&node, names::LEASES, RETRY_DELAY)).await {
                        Some(opened) => leases = opened,
                        None => return shut_down(&node, presence, tasks).await,
                    },
                }
                continue;
            }
            item = watch.recv() => item,
        };
        let reopen_after = match item {
            Some(CacheWatchItem::Change(change)) => {
                match faults::drop_watch(&change.key, change.value.is_some()) {
                    Some(gap) => gap,
                    None => {
                        let (id, offset) = (change.key.clone(), change.offset);
                        apply(&node, &mut tasks, &mut seen, change.key, change.value);
                        if let Some(r) = redelivery.as_mut()
                            && r.note(&id, offset)
                        {
                            prune(&mut tasks, &r.seen);
                            redelivery = None;
                        }
                        continue;
                    }
                }
            }
            // The watch ended; open a new one, which delivers every current
            // workload again.
            Some(CacheWatchItem::Lagged { .. } | CacheWatchItem::ShardMoved(_)) | None => {
                RETRY_DELAY
            }
        };
        drop(watch);
        match unless(
            shutdown.as_mut(),
            reopen(&node, names::WORKLOADS, reopen_after),
        )
        .await
        {
            Some(opened) => watch = opened,
            None => return shut_down(&node, presence, tasks).await,
        }
        redelivery = match watch.retained_count() {
            Some(count) => Some(Redelivery::new(count, watch.resume_offset())),
            None => {
                tracing::warn!("the new watch did not say how many workloads it redelivers");
                None
            }
        };
        if let Some(r) = redelivery.as_ref().filter(|r| r.done()) {
            prune(&mut tasks, &r.seen);
            redelivery = None;
        }
    }
}

/// Runs `work` unless `shutdown` completes first.
async fn unless<T>(
    shutdown: Pin<&mut impl Future<Output = ()>>,
    work: impl Future<Output = T>,
) -> Option<T> {
    tokio::select! {
        biased;
        () = shutdown => None,
        done = work => Some(done),
    }
}

async fn shut_down(
    node: &Node,
    presence: JoinHandle<()>,
    tasks: HashMap<String, Task>,
) -> Result<()> {
    presence.abort();
    stop_all(tasks).await;
    // Bounded: on a new cluster `nodes` may not exist, and the delete would
    // wait for it.
    let delete = node.felix.cache_delete(names::NODES, &node.node_id);
    let _ = tokio::time::timeout(RELEASE_WITHIN, delete).await;
    Ok(())
}

async fn reopen(node: &Node, cache: &str, delay: Duration) -> ClusterCacheWatch {
    tokio::time::sleep(delay).await;
    loop {
        match node.felix.watch_retained(cache).await {
            Ok(watch) => return watch,
            Err(error) => tracing::warn!(cache, %error, "could not watch, retrying"),
        }
        tokio::time::sleep(RETRY_DELAY).await;
    }
}

fn note_lease(seen: &mut HashMap<String, watch::Sender<Seen>>, change: CacheChange) {
    let now = match change.value {
        Some(_) => Seen::Held {
            expires_at_millis: change.expires_at_millis,
        },
        None => Seen::Free,
    };
    seen_for(seen, &change.key).send_replace(now);
}

fn seen_for<'a>(
    seen: &'a mut HashMap<String, watch::Sender<Seen>>,
    id: &str,
) -> &'a watch::Sender<Seen> {
    seen.entry(id.to_owned())
        .or_insert_with(|| watch::channel(Seen::Unknown).0)
}

/// Writes this node to `nodes` every TTL/3 with a TTL, so the entry goes
/// away soon after the node does.
async fn announce(node: Arc<Node>) {
    loop {
        let info = NodeInfo {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            max_workloads: node.max_workloads,
            held: node.held.load(Ordering::Relaxed),
        };
        let put = node
            .felix
            .cache_put_ttl(
                names::NODES,
                &node.node_id,
                Bytes::from(info.encode()),
                node.timing.ttl_millis(),
            )
            .await;
        if let Err(error) = put {
            tracing::warn!(%error, "could not write this node to nodes");
        }
        tokio::time::sleep(node.timing.renew_after()).await;
    }
}

/// What a new watch has redelivered. Its retained values come before any
/// live change and it says how many there are, so once they are all in, a
/// workload with a task that is not among them was deleted or stopped while
/// no watch was open.
#[derive(Debug)]
pub(crate) struct Redelivery {
    remaining: u64,
    live_from: u64,
    seen: HashSet<String>,
}

impl Redelivery {
    pub(crate) fn new(retained: u64, live_from: u64) -> Redelivery {
        Redelivery {
            remaining: retained,
            live_from,
            seen: HashSet::new(),
        }
    }

    /// Notes a change at cache offset `offset`. Returns whether every
    /// retained value is now in.
    pub(crate) fn note(&mut self, key: &str, offset: u64) -> bool {
        self.seen.insert(key.to_owned());
        if offset < self.live_from {
            self.remaining = self.remaining.saturating_sub(1);
        }
        // A live change also means the retained ones are done.
        self.done() || offset >= self.live_from
    }

    pub(crate) fn done(&self) -> bool {
        self.remaining == 0
    }
}

/// Stops the tasks of workloads not in `current`.
fn prune(tasks: &mut HashMap<String, Task>, current: &HashSet<String>) {
    tasks.retain(|id, task| {
        let keep = current.contains(id);
        if !keep {
            tracing::info!(workload = %id, "gone while the watch was closed, stopping");
            let _ = task.stop.send(true);
        }
        keep
    });
}

async fn stop_all(tasks: HashMap<String, Task>) {
    let mut handles = Vec::new();
    for (_, task) in tasks {
        let _ = task.stop.send(true);
        handles.push(task.handle);
    }
    for mut handle in handles {
        if tokio::time::timeout(RELEASE_WITHIN, &mut handle)
            .await
            .is_err()
        {
            handle.abort();
        }
    }
}

fn apply(
    node: &Arc<Node>,
    tasks: &mut HashMap<String, Task>,
    seen: &mut HashMap<String, watch::Sender<Seen>>,
    id: String,
    value: Option<Bytes>,
) {
    let spec = value.map(|v| WorkloadSpec::decode(&v));
    let wanted = match spec {
        Some(Ok(spec)) if spec.state == WorkloadState::Running => Some(spec),
        Some(Err(error)) => {
            tracing::error!(workload = %id, %error, "ignoring an unreadable spec");
            None
        }
        _ => None,
    };
    if let Some(task) = tasks.get(&id) {
        // A changed spec restarts the task, which resumes under it.
        if !task.handle.is_finished() && wanted.as_ref() == Some(&task.spec) {
            return;
        }
        let _ = task.stop.send(true);
        tasks.remove(&id);
    }
    if let Some(spec) = wanted {
        let (stop, stopped) = watch::channel(false);
        let lease = seen_for(seen, &id).subscribe();
        let handle = tokio::spawn(own(node.clone(), id.clone(), spec.clone(), lease, stopped));
        tasks.insert(id, Task { stop, handle, spec });
    }
}

/// Claims the workload whenever its lease is free and runs it while the claim
/// holds, until `stop` is set or the workload fails.
async fn own(
    node: Arc<Node>,
    id: String,
    spec: WorkloadSpec,
    mut lease: watch::Receiver<Seen>,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        if !node.wait_for_chance(&id, &mut lease, &mut stop).await {
            return;
        }
        let slot = Slot::take(&node);
        if slot.is_none() {
            continue;
        }
        let floor = written_epoch(&node.felix, &id, &spec.output);
        match lease::claim(&node.felix, &node.node_id, &id, node.timing, floor).await {
            Ok(Some(mut held)) => {
                tracing::info!(workload = %id, epoch = held.epoch, "claimed");
                let ended = run_held(&node, &id, &spec, &mut held, &mut stop).await;
                match ended {
                    Ended::Failed | Ended::Stopped => {
                        lease::release(&node.felix, &held).await;
                        return;
                    }
                    Ended::Lost => {}
                }
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(workload = %id, error = %format!("{error:#}"), "claim failed");
            }
        }
        drop(slot);
        // Gives the watch time to deliver the lease that refused us.
        tokio::select! {
            () = tokio::time::sleep(RETRY_DELAY.min(node.timing.renew_after())) => {}
            _ = stop.wait_for(|stop| *stop) => return,
        }
    }
}

/// One of this node's `max_workloads` places, given back when dropped.
struct Slot<'a>(&'a Node);

impl<'a> Slot<'a> {
    fn take(node: &'a Node) -> Option<Slot<'a>> {
        node.held
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                node.placement.has_room(held).then_some(held + 1)
            })
            .ok()
            .map(|_| Slot(node))
    }
}

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        self.0.held.fetch_sub(1, Ordering::SeqCst);
        self.0.room.notify_waiters();
    }
}

impl Node {
    /// Waits until the lease looks free, this node has room and the placement
    /// policy's delay has passed. `false` means stop.
    async fn wait_for_chance(
        &self,
        id: &str,
        lease: &mut watch::Receiver<Seen>,
        stop: &mut watch::Receiver<bool>,
    ) -> bool {
        loop {
            let wait = match *lease.borrow_and_update() {
                Seen::Unknown | Seen::Free => Duration::ZERO,
                Seen::Held { expires_at_millis } => {
                    self.timing.claim_wait(expires_at_millis, now_millis())
                }
            };
            if !wait.is_zero() {
                tokio::select! {
                    () = tokio::time::sleep(wait) => {}
                    _ = lease.changed() => continue,
                    _ = stop.wait_for(|stop| *stop) => return false,
                }
            }
            if let Some(freed) = freed_unless(&self.room, || {
                self.placement.has_room(self.held.load(Ordering::SeqCst))
            }) {
                tokio::select! {
                    () = freed => {}
                    () = tokio::time::sleep(self.timing.ttl) => {}
                    _ = stop.wait_for(|stop| *stop) => return false,
                }
                continue;
            }
            let delay = self.placement.claim_delay(id, self.random());
            tokio::select! {
                () = tokio::time::sleep(delay) => return true,
                // Someone may have claimed it meanwhile.
                _ = lease.changed() => {}
                _ = stop.wait_for(|stop| *stop) => return false,
            }
        }
    }

    /// Uniform in `[0, 1)`, from an xorshift. It only spreads claims.
    fn random(&self) -> f64 {
        let mut x = self.random.load(Ordering::Relaxed);
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.random.store(x, Ordering::Relaxed);
        (x >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// Completes when a slot is freed, or `None` if `has_room`. It is registered
/// before `has_room` is asked, so a slot freed in between is not missed.
fn freed_unless(room: &Notify, has_room: impl FnOnce() -> bool) -> Option<Pin<Box<Notified<'_>>>> {
    let mut freed = Box::pin(room.notified());
    freed.as_mut().enable();
    (!has_room()).then_some(freed)
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// The larger epoch of the last records on the output and snapshot streams,
/// or 0 if they have none. A record that does not decode counts as 0: the
/// runner fails the workload for it, which only a holder may do.
async fn written_epoch(felix: &Felix, id: &str, output: &str) -> Result<u64> {
    let last = async |stream: &str| -> Result<Option<Bytes>> {
        let tail = felix.tail(stream).await?;
        Ok(felix.last_record(stream, tail).await?.map(|r| r.payload))
    };
    let output = last(output)
        .await?
        .map_or(0, |r| output::decode(&r).map_or(0, |(w, _)| w.epoch));
    let snapshot = last(&names::snapshot_stream(id))
        .await?
        .map_or(0, |r| snapshot::decode(&r).map_or(0, |(t, _)| t.epoch));
    Ok(output.max(snapshot))
}

enum Ended {
    Failed,
    Stopped,
    Lost,
}

/// Runs the workload, and records its failure if it fails, while renewing the
/// lease, until `stop` is set. Renewing goes on until the failure is
/// recorded, so a slow write does not let the lease lapse with the workload
/// still `running`. It runs in its own task so that nothing the workload
/// does can delay it. `held` comes back with the version the last renewal
/// wrote, which a release needs.
async fn run_held(
    node: &Arc<Node>,
    id: &str,
    spec: &WorkloadSpec,
    held: &mut Held,
    stop: &mut watch::Receiver<bool>,
) -> Ended {
    let (deadline, fence) = held.fence();
    let (end_renewals, ended) = oneshot::channel::<()>();
    let mut renewals = AbortOnDrop(tokio::spawn({
        let (node, mut held) = (node.clone(), held.clone());
        async move {
            let lost = tokio::select! {
                lost = lease::hold(&node.felix, &mut held, &deadline) => Some(lost),
                _ = ended => None,
            };
            (held, lost)
        }
    }));
    let (me, epoch) = (held.node.clone(), held.epoch);
    let work = async {
        let outcome = loop {
            match run_workload(&node.felix, &node.modules, id, spec, &fence).await {
                Ok(outcome) => break outcome,
                Err(error) => {
                    tracing::warn!(workload = %id, error = %format!("{error:#}"), "retrying");
                    tokio::time::sleep(RETRY_DELAY).await;
                }
            }
        };
        let reason = match outcome {
            Outcome::Failed(message) => message,
            Outcome::Lost => return Ended::Lost,
            Outcome::Superseded(found) => {
                // A later owner means this lease is gone, and a refused
                // renewal will say so. Still holding it means epochs went
                // backwards, which nothing here can repair.
                match lease::still_held(&node.felix, &me, epoch, id).await {
                    Ok(true) => format!(
                        "epoch {found} on the output stream is above this holder's {epoch} \
                         while it still holds the lease, so lease epochs went backwards"
                    ),
                    Ok(false) => {
                        tracing::warn!(workload = %id, "a later owner has taken over");
                        return Ended::Lost;
                    }
                    Err(error) => {
                        tracing::warn!(workload = %id, %error, "a later owner wrote; could not read the lease");
                        return Ended::Lost;
                    }
                }
            }
        };
        tracing::error!(workload = %id, %reason, "workload failed");
        if mark_failed(&node.felix, id, &reason, &fence).await {
            Ended::Failed
        } else {
            // Not recorded: claim again, fail again and retry.
            Ended::Lost
        }
    };
    let ended = tokio::select! {
        renewed = &mut renewals.0 => {
            if let Ok((renewed, lost)) = renewed {
                tracing::warn!(workload = %id, ?lost, "lost the lease");
                *held = renewed;
            }
            return Ended::Lost;
        }
        ended = work => ended,
        _ = stop.wait_for(|stop| *stop) => Ended::Stopped,
    };
    let _ = end_renewals.send(());
    if let Ok((renewed, _)) = (&mut renewals.0).await {
        *held = renewed;
    }
    ended
}

/// Aborts the task when dropped, so renewals end with the workload's task.
struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Records why the workload failed in its spec, with `cache_put_if` on the
/// version read so a concurrent edit is not overwritten. Only a holder does
/// this, so a stale node cannot fail a workload another node is running.
/// Returns whether it was recorded before the fence lapsed.
async fn mark_failed(felix: &Felix, id: &str, message: &str, fence: &Fence) -> bool {
    let failure = message.lines().next().unwrap_or_default().to_owned();
    while fence.holds() {
        let attempt = async {
            let Some(found) = felix.cache_get_versioned(names::WORKLOADS, id).await? else {
                return Ok(true);
            };
            let mut spec = WorkloadSpec::decode(&found.value)?;
            if spec.state != WorkloadState::Running {
                return Ok(true);
            }
            spec.state = WorkloadState::Failed;
            spec.failure = Some(failure.clone());
            faults::before_failure_record().await;
            let put = felix
                .cache_put_if(
                    names::WORKLOADS,
                    id,
                    Bytes::from(spec.encode()),
                    None,
                    CacheCondition::Version(found.version),
                )
                .await?;
            anyhow::Ok(put.applied)
        };
        match attempt.await {
            Ok(true) => return true,
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(workload = %id, %error, "could not record the failure, retrying");
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests;
