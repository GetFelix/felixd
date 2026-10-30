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
use felixd_core::class::{ErrorClass, Failure, OperatorReason};
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
use crate::lease::{self, Confirmer, Fence, Held};
use crate::output::{self as output_log, Recorded, record_failure};
use crate::runner::{LostLease, ModuleMissing, Modules, NeedsOperator, Outcome, run_workload};
use crate::settings::Settings;
use crate::specs;

const RETRY_DELAY: Duration = Duration::from_secs(1);

/// How long opening a watch may take. Right after a broker dies, a client
/// whose routing still names it can wait on it with no error.
const OPEN_WATCH_WITHIN: Duration = Duration::from_secs(10);

/// How often the brokers serving the watches are asked whether they are
/// still there, and how long they have to answer.
const PROBE_WATCHES_EVERY: Duration = Duration::from_secs(5);

/// How long a spec record past the tail of `sys.specs` is waited for before
/// the workload is left to an operator.
const RECORD_WAIT: Duration = Duration::from_secs(30);

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
    let mut probe = tokio::time::interval(PROBE_WATCHES_EVERY);
    probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
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
            _ = probe.tick() => {
                if !node.felix.watch_answers(&leases, names::LEASES, PROBE_WATCHES_EVERY).await {
                    tracing::warn!(cache = names::LEASES, "the watched broker does not answer, watching again");
                    match unless(shutdown.as_mut(), reopen(&node, names::LEASES, Duration::ZERO)).await {
                        Some(opened) => leases = opened,
                        None => return shut_down(&node, presence, tasks).await,
                    }
                }
                if node.felix.watch_answers(&watch, names::WORKLOADS, PROBE_WATCHES_EVERY).await {
                    continue;
                }
                tracing::warn!(cache = names::WORKLOADS, "the watched broker does not answer, watching again");
                None
            }
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
        match tokio::time::timeout(OPEN_WATCH_WITHIN, node.felix.watch_retained(cache)).await {
            Ok(Ok(watch)) => return watch,
            Ok(Err(error)) => tracing::warn!(cache, %error, "could not watch, retrying"),
            Err(_) => tracing::warn!(cache, "opening a watch timed out, retrying"),
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
/// holds, until `stop` is set or the workload fails or needs an operator.
async fn own(
    node: Arc<Node>,
    id: String,
    spec: WorkloadSpec,
    mut lease: watch::Receiver<Seen>,
    mut stop: watch::Receiver<bool>,
) {
    // What is found before the lease claim. It is recorded under a claim, so
    // that the record is fenced like a failure and the other nodes skip the
    // workload instead of finding the same thing.
    let Some(mut pending) = before_claim(&node, &id, &spec, &mut stop).await else {
        return;
    };
    loop {
        if !node.wait_for_chance(&id, &mut lease, &mut stop).await {
            return;
        }
        match recorded_needs_operator(&node.felix, &spec).await {
            Ok(Some(reason)) => {
                tracing::error!(workload = %id, %reason, "the output stream records that the workload needs an operator, not running it");
                let _ = stop.wait_for(|stop| *stop).await;
                return;
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(workload = %id, error = %format!("{error:#}"), "could not read the output stream");
                if !pause(&mut stop).await {
                    return;
                }
                continue;
            }
        }
        // Before the lease claim, so a spec every node refuses does not
        // take an epoch on each of them. The runner looks again after its
        // claim is on the stream. A node with a problem to record looks too:
        // a stale one must not record over a later spec.
        match specs::refuse_stale_or_foreign(&node.felix, &id, &spec).await {
            Ok(()) => {}
            Err(error) => {
                if let Some(problem) = error.downcast_ref::<NeedsOperator>() {
                    if !recordable(&spec, problem) {
                        needs_operator(&id, problem, &mut stop).await;
                        return;
                    }
                    pending.get_or_insert_with(|| problem.clone());
                } else {
                    tracing::warn!(workload = %id, error = %format!("{error:#}"), "could not read the output stream");
                    if !pause(&mut stop).await {
                        return;
                    }
                    continue;
                }
            }
        }
        let slot = Slot::take(&node);
        if slot.is_none() {
            continue;
        }
        let output_tail = std::sync::Mutex::new(None);
        let floor = written_epoch(&node.felix, &id, &spec.output, &output_tail);
        match lease::claim(&node.felix, &node.node_id, &id, node.timing, floor).await {
            Ok(Some(mut held)) => {
                tracing::info!(workload = %id, epoch = held.epoch, "claimed");
                let claim_from = output_tail.lock().unwrap_or_else(|e| e.into_inner()).take();
                let run = Run {
                    spec: &spec,
                    claim_from,
                    pending: pending.as_ref(),
                };
                let ended = run_held(&node, &id, run, &mut held, &mut stop).await;
                match ended {
                    Ended::Failed | Ended::Stopped => {
                        lease::release(&node.felix, &held).await;
                        return;
                    }
                    Ended::NeedsOperator(problem) => {
                        lease::release(&node.felix, &held).await;
                        needs_operator(&id, &problem, &mut stop).await;
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

/// The checks before any claim: the module and the spec record. `None` means
/// stop; `Some(problem)` is a problem to record under a claim, if any.
async fn before_claim(
    node: &Node,
    id: &str,
    spec: &WorkloadSpec,
    stop: &mut watch::Receiver<bool>,
) -> Option<Option<NeedsOperator>> {
    let found = async {
        // A module missing for this long is not a stale read.
        let missing_until = tokio::time::Instant::now() + RECORD_WAIT;
        loop {
            let Err(error) = node.modules.compiled(&node.felix, &spec.module).await else {
                break;
            };
            if let Some(problem) = error.downcast_ref::<NeedsOperator>() {
                return Some(Some(problem.clone()));
            }
            // The runner fails the workload for a module it cannot load.
            if !error.is::<ModuleMissing>() {
                break;
            }
            if tokio::time::Instant::now() >= missing_until {
                let problem = NeedsOperator::new(OperatorReason::ModuleMissing, error.to_string());
                return Some(Some(problem));
            }
            tracing::warn!(workload = %id, %error, "waiting for the module");
            if !pause(stop).await {
                return None;
            }
        }
        // Only a spec in the history runs, so check and replay see what ran.
        let past_tail_until = tokio::time::Instant::now() + RECORD_WAIT;
        loop {
            let Err(error) = specs::check_record(&node.felix, id, spec).await else {
                return Some(None);
            };
            if let Some(problem) = error.downcast_ref::<NeedsOperator>() {
                return Some(Some(problem.clone()));
            }
            if let Some(past) = error.downcast_ref::<specs::PastTail>()
                && tokio::time::Instant::now() >= past_tail_until
            {
                let problem = NeedsOperator::new(OperatorReason::SpecUnrecorded, past.to_string());
                return Some(Some(problem));
            }
            tracing::warn!(workload = %id, error = %format!("{error:#}"), "could not read the spec record");
            if !pause(stop).await {
                return None;
            }
        }
    };
    match found.await? {
        Some(problem) if !recordable(spec, &problem) => {
            needs_operator(id, &problem, stop).await;
            None
        }
        found => Some(found),
    }
}

/// Whether `problem` is written to the output stream. A record names the
/// spec record, so a spec with none cannot have one. A stale spec is this
/// node's alone, and its watch restarts it once it catches up.
fn recordable(spec: &WorkloadSpec, problem: &NeedsOperator) -> bool {
    spec.record.is_some() && problem.reason != OperatorReason::SpecStale
}

/// Why, if the last record on the output stream says that the spec record
/// this spec comes from needs an operator. Only a spec change, which is a new
/// spec record, or removing the workload clears it.
async fn recorded_needs_operator(felix: &Felix, spec: &WorkloadSpec) -> Result<Option<String>> {
    let Some(record) = spec.record else {
        return Ok(None);
    };
    let tail = felix.tail(&spec.output).await?;
    let last = felix.last_record(&spec.output, tail).await?;
    Ok(last.and_then(|last| parked_by(&last.payload, record)))
}

/// Why `found` parks spec record `record`, if it does. A newer build may
/// record a reason this one cannot read, and that still parks it.
fn parked_by(found: &[u8], record: u64) -> Option<String> {
    match output::decode(found) {
        Ok(found) => found.needs_operator_under(record).map(|r| r.to_string()),
        Err(error) => (output::needs_operator_spec_of(found) == Some(record))
            .then(|| format!("a reason this build cannot read ({error})")),
    }
}

/// Waits a moment before trying again. `false` means stop.
async fn pause(stop: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        () = tokio::time::sleep(RETRY_DELAY) => true,
        _ = stop.wait_for(|stop| *stop) => false,
    }
}

/// Logs that the workload needs an operator and waits for its spec to
/// change or go, which restarts or stops the task.
async fn needs_operator(id: &str, problem: &NeedsOperator, stop: &mut watch::Receiver<bool>) {
    tracing::error!(workload = %id, reason = %problem, "the workload needs an operator, not running it");
    let _ = stop.wait_for(|stop| *stop).await;
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
/// claims included, or 0 if they have none. A record that does not decode
/// counts as 0: the runner fails the workload for it, which only a holder may
/// do. The output stream's tail it read goes in `output_tail`, where the
/// holder writes its claim.
async fn written_epoch(
    felix: &Felix,
    id: &str,
    output: &str,
    output_tail: &std::sync::Mutex<Option<u64>>,
) -> Result<u64> {
    let last = async |stream: &str| -> Result<(u64, Option<Bytes>)> {
        let tail = felix.tail(stream).await?;
        Ok((
            tail,
            felix.last_record(stream, tail).await?.map(|r| r.payload),
        ))
    };
    let (tail, output) = last(output).await?;
    *output_tail.lock().unwrap_or_else(|e| e.into_inner()) = Some(tail);
    let output = output.map_or(0, |r| output::decode(&r).map_or(0, |r| r.epoch()));
    let snapshot = last(&names::snapshot_stream(id))
        .await?
        .1
        .map_or(0, |r| snapshot::decode(&r).map_or(0, |r| r.epoch()));
    Ok(output.max(snapshot))
}

enum Ended {
    Failed,
    Stopped,
    Lost,
    NeedsOperator(NeedsOperator),
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
    run: Run<'_>,
    held: &mut Held,
    stop: &mut watch::Receiver<bool>,
) -> Ended {
    let Run {
        spec,
        mut claim_from,
        pending,
    } = run;
    let (deadline, fence) = held.fence();
    let (confirmer, mut asks) = Confirmer::new();
    let (end_renewals, ended) = oneshot::channel::<()>();
    let mut renewals = AbortOnDrop(tokio::spawn({
        let (node, mut held) = (node.clone(), held.clone());
        async move {
            let lost = tokio::select! {
                lost = lease::hold(&node.felix, &mut held, &deadline, &mut asks) => Some(lost),
                _ = ended => None,
            };
            (held, lost)
        }
    }));
    let epoch = held.epoch;
    let work = async {
        if let Some(problem) = pending {
            return record_needs_operator(node, id, spec, problem, &fence, &confirmer).await;
        }
        let outcome = loop {
            if faults::fail_workload() {
                break Outcome::Failed("injected failure".to_owned());
            }
            match run_workload(
                &node.felix,
                &node.modules,
                id,
                spec,
                &fence,
                &mut claim_from,
            )
            .await
            {
                Ok(outcome) => break outcome,
                Err(error) => {
                    tracing::warn!(workload = %id, error = %format!("{error:#}"), "retrying");
                    tokio::time::sleep(RETRY_DELAY).await;
                }
            }
        };
        // The failure record's epoch. It is the later epoch found when epochs
        // went backwards, so it stays above every record before it and below
        // any claim taken after.
        let mut at_epoch = epoch;
        let mut class = ErrorClass::Poison;
        let reason = match outcome {
            Outcome::Failed(message) => message,
            Outcome::Lost => return Ended::Lost,
            Outcome::NeedsOperator {
                problem,
                recorded: true,
            } => return Ended::NeedsOperator(problem),
            Outcome::NeedsOperator { problem, .. } if !recordable(spec, &problem) => {
                return Ended::NeedsOperator(problem);
            }
            Outcome::NeedsOperator { problem, .. } => {
                return record_needs_operator(node, id, spec, &problem, &fence, &confirmer).await;
            }
            Outcome::FailedOnStream {
                spec_version,
                failure,
            } => {
                tracing::error!(workload = %id, reason = %failure.detail, "the output stream records a failure");
                return if write_failed(&node.felix, id, spec_version, &failure).await {
                    Ended::Failed
                } else {
                    Ended::Lost
                };
            }
            Outcome::Superseded(found) => {
                // A later owner means this lease is gone. If a renewal at the
                // held version is still applied, epochs went backwards, which
                // nothing here can repair. A read of the lease would not do:
                // it can be stale.
                if !confirmer.confirm().await {
                    tracing::warn!(workload = %id, "a later owner has taken over");
                    return Ended::Lost;
                }
                at_epoch = found;
                class = ErrorClass::NeedsOperator;
                format!(
                    "epoch {found} on the output stream is above this holder's {epoch} \
                     while it still holds the lease, so lease epochs went backwards"
                )
            }
        };
        tracing::error!(workload = %id, %reason, "workload failed");
        let failing = Failing {
            spec,
            failure: Failure {
                class,
                detail: reason.lines().next().unwrap_or_default().to_owned(),
            },
            epoch: at_epoch,
        };
        if mark_failed(&node.felix, id, &failing, &fence, &confirmer).await {
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

/// A failure to record: the spec that ran, why, and the failure record's epoch.
struct Failing<'a> {
    spec: &'a WorkloadSpec,
    failure: Failure,
    epoch: u64,
}

/// What a claim that won runs: the spec, from the output stream tail read
/// with the epoch, or only the needs-operator record for `pending`.
struct Run<'a> {
    spec: &'a WorkloadSpec,
    claim_from: Option<u64>,
    pending: Option<&'a NeedsOperator>,
}

/// Records that the spec record `spec` comes from needs an operator, as a
/// holder: it renews through `confirmer` first, then appends the record
/// behind everything of earlier epochs, like a failure. A holder that claimed
/// only to record it writes no claim, so this is no takeover. It is not a
/// failure: the spec stays `running`, and the other nodes find the record
/// and leave the workload alone.
async fn record_needs_operator(
    node: &Node,
    id: &str,
    spec: &WorkloadSpec,
    problem: &NeedsOperator,
    fence: &Fence,
    confirmer: &Confirmer,
) -> Ended {
    let Some(record) = spec.record else {
        return Ended::NeedsOperator(problem.clone());
    };
    while fence.holds() {
        // The cache version, which a failure already on the stream is compared with.
        let version = match node.felix.cache_get_versioned(names::WORKLOADS, id).await {
            Ok(Some(found)) if WorkloadSpec::decode(&found.value).as_ref() == Ok(spec) => {
                found.version
            }
            // Stopped, edited, or a stale read: the watch restarts or stops
            // the task, which decides again.
            Ok(_) => return Ended::Lost,
            Err(error) => {
                tracing::warn!(workload = %id, %error, "could not read the spec, retrying");
                tokio::time::sleep(RETRY_DELAY).await;
                continue;
            }
        };
        faults::before_failure_record().await;
        if !confirmer.confirm().await || !fence.holds() {
            tracing::warn!(workload = %id, "the lease is lost, not recording that it needs an operator");
            return Ended::Lost;
        }
        let recorded = output_log::record_needs_operator(
            &node.felix,
            &spec.output,
            fence,
            fence.epoch(),
            (version, record),
            problem,
        )
        .await;
        match recorded {
            Ok(Recorded::NeedsOperator) => return Ended::NeedsOperator(problem.clone()),
            Ok(Recorded::Superseded(epoch)) => {
                tracing::warn!(workload = %id, epoch, "a later owner has written, not recording");
                return Ended::Lost;
            }
            Ok(Recorded::Failed {
                spec_version,
                failure,
            }) => {
                tracing::error!(workload = %id, reason = %failure.detail, "the output stream records a failure");
                return if write_failed(&node.felix, id, spec_version, &failure).await {
                    Ended::Failed
                } else {
                    Ended::Lost
                };
            }
            Err(error) if error.is::<LostLease>() => return Ended::Lost,
            Err(error) => {
                tracing::warn!(workload = %id, %error, "could not record that it needs an operator, retrying");
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    }
    Ended::Lost
}

/// Records that the workload failed: a failure record on its output stream,
/// then `failed` and the reason in its spec. Only a holder does this. Right
/// before the record it renews its lease through `confirmer`, and gives up if
/// that is refused. The record is what fences the failure: it is appended
/// with `publish_if` behind the holder's claim, so a node that was frozen
/// while another took over cannot fail the workload the other now runs, even
/// if it is frozen after the renewal. Returns whether the failure was recorded.
async fn mark_failed(
    felix: &Felix,
    id: &str,
    failing: &Failing<'_>,
    fence: &Fence,
    confirmer: &Confirmer,
) -> bool {
    let running = failing.spec;
    let failure = &failing.failure;
    while fence.holds() {
        let attempt = async {
            let Some(found) = felix.cache_get_versioned(names::WORKLOADS, id).await? else {
                return Ok(Err(true));
            };
            let spec = WorkloadSpec::decode(&found.value)?;
            if spec != *running {
                // Stopped, edited, or a stale read: the watch restarts or
                // stops the task, which decides again.
                return Ok(Err(false));
            }
            faults::before_failure_record().await;
            if !confirmer.confirm().await || !fence.holds() {
                tracing::warn!(workload = %id, "the lease is lost, not recording the failure");
                return Ok(Err(false));
            }
            let recorded = record_failure(
                felix,
                &spec.output,
                fence,
                failing.epoch,
                found.version,
                failure,
            )
            .await?;
            anyhow::Ok(Ok(recorded))
        };
        match attempt.await {
            Ok(Ok(Recorded::Failed {
                spec_version,
                failure,
            })) => return write_failed(felix, id, spec_version, &failure).await,
            Ok(Ok(Recorded::NeedsOperator)) => return false,
            Ok(Ok(Recorded::Superseded(epoch))) => {
                tracing::warn!(workload = %id, epoch, "a later owner has written, not failing");
                return false;
            }
            Ok(Err(done)) => return done,
            Err(error) => {
                tracing::warn!(workload = %id, %error, "could not record the failure, retrying");
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    }
    false
}

/// Writes the failure the output stream records under `version` into the
/// spec, with `cache_put_if` on that version so a later edit is not
/// overwritten. It needs no lease, because the record has decided. Returns
/// `false` if the spec was written again since, still running: the record
/// is then not about the spec that runs now.
/// What [`write_failed`] does with a spec read at `found` in `state`, for a
/// failure recorded under `version`.
#[derive(Debug, PartialEq, Eq)]
enum SpecWrite {
    /// Nothing to write. `true` if the workload is no longer running.
    Done(bool),
    /// The read is older than the record, so it is stale. Read again.
    Stale,
    Write,
}

fn spec_write(found: u64, state: WorkloadState, version: u64) -> SpecWrite {
    if found < version {
        SpecWrite::Stale
    } else if state != WorkloadState::Running {
        SpecWrite::Done(true)
    } else if found > version {
        SpecWrite::Done(false)
    } else {
        SpecWrite::Write
    }
}

async fn write_failed(felix: &Felix, id: &str, version: u64, failure: &Failure) -> bool {
    loop {
        let attempt = async {
            let Some(found) = felix.cache_get_versioned(names::WORKLOADS, id).await? else {
                return Ok(Some(true));
            };
            let mut spec = WorkloadSpec::decode(&found.value)?;
            match spec_write(found.version, spec.state, version) {
                SpecWrite::Done(done) => return Ok(Some(done)),
                SpecWrite::Stale => {
                    anyhow::bail!("read spec version {} below {version}", found.version)
                }
                SpecWrite::Write => {}
            }
            spec.state = WorkloadState::Failed;
            spec.failure = Some(failure.clone());
            let put = felix
                .cache_put_if(
                    names::WORKLOADS,
                    id,
                    Bytes::from(spec.encode()),
                    None,
                    CacheCondition::Version(version),
                )
                .await?;
            anyhow::Ok(put.applied.then_some(true))
        };
        match attempt.await {
            Ok(Some(done)) => return done,
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(workload = %id, %error, "could not write the failure, retrying");
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    }
}

#[cfg(test)]
mod tests;
