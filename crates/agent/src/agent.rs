//! The node loop. It watches the `workloads` and `leases` caches, writes this
//! node to `nodes`, and for every running workload whose lease is free tries
//! to claim it, as the placement policy allows. It runs a workload only while
//! it holds the lease.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use bytes::Bytes;
use felix_client::{CacheChange, CacheWatchItem, ClusterCacheWatch};
use felixd_core::lease::LeaseTiming;
use felixd_core::names;
use felixd_core::node::NodeInfo;
use felixd_core::placement::{FirstClaimWins, Offer, Placement};
use felixd_core::spec::{WorkloadSpec, WorkloadState};
use felixd_runtime::Runtime;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::felix::Felix;
use crate::lease::{self, Fence, Held};
use crate::runner::{Outcome, run_workload};
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
    runtime: Runtime,
    node_id: String,
    timing: LeaseTiming,
    placement: Box<dyn Placement>,
    max_workloads: usize,
    /// Workloads this node holds a lease on now.
    held: AtomicUsize,
    random: AtomicU64,
}

/// What the leases watch last said about one workload's lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Seen {
    /// No change seen yet.
    Unknown,
    /// Deleted or expired.
    Free,
    /// Written, expiring at this broker time (0 is never).
    Held { expires_at_millis: u64 },
}

/// A workload this agent is trying to claim or running.
struct Task {
    stop: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

/// Runs until `shutdown` completes, then releases the leases it holds.
pub async fn run(
    settings: &Settings,
    config: AgentConfig,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
        ^ u64::from(std::process::id()) << 32;
    let node = Arc::new(Node {
        felix: Felix::connect(settings).await?,
        runtime: Runtime::new().map_err(|e| anyhow::anyhow!("{e}"))?,
        node_id: config.node_id,
        timing: config.lease,
        placement: Box::new(config.placement),
        max_workloads: config.placement.max_workloads,
        held: AtomicUsize::new(0),
        random: AtomicU64::new(seed | 1),
    });
    tracing::info!(node = %node.node_id, "agent started");
    let presence = tokio::spawn(announce(node.clone()));
    let mut tasks: HashMap<String, Task> = HashMap::new();
    let mut seen: HashMap<String, watch::Sender<Seen>> = HashMap::new();
    let mut workloads = node.felix.watch_retained(names::WORKLOADS).await?;
    let mut leases = node.felix.watch_retained(names::LEASES).await?;
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => {
                presence.abort();
                stop_all(tasks).await;
                return Ok(());
            }
            item = workloads.recv() => match change(item) {
                Some(change) => apply(&node, &mut tasks, &mut seen, change.key, change.value),
                // A new watch delivers every workload again.
                None => workloads = rewatch(&node.felix, names::WORKLOADS).await,
            },
            item = leases.recv() => match change(item) {
                Some(change) => {
                    let now = if change.value.is_some() {
                        Seen::Held { expires_at_millis: change.expires_at_millis }
                    } else {
                        Seen::Free
                    };
                    seen_for(&mut seen, &change.key).send_replace(now);
                }
                None => leases = rewatch(&node.felix, names::LEASES).await,
            },
        }
    }
}

/// The change a watch delivered, or `None` when the watch has ended.
fn change(item: Option<CacheWatchItem>) -> Option<CacheChange> {
    match item {
        Some(CacheWatchItem::Change(change)) => Some(change),
        Some(CacheWatchItem::Lagged { .. } | CacheWatchItem::ShardMoved(_)) | None => None,
    }
}

async fn rewatch(felix: &Felix, cache: &str) -> ClusterCacheWatch {
    loop {
        tokio::time::sleep(RETRY_DELAY).await;
        match felix.watch_retained(cache).await {
            Ok(watch) => return watch,
            Err(error) => tracing::warn!(cache, %error, "could not watch, retrying"),
        }
    }
}

fn seen_for<'a>(seen: &'a mut HashMap<String, watch::Sender<Seen>>, id: &str) -> &'a watch::Sender<Seen> {
    seen.entry(id.to_owned())
        .or_insert_with(|| watch::channel(Seen::Unknown).0)
}

/// Writes this node to `nodes` every TTL/3, with a TTL, so the entry goes away
/// soon after the node does.
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
            tracing::warn!(%error, "could not announce this node");
        }
        tokio::time::sleep(node.timing.renew_after()).await;
    }
}

async fn stop_all(tasks: HashMap<String, Task>) {
    let mut handles = Vec::new();
    for (_, task) in tasks {
        let _ = task.stop.send(true);
        handles.push(task.handle);
    }
    for mut handle in handles {
        if tokio::time::timeout(RELEASE_WITHIN, &mut handle).await.is_err() {
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
        if !task.handle.is_finished() && wanted.is_some() {
            return;
        }
        let _ = task.stop.send(true);
        tasks.remove(&id);
    }
    if let Some(spec) = wanted {
        let (stop, stopped) = watch::channel(false);
        let lease = seen_for(seen, &id).subscribe();
        let handle = tokio::spawn(own(node.clone(), id.clone(), spec, lease, stopped));
        tasks.insert(id, Task { stop, handle });
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
        match lease::claim(&node.felix, &node.node_id, &id, node.timing).await {
            Ok(Some(mut held)) => {
                tracing::info!(workload = %id, epoch = held.epoch, "claimed");
                node.held.fetch_add(1, Ordering::Relaxed);
                let ended = tokio::select! {
                    ended = run_held(&node, &id, &spec, &mut held) => ended,
                    _ = stop.wait_for(|stop| *stop) => Ended::Stopped,
                };
                node.held.fetch_sub(1, Ordering::Relaxed);
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
        // Give the watch time to deliver the lease that refused us.
        tokio::select! {
            () = tokio::time::sleep(RETRY_DELAY.min(node.timing.renew_after())) => {}
            _ = stop.wait_for(|stop| *stop) => return,
        }
    }
}

impl Node {
    /// Waits until the lease looks free and the placement policy's delay has
    /// passed. `false` means stop.
    async fn wait_for_chance(
        &self,
        id: &str,
        lease: &mut watch::Receiver<Seen>,
        stop: &mut watch::Receiver<bool>,
    ) -> bool {
        loop {
            let wait = match *lease.borrow_and_update() {
                Seen::Unknown | Seen::Free => Duration::ZERO,
                Seen::Held { expires_at_millis } => self
                    .timing
                    .claim_wait(Some(expires_at_millis), now_millis()),
            };
            if !wait.is_zero() {
                tokio::select! {
                    () = tokio::time::sleep(wait) => {}
                    _ = lease.changed() => continue,
                    _ = stop.wait_for(|stop| *stop) => return false,
                }
            }
            let offer = Offer {
                workload: id,
                held: self.held.load(Ordering::Relaxed),
            };
            let Some(delay) = self.placement.claim_after(offer, self.random()) else {
                // Full. Look again in a while: a workload may have ended.
                tokio::select! {
                    () = tokio::time::sleep(self.timing.ttl) => continue,
                    _ = stop.wait_for(|stop| *stop) => return false,
                }
            };
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                _ = stop.wait_for(|stop| *stop) => return false,
            }
            // Someone may have claimed it during the delay.
            if lease.has_changed().unwrap_or(false)
                && matches!(*lease.borrow(), Seen::Held { expires_at_millis } if self.timing.claim_wait(Some(expires_at_millis), now_millis()) > Duration::ZERO)
            {
                continue;
            }
            return true;
        }
    }

    /// Uniform in `[0, 1)`, from an xorshift. Only spreads claims, so it needs
    /// no quality beyond that.
    fn random(&self) -> f64 {
        let mut x = self.random.load(Ordering::Relaxed);
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.random.store(x, Ordering::Relaxed);
        (x >> 11) as f64 / (1u64 << 53) as f64
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

enum Ended {
    Failed,
    Stopped,
    Lost,
}

/// Renews the lease and runs the workload until one of them ends.
async fn run_held(node: &Node, id: &str, spec: &WorkloadSpec, held: &mut Held) -> Ended {
    let (deadline, fence) = held.fence();
    let work = async {
        loop {
            match run_workload(&node.felix, &node.runtime, id, spec, &fence).await {
                Ok(Outcome::Failed(message)) => return Some(message),
                Ok(Outcome::Lost) => return None,
                Err(error) => {
                    tracing::warn!(workload = %id, error = %format!("{error:#}"), "retrying");
                    tokio::time::sleep(RETRY_DELAY).await;
                }
            }
        }
    };
    tokio::select! {
        lost = lease::hold(&node.felix, held, &deadline) => {
            tracing::warn!(workload = %id, ?lost, "lost the lease");
            Ended::Lost
        }
        failed = work => match failed {
            Some(message) => {
                tracing::error!(workload = %id, %message, "workload failed");
                mark_failed(&node.felix, id, spec.clone(), &message, &fence).await;
                Ended::Failed
            }
            None => {
                tracing::warn!(workload = %id, "a later owner has taken over");
                Ended::Lost
            }
        },
    }
}

/// Records why the workload failed in its spec. Only a holder does this, so a
/// stale node cannot fail a workload another node is running.
async fn mark_failed(felix: &Felix, id: &str, mut spec: WorkloadSpec, message: &str, fence: &Fence) {
    spec.state = WorkloadState::Failed;
    spec.failure = Some(message.lines().next().unwrap_or_default().to_owned());
    let value = Bytes::from(spec.encode());
    while fence.holds() {
        match felix.cache_put(names::WORKLOADS, id, value.clone()).await {
            Ok(()) => return,
            Err(error) => {
                tracing::warn!(workload = %id, %error, "could not record the failure, retrying");
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    }
}
