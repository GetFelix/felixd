//! The node loop. It watches the `workloads` cache and, for every workload
//! whose state is `running`, tries to claim its lease. It runs a workload only
//! while it holds the lease.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use felix_client::{CacheCondition, CacheWatchItem, ClusterCacheWatch};
use felixd_core::lease::{LeaseTiming, MIN_TTL};
use felixd_core::spec::{WorkloadSpec, WorkloadState};
use felixd_core::{names, output};
use felixd_runtime::Runtime;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::faults;
use crate::felix::Felix;
use crate::lease::{self, Fence, Held};
use crate::runner::{Outcome, run_workload};
use crate::settings::Settings;

const RETRY_DELAY: Duration = Duration::from_secs(1);

/// How long shutdown waits for workloads to release their leases.
const RELEASE_WITHIN: Duration = Duration::from_secs(2);

/// Who this node is and how it holds leases.
#[derive(Clone, Debug)]
pub struct AgentConfig {
    /// This node's name in the leases it writes. Each running agent needs its
    /// own.
    pub node_id: String,
    pub lease: LeaseTiming,
}

struct Node {
    felix: Felix,
    runtime: Runtime,
    config: AgentConfig,
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
    anyhow::ensure!(
        config.lease.ttl >= MIN_TTL,
        "the lease TTL must be at least {} ms",
        MIN_TTL.as_millis()
    );
    let node = Arc::new(Node {
        felix: Felix::connect(settings).await?,
        runtime: Runtime::new().map_err(|e| anyhow::anyhow!("{e}"))?,
        config,
    });
    tracing::info!(node = %node.config.node_id, "agent started");
    let mut tasks: HashMap<String, Task> = HashMap::new();
    let mut watch = node.felix.watch_retained(names::WORKLOADS).await?;
    // Set while a new watch is redelivering the current workloads.
    let mut redelivery: Option<Redelivery> = None;
    tokio::pin!(shutdown);
    loop {
        let item = tokio::select! {
            () = &mut shutdown => {
                stop_all(tasks).await;
                return Ok(());
            }
            item = watch.recv() => item,
        };
        let reopen_after = match item {
            Some(CacheWatchItem::Change(change)) => {
                match faults::drop_watch(&change.key, change.value.is_some()) {
                    Some(gap) => gap,
                    None => {
                        let (id, offset) = (change.key.clone(), change.offset);
                        apply(&node, &mut tasks, change.key, change.value);
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
        watch = reopen(&node, reopen_after).await;
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

async fn reopen(node: &Node, delay: Duration) -> ClusterCacheWatch {
    loop {
        tokio::time::sleep(delay).await;
        match node.felix.watch_retained(names::WORKLOADS).await {
            Ok(watch) => return watch,
            Err(error) => tracing::warn!(%error, "could not watch workloads, retrying"),
        }
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

fn apply(node: &Arc<Node>, tasks: &mut HashMap<String, Task>, id: String, value: Option<Bytes>) {
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
        let handle = tokio::spawn(own(node.clone(), id.clone(), spec, stopped));
        tasks.insert(id, Task { stop, handle });
    }
}

/// Claims the workload whenever its lease is free and runs it while the claim
/// holds, until `stop` is set or the workload fails.
async fn own(node: Arc<Node>, id: String, spec: WorkloadSpec, mut stop: watch::Receiver<bool>) {
    let timing = node.config.lease;
    loop {
        let floor = output_epoch(&node.felix, &spec.output);
        match lease::claim(&node.felix, &node.config.node_id, &id, timing, floor).await {
            Ok(Some(mut held)) => {
                tracing::info!(workload = %id, epoch = held.epoch, "claimed");
                let ended = tokio::select! {
                    ended = run_held(&node, &id, &spec, &mut held) => ended,
                    _ = stop.wait_for(|stop| *stop) => Ended::Stopped,
                };
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
        tokio::select! {
            () = tokio::time::sleep(timing.renew_after()) => {}
            _ = stop.wait_for(|stop| *stop) => return,
        }
    }
}

/// The epoch of the last record on `stream`, or 0 if it has none. A record
/// that does not decode counts as 0: the runner fails the workload for it,
/// which only a holder may do.
async fn output_epoch(felix: &Felix, stream: &str) -> Result<u64> {
    let tail = felix.tail(stream).await?;
    Ok(match felix.last_record(stream, tail).await? {
        Some(record) => output::decode(&record.payload).map_or(0, |(last, _)| last.epoch),
        None => 0,
    })
}

enum Ended {
    Failed,
    Stopped,
    Lost,
}

/// Runs the workload, and records its failure if it fails, while renewing the
/// lease. Renewing goes on until the failure is recorded, so a slow write
/// does not let the lease lapse with the workload still `running`.
async fn run_held(node: &Node, id: &str, spec: &WorkloadSpec, held: &mut Held) -> Ended {
    let (deadline, fence) = held.fence();
    let (me, epoch) = (held.node.clone(), held.epoch);
    let work = async {
        let outcome = loop {
            match run_workload(&node.felix, &node.runtime, id, spec, &fence).await {
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
    tokio::select! {
        lost = lease::hold(&node.felix, held, &deadline) => {
            tracing::warn!(workload = %id, ?lost, "lost the lease");
            Ended::Lost
        }
        ended = work => ended,
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
