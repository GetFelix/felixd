//! The node loop. It watches the `workloads` cache and, for every workload
//! whose state is `running`, tries to claim its lease. It runs a workload only
//! while it holds the lease.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use felix_client::CacheWatchItem;
use felixd_core::lease::LeaseTiming;
use felixd_core::names;
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
    let node = Arc::new(Node {
        felix: Felix::connect(settings).await?,
        runtime: Runtime::new().map_err(|e| anyhow::anyhow!("{e}"))?,
        config,
    });
    tracing::info!(node = %node.config.node_id, "agent started");
    let mut tasks: HashMap<String, Task> = HashMap::new();
    tokio::pin!(shutdown);
    loop {
        let mut watch = node.felix.watch_retained(names::WORKLOADS).await?;
        loop {
            let item = tokio::select! {
                () = &mut shutdown => {
                    stop_all(tasks).await;
                    return Ok(());
                }
                item = watch.recv() => item,
            };
            match item {
                Some(CacheWatchItem::Change(change)) => {
                    apply(&node, &mut tasks, change.key, change.value);
                }
                // The watch ended; open a new one, which delivers every
                // workload again.
                Some(CacheWatchItem::Lagged { .. } | CacheWatchItem::ShardMoved(_)) | None => break,
            }
        }
        tokio::time::sleep(RETRY_DELAY).await;
    }
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
        match lease::claim(&node.felix, &node.config.node_id, &id, timing).await {
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
async fn mark_failed(
    felix: &Felix,
    id: &str,
    mut spec: WorkloadSpec,
    message: &str,
    fence: &Fence,
) {
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
