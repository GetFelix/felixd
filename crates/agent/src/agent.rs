//! The node loop. It watches the `workloads` cache and runs every workload
//! whose state is `running`.
//!
//! There is no ownership yet: this agent runs all of them, so run one agent
//! per cluster until leases exist.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use felix_client::CacheWatchItem;
use felixd_core::names;
use felixd_core::spec::{WorkloadSpec, WorkloadState};
use felixd_runtime::Runtime;
use tokio::task::JoinHandle;

use crate::felix::Felix;
use crate::runner::{Outcome, run_workload};
use crate::settings::Settings;

const RETRY_DELAY: Duration = Duration::from_secs(1);

/// Runs until `shutdown` completes.
pub async fn run(settings: &Settings, shutdown: impl Future<Output = ()>) -> Result<()> {
    let felix = Arc::new(Felix::connect(settings).await?);
    let runtime = Arc::new(Runtime::new().map_err(|e| anyhow::anyhow!("{e}"))?);
    let mut running: HashMap<String, JoinHandle<()>> = HashMap::new();
    tokio::pin!(shutdown);
    loop {
        let mut watch = felix.watch_retained(names::WORKLOADS).await?;
        loop {
            let item = tokio::select! {
                () = &mut shutdown => {
                    for task in running.values() {
                        task.abort();
                    }
                    return Ok(());
                }
                item = watch.recv() => item,
            };
            match item {
                Some(CacheWatchItem::Change(change)) => {
                    apply(&felix, &runtime, &mut running, change.key, change.value);
                }
                // The watch ended; open a new one, which delivers every
                // workload again.
                Some(CacheWatchItem::Lagged { .. } | CacheWatchItem::ShardMoved(_)) | None => break,
            }
        }
        tokio::time::sleep(RETRY_DELAY).await;
    }
}

fn apply(
    felix: &Arc<Felix>,
    runtime: &Arc<Runtime>,
    running: &mut HashMap<String, JoinHandle<()>>,
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
    if let Some(task) = running.get(&id) {
        if !task.is_finished() && wanted.is_some() {
            return;
        }
        task.abort();
        running.remove(&id);
    }
    if let Some(spec) = wanted {
        let task = tokio::spawn(supervise(felix.clone(), runtime.clone(), id.clone(), spec));
        running.insert(id, task);
    }
}

/// Keeps one workload running: a Felix error is retried from the streams, and a
/// failure no retry can fix is recorded in its spec.
async fn supervise(felix: Arc<Felix>, runtime: Arc<Runtime>, id: String, spec: WorkloadSpec) {
    loop {
        match run_workload(&felix, &runtime, &id, &spec).await {
            Ok(Outcome::Failed(message)) => {
                tracing::error!(workload = %id, %message, "workload failed");
                mark_failed(&felix, &id, spec, message).await;
                return;
            }
            Err(error) => {
                tracing::warn!(workload = %id, error = %format!("{error:#}"), "retrying");
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    }
}

async fn mark_failed(felix: &Felix, id: &str, mut spec: WorkloadSpec, message: String) {
    spec.state = WorkloadState::Failed;
    spec.failure = Some(message.lines().next().unwrap_or_default().to_owned());
    let value = Bytes::from(spec.encode());
    loop {
        match felix.cache_put(names::WORKLOADS, id, value.clone()).await {
            Ok(()) => return,
            Err(error) => {
                tracing::warn!(workload = %id, %error, "could not record the failure, retrying");
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    }
}
