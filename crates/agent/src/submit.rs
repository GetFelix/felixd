//! `felixd run`: store a module and start a workload.

use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use bytes::Bytes;
use felixd_core::spec::{DEFAULT_SNAPSHOT_EVERY, WorkloadSpec, WorkloadState};
use felixd_core::{module, names};

use crate::admin::{self, StreamKind};
use crate::felix::Felix;
use crate::settings::Settings;

const VISIBLE_WITHIN: Duration = Duration::from_secs(15);

pub struct Submission {
    pub id: String,
    pub module: Vec<u8>,
    pub input: String,
    pub snapshot_every: u64,
    pub replication_factor: u32,
}

impl Submission {
    pub fn new(id: &str, module: Vec<u8>, input: &str) -> Submission {
        Submission {
            id: id.to_owned(),
            module,
            input: input.to_owned(),
            snapshot_every: DEFAULT_SNAPSHOT_EVERY,
            replication_factor: 1,
        }
    }
}

/// Creates the caches and the workload's streams, uploads the module and
/// writes the spec last, so an agent that sees the spec finds everything else.
/// Returns the module's hash.
pub async fn submit(settings: &Settings, submission: Submission) -> Result<String> {
    names::validate_id(&submission.id)?;
    if submission.snapshot_every == 0 {
        bail!("snapshot_every must be at least 1");
    }
    let kind = StreamKind {
        replication_factor: submission.replication_factor,
    };
    for cache in [names::WORKLOADS, names::MODULES] {
        admin::ensure_cache(settings, cache).await?;
    }
    let output = names::output_stream(&submission.id);
    admin::ensure_stream(settings, &output, kind).await?;
    admin::ensure_stream(settings, &names::snapshot_stream(&submission.id), kind).await?;

    let felix = Felix::connect(settings).await?;
    let hash = module::hash(&submission.module);
    let module = Bytes::from(submission.module);
    put_when_visible(&felix, names::MODULES, &hash, module).await?;
    let spec = WorkloadSpec {
        module: hash.clone(),
        input: submission.input,
        output,
        snapshot_every: submission.snapshot_every,
        state: WorkloadState::Running,
        failure: None,
    };
    let spec = Bytes::from(spec.encode());
    put_when_visible(&felix, names::WORKLOADS, &submission.id, spec).await?;
    Ok(hash)
}

/// A cache the control plane has just created is not known to the broker for a
/// moment, and writes to it are refused as not found until it is.
async fn put_when_visible(felix: &Felix, cache: &str, key: &str, value: Bytes) -> Result<()> {
    let deadline = Instant::now() + VISIBLE_WITHIN;
    loop {
        match felix.cache_put(cache, key, value.clone()).await {
            Err(error)
                if format!("{error:#}").contains("not found") && Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            result => return result,
        }
    }
}
