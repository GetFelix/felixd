//! `felixd run`: store a module and start a workload.

use anyhow::{Result, bail};
use bytes::Bytes;
use felixd_core::spec::{DEFAULT_SNAPSHOT_EVERY, Limits, WorkloadSpec, WorkloadState};
use felixd_core::{module, names};

use crate::admin::{self, StreamKind};
use crate::felix::Felix;
use crate::settings::Settings;

/// Replicas of felixd's caches and a workload's streams, enough to lose one
/// broker without losing an acknowledged write.
pub const DEFAULT_REPLICATION_FACTOR: u32 = 3;

pub struct Submission {
    pub id: String,
    pub module: Vec<u8>,
    pub input: String,
    pub snapshot_every: u64,
    pub limits: Limits,
    pub replication_factor: u32,
}

impl Submission {
    pub fn new(id: &str, module: Vec<u8>, input: &str) -> Submission {
        Submission {
            id: id.to_owned(),
            module,
            input: input.to_owned(),
            snapshot_every: DEFAULT_SNAPSHOT_EVERY,
            limits: Limits::default(),
            replication_factor: DEFAULT_REPLICATION_FACTOR,
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
    for cache in [
        names::WORKLOADS,
        names::MODULES,
        names::LEASES,
        names::EPOCHS,
        names::NODES,
    ] {
        admin::ensure_cache(settings, cache, kind).await?;
    }
    let output = names::output_stream(&submission.id);
    admin::ensure_stream(settings, &output, kind).await?;
    admin::ensure_stream(settings, &names::snapshot_stream(&submission.id), kind).await?;

    let felix = Felix::connect(settings).await?;
    let hash = module::hash(&submission.module);
    let snap_stream = names::snapshot_stream(&submission.id);
    let module = Bytes::from(submission.module);
    felix.cache_put(names::MODULES, &hash, module).await?;
    let spec = WorkloadSpec {
        module: hash.clone(),
        input: submission.input,
        output,
        snapshot_every: submission.snapshot_every,
        state: WorkloadState::Running,
        limits: submission.limits,
        failure: None,
    };
    if let Some(old) = felix.cache_get(names::WORKLOADS, &submission.id).await?
        && let Ok(old) = WorkloadSpec::decode(&old)
        && let Some(conflict) = old.conflicts_with_history(&spec)
        && (has_records(&felix, &spec.output).await? || has_records(&felix, &snap_stream).await?)
    {
        bail!(
            "workload {} already has output, and {conflict}; submit the change under a new id",
            submission.id
        );
    }
    let spec = Bytes::from(spec.encode());
    felix
        .cache_put(names::WORKLOADS, &submission.id, spec)
        .await?;
    Ok(hash)
}

/// Whether `stream` holds any record. Its tail alone does not say: a new
/// shard leader's generation-start record takes an offset but is not one.
async fn has_records(felix: &Felix, stream: &str) -> Result<bool> {
    let tail = felix.tail(stream).await?;
    Ok(felix.last_record(stream, tail).await?.is_some())
}
