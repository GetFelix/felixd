//! `felixd run`: store a module and start a workload.

use anyhow::{Result, anyhow, bail};
use bytes::Bytes;
use felixd_core::spec::{
    DEFAULT_SNAPSHOT_EVERY, Limits, SpecRecord, WorkloadSpec, WorkloadState, validate_inputs,
};
use felixd_core::{module, names};
use felixd_runtime::Runtime;

use crate::admin::{self, StreamKind};
use crate::felix::Felix;
use crate::settings::Settings;
use crate::specs;

/// Replicas of felixd's caches and a workload's streams, enough to lose one
/// broker without losing an acknowledged write.
pub const DEFAULT_REPLICATION_FACTOR: u32 = 3;

pub struct Submission {
    pub id: String,
    pub module: Vec<u8>,
    /// One stream, or several merged through a merge log.
    pub inputs: Vec<String>,
    pub snapshot_every: u64,
    pub snapshot_every_ms: Option<u64>,
    pub keep_snapshots: Option<u32>,
    pub limits: Limits,
    pub replication_factor: u32,
}

impl Submission {
    pub fn new(id: &str, module: Vec<u8>, input: &str) -> Submission {
        Submission {
            id: id.to_owned(),
            module,
            inputs: vec![input.to_owned()],
            snapshot_every: DEFAULT_SNAPSHOT_EVERY,
            snapshot_every_ms: None,
            keep_snapshots: None,
            limits: Limits::default(),
            replication_factor: DEFAULT_REPLICATION_FACTOR,
        }
    }
}

/// Checks the module is a component no node would refuse, then creates the
/// caches and streams, uploads the module, appends the spec record to
/// `sys.specs` and writes it into `workloads` last, so an agent that sees the
/// spec finds everything else. Returns the module's hash.
pub async fn submit(settings: &Settings, submission: Submission) -> Result<String> {
    names::validate_id(&submission.id)?;
    validate_inputs(&submission.inputs)?;
    Runtime::new()
        .map_err(|e| anyhow!("{e}"))?
        .compile(&submission.module)
        .map_err(|e| anyhow!("{e}"))?;
    if submission.snapshot_every == 0 {
        bail!("snapshot_every must be at least 1");
    }
    if submission.snapshot_every_ms == Some(0) {
        bail!("snapshot_every_ms must be at least 1");
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
    admin::ensure_stream(settings, names::SPECS, kind).await?;
    let output = names::output_stream(&submission.id);
    admin::ensure_stream(settings, &output, kind).await?;
    admin::ensure_stream(settings, &names::snapshot_stream(&submission.id), kind).await?;
    if submission.inputs.len() > 1 {
        admin::ensure_stream(settings, &names::merge_stream(&submission.id), kind).await?;
    }

    let felix = Felix::connect(settings).await?;
    let hash = module::hash(&submission.module);
    let snap_stream = names::snapshot_stream(&submission.id);
    let module = Bytes::from(submission.module);
    felix.cache_put(names::MODULES, &hash, module).await?;
    let mut spec = WorkloadSpec {
        module: hash.clone(),
        inputs: submission.inputs,
        output,
        snapshot_every: submission.snapshot_every,
        snapshot_every_ms: submission.snapshot_every_ms,
        keep_snapshots: submission.keep_snapshots,
        state: WorkloadState::Running,
        limits: submission.limits,
        failure: None,
        record: None,
    };
    // A new workload has nothing to conflict with, and its streams, just
    // created, may refuse reads until their leader has committed.
    let written = match previous_spec(&felix, &submission.id).await? {
        None => None,
        Some(previous) => match specs::written_under(&felix, &submission.id).await? {
            Some(written) => Some(written),
            None if specs::first_run_record(&felix, &spec.output)
                .await?
                .is_some()
                || has_records(&felix, &snap_stream).await? =>
            {
                Some(previous)
            }
            None => None,
        },
    };
    if let Some(conflict) = written.and_then(|w| w.conflicts_with_history(&spec)) {
        bail!(
            "workload {} already has output, and {conflict}; submit the change under a new id",
            submission.id
        );
    }
    let record = SpecRecord::new(&submission.id, spec.clone());
    let ours = specs::append(&felix, &record).await?;
    spec.record = Some(ours);
    if let Some(current) = specs::project(&felix, &submission.id, &spec).await? {
        bail!(
            "workload {}'s workloads entry names spec record {current}, not before this one at \
             {}:{ours}, so nothing changed; another submit won, or {} was recreated",
            submission.id,
            names::SPECS,
            names::SPECS
        );
    }
    Ok(hash)
}

/// The spec the `workloads` entry projects. Not the last record on
/// `sys.specs`, which may be one that lost to a later one and never ran. The
/// entry carries the spec, and agents run it only when its record agrees.
/// `None` for a workload with no entry, which no agent has run.
async fn previous_spec(felix: &Felix, id: &str) -> Result<Option<WorkloadSpec>> {
    let cached = felix.cache_get(names::WORKLOADS, id).await?;
    Ok(cached.and_then(|c| WorkloadSpec::decode(&c).ok()))
}

/// Whether `stream` holds any record. Its tail alone does not say: a new
/// shard leader's generation-start record takes an offset but is not one.
async fn has_records(felix: &Felix, stream: &str) -> Result<bool> {
    let tail = felix.tail(stream).await?;
    Ok(felix.last_record(stream, tail).await?.is_some())
}
