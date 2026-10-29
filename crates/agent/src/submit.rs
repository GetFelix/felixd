//! `felixd run`: store a module and start a workload.

use anyhow::{Result, anyhow, bail};
use bytes::Bytes;
use felixd_core::spec::{DEFAULT_SNAPSHOT_EVERY, Limits, SpecRecord, WorkloadSpec, WorkloadState};
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
    pub input: String,
    pub snapshot_every: u64,
    pub snapshot_every_ms: Option<u64>,
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
            snapshot_every_ms: None,
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

    let felix = Felix::connect(settings).await?;
    let hash = module::hash(&submission.module);
    let snap_stream = names::snapshot_stream(&submission.id);
    let module = Bytes::from(submission.module);
    felix.cache_put(names::MODULES, &hash, module).await?;
    let mut spec = WorkloadSpec {
        module: hash.clone(),
        input: submission.input,
        output,
        snapshot_every: submission.snapshot_every,
        snapshot_every_ms: submission.snapshot_every_ms,
        state: WorkloadState::Running,
        limits: submission.limits,
        failure: None,
        record: None,
    };
    let written = match specs::written_under(&felix, &submission.id).await? {
        Some(written) => Some(written),
        None if has_records(&felix, &spec.output).await?
            || has_records(&felix, &snap_stream).await? =>
        {
            previous_spec(&felix, &submission.id).await?
        }
        None => None,
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

/// The spec of a workload whose output predates claims that name one: its
/// last record, or for one written before specs were records, its cache
/// value.
async fn previous_spec(felix: &Felix, id: &str) -> Result<Option<WorkloadSpec>> {
    if let Some((_, spec)) = specs::history(felix, id).await?.latest() {
        return Ok(Some(spec.clone()));
    }
    let cached = felix.cache_get(names::WORKLOADS, id).await?;
    Ok(cached.and_then(|c| WorkloadSpec::decode(&c).ok()))
}

/// Whether `stream` holds any record. Its tail alone does not say: a new
/// shard leader's generation-start record takes an offset but is not one.
async fn has_records(felix: &Felix, stream: &str) -> Result<bool> {
    let tail = felix.tail(stream).await?;
    Ok(felix.last_record(stream, tail).await?.is_some())
}
