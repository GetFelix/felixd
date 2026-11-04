//! `felixd run`: store a module and start a workload.

use anyhow::{Result, anyhow, bail};
use felix_client::ConditionalWrite;
use felixd_core::module::ModuleInfo;
use felixd_core::snapshot::{self, Pending};
use felixd_core::spec::{
    DEFAULT_SNAPSHOT_EVERY, Limits, RestartIntensity, SpecRecord, Upgrade, WorkloadSpec,
    WorkloadState, validate_inputs,
};
use felixd_core::{module, names};
use felixd_runtime::Runtime;

use crate::admin::{self, StreamKind};
use crate::faults;
use crate::felix::Felix;
use crate::module_store;
use crate::runner::last_output;
use crate::settings::Settings;
use crate::snapshots;
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
    /// How many takeovers in a row without progress the spec allows.
    pub restart_intensity: Option<RestartIntensity>,
    pub limits: Limits,
    pub replication_factor: u32,
    /// Replace the workload's module with `module` from this input on, and
    /// keep running its earlier inputs under the old one.
    pub upgrade_at: Option<u64>,
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
            restart_intensity: None,
            limits: Limits::default(),
            replication_factor: DEFAULT_REPLICATION_FACTOR,
            upgrade_at: None,
        }
    }
}

/// Checks the module is a component no node would refuse, then creates the
/// caches and streams, uploads the module, appends the spec record to
/// `sys.specs` and writes it into `workloads` last, so an agent that sees the
/// spec finds everything else. Returns the module's hash.
pub async fn submit(settings: &Settings, submission: Submission) -> Result<String> {
    names::validate_id(&submission.id)?;
    validate_inputs(&submission.id, &submission.inputs)?;
    if let Some(why) =
        module::size_problem(submission.module.len() as u64, module::MAX_MODULE_BYTES)
    {
        bail!(why);
    }
    Runtime::new()
        .map_err(|e| anyhow!("{e}"))?
        .compile(&submission.module)
        .map_err(|e| anyhow!("{e}"))?;
    if submission.snapshot_every == 0 {
        bail!("snapshot_every must be at least 1");
    }
    if submission.keep_snapshots == Some(0) {
        bail!("keep_snapshots must be at least 1");
    }
    if submission.snapshot_every_ms == Some(0) {
        bail!("snapshot_every_ms must be at least 1");
    }
    if submission
        .restart_intensity
        .is_some_and(|r| r.window_ms == 0)
    {
        bail!("the restart intensity window must be at least 1 ms");
    }
    let kind = StreamKind {
        replication_factor: submission.replication_factor,
    };
    admin::ensure_namespace(settings).await?;
    for cache in [
        names::WORKLOADS,
        names::MODULES,
        names::LEASES,
        names::EPOCHS,
        names::NODES,
        names::DRAINS,
    ] {
        admin::ensure_cache(settings, cache, kind).await?;
    }
    admin::ensure_stream(settings, names::SPECS, kind).await?;
    // The scheduler's other input. Nodes append to it only while one runs.
    admin::ensure_stream(settings, names::FACTS, kind).await?;
    admin::ensure_stream(settings, names::INTAKE, kind).await?;
    let output = names::output_stream(&submission.id);
    admin::ensure_stream(settings, &output, kind).await?;
    admin::ensure_stream(settings, &names::snapshot_stream(&submission.id), kind).await?;
    if submission.inputs.len() > 1 {
        admin::ensure_stream(settings, &names::merge_stream(&submission.id), kind).await?;
    }

    let felix = Felix::connect(settings).await?;
    let hash = module::hash(&submission.module);
    let snap_stream = names::snapshot_stream(&submission.id);
    module_store::put(&felix, &submission.module).await?;
    let previous = previous_spec(&felix, &submission.id).await?;
    let (module, upgrades, checked) =
        modules_for(&felix, &submission, &hash, previous.as_ref()).await?;
    let mut spec = WorkloadSpec {
        module,
        inputs: submission.inputs,
        output,
        snapshot_every: submission.snapshot_every,
        snapshot_every_ms: submission.snapshot_every_ms,
        keep_snapshots: submission.keep_snapshots,
        restart_intensity: submission.restart_intensity,
        upgrades,
        state: WorkloadState::Running,
        limits: submission.limits,
        failure: None,
        record: None,
    };
    // A new workload has nothing to conflict with, and its streams, just
    // created, may refuse reads until their leader has committed.
    let written = match previous {
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
    if let Some(checked) = &checked {
        record_upgrade(&felix, checked).await?;
        if faults::stop_after_upgrade_record() {
            bail!("injected: stopped before the spec record");
        }
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

/// The module a submission starts with and the upgrades after it. An upgrade
/// is a module that takes over at an input the workload has not run yet, and
/// the new module must be able to restore the state of the one before it.
/// Without `upgrade_at`, a workload with upgrades keeps them, and submitting
/// the module it runs now is a change of limits, not of module.
async fn modules_for(
    felix: &Felix,
    submission: &Submission,
    hash: &str,
    previous: Option<&WorkloadSpec>,
) -> Result<(String, Vec<Upgrade>, Option<Checked>)> {
    let id = &submission.id;
    let Some(at) = submission.upgrade_at else {
        return Ok(match previous {
            Some(previous)
                if !previous.upgrades.is_empty() && hash == previous.module_at(u64::MAX) =>
            {
                (previous.module.clone(), previous.upgrades.clone(), None)
            }
            _ => (hash.to_owned(), Vec::new(), None),
        });
    };
    let Some(previous) = previous else {
        bail!("workload {id} does not exist, so there is no module to upgrade");
    };
    let running = previous.module_at(u64::MAX);
    if running == hash {
        bail!("workload {id} already runs module {hash}");
    }
    let (output, snapshots) = (&previous.output, names::snapshot_stream(id));
    let mut stepped = last_output(felix, output, felix.tail(output).await?)
        .await?
        .map(|(_, last)| last.header.input_offset);
    let snap_tail = felix.tail(&snapshots).await?;
    let snapshot = snapshots::latest(felix, &snapshots, snap_tail)
        .await
        .map_err(snapshots::ReadError::into_anyhow)?
        .snapshot
        .map(|s| s.taken().header.input_offset);
    stepped = stepped.max(snapshot);
    let mut upgrades = previous.upgrades.clone();
    upgrades.push(Upgrade {
        at,
        module: hash.to_owned(),
    });
    let next = WorkloadSpec {
        upgrades,
        ..previous.clone()
    };
    if at == 0 {
        bail!("a module cannot take over at input 0; submit the workload under a new id");
    }
    if let Some(last) = previous.upgrades.last()
        && at <= last.at
    {
        bail!(
            "the module of input {} is already recorded; an upgrade must come after it",
            last.at
        );
    }
    if let Some(conflict) = next.upgrade_conflict(previous, stepped) {
        bail!("workload {id}: {conflict}; choose a later input");
    }
    let old = module_info(felix, running).await?;
    let new = ModuleInfo::of(&submission.module).map_err(|e| anyhow!("the new module: {e}"))?;
    if let Err(why) = module::can_restore(&old, &new) {
        bail!("the new module cannot take over the state of {running}: {why}");
    }
    let checked = Checked {
        snapshots,
        snap_tail,
        at,
    };
    Ok((previous.module.clone(), next.upgrades, Some(checked)))
}

/// An upgrade checked against the snapshot stream up to `snap_tail`.
struct Checked {
    snapshots: String,
    snap_tail: u64,
    at: u64,
}

/// Appends the upgrade record at the tail the check read, before the spec
/// naming the upgrade. Landing there proves no snapshot was written since the
/// check, and an owner still on the old spec writes none after it, so none
/// can be taken past `at` by the old module. Refused, something landed
/// in between and the check must run again.
async fn record_upgrade(felix: &Felix, checked: &Checked) -> Result<()> {
    let pending = Pending {
        at: checked.at,
        specs_from: felix.tail(names::SPECS).await?,
    };
    let record = snapshot::encode_upgrade(&pending);
    match felix
        .publish_if(&checked.snapshots, vec![record], checked.snap_tail)
        .await?
    {
        ConditionalWrite::Written { .. } => Ok(()),
        ConditionalWrite::Refused { .. } => bail!(
            "a snapshot was written while the upgrade was checked; run it again to check \
             input {} against it",
            checked.at
        ),
    }
}

async fn module_info(felix: &Felix, hash: &str) -> Result<ModuleInfo> {
    let bytes = crate::check::module_bytes(felix, hash).await?;
    ModuleInfo::of(&bytes).map_err(|e| anyhow!("module {hash}: {e}"))
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
