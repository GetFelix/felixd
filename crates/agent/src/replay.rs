//! `felixd replay`: rebuilds a workload's state as of an input offset or a
//! time, from the nearest snapshot and the inputs after it, or compares the
//! outputs a replay produces with the ones on the stream. Writes nothing.
//! Another module may replay the recorded inputs, from the recorded module's
//! snapshot when it declares it can read it.

use anyhow::{Context, Result, anyhow, bail};
use felixd_core::check::Produced;
use felixd_core::module::{self, ModuleInfo, SectionError, Unreadable};
use felixd_core::names;
use felixd_core::replay::{self, Divergence, Initial, Recorded, Start};
use felixd_core::snapshot::SnapshotHeader;
use felixd_core::spec::{Limits, WorkloadSpec};
use felixd_runtime::{Runtime, Workload};

use crate::check::{judged, module_bytes};
use crate::felix::{Felix, Trimmed};
use crate::input::{self, Feed};
use crate::settings::Settings;
use crate::snapshots::{self, Snapshot};
use crate::specs;

/// Where a replay stops.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    /// The state after the input at this offset.
    Offset(u64),
    /// The state after every input appended before this time, in
    /// microseconds since the Unix epoch. For a workload with several
    /// inputs, after every merge entry appended to the merge log before it.
    Time(u64),
}

/// The state a replay rebuilt and how it got there.
#[derive(Debug)]
pub struct Replayed {
    /// Inputs below this offset are in the state.
    pub end: u64,
    /// Where the state began: a snapshot or the initial state.
    pub start: Start,
    /// How many inputs it ran after the snapshot.
    pub steps: u64,
    /// What the workload's `inspect` returned.
    pub state: String,
}

/// What a diff compared, and the first divergence it found.
#[derive(Debug)]
pub struct Diffed {
    /// Outputs of inputs in `from..end` were compared.
    pub from: u64,
    pub end: u64,
    /// Where the state began: a snapshot or the initial state.
    pub start: Start,
    /// How many outputs matched.
    pub matched: u64,
    pub divergence: Option<Divergence>,
    /// With `all`, every divergence after the first.
    pub more: Vec<Divergence>,
}

/// Rebuilds workload `id` as of `to` on a fresh instance under the limits in
/// its spec. With `module`, the hash of another module in the `modules`
/// cache or the path of a component file, that module runs instead. It
/// starts from the recorded module's nearest snapshot when it declares it can
/// read it, and from the initial state when it does not. With `from_start` no
/// snapshot is restored.
///
/// It reads the input and snapshot streams' tails, which Felix only reports to
/// a conditional write, so it needs publish grants on them although it writes
/// nothing (Felix #1621).
pub async fn replay(
    settings: &Settings,
    id: &str,
    to: Target,
    module: Option<&str>,
    from_start: bool,
) -> Result<Replayed> {
    let (felix, spec) = open(settings, id).await?;
    let mut feed = Feed::new(&felix, id, &spec);
    let input_tail = felix.tail(feed.stream()).await?;
    let end = end_of(&felix, feed.stream(), Some(to), input_tail).await?;
    let (mut workload, start) = start(&felix, id, &spec, module, end, from_start).await?;
    let mut steps = 0;
    run_inputs(
        &mut feed,
        &mut workload,
        &[],
        replay::first_input(start.snapshot()),
        end,
        |_, _| {
            steps += 1;
            None::<()>
        },
    )
    .await
    .map_err(|e| Trimmed::explained(e, "felixd replay"))?;
    let state = workload.inspect().context("inspect")?;
    Ok(Replayed {
        end,
        start,
        steps,
        state,
    })
}

/// Replays workload `id` over inputs `from` up to `to`, or every input, and
/// compares each output it produces with the one recorded on the output
/// stream, by header and payload digest. It stops at the first divergence,
/// and at the last input with a recorded output, which the workload may not
/// have got past yet. `module` and `from_start` are as for [`replay`]. With
/// `all` it carries on past divergences and reports each, which says
/// something for a workload whose outputs depend on their input alone, such
/// as admission: with another module, which recorded requests it decides
/// otherwise.
pub async fn diff(
    settings: &Settings,
    id: &str,
    from: u64,
    to: Option<Target>,
    module: Option<&str>,
    all: bool,
    from_start: bool,
) -> Result<Diffed> {
    let (felix, spec) = open(settings, id).await?;
    // The output stream first, so every input a recorded output came from is
    // on the input stream when its tail is read.
    let output_tail = felix.tail(&spec.output).await?;
    let records = felix.read_range(&spec.output, 0, output_tail).await?;
    let mut feed = Feed::new(&felix, id, &spec);
    let input_tail = felix.tail(feed.stream()).await?;
    let end = end_of(&felix, feed.stream(), to, input_tail).await?;
    let recorded = Recorded::read(records.iter().map(|r| r.payload.as_ref()), from..end)
        .map_err(|e| anyhow!("{} holds a record that does not decode: {e}", spec.output))?;
    let end = end.min(recorded.end());
    let (mut workload, start) = start(&felix, id, &spec, module, from, from_start).await?;
    let (_, limits) = judged(&felix, id, &spec, &records, false).await?;
    let mut matched = 0;
    let mut found_all = Vec::new();
    let first = run_inputs(
        &mut feed,
        &mut workload,
        &limits,
        replay::first_input(start.snapshot()),
        end,
        |offset, outputs| {
            if offset < from {
                return None;
            }
            let found = recorded.compare(offset, outputs);
            if found.is_none() {
                matched += outputs.len() as u64;
            }
            if all {
                found_all.extend(found);
                return None;
            }
            found
        },
    )
    .await
    .map_err(|e| Trimmed::explained(e, "felixd replay"))?;
    let mut found_all = first.into_iter().chain(found_all);
    Ok(Diffed {
        from,
        end,
        start,
        matched,
        divergence: found_all.next(),
        more: found_all.collect(),
    })
}

async fn open(settings: &Settings, id: &str) -> Result<(Felix, WorkloadSpec)> {
    let felix = Felix::connect(settings).await?;
    let spec = felix
        .cache_get(names::WORKLOADS, id)
        .await?
        .context("no such workload")?;
    let spec = specs::lineage(&felix, id, WorkloadSpec::decode(&spec)?).await?;
    Ok((felix, spec))
}

/// The step a replay to `to` stops below, on `stream`, the input or the
/// merge log; every step when `None`.
async fn end_of(felix: &Felix, stream: &str, to: Option<Target>, input_tail: u64) -> Result<u64> {
    match to {
        None => Ok(input_tail),
        Some(Target::Offset(offset)) => {
            if offset >= input_tail {
                bail!("input {offset} is not on {stream}: its next offset is {input_tail}");
            }
            Ok(offset + 1)
        }
        Some(Target::Time(micros)) => Ok(replay::end_for_time(
            felix.offset_for_time(stream, micros).await?,
            input_tail,
        )),
    }
}

/// A fresh instance of the workload's module, or of `module`, under the
/// spec's limits. The nearest snapshot of inputs below `before` is restored
/// into it, unless `from_start` asks for the initial state. Another module
/// restores the recorded module's snapshot only when it declares it can
/// ([`module::can_restore`]). Where the state began is returned.
async fn start(
    felix: &Felix,
    id: &str,
    spec: &WorkloadSpec,
    module: Option<&str>,
    before: u64,
    from_start: bool,
) -> Result<(Workload, Start)> {
    let (hash, bytes) = match module {
        Some(module) => candidate(felix, module).await?,
        None => (
            spec.module.clone(),
            module_bytes(felix, &spec.module).await?,
        ),
    };
    let runtime = Runtime::new().map_err(|e| anyhow!("{e}"))?;
    let mut workload = runtime
        .load(&bytes, spec.limits)
        .map_err(|e| anyhow!("{e}"))?;
    if from_start {
        return Ok((workload, Start::Initial(Initial::Requested)));
    }
    let snap_stream = names::snapshot_stream(id);
    let snap_tail = felix.tail(&snap_stream).await?;
    // From the oldest snapshot Felix holds: `keep_snapshots` deletes the rest.
    let (records, _) = felix.read_back(&snap_stream, 0, snap_tail).await?;
    let found =
        snapshots::restorable(&snap_stream, records.iter().map(|r| (r.offset, &r.payload)))?;
    let headers = found.iter().map(|s| s.taken().header);
    let Some(index) = replay::nearest_snapshot(headers, before) else {
        return Ok((workload, Start::Initial(Initial::NoSnapshot)));
    };
    let swapped = hash != spec.module;
    if swapped && let Some(why) = unreadable(felix, spec, &bytes).await? {
        return Ok((workload, Start::Initial(Initial::Unreadable(why))));
    }
    let header = restore(felix, &snap_stream, &found[index], &mut workload, swapped).await?;
    Ok((workload, Start::Snapshot(header)))
}

/// Restores `snapshot`, read from `stream`, into `workload`, and returns its
/// header. `swapped` says the workload runs another module than the one that
/// took the snapshot, which changes what a failed restore is called.
pub(crate) async fn restore(
    felix: &Felix,
    stream: &str,
    snapshot: &Snapshot,
    workload: &mut Workload,
    swapped: bool,
) -> Result<SnapshotHeader> {
    let header = snapshot.taken().header;
    let chunks = snapshots::chunks(felix, stream, snapshot)
        .await
        .map_err(snapshots::ReadError::into_anyhow)?;
    let chunks: Vec<&[u8]> = chunks.iter().map(|c| c.as_ref()).collect();
    workload.restore(&chunks).map_err(|e| {
        let offset = header.input_offset;
        if swapped {
            anyhow!("snapshot not readable by the new module: restoring the snapshot of input {offset} failed: {e}")
        } else {
            anyhow!(e).context(format!("restore the snapshot of input {offset}"))
        }
    })?;
    Ok(header)
}

/// The module a replay tries, as its hash and bytes. `module` is the hash of
/// one in the `modules` cache, or else the path of a component file, which
/// needs no upload and so changes nothing on Felix.
async fn candidate(felix: &Felix, which: &str) -> Result<(String, Vec<u8>)> {
    if is_hash(which) {
        return Ok((which.to_owned(), module_bytes(felix, which).await?));
    }
    let bytes = std::fs::read(which).with_context(|| {
        format!(
            "read {which}; --module takes the sha256 of a module in the modules cache or the path of a component"
        )
    })?;
    Ok((module::hash(&bytes), bytes))
}

fn unparsed(which: &str, error: SectionError) -> anyhow::Error {
    anyhow!("{which}: {error}; --from-start replays without reading snapshots")
}

fn is_hash(which: &str) -> bool {
    which.len() == 64
        && which
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Why `candidate`, a module's bytes, cannot restore what the module the
/// workload runs snapshotted, or `None` when it can.
async fn unreadable(
    felix: &Felix,
    spec: &WorkloadSpec,
    candidate: &[u8],
) -> Result<Option<Unreadable>> {
    let recorded = module_bytes(felix, &spec.module).await?;
    let recorded = ModuleInfo::of(&recorded)
        .map_err(|e| unparsed(&format!("the workload's module {}", spec.module), e))?;
    let candidate = ModuleInfo::of(candidate).map_err(|e| unparsed("the new module", e))?;
    Ok(module::can_restore(&recorded, &candidate).err())
}

/// Runs the steps at offsets `from..end` of `feed` through `workload`,
/// handing each step's offset and outputs to `each`, and stops at the first
/// step `each` returns something for. `limits` are `(first input, limits)`
/// changes, as [`judged`] gives them.
async fn run_inputs<T>(
    feed: &mut Feed<'_>,
    workload: &mut Workload,
    limits: &[(u64, Limits)],
    from: u64,
    end: u64,
    mut each: impl FnMut(u64, &[Produced]) -> Option<T>,
) -> Result<Option<T>> {
    let mut limits = limits.iter().peekable();
    let mut next = from;
    while next < end {
        let (page, after) = feed.page(next, end).await?;
        for step in page {
            while let Some((_, applied)) = limits.next_if(|(at, _)| *at <= step.at) {
                workload.set_limits(*applied);
            }
            let Some(input) = step.input? else {
                continue;
            };
            let stepped = workload
                .step(input.as_input())
                .with_context(|| format!("input {} fails the workload", step.at))?;
            let outputs = input::produced(stepped);
            if let Some(found) = each(step.at, &outputs) {
                return Ok(Some(found));
            }
        }
        next = after;
    }
    Ok(None)
}
