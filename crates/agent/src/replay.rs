//! `felixd replay`: rebuilds a workload's state as of an input offset or a
//! time, from the nearest snapshot and the inputs after it, or compares the
//! outputs a replay produces with the ones on the stream. Writes nothing.

use anyhow::{Context, Result, anyhow, bail};
use felixd_core::check::Produced;
use felixd_core::names;
use felixd_core::replay::{self, Divergence, Recorded};
use felixd_core::snapshot::SnapshotHeader;
use felixd_core::spec::{Limits, WorkloadSpec};
use felixd_runtime::{Runtime, Workload};

use crate::check::{judged, module_bytes};
use crate::felix::Felix;
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
    /// microseconds since the Unix epoch.
    Time(u64),
}

/// The state a replay rebuilt and how it got there.
#[derive(Debug)]
pub struct Replayed {
    /// Inputs below this offset are in the state.
    pub end: u64,
    /// The input offset of the snapshot it restored, if any.
    pub restored: Option<u64>,
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
    /// How many outputs matched.
    pub matched: u64,
    pub divergence: Option<Divergence>,
}

/// Rebuilds workload `id` as of `to` on a fresh instance under the limits in
/// its spec. With `module`, the hash of another module in the `modules`
/// cache, that module runs instead, from the initial state, since the
/// snapshots hold the recorded module's state.
///
/// It reads the input and snapshot streams' tails, which Felix only reports to
/// a conditional write, so it needs publish grants on them although it writes
/// nothing (Felix #1621).
pub async fn replay(
    settings: &Settings,
    id: &str,
    to: Target,
    module: Option<&str>,
) -> Result<Replayed> {
    let (felix, spec) = open(settings, id).await?;
    let mut feed = Feed::new(&felix, id, &spec);
    let input_tail = felix.tail(feed.stream()).await?;
    let end = end_of(&felix, feed.stream(), Some(to), input_tail).await?;
    let (mut workload, restored) = start(&felix, id, &spec, module, end).await?;
    let mut steps = 0;
    run_inputs(
        &mut feed,
        &mut workload,
        &[],
        replay::first_input(restored),
        end,
        |_, _| {
            steps += 1;
            None::<()>
        },
    )
    .await?;
    let state = workload.inspect().context("inspect")?;
    Ok(Replayed {
        end,
        restored: restored.map(|h| h.input_offset),
        steps,
        state,
    })
}

/// Replays workload `id` over inputs `from` up to `to`, or every input, and
/// compares each output it produces with the one recorded on the output
/// stream, by header and payload digest. It stops at the first divergence,
/// and at the last input with a recorded output, which the workload may not
/// have got past yet. `module` is as for [`replay`].
pub async fn diff(
    settings: &Settings,
    id: &str,
    from: u64,
    to: Option<Target>,
    module: Option<&str>,
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
    let (mut workload, restored) = start(&felix, id, &spec, module, from).await?;
    let (_, limits) = judged(&felix, id, &spec, &records).await?;
    let mut matched = 0;
    let divergence = run_inputs(
        &mut feed,
        &mut workload,
        &limits,
        replay::first_input(restored),
        end,
        |offset, outputs| {
            if offset < from {
                return None;
            }
            let found = recorded.compare(offset, outputs);
            if found.is_none() {
                matched += outputs.len() as u64;
            }
            found
        },
    )
    .await?;
    Ok(Diffed {
        from,
        end,
        matched,
        divergence,
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
/// spec's limits. The recorded module is restored from the nearest snapshot
/// of inputs below `before`, and the snapshot's header returned.
async fn start(
    felix: &Felix,
    id: &str,
    spec: &WorkloadSpec,
    module: Option<&str>,
    before: u64,
) -> Result<(Workload, Option<SnapshotHeader>)> {
    let bytes = module_bytes(felix, module.unwrap_or(&spec.module)).await?;
    let runtime = Runtime::new().map_err(|e| anyhow!("{e}"))?;
    let mut workload = runtime
        .load(&bytes, spec.limits)
        .map_err(|e| anyhow!("{e}"))?;
    if module.is_some_and(|m| m != spec.module) {
        return Ok((workload, None));
    }
    let snap_stream = names::snapshot_stream(id);
    let snap_tail = felix.tail(&snap_stream).await?;
    let records = felix.read_range(&snap_stream, 0, snap_tail).await?;
    let mut found = Vec::with_capacity(records.len());
    for record in &records {
        let snapshot = Snapshot::decode(&record.payload)
            .map_err(|e| anyhow!("snapshot at {snap_stream}:{}: {e}", record.offset))?;
        found.extend(snapshot);
    }
    let headers = found.iter().map(|s| s.taken().header);
    let Some(index) = replay::nearest_snapshot(headers, before) else {
        return Ok((workload, None));
    };
    let snapshot = &found[index];
    let header = snapshot.taken().header;
    let chunks = snapshots::chunks(felix, &snap_stream, snapshot)
        .await
        .map_err(snapshots::ReadError::into_anyhow)?;
    let chunks: Vec<&[u8]> = chunks.iter().map(|c| c.as_ref()).collect();
    workload
        .restore(&chunks)
        .with_context(|| format!("restore the snapshot of input {}", header.input_offset))?;
    Ok((workload, Some(header)))
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
