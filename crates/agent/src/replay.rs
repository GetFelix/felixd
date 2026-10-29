//! `felixd replay`: rebuilds a workload's state as of an input offset or a
//! time, from the nearest snapshot and the inputs after it, and writes
//! nothing.

use anyhow::{Context, Result, anyhow, bail};
use felixd_core::spec::WorkloadSpec;
use felixd_core::{names, output, replay, snapshot};
use felixd_runtime::{Runtime, Workload};

use crate::check::module_bytes;
use crate::felix::Felix;
use crate::settings::Settings;

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

/// Rebuilds workload `id` as of `to` on a fresh instance under the limits in
/// its spec.
///
/// It reads the input and snapshot streams' tails, which Felix only reports to
/// a conditional write, so it needs publish grants on them although it writes
/// nothing (Felix #1621).
pub async fn replay(settings: &Settings, id: &str, to: Target) -> Result<Replayed> {
    let felix = Felix::connect(settings).await?;
    let spec = felix
        .cache_get(names::WORKLOADS, id)
        .await?
        .context("no such workload")?;
    let spec = WorkloadSpec::decode(&spec)?;
    let input_tail = felix.tail(&spec.input).await?;
    let end = match to {
        Target::Offset(offset) => {
            if offset >= input_tail {
                bail!(
                    "input {offset} is not on {}: its next offset is {input_tail}",
                    spec.input
                );
            }
            offset + 1
        }
        Target::Time(micros) => replay::end_for_time(
            felix.offset_for_time(&spec.input, micros).await?,
            input_tail,
        ),
    };

    let snap_stream = names::snapshot_stream(id);
    let snap_tail = felix.tail(&snap_stream).await?;
    let records = felix.read_range(&snap_stream, 0, snap_tail).await?;
    let mut snapshots = Vec::with_capacity(records.len());
    for record in &records {
        let (taken, state) = snapshot::decode(&record.payload)
            .map_err(|e| anyhow!("snapshot at {snap_stream}:{}: {e}", record.offset))?;
        snapshots.push((taken.header, state));
    }
    let nearest = replay::nearest_snapshot(snapshots.iter().map(|(h, _)| *h), end)
        .map(|index| snapshots[index]);

    let bytes = module_bytes(&felix, &spec.module).await?;
    let runtime = Runtime::new().map_err(|e| anyhow!("{e}"))?;
    let mut workload = runtime
        .load(&bytes, spec.limits)
        .map_err(|e| anyhow!("{e}"))?;
    if let Some((header, state)) = nearest {
        workload
            .restore(state)
            .with_context(|| format!("restore the snapshot of input {}", header.input_offset))?;
    }
    let steps = run_inputs(
        &felix,
        &spec,
        &mut workload,
        replay::first_input(nearest.map(|(h, _)| h)),
        end,
    )
    .await?;
    let state = workload.inspect().context("inspect")?;
    Ok(Replayed {
        end,
        restored: nearest.map(|(h, _)| h.input_offset),
        steps,
        state,
    })
}

/// Runs the inputs at offsets `from..end` through `workload` and returns how
/// many it ran.
async fn run_inputs(
    felix: &Felix,
    spec: &WorkloadSpec,
    workload: &mut Workload,
    from: u64,
    end: u64,
) -> Result<u64> {
    let skip_claims = names::is_output_stream(&spec.input);
    let mut steps = 0;
    let mut next = from;
    while next < end {
        let (page, after) = felix.read_page(&spec.input, next, end).await?;
        for input in page {
            if skip_claims && output::is_marker(&input.payload) {
                continue;
            }
            workload
                .step(&input.payload)
                .with_context(|| format!("input {} fails the workload", input.offset))?;
            steps += 1;
        }
        next = after;
    }
    Ok(steps)
}
