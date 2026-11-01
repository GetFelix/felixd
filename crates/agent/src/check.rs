//! `felixd check`: replays each workload's module over its input and reports
//! what is wrong with its output stream. Needs read grants only.

use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use felixd_core::check::{self, Expected, Finding};
use felixd_core::output::{self, OutputHeader};
use felixd_core::replay::first_input;
use felixd_core::snapshot::SnapshotHeader;
use felixd_core::trim::Trim;

use crate::input::{self, Feed};
use felix_client::StreamRecord;
use felixd_core::spec::{Limits, WorkloadSpec};
use felixd_core::{module, names};
use felixd_runtime::{Runtime, Workload};

use crate::felix::{Felix, Trimmed};
use crate::settings::Settings;
use crate::snapshots::Snapshot;
use crate::specs;

/// What the check found for one workload.
#[derive(Debug)]
pub struct Report {
    pub workload: String,
    pub output: String,
    /// The streams the outputs were checked against.
    pub inputs: Vec<String>,
    /// Output records on the stream, claims not counted.
    pub records: usize,
    /// The input of the snapshot the check restored and started after, or
    /// `None` when it started at the first input.
    pub from_snapshot: Option<u64>,
    pub findings: Vec<Finding>,
}

/// Where a check starts replaying.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Origin {
    /// At the first input, unless Felix has deleted the head of the output
    /// stream, when the oldest snapshot the last trim record keeps.
    #[default]
    Auto,
    /// At the first input; fails once Felix has deleted history the replay
    /// needs.
    Start,
    /// After the oldest snapshot the last trim record keeps. The snapshot's
    /// state is taken as given, and outputs below its tail are not compared.
    Snapshot,
}

/// Checks the workloads in `ids`, or every workload in the `workloads` cache
/// when it is empty, a pipeline's stages upstream first
/// ([`check::upstream_first`]). `complete` makes outputs missing from the end
/// of a stream a finding. `upstream` also checks every workload whose output
/// stream one of them reads, and so on up the pipeline.
pub async fn check(
    settings: &Settings,
    ids: &[String],
    complete: bool,
    upstream: bool,
    origin: Origin,
) -> Result<Vec<Report>> {
    let felix = Felix::connect(settings).await?;
    let runtime = Runtime::new().map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut ids = if ids.is_empty() {
        workload_ids(&felix).await?
    } else {
        ids.to_vec()
    };
    let mut inputs = Vec::with_capacity(ids.len());
    let mut at = 0;
    while at < ids.len() {
        let id = ids[at].clone();
        let read = inputs_of(&felix, &id)
            .await
            .with_context(|| format!("check workload {id}"))?;
        for input in &read {
            if upstream
                && let Some(up) = names::output_of(input)
                && !ids.iter().any(|i| i == up)
                && felix.cache_get(names::WORKLOADS, up).await?.is_some()
            {
                ids.push(up.to_owned());
            }
        }
        inputs.push((id, read));
        at += 1;
    }
    let mut reports = Vec::new();
    for id in check::upstream_first(&inputs) {
        let report = check_one(&felix, &runtime, &id, complete, origin)
            .await
            .with_context(|| format!("check workload {id}"))?;
        reports.push(report);
    }
    Ok(reports)
}

async fn check_one(
    felix: &Felix,
    runtime: &Runtime,
    id: &str,
    complete: bool,
    origin: Origin,
) -> Result<Report> {
    let spec = felix
        .cache_get(names::WORKLOADS, id)
        .await?
        .context("no such workload")?;
    let cached = WorkloadSpec::decode(&spec)?;

    // Snapshots first, so every output a snapshot counts is on the stream when
    // it is read. Then the output: anything written after this is checked
    // against inputs that are all on the input stream already. With several
    // inputs the merge log comes next, and then the inputs, so every record
    // it names is on its input when that is read.
    let snap_stream = names::snapshot_stream(id);
    let snap_tail = felix.tail(&snap_stream).await?;
    let output_tail = felix.tail(&cached.output).await?;
    let held = read_held(felix, &cached.output, output_tail).await?;
    // The last trim names the snapshot a check may start from. A snapshot is
    // taken as given, so it is used only when asked for or when the head of
    // the stream is gone and the first input cannot be replayed from.
    let last_trim = held
        .records
        .iter()
        .rev()
        .find_map(|r| output::decode(&r.payload).ok()?.trim());
    let trim = last_trim.filter(|_| starts_at_snapshot(origin, held.cut));
    if trim.is_none() && origin == Origin::Snapshot {
        bail!("{} holds no trim record to start from", cached.output);
    }
    let cut = held.cut;
    let records = held.records;
    let mut trimmed = held.trimmed;
    if trim.is_none()
        && let Some(error) = trimmed.take()
    {
        // Reading from the start, so a head Felix has deleted is an error
        // that names the stream and the oldest offset it keeps.
        return Err(error);
    }
    // The module, input and limits the stream was written under, not those
    // of a later spec its owner refused.
    let (spec, limits) = judged(felix, id, &cached, &records, cut).await?;
    let mut limits = limits.into_iter().peekable();
    let bytes = module_bytes(felix, &spec.module).await?;
    let mut workload = runtime
        .load(&bytes, spec.limits)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let snapshots = felix
        .read_range(
            &snap_stream,
            trim.as_ref().map_or(0, |t| t.snap_floor),
            snap_tail,
        )
        .await?;
    let start = match &trim {
        Some(trim) => {
            Some(restore_oldest_kept(felix, &snap_stream, &snapshots, trim, &mut workload).await?)
        }
        None => None,
    };
    if let (Some(error), Some(header)) = (&trimmed, &start) {
        covered(error, header)?;
    }
    let first = first_input(start);
    let mut feed = Feed::new(felix, id, &spec);
    let mut findings = Vec::new();
    if spec.merged() {
        findings.extend(
            check_merge(
                felix,
                feed.stream(),
                &spec.inputs,
                complete,
                start.map(|h| h.input_offset),
            )
            .await?,
        );
    }
    let input_tail = felix.tail(feed.stream()).await?;
    let mut expected = Vec::new();
    let mut next = first;
    'steps: while next < input_tail {
        let (page, after) = feed.page(next, input_tail).await?;
        for step in page {
            while let Some((_, applied)) = limits.next_if(|(from, _)| *from <= step.at) {
                workload.set_limits(applied);
            }
            // An input that cannot be read or a step that fails ends the
            // workload there, on every node.
            let Ok(read) = step.input else {
                break 'steps;
            };
            let Some(input) = read else {
                continue;
            };
            let Ok(stepped) = workload.step(input.as_input()) else {
                break 'steps;
            };
            for (index, produced) in input::produced(stepped).into_iter().enumerate() {
                let header = OutputHeader {
                    input_offset: step.at,
                    index: u32::try_from(index)?,
                };
                expected.push(Expected { header, produced });
            }
        }
        next = after;
    }
    let on_stream: Vec<(u64, &[u8])> = records
        .iter()
        .map(|r| (r.offset, r.payload.as_ref()))
        .collect();
    // The snapshot covers every output below its tail.
    let checked =
        on_stream.partition_point(|&(offset, _)| offset < start.map_or(0, |h| h.output_tail));
    let epoch_before = on_stream[..checked]
        .iter()
        .filter_map(|(_, payload)| Some(output::decode(payload).ok()?.epoch()))
        .max();
    findings.extend(check::check_after(
        &expected,
        &on_stream[checked..],
        complete,
        epoch_before,
    ));
    let snapshots: Vec<(u64, &[u8])> = snapshots
        .iter()
        .map(|r| (r.offset, r.payload.as_ref()))
        .collect();
    findings.extend(check::check_snapshots(&snapshots, &on_stream, output_tail));
    Ok(Report {
        workload: id.to_owned(),
        output: spec.output.clone(),
        inputs: spec.inputs.clone(),
        records: on_stream[checked..]
            .iter()
            .filter(|(_, payload)| !output::is_marker(payload))
            .count(),
        from_snapshot: start.map(|h| h.input_offset),
        findings,
    })
}

/// Whether a check starts after a snapshot, given that Felix has deleted the
/// head of the output stream or not (`cut`).
fn starts_at_snapshot(origin: Origin, cut: bool) -> bool {
    match origin {
        Origin::Start => false,
        Origin::Snapshot => true,
        Origin::Auto => cut,
    }
}

/// Fails with `trimmed`, the refused read of the output stream's head, when
/// Felix has deleted records the snapshot's output tail says are still to be
/// compared. The outputs missing from the start of what it keeps would
/// otherwise show up as findings.
fn covered(trimmed: &anyhow::Error, snapshot: &SnapshotHeader) -> Result<()> {
    match Trimmed::of(trimmed) {
        Some(t) if t.oldest > snapshot.output_tail => Err(Trimmed {
            stream: t.stream.clone(),
            requested: t.requested,
            oldest: t.oldest,
        }
        .into()),
        _ => Ok(()),
    }
}

/// The records Felix holds on an output stream.
struct Held {
    records: Vec<StreamRecord>,
    /// Felix had deleted the head, and `records` start at the oldest it
    /// keeps. The read it refused, as an error.
    trimmed: Option<anyhow::Error>,
    cut: bool,
}

/// Reads `stream` up to `tail`, from its oldest record when Felix has
/// deleted the head, which `Held::trimmed` then says.
async fn read_held(felix: &Felix, stream: &str, tail: u64) -> Result<Held> {
    match felix.read_range(stream, 0, tail).await {
        Ok(records) => Ok(Held {
            records,
            trimmed: None,
            cut: false,
        }),
        Err(error) => match Trimmed::of(&error).map(|t| t.oldest) {
            Some(oldest) => Ok(Held {
                records: felix.read_range(stream, oldest.min(tail), tail).await?,
                trimmed: Some(error),
                cut: true,
            }),
            None => Err(error),
        },
    }
}

/// Restores the snapshot `trim` names, the oldest it keeps, from
/// `snapshots`, the records of the snapshot stream from the trim's floor.
async fn restore_oldest_kept(
    felix: &Felix,
    stream: &str,
    snapshots: &[StreamRecord],
    trim: &Trim,
    workload: &mut Workload,
) -> Result<SnapshotHeader> {
    let record = snapshots
        .iter()
        .find(|r| r.offset == trim.snapshot)
        .with_context(|| {
            format!(
                "the last trim names {stream}:{}, which holds no record",
                trim.snapshot
            )
        })?;
    let snapshot = Snapshot::decode(&record.payload)
        .map_err(|e| anyhow::anyhow!("snapshot at {stream}:{}: {e}", record.offset))?
        .with_context(|| {
            format!(
                "the last trim names {stream}:{}, which is not a snapshot",
                trim.snapshot
            )
        })?;
    if snapshot.taken().header.input_offset != trim.step {
        bail!(
            "the last trim says {stream}:{} is of input {}, and it is of input {}",
            trim.snapshot,
            trim.step,
            snapshot.taken().header.input_offset
        );
    }
    crate::replay::restore(felix, stream, &snapshot, workload).await
}

/// The spec workload `id`'s output stream was written under and the limits
/// each input ran under, from the spec records its claims name
/// ([`check::judged_from`]). `cached` is the `workloads` value, and `cut` says
/// `records` start after a head Felix deleted.
pub(crate) async fn judged(
    felix: &Felix,
    id: &str,
    cached: &WorkloadSpec,
    records: &[StreamRecord],
    cut: bool,
) -> Result<(WorkloadSpec, Vec<(u64, Limits)>)> {
    let payloads = records.iter().map(|r| r.payload.as_ref());
    let lineage = check::lineage_of(payloads.clone(), cut);
    let mut named = check::specs_claimed(payloads.clone());
    named.extend(lineage.filter(|spec| !named.contains(spec)));
    let mut claimed = HashMap::new();
    for offset in named {
        claimed.insert(offset, specs::read(felix, id, offset).await?);
    }
    Ok(check::judged_from(cached, &claimed, lineage, payloads))
}

/// Checks the merge log `stream` of a workload reading `inputs`
/// ([`check::check_merge_from`]). The log is read before the inputs. With
/// `after`, the log is checked from the entry after that step.
async fn check_merge(
    felix: &Felix,
    stream: &str,
    inputs: &[String],
    complete: bool,
    after: Option<u64>,
) -> Result<Vec<Finding>> {
    let tail = felix.tail(stream).await?;
    let mut log = felix.read_range(stream, after.unwrap_or(0), tail).await?;
    let start = match after {
        Some(step) => {
            let at_step = log.first().filter(|r| r.offset == step);
            let start = at_step.and_then(|r| check::MergeStart::of(&r.payload));
            let start = start.with_context(|| {
                format!("{stream}:{step}, a snapshot's step, is not a merge record")
            })?;
            log.remove(0);
            Some(start)
        }
        None => None,
    };
    let mut offsets = Vec::with_capacity(inputs.len());
    for (i, input) in inputs.iter().enumerate() {
        let tail = felix.read_tail(input).await?;
        let from = start
            .as_ref()
            .and_then(|s| s.next.get(i))
            .copied()
            .unwrap_or(0);
        let records = felix.read_range(input, from.min(tail), tail).await?;
        let found = records.iter().map(|r| (r.offset, r.timestamp_micros));
        offsets.push((input.as_str(), found.collect()));
    }
    let log: Vec<(u64, &[u8])> = log.iter().map(|r| (r.offset, r.payload.as_ref())).collect();
    Ok(check::check_merge_from(
        &log,
        &offsets,
        complete,
        start.as_ref(),
    ))
}

/// The inputs workload `id`'s output stream was written under, from the same
/// spec [`check_one`] judges it under.
async fn inputs_of(felix: &Felix, id: &str) -> Result<Vec<String>> {
    let cached = felix
        .cache_get(names::WORKLOADS, id)
        .await?
        .context("no such workload")?;
    let cached = WorkloadSpec::decode(&cached)?;
    let tail = felix.tail(&cached.output).await?;
    let held = read_held(felix, &cached.output, tail).await?;
    let (spec, _) = judged(felix, id, &cached, &held.records, held.cut).await?;
    Ok(spec.inputs)
}

/// The module `hash` from the `modules` cache.
pub(crate) async fn module_bytes(felix: &Felix, hash: &str) -> Result<Vec<u8>> {
    let bytes = felix
        .cache_get(names::MODULES, hash)
        .await?
        .with_context(|| format!("module {hash} is not in the modules cache"))?;
    if module::hash(&bytes) != hash {
        bail!("module {hash} does not match its hash");
    }
    Ok(bytes.to_vec())
}

/// Every key in the `workloads` cache. Felix has no call that lists a
/// cache's keys, so this reads the retained values of a new watch.
async fn workload_ids(felix: &Felix) -> Result<Vec<String>> {
    let entries = felix.entries(names::WORKLOADS).await?;
    Ok(entries.into_iter().map(|(id, _)| id).collect())
}

#[cfg(test)]
mod tests;
