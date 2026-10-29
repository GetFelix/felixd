//! `felixd check`: replays each workload's module over its input and reports
//! what is wrong with its output stream. Needs read grants only.

use std::collections::HashMap;

use anyhow::{Context, Result, bail};
use felix_client::CacheWatchItem;
use felixd_core::check::{self, Expected, Finding};
use felixd_core::output::{self, OutputHeader};

use crate::input::{self, StepInput};
use felix_client::StreamRecord;
use felixd_core::spec::{Limits, WorkloadSpec};
use felixd_core::{module, names};
use felixd_runtime::Runtime;

use crate::felix::Felix;
use crate::settings::Settings;
use crate::specs;

/// What the check found for one workload.
#[derive(Debug)]
pub struct Report {
    pub workload: String,
    pub output: String,
    /// The stream the outputs were checked against.
    pub input: String,
    /// Output records on the stream, claims not counted.
    pub records: usize,
    pub findings: Vec<Finding>,
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
        let input = input_of(&felix, &id)
            .await
            .with_context(|| format!("check workload {id}"))?;
        if upstream
            && let Some(up) = names::output_of(&input)
            && !ids.iter().any(|i| i == up)
            && felix.cache_get(names::WORKLOADS, up).await?.is_some()
        {
            ids.push(up.to_owned());
        }
        inputs.push((id, input));
        at += 1;
    }
    let mut reports = Vec::new();
    for id in check::upstream_first(&inputs) {
        let report = check_one(&felix, &runtime, &id, complete)
            .await
            .with_context(|| format!("check workload {id}"))?;
        reports.push(report);
    }
    Ok(reports)
}

async fn check_one(felix: &Felix, runtime: &Runtime, id: &str, complete: bool) -> Result<Report> {
    let spec = felix
        .cache_get(names::WORKLOADS, id)
        .await?
        .context("no such workload")?;
    let cached = WorkloadSpec::decode(&spec)?;

    // Snapshots first, so every output a snapshot counts is on the stream when
    // it is read. Then the output: anything written after this is checked
    // against inputs that are all on the input stream already.
    let snap_stream = names::snapshot_stream(id);
    let snap_tail = felix.tail(&snap_stream).await?;
    let snapshots = felix.read_range(&snap_stream, 0, snap_tail).await?;
    let output_tail = felix.tail(&cached.output).await?;
    let records = felix.read_range(&cached.output, 0, output_tail).await?;
    // The module, input and limits the stream was written under, not those
    // of a later spec its owner refused.
    let (spec, limits) = judged(felix, id, &cached, &records).await?;
    let mut limits = limits.into_iter().peekable();
    let bytes = module_bytes(felix, &spec.module).await?;
    let mut workload = runtime
        .load(&bytes, spec.limits)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let input_tail = felix.tail(&spec.input).await?;
    let mut expected = Vec::new();
    for record in felix.read_range(&spec.input, 0, input_tail).await? {
        while let Some((_, applied)) = limits.next_if(|(from, _)| *from <= record.offset) {
            workload.set_limits(applied);
        }
        // An input that cannot be read or a step that fails ends the workload
        // there, on every node.
        let Ok(read) = StepInput::read(
            &spec,
            record.offset,
            record.timestamp_micros,
            &record.payload,
        ) else {
            break;
        };
        let Some(step) = read else {
            continue;
        };
        let Ok(stepped) = workload.step(step.as_input()) else {
            break;
        };
        for (index, produced) in input::produced(stepped).into_iter().enumerate() {
            let header = OutputHeader {
                input_offset: record.offset,
                index: u32::try_from(index)?,
            };
            expected.push(Expected { header, produced });
        }
    }
    let on_stream: Vec<(u64, &[u8])> = records
        .iter()
        .map(|r| (r.offset, r.payload.as_ref()))
        .collect();
    let mut findings = check::check(&expected, &on_stream, complete);
    let snapshots: Vec<(u64, &[u8])> = snapshots
        .iter()
        .map(|r| (r.offset, r.payload.as_ref()))
        .collect();
    findings.extend(check::check_snapshots(&snapshots, &on_stream, output_tail));
    Ok(Report {
        workload: id.to_owned(),
        output: spec.output.clone(),
        input: spec.input.clone(),
        records: records
            .iter()
            .filter(|r| !output::is_marker(&r.payload))
            .count(),
        findings,
    })
}

/// The spec workload `id`'s output stream was written under and the limits
/// each input ran under, from the spec records its claims name
/// ([`check::judged_under`]). `cached` is the `workloads` value.
pub(crate) async fn judged(
    felix: &Felix,
    id: &str,
    cached: &WorkloadSpec,
    records: &[StreamRecord],
) -> Result<(WorkloadSpec, Vec<(u64, Limits)>)> {
    let payloads = records.iter().map(|r| r.payload.as_ref());
    let mut claimed = HashMap::new();
    for offset in check::specs_claimed(payloads.clone()) {
        claimed.insert(offset, specs::read(felix, id, offset).await?);
    }
    Ok(check::judged_under(cached, &claimed, payloads))
}

/// The input workload `id`'s output stream was written under.
async fn input_of(felix: &Felix, id: &str) -> Result<String> {
    let cached = felix
        .cache_get(names::WORKLOADS, id)
        .await?
        .context("no such workload")?;
    let spec = specs::lineage(felix, id, WorkloadSpec::decode(&cached)?).await?;
    Ok(spec.input)
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
    let mut watch = felix.watch_retained(names::WORKLOADS).await?;
    let count = watch
        .retained_count()
        .context("the watch did not say how many workloads it holds")?;
    let mut ids = Vec::new();
    while (ids.len() as u64) < count {
        match watch.recv().await {
            Some(CacheWatchItem::Change(change)) if change.value.is_some() => ids.push(change.key),
            Some(CacheWatchItem::Change(_)) => {}
            _ => bail!("the workloads watch ended early"),
        }
    }
    ids.sort();
    Ok(ids)
}
