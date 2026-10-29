//! `felixd check`: replays each workload's module over its input and reports
//! what is wrong with its output stream. Needs read grants only.

use anyhow::{Context, Result, bail};
use felix_client::CacheWatchItem;
use felixd_core::check::{self, Expected, Finding};
use felixd_core::output::OutputHeader;
use felixd_core::spec::WorkloadSpec;
use felixd_core::{module, names};
use felixd_runtime::Runtime;

use crate::felix::Felix;
use crate::settings::Settings;

/// What the check found for one workload.
#[derive(Debug)]
pub struct Report {
    pub workload: String,
    pub output: String,
    /// Output records on the stream.
    pub records: usize,
    pub findings: Vec<Finding>,
}

/// Checks the workloads in `ids`, or every workload in the `workloads` cache
/// when it is empty. `complete` makes outputs missing from the end of a stream
/// a finding.
pub async fn check(settings: &Settings, ids: &[String], complete: bool) -> Result<Vec<Report>> {
    let felix = Felix::connect(settings).await?;
    let runtime = Runtime::new().map_err(|e| anyhow::anyhow!("{e}"))?;
    let ids = if ids.is_empty() {
        workload_ids(&felix).await?
    } else {
        ids.to_vec()
    };
    let mut reports = Vec::new();
    for id in ids {
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
    let spec = WorkloadSpec::decode(&spec)?;
    let bytes = felix
        .cache_get(names::MODULES, &spec.module)
        .await?
        .with_context(|| format!("module {} is not in the modules cache", spec.module))?;
    if module::hash(&bytes) != spec.module {
        bail!("module {} does not match its hash", spec.module);
    }
    let mut workload = runtime
        .load(&bytes, spec.limits)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Snapshots first, so every output a snapshot counts is on the stream when
    // it is read. Then the output: anything written after this is checked
    // against inputs that are all on the input stream already.
    let snap_stream = names::snapshot_stream(id);
    let snap_tail = felix.tail(&snap_stream).await?;
    let snapshots = felix.read_range(&snap_stream, 0, snap_tail).await?;
    let output_tail = felix.tail(&spec.output).await?;
    let records = felix.read_range(&spec.output, 0, output_tail).await?;
    let input_tail = felix.tail(&spec.input).await?;
    let mut expected = Vec::new();
    for input in felix.read_range(&spec.input, 0, input_tail).await? {
        // A step that fails ends the workload there, on every node.
        let Ok(outputs) = workload.step(&input.payload) else {
            break;
        };
        for (index, payload) in outputs.into_iter().enumerate() {
            expected.push(Expected {
                header: OutputHeader {
                    input_offset: input.offset,
                    index: u32::try_from(index)?,
                },
                payload,
            });
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
        records: records.len(),
        findings,
    })
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
