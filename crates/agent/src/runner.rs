use anyhow::{Context, Result, ensure};
use felixd_core::output::{self, OutputHeader};
use felixd_core::resume::{self, Found};
use felixd_core::snapshot::{self, SnapshotHeader};
use felixd_core::spec::WorkloadSpec;
use felixd_core::{module, names};
use felixd_runtime::{Runtime, Workload, WorkloadError};

use crate::felix::Felix;
use crate::output::OutputLog;

/// How [`run_workload`] ended. It only returns when the workload cannot go on.
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The workload trapped, ran out of fuel or could not be loaded. It would
    /// fail the same way anywhere, so it is not retried.
    Failed(String),
}

/// Resumes the workload from Felix and runs it: for each input, `step`, append
/// the outputs, and write a snapshot every `snapshot_every` inputs.
///
/// An `Err` is a problem with Felix. Calling this again resumes from what is on
/// the streams.
pub async fn run_workload(
    felix: &Felix,
    runtime: &Runtime,
    id: &str,
    spec: &WorkloadSpec,
) -> Result<Outcome> {
    let bytes = felix
        .cache_get(names::MODULES, &spec.module)
        .await?
        .with_context(|| format!("module {} is not in the modules cache", spec.module))?;
    ensure!(
        module::hash(&bytes) == spec.module,
        "module {} does not match its hash",
        spec.module
    );
    let mut workload = match runtime.load(&bytes) {
        Ok(workload) => workload,
        Err(error) => return Ok(Outcome::Failed(error.to_string())),
    };

    let snap_stream = names::snapshot_stream(id);
    let snap_tail = felix.tail(&snap_stream).await?;
    let snapshot = match felix.last_record(&snap_stream, snap_tail).await? {
        Some(record) => {
            let (header, state) = snapshot::decode(&record.payload)?;
            Some((header, state.to_vec()))
        }
        None => None,
    };
    let output_tail = felix.tail(&spec.output).await?;
    let last_output = match felix.last_record(&spec.output, output_tail).await? {
        Some(record) => Some(output::decode(&record.payload)?.0),
        None => None,
    };
    let resume = resume::fold(Found {
        snapshot: snapshot.as_ref().map(|(header, _)| *header),
        last_output,
        output_tail,
    })?;
    if let Some((_, state)) = snapshot.as_ref().filter(|_| resume.restore)
        && let Err(WorkloadError::Failed(f)) = workload.restore(state)
    {
        return Ok(Outcome::Failed(f.message));
    }
    tracing::info!(workload = id, from = resume.next_input, "resumed");

    let mut log = OutputLog::new(felix, &spec.output, output_tail);
    let mut events = felix.subscribe_from(&spec.input, resume.next_input).await?;
    let mut next_input = resume.next_input;
    let mut since_snapshot = 0u64;
    loop {
        let event = events
            .next_event()
            .await?
            .context("the input stream closed")?;
        let offset = event
            .offset
            .context("the input stream is not durable, so it has no offsets")?;
        if offset < next_input {
            continue;
        }
        next_input = offset + 1;

        let outputs = match workload.step(&event.payload) {
            Ok(outputs) => outputs,
            Err(WorkloadError::Failed(f)) => return Ok(Outcome::Failed(f.message)),
        };
        let batch: Vec<Vec<u8>> = outputs
            .iter()
            .enumerate()
            .map(|(index, payload)| (index as u32, payload))
            .filter(|(index, _)| !resume.already_written(offset, *index))
            .map(|(index, payload)| {
                output::encode(
                    OutputHeader {
                        input_offset: offset,
                        index,
                    },
                    payload,
                )
            })
            .collect();
        log.append(batch).await?;

        since_snapshot += 1;
        if since_snapshot >= spec.snapshot_every {
            write_snapshot(felix, &snap_stream, &mut workload, offset, log.tail()).await?;
            since_snapshot = 0;
        }
    }
}

async fn write_snapshot(
    felix: &Felix,
    stream: &str,
    workload: &mut Workload,
    input_offset: u64,
    output_tail: u64,
) -> Result<()> {
    let state = workload.snapshot().map_err(|e| anyhow::anyhow!("{e}"))?;
    let header = SnapshotHeader {
        input_offset,
        output_tail,
    };
    felix
        .publish(stream, snapshot::encode(header, &state))
        .await
}
