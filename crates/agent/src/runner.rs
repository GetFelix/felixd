use std::collections::VecDeque;
use std::convert::Infallible;
use std::fmt;

use anyhow::{Context, Result};
use felix_client::StreamRecord;
use felixd_core::output::{self, OutputHeader};
use felixd_core::resume::{self, Found, Resume};
use felixd_core::snapshot::{self, SnapshotHeader};
use felixd_core::spec::WorkloadSpec;
use felixd_core::{MAX_WRITE_BYTES, module, names};
use felixd_runtime::{Runtime, Workload, WorkloadError};

use crate::faults;
use crate::felix::Felix;
use crate::output::OutputLog;

/// How [`run_workload`] ended. It only returns when the workload cannot go on.
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The workload trapped, ran out of fuel or could not be loaded, or what
    /// is on its streams cannot be resumed from. It would fail the same way
    /// anywhere, so it is not retried.
    Failed(String),
}

/// A state no node can get past. Retrying would replay the same history into
/// the same failure, so the workload is marked failed instead.
#[derive(Debug)]
pub(crate) struct Poison(String);

impl fmt::Display for Poison {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Poison {}

pub(crate) fn poison(message: impl fmt::Display) -> anyhow::Error {
    Poison(message.to_string()).into()
}

fn failed(error: WorkloadError) -> anyhow::Error {
    let WorkloadError::Failed(failure) = error;
    poison(failure.message)
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
    match run(felix, runtime, id, spec).await {
        Ok(never) => match never {},
        Err(error) if error.is::<Poison>() => Ok(Outcome::Failed(format!("{error:#}"))),
        Err(error) => Err(error),
    }
}

async fn run(
    felix: &Felix,
    runtime: &Runtime,
    id: &str,
    spec: &WorkloadSpec,
) -> Result<Infallible> {
    let bytes = felix
        .cache_get(names::MODULES, &spec.module)
        .await?
        .with_context(|| format!("module {} is not in the modules cache", spec.module))?;
    if module::hash(&bytes) != spec.module {
        return Err(poison(format!(
            "module {} does not match its hash",
            spec.module
        )));
    }
    let mut workload = runtime.load(&bytes, spec.limits).map_err(poison)?;

    let snap_stream = names::snapshot_stream(id);
    let snap_tail = felix.tail(&snap_stream).await?;
    let snapshot = match felix.last_record(&snap_stream, snap_tail).await? {
        Some(record) => {
            let (header, state) = snapshot::decode(&record.payload)
                .map_err(|e| poison(format!("snapshot at {snap_stream}:{}: {e}", record.offset)))?;
            Some((header, state.to_vec()))
        }
        None => None,
    };
    let output_tail = felix.tail(&spec.output).await?;
    let last_output = match felix.last_record(&spec.output, output_tail).await? {
        Some(record) => Some(
            output::decode(&record.payload)
                .map_err(|e| poison(format!("output at {}:{}: {e}", spec.output, record.offset)))?
                .0,
        ),
        None => None,
    };
    let resume = resume::fold(Found {
        snapshot: snapshot.as_ref().map(|(header, _)| *header),
        last_output,
        output_tail,
    })
    .map_err(poison)?;
    if let Some((_, state)) = snapshot.as_ref().filter(|_| resume.restore) {
        workload.restore(state).map_err(failed)?;
    }
    tracing::info!(workload = id, from = resume.next_input, "resumed");

    let mut steps = Steps {
        felix,
        spec,
        snap_stream: &snap_stream,
        workload,
        resume,
        log: OutputLog::new(felix, &spec.output, output_tail),
        written: Written {
            felix,
            stream: &spec.output,
            next: resume.output_from,
            end: output_tail,
            page: VecDeque::new(),
        },
        since_snapshot: 0,
    };
    let mut events = felix.subscribe_from(&spec.input, resume.next_input).await?;
    let mut next_input = resume.next_input;
    loop {
        let event = events
            .next_event()
            .await?
            .context("the input stream closed")?;
        let offset = event
            .offset
            .ok_or_else(|| poison("the input stream is not durable, so it has no offsets"))?;
        if offset < next_input || faults::drop_input(offset) {
            continue;
        }
        if offset > next_input {
            // Felix drops records for a slow subscriber by default, and a jump
            // in offsets is all that shows it. Offsets that hold no event jump
            // too, so read the gap back to see which it was.
            for record in felix.read_range(&spec.input, next_input, offset).await? {
                if record.offset >= next_input {
                    steps.step(record.offset, &record.payload).await?;
                    next_input = record.offset + 1;
                }
            }
        }
        steps.step(offset, &event.payload).await?;
        next_input = offset + 1;
    }
}

/// Runs inputs through the workload in order, writing what they produce.
struct Steps<'a> {
    felix: &'a Felix,
    spec: &'a WorkloadSpec,
    snap_stream: &'a str,
    workload: Workload,
    resume: Resume,
    log: OutputLog<'a>,
    written: Written<'a>,
    since_snapshot: u64,
}

impl Steps<'_> {
    async fn step(&mut self, offset: u64, input: &[u8]) -> Result<()> {
        let outputs = self.workload.step(input).map_err(failed)?;
        let mut batch = Vec::new();
        for (index, payload) in outputs.iter().enumerate() {
            let index = index as u32;
            let header = OutputHeader {
                input_offset: offset,
                index,
            };
            let record = output::encode(header, payload);
            if self.resume.already_written(offset, index) {
                self.written.check(&record).await?;
            } else {
                batch.push(record);
            }
        }
        let bytes: usize = batch.iter().map(Vec::len).sum();
        if bytes > MAX_WRITE_BYTES {
            return Err(poison(format!(
                "input {offset} produced {bytes} bytes of output, more than the {MAX_WRITE_BYTES} one write may hold"
            )));
        }
        self.log.append(batch).await?;

        self.since_snapshot += 1;
        if self.since_snapshot >= self.spec.snapshot_every && !self.resume.replaying(offset) {
            self.snapshot(offset).await?;
            self.since_snapshot = 0;
        }
        Ok(())
    }

    async fn snapshot(&mut self, input_offset: u64) -> Result<()> {
        faults::before_snapshot();
        let state = self.workload.snapshot().map_err(failed)?;
        let header = SnapshotHeader {
            input_offset,
            output_tail: self.log.tail(),
        };
        let record = snapshot::encode(header, &state);
        if record.len() > MAX_WRITE_BYTES {
            return Err(poison(format!(
                "the snapshot at input {input_offset} is {} bytes, more than the {MAX_WRITE_BYTES} one write may hold",
                record.len()
            )));
        }
        self.felix.publish(self.snap_stream, record).await?;
        faults::after_snapshot();
        Ok(())
    }
}

/// The output records a resuming owner skips, read a page at a time so each
/// can be checked against what the owner produced in its place.
struct Written<'a> {
    felix: &'a Felix,
    stream: &'a str,
    next: u64,
    end: u64,
    page: VecDeque<StreamRecord>,
}

impl Written<'_> {
    async fn check(&mut self, replayed: &[u8]) -> Result<()> {
        while self.page.is_empty() && self.next < self.end {
            let (records, after) = self
                .felix
                .read_page(self.stream, self.next, self.end)
                .await?;
            self.page.extend(records);
            self.next = if after > self.next { after } else { self.end };
        }
        let found = self.page.pop_front();
        let offset = found.as_ref().map_or(self.end, |r| r.offset);
        resume::check_skipped(offset, replayed, found.as_ref().map(|r| r.payload.as_ref()))
            .map_err(poison)
    }
}
