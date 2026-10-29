use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::fmt;
use std::sync::Mutex;

use anyhow::{Context, Result};
use felix_client::StreamRecord;
use felixd_core::output::{self, OutputHeader};
use felixd_core::resume::{self, Found, Resume};
use felixd_core::snapshot::{self, SnapshotHeader};
use felixd_core::spec::WorkloadSpec;
use felixd_core::{MAX_WRITE_BYTES, module, names};
use felixd_runtime::{Compiled, Runtime, Workload, WorkloadError};

use crate::faults;
use crate::felix::Felix;
use crate::lease::Fence;
use crate::output::OutputLog;

/// A node's runtime and the modules it has compiled, by hash.
pub(crate) struct Modules {
    pub(crate) runtime: Runtime,
    compiled: Mutex<HashMap<String, Compiled>>,
}

impl Modules {
    pub(crate) fn new(runtime: Runtime) -> Modules {
        Modules {
            runtime,
            compiled: Mutex::new(HashMap::new()),
        }
    }

    /// The module `hash`, compiled once per node. Compiling takes long
    /// enough to starve the lease renewals that share this task, so it runs
    /// on a blocking thread.
    async fn compiled(&self, felix: &Felix, hash: &str) -> Result<Compiled> {
        if let Some(compiled) = self.lock().get(hash) {
            return Ok(compiled.clone());
        }
        let bytes = felix
            .cache_get(names::MODULES, hash)
            .await?
            .with_context(|| format!("module {hash} is not in the modules cache"))?;
        if module::hash(&bytes) != hash {
            return Err(poison(format!("module {hash} does not match its hash")));
        }
        let runtime = self.runtime.clone();
        let compiled = tokio::task::spawn_blocking(move || {
            faults::during_compile();
            runtime.compile(&bytes)
        })
        .await?
        .map_err(poison)?;
        self.lock().insert(hash.to_owned(), compiled.clone());
        Ok(compiled)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Compiled>> {
        self.compiled.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// How [`run_workload`] ended. It only returns when the workload cannot go on
/// here.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The workload trapped, ran out of fuel or could not be loaded, or what
    /// is on its streams cannot be resumed from. It would fail the same way
    /// anywhere, so it is not retried.
    Failed(String),
    /// This node no longer holds the lease by its own clock.
    Lost,
    /// A record at this epoch was found on the output stream, above the
    /// owner's own: a later owner has written.
    Superseded(u64),
}

/// The runner found it no longer holds the lease.
#[derive(Debug)]
pub(crate) struct LostLease;

impl fmt::Display for LostLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the lease is lost")
    }
}

impl std::error::Error for LostLease {}

/// The output stream holds a record from a later epoch than the runner's.
#[derive(Debug)]
pub(crate) struct Superseded(pub(crate) u64);

impl fmt::Display for Superseded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a later owner wrote at epoch {}", self.0)
    }
}

impl std::error::Error for Superseded {}

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
/// Nothing is written once `fence` stops holding.
///
/// An `Err` is a problem with Felix. Calling this again resumes from what is on
/// the streams.
pub(crate) async fn run_workload(
    felix: &Felix,
    runtime: &Modules,
    id: &str,
    spec: &WorkloadSpec,
    fence: &Fence,
) -> Result<Outcome> {
    let error = match run(felix, runtime, id, spec, fence).await {
        Ok(never) => match never {},
        Err(error) => error,
    };
    if let Some(Superseded(epoch)) = error.downcast_ref::<Superseded>() {
        return Ok(Outcome::Superseded(*epoch));
    }
    if error.is::<Poison>() {
        Ok(Outcome::Failed(format!("{error:#}")))
    } else if error.is::<LostLease>() {
        Ok(Outcome::Lost)
    } else {
        Err(error)
    }
}

async fn run(
    felix: &Felix,
    runtime: &Modules,
    id: &str,
    spec: &WorkloadSpec,
    fence: &Fence,
) -> Result<Infallible> {
    let compiled = runtime.compiled(felix, &spec.module).await?;
    let mut workload = runtime
        .runtime
        .instantiate(&compiled, spec.limits)
        .map_err(poison)?;

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
    if let Some(last) = last_output.filter(|last| last.epoch > fence.epoch()) {
        tracing::info!(
            workload = id,
            epoch = last.epoch,
            "a later owner has written"
        );
        return Err(Superseded(last.epoch).into());
    }
    let last_output = last_output.map(|last| last.header);
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
        fence,
        workload,
        resume,
        log: OutputLog::new(felix, &spec.output, fence, output_tail),
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
    fence: &'a Fence,
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
            let record = output::encode(header, self.fence.epoch(), payload);
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
        if faults::skip_snapshot() {
            return Ok(());
        }
        faults::before_snapshot();
        if !self.fence.holds() {
            return Err(LostLease.into());
        }
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
            self.next = after;
        }
        let found = self.page.pop_front();
        let offset = found.as_ref().map_or(self.end, |r| r.offset);
        resume::check_skipped(offset, replayed, found.as_ref().map(|r| r.payload.as_ref()))
            .map_err(poison)
    }
}
