use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use bytes::Bytes;
use felix_client::{ConditionalWrite, StreamRecord};
use felixd_core::output::{self, OutputHeader};
use felixd_core::resume::{self, Found, LastOutput, Resume};
use felixd_core::snapshot::{self, SnapshotHeader};
use felixd_core::spec::WorkloadSpec;
use felixd_core::{MAX_WRITE_BYTES, module, names};
use felixd_runtime::{Compiled, Runtime, Workload, WorkloadError};
use tokio::sync::{OnceCell, Semaphore};

use crate::faults;
use crate::felix::Felix;
use crate::lease::Fence;
use crate::output::OutputLog;

/// A node's runtime, the modules it has compiled, by hash, and the limits on
/// blocking work.
pub(crate) struct Modules {
    pub(crate) runtime: Runtime,
    compiled: Once<Compiled>,
    compiles: Semaphore,
    calls: Semaphore,
}

impl Modules {
    pub(crate) fn new(runtime: Runtime) -> Modules {
        let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
        Modules {
            runtime,
            compiled: Once::default(),
            compiles: Semaphore::new(cores),
            calls: Semaphore::new(cores),
        }
    }

    /// The module `hash`, compiled once per node however many workloads ask
    /// for it at once.
    async fn compiled(&self, felix: &Felix, hash: &str) -> Result<Compiled> {
        self.compiled
            .get(hash, || async {
                let bytes = felix
                    .cache_get(names::MODULES, hash)
                    .await?
                    .with_context(|| format!("module {hash} is not in the modules cache"))?;
                if module::hash(&bytes) != hash {
                    return Err(poison(format!("module {hash} does not match its hash")));
                }
                let _permit = self.compiles.acquire().await?;
                let runtime = self.runtime.clone();
                tokio::task::spawn_blocking(move || {
                    faults::during_compile();
                    runtime.compile(&bytes)
                })
                .await?
                .map_err(poison)
            })
            .await
    }
}

/// Values built once per key. Callers that ask while one is being built wait
/// for it; a build that fails is tried again by the next caller.
pub(crate) struct Once<T> {
    cells: Mutex<HashMap<String, Arc<OnceCell<T>>>>,
}

impl<T> Default for Once<T> {
    fn default() -> Self {
        Once {
            cells: Mutex::new(HashMap::new()),
        }
    }
}

impl<T: Clone> Once<T> {
    pub(crate) async fn get<F, Fut>(&self, key: &str, build: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let cell = {
            let mut cells = self.cells.lock().unwrap_or_else(|e| e.into_inner());
            cells.entry(key.to_owned()).or_default().clone()
        };
        cell.get_or_try_init(build).await.cloned()
    }
}

/// Runs `call` on the value in `slot` on a blocking thread, taking one of
/// `permits`. A step can run for as long as its fuel allows, and run inline it
/// would hold a runtime worker that other workloads' lease renewals need.
/// The slot is empty only if an earlier call panicked or was cancelled.
pub(crate) async fn blocking<W, T>(
    permits: &Semaphore,
    slot: &mut Option<W>,
    call: impl FnOnce(&mut W) -> T + Send + 'static,
) -> Result<T>
where
    W: Send + 'static,
    T: Send + 'static,
{
    let _permit = permits.acquire().await?;
    let mut value = slot
        .take()
        .context("the workload was lost by an earlier call")?;
    let (value, out) = tokio::task::spawn_blocking(move || {
        let out = call(&mut value);
        (value, out)
    })
    .await?;
    *slot = Some(value);
    Ok(out)
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
    /// The output stream records that the workload failed under this spec
    /// version, which is the one this node runs or a later one.
    FailedOnStream { spec_version: u64, reason: String },
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

/// The output stream holds a failure record for the spec the runner runs.
#[derive(Debug)]
pub(crate) struct FailedOnStream {
    pub(crate) spec_version: u64,
    pub(crate) reason: String,
}

impl fmt::Display for FailedOnStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "the output stream records a failure: {}", self.reason)
    }
}

impl std::error::Error for FailedOnStream {}

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
/// the outputs, and write a snapshot every `snapshot_every` inputs and at the
/// first input of every `snapshot_every_ms` interval.
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
    claim_from: &mut Option<u64>,
) -> Result<Outcome> {
    let error = match run(felix, runtime, id, spec, fence, claim_from).await {
        Ok(never) => match never {},
        Err(error) => error,
    };
    if let Some(Superseded(epoch)) = error.downcast_ref::<Superseded>() {
        return Ok(Outcome::Superseded(*epoch));
    }
    if let Some(found) = error.downcast_ref::<FailedOnStream>() {
        return Ok(Outcome::FailedOnStream {
            spec_version: found.spec_version,
            reason: found.reason.clone(),
        });
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
    claim_from: &mut Option<u64>,
) -> Result<Infallible> {
    let spec_version = spec_version(felix, id, spec).await?;
    // The claim comes first, so that from here on a stale owner's append is
    // refused, whether or not this owner ever writes an output.
    let log = OutputLog::claim(felix, &spec.output, fence, spec_version, claim_from).await?;
    let output_tail = log.tail();
    let compiled = runtime.compiled(felix, &spec.module).await?;
    let mut workload = Some(
        runtime
            .runtime
            .instantiate(&compiled, spec.limits)
            .map_err(poison)?,
    );

    let snap_stream = names::snapshot_stream(id);
    let snap_tail = felix.tail(&snap_stream).await?;
    let snapshot = match felix.last_record(&snap_stream, snap_tail).await? {
        Some(record) => {
            let at = |e: &dyn fmt::Display| {
                poison(format!("snapshot at {snap_stream}:{}: {e}", record.offset))
            };
            let decoded = snapshot::decode(&record.payload).map_err(|e| at(&e))?;
            if decoded.epoch() > fence.epoch() {
                tracing::info!(
                    workload = id,
                    epoch = decoded.epoch(),
                    "a later owner took a snapshot"
                );
                return Err(Superseded(decoded.epoch()).into());
            }
            let snapshot::Record::Whole(taken, state) = decoded else {
                return Err(at(&"this build restores only version 2 snapshots"));
            };
            Some((taken.header, state.to_vec()))
        }
        None => None,
    };
    let last_output = last_output(felix, &spec.output, output_tail).await?;
    if let Some((_, last)) = last_output.filter(|(_, last)| last.epoch > fence.epoch()) {
        tracing::info!(
            workload = id,
            epoch = last.epoch,
            "a later owner has written"
        );
        return Err(Superseded(last.epoch).into());
    }
    let last_output = last_output.map(|(offset, last)| LastOutput {
        offset,
        header: last.header,
    });
    let resume = resume::fold(Found {
        snapshot: snapshot.as_ref().map(|(header, _)| *header),
        last_output,
        output_tail,
    })
    .map_err(poison)?;
    if let Some((_, state)) = snapshot.filter(|_| resume.restore) {
        blocking(&runtime.calls, &mut workload, move |w| w.restore(&state))
            .await?
            .map_err(failed)?;
    }
    tracing::info!(workload = id, from = resume.next_input, "resumed");
    let skip_claims = names::is_output_stream(&spec.input);
    let previous_stamp = match spec.snapshot_every_ms {
        Some(_) => last_input_stamp(felix, &spec.input, resume.next_input, skip_claims).await?,
        None => None,
    };

    let mut steps = Steps {
        felix,
        modules: runtime,
        spec,
        snap_stream: &snap_stream,
        snap_tail,
        fence,
        workload,
        resume,
        log,
        skip_claims,
        written: Written {
            felix,
            stream: &spec.output,
            next: resume.output_from,
            end: output_tail,
            page: VecDeque::new(),
        },
        since_snapshot: 0,
        previous_stamp,
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
                    steps
                        .step(record.offset, record.timestamp_micros, record.payload)
                        .await?;
                    next_input = record.offset + 1;
                }
            }
        }
        // A read stamps every record; an event without a stamp is from a
        // broker that keeps none, whose reads say 0.
        let stamp = event.timestamp_micros.unwrap_or(0);
        steps.step(offset, stamp, event.payload).await?;
        next_input = offset + 1;
    }
}

/// Runs inputs through the workload in order, writing what they produce.
struct Steps<'a> {
    felix: &'a Felix,
    modules: &'a Modules,
    spec: &'a WorkloadSpec,
    snap_stream: &'a str,
    /// The snapshot stream's next offset, as far as this owner knows.
    snap_tail: u64,
    fence: &'a Fence,
    workload: Option<Workload>,
    resume: Resume,
    log: OutputLog<'a>,
    /// Whether the input is another workload's output stream, whose claims
    /// are not inputs.
    skip_claims: bool,
    written: Written<'a>,
    since_snapshot: u64,
    /// The append time of the last input stepped, for time-based snapshots.
    previous_stamp: Option<u64>,
}

impl Steps<'_> {
    async fn step(&mut self, offset: u64, stamp: u64, input: Bytes) -> Result<()> {
        if self.skip_claims && output::passed_over(&input, output::DEFAULT_NAME) {
            return Ok(());
        }
        let interval = self
            .spec
            .snapshot_every_ms
            .is_some_and(|ms| snapshot::starts_interval(ms, self.previous_stamp, stamp));
        self.previous_stamp = Some(stamp);
        let outputs = blocking(&self.modules.calls, &mut self.workload, move |w| {
            w.step(&input)
        })
        .await?
        .map_err(failed)?;
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
        let due = interval || self.since_snapshot >= self.spec.snapshot_every;
        if due && !self.resume.replaying(offset) {
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
        let state = blocking(&self.modules.calls, &mut self.workload, Workload::snapshot)
            .await?
            .map_err(failed)?;
        let header = SnapshotHeader {
            input_offset,
            output_tail: self.log.tail(),
        };
        let record = snapshot::encode(header, self.fence.epoch(), &state);
        if record.len() > MAX_WRITE_BYTES {
            return Err(poison(format!(
                "the snapshot at input {input_offset} is {} bytes, more than the {MAX_WRITE_BYTES} one write may hold",
                record.len()
            )));
        }
        faults::before_snapshot_write();
        self.write_snapshot(record).await?;
        faults::after_snapshot();
        Ok(())
    }

    /// Appends `record` at the snapshot stream's tail with `publish_if`, so a
    /// stale owner cannot write a snapshot after a later owner's. Snapshots
    /// found at the tail from epochs up to this owner's are a stale owner's
    /// that landed first, which determinism makes correct, so it writes after
    /// them. One from a later epoch means this owner is superseded.
    async fn write_snapshot(&mut self, record: Vec<u8>) -> Result<()> {
        loop {
            if !self.fence.holds() {
                return Err(LostLease.into());
            }
            let seen_tail = match self
                .felix
                .publish_if(self.snap_stream, vec![record.clone()], self.snap_tail)
                .await?
            {
                ConditionalWrite::Written { offset } => {
                    self.snap_tail = offset + 1;
                    return Ok(());
                }
                ConditionalWrite::Refused { tail } => tail,
            };
            let found = self
                .felix
                .read_range(self.snap_stream, self.snap_tail, seen_tail)
                .await?;
            for found in &found {
                let epoch = snapshot::decode(&found.payload).map_or(0, |r| r.epoch());
                if epoch > self.fence.epoch() {
                    return Err(Superseded(epoch).into());
                }
            }
            self.snap_tail = seen_tail.max(self.snap_tail);
        }
    }
}

/// The cache version of `spec`, which a failure record names. A different
/// spec in the cache means this task is about to be restarted under it.
async fn spec_version(felix: &Felix, id: &str, spec: &WorkloadSpec) -> Result<u64> {
    let found = felix
        .cache_get_versioned(names::WORKLOADS, id)
        .await?
        .context("the spec is gone")?;
    let current = WorkloadSpec::decode(&found.value).map_err(|e| anyhow::anyhow!("{e}"))?;
    anyhow::ensure!(current == *spec, "the spec has changed");
    Ok(found.version)
}

/// The append time of the last input below `next`, passing over claims when
/// the input is another workload's output, so a resumed owner decides time
/// snapshot points from the same previous input as an uninterrupted one.
async fn last_input_stamp(
    felix: &Felix,
    stream: &str,
    next: u64,
    skip_claims: bool,
) -> Result<Option<u64>> {
    let mut end = next;
    let mut window = 16;
    while end > 0 {
        let from = end.saturating_sub(window);
        for record in felix.read_range(stream, from, end).await?.iter().rev() {
            if !(skip_claims && output::passed_over(&record.payload, output::DEFAULT_NAME)) {
                return Ok(Some(record.timestamp_micros));
            }
        }
        end = from;
        window = window.saturating_mul(8);
    }
    Ok(None)
}

/// The last output or rejection below `tail` and where it is, passing over
/// claims and failures.
async fn last_output(
    felix: &Felix,
    stream: &str,
    tail: u64,
) -> Result<Option<(u64, output::Written)>> {
    let mut end = tail;
    let mut window = 16;
    while end > 0 {
        let from = end.saturating_sub(window);
        for record in felix.read_range(stream, from, end).await?.iter().rev() {
            let found = output::decode(&record.payload)
                .map_err(|e| poison(format!("output at {stream}:{}: {e}", record.offset)))?;
            if let Some(written) = found.written() {
                return Ok(Some((record.offset, written)));
            }
        }
        end = from;
        window = window.saturating_mul(8);
    }
    Ok(None)
}

/// The output records a resuming owner skips, read a page at a time so each
/// can be checked against what the owner produced in its place. Claims are
/// passed over.
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
            self.page.extend(
                records
                    .into_iter()
                    .filter(|r| !output::is_marker(&r.payload)),
            );
            self.next = after;
        }
        let found = self.page.pop_front();
        let offset = found.as_ref().map_or(self.end, |r| r.offset);
        resume::check_skipped(offset, replayed, found.as_ref().map(|r| r.payload.as_ref()))
            .map_err(poison)
    }
}

#[cfg(test)]
mod tests;
