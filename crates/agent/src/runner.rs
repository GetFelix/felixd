use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use bytes::Bytes;
use felix_client::{ConditionalWrite, StreamRecord};
use felixd_core::output::{self, OutputHeader};
use felixd_core::resume::{self, Found, LastOutput, Resume};
use felixd_core::snapshot::{self, Manifest, SnapshotHeader, Taken};
use felixd_core::spec::WorkloadSpec;
use felixd_core::{MAX_WRITE_BYTES, module, names};
use felixd_runtime::{Compiled, Failure, FailureKind, LoadError, Runtime, Workload, WorkloadError};
use tokio::sync::{OnceCell, Semaphore};

use crate::faults;
use crate::felix::Felix;
use crate::input::{self, StepInput};
use crate::lease::Fence;
use crate::output::OutputLog;
use crate::snapshots::{self, ReadError, Snapshot};
use crate::specs;

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
    /// for it at once. A module built against the 0.1 contract is a
    /// [`NeedsOperator`] error.
    pub(crate) async fn compiled(&self, felix: &Felix, hash: &str) -> Result<Compiled> {
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
                .map_err(|error| match error {
                    LoadError::Contract01 => {
                        NeedsOperator(format!("module {hash}: {error}")).into()
                    }
                    error => poison(error),
                })
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
    /// No node of this build can run the workload, and it is not the
    /// workload's fault: an operator has to act.
    NeedsOperator(String),
}

/// A workload no node of this build can run, through no fault of its
/// inputs, such as a module built against the 0.1 contract. It is not
/// marked failed; the fix is a new module under a new id.
#[derive(Debug)]
pub(crate) struct NeedsOperator(pub(crate) String);

impl fmt::Display for NeedsOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for NeedsOperator {}

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
    match error {
        WorkloadError::Failed(failure) => poison(failure.message),
        WorkloadError::SnapshotOutOfFuel => poison("a snapshot ran out of fuel"),
    }
}

/// A snapshot ran out of `snapshot_fuel` and was abandoned. The wasm instance
/// cannot be entered again after the trap, so the run ends and resumes from
/// the last snapshot, which is still the one on the stream.
#[derive(Debug)]
struct SnapshotSkipped;

impl fmt::Display for SnapshotSkipped {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a snapshot ran out of fuel")
    }
}

impl std::error::Error for SnapshotSkipped {}

/// Snapshots abandoned for want of fuel since this process started.
static SNAPSHOTS_SKIPPED: AtomicU64 = AtomicU64::new(0);

fn read_error(error: ReadError) -> anyhow::Error {
    match error {
        ReadError::Felix(error) => error,
        ReadError::Bad(message) => poison(message),
    }
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
    // The first input a snapshot may be tried at again after one ran out of
    // fuel. Without it the run that resumes would retry at its first live
    // input and replay the whole gap each time.
    let mut hold = 0;
    let error = loop {
        match run(felix, runtime, id, spec, fence, claim_from, &mut hold).await {
            Ok(never) => match never {},
            Err(error) if error.is::<SnapshotSkipped>() => continue,
            Err(error) => break error,
        }
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
    if let Some(NeedsOperator(reason)) = error.downcast_ref::<NeedsOperator>() {
        return Ok(Outcome::NeedsOperator(reason.clone()));
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
    hold: &mut u64,
) -> Result<Infallible> {
    let spec_version = spec_version(felix, id, spec).await?;
    let spec_record = spec.record.context("the spec names no spec record")?;
    refuse_another_lineage(felix, id, spec).await?;
    // The claim comes first, so that from here on a stale owner's append is
    // refused, whether or not this owner ever writes an output.
    let log = OutputLog::claim(
        felix,
        &spec.output,
        fence,
        spec_version,
        spec_record,
        claim_from,
    )
    .await?;
    // Again after the claim: an owner of an older spec may have written the
    // first claim since the look above, and none can once this one is on.
    refuse_another_lineage(felix, id, spec).await?;
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
    let latest = snapshots::latest(felix, &snap_stream, snap_tail)
        .await
        .map_err(read_error)?;
    if let Some(epoch) = latest.last_epoch.filter(|&e| e > fence.epoch()) {
        tracing::info!(workload = id, epoch, "a later owner took a snapshot");
        return Err(Superseded(epoch).into());
    }
    let snapshot = latest.snapshot;
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
        snapshot: snapshot.as_ref().map(|s| s.taken().header),
        last_output,
        output_tail,
    })
    .map_err(poison)?;
    if let Some(snapshot) = snapshot.as_ref().filter(|_| resume.restore) {
        let chunks = snapshots::chunks(felix, &snap_stream, snapshot)
            .await
            .map_err(read_error)?;
        blocking(&runtime.calls, &mut workload, move |w| {
            let chunks: Vec<&[u8]> = chunks.iter().map(|c| c.as_ref()).collect();
            w.restore(&chunks)
        })
        .await?
        .map_err(|error| match error {
            WorkloadError::Failed(Failure {
                kind: FailureKind::OutOfFuel,
                ..
            }) => NeedsOperator(format!(
                "restoring the snapshot ran out of snapshot_fuel ({}), raise limits.snapshot_fuel",
                spec.limits.snapshot_fuel
            ))
            .into(),
            error => failed(error),
        })?;
    }
    tracing::info!(workload = id, from = resume.next_input, "resumed");
    let skip_claims = names::is_output_stream(&spec.input);
    // Chunks the next snapshot need not write again.
    let previous = snapshot.as_ref().and_then(Snapshot::manifest).cloned();
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
        previous,
        written: Written {
            felix,
            stream: &spec.output,
            next: resume.output_from,
            end: output_tail,
            page: VecDeque::new(),
        },
        since_snapshot: 0,
        previous_stamp,
        hold,
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
    /// The last snapshot manifest this owner restored or wrote.
    previous: Option<Manifest>,
    written: Written<'a>,
    since_snapshot: u64,
    /// The append time of the last input stepped, for time-based snapshots.
    previous_stamp: Option<u64>,
    /// No snapshot is taken at an input below this.
    hold: &'a mut u64,
}

impl Steps<'_> {
    async fn step(&mut self, offset: u64, stamp: u64, record: Bytes) -> Result<()> {
        let Some(input) = StepInput::read(self.spec, offset, stamp, &record).map_err(poison)?
        else {
            return Ok(());
        };
        let interval = self
            .spec
            .snapshot_every_ms
            .is_some_and(|ms| snapshot::starts_interval(ms, self.previous_stamp, stamp));
        self.previous_stamp = Some(stamp);
        let stepped = blocking(&self.modules.calls, &mut self.workload, move |w| {
            w.step(input.as_input())
        })
        .await?
        .map_err(failed)?;
        let mut batch = Vec::new();
        for (index, produced) in input::produced(stepped).iter().enumerate() {
            let index = index as u32;
            let header = OutputHeader {
                input_offset: offset,
                index,
            };
            let record = produced
                .encode(header, self.fence.epoch())
                .map_err(poison)?;
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
        if due && offset >= *self.hold && !self.resume.replaying(offset) {
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
        let chunks =
            match blocking(&self.modules.calls, &mut self.workload, Workload::snapshot).await? {
                Ok(chunks) => chunks,
                Err(WorkloadError::SnapshotOutOfFuel) => {
                    // Nothing has been written, so the previous snapshot is still
                    // the latest. Fuel is metered deterministically, so every
                    // replica skips the same snapshots.
                    let skipped = SNAPSHOTS_SKIPPED.fetch_add(1, Ordering::Relaxed) + 1;
                    tracing::warn!(
                        workload = self.snap_stream,
                        input = input_offset,
                        skipped,
                        "a snapshot ran out of snapshot_fuel and was abandoned"
                    );
                    *self.hold = input_offset + self.spec.snapshot_every.max(1);
                    return Err(SnapshotSkipped.into());
                }
                Err(error) => return Err(failed(error)),
            };
        let taken = Taken {
            header: SnapshotHeader {
                input_offset,
                output_tail: self.log.tail(),
            },
            epoch: self.fence.epoch(),
        };
        faults::before_snapshot_write();
        let manifest = self.write_snapshot(taken, &chunks).await?;
        self.previous = Some(manifest);
        faults::after_snapshot();
        Ok(())
    }

    /// Writes the chunks the previous manifest does not hold, then the
    /// manifest, which is what makes them a snapshot. Each write is a
    /// `publish_if` at the snapshot stream's tail, so a stale owner cannot
    /// write after a later owner's records. Records found at the tail from
    /// epochs up to this owner's are a stale owner's that landed first, which
    /// determinism makes correct, so it writes after them. One from a later
    /// epoch means this owner is superseded. Chunks that land without their
    /// manifest are inert.
    async fn write_snapshot(&mut self, taken: Taken, chunks: &[Vec<u8>]) -> Result<Manifest> {
        let refs: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();
        let plan = snapshot::plan(self.previous.as_ref(), &refs);
        let records: Vec<Vec<u8>> = plan
            .write
            .iter()
            .map(|&i| snapshot::encode_chunk(taken.epoch, &plan.hashes[i], &chunks[i]))
            .collect();
        let mut written_at = Vec::with_capacity(records.len());
        loop {
            let next = written_at.len();
            let mut end = next;
            let mut bytes = 0;
            while end < records.len()
                && (end == next || bytes + records[end].len() <= MAX_WRITE_BYTES)
            {
                bytes += records[end].len();
                end += 1;
            }
            let mut batch = records[next..end].to_vec();
            let mut manifest = None;
            if end == records.len() {
                // The manifest goes in the same write as the last chunks when
                // it fits, at the offsets they land at if the write succeeds.
                let at: Vec<u64> = written_at
                    .iter()
                    .copied()
                    .chain((0..batch.len() as u64).map(|i| self.snap_tail + i))
                    .collect();
                let m = plan
                    .manifest(taken, &at)
                    .expect("one offset per chunk written");
                let record = snapshot::encode_manifest(&m);
                if batch.is_empty() || bytes + record.len() <= MAX_WRITE_BYTES {
                    batch.push(record);
                    manifest = Some(m);
                }
            }
            if !self.fence.holds() {
                return Err(LostLease.into());
            }
            let count = batch.len() as u64;
            match self
                .felix
                .publish_if(self.snap_stream, batch, self.snap_tail)
                .await?
            {
                ConditionalWrite::Written { offset } => {
                    if offset != self.snap_tail {
                        return Err(poison(format!(
                            "{} wrote at {offset}, expected {}",
                            self.snap_stream, self.snap_tail
                        )));
                    }
                    let chunks_written = count - u64::from(manifest.is_some());
                    written_at.extend(offset..offset + chunks_written);
                    self.snap_tail = offset + count;
                    if let Some(manifest) = manifest {
                        return Ok(manifest);
                    }
                }
                ConditionalWrite::Refused { tail } => {
                    let found = self
                        .felix
                        .read_range(self.snap_stream, self.snap_tail, tail)
                        .await?;
                    for found in &found {
                        let epoch = snapshot::decode(&found.payload).map_or(0, |r| r.epoch());
                        if epoch > self.fence.epoch() {
                            return Err(Superseded(epoch).into());
                        }
                    }
                    self.snap_tail = tail.max(self.snap_tail);
                }
            }
        }
    }
}

/// Refuses to run `spec` over output written under another module or input,
/// which it would not reproduce. An operator puts the change under a new id.
async fn refuse_another_lineage(felix: &Felix, id: &str, spec: &WorkloadSpec) -> Result<()> {
    if let Some(written) = specs::written_under(felix, id).await?
        && let Some(conflict) = written.conflicts_with_history(spec)
    {
        return Err(NeedsOperator(format!(
            "its output was written while {conflict}, and this spec changes that; \
             submit the change under a new id"
        ))
        .into());
    }
    Ok(())
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
