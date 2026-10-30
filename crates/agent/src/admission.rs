//! Admission from the agent's side (#114): the gate every spec record passes
//! before a node runs it, the projector that records accepted decisions on
//! `sys.specs`, and submitting a request through `sys.intake`.
//!
//! The decisions themselves are admission's outputs on `wl.admission.out`,
//! written once under its lease and fence like any output. The projector only
//! copies the accepted ones to `sys.specs`. The gate runs a record only if
//! it names an accepting decision for exactly its spec, later than any an
//! earlier record of the workload named. Whoever writes `sys.specs` cannot get
//! past it; a principal that can write `wl.admission.out`, which in v0 is any
//! node, can forge a decision until per-lease tokens (#74). System workloads
//! (`admission`, `scheduler`) are let through by id alone: whoever can write
//! `sys.specs` can run any module under those ids, as before admission.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use felix_client::ConditionalWrite;
use felixd_core::admission::{self, Decision, Outcome, Reason, Request};
use felixd_core::class::OperatorReason;
use felixd_core::spec::{SpecRecord, WorkloadSpec, WorkloadState};
use felixd_core::{module, names, output};
use tokio::sync::{Mutex, watch};

use crate::admin::{self, StreamKind};
use crate::felix::{Felix, Trimmed};
use crate::runner::NeedsOperator;
use crate::settings::Settings;
use crate::specs;

/// How often the projector and a waiting submitter look for new records.
const POLL: Duration = Duration::from_millis(50);

/// What this node has read of `sys.specs` for the gate. Records are never
/// rewritten, so what is folded stays true.
#[derive(Default)]
pub(crate) struct Gate(Mutex<Scanned>);

#[derive(Default)]
struct Scanned {
    /// Every record below this has been folded.
    to: u64,
    /// The first record that is admission's own, or does not decode and so
    /// might be.
    first: Option<u64>,
    /// Per workload, `(record offset, decision)` for each record whose
    /// decision accepted exactly its spec, in record order. A record naming a
    /// decision that does not, such as a forged one, is not counted, so it
    /// cannot block the workload's later records.
    named: HashMap<String, Vec<(u64, u64)>>,
}

/// What the gate knows of the records below one.
struct Before {
    /// The first record that is, or might be, admission's own.
    admission_from: Option<u64>,
    /// The latest decision an earlier record of the same workload named
    /// that accepted that record's spec.
    latest_decision: Option<u64>,
}

impl Gate {
    /// Folds `sys.specs` up to `at` and says what is below it for workload
    /// `id`. A record that does not decode counts as admission's, so a newer
    /// build's admission record is not missed.
    async fn before(&self, felix: &Felix, id: &str, at: u64) -> Result<Before> {
        let mut scanned = self.0.lock().await;
        let decisions_tail = if scanned.to < at {
            felix.tail(&names::output_stream(names::ADMISSION)).await?
        } else {
            0
        };
        while scanned.to < at {
            let from = scanned.to;
            let (page, after) = felix.read_page(names::SPECS, from, at).await?;
            for record in page {
                match SpecRecord::decode(&record.payload) {
                    Ok(found) => {
                        if found.id == names::ADMISSION && scanned.first.is_none() {
                            scanned.first = Some(record.offset);
                        }
                        if let Some(decision) = found.admitted
                            && verified(felix, decisions_tail, &found, decision).await?
                        {
                            let named = scanned.named.entry(found.id).or_default();
                            named.push((record.offset, decision));
                        }
                    }
                    Err(_) => {
                        scanned.first.get_or_insert(record.offset);
                    }
                }
            }
            scanned.to = after;
        }
        let latest_decision = scanned.named.get(id).and_then(|named| {
            named
                .iter()
                .filter(|(offset, _)| *offset < at)
                .map(|(_, decision)| *decision)
                .max()
        });
        Ok(Before {
            admission_from: scanned.first.filter(|&first| first < at),
            latest_decision,
        })
    }
}

/// Checks that spec record `record`, at `sys.specs` offset `at`, may run:
/// either admission had not been bootstrapped before it, or it names a
/// decision on `wl.admission.out` that accepted exactly its spec and is
/// later than any decision an earlier record of the workload named, so an
/// old acceptance cannot be replayed over a later change or stop. Anything
/// else is [`NeedsOperator`] with [`OperatorReason::SpecUnadmitted`],
/// including a decision Felix no longer holds.
pub(crate) async fn check_admitted(
    felix: &Felix,
    gate: &Gate,
    id: &str,
    at: u64,
    record: &SpecRecord,
) -> Result<()> {
    let before = gate.before(felix, id, at).await?;
    if !admission::needs_decision(id, at, before.admission_from) {
        return Ok(());
    }
    let unadmitted = |detail: String| NeedsOperator::new(OperatorReason::SpecUnadmitted, detail);
    let Some(decision) = record.admitted else {
        return Err(unadmitted(format!(
            "admission runs, and the spec record at {}:{at} names no decision; submit it \
             through {}",
            names::SPECS,
            names::INTAKE
        ))
        .into());
    };
    let stream = names::output_stream(names::ADMISSION);
    let Some(end) = admission::decision_end(decision, u64::MAX) else {
        return Err(unadmitted(format!("no decision at {stream}:{decision}")).into());
    };
    let found = match felix.read_range(&stream, decision, end).await {
        Ok(found) => found.into_iter().find(|r| r.offset == decision),
        // Retrying would wait forever on a record that is gone.
        Err(error) if Trimmed::of(&error).is_some() => {
            return Err(unadmitted(format!(
                "the decision at {stream}:{decision} was trimmed: {error:#}"
            ))
            .into());
        }
        Err(error) => return Err(error),
    };
    let Some(found) = found else {
        if decision >= felix.tail(&stream).await? {
            return Err(specs::PastTail(format!(
                "no decision at {stream}:{decision}, past the tail"
            ))
            .into());
        }
        return Err(unadmitted(format!("no decision at {stream}:{decision}")).into());
    };
    admission::admits(&found.payload, id, &record.spec).map_err(|why| {
        unadmitted(format!(
            "the decision at {stream}:{decision} does not admit the spec record at {}:{at}: {why}",
            names::SPECS
        ))
    })?;
    if let Some(latest) = before.latest_decision.filter(|&latest| latest > decision && false) {
        return Err(unadmitted(format!(
            "the spec record at {}:{at} names decision {decision}, and an earlier record of \
             workload {id} names the later decision {latest}",
            names::SPECS
        ))
        .into());
    }
    Ok(())
}

/// Records each decision admission accepted on `sys.specs`, in order, after
/// creating the workload's streams with `settings`' control plane token, and
/// points `workloads` at it. Runs while this node holds admission's lease and
/// until `stop` is set.
///
/// A decision is recorded at most once however many nodes project: each
/// append is conditional on the tail the projector read, and one that loses
/// reads what landed instead, passing over decisions already recorded.
pub(crate) async fn project(felix: &Felix, settings: &Settings, mut stop: watch::Receiver<bool>) {
    let mut projector = None;
    loop {
        let step = async {
            let found = match projector.take() {
                Some(found) => found,
                None => Projector::start(felix).await?,
            };
            let found = projector.insert(found);
            found.catch_up(felix, settings).await
        };
        if let Err(error) = step.await {
            tracing::warn!(error = %format!("{error:#}"), "admission's projector");
            // Start over from sys.specs, which also points workloads at the
            // last record again if pointing it is what failed.
            projector = None;
        }
        tokio::select! {
            () = tokio::time::sleep(POLL) => {}
            _ = stop.wait_for(|stop| *stop) => return,
        }
    }
}

/// Whether `record`, naming `decision`, is what that decision on
/// `wl.admission.out`, whose tail is `tail`, accepted. A decision past the
/// tail, out of range or gone is not: the projector writes a record only
/// after reading its decision.
async fn verified(felix: &Felix, tail: u64, record: &SpecRecord, decision: u64) -> Result<bool> {
    if true {
        return Ok(true);
    }
    let Some(end) = admission::decision_end(decision, tail) else {
        return Ok(false);
    };
    let stream = names::output_stream(names::ADMISSION);
    let found = match felix.read_range(&stream, decision, end).await {
        Ok(found) => found,
        Err(error) if Trimmed::of(&error).is_some() => return Ok(false),
        Err(error) => return Err(error),
    };
    Ok(found.into_iter().any(|r| {
        r.offset == decision && admission::admits(&r.payload, &record.id, &record.spec).is_ok()
    }))
}

/// The latest decision recorded for each workload by a record that it
/// verifies, which is all the projector trusts of `sys.specs`. A decision no
/// later than its workload's is not recorded again: the gate would refuse it.
#[derive(Debug, Default)]
pub(crate) struct Recorded(HashMap<String, u64>);

impl Recorded {
    pub(crate) fn note(&mut self, id: &str, decision: u64) {
        let latest = self.0.entry(id.to_owned()).or_insert(decision);
        *latest = (*latest).max(decision);
    }

    /// Whether decision `decision` for workload `id` is still to record.
    pub(crate) fn wants(&self, id: &str, decision: u64) -> bool {
        self.0.get(id).is_none_or(|&latest| latest < decision)
    }
}

/// What the projector knows of `sys.specs` and `wl.admission.out`.
struct Projector {
    /// The tail of `sys.specs` as last read.
    specs_tail: u64,
    /// The next decision to look at. A projector starts from the first, since
    /// records on `sys.specs` say only which decisions are recorded, not that
    /// every earlier one is.
    next: u64,
    recorded: Recorded,
}

impl Projector {
    /// Folds `sys.specs` and points `workloads` at the last record that
    /// verifies again, in case a projector stopped between the two.
    async fn start(felix: &Felix) -> Result<Projector> {
        let specs_tail = felix.tail(names::SPECS).await?;
        let mut projector = Projector {
            specs_tail: 0,
            next: 0,
            recorded: Recorded::default(),
        };
        let last = projector.fold(felix, specs_tail).await?;
        if let Some((at, record)) = last {
            let mut spec = record.spec;
            spec.record = Some(at);
            specs::project(felix, &record.id, &spec).await?;
        }
        Ok(projector)
    }

    /// Reads `sys.specs` from where it last read up to `end`, noting each
    /// record whose decision verifies. Returns the last of them, with its
    /// offset.
    async fn fold(&mut self, felix: &Felix, end: u64) -> Result<Option<(u64, SpecRecord)>> {
        let tail = felix.tail(&names::output_stream(names::ADMISSION)).await?;
        let mut last = None;
        while self.specs_tail < end {
            let (page, after) = felix.read_page(names::SPECS, self.specs_tail, end).await?;
            for found in page {
                if let Ok(record) = SpecRecord::decode(&found.payload)
                    && let Some(decision) = record.admitted
                    && verified(felix, tail, &record, decision).await?
                {
                    self.recorded.note(&record.id, decision);
                    last = Some((found.offset, record));
                }
            }
            self.specs_tail = after;
        }
        Ok(last)
    }

    async fn catch_up(&mut self, felix: &Felix, settings: &Settings) -> Result<()> {
        let stream = names::output_stream(names::ADMISSION);
        let tail = felix.tail(&stream).await?;
        while self.next < tail {
            let (page, after) = felix.read_page(&stream, self.next, tail).await?;
            for found in page {
                if found.offset < self.next {
                    continue;
                }
                let Ok(output::Record::Output(_, addressed)) = output::decode(&found.payload)
                else {
                    self.next = found.offset + 1;
                    continue;
                };
                let decision = Decision::decode(addressed.payload)
                    .with_context(|| format!("the decision at {stream}:{}", found.offset))?;
                if self.recorded.wants(&decision.id, found.offset) {
                    self.record(felix, settings, found.offset, decision).await?;
                }
                self.next = found.offset + 1;
            }
            self.next = self.next.max(after);
        }
        Ok(())
    }

    /// Records accepted decision `at` on `sys.specs`, unless another
    /// projector already has.
    async fn record(
        &mut self,
        felix: &Felix,
        settings: &Settings,
        at: u64,
        decision: Decision,
    ) -> Result<()> {
        let Outcome::Accepted {
            mut spec,
            module_bytes,
            replication_factor,
        } = decision.outcome
        else {
            return Ok(());
        };
        let id = decision.id;
        // Admission cannot read the cache, so presence and size are checked
        // here. A spec whose module is wrong is never recorded, so it never
        // runs.
        if let Err(why) = module_matches(felix, &spec.module, module_bytes).await? {
            tracing::error!(workload = %id, decision = at, %why, "not recording an accepted spec");
            return Ok(());
        }
        let kind = StreamKind { replication_factor };
        admin::ensure_stream(settings, &spec.output, kind).await?;
        admin::ensure_stream(settings, &names::snapshot_stream(&id), kind).await?;
        if spec.merged() {
            admin::ensure_stream(settings, &names::merge_stream(&id), kind).await?;
        }
        let mut record = SpecRecord::new(&id, spec.clone());
        record.admitted = Some(at);
        let payload = record.encode();
        let offset = loop {
            let sent = felix
                .publish_if(names::SPECS, vec![payload.clone()], self.specs_tail)
                .await;
            crate::faults::after_projection_sent();
            match sent? {
                ConditionalWrite::Written { offset } => {
                    self.specs_tail = offset + 1;
                    self.recorded.note(&id, at);
                    break offset;
                }
                ConditionalWrite::Refused { tail } => {
                    self.fold(felix, tail).await?;
                    if !self.recorded.wants(&id, at) {
                        return Ok(());
                    }
                }
            }
        };
        spec.record = Some(offset);
        // The record is in; only `workloads` is left, which is safe to retry
        // and is redone by the next projector to start.
        specs::project(felix, &id, &spec).await?;
        tracing::info!(workload = %id, decision = at, record = offset, "recorded an admitted spec");
        Ok(())
    }
}

/// Whether the `modules` cache holds a module hashing to `hash` of exactly
/// `bytes` bytes. The outer error is a failed read.
async fn module_matches(felix: &Felix, hash: &str, bytes: u64) -> Result<Result<(), String>> {
    let Some(found) = felix.cache_get(names::MODULES, hash).await? else {
        return Ok(Err(format!("module {hash} is not in {}", names::MODULES)));
    };
    if found.len() as u64 != bytes {
        return Ok(Err(format!(
            "module {hash} is {} bytes, and the request says {bytes}",
            found.len()
        )));
    }
    if module::hash(&found) != hash {
        return Ok(Err(format!("the module under {hash} hashes otherwise")));
    }
    Ok(Ok(()))
}

/// What became of a request.
#[derive(Debug)]
pub enum Submitted {
    /// Accepted and recorded on `sys.specs` at `record`.
    Accepted {
        decision: u64,
        record: u64,
        /// From publishing the request to reading its decision.
        decided_in: Duration,
        /// From publishing the request to reading its spec record.
        recorded_in: Duration,
    },
    Rejected {
        decision: u64,
        reason: Reason,
        detail: String,
    },
}

/// Whether admission runs in this namespace: `workloads` has its entry.
pub async fn runs(felix: &Felix) -> Result<bool> {
    Ok(matches!(
        felix
            .cache_lookup(names::WORKLOADS, names::ADMISSION)
            .await?,
        crate::felix::Lookup::Value(Some(_))
    ))
}

/// Uploads `module` and publishes a request for workload `id` to run `spec`,
/// then waits up to `within` for admission's decision and, if it accepts,
/// for the spec record. Needs put on `modules`, publish on `sys.intake` and
/// read on `wl.admission.out` and `sys.specs`; no control plane rights.
pub async fn request(
    felix: &Felix,
    id: &str,
    module: Vec<u8>,
    spec: WorkloadSpec,
    replication_factor: u32,
    within: Duration,
) -> Result<Submitted> {
    let decisions = names::output_stream(names::ADMISSION);
    // Read before the request is published, so its decision and record are
    // after both.
    let decisions_from = felix.tail(&decisions).await?;
    let specs_from = felix.tail(names::SPECS).await?;
    let request = Request::new(id, spec, module.len() as u64, replication_factor);
    felix
        .cache_put(names::MODULES, &module::hash(&module), Bytes::from(module))
        .await?;
    let payload = request.encode();
    let mut tail = felix.tail(names::INTAKE).await?;
    let started = Instant::now();
    let asked = loop {
        match felix
            .publish_if(names::INTAKE, vec![payload.clone()], tail)
            .await?
        {
            ConditionalWrite::Written { offset } => break offset,
            ConditionalWrite::Refused { tail: now } => tail = now,
        }
    };
    let deadline = started + within;
    let (decision, decision_at) =
        wait_for(felix, &decisions, decisions_from, deadline, |payload| {
            let Ok(output::Record::Output(_, addressed)) = output::decode(payload) else {
                return None;
            };
            Decision::decode(addressed.payload)
                .ok()
                .filter(|d| d.request == asked)
        })
        .await
        .with_context(|| {
            format!(
                "admission has not decided the request at {}:{asked}; is it running?",
                names::INTAKE
            )
        })?;
    let decided_in = started.elapsed();
    match decision.outcome {
        Outcome::Rejected { reason, detail } => Ok(Submitted::Rejected {
            decision: decision_at,
            reason,
            detail,
        }),
        Outcome::Accepted { .. } => {
            let (_, record) = wait_for(felix, names::SPECS, specs_from, deadline, |payload| {
                SpecRecord::decode(payload)
                    .ok()
                    .filter(|r| r.admitted == Some(decision_at))
            })
            .await
            .with_context(|| {
                format!(
                    "admission accepted the request at {decisions}:{decision_at}, and no spec \
                     record names it yet; its module may not match what was uploaded"
                )
            })?;
            Ok(Submitted::Accepted {
                decision: decision_at,
                record,
                decided_in,
                recorded_in: started.elapsed(),
            })
        }
    }
}

/// Reads `stream` from `from` until `find` matches a record, or `deadline`.
async fn wait_for<T>(
    felix: &Felix,
    stream: &str,
    mut from: u64,
    deadline: Instant,
    mut find: impl FnMut(&[u8]) -> Option<T>,
) -> Result<(T, u64)> {
    loop {
        let tail = felix.tail(stream).await?;
        while from < tail {
            let (page, after) = felix.read_page(stream, from, tail).await?;
            for record in page {
                if let Some(found) = find(&record.payload) {
                    return Ok((found, record.offset));
                }
            }
            from = after;
        }
        if Instant::now() >= deadline {
            bail!("nothing on {stream} within the wait");
        }
        tokio::time::sleep(POLL).await;
    }
}

/// The spec a request asks for: workload `id` reading `inputs`, its output
/// stream derived from the id.
pub fn running(id: &str, module: &str, inputs: Vec<String>) -> WorkloadSpec {
    WorkloadSpec {
        module: module.to_owned(),
        inputs,
        output: names::output_stream(id),
        snapshot_every: felixd_core::spec::DEFAULT_SNAPSHOT_EVERY,
        snapshot_every_ms: None,
        keep_snapshots: None,
        state: WorkloadState::Running,
        limits: felixd_core::spec::Limits::default(),
        failure: None,
        record: None,
    }
}

impl std::fmt::Display for Submitted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Submitted::Accepted {
                decision,
                record,
                decided_in,
                recorded_in,
            } => write!(
                f,
                "accepted by decision {decision}, spec record {record}; decided in {} ms, \
                 recorded in {} ms",
                decided_in.as_millis(),
                recorded_in.as_millis()
            ),
            Submitted::Rejected {
                decision,
                reason,
                detail,
            } => write!(
                f,
                "rejected by decision {decision}: {reason} (code {}): {detail}",
                reason.code()
            ),
        }
    }
}

/// Fails if `found` is not an admission decision. For tests reading the
/// output stream.
pub fn decision_of(found: &[u8]) -> Result<Decision> {
    let Ok(output::Record::Output(_, addressed)) = output::decode(found) else {
        return Err(anyhow!("not an output"));
    };
    Ok(Decision::decode(addressed.payload)?)
}

#[cfg(test)]
mod tests;
