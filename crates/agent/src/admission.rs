//! Admission from the agent's side (#114): the gate every spec record passes
//! before a node runs it, the projector that records accepted decisions on
//! `sys.specs`, and submitting a request through `sys.intake`.
//!
//! The decisions themselves are admission's outputs on `wl.admission.out`,
//! written once under its lease and fence like any output. The projector only
//! copies the accepted ones to `sys.specs`. The gate runs a record only if
//! it names an accepting decision for exactly its spec, and no record of the
//! workload names a later one that does. Whoever writes `sys.specs` cannot get
//! past it; a principal that can write `wl.admission.out`, which in v0 is any
//! node, can forge a decision until per-lease tokens (#74). System workloads
//! (`admission`, `scheduler`) are let through by id alone: whoever can write
//! `sys.specs` can run any module under those ids, as before admission.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use felix_client::ConditionalWrite;
use felixd_core::admission::{self, Decision, Outcome, Reason, Request};
use felixd_core::class::OperatorReason;
use felixd_core::spec::{SpecRecord, WorkloadSpec, WorkloadState};
use felixd_core::{module, names, output};
use tokio::sync::watch;

use crate::admin::{self, StreamKind};
use crate::felix::{Felix, Trimmed};
use crate::runner::NeedsOperator;
use crate::settings::Settings;
use crate::specs;

/// How often the projector and a waiting submitter look for new records.
const POLL: Duration = Duration::from_millis(50);

/// Unread records one workload may pile up in the gate before it reads them,
/// which bounds the gate's memory however often a workload changes.
const MAX_UNVERIFIED: usize = 64;

/// What this node has read of `sys.specs` for the gate. Records are never
/// rewritten, so what is folded stays true. The lock is never held across a
/// read, so one slow read does not hold up every other workload's check.
#[derive(Default)]
pub(crate) struct Gate(StdMutex<Scanned>);

#[derive(Default)]
struct Scanned {
    /// Every record below this has been folded.
    to: u64,
    /// Whether a record is admission's own, `felixd init`'s marker, or does
    /// not decode and so might be.
    admission_runs: bool,
    /// Per workload, the records naming a decision.
    named: HashMap<String, Named>,
}

/// Records naming decisions, as far as they can still change which is the
/// newest decision that verifies: one that accepted exactly its record's
/// spec. A record whose decision does not, such as a forged one, never
/// counts, so it cannot block anything.
#[derive(Debug, Default)]
struct Named {
    /// The newest verified `(decision, record)`.
    verified: Option<(u64, u64)>,
    /// `(decision, record)` of each record naming a later decision than
    /// `verified`, not read yet.
    unverified: Vec<(u64, u64)>,
}

/// What reading a record's decision found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verdict {
    /// The decision accepted exactly the record's id and spec.
    Verified,
    /// It did not, and never will: records and decisions are never
    /// rewritten.
    Refused,
    /// The decision is past the tail. It may yet be written, so the record
    /// is read again next time rather than kept as unverified.
    NotYet,
}

impl Named {
    fn note(&mut self, record: u64, decision: u64) {
        if self.verified.is_none_or(|(newest, _)| newest < decision) {
            self.unverified.push((decision, record));
        }
    }

    fn settle(&mut self, (decision, record): (u64, u64), verdict: Verdict) {
        match verdict {
            Verdict::NotYet => {}
            Verdict::Refused => self.unverified.retain(|&found| found != (decision, record)),
            Verdict::Verified => {
                self.unverified.retain(|&found| found != (decision, record));
                if self.verified.is_none_or(|(newest, _)| newest < decision) {
                    self.verified = Some((decision, record));
                    self.unverified.retain(|&(later, _)| later > decision);
                }
            }
        }
    }
}

/// The newest verified `(decision, record)` of the records `with` gives
/// access to, reading candidates newest first with `verify(record, decision)`
/// and stopping at the first that verifies. Only definite verdicts are kept.
/// `with` is called only between reads, so it can take a lock.
async fn newest<V, Fut>(
    mut with: impl FnMut(&mut dyn FnMut(&mut Named)),
    mut verify: V,
) -> Result<Option<(u64, u64)>>
where
    V: FnMut(u64, u64) -> Fut,
    Fut: Future<Output = Result<Verdict>>,
{
    let mut not_yet = Vec::new();
    loop {
        let mut next = Err(None);
        with(&mut |named| {
            next = (named.unverified.iter().copied())
                .filter(|found| !not_yet.contains(found))
                .max()
                .ok_or(named.verified);
        });
        let candidate = match next {
            Ok(candidate) => candidate,
            Err(newest) => return Ok(newest),
        };
        let verdict = verify(candidate.1, candidate.0).await?;
        if verdict == Verdict::NotYet {
            not_yet.push(candidate);
        }
        with(&mut |named| named.settle(candidate, verdict));
    }
}

impl Gate {
    fn scanned(&self) -> std::sync::MutexGuard<'_, Scanned> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Folds `sys.specs` up to `end`. A record that does not decode counts as
    /// admission's, so a newer build's admission record is not missed. Skip
    /// records are not specs and do not count. Nothing here reads `wl.admission.out`, which a namespace without
    /// admission does not have.
    async fn fold(&self, felix: &Felix, end: u64) -> Result<()> {
        loop {
            let from = self.scanned().to;
            if from >= end {
                return Ok(());
            }
            let (page, after) = felix.read_page(names::SPECS, from, end).await?;
            let mut scanned = self.scanned();
            // Another check folded these meanwhile.
            if scanned.to != from {
                continue;
            }
            for record in page {
                match SpecRecord::decode(&record.payload) {
                    Ok(found) => {
                        scanned.admission_runs |= found.id == names::ADMISSION;
                        if let Some(decision) = found.admitted
                            && !found.skipped
                        {
                            let named = scanned.named.entry(found.id).or_default();
                            named.note(record.offset, decision);
                        }
                    }
                    Err(_) => scanned.admission_runs = true,
                }
            }
            scanned.to = after;
        }
    }

    fn admission_runs(&self) -> bool {
        self.scanned().admission_runs
    }

    fn settle(&self, id: &str, candidate: (u64, u64), verdict: Verdict) {
        let mut scanned = self.scanned();
        let named = scanned.named.entry(id.to_owned()).or_default();
        named.settle(candidate, verdict);
    }

    /// The newest verified `(decision, record)` of workload `id`'s records.
    /// Reads only the records naming a later decision than the newest known
    /// to verify, newest first, so usually none.
    async fn newest(&self, felix: &Felix, id: &str) -> Result<Option<(u64, u64)>> {
        newest(
            |f| f(self.scanned().named.entry(id.to_owned()).or_default()),
            |record, decision| verifies(felix, Some(id), record, decision),
        )
        .await
    }

    /// Reads the records of each workload with more than [`MAX_UNVERIFIED`]
    /// unread, which leaves at most one of them.
    async fn compact(&self, felix: &Felix) -> Result<()> {
        let crowded: Vec<String> = self
            .scanned()
            .named
            .iter()
            .filter(|(_, named)| named.unverified.len() > MAX_UNVERIFIED)
            .map(|(id, _)| id.clone())
            .collect();
        for id in crowded {
            self.newest(felix, &id).await?;
        }
        Ok(())
    }
}

/// A spec record naming an accepted decision when a later record of the same
/// workload names a later one, as when `workloads` still points at the record
/// before a just-recorded change. Retried: the workload runs once `workloads`
/// catches up, and never runs this record.
#[derive(Debug)]
pub(crate) struct Superseded(String);

impl std::fmt::Display for Superseded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Superseded {}

/// Checks that spec record `record`, at `sys.specs` offset `at`, may run:
/// either admission's own record is nowhere on `sys.specs`, or it names a
/// decision on `wl.admission.out` that accepted exactly its spec, and no
/// record of the workload anywhere on `sys.specs` names a later decision that
/// verifies, so an old acceptance cannot run over a later change or stop,
/// whether it is appended again or `workloads` is pointed back at it. A
/// later decision recorded earlier is [`NeedsOperator`] with
/// [`OperatorReason::SpecUnadmitted`], as is any record that is not admitted,
/// including one naming a decision Felix no longer holds; one recorded
/// later is [`Superseded`].
pub(crate) async fn check_admitted(
    felix: &Felix,
    gate: &Gate,
    id: &str,
    at: u64,
    record: &SpecRecord,
) -> Result<()> {
    let specs_tail = felix.tail(names::SPECS).await?;
    gate.fold(felix, specs_tail).await?;
    if !admission::needs_decision(id, gate.admission_runs()) {
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
    // This record verifies, so only records naming a later decision are read.
    gate.settle(id, (decision, at), Verdict::Verified);
    gate.compact(felix).await?;
    let Some((latest, by)) = gate
        .newest(felix, id)
        .await?
        .filter(|&(latest, _)| latest > decision)
    else {
        return Ok(());
    };
    if by < at {
        return Err(unadmitted(format!(
            "the spec record at {}:{at} names decision {decision}, and the earlier record {by} \
             of workload {id} names the later decision {latest}",
            names::SPECS
        ))
        .into());
    }
    Err(Superseded(format!(
        "the spec record at {}:{at} names decision {decision}, and the later record {by} of \
         workload {id} names the later decision {latest}",
        names::SPECS
    ))
    .into())
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

/// The tail of `wl.admission.out`, 0 while it does not exist, as in a
/// namespace without admission, where no decision verifies.
async fn decisions_tail(felix: &Felix) -> Result<u64> {
    let stream = names::output_stream(names::ADMISSION);
    Ok(felix.tail_if_exists(&stream).await?.unwrap_or(0))
}

/// Whether the record at `record`, of workload `id` if one is given, names
/// `decision` and that decision on `wl.admission.out` accepted exactly its id
/// and spec. The tail is read after the record: the projector writes a record
/// only after reading its decision, so an honest record's decision is below
/// it, and one past it is [`Verdict::NotYet`], since it may yet be written.
async fn verifies(felix: &Felix, id: Option<&str>, record: u64, decision: u64) -> Result<Verdict> {
    let found = felix
        .read_range(names::SPECS, record, record.saturating_add(1))
        .await?
        .into_iter()
        .find(|r| r.offset == record)
        .with_context(|| format!("the folded record at {}:{record} is gone", names::SPECS))?;
    let Ok(found) = SpecRecord::decode(&found.payload) else {
        return Ok(Verdict::Refused);
    };
    if found.admitted != Some(decision) || id.is_some_and(|id| id != found.id) {
        return Ok(Verdict::Refused);
    }
    let Some(end) = admission::decision_end(decision, decisions_tail(felix).await?) else {
        return Ok(match decision {
            u64::MAX => Verdict::Refused,
            _ => Verdict::NotYet,
        });
    };
    let stream = names::output_stream(names::ADMISSION);
    let decisions = match felix.read_range(&stream, decision, end).await {
        Ok(decisions) => decisions,
        Err(error) if Trimmed::of(&error).is_some() => return Ok(Verdict::Refused),
        Err(error) => return Err(error),
    };
    let admits = decisions.into_iter().any(|r| {
        r.offset == decision && admission::admits(&r.payload, &found.id, &found.spec).is_ok()
    });
    Ok(if admits {
        Verdict::Verified
    } else {
        Verdict::Refused
    })
}

/// The records on `sys.specs` naming decisions, as the projector reads them.
#[derive(Default)]
struct Recorded {
    /// Every record, skip records included: the newest verified one is the
    /// projector's progress.
    all: Named,
    /// Spec records alone, for pointing `workloads` again.
    specs: Named,
}

impl Recorded {
    fn note(&mut self, offset: u64, record: &SpecRecord) {
        if let Some(decision) = record.admitted {
            self.all.note(offset, decision);
            if !record.skipped {
                self.specs.note(offset, decision);
            }
        }
    }
}

/// The newest verified `(decision, record)` among `named`, reading newest
/// first, so usually one record.
async fn newest_of(felix: &Felix, named: &mut Named) -> Result<Option<(u64, u64)>> {
    newest(
        |f| f(named),
        |record, decision| verifies(felix, None, record, decision),
    )
    .await
}

/// What the projector knows of `sys.specs` and `wl.admission.out`.
struct Projector {
    /// The tail of `sys.specs` as last read.
    specs_tail: u64,
    /// The next decision to look at. Every earlier one is recorded, skipped
    /// with a skip record, or rejected. It comes from `sys.specs` alone, the
    /// newest decision a verified record names, so a projector keeps nothing
    /// of its own across a restart or a failover and never records a
    /// decision another passed over.
    next: u64,
}

impl Projector {
    /// Resumes after the newest recorded decision and points `workloads` at
    /// the newest spec record again, in case a projector stopped between the
    /// two.
    async fn start(felix: &Felix) -> Result<Projector> {
        let specs_tail = felix.tail(names::SPECS).await?;
        let mut projector = Projector {
            specs_tail: 0,
            next: 0,
        };
        let mut recorded = projector.read(felix, specs_tail).await?;
        if let Some((decision, _)) = newest_of(felix, &mut recorded.all).await? {
            projector.next = decision.saturating_add(1);
        }
        if let Some((_, at)) = newest_of(felix, &mut recorded.specs).await? {
            let found = felix
                .read_range(names::SPECS, at, at.saturating_add(1))
                .await?
                .into_iter()
                .find(|r| r.offset == at)
                .with_context(|| format!("the spec record at {}:{at} is gone", names::SPECS))?;
            let record = SpecRecord::decode(&found.payload)?;
            let mut spec = record.spec;
            spec.record = Some(at);
            specs::project(felix, &record.id, &spec).await?;
        }
        Ok(projector)
    }

    /// Reads `sys.specs` from where it last read up to `end`.
    async fn read(&mut self, felix: &Felix, end: u64) -> Result<Recorded> {
        let mut recorded = Recorded::default();
        while self.specs_tail < end {
            let (page, after) = felix.read_page(names::SPECS, self.specs_tail, end).await?;
            for found in page {
                if let Ok(record) = SpecRecord::decode(&found.payload) {
                    recorded.note(found.offset, &record);
                }
            }
            self.specs_tail = after;
        }
        Ok(recorded)
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
                self.record(felix, settings, found.offset, decision).await?;
                // Recording may have found another projector further on.
                self.next = self.next.max(found.offset + 1);
            }
            self.next = self.next.max(after);
        }
        Ok(())
    }

    /// Records accepted decision `at` on `sys.specs`, or a skip record if its
    /// module does not match, unless another projector has recorded it or a
    /// later one. Each is a conditional append at the tail last read.
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
        let mut record = SpecRecord::new(&id, spec.clone());
        record.admitted = Some(at);
        // Admission cannot read the cache, so presence and size are checked
        // here. A spec whose module is wrong is never recorded, so it never
        // runs; the skip record says so for good, so no later projector
        // records it once the module turns up.
        if let Err(why) = module_matches(felix, &spec.module, module_bytes).await? {
            tracing::error!(workload = %id, decision = at, %why, "skipping an accepted spec");
            record.skipped = true;
        } else {
            let kind = StreamKind { replication_factor };
            admin::ensure_stream(settings, &spec.output, kind).await?;
            admin::ensure_stream(settings, &names::snapshot_stream(&id), kind).await?;
            if spec.merged() {
                admin::ensure_stream(settings, &names::merge_stream(&id), kind).await?;
            }
        }
        let payload = record.encode();
        let offset = loop {
            let sent = felix
                .publish_if(names::SPECS, vec![payload.clone()], self.specs_tail)
                .await;
            crate::faults::after_projection_sent();
            match sent? {
                ConditionalWrite::Written { offset } => {
                    self.specs_tail = offset + 1;
                    break offset;
                }
                ConditionalWrite::Refused { tail } => {
                    let mut recorded = self.read(felix, tail).await?;
                    if let Some((newest, _)) = newest_of(felix, &mut recorded.all).await?
                        && newest >= at
                    {
                        self.next = self.next.max(newest.saturating_add(1));
                        return Ok(());
                    }
                }
            }
        };
        if record.skipped {
            return Ok(());
        }
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
            let (found, record) = wait_for(felix, names::SPECS, specs_from, deadline, |payload| {
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
            if found.skipped {
                bail!(
                    "admission accepted the request at {decisions}:{decision_at}, and its \
                     projector skipped it at {}:{record} for good: the module did not match \
                     what was uploaded; submit it again",
                    names::SPECS
                );
            }
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
