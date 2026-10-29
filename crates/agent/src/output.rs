use anyhow::{Result, anyhow};
use felix_client::ConditionalWrite;
use felixd_core::class;
use felixd_core::output::{self, Reconciled, Record};

use crate::faults;
use crate::felix::Felix;
use crate::lease::Fence;
use crate::runner::{
    FailedOnStream, LostLease, NeedsOperator, NeedsOperatorOnStream, Superseded, poison,
};
use crate::specs;

/// Consecutive failed attempts to write one batch before giving up.
const MAX_ATTEMPTS: u32 = 5;

/// Appends batches to a workload's output stream so that each lands once.
pub(crate) struct OutputLog<'a> {
    felix: &'a Felix,
    stream: &'a str,
    fence: &'a Fence,
    /// The version of the spec this owner runs. A failure record under it,
    /// or a later one, ends the run.
    spec_version: u64,
    /// The spec record it runs. A needs-operator record under it ends the
    /// claim.
    spec_record: u64,
    tail: u64,
}

/// How [`record_failure`] or [`record_needs_operator`] ended.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Recorded {
    /// The failure record landed, or was already there: under this spec
    /// version, for this reason.
    Failed {
        spec_version: u64,
        failure: class::Failure,
    },
    /// The needs-operator record landed, or one for the same spec record and
    /// reason was already there.
    NeedsOperator,
    /// A later owner has written, so this node must not fail the workload.
    Superseded(u64),
}

/// Appends a failure record at `epoch` under `spec_version` at the tail,
/// unless the last record there is from a later epoch or already a failure.
/// `epoch` is the holder's own, or the later one it found when epochs went
/// backwards.
///
/// Records below a later owner's claim are all earlier, so a record that
/// lands where the last one was no later than `epoch` lands before any
/// later owner has begun. That owner then finds the failure and
/// stops, and the spec's `failed` follows from the record.
pub(crate) async fn record_failure(
    felix: &Felix,
    stream: &str,
    fence: &Fence,
    epoch: u64,
    spec_version: u64,
    failure: &class::Failure,
) -> Result<Recorded> {
    let record = output::encode_failure(epoch, spec_version, failure.class, &failure.detail);
    let ours = Recorded::Failed {
        spec_version,
        failure: failure.clone(),
    };
    append_last(
        felix,
        stream,
        fence,
        epoch,
        record,
        ours,
        |found| match found {
            Record::Failure {
                spec_version: found,
                class,
                reason,
                ..
            } if *found >= spec_version => Some(Recorded::Failed {
                spec_version: *found,
                failure: class::Failure {
                    class: *class,
                    detail: String::from_utf8_lossy(reason).into_owned(),
                },
            }),
            _ => None,
        },
    )
    .await
}

/// Appends a record at `epoch` saying that spec record `spec` needs an
/// operator, fenced like [`record_failure`]. One already last for the same
/// spec record and reason is not written again, and a failure last under
/// `spec_version` or a later one ends the run instead.
pub(crate) async fn record_needs_operator(
    felix: &Felix,
    stream: &str,
    fence: &Fence,
    epoch: u64,
    (spec_version, spec): (u64, u64),
    problem: &NeedsOperator,
) -> Result<Recorded> {
    let record = output::encode_needs_operator(epoch, spec, problem.reason, &problem.detail);
    append_last(
        felix,
        stream,
        fence,
        epoch,
        record,
        Recorded::NeedsOperator,
        |found| needs_operator_found(found, (spec_version, spec), problem.reason),
    )
    .await
}

/// What [`record_needs_operator`] makes of `found`, the last record: a
/// failure under `spec_version` or later is the outcome, and a needs-operator
/// record for spec record `spec` and `reason` means it is already written.
/// One for another reason is not, so each reason is written once.
fn needs_operator_found(
    found: &Record<'_>,
    (spec_version, spec): (u64, u64),
    reason: class::OperatorReason,
) -> Option<Recorded> {
    match found {
        Record::Failure {
            spec_version: found,
            class,
            reason,
            ..
        } if *found >= spec_version => Some(Recorded::Failed {
            spec_version: *found,
            failure: class::Failure {
                class: *class,
                detail: String::from_utf8_lossy(reason).into_owned(),
            },
        }),
        found => {
            (found.needs_operator_under(spec) == Some(reason)).then_some(Recorded::NeedsOperator)
        }
    }
}

/// Appends `record`, the last of this run, at the tail unless the last record
/// there is from a later epoch or is what `already` recognises. Returns
/// `ours` once it lands.
async fn append_last(
    felix: &Felix,
    stream: &str,
    fence: &Fence,
    epoch: u64,
    record: Vec<u8>,
    ours: Recorded,
    already: impl Fn(&Record<'_>) -> Option<Recorded>,
) -> Result<Recorded> {
    let mut failures = 0;
    loop {
        if !fence.holds() {
            return Err(LostLease.into());
        }
        let tail = felix.tail(stream).await?;
        if let Some(last) = felix.last_record(stream, tail).await?
            && let Ok(found) = output::decode(&last.payload)
        {
            if found.epoch() > epoch {
                return Ok(Recorded::Superseded(found.epoch()));
            }
            if let Some(recorded) = already(&found) {
                return Ok(recorded);
            }
        }
        faults::before_failure_append().await;
        match send(
            felix,
            stream,
            vec![record.clone()],
            tail,
            faults::Write::Failure,
        )
        .await
        {
            Ok(ConditionalWrite::Written { .. }) => return Ok(ours),
            Ok(ConditionalWrite::Refused { .. }) => {}
            Err(error) => {
                failures += 1;
                if failures >= MAX_ATTEMPTS {
                    return Err(error.context(format!("append the last record on {stream}")));
                }
                tracing::warn!(stream, %error, "last record failed, checking the tail");
            }
        }
    }
}

/// `publish_if`, or the dropped request or lost answer a test fault asks for.
async fn send(
    felix: &Felix,
    stream: &str,
    records: Vec<Vec<u8>>,
    at: u64,
    write: faults::Write,
) -> Result<ConditionalWrite> {
    match faults::answer(write) {
        faults::Answer::Sent => felix.publish_if(stream, records, at).await,
        faults::Answer::Dropped => Err(anyhow!("injected: the request was dropped")),
        faults::Answer::Lost => {
            felix.publish_if(stream, records, at).await?;
            Err(anyhow!("injected: the answer was lost"))
        }
    }
}

impl<'a> OutputLog<'a> {
    pub(crate) fn new(
        felix: &'a Felix,
        stream: &'a str,
        fence: &'a Fence,
        (spec_version, spec_record): (u64, u64),
        tail: u64,
    ) -> Self {
        OutputLog {
            felix,
            stream,
            fence,
            spec_version,
            spec_record,
            tail,
        }
    }

    /// Claims the stream for this owner: appends a claim record naming the
    /// spec record it runs with `publish_if`, so any later append of a lower epoch is refused, and
    /// returns a log at the tail after it.
    ///
    /// `from` is the tail read with the epoch floor when the lease was
    /// claimed. Writing there rather than at a fresh tail means that anything
    /// appended since, such as a later owner's claim, refuses this one. A
    /// refused claim reads the records past the tail: a later epoch means this
    /// owner is superseded, its own claim means an earlier attempt landed, and
    /// anything else is claimed after. Once the claim is on the stream `from`
    /// is cleared, and later resumes only check that no later owner wrote.
    /// A failure record under this owner's spec, or a needs-operator record
    /// under its spec record, last before the tail or among the records past
    /// it, ends the claim instead, so that each is acted on once.
    pub(crate) async fn claim(
        felix: &'a Felix,
        stream: &'a str,
        fence: &'a Fence,
        (spec_version, spec_record): (u64, u64),
        from: &mut Option<u64>,
    ) -> Result<OutputLog<'a>> {
        let start = match *from {
            Some(start) => start,
            None => felix.tail(stream).await?,
        };
        let mut log = OutputLog::new(felix, stream, fence, (spec_version, spec_record), start);
        if let Some(last) = felix.last_record(stream, start).await? {
            log.check_found(&last.payload, last.offset)?;
        }
        if from.is_none() {
            return Ok(log);
        }
        // The highest spec claimed before this claim, which it carries
        // forward even if its owner then refuses its own older spec.
        let mut floor = specs::claim_floor(felix, stream, start).await?.unwrap_or(0);
        let mut failures = 0;
        loop {
            if !fence.holds() {
                return Err(LostLease.into());
            }
            faults::before_claim().await;
            let claim = vec![output::encode_claim_under(
                fence.epoch(),
                spec_record,
                floor,
                fence.node(),
            )];
            let sent = send(felix, stream, claim, log.tail, faults::Write::Claim).await;
            let seen_tail = match sent {
                Ok(ConditionalWrite::Written { offset }) if offset == log.tail => {
                    log.tail += 1;
                    *from = None;
                    return Ok(log);
                }
                Ok(ConditionalWrite::Written { offset }) => {
                    return Err(poison(format!(
                        "{stream} wrote a claim at {offset}, expected {}",
                        log.tail
                    )));
                }
                Ok(ConditionalWrite::Refused { tail }) => tail,
                Err(error) => {
                    failures += 1;
                    if failures >= MAX_ATTEMPTS {
                        return Err(error.context(format!("claim {stream}")));
                    }
                    tracing::warn!(stream, %error, "claim failed, checking the tail");
                    felix.tail(stream).await?
                }
            };
            if seen_tail < log.tail {
                return Err(poison(format!(
                    "{stream} ends at {seen_tail}, before the {} already read",
                    log.tail
                )));
            }
            let mut ours = false;
            for record in felix.read_range(stream, log.tail, seen_tail).await? {
                let found = log.check_found(&record.payload, record.offset)?;
                if let Record::Claim {
                    floor: Some(claimed),
                    ..
                } = found
                {
                    floor = floor.max(claimed);
                }
                floor = floor.max(found.needs_operator_spec().unwrap_or(0));
                // An earlier attempt that landed may carry an older floor.
                ours |= matches!(found, Record::Claim { epoch, node, spec, .. }
                    if epoch == fence.epoch()
                        && node == fence.node().as_bytes()
                        && spec == Some(spec_record));
            }
            log.tail = seen_tail;
            if ours {
                *from = None;
                return Ok(log);
            }
        }
    }

    fn decode<'r>(&self, record: &'r [u8], offset: u64) -> Result<Record<'r>> {
        output::decode(record)
            .map_err(|e| poison(format!("output at {}:{offset}: {e}", self.stream)))
    }

    /// Decodes a record found while claiming. One from a later epoch means
    /// this owner is superseded; a failure under this owner's spec, or a
    /// later one, means the workload has failed. Earlier failures belong to
    /// a run before the spec was written again, and are passed over.
    fn check_found<'r>(&self, record: &'r [u8], offset: u64) -> Result<Record<'r>> {
        let found = self.decode(record, offset)?;
        if found.epoch() > self.fence.epoch() {
            tracing::info!(
                stream = self.stream,
                epoch = found.epoch(),
                "a later owner claimed first"
            );
            return Err(Superseded(found.epoch()).into());
        }
        if let Record::Failure {
            spec_version,
            class,
            reason,
            ..
        } = found
            && spec_version >= self.spec_version
        {
            return Err(FailedOnStream {
                spec_version,
                failure: class::Failure {
                    class,
                    detail: String::from_utf8_lossy(reason).into_owned(),
                },
            }
            .into());
        }
        if let Record::NeedsOperator { reason, detail, .. } = found
            && found.needs_operator_under(self.spec_record).is_some()
        {
            let detail = String::from_utf8_lossy(detail).into_owned();
            return Err(NeedsOperatorOnStream(NeedsOperator::new(reason, detail)).into());
        }
        Ok(found)
    }

    /// The stream's next offset, as far as this owner knows.
    pub(crate) fn tail(&self) -> u64 {
        self.tail
    }

    /// Appends `batch` at the tail.
    ///
    /// A refused write, or one whose answer was lost, means someone else may
    /// have appended first. The stream is read at the tail we expected and
    /// [`output::reconcile`] decides: skip records an earlier owner (or this
    /// one) already wrote, stop if a later owner wrote, resume if an earlier
    /// owner got further than this batch, and fail on anything else.
    pub(crate) async fn append(&mut self, mut batch: Vec<Vec<u8>>) -> Result<()> {
        let mut failures = 0;
        while !batch.is_empty() {
            if !self.fence.holds() {
                return Err(LostLease.into());
            }
            faults::before_append().await;
            let sent = match faults::append_prefix(&batch) {
                Some(prefix) => self
                    .felix
                    .publish_if(self.stream, prefix, self.tail)
                    .await
                    .and_then(|_| Err(anyhow!("injected: the rest of the batch was lost"))),
                None => {
                    let write = faults::Write::Append;
                    send(self.felix, self.stream, batch.clone(), self.tail, write).await
                }
            };
            faults::after_append_sent().await;
            let seen_tail = match sent {
                Ok(ConditionalWrite::Written { offset }) if offset == self.tail => {
                    self.tail += batch.len() as u64;
                    return Ok(());
                }
                Ok(ConditionalWrite::Written { offset }) => {
                    return Err(poison(format!(
                        "{} wrote at {offset}, expected {}",
                        self.stream, self.tail
                    )));
                }
                Ok(ConditionalWrite::Refused { tail }) => tail,
                Err(error) => {
                    failures += 1;
                    if failures >= MAX_ATTEMPTS {
                        return Err(error.context(format!("append to {}", self.stream)));
                    }
                    tracing::warn!(stream = self.stream, %error, "append failed, checking the tail");
                    self.felix.tail(self.stream).await?
                }
            };
            self.skip_written(&mut batch, seen_tail).await?;
        }
        Ok(())
    }

    async fn skip_written(&mut self, batch: &mut Vec<Vec<u8>>, seen_tail: u64) -> Result<()> {
        if seen_tail < self.tail {
            return Err(poison(format!(
                "{} ends at {seen_tail}, before the {} already written",
                self.stream, self.tail
            )));
        }
        if seen_tail == self.tail {
            return Ok(());
        }
        let existing = self
            .felix
            .read_range(self.stream, self.tail, seen_tail)
            .await?;
        let found: Vec<&[u8]> = existing.iter().map(|r| r.payload.as_ref()).collect();
        match output::reconcile(self.fence.epoch(), batch, &found) {
            Reconciled::Skip(n) => {
                batch.drain(..n);
                self.tail = seen_tail;
                Ok(())
            }
            Reconciled::Superseded { epoch } => {
                tracing::info!(
                    stream = self.stream,
                    epoch,
                    "a later owner wrote to the output stream"
                );
                Err(Superseded(epoch).into())
            }
            Reconciled::Behind => Err(anyhow!(
                "an earlier owner wrote past {} on {}, resuming",
                self.tail,
                self.stream
            )),
            Reconciled::Failed {
                spec_version,
                class,
                reason,
            } if spec_version >= self.spec_version => Err(FailedOnStream {
                spec_version,
                failure: class::Failure {
                    class,
                    detail: reason,
                },
            }
            .into()),
            Reconciled::Failed { .. } | Reconciled::Diverged => Err(poison(format!(
                "{} holds records at {}..{} that differ from this workload's output",
                self.stream, self.tail, seen_tail
            ))),
        }
    }
}

#[cfg(test)]
mod tests;
