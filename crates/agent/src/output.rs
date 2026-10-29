use anyhow::{Result, anyhow};
use felix_client::ConditionalWrite;
use felixd_core::output::{self, Reconciled, Record};

use crate::faults;
use crate::felix::Felix;
use crate::lease::Fence;
use crate::runner::{FailedOnStream, LostLease, Superseded, poison};

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
    tail: u64,
}

/// How [`record_failure`] ended.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Recorded {
    /// The failure record landed, or was already there: under this spec
    /// version, for this reason.
    Failed { spec_version: u64, reason: String },
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
    reason: &str,
) -> Result<Recorded> {
    let mut failures = 0;
    loop {
        if !fence.holds() {
            return Err(LostLease.into());
        }
        let tail = felix.tail(stream).await?;
        if let Some(last) = felix.last_record(stream, tail).await? {
            match output::decode(&last.payload) {
                Ok(found) if found.epoch() > epoch => {
                    return Ok(Recorded::Superseded(found.epoch()));
                }
                Ok(Record::Failure {
                    spec_version: found,
                    reason,
                    ..
                }) if found >= spec_version => {
                    return Ok(Recorded::Failed {
                        spec_version: found,
                        reason: String::from_utf8_lossy(reason).into_owned(),
                    });
                }
                _ => {}
            }
        }
        faults::before_failure_append();
        let record = output::encode_failure(epoch, spec_version, reason);
        match send(felix, stream, vec![record], tail, faults::Write::Failure).await {
            Ok(ConditionalWrite::Written { .. }) => {
                return Ok(Recorded::Failed {
                    spec_version,
                    reason: reason.to_owned(),
                });
            }
            Ok(ConditionalWrite::Refused { .. }) => {}
            Err(error) => {
                failures += 1;
                if failures >= MAX_ATTEMPTS {
                    return Err(error.context(format!("record the failure on {stream}")));
                }
                tracing::warn!(stream, %error, "failure record failed, checking the tail");
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
        spec_version: u64,
        tail: u64,
    ) -> Self {
        OutputLog {
            felix,
            stream,
            fence,
            spec_version,
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
    /// A failure record under this owner's spec, last before the tail or among
    /// the records past it, ends the claim instead.
    pub(crate) async fn claim(
        felix: &'a Felix,
        stream: &'a str,
        fence: &'a Fence,
        spec_version: u64,
        spec_record: u64,
        from: &mut Option<u64>,
    ) -> Result<OutputLog<'a>> {
        let start = match *from {
            Some(start) => start,
            None => felix.tail(stream).await?,
        };
        let mut log = OutputLog::new(felix, stream, fence, spec_version, start);
        if let Some(last) = felix.last_record(stream, start).await? {
            log.check_found(&last.payload, last.offset)?;
        }
        if from.is_none() {
            return Ok(log);
        }
        let mut failures = 0;
        loop {
            if !fence.holds() {
                return Err(LostLease.into());
            }
            faults::before_claim();
            let claim = vec![output::encode_claim_under(
                fence.epoch(),
                spec_record,
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
                ours |= found
                    == Record::Claim {
                        epoch: fence.epoch(),
                        node: fence.node().as_bytes(),
                        spec: Some(spec_record),
                    };
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
            reason,
            ..
        } = found
            && spec_version >= self.spec_version
        {
            return Err(FailedOnStream {
                spec_version,
                reason: String::from_utf8_lossy(reason).into_owned(),
            }
            .into());
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
            faults::before_append();
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
            faults::after_append_sent();
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
                reason,
            } if spec_version >= self.spec_version => Err(FailedOnStream {
                spec_version,
                reason,
            }
            .into()),
            Reconciled::Failed { .. } | Reconciled::Diverged => Err(poison(format!(
                "{} holds records at {}..{} that differ from this workload's output",
                self.stream, self.tail, seen_tail
            ))),
        }
    }
}
