use anyhow::{Result, anyhow};
use felix_client::ConditionalWrite;
use felixd_core::output::{self, Reconciled, Record};

use crate::faults;
use crate::felix::Felix;
use crate::lease::Fence;
use crate::runner::{LostLease, Superseded, poison};

/// Consecutive failed attempts to write one batch before giving up.
const MAX_ATTEMPTS: u32 = 5;

/// Appends batches to a workload's output stream so that each lands once.
pub(crate) struct OutputLog<'a> {
    felix: &'a Felix,
    stream: &'a str,
    fence: &'a Fence,
    tail: u64,
}

impl<'a> OutputLog<'a> {
    pub(crate) fn new(felix: &'a Felix, stream: &'a str, fence: &'a Fence, tail: u64) -> Self {
        OutputLog {
            felix,
            stream,
            fence,
            tail,
        }
    }

    /// Claims the stream for this owner: appends a claim record with
    /// `publish_if`, so any later append of a lower epoch is refused, and
    /// returns a log at the tail after it.
    ///
    /// `from` is the tail read with the epoch floor when the lease was
    /// claimed. Writing there rather than at a fresh tail means that anything
    /// appended since, such as a later owner's claim, refuses this one. A
    /// refused claim reads the records past the tail: a later epoch means this
    /// owner is superseded, its own claim means an earlier attempt landed, and
    /// anything else is claimed after. Once the claim is on the stream `from`
    /// is cleared, and later resumes only check that no later owner wrote.
    pub(crate) async fn claim(
        felix: &'a Felix,
        stream: &'a str,
        fence: &'a Fence,
        from: &mut Option<u64>,
    ) -> Result<OutputLog<'a>> {
        let Some(start) = *from else {
            let log = OutputLog::new(felix, stream, fence, felix.tail(stream).await?);
            if let Some(last) = felix.last_record(stream, log.tail).await? {
                let epoch = log.decode(&last.payload, last.offset)?.epoch();
                if epoch > fence.epoch() {
                    return Err(Superseded(epoch).into());
                }
            }
            return Ok(log);
        };
        let mut log = OutputLog::new(felix, stream, fence, start);
        let mut failures = 0;
        loop {
            if !fence.holds() {
                return Err(LostLease.into());
            }
            faults::before_claim();
            let sent = felix
                .publish_if(stream, vec![output::encode_claim(fence.epoch())], log.tail)
                .await;
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
                let found = log.decode(&record.payload, record.offset)?;
                if found.epoch() > fence.epoch() {
                    tracing::info!(stream, epoch = found.epoch(), "a later owner claimed first");
                    return Err(Superseded(found.epoch()).into());
                }
                ours |= found
                    == Record::Claim {
                        epoch: fence.epoch(),
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
            let sent = self
                .felix
                .publish_if(self.stream, batch.clone(), self.tail)
                .await;
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
            Reconciled::Diverged => Err(poison(format!(
                "{} holds records at {}..{} that differ from this workload's output",
                self.stream, self.tail, seen_tail
            ))),
        }
    }
}
