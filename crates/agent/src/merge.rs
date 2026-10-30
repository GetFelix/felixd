//! Appends to a workload's merge log, fenced like its output stream: a claim
//! before anything else, then every append a `publish_if` at the tail.

use anyhow::{Result, anyhow, bail};
use felix_client::ConditionalWrite;
use felixd_core::merge::{self, Entry, Reconciled};

use crate::faults;
use crate::felix::Felix;
use crate::input::merge_unreadable;
use crate::lease::Fence;
use crate::runner::{LostLease, Superseded, poison};

/// Consecutive failed attempts at one write before giving up.
const MAX_ATTEMPTS: u32 = 5;

/// The merge log as this holder writes it.
pub(crate) struct MergeLog<'a> {
    felix: &'a Felix,
    stream: &'a str,
    fence: &'a Fence,
    tail: u64,
    /// Each input's next offset after the last record on the log.
    next: Vec<u64>,
}

impl<'a> MergeLog<'a> {
    /// Claims the log for this holder, which already holds the output
    /// stream's claim, and returns it at the tail after the claim.
    ///
    /// The claim goes at the tail just read, after checking the last record
    /// there, so a holder that finds a later epoch stops, and a later holder's
    /// claim landing first refuses this one. Entries an earlier holder
    /// appended before this claim stay: they are the order it chose, and this
    /// holder steps them like its own. Once this claim is on, an earlier
    /// holder's append is refused. A claim of this holder's already last,
    /// from an earlier run under the same lease, is not written again.
    pub(crate) async fn claim(
        felix: &'a Felix,
        stream: &'a str,
        fence: &'a Fence,
        inputs: usize,
    ) -> Result<MergeLog<'a>> {
        let mut failures = 0;
        loop {
            if !fence.holds() {
                return Err(LostLease.into());
            }
            let tail = felix.tail(stream).await?;
            let last = felix.last_record(stream, tail).await?;
            let decoded = match &last {
                Some(last) => Some(
                    merge::decode(&last.payload)
                        .map_err(|e| merge_unreadable(stream, last.offset, e))?,
                ),
                None => None,
            };
            let next = merge::cursor(decoded.as_ref(), inputs).map_err(poison)?;
            let mut log = MergeLog {
                felix,
                stream,
                fence,
                tail,
                next,
            };
            if let Some(found) = &decoded {
                if found.epoch() > fence.epoch() {
                    tracing::info!(stream, epoch = found.epoch(), "a later holder merged");
                    return Err(Superseded(found.epoch()).into());
                }
                // REVERT CHECK: claim again under the same epoch.
            }
            faults::before_merge_claim();
            let claim = merge::encode_claim(fence.epoch(), &log.next, fence.node());
            match send(felix, stream, vec![claim], tail, faults::Write::MergeClaim).await {
                Ok(ConditionalWrite::Written { offset }) if offset == tail => {
                    log.tail += 1;
                    return Ok(log);
                }
                Ok(ConditionalWrite::Written { offset }) => {
                    return Err(poison(format!(
                        "{stream} wrote a claim at {offset}, expected {tail}"
                    )));
                }
                // REVERT CHECK: a refused claim gives up.
                Ok(ConditionalWrite::Refused { .. }) => {
                    return Err(Superseded(fence.epoch()).into());
                }
                Err(error) => {
                    failures += 1;
                    if failures >= MAX_ATTEMPTS {
                        return Err(error.context(format!("claim {stream}")));
                    }
                    tracing::warn!(stream, %error, "merge claim failed, checking the tail");
                }
            }
        }
    }

    /// The log's next offset, as far as this holder knows.
    pub(crate) fn tail(&self) -> u64 {
        self.tail
    }

    /// Each input's next offset after the last record on the log.
    pub(crate) fn next(&self) -> &[u64] {
        &self.next
    }

    /// The most entries one [`Self::append`] may take.
    pub(crate) fn per_write(&self) -> usize {
        // REVERT CHECK: no limit on a write's entries.
        usize::MAX
    }

    /// Appends `entries`, at most [`Self::per_write`] of them, in one write and
    /// returns the offset each landed at, which is the offset of its step. A refused write, or one whose answer was lost, is
    /// settled by [`merge::reconcile`]: this holder's own records are taken as
    /// written, and a later holder's end the run.
    pub(crate) async fn append(&mut self, entries: &[Entry]) -> Result<Vec<u64>> {
        let mut next = self.next.clone();
        let mut batch: Vec<Vec<u8>> = entries
            .iter()
            .map(|&entry| merge::encode_entry(self.fence.epoch(), entry, &mut next))
            .collect();
        let mut landed = Vec::with_capacity(entries.len());
        let mut failures = 0;
        while !batch.is_empty() {
            if !self.fence.holds() {
                return Err(LostLease.into());
            }
            faults::before_merge();
            let write = faults::Write::Merge;
            let sent = send(self.felix, self.stream, batch.clone(), self.tail, write).await;
            let seen_tail = match sent {
                Ok(ConditionalWrite::Written { offset }) if offset == self.tail => {
                    let count = batch.len() as u64;
                    landed.extend(offset..offset + count);
                    self.tail += count;
                    break;
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
                    tracing::warn!(stream = self.stream, %error, "merge append failed, checking the tail");
                    self.felix.tail(self.stream).await?
                }
            };
            if seen_tail < self.tail {
                return Err(poison(format!(
                    "{} ends at {seen_tail}, before the {} already written",
                    self.stream, self.tail
                )));
            }
            let found = self
                .felix
                .read_range(self.stream, self.tail, seen_tail)
                .await?;
            let existing: Vec<&[u8]> = found.iter().map(|r| r.payload.as_ref()).collect();
            match merge::reconcile(self.fence.epoch(), &batch, &existing) {
                Reconciled::Skip(n) => {
                    landed.extend(found[..n].iter().map(|r| r.offset));
                    batch.drain(..n);
                    self.tail = seen_tail;
                }
                Reconciled::Superseded { epoch } => {
                    tracing::info!(stream = self.stream, epoch, "a later holder merged");
                    return Err(Superseded(epoch).into());
                }
                Reconciled::Diverged => {
                    return Err(anyhow!(
                        "{} holds records at {}..{seen_tail} this holder did not write",
                        self.stream,
                        self.tail
                    ));
                }
            }
        }
        self.next = next;
        faults::after_merge();
        Ok(landed)
    }
}

/// `publish_if`, or the dropped request, lost answer or refused oversized
/// write a test fault asks for.
async fn send(
    felix: &Felix,
    stream: &str,
    records: Vec<Vec<u8>>,
    at: u64,
    write: faults::Write,
) -> Result<ConditionalWrite> {
    if faults::merge_write_limit().is_some_and(|limit| records.len() > limit) {
        bail!(
            "injected: a write of {} merge records is too large",
            records.len()
        );
    }
    match faults::answer(write) {
        faults::Answer::Sent => felix.publish_if(stream, records, at).await,
        faults::Answer::Dropped => Err(anyhow!("injected: the request was dropped")),
        faults::Answer::Lost => {
            felix.publish_if(stream, records, at).await?;
            Err(anyhow!("injected: the answer was lost"))
        }
    }
}
