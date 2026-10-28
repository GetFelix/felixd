use anyhow::{Result, bail};
use felix_client::ConditionalWrite;

use crate::felix::Felix;

/// Consecutive failed attempts to write one batch before giving up.
const MAX_ATTEMPTS: u32 = 5;

/// Appends batches to a workload's output stream so that each lands once.
pub(crate) struct OutputLog<'a> {
    felix: &'a Felix,
    stream: &'a str,
    tail: u64,
}

impl<'a> OutputLog<'a> {
    pub(crate) fn new(felix: &'a Felix, stream: &'a str, tail: u64) -> Self {
        OutputLog {
            felix,
            stream,
            tail,
        }
    }

    /// The stream's next offset, as far as this owner knows.
    pub(crate) fn tail(&self) -> u64 {
        self.tail
    }

    /// Appends `batch` at the tail.
    ///
    /// A refused write, or one whose answer was lost, means someone else may
    /// have appended first. The stream is read at the tail we expected: records
    /// there that are exactly the start of our batch were written by an earlier
    /// owner of the same deterministic workload, so they are skipped, and
    /// anything else is a divergence we must not write over.
    pub(crate) async fn append(&mut self, mut batch: Vec<Vec<u8>>) -> Result<()> {
        let mut failures = 0;
        while !batch.is_empty() {
            let seen_tail = match self
                .felix
                .publish_if(self.stream, batch.clone(), self.tail)
                .await
            {
                Ok(ConditionalWrite::Written { offset }) if offset == self.tail => {
                    self.tail += batch.len() as u64;
                    return Ok(());
                }
                Ok(ConditionalWrite::Written { offset }) => {
                    bail!("{} wrote at {offset}, expected {}", self.stream, self.tail)
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
            bail!(
                "{} ends at {seen_tail}, before the {} already written",
                self.stream,
                self.tail
            );
        }
        if seen_tail == self.tail {
            return Ok(());
        }
        let existing = self
            .felix
            .read_range(self.stream, self.tail, seen_tail)
            .await?;
        let same = existing.len() <= batch.len()
            && existing
                .iter()
                .zip(batch.iter())
                .all(|(found, ours)| found.payload.as_ref() == ours.as_slice());
        if !same {
            bail!(
                "{} holds records at {}..{} that differ from this workload's output",
                self.stream,
                self.tail,
                seen_tail
            );
        }
        batch.drain(..existing.len());
        self.tail = seen_tail;
        Ok(())
    }
}
