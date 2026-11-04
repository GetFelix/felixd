//! Restart intensity: whether the last claim on an output stream takes its
//! spec record past the takeovers the spec allows.
//!
//! The count is a function of the stream's records and their append times,
//! which every node reads alike, so every node agrees on which claim is over
//! the limit. Nothing else holds the count, so a crash cannot lose or repeat
//! one. The rules are in `docs/design.md`, under System records and errors.

use crate::output::{self, Record};
use crate::spec::RestartIntensity;

/// Counts takeovers walking back from the end of an output stream. Feed it
/// records newest first with [`Takeovers::older`] until it gives a verdict,
/// or call [`Takeovers::verdict`] when the stream has nothing older.
#[derive(Clone, Debug)]
pub struct Takeovers {
    spec: u64,
    limit: RestartIntensity,
    /// Where the window starts, in microseconds: set by the newest claim.
    since: Option<u64>,
    claims: u32,
    /// The epoch of the oldest claim counted so far.
    lowest_epoch: u64,
}

/// What the walk found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// The holders of the last `takeovers` claims before the newest one made
    /// no progress, and that is more than the spec allows.
    Over {
        takeovers: u32,
    },
    Within,
}

impl Takeovers {
    /// A count for spec record `spec` under `limit`.
    pub fn new(spec: u64, limit: RestartIntensity) -> Takeovers {
        Takeovers {
            spec,
            limit,
            since: None,
            claims: 0,
            lowest_epoch: u64::MAX,
        }
    }

    /// Takes the next older record, appended at `at_micros` by the broker's
    /// clock. `Some` once the verdict is known.
    pub fn older(&mut self, record: &[u8], at_micros: Option<u64>) -> Option<Verdict> {
        let decoded = output::decode(record);
        // Notes after the newest claim, such as this holder's own degraded
        // record, are passed over before the window is known.
        if self.since.is_none() && is_note(&decoded) {
            return None;
        }
        // No append time is no evidence of when the takeover happened.
        let Some(at) = at_micros else {
            return Some(self.verdict());
        };
        let since = *self
            .since
            .get_or_insert_with(|| at.saturating_sub(self.limit.window_ms.saturating_mul(1000)));
        if at < since {
            return Some(self.verdict());
        }
        match decoded {
            Ok(Record::Claim {
                epoch,
                spec: Some(spec),
                ..
            }) if spec == self.spec => {
                // One takeover per epoch, whatever lands twice.
                if epoch < self.lowest_epoch {
                    self.lowest_epoch = epoch;
                    self.claims += 1;
                }
                let verdict = self.verdict();
                matches!(verdict, Verdict::Over { .. }).then_some(verdict)
            }
            ref found if is_note(found) => None,
            // Progress, a claim of another spec record or none, a trim, or a
            // record this build cannot read ends the run of takeovers.
            _ => Some(self.verdict()),
        }
    }

    /// The verdict from the records taken so far.
    pub fn verdict(&self) -> Verdict {
        // The oldest claim counted is the run's start; each one after it is a
        // takeover from a holder that got nowhere.
        let takeovers = self.claims.saturating_sub(1);
        match takeovers > self.limit.max_takeovers {
            true => Verdict::Over { takeovers },
            false => Verdict::Within,
        }
    }
}

/// Records no step produced and that say nothing about progress: they are
/// passed over.
fn is_note(record: &Result<Record<'_>, crate::DecodeError>) -> bool {
    matches!(
        record,
        Ok(Record::Failure { .. }
            | Record::NeedsOperator { .. }
            | Record::Degraded { .. }
            | Record::Divergence { .. })
    )
}

#[cfg(test)]
mod tests;
