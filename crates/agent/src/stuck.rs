//! When a workload that keeps failing on a transient error counts as stuck.
//!
//! A run that fails with an error from Felix is tried again, since the next
//! attempt resumes from what the streams hold. If attempts keep failing with
//! no progress in between, [`retry`] gives up so that the holder can release
//! the lease and let another node try (docs/design.md, "Leases and failure
//! detection").

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use tokio::time::Instant;

/// What the attempts of one holder have got done. An attempt made progress
/// if it stepped an input beyond the furthest any earlier attempt stepped, or
/// appended a record to the output stream. Replaying inputs the stream
/// already covers, or reconnecting, is not progress.
#[derive(Default)]
pub(crate) struct Progress {
    made: AtomicU64,
    /// One past the furthest input offset stepped as new work.
    furthest: AtomicU64,
}

impl Progress {
    /// Notes a finished step at `offset`. `new` is whether the output stream
    /// did not already cover it, and `wrote` whether it appended records.
    pub(crate) fn stepped(&self, offset: u64, new: bool, wrote: bool) {
        let beyond = new && self.furthest.fetch_max(offset + 1, Ordering::Relaxed) <= offset;
        if beyond || wrote {
            self.made.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn count(&self) -> u64 {
        self.made.load(Ordering::Relaxed)
    }
}

/// Failed attempts with no progress between them. A bound of zero never
/// calls the workload stuck.
struct Streak {
    bound: Duration,
    /// When the first failure of the streak happened.
    since: Option<Instant>,
}

impl Streak {
    /// Notes that an attempt failed at `now`. Returns whether the streak has
    /// lasted `bound`.
    ///
    /// The streak starts at a failure, not at the attempt before it, so an
    /// idle workload that ran well and then lost its connection is not
    /// counted for the time it was healthy. An attempt that made progress
    /// ends the streak before this failure.
    fn failed(&mut self, now: Instant, progressed: bool) -> bool {
        if progressed {
            self.since = None;
        }
        let since = *self.since.get_or_insert(now);
        !self.bound.is_zero() && now.saturating_duration_since(since) >= self.bound
    }
}

/// Runs `attempt` until it returns a value, waiting `delay` after each error
/// and passing it to `failed`. `Err` is the last error, once the attempts
/// have failed for `bound` with no progress between them. A `bound` of zero
/// retries for ever.
pub(crate) async fn retry<T, Fut>(
    bound: Duration,
    delay: Duration,
    progress: &Progress,
    mut attempt: impl FnMut() -> Fut,
    mut failed: impl FnMut(&anyhow::Error),
) -> Result<T>
where
    Fut: Future<Output = Result<T>>,
{
    let mut streak = Streak { bound, since: None };
    loop {
        let before = progress.count();
        let error = match attempt().await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        failed(&error);
        let progressed = progress.count() != before;
        if streak.failed(Instant::now(), progressed) {
            return Err(error);
        }
        tokio::time::sleep(delay).await;
    }
}

#[cfg(test)]
mod tests;
