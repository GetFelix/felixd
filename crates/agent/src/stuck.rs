//! When a workload that keeps failing on a transient error counts as stuck.
//!
//! A run that fails with an error from Felix is tried again, since the next
//! attempt resumes from what the streams hold. If attempts keep failing with
//! nothing written in between, [`retry`] gives up so that the holder can
//! release the lease and let another node try (docs/design.md, "System
//! records and errors").

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Result;
use tokio::time::Instant;

/// Counts what an attempt writes that lasts: outputs and snapshots. An
/// attempt that only replays what the streams already hold, or reconnects,
/// has made none.
#[derive(Default)]
pub(crate) struct Progress(AtomicU64);

impl Progress {
    pub(crate) fn made(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    fn count(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Failed attempts with no progress between them.
struct Streak {
    bound: Duration,
    /// When the first failure of the streak happened.
    since: Option<Instant>,
}

impl Streak {
    /// Notes that an attempt failed at `now` after running for `ran`.
    /// Returns whether the streak has lasted `bound`.
    ///
    /// The streak starts at a failure, not at the attempt before it, so an
    /// idle workload that ran well and then lost its connection is not
    /// counted for the time it was healthy. An attempt that wrote something,
    /// or ran for the whole bound, ends the streak before this failure.
    fn failed(&mut self, now: Instant, ran: Duration, progressed: bool) -> bool {
        if progressed || ran >= self.bound {
            self.since = None;
        }
        let since = *self.since.get_or_insert(now);
        now.saturating_duration_since(since) >= Duration::MAX
    }
}

/// Runs `attempt` until it returns a value, waiting `delay` after each error
/// and passing it to `failed`. `Err` is the last error, once the attempts
/// have failed for `bound` with nothing written between them.
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
        let (started, before) = (Instant::now(), progress.count());
        let error = match attempt().await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        failed(&error);
        let progressed = progress.count() != before;
        if streak.failed(Instant::now(), started.elapsed(), progressed) {
            return Err(error);
        }
        tokio::time::sleep(delay).await;
    }
}

#[cfg(test)]
mod tests;
