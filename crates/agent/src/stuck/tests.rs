use std::cell::Cell;

use super::*;

const BOUND: Duration = Duration::from_secs(10);
const DELAY: Duration = Duration::from_secs(1);

fn sink(_: &anyhow::Error) {}

#[tokio::test(start_paused = true)]
async fn attempts_that_keep_failing_with_nothing_written_are_stuck_after_the_bound() {
    let start = Instant::now();
    let attempts = Cell::new(0);
    let progress = Progress::default();
    let result: Result<()> = retry(
        BOUND,
        DELAY,
        &progress,
        || async {
            attempts.set(attempts.get() + 1);
            anyhow::bail!("attempt {}", attempts.get())
        },
        sink,
    )
    .await;
    assert_eq!(result.unwrap_err().to_string(), "attempt 11");
    assert_eq!(start.elapsed(), BOUND);
    assert_eq!(attempts.get(), 11);
}

#[tokio::test(start_paused = true)]
async fn a_value_ends_the_retries() {
    let attempts = Cell::new(0);
    let progress = Progress::default();
    let seen = Cell::new(0);
    let result = retry(
        BOUND,
        DELAY,
        &progress,
        || async {
            attempts.set(attempts.get() + 1);
            if attempts.get() < 4 {
                anyhow::bail!("not yet")
            }
            Ok(attempts.get())
        },
        |_| seen.set(seen.get() + 1),
    )
    .await;
    assert_eq!(result.unwrap(), 4);
    assert_eq!(seen.get(), 3, "each error is passed on");
}

/// A workload that writes between failures is slow, not stuck, however long
/// it goes on.
#[tokio::test(start_paused = true)]
async fn an_attempt_that_wrote_something_keeps_the_workload_unstuck() {
    let start = Instant::now();
    let attempts = Cell::new(0);
    let progress = Progress::default();
    let result = retry(
        BOUND,
        DELAY,
        &progress,
        || async {
            attempts.set(attempts.get() + 1);
            if attempts.get() == 100 {
                return Ok(());
            }
            progress.made();
            anyhow::bail!("failed after writing")
        },
        sink,
    )
    .await;
    assert!(result.is_ok());
    assert!(start.elapsed() > BOUND * 5);
}

#[tokio::test(start_paused = true)]
async fn progress_starts_the_count_again() {
    let start = Instant::now();
    let attempts = Cell::new(0);
    let progress = Progress::default();
    let result: Result<()> = retry(
        BOUND,
        DELAY,
        &progress,
        || async {
            attempts.set(attempts.get() + 1);
            // Failing for most of the bound, then one attempt that writes.
            if attempts.get() == 8 {
                progress.made();
            }
            anyhow::bail!("attempt {}", attempts.get())
        },
        sink,
    )
    .await;
    result.unwrap_err();
    // The write at attempt 8 (7 s in) restarts the streak at that failure.
    assert_eq!(start.elapsed(), Duration::from_secs(7) + BOUND);
}

/// An attempt that ran for the whole bound was not stuck, so the time it
/// spent before failing is not part of the streak.
#[tokio::test(start_paused = true)]
async fn a_long_healthy_attempt_does_not_count_toward_the_streak() {
    let start = Instant::now();
    let attempts = Cell::new(0);
    let progress = Progress::default();
    let result: Result<()> = retry(
        BOUND,
        DELAY,
        &progress,
        || async {
            attempts.set(attempts.get() + 1);
            // Two quick failures, then one attempt that holds on for the
            // whole bound before it fails too.
            if attempts.get() == 3 {
                tokio::time::sleep(BOUND).await;
            }
            anyhow::bail!("attempt {}", attempts.get())
        },
        sink,
    )
    .await;
    result.unwrap_err();
    // The third attempt starts at 2 s and fails at 12 s, which starts a new
    // streak. Counted from the first failure, the workload would be stuck
    // there.
    assert_eq!(start.elapsed(), Duration::from_secs(12) + BOUND);
}

/// A short idle attempt is not progress either: a connection that comes
/// back and drops again is still failing.
#[tokio::test(start_paused = true)]
async fn reconnecting_and_failing_again_is_not_progress() {
    let start = Instant::now();
    let progress = Progress::default();
    let result: Result<()> = retry(
        BOUND,
        DELAY,
        &progress,
        || async {
            tokio::time::sleep(Duration::from_secs(2)).await;
            anyhow::bail!("the input stream closed")
        },
        sink,
    )
    .await;
    result.unwrap_err();
    // Attempts fail at 2, 5, 8, 11 and 14 s. The streak starts at the first
    // and reaches the bound at the fifth.
    assert_eq!(start.elapsed(), Duration::from_secs(14));
}
