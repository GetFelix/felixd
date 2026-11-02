use std::cell::Cell;

use super::*;

const BOUND: Duration = Duration::from_secs(10);
const DELAY: Duration = Duration::from_secs(1);

fn sink(_: &anyhow::Error) {}

#[tokio::test(start_paused = true)]
async fn attempts_that_keep_failing_with_no_progress_are_stuck_after_the_bound() {
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

/// A bound of zero is the switch for a rolling upgrade: nothing is ever
/// stuck.
#[tokio::test(start_paused = true)]
async fn a_zero_bound_retries_for_ever() {
    let attempts = Cell::new(0);
    let progress = Progress::default();
    let result = retry(
        Duration::ZERO,
        DELAY,
        &progress,
        || async {
            attempts.set(attempts.get() + 1);
            if attempts.get() == 1000 {
                return Ok(());
            }
            anyhow::bail!("again")
        },
        sink,
    )
    .await;
    assert!(result.is_ok());
}

/// A workload that steps a new input between failures is slow, not stuck,
/// however long it goes on. This is the shape of an attempt that fails after
/// each append.
#[tokio::test(start_paused = true)]
async fn an_attempt_that_steps_a_new_input_keeps_the_workload_unstuck() {
    let start = Instant::now();
    let attempts = Cell::new(0u64);
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
            progress.stepped(attempts.get(), true, false);
            anyhow::bail!("failed after a step")
        },
        sink,
    )
    .await;
    assert!(result.is_ok());
    assert!(start.elapsed() > BOUND * 5);
}

/// An attempt that appended is progress even where the stream already
/// covered its input, as when the rest of a partly written step lands.
#[tokio::test(start_paused = true)]
async fn an_append_is_progress_even_for_an_input_already_covered() {
    let attempts = Cell::new(0);
    let progress = Progress::default();
    let result = retry(
        BOUND,
        DELAY,
        &progress,
        || async {
            attempts.set(attempts.get() + 1);
            if attempts.get() == 50 {
                return Ok(());
            }
            progress.stepped(3, false, true);
            anyhow::bail!("failed after an append")
        },
        sink,
    )
    .await;
    assert!(result.is_ok());
}

/// A long replay or restore that fails on its first new append steps the
/// same inputs every time. Only the first attempt got anywhere, so the
/// workload is stuck a bound after that.
#[tokio::test(start_paused = true)]
async fn replaying_the_same_inputs_every_attempt_is_not_progress() {
    let start = Instant::now();
    let progress = Progress::default();
    let result: Result<()> = retry(
        BOUND,
        DELAY,
        &progress,
        || async {
            // Inputs 0 to 999 are covered by the stream, so they are
            // replays. Input 1000 is new, and its append fails.
            for offset in 0..1000 {
                progress.stepped(offset, false, false);
            }
            progress.stepped(1000, true, false);
            anyhow::bail!("the append failed")
        },
        sink,
    )
    .await;
    result.unwrap_err();
    // The first attempt stepped input 1000, which counts. Its failure
    // starts the streak, and the bound passes 10 s later.
    assert_eq!(start.elapsed(), BOUND);
}

/// A new input that is the same one every attempt, such as a workload that
/// steps an input and fails before it can append, is not progress after the
/// first time either.
#[tokio::test(start_paused = true)]
async fn stepping_the_same_new_input_again_is_not_progress() {
    let start = Instant::now();
    let progress = Progress::default();
    let result: Result<()> = retry(
        BOUND,
        DELAY,
        &progress,
        || async {
            progress.stepped(7, true, false);
            anyhow::bail!("failed before the append")
        },
        sink,
    )
    .await;
    result.unwrap_err();
    assert_eq!(start.elapsed(), BOUND);
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
            // Failing for most of the bound, then one attempt that wrote.
            if attempts.get() == 8 {
                progress.stepped(0, true, true);
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

/// A connection that comes back and drops again is not progress.
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

/// A quiet workload makes no progress, so only the time an attempt ran
/// tells it from a stuck one. Failures hours apart, with a leader change
/// each time, are not a streak.
#[tokio::test(start_paused = true)]
async fn an_idle_workload_failing_hours_apart_is_not_stuck() {
    let hour = Duration::from_secs(3600);
    let start = Instant::now();
    let attempts = Cell::new(0);
    let progress = Progress::default();
    let result = retry(
        BOUND,
        DELAY,
        &progress,
        || async {
            attempts.set(attempts.get() + 1);
            // Idle for an hour, then the connection drops, five times, and
            // then the attempt ends the run.
            tokio::time::sleep(hour).await;
            if attempts.get() == 5 {
                return Ok(());
            }
            anyhow::bail!("the input stream closed")
        },
        sink,
    )
    .await;
    assert!(result.is_ok());
    assert!(start.elapsed() > hour * 5);
}

/// After a long healthy attempt fails, the workload is stuck only if it goes
/// on failing for the whole bound from there.
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
