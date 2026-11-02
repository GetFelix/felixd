//! Deleting what a trim record names from the streams a workload owns.
//!
//! A failed deletion is never the workload's failure: the trim record is
//! already on `.out`, and the next snapshot (or the next holder) deletes again.

use std::future::Future;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use felix_client::{BrokerError, ConditionalTrim, ErrorCode, TrimError};

use crate::felix::Felix;

/// How long a node that found Felix unable to trim waits before asking again,
/// so a fleet that has not finalized `head_trim` costs one call per interval,
/// and one that has just finalized it is picked up soon after.
const PAUSE: Duration = Duration::from_secs(10);

static PAUSED_UNTIL: Mutex<Option<Instant>> = Mutex::new(None);
static WARNED_NOT_ENABLED: AtomicBool = AtomicBool::new(false);
static WARNED_NO_GRANT: AtomicBool = AtomicBool::new(false);

/// What came of trimming one stream.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Deletion {
    /// Felix hides everything below the floor now. `landed` is where the
    /// trim record went, `None` when the floor already stood there.
    Done { landed: Option<u64> },
    /// Not done, for now. The next snapshot tries again.
    Later,
    /// Felix will not trim until an operator acts, and has been told so.
    Unavailable,
}

/// Whether to try deleting now. False for a short while after Felix said
/// it cannot trim.
pub(crate) fn may_delete() -> bool {
    let paused = PAUSED_UNTIL.lock().unwrap_or_else(|e| e.into_inner());
    paused.is_none_or(|until| Instant::now() >= until)
}

/// Hides the records of `stream` below `before`. `tail` is the stream's next
/// offset as far as the caller knows; the trim record lands there.
pub(crate) async fn trim_stream(felix: &Felix, stream: &str, before: u64, tail: u64) -> Deletion {
    let result = settle(tail, |expected| felix.trim_before(stream, before, expected)).await;
    match result {
        Ok(Settled::Trimmed(landed)) => Deletion::Done { landed },
        Ok(Settled::TailMoved) => {
            tracing::debug!(
                stream,
                before,
                "the tail kept moving, trimming at the next snapshot"
            );
            Deletion::Later
        }
        Err(error) => match classify(&error) {
            Failure::NotEnabled => {
                if !WARNED_NOT_ENABLED.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        stream,
                        %error,
                        "Felix cannot trim yet, so keep_snapshots records its floors and deletes \
                         nothing; finalize the head_trim fleet feature to let it delete"
                    );
                }
                pause();
                Deletion::Unavailable
            }
            Failure::NoGrant => {
                if !WARNED_NO_GRANT.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        stream,
                        %error,
                        "this node's token may not trim streams, so keep_snapshots deletes \
                         nothing; grant stream.trim (stream.manage includes it)"
                    );
                }
                pause();
                Deletion::Unavailable
            }
            Failure::TimedOut => {
                // The trim may have landed. The next one asks at the tail it
                // finds and learns which.
                tracing::info!(
                    stream,
                    before,
                    "a trim timed out, trimming at the next snapshot"
                );
                Deletion::Later
            }
            Failure::Other => {
                tracing::warn!(stream, before, %error, "trimming failed, trying at the next snapshot");
                Deletion::Later
            }
        },
    }
}

fn pause() {
    let mut paused = PAUSED_UNTIL.lock().unwrap_or_else(|e| e.into_inner());
    *paused = Some(Instant::now() + PAUSE);
}

/// How one trim call ended.
#[derive(Debug, PartialEq, Eq)]
enum Settled {
    /// The floor is in place; the trim record landed at the offset if any.
    Trimmed(Option<u64>),
    /// The tail was not the one asked for, twice running.
    TailMoved,
}

/// Calls `call` with the tail Felix expects, and once more with the tail a
/// refusal names. A third refusal means someone else is writing the stream
/// and this gives up quietly.
async fn settle<F, Fut>(mut expected: u64, mut call: F) -> Result<Settled>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<ConditionalTrim>>,
{
    for _ in 0..2 {
        match call(expected).await? {
            ConditionalTrim::Trimmed { offset, .. } => return Ok(Settled::Trimmed(offset)),
            ConditionalTrim::Refused { tail } => expected = tail,
        }
    }
    Ok(Settled::TailMoved)
}

/// Why Felix did not trim.
#[derive(Debug, PartialEq, Eq)]
enum Failure {
    /// The fleet has not finalized `head_trim`.
    NotEnabled,
    /// The token lacks `stream.trim`.
    NoGrant,
    /// The answer did not come in time; the trim may have landed.
    TimedOut,
    Other,
}

fn classify(error: &anyhow::Error) -> Failure {
    if error.downcast_ref::<TrimError>().is_some() {
        return Failure::NotEnabled;
    }
    match error.downcast_ref::<BrokerError>().map(|e| &e.code) {
        Some(ErrorCode::FeatureNotEnabled) => Failure::NotEnabled,
        Some(ErrorCode::Forbidden) => Failure::NoGrant,
        Some(ErrorCode::Unacknowledged) => Failure::TimedOut,
        _ => Failure::Other,
    }
}

#[cfg(test)]
mod tests;
