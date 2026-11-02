//! Deleting what a trim record names: from the streams a workload owns, from
//! its `.out` and from the inputs it shares with other workloads.
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
static FACTS_PAUSED_UNTIL: Mutex<Option<Instant>> = Mutex::new(None);
static SHARED_PAUSED_UNTIL: Mutex<Option<Instant>> = Mutex::new(None);
static WARNED_NOT_ENABLED: AtomicBool = AtomicBool::new(false);
static WARNED_NO_GRANT: AtomicBool = AtomicBool::new(false);
static WARNED_NO_FACTS_GRANT: AtomicBool = AtomicBool::new(false);
static WARNED_NO_SHARED_GRANT: AtomicBool = AtomicBool::new(false);

/// Which kind of stream is being trimmed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    /// One only the workload's holder writes: `.snap`, the merge log or
    /// `.out`. A refused tail may mean a later holder has written, so it is
    /// not chased.
    Owned,
    /// `sys.facts`, which every node appends to and whose trim grant is
    /// separate, so a node that lacks it still deletes the owned streams.
    Facts,
    /// An input other workloads read, written by others. A node whose token
    /// cannot trim it still deletes the owned streams.
    Shared,
}

impl Kind {
    /// How many times a refused tail is asked again at the one Felix names.
    fn retries(self) -> usize {
        match self {
            Kind::Owned => 0,
            Kind::Facts | Kind::Shared => BUSY_RETRIES,
        }
    }
}

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
    not_paused(&PAUSED_UNTIL)
}

/// As [`may_delete`], for `sys.facts`: also false for a short while after the
/// node's token turned out not to trim it.
pub(crate) fn may_delete_facts() -> bool {
    may_delete() && not_paused(&FACTS_PAUSED_UNTIL)
}

fn not_paused(until: &Mutex<Option<Instant>>) -> bool {
    let paused = until.lock().unwrap_or_else(|e| e.into_inner());
    paused.is_none_or(|until| Instant::now() >= until)
}

/// As [`may_delete`], for the inputs shared with other workloads.
pub(crate) fn may_delete_shared() -> bool {
    may_delete() && not_paused(&SHARED_PAUSED_UNTIL)
}

/// How many times a stream with other writers is asked again at the tail Felix
/// names.
const BUSY_RETRIES: usize = 3;

/// Hides the records of `stream` below `before`. `tail` is the stream's next
/// offset as far as the caller knows; the trim record lands there. For
/// [`Kind::Facts`] and [`Kind::Shared`] a refused tail is asked again at the
/// one Felix names.
pub(crate) async fn trim_stream(
    felix: &Felix,
    stream: &str,
    before: u64,
    tail: u64,
    kind: Kind,
) -> Deletion {
    let result = settle(tail, kind.retries(), |expected| {
        felix.trim_before(stream, before, expected)
    })
    .await;
    match result {
        Ok(Settled::Trimmed(landed)) => Deletion::Done { landed },
        Ok(Settled::TailMoved) => {
            tracing::debug!(
                stream,
                before,
                "the tail is not the one expected, trimming at the next snapshot"
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
                pause(Kind::Owned);
                Deletion::Unavailable
            }
            Failure::NoGrant => {
                let warned = match kind {
                    Kind::Owned => &WARNED_NO_GRANT,
                    Kind::Facts => &WARNED_NO_FACTS_GRANT,
                    Kind::Shared => &WARNED_NO_SHARED_GRANT,
                };
                if !warned.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        stream,
                        %error,
                        "this node's token may not trim this stream, so keep_snapshots does not \
                         delete from it; to let it delete, run `felixd init --grant-trim`, \
                         finalize the head_trim fleet feature and re-mint the node's token"
                    );
                }
                pause(kind);
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

/// Holds off deleting for a while: everything for the owned streams, and
/// only `sys.facts` or the shared inputs for a missing grant on them.
fn pause(kind: Kind) {
    let until = match kind {
        Kind::Owned => &PAUSED_UNTIL,
        Kind::Facts => &FACTS_PAUSED_UNTIL,
        Kind::Shared => &SHARED_PAUSED_UNTIL,
    };
    *until.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now() + PAUSE);
}

/// How one trim call ended.
#[derive(Debug, PartialEq, Eq)]
enum Settled {
    /// The floor is in place; the trim record landed at the offset if any.
    Trimmed(Option<u64>),
    /// The tail was not the one asked for.
    TailMoved,
}

/// Calls `call` at the tail the caller expects, and after a refusal at the
/// tail Felix names, up to `retries` more times. Retrying is safe, since a trim
/// only hides offsets below a floor already on `.out`. The owned streams
/// retry nothing (`retries` is 0): their expected tail may just be stale, so
/// giving up costs one snapshot interval, as the next snapshot write refreshes
/// it. For `.out` the refusal is also the fence: a holder a later one has
/// superseded finds the tail moved and must not trim at the new one. A stream
/// with many writers, like `sys.facts` or an input, moves its tail between any
/// two calls, and giving up there would never trim it.
async fn settle<F, Fut>(expected: u64, retries: usize, mut call: F) -> Result<Settled>
where
    F: FnMut(u64) -> Fut,
    Fut: Future<Output = Result<ConditionalTrim>>,
{
    let mut at = expected;
    for left in (0..=retries).rev() {
        match call(at).await? {
            ConditionalTrim::Trimmed { offset, .. } => return Ok(Settled::Trimmed(offset)),
            ConditionalTrim::Refused { tail } if left > 0 => at = tail,
            ConditionalTrim::Refused { .. } => break,
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
