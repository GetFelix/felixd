//! The placement record a holder appends to `wl.<id>.placed` after its claim
//! lands ([`felixd_core::placement::PlacementRecord`]).
//!
//! The holder does not wait on it before stepping: placement decides no
//! output. It retries until the record lands or the lease is lost, so a slow
//! broker delays the record rather than losing it. The append is a plain
//! publish, not fenced; `felixd check` finds a record whose epoch names no
//! claim.

use std::convert::Infallible;
use std::future::Future;
use std::time::Duration;

use anyhow::Result;
use felixd_core::names;
use felixd_core::placement::PlacementRecord;

use crate::felix::Felix;
use crate::lease::Fence;
use crate::stuck::Progress;

/// How long to wait after a failed append before trying again.
const RETRY_EVERY: Duration = Duration::from_secs(1);

/// Appends `record` once the claim it names is on the output stream, then
/// waits for ever, so it can sit beside the holder's work in a `select!`.
pub(crate) async fn record(
    felix: &Felix,
    id: &str,
    record: PlacementRecord,
    fence: &Fence,
    progress: &Progress,
) -> Infallible {
    progress.claim_on_stream().await;
    let stream = names::placed_stream(id);
    let ended = append_until_landed(
        || fence.held(),
        || async { Ok(felix.read_tail_if_exists(&stream).await?.is_some()) },
        || felix.publish(&stream, record.encode()),
    )
    .await;
    match ended {
        Ended::Landed => {
            tracing::debug!(workload = %id, epoch = record.epoch, placed = %record.authority, "recorded the placement")
        }
        Ended::NoStream => {
            tracing::debug!(workload = %id, "{stream} does not exist, so no placement is recorded")
        }
        Ended::Lost => {}
    }
    std::future::pending().await
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Ended {
    Landed,
    /// The workload was admitted before placement records, so it has no
    /// `.placed` stream.
    NoStream,
    /// The lease was lost before the record landed.
    Lost,
}

/// Tries `append` until it succeeds or `held` says the lease is gone. Looks
/// once, successfully, that the stream `exists` first.
pub(crate) async fn append_until_landed<H, HF, E, EF, A, AF>(held: H, exists: E, append: A) -> Ended
where
    H: Fn() -> HF,
    HF: Future<Output = bool>,
    E: Fn() -> EF,
    EF: Future<Output = Result<bool>>,
    A: Fn() -> AF,
    AF: Future<Output = Result<()>>,
{
    let mut looked = false;
    let mut failing = false;
    loop {
        if !held().await {
            return Ended::Lost;
        }
        let tried = async {
            if !looked {
                if !exists().await? {
                    return Ok(Ended::NoStream);
                }
                looked = true;
            }
            append().await.map(|()| Ended::Landed)
        };
        match tried.await {
            Ok(ended) => return ended,
            // Logged once per run of failures.
            Err(error) if !std::mem::replace(&mut failing, true) => {
                tracing::warn!(error = %format!("{error:#}"), "could not append the placement record, retrying");
            }
            Err(_) => {}
        }
        tokio::time::sleep(RETRY_EVERY).await;
    }
}

#[cfg(test)]
mod tests;
