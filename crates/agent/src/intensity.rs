//! Reads back an output stream to count a spec record's takeovers against
//! its restart intensity ([`felixd_core::intensity`]).

use std::future::Future;

use anyhow::Result;
use bytes::Bytes;
use felixd_core::class::OperatorReason;
use felixd_core::intensity::{Takeovers, Verdict};
use felixd_core::spec::RestartIntensity;

use crate::felix::Felix;
use crate::runner::NeedsOperator;

/// Offsets read at once. A run of takeovers holds claims and notes only, so
/// one read usually reaches its start.
const LOOK_RECORDS: u64 = 64;

/// An error if the last claim on `stream`, below `end`, takes spec record
/// `spec` past `limit`.
pub(crate) async fn refuse_over_limit(
    felix: &Felix,
    stream: &str,
    end: u64,
    spec: u64,
    limit: RestartIntensity,
) -> Result<()> {
    let verdict = count_in(end, spec, limit, |from, end| async move {
        let (records, ended) = felix.read_back(stream, from, end).await?;
        let records = records
            .into_iter()
            .map(|r| {
                (
                    r.payload,
                    (r.timestamp_micros > 0).then_some(r.timestamp_micros),
                )
            })
            .collect();
        Ok((records, ended))
    })
    .await?;
    match verdict {
        Verdict::Within => Ok(()),
        Verdict::Over { takeovers } => Err(NeedsOperator::new(
            OperatorReason::RestartIntensity,
            format!(
                "{takeovers} takeovers in a row without progress within {} ms, more than the {} the spec allows",
                limit.window_ms, limit.max_takeovers
            ),
        )
        .into()),
    }
}

/// [`refuse_over_limit`]'s count over `read`, which returns the records at
/// `from..end`, each with its append time, and whether there is nothing
/// older.
async fn count_in<F, Fut>(
    end: u64,
    spec: u64,
    limit: RestartIntensity,
    mut read: F,
) -> Result<Verdict>
where
    F: FnMut(u64, u64) -> Fut,
    Fut: Future<Output = Result<(Vec<(Bytes, Option<u64>)>, bool)>>,
{
    let mut count = Takeovers::new(spec, limit);
    let mut end = end;
    while end > 0 {
        let from = end.saturating_sub(LOOK_RECORDS);
        let (records, ended) = read(from, end).await?;
        for (record, at) in records.iter().rev() {
            if let Some(verdict) = count.older(record, *at) {
                return Ok(verdict);
            }
        }
        if ended {
            break;
        }
        end = from;
    }
    Ok(count.verdict())
}

#[cfg(test)]
mod tests;
