//! Spec records on `sys.specs`: appending one, reading one back, and folding
//! a workload's history.

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use felix_client::{CacheCondition, ConditionalWrite};
use felixd_core::names;
use felixd_core::output::{self, Record};
use felixd_core::spec::{History, SpecRecord, WorkloadSpec};

use crate::felix::Felix;
use crate::runner::NeedsOperator;

/// Appends `record` at the tail of `sys.specs` and returns its offset, which
/// is the spec's version.
pub(crate) async fn append(felix: &Felix, record: &SpecRecord) -> Result<u64> {
    let payload = record.encode();
    let mut tail = felix.tail(names::SPECS).await?;
    loop {
        match felix
            .publish_if(names::SPECS, vec![payload.clone()], tail)
            .await?
        {
            ConditionalWrite::Written { offset } => return Ok(offset),
            // Another submitter appended first; ours goes after it.
            ConditionalWrite::Refused { tail: now } => tail = now,
        }
    }
}

/// The spec record at `offset`, which must be workload `id`'s. One that is
/// unreadable or another workload's is [`NeedsOperator`]. A missing one is
/// not: a read can land before the append it follows is visible.
pub(crate) async fn read(felix: &Felix, id: &str, offset: u64) -> Result<WorkloadSpec> {
    let at = format!("{}:{offset}", names::SPECS);
    let found = felix
        .read_range(names::SPECS, offset, offset + 1)
        .await?
        .into_iter()
        .find(|r| r.offset == offset)
        .with_context(|| format!("no spec record at {at}"))?;
    let record = SpecRecord::decode(&found.payload)
        .map_err(|e| NeedsOperator(format!("the spec record at {at}: {e}")))?;
    if record.id != id {
        return Err(NeedsOperator(format!(
            "the spec record at {at} is workload {}'s",
            record.id
        ))
        .into());
    }
    Ok(record.spec)
}

/// Checks that `spec`, a `workloads` value, is what its spec record says, so
/// an agent runs only specs in the history. A mismatch is [`NeedsOperator`].
pub(crate) async fn check_record(felix: &Felix, id: &str, spec: &WorkloadSpec) -> Result<()> {
    let Some(offset) = spec.record else {
        return Err(NeedsOperator(
            "its workloads entry names no spec record; submit it again".to_owned(),
        )
        .into());
    };
    if !spec.matches_record(&read(felix, id, offset).await?) {
        return Err(NeedsOperator(format!(
            "its workloads entry differs from its spec record at {}:{offset}",
            names::SPECS
        ))
        .into());
    }
    Ok(())
}

/// Every spec record of workload `id`.
pub(crate) async fn history(felix: &Felix, id: &str) -> Result<History> {
    let tail = felix.tail(names::SPECS).await?;
    let records = felix.read_range(names::SPECS, 0, tail).await?;
    Ok(History::read(
        id,
        records.iter().map(|r| (r.offset, r.payload.as_ref())),
    )?)
}

/// Writes `spec`, the cache value of the record at `spec.record`, into
/// `workloads`, unless the entry already projects a later record. Two
/// submitters can append in one order and reach the cache in the other, and
/// the cache must not go back to the earlier record. Returns whether it wrote.
pub(crate) async fn project(felix: &Felix, id: &str, spec: &WorkloadSpec) -> Result<bool> {
    let ours = spec.record.context("a projected spec names its record")?;
    loop {
        let found = felix.cache_get_versioned(names::WORKLOADS, id).await?;
        let condition = match &found {
            Some(found) => {
                let current = WorkloadSpec::decode(&found.value).ok();
                if current.and_then(|c| c.record).is_some_and(|r| r >= ours) {
                    return Ok(false);
                }
                CacheCondition::Version(found.version)
            }
            None => CacheCondition::Absent,
        };
        let put = felix
            .cache_put_if(
                names::WORKLOADS,
                id,
                Bytes::from(spec.encode()),
                None,
                condition,
            )
            .await?;
        if put.applied {
            return Ok(true);
        }
    }
}

/// The spec the first claim on workload `id`'s output stream names. Owners
/// refuse a spec whose module or input differ from it, so everything on the
/// stream was written under that module and input. `None` for a stream with
/// no claim, or whose first claim or output predates spec records.
pub(crate) async fn written_under(felix: &Felix, id: &str) -> Result<Option<WorkloadSpec>> {
    let stream = names::output_stream(id);
    let tail = felix.tail(&stream).await?;
    let mut next = 0;
    while next < tail {
        let (page, after) = felix.read_page(&stream, next, tail).await?;
        if let Some(first) = page.first() {
            return match output::decode(&first.payload) {
                Ok(Record::Claim {
                    spec: Some(spec), ..
                }) => Ok(Some(read(felix, id, spec).await?)),
                Ok(_) => Ok(None),
                Err(error) => bail!("{stream}:{}: {error}", first.offset),
            };
        }
        next = after;
    }
    Ok(None)
}
