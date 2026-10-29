//! Spec records on `sys.specs`: appending one, reading one back, and folding
//! a workload's history.

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use felix_client::{CacheCondition, ConditionalWrite};
use felixd_core::names;
use felixd_core::output::{self, Record};
use felixd_core::spec::{History, SpecRecord, WorkloadSpec, moves_forward};

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

/// A spec record offset at or past the tail of `sys.specs`. Briefly it can
/// be a read that lands before the append is visible; for longer, the stream
/// was recreated or the cache was written by hand.
#[derive(Debug)]
pub(crate) struct PastTail(pub(crate) String);

impl std::fmt::Display for PastTail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for PastTail {}

/// The spec record at `offset`, which must be workload `id`'s. One that is
/// unreadable or another workload's is [`NeedsOperator`], one past the tail
/// is [`PastTail`].
pub(crate) async fn read(felix: &Felix, id: &str, offset: u64) -> Result<WorkloadSpec> {
    let at = format!("{}:{offset}", names::SPECS);
    let found = felix
        .read_range(names::SPECS, offset, offset + 1)
        .await?
        .into_iter()
        .find(|r| r.offset == offset);
    let Some(found) = found else {
        if offset >= felix.tail(names::SPECS).await? {
            return Err(PastTail(format!("no spec record at {at}, past the tail")).into());
        }
        bail!("no spec record at {at}");
    };
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
/// `workloads`, unless the entry already names that record or a later one
/// ([`moves_forward`]). Returns `None` if it wrote, or the record the entry
/// names if it did not.
pub(crate) async fn project(felix: &Felix, id: &str, spec: &WorkloadSpec) -> Result<Option<u64>> {
    let ours = spec.record.context("a projected spec names its record")?;
    loop {
        let found = felix.cache_get_versioned(names::WORKLOADS, id).await?;
        let condition = match &found {
            Some(found) => {
                let current = WorkloadSpec::decode(&found.value)
                    .ok()
                    .and_then(|c| c.record);
                if !moves_forward(current, ours) {
                    return Ok(current);
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
            return Ok(None);
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

/// Refuses to run `spec` over workload `id`'s output stream when the stream
/// was written under another module or input, which `spec` would not
/// reproduce, or when a claim there names a later spec record, which means
/// this node's view of the spec is stale. `own_epoch` is this owner's epoch
/// once its claim is on the stream, so its own claim is passed over. Both are
/// [`NeedsOperator`]; a stale node is restarted by its watch when it catches
/// up.
pub(crate) async fn refuse_stale_or_foreign(
    felix: &Felix,
    id: &str,
    spec: &WorkloadSpec,
    own_epoch: Option<u64>,
) -> Result<()> {
    if let Some(written) = written_under(felix, id).await?
        && let Some(conflict) = written.conflicts_with_history(spec)
    {
        return Err(NeedsOperator(format!(
            "the workload already has output, and {conflict}; submit the change under a new id"
        ))
        .into());
    }
    let ours = spec.record.context("the spec names no spec record")?;
    if let Some(later) = latest_claimed(felix, id, own_epoch)
        .await?
        .filter(|&claimed| claimed > ours)
    {
        return Err(NeedsOperator(format!(
            "spec record {later} has already run, and this node has {ours}"
        ))
        .into());
    }
    Ok(())
}

/// The spec record the latest claim on workload `id`'s output stream names,
/// passing over claims at `skip_epoch` and claims that name none. It reads
/// back from the tail, past every output since that claim.
async fn latest_claimed(felix: &Felix, id: &str, skip_epoch: Option<u64>) -> Result<Option<u64>> {
    let stream = names::output_stream(id);
    let mut end = felix.tail(&stream).await?;
    let mut window = 16;
    while end > 0 {
        let from = end.saturating_sub(window);
        for record in felix.read_range(&stream, from, end).await?.iter().rev() {
            if let Ok(Record::Claim {
                epoch,
                spec: Some(spec),
                ..
            }) = output::decode(&record.payload)
                && Some(epoch) != skip_epoch
            {
                return Ok(Some(spec));
            }
        }
        end = from;
        window = window.saturating_mul(8);
    }
    Ok(None)
}

/// The spec whose module and input workload `id`'s output stream was written
/// under: the first claim's, or `cached`, the `workloads` value, for a stream
/// that predates claims naming one. `felixd check` and replay run its module
/// and input, never a later spec's an owner refused.
pub(crate) async fn lineage(felix: &Felix, id: &str, cached: WorkloadSpec) -> Result<WorkloadSpec> {
    Ok(written_under(felix, id).await?.unwrap_or(cached))
}
