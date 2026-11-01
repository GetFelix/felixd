//! Spec records on `sys.specs`: appending one, reading one back, and folding
//! a workload's history.

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use felix_client::{CacheCondition, ConditionalWrite};
use felixd_core::class::OperatorReason;
use felixd_core::names;
use felixd_core::output::{self, Record};
use felixd_core::spec::{SpecRecord, WorkloadSpec, moves_forward};

use crate::admission::{self, Gate};
use crate::felix::{Felix, Trimmed};
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
    Ok(read_record(felix, id, offset).await?.spec)
}

/// As [`read`], the whole record.
pub(crate) async fn read_record(felix: &Felix, id: &str, offset: u64) -> Result<SpecRecord> {
    let at = format!("{}:{offset}", names::SPECS);
    let found = felix
        .read_range(names::SPECS, offset, offset + 1)
        .await?
        .into_iter()
        .find(|r| r.offset == offset);
    let Some(found) = found else {
        // A read, so a node needs no publish grant on sys.specs.
        if offset >= felix.read_tail(names::SPECS).await? {
            return Err(PastTail(format!("no spec record at {at}, past the tail")).into());
        }
        bail!("no spec record at {at}");
    };
    let record = SpecRecord::decode(&found.payload)
        .map_err(|e| unrecorded(format!("the spec record at {at}: {e}")))?;
    if record.id != id {
        return Err(unrecorded(format!(
            "the spec record at {at} is workload {}'s",
            record.id
        ))
        .into());
    }
    if record.skipped {
        return Err(unrecorded(format!("the record at {at} is a skip record, not a spec")).into());
    }
    Ok(record)
}

/// Checks that `spec`, a `workloads` value, is what its spec record says, so
/// an agent runs only specs in the history, and that admission accepted the
/// record ([`admission::check_admitted`]). A mismatch is [`NeedsOperator`].
pub(crate) async fn check_record(
    felix: &Felix,
    gate: &Gate,
    id: &str,
    spec: &WorkloadSpec,
) -> Result<()> {
    let Some(offset) = spec.record else {
        return Err(unrecorded("its workloads entry names no spec record; submit it again").into());
    };
    let record = read_record(felix, id, offset).await?;
    if !spec.matches_record(&record.spec) {
        return Err(unrecorded(format!(
            "its workloads entry differs from its spec record at {}:{offset}",
            names::SPECS
        ))
        .into());
    }
    admission::check_admitted(felix, gate, id, offset, &record).await
}

fn unrecorded(detail: impl Into<String>) -> NeedsOperator {
    NeedsOperator::new(OperatorReason::SpecUnrecorded, detail)
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

/// Records a backward or forward look at an output stream reads at once, so
/// memory stays bounded however long the stream or large its records.
const LOOK_RECORDS: u64 = 16;

/// The spec whose module and input workload `id`'s output stream was written
/// under. Owners refuse a spec whose module or input differ from the first
/// claim's, so that is everything on the stream. `None` for a stream with no
/// claim, or whose first claim or output predates spec records.
///
/// When Felix has deleted the stream's head, the first claim is gone. A trim
/// record carries the first claim's spec forward, and any claim that
/// survives names a spec with the same module and input, so the answer is
/// the first of those.
pub(crate) async fn written_under(felix: &Felix, id: &str) -> Result<Option<WorkloadSpec>> {
    match written_record(felix, &names::output_stream(id)).await? {
        Some(spec) => Ok(Some(read(felix, id, spec).await?)),
        None => Ok(None),
    }
}

/// The spec record [`written_under`] reads.
pub(crate) async fn written_record(felix: &Felix, stream: &str) -> Result<Option<u64>> {
    let tail = felix.tail(stream).await?;
    written_record_in(stream, tail, |from, end| page_of(felix, stream, from, end)).await
}

/// [`written_record`] over `read`, which returns the page of records
/// starting at `from` and below `end`, as [`Felix::read_page`] does.
async fn written_record_in<F, Fut>(stream: &str, tail: u64, mut read: F) -> Result<Option<u64>>
where
    F: FnMut(u64, u64) -> Fut,
    Fut: Future<Output = Result<Page>>,
{
    let (first, cut) = first_held(0, tail, is_run_record, &mut read).await?;
    let Some((offset, record)) = first else {
        return Ok(None);
    };
    let mut named = spec_named(&record).map_err(|e| anyhow!("{stream}:{offset}: {e}"))?;
    if named.is_none() && cut {
        let carrier = |record: &[u8]| spec_named(record).is_ok_and(|spec| spec.is_some());
        if let (Some((at, record)), _) = first_held(offset, tail, carrier, &mut read).await? {
            named = spec_named(&record).map_err(|e| anyhow!("{stream}:{at}: {e}"))?;
        }
    }
    Ok(named)
}

/// The spec record a claim names, or a trim record carries as the first
/// claim's.
fn spec_named(record: &[u8]) -> Result<Option<u64>, felixd_core::DecodeError> {
    Ok(output::decode(record)?.names_spec())
}

/// Whether `record` is not a needs-operator record. One can be written before
/// any claim and ran nothing, so it never says what the stream was written
/// under.
fn is_run_record(record: &[u8]) -> bool {
    output::decode(record).map_or(true, |found| found.needs_operator_spec().is_none())
}

/// The first record on `stream` that Felix still holds and that is not a
/// needs-operator record, with its offset.
pub(crate) async fn first_run_record(felix: &Felix, stream: &str) -> Result<Option<(u64, Bytes)>> {
    let tail = felix.tail(stream).await?;
    let read = |from, end| page_of(felix, stream, from, end);
    Ok(first_held(0, tail, is_run_record, read).await?.0)
}

/// A page of `(offset, payload)` records and the offset the next one starts at.
type Page = (Vec<(u64, Bytes)>, u64);

async fn page_of(felix: &Felix, stream: &str, from: u64, end: u64) -> Result<Page> {
    let (page, after) = felix.read_page(stream, from, end).await?;
    Ok((
        page.into_iter().map(|r| (r.offset, r.payload)).collect(),
        after,
    ))
}

/// The first record at `from..tail` that `wanted` accepts, and whether Felix
/// had deleted the stream's head, so that the search began at the oldest
/// record it keeps. `read` is as for [`written_record_in`].
async fn first_held<F, Fut>(
    from: u64,
    tail: u64,
    wanted: impl Fn(&[u8]) -> bool,
    mut read: F,
) -> Result<(Option<(u64, Bytes)>, bool)>
where
    F: FnMut(u64, u64) -> Fut,
    Fut: Future<Output = Result<Page>>,
{
    let mut next = from;
    let mut cut = false;
    while next < tail {
        let end = tail.min(next + LOOK_RECORDS);
        let (page, after) = match read(next, end).await {
            Ok(read) => read,
            // History below the oldest offset Felix holds is gone.
            Err(error) => match Trimmed::of(&error).map(|t| t.oldest) {
                Some(oldest) if oldest > next => {
                    next = oldest;
                    cut = true;
                    continue;
                }
                _ => return Err(error),
            },
        };
        if let Some(found) = page.into_iter().find(|(_, payload)| wanted(payload)) {
            return Ok((Some(found), cut));
        }
        next = after;
    }
    Ok((None, cut))
}

/// Refuses to run `spec` over workload `id`'s output stream when the stream
/// was written under another module or input, which `spec` would not
/// reproduce, or when the floor ([`claim_floor`]) is above `spec`'s record,
/// which means a later spec has been claimed or recorded and this node's view
/// is stale. Called
/// before the lease claim and again once this owner's claim is on the
/// stream, when the last claim is its own and its floor still says whether a
/// later spec was claimed before it. Both are [`NeedsOperator`]; a stale node
/// is restarted by its watch when it catches up.
pub(crate) async fn refuse_stale_or_foreign(
    felix: &Felix,
    id: &str,
    spec: &WorkloadSpec,
) -> Result<()> {
    if let Some(written) = written_under(felix, id).await?
        && let Some(conflict) = written.conflicts_with_history(spec)
    {
        return Err(NeedsOperator::new(
            OperatorReason::SpecConflict,
            format!(
                "the workload already has output, and {conflict}; submit the change under a new id"
            ),
        )
        .into());
    }
    let ours = spec.record.context("the spec names no spec record")?;
    let stream = names::output_stream(id);
    let tail = felix.tail(&stream).await?;
    if let Some(floor) = claim_floor(felix, &stream, tail)
        .await?
        .filter(|&floor| floor > ours)
    {
        return Err(NeedsOperator::new(
            OperatorReason::SpecStale,
            format!("spec record {floor} has already been claimed, and this node has {ours}"),
        )
        .into());
    }
    Ok(())
}

/// The highest spec record claimed on `stream` below `end`: the floor of
/// the last claim, raised by any needs-operator record after it, since that
/// record's writer passed the same refusals a claimer does. A trim record
/// carries the same floor, so the walk also stops at the last one that does.
/// `None` if there is neither, or the last claim predates spec records and
/// no needs-operator record follows it. It pages back from `end` a few
/// records at a time, so it reads every output since the last takeover or
/// trim but holds only one page, and ends at the oldest record Felix holds.
pub(crate) async fn claim_floor(felix: &Felix, stream: &str, end: u64) -> Result<Option<u64>> {
    claim_floor_in(end, |from, end| async move {
        let (records, ended) = felix.read_back(stream, from, end).await?;
        Ok((records.into_iter().map(|r| r.payload).collect(), ended))
    })
    .await
}

/// [`claim_floor`] over `read`, which returns the payloads at `from..end`
/// and whether there is nothing older to read. Never asks for more than
/// [`LOOK_RECORDS`] offsets at once.
async fn claim_floor_in<F, Fut, P>(end: u64, mut read: F) -> Result<Option<u64>>
where
    F: FnMut(u64, u64) -> Fut,
    Fut: Future<Output = Result<(Vec<P>, bool)>>,
    P: AsRef<[u8]>,
{
    let mut end = end;
    let mut recorded = None;
    while end > 0 {
        let from = end.saturating_sub(LOOK_RECORDS);
        let (records, ended) = read(from, end).await?;
        for record in records.iter().rev() {
            match output::decode(record.as_ref()) {
                Ok(Record::Claim { floor, .. }) => return Ok(floor.max(recorded)),
                Ok(found) => {
                    if let Some(claimed) = found.trim().and_then(|t| t.claimed) {
                        return Ok(Some(claimed).max(recorded));
                    }
                    recorded = recorded.max(found.needs_operator_spec());
                }
                Err(_) => {}
            }
        }
        if ended {
            break;
        }
        end = from;
    }
    Ok(recorded)
}

/// The spec whose module and input workload `id`'s output stream was written
/// under: the first claim's, or `cached`, the `workloads` value, for a stream
/// that predates claims naming one. `felixd check` and replay run its module
/// and input, never a later spec's an owner refused.
pub(crate) async fn lineage(felix: &Felix, id: &str, cached: WorkloadSpec) -> Result<WorkloadSpec> {
    Ok(written_under(felix, id).await?.unwrap_or(cached))
}

#[cfg(test)]
mod tests;
