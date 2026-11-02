//! Where a workload's history may be trimmed, by its last `keep` snapshots.
//!
//! A trim record on `.out` says below which offsets of the snapshot stream,
//! the merge log and each input this workload's resume reads nothing.
//! `felixd check --from-snapshot` starts at the oldest kept snapshot the last
//! trim names, and does so by default once the `.out` head is deleted;
//! `felixd replay` reads from the start while records are there.
//!
//! The record also carries what a head cut off `.out` would take with it:
//! the spec record of the stream's first claim, which says what module and
//! input the stream was written under, and the highest spec record claimed
//! so far, which a stale node is refused by.
//!
//! The holder then deletes what the record names on the streams that are the
//! workload's own, `.snap` and the merge log ([`Trim::deletions`]), and only
//! after the record has landed, so a crash at any point leaves less deleted
//! than recorded.
//!
//! An input, or the `.out` of a workload, is read by others too. The holder
//! trims one only below the lowest floor of all its readers
//! ([`crate::readers`]), after its own record has landed. `sys.facts`, which
//! only the scheduler reads, is deleted on the scheduler's floor alone
//! ([`Trim::facts_floor`]).

use crate::codec::{DecodeError, Reader};
use crate::names;
use crate::readers;

/// A snapshot among the last `keep`, as [`plan`] needs it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Kept {
    /// The offset of its manifest, or of the whole snapshot, on `.snap`.
    pub offset: u64,
    /// The epoch it was written at.
    pub epoch: u64,
    /// The last input (merge log offset, with several inputs) it includes.
    pub step: u64,
    /// The `.snap` offsets of the chunk records it reads. Empty for a whole
    /// snapshot.
    pub chunks: Vec<u64>,
}

/// What a trim record says is no longer needed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Trim {
    /// The `.snap` offset of the oldest kept snapshot.
    pub snapshot: u64,
    /// The last input that snapshot includes. A resume of this workload
    /// never steps an input up to it again.
    pub step: u64,
    /// `.snap` records below it are not read.
    pub snap_floor: u64,
    /// Merge log records below it are not read; 0 with one input.
    pub merge_floor: u64,
    /// Per input, in the spec's order, the offset below which this
    /// workload's resume reads nothing. Other readers of a shared input may
    /// need more.
    pub input_floors: Vec<u64>,
    /// The `sys.specs` record the stream's first claim names. The first trim
    /// that can read one writes it, and every trim after copies it. `None`
    /// when no claim names one, and in records written before it was carried.
    pub first_spec: Option<u64>,
    /// The highest spec record claimed on the stream up to this record, the
    /// floor a claim written here would carry. `None` when none was, and in
    /// records written before it was carried.
    pub claimed: Option<u64>,
}

/// A stream whose history the holder deletes on a trim record's word.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Owned {
    /// The snapshot stream, `.snap`.
    Snapshots,
    /// The merge log.
    MergeLog,
}

/// The floors a holder has already deleted below, on the streams it owns, on
/// its `.out` and on `sys.facts`. Zero where it has deleted nothing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Deleted {
    pub snap_floor: u64,
    pub merge_floor: u64,
    pub facts_floor: u64,
    pub out_floor: u64,
}

impl Trim {
    /// What is left to delete: each owned stream whose floor is past what is
    /// already `done`, with the offset to delete below. The merge log only
    /// counts for a `merged` workload.
    pub fn deletions(&self, merged: bool, done: Deleted) -> Vec<(Owned, u64)> {
        let mut left = Vec::new();
        if self.snap_floor > done.snap_floor {
            left.push((Owned::Snapshots, self.snap_floor));
        }
        if merged && self.merge_floor > done.merge_floor {
            left.push((Owned::MergeLog, self.merge_floor));
        }
        left
    }

    /// The offset to delete `sys.facts` below, if `workload` is the scheduler,
    /// reads it among its `inputs`, and that floor is past what is `done`.
    /// Only the scheduler reads `sys.facts`: [`crate::spec::validate_inputs`]
    /// refuses a system stream as any other workload's input. Its own floor is
    /// therefore the lowest.
    pub fn facts_floor(&self, workload: &str, inputs: &[String], done: Deleted) -> Option<u64> {
        if workload != names::SCHEDULER {
            return None;
        }
        let at = inputs.iter().position(|input| input == names::FACTS)?;
        let floor = *self.input_floors.get(at)?;
        (floor > done.facts_floor).then_some(floor)
    }

    /// The inputs the holder may trim on the readers' word
    /// ([`crate::readers::deletable_input`]) whose floor in this record is past
    /// what `done` holds, by position, with this workload's floor. `done` is
    /// the floor already deleted to per input, empty or short for none.
    pub fn shared_inputs(&self, inputs: &[String], done: &[u64]) -> Vec<(usize, u64)> {
        inputs
            .iter()
            .zip(&self.input_floors)
            .enumerate()
            .filter(|(at, (input, floor))| {
                readers::deletable_input(input) && **floor > done.get(*at).copied().unwrap_or(0)
            })
            .map(|(at, (_, floor))| (at, *floor))
            .collect()
    }

    /// Whether the holder of `workload` may delete the head of its `.out`
    /// on this record. Not before the record carries the first claim's spec,
    /// which is what tells a reader what the stream was written under once
    /// the claim is gone. Not a system workload's: admission's decisions are
    /// named by spec records for good, and the scheduler's output is read from
    /// its latest table on.
    pub fn cuts_output(&self, workload: &str) -> bool {
        self.first_spec.is_some() && !names::is_system(workload)
    }
}

/// Why [`plan`] wrote nothing.
#[derive(Debug, PartialEq, Eq)]
pub enum Skip {
    /// Fewer than `keep` snapshots, so nothing is past them.
    TooFew,
    /// The newest snapshot is not the holder's own. The trim append is also
    /// refused once a later holder has claimed, so this is a second guard.
    NotOwn,
    /// A floor would go below the previous trim's. History below that may be
    /// gone, so this would be a bug; nothing is trimmed.
    Regressed,
}

/// The trim a holder at `epoch` writes, from `newest`, the snapshots on
/// `.snap` newest first (at least `keep` of them when there are).
/// `merge_floors` gives the input floors at a merge log step, for a workload
/// with several inputs: the entry's `next`, lowered to the entry's own record
/// for its input. `None` with one input.
///
/// The oldest kept snapshot is the one with the lowest step, not the oldest
/// on the stream: a stale holder's snapshot can land after a newer holder's
/// with a lower step.
///
/// The record keeps `previous`'s `first_spec`; the caller fills in what it
/// reads from the stream when there is none, and `claimed`.
pub fn plan(
    epoch: u64,
    keep: usize,
    newest: &[Kept],
    merge_floors: impl FnOnce(u64) -> Option<Vec<u64>>,
    previous: Option<&Trim>,
) -> Result<Trim, Skip> {
    let keep = keep.max(1);
    if newest.len() < keep {
        return Err(Skip::TooFew);
    }
    if newest[0].epoch != epoch {
        return Err(Skip::NotOwn);
    }
    let kept = &newest[..keep];
    let oldest = kept
        .iter()
        .min_by_key(|k| (k.step, k.offset))
        .expect("keep is at least 1");
    let snap_floor = kept
        .iter()
        .flat_map(|k| std::iter::once(k.offset).chain(k.chunks.iter().copied()))
        .min()
        .expect("keep is at least 1");
    // The record at the step itself is kept: a resume reads it back for the
    // last append time (`snapshot_every_ms`), and the merge log entry there
    // holds the `next` each input carries on from.
    let (merge_floor, input_floors) = match merge_floors(oldest.step) {
        Some(floors) => (oldest.step, floors),
        None => (0, vec![oldest.step]),
    };
    let trim = Trim {
        snapshot: oldest.offset,
        step: oldest.step,
        snap_floor,
        merge_floor,
        input_floors,
        first_spec: previous.and_then(|p| p.first_spec),
        claimed: None,
    };
    if let Some(previous) = previous
        && (trim.step < previous.step
            || trim.snap_floor < previous.snap_floor
            || trim.merge_floor < previous.merge_floor
            || trim
                .input_floors
                .iter()
                .zip(&previous.input_floors)
                .any(|(now, before)| now < before))
    {
        return Err(Skip::Regressed);
    }
    Ok(trim)
}

const HAS_FIRST_SPEC: u8 = 1;
const HAS_CLAIMED: u8 = 2;

/// Appends a trim record's body: snapshot, step, snap floor, merge floor (8
/// each), the input count (2) and each input's floor (8). Then, only when
/// there is a spec to carry, a flags byte (1 for `first_spec`, 2 for
/// `claimed`) and each field it names (8), `first_spec` first.
pub(crate) fn encode_body(trim: &Trim, out: &mut Vec<u8>) {
    for field in [trim.snapshot, trim.step, trim.snap_floor, trim.merge_floor] {
        out.extend(field.to_be_bytes());
    }
    let count = u16::try_from(trim.input_floors.len()).expect("a spec has at most 64 inputs");
    out.extend(count.to_be_bytes());
    for floor in &trim.input_floors {
        out.extend(floor.to_be_bytes());
    }
    let mut flags = 0;
    if trim.first_spec.is_some() {
        flags |= HAS_FIRST_SPEC;
    }
    if trim.claimed.is_some() {
        flags |= HAS_CLAIMED;
    }
    if flags != 0 {
        out.push(flags);
        out.extend(
            trim.first_spec
                .iter()
                .chain(&trim.claimed)
                .flat_map(|n| n.to_be_bytes()),
        );
    }
}

/// Reads a trim record's body. A body that ends at the input floors carries
/// no spec. Flags other than 1, 2 and 3, and bytes past the fields they
/// name, are refused, so a later format is not misread as this one.
pub(crate) fn decode_body(body: &[u8]) -> Result<Trim, DecodeError> {
    let mut r = Reader::new(body);
    let snapshot = r.u64()?;
    let step = r.u64()?;
    let snap_floor = r.u64()?;
    let merge_floor = r.u64()?;
    let count = u16::from_be_bytes(r.array()?);
    let input_floors = (0..count).map(|_| r.u64()).collect::<Result<_, _>>()?;
    let (mut first_spec, mut claimed) = (None, None);
    if r.remaining() != 0 {
        let flags = r.u8()?;
        if flags == 0 || flags & !(HAS_FIRST_SPEC | HAS_CLAIMED) != 0 {
            return Err(DecodeError::InvalidTrimFlags(flags));
        }
        if flags & HAS_FIRST_SPEC != 0 {
            first_spec = Some(r.u64()?);
        }
        if flags & HAS_CLAIMED != 0 {
            claimed = Some(r.u64()?);
        }
    }
    if r.remaining() != 0 {
        return Err(DecodeError::TrailingBytes(r.remaining()));
    }
    Ok(Trim {
        snapshot,
        step,
        snap_floor,
        merge_floor,
        input_floors,
        first_spec,
        claimed,
    })
}

#[cfg(test)]
mod tests;
