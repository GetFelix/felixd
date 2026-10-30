//! Where a workload's history may be trimmed, by its last `keep` snapshots.
//!
//! A trim record on `.out` says which offsets of the snapshot stream, the
//! merge log and each input no felixd reader of this workload needs again.
//! Nothing is deleted yet: Felix has no call to trim a stream at an offset
//! (Felix#1662). The record is what that deletion will follow.

use crate::codec::{DecodeError, Reader};

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
    /// The last input that snapshot includes. Steps up to it are never run
    /// again by a resume, a replay or a check of this workload.
    pub step: u64,
    /// `.snap` records below it are not read.
    pub snap_floor: u64,
    /// Merge log records below it are not read; 0 with one input.
    pub merge_floor: u64,
    /// Per input, in the spec's order, the offset below which this workload
    /// reads nothing.
    pub input_floors: Vec<u64>,
}

/// Why [`plan`] wrote nothing.
#[derive(Debug, PartialEq, Eq)]
pub enum Skip {
    /// Fewer than `keep` snapshots, so nothing is past them.
    TooFew,
    /// The newest snapshot is not the holder's own. Until it is, a stale
    /// holder can still land a snapshot, of a lower step, that the next
    /// resume would restore.
    NotOwn,
    /// A floor would go below the previous trim's. History below that may be
    /// gone, so this would be a bug; nothing is trimmed.
    Regressed,
}

/// The trim a holder at `epoch` writes, from `newest`, the snapshots on
/// `.snap` newest first (at least `keep` of them when there are). `merge_next`
/// is the merge log's `next` at the entry of the oldest kept step, for a
/// workload with several inputs; `None` with one input.
///
/// The oldest kept snapshot is the one with the lowest step, not the oldest
/// on the stream: a stale holder's snapshot can land after a newer holder's
/// with a lower step.
pub fn plan(
    epoch: u64,
    keep: usize,
    newest: &[Kept],
    merge_next: impl FnOnce(u64) -> Option<Vec<u64>>,
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
    let (merge_floor, input_floors) = match merge_next(oldest.step) {
        // The entry at the step is kept, since its `next` is where each
        // input carries on.
        Some(next) => (oldest.step, next),
        None => (0, vec![oldest.step + 1]),
    };
    let trim = Trim {
        snapshot: oldest.offset,
        step: oldest.step,
        snap_floor,
        merge_floor,
        input_floors,
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

/// Appends a trim record's body: snapshot, step, snap floor, merge floor (8
/// each), the input count (2) and each input's floor (8).
pub(crate) fn encode_body(trim: &Trim, out: &mut Vec<u8>) {
    for field in [trim.snapshot, trim.step, trim.snap_floor, trim.merge_floor] {
        out.extend(field.to_be_bytes());
    }
    let count = u16::try_from(trim.input_floors.len()).expect("a spec has at most 64 inputs");
    out.extend(count.to_be_bytes());
    for floor in &trim.input_floors {
        out.extend(floor.to_be_bytes());
    }
}

/// Reads a trim record's body. Bytes past the input floors are refused, so a
/// later format is not misread as this one.
pub(crate) fn decode_body(body: &[u8]) -> Result<Trim, DecodeError> {
    let mut r = Reader::new(body);
    let snapshot = r.u64()?;
    let step = r.u64()?;
    let snap_floor = r.u64()?;
    let merge_floor = r.u64()?;
    let count = u16::from_be_bytes(r.array()?);
    let input_floors = (0..count).map(|_| r.u64()).collect::<Result<_, _>>()?;
    if r.remaining() != 0 {
        return Err(DecodeError::TrailingBytes(r.remaining()));
    }
    Ok(Trim {
        snapshot,
        step,
        snap_floor,
        merge_floor,
        input_floors,
    })
}

#[cfg(test)]
mod tests;
