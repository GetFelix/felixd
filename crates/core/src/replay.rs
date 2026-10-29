//! Where a replay starts and where it stops.
//!
//! A replay runs the inputs below an end offset. It restores the latest
//! snapshot that covers only inputs below that end and runs the rest.

use crate::snapshot::SnapshotHeader;

/// The end of a replay to a time, from what `offset_for_time` answered: the
/// first input offset appended at or after the time, or `None` when no input
/// is that recent, in which case every input below `input_tail` counts.
pub fn end_for_time(first_at_or_after: Option<u64>, input_tail: u64) -> u64 {
    first_at_or_after.unwrap_or(input_tail).min(input_tail)
}

/// Which of `snapshots`, in stream order, a replay to `end` restores: the one
/// with the highest input offset below `end`. `None` means the replay starts
/// from the initial state.
pub fn nearest_snapshot(
    snapshots: impl IntoIterator<Item = SnapshotHeader>,
    end: u64,
) -> Option<usize> {
    let mut best: Option<(usize, u64)> = None;
    for (index, header) in snapshots.into_iter().enumerate() {
        if header.input_offset >= end {
            continue;
        }
        if best.is_none_or(|(_, offset)| header.input_offset >= offset) {
            best = Some((index, header.input_offset));
        }
    }
    best.map(|(index, _)| index)
}

/// The first input a replay runs after restoring `snapshot`, or from the
/// initial state.
pub fn first_input(snapshot: Option<SnapshotHeader>) -> u64 {
    snapshot.map_or(0, |s| s.input_offset + 1)
}

#[cfg(test)]
mod tests;
