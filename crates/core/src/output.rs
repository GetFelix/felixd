//! The header on every record in a workload's output stream, and how an owner
//! reconciles its outputs with records it finds already written.
//!
//! A record is `version (1) | input_offset (8) | index (4) | epoch (8) |
//! payload`, integers big endian. The header says which input produced the
//! output and which of that step's outputs it is, so a new owner can tell what
//! its predecessor already wrote. `epoch` is the lease epoch of the owner that
//! wrote it; see [`crate::lease`].

use crate::codec::{DecodeError, split_header};

const VERSION: u8 = 2;
const HEADER_BYTES: usize = 20;

/// Where an output record came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct OutputHeader {
    /// Offset of the input record whose step produced this output.
    pub input_offset: u64,
    /// Position among that step's outputs, from 0.
    pub index: u32,
}

/// An output record's header and who wrote it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Written {
    pub header: OutputHeader,
    /// The writer's lease epoch.
    pub epoch: u64,
}

/// Builds an output record from its header, its writer's epoch and the
/// workload's payload.
pub fn encode(header: OutputHeader, epoch: u64, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + HEADER_BYTES + payload.len());
    out.push(VERSION);
    out.extend(header.input_offset.to_be_bytes());
    out.extend(header.index.to_be_bytes());
    out.extend(epoch.to_be_bytes());
    out.extend(payload);
    out
}

/// Splits an output record into its header and payload.
pub fn decode(record: &[u8]) -> Result<(Written, &[u8]), DecodeError> {
    let (header, payload) = split_header::<HEADER_BYTES>(record, VERSION)?;
    let (offset, rest) = header.split_at(8);
    let (index, epoch) = rest.split_at(4);
    Ok((
        Written {
            header: OutputHeader {
                input_offset: u64::from_be_bytes(offset.try_into().unwrap()),
                index: u32::from_be_bytes(index.try_into().unwrap()),
            },
            epoch: u64::from_be_bytes(epoch.try_into().unwrap()),
        },
        payload,
    ))
}

/// Whether two records hold the same output, whichever owner wrote them.
/// A record that does not decode is the same as nothing.
pub fn same_output(a: &[u8], b: &[u8]) -> bool {
    match (decode(a), decode(b)) {
        (Ok((a, pa)), Ok((b, pb))) => a.header == b.header && pa == pb,
        _ => false,
    }
}

/// What an owner at `epoch` does about `existing`, the records it found at the
/// tail it expected to write `batch` at.
#[derive(Debug, PartialEq, Eq)]
pub enum Reconciled {
    /// The first `n` records of the batch are already there, written by this
    /// owner (a lost answer) or an earlier one. Skip them and write the rest.
    Skip(usize),
    /// A later owner wrote there. This owner has lost the workload and must
    /// stop without writing anything more.
    Superseded { epoch: u64 },
    /// An earlier owner wrote past this batch. Resume from the streams again
    /// to skip what it wrote.
    Behind,
    /// What is there is not this workload's output.
    Diverged,
}

/// Decides what to do about records found at the expected tail.
///
/// A record from a later epoch wins over everything else. Without that rule
/// a stale owner that finds the new owner's identical records would skip them
/// and write after them, and two owners would take turns on the stream.
pub fn reconcile(epoch: u64, batch: &[Vec<u8>], existing: &[&[u8]]) -> Reconciled {
    let mut found = Vec::with_capacity(existing.len());
    for record in existing {
        let Ok((written, _)) = decode(record) else {
            return Reconciled::Diverged;
        };
        found.push(written);
    }
    if let Some(later) = found.iter().map(|w| w.epoch).filter(|&e| e > epoch).max() {
        return Reconciled::Superseded { epoch: later };
    }
    let compared = existing.len().min(batch.len());
    if !existing[..compared]
        .iter()
        .zip(batch)
        .all(|(found, ours)| same_output(found, ours))
    {
        return Reconciled::Diverged;
    }
    if existing.len() > batch.len() {
        Reconciled::Behind
    } else {
        Reconciled::Skip(existing.len())
    }
}

#[cfg(test)]
mod tests;
