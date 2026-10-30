//! The merge log of a workload with several inputs: the order its holder
//! chose to step their records in. It is a stream, `wl.<id>.merge`, written
//! like the output stream: a holder appends a claim before anything else and
//! every append is a `publish_if` at the tail it expects, so a stale holder
//! cannot add to it. A step's offset is its entry's offset on the merge log,
//! so outputs, snapshots, resume, replay and `felixd check` number steps by
//! the merge log as a one-input workload's are numbered by its input.
//!
//! Every record is `version (1) | kind (1) | epoch (8) | body`, integers big
//! endian, where `epoch` is the lease epoch of the holder that wrote it. The
//! kinds:
//!
//! - An entry, kind 0: `input (2) | offset (8) | timestamp (8) | next`. The
//!   input's position in the spec's list, the offset of the record on that
//!   input and the record's append time, which is what the step is given.
//! - A claim, kind 1: `next | node id`. A new holder appends one before it
//!   merges anything.
//!
//! `next` is `count (2)` and then `count` offsets (8): for each input, the
//! offset after the last of its records merged so far. The last record alone
//! says where each input carries on, however long one has been idle.

use std::fmt;

use crate::MAX_WRITE_BYTES;
use crate::codec::{DecodeError, Reader};

const VERSION: u8 = 1;
const ENTRY: u8 = 0;
const CLAIM: u8 = 1;

/// The most inputs a workload may read.
pub const MAX_INPUTS: usize = 64;

/// One record of one input, merged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Entry {
    /// The input's position in the spec's list.
    pub input: u16,
    /// The record's offset on that input.
    pub offset: u64,
    /// The record's append time, in microseconds since the Unix epoch.
    pub timestamp_micros: u64,
}

/// A decoded merge log record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Record<'a> {
    Entry {
        epoch: u64,
        entry: Entry,
        next: Vec<u64>,
    },
    Claim {
        epoch: u64,
        next: Vec<u64>,
        node: &'a [u8],
    },
}

impl Record<'_> {
    /// The epoch of the holder that wrote the record.
    pub fn epoch(&self) -> u64 {
        match self {
            Record::Entry { epoch, .. } | Record::Claim { epoch, .. } => *epoch,
        }
    }

    /// Each input's next offset as of this record.
    pub fn next(&self) -> &[u64] {
        match self {
            Record::Entry { next, .. } | Record::Claim { next, .. } => next,
        }
    }

    pub fn entry(&self) -> Option<Entry> {
        match self {
            Record::Entry { entry, .. } => Some(*entry),
            Record::Claim { .. } => None,
        }
    }
}

/// How many entries of a workload with `inputs` inputs one write holds,
/// within [`MAX_WRITE_BYTES`]. A holder that finds a long gap in an input
/// appends it in writes of this many, since Felix refuses a write too large
/// for one frame and would refuse it again on every retry.
pub fn entries_per_write(inputs: usize) -> usize {
    MAX_WRITE_BYTES / (10 + entry_body(inputs))
}

/// The bytes of an entry after its 10-byte header, for `inputs` inputs.
fn entry_body(inputs: usize) -> usize {
    20 + 8 * inputs
}

/// Builds the claim a holder at `epoch` on node `node` appends, carrying
/// `next` from the record before it.
pub fn encode_claim(epoch: u64, next: &[u64], node: &str) -> Vec<u8> {
    let mut out = header(CLAIM, epoch, 2 + 8 * next.len() + node.len());
    put_next(&mut out, next);
    out.extend(node.as_bytes());
    out
}

/// Builds the entry for `entry` and moves `next` past it. `next` must have a
/// place for the entry's input.
pub fn encode_entry(epoch: u64, entry: Entry, next: &mut [u64]) -> Vec<u8> {
    next[usize::from(entry.input)] = entry.offset + 1;
    let mut out = header(ENTRY, epoch, entry_body(next.len()));
    out.extend(entry.input.to_be_bytes());
    out.extend(entry.offset.to_be_bytes());
    out.extend(entry.timestamp_micros.to_be_bytes());
    put_next(&mut out, next);
    out
}

fn header(kind: u8, epoch: u64, body: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(10 + body);
    out.extend([VERSION, kind]);
    out.extend(epoch.to_be_bytes());
    out
}

fn put_next(out: &mut Vec<u8>, next: &[u64]) {
    let count = u16::try_from(next.len()).expect("at most MAX_INPUTS inputs");
    out.extend(count.to_be_bytes());
    for offset in next {
        out.extend(offset.to_be_bytes());
    }
}

/// Reads a merge log record. A version or kind this build does not know is
/// refused.
pub fn decode(record: &[u8]) -> Result<Record<'_>, DecodeError> {
    let mut r = Reader::new(record);
    let version = r.u8()?;
    if version != VERSION {
        return Err(DecodeError::UnknownVersion(version));
    }
    let kind = r.u8()?;
    let epoch = r.u64()?;
    match kind {
        ENTRY => {
            let entry = Entry {
                input: u16::from_be_bytes(r.array()?),
                offset: r.u64()?,
                timestamp_micros: r.u64()?,
            };
            let next = next(&mut r)?;
            if r.remaining() > 0 {
                return Err(DecodeError::TrailingBytes(r.remaining()));
            }
            Ok(Record::Entry { epoch, entry, next })
        }
        CLAIM => Ok(Record::Claim {
            epoch,
            next: next(&mut r)?,
            node: r.rest(),
        }),
        other => Err(DecodeError::UnknownKind(other)),
    }
}

fn next(r: &mut Reader<'_>) -> Result<Vec<u64>, DecodeError> {
    let count = u16::from_be_bytes(r.array()?);
    (0..count).map(|_| r.u64()).collect()
}

/// Where each of `inputs` inputs carries on, from the last record of the
/// merge log, or from the start when it has none.
pub fn cursor(last: Option<&Record<'_>>, inputs: usize) -> Result<Vec<u64>, WrongInputs> {
    match last {
        None => Ok(vec![0; inputs]),
        Some(record) if record.next().len() == inputs => Ok(record.next().to_vec()),
        Some(record) => Err(WrongInputs {
            found: record.next().len(),
            inputs,
        }),
    }
}

/// The merge log was written for another number of inputs than the spec has.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("the merge log was written for {found} inputs, the spec has {inputs}")]
pub struct WrongInputs {
    pub found: usize,
    pub inputs: usize,
}

/// What a holder at `epoch` does about `existing`, the records it found at
/// the tail it expected to append `batch` at.
#[derive(Debug, PartialEq, Eq)]
pub enum Reconciled {
    /// The first `n` records of the batch are there already: this holder's
    /// own, whose answer was lost.
    Skip(usize),
    /// A later holder wrote there. This one must stop.
    Superseded { epoch: u64 },
    /// Something else is there, which no holder past its claim can find.
    Diverged,
}

/// Decides what to do about records found at the expected tail. Once a
/// holder's claim is on the log, an earlier holder can no longer append, so
/// what it finds is either its own or a later holder's.
pub fn reconcile(epoch: u64, batch: &[Vec<u8>], existing: &[&[u8]]) -> Reconciled {
    let mut skipped = 0;
    for &record in existing {
        match decode(record) {
            Ok(found) if found.epoch() > epoch => {
                return Reconciled::Superseded {
                    epoch: found.epoch(),
                };
            }
            Ok(_) if batch.get(skipped).is_some_and(|ours| ours == record) => skipped += 1,
            _ => return Reconciled::Diverged,
        }
    }
    Reconciled::Skip(skipped)
}

impl fmt::Display for Entry {
    /// `input@offset`, with the input by its position.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "input {}@{}", self.input, self.offset)
    }
}

#[cfg(test)]
mod tests;
