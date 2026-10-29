//! The records on a workload's output stream, and how an owner reconciles its
//! outputs with records it finds already written.
//!
//! Every record is `version (1) | kind (1) | epoch (8) | body`, integers big
//! endian, where `epoch` is the lease epoch of the owner that wrote it; see
//! [`crate::lease`]. An output's body is `input_offset (8) | index (4) |
//! payload`: which input produced it and which of that step's outputs it is,
//! so a new owner can tell what its predecessor already wrote. A claim's body
//! is the owner's node id, which older claims leave out. A new owner appends one before it runs anything, so a stale owner's
//! next conditional append is refused even if the new owner never writes an
//! output. A failure's body is `spec_version (8) | reason`: the workload
//! failed under that version of its spec. It is the last record of that run.
//!
//! Version 2 records are outputs without a kind byte, with the epoch after the
//! index, and still read.

use crate::codec::DecodeError;

const VERSION: u8 = 3;
const OUTPUTS_ONLY: u8 = 2;
const OUTPUT: u8 = 0;
const CLAIM: u8 = 1;
const FAILURE: u8 = 2;
const COMMON_BYTES: usize = 1 + 8;
const OUTPUT_BYTES: usize = 8 + 4;

/// Where an output record came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

/// A decoded record of an output stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Record<'a> {
    /// One of the workload's outputs and its payload.
    Output(Written, &'a [u8]),
    /// The owner at `epoch`, on node `node`, took the workload over here. It
    /// holds no output. `node` is empty in claims written before it was
    /// recorded.
    Claim { epoch: u64, node: &'a [u8] },
    /// The owner at `epoch` failed the workload while it ran under spec
    /// version `spec_version`. It holds no output.
    Failure {
        epoch: u64,
        spec_version: u64,
        reason: &'a [u8],
    },
}

impl<'a> Record<'a> {
    /// The epoch of the owner that wrote the record.
    pub fn epoch(&self) -> u64 {
        match self {
            Record::Output(written, _) => written.epoch,
            Record::Claim { epoch, .. } | Record::Failure { epoch, .. } => *epoch,
        }
    }

    /// The output, unless this is a claim or a failure.
    pub fn output(self) -> Option<(Written, &'a [u8])> {
        match self {
            Record::Output(written, payload) => Some((written, payload)),
            Record::Claim { .. } | Record::Failure { .. } => None,
        }
    }
}

/// Builds an output record from its header, its writer's epoch and the
/// workload's payload.
pub fn encode(header: OutputHeader, epoch: u64, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + COMMON_BYTES + OUTPUT_BYTES + payload.len());
    out.extend([VERSION, OUTPUT]);
    out.extend(epoch.to_be_bytes());
    out.extend(header.input_offset.to_be_bytes());
    out.extend(header.index.to_be_bytes());
    out.extend(payload);
    out
}

/// Builds the record an owner at `epoch` appends when it takes a workload
/// over. `node` names the owner, so a reader can tell which node wrote each
/// epoch's records.
pub fn encode_claim(epoch: u64, node: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + COMMON_BYTES + node.len());
    out.extend([VERSION, CLAIM]);
    out.extend(epoch.to_be_bytes());
    out.extend(node.as_bytes());
    out
}

/// Builds the record an owner at `epoch` appends when the workload fails
/// under spec version `spec_version`.
pub fn encode_failure(epoch: u64, spec_version: u64, reason: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + COMMON_BYTES + 8 + reason.len());
    out.extend([VERSION, FAILURE]);
    out.extend(epoch.to_be_bytes());
    out.extend(spec_version.to_be_bytes());
    out.extend(reason.as_bytes());
    out
}

/// Reads an output stream record.
pub fn decode(record: &[u8]) -> Result<Record<'_>, DecodeError> {
    match record.first() {
        Some(&OUTPUTS_ONLY) => return decode_outputs_only(record),
        Some(&VERSION) => {}
        Some(&other) => return Err(DecodeError::UnknownVersion(other)),
        None => {
            return Err(DecodeError::Truncated {
                need: 1 + COMMON_BYTES,
                got: 0,
            });
        }
    }
    let Some((&[_, kind], rest)) = record.split_first_chunk::<2>() else {
        return Err(truncated(1 + COMMON_BYTES, record));
    };
    let need = match kind {
        OUTPUT => 1 + COMMON_BYTES + OUTPUT_BYTES,
        CLAIM => 1 + COMMON_BYTES,
        FAILURE => 1 + COMMON_BYTES + 8,
        other => return Err(DecodeError::UnknownKind(other)),
    };
    let Some((epoch, body)) = rest.split_first_chunk::<8>() else {
        return Err(truncated(need, record));
    };
    let epoch = u64::from_be_bytes(*epoch);
    if kind == CLAIM {
        return Ok(Record::Claim { epoch, node: body });
    }
    if kind == FAILURE {
        let Some((spec_version, reason)) = body.split_first_chunk::<8>() else {
            return Err(truncated(need, record));
        };
        return Ok(Record::Failure {
            epoch,
            spec_version: u64::from_be_bytes(*spec_version),
            reason,
        });
    }
    let Some((fields, payload)) = body.split_first_chunk::<OUTPUT_BYTES>() else {
        return Err(truncated(need, record));
    };
    let (offset, index) = fields.split_at(8);
    Ok(Record::Output(
        Written {
            header: OutputHeader {
                input_offset: u64::from_be_bytes(offset.try_into().unwrap()),
                index: u32::from_be_bytes(index.try_into().unwrap()),
            },
            epoch,
        },
        payload,
    ))
}

fn decode_outputs_only(record: &[u8]) -> Result<Record<'_>, DecodeError> {
    let need = 1 + OUTPUT_BYTES + 8;
    let Some((fields, payload)) = record[1..].split_first_chunk::<20>() else {
        return Err(truncated(need, record));
    };
    let (offset, rest) = fields.split_at(8);
    let (index, epoch) = rest.split_at(4);
    Ok(Record::Output(
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

fn truncated(need: usize, record: &[u8]) -> DecodeError {
    DecodeError::Truncated {
        need,
        got: record.len(),
    }
}

/// Whether `record` is a claim or a failure, which hold no output. A workload
/// reading another's output stream skips them, so only outputs reach its
/// `step`.
pub fn is_marker(record: &[u8]) -> bool {
    matches!(
        decode(record),
        Ok(Record::Claim { .. } | Record::Failure { .. })
    )
}

/// Whether two records hold the same output, whichever owner wrote them.
/// A record that does not decode, or holds no output, is the same as nothing.
pub fn same_output(a: &[u8], b: &[u8]) -> bool {
    let output = |r| decode(r).ok().and_then(Record::output);
    match (output(a), output(b)) {
        (Some((a, pa)), Some((b, pb))) => a.header == b.header && pa == pb,
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
    /// A failure record is there, written under `spec_version`.
    Failed { spec_version: u64, reason: String },
}

/// Decides what to do about records found at the expected tail.
///
/// A record from a later epoch, a claim included, wins over everything else.
/// Without that rule a stale owner that finds the new owner's identical
/// records would skip them and write after them, and two owners would take
/// turns on the stream. Then a failure record ends the run. Claims from
/// earlier epochs hold no output and are passed over.
pub fn reconcile(epoch: u64, batch: &[Vec<u8>], existing: &[&[u8]]) -> Reconciled {
    let mut found = Vec::with_capacity(existing.len());
    for &record in existing {
        let Ok(decoded) = decode(record) else {
            return Reconciled::Diverged;
        };
        found.push((decoded, record));
    }
    if let Some(later) = found
        .iter()
        .map(|(r, _)| r.epoch())
        .filter(|&e| e > epoch)
        .max()
    {
        return Reconciled::Superseded { epoch: later };
    }
    if let Some((spec_version, reason)) = found.iter().find_map(|(r, _)| match r {
        Record::Failure {
            spec_version,
            reason,
            ..
        } => Some((*spec_version, String::from_utf8_lossy(reason).into_owned())),
        _ => None,
    }) {
        return Reconciled::Failed {
            spec_version,
            reason,
        };
    }
    let outputs: Vec<&[u8]> = found
        .iter()
        .filter(|(r, _)| matches!(r, Record::Output(..)))
        .map(|&(_, record)| record)
        .collect();
    let compared = outputs.len().min(batch.len());
    if !outputs[..compared]
        .iter()
        .zip(batch)
        .all(|(found, ours)| same_output(found, ours))
    {
        return Reconciled::Diverged;
    }
    if outputs.len() > batch.len() {
        Reconciled::Behind
    } else {
        Reconciled::Skip(outputs.len())
    }
}

#[cfg(test)]
mod tests;
