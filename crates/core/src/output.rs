//! The records on a workload's output stream, and how an owner reconciles its
//! outputs with records it finds already written.
//!
//! Every record is `version (1) | kind (1) | epoch (8) | body`, integers big
//! endian, where `epoch` is the lease epoch of the owner that wrote it; see
//! [`crate::lease`]. The kinds:
//!
//! - An output: `input_offset (8) | index (4) | name length (1) | name |
//!   key flag (1) | [key length (4) | key] | payload`. Which input produced
//!   it and which of that step's outputs it is, so a new owner can tell what
//!   its predecessor already wrote.
//! - A claim: the owner's node id, which older claims leave out. A new owner
//!   appends one before it runs anything, so a stale owner's next conditional
//!   append is refused even if the new owner never writes an output.
//! - A claim under a spec: `spec (8) | node id`, a claim that also names the
//!   `sys.specs` offset of the spec record the owner runs, so the records
//!   after it are known to come from that spec.
//! - A failure: `spec_version (8) | reason`. The workload failed under that
//!   version of its spec. It is the last record of that run.
//! - A rejection: `input_offset (8) | index (4, always 0) | code (4) |
//!   detail`. It is that input's only output, and is matched, skipped and
//!   compared like one.
//!
//! Version 3 outputs have no name or key and read as name `out`. Version 2
//! records are outputs without a kind byte, with the epoch after the index.

use crate::codec::{DecodeError, Reader};
use crate::names::validate_id;

const VERSION: u8 = 4;
const UNNAMED: u8 = 3;
const OUTPUTS_ONLY: u8 = 2;
const OUTPUT: u8 = 0;
const CLAIM: u8 = 1;
const FAILURE: u8 = 2;
const REJECTION: u8 = 3;
const CLAIM_UNDER: u8 = 4;

/// The name an output gets when the workload gives none, and the one a
/// downstream workload reads unless its spec says otherwise.
pub const DEFAULT_NAME: &str = "out";

/// Where an output record came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OutputHeader {
    /// Offset of the input record whose step produced this output.
    pub input_offset: u64,
    /// Position among that step's outputs, from 0.
    pub index: u32,
}

/// An output or rejection record's header and who wrote it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Written {
    pub header: OutputHeader,
    /// The writer's lease epoch.
    pub epoch: u64,
}

/// An output's name, key and payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Addressed<'a> {
    /// 1 to 64 bytes of `a-z`, `0-9` and `-`.
    pub name: &'a str,
    pub key: Option<&'a [u8]>,
    pub payload: &'a [u8],
}

impl<'a> Addressed<'a> {
    /// An output under [`DEFAULT_NAME`] with no key.
    pub fn unnamed(payload: &'a [u8]) -> Self {
        Addressed {
            name: DEFAULT_NAME,
            key: None,
            payload,
        }
    }
}

/// Why a step's input was rejected. `detail` is for people.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rejected<'a> {
    pub code: u32,
    pub detail: &'a [u8],
}

/// A decoded record of an output stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Record<'a> {
    /// One of the workload's outputs.
    Output(Written, Addressed<'a>),
    /// The workload rejected the input at `header.input_offset`. Its index
    /// is always 0.
    Rejection(Written, Rejected<'a>),
    /// The owner at `epoch`, on node `node`, took the workload over here. It
    /// holds no output. `node` is empty in claims written before it was
    /// recorded. `spec` is the `sys.specs` offset of the spec record the
    /// owner runs, absent in claims written before specs were records.
    Claim {
        epoch: u64,
        node: &'a [u8],
        spec: Option<u64>,
    },
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
            Record::Output(written, _) | Record::Rejection(written, _) => written.epoch,
            Record::Claim { epoch, .. } | Record::Failure { epoch, .. } => *epoch,
        }
    }

    /// The header of an output or a rejection, the records a step produces.
    pub fn written(&self) -> Option<Written> {
        match self {
            Record::Output(written, _) | Record::Rejection(written, _) => Some(*written),
            Record::Claim { .. } | Record::Failure { .. } => None,
        }
    }

    /// The output, unless this is another kind of record.
    pub fn output(self) -> Option<(Written, Addressed<'a>)> {
        match self {
            Record::Output(written, addressed) => Some((written, addressed)),
            _ => None,
        }
    }
}

/// An output name that is not 1 to 64 bytes of `a-z`, `0-9` and `-`.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("an output name is 1 to 64 characters of a-z, 0-9 and '-'")]
pub struct InvalidName;

/// Checks an output name. Names follow the rules for workload ids.
pub fn validate_name(name: &str) -> Result<(), InvalidName> {
    validate_id(name).map_err(|_| InvalidName)
}

/// Builds an output record under [`DEFAULT_NAME`] with no key.
pub fn encode(header: OutputHeader, epoch: u64, payload: &[u8]) -> Vec<u8> {
    encode_addressed(header, epoch, Addressed::unnamed(payload)).expect("the default name is valid")
}

/// Builds an output record from its header, its writer's epoch and what the
/// workload returned.
pub fn encode_addressed(
    header: OutputHeader,
    epoch: u64,
    output: Addressed<'_>,
) -> Result<Vec<u8>, InvalidName> {
    validate_name(output.name)?;
    let key_bytes = output.key.map_or(0, |k| 4 + k.len());
    let mut out = Vec::with_capacity(24 + output.name.len() + key_bytes + output.payload.len());
    out.extend([VERSION, OUTPUT]);
    out.extend(epoch.to_be_bytes());
    out.extend(header.input_offset.to_be_bytes());
    out.extend(header.index.to_be_bytes());
    out.push(output.name.len() as u8);
    out.extend(output.name.as_bytes());
    match output.key {
        None => out.push(0),
        Some(key) => {
            out.push(1);
            out.extend(key_len(key).to_be_bytes());
            out.extend(key);
        }
    }
    out.extend(output.payload);
    Ok(out)
}

fn key_len(key: &[u8]) -> u32 {
    // A key that does not fit is far past what one write to Felix holds.
    u32::try_from(key.len()).unwrap_or(u32::MAX)
}

/// Builds the record an owner at `epoch` appends when the workload rejects
/// the input at `input_offset`.
pub fn encode_rejection(input_offset: u64, epoch: u64, rejected: Rejected<'_>) -> Vec<u8> {
    let mut out = Vec::with_capacity(26 + rejected.detail.len());
    out.extend([VERSION, REJECTION]);
    out.extend(epoch.to_be_bytes());
    out.extend(input_offset.to_be_bytes());
    out.extend(0u32.to_be_bytes());
    out.extend(rejected.code.to_be_bytes());
    out.extend(rejected.detail);
    out
}

/// Builds a claim that names no spec, the form written before specs were
/// records. Kept so readers and tests can build one.
pub fn encode_claim(epoch: u64, node: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(10 + node.len());
    out.extend([VERSION, CLAIM]);
    out.extend(epoch.to_be_bytes());
    out.extend(node.as_bytes());
    out
}

/// Builds the record an owner at `epoch` appends when it takes a workload
/// over. `node` names the owner, so a reader can tell which node wrote each
/// epoch's records, and `spec` is the `sys.specs` offset of the spec record
/// it runs.
pub fn encode_claim_under(epoch: u64, spec: u64, node: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(18 + node.len());
    out.extend([VERSION, CLAIM_UNDER]);
    out.extend(epoch.to_be_bytes());
    out.extend(spec.to_be_bytes());
    out.extend(node.as_bytes());
    out
}

/// Builds the record an owner at `epoch` appends when the workload fails
/// under spec version `spec_version`.
pub fn encode_failure(epoch: u64, spec_version: u64, reason: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(18 + reason.len());
    out.extend([VERSION, FAILURE]);
    out.extend(epoch.to_be_bytes());
    out.extend(spec_version.to_be_bytes());
    out.extend(reason.as_bytes());
    out
}

/// Reads an output stream record.
pub fn decode(record: &[u8]) -> Result<Record<'_>, DecodeError> {
    let mut r = Reader::new(record);
    let version = r.u8()?;
    if version == OUTPUTS_ONLY {
        let input_offset = r.u64()?;
        let index = r.u32()?;
        let epoch = r.u64()?;
        let header = OutputHeader {
            input_offset,
            index,
        };
        let payload = r.rest();
        return Ok(Record::Output(
            Written { header, epoch },
            Addressed::unnamed(payload),
        ));
    }
    if version != VERSION && version != UNNAMED {
        return Err(DecodeError::UnknownVersion(version));
    }
    let kind = r.u8()?;
    if !matches!(kind, OUTPUT | CLAIM | FAILURE)
        && !(matches!(kind, REJECTION | CLAIM_UNDER) && version == VERSION)
    {
        return Err(DecodeError::UnknownKind(kind));
    }
    let epoch = r.u64()?;
    match kind {
        CLAIM => Ok(Record::Claim {
            epoch,
            node: r.rest(),
            spec: None,
        }),
        CLAIM_UNDER => Ok(Record::Claim {
            epoch,
            spec: Some(r.u64()?),
            node: r.rest(),
        }),
        FAILURE => Ok(Record::Failure {
            epoch,
            spec_version: r.u64()?,
            reason: r.rest(),
        }),
        REJECTION => {
            let input_offset = r.u64()?;
            let index = r.u32()?;
            if index != 0 {
                return Err(DecodeError::RejectionIndex(index));
            }
            let code = r.u32()?;
            Ok(Record::Rejection(
                Written {
                    header: OutputHeader {
                        input_offset,
                        index,
                    },
                    epoch,
                },
                Rejected {
                    code,
                    detail: r.rest(),
                },
            ))
        }
        _ => {
            let header = OutputHeader {
                input_offset: r.u64()?,
                index: r.u32()?,
            };
            let written = Written { header, epoch };
            if version == UNNAMED {
                return Ok(Record::Output(written, Addressed::unnamed(r.rest())));
            }
            let name_len = r.u8()?;
            let name = std::str::from_utf8(r.bytes(name_len.into())?)
                .map_err(|_| DecodeError::InvalidName)?;
            validate_name(name).map_err(|_| DecodeError::InvalidName)?;
            let key = match r.u8()? {
                0 => None,
                1 => {
                    let len = r.u32()?;
                    Some(r.bytes(len as usize)?)
                }
                other => return Err(DecodeError::InvalidKeyFlag(other)),
            };
            Ok(Record::Output(
                written,
                Addressed {
                    name,
                    key,
                    payload: r.rest(),
                },
            ))
        }
    }
}

/// Whether `record` is a claim or a failure, which no step produced. The
/// owner's own checks pass over them; rejections are not passed over, since
/// they stand in for an input's outputs.
pub fn is_marker(record: &[u8]) -> bool {
    matches!(
        decode(record),
        Ok(Record::Claim { .. } | Record::Failure { .. })
    )
}

/// The output in `record` if it is addressed to `name`, for a workload whose
/// input is another workload's output stream. Claims, failures, rejections
/// and outputs of other names are `None`.
pub fn addressed_to<'a>(
    record: &'a [u8],
    name: &str,
) -> Result<Option<Addressed<'a>>, DecodeError> {
    Ok(decode(record)?
        .output()
        .map(|(_, addressed)| addressed)
        .filter(|addressed| addressed.name == name))
}

/// Whether a workload reading another's output stream for outputs named
/// `name` passes over `record`. A record that does not decode is not passed
/// over, so the step sees it and the reader does not silently drop records
/// of a newer format.
pub fn passed_over(record: &[u8], name: &str) -> bool {
    matches!(addressed_to(record, name), Ok(None))
}

/// Whether two records hold the same output or the same rejection, whichever
/// owner wrote them. A record that does not decode, or that no step
/// produced, is the same as nothing.
pub fn same_output(a: &[u8], b: &[u8]) -> bool {
    match (decode(a), decode(b)) {
        (Ok(a), Ok(b)) => same_produced(&a, &b),
        _ => false,
    }
}

fn same_produced(a: &Record<'_>, b: &Record<'_>) -> bool {
    match (a, b) {
        (Record::Output(wa, a), Record::Output(wb, b)) => wa.header == wb.header && a == b,
        (Record::Rejection(wa, a), Record::Rejection(wb, b)) => wa.header == wb.header && a == b,
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

/// Decides what to do about records found at the expected tail. `batch`
/// holds outputs or a rejection, which are compared alike.
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
        .filter(|(r, _)| r.written().is_some())
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
