//! The header on every record in a workload's output stream.
//!
//! A record is `version (1) | input_offset (8, big endian) | index (4, big
//! endian) | payload`. The header says which input produced the output and
//! which of that step's outputs it is, so a new owner can tell what its
//! predecessor already wrote.

use crate::codec::{DecodeError, VERSION, split_header};

/// Where an output record came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct OutputHeader {
    /// Offset of the input record whose step produced this output.
    pub input_offset: u64,
    /// Position among that step's outputs, from 0.
    pub index: u32,
}

/// Builds an output record from its header and the workload's payload.
pub fn encode(header: OutputHeader, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(13 + payload.len());
    out.push(VERSION);
    out.extend(header.input_offset.to_be_bytes());
    out.extend(header.index.to_be_bytes());
    out.extend(payload);
    out
}

/// Splits an output record into its header and payload.
pub fn decode(record: &[u8]) -> Result<(OutputHeader, &[u8]), DecodeError> {
    let (header, payload) = split_header::<12>(record)?;
    let (offset, index) = header.split_at(8);
    Ok((
        OutputHeader {
            input_offset: u64::from_be_bytes(offset.try_into().unwrap()),
            index: u32::from_be_bytes(index.try_into().unwrap()),
        },
        payload,
    ))
}

#[cfg(test)]
mod tests;
