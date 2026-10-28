//! The header on every record in a workload's snapshot stream.
//!
//! A record is `version (1) | input_offset (8, big endian) | output_tail (8,
//! big endian) | state`, where `state` is what the workload's `snapshot`
//! returned.

use crate::codec::{DecodeError, VERSION, split_header};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotHeader {
    /// Offset of the last input the state includes.
    pub input_offset: u64,
    /// The output stream's next offset when the snapshot was taken: every
    /// output of inputs up to `input_offset` lies below it.
    pub output_tail: u64,
}

/// Builds a snapshot record from its header and the workload's state.
pub fn encode(header: SnapshotHeader, state: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(17 + state.len());
    out.push(VERSION);
    out.extend(header.input_offset.to_be_bytes());
    out.extend(header.output_tail.to_be_bytes());
    out.extend(state);
    out
}

/// Splits a snapshot record into its header and the workload's state.
pub fn decode(record: &[u8]) -> Result<(SnapshotHeader, &[u8]), DecodeError> {
    let (header, state) = split_header::<16>(record)?;
    let (input_offset, output_tail) = header.split_at(8);
    Ok((
        SnapshotHeader {
            input_offset: u64::from_be_bytes(input_offset.try_into().unwrap()),
            output_tail: u64::from_be_bytes(output_tail.try_into().unwrap()),
        },
        state,
    ))
}

#[cfg(test)]
mod tests;
