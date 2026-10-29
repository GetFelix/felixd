//! The header on every record in a workload's snapshot stream.
//!
//! A record is `version (1) | input_offset (8, big endian) | output_tail (8,
//! big endian) | epoch (8, big endian) | state`, where `epoch` is the writer's
//! lease epoch and `state` is what the workload's `snapshot` returned. Version
//! 1 records have no epoch and read as epoch 0.

use crate::codec::{DecodeError, split_header};

const VERSION: u8 = 2;
const UNSTAMPED: u8 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotHeader {
    /// Offset of the last input the state includes.
    pub input_offset: u64,
    /// The output stream's next offset when the snapshot was taken: every
    /// output of inputs up to `input_offset` lies below it.
    pub output_tail: u64,
}

/// A decoded snapshot header and the epoch of the owner that wrote it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Taken {
    pub header: SnapshotHeader,
    pub epoch: u64,
}

/// Builds a snapshot record from its header, the writer's epoch and the
/// workload's state.
pub fn encode(header: SnapshotHeader, epoch: u64, state: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(25 + state.len());
    out.push(VERSION);
    out.extend(header.input_offset.to_be_bytes());
    out.extend(header.output_tail.to_be_bytes());
    out.extend(epoch.to_be_bytes());
    out.extend(state);
    out
}

/// Splits a snapshot record into its header and the workload's state.
pub fn decode(record: &[u8]) -> Result<(Taken, &[u8]), DecodeError> {
    if record.first() == Some(&UNSTAMPED) {
        let (header, state) = split_header::<16>(record, UNSTAMPED)?;
        return Ok((
            Taken {
                header: read_header(header),
                epoch: 0,
            },
            state,
        ));
    }
    let (fields, state) = split_header::<24>(record, VERSION)?;
    let (header, epoch) = fields.split_at(16);
    Ok((
        Taken {
            header: read_header(header),
            epoch: u64::from_be_bytes(epoch.try_into().unwrap()),
        },
        state,
    ))
}

fn read_header(fields: &[u8]) -> SnapshotHeader {
    let (input_offset, output_tail) = fields.split_at(8);
    SnapshotHeader {
        input_offset: u64::from_be_bytes(input_offset.try_into().unwrap()),
        output_tail: u64::from_be_bytes(output_tail.try_into().unwrap()),
    }
}

#[cfg(test)]
mod tests;
