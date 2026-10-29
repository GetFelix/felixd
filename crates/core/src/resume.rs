//! The resume fold: where a new owner picks a workload up.
//!
//! The owner reads the latest snapshot header and the last output record of
//! the output stream and passes both here. Felix also writes records that are
//! not events, such as the generation-start record a new shard leader appends,
//! so the stream's tail can be past its last output. The fold therefore
//! compares the last output's offset, never the tail, with the snapshot. The result says which input offset to
//! subscribe from and which outputs the stream already holds. The owner still
//! runs `step` for those inputs, checks each output it skips with
//! [`check_skipped`] against the record in its place, and takes no snapshot
//! until it is past them. Replay uses the same fold, so a replayed run skips
//! and compares exactly what a failover would.

use thiserror::Error;

use crate::output::OutputHeader;
use crate::snapshot::SnapshotHeader;

/// What a new owner knows when it starts.
#[derive(Clone, Copy, Debug)]
pub struct Found {
    /// The latest snapshot, if the workload has one.
    pub snapshot: Option<SnapshotHeader>,
    /// The last output record on the output stream, if it has any.
    pub last_output: Option<LastOutput>,
    /// The output stream's next offset, counting records that are not events.
    pub output_tail: u64,
}

/// The last output record on the output stream and where it is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LastOutput {
    pub offset: u64,
    pub header: OutputHeader,
}

/// How to resume.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Resume {
    /// Whether to restore the snapshot first. Without one, the workload starts
    /// from its initial state.
    pub restore: bool,
    /// The input offset to subscribe from.
    pub next_input: u64,
    /// The output stream offset at which the outputs the owner skips begin.
    pub output_from: u64,
    /// Outputs up to and including this position are already on the stream.
    skip_through: Option<OutputHeader>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ResumeError {
    /// The snapshot counts more outputs than the stream holds, so some were lost.
    #[error("snapshot expects the output stream to reach offset {expected}, but it ends at {tail}")]
    OutputBehindSnapshot { expected: u64, tail: u64 },
    /// An output past the snapshot's tail is attributed to an input the
    /// snapshot already covers, which a correct owner never writes.
    #[error("output stream holds input {found}'s output past a snapshot covering input {covered}")]
    OutputBeforeSnapshot { covered: u64, found: u64 },
    /// Running the workload again did not produce what the stream holds, so
    /// the workload is not deterministic or the stream was written by
    /// something else.
    #[error("replayed output differs from the record at offset {offset} of the output stream")]
    Diverged { offset: u64 },
}

impl Resume {
    /// Whether the output of input `input_offset` at `index` is already on the
    /// stream, so the owner must run the step and drop that output.
    pub fn already_written(&self, input_offset: u64, index: u32) -> bool {
        self.skip_through.is_some_and(|last| {
            OutputHeader {
                input_offset,
                index,
            } <= last
        })
    }

    /// Whether the stream holds outputs of inputs after `input_offset`. Until
    /// the owner is past them, the stream's tail is ahead of the workload's
    /// state, and a snapshot would record the wrong output tail.
    pub fn replaying(&self, input_offset: u64) -> bool {
        self.skip_through
            .is_some_and(|last| input_offset < last.input_offset)
    }
}

/// Checks an output the owner produced and skipped against `found`, the
/// record at `offset` of the output stream that should hold the same output.
/// The writer's epoch may differ. `None` means the stream ran out first.
pub fn check_skipped(
    offset: u64,
    replayed: &[u8],
    found: Option<&[u8]>,
) -> Result<(), ResumeError> {
    if found.is_some_and(|found| crate::output::same_output(found, replayed)) {
        Ok(())
    } else {
        Err(ResumeError::Diverged { offset })
    }
}

/// Works out how to resume from what [`Found`] holds.
pub fn fold(found: Found) -> Result<Resume, ResumeError> {
    let (restore, next_input, snapshot_tail) = match found.snapshot {
        Some(s) => (true, s.input_offset + 1, s.output_tail),
        None => (false, 0, 0),
    };
    if found.output_tail < snapshot_tail {
        return Err(ResumeError::OutputBehindSnapshot {
            expected: snapshot_tail,
            tail: found.output_tail,
        });
    }
    let skip_through = match found.last_output {
        None => None,
        Some(last) if last.offset >= snapshot_tail => {
            if let Some(s) = found.snapshot
                && last.header.input_offset <= s.input_offset
            {
                return Err(ResumeError::OutputBeforeSnapshot {
                    covered: s.input_offset,
                    found: last.header.input_offset,
                });
            }
            Some(last.header)
        }
        // The snapshot already includes every output on the stream.
        Some(_) => None,
    };
    Ok(Resume {
        restore,
        next_input,
        output_from: snapshot_tail,
        skip_through,
    })
}

#[cfg(test)]
mod tests;
