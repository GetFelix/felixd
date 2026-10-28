//! The resume fold: where a new owner picks a workload up.
//!
//! The owner reads the latest snapshot header and the last record of the
//! output stream and passes both here. The result says which input offset to
//! subscribe from and which outputs the stream already holds. Replay uses the
//! same fold, so a replayed run skips and compares exactly what a failover
//! would.

use thiserror::Error;

use crate::output::OutputHeader;
use crate::snapshot::SnapshotHeader;

/// What a new owner knows when it starts.
#[derive(Clone, Copy, Debug)]
pub struct Found {
    /// The latest snapshot, if the workload has one.
    pub snapshot: Option<SnapshotHeader>,
    /// The header of the last record on the output stream, if it has any.
    pub last_output: Option<OutputHeader>,
    /// The output stream's next offset.
    pub output_tail: u64,
}

/// How to resume.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Resume {
    /// Whether to restore the snapshot first. Without one, the workload starts
    /// from its initial state.
    pub restore: bool,
    /// The input offset to subscribe from.
    pub next_input: u64,
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
    #[error("output stream has records but no last output header was given")]
    MissingLastOutput,
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
    let has_later_outputs = found.output_tail > snapshot_tail;
    let skip_through = match found.last_output {
        None if found.output_tail > 0 => return Err(ResumeError::MissingLastOutput),
        None => None,
        Some(last) if has_later_outputs => {
            if let Some(s) = found.snapshot
                && last.input_offset <= s.input_offset
            {
                return Err(ResumeError::OutputBeforeSnapshot {
                    covered: s.input_offset,
                    found: last.input_offset,
                });
            }
            Some(last)
        }
        // The snapshot already includes everything on the stream.
        Some(_) => None,
    };
    Ok(Resume {
        restore,
        next_input,
        skip_through,
    })
}

#[cfg(test)]
mod tests;
