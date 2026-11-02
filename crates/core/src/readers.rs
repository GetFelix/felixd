//! How far a stream shared between workloads may be deleted.
//!
//! One workload's trim record says what its own resume needs. A stream
//! another workload reads, an input or a workload's `.out`, may be deleted only
//! below the lowest floor of every workload that reads it, and a reader
//! nothing is known about counts as needing all of it. Does no I/O: the agent
//! reads the workloads and their last trim records and hands them in as
//! [`Known`].

use crate::names;

/// One workload, as far as the deletion rule can tell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Known {
    /// The inputs its spec lists. `None` when the spec did not decode, so
    /// nothing says what it reads.
    pub inputs: Option<Vec<String>>,
    /// The input floors of the last trim record on its `.out`, in the spec's
    /// order. `None` when it has no trim record or reading it failed.
    pub floors: Option<Vec<u64>>,
}

impl Known {
    /// The floor this workload needs `stream` held from, `None` if it does
    /// not read it. Zero when nothing shows how far it has got.
    fn floor_on(&self, stream: &str) -> Option<u64> {
        let Some(inputs) = &self.inputs else {
            return Some(0);
        };
        let floors = self.floors.as_deref().filter(|f| f.len() == inputs.len());
        inputs
            .iter()
            .enumerate()
            .filter(|(_, input)| *input == stream)
            .map(|(at, _)| floors.map_or(0, |f| f[at]))
            .min()
    }
}

/// The lowest floor among the workloads that read `stream`, or `None` when no
/// workload does. Delete `stream` below it and no reader loses a record it
/// needs.
pub fn lowest_floor(stream: &str, workloads: &[Known]) -> Option<u64> {
    workloads.iter().filter_map(|w| w.floor_on(stream)).min()
}

/// Where a workload's `.out`, `stream`, may be deleted to: its own need, the
/// output tail of the oldest snapshot it keeps, lowered to the lowest floor of
/// the workloads that read it.
pub fn output_floor(own_need: u64, stream: &str, workloads: &[Known]) -> u64 {
    lowest_floor(stream, workloads).map_or(own_need, |readers| readers.min(own_need))
}

/// Whether a workload's holder may delete input `stream` on the readers' word.
/// Streams of the system namespace are the scheduler's own, and `wl.*` streams
/// belong to a workload, whose holder alone knows how far its owner needs
/// them ([`output_floor`]).
pub fn deletable_input(stream: &str) -> bool {
    !stream.starts_with(names::SYSTEM_PREFIX) && !stream.starts_with("wl.")
}

#[cfg(test)]
mod tests;
