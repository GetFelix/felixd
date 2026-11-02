//! Where a replay starts and where it stops.
//!
//! A replay runs the inputs below an end offset. It restores the latest
//! snapshot that covers only inputs below that end and runs the rest. A diff
//! compares what it produces with the outputs on the stream.

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;

use crate::check::Produced;
use crate::codec::DecodeError;
use crate::module::{self, Unreadable};
use crate::output::{self, OutputHeader, Record};
use crate::snapshot::SnapshotHeader;

/// The end of a replay to a time, from what `offset_for_time` answered: the
/// first input offset appended at or after the time, or `None` when no input
/// is that recent, in which case every input below `input_tail` counts.
pub fn end_for_time(first_at_or_after: Option<u64>, input_tail: u64) -> u64 {
    first_at_or_after.unwrap_or(input_tail).min(input_tail)
}

/// Which of `snapshots`, in stream order, a replay to `end` restores: the one
/// with the highest input offset below `end`. `None` means the replay starts
/// from the initial state.
pub fn nearest_snapshot(
    snapshots: impl IntoIterator<Item = SnapshotHeader>,
    end: u64,
) -> Option<usize> {
    let mut best: Option<(usize, u64)> = None;
    for (index, header) in snapshots.into_iter().enumerate() {
        if header.input_offset >= end {
            continue;
        }
        if best.is_none_or(|(_, offset)| header.input_offset >= offset) {
            best = Some((index, header.input_offset));
        }
    }
    best.map(|(index, _)| index)
}

/// The first input a replay runs after restoring `snapshot`, or from the
/// initial state.
pub fn first_input(snapshot: Option<SnapshotHeader>) -> u64 {
    snapshot.map_or(0, |s| s.input_offset + 1)
}

/// What a replay's state began as.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Start {
    /// The snapshot with this header, restored.
    Snapshot(SnapshotHeader),
    /// The initial state.
    Initial(Initial),
}

/// Why a replay began from the initial state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Initial {
    /// No snapshot covers only inputs below the replay's first.
    NoSnapshot,
    /// The user asked for the whole history.
    Requested,
    /// The module being tried cannot restore the snapshots the recorded
    /// module wrote.
    Unreadable(Unreadable),
}

impl Start {
    /// The snapshot restored, if any.
    pub fn snapshot(&self) -> Option<SnapshotHeader> {
        match self {
            Start::Snapshot(header) => Some(*header),
            Start::Initial(_) => None,
        }
    }
}

impl fmt::Display for Start {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Start::Snapshot(header) => write!(f, "the snapshot of input {}", header.input_offset),
            Start::Initial(Initial::NoSnapshot) => f.write_str("the initial state"),
            Start::Initial(Initial::Requested) => f.write_str("the initial state, as asked"),
            Start::Initial(Initial::Unreadable(why)) => {
                write!(f, "the initial state, since {why}")
            }
        }
    }
}

/// The outputs an output stream records for a range of inputs, and the nodes
/// its claims name.
#[derive(Debug, Default)]
pub struct Recorded {
    outputs: BTreeMap<OutputHeader, Kept>,
    nodes: BTreeMap<u64, String>,
    noted: BTreeMap<OutputHeader, Noted>,
}

#[derive(Debug)]
struct Kept {
    epoch: u64,
    summary: Summary,
}

/// A divergence record on the stream: a holder already found the output at
/// its header differing from its own replay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Noted {
    /// The record's offset on the output stream.
    pub offset: u64,
    pub epoch: u64,
    pub node: String,
    /// The build that wrote it: `contract/wasmtime/felixd` versions.
    pub build: String,
}

/// What a diff compares of an output or a rejection: its address and the
/// digest of its bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Summary {
    Output {
        name: String,
        key: Option<Vec<u8>>,
        /// The sha256 of the payload.
        sha256: String,
    },
    Rejection {
        code: u32,
        /// The sha256 of the detail.
        sha256: String,
    },
}

impl From<&Produced> for Summary {
    fn from(produced: &Produced) -> Summary {
        match produced {
            Produced::Output { name, key, payload } => Summary::Output {
                name: name.clone(),
                key: key.clone(),
                sha256: digest(payload),
            },
            Produced::Rejection { code, detail } => Summary::Rejection {
                code: *code,
                sha256: digest(detail),
            },
        }
    }
}

/// The first place a replay and the stream disagree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Divergence {
    pub header: OutputHeader,
    /// The recorded output's writer, when there is one.
    pub writer: Option<Writer>,
    /// The divergence record the stream holds for this output, when a holder
    /// wrote one.
    pub noted: Option<Noted>,
    /// The recorded output or rejection, when there is one.
    pub recorded: Option<Summary>,
    /// What the replay produced there, when it produced something.
    pub replayed: Option<Summary>,
}

/// Who wrote a record: its epoch, and the node its epoch's claim names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Writer {
    pub epoch: u64,
    pub node: Option<String>,
}

impl Recorded {
    /// Reads the output stream's records, in stream order with their
    /// offsets, keeping outputs of inputs in `inputs`. A second record for one
    /// output is `felixd check`'s business; the first one counts here. A
    /// divergence record is kept for the output it names, the last one
    /// winning, whatever the inputs; one this build cannot read is passed over.
    pub fn read<'a>(
        records: impl IntoIterator<Item = (u64, &'a [u8])>,
        inputs: Range<u64>,
    ) -> Result<Recorded, DecodeError> {
        let mut recorded = Recorded::default();
        for (offset, record) in records {
            if output::divergence_spec_of(record).is_some() && output::decode(record).is_err() {
                continue;
            }
            let decoded = output::decode(record)?;
            if let Record::Divergence { epoch, found } = &decoded {
                let text = |bytes: &[u8]| String::from_utf8_lossy(bytes).into_owned();
                recorded.noted.insert(
                    found.header,
                    Noted {
                        offset,
                        epoch: *epoch,
                        node: text(found.node),
                        build: format!(
                            "contract {}, wasmtime {}, felixd {}",
                            text(found.abi.contract),
                            text(found.abi.wasmtime),
                            text(found.abi.felixd)
                        ),
                    },
                );
            }
            if let Record::Claim { epoch, node, .. } = decoded
                && !node.is_empty()
            {
                let node = String::from_utf8_lossy(node).into_owned();
                recorded.nodes.insert(epoch, node);
            }
            let Some(produced) = Produced::of(&decoded) else {
                continue;
            };
            let written = decoded.written().expect("an output or a rejection");
            if inputs.contains(&written.header.input_offset) {
                recorded.outputs.entry(written.header).or_insert(Kept {
                    epoch: written.epoch,
                    summary: Summary::from(&produced),
                });
            }
        }
        Ok(recorded)
    }

    /// The input after the last one with a recorded output. The workload may
    /// not have reached later inputs yet, so a diff stops there.
    pub fn end(&self) -> u64 {
        self.outputs
            .keys()
            .next_back()
            .map_or(0, |h| h.input_offset + 1)
    }

    /// Compares what a replay produced for the input at `input_offset`, its
    /// outputs or its one rejection, with what is recorded.
    pub fn compare(&self, input_offset: u64, outputs: &[Produced]) -> Option<Divergence> {
        for (index, produced) in outputs.iter().enumerate() {
            let header = OutputHeader {
                input_offset,
                index: u32::try_from(index).ok()?,
            };
            let replayed = Summary::from(produced);
            match self.outputs.get(&header) {
                Some(kept) if kept.summary == replayed => {}
                kept => return Some(self.divergence(header, kept, Some(replayed))),
            }
        }
        let header = OutputHeader {
            input_offset,
            index: u32::try_from(outputs.len()).ok()?,
        };
        let extra = self.outputs.get(&header)?;
        Some(self.divergence(header, Some(extra), None))
    }

    fn divergence(
        &self,
        header: OutputHeader,
        kept: Option<&Kept>,
        replayed: Option<Summary>,
    ) -> Divergence {
        Divergence {
            header,
            writer: kept.map(|k| Writer {
                epoch: k.epoch,
                node: self.nodes.get(&k.epoch).cloned(),
            }),
            noted: self.noted.get(&header).cloned(),
            recorded: kept.map(|k| k.summary.clone()),
            replayed,
        }
    }
}

/// How payloads are compared: their sha256, in lowercase hex.
pub fn digest(payload: &[u8]) -> String {
    module::hash(payload)
}

impl fmt::Display for Summary {
    /// An output under the default name with no key shows as its digest
    /// alone.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Summary::Output { name, key, sha256 } => {
                if name != output::DEFAULT_NAME || key.is_some() {
                    write!(f, "name {name}, ")?;
                    match key {
                        Some(key) => write!(f, "key {}, ", hex(key))?,
                        None => f.write_str("no key, ")?,
                    }
                }
                write!(f, "sha256 {sha256}")
            }
            Summary::Rejection { code, sha256 } => {
                write!(f, "a rejection with code {code} and detail sha256 {sha256}")
            }
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

impl fmt::Display for Divergence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let OutputHeader {
            input_offset,
            index,
        } = self.header;
        write!(f, "input {input_offset} output {index}: ")?;
        match (&self.writer, &self.recorded) {
            (Some(writer), Some(recorded)) => {
                let node = writer.node.as_deref().unwrap_or("unknown");
                write!(
                    f,
                    "recorded by node {node} at epoch {} with {recorded}",
                    writer.epoch
                )?;
            }
            _ => f.write_str("not on the stream")?,
        }
        match &self.replayed {
            Some(replayed) => write!(f, ", replayed with {replayed}")?,
            None => f.write_str(", and the replay produced none")?,
        }
        if let Some(noted) = &self.noted {
            write!(
                f,
                "; a divergence record at offset {} says node {} found this at epoch {} ({})",
                noted.offset, noted.node, noted.epoch, noted.build
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
