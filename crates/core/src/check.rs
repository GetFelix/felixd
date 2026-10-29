//! What `felixd check` finds wrong with an output stream, given the outputs
//! a replay of the module over the input produced.
//!
//! The replay says what the stream should hold, in order. Every record on the
//! stream is matched to a replayed output by its header, and anything that
//! does not match up is a finding.

use std::collections::HashMap;
use std::fmt;

use crate::output::{self, OutputHeader};
use crate::snapshot::{self, SnapshotHeader};

/// One output a replay produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Expected {
    pub header: OutputHeader,
    pub payload: Vec<u8>,
}

/// Something wrong with an output stream. `offset` is the record's offset on
/// the output stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Finding {
    /// A replayed output that is not on the stream, though a later one is.
    Gap { header: OutputHeader },
    /// A second record for an output already on the stream.
    Duplicate {
        offset: u64,
        header: OutputHeader,
        first: u64,
    },
    /// A record for an output that comes before one already on the stream.
    OutOfOrder {
        offset: u64,
        header: OutputHeader,
        after: OutputHeader,
    },
    /// A record whose writer's epoch is below the previous record's.
    EpochRegression {
        offset: u64,
        epoch: u64,
        previous: u64,
    },
    /// A record whose payload differs from the replay's.
    Differs { offset: u64, header: OutputHeader },
    /// A record for an output the replay did not produce.
    Unexpected { offset: u64, header: OutputHeader },
    /// A record that is not an output record.
    Undecodable { offset: u64, error: String },
    /// Replayed outputs missing from the end of the stream. Only a finding
    /// when the stream should be complete.
    Behind { missing: usize, first: OutputHeader },
    /// A snapshot whose output tail does not split the output stream at its
    /// input: a resume from it would skip or repeat outputs. The tail should
    /// lie in `from..=to`. `offset` is on the snapshot stream.
    SnapshotTail {
        offset: u64,
        header: SnapshotHeader,
        from: u64,
        to: u64,
    },
    /// A record on the snapshot stream that is not a snapshot.
    UndecodableSnapshot { offset: u64, error: String },
}

/// Checks the records on an output stream, as `(offset, record)` in offset
/// order, against what a replay produced. `complete` says the stream should
/// already hold every replayed output.
pub fn check(expected: &[Expected], records: &[(u64, &[u8])], complete: bool) -> Vec<Finding> {
    let position: HashMap<OutputHeader, usize> = expected
        .iter()
        .enumerate()
        .map(|(i, e)| (e.header, i))
        .collect();
    let mut seen: HashMap<OutputHeader, u64> = HashMap::new();
    let mut findings = Vec::new();
    // The furthest replayed output found so far, and the last epoch.
    let mut furthest: Option<usize> = None;
    let mut last_epoch: Option<u64> = None;
    for &(offset, record) in records {
        let (written, payload) = match output::decode(record) {
            Ok(decoded) => decoded,
            Err(error) => {
                findings.push(Finding::Undecodable {
                    offset,
                    error: error.to_string(),
                });
                continue;
            }
        };
        let header = written.header;
        if let Some(previous) = last_epoch
            && written.epoch < previous
        {
            findings.push(Finding::EpochRegression {
                offset,
                epoch: written.epoch,
                previous,
            });
        }
        last_epoch = Some(last_epoch.map_or(written.epoch, |e| e.max(written.epoch)));
        let Some(&at) = position.get(&header) else {
            findings.push(Finding::Unexpected { offset, header });
            continue;
        };
        if let Some(&first) = seen.get(&header) {
            findings.push(Finding::Duplicate {
                offset,
                header,
                first,
            });
            continue;
        }
        seen.insert(header, offset);
        if payload != expected[at].payload.as_slice() {
            findings.push(Finding::Differs { offset, header });
        }
        match furthest {
            Some(f) if at < f => findings.push(Finding::OutOfOrder {
                offset,
                header,
                after: expected[f].header,
            }),
            _ => furthest = Some(at),
        }
    }
    let end = furthest.map_or(0, |f| f + 1);
    for e in &expected[..end] {
        if !seen.contains_key(&e.header) {
            findings.push(Finding::Gap { header: e.header });
        }
    }
    if complete && end < expected.len() {
        findings.push(Finding::Behind {
            missing: expected.len() - end,
            first: expected[end].header,
        });
    }
    findings
}

/// Checks each record on a snapshot stream, as `(offset, record)`, against the
/// output stream's `records` and `output_tail`, read after the snapshots. A
/// snapshot of input X must record an output tail with only outputs of inputs
/// up to X below it and only later ones from it on.
pub fn check_snapshots(
    snapshots: &[(u64, &[u8])],
    records: &[(u64, &[u8])],
    output_tail: u64,
) -> Vec<Finding> {
    let outputs: Vec<(u64, u64)> = records
        .iter()
        .filter_map(|&(offset, record)| {
            let (written, _) = output::decode(record).ok()?;
            Some((offset, written.header.input_offset))
        })
        .collect();
    // Both are monotonic, so each bound is a binary search.
    let mut max_before = Vec::with_capacity(outputs.len());
    for &(_, input) in &outputs {
        max_before.push(max_before.last().map_or(input, |&m: &u64| m.max(input)));
    }
    let mut min_after = vec![0; outputs.len()];
    let mut min = u64::MAX;
    for (i, &(_, input)) in outputs.iter().enumerate().rev() {
        min = min.min(input);
        min_after[i] = min;
    }
    let mut findings = Vec::new();
    for &(offset, record) in snapshots {
        let header = match snapshot::decode(record) {
            Ok((header, _)) => header,
            Err(error) => {
                findings.push(Finding::UndecodableSnapshot {
                    offset,
                    error: error.to_string(),
                });
                continue;
            }
        };
        let covered = header.input_offset;
        let last_covered = min_after.partition_point(|&m| m <= covered);
        let from = last_covered.checked_sub(1).map_or(0, |i| outputs[i].0 + 1);
        let first_later = max_before.partition_point(|&m| m <= covered);
        let to = outputs.get(first_later).map_or(output_tail, |o| o.0);
        if header.output_tail < from || header.output_tail > to {
            findings.push(Finding::SnapshotTail {
                offset,
                header,
                from,
                to,
            });
        }
    }
    findings
}

impl fmt::Display for Finding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let out = |h: &OutputHeader| format!("input {} output {}", h.input_offset, h.index);
        match self {
            Finding::Gap { header } => write!(f, "gap: {} is missing", out(header)),
            Finding::Duplicate {
                offset,
                header,
                first,
            } => write!(
                f,
                "duplicate at offset {offset}: {} is already at offset {first}",
                out(header)
            ),
            Finding::OutOfOrder {
                offset,
                header,
                after,
            } => write!(
                f,
                "out of order at offset {offset}: {} comes after {}",
                out(header),
                out(after)
            ),
            Finding::EpochRegression {
                offset,
                epoch,
                previous,
            } => write!(
                f,
                "epoch regression at offset {offset}: epoch {epoch} after epoch {previous}"
            ),
            Finding::Differs { offset, header } => write!(
                f,
                "differs at offset {offset}: {} is not what the replay produced",
                out(header)
            ),
            Finding::Unexpected { offset, header } => write!(
                f,
                "unexpected at offset {offset}: the replay produced no {}",
                out(header)
            ),
            Finding::Undecodable { offset, error } => {
                write!(f, "undecodable at offset {offset}: {error}")
            }
            Finding::Behind { missing, first } => write!(
                f,
                "behind: {missing} outputs not written, from {}",
                out(first)
            ),
            Finding::SnapshotTail {
                offset,
                header,
                from,
                to,
            } => write!(
                f,
                "snapshot at offset {offset} covers input {} with output tail {}, \
                 but the outputs of later inputs start in {from}..={to}",
                header.input_offset, header.output_tail
            ),
            Finding::UndecodableSnapshot { offset, error } => {
                write!(f, "undecodable snapshot at offset {offset}: {error}")
            }
        }
    }
}

#[cfg(test)]
mod tests;
