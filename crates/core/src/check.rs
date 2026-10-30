//! What `felixd check` finds wrong with an output stream, given the outputs
//! a replay of the module over the input produced.
//!
//! The replay says what the stream should hold, in order. Every output record
//! on the stream is matched to a replayed output by its header, and anything
//! that does not match up is a finding. A rejection is matched like an
//! output. Claims and failures hold no output, so only their epochs are
//! checked.

use std::collections::HashMap;
use std::fmt;

use crate::merge;
use crate::names;
use crate::output::{self, OutputHeader, Record};
use crate::snapshot::{self, SnapshotHeader};
use crate::spec::{Limits, WorkloadSpec};

/// One output or rejection a replay produced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Expected {
    pub header: OutputHeader,
    pub produced: Produced,
}

/// What a step produced at one header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Produced {
    Output {
        name: String,
        key: Option<Vec<u8>>,
        payload: Vec<u8>,
    },
    Rejection {
        code: u32,
        detail: Vec<u8>,
    },
}

impl Expected {
    /// An output under [`output::DEFAULT_NAME`] with no key.
    pub fn output(header: OutputHeader, payload: Vec<u8>) -> Self {
        Expected {
            header,
            produced: Produced::unnamed(payload),
        }
    }

    /// The rejection of the input at `input_offset`.
    pub fn rejection(input_offset: u64, code: u32, detail: Vec<u8>) -> Self {
        Expected {
            header: OutputHeader {
                input_offset,
                index: 0,
            },
            produced: Produced::Rejection { code, detail },
        }
    }
}

impl Produced {
    /// An output under [`output::DEFAULT_NAME`] with no key.
    pub fn unnamed(payload: Vec<u8>) -> Self {
        Produced::Output {
            name: output::DEFAULT_NAME.to_owned(),
            key: None,
            payload,
        }
    }

    /// What `record` holds, if it is an output or a rejection.
    pub fn of(record: &Record<'_>) -> Option<Self> {
        match record {
            Record::Output(_, found) => Some(Produced::Output {
                name: found.name.to_owned(),
                key: found.key.map(<[u8]>::to_vec),
                payload: found.payload.to_vec(),
            }),
            Record::Rejection(_, found) => Some(Produced::Rejection {
                code: found.code,
                detail: found.detail.to_vec(),
            }),
            Record::Claim { .. } | Record::Failure { .. } | Record::NeedsOperator { .. } => None,
        }
    }

    /// The record an owner at `epoch` writes for this at `header`.
    pub fn encode(&self, header: OutputHeader, epoch: u64) -> Result<Vec<u8>, output::InvalidName> {
        match self {
            Produced::Output { name, key, payload } => output::encode_addressed(
                header,
                epoch,
                output::Addressed {
                    name,
                    key: key.as_deref(),
                    payload,
                },
            ),
            Produced::Rejection { code, detail } => Ok(output::encode_rejection(
                header.input_offset,
                epoch,
                output::Rejected {
                    code: *code,
                    detail,
                },
            )),
        }
    }

    /// Whether `record` holds this, whatever its header and epoch.
    fn matches(&self, record: &Record<'_>) -> bool {
        match (self, record) {
            (Produced::Output { name, key, payload }, Record::Output(_, found)) => {
                found.name == name
                    && found.key == key.as_deref()
                    && found.payload == payload.as_slice()
            }
            (Produced::Rejection { code, detail }, Record::Rejection(_, found)) => {
                found.code == *code && found.detail == detail.as_slice()
            }
            _ => false,
        }
    }
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
    /// A record, output or claim, whose writer's epoch is below an earlier
    /// record's.
    EpochRegression {
        offset: u64,
        epoch: u64,
        previous: u64,
    },
    /// A record that differs from what the replay produced, including an
    /// output where it rejected the input or the other way round.
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
    /// A record on the merge log that is not a merge record. `offset` is on
    /// the merge log, as for every merge finding.
    UndecodableMerge { offset: u64, error: String },
    /// A merge record whose writer's epoch is below an earlier one's.
    MergeEpochRegression {
        offset: u64,
        epoch: u64,
        previous: u64,
    },
    /// A merge entry that does not name the next record of its input:
    /// `expected`, or none when the input holds no more. It merges a
    /// record twice, out of order or one that is not there, or passes over
    /// records of that input.
    MergeOrder {
        offset: u64,
        input: String,
        named: u64,
        expected: Option<u64>,
    },
    /// A merge record whose next offsets are not where the records before it
    /// leave each input.
    MergeCursor { offset: u64 },
    /// A merge entry naming an input the spec does not have.
    MergeUnknownInput { offset: u64, input: u16 },
    /// A merge entry whose append time is not its record's.
    MergeTimestamp {
        offset: u64,
        input: String,
        named: u64,
        recorded: u64,
        found: u64,
    },
    /// A merge entry of an epoch whose claim is not the last claim before it.
    MergeUnclaimed { offset: u64, epoch: u64 },
    /// Records of an input that are not on the merge log. Only a finding
    /// when the log should be complete.
    MergeBehind {
        input: String,
        missing: usize,
        first: u64,
    },
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
        let decoded = match output::decode(record) {
            Ok(decoded) => decoded,
            Err(error) => {
                findings.push(Finding::Undecodable {
                    offset,
                    error: error.to_string(),
                });
                continue;
            }
        };
        let epoch = decoded.epoch();
        if let Some(previous) = last_epoch
            && epoch < previous
        {
            findings.push(Finding::EpochRegression {
                offset,
                epoch,
                previous,
            });
        }
        last_epoch = Some(last_epoch.map_or(epoch, |e| e.max(epoch)));
        let Some(written) = decoded.written() else {
            continue;
        };
        let header = written.header;
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
        if !expected[at].produced.matches(&decoded) {
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
            let written = output::decode(record).ok()?.written()?;
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
            Ok(decoded) => match decoded.taken() {
                Some(taken) => taken.header,
                // A chunk is part of a snapshot, not one.
                None => continue,
            },
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

/// One input of a merged workload as [`check_merge`] reads it: its name and,
/// for each record in offset order, its offset and append time.
pub type InputRecords<'a> = (&'a str, Vec<(u64, u64)>);

/// Checks a merge log, as `(offset, record)` in offset order, against
/// `inputs`, read after the log. Every record of every input must be merged
/// once, in its input's order, with its own append time; each record's next
/// offsets must follow from the entries before it; and each epoch's entries
/// must follow that epoch's claim. `complete` says every record of every
/// input should be merged already.
pub fn check_merge(
    records: &[(u64, &[u8])],
    inputs: &[InputRecords<'_>],
    complete: bool,
) -> Vec<Finding> {
    let mut findings = Vec::new();
    // For each input, how many of its records are merged, and its next offset.
    let mut merged = vec![0; inputs.len()];
    let mut next = vec![0; inputs.len()];
    let mut last_epoch: Option<u64> = None;
    let mut claimed: Option<u64> = None;
    for &(offset, record) in records {
        let decoded = match merge::decode(record) {
            Ok(decoded) => decoded,
            Err(error) => {
                findings.push(Finding::UndecodableMerge {
                    offset,
                    error: error.to_string(),
                });
                continue;
            }
        };
        let epoch = decoded.epoch();
        if let Some(previous) = last_epoch
            && epoch < previous
        {
            findings.push(Finding::MergeEpochRegression {
                offset,
                epoch,
                previous,
            });
        }
        last_epoch = Some(last_epoch.map_or(epoch, |e| e.max(epoch)));
        let Some(entry) = decoded.entry() else {
            claimed = Some(epoch);
            if decoded.next() != next {
                findings.push(Finding::MergeCursor { offset });
                if decoded.next().len() == next.len() {
                    next = decoded.next().to_vec();
                }
            }
            continue;
        };
        if claimed != Some(epoch) {
            findings.push(Finding::MergeUnclaimed { offset, epoch });
        }
        let Some((name, on_input)) = inputs.get(usize::from(entry.input)) else {
            findings.push(Finding::MergeUnknownInput {
                offset,
                input: entry.input,
            });
            continue;
        };
        let at = &mut merged[usize::from(entry.input)];
        let expected = on_input.get(*at).copied();
        match expected {
            Some((found, stamp)) if found == entry.offset => {
                *at += 1;
                if stamp != entry.timestamp_micros {
                    findings.push(Finding::MergeTimestamp {
                        offset,
                        input: (*name).to_owned(),
                        named: entry.offset,
                        recorded: entry.timestamp_micros,
                        found: stamp,
                    });
                }
            }
            _ => {
                findings.push(Finding::MergeOrder {
                    offset,
                    input: (*name).to_owned(),
                    named: entry.offset,
                    expected: expected.map(|(found, _)| found),
                });
                // Carry on after the record it named, so one wrong entry is
                // one finding.
                *at = (*at).max(on_input.partition_point(|&(o, _)| o <= entry.offset));
            }
        }
        next[usize::from(entry.input)] = entry.offset + 1;
        if decoded.next() != next {
            findings.push(Finding::MergeCursor { offset });
            if decoded.next().len() == next.len() {
                next = decoded.next().to_vec();
            }
        }
    }
    if complete {
        for ((name, on_input), &at) in inputs.iter().zip(&merged) {
            if at < on_input.len() {
                findings.push(Finding::MergeBehind {
                    input: (*name).to_owned(),
                    missing: on_input.len() - at,
                    first: on_input[at].0,
                });
            }
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
            Finding::UndecodableMerge { offset, error } => {
                write!(f, "undecodable merge record at offset {offset}: {error}")
            }
            Finding::MergeEpochRegression {
                offset,
                epoch,
                previous,
            } => write!(
                f,
                "merge log epoch regression at offset {offset}: epoch {epoch} after epoch {previous}"
            ),
            Finding::MergeOrder {
                offset,
                input,
                named,
                expected,
            } => {
                write!(
                    f,
                    "merge entry at offset {offset} names {input}@{named}, but "
                )?;
                match expected {
                    Some(expected) => write!(f, "the next record of {input} is at {expected}"),
                    None => write!(f, "{input} holds no more records"),
                }
            }
            Finding::MergeCursor { offset } => write!(
                f,
                "merge record at offset {offset} does not carry on from the records before it"
            ),
            Finding::MergeUnknownInput { offset, input } => write!(
                f,
                "merge entry at offset {offset} names input {input}, which the spec does not have"
            ),
            Finding::MergeTimestamp {
                offset,
                input,
                named,
                recorded,
                found,
            } => write!(
                f,
                "merge entry at offset {offset} gives {input}@{named} append time {recorded}, \
                 but the record has {found}"
            ),
            Finding::MergeUnclaimed { offset, epoch } => write!(
                f,
                "merge entry at offset {offset} is at epoch {epoch}, with no claim of that epoch before it"
            ),
            Finding::MergeBehind {
                input,
                missing,
                first,
            } => write!(
                f,
                "merge log behind: {missing} records of {input} not merged, from offset {first}"
            ),
        }
    }
}

/// Which spec record each input ran under, from the claims on an output
/// stream in stream order: `(first input, spec offset)` pairs, ascending.
/// A claim's spec applies from the input after the last one with an output
/// before it, since the owner steps everything after that under its spec and
/// only checks what it steps again below. Inputs before the first claim that
/// names a spec are not covered. Claims whose spec `ran` says no to, or that
/// name a spec below the floor of the claim before them, are passed over: their owner
/// refused the spec and stepped nothing. Records that do not decode are
/// passed over too; [`check`] reports them.
pub fn specs_applied<'a>(
    records: impl IntoIterator<Item = &'a [u8]>,
    ran: impl Fn(u64) -> bool,
) -> Vec<(u64, u64)> {
    let mut applied: Vec<(u64, u64)> = Vec::new();
    let mut next_input = 0;
    let mut floor = 0;
    for record in records {
        match output::decode(record) {
            Ok(Record::Claim {
                spec: Some(spec),
                floor: claimed_floor,
                ..
            }) => {
                // The owner refuses a spec below the floor the last claim
                // carried, whether or not that claim ran, so a refused claim
                // still raises it. A claim with no floor resets it.
                let refused = spec < floor;
                floor = claimed_floor.unwrap_or(0);
                if refused || !ran(spec) || applied.last().is_some_and(|(_, s)| *s == spec) {
                    continue;
                }
                // Two claims with no output between them: the later one ran
                // from the same input.
                if applied.last().is_some_and(|(from, _)| *from == next_input) {
                    applied.pop();
                }
                applied.push((next_input, spec));
            }
            // Its writer refused every spec below it, as for a claim's floor.
            Ok(Record::NeedsOperator { spec, .. }) => floor = floor.max(spec),
            Ok(found) => {
                if let Some(written) = found.written() {
                    next_input = next_input.max(written.header.input_offset + 1);
                }
            }
            Err(_) => {}
        }
    }
    applied
}

/// What a check or replay of an output stream runs: the spec it was written
/// under, which is the first claim's when that names one and `cached`, the
/// `workloads` value, otherwise; and the limits each input ran under, as
/// `(first input, limits)`. `claimed` holds the spec record of every spec
/// the claims name ([`specs_claimed`]). A needs-operator record before the
/// first claim ran nothing and is passed over. A claim whose spec has another module
/// or input than the first claim's was refused by its owner and is passed
/// over, so a refused later spec never decides the module or the limits.
pub fn judged_under<'a, R>(
    cached: &WorkloadSpec,
    claimed: &HashMap<u64, WorkloadSpec>,
    records: R,
) -> (WorkloadSpec, Vec<(u64, Limits)>)
where
    R: IntoIterator<Item = &'a [u8]> + Clone,
{
    let first = records
        .clone()
        .into_iter()
        .filter_map(|r| output::decode(r).ok())
        .find(|r| r.needs_operator_spec().is_none());
    let lineage = match first {
        Some(Record::Claim {
            spec: Some(spec), ..
        }) => claimed.get(&spec).cloned(),
        _ => None,
    }
    .unwrap_or_else(|| cached.clone());
    let ran = |spec: u64| {
        claimed
            .get(&spec)
            .is_some_and(|s| s.conflicts_with_history(&lineage).is_none())
    };
    let limits = specs_applied(records, ran)
        .into_iter()
        .map(|(from, spec)| (from, claimed[&spec].limits))
        .collect();
    (lineage, limits)
}

/// Every spec the claims on an output stream name, in stream order, once each.
pub fn specs_claimed<'a>(records: impl IntoIterator<Item = &'a [u8]>) -> Vec<u64> {
    let mut claimed = Vec::new();
    for record in records {
        if let Ok(Record::Claim {
            spec: Some(spec), ..
        }) = output::decode(record)
            && !claimed.contains(&spec)
        {
            claimed.push(spec);
        }
    }
    claimed
}

/// The order to check `workloads`, given as `(id, input streams)`, in: a
/// workload after each one whose output stream it reads, when both are there,
/// and otherwise in the order given. Checked upstream first, each stage's
/// input is read after the stage before it was checked, so `--complete` on
/// every stage says every input the first stage saw went through the last.
/// A cycle, which no pipeline has, is broken where it is entered.
pub fn upstream_first(workloads: &[(String, Vec<String>)]) -> Vec<String> {
    fn visit(
        at: usize,
        workloads: &[(String, Vec<String>)],
        seen: &mut Vec<bool>,
        order: &mut Vec<String>,
    ) {
        if std::mem::replace(&mut seen[at], true) {
            return;
        }
        for input in &workloads[at].1 {
            let upstream = names::output_of(input)
                .and_then(|up| workloads.iter().position(|(id, _)| id == up));
            if let Some(upstream) = upstream {
                visit(upstream, workloads, seen, order);
            }
        }
        order.push(workloads[at].0.clone());
    }
    let mut seen = vec![false; workloads.len()];
    let mut order = Vec::with_capacity(workloads.len());
    for at in 0..workloads.len() {
        visit(at, workloads, &mut seen, &mut order);
    }
    order
}

#[cfg(test)]
mod tests;
