//! What a step is given and what it produced. A live step, a resume,
//! `felixd check` and `felixd replay` all go through here, so the guest sees
//! the same input on each.

use std::collections::VecDeque;

use anyhow::Result;
use bytes::Bytes;
use felix_client::StreamRecord;
use felixd_core::DecodeError;
use felixd_core::check::Produced;
use felixd_core::class::OperatorReason;
use felixd_core::merge;
use felixd_core::names;
use felixd_core::output::{self, DEFAULT_NAME};
use felixd_core::spec::WorkloadSpec;
use felixd_runtime::{Input, Stepped};

use crate::felix::Felix;
use crate::runner::{NeedsOperator, poison};

/// One input record, owned, so a step can run on a blocking thread.
pub(crate) struct StepInput {
    stream: String,
    offset: u64,
    timestamp_micros: u64,
    key: Option<Bytes>,
    payload: Bytes,
}

impl StepInput {
    /// The input at `offset` on input stream `stream`, appended at
    /// `timestamp_micros`, from its record. Every field comes from the record
    /// and the spec. When the input is another workload's output stream,
    /// only outputs addressed to `out` are inputs, and the step gets their
    /// key and payload; `None` means the record is passed over.
    ///
    /// A record there of a version or kind this build does not know is
    /// [`NeedsOperator`]: a newer build wrote it, and upgrading this node is
    /// the fix. An output that does not decode is poison.
    pub(crate) fn read(
        stream: &str,
        offset: u64,
        timestamp_micros: u64,
        record: &Bytes,
    ) -> anyhow::Result<Option<StepInput>> {
        let (key, payload) = if names::is_output_stream(stream) {
            match output::addressed_to(record, DEFAULT_NAME) {
                Ok(Some(addressed)) => (
                    addressed.key.map(|k| record.slice_ref(k)),
                    record.slice_ref(addressed.payload),
                ),
                Ok(None) => return Ok(None),
                Err(error) => {
                    let detail = format!(
                        "input {offset} on {stream} is not an output record this build reads: {error}"
                    );
                    return Err(match error {
                        DecodeError::UnknownVersion(_) | DecodeError::UnknownKind(_) => {
                            NeedsOperator::new(OperatorReason::InputUnreadable, detail).into()
                        }
                        _ => poison(detail),
                    });
                }
            }
        } else {
            (None, record.clone())
        };
        Ok(Some(StepInput {
            stream: stream.to_owned(),
            offset,
            timestamp_micros,
            key,
            payload,
        }))
    }

    pub(crate) fn timestamp_micros(&self) -> u64 {
        self.timestamp_micros
    }

    pub(crate) fn as_input(&self) -> Input<'_> {
        Input {
            stream: &self.stream,
            // Specs do not record incarnations yet (#72).
            incarnation: 0,
            offset: self.offset,
            timestamp_micros: self.timestamp_micros,
            key: self.key.as_deref(),
            payload: &self.payload,
        }
    }
}

/// A workload's steps in order, each at its offset on [`Feed::stream`]: the
/// records of its one input, or of its inputs in merge log order.
pub(crate) struct Feed<'a> {
    felix: &'a Felix,
    stream: String,
    /// Each input, for a workload with a merge log.
    sources: Option<Vec<Source>>,
}

/// One step of a [`Feed`].
pub(crate) struct Step {
    /// The step's offset on the feed's stream, which outputs and snapshots
    /// name.
    pub(crate) at: u64,
    /// What the step is given, `None` for a record that is passed over, or
    /// why the input cannot be read.
    pub(crate) input: Result<Option<StepInput>>,
}

impl<'a> Feed<'a> {
    pub(crate) fn new(felix: &'a Felix, id: &str, spec: &WorkloadSpec) -> Feed<'a> {
        if !spec.merged() {
            return Feed {
                felix,
                stream: spec.inputs[0].clone(),
                sources: None,
            };
        }
        let sources = spec.inputs.iter().map(|s| Source::new(s)).collect();
        Feed {
            felix,
            stream: names::merge_stream(id),
            sources: Some(sources),
        }
    }

    /// The stream whose offsets number the steps: the one input, or the
    /// merge log.
    pub(crate) fn stream(&self) -> &str {
        &self.stream
    }

    /// The steps at offsets `from..end` that one page of the feed's stream
    /// holds, and the offset the next page starts at. A merge log claim is
    /// no step.
    pub(crate) async fn page(&mut self, from: u64, end: u64) -> Result<(Vec<Step>, u64)> {
        let (records, after) = self.felix.read_page(&self.stream, from, end).await?;
        let mut steps = Vec::with_capacity(records.len());
        for record in records {
            let at = record.offset;
            let input = match &mut self.sources {
                None => StepInput::read(&self.stream, at, record.timestamp_micros, &record.payload),
                Some(sources) => match merge::decode(&record.payload) {
                    Ok(merge::Record::Claim { .. }) => continue,
                    Ok(merge::Record::Entry { entry, .. }) => {
                        merged(self.felix, sources, &self.stream, at, entry).await?
                    }
                    Err(error) => Err(merge_unreadable(&self.stream, at, error)),
                },
            };
            steps.push(Step { at, input });
        }
        Ok((steps, after))
    }
}

/// The input merge entry `entry`, at `at` on `stream`, names. The outer
/// error is a problem reading Felix.
async fn merged(
    felix: &Felix,
    sources: &mut [Source],
    stream: &str,
    at: u64,
    entry: merge::Entry,
) -> Result<Result<Option<StepInput>>> {
    let Some(source) = sources.get_mut(usize::from(entry.input)) else {
        return Ok(Err(poison(format!(
            "merge entry {stream}:{at} names {entry}, and the spec has {} inputs",
            sources.len()
        ))));
    };
    let Some(record) = source.record(felix, entry.offset).await? else {
        return Ok(Err(poison(format!(
            "merge entry {stream}:{at} names {}:{}, which holds no such record",
            source.stream, entry.offset
        ))));
    };
    Ok(StepInput::read(
        &source.stream,
        entry.offset,
        entry.timestamp_micros,
        &record.payload,
    ))
}

/// A merge log record this build cannot read. One of a version or kind it
/// does not know was written by a newer build, and upgrading the node fixes
/// it.
pub(crate) fn merge_unreadable(stream: &str, at: u64, error: DecodeError) -> anyhow::Error {
    let detail = format!("{stream}:{at} is not a merge record this build reads: {error}");
    match error {
        DecodeError::UnknownVersion(_) | DecodeError::UnknownKind(_) => {
            NeedsOperator::new(OperatorReason::InputUnreadable, detail).into()
        }
        _ => poison(detail),
    }
}

/// One input of a merged workload, read forward a page at a time. Merge
/// entries name each input's records in offset order, so the reader only
/// moves forward.
struct Source {
    stream: String,
    /// The offset the next page starts at.
    next: u64,
    /// The input's tail as last read.
    end: u64,
    page: VecDeque<StreamRecord>,
}

impl Source {
    fn new(stream: &str) -> Source {
        Source {
            stream: stream.to_owned(),
            next: 0,
            end: 0,
            page: VecDeque::new(),
        }
    }

    /// The record at `offset`, or `None` when the input holds none there.
    async fn record(&mut self, felix: &Felix, offset: u64) -> Result<Option<StreamRecord>> {
        loop {
            while self.page.front().is_some_and(|r| r.offset < offset) {
                self.page.pop_front();
            }
            if let Some(front) = self.page.front() {
                return Ok((front.offset == offset)
                    .then(|| self.page.pop_front())
                    .flatten());
            }
            if self.next > offset {
                return Ok(None);
            }
            self.next = offset;
            if self.next >= self.end {
                self.end = felix.read_tail(&self.stream).await?;
                if self.next >= self.end {
                    return Ok(None);
                }
            }
            let (records, after) = felix.read_page(&self.stream, self.next, self.end).await?;
            self.page.extend(records);
            self.next = after;
        }
    }
}

/// What a step produced, in the order it is written: its outputs, or its
/// one rejection.
pub(crate) fn produced(stepped: Stepped) -> Vec<Produced> {
    match stepped {
        Stepped::Outputs(outputs) => outputs
            .into_iter()
            .map(|o| Produced::Output {
                name: o.name,
                key: o.key,
                payload: o.payload,
            })
            .collect(),
        Stepped::Rejected { code, detail } => vec![Produced::Rejection {
            code,
            detail: detail.into_bytes(),
        }],
    }
}

#[cfg(test)]
mod tests;
