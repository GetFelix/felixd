//! What a step is given and what it produced. A live step, a resume,
//! `felixd check` and `felixd replay` all go through here, so the guest sees
//! the same input on each.

use bytes::Bytes;
use felixd_core::check::Produced;
use felixd_core::names;
use felixd_core::output::{self, DEFAULT_NAME};
use felixd_core::spec::WorkloadSpec;
use felixd_runtime::{Input, Stepped};

/// One input record, owned, so a step can run on a blocking thread.
pub(crate) struct StepInput {
    stream: String,
    offset: u64,
    timestamp_micros: u64,
    key: Option<Bytes>,
    payload: Bytes,
}

impl StepInput {
    /// The input at `offset` on the spec's input stream, appended at
    /// `timestamp_micros`, from its record. Every field comes from the record
    /// and the spec. When the input is another workload's output stream,
    /// only outputs addressed to `out` are inputs, and the step gets their
    /// key and payload; `None` means the record is passed over. A record
    /// there that does not decode is an error, since this build cannot tell
    /// what it holds.
    pub(crate) fn read(
        spec: &WorkloadSpec,
        offset: u64,
        timestamp_micros: u64,
        record: &Bytes,
    ) -> Result<Option<StepInput>, String> {
        let (key, payload) = if names::is_output_stream(&spec.input) {
            match output::addressed_to(record, DEFAULT_NAME) {
                Ok(Some(addressed)) => (
                    addressed.key.map(|k| record.slice_ref(k)),
                    record.slice_ref(addressed.payload),
                ),
                Ok(None) => return Ok(None),
                Err(error) => {
                    return Err(format!(
                        "input {offset} on {} is not an output record this build reads: {error}",
                        spec.input
                    ));
                }
            }
        } else {
            (None, record.clone())
        };
        Ok(Some(StepInput {
            stream: spec.input.clone(),
            offset,
            timestamp_micros,
            key,
            payload,
        }))
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
