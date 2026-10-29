//! Inputs for the runtime tests.

// Each test binary uses only some of these.
#![allow(dead_code)]

use felixd_runtime::{Input, Output, Stepped, Workload, WorkloadError};

pub(crate) const STREAM: &str = "in";

/// The input at `offset` with `payload`, stamped `offset * 1000`.
pub(crate) fn input(offset: u64, payload: &[u8]) -> Input<'_> {
    Input {
        stream: STREAM,
        incarnation: 0,
        offset,
        timestamp_micros: offset * 1000,
        key: None,
        payload,
    }
}

/// Steps `payload` at offset 0 and returns the outputs' payloads, panicking
/// on a rejection.
pub(crate) fn payloads(wl: &mut Workload, payload: &[u8]) -> Result<Vec<Vec<u8>>, WorkloadError> {
    match wl.step(input(0, payload))? {
        Stepped::Outputs(outputs) => Ok(outputs.into_iter().map(|o| o.payload).collect()),
        Stepped::Rejected { code, detail } => panic!("rejected with {code}: {detail}"),
    }
}

pub(crate) fn outputs(stepped: Stepped) -> Vec<Output> {
    match stepped {
        Stepped::Outputs(outputs) => outputs,
        Stepped::Rejected { code, detail } => panic!("rejected with {code}: {detail}"),
    }
}
