// The admission component around `felixd_core::admission::decide`, shared by
// `admission` and `admission-strict`, which differ only in `policy()`.

wit_bindgen::generate!({
    path: "../../wit",
    world: "workload",
});

use exports::felixd::workload::state;
use exports::felixd::workload::step::{self, Addressed, Input, InputBody, Output, StepError};
use felixd_core::admission::decide;

struct Component;

impl step::Guest for Component {
    /// One decision per request, a rejection included, so every request is
    /// answered on the output stream.
    fn step(input: Input) -> Result<Vec<Output>, StepError> {
        let InputBody::Record(record) = input.body else {
            return Err(StepError::Poison(
                "admission takes no effect responses".to_string(),
            ));
        };
        let decision = decide(&policy(), input.offset, &record.payload);
        let key = (!decision.id.is_empty()).then(|| decision.id.as_bytes().to_vec());
        Ok(vec![Output::Record(Addressed {
            name: "out".to_string(),
            key,
            payload: decision.encode(),
        })])
    }
}

// Admission keeps no state: a decision depends on its request and the policy
// alone, so a snapshot is empty and any replay can start anywhere.
impl state::Guest for Component {
    fn snapshot() -> u32 {
        1
    }

    fn snapshot_chunk(_index: u32) -> Vec<u8> {
        Vec::new()
    }

    fn restore(_chunks: u32) {}

    fn restore_chunk(_index: u32, _chunk: Vec<u8>) {}

    fn inspect() -> String {
        "{}".to_string()
    }
}

export!(Component);
