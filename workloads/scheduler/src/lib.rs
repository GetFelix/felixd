//! The scheduler: reads spec records from `sys.specs` and node facts from
//! `sys.facts`, and outputs an assignment each time where a workload should
//! run changes. The logic is `felixd_core::schedule::Scheduler`; this is the
//! component around it.

use std::cell::RefCell;

use felixd_core::schedule::Scheduler;

wit_bindgen::generate!({
    path: "../../wit",
    world: "workload",
});

use exports::felixd::workload::state;
use exports::felixd::workload::step::{
    self, Addressed, ErrorCode, Input, InputBody, Output, StepError,
};

/// The rejection code for an input the scheduler cannot read.
const UNREADABLE: u32 = 1;
/// Snapshot chunks are at most this long, well under the host's limit.
const CHUNK: usize = 1 << 20;

thread_local! {
    static STATE: RefCell<Scheduler> = RefCell::new(Scheduler::default());
    /// The snapshot being taken, or the chunks of the one being restored.
    static CHUNKS: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    /// How many chunks the restore under way has.
    static RESTORING: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

struct Component;

impl step::Guest for Component {
    fn step(input: Input) -> Result<Vec<Output>, StepError> {
        let InputBody::Record(record) = input.body else {
            return Err(StepError::Poison(
                "the scheduler takes no effect responses".to_string(),
            ));
        };
        let changed = STATE.with_borrow_mut(|s| {
            s.step(
                &input.source.stream,
                input.timestamp_micros,
                &record.payload,
            )
        });
        let changed = changed.map_err(|refused| {
            StepError::Reject(ErrorCode {
                code: UNREADABLE,
                detail: refused.to_string(),
            })
        })?;
        Ok(changed
            .into_iter()
            .map(|a| {
                Output::Record(Addressed {
                    name: "out".to_string(),
                    key: Some(a.workload.clone().into_bytes()),
                    payload: a.encode(),
                })
            })
            .collect())
    }
}

impl state::Guest for Component {
    fn snapshot() -> u32 {
        let bytes = STATE.with_borrow(Scheduler::encode);
        let chunks = bytes.len().div_ceil(CHUNK).max(1);
        CHUNKS.set(bytes);
        u32::try_from(chunks).expect("the scheduler state fits in u32 chunks")
    }

    fn snapshot_chunk(index: u32) -> Vec<u8> {
        let start = index as usize * CHUNK;
        CHUNKS.with_borrow(|bytes| {
            bytes[start.min(bytes.len())..(start + CHUNK).min(bytes.len())].to_vec()
        })
    }

    fn restore(chunks: u32) {
        CHUNKS.set(Vec::new());
        RESTORING.set(chunks);
    }

    fn restore_chunk(index: u32, chunk: Vec<u8>) {
        CHUNKS.with_borrow_mut(|bytes| bytes.extend(chunk));
        if index + 1 == RESTORING.get() {
            let bytes = CHUNKS.take();
            STATE.set(Scheduler::decode(&bytes).expect("a scheduler snapshot decodes"));
        }
    }

    fn inspect() -> String {
        String::from_utf8(STATE.with_borrow(Scheduler::encode)).expect("JSON is UTF-8")
    }
}

export!(Component);
