//! Counts its input records. Each step outputs the new total as eight
//! big-endian bytes, under the name `out`.

use std::cell::Cell;

wit_bindgen::generate!({
    path: "../../wit",
    world: "workload",
});

use exports::felixd::workload::state;
use exports::felixd::workload::step::{self, Addressed, Input, Output, StepError};

thread_local! {
    static COUNT: Cell<u64> = const { Cell::new(0) };
}

struct Counter;

impl step::Guest for Counter {
    fn step(_input: Input) -> Result<Vec<Output>, StepError> {
        let total = COUNT.get().wrapping_add(1);
        COUNT.set(total);
        Ok(vec![Output::Record(Addressed {
            name: "out".to_string(),
            key: None,
            payload: total.to_be_bytes().to_vec(),
        })])
    }
}

impl state::Guest for Counter {
    fn snapshot() -> u32 {
        1
    }

    fn snapshot_chunk(_index: u32) -> Vec<u8> {
        COUNT.get().to_be_bytes().to_vec()
    }

    fn restore(chunks: u32) {
        assert_eq!(chunks, 1, "counter state is one chunk");
    }

    fn restore_chunk(_index: u32, chunk: Vec<u8>) {
        let bytes: [u8; 8] = chunk.try_into().expect("counter state is eight bytes");
        COUNT.set(u64::from_be_bytes(bytes));
    }

    fn inspect() -> String {
        format!("{{\"count\":{}}}", COUNT.get())
    }
}

export!(Counter);
