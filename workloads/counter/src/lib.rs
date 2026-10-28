//! Counts its input records. Each step outputs the new total as eight
//! big-endian bytes.

use std::cell::Cell;

wit_bindgen::generate!({
    path: "../../wit",
    world: "workload",
});

thread_local! {
    static COUNT: Cell<u64> = const { Cell::new(0) };
}

struct Counter;

impl Guest for Counter {
    fn step(_input: Vec<u8>) -> Vec<Vec<u8>> {
        let total = COUNT.get().wrapping_add(1);
        COUNT.set(total);
        vec![total.to_be_bytes().to_vec()]
    }

    fn snapshot() -> Vec<u8> {
        COUNT.get().to_be_bytes().to_vec()
    }

    fn restore(state: Vec<u8>) {
        let bytes: [u8; 8] = state.try_into().expect("counter state is eight bytes");
        COUNT.set(u64::from_be_bytes(bytes));
    }

    fn inspect() -> String {
        format!("{{\"count\":{}}}", COUNT.get())
    }
}

export!(Counter);
