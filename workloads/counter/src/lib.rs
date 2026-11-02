//! Counts its input records. Each step outputs the new total as eight
//! big-endian bytes, under the name `out`.
//!
//! It declares its snapshot layout in the `felixd.snapshot` section. The cargo
//! features build the variants an upgrade check is tested with: see README.md.

use std::cell::Cell;

wit_bindgen::generate!({
    path: "../../wit",
    world: "workload",
});

use exports::felixd::workload::state;
use exports::felixd::workload::step::{self, Addressed, Input, Output, StepError};

// The `undeclared` variant declares nothing.
#[cfg_attr(feature = "undeclared", allow(unused_macros))]
macro_rules! declare_snapshot_layout {
    ($json:literal) => {
        #[used]
        #[unsafe(link_section = "felixd.snapshot")]
        static SNAPSHOT_LAYOUT: [u8; $json.len()] = *$json;
    };
}

#[cfg(not(any(
    feature = "layout-v2",
    feature = "foreign-layout",
    feature = "malformed-layout",
    feature = "undeclared"
)))]
declare_snapshot_layout!(br#"{"format":"counter","reads":[]}"#);
#[cfg(feature = "layout-v2")]
declare_snapshot_layout!(br#"{"format":"counter.2","reads":["counter"]}"#);
#[cfg(feature = "foreign-layout")]
declare_snapshot_layout!(br#"{"format":"tally","reads":[]}"#);
#[cfg(feature = "malformed-layout")]
declare_snapshot_layout!(br#"{"layout":"counter"}"#);

thread_local! {
    static COUNT: Cell<u64> = const { Cell::new(0) };
}

struct Counter;

impl step::Guest for Counter {
    fn step(_input: Input) -> Result<Vec<Output>, StepError> {
        let count = COUNT.get();
        // The `changed` variant counts by two once the total reaches ten.
        let by = if cfg!(feature = "changed") && count >= 10 {
            2
        } else {
            1
        };
        let total = count.wrapping_add(by);
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
        // The `unreadable` variant declares it reads the old layout but
        // expects a longer chunk, so its restore traps.
        let size = if cfg!(feature = "unreadable") { 16 } else { 8 };
        assert_eq!(chunk.len(), size, "counter state is {size} bytes");
        let bytes: [u8; 8] = chunk[..8].try_into().expect("eight bytes");
        COUNT.set(u64::from_be_bytes(bytes));
    }

    fn inspect() -> String {
        format!("{{\"count\":{}}}", COUNT.get())
    }
}

export!(Counter);
