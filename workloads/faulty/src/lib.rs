//! A test fixture that fails on request, to check that the host stops a
//! misbehaving workload. The input picks the failure; anything else is echoed.
//!
//! - `spin`: loops forever.
//! - `trap`: panics, which traps.
//! - `grow`: allocates 64 MiB.
//! - `huge`: outputs one 17 MiB record, more than Felix takes in one frame.
//! - `trap-snapshot`: outputs nothing, and every later `snapshot` traps.
//! - `split:<bytes>`: outputs each byte after the colon as its own record, so
//!   one step writes a batch of several.

use std::cell::Cell;

wit_bindgen::generate!({
    path: "../../wit",
    world: "workload",
});

thread_local! {
    static TRAP_SNAPSHOT: Cell<bool> = const { Cell::new(false) };
}

struct Faulty;

impl Guest for Faulty {
    fn step(input: Vec<u8>) -> Vec<Vec<u8>> {
        match input.as_slice() {
            b"spin" => loop {
                std::hint::spin_loop();
            },
            b"trap" => panic!("asked to trap"),
            b"grow" => {
                let mut big = vec![0u8; 64 << 20];
                big[0] = 1;
                vec![vec![std::hint::black_box(big)[0]]]
            }
            b"huge" => vec![vec![7u8; 17 << 20]],
            b"trap-snapshot" => {
                TRAP_SNAPSHOT.set(true);
                Vec::new()
            }
            _ => match input.strip_prefix(b"split:") {
                Some(bytes) => bytes.iter().map(|&b| vec![b]).collect(),
                None => vec![input],
            },
        }
    }

    fn snapshot() -> Vec<u8> {
        assert!(!TRAP_SNAPSHOT.get(), "asked to trap in snapshot");
        Vec::new()
    }

    fn restore(_state: Vec<u8>) {}

    fn inspect() -> String {
        "{}".to_string()
    }
}

export!(Faulty);
