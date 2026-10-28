//! A test fixture that fails on request, to check that the host stops a
//! misbehaving workload. The input picks the failure; anything else is echoed.
//!
//! - `spin`: loops forever.
//! - `trap`: panics, which traps.
//! - `grow`: allocates 64 MiB.

wit_bindgen::generate!({
    path: "../../wit",
    world: "workload",
});

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
            _ => vec![input],
        }
    }

    fn snapshot() -> Vec<u8> {
        Vec::new()
    }

    fn restore(_state: Vec<u8>) {}

    fn inspect() -> String {
        "{}".to_string()
    }
}

export!(Faulty);
