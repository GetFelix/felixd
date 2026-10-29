//! A test fixture that misbehaves on request, to check what the host does.
//! The input's payload picks the behaviour; anything else is echoed under
//! the name `out`.
//!
//! - `spin`: loops forever.
//! - `trap`: panics, which traps.
//! - `grow`: allocates 64 MiB.
//! - `huge`: outputs one 17 MiB record, more than Felix takes in one frame.
//! - `trap-snapshot`: outputs nothing, and every later `snapshot` traps.
//! - `split:<bytes>`: outputs each byte after the colon as its own record, so
//!   one step writes a batch of several.
//! - `reject:<code>`: rejects the input with that code.
//! - `poison`: returns `poison`.
//! - `request`: returns an effect request.
//! - `named:<name>:<key>:<payload>`: one output with that name and key, no
//!   key when it is empty. The name is not checked here.
//! - `meta`: outputs the input's metadata as text: stream, incarnation,
//!   offset, timestamp, key in hex (`-` for none) and payload in hex.
//! - `chunks:<n>`: the state becomes `n` counters, each its own snapshot
//!   chunk, so a snapshot has `n` chunks.
//! - `touch:<i>`: adds one to counter `i`, changing only its chunk, and
//!   outputs the counter's new value in decimal.
//! - `big-chunk`: outputs nothing, and every later snapshot has one chunk
//!   larger than the host allows.

use std::cell::{Cell, RefCell};

wit_bindgen::generate!({
    path: "../../wit",
    world: "workload",
});

use exports::felixd::workload::state;
use exports::felixd::workload::step::{
    self, Addressed, EffectRequest, ErrorCode, Input, InputBody, Output, StepError,
};

thread_local! {
    static TRAP_SNAPSHOT: Cell<bool> = const { Cell::new(false) };
    static BIG_CHUNK: Cell<bool> = const { Cell::new(false) };
    static COUNTERS: RefCell<Vec<u32>> = const { RefCell::new(Vec::new()) };
}

struct Faulty;

fn out(payload: Vec<u8>) -> Output {
    named("out", None, payload)
}

fn named(name: &str, key: Option<Vec<u8>>, payload: Vec<u8>) -> Output {
    Output::Record(Addressed {
        name: name.to_string(),
        key,
        payload,
    })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn number(bytes: &[u8]) -> u32 {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse().ok())
        .expect("a decimal number")
}

impl step::Guest for Faulty {
    fn step(input: Input) -> Result<Vec<Output>, StepError> {
        let InputBody::Record(record) = &input.body else {
            return Err(StepError::Poison(
                "faulty takes no effect responses".to_string(),
            ));
        };
        let payload = record.payload.as_slice();
        let outputs = match payload {
            b"spin" => loop {
                std::hint::spin_loop();
            },
            b"trap" => panic!("asked to trap"),
            b"grow" => {
                let mut big = vec![0u8; 64 << 20];
                big[0] = 1;
                vec![out(vec![std::hint::black_box(big)[0]])]
            }
            b"huge" => vec![out(vec![7u8; 17 << 20])],
            b"trap-snapshot" => {
                TRAP_SNAPSHOT.set(true);
                Vec::new()
            }
            b"big-chunk" => {
                BIG_CHUNK.set(true);
                Vec::new()
            }
            b"poison" => return Err(StepError::Poison("asked to poison".to_string())),
            b"request" => vec![Output::Request(EffectRequest {
                kind: "felixd:http/fetch@0.1.0".to_string(),
                credential: None,
                body: Vec::new(),
            })],
            b"meta" => {
                let key = record.key.as_deref().map_or("-".to_string(), hex);
                let text = format!(
                    "{} {} {} {} {key} {}",
                    input.source.stream,
                    input.source.incarnation,
                    input.offset,
                    input.timestamp_micros,
                    hex(payload)
                );
                vec![out(text.into_bytes())]
            }
            _ => {
                if let Some(bytes) = payload.strip_prefix(b"split:") {
                    bytes.iter().map(|&b| out(vec![b])).collect()
                } else if let Some(code) = payload.strip_prefix(b"reject:") {
                    return Err(StepError::Reject(ErrorCode {
                        code: number(code),
                        detail: "asked to reject".to_string(),
                    }));
                } else if let Some(rest) = payload.strip_prefix(b"named:") {
                    let mut parts = rest.splitn(3, |&b| b == b':');
                    let name = String::from_utf8_lossy(parts.next().unwrap_or_default());
                    let key = parts.next().unwrap_or_default();
                    let body = parts.next().unwrap_or_default();
                    let key = (!key.is_empty()).then(|| key.to_vec());
                    vec![named(&name, key, body.to_vec())]
                } else if let Some(n) = payload.strip_prefix(b"chunks:") {
                    COUNTERS.with_borrow_mut(|c| c.resize(number(n) as usize, 0));
                    Vec::new()
                } else if let Some(i) = payload.strip_prefix(b"touch:") {
                    let value = COUNTERS.with_borrow_mut(|c| {
                        let counter = &mut c[number(i) as usize];
                        *counter += 1;
                        *counter
                    });
                    vec![out(value.to_string().into_bytes())]
                } else {
                    vec![out(payload.to_vec())]
                }
            }
        };
        Ok(outputs)
    }
}

impl state::Guest for Faulty {
    fn snapshot() -> u32 {
        assert!(!TRAP_SNAPSHOT.get(), "asked to trap in snapshot");
        if BIG_CHUNK.get() {
            return 1;
        }
        COUNTERS.with_borrow(|c| c.len() as u32)
    }

    fn snapshot_chunk(index: u32) -> Vec<u8> {
        if BIG_CHUNK.get() {
            return vec![0; (4 << 20) + 1];
        }
        let counter = COUNTERS.with_borrow(|c| c[index as usize]);
        let mut chunk = index.to_be_bytes().to_vec();
        chunk.extend(counter.to_be_bytes());
        chunk
    }

    fn restore(chunks: u32) {
        COUNTERS.set(vec![0; chunks as usize]);
    }

    fn restore_chunk(index: u32, chunk: Vec<u8>) {
        let bytes: [u8; 8] = chunk.try_into().expect("a chunk is eight bytes");
        assert_eq!(
            bytes[..4],
            index.to_be_bytes(),
            "chunk {index} is out of place"
        );
        let counter = u32::from_be_bytes(bytes[4..].try_into().unwrap());
        COUNTERS.with_borrow_mut(|c| c[index as usize] = counter);
    }

    fn inspect() -> String {
        COUNTERS.with_borrow(|c| {
            let list: Vec<String> = c.iter().map(u32::to_string).collect();
            format!("{{\"chunks\":[{}]}}", list.join(","))
        })
    }
}

export!(Faulty);
