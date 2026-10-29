//! A tumbling-window aggregate over `(timestamp_ms, value)` records.
//!
//! An input is 16 bytes: a little-endian `u64` timestamp in milliseconds and a
//! little-endian `i64` value. Records are grouped into windows of
//! `WINDOW_MS`. A record that starts a later window closes the open one, and
//! that step outputs the closed window's summary as a JSON line. Steps inside
//! one window output nothing. A record with an earlier timestamp than the open
//! window is counted in it. A malformed record is rejected with code
//! [`MALFORMED`] and counted.

use std::cell::RefCell;

wit_bindgen::generate!({
    path: "../../wit",
    world: "workload",
});

use exports::felixd::workload::state;
use exports::felixd::workload::step::{
    self, Addressed, ErrorCode, Input, InputBody, Output, StepError,
};

const WINDOW_MS: u64 = 10_000;
/// The rejection code for an input that is not 16 bytes.
const MALFORMED: u32 = 1;
const STATE_LEN: usize = 1 + 7 * 8;

#[derive(Clone, Copy, Default, PartialEq)]
struct State {
    open: Option<Window>,
    emitted: u64,
    rejected: u64,
}

#[derive(Clone, Copy, PartialEq)]
struct Window {
    start: u64,
    count: u64,
    sum: i64,
    min: i64,
    max: i64,
}

impl Window {
    fn new(start: u64, value: i64) -> Self {
        Window {
            start,
            count: 1,
            sum: value,
            min: value,
            max: value,
        }
    }

    fn add(&mut self, value: i64) {
        self.count += 1;
        self.sum = self.sum.wrapping_add(value);
        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }

    fn summary(&self) -> Vec<u8> {
        format!(
            "{{\"window_start\":{},\"count\":{},\"sum\":{},\"min\":{},\"max\":{}}}",
            self.start, self.count, self.sum, self.min, self.max
        )
        .into_bytes()
    }
}

impl State {
    fn encode(&self) -> Vec<u8> {
        let w = self.open.unwrap_or(Window::new(0, 0));
        let mut out = Vec::with_capacity(STATE_LEN);
        out.push(self.open.is_some() as u8);
        out.extend(w.start.to_le_bytes());
        out.extend(w.count.to_le_bytes());
        out.extend(w.sum.to_le_bytes());
        out.extend(w.min.to_le_bytes());
        out.extend(w.max.to_le_bytes());
        out.extend(self.emitted.to_le_bytes());
        out.extend(self.rejected.to_le_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> Option<State> {
        if bytes.len() != STATE_LEN {
            return None;
        }
        let word = |i: usize| -> [u8; 8] { bytes[1 + i * 8..9 + i * 8].try_into().unwrap() };
        let open = match bytes[0] {
            0 => None,
            1 => Some(Window {
                start: u64::from_le_bytes(word(0)),
                count: u64::from_le_bytes(word(1)),
                sum: i64::from_le_bytes(word(2)),
                min: i64::from_le_bytes(word(3)),
                max: i64::from_le_bytes(word(4)),
            }),
            _ => return None,
        };
        Some(State {
            open,
            emitted: u64::from_le_bytes(word(5)),
            rejected: u64::from_le_bytes(word(6)),
        })
    }
}

thread_local! {
    static STATE: RefCell<State> = RefCell::new(State::default());
}

struct WindowAggregate;

fn out(payload: Vec<u8>) -> Output {
    Output::Record(Addressed {
        name: "out".to_string(),
        key: None,
        payload,
    })
}

impl step::Guest for WindowAggregate {
    fn step(input: Input) -> Result<Vec<Output>, StepError> {
        let InputBody::Record(record) = input.body else {
            return Err(StepError::Poison(
                "window takes no effect responses".to_string(),
            ));
        };
        STATE.with_borrow_mut(|state| {
            let Ok(bytes) = <[u8; 16]>::try_from(record.payload.as_slice()) else {
                state.rejected += 1;
                return Err(StepError::Reject(ErrorCode {
                    code: MALFORMED,
                    detail: format!("an input is 16 bytes, not {}", record.payload.len()),
                }));
            };
            let ts = u64::from_le_bytes(bytes[..8].try_into().unwrap());
            let value = i64::from_le_bytes(bytes[8..].try_into().unwrap());
            let start = ts - ts % WINDOW_MS;
            match &mut state.open {
                Some(w) if start <= w.start => {
                    w.add(value);
                    Ok(Vec::new())
                }
                Some(w) => {
                    let closed = w.summary();
                    *w = Window::new(start, value);
                    state.emitted += 1;
                    Ok(vec![out(closed)])
                }
                None => {
                    state.open = Some(Window::new(start, value));
                    Ok(Vec::new())
                }
            }
        })
    }
}

impl state::Guest for WindowAggregate {
    fn snapshot() -> u32 {
        1
    }

    fn snapshot_chunk(_index: u32) -> Vec<u8> {
        STATE.with_borrow(State::encode)
    }

    fn restore(chunks: u32) {
        assert_eq!(chunks, 1, "window state is one chunk");
    }

    fn restore_chunk(_index: u32, chunk: Vec<u8>) {
        let decoded = State::decode(&chunk).expect("window state is malformed");
        STATE.set(decoded);
    }

    fn inspect() -> String {
        STATE.with_borrow(|s| {
            let open = match &s.open {
                Some(w) => format!(
                    "{{\"start\":{},\"count\":{},\"sum\":{},\"min\":{},\"max\":{}}}",
                    w.start, w.count, w.sum, w.min, w.max
                ),
                None => "null".to_string(),
            };
            format!(
                "{{\"open\":{open},\"emitted\":{},\"rejected\":{}}}",
                s.emitted, s.rejected
            )
        })
    }
}

export!(WindowAggregate);
