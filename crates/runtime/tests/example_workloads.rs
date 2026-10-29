//! Runs the example workloads through the host: `step` must give the same
//! outputs on every run, and a restored snapshot must carry on identically.

mod common;

use common::input;
use felixd_runtime::{Limits, Output, Runtime, Stepped, Workload};

fn load(name: &str) -> Workload {
    Runtime::new()
        .unwrap()
        .load(&felixd_testkit::workload(name), Limits::default())
        .unwrap()
}

fn window_record(ts: u64, value: i64) -> Vec<u8> {
    let mut r = ts.to_le_bytes().to_vec();
    r.extend(value.to_le_bytes());
    r
}

fn window_inputs() -> Vec<Vec<u8>> {
    let mut inputs: Vec<Vec<u8>> = (0..500u64)
        .map(|i| window_record(i * 1_300, (i as i64 * 37) % 101 - 50))
        .collect();
    inputs.push(vec![1, 2, 3]);
    inputs
}

#[derive(Debug, PartialEq)]
struct Run {
    stepped: Vec<Stepped>,
    snapshot: Vec<Vec<u8>>,
    inspect: String,
}

fn run_all(name: &str, inputs: &[Vec<u8>]) -> Run {
    let mut wl = load(name);
    let stepped = (0..)
        .zip(inputs)
        .map(|(offset, i)| wl.step(input(offset, i)).unwrap())
        .collect();
    Run {
        stepped,
        snapshot: wl.snapshot().unwrap(),
        inspect: wl.inspect().unwrap(),
    }
}

fn out(payload: &[u8]) -> Output {
    Output::unnamed(payload.to_vec())
}

#[test]
fn counter_outputs_the_running_total() {
    let inputs: Vec<Vec<u8>> = (0..5).map(|i| vec![i]).collect();
    let run = run_all("counter", &inputs);
    let want: Vec<Stepped> = (1..=5u64)
        .map(|n| Stepped::Outputs(vec![out(&n.to_be_bytes())]))
        .collect();
    assert_eq!(run.stepped, want);
    assert_eq!(run.snapshot, [5u64.to_be_bytes()]);
    assert_eq!(run.inspect, r#"{"count":5}"#);
}

#[test]
fn counter_gives_the_same_outputs_every_run() {
    let inputs: Vec<Vec<u8>> = (0..200u8).map(|i| vec![i]).collect();
    assert_eq!(run_all("counter", &inputs), run_all("counter", &inputs));
}

#[test]
fn window_emits_a_summary_when_a_window_closes() {
    let mut wl = load("window");
    let none = Stepped::Outputs(Vec::new());
    assert_eq!(wl.step(input(0, &window_record(1_000, 5))).unwrap(), none);
    assert_eq!(wl.step(input(1, &window_record(9_999, -3))).unwrap(), none);
    let closed = wl.step(input(2, &window_record(10_000, 7))).unwrap();
    assert_eq!(
        closed,
        Stepped::Outputs(vec![out(
            br#"{"window_start":0,"count":2,"sum":2,"min":-3,"max":5}"#
        )])
    );
    // A late record joins the open window instead of reopening an old one.
    assert_eq!(wl.step(input(3, &window_record(2_000, 1))).unwrap(), none);
    assert_eq!(
        wl.step(input(4, &[0; 3])).unwrap(),
        Stepped::Rejected {
            code: 1,
            detail: "an input is 16 bytes, not 3".to_owned()
        }
    );
    assert_eq!(
        wl.inspect().unwrap(),
        r#"{"open":{"start":10000,"count":2,"sum":8,"min":1,"max":7},"emitted":1,"rejected":1}"#
    );
}

#[test]
fn window_gives_the_same_outputs_every_run() {
    let inputs = window_inputs();
    let first = run_all("window", &inputs);
    let outputs = |s: &Stepped| match s {
        Stepped::Outputs(o) => Some(o.len()),
        Stepped::Rejected { .. } => None,
    };
    assert!(first.stepped.iter().any(|s| outputs(s) == Some(1)));
    assert!(first.stepped.iter().any(|s| outputs(s) == Some(0)));
    assert!(first.stepped.iter().any(|s| outputs(s).is_none()));
    assert_eq!(first, run_all("window", &inputs));
}

#[test]
fn window_restored_from_a_snapshot_continues_the_same() {
    let inputs = window_inputs();
    let cut = 217;
    let mut original = load("window");
    let mut expected = Vec::new();
    let mut snapshot = Vec::new();
    for (i, payload) in inputs.iter().enumerate() {
        if i == cut {
            snapshot = original.snapshot().unwrap();
        }
        let stepped = original.step(input(i as u64, payload)).unwrap();
        if i >= cut {
            expected.push(stepped);
        }
    }
    let mut restored = load("window");
    let chunks: Vec<&[u8]> = snapshot.iter().map(Vec::as_slice).collect();
    restored.restore(&chunks).unwrap();
    let got: Vec<_> = inputs[cut..]
        .iter()
        .enumerate()
        .map(|(i, payload)| restored.step(input((cut + i) as u64, payload)).unwrap())
        .collect();
    assert_eq!(got, expected);
    assert_eq!(restored.inspect().unwrap(), original.inspect().unwrap());
}
