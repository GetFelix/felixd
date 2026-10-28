//! Runs the example workloads through the host: `step` must give the same
//! outputs on every run, and a restored snapshot must carry on identically.

use felixd_runtime::{Limits, Runtime, Workload};

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
    outputs: Vec<Vec<Vec<u8>>>,
    snapshot: Vec<u8>,
    inspect: String,
}

fn run_all(name: &str, inputs: &[Vec<u8>]) -> Run {
    let mut wl = load(name);
    let outputs = inputs.iter().map(|i| wl.step(i).unwrap()).collect();
    Run {
        outputs,
        snapshot: wl.snapshot().unwrap(),
        inspect: wl.inspect().unwrap(),
    }
}

#[test]
fn counter_outputs_the_running_total() {
    let inputs: Vec<Vec<u8>> = (0..5).map(|i| vec![i]).collect();
    let run = run_all("counter", &inputs);
    let totals: Vec<u64> = run
        .outputs
        .iter()
        .map(|o| u64::from_be_bytes(o[0].clone().try_into().unwrap()))
        .collect();
    assert_eq!(totals, [1, 2, 3, 4, 5]);
    assert_eq!(run.snapshot, 5u64.to_be_bytes());
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
    assert!(wl.step(&window_record(1_000, 5)).unwrap().is_empty());
    assert!(wl.step(&window_record(9_999, -3)).unwrap().is_empty());
    let out = wl.step(&window_record(10_000, 7)).unwrap();
    assert_eq!(
        out,
        [br#"{"window_start":0,"count":2,"sum":2,"min":-3,"max":5}"#.to_vec()]
    );
    // A late record joins the open window instead of reopening an old one.
    assert!(wl.step(&window_record(2_000, 1)).unwrap().is_empty());
    assert!(wl.step(&[0; 3]).unwrap().is_empty());
    assert_eq!(
        wl.inspect().unwrap(),
        r#"{"open":{"start":10000,"count":2,"sum":8,"min":1,"max":7},"emitted":1,"rejected":1}"#
    );
}

#[test]
fn window_gives_the_same_outputs_every_run() {
    let inputs = window_inputs();
    let first = run_all("window", &inputs);
    assert!(first.outputs.iter().any(|o| !o.is_empty()));
    assert!(first.outputs.iter().any(|o| o.is_empty()));
    assert_eq!(first, run_all("window", &inputs));
}

#[test]
fn window_restored_from_a_snapshot_continues_the_same() {
    let inputs = window_inputs();
    let cut = 217;
    let mut original = load("window");
    let mut expected = Vec::new();
    let mut snapshot = Vec::new();
    for (i, input) in inputs.iter().enumerate() {
        if i == cut {
            snapshot = original.snapshot().unwrap();
        }
        let out = original.step(input).unwrap();
        if i >= cut {
            expected.push(out);
        }
    }
    let mut restored = load("window");
    restored.restore(&snapshot).unwrap();
    let got: Vec<_> = inputs[cut..]
        .iter()
        .map(|i| restored.step(i).unwrap())
        .collect();
    assert_eq!(got, expected);
    assert_eq!(restored.inspect().unwrap(), original.inspect().unwrap());
}
