//! Runs the example workloads in wasmtime directly, to check the WIT world and
//! that `step` is repeatable. The host proper is built on top of this.

use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, Result, Store};

wasmtime::component::bindgen!({
    path: "../../wit",
    world: "workload",
});

struct Instance {
    store: Store<()>,
    workload: Workload,
}

impl Instance {
    fn new(name: &str) -> Result<Instance> {
        let mut config = Config::new();
        config.cranelift_nan_canonicalization(true);
        let engine = Engine::new(&config)?;
        let component = Component::new(&engine, felixd_testkit::workload(name))?;
        let mut store = Store::new(&engine, ());
        let workload = Workload::instantiate(&mut store, &component, &Linker::new(&engine))?;
        Ok(Instance { store, workload })
    }

    fn step(&mut self, input: &[u8]) -> Result<Vec<Vec<u8>>> {
        self.workload.call_step(&mut self.store, input)
    }
}

fn window_record(ts: u64, value: i64) -> Vec<u8> {
    let mut r = ts.to_le_bytes().to_vec();
    r.extend(value.to_le_bytes());
    r
}

fn window_inputs() -> Vec<Vec<u8>> {
    let mut inputs = Vec::new();
    for i in 0..500u64 {
        inputs.push(window_record(i * 1_300, (i as i64 * 37) % 101 - 50));
    }
    inputs.push(vec![1, 2, 3]);
    inputs
}

#[derive(Debug, PartialEq)]
struct Run {
    outputs: Vec<Vec<Vec<u8>>>,
    snapshot: Vec<u8>,
    inspect: String,
}

fn run_all(name: &str, inputs: &[Vec<u8>]) -> Result<Run> {
    let mut wl = Instance::new(name)?;
    let mut outputs = Vec::new();
    for input in inputs {
        outputs.push(wl.step(input)?);
    }
    let snapshot = wl.workload.call_snapshot(&mut wl.store)?;
    let inspect = wl.workload.call_inspect(&mut wl.store)?;
    Ok(Run {
        outputs,
        snapshot,
        inspect,
    })
}

#[test]
fn counter_outputs_the_running_total() -> Result<()> {
    let inputs: Vec<Vec<u8>> = (0..5).map(|i| vec![i]).collect();
    let Run {
        outputs,
        snapshot,
        inspect,
    } = run_all("counter", &inputs)?;
    let totals: Vec<u64> = outputs
        .iter()
        .map(|o| u64::from_be_bytes(o[0].clone().try_into().unwrap()))
        .collect();
    assert_eq!(totals, [1, 2, 3, 4, 5]);
    assert_eq!(snapshot, 5u64.to_be_bytes());
    assert_eq!(inspect, r#"{"count":5}"#);
    Ok(())
}

#[test]
fn counter_gives_the_same_outputs_every_run() -> Result<()> {
    let inputs: Vec<Vec<u8>> = (0..200u8).map(|i| vec![i]).collect();
    assert_eq!(run_all("counter", &inputs)?, run_all("counter", &inputs)?);
    Ok(())
}

#[test]
fn window_emits_a_summary_when_a_window_closes() -> Result<()> {
    let mut wl = Instance::new("window")?;
    assert!(wl.step(&window_record(1_000, 5))?.is_empty());
    assert!(wl.step(&window_record(9_999, -3))?.is_empty());
    let out = wl.step(&window_record(10_000, 7))?;
    assert_eq!(
        out,
        [br#"{"window_start":0,"count":2,"sum":2,"min":-3,"max":5}"#.to_vec()]
    );
    // A late record joins the open window instead of reopening an old one.
    assert!(wl.step(&window_record(2_000, 1))?.is_empty());
    assert!(wl.step(&[0; 3])?.is_empty());
    assert_eq!(
        wl.workload.call_inspect(&mut wl.store)?,
        r#"{"open":{"start":10000,"count":2,"sum":8,"min":1,"max":7},"emitted":1,"rejected":1}"#
    );
    Ok(())
}

#[test]
fn window_gives_the_same_outputs_every_run() -> Result<()> {
    let inputs = window_inputs();
    let first = run_all("window", &inputs)?;
    assert!(first.outputs.iter().any(|o| !o.is_empty()));
    assert!(first.outputs.iter().any(|o| o.is_empty()));
    assert_eq!(first, run_all("window", &inputs)?);
    Ok(())
}

#[test]
fn window_restored_from_a_snapshot_continues_the_same() -> Result<()> {
    let inputs = window_inputs();
    let cut = 217;
    let mut original = Instance::new("window")?;
    let mut expected = Vec::new();
    let mut snapshot = Vec::new();
    for (i, input) in inputs.iter().enumerate() {
        if i == cut {
            snapshot = original.workload.call_snapshot(&mut original.store)?;
        }
        let out = original.step(input)?;
        if i >= cut {
            expected.push(out);
        }
    }
    let mut restored = Instance::new("window")?;
    restored
        .workload
        .call_restore(&mut restored.store, &snapshot)?;
    let mut got = Vec::new();
    for input in &inputs[cut..] {
        got.push(restored.step(input)?);
    }
    assert_eq!(got, expected);
    assert_eq!(
        restored.workload.call_inspect(&mut restored.store)?,
        original.workload.call_inspect(&mut original.store)?
    );
    Ok(())
}

#[test]
fn examples_import_nothing() -> Result<()> {
    for name in ["counter", "window"] {
        let engine = Engine::default();
        let component = Component::new(&engine, felixd_testkit::workload(name))?;
        let imports = component.component_type().imports(&engine).count();
        assert_eq!(imports, 0, "{name} imports something");
    }
    Ok(())
}
