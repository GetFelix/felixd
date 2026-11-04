use felixd_core::spec::Limits;
use felixd_runtime::Input;

use super::*;

fn step(workload: &mut Workload, offset: u64) {
    workload
        .step(Input {
            stream: "in",
            incarnation: 0,
            offset,
            timestamp_micros: 0,
            key: None,
            payload: &[],
            publisher: None,
        })
        .unwrap();
}

fn count(workload: &mut Workload) -> String {
    workload.inspect().unwrap()
}

/// The counter adds one per input. The `changed` build adds two once the
/// total reaches ten. Switching at input 12 leaves 12 from the first build
/// and 2 per input from the second.
#[test]
fn state_carries_over_at_the_input_an_upgrade_names() {
    let runtime = Runtime::new().unwrap();
    let plain = felixd_testkit::workload("counter");
    let changed = felixd_testkit::workload_with("counter", &["changed"]);
    let mut workload = runtime.load(&plain, Limits::default()).unwrap();
    let mut upgrades = Upgrades {
        runtime: runtime.clone(),
        pending: vec![(12, changed)],
    };
    for offset in 0..15 {
        upgrades.reach(&mut workload, offset).unwrap();
        step(&mut workload, offset);
        if offset == 11 {
            assert_eq!(count(&mut workload), r#"{"count":12}"#);
        }
    }
    assert_eq!(count(&mut workload), r#"{"count":18}"#);
}

#[test]
fn an_upgrade_applies_at_the_next_input_when_its_own_is_passed_over() {
    let runtime = Runtime::new().unwrap();
    let plain = felixd_testkit::workload("counter");
    let changed = felixd_testkit::workload_with("counter", &["changed"]);
    let mut workload = runtime.load(&plain, Limits::default()).unwrap();
    let mut upgrades = Upgrades {
        runtime,
        pending: vec![(12, changed)],
    };
    for offset in 0..11 {
        step(&mut workload, offset);
    }
    upgrades.reach(&mut workload, 11).unwrap();
    // Input 12 was unreadable; 13 is the first stepped under the new module.
    upgrades.reach(&mut workload, 13).unwrap();
    step(&mut workload, 13);
    assert_eq!(count(&mut workload), r#"{"count":13}"#);
    step(&mut workload, 14);
    assert_eq!(count(&mut workload), r#"{"count":15}"#);
}
