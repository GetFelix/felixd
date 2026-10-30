use super::*;
use crate::spec::{Limits, WorkloadSpec};

const SECOND: u64 = 1_000_000;

fn spec_record(id: &str, state: WorkloadState) -> Vec<u8> {
    let spec = WorkloadSpec {
        module: "ab".repeat(32),
        inputs: vec!["in".into()],
        output: names::output_stream(id),
        snapshot_every: 10,
        snapshot_every_ms: None,
        keep_snapshots: None,
        state,
        limits: Limits::default(),
        failure: None,
        record: None,
    };
    SpecRecord::new(id, spec).encode()
}

fn fact(node: &str, draining: bool) -> Vec<u8> {
    NodeFact {
        node: node.into(),
        every_ms: 2000,
        max_workloads: 2,
        draining,
    }
    .encode()
}

fn assigned(workload: &str, node: &str) -> Assignment {
    Assignment {
        workload: workload.into(),
        node: Some(node.into()),
    }
}

/// A scheduler that has heard from `nodes` at time 0.
fn hearing(nodes: &[&str]) -> Scheduler {
    let mut s = Scheduler::default();
    for node in nodes {
        assert_eq!(s.step(names::FACTS, 0, &fact(node, false)), Ok(vec![]));
    }
    s
}

#[test]
fn a_new_workload_goes_to_the_least_loaded_node() {
    let mut s = hearing(&["b", "a"]);
    let run = |id| spec_record(id, WorkloadState::Running);
    assert_eq!(
        s.step(names::SPECS, 1, &run("w1")),
        Ok(vec![assigned("w1", "a")])
    );
    assert_eq!(
        s.step(names::SPECS, 2, &run("w2")),
        Ok(vec![assigned("w2", "b")])
    );
    assert_eq!(
        s.step(names::SPECS, 3, &run("w3")),
        Ok(vec![assigned("w3", "a")])
    );
    assert_eq!(
        s.step(names::SPECS, 4, &run("w4")),
        Ok(vec![assigned("w4", "b")])
    );
    // Both are full at two each.
    assert_eq!(s.step(names::SPECS, 5, &run("w5")), Ok(vec![]));
    assert_eq!(s.step(names::SPECS, 6, &run("w2")), Ok(vec![]));
    // A stop makes room, and the waiting workload gets it.
    assert_eq!(
        s.step(names::SPECS, 7, &spec_record("w1", WorkloadState::Stopped)),
        Ok(vec![
            Assignment {
                workload: "w1".into(),
                node: None
            },
            assigned("w5", "a")
        ])
    );
}

#[test]
fn the_scheduler_never_assigns_itself() {
    let mut s = hearing(&["a"]);
    let record = spec_record(names::SCHEDULER, WorkloadState::Running);
    assert_eq!(s.step(names::SPECS, 1, &record), Ok(vec![]));
    assert!(s.assignments().is_empty());
}

#[test]
fn a_stopped_workload_loses_its_assignment() {
    let mut s = hearing(&["a"]);
    s.step(names::SPECS, 1, &spec_record("w", WorkloadState::Running))
        .unwrap();
    let stopped = spec_record("w", WorkloadState::Stopped);
    assert_eq!(
        s.step(names::SPECS, 2, &stopped),
        Ok(vec![Assignment {
            workload: "w".into(),
            node: None
        }])
    );
    assert!(s.assignments().is_empty());
}

#[test]
fn a_silent_node_loses_its_workloads_by_recorded_time() {
    let mut s = hearing(&["a", "b"]);
    s.step(names::SPECS, 1, &spec_record("w", WorkloadState::Running))
        .unwrap();
    assert_eq!(s.assignments()["w"], "a");
    // b keeps writing; a has been silent for three of its 2 s intervals.
    assert_eq!(
        s.step(names::FACTS, 6 * SECOND, &fact("b", false)),
        Ok(vec![])
    );
    assert_eq!(
        s.step(names::FACTS, 6 * SECOND + 1, &fact("b", false)),
        Ok(vec![assigned("w", "b")])
    );
    // a coming back takes nothing off b.
    assert_eq!(
        s.step(names::FACTS, 7 * SECOND, &fact("a", false)),
        Ok(vec![])
    );
    assert_eq!(s.assignments()["w"], "b");
}

#[test]
fn a_draining_node_loses_its_workloads_at_once() {
    let mut s = hearing(&["a", "b"]);
    s.step(names::SPECS, 1, &spec_record("w", WorkloadState::Running))
        .unwrap();
    assert_eq!(
        s.step(names::FACTS, 2, &fact("a", true)),
        Ok(vec![assigned("w", "b")])
    );
    // Undraining takes nothing back.
    assert_eq!(s.step(names::FACTS, 3, &fact("a", false)), Ok(vec![]));
}

#[test]
fn with_nowhere_to_go_an_assignment_stays() {
    let mut s = hearing(&["a"]);
    s.step(names::SPECS, 1, &spec_record("w", WorkloadState::Running))
        .unwrap();
    assert_eq!(s.step(names::FACTS, 2, &fact("a", true)), Ok(vec![]));
    assert_eq!(s.assignments()["w"], "a");
    // A node that turns up takes it.
    assert_eq!(
        s.step(names::FACTS, 3, &fact("c", false)),
        Ok(vec![assigned("w", "c")])
    );
}

#[test]
fn time_never_goes_back() {
    let mut s = hearing(&["a"]);
    s.step(names::FACTS, 10 * SECOND, &fact("b", false))
        .unwrap();
    // An earlier append time, from a merge of two inputs, does not revive a.
    s.step(
        names::SPECS,
        SECOND,
        &spec_record("w", WorkloadState::Running),
    )
    .unwrap();
    assert_eq!(s.assignments()["w"], "b");
}

#[test]
fn bad_inputs_are_refused() {
    let mut s = Scheduler::default();
    assert!(matches!(
        s.step(names::SPECS, 0, b"nope"),
        Err(Refused::Spec(_))
    ));
    assert!(matches!(
        s.step(names::FACTS, 0, b"nope"),
        Err(Refused::Fact(_))
    ));
    assert_eq!(
        s.step("other", 0, &fact("a", false)),
        Err(Refused::Stream("other".into()))
    );
}

#[test]
fn the_same_inputs_give_the_same_assignments_and_state() {
    let inputs: Vec<(&str, u64, Vec<u8>)> = vec![
        (names::FACTS, 0, fact("a", false)),
        (names::FACTS, 1, fact("b", false)),
        (names::SPECS, 2, spec_record("w1", WorkloadState::Running)),
        (names::SPECS, 3, spec_record("w2", WorkloadState::Running)),
        (names::FACTS, 4, fact("a", true)),
        (names::FACTS, 9 * SECOND, fact("c", false)),
        (
            names::SPECS,
            9 * SECOND,
            spec_record("w1", WorkloadState::Stopped),
        ),
    ];
    let run = || {
        let mut s = Scheduler::default();
        let out: Vec<_> = inputs
            .iter()
            .map(|(stream, at, payload)| s.step(stream, *at, payload))
            .collect();
        (out, s)
    };
    let (first, state) = run();
    assert_eq!(run(), (first, state.clone()));
    assert_eq!(Scheduler::decode(&state.encode()), Ok(state));
}

#[test]
fn records_round_trip() {
    let fact = NodeFact {
        node: "n".into(),
        every_ms: 10,
        max_workloads: 3,
        draining: true,
    };
    assert_eq!(NodeFact::decode(&fact.encode()), Ok(fact));
    let gone = Assignment {
        workload: "w".into(),
        node: None,
    };
    assert_eq!(
        String::from_utf8(gone.encode()).unwrap(),
        r#"{"format":1,"value":{"workload":"w"}}"#
    );
    assert_eq!(Assignment::decode(&gone.encode()), Ok(gone));
}
