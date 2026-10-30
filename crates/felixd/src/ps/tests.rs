use felixd_agent::ps::{Node, Position, Report, Workload};
use felixd_core::class::{ErrorClass, Failure};
use felixd_core::lease::Lease;
use felixd_core::node::NodeInfo;
use felixd_core::status::{Parked, State};

use super::render;

fn workload(id: &str, state: State, holder: Option<Lease>) -> Workload {
    Workload {
        id: id.into(),
        state: Some(state),
        holder,
        holder_draining: false,
        outputs: 0,
        last_epoch: None,
        inputs: vec![Position {
            stream: "in".into(),
            next: 0,
            tail: 0,
        }],
        steps: None,
        unread: None,
    }
}

fn node(id: &str, up: bool, draining: bool, leases: usize) -> Node {
    Node {
        id: id.into(),
        info: up.then(|| NodeInfo {
            version: "0.1.0".into(),
            max_workloads: 64,
            held: leases,
        }),
        draining,
        leases,
    }
}

#[test]
fn every_input_and_the_merge_log_get_a_line() {
    let mut join = workload("join", State::Running, Some(Lease::held("node-2", 5)));
    join.holder_draining = true;
    join.outputs = 40;
    join.inputs = vec![
        Position {
            stream: "a".into(),
            next: 20,
            tail: 22,
        },
        Position {
            stream: "b".into(),
            next: 18,
            tail: 18,
        },
    ];
    join.steps = Some(Position {
        stream: "wl.join.merge".into(),
        next: 38,
        tail: 41,
    });
    let report = Report {
        workloads: vec![join],
        nodes: vec![node("node-2", true, true, 1)],
    };
    let printed = render(&report);
    let lines: Vec<&str> = printed.lines().collect();
    assert_eq!(
        lines[1].split_whitespace().collect::<Vec<_>>(),
        [
            "join",
            "running",
            "node-2",
            "(draining)",
            "5",
            "40",
            "a",
            "20",
            "22",
            "2"
        ],
        "{printed}"
    );
    assert_eq!(
        lines[2].split_whitespace().collect::<Vec<_>>(),
        ["b", "18", "18", "0"]
    );
    assert_eq!(
        lines[3].split_whitespace().collect::<Vec<_>>(),
        ["wl.join.merge", "38", "41", "3"]
    );
    assert!(
        printed.contains("node-2  yes  1       64   0.1.0    yes"),
        "{printed}"
    );
}

#[test]
fn a_parked_or_failed_workload_says_why() {
    let parked = Parked {
        reason: "module missing".into(),
        detail: "not in modules".into(),
    };
    let failed = Failure {
        class: ErrorClass::Poison,
        detail: "trap".into(),
    };
    let report = Report {
        workloads: vec![
            workload("a", State::NeedsOperator(parked), None),
            workload("b", State::Failed(failed), None),
        ],
        nodes: vec![node("n", true, false, 0)],
    };
    let printed = render(&report);
    assert!(
        printed.contains("a: needs operator: module missing (not in modules)"),
        "{printed}"
    );
    assert!(printed.contains("b: failed (poison): trap"), "{printed}");
    assert!(!printed.contains("no node is up"), "{printed}");
}

#[test]
fn waiting_with_every_node_draining_says_so() {
    let report = Report {
        workloads: vec![workload("a", State::Waiting, None)],
        nodes: vec![node("n", true, true, 0), node("gone", false, false, 0)],
    };
    let printed = render(&report);
    assert!(
        printed.contains("no node is up and not draining"),
        "{printed}"
    );
}
