use felixd_agent::ps::{Node, Position, Report, Workload};
use felixd_core::class::{ErrorClass, Failure};
use felixd_core::lease::Lease;
use felixd_core::node::{Drain, NodeInfo};
use felixd_core::status::{Degraded, Diverged, Parked, State};

use super::render;

fn workload(id: &str, state: State, holder: Option<Lease>) -> Workload {
    Workload {
        id: id.into(),
        state: Some(state),
        holder,
        holder_draining: false,
        assigned: None,
        outputs: 0,
        last_epoch: None,
        divergence: None,
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
        up,
        info: up.then(|| NodeInfo {
            version: "0.1.0".into(),
            max_workloads: 64,
            held: leases,
            draining: draining.then_some(1),
            blocked: false,
        }),
        draining,
        mark: draining.then_some(Drain { nonce: 1 }),
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
        scheduled: false,
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
        scheduled: false,
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
fn a_degraded_workload_shows_its_class_and_detail() {
    let degraded = Degraded {
        class: "transient".into(),
        node: "node-2".into(),
        detail: "the input stream closed".into(),
    };
    let report = Report {
        workloads: vec![workload("a", State::Degraded(degraded), None)],
        nodes: vec![node("n", true, false, 0)],
        scheduled: false,
    };
    let printed = render(&report);
    let row = printed.lines().nth(1).expect("a workload row");
    let cells: Vec<&str> = row.split_whitespace().take(2).collect();
    assert_eq!(cells, ["a", "degraded"], "{printed}");
    assert!(
        printed.contains("a: degraded (transient) on node-2: the input stream closed"),
        "{printed}"
    );
    // The spec stays running, so nothing says that no node will take it.
    assert!(!printed.contains("no node is up"), "{printed}");
}

/// The output the README shows for a degraded workload.
#[test]
fn a_degraded_workload_prints_as_the_readme_shows() {
    let mut counter = workload(
        "counter",
        State::Degraded(Degraded {
            class: "transient".into(),
            node: "node-2".into(),
            detail: "the input stream closed".into(),
        }),
        None,
    );
    counter.last_epoch = Some(7);
    counter.outputs = 120;
    counter.inputs = vec![Position {
        stream: "in".into(),
        next: 120,
        tail: 140,
    }];
    let report = Report {
        workloads: vec![counter],
        nodes: vec![node("n", true, false, 0)],
        scheduled: false,
    };
    let printed = render(&report);
    let shown: Vec<&str> = printed.lines().take(3).collect();
    assert_eq!(
        shown,
        [
            "WORKLOAD  STATE     HOLDER  EPOCH  OUTPUTS  INPUT  NEXT  TAIL  LAG",
            "counter   degraded  -       (7)    120      in     120   140   20",
            "counter: degraded (transient) on node-2: the input stream closed",
        ]
    );
}

#[test]
fn waiting_with_every_node_draining_says_so() {
    let report = Report {
        workloads: vec![workload("a", State::Waiting, None)],
        nodes: vec![node("n", true, true, 0), node("gone", false, false, 0)],
        scheduled: false,
    };
    let printed = render(&report);
    assert!(
        printed.contains("no node is up and not draining"),
        "{printed}"
    );
}

#[test]
fn a_mark_the_node_has_not_acknowledged_is_pending_and_an_unreadable_entry_is_up() {
    let mut old = node("old", true, true, 1);
    // An acknowledgement left from an earlier mark.
    old.info.as_mut().unwrap().draining = Some(0x5eed);
    let mut newer = node("newer", true, false, 0);
    newer.info = None;
    let report = Report {
        workloads: vec![],
        nodes: vec![old, newer],
        scheduled: false,
    };
    let printed = render(&report);
    assert!(
        printed.contains("old    yes  1       64   0.1.0    pending"),
        "{printed}"
    );
    assert!(
        printed.contains("newer  yes  0       ?    ?        no"),
        "{printed}"
    );
    assert!(
        printed.contains("pending: the node has not said"),
        "{printed}"
    );
}

#[test]
fn with_a_scheduler_each_workload_shows_its_assignment() {
    let mut placed = workload("wl", State::Running, Some(Lease::held("n-2", 3)));
    placed.assigned = Some(felixd_agent::assignments::Placed {
        node: "n-2".into(),
        offset: 912,
    });
    let unplaced = workload("new", State::Waiting, None);
    let report = Report {
        workloads: vec![placed, unplaced],
        nodes: vec![node("n-2", true, false, 1)],
        scheduled: true,
    };
    let printed = render(&report);
    let lines: Vec<Vec<&str>> = printed
        .lines()
        .take(3)
        .map(|l| l.split_whitespace().collect())
        .collect();
    assert_eq!(
        lines[0][..5],
        ["WORKLOAD", "STATE", "HOLDER", "ASSIGNED", "EPOCH"]
    );
    assert_eq!(lines[1][..5], ["wl", "running", "n-2", "n-2@912", "3"]);
    assert_eq!(lines[2][..4], ["new", "waiting", "-", "-"]);
}

const EXPECTED: &str = "7692c3ad3540bb803c020b3aee66cd8887123234ea0c6e7143c0add73ff431ed";
const ACTUAL: &str = "3fc4ccfe745870e2c0d99f71f30ff0656c8dedd41cc1d7d3d376b0dbe685e2f3";

fn diverged() -> Diverged {
    Diverged {
        node: "node-2".into(),
        epoch: 7,
        input_offset: 41,
        index: 0,
        snapshot: Some(30),
        contract: "0.2.0".into(),
        wasmtime: "50.0.0".into(),
        felixd: "0.1.0".into(),
        expected: Some(EXPECTED.into()),
        actual: Some(ACTUAL.into()),
        detail: "replayed output differs from the record at offset 52 of the output stream".into(),
    }
}

/// The output the README shows for a workload that failed on a divergence.
#[test]
fn a_failed_workload_prints_where_it_diverged_as_the_readme_shows() {
    let mut counter = workload(
        "counter",
        State::Failed(Failure {
            class: ErrorClass::Poison,
            detail: "replayed output differs from the record at offset 52 of the output stream"
                .into(),
        }),
        None,
    );
    counter.last_epoch = Some(7);
    counter.outputs = 54;
    counter.inputs = vec![Position {
        stream: "in".into(),
        next: 41,
        tail: 60,
    }];
    counter.divergence = Some(diverged());
    let report = Report {
        workloads: vec![counter],
        nodes: vec![node("n", true, false, 0)],
        scheduled: false,
    };
    let printed = render(&report);
    let shown: Vec<&str> = printed.lines().take(4).collect();
    assert_eq!(
        shown,
        [
            "WORKLOAD  STATE   HOLDER  EPOCH  OUTPUTS  INPUT  NEXT  TAIL  LAG",
            "counter   failed  -       (7)    54       in     41    60    19",
            "counter: failed (poison): replayed output differs from the record at offset 52 of the output stream",
            &format!(
                "counter: diverged at input 41 output 0 on node-2 at epoch 7: the stream holds sha256 {}, the replay produced sha256 {}, started from the snapshot of input 30, contract 0.2.0, wasmtime 50.0.0, felixd 0.1.0: replayed output differs from the record at offset 52 of the output stream",
                EXPECTED, ACTUAL
            ),
        ]
    );
}

#[test]
fn a_workload_with_no_divergence_prints_no_line_for_one() {
    let report = Report {
        workloads: vec![workload("a", State::Running, None)],
        nodes: vec![node("n", true, false, 0)],
        scheduled: false,
    };
    assert!(!render(&report).contains("diverged"));
}
