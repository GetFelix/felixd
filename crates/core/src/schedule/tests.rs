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

fn fact(node: &str) -> NodeFact {
    NodeFact {
        node: node.into(),
        every_ms: 2000,
        max_workloads: 2,
        held: 0,
        draining: false,
        blocked: false,
    }
}

/// A scheduler fed by one sys.specs and one sys.facts offset counter.
struct Feed {
    s: Scheduler,
    specs: u64,
    facts: u64,
}

impl Feed {
    /// A scheduler that has heard from `nodes` at time 0, with its first
    /// table out of the way.
    fn hearing(nodes: &[&str]) -> Feed {
        let mut feed = Feed {
            s: Scheduler::default(),
            specs: 0,
            facts: 0,
        };
        for (i, node) in nodes.iter().enumerate() {
            let out = feed.fact(0, fact(node));
            if i > 0 {
                assert_eq!(out, vec![]);
            }
        }
        feed
    }

    fn fact(&mut self, at: u64, fact: NodeFact) -> Vec<Record> {
        self.facts += 1;
        self.s
            .step(names::FACTS, self.facts - 1, at, &fact.encode())
            .unwrap()
    }

    fn spec(&mut self, at: u64, id: &str, state: WorkloadState) -> Vec<Record> {
        self.specs += 1;
        self.s
            .step(names::SPECS, self.specs - 1, at, &spec_record(id, state))
            .unwrap()
    }

    fn run(&mut self, at: u64, id: &str) -> Vec<Record> {
        self.spec(at, id, WorkloadState::Running)
    }

    fn assign(&self, workload: &str, node: Option<&str>) -> Record {
        Record {
            specs: self.specs,
            change: Change::Assign {
                workload: workload.into(),
                node: node.map(str::to_owned),
            },
        }
    }

    fn progress(&self) -> Record {
        Record {
            specs: self.specs,
            change: Change::Progress,
        }
    }
}

#[test]
fn the_first_step_writes_a_table() {
    let mut s = Scheduler::default();
    assert_eq!(
        s.step(names::FACTS, 0, 0, &fact("a").encode()),
        Ok(vec![Record {
            specs: 0,
            change: Change::Table {
                assignments: BTreeMap::new()
            }
        }])
    );
}

#[test]
fn a_new_workload_goes_to_the_least_loaded_node() {
    let mut f = Feed::hearing(&["b", "a"]);
    let out = f.run(1, "w1");
    assert_eq!(out, vec![f.assign("w1", Some("a"))]);
    let out = f.run(2, "w2");
    assert_eq!(out, vec![f.assign("w2", Some("b"))]);
    let out = f.run(3, "w3");
    assert_eq!(out, vec![f.assign("w3", Some("a"))]);
    let out = f.run(4, "w4");
    assert_eq!(out, vec![f.assign("w4", Some("b"))]);
    // Both are full at two each. The spec record still says how far the
    // scheduler has read.
    let out = f.run(5, "w5");
    assert_eq!(out, vec![f.progress()]);
    assert_eq!(out[0].specs, 5);
    let out = f.run(6, "w2");
    assert_eq!(out, vec![f.progress()]);
    // A stop makes room, and the waiting workload gets it.
    let out = f.spec(7, "w1", WorkloadState::Stopped);
    assert_eq!(out, vec![f.assign("w1", None), f.assign("w5", Some("a"))]);
}

#[test]
fn what_a_node_holds_by_first_claim_counts_against_its_room() {
    let mut f = Feed::hearing(&["a", "b"]);
    f.fact(
        1,
        NodeFact {
            held: 2,
            ..fact("a")
        },
    );
    let out = f.run(2, "w1");
    assert_eq!(out, vec![f.assign("w1", Some("b"))]);
    f.fact(
        3,
        NodeFact {
            held: 2,
            ..fact("b")
        },
    );
    let out = f.run(4, "w2");
    assert_eq!(out, vec![f.progress()]);
    // b's own fact counts w1 among what it holds: still full.
    assert!(!f.s.assignments().contains_key("w2"));
}

#[test]
fn the_scheduler_never_assigns_itself() {
    let mut f = Feed::hearing(&["a"]);
    let out = f.run(1, names::SCHEDULER);
    assert_eq!(out, vec![f.progress()]);
    assert!(f.s.assignments().is_empty());
}

#[test]
fn a_silent_node_loses_its_workloads_by_fact_time() {
    let mut f = Feed::hearing(&["a", "b"]);
    f.run(1, "w");
    assert_eq!(f.s.assignments()["w"], "a");
    // A spec record from a broker whose clock is far ahead does not make
    // anyone silent: silence is measured on sys.facts alone.
    let out = f.run(60 * SECOND, "other");
    assert_eq!(out, vec![f.assign("other", Some("b"))]);
    // b keeps writing; a has been silent for three of its 2 s intervals.
    assert_eq!(f.fact(6 * SECOND, fact("b")), vec![]);
    let out = f.fact(6 * SECOND + 1, fact("b"));
    assert_eq!(out, vec![f.assign("w", Some("b"))]);
    // a coming back takes nothing off b.
    assert_eq!(f.fact(7 * SECOND, fact("a")), vec![]);
    assert_eq!(f.s.assignments()["w"], "b");
}

#[test]
fn a_draining_or_blocked_node_loses_its_workloads_at_once() {
    let mut f = Feed::hearing(&["a", "b", "c"]);
    f.run(1, "w");
    let draining = NodeFact {
        draining: true,
        ..fact("a")
    };
    assert_eq!(f.fact(2, draining), vec![f.assign("w", Some("b"))]);
    let blocked = NodeFact {
        blocked: true,
        ..fact("b")
    };
    assert_eq!(f.fact(3, blocked), vec![f.assign("w", Some("c"))]);
    // Undraining takes nothing back.
    assert_eq!(f.fact(4, fact("a")), vec![]);
}

#[test]
fn with_nowhere_to_go_an_assignment_stays() {
    let mut f = Feed::hearing(&["a"]);
    f.run(1, "w");
    let draining = NodeFact {
        draining: true,
        ..fact("a")
    };
    assert_eq!(f.fact(2, draining), vec![]);
    assert_eq!(f.s.assignments()["w"], "a");
    assert_eq!(f.fact(3, fact("c")), vec![f.assign("w", Some("c"))]);
}

#[test]
fn a_long_silent_node_is_forgotten() {
    let mut f = Feed::hearing(&["a", "b"]);
    let forget = 2 * FORGET_INTERVALS * SECOND;
    f.fact(forget, fact("b"));
    assert!(f.s.knows("a"));
    f.fact(forget + 1, fact("b"));
    assert!(!f.s.knows("a"));
    assert!(f.s.knows("b"));
}

#[test]
fn the_whole_table_is_written_again_by_input_time() {
    let mut f = Feed::hearing(&["a"]);
    f.run(1, "w");
    assert_eq!(f.fact(TABLE_EVERY_MICROS - 1, fact("a")), vec![]);
    assert_eq!(
        f.fact(TABLE_EVERY_MICROS, fact("a")),
        vec![Record {
            specs: 1,
            change: Change::Table {
                assignments: BTreeMap::from([("w".into(), "a".into())])
            }
        }]
    );
}

#[test]
fn bad_inputs_are_refused() {
    let mut s = Scheduler::default();
    assert!(matches!(
        s.step(names::SPECS, 0, 0, b"nope"),
        Err(Refused::Spec(_))
    ));
    assert!(matches!(
        s.step(names::FACTS, 0, 0, b"nope"),
        Err(Refused::Fact(_))
    ));
    assert_eq!(
        s.step("other", 0, 0, &fact("a").encode()),
        Err(Refused::Stream("other".into()))
    );
}

#[test]
fn the_same_inputs_give_the_same_outputs_and_state() {
    let run = || {
        let mut f = Feed::hearing(&["a", "b"]);
        let mut out = f.run(2, "w1");
        out.extend(f.run(3, "w2"));
        out.extend(f.fact(
            4,
            NodeFact {
                draining: true,
                ..fact("a")
            },
        ));
        out.extend(f.fact(9 * SECOND, fact("c")));
        out.extend(f.spec(9 * SECOND, "w1", WorkloadState::Stopped));
        (out, f.s)
    };
    let (first, state) = run();
    assert_eq!(run(), (first, state.clone()));
    assert_eq!(Scheduler::decode(&state.encode()), Ok(state));
}

#[test]
fn a_snapshot_missing_fields_restores_with_defaults() {
    assert_eq!(Scheduler::decode(b"{}"), Ok(Scheduler::default()));
    let s = Scheduler::decode(br#"{"nodes":{"a":{"every_ms":5}}}"#).unwrap();
    assert!(s.knows("a"));
}

#[test]
fn records_round_trip() {
    let fact = NodeFact {
        held: 1,
        draining: true,
        ..fact("n")
    };
    assert_eq!(NodeFact::decode(&fact.encode()), Ok(fact));
    let gone = Record {
        specs: 4,
        change: Change::Assign {
            workload: "w".into(),
            node: None,
        },
    };
    assert_eq!(
        String::from_utf8(gone.encode()).unwrap(),
        r#"{"format":1,"value":{"specs":4,"kind":"assign","workload":"w"}}"#
    );
    assert_eq!(gone.key(), Some("w"));
    assert_eq!(Record::decode(&gone.encode()), Ok(gone));
    let progress = Record {
        specs: 9,
        change: Change::Progress,
    };
    assert_eq!(progress.key(), None);
    assert_eq!(Record::decode(&progress.encode()), Ok(progress));
}
