use super::*;
use crate::spec::{Group, Limits, WorkloadSpec};

const SECOND: u64 = 1_000_000;

fn spec_record(id: &str, state: WorkloadState) -> Vec<u8> {
    grouped_record(id, state, None)
}

fn grouped_record(id: &str, state: WorkloadState, group: Option<Group>) -> Vec<u8> {
    let spec = WorkloadSpec {
        module: "ab".repeat(32),
        inputs: vec!["in".into()],
        output: names::output_stream(id),
        snapshot_every: 10,
        snapshot_every_ms: None,
        keep_snapshots: None,
        restart_intensity: None,
        upgrades: Vec::new(),
        state,
        group,
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
        holding: BTreeSet::new(),
        draining: false,
        blocked: false,
        relist: 0,
    }
}

fn holding(node: &str, ids: &[&str]) -> NodeFact {
    NodeFact {
        held: ids.len(),
        holding: ids.iter().map(|id| id.to_string()).collect(),
        ..fact(node)
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

    fn run_in(&mut self, at: u64, id: &str, group: &Group) -> Vec<Record> {
        self.specs += 1;
        let record = grouped_record(id, WorkloadState::Running, Some(group.clone()));
        self.s
            .step(names::SPECS, self.specs - 1, at, &record)
            .unwrap()
    }

    fn group(&self, name: &str, members: &[(&str, &str)]) -> Record {
        Record {
            specs: self.specs,
            change: Change::Group {
                group: name.into(),
                members: members
                    .iter()
                    .map(|(m, n)| (m.to_string(), n.to_string()))
                    .collect(),
            },
        }
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
                groups: BTreeMap::new(),
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
    f.fact(1, holding("a", &["x", "y"]));
    let out = f.run(2, "w1");
    assert_eq!(out, vec![f.assign("w1", Some("b"))]);
    // b holds w1, its own assignment, and z: full, with w1 counted once.
    f.fact(3, holding("b", &["w1", "z"]));
    let out = f.run(4, "w2");
    assert_eq!(out, vec![f.progress()]);
    assert!(!f.s.assignments().contains_key("w2"));
    // a lets one go and w2 gets the room.
    let out = f.fact(5, holding("a", &["x"]));
    assert_eq!(out, vec![f.assign("w2", Some("a"))]);
}

#[test]
fn a_node_over_its_room_loses_what_it_does_not_hold() {
    let mut f = Feed::hearing(&["a", "b"]);
    f.run(1, "w1");
    f.run(2, "w2");
    f.run(3, "w3");
    assert_eq!(f.s.assignments()["w1"], "a");
    assert_eq!(f.s.assignments()["w3"], "a");
    // a took another by first claim wins: w3 no longer fits, and b has room
    // for it.
    let out = f.fact(4, holding("a", &["x", "w1"]));
    assert_eq!(out, vec![f.assign("w3", Some("b"))]);
    // b fills up too: w3 has nowhere to go and is unassigned.
    let out = f.fact(5, holding("b", &["w2", "y"]));
    assert_eq!(out, vec![f.assign("w3", None)]);
    assert!(!f.s.assignments().contains_key("w3"));
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
                groups: BTreeMap::new(),
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
        draining: true,
        ..holding("n", &["w"])
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

#[test]
fn a_fact_never_exceeds_its_worst_case() {
    let ids: BTreeSet<String> = (0..100)
        .map(|i| format!("{i:0>width$}", width = names::MAX_ID_LEN))
        .collect();
    let fact = NodeFact {
        node: "node-with-a-long-name".into(),
        every_ms: u64::MAX,
        max_workloads: 100,
        held: 100,
        holding: ids,
        draining: true,
        blocked: true,
        relist: 0,
    };
    let worst = worst_fact_bytes("node-with-a-long-name", 100);
    assert!(fact.encode().len() <= worst);
    assert!(worst - fact.encode().len() < 64, "the bound is tight");
}

#[test]
fn a_max_workloads_whose_facts_cannot_fit_is_refused() {
    let fits = (crate::MAX_WRITE_BYTES - 1024) / (names::MAX_ID_LEN + 3);
    assert_eq!(check_fact_fits("n", 64), Ok(()));
    assert_eq!(check_fact_fits("n", fits), Ok(()));
    assert!(check_fact_fits("n", fits * 2).is_err());
    assert!(check_fact_fits("n", usize::MAX).is_err());
}

#[test]
fn a_skip_record_assigns_nothing() {
    let mut f = Feed::hearing(&["a"]);
    let mut skip = SpecRecord::decode(&spec_record("w1", WorkloadState::Running)).unwrap();
    skip.admitted = Some(3);
    skip.skipped = true;
    f.specs += 1;
    let out = f.s.step(names::SPECS, 0, 1, &skip.encode()).unwrap();
    assert_eq!(out, vec![f.progress()]);
}

fn table(specs: u64, assignments: &[(&str, &str)]) -> Record {
    Record {
        specs,
        change: Change::Table {
            groups: BTreeMap::new(),
            assignments: assignments
                .iter()
                .map(|(w, n)| (w.to_string(), n.to_string()))
                .collect(),
        },
    }
}

#[test]
fn a_spec_record_never_ticks() {
    let mut s = Scheduler::default();
    let out = s
        .step(
            names::SPECS,
            0,
            0,
            &spec_record("w", WorkloadState::Running),
        )
        .unwrap();
    assert_eq!(
        out,
        vec![Record {
            specs: 1,
            change: Change::Progress
        }]
    );
    // The first fact is the first tick.
    let out = s.step(names::FACTS, 0, 1, &fact("a").encode()).unwrap();
    assert_eq!(out, vec![table(1, &[("w", "a")])]);
    let mut f = Feed::hearing(&["a"]);
    // A spec record past a boundary does not tick; the next fact does.
    assert_eq!(
        f.run(TABLE_EVERY_MICROS + 1, "w"),
        vec![f.assign("w", Some("a"))]
    );
    assert_eq!(
        f.fact(TABLE_EVERY_MICROS + 2, fact("a")),
        vec![table(1, &[("w", "a")])]
    );
}

#[test]
fn ticks_fall_on_fixed_boundaries() {
    let mut f = Feed::hearing(&[]);
    let half = TABLE_EVERY_MICROS / 2;
    assert_eq!(f.fact(half, fact("a")), vec![table(0, &[])]);
    // Not ten minutes after that table: at the next multiple of the period.
    assert_eq!(f.fact(TABLE_EVERY_MICROS - 1, fact("a")), vec![]);
    assert_eq!(f.fact(TABLE_EVERY_MICROS, fact("a")), vec![table(0, &[])]);
    assert_eq!(f.fact(TABLE_EVERY_MICROS + half, fact("a")), vec![]);
    // Crossing several boundaries at once is one tick.
    assert_eq!(
        f.fact(5 * TABLE_EVERY_MICROS, fact("a")),
        vec![table(0, &[])]
    );
    assert_eq!(f.fact(5 * TABLE_EVERY_MICROS + 1, fact("a")), vec![]);
}

#[test]
fn a_clock_stepping_back_does_not_tick_again() {
    let mut f = Feed::hearing(&["a"]);
    assert_eq!(f.fact(TABLE_EVERY_MICROS, fact("a")), vec![table(0, &[])]);
    // A leader change on sys.facts to a broker whose clock is behind.
    assert_eq!(f.fact(TABLE_EVERY_MICROS - SECOND, fact("a")), vec![]);
    // Back past the boundary it already ticked at: still no second tick.
    assert_eq!(f.fact(TABLE_EVERY_MICROS + SECOND, fact("a")), vec![]);
    assert_eq!(
        f.fact(2 * TABLE_EVERY_MICROS, fact("a")),
        vec![table(0, &[])]
    );
}

#[test]
fn a_replay_from_a_snapshot_ticks_where_the_original_did() {
    let times = [
        0,
        TABLE_EVERY_MICROS - SECOND,
        TABLE_EVERY_MICROS + SECOND,
        TABLE_EVERY_MICROS,
        2 * TABLE_EVERY_MICROS - 1,
        2 * TABLE_EVERY_MICROS + 3,
        3 * TABLE_EVERY_MICROS,
    ];
    let outputs = |s: &mut Scheduler, from: usize| -> Vec<Vec<Record>> {
        times[from..]
            .iter()
            .enumerate()
            .map(|(i, &at)| {
                s.step(names::FACTS, (from + i) as u64, at, &fact("a").encode())
                    .unwrap()
            })
            .collect()
    };
    let mut original = Scheduler::default();
    let all = outputs(&mut original, 0);
    for cut in 1..times.len() {
        let mut first = Scheduler::default();
        for (i, &at) in times[..cut].iter().enumerate() {
            first
                .step(names::FACTS, i as u64, at, &fact("a").encode())
                .unwrap();
        }
        let mut restored = Scheduler::decode(&first.encode()).unwrap();
        assert_eq!(outputs(&mut restored, cut), all[cut..], "cut at {cut}");
    }
}

#[test]
fn a_snapshot_from_before_ticks_restores_and_ticks_at_its_next_fact() {
    // A state as the build before ticks wrote it.
    let old = br#"{"now_micros":5,"facts_micros":5,"specs":0,"table_micros":5,"nodes":{},"wanted":[],"assigned":{}}"#;
    let mut s = Scheduler::decode(old).unwrap();
    assert_eq!(
        s.step(names::FACTS, 0, 6, &fact("a").encode()).unwrap(),
        vec![table(0, &[])]
    );
    assert_eq!(
        s.step(names::FACTS, 1, 7, &fact("a").encode()).unwrap(),
        vec![]
    );
}

fn pair(name: &str, a: &str, b: &str) -> Group {
    Group::new(name, [a.to_owned(), b.to_owned()])
}

#[test]
fn a_group_is_placed_in_one_record_once_every_member_is_admitted() {
    let mut f = Feed::hearing(&["a", "b"]);
    let g = pair("g", "x", "y");
    // The first member alone places nothing, not even by itself.
    assert_eq!(f.run_in(1, "x", &g), vec![f.progress()]);
    assert!(f.s.assignments().is_empty());
    let out = f.run_in(2, "y", &g);
    assert_eq!(out, vec![f.group("g", &[("x", "a"), ("y", "b")])]);
    assert_eq!(f.s.groups()["g"]["y"], "b");
    // Members are never in the per-workload table.
    assert!(f.s.assignments().is_empty());
}

#[test]
fn a_group_that_does_not_fit_whole_is_not_placed_at_all() {
    let mut f = Feed::hearing(&["a"]);
    let g = Group::new("g", ["x".to_owned(), "y".to_owned(), "z".to_owned()]);
    f.run_in(1, "x", &g);
    f.run_in(2, "y", &g);
    // The node has room for two of the three.
    assert_eq!(f.run_in(3, "z", &g), vec![f.progress()]);
    assert!(f.s.groups().is_empty());
    // Room appears, and the whole group goes.
    let more = NodeFact {
        max_workloads: 3,
        ..fact("a")
    };
    let out = f.fact(4, more);
    assert_eq!(
        out,
        vec![f.group("g", &[("x", "a"), ("y", "a"), ("z", "a")])]
    );
}

#[test]
fn a_member_stopping_withdraws_the_groups_placement() {
    let mut f = Feed::hearing(&["a", "b"]);
    let g = pair("g", "x", "y");
    f.run_in(1, "x", &g);
    f.run_in(2, "y", &g);
    let out = f.spec(3, "y", WorkloadState::Stopped);
    assert_eq!(out, vec![f.group("g", &[])]);
    assert!(f.s.groups().is_empty());
}

#[test]
fn a_member_freed_by_a_node_death_is_placed_again_beside_the_ones_still_running() {
    let mut f = Feed::hearing(&["a", "b"]);
    let g = pair("g", "x", "y");
    f.run_in(1, "x", &g);
    f.run_in(2, "y", &g);
    f.fact(3, holding("a", &["x"]));
    f.fact(4, holding("b", &["y"]));
    // b goes silent, which does not end its lease: y stays put.
    assert_eq!(f.fact(7 * SECOND, holding("a", &["x"])), vec![]);
    // b is forgotten: y is free, x keeps running on a.
    let out = f.fact(61 * SECOND, holding("a", &["x"]));
    assert_eq!(out, vec![f.group("g", &[("x", "a"), ("y", "a")])]);
}

#[test]
fn a_silent_node_that_comes_back_without_the_member_frees_it() {
    let mut f = Feed::hearing(&["a", "b"]);
    let g = pair("g", "x", "y");
    f.run_in(1, "x", &g);
    f.run_in(2, "y", &g);
    f.fact(3, holding("a", &["x"]));
    f.fact(4, holding("b", &["y"]));
    assert_eq!(f.fact(7 * SECOND, holding("a", &["x"])), vec![]);
    // b speaks again and holds nothing, but is draining.
    let draining = NodeFact {
        draining: true,
        ..fact("b")
    };
    let out = f.fact(7 * SECOND + 1, draining);
    assert_eq!(out, vec![f.group("g", &[("x", "a"), ("y", "a")])]);
}

#[test]
fn a_silent_node_that_comes_back_taking_work_is_held_again_when_silent() {
    let mut f = Feed::hearing(&["a", "b"]);
    let g = pair("g", "x", "y");
    f.run_in(1, "x", &g);
    f.run_in(2, "y", &g);
    f.fact(3, holding("a", &["x"]));
    f.fact(4, holding("b", &["y"]));
    assert_eq!(f.fact(7 * SECOND, holding("a", &["x"])), vec![]);
    // b speaks again without y but takes work, so the record stands, and
    // b may still claim y under it.
    assert_eq!(f.fact(7 * SECOND + 1, fact("b")), vec![]);
    // b goes silent again: y stays put until b is forgotten.
    assert_eq!(f.fact(14 * SECOND, holding("a", &["x"])), vec![]);
    assert_eq!(f.s.groups()["g"]["y"], "b");
    let out = f.fact(68 * SECOND, holding("a", &["x"]));
    assert_eq!(out, vec![f.group("g", &[("x", "a"), ("y", "a")])]);
}

#[test]
fn the_newer_fact_wins_a_member_two_nodes_say_they_hold() {
    let mut f = Feed::hearing(&["a", "b"]);
    let slow = NodeFact {
        every_ms: 100_000,
        ..fact("c")
    };
    f.fact(0, slow.clone());
    let g = pair("g", "x", "y");
    f.run_in(1, "x", &g);
    f.run_in(2, "y", &g);
    // c held x once and writes rarely; a claimed x after it.
    f.fact(
        SECOND,
        NodeFact {
            holding: ["x".to_owned()].into(),
            held: 1,
            ..slow
        },
    );
    f.fact(2 * SECOND, holding("a", &["x"]));
    f.spec(3 * SECOND, "y", WorkloadState::Stopped);
    // a is silent and c is not, but a's fact is the newer.
    f.fact(10 * SECOND, fact("b"));
    let out = f.run_in(10 * SECOND + 1, "y", &g);
    assert_eq!(out, vec![f.group("g", &[("x", "a"), ("y", "b")])]);
}

/// The group record of #247: both members on a, then b joins.
fn both_on_a() -> Feed {
    let mut f = Feed::hearing(&["a"]);
    let g = pair("g", "x", "y");
    f.run_in(1, "x", &g);
    assert_eq!(
        f.run_in(2, "y", &g),
        vec![f.group("g", &[("x", "a"), ("y", "a")])]
    );
    assert_eq!(f.fact(3, fact("b")), vec![]);
    f
}

#[test]
fn a_group_is_not_moved_off_a_silent_node_that_holds_a_member() {
    let mut f = both_on_a();
    // a claims x and says so, then goes quiet while b keeps writing.
    f.fact(4, holding("a", &["x"]));
    assert_eq!(f.fact(7 * SECOND, fact("b")), vec![]);
    assert_eq!(f.s.groups()["g"]["y"], "a");
}

#[test]
fn a_group_is_not_moved_off_a_silent_node_whose_claim_never_reached_a_fact() {
    let mut f = both_on_a();
    // a may have claimed x under the record after its last fact.
    f.fact(4, fact("a"));
    assert_eq!(f.fact(7 * SECOND, fact("b")), vec![]);
    assert_eq!(f.s.groups()["g"]["x"], "a");
    // Once a is forgotten its leases are long gone, and the group moves whole.
    let out = f.fact(61 * SECOND, fact("b"));
    assert_eq!(out, vec![f.group("g", &[("x", "b"), ("y", "b")])]);
}

#[test]
fn a_node_not_silent_reporting_a_member_ends_a_silent_hold() {
    let mut f = both_on_a();
    f.fact(4, holding("a", &["x"]));
    assert_eq!(f.fact(7 * SECOND, fact("b")), vec![]);
    // b runs x by first claim: a's lease on it is gone. x runs off the
    // record, so the group is split and left alone.
    assert_eq!(f.fact(7 * SECOND + 1, holding("b", &["x"])), vec![]);
    // x stops on b and a is still silent: y may still run on a.
    assert_eq!(f.fact(7 * SECOND + 2, fact("b")), vec![]);
    assert_eq!(f.s.groups()["g"]["y"], "a");
}

#[test]
fn a_group_with_no_record_is_placed_beside_the_members_still_running() {
    let mut f = Feed::hearing(&["a", "b"]);
    let g = pair("g", "x", "y");
    f.run_in(1, "x", &g);
    f.run_in(2, "y", &g);
    f.fact(3, holding("a", &["x"]));
    f.fact(4, holding("b", &["y"]));
    // y stops: the record is withdrawn while x keeps running on a.
    assert_eq!(
        f.spec(5, "y", WorkloadState::Stopped),
        vec![f.group("g", &[])]
    );
    f.fact(6, holding("b", &[]));
    // y is run again with the same group: x stays, y is placed beside it.
    let out = f.run_in(7, "y", &g);
    assert_eq!(out, vec![f.group("g", &[("x", "a"), ("y", "b")])]);
}

#[test]
fn a_split_group_is_left_alone_until_no_member_runs() {
    let mut f = Feed::hearing(&["a", "b", "c"]);
    let g = pair("g", "x", "y");
    f.run_in(1, "x", &g);
    f.run_in(2, "y", &g);
    assert_eq!(f.s.groups()["g"]["x"], "a");
    // x runs on c by first claim wins, off the record, and y runs on b.
    f.fact(3, holding("c", &["x"]));
    f.fact(4, holding("b", &["y"]));
    // b dies. y is free, but x runs off the record: no new placement.
    let out = f.fact(7 * SECOND, holding("c", &["x"]));
    assert_eq!(out, vec![]);
    assert_eq!(f.s.groups()["g"]["y"], "b");
    // x stops running, but silent b may still hold y.
    assert_eq!(f.fact(7 * SECOND + 1, fact("c")), vec![]);
    // b is forgotten: no member runs, and the group is placed whole again.
    let out = f.fact(61 * SECOND, fact("c"));
    assert_eq!(out.len(), 1);
    assert!(matches!(&out[0].change, Change::Group { members, .. } if members.len() == 2));
}

#[test]
fn two_complete_groups_under_one_name_place_neither() {
    let mut f = Feed::hearing(&["a", "b"]);
    let g = pair("g", "x", "y");
    let other = pair("g", "z", "w");
    f.run_in(1, "x", &g);
    f.run_in(2, "y", &g);
    assert_eq!(f.s.groups().len(), 1);
    // A second group of the same name completes: neither is placed.
    f.run_in(3, "z", &other);
    let out = f.run_in(4, "w", &other);
    assert_eq!(out, vec![f.group("g", &[])]);
    assert!(f.s.groups().is_empty());
}

#[test]
fn a_member_declaring_another_group_leaves_the_first_incomplete() {
    let mut f = Feed::hearing(&["a", "b"]);
    let g = pair("g", "x", "y");
    f.run_in(1, "x", &g);
    f.run_in(2, "y", &g);
    // y now says it is in a different group: g is no longer whole.
    let out = f.run_in(3, "y", &pair("g", "y", "z"));
    assert_eq!(out, vec![f.group("g", &[])]);
}

#[test]
fn the_table_carries_the_groups() {
    let mut f = Feed::hearing(&["a", "b"]);
    let g = pair("g", "x", "y");
    f.run_in(1, "x", &g);
    f.run_in(2, "y", &g);
    let out = f.fact(TABLE_EVERY_MICROS, fact("a"));
    let [
        Record {
            change: Change::Table { groups, .. },
            ..
        },
    ] = out.as_slice()
    else {
        panic!("{out:?}");
    };
    assert_eq!(groups["g"]["x"], "a");
}

#[test]
fn a_group_is_not_moved_off_a_blocked_node_that_may_be_claiming_a_member() {
    let mut f = both_on_a();
    // a's claim of x is under way when it cannot read its drain mark: its
    // fact says blocked and does not name x yet.
    let blocked = NodeFact {
        blocked: true,
        ..fact("a")
    };
    assert_eq!(f.fact(4, blocked), vec![]);
    assert_eq!(f.s.groups()["g"]["x"], "a");
    // A draining fact is said only with no claim under way, so it frees both.
    let draining = NodeFact {
        draining: true,
        ..fact("a")
    };
    let out = f.fact(5, draining);
    assert_eq!(out, vec![f.group("g", &[("x", "b"), ("y", "b")])]);
}

fn relisting(node: &str, relist: u64) -> NodeFact {
    NodeFact {
        relist,
        ..fact(node)
    }
}

#[test]
fn a_relisting_fact_is_answered_after_every_other_output_of_its_step() {
    let mut f = Feed::hearing(&["a", "b"]);
    f.run(1, "w");
    let draining = NodeFact {
        draining: true,
        ..fact("a")
    };
    assert_eq!(f.fact(2, draining), vec![f.assign("w", Some("b"))]);
    let relisted = |node: &str, relist| Record {
        specs: 1,
        change: Change::Relisted {
            node: node.into(),
            relist,
        },
    };
    // b drains too while c relists: the move comes first, so a reader that
    // has the answer has the move.
    let draining = NodeFact {
        draining: true,
        ..fact("b")
    };
    assert_eq!(f.fact(3, draining), vec![]);
    assert_eq!(
        f.fact(4, relisting("c", 7)),
        vec![f.assign("w", Some("c")), relisted("c", 7)]
    );
    // A tick answers after its table.
    let out = f.fact(TABLE_EVERY_MICROS, relisting("c", 8));
    assert!(matches!(out[0].change, Change::Table { .. }), "{out:?}");
    assert_eq!(out[1..], [relisted("c", 8)]);
    // A draining fact, or one with no nonce, is not answered.
    let draining = NodeFact {
        draining: true,
        ..relisting("a", 9)
    };
    assert_eq!(f.fact(TABLE_EVERY_MICROS + 1, draining), vec![]);
    assert_eq!(f.fact(TABLE_EVERY_MICROS + 2, fact("a")), vec![]);
}

#[test]
fn a_fact_without_a_nonce_encodes_as_before() {
    let encoded = String::from_utf8(fact("a").encode()).unwrap();
    assert!(!encoded.contains("relist"), "{encoded}");
    let back = NodeFact::decode(&relisting("a", 5).encode()).unwrap();
    assert_eq!(back.relist, 5);
}
