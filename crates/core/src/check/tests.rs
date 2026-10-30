use super::*;

fn header(input_offset: u64, index: u32) -> OutputHeader {
    OutputHeader {
        input_offset,
        index,
    }
}

/// A replay where inputs 0..n each produce one output, `[i]`, and input 2
/// produces a second one.
fn replay(n: u64) -> Vec<Expected> {
    let mut expected = Vec::new();
    for i in 0..n {
        expected.push(Expected::output(header(i, 0), vec![i as u8]));
        if i == 2 {
            expected.push(Expected::output(header(2, 1), vec![99]));
        }
    }
    expected
}

fn record(input: u64, index: u32, epoch: u64, payload: &[u8]) -> Vec<u8> {
    output::encode(header(input, index), epoch, payload)
}

fn encode(e: &Expected, epoch: u64) -> Vec<u8> {
    match &e.produced {
        Produced::Output { name, key, payload } => {
            let addressed = output::Addressed {
                name,
                key: key.as_deref(),
                payload,
            };
            output::encode_addressed(e.header, epoch, addressed).unwrap()
        }
        Produced::Rejection { code, detail } => output::encode_rejection(
            e.header.input_offset,
            epoch,
            output::Rejected {
                code: *code,
                detail,
            },
        ),
    }
}

/// The stream an uninterrupted owner at epoch 1 would write.
fn clean(expected: &[Expected]) -> Vec<Vec<u8>> {
    expected.iter().map(|e| encode(e, 1)).collect()
}

fn run(expected: &[Expected], records: &[Vec<u8>], complete: bool) -> Vec<Finding> {
    let records: Vec<(u64, &[u8])> = records
        .iter()
        .enumerate()
        .map(|(i, r)| (i as u64, r.as_slice()))
        .collect();
    check(expected, &records, complete)
}

#[test]
fn a_clean_stream_has_no_findings() {
    let expected = replay(5);
    assert_eq!(run(&expected, &clean(&expected), true), []);
}

#[test]
fn an_owner_change_is_not_a_finding() {
    let expected = replay(5);
    let mut records = clean(&expected);
    for r in &mut records[3..] {
        let (w, p) = output::decode(r).unwrap().output().unwrap();
        *r = output::encode(w.header, 2, p.payload);
    }
    assert_eq!(run(&expected, &records, true), []);
}

#[test]
fn claims_between_outputs_are_not_findings() {
    let expected = replay(5);
    let mut records = vec![output::encode_claim(1, "")];
    records.extend(clean(&expected[..3]));
    records.push(output::encode_claim(2, ""));
    records.push(output::encode_claim(3, ""));
    records.extend(expected[3..].iter().map(|e| encode(e, 3)));
    records.push(output::encode_claim(4, ""));
    assert_eq!(run(&expected, &records, true), []);
}

#[test]
fn an_output_below_a_claims_epoch_is_a_regression() {
    let expected = replay(2);
    let records = vec![
        record(0, 0, 1, &[0]),
        output::encode_claim(2, ""),
        record(1, 0, 1, &[1]),
    ];
    assert_eq!(
        run(&expected, &records, true),
        [Finding::EpochRegression {
            offset: 2,
            epoch: 1,
            previous: 2
        }]
    );
}

#[test]
fn a_missing_output_is_a_gap() {
    let expected = replay(5);
    let mut records = clean(&expected);
    records.remove(3);
    assert_eq!(
        run(&expected, &records, true),
        [Finding::Gap {
            header: header(2, 1)
        }]
    );
}

#[test]
fn a_repeated_output_is_a_duplicate() {
    let expected = replay(5);
    let mut records = clean(&expected);
    records.insert(2, records[1].clone());
    assert_eq!(
        run(&expected, &records, true),
        [Finding::Duplicate {
            offset: 2,
            header: header(1, 0),
            first: 1
        }]
    );
}

#[test]
fn outputs_written_out_of_order_are_found() {
    let expected = replay(5);
    let mut records = clean(&expected);
    records.swap(0, 1);
    assert_eq!(
        run(&expected, &records, true),
        [Finding::OutOfOrder {
            offset: 1,
            header: header(0, 0),
            after: header(1, 0)
        }]
    );
}

#[test]
fn a_lower_epoch_after_a_higher_one_is_a_regression() {
    let expected = replay(3);
    let records = vec![
        record(0, 0, 2, &[0]),
        record(1, 0, 1, &[1]),
        record(2, 0, 2, &[2]),
        record(2, 1, 2, &[99]),
    ];
    assert_eq!(
        run(&expected, &records, true),
        [Finding::EpochRegression {
            offset: 1,
            epoch: 1,
            previous: 2
        }]
    );
}

#[test]
fn a_payload_unlike_the_replay_differs() {
    let expected = replay(2);
    let records = vec![record(0, 0, 1, &[0]), record(1, 0, 1, &[7])];
    assert_eq!(
        run(&expected, &records, true),
        [Finding::Differs {
            offset: 1,
            header: header(1, 0)
        }]
    );
}

#[test]
fn an_output_the_replay_did_not_produce_is_unexpected() {
    let expected = replay(2);
    let mut records = clean(&expected);
    records.push(record(0, 5, 1, &[0]));
    assert_eq!(
        run(&expected, &records, true),
        [Finding::Unexpected {
            offset: 2,
            header: header(0, 5)
        }]
    );
}

#[test]
fn garbage_is_undecodable() {
    let expected = replay(1);
    let records = vec![clean(&expected)[0].clone(), b"x".to_vec()];
    let findings = run(&expected, &records, true);
    assert!(
        matches!(findings[..], [Finding::Undecodable { offset: 1, .. }]),
        "{findings:?}"
    );
}

#[test]
fn a_short_stream_is_behind_only_when_it_should_be_complete() {
    let expected = replay(5);
    let records = clean(&expected)[..4].to_vec();
    assert_eq!(run(&expected, &records, false), []);
    assert_eq!(
        run(&expected, &records, true),
        [Finding::Behind {
            missing: 2,
            first: header(3, 0)
        }]
    );
    assert_eq!(
        run(&expected, &[], true),
        [Finding::Behind {
            missing: 6,
            first: header(0, 0)
        }]
    );
}

#[test]
fn findings_read_as_sentences() {
    let finding = Finding::Duplicate {
        offset: 9,
        header: header(4, 0),
        first: 4,
    };
    assert_eq!(
        finding.to_string(),
        "duplicate at offset 9: input 4 output 0 is already at offset 4"
    );
}

fn snap(input_offset: u64, output_tail: u64) -> Vec<u8> {
    snapshot::encode(
        SnapshotHeader {
            input_offset,
            output_tail,
        },
        1,
        b"state",
    )
}

#[test]
fn a_snapshot_tail_must_split_the_stream_at_its_input() {
    // Inputs 0, 1, 3 and 4 have one output each; offset 2 is a gap.
    let records: Vec<Vec<u8>> = [0, 1, 3, 4]
        .iter()
        .map(|&i| record(i, 0, 1, &[0]))
        .collect();
    let on_stream: Vec<(u64, &[u8])> = [0, 1, 3, 4]
        .iter()
        .zip(&records)
        .map(|(&o, r)| (o, r.as_slice()))
        .collect();
    let good = [snap(1, 2), snap(1, 3), snap(2, 3), snap(4, 5), snap(0, 1)];
    let bad = [snap(1, 4), snap(3, 3), snap(4, 3), snap(0, 0)];
    let snapshots: Vec<(u64, &[u8])> = good
        .iter()
        .enumerate()
        .map(|(i, s)| (i as u64, s.as_slice()))
        .collect();
    assert_eq!(check_snapshots(&snapshots, &on_stream, 5), vec![]);
    for (i, s) in bad.iter().enumerate() {
        let found = check_snapshots(&[(i as u64, s.as_slice())], &on_stream, 5);
        assert!(
            matches!(found.as_slice(), [Finding::SnapshotTail { .. }]),
            "snapshot {i}: {found:?}"
        );
    }
}

#[test]
fn a_snapshot_tail_past_the_stream_is_a_finding() {
    let r = record(0, 0, 1, &[0]);
    let found = check_snapshots(&[(0, &snap(0, 2))], &[(0, &r)], 1);
    assert_eq!(
        found,
        vec![Finding::SnapshotTail {
            offset: 0,
            header: SnapshotHeader {
                input_offset: 0,
                output_tail: 2
            },
            from: 1,
            to: 1,
        }]
    );
}

#[test]
fn claims_do_not_move_a_snapshots_bounds() {
    // Outputs of inputs 0 and 1 at offsets 0 and 2, with claims at 1 and 3.
    let records = [
        record(0, 0, 1, &[0]),
        output::encode_claim(2, ""),
        record(1, 0, 2, &[1]),
        output::encode_claim(3, ""),
    ];
    let on_stream: Vec<(u64, &[u8])> = records
        .iter()
        .enumerate()
        .map(|(i, r)| (i as u64, r.as_slice()))
        .collect();
    for (s, ok) in [(snap(0, 1), true), (snap(0, 2), true), (snap(0, 3), false)] {
        let found = check_snapshots(&[(0, &s)], &on_stream, 4);
        assert_eq!(found.is_empty(), ok, "{found:?}");
    }
    for tail in [3, 4] {
        assert_eq!(check_snapshots(&[(0, &snap(1, tail))], &on_stream, 4), []);
    }
}

/// `replay(5)`, but the workload rejects input 3.
fn with_rejection() -> Vec<Expected> {
    let mut expected = replay(5);
    let at = expected
        .iter()
        .position(|e| e.header == header(3, 0))
        .unwrap();
    expected[at] = Expected::rejection(3, 22, b"bad".to_vec());
    expected
}

#[test]
fn a_rejection_is_checked_like_an_output() {
    let expected = with_rejection();
    let records = clean(&expected);
    assert_eq!(run(&expected, &records, true), []);
    // Missing, repeated and differing rejections are found like outputs.
    let mut missing = records.clone();
    missing.remove(4);
    assert_eq!(
        run(&expected, &missing, true),
        [Finding::Gap {
            header: header(3, 0)
        }]
    );
    let mut twice = records.clone();
    twice.insert(5, records[4].clone());
    assert!(matches!(
        run(&expected, &twice, true).as_slice(),
        [Finding::Duplicate { offset: 5, .. }]
    ));
    let mut other = records.clone();
    other[4] = encode(&Expected::rejection(3, 23, b"bad".to_vec()), 1);
    assert_eq!(
        run(&expected, &other, true),
        [Finding::Differs {
            offset: 4,
            header: header(3, 0)
        }]
    );
}

#[test]
fn an_output_where_the_replay_rejects_differs() {
    let expected = with_rejection();
    let mut records = clean(&expected);
    records[4] = record(3, 0, 1, &[3]);
    assert_eq!(
        run(&expected, &records, true),
        [Finding::Differs {
            offset: 4,
            header: header(3, 0)
        }]
    );
}

#[test]
fn a_rejection_where_the_replay_outputs_differs() {
    let expected = replay(5);
    let mut records = clean(&expected);
    records[4] = encode(&Expected::rejection(3, 22, b"bad".to_vec()), 1);
    assert_eq!(
        run(&expected, &records, true),
        [Finding::Differs {
            offset: 4,
            header: header(3, 0)
        }]
    );
}

#[test]
fn an_output_of_another_name_differs() {
    let expected = replay(2);
    let mut records = clean(&expected);
    let addressed = output::Addressed {
        name: "other",
        key: None,
        payload: &[1],
    };
    records[1] = output::encode_addressed(header(1, 0), 1, addressed).unwrap();
    assert_eq!(
        run(&expected, &records, true),
        [Finding::Differs {
            offset: 1,
            header: header(1, 0)
        }]
    );
}

#[test]
fn a_rejection_moves_a_snapshots_bounds_like_an_output() {
    // Input 0 is rejected at offset 0; input 1 has an output at offset 1.
    let records = [
        output::encode_rejection(
            0,
            1,
            output::Rejected {
                code: 1,
                detail: b"",
            },
        ),
        record(1, 0, 1, &[1]),
    ];
    let on_stream: Vec<(u64, &[u8])> = records
        .iter()
        .enumerate()
        .map(|(i, r)| (i as u64, r.as_slice()))
        .collect();
    assert_eq!(check_snapshots(&[(0, &snap(0, 1))], &on_stream, 2), []);
    assert!(!check_snapshots(&[(0, &snap(0, 0))], &on_stream, 2).is_empty());
}

#[test]
fn manifests_are_checked_and_chunks_are_not_snapshots() {
    let r = record(0, 0, 1, &[0]);
    let chunk = snapshot::encode_chunk(1, &snapshot::sha256(b"s"), b"s");
    let manifest = |output_tail| {
        snapshot::encode_manifest(&snapshot::Manifest {
            taken: snapshot::Taken {
                header: SnapshotHeader {
                    input_offset: 0,
                    output_tail,
                },
                epoch: 1,
            },
            chunks: vec![],
        })
    };
    let good = manifest(1);
    let snapshots = [(0, chunk.as_slice()), (1, good.as_slice())];
    assert_eq!(check_snapshots(&snapshots, &[(0, &r)], 1), []);
    let bad = manifest(2);
    assert!(matches!(
        check_snapshots(&[(0, &chunk), (1, &bad)], &[(0, &r)], 1).as_slice(),
        [Finding::SnapshotTail { offset: 1, .. }]
    ));
}

#[test]
fn each_input_runs_under_the_spec_of_the_claim_before_it() {
    let at = |input: u64, epoch: u64| output::encode(header(input, 0), epoch, b"x");
    let records = [
        output::encode_claim(1, ""),
        at(0, 1),
        output::encode_claim_under(2, 10, 10, "a"),
        at(1, 2),
        at(2, 2),
        // A claim under the same spec changes nothing.
        output::encode_claim_under(3, 10, 10, "b"),
        at(3, 3),
        // Two changes with no output between: the later one runs from 4.
        output::encode_claim_under(4, 11, 11, "a"),
        output::encode_claim_under(5, 12, 12, "b"),
        at(4, 5),
    ];
    assert_eq!(
        specs_applied(records.iter().map(Vec::as_slice), |_| true),
        [(1, 10), (4, 12)]
    );
    assert_eq!(
        specs_claimed(records.iter().map(Vec::as_slice)),
        [10, 11, 12]
    );
}

#[test]
fn a_refused_claim_does_not_displace_the_spec_before_it() {
    let records = [
        output::encode_claim_under(1, 10, 10, "a"),
        // Refused by its owner, which stepped nothing.
        output::encode_claim_under(2, 11, 11, "b"),
    ];
    assert_eq!(
        specs_applied(records.iter().map(Vec::as_slice), |spec| spec != 11),
        [(0, 10)]
    );
}

#[test]
fn a_claim_under_an_older_spec_than_one_claimed_is_passed_over() {
    let at = |input: u64, epoch: u64| output::encode(header(input, 0), epoch, b"x");
    let records = [
        output::encode_claim_under(1, 10, 10, "a"),
        at(0, 1),
        output::encode_claim_under(2, 12, 12, "a"),
        at(1, 2),
        // A node with a stale view claimed under 11 and refused it.
        output::encode_claim_under(3, 11, 11, "b"),
    ];
    assert_eq!(
        specs_applied(records.iter().map(Vec::as_slice), |_| true),
        [(0, 10), (1, 12)]
    );
}

#[test]
fn a_claim_below_a_refused_claims_floor_is_passed_over() {
    let at = |input: u64, epoch: u64| output::encode(header(input, 0), epoch, b"x");
    let records = [
        output::encode_claim_under(1, 10, 10, "a"),
        at(0, 1),
        // Refused after claiming, which still raises the floor to 13.
        output::encode_claim_under(2, 13, 13, "b"),
        // Below that floor, so its owner refused it too.
        output::encode_claim_under(3, 12, 13, "c"),
        at(1, 3),
    ];
    assert_eq!(
        specs_applied(records.iter().map(Vec::as_slice), |spec| spec != 13),
        [(0, 10)]
    );
}

/// rA's claim and outputs, then rB, a new module its owner refused after
/// claiming. Every input is judged under rA, whatever the cache now says.
#[test]
fn a_refused_new_module_never_decides_what_a_check_runs() {
    use crate::spec::{Limits, WorkloadSpec, WorkloadState};
    let spec = |module: &str, fuel: u64| WorkloadSpec {
        module: module.into(),
        inputs: vec!["in".into()],
        output: "wl.wl.out".into(),
        snapshot_every: 1000,
        snapshot_every_ms: None,
        state: WorkloadState::Running,
        limits: Limits {
            fuel_per_call: fuel,
            ..Limits::default()
        },
        failure: None,
        record: None,
    };
    let (a, b) = (spec("aa", 100), spec("bb", 1));
    let claimed = HashMap::from([(0, a.clone()), (1, b.clone())]);
    let records = [
        output::encode_claim_under(1, 0, 0, "x"),
        output::encode(header(0, 0), 1, b"x"),
        output::encode_claim_under(2, 1, 1, "y"),
    ];
    let cached = WorkloadSpec {
        record: Some(1),
        ..b.clone()
    };
    let (lineage, limits) = judged_under(&cached, &claimed, records.iter().map(Vec::as_slice));
    assert_eq!(lineage.module, "aa");
    assert_eq!(limits, [(0, a.limits)]);
}

/// A needs-operator record for r0 is the stream's first record, then r1
/// claims and writes. The check runs r1's module and limits, not the cache's
/// r2, which changed the input and was refused.
#[test]
fn a_needs_operator_record_before_the_first_claim_is_passed_over() {
    use crate::class::OperatorReason;
    use crate::spec::{Limits, WorkloadSpec, WorkloadState};
    let spec = |input: &str, fuel: u64| WorkloadSpec {
        module: "aa".into(),
        inputs: vec![input.into()],
        output: "wl.wl.out".into(),
        snapshot_every: 1000,
        snapshot_every_ms: None,
        state: WorkloadState::Running,
        limits: Limits {
            fuel_per_call: fuel,
            ..Limits::default()
        },
        failure: None,
        record: None,
    };
    let (r1, r2) = (spec("in", 100), spec("other", 1));
    let claimed = HashMap::from([(1, r1.clone())]);
    let records = [
        output::encode_needs_operator(0, 0, OperatorReason::ModuleMissing, "gone"),
        output::encode_claim_under(1, 1, 1, "x"),
        output::encode(header(0, 0), 1, b"x"),
    ];
    let cached = WorkloadSpec {
        record: Some(2),
        ..r2
    };
    let (lineage, limits) = judged_under(&cached, &claimed, records.iter().map(Vec::as_slice));
    assert_eq!(lineage.inputs, ["in"]);
    assert_eq!(limits, [(0, r1.limits)]);
}

/// A needs-operator record for r13 raises the floor like a claim: a stale
/// owner of r12 that claims after it refuses its spec.
#[test]
fn a_needs_operator_record_raises_the_floor() {
    use crate::class::OperatorReason;
    let records = [
        output::encode_claim_under(1, 10, 10, "a"),
        output::encode(header(0, 0), 1, b"x"),
        output::encode_needs_operator(2, 13, OperatorReason::ModuleMissing, "gone"),
        output::encode_claim_under(3, 12, 13, "c"),
        output::encode(header(1, 0), 3, b"x"),
    ];
    assert_eq!(
        specs_applied(records.iter().map(Vec::as_slice), |_| true),
        [(0, 10)]
    );
}

fn stage(id: &str, input: &str) -> (String, Vec<String>) {
    (id.to_owned(), vec![input.to_owned()])
}

#[test]
fn a_pipeline_is_checked_upstream_first() {
    let workloads = [
        stage("c", "wl.b.out"),
        stage("a", "in"),
        stage("other", "in"),
        stage("b", "wl.a.out"),
    ];
    assert_eq!(upstream_first(&workloads), ["a", "b", "c", "other"]);
}

#[test]
fn an_upstream_not_being_checked_changes_nothing() {
    let workloads = [stage("c", "wl.b.out"), stage("a", "in")];
    assert_eq!(upstream_first(&workloads), ["c", "a"]);
}

#[test]
fn a_cycle_still_checks_each_workload_once() {
    let workloads = [stage("a", "wl.b.out"), stage("b", "wl.a.out")];
    assert_eq!(upstream_first(&workloads), ["b", "a"]);
}

#[test]
fn a_join_is_checked_after_each_of_its_upstreams() {
    let workloads = [
        (
            "join".to_owned(),
            vec!["wl.b.out".to_owned(), "wl.a.out".to_owned()],
        ),
        stage("b", "in"),
        stage("a", "in"),
    ];
    assert_eq!(upstream_first(&workloads), ["b", "a", "join"]);
}

fn entry(input: u16, offset: u64) -> merge::Entry {
    merge::Entry {
        input,
        offset,
        timestamp_micros: 0,
    }
}

/// Encodes merge records in order, each carrying on from the one before.
fn merge_log(epoch: u64, entries: &[(u16, u64)]) -> Vec<Vec<u8>> {
    let mut next = vec![0, 0];
    let mut log = vec![merge::encode_claim(epoch, &next, "a")];
    for &(input, offset) in entries {
        log.push(merge::encode_entry(epoch, entry(input, offset), &mut next));
    }
    log
}

fn at(log: &[Vec<u8>]) -> Vec<(u64, &[u8])> {
    (0..).zip(log.iter().map(Vec::as_slice)).collect()
}

/// Two inputs whose records' append times are all 0, as `entry` gives.
fn inputs() -> Vec<InputRecords<'static>> {
    let at = |offsets: &[u64]| offsets.iter().map(|&o| (o, 0)).collect();
    vec![("orders", at(&[0, 1, 2])), ("payments", at(&[1, 3]))]
}

#[test]
fn a_merge_log_of_every_record_once_in_input_order_passes() {
    let log = merge_log(1, &[(0, 0), (1, 1), (0, 1), (1, 3), (0, 2)]);
    assert_eq!(check_merge(&at(&log), &inputs(), true), []);
}

#[test]
fn a_merge_log_still_catching_up_is_only_behind_when_complete() {
    let log = merge_log(1, &[(0, 0), (1, 1)]);
    assert_eq!(check_merge(&at(&log), &inputs(), false), []);
    assert_eq!(
        check_merge(&at(&log), &inputs(), true),
        [
            Finding::MergeBehind {
                input: "orders".into(),
                missing: 2,
                first: 1
            },
            Finding::MergeBehind {
                input: "payments".into(),
                missing: 1,
                first: 3
            },
        ]
    );
}

#[test]
fn a_record_merged_twice_skipped_or_missing_is_a_finding() {
    let order = |offset, input: &str, named, expected| Finding::MergeOrder {
        offset,
        input: input.into(),
        named,
        expected,
    };
    let twice = merge_log(1, &[(0, 0), (0, 0), (0, 1), (0, 2)]);
    assert_eq!(
        check_merge(&at(&twice), &inputs(), false),
        [order(2, "orders", 0, Some(1))]
    );
    let skipped = merge_log(1, &[(0, 0), (0, 2)]);
    assert_eq!(
        check_merge(&at(&skipped), &inputs(), false),
        [order(2, "orders", 2, Some(1))]
    );
    let missing = merge_log(1, &[(1, 2)]);
    assert_eq!(
        check_merge(&at(&missing), &inputs(), false),
        [order(1, "payments", 2, Some(1))]
    );
    // A record carrying another cursor than the entries before it leave.
    let mut wrong = merge_log(1, &[(0, 0)]);
    wrong.push(merge::encode_claim(1, &[0, 0], "b"));
    assert_eq!(
        check_merge(&at(&wrong), &inputs(), false),
        [Finding::MergeCursor { offset: 2 }]
    );
}

#[test]
fn a_merge_log_epoch_regression_and_junk_are_findings() {
    let mut log = merge_log(2, &[(0, 0)]);
    log.push(merge::encode_entry(1, entry(0, 1), &mut [1, 0]));
    log.push(b"junk".to_vec());
    let found = check_merge(&at(&log), &inputs(), false);
    assert_eq!(
        found[0],
        Finding::MergeEpochRegression {
            offset: 2,
            epoch: 1,
            previous: 2
        }
    );
    // Epoch 1's entry also follows epoch 2's claim, not its own.
    assert_eq!(
        found[1],
        Finding::MergeUnclaimed {
            offset: 2,
            epoch: 1
        }
    );
    assert!(matches!(
        found[2],
        Finding::UndecodableMerge { offset: 3, .. }
    ));
    assert_eq!(found.len(), 3, "{found:?}");
}

#[test]
fn an_entry_with_another_append_time_than_its_record_is_a_finding() {
    let log = merge_log(1, &[(0, 0), (0, 1)]);
    let mut stamped = inputs();
    stamped[0].1[1].1 = 77;
    assert_eq!(
        check_merge(&at(&log), &stamped, false),
        [Finding::MergeTimestamp {
            offset: 2,
            input: "orders".into(),
            named: 1,
            recorded: 0,
            found: 77
        }]
    );
}

#[test]
fn entries_must_follow_their_epochs_claim() {
    let mut log = merge_log(1, &[(0, 0)]);
    // Epoch 2 merges without having claimed the log.
    log.push(merge::encode_entry(2, entry(0, 1), &mut [1, 0]));
    assert_eq!(
        check_merge(&at(&log), &inputs(), false),
        [Finding::MergeUnclaimed {
            offset: 2,
            epoch: 2
        }]
    );
    // An entry before any claim.
    let first = [
        merge::encode_entry(1, entry(0, 0), &mut [0, 0]),
        merge::encode_claim(1, &[1, 0], "a"),
    ];
    assert_eq!(
        check_merge(&at(&first), &inputs(), false)[0],
        Finding::MergeUnclaimed {
            offset: 0,
            epoch: 1
        }
    );
}

#[test]
fn an_entry_naming_an_input_the_spec_lacks_is_its_own_finding() {
    let mut log = merge_log(1, &[]);
    log.push(merge::encode_entry(1, entry(2, 0), &mut [0, 0, 0]));
    let found = check_merge(&at(&log), &inputs(), false);
    assert_eq!(
        found[0],
        Finding::MergeUnknownInput {
            offset: 1,
            input: 2
        }
    );
}
