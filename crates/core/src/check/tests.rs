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
        expected.push(Expected {
            header: header(i, 0),
            payload: vec![i as u8],
        });
        if i == 2 {
            expected.push(Expected {
                header: header(2, 1),
                payload: vec![99],
            });
        }
    }
    expected
}

fn record(input: u64, index: u32, epoch: u64, payload: &[u8]) -> Vec<u8> {
    output::encode(header(input, index), epoch, payload)
}

/// The stream an uninterrupted owner at epoch 1 would write.
fn clean(expected: &[Expected]) -> Vec<Vec<u8>> {
    expected
        .iter()
        .map(|e| output::encode(e.header, 1, &e.payload))
        .collect()
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
        let (w, p) = output::decode(r).unwrap();
        *r = output::encode(w.header, 2, p);
    }
    assert_eq!(run(&expected, &records, true), []);
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
