use super::*;
use crate::output::{encode, encode_claim};

fn at(input_offset: u64) -> SnapshotHeader {
    SnapshotHeader {
        input_offset,
        output_tail: input_offset + 1,
    }
}

#[test]
fn the_latest_snapshot_below_the_end_is_restored() {
    let snapshots = [at(4), at(9), at(14)];
    assert_eq!(nearest_snapshot(snapshots, 15), Some(2));
    assert_eq!(nearest_snapshot(snapshots, 14), Some(1));
    assert_eq!(nearest_snapshot(snapshots, 10), Some(1));
    assert_eq!(nearest_snapshot(snapshots, 5), Some(0));
}

#[test]
fn no_snapshot_below_the_end_starts_from_the_initial_state() {
    assert_eq!(nearest_snapshot([at(4)], 4), None);
    assert_eq!(nearest_snapshot([], 100), None);
    assert_eq!(first_input(None), 0);
    assert_eq!(first_input(Some(at(4))), 5);
}

#[test]
fn snapshots_out_of_order_still_pick_the_highest() {
    // A stale owner's snapshot can land after a later one.
    assert_eq!(nearest_snapshot([at(9), at(4)], 20), Some(0));
    assert_eq!(nearest_snapshot([at(9), at(9)], 20), Some(1));
}

#[test]
fn a_time_past_every_input_replays_them_all() {
    assert_eq!(end_for_time(Some(3), 10), 3);
    assert_eq!(end_for_time(Some(0), 10), 0);
    assert_eq!(end_for_time(None, 10), 10);
    assert_eq!(end_for_time(Some(12), 10), 10);
}

fn out(input_offset: u64, index: u32, epoch: u64, payload: &[u8]) -> Vec<u8> {
    let header = OutputHeader {
        input_offset,
        index,
    };
    encode(header, epoch, payload)
}

fn recorded(records: &[Vec<u8>], inputs: Range<u64>) -> Recorded {
    Recorded::read(records.iter().map(Vec::as_slice), inputs).unwrap()
}

#[test]
fn matching_outputs_do_not_diverge() {
    let stream = [
        encode_claim(1, "a"),
        out(0, 0, 1, b"x"),
        out(0, 1, 1, b"y"),
        out(2, 0, 1, b"z"),
    ];
    let r = recorded(&stream, 0..10);
    assert_eq!(r.compare(0, &[b"x".to_vec(), b"y".to_vec()]), None);
    assert_eq!(r.compare(1, &[]), None);
    assert_eq!(r.compare(2, &[b"z".to_vec()]), None);
    assert_eq!(r.end(), 3);
}

#[test]
fn a_different_payload_names_the_writer_and_both_digests() {
    let stream = [
        encode_claim(1, "a"),
        out(0, 0, 1, b"x"),
        encode_claim(2, "b"),
        out(1, 0, 2, b"y"),
    ];
    let r = recorded(&stream, 0..10);
    let found = r.compare(1, &[b"other".to_vec()]).unwrap();
    assert_eq!(
        found,
        Divergence {
            header: OutputHeader {
                input_offset: 1,
                index: 0
            },
            writer: Some(Writer {
                epoch: 2,
                node: Some("b".into())
            }),
            recorded: Some(digest(b"y")),
            replayed: Some(digest(b"other")),
        }
    );
    assert_eq!(
        found.to_string(),
        format!(
            "input 1 output 0: recorded by node b at epoch 2 with sha256 {}, replayed with sha256 {}",
            digest(b"y"),
            digest(b"other")
        )
    );
}

#[test]
fn a_missing_or_extra_output_diverges() {
    let stream = [encode_claim(3, ""), out(0, 0, 3, b"x"), out(0, 1, 3, b"y")];
    let r = recorded(&stream, 0..10);
    let fewer = r.compare(0, &[b"x".to_vec()]).unwrap();
    assert_eq!(fewer.header.index, 1);
    assert_eq!(fewer.replayed, None);
    assert!(
        fewer.to_string().contains("node unknown at epoch 3"),
        "{fewer}"
    );
    let more = r
        .compare(0, &[b"x".to_vec(), b"y".to_vec(), b"z".to_vec()])
        .unwrap();
    assert_eq!(more.header.index, 2);
    assert_eq!(more.recorded, None);
    assert!(more.to_string().contains("not on the stream"), "{more}");
}

#[test]
fn outputs_outside_the_range_are_not_kept() {
    let stream = [out(0, 0, 1, b"x"), out(5, 0, 1, b"y")];
    let r = recorded(&stream, 1..5);
    assert_eq!(
        r.compare(0, &[b"other".to_vec()]).map(|d| d.recorded),
        Some(None)
    );
    assert_eq!(r.end(), 0);
}
