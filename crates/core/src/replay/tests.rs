use super::*;
use crate::output::{
    Addressed, Rejected, encode, encode_addressed, encode_claim, encode_rejection,
};

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

fn p(payload: &[u8]) -> Produced {
    Produced::unnamed(payload.to_vec())
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
    assert_eq!(r.compare(0, &[p(b"x"), p(b"y")]), None);
    assert_eq!(r.compare(1, &[]), None);
    assert_eq!(r.compare(2, &[p(b"z")]), None);
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
    let found = r.compare(1, &[p(b"other")]).unwrap();
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
            recorded: Some(Summary::from(&p(b"y"))),
            replayed: Some(Summary::from(&p(b"other"))),
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
    let fewer = r.compare(0, &[p(b"x")]).unwrap();
    assert_eq!(fewer.header.index, 1);
    assert_eq!(fewer.replayed, None);
    assert!(
        fewer.to_string().contains("node unknown at epoch 3"),
        "{fewer}"
    );
    let more = r.compare(0, &[p(b"x"), p(b"y"), p(b"z")]).unwrap();
    assert_eq!(more.header.index, 2);
    assert_eq!(more.recorded, None);
    assert!(more.to_string().contains("not on the stream"), "{more}");
}

#[test]
fn outputs_outside_the_range_are_not_kept() {
    let stream = [out(0, 0, 1, b"x"), out(5, 0, 1, b"y")];
    let r = recorded(&stream, 1..5);
    assert_eq!(r.compare(0, &[p(b"other")]).map(|d| d.recorded), Some(None));
    assert_eq!(r.end(), 0);
}

#[test]
fn a_rejection_is_compared_like_an_output() {
    let detail = b"too short";
    let stream = [
        encode_rejection(0, 1, Rejected { code: 7, detail }),
        out(1, 0, 1, b"x"),
    ];
    let r = recorded(&stream, 0..10);
    let rejected = |code| Produced::Rejection {
        code,
        detail: detail.to_vec(),
    };
    assert_eq!(r.compare(0, &[rejected(7)]), None);
    assert_eq!(r.end(), 2);
    let found = r.compare(0, &[rejected(8)]).unwrap();
    assert_eq!(
        found.to_string(),
        format!(
            "input 0 output 0: recorded by node unknown at epoch 1 with a rejection with code 7 and detail sha256 {d}, replayed with a rejection with code 8 and detail sha256 {d}",
            d = digest(detail)
        )
    );
    assert!(r.compare(0, &[p(b"x")]).is_some());
    assert!(r.compare(1, &[rejected(7)]).is_some());
    // A replay that rejects where nothing is recorded diverges too.
    assert!(r.compare(2, &[rejected(7)]).is_some());
}

#[test]
fn an_output_is_compared_by_name_and_key_as_well() {
    let header = OutputHeader {
        input_offset: 0,
        index: 0,
    };
    let addressed = Addressed {
        name: "alerts",
        key: Some(b"k"),
        payload: b"x",
    };
    let stream = [encode_addressed(header, 1, addressed).unwrap()];
    let r = recorded(&stream, 0..10);
    let named = |name: &str, key: Option<&[u8]>| Produced::Output {
        name: name.to_owned(),
        key: key.map(<[u8]>::to_vec),
        payload: b"x".to_vec(),
    };
    assert_eq!(r.compare(0, &[named("alerts", Some(b"k"))]), None);
    let found = r.compare(0, &[named("alerts", None)]).unwrap();
    assert_eq!(
        found.to_string(),
        format!(
            "input 0 output 0: recorded by node unknown at epoch 1 with name alerts, key 6b, sha256 {d}, replayed with name alerts, no key, sha256 {d}",
            d = digest(b"x")
        )
    );
    assert!(r.compare(0, &[named("out", Some(b"k"))]).is_some());
}
