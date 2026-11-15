use felixd_core::placement::{Authority, PlacementRecord, Reason};

use super::*;

fn record(epoch: u64, reason: Reason) -> Vec<u8> {
    PlacementRecord {
        epoch,
        authority: Authority::FirstClaim { reason },
    }
    .encode()
}

#[test]
fn two_records_for_one_epoch_read_as_the_newest() {
    // A lost acknowledgement and a retry leave both on the stream.
    let records = [
        record(3, Reason::Grace),
        record(3, Reason::Grace),
        record(2, Reason::Bound),
    ];
    let found = placed_by(records.iter().map(Vec::as_slice), 3);
    assert_eq!(
        found,
        PlacedBy::Recorded(Authority::FirstClaim {
            reason: Reason::Grace
        })
    );
}

#[test]
fn no_record_for_the_epoch_is_unknown() {
    let records = [record(2, Reason::Bound), b"nope".to_vec()];
    assert_eq!(
        placed_by(records.iter().map(Vec::as_slice), 3),
        PlacedBy::Unknown
    );
}

#[test]
fn a_record_from_a_newer_build_is_recorded_not_unknown() {
    let bytes = br#"{"format": 1, "value": {"epoch": 3, "placed": "first-claim", "reason": "group-bound"}}"#;
    assert_eq!(
        placed_by([bytes.as_slice()].into_iter(), 3),
        PlacedBy::Recorded(Authority::FirstClaim {
            reason: Reason::Other("group-bound".to_owned())
        })
    );
}
