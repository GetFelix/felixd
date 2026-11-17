use crate::assignments::{Followed, GroupPlacement, Placed};
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
    let bytes = br#"{"format": 1, "value": {"epoch": 3, "placed": "first-claim", "reason": "some-future-reason"}}"#;
    assert_eq!(
        placed_by([bytes.as_slice()].into_iter(), 3),
        PlacedBy::Recorded(Authority::FirstClaim {
            reason: Reason::Other("some-future-reason".to_owned())
        })
    );
}

fn held(m: &str, node: Option<&str>) -> (String, Option<String>) {
    (m.to_owned(), node.map(str::to_owned))
}

#[test]
fn a_group_waits_when_nothing_is_held_and_is_together_when_all_are() {
    assert_eq!(
        group_status(&[held("x", None), held("y", None)], None),
        GroupStatus::Waiting
    );
    let all = [held("x", Some("a")), held("y", Some("b"))];
    let record = BTreeMap::from([("x".into(), "a".into()), ("y".into(), "b".into())]);
    assert_eq!(group_status(&all, Some(&record)), GroupStatus::Together);
}

#[test]
fn held_members_with_no_record_in_force_are_released_not_together() {
    let all = [held("x", Some("a")), held("y", Some("b"))];
    assert_eq!(group_status(&all, None), GroupStatus::Released);
}

#[test]
fn a_record_for_another_member_list_is_not_in_force() {
    let group = Group::new("g", ["x".to_owned(), "y".to_owned()]);
    let mut followed = Followed::default();
    followed.groups.insert(
        "g".to_owned(),
        GroupPlacement {
            members: BTreeMap::from([("x".into(), "a".into())]),
            offset: 1,
            epoch: 1,
        },
    );
    assert_eq!(followed.group_record(&group), None);
    followed
        .groups
        .get_mut("g")
        .unwrap()
        .members
        .insert("y".into(), "b".into());
    assert!(followed.group_record(&group).is_some());
}

#[test]
fn a_group_is_split_with_a_member_free_or_held_off_its_record() {
    let part = [held("x", Some("a")), held("y", None)];
    assert_eq!(group_status(&part, None), GroupStatus::Split);
    let all = [held("x", Some("a")), held("y", Some("c"))];
    let record = BTreeMap::from([("x".into(), "a".into()), ("y".into(), "b".into())]);
    assert_eq!(group_status(&all, Some(&record)), GroupStatus::Split);
}

#[test]
fn an_assignment_claim_is_behind_by_the_records_since() {
    let placed = |offset| Placed {
        node: "n".into(),
        offset,
        epoch: 1,
    };
    let held = HeldAt::of(
        2,
        &PlacedBy::Recorded(Authority::Assigned {
            assignment: 10,
            scheduler_epoch: 1,
        }),
    );
    assert_eq!(held.assignment_lag(Some(&placed(10))), Some(0));
    assert_eq!(held.assignment_lag(Some(&placed(25))), Some(15));
    assert_eq!(held.assignment_lag(None), None);
    let first = HeldAt::of(
        3,
        &PlacedBy::Recorded(Authority::FirstClaim {
            reason: Reason::Grace,
        }),
    );
    assert_eq!(first.assignment_lag(Some(&placed(25))), None);
    assert_eq!(HeldAt::of(4, &PlacedBy::Unknown).assignment, None);
}
