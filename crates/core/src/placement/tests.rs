use super::*;

const POLICY: FirstClaimWins = FirstClaimWins {
    max_workloads: 2,
    max_delay: Duration::from_millis(800),
};

const SCHEDULED: Scheduled = Scheduled {
    fallback: POLICY,
    bound: Duration::from_secs(10),
    grace: Duration::from_secs(2),
    group_bound: Duration::from_secs(180),
};

const THIS_NODE: AssignedTo = AssignedTo::ThisNode {
    assignment: 912,
    scheduler_epoch: 3,
};

fn first(millis: u64, reason: Reason) -> Turn {
    Turn::Claim(
        Duration::from_millis(millis),
        Authority::FirstClaim { reason },
    )
}

fn assigned(to: AssignedTo, waited_secs: u64) -> Assigned {
    Assigned {
        scheduler: true,
        to,
        waited: Duration::from_secs(waited_secs),
        group: None,
    }
}

#[test]
fn a_claim_waits_a_share_of_the_delay() {
    let turn = |random| POLICY.turn("w", &Assigned::NONE, random);
    assert_eq!(turn(0.0), first(0, Reason::NoScheduler));
    assert_eq!(turn(0.5), first(400, Reason::NoScheduler));
}

#[test]
fn a_full_node_leaves_the_workload_to_others() {
    assert!(POLICY.has_room(1));
    assert!(!POLICY.has_room(2));
    assert!(!SCHEDULED.has_room(2));
}

#[test]
fn first_claim_wins_ignores_assignments() {
    let elsewhere = assigned(
        AssignedTo::Elsewhere {
            assignee: Assignee::Coming,
        },
        0,
    );
    assert_eq!(
        POLICY.turn("w", &elsewhere, 0.5),
        first(400, Reason::NoScheduler)
    );
}

#[test]
fn the_assignee_claims_at_once() {
    assert_eq!(
        SCHEDULED.turn("w", &assigned(THIS_NODE, 0), 0.9),
        Turn::Claim(
            Duration::ZERO,
            Authority::Assigned {
                assignment: 912,
                scheduler_epoch: 3
            }
        )
    );
}

#[test]
fn others_wait_for_the_assignee_up_to_the_bound() {
    let up = AssignedTo::Elsewhere {
        assignee: Assignee::Coming,
    };
    assert_eq!(
        SCHEDULED.turn("w", &assigned(up, 3), 0.5),
        Turn::Defer(Duration::from_secs(7))
    );
    assert_eq!(
        SCHEDULED.turn("w", &assigned(up, 10), 0.5),
        first(400, Reason::Bound)
    );
}

#[test]
fn an_assignment_not_yet_made_is_waited_for_only_the_grace() {
    let pending = AssignedTo::Nobody { decided: false };
    let mut waited = assigned(pending, 0);
    waited.waited = Duration::from_millis(500);
    assert_eq!(
        SCHEDULED.turn("w", &waited, 0.5),
        Turn::Defer(Duration::from_millis(1500))
    );
    assert_eq!(
        SCHEDULED.turn("w", &assigned(pending, 2), 0.5),
        first(400, Reason::Grace)
    );
}

#[test]
fn a_workload_the_scheduler_left_unassigned_is_first_claim_wins_at_once() {
    let decided = AssignedTo::Nobody { decided: true };
    assert_eq!(
        SCHEDULED.turn("w", &assigned(decided, 0), 0.5),
        first(400, Reason::Unassigned)
    );
}

#[test]
fn an_assignee_that_cannot_take_it_falls_back_at_once() {
    let down = assigned(
        AssignedTo::Elsewhere {
            assignee: Assignee::Unavailable,
        },
        0,
    );
    assert_eq!(
        SCHEDULED.turn("w", &down, 0.5),
        first(400, Reason::AssigneeUnavailable)
    );
}

#[test]
fn without_a_scheduler_it_is_first_claim_wins() {
    let mut nobody = assigned(AssignedTo::Nobody { decided: false }, 0);
    nobody.scheduler = false;
    assert_eq!(
        SCHEDULED.turn("w", &nobody, 0.5),
        first(400, Reason::NoScheduler)
    );
}

#[test]
fn the_scheduler_is_placed_by_first_claim_wins() {
    let elsewhere = assigned(
        AssignedTo::Elsewhere {
            assignee: Assignee::Coming,
        },
        0,
    );
    assert_eq!(
        SCHEDULED.turn(names::SCHEDULER, &elsewhere, 0.5),
        first(400, Reason::Scheduler)
    );
}

#[test]
fn an_assignee_whose_entry_cannot_be_read_is_recorded_as_unreadable() {
    let unread = assigned(
        AssignedTo::Elsewhere {
            assignee: Assignee::Unreadable,
        },
        0,
    );
    assert_eq!(
        SCHEDULED.turn("w", &unread, 0.5),
        first(400, Reason::Unreadable)
    );
}

#[test]
fn the_scheduler_reason_comes_before_no_scheduler() {
    assert_eq!(
        SCHEDULED.turn(names::SCHEDULER, &Assigned::NONE, 0.5),
        first(400, Reason::Scheduler)
    );
}

#[test]
fn a_placement_record_round_trips_in_the_documented_shape() {
    let assigned = PlacementRecord {
        epoch: 7,
        authority: Authority::Assigned {
            assignment: 912,
            scheduler_epoch: 3,
        },
    };
    let bytes = assigned.encode();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
        serde_json::json!({"format": 1, "value": {
            "epoch": 7, "placed": "assigned", "assignment": 912, "scheduler_epoch": 3
        }})
    );
    assert_eq!(PlacementRecord::decode(&bytes).unwrap(), assigned);

    let first = PlacementRecord {
        epoch: 8,
        authority: Authority::FirstClaim {
            reason: Reason::AssigneeUnavailable,
        },
    };
    let bytes = first.encode();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
        serde_json::json!({"format": 1, "value": {
            "epoch": 8, "placed": "first-claim", "reason": "assignee-unavailable"
        }})
    );
    assert_eq!(PlacementRecord::decode(&bytes).unwrap(), first);
}

#[test]
fn a_placement_record_of_another_format_is_refused() {
    let bytes =
        br#"{"format": 2, "value": {"epoch": 1, "placed": "first-claim", "reason": "grace"}}"#;
    assert!(PlacementRecord::decode(bytes).is_err());
}

#[test]
fn a_reason_from_a_newer_build_decodes_and_is_kept() {
    let bytes =
        br#"{"format": 1, "value": {"epoch": 4, "placed": "first-claim", "reason": "some-future-reason"}}"#;
    let record = PlacementRecord::decode(bytes).unwrap();
    assert_eq!(record.epoch, 4);
    assert_eq!(
        record.authority,
        Authority::FirstClaim {
            reason: Reason::Other("some-future-reason".to_owned())
        }
    );
    let again = PlacementRecord::decode(&record.encode()).unwrap();
    assert_eq!(again, record);
}

#[test]
fn an_authority_from_a_newer_build_decodes_as_unknown() {
    let bytes = br#"{"format": 1, "value": {"epoch": 4, "placed": "group", "members": ["a"]}}"#;
    let record = PlacementRecord::decode(bytes).unwrap();
    assert_eq!(record.epoch, 4);
    assert_eq!(record.authority, Authority::Unknown);
}

#[test]
fn reason_names_match_the_record() {
    for reason in [
        Reason::Scheduler,
        Reason::NoScheduler,
        Reason::Unreadable,
        Reason::Unassigned,
        Reason::AssigneeUnavailable,
        Reason::Grace,
        Reason::Bound,
    ] {
        assert_eq!(
            serde_json::to_value(&reason).unwrap(),
            serde_json::json!(reason.name())
        );
    }
}
