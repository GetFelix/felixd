use super::*;

const POLICY: FirstClaimWins = FirstClaimWins {
    max_workloads: 2,
    max_delay: Duration::from_millis(800),
};

const SCHEDULED: Scheduled = Scheduled {
    fallback: POLICY,
    bound: Duration::from_secs(10),
};

fn assigned(to: AssignedTo, waited_secs: u64) -> Assigned {
    Assigned {
        scheduler: true,
        to,
        waited: Duration::from_secs(waited_secs),
    }
}

#[test]
fn a_claim_waits_a_share_of_the_delay() {
    let turn = |random| POLICY.turn("w", &Assigned::NONE, random);
    assert_eq!(turn(0.0), Turn::Claim(Duration::ZERO));
    assert_eq!(turn(0.5), Turn::Claim(Duration::from_millis(400)));
}

#[test]
fn a_full_node_leaves_the_workload_to_others() {
    assert!(POLICY.has_room(1));
    assert!(!POLICY.has_room(2));
    assert!(!SCHEDULED.has_room(2));
}

#[test]
fn first_claim_wins_ignores_assignments() {
    let elsewhere = assigned(AssignedTo::Elsewhere { coming: true }, 0);
    assert_eq!(
        POLICY.turn("w", &elsewhere, 0.5),
        Turn::Claim(Duration::from_millis(400))
    );
}

#[test]
fn the_assignee_claims_at_once() {
    assert_eq!(
        SCHEDULED.turn("w", &assigned(AssignedTo::ThisNode, 0), 0.9),
        Turn::Claim(Duration::ZERO)
    );
}

#[test]
fn others_wait_for_the_assignee_up_to_the_bound() {
    let up = AssignedTo::Elsewhere { coming: true };
    assert_eq!(
        SCHEDULED.turn("w", &assigned(up, 3), 0.5),
        Turn::Defer(Duration::from_secs(7))
    );
    assert_eq!(
        SCHEDULED.turn("w", &assigned(up, 10), 0.5),
        Turn::Claim(Duration::from_millis(400))
    );
}

#[test]
fn an_assignment_not_yet_made_is_waited_for_up_to_the_bound() {
    let pending = AssignedTo::Nobody { decided: false };
    assert_eq!(
        SCHEDULED.turn("w", &assigned(pending, 4), 0.5),
        Turn::Defer(Duration::from_secs(6))
    );
    assert_eq!(
        SCHEDULED.turn("w", &assigned(pending, 11), 0.5),
        Turn::Claim(Duration::from_millis(400))
    );
}

#[test]
fn a_workload_the_scheduler_left_unassigned_is_first_claim_wins_at_once() {
    let decided = AssignedTo::Nobody { decided: true };
    assert_eq!(
        SCHEDULED.turn("w", &assigned(decided, 0), 0.5),
        Turn::Claim(Duration::from_millis(400))
    );
}

#[test]
fn an_assignee_that_cannot_take_it_falls_back_at_once() {
    let down = assigned(AssignedTo::Elsewhere { coming: false }, 0);
    assert_eq!(
        SCHEDULED.turn("w", &down, 0.5),
        Turn::Claim(Duration::from_millis(400))
    );
}

#[test]
fn without_a_scheduler_it_is_first_claim_wins() {
    let mut nobody = assigned(AssignedTo::Nobody { decided: false }, 0);
    nobody.scheduler = false;
    assert_eq!(
        SCHEDULED.turn("w", &nobody, 0.5),
        Turn::Claim(Duration::from_millis(400))
    );
}

#[test]
fn the_scheduler_is_placed_by_first_claim_wins() {
    let elsewhere = assigned(AssignedTo::Elsewhere { coming: true }, 0);
    assert_eq!(
        SCHEDULED.turn(names::SCHEDULER, &elsewhere, 0.5),
        Turn::Claim(Duration::from_millis(400))
    );
}
