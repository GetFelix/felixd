use super::*;

const POLICY: FirstClaimWins = FirstClaimWins {
    max_workloads: 2,
    max_delay: Duration::from_millis(800),
};

#[test]
fn a_claim_waits_a_share_of_the_delay() {
    assert_eq!(POLICY.claim_delay("w", 0.0), Duration::ZERO);
    assert_eq!(POLICY.claim_delay("w", 0.5), Duration::from_millis(400));
}

#[test]
fn a_full_node_leaves_the_workload_to_others() {
    assert!(POLICY.has_room(1));
    assert!(!POLICY.has_room(2));
}
