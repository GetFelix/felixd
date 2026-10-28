use super::*;

const POLICY: FirstClaimWins = FirstClaimWins {
    max_workloads: 2,
    max_delay: Duration::from_millis(800),
};

fn offer(held: usize) -> Offer<'static> {
    Offer {
        workload: "w",
        held,
    }
}

#[test]
fn a_node_with_room_claims_after_a_share_of_the_delay() {
    assert_eq!(POLICY.claim_after(offer(0), 0.0), Some(Duration::ZERO));
    assert_eq!(POLICY.claim_after(offer(1), 0.5), Some(Duration::from_millis(400)));
}

#[test]
fn a_full_node_leaves_the_workload_to_others() {
    assert_eq!(POLICY.claim_after(offer(2), 0.5), None);
}
