use super::*;

#[test]
fn a_forged_decision_never_noted_blocks_nothing() {
    let mut recorded = Recorded::default();
    // A record naming u64::MAX or a decision past the tail does not verify,
    // so it is never noted, and later decisions are still recorded.
    assert!(admission::decision_end(u64::MAX, u64::MAX).is_none());
    assert!(admission::decision_end(10, 10).is_none());
    assert!(recorded.wants("x", 5));
    recorded.note("x", 5);
    assert!(!recorded.wants("x", 5));
    assert!(
        !recorded.wants("x", 3),
        "an older decision would be refused"
    );
    assert!(recorded.wants("x", 6));
    assert!(recorded.wants("y", 1), "another workload is unaffected");
}
