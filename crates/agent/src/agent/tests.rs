use super::*;

#[test]
fn a_redelivery_is_done_only_once_every_retained_value_is_in() {
    let mut r = Redelivery::new(2, 100);
    assert!(!r.done());
    assert!(!r.note("a", 10));
    // However long the second retained value takes, nothing is pruned before.
    assert!(r.note("b", 40));
    assert_eq!(r.seen, HashSet::from(["a".to_owned(), "b".to_owned()]));
}

#[test]
fn a_live_change_ends_the_redelivery() {
    // A key whose newest write raced past the live edge is not retained; its
    // change arrives live instead, after the retained ones.
    let mut r = Redelivery::new(3, 100);
    assert!(!r.note("a", 10));
    assert!(r.note("b", 100));
    assert!(r.seen.contains("b"));
}

#[test]
fn an_empty_redelivery_is_done_at_once() {
    assert!(Redelivery::new(0, 7).done());
}
