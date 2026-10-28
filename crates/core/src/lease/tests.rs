use super::*;

#[test]
fn a_lease_round_trips() {
    let lease = Lease {
        owner: "node-a".into(),
        epoch: 3,
    };
    assert_eq!(Lease::decode(&lease.encode()), Ok(lease));
}

#[test]
fn the_json_form_is_fixed() {
    let lease = Lease::claim("n1", None);
    assert_eq!(lease.encode(), br#"{"owner":"n1","epoch":1}"#);
}

#[test]
fn each_claim_advances_the_epoch() {
    let first = Lease::claim("a", None);
    let second = Lease::claim("b", Some(&first));
    assert_eq!((second.owner.as_str(), second.epoch), ("b", 2));
}

#[test]
fn garbage_is_refused() {
    assert!(matches!(Lease::decode(b"{"), Err(DecodeError::Json(_))));
}
