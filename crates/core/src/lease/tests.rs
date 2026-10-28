use super::*;

#[test]
fn a_lease_round_trips() {
    let lease = Lease::renewal("node-a", 3);
    assert_eq!(Lease::decode(&lease.encode()), Ok(lease));
}

#[test]
fn the_json_form_is_fixed() {
    assert_eq!(Lease::claim("n1").encode(), br#"{"owner":"n1","epoch":0}"#);
}

#[test]
fn a_claims_epoch_is_its_version_and_renewals_keep_it() {
    assert_eq!(Lease::claim("a").epoch_at(41), 42);
    assert_eq!(Lease::claim("a").epoch_at(0), 1);
    assert_eq!(Lease::renewal("a", 42).epoch_at(97), 42);
}

#[test]
fn a_holder_renews_at_a_third_and_stops_at_two_thirds() {
    let timing = LeaseTiming {
        ttl: Duration::from_secs(9),
    };
    assert_eq!(timing.renew_after(), Duration::from_secs(3));
    assert_eq!(timing.stop_after(), Duration::from_secs(6));
    assert_eq!(timing.ttl_millis(), 9000);
}

#[test]
fn garbage_is_refused() {
    assert!(matches!(Lease::decode(b"{"), Err(DecodeError::Json(_))));
}
