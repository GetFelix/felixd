use super::*;

#[test]
fn a_lease_round_trips() {
    let lease = Lease::held("node-a", 3);
    assert_eq!(Lease::decode(&lease.encode()), Ok(lease));
}

#[test]
fn the_json_form_is_fixed() {
    assert_eq!(Lease::claim("n1").encode(), br#"{"owner":"n1","epoch":0}"#);
}

#[test]
fn a_new_epoch_is_above_the_counter_and_the_output_tail() {
    assert_eq!(Lease::next_epoch(0, 0), 1);
    assert_eq!(Lease::next_epoch(41, 7), 42);
    // The counter was lost but the output stream remembers epoch 500.
    assert_eq!(Lease::next_epoch(3, 500), 501);
}

#[test]
fn the_epoch_counter_round_trips() {
    assert_eq!(encode_epoch(501), b"501");
    assert_eq!(decode_epoch(&encode_epoch(501)), Ok(501));
    assert!(decode_epoch(b"x").is_err());
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
