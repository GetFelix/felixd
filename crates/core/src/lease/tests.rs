use super::*;

#[test]
fn a_lease_round_trips() {
    let lease = Lease::held("node-a", 3);
    assert_eq!(Lease::decode(&lease.encode()), Ok(lease));
}

#[test]
fn the_json_form_is_fixed() {
    assert_eq!(
        Lease::claim("n1").encode(),
        br#"{"format":1,"value":{"owner":"n1","epoch":0}}"#
    );
}

#[test]
fn values_from_before_formats_still_read() {
    assert_eq!(
        Lease::decode(br#"{"owner":"n1","epoch":3}"#),
        Ok(Lease::held("n1", 3))
    );
    assert_eq!(decode_epoch(b"501"), Ok(501));
}

#[test]
fn a_build_before_formats_refuses_the_values() {
    #[derive(serde::Deserialize)]
    #[allow(dead_code)]
    struct Before {
        owner: String,
        epoch: u64,
    }
    assert!(serde_json::from_slice::<Before>(&Lease::held("n1", 3).encode()).is_err());
    assert!(serde_json::from_slice::<u64>(&encode_epoch(501)).is_err());
}

#[test]
fn another_format_is_refused() {
    assert_eq!(
        Lease::decode(br#"{"format":2,"value":{"owner":"n1","epoch":3}}"#),
        Err(DecodeError::UnknownVersion(2))
    );
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
    assert_eq!(encode_epoch(501), br#"{"format":1,"value":501}"#);
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

#[test]
fn a_claim_is_tried_when_the_lease_expires_and_at_least_every_ttl() {
    let timing = LeaseTiming {
        ttl: Duration::from_secs(10),
    };
    assert_eq!(timing.claim_wait(8_000, 5_000), Duration::from_secs(3));
    assert_eq!(timing.claim_wait(4_000, 5_000), Duration::ZERO);
    // This node's clock is far behind the broker's.
    assert_eq!(timing.claim_wait(60_000, 5_000), Duration::from_secs(10));
    assert_eq!(timing.claim_wait(0, 5_000), Duration::from_secs(10));
}

#[test]
fn a_lease_is_live_only_while_recently_renewed() {
    let timing = LeaseTiming {
        ttl: Duration::from_millis(3000),
    };
    // One renewal interval and the slack.
    assert_eq!(timing.renewed_within(), Duration::from_millis(2000));
    // Renewed just now, and with the next renewal a little late.
    assert!(timing.renewed_lately(13_000, 10_000));
    assert!(timing.renewed_lately(11_100, 10_000));
    // Overdue by more than the slack: its holder may be gone.
    assert!(!timing.renewed_lately(10_900, 10_000));
    assert!(!timing.renewed_lately(9_000, 10_000));
    assert!(!timing.renewed_lately(0, 10_000));
}

#[test]
fn the_freshness_window_is_at_least_half_a_ttl_and_below_a_ttl() {
    let within = |ms| {
        LeaseTiming {
            ttl: Duration::from_millis(ms),
        }
        .renewed_within()
    };
    // The 2 s test TTL: capped at five sixths.
    assert_eq!(within(2000).as_millis(), 1666);
    assert_eq!(within(10_000), Duration::from_secs(5));
    assert_eq!(within(60_000), Duration::from_secs(30));
}
