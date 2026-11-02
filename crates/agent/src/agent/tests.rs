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

#[tokio::test]
async fn a_slot_freed_while_looking_for_room_is_not_missed() {
    let room = Notify::new();
    let freed = freed_unless(&room, || {
        room.notify_waiters();
        false
    })
    .expect("there is no room");
    tokio::time::timeout(Duration::from_millis(200), freed)
        .await
        .expect("missed the freed slot");
}

#[tokio::test]
async fn shutdown_ends_a_wait_that_never_finishes() {
    let shutdown = std::future::ready(());
    tokio::pin!(shutdown);
    assert_eq!(unless(shutdown, std::future::pending::<()>()).await, None);
}

#[test]
fn a_failed_spec_read_below_the_record_is_read_again() {
    // A stale read of an earlier run's failed spec is not the record's spec.
    assert_eq!(spec_write(2, WorkloadState::Failed, 3), SpecWrite::Stale);
    assert_eq!(spec_write(2, WorkloadState::Running, 3), SpecWrite::Stale);
    assert_eq!(spec_write(3, WorkloadState::Running, 3), SpecWrite::Write);
    assert_eq!(
        spec_write(4, WorkloadState::Failed, 3),
        SpecWrite::Done(true)
    );
    assert_eq!(
        spec_write(4, WorkloadState::Running, 3),
        SpecWrite::Done(false)
    );
}

fn config(ttl: Duration, stuck_after: Duration) -> AgentConfig {
    AgentConfig {
        node_id: "node".into(),
        lease: LeaseTiming { ttl },
        placement: FirstClaimWins {
            max_workloads: 4,
            max_delay: Duration::ZERO,
        },
        assignment_bound: Duration::from_secs(15),
        assignment_grace: Duration::from_secs(2),
        facts_every: Duration::from_secs(30),
        stuck_after,
        degraded_backoff: DEFAULT_DEGRADED_BACKOFF,
        admission_token: None,
    }
}

#[test]
fn a_stuck_bound_below_the_lease_ttl_is_refused() {
    let ttl = Duration::from_secs(10);
    assert!(check(&config(ttl, ttl)).is_ok());
    assert!(check(&config(ttl, DEFAULT_STUCK_AFTER)).is_ok());
    // Zero turns the bound off.
    assert!(check(&config(ttl, Duration::ZERO)).is_ok());
    let error = check(&config(ttl, ttl - Duration::from_millis(1))).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("--stuck-after-ms (9999 ms) must be 0 or at least the lease TTL (10000 ms)"),
        "{error}"
    );
}
