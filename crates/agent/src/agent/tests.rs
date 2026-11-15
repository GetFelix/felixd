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

#[tokio::test]
async fn a_read_that_hangs_does_not_hold_a_claim_permit() {
    let claims = Arc::new(Semaphore::new(1));
    let held = claims.clone();
    let hung = tokio::spawn(async move {
        let _turn = after_ready(&held, std::future::pending::<()>()).await;
    });
    tokio::task::yield_now().await;
    let turn = tokio::time::timeout(Duration::from_secs(5), after_ready(&claims, async {})).await;
    hung.abort();
    assert!(turn.is_ok(), "a hung read kept the permit");
}

fn watches() -> (
    watch::Sender<Seen>,
    watch::Receiver<Seen>,
    watch::Sender<u64>,
    watch::Sender<bool>,
) {
    let (lease_tx, lease) = watch::channel(Seen::Free);
    let (assignments_tx, _) = watch::channel(0u64);
    let (stop_tx, _) = watch::channel(false);
    (lease_tx, lease, assignments_tx, stop_tx)
}

#[tokio::test(start_paused = true)]
async fn an_assignment_that_lands_ends_the_wait_early() {
    let (_lease_tx, mut lease, assignments_tx, stop_tx) = watches();
    let mut assignments = assignments_tx.subscribe();
    let mut stop = stop_tx.subscribe();
    let landed = tokio::spawn(async move {
        tokio::task::yield_now().await;
        assignments_tx.send_modify(|n| *n += 1);
        assignments_tx
    });
    let paused = wait_or_change(
        Duration::from_secs(3600),
        &mut lease,
        &mut assignments,
        &mut stop,
    )
    .await;
    assert_eq!(paused, Paused::Changed);
    let _ = landed.await;
}

#[tokio::test(start_paused = true)]
async fn a_wait_with_no_change_runs_its_delay_and_stop_ends_it() {
    let (_lease_tx, mut lease, assignments_tx, stop_tx) = watches();
    let mut assignments = assignments_tx.subscribe();
    let mut stop = stop_tx.subscribe();
    let paused = wait_or_change(
        Duration::from_secs(5),
        &mut lease,
        &mut assignments,
        &mut stop,
    )
    .await;
    assert_eq!(paused, Paused::Elapsed);
    stop_tx.send_replace(true);
    let paused = wait_or_change(
        Duration::from_secs(3600),
        &mut lease,
        &mut assignments,
        &mut stop,
    )
    .await;
    assert_eq!(paused, Paused::Stop);
}

#[test]
fn the_placement_record_names_the_epoch_the_holder_took() {
    let record = placement_record(
        7,
        Authority::Assigned {
            assignment: 912,
            scheduler_epoch: 3,
        },
    );
    assert_eq!(record.epoch, 7);
    assert_eq!(
        record.authority,
        Authority::Assigned {
            assignment: 912,
            scheduler_epoch: 3
        }
    );
}
