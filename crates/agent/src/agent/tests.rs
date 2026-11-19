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
        group_bound: Duration::from_secs(180),
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

fn fact_of(standing: &Standing) -> NodeFact {
    standing.fact("n", Duration::from_secs(1), 4, &AtomicUsize::new(0))
}

#[test]
fn a_fact_does_not_say_draining_while_a_claim_is_under_way() {
    let standing = Standing::new();
    standing.draining.send_replace(true);
    assert!(fact_of(&standing).draining);
    let under_way = ClaimUnderWay::start(&standing);
    assert!(!fact_of(&standing).draining);
    drop(under_way);
    assert!(fact_of(&standing).draining);
}

#[test]
fn a_won_claim_is_held_by_the_time_the_claim_ends() {
    let standing = Standing::new();
    standing.draining.send_replace(true);
    // Once the claim is won, a fact that says draining must list it.
    let check = || {
        let fact = fact_of(&standing);
        assert!(!fact.draining || fact.holding.contains("w"), "{fact:?}");
    };
    let under_way = ClaimUnderWay::start(&standing);
    // The claim won: it is named before it stops counting as under way.
    check();
    let holds = Holds::start(&standing, "w");
    check();
    drop(under_way);
    check();
    assert!(fact_of(&standing).draining);
    drop(holds);
}

#[test]
fn a_fact_names_every_held_workload_even_past_max_workloads() {
    let standing = Standing::new();
    let names = ["a", "b", "c"];
    let _holds: Vec<_> = names.iter().map(|n| Holds::start(&standing, n)).collect();
    let fact = standing.fact("n", Duration::from_secs(1), 2, &AtomicUsize::new(3));
    assert_eq!(
        fact.holding,
        names.iter().map(|n| (*n).to_owned()).collect()
    );
}

fn scheduler_output(offset: u64, change: felixd_core::schedule::Change) -> Vec<u8> {
    use felixd_core::output::{Addressed, OutputHeader, encode_addressed};
    let payload = felixd_core::schedule::Record { specs: 0, change }.encode();
    let header = OutputHeader {
        input_offset: offset,
        index: 0,
    };
    let addressed = Addressed {
        name: "out",
        key: None,
        payload: &payload,
    };
    encode_addressed(header, 1, addressed).unwrap()
}

fn answer(node: &str, relist: u64) -> felixd_core::schedule::Change {
    felixd_core::schedule::Change::Relisted {
        node: node.into(),
        relist,
    }
}

#[test]
fn a_group_member_is_placed_here_only_once_relisted_since_start_and_every_lift() {
    let assignments = Assignments::new();
    let standing = Standing::new();
    let group = Group::new("g", ["x".to_owned(), "y".to_owned()]);
    let members = [("x".into(), "n".into()), ("y".into(), "n".into())];
    let record = felixd_core::schedule::Change::Group {
        group: "g".into(),
        members: members.into(),
    };
    assignments.fold(3, &scheduler_output(0, record));
    let placed = || group_placed(&assignments, &standing, "n", "x", &group);
    let here = GroupPlaced::ThisNode {
        assignment: 3,
        scheduler_epoch: 1,
    };
    // Just started: the scheduler's last fact of n may say it is draining.
    assert_eq!(placed(), GroupPlaced::Unplaced);
    let first = fact_of(&standing).relist;
    assert_ne!(first, 0);
    assignments.fold(4, &scheduler_output(1, answer("n", first)));
    assert_eq!(placed(), here);
    assert_eq!(fact_of(&standing).relist, 0);

    // Drained, then lifted before the scheduler folds a fact of n that is
    // not draining (#257): the answer to an earlier nonce does not count.
    assert!(standing.set_draining(true));
    assert_eq!(fact_of(&standing).relist, 0);
    assert!(standing.set_draining(false));
    let again = fact_of(&standing).relist;
    assert!(again != 0 && again != first);
    assert_eq!(placed(), GroupPlaced::Unplaced);
    assignments.fold(5, &scheduler_output(2, answer("o", again)));
    assert_eq!(placed(), GroupPlaced::Unplaced);
    assignments.fold(6, &scheduler_output(3, answer("n", again)));
    assert_eq!(placed(), here);
}

#[test]
fn a_group_member_is_placed_from_one_view_of_its_record_and_relist_answer() {
    let assignments = Assignments::new();
    let standing = Standing::new();
    let group = Group::new("g", ["x".to_owned(), "y".to_owned()]);
    let record = |node: &str| felixd_core::schedule::Change::Group {
        group: "g".into(),
        members: [("x".into(), node.into()), ("y".into(), node.into())].into(),
    };
    // The view in force places x on n; n is not yet relisted.
    assignments.fold(3, &scheduler_output(0, record("n")));
    let relist = fact_of(&standing).relist;
    assert_ne!(relist, 0);
    // The next view moves x to m and answers n's nonce. `follow` swaps it
    // in right after the first lock this decision takes.
    let mut next = assignments.view();
    next.fold(4, &scheduler_output(1, record("m")));
    next.fold(5, &scheduler_output(2, answer("n", relist)));
    assignments.swap_view_at_lock(1, next);
    let placed = group_placed(&assignments, &standing, "n", "x", &group);
    assert_eq!(placed, GroupPlaced::Unplaced);
    // The new view is in force from the next decision on.
    let _ = assignments.get("x");
    assert_eq!(
        group_placed(&assignments, &standing, "n", "x", &group),
        GroupPlaced::Elsewhere
    );
}

#[test]
fn a_lift_takes_a_new_nonce_before_it_shows() {
    let standing = Standing::new();
    standing.set_draining(true);
    let mut lifted = standing.draining.subscribe();
    let before = standing.next_relist.load(Ordering::SeqCst);
    standing.set_draining(false);
    assert!(lifted.has_changed().unwrap());
    assert!(!*lifted.borrow_and_update());
    assert_eq!(fact_of(&standing).relist, before);
    // Setting what is already set takes none.
    assert!(!standing.set_draining(false));
    assert_eq!(fact_of(&standing).relist, before);
}

#[test]
fn a_claim_decided_before_a_drain_and_lift_is_not_made() {
    let standing = Standing::new();
    let decided = standing.drain_changes();
    assert!(!standing.drained_since(decided));
    // A drain and lift between the decision and the claim's own mark read,
    // which then finds no mark.
    standing.set_draining(true);
    standing.set_draining(false);
    assert!(!*standing.draining.borrow());
    assert!(standing.drained_since(decided));
    let decided = standing.drain_changes();
    assert!(!standing.set_draining(false));
    assert!(!standing.drained_since(decided));
}

#[test]
fn a_node_whose_facts_stop_landing_fences_itself_and_must_be_relisted() {
    let standing = Standing::new();
    let bound = Duration::from_secs(300);
    let start = tokio::time::Instant::now();
    standing.facts_due_from(start);
    assert!(standing.relisted(|_| true));
    assert!(!standing.check_fence(start + bound, bound));
    let decided = standing.drain_changes();
    assert!(standing.check_fence(start + bound + Duration::from_millis(1), bound));
    assert!(*standing.fenced.borrow());
    // The scheduler may forget it from here on, so a record placing a member
    // here waits for an answer to a fact sent after the fence (#251).
    let relist = fact_of(&standing).relist;
    assert_ne!(relist, 0);
    assert!(standing.drained_since(decided));
    // A fact lands: the fence ends, and the new nonce still needs its answer.
    let landed = start + 2 * bound;
    standing.facts_due_from(landed);
    assert!(standing.check_fence(landed, bound));
    assert!(!*standing.fenced.borrow());
    assert_eq!(fact_of(&standing).relist, relist);
}

#[test]
fn a_group_member_keeps_the_node_writing_facts_whether_held_or_waiting() {
    let standing = Standing::new();
    let other = Holds::start(&standing, "w");
    assert_eq!(*standing.group_members.borrow(), 0);
    let member = GroupMember::start(&standing);
    assert_eq!(*standing.group_members.borrow(), 1);
    drop(other);
    assert_eq!(*standing.group_members.borrow(), 1);
    drop(member);
    assert_eq!(*standing.group_members.borrow(), 0);
}

#[test]
fn the_fence_bound_outlasts_a_slow_append_but_not_half_the_forget_window() {
    let s = Duration::from_secs;
    assert_eq!(fence_bound(s(30)), s(300));
    assert_eq!(fence_bound(s(1)), s(11));
    assert_eq!(
        fence_bound(Duration::from_millis(100)),
        Duration::from_millis(1500)
    );
}
