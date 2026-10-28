//! Two agents and one workload: only the lease holder runs it, another node
//! takes over when the holder dies, and a holder frozen past its lease writes
//! nothing once the new owner has.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use common::{ID, INPUT, LEASE_TTL, OUTPUT, assert_uninterrupted, eventually, test_felix};
use felixd_core::output::{self, OutputHeader};
use felixd_core::spec::WorkloadState;

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

async fn epochs(t: &common::TestFelix) -> Vec<u64> {
    t.read_all(OUTPUT)
        .await
        .iter()
        .map(|(_, r)| output::decode(r).unwrap().0.epoch)
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn only_the_lease_holder_runs_a_workload() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &["--snapshot-every", "10"]).await;
    let _a = t.start_agent();
    let _b = t.start_agent();
    t.publish_all(INPUT, (0..60).map(count)).await;
    assert_uninterrupted(&t, &module, 60).await;
    let (lease, _) = t.lease(ID).await.expect("a live lease");
    let written: BTreeSet<u64> = epochs(&t).await.into_iter().collect();
    assert_eq!(written.len(), 1, "more than one owner wrote: {written:?}");
    assert!(lease.epoch > 0, "the holder has renewed");
    assert_eq!(written.first(), Some(&lease.epoch));
}

#[tokio::test(flavor = "multi_thread")]
async fn another_node_takes_over_when_the_holder_dies() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &["--snapshot-every", "10"]).await;
    let mut agents = [t.start_agent(), t.start_agent()];
    t.publish_all(INPUT, (0..40).map(count)).await;
    eventually("output", Duration::from_secs(60), || async {
        (t.felix.tail(OUTPUT).await.unwrap() >= 40).then_some(())
    })
    .await;
    let (lease, _) = t.lease(ID).await.expect("a live lease");
    let holder = agents
        .iter()
        .position(|a| a.node_id == lease.owner)
        .expect("one of ours holds it");
    agents[holder].kill();

    t.publish_all(INPUT, (40..80).map(count)).await;
    let (after, _) = eventually("a new holder", LEASE_TTL * 5, || async {
        t.lease(ID).await.filter(|(l, _)| l.owner != lease.owner)
    })
    .await;
    assert_eq!(after.owner, agents[1 - holder].node_id);
    assert_uninterrupted(&t, &module, 80).await;
    let written: BTreeSet<u64> = epochs(&t).await.into_iter().collect();
    assert_eq!(written.len(), 2, "{written:?}");
}

/// The holder is stopped with `SIGSTOP` after it checked its lease and before
/// it appends, which is what a long GC pause or a frozen VM looks like. By
/// the time it carries on, another node has claimed the workload and written.
/// Its conditional append is refused, the records it finds carry a later
/// epoch, and it stops without writing.
#[tokio::test(flavor = "multi_thread")]
async fn a_holder_frozen_past_its_lease_writes_nothing_after_the_new_owner() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &["--snapshot-every", "10"]).await;
    t.publish_all(INPUT, (0..40).map(count)).await;
    let mut frozen = t.start_agent_with_faults("freeze-before-append=20");
    eventually("the freeze", Duration::from_secs(60), || async {
        frozen.stopped().then_some(())
    })
    .await;
    let (stale, _) = t.lease(ID).await.expect("the frozen node's lease");
    assert_eq!(t.felix.tail(OUTPUT).await.unwrap(), 19);

    let _next = t.start_agent();
    eventually(
        "the new owner's output",
        Duration::from_secs(60),
        || async { (t.felix.tail(OUTPUT).await.unwrap() >= 40).then_some(()) },
    )
    .await;
    let (owner, _) = t.lease(ID).await.expect("the new lease");
    assert_ne!(owner.owner, stale.owner);

    frozen.resume();
    t.publish_all(INPUT, (40..60).map(count)).await;
    assert_uninterrupted(&t, &module, 60).await;
    let epochs = epochs(&t).await;
    let stale_epoch = epochs[0];
    assert!(epochs[..19].iter().all(|&e| e == stale_epoch));
    assert!(
        epochs[19..].iter().all(|&e| e > stale_epoch),
        "the frozen node wrote after waking: {epochs:?}"
    );
    assert!(frozen.alive(), "the frozen node exited");
    assert_eq!(t.spec().await.state, WorkloadState::Running);
}

/// The holder's lease ends early without it knowing, as when the broker's
/// clock jumps ahead, and a later owner writes. The holder's own clock still
/// says it holds the lease, so only the epoch on the record it finds can stop
/// it.
#[tokio::test(flavor = "multi_thread")]
async fn a_holder_stops_at_the_first_record_of_a_later_epoch() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &["--snapshot-every", "10"]).await;
    t.publish_all(INPUT, (0..40).map(count)).await;
    let mut holder =
        t.start_node_with_ttl("holder", "freeze-before-append=20", Duration::from_secs(30));
    eventually("the freeze", Duration::from_secs(60), || async {
        holder.stopped().then_some(())
    })
    .await;
    assert!(t.lease(ID).await.is_some(), "the holder's lease is live");
    // What a later owner would write for input 19.
    let header = OutputHeader {
        input_offset: 19,
        index: 0,
    };
    let later = output::encode(header, 1 << 40, &20u64.to_be_bytes());
    t.felix.publish_if(OUTPUT, vec![later], 19).await.unwrap();

    holder.resume();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        t.felix.tail(OUTPUT).await.unwrap(),
        20,
        "the holder wrote after a later epoch: {:?}",
        epochs(&t).await
    );
    assert!(holder.alive());
    assert_eq!(t.spec().await.state, WorkloadState::Running);
}
