//! Two agents and one workload: only the lease holder runs it, another node
//! takes over when the holder dies, and a holder frozen past its lease writes
//! nothing once the new owner has.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use bytes::Bytes;
use common::{
    ID, INPUT, LEASE_TTL, OUTPUT, SNAPSHOTS, assert_uninterrupted, eventually, test_felix,
};
use felixd_core::names;
use felixd_core::output::{self, OutputHeader};
use felixd_core::snapshot::{self, SnapshotHeader};
use felixd_core::spec::Limits;
use felixd_core::spec::WorkloadState;
use felixd_runtime::Runtime;

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

/// What the counter writes for input `i`, stamped with `epoch`.
fn counted(i: u64, epoch: u64) -> Vec<u8> {
    let header = OutputHeader {
        input_offset: i,
        index: 0,
    };
    output::encode(header, epoch, &count(i + 1))
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

/// Compiling a module can take longer than a TTL on a slow machine. The
/// holder keeps renewing meanwhile, so it keeps the lease it took.
#[tokio::test(flavor = "multi_thread")]
async fn a_slow_compile_does_not_cost_the_lease() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..10).map(count)).await;
    let _agent = t.start_agent_with_faults("slow-compile=3000");
    assert_uninterrupted(&t, &module, 10).await;
    let written: BTreeSet<u64> = epochs(&t).await.into_iter().collect();
    assert_eq!(written, BTreeSet::from([1]), "the first claim was lost");
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
    let (_, version) = t.lease(ID).await.expect("the holder's lease is live");
    t.felix
        .cache_delete_if(names::LEASES, ID, version)
        .await
        .unwrap();
    // What a later owner would write for input 19.
    t.felix
        .publish_if(OUTPUT, vec![counted(19, 1 << 40)], 19)
        .await
        .unwrap();

    holder.resume();
    tokio::time::sleep(Duration::from_secs(3)).await;
    // The lease is free, so the holder may claim again, but only at an epoch
    // above the one it found.
    let epochs = epochs(&t).await;
    assert!(
        epochs[20..].iter().all(|&e| e > 1 << 40),
        "the holder wrote at its old epoch after a later one: {epochs:?}"
    );
    assert!(holder.alive());
    assert_eq!(t.spec().await.state, WorkloadState::Running);
}

/// A later epoch on the output stream while the holder's lease is still its
/// own can only mean epochs went backwards. The holder says so in the spec
/// instead of stopping and claiming again forever.
#[tokio::test(flavor = "multi_thread")]
async fn a_later_epoch_under_a_live_lease_fails_the_workload() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &["--snapshot-every", "10"]).await;
    t.publish_all(INPUT, (0..40).map(count)).await;
    let holder =
        t.start_node_with_ttl("holder", "freeze-before-append=20", Duration::from_secs(30));
    eventually("the freeze", Duration::from_secs(60), || async {
        holder.stopped().then_some(())
    })
    .await;
    t.felix
        .publish_if(OUTPUT, vec![counted(19, 1 << 40)], 19)
        .await
        .unwrap();
    holder.resume();
    let failure = t.failure().await;
    assert!(failure.contains("epochs went backwards"), "{failure}");
    assert_eq!(t.felix.tail(OUTPUT).await.unwrap(), 20);
}

/// The output stream holds epoch 500 but the leases cache is new, as after
/// it was recreated, so its versions start again. A claim must still take an
/// epoch above 500, or its first append would find a "later" owner and the
/// workload would stall.
#[tokio::test(flavor = "multi_thread")]
async fn a_claim_takes_an_epoch_above_the_output_streams() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &["--snapshot-every", "10"]).await;
    t.publish_all(INPUT, (0..10).map(count)).await;
    t.publish_if(OUTPUT, (0..5).map(|i| counted(i, 500)).collect(), 0)
        .await;
    let _agent = t.start_agent();
    assert_uninterrupted(&t, &module, 10).await;
    assert_eq!(epochs(&t).await, [[500; 5], [501; 5]].concat());
}

/// The new owner is stopped just before its first append. A stale owner's
/// write of the same input then lands first, at a lower epoch, and the new
/// owner skips it.
#[tokio::test(flavor = "multi_thread")]
async fn a_stale_write_that_lands_first_is_skipped() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &["--snapshot-every", "10"]).await;
    t.publish_all(INPUT, (0..10).map(count)).await;
    let owner = t.start_node_with_ttl("owner", "freeze-before-append=1", Duration::from_secs(30));
    eventually("the freeze", Duration::from_secs(60), || async {
        owner.stopped().then_some(())
    })
    .await;
    let (lease, _) = t.lease(ID).await.expect("the owner's lease");
    t.felix
        .publish_if(OUTPUT, vec![counted(0, lease.epoch - 1)], 0)
        .await
        .unwrap();
    owner.resume();
    assert_uninterrupted(&t, &module, 10).await;
    let mut want = vec![lease.epoch; 10];
    want[0] = lease.epoch - 1;
    assert_eq!(epochs(&t).await, want);
}

/// As above, but the stale owner got further than the new owner's batch. The
/// new owner resumes from the streams, checks what the stale one wrote and
/// carries on after it.
#[tokio::test(flavor = "multi_thread")]
async fn a_stale_writer_that_got_further_makes_the_new_owner_resume() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &["--snapshot-every", "10"]).await;
    t.publish_all(INPUT, (0..10).map(count)).await;
    let owner = t.start_node_with_ttl("owner", "freeze-before-append=1", Duration::from_secs(30));
    eventually("the freeze", Duration::from_secs(60), || async {
        owner.stopped().then_some(())
    })
    .await;
    let (lease, _) = t.lease(ID).await.expect("the owner's lease");
    let stale = lease.epoch - 1;
    t.felix
        .publish_if(OUTPUT, (0..5).map(|i| counted(i, stale)).collect(), 0)
        .await
        .unwrap();
    owner.resume();
    assert_uninterrupted(&t, &module, 10).await;
    assert_eq!(epochs(&t).await, [[stale; 5], [lease.epoch; 5]].concat());
    assert_eq!(t.spec().await.state, WorkloadState::Running);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deleted_workload_is_stopped_and_its_lease_released() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    let _agent = t.start_agent();
    eventually("a lease", Duration::from_secs(30), || t.lease(ID)).await;
    t.felix.cache_delete(names::WORKLOADS, ID).await.unwrap();
    eventually("the lease to go", Duration::from_secs(10), || async {
        t.lease(ID).await.is_none().then_some(())
    })
    .await;
}

/// The agent's workloads watch ends and it opens a new one. A workload still
/// there keeps running; one deleted while no watch was open is stopped once
/// the new watch has redelivered everything else.
#[tokio::test(flavor = "multi_thread")]
async fn a_new_watch_stops_only_workloads_deleted_while_it_was_closed() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    let _agent = t.start_agent_with_faults("drop-watch=2000");
    let (held, _) = eventually("a stamped lease", Duration::from_secs(30), || async {
        t.lease(ID).await.filter(|(l, _)| l.epoch > 0)
    })
    .await;
    let drop_watch = || async {
        t.felix
            .cache_put(names::WORKLOADS, "drop-watch", Bytes::from_static(b"x"))
            .await
            .unwrap();
        t.felix
            .cache_delete(names::WORKLOADS, "drop-watch")
            .await
            .unwrap();
    };

    drop_watch().await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    let (after, _) = t.lease(ID).await.expect("the workload kept its lease");
    assert_eq!(after, held, "the workload was stopped by the new watch");

    drop_watch().await;
    t.felix.cache_delete(names::WORKLOADS, ID).await.unwrap();
    eventually("the lease to go", Duration::from_secs(10), || async {
        t.lease(ID).await.is_none().then_some(())
    })
    .await;
}

/// A holder frozen after taking a snapshot, before writing it, whose lease
/// ends early. The snapshot is written with `publish_if`, so once the new
/// owner has written one the stale snapshot is refused. The holder's long TTL
/// keeps its own renewal from noticing first.
#[tokio::test(flavor = "multi_thread")]
async fn a_holder_frozen_before_a_snapshot_write_writes_no_snapshot_after_the_new_owner() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &["--snapshot-every", "10"]).await;
    t.publish_all(INPUT, (0..30).map(count)).await;
    let frozen = t.start_node_with_ttl(
        "holder",
        "freeze-before-snapshot-write=1",
        Duration::from_secs(30),
    );
    eventually("the freeze", Duration::from_secs(60), || async {
        frozen.stopped().then_some(())
    })
    .await;
    let (stale, version) = t.lease(ID).await.expect("the frozen node's lease");
    assert_eq!(t.felix.tail(SNAPSHOTS).await.unwrap(), 0);
    t.felix
        .cache_delete_if(names::LEASES, ID, version)
        .await
        .unwrap();

    let _next = t.start_agent();
    eventually(
        "the new owner's snapshots",
        Duration::from_secs(60),
        || async { (t.felix.tail(SNAPSHOTS).await.unwrap() >= 3).then_some(()) },
    )
    .await;
    frozen.resume();
    assert_uninterrupted(&t, &module, 30).await;
    let epochs: Vec<u64> = t
        .read_all(SNAPSHOTS)
        .await
        .iter()
        .map(|(_, r)| snapshot::decode(r).unwrap().0.epoch)
        .collect();
    assert!(
        epochs.iter().all(|&e| e > stale.epoch),
        "the frozen node wrote a snapshot at epoch {}: {epochs:?}",
        stale.epoch
    );
}

/// A snapshot can carry a later epoch than any output, from an owner whose
/// steps wrote nothing. A new claim must take an epoch above it, or it would
/// find itself superseded by its own history.
#[tokio::test(flavor = "multi_thread")]
async fn a_claim_takes_an_epoch_above_the_last_snapshot() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &["--snapshot-every", "1000"]).await;
    t.publish_all(INPUT, (0..10).map(count)).await;
    let mut workload = Runtime::new()
        .unwrap()
        .load(&module, Limits::default())
        .unwrap();
    for i in 0..5 {
        workload.step(&count(i)).unwrap();
    }
    let state = workload.snapshot().unwrap();
    t.publish_all(OUTPUT, (0..5).map(|i| counted(i, 1))).await;
    let header = SnapshotHeader {
        input_offset: 4,
        output_tail: 5,
    };
    t.publish_all(SNAPSHOTS, [snapshot::encode(header, 7, &state)])
        .await;

    let _agent = t.start_agent();
    assert_uninterrupted(&t, &module, 10).await;
    let epochs = epochs(&t).await;
    assert!(epochs[5..].iter().all(|&e| e > 7), "{epochs:?}");
}
