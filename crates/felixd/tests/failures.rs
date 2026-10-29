//! States no node can get past are recorded as a failed workload, not retried
//! forever.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::time::Duration;

use common::{ID, INPUT, OUTPUT, SNAPSHOTS, assert_uninterrupted, eventually, test_felix};
use felixd_core::output::{self, OutputHeader};
use felixd_core::spec::WorkloadState;

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

fn output_record(input_offset: u64, payload: &[u8]) -> Vec<u8> {
    output::encode(
        OutputHeader {
            input_offset,
            index: 0,
        },
        0,
        payload,
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn a_trap_in_snapshot_fails_the_workload() {
    let Some(t) = test_felix().await else { return };
    t.submit("faulty", &["--snapshot-every", "1"]).await;
    t.publish_all(INPUT, [b"hello".to_vec(), b"trap-snapshot".to_vec()])
        .await;
    let _agent = t.start_agent();
    let failure = t.failure().await;
    assert!(failure.contains("unreachable"), "{failure}");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unreadable_snapshot_fails_the_workload() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    t.publish_all(SNAPSHOTS, [b"junk".to_vec()]).await;
    let _agent = t.start_agent();
    let failure = t.failure().await;
    assert!(failure.contains("unknown record version"), "{failure}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replayed_output_that_differs_from_the_stream_fails_the_workload() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..3).map(count)).await;
    // The counter's first two outputs are 1 and 2.
    t.publish_all(
        OUTPUT,
        [output_record(0, &count(9)), output_record(1, &count(2))],
    )
    .await;
    let _agent = t.start_agent();
    let failure = t.failure().await;
    assert!(failure.contains("replayed output differs"), "{failure}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_different_record_at_the_tail_fails_the_workload() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..5).map(count)).await;
    let _agent = t.start_agent();
    eventually("the first outputs", Duration::from_secs(30), || async {
        (t.outputs(OUTPUT).await.len() >= 5).then_some(())
    })
    .await;
    // Something else writes where input 5's output goes.
    t.publish_all(OUTPUT, [output_record(5, b"bogus")]).await;
    t.publish_all(INPUT, [count(5)]).await;
    let failure = t.failure().await;
    assert!(
        failure.contains("differ from this workload's output"),
        "{failure}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn output_too_large_for_one_write_fails_the_workload() {
    let Some(t) = test_felix().await else { return };
    t.submit("faulty", &[]).await;
    t.publish_all(INPUT, [b"huge".to_vec()]).await;
    let _agent = t.start_agent();
    let failure = t.failure().await;
    assert!(failure.contains("one write may hold"), "{failure}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_fuel_limit_comes_from_the_spec() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &["--fuel", "10"]).await;
    t.publish_all(INPUT, [count(0)]).await;
    let _agent = t.start_agent();
    let failure = t.failure().await;
    assert!(failure.contains("fuel"), "{failure}");
}

/// Recording the failure is slow, longer than the lease TTL. The lease is
/// renewed meanwhile, so it is still the holder's until the failure is in
/// the spec, and then released.
#[tokio::test(flavor = "multi_thread")]
async fn the_lease_is_renewed_while_a_slow_failure_is_recorded() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &["--fuel", "10"]).await;
    t.publish_all(INPUT, [count(0)]).await;
    let _agent = t.start_agent_with_faults("delay-failure-record=5000");
    eventually("a lease", Duration::from_secs(30), || t.lease(ID)).await;
    eventually("the failure", Duration::from_secs(30), || async {
        let lease = t.lease(ID).await;
        let spec = t.spec().await;
        if lease.is_none() {
            assert_eq!(
                spec.state,
                WorkloadState::Failed,
                "the lease lapsed before the failure was recorded"
            );
            return Some(());
        }
        None
    })
    .await;
}

/// The spec is edited while the holder is recording a failure. The failure
/// is written onto the edited spec, not over it.
#[tokio::test(flavor = "multi_thread")]
async fn a_failure_is_recorded_onto_a_spec_edited_meanwhile() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &["--fuel", "10", "--snapshot-every", "10"])
        .await;
    t.publish_all(INPUT, [count(0)]).await;
    let _agent = t.start_agent_with_faults("delay-failure-record=5000");
    eventually("a lease", Duration::from_secs(30), || t.lease(ID)).await;
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let mut spec = t.spec().await;
    assert_eq!(spec.state, WorkloadState::Running);
    spec.snapshot_every = 11;
    t.put_spec(&spec).await;
    let failure = t.failure().await;
    assert!(failure.contains("fuel"), "{failure}");
    assert_eq!(t.spec().await.snapshot_every, 11);
}

/// A node decides to fail the workload and is frozen before it writes the
/// failure, long enough for another node to claim. When it wakes its lease is
/// gone, so it must not fail the workload the other node now runs.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_frozen_before_recording_a_failure_does_not_fail_a_later_owners_workload() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..5).map(count)).await;
    let stale = t.start_agent_with_faults("fail-workload=1,freeze-before-failure-record=1");
    eventually("the freeze", Duration::from_secs(60), || async {
        stale.stopped().then_some(())
    })
    .await;
    let next = t.start_agent();
    eventually("a new owner", Duration::from_secs(30), || async {
        t.lease(ID)
            .await
            .filter(|(lease, _)| lease.owner == next.node_id && lease.epoch > 0)
    })
    .await;
    stale.resume();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let spec = t.spec().await;
    assert_eq!(
        spec.state,
        WorkloadState::Running,
        "the stale node failed it: {:?}",
        spec.failure
    );
    assert_uninterrupted(&t, &module, 5).await;
}
