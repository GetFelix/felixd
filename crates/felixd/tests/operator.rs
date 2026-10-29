//! A workload that needs an operator is recorded once on its output stream,
//! and every node leaves it alone until its spec changes.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::time::Duration;

use common::{ID, INPUT, LEASE_TTL, OUTPUT, eventually, test_felix};
use felixd_core::class::OperatorReason;
use felixd_core::names;
use felixd_core::output::{self, Record};
use felixd_core::spec::WorkloadState;

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

fn needs_operator(epoch: u64, spec: u64) -> Vec<u8> {
    output::encode_needs_operator(epoch, spec, OperatorReason::ModuleMissing, "from the test")
}

/// A node that finds the record for its spec record skips the workload
/// before it claims, so it takes no epoch. A new spec record clears it.
#[tokio::test(flavor = "multi_thread")]
async fn a_recorded_workload_is_skipped_before_the_claim_until_the_spec_changes() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..3).map(count)).await;
    let spec = t.spec().await;
    t.publish_if(OUTPUT, vec![needs_operator(0, spec.record.unwrap())], 0)
        .await;
    let _agent = t.start_agent();
    tokio::time::sleep(LEASE_TTL * 3).await;
    assert!(t.outputs(OUTPUT).await.is_empty(), "it ran");
    assert!(
        t.felix
            .cache_get(names::EPOCHS, ID)
            .await
            .unwrap()
            .is_none(),
        "a node claimed it"
    );
    assert_eq!(t.spec().await.state, WorkloadState::Running);

    t.put_spec(&spec).await;
    eventually("the outputs", Duration::from_secs(60), || async {
        (t.outputs(OUTPUT).await.len() >= 3).then_some(())
    })
    .await;
}

/// The record lands while a node that already holds the lease is frozen
/// just before its claim append. Its claim is refused, and it finds the
/// record past the tail and stops instead of claiming after it.
#[tokio::test(flavor = "multi_thread")]
async fn an_owner_finds_the_record_past_the_tail_and_stops() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    let record = t.spec().await.record.unwrap();
    let frozen = t.start_node_with_ttl("frozen", "freeze-before-claim=1", Duration::from_secs(30));
    eventually("the freeze", Duration::from_secs(60), || async {
        frozen.stopped().then_some(())
    })
    .await;
    let (lease, _) = t.lease(ID).await.expect("the frozen node's lease");
    t.publish_if(OUTPUT, vec![needs_operator(lease.epoch - 1, record)], 0)
        .await;
    frozen.resume();
    t.publish_all(INPUT, (0..3).map(count)).await;
    eventually(
        "the lease to be released",
        Duration::from_secs(30),
        || async { t.lease(ID).await.is_none().then_some(()) },
    )
    .await;
    tokio::time::sleep(LEASE_TTL).await;
    assert!(t.outputs(OUTPUT).await.is_empty(), "it ran");
    for (_, found) in t.read_all(OUTPUT).await {
        assert!(
            !matches!(output::decode(&found), Ok(Record::Claim { .. })),
            "it claimed after the record"
        );
    }
    assert_eq!(t.spec().await.state, WorkloadState::Running);
}
