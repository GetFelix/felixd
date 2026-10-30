//! A workload that needs an operator is recorded once on its output stream,
//! and every node leaves it alone until its spec changes.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::time::Duration;

use common::{ID, INPUT, LEASE_TTL, OUTPUT, TestFelix, eventually, test_felix};
use felixd_agent::admin::{StreamKind, ensure_stream};
use felixd_core::class::{ErrorClass, OperatorReason};
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

/// Workload `other`'s spec record, put in [`ID`]'s `workloads` entry, so
/// that every node finds a problem to record before it claims.
async fn unrecorded_spec(t: &TestFelix) {
    t.submit_as("other", "counter", &[]).await;
    let other = t.spec_of("other").await.record;
    t.submit("counter", &[]).await;
    let mut spec = t.spec().await;
    spec.record = other;
    t.put_cache_spec(&spec).await;
}

/// The epoch of the last record on the output stream, and its tail.
async fn last_epoch(t: &TestFelix) -> (u64, u64) {
    let records = t.read_all(OUTPUT).await;
    let epoch = records
        .last()
        .map_or(0, |(_, r)| output::decode(r).unwrap().epoch());
    (epoch, t.felix.tail(OUTPUT).await.unwrap())
}

/// The first record is a needs-operator record for r1, written before any
/// claim. r2 then runs. r3 changes the input, which r2's output was not
/// written from, so the owner refuses it: the history starts at r2's claim,
/// not at the record.
#[tokio::test(flavor = "multi_thread")]
async fn a_record_before_the_first_claim_does_not_hide_the_history() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    let spec = t.spec().await;
    t.publish_if(OUTPUT, vec![needs_operator(0, spec.record.unwrap())], 0)
        .await;
    t.put_spec(&spec).await;
    let _agent = t.start_agent();
    t.publish_all(INPUT, (0..3).map(count)).await;
    eventually("the outputs", Duration::from_secs(60), || async {
        (t.outputs(OUTPUT).await.len() >= 3).then_some(())
    })
    .await;

    let moved = "in-2";
    let kind = StreamKind {
        replication_factor: common::replication_factor(),
    };
    ensure_stream(&t.settings, moved, kind).await.unwrap();
    let mut changed = t.spec().await;
    changed.input = moved.into();
    t.put_spec(&changed).await;
    t.publish_all(moved, (0..5).map(count)).await;
    assert_eq!(
        t.needs_operator_recorded().await,
        OperatorReason::SpecConflict
    );
    assert_eq!(t.outputs(OUTPUT).await.len(), 3, "the changed input ran");
}

/// r0 ran, and a node recorded that r2 needs an operator without claiming.
/// A node that still sees r0 must refuse instead of claiming and running r0
/// over the record.
#[tokio::test(flavor = "multi_thread")]
async fn a_stale_node_does_not_claim_over_a_later_specs_record() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    let mut first = t.start_agent();
    t.publish_all(INPUT, (0..3).map(count)).await;
    eventually("the outputs", Duration::from_secs(60), || async {
        (t.outputs(OUTPUT).await.len() >= 3).then_some(())
    })
    .await;
    first.kill();
    let stale = t.spec().await;
    t.put_spec(&stale).await;
    let later = t.spec().await.record.unwrap();
    let (epoch, tail) = last_epoch(&t).await;
    t.publish_if(OUTPUT, vec![needs_operator(epoch, later)], tail)
        .await;
    t.put_cache_spec(&stale).await;

    let _second = t.start_agent();
    t.publish_all(INPUT, (3..6).map(count)).await;
    tokio::time::sleep(LEASE_TTL * 4).await;
    assert_eq!(t.outputs(OUTPUT).await.len(), 3, "the stale spec ran");
    let after: Vec<_> = t
        .read_all(OUTPUT)
        .await
        .into_iter()
        .filter(|&(offset, _)| offset > tail)
        .collect();
    assert!(after.is_empty(), "something was written after the record");
}

/// A node with a problem to record still checks that its spec is not
/// stale: a later spec record has been claimed, so it records nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_stale_node_with_a_problem_does_not_record_it() {
    let Some(t) = test_felix().await else { return };
    t.submit_as("other", "counter", &[]).await;
    let older = t.spec_of("other").await.record;
    t.submit("counter", &[]).await;
    let mut spec = t.spec().await;
    let newer = spec.record.unwrap();
    assert!(older < Some(newer));
    t.publish_if(
        OUTPUT,
        vec![output::encode_claim_under(0, newer, newer, "newer")],
        0,
    )
    .await;
    spec.record = older;
    t.put_cache_spec(&spec).await;
    let _agent = t.start_agent();
    tokio::time::sleep(LEASE_TTL * 3).await;
    assert_eq!(t.needs_operator().await.0, [], "the stale node recorded");
}

/// The holder is frozen before it confirms its lease to record that the
/// workload needs an operator, and its lease ends meanwhile. Its local
/// deadline, from a long TTL, still says it holds. The renewal right before
/// the append is refused, so it writes nothing at the old epoch.
#[tokio::test(flavor = "multi_thread")]
async fn a_holder_renews_before_it_records_that_a_workload_needs_an_operator() {
    let Some(t) = test_felix().await else { return };
    unrecorded_spec(&t).await;
    let frozen = t.start_node_with_ttl(
        "holder",
        "freeze-before-failure-record=1",
        Duration::from_secs(30),
    );
    eventually("the freeze", Duration::from_secs(60), || async {
        frozen.stopped().then_some(())
    })
    .await;
    let (stale, version) = t.lease(ID).await.expect("the frozen node's lease");
    t.felix
        .cache_delete_if(names::LEASES, ID, version)
        .await
        .unwrap();
    frozen.resume();
    t.needs_operator_recorded().await;
    for (_, found) in t.read_all(OUTPUT).await {
        assert_ne!(
            output::decode(&found).unwrap().epoch(),
            stale.epoch,
            "it recorded at the epoch it no longer held"
        );
    }
}

/// A failure under the current spec version is last on the stream when a
/// holder comes to record that the workload needs an operator. It writes
/// `failed` into the spec instead, as a failure found anywhere else does.
#[tokio::test(flavor = "multi_thread")]
async fn a_failure_found_while_recording_fails_the_spec() {
    let Some(t) = test_felix().await else { return };
    unrecorded_spec(&t).await;
    let version = t.spec_version().await;
    let failure = output::encode_failure(0, version, ErrorClass::Poison, "from the test");
    t.publish_if(OUTPUT, vec![failure], 0).await;
    let _agent = t.start_agent();
    let found = t.failure().await;
    assert!(found.contains("from the test"), "{found}");
    assert_eq!(t.needs_operator().await.0, []);
}

/// The downstream's output says it stepped inputs past the end of its input
/// stream, which is what an upstream stream deleted and created again looks
/// like from the downstream. Felix 0.6.2 leaves a recreated stream's shard
/// unplaced in the single-broker test setup, so the test writes that output
/// instead of recreating the stream. The node needs an operator rather than
/// waiting for the new stream to reach offsets the old one held.
#[tokio::test(flavor = "multi_thread")]
async fn an_input_that_ends_below_what_was_stepped_needs_an_operator() {
    let Some(t) = test_felix().await else { return };
    let up = "wl.up.out";
    let kind = StreamKind {
        replication_factor: common::replication_factor(),
    };
    ensure_stream(&t.settings, up, kind).await.unwrap();
    let records: Vec<Vec<u8>> = (0..4)
        .map(|i| output::encode(header(i), 1, &count(i)))
        .collect();
    t.publish_if(up, records, 0).await;
    let module = felixd_testkit::workload("counter");
    assert!(t.run(ID, &module, up, &[]).success());
    let mut agent = t.start_agent();
    eventually("the outputs", Duration::from_secs(60), || async {
        (t.outputs(OUTPUT).await.len() >= 4).then_some(())
    })
    .await;
    agent.kill();

    let (epoch, tail) = last_epoch(&t).await;
    t.publish_if(OUTPUT, vec![output::encode(header(100), epoch, b"")], tail)
        .await;
    let _agent = t.start_agent();
    assert_eq!(
        t.needs_operator_recorded().await,
        OperatorReason::InputBehind
    );
    assert_eq!(t.outputs(OUTPUT).await.len(), 5);
}

/// As above, but the input only looks behind once the subscription goes
/// below the workload's position, as a stream that started over replays
/// offsets already stepped. The fault skips the check on resume.
#[tokio::test(flavor = "multi_thread")]
async fn a_subscription_below_the_position_into_a_short_input_needs_an_operator() {
    let Some(t) = test_felix().await else { return };
    let up = "wl.up.out";
    let kind = StreamKind {
        replication_factor: common::replication_factor(),
    };
    ensure_stream(&t.settings, up, kind).await.unwrap();
    let records: Vec<Vec<u8>> = (0..4)
        .map(|i| output::encode(header(i), 1, &count(i)))
        .collect();
    t.publish_if(up, records, 0).await;
    let module = felixd_testkit::workload("counter");
    assert!(t.run(ID, &module, up, &[]).success());
    let mut agent = t.start_agent();
    eventually("the outputs", Duration::from_secs(60), || async {
        (t.outputs(OUTPUT).await.len() >= 4).then_some(())
    })
    .await;
    agent.kill();

    let (epoch, tail) = last_epoch(&t).await;
    t.publish_if(OUTPUT, vec![output::encode(header(100), epoch, b"")], tail)
        .await;
    let _agent = t.start_agent_with_faults("subscribe-from=0");
    assert_eq!(
        t.needs_operator_recorded().await,
        OperatorReason::InputBehind
    );
    assert_eq!(t.outputs(OUTPUT).await.len(), 5);
}

/// A subscription that goes below the position into an input that still
/// reaches it passes over what it replays, so nothing is stepped twice.
#[tokio::test(flavor = "multi_thread")]
async fn a_subscription_below_the_position_is_passed_over_while_the_input_reaches_it() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..4).map(count)).await;
    let mut agent = t.start_agent();
    eventually("the outputs", Duration::from_secs(60), || async {
        (t.outputs(OUTPUT).await.len() >= 4).then_some(())
    })
    .await;
    agent.kill();

    let _agent = t.start_agent_with_faults("subscribe-from=0");
    t.publish_all(INPUT, (4..6).map(count)).await;
    eventually("the outputs", Duration::from_secs(60), || async {
        (t.outputs(OUTPUT).await.len() >= 6).then_some(())
    })
    .await;
    tokio::time::sleep(LEASE_TTL).await;
    common::assert_uninterrupted(&t, &module, 6).await;
    assert_eq!(t.needs_operator().await.0, []);
}

/// A needs-operator record with a reason this build does not know, as a
/// newer build writes, still parks the spec record it names.
#[tokio::test(flavor = "multi_thread")]
async fn a_record_with_an_unknown_reason_still_parks_the_workload() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..3).map(count)).await;
    let spec = t.spec().await;
    let mut record = needs_operator(0, spec.record.unwrap());
    record[18..20].copy_from_slice(&u16::MAX.to_be_bytes());
    t.publish_if(OUTPUT, vec![record], 0).await;
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
}

fn header(input_offset: u64) -> output::OutputHeader {
    output::OutputHeader {
        input_offset,
        index: 0,
    }
}
