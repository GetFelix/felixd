//! A `publish_if` whose answer never comes may or may not have landed. The
//! writer reads the stream at the tail it expected and decides on what it
//! finds: its own records landed, someone else's did, or nothing did. The
//! `drop-*` faults lose the request before it lands and the `lose-*-answer`
//! faults lose the answer after it lands, for outputs, claims and failure
//! records.
//!
//! Needs a Felix broker; see `common`. The `cluster` CI job runs it against
//! three brokers too.

mod common;

use std::time::Duration;

use common::{ID, INPUT, OUTPUT, assert_uninterrupted, eventually, test_felix};
use felixd_core::names;
use felixd_core::output::{self, Record};
use felixd_core::spec::WorkloadState;

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

/// How many claims and failure records the output stream holds.
async fn markers(t: &common::TestFelix) -> (usize, usize) {
    let records = t.read_all(OUTPUT).await;
    let decoded: Vec<Record<'_>> = records
        .iter()
        .map(|(_, r)| output::decode(r).unwrap())
        .collect();
    let claims = decoded
        .iter()
        .filter(|r| matches!(r, Record::Claim { .. }))
        .count();
    let failures = decoded
        .iter()
        .filter(|r| matches!(r, Record::Failure { .. }))
        .count();
    (claims, failures)
}

/// The owner runs ten inputs under `faults` and must write each output once.
async fn outputs_survive(faults: &str) {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..10).map(count)).await;
    let mut agent = t.start_agent_with_faults(faults);
    assert_uninterrupted(&t, &module, 10).await;
    assert!(agent.alive(), "the owner gave up");
    assert_eq!(markers(&t).await, (1, 0));
}

/// The fifth output never reaches Felix. The owner finds nothing at its
/// tail and writes it again.
#[tokio::test(flavor = "multi_thread")]
async fn an_output_dropped_before_it_lands_is_written_again() {
    outputs_survive("drop-append=5").await;
}

/// The fifth output lands and its answer is lost. The owner finds its own
/// record at its tail and goes on after it.
#[tokio::test(flavor = "multi_thread")]
async fn an_output_whose_answer_is_lost_is_not_written_again() {
    outputs_survive("lose-append-answer=5").await;
}

/// The claim never reaches Felix, so the owner claims again.
#[tokio::test(flavor = "multi_thread")]
async fn a_claim_dropped_before_it_lands_is_written_again() {
    outputs_survive("drop-claim=1").await;
}

/// The claim lands and its answer is lost. The owner finds its own claim and
/// does not write a second one.
#[tokio::test(flavor = "multi_thread")]
async fn a_claim_whose_answer_is_lost_is_not_written_again() {
    outputs_survive("lose-claim-answer=1").await;
}

/// The owner fails the workload under `faults` and must leave one failure
/// record, after which the spec says `failed`.
async fn failure_survives(faults: &str) {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..5).map(count)).await;
    let _agent = t.start_agent_with_faults(&format!("fail-workload=1,{faults}"));
    t.failure().await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let (_, failures) = markers(&t).await;
    assert_eq!(failures, 1);
    let last = t.read_all(OUTPUT).await.pop().unwrap().1;
    assert!(matches!(
        output::decode(&last).unwrap(),
        Record::Failure { .. }
    ));
    assert_eq!(t.spec().await.state, WorkloadState::Failed);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failure_record_dropped_before_it_lands_is_written_again() {
    failure_survives("drop-failure=1").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failure_record_whose_answer_is_lost_is_not_written_again() {
    failure_survives("lose-failure-answer=1").await;
}

/// The fifth output never reaches Felix and the owner is frozen before it
/// looks. Another node takes over and writes. The first has a long TTL and
/// its lease is ended early, so when it wakes it still believes it holds the
/// lease: only the later owner's records at its tail can stop it.
#[tokio::test(flavor = "multi_thread")]
async fn an_owner_that_lost_an_answer_finds_a_later_owner_and_stops() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..10).map(count)).await;
    let mut stale = t.start_node_with_ttl(
        "holder",
        "drop-append=5,freeze-after-append-sent=5",
        Duration::from_secs(30),
    );
    eventually("the freeze", Duration::from_secs(60), || async {
        stale.stopped().then_some(())
    })
    .await;
    let (frozen, version) = t.lease(ID).await.expect("the frozen node's lease");
    t.felix
        .cache_delete_if(names::LEASES, ID, version)
        .await
        .unwrap();

    let _next = t.start_agent();
    eventually(
        "the next owner's outputs",
        Duration::from_secs(60),
        || async { (t.outputs(OUTPUT).await.len() >= 10).then_some(()) },
    )
    .await;
    stale.resume();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_uninterrupted(&t, &module, 10).await;
    let epochs: Vec<u64> = t
        .outputs(OUTPUT)
        .await
        .iter()
        .map(|(_, r)| output::decode(r).unwrap().epoch())
        .collect();
    assert!(epochs[..4].iter().all(|&e| e == frozen.epoch), "{epochs:?}");
    assert!(epochs[4..].iter().all(|&e| e > frozen.epoch), "{epochs:?}");
    assert!(stale.alive(), "the stale node exited");
}
