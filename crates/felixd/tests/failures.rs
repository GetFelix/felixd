//! States no node can get past are recorded as a failed workload, not retried
//! forever.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::time::Duration;

use common::{INPUT, OUTPUT, SNAPSHOTS, eventually, test_felix};
use felixd_core::output::{self, OutputHeader};

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

fn output_record(input_offset: u64, payload: &[u8]) -> Vec<u8> {
    output::encode(
        OutputHeader {
            input_offset,
            index: 0,
        },
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
        (t.felix.tail(OUTPUT).await.unwrap() >= 5).then_some(())
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
