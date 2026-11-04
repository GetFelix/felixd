//! A spec's restart intensity, counted from the claim records on its output
//! stream: past it the workload is recorded as needing an operator, and a new
//! spec record starts the count again.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::time::Duration;

use common::{INPUT, OUTPUT, eventually, test_felix};
use felixd_core::class::OperatorReason;
use felixd_core::output;

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

/// Two earlier holders claimed the spec record and wrote nothing. With one
/// takeover allowed, the next claim is the second in a row, so its holder
/// records the workload as needing an operator and runs nothing. A new spec
/// record runs.
#[tokio::test(flavor = "multi_thread")]
async fn a_claim_past_the_limit_is_recorded_and_a_new_spec_runs() {
    let Some(t) = test_felix().await else { return };
    let args = ["--max-takeovers", "1", "--takeover-window-ms", "600000"];
    t.submit("counter", &args).await;
    t.publish_all(INPUT, (0..3).map(count)).await;
    let spec = t.spec().await;
    let record = spec.record.unwrap();
    let claims = (1..=2)
        .map(|epoch| output::encode_claim_under(epoch, record, record, "gone"))
        .collect();
    t.publish_if(OUTPUT, claims, 0).await;
    let _agent = t.start_agent();

    assert_eq!(
        t.needs_operator_recorded().await,
        OperatorReason::RestartIntensity
    );
    assert!(t.outputs(OUTPUT).await.is_empty(), "it ran");

    t.put_spec(&spec).await;
    eventually("the outputs", Duration::from_secs(60), || async {
        (t.outputs(OUTPUT).await.len() >= 3).then_some(())
    })
    .await;
}

/// The same history with two takeovers allowed is within the limit.
#[tokio::test(flavor = "multi_thread")]
async fn a_claim_within_the_limit_runs() {
    let Some(t) = test_felix().await else { return };
    let args = ["--max-takeovers", "2", "--takeover-window-ms", "600000"];
    t.submit("counter", &args).await;
    t.publish_all(INPUT, (0..3).map(count)).await;
    let record = t.spec().await.record.unwrap();
    let claims = (1..=2)
        .map(|epoch| output::encode_claim_under(epoch, record, record, "gone"))
        .collect();
    t.publish_if(OUTPUT, claims, 0).await;
    let _agent = t.start_agent();

    eventually("the outputs", Duration::from_secs(60), || async {
        (t.outputs(OUTPUT).await.len() >= 3).then_some(())
    })
    .await;
    assert!(t.needs_operator().await.0.is_empty());
}
