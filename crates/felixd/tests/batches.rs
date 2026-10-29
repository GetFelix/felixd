//! A multi-record `publish_if` is not all or nothing: a `Quorum` leader that
//! loses its shard can leave a prefix of the batch on the stream while the
//! writer gets an error. The `append-prefix` fault writes such a prefix.
//!
//! Needs a Felix broker; see `common`. The `cluster` CI job runs it against
//! three brokers too.

mod common;

use std::time::Duration;

use common::{INPUT, OUTPUT, assert_uninterrupted, eventually, test_felix};
use felixd_core::output;

/// Four inputs. The third outputs four records in one batch, and is the
/// third append.
fn inputs() -> Vec<Vec<u8>> {
    vec![
        b"one".to_vec(),
        b"two".to_vec(),
        b"split:abcd".to_vec(),
        b"three".to_vec(),
    ]
}

/// The epochs of the split input's outputs, in stream order.
async fn split_epochs(t: &common::TestFelix) -> Vec<u64> {
    let split = t.read_all(INPUT).await[2].0;
    t.outputs(OUTPUT)
        .await
        .iter()
        .filter_map(|(_, r)| {
            let record = output::decode(r).unwrap();
            let epoch = record.epoch();
            let (written, _) = record.output()?;
            (written.header.input_offset == split).then_some(epoch)
        })
        .collect()
}

/// The owner gets an error after half the batch landed. It reads the stream
/// at the tail it expected, finds its own records there, and writes only
/// the rest.
#[tokio::test(flavor = "multi_thread")]
async fn the_owner_skips_a_prefix_of_its_own_batch() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("faulty", &[]).await;
    t.publish_all(INPUT, inputs()).await;
    let mut agent = t.start_agent_with_faults("append-prefix=3");
    assert_uninterrupted(&t, &module, 4).await;
    assert!(agent.alive(), "the owner gave up on a partial batch");
    let epochs = split_epochs(&t).await;
    assert_eq!(epochs.len(), 4);
    assert!(epochs.iter().all(|&e| e == epochs[0]), "{epochs:?}");
}

/// The owner dies after half the batch landed. The next owner resumes in
/// the middle of that step: it runs the step, drops the outputs already on
/// the stream and writes the rest.
#[tokio::test(flavor = "multi_thread")]
async fn the_next_owner_resumes_in_the_middle_of_a_step() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("faulty", &[]).await;
    t.publish_all(INPUT, inputs()).await;
    let mut first = t.start_agent_with_faults("append-prefix=3,crash-after-append-sent=3");
    first.exited(Duration::from_secs(60)).await;
    let prefix = split_epochs(&t).await;
    assert_eq!(prefix.len(), 2, "the crash left {prefix:?}");

    let _next = t.start_agent();
    eventually(
        "the next owner's outputs",
        Duration::from_secs(60),
        || async { (t.outputs(OUTPUT).await.len() >= 7).then_some(()) },
    )
    .await;
    assert_uninterrupted(&t, &module, 4).await;
    let epochs = split_epochs(&t).await;
    assert_eq!(epochs[..2], prefix[..]);
    assert!(epochs[2] > prefix[1], "{epochs:?}");
    assert_eq!(epochs[2], epochs[3]);
}
