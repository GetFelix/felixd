//! `felixd check` against real streams: a clean run passes, and a hand-made
//! duplicate, gap or stale epoch fails it.
//!
//! Needs a Felix broker; see `common`.

mod common;

use common::{ID, INPUT, OUTPUT, TestFelix, assert_uninterrupted, test_felix};
use felixd_core::output::{self, OutputHeader};

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

/// A workload whose output stream is written by hand, so no agent runs.
async fn by_hand(t: &TestFelix, records: impl IntoIterator<Item = Vec<u8>>) {
    t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..5).map(count)).await;
    t.publish_all(OUTPUT, records).await;
}

fn assert_finds(t: &TestFelix, what: &str) {
    let (ok, report) = t.check(&["--id", ID, "--complete"]);
    assert!(!ok, "check passed:\n{report}");
    assert!(report.contains(what), "{report}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_clean_run_passes() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &[]).await;
    let _agent = t.start_agent();
    t.publish_all(INPUT, (0..20).map(count)).await;
    // This runs `felixd check --complete` last.
    assert_uninterrupted(&t, &module, 20).await;
    let (ok, report) = t.check(&[]);
    assert!(ok, "{report}");
    assert!(report.contains("wl.wl.out: 20 records, ok"), "{report}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_duplicate_fails_the_check() {
    let Some(t) = test_felix().await else { return };
    let mut records: Vec<_> = (0..5).map(|i| counted(i, 1)).collect();
    records.insert(3, counted(2, 1));
    by_hand(&t, records).await;
    assert_finds(&t, "duplicate at offset 3: input 2 output 0");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_gap_fails_the_check() {
    let Some(t) = test_felix().await else { return };
    by_hand(&t, [0, 1, 3, 4].map(|i| counted(i, 1))).await;
    assert_finds(&t, "gap: input 2 output 0 is missing");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lower_epoch_after_a_higher_one_fails_the_check() {
    let Some(t) = test_felix().await else { return };
    by_hand(
        &t,
        [(0, 2), (1, 2), (2, 1), (3, 2), (4, 2)].map(|(i, e)| counted(i, e)),
    )
    .await;
    assert_finds(&t, "epoch regression at offset 2: epoch 1 after epoch 2");
}

#[tokio::test(flavor = "multi_thread")]
async fn outputs_out_of_order_fail_the_check() {
    let Some(t) = test_felix().await else { return };
    by_hand(&t, [0, 2, 1, 3, 4].map(|i| counted(i, 1))).await;
    assert_finds(&t, "out of order at offset 2: input 1 output 0");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stream_that_stops_short_fails_only_a_complete_check() {
    let Some(t) = test_felix().await else { return };
    by_hand(&t, (0..3).map(|i| counted(i, 1))).await;
    let (ok, report) = t.check(&["--id", ID]);
    assert!(ok, "{report}");
    assert_finds(&t, "behind: 2 outputs not written");
}
