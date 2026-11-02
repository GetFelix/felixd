//! `keep_snapshots` deletes: once a trim record is on `.out`, Felix no longer
//! serves `.snap` (and the merge log, see `merge.rs`) below its floors. A
//! failover after that still produces every output exactly once, and a crash
//! between the record and the deletion is finished by the next holder.
//!
//! Needs a Felix broker that trims, see `common`. A cluster must have
//! finalized `head_trim`.

mod common;

use std::time::Duration;

use common::{
    ID, INPUT, OUTPUT, SNAPSHOTS, TestFelix, assert_uninterrupted, eventually, test_felix,
};
use felixd_core::output::{self, Record};
use felixd_core::snapshot;
use felixd_core::trim::Trim;

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

/// The trims on the output stream, in order.
async fn trims(t: &TestFelix) -> Vec<Trim> {
    t.read_all(OUTPUT)
        .await
        .iter()
        .filter_map(|(_, r)| output::decode(r).unwrap().trim())
        .collect()
}

/// The snapshots Felix still serves from `.snap`, as their input offsets.
async fn kept(t: &TestFelix) -> Vec<u64> {
    let from = t.readable_from(SNAPSHOTS).await;
    let tail = t.felix.tail(SNAPSHOTS).await.unwrap();
    t.felix
        .read_range(SNAPSHOTS, from, tail)
        .await
        .unwrap()
        .iter()
        .filter_map(|r| snapshot::decode(&r.payload).unwrap().taken())
        .map(|taken| taken.header.input_offset)
        .collect()
}

/// A holder snapshots every five inputs and keeps two. Once Felix serves
/// `.snap` only from the oldest kept snapshot's floor, the holder is killed
/// and the next one resumes from what is left: every output is there once,
/// and `felixd check` and `felixd replay` work from the snapshots that remain.
#[tokio::test(flavor = "multi_thread")]
async fn a_failover_after_the_snapshot_stream_is_trimmed_loses_nothing() {
    let Some(t) = test_felix().await else { return };
    let module = t
        .submit(
            "counter",
            &["--snapshot-every", "5", "--keep-snapshots", "2"],
        )
        .await;
    t.publish_all(INPUT, (0..60).map(count)).await;
    let mut first = t.start_agent();
    let floor = eventually(
        "the snapshot stream trimmed",
        Duration::from_secs(60),
        || async {
            let floor = trims(&t).await.last()?.snap_floor;
            (floor > 0 && t.readable_from(SNAPSHOTS).await >= floor).then_some(floor)
        },
    )
    .await;
    first.kill();

    let _next = t.start_agent();
    t.publish_all(INPUT, (60..90).map(count)).await;
    assert_uninterrupted(&t, &module, 90).await;
    assert!(t.readable_from(SNAPSHOTS).await >= floor);
    let kept = kept(&t).await;
    assert!(kept.len() >= 2, "two snapshots are kept: {kept:?}");

    // Replay starts from a kept snapshot, so a recent offset still works.
    let (ok, replayed) = t.replay(&["--id", ID, "--to", "89"]);
    assert!(ok, "{replayed}");
    assert_eq!(replayed, "{\"count\":90}");
    assert!(!parked(&t).await, "the resume read deleted history");
}

/// The holder dies right after the second trim record lands, before it
/// deletes anything. No input follows, so no snapshot does either: only the
/// next holder, finishing what the record says on resume, can delete.
#[tokio::test(flavor = "multi_thread")]
async fn a_crash_between_the_trim_record_and_the_deletion_is_finished_by_the_next_holder() {
    let Some(t) = test_felix().await else { return };
    let module = t
        .submit(
            "counter",
            &["--snapshot-every", "5", "--keep-snapshots", "1"],
        )
        .await;
    t.publish_all(INPUT, (0..10).map(count)).await;
    let mut first = t.start_agent_with_faults("crash-after-trim-sent=2");
    assert!(!first.exited(Duration::from_secs(60)).await.success());

    let found = trims(&t).await;
    let floor = found.last().expect("the second trim landed").snap_floor;
    assert!(floor > 0, "{found:?}");
    assert_eq!(
        t.readable_from(SNAPSHOTS).await,
        0,
        "nothing is deleted before the crash"
    );

    let _next = t.start_agent();
    eventually(
        "the next holder to delete",
        Duration::from_secs(60),
        || async { (t.readable_from(SNAPSHOTS).await >= floor).then_some(()) },
    )
    .await;
    t.publish_all(INPUT, (10..25).map(count)).await;
    assert_uninterrupted(&t, &module, 25).await;
}

/// `felixd check` reads the snapshots Felix still has rather than stopping at
/// the first offset it no longer serves.
#[tokio::test(flavor = "multi_thread")]
async fn check_reads_the_snapshots_that_are_left() {
    let Some(t) = test_felix().await else { return };
    let module = t
        .submit(
            "counter",
            &["--snapshot-every", "5", "--keep-snapshots", "1"],
        )
        .await;
    t.publish_all(INPUT, (0..30).map(count)).await;
    let _agent = t.start_agent();
    eventually("a deletion", Duration::from_secs(60), || async {
        (t.readable_from(SNAPSHOTS).await > 0).then_some(())
    })
    .await;
    assert_uninterrupted(&t, &module, 30).await;
}

/// Whether a needs-operator record is on the output stream.
async fn parked(t: &TestFelix) -> bool {
    t.read_all(OUTPUT)
        .await
        .iter()
        .any(|(_, r)| matches!(output::decode(r).unwrap(), Record::NeedsOperator { .. }))
}
