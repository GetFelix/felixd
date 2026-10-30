//! A workload that keeps its last snapshots writes trim records, and a crash
//! with one in flight changes nothing a resume, `check` or `replay` reads.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::time::Duration;

use common::{Agent, ID, INPUT, OUTPUT, SNAPSHOTS, assert_uninterrupted, eventually, test_felix};
use felixd_core::names;
use felixd_core::output::{self, Record};
use felixd_core::snapshot;

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

/// The trims on the output stream, in order, with their epochs.
async fn trims(t: &common::TestFelix) -> Vec<(u64, felixd_core::trim::Trim)> {
    t.read_all(OUTPUT)
        .await
        .iter()
        .filter_map(|(_, r)| {
            let decoded = output::decode(r).unwrap();
            matches!(decoded, Record::Trim { .. })
                .then(|| (decoded.epoch(), decoded.trim().unwrap()))
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crash_with_a_trim_in_flight_resumes_and_checks() {
    let Some(t) = test_felix().await else { return };
    let module = t
        .submit(
            "counter",
            &["--snapshot-every", "10", "--keep-snapshots", "2"],
        )
        .await;
    t.publish_all(INPUT, (0..60).map(count)).await;
    // The second trim follows the third snapshot, at input 29.
    let mut agent = t.start_agent_with_faults("crash-after-trim-sent=2");
    assert!(!agent.exited(Duration::from_secs(60)).await.success());

    let mut agent = t.start_agent();
    t.publish_all(INPUT, (60..90).map(count)).await;
    assert_uninterrupted(&t, &module, 90).await;
    agent.kill();

    let found = trims(&t).await;
    assert!(found.len() >= 3, "{found:?}");
    let snapshots = t.read_all(SNAPSHOTS).await;
    for pair in found.windows(2) {
        let (before, after) = (&pair[0].1, &pair[1].1);
        assert!(after.step >= before.step, "{found:?}");
        assert!(after.snap_floor >= before.snap_floor, "{found:?}");
    }
    for (_, trim) in &found {
        // The record names a snapshot of the step it says, and keeps the
        // input it stepped last.
        let (_, record) = snapshots
            .iter()
            .find(|(offset, _)| *offset == trim.snapshot)
            .expect("the trim names a snapshot record");
        let taken = snapshot::decode(record).unwrap().taken().unwrap();
        assert_eq!(taken.header.input_offset, trim.step);
        assert_eq!(trim.input_floors, vec![trim.step]);
        assert!(trim.snap_floor <= trim.snapshot);
    }
    let (ok, report) = t.check(&["--id", ID, "--complete"]);
    assert!(ok, "{report}");
}

/// A holder is frozen just before its first trim record, after its own
/// snapshot, and its lease ends. The next holder claims, snapshots and
/// trims. When the first wakes, its own lease check passes, so only the
/// `.out` fence can stop its trim: it must never land.
#[tokio::test(flavor = "multi_thread")]
async fn a_stale_holders_trim_is_refused_by_the_claim() {
    let Some(t) = test_felix().await else { return };
    let module = t
        .submit(
            "counter",
            &["--snapshot-every", "5", "--keep-snapshots", "1"],
        )
        .await;
    t.publish_all(INPUT, (0..8).map(count)).await;
    let node = t.start_node_with_ttl("holder", "freeze-before-trim=1", Duration::from_secs(30));
    eventually("the freeze", Duration::from_secs(60), || async {
        node.stopped().then_some(())
    })
    .await;
    let (held, version) = t.lease(ID).await.expect("the frozen node's lease");
    assert_eq!(held.owner, "holder");
    let stale = held.epoch;
    t.felix
        .cache_delete_if(names::LEASES, ID, version)
        .await
        .unwrap();
    assert!(trims(&t).await.is_empty());

    let mut next = t.start_agent();
    t.publish_all(INPUT, (8..20).map(count)).await;
    eventually(
        "the next holder's trim",
        Duration::from_secs(60),
        || async {
            trims(&t)
                .await
                .iter()
                .any(|(epoch, _)| *epoch > stale)
                .then_some(())
        },
    )
    .await;
    let mut frozen: Agent = node;
    frozen.resume();
    tokio::time::sleep(Duration::from_secs(3)).await;
    t.publish_all(INPUT, (20..25).map(count)).await;
    assert_uninterrupted(&t, &module, 25).await;
    next.kill();

    let found = trims(&t).await;
    assert!(
        found.iter().all(|(epoch, _)| *epoch > stale),
        "a stale holder's trim landed: {found:?}"
    );
    assert!(frozen.alive(), "the frozen node exited");
}

/// Whether a needs-operator record is on the output stream.
async fn parked(t: &common::TestFelix) -> bool {
    t.read_all(OUTPUT)
        .await
        .iter()
        .any(|(_, r)| matches!(output::decode(r).unwrap(), Record::NeedsOperator { .. }))
}

/// A resume after a trim reads nothing below its floors, the last input's
/// append time for `snapshot_every_ms` included: with every read below them
/// refused, as once they are deleted, the next holder carries on.
#[tokio::test(flavor = "multi_thread")]
async fn a_resume_reads_nothing_below_the_trim_floors() {
    let Some(t) = test_felix().await else { return };
    let module = t
        .submit(
            "counter",
            &[
                "--snapshot-every",
                "5",
                "--snapshot-every-ms",
                "3600000",
                "--keep-snapshots",
                "1",
            ],
        )
        .await;
    t.publish_all(INPUT, (0..22).map(count)).await;
    let mut first = t.start_agent();
    eventually("a trim past the start", Duration::from_secs(60), || async {
        trims(&t)
            .await
            .iter()
            .any(|(_, trim)| trim.step >= 14)
            .then_some(())
    })
    .await;
    first.kill();

    let mut next = t.start_agent_with_faults("enforce-trim-floors=1");
    t.publish_all(INPUT, (22..30).map(count)).await;
    assert_uninterrupted(&t, &module, 30).await;
    // A read below a floor that reached the run would have aborted it.
    assert!(next.alive(), "a read below the floors reached the run");
    next.kill();
    assert!(!parked(&t).await, "the resume read below the floors");
}

/// Floors never go back across owners. `keep_snapshots` is raised from 1 to
/// 5 while no node runs the workload; the next owner's last five snapshots
/// reach further back than the last trim, so it keeps that trim rather than
/// writing lower floors.
#[tokio::test(flavor = "multi_thread")]
async fn raising_keep_snapshots_never_lowers_a_floor() {
    let Some(t) = test_felix().await else { return };
    let args = |keep: &'static str| ["--snapshot-every", "5", "--keep-snapshots", keep];
    let module = t.submit("counter", &args("1")).await;
    t.publish_all(INPUT, (0..30).map(count)).await;
    let mut first = t.start_agent();
    eventually(
        "a trim at the last snapshot",
        Duration::from_secs(60),
        || async {
            trims(&t)
                .await
                .iter()
                .any(|(_, trim)| trim.step >= 29)
                .then_some(())
        },
    )
    .await;
    first.kill();
    assert!(t.run(ID, &module, INPUT, &args("5")).success());

    let mut next = t.start_agent();
    t.publish_all(INPUT, (30..45).map(count)).await;
    assert_uninterrupted(&t, &module, 45).await;
    next.kill();
    let found = trims(&t).await;
    for pair in found.windows(2) {
        let (before, after) = (&pair[0].1, &pair[1].1);
        assert!(
            after.step >= before.step
                && after.snap_floor >= before.snap_floor
                && after.input_floors >= before.input_floors,
            "a floor went back: {found:?}"
        );
    }
}
