//! `keep_snapshots` deletes: once a trim record is on `.out`, Felix no longer
//! serves `.snap` (and the merge log, see `merge.rs`) below its floors. A
//! failover after that still produces every output exactly once, and a crash
//! between the record and the deletion is finished by the next holder.
//!
//! The tests from `shared_streams` on also let the agents delete `.out` and
//! the inputs, each only below the lowest floor of every workload that reads
//! it. The others leave those whole, with `skip-shared-deletion`, which
//! `common` adds to every agent.
//!
//! Needs a Felix broker that trims, see `common`. A cluster must have
//! finalized `head_trim`.

mod common;

use std::time::Duration;

use common::{
    ID, INPUT, OUTPUT, SNAPSHOTS, TestFelix, assert_uninterrupted, eventually, test_felix,
};
use felixd_core::class::OperatorReason;
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
    // The last input's offset: a cluster's inputs start after a leader marker.
    let last = t.felix.tail(INPUT).await.unwrap() - 1;
    let (ok, replayed) = t.replay(&["--id", ID, "--to", &last.to_string()]);
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
    // On a cluster the stream's head is a leader marker, so it starts at 1.
    assert!(
        t.readable_from(SNAPSHOTS).await < floor,
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
    t.read_readable(OUTPUT)
        .await
        .iter()
        .any(|(_, r)| matches!(output::decode(r).unwrap(), Record::NeedsOperator { .. }))
}

// Shared streams.

/// Lets an agent delete `.out` and the inputs, which `common` otherwise turns
/// off. The first of two entries for a fault wins.
const DELETE_SHARED: &str = "skip-shared-deletion=0";

/// The trim records on `output` that Felix still serves, in order.
async fn trims_on(t: &TestFelix, output: &str) -> Vec<Trim> {
    t.read_readable(output)
        .await
        .iter()
        .filter_map(|(_, r)| output::decode(r).unwrap().trim())
        .collect()
}

/// The input floor of the last trim on `output`, if there is a trim.
async fn input_floor(t: &TestFelix, output: &str) -> Option<u64> {
    Some(trims_on(t, output).await.last()?.input_floors[0])
}

/// The totals the counter workload wrote to `output` that Felix still serves.
async fn counts(t: &TestFelix, output: &str) -> Vec<u64> {
    t.read_readable(output)
        .await
        .iter()
        .filter_map(|(_, r)| match output::decode(r).unwrap() {
            Record::Output(_, addressed) => Some(u64::from_be_bytes(
                addressed.payload.try_into().expect("a counter total"),
            )),
            _ => None,
        })
        .collect()
}

/// Whether the counter's last total on `output` is `total`.
async fn stepped(t: &TestFelix, output: &str, total: u64) -> bool {
    counts(t, output).await.last() == Some(&total)
}

/// Where a resume or a check from the snapshot `trim` names reads `output`
/// from: that snapshot's output tail, and the farthest a workload's own
/// `.out` goes.
async fn output_need(t: &TestFelix, snapshots: &str, trim: &Trim) -> u64 {
    let found = t
        .felix
        .read_range(snapshots, trim.snapshot, trim.snapshot + 1)
        .await
        .unwrap();
    snapshot::decode(&found[0].payload)
        .unwrap()
        .taken()
        .expect("the trim names a snapshot")
        .header
        .output_tail
}

/// The reason a needs-operator record on `output` gives.
async fn parked_for(t: &TestFelix, output: &str) -> Option<OperatorReason> {
    t.read_all(output)
        .await
        .iter()
        .find_map(|(_, r)| match output::decode(r).unwrap() {
            Record::NeedsOperator { reason, .. } => Some(reason),
            _ => None,
        })
}

/// Two workloads read one input. The one that snapshots more often has the
/// higher floor, and deleting on it would take records the other still
/// resumes from, so the input goes only as far as the lower floor.
#[tokio::test(flavor = "multi_thread")]
async fn a_shared_input_is_deleted_only_below_the_lowest_floor() {
    let Some(t) = test_felix().await else { return };
    t.submit_as(
        "fast",
        "counter",
        &["--snapshot-every", "5", "--keep-snapshots", "1"],
    )
    .await;
    t.submit_as(
        "slow",
        "counter",
        &["--snapshot-every", "20", "--keep-snapshots", "1"],
    )
    .await;
    let _agent = t.start_agent_with_faults(DELETE_SHARED);
    t.publish_all(INPUT, (0..30).map(count)).await;
    let (high, low) = eventually(
        "the slow workload to trim and the fast one to pass it",
        Duration::from_secs(90),
        || async {
            let low = input_floor(&t, "wl.slow.out").await?;
            let high = input_floor(&t, "wl.fast.out").await?;
            (low > 0 && high > low).then_some((high, low))
        },
    )
    .await;
    eventually(
        "both to step every input",
        Duration::from_secs(60),
        || async {
            (stepped(&t, "wl.fast.out", 30).await && stepped(&t, "wl.slow.out", 30).await)
                .then_some(())
        },
    )
    .await;

    // The fast workload snapshots again and finds the slow one's record.
    t.publish_all(INPUT, (30..35).map(count)).await;
    eventually(
        "the fast workload to trim again",
        Duration::from_secs(60),
        || async { (input_floor(&t, "wl.fast.out").await? > high).then_some(()) },
    )
    .await;
    eventually(
        "the input to be deleted to the lower floor",
        Duration::from_secs(60),
        || async { (t.readable_from(INPUT).await >= low).then_some(()) },
    )
    .await;
    let kept_from = t.readable_from(INPUT).await;
    assert!(
        kept_from <= low,
        "the input is deleted to {kept_from}, past the slow workload's floor {low}"
    );
    for id in ["fast", "slow"] {
        let (ok, report) = t.check(&["--id", id, "--complete", "--from-snapshot"]);
        assert!(ok, "{id}:\n{report}");
    }
}

/// A workload with no `keep_snapshots` has no trim record, so nothing shows
/// how far it has read, and the input stays whole for it.
#[tokio::test(flavor = "multi_thread")]
async fn a_reader_with_no_keep_snapshots_blocks_deleting_the_input() {
    let Some(t) = test_felix().await else { return };
    t.submit_as(
        "kept",
        "counter",
        &["--snapshot-every", "5", "--keep-snapshots", "1"],
    )
    .await;
    t.submit_as("plain", "counter", &[]).await;
    let _agent = t.start_agent_with_faults(DELETE_SHARED);
    t.publish_all(INPUT, (0..30).map(count)).await;
    // The workload that keeps snapshots deletes its own `.snap` in the same
    // pass that would delete the input, so the input is not just untried.
    eventually(
        "the snapshot stream trimmed",
        Duration::from_secs(90),
        || async { (t.readable_from("wl.kept.snap").await > 0).then_some(()) },
    )
    .await;
    let floor = input_floor(&t, "wl.kept.out").await.unwrap();
    t.publish_all(INPUT, (30..40).map(count)).await;
    eventually("another trim", Duration::from_secs(60), || async {
        (input_floor(&t, "wl.kept.out").await? > floor).then_some(())
    })
    .await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(t.readable_from(INPUT).await, 0, "the input was deleted");
}

/// A pipeline stage's `.out` is the next stage's input. Its holder deletes it
/// only below what the next stage still reads, though its own snapshots
/// would let it go further.
#[tokio::test(flavor = "multi_thread")]
async fn a_pipeline_stage_is_deleted_only_below_its_readers_floor() {
    let Some(t) = test_felix().await else { return };
    let module = t
        .submit_as(
            "up",
            "counter",
            &["--snapshot-every", "5", "--keep-snapshots", "1"],
        )
        .await;
    assert!(
        t.run(
            "down",
            &module,
            "wl.up.out",
            &["--snapshot-every", "20", "--keep-snapshots", "1"],
        )
        .success()
    );
    let _agent = t.start_agent_with_faults(DELETE_SHARED);
    t.publish_all(INPUT, (0..30).map(count)).await;
    let floor = eventually(
        "the downstream stage to trim",
        Duration::from_secs(90),
        || async { input_floor(&t, "wl.down.out").await.filter(|&f| f > 0) },
    )
    .await;
    eventually(
        "the downstream stage to step every output",
        Duration::from_secs(60),
        || async { stepped(&t, "wl.down.out", 30).await.then_some(()) },
    )
    .await;

    // The upstream stage snapshots again and finds the downstream's record.
    t.publish_all(INPUT, (30..35).map(count)).await;
    eventually(
        "the output to be deleted to the reader's floor",
        Duration::from_secs(60),
        || async { (t.readable_from("wl.up.out").await >= floor).then_some(()) },
    )
    .await;
    let kept_from = t.readable_from("wl.up.out").await;
    assert!(
        kept_from <= floor,
        "wl.up.out is deleted to {kept_from}, past the downstream's floor {floor}"
    );
    let up = trims_on(&t, "wl.up.out").await;
    let need = output_need(&t, "wl.up.snap", up.last().unwrap()).await;
    assert!(
        floor < need,
        "the downstream's floor {floor} should be what holds the output back, not its own need {need}"
    );
    let (ok, report) = t.check(&["--id", "up", "--complete", "--from-snapshot"]);
    assert!(ok, "{report}");
    assert!(parked_for(&t, "wl.down.out").await.is_none());
}

/// A workload with no downstream reader deletes its own `.out` as far as its
/// oldest kept snapshot needs it. The holder dies right after the second trim
/// record lands. The next holder finishes the deletion of `.snap`, `.out` and
/// the input from that record alone, and goes on to write every output once.
#[tokio::test(flavor = "multi_thread")]
async fn a_failover_after_the_trim_landed_finishes_deleting_out_and_the_input() {
    let Some(t) = test_felix().await else { return };
    t.submit(
        "counter",
        &["--snapshot-every", "5", "--keep-snapshots", "1"],
    )
    .await;
    t.publish_all(INPUT, (0..10).map(count)).await;
    let faults = format!("crash-after-trim-sent=2,{DELETE_SHARED}");
    let mut first = t.start_agent_with_faults(&faults);
    assert!(!first.exited(Duration::from_secs(60)).await.success());

    let found = trims_on(&t, OUTPUT).await;
    let second = found.last().expect("the second trim landed").clone();
    assert!(found.len() >= 2, "{found:?}");
    let need = output_need(&t, SNAPSHOTS, &second).await;
    assert!(
        t.readable_from(INPUT).await < second.input_floors[0],
        "the input was deleted to the second record's floor before the crash"
    );
    assert!(
        t.readable_from(OUTPUT).await < need,
        "`.out` was deleted to the second record's floor before the crash"
    );

    let _next = t.start_agent_with_faults(DELETE_SHARED);
    eventually(
        "the next holder to delete",
        Duration::from_secs(60),
        || async {
            let done = t.readable_from(SNAPSHOTS).await >= second.snap_floor
                && t.readable_from(INPUT).await >= second.input_floors[0]
                && t.readable_from(OUTPUT).await >= need;
            done.then_some(())
        },
    )
    .await;
    assert!(t.readable_from(INPUT).await <= second.input_floors[0]);
    assert!(t.readable_from(OUTPUT).await <= need);

    t.publish_all(INPUT, (10..25).map(count)).await;
    eventually("every output", Duration::from_secs(60), || async {
        stepped(&t, OUTPUT, 25).await.then_some(())
    })
    .await;
    let totals = counts(&t, OUTPUT).await;
    assert!(
        totals.windows(2).all(|w| w[1] == w[0] + 1),
        "outputs repeat or skip: {totals:?}"
    );
    let (ok, report) = t.check(&["--id", ID, "--complete", "--from-snapshot"]);
    assert!(ok, "{report}");
    assert!(!parked(&t).await);
}

/// With the head of `.out` deleted, `felixd replay --diff` compares from the
/// input after the oldest kept snapshot instead of failing on the missing
/// records, and has nothing to compare when asked to start from scratch.
#[tokio::test(flavor = "multi_thread")]
async fn a_diff_of_an_output_with_a_cut_head_starts_after_the_oldest_snapshot() {
    let Some(t) = test_felix().await else { return };
    t.submit(
        "counter",
        &["--snapshot-every", "5", "--keep-snapshots", "1"],
    )
    .await;
    let _agent = t.start_agent_with_faults(DELETE_SHARED);
    t.publish_all(INPUT, (0..30).map(count)).await;
    eventually(
        "`.out` to lose its head",
        Duration::from_secs(90),
        || async { (t.readable_from(OUTPUT).await > 0).then_some(()) },
    )
    .await;
    eventually("every output", Duration::from_secs(60), || async {
        stepped(&t, OUTPUT, 30).await.then_some(())
    })
    .await;
    let (ok, report) = t.replay(&["--id", ID, "--diff"]);
    assert!(ok, "{report}");
    assert!(report.contains("outputs match"), "{report}");
    let (ok, _) = t.replay(&["--id", ID, "--diff", "--from-start"]);
    assert!(!ok, "a diff from the start needs the deleted head");
}

/// A workload added after the input was deleted asks for records that are
/// gone. It is left to an operator rather than started in the middle.
#[tokio::test(flavor = "multi_thread")]
async fn a_workload_added_after_the_input_was_deleted_is_left_to_an_operator() {
    let Some(t) = test_felix().await else { return };
    let module = t
        .submit(
            "counter",
            &["--snapshot-every", "5", "--keep-snapshots", "1"],
        )
        .await;
    let _agent = t.start_agent_with_faults(DELETE_SHARED);
    t.publish_all(INPUT, (0..30).map(count)).await;
    eventually("the input deleted", Duration::from_secs(90), || async {
        (t.readable_from(INPUT).await > 0).then_some(())
    })
    .await;

    assert!(t.run("late", &module, INPUT, &[]).success());
    let reason = eventually(
        "the late workload to be parked",
        Duration::from_secs(90),
        || async { parked_for(&t, "wl.late.out").await },
    )
    .await;
    assert_eq!(reason, OperatorReason::HistoryTrimmed);
    assert!(!parked(&t).await, "the first workload was parked");
}
