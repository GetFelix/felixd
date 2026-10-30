//! A workload that keeps its last snapshots writes trim records, and a crash
//! with one in flight changes nothing a resume, `check` or `replay` reads.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::time::Duration;

use common::{ID, INPUT, OUTPUT, SNAPSHOTS, assert_uninterrupted, test_felix};
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
        // The record names a snapshot of the step it says, and its input
        // floor is the next input.
        let (_, record) = snapshots
            .iter()
            .find(|(offset, _)| *offset == trim.snapshot)
            .expect("the trim names a snapshot record");
        let taken = snapshot::decode(record).unwrap().taken().unwrap();
        assert_eq!(taken.header.input_offset, trim.step);
        assert_eq!(trim.input_floors, vec![trim.step + 1]);
        assert!(trim.snap_floor <= trim.snapshot);
    }
    let (ok, report) = t.check(&["--id", ID, "--complete"]);
    assert!(ok, "{report}");
}
