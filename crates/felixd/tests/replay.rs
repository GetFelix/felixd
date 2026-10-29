//! `felixd replay`: the state it rebuilds at an offset or a time is the one
//! the live workload had there, after a takeover too, and it writes nothing.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::time::Duration;

use common::{ID, INPUT, OUTPUT, SNAPSHOTS, assert_uninterrupted, eventually, test_felix};
use felixd_core::output;
use felixd_core::snapshot::{self, SnapshotHeader};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

fn state(n: u64) -> String {
    format!("{{\"count\":{n}}}")
}

fn rfc3339(micros: u64) -> String {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(micros) * 1000)
        .unwrap()
        .format(&Rfc3339)
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replay_after_a_takeover_matches_the_live_output() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &["--snapshot-every", "7"]).await;
    let mut first = t.start_agent();
    t.publish_all(INPUT, (0..20).map(count)).await;
    eventually(
        "the first owner's output",
        Duration::from_secs(60),
        || async { (t.outputs(OUTPUT).await.len() >= 20).then_some(()) },
    )
    .await;
    first.kill();
    let _second = t.start_agent();
    t.publish_all(INPUT, (20..40).map(count)).await;
    assert_uninterrupted(&t, &module, 40).await;

    let snapshots = t.read_all(SNAPSHOTS).await;
    let epochs: Vec<u64> = snapshots
        .iter()
        .map(|(_, r)| snapshot::decode(r).unwrap().0.epoch)
        .collect();
    assert!(
        epochs.first() < epochs.last(),
        "both owners should have snapshotted: {epochs:?}"
    );
    let outputs = t.outputs(OUTPUT).await;
    for (_, record) in &outputs {
        let decoded = output::decode(record).unwrap();
        let (written, payload) = decoded.output().unwrap();
        let live = u64::from_be_bytes(payload.try_into().unwrap());
        let offset = written.header.input_offset.to_string();
        let (ok, replayed) = t.replay(&["--id", ID, "--to", &offset]);
        assert!(ok, "replay to {offset} failed");
        assert_eq!(replayed, state(live), "replay to input {offset}");
    }
    // Replay writes nothing.
    assert_eq!(t.read_all(SNAPSHOTS).await, snapshots);
    assert_eq!(t.outputs(OUTPUT).await, outputs);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replay_restores_the_nearest_snapshot_at_or_before_the_target() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..10).map(count)).await;
    // States no uninterrupted run has, so the output shows which was restored.
    let at = |input_offset, n: u64| {
        let header = SnapshotHeader {
            input_offset,
            output_tail: input_offset + 1,
        };
        snapshot::encode(header, 1, &count(n))
    };
    t.publish_all(SNAPSHOTS, [at(2, 100), at(6, 200)]).await;
    for (to, want) in [(0, 1), (1, 2), (2, 100), (5, 103), (6, 200), (9, 203)] {
        let (ok, replayed) = t.replay(&["--id", ID, "--to", &to.to_string()]);
        assert!(ok, "replay to {to} failed");
        assert_eq!(replayed, state(want), "replay to input {to}");
    }
    let (ok, _) = t.replay(&["--id", ID, "--to", "10"]);
    assert!(!ok, "a replay past the input's tail should fail");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replay_to_a_time_includes_the_inputs_appended_before_it() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..3).map(count)).await;
    // Apart in time, so the broker stamps input 3 later than input 2.
    tokio::time::sleep(Duration::from_millis(50)).await;
    t.publish_all(INPUT, (3..5).map(count)).await;
    let tail = t.felix.tail(INPUT).await.unwrap();
    let inputs = t.felix.read_range(INPUT, 0, tail).await.unwrap();
    let third = inputs[3].timestamp_micros;
    assert!(inputs[2].timestamp_micros < third);

    let cases = [
        (rfc3339(third), 3),
        (rfc3339(third + 1), 4),
        (rfc3339(third + 3_600_000_000), 5),
        ("1970-01-01T00:00:00Z".to_owned(), 0),
    ];
    for (to, want) in cases {
        let (ok, replayed) = t.replay(&["--id", ID, "--to", &to]);
        assert!(ok, "replay to {to} failed");
        assert_eq!(replayed, state(want), "replay to {to}");
    }
}
