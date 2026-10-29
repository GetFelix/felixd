//! `felixd replay`: the state it rebuilds at an offset or a time is the one
//! the live workload had there, after a takeover too, and it writes nothing.
//! `--diff` finds no divergence in a live run of any example workload, and
//! finds a changed output or a changed module.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::time::Duration;

use common::{
    ID, INPUT, OUTPUT, SNAPSHOTS, assert_uninterrupted, assert_uninterrupted_on, eventually,
    test_felix,
};
use felixd_core::output::{self, OutputHeader, Record};
use felixd_core::replay::digest;
use felixd_core::snapshot::{self, SnapshotHeader};
use felixd_core::{module, names};
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
        .map(|(_, r)| snapshot::decode(r).unwrap().epoch())
        .collect();
    assert!(
        epochs.first() < epochs.last(),
        "both owners should have snapshotted: {epochs:?}"
    );
    let outputs = t.outputs(OUTPUT).await;
    for (_, record) in &outputs {
        let decoded = output::decode(record).unwrap();
        let (written, payload) = decoded.output().unwrap();
        let live = u64::from_be_bytes(payload.payload.try_into().unwrap());
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

/// An input every example workload takes: the window workload reads a
/// timestamp and a value, the others count or echo it.
fn window_input(i: u64) -> Vec<u8> {
    let mut r = (i * 3_000).to_le_bytes().to_vec();
    r.extend((i as i64 % 7 - 3).to_le_bytes());
    r
}

#[tokio::test(flavor = "multi_thread")]
async fn a_diff_after_a_takeover_finds_every_example_workload_deterministic() {
    let Some(t) = test_felix().await else { return };
    let names = ["counter", "window", "faulty"];
    let mut modules = Vec::new();
    for name in names {
        modules.push(t.submit_as(name, name, &["--snapshot-every", "5"]).await);
    }
    let mut first = t.start_agent();
    t.publish_all(INPUT, (0..15).map(window_input)).await;
    eventually(
        "the first owner's output",
        Duration::from_secs(60),
        || async { (t.outputs("wl.counter.out").await.len() >= 15).then_some(()) },
    )
    .await;
    first.kill();
    let second = t.start_agent();
    t.publish_all(INPUT, (15..30).map(window_input)).await;
    for (name, module) in names.iter().zip(&modules) {
        let output = names::output_stream(name);
        assert_uninterrupted_on(&t, module, 30, &output).await;
        let claimed: Vec<Vec<u8>> = t
            .read_all(&output)
            .await
            .iter()
            .filter_map(|(_, r)| match output::decode(r) {
                Ok(Record::Claim { node, .. }) => Some(node.to_vec()),
                _ => None,
            })
            .collect();
        assert_eq!(
            claimed.last().map(Vec::as_slice),
            Some(second.node_id.as_bytes()),
            "{output}: {claimed:?}"
        );
        for args in [&["--diff"][..], &["--diff", "--from", "12"]] {
            let (ok, report) = t.replay(&[&["--id", name][..], args].concat());
            assert!(ok, "{name} {args:?}: {report}");
            assert!(report.contains("outputs match"), "{name}: {report}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_diff_names_the_node_epoch_and_digests_of_a_changed_output() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..5).map(count)).await;
    let at = |i: u64, epoch, payload: &[u8]| {
        let header = OutputHeader {
            input_offset: i,
            index: 0,
        };
        output::encode(header, epoch, payload)
    };
    let mut records = vec![output::encode_claim(1, "node-a")];
    records.extend((0..3).map(|i| at(i, 1, &count(i + 1))));
    records.push(output::encode_claim(2, "node-b"));
    records.push(at(3, 2, &count(40)));
    records.push(at(4, 2, &count(5)));
    t.publish_all(OUTPUT, records).await;

    let (ok, report) = t.replay(&["--id", ID, "--diff"]);
    assert!(!ok, "the diff passed: {report}");
    let want = format!(
        "first divergence at input 3 output 0: recorded by node node-b at epoch 2 with sha256 {}, replayed with sha256 {}",
        digest(&count(40)),
        digest(&count(4))
    );
    assert_eq!(report, want);
    let (ok, report) = t.replay(&["--id", ID, "--diff", "--to", "2"]);
    assert!(ok, "{report}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_diff_with_another_module_finds_where_it_changes_the_output() {
    let Some(t) = test_felix().await else { return };
    let counter = t.submit("counter", &[]).await;
    let _agent = t.start_agent();
    // The echoing faulty workload outputs what the counter would for these.
    t.publish_all(INPUT, (1..=5).map(count)).await;
    t.publish_all(INPUT, [count(100), count(7)]).await;
    assert_uninterrupted(&t, &counter, 7).await;
    let faulty = felixd_testkit::workload("faulty");
    let hash = module::hash(&faulty);
    t.felix
        .cache_put(names::MODULES, &hash, faulty.into())
        .await
        .unwrap();
    let before = t.read_all(OUTPUT).await;

    let (ok, report) = t.replay(&["--id", ID, "--diff", "--module", &hash]);
    assert!(!ok, "the diff passed: {report}");
    assert!(
        report.starts_with("first divergence at input 5 output 0: recorded by node "),
        "{report}"
    );
    assert!(report.ends_with(&digest(&count(100))), "{report}");
    let (ok, report) = t.replay(&["--id", ID, "--diff", "--to", "4", "--module", &hash]);
    assert!(ok, "{report}");
    let (ok, report) = t.replay(&["--id", ID, "--diff", "--module", &module::hash(&counter)]);
    assert!(ok, "{report}");
    let (ok, state) = t.replay(&["--id", ID, "--to", "6", "--module", &hash]);
    assert!(ok);
    assert_eq!(state, r#"{"chunks":[]}"#);
    assert_eq!(t.read_all(OUTPUT).await, before);
}
