//! `felixd replay --module`: a new version of a workload's module runs over
//! the recorded inputs, from a snapshot the current version took when it
//! declares it can read it. An upgrade that behaves the same reports no
//! difference, one that does not is reported at the first input it differs
//! on, and one that cannot read the snapshot starts from the beginning or
//! says why it cannot.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::process::Stdio;
use std::time::Duration;

use common::{
    ID, INPUT, SNAPSHOTS, TestFelix, assert_uninterrupted, eventually, replication_factor,
    test_felix,
};
use felixd_agent::admin::{StreamKind, ensure_stream};
use felixd_core::replay::digest;
use felixd_core::snapshot::{self, SnapshotHeader};
use felixd_core::{module, names};

const INPUTS: u64 = 30;

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

fn state(n: u64) -> String {
    format!("{{\"count\":{n}}}")
}

/// What `felixd replay` did.
struct Ran {
    ok: bool,
    stdout: String,
    stderr: String,
}

fn replay(t: &TestFelix, args: &[&str]) -> Ran {
    let out = t
        .felixd(&["replay", "--id", ID])
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    Ran {
        ok: out.status.success(),
        stdout: String::from_utf8_lossy(&out.stdout).trim().to_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).trim().to_owned(),
    }
}

/// Puts `bytes` in the `modules` cache and returns its hash.
async fn put_module(t: &TestFelix, bytes: Vec<u8>) -> String {
    let hash = module::hash(&bytes);
    t.felix
        .cache_put(names::MODULES, &hash, bytes.into())
        .await
        .unwrap();
    hash
}

/// A recorded run of the counter.
struct Recorded {
    /// The input offsets of its snapshots.
    offsets: Vec<u64>,
    /// The module that ran.
    module: Vec<u8>,
}

/// The counter, built with `features`, run on an agent over [`INPUTS`] inputs
/// and snapshotted every five. Each input is its own number from 1, so a
/// module that echoes its input outputs what the counter did.
async fn recorded_counter(t: &TestFelix, features: &[&str]) -> Recorded {
    let kind = StreamKind {
        replication_factor: replication_factor(),
    };
    ensure_stream(&t.settings, INPUT, kind).await.unwrap();
    let module = felixd_testkit::workload_with("counter", features);
    assert!(
        t.run(ID, &module, INPUT, &["--snapshot-every", "5"])
            .success()
    );
    let mut agent = t.start_agent();
    t.publish_all(INPUT, (1..=INPUTS).map(count)).await;
    assert_uninterrupted(t, &module, INPUTS).await;
    let last = INPUTS - 1;
    let offsets = eventually("the last snapshot", Duration::from_secs(60), || async {
        let offsets = snapshot_offsets(t).await;
        offsets.contains(&last).then_some(offsets)
    })
    .await;
    agent.kill();
    Recorded { offsets, module }
}

async fn snapshot_offsets(t: &TestFelix) -> Vec<u64> {
    t.read_all(SNAPSHOTS)
        .await
        .iter()
        .filter_map(|(_, r)| snapshot::decode(r).ok()?.taken())
        .map(|taken| taken.header.input_offset)
        .collect()
}

/// The snapshot a replay that begins at input `from` restores.
fn nearest(offsets: &[u64], from: u64) -> u64 {
    *offsets.iter().filter(|&&o| o < from).max().unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_upgrade_that_behaves_the_same_matches_from_the_snapshot() {
    let Some(t) = test_felix().await else { return };
    let Recorded { offsets, module } = recorded_counter(&t, &[]).await;
    let upgrade = felixd_testkit::workload_with("counter", &["layout-v2"]);
    let hash = put_module(&t, upgrade.clone()).await;

    let from = 20;
    let at = nearest(&offsets, from);
    assert!(at >= 14, "{offsets:?}");
    let ran = replay(&t, &["--diff", "--from", "20", "--module", &hash]);
    assert!(ran.ok, "{} {}", ran.stdout, ran.stderr);
    assert_eq!(ran.stdout, "inputs from 20 below 30: 10 outputs match");
    assert_eq!(
        ran.stderr,
        format!("started from the snapshot of input {at}")
    );

    // The whole history gives the same answer, from the start.
    let ran = replay(
        &t,
        &["--diff", "--from", "20", "--module", &hash, "--from-start"],
    );
    assert!(ran.ok, "{} {}", ran.stdout, ran.stderr);
    assert_eq!(ran.stdout, "inputs from 20 below 30: 10 outputs match");
    assert_eq!(ran.stderr, "started from the initial state, as asked");

    // The workload's own module, by hash or as a file, restores its snapshot
    // as it always has.
    let own = module::hash(&module);
    let ran = replay(&t, &["--diff", "--from", "20", "--module", &own]);
    assert!(ran.ok, "{} {}", ran.stdout, ran.stderr);
    assert_eq!(ran.stdout, "inputs from 20 below 30: 10 outputs match");
    assert_eq!(
        ran.stderr,
        format!("started from the snapshot of input {at}")
    );

    // A component file does as well as one in the cache, and uploads nothing.
    let path = std::env::temp_dir().join(format!("felixd-upgrade-{hash}.wasm"));
    std::fs::write(&path, &upgrade).unwrap();
    let file = path.to_str().unwrap();
    let ran = replay(&t, &["--diff", "--from", "20", "--module", file]);
    assert!(ran.ok, "{} {}", ran.stdout, ran.stderr);
    assert_eq!(ran.stdout, "inputs from 20 below 30: 10 outputs match");
    assert_eq!(
        ran.stderr,
        format!("started from the snapshot of input {at}")
    );
    std::fs::remove_file(&path).unwrap();
    let ran = replay(&t, &["--diff", "--from", "20", "--module", file]);
    assert!(!ran.ok);
    assert!(
        ran.stderr.contains("--module takes the sha256"),
        "{}",
        ran.stderr
    );

    // From the first input there is no snapshot to start from.
    let ran = replay(&t, &["--diff", "--module", &hash]);
    assert!(ran.ok, "{} {}", ran.stdout, ran.stderr);
    assert_eq!(ran.stdout, "inputs from 0 below 30: 30 outputs match");
    assert_eq!(ran.stderr, "started from the initial state");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_upgrade_that_changes_behaviour_is_reported_at_the_first_input_it_differs_on() {
    let Some(t) = test_felix().await else { return };
    let offsets = recorded_counter(&t, &[]).await.offsets;
    let hash = put_module(&t, felixd_testkit::workload_with("counter", &["changed"])).await;

    // The new counter counts by two once the total reaches ten, so the
    // eleventh input, offset 10, is the first it answers differently.
    let at = nearest(&offsets, 5);
    let ran = replay(&t, &["--diff", "--from", "5", "--module", &hash]);
    assert!(!ran.ok, "{} {}", ran.stdout, ran.stderr);
    assert_eq!(
        ran.stderr,
        format!("started from the snapshot of input {at}")
    );
    let divergence = ran.stdout.lines().next().unwrap();
    assert!(
        divergence.starts_with("first divergence at input 10 output 0: recorded by node "),
        "{divergence}"
    );
    let digests = format!(
        "sha256 {}, replayed with sha256 {}",
        digest(&count(11)),
        digest(&count(12))
    );
    assert!(divergence.ends_with(&digests), "{divergence}");

    // Not looking at it, the same upgrade is fine.
    let ran = replay(
        &t,
        &["--diff", "--from", "5", "--to", "9", "--module", &hash],
    );
    assert!(ran.ok, "{} {}", ran.stdout, ran.stderr);
    assert_eq!(ran.stdout, "inputs from 5 below 10: 5 outputs match");

    // Every input from there on differs, and --all lists them all.
    let ran = replay(&t, &["--diff", "--all", "--from", "5", "--module", &hash]);
    assert!(!ran.ok);
    assert_eq!(
        ran.stdout.lines().count() as u64,
        INPUTS - 10,
        "{}",
        ran.stdout
    );
    assert!(
        ran.stdout
            .lines()
            .nth(1)
            .unwrap()
            .starts_with("divergence at input 11 ")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_module_that_declares_no_layout_it_can_read_starts_from_the_beginning() {
    let Some(t) = test_felix().await else { return };
    recorded_counter(&t, &[]).await;
    let faulty = put_module(&t, felixd_testkit::workload("faulty")).await;
    let foreign = put_module(
        &t,
        felixd_testkit::workload_with("counter", &["foreign-layout"]),
    )
    .await;

    // The faulty fixture echoes its input, which is what the counter output,
    // and declares nothing.
    let ran = replay(&t, &["--diff", "--from", "20", "--module", &faulty]);
    assert!(ran.ok, "{} {}", ran.stdout, ran.stderr);
    assert_eq!(ran.stdout, "inputs from 20 below 30: 10 outputs match");
    assert_eq!(
        ran.stderr,
        "started from the initial state, since the new module declares no snapshot layout"
    );

    // The counter built to write a layout of its own, and to read none.
    let ran = replay(&t, &["--diff", "--from", "20", "--module", &foreign]);
    assert!(ran.ok, "{} {}", ran.stdout, ran.stderr);
    assert_eq!(ran.stdout, "inputs from 20 below 30: 10 outputs match");
    assert_eq!(
        ran.stderr,
        "started from the initial state, since the snapshots have the layout \"counter\", \
         which the new module neither writes (\"tally\") nor lists in reads"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_the_new_module_declared_it_reads_but_cannot_is_not_a_divergence() {
    let Some(t) = test_felix().await else { return };
    let offsets = recorded_counter(&t, &[]).await.offsets;
    let hash = put_module(
        &t,
        felixd_testkit::workload_with("counter", &["unreadable"]),
    )
    .await;

    let at = nearest(&offsets, 20);
    let ran = replay(&t, &["--diff", "--from", "20", "--module", &hash]);
    assert!(!ran.ok, "{} {}", ran.stdout, ran.stderr);
    assert!(ran.stdout.is_empty(), "{}", ran.stdout);
    assert!(
        ran.stderr.contains(&format!(
            "snapshot not readable by the new module: restoring the snapshot of input {at} failed"
        )),
        "{}",
        ran.stderr
    );

    // Starting from the beginning does not read the snapshot.
    let ran = replay(
        &t,
        &["--diff", "--from", "20", "--module", &hash, "--from-start"],
    );
    assert!(ran.ok, "{} {}", ran.stdout, ran.stderr);
    assert_eq!(ran.stdout, "inputs from 20 below 30: 10 outputs match");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replay_to_an_offset_continues_the_new_module_from_the_snapshot() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..10).map(count)).await;
    // States no uninterrupted run has, so the state shows which was restored.
    let at = |input_offset, n: u64| {
        let header = SnapshotHeader {
            input_offset,
            output_tail: input_offset + 1,
        };
        snapshot::encode(header, 1, &count(n))
    };
    t.publish_all(SNAPSHOTS, [at(2, 100), at(6, 200)]).await;
    let upgrade = put_module(&t, felixd_testkit::workload_with("counter", &["layout-v2"])).await;
    let undeclared = put_module(&t, felixd_testkit::workload("faulty")).await;

    let ran = replay(&t, &["--to", "8", "--module", &upgrade]);
    assert!(ran.ok, "{}", ran.stderr);
    assert_eq!(ran.stdout, state(202));
    assert_eq!(
        ran.stderr,
        "started from the snapshot of input 6\nran 2 more of the inputs below 9"
    );

    let ran = replay(&t, &["--to", "8", "--module", &upgrade, "--from-start"]);
    assert!(ran.ok, "{}", ran.stderr);
    assert_eq!(ran.stdout, state(9));
    assert_eq!(
        ran.stderr,
        "started from the initial state, as asked\nran 9 more of the inputs below 9"
    );

    // The faulty fixture keeps no counter, and starts from the beginning.
    let ran = replay(&t, &["--to", "8", "--module", &undeclared]);
    assert!(ran.ok, "{}", ran.stderr);
    assert_eq!(ran.stdout, r#"{"chunks":[]}"#);
    assert!(
        ran.stderr.starts_with(
            "started from the initial state, since the new module declares no snapshot layout"
        ),
        "{}",
        ran.stderr
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_recorded_module_that_declares_no_layout_has_no_snapshot_to_start_from() {
    let Some(t) = test_felix().await else { return };
    recorded_counter(&t, &["undeclared"]).await;
    // Behaves as the recorded module did, and declares a layout.
    let upgrade = put_module(&t, felixd_testkit::workload("counter")).await;

    let ran = replay(&t, &["--diff", "--from", "20", "--module", &upgrade]);
    assert!(ran.ok, "{} {}", ran.stdout, ran.stderr);
    assert_eq!(ran.stdout, "inputs from 20 below 30: 10 outputs match");
    assert_eq!(
        ran.stderr,
        "started from the initial state, since the module the workload runs declares no snapshot \
         layout, so only that module reads its snapshots"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_section_that_does_not_parse_is_an_error_only_where_it_is_read() {
    let Some(t) = test_felix().await else { return };
    let Recorded { module, .. } = recorded_counter(&t, &[]).await;
    let broken = put_module(
        &t,
        felixd_testkit::workload_with("counter", &["malformed-layout"]),
    )
    .await;

    // It is read to decide whether to restore a snapshot, and says how to go on.
    let ran = replay(&t, &["--diff", "--from", "20", "--module", &broken]);
    assert!(!ran.ok, "{} {}", ran.stdout, ran.stderr);
    assert!(ran.stdout.is_empty(), "{}", ran.stdout);
    assert!(
        ran.stderr
            .contains("the new module: the felixd.snapshot section"),
        "{}",
        ran.stderr
    );
    assert!(ran.stderr.contains("--from-start"), "{}", ran.stderr);

    // It is not read when no snapshot is in play.
    for args in [&["--diff", "--from", "20", "--from-start"][..], &["--diff"]] {
        let ran = replay(&t, &[args, &["--module", &broken]].concat());
        assert!(ran.ok, "{args:?}: {} {}", ran.stdout, ran.stderr);
        assert!(ran.stdout.ends_with("outputs match"), "{}", ran.stdout);
    }
    let own = module::hash(&module);
    let ran = replay(&t, &["--diff", "--from", "20", "--module", &own]);
    assert!(ran.ok, "{} {}", ran.stdout, ran.stderr);
}

/// Two inputs, so the workload has a merge log, and its snapshots and the
/// replay's offsets are steps on it.
#[tokio::test(flavor = "multi_thread")]
async fn an_upgrade_of_a_workload_with_two_inputs_starts_from_a_snapshot_on_the_merge_log() {
    let Some(t) = test_felix().await else { return };
    const INPUTS: [&str; 2] = ["orders", "payments"];
    let kind = StreamKind {
        replication_factor: replication_factor(),
    };
    for input in INPUTS {
        ensure_stream(&t.settings, input, kind).await.unwrap();
    }
    let module = felixd_testkit::workload("counter");
    assert!(
        t.run_on(ID, &module, &INPUTS, &["--snapshot-every", "4"])
            .success()
    );
    t.publish_all(INPUTS[0], (0..6).map(count)).await;
    t.publish_all(INPUTS[1], (0..6).map(count)).await;
    let mut agent = t.start_agent();
    let offsets = eventually("three snapshots", Duration::from_secs(60), || async {
        let offsets = snapshot_offsets(&t).await;
        (offsets.len() >= 3).then_some(offsets)
    })
    .await;
    agent.kill();
    let upgrade = put_module(&t, felixd_testkit::workload_with("counter", &["layout-v2"])).await;

    // The merge log offset of the second to last snapshot.
    let mut sorted = offsets.clone();
    sorted.sort_unstable();
    let at = sorted[sorted.len() - 2];
    let from = (at + 1).to_string();
    let ran = replay(&t, &["--diff", "--from", &from, "--module", &upgrade]);
    assert!(ran.ok, "{} {}", ran.stdout, ran.stderr);
    assert!(ran.stdout.ends_with("4 outputs match"), "{}", ran.stdout);
    assert_eq!(
        ran.stderr,
        format!("started from the snapshot of input {at}")
    );

    let changed = put_module(&t, felixd_testkit::workload_with("counter", &["changed"])).await;
    let ran = replay(&t, &["--diff", "--module", &changed]);
    assert!(!ran.ok, "{} {}", ran.stdout, ran.stderr);
    assert!(
        ran.stdout.starts_with("first divergence at input "),
        "{}",
        ran.stdout
    );
}
