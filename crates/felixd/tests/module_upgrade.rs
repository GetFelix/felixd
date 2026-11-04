//! `felixd run --upgrade-at`: a new module takes over a running workload at an
//! input offset. Inputs before it keep the old module, so the output stream
//! stays what an uninterrupted run of the two modules in turn would write, on
//! every node that resumes it.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::process::Stdio;
use std::time::Duration;

use common::{ID, INPUT, eventually, test_felix};
use felixd_core::{output, snapshot};

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

/// The counter's total after each input: one per input, and two per input
/// once the `changed` build has the total at ten. Upgrading at input 20 from
/// the plain build gives 1 to 20, then 22, 24 and on.
fn totals(inputs: u64, upgrade_at: u64) -> Vec<u64> {
    let mut total = 0;
    (0..inputs)
        .map(|at| {
            total += if at >= upgrade_at && total >= 10 {
                2
            } else {
                1
            };
            total
        })
        .collect()
}

async fn written(t: &common::TestFelix, outputs: usize) -> Vec<u64> {
    let found = eventually("the outputs", Duration::from_secs(60), || async {
        let found = t.outputs(common::OUTPUT).await;
        (found.len() >= outputs).then_some(found)
    })
    .await;
    found
        .iter()
        .map(|(_, record)| {
            let (_, out) = output::decode(record).unwrap().output().unwrap();
            u64::from_be_bytes(out.payload.try_into().unwrap())
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_module_takes_over_at_the_input_the_spec_names() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &["--snapshot-every", "5"]).await;
    let mut agent = t.start_agent();
    t.publish_all(INPUT, (0..15).map(count)).await;
    assert_eq!(written(&t, 15).await, totals(15, 20));

    // Input 14 has run, so the new module cannot take over there.
    let changed = felixd_testkit::workload_with("counter", &["changed"]);
    let behind = t.run(ID, &changed, INPUT, &["--upgrade-at", "10"]);
    assert!(!behind.success());

    assert!(
        t.run(ID, &changed, INPUT, &["--upgrade-at", "20"])
            .success()
    );
    t.publish_all(INPUT, (15..30).map(count)).await;
    assert_eq!(written(&t, 30).await, totals(30, 20));

    // A node that takes over resumes from a snapshot of the old module and
    // reaches the upgrade on its own.
    agent.kill();
    let mut second = t.start_agent();
    t.publish_all(INPUT, (30..40).map(count)).await;
    assert_eq!(written(&t, 40).await, totals(40, 20));
    second.kill();

    let (ok, report) = t.check(&["--id", ID, "--complete"]);
    assert!(ok, "felixd check found problems:\n{report}");

    // Replay reaches the same state, whichever side of the upgrade it ends on.
    let replayed = |to: &str| {
        let out = t
            .felixd(&["replay", "--id", ID, "--to", to])
            .stdout(Stdio::piped())
            .output()
            .unwrap();
        assert!(out.status.success());
        String::from_utf8_lossy(&out.stdout).trim().to_owned()
    };
    assert_eq!(replayed("19"), r#"{"count":20}"#);
    assert_eq!(
        replayed("39"),
        format!(r#"{{"count":{}}}"#, totals(40, 20)[39])
    );
}

/// The inputs of the snapshots on the stream.
async fn snapshots_taken(t: &common::TestFelix) -> Vec<u64> {
    t.read_all(common::SNAPSHOTS)
        .await
        .iter()
        .filter_map(|(_, r)| snapshot::decode(r).ok()?.taken())
        .map(|taken| taken.header.input_offset)
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_owner_on_the_old_spec_snapshots_nothing_once_an_upgrade_is_on_the_way() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &["--snapshot-every", "5"]).await;
    let _agent = t.start_agent();
    t.publish_all(INPUT, (0..15).map(count)).await;
    assert_eq!(written(&t, 15).await, totals(15, u64::MAX));

    // The submit writes its upgrade record and stops before the spec, as
    // when the old owner has not yet seen the spec: it keeps running the
    // old module past input 20, and must not snapshot there.
    let changed = felixd_testkit::workload_with("counter", &["changed"]);
    let dir = std::env::temp_dir().join(format!("felixd-{}", t.settings.namespace));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("changed.wasm");
    std::fs::write(&path, &changed).unwrap();
    let stopped = t
        .felixd(&["run", "--id", ID, "--input", INPUT, "--upgrade-at", "20"])
        .args([
            "--replication-factor",
            &common::replication_factor().to_string(),
        ])
        .arg(&path)
        .env("FELIXD_FAULTS", "stop-after-upgrade-record=1")
        .status()
        .unwrap();
    assert!(!stopped.success());
    t.publish_all(INPUT, (15..30).map(count)).await;
    assert_eq!(written(&t, 30).await, totals(30, u64::MAX));
    tokio::time::sleep(Duration::from_secs(2)).await;
    let late: Vec<u64> = snapshots_taken(&t)
        .await
        .into_iter()
        .filter(|&at| at >= 20)
        .collect();
    assert!(
        late.is_empty(),
        "the old module snapshotted inputs {late:?}"
    );

    // The spec that names an upgrade is written after the record, so its
    // owner snapshots again.
    assert!(
        t.run(ID, &changed, INPUT, &["--upgrade-at", "40"])
            .success()
    );
    t.publish_all(INPUT, (30..50).map(count)).await;
    assert_eq!(written(&t, 50).await, totals(50, 40));
    eventually(
        "a snapshot after the upgrade",
        Duration::from_secs(60),
        || async {
            snapshots_taken(&t)
                .await
                .iter()
                .any(|&at| at >= 40)
                .then_some(())
        },
    )
    .await;
}
