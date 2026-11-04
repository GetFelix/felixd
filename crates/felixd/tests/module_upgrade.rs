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
use felixd_core::output;

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
