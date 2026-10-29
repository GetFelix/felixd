//! Changing a workload's spec: a running workload restarts under the new one,
//! a change that would not reproduce what is already written is refused, and
//! only specs on `sys.specs` run.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::time::Duration;

use common::{ID, INPUT, LEASE_TTL, OUTPUT, eventually, test_felix};
use felixd_core::module;
use felixd_core::output::{self, Record};
use felixd_core::spec::WorkloadState;

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_running_workload_restarts_under_a_changed_spec() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    let _agent = t.start_agent();
    t.publish_all(INPUT, (0..5).map(count)).await;
    eventually("output", Duration::from_secs(60), || async {
        (t.outputs(OUTPUT).await.len() >= 5).then_some(())
    })
    .await;
    // Too little fuel for any step: only a task that took the new spec fails.
    let mut spec = t.spec().await;
    spec.limits.fuel_per_call = 1;
    t.put_spec(&spec).await;
    t.publish_all(INPUT, (5..10).map(count)).await;
    let failure = t.failure().await;
    assert!(!failure.is_empty());
    // The outputs were written under the first spec's fuel, and are judged
    // under it rather than the latest.
    let (ok, report) = t.check(&["--id", ID]);
    assert!(ok, "{report}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_workloads_entry_without_a_spec_record_is_not_run() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    let mut spec = t.spec().await;
    spec.record = None;
    spec.snapshot_every = 7;
    t.put_cache_spec(&spec).await;
    let _agent = t.start_agent();
    t.publish_all(INPUT, (0..3).map(count)).await;
    tokio::time::sleep(LEASE_TTL * 3).await;
    assert!(t.lease(ID).await.is_none(), "the workload was claimed");
    assert!(t.read_all(OUTPUT).await.is_empty(), "something was written");
    assert_eq!(t.spec().await.state, WorkloadState::Running);
}

/// A new module written past `felixd run`'s check reaches the agent, which
/// leaves it for an operator instead of running it over the old output.
#[tokio::test(flavor = "multi_thread")]
async fn an_agent_refuses_a_spec_record_with_a_new_module_once_there_is_output() {
    let Some(t) = test_felix().await else { return };
    let window = t.submit_as("other", "window", &[]).await;
    t.submit("counter", &[]).await;
    let _agent = t.start_agent();
    t.publish_all(INPUT, (0..3).map(count)).await;
    eventually("output", Duration::from_secs(60), || async {
        (t.outputs(OUTPUT).await.len() >= 3).then_some(())
    })
    .await;

    let mut spec = t.spec().await;
    spec.module = module::hash(&window);
    t.put_spec(&spec).await;
    t.publish_all(INPUT, (3..6).map(count)).await;
    tokio::time::sleep(LEASE_TTL * 3).await;
    assert_eq!(t.outputs(OUTPUT).await.len(), 3, "the new module ran");
    let spec = t.spec().await;
    assert_eq!(spec.state, WorkloadState::Running, "{:?}", spec.failure);
    assert_eq!(spec.module, module::hash(&window));
    // The agent refused before claiming, so no claim names the new spec.
    for (_, record) in t.read_all(OUTPUT).await {
        if let Ok(Record::Claim { spec: named, .. }) = output::decode(&record) {
            assert_ne!(named, spec.record, "the refused spec was claimed");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_module_is_refused_once_the_workload_has_output() {
    let Some(t) = test_felix().await else { return };
    // With nothing written yet, the module may change.
    t.submit("counter", &[]).await;
    let window = t.submit("window", &[]).await;
    let module = t.submit("counter", &[]).await;
    assert_ne!(window, module);
    let _agent = t.start_agent();
    t.publish_all(INPUT, (0..3).map(count)).await;
    eventually("output", Duration::from_secs(60), || async {
        (t.outputs(OUTPUT).await.len() >= 3).then_some(())
    })
    .await;
    let before = t.spec().await.module;

    let dir = std::env::temp_dir().join(format!("felixd-{}", t.settings.namespace));
    let path = dir.join(format!("{}.wasm", felixd_core::module::hash(&window)));
    let out = t
        .felixd(&["run", "--id", common::ID, "--input", INPUT])
        .args([
            "--replication-factor",
            &common::replication_factor().to_string(),
        ])
        .arg(&path)
        .stderr(std::process::Stdio::piped())
        .output()
        .unwrap();
    assert!(!out.status.success(), "the new module was accepted");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("already has output"), "{stderr}");
    assert_eq!(t.spec().await.module, before);
}
