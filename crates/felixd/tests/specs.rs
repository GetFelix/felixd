//! Changing a workload's spec: a running workload restarts under the new one,
//! and a change that would not reproduce what is already written is refused.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::time::Duration;

use common::{INPUT, OUTPUT, eventually, test_felix};

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
        (t.felix.tail(OUTPUT).await.unwrap() >= 5).then_some(())
    })
    .await;
    // Too little fuel for any step: only a task that took the new spec fails.
    let mut spec = t.spec().await;
    spec.limits.fuel_per_call = 1;
    t.put_spec(&spec).await;
    t.publish_all(INPUT, (5..10).map(count)).await;
    let failure = t.failure().await;
    assert!(!failure.is_empty());
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
        (t.felix.tail(OUTPUT).await.unwrap() >= 3).then_some(())
    })
    .await;
    let before = t.spec().await.module;

    let dir = std::env::temp_dir().join(format!("felixd-{}", t.settings.namespace));
    let path = dir.join("window.wasm");
    let out = t
        .felixd(&["run", "--id", common::ID, "--input", INPUT])
        .arg(&path)
        .stderr(std::process::Stdio::piped())
        .output()
        .unwrap();
    assert!(!out.status.success(), "the new module was accepted");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("already has output"), "{stderr}");
    assert_eq!(t.spec().await.module, before);
}
