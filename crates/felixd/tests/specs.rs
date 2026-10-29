//! Changing a workload's spec: a running workload restarts under the new one,
//! a change that would not reproduce what is already written is refused, and
//! only specs on `sys.specs` run.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::time::Duration;

use common::{ID, INPUT, LEASE_TTL, OUTPUT, eventually, test_felix};
use felixd_core::output::{self, Record};
use felixd_core::spec::WorkloadState;
use felixd_core::{module, names};

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
    // check runs the module the output was written under, not the refused one.
    let (ok, report) = t.check(&["--id", ID]);
    assert!(ok, "{report}");
}

/// The first claim lands while the owner of a new module is frozen between
/// its look at the empty stream and its own claim. It must refuse after its
/// claim, writing nothing.
#[tokio::test(flavor = "multi_thread")]
async fn an_owner_refuses_after_its_claim_when_an_older_spec_claimed_first() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    // Only the module: a second workload would take the node's one freeze.
    let window = felixd_testkit::workload("window");
    t.felix
        .cache_put(
            names::MODULES,
            &module::hash(&window),
            window.clone().into(),
        )
        .await
        .unwrap();
    let older = t.spec().await;
    let mut newer = older.clone();
    newer.module = module::hash(&window);
    t.put_spec(&newer).await;
    let mut frozen =
        t.start_node_with_ttl("frozen", "freeze-before-claim=1", Duration::from_secs(30));
    eventually("the freeze", Duration::from_secs(60), || async {
        frozen.stopped().then_some(())
    })
    .await;
    let (lease, _) = t.lease(ID).await.expect("the frozen node's lease");
    let claim = output::encode_claim_under(lease.epoch - 1, older.record.unwrap(), 0, "older");
    t.publish_if(OUTPUT, vec![claim], 0).await;
    frozen.resume();
    t.publish_all(INPUT, (0..3).map(count)).await;
    eventually("the frozen node's claim", Duration::from_secs(30), || async {
        t.read_all(OUTPUT)
            .await
            .iter()
            .any(|(_, r)| matches!(output::decode(r), Ok(Record::Claim { epoch, .. }) if epoch == lease.epoch))
            .then_some(())
    })
    .await;
    eventually(
        "the lease to be released",
        Duration::from_secs(30),
        || async { t.lease(ID).await.is_none().then_some(()) },
    )
    .await;
    tokio::time::sleep(LEASE_TTL).await;
    assert!(t.outputs(OUTPUT).await.is_empty(), "the new module ran");
    assert_eq!(t.spec().await.state, WorkloadState::Running);
    assert!(frozen.alive(), "the frozen node exited");
}

/// A node whose view of the spec is stale finds a claim under a later record
/// and refuses, so claims never go back to an older spec.
#[tokio::test(flavor = "multi_thread")]
async fn an_agent_refuses_a_spec_older_than_one_already_claimed() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    let older = t.spec().await;
    t.put_spec(&older).await;
    let newer = t.spec().await.record.unwrap();
    t.publish_if(
        OUTPUT,
        vec![output::encode_claim_under(1, newer, newer, "newer")],
        0,
    )
    .await;
    t.put_cache_spec(&older).await;
    let _agent = t.start_agent();
    t.publish_all(INPUT, (0..3).map(count)).await;
    tokio::time::sleep(LEASE_TTL * 3).await;
    assert!(t.outputs(OUTPUT).await.is_empty(), "the older spec ran");
    for (_, record) in t.read_all(OUTPUT).await {
        if let Ok(Record::Claim { spec: named, .. }) = output::decode(&record) {
            assert_eq!(named, Some(newer), "the older spec was claimed");
        }
    }
    assert_eq!(t.spec().await.state, WorkloadState::Running);
}

#[tokio::test(flavor = "multi_thread")]
async fn submit_never_moves_the_cache_back() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &[]).await;
    // As if sys.specs were recreated and its offsets restarted below this.
    let mut spec = t.spec().await;
    spec.record = Some(1000);
    t.put_cache_spec(&spec).await;
    assert!(!t.run(ID, &module, INPUT, &["--fuel", "7"]).success());
    let after = t.spec().await;
    assert_eq!(after.record, Some(1000));
    assert_eq!(after, spec);
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

/// Claims 10 and 12 are on the stream when a node holding 11 passed its
/// look before 12 landed. Its own claim must carry 12 forward as the floor
/// and it refuses; a second node holding 11 then sees that floor and refuses
/// before claiming, although the last claim names 11.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_claim_carries_the_floor_so_the_next_stale_node_refuses() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    let stale = t.spec().await;
    t.put_spec(&stale).await;
    let newer = t.spec().await.record.unwrap();
    t.put_cache_spec(&stale).await;
    let mut first =
        t.start_node_with_ttl("stale-1", "freeze-before-claim=1", Duration::from_secs(30));
    eventually("the freeze", Duration::from_secs(60), || async {
        first.stopped().then_some(())
    })
    .await;
    let (lease, _) = t.lease(ID).await.expect("the frozen node's lease");
    let claim = output::encode_claim_under(lease.epoch - 1, newer, newer, "newer");
    t.publish_if(OUTPUT, vec![claim], 0).await;
    first.resume();
    let floor = eventually(
        "the stale node's claim",
        Duration::from_secs(30),
        || async {
            t.read_all(OUTPUT)
                .await
                .iter()
                .find_map(|(_, r)| match output::decode(r) {
                    Ok(Record::Claim {
                        epoch, spec, floor, ..
                    }) if epoch == lease.epoch => Some((spec, floor)),
                    _ => None,
                })
        },
    )
    .await;
    assert_eq!(floor, (stale.record, Some(newer)));
    eventually(
        "the lease to be released",
        Duration::from_secs(30),
        || async { t.lease(ID).await.is_none().then_some(()) },
    )
    .await;

    let _second = t.start_agent();
    t.publish_all(INPUT, (0..3).map(count)).await;
    tokio::time::sleep(LEASE_TTL * 3).await;
    assert!(t.outputs(OUTPUT).await.is_empty(), "the stale spec ran");
    let claims = t
        .read_all(OUTPUT)
        .await
        .iter()
        .filter(|(_, r)| matches!(output::decode(r), Ok(Record::Claim { .. })))
        .count();
    assert_eq!(claims, 2, "the second stale node claimed");
    assert_eq!(
        t.needs_operator().await.0,
        [],
        "a stale spec was recorded as needing an operator"
    );
    assert!(first.alive(), "the first stale node exited");
}
