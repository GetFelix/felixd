//! `felixd drain` hands a node's workloads to other nodes without waiting
//! for its leases to expire, and the node claims nothing until `felixd
//! undrain`.
//!
//! Needs a Felix broker; see `common`. The `cluster` CI job runs it against
//! three brokers too.

mod common;

use std::process::Stdio;
use std::time::{Duration, Instant};

use common::{
    ID, INPUT, LEASE_TTL, OUTPUT, TestFelix, assert_uninterrupted, eventually, replication_factor,
    test_felix,
};
use felixd_agent::felix::Felix;
use felixd_agent::ps::ps;
use felixd_core::names;
use felixd_core::output::{self, Record};
use felixd_core::spec::WorkloadState;
use felixd_core::status::State;

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

/// Runs `felixd <args>`, killing it after `within`. Returns whether it
/// succeeded, and what it printed on stdout and stderr.
async fn felixd_within(t: &TestFelix, args: &[&str], within: Duration) -> (bool, String, String) {
    let mut child = t
        .felixd(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + within;
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            let _ = child.kill();
            panic!("felixd {args:?} took longer than {within:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let out = child.wait_with_output().unwrap();
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

async fn drain(t: &TestFelix, node: &str, wait: bool) -> String {
    let rf = replication_factor().to_string();
    let mut args = vec!["drain", node, "--replication-factor", &rf];
    if wait {
        args.push("--wait");
    }
    let (ok, _, stderr) = felixd_within(t, &args, Duration::from_secs(60)).await;
    assert!(ok, "felixd drain failed: {stderr}");
    stderr
}

async fn undrain(t: &TestFelix, node: &str) {
    let (ok, _, stderr) = felixd_within(t, &["undrain", node], Duration::from_secs(60)).await;
    assert!(ok, "felixd undrain failed: {stderr}");
}

/// The node each claim on the output stream names, in order.
async fn claims(t: &TestFelix) -> Vec<(u64, String)> {
    t.read_all(OUTPUT)
        .await
        .iter()
        .filter_map(|(_, r)| match output::decode(r).unwrap() {
            Record::Claim { epoch, node, .. } => {
                Some((epoch, String::from_utf8_lossy(node).into_owned()))
            }
            _ => None,
        })
        .collect()
}

/// Waits for `node` to hold the workload with an epoch.
async fn held_by(t: &TestFelix, node: &str) -> u64 {
    let (lease, _) = eventually(
        &format!("{node}'s claim"),
        Duration::from_secs(60),
        || async {
            t.lease(ID)
                .await
                .filter(|(l, _)| l.owner == node && l.epoch > 0)
        },
    )
    .await;
    lease.epoch
}

/// The holder is drained while a producer keeps it stepping. Its lease is
/// released, not left to expire: the other node takes over well within the
/// 30 s TTL, and the output has every input once.
#[tokio::test(flavor = "multi_thread")]
async fn a_holder_drained_mid_step_hands_off_without_waiting_for_its_lease() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &[]).await;
    let ttl = Duration::from_secs(30);
    let mut first = t.start_node_with_ttl("first", "", ttl);
    held_by(&t, "first").await;

    let producer = tokio::spawn({
        let settings = t.settings.clone();
        async move {
            let felix = Felix::connect(&settings).await.unwrap();
            for i in 0..300 {
                felix.publish(INPUT, count(i)).await.unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    });
    let mut second = t.start_node_with_ttl("second", "", ttl);
    eventually("second to be up", Duration::from_secs(30), || async {
        t.felix
            .cache_get(names::NODES, "second")
            .await
            .unwrap()
            .map(|_| ())
    })
    .await;
    eventually("some outputs", Duration::from_secs(30), || async {
        (t.outputs(OUTPUT).await.len() >= 20).then_some(())
    })
    .await;

    let drained_at = Instant::now();
    drain(&t, "first", false).await;
    held_by(&t, "second").await;
    let handoff = drained_at.elapsed();
    assert!(
        handoff < Duration::from_secs(10),
        "the handoff took {handoff:?}, as long as waiting for the lease to expire"
    );

    producer.await.unwrap();
    assert_uninterrupted(&t, &module, 300).await;
    let (ok, found) = t.check(&["--complete"]);
    assert!(ok, "{found}");
    let claimed: Vec<String> = claims(&t).await.into_iter().map(|(_, n)| n).collect();
    assert_eq!(
        claimed,
        ["first", "second"],
        "the drained node claimed again"
    );

    let report = ps(&t.settings, &[]).await.unwrap();
    let node = report.nodes.iter().find(|n| n.id == "first").unwrap();
    assert!(
        node.draining && node.info.is_some() && node.leases == 0,
        "{node:?}"
    );
    let workload = &report.workloads[0];
    assert_eq!(workload.state, Some(State::Running));
    assert!(!workload.holder_draining);
    assert_eq!(workload.inputs[0].lag(), 0, "{workload:?}");
    assert!(first.alive() && second.alive());
}

/// The holder is stopped, drained, and its lease runs out. Its regular read
/// of the drain mark is made to miss it, as when that read has not come
/// round yet after `SIGCONT`, so the read before each claim is what keeps it
/// from taking the workload back when it wakes.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_drained_while_frozen_claims_nothing_when_it_wakes() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..10).map(count)).await;
    let mut frozen = t.start_node("frozen", "freeze-before-append=5,miss-drain-marks=1");
    eventually("the freeze", Duration::from_secs(60), || async {
        frozen.stopped().then_some(())
    })
    .await;
    let (stale, _) = t.lease(ID).await.expect("the frozen node's lease");

    drain(&t, "frozen", false).await;
    eventually("the lease to expire", Duration::from_secs(30), || async {
        t.lease(ID).await.is_none().then_some(())
    })
    .await;
    frozen.resume();
    // Time for several claims: each waits up to a second first.
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert!(frozen.alive(), "the frozen node exited");
    assert_eq!(t.lease(ID).await.map(|(l, _)| l), None);
    let epochs: Vec<u64> = t
        .read_all(OUTPUT)
        .await
        .iter()
        .map(|(_, r)| output::decode(r).unwrap().epoch())
        .collect();
    assert!(
        epochs.iter().all(|&e| e <= stale.epoch),
        "the drained node claimed again: {epochs:?}"
    );

    let _next = t.start_agent();
    assert_uninterrupted(&t, &module, 10).await;
    let (ok, found) = t.check(&["--complete"]);
    assert!(ok, "{found}");
}

/// The only node is drained. Its workload waits, held by nobody, neither
/// failed nor parked, and the node takes it back after `felixd undrain`.
#[tokio::test(flavor = "multi_thread")]
async fn draining_the_last_node_leaves_its_workloads_waiting_until_it_returns() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..10).map(count)).await;
    let mut only = t.start_node("only", "");
    held_by(&t, "only").await;
    eventually("ten outputs", Duration::from_secs(60), || async {
        (t.outputs(OUTPUT).await.len() == 10).then_some(())
    })
    .await;

    let warned = drain(&t, "only", true).await;
    assert!(
        warned.contains("no other node is up and not draining"),
        "{warned}"
    );
    assert_eq!(t.lease(ID).await.map(|(l, _)| l), None);
    t.publish_all(INPUT, (10..20).map(count)).await;
    // Several reads of the mark and chances to claim.
    tokio::time::sleep(Duration::from_secs(4) + LEASE_TTL).await;
    assert_eq!(t.lease(ID).await.map(|(l, _)| l), None);
    assert_eq!(t.spec().await.state, WorkloadState::Running);
    assert_eq!(t.outputs(OUTPUT).await.len(), 10);
    let report = ps(&t.settings, &[]).await.unwrap();
    let workload = &report.workloads[0];
    assert_eq!(workload.state, Some(State::Waiting), "{workload:?}");
    assert_eq!(workload.holder, None);
    assert_eq!(
        (workload.inputs[0].next, workload.inputs[0].lag()),
        (t.felix.tail(INPUT).await.unwrap() - 10, 10),
        "{workload:?}"
    );
    let node = report.nodes.iter().find(|n| n.id == "only").unwrap();
    assert!(
        node.draining && node.info.is_some() && node.leases == 0,
        "{node:?}"
    );
    let (ok, printed, _) = felixd_within(&t, &["ps"], Duration::from_secs(60)).await;
    assert!(ok);
    assert!(
        printed.contains("no node is up and not draining"),
        "{printed}"
    );

    undrain(&t, "only").await;
    held_by(&t, "only").await;
    assert_uninterrupted(&t, &module, 20).await;
    let (ok, found) = t.check(&["--complete"]);
    assert!(ok, "{found}");
    assert!(only.alive());
}

/// No lease on the workload for `secs` seconds.
async fn never_held(t: &TestFelix, secs: u64, why: &str) {
    let until = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < until {
        if let Some((lease, _)) = t.lease(ID).await {
            panic!("{why}: {lease:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// After the node has read its mark, every read is answered as if the
/// broker did not know the `drains` cache, as during a broker restart. That
/// is not "not draining": the node keeps what it read and claims nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_drained_node_claims_nothing_while_the_broker_does_not_know_the_drains_cache() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..5).map(count)).await;
    let _only = t.start_node("only", "drains-gone-after-mark=1");
    held_by(&t, "only").await;
    drain(&t, "only", false).await;
    eventually("the release", Duration::from_secs(30), || async {
        t.lease(ID).await.is_none().then_some(())
    })
    .await;
    never_held(
        &t,
        6,
        "the drained node claimed when its mark read went missing",
    )
    .await;
    assert_eq!(claims(&t).await.len(), 1);
}

/// The node reads its mark before the drain and then waits 10 s before
/// claiming. `drain --wait` must not return while that claim is under way.
#[tokio::test(flavor = "multi_thread")]
async fn drain_wait_covers_a_claim_already_under_way() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    let _only = t.start_node_with_args(
        "only",
        "delay-claim-after-drain-read=10000",
        LEASE_TTL,
        &["--claim-delay-ms", "0"],
    );
    eventually("the node to be up", Duration::from_secs(30), || async {
        t.felix
            .cache_get(names::NODES, "only")
            .await
            .unwrap()
            .map(|_| ())
    })
    .await;
    // Time to reach its read of the mark.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let started = Instant::now();
    drain(&t, "only", true).await;
    let returned = started.elapsed();
    // A claim that wins takes an epoch, however soon the drain releases it.
    let epoch = last_epoch(&t).await;
    tokio::time::sleep(Duration::from_secs(12)).await;
    assert_eq!(
        last_epoch(&t).await,
        epoch,
        "a claim landed after drain --wait returned"
    );
    assert!(
        returned > Duration::from_secs(5),
        "drain --wait returned after {returned:?}, before the claim it covers"
    );
}

/// The last epoch handed out for the workload.
async fn last_epoch(t: &TestFelix) -> Option<u64> {
    let value = t.felix.cache_get(names::EPOCHS, ID).await.unwrap();
    value.map(|v| felixd_core::lease::decode_epoch(&v).unwrap())
}

/// A node that cannot read its drain mark claims nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_that_cannot_read_its_mark_claims_nothing() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..5).map(count)).await;
    let mut blind = t.start_node("blind", "drain-read-error=1");
    never_held(&t, 8, "a node that cannot read its mark claimed").await;
    assert!(blind.alive());
    assert_eq!(t.outputs(OUTPUT).await.len(), 0);
}

/// The holder fails the workload, but the drain lands while it waits to
/// record the failure. Dropping the work drops the failure: it is not
/// recorded after the lease is released, and the next node runs the workload.
#[tokio::test(flavor = "multi_thread")]
async fn a_drain_racing_a_failure_leaves_nothing_recorded_after_the_release() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..5).map(count)).await;
    let _failing = t.start_node("failing", "fail-workload=1,delay-failure-record=8000");
    held_by(&t, "failing").await;
    drain(&t, "failing", false).await;
    eventually("the release", Duration::from_secs(30), || async {
        t.lease(ID).await.is_none().then_some(())
    })
    .await;
    let _next = t.start_node("next", "");
    held_by(&t, "next").await;
    assert_uninterrupted(&t, &module, 5).await;
    // Past the failing node's delay.
    tokio::time::sleep(Duration::from_secs(8)).await;
    assert_eq!(t.spec().await.state, WorkloadState::Running);
    let failures = t
        .read_all(OUTPUT)
        .await
        .iter()
        .filter(|(_, r)| matches!(output::decode(r).unwrap(), Record::Failure { .. }))
        .count();
    assert_eq!(failures, 0);
}
