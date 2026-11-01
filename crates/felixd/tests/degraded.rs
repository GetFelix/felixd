//! A workload stuck on a transient error on one node is recorded as degraded
//! and its lease released, so another node takes it over.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::process::Stdio;
use std::time::Duration;

use common::{ID, INPUT, LEASE_TTL, OUTPUT, assert_uninterrupted, eventually, test_felix};
use felixd_agent::ps::ps;
use felixd_core::class::ErrorClass;
use felixd_core::output::{self, Record};
use felixd_core::spec::WorkloadState;
use felixd_core::status::State;

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

/// The degraded records on the output stream for `spec`, as `(epoch, class,
/// node, detail)`.
async fn degraded(t: &common::TestFelix, record: u64) -> Vec<(u64, ErrorClass, String, String)> {
    let mut found = Vec::new();
    for (_, bytes) in t.read_all(OUTPUT).await {
        if let Ok(Record::Degraded {
            epoch,
            spec,
            class,
            node,
            detail,
        }) = output::decode(&bytes)
            && spec == record
        {
            found.push((
                epoch,
                class,
                String::from_utf8_lossy(node).into_owned(),
                String::from_utf8_lossy(detail).into_owned(),
            ));
        }
    }
    found
}

/// Node `stuck` fails every attempt to run the workload with an error that is
/// retried, so it cannot get anywhere. Past `--stuck-after-ms` it records the
/// workload as degraded and releases the lease. `felixd ps` says so, the
/// spec stays running, and a node without the fault takes the workload and
/// writes every output once.
#[tokio::test(flavor = "multi_thread")]
async fn a_stuck_workload_is_recorded_and_released_and_another_node_takes_it() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..5).map(count)).await;
    let record = t.spec().await.record.expect("a spec record");
    let stuck_after = LEASE_TTL.as_millis().to_string();
    let stuck = t.start_node_with_args(
        "stuck",
        "fail-transient=1",
        LEASE_TTL,
        &[
            "--stuck-after-ms",
            &stuck_after,
            "--degraded-backoff-ms",
            "600000",
        ],
    );

    let (epoch, class, node, detail) =
        eventually("a degraded record", Duration::from_secs(60), || async {
            degraded(&t, record).await.into_iter().next()
        })
        .await;
    assert_eq!(class, ErrorClass::Transient);
    assert_eq!(node, "stuck");
    assert!(detail.contains("a transient failure"), "{detail}");
    eventually(
        "the lease to be released",
        Duration::from_secs(30),
        || async { t.lease(ID).await.is_none().then_some(()) },
    )
    .await;

    let report = ps(&t.settings, &[ID.to_owned()]).await.unwrap();
    let w = &report.workloads[0];
    let shown = match &w.state {
        Some(State::Degraded(shown)) => shown,
        other => panic!("not degraded: {other:?}"),
    };
    assert_eq!(shown.class, "transient");
    assert_eq!(shown.node, "stuck");
    assert!(shown.detail.contains("a transient failure"), "{shown:?}");
    assert_eq!(w.last_epoch, Some(epoch));
    let printed = t.felixd(&["ps"]).stdout(Stdio::piped()).output().unwrap();
    let printed = String::from_utf8_lossy(&printed.stdout).into_owned();
    assert!(
        printed.contains(&format!("{ID}: degraded (transient) on stuck: ")),
        "{printed}"
    );
    // An observation: the spec still says running, and nothing was run.
    assert_eq!(t.spec().await.state, WorkloadState::Running);
    assert!(t.outputs(OUTPUT).await.is_empty(), "the stuck node ran it");

    let next = t.start_node("next", "");
    eventually(
        "the other node to hold it",
        Duration::from_secs(60),
        || async {
            t.lease(ID)
                .await
                .filter(|(lease, _)| lease.owner == next.node_id && lease.epoch > epoch)
        },
    )
    .await;
    assert_uninterrupted(&t, &module, 5).await;
    assert_eq!(
        degraded(&t, record).await.len(),
        1,
        "the stuck node recorded it again, or took the workload back"
    );
    let report = ps(&t.settings, &[ID.to_owned()]).await.unwrap();
    assert_eq!(report.workloads[0].state, Some(State::Running));
    drop(stuck);
}

/// Each attempt on node `slow` appends one output and then fails with an
/// error that is retried, so it takes many attempts, spread over far longer
/// than `--stuck-after-ms`, to finish. Each one steps a new input, which is
/// progress, so the workload is slow and not stuck: no degraded record, no
/// release, and every output once.
#[tokio::test(flavor = "multi_thread")]
async fn a_workload_that_gets_further_on_each_attempt_is_not_stuck() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..6).map(count)).await;
    let record = t.spec().await.record.expect("a spec record");
    let stuck_after = LEASE_TTL.as_millis().to_string();
    let _slow = t.start_node_with_args(
        "slow",
        "fail-after-append=1",
        LEASE_TTL,
        &["--stuck-after-ms", &stuck_after],
    );
    assert_uninterrupted(&t, &module, 6).await;
    assert!(
        degraded(&t, record).await.is_empty(),
        "a workload that progressed was called stuck"
    );
    let (lease, _) = t.lease(ID).await.expect("the node still holds it");
    assert_eq!(lease.owner, "slow");
}

/// `--stuck-after-ms 0` turns the bound off, as a rolling upgrade from a
/// build that cannot read the record needs: the node retries for ever and
/// writes nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_zero_bound_never_records_or_releases() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..3).map(count)).await;
    let record = t.spec().await.record.expect("a spec record");
    let mut off = t.start_node_with_args(
        "off",
        "fail-transient=1",
        LEASE_TTL,
        &["--stuck-after-ms", "0"],
    );
    eventually("its lease", Duration::from_secs(30), || async {
        t.lease(ID).await.filter(|(lease, _)| lease.owner == "off")
    })
    .await;
    // Long past the default bound of a few TTLs and the 2 s a test would use.
    tokio::time::sleep(Duration::from_secs(10)).await;
    assert!(off.alive());
    assert!(degraded(&t, record).await.is_empty());
    let (lease, _) = t.lease(ID).await.expect("it still holds the lease");
    assert_eq!(lease.owner, "off");
}

/// A bound below the lease TTL would call a workload stuck while its lease
/// could still be waiting out a broker change, so the agent refuses to start
/// and says why.
#[tokio::test(flavor = "multi_thread")]
async fn an_agent_refuses_a_stuck_bound_below_the_lease_ttl() {
    let Some(t) = test_felix().await else { return };
    let mut agent = t
        .felixd(&[
            "agent",
            "--lease-ttl-ms",
            "10000",
            "--stuck-after-ms",
            "9999",
        ])
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = agent.try_wait().unwrap() {
            break status;
        }
        if std::time::Instant::now() >= deadline {
            let _ = agent.kill();
            let _ = agent.wait();
            panic!("the agent started with a stuck bound below the lease TTL");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(!status.success());
    let mut said = String::new();
    std::io::Read::read_to_string(&mut agent.stderr.take().unwrap(), &mut said).unwrap();
    assert!(
        said.contains("--stuck-after-ms (9999 ms) must be 0 or at least the lease TTL (10000 ms)"),
        "{said}"
    );
}
