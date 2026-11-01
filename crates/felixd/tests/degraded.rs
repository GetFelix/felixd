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
/// detail)`.
async fn degraded(t: &common::TestFelix, record: u64) -> Vec<(u64, ErrorClass, String)> {
    let mut found = Vec::new();
    for (_, bytes) in t.read_all(OUTPUT).await {
        if let Ok(Record::Degraded {
            epoch,
            spec,
            class,
            detail,
        }) = output::decode(&bytes)
            && spec == record
        {
            found.push((epoch, class, String::from_utf8_lossy(detail).into_owned()));
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

    let (epoch, class, detail) =
        eventually("a degraded record", Duration::from_secs(60), || async {
            degraded(&t, record).await.into_iter().next()
        })
        .await;
    assert_eq!(class, ErrorClass::Transient);
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
    assert!(shown.detail.contains("a transient failure"), "{shown:?}");
    assert_eq!(w.last_epoch, Some(epoch));
    let printed = t.felixd(&["ps"]).stdout(Stdio::piped()).output().unwrap();
    let printed = String::from_utf8_lossy(&printed.stdout).into_owned();
    assert!(
        printed.contains(&format!("{ID}: degraded (transient)")),
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

/// A bound below the lease TTL would call a workload stuck while its lease
/// could still be waiting out a broker change, so the agent refuses to start.
#[tokio::test(flavor = "multi_thread")]
async fn an_agent_refuses_a_stuck_bound_below_the_lease_ttl() {
    let Some(t) = test_felix().await else { return };
    let status = t
        .felixd(&[
            "agent",
            "--lease-ttl-ms",
            "10000",
            "--stuck-after-ms",
            "9999",
        ])
        .status()
        .unwrap();
    assert!(!status.success());
}
