//! `felixd agent --metrics-bind` reports what the node does.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::net::TcpListener;
use std::time::Duration;

use common::{ID, INPUT, eventually, replication_factor, test_felix};

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

/// A port nothing listens on, for the agent to bind.
fn free_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

/// The value of the series that starts with `series`, from a scrape of `addr`.
async fn scrape(addr: &str, series: &str) -> Option<f64> {
    let url = format!("http://{addr}/metrics");
    let body = tokio::task::spawn_blocking(move || {
        ureq::get(&url)
            .call()
            .ok()?
            .body_mut()
            .read_to_string()
            .ok()
    })
    .await
    .unwrap()?;
    body.lines()
        .find(|l| l.starts_with(series))
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
}

async fn reaches(addr: &str, series: &str, at_least: f64) -> f64 {
    eventually(series, Duration::from_secs(60), || async {
        scrape(addr, series).await.filter(|&v| v >= at_least)
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_reports_its_claims_steps_outputs_lag_snapshots_and_drain() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &["--snapshot-every", "5"]).await;
    let addr = free_addr();
    let _agent = t.start_node_with_args(
        "metrics-node",
        "",
        Duration::from_secs(2),
        &["--metrics-bind", &addr],
    );
    reaches(&addr, "felixd_lease_claims_total{result=\"won\"}", 1.0).await;
    assert_eq!(scrape(&addr, "felixd_leases_held").await, Some(1.0));
    assert_eq!(scrape(&addr, "felixd_node_draining").await, Some(0.0));

    t.publish_all(INPUT, (0..20).map(count)).await;
    let steps = format!("felixd_steps_total{{workload=\"{ID}\"}}");
    reaches(&addr, &steps, 20.0).await;
    reaches(
        &addr,
        &format!("felixd_output_records_total{{workload=\"{ID}\"}}"),
        20.0,
    )
    .await;
    reaches(
        &addr,
        &format!("felixd_step_duration_seconds_count{{workload=\"{ID}\"}}"),
        20.0,
    )
    .await;
    reaches(&addr, "felixd_snapshots_total{result=\"written\"}", 1.0).await;
    reaches(
        &addr,
        &format!("felixd_snapshot_bytes{{workload=\"{ID}\"}}"),
        1.0,
    )
    .await;
    // Caught up, as of a tail read within the sampling interval.
    let lag = format!("felixd_input_lag_records{{workload=\"{ID}\",input=\"{INPUT}\"}}");
    eventually("the lag to be read", Duration::from_secs(30), || async {
        scrape(&addr, &lag).await.filter(|&v| v == 0.0)
    })
    .await;
    assert_eq!(
        scrape(&addr, "felixd_lease_handoffs_total{reason=\"drained\"}").await,
        Some(0.0)
    );

    let rf = replication_factor().to_string();
    let drained = t
        .felixd(&[
            "drain",
            "metrics-node",
            "--wait",
            "--replication-factor",
            &rf,
        ])
        .status()
        .unwrap();
    assert!(drained.success());
    reaches(&addr, "felixd_node_draining", 1.0).await;
    reaches(
        &addr,
        "felixd_lease_handoffs_total{reason=\"drained\"}",
        1.0,
    )
    .await;
    eventually("no lease held", Duration::from_secs(30), || async {
        scrape(&addr, "felixd_leases_held")
            .await
            .filter(|&v| v == 0.0)
    })
    .await;
}
