use std::time::Duration;

use super::*;

fn metrics(max_workloads: usize) -> Metrics {
    Metrics {
        node: "n\"1".into(),
        max_workloads,
        draining: AtomicBool::new(false),
        leases_held: AtomicI64::new(0),
        claims: Default::default(),
        handoffs: Default::default(),
        needs_operator: Default::default(),
        failures: AtomicU64::new(0),
        snapshots_written: AtomicU64::new(0),
        snapshots_skipped: AtomicU64::new(0),
        snapshot_seconds: Histogram::new(SNAPSHOT_BUCKETS),
        workloads: Mutex::new(BTreeMap::new()),
    }
}

fn line<'a>(text: &'a str, series: &str) -> &'a str {
    text.lines()
        .find(|l| l.starts_with(series))
        .unwrap_or_else(|| panic!("no series {series} in\n{text}"))
}

#[test]
fn a_step_moves_the_counter_and_the_histogram() {
    let m = metrics(10);
    let (w, shared) = m.workload("counter");
    assert!(!shared);
    w.step(Duration::from_millis(3));
    w.step(Duration::from_secs(20));
    let text = m.render();
    assert_eq!(
        line(&text, "felixd_steps_total{workload=\"counter\"}"),
        "felixd_steps_total{workload=\"counter\"} 2"
    );
    let bucket = |le: &str| {
        line(
            &text,
            &format!("felixd_step_duration_seconds_bucket{{workload=\"counter\",le=\"{le}\"}}"),
        )
        .rsplit(' ')
        .next()
        .unwrap()
        .to_owned()
    };
    assert_eq!(bucket("0.0005"), "0");
    assert_eq!(bucket("0.005"), "1");
    assert_eq!(bucket("5"), "1");
    assert_eq!(bucket("+Inf"), "2");
    assert!(
        line(
            &text,
            "felixd_step_duration_seconds_count{workload=\"counter\"} 2"
        )
        .ends_with(" 2")
    );
}

#[test]
fn outputs_refusals_merges_and_restarts_are_counted_per_workload() {
    let m = metrics(10);
    let (w, _) = m.workload("a");
    w.output_written(3);
    w.output_refused(false);
    w.output_refused(true);
    w.output_refused(true);
    w.merged(5);
    w.restarted();
    w.snapshot_size(42);
    let text = m.render();
    for expected in [
        "felixd_output_records_total{workload=\"a\"} 3",
        "felixd_output_refused_total{workload=\"a\",result=\"reconciled\"} 1",
        "felixd_merge_log_appends_total{workload=\"a\"} 5",
        "felixd_workload_restarts_total{workload=\"a\"} 1",
        "felixd_output_refused_total{workload=\"a\",result=\"fenced\"} 2",
        "felixd_workload_snapshot_bytes{workload=\"a\"} 42",
    ] {
        assert!(
            text.lines().any(|l| l == expected),
            "{expected} missing from\n{text}"
        );
    }
}

#[test]
fn lag_is_the_tail_past_the_next_input_and_hidden_until_the_tail_is_read() {
    let m = metrics(10);
    let (w, _) = m.workload("a");
    w.set_inputs(&["in".to_owned(), "other".to_owned()], false);
    let inputs = w.inputs();
    WorkloadMetrics::position(&inputs, 0, 4);
    assert!(
        !m.render().contains("felixd_input_lag_records{"),
        "{}",
        m.render()
    );
    WorkloadMetrics::tail(&inputs, 0, 10);
    WorkloadMetrics::position(&inputs, 0, 7);
    let text = m.render();
    assert!(
        text.contains("felixd_input_lag_records{workload=\"a\",input=\"in\"} 3"),
        "{text}"
    );
    assert!(!text.contains("input=\"other\""), "{text}");
    // Caught up and past a tail read earlier.
    WorkloadMetrics::position(&inputs, 0, 12);
    assert!(m.render().contains("input=\"in\"} 0"));
}

#[test]
fn workloads_past_the_limit_share_one_series() {
    let m = metrics(2);
    let (a, _) = m.workload("a");
    let (b, _) = m.workload("b");
    let (c, c_shared) = m.workload("c");
    let (d, _) = m.workload("d");
    assert!(c_shared);
    c.step(Duration::ZERO);
    d.step(Duration::ZERO);
    a.step(Duration::ZERO);
    assert!(Arc::ptr_eq(&c, &d));
    assert!(!Arc::ptr_eq(&a, &b));
    // A named workload still finds its own after the limit was hit.
    assert!(Arc::ptr_eq(&m.workload("a").0, &a));
    let text = m.render();
    assert!(
        text.contains("felixd_steps_total{workload=\"_other\"} 2"),
        "{text}"
    );
    assert!(
        text.contains("felixd_steps_total{workload=\"a\"} 1"),
        "{text}"
    );
    assert!(!text.contains("workload=\"c\"") && !text.contains("workload=\"d\""));
    c.set_inputs(&["in".into()], true);
    assert!(c.inputs().is_empty());
}

#[test]
fn node_series_have_a_fixed_label_set_and_escape_the_node_name() {
    let m = metrics(1);
    m.claims[Claim::Won as usize].fetch_add(2, Ordering::Relaxed);
    m.claims[Claim::Contended as usize].fetch_add(1, Ordering::Relaxed);
    m.handoffs[Handoff::Drained as usize].fetch_add(1, Ordering::Relaxed);
    m.needs_operator[2].fetch_add(1, Ordering::Relaxed);
    m.draining.store(true, Ordering::Relaxed);
    m.leases_held.store(2, Ordering::Relaxed);
    m.snapshots_written.fetch_add(1, Ordering::Relaxed);
    m.snapshot_seconds.observe(Duration::from_millis(30));
    let text = m.render();
    for expected in [
        "felixd_lease_claims_total{result=\"won\"} 2",
        "felixd_lease_claims_total{result=\"contended\"} 1",
        "felixd_lease_handoffs_total{reason=\"drained\"} 1",
        "felixd_needs_operator_total{reason=\"spec_unrecorded\"} 1",
        "felixd_needs_operator_total{reason=\"module_missing\"} 0",
        "felixd_node_draining 1",
        "felixd_leases_held 2",
        "felixd_snapshots_total{result=\"written\"} 1",
        "felixd_snapshot_duration_seconds_count 1",
        "felixd_snapshot_duration_seconds_bucket{le=\"0.1\"} 1",
    ] {
        assert!(
            text.lines().any(|l| l == expected),
            "{expected} missing from\n{text}"
        );
    }
    assert!(text.contains("node=\"n\\\"1\""), "{text}");
    for l in text.lines().filter(|l| !l.starts_with('#')) {
        assert!(l.starts_with("felixd_"), "{l}");
    }
}

#[test]
fn holding_counts_the_lease_until_dropped_and_forgets_the_inputs() {
    let m = enable("holding-test", 10);
    let before = m.leases_held.load(Ordering::Relaxed);
    let workload = workload("held");
    let held = Holding::start(&workload);
    let (w, _) = workload.clone().unwrap();
    w.set_inputs(&["in".into()], false);
    assert_eq!(m.leases_held.load(Ordering::Relaxed), before + 1);
    assert!(
        m.render()
            .contains("felixd_workload_held{workload=\"held\"} 1")
    );
    drop(held);
    assert_eq!(m.leases_held.load(Ordering::Relaxed), before);
    assert!(
        m.render()
            .contains("felixd_workload_held{workload=\"held\"} 0")
    );
    assert!(w.inputs().is_empty());
}

#[tokio::test]
async fn the_endpoint_serves_metrics_and_liveness_and_nothing_else() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let addr = start("127.0.0.1:0".parse().unwrap(), "endpoint-test", 10)
        .await
        .unwrap();
    claim(Claim::Won);
    let get = |path: &'static str| async move {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut body = String::new();
        stream.read_to_string(&mut body).await.unwrap();
        body
    };
    let metrics = get("/metrics").await;
    assert!(metrics.starts_with("HTTP/1.1 200 OK"), "{metrics}");
    assert!(metrics.contains("text/plain; version=0.0.4"));
    assert!(metrics.contains("felixd_node_info{node=\""), "{metrics}");
    assert!(get("/live").await.starts_with("HTTP/1.1 200 OK"));
    assert!(get("/other").await.starts_with("HTTP/1.1 404"));
}

#[test]
fn churn_past_the_cap_drops_released_workloads_and_keeps_held_ones() {
    let m = metrics(3);
    let (held, _) = m.workload("held");
    held.held.store(1, Ordering::Relaxed);
    for i in 0..50 {
        let (w, shared) = m.workload(&format!("w{i}"));
        assert!(!shared, "w{i} fell into _other with only idle workloads");
        w.step(Duration::ZERO);
    }
    let text = m.render();
    assert!(
        text.contains("felixd_workload_held{workload=\"held\"} 1"),
        "{text}"
    );
    assert!(text.contains("workload=\"w49\""), "{text}");
    assert!(!text.contains("workload=\"w0\""), "{text}");
    assert!(!text.contains("workload=\"_other\""), "{text}");
    let series = text
        .lines()
        .filter(|l| l.contains("felixd_steps_total{"))
        .count();
    assert_eq!(series, 3);
    // All held: nothing can be dropped.
    let m = metrics(1);
    let (a, _) = m.workload("a");
    a.held.store(1, Ordering::Relaxed);
    assert!(m.workload("b").1);
}

#[test]
fn snapshot_size_is_not_reported_for_the_shared_series() {
    let m = metrics(1);
    let (a, _) = m.workload("a");
    a.held.store(1, Ordering::Relaxed);
    a.snapshot_size(7);
    let (other, shared) = m.workload("b");
    assert!(shared);
    other.snapshot_size(9);
    let text = m.render();
    assert!(
        text.contains("felixd_workload_snapshot_bytes{workload=\"a\"} 7"),
        "{text}"
    );
    assert!(
        !text.contains("felixd_workload_snapshot_bytes{workload=\"_other\""),
        "{text}"
    );
}

#[test]
fn lag_is_dropped_when_tails_stop_being_read_and_a_tail_never_goes_back() {
    let m = metrics(2);
    let (w, _) = m.workload("a");
    w.set_inputs(&["in".to_owned()], false);
    let inputs = w.inputs();
    WorkloadMetrics::tail(&inputs, 0, 10);
    WorkloadMetrics::tail(&inputs, 0, 8);
    assert!(m.render().contains("input=\"in\"} 10"), "{}", m.render());
    WorkloadMetrics::forget_tail(&inputs, 0);
    assert!(!m.render().contains("felixd_input_lag_records{"));
    WorkloadMetrics::tail(&inputs, 0, 3);
    assert!(m.render().contains("input=\"in\"} 3"), "{}", m.render());
}

#[tokio::test]
async fn a_client_that_never_reads_or_never_finishes_does_not_hold_the_endpoint() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let addr = start("127.0.0.1:0".parse().unwrap(), "slow-test", 10)
        .await
        .unwrap();
    // More idle connections than the limit: the extra ones are closed.
    let mut idle = Vec::new();
    for _ in 0..(serve::MAX_CONNECTIONS + 8) {
        idle.push(tokio::net::TcpStream::connect(addr).await.unwrap());
    }
    // A request in two segments and with a query still gets served once
    // there is room, and the endpoint answers while the others sit idle.
    let mut closed = 0;
    for stream in &mut idle {
        let mut byte = [0u8; 1];
        if let Ok(Ok(0)) =
            tokio::time::timeout(Duration::from_millis(20), stream.read(&mut byte)).await
        {
            closed += 1;
        }
    }
    assert!(closed >= 8, "only {closed} connections were refused");
    idle.clear();
    let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
    client.write_all(b"GET /metrics?x=1 HT").await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    client
        .write_all(b"TP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    let mut body = String::new();
    client.read_to_string(&mut body).await.unwrap();
    assert!(body.starts_with("HTTP/1.1 200 OK"), "{body}");
}
