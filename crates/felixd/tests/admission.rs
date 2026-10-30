//! Admission (#114): a request on `sys.intake` is accepted or rejected by the
//! admission workload, only accepted specs run, a spec record written around
//! admission never runs, a node dying mid-decision or mid-projection leaves
//! each request decided and recorded once, and a replay of admission matches
//! and lists the requests a policy change decides otherwise.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::time::Duration;

use common::{
    ID, INPUT, LEASE_TTL, OUTPUT, SNAPSHOTS, TestFelix, assert_uninterrupted, eventually,
    replication_factor, test_felix,
};
use felix_client::ConditionalWrite;
use felixd_agent::admin::{StreamKind, ensure_stream};
use felixd_agent::admission::{self, Submitted};
use felixd_core::admission::{Decision, Outcome, Policy, Reason};
use felixd_core::spec::{Limits, SpecRecord, WorkloadSpec, WorkloadState};
use felixd_core::{module, names};

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

fn kind() -> StreamKind {
    StreamKind {
        replication_factor: replication_factor(),
    }
}

/// Creates the input stream and runs `felixd init` with the admission
/// workload.
async fn init(t: &TestFelix) {
    ensure_stream(&t.settings, INPUT, kind()).await.unwrap();
    let dir = std::env::temp_dir().join(format!("felixd-{}", t.settings.namespace));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("admission.wasm");
    std::fs::write(&path, felixd_testkit::workload("admission")).unwrap();
    let replicas = replication_factor().to_string();
    let init = |name: &str| {
        let status = t
            .felixd(&["init", "--replication-factor", &replicas])
            .arg(&path)
            .status()
            .unwrap();
        assert!(status.success(), "{name} felixd init failed");
    };
    init("the first");
    let records = t.read_all(names::SPECS).await.len();
    // A second init with the same module changes nothing.
    init("a second");
    assert_eq!(t.read_all(names::SPECS).await.len(), records);
}

/// Admission's decisions, with their offsets on `wl.admission.out`.
async fn decisions(t: &TestFelix) -> Vec<(u64, Decision)> {
    let stream = names::output_stream(names::ADMISSION);
    t.read_all(&stream)
        .await
        .into_iter()
        .filter_map(|(at, r)| {
            felixd_agent::admission::decision_of(&r)
                .ok()
                .map(|d| (at, d))
        })
        .collect()
}

async fn decisions_for(t: &TestFelix, id: &str) -> Vec<(u64, Decision)> {
    let mut found = decisions(t).await;
    found.retain(|(_, d)| d.id == id);
    found
}

/// The spec records on `sys.specs`, with their offsets.
async fn records(t: &TestFelix) -> Vec<(u64, SpecRecord)> {
    t.read_all(names::SPECS)
        .await
        .into_iter()
        .map(|(at, r)| (at, SpecRecord::decode(&r).unwrap()))
        .collect()
}

/// Waits until `node` holds admission's lease.
async fn admission_held_by(t: &TestFelix, node: &str) {
    eventually("admission's lease", Duration::from_secs(60), || async {
        t.lease(names::ADMISSION)
            .await
            .filter(|(lease, _)| lease.owner == node)
            .map(|_| ())
    })
    .await;
}

/// Appends `record` to `sys.specs` and points `workloads` at it, the way a
/// submitter with direct write rights could, skipping admission.
async fn write_around_admission(t: &TestFelix, record: SpecRecord) -> u64 {
    let payload = record.encode();
    let offset = loop {
        let tail = t.felix.tail(names::SPECS).await.unwrap();
        let sent = t
            .felix
            .publish_if(names::SPECS, vec![payload.clone()], tail)
            .await
            .unwrap();
        if let ConditionalWrite::Written { offset } = sent {
            break offset;
        }
    };
    let mut spec = record.spec;
    spec.record = Some(offset);
    t.put_cache_spec(&spec).await;
    offset
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_request_never_runs_and_an_accepted_one_does() {
    let Some(t) = test_felix().await else { return };
    init(&t).await;
    let _a = t.start_agent();
    let counter = felixd_testkit::workload("counter");

    let over = (Policy::default().max_fuel_per_call + 1).to_string();
    let status = t.run("big", &counter, INPUT, &["--fuel", &over]);
    assert_eq!(status.code(), Some(1), "a rejected request exits with 1");
    let [(_, rejected)] = &decisions_for(&t, "big").await[..] else {
        panic!("not one decision for the rejected request");
    };
    assert!(matches!(
        rejected.outcome,
        Outcome::Rejected {
            reason: Reason::OverLimit,
            ..
        }
    ));

    assert!(t.run(ID, &counter, INPUT, &[]).success());
    t.publish_all(INPUT, (0..5).map(count)).await;
    assert_uninterrupted(&t, &counter, 5).await;

    // A system workload's id is refused through intake, whatever it asks:
    // only an operator writes the scheduler's spec.
    let mut spec = admission::running(names::SCHEDULER, &module::hash(&counter), vec![]);
    spec.inputs = vec![names::SPECS.to_owned(), names::FACTS.to_owned()];
    let submitted = admission::request(
        &t.felix,
        names::SCHEDULER,
        counter.clone(),
        spec,
        replication_factor(),
        Duration::from_secs(60),
    )
    .await
    .unwrap();
    assert!(
        matches!(
            submitted,
            Submitted::Rejected {
                reason: Reason::Reserved,
                ..
            }
        ),
        "{submitted}"
    );
    assert!(
        records(&t)
            .await
            .iter()
            .all(|(_, r)| r.id != names::SCHEDULER)
    );

    // Nothing of the rejected request was recorded, so nothing could run it.
    assert!(records(&t).await.iter().all(|(_, r)| r.id != "big"));
    let entry = t.felix.cache_get(names::WORKLOADS, "big").await.unwrap();
    assert!(
        entry.is_none(),
        "the rejected workload has a workloads entry"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_spec_written_around_admission_never_runs() {
    let Some(t) = test_felix().await else { return };
    init(&t).await;
    let _a = t.start_agent();
    let counter = felixd_testkit::workload("counter");
    // A workload through admission, so the node is known to run what it may.
    assert!(t.run("ok", &counter, INPUT, &[]).success());
    let [(accepted, _)] = decisions_for(&t, "ok").await[..] else {
        panic!("not one decision for the accepted request");
    };

    // Workload ID, written straight to sys.specs with its streams created by
    // hand, as `felixd run` did before admission.
    ensure_stream(&t.settings, OUTPUT, kind()).await.unwrap();
    ensure_stream(&t.settings, SNAPSHOTS, kind()).await.unwrap();
    let spec = WorkloadSpec {
        module: module::hash(&counter),
        inputs: vec![INPUT.to_owned()],
        output: OUTPUT.to_owned(),
        snapshot_every: 10,
        snapshot_every_ms: None,
        keep_snapshots: None,
        state: WorkloadState::Running,
        limits: Limits::default(),
        failure: None,
        record: None,
    };
    write_around_admission(&t, SpecRecord::new(ID, spec.clone())).await;
    t.publish_all(INPUT, (0..5).map(count)).await;
    let ok = names::output_stream("ok");
    eventually(
        "the admitted workload's output",
        Duration::from_secs(60),
        || async { (t.outputs(&ok).await.len() >= 5).then_some(()) },
    )
    .await;
    tokio::time::sleep(3 * LEASE_TTL).await;
    assert!(
        t.read_all(OUTPUT).await.is_empty(),
        "a spec admission never saw was claimed or ran"
    );

    // One naming a real decision, which accepted another workload's spec.
    let mut forged = SpecRecord::new(ID, spec.clone());
    forged.admitted = Some(accepted);
    write_around_admission(&t, forged).await;
    tokio::time::sleep(3 * LEASE_TTL).await;
    assert!(
        t.read_all(OUTPUT).await.is_empty(),
        "a spec naming another workload's decision was claimed or ran"
    );

    // One naming a decision that accepted this workload, under another spec:
    // stopped, with other limits.
    let mut stopped = spec.clone();
    stopped.state = WorkloadState::Stopped;
    stopped.limits.fuel_per_call /= 2;
    let submitted = admission::request(
        &t.felix,
        ID,
        counter.clone(),
        stopped,
        replication_factor(),
        Duration::from_secs(60),
    )
    .await
    .unwrap();
    let Submitted::Accepted { decision, .. } = submitted else {
        panic!("{submitted}");
    };
    let mut forged = SpecRecord::new(ID, spec);
    forged.admitted = Some(decision);
    write_around_admission(&t, forged).await;
    tokio::time::sleep(3 * LEASE_TTL).await;
    assert!(
        t.read_all(OUTPUT).await.is_empty(),
        "a spec naming a decision for another spec of the workload was claimed or ran"
    );
}

/// Starts node `a` with `faults`, lets it take admission, starts a healthy
/// node and submits ID. Node `a` dies at the fault; the other takes over.
/// Returns the module, once the submit has returned.
async fn submit_through_a_crash(t: &TestFelix, faults: &str) -> (Vec<u8>, common::Agent) {
    let mut a = t.start_agent_with_faults(faults);
    admission_held_by(t, &a.node_id).await;
    let b = t.start_agent();
    let counter = felixd_testkit::workload("counter");
    let started = std::time::Instant::now();
    assert!(t.run(ID, &counter, INPUT, &[]).success());
    eprintln!(
        "submitted through a takeover in {} ms",
        started.elapsed().as_millis()
    );
    // The record can be read before the aborting process has exited.
    let status = a.exited(Duration::from_secs(10)).await;
    assert!(!status.success(), "node a did not hit {faults}");
    (counter, b)
}

/// Every request on `sys.intake` has exactly one decision, and every accepted
/// one exactly one spec record.
async fn each_decided_and_recorded_once(t: &TestFelix) {
    let requests = t.read_all(names::INTAKE).await;
    let decided = decisions(t).await;
    let records = records(t).await;
    for (request, _) in &requests {
        let found: Vec<_> = decided
            .iter()
            .filter(|(_, d)| d.request == *request)
            .collect();
        assert_eq!(
            found.len(),
            1,
            "request {request} has {} decisions",
            found.len()
        );
        let (at, decision) = found[0];
        let recorded = records
            .iter()
            .filter(|(_, r)| r.admitted == Some(*at))
            .count();
        let want = usize::from(decision.accepted().is_some());
        assert_eq!(recorded, want, "decision {at} has {recorded} spec records");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_dying_mid_decision_leaves_each_request_decided_once() {
    let Some(t) = test_felix().await else { return };
    init(&t).await;
    // Admission's first decision is the first output node a sends.
    let (counter, _b) = submit_through_a_crash(&t, "crash-after-append-sent=1").await;
    each_decided_and_recorded_once(&t).await;
    t.publish_all(INPUT, (0..5).map(count)).await;
    assert_uninterrupted(&t, &counter, 5).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_dying_mid_projection_leaves_each_decision_recorded_once() {
    let Some(t) = test_felix().await else { return };
    init(&t).await;
    // It dies with the spec record sent and `workloads` not yet pointed at
    // it; the next node points it.
    let (counter, _b) = submit_through_a_crash(&t, "crash-after-projection-sent=1").await;
    each_decided_and_recorded_once(&t).await;
    t.publish_all(INPUT, (0..5).map(count)).await;
    assert_uninterrupted(&t, &counter, 5).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replay_of_admission_matches_and_lists_what_a_policy_change_decides_otherwise() {
    let Some(t) = test_felix().await else { return };
    init(&t).await;
    let _a = t.start_agent();
    let counter = felixd_testkit::workload("counter");
    let fuel = Limits::default().fuel_per_call;
    // Accepted under both policies, under the default one only, and under
    // neither.
    assert!(t.run("plain", &counter, INPUT, &[]).success());
    let more = (2 * fuel).to_string();
    assert!(
        t.run("hungry", &counter, INPUT, &["--fuel", &more])
            .success()
    );
    // Over a limit both policies share, so both reject it the same way.
    let over = (Policy::default().max_memory_bytes + 1).to_string();
    let status = t.run("over", &counter, INPUT, &["--max-memory", &over]);
    assert_eq!(status.code(), Some(1));
    each_decided_and_recorded_once(&t).await;

    let (ok, report) = t.replay(&["--id", names::ADMISSION, "--diff"]);
    assert!(ok, "admission's replay diverged: {report}");

    let strict = felixd_testkit::workload("admission-strict");
    let hash = module::hash(&strict);
    t.felix
        .cache_put(names::MODULES, &hash, strict.into())
        .await
        .unwrap();
    let (ok, report) = t.replay(&[
        "--id",
        names::ADMISSION,
        "--diff",
        "--all",
        "--module",
        &hash,
    ]);
    assert!(!ok, "the stricter policy decided every request the same");
    let [(_, hungry)] = &decisions_for(&t, "hungry").await[..] else {
        panic!("not one decision for the request the policies differ on");
    };
    let lines: Vec<&str> = report.lines().collect();
    assert_eq!(lines.len(), 1, "{report}");
    assert!(
        lines[0].starts_with(&format!(
            "first divergence at input {} output 0",
            hungry.request
        )),
        "{report}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_old_acceptance_replayed_after_a_stop_never_runs() {
    let Some(t) = test_felix().await else { return };
    init(&t).await;
    let _a = t.start_agent();
    let counter = felixd_testkit::workload("counter");
    assert!(t.run(ID, &counter, INPUT, &[]).success());
    t.publish_all(INPUT, (0..5).map(count)).await;
    eventually("the first outputs", Duration::from_secs(60), || async {
        (t.outputs(OUTPUT).await.len() >= 5).then_some(())
    })
    .await;
    let (_, running) = records(&t)
        .await
        .into_iter()
        .rfind(|(_, r)| r.id == ID)
        .unwrap();

    // Stopped through admission, a later decision.
    let mut stopped = running.spec.clone();
    stopped.state = WorkloadState::Stopped;
    let submitted = admission::request(
        &t.felix,
        ID,
        counter.clone(),
        stopped,
        replication_factor(),
        Duration::from_secs(60),
    )
    .await
    .unwrap();
    assert!(
        matches!(submitted, Submitted::Accepted { .. }),
        "{submitted}"
    );
    eventually("the workload to stop", Duration::from_secs(30), || async {
        t.lease(ID).await.is_none().then_some(())
    })
    .await;

    // The running record appended again, naming its own, older decision.
    write_around_admission(&t, running).await;
    t.publish_all(INPUT, (5..10).map(count)).await;
    tokio::time::sleep(3 * LEASE_TTL).await;
    assert_eq!(
        t.outputs(OUTPUT).await.len(),
        5,
        "a replayed acceptance ran the workload again after its stop"
    );
}
