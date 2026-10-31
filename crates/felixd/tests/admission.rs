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
use felixd_core::admission::{Decision, Outcome, Policy, Reason, Request};
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
    let offset = append_record(t, &record).await;
    let mut spec = record.spec;
    spec.record = Some(offset);
    t.put_cache_spec(&spec).await;
    offset
}

/// Appends `record` to `sys.specs` alone.
async fn append_record(t: &TestFelix, record: &SpecRecord) -> u64 {
    let payload = record.encode();
    loop {
        let tail = t.felix.tail(names::SPECS).await.unwrap();
        let sent = t
            .felix
            .publish_if(names::SPECS, vec![payload.clone()], tail)
            .await
            .unwrap();
        if let ConditionalWrite::Written { offset } = sent {
            break offset;
        }
    }
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

#[tokio::test(flavor = "multi_thread")]
async fn a_forged_record_naming_a_huge_decision_blocks_nothing() {
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
    let mut forged = running.clone();
    forged.admitted = Some(u64::MAX);
    append_record(&t, &forged).await;

    // A stop through admission still takes effect.
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

    // And so does running it again: the forged record is not counted as a
    // later decision.
    assert!(t.run(ID, &counter, INPUT, &[]).success());
    t.publish_all(INPUT, (5..10).map(count)).await;
    eventually(
        "the outputs after the restart",
        Duration::from_secs(60),
        || async { (t.outputs(OUTPUT).await.len() >= 10).then_some(()) },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pointing_workloads_back_at_an_old_acceptance_after_a_stop_never_runs() {
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
    let (at, running) = records(&t)
        .await
        .into_iter()
        .rfind(|(_, r)| r.id == ID)
        .unwrap();

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

    // Nothing appended: `workloads` points back at the running record, which
    // names an accepting decision older than the stop's.
    let mut spec = running.spec;
    spec.record = Some(at);
    t.put_cache_spec(&spec).await;
    t.publish_all(INPUT, (5..10).map(count)).await;
    tokio::time::sleep(3 * LEASE_TTL).await;
    assert_eq!(
        t.outputs(OUTPUT).await.len(),
        5,
        "pointing workloads back at an old acceptance ran the workload after its stop"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_record_naming_a_decision_without_admission_slows_nothing() {
    let Some(t) = test_felix().await else { return };
    ensure_stream(&t.settings, INPUT, kind()).await.unwrap();
    // No `felixd init`, so there is no wl.admission.out. A first workload
    // creates sys.specs.
    let counter = felixd_testkit::workload("counter");
    assert!(t.run("first", &counter, INPUT, &[]).success());
    let mut odd = SpecRecord::new(
        "odd",
        admission::running("odd", &module::hash(&counter), vec![INPUT.to_owned()]),
    );
    odd.admitted = Some(0);
    append_record(&t, &odd).await;

    let _a = t.start_agent();
    assert!(t.run(ID, &counter, INPUT, &[]).success());
    t.publish_all(INPUT, (0..5).map(count)).await;
    eventually("the outputs", Duration::from_secs(60), || async {
        (t.outputs(OUTPUT).await.len() >= 5).then_some(())
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_skipped_acceptance_stays_skipped_after_the_projector_restarts() {
    let Some(t) = test_felix().await else { return };
    init(&t).await;
    let a = t.start_agent();
    admission_held_by(&t, &a.node_id).await;

    // Accepted, then skipped by the projector with a skip record: its module
    // is not uploaded.
    let window = felixd_testkit::workload("window");
    let spec = admission::running("skipped", &module::hash(&window), vec![INPUT.to_owned()]);
    let request = Request::new("skipped", spec, window.len() as u64, replication_factor());
    t.felix
        .publish(names::INTAKE, request.encode())
        .await
        .unwrap();
    eventually("the decision", Duration::from_secs(60), || async {
        let found = decisions_for(&t, "skipped").await;
        found.first().map(|(_, d)| {
            assert!(matches!(d.outcome, Outcome::Accepted { .. }), "{d:?}");
        })
    })
    .await;
    // A later acceptance is recorded, so the projector has passed the first.
    let counter = felixd_testkit::workload("counter");
    assert!(t.run("later", &counter, INPUT, &[]).success());

    // The module turns up, and another node's projector takes over.
    t.felix
        .cache_put(
            names::MODULES,
            &module::hash(&window),
            window.clone().into(),
        )
        .await
        .unwrap();
    let b = t.start_agent();
    drop(a);
    admission_held_by(&t, &b.node_id).await;
    assert!(t.run("last", &counter, INPUT, &[]).success());
    let skipped: Vec<_> = records(&t)
        .await
        .into_iter()
        .filter(|(_, r)| r.id == "skipped")
        .collect();
    assert!(
        matches!(&skipped[..], [(_, r)] if r.skipped),
        "a restarted projector recorded an acceptance its predecessor skipped: {skipped:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_projector_that_starts_points_every_workload_at_its_newest_record() {
    let Some(t) = test_felix().await else { return };
    init(&t).await;
    let a = t.start_agent();
    admission_held_by(&t, &a.node_id).await;
    let counter = felixd_testkit::workload("counter");
    assert!(t.run(ID, &counter, INPUT, &[]).success());
    let (running_at, running) = records(&t)
        .await
        .into_iter()
        .rfind(|(_, r)| r.id == ID)
        .unwrap();
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
    // Another workload's record is the newest on sys.specs.
    assert!(t.run("other", &counter, INPUT, &[]).success());
    let (stop_at, _) = records(&t)
        .await
        .into_iter()
        .rfind(|(_, r)| r.id == ID)
        .unwrap();
    assert!(stop_at > running_at);

    // A projector that died after appending the stop but before pointing
    // `workloads` at it leaves the older record there.
    let mut spec = running.spec;
    spec.record = Some(running_at);
    t.put_cache_spec(&spec).await;

    let b = t.start_agent();
    drop(a);
    admission_held_by(&t, &b.node_id).await;
    eventually(
        "workloads to name the newest record",
        Duration::from_secs(60),
        || async {
            let found = t.felix.cache_get(names::WORKLOADS, ID).await.unwrap()?;
            let current = WorkloadSpec::decode(&found).unwrap();
            (current.record == Some(stop_at)).then_some(())
        },
    )
    .await;
}
