use super::*;
use crate::output::{self, Addressed, OutputHeader};
use crate::spec::Limits;

fn spec(id: &str) -> WorkloadSpec {
    WorkloadSpec {
        module: "ab".repeat(32),
        inputs: vec!["in".to_owned()],
        output: names::output_stream(id),
        snapshot_every: 10,
        snapshot_every_ms: None,
        keep_snapshots: None,
        state: WorkloadState::Running,
        limits: Limits::default(),
        failure: None,
        record: None,
    }
}

fn request(id: &str) -> Request {
    Request::new(id, spec(id), 1000, 1)
}

fn decided(request: &Request) -> Decision {
    decide(&Policy::default(), 7, &request.encode())
}

fn rejected(decision: &Decision) -> Reason {
    match decision.outcome {
        Outcome::Rejected { reason, .. } => reason,
        Outcome::Accepted { .. } => panic!("accepted {decision:?}"),
    }
}

#[test]
fn a_valid_request_is_accepted_with_its_spec() {
    let decision = decided(&request("wl"));
    assert_eq!(decision.request, 7);
    assert_eq!(decision.id, "wl");
    assert_eq!(decision.accepted(), Some(&spec("wl")));
    assert_eq!(Decision::decode(&decision.encode()).unwrap(), decision);
}

#[test]
fn every_request_gets_one_decision_even_unreadable() {
    let decision = decide(&Policy::default(), 3, b"not json");
    assert_eq!(decision.request, 3);
    assert_eq!(rejected(&decision), Reason::Unreadable);
    let mut future = request("wl");
    future.format = 2;
    assert_eq!(
        rejected(&decide(&Policy::default(), 0, &future.encode())),
        Reason::Unreadable
    );
}

#[test]
fn the_same_request_and_policy_decide_the_same() {
    let bytes = request("wl").encode();
    let policy = Policy::default();
    assert_eq!(
        decide(&policy, 1, &bytes).encode(),
        decide(&policy, 1, &bytes).encode()
    );
}

#[test]
fn reserved_and_invalid_ids_are_rejected() {
    let mut own = request("wl");
    own.id = names::ADMISSION.to_owned();
    own.spec.output = names::output_stream(names::ADMISSION);
    assert_eq!(rejected(&decided(&own)), Reason::Reserved);
    let mut bad = request("wl");
    bad.id = "Bad Id".to_owned();
    assert_eq!(rejected(&decided(&bad)), Reason::InvalidId);
}

#[test]
fn system_streams_and_admissions_are_no_admitted_workloads_input() {
    let decisions = names::output_stream(names::ADMISSION);
    for input in [
        names::INTAKE,
        names::SPECS,
        names::FACTS,
        decisions.as_str(),
    ] {
        let mut request = request("wl");
        request.spec.inputs = vec![input.to_owned()];
        assert_eq!(
            rejected(&decided(&request)),
            Reason::InvalidInputs,
            "{input}"
        );
    }
}

#[test]
fn system_workloads_are_refused_through_intake() {
    let mut scheduler = request(names::SCHEDULER);
    scheduler.spec.inputs = vec![names::SPECS.to_owned(), names::FACTS.to_owned()];
    assert_eq!(rejected(&decided(&scheduler)), Reason::Reserved);
}

#[test]
fn a_spec_a_submitter_may_not_write_is_rejected() {
    let mut output = request("wl");
    output.spec.output = "elsewhere".to_owned();
    let mut failed = request("wl");
    failed.spec.state = WorkloadState::Failed;
    let mut recorded = request("wl");
    recorded.spec.record = Some(3);
    let mut module = request("wl");
    module.spec.module = "../etc".to_owned();
    let mut zero = request("wl");
    zero.spec.snapshot_every = 0;
    for request in [output, failed, recorded, module, zero] {
        assert_eq!(rejected(&decided(&request)), Reason::InvalidSpec);
    }
}

#[test]
fn a_stop_is_accepted() {
    let mut stop = request("wl");
    stop.spec.state = WorkloadState::Stopped;
    assert!(decided(&stop).accepted().is_some());
}

#[test]
fn limits_over_the_policy_are_rejected() {
    let policy = Policy::default();
    let mut fuel = request("wl");
    fuel.spec.limits.fuel_per_call = policy.max_fuel_per_call + 1;
    let mut memory = request("wl");
    memory.spec.limits.max_memory_bytes = policy.max_memory_bytes + 1;
    let mut snapshot = request("wl");
    snapshot.spec.limits.snapshot_fuel = policy.max_snapshot_fuel + 1;
    let mut replicas = request("wl");
    replicas.replication_factor = 0;
    for request in [fuel, memory, snapshot, replicas] {
        assert_eq!(rejected(&decided(&request)), Reason::OverLimit);
    }
    let mut module = request("wl");
    module.module_bytes = policy.max_module_bytes + 1;
    assert_eq!(rejected(&decided(&module)), Reason::ModuleTooLarge);
}

#[test]
fn a_stricter_policy_decides_a_past_request_differently() {
    let bytes = request("wl").encode();
    let strict = Policy {
        max_fuel_per_call: 1,
        ..Policy::default()
    };
    assert!(decide(&Policy::default(), 0, &bytes).accepted().is_some());
    assert!(decide(&strict, 0, &bytes).accepted().is_none());
}

fn on_stream(decision: &Decision) -> Vec<u8> {
    let header = OutputHeader {
        input_offset: decision.request,
        index: 0,
    };
    let payload = decision.encode();
    output::encode_addressed(
        header,
        1,
        Addressed {
            name: "out",
            key: Some(decision.id.as_bytes()),
            payload: &payload,
        },
    )
    .unwrap()
}

#[test]
fn only_an_accepting_decision_for_exactly_the_spec_admits() {
    let accepted = on_stream(&decided(&request("wl")));
    assert!(admits(&accepted, "wl", &spec("wl")).is_ok());
    assert!(admits(&accepted, "other", &spec("wl")).is_err());
    let mut changed = spec("wl");
    changed.limits.fuel_per_call += 1;
    assert!(admits(&accepted, "wl", &changed).is_err());
    let mut over = request("wl");
    over.spec.limits.fuel_per_call = u64::MAX;
    let refused = on_stream(&decided(&over));
    assert!(admits(&refused, "wl", &over.spec).is_err());
    assert!(admits(&output::encode_claim(1, "n"), "wl", &spec("wl")).is_err());
    assert!(admits(b"garbage", "wl", &spec("wl")).is_err());
}

#[test]
fn records_after_admissions_first_need_a_decision() {
    assert!(!needs_decision("wl", 5, None));
    assert!(!needs_decision("wl", 5, Some(9)));
    assert!(needs_decision("wl", 10, Some(9)));
    assert!(!needs_decision(names::ADMISSION, 10, Some(9)));
    assert!(!needs_decision(names::SCHEDULER, 10, Some(9)));
}
