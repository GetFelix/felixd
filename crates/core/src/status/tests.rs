use super::*;
use crate::class::{ErrorClass, OperatorReason};

fn spec(state: WorkloadState, record: Option<u64>) -> WorkloadSpec {
    WorkloadSpec {
        module: "m".into(),
        inputs: vec!["in".into()],
        output: "wl.w.out".into(),
        snapshot_every: 10,
        snapshot_every_ms: None,
        state,
        limits: Default::default(),
        failure: None,
        record,
    }
}

#[test]
fn the_lease_says_whether_a_running_workload_is_held() {
    let running = spec(WorkloadState::Running, Some(3));
    let found = |lease| Found {
        spec: &running,
        lease,
        last_record: None,
    };
    let (claimed, held) = (Lease::claim("a"), Lease::held("a", 4));
    assert_eq!(state(found(None)), State::Waiting);
    assert_eq!(state(found(Some(&claimed))), State::Claiming);
    assert_eq!(state(found(Some(&held))), State::Running);
}

#[test]
fn a_needs_operator_record_for_this_spec_record_parks_it_whoever_holds_it() {
    let running = spec(WorkloadState::Running, Some(3));
    let ours = output::encode_needs_operator(2, 3, OperatorReason::ModuleMissing, "gone");
    let held = Lease::held("a", 2);
    let found = Found {
        spec: &running,
        lease: Some(&held),
        last_record: Some(&ours),
    };
    assert_eq!(
        state(found),
        State::NeedsOperator(Parked {
            reason: "module missing".into(),
            detail: "gone".into(),
        })
    );
    // A record for an earlier spec record was cleared by the new one.
    let older = output::encode_needs_operator(2, 1, OperatorReason::ModuleMissing, "gone");
    let found = Found {
        last_record: Some(&older),
        ..found
    };
    assert_eq!(state(found), State::Running);
}

#[test]
fn a_reason_this_build_cannot_read_still_parks_its_spec() {
    let mut newer = output::encode_needs_operator(1, 7, OperatorReason::InputBehind, "x");
    newer[18..20].copy_from_slice(&u16::MAX.to_be_bytes());
    assert!(parked(&newer, 7).is_some());
    assert_eq!(parked(&newer, 8), None);
    assert_eq!(parked(&output::encode_claim(1, "n"), 7), None);
}

#[test]
fn the_spec_state_comes_first() {
    let mut failed = spec(WorkloadState::Failed, Some(3));
    failed.failure = Some(Failure {
        class: ErrorClass::Poison,
        detail: "trap".into(),
    });
    let parks = output::encode_needs_operator(2, 3, OperatorReason::ModuleMissing, "");
    let found = Found {
        spec: &failed,
        lease: None,
        last_record: Some(&parks),
    };
    assert_eq!(state(found).to_string(), "failed (poison): trap");
    let stopped = spec(WorkloadState::Stopped, Some(3));
    let found = Found {
        spec: &stopped,
        ..found
    };
    assert_eq!(state(found), State::Stopped);
}

#[test]
fn the_next_step_is_past_the_later_of_the_last_output_and_the_snapshot() {
    let written = |input_offset| OutputHeader {
        input_offset,
        index: 0,
    };
    let snap = |input_offset| SnapshotHeader {
        input_offset,
        output_tail: 0,
    };
    assert_eq!(next_step(None, None), 0);
    assert_eq!(next_step(Some(written(4)), None), 5);
    assert_eq!(next_step(Some(written(4)), Some(snap(9))), 10);
    assert_eq!(next_step(Some(written(12)), Some(snap(9))), 13);
}
