use super::*;
use crate::class::{ErrorClass, OperatorReason};

fn spec(state: WorkloadState, record: Option<u64>) -> WorkloadSpec {
    WorkloadSpec {
        module: "m".into(),
        inputs: vec!["in".into()],
        output: "wl.w.out".into(),
        snapshot_every: 10,
        snapshot_every_ms: None,
        keep_snapshots: None,
        restart_intensity: None,
        upgrades: Vec::new(),
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

#[test]
fn a_degraded_record_for_this_spec_record_shows_until_the_next_claim() {
    let running = spec(WorkloadState::Running, Some(3));
    let ours = output::encode_degraded(2, 3, ErrorClass::Transient, "node-a", "the broker is gone");
    let (claimed, held) = (Lease::claim("b"), Lease::held("b", 3));
    let found = |lease, last| Found {
        spec: &running,
        lease,
        last_record: last,
    };
    let degraded = State::Degraded(Degraded {
        class: "transient".into(),
        node: "node-a".into(),
        detail: "the broker is gone".into(),
    });
    assert_eq!(state(found(None, Some(&ours))), degraded);
    assert_eq!(
        state(found(None, Some(&ours))).to_string(),
        "degraded (transient) on node-a: the broker is gone"
    );
    // A node that holds the lease but has written nothing yet changes nothing.
    assert_eq!(state(found(Some(&claimed), Some(&ours))), degraded);
    assert_eq!(state(found(Some(&held), Some(&ours))), degraded);
    // Its claim is the last record once it has run.
    let claim = output::encode_claim_under(3, 3, 3, "b");
    assert_eq!(state(found(Some(&held), Some(&claim))), State::Running);
    // A record for an earlier spec record was cleared by the new one.
    let older = output::encode_degraded(2, 1, ErrorClass::Transient, "node-a", "x");
    assert_eq!(state(found(None, Some(&older))), State::Waiting);
}

#[test]
fn a_class_this_build_cannot_read_still_shows_as_degraded() {
    let mut newer = output::encode_degraded(1, 7, ErrorClass::Transient, "node-a", "x");
    newer[18] = 99;
    assert!(degraded(&newer, 7).is_some());
    assert_eq!(degraded(&newer, 8), None);
    assert_eq!(degraded(&output::encode_claim(1, "n"), 7), None);
}

#[test]
fn degraded_comes_after_the_spec_state_and_a_needs_operator_record() {
    let degraded_record = output::encode_degraded(2, 3, ErrorClass::Transient, "node-a", "x");
    let mut failed = spec(WorkloadState::Failed, Some(3));
    failed.failure = Some(Failure {
        class: ErrorClass::Poison,
        detail: "trap".into(),
    });
    let found = Found {
        spec: &failed,
        lease: None,
        last_record: Some(&degraded_record),
    };
    assert_eq!(state(found).to_string(), "failed (poison): trap");
    let stopped = spec(WorkloadState::Stopped, Some(3));
    assert_eq!(
        state(Found {
            spec: &stopped,
            ..found
        }),
        State::Stopped
    );
    // Only the last record counts, so one parks the spec or the other shows.
    let parks = output::encode_needs_operator(2, 3, OperatorReason::ModuleMissing, "");
    let running = spec(WorkloadState::Running, Some(3));
    let found = Found {
        spec: &running,
        lease: None,
        last_record: Some(&parks),
    };
    assert!(matches!(state(found), State::NeedsOperator(_)));
    let found = Found {
        last_record: Some(&degraded_record),
        ..found
    };
    assert!(matches!(state(found), State::Degraded(_)));
}

fn divergence_record(spec: u64, epoch: u64) -> Vec<u8> {
    use crate::output::{Abi, Diverged, OutputHeader, encode_divergence};
    encode_divergence(
        epoch,
        &Diverged {
            spec,
            header: OutputHeader {
                input_offset: 4,
                index: 1,
            },
            snapshot: Some(2),
            node: b"node-a",
            abi: Abi {
                contract: b"0.2.0",
                wasmtime: b"50.0.0",
                felixd: b"1.2.3",
            },
            expected: Some(&[0xab; 32]),
            actual: None,
            detail: b"replayed output differs",
        },
    )
}

fn recent(records: &[Vec<u8>], spec: u64) -> Option<Diverged> {
    divergence(records.iter().map(Vec::as_slice), spec)
}

#[test]
fn the_divergence_behind_a_failure_is_shown() {
    let records = [
        output::encode_claim_under(2, 3, 3, "node-a"),
        divergence_record(3, 2),
        output::encode_failure(2, 7, ErrorClass::Poison, "replayed output differs"),
    ];
    let found = recent(&records, 3).expect("the divergence record");
    assert_eq!(found.node, "node-a");
    assert_eq!((found.epoch, found.input_offset, found.index), (2, 4, 1));
    assert_eq!(found.snapshot, Some(2));
    assert_eq!(found.expected, Some("ab".repeat(32)));
    assert_eq!(found.actual, None);
    assert_eq!(
        found.to_string(),
        format!(
            "diverged at input 4 output 1 on node-a at epoch 2: the stream holds sha256 {}, \
             the replay produced nothing that compares, started from the snapshot of input 2, \
             contract 0.2.0, wasmtime 50.0.0, felixd 1.2.3: replayed output differs",
            "ab".repeat(32)
        )
    );
    // Another spec record's divergence is not this one's.
    assert_eq!(recent(&records, 4), None);
}

/// Anything but a failure, a needs-operator or a degraded record between
/// the divergence and the end means a holder has run since.
#[test]
fn a_divergence_a_later_holder_has_run_past_is_not_shown() {
    let failure = output::encode_failure(2, 7, ErrorClass::Poison, "x");
    let degraded = output::encode_degraded(2, 3, ErrorClass::Transient, "n", "x");
    let parked = output::encode_needs_operator(2, 3, OperatorReason::ModuleMissing, "x");
    let records = [
        divergence_record(3, 2),
        degraded.clone(),
        parked.clone(),
        failure.clone(),
    ];
    assert!(recent(&records, 3).is_some());
    for between in [
        output::encode_claim_under(3, 3, 3, "node-b"),
        output::encode(
            OutputHeader {
                input_offset: 5,
                index: 0,
            },
            3,
            b"x",
        ),
    ] {
        let records = [divergence_record(3, 2), between, failure.clone()];
        assert_eq!(recent(&records, 3), None);
    }
    assert_eq!(recent(&[], 3), None);
}

#[test]
fn a_divergence_record_this_build_cannot_read_still_shows() {
    let mut record = divergence_record(3, 2);
    record[30] = 2;
    assert!(output::decode(&record).is_err());
    let failure = output::encode_failure(2, 7, ErrorClass::Poison, "x");
    let found = recent(&[record.clone(), failure.clone()], 3).expect("shown");
    assert!(found.detail.contains("cannot read"), "{found:?}");
    assert_eq!((found.epoch, found.input_offset, found.index), (2, 4, 1));
    assert!(found.to_string().starts_with("diverged at input"));
    assert_eq!(recent(&[record, failure], 4), None);
}

#[test]
fn only_a_note_a_divergence_can_sit_behind_is_worth_a_look() {
    assert!(may_follow_divergence(&output::encode_failure(
        1,
        2,
        ErrorClass::Poison,
        "x"
    )));
    assert!(may_follow_divergence(&divergence_record(3, 2)));
    let mut unreadable = divergence_record(3, 2);
    unreadable[30] = 2;
    assert!(may_follow_divergence(&unreadable));
    assert!(!may_follow_divergence(&output::encode_claim_under(
        1, 2, 2, "n"
    )));
    assert!(!may_follow_divergence(b"junk"));
}
