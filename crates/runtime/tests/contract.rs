//! What the host does with each result the 0.2 contract allows or forbids,
//! using the `faulty` fixture.

mod common;

use common::{STREAM, input, outputs, payloads};
use felixd_runtime::{
    FailureKind, Input, Limits, LoadError, Output, Runtime, Stepped, Workload, WorkloadError,
};

fn load(limits: Limits) -> Workload {
    Runtime::new()
        .unwrap()
        .load(&felixd_testkit::workload("faulty"), limits)
        .unwrap()
}

fn failure<T: std::fmt::Debug>(result: Result<T, WorkloadError>) -> felixd_runtime::Failure {
    match result {
        Err(WorkloadError::Failed(f)) => f,
        Ok(out) => panic!("expected a failure, got {out:?}"),
    }
}

#[test]
fn a_rejection_leaves_the_workload_running() {
    let mut wl = load(Limits::default());
    assert_eq!(
        wl.step(input(0, b"reject:42")).unwrap(),
        Stepped::Rejected {
            code: 42,
            detail: "asked to reject".to_owned()
        }
    );
    assert!(wl.failure().is_none());
    assert_eq!(payloads(&mut wl, b"hi").unwrap(), [b"hi".to_vec()]);
}

#[test]
fn poison_fails_the_workload_as_poison() {
    let mut wl = load(Limits::default());
    let failed = failure(wl.step(input(0, b"poison")));
    assert_eq!(failed.kind, FailureKind::Poison);
    assert_eq!(failed.message, "poison: asked to poison");
    assert_eq!(failure(wl.step(input(1, b"hi"))), failed);
}

#[test]
fn an_output_carries_its_name_and_key() {
    let mut wl = load(Limits::default());
    let got = outputs(wl.step(input(0, b"named:alerts:k1:hot")).unwrap());
    assert_eq!(
        got,
        [Output {
            name: "alerts".to_owned(),
            key: Some(b"k1".to_vec()),
            payload: b"hot".to_vec(),
        }]
    );
    let got = outputs(wl.step(input(1, b"named:out::cold")).unwrap());
    assert_eq!(got, [Output::unnamed(b"cold".to_vec())]);
}

#[test]
fn an_invalid_output_name_breaks_the_contract() {
    for name in ["Upper", "", "a.b", &"x".repeat(65)] {
        let mut wl = load(Limits::default());
        let step = format!("named:{name}::p");
        let failed = failure(wl.step(input(0, step.as_bytes())));
        assert_eq!(failed.kind, FailureKind::Contract, "{name:?}");
    }
    let mut wl = load(Limits::default());
    let name = "a".repeat(64);
    let step = format!("named:{name}::p");
    assert_eq!(
        outputs(wl.step(input(0, step.as_bytes())).unwrap())[0].name,
        name
    );
}

#[test]
fn an_effect_request_breaks_the_contract() {
    let mut wl = load(Limits::default());
    let failed = failure(wl.step(input(0, b"request")));
    assert_eq!(failed.kind, FailureKind::Contract);
    assert!(
        failed
            .message
            .contains("this build has no effect executors"),
        "{}",
        failed.message
    );
}

#[test]
fn the_guest_sees_the_metadata_it_is_given() {
    let mut wl = load(Limits::default());
    let meta = Input {
        stream: "sensors",
        incarnation: 0,
        offset: 17,
        timestamp_micros: 1_700_000_000_000_001,
        key: Some(b"\x01\xff"),
        payload: b"meta",
    };
    let got = outputs(wl.step(meta).unwrap());
    assert_eq!(
        got[0].payload,
        b"sensors 0 17 1700000000000001 01ff 6d657461"
    );
    let got = outputs(wl.step(input(3, b"meta")).unwrap());
    assert_eq!(
        String::from_utf8(got[0].payload.clone()).unwrap(),
        format!("{STREAM} 0 3 3000 - 6d657461")
    );
}

#[test]
fn a_snapshot_of_many_chunks_restores_identically() {
    let mut original = load(Limits::default());
    for step in ["chunks:5", "touch:1", "touch:3", "touch:3"] {
        original.step(input(0, step.as_bytes())).unwrap();
    }
    let chunks = original.snapshot().unwrap();
    assert_eq!(chunks.len(), 5);
    let inspect = original.inspect().unwrap();
    assert_eq!(inspect, r#"{"chunks":[0,1,0,2,0]}"#);
    // Taking a snapshot does not change the state.
    assert_eq!(original.snapshot().unwrap(), chunks);

    let mut restored = load(Limits::default());
    let refs: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();
    restored.restore(&refs).unwrap();
    assert_eq!(restored.inspect().unwrap(), inspect);
    for wl in [&mut original, &mut restored] {
        wl.step(input(1, b"touch:4")).unwrap();
    }
    assert_eq!(restored.snapshot().unwrap(), original.snapshot().unwrap());
    // Only the touched chunk changed.
    let after = original.snapshot().unwrap();
    let changed: Vec<usize> = (0..5).filter(|&i| after[i] != chunks[i]).collect();
    assert_eq!(changed, [4]);
}

#[test]
fn a_chunk_over_4_mib_breaks_the_contract() {
    let mut wl = load(Limits::default());
    wl.step(input(0, b"big-chunk")).unwrap();
    let failed = failure(wl.snapshot());
    assert_eq!(failed.kind, FailureKind::Contract);
    assert!(failed.message.contains("4194304"), "{}", failed.message);
}

#[test]
fn more_than_65536_chunks_break_the_contract() {
    let mut wl = load(Limits::default());
    wl.step(input(0, b"chunks:65536")).unwrap();
    assert_eq!(wl.snapshot().unwrap().len(), 65_536);
    wl.step(input(1, b"chunks:65537")).unwrap();
    assert_eq!(failure(wl.snapshot()).kind, FailureKind::Contract);
}

#[test]
fn a_snapshot_runs_under_snapshot_fuel_not_step_fuel() {
    // Each chunk call fits in the step fuel, all of them together do not.
    let limits = Limits {
        fuel_per_call: 2_000_000,
        snapshot_fuel: 200_000_000,
        ..Limits::default()
    };
    let mut wl = load(limits);
    wl.step(input(0, b"chunks:20000")).unwrap();
    let chunks = wl.snapshot().unwrap();
    let refs: Vec<&[u8]> = chunks.iter().map(Vec::as_slice).collect();
    load(limits).restore(&refs).unwrap();

    let tight = Limits {
        snapshot_fuel: 2_000_000,
        ..limits
    };
    let mut wl = load(tight);
    wl.step(input(0, b"chunks:20000")).unwrap();
    assert_eq!(failure(wl.snapshot()).kind, FailureKind::OutOfFuel);
    assert_eq!(
        failure(load(tight).restore(&refs)).kind,
        FailureKind::OutOfFuel
    );
}

#[test]
fn a_contract_01_workload_is_refused() {
    let err = Runtime::new()
        .unwrap()
        .load(&felixd_testkit::contract_01_component(), Limits::default())
        .err()
        .expect("refused");
    assert!(matches!(err, LoadError::Contract01), "{err}");
    assert!(err.to_string().contains("felixd:workload@0.1.0"), "{err}");
}
