//! A workload that misbehaves is stopped and not retried.

mod common;

use common::payloads;
use felixd_runtime::{FailureKind, Limits, Runtime, Workload, WorkloadError};

fn load(limits: Limits) -> Workload {
    Runtime::new()
        .unwrap()
        .load(&felixd_testkit::workload("faulty"), limits)
        .unwrap()
}

fn failure(result: Result<Vec<Vec<u8>>, WorkloadError>) -> felixd_runtime::Failure {
    match result {
        Err(WorkloadError::Failed(f)) => f,
        Err(other) => panic!("expected a failure, got {other}"),
        Ok(out) => panic!("expected a failure, got {out:?}"),
    }
}

#[test]
fn an_endless_loop_is_stopped_by_fuel() {
    let mut wl = load(Limits {
        fuel_per_call: 1_000_000,
        ..Limits::default()
    });
    assert_eq!(
        failure(payloads(&mut wl, b"spin")).kind,
        FailureKind::OutOfFuel
    );
    assert_eq!(wl.failure().unwrap().kind, FailureKind::OutOfFuel);
}

#[test]
fn a_trap_fails_the_workload() {
    let mut wl = load(Limits::default());
    assert_eq!(failure(payloads(&mut wl, b"trap")).kind, FailureKind::Trap);
}

#[test]
fn growing_past_the_memory_cap_fails_the_workload() {
    let mut wl = load(Limits {
        max_memory_bytes: 16 << 20,
        fuel_per_call: 1 << 32,
        ..Limits::default()
    });
    assert_eq!(failure(payloads(&mut wl, b"grow")).kind, FailureKind::Trap);
}

#[test]
fn the_same_allocation_is_fine_under_a_larger_cap() {
    let mut wl = load(Limits {
        max_memory_bytes: 128 << 20,
        fuel_per_call: 1 << 32,
        ..Limits::default()
    });
    assert_eq!(payloads(&mut wl, b"grow").unwrap(), [vec![1]]);
}

#[test]
fn a_failed_workload_is_not_called_again() {
    let mut wl = load(Limits::default());
    let first = failure(payloads(&mut wl, b"trap"));
    // Echo would succeed on a healthy workload.
    assert_eq!(failure(payloads(&mut wl, b"hello")), first);
    assert!(wl.snapshot().is_err());
    assert!(wl.inspect().is_err());
}

#[test]
fn fuel_is_per_call_not_per_workload() {
    let mut wl = load(Limits {
        fuel_per_call: 1_000_000,
        ..Limits::default()
    });
    for _ in 0..200 {
        assert_eq!(payloads(&mut wl, b"hi").unwrap(), [b"hi".to_vec()]);
    }
}
