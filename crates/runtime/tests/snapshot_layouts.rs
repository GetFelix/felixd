//! A module declares which snapshots it can restore. The variants of the
//! `counter` workload, built from one source, check that a declared layout
//! reads as declared and that a snapshot restores into a module that lists
//! it.

mod common;

use common::{input, outputs};
use felixd_core::module::{self, Layouts, ModuleInfo, Unreadable};
use felixd_runtime::{FailureKind, Limits, Runtime, Workload, WorkloadError};

fn build(features: &[&str]) -> Vec<u8> {
    felixd_testkit::workload_with("counter", features)
}

fn load(bytes: &[u8]) -> Workload {
    Runtime::new()
        .unwrap()
        .load(bytes, Limits::default())
        .unwrap()
}

/// A counter that has taken `steps` inputs, and its snapshot.
fn counted(steps: u64) -> (Workload, Vec<Vec<u8>>) {
    let mut counter = load(&build(&[]));
    for offset in 0..steps {
        counter.step(input(offset, &[])).unwrap();
    }
    let snapshot = counter.snapshot().unwrap();
    (counter, snapshot)
}

fn restore(workload: &mut Workload, snapshot: &[Vec<u8>]) -> Result<(), WorkloadError> {
    let chunks: Vec<&[u8]> = snapshot.iter().map(Vec::as_slice).collect();
    workload.restore(&chunks)
}

fn total(workload: &mut Workload, offset: u64) -> u64 {
    let stepped = workload.step(input(offset, &[])).unwrap();
    let payload = outputs(stepped).remove(0).payload;
    u64::from_be_bytes(payload.try_into().unwrap())
}

#[test]
fn the_built_component_carries_the_declared_layout() {
    let layouts = |features: &[&str]| module::layouts(&build(features)).unwrap();
    let want = |format: &str, reads: &[&str]| {
        Some(Layouts {
            format: format.to_owned(),
            reads: reads.iter().map(|r| (*r).to_owned()).collect(),
        })
    };
    assert_eq!(layouts(&[]), want("counter", &[]));
    assert_eq!(layouts(&["layout-v2"]), want("counter.2", &["counter"]));
    assert_eq!(layouts(&["foreign-layout"]), want("tally", &[]));
    // A fixture that declares nothing.
    assert_eq!(
        module::layouts(&felixd_testkit::workload("faulty")).unwrap(),
        None
    );
}

#[test]
fn a_module_that_lists_the_layout_restores_the_snapshot_and_carries_on_the_same() {
    let (mut original, snapshot) = counted(12);
    let recorded = ModuleInfo::of(&build(&[])).unwrap();
    let upgrade = build(&["layout-v2"]);
    assert_ne!(recorded.hash, module::hash(&upgrade));
    assert_eq!(
        module::can_restore(&recorded, &ModuleInfo::of(&upgrade).unwrap()),
        Ok(())
    );

    let mut restored = load(&upgrade);
    restore(&mut restored, &snapshot).unwrap();
    assert_eq!(restored.inspect().unwrap(), original.inspect().unwrap());
    for offset in 12..40 {
        assert_eq!(total(&mut restored, offset), total(&mut original, offset));
    }
    assert_eq!(restored.snapshot().unwrap(), original.snapshot().unwrap());
}

#[test]
fn a_changed_module_continues_from_the_restored_state_and_counts_differently() {
    let (_, snapshot) = counted(8);
    let mut changed = load(&build(&["changed"]));
    restore(&mut changed, &snapshot).unwrap();
    // The recorded counter would say 9, 10, 11; this one counts by two from
    // ten on.
    let totals: Vec<u64> = (8..12).map(|offset| total(&mut changed, offset)).collect();
    assert_eq!(totals, [9, 10, 12, 14]);
}

#[test]
fn a_module_that_declares_a_layout_it_cannot_restore_traps_in_restore() {
    let (_, snapshot) = counted(3);
    let recorded = ModuleInfo::of(&build(&[])).unwrap();
    let unreadable = build(&["unreadable"]);
    assert_eq!(
        module::can_restore(&recorded, &ModuleInfo::of(&unreadable).unwrap()),
        Ok(())
    );
    let mut candidate = load(&unreadable);
    match restore(&mut candidate, &snapshot).unwrap_err() {
        WorkloadError::Failed(failure) => assert_eq!(failure.kind, FailureKind::Trap),
        other => panic!("{other:?}"),
    }
}

#[test]
fn modules_that_do_not_list_the_layout_do_not_restore_it() {
    let recorded = ModuleInfo::of(&build(&[])).unwrap();
    let foreign = ModuleInfo::of(&build(&["foreign-layout"])).unwrap();
    assert_eq!(
        module::can_restore(&recorded, &foreign),
        Err(Unreadable::NotListed {
            layout: "counter".to_owned(),
            writes: "tally".to_owned(),
        })
    );
    let faulty = ModuleInfo::of(&felixd_testkit::workload("faulty")).unwrap();
    assert_eq!(
        module::can_restore(&recorded, &faulty),
        Err(Unreadable::CandidateUndeclared)
    );
    assert_eq!(
        module::can_restore(&faulty, &recorded),
        Err(Unreadable::RecordedUndeclared)
    );
}
