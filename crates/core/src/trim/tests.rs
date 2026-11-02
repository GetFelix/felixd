use super::*;
use crate::output::{self, Record};

fn kept(offset: u64, epoch: u64, step: u64, chunks: &[u64]) -> Kept {
    Kept {
        offset,
        epoch,
        step,
        chunks: chunks.to_vec(),
    }
}

fn one_input(_: u64) -> Option<Vec<u64>> {
    None
}

#[test]
fn keeps_the_last_snapshots_and_what_they_read() {
    let newest = [
        kept(40, 3, 300, &[38, 39]),
        kept(30, 3, 200, &[12, 29]),
        kept(20, 2, 100, &[12, 19]),
    ];
    let trim = plan(3, 2, &newest, one_input, None).unwrap();
    assert_eq!(
        trim,
        Trim {
            snapshot: 30,
            step: 200,
            // A chunk the kept snapshot shares with an older one is kept.
            snap_floor: 12,
            merge_floor: 0,
            input_floors: vec![200],
            first_spec: None,
            claimed: None,
        }
    );
}

#[test]
fn nothing_is_trimmed_with_fewer_snapshots_than_kept() {
    let newest = [kept(10, 1, 50, &[])];
    assert_eq!(plan(1, 2, &newest, one_input, None), Err(Skip::TooFew));
    assert!(plan(1, 1, &newest, one_input, None).is_ok());
}

/// A stale holder's snapshot can land after a newer holder's and have a
/// lower step. The oldest kept is the lowest step, wherever it sits.
#[test]
fn the_oldest_kept_is_the_lowest_step_not_the_oldest_record() {
    let newest = [
        kept(50, 4, 400, &[]),
        kept(40, 3, 90, &[]),
        kept(30, 4, 120, &[]),
    ];
    let trim = plan(4, 3, &newest, one_input, None).unwrap();
    assert_eq!((trim.snapshot, trim.step), (40, 90));
    assert_eq!(trim.input_floors, vec![90]);
    assert_eq!(trim.snap_floor, 30);
}

/// A holder that has not written the newest snapshot trims nothing, and nor
/// does a stale holder that finds a later holder's newest. The `.out` fence
/// refuses the latter's trim anyway; this is a second guard.
#[test]
fn a_holder_trims_only_after_its_own_snapshot() {
    let newest = [kept(20, 2, 200, &[]), kept(10, 1, 100, &[])];
    assert_eq!(plan(3, 1, &newest, one_input, None), Err(Skip::NotOwn));
    assert_eq!(plan(1, 1, &newest, one_input, None), Err(Skip::NotOwn));
    assert!(plan(2, 1, &newest, one_input, None).is_ok());
}

/// With several inputs, each input carries on from the merge log's `next` at
/// the oldest kept step, not from the step itself, and the entry at the step
/// is kept for that `next`.
#[test]
fn a_merged_workload_keeps_each_input_from_next_at_the_step() {
    let newest = [kept(9, 1, 70, &[]), kept(5, 1, 40, &[])];
    let mut asked = None;
    let trim = plan(
        1,
        2,
        &newest,
        |step| {
            asked = Some(step);
            Some(vec![17, 3])
        },
        None,
    )
    .unwrap();
    assert_eq!(asked, Some(40));
    assert_eq!(trim.merge_floor, 40);
    assert_eq!(trim.input_floors, vec![17, 3]);
}

#[test]
fn a_floor_never_goes_back() {
    let previous = Trim {
        snapshot: 30,
        step: 200,
        snap_floor: 12,
        merge_floor: 0,
        input_floors: vec![201],
        first_spec: None,
        claimed: None,
    };
    let newest = [kept(40, 3, 300, &[11])];
    assert_eq!(
        plan(3, 1, &newest, one_input, Some(&previous)),
        Err(Skip::Regressed)
    );
    let newest = [kept(40, 3, 300, &[39])];
    assert!(plan(3, 1, &newest, one_input, Some(&previous)).is_ok());
}

#[test]
fn a_trim_record_round_trips_and_is_a_marker() {
    let trim = Trim {
        snapshot: 7,
        step: 99,
        snap_floor: 3,
        merge_floor: 99,
        input_floors: vec![10, 20],
        first_spec: None,
        claimed: None,
    };
    let record = output::encode_trim(5, &trim);
    let decoded = output::decode(&record).unwrap();
    assert_eq!(decoded.epoch(), 5);
    assert_eq!(decoded.trim(), Some(trim));
    assert_eq!(decoded.written(), None);
    assert!(matches!(decoded, Record::Trim { .. }));
    assert!(output::is_marker(&record));
}

/// A downstream reading this stream as its input passes over a trim record,
/// as it does a claim, rather than refusing it as an unknown kind.
#[test]
fn a_downstream_passes_over_a_trim_record() {
    let trim = Trim {
        snapshot: 0,
        step: 0,
        snap_floor: 0,
        merge_floor: 0,
        input_floors: vec![1],
        first_spec: None,
        claimed: None,
    };
    let record = output::encode_trim(1, &trim);
    assert!(output::passed_over(&record, output::DEFAULT_NAME));
}

fn floors(snap_floor: u64, merge_floor: u64) -> Trim {
    Trim {
        snapshot: snap_floor,
        step: merge_floor,
        snap_floor,
        merge_floor,
        input_floors: vec![merge_floor],
        first_spec: None,
        claimed: None,
    }
}

/// Only the workload's own streams are deleted: `.snap`, and the merge log
/// when there is one. The input floors are never acted on.
#[test]
fn a_trim_deletes_the_snapshots_and_the_merge_log_only() {
    let trim = floors(12, 40);
    assert_eq!(
        trim.deletions(true, Deleted::default()),
        vec![(Owned::Snapshots, 12), (Owned::MergeLog, 40)]
    );
    assert_eq!(
        trim.deletions(false, Deleted::default()),
        vec![(Owned::Snapshots, 12)]
    );
}

/// Deleting again below a floor already deleted to asks for nothing, so a
/// resumed holder repairs only what a crash left.
#[test]
fn nothing_is_deleted_twice() {
    let trim = floors(12, 40);
    let done = Deleted {
        snap_floor: 12,
        merge_floor: 10,
        ..Deleted::default()
    };
    assert_eq!(trim.deletions(true, done), vec![(Owned::MergeLog, 40)]);
    let all = Deleted {
        snap_floor: 12,
        merge_floor: 40,
        ..Deleted::default()
    };
    assert!(trim.deletions(true, all).is_empty());
    assert!(floors(0, 0).deletions(true, Deleted::default()).is_empty());
}

fn trim_with(first_spec: Option<u64>, claimed: Option<u64>) -> Trim {
    Trim {
        snapshot: 7,
        step: 99,
        snap_floor: 3,
        merge_floor: 99,
        input_floors: vec![10, 20],
        first_spec,
        claimed,
    }
}

#[test]
fn the_specs_a_trim_carries_round_trip() {
    for (first_spec, claimed) in [
        (None, None),
        (Some(4), None),
        (None, Some(9)),
        (Some(0), Some(9)),
    ] {
        let trim = trim_with(first_spec, claimed);
        let record = output::encode_trim(5, &trim);
        assert_eq!(output::decode(&record).unwrap().trim(), Some(trim));
    }
}

/// A trim record written before it carried specs ends at the input floors.
/// It reads as carrying none, and a trim with none writes the same bytes.
#[test]
fn a_trim_record_without_specs_is_the_original_layout() {
    let mut original = vec![4, 7, 0, 0, 0, 0, 0, 0, 0, 5];
    for field in [7u64, 99, 3, 99] {
        original.extend(field.to_be_bytes());
    }
    original.extend(2u16.to_be_bytes());
    for floor in [10u64, 20] {
        original.extend(floor.to_be_bytes());
    }
    assert_eq!(output::encode_trim(5, &trim_with(None, None)), original);
    let decoded = output::decode(&original).unwrap();
    assert_eq!(decoded.trim(), Some(trim_with(None, None)));
}

#[test]
fn a_trim_record_with_unknown_flags_or_extra_bytes_is_refused() {
    let base = output::encode_trim(5, &trim_with(None, None));
    let with = |tail: &[u8]| [base.as_slice(), tail].concat();
    // Flags that name nothing, or a field this build does not know.
    assert_eq!(
        output::decode(&with(&[0])),
        Err(DecodeError::InvalidTrimFlags(0))
    );
    assert_eq!(
        output::decode(&with(&[4, 0, 0, 0, 0, 0, 0, 0, 1])),
        Err(DecodeError::InvalidTrimFlags(4))
    );
    // A field cut short, and bytes past the last field.
    assert!(matches!(
        output::decode(&with(&[1, 0, 0])),
        Err(DecodeError::Truncated { .. })
    ));
    let mut long = output::encode_trim(5, &trim_with(Some(1), Some(2)));
    long.push(0);
    assert_eq!(output::decode(&long), Err(DecodeError::TrailingBytes(1)));
}

/// The first claim's spec is carried from one trim to the next, never
/// replaced by what the next trim might read.
#[test]
fn a_plan_carries_the_first_spec_forward() {
    let previous = Trim {
        first_spec: Some(3),
        claimed: Some(8),
        snapshot: 20,
        step: 50,
        snap_floor: 1,
        merge_floor: 0,
        input_floors: vec![50],
    };
    let newest = [kept(400, 3, 300, &[399])];
    let trim = plan(3, 1, &newest, one_input, Some(&previous)).unwrap();
    assert_eq!((trim.first_spec, trim.claimed), (Some(3), None));
    assert_eq!(
        plan(3, 1, &newest, one_input, None).unwrap().first_spec,
        None
    );
}

fn scheduler_trim(specs: u64, facts: u64) -> Trim {
    Trim {
        input_floors: vec![specs, facts],
        ..floors(12, 40)
    }
}

fn scheduler_inputs() -> Vec<String> {
    vec![names::SPECS.to_owned(), names::FACTS.to_owned()]
}

/// The floor for `sys.facts` is the scheduler's own for that input, found by
/// the input's name and not its position, and no other input's.
#[test]
fn the_facts_floor_is_the_schedulers_floor_for_that_input() {
    let trim = scheduler_trim(7, 31);
    let inputs = scheduler_inputs();
    assert_eq!(
        trim.facts_floor(names::SCHEDULER, &inputs, Deleted::default()),
        Some(31)
    );
    let swapped = vec![names::FACTS.to_owned(), names::SPECS.to_owned()];
    assert_eq!(
        trim.facts_floor(names::SCHEDULER, &swapped, Deleted::default()),
        Some(7)
    );
}

/// `sys.facts` is deleted for the scheduler alone, and only when it reads it.
#[test]
fn only_the_scheduler_deletes_facts() {
    let trim = scheduler_trim(7, 31);
    let done = Deleted::default();
    assert_eq!(trim.facts_floor("orders", &scheduler_inputs(), done), None);
    let specs_only = vec![names::SPECS.to_owned()];
    assert_eq!(trim.facts_floor(names::SCHEDULER, &specs_only, done), None);
    // A record from a build with fewer inputs names no floor for it.
    let short = Trim {
        input_floors: vec![7],
        ..floors(12, 40)
    };
    assert_eq!(
        short.facts_floor(names::SCHEDULER, &scheduler_inputs(), done),
        None
    );
}

#[test]
fn facts_are_not_deleted_twice_or_below_zero() {
    let inputs = scheduler_inputs();
    let done = Deleted {
        facts_floor: 31,
        ..Deleted::default()
    };
    let trim = scheduler_trim(7, 31);
    assert_eq!(trim.facts_floor(names::SCHEDULER, &inputs, done), None);
    let later = scheduler_trim(7, 50);
    assert_eq!(later.facts_floor(names::SCHEDULER, &inputs, done), Some(50));
    let none = scheduler_trim(0, 0);
    assert_eq!(
        none.facts_floor(names::SCHEDULER, &inputs, Deleted::default()),
        None
    );
}

fn streams(names: &[&str]) -> Vec<String> {
    names.iter().map(|s| (*s).to_owned()).collect()
}

/// Only inputs other workloads may read are candidates: not a system stream,
/// not a workload's own streams, whose holder has its own rule.
#[test]
fn only_plain_inputs_are_trimmed_on_the_readers_word() {
    let trim = Trim {
        input_floors: vec![5, 6, 7],
        ..floors(12, 40)
    };
    let inputs = streams(&["orders", names::FACTS, "wl.up.out"]);
    assert_eq!(trim.shared_inputs(&inputs, &[]), vec![(0, 5)]);
}

#[test]
fn an_input_is_not_trimmed_twice_or_at_zero() {
    let trim = Trim {
        input_floors: vec![5, 0],
        ..floors(12, 40)
    };
    let inputs = streams(&["a", "b"]);
    assert_eq!(trim.shared_inputs(&inputs, &[]), vec![(0, 5)]);
    assert!(trim.shared_inputs(&inputs, &[5, 0]).is_empty());
    assert_eq!(trim.shared_inputs(&inputs, &[3]), vec![(0, 5)]);
}

#[test]
fn the_head_of_out_goes_only_when_the_record_carries_the_first_spec() {
    let mut trim = floors(1, 1);
    assert!(!trim.keeps_lineage());
    trim.first_spec = Some(0);
    assert!(trim.keeps_lineage());
}
