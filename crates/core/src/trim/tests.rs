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

/// Until the newest snapshot is the holder's own, an earlier holder can still
/// append one, so a holder that has not written one trims nothing. A stale
/// holder that finds a later holder's snapshot newest trims nothing either.
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
    };
    let record = output::encode_trim(5, &trim);
    let decoded = output::decode(&record).unwrap();
    assert_eq!(decoded.epoch(), 5);
    assert_eq!(decoded.trim(), Some(trim));
    assert_eq!(decoded.written(), None);
    assert!(matches!(decoded, Record::Trim { .. }));
    assert!(output::is_marker(&record));
    let mut long = record.clone();
    long.push(0);
    assert_eq!(output::decode(&long), Err(DecodeError::TrailingBytes(1)));
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
    };
    let record = output::encode_trim(1, &trim);
    assert!(output::passed_over(&record, output::DEFAULT_NAME));
}
