use super::*;

fn out(input_offset: u64, index: u32) -> OutputHeader {
    OutputHeader {
        input_offset,
        index,
    }
}

fn snap(input_offset: u64, output_tail: u64) -> SnapshotHeader {
    SnapshotHeader {
        input_offset,
        output_tail,
    }
}

#[test]
fn a_new_workload_starts_from_input_zero() {
    let resume = fold(Found {
        snapshot: None,
        last_output: None,
        output_tail: 0,
    })
    .unwrap();
    assert!(!resume.restore);
    assert_eq!(resume.next_input, 0);
    assert!(!resume.already_written(0, 0));
}

#[test]
fn no_snapshot_but_outputs_replays_from_zero_and_skips_them() {
    let resume = fold(Found {
        snapshot: None,
        last_output: Some(out(7, 0)),
        output_tail: 8,
    })
    .unwrap();
    assert!(!resume.restore);
    assert_eq!(resume.next_input, 0);
    assert!(resume.already_written(0, 0));
    assert!(resume.already_written(7, 0));
    assert!(!resume.already_written(8, 0));
}

#[test]
fn a_snapshot_with_no_later_output_resumes_after_it_and_skips_nothing() {
    let resume = fold(Found {
        snapshot: Some(snap(99, 40)),
        last_output: Some(out(95, 0)),
        output_tail: 40,
    })
    .unwrap();
    assert!(resume.restore);
    assert_eq!(resume.next_input, 100);
    assert!(!resume.already_written(100, 0));
}

#[test]
fn a_snapshot_with_an_empty_output_stream_skips_nothing() {
    let resume = fold(Found {
        snapshot: Some(snap(9, 0)),
        last_output: None,
        output_tail: 0,
    })
    .unwrap();
    assert_eq!(resume.next_input, 10);
    assert!(!resume.already_written(10, 0));
}

#[test]
fn outputs_past_the_snapshot_are_skipped_up_to_the_last_one() {
    let resume = fold(Found {
        snapshot: Some(snap(99, 40)),
        last_output: Some(out(104, 1)),
        output_tail: 43,
    })
    .unwrap();
    assert!(resume.restore);
    assert_eq!(resume.next_input, 100);
    assert!(resume.already_written(100, 0));
    assert!(resume.already_written(103, 5));
    assert!(resume.already_written(104, 0));
    assert!(resume.already_written(104, 1));
    assert!(!resume.already_written(104, 2));
    assert!(!resume.already_written(105, 0));
}

#[test]
fn steps_with_no_output_are_skipped_by_position_not_by_count() {
    // Inputs 101 to 103 produced nothing; 100 and 104 did.
    let resume = fold(Found {
        snapshot: Some(snap(99, 40)),
        last_output: Some(out(104, 0)),
        output_tail: 42,
    })
    .unwrap();
    for input in 100..=104 {
        assert!(resume.already_written(input, 0), "{input}");
    }
    assert!(!resume.already_written(105, 0));
}

#[test]
fn a_batch_cut_short_resumes_at_its_first_missing_output() {
    // Input 12 produced three outputs and only the first two landed.
    let resume = fold(Found {
        snapshot: None,
        last_output: Some(out(12, 1)),
        output_tail: 5,
    })
    .unwrap();
    assert!(resume.already_written(12, 1));
    assert!(!resume.already_written(12, 2));
}

#[test]
fn a_stream_shorter_than_the_snapshot_expects_is_refused() {
    let err = fold(Found {
        snapshot: Some(snap(99, 40)),
        last_output: Some(out(90, 0)),
        output_tail: 39,
    })
    .unwrap_err();
    assert_eq!(
        err,
        ResumeError::OutputBehindSnapshot {
            expected: 40,
            tail: 39
        }
    );
}

#[test]
fn an_output_for_an_input_the_snapshot_covers_is_refused() {
    let err = fold(Found {
        snapshot: Some(snap(99, 40)),
        last_output: Some(out(98, 0)),
        output_tail: 41,
    })
    .unwrap_err();
    assert_eq!(
        err,
        ResumeError::OutputBeforeSnapshot {
            covered: 99,
            found: 98
        }
    );
}

#[test]
fn records_with_no_last_header_are_refused() {
    let err = fold(Found {
        snapshot: None,
        last_output: None,
        output_tail: 3,
    })
    .unwrap_err();
    assert_eq!(err, ResumeError::MissingLastOutput);
}
