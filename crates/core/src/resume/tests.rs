use super::*;

fn out(input_offset: u64, index: u32) -> OutputHeader {
    OutputHeader {
        input_offset,
        index,
    }
}

fn at(offset: u64, input_offset: u64, index: u32) -> LastOutput {
    LastOutput {
        offset,
        header: out(input_offset, index),
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
        last_output: Some(at(7, 7, 0)),
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
        last_output: Some(at(39, 95, 0)),
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
        last_output: Some(at(42, 104, 1)),
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
        last_output: Some(at(41, 104, 0)),
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
        last_output: Some(at(4, 12, 1)),
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
        last_output: Some(at(38, 90, 0)),
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
        last_output: Some(at(40, 98, 0)),
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

// A new shard leader appends a generation-start record, which takes an
// offset but is not an output, so the tail can be past the last output.

#[test]
fn a_stream_of_only_generation_starts_starts_from_input_zero() {
    let resume = fold(Found {
        snapshot: None,
        last_output: None,
        output_tail: 3,
    })
    .unwrap();
    assert!(!resume.restore);
    assert_eq!(resume.next_input, 0);
    assert!(!resume.already_written(0, 0));
}

#[test]
fn a_generation_start_at_the_snapshot_tail_is_not_a_later_output() {
    let resume = fold(Found {
        snapshot: Some(snap(99, 40)),
        last_output: Some(at(39, 95, 0)),
        output_tail: 42,
    })
    .unwrap();
    assert!(resume.restore);
    assert_eq!(resume.next_input, 100);
    assert_eq!(resume.output_from, 40);
    assert!(!resume.already_written(100, 0));
    assert!(!resume.replaying(100));
}

#[test]
fn outputs_after_a_generation_start_are_still_skipped() {
    let resume = fold(Found {
        snapshot: Some(snap(99, 40)),
        last_output: Some(at(41, 100, 0)),
        output_tail: 43,
    })
    .unwrap();
    assert!(resume.already_written(100, 0));
    assert!(!resume.already_written(101, 0));
}

#[test]
fn skipped_outputs_start_at_the_snapshot_tail() {
    let resume = fold(Found {
        snapshot: Some(snap(99, 40)),
        last_output: Some(at(42, 104, 1)),
        output_tail: 43,
    })
    .unwrap();
    assert_eq!(resume.output_from, 40);
    let fresh = fold(Found {
        snapshot: None,
        last_output: Some(at(7, 7, 0)),
        output_tail: 8,
    })
    .unwrap();
    assert_eq!(fresh.output_from, 0);
}

#[test]
fn the_owner_is_replaying_until_it_reaches_the_last_written_input() {
    let resume = fold(Found {
        snapshot: Some(snap(99, 40)),
        last_output: Some(at(42, 104, 1)),
        output_tail: 43,
    })
    .unwrap();
    assert!(resume.replaying(100));
    assert!(resume.replaying(103));
    // After the step for 104 the output tail is its true value again.
    assert!(!resume.replaying(104));
    assert!(!resume.replaying(105));
}

#[test]
fn nothing_to_skip_is_not_replaying() {
    let resume = fold(Found {
        snapshot: Some(snap(99, 40)),
        last_output: Some(at(39, 95, 0)),
        output_tail: 40,
    })
    .unwrap();
    assert!(!resume.replaying(100));
}

#[test]
fn a_skipped_output_must_match_the_record_in_its_place() {
    let record = |epoch, payload: &[u8]| crate::output::encode(out(5, 0), epoch, payload);
    assert_eq!(
        check_skipped(3, &record(2, b"abc"), Some(&record(1, b"abc"))),
        Ok(())
    );
    assert_eq!(
        check_skipped(3, &record(1, b"abc"), Some(&record(1, b"abd"))),
        Err(ResumeError::Diverged { offset: 3 })
    );
    assert_eq!(
        check_skipped(9, &record(1, b"abc"), None),
        Err(ResumeError::Diverged { offset: 9 })
    );
}

#[test]
fn claims_after_the_outputs_a_snapshot_covers_skip_nothing() {
    // Offsets 40 and 41 hold claims, the snapshot was taken after the first.
    let resume = fold(Found {
        snapshot: Some(snap(99, 41)),
        last_output: Some(at(39, 95, 0)),
        output_tail: 42,
    })
    .unwrap();
    assert_eq!(resume.next_input, 100);
    assert_eq!(resume.output_from, 41);
    assert!(!resume.already_written(100, 0));
}
