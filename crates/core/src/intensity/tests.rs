use super::*;
use crate::class::{ErrorClass, OperatorReason};
use crate::output::OutputHeader;

const SPEC: u64 = 7;
const SECOND: u64 = 1_000_000;

fn limit(max_takeovers: u32) -> RestartIntensity {
    RestartIntensity {
        max_takeovers,
        window_ms: 60_000,
    }
}

fn claim(epoch: u64) -> Vec<u8> {
    output::encode_claim_under(epoch, SPEC, SPEC, "n")
}

fn degraded(epoch: u64) -> Vec<u8> {
    output::encode_degraded(epoch, SPEC, ErrorClass::Transient, "n", "stuck")
}

fn an_output(epoch: u64) -> Vec<u8> {
    let header = OutputHeader {
        input_offset: 0,
        index: 0,
    };
    output::encode(header, epoch, b"x")
}

/// The verdict over `records`, oldest first, each with its append time in
/// seconds.
fn verdict(limit: RestartIntensity, records: &[(Vec<u8>, Option<u64>)]) -> Verdict {
    let mut count = Takeovers::new(SPEC, limit);
    for (record, at) in records.iter().rev() {
        if let Some(found) = count.older(record, at.map(|s| s * SECOND)) {
            return found;
        }
    }
    count.verdict()
}

fn claims(epochs: std::ops::RangeInclusive<u64>) -> Vec<(Vec<u8>, Option<u64>)> {
    epochs.map(|e| (claim(e), Some(100 + e))).collect()
}

#[test]
fn takeovers_up_to_the_limit_are_within_it_and_one_more_is_over() {
    // The first claim starts the run; each after it is a takeover.
    assert_eq!(verdict(limit(2), &claims(1..=3)), Verdict::Within);
    assert_eq!(
        verdict(limit(2), &claims(1..=4)),
        Verdict::Over { takeovers: 3 }
    );
    assert_eq!(
        verdict(limit(0), &claims(1..=2)),
        Verdict::Over { takeovers: 1 }
    );
}

#[test]
fn an_output_between_claims_ends_the_run() {
    let mut records = claims(1..=3);
    records.push((an_output(3), Some(104)));
    records.extend(claims(4..=5));
    assert_eq!(verdict(limit(1), &records), Verdict::Within);
    records.extend(claims(6..=6));
    assert_eq!(verdict(limit(1), &records), Verdict::Over { takeovers: 2 });
}

#[test]
fn claims_older_than_the_window_do_not_count() {
    let mut records = vec![(claim(1), Some(0)), (claim(2), Some(1))];
    records.push((claim(3), Some(100)));
    assert_eq!(verdict(limit(0), &records[1..]), Verdict::Within);
    records.push((claim(4), Some(130)));
    assert_eq!(verdict(limit(0), &records), Verdict::Over { takeovers: 1 });
}

#[test]
fn notes_between_claims_are_passed_over() {
    let records = vec![
        (claim(1), Some(1)),
        (degraded(1), Some(2)),
        (claim(2), Some(3)),
        (
            output::encode_failure(2, 1, ErrorClass::Transient, "x"),
            Some(4),
        ),
        (claim(3), Some(5)),
        // The newest claim's own note does not open the window early.
        (degraded(3), Some(500)),
    ];
    assert_eq!(verdict(limit(1), &records), Verdict::Over { takeovers: 2 });
}

#[test]
fn a_claim_of_the_same_epoch_counts_once() {
    let records = vec![
        (claim(1), Some(1)),
        (claim(2), Some(2)),
        (claim(2), Some(3)),
    ];
    assert_eq!(verdict(limit(1), &records), Verdict::Within);
}

#[test]
fn claims_of_another_spec_record_end_the_run() {
    let records = vec![
        (
            output::encode_claim_under(1, SPEC - 1, SPEC - 1, "n"),
            Some(1),
        ),
        (
            output::encode_claim_under(2, SPEC - 1, SPEC - 1, "n"),
            Some(2),
        ),
        (claim(3), Some(3)),
        (claim(4), Some(4)),
    ];
    assert_eq!(verdict(limit(1), &records), Verdict::Within);
}

#[test]
fn a_record_with_no_append_time_or_unreadable_ends_the_run() {
    let mut records = claims(1..=3);
    records[1].1 = None;
    assert_eq!(verdict(limit(0), &records), Verdict::Within);
    let records = vec![
        (claim(1), Some(1)),
        (vec![99, 0], Some(2)),
        (claim(2), Some(3)),
    ];
    assert_eq!(verdict(limit(0), &records), Verdict::Within);
}

#[test]
fn a_needs_operator_record_is_a_note() {
    let records = vec![
        (claim(1), Some(1)),
        (
            output::encode_needs_operator(1, SPEC, OperatorReason::RestoreFuel, "x"),
            Some(2),
        ),
        (claim(2), Some(3)),
    ];
    assert_eq!(verdict(limit(0), &records), Verdict::Over { takeovers: 1 });
}
