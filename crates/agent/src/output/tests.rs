use felixd_core::class::{ErrorClass, OperatorReason};

use super::*;

fn decoded(record: &[u8]) -> Record<'_> {
    output::decode(record).unwrap()
}

#[test]
fn a_needs_operator_record_is_written_once_per_reason() {
    let missing = output::encode_needs_operator(1, 7, OperatorReason::ModuleMissing, "gone");
    let found = decoded(&missing);
    assert_eq!(
        needs_operator_found(&found, (3, 7), OperatorReason::ModuleMissing),
        Some(Recorded::NeedsOperator)
    );
    assert_eq!(
        needs_operator_found(&found, (3, 7), OperatorReason::RestoreFuel),
        None,
        "another reason was taken as written"
    );
    assert_eq!(
        needs_operator_found(&found, (3, 8), OperatorReason::ModuleMissing),
        None,
        "another spec record's was taken as this one's"
    );
}

#[test]
fn a_failure_under_this_spec_version_is_the_outcome() {
    let failure = output::encode_failure(1, 3, ErrorClass::Poison, "trap");
    let found = decoded(&failure);
    assert_eq!(
        needs_operator_found(&found, (3, 7), OperatorReason::ModuleMissing),
        Some(Recorded::Failed {
            spec_version: 3,
            failure: class::Failure {
                class: ErrorClass::Poison,
                detail: "trap".into(),
            },
        })
    );
    assert_eq!(
        needs_operator_found(&found, (4, 7), OperatorReason::ModuleMissing),
        None,
        "an earlier version's failure ended the run"
    );
}

#[test]
fn a_degraded_record_is_written_once_per_spec_record() {
    let degraded = output::encode_degraded(1, 7, ErrorClass::Transient, "node-a", "gone");
    let found = decoded(&degraded);
    assert_eq!(degraded_found(&found, (3, 7)), Some(Recorded::Degraded));
    assert_eq!(
        degraded_found(&found, (3, 8)),
        None,
        "another spec record's was taken as this one's"
    );
    let claim = output::encode_claim_under(2, 7, 7, "b");
    assert_eq!(degraded_found(&decoded(&claim), (3, 7)), None);
}

/// A degraded record behind either of these would leave the last record
/// saying nothing, and the nodes that read it would run what it ended.
#[test]
fn a_degraded_record_never_lands_behind_a_failure_or_a_parked_spec() {
    let failure = output::encode_failure(1, 3, ErrorClass::Poison, "trap");
    assert_eq!(
        degraded_found(&decoded(&failure), (3, 7)),
        Some(Recorded::Failed {
            spec_version: 3,
            failure: class::Failure {
                class: ErrorClass::Poison,
                detail: "trap".into(),
            },
        })
    );
    assert_eq!(
        degraded_found(&decoded(&failure), (4, 7)),
        None,
        "an earlier version's failure ended an earlier run"
    );
    let parked = output::encode_needs_operator(1, 7, OperatorReason::ModuleMissing, "gone");
    assert_eq!(
        degraded_found(&decoded(&parked), (3, 7)),
        Some(Recorded::NeedsOperator)
    );
    assert_eq!(degraded_found(&decoded(&parked), (3, 8)), None);
}

fn found_at<'a>(header: OutputHeader, expected: Option<&'a [u8; 32]>) -> output::Diverged<'a> {
    output::Diverged {
        spec: 7,
        header,
        snapshot: None,
        node: b"node-a",
        abi: output::Abi {
            contract: b"0.2.0",
            wasmtime: b"50.0.0",
            felixd: b"1.2.3",
        },
        expected,
        actual: None,
        detail: b"differs",
    }
}

fn header(input_offset: u64) -> OutputHeader {
    OutputHeader {
        input_offset,
        index: 0,
    }
}

/// The same finding twice is one record; another output, spec record or
/// digest is a new finding.
#[test]
fn a_divergence_record_is_written_once_per_finding() {
    let ours = found_at(header(3), Some(&[1; 32]));
    let record = output::encode_divergence(1, &ours);
    let last = decoded(&record);
    assert_eq!(
        divergence_found(&last, (3, 7), &ours),
        Some(Recorded::Diverged)
    );
    for other in [
        found_at(header(4), Some(&[1; 32])),
        found_at(header(3), Some(&[2; 32])),
        found_at(header(3), None),
    ] {
        assert_eq!(divergence_found(&last, (3, 7), &other), None, "{other:?}");
    }
    assert_eq!(
        divergence_found(&last, (3, 8), &ours),
        None,
        "another spec record's was taken as this one's"
    );
    let claim = output::encode_claim_under(2, 7, 7, "b");
    assert_eq!(divergence_found(&decoded(&claim), (3, 7), &ours), None);
}

/// A divergence record behind either of these would leave the last record
/// saying nothing about how the run ended.
#[test]
fn a_divergence_record_never_lands_behind_a_failure_or_a_parked_spec() {
    let ours = found_at(header(3), None);
    let failure = output::encode_failure(1, 3, ErrorClass::Poison, "trap");
    assert_eq!(
        divergence_found(&decoded(&failure), (3, 7), &ours),
        Some(Recorded::Failed {
            spec_version: 3,
            failure: class::Failure {
                class: ErrorClass::Poison,
                detail: "trap".into(),
            },
        })
    );
    assert_eq!(
        divergence_found(&decoded(&failure), (4, 7), &ours),
        None,
        "an earlier version's failure ended an earlier run"
    );
    let parked = output::encode_needs_operator(1, 7, OperatorReason::ModuleMissing, "gone");
    assert_eq!(
        divergence_found(&decoded(&parked), (3, 7), &ours),
        Some(Recorded::NeedsOperator)
    );
    assert_eq!(divergence_found(&decoded(&parked), (3, 8), &ours), None);
}

fn out(input_offset: u64, epoch: u64, payload: &[u8]) -> Vec<u8> {
    output::encode(header(input_offset), epoch, payload)
}

/// The digests are of what was compared, so the owner's epoch on either
/// side is no part of them, and the finding is at the batch's record.
#[test]
fn a_reconciled_divergence_names_the_output_and_digests_what_was_compared() {
    let batch = vec![out(3, 9, b"mine"), out(4, 9, b"also")];
    let theirs = [out(3, 2, b"theirs")];
    let found = divergence_in(&batch, &[theirs[0].as_slice()], (0, Some(0)), Some(1));
    assert_eq!(found.header, header(3));
    assert_eq!(found.snapshot, Some(1));
    assert_eq!(
        found.expected,
        output::compared_digest(&out(3, 0, b"theirs"))
    );
    assert_eq!(found.actual, output::compared_digest(&out(3, 0, b"mine")));
    assert_ne!(found.expected, found.actual);
    // The second record of the batch is the one that differs.
    let foreign = [out(3, 2, b"mine"), out(4, 2, b"other")];
    let existing: Vec<&[u8]> = foreign.iter().map(Vec::as_slice).collect();
    let found = divergence_in(&batch, &existing, (1, Some(1)), None);
    assert_eq!(found.header, header(4));
    assert_eq!(found.snapshot, None);
}

/// A record that does not decode has no digest and takes the batch's first
/// place.
#[test]
fn a_foreign_record_that_does_not_decode_is_a_divergence_with_no_expected_digest() {
    let batch = vec![out(3, 9, b"mine")];
    let found = divergence_in(&batch, &[b"junk"], (0, Some(0)), None);
    assert_eq!(found.header, header(3));
    assert_eq!(found.expected, None);
    assert_eq!(found.actual, output::compared_digest(&out(3, 0, b"mine")));
}

/// A record that does not decode after the batch's last output has no place
/// in the batch. It is named after that last output, not the first.
#[test]
fn an_undecodable_record_past_the_batch_is_named_after_its_last_output() {
    let batch = vec![out(3, 9, b"a"), out(4, 9, b"b")];
    let found = divergence_in(&batch, &[b"junk"], (0, None), None);
    assert_eq!(found.header, header(4));
    assert_eq!(found.expected, None);
    assert_eq!(found.actual, None);
}
