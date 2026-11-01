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
    let degraded = output::encode_degraded(1, 7, ErrorClass::Transient, "gone");
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
