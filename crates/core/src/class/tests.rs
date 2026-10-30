use super::*;

#[test]
fn codes_round_trip() {
    for class in [
        ErrorClass::Transient,
        ErrorClass::NeedsOperator,
        ErrorClass::Poison,
    ] {
        assert_eq!(ErrorClass::from_code(class.code()), Ok(class));
    }
    for reason in [
        OperatorReason::ModuleRefused,
        OperatorReason::ModuleMissing,
        OperatorReason::SpecUnrecorded,
        OperatorReason::SpecConflict,
        OperatorReason::SpecStale,
        OperatorReason::RestoreFuel,
        OperatorReason::InputUnreadable,
        OperatorReason::InputBehind,
    ] {
        assert_eq!(OperatorReason::from_code(reason.code()), Ok(reason));
    }
}

#[test]
fn an_unknown_code_is_refused() {
    assert_eq!(ErrorClass::from_code(3), Err(DecodeError::UnknownClass(3)));
    assert_eq!(
        OperatorReason::from_code(0),
        Err(DecodeError::UnknownReason(0))
    );
}

#[test]
fn a_class_is_kebab_case_in_json() {
    let json = serde_json::to_string(&ErrorClass::NeedsOperator).unwrap();
    assert_eq!(json, "\"needs-operator\"");
}
