use super::*;
use crate::class::{ErrorClass, Failure};

fn spec() -> WorkloadSpec {
    WorkloadSpec {
        module: "ab".repeat(32),
        input: "sensors".into(),
        output: "wl.temp.out".into(),
        snapshot_every: 50,
        snapshot_every_ms: None,
        state: WorkloadState::Running,
        limits: Limits {
            fuel_per_call: 1000,
            max_memory_bytes: 1 << 20,
            snapshot_fuel: 5000,
        },
        failure: None,
        record: None,
    }
}

#[test]
fn a_spec_round_trips() {
    assert_eq!(WorkloadSpec::decode(&spec().encode()), Ok(spec()));
}

#[test]
fn the_json_form_is_fixed() {
    let json = String::from_utf8(spec().encode()).unwrap();
    assert_eq!(
        json,
        format!(
            r#"{{"format":1,"value":{{"module":"{}","input":"sensors","output":"wl.temp.out","snapshot_every":50,"state":"running","limits":{{"fuel_per_call":1000,"max_memory_bytes":1048576,"snapshot_fuel":5000}}}}}}"#,
            "ab".repeat(32)
        )
    );
}

#[test]
fn optional_fields_have_defaults() {
    let json = format!(
        r#"{{"module":"{}","input":"i","output":"o","state":"stopped"}}"#,
        "00".repeat(32)
    );
    let decoded = WorkloadSpec::decode(json.as_bytes()).unwrap();
    assert_eq!(decoded.snapshot_every, DEFAULT_SNAPSHOT_EVERY);
    assert_eq!(decoded.limits, Limits::default());
    assert_eq!(decoded.failure, None);
    assert_eq!(decoded.state, WorkloadState::Stopped);
}

#[test]
fn limits_without_snapshot_fuel_get_ten_times_the_step_default() {
    let limits: Limits =
        serde_json::from_str(r#"{"fuel_per_call":1000,"max_memory_bytes":1048576}"#).unwrap();
    assert_eq!(limits.snapshot_fuel, 500_000_000);
    assert_eq!(limits.snapshot_fuel, 10 * Limits::default().fuel_per_call);
}

#[test]
fn a_failed_spec_keeps_its_reason() {
    let failed = WorkloadSpec {
        state: WorkloadState::Failed,
        failure: Some(Failure {
            class: ErrorClass::Poison,
            detail: "out of fuel".into(),
        }),
        ..spec()
    };
    assert_eq!(WorkloadSpec::decode(&failed.encode()), Ok(failed));
}

#[test]
fn a_spec_from_before_formats_still_reads() {
    let mut legacy = serde_json::to_value(spec()).unwrap();
    legacy["state"] = "failed".into();
    legacy["failure"] = "trap".into();
    let read = WorkloadSpec::decode(&serde_json::to_vec(&legacy).unwrap()).unwrap();
    assert_eq!(
        read.failure,
        Some(Failure {
            class: ErrorClass::Poison,
            detail: "trap".into()
        })
    );
}

#[test]
fn a_build_before_formats_refuses_a_spec_instead_of_misreading_it() {
    // What an older build decodes a `workloads` value into: the fields at the
    // top, unknown ones ignored.
    #[derive(serde::Deserialize)]
    #[allow(dead_code)]
    struct Before {
        module: String,
        input: String,
        output: String,
        state: WorkloadState,
    }
    assert!(serde_json::from_slice::<Before>(&spec().encode()).is_err());
}

#[test]
fn a_spec_of_another_format_is_refused() {
    let mut value: serde_json::Value = serde_json::from_slice(&spec().encode()).unwrap();
    value["format"] = 2.into();
    assert_eq!(
        WorkloadSpec::decode(&serde_json::to_vec(&value).unwrap()),
        Err(DecodeError::UnknownVersion(2))
    );
}

#[test]
fn an_unknown_state_is_refused() {
    let json = br#"{"module":"m","input":"i","output":"o","state":"paused"}"#;
    assert!(matches!(
        WorkloadSpec::decode(json),
        Err(DecodeError::Json(_))
    ));
}

#[test]
fn only_a_new_module_or_input_conflicts_with_written_history() {
    let old = spec();
    let mut next = spec();
    next.snapshot_every = 7;
    next.limits.fuel_per_call = 1;
    assert_eq!(old.conflicts_with_history(&next), None);
    next.module = "cd".repeat(32);
    assert!(old.conflicts_with_history(&next).is_some());
    let mut next = spec();
    next.input = "other".into();
    assert!(old.conflicts_with_history(&next).is_some());
}

#[test]
fn a_spec_record_round_trips_and_an_unknown_format_is_refused() {
    let record = SpecRecord::new("temp", spec());
    assert_eq!(SpecRecord::decode(&record.encode()), Ok(record.clone()));
    let mut newer = record;
    newer.format = 2;
    assert_eq!(
        SpecRecord::decode(&newer.encode()),
        Err(DecodeError::UnknownVersion(2))
    );
}

#[test]
fn history_keeps_one_workloads_records_in_order() {
    let mut second = spec();
    second.limits.fuel_per_call = 7;
    let records = [
        (0, SpecRecord::new("temp", spec()).encode()),
        (1, SpecRecord::new("other", second.clone()).encode()),
        (2, SpecRecord::new("temp", second.clone()).encode()),
    ];
    let history = History::read("temp", records.iter().map(|(o, r)| (*o, r.as_slice()))).unwrap();
    assert_eq!(history.latest(), Some(&(2, second.clone())));
    assert_eq!(history.at(0), Some(&spec()));
    assert_eq!(history.at(1), None);
    let bad = [(0, b"{".to_vec())];
    assert!(History::read("temp", bad.iter().map(|(o, r)| (*o, r.as_slice()))).is_err());
}

#[test]
fn a_cache_value_matches_its_record_whatever_its_state() {
    let record = spec();
    let mut cached = WorkloadSpec {
        record: Some(4),
        ..spec()
    };
    assert!(cached.matches_record(&record));
    cached.state = WorkloadState::Failed;
    cached.failure = Some(Failure {
        class: ErrorClass::Poison,
        detail: "trap".into(),
    });
    assert!(cached.matches_record(&record));
    cached.limits.fuel_per_call = 1;
    assert!(!cached.matches_record(&record));
    let stopped = WorkloadSpec {
        state: WorkloadState::Stopped,
        ..spec()
    };
    assert!(!stopped.matches_record(&record));
}

#[test]
fn the_cache_only_moves_to_a_later_record() {
    assert!(moves_forward(None, 0));
    assert!(moves_forward(Some(3), 4));
    assert!(!moves_forward(Some(4), 4));
    assert!(!moves_forward(Some(9), 2));
}
