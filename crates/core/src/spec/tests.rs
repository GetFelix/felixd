use super::*;

fn spec() -> WorkloadSpec {
    WorkloadSpec {
        module: "ab".repeat(32),
        input: "sensors".into(),
        output: "wl.temp.out".into(),
        snapshot_every: 50,
        state: WorkloadState::Running,
        limits: Limits {
            fuel_per_call: 1000,
            max_memory_bytes: 1 << 20,
        },
        failure: None,
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
            r#"{{"module":"{}","input":"sensors","output":"wl.temp.out","snapshot_every":50,"state":"running","limits":{{"fuel_per_call":1000,"max_memory_bytes":1048576}}}}"#,
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
fn a_failed_spec_keeps_its_reason() {
    let failed = WorkloadSpec {
        state: WorkloadState::Failed,
        failure: Some("out of fuel".into()),
        ..spec()
    };
    assert_eq!(WorkloadSpec::decode(&failed.encode()), Ok(failed));
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
