use super::*;

fn spec() -> WorkloadSpec {
    WorkloadSpec {
        module: "ab".repeat(32),
        input: "sensors".into(),
        output: "wl.temp.out".into(),
        snapshot_every: 50,
        state: WorkloadState::Running,
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
            r#"{{"module":"{}","input":"sensors","output":"wl.temp.out","snapshot_every":50,"state":"running"}}"#,
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
