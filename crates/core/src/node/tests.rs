use super::*;

#[test]
fn node_info_round_trips() {
    let info = NodeInfo {
        version: "0.0.0".into(),
        max_workloads: 4,
        held: 1,
        draining: false,
    };
    assert_eq!(NodeInfo::decode(&info.encode()), Ok(info.clone()));
    // Not draining is written as before the field.
    let older = br#"{"format":1,"value":{"version":"0.0.0","max_workloads":4,"held":1}}"#;
    assert_eq!(info.encode(), older);
    let draining = NodeInfo {
        draining: true,
        ..info
    };
    assert_eq!(NodeInfo::decode(&draining.encode()), Ok(draining));
}

#[test]
fn a_drain_mark_round_trips_and_refuses_another_format() {
    assert_eq!(Drain::decode(&Drain {}.encode()), Ok(Drain {}));
    assert!(Drain::decode(br#"{"format": 2, "value": {}}"#).is_err());
}
