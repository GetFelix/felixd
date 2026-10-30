use super::*;

#[test]
fn node_info_round_trips() {
    let info = NodeInfo {
        version: "0.0.0".into(),
        max_workloads: 4,
        held: 1,
    };
    assert_eq!(NodeInfo::decode(&info.encode()), Ok(info));
}

#[test]
fn a_drain_mark_round_trips_and_refuses_another_format() {
    assert_eq!(Drain::decode(&Drain {}.encode()), Ok(Drain {}));
    assert!(Drain::decode(br#"{"format": 2, "value": {}}"#).is_err());
}
