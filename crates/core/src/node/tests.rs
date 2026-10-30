use super::*;

#[test]
fn node_info_round_trips() {
    let info = NodeInfo {
        version: "0.0.0".into(),
        max_workloads: 4,
        held: 1,
        draining: None,
    };
    assert_eq!(NodeInfo::decode(&info.encode()), Ok(info.clone()));
    // Not draining is written as before the field.
    let older = br#"{"format":1,"value":{"version":"0.0.0","max_workloads":4,"held":1}}"#;
    assert_eq!(info.encode(), older);
    let draining = NodeInfo {
        draining: Some(7),
        ..info
    };
    assert_eq!(NodeInfo::decode(&draining.encode()), Ok(draining));
}

#[test]
fn only_an_acknowledgement_of_this_mark_counts() {
    let info = |draining| NodeInfo {
        version: "0.0.0".into(),
        max_workloads: 4,
        held: 0,
        draining,
    };
    let mark = Drain { nonce: 9 };
    assert!(acknowledges(Some(&info(Some(9))), Some(&mark)));
    // Left from an earlier drain.
    assert!(!acknowledges(Some(&info(Some(8))), Some(&mark)));
    assert!(!acknowledges(Some(&info(None)), Some(&mark)));
    assert!(!acknowledges(None, Some(&mark)));
    assert!(!acknowledges(Some(&info(Some(9))), None));
    assert!(!acknowledges(
        Some(&info(Some(0))),
        Some(&Drain { nonce: 0 })
    ));
}

#[test]
fn a_drain_mark_round_trips_and_refuses_another_format() {
    let mark = Drain { nonce: 3 };
    assert_eq!(Drain::decode(&mark.encode()), Ok(mark));
    assert_eq!(
        Drain::decode(br#"{"format": 1, "value": {}}"#),
        Ok(Drain { nonce: 0 })
    );
    assert!(Drain::decode(br#"{"format": 2, "value": {}}"#).is_err());
}
