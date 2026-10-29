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
