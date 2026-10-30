use super::*;
use felixd_core::output::{Addressed, OutputHeader, encode_addressed, encode_claim};

fn out(offset: u64, workload: &str, node: Option<&str>) -> Vec<u8> {
    let payload = Assignment {
        workload: workload.into(),
        node: node.map(str::to_owned),
    }
    .encode();
    let header = OutputHeader {
        input_offset: offset,
        index: 0,
    };
    let addressed = Addressed {
        name: "out",
        key: Some(workload.as_bytes()),
        payload: &payload,
    };
    encode_addressed(header, 1, addressed).unwrap()
}

#[test]
fn the_latest_assignment_per_workload_is_in_force() {
    let mut table = HashMap::new();
    let records = [
        encode_claim(1, "n-1"),
        out(0, "a", Some("n-1")),
        out(1, "b", Some("n-2")),
        b"not a record".to_vec(),
        out(2, "a", Some("n-3")),
        out(3, "b", None),
    ];
    for (offset, record) in (10..).zip(&records) {
        fold(&mut table, offset, record);
    }
    assert_eq!(
        table,
        HashMap::from([(
            "a".to_owned(),
            Placed {
                node: "n-3".into(),
                offset: 14
            }
        )])
    );
}

#[tokio::test]
async fn a_change_wakes_whoever_waits_on_it() {
    let assignments = Assignments::new();
    let mut changes = assignments.changes();
    assert_eq!(assignments.get("a"), None);
    let placed = Placed {
        node: "n-1".into(),
        offset: 3,
    };
    assignments.replace(HashMap::from([("a".to_owned(), placed.clone())]));
    changes.changed().await.unwrap();
    assert_eq!(assignments.get("a"), Some(placed));

    let scheduler = assignments.scheduler_changes();
    assignments.set_scheduler(false);
    assert!(!scheduler.has_changed().unwrap());
    assignments.set_scheduler(true);
    assert!(scheduler.has_changed().unwrap());
    assert!(assignments.scheduler());
}
