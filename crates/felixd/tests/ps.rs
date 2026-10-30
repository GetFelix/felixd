//! `felixd ps` reads each workload's state, holder and positions from the
//! caches and streams.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::process::Stdio;
use std::time::Duration;

use common::{ID, INPUT, OUTPUT, TestFelix, eventually, replication_factor, test_felix};
use felixd_agent::admin::{StreamKind, ensure_stream};
use felixd_agent::ps::{Report, ps};
use felixd_core::class::OperatorReason;
use felixd_core::output;
use felixd_core::status::{Parked, State};

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

fn printed(t: &TestFelix) -> String {
    let out = t.felixd(&["ps"]).stdout(Stdio::piped()).output().unwrap();
    assert!(out.status.success());
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// `None` while a stream just created is not served yet.
async fn report(t: &TestFelix) -> Option<Report> {
    ps(&t.settings, &[ID.to_owned()]).await.ok()
}

#[tokio::test(flavor = "multi_thread")]
async fn ps_shows_the_holder_and_how_far_it_is_along_its_input() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..10).map(count)).await;
    let agent = t.start_agent();
    let report = eventually(
        "the workload to catch up",
        Duration::from_secs(60),
        || async {
            let report = report(&t).await?;
            let w = &report.workloads[0];
            (w.state == Some(State::Running) && w.inputs[0].next > 0 && w.inputs[0].lag() == 0)
                .then_some(report)
        },
    )
    .await;
    let w = &report.workloads[0];
    assert_eq!(w.holder.as_ref().unwrap().owner, agent.node_id);
    assert_eq!(w.inputs[0].stream, INPUT);
    assert_eq!(w.inputs[0].tail, t.felix.tail(INPUT).await.unwrap());
    assert_eq!(w.outputs, t.felix.tail(OUTPUT).await.unwrap());
    assert_eq!(w.last_epoch, Some(w.holder.as_ref().unwrap().epoch));
    assert!(w.steps.is_none() && w.unread.is_none(), "{w:?}");
    let node = report.nodes.iter().find(|n| n.id == agent.node_id).unwrap();
    assert!(
        node.info.is_some() && node.leases == 1 && !node.draining,
        "{node:?}"
    );
    let printed = printed(&t);
    assert!(printed.contains(&agent.node_id), "{printed}");
    assert!(printed.contains("running"), "{printed}");
}

/// The state comes from the output stream: a needs-operator record for the
/// current spec record parks the workload, with no node involved.
#[tokio::test(flavor = "multi_thread")]
async fn ps_shows_a_parked_workload_and_why() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    let record = t.spec().await.record.expect("a spec record");
    let parks = output::encode_needs_operator(1, record, OperatorReason::ModuleMissing, "gone");
    let tail = t.felix.tail(OUTPUT).await.unwrap();
    t.publish_if(OUTPUT, vec![parks], tail).await;

    let report = report(&t).await.expect("a report");
    let w = &report.workloads[0];
    let parked = Parked {
        reason: "module missing".into(),
        detail: "gone".into(),
    };
    assert_eq!(w.state, Some(State::NeedsOperator(parked)), "{w:?}");
    assert_eq!(w.holder, None);
    let printed = printed(&t);
    assert!(
        printed.contains(&format!("{ID}: needs operator: module missing (gone)")),
        "{printed}"
    );
}

/// A workload with two inputs shows how far each is merged and how far the
/// merge log is stepped.
#[tokio::test(flavor = "multi_thread")]
async fn ps_shows_each_input_of_a_merged_workload() {
    let Some(t) = test_felix().await else { return };
    let inputs = ["orders", "payments"];
    let kind = StreamKind {
        replication_factor: replication_factor(),
    };
    for input in inputs {
        ensure_stream(&t.settings, input, kind).await.unwrap();
    }
    let module = felixd_testkit::workload("faulty");
    assert!(t.run_on(ID, &module, &inputs, &[]).success());
    t.publish_all("orders", (0..3).map(|_| b"meta".to_vec()))
        .await;
    t.publish_all("payments", (0..2).map(|_| b"meta".to_vec()))
        .await;
    let _agent = t.start_agent();
    let report = eventually(
        "both inputs merged and stepped",
        Duration::from_secs(60),
        || async {
            let report = report(&t).await?;
            let w = &report.workloads[0];
            let caught_up = w.inputs.len() == 2
                && w.inputs.iter().all(|p| p.next > 0 && p.lag() == 0)
                && w.steps.as_ref().is_some_and(|s| s.next > 0 && s.lag() == 0);
            caught_up.then_some(report)
        },
    )
    .await;
    let w = &report.workloads[0];
    assert_eq!(w.inputs[0].stream, "orders");
    assert_eq!(w.inputs[1].stream, "payments");
    for p in &w.inputs {
        assert_eq!(p.tail, t.felix.tail(&p.stream).await.unwrap());
    }
    let steps = w.steps.as_ref().unwrap();
    assert_eq!(steps.stream, "wl.wl.merge");
    assert_eq!(steps.tail, t.felix.tail("wl.wl.merge").await.unwrap());
}
