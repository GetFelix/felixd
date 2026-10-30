//! The scheduler workload (#60): it assigns workloads to nodes, the nodes
//! claim what is assigned to them through the lease, its assignments carry
//! on after its own node dies, an assignment no node acts on gives way to
//! first claim wins within the bound, and a replay assigns the same way.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::collections::{BTreeSet, HashMap};
use std::process::ExitStatus;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::{Agent, INPUT, LEASE_TTL, TestFelix, eventually, replication_factor, test_felix};
use felixd_agent::admin::{StreamKind, ensure_stream};
use felixd_agent::assignments::{self, Placed};
use felixd_core::names;
use felixd_core::node::NodeInfo;
use felixd_core::output;
use felixd_core::schedule::NodeFact;
use felixd_core::spec::{SpecRecord, WorkloadState};

/// How long the tests wait for anything that should happen within a few
/// lease TTLs.
const SOON: Duration = Duration::from_secs(60);

fn start(t: &TestFelix, node: &str, bound: Duration) -> Agent {
    let bound = bound.as_millis().to_string();
    t.start_node_with_args(node, "", LEASE_TTL, &["--assignment-bound-ms", &bound])
}

/// Submits the scheduler, snapshotting often so a takeover restores from a
/// snapshot.
fn run_scheduler(t: &TestFelix) {
    let module = felixd_testkit::workload("scheduler");
    let status = t.run_on(
        names::SCHEDULER,
        &module,
        &[names::SPECS, names::FACTS],
        &["--snapshot-every", "5"],
    );
    assert!(status.success());
}

/// Submits the counter as `id`, reading [`INPUT`].
async fn run_counter(t: &TestFelix, id: &str) -> ExitStatus {
    let kind = StreamKind {
        replication_factor: replication_factor(),
    };
    ensure_stream(&t.settings, INPUT, kind).await.unwrap();
    t.run(id, &felixd_testkit::workload("counter"), INPUT, &[])
}

/// The node holding `id`'s lease once it has its epoch.
async fn holder(t: &TestFelix, id: &str) -> Option<String> {
    t.lease(id)
        .await
        .filter(|(lease, _)| lease.epoch > 0)
        .map(|(lease, _)| lease.owner)
}

async fn assigned(t: &TestFelix) -> HashMap<String, Placed> {
    assignments::read(&t.felix).await.unwrap()
}

/// Waits for `id` to be assigned, other than at `before`, and returns the
/// assignment.
async fn assignment(t: &TestFelix, id: &str, before: Option<u64>) -> Placed {
    eventually(&format!("an assignment for {id}"), SOON, || async {
        assigned(t)
            .await
            .remove(id)
            .filter(|p| Some(p.offset) != before)
    })
    .await
}

/// Waits until every one of `nodes` has appended a fact.
async fn facts_from(t: &TestFelix, nodes: &[&str]) {
    eventually("a fact from every node", SOON, || async {
        let heard: BTreeSet<String> = t
            .read_all(names::FACTS)
            .await
            .iter()
            .filter_map(|(_, r)| NodeFact::decode(r).ok())
            .map(|f| f.node)
            .collect();
        nodes.iter().all(|n| heard.contains(*n)).then_some(())
    })
    .await;
}

/// Waits for `id` to be held and checks it is held by its assignee.
async fn held_by_assignee(t: &TestFelix, id: &str) -> String {
    let placed = assignment(t, id, None).await;
    let owner = eventually(&format!("{id} to be claimed"), SOON, || holder(t, id)).await;
    assert_eq!(
        owner, placed.node,
        "{id} is assigned to {} but {owner} claimed it",
        placed.node
    );
    owner
}

/// The output records on the scheduler's output stream.
async fn scheduler_outputs(t: &TestFelix) -> usize {
    t.read_all(&names::output_stream(names::SCHEDULER))
        .await
        .iter()
        .filter(|(_, r)| !output::is_marker(r))
        .count()
}

#[tokio::test]
async fn assignments_carry_on_after_the_scheduler_s_node_dies() {
    let Some(t) = test_felix().await else {
        return;
    };
    // A bound far past the test's waits, so every claim below is one the
    // assignment made, not the fallback.
    let bound = Duration::from_secs(120);
    let ids = ["s1-a", "s1-b", "s1-c"];
    let mut agents: HashMap<&str, Agent> = ids.iter().map(|&n| (n, start(&t, n, bound))).collect();
    run_scheduler(&t);
    let first = eventually("the scheduler to be claimed", SOON, || {
        holder(&t, names::SCHEDULER)
    })
    .await;
    facts_from(&t, &ids).await;

    // With every node known and none loaded, one workload goes to each.
    let before = ["w-1", "w-2", "w-3"];
    for id in before {
        assert!(run_counter(&t, id).await.success());
    }
    let mut on = BTreeSet::new();
    for id in before {
        on.insert(held_by_assignee(&t, id).await);
    }
    assert_eq!(on.len(), 3, "the three workloads share nodes");
    let orphan = assigned(&t)
        .await
        .into_iter()
        .find(|(id, p)| p.node == first && id.as_str() != names::SCHEDULER)
        .expect("the scheduler's node has a workload");

    eprintln!("killing {first}, which runs the scheduler");
    agents.get_mut(first.as_str()).unwrap().kill();
    let second = eventually("another node to take the scheduler over", SOON, || async {
        holder(&t, names::SCHEDULER).await.filter(|n| *n != first)
    })
    .await;
    eprintln!("{second} took the scheduler over");

    // The new holder finds the dead node silent and moves its workload.
    let moved = assignment(&t, &orphan.0, Some(orphan.1.offset)).await;
    assert_ne!(moved.node, first);
    assert!(moved.offset > orphan.1.offset);

    // New workloads are assigned to live nodes and claimed by them.
    for id in ["w-4", "w-5", "w-6"] {
        assert!(run_counter(&t, id).await.success());
        let owner = held_by_assignee(&t, id).await;
        assert_ne!(owner, first);
    }
    let (ok, report) = t.check(&["--id", names::SCHEDULER]);
    assert!(ok, "felixd check found problems:\n{report}");
}

#[tokio::test]
async fn a_workload_assigned_to_a_dead_node_is_claimed_within_the_bound() {
    let Some(t) = test_felix().await else {
        return;
    };
    let bound = Duration::from_secs(10);
    let nodes = ["s2-a", "s2-b"];
    let _agents: Vec<Agent> = nodes.iter().map(|n| start(&t, n, bound)).collect();
    run_scheduler(&t);
    eventually("the scheduler to be claimed", SOON, || {
        holder(&t, names::SCHEDULER)
    })
    .await;
    facts_from(&t, &nodes).await;
    // A node that said it is there and is not: no `nodes` entry. Its id
    // sorts first, and its interval keeps the scheduler from finding it
    // silent during the test, so the scheduler picks it.
    let ghost = NodeFact {
        node: "a-ghost".into(),
        every_ms: 3_600_000,
        max_workloads: 64,
        draining: false,
    };
    t.felix.publish(names::FACTS, ghost.encode()).await.unwrap();
    facts_from(&t, &["a-ghost"]).await;

    assert!(run_counter(&t, "dead").await.success());
    let placed = assignment(&t, "dead", None).await;
    assert_eq!(placed.node, "a-ghost");
    let seen = Instant::now();
    let owner = eventually("a live node to claim it", bound, || holder(&t, "dead")).await;
    assert!(nodes.contains(&owner.as_str()), "{owner}");
    eprintln!(
        "{owner} claimed it {:?} after the assignment",
        seen.elapsed()
    );
}

#[tokio::test]
async fn an_assignee_that_never_claims_is_passed_over_after_the_bound() {
    let Some(t) = test_felix().await else {
        return;
    };
    let bound = Duration::from_secs(8);
    let nodes = ["s3-a", "s3-b"];
    let _agents: Vec<Agent> = nodes.iter().map(|n| start(&t, n, bound)).collect();
    run_scheduler(&t);
    eventually("the scheduler to be claimed", SOON, || {
        holder(&t, names::SCHEDULER)
    })
    .await;
    facts_from(&t, &nodes).await;
    // This one looks up in `nodes` too, but never claims anything.
    let ghost = NodeFact {
        node: "a-stuck".into(),
        every_ms: 3_600_000,
        max_workloads: 64,
        draining: false,
    };
    let info = NodeInfo {
        version: "0.0.0".into(),
        max_workloads: 64,
        held: 0,
        draining: None,
    };
    t.felix
        .cache_put(names::NODES, "a-stuck", Bytes::from(info.encode()))
        .await
        .unwrap();
    t.felix.publish(names::FACTS, ghost.encode()).await.unwrap();
    facts_from(&t, &["a-stuck"]).await;

    assert!(run_counter(&t, "stuck").await.success());
    let placed = assignment(&t, "stuck", None).await;
    assert_eq!(placed.node, "a-stuck");
    let seen = Instant::now();
    // Agents may read the assignment up to a poll before this test does.
    let early = bound - Duration::from_millis(1500);
    let owner = eventually(
        "a live node to claim it",
        bound + Duration::from_secs(10),
        || holder(&t, "stuck"),
    )
    .await;
    let after = seen.elapsed();
    assert!(nodes.contains(&owner.as_str()), "{owner}");
    assert!(
        after >= early,
        "{owner} claimed it {after:?} after the assignment, inside the {bound:?} bound"
    );
}

#[tokio::test]
async fn a_replay_of_the_scheduler_gives_the_same_assignments() {
    let Some(t) = test_felix().await else {
        return;
    };
    let bound = Duration::from_secs(60);
    let nodes = ["s4-a", "s4-b", "s4-c"];
    let mut agents: HashMap<&str, Agent> =
        nodes.iter().map(|&n| (n, start(&t, n, bound))).collect();
    run_scheduler(&t);
    let first = eventually("the scheduler to be claimed", SOON, || {
        holder(&t, names::SCHEDULER)
    })
    .await;
    facts_from(&t, &nodes).await;
    for id in ["r-1", "r-2", "r-3", "r-4"] {
        assert!(run_counter(&t, id).await.success());
        assignment(&t, id, None).await;
    }

    // A stop, written as a spec record, drops an assignment.
    let mut stopped = t.spec_of("r-2").await;
    stopped.state = WorkloadState::Stopped;
    stopped.record = None;
    t.felix
        .publish(names::SPECS, SpecRecord::new("r-2", stopped).encode())
        .await
        .unwrap();
    eventually("r-2 to be unassigned", SOON, || async {
        (!assigned(&t).await.contains_key("r-2")).then_some(())
    })
    .await;

    // The scheduler fails over mid-history, so the replay crosses a takeover
    // and a snapshot restore.
    agents.get_mut(first.as_str()).unwrap().kill();
    eventually("another node to take the scheduler over", SOON, || async {
        holder(&t, names::SCHEDULER).await.filter(|n| *n != first)
    })
    .await;
    // A draining node's workloads move.
    let survivor = nodes.iter().find(|&&n| n != first).unwrap();
    let draining_on: Vec<String> = assigned(&t)
        .await
        .into_iter()
        .filter(|(_, p)| p.node == *survivor)
        .map(|(id, _)| id)
        .collect();
    let rf = replication_factor().to_string();
    let drain = t
        .felixd(&["drain", survivor, "--replication-factor", &rf])
        .status()
        .unwrap();
    assert!(drain.success());
    eventually("the drained node's workloads to move", SOON, || async {
        let now = assigned(&t).await;
        draining_on
            .iter()
            .all(|id| now.get(id).is_some_and(|p| p.node != *survivor))
            .then_some(())
    })
    .await;

    let recorded = scheduler_outputs(&t).await;
    assert!(recorded >= 5, "only {recorded} assignments");
    let (ok, report) = t.check(&["--id", names::SCHEDULER]);
    assert!(ok, "felixd check found problems:\n{report}");
    let (ok, diffed) = t.replay(&["--id", names::SCHEDULER, "--diff"]);
    assert!(ok, "felixd replay --diff: {diffed}");
    // The diff compares every output up to the merge log's tail. Outputs
    // written after the read above can only add to it.
    let matched: usize = diffed
        .split(": ")
        .nth(1)
        .and_then(|rest| rest.split(' ').next())
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("unexpected replay output: {diffed}"));
    assert!(
        matched >= recorded,
        "{matched} outputs match, of {recorded} recorded: {diffed}"
    );
}
