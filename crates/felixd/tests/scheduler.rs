//! The scheduler workload (#60): it assigns workloads to nodes, the nodes
//! claim what is assigned to them through the lease, its assignments carry
//! on after its own node dies, nothing waits on an assignee that cannot take
//! the work or on a scheduler with no holder, an assignee that does nothing
//! is passed over after the bound, and a replay assigns the same way.
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
use felixd_agent::felix::Felix;
use felixd_core::names;
use felixd_core::node::NodeInfo;
use felixd_core::output;
use felixd_core::schedule::{NodeFact, SILENT_INTERVALS};
use felixd_core::spec::{SpecRecord, WorkloadState};

/// How long the tests wait for anything that should happen within a few
/// lease TTLs.
const SOON: Duration = Duration::from_secs(90);
/// How often test nodes append facts, so a dead one goes silent quickly.
const FACTS_EVERY: Duration = Duration::from_secs(1);

/// Starts a node that leaves workloads to their assignee for up to `bound`
/// and waits as long for the scheduler to assign a new one, so a test sees
/// the fallback only where it means to.
fn start(t: &TestFelix, node: &str, bound: Duration) -> Agent {
    start_with(t, node, bound, "", &[])
}

fn start_with(t: &TestFelix, node: &str, bound: Duration, faults: &str, more: &[&str]) -> Agent {
    let bound = bound.as_millis().to_string();
    let every = FACTS_EVERY.as_millis().to_string();
    let mut args = vec![
        "--assignment-bound-ms",
        &bound,
        "--assignment-grace-ms",
        &bound,
        "--facts-every-ms",
        &every,
    ];
    args.extend(more);
    t.start_node_with_args(node, faults, LEASE_TTL, &args)
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
    assignments::read(&t.felix).await.unwrap().table
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

/// Starts the scheduler on `nodes` and waits until it runs and has heard
/// from all of them. Returns its holder.
async fn scheduler_on(t: &TestFelix, nodes: &[&str]) -> String {
    run_scheduler(t);
    let holder = eventually("the scheduler to be claimed", SOON, || {
        holder(t, names::SCHEDULER)
    })
    .await;
    facts_from(t, nodes).await;
    holder
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

/// A fact for a node that is not running, which the scheduler then takes
/// for one with room. Its long interval keeps it from going silent during
/// a test, and its id sorts before every test node's, so it is picked first.
async fn fake_fact(t: &TestFelix, node: &str) {
    let fact = NodeFact {
        node: node.into(),
        every_ms: 3_600_000,
        max_workloads: 64,
        held: 0,
        holding: Default::default(),
        draining: false,
        blocked: false,
    };
    t.felix.publish(names::FACTS, fact.encode()).await.unwrap();
    facts_from(t, &[node]).await;
}

/// A `nodes` entry for a node that is not running, rewritten every renewal
/// interval as a live node's is, until dropped.
struct FakeEntry(tokio::task::JoinHandle<()>);

impl Drop for FakeEntry {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn fake_entry(t: &TestFelix, node: &str, held: usize) -> FakeEntry {
    let info = NodeInfo {
        version: "0.0.0".into(),
        max_workloads: 64,
        held,
        draining: None,
        blocked: false,
    };
    let felix = Felix::connect(&t.settings).await.unwrap();
    let node = node.to_owned();
    let every = LEASE_TTL / 3;
    FakeEntry(tokio::spawn(async move {
        loop {
            let value = Bytes::from(info.encode());
            felix.cache_put(names::NODES, &node, value).await.unwrap();
            tokio::time::sleep(every).await;
        }
    }))
}

/// Submits `id`, waits for it to be assigned to `to` and returns when the
/// test saw that.
async fn assigned_to(t: &TestFelix, id: &str, to: &str) -> Instant {
    assert!(run_counter(t, id).await.success());
    let placed = assignment(t, id, None).await;
    assert_eq!(placed.node, to);
    Instant::now()
}

/// How long until a node in `nodes` holds `id`, at most `within`.
async fn claimed_by(t: &TestFelix, id: &str, nodes: &[&str], within: Duration) -> Duration {
    let since = Instant::now();
    let owner = eventually(&format!("a live node to claim {id}"), within, || {
        holder(t, id)
    })
    .await;
    assert!(nodes.contains(&owner.as_str()), "{owner} holds {id}");
    since.elapsed()
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
    let bound = Duration::from_secs(300);
    let ids = ["s1-a", "s1-b", "s1-c"];
    let mut agents: HashMap<&str, Agent> = ids.iter().map(|&n| (n, start(&t, n, bound))).collect();
    let first = scheduler_on(&t, &ids).await;
    for id in ["w-1", "w-2", "w-3"] {
        assert!(run_counter(&t, id).await.success());
        held_by_assignee(&t, id).await;
    }
    let on_first: Vec<(String, Placed)> = assigned(&t)
        .await
        .into_iter()
        .filter(|(_, p)| p.node == first)
        .collect();

    eprintln!("killing {first}, which runs the scheduler");
    agents.get_mut(first.as_str()).unwrap().kill();
    let second = eventually("another node to take the scheduler over", SOON, || async {
        holder(&t, names::SCHEDULER).await.filter(|n| *n != first)
    })
    .await;
    eprintln!("{second} took the scheduler over");
    // The new holder finds the dead node silent and moves what it had.
    for (id, was) in &on_first {
        let moved = assignment(&t, id, Some(was.offset)).await;
        assert_ne!(moved.node, first);
    }
    // Until the scheduler finds the dead node silent it may still assign to
    // it, and other nodes then take that by first claim wins.
    tokio::time::sleep(FACTS_EVERY * SILENT_INTERVALS as u32 + Duration::from_secs(5)).await;

    for id in ["w-4", "w-5", "w-6"] {
        assert!(run_counter(&t, id).await.success());
        let owner = held_by_assignee(&t, id).await;
        assert_ne!(owner, first);
    }
    let (ok, report) = t.check(&["--id", names::SCHEDULER]);
    assert!(ok, "felixd check found problems:\n{report}");
}

#[tokio::test]
async fn a_workload_assigned_to_a_dead_node_is_claimed_at_once() {
    let Some(t) = test_felix().await else {
        return;
    };
    let bound = Duration::from_secs(30);
    let nodes = ["s2-a", "s2-b"];
    let _agents: Vec<Agent> = nodes.iter().map(|n| start(&t, n, bound)).collect();
    scheduler_on(&t, &nodes).await;
    // It said it was there, but it has no `nodes` entry.
    fake_fact(&t, "a-ghost").await;
    assigned_to(&t, "dead", "a-ghost").await;
    let took = claimed_by(&t, "dead", &nodes, bound).await;
    assert!(took < bound / 2, "claimed {took:?} after the assignment");
}

#[tokio::test]
async fn a_workload_assigned_to_a_full_node_is_claimed_at_once() {
    let Some(t) = test_felix().await else {
        return;
    };
    let bound = Duration::from_secs(30);
    let nodes = ["s5-a", "s5-b"];
    let _agents: Vec<Agent> = nodes.iter().map(|n| start(&t, n, bound)).collect();
    scheduler_on(&t, &nodes).await;
    // Its fact is out of date: its `nodes` entry says it is full.
    let _entry = fake_entry(&t, "a-full", 64).await;
    fake_fact(&t, "a-full").await;
    assigned_to(&t, "full", "a-full").await;
    let took = claimed_by(&t, "full", &nodes, bound).await;
    assert!(took < bound / 2, "claimed {took:?} after the assignment");
}

#[tokio::test]
async fn with_the_scheduler_down_a_new_workload_does_not_wait() {
    let Some(t) = test_felix().await else {
        return;
    };
    let bound = Duration::from_secs(30);
    let nodes = ["s6-a", "s6-b"];
    let _agents: Vec<Agent> = nodes.iter().map(|n| start(&t, n, bound)).collect();
    assert!(run_counter(&t, "seed").await.success());
    eventually("seed to be claimed", SOON, || holder(&t, "seed")).await;
    // A scheduler spec no node will run: without its spec record every node
    // leaves it to an operator, so its lease never has a holder.
    let mut spec = t.spec_of("seed").await;
    spec.record = None;
    spec.output = names::output_stream(names::SCHEDULER);
    t.felix
        .cache_put(
            names::WORKLOADS,
            names::SCHEDULER,
            Bytes::from(spec.encode()),
        )
        .await
        .unwrap();
    // Every node has seen it before the workload comes.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(holder(&t, names::SCHEDULER).await.is_none());
    assert!(run_counter(&t, "free").await.success());
    let took = claimed_by(&t, "free", &nodes, bound).await;
    assert!(took < bound / 2, "claimed {took:?} after it was submitted");
}

#[tokio::test]
async fn a_workload_the_scheduler_has_no_room_for_is_claimed_where_room_appears() {
    let Some(t) = test_felix().await else {
        return;
    };
    // Long waits, so a node that waited on the scheduler would show.
    let bound = Duration::from_secs(60);
    let nodes = ["s7-a", "s7-b"];
    let _agents: Vec<Agent> = nodes
        .iter()
        .map(|n| start_with(&t, n, bound, "", &["--max-workloads", "1"]))
        .collect();
    scheduler_on(&t, &nodes).await;
    // One node holds the scheduler, the other this: both are full.
    assert!(run_counter(&t, "first").await.success());
    eventually("first to be claimed", SOON, || holder(&t, "first")).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(run_counter(&t, "late").await.success());
    let record = t.spec_of("late").await.record.expect("a spec record");
    eventually("the scheduler to pass late's spec", SOON, || async {
        (assignments::read(&t.felix).await.unwrap().specs > record).then_some(())
    })
    .await;
    assert!(!assigned(&t).await.contains_key("late"));
    // Room turns up on a node the scheduler does not hear of.
    let _roomy = start_with(&t, "s7-c", bound, "no-facts=1", &["--max-workloads", "4"]);
    let took = claimed_by(&t, "late", &["s7-c"], bound).await;
    assert!(
        took < Duration::from_secs(20),
        "claimed {took:?} after room turned up"
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
    scheduler_on(&t, &nodes).await;
    // This one looks able in `nodes` too, but never claims anything.
    let _entry = fake_entry(&t, "a-stuck", 0).await;
    fake_fact(&t, "a-stuck").await;
    // The bound runs from when a node first saw the workload free, which is
    // after it was submitted.
    let submitted = Instant::now();
    assigned_to(&t, "stuck", "a-stuck").await;
    claimed_by(&t, "stuck", &nodes, bound + Duration::from_secs(60)).await;
    let after = submitted.elapsed();
    assert!(
        after >= bound - Duration::from_millis(500),
        "claimed {after:?} after it was submitted, inside the {bound:?} bound"
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
    let first = scheduler_on(&t, &nodes).await;
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
    assert!(recorded >= 5, "only {recorded} scheduler outputs");
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
