//! The scheduler workload (#60): it assigns workloads to nodes, the nodes
//! claim what is assigned to them through the lease, its assignments carry
//! on after its own node dies, nothing waits on an assignee that cannot take
//! the work or on a scheduler with no holder, an assignee that does nothing
//! is passed over after the bound, a replay assigns the same way, each claim
//! records who placed it on `wl.<id>.placed`, and with `keep_snapshots` its
//! holder deletes the node facts it no longer reads.
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
use felixd_agent::drain;
use felixd_agent::felix::Felix;
use felixd_core::names;
use felixd_core::node::NodeInfo;
use felixd_core::output;
use felixd_core::placement::{Authority, PlacementRecord, Reason};
use felixd_core::schedule::{self, Change, NodeFact, SILENT_INTERVALS};
use felixd_core::spec::{SpecRecord, WorkloadState};
use felixd_core::trim::Trim;

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
/// snapshot, with `more` arguments.
fn run_scheduler(t: &TestFelix, more: &[&str]) {
    let module = felixd_testkit::workload("scheduler");
    let mut args = vec!["--snapshot-every", "5"];
    args.extend(more);
    let status = t.run_on(
        names::SCHEDULER,
        &module,
        &[names::SPECS, names::FACTS],
        &args,
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

/// Waits until every one of `nodes` has a fact `sys.facts` still holds. A
/// scheduler that trims the stream may already have deleted the first ones.
async fn facts_from(t: &TestFelix, nodes: &[&str]) {
    eventually("a fact from every node", SOON, || async {
        let heard: BTreeSet<String> = t
            .read_readable(names::FACTS)
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
    scheduler_with(t, nodes, &[]).await
}

/// As [`scheduler_on`], submitted with `more` arguments.
async fn scheduler_with(t: &TestFelix, nodes: &[&str], more: &[&str]) -> String {
    run_scheduler(t, more);
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
        relist: 0,
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

/// The placement record of `id`'s claim at `epoch`, once it lands.
async fn placement_of(t: &TestFelix, id: &str, epoch: u64) -> Authority {
    let stream = names::placed_stream(id);
    eventually(
        &format!("a placement record for {id} at {epoch}"),
        SOON,
        || async {
            t.read_all(&stream)
                .await
                .iter()
                .filter_map(|(_, r)| PlacementRecord::decode(r).ok())
                .find(|p| p.epoch == epoch)
                .map(|p| p.authority)
        },
    )
    .await
}

async fn epoch_of(t: &TestFelix, id: &str) -> u64 {
    t.lease(id).await.expect("a lease").0.epoch
}

#[tokio::test]
async fn each_claim_records_who_placed_it_and_check_finds_a_record_without_a_claim() {
    let Some(t) = test_felix().await else {
        return;
    };
    let bound = Duration::from_secs(300);
    let nodes = ["s9-a", "s9-b"];
    let mut agents: HashMap<&str, Agent> =
        nodes.iter().map(|&n| (n, start(&t, n, bound))).collect();
    scheduler_on(&t, &nodes).await;
    let scheduler_epoch = epoch_of(&t, names::SCHEDULER).await;
    assert_eq!(
        placement_of(&t, names::SCHEDULER, scheduler_epoch).await,
        Authority::FirstClaim {
            reason: Reason::Scheduler
        }
    );

    assert!(run_counter(&t, "placed").await.success());
    let owner = held_by_assignee(&t, "placed").await;
    let first = epoch_of(&t, "placed").await;
    let Authority::Assigned {
        assignment,
        scheduler_epoch: by,
    } = placement_of(&t, "placed", first).await
    else {
        panic!("the assignee's claim is not recorded as assigned");
    };
    assert_eq!(by, scheduler_epoch);
    assert_eq!(
        assignment_at(&t, assignment, "placed").await.as_deref(),
        Some(owner.as_str()),
        "the placement record names no record that put the workload on {owner}"
    );

    // A crash: the next holder records its own claim.
    eprintln!("killing {owner}, which holds the workload");
    agents.get_mut(owner.as_str()).unwrap().kill();
    let next = eventually("another node to claim the workload", SOON, || async {
        let (lease, _) = t.lease("placed").await?;
        (lease.epoch > first && lease.owner != owner).then_some(lease.epoch)
    })
    .await;
    placement_of(&t, "placed", next).await;
    let (ok, report) = t.check(&["--id", "placed"]);
    assert!(ok, "felixd check found problems:\n{report}");

    // A record no claim stands behind, as a node without the lease could write.
    let rogue = PlacementRecord {
        epoch: next + 100,
        authority: Authority::FirstClaim {
            reason: Reason::NoScheduler,
        },
    };
    t.felix
        .publish(&names::placed_stream("placed"), rogue.encode())
        .await
        .unwrap();
    let (ok, report) = t.check(&["--id", "placed"]);
    assert!(!ok, "felixd check passed a placement record with no claim");
    assert!(
        report.contains(&format!("names epoch {}", next + 100)),
        "{report}"
    );
}

/// The node the scheduler output at `offset` puts `id` on.
async fn assignment_at(t: &TestFelix, offset: u64, id: &str) -> Option<String> {
    let records = t.read_all(&names::output_stream(names::SCHEDULER)).await;
    let (_, record) = records.iter().find(|(at, _)| *at == offset)?;
    let output::Record::Output(_, addressed) = output::decode(record).ok()? else {
        return None;
    };
    match schedule::Record::decode(addressed.payload).ok()?.change {
        Change::Assign { workload, node } if workload == id => node,
        Change::Table {
            mut assignments, ..
        } => assignments.remove(id),
        _ => None,
    }
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

/// The trim records on the scheduler's output stream, in order.
async fn scheduler_trims(t: &TestFelix) -> Vec<Trim> {
    t.read_all(&names::output_stream(names::SCHEDULER))
        .await
        .iter()
        .filter_map(|(_, r)| output::decode(r).unwrap().trim())
        .collect()
}

/// The offset below which a trim record has the scheduler read nothing of
/// `sys.facts`, its second input.
fn facts_floor(trim: &Trim) -> u64 {
    trim.input_floors[1]
}

/// With `keep_snapshots` the scheduler's holder trims `sys.facts` below the
/// floor its trim record names, while the nodes keep appending to it. A
/// failover then resumes from a kept snapshot and places new workloads.
/// A replay starts from a snapshot, so it never reads `sys.facts` below the
/// floor and works. (`felixd check` starts at offset 0 and stops on the
/// trimmed merge log first, which #19 already covers.)
#[tokio::test]
async fn sys_facts_is_trimmed_below_the_schedulers_floor_and_failover_carries_on() {
    let Some(t) = test_felix().await else {
        return;
    };
    let bound = Duration::from_secs(300);
    let ids = ["s7-a", "s7-b"];
    let mut agents: HashMap<&str, Agent> = ids.iter().map(|&n| (n, start(&t, n, bound))).collect();
    let first = scheduler_with(&t, &ids, &["--keep-snapshots", "2"]).await;

    // The first floor past zero is the target: later records only raise it,
    // so it holds still while the nodes' facts keep the stream growing.
    let floor = eventually("sys.facts trimmed", Duration::from_secs(120), || async {
        let floor = scheduler_trims(&t)
            .await
            .iter()
            .map(facts_floor)
            .find(|&floor| floor > 0)?;
        (t.readable_from(names::FACTS).await >= floor).then_some(floor)
    })
    .await;

    agents.get_mut(first.as_str()).unwrap().kill();
    let second = eventually("another node to take the scheduler over", SOON, || async {
        holder(&t, names::SCHEDULER).await.filter(|n| *n != first)
    })
    .await;
    eprintln!("{second} took the scheduler over");
    for id in ["f-1", "f-2"] {
        assert!(run_counter(&t, id).await.success());
        assignment(&t, id, None).await;
    }
    let held = t.read_readable(names::FACTS).await;
    assert!(!held.is_empty(), "sys.facts holds nothing");
    assert!(
        held[0].0 >= floor,
        "{} is below the floor {floor}",
        held[0].0
    );
    assert!(
        held.windows(2).all(|w| w[1].0 > w[0].0),
        "sys.facts is out of order above the floor"
    );
    let below = t.felix.read_range(names::FACTS, 0, floor).await;
    assert!(
        below
            .as_ref()
            .is_err_and(|e| felixd_agent::felix::Trimmed::of(e).is_some()),
        "records below the floor {floor} are still readable: {below:?}"
    );
    let parked = t
        .read_all(&names::output_stream(names::SCHEDULER))
        .await
        .iter()
        .any(|(_, r)| {
            matches!(
                output::decode(r).unwrap(),
                output::Record::NeedsOperator { .. }
            )
        });
    assert!(!parked, "the scheduler resumed into trimmed history");

    // A running scheduler keeps snapshotting and trimming, so by the time the
    // replay connects the oldest kept snapshot can be past `last`, and there
    // is nothing to start from. Stop the nodes so the history holds still.
    for agent in agents.values_mut() {
        agent.kill();
    }
    let merge = names::merge_stream(names::SCHEDULER);
    let last = t.felix.tail(&merge).await.unwrap() - 1;
    let (ok, state) = t.replay(&["--id", names::SCHEDULER, "--to", &last.to_string()]);
    assert!(ok, "felixd replay: {state}");
    assert!(state.contains("f-2"), "{state}");
}

/// Submits the counter as `id` in the group `spec` (`name=a,b`).
async fn run_in_group(t: &TestFelix, id: &str, spec: &str) {
    let kind = StreamKind {
        replication_factor: replication_factor(),
    };
    ensure_stream(&t.settings, INPUT, kind).await.unwrap();
    let module = felixd_testkit::workload("counter");
    assert!(t.run(id, &module, INPUT, &["--group", spec]).success());
}

/// The node each member of `group` is placed on by the record in force.
async fn group_record(t: &TestFelix, group: &str) -> Option<Vec<(String, String)>> {
    let followed = assignments::read(&t.felix).await.unwrap();
    let placed = followed.groups.get(group)?;
    Some(placed.members.clone().into_iter().collect())
}

/// The scheduler output offset of the `group` record in force.
async fn group_offset(t: &TestFelix, group: &str) -> u64 {
    let followed = assignments::read(&t.felix).await.unwrap();
    followed.groups.get(group).expect("a group record").offset
}

/// Waits until both members of `group` are held and returns their holders.
async fn group_held(t: &TestFelix, group: &str, members: [&str; 2]) -> [String; 2] {
    eventually(&format!("group {group} to be held"), SOON, || async {
        let x = holder(t, members[0]).await?;
        let y = holder(t, members[1]).await?;
        Some([x, y])
    })
    .await
}

fn group_args(bound: &str) -> Vec<&str> {
    vec!["--group-bound-ms", bound]
}

#[tokio::test]
async fn a_group_is_placed_whole_and_both_members_follow_its_record() {
    let Some(t) = test_felix().await else {
        return;
    };
    let nodes = ["g1-a", "g1-b", "g1-c"];
    let more = group_args("300000");
    let _agents: Vec<Agent> = nodes
        .iter()
        .map(|n| start_with(&t, n, Duration::from_secs(300), "", &more))
        .collect();
    scheduler_on(&t, &nodes).await;
    run_in_group(&t, "gx", "pair=gx,gy").await;
    run_in_group(&t, "gy", "pair=gx,gy").await;
    let held = group_held(&t, "pair", ["gx", "gy"]).await;
    let record = group_record(&t, "pair").await.expect("a group record");
    assert_eq!(
        record,
        [
            ("gx".to_owned(), held[0].clone()),
            ("gy".to_owned(), held[1].clone())
        ]
    );
    for (id, epoch) in [
        ("gx", epoch_of(&t, "gx").await),
        ("gy", epoch_of(&t, "gy").await),
    ] {
        assert!(matches!(
            placement_of(&t, id, epoch).await,
            Authority::Assigned { .. }
        ));
    }
    let (ok, report) = t.check(&[]);
    assert!(ok, "felixd check found problems:\n{report}");
}

#[tokio::test]
async fn a_group_that_never_completes_is_released_after_the_bound() {
    let Some(t) = test_felix().await else {
        return;
    };
    let nodes = ["g2-a", "g2-b"];
    let bound = Duration::from_secs(8);
    let more = group_args("8000");
    let _agents: Vec<Agent> = nodes
        .iter()
        .map(|n| start_with(&t, n, Duration::from_secs(300), "", &more))
        .collect();
    scheduler_on(&t, &nodes).await;
    // Its partner is never submitted.
    let submitted = Instant::now();
    run_in_group(&t, "lonely", "pair=lonely,missing").await;
    claimed_by(&t, "lonely", &nodes, bound + Duration::from_secs(60)).await;
    let after = submitted.elapsed();
    assert!(
        after >= bound - Duration::from_millis(500),
        "claimed {after:?} after it was submitted, inside the {bound:?} bound"
    );
    let epoch = epoch_of(&t, "lonely").await;
    assert_eq!(
        placement_of(&t, "lonely", epoch).await,
        Authority::FirstClaim {
            reason: Reason::GroupBound
        }
    );
}

#[tokio::test]
async fn a_member_freed_by_a_node_death_is_placed_again_inside_the_bound() {
    let Some(t) = test_felix().await else {
        return;
    };
    let nodes = ["g3-a", "g3-b", "g3-c"];
    let more = group_args("300000");
    let mut agents: HashMap<&str, Agent> = nodes
        .iter()
        .map(|&n| (n, start_with(&t, n, Duration::from_secs(300), "", &more)))
        .collect();
    let sched = scheduler_on(&t, &nodes).await;
    run_in_group(&t, "dx", "pair=dx,dy").await;
    run_in_group(&t, "dy", "pair=dx,dy").await;
    let held = group_held(&t, "pair", ["dx", "dy"]).await;
    // Members land on different nodes, so one of them is not the
    // scheduler's.
    let (freed, kept) = if held[0] != sched { (0, 1) } else { (1, 0) };
    let ids = ["dx", "dy"];
    let victim = held[freed].clone();
    eprintln!("killing {victim}, which holds {}", ids[freed]);
    agents.get_mut(victim.as_str()).unwrap().kill();
    let again = eventually("the freed member to be held again", SOON, || async {
        holder(&t, ids[freed]).await.filter(|n| *n != victim)
    })
    .await;
    assert_ne!(again, victim);
    // The surviving member stayed, and the one placed again was placed by
    // the record, not by the bound.
    assert_eq!(
        holder(&t, ids[kept]).await.as_deref(),
        Some(held[kept].as_str())
    );
    let epoch = epoch_of(&t, ids[freed]).await;
    assert!(matches!(
        placement_of(&t, ids[freed], epoch).await,
        Authority::Assigned { .. }
    ));
}

#[tokio::test]
async fn a_group_record_survives_a_scheduler_crash_and_replays() {
    let Some(t) = test_felix().await else {
        return;
    };
    let nodes = ["g4-a", "g4-b", "g4-c"];
    let more = group_args("300000");
    let mut agents: HashMap<&str, Agent> = nodes
        .iter()
        .map(|&n| (n, start_with(&t, n, Duration::from_secs(300), "", &more)))
        .collect();
    let first = scheduler_on(&t, &nodes).await;
    run_in_group(&t, "rx", "pair=rx,ry").await;
    run_in_group(&t, "ry", "pair=rx,ry").await;
    let held = group_held(&t, "pair", ["rx", "ry"]).await;
    let before = group_record(&t, "pair").await.expect("a group record");
    let offset = group_offset(&t, "pair").await;
    agents.get_mut(first.as_str()).unwrap().kill();
    eventually("another node to take the scheduler over", SOON, || async {
        holder(&t, names::SCHEDULER).await.filter(|n| *n != first)
    })
    .await;
    // The new holder restores the groups from its snapshot or the stream
    // and does not move members that still run.
    tokio::time::sleep(FACTS_EVERY * SILENT_INTERVALS as u32 + Duration::from_secs(5)).await;
    for (id, node) in ["rx", "ry"].iter().zip(&held) {
        if *node != first {
            assert_eq!(holder(&t, id).await.as_ref(), Some(node));
        }
    }
    let after = group_record(&t, "pair").await.expect("a group record");
    for ((m, was), (_, now)) in before.iter().zip(&after) {
        if *was != first {
            assert_eq!(was, now, "{m} moved");
        }
    }
    // A holder that did not restore the groups would write the record again
    // for the same nodes, at a later offset.
    if before.iter().all(|(_, node)| *node != first) {
        assert_eq!(
            group_offset(&t, "pair").await,
            offset,
            "the group was placed again"
        );
    }
    let (ok, report) = t.check(&["--id", names::SCHEDULER]);
    assert!(ok, "felixd check found problems:\n{report}");
    let (ok, diffed) = t.replay(&["--id", names::SCHEDULER, "--diff"]);
    assert!(ok, "felixd replay --diff: {diffed}");
}

/// Marks `node` as draining and waits until it holds nothing.
async fn drained(t: &TestFelix, node: &str) {
    drain::mark(&t.settings, node, replication_factor())
        .await
        .unwrap();
    eventually(&format!("{node} to drain"), SOON, || async {
        let progress = drain::progress(&t.felix, node).await.ok()?;
        progress.done().then_some(())
    })
    .await;
}

/// A node lifted from a drain waits for the scheduler to answer its relist
/// nonce before it claims a group member its record places on it (#257),
/// and that answer comes: the members come back long before the bound.
#[tokio::test]
async fn an_undrained_node_takes_its_group_back_well_before_the_bound() {
    let Some(t) = test_felix().await else {
        return;
    };
    let nodes = ["g9-a", "g9-b"];
    let more = group_args("300000");
    let _agents: Vec<Agent> = nodes
        .iter()
        .map(|n| start_with(&t, n, Duration::from_secs(300), "", &more))
        .collect();
    scheduler_on(&t, &nodes).await;
    run_in_group(&t, "g9x", "pair=g9x,g9y").await;
    run_in_group(&t, "g9y", "pair=g9x,g9y").await;
    // The first record may put the members on different nodes, so drain the
    // other node and wait for the scheduler to join the group on `on`.
    let [on, _] = group_held(&t, "pair", ["g9x", "g9y"]).await;
    let other = nodes.iter().find(|n| **n != on).unwrap();
    drained(&t, other).await;
    eventually("the group record to join on one node", SOON, || async {
        let record = group_record(&t, "pair").await?;
        record.iter().all(|(_, n)| *n == on).then_some(())
    })
    .await;
    let held = group_held(&t, "pair", ["g9x", "g9y"]).await;
    assert_eq!(held, [on.clone(), on.clone()]);
    // With no taker left, the record keeps the group on `on`.
    drained(&t, &on).await;
    let record = group_record(&t, "pair").await.expect("a group record");
    assert!(record.iter().all(|(_, n)| *n == on), "{record:?}");

    // SOON is well under the bound.
    assert!(drain::lift(&t.settings, &on).await.unwrap());
    let held = group_held(&t, "pair", ["g9x", "g9y"]).await;
    assert_eq!(held, [on.clone(), on.clone()]);
    for id in ["g9x", "g9y"] {
        let epoch = epoch_of(&t, id).await;
        assert!(matches!(
            placement_of(&t, id, epoch).await,
            Authority::Assigned { .. }
        ));
    }
}
