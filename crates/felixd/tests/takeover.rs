//! Several agents and many workloads: they spread across the nodes, a node
//! never holds more than `--max-workloads`, and when a node dies the others
//! take its workloads over within one TTL plus the claim delay.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use common::{Agent, INPUT, LEASE_TTL, TestFelix, assert_uninterrupted_on, eventually, test_felix};
use felixd_core::names;

const CLAIM_DELAY: Duration = Duration::from_millis(1000);

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

fn ids(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("w{i}")).collect()
}

fn start(t: &TestFelix, node_id: &str, max_workloads: usize, ttl: Duration) -> Agent {
    let max = max_workloads.to_string();
    let delay = CLAIM_DELAY.as_millis().to_string();
    let args = ["--max-workloads", &max, "--claim-delay-ms", &delay];
    t.start_node_with_args(node_id, "", ttl, &args)
}

/// Who holds each workload that has a stamped lease.
async fn holders(t: &TestFelix, ids: &[String]) -> BTreeMap<String, String> {
    let mut held = BTreeMap::new();
    for id in ids {
        if let Some((lease, _)) = t.lease(id).await
            && lease.epoch > 0
        {
            held.insert(id.clone(), lease.owner);
        }
    }
    held
}

fn per_node(held: &BTreeMap<String, String>) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for owner in held.values() {
        *counts.entry(owner.clone()).or_default() += 1;
    }
    counts
}

async fn nodes(t: &TestFelix, ids: &[&str]) -> BTreeSet<String> {
    let mut live = BTreeSet::new();
    for id in ids {
        if t.felix.cache_get(names::NODES, id).await.unwrap().is_some() {
            live.insert((*id).to_owned());
        }
    }
    live
}

#[tokio::test(flavor = "multi_thread")]
async fn three_nodes_split_twenty_workloads_and_take_over_a_dead_ones() {
    let Some(t) = test_felix().await else { return };
    let workloads = ids(20);
    let names = ["a", "b", "c"];
    // Each node holds at most 10, so the two that are left can hold all 20.
    let mut agents: Vec<Agent> = names.iter().map(|n| start(&t, n, 10, LEASE_TTL)).collect();
    let mut module = Vec::new();
    for id in &workloads {
        module = t
            .submit_as(id, "counter", &["--snapshot-every", "10"])
            .await;
    }
    t.publish_all(INPUT, (0..50).map(count)).await;

    let held = eventually("every workload held", Duration::from_secs(60), || async {
        let held = holders(&t, &workloads).await;
        (held.len() == workloads.len()).then_some(held)
    })
    .await;
    let counts = per_node(&held);
    assert_eq!(counts.len(), 3, "not spread over every node: {counts:?}");
    assert!(counts.values().all(|&n| n <= 10), "{counts:?}");
    assert_eq!(nodes(&t, &names).await.len(), 3, "every node is in nodes");

    let publishing = {
        let felix = felixd_agent::felix::Felix::connect(&t.settings)
            .await
            .unwrap();
        tokio::spawn(async move {
            for i in 50..250 {
                felix.publish(INPUT, count(i)).await.unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (victim, _) = counts.iter().max_by_key(|(_, n)| **n).unwrap();
    let index = names.iter().position(|n| n == victim).unwrap();
    let moved: Vec<String> = held
        .iter()
        .filter(|(_, owner)| *owner == victim)
        .map(|(id, _)| id.clone())
        .collect();
    let killed_at = Instant::now();
    agents[index].kill();

    eventually(
        "the dead node's workloads to move",
        Duration::from_secs(30),
        || async {
            let now = holders(&t, &moved).await;
            (now.len() == moved.len() && now.values().all(|o| o != victim)).then_some(())
        },
    )
    .await;
    let took = killed_at.elapsed();
    // One TTL for the lease to expire and the claim delay, plus room for the
    // claim's own writes on a busy runner.
    assert!(
        took < LEASE_TTL + CLAIM_DELAY + Duration::from_secs(3),
        "takeover took {took:?}"
    );
    let after = per_node(&holders(&t, &workloads).await);
    assert!(after.values().all(|&n| n <= 10), "{after:?}");
    eventually("the dead node to leave nodes", LEASE_TTL * 3, || async {
        (!nodes(&t, &names).await.contains(victim)).then_some(())
    })
    .await;

    publishing.await.unwrap();
    for id in &workloads {
        assert_uninterrupted_on(&t, &module, 250, &names::output_stream(id)).await;
    }
}

/// A node at `--max-workloads` leaves the rest to other nodes.
#[tokio::test(flavor = "multi_thread")]
async fn a_full_node_leaves_workloads_to_others() {
    let Some(t) = test_felix().await else { return };
    let workloads = ids(3);
    for id in &workloads {
        t.submit_as(id, "counter", &[]).await;
    }
    let _a = start(&t, "a", 2, LEASE_TTL);
    eventually("two workloads held", Duration::from_secs(30), || async {
        (holders(&t, &workloads).await.len() == 2).then_some(())
    })
    .await;
    tokio::time::sleep(LEASE_TTL * 2).await;
    assert_eq!(per_node(&holders(&t, &workloads).await)["a"], 2);

    let _b = start(&t, "b", 2, LEASE_TTL);
    let held = eventually("every workload held", Duration::from_secs(30), || async {
        let held = holders(&t, &workloads).await;
        (held.len() == 3).then_some(held)
    })
    .await;
    assert_eq!(
        per_node(&held),
        BTreeMap::from([("a".into(), 2), ("b".into(), 1)])
    );
}

/// A node that shuts down cleanly deletes its lease, and the other node,
/// which sees the delete on its leases watch, claims at once instead of at
/// its next retry.
#[tokio::test(flavor = "multi_thread")]
async fn a_released_lease_is_claimed_at_once() {
    let Some(t) = test_felix().await else { return };
    let workloads = ids(1);
    t.submit_as(&workloads[0], "counter", &[]).await;
    let ttl = Duration::from_secs(30);
    let a = start(&t, "a", 1, ttl);
    eventually("a holds it", Duration::from_secs(30), || async {
        holders(&t, &workloads)
            .await
            .get("w0")
            .filter(|o| *o == "a")
            .map(|_| ())
    })
    .await;
    let _b = start(&t, "b", 1, ttl);
    tokio::time::sleep(Duration::from_secs(2)).await;

    let released_at = Instant::now();
    a.interrupt();
    eventually("b to claim it", Duration::from_secs(30), || async {
        holders(&t, &workloads)
            .await
            .get("w0")
            .filter(|o| *o == "b")
            .map(|_| ())
    })
    .await;
    let took = released_at.elapsed();
    assert!(
        took < CLAIM_DELAY + Duration::from_secs(3),
        "claim took {took:?}"
    );
}

/// Takeover goes by the lease's expiry time, not by the broker's expiry
/// event: the second node ignores every delete its leases watch delivers.
#[tokio::test(flavor = "multi_thread")]
async fn a_dead_holders_lease_is_claimed_without_an_expiry_event() {
    let Some(t) = test_felix().await else { return };
    let workloads = ids(1);
    t.submit_as(&workloads[0], "counter", &[]).await;
    let mut a = start(&t, "a", 1, LEASE_TTL);
    eventually("a holds it", Duration::from_secs(30), || async {
        holders(&t, &workloads)
            .await
            .get("w0")
            .filter(|o| *o == "a")
            .map(|_| ())
    })
    .await;
    let max = "1".to_owned();
    let delay = CLAIM_DELAY.as_millis().to_string();
    let args = ["--max-workloads", &max, "--claim-delay-ms", &delay];
    let _b = t.start_node_with_args("b", "ignore-lease-deletes=1", LEASE_TTL, &args);
    tokio::time::sleep(Duration::from_secs(2)).await;

    let killed_at = Instant::now();
    a.kill();
    eventually("b to claim it", Duration::from_secs(30), || async {
        holders(&t, &workloads)
            .await
            .get("w0")
            .filter(|o| *o == "b")
            .map(|_| ())
    })
    .await;
    let took = killed_at.elapsed();
    assert!(
        took < LEASE_TTL + CLAIM_DELAY + Duration::from_secs(3),
        "takeover took {took:?}"
    );
}
