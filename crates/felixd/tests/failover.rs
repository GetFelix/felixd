//! Three agents, several workloads and a steady producer, while a kill loop
//! repeatedly kills the node holding the most leases and starts it again
//! under the same node id. At the end every output stream must pass
//! `felixd check --complete`.
//!
//! `FELIXD_FAILOVER_SECS` sets how long the kill loop runs (default 30); the
//! scheduled soak in CI runs it for longer.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use common::{Agent, INPUT, LEASE_TTL, TestFelix, assert_uninterrupted_on, eventually, test_felix};
use felixd_core::names;

const NODES: [&str; 3] = ["a", "b", "c"];
const WORKLOADS: usize = 6;
/// How often the loop kills a node.
const KILL_EVERY: Duration = Duration::from_secs(3);
/// How long a killed node stays down.
const DOWN_FOR: Duration = Duration::from_millis(1500);

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

fn start(t: &TestFelix, node_id: &str) -> Agent {
    let args = ["--max-workloads", "6", "--claim-delay-ms", "500"];
    t.start_node_with_args(node_id, "", LEASE_TTL, &args)
}

/// How many leases each node holds.
async fn holders(t: &TestFelix, ids: &[String]) -> BTreeMap<String, usize> {
    let mut held = BTreeMap::new();
    for id in ids {
        if let Some((lease, _)) = t.lease(id).await
            && lease.epoch > 0
        {
            *held.entry(lease.owner).or_default() += 1;
        }
    }
    held
}

#[tokio::test(flavor = "multi_thread")]
async fn output_survives_repeated_kills_of_the_lease_holders() {
    let Some(t) = test_felix().await else { return };
    let secs: u64 = std::env::var("FELIXD_FAILOVER_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);
    let ids: Vec<String> = (0..WORKLOADS).map(|i| format!("f{i}")).collect();
    let mut module = Vec::new();
    for id in &ids {
        module = t
            .submit_as(id, "counter", &["--snapshot-every", "25"])
            .await;
    }
    let mut agents: Vec<Agent> = NODES.iter().map(|n| start(&t, n)).collect();

    let stop = Arc::new(AtomicBool::new(false));
    let published = Arc::new(AtomicU64::new(0));
    let producer = {
        let felix = felixd_agent::felix::Felix::connect(&t.settings)
            .await
            .unwrap();
        let (stop, published) = (stop.clone(), published.clone());
        tokio::spawn(async move {
            let mut i = 0;
            while !stop.load(Ordering::Relaxed) {
                felix.publish(INPUT, count(i)).await.unwrap();
                i += 1;
                published.store(i, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
    };

    let until = Instant::now() + Duration::from_secs(secs);
    let mut kills = 0;
    while Instant::now() < until {
        tokio::time::sleep(KILL_EVERY).await;
        let held = holders(&t, &ids).await;
        let Some((victim, n)) = held.iter().max_by_key(|(_, n)| **n) else {
            continue;
        };
        let index = NODES.iter().position(|node| node == victim).unwrap();
        eprintln!("kill {victim}, holding {n}");
        agents[index].kill();
        kills += 1;
        tokio::time::sleep(DOWN_FOR).await;
        agents[index] = start(&t, victim);
    }
    stop.store(true, Ordering::Relaxed);
    producer.await.unwrap();
    let inputs = published.load(Ordering::Relaxed);
    eprintln!("{kills} kills, {inputs} inputs");
    assert!(
        kills >= secs / KILL_EVERY.as_secs() / 2,
        "only {kills} kills"
    );

    for id in &ids {
        assert_uninterrupted_on(&t, &module, inputs, &names::output_stream(id)).await;
    }
    let (ok, report) = t.check(&["--complete"]);
    assert!(ok, "felixd check found problems:\n{report}");
    eventually("every workload held", Duration::from_secs(30), || async {
        (holders(&t, &ids).await.values().sum::<usize>() == WORKLOADS).then_some(())
    })
    .await;
}
