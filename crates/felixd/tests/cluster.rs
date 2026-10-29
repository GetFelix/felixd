//! Against three Felix brokers with three-replica `Quorum` streams
//! (`tests/felix/cluster.yaml`). Skipped unless `FELIXD_CLUSTER` names that
//! compose file. `FELIXD_COMPOSE` is the compose command (default
//! `docker compose`) and `FELIX_OPERATOR_TOKEN_FILE` a token that may read
//! shard assignments.

mod common;

use std::process::Command;
use std::time::Duration;

use common::{
    INPUT, OUTPUT, SNAPSHOTS, TestFelix, assert_uninterrupted_on, eventually, test_felix,
};
use felixd_agent::felix::Felix;
use felixd_core::spec::WorkloadState;
use felixd_core::{names, snapshot};
use serde_json::Value;

const SNAPSHOT_EVERY: u64 = 5;

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

fn compose(args: &[&str]) {
    let file = std::env::var("FELIXD_CLUSTER").unwrap();
    let command = std::env::var("FELIXD_COMPOSE").unwrap_or_else(|_| "docker compose".into());
    let mut words = command.split_whitespace();
    let status = Command::new(words.next().unwrap())
        .args(words)
        .args(["-f", &file])
        .args(args)
        .status()
        .unwrap();
    assert!(status.success(), "compose {args:?} failed");
}

/// Every shard assignment in the test's namespace.
async fn assignments(t: &TestFelix) -> Vec<Value> {
    let url = format!("{}/v1/shard-assignments", t.settings.controlplane_url);
    let token = std::fs::read_to_string(std::env::var("FELIX_OPERATOR_TOKEN_FILE").unwrap())
        .unwrap()
        .trim()
        .to_owned();
    let body: Value = tokio::task::spawn_blocking(move || {
        ureq::get(&url)
            .header("Authorization", &format!("Bearer {token}"))
            .call()
            .unwrap()
            .body_mut()
            .read_json()
            .unwrap()
    })
    .await
    .unwrap();
    let items = body["items"].as_array().unwrap().iter();
    items
        .filter(|item| item["namespace"] == t.settings.namespace.as_str())
        .cloned()
        .collect()
}

/// The broker leading `stream`'s shard and the assignment's generation.
async fn leader(t: &TestFelix, stream: &str) -> Option<(String, u64)> {
    assignments(t).await.iter().find_map(|item| {
        (item["stream"] == stream).then(|| {
            let leader = item["leader"].as_str().unwrap().to_owned();
            (leader, item["generation"].as_u64().unwrap())
        })
    })
}

/// Creates felixd's caches with three replicas before `felixd run` would
/// create them with one, so losing a broker loses no control state.
async fn replicated_caches(t: &TestFelix) {
    let settings = &t.settings;
    let url = format!(
        "{}/v1/tenants/{}/namespaces/{}/caches",
        settings.controlplane_url, settings.tenant, settings.namespace
    );
    let token = settings.controlplane_token.clone().unwrap();
    for cache in [
        names::WORKLOADS,
        names::MODULES,
        names::LEASES,
        names::EPOCHS,
        names::NODES,
    ] {
        let body = serde_json::json!({
            "cache": cache,
            "display_name": cache,
            "replication_factor": 3,
            "consistency": "Quorum",
        });
        let (url, token) = (url.clone(), token.clone());
        tokio::task::spawn_blocking(move || {
            ureq::post(&url)
                .header("Authorization", &format!("Bearer {token}"))
                .send_json(&body)
                .unwrap();
        })
        .await
        .unwrap();
    }
}

async fn publish_range(t: &TestFelix, inputs: std::ops::Range<u64>) {
    for i in inputs {
        // The client learns of a new leader from a failed attempt. Only
        // called once the old leader is down, so a failed one did not land.
        let mut attempts = 0;
        while let Err(error) = t.felix.publish(INPUT, count(i)).await {
            attempts += 1;
            assert!(attempts < 5, "publish failed: {error:#}");
        }
    }
}

/// The owner is killed with the broker that leads `.out`. A promotion writes
/// a generation-start record at the output stream's tail, right after a
/// snapshot that covers every output, and the next owner must resume past it.
#[tokio::test(flavor = "multi_thread")]
async fn generation_starts_after_failover() {
    if std::env::var_os("FELIXD_CLUSTER").is_none() {
        eprintln!("skipping: FELIXD_CLUSTER is not set");
        return;
    }
    let Some(mut t) = test_felix().await else {
        return;
    };
    replicated_caches(&t).await;
    let every = SNAPSHOT_EVERY.to_string();
    let module = t.submit("counter", &["--snapshot-every", &every]).await;
    let mut first = t.start_agent();

    // A multiple of the snapshot interval, so the last snapshot ends at the
    // output stream's tail.
    // New shards start with a generation-start record too, so input offsets
    // are not input counts.
    publish_range(&t, 0..4 * SNAPSHOT_EVERY).await;
    let (last_input, _) = t.read_all(INPUT).await.pop().unwrap();
    eventually(
        "a snapshot of the last input",
        Duration::from_secs(60),
        || async {
            let last = t.read_all(SNAPSHOTS).await.pop()?;
            let (taken, _) = snapshot::decode(&last.1).unwrap();
            (taken.header.input_offset == last_input).then_some(())
        },
    )
    .await;
    first.kill();

    let (old, generation) = leader(&t, OUTPUT)
        .await
        .expect("the output stream has a leader");
    eprintln!("killing {old}, which leads {OUTPUT} at generation {generation}");
    compose(&["kill", &old]);
    let (new, _) = eventually(
        "a new leader for the output",
        Duration::from_secs(90),
        || async {
            leader(&t, OUTPUT)
                .await
                .filter(|(leader, g)| *leader != old && *g > generation)
        },
    )
    .await;
    eprintln!("{new} leads {OUTPUT} now");
    eventually(
        "every shard to leave the killed broker",
        Duration::from_secs(90),
        || async {
            let items = assignments(&t).await;
            items
                .iter()
                .all(|item| item["leader"] != old.as_str())
                .then_some(())
        },
    )
    .await;
    // A client that knew the old leaders can still try them first.
    t.felix = Felix::connect(&t.settings).await.unwrap();

    let _second = t.start_agent();
    publish_range(&t, 4 * SNAPSHOT_EVERY..8 * SNAPSHOT_EVERY).await;
    assert_uninterrupted_on(&t, &module, 8 * SNAPSHOT_EVERY, OUTPUT).await;
    assert_eq!(t.spec().await.state, WorkloadState::Running);
    let tail = t.felix.tail(OUTPUT).await.unwrap();
    let records = t.read_all(OUTPUT).await.len() as u64;
    assert!(
        tail > records,
        "{OUTPUT} ends at {tail} with {records} records, so no generation start was written"
    );
    let (ok, report) = t.check(&["--complete"]);
    assert!(ok, "felixd check found problems:\n{report}");
    compose(&["start", &old]);
}
