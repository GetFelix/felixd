//! An agent dials brokers by host name, and keeps running when its token file
//! is rotated underneath it.
//!
//! Needs a Felix broker; see `common`. The rotation test swaps the file's
//! content through the gaps a rotation leaves (missing, empty, half written)
//! and back; it cannot mint a token that expires, so a token outliving its
//! expiry is covered by the unit tests of the token source.

mod common;

use std::time::Duration;

use common::{ID, INPUT, LEASE_TTL, assert_uninterrupted, eventually, test_felix};

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_agent_dials_brokers_by_name() {
    let Some(t) = test_felix().await else { return };
    let Ok(brokers) = std::env::var("FELIX_BROKERS") else {
        return;
    };
    if !brokers.split(',').all(|b| b.starts_with("127.0.0.1:")) {
        eprintln!("skipping: the brokers are not on loopback, so localhost does not reach them");
        return;
    }
    let named = brokers.replace("127.0.0.1:", "localhost:");
    let module = t.submit("counter", &["--snapshot-every", "10"]).await;
    let ttl = LEASE_TTL.as_millis().to_string();
    let mut agent = t
        .felixd(&["agent", "--node-id", "named", "--lease-ttl-ms", &ttl])
        .env("FELIX_BROKERS", &named)
        .spawn()
        .unwrap();
    t.publish_all(INPUT, (0..20).map(count)).await;
    assert_uninterrupted(&t, &module, 20).await;
    assert!(agent.try_wait().unwrap().is_none());
    agent.kill().unwrap();
    agent.wait().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rotated_token_file_does_not_stop_a_running_agent() {
    let Some(t) = test_felix().await else { return };
    let dir = std::env::temp_dir().join(format!("felixd-rotate-{}", t.settings.namespace));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("broker.jwt");
    let token = t.settings.token.current();
    std::fs::write(&file, token.expose()).unwrap();

    let module = t.submit("counter", &["--snapshot-every", "10"]).await;
    let mut agent = t.start_node_with_token("rotated", file.to_str().unwrap(), "");
    t.publish_all(INPUT, (0..20).map(count)).await;
    assert_uninterrupted(&t, &module, 20).await;
    let (lease, _) = t.lease(ID).await.expect("a live lease");

    // The gaps a rotation can leave, then the new file.
    std::fs::remove_file(&file).unwrap();
    tokio::time::sleep(LEASE_TTL).await;
    std::fs::write(&file, "").unwrap();
    tokio::time::sleep(LEASE_TTL).await;
    std::fs::write(&file, token.expose()).unwrap();

    t.publish_all(INPUT, (20..40).map(count)).await;
    assert_uninterrupted(&t, &module, 40).await;
    eventually("the lease still held", Duration::from_secs(10), || async {
        let (now, _) = t.lease(ID).await?;
        (now.owner == lease.owner).then_some(())
    })
    .await;
    assert!(
        agent.alive(),
        "the agent exited while its token was rotated"
    );
    agent.kill();
    std::fs::remove_dir_all(&dir).unwrap();
}
