//! A node holding only the grants docs/design.md gives one runs a workload
//! whose input it may read but not publish to.
//!
//! Needs a Felix broker; see `common`. The node's token comes from
//! `FELIXD_RESTRICTED_TOKEN_FILE`, which `tests/felix/compose.yaml` mints for
//! namespace `restricted` only.

mod common;

use std::time::Duration;

use common::{INPUT, OUTPUT, eventually, test_felix_in};

const NAMESPACE: &str = "restricted";

#[tokio::test(flavor = "multi_thread")]
async fn a_node_without_publish_on_its_input_runs_the_workload() {
    let Ok(token) = std::env::var("FELIXD_RESTRICTED_TOKEN_FILE") else {
        if std::env::var_os("FELIXD_REQUIRE_FELIX").is_some() {
            panic!("FELIXD_REQUIRE_FELIX is set but FELIXD_RESTRICTED_TOKEN_FILE is not");
        }
        eprintln!("skipping: FELIXD_RESTRICTED_TOKEN_FILE is not set");
        return;
    };
    let Some(t) = test_felix_in(Some(NAMESPACE)).await else {
        return;
    };
    t.submit("counter", &[]).await;
    // The namespace outlives a run against the same stack, so count from
    // what is already there.
    let before = t.read_all(INPUT).await.len();
    t.publish_all(INPUT, (0..3u64).map(|i| i.to_be_bytes().to_vec()))
        .await;

    let mut agent = t.start_node_with_token("restricted-node", &token);
    let want = before + 3;
    eventually("the outputs", Duration::from_secs(60), || async {
        (t.outputs(OUTPUT).await.len() >= want).then_some(())
    })
    .await;
    assert!(agent.alive());
    assert_eq!(t.needs_operator().await.0, []);
}
