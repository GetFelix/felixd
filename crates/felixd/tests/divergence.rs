//! A node whose replay differs from the output stream records the divergence
//! before it fails the workload, and a node that has been deposed does not.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::process::Stdio;
use std::time::Duration;

use common::{Agent, ID, INPUT, OUTPUT, eventually, test_felix};
use felixd_agent::ps::ps;
use felixd_core::class::ErrorClass;
use felixd_core::names;
use felixd_core::output::{self, OutputHeader, Record, compared_digest};
use felixd_core::spec::WorkloadState;
use felixd_core::status::State;

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

fn header(input_offset: u64) -> OutputHeader {
    OutputHeader {
        input_offset,
        index: 0,
    }
}

fn output_record(input_offset: u64, epoch: u64, payload: &[u8]) -> Vec<u8> {
    output::encode(header(input_offset), epoch, payload)
}

/// What a divergence record on the output stream holds, with its offset.
#[derive(Debug)]
struct Found {
    offset: u64,
    epoch: u64,
    spec: u64,
    header: OutputHeader,
    snapshot: Option<u64>,
    node: String,
    abi: (String, String, String),
    expected: Option<[u8; 32]>,
    actual: Option<[u8; 32]>,
    detail: String,
}

async fn divergences(t: &common::TestFelix) -> Vec<Found> {
    let text = |bytes: &[u8]| String::from_utf8_lossy(bytes).into_owned();
    let mut found = Vec::new();
    for (offset, bytes) in t.read_all(OUTPUT).await {
        if let Ok(Record::Divergence { epoch, found: d }) = output::decode(&bytes) {
            found.push(Found {
                offset,
                epoch,
                spec: d.spec,
                header: d.header,
                snapshot: d.snapshot,
                node: text(d.node),
                abi: (
                    text(d.abi.contract),
                    text(d.abi.wasmtime),
                    text(d.abi.felixd),
                ),
                expected: d.expected.copied(),
                actual: d.actual.copied(),
                detail: text(d.detail),
            });
        }
    }
    found
}

/// The offsets of the failure records on the output stream, with their epochs.
async fn failures(t: &common::TestFelix) -> Vec<(u64, u64)> {
    t.read_all(OUTPUT)
        .await
        .into_iter()
        .filter_map(|(offset, bytes)| match output::decode(&bytes) {
            Ok(Record::Failure { epoch, .. }) => Some((offset, epoch)),
            _ => None,
        })
        .collect()
}

/// The stream holds a wrong output for input 0, as when the module that
/// wrote it behaved otherwise. The node replays input 0 from the initial
/// state, since no snapshot exists, finds its output differing from the
/// record, and appends a divergence record before the failure record. The
/// failure stays poison, `felixd ps` shows the divergence, and
/// `felixd replay --diff` names the record.
#[tokio::test(flavor = "multi_thread")]
async fn a_replay_that_differs_from_the_stream_is_recorded_before_the_failure() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..3).map(count)).await;
    // The counter's first two outputs are 1 and 2.
    let held = output_record(0, 0, &count(9));
    t.publish_all(OUTPUT, [held.clone(), output_record(1, 0, &count(2))])
        .await;
    let record = t.spec().await.record.expect("a spec record");
    let agent = t.start_agent();

    let failure = t.failure().await;
    assert!(failure.contains("replayed output differs"), "{failure}");
    let spec = t.spec().await;
    assert_eq!(spec.state, WorkloadState::Failed);
    assert_eq!(spec.failure.expect("a reason").class, ErrorClass::Poison);

    let found = divergences(&t).await;
    assert_eq!(found.len(), 1, "{found:?}");
    let d = &found[0];
    assert_eq!((d.spec, d.header, d.snapshot), (record, header(0), None));
    assert_eq!(d.node, agent.node_id);
    assert_eq!(d.abi.0, "0.2.0");
    assert_eq!(d.abi.1, "50.0.0");
    assert_eq!(d.abi.2, env!("CARGO_PKG_VERSION"));
    assert_eq!(d.expected, compared_digest(&held));
    assert_eq!(d.actual, compared_digest(&output_record(0, 0, &count(1))));
    assert_ne!(d.expected, d.actual);
    assert!(d.detail.contains("replayed output differs"), "{}", d.detail);

    // The failure follows it under the same epoch.
    let failed = failures(&t).await;
    assert_eq!(failed.len(), 1, "{failed:?}");
    assert!(failed[0].0 > d.offset, "{failed:?}");
    assert_eq!(failed[0].1, d.epoch);

    let report = ps(&t.settings, &[ID.to_owned()]).await.unwrap();
    let shown = report.workloads[0]
        .divergence
        .as_ref()
        .expect("ps shows the divergence");
    assert_eq!((shown.input_offset, shown.index), (0, 0));
    assert_eq!(shown.node, agent.node_id);
    assert_eq!(shown.snapshot, None);
    assert_eq!(shown.epoch, d.epoch);
    let printed = t.felixd(&["ps"]).stdout(Stdio::piped()).output().unwrap();
    let printed = String::from_utf8_lossy(&printed.stdout).into_owned();
    assert!(
        printed.contains(&format!(
            "{ID}: diverged at input 0 output 0 on {} at epoch {}",
            agent.node_id, d.epoch
        )),
        "{printed}"
    );
    assert!(
        matches!(report.workloads[0].state, Some(State::Failed(_))),
        "{:?}",
        report.workloads[0].state
    );

    let (ok, diff) = t.replay(&["--id", ID, "--diff"]);
    assert!(!ok, "the diff passed: {diff}");
    assert!(
        diff.starts_with("first divergence at input 0 output 0: "),
        "{diff}"
    );
    assert!(
        diff.contains(&format!(
            "; a divergence record at offset {} says node {} found this at epoch {} \
             (contract 0.2.0, wasmtime 50.0.0, felixd {})",
            d.offset,
            agent.node_id,
            d.epoch,
            env!("CARGO_PKG_VERSION")
        )),
        "{diff}"
    );
}

/// A record that is not this workload's output lands where the next output
/// goes. The node finds it when it appends, which is the reconcile path
/// instead of the skipped-output check, and records the divergence at that
/// input.
#[tokio::test(flavor = "multi_thread")]
async fn a_foreign_record_at_the_tail_is_recorded_as_a_divergence() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..5).map(count)).await;
    let _agent = t.start_agent();
    eventually("the first outputs", Duration::from_secs(30), || async {
        (t.outputs(OUTPUT).await.len() >= 5).then_some(())
    })
    .await;
    let bogus = output_record(5, 0, b"bogus");
    t.publish_all(OUTPUT, [bogus.clone()]).await;
    t.publish_all(INPUT, [count(5)]).await;
    let failure = t.failure().await;
    assert!(
        failure.contains("differ from this workload's output"),
        "{failure}"
    );
    let found = divergences(&t).await;
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].header, header(5));
    assert_eq!(found[0].expected, compared_digest(&bogus));
    assert_eq!(
        found[0].actual,
        compared_digest(&output_record(5, 0, &count(6)))
    );
    assert_eq!(failures(&t).await.len(), 1);
}

/// Starts `node_id` with a long TTL and `faults`, waits for it to freeze and
/// ends its lease early, so that once it wakes its own lease check still
/// passes and only what the streams hold can stop it. Returns the node and
/// the epoch it held.
async fn freeze_holder(t: &common::TestFelix, node_id: &str, faults: &str) -> (Agent, u64) {
    let node = t.start_node_with_ttl(node_id, faults, Duration::from_secs(30));
    eventually("the freeze", Duration::from_secs(60), || async {
        node.stopped().then_some(())
    })
    .await;
    let (held, version) = t.lease(ID).await.expect("the frozen node's lease");
    assert_eq!(held.owner, node_id);
    t.felix
        .cache_delete_if(names::LEASES, ID, version)
        .await
        .unwrap();
    (node, held.epoch)
}

/// A node finds the divergence and is frozen just before it appends the
/// record, long enough for another node to take the workload and write its
/// own claim. When it wakes its lease check passes, so only the stream can
/// stop it, and it must write neither the divergence record nor the failure.
#[tokio::test(flavor = "multi_thread")]
async fn a_deposed_node_writes_no_divergence_record() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..3).map(count)).await;
    t.publish_all(
        OUTPUT,
        [
            output_record(0, 0, &count(9)),
            output_record(1, 0, &count(2)),
        ],
    )
    .await;
    let (stale, stale_epoch) = freeze_holder(&t, "stale", "freeze-before-failure-append=1").await;
    let next = t.start_agent();
    let failure = t.failure().await;
    assert!(failure.contains("replayed output differs"), "{failure}");

    stale.resume();
    tokio::time::sleep(Duration::from_secs(3)).await;

    let found = divergences(&t).await;
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].node, next.node_id);
    assert!(found[0].epoch > stale_epoch, "{found:?}");
    let failed = failures(&t).await;
    assert_eq!(failed.len(), 1, "{failed:?}");
    assert!(failed[0].1 > stale_epoch, "{failed:?}");
    let by_stale = t
        .read_all(OUTPUT)
        .await
        .into_iter()
        .filter(|(_, bytes)| output::decode(bytes).is_ok_and(|r| r.epoch() == stale_epoch))
        .count();
    assert_eq!(by_stale, 1, "the stale node wrote more than its claim");
}
