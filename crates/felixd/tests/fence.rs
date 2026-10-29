//! A new owner's claim record fences the output stream before it runs
//! anything, and every reader passes over claims.
//!
//! Needs a Felix broker; see `common`. The `cluster` CI job runs it against
//! three brokers too.

mod common;

use std::time::Duration;

use common::{Agent, ID, INPUT, OUTPUT, SNAPSHOTS, assert_uninterrupted, eventually, test_felix};
use felixd_core::names;
use felixd_core::output::{self, OutputHeader, Record};
use felixd_core::snapshot::{self, SnapshotHeader};
use felixd_core::spec::{Limits, WorkloadState};
use felixd_runtime::Runtime;

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

/// The offsets of the records on the input stream. On a cluster a
/// generation-start record can take offset 0.
async fn input_offsets(t: &common::TestFelix) -> Vec<u64> {
    t.read_all(INPUT).await.iter().map(|(o, _)| *o).collect()
}

/// What the counter writes for the `i`th input, at offset `at[i]`, stamped
/// with `epoch`.
fn counted(at: &[u64], i: usize, epoch: u64) -> Vec<u8> {
    let header = OutputHeader {
        input_offset: at[i],
        index: 0,
    };
    output::encode(header, epoch, &count(i as u64 + 1))
}

/// The epoch of every record on the output stream, and whether it is a claim.
async fn records(t: &common::TestFelix) -> Vec<(u64, bool)> {
    t.read_all(OUTPUT)
        .await
        .iter()
        .map(|(_, r)| {
            let record = output::decode(r).unwrap();
            (record.epoch(), matches!(record, Record::Claim { .. }))
        })
        .collect()
}

/// The holder is frozen just before appending input 19's output, and no more
/// input arrives. The next owner never sees input 19 either, so it writes no
/// output, only its claim. When the frozen holder wakes, its append must be
/// refused by that claim alone.
#[tokio::test(flavor = "multi_thread")]
async fn a_claim_alone_fences_a_frozen_holder_of_an_idle_workload() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &["--snapshot-every", "10"]).await;
    t.publish_all(INPUT, (0..20).map(count)).await;
    let mut frozen = t.start_agent_with_faults("freeze-before-append=20");
    eventually("the freeze", Duration::from_secs(60), || async {
        frozen.stopped().then_some(())
    })
    .await;
    let (stale, _) = t.lease(ID).await.expect("the frozen node's lease");

    let last_input = input_offsets(&t).await[19];
    let _next = t.start_agent_with_faults(&format!("drop-input={last_input}"));
    let (owner, _) = eventually(
        "the next owner's claim",
        Duration::from_secs(60),
        || async {
            t.lease(ID)
                .await
                .filter(|(l, _)| l.owner != stale.owner && l.epoch > 0)
        },
    )
    .await;
    // Time to resume, which writes no output here.
    tokio::time::sleep(Duration::from_secs(3)).await;
    frozen.resume();
    tokio::time::sleep(Duration::from_secs(3)).await;

    let found = records(&t).await;
    let stale_outputs = found
        .iter()
        .filter(|&&(e, claim)| !claim && e == stale.epoch)
        .count();
    assert_eq!(
        stale_outputs, 19,
        "the frozen node wrote after the new claim: {found:?}"
    );
    assert_eq!(found.last(), Some(&(owner.epoch, true)), "{found:?}");
    assert!(frozen.alive(), "the frozen node exited");
    let (held, _) = t.lease(ID).await.expect("a live lease");
    assert_eq!(held.owner, owner.owner);

    t.publish_all(INPUT, [count(20)]).await;
    assert_uninterrupted(&t, &module, 21).await;
    let epochs: Vec<u64> = t
        .outputs(OUTPUT)
        .await
        .iter()
        .map(|(_, r)| output::decode(r).unwrap().epoch())
        .collect();
    assert!(epochs[19..].iter().all(|&e| e == owner.epoch), "{epochs:?}");
    assert_eq!(t.spec().await.state, WorkloadState::Running);
}

/// A holder is frozen after taking its epoch and before writing its claim,
/// and its lease ends. The next owner claims and writes. When the first
/// wakes, its claim goes to the tail it read with its epoch floor, so it is
/// refused, it finds the later claim and stops without writing anything.
#[tokio::test(flavor = "multi_thread")]
async fn a_holder_frozen_before_its_claim_never_claims_after_a_later_owner() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &["--snapshot-every", "10"]).await;
    t.publish_all(INPUT, (0..10).map(count)).await;
    let mut frozen =
        t.start_node_with_ttl("holder", "freeze-before-claim=1", Duration::from_secs(30));
    eventually("the freeze", Duration::from_secs(60), || async {
        frozen.stopped().then_some(())
    })
    .await;
    let (stale, version) = t.lease(ID).await.expect("the frozen node's lease");
    assert!(stale.epoch > 0, "the frozen node took no epoch");
    // Its lease ends early, and its long TTL keeps its own renewal from
    // noticing before it tries to claim.
    t.felix
        .cache_delete_if(names::LEASES, ID, version)
        .await
        .unwrap();

    let _next = t.start_agent();
    eventually(
        "the next owner's output",
        Duration::from_secs(60),
        || async { (t.outputs(OUTPUT).await.len() >= 10).then_some(()) },
    )
    .await;
    frozen.resume();
    tokio::time::sleep(Duration::from_secs(3)).await;
    t.publish_all(INPUT, (10..15).map(count)).await;
    assert_uninterrupted(&t, &module, 15).await;
    let found = records(&t).await;
    assert!(
        found.iter().all(|&(e, _)| e != stale.epoch),
        "the frozen node wrote at epoch {}: {found:?}",
        stale.epoch
    );
    assert!(frozen.alive(), "the frozen node exited");
}

/// Several earlier owners left claims between their outputs and after them.
/// A new owner restores the snapshot, skips and checks the outputs past it
/// with the claims passed over, and carries on.
#[tokio::test(flavor = "multi_thread")]
async fn a_resume_passes_over_claims_between_outputs() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &["--snapshot-every", "1000"]).await;
    t.publish_all(INPUT, (0..10).map(count)).await;
    let at = input_offsets(&t).await;
    let mut workload = Runtime::new()
        .unwrap()
        .load(&module, Limits::default())
        .unwrap();
    for i in 0..4 {
        workload.step(&count(i)).unwrap();
    }
    let state = workload.snapshot().unwrap();

    let mut stream = vec![output::encode_claim(1, "")];
    stream.extend((0..5).map(|i| counted(&at, i, 1)));
    stream.extend([output::encode_claim(2, ""), output::encode_claim(3, "")]);
    stream.extend((5..7).map(|i| counted(&at, i, 3)));
    stream.push(output::encode_claim(4, ""));
    t.publish_all(OUTPUT, stream).await;
    // Covers the first four inputs, whose outputs are the four records after
    // the first claim.
    let fourth = t.read_all(OUTPUT).await[4].0;
    let header = SnapshotHeader {
        input_offset: at[3],
        output_tail: fourth + 1,
    };
    t.publish_all(SNAPSHOTS, [snapshot::encode(header, 1, &state)])
        .await;

    let _agent = t.start_agent();
    assert_uninterrupted(&t, &module, 10).await;
    let found = records(&t).await;
    // The eleven written by hand, the new owner's claim and three outputs.
    assert_eq!(found.len(), 15, "{found:?}");
    let (epoch, claim) = found[11];
    assert!(claim && epoch > 4, "{found:?}");
    let (ok, report) = t.check(&["--id", ID, "--complete"]);
    assert!(ok, "{report}");
    assert!(report.contains("wl.wl.out: 10 records, ok"), "{report}");
}

/// `felixd check` reads claims as ownership changes, not as outputs.
#[tokio::test(flavor = "multi_thread")]
async fn check_passes_on_a_stream_with_claims() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..5).map(count)).await;
    let at = input_offsets(&t).await;
    let mut stream = vec![output::encode_claim(1, "")];
    stream.extend((0..3).map(|i| counted(&at, i, 1)));
    stream.push(output::encode_claim(2, ""));
    stream.extend((3..5).map(|i| counted(&at, i, 2)));
    stream.push(output::encode_claim(3, ""));
    t.publish_all(OUTPUT, stream).await;
    let (ok, report) = t.check(&["--id", ID, "--complete"]);
    assert!(ok, "{report}");
    assert!(report.contains("wl.wl.out: 5 records, ok"), "{report}");
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

/// Two holders are frozen in turn, each past a takeover: the first before
/// its fifth append, the second, which took over from it, before its third.
/// A third node takes over from both. When the two wake together, each
/// finds a later claim at its tail and writes nothing more.
#[tokio::test(flavor = "multi_thread")]
async fn holders_frozen_across_several_takeovers_write_nothing_after_them() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..10).map(count)).await;
    let (mut first, first_epoch) = freeze_holder(&t, "first", "freeze-before-append=5").await;
    let (mut second, second_epoch) = freeze_holder(&t, "second", "freeze-before-append=3").await;
    assert!(second_epoch > first_epoch);

    let last = t.start_agent();
    eventually(
        "the last owner's outputs",
        Duration::from_secs(60),
        || async { (t.outputs(OUTPUT).await.len() >= 10).then_some(()) },
    )
    .await;
    first.resume();
    second.resume();
    tokio::time::sleep(Duration::from_secs(3)).await;
    t.publish_all(INPUT, (10..15).map(count)).await;
    assert_uninterrupted(&t, &module, 15).await;

    let found = records(&t).await;
    let outputs = |epoch| found.iter().filter(|&&(e, c)| !c && e == epoch).count();
    assert_eq!(outputs(first_epoch), 4, "{found:?}");
    assert_eq!(outputs(second_epoch), 2, "{found:?}");
    let (held, _) = t.lease(ID).await.expect("a live lease");
    assert_eq!(held.owner, last.node_id);
    assert!(held.epoch > second_epoch);
    assert!(first.alive() && second.alive(), "a frozen node exited");
}

/// A holder is frozen with a snapshot taken and not yet written, and the
/// next owner writes none of its own. The stale snapshot lands after the
/// takeover, which is safe: it is of a state the stream agrees with. A third
/// owner restores it and resumes past the second owner's outputs.
#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_frozen_past_a_takeover_is_safe_to_resume_from() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &["--snapshot-every", "5"]).await;
    t.publish_all(INPUT, (0..8).map(count)).await;
    let (frozen, stale) = freeze_holder(&t, "holder", "freeze-before-snapshot-write=1").await;
    // On a cluster a generation-start record can take offset 0, so count
    // records rather than read the tail.
    assert!(t.read_all(SNAPSHOTS).await.is_empty());

    let mut next = t.start_agent_with_faults("skip-snapshots=100");
    eventually(
        "the next owner's outputs",
        Duration::from_secs(60),
        || async { (t.outputs(OUTPUT).await.len() >= 8).then_some(()) },
    )
    .await;
    frozen.resume();
    eventually("the stale snapshot", Duration::from_secs(30), || async {
        (!t.read_all(SNAPSHOTS).await.is_empty()).then_some(())
    })
    .await;
    let snapshots = t.read_all(SNAPSHOTS).await;
    let taken = snapshot::decode(&snapshots[0].1).unwrap().taken().unwrap();
    assert_eq!(taken.epoch, stale);

    next.kill();
    let _last = t.start_agent();
    t.publish_all(INPUT, (8..12).map(count)).await;
    assert_uninterrupted(&t, &module, 12).await;
    let epochs: Vec<u64> = t
        .outputs(OUTPUT)
        .await
        .iter()
        .map(|(_, r)| output::decode(r).unwrap().epoch())
        .collect();
    assert!(epochs[5..].iter().all(|&e| e > stale), "{epochs:?}");
}
