//! A workload with two inputs steps their records in the order its merge log
//! records, across a holder killed between merging and stepping and a holder
//! frozen before it merges.
//!
//! Needs a Felix broker; see `common`. The `cluster` CI job runs it against
//! three brokers too.

mod common;

use std::collections::HashMap;
use std::time::Duration;

use common::{ID, OUTPUT, TestFelix, eventually, replication_factor, test_felix};
use felixd_agent::admin::{StreamKind, ensure_stream};
use felixd_core::output::{self, Addressed, OutputHeader};
use felixd_core::spec::Limits;
use felixd_core::{merge, names};
use felixd_runtime::{Input, Runtime, Stepped};

const ORDERS: &str = "orders";
const PAYMENTS: &str = "payments";
const INPUTS: [&str; 2] = [ORDERS, PAYMENTS];
const MERGE: &str = "wl.wl.merge";

/// What `faulty` outputs for a `meta` record: the stream, offset and append
/// time it was given. So the outputs show which record each step ran.
fn meta() -> Vec<u8> {
    b"meta".to_vec()
}

/// Creates both inputs and submits `faulty` as [`ID`] reading them.
async fn submit(t: &TestFelix) -> Vec<u8> {
    let kind = StreamKind {
        replication_factor: replication_factor(),
    };
    for input in INPUTS {
        ensure_stream(&t.settings, input, kind).await.unwrap();
    }
    let module = felixd_testkit::workload("faulty");
    assert!(t.run_on(ID, &module, &INPUTS, &[]).success());
    module
}

/// Each merge log record as `(offset, epoch, entry)`, `None` for a claim.
async fn merge_log(t: &TestFelix) -> Vec<(u64, u64, Option<merge::Entry>)> {
    t.read_all(MERGE)
        .await
        .iter()
        .map(|(at, r)| {
            let record = merge::decode(r).unwrap();
            (*at, record.epoch(), record.entry())
        })
        .collect()
}

/// What the output stream should hold: the module run without interruption
/// over the records the merge log names, in its order, each step numbered by
/// its entry's offset. Checks each entry against the record it names.
async fn expected(t: &TestFelix, module: &[u8]) -> Vec<Vec<u8>> {
    let mut records = Vec::new();
    for input in INPUTS {
        let on: HashMap<u64, (u64, Vec<u8>)> = t
            .felix
            .read_range(input, 0, t.felix.tail(input).await.unwrap())
            .await
            .unwrap()
            .into_iter()
            .map(|r| (r.offset, (r.timestamp_micros, r.payload.to_vec())))
            .collect();
        records.push(on);
    }
    let mut workload = Runtime::new()
        .unwrap()
        .load(module, Limits::default())
        .unwrap();
    let mut want = Vec::new();
    for (at, _, entry) in merge_log(t).await {
        let Some(entry) = entry else { continue };
        let input = usize::from(entry.input);
        let (stamp, payload) = &records[input][&entry.offset];
        assert_eq!(*stamp, entry.timestamp_micros, "{entry} at {at}");
        let given = Input {
            stream: INPUTS[input],
            incarnation: 0,
            offset: entry.offset,
            timestamp_micros: entry.timestamp_micros,
            key: None,
            payload,
        };
        let Stepped::Outputs(outputs) = workload.step(given).unwrap() else {
            panic!("faulty rejected {entry}");
        };
        for (index, out) in outputs.iter().enumerate() {
            let header = OutputHeader {
                input_offset: at,
                index: index as u32,
            };
            let addressed = Addressed {
                name: &out.name,
                key: out.key.as_deref(),
                payload: &out.payload,
            };
            want.push(output::encode_addressed(header, 0, addressed).unwrap());
        }
    }
    want
}

/// Waits for an output for each of `records` input records, then checks the
/// output stream holds what the merge log says, that `felixd check
/// --complete` finds every record merged once and that a replay matches.
async fn assert_merged(t: &TestFelix, module: &[u8], records: usize) {
    eventually("every record stepped", Duration::from_secs(60), || async {
        (t.outputs(OUTPUT).await.len() >= records).then_some(())
    })
    .await;
    // Give a duplicate time to show up before comparing.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let want = expected(t, module).await;
    assert_eq!(want.len(), records, "the merge log names every record once");
    let got = t.outputs(OUTPUT).await;
    assert_eq!(got.len(), want.len());
    for ((at, got), want) in got.iter().zip(&want) {
        assert!(output::same_output(got, want), "output at {at} differs");
    }
    let (ok, report) = t.check(&["--id", ID, "--complete"]);
    assert!(ok, "felixd check found problems:\n{report}");
    let (ok, diffed) = t.replay(&["--id", ID, "--diff"]);
    assert!(ok, "replay --diff: {diffed}");
    assert!(
        diffed.ends_with(&format!("{records} outputs match")),
        "{diffed}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn two_inputs_are_stepped_in_merge_log_order() {
    let Some(t) = test_felix().await else { return };
    let module = submit(&t).await;
    let mut published = 0;
    for _ in 0..5 {
        t.publish_all(ORDERS, [meta(), meta()]).await;
        t.publish_all(PAYMENTS, [meta()]).await;
        published += 3;
    }
    let _agent = t.start_agent();
    eventually("the first steps", Duration::from_secs(60), || async {
        (!t.outputs(OUTPUT).await.is_empty()).then_some(())
    })
    .await;
    // Records that arrive while it runs are merged as they come.
    for _ in 0..5 {
        t.publish_all(PAYMENTS, [meta()]).await;
        t.publish_all(ORDERS, [meta()]).await;
        published += 2;
    }
    assert_merged(&t, &module, published).await;
    let log = merge_log(&t).await;
    assert!(log[0].2.is_none(), "the log starts with a claim: {log:?}");
}

/// The holder aborts once its first merge append lands, before stepping any
/// of it. The next holder steps those entries in the order they were
/// merged, then merges the rest.
#[tokio::test(flavor = "multi_thread")]
async fn a_holder_killed_mid_merge_leaves_its_order_to_the_next() {
    let Some(t) = test_felix().await else { return };
    let module = submit(&t).await;
    for _ in 0..10 {
        t.publish_all(ORDERS, [meta()]).await;
        t.publish_all(PAYMENTS, [meta()]).await;
    }
    let mut first = t.start_agent_with_faults("crash-after-merge=1");
    first.exited(Duration::from_secs(60)).await;
    let merged: Vec<_> = merge_log(&t).await;
    let entries = merged.iter().filter(|(_, _, e)| e.is_some()).count();
    assert!(entries > 0, "nothing was merged: {merged:?}");
    assert!(
        t.outputs(OUTPUT).await.is_empty(),
        "the holder stepped before it crashed"
    );

    let _next = t.start_agent();
    for _ in 0..5 {
        t.publish_all(PAYMENTS, [meta()]).await;
        t.publish_all(ORDERS, [meta()]).await;
    }
    assert_merged(&t, &module, 30).await;
    // The crashed holder's order stands: its entries are still first.
    assert_eq!(merge_log(&t).await[..merged.len()], merged[..]);
}

/// The holder is frozen just before its first merge append. The next holder
/// drops that record's delivery, so it merges nothing and its claim is all
/// it writes to the merge log. When the frozen holder wakes, its append must
/// be refused by that claim alone.
#[tokio::test(flavor = "multi_thread")]
async fn a_frozen_holder_cannot_merge_after_the_next_holder_claims() {
    let Some(t) = test_felix().await else { return };
    let module = submit(&t).await;
    t.publish_all(ORDERS, [meta()]).await;
    let record = t.read_all(ORDERS).await[0].0;
    let mut frozen = t.start_agent_with_faults("freeze-before-merge=1");
    eventually("the freeze", Duration::from_secs(60), || async {
        frozen.stopped().then_some(())
    })
    .await;
    let (stale, _) = t.lease(ID).await.expect("the frozen node's lease");

    let _next = t.start_agent_with_faults(&format!("drop-input={record}"));
    let (owner, _) = eventually(
        "the next owner's lease",
        Duration::from_secs(60),
        || async {
            t.lease(ID)
                .await
                .filter(|(l, _)| l.owner != stale.owner && l.epoch > 0)
        },
    )
    .await;
    eventually(
        "the next owner's merge claim",
        Duration::from_secs(60),
        || async {
            let log = merge_log(&t).await;
            log.iter()
                .any(|&(_, epoch, entry)| epoch == owner.epoch && entry.is_none())
                .then_some(())
        },
    )
    .await;
    frozen.resume();
    tokio::time::sleep(Duration::from_secs(3)).await;

    let log = merge_log(&t).await;
    assert!(
        log.iter()
            .all(|&(_, epoch, entry)| entry.is_none() || epoch != stale.epoch),
        "the frozen node merged after the new claim: {log:?}"
    );
    assert!(frozen.alive(), "the frozen node exited");
    let (held, _) = t.lease(ID).await.expect("a live lease");
    assert_eq!(held.owner, owner.owner);

    // The next record makes the holder read back the one it dropped.
    t.publish_all(ORDERS, [meta()]).await;
    assert_merged(&t, &module, 2).await;
    let log = merge_log(&t).await;
    let epochs: Vec<u64> = log.iter().map(|&(_, epoch, _)| epoch).collect();
    assert!(epochs.is_sorted(), "two holders took turns: {log:?}");
    assert!(
        log.iter().all(|&(_, epoch, entry)| match entry {
            Some(_) => epoch == owner.epoch,
            None => true,
        }),
        "{log:?}"
    );
}

/// Each record of the merge log as `(epoch, is a claim)`.
async fn shape(t: &TestFelix) -> Vec<(u64, bool)> {
    merge_log(t)
        .await
        .iter()
        .map(|&(_, epoch, entry)| (epoch, entry.is_none()))
        .collect()
}

/// Felix refuses a write too large for a frame, every time it is sent. A
/// gap in one input, read back after a dropped delivery, is merged in
/// writes that fit; one write of the whole gap would be refused forever.
/// The fault caps a write at one entry, so the gap and its next record
/// cross that limit.
#[tokio::test(flavor = "multi_thread")]
async fn a_gap_longer_than_one_write_is_merged_in_several() {
    let Some(t) = test_felix().await else { return };
    let module = submit(&t).await;
    t.publish_all(ORDERS, [meta()]).await;
    let first = t.read_all(ORDERS).await[0].0;
    let faults = format!("merge-write-limit=1,drop-input={first}");
    let _agent = t.start_agent_with_faults(&faults);
    // Long enough for the holder to be subscribed when the next ones land.
    eventually("the merge claim", Duration::from_secs(60), || async {
        (!merge_log(&t).await.is_empty()).then_some(())
    })
    .await;
    t.publish_all(ORDERS, [meta(), meta(), meta()]).await;
    t.publish_all(PAYMENTS, [meta(), meta()]).await;
    assert_merged(&t, &module, 6).await;
}

/// The holder is frozen after reading the merge log's tail, before its
/// claim. A stale holder's claim and entry land at that tail meanwhile,
/// written here at epoch 0, below every lease's: a stale holder can be
/// frozen with its write in flight, but whether that write is sent before
/// its task is cancelled depends on timing. The holder's claim is refused
/// there; it must claim again after the entry and step it, at its own
/// epoch, rather than give up.
#[tokio::test(flavor = "multi_thread")]
async fn a_stale_entry_before_the_next_claim_is_adopted() {
    let Some(t) = test_felix().await else { return };
    let module = submit(&t).await;
    t.publish_all(ORDERS, [meta()]).await;
    let tail = t.felix.tail(ORDERS).await.unwrap();
    let record = t.felix.read_range(ORDERS, 0, tail).await.unwrap()[0].clone();
    let holder = t.start_agent_with_faults("freeze-before-merge-claim=1");
    eventually("the freeze", Duration::from_secs(60), || async {
        holder.stopped().then_some(())
    })
    .await;
    let (held, _) = t.lease(ID).await.expect("the holder's lease");
    assert!(held.epoch > 0);

    let mut next = vec![0, 0];
    let entry = merge::Entry {
        input: 0,
        offset: record.offset,
        timestamp_micros: record.timestamp_micros,
    };
    let stale = vec![
        merge::encode_claim(0, &next, "stale"),
        merge::encode_entry(0, entry, &mut next),
    ];
    // The tail the holder read: on a cluster a generation-start record can
    // already take offset 0.
    let at = t.felix.tail(MERGE).await.unwrap();
    t.publish_if(MERGE, stale, at).await;
    holder.resume();
    assert_merged(&t, &module, 1).await;
    assert_eq!(
        shape(&t).await,
        [(0, true), (0, false), (held.epoch, true)],
        "the holder did not claim after the stale entry at its own epoch"
    );
    let epochs: Vec<u64> = t
        .outputs(OUTPUT)
        .await
        .iter()
        .map(|(_, r)| output::decode(r).unwrap().epoch())
        .collect();
    assert_eq!(epochs, [held.epoch]);
}

/// The first merge claim's answer and the first append's are lost, and the
/// second append is dropped. Each lands once: a holder that finds its own
/// claim last does not claim again, and its own entries past the tail are
/// taken as written.
#[tokio::test(flavor = "multi_thread")]
async fn merge_writes_land_once_through_lost_answers_and_dropped_requests() {
    let Some(t) = test_felix().await else { return };
    let module = submit(&t).await;
    let faults = "lose-merge-claim-answer=1,lose-merge-answer=1,drop-merge=2";
    let _agent = t.start_agent_with_faults(faults);
    eventually("the merge claim", Duration::from_secs(60), || async {
        (!merge_log(&t).await.is_empty()).then_some(())
    })
    .await;
    for _ in 0..3 {
        t.publish_all(ORDERS, [meta()]).await;
        t.publish_all(PAYMENTS, [meta()]).await;
    }
    assert_merged(&t, &module, 6).await;
    let claims = shape(&t).await.iter().filter(|(_, claim)| *claim).count();
    assert_eq!(claims, 1, "{:?}", shape(&t).await);
}

/// A snapshot that runs out of fuel restarts the run under the same lease.
/// The holder's claim is already the last of its epoch on the merge log, so
/// it does not claim again.
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_under_the_same_lease_does_not_claim_the_merge_log_again() {
    let Some(t) = test_felix().await else { return };
    let kind = StreamKind {
        replication_factor: replication_factor(),
    };
    for input in INPUTS {
        ensure_stream(&t.settings, input, kind).await.unwrap();
    }
    let module = felixd_testkit::workload("faulty");
    let args = [
        "--snapshot-every",
        "1",
        "--fuel",
        "2000000",
        "--snapshot-fuel",
        "2000000",
    ];
    assert!(t.run_on(ID, &module, &INPUTS, &args).success());
    let _agent = t.start_agent();
    t.publish_all(ORDERS, [meta()]).await;
    eventually("the first step", Duration::from_secs(60), || async {
        (!t.outputs(OUTPUT).await.is_empty()).then_some(())
    })
    .await;
    // A snapshot of 20,000 chunks does not fit in the fuel.
    t.publish_all(PAYMENTS, [b"chunks:20000".to_vec()]).await;
    t.publish_all(ORDERS, [meta()]).await;
    t.publish_all(PAYMENTS, [meta()]).await;
    assert_merged(&t, &module, 3).await;
    let found = shape(&t).await;
    assert_eq!(
        found.iter().filter(|(_, claim)| *claim).count(),
        1,
        "{found:?}"
    );
    assert!(t.lease(ID).await.is_some(), "the workload is still held");
}

/// The holder dies after its second snapshot. The next restores it, finds the
/// time-based snapshot point from the merge log and carries on.
#[tokio::test(flavor = "multi_thread")]
async fn a_merged_workload_resumes_from_a_snapshot() {
    let Some(t) = test_felix().await else { return };
    let kind = StreamKind {
        replication_factor: replication_factor(),
    };
    for input in INPUTS {
        ensure_stream(&t.settings, input, kind).await.unwrap();
    }
    let module = felixd_testkit::workload("faulty");
    let args = ["--snapshot-every", "4", "--snapshot-every-ms", "1"];
    assert!(t.run_on(ID, &module, &INPUTS, &args).success());
    for _ in 0..6 {
        t.publish_all(ORDERS, [meta()]).await;
        t.publish_all(PAYMENTS, [meta()]).await;
    }
    let mut first = t.start_agent_with_faults("crash-after-snapshot=2");
    first.exited(Duration::from_secs(60)).await;
    assert!(
        !t.read_all(common::SNAPSHOTS).await.is_empty(),
        "no snapshot was written"
    );
    let _next = t.start_agent();
    for _ in 0..3 {
        t.publish_all(PAYMENTS, [meta()]).await;
        t.publish_all(ORDERS, [meta()]).await;
    }
    assert_merged(&t, &module, 18).await;
}

/// A merge log written for another number of inputs than the spec has
/// cannot be carried on from, on any node.
#[tokio::test(flavor = "multi_thread")]
async fn a_merge_log_for_another_number_of_inputs_fails_the_workload() {
    let Some(t) = test_felix().await else { return };
    submit(&t).await;
    t.publish_all(MERGE, [merge::encode_claim(0, &[0, 0, 0], "elsewhere")])
        .await;
    t.publish_all(ORDERS, [meta()]).await;
    let _agent = t.start_agent();
    let failure = t.failure().await;
    assert!(
        failure.contains("the merge log was written for 3 inputs, the spec has 2"),
        "{failure}"
    );
}

/// `felixd check --upstream` on a join checks both stages it reads first.
#[tokio::test(flavor = "multi_thread")]
async fn check_upstream_on_a_join_checks_each_upstream_first() {
    let Some(t) = test_felix().await else { return };
    let counter = t.submit_as("a", "counter", &[]).await;
    t.submit_as("b", "counter", &[]).await;
    let ups = ["wl.a.out", "wl.b.out"];
    assert!(t.run_on("join", &counter, &ups, &[]).success());
    let _agent = t.start_agent();
    t.publish_all(common::INPUT, (0..3).map(|i: u8| vec![i]))
        .await;
    eventually("the join's outputs", Duration::from_secs(90), || async {
        let join = t.outputs(&names::output_stream("join")).await;
        (join.len() >= 6).then_some(())
    })
    .await;
    let (ok, report) = t.check(&["--id", "join", "--complete", "--upstream"]);
    assert!(ok, "{report}");
    let order: Vec<&str> = report
        .lines()
        .map(|l| l.split(':').next().unwrap())
        .collect();
    assert_eq!(order.len(), 3, "{report}");
    assert_eq!(order[2], "wl.join.out", "{report}");
    assert!(order[..2].contains(&"wl.a.out") && order[..2].contains(&"wl.b.out"));
}
