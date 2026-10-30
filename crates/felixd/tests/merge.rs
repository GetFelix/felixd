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
use felixd_core::merge;
use felixd_core::output::{self, Addressed, OutputHeader};
use felixd_core::spec::Limits;
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
    for _ in 0..5 {
        t.publish_all(ORDERS, [meta(), meta()]).await;
        t.publish_all(PAYMENTS, [meta()]).await;
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
    }
    assert_merged(&t, &module, 30).await;
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
