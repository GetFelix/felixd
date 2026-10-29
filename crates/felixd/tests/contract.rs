//! Workload contract 0.2 end to end: rejections, poison, addressed outputs,
//! chunked snapshots, input metadata and 0.1 modules, each through a takeover
//! where it matters, and checked with `felixd check` and `felixd replay`.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::time::Duration;

use bytes::Bytes;
use common::{
    ID, INPUT, LEASE_TTL, OUTPUT, SNAPSHOTS, TestFelix, assert_uninterrupted, eventually,
    test_felix,
};
use felixd_agent::admin::{StreamKind, ensure_stream};
use felixd_core::output::{self, OutputHeader, Record, Rejected};
use felixd_core::snapshot::{self, ChunkRef, Manifest, SnapshotHeader, Taken};
use felixd_core::spec::{Limits, WorkloadSpec, WorkloadState};
use felixd_core::{module, names};

fn inputs(payloads: &[&str]) -> Vec<Vec<u8>> {
    payloads.iter().map(|p| p.as_bytes().to_vec()).collect()
}

/// Waits for `n` outputs and rejections on `stream`.
async fn produced(t: &TestFelix, stream: &str, n: usize) {
    eventually("the outputs", Duration::from_secs(60), || async {
        (t.outputs(stream).await.len() >= n).then_some(())
    })
    .await;
}

async fn read_stamped(t: &TestFelix, stream: &str) -> Vec<felix_client::StreamRecord> {
    let tail = t.felix.tail(stream).await.unwrap();
    t.felix.read_range(stream, 0, tail).await.unwrap()
}

fn diff_passes(t: &TestFelix, id: &str) {
    let (ok, report) = t.replay(&["--id", id, "--diff"]);
    assert!(ok, "{report}");
    assert!(report.contains("outputs match"), "{report}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejection_is_recorded_and_the_workload_keeps_running() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("faulty", &["--snapshot-every", "1000"]).await;
    let mut first = t.start_agent();
    t.publish_all(INPUT, inputs(&["a", "reject:7", "b"])).await;
    produced(&t, OUTPUT, 3).await;
    // The next owner steps the rejected input again and checks the record.
    first.kill();
    let _second = t.start_agent();
    t.publish_all(INPUT, inputs(&["reject:8", "c"])).await;
    assert_uninterrupted(&t, &module, 5).await;

    let input_offsets: Vec<u64> = t.read_all(INPUT).await.iter().map(|r| r.0).collect();
    let rejections: Vec<(u64, u32)> = t
        .outputs(OUTPUT)
        .await
        .iter()
        .filter_map(|(_, r)| match output::decode(r).unwrap() {
            Record::Rejection(written, Rejected { code, detail }) => {
                assert_eq!(written.header.index, 0);
                assert_eq!(detail, b"asked to reject");
                Some((written.header.input_offset, code))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        rejections,
        [(input_offsets[1], 7), (input_offsets[3], 8)],
        "the rejections"
    );
    let spec = t.spec().await;
    assert_eq!(spec.state, WorkloadState::Running);
    assert!(t.lease(ID).await.is_some(), "the workload is still held");
    diff_passes(&t, ID);
}

#[tokio::test(flavor = "multi_thread")]
async fn poison_fails_the_workload() {
    let Some(t) = test_felix().await else { return };
    t.submit("faulty", &[]).await;
    t.publish_all(INPUT, inputs(&["a", "poison", "b"])).await;
    let _agent = t.start_agent();
    let failure = t.failure().await;
    assert!(failure.contains("poison: asked to poison"), "{failure}");
    let records = t.read_all(OUTPUT).await;
    assert!(
        records
            .iter()
            .any(|(_, r)| matches!(output::decode(r), Ok(Record::Failure { .. }))),
        "no failure record"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_result_that_breaks_the_contract_fails_the_workload() {
    let Some(t) = test_felix().await else { return };
    let cases = [
        ("request", "request", "this build has no effect executors"),
        (
            "name",
            "named:Not-Valid::x",
            "which is not 1 to 64 characters",
        ),
        ("chunk", "big-chunk", "more than the 4194304 allowed"),
    ];
    let module = felixd_testkit::workload("faulty");
    let kind = StreamKind {
        replication_factor: common::replication_factor(),
    };
    for (id, _, _) in cases {
        let input = format!("in-{id}");
        ensure_stream(&t.settings, &input, kind).await.unwrap();
        let run = t.run(id, &module, &input, &["--snapshot-every", "1"]);
        assert!(run.success());
    }
    let _agent = t.start_agent();
    for (id, input, _) in cases {
        let stream = format!("in-{id}");
        t.publish_all(&stream, [input.as_bytes().to_vec()]).await;
    }
    for (id, _, want) in cases {
        let failure = t.failure_of(id).await;
        assert!(failure.contains(want), "{id}: {failure}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn addressed_outputs_land_with_their_name_and_key() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("faulty", &["--snapshot-every", "2"]).await;
    let steps = [
        "named:alerts:k1:hot",
        "named:out::cold",
        "split:ab",
        "named:out:k9:meta",
        "named:other::meta",
        "meta",
    ];
    let mut first = t.start_agent();
    t.publish_all(INPUT, inputs(&steps[..3])).await;
    produced(&t, OUTPUT, 4).await;
    first.kill();
    let _second = t.start_agent();
    t.publish_all(INPUT, inputs(&steps[3..])).await;
    assert_uninterrupted(&t, &module, steps.len() as u64).await;

    let found: Vec<(String, Option<Vec<u8>>, Vec<u8>)> = t
        .outputs(OUTPUT)
        .await
        .iter()
        .map(|(_, r)| {
            let (_, a) = output::decode(r).unwrap().output().unwrap();
            (
                a.name.to_owned(),
                a.key.map(<[u8]>::to_vec),
                a.payload.to_vec(),
            )
        })
        .collect();
    let want = |name: &str, key: Option<&[u8]>, payload: &[u8]| {
        (name.to_owned(), key.map(<[u8]>::to_vec), payload.to_vec())
    };
    assert_eq!(found[0], want("alerts", Some(b"k1"), b"hot"));
    assert_eq!(found[1], want("out", None, b"cold"));
    assert_eq!(found[2], want("out", None, b"a"));
    assert_eq!(found[4], want("out", Some(b"k9"), b"meta"));
    assert_eq!(found[5], want("other", None, b"meta"));
    diff_passes(&t, ID);

    // A workload reading this one's output gets only what is addressed to
    // `out`, as its key and payload.
    assert!(t.run("down", &module, OUTPUT, &[]).success());
    let down = names::output_stream("down");
    let upstream = t.read_all(OUTPUT).await;
    let addressed: Vec<(u64, Vec<u8>)> = upstream
        .iter()
        .filter(|(_, r)| matches!(output::addressed_to(r, "out"), Ok(Some(_))))
        .cloned()
        .collect();
    produced(&t, &down, addressed.len()).await;
    let got: Vec<Vec<u8>> = t.outputs(&down).await.into_iter().map(|r| r.1).collect();
    assert_eq!(got.len(), addressed.len());
    let stamps = read_stamped(&t, OUTPUT).await;
    let stamp_of = |offset: u64| {
        stamps
            .iter()
            .find(|r| r.offset == offset)
            .unwrap()
            .timestamp_micros
    };
    for (record, (offset, _)) in got.iter().zip(&addressed) {
        let (written, a) = output::decode(record).unwrap().output().unwrap();
        assert_eq!(written.header.input_offset, *offset);
        if a.payload.starts_with(b"wl.wl.out ") {
            let text = String::from_utf8(a.payload.to_vec()).unwrap();
            assert_eq!(
                text,
                format!("wl.wl.out 0 {offset} {} 6b39 6d657461", stamp_of(*offset))
            );
        }
    }
    let metas = got
        .iter()
        .filter(|r| {
            let (_, a) = output::decode(r).unwrap().output().unwrap();
            a.payload.starts_with(b"wl.wl.out ")
        })
        .count();
    assert_eq!(metas, 1, "only the `out` meta record reaches the reader");
    let (ok, report) = t.check(&["--id", "down", "--complete"]);
    assert!(ok, "{report}");
    diff_passes(&t, "down");
}

/// The kinds on the snapshot stream: `C` for a chunk, `M` for a manifest.
async fn snapshot_layout(t: &TestFelix) -> String {
    t.read_all(SNAPSHOTS)
        .await
        .iter()
        .map(|(_, r)| match snapshot::decode(r).unwrap() {
            snapshot::Record::Chunk { .. } => 'C',
            snapshot::Record::Manifest(_) => 'M',
            snapshot::Record::Whole(..) => 'W',
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn chunked_snapshots_write_only_changed_chunks_and_restore_identically() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("faulty", &["--snapshot-every", "1"]).await;
    let mut first = t.start_agent();
    t.publish_all(INPUT, inputs(&["chunks:4", "touch:1", "touch:1", "x"]))
        .await;
    eventually("four snapshots", Duration::from_secs(60), || async {
        (snapshot_layout(&t).await.matches('M').count() >= 4).then_some(())
    })
    .await;
    // Four new chunks, then one changed chunk twice, then none.
    assert_eq!(snapshot_layout(&t).await, "CCCCMCMCMM");
    first.kill();

    // The next owner restores the last manifest, whose chunks were written
    // by three different snapshots, and carries on from that state.
    let _second = t.start_agent();
    t.publish_all(INPUT, inputs(&["touch:1", "touch:3"])).await;
    assert_uninterrupted(&t, &module, 6).await;
    let touched: Vec<Vec<u8>> = t.outputs(OUTPUT).await.into_iter().map(|r| r.1).collect();
    let last = output::decode(&touched[touched.len() - 2]).unwrap();
    assert_eq!(last.output().unwrap().1.payload, b"3");
    let (ok, state) = t.replay(&[
        "--id",
        ID,
        "--to",
        &t.read_all(INPUT).await[5].0.to_string(),
    ]);
    assert!(ok, "{state}");
    assert_eq!(state, r#"{"chunks":[0,3,0,1]}"#);
    diff_passes(&t, ID);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_chunk_that_does_not_match_its_manifest_fails_the_workload() {
    let Some(t) = test_felix().await else { return };
    t.submit("counter", &[]).await;
    t.publish_all(INPUT, [vec![0]]).await;
    let input_offset = t.read_all(INPUT).await[0].0;
    let header = OutputHeader {
        input_offset,
        index: 0,
    };
    t.publish_all(OUTPUT, [output::encode(header, 1, &1u64.to_be_bytes())])
        .await;
    let state = 1u64.to_be_bytes();
    let at = t.felix.tail(SNAPSHOTS).await.unwrap();
    let chunk = snapshot::encode_chunk(1, &snapshot::sha256(&state), &state);
    let manifest = Manifest {
        taken: Taken {
            header: SnapshotHeader {
                input_offset,
                output_tail: t.read_all(OUTPUT).await[0].0 + 1,
            },
            epoch: 1,
        },
        chunks: vec![ChunkRef {
            sha256: snapshot::sha256(b"something else"),
            offset: at,
        }],
    };
    t.publish_if(
        SNAPSHOTS,
        vec![chunk, snapshot::encode_manifest(&manifest)],
        at,
    )
    .await;
    let _agent = t.start_agent();
    let failure = t.failure().await;
    assert!(failure.contains("does not have the hash"), "{failure}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_input_metadata_is_the_same_live_after_a_takeover_and_in_a_replay() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("faulty", &["--snapshot-every", "3"]).await;
    let mut first = t.start_agent();
    t.publish_all(INPUT, inputs(&["meta"; 5])).await;
    produced(&t, OUTPUT, 5).await;
    first.kill();
    let _second = t.start_agent();
    t.publish_all(INPUT, inputs(&["meta"; 5])).await;
    // Compares every output with one computed here from the input stream.
    assert_uninterrupted(&t, &module, 10).await;
    let input = read_stamped(&t, INPUT).await;
    let first_output = t.outputs(OUTPUT).await[0].1.clone();
    let (_, a) = output::decode(&first_output).unwrap().output().unwrap();
    assert_eq!(
        String::from_utf8(a.payload.to_vec()).unwrap(),
        format!(
            "{INPUT} 0 {} {} - 6d657461",
            input[0].offset, input[0].timestamp_micros
        )
    );
    diff_passes(&t, ID);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_contract_01_module_is_refused_and_left_for_an_operator() {
    let Some(t) = test_felix().await else { return };
    // Another workload, so the caches exist and an agent has work it does.
    t.submit_as("other", "counter", &[]).await;
    let old = felixd_testkit::contract_01_component();
    let hash = module::hash(&old);
    assert!(!t.run(ID, &old, INPUT, &[]).success(), "felixd run took it");
    assert!(
        t.felix
            .cache_get(names::MODULES, &hash)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        t.felix
            .cache_get(names::WORKLOADS, ID)
            .await
            .unwrap()
            .is_none()
    );

    // A spec written some other way, as an older felixd would have.
    let kind = StreamKind {
        replication_factor: common::replication_factor(),
    };
    for stream in [OUTPUT, SNAPSHOTS] {
        ensure_stream(&t.settings, stream, kind).await.unwrap();
    }
    t.felix
        .cache_put(names::MODULES, &hash, Bytes::from(old))
        .await
        .unwrap();
    t.put_spec(&WorkloadSpec {
        module: hash,
        input: INPUT.into(),
        output: OUTPUT.into(),
        snapshot_every: 1000,
        snapshot_every_ms: None,
        state: WorkloadState::Running,
        limits: Limits::default(),
        failure: None,
    })
    .await;
    let _agent = t.start_agent();
    t.publish_all(INPUT, [vec![1]]).await;
    produced(&t, &names::output_stream("other"), 1).await;
    tokio::time::sleep(LEASE_TTL * 2).await;
    assert!(t.lease(ID).await.is_none(), "the 0.1 workload was claimed");
    assert!(t.read_all(OUTPUT).await.is_empty(), "something was written");
    let spec = t.spec().await;
    assert_eq!(spec.state, WorkloadState::Running);
    assert_eq!(spec.failure, None);
}

/// The module is not in the cache when the agent starts, so the check before
/// the claim cannot see it. The agent claims, and finds the 0.1 module only
/// once it has the lease.
#[tokio::test(flavor = "multi_thread")]
async fn a_contract_01_module_found_after_the_claim_is_left_for_an_operator() {
    let Some(t) = test_felix().await else { return };
    let old = felixd_testkit::contract_01_component();
    let hash = module::hash(&old);
    let kind = StreamKind {
        replication_factor: common::replication_factor(),
    };
    for stream in [INPUT, OUTPUT, SNAPSHOTS] {
        ensure_stream(&t.settings, stream, kind).await.unwrap();
    }
    t.put_spec(&WorkloadSpec {
        module: hash.clone(),
        input: INPUT.into(),
        output: OUTPUT.into(),
        snapshot_every: 1000,
        snapshot_every_ms: None,
        state: WorkloadState::Running,
        limits: Limits::default(),
        failure: None,
    })
    .await;
    let _agent = t.start_agent();
    eventually("the claim", Duration::from_secs(60), || async {
        t.lease(ID).await.map(|_| ())
    })
    .await;

    t.felix
        .cache_put(names::MODULES, &hash, Bytes::from(old))
        .await
        .unwrap();
    eventually("the release", Duration::from_secs(60), || async {
        t.lease(ID).await.is_none().then_some(())
    })
    .await;
    tokio::time::sleep(LEASE_TTL * 2).await;
    assert!(
        t.lease(ID).await.is_none(),
        "the workload was claimed again"
    );
    assert!(t.outputs(OUTPUT).await.is_empty(), "something was written");
    let spec = t.spec().await;
    assert_eq!(spec.state, WorkloadState::Running);
    assert_eq!(spec.failure, None);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_that_runs_out_of_fuel_is_skipped_and_the_workload_goes_on() {
    let Some(t) = test_felix().await else { return };
    // Each step fits in the fuel; a snapshot of 20,000 chunks does not.
    let module = t
        .submit(
            "faulty",
            &[
                "--snapshot-every",
                "1",
                "--fuel",
                "2000000",
                "--snapshot-fuel",
                "2000000",
            ],
        )
        .await;
    let mut first = t.start_agent();
    t.publish_all(INPUT, inputs(&["a"])).await;
    eventually("the first snapshot", Duration::from_secs(60), || async {
        snapshot_layout(&t).await.contains('M').then_some(())
    })
    .await;
    let before = snapshot_layout(&t).await;
    t.publish_all(INPUT, inputs(&["chunks:20000", "b", "c"]))
        .await;
    // "chunks" outputs nothing, so a, b and c make three.
    produced(&t, OUTPUT, 3).await;
    assert_eq!(snapshot_layout(&t).await, before, "a snapshot was written");
    let spec = t.spec().await;
    assert_eq!(spec.state, WorkloadState::Running);
    assert_eq!(spec.failure, None);
    assert!(t.lease(ID).await.is_some(), "the workload is still held");

    // The next owner restores the older snapshot and replays from it.
    first.kill();
    let _second = t.start_agent();
    t.publish_all(INPUT, inputs(&["d"])).await;
    assert_uninterrupted(&t, &module, 5).await;
    assert_eq!(snapshot_layout(&t).await, before);
    diff_passes(&t, ID);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restore_that_runs_out_of_fuel_is_left_for_an_operator() {
    let Some(t) = test_felix().await else { return };
    t.submit("faulty", &["--snapshot-every", "1"]).await;
    let mut first = t.start_agent();
    t.publish_all(INPUT, inputs(&["chunks:20000", "x"])).await;
    eventually("two snapshots", Duration::from_secs(60), || async {
        (snapshot_layout(&t).await.matches('M').count() >= 2).then_some(())
    })
    .await;
    first.kill();
    let outputs = t.outputs(OUTPUT).await.len();

    let mut spec = t.spec().await;
    spec.limits.snapshot_fuel = 2_000_000;
    t.put_spec(&spec).await;
    let _second = t.start_agent();
    t.publish_all(INPUT, inputs(&["y"])).await;
    tokio::time::sleep(LEASE_TTL * 2).await;
    assert!(t.lease(ID).await.is_none(), "the workload was held");
    assert_eq!(
        t.outputs(OUTPUT).await.len(),
        outputs,
        "something was written"
    );
    let spec = t.spec().await;
    assert_eq!(spec.state, WorkloadState::Running);
    assert_eq!(spec.failure, None);
}
