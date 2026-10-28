//! Kills the agent while a workload is mid-stream and restarts it. The output
//! stream must hold exactly what an uninterrupted run would: no gap and no
//! duplicate.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::time::Duration;

use common::{TestFelix, eventually, test_felix};
use felixd_agent::admin::{StreamKind, ensure_stream};
use felixd_core::output;
use felixd_runtime::{Limits, Runtime};

const INPUTS: u64 = 240;

/// What the output stream should hold: every output of every input, run
/// through the same module without interruption.
fn expected(module: &[u8], inputs: &[(u64, Vec<u8>)]) -> Vec<Vec<u8>> {
    let mut workload = Runtime::new(Limits::default())
        .unwrap()
        .load(module)
        .unwrap();
    let mut records = Vec::new();
    for (offset, payload) in inputs {
        for (index, out) in workload.step(payload).unwrap().iter().enumerate() {
            let header = output::OutputHeader {
                input_offset: *offset,
                index: index as u32,
            };
            records.push(output::encode(header, out));
        }
    }
    records
}

async fn read_all(t: &TestFelix, stream: &str) -> Vec<(u64, Vec<u8>)> {
    let tail = t.felix.tail(stream).await.unwrap();
    t.felix
        .read_range(stream, 0, tail)
        .await
        .unwrap()
        .into_iter()
        .map(|r| (r.offset, r.payload.to_vec()))
        .collect()
}

/// `snapshot_every` past the number of inputs means no snapshot is ever
/// written, so the restart replays from the first input.
async fn kill_and_restart(
    workload: &str,
    snapshot_every: u64,
    input_for: impl Fn(u64) -> Vec<u8> + Send + 'static,
) {
    let Some(t) = test_felix().await else { return };
    let module = felixd_testkit::workload(workload);
    let dir = std::env::temp_dir().join(format!("felixd-{}", t.settings.namespace));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{workload}.wasm"));
    std::fs::write(&path, &module).unwrap();

    ensure_stream(
        &t.settings,
        "in",
        StreamKind {
            replication_factor: 1,
        },
    )
    .await
    .unwrap();
    let status = t
        .felixd(&[
            "run",
            "--id",
            "wl",
            "--input",
            "in",
            "--snapshot-every",
            "25",
        ])
        .arg(&path)
        .status()
        .unwrap();
    assert!(status.success());

    let mut agent = t.start_agent();
    let publishing = {
        let felix = felixd_agent::felix::Felix::connect(&t.settings)
            .await
            .unwrap();
        tokio::spawn(async move {
            for i in 0..INPUTS {
                felix.publish("in", input_for(i)).await.unwrap();
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
        })
    };

    // Let the workload get going, past two snapshots when it takes them, then
    // kill the agent without warning.
    let (stream, records) = if snapshot_every < INPUTS {
        ("wl.wl.snap", 2)
    } else {
        ("wl.wl.out", 20)
    };
    eventually("progress", Duration::from_secs(60), || async {
        (t.felix.tail(stream).await.unwrap() >= records).then_some(())
    })
    .await;
    agent.kill().unwrap();
    agent.wait().unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let mut agent = t.start_agent();
    publishing.await.unwrap();

    let inputs = read_all(&t, "in").await;
    assert_eq!(inputs.len() as u64, INPUTS);
    let want = expected(&module, &inputs);
    eventually("all output", Duration::from_secs(120), || async {
        let got = read_all(&t, "wl.wl.out").await;
        (got.len() >= want.len()).then_some(())
    })
    .await;
    // Give a duplicate time to show up before comparing.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let got: Vec<Vec<u8>> = read_all(&t, "wl.wl.out")
        .await
        .into_iter()
        .map(|r| r.1)
        .collect();
    agent.kill().unwrap();
    agent.wait().unwrap();
    assert_eq!(got.len(), want.len(), "output length");
    assert!(got == want, "output differs from an uninterrupted run");
}

#[tokio::test(flavor = "multi_thread")]
async fn counter_resumes_with_no_gap_or_duplicate() {
    kill_and_restart("counter", 25, |i| i.to_be_bytes().to_vec()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn window_resumes_with_no_gap_or_duplicate() {
    // Most steps output nothing here, so the output stream alone does not say
    // how far the old agent got.
    kill_and_restart("window", 25, |i| {
        let mut r = (i * 700).to_le_bytes().to_vec();
        r.extend((i as i64 % 17 - 8).to_le_bytes());
        r
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn counter_without_a_snapshot_replays_and_skips_what_is_written() {
    kill_and_restart("counter", u64::MAX, |i| i.to_be_bytes().to_vec()).await;
}
