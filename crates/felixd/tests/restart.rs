//! Kills the agent while a workload is mid-stream and restarts it. The output
//! stream must hold exactly what an uninterrupted run would: no gap and no
//! duplicate.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::time::Duration;

use common::{INPUT, OUTPUT, SNAPSHOTS, TestFelix, eventually, test_felix};
use felixd_core::output;
use felixd_core::spec::Limits;
use felixd_runtime::Runtime;

const INPUTS: u64 = 240;

/// What the output stream should hold: every output of every input, run
/// through the same module without interruption.
fn expected(module: &[u8], inputs: &[(u64, Vec<u8>)]) -> Vec<Vec<u8>> {
    let mut workload = Runtime::new()
        .unwrap()
        .load(module, Limits::default())
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

/// Waits for the output of every input on the input stream, then checks the
/// output stream holds exactly that.
async fn assert_uninterrupted(t: &TestFelix, module: &[u8], inputs: u64) {
    let inputs_found = t.read_all(INPUT).await;
    assert_eq!(inputs_found.len() as u64, inputs);
    let want = expected(module, &inputs_found);
    eventually("all output", Duration::from_secs(60), || async {
        let got = t.read_all(OUTPUT).await;
        (got.len() >= want.len()).then_some(())
    })
    .await;
    // Give a duplicate time to show up before comparing.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let got: Vec<Vec<u8>> = t.read_all(OUTPUT).await.into_iter().map(|r| r.1).collect();
    assert_eq!(got.len(), want.len(), "output length");
    assert!(got == want, "output differs from an uninterrupted run");
}

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

/// `snapshot_every` past the number of inputs means no snapshot is ever
/// written, so the restart replays from the first input.
async fn kill_and_restart(
    workload: &str,
    snapshot_every: u64,
    input_for: impl Fn(u64) -> Vec<u8> + Send + 'static,
) {
    let Some(t) = test_felix().await else { return };
    let every = snapshot_every.to_string();
    let module = t.submit(workload, &["--snapshot-every", &every]).await;

    let mut agent = t.start_agent();
    let publishing = {
        let felix = felixd_agent::felix::Felix::connect(&t.settings)
            .await
            .unwrap();
        tokio::spawn(async move {
            for i in 0..INPUTS {
                felix.publish(INPUT, input_for(i)).await.unwrap();
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
        })
    };

    // Let the workload get going, past two snapshots when it takes them, then
    // kill the agent without warning.
    let (stream, records) = if snapshot_every < INPUTS {
        (SNAPSHOTS, 2)
    } else {
        (OUTPUT, 20)
    };
    eventually("progress", Duration::from_secs(60), || async {
        (t.felix.tail(stream).await.unwrap() >= records).then_some(())
    })
    .await;
    agent.kill();
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let mut agent = t.start_agent();
    publishing.await.unwrap();
    assert_uninterrupted(&t, &module, INPUTS).await;
    agent.kill();
    if snapshot_every >= INPUTS {
        assert_eq!(
            t.felix.tail(SNAPSHOTS).await.unwrap(),
            0,
            "a snapshot was written"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn counter_resumes_with_no_gap_or_duplicate() {
    kill_and_restart("counter", 25, count).await;
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
    kill_and_restart("counter", u64::MAX, count).await;
}

/// A snapshot taken while replaying would record the output stream's tail,
/// which is ahead of the workload's state. If the agent then dies before
/// passing the outputs already written, the next start trusts that snapshot,
/// skips nothing and writes those outputs again.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_crash_while_replaying_repeats_no_output() {
    let Some(t) = test_felix().await else { return };
    let module = t
        .submit("counter", &["--snapshot-every", &u64::MAX.to_string()])
        .await;
    t.publish_all(INPUT, (0..120).map(count)).await;
    let mut agent = t.start_agent();
    eventually(
        "the first run's output",
        Duration::from_secs(60),
        || async { (t.felix.tail(OUTPUT).await.unwrap() >= 120).then_some(()) },
    )
    .await;
    agent.kill();

    // The next run replays all 120 inputs, and would snapshot every 10 of them.
    let mut spec = t.spec().await;
    spec.snapshot_every = 10;
    t.put_spec(&spec).await;
    let mut agent = t.start_agent_with_faults("crash-after-snapshot=1");
    assert!(!agent.exited(Duration::from_secs(60)).await.success());

    let mut agent = t.start_agent();
    t.publish_all(INPUT, (120..150).map(count)).await;
    assert_uninterrupted(&t, &module, 150).await;
    agent.kill();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crash_between_append_and_snapshot_resumes_from_the_last_snapshot() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &["--snapshot-every", "10"]).await;
    t.publish_all(INPUT, (0..60).map(count)).await;
    let mut agent = t.start_agent_with_faults("crash-before-snapshot=3");
    assert!(!agent.exited(Duration::from_secs(60)).await.success());
    // Input 29's output is written and its snapshot is not.
    assert_eq!(t.felix.tail(OUTPUT).await.unwrap(), 30);
    assert_eq!(t.felix.tail(SNAPSHOTS).await.unwrap(), 2);

    let mut agent = t.start_agent();
    t.publish_all(INPUT, (60..80).map(count)).await;
    assert_uninterrupted(&t, &module, 80).await;
    agent.kill();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_input_the_subscription_dropped_is_read_back() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit("counter", &[]).await;
    t.publish_all(INPUT, (0..40).map(count)).await;
    let mut agent = t.start_agent_with_faults("drop-input=17");
    assert_uninterrupted(&t, &module, 40).await;
    agent.kill();
}
