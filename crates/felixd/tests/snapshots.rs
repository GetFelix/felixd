//! Snapshots by count and by time: two nodes running the same workload over
//! the same input, one starting late, snapshot at the same inputs, and those
//! are the ones the input's append times and the count name.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::time::Duration;

use common::{INPUT, LEASE_TTL, assert_uninterrupted_on, eventually, test_felix};
use felixd_core::names;
use felixd_core::snapshot;

const EVERY: u64 = 8;
const EVERY_MS: u64 = 300;

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

/// The inputs an uninterrupted owner snapshots at, and whether each was the
/// time trigger.
fn points(stamps: &[(u64, u64)]) -> Vec<(u64, bool)> {
    let mut points = Vec::new();
    let mut since = 0;
    let mut previous = None;
    for &(offset, stamp) in stamps {
        let interval = snapshot::starts_interval(EVERY_MS, previous, stamp);
        previous = Some(stamp);
        since += 1;
        if interval || since >= EVERY {
            points.push((offset, interval));
            since = 0;
        }
    }
    points
}

#[tokio::test(flavor = "multi_thread")]
async fn time_snapshots_land_at_the_same_inputs_on_two_nodes() {
    let Some(t) = test_felix().await else { return };
    let every = EVERY.to_string();
    let every_ms = EVERY_MS.to_string();
    let args = ["--snapshot-every", &every, "--snapshot-every-ms", &every_ms];
    let module = t.submit_as("a", "counter", &args).await;
    t.submit_as("b", "counter", &args).await;
    let one_each = ["--max-workloads", "1", "--claim-delay-ms", "0"];
    let _one = t.start_node_with_args("one", "", LEASE_TTL, &one_each);
    // Slow inputs cross an interval every two or three; a burst only reaches
    // the count.
    for i in 0..20 {
        t.publish_all(INPUT, [count(i)]).await;
        tokio::time::sleep(Duration::from_millis(130)).await;
    }
    // The second node starts late and runs its workload's history at once.
    let _two = t.start_node_with_args("two", "", LEASE_TTL, &one_each);
    t.publish_all(INPUT, (20..40).map(count)).await;
    for i in 40..46 {
        tokio::time::sleep(Duration::from_millis(400)).await;
        t.publish_all(INPUT, [count(i)]).await;
    }
    for id in ["a", "b"] {
        assert_uninterrupted_on(&t, &module, 46, &names::output_stream(id)).await;
    }
    let owners = (t.lease("a").await.unwrap().0, t.lease("b").await.unwrap().0);
    assert_ne!(owners.0.owner, owners.1.owner);

    let tail = t.felix.tail(INPUT).await.unwrap();
    let stamps: Vec<(u64, u64)> = t
        .felix
        .read_range(INPUT, 0, tail)
        .await
        .unwrap()
        .iter()
        .map(|r| (r.offset, r.timestamp_micros))
        .collect();
    let want = points(&stamps);
    assert!(
        want.iter().any(|(_, time)| *time),
        "no time trigger: {want:?}"
    );
    assert!(
        want.iter().any(|(_, time)| !*time),
        "no count trigger: {want:?}"
    );
    let want: Vec<u64> = want.into_iter().map(|(offset, _)| offset).collect();
    for id in ["a", "b"] {
        let stream = names::snapshot_stream(id);
        eventually("every snapshot", Duration::from_secs(30), || async {
            let got: Vec<u64> = t
                .read_all(&stream)
                .await
                .iter()
                .map(|(_, r)| snapshot::decode(r).unwrap().0.header.input_offset)
                .collect();
            (got == want).then_some(())
        })
        .await;
    }
}
