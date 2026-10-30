//! A three-stage pipeline, `a` reading the input, `b` reading `wl.a.out` and
//! `c` reading `wl.b.out`, survives losing the holder of each stage in turn.
//!
//! Needs a Felix broker; see `common`. The `cluster` CI job runs it against
//! three brokers too.

mod common;

use std::time::Duration;

use common::{Agent, INPUT, LEASE_TTL, OUTPUT, TestFelix, eventually, test_felix};
use felixd_core::names;
use felixd_core::output::{self, DEFAULT_NAME};
use felixd_core::spec::Limits;
use felixd_runtime::{Input, Runtime, Stepped};

const STAGES: [&str; 3] = ["a", "b", "c"];

/// One round of input. `a` splits the first into a batch of four, rejects
/// one and writes one under another name, so `b` reads past rejections,
/// other names and claims. The `named:out:...` records hand `b` a command:
/// a batch of two, and a keyed input it rejects.
fn round(n: u64) -> Vec<Vec<u8>> {
    [
        "split:abcd".to_owned(),
        "named:out::split:xy".to_owned(),
        "named:out:k1:reject:3".to_owned(),
        "named:side::s".to_owned(),
        "reject:2".to_owned(),
        format!("plain-{n}"),
    ]
    .map(String::into_bytes)
    .into()
}

type Keyed = (Option<Vec<u8>>, Vec<u8>);

/// What `module` addresses to `out` for `inputs`, run without interruption.
/// None of the modules here read offsets or times.
fn stage(module: &[u8], inputs: &[Keyed]) -> Vec<Keyed> {
    let mut workload = Runtime::new()
        .unwrap()
        .load(module, Limits::default())
        .unwrap();
    let mut out = Vec::new();
    for (offset, (key, payload)) in (0..).zip(inputs) {
        let input = Input {
            stream: INPUT,
            incarnation: 0,
            offset,
            timestamp_micros: 0,
            key: key.as_deref(),
            payload,
        };
        if let Stepped::Outputs(outputs) = workload.step(input).unwrap() {
            out.extend(
                outputs
                    .into_iter()
                    .filter(|o| o.name == DEFAULT_NAME)
                    .map(|o| (o.key, o.payload)),
            );
        }
    }
    out
}

/// What each stage addresses to `out` for `inputs` on the first stage's
/// input, run without interruption.
fn pipeline(modules: &[Vec<u8>], inputs: &[Vec<u8>]) -> Vec<Vec<Keyed>> {
    let mut feed: Vec<Keyed> = inputs.iter().map(|p| (None, p.clone())).collect();
    modules
        .iter()
        .map(|module| {
            feed = stage(module, &feed);
            feed.clone()
        })
        .collect()
}

/// What stage `id` addresses to `out`, in stream order.
async fn addressed(t: &TestFelix, id: &str) -> Vec<Keyed> {
    t.read_all(&names::output_stream(id))
        .await
        .iter()
        .filter_map(|(_, r)| {
            let a = output::addressed_to(r, DEFAULT_NAME).unwrap()?;
            Some((a.key.map(<[u8]>::to_vec), a.payload.to_vec()))
        })
        .collect()
}

/// Waits until every stage has written what an uninterrupted run would.
async fn caught_up(t: &TestFelix, want: &[Vec<Keyed>]) {
    for (id, want) in STAGES.iter().zip(want) {
        eventually(
            &format!("stage {id} to catch up"),
            Duration::from_secs(90),
            || async { (addressed(t, id).await.len() >= want.len()).then_some(()) },
        )
        .await;
    }
}

/// Starts node `id`, which holds at most one stage, so each stage runs on
/// its own node and a spare takes whichever is free.
fn start(t: &TestFelix, id: &str, faults: &str) -> Agent {
    let args = ["--max-workloads", "1", "--claim-delay-ms", "200"];
    t.start_node_with_args(id, faults, LEASE_TTL, &args)
}

/// Waits for `stage` to be held, with an epoch, by `node`.
async fn held_by(t: &TestFelix, stage: &str, node: &str) {
    eventually(
        &format!("{node} to hold {stage}"),
        Duration::from_secs(60),
        || async {
            t.lease(stage)
                .await
                .filter(|(l, _)| l.owner == node && l.epoch > 0)
                .map(|_| ())
        },
    )
    .await;
}

/// Kills the live node holding `stage` and returns its id.
async fn kill_holder(t: &TestFelix, agents: &mut Vec<Agent>, stage: &str) -> String {
    let (holder, _) = t.lease(stage).await.expect("the stage is held");
    let at = agents
        .iter()
        .position(|a| a.node_id == holder.owner)
        .expect("a live node holds the stage");
    agents.remove(at).kill();
    holder.owner
}

#[tokio::test(flavor = "multi_thread")]
async fn a_three_stage_pipeline_survives_failover_at_each_stage() {
    let Some(t) = test_felix().await else { return };
    let args = ["--snapshot-every", "3"];
    let faulty = t.submit_as("a", "faulty", &args).await;
    assert!(t.run("b", &faulty, "wl.a.out", &args).success());
    let counter = felixd_testkit::workload("counter");
    assert!(t.run("c", &counter, "wl.b.out", &args).success());
    let modules = [faulty.clone(), faulty, counter];

    let mut agents: Vec<Agent> = ["n1", "n2", "n3"]
        .iter()
        .map(|n| start(&t, n, ""))
        .collect();
    let mut inputs = Vec::new();
    let publish = async |inputs: &mut Vec<Vec<u8>>, n: u64| {
        let more = round(n);
        t.publish_all(INPUT, more.clone()).await;
        inputs.extend(more);
        pipeline(&modules, inputs)
    };
    let want = publish(&mut inputs, 0).await;
    caught_up(&t, &want).await;

    // Stage a: its new holder sends half of its first batch, four records,
    // and dies. The next holder claims between the halves and writes the
    // rest, while b reads past the claim.
    kill_holder(&t, &mut agents, "a").await;
    let mut torn = start(&t, "torn", "append-prefix=1,crash-after-append-sent=1");
    held_by(&t, "a", "torn").await;
    let want = publish(&mut inputs, 1).await;
    torn.exited(Duration::from_secs(60)).await;
    agents.push(start(&t, "n4", ""));
    held_by(&t, "a", "n4").await;
    caught_up(&t, &want).await;
    // The input offset of each output on a's stream, `None` for a claim.
    let a_stream: Vec<Option<u64>> = t
        .read_all(&names::output_stream("a"))
        .await
        .iter()
        .filter_map(|(_, r)| match output::decode(r).unwrap() {
            output::Record::Claim { .. } => Some(None),
            r => r.written().map(|w| Some(w.header.input_offset)),
        })
        .collect();
    let claims_between = a_stream
        .windows(3)
        .filter(|w| w[1].is_none() && w[0].is_some() && w[0] == w[2])
        .count();
    assert_eq!(claims_between, 1, "{a_stream:?}");

    // Stage b: killed, then its new holder is frozen past its lease before
    // its first append, and a third node takes over. When it wakes, its
    // append is refused by that node's claim.
    kill_holder(&t, &mut agents, "b").await;
    let frozen = start(&t, "frozen", "freeze-before-append=1");
    held_by(&t, "b", "frozen").await;
    let want = publish(&mut inputs, 2).await;
    eventually("the freeze", Duration::from_secs(60), || async {
        frozen.stopped().then_some(())
    })
    .await;
    agents.push(start(&t, "n5", ""));
    held_by(&t, "b", "n5").await;
    caught_up(&t, &want).await;
    frozen.resume();
    agents.push(frozen);
    tokio::time::sleep(LEASE_TTL).await;

    // Stage c: killed while b and a carry on; the woken node or a new one
    // takes it.
    kill_holder(&t, &mut agents, "c").await;
    agents.push(start(&t, "n6", ""));
    let want = publish(&mut inputs, 3).await;
    caught_up(&t, &want).await;
    let want = publish(&mut inputs, 4).await;
    caught_up(&t, &want).await;
    // Give a duplicate time to show up.
    tokio::time::sleep(Duration::from_secs(2)).await;

    for (id, want) in STAGES.iter().zip(&want) {
        let stream = names::output_stream(id);
        assert_eq!(&addressed(&t, id).await, want, "{stream}");
        let epochs: Vec<u64> = t
            .read_all(&stream)
            .await
            .iter()
            .map(|(_, r)| output::decode(r).unwrap().epoch())
            .collect();
        assert!(epochs.is_sorted(), "two owners took turns on {stream}");
        let (ok, report) = t.replay(&["--id", id, "--diff"]);
        assert!(ok && report.contains("outputs match"), "{report}");
    }
    // Every stage, checked upstream first.
    let (ok, report) = t.check(&["--complete"]);
    assert!(ok, "felixd check found problems:\n{report}");
    let (ok, report) = t.check(&["--id", "c", "--upstream", "--complete"]);
    assert!(ok, "{report}");
    let order: Vec<usize> = ["wl.a.out: ", "wl.b.out: ", "wl.c.out: "]
        .iter()
        .map(|s| report.find(s).unwrap_or_else(|| panic!("{report}")))
        .collect();
    assert!(order.is_sorted(), "{report}");

    // The last stage replays to the end of its input to the count of b's
    // outputs.
    let last = t.read_all("wl.b.out").await.last().unwrap().0;
    let (ok, state) = t.replay(&["--id", "c", "--to", &last.to_string()]);
    assert!(ok, "{state}");
    let count = want[1].len();
    assert!(
        state.ends_with(&format!("{{\"count\":{count}}}")),
        "{state}"
    );
}

/// A downstream's input is part of its history like any other: once it has
/// output, `felixd run` refuses to point it at another upstream, and an
/// agent leaves such a spec to an operator.
#[tokio::test(flavor = "multi_thread")]
async fn a_downstream_cannot_switch_upstreams_once_it_has_output() {
    let Some(t) = test_felix().await else { return };
    let module = t.submit_as("up", "counter", &[]).await;
    t.submit_as("other", "counter", &[]).await;
    assert!(t.run(common::ID, &module, "wl.up.out", &[]).success());
    let _agent = t.start_agent();
    t.publish_all(INPUT, (0..3u64).map(|i| i.to_be_bytes().to_vec()))
        .await;
    eventually("downstream output", Duration::from_secs(60), || async {
        (t.outputs(OUTPUT).await.len() >= 3).then_some(())
    })
    .await;

    assert!(
        !t.run(common::ID, &module, "wl.other.out", &[]).success(),
        "the switch was accepted"
    );
    assert_eq!(t.spec().await.input, "wl.up.out");

    let mut spec = t.spec().await;
    spec.input = "wl.other.out".to_owned();
    t.put_spec(&spec).await;
    let reason = t.needs_operator_recorded().await;
    assert_eq!(
        reason,
        felixd_core::class::OperatorReason::SpecConflict,
        "{reason:?}"
    );
    t.publish_all(INPUT, [3u64.to_be_bytes().to_vec()]).await;
    tokio::time::sleep(LEASE_TTL * 2).await;
    assert_eq!(t.outputs(OUTPUT).await.len(), 3, "the switched spec ran");
    let (ok, report) = t.check(&["--id", common::ID, "--upstream"]);
    assert!(ok, "{report}");
    assert!(report.contains("ok, from wl.up.out"), "{report}");
}
