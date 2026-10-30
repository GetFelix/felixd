//! No credential reaches a stream, a cache, a snapshot, a metric, an error
//! or a log line. A run with every log turned up, a failover and refused
//! credentials is scanned for the token bytes afterwards.
//!
//! Needs a Felix broker; see `common`.

mod common;

use std::fs::File;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use common::{Agent, INPUT, LEASE_TTL, OUTPUT, SNAPSHOTS, TestFelix, eventually, test_felix};
use felixd_core::names;

/// Everything every crate logs, except the code generator's, which logs the
/// workload's compiled code and nothing about connections.
const LOGS: &str = "trace,cranelift_codegen=info,cranelift_frontend=info,wasmtime_cranelift=info";

/// A token that looks like a real one, for the runs that must be refused.
const CANARY: &str = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.ZmVsaXhkLWxlYWstY2FuYXJ5LWNsYWltcw.ZmVsaXhkLWxlYWstY2FuYXJ5LXNpZ25hdHVyZQ";

fn count(i: u64) -> Vec<u8> {
    i.to_be_bytes().to_vec()
}

fn free_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

/// The parts of `token` that must never be seen: the whole of it, and its
/// signature, which is the part that cannot be rebuilt from public claims.
fn needles(token: &str) -> Vec<String> {
    let mut found = vec![token.to_owned()];
    if let Some(signature) = token.rsplit('.').next().filter(|s| s.len() >= 16) {
        found.push(signature.to_owned());
    }
    found
}

fn assert_clean(what: &str, haystack: &[u8], needles: &[String]) {
    for needle in needles {
        let leaked = haystack
            .windows(needle.len())
            .any(|w| w == needle.as_bytes());
        assert!(!leaked, "a credential appears in {what}");
    }
}

struct Logs {
    dir: PathBuf,
}

impl Logs {
    fn new(t: &TestFelix) -> Logs {
        let dir = std::env::temp_dir().join(format!("felixd-secrets-{}", t.settings.namespace));
        std::fs::create_dir_all(&dir).unwrap();
        Logs { dir }
    }

    fn file(&self, name: &str) -> (PathBuf, File, File) {
        let path = self.dir.join(name);
        let out = File::create(&path).unwrap();
        let err = out.try_clone().unwrap();
        (path, out, err)
    }

    /// Runs `command` with its output in a log file, and returns whether it
    /// succeeded.
    fn run(&self, name: &str, mut command: Command) -> bool {
        let (_, out, err) = self.file(name);
        command
            .env("RUST_LOG", LOGS)
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(err))
            .status()
            .unwrap()
            .success()
    }

    fn agent(&self, t: &TestFelix, node_id: &str, metrics: &str) -> Agent {
        let (_, out, err) = self.file(&format!("{node_id}.log"));
        let ttl = LEASE_TTL.as_millis().to_string();
        let child = t
            .felixd(&["agent", "--node-id", node_id, "--lease-ttl-ms", &ttl])
            .args(["--metrics-bind", metrics])
            .env("RUST_LOG", LOGS)
            .stdout(Stdio::from(out))
            .stderr(Stdio::from(err))
            .spawn()
            .unwrap();
        Agent::new(child, node_id)
    }

    fn scan(&self, needles: &[String]) {
        for entry in std::fs::read_dir(&self.dir).unwrap() {
            let path = entry.unwrap().path();
            let text = std::fs::read(&path).unwrap();
            assert_clean(&path.display().to_string(), &text, needles);
        }
    }
}

fn token_file(dir: &Path, name: &str, token: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, token).unwrap();
    path
}

/// The names of every `kind` (`streams` or `caches`) in the test's
/// namespace, from the control plane, every page.
async fn listed(t: &TestFelix, kind: &str, field: &str) -> Vec<String> {
    let s = &t.settings;
    let base = format!(
        "{}/v1/tenants/{}/namespaces/{}/{kind}",
        s.controlplane_url.trim_end_matches('/'),
        s.tenant,
        s.namespace
    );
    let authorization = format!("Bearer {}", s.controlplane_token.as_ref().unwrap().expose());
    let field = field.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut names = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut request = ureq::get(&base).header("Authorization", &authorization);
            if let Some(c) = &cursor {
                request = request.query("cursor", c);
            }
            let page: serde_json::Value = request.call().unwrap().body_mut().read_json().unwrap();
            for item in page["items"].as_array().unwrap() {
                names.push(item[field.as_str()].as_str().unwrap().to_owned());
            }
            match page["next_cursor"].as_str() {
                Some(next) => cursor = Some(next.to_owned()),
                None => return names,
            }
        }
    })
    .await
    .unwrap()
}

async fn scrape(addr: &str) -> String {
    let url = format!("http://{addr}/metrics");
    tokio::task::spawn_blocking(move || {
        ureq::get(&url)
            .call()
            .unwrap()
            .body_mut()
            .read_to_string()
            .unwrap()
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn no_credential_reaches_a_record_a_metric_an_error_or_a_log() {
    let Some(t) = test_felix().await else { return };
    let mut secrets = needles(t.settings.token.expose());
    secrets.extend(needles(
        t.settings.controlplane_token.as_ref().unwrap().expose(),
    ));
    secrets.extend(needles(CANARY));
    let logs = Logs::new(&t);

    // Submit through the command line, so its own output is scanned too.
    let kind = felixd_agent::admin::StreamKind {
        replication_factor: common::replication_factor(),
    };
    felixd_agent::admin::ensure_stream(&t.settings, INPUT, kind)
        .await
        .unwrap();
    let module = felixd_testkit::workload("counter");
    let wasm = logs.dir.join("counter.wasm.bin");
    std::fs::write(&wasm, &module).unwrap();
    let mut run = t.felixd(&["run", "--id", common::ID, "--input", INPUT]);
    run.args([
        "--replication-factor",
        &common::replication_factor().to_string(),
        "--snapshot-every",
        "3",
    ])
    .arg(&wasm);
    assert!(logs.run("run.log", run));

    let metrics_a = free_addr();
    let mut a = logs.agent(&t, "secrets-a", &metrics_a);
    t.publish_all(INPUT, (0..10).map(count)).await;
    eventually("the first outputs", Duration::from_secs(60), || async {
        (t.outputs(OUTPUT).await.len() >= 10).then_some(())
    })
    .await;
    let scraped_a = scrape(&metrics_a).await;

    // A failover: the second node claims, resumes from a snapshot and writes on.
    a.kill();
    let metrics_b = free_addr();
    let _b = logs.agent(&t, "secrets-b", &metrics_b);
    t.publish_all(INPUT, (10..20).map(count)).await;
    eventually(
        "the outputs after failover",
        Duration::from_secs(60),
        || async { (t.outputs(OUTPUT).await.len() >= 20).then_some(()) },
    )
    .await;
    let scraped_b = scrape(&metrics_b).await;

    assert!(logs.run("ps.log", t.felixd(&["ps"])));
    assert!(logs.run("check.log", t.felixd(&["check", "--id", common::ID])));

    // Refused credentials are where a secret most often ends up in an error.
    let bad_broker = token_file(&logs.dir, "canary.jwt", CANARY);
    let mut ps = t.felixd(&["ps"]);
    ps.env("FELIX_AUTH_TOKEN_FILE", &bad_broker);
    assert!(!logs.run("ps-refused.log", ps));
    let mut run = t.felixd(&["run", "--id", "refused", "--input", INPUT]);
    run.env("FELIX_CONTROLPLANE_TOKEN_FILE", &bad_broker)
        .arg(&wasm);
    assert!(!logs.run("run-refused.log", run));
    // The scan reads only logs, so the token files and module go first.
    std::fs::remove_file(&bad_broker).unwrap();
    std::fs::remove_file(&wasm).unwrap();

    // Every stream and cache in the namespace, as the control plane lists
    // them, so one added later is scanned without changing this test.
    let streams = listed(&t, "streams", "stream").await;
    let caches = listed(&t, "caches", "cache").await;
    for want in [INPUT, OUTPUT, SNAPSHOTS, names::SPECS, names::FACTS] {
        assert!(
            streams.iter().any(|s| s == want),
            "{want} not in {streams:?}"
        );
    }
    for want in [names::WORKLOADS, names::LEASES, names::EPOCHS, names::NODES] {
        assert!(caches.iter().any(|c| c == want), "{want} not in {caches:?}");
    }
    let mut records = 0;
    for stream in &streams {
        for (offset, payload) in t.read_all(stream).await {
            assert_clean(&format!("{stream} at {offset}"), &payload, &secrets);
            records += 1;
        }
    }
    assert!(
        !t.read_all(SNAPSHOTS).await.is_empty(),
        "no snapshot written"
    );
    for cache in &caches {
        for (key, value) in t.felix.entries(cache).await.unwrap() {
            assert_clean(&format!("{cache}/{key}"), key.as_bytes(), &secrets);
            assert_clean(&format!("{cache}/{key}"), &value, &secrets);
            records += 1;
        }
    }
    assert_clean("metrics", scraped_a.as_bytes(), &secrets);
    assert_clean("metrics", scraped_b.as_bytes(), &secrets);

    logs.scan(&secrets);
    // The scan means something only if the connect and auth path logged at
    // the levels it runs at, and not just the compiler.
    for (name, line) in [
        ("secrets-a.log", "stream authenticated"),
        ("secrets-b.log", "stream authenticated"),
        ("ps.log", "stream authenticated"),
    ] {
        let text = std::fs::read_to_string(logs.dir.join(name)).unwrap();
        assert!(text.contains(line), "{name} has no \"{line}\" line");
    }
    assert!(records > 30, "only {records} records scanned");
    std::fs::remove_dir_all(&logs.dir).unwrap();
}
