//! A Felix to test against, found through the same environment variables the
//! CLI reads. CI starts it from `tests/felix/compose.yaml`.

// Each test binary uses only some of these.
#![allow(dead_code)]

use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use felix_client::ConditionalWrite;
use felixd_agent::admin::{StreamKind, ensure_stream};
use felixd_agent::felix::Felix;
use felixd_agent::settings::Settings;
use felixd_core::class::OperatorReason;
use felixd_core::lease::Lease;
use felixd_core::names;
use felixd_core::output;
use felixd_core::spec::Limits;
use felixd_core::spec::{SpecRecord, WorkloadSpec, WorkloadState};
use felixd_runtime::{Input, Runtime, Stepped, Workload};

/// The id every test gives its one workload, and its streams.
pub(crate) const ID: &str = "wl";
pub(crate) const INPUT: &str = "in";
pub(crate) const OUTPUT: &str = "wl.wl.out";
pub(crate) const SNAPSHOTS: &str = "wl.wl.snap";

pub(crate) struct TestFelix {
    pub(crate) settings: Settings,
    pub(crate) felix: Felix,
}

/// Connects to the test Felix in its own fresh namespace. `None` when no Felix
/// is configured, unless `FELIXD_REQUIRE_FELIX` is set, which CI does.
pub(crate) async fn test_felix() -> Option<TestFelix> {
    test_felix_in(None).await
}

/// As [`test_felix`], in `namespace` if given instead of a fresh one.
pub(crate) async fn test_felix_in(namespace: Option<&str>) -> Option<TestFelix> {
    let Ok(brokers) = std::env::var("FELIX_BROKERS") else {
        if std::env::var_os("FELIXD_REQUIRE_FELIX").is_some() {
            panic!("FELIXD_REQUIRE_FELIX is set but FELIX_BROKERS is not");
        }
        eprintln!("skipping: FELIX_BROKERS is not set");
        return None;
    };
    let var = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("{name} is not set"));
    let read = |name: &str| {
        std::fs::read_to_string(var(name))
            .unwrap()
            .trim()
            .to_owned()
    };
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let settings = Settings {
        brokers: brokers.split(',').map(|b| b.parse().unwrap()).collect(),
        server_name: "localhost".into(),
        tenant: var("FELIX_AUTH_TENANT"),
        // The clock alone can repeat across tests that start together.
        namespace: namespace.map_or_else(
            || {
                format!(
                    "t{nanos:x}{:x}",
                    NEXT_NAMESPACE.fetch_add(1, Ordering::Relaxed)
                )
            },
            str::to_owned,
        ),
        token: read("FELIX_AUTH_TOKEN_FILE"),
        ca_file: Some(PathBuf::from(var("FELIX_CA_FILE"))),
        controlplane_url: var("FELIX_CONTROLPLANE_URL"),
        controlplane_token: Some(read("FELIX_CONTROLPLANE_TOKEN_FILE")),
    };
    felixd_agent::admin::ensure_namespace(&settings)
        .await
        .unwrap();
    let felix = Felix::connect(&settings).await.unwrap();
    Some(TestFelix { settings, felix })
}

impl TestFelix {
    /// Runs `felixd <args>` against this test's namespace.
    pub(crate) fn felixd(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_felixd"));
        command
            .args(args)
            .env("FELIX_NAMESPACE", &self.settings.namespace)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        command
    }

    pub(crate) fn start_agent(&self) -> Agent {
        self.start_agent_with_faults("")
    }

    /// Starts an agent with `FELIXD_FAULTS` set, see `felixd_agent::faults`.
    pub(crate) fn start_agent_with_faults(&self, faults: &str) -> Agent {
        let id = format!("node-{}", NEXT_NODE.fetch_add(1, Ordering::Relaxed));
        self.start_node(&id, faults)
    }

    /// Starts an agent named `node_id` with a short lease TTL, so that a node
    /// a test kills is replaced quickly.
    pub(crate) fn start_node(&self, node_id: &str, faults: &str) -> Agent {
        self.start_node_with_ttl(node_id, faults, LEASE_TTL)
    }

    pub(crate) fn start_node_with_ttl(&self, node_id: &str, faults: &str, ttl: Duration) -> Agent {
        self.start_node_with_args(node_id, faults, ttl, &[])
    }

    /// Starts an agent with more `felixd agent` arguments.
    pub(crate) fn start_node_with_args(
        &self,
        node_id: &str,
        faults: &str,
        ttl: Duration,
        args: &[&str],
    ) -> Agent {
        let ttl = ttl.as_millis().to_string();
        let child = self
            .felixd(&["agent", "--node-id", node_id, "--lease-ttl-ms", &ttl])
            .args(args)
            .env("FELIXD_FAULTS", faults)
            .spawn()
            .expect("start felixd agent");
        Agent {
            child,
            node_id: node_id.to_owned(),
        }
    }

    /// Starts an agent that connects with the token in `token_file`.
    pub(crate) fn start_node_with_token(
        &self,
        node_id: &str,
        token_file: &str,
        faults: &str,
    ) -> Agent {
        let ttl = LEASE_TTL.as_millis().to_string();
        let child = self
            .felixd(&["agent", "--node-id", node_id, "--lease-ttl-ms", &ttl])
            .env("FELIX_AUTH_TOKEN_FILE", token_file)
            .env("FELIXD_FAULTS", faults)
            .spawn()
            .expect("start felixd agent");
        Agent {
            child,
            node_id: node_id.to_owned(),
        }
    }

    /// The workload's lease, if one is live: its owner and its version.
    pub(crate) async fn lease(&self, id: &str) -> Option<(Lease, u64)> {
        let found = self
            .felix
            .cache_get_versioned(names::LEASES, id)
            .await
            .unwrap()?;
        Some((Lease::decode(&found.value).unwrap(), found.version))
    }

    /// Creates the input stream and submits example workload `workload` as
    /// [`ID`] with `felixd run <args>`. Returns the module.
    pub(crate) async fn submit(&self, workload: &str, args: &[&str]) -> Vec<u8> {
        self.submit_as(ID, workload, args).await
    }

    /// As [`Self::submit`], as workload `id`.
    pub(crate) async fn submit_as(&self, id: &str, workload: &str, args: &[&str]) -> Vec<u8> {
        let kind = StreamKind {
            replication_factor: replication_factor(),
        };
        ensure_stream(&self.settings, INPUT, kind).await.unwrap();
        let module = felixd_testkit::workload(workload);
        assert!(self.run(id, &module, INPUT, args).success());
        module
    }

    /// Runs `felixd run <args>` for `module` as workload `id` reading
    /// `input`, which must exist.
    pub(crate) fn run(&self, id: &str, module: &[u8], input: &str, args: &[&str]) -> ExitStatus {
        self.run_on(id, module, &[input], args)
    }

    /// As [`Self::run`], reading each of `inputs`.
    pub(crate) fn run_on(
        &self,
        id: &str,
        module: &[u8],
        inputs: &[&str],
        args: &[&str],
    ) -> ExitStatus {
        let dir = std::env::temp_dir().join(format!("felixd-{}", self.settings.namespace));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{}.wasm", felixd_core::module::hash(module)));
        std::fs::write(&path, module).unwrap();
        self.felixd(&["run", "--id", id])
            .args(inputs.iter().flat_map(|&input| ["--input", input]))
            .args(["--replication-factor", &replication_factor().to_string()])
            .args(args)
            .arg(&path)
            .status()
            .unwrap()
    }

    pub(crate) async fn publish_all(
        &self,
        stream: &str,
        records: impl IntoIterator<Item = Vec<u8>>,
    ) {
        for record in records {
            self.felix.publish(stream, record).await.unwrap();
        }
    }

    /// Appends `records` at offset `at`, which must be the tail. A stream
    /// just created may not be served yet; `tail` waits for that, as the
    /// agent does.
    pub(crate) async fn publish_if(&self, stream: &str, records: Vec<Vec<u8>>, at: u64) {
        assert_eq!(self.felix.tail(stream).await.unwrap(), at);
        let written = self.felix.publish_if(stream, records, at).await.unwrap();
        assert!(
            matches!(written, felix_client::ConditionalWrite::Written { offset } if offset == at),
            "{written:?}"
        );
    }

    /// Every record on `stream`. A quorum stream just created refuses reads
    /// with a typed not_ready until its first committed mark, so this waits
    /// for that, up to a bound.
    pub(crate) async fn read_all(&self, stream: &str) -> Vec<(u64, Vec<u8>)> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match self.try_read_all(stream).await {
                Ok(records) => return records,
                Err(error) if Instant::now() < deadline && is_settling(&error) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => panic!("reading {stream}: {error:#}"),
            }
        }
    }

    async fn try_read_all(&self, stream: &str) -> anyhow::Result<Vec<(u64, Vec<u8>)>> {
        let tail = self.felix.tail(stream).await?;
        let records = self.felix.read_range(stream, 0, tail).await?;
        Ok(records
            .into_iter()
            .map(|r| (r.offset, r.payload.to_vec()))
            .collect())
    }

    /// The output records on `stream`, without claims and failures.
    pub(crate) async fn outputs(&self, stream: &str) -> Vec<(u64, Vec<u8>)> {
        let mut records = self.read_all(stream).await;
        records.retain(|(_, r)| !output::is_marker(r));
        records
    }

    /// The records on the output stream saying that a spec record needs an
    /// operator, as `(spec record, reason)`, and how many other records
    /// there are besides outputs.
    pub(crate) async fn needs_operator(&self) -> (Vec<(u64, OperatorReason)>, usize) {
        let mut found = Vec::new();
        let mut others = 0;
        for (_, record) in self.read_all(OUTPUT).await {
            match output::decode(&record) {
                Ok(output::Record::NeedsOperator { spec, reason, .. }) => {
                    found.push((spec, reason))
                }
                Ok(record) if record.written().is_some() => {}
                _ => others += 1,
            }
        }
        (found, others)
    }

    /// Waits for the output stream to say that the current spec record
    /// needs an operator, and returns why.
    pub(crate) async fn needs_operator_recorded(&self) -> OperatorReason {
        let record = self.spec().await.record.expect("a spec record");
        eventually(
            "a needs-operator record",
            Duration::from_secs(90),
            || async {
                self.needs_operator()
                    .await
                    .0
                    .into_iter()
                    .find(|(spec, _)| *spec == record)
                    .map(|(_, reason)| reason)
            },
        )
        .await
    }

    pub(crate) async fn spec(&self) -> WorkloadSpec {
        self.spec_of(ID).await
    }

    pub(crate) async fn spec_of(&self, id: &str) -> WorkloadSpec {
        let value = self.felix.cache_get(names::WORKLOADS, id).await.unwrap();
        WorkloadSpec::decode(&value.expect("the workload has a spec")).unwrap()
    }

    /// The cache version of the workload's spec.
    pub(crate) async fn spec_version(&self) -> u64 {
        let found = self
            .felix
            .cache_get_versioned(names::WORKLOADS, ID)
            .await
            .unwrap();
        found.expect("the workload has a spec").version
    }

    /// Writes `spec` the way `felixd run` does, skipping its checks: a record
    /// on `sys.specs`, then the cache value naming it.
    pub(crate) async fn put_spec(&self, spec: &WorkloadSpec) {
        let kind = StreamKind {
            replication_factor: replication_factor(),
        };
        ensure_stream(&self.settings, names::SPECS, kind)
            .await
            .unwrap();
        let mut spec = WorkloadSpec {
            record: None,
            failure: None,
            ..spec.clone()
        };
        let record = SpecRecord::new(ID, spec.clone()).encode();
        let offset = loop {
            let tail = self.felix.tail(names::SPECS).await.unwrap();
            let sent = self
                .felix
                .publish_if(names::SPECS, vec![record.clone()], tail)
                .await
                .unwrap();
            if let ConditionalWrite::Written { offset } = sent {
                break offset;
            }
        };
        spec.record = Some(offset);
        self.put_cache_spec(&spec).await;
    }

    /// Writes `spec` into the cache alone, as a felixd from before spec
    /// records did.
    pub(crate) async fn put_cache_spec(&self, spec: &WorkloadSpec) {
        let value = Bytes::from(spec.encode());
        self.felix
            .cache_put(names::WORKLOADS, ID, value)
            .await
            .unwrap();
    }

    /// Runs `felixd check <args>`. Returns whether it passed, and what it
    /// printed.
    pub(crate) fn check(&self, args: &[&str]) -> (bool, String) {
        let out = self
            .felixd(&["check"])
            .args(args)
            .stdout(Stdio::piped())
            .output()
            .unwrap();
        (
            out.status.success(),
            String::from_utf8_lossy(&out.stdout).into_owned(),
        )
    }

    /// Runs `felixd replay <args>`. Returns whether it succeeded, and what it
    /// printed on stdout.
    pub(crate) fn replay(&self, args: &[&str]) -> (bool, String) {
        let out = self
            .felixd(&["replay"])
            .args(args)
            .stdout(Stdio::piped())
            .output()
            .unwrap();
        (
            out.status.success(),
            String::from_utf8_lossy(&out.stdout).trim().to_owned(),
        )
    }

    /// Waits for the workload to be marked failed and returns why.
    pub(crate) async fn failure(&self) -> String {
        self.failure_of(ID).await
    }

    /// As [`Self::failure`], for workload `id`.
    pub(crate) async fn failure_of(&self, id: &str) -> String {
        eventually("the workload to fail", Duration::from_secs(30), || async {
            let spec = self.spec_of(id).await;
            (spec.state == WorkloadState::Failed)
                .then(|| spec.failure.map(|f| f.detail).unwrap_or_default())
        })
        .await
    }
}

/// The lease TTL test agents run with.
/// Streams get three replicas against the three-broker cluster
/// (`FELIXD_CLUSTER` is set), one otherwise.
pub(crate) fn replication_factor() -> u32 {
    if std::env::var_os("FELIXD_CLUSTER").is_some() {
        3
    } else {
        1
    }
}

pub(crate) const LEASE_TTL: Duration = Duration::from_secs(2);

static NEXT_NODE: AtomicU32 = AtomicU32::new(0);
static NEXT_NAMESPACE: AtomicU32 = AtomicU32::new(0);

/// A running `felixd agent`, killed when dropped so a failing test does not
/// leave it behind.
pub(crate) struct Agent {
    child: Child,
    pub(crate) node_id: String,
}

impl Agent {
    /// Asks the agent to shut down cleanly, as Ctrl-C does.
    pub(crate) fn interrupt(&self) {
        self.signal("INT");
    }

    pub(crate) fn kill(&mut self) {
        self.signal("CONT");
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// Whether the process is still running.
    pub(crate) fn alive(&mut self) -> bool {
        self.child.try_wait().unwrap().is_none()
    }

    /// Sends `SIGCONT` to an agent a fault stopped.
    pub(crate) fn resume(&self) {
        self.signal("CONT");
    }

    /// Whether the process is stopped by a signal.
    pub(crate) fn stopped(&self) -> bool {
        let out = Command::new("ps")
            .args(["-o", "stat=", "-p", &self.child.id().to_string()])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout)
            .trim_start()
            .starts_with('T')
    }

    fn signal(&self, name: &str) {
        let _ = Command::new("kill")
            .args([&format!("-{name}"), &self.child.id().to_string()])
            .status();
    }

    /// Waits for the agent to exit by itself.
    pub(crate) async fn exited(&mut self, timeout: Duration) -> ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for the agent to exit"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Drop for Agent {
    fn drop(&mut self) {
        self.kill();
    }
}

/// Polls `check` until it returns `Some`, or panics after `timeout`.
pub(crate) async fn eventually<T, F, Fut>(what: &str, timeout: Duration, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = check().await {
            return value;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The input at `offset` on [`INPUT`], as the agent hands it to a step.
pub(crate) fn input(offset: u64, timestamp_micros: u64, payload: &[u8]) -> Input<'_> {
    Input {
        stream: INPUT,
        incarnation: 0,
        offset,
        timestamp_micros,
        key: None,
        payload,
    }
}

/// Steps `workload` through `payloads` at offsets from 0, for a workload
/// that reads neither offsets nor times.
pub(crate) fn step_all(workload: &mut Workload, payloads: impl IntoIterator<Item = Vec<u8>>) {
    for (offset, payload) in (0..).zip(payloads) {
        workload.step(input(offset, 0, &payload)).unwrap();
    }
}

/// What the output stream should hold: every output or rejection of every
/// input on [`INPUT`], run through the same module without interruption.
pub(crate) fn expected(module: &[u8], inputs: &[felix_client::StreamRecord]) -> Vec<Vec<u8>> {
    let mut workload = Runtime::new()
        .unwrap()
        .load(module, Limits::default())
        .unwrap();
    let mut records = Vec::new();
    for record in inputs {
        let offset = record.offset;
        let stepped = workload
            .step(input(offset, record.timestamp_micros, &record.payload))
            .unwrap();
        match stepped {
            Stepped::Outputs(outputs) => {
                for (index, out) in outputs.iter().enumerate() {
                    let header = output::OutputHeader {
                        input_offset: offset,
                        index: index as u32,
                    };
                    let addressed = output::Addressed {
                        name: &out.name,
                        key: out.key.as_deref(),
                        payload: &out.payload,
                    };
                    records.push(output::encode_addressed(header, 0, addressed).unwrap());
                }
            }
            Stepped::Rejected { code, detail } => {
                let rejected = output::Rejected {
                    code,
                    detail: detail.as_bytes(),
                };
                records.push(output::encode_rejection(offset, 0, rejected));
            }
        }
    }
    records
}

/// Waits for the output of every input on the input stream, then checks the
/// output stream holds exactly that.
pub(crate) async fn assert_uninterrupted(t: &TestFelix, module: &[u8], inputs: u64) {
    assert_uninterrupted_on(t, module, inputs, OUTPUT).await;
}

/// As [`assert_uninterrupted`], for the output stream `output`.
pub(crate) async fn assert_uninterrupted_on(
    t: &TestFelix,
    module: &[u8],
    inputs: u64,
    output: &str,
) {
    let tail = t.felix.tail(INPUT).await.unwrap();
    let inputs_found = t.felix.read_range(INPUT, 0, tail).await.unwrap();
    assert_eq!(inputs_found.len() as u64, inputs);
    let want = expected(module, &inputs_found);
    eventually("all output", Duration::from_secs(60), || async {
        (t.outputs(output).await.len() >= want.len()).then_some(())
    })
    .await;
    // Give a duplicate time to show up before comparing.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let records: Vec<Vec<u8>> = t.read_all(output).await.into_iter().map(|r| r.1).collect();
    let epochs: Vec<u64> = records
        .iter()
        .map(|r| output::decode(r).unwrap().epoch())
        .collect();
    assert!(
        epochs.is_sorted(),
        "two owners took turns on {output}: {epochs:?}"
    );
    let got: Vec<&Vec<u8>> = records.iter().filter(|r| !output::is_marker(r)).collect();
    assert_eq!(got.len(), want.len(), "output length of {output}");
    assert!(
        got.iter()
            .zip(&want)
            .all(|(g, w)| output::same_output(g, w)),
        "{output} differs from an uninterrupted run"
    );
    let id = output
        .strip_prefix("wl.")
        .and_then(|o| o.strip_suffix(".out"))
        .unwrap();
    let (ok, report) = t.check(&["--id", id, "--complete"]);
    assert!(ok, "felixd check found problems:\n{report}");
}

/// Felix's shard_unavailable refusal with reason not_ready, matched by code
/// and reason, not by message text.
fn is_settling(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<felix_client::BrokerError>()
        .is_some_and(|e| {
            e.code == felix_client::ErrorCode::ShardUnavailable && e.reason() == Some("not_ready")
        })
}
