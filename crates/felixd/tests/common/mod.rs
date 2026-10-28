//! A Felix to test against, found through the same environment variables the
//! CLI reads. CI starts it from `tests/felix/compose.yaml`.

// Each test binary uses only some of these.
#![allow(dead_code)]

use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use felixd_agent::admin::{StreamKind, ensure_stream};
use felixd_agent::felix::Felix;
use felixd_agent::settings::Settings;
use felixd_core::names;
use felixd_core::spec::{WorkloadSpec, WorkloadState};

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
        brokers: vec![brokers.parse().unwrap()],
        server_name: "localhost".into(),
        tenant: var("FELIX_AUTH_TENANT"),
        namespace: format!("t{nanos:x}"),
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
        Agent(
            self.felixd(&["agent"])
                .env("FELIXD_FAULTS", faults)
                .spawn()
                .expect("start felixd agent"),
        )
    }

    /// Creates the input stream and submits example workload `workload` as
    /// [`ID`] with `felixd run <args>`. Returns the module.
    pub(crate) async fn submit(&self, workload: &str, args: &[&str]) -> Vec<u8> {
        let module = felixd_testkit::workload(workload);
        let dir = std::env::temp_dir().join(format!("felixd-{}", self.settings.namespace));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{workload}.wasm"));
        std::fs::write(&path, &module).unwrap();
        let kind = StreamKind {
            replication_factor: 1,
        };
        ensure_stream(&self.settings, INPUT, kind).await.unwrap();
        let status = self
            .felixd(&["run", "--id", ID, "--input", INPUT])
            .args(args)
            .arg(&path)
            .status()
            .unwrap();
        assert!(status.success());
        module
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

    pub(crate) async fn read_all(&self, stream: &str) -> Vec<(u64, Vec<u8>)> {
        let tail = self.felix.tail(stream).await.unwrap();
        self.felix
            .read_range(stream, 0, tail)
            .await
            .unwrap()
            .into_iter()
            .map(|r| (r.offset, r.payload.to_vec()))
            .collect()
    }

    pub(crate) async fn spec(&self) -> WorkloadSpec {
        let value = self.felix.cache_get(names::WORKLOADS, ID).await.unwrap();
        WorkloadSpec::decode(&value.expect("the workload has a spec")).unwrap()
    }

    pub(crate) async fn put_spec(&self, spec: &WorkloadSpec) {
        let value = Bytes::from(spec.encode());
        self.felix
            .cache_put(names::WORKLOADS, ID, value)
            .await
            .unwrap();
    }

    /// Waits for the workload to be marked failed and returns why.
    pub(crate) async fn failure(&self) -> String {
        eventually("the workload to fail", Duration::from_secs(30), || async {
            let spec = self.spec().await;
            (spec.state == WorkloadState::Failed).then(|| spec.failure.unwrap_or_default())
        })
        .await
    }
}

/// A running `felixd agent`, killed when dropped so a failing test does not
/// leave it behind.
pub(crate) struct Agent(Child);

impl Agent {
    pub(crate) fn kill(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }

    /// Waits for the agent to exit by itself.
    pub(crate) async fn exited(&mut self, timeout: Duration) -> ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
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
