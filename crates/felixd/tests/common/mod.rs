//! A Felix to test against, found through the same environment variables the
//! CLI reads. CI starts it from `tests/felix/compose.yaml`.

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use felixd_agent::felix::Felix;
use felixd_agent::settings::Settings;

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

    pub(crate) fn start_agent(&self) -> Child {
        self.felixd(&["agent"]).spawn().expect("start felixd agent")
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
