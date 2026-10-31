//! The grants docs/design.md gives felixd's roles hold: a node runs a
//! workload whose input it may read but not publish to; `felixd init` writes
//! the roles' grants, once however often it runs; a submitter and a node are
//! refused on `sys.specs` and on creating streams; and with each principal
//! holding only its role, admission records an accepted spec and the
//! workload runs.
//!
//! Needs a Felix broker; see `common`. The restricted node's token comes from
//! `FELIXD_RESTRICTED_TOKEN_FILE`, which `tests/felix/compose.yaml` mints for
//! namespace `restricted` only. The roles' refresh tokens come from
//! `FELIXD_GRANTS_DIR`: compose mints them before `felixd init` has granted
//! anything, and a refresh re-reads the grants.

mod common;

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::{INPUT, OUTPUT, TestFelix, eventually, replication_factor, test_felix_in};
use felix_client::{BrokerError, ErrorCode};
use felixd_agent::admin::{StreamKind, ensure_stream};
use felixd_agent::admission;
use felixd_agent::felix::Felix;
use felixd_agent::settings::{Secret, Settings};
use felixd_core::grants::{self, Policy};
use felixd_core::names;
use felixd_core::spec::SpecRecord;

const NAMESPACE: &str = "restricted";

/// The namespace `felixd init` grants the roles in, as compose assigns them.
const GRANTED: &str = "granted";

#[tokio::test(flavor = "multi_thread")]
async fn a_node_without_publish_on_its_input_runs_the_workload() {
    let Ok(token) = std::env::var("FELIXD_RESTRICTED_TOKEN_FILE") else {
        if std::env::var_os("FELIXD_REQUIRE_FELIX").is_some() {
            panic!("FELIXD_REQUIRE_FELIX is set but FELIXD_RESTRICTED_TOKEN_FILE is not");
        }
        eprintln!("skipping: FELIXD_RESTRICTED_TOKEN_FILE is not set");
        return;
    };
    let Some(t) = test_felix_in(Some(NAMESPACE)).await else {
        return;
    };
    t.submit("counter", &[]).await;
    // The namespace outlives a run against the same stack, so count from
    // what is already there.
    let before = t.read_all(INPUT).await.len();
    t.publish_all(INPUT, (0..3u64).map(|i| i.to_be_bytes().to_vec()))
        .await;

    // The subscription also goes below the workload's position once, which
    // reads the input's tail again.
    let faults = format!("restart-input-after={}", before + 1);
    let mut agent = t.start_node_with_token("restricted-node", &token, &faults);
    let want = before + 3;
    eventually("the outputs", Duration::from_secs(60), || async {
        (t.outputs(OUTPUT).await.len() >= want).then_some(())
    })
    .await;
    assert!(agent.alive());
    assert_eq!(t.needs_operator().await.0, []);
}

/// Each role's tokens, after `felixd init` has granted the roles.
struct Roles {
    dir: PathBuf,
}

impl Roles {
    /// File holding the broker token of `role`, or its control plane token
    /// with `cp`.
    fn token_file(&self, role: &str, cp: bool) -> PathBuf {
        let cp = if cp { "-cp" } else { "" };
        self.dir.join(format!("felixd-{role}{cp}.jwt"))
    }

    fn token(&self, role: &str, cp: bool) -> Secret {
        Secret::new(std::fs::read_to_string(self.token_file(role, cp)).unwrap())
    }

    /// `t`'s settings with only `role`'s tokens.
    fn settings(&self, t: &TestFelix, role: &str) -> Settings {
        Settings {
            token: self.token(role, false),
            controlplane_token: Some(self.token(role, true)),
            ..t.settings.clone()
        }
    }
}

const ROLES: [&str; 3] = ["node", "admission", "submitter"];

/// Runs `felixd init` in [`GRANTED`] twice as the operator, checks the second
/// writes no grant again, then refreshes each role's tokens so they carry
/// what init granted. Once per test binary: a refresh token is spent when
/// used, so the new one is written back for the next run against the stack.
async fn roles() -> Option<(TestFelix, &'static Roles)> {
    let Ok(dir) = std::env::var("FELIXD_GRANTS_DIR") else {
        if std::env::var_os("FELIXD_REQUIRE_FELIX").is_some() {
            panic!("FELIXD_REQUIRE_FELIX is set but FELIXD_GRANTS_DIR is not");
        }
        eprintln!("skipping: FELIXD_GRANTS_DIR is not set");
        return None;
    };
    let t = test_felix_in(Some(GRANTED)).await?;
    static ROLES_ONCE: OnceLock<Roles> = OnceLock::new();
    let roles = ROLES_ONCE.get_or_init(|| init_and_refresh(&t, Path::new(&dir)));
    Some((t, roles))
}

fn init_and_refresh(t: &TestFelix, from: &Path) -> Roles {
    let dir = std::env::temp_dir().join(format!("felixd-grants-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let module = dir.join("admission.wasm");
    std::fs::write(&module, felixd_testkit::workload("admission")).unwrap();
    let replicas = replication_factor().to_string();
    for run in ["the first", "a second"] {
        let status = t
            .felixd(&["init", "--replication-factor", &replicas])
            .arg(&module)
            .status()
            .unwrap();
        assert!(status.success(), "{run} felixd init failed");
    }
    let held = list_policies(&t.settings);
    for policy in grants::policies(&t.settings.tenant, GRANTED) {
        let times = held.iter().filter(|p| **p == policy).count();
        assert_eq!(times, 1, "{policy:?} is held {times} times");
    }
    for role in ROLES {
        for cp in ["", "-cp"] {
            let refresh = from.join(format!("felixd-{role}{cp}-refresh.jwt"));
            let (access, next) = refreshed(&t.settings, &refresh);
            std::fs::write(&refresh, next).unwrap();
            std::fs::write(dir.join(format!("felixd-{role}{cp}.jwt")), access).unwrap();
        }
    }
    Roles { dir }
}

/// The tenant's RBAC policies, as the operator sees them.
fn list_policies(settings: &Settings) -> Vec<Policy> {
    let url = format!(
        "{}/v1/tenants/{}/rbac/policies",
        settings.controlplane_url.trim_end_matches('/'),
        settings.tenant
    );
    let token = settings.controlplane_token.as_ref().unwrap().expose();
    ureq::get(&url)
        .header("Authorization", &format!("Bearer {token}"))
        .call()
        .unwrap()
        .body_mut()
        .read_json()
        .unwrap()
}

/// Spends the refresh token in `file` for a new access token and a new
/// refresh token.
fn refreshed(settings: &Settings, file: &Path) -> (String, String) {
    let token = std::fs::read_to_string(file).unwrap();
    let url = format!(
        "{}/v1/tenants/{}/token/refresh",
        settings.controlplane_url.trim_end_matches('/'),
        settings.tenant
    );
    let answer: serde_json::Value = ureq::post(&url)
        .send_json(serde_json::json!({"refresh_token": token.trim()}))
        .unwrap_or_else(|e| panic!("refreshing {}: {e}", file.display()))
        .body_mut()
        .read_json()
        .unwrap();
    let field = |name: &str| answer[name].as_str().unwrap().to_owned();
    (field("felix_token"), field("refresh_token"))
}

fn forbidden(error: &anyhow::Error) -> bool {
    error
        .chain()
        .filter_map(|e| e.downcast_ref::<BrokerError>())
        .any(|e| e.code == ErrorCode::Forbidden)
}

fn kind() -> StreamKind {
    StreamKind {
        replication_factor: replication_factor(),
    }
}

/// Checks that `settings` may not append to `sys.specs` or create a stream.
async fn refused_specs_and_streams(role: &str, settings: &Settings) {
    let felix = Felix::connect(settings).await.unwrap();
    let tail = felix.read_tail(names::SPECS).await.unwrap();
    let forged = admission::running("forged", &"0".repeat(64), vec![INPUT.to_owned()]);
    let record = SpecRecord::new("forged", forged).encode();
    match felix.publish_if(names::SPECS, vec![record], tail).await {
        Err(error) => assert!(forbidden(&error), "{role}: {error:#}"),
        Ok(written) => panic!("{role} appended to {}: {written:?}", names::SPECS),
    }
    let created = ensure_stream(settings, "wl.forged.out", kind()).await;
    let error = created.expect_err(&format!("{role} created a stream"));
    assert!(
        format!("{error:#}").contains("HTTP 403"),
        "{role}: {error:#}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_submitter_is_refused_on_specs_and_on_creating_streams() {
    let Some((t, roles)) = roles().await else {
        return;
    };
    let submitter = roles.settings(&t, "submitter");
    refused_specs_and_streams("the submitter", &submitter).await;
    // What it may do: probe sys.intake's tail, which asks for a publish.
    let felix = Felix::connect(&submitter).await.unwrap();
    felix.tail(names::INTAKE).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_is_refused_on_specs_and_on_creating_streams() {
    let Some((t, roles)) = roles().await else {
        return;
    };
    refused_specs_and_streams("the node", &roles.settings(&t, "node")).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn with_only_its_role_each_principal_runs_a_submitted_workload() {
    let Some((t, roles)) = roles().await else {
        return;
    };
    // Its own workload and input, since the namespace outlives a run.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let id = format!("g{nanos:x}");
    let input = format!("in-{id}");
    ensure_stream(&t.settings, &input, kind()).await.unwrap();

    // A node, holding admission's credential beside its own token.
    let ttl = common::LEASE_TTL.as_millis().to_string();
    let child = t
        .felixd(&[
            "agent",
            "--node-id",
            &format!("node-{id}"),
            "--lease-ttl-ms",
            &ttl,
        ])
        .env("FELIX_AUTH_TOKEN_FILE", roles.token_file("node", false))
        .env(
            "FELIX_CONTROLPLANE_TOKEN_FILE",
            roles.token_file("admission", true),
        )
        .env(
            "FELIXD_ADMISSION_TOKEN_FILE",
            roles.token_file("admission", false),
        )
        .spawn()
        .unwrap();
    let mut agent = common::Agent::new(child, &format!("node-{id}"));

    // A submitter, with no control plane token.
    let module = felixd_testkit::workload("counter");
    let wasm = roles.dir.join(format!("{id}.wasm"));
    std::fs::write(&wasm, &module).unwrap();
    let status = t
        .felixd(&["run", "--id", &id, "--input", &input])
        .args(["--replication-factor", &replication_factor().to_string()])
        .env(
            "FELIX_AUTH_TOKEN_FILE",
            roles.token_file("submitter", false),
        )
        .env_remove("FELIX_CONTROLPLANE_TOKEN_FILE")
        .arg(&wasm)
        .status()
        .unwrap();
    assert!(status.success(), "the submitter's felixd run failed");

    let output = names::output_stream(&id);
    let recorded = t.read_all(names::SPECS).await.into_iter().any(|(_, r)| {
        SpecRecord::decode(&r).is_ok_and(|r| r.id == id && r.admitted.is_some() && !r.skipped)
    });
    assert!(recorded, "no admitted spec record for {id}");
    t.publish_all(&input, (0..3u64).map(|i| i.to_be_bytes().to_vec()))
        .await;
    eventually("the outputs", Duration::from_secs(60), || async {
        (t.outputs(&output).await.len() >= 3).then_some(())
    })
    .await;
    assert!(agent.alive());
}
