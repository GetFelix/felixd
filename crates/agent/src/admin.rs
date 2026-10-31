//! The control plane's REST API. `felixd run` and `felixd init` use it, and a
//! node only with admission's credential, to create an accepted workload's
//! streams.

use anyhow::{Context, Result, bail};
use felixd_core::grants::{self, Policy};
use felixd_core::names;
use serde_json::{Value, json};

use crate::settings::Settings;

/// How a stream or cache is replicated.
#[derive(Clone, Copy, Debug)]
pub struct StreamKind {
    pub replication_factor: u32,
}

impl StreamKind {
    /// `Quorum` with more than one replica, so an acknowledged write survives
    /// the loss of its leader. `Leader` otherwise.
    fn consistency(self) -> &'static str {
        if self.replication_factor > 1 {
            "Quorum"
        } else {
            "Leader"
        }
    }
}

/// Creates the namespace and its system namespace, if they do not exist.
pub async fn ensure_namespace(settings: &Settings) -> Result<()> {
    for namespace in [
        settings.namespace.clone(),
        names::system_namespace(&settings.namespace),
    ] {
        let body = json!({"namespace": namespace, "display_name": namespace});
        post(settings, "namespaces", body).await?;
    }
    Ok(())
}

/// Creates a single-shard durable stream, if it does not exist.
pub async fn ensure_stream(settings: &Settings, stream: &str, kind: StreamKind) -> Result<()> {
    let body = json!({
        "stream": stream,
        "kind": "Stream",
        "shards": 1,
        "replication_factor": kind.replication_factor,
        "retention": {"max_age_seconds": null, "max_size_bytes": null},
        "consistency": kind.consistency(),
        "delivery": "AtLeastOnce",
        "durable": true,
    });
    let namespace = names::namespace_of(&settings.namespace, stream);
    post(settings, &format!("namespaces/{namespace}/streams"), body).await
}

/// Creates a cache, if it does not exist. An existing cache keeps the
/// replication it was created with.
pub async fn ensure_cache(settings: &Settings, cache: &str, kind: StreamKind) -> Result<()> {
    let body = json!({
        "cache": cache,
        "display_name": cache,
        "replication_factor": kind.replication_factor,
        "consistency": kind.consistency(),
    });
    let namespace = &settings.namespace;
    post(settings, &format!("namespaces/{namespace}/caches"), body).await
}

/// Writes every rule of [`grants::policies`] for the namespace that the
/// control plane does not already hold, so running it again writes nothing.
/// Needs `rbac.view` and `rbac.policy.manage` over the namespace and its
/// system namespace. Assigning principals to the roles is left to an
/// operator.
pub async fn ensure_grants(settings: &Settings) -> Result<()> {
    let (status, text) = call(settings, "GET", "rbac/policies", None).await?;
    if status != 200 {
        bail!("listing RBAC policies failed with HTTP {status}: {text}");
    }
    let held: Vec<Policy> =
        serde_json::from_str(&text).context("the control plane's RBAC policy listing")?;
    for policy in grants::policies(&settings.tenant, &settings.namespace) {
        if held.contains(&policy) {
            continue;
        }
        let body = serde_json::to_value(&policy)?;
        let (status, text) = call(settings, "POST", "rbac/policies", Some(body)).await?;
        if !(200..=299).contains(&status) {
            bail!(
                "granting {} {} to {} failed with HTTP {status}: {text}",
                policy.action,
                policy.object,
                policy.subject
            );
        }
    }
    Ok(())
}

/// POSTs to `/v1/tenants/{tenant}/{path}`. A 409 means it already exists.
async fn post(settings: &Settings, path: &str, body: Value) -> Result<()> {
    let (status, text) = call(settings, "POST", path, Some(body)).await?;
    match status {
        200..=299 | 409 => Ok(()),
        _ => bail!("creating {path} failed with HTTP {status}: {text}"),
    }
}

/// Calls `/v1/tenants/{tenant}/{path}` with the control plane token, and
/// returns the status and body.
async fn call(
    settings: &Settings,
    method: &'static str,
    path: &str,
    body: Option<Value>,
) -> Result<(u16, String)> {
    let token = settings
        .controlplane_token
        .clone()
        .context("a control plane token is needed to create streams, caches and grants")?;
    let authorization = format!("Bearer {}", token.expose());
    let url = format!(
        "{}/v1/tenants/{}/{}",
        settings.controlplane_url.trim_end_matches('/'),
        settings.tenant,
        path
    );
    tokio::task::spawn_blocking(move || -> Result<(u16, String)> {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .into();
        let sent = match body {
            Some(body) => agent
                .post(&url)
                .header("Authorization", &authorization)
                .send_json(&body),
            None => agent
                .get(&url)
                .header("Authorization", &authorization)
                .call(),
        };
        let mut response = sent.with_context(|| format!("{method} {url}"))?;
        let text = response.body_mut().read_to_string().unwrap_or_default();
        Ok((response.status().as_u16(), text))
    })
    .await?
}
