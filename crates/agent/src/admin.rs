//! The control plane's REST API. Only `felixd run` and tests use it: agents
//! never create streams.

use anyhow::{Context, Result, bail};
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

/// Creates the namespace, if it does not exist.
pub async fn ensure_namespace(settings: &Settings) -> Result<()> {
    let body = json!({"namespace": settings.namespace, "display_name": settings.namespace});
    post(settings, "namespaces", body).await
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
    post(
        settings,
        &format!("{}/streams", namespace_path(settings)),
        body,
    )
    .await
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
    post(
        settings,
        &format!("{}/caches", namespace_path(settings)),
        body,
    )
    .await
}

/// Deletes a stream. Only tests do this, to recreate one under its name.
pub async fn delete_stream(settings: &Settings, stream: &str) -> Result<()> {
    let path = format!("{}/streams/{stream}", namespace_path(settings));
    send(settings, &path, None).await
}

fn namespace_path(settings: &Settings) -> String {
    format!("namespaces/{}", settings.namespace)
}

/// POSTs to `/v1/tenants/{tenant}/{path}`. A 409 means it already exists.
async fn post(settings: &Settings, path: &str, body: Value) -> Result<()> {
    send(settings, path, Some(body)).await
}

/// POSTs `body` to `/v1/tenants/{tenant}/{path}`, or DELETEs it without
/// one. A 409 counts as done.
async fn send(settings: &Settings, path: &str, body: Option<Value>) -> Result<()> {
    let method = if body.is_some() { "POST" } else { "DELETE" };
    let token = settings
        .controlplane_token
        .clone()
        .context("a control plane token is needed to create streams and caches")?;
    let url = format!(
        "{}/v1/tenants/{}/{}",
        settings.controlplane_url.trim_end_matches('/'),
        settings.tenant,
        path
    );
    let (status, text) = tokio::task::spawn_blocking(move || -> Result<(u16, String)> {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .into();
        let auth = format!("Bearer {token}");
        let mut response = match body {
            Some(body) => agent
                .post(&url)
                .header("Authorization", &auth)
                .send_json(&body),
            None => agent.delete(&url).header("Authorization", &auth).call(),
        }
        .with_context(|| format!("{method} {url}"))?;
        let text = response.body_mut().read_to_string().unwrap_or_default();
        Ok((response.status().as_u16(), text))
    })
    .await??;
    match status {
        200..=299 | 409 => Ok(()),
        _ => bail!("{method} {path} failed with HTTP {status}: {text}"),
    }
}
