//! The control plane's REST API. Only `felixd run` and tests use it: agents
//! never create streams.

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use crate::settings::Settings;

/// How a stream is created.
#[derive(Clone, Copy, Debug)]
pub struct StreamKind {
    pub replication_factor: u32,
}

/// Creates the namespace, if it does not exist.
pub async fn ensure_namespace(settings: &Settings) -> Result<()> {
    let body = json!({"namespace": settings.namespace, "display_name": settings.namespace});
    post(settings, "namespaces", body).await
}

/// Creates a single-shard durable stream, if it does not exist. With more than
/// one replica it is a `Quorum` stream, otherwise `Leader`.
pub async fn ensure_stream(settings: &Settings, stream: &str, kind: StreamKind) -> Result<()> {
    let consistency = if kind.replication_factor > 1 {
        "Quorum"
    } else {
        "Leader"
    };
    let body = json!({
        "stream": stream,
        "kind": "Stream",
        "shards": 1,
        "replication_factor": kind.replication_factor,
        "retention": {"max_age_seconds": null, "max_size_bytes": null},
        "consistency": consistency,
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

/// Creates a cache, if it does not exist.
pub async fn ensure_cache(settings: &Settings, cache: &str) -> Result<()> {
    let body = json!({"cache": cache, "display_name": cache});
    post(
        settings,
        &format!("{}/caches", namespace_path(settings)),
        body,
    )
    .await
}

fn namespace_path(settings: &Settings) -> String {
    format!("namespaces/{}", settings.namespace)
}

/// POSTs to `/v1/tenants/{tenant}/{path}`. A 409 means it already exists.
async fn post(settings: &Settings, path: &str, body: Value) -> Result<()> {
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
        let mut response = agent
            .post(&url)
            .header("Authorization", &format!("Bearer {token}"))
            .send_json(&body)
            .with_context(|| format!("POST {url}"))?;
        let text = response.body_mut().read_to_string().unwrap_or_default();
        Ok((response.status().as_u16(), text))
    })
    .await??;
    match status {
        200..=299 | 409 => Ok(()),
        _ => bail!("creating {path} failed with HTTP {status}: {text}"),
    }
}
