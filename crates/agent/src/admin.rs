//! The control plane's REST API. `felixd run` and `felixd init` use it, and a
//! node only with admission's credential, to create an accepted workload's
//! streams.

use anyhow::{Context, Result, bail};
use felixd_core::grants::{self, Grouping, Policy};
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
    names::validate_namespace(&settings.namespace)?;
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

/// Writes every rule of [`grants::policies`] the control plane does not hold,
/// so running it again writes nothing. It first lists every rule and role
/// assignment and refuses, writing nothing, when a felixd role holds more than
/// [`grants::policies`] gives it or one principal holds two roles
/// ([`grants::audit`]). Needs `rbac.view` over the whole tenant, because a
/// scope-filtered listing could hide the rule that breaks the roles apart; a
/// token with less is refused. Also needs `rbac.policy.manage`. Assigning
/// principals to the roles is left to an operator.
pub async fn ensure_grants(settings: &Settings) -> Result<()> {
    names::validate_namespace(&settings.namespace)?;
    require_tenant_view(settings)?;
    let held: Vec<Policy> = list(settings, "rbac/policies").await?;
    let groupings: Vec<Grouping> = list(settings, "rbac/groupings").await?;
    let missing = grants::audit(&settings.tenant, &settings.namespace, &held, &groupings)
        .map_err(|why| anyhow::anyhow!("refusing to continue: {why}"))?;
    for policy in missing {
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

/// The system streams that exist in the workload namespace itself, where they
/// lived before they moved to the system namespace. Asks the control plane,
/// not a broker: a broker answers "unassigned" for a stream it has never
/// heard of and "not found" for one it has not synced yet, so it can neither
/// confirm nor rule one out. A reply that is neither found nor not found
/// is an error, never "absent".
pub async fn legacy_system_streams(settings: &Settings) -> Result<Vec<String>> {
    let mut found = Vec::new();
    for stream in names::LEGACY_SYSTEM_STREAMS {
        let path = format!("namespaces/{}/streams/{stream}", settings.namespace);
        let (status, text) = call(settings, "GET", &path, None).await?;
        match status {
            200 => found.push(format!("{}/{stream}", settings.namespace)),
            404 => {}
            _ => bail!("looking for {path} failed with HTTP {status}: {text}"),
        }
    }
    Ok(found)
}

/// Fails when [`legacy_system_streams`] finds any. A node's publish grant on
/// every workload stream still reaches the old ones, so nothing may start
/// while one exists.
pub async fn refuse_legacy_streams(settings: &Settings) -> Result<()> {
    let found = legacy_system_streams(settings).await?;
    if found.is_empty() {
        return Ok(());
    }
    bail!(
        "{} still exist in namespace {}: the sys.* streams moved to {}, and a node's \
         publish grant on every stream of {} still reaches the old ones. Migrate first: \
         see docs/design.md, Migrating the system streams",
        found.join(", "),
        settings.namespace,
        names::system_namespace(&settings.namespace),
        settings.namespace
    )
}

/// Lists a control plane collection, unpaged.
async fn list<T: serde::de::DeserializeOwned>(settings: &Settings, path: &str) -> Result<Vec<T>> {
    let (status, text) = call(settings, "GET", path, None).await?;
    if status != 200 {
        bail!("listing {path} failed with HTTP {status}: {text}");
    }
    serde_json::from_str(&text).with_context(|| format!("the control plane's {path} listing"))
}

/// Refuses a control plane token that does not carry `rbac.view` over the
/// whole tenant. The control plane filters its policy listing to the caller's
/// scope, so with a narrower token the audit would be reading a partial list.
/// A token that cannot be read is refused too.
fn require_tenant_view(settings: &Settings) -> Result<()> {
    let token = settings
        .controlplane_token
        .as_ref()
        .context("a control plane token is needed to create streams, caches and grants")?;
    if view_is_tenant_wide(token.expose(), &settings.tenant) {
        return Ok(());
    }
    bail!(
        "the control plane token must carry rbac.view over tenant:{}, not a narrower scope: \
         the policy listing is filtered to the caller's scope, and a partial listing cannot \
         show whether a felixd role holds more than it should",
        settings.tenant
    )
}

/// Whether the JWT's `perms` claim holds `rbac.view` over `tenant:{tenant}`.
/// The signature is not checked: the control plane does that, and a forged
/// claim only makes the audit read a listing it would filter anyway.
fn view_is_tenant_wide(token: &str, tenant: &str) -> bool {
    use base64::Engine as _;
    let Some(payload) = token.split('.').nth(1) else {
        return false;
    };
    let Ok(bytes) = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload) else {
        return false;
    };
    let Ok(claims) = serde_json::from_slice::<Value>(&bytes) else {
        return false;
    };
    let wanted = format!("rbac.view:tenant:{tenant}");
    claims["perms"]
        .as_array()
        .is_some_and(|perms| perms.iter().any(|p| p.as_str() == Some(wanted.as_str())))
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

#[cfg(test)]
mod tests;
