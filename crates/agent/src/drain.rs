//! Draining a node. An operator writes a mark under the node's id in the
//! `drains` cache. The node's agent then stops each workload it holds and
//! deletes its lease, so another node can claim it at once, and claims
//! nothing until the mark is deleted.

use anyhow::Result;
use bytes::Bytes;
use felixd_core::lease::Lease;
use felixd_core::names;
use felixd_core::node::Drain;

use crate::admin::{self, StreamKind};
use crate::felix::Felix;
use crate::settings::Settings;

/// Marks `node` as draining. Creates the `drains` cache with
/// `replication_factor` replicas if it does not exist.
pub async fn mark(settings: &Settings, node: &str, replication_factor: u32) -> Result<()> {
    let kind = StreamKind { replication_factor };
    admin::ensure_cache(settings, names::DRAINS, kind).await?;
    let felix = Felix::connect(settings).await?;
    felix
        .cache_put(names::DRAINS, node, Bytes::from(Drain {}.encode()))
        .await
}

/// Deletes `node`'s drain mark, so it claims work again. Returns whether
/// there was one.
pub async fn lift(settings: &Settings, node: &str) -> Result<bool> {
    let felix = Felix::connect(settings).await?;
    if felix
        .cache_get_if_exists(names::DRAINS, node)
        .await?
        .is_none()
    {
        return Ok(false);
    }
    felix.cache_delete(names::DRAINS, node).await?;
    Ok(true)
}

/// Whether `node` is marked as draining. A mark this build cannot read
/// still counts, so that an older node never takes work a newer operator
/// meant to keep from it.
pub(crate) async fn marked(felix: &Felix, node: &str) -> Result<bool> {
    let Some(found) = felix.cache_get_if_exists(names::DRAINS, node).await? else {
        return Ok(false);
    };
    if let Err(error) = Drain::decode(&found) {
        tracing::warn!(%error, "a drain mark this build cannot read, draining anyway");
    }
    Ok(true)
}

/// The workloads whose live lease names `node`.
pub async fn held_by(settings: &Settings, node: &str) -> Result<Vec<String>> {
    let felix = Felix::connect(settings).await?;
    Ok(leases(&felix)
        .await?
        .into_iter()
        .filter(|(_, lease)| lease.owner == node)
        .map(|(id, _)| id)
        .collect())
}

/// Every live lease, by workload id. A value that does not decode is left
/// out.
pub(crate) async fn leases(felix: &Felix) -> Result<Vec<(String, Lease)>> {
    Ok(felix
        .entries(names::LEASES)
        .await?
        .into_iter()
        .filter_map(|(id, value)| Some((id, Lease::decode(&value).ok()?)))
        .collect())
}

/// The nodes marked as draining.
pub(crate) async fn draining(felix: &Felix) -> Result<Vec<String>> {
    Ok(felix
        .entries(names::DRAINS)
        .await?
        .into_iter()
        .map(|(node, _)| node)
        .collect())
}
