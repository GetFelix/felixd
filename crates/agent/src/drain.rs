//! Draining a node. An operator writes a mark under the node's id in the
//! `drains` cache. The node's agent then stops each workload it holds and
//! deletes its lease, so another node can claim it at once, and claims
//! nothing until the mark is deleted.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use anyhow::{Result, bail};
use bytes::Bytes;
use felixd_core::lease::Lease;
use felixd_core::names;
use felixd_core::node::{self, Drain, NodeInfo};

use crate::admin::{self, StreamKind};
use crate::faults;
use crate::felix::{Felix, Lookup};
use crate::settings::Settings;

/// Marks `node` as draining, with a new nonce. Creates the `drains` cache
/// with `replication_factor` replicas if it does not exist.
pub async fn mark(settings: &Settings, node: &str, replication_factor: u32) -> Result<()> {
    let kind = StreamKind { replication_factor };
    admin::ensure_cache(settings, names::DRAINS, kind).await?;
    let felix = Felix::connect(settings).await?;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let mark = Drain {
        nonce: (nanos ^ (u64::from(std::process::id()) << 32)) | 1,
    };
    felix
        .cache_put(names::DRAINS, node, Bytes::from(mark.encode()))
        .await
}

/// Deletes `node`'s drain mark, so it claims work again. Returns whether
/// there was one.
pub async fn lift(settings: &Settings, node: &str) -> Result<bool> {
    let felix = Felix::connect(settings).await?;
    match felix.cache_lookup(names::DRAINS, node).await? {
        Lookup::Value(None) => Ok(false),
        Lookup::Value(Some(_)) => {
            felix.cache_delete(names::DRAINS, node).await?;
            Ok(true)
        }
        Lookup::NoCache => bail!(
            "the broker does not serve the {} cache right now; if no node was ever drained \
             there is no mark to lift, otherwise try again",
            names::DRAINS
        ),
    }
}

/// What a node has learned from reading its own drain mark, across reads.
#[derive(Debug, Default)]
pub(crate) struct MarkReads {
    /// A read has found the `drains` cache.
    found_cache: AtomicBool,
    /// The warning about a mark this build cannot read has been logged.
    warned: AtomicBool,
    /// The nonce of the mark last read, 0 for none or one this build
    /// cannot read.
    nonce: AtomicU64,
}

impl MarkReads {
    /// Whether `node` is marked as draining. A mark this build cannot read
    /// still counts, so that an older node never takes work a newer operator
    /// meant to keep from it.
    pub(crate) async fn marked(&self, felix: &Felix, node: &str) -> Result<bool> {
        if faults::drain_read_error() {
            bail!("injected drain mark read error");
        }
        let found = felix.cache_lookup(names::DRAINS, node).await?;
        self.decide(faults::drain_lookup(found))
    }

    /// The nonce of the mark last read, if there is one this build reads.
    pub(crate) fn nonce(&self) -> Option<u64> {
        Some(self.nonce.load(Ordering::SeqCst)).filter(|&n| n != 0)
    }

    /// A broker that does not know the cache is believed only until a read
    /// has found it: after that it is a broker restarting or not yet serving
    /// it, and the answer is unknown, never "not draining".
    pub(crate) fn decide(&self, found: Lookup) -> Result<bool> {
        match found {
            Lookup::NoCache if self.found_cache.load(Ordering::SeqCst) => bail!(
                "the broker does not serve the {} cache, which this node has read before",
                names::DRAINS
            ),
            Lookup::NoCache => {
                self.nonce.store(0, Ordering::SeqCst);
                Ok(false)
            }
            Lookup::Value(value) => {
                self.found_cache.store(true, Ordering::SeqCst);
                let Some(value) = value else {
                    self.nonce.store(0, Ordering::SeqCst);
                    return Ok(false);
                };
                match Drain::decode(&value) {
                    Ok(mark) => self.nonce.store(mark.nonce, Ordering::SeqCst),
                    Err(error) => {
                        self.nonce.store(0, Ordering::SeqCst);
                        if !self.warned.swap(true, Ordering::SeqCst) {
                            tracing::warn!(%error, "a drain mark this build cannot read, draining anyway");
                        }
                    }
                }
                Ok(true)
            }
        }
    }
}

/// How far a drain has got.
#[derive(Debug, PartialEq, Eq)]
pub struct Progress {
    /// Whether the node has an entry in `nodes`.
    pub up: bool,
    /// Whether the node has said it read the current mark and has no claim
    /// under way. An acknowledgement of an earlier mark does not count.
    pub acknowledged: bool,
    /// The workloads whose live lease names it.
    pub held: Vec<String>,
}

impl Progress {
    /// Nothing names the node and it will claim nothing: it has said so, or
    /// it is not up. A node that is stopped, and so not up, with a claim
    /// under way can still land that claim when it wakes; it releases it
    /// at its next read of the mark.
    pub fn done(&self) -> bool {
        self.held.is_empty() && (self.acknowledged || !self.up)
    }
}

/// Reads how far draining `node` has got. The node's entry is read before
/// the leases, so a lease it took before saying it was draining is seen.
pub async fn progress(felix: &Felix, node: &str) -> Result<Progress> {
    let mark = felix.cache_get(names::DRAINS, node).await?;
    let mark = mark.and_then(|m| Drain::decode(&m).ok());
    let entry = felix.cache_get(names::NODES, node).await?;
    let info = entry.as_ref().and_then(|e| NodeInfo::decode(e).ok());
    let acknowledged = node::acknowledges(info.as_ref(), mark.as_ref());
    let held = leases(felix)
        .await?
        .into_iter()
        .filter(|(_, lease)| lease.owner == node)
        .map(|(id, _)| id)
        .collect();
    Ok(Progress {
        up: entry.is_some(),
        acknowledged,
        held,
    })
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

/// The nodes marked as draining, with each mark if this build reads it.
pub(crate) async fn draining(felix: &Felix) -> Result<Vec<(String, Option<Drain>)>> {
    Ok(felix
        .entries(names::DRAINS)
        .await?
        .into_iter()
        .map(|(node, mark)| (node, Drain::decode(&mark).ok()))
        .collect())
}

#[cfg(test)]
mod tests;
