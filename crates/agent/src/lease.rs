//! Claiming, renewing and releasing a workload's lease.
//!
//! A claim is a `cache_put_if` on "absent", so it succeeds only once the
//! broker itself counts the previous lease as expired or deleted. The holder
//! renews every TTL/3 at the version it last wrote. A refused renewal is
//! evidence that the lease is gone, and the holder stops at once. A holder that
//! cannot reach Felix stops by its own clock at 2/3 of the TTL, before any
//! other node can claim.
//!
//! The lease decides who should run a workload. What keeps the output correct
//! when a holder is frozen or its clock is wrong is the epoch on every output
//! record; see `felixd_core::output::reconcile`.

use std::future::Future;

use anyhow::Result;
use bytes::Bytes;
use felix_client::{CacheCondition, CacheConditionResult, VersionedValue};
use felixd_core::lease::{Lease, LeaseTiming};
use felixd_core::names;
use tokio::sync::watch;
use tokio::time::{Duration, Instant};

use crate::felix::Felix;

/// How soon a renewal that failed with an error is tried again.
const RETRY_RENEWAL: Duration = Duration::from_millis(250);

/// The cache calls a lease needs, so the rules here can be tested without a
/// broker.
pub(crate) trait LeaseStore: Send + Sync {
    fn put_if(
        &self,
        key: &str,
        value: Bytes,
        ttl_ms: u64,
        condition: CacheCondition,
    ) -> impl Future<Output = Result<CacheConditionResult>> + Send;

    fn get_versioned(
        &self,
        key: &str,
    ) -> impl Future<Output = Result<Option<VersionedValue>>> + Send;

    fn delete_if(
        &self,
        key: &str,
        version: u64,
    ) -> impl Future<Output = Result<CacheConditionResult>> + Send;
}

impl LeaseStore for Felix {
    async fn put_if(
        &self,
        key: &str,
        value: Bytes,
        ttl_ms: u64,
        condition: CacheCondition,
    ) -> Result<CacheConditionResult> {
        self.cache_put_if(names::LEASES, key, value, Some(ttl_ms), condition)
            .await
    }

    async fn get_versioned(&self, key: &str) -> Result<Option<VersionedValue>> {
        self.cache_get_versioned(names::LEASES, key).await
    }

    async fn delete_if(&self, key: &str, version: u64) -> Result<CacheConditionResult> {
        self.cache_delete_if(names::LEASES, key, version).await
    }
}

/// A lease this node holds.
#[derive(Clone, Debug)]
pub(crate) struct Held {
    pub(crate) node: String,
    pub(crate) workload: String,
    pub(crate) timing: LeaseTiming,
    pub(crate) epoch: u64,
    /// The version the last applied write made, which the next renewal must
    /// find.
    pub(crate) version: u64,
    /// When the last applied write was sent.
    pub(crate) sent_at: Instant,
}

impl Held {
    fn deadline(&self) -> Instant {
        self.sent_at + self.timing.stop_after()
    }

    /// The fence a runner checks before each write, and the sender that
    /// [`hold`] moves forward on each renewal.
    pub(crate) fn fence(&self) -> (watch::Sender<Instant>, Fence) {
        let (tx, rx) = watch::channel(self.deadline());
        (
            tx,
            Fence {
                epoch: self.epoch,
                deadline: rx,
            },
        )
    }
}

/// What a runner holds while its node has the lease.
#[derive(Clone, Debug)]
pub(crate) struct Fence {
    epoch: u64,
    deadline: watch::Receiver<Instant>,
}

impl Fence {
    /// The lease epoch, stamped on every output record.
    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Whether this node may still write, by its own clock.
    pub(crate) fn holds(&self) -> bool {
        Instant::now() < *self.deadline.borrow()
    }
}

/// Why a holder stopped holding.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Lost {
    /// A renewal was refused: the lease expired, or another node holds it.
    Refused,
    /// Felix could not be reached before the local deadline.
    Unreachable,
}

/// Claims `workload` for `node` if no live lease exists. `None` means someone
/// holds it.
///
/// An error leaves the claim unknown. It is not adopted even if it was applied:
/// the lease then expires unused, and the workload waits one TTL.
pub(crate) async fn claim<S: LeaseStore>(
    store: &S,
    node: &str,
    workload: &str,
    timing: LeaseTiming,
) -> Result<Option<Held>> {
    let sent_at = Instant::now();
    let value = Bytes::from(Lease::claim(node).encode());
    let result = store
        .put_if(workload, value, timing.ttl_millis(), CacheCondition::Absent)
        .await?;
    Ok(match (result.applied, result.version) {
        (true, Some(version)) => Some(Held {
            node: node.to_owned(),
            workload: workload.to_owned(),
            timing,
            epoch: Lease::claim_epoch(version),
            version,
            sent_at,
        }),
        _ => None,
    })
}

/// Renews `held` until it is lost, moving `deadline` forward after each
/// renewal. Runs until it returns or is dropped.
pub(crate) async fn hold<S: LeaseStore>(
    store: &S,
    held: &mut Held,
    deadline: &watch::Sender<Instant>,
) -> Lost {
    loop {
        tokio::time::sleep_until(held.sent_at + held.timing.renew_after()).await;
        loop {
            match renew(store, held).await {
                Ok(true) => {
                    let _ = deadline.send(held.deadline());
                    break;
                }
                Ok(false) => return Lost::Refused,
                Err(error) => {
                    if Instant::now() >= held.deadline() {
                        tracing::warn!(workload = %held.workload, %error, "lease not renewed in time");
                        return Lost::Unreachable;
                    }
                    tracing::warn!(workload = %held.workload, %error, "renewing the lease failed, retrying");
                    tokio::time::sleep(RETRY_RENEWAL.min(held.deadline() - Instant::now())).await;
                }
            }
        }
    }
}

/// One renewal. `Ok(false)` is a refusal.
async fn renew<S: LeaseStore>(store: &S, held: &mut Held) -> Result<bool> {
    let sent_at = Instant::now();
    let value = Lease::renewal(&held.node, held.epoch);
    let put = store
        .put_if(
            &held.workload,
            Bytes::from(value.encode()),
            held.timing.ttl_millis(),
            CacheCondition::Version(held.version),
        )
        .await;
    match put {
        Ok(CacheConditionResult {
            applied: true,
            version: Some(version),
        }) => {
            held.version = version;
            held.sent_at = sent_at;
            Ok(true)
        }
        Ok(_) => Ok(false),
        // The answer was lost, so the renewal may have been applied. Only this
        // holder writes its own epoch, so finding it at a newer version means
        // the renewal landed.
        Err(error) => match store.get_versioned(&held.workload).await? {
            Some(found) if found.version == held.version => Err(error),
            Some(found) if Lease::decode(&found.value).as_ref() == Ok(&value) => {
                held.version = found.version;
                held.sent_at = sent_at;
                Ok(true)
            }
            _ => Ok(false),
        },
    }
}

/// Gives the lease up so another node can claim it at once. Best effort: if it
/// fails, the lease expires by itself.
pub(crate) async fn release<S: LeaseStore>(store: &S, held: &Held) {
    if let Err(error) = store.delete_if(&held.workload, held.version).await {
        tracing::warn!(workload = %held.workload, %error, "could not release the lease");
    }
}

#[cfg(test)]
mod tests;
