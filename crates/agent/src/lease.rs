//! Claiming, renewing and releasing a workload's lease.
//!
//! A claim is a `cache_put_if` on "absent", so it succeeds only once the
//! broker itself counts the previous lease as expired or deleted. The holder
//! renews every TTL/3 at the version it last wrote. A refused renewal is
//! evidence that the lease is gone, and the holder stops at once. A holder that
//! cannot reach Felix stops by its own clock at 2/3 of the TTL, before any
//! other node can claim.
//!
//! A claim that wins then takes the next epoch from the `epochs` cache with
//! `cache_put_if`, and writes it into the lease before it runs anything.
//!
//! A holder about to act on what it read, such as failing a workload, first
//! renews at once through [`Confirmer`]. The cache offers only absent and
//! version conditions, so an applied renewal at the held version is the only
//! evidence that the lease is still this holder's; a read could be stale.
//!
//! The lease decides who should run a workload. What keeps the output correct
//! when a holder is frozen or its clock is wrong is the claim a new owner
//! appends to the output stream and the epoch on every record; see
//! `felixd_core::output`.

use std::future::Future;

use anyhow::Result;
use bytes::Bytes;
use felix_client::{CacheCondition, CacheConditionResult, VersionedValue};
use felixd_core::lease::{self as rules, Lease, LeaseTiming};
use felixd_core::names;
use tokio::sync::{mpsc, oneshot, watch};
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

    /// The last epoch handed out for `key`, in the `epochs` cache.
    fn get_epoch(&self, key: &str) -> impl Future<Output = Result<Option<VersionedValue>>> + Send;

    fn put_epoch_if(
        &self,
        key: &str,
        value: Bytes,
        condition: CacheCondition,
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

    async fn get_epoch(&self, key: &str) -> Result<Option<VersionedValue>> {
        self.cache_get_versioned(names::EPOCHS, key).await
    }

    async fn put_epoch_if(
        &self,
        key: &str,
        value: Bytes,
        condition: CacheCondition,
    ) -> Result<CacheConditionResult> {
        self.cache_put_if(names::EPOCHS, key, value, None, condition)
            .await
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
    /// When the first renewal since then whose outcome is unknown was sent.
    /// If a read-back finds that one of them landed, it is credited from here,
    /// since it may have been the earliest.
    pub(crate) unconfirmed_since: Option<Instant>,
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
/// holds it, or took an epoch first.
///
/// `output_epoch` is the highest epoch on the workload's output and snapshot
/// streams. It is read only once the claim has won, so every earlier holder's
/// lease has expired by then.
///
/// An error leaves the claim unknown. It is not adopted even if it was applied:
/// the lease then expires unused, and the workload waits one TTL.
pub(crate) async fn claim<S: LeaseStore>(
    store: &S,
    node: &str,
    workload: &str,
    timing: LeaseTiming,
    output_epoch: impl Future<Output = Result<u64>>,
) -> Result<Option<Held>> {
    let value = Bytes::from(Lease::claim(node).encode());
    let result = store
        .put_if(workload, value, timing.ttl_millis(), CacheCondition::Absent)
        .await?;
    let (true, Some(claimed)) = (result.applied, result.version) else {
        return Ok(None);
    };
    let held = take_epoch(store, node, workload, timing, claimed, output_epoch).await;
    if !matches!(held, Ok(Some(_))) {
        let _ = store.delete_if(workload, claimed).await;
    }
    held
}

/// Takes the next epoch from the counter and writes it into the lease. Both
/// writes are conditional, so a claimer that was frozen after winning and
/// lost its lease meanwhile gets no epoch, or no lease to write it into.
async fn take_epoch<S: LeaseStore>(
    store: &S,
    node: &str,
    workload: &str,
    timing: LeaseTiming,
    claimed: u64,
    output_epoch: impl Future<Output = Result<u64>>,
) -> Result<Option<Held>> {
    let floor = output_epoch.await?;
    let (last, condition) = match store.get_epoch(workload).await? {
        Some(found) => (
            rules::decode_epoch(&found.value)?,
            CacheCondition::Version(found.version),
        ),
        None => (0, CacheCondition::Absent),
    };
    let epoch = Lease::next_epoch(last, floor);
    let taken = store
        .put_epoch_if(workload, Bytes::from(rules::encode_epoch(epoch)), condition)
        .await?;
    if !taken.applied {
        return Ok(None);
    }
    let sent_at = Instant::now();
    let value = Bytes::from(Lease::held(node, epoch).encode());
    let put = store
        .put_if(
            workload,
            value,
            timing.ttl_millis(),
            CacheCondition::Version(claimed),
        )
        .await?;
    Ok(match (put.applied, put.version) {
        (true, Some(version)) => Some(Held {
            node: node.to_owned(),
            workload: workload.to_owned(),
            timing,
            epoch,
            version,
            sent_at,
            unconfirmed_since: None,
        }),
        _ => None,
    })
}

/// Asks the renewal task for a renewal now. See [`Confirmer::confirm`].
pub(crate) type Asks = mpsc::Receiver<oneshot::Sender<()>>;

/// Lets a holder check that its lease is still its own before it acts.
#[derive(Clone)]
pub(crate) struct Confirmer(mpsc::Sender<oneshot::Sender<()>>);

impl Confirmer {
    pub(crate) fn new() -> (Confirmer, Asks) {
        let (tx, rx) = mpsc::channel(4);
        (Confirmer(tx), rx)
    }

    /// Renews the lease at once, conditional on the held version. `true` only
    /// if that renewal was applied. Anything else, including a renewal task
    /// that has stopped, means the lease may be another node's.
    pub(crate) async fn confirm(&self) -> bool {
        let (tx, rx) = oneshot::channel();
        self.0.send(tx).await.is_ok() && rx.await.is_ok()
    }
}

/// Renews `held` until it is lost, moving `deadline` forward after each
/// renewal. A request on `asks` renews at once and is answered only if that
/// renewal is applied. Runs until it returns or is dropped.
pub(crate) async fn hold<S: LeaseStore>(
    store: &S,
    held: &mut Held,
    deadline: &watch::Sender<Instant>,
    asks: &mut Asks,
) -> Lost {
    loop {
        let asked = tokio::select! {
            () = tokio::time::sleep_until(held.sent_at + held.timing.renew_after()) => None,
            Some(reply) = asks.recv() => Some(reply),
        };
        loop {
            match renew(store, held).await {
                Ok(true) => {
                    let _ = deadline.send(held.deadline());
                    if let Some(reply) = asked {
                        let _ = reply.send(());
                    }
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
    let earlier_unknown = held.unconfirmed_since.is_some();
    let first_unknown = *held.unconfirmed_since.get_or_insert(sent_at);
    let value = Lease::held(&held.node, held.epoch);
    let put = store
        .put_if(
            &held.workload,
            Bytes::from(value.encode()),
            held.timing.ttl_millis(),
            CacheCondition::Version(held.version),
        )
        .await;
    let error = match put {
        Ok(CacheConditionResult {
            applied: true,
            version: Some(version),
        }) => {
            // Any earlier renewal still unknown would have moved the version,
            // and this one would have been refused.
            held.version = version;
            held.sent_at = sent_at;
            held.unconfirmed_since = None;
            return Ok(true);
        }
        // Refused, and no earlier renewal can be what moved the version.
        Ok(_) if !earlier_unknown => return Ok(false),
        Ok(_) => None,
        Err(error) => Some(error),
    };
    // A renewal whose answer was lost may have been applied. Only this holder
    // writes its own epoch, so finding it at a newer version means one landed.
    match store.get_versioned(&held.workload).await? {
        Some(found) if found.version == held.version => match error {
            Some(error) => Err(error),
            None => Ok(false),
        },
        Some(found) if Lease::decode(&found.value).as_ref() == Ok(&value) => {
            held.version = found.version;
            held.sent_at = first_unknown;
            held.unconfirmed_since = None;
            Ok(true)
        }
        _ => Ok(false),
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
