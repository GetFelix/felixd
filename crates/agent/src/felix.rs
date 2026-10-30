//! The Felix calls felixd makes, on one client.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use felix_client::{
    AckMode, BrokerError, CacheCondition, CacheConditionResult, ClientConfig, ClusterClient,
    ConditionalWrite, CursorErrorReason, ErrorCode, StartPosition, StreamRecord,
    SubscribeCursorError, VersionedValue, root_store_from_pem_file,
};

use crate::settings::Settings;

/// Reads of a stream page by page stay this small so one reply stays small.
const PAGE_RECORDS: u32 = 256;

/// How long a stream or cache the control plane just created may take to be
/// served by the broker.
const READY_WITHIN: Duration = Duration::from_secs(20);

/// A connection to Felix, scoped to one tenant and namespace. All streams here
/// have a single shard.
pub struct Felix {
    client: Arc<ClusterClient>,
    tenant: String,
    namespace: String,
}

pub use felix_client::{ClusterSubscription as Subscription, Event};

/// What [`Felix::cache_lookup`] found.
#[derive(Debug, PartialEq, Eq)]
pub enum Lookup {
    /// The broker does not know the cache, right now.
    NoCache,
    /// The value under the key, if any.
    Value(Option<Bytes>),
}

impl Felix {
    pub async fn connect(settings: &Settings) -> Result<Felix> {
        let roots = match &settings.ca_file {
            Some(path) => Some(Arc::new(root_store_from_pem_file(path)?)),
            None => None,
        };
        let quinn = felix_client::quic_client_config(roots, true)?;
        let config = ClientConfig {
            auth_tenant_id: Some(settings.tenant.clone()),
            auth_token: Some(settings.token.expose().to_owned()),
            // Time-based snapshot points come from input append times.
            timestamps: true,
            ..ClientConfig::optimized_defaults(quinn)
        };
        let client = ClusterClient::connect(&settings.brokers, &settings.server_name, config)
            .await
            .context("connect to Felix")?;
        Ok(Felix {
            client: Arc::new(client),
            tenant: settings.tenant.clone(),
            namespace: settings.namespace.clone(),
        })
    }

    pub async fn cache_get(&self, cache: &str, key: &str) -> Result<Option<Bytes>> {
        until_ready(|| {
            self.client
                .cache_get(&self.tenant, &self.namespace, cache, key)
        })
        .await
    }

    pub async fn cache_put(&self, cache: &str, key: &str, value: Bytes) -> Result<()> {
        until_ready(|| {
            self.client.cache_put(
                &self.tenant,
                &self.namespace,
                cache,
                key,
                value.clone(),
                None,
            )
        })
        .await
    }

    /// Stores `value` under `key` until `ttl_ms` passes without another put.
    pub async fn cache_put_ttl(
        &self,
        cache: &str,
        key: &str,
        value: Bytes,
        ttl_ms: u64,
    ) -> Result<()> {
        until_ready(|| {
            self.client.cache_put(
                &self.tenant,
                &self.namespace,
                cache,
                key,
                value.clone(),
                Some(ttl_ms),
            )
        })
        .await
    }

    pub async fn cache_delete(&self, cache: &str, key: &str) -> Result<()> {
        until_ready(|| {
            self.client
                .cache_delete(&self.tenant, &self.namespace, cache, key)
        })
        .await?;
        Ok(())
    }

    /// The value under `key` and its version.
    pub async fn cache_get_versioned(
        &self,
        cache: &str,
        key: &str,
    ) -> Result<Option<VersionedValue>> {
        until_ready(|| {
            self.client
                .cache_get_versioned(&self.tenant, &self.namespace, cache, key)
        })
        .await
    }

    /// Stores `value` only if the key's entry meets `condition`. An expired
    /// entry counts as absent.
    ///
    /// Not retried: a resend of a put that was applied would be refused by
    /// its own version and look like someone else's write.
    pub async fn cache_put_if(
        &self,
        cache: &str,
        key: &str,
        value: Bytes,
        ttl_ms: Option<u64>,
        condition: CacheCondition,
    ) -> Result<CacheConditionResult> {
        self.client
            .cache_put_if(
                &self.tenant,
                &self.namespace,
                cache,
                key,
                value,
                ttl_ms,
                condition,
            )
            .await
    }

    /// Removes `key` only if its version is `version`. Not retried, like
    /// [`Self::cache_put_if`].
    pub async fn cache_delete_if(
        &self,
        cache: &str,
        key: &str,
        version: u64,
    ) -> Result<CacheConditionResult> {
        self.client
            .cache_delete_if(&self.tenant, &self.namespace, cache, key, version)
            .await
    }

    /// Publishes one record and waits until it is acknowledged.
    pub async fn publish(&self, stream: &str, payload: Vec<u8>) -> Result<()> {
        until_ready(|| {
            self.client.publish(
                &self.tenant,
                &self.namespace,
                stream,
                payload.clone(),
                AckMode::PerMessage,
            )
        })
        .await?;
        Ok(())
    }

    /// Appends `payloads` only if the stream's next offset is `expected`.
    pub async fn publish_if(
        &self,
        stream: &str,
        payloads: Vec<Vec<u8>>,
        expected: u64,
    ) -> Result<ConditionalWrite> {
        self.client
            .publish_if(
                &self.tenant,
                &self.namespace,
                stream,
                None,
                payloads,
                expected,
            )
            .await
    }

    /// The stream's next offset.
    ///
    /// Felix has no call for this that a node's own grants allow, so this asks
    /// for a conditional write at an offset no stream reaches. The refusal
    /// carries the tail and nothing is written.
    pub async fn tail(&self, stream: &str) -> Result<u64> {
        let probe = until_ready(|| self.publish_if(stream, vec![vec![0]], u64::MAX)).await?;
        match probe {
            ConditionalWrite::Refused { tail } => Ok(tail),
            ConditionalWrite::Written { offset } => {
                bail!("probe write to {stream} landed at offset {offset}")
            }
        }
    }

    /// The stream's next offset, asked with a read so that the read grant is
    /// enough. A read past the end is refused with the leading broker's log
    /// tail, which counts every committed record, so it is never below what
    /// a reader has already been given.
    pub async fn read_tail(&self, stream: &str) -> Result<u64> {
        let read = until_ready(|| {
            self.client
                .read(&self.tenant, &self.namespace, stream, 0, u64::MAX, None, 1)
        })
        .await;
        match read {
            Err(error) => match error.downcast_ref::<SubscribeCursorError>() {
                Some(refused) if matches!(refused.reason, CursorErrorReason::InFuture) => {
                    Ok(refused.available)
                }
                _ => Err(error),
            },
            Ok(page) => bail!(
                "reading {stream} past its end answered with a page to {}",
                page.next_offset
            ),
        }
    }

    /// The records at offsets `from..end`, in order. Offsets that hold no
    /// event, such as a new leader's first record, are absent.
    pub async fn read_range(&self, stream: &str, from: u64, end: u64) -> Result<Vec<StreamRecord>> {
        let mut records = Vec::new();
        let mut next = from;
        while next < end {
            let (page, after) = self.read_page(stream, next, end).await?;
            records.extend(page);
            next = after;
        }
        Ok(records)
    }

    /// One page of the records at offsets `from..end`, and the offset the
    /// next page starts at, which is past `from`. `from` must be below `end`.
    pub async fn read_page(
        &self,
        stream: &str,
        from: u64,
        end: u64,
    ) -> Result<(Vec<StreamRecord>, u64)> {
        advancing(stream, from, || self.read_one_page(stream, from, end)).await
    }

    async fn read_one_page(
        &self,
        stream: &str,
        from: u64,
        end: u64,
    ) -> Result<(Vec<StreamRecord>, u64)> {
        if let Some(oldest) = crate::faults::trimmed_below(stream, from) {
            return Err(Trimmed {
                stream: stream.to_owned(),
                requested: from,
                oldest,
            }
            .into());
        }
        let page = self
            .client
            .read(
                &self.tenant,
                &self.namespace,
                stream,
                0,
                from,
                Some(end),
                PAGE_RECORDS,
            )
            .await
            .map_err(|error| trimmed_from(error, stream))?;
        Ok((page.records, page.next_offset))
    }

    /// The records at `from..end` for a reader walking back from a tail.
    /// When Felix has trimmed below `end`, it reads from the oldest offset
    /// still held instead, and says the walk ends there, so a reader that
    /// only needs records above a trim point never asks for one below it.
    pub(crate) async fn read_back(
        &self,
        stream: &str,
        from: u64,
        end: u64,
    ) -> Result<(Vec<StreamRecord>, bool)> {
        match self.read_range(stream, from, end).await {
            Ok(records) => Ok((records, from == 0)),
            Err(error) => match Trimmed::of(&error).map(|t| t.oldest) {
                Some(oldest) if oldest > from && oldest >= end => Ok((Vec::new(), true)),
                Some(oldest) if oldest > from => {
                    Ok((self.read_range(stream, oldest, end).await?, true))
                }
                _ => Err(error),
            },
        }
    }

    /// The last record of the stream, given its `tail`.
    pub async fn last_record(&self, stream: &str, tail: u64) -> Result<Option<StreamRecord>> {
        let mut window = 16;
        loop {
            let from = tail.saturating_sub(window);
            let (mut records, ended) = self.read_back(stream, from, tail).await?;
            if let Some(last) = records.pop() {
                return Ok(Some(last));
            }
            if ended {
                return Ok(None);
            }
            window *= 8;
        }
    }

    /// The first offset appended at or after `at_micros` (microseconds since
    /// the Unix epoch), by the clock of the broker that led the shard. `None`
    /// when no record is that recent.
    pub async fn offset_for_time(&self, stream: &str, at_micros: u64) -> Result<Option<u64>> {
        until_ready(|| {
            self.client
                .offset_for_time(&self.tenant, &self.namespace, stream, 0, at_micros)
        })
        .await
    }

    /// Subscribes to the stream starting at input offset `from`.
    pub async fn subscribe_from(&self, stream: &str, from: u64) -> Result<Subscription> {
        if let Some(oldest) = crate::faults::trimmed_below(stream, from) {
            return Err(Trimmed {
                stream: stream.to_owned(),
                requested: from,
                oldest,
            }
            .into());
        }
        self.client
            .subscribe_from(
                &self.tenant,
                &self.namespace,
                stream,
                Some(StartPosition::Offset(from)),
            )
            .await
            .map_err(|error| trimmed_from(error, stream))
    }

    /// Whether the broker serving `watch` answers a read of `cache` within
    /// `within`. A watch whose broker died can wait forever without ending,
    /// so the agent asks now and then.
    pub async fn watch_answers(
        &self,
        watch: &felix_client::ClusterCacheWatch,
        cache: &str,
        within: Duration,
    ) -> bool {
        let read = watch
            .client()
            .cache_get(&self.tenant, &self.namespace, cache, "felixd-probe");
        matches!(tokio::time::timeout(within, read).await, Ok(Ok(_)))
    }

    /// Watches every key of `cache`, delivering the current entries first.
    pub async fn watch_retained(&self, cache: &str) -> Result<felix_client::ClusterCacheWatch> {
        self.client
            .watch_cache_retained(
                &self.tenant,
                &self.namespace,
                cache,
                felix_client::CacheWatchFilter::Prefix(String::new()),
            )
            .await
    }

    /// The value under `key`, telling a cache the broker does not know
    /// apart from a missing key, and without [`Felix::cache_get`]'s wait for
    /// a cache that is not served yet. A broker that is restarting or has
    /// not loaded the cache answers the same way as for one never created,
    /// so [`Lookup::NoCache`] is not proof that the cache does not exist.
    pub async fn cache_lookup(&self, cache: &str, key: &str) -> Result<Lookup> {
        match self
            .client
            .cache_get(&self.tenant, &self.namespace, cache, key)
            .await
        {
            Ok(value) => Ok(Lookup::Value(value)),
            Err(error) if not_found(&error) => Ok(Lookup::NoCache),
            Err(error) => Err(error),
        }
    }

    /// Every live entry of `cache`, sorted by key, or none when the cache
    /// does not exist.
    pub async fn entries(&self, cache: &str) -> Result<Vec<(String, Bytes)>> {
        let mut watch = match self.watch_retained(cache).await {
            Ok(watch) => watch,
            Err(error) if not_found(&error) => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let count = watch
            .retained_count()
            .with_context(|| format!("the {cache} watch did not say how many entries it holds"))?;
        let mut entries = Vec::new();
        while (entries.len() as u64) < count {
            match watch.recv().await {
                Some(felix_client::CacheWatchItem::Change(change)) => {
                    if let Some(value) = change.value {
                        entries.push((change.key, value));
                    }
                }
                _ => bail!("the {cache} watch ended early"),
            }
        }
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(entries)
    }
}

/// Calls `page` until it gets past `from`. A page stops short at what the
/// broker has committed, which can lag the tail a caller was given, for
/// example just after a leader change. Returning that short page as the whole
/// range would make the records past it look absent.
async fn advancing<T, F, Fut>(stream: &str, from: u64, mut page: F) -> Result<(Vec<T>, u64)>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(Vec<T>, u64)>>,
{
    let deadline = tokio::time::Instant::now() + READY_WITHIN;
    loop {
        let (records, after) = page().await?;
        if after > from {
            return Ok((records, after));
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("reading {stream} made no progress past offset {from}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Retries `call` while the broker says it does not serve the stream or cache
/// yet, which is how a resource the control plane has just created looks for a
/// moment.
async fn until_ready<T, F, Fut>(mut call: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let deadline = Instant::now() + READY_WITHIN;
    loop {
        match call().await {
            Err(error) if Instant::now() < deadline && not_ready(&error) => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            result => return result,
        }
    }
}

fn not_found(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<BrokerError>()
        .is_some_and(|broker| broker.code == ErrorCode::NotFound)
}

fn not_ready(error: &anyhow::Error) -> bool {
    error.downcast_ref::<BrokerError>().is_some_and(|broker| {
        matches!(
            broker.code,
            ErrorCode::NotFound | ErrorCode::ShardUnavailable
        )
    })
}

#[cfg(test)]
mod tests;

/// A read or subscription asked for records Felix no longer holds: retention
/// or a trim passed them.
#[derive(Debug)]
pub struct Trimmed {
    pub stream: String,
    pub requested: u64,
    pub oldest: u64,
}

impl std::fmt::Display for Trimmed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} no longer holds offset {}: history before {} is trimmed",
            self.stream, self.requested, self.oldest
        )
    }
}

impl std::error::Error for Trimmed {}

impl Trimmed {
    /// The trim `error` reports, if it is one.
    pub fn of(error: &anyhow::Error) -> Option<&Trimmed> {
        error.downcast_ref::<Trimmed>()
    }
}

/// `error` as [`Trimmed`] when Felix said the offset is too old.
fn trimmed_from(error: anyhow::Error, stream: &str) -> anyhow::Error {
    match error.downcast_ref::<SubscribeCursorError>() {
        Some(refused) if matches!(refused.reason, CursorErrorReason::TooOld) => Trimmed {
            stream: stream.to_owned(),
            requested: refused.requested,
            oldest: refused.available,
        }
        .into(),
        _ => error,
    }
}
