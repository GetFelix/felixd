//! The Felix calls felixd makes, on one client.

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use felix_client::{
    AckMode, ClientConfig, ClusterClient, ConditionalWrite, StartPosition, StreamRecord,
    root_store_from_pem_file,
};

use crate::settings::Settings;

/// Reads of a stream page by page stay this small so one reply stays small.
const PAGE_RECORDS: u32 = 256;

/// A connection to Felix, scoped to one tenant and namespace. All streams here
/// have a single shard.
pub struct Felix {
    client: Arc<ClusterClient>,
    tenant: String,
    namespace: String,
}

pub use felix_client::{ClusterSubscription as Subscription, Event};

impl Felix {
    pub async fn connect(settings: &Settings) -> Result<Felix> {
        let roots = match &settings.ca_file {
            Some(path) => Some(Arc::new(root_store_from_pem_file(path)?)),
            None => None,
        };
        let quinn = felix_client::quic_client_config(roots, true)?;
        let config = ClientConfig {
            auth_tenant_id: Some(settings.tenant.clone()),
            auth_token: Some(settings.token.clone()),
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
        self.client
            .cache_get(&self.tenant, &self.namespace, cache, key)
            .await
    }

    pub async fn cache_put(&self, cache: &str, key: &str, value: Bytes) -> Result<()> {
        self.client
            .cache_put(&self.tenant, &self.namespace, cache, key, value, None)
            .await
    }

    /// Publishes one record and waits until it is acknowledged.
    pub async fn publish(&self, stream: &str, payload: Vec<u8>) -> Result<()> {
        self.client
            .publish(
                &self.tenant,
                &self.namespace,
                stream,
                payload,
                AckMode::PerMessage,
            )
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
        match self.publish_if(stream, vec![vec![0]], u64::MAX).await? {
            ConditionalWrite::Refused { tail } => Ok(tail),
            ConditionalWrite::Written { offset } => {
                bail!("probe write to {stream} landed at offset {offset}")
            }
        }
    }

    /// The records at offsets `from..end`, in order. Offsets that hold no
    /// event, such as a new leader's first record, are absent.
    pub async fn read_range(&self, stream: &str, from: u64, end: u64) -> Result<Vec<StreamRecord>> {
        let mut records = Vec::new();
        let mut next = from;
        while next < end {
            let page = self
                .client
                .read(
                    &self.tenant,
                    &self.namespace,
                    stream,
                    0,
                    next,
                    Some(end),
                    PAGE_RECORDS,
                )
                .await?;
            records.extend(page.records);
            if page.next_offset <= next {
                break;
            }
            next = page.next_offset;
        }
        Ok(records)
    }

    /// The last record of the stream, given its `tail`.
    pub async fn last_record(&self, stream: &str, tail: u64) -> Result<Option<StreamRecord>> {
        let mut window = 16;
        loop {
            let from = tail.saturating_sub(window);
            if let Some(last) = self.read_range(stream, from, tail).await?.pop() {
                return Ok(Some(last));
            }
            if from == 0 {
                return Ok(None);
            }
            window *= 8;
        }
    }

    /// Subscribes to the stream starting at input offset `from`.
    pub async fn subscribe_from(&self, stream: &str, from: u64) -> Result<Subscription> {
        self.client
            .subscribe_from(
                &self.tenant,
                &self.namespace,
                stream,
                Some(StartPosition::Offset(from)),
            )
            .await
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
}
