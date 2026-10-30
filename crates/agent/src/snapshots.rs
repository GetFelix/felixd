//! Reading snapshots back from a workload's snapshot stream.

use std::collections::BTreeMap;

use anyhow::{Result, anyhow};
use bytes::Bytes;
use felixd_core::snapshot::{self, Manifest, Record, Taken};
use felixd_core::trim::Kept;

use crate::felix::Felix;

/// A snapshot as it is on the stream.
#[derive(Clone, Debug)]
pub(crate) enum Snapshot {
    /// Version 1 or 2: the state in one record.
    Whole(Taken, Bytes),
    /// Version 3: chunk records named by a manifest.
    Chunked(Manifest),
}

impl Snapshot {
    pub(crate) fn taken(&self) -> Taken {
        match self {
            Snapshot::Whole(taken, _) => *taken,
            Snapshot::Chunked(manifest) => manifest.taken,
        }
    }

    /// The snapshot `record` holds, `None` for a chunk record.
    pub(crate) fn decode(record: &Bytes) -> Result<Option<Snapshot>, felixd_core::DecodeError> {
        Ok(match snapshot::decode(record)? {
            Record::Whole(taken, state) => Some(Snapshot::Whole(taken, record.slice_ref(state))),
            Record::Chunk { .. } => None,
            Record::Manifest(manifest) => Some(Snapshot::Chunked(manifest)),
        })
    }

    /// The manifest, for a chunked snapshot.
    pub(crate) fn manifest(&self) -> Option<&Manifest> {
        match self {
            Snapshot::Whole(..) => None,
            Snapshot::Chunked(manifest) => Some(manifest),
        }
    }
}

/// The end of a snapshot stream: the epoch of its last record of any kind,
/// and the latest snapshot, passing over chunk records.
pub(crate) struct Latest {
    pub(crate) last_epoch: Option<u64>,
    pub(crate) snapshot: Option<Snapshot>,
}

/// Reads back from `tail` for the latest snapshot. A record that does not
/// decode is an error, with its offset.
pub(crate) async fn latest(felix: &Felix, stream: &str, tail: u64) -> Result<Latest, ReadError> {
    let mut latest = Latest {
        last_epoch: None,
        snapshot: None,
    };
    let mut end = tail;
    let mut window = 16;
    while end > 0 {
        let from = end.saturating_sub(window);
        let records = felix
            .read_back(stream, from, end)
            .await
            .map_err(ReadError::Felix)?;
        let (records, ended) = records;
        for record in records.iter().rev() {
            let at = |e: &dyn std::fmt::Display| {
                ReadError::Bad(format!("snapshot at {stream}:{}: {e}", record.offset))
            };
            let decoded = snapshot::decode(&record.payload).map_err(|e| at(&e))?;
            latest.last_epoch.get_or_insert(decoded.epoch());
            if let Some(found) = Snapshot::decode(&record.payload).map_err(|e| at(&e))? {
                latest.snapshot = Some(found);
                return Ok(latest);
            }
        }
        if ended {
            break;
        }
        end = from;
        window = window.saturating_mul(8);
    }
    Ok(latest)
}

/// The newest `keep` snapshots on the stream, newest first, as
/// [`felixd_core::trim::plan`] takes them. Fewer when the stream holds fewer.
pub(crate) async fn newest(
    felix: &Felix,
    stream: &str,
    tail: u64,
    keep: usize,
) -> Result<Vec<Kept>, ReadError> {
    let mut found = Vec::new();
    let mut end = tail;
    let mut window = 16;
    while end > 0 && found.len() < keep {
        let from = end.saturating_sub(window);
        let records = felix
            .read_back(stream, from, end)
            .await
            .map_err(ReadError::Felix)?;
        let (records, ended) = records;
        for record in records.iter().rev() {
            let snapshot = Snapshot::decode(&record.payload).map_err(|e| {
                ReadError::Bad(format!("snapshot at {stream}:{}: {e}", record.offset))
            })?;
            if let Some(snapshot) = snapshot {
                let taken = snapshot.taken();
                found.push(Kept {
                    offset: record.offset,
                    epoch: taken.epoch,
                    step: taken.header.input_offset,
                    chunks: snapshot
                        .manifest()
                        .map(|m| m.chunks.iter().map(|c| c.offset).collect())
                        .unwrap_or_default(),
                });
                if found.len() == keep {
                    break;
                }
            }
        }
        if ended {
            break;
        }
        end = from;
        window = window.saturating_mul(8);
    }
    Ok(found)
}

/// The state's chunks, in order, each checked against the hash its manifest
/// names. A version 2 snapshot is one chunk.
pub(crate) async fn chunks(
    felix: &Felix,
    stream: &str,
    snapshot: &Snapshot,
) -> Result<Vec<Bytes>, ReadError> {
    let manifest = match snapshot {
        Snapshot::Whole(_, state) => return Ok(vec![state.clone()]),
        Snapshot::Chunked(manifest) => manifest,
    };
    // Chunks written together sit next to each other, so read runs of
    // offsets rather than one record at a time.
    let mut wanted: Vec<u64> = manifest.chunks.iter().map(|c| c.offset).collect();
    wanted.sort_unstable();
    wanted.dedup();
    let mut records: BTreeMap<u64, Bytes> = BTreeMap::new();
    let mut run = 0;
    while run < wanted.len() {
        let mut end = run + 1;
        while end < wanted.len() && wanted[end] == wanted[end - 1] + 1 {
            end += 1;
        }
        let read = felix
            .read_range(stream, wanted[run], wanted[end - 1] + 1)
            .await
            .map_err(ReadError::Felix)?;
        records.extend(read.into_iter().map(|r| (r.offset, r.payload)));
        run = end;
    }
    let mut chunks = Vec::with_capacity(manifest.chunks.len());
    for chunk in &manifest.chunks {
        let record = records.get(&chunk.offset).ok_or_else(|| {
            ReadError::Bad(format!(
                "the snapshot names a chunk at {stream}:{}, which holds no record",
                chunk.offset
            ))
        })?;
        let bytes = snapshot::read_chunk(chunk, record)
            .map_err(|e| ReadError::Bad(format!("snapshot on {stream}: {e}")))?;
        chunks.push(record.slice_ref(bytes));
    }
    Ok(chunks)
}

/// Why a snapshot could not be read.
pub(crate) enum ReadError {
    /// Felix failed; trying again may work.
    Felix(anyhow::Error),
    /// The stream holds something no node can restore from.
    Bad(String),
}

impl ReadError {
    /// As an error for a caller that does not tell the two apart.
    pub(crate) fn into_anyhow(self) -> anyhow::Error {
        match self {
            ReadError::Felix(e) => e,
            ReadError::Bad(message) => anyhow!(message),
        }
    }
}
