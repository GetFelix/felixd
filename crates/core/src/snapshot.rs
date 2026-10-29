//! The records on a workload's snapshot stream.
//!
//! A version 3 snapshot is chunk records followed by one manifest record.
//! Each is `version (1) | kind (1) | epoch (8) | body`, integers big endian,
//! where `epoch` is the writer's lease epoch. A chunk's body is `sha256 (32) |
//! bytes`. A manifest's is `input_offset (8) | output_tail (8) | count (4)`,
//! then for each chunk its `sha256 (32) | offset (8)`, the offset on the
//! snapshot stream of a chunk record holding it. Only a manifest is a
//! snapshot, and it may name chunks written for an earlier one.
//!
//! A version 2 record is a whole snapshot, `version | input_offset (8) |
//! output_tail (8) | epoch (8) | state`, and reads as one chunk. Version 1
//! has no epoch and reads as epoch 0.

use sha2::{Digest, Sha256};

use crate::codec::{DecodeError, Reader};

const VERSION: u8 = 3;
const WHOLE: u8 = 2;
const UNSTAMPED: u8 = 1;
const CHUNK: u8 = 0;
const MANIFEST: u8 = 1;

/// The most chunks a snapshot may have.
pub const MAX_CHUNKS: u32 = 65_536;
/// The largest chunk a snapshot may have.
pub const MAX_CHUNK_BYTES: usize = 4 << 20;

/// A chunk's sha256.
pub type Hash = [u8; 32];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotHeader {
    /// Offset of the last input the state includes.
    pub input_offset: u64,
    /// The output stream's next offset when the snapshot was taken: every
    /// output of inputs up to `input_offset` lies below it.
    pub output_tail: u64,
}

/// A decoded snapshot header and the epoch of the owner that wrote it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Taken {
    pub header: SnapshotHeader,
    pub epoch: u64,
}

/// Where a manifest finds one of its chunks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkRef {
    pub sha256: Hash,
    /// The offset on the snapshot stream of a chunk record holding it.
    pub offset: u64,
}

/// A snapshot made of chunks, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub taken: Taken,
    pub chunks: Vec<ChunkRef>,
}

/// A decoded record of a snapshot stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Record<'a> {
    /// A version 1 or 2 snapshot, its state in one piece.
    Whole(Taken, &'a [u8]),
    /// One chunk of a snapshot. Not a snapshot by itself.
    Chunk {
        epoch: u64,
        sha256: Hash,
        bytes: &'a [u8],
    },
    /// A snapshot.
    Manifest(Manifest),
}

impl Record<'_> {
    /// The epoch of the owner that wrote the record.
    pub fn epoch(&self) -> u64 {
        match self {
            Record::Whole(taken, _) => taken.epoch,
            Record::Chunk { epoch, .. } => *epoch,
            Record::Manifest(manifest) => manifest.taken.epoch,
        }
    }

    /// The snapshot's header, unless this is a chunk.
    pub fn taken(&self) -> Option<Taken> {
        match self {
            Record::Whole(taken, _) => Some(*taken),
            Record::Chunk { .. } => None,
            Record::Manifest(manifest) => Some(manifest.taken),
        }
    }
}

/// The sha256 of a chunk.
pub fn sha256(bytes: &[u8]) -> Hash {
    Sha256::digest(bytes).into()
}

/// Builds a version 2 snapshot record from its header, the writer's epoch and
/// the workload's whole state.
pub fn encode(header: SnapshotHeader, epoch: u64, state: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(25 + state.len());
    out.push(WHOLE);
    out.extend(header.input_offset.to_be_bytes());
    out.extend(header.output_tail.to_be_bytes());
    out.extend(epoch.to_be_bytes());
    out.extend(state);
    out
}

/// Builds a chunk record. `sha256` is the chunk's hash, from [`sha256`] or a
/// [`Plan`].
pub fn encode_chunk(epoch: u64, sha256: &Hash, bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(42 + bytes.len());
    out.extend([VERSION, CHUNK]);
    out.extend(epoch.to_be_bytes());
    out.extend(sha256);
    out.extend(bytes);
    out
}

/// Builds a manifest record.
pub fn encode_manifest(manifest: &Manifest) -> Vec<u8> {
    let mut out = Vec::with_capacity(30 + 40 * manifest.chunks.len());
    out.extend([VERSION, MANIFEST]);
    out.extend(manifest.taken.epoch.to_be_bytes());
    out.extend(manifest.taken.header.input_offset.to_be_bytes());
    out.extend(manifest.taken.header.output_tail.to_be_bytes());
    out.extend((manifest.chunks.len() as u32).to_be_bytes());
    for chunk in &manifest.chunks {
        out.extend(chunk.sha256);
        out.extend(chunk.offset.to_be_bytes());
    }
    out
}

/// Reads a snapshot stream record.
pub fn decode(record: &[u8]) -> Result<Record<'_>, DecodeError> {
    let mut r = Reader::new(record);
    match r.u8()? {
        UNSTAMPED => {
            let header = read_header(&mut r)?;
            Ok(Record::Whole(Taken { header, epoch: 0 }, r.rest()))
        }
        WHOLE => {
            let header = read_header(&mut r)?;
            let epoch = r.u64()?;
            Ok(Record::Whole(Taken { header, epoch }, r.rest()))
        }
        VERSION => {
            let kind = r.u8()?;
            if kind != CHUNK && kind != MANIFEST {
                return Err(DecodeError::UnknownKind(kind));
            }
            let epoch = r.u64()?;
            if kind == CHUNK {
                return Ok(Record::Chunk {
                    epoch,
                    sha256: r.array()?,
                    bytes: r.rest(),
                });
            }
            let header = read_header(&mut r)?;
            let count = r.u32()?;
            if count > MAX_CHUNKS {
                return Err(DecodeError::TooManyChunks(count));
            }
            let mut chunks = Vec::with_capacity((count as usize).min(r.remaining() / 40));
            for _ in 0..count {
                chunks.push(ChunkRef {
                    sha256: r.array()?,
                    offset: r.u64()?,
                });
            }
            if r.remaining() > 0 {
                return Err(DecodeError::TrailingBytes(r.remaining()));
            }
            Ok(Record::Manifest(Manifest {
                taken: Taken { header, epoch },
                chunks,
            }))
        }
        other => Err(DecodeError::UnknownVersion(other)),
    }
}

fn read_header(r: &mut Reader<'_>) -> Result<SnapshotHeader, DecodeError> {
    Ok(SnapshotHeader {
        input_offset: r.u64()?,
        output_tail: r.u64()?,
    })
}

/// Why a chunk record does not hold the chunk a manifest names.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum ChunkError {
    #[error("record at offset {offset} does not decode: {error}")]
    Undecodable { offset: u64, error: DecodeError },
    #[error("record at offset {0} is not a chunk")]
    NotAChunk(u64),
    #[error("chunk at offset {0} does not have the hash its manifest names")]
    HashMismatch(u64),
}

/// The bytes of the chunk `chunk` names, from `record`, the record at its
/// offset. The hash is checked against the bytes, not only the record's
/// stored hash.
pub fn read_chunk<'a>(chunk: &ChunkRef, record: &'a [u8]) -> Result<&'a [u8], ChunkError> {
    let offset = chunk.offset;
    match decode(record) {
        Ok(Record::Chunk {
            sha256: stored,
            bytes,
            ..
        }) => {
            if stored == chunk.sha256 && sha256(bytes) == chunk.sha256 {
                Ok(bytes)
            } else {
                Err(ChunkError::HashMismatch(offset))
            }
        }
        Ok(_) => Err(ChunkError::NotAChunk(offset)),
        Err(error) => Err(ChunkError::Undecodable { offset, error }),
    }
}

/// What to write for a new snapshot, given the previous manifest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Plan {
    /// Each chunk's hash, in order.
    pub hashes: Vec<Hash>,
    /// The indexes of the chunks to write as chunk records, in order. A chunk
    /// the previous manifest holds, or an earlier chunk of this snapshot
    /// already holds, is not written again.
    pub write: Vec<usize>,
    sources: Vec<Source>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Source {
    /// A chunk record already on the stream.
    At(u64),
    /// The `n`th record of [`Plan::write`].
    Written(usize),
}

/// Plans a snapshot of `chunks`. A chunk whose hash `previous` names is
/// referenced at the offset `previous` gives it.
pub fn plan(previous: Option<&Manifest>, chunks: &[&[u8]]) -> Plan {
    let hashes: Vec<Hash> = chunks.iter().map(|c| sha256(c)).collect();
    let mut write = Vec::new();
    let mut sources = Vec::with_capacity(chunks.len());
    for (i, hash) in hashes.iter().enumerate() {
        let held = previous.and_then(|m| m.chunks.iter().find(|c| c.sha256 == *hash));
        let source = if let Some(held) = held {
            Source::At(held.offset)
        } else if let Some(n) = write.iter().position(|&w| hashes[w] == *hash) {
            Source::Written(n)
        } else {
            write.push(i);
            Source::Written(write.len() - 1)
        };
        sources.push(source);
    }
    Plan {
        hashes,
        write,
        sources,
    }
}

impl Plan {
    /// The manifest, once the chunk records in [`Plan::write`] are on the
    /// stream at `written_at`, in the same order. `None` if `written_at` does
    /// not have one offset per record written.
    pub fn manifest(&self, taken: Taken, written_at: &[u64]) -> Option<Manifest> {
        if written_at.len() != self.write.len() {
            return None;
        }
        let chunks = self
            .hashes
            .iter()
            .zip(&self.sources)
            .map(|(&sha256, source)| ChunkRef {
                sha256,
                offset: match *source {
                    Source::At(offset) => offset,
                    Source::Written(n) => written_at[n],
                },
            })
            .collect();
        Some(Manifest { taken, chunks })
    }
}

/// Whether the input appended at `micros` starts a new interval of
/// `interval_ms`, counted from the Unix epoch, after the input before it,
/// appended at `previous`. Both times are the broker's append stamps on the
/// input stream, never a node's clock, so every node snapshots at the same
/// inputs. The first input has no previous one and starts nothing.
pub fn starts_interval(interval_ms: u64, previous: Option<u64>, micros: u64) -> bool {
    let Some(previous) = previous else {
        return false;
    };
    let width = interval_ms.saturating_mul(1000).max(1);
    micros / width > previous / width
}

#[cfg(test)]
mod tests;
