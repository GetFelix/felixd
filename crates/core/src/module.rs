//! How a module is identified and laid out in the `modules` cache.
//!
//! A module is named by the sha256 of its bytes. A module of at most
//! [`CHUNK_BYTES`] is stored whole under that key, as raw WebAssembly, which
//! starts with `\0asm`. A larger one is cut into chunks of [`CHUNK_BYTES`]
//! (the last may be shorter) stored under `<sha256>/<n>`, and the key holds a
//! JSON [`Manifest`] naming each chunk's hash. A reader tells the two apart by
//! the first bytes and checks every chunk and then the whole against the key,
//! so a store that returns the wrong bytes is caught, not run.
//!
//! A writer stores the chunks first and the manifest last, so a manifest never
//! names a chunk that was not written.

use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// The largest value stored under one `modules` key. Felix takes a frame of
/// 16 MiB, and a chunk of this size leaves room for the framing and for
/// several to be in flight.
pub const CHUNK_BYTES: usize = 4 << 20;

/// The largest module felixd stores: sixteen chunks. Admission's default
/// policy allows up to this.
pub const MAX_MODULE_BYTES: u64 = 64 << 20;

const FORMAT: u32 = 1;
const WASM_MAGIC: &[u8] = b"\0asm";

/// The key a module is stored under in the `modules` cache.
pub fn hash(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Why a module's length is not allowed, if it is not: the one message
/// admission and the uploader both give.
pub fn size_problem(bytes: u64, max: u64) -> Option<String> {
    (bytes == 0 || bytes > max).then(|| format!("the module is {bytes} bytes, not 1 to {max}"))
}

/// What a `modules` value found under a module's key is wrong with.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Corrupt {
    #[error("module {0} is neither WebAssembly nor a chunk manifest")]
    NotAModule(String),
    #[error("module {hash} has a manifest of format {format}, which this build does not read")]
    UnknownFormat { hash: String, format: u32 },
    #[error("module {hash} has a manifest that {why}")]
    Manifest { hash: String, why: &'static str },
    #[error("chunk {n} of module {hash} is {found} bytes, not {expected}")]
    ChunkLength {
        hash: String,
        n: usize,
        found: usize,
        expected: usize,
    },
    #[error("chunk {n} of module {hash} does not match its hash")]
    ChunkHash { hash: String, n: usize },
    #[error("module {0} does not match its hash")]
    Hash(String),
}

/// A module cut into chunks: the value for the module's own key and the
/// chunks it names. Store `chunks` before `head`.
#[derive(Debug, PartialEq, Eq)]
pub struct Layout<'a> {
    /// The value under the module's key: the module itself, or a manifest.
    pub head: Cow<'a, [u8]>,
    /// Each chunk's key and bytes. Empty for a module stored whole.
    pub chunks: Vec<(String, &'a [u8])>,
}

/// Lays `module` out for the cache.
pub fn layout(module: &[u8]) -> Layout<'_> {
    if module.len() <= CHUNK_BYTES {
        return Layout {
            head: Cow::Borrowed(module),
            chunks: Vec::new(),
        };
    }
    let key = hash(module);
    let parts: Vec<&[u8]> = module.chunks(CHUNK_BYTES).collect();
    let manifest = Manifest {
        format: FORMAT,
        bytes: module.len() as u64,
        chunks: parts.iter().map(|part| hash(part)).collect(),
    };
    Layout {
        head: Cow::Owned(serde_json::to_vec(&manifest).expect("a manifest encodes")),
        chunks: parts
            .into_iter()
            .enumerate()
            .map(|(n, part)| (chunk_key(&key, n), part))
            .collect(),
    }
}

/// The key of chunk `n` of module `key`.
pub fn chunk_key(key: &str, n: usize) -> String {
    format!("{key}/{n}")
}

/// The chunks of a module too large to store whole.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    format: u32,
    bytes: u64,
    /// The sha256 of each chunk, in order.
    chunks: Vec<String>,
}

impl Manifest {
    /// The module's length.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// How many chunks the module has.
    pub fn len(&self) -> usize {
        self.chunks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Checks chunk `n` of module `key` against its length and hash.
    pub fn check_chunk(&self, key: &str, n: usize, chunk: &[u8]) -> Result<(), Corrupt> {
        let expected = if n + 1 == self.chunks.len() {
            self.bytes as usize - n * CHUNK_BYTES
        } else {
            CHUNK_BYTES
        };
        if chunk.len() != expected {
            return Err(Corrupt::ChunkLength {
                hash: key.to_owned(),
                n,
                found: chunk.len(),
                expected,
            });
        }
        if self.chunks[n] != hash(chunk) {
            return Err(Corrupt::ChunkHash {
                hash: key.to_owned(),
                n,
            });
        }
        Ok(())
    }

    fn check(&self, key: &str) -> Result<(), Corrupt> {
        let bad = |why| Corrupt::Manifest {
            hash: key.to_owned(),
            why,
        };
        if self.format != FORMAT {
            return Err(Corrupt::UnknownFormat {
                hash: key.to_owned(),
                format: self.format,
            });
        }
        if self.bytes == 0 || self.bytes > MAX_MODULE_BYTES {
            return Err(bad("has a length outside the limit"));
        }
        if self.chunks.len() as u64 != self.bytes.div_ceil(CHUNK_BYTES as u64) {
            return Err(bad("names the wrong number of chunks for its length"));
        }
        Ok(())
    }
}

/// What a module's key holds.
#[derive(Debug, PartialEq, Eq)]
pub enum Head {
    /// The module itself, already checked against its hash.
    Whole,
    /// A manifest; the chunks are under [`chunk_key`].
    Chunked(Manifest),
}

/// Reads the value found under module `key`, whole or a manifest.
pub fn read_head(key: &str, value: &[u8]) -> Result<Head, Corrupt> {
    if value.starts_with(WASM_MAGIC) {
        verify(key, value)?;
        return Ok(Head::Whole);
    }
    let manifest: Manifest =
        serde_json::from_slice(value).map_err(|_| Corrupt::NotAModule(key.to_owned()))?;
    manifest.check(key)?;
    Ok(Head::Chunked(manifest))
}

/// Checks that `module` is the module named `key`.
pub fn verify(key: &str, module: &[u8]) -> Result<(), Corrupt> {
    if hash(module) == key {
        Ok(())
    } else {
        Err(Corrupt::Hash(key.to_owned()))
    }
}

#[cfg(test)]
mod tests;
