//! The `modules` cache: storing a module, whole or in chunks, and reading it
//! back. Every reader of module bytes comes through [`fetch`], so a chunked
//! module is joined and checked in one place. The layout is described in
//! [`felixd_core::module`].

use anyhow::{Result, bail};
use bytes::Bytes;
use felixd_core::module::{self, CHUNK_BYTES, Corrupt, Head, Manifest};
use felixd_core::names;
use tokio::task::JoinSet;

use crate::felix::Felix;

/// How many chunks are in flight at once.
const PARALLEL: usize = 4;

/// What [`fetch`] found under a module's key.
#[derive(Debug)]
pub enum Fetched {
    /// The module, checked against its key.
    Found(Bytes),
    /// The module, or one of its chunks, is not in the cache. This names it.
    Missing(String),
    /// What is stored does not make up the module.
    Corrupt(Corrupt),
}

/// Stores `module` in the `modules` cache and returns its key. Chunks go
/// first and the manifest last, so a key never names a chunk that was not
/// written. Putting the same module again rewrites the same bytes, which
/// repairs a chunk that was lost. Chunks are never deleted, so a put that
/// fails part way leaves chunks no manifest names.
pub async fn put(felix: &Felix, module: &[u8]) -> Result<String> {
    if let Some(why) = module::size_problem(module.len() as u64, module::MAX_MODULE_BYTES) {
        bail!(why);
    }
    let key = module::hash(module);
    let parts = module::split(module);
    // A small value that is not WebAssembly would be stored but never read.
    module::read_head(&key, &parts.head)?;
    let mut writes = JoinSet::new();
    let mut chunks = parts.chunks.iter();
    loop {
        while writes.len() < PARALLEL
            && let Some((chunk_key, chunk)) = chunks.next()
        {
            let felix = felix.clone();
            let (chunk_key, chunk) = (chunk_key.clone(), Bytes::copy_from_slice(chunk));
            writes.spawn(async move { felix.cache_put(names::MODULES, &chunk_key, chunk).await });
        }
        match writes.join_next().await {
            Some(done) => done??,
            None => break,
        }
    }
    felix
        .cache_put(names::MODULES, &key, Bytes::copy_from_slice(&parts.head))
        .await?;
    Ok(key)
}

/// Reads module `key`. The error is a failed read of the cache; a module that
/// is absent or wrong is a [`Fetched`] outcome.
pub async fn fetch(felix: &Felix, key: &str) -> Result<Fetched> {
    let Some(value) = felix.cache_get(names::MODULES, key).await? else {
        return Ok(Fetched::Missing(format!("module {key}")));
    };
    let manifest = match module::read_head(key, &value) {
        Ok(Head::Whole) => return Ok(Fetched::Found(value)),
        Ok(Head::Chunked(manifest)) => manifest,
        Err(corrupt) => return Ok(Fetched::Corrupt(corrupt)),
    };
    let joined = match fetch_chunks(felix, key, &manifest).await? {
        Ok(joined) => joined,
        Err(other) => return Ok(other),
    };
    match module::verify(key, &joined) {
        Ok(()) => Ok(Fetched::Found(joined.into())),
        Err(corrupt) => Ok(Fetched::Corrupt(corrupt)),
    }
}

/// The chunks of module `key`, checked one by one and joined. A few are read
/// at a time; each lands at its own offset, so the order is the manifest's.
/// The inner error is the first missing or wrong chunk to come back.
async fn fetch_chunks(
    felix: &Felix,
    key: &str,
    manifest: &Manifest,
) -> Result<std::result::Result<Vec<u8>, Fetched>> {
    let mut joined = vec![0u8; manifest.bytes() as usize];
    let mut reads = JoinSet::new();
    let mut next = 0;
    loop {
        while reads.len() < PARALLEL && next < manifest.len() {
            let felix = felix.clone();
            let n = next;
            let chunk_key = module::chunk_key(key, n);
            reads.spawn(async move {
                let chunk = felix.cache_get(names::MODULES, &chunk_key).await;
                (n, chunk)
            });
            next += 1;
        }
        let Some(done) = reads.join_next().await else {
            return Ok(Ok(joined));
        };
        let (n, chunk) = done?;
        let Some(chunk) = chunk? else {
            return Ok(Err(Fetched::Missing(format!("chunk {n} of module {key}"))));
        };
        if let Err(corrupt) = manifest.check_chunk(key, n, &chunk) {
            return Ok(Err(Fetched::Corrupt(corrupt)));
        }
        let at = n * CHUNK_BYTES;
        joined[at..at + chunk.len()].copy_from_slice(&chunk);
    }
}
