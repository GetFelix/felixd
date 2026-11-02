//! The `modules` cache: storing a module, whole or in chunks, and reading it
//! back. Every reader of module bytes comes through [`fetch`], so a chunked
//! module is joined and checked in one place. The layout is described in
//! [`felixd_core::module`].

use anyhow::{Result, bail};
use bytes::Bytes;
use felixd_core::module::{self, Corrupt, Head};
use felixd_core::names;

use crate::felix::Felix;

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
/// repairs a chunk that was lost.
pub async fn put(felix: &Felix, module: &[u8]) -> Result<String> {
    if let Some(why) = module::size_problem(module.len() as u64, module::MAX_MODULE_BYTES) {
        bail!(why);
    }
    let key = module::hash(module);
    let parts = module::split(module);
    // A small value that is not WebAssembly would be stored but never read.
    module::read_head(&key, &parts.head)?;
    for (chunk_key, chunk) in &parts.chunks {
        felix
            .cache_put(names::MODULES, chunk_key, Bytes::copy_from_slice(chunk))
            .await?;
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
    let mut joined = Vec::with_capacity(manifest.bytes() as usize);
    for n in 0..manifest.len() {
        let Some(chunk) = felix
            .cache_get(names::MODULES, &module::chunk_key(key, n))
            .await?
        else {
            return Ok(Fetched::Missing(format!("chunk {n} of module {key}")));
        };
        if let Err(corrupt) = manifest.check_chunk(key, n, &chunk) {
            return Ok(Fetched::Corrupt(corrupt));
        }
        joined.extend_from_slice(&chunk);
    }
    match module::verify(key, &joined) {
        Ok(()) => Ok(Fetched::Found(joined.into())),
        Err(corrupt) => Ok(Fetched::Corrupt(corrupt)),
    }
}
