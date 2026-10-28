//! How a module is identified: by the sha256 of its bytes, in lowercase hex.

use sha2::{Digest, Sha256};

/// The key a module is stored under in the `modules` cache.
pub fn hash(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
mod tests;
