use thiserror::Error;

/// Why a record could not be read.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("record is {got} bytes, a header needs {need}")]
    Truncated { need: usize, got: usize },
    #[error("unknown record version {0}")]
    UnknownVersion(u8),
    #[error("invalid JSON value: {0}")]
    Json(String),
}

/// The most bytes felixd writes to Felix at once: one step's output records,
/// or one snapshot record. Felix refuses a frame over 16 MiB by default, and
/// this leaves room for the framing.
pub const MAX_WRITE_BYTES: usize = 8 << 20;

/// Version byte at the start of every binary record. A format change takes a
/// new number, so a reader refuses what it does not understand instead of
/// misreading it.
pub(crate) const VERSION: u8 = 1;

/// Splits `bytes` into a `N`-byte header and the rest, after checking the version.
pub(crate) fn split_header<const N: usize>(bytes: &[u8]) -> Result<(&[u8; N], &[u8]), DecodeError> {
    let need = N + 1;
    let Some((&version, rest)) = bytes.split_first() else {
        return Err(DecodeError::Truncated { need, got: 0 });
    };
    if version != VERSION {
        return Err(DecodeError::UnknownVersion(version));
    }
    let Some((header, body)) = rest.split_first_chunk::<N>() else {
        return Err(DecodeError::Truncated {
            need,
            got: bytes.len(),
        });
    };
    Ok((header, body))
}
