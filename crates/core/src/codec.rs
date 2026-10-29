use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

/// Why a record could not be read.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("record is {got} bytes, a header needs {need}")]
    Truncated { need: usize, got: usize },
    #[error("unknown record version {0}")]
    UnknownVersion(u8),
    #[error("unknown record kind {0}")]
    UnknownKind(u8),
    #[error("invalid JSON value: {0}")]
    Json(String),
    #[error("output name is not 1 to 64 bytes of a-z, 0-9 and '-'")]
    InvalidName,
    #[error("key flag is {0}, not 0 or 1")]
    InvalidKeyFlag(u8),
    #[error("rejection has index {0}, not 0")]
    RejectionIndex(u32),
    #[error("record has {0} bytes after its last field")]
    TrailingBytes(usize),
    #[error("unknown error class {0}")]
    UnknownClass(u8),
    #[error("unknown needs-operator reason {0}")]
    UnknownReason(u16),
    #[error("manifest names {0} chunks, more than {max}", max = crate::snapshot::MAX_CHUNKS)]
    TooManyChunks(u32),
}

/// The most bytes felixd writes to Felix at once: one step's output records,
/// or one snapshot record. Felix refuses a frame over 16 MiB by default, and
/// this leaves room for the framing.
pub const MAX_WRITE_BYTES: usize = 8 << 20;

/// Reads a record's fields front to back. A field past the end is
/// `Truncated`, with `need` counting the bytes up to the end of that field.
pub(crate) struct Reader<'a> {
    record: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(record: &'a [u8]) -> Self {
        Reader { record, at: 0 }
    }

    pub(crate) fn bytes(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let end = self.at.saturating_add(n);
        let Some(field) = self.record.get(self.at..end) else {
            return Err(DecodeError::Truncated {
                need: end,
                got: self.record.len(),
            });
        };
        self.at = end;
        Ok(field)
    }

    pub(crate) fn array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        let mut out = [0; N];
        out.copy_from_slice(self.bytes(N)?);
        Ok(out)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.array::<1>()?[0])
    }

    pub(crate) fn u32(&mut self) -> Result<u32, DecodeError> {
        self.array().map(u32::from_be_bytes)
    }

    pub(crate) fn u64(&mut self) -> Result<u64, DecodeError> {
        self.array().map(u64::from_be_bytes)
    }

    pub(crate) fn rest(&mut self) -> &'a [u8] {
        let rest = &self.record[self.at..];
        self.at = self.record.len();
        rest
    }

    pub(crate) fn remaining(&self) -> usize {
        self.record.len() - self.at
    }
}

/// A cache value: `{"format": N, "value": ...}`. The value sits under its own
/// key so that a build from before formats, which reads the value's fields at
/// the top, refuses it instead of reading what it can.
#[derive(Serialize)]
struct Envelope<'a, T> {
    format: u32,
    value: &'a T,
}

pub(crate) fn encode_json<T: Serialize>(format: u32, value: &T) -> Vec<u8> {
    serde_json::to_vec(&Envelope { format, value }).expect("a cache value is always valid JSON")
}

/// Reads a cache value of `format`. Another format is refused. A value
/// without a format is from a build before formats and reads as `T` itself.
pub(crate) fn decode_json<T: DeserializeOwned>(
    format: u32,
    bytes: &[u8],
) -> Result<T, DecodeError> {
    let json = |e: serde_json::Error| DecodeError::Json(e.to_string());
    let mut value: serde_json::Value = serde_json::from_slice(bytes).map_err(json)?;
    if let Some(object) = value.as_object_mut()
        && let Some(found) = object.remove("format")
    {
        let found = found.as_u64().unwrap_or(u64::MAX);
        if found != u64::from(format) {
            return Err(DecodeError::UnknownVersion(
                u8::try_from(found).unwrap_or(u8::MAX),
            ));
        }
        let inner = object.remove("value").unwrap_or(serde_json::Value::Null);
        return serde_json::from_value(inner).map_err(json);
    }
    serde_json::from_value(value).map_err(json)
}
