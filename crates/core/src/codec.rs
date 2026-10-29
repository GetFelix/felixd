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
