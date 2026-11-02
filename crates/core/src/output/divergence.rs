//! The divergence record: what an owner found when a replay of its own
//! inputs did not produce what the output stream holds.

use sha2::{Digest, Sha256};

use super::{
    DIVERGENCE, OutputHeader, Record, VERSION, decode, encode_addressed, encode_rejection,
};
use crate::codec::{DecodeError, Reader};

/// What a build's determinism rests on. A replay that differs under another
/// ABI than the one that wrote the stream is a different finding from one
/// that differs under the same.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Abi<'a> {
    /// The workload contract version the build runs, such as `0.2.0`.
    pub contract: &'a [u8],
    /// The wasmtime version the build links.
    pub wasmtime: &'a [u8],
    /// The felixd version of the build.
    pub felixd: &'a [u8],
}

/// Where a replay and the output stream disagreed, as a divergence record
/// holds it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Diverged<'a> {
    /// The `sys.specs` offset of the spec record the owner ran.
    pub spec: u64,
    /// The output the two disagree on.
    pub header: OutputHeader,
    /// The input offset of the snapshot the replay restored, or `None` when
    /// it started from the initial state.
    pub snapshot: Option<u64>,
    /// The node that found it.
    pub node: &'a [u8],
    pub abi: Abi<'a>,
    /// The digest of what the output stream holds there ([`compared_digest`]),
    /// or `None` when it holds nothing that compares, such as a claim or a
    /// record that does not decode.
    pub expected: Option<&'a [u8; 32]>,
    /// The digest of what this node's replay produced there.
    pub actual: Option<&'a [u8; 32]>,
    /// What was found, for people.
    pub detail: &'a [u8],
}

/// The sha256 of the bytes two outputs or two rejections are compared by:
/// the record's header and content with the epoch set to 0, so that the
/// same output written by two owners has one digest. `None` for a record
/// that is neither, or does not decode.
pub fn compared_digest(record: &[u8]) -> Option<[u8; 32]> {
    let canonical = match decode(record).ok()? {
        Record::Output(written, addressed) => {
            encode_addressed(written.header, 0, addressed).ok()?
        }
        Record::Rejection(written, rejected) => {
            encode_rejection(written.header.input_offset, 0, rejected)
        }
        _ => return None,
    };
    Some(Sha256::digest(canonical).into())
}

/// The longest a text field of the record is.
const MAX_FIELD: usize = u8::MAX as usize;

/// Builds the record an owner at `epoch` appends when its replay and the
/// output stream disagree. A node id or version past 255 bytes is cut there,
/// at a character boundary.
pub fn encode_divergence(epoch: u64, found: &Diverged<'_>) -> Vec<u8> {
    let mut out = Vec::with_capacity(160 + found.detail.len());
    out.extend([VERSION, DIVERGENCE]);
    out.extend(epoch.to_be_bytes());
    out.extend(found.spec.to_be_bytes());
    out.extend(found.header.input_offset.to_be_bytes());
    out.extend(found.header.index.to_be_bytes());
    out.push(u8::from(found.snapshot.is_some()));
    out.extend(found.snapshot.unwrap_or(0).to_be_bytes());
    for digest in [found.expected, found.actual] {
        match digest {
            Some(digest) => {
                out.push(32);
                out.extend(digest);
            }
            None => out.push(0),
        }
    }
    for text in [
        found.node,
        found.abi.contract,
        found.abi.wasmtime,
        found.abi.felixd,
    ] {
        let text = clip(text);
        out.push(text.len() as u8);
        out.extend(text);
    }
    out.extend(found.detail);
    out
}

/// `text` cut to what a length byte holds, not inside a UTF-8 character.
fn clip(text: &[u8]) -> &[u8] {
    let mut end = text.len().min(MAX_FIELD);
    while end > 0 && end < text.len() && text[end] & 0xC0 == 0x80 {
        end -= 1;
    }
    &text[..end]
}

/// Reads the fields after a divergence record's epoch.
pub(super) fn decode_body<'a>(r: &mut Reader<'a>) -> Result<Diverged<'a>, DecodeError> {
    let spec = r.u64()?;
    let header = OutputHeader {
        input_offset: r.u64()?,
        index: r.u32()?,
    };
    let has_snapshot = r.u8()?;
    let snapshot = r.u64()?;
    let snapshot = match has_snapshot {
        0 => None,
        1 => Some(snapshot),
        other => return Err(DecodeError::InvalidSnapshotFlag(other)),
    };
    let expected = digest(r)?;
    let actual = digest(r)?;
    let node = text(r)?;
    let abi = Abi {
        contract: text(r)?,
        wasmtime: text(r)?,
        felixd: text(r)?,
    };
    Ok(Diverged {
        spec,
        header,
        snapshot,
        node,
        abi,
        expected,
        actual,
        detail: r.rest(),
    })
}

fn digest<'a>(r: &mut Reader<'a>) -> Result<Option<&'a [u8; 32]>, DecodeError> {
    match r.u8()? {
        0 => Ok(None),
        32 => Ok(Some(
            r.bytes(32)?.try_into().expect("32 bytes were just read"),
        )),
        other => Err(DecodeError::InvalidDigest(other)),
    }
}

fn text<'a>(r: &mut Reader<'a>) -> Result<&'a [u8], DecodeError> {
    let len = r.u8()?;
    r.bytes(len.into())
}
