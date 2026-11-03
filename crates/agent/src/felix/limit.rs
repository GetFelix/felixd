//! The size of one request to Felix, checked before it is sent.
//!
//! Felix refuses a frame over its limit by dropping the connection, which the
//! client reports as a failed write and reconnects on. A caller cannot tell
//! that from a broker going away, and a retry sends the same frame again. So
//! felixd measures first and returns [`TooLarge`], which names what was too
//! big and the most Felix takes.

use std::fmt;

/// What the frame carries besides the payload and the names: the header and
/// the message fields. The real figure is under 200 bytes; the rest is slack.
const ENVELOPE: usize = 1024;

/// How the client puts a payload into the frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Wire {
    /// Base64 inside a JSON message: cache writes and conditional publishes.
    Json,
    /// Raw bytes with a length prefix: plain publishes.
    Binary,
}

impl Wire {
    /// Bytes one payload of `len` takes in the frame, with its separators.
    fn encoded(self, len: usize) -> usize {
        match self {
            Wire::Json => len.div_ceil(3) * 4 + 4,
            Wire::Binary => len + 8,
        }
    }

    /// The longest payload that fits in `room` bytes of frame.
    fn longest(self, room: usize) -> usize {
        match self {
            Wire::Json => room.saturating_sub(4) / 4 * 3,
            Wire::Binary => room.saturating_sub(8),
        }
    }
}

/// A request that would not fit in a frame of the size the client reads.
#[derive(Debug, PartialEq, Eq)]
pub struct TooLarge {
    what: String,
    bytes: usize,
    limit: usize,
    frame: usize,
}

impl TooLarge {
    /// Bytes the request carries.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// The most bytes of the same kind that fit in one request.
    pub fn limit(&self) -> usize {
        self.limit
    }
}

impl fmt::Display for TooLarge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} is {} bytes, over the {} bytes Felix takes in one request \
             (its {}-byte frame limit, less encoding)",
            self.what, self.bytes, self.limit, self.frame
        )
    }
}

impl std::error::Error for TooLarge {}

/// Refuses a request whose payloads, with the names around them, would not
/// fit in a frame of `frame` bytes. `names` is the combined length of the
/// tenant, namespace, cache or stream and key.
pub(crate) fn check(
    frame: usize,
    wire: Wire,
    names: usize,
    payloads: impl IntoIterator<Item = usize>,
    what: impl FnOnce() -> String,
) -> Result<(), TooLarge> {
    let room = frame.saturating_sub(ENVELOPE + names);
    let (mut count, mut bytes, mut wire_bytes) = (0usize, 0usize, 0usize);
    for len in payloads {
        count += 1;
        bytes += len;
        wire_bytes += wire.encoded(len);
    }
    if wire_bytes <= room {
        return Ok(());
    }
    // The other records of a batch take their separators out of the room.
    let spare = room.saturating_sub(count.saturating_sub(1) * wire.encoded(0));
    Err(TooLarge {
        what: what(),
        bytes,
        limit: wire.longest(spare),
        frame,
    })
}

#[cfg(test)]
mod tests;
