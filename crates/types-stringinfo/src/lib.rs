//! `StringInfoData` (`lib/stringinfo.h`): the expansible buffer used to
//! assemble outgoing frontend/backend messages and to parse incoming ones.
//!
//! ```text
//! struct StringInfoData { char *data; int len; int maxlen; int cursor; };
//! ```
//!
//! `data`/`len`/`maxlen` collapse into a context-allocated `PgVec<u8>`
//! (`len` = `data.len()`, `maxlen` = capacity). `cursor` is kept as a field
//! because callers really do use it for two distinct things, exactly as in C:
//!
//! * **receive side** — the `pq_getmsg*` readers track how far the message has
//!   been consumed;
//! * **send side** — `pq_beginmessage` stashes the one-byte message type in
//!   `cursor` for `pq_endmessage` to read back, on the documented contract
//!   that the `pq_send*` routines never touch it.
//!
//! One deliberate representational difference: C's StringInfo guarantees a
//! trailing `'\0'` sentinel after `data[len-1]`; no sentinel byte is stored
//! here. Readers that relied on it (`pq_getmsgstring` and friends) scan for an
//! embedded NUL and apply C's own `cursor + slen >= len` bound instead, which
//! is observably identical.
//!
//! Buffer *logic* (`initStringInfo`'s 1024-byte prealloc, `enlargeStringInfo`'s
//! 1GB cap, the append family) belongs to the owning units
//! (`common/stringinfo.c`, `libpq/pqformat.c`); this crate holds only the type.

#![no_std]
#![forbid(unsafe_code)]

use mcx::{Mcx, PgVec};

/// The buffer. Fields are public, mirroring C's plain struct: the owning units
/// (`stringinfo.c`, `pqformat.c`) manipulate them directly.
pub struct StringInfo<'mcx> {
    /// `data[0 .. len]` (no trailing-NUL sentinel; see module doc).
    pub data: PgVec<'mcx, u8>,
    /// Read offset for the `pq_getmsg*` family, or the stashed message-type
    /// byte between `pq_beginmessage` and `pq_endmessage`.
    pub cursor: usize,
}

impl<'mcx> StringInfo<'mcx> {
    /// An empty buffer charged to `mcx` (no allocation until first growth).
    pub fn new_in(mcx: Mcx<'mcx>) -> Self {
        StringInfo { data: PgVec::new_in(mcx), cursor: 0 }
    }

    /// Wrap an already-filled buffer as a fresh read cursor (`cursor = 0`),
    /// e.g. a message just read off the wire by `pq_getmessage`.
    pub fn from_vec(data: PgVec<'mcx, u8>) -> Self {
        StringInfo { data, cursor: 0 }
    }

    /// `msg->len`.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// `data[0 .. len]`.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// The owning context handle.
    pub fn allocator(&self) -> Mcx<'mcx> {
        *self.data.allocator()
    }

    /// `resetStringInfo`: clear the contents (keeping the allocation) and
    /// reset the cursor.
    pub fn reset(&mut self) {
        self.data.clear();
        self.cursor = 0;
    }

    /// Recover the owned byte buffer.
    pub fn into_vec(self) -> PgVec<'mcx, u8> {
        self.data
    }
}
