//! Context-allocated UTF-8 string: the `String`-shaped companion to
//! [`PgVec`](crate::PgVec). allocator-api2 has no `String` type, so this is
//! the one collection we wrap ourselves — a thin layer over `PgVec<'mcx, u8>`
//! whose only invariant is "the bytes are valid UTF-8".

use core::fmt;

use crate::{Mcx, PgVec};
use types_error::PgResult;

pub struct PgString<'mcx> {
    /// Invariant: always valid UTF-8 (only ever extended with `&str` bytes or
    /// truncated on `char` boundaries).
    bytes: PgVec<'mcx, u8>,
}

impl<'mcx> PgString<'mcx> {
    pub fn new_in(mcx: Mcx<'mcx>) -> Self {
        PgString { bytes: PgVec::new_in(mcx) }
    }

    pub fn from_str_in(s: &str, mcx: Mcx<'mcx>) -> PgResult<Self> {
        let mut out = Self::new_in(mcx);
        out.try_push_str(s)?;
        Ok(out)
    }

    /// C `pchomp`: copy of `s` with all trailing newlines removed.
    pub fn chomp_in(s: &str, mcx: Mcx<'mcx>) -> PgResult<Self> {
        Self::from_str_in(s.trim_end_matches('\n'), mcx)
    }

    pub fn try_push_str(&mut self, s: &str) -> PgResult<()> {
        let mcx = *self.bytes.allocator();
        self.bytes
            .try_reserve(s.len())
            .map_err(|_| mcx.oom(s.len()))?;
        self.bytes.extend_from_slice(s.as_bytes());
        Ok(())
    }

    pub fn try_push(&mut self, c: char) -> PgResult<()> {
        self.try_push_str(c.encode_utf8(&mut [0u8; 4]))
    }

    pub fn as_str(&self) -> &str {
        // Invariant: `bytes` only ever holds whole `&str` contents.
        unsafe { core::str::from_utf8_unchecked(&self.bytes) }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The underlying byte vector (still context-allocated).
    pub fn into_bytes(self) -> PgVec<'mcx, u8> {
        self.bytes
    }

    /// Bytes that are valid UTF-8, reusing the allocation (C idiom:
    /// `text`/`cstring` payloads validated then treated as strings).
    pub fn from_utf8(bytes: PgVec<'mcx, u8>) -> Result<Self, core::str::Utf8Error> {
        core::str::from_utf8(&bytes)?;
        Ok(PgString { bytes })
    }

    /// Copy into another context (C idiom: `MemoryContextStrdup`).
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<PgString<'b>> {
        PgString::from_str_in(self.as_str(), mcx)
    }

    pub fn allocator(&self) -> Mcx<'mcx> {
        *self.bytes.allocator()
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Bytes currently reserved from the context (== what accounting charges).
    pub fn capacity_bytes(&self) -> usize {
        self.bytes.capacity()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn clear(&mut self) {
        self.bytes.clear();
    }

    /// Truncate to `new_len` bytes; panics off a char boundary (as `String`).
    pub fn truncate(&mut self, new_len: usize) {
        if new_len < self.len() {
            assert!(self.as_str().is_char_boundary(new_len), "truncate off char boundary");
            self.bytes.truncate(new_len);
        }
    }
}

impl fmt::Debug for PgString<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

impl fmt::Display for PgString<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Write for PgString<'_> {
    /// Infallible-allocation path for `write!`; prefer `try_push_str` where
    /// the C handled OOM softly.
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.try_push_str(s).map_err(|_| fmt::Error)
    }
}

impl core::ops::Deref for PgString<'_> {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq<str> for PgString<'_> {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for PgString<'_> {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq for PgString<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for PgString<'_> {}

impl PartialOrd for PgString<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PgString<'_> {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl core::hash::Hash for PgString<'_> {
    /// Hashes like `str`, so a `PgString` key probes equal to its `&str`
    /// (with hashbrown's `Equivalent` borrowed-key lookups).
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.as_str().hash(state)
    }
}

impl AsRef<str> for PgString<'_> {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl core::borrow::Borrow<str> for PgString<'_> {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}
