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
