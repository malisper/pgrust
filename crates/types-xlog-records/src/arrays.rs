//! Typed views of the variable-length arrays WAL record bodies carry
//! (`FLEXIBLE_ARRAY_MEMBER` tails). The bytes are potentially unaligned, so
//! elements are decoded on access instead of reborrowed as `&[T]`.

use crate::bytes::{u16_at, u32_at};
use types_core::{Oid, OffsetNumber};

/// `sizeof(OffsetNumber)`.
pub const SIZEOF_OFFSET_NUMBER: usize = 2;
/// `sizeof(Oid)`.
pub const SIZEOF_OID: usize = 4;

/// An `OffsetNumber[]` borrowed from a record body.
#[derive(Clone, Copy, Debug)]
pub struct OffsetNumbers<'a> {
    bytes: &'a [u8],
}

impl<'a> OffsetNumbers<'a> {
    pub const fn from_bytes(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// Element `i`; panics past the end of the bytes (C reads garbage there).
    pub fn get(&self, i: usize) -> OffsetNumber {
        u16_at(self.bytes, i * SIZEOF_OFFSET_NUMBER)
    }

    /// The raw bytes of the first `count` elements (for generic array
    /// walkers); panics if the record is shorter.
    pub fn bytes_of(&self, count: usize) -> &'a [u8] {
        &self.bytes[..count * SIZEOF_OFFSET_NUMBER]
    }

    /// The view starting after the first `count` elements.
    pub fn skip(&self, count: usize) -> Self {
        Self {
            bytes: &self.bytes[count * SIZEOF_OFFSET_NUMBER..],
        }
    }
}

/// An `OffsetNumber[, 2]` pair array (e.g. `xl_heap_prune` redirections:
/// `[from, to]` pairs).
#[derive(Clone, Copy, Debug)]
pub struct OffsetNumberPairs<'a> {
    bytes: &'a [u8],
}

impl<'a> OffsetNumberPairs<'a> {
    pub const fn from_bytes(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// Pair `i` as `(from, to)`; panics past the end of the bytes.
    pub fn get(&self, i: usize) -> (OffsetNumber, OffsetNumber) {
        let off = i * 2 * SIZEOF_OFFSET_NUMBER;
        (u16_at(self.bytes, off), u16_at(self.bytes, off + SIZEOF_OFFSET_NUMBER))
    }

    /// The raw bytes of the first `count` pairs.
    pub fn bytes_of(&self, count: usize) -> &'a [u8] {
        &self.bytes[..count * 2 * SIZEOF_OFFSET_NUMBER]
    }
}

/// An `Oid[]` borrowed from a record body.
#[derive(Clone, Copy, Debug)]
pub struct Oids<'a> {
    bytes: &'a [u8],
}

impl<'a> Oids<'a> {
    pub const fn from_bytes(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    /// Element `i`; panics past the end of the bytes.
    pub fn get(&self, i: usize) -> Oid {
        u32_at(self.bytes, i * SIZEOF_OID)
    }

    /// The raw bytes of the first `count` elements.
    pub fn bytes_of(&self, count: usize) -> &'a [u8] {
        &self.bytes[..count * SIZEOF_OID]
    }
}
