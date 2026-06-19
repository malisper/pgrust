//! TID-bitmap vocabulary (`nodes/tidbitmap.h`), trimmed to what bitmap-scan
//! consumers store and pass.
//!
//! `TIDBitmap`, `TBMPrivateIterator`, and `TBMSharedIterator` are genuinely
//! private (opaque) structs in C — the header only forward-declares them and
//! `tidbitmap.c` (the `backend-nodes-core` unit) owns their layout. The
//! opacity is semantic, so they stay opaque here (the executor only stores and
//! hands them back). `TBMIterator` is the public struct that unifies the two
//! iterator kinds.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::boxed::Box;
use core::any::Any;

/// `dsa_pointer` (`utils/dsa.h`) — an offset into a `dsa_area`.
pub type dsa_pointer = u64;

/// `InvalidDsaPointer` (`utils/dsa.h`).
pub const InvalidDsaPointer: dsa_pointer = 0;

/// `DsaPointerIsValid(x)` (`utils/dsa.h`): `((x) != InvalidDsaPointer)`.
#[inline]
pub fn dsa_pointer_is_valid(p: dsa_pointer) -> bool {
    p != InvalidDsaPointer
}

/// `TIDBitmap` (`nodes/tidbitmap.c`) — opaque to every consumer outside
/// `tidbitmap.c`, which owns the real `struct TIDBitmap`. The box holds that
/// real interior by value (the owner downcasts it); the executor / table-AM /
/// index AMs only store and pass this carrier — it *is* the owning `TIDBitmap
/// *` (no side table, no integer handle). `None` is the C `NULL`.
#[derive(Default)]
pub struct TIDBitmap(pub Option<Box<dyn Any>>);

impl core::fmt::Debug for TIDBitmap {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self.0 {
            Some(_) => f.write_str("TIDBitmap(<set>)"),
            None => f.write_str("TIDBitmap(<null>)"),
        }
    }
}

/// `TBMPrivateIterator` (`nodes/tidbitmap.c`) — opaque, owned by tidbitmap.c.
#[derive(Default)]
pub struct TBMPrivateIterator(pub Option<Box<dyn Any>>);

impl core::fmt::Debug for TBMPrivateIterator {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("TBMPrivateIterator")
    }
}

/// `TBMSharedIterator` (`nodes/tidbitmap.c`) — opaque, owned by tidbitmap.c.
#[derive(Default)]
pub struct TBMSharedIterator(pub Option<Box<dyn Any>>);

impl core::fmt::Debug for TBMSharedIterator {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("TBMSharedIterator")
    }
}

/// `TBMIterator` (`nodes/tidbitmap.h`) — the unified private/shared iterator.
/// C uses a `bool shared` plus a union of the two iterator pointers; the owned
/// model carries the discriminated pair directly.
#[derive(Debug, Default)]
pub struct TBMIterator {
    /// `bool shared` — whether `i` is the shared iterator.
    pub shared: bool,
    /// `union { TBMPrivateIterator *private_iterator; TBMSharedIterator
    /// *shared_iterator; } i` — `None` is the C `NULL` (set by
    /// `tbm_end_iterate`).
    pub private_iterator: Option<Box<TBMPrivateIterator>>,
    pub shared_iterator: Option<Box<TBMSharedIterator>>,
}

/// The decoded result of one `tbm_iterate` step, as the bitmap-scan table-AM
/// (`heapam_scan_bitmap_next_tuple` / `BitmapHeapScanNextBlock`) consumes it.
/// Combines `tbm_iterate` and `tbm_extract_page_tuple`: a `TBMIterateResult`
/// (`nodes/tidbitmap.h`, owned by `tidbitmap.c`) plus, for an exact (non-lossy)
/// page, the extracted per-tuple `OffsetNumber`s. The real `TBMIterateResult`
/// stays private to `tidbitmap.c`; the table-AM only needs these decoded
/// fields, so the seam hands them back as a plain value.
#[derive(Clone, Debug, Default)]
pub struct TBMIterateOutcome {
    /// `TBMIterateResult.blockno` — page containing tuples from the bitmap.
    pub blockno: u32,
    /// `TBMIterateResult.lossy` — whether the bitmap is lossy for this page.
    pub lossy: bool,
    /// `TBMIterateResult.recheck` — whether to recheck the qual conditions.
    pub recheck: bool,
    /// `tbm_extract_page_tuple(tbmres, offsets, ...)` for an exact page — the
    /// `OffsetNumber`s of the candidate tuples on the page. Empty for a lossy
    /// page (the AM scans every line pointer instead).
    pub offsets: alloc::vec::Vec<u16>,
}

impl TBMIterator {
    /// `tbm_exhausted(iterator)` (`nodes/tidbitmap.h`): `!iterator->i.<ptr>`.
    /// After `tbm_end_iterate` both pointers are NULL, so checking either is
    /// equivalent (C checks `private_iterator`).
    #[inline]
    pub fn exhausted(&self) -> bool {
        self.private_iterator.is_none() && self.shared_iterator.is_none()
    }
}
