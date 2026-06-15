//! On-disk / private ABI structures and constants for the SP-GiST access
//! method.
//!
//! These mirror `src/include/access/spgist_private.h` from PostgreSQL 18.3.
//! The on-disk page structs (`SpGistPageOpaqueData`, `SpGistMetaPageData`,
//! `SpGistInnerTupleData`, `SpGistLeafTupleData`) live here as `#[repr(C)]`
//! with compile-time layout assertions.  `SpGistState`/`SpGistTypeDesc` are the
//! in-memory working state, carried across the crate boundary as ABI structs;
//! their full runtime semantics live inside `backend-access-spgist`.

use core::ffi::c_void;

use crate::spgist::spgConfigOut;
use crate::{uint16, uint32, BlockNumber, ItemPointerData, Oid, TransactionId, TupleDesc};

/// `SPGIST_MAGIC_NUMBER` -- metapage identity cross-check (spgist_private.h).
pub const SPGIST_MAGIC_NUMBER: uint32 = 0xBA0B_ABEE;

/// `SPGIST_CACHED_PAGES` -- number of last-used-page cache slots
/// (spgist_private.h).
pub const SPGIST_CACHED_PAGES: usize = 8;

// `spgist_page_id` and tupstate values (spgist_private.h).
pub const SPGIST_PAGE_ID: uint16 = 0xFF82;
pub const SPGIST_LIVE: u32 = 0; // normal live tuple (either inner or leaf)
pub const SPGIST_REDIRECT: u32 = 1; // temporary redirection placeholder
pub const SPGIST_DEAD: u32 = 2; // dead, cannot be removed because of links
pub const SPGIST_PLACEHOLDER: u32 = 3; // placeholder, used to preserve offsets

/// `SpGistPageOpaqueData` -- the special area of every SP-GiST page
/// (spgist_private.h).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SpGistPageOpaqueData {
    /// see bit definitions in `backend-access-spgist`.
    pub flags: uint16,
    /// number of redirection tuples on page.
    pub nRedirection: uint16,
    /// number of placeholder tuples on page.
    pub nPlaceholder: uint16,
    /// for identification of SP-GiST indexes.
    pub spgist_page_id: uint16,
}

/// `SpGistLastUsedPage` -- one last-used-page cache slot (spgist_private.h).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SpGistLastUsedPage {
    /// block number, or `InvalidBlockNumber`.
    pub blkno: BlockNumber,
    /// page's free space (could be obsolete!).
    pub freeSpace: i32,
}

/// `SpGistLUPCache` -- shared storage of last-used-page info (spgist_private.h).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SpGistLUPCache {
    pub cachedPage: [SpGistLastUsedPage; SPGIST_CACHED_PAGES],
}

/// `SpGistMetaPageData` -- contents of the SP-GiST metapage (spgist_private.h).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SpGistMetaPageData {
    /// for identity cross-check.
    pub magicNumber: uint32,
    /// shared storage of last-used info.
    pub lastUsedPages: SpGistLUPCache,
}

/// `SpGistInnerTupleData` -- on-disk inner tuple header (spgist_private.h).
///
/// The C struct packs four bit fields into one `unsigned int` word:
/// `tupstate:2, allTheSame:1, nNodes:13, prefixSize:16`.  We model that word as
/// a single `uint32` and provide accessors so the on-disk layout is exact.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SpGistInnerTupleData {
    /// packed bit fields: tupstate:2, allTheSame:1, nNodes:13, prefixSize:16.
    pub bits: uint32,
    /// total size of inner tuple.
    pub size: uint16,
    /* On most machines there will be a couple of wasted bytes here.
     * prefix datum follows, then nodes. */
}

impl SpGistInnerTupleData {
    /// `tupstate:2`.
    #[inline]
    pub const fn tupstate(&self) -> u32 {
        self.bits & 0x3
    }
    /// `allTheSame:1`.
    #[inline]
    pub const fn all_the_same(&self) -> bool {
        (self.bits >> 2) & 0x1 != 0
    }
    /// `nNodes:13`.
    #[inline]
    pub const fn n_nodes(&self) -> u32 {
        (self.bits >> 3) & 0x1FFF
    }
    /// `prefixSize:16`.
    #[inline]
    pub const fn prefix_size(&self) -> u32 {
        (self.bits >> 16) & 0xFFFF
    }
}

/// `SpGistLeafTupleData` -- on-disk leaf tuple header (spgist_private.h).
///
/// The C struct packs `tupstate:2, size:30` into one `unsigned int` word,
/// modelled here as a single `uint32`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SpGistLeafTupleData {
    /// packed bit fields: tupstate:2, size:30.
    pub bits: uint32,
    /// nextOffset (14 bits) plus two flag bits.
    pub t_info: uint16,
    /// TID of represented heap tuple.
    pub heapPtr: ItemPointerData,
    /* nulls bitmap follows if the flag bit for it is set;
     * leaf datum, then any included datums, follow on a MAXALIGN boundary. */
}

impl SpGistLeafTupleData {
    /// `tupstate:2`.
    #[inline]
    pub const fn tupstate(&self) -> u32 {
        self.bits & 0x3
    }
    /// `size:30`.
    #[inline]
    pub const fn size(&self) -> u32 {
        self.bits >> 2
    }
}

/// `SpGistTypeDesc` -- per-datatype info needed in `SpGistState`
/// (spgist_private.h).  In-memory ABI struct.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SpGistTypeDesc {
    pub type_: Oid,
    pub attlen: i16,
    pub attbyval: bool,
    pub attalign: i8,
    pub attstorage: i8,
}

/// `SpGistState` -- the per-operation working state (spgist_private.h).
///
/// In-memory ABI struct carried across the crate boundary; pointer members are
/// opaque to the FFI layer.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SpGistState {
    /// index we're working with (`Relation`, opaque).
    pub index: *mut c_void,
    /// filled in by opclass config method.
    pub config: spgConfigOut,
    /// type of values to be indexed/restored.
    pub attType: SpGistTypeDesc,
    /// type of leaf-tuple values.
    pub attLeafType: SpGistTypeDesc,
    /// type of inner-tuple prefix values.
    pub attPrefixType: SpGistTypeDesc,
    /// type of node label values.
    pub attLabelType: SpGistTypeDesc,
    /// descriptor for leaf-level tuples.
    pub leafTupDesc: TupleDesc,
    /// workspace for spgFormDeadTuple (`char *`).
    pub deadTupleStorage: *mut c_void,
    /// XID to use when creating a redirect tuple.
    pub redirectXid: TransactionId,
    /// true if doing index build.
    pub isBuild: bool,
}

// ---------------------------------------------------------------------------
// Compile-time layout assertions.
// ---------------------------------------------------------------------------

const _: () = {
    // SpGistPageOpaqueData: 4 * uint16 = 8.
    assert!(core::mem::size_of::<SpGistPageOpaqueData>() == 8);
    assert!(core::mem::align_of::<SpGistPageOpaqueData>() == 2);

    // SpGistLastUsedPage: BlockNumber (4) + int (4) = 8.
    assert!(core::mem::size_of::<SpGistLastUsedPage>() == 8);

    // SpGistMetaPageData: magic (4) + 8 cache slots * 8 = 4 + 64 = 68.
    assert!(core::mem::size_of::<SpGistMetaPageData>() == 4 + SPGIST_CACHED_PAGES * 8);

    // SpGistInnerTupleData: uint32 (4) + uint16 (2), padded to align 4 = 8.
    assert!(core::mem::offset_of!(SpGistInnerTupleData, size) == 4);
    assert!(core::mem::align_of::<SpGistInnerTupleData>() == 4);

    // SpGistLeafTupleData: uint32 (4) + uint16 (2) + ItemPointerData (6) = 12.
    assert!(core::mem::offset_of!(SpGistLeafTupleData, t_info) == 4);
    assert!(core::mem::offset_of!(SpGistLeafTupleData, heapPtr) == 6);
    assert!(core::mem::size_of::<SpGistLeafTupleData>() == 12);
};
