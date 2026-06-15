//! ABI definitions for the public TID bitmap API (`nodes/tidbitmap.h`) plus the
//! on-DSA / pointer-indexed structures `tidbitmap.c` shares across processes.
//!
//! The high-level `TIDBitmap`, `TBMPrivateIterator`, and `TBMSharedIterator`
//! handles are private to the `tidbitmap` crate (idiomatic, not ABI), exactly as
//! in C where they are "private to tidbitmap.c".  But the per-page bitmap entry
//! ([`PagetableEntry`]) and the DSA-resident arrays that wrap it
//! ([`PTEntryArray`], [`PTIterationArray`], [`TBMSharedIteratorState`]) are
//! addressed by pointer arithmetic (`page - ptbase->ptentry`) and laid out in
//! shared memory, so their layout must match the C ABI exactly.

use core::ffi::c_void;

use crate::bitmapset::{bitmapword, BITS_PER_BITMAPWORD};
use crate::storage::{pg_atomic_uint32, LWLock, MaxHeapTuplesPerPage};
use crate::types::{BlockNumber, BLCKSZ};

/// `TBM_MAX_TUPLES_PER_PAGE` (`nodes/tidbitmap.h`): `MaxHeapTuplesPerPage`.
pub const TBM_MAX_TUPLES_PER_PAGE: usize = MaxHeapTuplesPerPage as usize;

/// `PAGES_PER_CHUNK` (`tidbitmap.c`): `BLCKSZ / 32` pages aggregated into one
/// lossy chunk; a power of two so the chunk index is a cheap mask.
pub const PAGES_PER_CHUNK: usize = BLCKSZ / 32;

/// `WORDS_PER_PAGE` (`tidbitmap.c`): active words for an exact page,
/// `(TBM_MAX_TUPLES_PER_PAGE - 1) / BITS_PER_BITMAPWORD + 1`.
pub const WORDS_PER_PAGE: usize = (TBM_MAX_TUPLES_PER_PAGE - 1) / BITS_PER_BITMAPWORD + 1;

/// `WORDS_PER_CHUNK` (`tidbitmap.c`): active words for a lossy chunk,
/// `(PAGES_PER_CHUNK - 1) / BITS_PER_BITMAPWORD + 1`.
pub const WORDS_PER_CHUNK: usize = (PAGES_PER_CHUNK - 1) / BITS_PER_BITMAPWORD + 1;

/// `Max(WORDS_PER_PAGE, WORDS_PER_CHUNK)` — the fixed `words[]` length stored in
/// every [`PagetableEntry`].
pub const WORDS_PER_PAGETABLE_ENTRY: usize = if WORDS_PER_PAGE > WORDS_PER_CHUNK {
    WORDS_PER_PAGE
} else {
    WORDS_PER_CHUNK
};

/// `PagetableEntry` (`tidbitmap.c`): a hashtable entry.  For an exact page,
/// `blockno` is the page number and bit `k` of the bitmap represents tuple offset
/// `k+1`.  For a lossy chunk, `blockno` is the first page in the chunk (a multiple
/// of `PAGES_PER_CHUNK`) and bit `k` represents page `blockno+k`.
///
/// This is addressed by pointer arithmetic in the shared path (`page -
/// ptbase->ptentry`) and laid out in DSA, so its layout matches the C ABI
/// exactly: `BlockNumber`, `char status`, two `bool`s, then the
/// (bitmapword-aligned) words array.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PagetableEntry {
    /// page number (hashtable key)
    pub blockno: BlockNumber,
    /// hash entry status (`SH_STATUS_EMPTY`/`SH_STATUS_IN_USE`); a `char` in C.
    pub status: i8,
    /// `true` = lossy storage, `false` = exact.
    pub ischunk: bool,
    /// should the tuples be rechecked? (exact pages only)
    pub recheck: bool,
    /// per-tuple (exact) or per-page (lossy chunk) bitmap.
    pub words: [bitmapword; WORDS_PER_PAGETABLE_ENTRY],
}

impl Default for PagetableEntry {
    fn default() -> Self {
        Self {
            blockno: 0,
            status: 0,
            ischunk: false,
            recheck: false,
            words: [0; WORDS_PER_PAGETABLE_ENTRY],
        }
    }
}

/// `PTEntryArray` (`tidbitmap.c`): holds the array of pagetable entries when the
/// bitmap is DSA-backed.  `ptentry` is a C `FLEXIBLE_ARRAY_MEMBER`; the trailing
/// entries live immediately after the header.
#[repr(C)]
pub struct PTEntryArray {
    /// number of iterators attached
    pub refcount: pg_atomic_uint32,
    /// flexible array of [`PagetableEntry`]
    pub ptentry: [PagetableEntry; 0],
}

/// `PTIterationArray` (`tidbitmap.c`): the sorted page or chunk index array.
/// `index` is a C `FLEXIBLE_ARRAY_MEMBER`.
#[repr(C)]
pub struct PTIterationArray {
    /// number of iterators attached
    pub refcount: pg_atomic_uint32,
    /// flexible index array
    pub index: [i32; 0],
}

/// `TBMSharedIteratorState` (`tidbitmap.c`): the shared members of an iterator,
/// allocated in DSA so multiple processes can iterate jointly.
#[repr(C)]
pub struct TBMSharedIteratorState {
    /// number of entries in pagetable
    pub nentries: i32,
    /// limit on same to meet maxbytes
    pub maxentries: i32,
    /// number of exact entries in pagetable
    pub npages: i32,
    /// number of lossy entries in pagetable
    pub nchunks: i32,
    /// dsa pointer to head of pagetable data
    pub pagetable: u64,
    /// dsa pointer to page array
    pub spages: u64,
    /// dsa pointer to chunk array
    pub schunks: u64,
    /// lock to protect the members below
    pub lock: LWLock,
    /// next spages index
    pub spageptr: i32,
    /// next schunks index
    pub schunkptr: i32,
    /// next bit to check in current schunk
    pub schunkbit: i32,
}

/// `TBMIterateResult` (`nodes/tidbitmap.h`): result of one iteration step.
/// `internal_page` is a `void *` to avoid exposing the private
/// `PagetableEntry` layout to callers.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TBMIterateResult {
    /// block number containing tuples
    pub blockno: BlockNumber,
    pub lossy: bool,
    /// whether the tuples should be rechecked (always true for a lossy page)
    pub recheck: bool,
    /// pointer to the page's bitmap (`PagetableEntry *`, kept opaque)
    pub internal_page: *mut c_void,
}

/// `TBMIterator` (`nodes/tidbitmap.h`): unified private/shared iterator. The
/// `shared` flag selects which arm of the (Rust: pointer-sized) union is valid.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TBMIterator {
    pub shared: bool,
    /// `union { TBMPrivateIterator *private_iterator; TBMSharedIterator *shared_iterator; }`
    pub i: *mut c_void,
}

const _: () = {
    use core::mem::{align_of, offset_of, size_of};

    // PagetableEntry: blockno(u32) + status(i8) + ischunk(bool) + recheck(bool),
    // padded out to the bitmapword alignment before the words[] array.
    assert!(offset_of!(PagetableEntry, blockno) == 0);
    assert!(offset_of!(PagetableEntry, status) == 4);
    assert!(offset_of!(PagetableEntry, ischunk) == 5);
    assert!(offset_of!(PagetableEntry, recheck) == 6);
    // bitmapword is 8-byte aligned (u64) on the default build, so the words[]
    // array starts at offset 8 right after the 4-byte-padded header.
    assert!(align_of::<bitmapword>() == 8);
    assert!(offset_of!(PagetableEntry, words) == 8);
    assert!(align_of::<PagetableEntry>() == align_of::<bitmapword>());
    // Default 8K BLCKSZ, 64-bit bitmapword build: 291 tuples/page -> 5 words.
    assert!(WORDS_PER_PAGETABLE_ENTRY == 5);
    assert!(size_of::<PagetableEntry>() == 8 + WORDS_PER_PAGETABLE_ENTRY * 8);

    // PTEntryArray / PTIterationArray: a refcount header then a flexible array
    // that must begin on the element's alignment boundary (8 for PagetableEntry).
    assert!(offset_of!(PTEntryArray, ptentry) == 8);
    assert!(offset_of!(PTIterationArray, index) == 4);

    // TBMSharedIteratorState begins with the four ints in declaration order.
    assert!(offset_of!(TBMSharedIteratorState, nentries) == 0);
    assert!(offset_of!(TBMSharedIteratorState, maxentries) == 4);
    assert!(offset_of!(TBMSharedIteratorState, npages) == 8);
    assert!(offset_of!(TBMSharedIteratorState, nchunks) == 12);

    // TBMIterateResult begins with the block number; internal_page stays opaque.
    assert!(offset_of!(TBMIterateResult, blockno) == 0);
    assert!(size_of::<TBMIterator>() >= size_of::<usize>());
};
