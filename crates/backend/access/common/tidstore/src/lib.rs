#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
//! Port of `src/backend/access/common/tidstore.c`.
//!
//! `TidStore` is an in-memory set of TIDs (`ItemPointerData`). Internally it
//! uses a radix tree as the storage for TIDs: the key is the `BlockNumber` and
//! the value is a `BlocktableEntry` — a bitmap of in-use offsets for that
//! block. A `TidStore` may live in backend-local memory or, for parallel
//! workers, in a DSA-shared area guarded by the radix tree's own LWLock.
//!
//! ## What is this crate's own logic vs. seamed substrate
//!
//! The *bit math* of `tidstore.c` is the actual content of the file and is
//! ported here 1:1: [`BlocktableEntry::from_offsets`] packs a sorted ascending
//! offset array into either the small `full_offsets` header form (when
//! `num_offsets <= NUM_FULL_OFFSETS`) or the compressed `words` bitmap form,
//! with the `nwords` discriminator, the per-word threshold walk, and the
//! out-of-range offset check; [`BlocktableEntry::contains`] is the membership
//! test; [`BlocktableEntry::offsets_into`] unpacks an entry back into an offset
//! array; and the `WORDNUM` / `BITNUM` / `WORDS_PER_PAGE` / `MAX_OFFSET_IN_BITMAP`
//! helpers are pure arithmetic.
//!
//! What lives *outside* the bit math is the radix-tree container itself
//! (`lib/radixtree.h`, instantiated as `local_ts_*` / `shared_ts_*`), the DSA
//! area + shared-memory allocation, and the LWLock guarding a shared store.
//! That container is its own unit; it is reached through
//! [`radixtree_seams`], threading the owned [`TidStore`] descriptor
//! and an opaque [`TidStoreIterHandle`]. The radix value (a `BlocktableEntry`)
//! crosses the seam as a `Vec<bitmapword>` wire image — see
//! [`BlocktableEntry::encode`] / [`BlocktableEntry::decode`].
//!
//! ## Wire format across the radix seam
//!
//! A [`BlocktableEntry`] is encoded as a `Vec<bitmapword>` whose first element
//! is the packed header (a byte-faithful image of the C `header` `uintptr_t`
//! slot: `flags` in byte 0, `nwords` in byte 1, then the `NUM_FULL_OFFSETS`
//! little-endian `OffsetNumber`s) followed by the `nwords` bitmap words. This
//! mirrors the C in-memory layout, where the header occupies one pointer-sized
//! slot immediately followed by `words[]`.

extern crate alloc;

use alloc::format;
use alloc::vec::Vec;

use ::radixtree_seams::{
    radixtree_attach, radixtree_begin_iterate, radixtree_create_local, radixtree_create_shared,
    radixtree_detach, radixtree_end_iterate, radixtree_find, radixtree_free, radixtree_get_dsa,
    radixtree_get_handle, radixtree_iterate_next, radixtree_lock, radixtree_memory_usage,
    radixtree_set,
};
use ::page::ItemPointerGetBlockNumber;
use ::types_core::{BlockNumber, OffsetNumber};
use ::types_dsa::{DsaHandle, DsaPointer};
use ::types_error::{PgError, PgResult};
use ::nodes::bitmapset::{bitmapword, BITS_PER_BITMAPWORD};
use ::types_storage::bufpage::MaxOffsetNumber;
use ::types_tuple::heaptuple::{ItemPointerData, INVALID_OFFSET_NUMBER as InvalidOffsetNumber};
pub use ::types_vacuum::vacuumlazy::{ReapBlockInfo, TidStore, TidStoreIterHandle};

// ===========================================================================
// Bit-index helpers (`WORDNUM` / `BITNUM` / `WORDS_PER_PAGE`).
// ===========================================================================

/// `#define WORDNUM(x) ((x) / BITS_PER_BITMAPWORD)`.
#[inline]
const fn wordnum(x: usize) -> usize {
    x / BITS_PER_BITMAPWORD
}

/// `#define BITNUM(x) ((x) % BITS_PER_BITMAPWORD)`.
#[inline]
const fn bitnum(x: usize) -> usize {
    x % BITS_PER_BITMAPWORD
}

/// `#define WORDS_PER_PAGE(n) ((n) / BITS_PER_BITMAPWORD + 1)` — number of
/// active words required to address offset `n`.
#[inline]
const fn words_per_page(n: usize) -> usize {
    n / BITS_PER_BITMAPWORD + 1
}

/// `#define NUM_FULL_OFFSETS \
///     ((sizeof(uintptr_t) - sizeof(uint8) - sizeof(int8)) / sizeof(OffsetNumber))`
///
/// The number of offsets that fit in the entry header alongside `flags`
/// (`uint8`) and `nwords` (`int8`). With a 64-bit `uintptr_t` and a 16-bit
/// `OffsetNumber` this is `(8 - 1 - 1) / 2 == 3`.
pub const NUM_FULL_OFFSETS: usize = (core::mem::size_of::<usize>()
    - core::mem::size_of::<u8>()
    - core::mem::size_of::<i8>())
    / core::mem::size_of::<OffsetNumber>();

/// `#define MAX_OFFSET_IN_BITMAP \
///     Min(BITS_PER_BITMAPWORD * PG_INT8_MAX - 1, MaxOffsetNumber)`
///
/// The `int8` width of `header.nwords` caps how many bitmap words an entry can
/// hold, which caps the largest offset the bitmap can address. In practice this
/// is almost always exactly `MaxOffsetNumber`.
const BITMAP_LIMIT_OFFSET: usize = BITS_PER_BITMAPWORD * (i8::MAX as usize) - 1;
pub const MAX_OFFSET_IN_BITMAP: OffsetNumber = if BITMAP_LIMIT_OFFSET < MaxOffsetNumber as usize {
    BITMAP_LIMIT_OFFSET as OffsetNumber
} else {
    MaxOffsetNumber
};

// ===========================================================================
// BlocktableEntry — the per-block value stored in the radix tree.
// ===========================================================================

/// `struct BlocktableEntry` — the radix-tree value for one block: a small set
/// of offsets stored either inline in the header (`full_offsets`, when
/// `nwords == 0`) or as a bitmap of `words`.
///
/// This owned form replaces the C flexible-array-member struct. The
/// across-seam byte image is produced by [`Self::encode`] / consumed by
/// [`Self::decode`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BlocktableEntry {
    /// `header.flags` — reserved by the radix tree to tag embedded values; the
    /// TidStore code itself never sets it, but it is preserved faithfully.
    pub flags: u8,
    /// `header.nwords` — number of active bitmap `words`. `0` selects the
    /// inline `full_offsets` form.
    pub nwords: i8,
    /// `header.full_offsets[NUM_FULL_OFFSETS]` — inline offsets used when
    /// `nwords == 0`. Unused slots are `InvalidOffsetNumber` (0).
    pub full_offsets: [OffsetNumber; NUM_FULL_OFFSETS],
    /// `words[FLEXIBLE_ARRAY_MEMBER]` — the offset bitmap when `nwords > 0`.
    pub words: Vec<bitmapword>,
}

impl BlocktableEntry {
    /// Build the packed entry for a block's sorted ascending `offsets`.
    /// Mirrors the body of `TidStoreSetBlockOffsets` that fills the local
    /// `BlocktableEntry` before it is handed to the radix tree.
    fn from_offsets(offsets: &[OffsetNumber]) -> PgResult<Self> {
        debug_assert!(!offsets.is_empty(), "num_offsets > 0");
        // C: Assert the given offset numbers are ordered.
        debug_assert!(
            offsets.windows(2).all(|pair| pair[1] > pair[0]),
            "offsets must be ascending"
        );

        let num_offsets = offsets.len();
        let mut entry = BlocktableEntry::default();

        if num_offsets <= NUM_FULL_OFFSETS {
            for (i, &off) in offsets.iter().enumerate() {
                // Safety check to ensure we don't overrun bit array bounds.
                if off == InvalidOffsetNumber || off > MAX_OFFSET_IN_BITMAP {
                    return Err(tuple_offset_out_of_range(off));
                }
                entry.full_offsets[i] = off;
            }
            entry.nwords = 0;
            return Ok(entry);
        }

        let last_off = offsets[num_offsets - 1] as usize;
        let mut words: Vec<bitmapword> = Vec::new();
        // The words array is sized to the highest offset; this is data-derived,
        // so reserve fallibly to keep allocation OOM-safe.
        words
            .try_reserve_exact(words_per_page(last_off))
            .map_err(|_| PgError::error("out of memory"))?;

        let mut idx = 0usize;
        let mut wordnum_cur: usize = 0;
        let mut next_word_threshold = BITS_PER_BITMAPWORD;
        while wordnum_cur <= wordnum(last_off) {
            let mut word: bitmapword = 0;

            while idx < num_offsets {
                let off = offsets[idx];

                // Safety check to ensure we don't overrun bit array bounds.
                if off == InvalidOffsetNumber || off > MAX_OFFSET_IN_BITMAP {
                    return Err(tuple_offset_out_of_range(off));
                }

                if (off as usize) >= next_word_threshold {
                    break;
                }

                word |= (1 as bitmapword) << bitnum(off as usize);
                idx += 1;
            }

            // Write out offset bitmap for this wordnum.
            words.push(word);

            wordnum_cur += 1;
            next_word_threshold += BITS_PER_BITMAPWORD;
        }

        entry.nwords = words.len() as i8;
        debug_assert_eq!(
            entry.nwords as usize,
            words_per_page(last_off),
            "nwords == WORDS_PER_PAGE(offsets[num_offsets - 1])"
        );
        entry.words = words;
        Ok(entry)
    }

    /// Test whether `off` is present in this entry. Mirrors the membership
    /// test in `TidStoreIsMember`.
    fn contains(&self, off: OffsetNumber) -> bool {
        if self.nwords == 0 {
            // We have offsets in the header.
            self.full_offsets.iter().any(|&slot| slot == off)
        } else {
            let wn = wordnum(off as usize);
            let bn = bitnum(off as usize);

            // No bitmap for the off.
            if wn >= self.nwords as usize {
                return false;
            }

            (self.words[wn] & ((1 as bitmapword) << bn)) != 0
        }
    }

    /// Unpack this entry's offsets into `offsets`, returning the number of
    /// offsets found. If more offsets exist than `offsets` can hold, fills as
    /// many as fit and returns the total count required. Mirrors
    /// `TidStoreGetBlockOffsets`.
    fn offsets_into(&self, offsets: &mut [OffsetNumber]) -> usize {
        let max_offsets = offsets.len();
        let mut num_offsets = 0usize;

        if self.nwords == 0 {
            // We have offsets in the header.
            for &candidate in self.full_offsets.iter() {
                if candidate != InvalidOffsetNumber {
                    if num_offsets < max_offsets {
                        offsets[num_offsets] = candidate;
                    }
                    num_offsets += 1;
                }
            }
        } else {
            for wn in 0..(self.nwords as usize) {
                let mut w = self.words[wn];
                let mut off = wn * BITS_PER_BITMAPWORD;

                while w != 0 {
                    if w & 1 != 0 {
                        if num_offsets < max_offsets {
                            offsets[num_offsets] = off as OffsetNumber;
                        }
                        num_offsets += 1;
                    }
                    off += 1;
                    w >>= 1;
                }
            }
        }

        num_offsets
    }

    /// Encode this entry into the `Vec<bitmapword>` wire image that crosses the
    /// radix seam: `wire[0]` is the packed header, `wire[1..]` are the bitmap
    /// words.
    fn encode(&self) -> PgResult<Vec<bitmapword>> {
        let mut wire: Vec<bitmapword> = Vec::new();
        wire.try_reserve_exact(1 + self.words.len())
            .map_err(|_| PgError::error("out of memory"))?;

        // Pack header faithfully to the C `uintptr_t`-sized `header` slot:
        // byte 0 = flags, byte 1 = nwords (int8, sign-preserved as a byte),
        // then NUM_FULL_OFFSETS little-endian 16-bit OffsetNumbers.
        let mut header: bitmapword = 0;
        header |= self.flags as bitmapword;
        header |= ((self.nwords as u8) as bitmapword) << 8;
        for (i, &off) in self.full_offsets.iter().enumerate() {
            header |= (off as bitmapword) << (16 + i * 16);
        }
        wire.push(header);

        for &w in self.words.iter() {
            wire.push(w);
        }

        Ok(wire)
    }

    /// Decode a `Vec<bitmapword>` wire image produced by [`Self::encode`] back
    /// into a `BlocktableEntry`.
    fn decode(wire: &[bitmapword]) -> PgResult<Self> {
        let header = *wire
            .first()
            .ok_or_else(|| PgError::error("TidStore radix entry is empty"))?;

        let flags = (header & 0xff) as u8;
        let nwords = ((header >> 8) & 0xff) as u8 as i8;
        let mut full_offsets = [InvalidOffsetNumber; NUM_FULL_OFFSETS];
        for (i, slot) in full_offsets.iter_mut().enumerate() {
            *slot = ((header >> (16 + i * 16)) & 0xffff) as OffsetNumber;
        }

        let mut words: Vec<bitmapword> = Vec::new();
        let body = &wire[1..];
        words
            .try_reserve_exact(body.len())
            .map_err(|_| PgError::error("out of memory"))?;
        words.extend_from_slice(body);

        Ok(Self {
            flags,
            nwords,
            full_offsets,
            words,
        })
    }
}

/// `elog(ERROR, "tuple offset out of range: %u", off)`.
fn tuple_offset_out_of_range(off: OffsetNumber) -> PgError {
    PgError::error(format!("tuple offset out of range: {off}"))
}

// ===========================================================================
// TidStore — per-backend handle to the (local or shared) radix-tree store.
// ===========================================================================
//
// `struct TidStore` — the per-backend descriptor is the canonical
// `::types_vacuum::vacuumlazy::TidStore` handle. In C the struct holds the
// radix-tree pointer (local or shared), the local `rt_context`
// `MemoryContext`, and the optional DSA `area`; here the whole radix/DSA/LWLock
// substrate is owned by the radix-tree provider and reached through the
// descriptor's `id`. `TidStoreIsShared(ts)` is `area != NULL` in C; the radix
// owner records each tree's flavor, so the lock / handle ops delegate
// unconditionally and the owner no-ops the lock for a local tree.

/// `ALLOCSET_DEFAULT_MINSIZE` (`utils/memutils.h`): `0`.
const ALLOCSET_DEFAULT_MINSIZE: usize = 0;
/// `ALLOCSET_DEFAULT_INITSIZE` (`utils/memutils.h`): `8 * 1024`.
const ALLOCSET_DEFAULT_INITSIZE: usize = 8 * 1024;
/// `ALLOCSET_DEFAULT_MAXSIZE` (`utils/memutils.h`): `8 * 1024 * 1024`.
const ALLOCSET_DEFAULT_MAXSIZE: usize = 8 * 1024 * 1024;

/// `TidStore *TidStoreCreateLocal(size_t max_bytes, bool insert_only)`.
///
/// Creates a backend-local radix-tree-backed store. `max_bytes` is only a hint
/// used to cap the storage memory context's block size; it is not enforced.
/// The block-size policy (this function's own logic) is computed here; the
/// memory-context + radix-tree creation is the external substrate.
pub fn TidStoreCreateLocal(max_bytes: usize, insert_only: bool) -> PgResult<TidStore> {
    let init_block_size = ALLOCSET_DEFAULT_INITSIZE;
    let min_context_size = ALLOCSET_DEFAULT_MINSIZE;
    let mut max_block_size = ALLOCSET_DEFAULT_MAXSIZE;

    // Choose the maxBlockSize to be no larger than 1/16 of max_bytes.
    while 16 * max_block_size > max_bytes {
        max_block_size >>= 1;
    }

    if max_block_size < ALLOCSET_DEFAULT_INITSIZE {
        max_block_size = ALLOCSET_DEFAULT_INITSIZE;
    }

    radixtree_create_local::call(min_context_size, init_block_size, max_block_size, insert_only)
}

/// `TidStore *TidStoreCreateShared(size_t max_bytes, int tranche_id)`.
///
/// Like [`TidStoreCreateLocal`] but the radix tree lives on a DSA area whose
/// segment sizing (this function's own logic) is derived from `max_bytes`. The
/// returned object is backend-local; the DSA segment and shared tree are the
/// external substrate.
pub fn TidStoreCreateShared(max_bytes: usize, tranche_id: i32) -> PgResult<TidStore> {
    let mut dsa_init_size = ::types_dsa::DSA_DEFAULT_INIT_SEGMENT_SIZE;
    let mut dsa_max_size = ::types_dsa::DSA_MAX_SEGMENT_SIZE;

    // Choose the initial and maximum DSA segment sizes to be no longer than
    // 1/8 of max_bytes.
    while 8 * dsa_max_size > max_bytes {
        dsa_max_size >>= 1;
    }

    if dsa_max_size < ::types_dsa::DSA_MIN_SEGMENT_SIZE {
        dsa_max_size = ::types_dsa::DSA_MIN_SEGMENT_SIZE;
    }

    if dsa_init_size > dsa_max_size {
        dsa_init_size = dsa_max_size;
    }

    radixtree_create_shared::call(dsa_init_size, dsa_max_size, tranche_id)
}

/// `TidStore *TidStoreAttach(dsa_handle area_handle, dsa_pointer handle)`.
///
/// Attach to a shared TidStore created in another backend. `area_handle` names
/// the DSA area, `handle` is the value from [`TidStoreGetHandle`]. The returned
/// object is backend-local.
pub fn TidStoreAttach(area_handle: DsaHandle, handle: DsaPointer) -> PgResult<TidStore> {
    debug_assert!(area_handle != ::types_dsa::DSA_HANDLE_INVALID);
    debug_assert!(handle != ::types_dsa::INVALID_DSA_POINTER);
    radixtree_attach::call(area_handle, handle)
}

/// `void TidStoreDetach(TidStore *ts)` — detach from a shared TidStore,
/// releasing backend-local resources. Asserts the store is shared.
pub fn TidStoreDetach(ts: &TidStore) -> PgResult<()> {
    radixtree_detach::call(*ts)
}

/// `void TidStoreLockExclusive(TidStore *ts)` — take the radix tree's exclusive
/// lock (no-op for a local store; the radix owner knows the tree's flavor).
pub fn TidStoreLockExclusive(ts: &TidStore) -> PgResult<()> {
    radixtree_lock::call(*ts, Some(true))
}

/// `void TidStoreLockShare(TidStore *ts)` — take the radix tree's shared lock
/// (no-op for a local store).
pub fn TidStoreLockShare(ts: &TidStore) -> PgResult<()> {
    radixtree_lock::call(*ts, Some(false))
}

/// `void TidStoreUnlock(TidStore *ts)` — release the radix tree's lock (no-op
/// for a local store).
pub fn TidStoreUnlock(ts: &TidStore) -> PgResult<()> {
    radixtree_lock::call(*ts, None)
}

/// `void TidStoreDestroy(TidStore *ts)` — destroy the store, returning all
/// memory. For a shared store this frees the shared radix tree and detaches the
/// DSA; for a local store it frees the tree and deletes `rt_context`.
pub fn TidStoreDestroy(ts: &TidStore) -> PgResult<()> {
    radixtree_free::call(*ts)
}

/// `void TidStoreSetBlockOffsets(TidStore *ts, BlockNumber blkno,
///                               OffsetNumber *offsets, int num_offsets)`.
///
/// Create or replace the entry for `blkno` from the sorted ascending
/// `offsets`. NB (per the C contract): `offsets` must be ascending, and an
/// existing block's entry is *replaced* — there is no add/remove of individual
/// offsets.
pub fn TidStoreSetBlockOffsets(
    ts: &TidStore,
    blkno: BlockNumber,
    offsets: &[OffsetNumber],
) -> PgResult<()> {
    debug_assert!(!offsets.is_empty(), "num_offsets > 0");

    let entry = BlocktableEntry::from_offsets(offsets)?;
    let wire = entry.encode()?;
    radixtree_set::call(*ts, blkno, wire)
}

/// `bool TidStoreIsMember(TidStore *ts, ItemPointer tid)` — return true if
/// `tid` is present in the store.
pub fn TidStoreIsMember(ts: &TidStore, tid: &ItemPointerData) -> PgResult<bool> {
    let blk = ItemPointerGetBlockNumber(tid);
    let off = ItemPointerGetOffsetNumber(tid);

    let Some(wire) = radixtree_find::call(*ts, blk)? else {
        // No entry for the blk.
        return Ok(false);
    };

    let entry = BlocktableEntry::decode(&wire)?;
    Ok(entry.contains(off))
}

/// `struct TidStoreIterResult` — the per-block result of an iteration step. In
/// C `internal_page` is an opaque `void *` into the radix tree, decoded later
/// by `TidStoreGetBlockOffsets`. Here the decoded [`BlocktableEntry`] is
/// carried directly.
#[derive(Clone, Debug)]
pub struct TidStoreIterResult {
    /// `result->blkno`.
    pub blkno: BlockNumber,
    /// The decoded entry for `blkno` (C's opaque `internal_page`).
    page: BlocktableEntry,
}

impl TidStoreIterResult {
    /// The block number this result describes.
    pub fn blkno(&self) -> BlockNumber {
        self.blkno
    }
}

/// `struct TidStoreIter` — an in-progress forward iteration over a store. The
/// underlying radix-tree iterator is the external substrate, reached through
/// `iter_handle`.
pub struct TidStoreIter {
    iter_handle: TidStoreIterHandle,
}

impl TidStoreIter {
    /// The runtime iterator handle backing this iteration.
    pub fn iter_handle(&self) -> TidStoreIterHandle {
        self.iter_handle
    }

    /// Reconstruct an in-progress iteration from its bare runtime iterator
    /// handle (the inverse of [`Self::iter_handle`]).
    pub fn from_iter_handle(iter_handle: TidStoreIterHandle) -> Self {
        TidStoreIter { iter_handle }
    }
}

/// `TidStoreIter *TidStoreBeginIterate(TidStore *ts)` — prepare to iterate the
/// store. The caller is responsible for locking the store until the iteration
/// is finished.
pub fn TidStoreBeginIterate(ts: &TidStore) -> PgResult<TidStoreIter> {
    let iter_handle = radixtree_begin_iterate::call(*ts)?;
    Ok(TidStoreIter { iter_handle })
}

/// `TidStoreIterResult *TidStoreIterateNext(TidStoreIter *iter)` — the next
/// `(blkno, page)` in ascending block order, or `None` at end.
pub fn TidStoreIterateNext(iter: &mut TidStoreIter) -> PgResult<Option<TidStoreIterResult>> {
    let Some((blkno, wire)) = radixtree_iterate_next::call(iter.iter_handle)? else {
        return Ok(None);
    };

    let page = BlocktableEntry::decode(&wire)?;
    Ok(Some(TidStoreIterResult { blkno, page }))
}

/// `void TidStoreEndIterate(TidStoreIter *iter)` — finish the iteration,
/// releasing the iterator. The caller is responsible for releasing any locks.
pub fn TidStoreEndIterate(iter: TidStoreIter) -> PgResult<()> {
    radixtree_end_iterate::call(iter.iter_handle)
}

/// `size_t TidStoreMemoryUsage(TidStore *ts)` — the radix tree's current memory
/// footprint.
pub fn TidStoreMemoryUsage(ts: &TidStore) -> PgResult<usize> {
    radixtree_memory_usage::call(*ts)
}

/// `dsa_area *TidStoreGetDSA(TidStore *ts)` — the DSA area the shared store
/// lives in (its `dsa_handle`). Asserts the store is shared.
pub fn TidStoreGetDSA(ts: &TidStore) -> PgResult<DsaHandle> {
    radixtree_get_dsa::call(*ts)
}

/// `dsa_pointer TidStoreGetHandle(TidStore *ts)` — the handle by which another
/// backend can [`TidStoreAttach`] to the shared store. Asserts the store is
/// shared.
pub fn TidStoreGetHandle(ts: &TidStore) -> PgResult<DsaPointer> {
    radixtree_get_handle::call(*ts)
}

/// `int TidStoreGetBlockOffsets(TidStoreIterResult *result,
///                              OffsetNumber *offsets, int max_offsets)`.
///
/// Extract the offsets for the block described by `result` into `offsets`.
/// Returns the number filled in if `<= max_offsets`; otherwise fills as much as
/// fits and returns the size of the buffer that would be needed.
pub fn TidStoreGetBlockOffsets(
    result: &TidStoreIterResult,
    offsets: &mut [OffsetNumber],
) -> usize {
    result.page.offsets_into(offsets)
}

/// `OffsetNumber ItemPointerGetOffsetNumber(const ItemPointerData *)`
/// (`storage/itemptr.h`). Returns the offset half of the TID directly.
#[inline]
pub fn ItemPointerGetOffsetNumber(pointer: &ItemPointerData) -> OffsetNumber {
    pointer.ip_posid
}

/// Unpack a freshly-iterated entry into a [`ReapBlockInfo`] — the high-level
/// shape `vacuumlazy.c` consumes from `tidstore_iterate_next`. Sizes the
/// offsets vector to the entry's exact count (C's two-phase
/// `TidStoreGetBlockOffsets` measure-then-fill).
fn iter_result_to_reap(result: &TidStoreIterResult) -> PgResult<ReapBlockInfo> {
    let count = result.page.offsets_into(&mut []);
    let mut offsets: Vec<OffsetNumber> = Vec::new();
    offsets
        .try_reserve_exact(count)
        .map_err(|_| PgError::error("out of memory"))?;
    offsets.resize(count, InvalidOffsetNumber);
    let filled = result.page.offsets_into(&mut offsets);
    debug_assert_eq!(filled, count, "offset count stable across measure/fill");
    Ok(ReapBlockInfo {
        blkno: result.blkno,
        offsets,
    })
}

/// Install this crate's seams: the `access/tidstore.h` surface other crates
/// reach across a dependency cycle (declared in
/// `backend-access-common-tidstore-seams`). Each marshals to the in-crate
/// public function and delegates the radix substrate outward.
pub fn init_seams() {
    use tidstore_seams as v;

    v::tidstore_create_local::set(|max_bytes, insert_only| {
        TidStoreCreateLocal(max_bytes, insert_only)
    });
    v::tidstore_destroy::set(|ts| TidStoreDestroy(&ts));
    v::tidstore_set_block_offsets::set(|ts, blkno, offsets| {
        TidStoreSetBlockOffsets(&ts, blkno, &offsets)
    });
    v::tidstore_memory_usage::set(|ts| TidStoreMemoryUsage(&ts));
    v::tidstore_begin_iterate::set(|ts| {
        let iter = TidStoreBeginIterate(&ts)?;
        Ok(iter.iter_handle())
    });
    v::tidstore_iterate_next::set(|iter_handle| {
        let mut iter = TidStoreIter::from_iter_handle(iter_handle);
        match TidStoreIterateNext(&mut iter)? {
            Some(result) => Ok(Some(iter_result_to_reap(&result)?)),
            None => Ok(None),
        }
    });
    v::tidstore_end_iterate::set(|iter_handle| {
        TidStoreEndIterate(TidStoreIter::from_iter_handle(iter_handle))
    });
    v::tidstore_is_member::set(|ts, tid| TidStoreIsMember(&ts, &tid));
}

#[cfg(test)]
mod tests;
