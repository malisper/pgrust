//! Port of `src/backend/access/heap/visibilitymap.c` — the per-relation
//! visibility map (VM fork), a bitmap with two bits (all-visible, all-frozen)
//! per heap page recording, conservatively, whether every tuple on the page is
//! visible to all transactions and/or completely frozen.
//!
//! ## Structure
//!
//! 1:1 with `visibilitymap.c`: the public interface routines
//! (`visibilitymap_clear`, `visibilitymap_pin`, `visibilitymap_pin_ok`,
//! `visibilitymap_set`, `visibilitymap_get_status`, `visibilitymap_count`,
//! `visibilitymap_prepare_truncate`) and the static helpers (`vm_readbuf`,
//! `vm_extend`). The bit arithmetic, byte-level map manipulation, branch order,
//! critical-section bracketing, and WAL-decision logic match PostgreSQL 18.3
//! exactly.
//!
//! ## Seamed callees
//!
//! Like the sibling `freespace.c` port, the buffer manager owns the shared
//! page, so VM crosses the boundary by `Buffer` id rather than holding a `Page`
//! pointer:
//!
//!   * smgr fork geometry — `smgr_cached_nblocks` / `smgrexists` / `smgrnblocks`
//!     (`backend-storage-smgr-seams`);
//!   * buffer ops — `read_buffer_extended_vm` / `extend_buffered_rel_to_vm`
//!     (VM-fork analogs of the FSM-fork pair), `lock_buffer`,
//!     `mark_buffer_dirty`, `unlock_release_buffer`, `release_buffer`,
//!     `buffer_get_block_number`, `with_buffer_page`, `page_is_new`,
//!     `page_init`, `page_set_lsn` (`backend-storage-buffer-bufmgr-seams`);
//!   * `CacheInvalidateSmgr` (`backend-utils-cache-inval-seams`);
//!   * the WAL predicates `RelationNeedsWAL` (`backend-utils-cache-relcache-seams`),
//!     `XLogHintBitIsNeeded` / `InRecovery` (`backend-access-transam-xlog-seams`),
//!     and the WAL emitters `log_heap_visible` (`backend-access-heap-heapam-seams`)
//!     and `log_newpage_buffer` (`backend-access-transam-xloginsert-seams`);
//!   * the critical-section macros (`backend-utils-init-miscinit-seams`).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
// `PgResult`'s `Err` arm is fixed-size and large by project convention.
#![allow(clippy::result_large_err)]

use types_core::primitive::{
    BlockNumber, ForkNumber, InvalidBlockNumber, TransactionId, XLogRecPtr, BLCKSZ,
};
use types_error::{PgError, PgResult, ERROR};
use types_rel::{Relation, RelationData};
use types_storage::buf::{BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_UNLOCK};
use types_storage::{Buffer, BufferIsValid, InvalidBuffer, RelFileLocatorBackend};

use backend_utils_error::ereport;

use backend_access_heap_heapam_seams as heapam;
use backend_access_transam_xlog_seams as xlog;
use backend_access_transam_xloginsert_seams as xloginsert;
use backend_storage_buffer_bufmgr_seams as bufmgr;
use backend_storage_smgr_seams as smgr;
use backend_utils_cache_inval_seams as inval;
use backend_utils_cache_relcache_seams as relcache;
use backend_utils_init_miscinit_seams as miscadmin;

// ---------------------------------------------------------------------------
// Module-local constants, mirroring the `#define`s at the top of
// visibilitymap.c and `visibilitymapdefs.h`. These are caller-irrelevant
// layout constants.
// ---------------------------------------------------------------------------

/// `BITS_PER_BYTE` (c.h).
const BITS_PER_BYTE: u32 = 8;

/// `#define BITS_PER_HEAPBLOCK 2` (visibilitymapdefs.h).
const BITS_PER_HEAPBLOCK: u32 = 2;

/// `#define VISIBILITYMAP_ALL_VISIBLE 0x01` (visibilitymapdefs.h).
pub const VISIBILITYMAP_ALL_VISIBLE: u8 = 0x01;
/// `#define VISIBILITYMAP_ALL_FROZEN 0x02` (visibilitymapdefs.h).
pub const VISIBILITYMAP_ALL_FROZEN: u8 = 0x02;
/// `#define VISIBILITYMAP_VALID_BITS 0x03` (visibilitymapdefs.h) — OR of all
/// valid visibility-map bits.
pub const VISIBILITYMAP_VALID_BITS: u8 = 0x03;

/// `MAXALIGN(LEN)` — round `len` up to `MAXIMUM_ALIGNOF` (8), matching c.h.
const fn maxalign(len: usize) -> usize {
    const MAXIMUM_ALIGNOF: usize = 8;
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `SizeOfPageHeaderData` (storage/bufpage.h) — 24 bytes.
const SizeOfPageHeaderData: usize = 24;

/// `#define MAPSIZE (BLCKSZ - MAXALIGN(SizeOfPageHeaderData))` — bytes of bitmap
/// on each VM page (the whole page minus the standard header; no extra headers).
const MAPSIZE: u32 = (BLCKSZ - maxalign(SizeOfPageHeaderData)) as u32;

/// Byte offset of the bitmap within a page (`PageGetContents` start) — the
/// MAXALIGN'd page-header size. The map byte arithmetic indexes the page slice
/// beginning here.
const CONTENTS_OFF: usize = maxalign(SizeOfPageHeaderData);

/// `#define HEAPBLOCKS_PER_BYTE (BITS_PER_BYTE / BITS_PER_HEAPBLOCK)`.
const HEAPBLOCKS_PER_BYTE: u32 = BITS_PER_BYTE / BITS_PER_HEAPBLOCK;

/// `#define HEAPBLOCKS_PER_PAGE (MAPSIZE * HEAPBLOCKS_PER_BYTE)`.
const HEAPBLOCKS_PER_PAGE: u32 = MAPSIZE * HEAPBLOCKS_PER_BYTE;

/// `#define VISIBLE_MASK8 (0x55)` — the lower bit of each bit pair.
const VISIBLE_MASK8: u8 = 0x55;
/// `#define FROZEN_MASK8 (0xaa)` — the upper bit of each bit pair.
const FROZEN_MASK8: u8 = 0xaa;

/// `#define HEAPBLK_TO_MAPBLOCK(x) ((x) / HEAPBLOCKS_PER_PAGE)`.
#[inline]
fn HEAPBLK_TO_MAPBLOCK(x: BlockNumber) -> BlockNumber {
    x / HEAPBLOCKS_PER_PAGE
}

/// `#define HEAPBLK_TO_MAPBYTE(x) (((x) % HEAPBLOCKS_PER_PAGE) / HEAPBLOCKS_PER_BYTE)`.
#[inline]
fn HEAPBLK_TO_MAPBYTE(x: BlockNumber) -> u32 {
    (x % HEAPBLOCKS_PER_PAGE) / HEAPBLOCKS_PER_BYTE
}

/// `#define HEAPBLK_TO_OFFSET(x) (((x) % HEAPBLOCKS_PER_BYTE) * BITS_PER_HEAPBLOCK)`.
#[inline]
fn HEAPBLK_TO_OFFSET(x: BlockNumber) -> u32 {
    (x % HEAPBLOCKS_PER_BYTE) * BITS_PER_HEAPBLOCK
}

/// `XLogRecPtrIsInvalid(r)` — true for `InvalidXLogRecPtr` (== 0, xlogdefs.h).
#[inline]
fn XLogRecPtrIsInvalid(recptr: XLogRecPtr) -> bool {
    recptr == 0
}

/// The physical address (`smgr_rlocator`) a VM buffer read targets.
#[inline]
fn rel_locator_backend(rel: &RelationData) -> RelFileLocatorBackend {
    RelFileLocatorBackend {
        locator: rel.rd_locator,
        backend: rel.rd_backend,
    }
}

/// `elog(ERROR, message)` for the visibility-map internal-consistency checks,
/// materialized as a `PgError` (PG uses `elog`, i.e. an `errmsg_internal`
/// with the default internal sqlstate, for these "wrong buffer passed" /
/// bounds invariants).
#[inline]
fn vm_error(message: &'static str) -> PgError {
    ereport(ERROR).errmsg_internal(message).into_error()
}

/// `pg_popcount_masked(buf, bytes, mask)` (port/pg_bitutils.h) — population
/// count of `buf` after masking each byte with `mask`.
fn pg_popcount_masked(buf: &[u8], mask: u8) -> u64 {
    let mut popcnt: u64 = 0;
    for &b in buf {
        popcnt += (b & mask).count_ones() as u64;
    }
    popcnt
}

// ---------------------------------------------------------------------------
// Page-map byte access — index the `PageGetContents` region of the page bytes.
//
// `PageGetContents(page)` == `page + MAXALIGN(SizeOfPageHeaderData)`; the map
// bytes are `contents[mapByte]`. The "the bit arithmetic guarantees it" C
// reasoning is promoted to real bounds checks (`mapByte < MAPSIZE`).
// ---------------------------------------------------------------------------

/// `&map[mapByte]` (read) over the full page bytes.
#[inline]
fn map_byte(bytes: &[u8], mapByte: usize) -> PgResult<u8> {
    bytes
        .get(CONTENTS_OFF + mapByte)
        .copied()
        .ok_or_else(|| vm_error("vm map byte out of range"))
}

/// `&map[mapByte]` (write) over the full page bytes.
#[inline]
fn map_byte_mut(bytes: &mut [u8], mapByte: usize) -> PgResult<&mut u8> {
    bytes
        .get_mut(CONTENTS_OFF + mapByte)
        .ok_or_else(|| vm_error("vm map byte out of range"))
}

// ---------------------------------------------------------------------------
// Internal routines: vm_readbuf / vm_extend.
// ---------------------------------------------------------------------------

/// `vm_readbuf` — read a visibility map page.
///
/// If the page doesn't exist, `InvalidBuffer` is returned, or if `extend` is
/// true, the visibility map file is extended.
fn vm_readbuf(rel: &Relation<'_>, blkno: BlockNumber, extend: bool) -> PgResult<Buffer> {
    let buf: Buffer;

    // If we haven't cached the size of the visibility map fork yet, check it
    // first. (In C this inspects/populates
    // reln->smgr_cached_nblocks[VISIBILITYMAP_FORKNUM]; the smgr cache lives in
    // the SMgrRelation, so this re-derivation runs over the smgr seams:
    // `smgrexists ? smgrnblocks : 0`.)
    let nblocks = vm_cached_nblocks(rel)?;

    // For reading we use ZERO_ON_ERROR mode, and initialize the page if
    // necessary. It's always safe to clear bits, so it's better to clear corrupt
    // pages than error out.
    //
    // We use the same path below to initialize pages when extending the
    // relation, as a concurrent extension can end up with vm_extend() returning
    // an already-initialized page.
    if blkno >= nblocks {
        if extend {
            buf = vm_extend(rel, blkno + 1)?;
        } else {
            return Ok(InvalidBuffer);
        }
    } else {
        buf = bufmgr::read_buffer_extended_vm::call(rel, blkno)?;
    }

    // Initializing the page when needed is trickier than it looks, because of
    // the possibility of multiple backends doing this concurrently, and our
    // desire to not uselessly take the buffer lock in the normal path where the
    // page is OK. We must take the lock to initialize the page, so recheck page
    // newness after we have the lock, in case someone else already did it.
    if bufmgr::page_is_new::call(buf)? {
        bufmgr::lock_buffer::call(buf, BUFFER_LOCK_EXCLUSIVE)?;
        if bufmgr::page_is_new::call(buf)? {
            bufmgr::page_init::call(buf)?;
        }
        bufmgr::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
    }
    Ok(buf)
}

/// `RelationGetSmgr(rel)->smgr_cached_nblocks[VISIBILITYMAP_FORKNUM]` from
/// `vm_readbuf`: when the count is not cached, re-derive it from
/// `smgrexists ? smgrnblocks : 0`. Returns the effective VM-fork block count to
/// compare `blkno` against.
fn vm_cached_nblocks(rel: &RelationData) -> PgResult<BlockNumber> {
    let rlocator = rel.rd_locator;
    let backend = rel.rd_backend;

    let cached = smgr::smgr_cached_nblocks::call(rlocator, backend, ForkNumber::VISIBILITYMAP_FORKNUM);
    if cached == InvalidBlockNumber {
        if smgr::smgrexists::call(rlocator, backend, ForkNumber::VISIBILITYMAP_FORKNUM)? {
            return smgr::smgrnblocks::call(rlocator, backend, ForkNumber::VISIBILITYMAP_FORKNUM);
        }
        return Ok(0);
    }
    Ok(cached)
}

/// `vm_extend` — ensure that the visibility map fork is at least `vm_nblocks`
/// long, extending it if necessary with zeroed pages.
fn vm_extend(rel: &Relation<'_>, vm_nblocks: BlockNumber) -> PgResult<Buffer> {
    // ExtendBufferedRelTo(BMR_REL(rel), VISIBILITYMAP_FORKNUM, NULL,
    //                     EB_CREATE_FORK_IF_NEEDED | EB_CLEAR_SIZE_CACHE,
    //                     vm_nblocks, RBM_ZERO_ON_ERROR);
    let buf = bufmgr::extend_buffered_rel_to_vm::call(rel, vm_nblocks)?;

    // Send a shared-inval message to force other backends to close any smgr
    // references they may have for this rel, which we are about to change.
    inval::cache_invalidate_smgr::call(rel_locator_backend(rel))?;

    Ok(buf)
}

// ---------------------------------------------------------------------------
// Public interface routines.
// ---------------------------------------------------------------------------

/// `visibilitymap_clear` — clear specified bits for one page in visibility map.
///
/// You must pass a buffer containing the correct map page to this function.
/// Call [`visibilitymap_pin`] first to pin the right one. This function doesn't
/// do any I/O. Returns true if any bits have been cleared and false otherwise.
pub fn visibilitymap_clear(
    _rel: &Relation<'_>,
    heapBlk: BlockNumber,
    vmbuf: Buffer,
    flags: u8,
) -> PgResult<bool> {
    let mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);
    let mapByte = HEAPBLK_TO_MAPBYTE(heapBlk) as usize;
    let mapOffset = HEAPBLK_TO_OFFSET(heapBlk);
    let mask: u8 = flags << mapOffset;
    let mut cleared = false;

    // Must never clear all_visible bit while leaving all_frozen bit set.
    debug_assert!(flags & VISIBILITYMAP_VALID_BITS != 0);
    debug_assert!(flags != VISIBILITYMAP_ALL_VISIBLE);

    if !BufferIsValid(vmbuf) || bufmgr::buffer_get_block_number::call(vmbuf) != mapBlock {
        return Err(vm_error("wrong buffer passed to visibilitymap_clear"));
    }

    bufmgr::lock_buffer::call(vmbuf, BUFFER_LOCK_EXCLUSIVE)?;

    // map = PageGetContents(BufferGetPage(vmbuf)); if (map[mapByte] & mask) ...
    bufmgr::with_buffer_page::call(vmbuf, &mut |bytes| {
        let cell = map_byte_mut(bytes, mapByte)?;
        if *cell & mask != 0 {
            *cell &= !mask;
            cleared = true;
        }
        Ok(())
    })?;

    if cleared {
        bufmgr::mark_buffer_dirty::call(vmbuf);
    }

    bufmgr::lock_buffer::call(vmbuf, BUFFER_LOCK_UNLOCK)?;

    Ok(cleared)
}

/// `visibilitymap_pin` — pin a map page for setting a bit.
///
/// On entry, `*vmbuf` should be `InvalidBuffer` (0) or a valid buffer returned
/// by an earlier call to [`visibilitymap_pin`] or [`visibilitymap_get_status`]
/// on the same relation. On return, `*vmbuf` is a valid buffer with the map
/// page containing the bit for `heapBlk`. If the page doesn't exist in the map
/// file yet, it is extended.
pub fn visibilitymap_pin(
    rel: &Relation<'_>,
    heapBlk: BlockNumber,
    vmbuf: &mut Buffer,
) -> PgResult<()> {
    let mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);

    // Reuse the old pinned buffer if possible.
    if BufferIsValid(*vmbuf) {
        if bufmgr::buffer_get_block_number::call(*vmbuf) == mapBlock {
            return Ok(());
        }

        bufmgr::release_buffer::call(*vmbuf);
    }
    *vmbuf = vm_readbuf(rel, mapBlock, true)?;
    Ok(())
}

/// `visibilitymap_pin_ok` — do we already have the correct page pinned?
///
/// On entry, `vmbuf` should be `InvalidBuffer` (0) or a valid buffer returned by
/// an earlier call to [`visibilitymap_pin`] or [`visibilitymap_get_status`] on
/// the same relation. The return value indicates whether the buffer covers the
/// given `heapBlk`.
pub fn visibilitymap_pin_ok(heapBlk: BlockNumber, vmbuf: Buffer) -> bool {
    let mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);

    BufferIsValid(vmbuf) && bufmgr::buffer_get_block_number::call(vmbuf) == mapBlock
}

/// `visibilitymap_set` — set bit(s) on a previously pinned page.
///
/// `recptr` is the LSN of the XLOG record we're replaying, if we're in
/// recovery, or `InvalidXLogRecPtr` in normal running. The VM page LSN is
/// advanced to the one provided; in normal running, we generate a new XLOG
/// record and set the page LSN to that value. `cutoff_xid` is the largest xmin
/// on the page being marked all-visible.
///
/// You must pass a buffer containing the correct map page to this function.
/// Returns the state of the page's VM bits before setting `flags`.
pub fn visibilitymap_set(
    rel: &Relation<'_>,
    heapBlk: BlockNumber,
    heapBuf: Buffer,
    mut recptr: XLogRecPtr,
    vmBuf: Buffer,
    cutoff_xid: TransactionId,
    flags: u8,
) -> PgResult<u8> {
    let mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);
    let mapByte = HEAPBLK_TO_MAPBYTE(heapBlk) as usize;
    let mapOffset = HEAPBLK_TO_OFFSET(heapBlk);

    // Assert(InRecovery || XLogRecPtrIsInvalid(recptr)); — recovery state is
    // external; the assert's only effect is a debug-build abort.
    // Assert(InRecovery || PageIsAllVisible(BufferGetPage(heapBuf))); — the
    // caller is responsible for having set PD_ALL_VISIBLE before calling.
    // Assert((flags & VISIBILITYMAP_VALID_BITS) == flags);
    debug_assert!((flags & VISIBILITYMAP_VALID_BITS) == flags);
    // Must never set all_frozen bit without also setting all_visible bit.
    debug_assert!(flags != VISIBILITYMAP_ALL_FROZEN);

    // Check that we have the right heap page pinned, if present.
    if BufferIsValid(heapBuf) && bufmgr::buffer_get_block_number::call(heapBuf) != heapBlk {
        return Err(vm_error("wrong heap buffer passed to visibilitymap_set"));
    }

    // Check that we have the right VM page pinned.
    if !BufferIsValid(vmBuf) || bufmgr::buffer_get_block_number::call(vmBuf) != mapBlock {
        return Err(vm_error("wrong VM buffer passed to visibilitymap_set"));
    }

    bufmgr::lock_buffer::call(vmBuf, BUFFER_LOCK_EXCLUSIVE)?;

    // status = (map[mapByte] >> mapOffset) & VISIBILITYMAP_VALID_BITS;
    let mut status: u8 = 0;
    bufmgr::with_buffer_page::call(vmBuf, &mut |bytes| {
        status = (map_byte(bytes, mapByte)? >> mapOffset) & VISIBILITYMAP_VALID_BITS;
        Ok(())
    })?;

    if flags != status {
        // START_CRIT_SECTION();
        miscadmin::start_crit_section::call();

        // map[mapByte] |= (flags << mapOffset);
        bufmgr::with_buffer_page::call(vmBuf, &mut |bytes| {
            let cell = map_byte_mut(bytes, mapByte)?;
            *cell |= flags << mapOffset;
            Ok(())
        })?;
        bufmgr::mark_buffer_dirty::call(vmBuf);

        if relcache::relation_needs_wal::call(rel) {
            if XLogRecPtrIsInvalid(recptr) {
                // Assert(!InRecovery);
                recptr = heapam::log_heap_visible::call(rel, heapBuf, vmBuf, cutoff_xid, flags)?;

                // If data checksums are enabled (or wal_log_hints=on), we need
                // to protect the heap page from being torn. If not, then we must
                // *not* update the heap page's LSN: the FPI for the heap page was
                // omitted from the WAL record inserted above, so it would be
                // incorrect to update the heap page's LSN.
                if xlog::xlog_hint_bit_is_needed::call() {
                    bufmgr::page_set_lsn::call(heapBuf, recptr)?;
                }
            }
            bufmgr::page_set_lsn::call(vmBuf, recptr)?;
        }

        // END_CRIT_SECTION();
        miscadmin::end_crit_section::call();
    }

    bufmgr::lock_buffer::call(vmBuf, BUFFER_LOCK_UNLOCK)?;
    Ok(status)
}

/// `visibilitymap_get_status` — get status of bits.
///
/// Are all tuples on `heapBlk` visible to all or are marked frozen, according
/// to the visibility map?
///
/// On entry, `*vmbuf` should be `InvalidBuffer` (0) or a valid buffer returned
/// by an earlier call to [`visibilitymap_pin`] or [`visibilitymap_get_status`]
/// on the same relation. On return, `*vmbuf` is a valid buffer with the map
/// page containing the bit for `heapBlk`, or `InvalidBuffer` (0).
pub fn visibilitymap_get_status(
    rel: &Relation<'_>,
    heapBlk: BlockNumber,
    vmbuf: &mut Buffer,
) -> PgResult<u8> {
    let mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);
    let mapByte = HEAPBLK_TO_MAPBYTE(heapBlk) as usize;
    let mapOffset = HEAPBLK_TO_OFFSET(heapBlk);

    // Reuse the old pinned buffer if possible.
    if BufferIsValid(*vmbuf) && bufmgr::buffer_get_block_number::call(*vmbuf) != mapBlock {
        bufmgr::release_buffer::call(*vmbuf);
        *vmbuf = InvalidBuffer;
    }

    if !BufferIsValid(*vmbuf) {
        *vmbuf = vm_readbuf(rel, mapBlock, false)?;
        if !BufferIsValid(*vmbuf) {
            return Ok(0);
        }
    }

    // A single byte read is atomic. There could be memory-ordering effects
    // here, but for performance reasons we make it the caller's job to worry
    // about that.
    //
    // result = (map[mapByte] >> mapOffset) & VISIBILITYMAP_VALID_BITS;
    let mut result: u8 = 0;
    bufmgr::with_buffer_page::call(*vmbuf, &mut |bytes| {
        result = (map_byte(bytes, mapByte)? >> mapOffset) & VISIBILITYMAP_VALID_BITS;
        Ok(())
    })?;
    Ok(result)
}

/// `visibilitymap_count` — count number of bits set in visibility map.
///
/// Returns `(all_visible, all_frozen)`. The C function takes a nullable
/// `all_frozen` out-parameter; here it is always computed (the only caller that
/// passed `NULL` for it ignores the second tuple element).
pub fn visibilitymap_count(rel: &Relation<'_>) -> PgResult<(BlockNumber, BlockNumber)> {
    let mut nvisible: BlockNumber = 0;
    let mut nfrozen: BlockNumber = 0;

    let mut mapBlock: BlockNumber = 0;
    loop {
        // Read till we fall off the end of the map. We assume that any extra
        // bytes in the last page are zeroed, so we don't bother excluding them
        // from the count.
        let mapBuffer = vm_readbuf(rel, mapBlock, false)?;
        if !BufferIsValid(mapBuffer) {
            break;
        }

        // We choose not to lock the page, since the result is going to be
        // immediately stale anyway if anyone is concurrently setting or clearing
        // bits, and we only really need an approximate value.
        let mut vis: u64 = 0;
        let mut frz: u64 = 0;
        bufmgr::with_buffer_page::call(mapBuffer, &mut |bytes| {
            let map = bytes
                .get(CONTENTS_OFF..CONTENTS_OFF + MAPSIZE as usize)
                .ok_or_else(|| vm_error("vm page too small"))?;
            vis = pg_popcount_masked(map, VISIBLE_MASK8);
            frz = pg_popcount_masked(map, FROZEN_MASK8);
            Ok(())
        })?;

        nvisible = nvisible.wrapping_add(vis as BlockNumber);
        nfrozen = nfrozen.wrapping_add(frz as BlockNumber);

        bufmgr::release_buffer::call(mapBuffer);
        mapBlock += 1;
    }

    Ok((nvisible, nfrozen))
}

/// `visibilitymap_prepare_truncate` — prepare for truncation of the visibility
/// map.
///
/// `nheapblocks` is the new size of the heap. Returns the number of blocks of
/// the new visibility map. If it's `InvalidBlockNumber`, there is nothing to
/// truncate; otherwise the caller is responsible for calling `smgrtruncate()`
/// to truncate the visibility map pages.
pub fn visibilitymap_prepare_truncate(
    rel: &Relation<'_>,
    nheapblocks: BlockNumber,
) -> PgResult<BlockNumber> {
    let newnblocks: BlockNumber;

    // last remaining block, byte, and bit
    let truncBlock = HEAPBLK_TO_MAPBLOCK(nheapblocks);
    let truncByte = HEAPBLK_TO_MAPBYTE(nheapblocks) as usize;
    let truncOffset = HEAPBLK_TO_OFFSET(nheapblocks);

    // If no visibility map has been created yet for this relation, there's
    // nothing to truncate.
    if !smgr::smgrexists::call(rel.rd_locator, rel.rd_backend, ForkNumber::VISIBILITYMAP_FORKNUM)? {
        return Ok(InvalidBlockNumber);
    }

    // Unless the new size is exactly at a visibility map page boundary, the tail
    // bits in the last remaining map page, representing truncated heap blocks,
    // need to be cleared. This is not only tidy, but also necessary because we
    // don't get a chance to clear the bits if the heap is extended again.
    if truncByte != 0 || truncOffset != 0 {
        newnblocks = truncBlock + 1;

        let mapBuffer = vm_readbuf(rel, truncBlock, false)?;
        if !BufferIsValid(mapBuffer) {
            // nothing to do, the file was already smaller
            return Ok(InvalidBlockNumber);
        }

        bufmgr::lock_buffer::call(mapBuffer, BUFFER_LOCK_EXCLUSIVE)?;

        // NO EREPORT(ERROR) from here till changes are logged.
        miscadmin::start_crit_section::call();

        bufmgr::with_buffer_page::call(mapBuffer, &mut |bytes| {
            // Clear out the unwanted bytes:
            //   MemSet(&map[truncByte + 1], 0, MAPSIZE - (truncByte + 1));
            bytes
                .get_mut(CONTENTS_OFF + truncByte + 1..CONTENTS_OFF + MAPSIZE as usize)
                .ok_or_else(|| vm_error("vm truncate range"))?
                .fill(0);

            // Mask out the unwanted bits of the last remaining byte:
            //   map[truncByte] &= (1 << truncOffset) - 1;
            let cell = map_byte_mut(bytes, truncByte)?;
            *cell &= ((1u32 << truncOffset) - 1) as u8;
            Ok(())
        })?;

        // Truncation of a relation is WAL-logged at a higher-level, and we will
        // be called at WAL replay. But if checksums are enabled, we need to
        // still write a WAL record to protect against a torn page, if the page
        // is flushed to disk before the truncation WAL record. We cannot use
        // MarkBufferDirtyHint here, because that will not dirty the page during
        // recovery.
        bufmgr::mark_buffer_dirty::call(mapBuffer);
        if !xlog::in_recovery::call()
            && relcache::relation_needs_wal::call(rel)
            && xlog::xlog_hint_bit_is_needed::call()
        {
            xloginsert::log_newpage_buffer::call(mapBuffer, false)?;
        }

        // END_CRIT_SECTION();
        miscadmin::end_crit_section::call();

        bufmgr::unlock_release_buffer::call(mapBuffer);
    } else {
        newnblocks = truncBlock;
    }

    if smgr::smgrnblocks::call(rel.rd_locator, rel.rd_backend, ForkNumber::VISIBILITYMAP_FORKNUM)?
        <= newnblocks
    {
        // nothing to do, the file was already smaller than requested size
        return Ok(InvalidBlockNumber);
    }

    Ok(newnblocks)
}

// ---------------------------------------------------------------------------
// Seam installation — the inward visibility-map seam this unit owns
// (`backend-access-heap-visibilitymap-seams`).
//
// The five VM seams homed in `backend-access-heap-vacuumlazy-seams`
// (visibilitymap_count/get_status/pin/set/clear) are now `&Relation<'mcx>`-keyed
// (the vacuumlazy-mcx keystone re-signed them off bare `Oid`). They are this
// unit's true inward surface — the heap-vacuum driver, heapam, and
// catalog-indexing all reach the VM through them — so this owner installs them
// here by delegating straight to the in-crate implementations.
// ---------------------------------------------------------------------------

/// Install every seam declared in `backend-access-heap-visibilitymap-seams` to
/// the real implementations in this crate.
pub fn init_seams() {
    backend_access_heap_visibilitymap_seams::visibilitymap_get_status::set(
        |rel, heap_blk, vmbuf_in| {
            let mut vmbuf = vmbuf_in;
            let status = visibilitymap_get_status(&rel, heap_blk, &mut vmbuf)?;
            Ok((status, vmbuf))
        },
    );
    backend_access_heap_visibilitymap_seams::visibilitymap_pin::set(
        |rel, heap_blk, vmbuf_in| {
            let mut vmbuf = vmbuf_in;
            visibilitymap_pin(&rel, heap_blk, &mut vmbuf)?;
            Ok(vmbuf)
        },
    );
    backend_access_heap_visibilitymap_seams::visibilitymap_pin_ok::set(|heap_blk, vmbuf| {
        visibilitymap_pin_ok(heap_blk, vmbuf)
    });

    // The five `&Relation<'mcx>`-keyed VM seams in vacuumlazy-seams.
    backend_access_heap_vacuumlazy_seams::visibilitymap_count::set(|rel| {
        visibilitymap_count(rel)
    });
    backend_access_heap_vacuumlazy_seams::visibilitymap_get_status::set(|rel, heap_blk, vmbuf_in| {
        let mut vmbuf = vmbuf_in;
        let status = visibilitymap_get_status(rel, heap_blk, &mut vmbuf)?;
        Ok((status, vmbuf))
    });
    backend_access_heap_vacuumlazy_seams::visibilitymap_pin::set(|rel, heap_blk, vmbuf_in| {
        let mut vmbuf = vmbuf_in;
        visibilitymap_pin(rel, heap_blk, &mut vmbuf)?;
        Ok(vmbuf)
    });
    backend_access_heap_vacuumlazy_seams::visibilitymap_set::set(|rel, args| {
        visibilitymap_set(
            rel,
            args.heap_blk,
            args.heap_buf,
            args.rec_ptr,
            args.vm_buf,
            args.cutoff_xid,
            args.flags,
        )
    });
    backend_access_heap_vacuumlazy_seams::visibilitymap_clear::set(|rel, heap_blk, vmbuf, flags| {
        visibilitymap_clear(rel, heap_blk, vmbuf, flags)
    });
}

#[cfg(test)]
mod tests;
