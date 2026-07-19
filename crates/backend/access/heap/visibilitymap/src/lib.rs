//! visibilitymap.c. Read lane (get_status/pin/pin_ok/count) and write lane
//! (set/clear/prepare_truncate); vm_extend still reaches bufmgr's
//! ExtendBufferedRelTo phase-2 panic until the extend machinery lands.

#![allow(non_snake_case)]

use ::bufmgr_seams::BufferPin;
use ::types_core::{
    BlockNumber, Buffer, BufferIsValid, ForkNumber, InvalidBlockNumber, InvalidXLogRecPtr,
    TransactionId, XLogRecPtr, BLCKSZ,
};
use ::types_error::PgResult;
use ::types_rel::RelationData;
use ::types_storage::bufpage::{PageMut, PageRef, SizeOfPageHeaderData};
use ::types_storage::ReadBufferMode;
use ::xloginsert_seams::{XLogRegBuf, REGBUF_NO_IMAGE, REGBUF_STANDARD};

pub const VISIBILITYMAP_ALL_VISIBLE: u8 = 0x01;
pub const VISIBILITYMAP_ALL_FROZEN: u8 = 0x02;
pub const VISIBILITYMAP_VALID_BITS: u8 = 0x03;
// visibilitymapdefs.h: flag in xl_heap_visible only, never in the map itself.
pub const VISIBILITYMAP_XLOG_CATALOG_REL: u8 = 0x04;

const BITS_PER_HEAPBLOCK: u32 = 2;
const CONTENTS_OFF: usize = (SizeOfPageHeaderData + 7) & !7;
const MAPSIZE: u32 = (BLCKSZ - CONTENTS_OFF) as u32;
const HEAPBLOCKS_PER_BYTE: u32 = 8 / BITS_PER_HEAPBLOCK;
const HEAPBLOCKS_PER_PAGE: u32 = MAPSIZE * HEAPBLOCKS_PER_BYTE;
const VISIBLE_MASK8: u8 = 0x55;
const FROZEN_MASK8: u8 = 0xaa;

#[inline(always)]
fn HEAPBLK_TO_MAPBLOCK(x: BlockNumber) -> BlockNumber {
    x / HEAPBLOCKS_PER_PAGE
}

#[inline(always)]
fn HEAPBLK_TO_MAPBYTE(x: BlockNumber) -> u32 {
    (x % HEAPBLOCKS_PER_PAGE) / HEAPBLOCKS_PER_BYTE
}

#[inline(always)]
fn HEAPBLK_TO_OFFSET(x: BlockNumber) -> u32 {
    (x % HEAPBLOCKS_PER_BYTE) * BITS_PER_HEAPBLOCK
}

/// C's `Buffer *vmbuf` carrier. `map_block` caches BufferGetBlockNumber(pin)
/// (the pin fixes the mapping), so the repeat-probe path is compare + load.
#[derive(Debug, Default)]
pub struct VmBuffer {
    pin: Option<BufferPin>,
    map_block: BlockNumber,
}

impl VmBuffer {
    #[inline]
    pub const fn new() -> VmBuffer {
        VmBuffer { pin: None, map_block: 0 }
    }

    #[inline]
    pub fn is_valid(&self) -> bool {
        self.pin.is_some()
    }

    /// `ReleaseBuffer(vmbuffer)` at scan end / vacuum page switch.
    #[inline]
    pub fn release(&mut self) {
        if let Some(pin) = self.pin.take() {
            pin.release();
        }
    }

    #[inline]
    pub fn buffer(&self) -> Buffer {
        self.pin.as_ref().map_or(0, BufferPin::buffer)
    }
}

#[inline(always)]
fn status_from_page(page: PageRef<'_>, heapBlk: BlockNumber) -> u8 {
    let mapByte = HEAPBLK_TO_MAPBYTE(heapBlk) as usize;
    let mapOffset = HEAPBLK_TO_OFFSET(heapBlk);
    // SAFETY: mapByte < MAPSIZE by the mod arithmetic; page live for the pin-scoped borrow.
    let byte = unsafe { *page.as_ptr().add(CONTENTS_OFF + mapByte) };
    (byte >> mapOffset) & VISIBILITYMAP_VALID_BITS
}

/// `visibilitymap_get_status`. Concurrency caveats are the caller's, as in C.
#[inline(always)]
pub fn visibilitymap_get_status(
    rel: &RelationData<'_>,
    heapBlk: BlockNumber,
    vmbuf: &mut VmBuffer,
) -> PgResult<u8> {
    let mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);
    match &vmbuf.pin {
        Some(pin) if vmbuf.map_block == mapBlock => Ok(status_from_page(pin.page(), heapBlk)),
        _ => vm_status_switch(rel, heapBlk, mapBlock, vmbuf),
    }
}

#[cold]
#[inline(never)]
fn vm_status_switch(
    rel: &RelationData<'_>,
    heapBlk: BlockNumber,
    mapBlock: BlockNumber,
    vmbuf: &mut VmBuffer,
) -> PgResult<u8> {
    if let Some(pin) = vmbuf.pin.take() {
        pin.release();
    }
    let Some(pin) = vm_readbuf(rel, mapBlock, false)? else {
        return Ok(0);
    };
    let status = status_from_page(pin.page(), heapBlk);
    vmbuf.pin = Some(pin);
    vmbuf.map_block = mapBlock;
    Ok(status)
}

/// `VM_ALL_VISIBLE` (visibilitymap.h).
#[inline(always)]
pub fn vm_all_visible(
    rel: &RelationData<'_>,
    heapBlk: BlockNumber,
    vmbuf: &mut VmBuffer,
) -> PgResult<bool> {
    Ok(visibilitymap_get_status(rel, heapBlk, vmbuf)? & VISIBILITYMAP_ALL_VISIBLE != 0)
}

/// `VM_ALL_FROZEN` (visibilitymap.h).
#[inline(always)]
pub fn vm_all_frozen(
    rel: &RelationData<'_>,
    heapBlk: BlockNumber,
    vmbuf: &mut VmBuffer,
) -> PgResult<bool> {
    Ok(visibilitymap_get_status(rel, heapBlk, vmbuf)? & VISIBILITYMAP_ALL_FROZEN != 0)
}

/// `visibilitymap_pin`; extends the VM fork if the map page doesn't exist yet
/// (that arm reaches bufmgr's ExtendBufferedRelTo phase-2 panic).
pub fn visibilitymap_pin(
    rel: &RelationData<'_>,
    heapBlk: BlockNumber,
    vmbuf: &mut VmBuffer,
) -> PgResult<()> {
    let mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);
    if let Some(pin) = vmbuf.pin.take() {
        if vmbuf.map_block == mapBlock {
            vmbuf.pin = Some(pin);
            return Ok(());
        }
        pin.release();
    }
    let pin = vm_readbuf(rel, mapBlock, true)?;
    debug_assert!(pin.is_some());
    vmbuf.pin = pin;
    vmbuf.map_block = mapBlock;
    Ok(())
}

/// `visibilitymap_pin_ok`.
#[inline]
pub fn visibilitymap_pin_ok(heapBlk: BlockNumber, vmbuf: &VmBuffer) -> bool {
    vmbuf.pin.is_some() && vmbuf.map_block == HEAPBLK_TO_MAPBLOCK(heapBlk)
}

/// `visibilitymap_count` -> (all_visible, all_frozen); C's nullable
/// `all_frozen` out-param is always computed (its one NULL caller ignores it).
pub fn visibilitymap_count(rel: &RelationData<'_>) -> PgResult<(BlockNumber, BlockNumber)> {
    let mut nvisible: u64 = 0;
    let mut nfrozen: u64 = 0;
    let mut mapBlock: BlockNumber = 0;
    loop {
        let Some(pin) = vm_readbuf(rel, mapBlock, false)? else {
            break;
        };
        let page = pin.page();
        // SAFETY: CONTENTS_OFF..CONTENTS_OFF+MAPSIZE is in-page; live while
        // pinned. Unlocked read, as C (approximate result by design).
        let map = unsafe {
            core::slice::from_raw_parts(page.as_ptr().add(CONTENTS_OFF), MAPSIZE as usize)
        };
        nvisible += pg_bitutils::pg_popcount_masked(map, VISIBLE_MASK8);
        nfrozen += pg_bitutils::pg_popcount_masked(map, FROZEN_MASK8);
        pin.release();
        mapBlock += 1;
    }
    Ok((nvisible as BlockNumber, nfrozen as BlockNumber))
}

// XLogHintBitIsNeeded() (xlog.h).
pub fn xlog_hint_bit_is_needed() -> bool {
    transam_xlog_seams::data_checksums_enabled::call() || guc_tables::vars::wal_log_hints.read()
}

// RelationNeedsWAL (rel.h), including the wal_level=minimal skip-WAL clause.
fn relation_needs_wal(rel: &RelationData<'_>) -> bool {
    rel.is_permanent()
        && (transam_xlog_seams::xlog_standby_info_active::call()
            || (rel.rd_createSubid.get() == types_core::InvalidSubTransactionId
                && rel.rd_firstRelfilelocatorSubid.get() == types_core::InvalidSubTransactionId))
}

const XLOG_HEAP2_VISIBLE: u8 = 0x40;
const RM_HEAP2_ID: u8 = types_core::RmgrIds::RM_HEAP2_ID as u8;

// RelationIsAccessibleInLogicalDecoding (rel.h): a standby uses the
// VISIBILITYMAP_XLOG_CATALOG_REL bit to invalidate logical slots whose
// catalog_xmin this all-visible cutoff overtook.
fn relation_is_accessible_in_logical_decoding(rel: &RelationData<'_>) -> bool {
    transam_xlog_seams::xlog_logical_info_active::call()
        && relation_needs_wal(rel)
        && (catalog_seams::is_catalog_relation::call(rel) || rel.is_used_as_catalog_table())
}

// log_heap_visible (heapam.c).
fn log_heap_visible(
    rel: &RelationData<'_>,
    heap_buffer: Buffer,
    vm_buffer: Buffer,
    snapshot_conflict_horizon: TransactionId,
    vmflags: u8,
) -> PgResult<XLogRecPtr> {
    debug_assert!(BufferIsValid(heap_buffer));
    debug_assert!(BufferIsValid(vm_buffer));

    let mut flags = vmflags;
    if relation_is_accessible_in_logical_decoding(rel) {
        flags |= VISIBILITYMAP_XLOG_CATALOG_REL;
    }

    let mut xlrec = [0u8; 5];
    xlrec[0..4].copy_from_slice(&snapshot_conflict_horizon.to_ne_bytes());
    xlrec[4] = flags;

    let mut heap_flags = REGBUF_STANDARD;
    if !xlog_hint_bit_is_needed() {
        heap_flags |= REGBUF_NO_IMAGE;
    }
    xloginsert_seams::xlog_insert_record::call(
        RM_HEAP2_ID,
        XLOG_HEAP2_VISIBLE,
        0,
        &[&xlrec],
        &[
            XLogRegBuf { block_id: 0, buffer: vm_buffer, flags: 0, bufdata: &[] },
            XLogRegBuf { block_id: 1, buffer: heap_buffer, flags: heap_flags, bufdata: &[] },
        ],
    )
}

/// `visibilitymap_set` -> the page's VM bits before `flags` were set.
pub fn visibilitymap_set(
    rel: &RelationData<'_>,
    heapBlk: BlockNumber,
    heapBuf: Buffer,
    recptr: XLogRecPtr,
    vmbuf: &VmBuffer,
    cutoff_xid: TransactionId,
    flags: u8,
) -> PgResult<u8> {
    let mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);
    let mapByte = HEAPBLK_TO_MAPBYTE(heapBlk) as usize;
    let mapOffset = HEAPBLK_TO_OFFSET(heapBlk);

    let in_recovery = xlogutils_seams::in_recovery::call();
    debug_assert!(in_recovery || recptr == InvalidXLogRecPtr);
    debug_assert!(in_recovery || heap_page(heapBuf).is_all_visible());
    debug_assert!((flags & VISIBILITYMAP_VALID_BITS) == flags);
    debug_assert!(flags != VISIBILITYMAP_ALL_FROZEN);

    if BufferIsValid(heapBuf)
        && bufmgr_seams::buffer_get_block_number::call(heapBuf) != heapBlk
    {
        return Err(wrong_buffer("wrong heap buffer passed to visibilitymap_set"));
    }
    let Some(pin) = vmbuf.pin.as_ref().filter(|_| vmbuf.map_block == mapBlock) else {
        return Err(wrong_buffer("wrong VM buffer passed to visibilitymap_set"));
    };

    let guard = pin.lock_exclusive()?;
    // SAFETY: exclusive content lock held for `guard`'s lifetime; mapByte < MAPSIZE.
    let map_byte_ptr = unsafe {
        bufmgr_seams::buffer_get_page::call(pin.buffer())
            .as_ptr()
            .add(CONTENTS_OFF + mapByte)
    };
    // SAFETY: as above.
    let status = (unsafe { *map_byte_ptr } >> mapOffset) & VISIBILITYMAP_VALID_BITS;
    let res = (|| -> PgResult<()> {
        if flags != status {
            init_small::globals::StartCriticalSection();
            let res = set_bits_and_log(
                rel, heapBuf, recptr, pin, cutoff_xid, flags, map_byte_ptr, mapOffset,
            );
            init_small::globals::EndCriticalSection();
            res?;
        }
        Ok(())
    })();
    guard.unlock();
    res?;
    Ok(status)
}

fn set_bits_and_log(
    rel: &RelationData<'_>,
    heapBuf: Buffer,
    mut recptr: XLogRecPtr,
    pin: &BufferPin,
    cutoff_xid: TransactionId,
    flags: u8,
    map_byte_ptr: *mut u8,
    mapOffset: u32,
) -> PgResult<()> {
    // SAFETY: caller holds the exclusive content lock; in-page pointer.
    unsafe { *map_byte_ptr |= flags << mapOffset };
    bufmgr_seams::mark_buffer_dirty::call(pin.buffer())?;

    if relation_needs_wal(rel) {
        if recptr == InvalidXLogRecPtr {
            debug_assert!(!xlogutils_seams::in_recovery::call());
            recptr = log_heap_visible(rel, heapBuf, pin.buffer(), cutoff_xid, flags)?;

            // Without checksums/wal_log_hints the heap FPI was omitted above,
            // so the heap LSN must not move.
            if xlog_hint_bit_is_needed() {
                // SAFETY: caller holds the heap buffer exclusively locked
                // (visibilitymap_set contract).
                let mut hp = unsafe {
                    PageMut::from_raw(bufmgr_seams::buffer_get_page::call(heapBuf))
                };
                hp.set_lsn(recptr);
            }
        }
        // SAFETY: exclusive content lock held by the caller.
        let mut vp =
            unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
        vp.set_lsn(recptr);
    }
    Ok(())
}

/// `visibilitymap_clear` -> whether any bit was cleared. Not WAL-logged: the
/// caller's operation clears the bit again at replay.
pub fn visibilitymap_clear(
    _rel: &RelationData<'_>,
    heapBlk: BlockNumber,
    vmbuf: &VmBuffer,
    flags: u8,
) -> PgResult<bool> {
    let mapBlock = HEAPBLK_TO_MAPBLOCK(heapBlk);
    let mapByte = HEAPBLK_TO_MAPBYTE(heapBlk) as usize;
    let mapOffset = HEAPBLK_TO_OFFSET(heapBlk);
    let mask = flags << mapOffset;

    debug_assert!(flags & VISIBILITYMAP_VALID_BITS != 0);
    debug_assert!(flags != VISIBILITYMAP_ALL_VISIBLE);

    let Some(pin) = vmbuf.pin.as_ref().filter(|_| vmbuf.map_block == mapBlock) else {
        return Err(wrong_buffer("wrong buffer passed to visibilitymap_clear"));
    };

    let guard = pin.lock_exclusive()?;
    // SAFETY: exclusive content lock held for `guard`'s lifetime; mapByte < MAPSIZE.
    let map_byte_ptr = unsafe {
        bufmgr_seams::buffer_get_page::call(pin.buffer())
            .as_ptr()
            .add(CONTENTS_OFF + mapByte)
    };
    let mut cleared = false;
    // SAFETY: as above.
    if unsafe { *map_byte_ptr } & mask != 0 {
        // SAFETY: as above.
        unsafe { *map_byte_ptr &= !mask };
        let res = bufmgr_seams::mark_buffer_dirty::call(pin.buffer());
        if let Err(e) = res {
            guard.unlock();
            return Err(e);
        }
        cleared = true;
    }
    guard.unlock();
    Ok(cleared)
}

/// `visibilitymap_prepare_truncate` -> the new VM length in blocks, or
/// `InvalidBlockNumber` when there is nothing to truncate; the caller runs
/// the smgrtruncate.
pub fn visibilitymap_prepare_truncate(
    rel: &RelationData<'_>,
    nheapblocks: BlockNumber,
) -> PgResult<BlockNumber> {
    let truncBlock = HEAPBLK_TO_MAPBLOCK(nheapblocks);
    let truncByte = HEAPBLK_TO_MAPBYTE(nheapblocks) as usize;
    let truncOffset = HEAPBLK_TO_OFFSET(nheapblocks);

    let rlocator = bufmgr_seams::relation_smgr_locator::call(rel);
    if !smgr_seams::smgr_exists::call(rlocator, ForkNumber::VISIBILITYMAP_FORKNUM)? {
        return Ok(InvalidBlockNumber);
    }

    // Off a map-page boundary the tail bits of the last remaining page must be
    // cleared now: no later chance if the heap is re-extended.
    let newnblocks = if truncByte != 0 || truncOffset != 0 {
        let Some(pin) = vm_readbuf(rel, truncBlock, false)? else {
            return Ok(InvalidBlockNumber);
        };
        let guard = pin.lock_exclusive()?;
        init_small::globals::StartCriticalSection();
        let res = (|| -> PgResult<()> {
            // SAFETY: exclusive content lock held for `guard`'s lifetime;
            // CONTENTS_OFF..CONTENTS_OFF+MAPSIZE is in-page.
            let map = unsafe {
                core::slice::from_raw_parts_mut(
                    bufmgr_seams::buffer_get_page::call(pin.buffer())
                        .as_ptr()
                        .add(CONTENTS_OFF),
                    MAPSIZE as usize,
                )
            };
            map[truncByte + 1..].fill(0);
            map[truncByte] &= ((1u32 << truncOffset) - 1) as u8;

            bufmgr_seams::mark_buffer_dirty::call(pin.buffer())?;
            // Truncation WAL covers replay; the FPI guards a torn flush racing
            // it. Not MarkBufferDirtyHint: that skips dirtying in recovery.
            if !xlogutils_seams::in_recovery::call()
                && relation_needs_wal(rel)
                && xlog_hint_bit_is_needed()
            {
                xloginsert_seams::log_newpage_buffer::call(pin.buffer(), false)?;
            }
            Ok(())
        })();
        init_small::globals::EndCriticalSection();
        guard.unlock();
        pin.release();
        res?;
        truncBlock + 1
    } else {
        truncBlock
    };

    if smgr_seams::smgr_nblocks::call(rlocator, ForkNumber::VISIBILITYMAP_FORKNUM)?
        <= newnblocks
    {
        return Ok(InvalidBlockNumber);
    }
    Ok(newnblocks)
}

fn heap_page(buf: Buffer) -> PageRef<'static> {
    // SAFETY: debug-assert-only probe; caller pins the heap buffer for the
    // call (visibilitymap_set contract).
    unsafe { PageRef::from_raw(bufmgr_seams::buffer_get_page::call(buf)) }
}

#[cold]
#[inline(never)]
fn wrong_buffer(msg: &str) -> Box<types_error::PgError> {
    Box::new(types_error::PgError::new(types_error::ERROR, msg.to_string()))
}

fn vm_readbuf(
    rel: &RelationData<'_>,
    blkno: BlockNumber,
    extend: bool,
) -> PgResult<Option<BufferPin>> {
    let rlocator = bufmgr_seams::relation_smgr_locator::call(rel);
    let fork = ForkNumber::VISIBILITYMAP_FORKNUM;

    let mut nblocks = smgr_seams::smgr_cached_nblocks::call(rlocator, fork);
    if nblocks == InvalidBlockNumber {
        if smgr_seams::smgr_exists::call(rlocator, fork)? {
            nblocks = smgr_seams::smgr_nblocks::call(rlocator, fork)?;
        } else {
            smgr_seams::smgr_set_cached_nblocks::call(rlocator, fork, 0)?;
            nblocks = 0;
        }
    }

    // ZERO_ON_ERROR: always safe to clear bits, so clear corrupt pages rather
    // than error out; also the init path for concurrently-extended pages.
    let buf = if blkno >= nblocks {
        if !extend {
            return Ok(None);
        }
        vm_extend(rel, blkno + 1)?
    } else {
        bufmgr_seams::read_buffer_extended::call(
            rel,
            fork,
            blkno,
            ReadBufferMode::ZeroOnError,
            None,
        )?
    };

    let pin = BufferPin::adopt(buf).expect("vm_readbuf: invalid buffer");
    // Unlocked newness probe first, as C: don't take the lock on the normal
    // path; recheck under the lock before initializing.
    if pin.page().is_new() {
        let guard = pin.lock_exclusive()?;
        if guard.page().is_new() {
            // SAFETY: exclusive content lock held for `guard`'s lifetime.
            let mut page =
                unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
            page.init(0);
        }
        guard.unlock();
    }
    Ok(Some(pin))
}

fn vm_extend(rel: &RelationData<'_>, vm_nblocks: BlockNumber) -> PgResult<Buffer> {
    let buf = bufmgr_seams::extend_buffered_rel_to_rel::call(
        rel,
        ForkNumber::VISIBILITYMAP_FORKNUM,
        None,
        bufmgr_seams::EB_CREATE_FORK_IF_NEEDED | bufmgr_seams::EB_CLEAR_SIZE_CACHE,
        vm_nblocks,
        ReadBufferMode::ZeroOnError,
    )?;
    inval::invalidate::CacheInvalidateSmgr(bufmgr_seams::relation_smgr_locator::call(rel))?;
    Ok(buf)
}

#[cfg(test)]
mod tests;
