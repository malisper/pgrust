//! visibilitymap.c READ lane (get_status/pin/pin_ok/count); the write lane
//! (set/clear/prepare_truncate) is loud named panics for the vacuum/DML lanes.

#![allow(non_snake_case)]

use ::bufmgr_seams::BufferPin;
use ::types_core::{
    BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, TransactionId, XLogRecPtr, BLCKSZ,
};
use ::types_error::PgResult;
use ::types_rel::RelationData;
use ::types_storage::bufpage::{PageMut, PageRef, SizeOfPageHeaderData};
use ::types_storage::{ReadBufferMode, RelFileLocatorBackend};

pub const VISIBILITYMAP_ALL_VISIBLE: u8 = 0x01;
pub const VISIBILITYMAP_ALL_FROZEN: u8 = 0x02;
pub const VISIBILITYMAP_VALID_BITS: u8 = 0x03;

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

/// C's `Buffer *vmbuf` carrier (scan-state `ioss_VMBuffer`, vacuum `vmbuffer`).
/// `map_block` caches BufferGetBlockNumber(pin) — the pin holds the
/// buffer->block mapping fixed, so the repeat-probe path is compare + load,
/// no descriptor read.
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
    // SAFETY: CONTENTS_OFF + mapByte < BLCKSZ — mapByte < MAPSIZE by the mod
    // arithmetic; page is BLCKSZ and live for the pin-scoped borrow.
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
        nvisible += pg_popcount_masked(map, VISIBLE_MASK8);
        nfrozen += pg_popcount_masked(map, FROZEN_MASK8);
        pin.release();
        mapBlock += 1;
    }
    Ok((nvisible as BlockNumber, nfrozen as BlockNumber))
}

fn pg_popcount_masked(buf: &[u8], mask: u8) -> u64 {
    buf.iter().map(|&b| (b & mask).count_ones() as u64).sum()
}

pub fn visibilitymap_set(
    _rel: &RelationData<'_>,
    _heapBlk: BlockNumber,
    _heapBuf: Buffer,
    _recptr: XLogRecPtr,
    _vmbuf: &mut VmBuffer,
    _cutoff_xid: TransactionId,
    _flags: u8,
) -> ! {
    unported("visibilitymap_set (vacuum/redo set lane, visibilitymap.c)");
}

pub fn visibilitymap_clear(
    _rel: &RelationData<'_>,
    _heapBlk: BlockNumber,
    _vmbuf: &VmBuffer,
    _flags: u8,
) -> ! {
    unported("visibilitymap_clear (DML clear lane, visibilitymap.c)");
}

pub fn visibilitymap_prepare_truncate(_rel: &RelationData<'_>, _nheapblocks: BlockNumber) -> ! {
    unported("visibilitymap_prepare_truncate (rel truncate lane, visibilitymap.c)");
}

#[cold]
#[inline(never)]
fn unported(unit: &'static str) -> ! {
    panic!("unported callee reached from visibilitymap.c: {unit}");
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
        vm_extend(rlocator, blkno + 1)?
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

fn vm_extend(rlocator: RelFileLocatorBackend, vm_nblocks: BlockNumber) -> PgResult<Buffer> {
    let buf = bufmgr_seams::extend_buffered_rel_to::call(
        rlocator,
        ForkNumber::VISIBILITYMAP_FORKNUM,
        None,
        bufmgr_seams::EB_CREATE_FORK_IF_NEEDED | bufmgr_seams::EB_CLEAR_SIZE_CACHE,
        vm_nblocks,
        ReadBufferMode::ZeroOnError,
    )?;
    inval::invalidate::CacheInvalidateSmgr(rlocator)?;
    Ok(buf)
}

#[cfg(test)]
mod tests;
