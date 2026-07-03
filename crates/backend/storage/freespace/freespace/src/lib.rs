//! freespace.c/fsmpage.c/indexfsm.c INSERT lane; the vacuum/truncate/redo
//! lanes are loud named panics.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

mod fsmpage;

pub use fsmpage::{
    fsm_get_avail, fsm_get_max_avail, fsm_rebuild_page, fsm_search_avail, fsm_set_avail,
    fsm_truncate_avail, FsmPage, LEAF_NODES_PER_PAGE, NODES_PER_PAGE, NON_LEAF_NODES_PER_PAGE,
    SLOTS_PER_FSM_PAGE,
};

use ::bufmgr_seams::{
    BufferPin, BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK,
    EB_CLEAR_SIZE_CACHE, EB_CREATE_FORK_IF_NEEDED,
};
use ::types_core::{BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, Size, BLCKSZ};
use ::types_error::{PgError, PgResult};
use ::types_rel::RelationData;
use ::types_storage::bufpage::{MaxHeapTupleSize, PageMut};
use ::types_storage::{ReadBufferMode, RelFileLocator, RelFileLocatorBackend};

const FSM_CATEGORIES: Size = 256;
const FSM_CAT_STEP: Size = BLCKSZ / FSM_CATEGORIES;
const MaxFSMRequestSize: Size = MaxHeapTupleSize;

const FSM_TREE_DEPTH: i32 = if SLOTS_PER_FSM_PAGE >= 1626 { 3 } else { 4 };
const FSM_ROOT_LEVEL: i32 = FSM_TREE_DEPTH - 1;
const FSM_BOTTOM_LEVEL: i32 = 0;

// Transcription guards: a wrong slot count silently corrupts addressing.
const _: () = assert!(SLOTS_PER_FSM_PAGE == 4069);
const _: () = assert!(NODES_PER_PAGE == 8164);
const _: () = assert!(FSM_CAT_STEP == 32);
const _: () = assert!(MaxFSMRequestSize == 8160);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct FSMAddress {
    level: i32,
    logpageno: i32,
}

const FSM_ROOT_ADDRESS: FSMAddress = FSMAddress { level: FSM_ROOT_LEVEL, logpageno: 0 };

pub fn GetPageWithFreeSpace(rel: &RelationData<'_>, spaceNeeded: Size) -> PgResult<BlockNumber> {
    let min_cat = fsm_space_needed_to_cat(spaceNeeded)?;
    fsm_search(rel, min_cat)
}

pub fn RecordAndGetPageWithFreeSpace(
    rel: &RelationData<'_>,
    oldPage: BlockNumber,
    oldSpaceAvail: Size,
    spaceNeeded: Size,
) -> PgResult<BlockNumber> {
    let old_cat = fsm_space_avail_to_cat(oldSpaceAvail);
    let search_cat = fsm_space_needed_to_cat(spaceNeeded)?;

    let (addr, slot) = fsm_get_location(oldPage);
    let search_slot = fsm_set_and_search(rel, addr, slot, old_cat, search_cat)?;

    if search_slot != -1 {
        let blknum = fsm_get_heap_blk(addr, search_slot as u16);
        if fsm_does_block_exist(rel, blknum)? {
            return Ok(blknum);
        }
    }
    fsm_search(rel, search_cat)
}

/// New space above the old value only becomes searchable at the next
/// FreeSpaceMapVacuum, which updates the upper levels (C contract).
pub fn RecordPageWithFreeSpace(
    rel: &RelationData<'_>,
    heapBlk: BlockNumber,
    spaceAvail: Size,
) -> PgResult<()> {
    let new_cat = fsm_space_avail_to_cat(spaceAvail);
    let (addr, slot) = fsm_get_location(heapBlk);
    fsm_set_and_search(rel, addr, slot, new_cat, 0)?;
    Ok(())
}

pub fn GetRecordedFreeSpace(rel: &RelationData<'_>, heapBlk: BlockNumber) -> PgResult<Size> {
    let (addr, slot) = fsm_get_location(heapBlk);
    let Some(pin) = fsm_readbuf(rel, addr, false)? else {
        return Ok(0);
    };
    // SAFETY: pinned; single-byte unlocked read, as C.
    let page = unsafe { FsmPage::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
    let cat = fsm_get_avail(page, slot as i32);
    pin.release();
    Ok(fsm_space_cat_to_avail(cat))
}

pub fn XLogRecordPageWithFreeSpace(
    _rlocator: RelFileLocator,
    _heapBlk: BlockNumber,
    _spaceAvail: Size,
) -> ! {
    unported("XLogRecordPageWithFreeSpace (redo lane, freespace.c)");
}

pub fn FreeSpaceMapPrepareTruncateRel(_rel: &RelationData<'_>, _nblocks: BlockNumber) -> ! {
    unported("FreeSpaceMapPrepareTruncateRel (rel truncate lane, freespace.c)");
}

pub fn FreeSpaceMapVacuum(_rel: &RelationData<'_>) -> ! {
    unported("FreeSpaceMapVacuum (vacuum lane, freespace.c)");
}

pub fn FreeSpaceMapVacuumRange(
    _rel: &RelationData<'_>,
    _start: BlockNumber,
    _end: BlockNumber,
) -> ! {
    unported("FreeSpaceMapVacuumRange (vacuum lane, freespace.c)");
}

pub fn GetFreeIndexPage(rel: &RelationData<'_>) -> PgResult<BlockNumber> {
    let blkno = GetPageWithFreeSpace(rel, BLCKSZ / 2)?;
    if blkno != InvalidBlockNumber {
        RecordUsedIndexPage(rel, blkno)?;
    }
    Ok(blkno)
}

pub fn RecordFreeIndexPage(rel: &RelationData<'_>, freeBlock: BlockNumber) -> PgResult<()> {
    RecordPageWithFreeSpace(rel, freeBlock, BLCKSZ - 1)
}

pub fn RecordUsedIndexPage(rel: &RelationData<'_>, usedBlock: BlockNumber) -> PgResult<()> {
    RecordPageWithFreeSpace(rel, usedBlock, 0)
}

pub fn IndexFreeSpaceMapVacuum(rel: &RelationData<'_>) -> ! {
    FreeSpaceMapVacuum(rel)
}

fn fsm_space_avail_to_cat(avail: Size) -> u8 {
    debug_assert!(avail < BLCKSZ);
    if avail >= MaxFSMRequestSize {
        return 255;
    }
    let cat = avail / FSM_CAT_STEP;
    if cat > 254 {
        return 254;
    }
    cat as u8
}

fn fsm_space_cat_to_avail(cat: u8) -> Size {
    // Category 255 represents exactly MaxFSMRequestSize bytes.
    if cat == 255 {
        MaxFSMRequestSize
    } else {
        cat as Size * FSM_CAT_STEP
    }
}

/// Rounds up, where fsm_space_avail_to_cat rounds down.
fn fsm_space_needed_to_cat(needed: Size) -> PgResult<u8> {
    if needed > MaxFSMRequestSize {
        return Err(invalid_fsm_request_size(needed));
    }
    if needed == 0 {
        return Ok(1);
    }
    let cat = (needed + FSM_CAT_STEP - 1) / FSM_CAT_STEP;
    if cat > 255 {
        return Ok(255);
    }
    Ok(cat as u8)
}

fn fsm_logical_to_physical(addr: FSMAddress) -> BlockNumber {
    // C int arithmetic; wrapping mirrors C's (theoretical) signed overflow.
    let mut leafno: i32 = addr.logpageno;
    for _ in 0..addr.level {
        leafno = leafno.wrapping_mul(SLOTS_PER_FSM_PAGE);
    }

    let mut pages: BlockNumber = 0;
    for _ in 0..FSM_TREE_DEPTH {
        pages = pages.wrapping_add(leafno.wrapping_add(1) as BlockNumber);
        leafno /= SLOTS_PER_FSM_PAGE;
    }
    pages = pages.wrapping_sub(addr.level as BlockNumber);
    pages.wrapping_sub(1)
}

fn fsm_get_location(heapblk: BlockNumber) -> (FSMAddress, u16) {
    let addr = FSMAddress {
        level: FSM_BOTTOM_LEVEL,
        logpageno: (heapblk / SLOTS_PER_FSM_PAGE as BlockNumber) as i32,
    };
    (addr, (heapblk % SLOTS_PER_FSM_PAGE as BlockNumber) as u16)
}

fn fsm_get_heap_blk(addr: FSMAddress, slot: u16) -> BlockNumber {
    debug_assert!(addr.level == FSM_BOTTOM_LEVEL);
    (addr.logpageno as BlockNumber).wrapping_mul(SLOTS_PER_FSM_PAGE as BlockNumber)
        + slot as BlockNumber
}

fn fsm_get_parent(child: FSMAddress) -> (FSMAddress, u16) {
    debug_assert!(child.level < FSM_ROOT_LEVEL);
    let parent = FSMAddress {
        level: child.level + 1,
        logpageno: child.logpageno / SLOTS_PER_FSM_PAGE,
    };
    (parent, (child.logpageno % SLOTS_PER_FSM_PAGE) as u16)
}

fn fsm_get_child(parent: FSMAddress, slot: u16) -> FSMAddress {
    debug_assert!(parent.level > FSM_BOTTOM_LEVEL);
    FSMAddress {
        level: parent.level - 1,
        logpageno: parent.logpageno * SLOTS_PER_FSM_PAGE + slot as i32,
    }
}

fn fsm_readbuf(
    rel: &RelationData<'_>,
    addr: FSMAddress,
    extend: bool,
) -> PgResult<Option<BufferPin>> {
    let blkno = fsm_logical_to_physical(addr);
    let rlocator = bufmgr_seams::relation_smgr_locator::call(rel);
    let fork = ForkNumber::FSM_FORKNUM;

    // Cached size can be stale past-end (no smgr inval on extension).
    let mut nblocks = smgr_seams::smgr_cached_nblocks::call(rlocator, fork);
    if nblocks == InvalidBlockNumber || blkno >= nblocks {
        smgr_seams::smgr_set_cached_nblocks::call(rlocator, fork, InvalidBlockNumber)?;
        if smgr_seams::smgr_exists::call(rlocator, fork)? {
            nblocks = smgr_seams::smgr_nblocks::call(rlocator, fork)?;
        } else {
            smgr_seams::smgr_set_cached_nblocks::call(rlocator, fork, 0)?;
            nblocks = 0;
        }
    }

    // ZERO_ON_ERROR: the FSM is advisory and un-WAL-logged — clear torn
    // pages rather than error.
    let buf = if blkno >= nblocks {
        if !extend {
            return Ok(None);
        }
        fsm_extend(rlocator, blkno + 1)?
    } else {
        bufmgr_seams::read_buffer_extended::call(
            rel,
            fork,
            blkno,
            ReadBufferMode::ZeroOnError,
            None,
        )?
    };

    let pin = BufferPin::adopt(buf).expect("fsm_readbuf: InvalidBuffer");
    // Unlocked newness probe first, recheck under the lock, as C.
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

/// Extends with zero-filled pages: all-zero = no free space.
fn fsm_extend(rlocator: RelFileLocatorBackend, fsm_nblocks: BlockNumber) -> PgResult<Buffer> {
    bufmgr_seams::extend_buffered_rel_to::call(
        rlocator,
        ForkNumber::FSM_FORKNUM,
        None,
        EB_CREATE_FORK_IF_NEEDED | EB_CLEAR_SIZE_CACHE,
        fsm_nblocks,
        ReadBufferMode::ZeroOnError,
    )
}

fn fsm_set_and_search(
    rel: &RelationData<'_>,
    addr: FSMAddress,
    slot: u16,
    newValue: u8,
    minValue: u8,
) -> PgResult<i32> {
    let pin = fsm_readbuf(rel, addr, true)?.expect("fsm_readbuf(extend=true) returned no buffer");
    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;

    // SAFETY: pinned + exclusively locked just above.
    let page = unsafe { FsmPage::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer())) };
    if fsm_set_avail(page, slot as i32, newValue) {
        bufmgr_seams::mark_buffer_dirty_hint::call(pin.buffer(), false)?;
    }

    let mut newslot = -1;
    if minValue != 0 {
        newslot = fsm_search_avail(&pin, minValue, addr.level == FSM_BOTTOM_LEVEL, true)?;
    }

    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
    pin.release();
    Ok(newslot)
}

fn fsm_search(rel: &RelationData<'_>, min_cat: u8) -> PgResult<BlockNumber> {
    let mut restarts = 0;
    let mut addr = FSM_ROOT_ADDRESS;

    loop {
        let mut slot: i32 = -1;
        let mut max_avail: u8 = 0;
        let mut held = fsm_readbuf(rel, addr, false)?;

        if let Some(pin) = &held {
            bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_SHARE)?;
            slot = fsm_search_avail(pin, min_cat, addr.level == FSM_BOTTOM_LEVEL, false)?;
            if slot == -1 {
                // SAFETY: pinned; root byte read while share-locked.
                let page = unsafe {
                    FsmPage::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer()))
                };
                max_avail = fsm_get_max_avail(page);
                bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
                held.take().unwrap().release();
            } else {
                // Keep the pin for the possible past-EOF update below.
                bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
            }
        }

        if slot != -1 {
            if addr.level == FSM_BOTTOM_LEVEL {
                let pin = held.take().unwrap();
                let blkno = fsm_get_heap_blk(addr, slot as u16);

                if fsm_does_block_exist(rel, blkno)? {
                    pin.release();
                    return Ok(blkno);
                }

                // Past relation end: clear the slot, restart from the root.
                bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_EXCLUSIVE)?;
                // SAFETY: pinned + exclusively locked just above.
                let page = unsafe {
                    FsmPage::from_raw(bufmgr_seams::buffer_get_page::call(pin.buffer()))
                };
                fsm_set_avail(page, slot, 0);
                bufmgr_seams::mark_buffer_dirty_hint::call(pin.buffer(), false)?;
                bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
                pin.release();
                if restarts > 10000 {
                    return Ok(InvalidBlockNumber);
                }
                restarts += 1;
                addr = FSM_ROOT_ADDRESS;
            } else {
                held.take().unwrap().release();
            }
            // C descends unconditionally here — including after the past-EOF
            // arm just reset addr to the root.
            addr = fsm_get_child(addr, slot as u16);
        } else if addr.level == FSM_ROOT_LEVEL {
            return Ok(InvalidBlockNumber);
        } else {
            // Stale upper-level node: fix the parent, restart from the root
            // (the lost-update race is tolerated, fixed by vacuum — as C).
            let (parent, parentslot) = fsm_get_parent(addr);
            fsm_set_and_search(rel, parent, parentslot, max_avail, 0)?;

            if restarts > 10000 {
                return Ok(InvalidBlockNumber);
            }
            restarts += 1;
            addr = FSM_ROOT_ADDRESS;
        }
    }
}

/// Past the cached main-fork length: confirm against a fresh nblocks before
/// disbelieving the FSM (extension sends no inval).
fn fsm_does_block_exist(rel: &RelationData<'_>, blknumber: BlockNumber) -> PgResult<bool> {
    let rlocator = bufmgr_seams::relation_smgr_locator::call(rel);
    let cached = smgr_seams::smgr_cached_nblocks::call(rlocator, ForkNumber::MAIN_FORKNUM);
    if cached != InvalidBlockNumber && blknumber < cached {
        return Ok(true);
    }
    Ok(blknumber
        < bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
            rel,
            ForkNumber::MAIN_FORKNUM,
        )?)
}

#[cold]
#[inline(never)]
fn invalid_fsm_request_size(needed: Size) -> Box<PgError> {
    Box::new(PgError::error(std::format!("invalid FSM request size {needed}")))
}

#[cold]
#[inline(never)]
fn unported(unit: &'static str) -> ! {
    panic!("unported callee reached from freespace.c: {unit}");
}

pub fn init_seams() {
    freespace_seams::get_page_with_free_space::set(GetPageWithFreeSpace);
    freespace_seams::record_and_get_page_with_free_space::set(RecordAndGetPageWithFreeSpace);
    freespace_seams::record_page_with_free_space::set(RecordPageWithFreeSpace);
}

#[cfg(test)]
mod tests;
