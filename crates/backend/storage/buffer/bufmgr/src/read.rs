use core::sync::atomic::Ordering;

use elog::ereport;
use lwlock::{LWLockAcquire, LWLockConditionalAcquire, LWLockRelease, LW_EXCLUSIVE, LW_SHARED};
use types_core::{
    BlockNumber, Buffer, BufferIsValid, ForkNumber, InvalidBlockNumber, BLCKSZ, INIT_FORKNUM,
    INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP, RELPERSISTENCE_UNLOGGED,
};
use types_error::{
    ErrorLocation, PgResult, ERRCODE_DATA_CORRUPTED, ERROR, WARNING,
};
use types_storage::buf::{
    buftag, BufferAccessStrategy, BM_DIRTY, BM_IO_ERROR, BM_IO_IN_PROGRESS, BM_PERMANENT,
    BM_TAG_VALID, BM_VALID, BUF_FLAG_MASK, BUF_USAGECOUNT_MASK, BUF_USAGECOUNT_ONE,
};
use types_storage::{ReadBufferMode, RelFileLocator, RelFileLocatorBackend};

use crate::buf_hdr::{
    cleared_buftag, BufferDesc, BufferDescriptorGetBuffer, BufferGetBlockPtr, GetBufferDescriptor,
    LockBufHdr, UnlockBufHdr,
};
use crate::buf_table::{
    BufMappingPartitionLock, BufTableDelete, BufTableHashCode, BufTableInsert, BufTableLookup,
};
use crate::counters;
use crate::freelist::{StrategyFreeBuffer, StrategyGetBuffer};
use crate::ops::{LockBuffer, LockBufferForCleanup, BUFFER_LOCK_EXCLUSIVE};
use crate::pin::{buffer_refcount, PinBuffer, PinBuffer_Locked, UnpinBuffer};
use crate::privref::{GetPrivateRefCount, ReservePrivateRefCountEntry as reserve_entry};

const P_NEW: BlockNumber = InvalidBlockNumber;

#[cold]
fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("bufmgr.c", 0, funcname)
}

fn init_buffer_tag(rlocator: RelFileLocator, forknum: ForkNumber, blkno: BlockNumber) -> buftag {
    buftag {
        spcOid: rlocator.spcOid,
        dbOid: rlocator.dbOid,
        relNumber: rlocator.relNumber,
        forkNum: forknum,
        blockNum: blkno,
    }
}

pub fn ReadBufferWithoutRelcache(
    rlocator: RelFileLocator,
    forknum: ForkNumber,
    blkno: BlockNumber,
    mode: ReadBufferMode,
    strategy: BufferAccessStrategy,
    permanent: bool,
) -> PgResult<Buffer> {
    let smgr = RelFileLocatorBackend {
        locator: rlocator,
        backend: INVALID_PROC_NUMBER,
    };
    let persistence = if permanent {
        RELPERSISTENCE_PERMANENT
    } else {
        RELPERSISTENCE_UNLOGGED
    };
    ReadBuffer_common(smgr, persistence, forknum, blkno, mode, strategy)
}

/// Single-block synchronous core: PG18's StartReadBuffer/WaitReadBuffers pgaio
/// pipeline collapsed to its io_method=sync behavior (aio unit owns async).
pub fn ReadBuffer_common(
    smgr: RelFileLocatorBackend,
    persistence: u8,
    forknum: ForkNumber,
    blkno: BlockNumber,
    mode: ReadBufferMode,
    strategy: BufferAccessStrategy,
) -> PgResult<Buffer> {
    if blkno == P_NEW {
        panic!("unported callee reached from bufmgr.c ReadBuffer_common: ExtendBufferedRel (P_NEW back-compat path)");
    }
    if matches!(
        mode,
        ReadBufferMode::ZeroAndLock | ReadBufferMode::ZeroAndCleanupLock
    ) {
        let (buffer, found) = PinBufferForBlock(smgr, persistence, forknum, blkno, &strategy)?;
        ZeroAndLockBuffer(buffer, mode, found)?;
        return Ok(buffer);
    }
    let (buffer, found) = PinBufferForBlock(smgr, persistence, forknum, blkno, &strategy)?;
    if !found {
        // C consults zero_damaged_pages only on the miss/completion side.
        let zero_on_error =
            mode == ReadBufferMode::ZeroOnError || crate::gucs::zero_damaged_pages();
        complete_read_sync(smgr, forknum, blkno, buffer, zero_on_error)?;
    }
    Ok(buffer)
}

fn PinBufferForBlock(
    smgr: RelFileLocatorBackend,
    persistence: u8,
    forknum: ForkNumber,
    blkno: BlockNumber,
    strategy: &BufferAccessStrategy,
) -> PgResult<(Buffer, bool)> {
    debug_assert!(blkno != P_NEW);
    if persistence == RELPERSISTENCE_TEMP {
        panic!("unported callee reached from bufmgr.c PinBufferForBlock: LocalBufferAlloc (localbuf.c)");
    }
    let (buffer, found) = BufferAlloc(smgr, persistence, forknum, blkno, strategy)?;
    if found {
        counters::hit();
    }
    Ok((buffer, found))
}

/// The partitioned mapping lookup, warm-hit pin, and victim install.
fn BufferAlloc(
    smgr: RelFileLocatorBackend,
    relpersistence: u8,
    forknum: ForkNumber,
    blkno: BlockNumber,
    strategy: &BufferAccessStrategy,
) -> PgResult<(Buffer, bool)> {
    reserve_entry();

    let new_tag = init_buffer_tag(smgr.locator, forknum, blkno);
    let new_hash = BufTableHashCode(&new_tag);
    let partition_lock = BufMappingPartitionLock(new_hash);

    // M2 swizzling decision site: shared partition LWLock + hash probe + pin
    // CAS on every warm hit — the block a swizzled parent pointer with
    // optimistic version validation removes entirely (strategy.md lever 8).
    LWLockAcquire(partition_lock, LW_SHARED, init_small::globals::MyProcNumber())?;
    let existing_id = BufTableLookup(&new_tag, new_hash)?;
    if existing_id >= 0 {
        let desc = GetBufferDescriptor(existing_id);
        let valid = PinBuffer(desc, strategy);
        LWLockRelease(partition_lock)?;
        let mut found = true;
        if !valid && StartBufferIO(desc, true, false)? {
            found = false;
        }
        return Ok((BufferDescriptorGetBuffer(desc), found));
    }
    LWLockRelease(partition_lock)?;

    let victim_buffer = GetVictimBuffer(strategy)?;
    let victim_desc = GetBufferDescriptor(victim_buffer - 1);

    LWLockAcquire(partition_lock, LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
    let existing_id = BufTableInsert(&new_tag, new_hash, victim_desc.buf_id)?;
    if existing_id >= 0 {
        let existing_desc = GetBufferDescriptor(existing_id);
        let valid = PinBuffer(existing_desc, strategy);
        LWLockRelease(partition_lock)?;
        UnpinBuffer(victim_desc);
        StrategyFreeBuffer(victim_desc.buf_id);
        let mut found = true;
        if !valid && StartBufferIO(existing_desc, true, false)? {
            found = false;
        }
        return Ok((BufferDescriptorGetBuffer(existing_desc), found));
    }

    let mut victim_state = LockBufHdr(victim_desc);
    debug_assert!(buffer_refcount(victim_state) == 1);
    debug_assert!(
        victim_state & (BM_TAG_VALID | BM_VALID | BM_DIRTY | BM_IO_IN_PROGRESS) == 0
    );
    // SAFETY: header lock held, our pin is the only reference (asserted).
    unsafe { victim_desc.set_tag(new_tag) };
    victim_state |= BM_TAG_VALID | BUF_USAGECOUNT_ONE;
    if relpersistence == RELPERSISTENCE_PERMANENT || forknum == INIT_FORKNUM {
        victim_state |= BM_PERMANENT;
    }
    UnlockBufHdr(victim_desc, victim_state);
    LWLockRelease(partition_lock)?;
    Ok((victim_buffer, false))
}

/// Clock-sweep victim, pinned, evicted from the mapping table.
fn GetVictimBuffer(strategy: &BufferAccessStrategy) -> PgResult<Buffer> {
    loop {
        reserve_entry();
        let (victim, _from_ring) = StrategyGetBuffer(strategy)?;
        let (buf_id, buf_state) = victim.into_parts();
        let desc = GetBufferDescriptor(buf_id);
        debug_assert!(buffer_refcount(buf_state) == 0);
        PinBuffer_Locked(desc);
        debug_assert!(GetPrivateRefCount(BufferDescriptorGetBuffer(desc)) == 1);

        if buf_state & BM_DIRTY != 0 {
            debug_assert!(buf_state & BM_TAG_VALID != 0);
            debug_assert!(buf_state & BM_VALID != 0);
            // Conditional share-lock: an unconditional wait can deadlock
            // against a backend already holding this page exclusively.
            if !LWLockConditionalAcquire(&desc.content_lock, LW_SHARED)? {
                UnpinBuffer(desc);
                continue;
            }
            if strategy.is_some() {
                let hdr_state = LockBufHdr(desc);
                let lsn = crate::ops::buffer_page_get_lsn(BufferDescriptorGetBuffer(desc));
                UnlockBufHdr(desc, hdr_state);
                if transam_xlog_seams::xlog_needs_flush::call(lsn)
                    && crate::freelist::StrategyRejectBuffer(strategy, desc.buf_id, _from_ring)
                {
                    LWLockRelease(&desc.content_lock)?;
                    UnpinBuffer(desc);
                    continue;
                }
            }
            let flush_result = crate::write::FlushBuffer(desc);
            LWLockRelease(&desc.content_lock)?;
            flush_result?;
            crate::write::schedule_backend_writeback(&desc.tag())?;
        }
        if buf_state & BM_VALID != 0 {
            counters::evict();
        }
        if buf_state & BM_TAG_VALID != 0 && !InvalidateVictimBuffer(desc)? {
            UnpinBuffer(desc);
            continue;
        }
        return Ok(BufferDescriptorGetBuffer(desc));
    }
}

fn InvalidateVictimBuffer(desc: &BufferDesc) -> PgResult<bool> {
    debug_assert!(desc.state.load(Ordering::Acquire) & BM_TAG_VALID != 0);
    let tag = desc.tag();
    let hash = BufTableHashCode(&tag);
    let partition_lock = BufMappingPartitionLock(hash);

    LWLockAcquire(partition_lock, LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
    let mut buf_state = LockBufHdr(desc);
    if buffer_refcount(buf_state) != 1 || buf_state & BM_DIRTY != 0 {
        UnlockBufHdr(desc, buf_state);
        LWLockRelease(partition_lock)?;
        return Ok(false);
    }
    // SAFETY: header lock held, refcount==1 is our own pin (checked above).
    unsafe { desc.set_tag(cleared_buftag()) };
    buf_state &= !(BUF_FLAG_MASK | BUF_USAGECOUNT_MASK);
    UnlockBufHdr(desc, buf_state);
    BufTableDelete(&tag, hash)?;
    LWLockRelease(partition_lock)?;
    Ok(true)
}

/// WaitIO's sleep arm needs the per-buffer CV (second-backend-only).
pub(crate) fn StartBufferIO(desc: &BufferDesc, for_input: bool, nowait: bool) -> PgResult<bool> {
    loop {
        let buf_state = LockBufHdr(desc);
        if buf_state & BM_IO_IN_PROGRESS == 0 {
            let done = if for_input {
                buf_state & BM_VALID != 0
            } else {
                buf_state & BM_DIRTY == 0
            };
            if done {
                UnlockBufHdr(desc, buf_state);
                return Ok(false);
            }
            UnlockBufHdr(desc, buf_state | BM_IO_IN_PROGRESS);
            return Ok(true);
        }
        UnlockBufHdr(desc, buf_state);
        if nowait {
            return Ok(false);
        }
        panic!("unported callee reached from bufmgr.c WaitIO: ConditionVariableSleep on BufferIO CV (condition_variable unported)");
    }
}

/// TerminateBufferIO (bufmgr.c), sans the pgaio release arm.
pub(crate) fn TerminateBufferIO(desc: &BufferDesc, clear_dirty: bool, set_flag_bits: u32) {
    let mut buf_state = LockBufHdr(desc);
    debug_assert!(buf_state & BM_IO_IN_PROGRESS != 0);
    buf_state &= !(BM_IO_IN_PROGRESS | BM_IO_ERROR);
    if clear_dirty && buf_state & types_storage::buf::BM_JUST_DIRTIED == 0 {
        buf_state &= !(BM_DIRTY | types_storage::buf::BM_CHECKPOINT_NEEDED);
    }
    buf_state |= set_flag_bits;
    UnlockBufHdr(desc, buf_state);
    // ConditionVariableBroadcast(BufferDescriptorGetIOCV): sound to elide —
    // any would-be waiter panics in StartBufferIO/WaitIO before sleeping, so
    // no waiter can exist until the condition_variable unit lands.
}

fn complete_read_sync(
    smgr: RelFileLocatorBackend,
    forknum: ForkNumber,
    blkno: BlockNumber,
    buffer: Buffer,
    zero_on_error: bool,
) -> PgResult<()> {
    let desc = GetBufferDescriptor(buffer - 1);
    if !StartBufferIO(desc, true, false)? {
        return Ok(());
    }
    let blk = BufferGetBlockPtr(buffer);
    // SAFETY: pinned + BM_IO_IN_PROGRESS: we own the (not yet valid) page image.
    let page = unsafe { core::slice::from_raw_parts_mut(blk, BLCKSZ) };
    if let Err(e) = smgr_seams::smgr_read::call(smgr, forknum, blkno, page) {
        TerminateBufferIO(desc, false, BM_IO_ERROR);
        return Err(e);
    }
    counters::read();
    if !page_is_verified(blk) {
        if zero_on_error {
            let _ = elog::elog(
                WARNING,
                format!(
                    "invalid page in block {blkno} of relation {}; zeroing out page",
                    relpath_desc(smgr.locator, forknum)
                ),
            );
            // SAFETY: as above; zeroed page is the C zero_damaged_pages result.
            unsafe { core::ptr::write_bytes(blk, 0, BLCKSZ) };
        } else {
            TerminateBufferIO(desc, false, BM_IO_ERROR);
            ereport(ERROR)
                .errcode(ERRCODE_DATA_CORRUPTED)
                .errmsg(format!(
                    "invalid page in block {blkno} of relation {}",
                    relpath_desc(smgr.locator, forknum)
                ))
                .finish(loc("WaitReadBuffers"))?;
            unreachable!("ERROR reported");
        }
    }
    TerminateBufferIO(desc, false, BM_VALID);
    Ok(())
}

/// PageIsVerified (bufpage.c) header-sanity core; the checksum arm pends
/// ControlFile (tracked divergence: checksum-enabled clusters unverified).
fn page_is_verified(page: *const u8) -> bool {
    // SAFETY: caller owns a pinned BLCKSZ page image; u16 fields are 2-aligned
    // (page images are MAXALIGNed).
    unsafe {
        let pd_flags = page.add(10).cast::<u16>().read();
        let pd_lower = page.add(12).cast::<u16>().read();
        let pd_upper = page.add(14).cast::<u16>().read();
        let pd_special = page.add(16).cast::<u16>().read();
        let pagesize_version = page.add(18).cast::<u16>().read();
        let header_sane = pd_flags & !types_storage::bufpage::PD_VALID_FLAG_BITS == 0
            && (pd_lower as usize) >= types_storage::bufpage::SizeOfPageHeaderData
            && pd_lower <= pd_upper
            && (pd_upper as usize) <= (pd_special as usize)
            && (pd_special as usize) <= BLCKSZ
            && pagesize_version
                == (BLCKSZ as u16) | types_storage::bufpage::PG_PAGE_LAYOUT_VERSION as u16;
        if header_sane {
            return true;
        }
        let s = core::slice::from_raw_parts(page, BLCKSZ);
        s.iter().all(|&b| b == 0)
    }
}

fn relpath_desc(locator: RelFileLocator, forknum: ForkNumber) -> String {
    format!(
        "base/{}/{}{}",
        locator.dbOid,
        locator.relNumber,
        match forknum {
            ForkNumber::MAIN_FORKNUM => String::new(),
            f => format!("_{}", f as i32),
        }
    )
}

fn ZeroAndLockBuffer(buffer: Buffer, mode: ReadBufferMode, already_valid: bool) -> PgResult<()> {
    let desc = GetBufferDescriptor(buffer - 1);
    let mut need_to_zero = false;
    if !already_valid {
        need_to_zero = StartBufferIO(desc, true, false)?;
    }
    if need_to_zero {
        let blk = BufferGetBlockPtr(buffer);
        // SAFETY: pinned + we won the IO: sole writer of the invalid image.
        unsafe { core::ptr::write_bytes(blk, 0, BLCKSZ) };
        LWLockAcquire(
            &desc.content_lock,
            LW_EXCLUSIVE,
            init_small::globals::MyProcNumber(),
        )?;
        TerminateBufferIO(desc, false, BM_VALID);
    } else if mode == ReadBufferMode::ZeroAndLock {
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE)?;
    } else {
        LockBufferForCleanup(buffer)?;
    }
    Ok(())
}

/// Mapping-table-free re-pin fastpath.
pub fn ReadRecentBuffer(
    rlocator: RelFileLocator,
    forknum: ForkNumber,
    blkno: BlockNumber,
    recent_buffer: Buffer,
) -> PgResult<bool> {
    debug_assert!(BufferIsValid(recent_buffer));
    reserve_entry();
    let tag = init_buffer_tag(rlocator, forknum, blkno);
    if recent_buffer < 0 {
        panic!("unported callee reached from bufmgr.c ReadRecentBuffer: local buffers (localbuf.c)");
    }
    let desc = GetBufferDescriptor(recent_buffer - 1);
    let have_private_ref = GetPrivateRefCount(recent_buffer) > 0;
    if have_private_ref {
        let buf_state = desc.state.load(Ordering::Acquire);
        if buf_state & BM_VALID != 0 && desc.tag() == tag {
            PinBuffer(desc, &None);
            counters::hit();
            return Ok(true);
        }
    } else {
        let buf_state = LockBufHdr(desc);
        if buf_state & BM_VALID != 0 && desc.tag() == tag {
            PinBuffer_Locked(desc);
            counters::hit();
            return Ok(true);
        }
        UnlockBufHdr(desc, buf_state);
    }
    Ok(false)
}
