use core::sync::atomic::Ordering;

use condition_variable::{
    ConditionVariableBroadcast, ConditionVariableCancelSleep, ConditionVariablePrepareToSleep,
    ConditionVariableSleep,
};
use datum::Datum;
use elog::ereport;
use types_resowner::{ResourceOwnerDesc, RELEASE_PRIO_BUFFER_IOS, RESOURCE_RELEASE_BEFORE_LOCKS};
use lwlock::{LWLockAcquire, LWLockConditionalAcquire, LWLockRelease, LW_EXCLUSIVE, LW_SHARED};
use types_core::{
    BlockNumber, Buffer, BufferIsValid, ForkNumber, InvalidBlockNumber, BLCKSZ, INIT_FORKNUM,
    INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP, RELPERSISTENCE_UNLOGGED,
};
use types_error::{
    ErrorLocation, PgResult, ERRCODE_DATA_CORRUPTED, ERROR, WARNING,
};
use pgstat::io::{
    pgstat_count_io_op, pgstat_count_io_op_time, pgstat_prepare_io_time, IOObject, IOOp,
};
use types_storage::buf::{
    buftag, BufferAccessStrategy, IOContext, BM_DIRTY, BM_IO_ERROR, BM_IO_IN_PROGRESS,
    BM_PERMANENT, BM_TAG_VALID, BM_VALID, BUF_FLAG_MASK, BUF_USAGECOUNT_MASK, BUF_USAGECOUNT_ONE,
};
use types_storage::{ReadBufferMode, RelFileLocator, RelFileLocatorBackend};

use crate::buf_hdr::{
    cleared_buftag, BufferDesc, BufferDescriptorGetBuffer, BufferDescriptorGetIOCV,
    BufferGetBlockPtr, GetBufferDescriptor, LockBufHdr, UnlockBufHdr,
};
use crate::buf_table::{
    BufMappingPartitionLock, BufTableDelete, BufTableHashCode, BufTableInsert, BufTableLookup,
};
use crate::counters;
use crate::freelist::{IOContextForStrategy, StrategyFreeBuffer, StrategyGetBuffer};
use crate::ops::{LockBuffer, LockBufferForCleanup, BUFFER_LOCK_EXCLUSIVE};
use crate::pin::{buffer_refcount, PinBuffer, PinBuffer_Locked, UnpinBuffer};
use crate::privref::{GetPrivateRefCount, ReservePrivateRefCountEntry as reserve_entry};

const P_NEW: BlockNumber = InvalidBlockNumber;

const PG_WAIT_IPC: u32 = 0x0800_0000;
const WAIT_EVENT_BUFFER_IO: u32 = PG_WAIT_IPC + 8;

// Abort-time cleanup of unterminated IO; without it WaitIO sleepers hang.
static BUFFER_IO_DESC: ResourceOwnerDesc = ResourceOwnerDesc {
    name: "buffer io",
    release_phase: RESOURCE_RELEASE_BEFORE_LOCKS,
    release_priority: RELEASE_PRIO_BUFFER_IOS,
    ReleaseResource: ResOwnerReleaseBufferIO,
    DebugPrint: Some(ResOwnerPrintBufferIO),
};

fn ResOwnerReleaseBufferIO(res: Datum) {
    AbortBufferIO(res.as_i32());
}

fn ResOwnerPrintBufferIO<'a>(mcx: mcx::Mcx<'a>, res: Datum) -> PgResult<mcx::PgString<'a>> {
    mcx::PgString::from_str_in(
        &format!("lost track of buffer IO on buffer {}", res.as_i32()),
        mcx,
    )
}

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
        complete_read_sync(
            smgr,
            forknum,
            blkno,
            buffer,
            zero_on_error,
            IOContextForStrategy(&strategy),
        )?;
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
        let (buffer, found) = crate::localbuf::LocalBufferAlloc(smgr, forknum, blkno)?;
        if found {
            counters::local_hit();
            pgstat_count_io_op(
                IOObject::TempRelation,
                IOContext::IOCONTEXT_NORMAL,
                IOOp::Hit,
                1,
                0,
            );
        }
        return Ok((buffer, found));
    }
    let io_context = IOContextForStrategy(strategy);
    let (buffer, found) = BufferAlloc(smgr, persistence, forknum, blkno, strategy, io_context)?;
    if found {
        counters::hit();
        pgstat_count_io_op(IOObject::Relation, io_context, IOOp::Hit, 1, 0);
    }
    Ok((buffer, found))
}

/// The partitioned mapping lookup, warm-hit pin, and victim install.
pub(crate) fn BufferAlloc(
    smgr: RelFileLocatorBackend,
    relpersistence: u8,
    forknum: ForkNumber,
    blkno: BlockNumber,
    strategy: &BufferAccessStrategy,
    io_context: IOContext,
) -> PgResult<(Buffer, bool)> {
    reserve_entry();
    crate::pin::resowner_enlarge_for_pin()?;

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
        // !valid: in-progress or failed read. No StartBufferIO here —
        // complete_read_sync owns it; claiming both self-deadlocks WaitIO.
        let valid = PinBuffer(desc, strategy);
        LWLockRelease(partition_lock)?;
        return Ok((BufferDescriptorGetBuffer(desc), valid));
    }
    LWLockRelease(partition_lock)?;

    let victim_buffer = GetVictimBuffer(strategy, io_context)?;
    let victim_desc = GetBufferDescriptor(victim_buffer - 1);

    LWLockAcquire(partition_lock, LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
    let existing_id = BufTableInsert(&new_tag, new_hash, victim_desc.buf_id)?;
    if existing_id >= 0 {
        let existing_desc = GetBufferDescriptor(existing_id);
        // Unpin-before-pin is load-bearing: dropping the victim's last local
        // ref refills the reserved privref entry PinBuffer consumes (as in C).
        UnpinBuffer(victim_desc);
        StrategyFreeBuffer(victim_desc.buf_id);
        let valid = PinBuffer(existing_desc, strategy);
        LWLockRelease(partition_lock)?;
        return Ok((BufferDescriptorGetBuffer(existing_desc), valid));
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
pub(crate) fn GetVictimBuffer(
    strategy: &BufferAccessStrategy,
    io_context: IOContext,
) -> PgResult<Buffer> {
    loop {
        reserve_entry();
        crate::pin::resowner_enlarge_for_pin()?;
        let (victim, from_ring) = StrategyGetBuffer(strategy)?;
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
                    && crate::freelist::StrategyRejectBuffer(strategy, desc.buf_id, from_ring)
                {
                    LWLockRelease(&desc.content_lock)?;
                    UnpinBuffer(desc);
                    continue;
                }
            }
            let flush_result = crate::write::FlushBuffer(desc, io_context);
            LWLockRelease(&desc.content_lock)?;
            flush_result?;
            crate::write::schedule_backend_writeback(io_context, &desc.tag())?;
        }
        if buf_state & BM_VALID != 0 {
            counters::evict();
            pgstat_count_io_op(
                IOObject::Relation,
                io_context,
                if from_ring { IOOp::Reuse } else { IOOp::Evict },
                1,
                0,
            );
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

/// WaitIO (bufmgr.c): the pgaio_wref_wait arm routes to the uring ring drain
/// (any thread may complete any IO — C's deadlock rule).
pub(crate) fn WaitIO(desc: &BufferDesc) -> PgResult<()> {
    let cv = BufferDescriptorGetIOCV(desc);
    ConditionVariablePrepareToSleep(cv);
    loop {
        let buf_state = LockBufHdr(desc);
        let wref = desc.io_wref();
        UnlockBufHdr(desc, buf_state);
        if buf_state & BM_IO_IN_PROGRESS == 0 {
            break;
        }
        if wref.aio_index != 0 || wref.generation_upper != 0 || wref.generation_lower != 0 {
            if !aio_seams::uring_buf_read_wait::is_installed() {
                panic!("unported callee reached from bufmgr.c WaitIO: pgaio_wref_wait (io_wref armed with no uring backend)");
            }
            let gen = ((wref.generation_upper as u64) << 32) | wref.generation_lower as u64;
            aio_seams::uring_buf_read_wait::call(wref.aio_index, gen);
            continue;
        }
        if let Err(e) = ConditionVariableSleep(cv, WAIT_EVENT_BUFFER_IO) {
            // C divergence: C cancels at abort; PgResult must de-list here.
            ConditionVariableCancelSleep();
            return Err(e);
        }
    }
    ConditionVariableCancelSleep();
    Ok(())
}

pub(crate) fn StartBufferIO(
    desc: &BufferDesc,
    for_input: bool,
    nowait: bool,
    remember_owner: bool,
) -> PgResult<bool> {
    resowner::ResourceOwnerEnlarge(resowner::CurrentResourceOwner())?;
    let buf_state = loop {
        let buf_state = LockBufHdr(desc);
        if buf_state & BM_IO_IN_PROGRESS == 0 {
            break buf_state;
        }
        UnlockBufHdr(desc, buf_state);
        if nowait {
            return Ok(false);
        }
        WaitIO(desc)?;
    };
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
    // remember_owner=false: uring reads track abort cleanup via the ring drain
    // in AtEOXact_Buffers, not AbortBufferIO (C 18 buffer AIO shape).
    if remember_owner {
        resowner::ResourceOwnerRemember(
            resowner::CurrentResourceOwner(),
            Datum::from_i32(BufferDescriptorGetBuffer(desc)),
            &BUFFER_IO_DESC,
        )
        .expect("ResourceOwnerRememberBufferIO");
    }
    Ok(true)
}

/// TerminateBufferIO (bufmgr.c), sans the release_aio arm (aio unit).
pub(crate) fn TerminateBufferIO(
    desc: &BufferDesc,
    clear_dirty: bool,
    set_flag_bits: u32,
    forget_owner: bool,
) {
    let mut buf_state = LockBufHdr(desc);
    debug_assert!(buf_state & BM_IO_IN_PROGRESS != 0);
    buf_state &= !(BM_IO_IN_PROGRESS | BM_IO_ERROR);
    if clear_dirty && buf_state & types_storage::buf::BM_JUST_DIRTIED == 0 {
        buf_state &= !(BM_DIRTY | types_storage::buf::BM_CHECKPOINT_NEEDED);
    }
    buf_state |= set_flag_bits;
    UnlockBufHdr(desc, buf_state);
    if forget_owner {
        resowner::ResourceOwnerForget(
            resowner::CurrentResourceOwner(),
            Datum::from_i32(BufferDescriptorGetBuffer(desc)),
            &BUFFER_IO_DESC,
        )
        .expect("ResourceOwnerForgetBufferIO");
    }
    ConditionVariableBroadcast(BufferDescriptorGetIOCV(desc));
}

/// AbortBufferIO (bufmgr.c): resowner release callback only; buffer still
/// pinned (IOs release before pins, prio 100 < 200).
fn AbortBufferIO(buffer: Buffer) {
    let desc = GetBufferDescriptor(buffer - 1);
    let buf_state = LockBufHdr(desc);
    debug_assert!(buf_state & (BM_IO_IN_PROGRESS | BM_TAG_VALID) != 0);
    if buf_state & BM_VALID == 0 {
        debug_assert!(buf_state & BM_DIRTY == 0);
        UnlockBufHdr(desc, buf_state);
    } else {
        UnlockBufHdr(desc, buf_state);
        panic!("unported arm reached from bufmgr.c AbortBufferIO: dirty-write abort (every FlushBuffer error terminates inline; no write path leaks BM_IO_IN_PROGRESS)");
    }
    TerminateBufferIO(desc, false, BM_IO_ERROR, false);
}

fn complete_read_local(
    smgr: RelFileLocatorBackend,
    forknum: ForkNumber,
    blkno: BlockNumber,
    buffer: Buffer,
    zero_on_error: bool,
) -> PgResult<()> {
    if !crate::localbuf::StartLocalBufferIO(buffer, true) {
        counters::local_hit();
        pgstat_count_io_op(
            IOObject::TempRelation,
            IOContext::IOCONTEXT_NORMAL,
            IOOp::Hit,
            1,
            0,
        );
        return Ok(());
    }
    let blk = crate::localbuf::local_block_ptr(buffer);
    // SAFETY: pinned local block, single thread; image not yet valid.
    let page = unsafe { core::slice::from_raw_parts_mut(blk, BLCKSZ) };
    let io_start = pgstat_prepare_io_time(crate::gucs::track_io_timing());
    smgr_seams::smgr_read::call(smgr, forknum, blkno, page)?;
    pgstat_count_io_op_time(
        IOObject::TempRelation,
        IOContext::IOCONTEXT_NORMAL,
        IOOp::Read,
        io_start,
        1,
        BLCKSZ as u64,
    );
    counters::local_read();
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
    crate::localbuf::TerminateLocalBufferIO(buffer, false, BM_VALID);
    Ok(())
}

fn complete_read_sync(
    smgr: RelFileLocatorBackend,
    forknum: ForkNumber,
    blkno: BlockNumber,
    buffer: Buffer,
    zero_on_error: bool,
    io_context: IOContext,
) -> PgResult<()> {
    if buffer < 0 {
        return complete_read_local(smgr, forknum, blkno, buffer, zero_on_error);
    }
    let desc = GetBufferDescriptor(buffer - 1);
    if !StartBufferIO(desc, true, false, true)? {
        // Another backend completed the read: our miss became a hit
        // (C WaitReadBuffers' already-valid arm).
        counters::hit();
        pgstat_count_io_op(IOObject::Relation, io_context, IOOp::Hit, 1, 0);
        return Ok(());
    }
    let blk = BufferGetBlockPtr(buffer);
    // SAFETY: pinned + BM_IO_IN_PROGRESS: we own the (not yet valid) page image.
    let page = unsafe { core::slice::from_raw_parts_mut(blk, BLCKSZ) };
    let io_start = pgstat_prepare_io_time(crate::gucs::track_io_timing());
    if let Err(e) = smgr_seams::smgr_read::call(smgr, forknum, blkno, page) {
        TerminateBufferIO(desc, false, BM_IO_ERROR, true);
        return Err(e);
    }
    pgstat_count_io_op_time(
        IOObject::Relation,
        io_context,
        IOOp::Read,
        io_start,
        1,
        BLCKSZ as u64,
    );
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
            TerminateBufferIO(desc, false, BM_IO_ERROR, true);
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
    TerminateBufferIO(desc, false, BM_VALID, true);
    Ok(())
}

/// PageIsVerified (bufpage.c) header-sanity core; the checksum arm pends
/// ControlFile (tracked divergence: checksum-enabled clusters unverified).
pub fn page_is_verified(page: *const u8) -> bool {
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

pub fn relpath_desc(locator: RelFileLocator, forknum: ForkNumber) -> String {
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
    if buffer < 0 {
        if !already_valid && crate::localbuf::StartLocalBufferIO(buffer, true) {
            let blk = crate::localbuf::local_block_ptr(buffer);
            // SAFETY: pinned local block, single thread; we own the image.
            unsafe { core::ptr::write_bytes(blk, 0, BLCKSZ) };
            crate::localbuf::TerminateLocalBufferIO(buffer, false, BM_VALID);
        }
        return Ok(());
    }
    let desc = GetBufferDescriptor(buffer - 1);
    let mut need_to_zero = false;
    if !already_valid {
        need_to_zero = StartBufferIO(desc, true, false, true)?;
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
        TerminateBufferIO(desc, false, BM_VALID, true);
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
    crate::pin::resowner_enlarge_for_pin()?;
    let tag = init_buffer_tag(rlocator, forknum, blkno);
    if recent_buffer < 0 {
        let desc = crate::localbuf::local_desc(recent_buffer);
        let state = desc.state.load(Ordering::Relaxed);
        if state & BM_VALID != 0 && desc.tag() == tag {
            crate::localbuf::PinLocalBuffer(recent_buffer, true);
            counters::local_hit();
            return Ok(true);
        }
        return Ok(false);
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

/// C PrefetchBuffer/PrefetchSharedBuffer collapsed to an outcome enum
/// (recent_buffer is never surfaced: our callers only steer a distance
/// heuristic). Advisory only — never changes what a later read returns.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrefetchOutcome {
    /// Block already in shared buffers (C result.recent_buffer valid).
    Cached,
    /// posix_fadvise issued (C result.initiated_io).
    Issued,
    /// Local/temp relation, direct I/O, or missing file — nothing issued.
    Skipped,
}

pub fn PrefetchBuffer(
    rel: &types_rel::RelationData<'_>,
    forknum: ForkNumber,
    blkno: BlockNumber,
) -> PgResult<PrefetchOutcome> {
    debug_assert!(blkno != P_NEW);
    if rel.rd_rel.relpersistence == RELPERSISTENCE_TEMP {
        return Ok(PrefetchOutcome::Skipped);
    }
    let smgr = crate::rel_locator_backend(rel);
    let tag = init_buffer_tag(smgr.locator, forknum, blkno);
    let hash = BufTableHashCode(&tag);
    let partition_lock = BufMappingPartitionLock(hash);
    LWLockAcquire(partition_lock, LW_SHARED, init_small::globals::MyProcNumber())?;
    let buf_id = BufTableLookup(&tag, hash)?;
    LWLockRelease(partition_lock)?;
    if buf_id >= 0 {
        return Ok(PrefetchOutcome::Cached);
    }
    if fd::io_direct_flags() & types_storage::IO_DIRECT_DATA != 0 {
        return Ok(PrefetchOutcome::Skipped);
    }
    if aio_seams::uring_available::is_installed() && aio_seams::uring_available::call() {
        if let Some(outcome) =
            crate::uring::start_read(smgr, rel.rd_rel.relpersistence, forknum, blkno)?
        {
            return Ok(outcome);
        }
        // Ring momentarily full or submit failed: advisory fallback below.
    }
    Ok(if smgr_seams::smgr_prefetch::call(smgr, forknum, blkno, 1)? {
        PrefetchOutcome::Issued
    } else {
        PrefetchOutcome::Skipped
    })
}
