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
    BlockNumber, Buffer, BufferIsValid, ForkNumber, InvalidBlockNumber, InvalidBuffer, BLCKSZ,
    INIT_FORKNUM,
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

// ResourceOwnerForgetBufferIO for the AIO stage callback (pin handover).
pub(crate) fn forget_buffer_io_resowner(buffer: Buffer) {
    resowner::ResourceOwnerForget(
        resowner::CurrentResourceOwner(),
        Datum::from_i32(buffer),
        &BUFFER_IO_DESC,
    )
    .expect("ResourceOwnerForgetBufferIO");
}

fn ResOwnerPrintBufferIO<'a>(mcx: mcx::Mcx<'a>, res: Datum) -> PgResult<mcx::PgString<'a>> {
    mcx::PgString::from_str_in(
        &format!("lost track of buffer IO on buffer {}", res.as_i32()),
        mcx,
    )
}

#[cold]
#[track_caller]
pub(crate) fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
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
    ReadBuffer_common(smgr, persistence, forknum, blkno, mode, strategy).map(|(b, _)| b)
}

/// ReadBuffer_common (bufmgr.c, C 18 shape): warm hits short-circuit at
/// PinBufferForBlock; misses run the StartReadBuffer/WaitReadBuffers pgaio
/// pipeline (temp relations keep the direct pre-AIO path — C routes them
/// through pgaio with REFERENCES_LOCAL + forced-synchronous execution, which
/// is behaviorally the same preadv in the same backend; tracked divergence
/// until the local-buffer AIO arms land with io_uring).
pub fn ReadBuffer_common(
    smgr: RelFileLocatorBackend,
    persistence: u8,
    forknum: ForkNumber,
    blkno: BlockNumber,
    mode: ReadBufferMode,
    strategy: BufferAccessStrategy,
) -> PgResult<(Buffer, bool)> {
    if blkno == P_NEW {
        panic!("unported callee reached from bufmgr.c ReadBuffer_common: ExtendBufferedRel (P_NEW back-compat path)");
    }
    if matches!(
        mode,
        ReadBufferMode::ZeroAndLock | ReadBufferMode::ZeroAndCleanupLock
    ) {
        let (buffer, found) = PinBufferForBlock(smgr, persistence, forknum, blkno, &strategy)?;
        ZeroAndLockBuffer(buffer, mode, found)?;
        return Ok((buffer, found));
    }
    let (buffer, found) = PinBufferForBlock(smgr, persistence, forknum, blkno, &strategy)?;
    if found {
        return Ok((buffer, true));
    }
    if persistence == RELPERSISTENCE_TEMP {
        let mut local_flags = 0;
        if mode == ReadBufferMode::ZeroOnError || crate::gucs::zero_damaged_pages() {
            local_flags |= READ_BUFFERS_ZERO_ON_ERROR;
        }
        if crate::gucs::ignore_checksum_failure() {
            local_flags |= READ_BUFFERS_IGNORE_CHECKSUM_FAILURES;
        }
        let hit = complete_read_local(smgr, forknum, blkno, buffer, local_flags)?;
        return Ok((buffer, hit));
    }

    // Signal that we are going to immediately wait: no benefit in executing
    // Immediate wait follows: READ_BUFFERS_SYNCHRONOUSLY (C's hint).
    let mut flags = READ_BUFFERS_SYNCHRONOUSLY;
    if mode == ReadBufferMode::ZeroOnError {
        flags |= READ_BUFFERS_ZERO_ON_ERROR;
    }
    let mut operation = ReadBuffersOperation::new(smgr, persistence, forknum, strategy, flags);
    operation.buffers[0] = buffer;
    operation.blocknum = blkno;
    operation.nblocks = 1;
    let mut nblocks = 1;
    let hit = if start_read_buffers_impl(&mut operation, &mut nblocks)? {
        WaitReadBuffers(&mut operation)?;
        false
    } else {
        // Another backend completed it between the pin and StartBufferIO.
        true
    };
    Ok((buffer, hit))
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

pub(crate) fn InvalidateVictimBuffer(desc: &BufferDesc) -> PgResult<bool> {
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
            if wref.aio_index & types_storage::aio::PGAIO_WREF_TAG != 0 {
                // pgaio-armed wref (buffer_stage_common tags it so this arm
                // Tagged = pgaio-armed wref (untagged = uring prefetch lane).
                let untagged = types_storage::buf::PgAioWaitRef {
                    aio_index: wref.aio_index & !types_storage::aio::PGAIO_WREF_TAG,
                    generation_upper: wref.generation_upper,
                    generation_lower: wref.generation_lower,
                };
                aio_core::pgaio_wref_wait(&untagged)?;
                // pgaio may have removed us from this CV; re-arm before the
                // recheck (C 18 WaitIO does the same).
                ConditionVariablePrepareToSleep(cv);
                continue;
            }
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

/// TerminateBufferIO (bufmgr.c); release_aio drops the AIO subsystem's own
/// pin (taken in buffer_stage_common) and clears the wref.
pub(crate) fn TerminateBufferIO(
    desc: &BufferDesc,
    clear_dirty: bool,
    set_flag_bits: u32,
    forget_owner: bool,
    release_aio: bool,
) {
    let mut buf_state = LockBufHdr(desc);
    debug_assert!(buf_state & BM_IO_IN_PROGRESS != 0);
    buf_state &= !(BM_IO_IN_PROGRESS | BM_IO_ERROR);
    if clear_dirty && buf_state & types_storage::buf::BM_JUST_DIRTIED == 0 {
        buf_state &= !(BM_DIRTY | types_storage::buf::BM_CHECKPOINT_NEEDED);
    }
    if release_aio {
        debug_assert!(buffer_refcount(buf_state) > 0);
        buf_state -= types_storage::buf::BUF_REFCOUNT_ONE;
        // SAFETY: header lock held.
        unsafe { desc.set_io_wref(types_storage::buf::PgAioWaitRef::default()) };
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

    // Support LockBufferForCleanup: completing another backend's IO may drop
    // LockBufferForCleanup support: this may drop the last competing pin.
    if release_aio && buf_state & types_storage::buf::BM_PIN_COUNT_WAITER != 0 {
        crate::pin::WakePinCountWaiter(desc);
    }
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
    TerminateBufferIO(desc, false, BM_IO_ERROR, false, false);
}

fn complete_read_local(
    smgr: RelFileLocatorBackend,
    forknum: ForkNumber,
    blkno: BlockNumber,
    buffer: Buffer,
    flags: u32,
) -> PgResult<bool> {
    if !crate::localbuf::StartLocalBufferIO(buffer, true) {
        counters::local_hit();
        pgstat_count_io_op(
            IOObject::TempRelation,
            IOContext::IOCONTEXT_NORMAL,
            IOOp::Hit,
            1,
            0,
        );
        return Ok(true);
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

    // C 18 routes temp reads through the shared completion path
    // (buffer_readv_complete_one, is_temp); this direct path renders the same
    // observable ladder: server-log LOG for the checksum detail + per-buffer
    // line, definer-level ERROR/WARNING, checksum-failure stats.
    let mut piv_flags = PIV_LOG_LOG;
    if flags & READ_BUFFERS_IGNORE_CHECKSUM_FAILURES != 0 {
        piv_flags |= PIV_IGNORE_CHECKSUM_FAILURE;
    }
    let mut failed_checksum = false;
    let verified = page_is_verified(blk, blkno, piv_flags, Some(&mut failed_checksum));
    let mut zeroed = false;
    if !verified && flags & READ_BUFFERS_ZERO_ON_ERROR != 0 {
        // SAFETY: as above; zeroed page is the C zero_damaged_pages result.
        unsafe { core::ptr::write_bytes(blk, 0, BLCKSZ) };
        zeroed = true;
    }
    if !verified || failed_checksum {
        let rpath = relpath_backend_desc(smgr.locator, smgr.backend, forknum);
        let msg = if zeroed {
            format!("invalid page in block {blkno} of relation \"{rpath}\"; zeroing out page")
        } else if !verified {
            format!("invalid page in block {blkno} of relation \"{rpath}\"")
        } else {
            format!("ignoring checksum failure in block {blkno} of relation \"{rpath}\"")
        };
        // The per-buffer server-log line (C emits it from the completion
        // callback via buffer_readv_report at LOG_SERVER_ONLY).
        let _ = ereport(types_error::LOG_SERVER_ONLY)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(msg.clone())
            .finish(loc("WaitReadBuffers"));
        if failed_checksum {
            // C buffer_readv_complete's is_temp arm.
            pgstat::pgstat_report_checksum_failures_in_db(smgr.locator.dbOid, 1);
        }
        // The definer-level surface (C ProcessReadBuffersResult).
        if !verified && !zeroed {
            ereport(ERROR)
                .errcode(ERRCODE_DATA_CORRUPTED)
                .errmsg(msg)
                .finish(loc("WaitReadBuffers"))?;
            unreachable!("ERROR reported");
        }
        let _ = ereport(WARNING)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(msg)
            .finish(loc("WaitReadBuffers"));
    }
    crate::localbuf::TerminateLocalBufferIO(buffer, false, BM_VALID);
    Ok(false)
}

const MAX_READ_BATCH: usize = 64;

pub(crate) const READ_BUFFERS_ZERO_ON_ERROR: u32 = 1 << 0;
pub(crate) const READ_BUFFERS_IGNORE_CHECKSUM_FAILURES: u32 = 1 << 2;
pub(crate) const READ_BUFFERS_SYNCHRONOUSLY: u32 = 1 << 3;

/// ReadBuffersOperation (bufmgr.h). Must not move in memory between
/// start_read_buffers_impl and the end of WaitReadBuffers: the AIO handle
/// holds a raw report_return pointer to io_return (C contract).
pub(crate) struct ReadBuffersOperation {
    smgr: RelFileLocatorBackend,
    persistence: u8,
    forknum: ForkNumber,
    strategy: BufferAccessStrategy,
    flags: u32,
    blocknum: BlockNumber,
    nblocks: i16,
    nblocks_done: i16,
    buffers: [Buffer; MAX_READ_BATCH],
    io_wref: types_storage::buf::PgAioWaitRef,
    io_return: types_storage::aio::PgAioReturn,
}

impl ReadBuffersOperation {
    fn new(
        smgr: RelFileLocatorBackend,
        persistence: u8,
        forknum: ForkNumber,
        strategy: BufferAccessStrategy,
        flags: u32,
    ) -> Self {
        let mut op = ReadBuffersOperation {
            smgr,
            persistence,
            forknum,
            strategy,
            flags,
            blocknum: 0,
            nblocks: 0,
            nblocks_done: 0,
            buffers: [InvalidBuffer; MAX_READ_BATCH],
            io_wref: types_storage::buf::PgAioWaitRef::default(),
            io_return: types_storage::aio::PgAioReturn::default(),
        };
        aio_core::pgaio_wref_clear(&mut op.io_wref);
        op
    }
}

/// StartReadBuffersImpl (bufmgr.c): pin the run, split at hits, and (except
/// under io_method=sync) issue the IO. On return `*nblocks` is the accepted
/// count; slots beyond it may hold still-pinned "forwarded" buffers the
/// caller must release (our callers never continue a split operation).
fn start_read_buffers_impl(
    operation: &mut ReadBuffersOperation,
    nblocks: &mut i32,
) -> PgResult<bool> {
    let mut actual_nblocks = *nblocks;
    debug_assert!(*nblocks > 0 && *nblocks <= MAX_READ_BATCH as i32);

    let blocknum = operation.blocknum;
    let mut i = 0;
    while i < actual_nblocks {
        let idx = i as usize;
        let found;
        if operation.buffers[idx] != InvalidBuffer {
            let desc = GetBufferDescriptor(operation.buffers[idx] - 1);
            found = desc.state.load(Ordering::Relaxed) & BM_VALID != 0;
        } else {
            let (buffer, f) = PinBufferForBlock(
                operation.smgr,
                operation.persistence,
                operation.forknum,
                blocknum + i as BlockNumber,
                &operation.strategy,
            )?;
            operation.buffers[idx] = buffer;
            found = f;
        }

        if found {
            if i == 0 {
                *nblocks = 1;
                return Ok(false);
            }
            // Split: this valid buffer stays pinned as a forwarded buffer in
            actual_nblocks = i;
            break;
        }
        if i == 0 && actual_nblocks > 1 {
            // smgrmaxcombine: md refuses IOs crossing a segment boundary.
            let maxcombine = (types_storage::smgr::RELSEG_SIZE
                - (blocknum % types_storage::smgr::RELSEG_SIZE))
                as i32;
            if maxcombine < actual_nblocks {
                actual_nblocks = maxcombine;
            }
        }
        i += 1;
    }
    *nblocks = actual_nblocks;

    operation.nblocks = actual_nblocks as i16;
    operation.nblocks_done = 0;
    aio_core::pgaio_wref_clear(&mut operation.io_wref);

    let did_start_io;
    if aio_core::io_method() != guc_tables::consts::IOMETHOD_SYNC {
        did_start_io = AsyncReadBuffers(operation, nblocks)?;
        operation.nblocks = *nblocks as i16;
    } else {
        // The dedicated IOMETHOD_SYNC path (C keeps it to de-risk AIO): the
        // IO is issued from within WaitReadBuffers.
        operation.flags |= READ_BUFFERS_SYNCHRONOUSLY;
        did_start_io = true;
    }

    Ok(did_start_io)
}

/// ReadBuffersCanStartIO (bufmgr.c): submit staged IO before a blocking
/// StartBufferIO wait (deadlock avoidance under batchmode).
fn ReadBuffersCanStartIO(buffer: Buffer, nowait: bool) -> PgResult<bool> {
    debug_assert!(buffer > 0, "temp relations keep the pre-AIO path");
    let desc = GetBufferDescriptor(buffer - 1);
    if !nowait && aio_core::pgaio_have_staged() {
        if StartBufferIO(desc, true, true, true)? {
            return Ok(true);
        }
        aio_core::pgaio_submit_staged()?;
    }
    StartBufferIO(desc, true, nowait, true)
}

/// ProcessReadBuffersResult (bufmgr.c): consume one IO's distilled result,
/// reporting errors/warnings at the appropriate level.
fn ProcessReadBuffersResult(operation: &mut ReadBuffersOperation) -> PgResult<()> {
    use types_storage::aio::PgAioResultStatus as Rs;

    let rs = operation.io_return.result.status;
    debug_assert!(aio_core::pgaio_wref_valid(&operation.io_wref));
    debug_assert!(rs != Rs::Unknown);

    let newly_read_blocks = if rs != Rs::Error { operation.io_return.result.result } else { 0 };

    if rs == Rs::Error || rs == Rs::Warning {
        aio_core::pgaio_result_report(
            operation.io_return.result,
            &operation.io_return.target_data,
            if rs == Rs::Error { ERROR } else { WARNING },
        )?;
    } else if rs == Rs::Partial {
        aio_core::pgaio_result_report(
            operation.io_return.result,
            &operation.io_return.target_data,
            types_error::DEBUG1,
        )?;
    }

    debug_assert!(newly_read_blocks > 0 && newly_read_blocks <= MAX_READ_BATCH as i32);
    operation.nblocks_done += newly_read_blocks as i16;
    debug_assert!(operation.nblocks_done <= operation.nblocks);
    Ok(())
}

/// WaitReadBuffers (bufmgr.c): wait out (and, for partial reads and
/// io_method=sync, re-issue) the operation's IO until every block is done.
pub(crate) fn WaitReadBuffers(operation: &mut ReadBuffersOperation) -> PgResult<()> {
    let io_context = IOContextForStrategy(&operation.strategy);

    if !aio_core::pgaio_wref_valid(&operation.io_wref)
        && aio_core::io_method() != guc_tables::consts::IOMETHOD_SYNC
    {
        ereport(ERROR)
            .errmsg_internal("waiting for read operation that didn't read")
            .finish(loc("WaitReadBuffers"))?;
    }

    loop {
        if aio_core::pgaio_wref_valid(&operation.io_wref) {
            use types_storage::aio::PgAioResultStatus as Rs;
            // Only pay the timestamping when we may actually wait.
            if operation.io_return.result.status == Rs::Unknown
                && !aio_core::pgaio_wref_check_done(&operation.io_wref)
            {
                let io_start = pgstat_prepare_io_time(crate::gucs::track_io_timing());
                aio_core::pgaio_wref_wait(&operation.io_wref)?;
                // The IO itself was counted at issue; this is the wait time.
                pgstat_count_io_op_time(IOObject::Relation, io_context, IOOp::Read, io_start, 0, 0);
            }
            ProcessReadBuffersResult(operation)?;
        }

        if operation.nblocks_done == operation.nblocks {
            break;
        }

        postgres_seams::check_for_interrupts::call()?;

        let mut ignored_nblocks_progress = 0;
        AsyncReadBuffers(operation, &mut ignored_nblocks_progress)?;
    }
    Ok(())
}

/// AsyncReadBuffers (bufmgr.c): issue ONE IO for the operation's next
/// contiguous run of not-yet-valid buffers. Returns true if IO was initiated.
fn AsyncReadBuffers(
    operation: &mut ReadBuffersOperation,
    nblocks_progress: &mut i32,
) -> PgResult<bool> {
    let nblocks_done = operation.nblocks_done as usize;
    let blocknum = operation.blocknum;
    let forknum = operation.forknum;
    let mut flags = operation.flags;
    let io_context = IOContextForStrategy(&operation.strategy);

    let mut ioh_flags: u8 = 0;
    if flags & READ_BUFFERS_SYNCHRONOUSLY != 0 {
        ioh_flags |= types_storage::aio::PGAIO_HF_SYNCHRONOUS;
    }

    // The completion callback may run under another backend's (or an IO
    // worker's) GUC state: bake this backend's zero_damaged_pages /
    // ignore_checksum_failure into the callback data.
    if crate::gucs::zero_damaged_pages() {
        flags |= READ_BUFFERS_ZERO_ON_ERROR;
    }
    if crate::gucs::ignore_checksum_failure() {
        flags |= READ_BUFFERS_IGNORE_CHECKSUM_FAILURES;
    }

    // To be allowed to report stats in the local completion callback we need
    // to prepare to report stats now (C: even in a critical section).
    pgstat::pgstat_prepare_report_checksum_failure(operation.smgr.locator.dbOid);

    let ret_ptr: *mut types_storage::aio::PgAioReturn = &mut operation.io_return;
    let ioh = match aio_core::pgaio_io_acquire_nb(Some(resowner::CurrentResourceOwner()), ret_ptr)?
    {
        Some(ioh) => ioh,
        None => {
            aio_core::pgaio_submit_staged()?;
            aio_core::pgaio_io_acquire(Some(resowner::CurrentResourceOwner()), ret_ptr)?
        }
    };

    if !ReadBuffersCanStartIO(operation.buffers[nblocks_done], false)? {
        operation.nblocks_done += 1;
        *nblocks_progress = 1;
        aio_core::pgaio_io_release(ioh)?;
        aio_core::pgaio_wref_clear(&mut operation.io_wref);
        // A hit for this backend, even though it began as a miss at pin time;
        counters::hit();
        pgstat_count_io_op(IOObject::Relation, io_context, IOOp::Hit, 1, 0);
        return Ok(false);
    }

    let mut io_pages: [*mut u8; MAX_READ_BATCH] = [core::ptr::null_mut(); MAX_READ_BATCH];
    let mut io_buffer_ids: [u32; MAX_READ_BATCH] = [0; MAX_READ_BATCH];
    io_pages[0] = BufferGetBlockPtr(operation.buffers[nblocks_done]);
    io_buffer_ids[0] = operation.buffers[nblocks_done] as u32;
    let mut io_buffers_len = 1usize;

    for i in (nblocks_done + 1)..operation.nblocks as usize {
        if !ReadBuffersCanStartIO(operation.buffers[i], true)? {
            break;
        }
        debug_assert!(operation.buffers[i] != InvalidBuffer);
        io_pages[io_buffers_len] = BufferGetBlockPtr(operation.buffers[i]);
        io_buffer_ids[io_buffers_len] = operation.buffers[i] as u32;
        io_buffers_len += 1;
    }

    operation.io_wref = aio_core::pgaio_io_get_wref(ioh);

    aio_core::pgaio_io_set_handle_data_32(ioh, &io_buffer_ids[..io_buffers_len]);

    aio_core::pgaio_io_register_callbacks(
        ioh,
        types_storage::aio::PGAIO_HCB_SHARED_BUFFER_READV,
        flags as u8,
    );
    aio_core::pgaio_io_set_flag(ioh, ioh_flags);

    // Track the IO at issue time even for async execution: under
    let io_start = pgstat_prepare_io_time(crate::gucs::track_io_timing());
    smgr_seams::smgr_startreadv::call(
        operation.smgr,
        forknum,
        blocknum + nblocks_done as BlockNumber,
        &io_pages[..io_buffers_len],
    )?;
    pgstat_count_io_op_time(
        IOObject::Relation,
        io_context,
        IOOp::Read,
        io_start,
        1,
        (io_buffers_len * BLCKSZ) as u64,
    );
    counters::read_n(io_buffers_len as u64);

    *nblocks_progress = io_buffers_len as i32;
    Ok(true)
}

/// Sequential-batch read used by the scan-side callers: read `blkno` and,
/// while the following blocks also miss, cover up to min(nblocks_hint,
/// io_combine_limit) blocks of the MAIN fork in one operation. Extras end
/// valid-and-unpinned, so the scan's next fetches take the hit path.
pub(crate) fn ReadBuffer_batched(
    smgr: RelFileLocatorBackend,
    persistence: u8,
    blkno: BlockNumber,
    nblocks_hint: BlockNumber,
    strategy: BufferAccessStrategy,
) -> PgResult<(Buffer, bool)> {
    let forknum = ForkNumber::MAIN_FORKNUM;
    if persistence == RELPERSISTENCE_TEMP {
        return ReadBuffer_common(
            smgr,
            persistence,
            forknum,
            blkno,
            ReadBufferMode::Normal,
            strategy,
        );
    }
    // The extra blocks each hold a pin until the read completes: cap the run
    // pinned-buffer budget with GetAdditionalPinLimit(), which may be zero).
    // Without this, a seqscan on a tiny pool pins it whole and any concurrent
    // (or own) allocation dies with "no unpinned buffers available".
    let pin_room = 1 + crate::extend::GetAdditionalPinLimit() as usize;
    let cap = (crate::gucs::io_combine_limit().clamp(1, MAX_READ_BATCH as i32) as usize)
        .min(nblocks_hint.max(1) as usize)
        .min(pin_room);

    // worker mode the IO is queued to the pool and the issuer waits on the
    let mut operation = ReadBuffersOperation::new(smgr, persistence, forknum, strategy, 0);
    operation.blocknum = blkno;
    let mut nblocks = cap as i32;
    let did_start_io = start_read_buffers_impl(&mut operation, &mut nblocks)?;

    // Forwarded buffers (pinned beyond the accepted range at a hit/racing-IO
    // split): this caller never continues a split operation — release them.
    for i in (nblocks as usize)..cap {
        if operation.buffers[i] != InvalidBuffer {
            UnpinBuffer(GetBufferDescriptor(operation.buffers[i] - 1));
            operation.buffers[i] = InvalidBuffer;
        }
    }

    let hit = if did_start_io {
        WaitReadBuffers(&mut operation)?;
        false
    } else {
        true
    };

    // Extras end valid-and-unpinned (the batched-read contract).
    for i in 1..(nblocks as usize) {
        UnpinBuffer(GetBufferDescriptor(operation.buffers[i] - 1));
    }
    Ok((operation.buffers[0], hit))
}

// Flags for page_is_verified (bufpage.h PIV_*).
pub const PIV_LOG_WARNING: u32 = 1 << 0;
pub const PIV_LOG_LOG: u32 = 1 << 1;
pub const PIV_IGNORE_CHECKSUM_FAILURE: u32 = 1 << 2;

/// PageIsVerified (bufpage.c, C 18): header sanity + checksum verification
/// when the cluster has data checksums. `checksum_failure_p` reports checksum
/// failures even when the page is accepted (PIV_IGNORE_CHECKSUM_FAILURE), so
/// callers can count them.
pub fn page_is_verified(
    page: *const u8,
    blkno: BlockNumber,
    flags: u32,
    mut checksum_failure_p: Option<&mut bool>,
) -> bool {
    let mut checksum_failure = false;
    let mut header_sane = false;
    let mut checksum: u16 = 0;

    if let Some(p) = checksum_failure_p.as_deref_mut() {
        *p = false;
    }

    // SAFETY: caller owns a pinned BLCKSZ page image; u16 fields are 2-aligned
    // (page images are MAXALIGNed).
    let (pd_checksum, pd_flags, pd_lower, pd_upper, pd_special) = unsafe {
        (
            page.add(8).cast::<u16>().read(),
            page.add(10).cast::<u16>().read(),
            page.add(12).cast::<u16>().read(),
            page.add(14).cast::<u16>().read(),
            page.add(16).cast::<u16>().read(),
        )
    };

    // Don't verify page data unless the page passes basic non-zero test
    // (PageIsNew: pd_upper == 0).
    if pd_upper != 0 {
        if transam_xlog_seams::data_checksums_enabled::is_installed()
            && transam_xlog_seams::data_checksums_enabled::call()
        {
            // SAFETY: as above; page images are 4-aligned.
            checksum = unsafe { crate::write::checksum::page_checksum_raw(page, blkno) };
            if checksum != pd_checksum {
                checksum_failure = true;
                if let Some(p) = checksum_failure_p.as_deref_mut() {
                    *p = true;
                }
            }
        }

        // These checks don't prove the header correct, only sane enough to
        // allow into the buffer pool (C's exact conjunct set).
        if pd_flags & !types_storage::bufpage::PD_VALID_FLAG_BITS == 0
            && pd_lower <= pd_upper
            && pd_upper <= pd_special
            && (pd_special as usize) <= BLCKSZ
            && pd_special & 7 == 0
        {
            header_sane = true;
        }

        if header_sane && !checksum_failure {
            return true;
        }
    }

    // Check all-zeroes case.
    // SAFETY: caller contract, BLCKSZ readable.
    let s = unsafe { core::slice::from_raw_parts(page, BLCKSZ) };
    if s.iter().all(|&b| b == 0) {
        return true;
    }

    // Throw a WARNING/LOG, as instructed by PIV_LOG_*, if the checksum fails,
    // but only after we've checked for the all-zeroes case.
    if checksum_failure {
        if flags & (PIV_LOG_WARNING | PIV_LOG_LOG) != 0 {
            let level = if flags & PIV_LOG_WARNING != 0 { WARNING } else { types_error::LOG };
            let _ = ereport(level)
                .errcode(ERRCODE_DATA_CORRUPTED)
                .errmsg(format!(
                    "page verification failed, calculated checksum {checksum} but expected {pd_checksum}"
                ))
                .finish(loc("PageIsVerified"));
        }

        if header_sane && flags & PIV_IGNORE_CHECKSUM_FAILURE != 0 {
            return true;
        }
    }

    false
}

pub fn relpath_desc(locator: RelFileLocator, forknum: ForkNumber) -> String {
    relpath_backend_desc(locator, INVALID_PROC_NUMBER, forknum)
}

/// relpathbackend (relpath.h) for error texts; falls back to a hand-rolled
/// default-tablespace rendering when the relpath seam isn't installed (unit
/// suites).
pub fn relpath_backend_desc(
    locator: RelFileLocator,
    backend: types_core::ProcNumber,
    forknum: ForkNumber,
) -> String {
    if relpath_seams::relpathbackend::is_installed() {
        return relpath_seams::relpathbackend::call(locator, backend, forknum);
    }
    let t = if backend != INVALID_PROC_NUMBER {
        format!("t{backend}_")
    } else {
        String::new()
    };
    format!(
        "base/{}/{}{}{}",
        locator.dbOid,
        t,
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
        TerminateBufferIO(desc, false, BM_VALID, true, false);
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
    let result = PrefetchSharedBuffer(smgr, rel.rd_rel.relpersistence, forknum, blkno)?;
    Ok(if BufferIsValid(result.recent_buffer) {
        PrefetchOutcome::Cached
    } else if result.initiated_io {
        PrefetchOutcome::Issued
    } else {
        PrefetchOutcome::Skipped
    })
}

/// C PrefetchBufferResult: `recent_buffer` valid = already resident (recovery
/// stores it in the decoded block so redo can try ReadRecentBuffer).
#[derive(Clone, Copy, Debug)]
pub struct PrefetchBufferResult {
    pub recent_buffer: Buffer,
    pub initiated_io: bool,
}

pub fn PrefetchSharedBuffer(
    smgr: RelFileLocatorBackend,
    relpersistence: u8,
    forknum: ForkNumber,
    blkno: BlockNumber,
) -> PgResult<PrefetchBufferResult> {
    debug_assert!(blkno != P_NEW);
    let tag = init_buffer_tag(smgr.locator, forknum, blkno);
    let hash = BufTableHashCode(&tag);
    let partition_lock = BufMappingPartitionLock(hash);
    let lookup = || -> PgResult<Buffer> {
        LWLockAcquire(partition_lock, LW_SHARED, init_small::globals::MyProcNumber())?;
        let buf_id = BufTableLookup(&tag, hash)?;
        LWLockRelease(partition_lock)?;
        Ok(if buf_id >= 0 { buf_id + 1 } else { InvalidBuffer })
    };
    let recent_buffer = lookup()?;
    if BufferIsValid(recent_buffer) {
        return Ok(PrefetchBufferResult { recent_buffer, initiated_io: false });
    }
    if fd::io_direct_flags() & types_storage::IO_DIRECT_DATA != 0 {
        return Ok(PrefetchBufferResult { recent_buffer: InvalidBuffer, initiated_io: false });
    }
    if aio_seams::uring_available::is_installed() && aio_seams::uring_available::call() {
        match crate::uring::start_read(smgr, relpersistence, forknum, blkno)? {
            Some(PrefetchOutcome::Issued) => {
                return Ok(PrefetchBufferResult {
                    recent_buffer: InvalidBuffer,
                    initiated_io: true,
                });
            }
            Some(_) => {
                // start_read found (or left) the block in the pool; re-probe
                // for its id. An eviction race downgrades to initiated_io.
                let recent_buffer = lookup()?;
                return Ok(PrefetchBufferResult {
                    recent_buffer,
                    initiated_io: !BufferIsValid(recent_buffer),
                });
            }
            None => {} // Ring momentarily full or submit failed: advisory fallback.
        }
    }
    Ok(PrefetchBufferResult {
        recent_buffer: InvalidBuffer,
        initiated_io: smgr_seams::smgr_prefetch::call(smgr, forknum, blkno, 1)?,
    })
}
