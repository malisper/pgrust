use elog::ereport;
use lwlock::{LWLockAcquire, LWLockRelease, LW_EXCLUSIVE};
use types_core::{
    BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, InvalidBuffer, MaxBlockNumber, BLCKSZ,
    INIT_FORKNUM, RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP,
};
use types_error::{ErrorLocation, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR};
use pgstat::io::{pgstat_count_io_op_time, pgstat_prepare_io_time, IOObject, IOOp};
use types_storage::buf::{
    BufferAccessStrategy, BM_DIRTY, BM_JUST_DIRTIED, BM_PERMANENT, BM_TAG_VALID, BM_VALID,
    BUF_USAGECOUNT_ONE,
};
use types_storage::lock::ExclusiveLock;
use types_storage::{ReadBufferMode, RelFileLocatorBackend};
use types_rel::rel::RelationData;

use crate::buf_hdr::{BufferDescriptorGetBuffer, BufferGetBlockPtr, GetBufferDescriptor, LockBufHdr, UnlockBufHdr};
use crate::buf_table::{BufMappingPartitionLock, BufTableHashCode, BufTableInsert};
use crate::freelist::{GetPinLimit, IOContextForStrategy, StrategyFreeBuffer};
use crate::pin::{buffer_refcount, PinBuffer, UnpinBuffer};
use crate::privref::{overflowed_count, ReservePrivateRefCountEntry, REFCOUNT_ARRAY_ENTRIES};
use crate::read::{GetVictimBuffer, ReadBuffer_common, StartBufferIO, TerminateBufferIO};

#[cold]
#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

const MAX_BUFFERS_TO_EXTEND_BY: usize = 64;

pub(crate) fn GetAdditionalPinLimit() -> u32 {
    let estimated_pins_held = overflowed_count() + REFCOUNT_ARRAY_ENTRIES as u32;
    (GetPinLimit().max(0) as u32).saturating_sub(estimated_pins_held)
}

fn LimitAdditionalPins(additional_pins: &mut u32) {
    if *additional_pins <= 1 {
        return;
    }
    let limit = GetAdditionalPinLimit().max(1);
    if limit < *additional_pins {
        *additional_pins = limit;
    }
}

pub fn ExtendBufferedRelBy(
    rel: &RelationData<'_>,
    fork: ForkNumber,
    strategy: BufferAccessStrategy,
    flags: u32,
    extend_by: u32,
) -> PgResult<(Buffer, u32)> {
    debug_assert!(extend_by > 0);
    let smgr = crate::rel_locator_backend(rel);
    let relpersistence = rel.rd_rel.relpersistence;
    let mut buffers = [InvalidBuffer; MAX_BUFFERS_TO_EXTEND_BY];
    let (_, extended_by) = ExtendBufferedRelCommon(
        Some(rel),
        smgr,
        relpersistence,
        fork,
        &strategy,
        flags,
        extend_by,
        InvalidBlockNumber,
        &mut buffers,
    )?;
    // C returns every extended buffer pinned; the only multi-page caller
    // (hio.c RelationAddBlocks) uses buffers[1..] solely to ReleaseBuffer
    // them, so the pins drop here and only the first crosses the seam.
    for buf in buffers.iter().take(extended_by as usize).skip(1) {
        if *buf < 0 {
            crate::localbuf::UnpinLocalBuffer(*buf);
        } else {
            UnpinBuffer(GetBufferDescriptor(*buf - 1));
        }
    }
    Ok((buffers[0], extended_by))
}

pub fn ExtendBufferedRelTo(
    rel: &RelationData<'_>,
    fork: ForkNumber,
    strategy: BufferAccessStrategy,
    flags: u32,
    extend_to: BlockNumber,
    mode: ReadBufferMode,
) -> PgResult<Buffer> {
    let smgr = crate::rel_locator_backend(rel);
    extend_to_guts(Some(rel), smgr, rel.rd_rel.relpersistence, fork, strategy, flags, extend_to, mode)
}

// The BMR_SMGR form: recovery/init callers hold no Relation, and every C call
// site of this shape passes EB_SKIP_EXTENSION_LOCK (asserted below).
pub fn ExtendBufferedRelToSmgr(
    smgr: RelFileLocatorBackend,
    relpersistence: u8,
    fork: ForkNumber,
    strategy: BufferAccessStrategy,
    flags: u32,
    extend_to: BlockNumber,
    mode: ReadBufferMode,
) -> PgResult<Buffer> {
    assert!(
        flags & bufmgr_seams::EB_SKIP_EXTENSION_LOCK != 0,
        "ExtendBufferedRelTo: smgr form cannot take the relation extension lock"
    );
    extend_to_guts(None, smgr, relpersistence, fork, strategy, flags, extend_to, mode)
}

#[allow(clippy::too_many_arguments)]
fn extend_to_guts(
    rel: Option<&RelationData<'_>>,
    smgr: RelFileLocatorBackend,
    relpersistence: u8,
    fork: ForkNumber,
    strategy: BufferAccessStrategy,
    mut flags: u32,
    extend_to: BlockNumber,
    mode: ReadBufferMode,
) -> PgResult<Buffer> {
    debug_assert!(extend_to != InvalidBlockNumber && extend_to > 0);

    if flags & bufmgr_seams::EB_CREATE_FORK_IF_NEEDED != 0 && !smgr_seams::smgr_exists::call(smgr, fork)? {
        if let Some(rel) = rel {
            lmgr::LockRelationForExtension(rel, ExclusiveLock)?;
            if !smgr_seams::smgr_exists::call(smgr, fork)? {
                smgr_seams::smgr_create::call(smgr, fork, flags & bufmgr_seams::EB_PERFORMING_RECOVERY != 0)?;
            }
            lmgr::UnlockRelationForExtension(rel, ExclusiveLock)?;
        } else {
            smgr_seams::smgr_create::call(smgr, fork, flags & bufmgr_seams::EB_PERFORMING_RECOVERY != 0)?;
        }
    }

    if flags & bufmgr_seams::EB_CLEAR_SIZE_CACHE != 0 {
        smgr_seams::smgr_set_cached_nblocks::call(smgr, fork, InvalidBlockNumber)?;
    }

    let mut current_size = smgr_seams::smgr_nblocks::call(smgr, fork)?;

    if matches!(mode, ReadBufferMode::ZeroAndLock | ReadBufferMode::ZeroAndCleanupLock) {
        flags |= bufmgr_seams::EB_LOCK_TARGET;
    }

    let mut buffer = InvalidBuffer;
    while current_size < extend_to {
        let mut buffers = [InvalidBuffer; MAX_BUFFERS_TO_EXTEND_BY];
        let mut num_pages = MAX_BUFFERS_TO_EXTEND_BY as u32;
        if current_size as u64 + num_pages as u64 > extend_to as u64 {
            num_pages = extend_to - current_size;
        }
        let (first_block, extended_by) = ExtendBufferedRelCommon(
            rel,
            smgr,
            relpersistence,
            fork,
            &strategy,
            flags,
            num_pages,
            extend_to,
            &mut buffers,
        )?;
        current_size = first_block + extended_by;
        debug_assert!(num_pages != 0 || current_size >= extend_to);

        for (i, buf) in buffers.iter().enumerate().take(extended_by as usize) {
            if first_block + i as u32 != extend_to - 1 {
                crate::pin::ReleaseBuffer(*buf)?;
            } else {
                buffer = *buf;
            }
        }
    }

    // A concurrent extension may have covered extend_to already: fall back to
    // reading the target block (C keeps the caller's original mode here).
    if buffer == InvalidBuffer {
        buffer = ReadBuffer_common(smgr, relpersistence, fork, extend_to - 1, mode, strategy)?.0;
    }
    Ok(buffer)
}

#[allow(clippy::too_many_arguments)]
fn ExtendBufferedRelCommon(
    rel: Option<&RelationData<'_>>,
    smgr: RelFileLocatorBackend,
    relpersistence: u8,
    fork: ForkNumber,
    strategy: &BufferAccessStrategy,
    flags: u32,
    extend_by: u32,
    extend_upto: BlockNumber,
    buffers: &mut [Buffer],
) -> PgResult<(BlockNumber, u32)> {
    if relpersistence == RELPERSISTENCE_TEMP {
        // Canonical place to reject extending another session's temp
        // relation: about-to-be-created local buffers cannot be transferred
        // to the owning session (C 3aaefe8).
        if rel.is_some_and(|r| !r.rd_islocaltemp) {
            return Err(Box::new(
                types_error::PgError::new(ERROR, "cannot access temporary tables of other sessions")
                    .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .with_error_location(ErrorLocation::new(
                        "bufmgr.c",
                        0,
                        "ExtendBufferedRelCommon",
                    )),
            ));
        }
        return crate::localbuf::ExtendBufferedRelLocal(smgr, fork, extend_by, extend_upto, buffers);
    }
    ExtendBufferedRelShared(rel, smgr, relpersistence, fork, strategy, flags, extend_by, extend_upto, buffers)
}

#[allow(clippy::too_many_arguments)]
fn ExtendBufferedRelShared(
    rel: Option<&RelationData<'_>>,
    smgr: RelFileLocatorBackend,
    relpersistence: u8,
    fork: ForkNumber,
    strategy: &BufferAccessStrategy,
    flags: u32,
    mut extend_by: u32,
    extend_upto: BlockNumber,
    buffers: &mut [Buffer],
) -> PgResult<(BlockNumber, u32)> {
    let io_context = IOContextForStrategy(strategy);
    LimitAdditionalPins(&mut extend_by);
    debug_assert!(extend_by as usize <= buffers.len());

    // Victim acquisition and zeroing happen before the extension lock: the
    // pins keep these invalid buffers from being re-victimized.
    for slot in buffers.iter_mut().take(extend_by as usize) {
        *slot = GetVictimBuffer(strategy, io_context)?;
        // SAFETY: pinned, tag-invalid victim: this backend owns the image.
        unsafe { core::ptr::write_bytes(BufferGetBlockPtr(*slot), 0, BLCKSZ) };
    }

    let hold_extension_lock = flags & bufmgr_seams::EB_SKIP_EXTENSION_LOCK == 0;
    if hold_extension_lock {
        let rel = rel.expect("relation extension lock requires a Relation");
        lmgr::LockRelationForExtension(rel, ExclusiveLock)?;
    }

    if flags & bufmgr_seams::EB_CLEAR_SIZE_CACHE != 0 {
        smgr_seams::smgr_set_cached_nblocks::call(smgr, fork, InvalidBlockNumber)?;
    }

    let first_block = smgr_seams::smgr_nblocks::call(smgr, fork)?;

    if extend_upto != InvalidBlockNumber {
        let orig_extend_by = extend_by;
        if first_block > extend_upto {
            extend_by = 0;
        } else if first_block as u64 + extend_by as u64 > extend_upto as u64 {
            extend_by = extend_upto - first_block;
        }
        for buf in buffers.iter().take(orig_extend_by as usize).skip(extend_by as usize) {
            let desc = GetBufferDescriptor(*buf - 1);
            StrategyFreeBuffer(desc.buf_id);
            UnpinBuffer(desc);
        }
        if extend_by == 0 {
            if hold_extension_lock {
                lmgr::UnlockRelationForExtension(rel.unwrap(), ExclusiveLock)?;
            }
            return Ok((first_block, 0));
        }
    }

    if first_block as u64 + extend_by as u64 >= MaxBlockNumber as u64 {
        ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(format!(
                "cannot extend relation base/{}/{} beyond {} blocks",
                smgr.locator.dbOid, smgr.locator.relNumber, MaxBlockNumber
            ))
            .finish(loc("ExtendBufferedRelShared"))?;
    }

    // Insert into the mapping table (IO-in-progress) before smgrzeroextend:
    // once the relation grows, other backends may read these blocks.
    for i in 0..extend_by as usize {
        let victim_buf = buffers[i];
        let victim_desc = GetBufferDescriptor(victim_buf - 1);
        ReservePrivateRefCountEntry();
        crate::pin::resowner_enlarge_for_pin()?;

        let tag = types_storage::buf::buftag {
            spcOid: smgr.locator.spcOid,
            dbOid: smgr.locator.dbOid,
            relNumber: smgr.locator.relNumber,
            forkNum: fork,
            blockNum: first_block + i as u32,
        };
        let hash = BufTableHashCode(&tag);
        let partition_lock = BufMappingPartitionLock(hash);

        LWLockAcquire(partition_lock, LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
        let existing_id = BufTableInsert(&tag, hash, victim_desc.buf_id)?;

        if existing_id >= 0 {
            // A failed prior extension or a beyond-EOF read left a buffer for
            // this block; legitimate leftovers are zero-filled.
            let existing_desc = GetBufferDescriptor(existing_id);
            let valid = PinBuffer(existing_desc, strategy);
            LWLockRelease(partition_lock)?;
            StrategyFreeBuffer(victim_desc.buf_id);
            UnpinBuffer(victim_desc);

            buffers[i] = BufferDescriptorGetBuffer(existing_desc);
            let existing_page = BufferGetBlockPtr(buffers[i]);
            // SAFETY: pinned page image; PageIsNew reads pd_upper at offset 14.
            let page_is_new = unsafe { existing_page.add(14).cast::<u16>().read() == 0 };
            if valid && !page_is_new {
                ereport(ERROR)
                    .errmsg(format!(
                        "unexpected data beyond EOF in block {} of relation base/{}/{}",
                        tag.blockNum, smgr.locator.dbOid, smgr.locator.relNumber
                    ))
                    .errhint("This has been seen to occur with buggy kernels; consider updating your system.")
                    .finish(loc("ExtendBufferedRelShared"))?;
            }
            loop {
                let buf_state = LockBufHdr(existing_desc);
                UnlockBufHdr(existing_desc, buf_state & !BM_VALID);
                if StartBufferIO(existing_desc, true, false, true)? {
                    break;
                }
            }
        } else {
            let mut buf_state = LockBufHdr(victim_desc);
            debug_assert!(buf_state & (BM_VALID | BM_TAG_VALID | BM_DIRTY | BM_JUST_DIRTIED) == 0);
            debug_assert!(buffer_refcount(buf_state) == 1);
            // SAFETY: header lock held, our pin is the only reference.
            unsafe { victim_desc.set_tag(tag) };
            buf_state |= BM_TAG_VALID | BUF_USAGECOUNT_ONE;
            if relpersistence == RELPERSISTENCE_PERMANENT || fork == INIT_FORKNUM {
                buf_state |= BM_PERMANENT;
            }
            UnlockBufHdr(victim_desc, buf_state);
            LWLockRelease(partition_lock)?;
            StartBufferIO(victim_desc, true, false, true)?;
        }
    }

    let io_start = pgstat_prepare_io_time(crate::gucs::track_io_timing());

    smgr_seams::smgr_zeroextend::call(smgr, fork, first_block, extend_by as i32, false)?;

    if hold_extension_lock {
        lmgr::UnlockRelationForExtension(rel.unwrap(), ExclusiveLock)?;
    }

    pgstat_count_io_op_time(
        IOObject::Relation,
        io_context,
        IOOp::Extend,
        io_start,
        1,
        extend_by as u64 * BLCKSZ as u64,
    );

    for i in 0..extend_by as usize {
        let buf = buffers[i];
        let desc = GetBufferDescriptor(buf - 1);
        let lock = (flags & bufmgr_seams::EB_LOCK_FIRST != 0 && i == 0)
            || (flags & bufmgr_seams::EB_LOCK_TARGET != 0 && {
                debug_assert!(extend_upto != InvalidBlockNumber);
                first_block + i as u32 + 1 == extend_upto
            });
        if lock {
            LWLockAcquire(&desc.content_lock, LW_EXCLUSIVE, init_small::globals::MyProcNumber())?;
        }
        TerminateBufferIO(desc, false, BM_VALID, true, false);
    }

    for _ in 0..extend_by {
        crate::counters::written();
    }
    Ok((first_block, extend_by))
}
