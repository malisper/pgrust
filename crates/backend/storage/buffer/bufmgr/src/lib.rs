//! bufmgr.c + buf_init.c + buf_table.c + freelist.c read/pin/mapping/eviction
//! core, the checkpoint write-back lane (FlushBuffer/BufferSync/
//! CheckPointBuffers), the extend lane (ExtendBufferedRelBy/To), and localbuf
//! (backend-local temp buffers, negative Buffer ids). AIO and the remaining
//! write-back arms are phase 2: every entry point is a loud panic naming its
//! C function.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

mod aio_read;
mod bgwriter_sync;
mod buf_hdr;
mod buf_table;
pub mod counters;
mod extend;
mod freelist;
mod gucs;
mod localbuf;
mod ops;
mod pin;
mod privref;
mod dbcopy;
mod evict;
mod read;
mod uring;
mod write;
mod drop_buffers;

use types_core::{
    BlockNumber, Buffer, ForkNumber, Oid, INVALID_PROC_NUMBER, RELPERSISTENCE_TEMP,
};
use types_error::{ErrorLocation, PgResult, ERROR};
use types_storage::buf::BufferAccessStrategy;
use types_storage::{ReadBufferMode, RelFileLocator, RelFileLocatorBackend};
use types_rel::rel::RelationData;

pub use buf_hdr::{
    BufferDesc, BufferDescriptorGetBuffer, BufferGetBlockPtr, BufferManagerShmemInit,
    BufferManagerShmemResetAfterCrash, GetBufferDescriptor, LockBufHdr, NBuffersInited,
    UnlockBufHdr, BUFFERDESC_PAD_TO_SIZE,
};
pub use buf_table::{BufMappingPartitionLock, BufTableHashCode, BufTableLookup};
pub use freelist::{
    have_free_buffer, FreeAccessStrategy, GetAccessStrategy, GetAccessStrategyWithSize,
    GetPinLimit, IOContextForStrategy, StrategyFreeBuffer, StrategyGetBuffer,
    StrategyNotifyBgWriter, StrategySyncStart,
};
pub use ops::{
    buffer_page_get_lsn, buffer_page_is_new, buffer_page_ref, buffer_page_set_lsn,
    overwrite_buffer_page, BufferGetBlockNumber, BufferGetPagePtr, BufferGetTag,
    ConditionalLockBuffer, IsBufferCleanupOK, LockBuffer, LockBufferForCleanup, MarkBufferDirty,
    UnlockReleaseBuffer, BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK,
};
pub use bgwriter_sync::{BgBufferSync, BgwSyncState};
pub use write::{BufferSync, CheckPointBuffers, FlushOneBuffer, PageSetChecksumInplace};
pub use pin::{
    AtEOXact_Buffers, BufferIsPinned, CheckBufferIsPinnedOnce, IncrBufferRefCount, ReleaseBuffer,
    UnlockBuffers,
};
pub use privref::{debug_all_private_pins, GetPrivateRefCount, ReservePrivateRefCountEntry};
pub use evict::{EvictAllUnpinnedBuffers, EvictCounts, EvictRelUnpinnedBuffers, EvictUnpinnedBuffer};

// Diagnostic (PGRUST_REDO_PIN_CHECK): wait out this thread's in-flight uring
// prefetch pins so the check sees only genuine leaks.
pub fn debug_drain_prefetch_pins() {
    uring::drain_own();
}

// Diagnostic (PGRUST_REDO_PIN_CHECK): tag string for a pinned buffer.
pub fn debug_buffer_tag_string(buffer: types_core::Buffer) -> String {
    let t = buf_hdr::GetBufferDescriptor(buffer - 1).tag();
    format!(
        "({}/{}/{} fork={} blk={})",
        t.spcOid, t.dbOid, t.relNumber, t.forkNum as i32, t.blockNum
    )
}
pub use localbuf::{
    n_loc_buffer, AtEOXact_LocalBuffers, AtProcExit_LocalBuffers, DropRelationAllLocalBuffers,
    DropRelationLocalBuffers,
};
pub use gucs::ignore_checksum_failure;
pub use read::{
    page_is_verified, relpath_backend_desc, relpath_desc, ReadBufferWithoutRelcache,
    ReadBuffer_common, ReadRecentBuffer, PIV_IGNORE_CHECKSUM_FAILURE, PIV_LOG_LOG, PIV_LOG_WARNING,
};
pub use extend::{ExtendBufferedRelBy, ExtendBufferedRelTo, ExtendBufferedRelToSmgr};

const DEFAULTTABLESPACE_OID: Oid = 1663;
const GLOBALTABLESPACE_OID: Oid = 1664;

/// rd_locator read; the compute arm backfills entries built outside relcache
/// (tests, pre-InitPhysicalAddr builds) with RelationInitPhysicalAddr's
/// steady-state rules — C's invariant is "valid before any smgr access".
/// Twin of smgr::rel_file_locator (seam boundary keeps the crates apart).
fn rel_locator_backend(rel: &RelationData<'_>) -> RelFileLocatorBackend {
    let mut locator = rel.rd_locator.get();
    if locator.relNumber == 0 {
        locator = compute_rel_locator(rel);
        rel.rd_locator.set(locator);
    }
    RelFileLocatorBackend {
        locator,
        backend: if rel.rd_rel.relpersistence == RELPERSISTENCE_TEMP {
            rel.rd_backend
        } else {
            INVALID_PROC_NUMBER
        },
    }
}

#[cold]
#[inline(never)]
fn compute_rel_locator(rel: &RelationData<'_>) -> RelFileLocator {
    let form = &rel.rd_rel;
    let rel_number = if form.relfilenode == 0 {
        let n = relmapper_seams::relation_map_oid_to_filenumber::call(rel.rd_id, form.relisshared);
        // C elog(ERROR)s on a missing mapping; can't-happen once maps load.
        assert!(
            n != 0,
            "could not find relation mapping for relation \"{}\", OID {}",
            String::from_utf8_lossy(form.relname.name_str()),
            rel.rd_id
        );
        n
    } else {
        form.relfilenode
    };
    let spc = if form.reltablespace != 0 {
        form.reltablespace
    } else {
        DEFAULTTABLESPACE_OID
    };
    let db = if spc == GLOBALTABLESPACE_OID {
        0
    } else {
        init_small::globals::MyDatabaseId()
    };
    RelFileLocator {
        spcOid: spc,
        dbOid: db,
        relNumber: rel_number,
    }
}

pub fn ReadBuffer(rel: &RelationData<'_>, block_num: BlockNumber) -> PgResult<Buffer> {
    ReadBufferExtended(
        rel,
        ForkNumber::MAIN_FORKNUM,
        block_num,
        ReadBufferMode::Normal,
        None,
    )
}

pub fn ReadBufferExtended(
    rel: &RelationData<'_>,
    forknum: ForkNumber,
    block_num: BlockNumber,
    mode: ReadBufferMode,
    strategy: BufferAccessStrategy,
) -> PgResult<Buffer> {
    if rel.rd_rel.relpersistence == RELPERSISTENCE_TEMP && !rel.rd_islocaltemp {
        return Err(Box::new(
            types_error::PgError::new(
                ERROR,
                "cannot access temporary tables of other sessions",
            )
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "ReadBufferExtended")),
        ));
    }
    let (buffer, hit) = read::ReadBuffer_common(
        rel_locator_backend(rel),
        rel.rd_rel.relpersistence,
        forknum,
        block_num,
        mode,
        strategy,
    )?;
    pgstat_count_buffer(rel, hit);
    Ok(buffer)
}

// bufmgr.c:1166-1168: per-relation blocks_fetched/blocks_hit; the
// pgstat_enabled Cell is C's pgstat_should_count_relation, and pgstat_link is
// C's rel->pgstat_info — count through the cached pointer while its gen is
// current, re-assoc (one map probe) when pgstat invalidated it.
fn pgstat_count_buffer(rel: &RelationData<'_>, hit: bool) {
    if !rel.pgstat_enabled.get() {
        return;
    }
    let cur = pgstat::relation::pgstat_relation_link_gen();
    let (gen, mut counts) = rel.pgstat_link.get();
    if gen != cur || counts.is_null() {
        counts = pgstat::relation::pgstat_relation_link_counts(rel.rd_id, rel.rd_rel.relisshared);
        rel.pgstat_link.set((cur, counts));
    }
    unsafe { pgstat::relation::pgstat_count_buffer_read_via(counts, hit) };
}

/// Sequential-batch ReadBuffer (StartReadBuffers collapsed): see
/// read::ReadBuffer_batched. Hit path is identical to ReadBuffer.
pub fn ReadBufferBatched(
    rel: &RelationData<'_>,
    block_num: BlockNumber,
    nblocks_hint: BlockNumber,
    strategy: BufferAccessStrategy,
) -> PgResult<Buffer> {
    if rel.rd_rel.relpersistence == RELPERSISTENCE_TEMP && !rel.rd_islocaltemp {
        return Err(Box::new(
            types_error::PgError::new(
                ERROR,
                "cannot access temporary tables of other sessions",
            )
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "ReadBufferExtended")),
        ));
    }
    let (buffer, hit) = read::ReadBuffer_batched(
        rel_locator_backend(rel),
        rel.rd_rel.relpersistence,
        block_num,
        nblocks_hint,
        strategy,
    )?;
    pgstat_count_buffer(rel, hit);
    Ok(buffer)
}

/// Same-block fastpath keeps the pin (heapam's re-read path).
pub fn ReleaseAndReadBuffer(
    buffer: Buffer,
    rel: &RelationData<'_>,
    block_num: BlockNumber,
) -> PgResult<Buffer> {
    if types_core::BufferIsValid(buffer) {
        debug_assert!(BufferIsPinned(buffer));
        if buffer < 0 {
            let tag = localbuf::local_desc(buffer).tag();
            let loc = rel_locator_backend(rel).locator;
            if tag.blockNum == block_num
                && tag.spcOid == loc.spcOid
                && tag.dbOid == loc.dbOid
                && tag.relNumber == loc.relNumber
                && tag.forkNum == ForkNumber::MAIN_FORKNUM
            {
                return Ok(buffer);
            }
            localbuf::UnpinLocalBuffer(buffer);
            return ReadBuffer(rel, block_num);
        }
        let tag = GetBufferDescriptor(buffer - 1).tag();
        let loc = rel_locator_backend(rel).locator;
        if tag.blockNum == block_num
            && tag.spcOid == loc.spcOid
            && tag.dbOid == loc.dbOid
            && tag.relNumber == loc.relNumber
            && tag.forkNum == ForkNumber::MAIN_FORKNUM
        {
            return Ok(buffer);
        }
        pin::ReleaseBuffer(buffer)?;
    }
    ReadBuffer(rel, block_num)
}

/// RelationGetNumberOfBlocksInFork (bufmgr.c): smgrnblocks(RelationGetSmgr(rel), ..)
/// — the rd_smgr pin keeps the smgr entry and its fds alive across queries.
pub fn RelationGetNumberOfBlocksInFork(
    rel: &RelationData<'_>,
    forknum: ForkNumber,
) -> PgResult<BlockNumber> {
    smgr_seams::rel_smgr_nblocks::call(rel, forknum)
}

macro_rules! unported {
    ($(fn $name:ident($($ty:ty),*) -> $ret:ty, $cfn:literal;)+) => {
        $(pub fn $name($(_: $ty),*) -> $ret {
            panic!(concat!("unported callee reached from bufmgr.c: ", $cfn, " (phase 2)"));
        })+
    };
}

/// FlushRelationBuffers (bufmgr.c): the shared arm's per-buffer flush loop is
/// FlushRelationsAllBuffers with one locator (same header-locked scan).
pub fn FlushRelationBuffers(rlocator: RelFileLocatorBackend) -> PgResult<()> {
    if rlocator.backend != INVALID_PROC_NUMBER {
        return localbuf::FlushRelationLocalBuffers(rlocator.locator);
    }
    write::FlushRelationsAllBuffers(&[rlocator])
}

pub use dbcopy::CreateAndCopyRelationData;
pub use drop_buffers::DropDatabaseBuffers;
pub use write::FlushDatabaseBuffers;

// HoldingBufferPinThatDelaysRecovery (bufmgr.c:5822): are we holding a pin on
// the buffer the Startup process is waiting for? bufid may already be cleared
// (slow wake / spurious interrupt): do nothing then.
pub fn HoldingBufferPinThatDelaysRecovery() -> bool {
    let bufid = lmgr_proc::GetStartupBufferPinWaitBufId();
    if bufid < 0 {
        return false;
    }
    privref::GetPrivateRefCount(bufid + 1) > 0
}

pub use read::{PrefetchBuffer, PrefetchBufferResult, PrefetchOutcome, PrefetchSharedBuffer};
pub use uring::{
    collect_done as uring_collect_pins, drain_own as uring_drain_pins,
    start_read as uring_start_read, uring_clear_io_wref, uring_read_complete, uring_set_io_wref,
};

pub fn BufferIsPermanent(buffer: Buffer) -> bool {
    if buffer < 0 {
        return false;
    }
    debug_assert!(pin::BufferIsPinned(buffer));
    let desc = GetBufferDescriptor(buffer - 1);
    desc.state.load(core::sync::atomic::Ordering::Relaxed) & types_storage::buf::BM_PERMANENT != 0
}

// XLogHintBitIsNeeded() (xlog.h). Uninstalled slots read as their boot
// defaults (false / checksums-off) until the owning units wire them.
fn xlog_hint_bit_is_needed() -> bool {
    (guc_tables::vars::wal_log_hints.installed() && guc_tables::vars::wal_log_hints.read())
        || (transam_xlog_seams::data_checksums_enabled::is_installed()
            && transam_xlog_seams::data_checksums_enabled::call())
}

pub fn BufferGetLSNAtomic(buffer: Buffer) -> types_core::XLogRecPtr {
    if !xlog_hint_bit_is_needed() || buffer < 0 {
        return ops::buffer_page_get_lsn(buffer);
    }
    debug_assert!(types_core::BufferIsValid(buffer));
    debug_assert!(pin::BufferIsPinned(buffer));
    let desc = GetBufferDescriptor(buffer - 1);
    let state = LockBufHdr(desc);
    let lsn = ops::buffer_page_get_lsn(buffer);
    UnlockBufHdr(desc, state);
    lsn
}

// DELAY_CHKPT_START (proc.h).
const DELAY_CHKPT_START: i32 = 1 << 0;

pub fn MarkBufferDirtyHint(buffer: Buffer, buffer_std: bool) -> PgResult<()> {
    use types_storage::buf::{BM_DIRTY, BM_JUST_DIRTIED, BM_PERMANENT};
    if !types_core::BufferIsValid(buffer) {
        return Err(Box::new(types_error::PgError::new(
            ERROR,
            format!("bad buffer ID: {buffer}"),
        )));
    }
    if buffer < 0 {
        localbuf::MarkLocalBufferDirty(buffer);
        return Ok(());
    }
    let desc = GetBufferDescriptor(buffer - 1);
    debug_assert!(GetPrivateRefCount(buffer) > 0);

    let state = desc.state.load(core::sync::atomic::Ordering::Relaxed);
    if state & (BM_DIRTY | BM_JUST_DIRTIED) == (BM_DIRTY | BM_JUST_DIRTIED) {
        return Ok(());
    }

    let mut lsn: types_core::XLogRecPtr = 0;
    let mut delay_chkpt = false;
    if xlog_hint_bit_is_needed() && state & BM_PERMANENT != 0 {
        // No WAL in recovery or for a WAL-skipped relfilelocator: leave the
        // page clean so the hint is lost on eviction instead of torn on disk.
        let tag = ops::BufferGetTag(buffer);
        if transam_xlog_seams::recovery_in_progress::call()
            || catalog_storage_seams::rel_file_locator_skipping_wal::call(
                types_storage::RelFileLocator::new(tag.spcOid, tag.dbOid, tag.relNumber),
            )
        {
            return Ok(());
        }
        // The dirty-page-without-usable-LSN window must not span a
        // checkpoint's REDO-pointer read (C delayChkptFlags contract).
        if let Some(procno) = lmgr_proc::MyProc() {
            lmgr_proc::GetPGProcByNumber(procno)
                .delayChkptFlags
                .fetch_or(DELAY_CHKPT_START, core::sync::atomic::Ordering::Relaxed);
            delay_chkpt = true;
        }
        lsn = xloginsert_seams::xlog_save_buffer_for_hint::call(buffer, buffer_std)?;
    }

    let mut buf_state = LockBufHdr(desc);
    debug_assert!(pin::buffer_refcount(buf_state) > 0);
    let mut dirtied = false;
    if buf_state & BM_DIRTY == 0 {
        dirtied = true;
        if lsn != 0 {
            ops::buffer_page_set_lsn(buffer, lsn);
        }
    }
    buf_state |= BM_DIRTY | BM_JUST_DIRTIED;
    UnlockBufHdr(desc, buf_state);

    if delay_chkpt {
        if let Some(procno) = lmgr_proc::MyProc() {
            lmgr_proc::GetPGProcByNumber(procno)
                .delayChkptFlags
                .fetch_and(!DELAY_CHKPT_START, core::sync::atomic::Ordering::Relaxed);
        }
    }
    if dirtied {
        counters::dirtied();
    }
    Ok(())
}

/// Private-refcount TLS is const-init; AtProcExit leak check pends proc unit.
pub fn InitBufferManagerAccess() {}

pub fn init_seams() {
    aio_read::init_seams();
    gucs::install_guc_backing();
    localbuf::install_check_temp_buffers_hook();

    bufmgr_seams::read_recent_buffer::set(read::ReadRecentBuffer);
    bufmgr_seams::read_buffer_without_relcache::set(read::ReadBufferWithoutRelcache);
    // The BMR_SMGR seam form carries no relpersistence: its only callers
    // (recovery/init paths) extend permanent relations, per C's call sites.
    bufmgr_seams::extend_buffered_rel_to::set(|smgr, fork, strategy, flags, extend_to, mode| {
        extend::ExtendBufferedRelToSmgr(
            smgr,
            types_core::RELPERSISTENCE_PERMANENT,
            fork,
            strategy,
            flags,
            extend_to,
            mode,
        )
    });
    bufmgr_seams::extend_buffered_rel_by::set(extend::ExtendBufferedRelBy);
    bufmgr_seams::extend_buffered_rel_to_rel::set(extend::ExtendBufferedRelTo);
    bufmgr_seams::release_buffer::set(pin::ReleaseBuffer);
    bufmgr_seams::mark_buffer_dirty::set(ops::MarkBufferDirty);
    bufmgr_seams::flush_one_buffer::set(write::FlushOneBuffer);
    bufmgr_seams::check_point_buffers::set(write::CheckPointBuffers);
    bufmgr_seams::lock_buffer::set(ops::LockBuffer);
    bufmgr_seams::conditional_lock_buffer::set(ops::ConditionalLockBuffer);
    bufmgr_seams::lock_buffer_for_cleanup::set(ops::LockBufferForCleanup);
    bufmgr_seams::conditional_lock_buffer_for_cleanup::set(ops::ConditionalLockBufferForCleanup);
    bufmgr_seams::buffer_page_is_new::set(ops::buffer_page_is_new);
    bufmgr_seams::buffer_page_get_lsn::set(ops::buffer_page_get_lsn);
    bufmgr_seams::buffer_page_set_lsn::set(ops::buffer_page_set_lsn);
    bufmgr_seams::overwrite_buffer_page::set(ops::overwrite_buffer_page);
    bufmgr_seams::read_buffer::set(ReadBuffer);
    bufmgr_seams::release_and_read_buffer::set(ReleaseAndReadBuffer);
    bufmgr_seams::read_buffer_strategy::set(|rel, blkno, strategy| {
        ReadBufferExtended(
            rel,
            ForkNumber::MAIN_FORKNUM,
            blkno,
            ReadBufferMode::Normal,
            strategy,
        )
    });
    bufmgr_seams::read_buffer_batched::set(ReadBufferBatched);
    bufmgr_seams::read_buffer_extended::set(ReadBufferExtended);
    bufmgr_seams::prefetch_buffer::set(|rel, forknum, blkno| {
        Ok(!matches!(
            read::PrefetchBuffer(rel, forknum, blkno)?,
            read::PrefetchOutcome::Skipped
        ))
    });
    bufmgr_seams::relation_smgr_locator::set(rel_locator_backend);
    bufmgr_seams::buffer_get_block_number::set(ops::BufferGetBlockNumber);
    bufmgr_seams::buffer_get_page::set(ops::BufferGetPagePtr);
    bufmgr_seams::incr_buffer_ref_count::set(pin::IncrBufferRefCount);
    bufmgr_seams::get_access_strategy::set(freelist::GetAccessStrategy);
    bufmgr_seams::free_access_strategy::set(freelist::FreeAccessStrategy);
    bufmgr_seams::get_access_strategy_with_size::set(freelist::GetAccessStrategyWithSize);
    bufmgr_seams::get_access_strategy_buffer_count::set(freelist::GetAccessStrategyBufferCount);
    bufmgr_seams::relation_get_number_of_blocks_in_fork::set(RelationGetNumberOfBlocksInFork);
    bufmgr_seams::drop_relation_buffers::set(drop_buffers::DropRelationBuffers);
    bufmgr_seams::drop_relations_all_buffers::set(drop_buffers::DropRelationsAllBuffers);
    bufmgr_seams::flush_relations_all_buffers::set(write::FlushRelationsAllBuffers);
    bufmgr_seams::flush_relation_buffers::set(FlushRelationBuffers);
    bufmgr_seams::mark_buffer_dirty_hint::set(MarkBufferDirtyHint);
    bufmgr_seams::buffer_is_permanent::set(BufferIsPermanent);
    bufmgr_seams::buffer_get_lsn_atomic::set(BufferGetLSNAtomic);
}

// Internal pin kernel exposure for bench/rig only (PinBuffer is pub(crate)).
#[doc(hidden)]
pub mod bench {
    use types_core::Buffer;

    #[inline]
    pub fn pin_unpin(buffer: Buffer) {
        crate::privref::ReservePrivateRefCountEntry();
        crate::pin::resowner_enlarge_for_pin().expect("ResourceOwnerEnlarge");
        let desc = crate::buf_hdr::GetBufferDescriptor(buffer - 1);
        crate::pin::PinBuffer(desc, &None);
        crate::pin::UnpinBuffer(desc);
    }
}

#[cfg(test)]
mod tests;
