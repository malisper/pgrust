//! bufmgr.c + buf_init.c + buf_table.c + freelist.c read/pin/mapping/eviction
//! core, plus the checkpoint write-back lane (FlushBuffer/BufferSync/
//! CheckPointBuffers). Extend, drop-rel, localbuf, AIO and hint-bit lanes are
//! phase 2: every entry point is a loud panic naming its C function.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

mod bgwriter_sync;
mod buf_hdr;
mod buf_table;
pub mod counters;
mod freelist;
mod gucs;
mod ops;
mod pin;
mod privref;
mod read;
mod write;

use types_core::{
    BlockNumber, Buffer, ForkNumber, Oid, INVALID_PROC_NUMBER, RELPERSISTENCE_TEMP,
};
use types_error::{ErrorLocation, PgResult, ERROR};
use types_storage::buf::BufferAccessStrategy;
use types_storage::{ReadBufferMode, RelFileLocator, RelFileLocatorBackend};
use types_rel::rel::RelationData;

pub use buf_hdr::{
    BufferDesc, BufferDescriptorGetBuffer, BufferGetBlockPtr, BufferManagerShmemInit,
    GetBufferDescriptor, LockBufHdr, NBuffersInited, UnlockBufHdr, BUFFERDESC_PAD_TO_SIZE,
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
    ConditionalLockBuffer, LockBuffer, LockBufferForCleanup, MarkBufferDirty,
    UnlockReleaseBuffer, BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK,
};
pub use bgwriter_sync::{bgwriter_writeback_context_init, pending_bgwriter_stats, BgBufferSync};
pub use write::{BufferSync, CheckPointBuffers, FlushOneBuffer};
pub use pin::{
    AtEOXact_Buffers, BufferIsPinned, CheckBufferIsPinnedOnce, IncrBufferRefCount, ReleaseBuffer,
    UnlockBuffers,
};
pub use privref::{GetPrivateRefCount, ReservePrivateRefCountEntry};
pub use read::{ReadBufferWithoutRelcache, ReadBuffer_common, ReadRecentBuffer};

const DEFAULTTABLESPACE_OID: Oid = 1663;
const GLOBALTABLESPACE_OID: Oid = 1664;

/// RelationInitPhysicalAddr's steady-state rules (relcache.c), pending rd_locator.
fn rel_locator_backend(rel: &RelationData<'_>) -> RelFileLocatorBackend {
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
    RelFileLocatorBackend {
        locator: RelFileLocator {
            spcOid: spc,
            dbOid: db,
            relNumber: rel_number,
        },
        backend: if form.relpersistence == RELPERSISTENCE_TEMP {
            rel.rd_backend
        } else {
            INVALID_PROC_NUMBER
        },
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
            .with_error_location(ErrorLocation::new("bufmgr.c", 0, "ReadBufferExtended")),
        ));
    }
    read::ReadBuffer_common(
        rel_locator_backend(rel),
        rel.rd_rel.relpersistence,
        forknum,
        block_num,
        mode,
        strategy,
    )
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
            panic!("unported callee reached from bufmgr.c ReleaseAndReadBuffer: local buffers (localbuf.c)");
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

/// RelationGetNumberOfBlocksInFork (bufmgr.c): smgrnblocks via the smgr seam.
pub fn RelationGetNumberOfBlocksInFork(
    rel: &RelationData<'_>,
    forknum: ForkNumber,
) -> PgResult<BlockNumber> {
    smgr_seams::smgr_nblocks::call(rel_locator_backend(rel), forknum)
}

macro_rules! unported {
    ($(fn $name:ident($($ty:ty),*) -> $ret:ty, $cfn:literal;)+) => {
        $(pub fn $name($(_: $ty),*) -> $ret {
            panic!(concat!("unported callee reached from bufmgr.c: ", $cfn, " (phase 2)"));
        })+
    };
}

unported! {
    fn FlushRelationBuffers(RelFileLocatorBackend) -> (), "FlushRelationBuffers";
    fn FlushDatabaseBuffers(Oid) -> (), "FlushDatabaseBuffers";
    fn DropDatabaseBuffers(Oid) -> (), "DropDatabaseBuffers";
    fn MarkBufferDirtyHint(Buffer, bool) -> (), "MarkBufferDirtyHint (needs XLogSaveBufferForHint)";
    fn BufferGetLSNAtomic(Buffer) -> u64, "BufferGetLSNAtomic";
    fn BufferIsPermanent(Buffer) -> bool, "BufferIsPermanent";
    fn ConditionalLockBufferForCleanup(Buffer) -> bool, "ConditionalLockBufferForCleanup";
    fn IsBufferCleanupOK(Buffer) -> bool, "IsBufferCleanupOK";
    fn HoldingBufferPinThatDelaysRecovery() -> bool, "HoldingBufferPinThatDelaysRecovery";
    fn PrefetchBuffer(RelFileLocatorBackend, ForkNumber, BlockNumber) -> (), "PrefetchBuffer";
}

/// Private-refcount TLS is const-init; AtProcExit leak check pends proc unit.
pub fn InitBufferManagerAccess() {}

pub fn init_seams() {
    gucs::install_guc_backing();

    bufmgr_seams::read_recent_buffer::set(read::ReadRecentBuffer);
    bufmgr_seams::read_buffer_without_relcache::set(read::ReadBufferWithoutRelcache);
    bufmgr_seams::extend_buffered_rel_to::set(|_, _, _, _, _, _| {
        panic!("unported callee reached from bufmgr.c: ExtendBufferedRelTo (extend machinery, phase 2)")
    });
    bufmgr_seams::extend_buffered_rel_by::set(|_, _, _, _, _| {
        panic!("unported callee reached from bufmgr.c: ExtendBufferedRelBy (extend machinery, phase 2)")
    });
    bufmgr_seams::release_buffer::set(pin::ReleaseBuffer);
    bufmgr_seams::mark_buffer_dirty::set(ops::MarkBufferDirty);
    bufmgr_seams::flush_one_buffer::set(write::FlushOneBuffer);
    bufmgr_seams::check_point_buffers::set(write::CheckPointBuffers);
    bufmgr_seams::lock_buffer::set(ops::LockBuffer);
    bufmgr_seams::lock_buffer_for_cleanup::set(ops::LockBufferForCleanup);
    bufmgr_seams::buffer_page_is_new::set(ops::buffer_page_is_new);
    bufmgr_seams::buffer_page_get_lsn::set(ops::buffer_page_get_lsn);
    bufmgr_seams::buffer_page_set_lsn::set(ops::buffer_page_set_lsn);
    bufmgr_seams::overwrite_buffer_page::set(ops::overwrite_buffer_page);
    bufmgr_seams::at_eoxact_buffers::set(pin::AtEOXact_Buffers);
    bufmgr_seams::unlock_buffers::set(pin::UnlockBuffers);
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
    bufmgr_seams::read_buffer_extended::set(ReadBufferExtended);
    bufmgr_seams::relation_smgr_locator::set(rel_locator_backend);
    bufmgr_seams::buffer_get_block_number::set(ops::BufferGetBlockNumber);
    bufmgr_seams::buffer_get_page::set(ops::BufferGetPagePtr);
    bufmgr_seams::incr_buffer_ref_count::set(pin::IncrBufferRefCount);
    bufmgr_seams::get_access_strategy::set(freelist::GetAccessStrategy);
    bufmgr_seams::free_access_strategy::set(freelist::FreeAccessStrategy);
    bufmgr_seams::relation_get_number_of_blocks_in_fork::set(RelationGetNumberOfBlocksInFork);
    bufmgr_seams::drop_relation_buffers::set(|_, _, _| {
        panic!("unported callee reached from bufmgr.c: DropRelationBuffers (phase 2)")
    });
    bufmgr_seams::drop_relations_all_buffers::set(|_| {
        panic!("unported callee reached from bufmgr.c: DropRelationsAllBuffers (phase 2)")
    });
    bufmgr_seams::flush_relations_all_buffers::set(|_| {
        panic!("unported callee reached from bufmgr.c: FlushRelationsAllBuffers (write-back, phase 2)")
    });
    bufmgr_seams::mark_buffer_dirty_hint::set(|b, s| {
        MarkBufferDirtyHint(b, s);
        Ok(())
    });
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
        let desc = crate::buf_hdr::GetBufferDescriptor(buffer - 1);
        crate::pin::PinBuffer(desc, &None);
        crate::pin::UnpinBuffer(desc);
    }
}

#[cfg(test)]
mod tests;
