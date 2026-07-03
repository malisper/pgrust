use types_core::{BlockNumber, Buffer, ForkNumber, XLogRecPtr};
use types_error::PgResult;
use types_storage::buf::BufferAccessStrategy;
use types_storage::{ReadBufferMode, RelFileLocator, RelFileLocatorBackend};

pub const BUFFER_LOCK_UNLOCK: i32 = 0;
pub const BUFFER_LOCK_SHARE: i32 = 1;
pub const BUFFER_LOCK_EXCLUSIVE: i32 = 2;

pub const EB_SKIP_EXTENSION_LOCK: u32 = 1 << 0;
pub const EB_PERFORMING_RECOVERY: u32 = 1 << 1;

seam_core::seam!(
    pub fn read_recent_buffer(
        rlocator: RelFileLocator,
        forknum: ForkNumber,
        blkno: BlockNumber,
        recent_buffer: Buffer,
    ) -> PgResult<bool>
);

seam_core::seam!(
    pub fn read_buffer_without_relcache(
        rlocator: RelFileLocator,
        forknum: ForkNumber,
        blkno: BlockNumber,
        mode: ReadBufferMode,
        strategy: BufferAccessStrategy,
        permanent: bool,
    ) -> PgResult<Buffer>
);

seam_core::seam!(
    // BMR_SMGR handle crosses as its locator key; resolution stays inside.
    pub fn extend_buffered_rel_to(
        smgr_rlocator: RelFileLocatorBackend,
        forknum: ForkNumber,
        strategy: BufferAccessStrategy,
        flags: u32,
        extend_to: BlockNumber,
        mode: ReadBufferMode,
    ) -> PgResult<Buffer>
);

seam_core::seam!(
    // BMR_REL form: fork creation takes the relation extension lock inside.
    pub fn extend_buffered_rel_to_rel<'a, 'mcx>(
        rel: &'a types_rel::RelationData<'mcx>,
        forknum: ForkNumber,
        strategy: BufferAccessStrategy,
        flags: u32,
        extend_to: BlockNumber,
        mode: ReadBufferMode,
    ) -> PgResult<Buffer>
);

seam_core::seam!(
    pub fn release_buffer(buffer: Buffer) -> PgResult<()>
);

seam_core::seam!(
    pub fn mark_buffer_dirty(buffer: Buffer) -> PgResult<()>
);

seam_core::seam!(
    pub fn flush_one_buffer(buffer: Buffer) -> PgResult<()>
);

seam_core::seam!(
    pub fn lock_buffer(buffer: Buffer, mode: i32) -> PgResult<()>
);

seam_core::seam!(
    pub fn lock_buffer_for_cleanup(buffer: Buffer) -> PgResult<()>
);

seam_core::seam!(
    pub fn conditional_lock_buffer(buffer: Buffer) -> PgResult<bool>
);

seam_core::seam!(
    // Page macros compose with BufferGetPage on the bufmgr side.
    pub fn buffer_page_is_new(buffer: Buffer) -> bool
);

seam_core::seam!(
    pub fn buffer_page_get_lsn(buffer: Buffer) -> XLogRecPtr
);

seam_core::seam!(
    pub fn buffer_page_set_lsn(buffer: Buffer, lsn: XLogRecPtr)
);

seam_core::seam!(
    // RestoreBlockImage's final memcpy onto BufferGetPage(buffer).
    pub fn overwrite_buffer_page(buffer: Buffer, page: &[u8])
);

seam_core::seam!(
    // ReadBuffer(reln, blockNum); the open Relation crosses whole, as C's pointer.
    pub fn read_buffer<'a, 'mcx>(
        rel: &'a types_rel::RelationData<'mcx>,
        block_num: BlockNumber,
    ) -> PgResult<Buffer>
);

seam_core::seam!(
    pub fn read_buffer_strategy<'a, 'mcx>(
        rel: &'a types_rel::RelationData<'mcx>,
        block_num: BlockNumber,
        strategy: BufferAccessStrategy,
    ) -> PgResult<Buffer>
);

seam_core::seam!(
    // ReadBufferExtended(reln, forkNum, blockNum, mode, strategy).
    pub fn read_buffer_extended<'a, 'mcx>(
        rel: &'a types_rel::RelationData<'mcx>,
        forknum: ForkNumber,
        block_num: BlockNumber,
        mode: ReadBufferMode,
        strategy: BufferAccessStrategy,
    ) -> PgResult<Buffer>
);

seam_core::seam!(
    // RelationGetSmgr(rel)'s locator key (RelationInitPhysicalAddr steady
    // state); homed in bufmgr until rd_locator lands on RelationData.
    pub fn relation_smgr_locator<'a, 'mcx>(
        rel: &'a types_rel::RelationData<'mcx>,
    ) -> RelFileLocatorBackend
);

seam_core::seam!(
    pub fn buffer_get_block_number(buffer: Buffer) -> BlockNumber
);

seam_core::seam!(
    // BufferGetPage: BLCKSZ bytes, valid while pinned.
    pub fn buffer_get_page(buffer: Buffer) -> core::ptr::NonNull<u8>
);

seam_core::seam!(
    pub fn incr_buffer_ref_count(buffer: Buffer)
);

seam_core::seam!(
    pub fn get_access_strategy(
        btype: types_storage::buf::BufferAccessStrategyType,
    ) -> BufferAccessStrategy
);

seam_core::seam!(
    pub fn free_access_strategy(strategy: BufferAccessStrategy)
);

seam_core::seam!(
    pub fn relation_get_number_of_blocks_in_fork<'a, 'mcx>(
        rel: &'a types_rel::RelationData<'mcx>,
        fork_num: ForkNumber,
    ) -> PgResult<BlockNumber>
);

seam_core::seam!(
    // DropRelationBuffers(smgr_reln, forkNum, nforks, firstDelBlock)
    // (bufmgr.c); the SMgrRelation handle crosses as its locator key.
    pub fn drop_relation_buffers(
        rlocator: RelFileLocatorBackend,
        forknum: &[ForkNumber],
        first_del_block: &[BlockNumber],
    ) -> PgResult<()>
);

seam_core::seam!(
    // DropRelationsAllBuffers(smgr_reln, nlocators) (bufmgr.c).
    pub fn drop_relations_all_buffers(rlocators: &[RelFileLocatorBackend]) -> PgResult<()>
);

seam_core::seam!(
    // FlushRelationsAllBuffers(smgrs, nrels) (bufmgr.c).
    pub fn flush_relations_all_buffers(rlocators: &[RelFileLocatorBackend]) -> PgResult<()>
);

seam_core::seam!(
    // MarkBufferDirtyHint(buffer, buffer_std) (bufmgr.c); can ereport via
    // XLogSaveBufferForHint.
    pub fn mark_buffer_dirty_hint(buffer: Buffer, buffer_std: bool) -> PgResult<()>
);

seam_core::seam!(
    // BufferIsPermanent(buffer) (bufmgr.c): Assert-only in C.
    pub fn buffer_is_permanent(buffer: Buffer) -> bool
);

seam_core::seam!(
    pub fn buffer_get_lsn_atomic(buffer: Buffer) -> XLogRecPtr
);

seam_core::seam!(
    // ReleaseAndReadBuffer(buffer, relation, blockNum): keeps the pin when the
    // held buffer already holds blockNum (C's same-block fastpath lives inside).
    pub fn release_and_read_buffer<'a, 'mcx>(
        buffer: Buffer,
        rel: &'a types_rel::RelationData<'mcx>,
        block_num: BlockNumber,
    ) -> PgResult<Buffer>
);

pub mod pin;
pub use pin::{BufferPin, ContentLockGuard};

// C's `extern char *BufferBlocks` header global: keeps BufferGetPage a header
// inline instead of a per-tuple seam hop.
static BUFFER_BLOCKS: core::sync::atomic::AtomicPtr<u8> =
    core::sync::atomic::AtomicPtr::new(core::ptr::null_mut());

/// Publish the pool base (spans `NBuffers * BLCKSZ` for the process lifetime).
pub fn publish_buffer_blocks(base: *mut u8) {
    BUFFER_BLOCKS.store(base, core::sync::atomic::Ordering::Release);
}

#[inline]
pub(crate) fn buffer_blocks() -> *mut u8 {
    BUFFER_BLOCKS.load(core::sync::atomic::Ordering::Relaxed)
}

seam_core::seam!(
    // CheckPointBuffers(flags) (bufmgr.c).
    pub fn check_point_buffers(flags: i32) -> PgResult<()>
);

pub const EB_CREATE_FORK_IF_NEEDED: u32 = 1 << 2;
pub const EB_LOCK_FIRST: u32 = 1 << 3;
pub const EB_CLEAR_SIZE_CACHE: u32 = 1 << 4;
pub const EB_LOCK_TARGET: u32 = 1 << 5;

seam_core::seam!(
    // ExtendBufferedRelBy(BMR_REL(rel), fork, strategy, flags, extend_by):
    // returns (first new buffer, extended_by); EB_LOCK_FIRST leaves the first
    // buffer exclusively locked (heap DML extension arm).
    pub fn extend_buffered_rel_by<'a, 'mcx>(
        rel: &'a types_rel::RelationData<'mcx>,
        fork: ForkNumber,
        strategy: BufferAccessStrategy,
        flags: u32,
        extend_by: u32,
    ) -> PgResult<(Buffer, u32)>
);

seam_core::seam!(
    // ConditionalLockBufferForCleanup(buffer) (bufmgr.c): pin held by caller;
    // true = exclusive lock acquired and no other pins.
    pub fn conditional_lock_buffer_for_cleanup(buffer: Buffer) -> PgResult<bool>
);
