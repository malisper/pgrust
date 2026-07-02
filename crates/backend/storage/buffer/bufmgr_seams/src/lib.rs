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
    pub fn at_eoxact_buffers(is_commit: bool)
);

seam_core::seam!(
    pub fn unlock_buffers()
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

pub mod pin;
pub use pin::{BufferPin, ContentLockGuard};
