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
        flags: u32,
        extend_to: BlockNumber,
        mode: ReadBufferMode,
    ) -> PgResult<Buffer>
);

seam_core::seam!(
    pub fn release_buffer(buffer: Buffer)
);

seam_core::seam!(
    pub fn mark_buffer_dirty(buffer: Buffer)
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
