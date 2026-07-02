use types_core::{BlockNumber, ForkNumber};
use types_error::PgResult;
use types_storage::RelFileLocatorBackend;

seam_core::seam!(
    pub fn smgr_release_rel_locator(rlocator: RelFileLocatorBackend) -> PgResult<()>
);

seam_core::seam!(
    // smgrcreate(smgropen(rlocator), forknum, isRedo) (smgr.c); C's
    // SMgrRelation handle crosses as its locator key — the open-handle cache
    // lookup stays inside smgr.
    pub fn smgr_create(rlocator: RelFileLocatorBackend, forknum: ForkNumber, is_redo: bool) -> PgResult<()>
);

seam_core::seam!(
    // smgrnblocks(smgropen(rlocator), forknum) (smgr.c).
    pub fn smgr_nblocks(rlocator: RelFileLocatorBackend, forknum: ForkNumber) -> PgResult<BlockNumber>
);

seam_core::seam!(
    // smgrdestroyall() (smgr.c).
    pub fn smgr_destroy_all() -> PgResult<()>
);

seam_core::seam!(
    // smgrreadv(smgropen(rlocator), forknum, blocknum, 1 iov) (smgr.c);
    // buffer is exactly BLCKSZ.
    pub fn smgr_read(
        rlocator: RelFileLocatorBackend,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        buffer: &mut [u8],
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn at_eoxact_smgr()
);

seam_core::seam!(
    // ProcessBarrierSmgrRelease() (smgr.c); barrier processors may ereport.
    pub fn process_barrier_smgr_release() -> PgResult<bool>
);

seam_core::seam!(
    // smgrwrite(smgropen(rlocator), forknum, blocknum, buffer, skipFsync)
    // (smgr.c); buffer is exactly BLCKSZ.
    pub fn smgr_write(
        rlocator: RelFileLocatorBackend,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        buffer: &[u8],
        skip_fsync: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    // smgrwriteback(smgropen(rlocator), forknum, blocknum, nblocks) (smgr.c).
    pub fn smgr_writeback(
        rlocator: RelFileLocatorBackend,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        nblocks: BlockNumber,
    ) -> PgResult<()>
);
