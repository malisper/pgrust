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
