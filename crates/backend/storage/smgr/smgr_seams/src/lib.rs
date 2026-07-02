use types_error::PgResult;
use types_storage::RelFileLocatorBackend;

seam_core::seam!(
    pub fn smgr_release_rel_locator(rlocator: RelFileLocatorBackend) -> PgResult<()>
);
