use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    pub fn search_syscache_exists_reloid(reloid: Oid) -> PgResult<bool>
);
