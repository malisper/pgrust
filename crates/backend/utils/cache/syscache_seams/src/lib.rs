use types_core::Oid;
use types_error::PgResult;

// SearchSysCacheExists1(RELOID, ObjectIdGetDatum(reloid)); can ereport on a
// cache-miss catalog scan.
seam_core::seam!(
    pub fn search_syscache_exists_reloid(reloid: Oid) -> PgResult<bool>
);
