//! Seam declarations for the `backend-utils-cache-syscache` unit
//! (`utils/cache/syscache.c` `SearchSysCache*` reads), expressed as
//! caller-shaped projected catalog rows.
//!
//! The owning unit installs these from its `init_seams()` when it lands
//! (catcache lookup + deform + field projection — thin marshal only); until
//! then a call panics loudly. A cache miss is `Ok(None)` / an empty list — the
//! caller raises its own `cache lookup failed` error, as in C.

use types_core::Oid;
use types_error::PgResult;
use types_hash::backend_access_hash_hashvalidate::{AmopRow, AmprocRow, OpclassForm};

seam_core::seam!(
    /// `SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid))` projected to the
    /// `Form_pg_opclass` fields the hash validator reads. `Ok(None)` on a cache
    /// miss (`!HeapTupleIsValid`).
    pub fn search_opclass(opclassoid: Oid) -> PgResult<Option<OpclassForm>>
);

seam_core::seam!(
    /// `SearchSysCacheList1(AMOPSTRATEGY, ObjectIdGetDatum(opfamilyoid))`
    /// member rows, projected.
    pub fn search_amop_list(opfamilyoid: Oid) -> PgResult<Vec<AmopRow>>
);

seam_core::seam!(
    /// `SearchSysCacheList1(AMPROCNUM, ObjectIdGetDatum(opfamilyoid))`
    /// member rows, projected.
    pub fn search_amproc_list(opfamilyoid: Oid) -> PgResult<Vec<AmprocRow>>
);
