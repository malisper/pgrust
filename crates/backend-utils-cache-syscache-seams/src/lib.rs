//! Seam declarations for the `backend-utils-cache-syscache` unit
//! (`utils/cache/syscache.c` `SearchSysCache*` reads), expressed as
//! caller-shaped projected catalog rows.
//!
//! The owning unit installs these from its `init_seams()` when it lands
//! (catcache lookup + deform + field projection — thin marshal only); until
//! then a call panics loudly. A cache miss is `Ok(None)` / an empty list — the
//! caller raises its own `cache lookup failed` error, as in C.
//!
//! The projected rows are copies out of the catcache (the cache entries live
//! in `CacheMemoryContext`), so each lookup takes the caller's `Mcx` and the
//! allocated outputs carry its lifetime; `Err` includes OOM from the copy.

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;
use types_hash::backend_access_hash_hashvalidate::{AmopRow, AmprocRow, OpclassForm};

seam_core::seam!(
    /// `SearchSysCache1(RELOID, ObjectIdGetDatum(relid))` projected to the
    /// `Form_pg_class.relam` field (the relation's access method). `Ok(None)`
    /// on a cache miss (`!HeapTupleIsValid`); the installer owns the
    /// `ReleaseSysCache`.
    pub fn search_relation_relam(relid: Oid) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    /// `SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid))` projected to the
    /// `Form_pg_opclass` fields the hash validator reads, copied into `mcx`.
    /// `Ok(None)` on a cache miss (`!HeapTupleIsValid`).
    pub fn search_opclass<'mcx>(
        mcx: Mcx<'mcx>,
        opclassoid: Oid,
    ) -> PgResult<Option<OpclassForm<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCacheList1(AMOPSTRATEGY, ObjectIdGetDatum(opfamilyoid))`
    /// member rows, projected and copied into `mcx`.
    pub fn search_amop_list<'mcx>(
        mcx: Mcx<'mcx>,
        opfamilyoid: Oid,
    ) -> PgResult<PgVec<'mcx, AmopRow>>
);

seam_core::seam!(
    /// `SearchSysCacheList1(AMPROCNUM, ObjectIdGetDatum(opfamilyoid))`
    /// member rows, projected and copied into `mcx`.
    pub fn search_amproc_list<'mcx>(
        mcx: Mcx<'mcx>,
        opfamilyoid: Oid,
    ) -> PgResult<PgVec<'mcx, AmprocRow>>
);
