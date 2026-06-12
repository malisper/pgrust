//! Seam declarations for the `backend-utils-cache-lsyscache` unit
//! (`utils/cache/lsyscache.c` convenience catalog lookups).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgString};
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `get_opfamily_name(opfid, missing_ok)` (lsyscache.c): the opfamily's
    /// name, copied out of the syscache into `mcx` (C: `pstrdup` in the
    /// current context). With `missing_ok = false` a missing opfamily raises
    /// (`Err`); with `missing_ok = true` it is `Ok(None)`. `Err` includes OOM
    /// from the copy.
    pub fn get_opfamily_name<'mcx>(
        mcx: Mcx<'mcx>,
        opfid: Oid,
        missing_ok: bool,
    ) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `get_opclass_input_type(opclass)` (lsyscache.c): the opclass's
    /// `opcintype`. A missing opclass is the C `elog(ERROR, "cache lookup
    /// failed for opclass %u")`, carried on `Err`.
    pub fn get_opclass_input_type(opclass: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_relname_relid(relname, relnamespace)` (lsyscache.c):
    /// `GetSysCacheOid2(RELNAMENSP, ...)` — the relation's OID or
    /// `InvalidOid`. `Err` carries catcache-path `ereport(ERROR)`s.
    pub fn get_relname_relid(relname: &str, relnamespace: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_namespace_name(nspid)` (lsyscache.c): the namespace's name
    /// (C: `pstrdup` copy), or `None` if there is no such namespace. `Err`
    /// includes OOM from the copy.
    pub fn get_namespace_name(nspid: Oid) -> PgResult<Option<String>>
);

seam_core::seam!(
    /// `get_rel_relkind(relid)` (lsyscache.c): the relation's relkind char,
    /// or 0 (the C `'\0'`) if there is no such relation.
    pub fn get_rel_relkind(relid: Oid) -> PgResult<u8>
);
