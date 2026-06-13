//! Seam declarations for the `backend-catalog-namespace` unit
//! (`catalog/namespace.c`), search-path-aware object name resolution.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;
use mcx::Mcx;
use types_tuple::access::RangeVar;
use types_storage::lock::LOCKMODE;

seam_core::seam!(
    /// `get_ts_config_oid(names, missing_ok)` (namespace.c): the OID of a
    /// text-search configuration given its possibly-qualified name list.
    /// With `missing_ok = false` a missing configuration raises
    /// `ERRCODE_UNDEFINED_OBJECT` (`Err`); with `missing_ok = true` it is
    /// `Ok(InvalidOid)`.
    pub fn get_ts_config_oid(names: &[&str], missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// `get_namespace_oid(nspname, missing_ok)` (namespace.c): the
    /// namespace's OID; with `missing_ok = false` a missing schema raises
    /// `ERRCODE_UNDEFINED_SCHEMA`, carried on `Err`.
    pub fn get_namespace_oid(nspname: &str, missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// `RangeVarGetRelid(relation, lockmode, missing_ok)` (namespace.h macro
    /// over `RangeVarGetRelidExtended` with no callback and `RVR_MISSING_OK`
    /// per `missing_ok`). `mcx` is the C current context the lookup's
    /// transient catalog copies are made in.
    pub fn range_var_get_relid(
        mcx: Mcx<'_>,
        relation: &RangeVar,
        lockmode: LOCKMODE,
        missing_ok: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `LookupExplicitNamespace(nspname, missing_ok)` (namespace.c): resolve
    /// an explicit schema name and verify USAGE rights.
    pub fn lookup_explicit_namespace(nspname: &str, missing_ok: bool) -> PgResult<Oid>
);
