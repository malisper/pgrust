//! Seam declarations for the `backend-catalog-namespace` unit
//! (`catalog/namespace.c`), for callers that would otherwise form a
//! dependency cycle (catalog/commands layers above it). The owning crate
//! installs every one of these from its `init_seams()`.

use types_core::Oid;
use types_error::PgResult;
use types_nodes::parsenodes::RangeVar;
use types_tuple::access::LOCKMODE;

seam_core::seam!(
    /// `get_namespace_oid(nspname, missing_ok)` (namespace.c): the
    /// namespace's OID; with `missing_ok = false` a missing schema raises
    /// `ERRCODE_UNDEFINED_SCHEMA`, carried on `Err`.
    pub fn get_namespace_oid(nspname: &str, missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// `RangeVarGetRelid(relation, lockmode, missing_ok)` (namespace.h macro
    /// over `RangeVarGetRelidExtended` with no callback and `RVR_MISSING_OK`
    /// per `missing_ok`).
    pub fn range_var_get_relid(
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
