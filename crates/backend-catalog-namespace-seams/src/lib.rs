//! Seam declarations for the `backend-catalog-namespace` unit
//! (`catalog/namespace.c`), search-path-aware object name resolution.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::Mcx;
use types_core::Oid;
use types_core::SubTransactionId;
use types_error::PgResult;
use types_storage::lock::LOCKMODE;
use types_tuple::access::RangeVar;

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

seam_core::seam!(
    /// `RangeVarGetRelid(makeRangeVarFromNameList(textToQualifiedNameList(name)),
    /// lockmode, missing_ok)` — the SQL-function idiom for resolving a
    /// possibly-qualified relation name text to its OID without holding a lock
    /// (callers that lack privileges to lock it). With `missing_ok = false` a
    /// missing relation raises `ERRCODE_UNDEFINED_TABLE`, carried on `Err`.
    pub fn range_var_get_relid_from_text(
        mcx: Mcx<'_>,
        name: &str,
        lockmode: LOCKMODE,
        missing_ok: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// `AtEOXact_Namespace(isCommit, parallel)` — end-of-xact temp-namespace
    /// and search-path cleanup.
    pub fn at_eoxact_namespace(is_commit: bool, parallel: bool)
);

seam_core::seam!(
    /// `AtEOSubXact_Namespace(isCommit, mySubid, parentSubid)`.
    pub fn at_eosubxact_namespace(
        is_commit: bool,
        my_subid: SubTransactionId,
        parent_subid: SubTransactionId,
    )
);

/* ---- CLUSTER target resolution (used by backend-commands-cluster) -------- */

seam_core::seam!(
    /// `RangeVarGetRelidExtended(relation, AccessExclusiveLock, 0,
    /// RangeVarCallbackMaintainsTable, NULL)` (namespace.c): resolve+lock the
    /// CLUSTER target, running the maintains-table permission callback.
    pub fn range_var_get_relid_maintains_table<'mcx>(
        mcx: Mcx<'mcx>,
        relation: &RangeVar,
        lockmode: LOCKMODE,
    ) -> PgResult<Oid>
);
seam_core::seam!(
    /// `LookupCreationNamespace(nspname)` (namespace.c): OID of the namespace
    /// to create in (`pg_temp` for temp); `Err` on ACL/lookup failure.
    pub fn lookup_creation_namespace(nspname: &str) -> PgResult<Oid>
);
seam_core::seam!(
    /// `RestrictSearchPath()` (namespace.c): set search_path to a safe value
    /// for a security-restricted operation.
    pub fn restrict_search_path() -> PgResult<()>
);
