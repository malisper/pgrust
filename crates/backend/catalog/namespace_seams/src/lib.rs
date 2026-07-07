use mcx::Mcx;
use rel_vocab::RangeVar;
use types_core::Oid;
use types_error::PgResult;
use types_rel::LOCKMODE;

seam_core::seam!(
    pub fn range_var_get_relid(
        mcx: Mcx<'_>,
        relation: &RangeVar,
        lockmode: LOCKMODE,
        missing_ok: bool,
    ) -> PgResult<Oid>
);

// RangeVarGetRelidExtended flags (namespace.h).
pub const RVR_MISSING_OK: u32 = 1 << 0;
pub const RVR_NOWAIT: u32 = 1 << 1;
pub const RVR_SKIP_LOCKED: u32 = 1 << 2;

seam_core::seam!(
    // RangeVarGetRelidExtended (namespace.c), callback-free callers.
    // RVR_SKIP_LOCKED returns InvalidOid when the lock is unavailable.
    pub fn range_var_get_relid_extended(
        mcx: Mcx<'_>,
        relation: &RangeVar,
        lockmode: LOCKMODE,
        flags: u32,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    // get_ts_parser_oid (namespace.c) — wparser_def byname entries resolve
    // parser names (a direct wparser_def -> catalog_namespace dep would cycle
    // through fmgr_core).
    pub fn get_ts_parser_oid(names: &[&str], missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    pub fn at_eoxact_namespace(is_commit: bool, is_parallel_worker: bool)
);

seam_core::seam!(
    // isTempToastNamespace(namespaceId) (namespace.c): infallible, no catalog
    // access — reads only myTempToastNamespace.
    pub fn is_temp_toast_namespace(namespace_id: Oid) -> bool
);

seam_core::seam!(
    // isTempNamespace(namespaceId) (namespace.c): infallible, reads only
    // myTempNamespace.
    pub fn is_temp_namespace(namespace_id: Oid) -> bool
);

seam_core::seam!(
    pub fn at_eosubxact_namespace(
        is_commit: bool,
        my_subid: types_core::SubTransactionId,
        parent_subid: types_core::SubTransactionId,
    )
);

seam_core::seam!(
    // isTempOrTempToastNamespace(namespaceId) (namespace.c): infallible.
    pub fn is_temp_or_temp_toast_namespace(namespace_id: Oid) -> bool
);

seam_core::seam!(
    // TypeIsVisible (namespace.c): syscache reads + path recompute, so it
    // carries the cache-lookup-failed elog(ERROR) surface.
    pub fn type_is_visible(typid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    // GetTempNamespaceProcNumber(namespaceId) (namespace.c): reads the
    // pg_namespace syscache (get_namespace_name), so it carries that lookup's
    // elog(ERROR) surface.
    pub fn get_temp_namespace_proc_number(
        namespace_id: Oid,
    ) -> PgResult<types_core::ProcNumber>
);

seam_core::seam!(
    // InitializeSearchPath (namespace.c).
    pub fn initialize_search_path() -> PgResult<()>
);

seam_core::seam!(
    // fetch_search_path(includeImplicit) (namespace.c); recomputes the path,
    // which can require catalog access.
    pub fn fetch_search_path<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        include_implicit: bool,
    ) -> PgResult<mcx::PgVec<'mcx, Oid>>
);

seam_core::seam!(
    // get_collation_oid(names, missing_ok) (namespace.c): encoding-aware
    // search-path lookup; carries the undefined-collation error surface.
    pub fn get_collation_oid(collname: &[&str], missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    // get_ts_config_oid(names, missing_ok) (namespace.c).
    pub fn get_ts_config_oid(names: &[&str], missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    // get_ts_dict_oid(names, missing_ok) (namespace.c).
    pub fn get_ts_dict_oid(names: &[&str], missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    // OpernameGetOprid(names, oprleft, oprright) (namespace.c).
    pub fn opername_get_oprid(names: &[&str], oprleft: Oid, oprright: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    // OpernameGetCandidates(names, oprkind, missing_schema_ok) (namespace.c)
    // projected to candidate oids, list order.
    pub fn opername_get_candidate_oids<'mcx>(
        mcx: Mcx<'mcx>,
        names: &[&str],
        oprkind: i8,
        missing_schema_ok: bool,
    ) -> PgResult<mcx::PgVec<'mcx, Oid>>
);

seam_core::seam!(
    // FindDefaultConversionProc(for_encoding, to_encoding) (namespace.c):
    // OID of the default conversion proc on the search path, or InvalidOid.
    // No Mcx: the owner runs its catalog lookups in a scratch context.
    pub fn find_default_conversion_proc(for_encoding: i32, to_encoding: i32) -> PgResult<Oid>
);


seam_core::seam!(
    // LookupExplicitNamespace (namespace.c); InvalidOid when missing_ok.
    pub fn lookup_explicit_namespace(nspname: &str, missing_ok: bool) -> PgResult<Oid>
);
