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

seam_core::seam!(
    pub fn at_eoxact_namespace(is_commit: bool, is_parallel_worker: bool)
);

seam_core::seam!(
    // isTempToastNamespace(namespaceId) (namespace.c): infallible, no catalog
    // access — reads only myTempToastNamespace.
    pub fn is_temp_toast_namespace(namespace_id: Oid) -> bool
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
    // GetTempNamespaceProcNumber(namespaceId) (namespace.c): infallible.
    pub fn get_temp_namespace_proc_number(namespace_id: Oid) -> types_core::ProcNumber
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
