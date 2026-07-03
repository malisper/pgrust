// prepare.c declarations for the extended-query protocol (postgres cannot dep
// prepare directly: prepare deps postgres for pg_analyze_and_rewrite).
use plancache::CachedPlanSourceHandle;
use types_error::PgResult;

seam_core::seam!(
    pub fn store_prepared_statement(
        stmt_name: &str,
        plansource: CachedPlanSourceHandle,
        from_sql: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    // FetchPreparedStatement projected to its plansource (the only field the
    // protocol reads); None only when throw_error is false.
    pub fn fetch_prepared_statement_plansource(
        stmt_name: &str,
        throw_error: bool,
    ) -> PgResult<Option<CachedPlanSourceHandle>>
);

seam_core::seam!(
    pub fn drop_prepared_statement(stmt_name: &str, show_error: bool) -> PgResult<()>
);
