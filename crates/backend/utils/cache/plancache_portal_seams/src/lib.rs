seam_core::seam!(
    // ReleaseCachedPlan(plan, NULL) (plancache.c): drops the refcount that keeps
    // the portal->stmts share of cplan->stmt_list alive (fabled #359).
    pub fn release_cached_plan(cplan: types_portal::CachedPlanHandle)
);

seam_core::seam!(
    // InitPlanCache (plancache.c): registers plancache inval callbacks.
    pub fn init_plan_cache() -> types_error::PgResult<()>
);

seam_core::seam!(
    // Extra refcount on a live plan (no C counterpart; C pins via portals
    // only): the executor-skeleton cache pins its parked plan with this.
    pub fn incr_cached_plan(cplan: types_portal::CachedPlanHandle)
);
