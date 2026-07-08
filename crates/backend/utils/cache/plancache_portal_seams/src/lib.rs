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

seam_core::seam!(
    // True iff cplan is its plansource's current generic plan — the only
    // plan a later GetCachedPlan can hand back unchanged. One-shot custom
    // plans never recur, so skeleton/portal parking on them can never hit.
    pub fn is_source_generic_plan(cplan: types_portal::CachedPlanHandle) -> bool
);

seam_core::seam!(
    // Portal retention: DropCachedPlan discards the parked portal shell for
    // this plansource eagerly (DEALLOCATE / DISCARD ALL), releasing its plan
    // pin. Invalidation-driven discard is lazy: the shell's cplan no longer
    // matches GetCachedPlan's result at the next bind.
    pub fn discard_parked_portal(plansource: types_portal::PlanSourceHandle)
);
