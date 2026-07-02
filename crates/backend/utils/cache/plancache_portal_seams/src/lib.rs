seam_core::seam!(
    // ReleaseCachedPlan(plan, NULL) (plancache.c): drops the refcount that keeps
    // the portal->stmts share of cplan->stmt_list alive (an internal issue).
    pub fn release_cached_plan(cplan: types_portal::CachedPlanHandle)
);
