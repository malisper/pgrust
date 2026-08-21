seam_core::seam!(
    // ReleaseCachedPlan(plan, NULL) (plancache.c): drops the refcount that keeps
    // the portal->stmts share of cplan->stmt_list alive (an internal issue).
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

seam_core::seam!(
    // The statement-plane program cache's identity supply (the lanev4 A1
    // #512 seam, carried into the sqe statement memo): positional plan
    // identity for a running CachedPlan —
    // (plansource handle id, generation at build, is-current-generic-plan).
    // The executor keys plan-lifetime memoized artifacts on (handle,
    // generation); the generic bit gates caching (one-shot custom plans
    // never recur — GetCachedPlan can only ever hand the generic plan back
    // unchanged, so caching a custom execution's artifact can never hit).
    pub fn plan_identity(cplan: types_portal::CachedPlanHandle) -> (u64, i64, bool)
);

seam_core::seam!(
    // Statement-memo invalidation, the EAGER half (the lanev4 #511 face):
    // DropCachedPlan (DEALLOCATE / DISCARD ALL / prepared-stmt teardown)
    // releases every memoized artifact of the dropped plansource.
    // Invalidation-driven staleness is the LAZY half and needs no callback:
    // every plan rebuild bumps the source generation, so a stale artifact is
    // a structural (handle, generation) miss — the discard_parked_portal
    // two-mode discipline, applied to the statement memo.
    pub fn engine_plan_dropped(plansource: types_portal::PlanSourceHandle)
);

seam_core::seam!(
    // Statement-memo invalidation, the sys-reset face (#511): the
    // ResetPlanCache event class drops the whole memo plane
    // (GC/defense-in-depth — soundness rides the generation key; resources
    // ride this face).
    pub fn engine_plan_cache_reset()
);
