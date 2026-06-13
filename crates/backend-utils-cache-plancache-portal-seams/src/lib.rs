//! Seam declaration for the `utils/cache/plancache.c` function portalmem calls
//! when dropping a portal's cached-plan reference. The plan cache owns the
//! `CachedPlan`; portalmem threads its identity as a [`CachedPlanHandle`].
//! (A `-portal-` suffix avoids colliding with the in-band plancache seam
//! vocabulary; the handle/typed reconciliation is DESIGN_DEBT until plancache
//! lands.)

use types_portal::CachedPlanHandle;

seam_core::seam!(
    /// `ReleaseCachedPlan(plan, owner)` — portalmem passes the portal's owner;
    /// since the owner is threaded separately and the C call only needs the
    /// plan to drop a refcount, the seam takes the plan handle.
    pub fn release_cached_plan(plan: CachedPlanHandle)
);
