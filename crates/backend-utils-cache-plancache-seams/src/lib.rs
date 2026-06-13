//! Seam declarations for `utils/cache/plancache.c`.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::PgResult;

seam_core::seam!(
    /// `InitPlanCache()` (plancache.c): set up the plan-cache invalidation
    /// callbacks. `Err` carries its `ereport` surface.
    pub fn init_plan_cache() -> PgResult<()>
);
