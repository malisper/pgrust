//! plancache's slice of the syscache (`utils/cache/syscache.c`): resolving the
//! integer `SysCacheIdentifier` for the caches `InitPlanCache` hooks. The
//! owning unit installs this; until then a call panics loudly.

use ::types_error::PgResult;
use ::types_plancache::SysCacheId;

seam_core::seam!(
    /// The `SysCacheIdentifier` integer for the named syscache (`utils/syscache.h`).
    pub fn syscache_id(which: SysCacheId) -> PgResult<i32>
);
