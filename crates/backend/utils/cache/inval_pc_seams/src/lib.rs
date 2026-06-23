//! plancache's slice of the cache-invalidation registry (`utils/cache/inval.c`).
//! `InitPlanCache` registers its relcache/syscache callbacks here. The `Datum
//! arg` plancache passes is always 0, so the callback shapes drop it. The
//! owning inval unit installs these; until then a call panics loudly.

use types_error::PgResult;
use types_plancache::{RelcacheCallbackFn, SyscacheCallbackFn};

seam_core::seam!(
    /// `CacheRegisterRelcacheCallback(func, 0)`.
    pub fn register_relcache_callback(func: RelcacheCallbackFn) -> PgResult<()>
);

seam_core::seam!(
    /// `CacheRegisterSyscacheCallback(cacheid, func, 0)`.
    pub fn register_syscache_callback(cacheid: i32, func: SyscacheCallbackFn) -> PgResult<()>
);
