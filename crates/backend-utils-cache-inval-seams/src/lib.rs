//! Seam declarations for the `backend-utils-cache-inval` unit
//! (`utils/cache/inval.c`), the cache-invalidation dispatcher.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_cache::{RelcacheCallbackFunction, SyscacheCallbackFunction};
use types_datum::Datum;
use types_error::PgResult;

seam_core::seam!(
    /// `CacheRegisterSyscacheCallback(cacheid, func, arg)` (inval.c):
    /// register `func` to be called whenever the given syscache is
    /// invalidated. `Err` carries the C `elog(FATAL, "out of syscache_callback_list slots")`.
    pub fn cache_register_syscache_callback(
        cacheid: i32,
        func: SyscacheCallbackFunction,
        arg: Datum,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `CacheRegisterRelcacheCallback(func, arg)` (inval.c): register `func`
    /// to be called on relcache invalidation events. `Err` carries the C
    /// `elog(FATAL, "out of relcache_callback_list slots")`.
    pub fn cache_register_relcache_callback(
        func: RelcacheCallbackFunction,
        arg: Datum,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AcceptInvalidationMessages()` (inval.c): read and process the shared
    /// invalidation queue. `Err` carries any error raised by an invalidation
    /// callback or the catchup machinery.
    pub fn accept_invalidation_messages() -> PgResult<()>
);
