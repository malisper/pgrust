//! Seam declarations for the `backend-utils-cache-inval` unit
//! (`utils/cache/inval.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_datum::Datum;
use types_error::PgResult;
use types_syscache::SysCacheIdentifier;

seam_core::seam!(
    /// `AcceptInvalidationMessages()` (inval.c): process pending shared
    /// invalidation messages. Invalidation callbacks can `ereport(ERROR)`,
    /// carried on `Err`.
    pub fn accept_invalidation_messages() -> PgResult<()>
);

seam_core::seam!(
    /// `CacheRegisterSyscacheCallback(cacheid, func, arg)` (inval.c):
    /// register a syscache-invalidation callback; the callback receives
    /// `(arg, cacheid, hashvalue)` as in C's `SyscacheCallbackFunction`.
    /// C `elog(FATAL)`s when the callback table is full, carried on `Err`.
    pub fn cache_register_syscache_callback(
        cacheid: SysCacheIdentifier,
        callback: fn(arg: Datum, cacheid: i32, hashvalue: u32),
        arg: Datum,
    ) -> PgResult<()>
);
