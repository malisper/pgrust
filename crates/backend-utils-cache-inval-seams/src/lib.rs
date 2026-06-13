//! Seam declarations for the `backend-utils-cache-inval` unit
//! (`utils/cache/inval.c`), the cache-invalidation dispatcher.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgVec};
use types_cache::{RelcacheCallbackFunction, SyscacheCallbackFunction};
use types_core::Oid;
use types_datum::Datum;
use types_error::PgResult;
use types_storage::SharedInvalidationMessage;
use types_syscache::SysCacheIdentifier;

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

seam_core::seam!(
    /// `ProcessCommittedInvalidationMessages(msgs, nmsgs,
    /// RelcacheInitFileInval, dbid, tsid)` — apply invalidation messages from
    /// a committed transaction during WAL replay.
    pub fn process_committed_invalidation_messages(
        msgs: &[SharedInvalidationMessage],
        relcache_init_file_inval: bool,
        dbid: Oid,
        tsid: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `CommandEndInvalidationMessages()` — make the just-completed command's
    /// catalog changes visible locally; allocates (OOM).
    pub fn command_end_invalidation_messages() -> PgResult<()>
);

seam_core::seam!(
    /// `AtEOXact_Inval(isCommit)` — process/discard pending invalidations at
    /// top-level transaction end.
    pub fn at_eoxact_inval(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `AtEOSubXact_Inval(isCommit)`.
    pub fn at_eosubxact_inval(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `PostPrepare_Inval()` — discard pending invals after a PREPARE.
    pub fn post_prepare_inval()
);

seam_core::seam!(
    /// `LogLogicalInvalidations()` — WAL-log pending invalidations for logical
    /// decoding of in-progress transactions.
    pub fn log_logical_invalidations() -> PgResult<()>
);

seam_core::seam!(
    /// `xactGetCommittedInvalidationMessages(&msgs, &RelcacheInitFileInval)` —
    /// collect the transaction's invalidation messages for the commit record.
    /// Returns `(messages, RelcacheInitFileInval)`; the array is allocated in
    /// `mcx` (C: CurTransactionContext).
    pub fn xact_get_committed_invalidation_messages<'mcx>(
        mcx: Mcx<'mcx>,
    ) -> PgResult<(PgVec<'mcx, SharedInvalidationMessage>, bool)>
);
