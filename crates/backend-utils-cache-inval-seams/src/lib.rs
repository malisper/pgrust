//! Seam declarations for the `backend-utils-cache-inval` unit
//! (`utils/cache/inval.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.
//!
//! Invalidation messages cross these seams as the raw
//! `SharedInvalidationMessage` array bytes (16 bytes per message), opaque to
//! the transaction engine, exactly as xact.c treats them.

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `AcceptInvalidationMessages()` — process queued-up sinval messages
    /// (catchup work can `ereport(ERROR)`).
    pub fn accept_invalidation_messages() -> PgResult<()>
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
    /// Returns `(raw message bytes, nmsgs, RelcacheInitFileInval)`; the array
    /// is allocated in `mcx` (C: CurTransactionContext).
    pub fn xact_get_committed_invalidation_messages<'mcx>(
        mcx: Mcx<'mcx>,
    ) -> PgResult<(PgVec<'mcx, u8>, i32, bool)>
);

seam_core::seam!(
    /// `ProcessCommittedInvalidationMessages(msgs, nmsgs,
    /// RelcacheInitFileInval, dbid, tsid)` — redo-side delivery of the commit
    /// record's invalidations.
    pub fn process_committed_invalidation_messages(
        msgs: &[u8],
        nmsgs: i32,
        relcache_init_file_inval: bool,
        dbid: Oid,
        tsid: Oid,
    ) -> PgResult<()>
);
