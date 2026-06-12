//! Seam declarations for the `backend-catalog-storage` unit
//! (`catalog/storage.c`, pending relation deletes/syncs). The owning unit
//! installs these from its `init_seams()` when it lands; until then a call
//! panics loudly.

use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_storage::RelFileLocator;

seam_core::seam!(
    /// `smgrDoPendingSyncs(isCommit, isParallelWorker)` — fsync files created
    /// and not WAL-logged in this transaction.
    pub fn smgr_do_pending_syncs(is_commit: bool, is_parallel_worker: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `smgrDoPendingDeletes(isCommit)` — drop files scheduled for deletion.
    pub fn smgr_do_pending_deletes(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `smgrGetPendingDeletes(forCommit, &ptr)` — list the non-temp relation
    /// files this transaction will delete; allocated in `mcx` (C: palloc in
    /// the caller's context).
    pub fn smgr_get_pending_deletes<'mcx>(
        mcx: Mcx<'mcx>,
        for_commit: bool,
    ) -> PgResult<PgVec<'mcx, RelFileLocator>>
);

seam_core::seam!(
    /// `AtSubCommit_smgr()` — reparent pending deletes to the parent subxact.
    pub fn at_subcommit_smgr()
);

seam_core::seam!(
    /// `AtSubAbort_smgr()` — delete files created in the aborted subxact.
    pub fn at_subabort_smgr() -> PgResult<()>
);

seam_core::seam!(
    /// `PostPrepare_smgr()` — forget pending deletes (2PC takes over).
    pub fn post_prepare_smgr()
);
