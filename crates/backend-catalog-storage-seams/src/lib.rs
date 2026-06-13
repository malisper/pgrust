//! Seam declarations for the `backend-catalog-storage` unit (`storage.c`): the rmgr-table
//! callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_storage::RelFileLocator;

seam_core::seam!(
    /// `smgr_redo(record)` (storage.c) — WAL redo for this resource manager's
    /// records (`rm_redo` slot). Can `ereport(ERROR)`, carried on `Err`.
    pub fn smgr_redo(record: &mut types_wal::rmgr::XLogReaderState<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `RelFileLocatorSkippingWAL(rlocator)` (storage.c): true if the relation
    /// is in the `pendingDeletes`/`pendingSyncs` set such that WAL is being
    /// skipped for its current relfilenode this transaction
    /// (`wal_skip_threshold`). Pure in-memory hash lookup; cannot `ereport`.
    pub fn rel_file_locator_skipping_wal(rlocator: RelFileLocator) -> bool
);

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

seam_core::seam!(
    /// `DropRelationFiles(delrels, ndelrels, isRedo=false)` (storage.c) — drop
    /// the physical files a finished prepared transaction was supposed to
    /// delete. Can `ereport(ERROR)`, carried on `Err`.
    pub fn drop_relation_files(rels: &[types_wal::RelFileLocator]) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `RelationPreserveStorage(rlocator, atCommit)` (storage.c) — protect the
    /// physical file named by `rlocator` from deletion at transaction
    /// end/abort. relmapper calls this with `atCommit=false` for each mapped
    /// file when committing a relmap update, inside a critical section.
    pub fn relation_preserve_storage(
        rlocator: RelFileLocator,
        at_commit: bool,
    ) -> PgResult<()>
);
