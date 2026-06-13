//! Seam declarations for the `backend-storage-lmgr-predicate` unit
//! (`storage/lmgr/predicate.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `PredicateLockPage(relation, blkno, snapshot)` (predicate.c): acquire a
    /// page-level predicate (SIREAD) lock, as an index-only scan must when it
    /// returns a tuple without visiting the heap. The snapshot is the
    /// active-snapshot token owned by snapmgr. Can `ereport(ERROR)`.
    pub fn predicate_lock_page<'mcx>(
        relation: types_rel::Relation<'mcx>,
        blkno: types_core::primitive::BlockNumber,
        snapshot: Option<types_scan::snapshot::SnapshotHandle>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `predicatelock_twophase_recover(xid, info, recdata, len)` — restore a
    /// prepared transaction's SIREAD predicate locks at recovery (slot
    /// `TWOPHASE_RM_PREDICATELOCK_ID` of `twophase_recover_callbacks`).
    pub fn predicatelock_twophase_recover(
        xid: types_core::primitive::TransactionId,
        info: u16,
        recdata: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `RegisterPredicateLockingXid(xid)` — tell the predicate locking system
    /// the top-level transaction's XID.
    pub fn register_predicate_locking_xid(
        xid: types_core::primitive::TransactionId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `PreCommit_CheckForSerializationFailure()` — raise a serialization
    /// failure detected at commit time.
    pub fn pre_commit_check_for_serialization_failure() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `AtPrepare_PredicateLocks()`.
    pub fn at_prepare_predicate_locks() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `PostPrepare_PredicateLocks(xid)`.
    pub fn post_prepare_predicate_locks(
        xid: types_core::primitive::TransactionId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `PredicateLockTwoPhaseFinish(xid, isCommit)` (predicate.c) — release the
    /// SIREAD predicate locks held by a finishing prepared transaction. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn predicate_lock_twophase_finish(
        xid: types_core::primitive::TransactionId,
        is_commit: bool,
    ) -> types_error::PgResult<()>
);
