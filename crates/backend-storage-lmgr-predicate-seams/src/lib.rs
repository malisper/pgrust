//! Seam declarations for the `backend-storage-lmgr-predicate` unit
//! (`storage/lmgr/predicate.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

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
    /// `PredicateLockTwoPhaseFinish(xid, isCommit)` (predicate.c) — release the
    /// SIREAD predicate locks held by a finishing prepared transaction. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn predicate_lock_twophase_finish(
        xid: types_core::primitive::TransactionId,
        is_commit: bool,
    ) -> types_error::PgResult<()>
);
