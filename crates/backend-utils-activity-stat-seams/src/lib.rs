//! Seam declarations for the `backend-utils-activity-stat` unit (the per-kind
//! stats implementation files `pgstat_io.c`, `pgstat_database.c`,
//! `pgstat_relation.c`, ... bundled by the catalog).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `pgstat_flush_io(bool nowait)` (`utils/activity/pgstat_io.c`) — flush
    /// the backend's pending IO statistics. Returns true if some stats could
    /// not be flushed because of contention (`pgstat_io_flush_cb`'s result).
    /// `Err` carries `LWLockAcquire`'s `elog(ERROR, "too many LWLocks
    /// taken")` on the blocking (`!nowait`) path.
    pub fn pgstat_flush_io(nowait: bool) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `pgstat_twophase_postcommit(xid, info, recdata, len)` — apply the
    /// prepared transaction's per-table stats deltas on COMMIT PREPARED (slot
    /// `TWOPHASE_RM_PGSTAT_ID` of `twophase_postcommit_callbacks`).
    pub fn pgstat_twophase_postcommit(
        xid: types_core::primitive::TransactionId,
        info: u16,
        recdata: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `pgstat_twophase_postabort(xid, info, recdata, len)` — apply the
    /// prepared transaction's per-table stats deltas on ROLLBACK PREPARED
    /// (slot `TWOPHASE_RM_PGSTAT_ID` of `twophase_postabort_callbacks`).
    pub fn pgstat_twophase_postabort(
        xid: types_core::primitive::TransactionId,
        info: u16,
        recdata: &[u8],
    ) -> types_error::PgResult<()>
);
