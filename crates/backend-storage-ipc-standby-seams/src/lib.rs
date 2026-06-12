//! Seam declarations for the `backend-storage-ipc-standby` unit
//! (`storage/ipc/standby.c`): Hot Standby recovery-conflict resolution,
//! recovery AccessExclusiveLock tracking, and standby WAL logging. The
//! owning unit installs all of these from its `init_seams()`.

use types_core::{Oid, TimestampTz, TransactionId, XLogRecPtr};
use types_core::xact::FullTransactionId;
use types_error::PgResult;
use types_storage::{
    LOCKTAG, ProcSignalReason, RelFileLocator, SharedInvalidationMessage, VirtualTransactionId,
};

seam_core::seam!(
    /// `InitRecoveryTransactionEnvironment()`.
    pub fn init_recovery_transaction_environment() -> PgResult<()>
);

seam_core::seam!(
    /// `ShutdownRecoveryTransactionEnvironment()`.
    pub fn shutdown_recovery_transaction_environment() -> PgResult<()>
);

seam_core::seam!(
    /// `LogRecoveryConflict(reason, wait_start, now, wait_list, still_waiting)`.
    pub fn log_recovery_conflict(
        reason: ProcSignalReason,
        wait_start: TimestampTz,
        now: TimestampTz,
        wait_list: Option<&[VirtualTransactionId]>,
        still_waiting: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ResolveRecoveryConflictWithSnapshot(snapshotConflictHorizon,
    /// isCatalogRel, locator)`.
    pub fn resolve_recovery_conflict_with_snapshot(
        snapshot_conflict_horizon: TransactionId,
        is_catalog_rel: bool,
        locator: RelFileLocator,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ResolveRecoveryConflictWithSnapshotFullXid(snapshotConflictHorizon,
    /// isCatalogRel, locator)`.
    pub fn resolve_recovery_conflict_with_snapshot_full_xid(
        snapshot_conflict_horizon: FullTransactionId,
        is_catalog_rel: bool,
        locator: RelFileLocator,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ResolveRecoveryConflictWithTablespace(tsid)`.
    pub fn resolve_recovery_conflict_with_tablespace(tsid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `ResolveRecoveryConflictWithDatabase(dbid)`.
    pub fn resolve_recovery_conflict_with_database(dbid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `ResolveRecoveryConflictWithLock(locktag, logging_conflict)`.
    pub fn resolve_recovery_conflict_with_lock(
        locktag: LOCKTAG,
        logging_conflict: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ResolveRecoveryConflictWithBufferPin()`.
    pub fn resolve_recovery_conflict_with_buffer_pin() -> PgResult<()>
);

seam_core::seam!(
    /// `CheckRecoveryConflictDeadlock()` — `ereport(ERROR)` (as `Err`) if this
    /// backend holds the buffer pin the Startup process waits for.
    pub fn check_recovery_conflict_deadlock() -> PgResult<()>
);

seam_core::seam!(
    /// `StandbyDeadLockHandler()` — STANDBY_DEADLOCK_TIMEOUT handler.
    pub fn standby_dead_lock_handler()
);

seam_core::seam!(
    /// `StandbyTimeoutHandler()` — STANDBY_TIMEOUT handler.
    pub fn standby_timeout_handler()
);

seam_core::seam!(
    /// `StandbyLockTimeoutHandler()` — STANDBY_LOCK_TIMEOUT handler.
    pub fn standby_lock_timeout_handler()
);

seam_core::seam!(
    /// `StandbyAcquireAccessExclusiveLock(xid, dbOid, relOid)`.
    pub fn standby_acquire_access_exclusive_lock(
        xid: TransactionId,
        db_oid: Oid,
        rel_oid: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `StandbyReleaseLockTree(xid, nsubxids, subxids)`.
    pub fn standby_release_lock_tree(xid: TransactionId, subxids: &[TransactionId])
);

seam_core::seam!(
    /// `StandbyReleaseAllLocks()`.
    pub fn standby_release_all_locks()
);

seam_core::seam!(
    /// `StandbyReleaseOldLocks(oldxid)`.
    pub fn standby_release_old_locks(oldxid: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    /// `standby_redo(record)` — replay one `RM_STANDBY_ID` record. `info` is
    /// the raw `XLogRecGetInfo` byte (the implementation masks
    /// `XLR_INFO_MASK`), `data` is `XLogRecGetData`, and
    /// `has_any_block_refs` is `XLogRecHasAnyBlockRefs` (asserted false).
    pub fn standby_redo(info: u8, data: &[u8], has_any_block_refs: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `LogStandbySnapshot()` — log AccessExclusiveLocks and running xacts to
    /// WAL; returns the RecPtr of the last inserted record. Takes the
    /// caller's context for the transient locks array
    /// (`GetRunningTransactionLocks` pallocs there).
    pub fn log_standby_snapshot(mcx: mcx::Mcx<'_>) -> PgResult<XLogRecPtr>
);

seam_core::seam!(
    /// `LogAccessExclusiveLock(dbOid, relOid)`.
    pub fn log_access_exclusive_lock(db_oid: Oid, rel_oid: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `LogAccessExclusiveLockPrepare()`.
    pub fn log_access_exclusive_lock_prepare() -> PgResult<()>
);

seam_core::seam!(
    /// `LogStandbyInvalidations(nmsgs, msgs, relcacheInitFileInval)`.
    pub fn log_standby_invalidations(
        msgs: &[SharedInvalidationMessage],
        relcache_init_file_inval: bool,
    ) -> PgResult<()>
);
