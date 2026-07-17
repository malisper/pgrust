use types_core::{FullTransactionId, Oid, TransactionId};
use types_error::PgResult;
use types_storage::lock::LOCKTAG;
use types_storage::{RelFileLocator, SharedInvalidationMessage};

seam_core::seam!(
    // LogStandbyInvalidations(nmsgs, msgs, relcacheInitFileInval) (standby.c).
    pub fn log_standby_invalidations<'a>(
        msgs: &'a [SharedInvalidationMessage],
        relcache_init_file_inval: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn standby_release_lock_tree<'a>(
        xid: TransactionId,
        subxids: &'a [TransactionId],
    ) -> PgResult<()>
);

seam_core::seam!(
    // LogAccessExclusiveLockPrepare() (standby.c).
    pub fn log_access_exclusive_lock_prepare() -> types_error::PgResult<()>
);

seam_core::seam!(
    // LogAccessExclusiveLock(dbOid, relOid) (standby.c).
    pub fn log_access_exclusive_lock(db_oid: types_core::Oid, rel_oid: types_core::Oid) -> types_error::PgResult<()>
);

seam_core::seam!(
    // LogStandbySnapshot() (standby.c); bgwriter's periodic running-xacts record.
    pub fn log_standby_snapshot() -> PgResult<types_core::XLogRecPtr>
);

seam_core::seam!(
    // ShutdownRecoveryTransactionEnvironment() (standby.c).
    pub fn shutdown_recovery_transaction_environment() -> PgResult<()>
);

seam_core::seam!(
    // InitRecoveryTransactionEnvironment() (standby.c).
    pub fn init_recovery_transaction_environment() -> PgResult<()>
);

seam_core::seam!(
    // StandbyAcquireAccessExclusiveLock(xid, dbOid, relOid) (standby.c);
    // lock.c's twophase standby recovery is the cyclic caller.
    pub fn standby_acquire_access_exclusive_lock(
        xid: TransactionId,
        db_oid: Oid,
        rel_oid: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    // ResolveRecoveryConflictWithLock(locktag, logging_conflict) (standby.c);
    // proc.c's ProcSleep InHotStandby arm is the cyclic caller.
    pub fn resolve_recovery_conflict_with_lock(
        locktag: LOCKTAG,
        logging_conflict: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    // ResolveRecoveryConflictWithBufferPin() (standby.c); bufmgr's
    // LockBufferForCleanup InHotStandby arm is the cyclic caller.
    pub fn resolve_recovery_conflict_with_buffer_pin() -> PgResult<()>
);

seam_core::seam!(
    // LogRecoveryConflict (standby.c); ProcSleep's InHotStandby
    // log_recovery_conflict_waits reporting is the cyclic caller.
    pub fn log_recovery_conflict<'a>(
        reason: types_storage::storage::ProcSignalReason,
        wait_start: types_core::TimestampTz,
        now: types_core::TimestampTz,
        wait_list: Option<&'a [types_storage::storage::VirtualTransactionId]>,
        still_waiting: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    // CheckRecoveryConflictDeadlock() (standby.c); proc.c ProcSleep caller.
    pub fn check_recovery_conflict_deadlock() -> PgResult<()>
);

seam_core::seam!(
    // ResolveRecoveryConflictWithSnapshot (standby.c).
    pub fn resolve_recovery_conflict_with_snapshot(
        snapshot_conflict_horizon: TransactionId,
        is_catalog_rel: bool,
        locator: RelFileLocator,
    ) -> PgResult<()>
);

seam_core::seam!(
    // ResolveRecoveryConflictWithSnapshotFullXid (standby.c).
    pub fn resolve_recovery_conflict_with_snapshot_full_xid(
        snapshot_conflict_horizon: FullTransactionId,
        is_catalog_rel: bool,
        locator: RelFileLocator,
    ) -> PgResult<()>
);
