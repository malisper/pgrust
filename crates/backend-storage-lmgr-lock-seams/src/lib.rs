//! Seam declarations for the `backend-storage-lmgr-lock` unit
//! (`storage/lmgr/lock.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `lock_twophase_recover(xid, info, recdata, len)` — re-acquire a prepared
    /// transaction's locks at recovery startup (slot `TWOPHASE_RM_LOCK_ID` of
    /// `twophase_recover_callbacks`).
    pub fn lock_twophase_recover(
        xid: types_core::primitive::TransactionId,
        info: u16,
        recdata: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `lock_twophase_postcommit(xid, info, recdata, len)` — release a prepared
    /// transaction's locks on COMMIT PREPARED (slot `TWOPHASE_RM_LOCK_ID` of
    /// `twophase_postcommit_callbacks`).
    pub fn lock_twophase_postcommit(
        xid: types_core::primitive::TransactionId,
        info: u16,
        recdata: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `lock_twophase_postabort(xid, info, recdata, len)` — release a prepared
    /// transaction's locks on ROLLBACK PREPARED (slot `TWOPHASE_RM_LOCK_ID` of
    /// `twophase_postabort_callbacks`).
    pub fn lock_twophase_postabort(
        xid: types_core::primitive::TransactionId,
        info: u16,
        recdata: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `lock_twophase_standby_recover(xid, info, recdata, len)` — acquire a
    /// prepared transaction's AccessExclusiveLocks at hot-standby startup
    /// (slot `TWOPHASE_RM_LOCK_ID` of `twophase_standby_recover_callbacks`).
    pub fn lock_twophase_standby_recover(
        xid: types_core::primitive::TransactionId,
        info: u16,
        recdata: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `LockAcquire(locktag, lockmode, sessionLock, dontWait)` — can
    /// `ereport(ERROR)` (out of shared memory, deadlock).
    pub fn lock_acquire(
        locktag: &types_storage::LOCKTAG,
        lockmode: types_storage::LOCKMODE,
        session_lock: bool,
        dont_wait: bool,
    ) -> types_error::PgResult<types_storage::LockAcquireResult>
);

seam_core::seam!(
    /// `LockRelease(locktag, lockmode, sessionLock)` — false (with a WARNING)
    /// when the lock was not held.
    pub fn lock_release(
        locktag: &types_storage::LOCKTAG,
        lockmode: types_storage::LOCKMODE,
        session_lock: bool,
    ) -> bool
);

seam_core::seam!(
    /// `GetLockConflicts(locktag, lockmode, countp)` — VXIDs of transactions
    /// holding conflicting locks; the C terminator is dropped and `countp`
    /// folds into the length. The result array is allocated in `mcx` (C
    /// reuses a TopMemoryContext-static array; the owner copies into the
    /// caller's context instead).
    pub fn get_lock_conflicts<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        locktag: &types_storage::LOCKTAG,
        lockmode: types_storage::LOCKMODE,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, types_storage::VirtualTransactionId>>
);

seam_core::seam!(
    /// `GetRunningTransactionLocks(*nlocks)` — every held AccessExclusiveLock
    /// with an assigned xid, for snapshot logging. C pallocs the array in the
    /// caller's context (the caller pfrees it), so the seam takes the target
    /// context.
    pub fn get_running_transaction_locks<'mcx>(
        mcx: mcx::Mcx<'mcx>,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, types_storage::xl_standby_lock>>
);

seam_core::seam!(
    /// `VirtualXactLock(vxid, wait)` — true if the vxid has ended (or its
    /// lock was acquired); false when `wait == false` and it is still around.
    pub fn virtual_xact_lock(
        vxid: types_storage::VirtualTransactionId,
        wait: bool,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `VirtualXactLockTableInsert(vxid)`.
    pub fn virtual_xact_lock_table_insert(
        vxid: types_storage::VirtualTransactionId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `VirtualXactLockTableCleanup()`.
    pub fn virtual_xact_lock_table_cleanup() -> types_error::PgResult<()>
);
