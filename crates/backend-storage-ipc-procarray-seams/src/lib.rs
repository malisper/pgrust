//! Seam declarations for the `backend-storage-ipc-procarray` unit
//! (`storage/ipc/procarray.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use mcx::{Mcx, PgVec};
use types_core::{Oid, ProcNumber, TransactionId, XLogRecPtr};
use types_error::PgResult;
use types_storage::{
    ProcSignalReason, RunningTransactionLocksHeld, RunningTransactionsData, VirtualTransactionId,
};

seam_core::seam!(
    /// `GetConflictingVirtualXIDs(limitXmin, dbOid)` — VXIDs of backends whose
    /// snapshots could still see `limitXmin`. The C
    /// `InvalidVirtualTransactionId` terminator is dropped; the result array
    /// is allocated in `mcx` (C reuses a TopMemoryContext-static array; the
    /// owner copies into the caller's context instead).
    pub fn get_conflicting_virtual_xids<'mcx>(
        mcx: Mcx<'mcx>,
        limit_xmin: TransactionId,
        db_oid: Oid,
    ) -> PgResult<PgVec<'mcx, VirtualTransactionId>>
);

seam_core::seam!(
    /// `ProcArrayApplyRecoveryInfo(running)`.
    pub fn proc_array_apply_recovery_info(running: &RunningTransactionsData<'_>) -> PgResult<()>
);

seam_core::seam!(
    /// `ExpireAllKnownAssignedTransactionIds()`.
    pub fn expire_all_known_assigned_transaction_ids() -> PgResult<()>
);

seam_core::seam!(
    /// `GetRunningTransactionData()` — C returns with `ProcArrayLock` and
    /// `XidGenLock` held and the caller releases them by hand. Here the
    /// owner acquires the locks, builds the snapshot, and runs `f` with both
    /// held; `locks.release_proc_array_lock()` lets the callback release
    /// `ProcArrayLock` early (the `wal_level < logical` path). Every lock
    /// still held when `f` returns — on success or error — is released by
    /// the owner, so no out-of-band release contract survives the seam. The
    /// `XLogRecPtr` is the callback's result, passed through.
    pub fn get_running_transaction_data(
        f: &mut dyn FnMut(
            &RunningTransactionsData<'_>,
            &mut dyn RunningTransactionLocksHeld,
        ) -> PgResult<XLogRecPtr>,
    ) -> PgResult<XLogRecPtr>
);

seam_core::seam!(
    /// `CountDBBackends(databaseid)`.
    pub fn count_db_backends(databaseid: Oid) -> PgResult<i32>
);

seam_core::seam!(
    /// `CancelDBBackends(databaseid, sigmode, conflictPending)`.
    pub fn cancel_db_backends(
        databaseid: Oid,
        sigmode: ProcSignalReason,
        conflict_pending: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `CancelVirtualTransaction(vxid, sigmode)` — returns the pid of the
    /// signalled process, or 0 if not found.
    pub fn cancel_virtual_transaction(
        vxid: VirtualTransactionId,
        sigmode: ProcSignalReason,
    ) -> PgResult<i32>
);

seam_core::seam!(
    /// `SignalVirtualTransaction(vxid, sigmode, conflictPending)` — returns
    /// the pid of the signalled process, or 0 if not found.
    pub fn signal_virtual_transaction(
        vxid: VirtualTransactionId,
        sigmode: ProcSignalReason,
        conflict_pending: bool,
    ) -> PgResult<i32>
);

seam_core::seam!(
    /// `ProcNumberGetProc(procNumber)->pid` — the pid of the PGPROC in that
    /// slot, or 0 when the slot is not active (C NULL result).
    pub fn proc_number_get_proc_pid(proc_number: ProcNumber) -> i32
);
