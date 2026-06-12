//! Seam declarations for the `backend-storage-ipc-procarray` unit
//! (`storage/ipc/procarray.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::{Oid, ProcNumber, TransactionId};
use types_error::PgResult;
use types_storage::{ProcSignalReason, RunningTransactionsData, VirtualTransactionId};

seam_core::seam!(
    /// `GetConflictingVirtualXIDs(limitXmin, dbOid)` — VXIDs of backends whose
    /// snapshots could still see `limitXmin`. The C `InvalidVirtualTransactionId`
    /// terminator is dropped; the returned `Vec` is a snapshot of the
    /// owner-managed (TopMemoryContext-static) result array. Fallible: the
    /// first call allocates that array.
    pub fn get_conflicting_virtual_xids(
        limit_xmin: TransactionId,
        db_oid: Oid,
    ) -> PgResult<std::vec::Vec<VirtualTransactionId>>
);

seam_core::seam!(
    /// `ProcArrayApplyRecoveryInfo(running)`.
    pub fn proc_array_apply_recovery_info(running: &RunningTransactionsData) -> PgResult<()>
);

seam_core::seam!(
    /// `ExpireAllKnownAssignedTransactionIds()`.
    pub fn expire_all_known_assigned_transaction_ids() -> PgResult<()>
);

seam_core::seam!(
    /// `GetRunningTransactionData()`. Returns with `ProcArrayLock` and
    /// `XidGenLock` HELD; the caller releases them (`lwlock_release_builtin`).
    pub fn get_running_transaction_data() -> PgResult<RunningTransactionsData>
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
