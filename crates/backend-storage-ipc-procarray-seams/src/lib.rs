//! Seam declarations for the `backend-storage-ipc-procarray` unit
//! (`storage/ipc/procarray.c`), incl. the subset consumed by logical decoding.
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use types_core::{Oid, ProcNumber, TransactionId, XLogRecPtr};
use types_error::PgResult;
use types_snapshot::SnapshotData;
use types_storage::{

    ProcSignalReason, RunningTransactionLocksHeld, RunningTransactionsData, VirtualTransactionId,

};

seam_core::seam!(
    /// `GetSnapshotData(snapshot)` (procarray.c) — fill an MVCC snapshot's
    /// xmin/xmax/xip/subxip from the running-transactions state. C writes into
    /// a caller-provided static struct and also advances the per-backend
    /// `MyProc->xmin`/`TransactionXmin`/`RecentXmin`; this seam returns only
    /// the computed snapshot fields, and snapmgr replays the xmin updates via
    /// the proc seam. Allocates the XID arrays and can `ereport(ERROR)`.
    pub fn get_snapshot_data() -> PgResult<SnapshotData>
);

seam_core::seam!(
    /// `ProcArrayInstallImportedXmin(xmin, sourcevxid)` (procarray.c) — make
    /// our `MyProc->xmin` safe to set from an imported snapshot, verifying the
    /// source vxid is still running. Returns false when the source vanished.
    pub fn proc_array_install_imported_xmin(
        xmin: TransactionId,
        sourcevxid: VirtualTransactionId,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `ProcArrayInstallRestoredXmin(xmin, proc)` (procarray.c) — like above
    /// but the source is identified by a PGPROC (parallel-worker restore).
    pub fn proc_array_install_restored_xmin(
        xmin: TransactionId,
        source_proc: ProcNumber,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `GetMaxSnapshotXidCount()` (procarray.c) — the largest possible xip[]
    /// length. Pure shared-config read; cannot `ereport`.
    pub fn get_max_snapshot_xid_count() -> i32
);

seam_core::seam!(
    /// `GetMaxSnapshotSubxidCount()` (procarray.c) — the largest possible
    /// subxip[] length. Pure shared-config read; cannot `ereport`.
    pub fn get_max_snapshot_subxid_count() -> i32
);

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

seam_core::seam!(
    /// `ProcNumberGetProc(procNumber)` projected to the two PGPROC fields
    /// `checkTempNamespaceStatus` reads: `Some((proc->databaseId,
    /// proc->tempNamespaceId))`, or `None` when the slot is empty (backend
    /// not alive). Shared-memory read; cannot `ereport`.
    pub fn proc_status(proc_number: ProcNumber) -> Option<(Oid, Oid)>
);

seam_core::seam!(
    /// `TransactionIdIsInProgress(xid)` (procarray.c) — is the given XID still
    /// shown running in the ProcArray (or a still-running subxact)? Allocates a
    /// scratch xids array via palloc on first use, so its OOM `ereport` surface
    /// is carried on `Err`.
    pub fn transaction_id_is_in_progress(xid: TransactionId) -> PgResult<bool>
);

seam_core::seam!(
    /// `ProcArrayEndTransaction(MyProc, latestXid)` — advertise no transaction
    /// in progress (the proc argument is always `MyProc` from xact.c).
    pub fn proc_array_end_transaction(latest_xid: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    /// `ProcArrayClearTransaction(MyProc)` — PREPARE's variant.
    pub fn proc_array_clear_transaction() -> PgResult<()>
);

seam_core::seam!(
    /// `XidCacheRemoveRunningXids(xid, nxids, xids, latestXid)` — drop aborted
    /// subxids from PGPROC's subxid cache.
    pub fn xid_cache_remove_running_xids(
        xid: TransactionId,
        children: &[TransactionId],
        latest_xid: TransactionId,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ProcArrayApplyXidAssignment(topxid, nsubxids, subxids)` — redo-side
    /// subxid bookkeeping for hot standby.
    pub fn proc_array_apply_xid_assignment(
        xtop: TransactionId,
        subxids: &[TransactionId],
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `ProcArrayAdd(GetPGProcByNumber(pgprocno))` (procarray.c) — enter the
    /// dummy prepared-xact proc into the global ProcArray so
    /// `TransactionIdIsInProgress` sees its XID running. Takes
    /// `ProcArrayLock`; the `ereport(FATAL)` past `maxProcs` is carried on
    /// `Err`.
    pub fn proc_array_add(pgprocno: ProcNumber) -> PgResult<()>
);

seam_core::seam!(
    /// `ProcArrayRemove(GetPGProcByNumber(pgprocno), latestXid)` (procarray.c)
    /// — remove the dummy proc from the global ProcArray on COMMIT/ABORT
    /// PREPARED, advancing the latest-completed xid to `latest_xid`. Takes
    /// `ProcArrayLock`; cannot `ereport` at ERROR but carries the surface.
    pub fn proc_array_remove(pgprocno: ProcNumber, latest_xid: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    /// `CountUserBackends(roleid)` (`storage/ipc/procarray.c`) — number of
    /// regular client backends running as `roleid` (used by
    /// `InitializeSessionUserId` for the per-role connection limit). Scans
    /// `ProcGlobal`; cannot `ereport`, but the scan crosses shmem so the seam
    /// returns `PgResult` for the wider procarray failure surface consistency.
    pub fn count_user_backends(roleid: Oid) -> PgResult<i32>
);

// --- Subset consumed by logical decoding ---

seam_core::seam!(
    /// `GetOldestSafeDecodingTransactionId(catalogOnly)`.
    pub fn GetOldestSafeDecodingTransactionId(catalog_only: bool) -> TransactionId
);

seam_core::seam!(
    /// `LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE)`.
    pub fn ProcArrayLock_acquire_exclusive()
);

seam_core::seam!(
    /// `LWLockRelease(ProcArrayLock)`.
    pub fn ProcArrayLock_release()
);

seam_core::seam!(
    /// `MyProc->statusFlags |= PROC_IN_LOGICAL_DECODING;
    /// ProcGlobal->statusFlags[MyProc->pgxactoff] = MyProc->statusFlags;`
    /// performed while holding `ProcArrayLock`.
    pub fn mark_proc_in_logical_decoding()
);

// --- Subset consumed by slot.c ---

seam_core::seam!(
    /// `void ProcArraySetReplicationSlotXmin(TransactionId xmin,
    /// TransactionId catalog_xmin, bool already_locked)` (procarray.c) —
    /// publish the aggregate slot xmin horizons into the ProcArray.
    pub fn proc_array_set_replication_slot_xmin(
        xmin: TransactionId,
        catalog_xmin: TransactionId,
        already_locked: bool,
    )
);

seam_core::seam!(
    /// Clear `PROC_IN_LOGICAL_DECODING` on `MyProc` and mirror it into
    /// `ProcGlobal->statusFlags[MyProc->pgxactoff]`, under `ProcArrayLock`
    /// exclusive (slot.c `ReplicationSlotRelease`). The acquire/release of
    /// `ProcArrayLock` is part of this operation in the owner.
    pub fn proc_array_clear_logical_decoding_flag()
);

seam_core::seam!(
    /// `GetReplicationHorizons(&xmin, &catalog_xmin)` (procarray.c) — the
    /// oldest xmins to advertise via hot-standby feedback.
    pub fn get_replication_horizons() -> (TransactionId, TransactionId)
);

seam_core::seam!(
    /// `IsBackendPid(pid)` (procarray.c) — is `pid` the PID of a live backend
    /// (`BackendPidGetProc(pid) != NULL`)? Shared-memory scan; cannot
    /// `ereport`.
    pub fn is_backend_pid(pid: i32) -> bool
);

// --- backend-utils-init-postinit consumer (procarray.c) ---

seam_core::seam!(
    /// `CountDBConnections(databaseid)` (procarray.c): the number of backends
    /// currently connected to `databaseid`. `Err` carries its `ereport`
    /// surface.
    pub fn count_db_connections(databaseid: types_core::Oid) -> types_error::PgResult<i32>
);

seam_core::seam!(
    /// `GetOldestSafeDecodingTransactionId(catalogOnly)` (procarray.c): the
    /// oldest xid it is safe to start decoding from. `catalogOnly` restricts
    /// the horizon to catalog tables. Called with `ProcArrayLock` held.
    pub fn get_oldest_safe_decoding_transaction_id(catalog_only: bool) -> TransactionId
);

seam_core::seam!(
    /// `ProcArrayShmemSize()` (ipci.c `CalculateShmemSize` accumulator) — shared-memory
    /// bytes this subsystem needs. `Err` carries the `add_size`/`mul_size`
    /// overflow `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn proc_array_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `ProcArrayShmemInit()` (ipci.c `CreateOrAttachShmemStructs`) — allocate-or-attach
    /// this subsystem's shared-memory structures. `Err` carries the C
    /// out-of-shared-memory `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn proc_array_shmem_init() -> types_error::PgResult<()>
);
