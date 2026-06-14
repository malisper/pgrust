//! F2 — snapshot computation (procarray.c). The hot path `GetSnapshotData` and
//! the running-xacts / replication-slot-xmin / decoding-horizon family.
//!
//! Builds on the F0 shmem model + the F3 horizons. Returns
//! `SnapshotData`/`RunningTransactionsData` (types in `types_snapshot` /
//! `types_storage`).

use mcx::PgVec;
use types_core::{Oid, ProcNumber, TransactionId, XLogRecPtr};
use types_error::PgResult;
use types_snapshot::SnapshotData;
use types_storage::{RunningTransactionLocksHeld, RunningTransactionsData, VirtualTransactionId};

/// `GetSnapshotData(Snapshot snapshot)` (procarray.c) — the hot path: fill an
/// MVCC snapshot's xmin/xmax/xip/subxip from the running-transactions state.
/// The seam returns only the computed snapshot fields; snapmgr replays the
/// `MyProc->xmin`/`TransactionXmin`/`RecentXmin` updates via the proc seam.
pub fn GetSnapshotData() -> PgResult<SnapshotData> {
    panic!("decomp: GetSnapshotData not yet filled")
}

/// `GetSnapshotDataReuse(Snapshot snapshot)` (procarray.c, static) — the
/// fast-path that re-uses the previous snapshot's arrays when nothing relevant
/// has changed since `xactCompletionCount`. Returns whether the reuse was
/// taken.
pub fn GetSnapshotDataReuse(_snapshot: &mut SnapshotData) -> bool {
    panic!("decomp: GetSnapshotDataReuse not yet filled")
}

/// `ProcArrayInstallImportedXmin(TransactionId xmin,
/// VirtualTransactionId *sourcevxid)` (procarray.c) — make our `MyProc->xmin`
/// safe to set from an imported snapshot, verifying the source vxid is still
/// running. `false` when the source vanished.
pub fn ProcArrayInstallImportedXmin(
    _xmin: TransactionId,
    _sourcevxid: VirtualTransactionId,
) -> PgResult<bool> {
    panic!("decomp: ProcArrayInstallImportedXmin not yet filled")
}

/// `ProcArrayInstallRestoredXmin(TransactionId xmin, PGPROC *proc)`
/// (procarray.c) — like the imported variant but the source is a PGPROC
/// (parallel-worker restore).
pub fn ProcArrayInstallRestoredXmin(
    _xmin: TransactionId,
    _source_proc: ProcNumber,
) -> PgResult<bool> {
    panic!("decomp: ProcArrayInstallRestoredXmin not yet filled")
}

/// `GetRunningTransactionData(void)` (procarray.c) — build the running-xacts
/// snapshot for a `XLOG_RUNNING_XACTS` record. The C returns with
/// `ProcArrayLock` + `XidGenLock` held; here the owner holds both across the
/// callback `f` and releases everything still held when `f` returns.
pub fn GetRunningTransactionData(
    _f: &mut dyn FnMut(
        &RunningTransactionsData<'_>,
        &mut dyn RunningTransactionLocksHeld,
    ) -> PgResult<XLogRecPtr>,
) -> PgResult<XLogRecPtr> {
    panic!("decomp: GetRunningTransactionData not yet filled")
}

/// `GetOldestActiveTransactionId(void)` (procarray.c) — oldest XID still
/// running of any backend (no replication-slot influence).
pub fn GetOldestActiveTransactionId() -> PgResult<TransactionId> {
    panic!("decomp: GetOldestActiveTransactionId not yet filled")
}

/// `GetOldestSafeDecodingTransactionId(bool catalogOnly)` (procarray.c) — the
/// oldest xid it is safe to start logical decoding from. Called with
/// `ProcArrayLock` held.
pub fn GetOldestSafeDecodingTransactionId(_catalog_only: bool) -> TransactionId {
    panic!("decomp: GetOldestSafeDecodingTransactionId not yet filled")
}

/// `ProcArraySetReplicationSlotXmin(TransactionId xmin,
/// TransactionId catalog_xmin, bool already_locked)` (procarray.c) — publish
/// the aggregate slot xmin horizons into the ProcArray.
pub fn ProcArraySetReplicationSlotXmin(
    _xmin: TransactionId,
    _catalog_xmin: TransactionId,
    _already_locked: bool,
) {
    panic!("decomp: ProcArraySetReplicationSlotXmin not yet filled")
}

/// `ProcArrayGetReplicationSlotXmin(TransactionId *xmin,
/// TransactionId *catalog_xmin)` (procarray.c) — read back the published slot
/// xmin horizons.
pub fn ProcArrayGetReplicationSlotXmin() -> (TransactionId, TransactionId) {
    panic!("decomp: ProcArrayGetReplicationSlotXmin not yet filled")
}

/// `GetReplicationHorizons(TransactionId *xmin, TransactionId *catalog_xmin)`
/// (procarray.c) — oldest xmins to advertise via hot-standby feedback.
pub fn GetReplicationHorizons() -> (TransactionId, TransactionId) {
    panic!("decomp: GetReplicationHorizons not yet filled")
}

// --- Logical-decoding flag bookkeeping under ProcArrayLock (procarray.c) ---

/// `LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE)` exposed for logical decoding's
/// out-of-band acquire/release bracket around the decoding-flag set.
pub fn ProcArrayLockAcquireExclusive() {
    panic!("decomp: ProcArrayLockAcquireExclusive not yet filled")
}

/// `LWLockRelease(ProcArrayLock)` — matching release for the above.
pub fn ProcArrayLockRelease() {
    panic!("decomp: ProcArrayLockRelease not yet filled")
}

/// `MyProc->statusFlags |= PROC_IN_LOGICAL_DECODING` (mirrored into
/// `ProcGlobal->statusFlags[MyProc->pgxactoff]`) while holding `ProcArrayLock`.
pub fn MarkProcInLogicalDecoding() {
    panic!("decomp: MarkProcInLogicalDecoding not yet filled")
}

/// Clear `PROC_IN_LOGICAL_DECODING` on `MyProc` (and its dense mirror) under
/// `ProcArrayLock` exclusive — slot.c `ReplicationSlotRelease`.
pub fn ProcArrayClearLogicalDecodingFlag() {
    panic!("decomp: ProcArrayClearLogicalDecodingFlag not yet filled")
}

/// `GetConflictingVirtualXIDs(TransactionId limitXmin, Oid dbOid)`
/// (procarray.c) — VXIDs of backends whose snapshots could still see
/// `limitXmin`. Returns an `mcx`-allocated array (the C `InvalidVirtualTransactionId`
/// terminator is dropped).
pub fn GetConflictingVirtualXIDs<'mcx>(
    _mcx: mcx::Mcx<'mcx>,
    _limit_xmin: TransactionId,
    _db_oid: Oid,
) -> PgResult<PgVec<'mcx, VirtualTransactionId>> {
    panic!("decomp: GetConflictingVirtualXIDs not yet filled")
}

/// Install the F2-owned inward seams (snapshot + running-xacts + slot-xmin +
/// decoding-horizon), heavily consumed by snapmgr, slot, and logical decoding.
pub fn init_seams() {
    use backend_storage_ipc_procarray_seams as seams;

    seams::get_snapshot_data::set(GetSnapshotData);
    seams::proc_array_install_imported_xmin::set(ProcArrayInstallImportedXmin);
    seams::proc_array_install_restored_xmin::set(ProcArrayInstallRestoredXmin);
    seams::get_running_transaction_data::set(GetRunningTransactionData);
    seams::get_oldest_safe_decoding_transaction_id::set(GetOldestSafeDecodingTransactionId);
    seams::GetOldestSafeDecodingTransactionId::set(GetOldestSafeDecodingTransactionId);
    seams::proc_array_set_replication_slot_xmin::set(ProcArraySetReplicationSlotXmin);
    seams::get_replication_horizons::set(GetReplicationHorizons);
    seams::get_conflicting_virtual_xids::set(GetConflictingVirtualXIDs);

    // Logical-decoding flag bookkeeping + ProcArrayLock bracket.
    seams::ProcArrayLock_acquire_exclusive::set(ProcArrayLockAcquireExclusive);
    seams::ProcArrayLock_release::set(ProcArrayLockRelease);
    seams::mark_proc_in_logical_decoding::set(MarkProcInLogicalDecoding);
    seams::proc_array_clear_logical_decoding_flag::set(ProcArrayClearLogicalDecodingFlag);
}
