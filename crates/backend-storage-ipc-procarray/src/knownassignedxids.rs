//! F5 — hot-standby `KnownAssignedXids` ring + recovery xid bookkeeping
//! (procarray.c).
//!
//! The compressed ring add/search/remove/get operations, the recovery-info
//! application (`ProcArrayApplyRecoveryInfo`/`ProcArrayInitRecovery`/
//! `ProcArrayApplyXidAssignment`), and the expire/idle-maintenance helpers. The
//! ring buffer lives in the F0-owned shmem region (`KNOWN_ASSIGNED_XIDS` +
//! `KNOWN_ASSIGNED_XIDS_VALID` with the cursor bounds in `ProcArrayStruct`); the
//! `RunningTransactionsData` input comes from standby and pg_subtrans is read
//! via the subtrans seam.

use types_core::TransactionId;
use types_error::PgResult;
use types_storage::RunningTransactionsData;

/// `KnownAssignedXidsCompress(KAXCompressReason reason, bool haveLock)`
/// (procarray.c, static) — slide the valid ring entries down to the front,
/// dropping invalidated slots, when the ring has become sparse.
pub fn KnownAssignedXidsCompress(_reason: i32, _have_lock: bool) {
    panic!("decomp: KnownAssignedXidsCompress not yet filled")
}

/// `KnownAssignedXidsAdd(TransactionId from_xid, TransactionId to_xid,
/// bool exclusive_lock)` (procarray.c) — append the (possibly multi-xid) range
/// to the ring, compressing first if needed.
pub fn KnownAssignedXidsAdd(
    _from_xid: TransactionId,
    _to_xid: TransactionId,
    _exclusive_lock: bool,
) -> PgResult<()> {
    panic!("decomp: KnownAssignedXidsAdd not yet filled")
}

/// `KnownAssignedXidsSearch(TransactionId xid, bool remove)` (procarray.c,
/// static) — binary-search the ring for `xid`, optionally invalidating its slot.
pub fn KnownAssignedXidsSearch(_xid: TransactionId, _remove: bool) -> bool {
    panic!("decomp: KnownAssignedXidsSearch not yet filled")
}

/// `KnownAssignedXidExists(TransactionId xid)` (procarray.c).
pub fn KnownAssignedXidExists(_xid: TransactionId) -> bool {
    panic!("decomp: KnownAssignedXidExists not yet filled")
}

/// `KnownAssignedXidsRemove(TransactionId xid)` (procarray.c).
pub fn KnownAssignedXidsRemove(_xid: TransactionId) {
    panic!("decomp: KnownAssignedXidsRemove not yet filled")
}

/// `KnownAssignedXidsRemoveTree(TransactionId xid, int nsubxids,
/// TransactionId *subxids)` (procarray.c) — remove a top xid and its subxids.
pub fn KnownAssignedXidsRemoveTree(_xid: TransactionId, _subxids: &[TransactionId]) {
    panic!("decomp: KnownAssignedXidsRemoveTree not yet filled")
}

/// `KnownAssignedXidsRemovePreceding(TransactionId removeXid)` (procarray.c) —
/// drop every entry `<= removeXid`.
pub fn KnownAssignedXidsRemovePreceding(_remove_xid: TransactionId) {
    panic!("decomp: KnownAssignedXidsRemovePreceding not yet filled")
}

/// `KnownAssignedXidsGet(TransactionId *xarray, TransactionId xmax)`
/// (procarray.c) — copy the valid entries `<= xmax` into a caller array;
/// returns the count.
pub fn KnownAssignedXidsGet(_xarray: &mut [TransactionId], _xmax: TransactionId) -> i32 {
    panic!("decomp: KnownAssignedXidsGet not yet filled")
}

/// `KnownAssignedXidsGetAndSetXmin(TransactionId *xarray, TransactionId *xmin,
/// TransactionId xmax)` (procarray.c) — like `Get` but also lowers `*xmin` to
/// the oldest entry seen.
pub fn KnownAssignedXidsGetAndSetXmin(
    _xarray: &mut [TransactionId],
    _xmin: &mut TransactionId,
    _xmax: TransactionId,
) -> i32 {
    panic!("decomp: KnownAssignedXidsGetAndSetXmin not yet filled")
}

/// `KnownAssignedXidsGetOldestXmin(void)` (procarray.c) — the oldest still-valid
/// entry, or `InvalidTransactionId`.
pub fn KnownAssignedXidsGetOldestXmin() -> TransactionId {
    panic!("decomp: KnownAssignedXidsGetOldestXmin not yet filled")
}

/// `KnownAssignedXidsDisplay(int trace_level)` (procarray.c) — debug dump of the
/// ring.
pub fn KnownAssignedXidsDisplay(_trace_level: i32) {
    panic!("decomp: KnownAssignedXidsDisplay not yet filled")
}

/// `KnownAssignedXidsReset(void)` (procarray.c) — drop the entire ring (standby
/// promotion / shutdown).
pub fn KnownAssignedXidsReset() {
    panic!("decomp: KnownAssignedXidsReset not yet filled")
}

/// `KnownAssignedXidsIdleMaintenance(void)` (procarray.c) — opportunistic
/// compression while the startup process is idle.
pub fn KnownAssignedXidsIdleMaintenance() {
    panic!("decomp: KnownAssignedXidsIdleMaintenance not yet filled")
}

/// `KnownAssignedTransactionIdsIdleMaintenance(void)` (procarray.c) — the public
/// idle-maintenance entry the startup process calls.
pub fn KnownAssignedTransactionIdsIdleMaintenance() {
    panic!("decomp: KnownAssignedTransactionIdsIdleMaintenance not yet filled")
}

/// `RecordKnownAssignedTransactionIds(TransactionId xid)` (procarray.c) — extend
/// the ring (and `latestObservedXid`) to cover a newly-seen xid during recovery.
pub fn RecordKnownAssignedTransactionIds(_xid: TransactionId) -> PgResult<()> {
    panic!("decomp: RecordKnownAssignedTransactionIds not yet filled")
}

/// `ExpireTreeKnownAssignedTransactionIds(TransactionId xid, int nsubxids,
/// TransactionId *subxids, TransactionId max_xid)` (procarray.c) — remove a
/// committed/aborted xid tree from the ring and advance latest-completed.
pub fn ExpireTreeKnownAssignedTransactionIds(
    _xid: TransactionId,
    _subxids: &[TransactionId],
    _max_xid: TransactionId,
) -> PgResult<()> {
    panic!("decomp: ExpireTreeKnownAssignedTransactionIds not yet filled")
}

/// `ExpireAllKnownAssignedTransactionIds(void)` (procarray.c) — drop the whole
/// ring (e.g. on `XLOG_RUNNING_XACTS` with an empty snapshot).
pub fn ExpireAllKnownAssignedTransactionIds() -> PgResult<()> {
    panic!("decomp: ExpireAllKnownAssignedTransactionIds not yet filled")
}

/// `ExpireOldKnownAssignedTransactionIds(TransactionId xid)` (procarray.c) —
/// drop entries older than `xid`.
pub fn ExpireOldKnownAssignedTransactionIds(_xid: TransactionId) {
    panic!("decomp: ExpireOldKnownAssignedTransactionIds not yet filled")
}

/// `ProcArrayApplyRecoveryInfo(RunningTransactions running)` (procarray.c) —
/// rebuild the KnownAssignedXids ring from a `XLOG_RUNNING_XACTS` record.
pub fn ProcArrayApplyRecoveryInfo(_running: &RunningTransactionsData<'_>) -> PgResult<()> {
    panic!("decomp: ProcArrayApplyRecoveryInfo not yet filled")
}

/// `ProcArrayInitRecovery(TransactionId initializedUptoXID)` (procarray.c) —
/// seed `latestObservedXid` at the start of recovery.
pub fn ProcArrayInitRecovery(_initialized_upto_xid: TransactionId) {
    panic!("decomp: ProcArrayInitRecovery not yet filled")
}

/// `ProcArrayApplyXidAssignment(TransactionId topxid, int nsubxids,
/// TransactionId *subxids)` (procarray.c) — redo-side subxid bookkeeping.
pub fn ProcArrayApplyXidAssignment(
    _xtop: TransactionId,
    _subxids: &[TransactionId],
) -> PgResult<()> {
    panic!("decomp: ProcArrayApplyXidAssignment not yet filled")
}

/// Install the F5-owned inward seams: recovery-info / expire-all /
/// xid-assignment, consumed by standby + xact redo.
pub fn init_seams() {
    use backend_storage_ipc_procarray_seams as seams;

    seams::proc_array_apply_recovery_info::set(ProcArrayApplyRecoveryInfo);
    seams::expire_all_known_assigned_transaction_ids::set(ExpireAllKnownAssignedTransactionIds);
    seams::proc_array_apply_xid_assignment::set(ProcArrayApplyXidAssignment);
    seams::record_known_assigned_transaction_ids::set(RecordKnownAssignedTransactionIds);
    seams::expire_tree_known_assigned_transaction_ids::set(ExpireTreeKnownAssignedTransactionIds);
}
