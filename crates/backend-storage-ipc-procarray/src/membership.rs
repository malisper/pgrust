//! F1 — ProcArray slot add/remove + end-of-xact membership (procarray.c).
//!
//! `ProcArrayAdd`/`ProcArrayRemove` (PREPARE dummy-proc entry/exit),
//! `ProcArrayEndTransaction` + its internal helper, `ProcArrayClearTransaction`
//! (PREPARE variant), the `MaintainLatestCompletedXid*` advance, and the
//! lock-batching group-clear path (`ProcArrayGroupClearXid`) with its
//! `pg_atomic_*` CAS over `ProcGlobal->procArrayGroupFirst`.
//!
//! Builds on the F0 shmem model + `ProcArrayLock` (lwlock) and reaches PGPROC /
//! ProcGlobal slot fields + `ProcSendSignal` via the proc seam crate.

use types_core::{ProcNumber, TransactionId};
use types_error::PgResult;

/// `ProcArrayAdd(PGPROC *proc)` (procarray.c) — enter `proc` (a dummy
/// prepared-xact PGPROC, identified here by its `ProcNumber`) into the global
/// ProcArray under `ProcArrayLock`. `ereport(FATAL)` past `maxProcs` carried on
/// `Err`.
pub fn ProcArrayAdd(_pgprocno: ProcNumber) -> PgResult<()> {
    panic!("decomp: ProcArrayAdd not yet filled")
}

/// `ProcArrayRemove(PGPROC *proc, TransactionId latestXid)` (procarray.c) —
/// remove the proc from the global ProcArray on COMMIT/ABORT PREPARED,
/// advancing latest-completed to `latest_xid`, under `ProcArrayLock`.
pub fn ProcArrayRemove(_pgprocno: ProcNumber, _latest_xid: TransactionId) -> PgResult<()> {
    panic!("decomp: ProcArrayRemove not yet filled")
}

/// `ProcArrayEndTransaction(PGPROC *proc, TransactionId latestXid)`
/// (procarray.c) — advertise no transaction in progress for `MyProc`. Takes the
/// group-clear fast path when `ProcArrayLock` is contended.
pub fn ProcArrayEndTransaction(_latest_xid: TransactionId) -> PgResult<()> {
    panic!("decomp: ProcArrayEndTransaction not yet filled")
}

/// `ProcArrayEndTransactionInternal(PGPROC *proc, TransactionId latestXid)`
/// (procarray.c, static) — the actual per-proc clear of `xid`/`xmin`/subxids
/// run with `ProcArrayLock` held (directly, or batched on behalf of the group
/// leader). Maintains `latestCompletedXid`.
pub fn ProcArrayEndTransactionInternal(_pgprocno: ProcNumber, _latest_xid: TransactionId) {
    panic!("decomp: ProcArrayEndTransactionInternal not yet filled")
}

/// `ProcArrayClearTransaction(PGPROC *proc)` (procarray.c) — PREPARE's variant:
/// clear the xid/xmin bookkeeping without ending the proc's ProcArray presence.
pub fn ProcArrayClearTransaction() -> PgResult<()> {
    panic!("decomp: ProcArrayClearTransaction not yet filled")
}

/// `MaintainLatestCompletedXid(TransactionId latestXid)` (procarray.c, static) —
/// advance `TransamVariables->latestCompletedXid` to include `latestXid`
/// (normal-running path).
pub fn MaintainLatestCompletedXid(_latest_xid: TransactionId) {
    panic!("decomp: MaintainLatestCompletedXid not yet filled")
}

/// `MaintainLatestCompletedXidRecovery(TransactionId latestXid)` (procarray.c,
/// static) — the hot-standby recovery-side variant.
pub fn MaintainLatestCompletedXidRecovery(_latest_xid: TransactionId) {
    panic!("decomp: MaintainLatestCompletedXidRecovery not yet filled")
}

/// `ProcArrayGroupClearXid(PGPROC *proc, TransactionId latestXid)`
/// (procarray.c, static) — the lock-batching group-clear path: CAS this proc
/// onto `ProcGlobal->procArrayGroupFirst`, then either become the leader who
/// clears the whole batch under `ProcArrayLock` or sleep on the latch until the
/// leader clears us.
pub fn ProcArrayGroupClearXid(_pgprocno: ProcNumber, _latest_xid: TransactionId) {
    panic!("decomp: ProcArrayGroupClearXid not yet filled")
}

/// Install the F1-owned inward seams: the membership + end-of-xact seams
/// consumed by twophase / xact.
pub fn init_seams() {
    use backend_storage_ipc_procarray_seams as seams;

    seams::proc_array_add::set(ProcArrayAdd);
    seams::proc_array_remove::set(ProcArrayRemove);
    seams::proc_array_end_transaction::set(ProcArrayEndTransaction);
    seams::proc_array_clear_transaction::set(ProcArrayClearTransaction);
}
