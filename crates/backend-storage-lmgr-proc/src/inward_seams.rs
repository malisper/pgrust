//! Implementations of the inward seams this unit owns
//! (`backend-storage-lmgr-proc-seams`): the `PGPROC`-field accessors the
//! LWLock / CV / latch / twophase machinery reads and writes, plus the small
//! proc.c helpers other units call (`ProcWaitForSignal`, `LockErrorCleanup`,
//! the `DeadlockTimeout`/`TransactionTimeout` GUCs, ...).
//!
//! SCAFFOLD STAGE: every body is `todo!()`. They become real reads/writes of
//! `MyProc` / `GetPGProcByNumber(procno)` over this crate's owned `PGPROC`
//! array once the per-backend `MyProc` / `ProcGlobal` state lands.

use backend_storage_lmgr_proc_seams as seams;
use types_core::{LocalTransactionId, Oid, ProcNumber, TimestampTz, TransactionId};
use types_error::PgResult;
use types_storage::latch::LatchHandle;
use types_storage::{proclist_node, LWLockMode, LWLockWaitState};

fn proc_lw_waiting(_procno: ProcNumber) -> LWLockWaitState {
    todo!("proc.c: read GetPGProcByNumber(procno)->lwWaiting")
}

fn set_proc_lw_waiting(_procno: ProcNumber, _state: LWLockWaitState) {
    todo!("proc.c: write GetPGProcByNumber(procno)->lwWaiting")
}

fn proc_lw_wait_mode(_procno: ProcNumber) -> LWLockMode {
    todo!("proc.c: read GetPGProcByNumber(procno)->lwWaitMode")
}

fn set_proc_lw_wait_mode(_procno: ProcNumber, _mode: LWLockMode) {
    todo!("proc.c: write GetPGProcByNumber(procno)->lwWaitMode")
}

fn proc_lw_wait_link(_procno: ProcNumber) -> proclist_node {
    todo!("proc.c: read GetPGProcByNumber(procno)->lwWaitLink")
}

fn set_proc_lw_wait_link(_procno: ProcNumber, _node: proclist_node) {
    todo!("proc.c: write GetPGProcByNumber(procno)->lwWaitLink")
}

fn proc_cv_wait_link(_procno: ProcNumber) -> proclist_node {
    todo!("proc.c: read GetPGProcByNumber(procno)->cvWaitLink")
}

fn set_proc_cv_wait_link(_procno: ProcNumber, _node: proclist_node) {
    todo!("proc.c: write GetPGProcByNumber(procno)->cvWaitLink")
}

fn set_proc_latch(_procno: ProcNumber) {
    todo!("proc.c: SetLatch(&GetPGProcByNumber(procno)->procLatch)")
}

fn pg_semaphore_lock(_procno: ProcNumber) {
    todo!("proc.c: PGSemaphoreLock(GetPGProcByNumber(procno)->sem)")
}

fn pg_semaphore_unlock(_procno: ProcNumber) {
    todo!("proc.c: PGSemaphoreUnlock(GetPGProcByNumber(procno)->sem)")
}

fn proc_wait_for_signal(_wait_event_info: u32) -> PgResult<()> {
    todo!("proc.c: ProcWaitForSignal — see proc_misc::ProcWaitForSignal")
}

fn deadlock_timeout() -> i32 {
    todo!("proc.c: DeadlockTimeout GUC")
}

fn my_proc_wait_start() -> TimestampTz {
    todo!("proc.c: pg_atomic_read_u64(&MyProc->waitStart)")
}

fn set_my_proc_wait_start(_value: TimestampTz) {
    todo!("proc.c: pg_atomic_write_u64(&MyProc->waitStart, value)")
}

fn set_my_proc_vxid_proc_number(_value: ProcNumber) {
    todo!("proc.c: MyProc->vxid.procNumber = value")
}

fn set_my_proc_temp_namespace_id(_nspid: Oid) {
    todo!("proc.c: MyProc->tempNamespaceId = nspid")
}

fn my_proc_lxid() -> LocalTransactionId {
    todo!("proc.c: read MyProc->vxid.lxid")
}

fn set_my_proc_lxid(_lxid: LocalTransactionId) {
    todo!("proc.c: write MyProc->vxid.lxid")
}

fn transaction_timeout() -> i32 {
    todo!("proc.c: TransactionTimeout GUC")
}

fn lock_error_cleanup() {
    todo!("proc.c: LockErrorCleanup — see proc_waitqueue::LockErrorCleanup")
}

fn my_proc_set_delay_chkpt_start(_on: bool) {
    todo!("proc.c: set/clear DELAY_CHKPT_START in MyProc->delayChkptFlags")
}

fn proc_latch(_procno: ProcNumber) -> LatchHandle {
    todo!("proc.c: &GetPGProcByNumber(procno)->procLatch")
}

fn proc_init_prepared(
    _pgprocno: ProcNumber,
    _xid: TransactionId,
    _owner: Oid,
    _databaseid: Oid,
) -> PgResult<()> {
    todo!("proc.c: MarkAsPreparingGuts dummy-PGPROC init")
}

fn gxact_load_subxact_data(_pgprocno: ProcNumber, _children: &[TransactionId]) -> PgResult<()> {
    todo!("proc.c: GXactLoadSubxactData")
}

fn my_proc_number() -> ProcNumber {
    todo!("proc.c: MyProcNumber")
}

fn proc_database_id(_pgprocno: ProcNumber) -> Oid {
    todo!("proc.c: GetPGProcByNumber(pgprocno)->databaseId")
}

fn proc_xid(_pgprocno: ProcNumber) -> TransactionId {
    todo!("proc.c: GetPGProcByNumber(pgprocno)->xid")
}

fn proc_vxid(_pgprocno: ProcNumber) -> (ProcNumber, u32) {
    todo!("proc.c: GET_VXID_FROM_PGPROC(vxid, *GetPGProcByNumber(pgprocno))")
}

fn prepared_xact_procno(_i: i32) -> ProcNumber {
    todo!("proc.c: GetNumberFromPGProc(&PreparedXactProcs[i])")
}

fn set_delay_chkpt_start(_on: bool) {
    todo!("proc.c: set/clear DELAY_CHKPT_START in MyProc->delayChkptFlags")
}

// --- wait-queue PGPROC accessors (proc_waitqueue family) --------------------
// Bodies remain todo!() until InitProcGlobal / InitProcess land the real
// ProcGlobal->allProcs array and per-backend MyProc.

fn pgproc_number(_proc: &types_storage::storage::PGPROC) -> ProcNumber {
    todo!("proc.c: GetNumberFromPGProc(proc)")
}

fn proc_lock_group_leader(_procno: ProcNumber) -> ProcNumber {
    todo!("proc.c: GetPGProcByNumber(procno)->lockGroupLeader as ProcNumber")
}

fn set_proc_held_locks(_procno: ProcNumber, _mask: types_storage::lock::LOCKMASK) {
    todo!("proc.c: GetPGProcByNumber(procno)->heldLocks = mask")
}

fn proc_held_locks(_procno: ProcNumber) -> types_storage::lock::LOCKMASK {
    todo!("proc.c: read GetPGProcByNumber(procno)->heldLocks")
}

fn proc_wait_lock_mode(_procno: ProcNumber) -> types_storage::lock::LOCKMODE {
    todo!("proc.c: read GetPGProcByNumber(procno)->waitLockMode")
}

fn proc_wait_status(_procno: ProcNumber) -> types_storage::storage::ProcWaitStatus {
    todo!("proc.c: read GetPGProcByNumber(procno)->waitStatus")
}

fn set_proc_wait_fields(
    _procno: ProcNumber,
    _lock: types_storage::lock::LOCKTAG,
    _holder: ProcNumber,
    _lockmode: types_storage::lock::LOCKMODE,
) {
    todo!("proc.c: MyProc->{{waitLock,waitProcLock,waitLockMode,waitStatus}}")
}

fn set_proc_wait_start(_procno: ProcNumber, _value: u64) {
    todo!("proc.c: pg_atomic_write_u64(&GetPGProcByNumber(procno)->waitStart, value)")
}

fn proc_wait_link_is_detached(_procno: ProcNumber) -> bool {
    todo!("proc.c: dlist_node_is_detached(&GetPGProcByNumber(procno)->links)")
}

fn wakeup_proc_clear_wait(_procno: ProcNumber, _status: types_storage::storage::ProcWaitStatus) {
    todo!("proc.c: ProcWakeup state reset (waitLock/waitProcLock/waitStatus/waitStart)")
}

fn proc_unlinked_from_wait_queue(_procno: ProcNumber) -> bool {
    todo!("proc.c: MyProc->links.prev == NULL || MyProc->links.next == NULL")
}

fn proc_is_waiting_on_lock(_procno: ProcNumber) -> bool {
    todo!("proc.c: MyProc->waitLock != NULL")
}

fn proc_wait_lock_tag(_procno: ProcNumber) -> types_storage::lock::LOCKTAG {
    todo!("proc.c: MyProc->waitLock->tag")
}

fn proc_pgxactoff(_procno: ProcNumber) -> i32 {
    todo!("proc.c: GetPGProcByNumber(procno)->pgxactoff")
}

fn proc_global_status_flags(_pgxactoff: i32) -> u8 {
    todo!("proc.c: ProcGlobal->statusFlags[pgxactoff]")
}

fn proc_pid(_procno: ProcNumber) -> i32 {
    todo!("proc.c: GetPGProcByNumber(procno)->pid")
}

/// Install every inward seam this unit owns.
pub(crate) fn install() {
    seams::proc_lw_waiting::set(proc_lw_waiting);
    seams::set_proc_lw_waiting::set(set_proc_lw_waiting);
    seams::proc_lw_wait_mode::set(proc_lw_wait_mode);
    seams::set_proc_lw_wait_mode::set(set_proc_lw_wait_mode);
    seams::proc_lw_wait_link::set(proc_lw_wait_link);
    seams::set_proc_lw_wait_link::set(set_proc_lw_wait_link);
    seams::proc_cv_wait_link::set(proc_cv_wait_link);
    seams::set_proc_cv_wait_link::set(set_proc_cv_wait_link);
    seams::set_proc_latch::set(set_proc_latch);
    seams::pg_semaphore_lock::set(pg_semaphore_lock);
    seams::pg_semaphore_unlock::set(pg_semaphore_unlock);
    seams::proc_wait_for_signal::set(proc_wait_for_signal);
    seams::deadlock_timeout::set(deadlock_timeout);
    seams::my_proc_wait_start::set(my_proc_wait_start);
    seams::set_my_proc_wait_start::set(set_my_proc_wait_start);
    seams::set_my_proc_vxid_proc_number::set(set_my_proc_vxid_proc_number);
    seams::set_my_proc_temp_namespace_id::set(set_my_proc_temp_namespace_id);
    seams::my_proc_lxid::set(my_proc_lxid);
    seams::set_my_proc_lxid::set(set_my_proc_lxid);
    seams::transaction_timeout::set(transaction_timeout);
    seams::lock_error_cleanup::set(lock_error_cleanup);
    seams::my_proc_set_delay_chkpt_start::set(my_proc_set_delay_chkpt_start);
    seams::proc_latch::set(proc_latch);
    seams::proc_init_prepared::set(proc_init_prepared);
    seams::gxact_load_subxact_data::set(gxact_load_subxact_data);
    seams::my_proc_number::set(my_proc_number);
    seams::proc_database_id::set(proc_database_id);
    seams::proc_xid::set(proc_xid);
    seams::proc_vxid::set(proc_vxid);
    seams::prepared_xact_procno::set(prepared_xact_procno);
    seams::set_delay_chkpt_start::set(set_delay_chkpt_start);

    // wait-queue PGPROC accessors
    seams::pgproc_number::set(pgproc_number);
    seams::proc_lock_group_leader::set(proc_lock_group_leader);
    seams::set_proc_held_locks::set(set_proc_held_locks);
    seams::proc_held_locks::set(proc_held_locks);
    seams::proc_wait_lock_mode::set(proc_wait_lock_mode);
    seams::proc_wait_status::set(proc_wait_status);
    seams::set_proc_wait_fields::set(set_proc_wait_fields);
    seams::set_proc_wait_start::set(set_proc_wait_start);
    seams::proc_wait_link_is_detached::set(proc_wait_link_is_detached);
    seams::wakeup_proc_clear_wait::set(wakeup_proc_clear_wait);
    seams::proc_unlinked_from_wait_queue::set(proc_unlinked_from_wait_queue);
    seams::proc_is_waiting_on_lock::set(proc_is_waiting_on_lock);
    seams::proc_wait_lock_tag::set(proc_wait_lock_tag);
    seams::proc_pgxactoff::set(proc_pgxactoff);
    seams::proc_global_status_flags::set(proc_global_status_flags);
    seams::proc_pid::set(proc_pid);
}
