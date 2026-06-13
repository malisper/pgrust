//! Per-owner seams reached from [`crate::proc_lifecycle`] (`proc.c`).
//!
//! proc.c's lifecycle code reaches into two kinds of state that this family
//! module does not itself own:
//!
//! * The cluster-wide `ProcGlobal` (`PROC_HDR`) / `AuxiliaryProcs` /
//!   `ProcStructLock` substrate and the freelist `dlist` over the real
//!   `PGPROC` array, plus the per-backend `MyProc` / `MyProcNumber`. These are
//!   set up by the sibling [`crate::proc_shmem`] family module
//!   (`InitProcGlobal`). Until it lands, the accessors here panic — a faithful
//!   panic-through that mirrors each C reach into `ProcGlobal` /
//!   `GetNumberFromPGProc` / `GetPGProcByNumber` exactly.
//!
//! * The unported outward neighbours proc.c calls: procarray
//!   (`ProcArrayAdd`/`ProcArrayRemove`), lwlock (`LWLockReleaseAll` /
//!   `LWLockAcquire` / `LWLockRelease` / `InitLWLockAccess`), latch (`OwnLatch`
//!   / `DisownLatch` / `SwitchToSharedLatch` / `SwitchBackToLocalLatch`),
//!   syncrep (`SyncRepCleanupAtProcExit`), condition-variable
//!   (`ConditionVariableCancelSleep`), pgstat wait-event, pmsignal
//!   (`RegisterPostmasterChildActive`), the deadlock checker
//!   (`InitDeadLockChecking`), the backend-class predicates
//!   (`AmAutoVacuumWorkerProcess` &c.), and the libc `getpid` / `kill`.
//!
//! These route through their owners' per-owner seam crates once those land;
//! the scaffold stage panics through them so the control flow in
//! `proc_lifecycle` is the real one, never a stub of proc.c's own logic.

use types_core::{LocalTransactionId, ProcNumber, TransactionId};
use types_datum::Datum;
use types_error::PgResult;
use types_storage::lock::{DeadLockState, LOCKMODE, LOCKTAG};
use types_storage::storage::PGPROC;

/// Which of the four `ProcGlobal` freelists supplies / receives a `PGPROC`,
/// matching the by-class partitioning `InitProcGlobal` builds. Re-exported from
/// the owned `PROC_HDR` layout in `types-storage`.
pub(crate) use types_storage::storage::FreeListId as FreeList;

// ---- ProcGlobal / MyProc substrate (owned by proc_shmem) ----
//
// These are this unit's OWN state: the `PROC_HDR` (`ProcGlobal`) value built by
// `InitProcGlobal` (held in `proc_shmem`'s `PROC_GLOBAL` thread-local) and the
// per-backend `MyProc` / `MyProcNumber` / `MyProcPid`. Each accessor is a thin
// read/write of that owned storage via `proc_shmem`.

/// `ProcGlobal != NULL`.
pub(crate) fn proc_global_is_set() -> bool {
    crate::proc_shmem::proc_global_initialized()
}

/// `AuxiliaryProcs != NULL`. `AuxiliaryProcs` is `&ProcGlobal->allProcs[
/// MaxBackends]`, so it exists exactly when `ProcGlobal` is built.
pub(crate) fn auxiliary_procs_is_set() -> bool {
    crate::proc_shmem::proc_global_initialized()
}

/// `MyProc != NULL`.
pub(crate) fn my_proc_is_set() -> bool {
    crate::proc_shmem::my_proc_is_set()
}

/// `MyProc = GetPGProcByNumber(procno)`.
pub(crate) fn set_my_proc(procno: ProcNumber) {
    crate::proc_shmem::set_my_proc_number(procno);
}

/// `MyProc = NULL`.
pub(crate) fn clear_my_proc() {
    crate::proc_shmem::clear_my_proc();
}

/// Run `f` with mutable access to `*MyProc` (`&mut *MyProc`). Replaces the
/// former `my_proc_mut() -> &'static mut PGPROC`: the borrow is scoped to the
/// closure, so no `&'static mut` escapes.
pub(crate) fn with_my_proc<R>(f: impl FnOnce(&mut PGPROC) -> R) -> R {
    crate::proc_shmem::with_my_proc(f)
}

/// `MyProcNumber` (proc.c backend-local global).
pub(crate) fn my_proc_number() -> ProcNumber {
    crate::proc_shmem::my_proc_number()
}

/// `MyProcNumber = procno` (`GetNumberFromPGProc(MyProc)`).
pub(crate) fn set_my_proc_number(procno: ProcNumber) {
    crate::proc_shmem::set_my_proc_number(procno);
}

/// `MyProcPid` — the backend's PID. Declared in `globals.c` (miscinit), not
/// proc.c, so it is read through the init-small owner's seam (matching how
/// `proc_waitqueue` reads it). Class-B panic-through until that owner lands.
pub(crate) fn my_proc_pid() -> i32 {
    backend_utils_init_small_seams::my_proc_pid::call()
}

/// `GetPGProcByNumber(procno)->procgloballist` mapped to a [`FreeList`].
pub(crate) fn proc_globallist_of(procno: ProcNumber) -> FreeList {
    crate::proc_shmem::proc_globallist_of(procno)
}

/// `dlist_container(PGPROC, links, dlist_pop_head_node(<list>))` — pop the head
/// of the chosen freelist, or `None` if it is empty. Caller holds
/// `ProcStructLock`.
pub(crate) fn freelist_pop_head(list: FreeList) -> Option<ProcNumber> {
    crate::proc_shmem::freelist_pop_head(list)
}

/// `dlist_push_head(<list>, &GetPGProcByNumber(procno)->links)`. Caller holds
/// `ProcStructLock`.
pub(crate) fn freelist_push_head(list: FreeList, procno: ProcNumber) {
    crate::proc_shmem::freelist_push_head(list, procno);
}

/// `dlist_push_tail(<list>, &GetPGProcByNumber(procno)->links)`. Caller holds
/// `ProcStructLock`.
pub(crate) fn freelist_push_tail(list: FreeList, procno: ProcNumber) {
    crate::proc_shmem::freelist_push_tail(list, procno);
}

/// Iterator over `ProcGlobal->freeProcs` (`dlist_foreach` in `HaveNFreeProcs`),
/// yielding once per entry. Caller holds `ProcStructLock`.
pub(crate) fn freelist_regular_iter() -> impl Iterator<Item = ProcNumber> {
    crate::proc_shmem::freelist_regular_snapshot().into_iter()
}

/// `SpinLockAcquire(ProcStructLock)`.
pub(crate) fn spin_lock_acquire_proc_struct_lock() {
    todo!("s_lock: SpinLockAcquire(ProcStructLock)")
}

/// `SpinLockRelease(ProcStructLock)`.
pub(crate) fn spin_lock_release_proc_struct_lock() {
    todo!("s_lock: SpinLockRelease(ProcStructLock)")
}

/// `ProcGlobal->spins_per_delay`.
pub(crate) fn proc_global_spins_per_delay() -> i32 {
    crate::proc_shmem::spins_per_delay()
}

/// `ProcGlobal->spins_per_delay = value`.
pub(crate) fn set_proc_global_spins_per_delay(value: i32) {
    crate::proc_shmem::set_spins_per_delay(value);
}

/// `ProcGlobal->startupBufferPinWaitBufId`.
pub(crate) fn proc_global_startup_buffer_pin_wait_buf_id() -> i32 {
    crate::proc_shmem::startup_buffer_pin_wait_buf_id()
}

/// `ProcGlobal->startupBufferPinWaitBufId = bufid`.
pub(crate) fn set_proc_global_startup_buffer_pin_wait_buf_id(bufid: i32) {
    crate::proc_shmem::set_startup_buffer_pin_wait_buf_id(bufid);
}

/// Index of the first `AuxiliaryProcs[i]` with `pid == 0`, or `None`. Caller
/// holds `ProcStructLock`.
pub(crate) fn auxiliary_proc_find_free() -> Option<i32> {
    crate::proc_shmem::auxiliary_proc_find_free()
}

/// `GetNumberFromPGProc(&AuxiliaryProcs[proctype])`.
pub(crate) fn auxiliary_proc_procno(proctype: i32) -> ProcNumber {
    crate::proc_shmem::auxiliary_proc_procno(proctype)
}

// ---- per-PGPROC field access on a slot by proc number (owned by proc_shmem) ----

/// `GetPGProcByNumber(procno)->pid`.
pub(crate) fn proc_pid(procno: ProcNumber) -> i32 {
    crate::proc_shmem::with_proc_by_number(procno, |p| p.pid)
}

/// `GetPGProcByNumber(procno)->pid = pid`.
pub(crate) fn set_proc_pid(procno: ProcNumber, pid: i32) {
    crate::proc_shmem::with_proc_by_number(procno, |p| p.pid = pid);
}

/// `GetPGProcByNumber(procno)->vxid.procNumber = value`.
pub(crate) fn set_proc_vxid_proc_number(procno: ProcNumber, value: ProcNumber) {
    crate::proc_shmem::with_proc_by_number(procno, |p| p.vxid.procNumber = value);
}

/// `GetPGProcByNumber(procno)->vxid.lxid = value`.
pub(crate) fn set_proc_vxid_lxid(procno: ProcNumber, value: LocalTransactionId) {
    crate::proc_shmem::with_proc_by_number(procno, |p| p.vxid.lxid = value);
}

/// `GetPGProcByNumber(procno)->lockGroupLeader` as a proc number, or `None`.
pub(crate) fn proc_lock_group_leader(procno: ProcNumber) -> Option<ProcNumber> {
    crate::proc_shmem::with_proc_by_number(procno, |p| p.lockGroupLeader)
}

/// `GetPGProcByNumber(procno)->lockGroupLeader = leader`.
pub(crate) fn set_proc_lock_group_leader(procno: ProcNumber, leader: Option<ProcNumber>) {
    crate::proc_shmem::with_proc_by_number(procno, |p| p.lockGroupLeader = leader);
}

/// `dlist_is_empty(&GetPGProcByNumber(procno)->lockGroupMembers)`.
pub(crate) fn proc_lock_group_members_is_empty(procno: ProcNumber) -> bool {
    crate::proc_shmem::with_proc_by_number(procno, |p| p.lockGroupMembers.members.is_empty())
}

/// `dlist_delete(&GetPGProcByNumber(procno)->lockGroupLink)` — remove `procno`
/// from its leader's `lockGroupMembers` list.
pub(crate) fn dlist_delete_lock_group_link(procno: ProcNumber) {
    crate::proc_shmem::dlist_delete_lock_group_link(procno);
}

/// `lockAwaited != NULL`. `lockAwaited` is a `LOCALLOCK *` into lock.c's local
/// lock table; proc.c reaches it (here and in `LockErrorCleanup`) through lock.c
/// seams, where `get_awaited_lock_hashcode()` returns `-1` for a NULL
/// `lockAwaited`. Class-B (lock.c-owned), consistent with the sibling
/// `grant_awaited_lock` / `reset_awaited_lock` ops.
pub(crate) fn lock_awaited_is_set() -> bool {
    backend_storage_lmgr_lock_seams::get_awaited_lock_hashcode::call() != -1
}

// ---- backend-class predicates (miscadmin.h) ----

/// `AmAutoVacuumWorkerProcess()`.
pub(crate) fn am_autovacuum_worker_process() -> bool {
    todo!("miscadmin: AmAutoVacuumWorkerProcess()")
}

/// `AmSpecialWorkerProcess()`.
pub(crate) fn am_special_worker_process() -> bool {
    todo!("miscadmin: AmSpecialWorkerProcess()")
}

/// `AmBackgroundWorkerProcess()`.
pub(crate) fn am_background_worker_process() -> bool {
    todo!("miscadmin: AmBackgroundWorkerProcess()")
}

/// `AmWalSenderProcess()`.
pub(crate) fn am_wal_sender_process() -> bool {
    todo!("miscadmin: AmWalSenderProcess()")
}

/// `AmRegularBackendProcess()`.
pub(crate) fn am_regular_backend_process() -> bool {
    todo!("miscadmin: AmRegularBackendProcess()")
}

/// `IsUnderPostmaster`.
pub(crate) fn is_under_postmaster() -> bool {
    todo!("miscadmin: IsUnderPostmaster")
}

/// `max_wal_senders` GUC.
pub(crate) fn max_wal_senders() -> i32 {
    todo!("walsender: max_wal_senders GUC")
}

/// `AutovacuumLauncherPid` (postmaster global).
pub(crate) fn autovacuum_launcher_pid() -> i32 {
    todo!("postmaster: AutovacuumLauncherPid")
}

// ---- libc ----

/// `getpid()`.
pub(crate) fn getpid() -> i32 {
    todo!("libc: getpid()")
}

/// `kill(pid, SIGUSR2)`.
pub(crate) fn kill_sigusr2(_pid: i32) {
    todo!("libc: kill(pid, SIGUSR2)")
}

// ---- spin-delay estimate (s_lock.c) ----

/// `set_spins_per_delay(value)`.
pub(crate) fn set_spins_per_delay(_value: i32) {
    todo!("s_lock: set_spins_per_delay(value)")
}

/// `update_spins_per_delay(value)`.
pub(crate) fn update_spins_per_delay(_value: i32) -> i32 {
    todo!("s_lock: update_spins_per_delay(value)")
}

// ---- pmsignal ----

/// `RegisterPostmasterChildActive()`.
pub(crate) fn register_postmaster_child_active() {
    todo!("pmsignal: RegisterPostmasterChildActive()")
}

// ---- ipc ----

/// `on_shmem_exit(callback, arg)` — register a backend-exit callback.
pub(crate) fn on_shmem_exit(_callback: fn(i32, Datum), _arg: Datum) {
    todo!("ipc: on_shmem_exit(callback, arg)")
}

// ---- latch ----

/// `OwnLatch(&GetPGProcByNumber(procno)->procLatch)`.
pub(crate) fn own_latch(_procno: ProcNumber) {
    todo!("latch: OwnLatch(&proc->procLatch)")
}

/// `DisownLatch(&GetPGProcByNumber(procno)->procLatch)`.
pub(crate) fn disown_latch(_procno: ProcNumber) {
    todo!("latch: DisownLatch(&proc->procLatch)")
}

/// `SwitchToSharedLatch()`.
pub(crate) fn switch_to_shared_latch() {
    todo!("latch: SwitchToSharedLatch()")
}

/// `SwitchBackToLocalLatch()`.
pub(crate) fn switch_back_to_local_latch() {
    todo!("latch: SwitchBackToLocalLatch()")
}

// ---- pgstat wait events ----

/// `pgstat_set_wait_event_storage(&GetPGProcByNumber(procno)->wait_event_info)`.
pub(crate) fn pgstat_set_wait_event_storage(_procno: ProcNumber) {
    todo!("pgstat: pgstat_set_wait_event_storage(&proc->wait_event_info)")
}

/// `pgstat_reset_wait_event_storage()`.
pub(crate) fn pgstat_reset_wait_event_storage() {
    todo!("pgstat: pgstat_reset_wait_event_storage()")
}

// ---- semaphore ----

/// `PGSemaphoreReset(GetPGProcByNumber(procno)->sem)`.
pub(crate) fn pg_semaphore_reset(_procno: ProcNumber) {
    todo!("pg_sema: PGSemaphoreReset(proc->sem)")
}

// ---- lwlock ----

/// `InitLWLockAccess()`.
pub(crate) fn init_lwlock_access() {
    todo!("lwlock: InitLWLockAccess()")
}

/// `LWLockReleaseAll()`.
pub(crate) fn lwlock_release_all() {
    todo!("lwlock: LWLockReleaseAll()")
}

/// An LWLock handle returned by [`lock_hash_partition_lock_by_proc`].
pub(crate) type LWLockHandle = ProcNumber;

/// `LockHashPartitionLockByProc(GetPGProcByNumber(procno))` — the lock-manager
/// partition LWLock guarding `procno`'s lock group (lock.c).
pub(crate) fn lock_hash_partition_lock_by_proc(_procno: ProcNumber) -> LWLockHandle {
    todo!("lock: LockHashPartitionLockByProc(leader)")
}

/// `LWLockAcquire(lock, LW_EXCLUSIVE)`.
pub(crate) fn lwlock_acquire_exclusive(_lock: LWLockHandle) {
    todo!("lwlock: LWLockAcquire(lock, LW_EXCLUSIVE)")
}

/// `LWLockRelease(lock)`.
pub(crate) fn lwlock_release(_lock: LWLockHandle) {
    todo!("lwlock: LWLockRelease(lock)")
}

// ---- deadlock checker ----
//
// proc.c's relationship to deadlock.c is the C `DeadLockCheck(MyProc)` boundary:
// proc.c hands the checker its `MyProc` (a `ProcNumber` here) and reads back a
// `DeadLockState` / the blocking-autovacuum proc, while the checker walks the
// lock.c-owned shmem LOCK/PROCLOCK tables. That arena is owned by lock.c (still
// unported), so these stay Class-B panic-through with proc.c's own narrow
// `ProcNumber`/`DeadLockState` capability. (The merged `*-deadlock-seams` crate
// models the same calls over a lock.c-built `LockSpace` arena; wiring proc.c to
// that interface is the lock.c-integration step, not this unit's own logic.)

/// `InitDeadLockChecking()`.
pub(crate) fn init_deadlock_checking() {
    todo!("deadlock: InitDeadLockChecking()")
}

/// `DeadLockCheck(MyProc)` — run the deadlock check rooted at this backend's
/// proc, returning the resulting state. Walks lock.c-owned shmem tables.
pub(crate) fn deadlock_check(_procno: ProcNumber) -> DeadLockState {
    todo!("deadlock: DeadLockCheck(MyProc)")
}

/// `GetBlockingAutoVacuumPgproc()` — the autovacuum worker found by the last
/// `DeadLockCheck` to be directly blocking us, as a `ProcNumber`.
pub(crate) fn get_blocking_autovacuum_pgproc() -> ProcNumber {
    todo!("deadlock: GetBlockingAutoVacuumPgproc()")
}

/// `RememberSimpleDeadLock(proc1, lockmode, lock, proc2)` — record an
/// already-detected (non-search) two-way deadlock for the eventual report.
pub(crate) fn remember_simple_deadlock(
    _proc1: ProcNumber,
    _lockmode: LOCKMODE,
    _lock: LOCKTAG,
    _proc2: ProcNumber,
) {
    todo!("deadlock: RememberSimpleDeadLock(proc1, lockmode, lock, proc2)")
}

// ---- condition variable ----

/// `ConditionVariableCancelSleep()`.
pub(crate) fn condition_variable_cancel_sleep() {
    todo!("condition_variable: ConditionVariableCancelSleep()")
}

// ---- syncrep ----

/// `SyncRepCleanupAtProcExit()`.
pub(crate) fn sync_rep_cleanup_at_proc_exit() {
    todo!("syncrep: SyncRepCleanupAtProcExit()")
}

// ---- procarray ----

/// `ProcArrayAdd(GetPGProcByNumber(procno))`.
pub(crate) fn proc_array_add(_procno: ProcNumber) {
    todo!("procarray: ProcArrayAdd(MyProc)")
}

/// `ProcArrayRemove(GetPGProcByNumber(procno), latestXid)`.
pub(crate) fn proc_array_remove(_procno: ProcNumber, _latest_xid: TransactionId) {
    todo!("procarray: ProcArrayRemove(MyProc, InvalidTransactionId)")
}

// ---- elog / ereport ----

/// `elog(PANIC, msg)`.
pub(crate) fn elog_panic(_msg: &str) -> ! {
    todo!("elog(PANIC, ...)")
}

/// `elog(FATAL, msg)`.
pub(crate) fn elog_fatal(_msg: &str) -> ! {
    todo!("elog(FATAL, ...)")
}

/// `elog(ERROR, msg)` — surfaced as the `PgResult` error path.
pub(crate) fn elog_error(_msg: &str) -> PgResult<()> {
    todo!("elog(ERROR, ...)")
}

/// `ereport(FATAL, errcode(ERRCODE_TOO_MANY_CONNECTIONS), errmsg("sorry, too
/// many clients already"))`.
pub(crate) fn ereport_fatal_too_many_clients() -> PgResult<()> {
    todo!("ereport(FATAL, ERRCODE_TOO_MANY_CONNECTIONS, 'sorry, too many clients already')")
}

/// `ereport(FATAL, errcode(ERRCODE_TOO_MANY_CONNECTIONS), errmsg("number of
/// requested standby connections exceeds max_wal_senders (currently %d)"))`.
pub(crate) fn ereport_fatal_too_many_wal_senders(_max_wal_senders: i32) -> PgResult<()> {
    todo!("ereport(FATAL, ERRCODE_TOO_MANY_CONNECTIONS, 'exceeds max_wal_senders')")
}
