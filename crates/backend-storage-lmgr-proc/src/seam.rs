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

use types_core::init::BackendType;
use types_core::{LocalTransactionId, ProcNumber, TransactionId};
use types_tuple::Datum;
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

/// `SpinLockAcquire(ProcStructLock)`. The `ProcStructLock` word is this unit's
/// own state (held in `proc_shmem`); the contended-acquire backoff loop is the
/// merged `s_lock.c` primitive.
pub(crate) fn spin_lock_acquire_proc_struct_lock() {
    crate::proc_shmem::spin_lock_acquire_proc_struct_lock();
}

/// `SpinLockRelease(ProcStructLock)`.
pub(crate) fn spin_lock_release_proc_struct_lock() {
    crate::proc_shmem::spin_lock_release_proc_struct_lock();
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
//
// These are pure macros over `MyBackendType` (globals.c), so each is a direct
// read of the `my_backend_type()` global through the init-small owner's seam
// and a compare, exactly as the C macro expands.

/// `AmAutoVacuumWorkerProcess()` — `MyBackendType == B_AUTOVAC_WORKER`.
pub(crate) fn am_autovacuum_worker_process() -> bool {
    backend_utils_init_small_seams::my_backend_type::call() == BackendType::AutovacWorker
}

/// `AmSpecialWorkerProcess()` — `AmAutoVacuumLauncherProcess() ||
/// AmLogicalSlotSyncWorkerProcess()`.
pub(crate) fn am_special_worker_process() -> bool {
    let bt = backend_utils_init_small_seams::my_backend_type::call();
    bt == BackendType::AutovacLauncher || bt == BackendType::SlotsyncWorker
}

/// `AmBackgroundWorkerProcess()` — `MyBackendType == B_BG_WORKER`.
pub(crate) fn am_background_worker_process() -> bool {
    backend_utils_init_small_seams::my_backend_type::call() == BackendType::BgWorker
}

/// `AmWalSenderProcess()` — `MyBackendType == B_WAL_SENDER`.
pub(crate) fn am_wal_sender_process() -> bool {
    backend_utils_init_small_seams::my_backend_type::call() == BackendType::WalSender
}

/// `AmRegularBackendProcess()` — `MyBackendType == B_BACKEND`.
pub(crate) fn am_regular_backend_process() -> bool {
    backend_utils_init_small_seams::my_backend_type::call() == BackendType::Backend
}

/// `IsUnderPostmaster` (globals.c).
pub(crate) fn is_under_postmaster() -> bool {
    backend_utils_init_small_seams::is_under_postmaster::call()
}

/// `AmStartupProcess()` — `MyBackendType == B_STARTUP`. Only used in a
/// `MarkAsPreparingGuts` assert.
pub(crate) fn am_startup_process() -> bool {
    backend_utils_init_small_seams::my_backend_type::call() == BackendType::Startup
}

/// `max_wal_senders` GUC (walsender.c).
pub(crate) fn max_wal_senders() -> i32 {
    backend_replication_walsender_seams::max_wal_senders::call()
}

/// `AutovacuumLauncherPid` (autovacuum.c postmaster-side global).
pub(crate) fn autovacuum_launcher_pid() -> i32 {
    backend_postmaster_autovacuum_ext_seams::get_launcher_pid::call()
}

// ---- libc ----

/// `getpid()`.
pub(crate) fn getpid() -> i32 {
    std::process::id() as i32
}

/// `kill(pid, SIGUSR2)`.
pub(crate) fn kill_sigusr2(pid: i32) {
    // C `kill(AutovacuumLauncherPid, SIGUSR2)`: ProcKill's nudge of the
    // autovacuum launcher when an AV worker that was blocking us exits. A
    // failure here is non-fatal in C (the return value is ignored), so the
    // errno is dropped, matching the C call site.
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGUSR2);
    }
}

// ---- spin-delay estimate (s_lock.c) ----

/// `set_spins_per_delay(value)` (s_lock.c).
pub(crate) fn set_spins_per_delay(value: i32) {
    backend_storage_lmgr_s_lock::set_spins_per_delay(value);
}

/// `update_spins_per_delay(value)` (s_lock.c).
pub(crate) fn update_spins_per_delay(value: i32) -> i32 {
    backend_storage_lmgr_s_lock::update_spins_per_delay(value)
}

// ---- pmsignal ----

/// `RegisterPostmasterChildActive()` (pmsignal.c).
pub(crate) fn register_postmaster_child_active() {
    backend_storage_ipc_pmsignal_seams::register_postmaster_child_active::call()
        .expect("RegisterPostmasterChildActive");
}

// ---- ipc ----

/// `on_shmem_exit(callback, arg)` — register a backend-exit callback.
pub(crate) fn on_shmem_exit(callback: fn(i32, Datum<'static>) -> PgResult<()>, arg: Datum<'static>) {
    // C `on_shmem_exit` ereport(FATAL)s only on the static-array overflow past
    // `MAX_ON_EXITS`; surface that `Err` as a panic rather than swallow.
    backend_storage_ipc_dsm_core_seams::on_shmem_exit::call(callback, arg)
        .expect("on_shmem_exit: callback array full");
}

// ---- latch ----
//
// `OwnLatch`/`DisownLatch` (latch.c) and `SwitchToSharedLatch`/
// `SwitchBackToLocalLatch` (miscinit.c) operate on the latch *embedded in this
// backend's PGPROC* (`&MyProc->procLatch`). The ported latch unit reaches
// latches through a handle registry that does not yet know the PGPROC-embedded
// `procLatch`; registering it (so `OwnLatch` can resolve the handle and
// `SwitchToSharedLatch` can repoint `MyLatch` at it) is the latch <-> proc
// integration step. Until that bridge lands these abort loudly rather than call
// the registry with an unregistered handle.

/// `OwnLatch(&GetPGProcByNumber(procno)->procLatch)`.
pub(crate) fn own_latch(_procno: ProcNumber) {
    panic!("OwnLatch(&proc->procLatch): latch <-> proc PGPROC-latch bridge not yet wired")
}

/// `DisownLatch(&GetPGProcByNumber(procno)->procLatch)`.
pub(crate) fn disown_latch(_procno: ProcNumber) {
    panic!("DisownLatch(&proc->procLatch): latch <-> proc PGPROC-latch bridge not yet wired")
}

/// `SwitchToSharedLatch()` (miscinit.c).
pub(crate) fn switch_to_shared_latch() {
    panic!("SwitchToSharedLatch(): latch <-> proc PGPROC-latch bridge not yet wired")
}

/// `SwitchBackToLocalLatch()` (miscinit.c).
pub(crate) fn switch_back_to_local_latch() {
    panic!("SwitchBackToLocalLatch(): latch <-> proc PGPROC-latch bridge not yet wired")
}

// ---- pgstat wait events ----

/// `pgstat_set_wait_event_storage(&GetPGProcByNumber(procno)->wait_event_info)`.
pub(crate) fn pgstat_set_wait_event_storage(procno: ProcNumber) {
    backend_utils_activity_pgstat_seams::pgstat_set_wait_event_storage_for_proc::call(procno);
}

/// `pgstat_reset_wait_event_storage()`.
pub(crate) fn pgstat_reset_wait_event_storage() {
    backend_utils_activity_pgstat_seams::pgstat_reset_wait_event_storage::call();
}

// ---- semaphore ----

/// `PGSemaphoreReset(GetPGProcByNumber(procno)->sem)`.
pub(crate) fn pg_semaphore_reset(procno: ProcNumber) {
    backend_port_pg_sema_seams::pg_semaphore_reset::call(procno);
}

// ---- lwlock ----

/// `InitLWLockAccess()`.
pub(crate) fn init_lwlock_access() {
    backend_storage_lmgr_lwlock_seams::init_lwlock_access::call();
}

/// `LWLockReleaseAll()`.
pub(crate) fn lwlock_release_all() {
    backend_storage_lmgr_lwlock_seams::lwlock_release_all::call();
}

/// An LWLock handle returned by [`lock_hash_partition_lock_by_proc`] — the
/// `MainLWLockArray` offset of the lock-hash partition lock (lock.c).
pub(crate) type LWLockHandle = usize;

/// `LockHashPartitionLockByProc(GetPGProcByNumber(procno))` — the lock-manager
/// partition LWLock guarding `procno`'s lock group (lock.c). The offset
/// computation is lock.h's own arithmetic, reused from `proc_misc`.
pub(crate) fn lock_hash_partition_lock_by_proc(procno: ProcNumber) -> LWLockHandle {
    crate::proc_misc::lock_hash_partition_lock_offset_by_proc(procno)
}

/// `LWLockAcquire(lock, LW_EXCLUSIVE)`.
pub(crate) fn lwlock_acquire_exclusive(lock: LWLockHandle) {
    // C `LWLockAcquire` returns whether the lock had to wait; proc.c's caller
    // ignores it. The guard is dropped immediately because the matching
    // `lwlock_release` here issues an explicit release (mirroring the C
    // `LWLockRelease(lock)` rather than RAII), so `core::mem::forget` keeps the
    // owner-side acquisition live until that release.
    let guard = backend_storage_lmgr_lwlock_seams::lwlock_acquire_main::call(
        lock,
        types_storage::LWLockMode::LW_EXCLUSIVE,
    )
    .expect("LWLockAcquire(lock_group partition, LW_EXCLUSIVE)");
    core::mem::forget(guard);
}

/// `LWLockRelease(lock)`.
pub(crate) fn lwlock_release(lock: LWLockHandle) {
    backend_storage_lmgr_lwlock_seams::lwlock_release_main::call(lock)
        .expect("LWLockRelease(lock_group partition)");
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
    // C is void and aborts on OOM; surface the seam's `Err` as a panic.
    backend_storage_lmgr_deadlock_seams::init_dead_lock_checking::call()
        .expect("InitDeadLockChecking");
}

/// `DeadLockCheck(MyProc)` — run the deadlock check rooted at this backend's
/// proc, returning the resulting state. The merged deadlock checker's
/// `dead_lock_check` operates over a lock.c-built `LockSpace` arena (the lock
/// and proc records) that proc.c cannot construct until lock.c lands; that
/// bridge is the lock.c-integration step (see module note above), so this
/// aborts loudly until then rather than fabricating an arena.
pub(crate) fn deadlock_check(_procno: ProcNumber) -> DeadLockState {
    panic!("DeadLockCheck(MyProc) over the lock.c-built LockSpace arena: lock.c not yet ported")
}

/// `GetBlockingAutoVacuumPgproc()` — the autovacuum worker found by the last
/// `DeadLockCheck` to be directly blocking us, as a `ProcNumber` (or
/// `INVALID_PROC_NUMBER` when none).
pub(crate) fn get_blocking_autovacuum_pgproc() -> ProcNumber {
    match backend_storage_lmgr_deadlock_seams::get_blocking_auto_vacuum_pgproc::call() {
        Some(proc_id) => proc_id.0 as ProcNumber,
        None => types_core::INVALID_PROC_NUMBER,
    }
}

/// `RememberSimpleDeadLock(proc1, lockmode, lock, proc2)` — record an
/// already-detected (non-search) two-way deadlock for the eventual report.
/// Like [`deadlock_check`], the merged `remember_simple_dead_lock` records into
/// the lock.c-built `LockSpace` arena that proc.c cannot construct yet; this is
/// the lock.c-integration boundary, so it aborts loudly until lock.c lands.
pub(crate) fn remember_simple_deadlock(
    _proc1: ProcNumber,
    _lockmode: LOCKMODE,
    _lock: LOCKTAG,
    _proc2: ProcNumber,
) {
    panic!(
        "RememberSimpleDeadLock over the lock.c-built LockSpace arena: lock.c not yet ported"
    )
}

// ---- condition variable ----

/// `ConditionVariableCancelSleep()`.
pub(crate) fn condition_variable_cancel_sleep() {
    // C returns whether a sleep was actually cancelled; proc.c ignores it.
    backend_storage_lmgr_condition_variable_seams::condition_variable_cancel_sleep::call();
}

// ---- syncrep ----

/// `SyncRepCleanupAtProcExit()`.
pub(crate) fn sync_rep_cleanup_at_proc_exit() {
    backend_replication_syncrep_seams::sync_rep_cleanup_at_proc_exit::call();
}

// ---- procarray ----

/// `ProcArrayAdd(GetPGProcByNumber(procno))`.
pub(crate) fn proc_array_add(procno: ProcNumber) {
    // C is void and aborts on out-of-shared-memory; surface `Err` as a panic.
    backend_storage_ipc_procarray_seams::proc_array_add::call(procno).expect("ProcArrayAdd");
}

/// `ProcArrayRemove(GetPGProcByNumber(procno), latestXid)`.
pub(crate) fn proc_array_remove(procno: ProcNumber, latest_xid: TransactionId) {
    backend_storage_ipc_procarray_seams::proc_array_remove::call(procno, latest_xid)
        .expect("ProcArrayRemove");
}

// ---- elog / ereport ----
//
// proc.c's diagnostic ereports are this unit's own logic, emitted through the
// merged `backend-utils-error` crate (a direct dependency, not a seam).

/// Source file for the `ErrorLocation` of proc.c's ereports.
const SRC: &str = "src/backend/storage/lmgr/proc.c";

/// `elog(PANIC, msg)`.
pub(crate) fn elog_panic(msg: &str) -> ! {
    // PANIC is unconditionally fatal; the error path never returns. The merged
    // elog promotes PANIC to a process abort, realized here as a panic.
    let _ = backend_utils_error::elog(types_error::PANIC, msg.to_string());
    panic!("PANIC: {msg}")
}

/// `elog(FATAL, msg)`.
pub(crate) fn elog_fatal(msg: &str) -> ! {
    let _ = backend_utils_error::elog(types_error::FATAL, msg.to_string());
    panic!("FATAL: {msg}")
}

/// `elog(ERROR, msg)` — surfaced as the `PgResult` error path.
pub(crate) fn elog_error(msg: &str) -> PgResult<()> {
    backend_utils_error::elog(types_error::ERROR, msg.to_string())
}

/// `ereport(FATAL, errcode(ERRCODE_TOO_MANY_CONNECTIONS), errmsg("sorry, too
/// many clients already"))`.
pub(crate) fn ereport_fatal_too_many_clients() -> PgResult<()> {
    backend_utils_error::ereport(types_error::FATAL)
        .errcode(types_error::ERRCODE_TOO_MANY_CONNECTIONS)
        .errmsg("sorry, too many clients already")
        .finish(types_error::ErrorLocation::new(SRC, 457, "InitProcess"))
}

/// `ereport(FATAL, errcode(ERRCODE_TOO_MANY_CONNECTIONS), errmsg("number of
/// requested standby connections exceeds \"max_wal_senders\" (currently %d)"))`.
pub(crate) fn ereport_fatal_too_many_wal_senders(max_wal_senders: i32) -> PgResult<()> {
    backend_utils_error::ereport(types_error::FATAL)
        .errcode(types_error::ERRCODE_TOO_MANY_CONNECTIONS)
        .errmsg(format!(
            "number of requested standby connections exceeds \"max_wal_senders\" (currently {max_wal_senders})"
        ))
        .finish(types_error::ErrorLocation::new(SRC, 453, "InitProcess"))
}
