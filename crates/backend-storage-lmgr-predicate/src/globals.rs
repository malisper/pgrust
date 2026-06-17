//! Thin wrappers over the inward-dependency seams predicate.c calls, plus the
//! named individual LWLock accessors and this module's GUC globals.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use core::cell::Cell;

use backend_storage_lmgr_lwlock::main_lock_ref;
use types_storage::LWLock;
use types_core::primitive::ProcNumber;
use types_storage::{
    PREDICATELOCK_MANAGER_LWLOCK_OFFSET, SERIALIZABLE_FINISHED_LIST_LOCK,
    SERIALIZABLE_PREDICATE_LIST_LOCK, SERIALIZABLE_XACT_HASH_LOCK, SERIAL_CONTROL_LOCK,
    NUM_PREDICATELOCK_PARTITIONS,
};

// ---------------------------------------------------------------------------
// Named individual LWLocks (lwlocklist.h offsets).
// ---------------------------------------------------------------------------

/// `SerializableXactHashLock` — protects PredXact and SerializableXidHash.
#[inline]
pub fn SerializableXactHashLock() -> &'static LWLock {
    main_lock_ref(SERIALIZABLE_XACT_HASH_LOCK as usize)
}

/// `SerializableFinishedListLock`.
#[inline]
pub fn SerializableFinishedListLock() -> &'static LWLock {
    main_lock_ref(SERIALIZABLE_FINISHED_LIST_LOCK as usize)
}

/// `SerializablePredicateListLock`.
#[inline]
pub fn SerializablePredicateListLock() -> &'static LWLock {
    main_lock_ref(SERIALIZABLE_PREDICATE_LIST_LOCK as usize)
}

/// `SerialControlLock`.
#[inline]
pub fn SerialControlLock() -> &'static LWLock {
    main_lock_ref(SERIAL_CONTROL_LOCK as usize)
}

/// `PredicateLockHashPartitionLock(hashcode)`.
#[inline]
pub fn PredicateLockHashPartitionLock(hashcode: u32) -> &'static LWLock {
    main_lock_ref(
        (PREDICATELOCK_MANAGER_LWLOCK_OFFSET as usize)
            + (hashcode as usize % NUM_PREDICATELOCK_PARTITIONS as usize),
    )
}

/// `PredicateLockHashPartitionLockByIndex(i)`.
#[inline]
pub fn PredicateLockHashPartitionLockByIndex(i: i32) -> &'static LWLock {
    main_lock_ref((PREDICATELOCK_MANAGER_LWLOCK_OFFSET as usize) + (i as usize))
}

// ---------------------------------------------------------------------------
// Inward-dependency seam wrappers.
// ---------------------------------------------------------------------------

#[inline]
pub fn my_proc_number() -> ProcNumber {
    backend_storage_lmgr_proc_seams::my_proc_number::call()
}

#[inline]
pub fn my_proc_pid() -> i32 {
    backend_utils_init_small_seams::my_proc_pid::call()
}

#[inline]
pub fn my_proc_vxid() -> types_core::VirtualTransactionId {
    backend_storage_lmgr_proc_seams::my_proc_vxid::call()
}

/// `ProcSendSignal(pgprocno)` — predicate.c wakes a waiting DEFERRABLE backend;
/// the latch-based equivalent in this tree is `SetLatch(&proc->procLatch)`.
#[inline]
pub fn proc_send_signal(procno: ProcNumber) {
    backend_storage_lmgr_proc_seams::set_proc_latch::call(procno);
}

/// `ProcWaitForSignal(WAIT_EVENT_SAFE_SNAPSHOT)`.
#[inline]
pub fn proc_wait_for_signal_safe_snapshot() -> types_error::PgResult<()> {
    backend_storage_lmgr_proc_seams::proc_wait_for_signal::call(WAIT_EVENT_SAFE_SNAPSHOT)
}

/// `WAIT_EVENT_SAFE_SNAPSHOT` (wait_event.h, IPC class) — re-exported from the
/// canonical `types-pgstat` definition (`PG_WAIT_IPC + 51`, the 0-based IPC
/// index of `SAFE_SNAPSHOT` in `wait_event_names.txt`).
pub use types_pgstat::wait_event::WAIT_EVENT_SAFE_SNAPSHOT;

#[inline]
pub fn recovery_in_progress() -> bool {
    backend_access_transam_xlog_seams::recovery_in_progress::call()
}

#[inline]
pub fn is_in_parallel_mode() -> bool {
    backend_access_transam_xact_seams::is_in_parallel_mode::call()
}

#[inline]
pub fn is_parallel_worker() -> bool {
    backend_access_transam_parallel::is_parallel_worker()
}

#[inline]
pub fn is_sub_transaction() -> bool {
    backend_access_transam_xact_seams::is_sub_transaction::call()
}

#[inline]
pub fn transaction_id_is_current_transaction_id(xid: types_core::TransactionId) -> bool {
    backend_access_transam_xact_seams::transaction_id_is_current_transaction_id::call(xid)
}

#[inline]
pub fn get_top_transaction_id_if_any() -> types_core::TransactionId {
    backend_access_transam_xact_seams::get_top_transaction_id_if_any::call()
}

#[inline]
pub fn get_transaction_snapshot() -> types_error::PgResult<types_snapshot::SnapshotData> {
    backend_utils_time_snapmgr_seams::get_transaction_snapshot::call()
}

#[inline]
pub fn get_snapshot_data() -> types_error::PgResult<types_snapshot::SnapshotData> {
    backend_storage_ipc_procarray_seams::get_snapshot_data::call()
}

#[inline]
pub fn proc_array_install_imported_xmin(
    xmin: types_core::TransactionId,
    sourcevxid: types_core::VirtualTransactionId,
) -> types_error::PgResult<bool> {
    backend_storage_ipc_procarray_seams::proc_array_install_imported_xmin::call(xmin, sourcevxid)
}

#[inline]
pub fn isolation_is_serializable() -> bool {
    // `IsolationIsSerializable()` == `XactIsoLevel == XACT_SERIALIZABLE`.
    // XACT_SERIALIZABLE == 3.
    backend_access_transam_xact_seams::xact_iso_level::call() == XACT_SERIALIZABLE
}

/// `XACT_SERIALIZABLE` (xact.h).
pub const XACT_SERIALIZABLE: i32 = 3;

#[inline]
pub fn xact_read_only() -> bool {
    backend_access_transam_xact_seams::xact_read_only::call()
}

#[inline]
pub fn read_next_transaction_id() -> types_core::TransactionId {
    backend_access_transam_varsup_seams::read_next_transaction_id::call()
}

#[inline]
pub fn max_backends() -> i32 {
    backend_utils_init_small_seams::max_backends::call()
}

#[inline]
pub fn max_prepared_xacts() -> i32 {
    backend_utils_init_small_seams::max_prepared_xacts::call()
}

#[inline]
pub fn register_two_phase_record(rmid: u8, info: u16, data: &[u8]) -> types_error::PgResult<()> {
    backend_access_transam_twophase_seams::register_two_phase_record::call(rmid, info, data)
}

// ---------------------------------------------------------------------------
// GUC globals (in C: `int max_predicate_locks_per_xact; /* in guc_tables.c */`,
// etc.). predicate.c owns these module globals (declared extern in guc_tables);
// modelled here as backend-local cells with the PostgreSQL defaults. The GUC
// engine sets them at startup.
// ---------------------------------------------------------------------------

thread_local! {
    static MAX_PREDICATE_LOCKS_PER_XACT: Cell<i32> = const { Cell::new(64) };
    static MAX_PREDICATE_LOCKS_PER_RELATION: Cell<i32> = const { Cell::new(-2) };
    static MAX_PREDICATE_LOCKS_PER_PAGE: Cell<i32> = const { Cell::new(2) };
    static SERIALIZABLE_BUFFERS: Cell<i32> = const { Cell::new(32) };
}

#[inline]
pub fn max_predicate_locks_per_xact() -> i32 {
    MAX_PREDICATE_LOCKS_PER_XACT.with(|c| c.get())
}
#[inline]
pub fn set_max_predicate_locks_per_xact(v: i32) {
    MAX_PREDICATE_LOCKS_PER_XACT.with(|c| c.set(v));
}
#[inline]
pub fn max_predicate_locks_per_relation() -> i32 {
    MAX_PREDICATE_LOCKS_PER_RELATION.with(|c| c.get())
}
#[inline]
pub fn set_max_predicate_locks_per_relation(v: i32) {
    MAX_PREDICATE_LOCKS_PER_RELATION.with(|c| c.set(v));
}
#[inline]
pub fn max_predicate_locks_per_page() -> i32 {
    MAX_PREDICATE_LOCKS_PER_PAGE.with(|c| c.get())
}
#[inline]
pub fn set_max_predicate_locks_per_page(v: i32) {
    MAX_PREDICATE_LOCKS_PER_PAGE.with(|c| c.set(v));
}
#[inline]
pub fn serializable_buffers() -> i32 {
    SERIALIZABLE_BUFFERS.with(|c| c.get())
}
#[inline]
pub fn set_serializable_buffers(v: i32) {
    SERIALIZABLE_BUFFERS.with(|c| c.set(v));
}
/// `bool XactDeferrable` (xact.c global) via the xact seam.
#[inline]
pub fn xact_deferrable() -> bool {
    backend_access_transam_xact_seams::xact_deferrable::call()
}

/// `ParallelContextActive()` (parallel.c) — true if a ParallelContext is alive
/// in this backend; used in a debug `Assert` in `ReleasePredicateLocks`.
#[inline]
pub fn parallel_context_active() -> bool {
    backend_access_transam_parallel::parallel_context_active()
}

/// `pg_lfind32(value, base, nelem)` (port/pg_lfind.h) — linear scan.
#[inline]
pub fn pg_lfind32(value: u32, base: &[u32], nelem: u32) -> bool {
    base[..nelem as usize].iter().any(|&x| x == value)
}
