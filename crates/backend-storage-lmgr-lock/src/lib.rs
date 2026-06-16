//! POSTGRES primary heavyweight-lock manager — `storage/lmgr/lock.c`.
//!
//! This crate is the ambient-global owner of the shared lock table
//! (`LockMethodLockHash` / `LockMethodProcLockHash`), the backend-private
//! LOCALLOCK table (`LockMethodLocalHash`), the fast-path strong-lock counts,
//! and the lock-method conflict tables. It installs the fine-grained,
//! zero-context seams declared in [`backend-storage-lmgr-lock-seams`] that
//! proc.c's wait-queue machinery (`proc_waitqueue.rs`) and deadlock.c call
//! back into, keyed on `(LOCKTAG, ProcNumber)`.
//!
//! # Architecture (why this is an ambient global, not a value object)
//!
//! Two already-merged consumers pin the contract:
//!   * **proc.c** owns the wait queue — `JoinWaitQueue(&mut LOCALLOCK, ...)` /
//!     `ProcSleep(&mut LOCK, ...)` / `ProcWakeup` / `ProcLockWakeup` are public
//!     functions in the merged proc crate that take `&mut LOCALLOCK` / `&mut
//!     LOCK`. lock.c (here) holds those structs in its ambient table and lends
//!     `&mut` to them when calling proc.c; proc.c in turn calls lock.c's
//!     context-free seams (`grant_lock` / `lock_check_conflicts` / the
//!     `lock_wait_queue_*` family / `proclock_hold_mask` / ...) which reach the
//!     ambient table by `(LOCKTAG, ProcNumber)`.
//!   * **deadlock.c** models the graph in its own `types_deadlock::LockSpace`
//!     arena; lock.c only answers `get_lock_method_table(&LockSpace, LockId)`
//!     and `get_lockmode_name` over that vocabulary.
//!
//! Following the repo's single-process shmem model (proc.c's `PROC_GLOBAL`),
//! the shared tables are per-backend `thread_local`s (see [`state`]).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

mod state;
mod tables;

use types_core::Size;
use types_error::PgResult;
use types_storage::lock::{LOCKMETHODID, LOCKMODE, LOCKTAG};

// ===========================================================================
// GUC variables owned by lock.c (`int max_locks_per_xact`,
// `bool log_lock_failures`).
// ===========================================================================

thread_local! {
    /// `int max_locks_per_xact` (lock.c GUC `max_locks_per_transaction`).
    static MAX_LOCKS_PER_XACT: core::cell::Cell<i32> = const { core::cell::Cell::new(64) };
    /// `bool log_lock_failures` (lock.c GUC `log_lock_failures`).
    static LOG_LOCK_FAILURES: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };
}

fn max_locks_per_xact_get() -> i32 {
    MAX_LOCKS_PER_XACT.with(|c| c.get())
}
fn max_locks_per_xact_set(v: i32) {
    MAX_LOCKS_PER_XACT.with(|c| c.set(v));
}
fn log_lock_failures_get() -> bool {
    LOG_LOCK_FAILURES.with(|c| c.get())
}
fn log_lock_failures_set(v: bool) {
    LOG_LOCK_FAILURES.with(|c| c.set(v));
}

/// `max_locks_per_xact` — the value of the `max_locks_per_transaction` GUC.
pub fn max_locks_per_xact() -> i32 {
    max_locks_per_xact_get()
}

// ===========================================================================
// F0: shmem table init / size, backend-local init, hashcodes.
// ===========================================================================

/// `NLOCKENTS()` (lock.c) —
/// `mul_size(max_locks_per_xact, add_size(MaxBackends, max_prepared_xacts))`.
fn nlockents() -> PgResult<i64> {
    let max_backends = backend_utils_init_small_seams::max_backends::call() as Size;
    let max_prepared = backend_utils_init_small_seams::max_prepared_xacts::call() as Size;
    let sum = backend_storage_ipc_shmem_seams::add_size::call(max_backends, max_prepared)?;
    let total =
        backend_storage_ipc_shmem_seams::mul_size::call(max_locks_per_xact_get() as Size, sum)?;
    Ok(total as i64)
}

/// `LockManagerShmemInit()` (lock.c) — allocate the shared LOCK / PROCLOCK
/// hash tables and the fast-path strong-lock struct. In the single-process
/// model these are this backend's `thread_local`s; we simply (re)initialize
/// them empty (matching `ShmemInitHash` for a fresh segment + `SpinLockInit`).
pub fn LockManagerShmemInit() -> PgResult<()> {
    // The C computes init/max table sizes for ShmemInitHash; the HashMap model
    // grows on demand, so we only ensure the structures exist and are empty.
    state::with_shared(|s| {
        s.locks.clear();
        s.proclocks.clear();
    });
    state::FP_STRONG.with(|c| *c.borrow_mut() = state::FastPathStrongRelationLockData::default());
    Ok(())
}

/// `InitLockManagerAccess()` (lock.c) — initialize the backend-private
/// LOCALLOCK hash table.
pub fn InitLockManagerAccess() -> PgResult<()> {
    state::with_local(|m| m.clear());
    Ok(())
}

/// `LockManagerShmemSize()` (lock.c) — the shared-memory bytes the lock
/// manager needs (for ipci.c's accumulator). Computed exactly as in C:
/// `hash_estimate_size` of the LOCK table + the PROCLOCK table (2× entries) +
/// a 10% safety margin.
pub fn LockManagerShmemSize() -> PgResult<Size> {
    use backend_storage_ipc_shmem_seams::add_size;
    use backend_utils_hash_dynahash_seams::hash_estimate_size;

    let max_table_size = nlockents()?;
    let lock_sz = hash_estimate_size::call(max_table_size, core::mem::size_of::<
        types_storage::lock::LOCK,
    >());
    let mut size = add_size::call(0, lock_sz)?;

    // proclock hash table: 2× entries.
    let proclock_entries = max_table_size.saturating_mul(2);
    let proclock_sz = hash_estimate_size::call(
        proclock_entries,
        core::mem::size_of::<types_storage::lock::PROCLOCK>(),
    );
    size = add_size::call(size, proclock_sz)?;

    // 10% safety margin.
    size = add_size::call(size, size / 10)?;
    Ok(size)
}

/// Serialize a `LOCKTAG` to its 16-byte little-endian C layout (3×u32 + u16 +
/// u8 + u8) so the hash matches dynahash `tag_hash` over `sizeof(LOCKTAG)`.
fn locktag_bytes(tag: &LOCKTAG) -> [u8; 16] {
    let mut b = [0u8; 16];
    b[0..4].copy_from_slice(&tag.locktag_field1.to_ne_bytes());
    b[4..8].copy_from_slice(&tag.locktag_field2.to_ne_bytes());
    b[8..12].copy_from_slice(&tag.locktag_field3.to_ne_bytes());
    b[12..14].copy_from_slice(&tag.locktag_field4.to_ne_bytes());
    b[14] = tag.locktag_type;
    b[15] = tag.locktag_lockmethodid;
    b
}

/// `LockTagHashCode(locktag)` (lock.c) — the hash value of a LOCKTAG, computed
/// via the same `tag_hash` dynahash uses for `HASH_BLOBS` keys.
pub fn LockTagHashCode(locktag: &LOCKTAG) -> u32 {
    let bytes = locktag_bytes(locktag);
    common_hashfn_seams::tag_hash::call(&bytes, bytes.len())
}

// ===========================================================================
// F0: introspection / name helpers usable now.
// ===========================================================================

/// `GetLockmodeName(lockmethodid, mode)` (lock.c).
pub fn get_lockmode_name(lockmethodid: LOCKMETHODID, mode: LOCKMODE) -> alloc::string::String {
    alloc::string::String::from(tables::get_lockmode_name(lockmethodid, mode))
}

/// `lockMethodTable->conflictTab[mode]` for a given method id.
pub fn conflict_tab(
    lockmethodid: u8,
    mode: LOCKMODE,
) -> types_storage::lock::LOCKMASK {
    tables::conflict_tab_for(lockmethodid as LOCKMETHODID, mode)
}

// ===========================================================================
// deadlock.c bridge: get_lock_method_table over its own LockSpace vocabulary.
// ===========================================================================

/// `GetLocksMethodTable(lock)` (lock.c), expressed over deadlock.c's
/// `LockSpace` arena: build the `types_deadlock::LockMethodData` for the lock's
/// method (number of modes, conflict table, mode names).
pub fn get_lock_method_table(
    space: &types_deadlock::LockSpace,
    lock: types_deadlock::LockId,
) -> types_deadlock::LockMethodData {
    let tag = space.lock(lock).tag;
    let lockmethodid = tag.locktag_lockmethodid as LOCKMETHODID;

    let conflicts = tables::lock_conflicts();
    let mut conflict_tab = [0i32; types_storage::lock::MAX_LOCKMODES];
    for (i, c) in conflicts.iter().enumerate() {
        conflict_tab[i] = *c;
    }
    let mut lock_mode_names = [""; types_storage::lock::MAX_LOCKMODES];
    for (i, n) in tables::LOCK_MODE_NAMES.iter().enumerate() {
        lock_mode_names[i] = n;
    }

    types_deadlock::LockMethodData {
        num_lock_modes: tables::num_lock_modes(lockmethodid),
        conflict_tab,
        lock_mode_names,
    }
}

/// `GetLockmodeName(lockmethodid, mode)` as the deadlock-seam expects it
/// (returns an owned String).
pub fn get_lockmode_name_deadlock(
    lockmethodid: LOCKMETHODID,
    mode: LOCKMODE,
) -> alloc::string::String {
    get_lockmode_name(lockmethodid, mode)
}

// ===========================================================================
// Seam installation.
// ===========================================================================

/// Install the inward seams this unit can fully serve (F0). Called once from
/// `seams-init::init_all()`.
pub fn init_seams() {
    use backend_storage_lmgr_lock_seams as seams;
    use backend_utils_misc_guc_tables::{vars, GucVarAccessors};

    // GUC variable storage owned by lock.c.
    vars::max_locks_per_xact.install(GucVarAccessors {
        get: max_locks_per_xact_get,
        set: max_locks_per_xact_set,
    });
    vars::log_lock_failures.install(GucVarAccessors {
        get: log_lock_failures_get,
        set: log_lock_failures_set,
    });

    // F0 seams.
    seams::max_locks_per_xact::set(max_locks_per_xact);
    seams::lock_manager_shmem_init::set(LockManagerShmemInit);
    seams::lock_manager_shmem_size::set(LockManagerShmemSize);
    seams::init_lock_manager_access::set(InitLockManagerAccess);
    seams::lock_tag_hash_code::set(|tag| LockTagHashCode(&tag));
    seams::get_lockmode_name::set(get_lockmode_name);
    seams::conflict_tab::set(conflict_tab);
    seams::get_lock_method_table::set(get_lock_method_table);
}
