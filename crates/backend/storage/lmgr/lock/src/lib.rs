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

mod fastpath;
mod locking;
mod recovery;
mod state;
mod tables;

// resowner.c's LOCKS-phase release path (`ResourceOwnerRelease`) calls these to
// hand a subtransaction's locks to its parent (commit) or release them (abort).
pub use locking::{LockReassignCurrentOwner, LockReleaseCurrentOwner};

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
    let max_backends = init_small_seams::max_backends::call() as Size;
    let max_prepared = init_small_seams::max_prepared_xacts::call() as Size;
    let sum = ipc_shmem_seams::add_size::call(max_backends, max_prepared)?;
    let total =
        ipc_shmem_seams::mul_size::call(max_locks_per_xact_get() as Size, sum)?;
    Ok(total as i64)
}

/// Total PGPROC slots = the holder `ProcNumber` range:
/// `MaxBackends + NUM_AUXILIARY_PROCS + max_prepared_xacts` (proc_shmem's
/// `TotalProcs`). Sizes the per-proc `myProcLocks` head array so every possible
/// holder (regular backends, auxiliary procs, and 2PC dummy procs) has a slot.
fn total_procs() -> usize {
    let max_backends = init_small_seams::max_backends::call() as usize;
    let max_prepared = init_small_seams::max_prepared_xacts::call() as usize;
    let num_aux = types_storage::storage::NUM_AUXILIARY_PROCS as usize;
    max_backends + num_aux + max_prepared
}

/// `LockManagerShmemInit()` (lock.c) — allocate the shared LOCK / PROCLOCK
/// hash tables and the fast-path strong-lock struct. In the single-process
/// model these are this backend's `thread_local`s; we simply (re)initialize
/// them empty (matching `ShmemInitHash` for a fresh segment + `SpinLockInit`).
pub fn LockManagerShmemInit() -> PgResult<()> {
    use ipc_shmem_seams::shmem_init_struct;

    // The C `ShmemInitHash` table sizes: `max_table_size` LOCK entries, 2x that
    // PROCLOCK entries (lock.c `InitLocks`). Our flat arena pre-sizes the LOCK /
    // PROCLOCK pools to exactly those caps in genuine cross-process shmem.
    let n_locks = nlockents()? as usize;
    let n_proclocks = n_locks.saturating_mul(2);

    let n_procs = total_procs();
    let bytes = state::arena_bytes(n_locks, n_proclocks, n_procs);
    let (base, found) = shmem_init_struct::call("Lock Table Arena", bytes)?;
    state::shmem_init(base, found, n_locks, n_proclocks, n_procs);

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
    use ipc_shmem_seams::add_size;
    use dynahash_seams::hash_estimate_size;

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

    // Our flat `ShmemInitStruct` arena (the genuine cross-process backing store
    // for the LOCK/PROCLOCK tables) must also fit; reserve at least its byte
    // size so ipci's accumulator covers the real allocation.
    let n_locks = max_table_size as usize;
    let n_proclocks = n_locks.saturating_mul(2);
    let arena = state::arena_bytes(n_locks, n_proclocks, total_procs());
    size = add_size::call(size, arena)?;
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
    hashfn_seams::tag_hash::call(&bytes, bytes.len())
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
// deadlock.c bridge: project the live shared LOCK/PROCLOCK graph into a
// `types_deadlock::LockSpace` arena (the C `DeadLockCheck` read of shared
// memory under all partition LWLocks).
// ===========================================================================

/// Build the `types_deadlock::LockSpace` arena from the live shared lock table,
/// returning it plus the `ProcId` for `my_proc`. The caller (proc.c
/// `CheckDeadLock`) holds all lock partition LWLocks, so the snapshot is a
/// consistent picture of the wait-for graph.
///
/// Identity convention: `ProcId(n) == ProcNumber(n)` for every PGPROC slot
/// (`0..total_procs`). Every such slot gets a `ProcSlot` (idle ones are simply
/// not waiting / hold nothing), so the detector's `my_proc` parameter and the
/// soft-deadlock writeback can map `ProcId` <-> `ProcNumber` directly. Locks are
/// appended after the proc slots, addressed by their own `LockId`.
///
/// What is projected (exactly the fields the detector reads):
///   * per proc: `pid`, `status_flags`, `lock_group_leader`/`lock_group_members`,
///     and — derived from the shared wait queues — `wait_lock` / `wait_lock_mode`
///     / `is_on_wait_queue`;
///   * per lock: `tag`, the holder `PROCLOCK`s (`my_proc` + `hold_mask`), and the
///     ordered `wait_procs` queue.
pub fn build_dead_lock_space(
    my_proc: types_core::ProcNumber,
) -> (types_deadlock::LockSpace, types_deadlock::ProcId) {
    use lmgr_proc_seams as proc;
    use types_core::INVALID_PROC_NUMBER;
    use types_deadlock::{LockId, LockSlot, LockSpace, ProcId, ProcLockSlot, ProcSlot};

    let n_procs = total_procs();
    let mut space = LockSpace::new();

    // One ProcSlot per PGPROC slot so ProcId(n) == ProcNumber(n). The per-proc
    // wait fields (wait_lock / wait_lock_mode / is_on_wait_queue) are filled in
    // below from the shared wait queues; here we record the proc-private fields.
    for n in 0..n_procs {
        let procno = n as types_core::ProcNumber;
        let mut slot = ProcSlot::new(proc::proc_pid::call(procno));
        slot.status_flags = proc::proc_status_flags::call(procno);
        slot.wait_lock_mode = proc::proc_wait_lock_mode::call(procno);
        let leader = proc::proc_lock_group_leader::call(procno);
        // C's lockGroupLeader points to self when in a group; a member's leader
        // field is the leader proc, the leader's own field also points to itself.
        // The detector treats `Some(leader)` (even == self) as "follow the leader",
        // which is correct: group_leader(self) == self is the identity case. But to
        // match the C `proc->lockGroupLeader ? ... : proc` we record None when the
        // proc is its own leader / has no group, so idle non-group procs stay None.
        slot.lock_group_leader = if leader == INVALID_PROC_NUMBER || leader == procno {
            None
        } else {
            Some(ProcId(leader as usize))
        };
        // Only a leader has a non-empty member list; record members so the
        // detector can follow group edges from a non-waiting leader.
        slot.lock_group_members = proc::proc_lock_group_members::call(procno)
            .into_iter()
            .map(|m| ProcId(m as usize))
            .collect();
        space.add_proc(slot);
    }

    // Enumerate every live LOCK from a full PROCLOCK scan (each distinct tag is a
    // LOCK), then for each LOCK record its holders (PROCLOCKs with a non-zero
    // holdMask) and its ordered wait queue.
    let all_proclocks = state::with_shared(|s| s.proclock_scan());

    // Distinct lock tags, preserving first-seen order for deterministic ids.
    let mut lock_id_of: alloc::vec::Vec<(LOCKTAG, LockId)> = alloc::vec::Vec::new();
    let find_or_add_lock = |space: &mut LockSpace,
                            lock_id_of: &mut alloc::vec::Vec<(LOCKTAG, LockId)>,
                            tag: &LOCKTAG|
     -> LockId {
        if let Some((_, id)) = lock_id_of.iter().find(|(t, _)| t == tag) {
            return *id;
        }
        let id = space.add_lock(LockSlot::new(*tag));
        lock_id_of.push((*tag, id));
        id
    };

    for (tag, holder, pl) in &all_proclocks {
        let lock_id = find_or_add_lock(&mut space, &mut lock_id_of, tag);
        // Record the holder PROCLOCK (the detector reads my_proc + hold_mask).
        let pl_id = space.add_proc_lock(ProcLockSlot {
            my_lock: lock_id,
            my_proc: ProcId(*holder as usize),
            hold_mask: pl.hold_mask,
        });
        space.lock_mut(lock_id).proc_locks.push(pl_id);
    }

    // Project each LOCK's ordered wait queue. A proc on a lock's wait queue gets
    // wait_lock / is_on_wait_queue set; the queue order is preserved so the
    // detector's soft-edge scan sees the true ordering. We iterate the distinct
    // lock tags we discovered above.
    let lock_tags: alloc::vec::Vec<(LOCKTAG, LockId)> = lock_id_of.clone();
    for (tag, lock_id) in lock_tags {
        let waiters = state::with_shared(|s| s.waiters(&tag));
        for w in waiters {
            let widx = w as usize;
            if widx < n_procs {
                let p = space.proc_mut(ProcId(widx));
                p.wait_lock = Some(lock_id);
                p.is_on_wait_queue = true;
            }
            space.lock_mut(lock_id).wait_procs.push(ProcId(widx));
        }
    }

    (space, ProcId(my_proc as usize))
}

/// Write the deadlock detector's resolved soft-deadlock wait order for one lock
/// back into the live shared wait queue, then wake any now-grantable waiters
/// (the C `DeadLockCheck` queue rearrangement + `ProcLockWakeup`).
pub fn apply_soft_deadlock_wait_order(
    lock: LOCKTAG,
    new_order: alloc::vec::Vec<types_core::ProcNumber>,
) {
    state::with_shared(|s| s.waitq_set_order(&lock, &new_order));
    locking::proc_lock_wakeup_for_tag(&lock);
}

// ===========================================================================
// Seam installation.
// ===========================================================================

/// Install the inward seams this unit can fully serve (F0). Called once from
/// `seams-init::init_all()`.
pub fn init_seams() {
    use lock_seams as seams;
    use guc_tables::{vars, GucVarAccessors};

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
    seams::do_lock_modes_conflict::set(locking::DoLockModesConflict);
    seams::get_lock_method_table::set(get_lock_method_table);
    seams::build_dead_lock_space::set(build_dead_lock_space);
    seams::apply_soft_deadlock_wait_order::set(apply_soft_deadlock_wait_order);

    // F1 LockAcquire / LockRelease pair.
    seams::lock_acquire_impl::set(|tag, mode, session, dont_wait| {
        locking::LockAcquire(tag, mode, session, dont_wait)
    });
    seams::lock_acquire::set(|tag, mode, session, dont_wait| {
        locking::LockAcquire(tag, mode, session, dont_wait)
    });
    seams::lock_acquire_extended::set(|tag, mode, session, dont_wait, log_lock_failure| {
        locking::LockAcquireExtended(tag, mode, session, dont_wait, log_lock_failure)
    });
    seams::lock_release_impl::set(|tag, mode, session| locking::LockRelease(tag, mode, session));
    // The bare `lock_release` seam discards the C `elog(ERROR, "unrecognized
    // lock mode")` leg into the bool (the WARNING-and-false path returns false);
    // no consumer relies on that error surface, so map any Err to false.
    seams::lock_release::set(|tag, mode, session| {
        locking::LockRelease(tag, mode, session).unwrap_or(false)
    });
    seams::mark_lock_clear::set(|tag, mode| locking::MarkLockClear(&tag, mode));

    // F1 release-all / session / introspection.
    seams::lock_release_all::set(locking::LockReleaseAll);
    seams::lock_release_all_user::set(|| {
        locking::LockReleaseAll(types_storage::lock::USER_LOCKMETHOD, true)
    });
    seams::lock_release_session::set(|lockmethodid| {
        // The C is infallible (no ereport on a valid method id from advisory
        // unlock-all); surface any Err as a no-op (cannot fire for a valid id).
        let _ = locking::LockReleaseSession(lockmethodid);
    });
    seams::lock_held_by_me::set(|tag, mode, orstronger| {
        locking::LockHeldByMe(&tag, mode, orstronger)
    });
    seams::lock_has_waiters::set(|tag, mode, session| {
        locking::LockHasWaiters(&tag, mode, session)
    });
    seams::lock_waiter_count::set(|tag| locking::LockWaiterCount(&tag));

    // F2 awaited-lock + strong-lock interlock (proc.c LockErrorCleanup).
    seams::abort_strong_lock_acquire::set(locking::AbortStrongLockAcquire);
    seams::get_awaited_lock_hashcode::set(locking::GetAwaitedLockHashcode);
    seams::grant_awaited_lock::set(locking::GrantAwaitedLock);
    seams::reset_awaited_lock::set(locking::ResetAwaitedLock);
    seams::remove_from_wait_queue::set(locking::RemoveFromWaitQueue);

    // Fine-grained (LOCKTAG, ProcNumber) callbacks proc.c's wait-queue
    // machinery (JoinWaitQueue / ProcSleep / ProcLockWakeup / CheckDeadLock)
    // and deadlock.c call back into. These RETIRE the latent panics those
    // merged units carried for the lmgr/deadlock wait path.
    seams::grant_lock::set(locking::seam_grant_lock);
    seams::lock_check_conflicts::set(locking::seam_lock_check_conflicts);
    seams::proclock_hold_mask::set(locking::seam_proclock_hold_mask);
    seams::lock_group_held_locks::set(locking::seam_lock_group_held_locks);
    seams::lock_wait_queue_is_empty::set(locking::seam_lock_wait_queue_is_empty);
    seams::lock_wait_queue_insert_before::set(locking::seam_lock_wait_queue_insert_before);
    seams::lock_wait_queue_push_tail::set(locking::seam_lock_wait_queue_push_tail);
    seams::lock_set_wait_mask_bit::set(locking::seam_lock_set_wait_mask_bit);
    seams::lock_wait_queue_delete::set(locking::seam_lock_wait_queue_delete);
    seams::lock_wait_queue_waiters_snapshot::set(locking::seam_lock_wait_queue_waiters_snapshot);
    seams::get_lock_holders_and_waiters::set(locking::get_lock_holders_and_waiters_seam);
    seams::get_running_transaction_locks::set(locking::GetRunningTransactionLocks);

    seams::virtual_xact_lock::set(locking::VirtualXactLock);
    seams::virtual_xact_lock_table_insert::set(locking::VirtualXactLockTableInsert);
    seams::virtual_xact_lock_table_cleanup::set(locking::VirtualXactLockTableCleanup);

    // Recovery / two-phase-commit / introspection entry points (recovery.rs).
    seams::at_prepare_locks::set(recovery::AtPrepare_Locks);
    seams::post_prepare_locks::set(recovery::PostPrepare_Locks);
    seams::lock_twophase_recover::set(recovery::lock_twophase_recover);
    seams::lock_twophase_postcommit::set(recovery::lock_twophase_postcommit);
    seams::lock_twophase_postabort::set(recovery::lock_twophase_postabort);
    seams::lock_twophase_standby_recover::set(recovery::lock_twophase_standby_recover);
    seams::get_lock_conflicts::set(recovery::GetLockConflicts);
    seams::get_lock_status_data::set(recovery::GetLockStatusData);
    seams::proc_locks_hold_masks::set(recovery::proc_locks_hold_masks);
    seams::blocking_pids::set(recovery::blocking_pids);

    // `describe_lock_tag` (proc.c's log path): delegate to lmgr.c's owner.
    seams::describe_lock_tag::set(|tag| {
        lmgr_seams::describe_lock_tag::call(tag)
    });
}
