//! Unit tests for the lightweight-lock manager. The PGPROC array, wait
//! semaphores, interrupt holdoff, and the shmem spinlock are modeled by
//! process-wide test fakes so the in-crate wait-list/wakeup machinery can be
//! exercised single-threaded.

use super::*;
use core::cell::{Cell, RefCell};
use core::sync::atomic::{AtomicBool, Ordering as O};
use std::sync::{Mutex, Once};
use types_storage::{pg_atomic_uint32, LWLockWaitState};

// The seam slots are process-wide and the held-lock table is thread-local;
// serialize the tests that use them.
static TEST_LOCK: Mutex<()> = Mutex::new(());
static INSTALL: Once = Once::new();

fn guard() -> std::sync::MutexGuard<'static, ()> {
    let g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    INSTALL.call_once(install_seams);
    g
}

// ---- process-wide test fakes for the genuine externals --------------------

#[derive(Clone, Copy, Default)]
struct FakeProc {
    lw_waiting: LWLockWaitState,
    lw_wait_mode: LWLockMode,
    lw_wait_link: proclist_node,
    sem_count: i32,
}

thread_local! {
    static PROCS: RefCell<Vec<FakeProc>> = const { RefCell::new(Vec::new()) };
    static MY_PROC: Cell<i32> = const { Cell::new(INVALID_PROC_NUMBER) };
    static SEM_UNLOCKS: RefCell<Vec<i32>> = const { RefCell::new(Vec::new()) };
}

static UNDER_POSTMASTER: AtomicBool = AtomicBool::new(false);
static SHMEM_REQUEST_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

fn install_seams() {
    shmem::add_size::set(|a, b| {
        a.checked_add(b)
            .ok_or_else(|| PgError::error("requested shared memory size overflows size_t"))
    });
    shmem::mul_size::set(|a, b| {
        a.checked_mul(b)
            .ok_or_else(|| PgError::error("requested shared memory size overflows size_t"))
    });
    shmem::shmem_lock_acquire::set(|| {});
    shmem::shmem_lock_release::set(|| {});

    globals::is_under_postmaster::set(|| UNDER_POSTMASTER.load(O::SeqCst));
    globals::hold_interrupts::set(|| {});
    globals::resume_interrupts::set(|| {});
    globals::my_proc_number::set(|| MY_PROC.with(|p| p.get()));

    miscinit::process_shmem_requests_in_progress::set(|| SHMEM_REQUEST_IN_PROGRESS.load(O::SeqCst));

    waitevent::pgstat_report_wait_start::set(|_| {});
    waitevent::pgstat_report_wait_end::set(|| {});

    proc_s::proc_lw_waiting::set(|procno| PROCS.with(|p| p.borrow()[procno as usize].lw_waiting));
    proc_s::set_proc_lw_waiting::set(|procno, state| {
        PROCS.with(|p| p.borrow_mut()[procno as usize].lw_waiting = state)
    });
    proc_s::proc_lw_wait_mode::set(|procno| {
        PROCS.with(|p| p.borrow()[procno as usize].lw_wait_mode)
    });
    proc_s::set_proc_lw_wait_mode::set(|procno, mode| {
        PROCS.with(|p| p.borrow_mut()[procno as usize].lw_wait_mode = mode)
    });
    proc_s::proc_lw_wait_link::set(|procno| {
        PROCS.with(|p| p.borrow()[procno as usize].lw_wait_link)
    });
    proc_s::set_proc_lw_wait_link::set(|procno, node| {
        PROCS.with(|p| p.borrow_mut()[procno as usize].lw_wait_link = node)
    });
    proc_s::pg_semaphore_lock::set(|procno| {
        // Single-threaded test: the sem must already be posted.
        PROCS.with(|p| p.borrow_mut()[procno as usize].sem_count -= 1)
    });
    proc_s::pg_semaphore_unlock::set(|procno| {
        PROCS.with(|p| p.borrow_mut()[procno as usize].sem_count += 1);
        SEM_UNLOCKS.with(|s| s.borrow_mut().push(procno));
    });
}

fn reset_world(n_procs: usize, my: i32) {
    LWLOCK_COUNTER.store(LWTRANCHE_FIRST_USER_DEFINED, O::SeqCst);
    UNDER_POSTMASTER.store(false, O::SeqCst);
    SHMEM_REQUEST_IN_PROGRESS.store(true, O::SeqCst);
    PROCS.with(|p| *p.borrow_mut() = vec![FakeProc::default(); n_procs]);
    MY_PROC.with(|p| p.set(my));
    SEM_UNLOCKS.with(|s| s.borrow_mut().clear());
    LWLOCK_TRANCHE_NAMES.with(|n| n.borrow_mut().clear());
    NAMED_LWLOCK_TRANCHE_REQUESTS.with(|r| r.borrow_mut().clear());
    // Drain any leftover held locks from a previous test on this thread.
    let _ = LWLockReleaseAll();
}

fn set_my(p: i32) {
    MY_PROC.with(|c| c.set(p));
}

fn waiting(p: i32) -> LWLockWaitState {
    PROCS.with(|procs| procs.borrow()[p as usize].lw_waiting)
}

fn make_lock() -> LWLock {
    let mut lock = LWLock::default();
    LWLockInitialize(&mut lock, LWTRANCHE_BUFFER_MAPPING);
    lock
}

// ---- tests ----------------------------------------------------------------

#[test]
fn initializes_lwlock_like_postgres() {
    let _g = guard();
    let mut lock = LWLock {
        tranche: 0,
        state: pg_atomic_uint32::new(0),
        waiters: proclist_head { head: 0, tail: 0 },
    };
    LWLockInitialize(&mut lock, LWTRANCHE_BUFFER_MAPPING);
    assert_eq!(lock.tranche, LWTRANCHE_BUFFER_MAPPING as uint16);
    assert_eq!(lock.state.read(), LW_FLAG_RELEASE_OK);
    assert_eq!(lock.waiters.head, INVALID_PROC_NUMBER);
    assert_eq!(lock.waiters.tail, INVALID_PROC_NUMBER);
}

#[test]
fn flag_and_value_constants_match_postgres() {
    assert_eq!(LW_VAL_EXCLUSIVE, MAX_BACKENDS + 1);
    assert_eq!((MAX_BACKENDS + 1) & MAX_BACKENDS, 0);
    assert_eq!(LW_VAL_EXCLUSIVE & LW_FLAG_MASK, 0);
    assert_eq!(MAX_BACKENDS & LW_FLAG_MASK, 0);
}

#[test]
fn builtin_tranche_names_match_postgres() {
    let _g = guard();
    assert_eq!(GetLWTrancheName(1), "ShmemIndex");
    assert_eq!(
        GetLWTrancheName(NUM_INDIVIDUAL_LWLOCKS as uint16 - 1),
        "AioWorkerSubmissionQueue"
    );
    assert_eq!(
        GetLWTrancheName(LWTRANCHE_BUFFER_MAPPING as uint16),
        "BufferMapping"
    );
    assert_eq!(
        GetLWTrancheName(LWTRANCHE_LOCK_MANAGER as uint16),
        "LockManager"
    );
    assert_eq!(
        GetLWTrancheName(LWTRANCHE_PREDICATE_LOCK_MANAGER as uint16),
        "PredicateLockManager"
    );
    assert_eq!(
        GetLWTrancheName(LWTRANCHE_FIRST_USER_DEFINED as uint16 - 1),
        "AioUringCompletion"
    );
    // A builtin gap (removed lwlocklist.h id, NULL in C) maps to "unknown".
    assert_eq!(GetLWTrancheName(0), "unknown");
    assert_eq!(GetLWTrancheName(10), "unknown");
}

#[test]
fn dynamic_tranches_default_to_extension_until_registered() {
    let _g = guard();
    reset_world(0, INVALID_PROC_NUMBER);

    let tranche_id = LWLockNewTrancheId();
    assert_eq!(tranche_id, LWTRANCHE_FIRST_USER_DEFINED);
    assert_eq!(GetLWTrancheName(tranche_id as uint16), "extension");
    LWLockRegisterTranche(tranche_id, "ExtensionLock").unwrap();
    assert_eq!(GetLWTrancheName(tranche_id as uint16), "ExtensionLock");
    assert_eq!(
        GetLWLockIdentifier(PG_WAIT_LWLOCK, tranche_id as uint16),
        "ExtensionLock"
    );
}

#[test]
fn named_request_size_matches_postgres_formula() {
    let _g = guard();
    reset_world(0, INVALID_PROC_NUMBER);

    RequestNamedLWLockTranche("RequestA", 2).unwrap();

    let size = LWLockShmemSize().unwrap();
    let expected = (NUM_FIXED_LWLOCKS as usize + 2) * core::mem::size_of::<LWLockPadded>()
        + core::mem::size_of::<i32>()
        + LWLOCK_PADDED_SIZE
        + core::mem::size_of::<NamedLWLockTranche>()
        + "RequestA".len()
        + 1;
    assert_eq!(size, expected);
}

#[test]
fn create_lwlocks_initializes_fixed_and_named_tranches() {
    let _g = guard();
    reset_world(0, INVALID_PROC_NUMBER);

    RequestNamedLWLockTranche("RequestA", 2).unwrap();
    let mut table = CreateLWLocks().unwrap();

    assert_eq!(table.locks().len(), NUM_FIXED_LWLOCKS as usize + 2);
    assert_eq!(table.lock(0).unwrap().tranche, 0);
    assert_eq!(
        table
            .lock(BUFFER_MAPPING_LWLOCK_OFFSET as usize)
            .unwrap()
            .tranche,
        LWTRANCHE_BUFFER_MAPPING as uint16
    );
    assert_eq!(
        table
            .lock(LOCK_MANAGER_LWLOCK_OFFSET as usize)
            .unwrap()
            .tranche,
        LWTRANCHE_LOCK_MANAGER as uint16
    );

    let named = GetNamedLWLockTranche(&mut table, "RequestA").unwrap();
    assert_eq!(named.len(), 2);
    for slot in named.iter() {
        assert_eq!(slot.lock.tranche as i32, LWTRANCHE_FIRST_USER_DEFINED);
        assert_eq!(slot.lock.state.read(), LW_FLAG_RELEASE_OK);
    }
    assert_eq!(
        GetLWTrancheName(LWTRANCHE_FIRST_USER_DEFINED as uint16),
        "RequestA"
    );

    let missing = GetNamedLWLockTranche(&mut table, "Missing").unwrap_err();
    assert_eq!(missing.message(), "requested tranche is not registered");
}

#[test]
fn attached_backend_rederives_named_tranche_ranges() {
    let _g = guard();
    reset_world(0, INVALID_PROC_NUMBER);
    UNDER_POSTMASTER.store(true, O::SeqCst);

    RequestNamedLWLockTranche("RequestA", 1).unwrap();
    RequestNamedLWLockTranche("RequestB", 3).unwrap();
    let table = CreateLWLocks().unwrap();

    assert_eq!(table.locks().len(), NUM_FIXED_LWLOCKS as usize + 4);
    assert_eq!(table.named_tranches().len(), 2);
    let b = &table.named_tranches()[1];
    assert_eq!(b.tranche_name, "RequestB");
    assert_eq!(b.tranche_id, LWTRANCHE_FIRST_USER_DEFINED + 1);
    assert_eq!(b.start, NUM_FIXED_LWLOCKS as usize + 1);
    assert_eq!(b.len, 3);
    assert_eq!(
        GetLWTrancheName((LWTRANCHE_FIRST_USER_DEFINED + 1) as uint16),
        "RequestB"
    );
}

#[test]
fn conditional_acquire_and_release_update_state() {
    let _g = guard();
    reset_world(0, INVALID_PROC_NUMBER);

    let mut lock = make_lock();
    assert!(LWLockConditionalAcquire(&mut lock, LW_SHARED).unwrap());
    assert_eq!(lock.state.read() & LW_SHARED_MASK, 1);
    assert!(LWLockConditionalAcquire(&mut lock, LW_SHARED).unwrap());
    assert_eq!(lock.state.read() & LW_SHARED_MASK, 2);
    LWLockRelease(&mut lock).unwrap();
    LWLockRelease(&mut lock).unwrap();
    assert_eq!(lock.state.read() & LW_LOCK_MASK, 0);

    assert!(LWLockConditionalAcquire(&mut lock, LW_EXCLUSIVE).unwrap());
    assert!(!LWLockConditionalAcquire(&mut lock, LW_SHARED).unwrap());
    LWLockRelease(&mut lock).unwrap();
    assert_eq!(lock.state.read() & LW_LOCK_MASK, 0);
}

#[test]
fn acquire_release_track_held_locks_like_postgres() {
    let _g = guard();
    reset_world(0, INVALID_PROC_NUMBER);

    let mut lock = make_lock();
    assert!(LWLockAcquire(&mut lock, LW_EXCLUSIVE).unwrap());
    assert!(LWLockHeldByMe(&lock));
    assert!(LWLockHeldByMeInMode(&lock, LW_EXCLUSIVE));
    assert!(!LWLockHeldByMeInMode(&lock, LW_SHARED));
    LWLockRelease(&mut lock).unwrap();
    assert!(!LWLockHeldByMe(&lock));
}

#[test]
fn release_of_unheld_lock_reports_tranche_name() {
    let _g = guard();
    reset_world(0, INVALID_PROC_NUMBER);

    let mut lock = make_lock();
    let err = LWLockRelease(&mut lock).unwrap_err();
    assert_eq!(err.message(), "lock BufferMapping is not held");
}

#[test]
fn disown_stops_tracking_without_releasing_lock() {
    let _g = guard();
    reset_world(0, INVALID_PROC_NUMBER);

    let mut lock = make_lock();
    assert!(LWLockConditionalAcquire(&mut lock, LW_SHARED).unwrap());
    LWLockDisown(&mut lock).unwrap();
    assert!(!LWLockHeldByMe(&lock));
    assert_eq!(lock.state.read() & LW_SHARED_MASK, 1);
    LWLockReleaseDisowned(&mut lock, LW_SHARED);
    assert_eq!(lock.state.read() & LW_LOCK_MASK, 0);
}

#[test]
fn release_all_releases_held_locks() {
    let _g = guard();
    reset_world(0, INVALID_PROC_NUMBER);

    let mut first = make_lock();
    let mut second = make_lock();
    assert!(LWLockConditionalAcquire(&mut first, LW_SHARED).unwrap());
    assert!(LWLockConditionalAcquire(&mut second, LW_EXCLUSIVE).unwrap());
    LWLockReleaseAll().unwrap();
    assert_eq!(first.state.read() & LW_LOCK_MASK, 0);
    assert_eq!(second.state.read() & LW_LOCK_MASK, 0);
    assert!(!LWLockHeldByMe(&first));
    assert!(!LWLockHeldByMe(&second));
}

#[test]
fn for_each_and_any_held_by_me() {
    let _g = guard();
    reset_world(0, INVALID_PROC_NUMBER);

    let mut padded = vec![LWLockPadded::default(), LWLockPadded::default()];
    LWLockInitialize(&mut padded[0].lock, LWTRANCHE_BUFFER_MAPPING);
    LWLockInitialize(&mut padded[1].lock, LWTRANCHE_BUFFER_MAPPING);

    assert!(!LWLockAnyHeldByMe(&padded));

    assert!(LWLockConditionalAcquire(&mut padded[1].lock, LW_SHARED).unwrap());
    let addr1 = &padded[1].lock as *const LWLock as usize;

    let mut seen: Vec<(usize, LWLockMode)> = Vec::new();
    ForEachLWLockHeldByMe(|addr, mode| seen.push((addr, mode)));
    assert_eq!(seen, vec![(addr1, LW_SHARED)]);

    assert!(LWLockAnyHeldByMe(&padded));

    LWLockRelease(&mut padded[1].lock).unwrap();
}

#[test]
fn release_clear_var_stores_value_before_unlock() {
    let _g = guard();
    reset_world(0, INVALID_PROC_NUMBER);

    let mut lock = make_lock();
    let value = pg_atomic_uint64::new(1);
    assert!(LWLockConditionalAcquire(&mut lock, LW_EXCLUSIVE).unwrap());
    LWLockReleaseClearVar(&mut lock, &value, 42).unwrap();
    assert_eq!(value.read(), 42);
    assert_eq!(lock.state.read() & LW_LOCK_MASK, 0);
}

#[test]
fn queue_self_orders_head_and_tail_by_mode() {
    let _g = guard();
    reset_world(4, 0);

    let lock = make_lock();
    // Three exclusive waiters enqueue at the tail in order 1, 2, 3.
    for p in [1, 2, 3] {
        set_my(p);
        LWLockQueueSelf(&lock, LW_EXCLUSIVE);
    }
    assert_eq!(lock.waiters.head, 1);
    assert_eq!(lock.waiters.tail, 3);
    assert_ne!(lock.state.read() & LW_FLAG_HAS_WAITERS, 0);

    // A LW_WAIT_UNTIL_FREE waiter jumps to the head.
    set_my(0);
    LWLockQueueSelf(&lock, LW_WAIT_UNTIL_FREE);
    assert_eq!(lock.waiters.head, 0);
    assert_eq!(lock.waiters.tail, 3);

    assert_eq!(collect_waiters(&lock.waiters), vec![0, 1, 2, 3]);
}

/// Test-only: snapshot a wait list's pgprocnos head-to-tail via the same
/// `proclist_foreach_modify` traversal the production code uses.
fn collect_waiters(list: &proclist_head) -> Vec<i32> {
    let mut out = Vec::new();
    proclist_foreach_modify(list.head, |cur| {
        out.push(cur);
        ControlFlow::Continue(())
    });
    out
}

#[test]
fn wakeup_wakes_one_exclusive_and_clears_flags() {
    let _g = guard();
    reset_world(4, 0);

    let lock = make_lock();
    for p in [1, 2] {
        set_my(p);
        LWLockQueueSelf(&lock, LW_EXCLUSIVE);
    }

    set_my(0);
    LWLockWakeup(&lock);

    assert_eq!(SEM_UNLOCKS.with(|s| s.borrow().clone()), vec![1]);
    assert_eq!(waiting(1), LW_WS_NOT_WAITING);
    assert_eq!(waiting(2), LW_WS_WAITING); // still queued
    assert_eq!(lock.waiters.head, 2);
    assert_eq!(lock.waiters.tail, 2);
    assert_eq!(lock.state.read() & LW_FLAG_RELEASE_OK, 0);
    assert_ne!(lock.state.read() & LW_FLAG_HAS_WAITERS, 0);
    assert_eq!(lock.state.read() & LW_FLAG_LOCKED, 0);
}

#[test]
fn wakeup_wakes_all_shared_waiters() {
    let _g = guard();
    reset_world(4, 0);

    let lock = make_lock();
    for p in [1, 2, 3] {
        set_my(p);
        LWLockQueueSelf(&lock, LW_SHARED);
    }

    set_my(0);
    LWLockWakeup(&lock);

    assert_eq!(SEM_UNLOCKS.with(|s| s.borrow().clone()), vec![1, 2, 3]);
    assert!(proclist_is_empty(&lock.waiters));
    assert_eq!(lock.state.read() & LW_FLAG_HAS_WAITERS, 0);
    assert_eq!(lock.state.read() & LW_FLAG_RELEASE_OK, 0);
}

#[test]
fn dequeue_self_removes_and_clears_has_waiters() {
    let _g = guard();
    reset_world(2, 1);

    let lock = make_lock();
    LWLockQueueSelf(&lock, LW_EXCLUSIVE);
    assert_ne!(lock.state.read() & LW_FLAG_HAS_WAITERS, 0);

    LWLockDequeueSelf(&lock);
    assert!(proclist_is_empty(&lock.waiters));
    assert_eq!(lock.state.read() & LW_FLAG_HAS_WAITERS, 0);
    assert_eq!(waiting(1), LW_WS_NOT_WAITING);
}

#[test]
fn wait_list_lock_unlock_round_trips_the_flag() {
    let _g = guard();
    reset_world(0, INVALID_PROC_NUMBER);

    let lock = make_lock();
    assert_eq!(lock.state.read() & LW_FLAG_LOCKED, 0);
    LWLockWaitListLock(&lock);
    assert_ne!(lock.state.read() & LW_FLAG_LOCKED, 0);
    LWLockWaitListUnlock(&lock);
    assert_eq!(lock.state.read() & LW_FLAG_LOCKED, 0);
}

#[test]
fn wait_for_var_returns_free_when_unlocked() {
    let _g = guard();
    reset_world(0, INVALID_PROC_NUMBER);

    let mut lock = make_lock();
    let value = pg_atomic_uint64::new(7);
    let mut newval = 0u64;
    // Lock is free (not exclusive): WaitForVar returns true immediately.
    assert!(LWLockWaitForVar(&mut lock, &value, 7, &mut newval).unwrap());
}

#[test]
fn wait_for_var_returns_false_on_value_mismatch() {
    let _g = guard();
    reset_world(1, 0);

    let mut lock = make_lock();
    // Hold exclusively so the slot is not free.
    assert!(LWLockConditionalAcquire(&mut lock, LW_EXCLUSIVE).unwrap());
    let value = pg_atomic_uint64::new(99);
    let mut newval = 0u64;
    // oldval (7) != current (99): no wait, returns false, newval = 99.
    assert!(!LWLockWaitForVar(&mut lock, &value, 7, &mut newval).unwrap());
    assert_eq!(newval, 99);
    LWLockRelease(&mut lock).unwrap();
}

#[test]
fn update_var_wakes_until_free_waiters() {
    let _g = guard();
    reset_world(2, 0);

    let mut lock = make_lock();
    // Hold exclusively (required precondition for LWLockUpdateVar).
    assert!(LWLockConditionalAcquire(&mut lock, LW_EXCLUSIVE).unwrap());

    // Proc 1 queues as LW_WAIT_UNTIL_FREE.
    set_my(1);
    LWLockQueueSelf(&lock, LW_WAIT_UNTIL_FREE);
    set_my(0);

    let value = pg_atomic_uint64::new(0);
    LWLockUpdateVar(&mut lock, &value, 123);

    assert_eq!(value.read(), 123);
    assert_eq!(SEM_UNLOCKS.with(|s| s.borrow().clone()), vec![1]);
    assert_eq!(waiting(1), LW_WS_NOT_WAITING);

    LWLockRelease(&mut lock).unwrap();
}

#[test]
fn too_many_lwlocks_taken_matches_postgres_message() {
    let _g = guard();
    reset_world(0, INVALID_PROC_NUMBER);

    let mut locks: Vec<LWLock> = (0..MAX_SIMUL_LWLOCKS).map(|_| make_lock()).collect();
    for lock in locks.iter_mut() {
        assert!(LWLockConditionalAcquire(lock, LW_SHARED).unwrap());
    }
    let mut one_more = make_lock();
    let err = LWLockAcquire(&mut one_more, LW_SHARED).unwrap_err();
    assert_eq!(err.message(), "too many LWLocks taken");
    let err = LWLockConditionalAcquire(&mut one_more, LW_SHARED).unwrap_err();
    assert_eq!(err.message(), "too many LWLocks taken");
    let err = LWLockAcquireOrWait(&mut one_more, LW_SHARED).unwrap_err();
    assert_eq!(err.message(), "too many LWLocks taken");
    LWLockReleaseAll().unwrap();
}

#[test]
fn acquire_or_wait_acquires_free_lock() {
    let _g = guard();
    reset_world(1, 0);

    let mut lock = make_lock();
    assert!(LWLockAcquireOrWait(&mut lock, LW_EXCLUSIVE).unwrap());
    assert!(LWLockHeldByMe(&lock));
    LWLockRelease(&mut lock).unwrap();
}
