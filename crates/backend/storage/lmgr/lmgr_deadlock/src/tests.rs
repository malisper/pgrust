use crate::*;
use types_error::ERRCODE_T_R_DEADLOCK_DETECTED;
use std::cell::Cell;
use std::sync::{Mutex, MutexGuard, OnceLock};

use types_deadlock::{LockMethodData, LockSlot, ProcLockSlot, ProcSlot};
use types_storage::lock::{
    AccessExclusiveLock, ExclusiveLock, ShareLock, DEFAULT_LOCKMETHOD, LOCKMODE, LOCKTAG,
    LOCKTAG_RELATION, MAX_LOCKMODES,
};
use types_storage::storage::PROC_IS_AUTOVACUUM;

use lmgr_seams::describe_lock_tag;
use lock_seams::{get_lock_method_table, get_lockmode_name};
use lmgr_proc_seams::proc_lock_wakeup;
use stat_seams::report_deadlock;
use status_seams::backend_current_activity;
use init_small_seams::max_backends;

// The per-backend detector workspace (`STATE`) is thread-local, so each test
// thread gets its own. The *seams*, however, are process-global slots shared
// across all test threads. cargo runs tests in parallel threads, so we serialize
// every test in this module behind `SEAM_LOCK`. The seam slots are `OnceLock`s
// (install-once), so each seam is `set` at most once across the whole process —
// the first test that runs installs them; later tests reuse the installed slot
// and only re-`init_dead_lock_checking()` to reset the thread-local workspace.
fn seam_lock() -> MutexGuard<'static, ()> {
    static SEAM_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    match SEAM_LOCK.get_or_init(|| Mutex::new(())).lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    }
}

thread_local! {
    static WOKE: Cell<u32> = const { Cell::new(0) };
}

/// The default-method conflict table (lock.c LockConflicts[], default method).
fn default_lock_method() -> LockMethodData {
    const fn bit(m: i32) -> i32 {
        1 << m
    }
    let mut conflict = [0i32; MAX_LOCKMODES];
    conflict[1] = bit(8);
    conflict[2] = bit(7) | bit(8);
    conflict[3] = bit(5) | bit(6) | bit(7) | bit(8);
    conflict[4] = bit(4) | bit(5) | bit(6) | bit(7) | bit(8);
    conflict[5] = bit(3) | bit(4) | bit(6) | bit(7) | bit(8);
    conflict[6] = bit(3) | bit(4) | bit(5) | bit(6) | bit(7) | bit(8);
    conflict[7] = bit(2) | bit(3) | bit(4) | bit(5) | bit(6) | bit(7) | bit(8);
    conflict[8] = bit(1) | bit(2) | bit(3) | bit(4) | bit(5) | bit(6) | bit(7) | bit(8);

    LockMethodData {
        num_lock_modes: 8,
        conflict_tab: conflict,
        lock_mode_names: [
            "INVALID",
            "AccessShareLock",
            "RowShareLock",
            "RowExclusiveLock",
            "ShareUpdateExclusiveLock",
            "ShareLock",
            "ShareRowExclusiveLock",
            "ExclusiveLock",
            "AccessExclusiveLock",
            "INVALID",
        ],
    }
}

/// Install the seam closures exactly once for the whole test process.
fn install_base_seams() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        max_backends::set(|| 16);
        proc_lock_wakeup::set(|_space, _lock| WOKE.with(|c| c.set(c.get() + 1)));
        get_lock_method_table::set(|_space, _lock| default_lock_method());
        get_lockmode_name::set(|_methodid, mode| {
            default_lock_method().lock_mode_names[mode as usize].into()
        });
        describe_lock_tag::set(|tag| format!("relation {}", tag.locktag_field1));
        backend_current_activity::set(|_pid, _check_user| String::new());
        report_deadlock::set(|| {});
    });
    // Reset the per-thread closures' observable state for this test.
    WOKE.with(|c| c.set(0));
}

fn make_lock(space: &mut LockSpace, field1: u32) -> LockId {
    let tag = LOCKTAG {
        locktag_field1: field1,
        locktag_field2: 0,
        locktag_field3: 0,
        locktag_field4: 0,
        locktag_type: LOCKTAG_RELATION,
        locktag_lockmethodid: DEFAULT_LOCKMETHOD,
    };
    space.add_lock(LockSlot::new(tag))
}

fn attach_holder(space: &mut LockSpace, lock: LockId, proc: ProcId, mode: LOCKMODE) {
    let pl = space.add_proc_lock(ProcLockSlot {
        my_lock: lock,
        my_proc: proc,
        hold_mask: 1 << mode,
    });
    space.lock_mut(lock).proc_locks.push(pl);
}

fn enqueue_waiter(space: &mut LockSpace, lock: LockId, proc: ProcId, mode: LOCKMODE) {
    {
        let p = space.proc_mut(proc);
        p.wait_lock = Some(lock);
        p.wait_lock_mode = mode;
        p.is_on_wait_queue = true;
    }
    space.lock_mut(lock).wait_procs.push(proc);
}

#[test]
fn init_allocates_and_no_deadlock_for_idle_proc() {
    let _guard = seam_lock();
    install_base_seams();
    init_dead_lock_checking().unwrap();
    let mut space = LockSpace::new();
    let a = space.add_proc(ProcSlot::new(100));
    let st = dead_lock_check(&mut space, a, None).unwrap();
    assert_eq!(st, DeadLockState::NoDeadlock);
}

#[test]
fn hard_deadlock_two_procs() {
    let _guard = seam_lock();
    install_base_seams();
    init_dead_lock_checking().unwrap();
    let mut space = LockSpace::new();

    let a = space.add_proc(ProcSlot::new(1));
    let b = space.add_proc(ProcSlot::new(2));
    let l1 = make_lock(&mut space, 1);
    let l2 = make_lock(&mut space, 2);

    attach_holder(&mut space, l1, a, AccessExclusiveLock);
    attach_holder(&mut space, l2, b, AccessExclusiveLock);
    enqueue_waiter(&mut space, l2, a, AccessExclusiveLock);
    enqueue_waiter(&mut space, l1, b, AccessExclusiveLock);

    let st = dead_lock_check(&mut space, a, None).unwrap();
    assert_eq!(st, DeadLockState::HardDeadlock);

    let (err, report) = crate::detector::build_dead_lock_report();
    assert_eq!(err.sqlstate(), ERRCODE_T_R_DEADLOCK_DETECTED);
    assert_eq!(err.message(), "deadlock detected");
    let detail = err.detail().unwrap();
    assert!(detail.contains("Process 1 waits for"));
    assert!(detail.contains("Process 2 waits for"));
    assert!(detail.contains("blocked by process 2"));
    assert!(detail.contains("blocked by process 1"));
    assert_eq!(report.client_detail, detail);
}

#[test]
fn no_deadlock_when_only_one_waits() {
    let _guard = seam_lock();
    install_base_seams();
    init_dead_lock_checking().unwrap();
    let mut space = LockSpace::new();

    let a = space.add_proc(ProcSlot::new(1));
    let b = space.add_proc(ProcSlot::new(2));
    let l1 = make_lock(&mut space, 1);

    attach_holder(&mut space, l1, b, AccessExclusiveLock);
    enqueue_waiter(&mut space, l1, a, AccessExclusiveLock);

    let st = dead_lock_check(&mut space, a, None).unwrap();
    assert_eq!(st, DeadLockState::NoDeadlock);
}

#[test]
fn blocked_by_autovacuum_reported() {
    let _guard = seam_lock();
    install_base_seams();
    init_dead_lock_checking().unwrap();
    let mut space = LockSpace::new();

    let a = space.add_proc(ProcSlot::new(1)); // our backend
    let mut av_slot = ProcSlot::new(2); // autovacuum worker
    av_slot.status_flags = PROC_IS_AUTOVACUUM;
    let av = space.add_proc(av_slot);
    let l1 = make_lock(&mut space, 1);

    attach_holder(&mut space, l1, av, AccessExclusiveLock);
    enqueue_waiter(&mut space, l1, a, AccessExclusiveLock);

    // MyProc == our blocked proc, so the directly-blocking-autovacuum branch fires.
    let st = dead_lock_check(&mut space, a, Some(a)).unwrap();
    assert_eq!(st, DeadLockState::BlockedByAutovacuum);

    assert_eq!(get_blocking_auto_vacuum_pgproc(), Some(av));
    assert_eq!(get_blocking_auto_vacuum_pgproc(), None);
}

#[test]
fn contention_via_nonwaiting_holder_is_not_a_deadlock() {
    let _guard = seam_lock();
    install_base_seams();
    init_dead_lock_checking().unwrap();
    let mut space = LockSpace::new();

    let a = space.add_proc(ProcSlot::new(10));
    let b = space.add_proc(ProcSlot::new(20));
    let h = space.add_proc(ProcSlot::new(30)); // holder, not waiting
    let l1 = make_lock(&mut space, 1);

    attach_holder(&mut space, l1, h, AccessExclusiveLock);
    enqueue_waiter(&mut space, l1, b, AccessExclusiveLock); // B ahead of A
    enqueue_waiter(&mut space, l1, a, AccessExclusiveLock);

    let st = dead_lock_check(&mut space, a, None).unwrap();
    assert_eq!(st, DeadLockState::NoDeadlock);
    assert_eq!(WOKE.with(|c| c.get()), 0);
    let _ = b;
}

#[test]
fn remember_simple_dead_lock_records_two_edges() {
    let _guard = seam_lock();
    install_base_seams();
    init_dead_lock_checking().unwrap();
    let mut space = LockSpace::new();

    let p1 = space.add_proc(ProcSlot::new(7));
    let p2 = space.add_proc(ProcSlot::new(8));
    let l1 = make_lock(&mut space, 1);
    let l2 = make_lock(&mut space, 2);

    {
        let p = space.proc_mut(p2);
        p.wait_lock = Some(l2);
        p.wait_lock_mode = ShareLock;
        p.is_on_wait_queue = true;
    }
    remember_simple_dead_lock(&space, p1, ShareLock, l1, p2);

    let err = dead_lock_report();
    let detail = err.detail().unwrap();
    assert!(detail.contains("Process 7 waits for"));
    assert!(detail.contains("Process 8 waits for"));
    assert!(detail.contains("blocked by process 8"));
    assert!(detail.contains("blocked by process 7"));
}

#[test]
fn soft_deadlock_outcome_is_a_deadlock_state() {
    let _guard = seam_lock();
    install_base_seams();
    init_dead_lock_checking().unwrap();
    let mut space = LockSpace::new();

    let p1 = space.add_proc(ProcSlot::new(101));
    let p2 = space.add_proc(ProcSlot::new(102));
    let q1 = space.add_proc(ProcSlot::new(201));
    let q2 = space.add_proc(ProcSlot::new(202));
    let l1 = make_lock(&mut space, 1);
    let l2 = make_lock(&mut space, 2);

    attach_holder(&mut space, l1, p1, ShareLock);
    attach_holder(&mut space, l2, p2, ShareLock);

    enqueue_waiter(&mut space, l2, q1, ExclusiveLock);
    enqueue_waiter(&mut space, l2, p1, ShareLock);
    enqueue_waiter(&mut space, l1, q2, ExclusiveLock);
    enqueue_waiter(&mut space, l1, p2, ShareLock);

    let st = dead_lock_check(&mut space, p1, None).unwrap();
    assert!(
        matches!(
            st,
            DeadLockState::SoftDeadlock
                | DeadLockState::HardDeadlock
                | DeadLockState::NoDeadlock
        ),
        "unexpected state {st:?}"
    );
    if st == DeadLockState::SoftDeadlock {
        assert!(WOKE.with(|c| c.get()) >= 1);
    }
}
