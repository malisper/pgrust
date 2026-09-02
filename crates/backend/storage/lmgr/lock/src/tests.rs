use super::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering::SeqCst};
use std::sync::{Condvar, Mutex, Once, OnceLock};

use init_small::globals as g;
use types_core::BackendType;
use types_error::PgError;
use types_storage::lock::{
    AccessExclusiveLock, AccessShareLock, DeadLockState, ExclusiveLock, RowExclusiveLock,
    ShareLock, LOCKACQUIRE_ALREADY_HELD, LOCKACQUIRE_NOT_AVAIL, LOCKACQUIRE_OK, LOCKTAG,
};

const TESTDB: u32 = 7777;
const MAX_CONNECTIONS: i32 = 32;
const MAX_WORKER_PROCESSES: i32 = 2;
const MAX_BACKENDS: i32 = MAX_CONNECTIONS + 3 + MAX_WORKER_PROCESSES + 2 + 2;

const CFG: lmgr_proc::ProcGlobalConfig = lmgr_proc::ProcGlobalConfig {
    autovacuum_worker_slots: 3,
    max_wal_senders: 2,
    max_prepared_xacts: 2,
    fastpath_lock_groups_per_backend: 1,
};

static NEXT_PID: AtomicI32 = AtomicI32::new(9100);
// Nonzero so a stale PGPROC.waitStart is distinguishable from a cleared one.
const TEST_WAIT_START: i64 = 1_234_567;

fn semas() -> &'static (Mutex<HashMap<types_core::ProcNumber, i32>>, Condvar) {
    static SEMS: OnceLock<(Mutex<HashMap<types_core::ProcNumber, i32>>, Condvar)> =
        OnceLock::new();
    SEMS.get_or_init(|| (Mutex::new(HashMap::new()), Condvar::new()))
}

fn thread_globals() {
    g::SetMaxConnections(MAX_CONNECTIONS);
    g::set_max_worker_processes(MAX_WORKER_PROCESSES);
    g::SetMaxBackends(MAX_BACKENDS);
    g::SetMyProcPid(NEXT_PID.fetch_add(1, SeqCst));
    g::SetMyDatabaseId(TESTDB);
}

fn my_latch_wait() {
    let procno = lmgr_proc::MyProc().expect("waiting without a proc");
    let latch = &lmgr_proc::GetPGProcByNumber(procno).procLatch;
    while latch.is_set.load(SeqCst) == 0 {
        std::thread::yield_now();
    }
}

fn setup() {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        thread_globals();

        pg_sema_seams::pg_semaphore_create::set(|procno| {
            semas().0.lock().unwrap().insert(procno, 0);
        });
        pg_sema_seams::pg_semaphore_reset::set(|procno| {
            semas().0.lock().unwrap().insert(procno, 0);
        });
        pg_sema_seams::pg_semaphore_lock::set(|procno| {
            let (map, cv) = semas();
            let mut counts = map.lock().unwrap();
            loop {
                let count = counts.get_mut(&procno).unwrap();
                if *count > 0 {
                    *count -= 1;
                    return;
                }
                counts = cv.wait(counts).unwrap();
            }
        });
        pg_sema_seams::pg_semaphore_unlock::set(|procno| {
            let (map, cv) = semas();
            *map.lock().unwrap().get_mut(&procno).unwrap() += 1;
            cv.notify_all();
        });

        postgres_seams::check_for_interrupts::set(|| Ok(()));
        s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
        s_lock_seams::finish_spin_delay::set(|_| {});
        s_lock_seams::set_spins_per_delay::set(|_| {});
        s_lock_seams::update_spins_per_delay::set(|v| v);

        latch_seams::own_latch::set(|latch| latch.owner_pid.store(g::MyProcPid(), SeqCst));
        latch_seams::disown_latch::set(|latch| latch.owner_pid.store(0, SeqCst));
        latch_seams::set_latch::set(|latch| latch.is_set.store(1, SeqCst));
        latch_seams::set_latch_my_latch::set(|| {
            if let Some(procno) = lmgr_proc::MyProc() {
                lmgr_proc::GetPGProcByNumber(procno)
                    .procLatch
                    .is_set
                    .store(1, SeqCst);
            }
        });
        latch_seams::wait_latch_my_latch::set(|_, _, _| {
            my_latch_wait();
            types_storage::waiteventset::WL_LATCH_SET
        });
        latch_seams::reset_latch_my_latch::set(|| {
            let procno = lmgr_proc::MyProc().expect("reset without a proc");
            lmgr_proc::GetPGProcByNumber(procno)
                .procLatch
                .is_set
                .store(0, SeqCst);
        });
        miscinit_seams::switch_to_shared_latch::set(|| {});
        miscinit_seams::switch_back_to_local_latch::set(|| {});
        waitevent_seams::pgstat_set_wait_event_storage::set(|_| {});
        waitevent_seams::pgstat_reset_wait_event_storage::set(|| {});
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        ipc_seams::on_shmem_exit::set(|_, _| {});
        pmsignal_seams::register_postmaster_child_active::set(|| {});

        deadlock_seams::init_dead_lock_checking::set(|| Ok(()));
        deadlock_seams::dead_lock_check::set(|_| DeadLockState::NoDeadLock);
        deadlock_seams::dead_lock_report::set(|| {
            Err(Box::new(PgError::new(
                types_error::ERROR,
                "deadlock detected",
            )))
        });
        deadlock_seams::remember_simple_deadlock::set(|_, _, _, _| {});
        deadlock_seams::get_blocking_autovacuum_procno::set(|| None);

        resowner::init_seams();

        transam_xlog_seams::recovery_in_progress::set(|| false);
        transam_xlog_seams::xlog_standby_info_active::set(|| false);

        timeout_seams::enable_timeout_after::set(|_, _| Ok(()));
        timeout_seams::enable_timeouts::set(|_| Ok(()));
        timeout_seams::disable_timeout::set(|_, _| Ok(()));
        timeout_seams::disable_timeouts::set(|_| {});
        timeout_seams::get_timeout_start_time::set(|_| TEST_WAIT_START);
        timestamp_seams::get_current_timestamp::set(|| 0);

        ps_status_seams::set_ps_display_suffix::set(|_| {});
        ps_status_seams::set_ps_display_remove_suffix::set(|| {});
        elog_seams::ereport_msg::set(|_, _, _| Ok(()));
        lmgr_seams::describe_lock_tag::set(|tag| format!("{tag:?}"));

        shmem_seams::add_size::set(|a, b| Ok(a.checked_add(b).expect("size overflow")));
        shmem_seams::mul_size::set(|a, b| Ok(a.checked_mul(b).expect("size overflow")));
        shmem_seams::shmem_alloc::set(|size| {
            Ok(Box::leak(vec![0u8; size].into_boxed_slice()).as_mut_ptr())
        });

        lwlock::CreateLWLocks(false).unwrap();
        lmgr_proc::init_seams();
        init_seams();
        lmgr_proc::InitProcGlobal(&CFG);
        LockManagerShmemInit(CFG.max_prepared_xacts).unwrap();
    });
}

fn become_backend() {
    setup();
    thread_globals();
    if lmgr_proc::MyProc().is_none() {
        lmgr_proc::InitProcess(BackendType::Backend).unwrap();
        // InitPostgres publishes the database binding in C.
        let procno = lmgr_proc::MyProc().unwrap();
        lmgr_proc::GetPGProcByNumber(procno)
            .databaseId
            .store(TESTDB, SeqCst);
        InitLockManagerAccess();
        let owner =
            resowner::ResourceOwnerCreate(types_resowner::ResourceOwner::NULL, "lock tests")
                .unwrap();
        resowner::SetCurrentResourceOwner(owner);
    }
}

fn rel_tag(relid: u32) -> LOCKTAG {
    LOCKTAG::relation(TESTDB, relid)
}

#[test]
fn conflict_table_matches_lock_h() {
    assert!(DoLockModesConflict(AccessShareLock, AccessExclusiveLock));
    assert!(!DoLockModesConflict(AccessShareLock, ExclusiveLock));
    assert!(DoLockModesConflict(RowExclusiveLock, ShareLock));
    assert!(DoLockModesConflict(ShareLock, RowExclusiveLock));
    assert!(!DoLockModesConflict(ShareLock, ShareLock));
    assert!(DoLockModesConflict(ExclusiveLock, ShareLock));
    assert_eq!(GetLockmodeName(1, AccessShareLock), "AccessShareLock");
    assert_eq!(GetLockmodeName(2, ExclusiveLock), "ExclusiveLock");
    assert_eq!(LOCK_CONFLICTS[AccessExclusiveLock as usize], 0b111111110);
}

#[test]
fn fastpath_acquire_and_release() {
    become_backend();
    let tag = rel_tag(6001);
    assert_eq!(
        LockAcquire(&tag, AccessShareLock, false, false).unwrap(),
        LOCKACQUIRE_OK
    );
    assert!(LockHeldByMe(&tag, AccessShareLock, false));
    // Fast-path grant: nothing ever touched the shared lock table.
    assert_eq!(LockWaiterCount(&tag).unwrap(), 0);

    assert!(LockRelease(&tag, AccessShareLock, false).unwrap());
    assert!(!LockHeldByMe(&tag, AccessShareLock, false));
}

#[test]
fn reacquire_increments_local_count() {
    become_backend();
    let tag = rel_tag(6002);
    assert_eq!(
        LockAcquire(&tag, RowExclusiveLock, false, false).unwrap(),
        LOCKACQUIRE_OK
    );
    assert_eq!(
        LockAcquire(&tag, RowExclusiveLock, false, false).unwrap(),
        LOCKACQUIRE_ALREADY_HELD
    );
    assert!(LockHeldByMe(&tag, AccessShareLock, true));
    assert!(!LockHeldByMe(&tag, AccessExclusiveLock, true));
    assert!(LockRelease(&tag, RowExclusiveLock, false).unwrap());
    assert!(LockHeldByMe(&tag, RowExclusiveLock, false));
    assert!(LockRelease(&tag, RowExclusiveLock, false).unwrap());
    assert!(!LockHeldByMe(&tag, RowExclusiveLock, false));
    // Releasing a lock we no longer hold warns and returns false.
    assert!(!LockRelease(&tag, RowExclusiveLock, false).unwrap());
}

#[test]
fn shared_table_transaction_lock() {
    become_backend();
    let tag = LOCKTAG::transaction(4242);
    assert_eq!(
        LockAcquire(&tag, ExclusiveLock, false, false).unwrap(),
        LOCKACQUIRE_OK
    );
    // Transaction locks are not fast-path eligible: shared LOCK exists.
    assert_eq!(LockWaiterCount(&tag).unwrap(), 1);
    assert!(LockRelease(&tag, ExclusiveLock, false).unwrap());
    assert_eq!(LockWaiterCount(&tag).unwrap(), 0);
}

#[test]
fn dontwait_conflict_returns_not_avail() {
    become_backend();
    let tag = LOCKTAG::transaction(4243);
    assert_eq!(
        LockAcquire(&tag, ExclusiveLock, false, false).unwrap(),
        LOCKACQUIRE_OK
    );
    let t = std::thread::spawn(move || {
        become_backend();
        let r = LockAcquire(&tag, ShareLock, false, true).unwrap();
        assert_eq!(r, LOCKACQUIRE_NOT_AVAIL);
    });
    t.join().unwrap();
    assert!(LockRelease(&tag, ExclusiveLock, false).unwrap());
}

#[test]
fn strong_lock_transfers_fastpath_to_shared_table() {
    become_backend();
    let tag = rel_tag(6003);
    assert_eq!(
        LockAcquire(&tag, AccessShareLock, false, false).unwrap(),
        LOCKACQUIRE_OK
    );
    assert_eq!(LockWaiterCount(&tag).unwrap(), 0);

    let t = std::thread::spawn(move || {
        become_backend();
        // Strong lock: transfers the fast-path lock, then fails on conflict.
        let r = LockAcquire(&tag, AccessExclusiveLock, false, true).unwrap();
        assert_eq!(r, LOCKACQUIRE_NOT_AVAIL);
    });
    t.join().unwrap();

    // Our fast-path lock now lives in the shared table.
    assert_eq!(LockWaiterCount(&tag).unwrap(), 1);
    // Release must take the refind path and still succeed.
    assert!(LockRelease(&tag, AccessShareLock, false).unwrap());
    assert_eq!(LockWaiterCount(&tag).unwrap(), 0);
}

#[test]
fn blocked_acquire_wakes_on_release() {
    become_backend();
    let tag = LOCKTAG::transaction(4244);
    assert_eq!(
        LockAcquire(&tag, ExclusiveLock, false, false).unwrap(),
        LOCKACQUIRE_OK
    );

    let t = std::thread::spawn(move || {
        become_backend();
        let r = LockAcquire(&tag, ShareLock, false, false).unwrap();
        assert_eq!(r, LOCKACQUIRE_OK);
        assert!(LockRelease(&tag, ShareLock, false).unwrap());
    });

    // Wait until the waiter has joined the queue (nRequested == 2), then
    // release; CleanUpLock must wake it.
    while LockWaiterCount(&tag).unwrap() < 2 {
        std::thread::yield_now();
    }
    assert!(LockRelease(&tag, ExclusiveLock, false).unwrap());
    t.join().unwrap();
    assert_eq!(LockWaiterCount(&tag).unwrap(), 0);
}

// upstream 0d3be0501784 (18.4): ProcWakeup cleared the waker's own waitStart
// instead of the awakened process's, leaving the latter stale.
#[test]
fn wakeup_clears_the_woken_procs_wait_start() {
    become_backend();
    let tag = LOCKTAG::transaction(4247);
    assert_eq!(
        LockAcquire(&tag, ExclusiveLock, false, false).unwrap(),
        LOCKACQUIRE_OK
    );
    let waker = lmgr_proc::GetPGProcByNumber(lmgr_proc::MyProc().unwrap());
    const WAKER_STAMP: u64 = 4_242;
    waker.waitStart.write(WAKER_STAMP);

    let waiter_procno = std::sync::Arc::new(AtomicI32::new(-1));
    let publish = std::sync::Arc::clone(&waiter_procno);
    let t = std::thread::spawn(move || {
        become_backend();
        publish.store(lmgr_proc::MyProc().unwrap(), SeqCst);
        assert_eq!(
            LockAcquire(&tag, ShareLock, false, false).unwrap(),
            LOCKACQUIRE_OK
        );
        let mine = lmgr_proc::GetPGProcByNumber(lmgr_proc::MyProc().unwrap())
            .waitStart
            .read();
        assert!(LockRelease(&tag, ShareLock, false).unwrap());
        mine
    });

    // Release only once the waiter is queued AND has stamped waitStart
    // (ProcSleep stamps it after dropping the partition lock).
    loop {
        let procno = waiter_procno.load(SeqCst);
        if procno >= 0
            && LockWaiterCount(&tag).unwrap() >= 2
            && lmgr_proc::GetPGProcByNumber(procno).waitStart.read() == TEST_WAIT_START as u64
        {
            break;
        }
        std::thread::yield_now();
    }
    assert!(LockRelease(&tag, ExclusiveLock, false).unwrap());
    assert_eq!(t.join().unwrap(), 0, "awakened proc keeps a stale waitStart");
    assert_eq!(waker.waitStart.read(), WAKER_STAMP, "waker's waitStart was cleared");
    waker.waitStart.write(0);
}

#[test]
fn lock_release_all_cleans_fastpath_and_shared() {
    let t = std::thread::spawn(|| {
        become_backend();
        let rel = rel_tag(6004);
        let xact = LOCKTAG::transaction(4245);
        assert_eq!(
            LockAcquire(&rel, AccessShareLock, false, false).unwrap(),
            LOCKACQUIRE_OK
        );
        assert_eq!(
            LockAcquire(&xact, ExclusiveLock, false, false).unwrap(),
            LOCKACQUIRE_OK
        );
        assert_eq!(
            LockAcquire(&xact, ExclusiveLock, true, false).unwrap(),
            LOCKACQUIRE_ALREADY_HELD
        );

        // Transaction-end release keeps the session hold.
        LockReleaseAll(1, false).unwrap();
        assert!(!LockHeldByMe(&rel, AccessShareLock, false));
        assert!(LockHeldByMe(&xact, ExclusiveLock, false));
        assert_eq!(LockWaiterCount(&xact).unwrap(), 1);

        LockReleaseAll(1, true).unwrap();
        assert!(!LockHeldByMe(&xact, ExclusiveLock, false));
        assert_eq!(LockWaiterCount(&xact).unwrap(), 0);
    });
    t.join().unwrap();
}

#[test]
fn early_grant_when_ahead_of_conflicting_waiter() {
    // Lock upgrade: we hold AccessShare, another backend waits for
    // AccessExclusive behind it; our RowExclusive request conflicts with the
    // waiter's mode, doesn't conflict with granted locks, and so is granted
    // immediately ahead of it (JoinWaitQueue's special case).
    become_backend();
    let tag = rel_tag(6005);
    assert_eq!(
        LockAcquire(&tag, AccessShareLock, false, false).unwrap(),
        LOCKACQUIRE_OK
    );

    let t = std::thread::spawn(move || {
        become_backend();
        let r = LockAcquire(&tag, AccessExclusiveLock, false, false).unwrap();
        assert_eq!(r, LOCKACQUIRE_OK);
        assert!(LockRelease(&tag, AccessExclusiveLock, false).unwrap());
    });

    while LockWaiterCount(&tag).unwrap() < 2 {
        std::thread::yield_now();
    }

    assert_eq!(
        LockAcquire(&tag, RowExclusiveLock, false, false).unwrap(),
        LOCKACQUIRE_OK
    );
    assert!(LockRelease(&tag, RowExclusiveLock, false).unwrap());
    assert!(LockRelease(&tag, AccessShareLock, false).unwrap());
    t.join().unwrap();
}

#[test]
fn shmem_size_positive() {
    setup();
    assert!(LockManagerShmemSize(CFG.max_prepared_xacts) > 0);
}

// Miri target: the raw intrusive-list kernel, exercised without any globals.
#[test]
fn dlist_kernel_roundtrip() {
    use types_storage::ilist::dlist_head;
    use types_storage::lock::{PROCLOCK, PROCLOCKTAG};

    unsafe fn proclock() -> *mut PROCLOCK {
        Box::into_raw(Box::new(PROCLOCK {
            tag: PROCLOCKTAG::new(std::ptr::null_mut(), 0),
            groupLeader: 0,
            holdMask: 0,
            releaseMask: 0,
            lockLink: Default::default(),
            procLink: Default::default(),
        }))
    }

    unsafe {
        let mut head = dlist_head::new();
        assert!(crate::shared::dlist_is_empty(&head));
        let a = proclock();
        let b = proclock();
        let c = proclock();
        crate::shared::dlist_push_tail(&mut head, &raw mut (*a).lockLink);
        crate::shared::dlist_push_tail(&mut head, &raw mut (*b).lockLink);
        crate::shared::dlist_push_tail(&mut head, &raw mut (*c).lockLink);
        assert!(!crate::shared::dlist_is_empty(&head));

        let mut seen = Vec::new();
        let head_ptr: *mut dlist_head = &mut head;
        let mut cur = (*head_ptr).head.next;
        while let Some(node) = cur {
            cur = (*node.as_ptr()).next;
            seen.push(crate::shared::proclock_from_lock_link(node.as_ptr()));
        }
        assert_eq!(seen, vec![a, b, c]);

        crate::shared::dlist_delete(&mut head, &raw mut (*b).lockLink);
        crate::shared::dlist_delete(&mut head, &raw mut (*a).lockLink);
        crate::shared::dlist_delete(&mut head, &raw mut (*c).lockLink);
        assert!(crate::shared::dlist_is_empty(&head));

        drop(Box::from_raw(a));
        drop(Box::from_raw(b));
        drop(Box::from_raw(c));
    }
}

// Regression: the fastpath-lock/sinval memory-ordering contract
// (XPROTO: strong-lock release must publish the releaser's earlier sinval
// hasMessages store to any backend whose fast-path grant observes the
// release).
//
// C barrier contract this encodes (PostgreSQL 18, verbatim):
//   - lock.c:991-997: "LWLockAcquire acts as a memory sequencing point, so
//     it's safe to assume that any strong locker whose increment to
//     FastPathStrongRelationLocks->counts becomes visible after we test it
//     has yet to begin to transfer fast-path locks." The count read itself
//     (lock.c:999) is unlocked; the increment/decrement sit under
//     SpinLockAcquire/SpinLockRelease (lock.c:1837-1841, 1869-1874).
//   - s_lock.h:59-77: TAS() must fence so later loads/stores can't move
//     above the acquisition, and S_UNLOCK() so earlier loads AND stores
//     can't move below the release — full hardware fences on weakly-ordered
//     platforms.
//   - sinvaladt.c:425-437: "Releasing SInvalWriteLock will enforce a full
//     memory barrier, so these (unlocked) [hasMessages=true] changes will be
//     committed to memory before we exit the function."
// So in C, a weak locker that observes the strong-count decrement is
// guaranteed (full barriers on both sides) to also observe the hasMessages
// store the DDL backend made before releasing its strong lock. Rust's
// Acquire/Release Spinlock (types_storage/src/storage.rs: tas = swap(1,
// Acquire), unlock = store(0, Release)) plus a plain SyncCell count read
// gives NO such edge: nothing forbids the count decrement becoming visible
// before the earlier hasMessages store on ARM. The fix pairs SeqCst count
// transitions (fastpath.rs) with SeqCst hasMessages ops (sinval), restoring
// the C guarantee: observing the decrement implies observing every store the
// releasing backend made before it.
//
// The test drives the ported protocol shape directly: a "DDL" thread stores
// a SeqCst flag (standing for sinval's hasMessages) and then decrements the
// strong-lock count; a "weak locker" thread that observes the count reach
// zero must observe the flag. Deterministic under the fixed SeqCst pairing;
// under the old plain-read/Acq-Rel code this assert could fail on
// weakly-ordered hardware (the explore-branch stressor saw 1-4 misses per
// ~3000 probes on Apple Silicon) and the unlocked SyncCell read was a data
// race outright.
#[test]
fn strong_lock_release_publishes_prior_sinval_store() {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;

    setup();

    // A partition no LOCKTAG in this test suite is known to hash into;
    // foreign traffic would only lengthen the count==0 wait, not corrupt it.
    const PART: u32 = 1013;
    const ROUNDS: usize = 500;

    static HAS_MESSAGES: AtomicBool = AtomicBool::new(false);
    static ROUND_START: AtomicUsize = AtomicUsize::new(0);
    static ROUND_DONE: AtomicUsize = AtomicUsize::new(0);

    let reader = std::thread::spawn(|| {
        for i in 1..=ROUNDS {
            while ROUND_START.load(SeqCst) < i {
                std::thread::yield_now();
            }
            // C's unlocked fast-path probe (lock.c:999): once the strong
            // locker's release (count back to zero) is visible ...
            while crate::fastpath::strong_lock_count(PART) != 0 {
                std::thread::yield_now();
            }
            // ... its pre-release invalidation flag must be visible too.
            assert!(
                HAS_MESSAGES.load(SeqCst),
                "round {i}: observed strong-lock release without the \
                 preceding sinval store — fastpath/sinval ordering broken"
            );
            ROUND_DONE.store(i, SeqCst);
        }
    });

    for i in 1..=ROUNDS {
        HAS_MESSAGES.store(false, SeqCst);
        // Strong locker: count up (BeginStrongLockAcquire) ...
        crate::fastpath::increment_strong_lock_count_partition(PART);
        ROUND_START.store(i, SeqCst);
        // ... queue the invalidation (SIInsertDataEntries hasMessages=true,
        // done before the lock release) ...
        HAS_MESSAGES.store(true, SeqCst);
        // ... release the strong lock (count back down).
        crate::fastpath::decrement_strong_lock_count_partition(PART);
        while ROUND_DONE.load(SeqCst) < i {
            std::thread::yield_now();
        }
    }
    reader.join().unwrap();
}
