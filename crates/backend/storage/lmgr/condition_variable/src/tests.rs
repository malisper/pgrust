use super::*;
use std::sync::{Mutex, Once};
use types_storage::latch::LatchHandle;
use types_storage::storage::NUM_SPECIAL_WORKER_PROCS;

fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn setup() {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        use init_small::globals as g;
        s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
        s_lock_seams::finish_spin_delay::set(|_| {});
        shmem_seams::mul_size::set(|a, b| Ok(a * b));
        shmem_seams::add_size::set(|a, b| Ok(a + b));
        ipc_seams::on_shmem_exit::set(|_, _| {});
        pg_sema_seams::pg_semaphore_create::set(|_| {});
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        postgres_seams::check_for_interrupts::set(|| Ok(()));
        lmgr_proc_seams::proc_latch::set(|p| &lmgr_proc::GetPGProcByNumber(p).procLatch);
        g::SetIsUnderPostmaster(false);
        g::SetMaxConnections(4);
        g::set_max_worker_processes(2);
        g::SetMaxBackends(4 + 3 + 2 + 2 + NUM_SPECIAL_WORKER_PROCS);
        lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
            autovacuum_worker_slots: 3,
            max_wal_senders: 2,
            max_prepared_xacts: 2,
            fastpath_lock_groups_per_backend: 1,
        });
        waiteventset::init_seams();
        latch::init_seams();
        init_seams();
    });
}

fn become_backend(procno: ProcNumber, pid: i32) {
    use init_small::globals as g;
    g::SetMyProcNumber(procno);
    g::SetMyProcPid(pid);
    waiteventset::InitializeWaitEventSupport().unwrap();
    let h = LatchHandle::proc(procno);
    // Tests reuse proc slots across serialized test threads; drop stale owners.
    lmgr_proc::GetPGProcByNumber(procno)
        .procLatch
        .owner_pid
        .store(0, std::sync::atomic::Ordering::SeqCst);
    latch::OwnLatch(h).unwrap();
    g::SetMyLatch(Some(h));
    latch::InitializeLatchWaitSet().unwrap();
}

fn wakeup_len(cv: &ConditionVariable) -> usize {
    let head = cv.wakeup.get();
    let mut n = 0;
    let mut cur = head.head;
    while cur != INVALID_PROC_NUMBER {
        n += 1;
        cur = cv_wait_link(cur).next;
    }
    n
}

fn wakeup_len_locked(cv: &ConditionVariable) -> usize {
    spin_acquire(&cv.mutex);
    let n = wakeup_len(cv);
    cv.mutex.unlock();
    n
}

#[test]
fn cancel_without_prepare_is_false() {
    let _s = serial();
    setup();
    become_backend(0, 7000);
    assert!(!ConditionVariableCancelSleep());
}

#[test]
fn prepare_then_cancel_unsignaled() {
    let _s = serial();
    setup();
    become_backend(0, 7000);
    static CV: ConditionVariable = ConditionVariable::new();
    ConditionVariablePrepareToSleep(&CV);
    assert_eq!(wakeup_len_locked(&CV), 1);
    assert!(!ConditionVariableCancelSleep());
    assert_eq!(wakeup_len_locked(&CV), 0);
}

#[test]
fn signal_wakes_sleeper_who_reports_signaled() {
    let _s = serial();
    setup();
    become_backend(0, 7000);
    static CV: ConditionVariable = ConditionVariable::new();
    static CONDITION: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    CONDITION.store(false, std::sync::atomic::Ordering::SeqCst);
    let sleeper = std::thread::spawn(move || {
        become_backend(2, 7002);
        ConditionVariablePrepareToSleep(&CV);
        while !CONDITION.load(std::sync::atomic::Ordering::SeqCst) {
            ConditionVariableSleep(&CV, 0).unwrap();
        }
        ConditionVariableCancelSleep();
    });
    while wakeup_len_locked(&CV) == 0 {
        std::thread::yield_now();
    }
    CONDITION.store(true, std::sync::atomic::Ordering::SeqCst);
    ConditionVariableSignal(&CV);
    sleeper.join().unwrap();
    assert_eq!(wakeup_len_locked(&CV), 0);
}

#[test]
fn broadcast_wakes_all_sleepers() {
    let _s = serial();
    setup();
    become_backend(0, 7000);
    static CV: ConditionVariable = ConditionVariable::new();
    static WOKEN: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    WOKEN.store(0, std::sync::atomic::Ordering::SeqCst);
    let mut handles = Vec::new();
    for i in 0..2 {
        let procno = 3 + i as ProcNumber;
        handles.push(std::thread::spawn(move || {
            become_backend(procno, 7100 + i as i32);
            ConditionVariablePrepareToSleep(&CV);
            while WOKEN.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                ConditionVariableSleep(&CV, 0).unwrap();
            }
            ConditionVariableCancelSleep()
        }));
    }
    while wakeup_len_locked(&CV) != 2 {
        std::thread::yield_now();
    }
    WOKEN.store(1, std::sync::atomic::Ordering::SeqCst);
    ConditionVariableBroadcast(&CV);
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(wakeup_len_locked(&CV), 0);
}

#[test]
fn timed_sleep_reports_timeout() {
    let _s = serial();
    setup();
    become_backend(0, 7000);
    static CV: ConditionVariable = ConditionVariable::new();
    // A call without a prepared sleep only prepares and returns.
    assert!(!ConditionVariableTimedSleep(&CV, 30, 0).unwrap());
    assert!(ConditionVariableTimedSleep(&CV, 30, 0).unwrap());
    assert!(!ConditionVariableCancelSleep());
}

#[test]
fn seams_delegate_to_owned_storage() {
    let _s = serial();
    setup();
    become_backend(0, 7000);
    condition_variable_seams::checkpointer_cv_prepare_to_sleep::call(
        condition_variable_seams::CheckpointerCv::Start,
    );
    assert_eq!(wakeup_len_locked(&CHECKPOINTER_CVS[0]), 1);
    assert!(!condition_variable_seams::condition_variable_cancel_sleep::call());
    assert_eq!(wakeup_len_locked(&CHECKPOINTER_CVS[0]), 0);
    condition_variable_seams::proc_signal_barrier_cv_broadcast::call(1);
}

// A backend killed while parked leaves its procno in wakeup and (post
// ProcGlobalReset) a zeroed cvWaitLink; the reset arm must clear both sides
// so a fresh backend's park/broadcast cycle works.
fn crash_reset_cycle(cv: &'static ConditionVariable, reset: fn(), pid_base: i32) {
    std::thread::spawn(move || {
        become_backend(1, pid_base);
        ConditionVariablePrepareToSleep(cv);
        // Crash mid-critical-section: die holding the CV spinlock, sleeper
        // still enqueued (no CancelSleep).
        spin_acquire(&cv.mutex);
    })
    .join()
    .unwrap();
    assert_eq!(wakeup_len(cv), 1);

    // ProcGlobalResetAfterCrash zeroes the dead backend's cvWaitLink while
    // its procno still heads the wakeup list; the CV arm must clear the list
    // side too or the next walk is corrupt.
    set_cv_wait_link(1, proclist_node { next: 0, prev: 0 });
    reset();
    assert_eq!(wakeup_len_locked(cv), 0);

    become_backend(2, pid_base + 1);
    ConditionVariablePrepareToSleep(cv);
    assert_eq!(wakeup_len_locked(cv), 1);
    std::thread::spawn(move || {
        become_backend(3, pid_base + 2);
        ConditionVariableBroadcast(cv);
    })
    .join()
    .unwrap();
    assert_eq!(wakeup_len_locked(cv), 0);
    assert!(ConditionVariableCancelSleep());
}

#[test]
fn crash_reset_barrier_cv_survives_killed_sleeper() {
    let _s = serial();
    setup();
    crash_reset_cycle(barrier_cv(0), ProcSignalBarrierCvsResetAfterCrash, 7200);
}

#[test]
fn crash_reset_checkpointer_cv_survives_killed_sleeper() {
    let _s = serial();
    setup();
    crash_reset_cycle(
        checkpointer_cv(condition_variable_seams::CheckpointerCv::Start),
        CheckpointerCvsResetAfterCrash,
        7300,
    );
}
