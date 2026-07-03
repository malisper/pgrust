use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, Once};

use super::*;

static LOCK_TIMEOUT_INDICATOR: AtomicBool = AtomicBool::new(false);
static STMT_TIMEOUT_INDICATOR: AtomicBool = AtomicBool::new(false);
// Serializes tests that reach the QueryCancel arm: the timeout-indicator
// stubs are process-global while the flags they mimic are per-backend.
static CANCEL_ARM: Mutex<()> = Mutex::new(());

fn install_test_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        lock_seams::abort_strong_lock_acquire::set(|| {});
        lock_seams::get_awaited_lock_hashcode::set(|| None);
        timeout_seams::get_timeout_indicator::set(|id, reset| {
            let slot = match id {
                timeout_seams::LOCK_TIMEOUT => &LOCK_TIMEOUT_INDICATOR,
                timeout_seams::STATEMENT_TIMEOUT => &STMT_TIMEOUT_INDICATOR,
                _ => return false,
            };
            if reset {
                slot.swap(false, Ordering::Relaxed)
            } else {
                slot.load(Ordering::Relaxed)
            }
        });
        timeout_seams::get_timeout_finish_time::set(|_| 0);
    });
}

fn my_latch() {
    use init_small::globals as g;
    if g::MyLatch().is_none() {
        let h = latch::allocate_local_latch();
        latch::InitLatch(h);
        g::SetMyLatch(Some(h));
    }
}

#[test]
fn xact_started_flag_roundtrip() {
    assert!(!xact_started());
    set_xact_started(true);
    assert!(xact_started());
    set_xact_started(false);
}

#[test]
fn is_transaction_exit_stmt_none_is_false() {
    assert!(!simple_query::IsTransactionExitStmt(None));
}

#[test]
fn process_interrupts_noop_when_nothing_pending() {
    init_small::globals::SetInterruptPending(false);
    assert!(check_for_interrupts().is_ok());
}

#[test]
fn process_interrupts_die_is_fatal() {
    install_test_seams();
    init_small::globals::SetInterruptPending(true);
    init_small::globals::SetProcDiePending(true);
    let err = check_for_interrupts().unwrap_err();
    assert_eq!(err.level(), types_error::FATAL);
    assert_eq!(err.sqlstate, types_error::ERRCODE_ADMIN_SHUTDOWN);
    assert!(!init_small::globals::ProcDiePending());
}

#[test]
fn process_interrupts_cancel_is_error_57014() {
    install_test_seams();
    let _serial = CANCEL_ARM.lock().unwrap();
    init_small::globals::SetInterruptPending(true);
    init_small::globals::SetQueryCancelPending(true);
    let err = check_for_interrupts().unwrap_err();
    assert_eq!(err.level(), types_error::ERROR);
    assert_eq!(err.sqlstate, types_error::ERRCODE_QUERY_CANCELED);
    assert!(err.message.contains("user request"));
}

#[test]
fn process_interrupts_lock_timeout_is_55p03() {
    install_test_seams();
    let _serial = CANCEL_ARM.lock().unwrap();
    LOCK_TIMEOUT_INDICATOR.store(true, Ordering::Relaxed);
    init_small::globals::SetInterruptPending(true);
    init_small::globals::SetQueryCancelPending(true);
    let err = check_for_interrupts().unwrap_err();
    assert_eq!(err.sqlstate, types_error::ERRCODE_LOCK_NOT_AVAILABLE);
    assert!(err.message.contains("lock timeout"));
    assert!(!LOCK_TIMEOUT_INDICATOR.load(Ordering::Relaxed)); /* reset consumed it */
}

#[test]
fn process_interrupts_statement_timeout_is_57014() {
    install_test_seams();
    let _serial = CANCEL_ARM.lock().unwrap();
    STMT_TIMEOUT_INDICATOR.store(true, Ordering::Relaxed);
    init_small::globals::SetInterruptPending(true);
    init_small::globals::SetQueryCancelPending(true);
    let err = check_for_interrupts().unwrap_err();
    assert_eq!(err.sqlstate, types_error::ERRCODE_QUERY_CANCELED);
    assert!(err.message.contains("statement timeout"));
}

#[test]
fn process_interrupts_held_off_is_deferred() {
    init_small::globals::HoldInterrupts();
    init_small::globals::SetInterruptPending(true);
    init_small::globals::SetQueryCancelPending(true);
    assert!(check_for_interrupts().is_ok());
    assert!(init_small::globals::QueryCancelPending());
    init_small::globals::SetQueryCancelPending(false);
    init_small::globals::SetInterruptPending(false);
    init_small::globals::ResumeInterrupts();
}

#[test]
fn process_interrupts_cancel_holdoff_rearms() {
    init_small::globals::HoldCancelInterrupts();
    init_small::globals::SetInterruptPending(true);
    init_small::globals::SetQueryCancelPending(true);
    assert!(check_for_interrupts().is_ok());
    assert!(init_small::globals::InterruptPending()); /* re-armed */
    assert!(init_small::globals::QueryCancelPending());
    init_small::globals::SetQueryCancelPending(false);
    init_small::globals::SetInterruptPending(false);
    init_small::globals::ResumeCancelInterrupts();
}

#[test]
fn recovery_conflict_arm_is_loud() {
    install_test_seams();
    HandleRecoveryConflictInterrupt(5);
    assert!(init_small::globals::InterruptPending());
    let outcome = std::panic::catch_unwind(check_for_interrupts);
    let msg = *outcome.unwrap_err().downcast::<String>().unwrap();
    assert!(msg.contains("ProcessRecoveryConflictInterrupts"));
}

#[test]
fn idle_in_transaction_timeout_arm_is_loud() {
    install_test_seams();
    init_small::globals::SetInterruptPending(true);
    init_small::globals::SetIdleInTransactionSessionTimeoutPending(true);
    let payload = std::panic::catch_unwind(check_for_interrupts).unwrap_err();
    let msg = payload
        .downcast_ref::<&str>()
        .copied()
        .unwrap_or_else(|| payload.downcast_ref::<String>().unwrap());
    assert!(msg.contains("IdleInTransactionSessionTimeout"));
    assert!(!init_small::globals::IdleInTransactionSessionTimeoutPending());
}

#[test]
fn die_sets_flags_and_latch() {
    my_latch();
    init_small::globals::HoldInterrupts(); /* keep ProcessInterrupts inert */
    assert!(die().is_ok());
    assert!(init_small::globals::InterruptPending());
    assert!(init_small::globals::ProcDiePending());
    assert_eq!(
        pgstat::database::pgstat_session_end_cause(),
        pgstat::database::SessionEndType::DisconnectKilled
    );
    init_small::globals::SetProcDiePending(false);
    init_small::globals::SetInterruptPending(false);
    init_small::globals::ResumeInterrupts();
}

#[test]
fn statement_cancel_handler_sets_flags() {
    my_latch();
    StatementCancelHandler();
    assert!(init_small::globals::InterruptPending());
    assert!(init_small::globals::QueryCancelPending());
    init_small::globals::SetQueryCancelPending(false);
    init_small::globals::SetInterruptPending(false);
}

#[test]
fn float_exception_handler_is_22p01() {
    let err = FloatExceptionHandler().unwrap_err();
    assert_eq!(err.sqlstate, types_error::ERRCODE_FLOATING_POINT_EXCEPTION);
}

#[test]
fn show_usage_reports_without_reset() {
    // Without ResetUsage, ShowUsage still reports (totals leg).
    let _ = ShowUsage("TEST STATISTICS");
}

fn install_ipc_stubs() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        use init_small::globals as g;
        s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
        s_lock_seams::finish_spin_delay::set(|_| {});
        shmem_seams::mul_size::set(|a, b| Ok(a * b));
        shmem_seams::add_size::set(|a, b| Ok(a + b));
        ipc_seams::on_shmem_exit::set(|_, _| {});
        pg_sema_seams::pg_semaphore_create::set(|_| {});
        g::SetMaxConnections(4);
        g::set_max_worker_processes(2);
        g::SetMaxBackends(4 + 3 + 2 + 2 + types_storage::storage::NUM_SPECIAL_WORKER_PROCS);
        lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
            autovacuum_worker_slots: 3,
            max_wal_senders: 2,
            max_prepared_xacts: 2,
            fastpath_lock_groups_per_backend: 1,
        });
        procsignal::ProcSignalShmemInit();
    });
}

// The shutdown/cancel delivery spine end-to-end: another thread "kills" this
// backend through the procsignal surface; the parked backend wakes, drains,
// and its CFI raises C's exact SQLSTATEs (57014 cancel, 57P01 die).
#[test]
fn thread_signal_sigint_cancels_and_sigterm_terminates() {
    install_test_seams();
    install_ipc_stubs();
    let _serial = CANCEL_ARM.lock().unwrap();

    let (err_tx, err_rx) = std::sync::mpsc::channel();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let backend = std::thread::spawn(move || {
        use init_small::globals as g;
        g::SetMyProcNumber(2);
        g::SetMyProcPid(6161);
        procsignal::ProcSignalInit(&[]).unwrap();
        let h = latch::allocate_local_latch();
        latch::InitLatch(h);
        g::SetMyLatch(Some(h));
        install_thread_signal_handlers();
        ready_tx.send(()).unwrap();

        let proc_latch = &lmgr_proc::GetPGProcByNumber(2).procLatch;
        loop {
            while !proc_latch.is_set() {
                std::thread::yield_now();
            }
            proc_latch.is_set.store(0, Ordering::SeqCst);
            // The WaitLatch wake path: drain dispositions, then CFI.
            if let Err(e) = procsignal::DrainThreadSignals().and_then(|_| check_for_interrupts())
            {
                let fatal = e.level() >= types_error::FATAL;
                err_tx.send(e).unwrap();
                if fatal {
                    return;
                }
            }
        }
    });

    ready_rx.recv().unwrap();
    let timeout = std::time::Duration::from_secs(10);

    assert_eq!(procsignal::SendThreadSignal(6161, libc::SIGINT), 0);
    let err = err_rx.recv_timeout(timeout).expect("backend must surface the cancel");
    assert_eq!(err.level(), types_error::ERROR);
    assert_eq!(err.sqlstate, types_error::ERRCODE_QUERY_CANCELED); /* 57014 */
    assert!(err.message.contains("user request"));

    assert_eq!(procsignal::SendThreadSignal(6161, libc::SIGTERM), 0);
    let err = err_rx.recv_timeout(timeout).expect("backend must surface the die");
    assert_eq!(err.level(), types_error::FATAL);
    assert_eq!(err.sqlstate, types_error::ERRCODE_ADMIN_SHUTDOWN); /* 57P01 */
    backend.join().unwrap();
}
