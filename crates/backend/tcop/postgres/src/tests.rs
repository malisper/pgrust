use std::sync::atomic::Ordering;
use std::sync::Mutex;

use super::*;

use crate::session_tests::{LOCK_TIMEOUT_INDICATOR, STMT_TIMEOUT_INDICATOR};

// Serializes tests that reach the QueryCancel arm: the timeout-indicator
// stubs are process-global while the flags they mimic are per-backend.
static CANCEL_ARM: Mutex<()> = Mutex::new(());

fn install_test_seams() {
    crate::session_tests::install_shared_stubs();
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
fn recovery_conflict_arm_is_handled() {
    // PORTED (t25 car-10, recovery-t24-merge): ProcessRecoveryConflictInterrupts
    // is real — the arm SERVICES the conflict signal instead of panicking
    // loudly (the pre-port stub assertion this test used to pin). Outside
    // recovery/transaction state (this unit environment) C resolves the
    // conflict as a no-op: no panic, clean return, interrupt consumed.
    install_test_seams();
    HandleRecoveryConflictInterrupt(5);
    assert!(init_small::globals::InterruptPending());
    check_for_interrupts()
        .expect("ported recovery-conflict arm must service the interrupt cleanly");
    assert!(!init_small::globals::InterruptPending());
}

#[test]
fn idle_and_transaction_timeout_arms() {
    install_test_seams();

    // GUC reset to zero between firing and servicing: signal ignored.
    init_small::globals::SetInterruptPending(true);
    init_small::globals::SetIdleInTransactionSessionTimeoutPending(true);
    lmgr_proc::globals::set_IdleInTransactionSessionTimeout(0);
    assert!(check_for_interrupts().is_ok());
    assert!(!init_small::globals::IdleInTransactionSessionTimeoutPending());

    for (set_pending, set_guc, sqlstate, msg) in [
        (
            init_small::globals::SetIdleInTransactionSessionTimeoutPending as fn(bool),
            lmgr_proc::globals::set_IdleInTransactionSessionTimeout as fn(i32),
            types_error::ERRCODE_IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
            "terminating connection due to idle-in-transaction timeout",
        ),
        (
            init_small::globals::SetTransactionTimeoutPending,
            lmgr_proc::globals::set_TransactionTimeout,
            types_error::ERRCODE_TRANSACTION_TIMEOUT,
            "terminating connection due to transaction timeout",
        ),
        (
            init_small::globals::SetIdleSessionTimeoutPending,
            lmgr_proc::globals::set_IdleSessionTimeout,
            types_error::ERRCODE_IDLE_SESSION_TIMEOUT,
            "terminating connection due to idle-session timeout",
        ),
    ] {
        init_small::globals::SetInterruptPending(true);
        set_pending(true);
        set_guc(100);
        let err = check_for_interrupts().unwrap_err();
        assert_eq!(err.level(), types_error::FATAL);
        assert_eq!(err.sqlstate, sqlstate);
        assert_eq!(err.message, msg);
        set_pending(false);
        set_guc(0);
    }
    init_small::globals::SetInterruptPending(false);
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
    crate::session_tests::install_shared_stubs();
    crate::session_tests::install_shared_proc_fixture();
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

// ---- check_log_duration sampling (GL-GUCBATCH-1; postgres.c:2427 parity) ----

fn sampling_gucs() -> simple_query::LogDurationGucs {
    simple_query::LogDurationGucs {
        log_duration: false,
        log_min: -1,
        log_min_sample: -1,
        sample_rate: 1.0,
        xact_is_sampled: false,
    }
}

#[test]
fn log_sampling_boot_defaults_return_zero_without_a_draw() {
    let g = sampling_gucs();
    let (code, msec) =
        simple_query::check_log_duration_impl(false, 5_000_000, &g, || panic!("no draw"));
    assert_eq!(code, 0);
    assert!(msec.is_empty());
}

#[test]
fn log_sampling_rate_zero_never_logs_and_never_draws() {
    let mut g = sampling_gucs();
    g.log_min_sample = 0; // every statement exceeds the sample threshold
    g.sample_rate = 0.0;
    let (code, _) =
        simple_query::check_log_duration_impl(false, 5_000, &g, || panic!("no draw at rate 0"));
    assert_eq!(code, 0);
}

#[test]
fn log_sampling_rate_one_always_logs_and_never_draws() {
    let mut g = sampling_gucs();
    g.log_min_sample = 0;
    g.sample_rate = 1.0;
    let (code, msec) =
        simple_query::check_log_duration_impl(false, 1_234_567, &g, || panic!("no draw at rate 1"));
    assert_eq!(code, 2);
    // C: snprintf "%ld.%03d" of (secs*1000 + msecs, usecs % 1000).
    assert_eq!(msec, "1234.567");
    // Already-logged statements report 1 (log duration only), C's return 1 arm.
    let (code, _) =
        simple_query::check_log_duration_impl(true, 1_234_567, &g, || panic!("no draw at rate 1"));
    assert_eq!(code, 1);
}

#[test]
fn log_sampling_min_duration_sample_gates_the_draw() {
    let mut g = sampling_gucs();
    g.log_min_sample = 100; // ms
    g.sample_rate = 1.0;
    // 50ms: under the sample threshold — no draw, no log.
    let (code, _) =
        simple_query::check_log_duration_impl(false, 50_000, &g, || panic!("under threshold"));
    assert_eq!(code, 0);
    // 150ms: over — rate 1 logs.
    let (code, _) =
        simple_query::check_log_duration_impl(false, 150_000, &g, || panic!("no draw at rate 1"));
    assert_eq!(code, 2);
}

#[test]
fn log_sampling_fractional_rate_statistical_n1000() {
    let mut g = sampling_gucs();
    g.log_min_sample = 0;
    g.sample_rate = 0.25;
    let prng = std::cell::RefCell::new(pg_prng::PgPrng::seeded(0x6763_6261_7463_6831));
    let mut hits = 0;
    for _ in 0..1000 {
        let (code, _) = simple_query::check_log_duration_impl(false, 5_000, &g, || {
            prng.borrow_mut().next_f64()
        });
        if code == 2 {
            hits += 1;
        }
    }
    // Seeded, hence deterministic; the band guards the decision inequality
    // (<= rate) rather than the generator's exact stream.
    assert!((200..=300).contains(&hits), "hits={hits} outside 200..=300 for rate 0.25");
}

#[test]
fn log_sampling_xact_sampled_forces_logging() {
    let mut g = sampling_gucs();
    g.xact_is_sampled = true;
    let (code, msec) =
        simple_query::check_log_duration_impl(false, 2_000, &g, || panic!("no draw"));
    assert_eq!(code, 2);
    assert_eq!(msec, "2.000");
    let (code, _) =
        simple_query::check_log_duration_impl(true, 2_000, &g, || panic!("no draw"));
    assert_eq!(code, 1);
}

#[test]
fn log_sampling_leaves_log_min_duration_statement_intact() {
    let mut g = sampling_gucs();
    g.log_min = 0; // log_min_duration_statement=0: log everything, no sampling
    g.sample_rate = 0.0;
    let (code, _) = simple_query::check_log_duration_impl(false, 10, &g, || panic!("no draw"));
    assert_eq!(code, 2);
}
