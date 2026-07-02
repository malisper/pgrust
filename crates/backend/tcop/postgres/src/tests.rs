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
