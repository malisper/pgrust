//! Tests for the timeout manager. The outward seams (current time, latch,
//! pqsignal, interrupt holdoff) are installed once with deterministic stubs;
//! a thread-local "now" lets a test advance the clock.

use std::cell::Cell;
use std::sync::Once;

use ::types_timeout::{
    DisableTimeoutParams, EnableTimeoutParams, TimeoutId, TimeoutType, MAX_TIMEOUTS,
};

use super::*;

thread_local! {
    static MOCK_NOW: Cell<TimestampTz> = const { Cell::new(0) };
    static LAST_SCHEDULED_SIGNAL: Cell<bool> = const { Cell::new(false) };
}

static FIRED: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

fn set_now(now: TimestampTz) {
    MOCK_NOW.with(|c| c.set(now));
}

fn install_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        timestamp_seams::get_current_timestamp::set(|| MOCK_NOW.with(|c| c.get()));
        timestamp_seams::timestamp_difference::set(|start, stop| {
            let diff = (stop - start).max(0);
            (diff / 1_000_000, (diff % 1_000_000) as i32)
        });
        latch_seams::set_latch_my_latch::set(|| {});
        init_small_seams::hold_interrupts::set(|| {});
        init_small_seams::resume_interrupts::set(|| {});
        port_pqsignal_seams::pqsignal::set(|_signo, _func| {});
        // The mock clock can drive schedule_alarm to arm a real ~1us SIGALRM
        // timer; ignore the signal so a stray delivery can't kill the test
        // process (we drive handle_sig_alarm directly instead).
        unsafe {
            libc::signal(libc::SIGALRM, libc::SIG_IGN);
        }
    });
}

fn dummy_handler() {}

fn counting_handler() {
    FIRED.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
}

/// Active-list order: ids in fin_time order, ties broken by lower id first.
fn active_order() -> Vec<usize> {
    TIMEOUT_DATA.with(|d| {
        let data = d.borrow();
        (0..data.num_active_timeouts)
            .map(|i| data.active_timeouts[i])
            .collect()
    })
}

#[test]
fn register_then_enable_and_disable() {
    install_seams();
    set_now(1_000_000);
    InitializeTimeouts();
    RegisterTimeout(TimeoutId::STATEMENT_TIMEOUT, dummy_handler);

    assert!(!get_timeout_active(TimeoutId::STATEMENT_TIMEOUT));
    enable_timeout_after(TimeoutId::STATEMENT_TIMEOUT, 100).unwrap();
    assert!(get_timeout_active(TimeoutId::STATEMENT_TIMEOUT));

    // fin_time = now + 100ms in usec.
    assert_eq!(
        get_timeout_finish_time(TimeoutId::STATEMENT_TIMEOUT),
        1_000_000 + 100 * 1000
    );
    assert_eq!(get_timeout_start_time(TimeoutId::STATEMENT_TIMEOUT), 1_000_000);

    disable_timeout(TimeoutId::STATEMENT_TIMEOUT, false);
    assert!(!get_timeout_active(TimeoutId::STATEMENT_TIMEOUT));
    assert_eq!(active_order().len(), 0);
}

#[test]
fn active_list_sorted_by_fin_time_then_priority() {
    install_seams();
    set_now(0);
    InitializeTimeouts();
    RegisterTimeout(TimeoutId::DEADLOCK_TIMEOUT, dummy_handler);
    RegisterTimeout(TimeoutId::LOCK_TIMEOUT, dummy_handler);
    RegisterTimeout(TimeoutId::STATEMENT_TIMEOUT, dummy_handler);

    // STATEMENT fires latest, LOCK and DEADLOCK at the same time -> tie broken
    // by lower id (DEADLOCK=1 before LOCK=2).
    enable_timeout_at(TimeoutId::STATEMENT_TIMEOUT, 5_000).unwrap();
    enable_timeout_at(TimeoutId::LOCK_TIMEOUT, 1_000).unwrap();
    enable_timeout_at(TimeoutId::DEADLOCK_TIMEOUT, 1_000).unwrap();

    assert_eq!(
        active_order(),
        vec![
            TimeoutId::DEADLOCK_TIMEOUT.as_index(),
            TimeoutId::LOCK_TIMEOUT.as_index(),
            TimeoutId::STATEMENT_TIMEOUT.as_index(),
        ]
    );

    // Re-enabling an active timeout reschedules it (moves position).
    enable_timeout_at(TimeoutId::DEADLOCK_TIMEOUT, 9_000).unwrap();
    assert_eq!(
        active_order(),
        vec![
            TimeoutId::LOCK_TIMEOUT.as_index(),
            TimeoutId::STATEMENT_TIMEOUT.as_index(),
            TimeoutId::DEADLOCK_TIMEOUT.as_index(),
        ]
    );
}

#[test]
fn enable_and_disable_multiple() {
    install_seams();
    set_now(0);
    InitializeTimeouts();
    RegisterTimeout(TimeoutId::DEADLOCK_TIMEOUT, dummy_handler);
    RegisterTimeout(TimeoutId::LOCK_TIMEOUT, dummy_handler);

    enable_timeouts(&[
        EnableTimeoutParams {
            id: TimeoutId::DEADLOCK_TIMEOUT,
            r#type: TimeoutType::TMPARAM_AFTER,
            delay_ms: 10,
            fin_time: 0,
        },
        EnableTimeoutParams {
            id: TimeoutId::LOCK_TIMEOUT,
            r#type: TimeoutType::TMPARAM_AT,
            delay_ms: 0,
            fin_time: 4_000,
        },
    ])
    .unwrap();

    assert_eq!(active_order().len(), 2);
    assert!(get_timeout_active(TimeoutId::DEADLOCK_TIMEOUT));
    assert!(get_timeout_active(TimeoutId::LOCK_TIMEOUT));

    disable_timeouts(&[
        DisableTimeoutParams {
            id: TimeoutId::DEADLOCK_TIMEOUT,
            keep_indicator: false,
        },
        DisableTimeoutParams {
            id: TimeoutId::LOCK_TIMEOUT,
            keep_indicator: false,
        },
    ])
    .unwrap();
    assert_eq!(active_order().len(), 0);
}

#[test]
fn disable_all_clears_active_list() {
    install_seams();
    set_now(0);
    InitializeTimeouts();
    RegisterTimeout(TimeoutId::DEADLOCK_TIMEOUT, dummy_handler);
    RegisterTimeout(TimeoutId::LOCK_TIMEOUT, dummy_handler);
    enable_timeout_at(TimeoutId::DEADLOCK_TIMEOUT, 1_000).unwrap();
    enable_timeout_at(TimeoutId::LOCK_TIMEOUT, 2_000).unwrap();
    assert_eq!(active_order().len(), 2);

    disable_all_timeouts(false).unwrap();
    assert_eq!(active_order().len(), 0);
    assert!(!get_timeout_active(TimeoutId::DEADLOCK_TIMEOUT));
    assert!(!get_timeout_active(TimeoutId::LOCK_TIMEOUT));
}

#[test]
fn signal_handler_fires_reached_timeouts() {
    install_seams();
    set_now(0);
    FIRED.store(0, std::sync::atomic::Ordering::SeqCst);
    InitializeTimeouts();
    RegisterTimeout(TimeoutId::DEADLOCK_TIMEOUT, counting_handler);
    enable_timeout_at(TimeoutId::DEADLOCK_TIMEOUT, 1_000).unwrap();

    // Advance past the fin_time, then run the handler body directly.
    set_now(2_000);
    handle_sig_alarm(libc::SIGALRM);

    assert_eq!(FIRED.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(!get_timeout_active(TimeoutId::DEADLOCK_TIMEOUT));
    // The indicator is set, and reading-with-reset clears it.
    assert!(get_timeout_indicator(TimeoutId::DEADLOCK_TIMEOUT, true));
    assert!(!get_timeout_indicator(TimeoutId::DEADLOCK_TIMEOUT, true));
}

#[test]
fn periodic_timeout_reschedules() {
    install_seams();
    set_now(0);
    FIRED.store(0, std::sync::atomic::Ordering::SeqCst);
    InitializeTimeouts();
    RegisterTimeout(TimeoutId::DEADLOCK_TIMEOUT, counting_handler);
    // Every 1ms, first firing at 500us.
    enable_timeout_every(TimeoutId::DEADLOCK_TIMEOUT, 500, 1);

    set_now(600);
    handle_sig_alarm(libc::SIGALRM);
    assert_eq!(FIRED.load(std::sync::atomic::Ordering::SeqCst), 1);
    // Re-armed: still active, next fin = 500 + 1ms = 1500.
    assert!(get_timeout_active(TimeoutId::DEADLOCK_TIMEOUT));
    assert_eq!(get_timeout_finish_time(TimeoutId::DEADLOCK_TIMEOUT), 500 + 1000);
}

#[test]
fn register_user_timeout_allocates_slot() {
    install_seams();
    set_now(0);
    InitializeTimeouts();
    let id = RegisterTimeout(TimeoutId::USER_TIMEOUT, dummy_handler);
    // First free user slot is USER_TIMEOUT itself (index 13).
    assert_eq!(id.as_index(), TimeoutId::USER_TIMEOUT.as_index());
    assert_eq!(MAX_TIMEOUTS, 23);
}
