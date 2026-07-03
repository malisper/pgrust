use std::sync::atomic::{AtomicI64, Ordering as O};
use std::sync::Once;
use std::time::Duration;

use super::*;

static SETUP: Once = Once::new();
static NOW: AtomicI64 = AtomicI64::new(1_000_000_000);

thread_local! {
    static FIRED: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
}

fn setup_thread(pid: i32) {
    SETUP.call_once(|| {
        timestamp_seams::get_current_timestamp::set(|| NOW.load(O::Relaxed));
    });
    globals::SetMyProcPid(pid);
    globals::SetMyLatch(Some(latch::allocate_local_latch()));
    InitializeTimeouts();
}

fn advance_ms(ms: i64) {
    NOW.fetch_add(ms * 1000, O::Relaxed);
}

fn handler_a() {
    FIRED.with(|f| f.borrow_mut().push("a"));
}

fn handler_b() {
    FIRED.with(|f| f.borrow_mut().push("b"));
}

fn drain_when_posted() {
    // Wait for the timer thread to post, then run the synchronous delivery.
    let posted = POSTED.with(|p| p.borrow().as_ref().map(std::sync::Arc::clone).unwrap());
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !posted.load(O::SeqCst) {
        assert!(std::time::Instant::now() < deadline, "timer thread never fired");
        std::thread::sleep(Duration::from_millis(1));
    }
    ProcessTimeoutInterrupt();
}

#[test]
fn register_enable_fire_after() {
    setup_thread(9001);
    let id = RegisterTimeout(STATEMENT_TIMEOUT, handler_a);
    assert_eq!(id, STATEMENT_TIMEOUT);

    enable_timeout_after(STATEMENT_TIMEOUT, 5);
    assert!(get_timeout_active(STATEMENT_TIMEOUT));
    assert!(!get_timeout_indicator(STATEMENT_TIMEOUT, false));
    assert_eq!(get_timeout_start_time(STATEMENT_TIMEOUT), NOW.load(O::Relaxed));
    assert_eq!(
        get_timeout_finish_time(STATEMENT_TIMEOUT),
        NOW.load(O::Relaxed) + 5000
    );

    advance_ms(10);
    drain_when_posted();

    assert_eq!(FIRED.with(|f| f.borrow().clone()), vec!["a"]);
    assert!(!get_timeout_active(STATEMENT_TIMEOUT));
    assert!(get_timeout_indicator(STATEMENT_TIMEOUT, true));
    assert!(!get_timeout_indicator(STATEMENT_TIMEOUT, false));
    // The drain also set our latch, as C's handler does.
    let latch = globals::MyLatch().unwrap();
    assert_eq!(latch::latch_ref(latch).is_set.load(O::SeqCst), 1);
}

#[test]
fn priority_and_ordering() {
    setup_thread(9002);
    RegisterTimeout(DEADLOCK_TIMEOUT, handler_a);
    RegisterTimeout(LOCK_TIMEOUT, handler_b);

    // Same fin_time: lower TimeoutId (DEADLOCK) sorts first even if enabled
    // second.
    enable_timeouts(&[
        EnableTimeoutParams::After {
            id: LOCK_TIMEOUT,
            delay_ms: 7,
        },
        EnableTimeoutParams::After {
            id: DEADLOCK_TIMEOUT,
            delay_ms: 7,
        },
    ]);
    DATA.with(|d| {
        let data = d.borrow();
        assert_eq!(data.num_active, 2);
        assert_eq!(data.active_timeouts[0], DEADLOCK_TIMEOUT);
        assert_eq!(data.active_timeouts[1], LOCK_TIMEOUT);
    });

    advance_ms(20);
    drain_when_posted();
    assert_eq!(FIRED.with(|f| f.borrow().clone()), vec!["a", "b"]);
}

#[test]
fn disable_and_reschedule() {
    setup_thread(9003);
    RegisterTimeout(DEADLOCK_TIMEOUT, handler_a);
    RegisterTimeout(LOCK_TIMEOUT, handler_b);

    enable_timeout_after(DEADLOCK_TIMEOUT, 5);
    enable_timeout_after(LOCK_TIMEOUT, 50_000);
    disable_timeout(DEADLOCK_TIMEOUT, false);
    assert!(!get_timeout_active(DEADLOCK_TIMEOUT));
    assert!(get_timeout_active(LOCK_TIMEOUT));

    disable_timeouts(&[DisableTimeoutParams {
        id: LOCK_TIMEOUT,
        keep_indicator: true,
    }]);
    assert!(!get_timeout_active(LOCK_TIMEOUT));
    assert_eq!(DATA.with(|d| d.borrow().num_active), 0);

    // Re-enable then wipe everything; indicators kept on request.
    enable_timeout_after(DEADLOCK_TIMEOUT, 5);
    DATA.with(|d| d.borrow_mut().all_timeouts[LOCK_TIMEOUT as usize].indicator = true);
    disable_all_timeouts(true);
    assert_eq!(DATA.with(|d| d.borrow().num_active), 0);
    assert!(get_timeout_indicator(LOCK_TIMEOUT, true));

    reschedule_timeouts();

    // A posted wake with everything disabled is C's spurious SIGALRM: the
    // drain only SetLatches.
    advance_ms(60_000);
    drain_when_posted();
    assert!(FIRED.with(|f| f.borrow().is_empty()));
}

#[test]
fn periodic_reenables_itself() {
    setup_thread(9004);
    RegisterTimeout(IDLE_STATS_UPDATE_TIMEOUT, handler_b);
    let fin = NOW.load(O::Relaxed) + 5000;
    enable_timeout_every(IDLE_STATS_UPDATE_TIMEOUT, fin, 5);

    advance_ms(6);
    drain_when_posted();
    assert_eq!(FIRED.with(|f| f.borrow().clone()), vec!["b"]);
    // Re-enabled for the next interval; enable_timeout cleared the indicator.
    assert!(get_timeout_active(IDLE_STATS_UPDATE_TIMEOUT));
    assert!(!get_timeout_indicator(IDLE_STATS_UPDATE_TIMEOUT, false));
    disable_all_timeouts(false);
}

#[test]
fn timer_post_raises_interrupt_pending() {
    setup_thread(9007);
    RegisterTimeout(STATEMENT_TIMEOUT, handler_a);
    globals::SetInterruptPending(false);

    enable_timeout_after(STATEMENT_TIMEOUT, 5);
    advance_ms(10);
    drain_when_posted();

    // The timer thread raised this backend's InterruptPending alongside the
    // post, so a CPU-bound CHECK_FOR_INTERRUPTS reaches ProcessInterrupts.
    assert!(globals::InterruptPending());
    globals::SetInterruptPending(false);
}

#[test]
fn user_timeout_allocation() {
    setup_thread(9005);
    let first = RegisterTimeout(USER_TIMEOUT, handler_a);
    assert_eq!(first, USER_TIMEOUT);
    let second = RegisterTimeout(USER_TIMEOUT, handler_b);
    assert_eq!(second, USER_TIMEOUT + 1);
}

#[test]
fn seams_installed_once() {
    // init_seams is process-global (seams are set-once); exercise the seam
    // surface from this thread.
    setup_thread(9006);
    init_seams();
    RegisterTimeout(TRANSACTION_TIMEOUT, handler_a);
    timeout_seams::enable_timeout_after::call(TRANSACTION_TIMEOUT, 30_000).unwrap();
    assert!(timeout_seams::get_timeout_active::call(TRANSACTION_TIMEOUT));
    timeout_seams::disable_timeout::call(TRANSACTION_TIMEOUT, false).unwrap();
    assert!(!timeout_seams::get_timeout_active::call(TRANSACTION_TIMEOUT));
    timeout_seams::disable_all_timeouts::call(false).unwrap();
    timeout_seams::reschedule_timeouts::call().unwrap();
    timeout_seams::process_timeout_interrupt::call();
}
