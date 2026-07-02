use super::*;
use types_core::init::BackendType;

#[test]
fn btmask_shapes_match_c() {
    assert_eq!(BTYPE_MASK_ALL, (1 << 18) - 1);
    assert_eq!(btmask(BackendType::Invalid), 1);
    assert_eq!(btmask(BackendType::Backend), 2);
    let m = btmask_all_except(&[BackendType::Logger]);
    assert!(!btmask_contains(m, BackendType::Logger));
    assert!(btmask_contains(m, BackendType::Backend));
    assert_eq!(m.count_ones(), 17);
}

#[test]
fn pmstate_order_is_load_bearing() {
    assert!(PMState::PM_STARTUP < PMState::PM_STOP_BACKENDS);
    assert!(PMState::PM_RUN < PMState::PM_STOP_BACKENDS);
    assert!(PMState::PM_STOP_BACKENDS < PMState::PM_WAIT_BACKENDS);
    assert!(PMState::PM_WAIT_DEAD_END < PMState::PM_NO_CHILDREN);
    assert_eq!(pmstate_name(PMState::PM_WAIT_XLOG_SHUTDOWN), "PM_WAIT_XLOG_SHUTDOWN");
}

#[test]
fn shutdown_signal_handlers_set_most_immediate() {
    use std::sync::atomic::Ordering;
    handle_pm_shutdown_request_signal(libc::SIGTERM);
    assert!(PENDING_PM_SHUTDOWN_REQUEST.load(Ordering::Acquire));
    assert!(!PENDING_PM_IMMEDIATE_SHUTDOWN_REQUEST.load(Ordering::Acquire));

    handle_pm_shutdown_request_signal(libc::SIGINT);
    assert!(PENDING_PM_FAST_SHUTDOWN_REQUEST.load(Ordering::Acquire));

    handle_pm_shutdown_request_signal(libc::SIGQUIT);
    assert!(PENDING_PM_IMMEDIATE_SHUTDOWN_REQUEST.load(Ordering::Acquire));

    PENDING_PM_SHUTDOWN_REQUEST.store(false, Ordering::Release);
    PENDING_PM_FAST_SHUTDOWN_REQUEST.store(false, Ordering::Release);
    PENDING_PM_IMMEDIATE_SHUTDOWN_REQUEST.store(false, Ordering::Release);
}

#[test]
fn can_accept_connections_matches_c_gates() {
    use types_startup::CacState;
    with_pm(|pm| {
        pm.pm_state = PMState::PM_STARTUP;
        pm.shutdown = NoShutdown;
        pm.fatal_error = false;
        pm.conns_allowed = false;
    });
    assert_eq!(serverloop::canAcceptConnections(BackendType::Backend), CacState::Startup);

    with_pm(|pm| pm.pm_state = PMState::PM_RECOVERY);
    assert_eq!(serverloop::canAcceptConnections(BackendType::Backend), CacState::NotHotStandby);

    with_pm(|pm| {
        pm.pm_state = PMState::PM_RUN;
        pm.conns_allowed = true;
    });
    assert_eq!(serverloop::canAcceptConnections(BackendType::Backend), CacState::Ok);

    // Smart shutdown gates only client backends.
    with_pm(|pm| pm.conns_allowed = false);
    assert_eq!(serverloop::canAcceptConnections(BackendType::Backend), CacState::Shutdown);
    assert_eq!(serverloop::canAcceptConnections(BackendType::AutovacWorker), CacState::Ok);

    with_pm(|pm| {
        pm.pm_state = PMState::PM_STARTUP;
        pm.shutdown = SmartShutdown;
    });
    assert_eq!(serverloop::canAcceptConnections(BackendType::Backend), CacState::Shutdown);

    with_pm(|pm| *pm = PostmasterState::new_for_tests());
}

impl PostmasterState {
    pub(crate) fn new_for_tests() -> Self {
        Self::new()
    }
}

#[test]
fn shutdown_request_reaches_named_pmchild_seam() {
    // Boot-readiness probe: a SIGTERM-shaped request must walk the C sequence
    // and stop at a NAMED uninstalled seam (pmchild count_children), not a
    // mystery. PM_RUN + conns_allowed=false drives the smart-shutdown arm.
    let result = std::panic::catch_unwind(|| {
        with_pm(|pm| {
            pm.pm_state = PMState::PM_RUN;
            pm.shutdown = NoShutdown;
            pm.conns_allowed = true;
        });
        handle_pm_shutdown_request_signal(libc::SIGTERM);
        let _ = statemachine::process_pm_shutdown_request();
    });
    let err = result.expect_err("must stop at pmchild seam");
    let msg = err
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| err.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_default();
    assert!(
        msg.contains("seam not installed") && msg.contains("pmchild"),
        "panic must name pmchild, got: {msg}"
    );
    with_pm(|pm| *pm = PostmasterState::new_for_tests());
    std::sync::atomic::AtomicBool::store(&PENDING_PM_SHUTDOWN_REQUEST, false, std::sync::atomic::Ordering::Release);
}
