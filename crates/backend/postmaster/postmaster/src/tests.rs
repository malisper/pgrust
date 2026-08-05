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

// Both shutdown tests drive the same PENDING_PM_* statics; serialize them.
static SHUTDOWN_FLAGS_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn shutdown_signal_handlers_set_most_immediate() {
    use std::sync::atomic::Ordering;
    let _g = SHUTDOWN_FLAGS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
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
    let _g = SHUTDOWN_FLAGS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
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

// ---------------------------------------------------------------------------
// GL-GANGWEDGE-1: the shutdown-stall watchdog predicate.
// ---------------------------------------------------------------------------

/// Every shutdown state the watchdog is responsible for, i.e. every state a
/// fast/smart shutdown can sit in while waiting for a child that has already
/// been told to stop. PM_WAIT_XLOG_SHUTDOWN is excluded on purpose (the
/// shutdown checkpoint does work there).
const WATCHED_STATES: &[PMState] = &[
    PMState::PM_STOP_BACKENDS,
    PMState::PM_WAIT_BACKENDS,
    PMState::PM_WAIT_XLOG_ARCHIVAL,
    PMState::PM_WAIT_IO_WORKERS,
    PMState::PM_WAIT_DEAD_END,
    PMState::PM_WAIT_CHECKPOINTER,
];

fn due(state: PMState, elapsed: i64) -> bool {
    crate::serverloop::shutdown_stall_due(
        FastShutdown,
        false,
        false,
        state,
        1_000,
        1_000 + elapsed,
        PM_SHUTDOWN_STALL_SECS,
    )
}

#[test]
fn stall_watchdog_fires_in_every_shutdown_wait_state() {
    // The two field shapes: PM_WAIT_BACKENDS (disconnect-side sighting) and
    // the post-checkpoint tail (both shutdown-side sightings). Neither may be
    // able to hang forever.
    for &s in WATCHED_STATES {
        assert!(
            due(s, PM_SHUTDOWN_STALL_SECS),
            "watchdog must fire in {} — an unbounded wait there is an outage",
            pmstate_name(s)
        );
        assert!(!due(s, PM_SHUTDOWN_STALL_SECS - 1), "must not fire early in {}", pmstate_name(s));
    }
}

#[test]
fn stall_watchdog_never_bounds_the_shutdown_checkpoint() {
    // A shutdown checkpoint on a large buffer pool legitimately runs for
    // minutes; escalating there would forfeit a checkpoint that was about to
    // succeed.
    assert!(!due(PMState::PM_WAIT_XLOG_SHUTDOWN, 100 * PM_SHUTDOWN_STALL_SECS));
}

#[test]
fn stall_watchdog_ignores_pre_stop_and_terminal_states() {
    // Smart shutdown waits in PM_RUN for idle clients to leave, which is
    // legitimate and unbounded; PM_NO_CHILDREN exits on its own.
    for &s in &[
        PMState::PM_INIT,
        PMState::PM_STARTUP,
        PMState::PM_RECOVERY,
        PMState::PM_HOT_STANDBY,
        PMState::PM_RUN,
        PMState::PM_NO_CHILDREN,
    ] {
        assert!(!due(s, 100 * PM_SHUTDOWN_STALL_SECS), "must not fire in {}", pmstate_name(s));
    }
}

#[test]
fn stall_watchdog_yields_to_the_immediate_ladder_and_fires_once() {
    let long = 100 * PM_SHUTDOWN_STALL_SECS;
    let p = |shutdown, fatal, escalated, since| {
        crate::serverloop::shutdown_stall_due(
            shutdown,
            fatal,
            escalated,
            PMState::PM_WAIT_BACKENDS,
            since,
            1_000 + long,
            PM_SHUTDOWN_STALL_SECS,
        )
    };
    // Immediate shutdown and the crash cycle are already owned by the
    // SIGKILL + FORCED_EXIT_AFTER_LETHAL_SECS ladder this escalates into.
    assert!(!p(ImmediateShutdown, false, false, 1_000));
    assert!(!p(FastShutdown, true, false, 1_000));
    // No shutdown in progress at all.
    assert!(!p(NoShutdown, false, false, 1_000));
    // Fires at most once per shutdown.
    assert!(!p(FastShutdown, false, true, 1_000));
    // Never stamped => no measurement to make.
    assert!(!p(FastShutdown, false, false, 0));
    // Smart shutdown gets the same protection as fast.
    assert!(p(SmartShutdown, false, false, 1_000));
    assert!(p(FastShutdown, false, false, 1_000));
}

#[test]
fn stall_watchdog_bound_zero_restores_the_unbounded_wait() {
    assert!(!crate::serverloop::shutdown_stall_due(
        FastShutdown,
        false,
        false,
        PMState::PM_WAIT_BACKENDS,
        1_000,
        1_000 + 100 * PM_SHUTDOWN_STALL_SECS,
        0,
    ));
}

#[test]
fn stall_early_wake_never_armed_outside_a_shutdown() {
    // Regression: DetermineSleepTime schedules its early stall-watchdog wake
    // via `shutdown_stall_armed`. That gate MUST agree with the watchdog's own
    // arming (`shutdown_stall_due` minus the elapsed-time test) in every state
    // — otherwise the postmaster schedules a wake the watchdog will never
    // honor. The specific outage: in steady-state PM_RUN with no shutdown in
    // progress, `pm_state_since` is stamped at boot, so once uptime exceeds the
    // bound the wake computed a 0 ms sleep and the postmaster busy-spun
    // epoll_pwait(…, 0) at 100% CPU. The gate must be false there.
    let armed = |shutdown, state, since, bound| {
        crate::serverloop::shutdown_stall_armed(shutdown, false, false, state, since, bound)
    };

    // The exact spin scenario: normal PM_RUN, stamped at boot, uptime far past
    // the bound. Must NOT arm.
    assert!(
        !armed(NoShutdown, PMState::PM_RUN, 1_000, PM_SHUTDOWN_STALL_SECS),
        "steady-state PM_RUN must not arm the early wake — this is the 100% CPU spin"
    );

    // No-shutdown must never arm in ANY state, at any uptime.
    for &s in &[
        PMState::PM_INIT,
        PMState::PM_STARTUP,
        PMState::PM_RECOVERY,
        PMState::PM_HOT_STANDBY,
        PMState::PM_RUN,
        PMState::PM_STOP_BACKENDS,
        PMState::PM_WAIT_BACKENDS,
        PMState::PM_NO_CHILDREN,
    ] {
        assert!(
            !armed(NoShutdown, s, 1_000, PM_SHUTDOWN_STALL_SECS),
            "no shutdown in progress must not arm the early wake in {}",
            pmstate_name(s)
        );
    }

    // `armed` is exactly `shutdown_stall_due` with the time test removed: in
    // every watched state, an armed gate + enough elapsed time is due, and a
    // non-armed gate is never due regardless of elapsed time.
    for &s in WATCHED_STATES {
        assert!(armed(FastShutdown, s, 1_000, PM_SHUTDOWN_STALL_SECS));
        assert!(due(s, PM_SHUTDOWN_STALL_SECS));
    }
    assert!(!armed(NoShutdown, PMState::PM_WAIT_BACKENDS, 1_000, PM_SHUTDOWN_STALL_SECS));
    assert!(!due(PMState::PM_RUN, 100 * PM_SHUTDOWN_STALL_SECS));
}

#[test]
fn wedge_marker_is_greppable_and_stable() {
    // Scored runs and the coldopen rig belt grep for this exact token; it is
    // part of the lane's contract with the harness.
    assert_eq!(WEDGE_MARKER, "PGRUST-SHUTDOWN-WEDGE");
}
