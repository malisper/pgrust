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

// ---------------------------------------------------------------------------
// audit-18.6 remediation lane b032 (backend/postmaster/postmaster): witnesses
// for the C-shape of the postmaster's supervisor paths. Each asserts the
// postmaster.c REL_18_6 behaviour named in its comment.
// ---------------------------------------------------------------------------

mod b032 {
    use super::*;
    use std::cell::RefCell;
    use std::sync::atomic::{AtomicUsize, Ordering as AtOrd};
    use std::sync::Once;
    use types_core::pid_t;
    use types_error::PgError;

    thread_local! {
        static CAPTURED: RefCell<Vec<PgError>> = const { RefCell::new(Vec::new()) };
    }
    fn capture_hook(e: &PgError, _output_to_server: &mut bool) {
        CAPTURED.with(|c| c.borrow_mut().push(e.clone()));
    }
    /// Every report emitted on this thread while `f` runs (the emit_log_hook
    /// is thread-local, so tests never see each other's output).
    fn captured(f: impl FnOnce()) -> Vec<PgError> {
        CAPTURED.with(|c| c.borrow_mut().clear());
        let prev = elog::set_emit_log_hook(Some(capture_hook));
        f();
        elog::set_emit_log_hook(prev);
        CAPTURED.with(|c| c.borrow_mut().drain(..).collect())
    }
    fn messages(logs: &[PgError]) -> Vec<String> {
        logs.iter().map(|e| e.message.clone()).collect()
    }

    static TOUCH_SOCKET_FILES_CALLS: AtomicUsize = AtomicUsize::new(0);
    static REMOVE_SOCKET_FILES_CALLS: AtomicUsize = AtomicUsize::new(0);
    static AUTOVAC_WORKER_FAILED_CALLS: AtomicUsize = AtomicUsize::new(0);
    static SEAMS: Once = Once::new();
    const CRASHED_PID: pid_t = 424_242;
    const CRASHED_ACTIVITY: &str = "SELECT pg_sleep(30)";

    /// Process-global set-once seams the witnesses observe through. Counters
    /// are compared as deltas because tests run concurrently.
    fn install_seams() {
        SEAMS.call_once(|| {
            pqcomm_seams::touch_socket_files::set(|| {
                TOUCH_SOCKET_FILES_CALLS.fetch_add(1, AtOrd::SeqCst);
            });
            pqcomm_seams::remove_socket_files::set(|| {
                REMOVE_SOCKET_FILES_CALLS.fetch_add(1, AtOrd::SeqCst);
            });
            autovacuum_seams::autovac_worker_failed::set(|| {
                AUTOVAC_WORKER_FAILED_CALLS.fetch_add(1, AtOrd::SeqCst);
            });
            backend_status_seams::pgstat_get_crashed_backend_activity::set(|pid| {
                (pid == CRASHED_PID).then(|| CRASHED_ACTIVITY.to_string())
            });
            postmaster_seams::signal_postmaster_sigusr1::set(|| {});
            // Every pool exhausted: StartChildProcess' "no slot available" arm.
            pmchild_seams::assign_postmaster_child_slot::set(|_btype| None);
        });
    }

    fn fresh_pm() {
        with_pm(|pm| *pm = PostmasterState::new_for_tests());
    }

    #[test]
    fn bgworker_start_time_gate_falls_through_like_c() {
        // postmaster.c:4180-4196 bgworker_should_start_now: PM_RUN falls
        // through to the PM_HOT_STANDBY and PM_RECOVERY/STARTUP/INIT arms, so
        // it admits every start time; PM_HOT_STANDBY admits ConsistentState
        // and PostmasterStart; the recovery states only PostmasterStart; the
        // shutdown/crash states nothing.
        use bgworker::BgWorkerStartTime::*;
        let gate = |state: PMState, st: bgworker::BgWorkerStartTime| {
            with_pm(|pm| pm.pm_state = state);
            statemachine::bgworker_should_start_now(st)
        };
        let all = [PostmasterStart, ConsistentState, RecoveryFinished];
        for st in all {
            assert!(gate(PMState::PM_RUN, st), "PM_RUN must start {st:?}");
        }
        assert!(gate(PMState::PM_HOT_STANDBY, PostmasterStart));
        assert!(gate(PMState::PM_HOT_STANDBY, ConsistentState));
        assert!(!gate(PMState::PM_HOT_STANDBY, RecoveryFinished));
        for s in [PMState::PM_RECOVERY, PMState::PM_STARTUP, PMState::PM_INIT] {
            assert!(gate(s, PostmasterStart), "{} must start PostmasterStart", pmstate_name(s));
            assert!(!gate(s, ConsistentState));
            assert!(!gate(s, RecoveryFinished));
        }
        for s in [
            PMState::PM_STOP_BACKENDS,
            PMState::PM_WAIT_BACKENDS,
            PMState::PM_WAIT_XLOG_SHUTDOWN,
            PMState::PM_WAIT_XLOG_ARCHIVAL,
            PMState::PM_WAIT_IO_WORKERS,
            PMState::PM_WAIT_CHECKPOINTER,
            PMState::PM_WAIT_DEAD_END,
            PMState::PM_NO_CHILDREN,
        ] {
            for st in all {
                assert!(!gate(s, st), "{} must not start {st:?}", pmstate_name(s));
            }
        }
        fresh_pm();
    }

    #[test]
    fn begin_hot_standby_schedules_consistent_state_workers() {
        // postmaster.c:3733-3741 PMSIGNAL_BEGIN_HOT_STANDBY: after
        // UpdatePMState(PM_HOT_STANDBY) and connsAllowed = true, C sets
        // StartWorkerNeeded = true so BgWorkerStart_ConsistentState workers
        // launch on the next ServerLoop pass instead of waiting for an
        // unrelated worker event.
        install_seams();
        pmsignal::PMSignalShmemInit(8);
        fresh_pm();
        with_pm(|pm| {
            pm.pm_state = PMState::PM_RECOVERY;
            pm.shutdown = NoShutdown;
            pm.conns_allowed = false;
            pm.start_worker_needed = false;
        });
        let was_under = init_small::globals::IsUnderPostmaster();
        init_small::globals::SetIsUnderPostmaster(true);
        pmsignal::SendPostmasterSignal(pmsignal::PMSignalReason::PMSIGNAL_BEGIN_HOT_STANDBY);
        init_small::globals::SetIsUnderPostmaster(was_under);
        PENDING_PM_PMSIGNAL.store(true, std::sync::atomic::Ordering::Release);

        process_pm_pmsignal().expect("process_pm_pmsignal");

        let (state, conns_allowed, start_worker_needed) =
            with_pm(|pm| (pm.pm_state, pm.conns_allowed, pm.start_worker_needed));
        assert_eq!(state, PMState::PM_HOT_STANDBY);
        assert!(conns_allowed);
        assert!(
            start_worker_needed,
            "entering hot standby must set StartWorkerNeeded (postmaster.c:3740)"
        );
        fresh_pm();
    }

    #[test]
    fn autovacuum_worker_request_outside_run_state_notifies_launcher() {
        // postmaster.c:4024-4056 StartAutovacuumWorker: when
        // canAcceptConnections(B_AUTOVAC_WORKER) != CAC_OK no worker is
        // started and C falls through to AutoVacWorkerFailed() +
        // avlauncher_needs_signal = true (only while a launcher runs).
        install_seams();
        let launcher =
            PmChild { child_slot: 7, bkend_type: BackendType::AutovacLauncher, pid: 777 };
        fresh_pm();
        with_pm(|pm| {
            pm.pm_state = PMState::PM_STARTUP;
            pm.shutdown = NoShutdown;
            pm.fatal_error = false;
            pm.autovac_launcher = Some(launcher);
            pm.avlauncher_needs_signal = false;
        });
        let before = AUTOVAC_WORKER_FAILED_CALLS.load(AtOrd::SeqCst);
        statemachine::StartAutovacuumWorker();
        assert_eq!(
            AUTOVAC_WORKER_FAILED_CALLS.load(AtOrd::SeqCst) - before,
            1,
            "AutoVacWorkerFailed must be reported when the worker cannot start"
        );
        assert!(with_pm(|pm| pm.avlauncher_needs_signal));

        // No launcher (AutoVacLauncherPMChild == NULL): nothing to notify.
        with_pm(|pm| {
            pm.autovac_launcher = None;
            pm.avlauncher_needs_signal = false;
        });
        let before = AUTOVAC_WORKER_FAILED_CALLS.load(AtOrd::SeqCst);
        statemachine::StartAutovacuumWorker();
        assert_eq!(AUTOVAC_WORKER_FAILED_CALLS.load(AtOrd::SeqCst) - before, 0);
        assert!(!with_pm(|pm| pm.avlauncher_needs_signal));
        fresh_pm();
    }

    #[test]
    fn fork_failure_client_packet_carries_strerror() {
        // postmaster.c:3614-3616 report_fork_failure_to_client: the V2 error
        // packet is "E" + "could not fork new process for connection: " +
        // strerror(errnum) + "\n" + NUL.
        let mut fds = [0i32; 2];
        // SAFETY: plain socketpair on an out array we own.
        let rc = unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
        assert_eq!(rc, 0, "socketpair");
        let client = types_startup::ClientSocket { sock: fds[0], raddr: ip::SockAddr::zeroed() };
        serverloop::report_fork_failure_to_client(&client, libc::ENOMEM);
        let mut buf = vec![0u8; 1024];
        // SAFETY: recv into our own buffer on the peer we still own.
        let n = unsafe { libc::recv(fds[1], buf.as_mut_ptr() as *mut libc::c_void, buf.len(), 0) };
        // SAFETY: closing the peer fd we own (the callee closed fds[0]).
        unsafe { libc::close(fds[1]) };
        assert!(n > 0, "no packet reached the client");
        let expected = format!(
            "Ecould not fork new process for connection: {}\n\0",
            elog::errno::strerror(libc::ENOMEM)
        );
        assert_eq!(
            String::from_utf8_lossy(&buf[..n as usize]),
            expected,
            "client packet must carry ': strerror' (postmaster.c:3614)"
        );
    }

    #[test]
    fn close_server_ports_reports_close_failures_and_unlinks_socket_files() {
        // postmaster.c:1425-1438 CloseServerPorts: a failed closesocket() is
        // LOG "could not close listen socket: %m", then RemoveSocketFiles().
        install_seams();
        fresh_pm();
        with_pm(|pm| pm.listen_sockets = vec![-1]);
        let before = REMOVE_SOCKET_FILES_CALLS.load(AtOrd::SeqCst);
        let logs = captured(|| main_entry::close_server_ports_cb(0, 0));
        assert!(with_pm(|pm| pm.listen_sockets.is_empty()));
        let expected = format!("could not close listen socket: {}", elog::errno::strerror(libc::EBADF));
        assert!(
            logs.iter().any(|e| e.level == LOG && e.message == expected),
            "expected {expected:?}, got {:?}",
            messages(&logs)
        );
        assert_eq!(
            REMOVE_SOCKET_FILES_CALLS.load(AtOrd::SeqCst) - before,
            1,
            "CloseServerPorts must unlink the Unix-socket files (postmaster.c:1438)"
        );
        fresh_pm();
    }

    #[test]
    fn server_loop_touch_tick_touches_socket_files() {
        // postmaster.c:1796-1800: the 58-minute tick touches the socket
        // files (TouchSocketFiles) as well as their lock files.
        install_seams();
        let before = TOUCH_SOCKET_FILES_CALLS.load(AtOrd::SeqCst);
        serverloop::touch_socket_and_lock_files();
        assert_eq!(
            TOUCH_SOCKET_FILES_CALLS.load(AtOrd::SeqCst) - before,
            1,
            "the touch tick must reach pqcomm's TouchSocketFiles"
        );
    }

    #[test]
    fn log_child_exit_matches_c_message_shape() {
        // postmaster.c:2809-2845 LogChildExit: a non-zero status attaches
        // DETAIL "Failed process was running: %s" from
        // pgstat_get_crashed_backend_activity; a signal death carries
        // ": %s" = pg_strsignal(WTERMSIG).
        install_seams();
        let logs = captured(|| log_child_exit_at(LOG, "client backend", CRASHED_PID, 11));
        let e = logs
            .iter()
            .find(|e| e.message.starts_with("client backend (PID"))
            .unwrap_or_else(|| panic!("no LogChildExit report in {:?}", messages(&logs)));
        assert_eq!(
            e.message,
            format!(
                "client backend (PID {CRASHED_PID}) was terminated by signal 11: {}",
                wait_error::pg_strsignal(11)
            )
        );
        assert_eq!(
            e.detail.as_deref(),
            Some(format!("Failed process was running: {CRASHED_ACTIVITY}").as_str())
        );

        let logs = captured(|| log_child_exit_at(LOG, "client backend", CRASHED_PID, 2 << 8));
        let e = logs
            .iter()
            .find(|e| e.message.starts_with("client backend (PID"))
            .unwrap_or_else(|| panic!("no LogChildExit report in {:?}", messages(&logs)));
        assert_eq!(e.message, format!("client backend (PID {CRASHED_PID}) exited with exit code 2"));
        assert_eq!(
            e.detail.as_deref(),
            Some(format!("Failed process was running: {CRASHED_ACTIVITY}").as_str())
        );

        // Exit status 0: no activity lookup, no DETAIL.
        let logs = captured(|| log_child_exit_at(LOG, "client backend", CRASHED_PID, 0));
        let e = logs
            .iter()
            .find(|e| e.message.starts_with("client backend (PID"))
            .unwrap_or_else(|| panic!("no LogChildExit report in {:?}", messages(&logs)));
        assert_eq!(e.message, format!("client backend (PID {CRASHED_PID}) exited with exit code 0"));
        assert_eq!(e.detail, None);
    }

    #[test]
    fn start_child_fork_failure_message_matches_c() {
        // postmaster.c:3966-3968: could not fork "%s" process: %m, with
        // PostmasterChildName(type).
        let logs = captured(|| statemachine::log_fork_failure(BackendType::Startup, libc::EAGAIN));
        let expected = format!(
            "could not fork \"{}\" process: {}",
            launch_backend::postmaster_child_name(BackendType::Startup),
            elog::errno::strerror(libc::EAGAIN)
        );
        assert!(
            logs.iter().any(|e| e.level == LOG && e.message == expected),
            "expected {expected:?}, got {:?}",
            messages(&logs)
        );
    }

    #[test]
    fn start_child_without_a_free_slot_logs_like_c() {
        // postmaster.c:3948-3958 StartChildProcess: no PMChild slot ->
        // LOG (ERRCODE_CONFIGURATION_LIMIT_EXCEEDED) "no slot available for
        // new autovacuum worker process" for B_AUTOVAC_WORKER, else
        // LOG "no postmaster child slot available for aux process".
        install_seams();
        let logs = captured(|| {
            assert!(statemachine::StartChildProcess(BackendType::AutovacWorker).is_none());
        });
        let e = logs
            .iter()
            .find(|e| e.message == "no slot available for new autovacuum worker process")
            .unwrap_or_else(|| panic!("no slot-exhausted LOG in {:?}", messages(&logs)));
        assert_eq!(e.level, LOG);
        assert_eq!(e.sqlstate, types_error::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED);

        let logs = captured(|| {
            assert!(statemachine::StartChildProcess(BackendType::Checkpointer).is_none());
        });
        assert!(
            logs.iter()
                .any(|e| e.level == LOG && e.message == "no postmaster child slot available for aux process"),
            "got {:?}",
            messages(&logs)
        );
    }
}
