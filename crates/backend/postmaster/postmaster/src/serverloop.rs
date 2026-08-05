//! ServerLoop and connection intake: the C loop shape — wait set over the
//! postmaster latch + listen sockets, unconditional pending-flag processing,
//! backend spawn, SIGKILL escalation, lockfile upkeep.

use std::sync::atomic::Ordering;

use types_core::init::BackendType;
use types_core::{PGINVALID_SOCKET, STATUS_ERROR, STATUS_OK};
use types_error::{PgResult, DEBUG2, LOG};
use types_startup::{BackendStartupData, CacState, ClientSocket, StartupData};
use types_storage::waiteventset::{WaitEvent, WL_LATCH_SET, WL_SOCKET_ACCEPT};

use crate::statemachine::{signal_child, ExitPostmaster, StartChildProcess, TerminateChildren};
use crate::{
    btmask_all_except, loc, pmstate_name, report, report_internal, with_pm, PMState, FastShutdown,
    ImmediateShutdown, NoShutdown, FORCED_EXIT_AFTER_LETHAL_SECS, MAXLISTEN,
    SIGKILL_CHILDREN_AFTER_SECS,
};

const SECS_PER_MINUTE: i64 = 60;

pub fn ConfigurePostmasterWaitSet(accept_connections: bool) -> PgResult<()> {
    with_pm(|pm| -> PgResult<()> {
        if let Some(set) = pm.pm_wait_set.take() {
            waiteventset::FreeWaitEventSet(set);
        }
        let n = if accept_connections { 1 + pm.listen_sockets.len() } else { 1 };
        let set = waiteventset::CreateWaitEventSet(n as i32)?;
        waiteventset::AddWaitEventToSet(
            set,
            WL_LATCH_SET,
            PGINVALID_SOCKET,
            init_small::globals::MyLatch(),
            None,
        )?;
        if accept_connections {
            for fd in &pm.listen_sockets {
                waiteventset::AddWaitEventToSet(set, WL_SOCKET_ACCEPT, *fd, None, None)?;
            }
        }
        pm.pm_wait_set = Some(set);
        Ok(())
    })
}

/// DetermineSleepTime (postmaster.c): with crashed restartable workers, sleep
/// just long enough that they restart on schedule.
pub fn DetermineSleepTime() -> i64 {
    let (shutdown, abort_start_time, start_worker_needed, have_crashed_worker) = with_pm(|pm| {
        (pm.shutdown, pm.abort_start_time, pm.start_worker_needed, pm.have_crashed_worker)
    });
    if shutdown > NoShutdown || (!start_worker_needed && !have_crashed_worker) {
        if abort_start_time != 0 {
            let seconds = SIGKILL_CHILDREN_AFTER_SECS - (now_secs() - abort_start_time);
            return (seconds * 1000).max(0);
        }
        let lethal_time = with_pm(|pm| pm.lethal_time);
        if lethal_time != 0 {
            // Forced-exit floor armed: wake in time to fire it.
            let seconds = FORCED_EXIT_AFTER_LETHAL_SECS - (now_secs() - lethal_time);
            return (seconds * 1000).max(0);
        }
        // GL-GANGWEDGE-1: wake in time for the shutdown-stall watchdog. A
        // wedged shutdown produces no events at all, so without this the
        // postmaster would sleep on its latch for the full 60s tick and the
        // bound would be quantized to it. Gate on `shutdown_stall_armed` — the
        // exact predicate the watchdog itself uses — so this early wake is only
        // scheduled when the watchdog can actually fire. In steady-state
        // PM_RUN (no shutdown in progress) this is false and we fall through to
        // the 60s tick; scheduling a wake there computed a 0 ms sleep once
        // uptime passed the bound and spun the postmaster at 100% CPU.
        let stall_bound = crate::pm_shutdown_stall_secs();
        let (fatal_error, escalated, pm_state, state_since) = with_pm(|pm| {
            (pm.fatal_error, pm.stall_escalated, pm.pm_state, pm.pm_state_since)
        });
        if shutdown_stall_armed(shutdown, fatal_error, escalated, pm_state, state_since, stall_bound)
        {
            let seconds = stall_bound - (now_secs() - state_since);
            return (seconds * 1000).max(0).min(60 * 1000);
        }
        return 60 * 1000;
    }

    if start_worker_needed {
        return 0;
    }

    let mut next_wakeup: i64 = 0;
    for idx in bgworker::registered_indexes() {
        if bgworker::rw_crashed_at(idx) == 0 {
            continue;
        }
        if bgworker::rw_restart_time(idx) == bgworker::BGW_NEVER_RESTART
            || bgworker::rw_terminate(idx)
        {
            bgworker::ForgetBackgroundWorker(idx);
            continue;
        }
        let this_wakeup =
            bgworker::rw_crashed_at(idx) + (bgworker::rw_restart_time(idx) as i64) * 1000 * 1000;
        if next_wakeup == 0 || this_wakeup < next_wakeup {
            next_wakeup = this_wakeup;
        }
    }

    if next_wakeup != 0 {
        // Microsecond TimestampTz difference, clamped like C (0 .. 60s), in ms.
        let now = timestamp_seams::get_current_timestamp::call();
        let ms = (next_wakeup - now).max(0) / 1000;
        return ms.min(60 * 1000);
    }

    60 * 1000
}

fn now_secs() -> i64 {
    // DST P2 (contract §1.2): crash-loop window stamps on pg_clock.
    pg_clock::wall_secs()
}

/// GL-GANGWEDGE-1 shutdown-stall predicate — the pure half, unit-pinned.
///
/// True when a NON-immediate shutdown has sat in one shutdown state, without
/// advancing, for at least `bound` seconds. Deliberately excluded:
/// * `bound == 0` — the watchdog is disabled (rig escape hatch).
/// * immediate shutdown / fatal_error — already owned by the SIGKILL +
///   forced-exit ladder, which this predicate escalates INTO.
/// * `escalated` — fires at most once per shutdown.
/// * states before PM_STOP_BACKENDS — nothing has been asked to stop yet, and
///   smart shutdown's legitimate indefinite wait for idle clients to
///   disconnect happens in PM_RUN.
/// * PM_WAIT_XLOG_SHUTDOWN — the shutdown checkpoint is doing work, not
///   waiting on a child to notice a stop request; it may legitimately take
///   minutes on a large buffer pool.
/// * PM_NO_CHILDREN — the ceremony is over; ExitPostmaster runs from there.
/// The stall watchdog's state gate: every condition of `shutdown_stall_due`
/// EXCEPT the elapsed-time test. When this is false the watchdog cannot fire
/// in the current postmaster state, so `DetermineSleepTime` must not schedule
/// an early wake for it either — the two must share exactly this gate. (They
/// diverged once: the wake path checked only `bound`/`state_since`/`escalated`
/// and fired in steady-state PM_RUN, where `pm_state_since` is stamped at boot
/// and `shutdown == NoShutdown`; once uptime passed `bound` the wake computed a
/// 0 ms sleep and the postmaster busy-spun `epoll_pwait(…, 0)` at 100% CPU.)
pub(crate) fn shutdown_stall_armed(
    shutdown: i32,
    fatal_error: bool,
    escalated: bool,
    state: PMState,
    state_since: i64,
    bound: i64,
) -> bool {
    if bound == 0 || escalated || fatal_error || state_since == 0 {
        return false;
    }
    if shutdown <= NoShutdown || shutdown >= ImmediateShutdown {
        return false;
    }
    if state < PMState::PM_STOP_BACKENDS
        || state >= PMState::PM_NO_CHILDREN
        || state == PMState::PM_WAIT_XLOG_SHUTDOWN
    {
        return false;
    }
    true
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn shutdown_stall_due(
    shutdown: i32,
    fatal_error: bool,
    escalated: bool,
    state: PMState,
    state_since: i64,
    now: i64,
    bound: i64,
) -> bool {
    shutdown_stall_armed(shutdown, fatal_error, escalated, state, state_since, bound)
        && (now - state_since) >= bound
}

pub fn ServerLoop() -> PgResult<i32> {
    // M1: spawn the morsel-runtime worker pool at postmaster start. DEFAULT ON
    // since the M5 boarding flip (runtime::runtime_enabled: `PGRUST_RUNTIME=0`
    // is the kill switch, unset/anything-else enables). The pool sizes to
    // available cores (RuntimeConfig::from_env). Together with the default
    // pgrust.parallel_engine=runtime + pgrust.runtime_dop=0(=cores), this is
    // what makes the analytics fast path engage at DOP=cores out of the box
    // for router-covered shapes — no SET, no env var required.
    let _ = launch_backend::rtpool::start_if_enabled();
    // GL-MEMWATCH-1: the process memory watchdog sampler thread. Near-free
    // (a 1s tick off every query path); pgrust.memory_watchdog gates the
    // work per tick, so SIGHUP can arm/disarm without a restart.
    memwatchdog::start();
    // M2 pool-binding: wire the STANDING runtime executor gang (boot
    // captures + spawner install; threads spawn lazily at first
    // engagement). No-op unless PGRUST_RUNTIME=1, with PGRUST_RUNTIME_
    // POOLBIND=0 as the increment kill — the pool's layering above.
    launch_backend::rtgang::install_if_enabled();
    // GL-GANGWEDGE-1: test-only fault injector for the shutdown backstop
    // (PGRUST_TEST_WEDGE_SHM_BUSY_MS). Inert unless the env var is set; routed
    // through launch_backend because tcop/postmaster cannot name parallel
    // (the seam package-cycle rule).
    launch_backend::install_wedge_shm_busy_injection();
    ConfigurePostmasterWaitSet(true)?;
    let mut last_lockfile_recheck_time = now_secs();
    let mut last_touch_time = last_lockfile_recheck_time;

    let mut events = [WaitEvent::default(); MAXLISTEN];

    loop {
        let set = with_pm(|pm| pm.pm_wait_set).expect("pm_wait_set configured");
        let nevents = waiteventset::WaitEventSetWait(
            set,
            DetermineSleepTime(),
            &mut events,
            0, /* postmaster posts no wait_events */
        )?;

        for event in events.iter().take(nevents.max(0) as usize) {
            if event.events & WL_LATCH_SET != 0 {
                if let Some(l) = init_small::globals::MyLatch() {
                    latch::ResetLatch(l);
                }
            }

            if crate::PENDING_PM_SHUTDOWN_REQUEST.load(Ordering::Acquire) {
                crate::statemachine::process_pm_shutdown_request()?;
            }
            if crate::PENDING_PM_RELOAD_REQUEST.load(Ordering::Acquire) {
                crate::process_pm_reload_request()?;
            }
            if crate::PENDING_PM_CHILD_EXIT.load(Ordering::Acquire) {
                crate::process_pm_child_exit()?;
            }
            if crate::PENDING_PM_PMSIGNAL.load(Ordering::Acquire) {
                crate::process_pm_pmsignal()?;
            }

            if event.events & WL_SOCKET_ACCEPT != 0 {
                if let Ok(s) = pqcomm_seams::accept_connection::call(event.fd) {
                    let _ = BackendStartup(s);
                    // The child thread shares the fd table: the socket is
                    // owned by the spawned backend, no postmaster-side close
                    // (C closes its fork copy here).
                }
            }
        }

        LaunchMissingBackgroundProcesses();

        if with_pm(|pm| pm.avlauncher_needs_signal) {
            with_pm(|pm| pm.avlauncher_needs_signal = false);
            let launcher = with_pm(|pm| pm.autovac_launcher);
            if let Some(launcher) = launcher {
                signal_child(&launcher, procsignal::signums::SIGUSR2);
            }
        }

        let now = now_secs();

        // GL-GANGWEDGE-1 SHUTDOWN-STALL WATCHDOG. A fast/smart shutdown that
        // stops advancing through the shutdown states is a WEDGE, not a slow
        // shutdown: every gate past PM_STOP_BACKENDS waits only on children
        // that have already been told to stop. C can wait forever there
        // because its stop vector (a signal to a process) cannot be missed;
        // ours can (see PM_SHUTDOWN_STALL_SECS). Escalate to immediate
        // shutdown — the operator action C assumes — and let the audited kill
        // ladder below finish the job. Loud, bounded, crash-recovery-safe.
        if with_pm(|pm| {
            shutdown_stall_due(
                pm.shutdown,
                pm.fatal_error,
                pm.stall_escalated,
                pm.pm_state,
                pm.pm_state_since,
                now,
                crate::pm_shutdown_stall_secs(),
            )
        }) {
            let state = with_pm(|pm| pm.pm_state);
            let stalled = now - with_pm(|pm| pm.pm_state_since);
            let remaining =
                pmchild_seams::count_children::call(btmask_all_except(&[BackendType::Logger]));
            let gang = if postmaster_seams::rtgang_live::is_installed() {
                postmaster_seams::rtgang_live::call()
            } else {
                0
            };
            // The marker a scored run greps for. One line, unmistakable,
            // carries everything needed to attribute the stall offline.
            report(
                LOG,
                format!(
                    "{}: shutdown made no progress in {} for {stalled}s \
                     ({remaining} child thread(s), {gang} standing runtime executor(s) \
                     still live); escalating to immediate shutdown. A registry-invisible \
                     runtime thread that missed its stop fence is the known cause; the \
                     next start will run crash recovery.",
                    crate::WEDGE_MARKER,
                    pmstate_name(state)
                ),
                1758,
                "ServerLoop",
            );
            with_pm(|pm| pm.stall_escalated = true);
            // Re-enter the shutdown request path as an immediate request
            // rather than open-coding the escalation: that keeps exactly one
            // implementation of "become an immediate shutdown" (SIGQUIT
            // reason, gang fence, TerminateChildren, PM_WAIT_BACKENDS,
            // abort_start_time) and inherits any future fix to it.
            crate::PENDING_PM_IMMEDIATE_SHUTDOWN_REQUEST.store(true, Ordering::Release);
            crate::PENDING_PM_SHUTDOWN_REQUEST.store(true, Ordering::Release);
            crate::statemachine::process_pm_shutdown_request()?;
        }

        if with_pm(|pm| {
            (pm.shutdown >= ImmediateShutdown || pm.fatal_error)
                && pm.abort_start_time != 0
                && (now - pm.abort_start_time) >= SIGKILL_CHILDREN_AFTER_SECS
        }) {
            let send_abort = guc_tables::vars::send_abort_for_kill.read();
            report(
                LOG,
                format!(
                    "issuing {} to recalcitrant children",
                    if send_abort { "SIGABRT" } else { "SIGKILL" }
                ),
                1758,
                "ServerLoop",
            );
            TerminateChildren(if send_abort { procsignal::signums::SIGABRT } else { procsignal::signums::SIGKILL });
            with_pm(|pm| {
                pm.abort_start_time = 0;
                // Arm the forced-exit floor: the thread rendering of SIGKILL
                // (SendThreadKill) lands only at a drain point, so unlike
                // C's, this broadcast can LOSE (GL-DISCONNECT-WEDGE-1).
                pm.lethal_time = now;
            });
        }

        // Forced-exit floor (see FORCED_EXIT_AFTER_LETHAL_SECS): if children
        // survive the kill broadcast past the grace period, the quiescence
        // gates can never pass — a thread wedged off its drain points is
        // unkillable in the thread model. C's immediate shutdown/crash cycle
        // never waits on an unkillable child (process SIGKILL is
        // unconditional); render that guarantee the only way a single
        // process can — exit it. Crash-equivalent by design: immediate
        // shutdown skips the shutdown checkpoint anyway, and the next start
        // runs crash recovery.
        if with_pm(|pm| {
            (pm.shutdown >= ImmediateShutdown || pm.fatal_error)
                && pm.lethal_time != 0
                && (now - pm.lethal_time) >= FORCED_EXIT_AFTER_LETHAL_SECS
        }) {
            let remaining =
                pmchild_seams::count_children::call(btmask_all_except(&[BackendType::Logger]));
            let gang = if postmaster_seams::rtgang_live::is_installed() {
                postmaster_seams::rtgang_live::call()
            } else {
                0
            };
            if remaining == 0 && gang == 0 {
                // Everyone drained after all; let the state machine finish
                // the ceremony normally.
                with_pm(|pm| pm.lethal_time = 0);
            } else {
                report(
                    LOG,
                    format!(
                        "issuing forced postmaster exit: {remaining} child thread(s) \
                         (+{gang} standing runtime executor(s)) survived the kill \
                         broadcast {FORCED_EXIT_AFTER_LETHAL_SECS}s; threads wedged off \
                         their drain points cannot be killed in-process"
                    ),
                    1758,
                    "ServerLoop",
                );
                ExitPostmaster(1);
            }
        }

        if now - last_lockfile_recheck_time >= SECS_PER_MINUTE {
            if !miscinit::RecheckDataDirLockFile()? {
                report(
                    LOG,
                    "performing immediate shutdown because data directory lock file is invalid"
                        .into(),
                    1779,
                    "ServerLoop",
                );
                crate::handle_pm_shutdown_request_signal(procsignal::signums::SIGQUIT);
            }
            last_lockfile_recheck_time = now;
        }

        if now - last_touch_time >= 58 * SECS_PER_MINUTE {
            // TouchSocketFiles rides with the pqcomm socket-half owner.
            miscinit::TouchSocketLockFiles();
            last_touch_time = now;
        }
    }
}

pub fn canAcceptConnections(backend_type: BackendType) -> CacState {
    debug_assert!(
        backend_type == BackendType::Backend || backend_type == BackendType::AutovacWorker
    );

    let (pm_state, shutdown, fatal_error, conns_allowed) = with_pm(|pm| {
        (pm.pm_state, pm.shutdown, pm.fatal_error, pm.conns_allowed)
    });

    if pm_state != PMState::PM_RUN && pm_state != PMState::PM_HOT_STANDBY {
        if shutdown > NoShutdown {
            return CacState::Shutdown;
        } else if !fatal_error && pm_state == PMState::PM_STARTUP {
            return CacState::Startup;
        } else if !fatal_error && pm_state == PMState::PM_RECOVERY {
            return CacState::NotHotStandby;
        } else {
            return CacState::Recovery;
        }
    }

    if !conns_allowed && backend_type == BackendType::Backend {
        return CacState::Shutdown;
    }

    CacState::Ok
}

pub fn BackendStartup(client_sock: ClientSocket) -> i32 {
    let socket_created = timestamp_seams::get_current_timestamp::call();

    let mut cac = canAcceptConnections(BackendType::Backend);
    let mut child: Option<(pmchild_seams::PmChildSlot, BackendType)> = None;
    if cac == CacState::Ok {
        match pmchild_seams::assign_postmaster_child_slot::call(BackendType::Backend) {
            Some(slot) => child = Some((slot, BackendType::Backend)),
            None => cac = CacState::TooMany,
        }
    }
    let (child_slot, bkend_type) = match child {
        Some(c) => c,
        None => match pmchild_seams::alloc_dead_end_child::call() {
            Some(slot) => (slot, BackendType::DeadEndBackend),
            None => {
                report(LOG, "out of memory".into(), 3576, "BackendStartup");
                return STATUS_ERROR;
            }
        },
    };

    let startup_data = StartupData::Backend(BackendStartupData {
        can_accept_connections: cac,
        socket_created,
        fork_started: 0,
    });

    let pid = launch_backend::postmaster_child_launch(
        bkend_type,
        child_slot,
        startup_data,
        Some(client_sock),
    );
    if pid < 0 {
        pmchild_seams::release_postmaster_child_slot::call(child_slot);
        report(LOG, "could not fork new process for connection".into(), 3608, "BackendStartup");
        report_fork_failure_to_client(&client_sock);
        return STATUS_ERROR;
    }

    report_internal(
        DEBUG2,
        format!(
            "forked new {}, pid={} socket={}",
            miscinit::GetBackendTypeDesc(bkend_type),
            pid,
            client_sock.sock
        ),
        3619,
        "BackendStartup",
    );

    pmchild_seams::set_child_pid::call(child_slot, pid);
    STATUS_OK
}

// C's report_fork_failure_to_client, plus the close C gets for free: there
// the postmaster still owns its fork copy of the accepted socket and closes
// it on the way out of BackendStartup. Here the socket was handed to a child
// that never started, so nobody else will ever close it — and an unclosed
// socket is a client that waits forever (GL-FDLIMIT-1).
fn report_fork_failure_to_client(client_sock: &ClientSocket) {
    launch_backend::report_startup_failure_to_client(
        client_sock.sock,
        "could not fork new process for connection",
    );
}

/// maybe_adjust_io_workers: close the gap between running and configured IO
/// workers (io_workers GUC changes and worker deaths both land here).
pub fn maybe_adjust_io_workers() {
    if !aio_core::pgaio_workers_enabled() {
        return;
    }

    // Final shutdown: just waiting for processes to exit.
    if with_pm(|pm| pm.pm_state >= PMState::PM_WAIT_IO_WORKERS) {
        return;
    }
    // No new workers during an immediate shutdown either.
    if with_pm(|pm| pm.shutdown >= crate::ImmediateShutdown) {
        return;
    }
    // Nor in the shutdown phase of a crash restart (but do restart once the
    // cycle is starting up again).
    if with_pm(|pm| pm.fatal_error && pm.pm_state >= PMState::PM_STOP_BACKENDS) {
        return;
    }

    let target = guc_tables::vars::io_workers.read();

    // Not enough running?
    while with_pm(|pm| pm.io_worker_count) < target {
        let free = with_pm(|pm| pm.io_worker_children.iter().position(|c| c.is_none()));
        let Some(i) = free else {
            panic!("could not find a free IO worker slot");
        };
        match StartChildProcess(BackendType::IoWorker) {
            Some(child) => with_pm(|pm| {
                pm.io_worker_children[i] = Some(child);
                pm.io_worker_count += 1;
            }),
            None => break, // try again next time
        }
    }

    // Too many running? Ask the worker in the highest slot to exit.
    if with_pm(|pm| pm.io_worker_count) > target {
        let victim = with_pm(|pm| pm.io_worker_children.iter().rev().flatten().next().copied());
        if let Some(child) = victim {
            crate::statemachine::signal_child(&child, procsignal::signums::SIGUSR2);
        }
    }
}

pub fn LaunchMissingBackgroundProcesses() {
    if with_pm(|pm| pm.syslogger.is_none()) && guc_tables::vars::Logging_collector.read() {
        crate::statemachine::StartSysLogger();
    }

    maybe_adjust_io_workers();

    if with_pm(|pm| {
        matches!(
            pm.pm_state,
            PMState::PM_RUN | PMState::PM_RECOVERY | PMState::PM_HOT_STANDBY | PMState::PM_STARTUP
        )
    }) {
        if with_pm(|pm| pm.checkpointer.is_none()) {
            let c = StartChildProcess(BackendType::Checkpointer);
            with_pm(|pm| pm.checkpointer = c);
        }
        if with_pm(|pm| pm.bgwriter.is_none()) {
            let c = StartChildProcess(BackendType::BgWriter);
            with_pm(|pm| pm.bgwriter = c);
        }
    }

    if with_pm(|pm| pm.walwriter.is_none() && pm.pm_state == PMState::PM_RUN) {
        let c = StartChildProcess(BackendType::WalWriter);
        with_pm(|pm| pm.walwriter = c);
    }

    if !init_small::globals::IsBinaryUpgrade()
        && with_pm(|pm| pm.autovac_launcher.is_none())
        && (autovacuum_seams::autovacuuming_active::call()
            || with_pm(|pm| pm.start_autovac_launcher))
        && with_pm(|pm| pm.pm_state == PMState::PM_RUN)
    {
        let c = StartChildProcess(BackendType::AutovacLauncher);
        with_pm(|pm| {
            pm.autovac_launcher = c;
            if c.is_some() {
                pm.start_autovac_launcher = false;
            }
        });
    }

    let archive_mode = guc_tables::vars::XLogArchiveMode.read();
    if with_pm(|pm| pm.pgarch.is_none())
        && ((archive_mode > 0 && with_pm(|pm| pm.pm_state == PMState::PM_RUN))
            || (archive_mode >= 2
                && with_pm(|pm| {
                    matches!(pm.pm_state, PMState::PM_RECOVERY | PMState::PM_HOT_STANDBY)
                })))
        && pgarch::PgArchCanRestart()
    {
        let c = StartChildProcess(BackendType::Archiver);
        with_pm(|pm| pm.pgarch = c);
    }

    if with_pm(|pm| pm.slotsync_worker.is_none() && pm.pm_state == PMState::PM_HOT_STANDBY)
        && with_pm(|pm| pm.shutdown <= crate::SmartShutdown)
        && guc_tables::vars::sync_replication_slots.read()
        && slotsync::ValidateSlotSyncParams(false).unwrap_or(false)
        && slotsync::SlotSyncWorkerCanRestart()
    {
        let c = StartChildProcess(BackendType::SlotsyncWorker);
        with_pm(|pm| pm.slotsync_worker = c);
    }

    if with_pm(|pm| pm.wal_receiver_requested) {
        if with_pm(|pm| {
            pm.walreceiver.is_none()
                && matches!(
                    pm.pm_state,
                    PMState::PM_STARTUP | PMState::PM_RECOVERY | PMState::PM_HOT_STANDBY
                )
                && pm.shutdown <= crate::SmartShutdown
        }) {
            let c = StartChildProcess(BackendType::WalReceiver);
            with_pm(|pm| {
                pm.walreceiver = c;
                if c.is_some() {
                    pm.wal_receiver_requested = false;
                }
            });
        }
    }

    if guc_tables::vars::summarize_wal.read()
        && with_pm(|pm| {
            pm.walsummarizer.is_none()
                && matches!(pm.pm_state, PMState::PM_RUN | PMState::PM_HOT_STANDBY)
                && pm.shutdown <= crate::SmartShutdown
        })
    {
        let c = StartChildProcess(BackendType::WalSummarizer);
        with_pm(|pm| pm.walsummarizer = c);
    }

    if with_pm(|pm| pm.start_worker_needed || pm.have_crashed_worker) {
        crate::statemachine::maybe_start_bgworkers();
    }

    if with_pm(|pm| pm.shutdown == NoShutdown && !pm.fatal_error) {
        launch_backend::wpool::maintain();
    }
    let _ = loc(3400, "LaunchMissingBackgroundProcesses");
    let _ = FastShutdown;
}
