//! ServerLoop and connection intake: the C loop shape — wait set over the
//! postmaster latch + listen sockets, unconditional pending-flag processing,
//! backend spawn, SIGKILL escalation, lockfile upkeep.

use std::sync::atomic::Ordering;

use types_core::init::BackendType;
use types_core::{PGINVALID_SOCKET, STATUS_ERROR, STATUS_OK};
use types_error::{PgResult, DEBUG2, LOG};
use types_startup::{BackendStartupData, CacState, ClientSocket, StartupData};
use types_storage::waiteventset::{WaitEvent, WL_LATCH_SET, WL_SOCKET_ACCEPT};

use crate::statemachine::{signal_child, StartChildProcess, TerminateChildren};
use crate::{
    loc, report, report_internal, with_pm, PMState, FastShutdown, ImmediateShutdown, NoShutdown,
    MAXLISTEN, SIGKILL_CHILDREN_AFTER_SECS,
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
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

pub fn ServerLoop() -> PgResult<i32> {
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
                signal_child(&launcher, libc::SIGUSR2);
            }
        }

        let now = now_secs();

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
            TerminateChildren(if send_abort { libc::SIGABRT } else { libc::SIGKILL });
            with_pm(|pm| pm.abort_start_time = 0);
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
                crate::handle_pm_shutdown_request_signal(libc::SIGQUIT);
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

fn report_fork_failure_to_client(client_sock: &ClientSocket) {
    let msg = b"Ecould not fork new process for connection\n\0";
    // SAFETY: fcntl/send on the accepted fd we own; failure is ignored by design.
    unsafe {
        let flags = libc::fcntl(client_sock.sock, libc::F_GETFL);
        if flags < 0
            || libc::fcntl(client_sock.sock, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0
        {
            return;
        }
        loop {
            let rc = libc::send(
                client_sock.sock,
                msg.as_ptr() as *const libc::c_void,
                msg.len(),
                0,
            );
            if rc >= 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                break;
            }
        }
    }
}

/// maybe_adjust_io_workers. io_method defaults to "worker": the first launch
/// reaches launch_backend's named IoWorkerMain-unported panic.
pub fn maybe_adjust_io_workers() {
    // io_method enum: 0 = sync, 1 = worker, 2 = io_uring (guc_tables).
    if guc_tables::vars::io_method.read() != 1 {
        return;
    }
    let target = guc_tables::vars::io_workers.read();
    while with_pm(|pm| pm.io_worker_count) < target {
        if !with_pm(|pm| {
            pm.pm_state >= PMState::PM_STARTUP && pm.pm_state < PMState::PM_WAIT_IO_WORKERS
        }) {
            break;
        }
        match StartChildProcess(BackendType::IoWorker) {
            Some(_) => with_pm(|pm| pm.io_worker_count += 1),
            None => break,
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
