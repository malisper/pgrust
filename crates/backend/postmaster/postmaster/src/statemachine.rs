//! Shutdown sequencing: process_pm_shutdown_request, PostmasterStateMachine,
//! HandleFatalError, the crash-reinit arm, child signaling, child launch,
//! ExitPostmaster. Child bookkeeping crosses the pmchild seams.

use std::sync::atomic::Ordering;

use types_core::init::BackendType;
use types_error::{DEBUG1, DEBUG2, DEBUG3, LOG};
use types_startup::StartupData;

use crate::serverloop::{canAcceptConnections, ConfigurePostmasterWaitSet};
use crate::{
    btmask, btmask_add, btmask_all_except, loc, pmstate_name, report, report_internal, with_pm,
    BackendTypeMask, FastShutdown, ImmediateShutdown, PMState, PgResult, PmChild, SmartShutdown,
    StartupStatusEnum, BTYPE_MASK_NONE, LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STOPPING,
};

pub fn UpdatePMState(new_state: PMState) {
    let old = with_pm(|pm| pm.pm_state);
    report_internal(
        DEBUG1,
        format!("updating PMState from {} to {}", pmstate_name(old), pmstate_name(new_state)),
        3277,
        "UpdatePMState",
    );
    with_pm(|pm| {
        pm.pm_state = new_state;
        // GL-GANGWEDGE-1: the shutdown-stall watchdog's progress stamp. Any
        // state change is progress, so re-stamping here (rather than on entry
        // to one named state) is what makes the watchdog measure "stuck"
        // instead of "slow".
        if old != new_state {
            pm.pm_state_since = now_secs();
        }
    });
}

fn pm_signame(signal: i32) -> &'static str {
    match signal {
        procsignal::signums::SIGABRT => "SIGABRT",
        procsignal::signums::SIGCHLD => "SIGCHLD",
        procsignal::signums::SIGHUP => "SIGHUP",
        procsignal::signums::SIGINT => "SIGINT",
        procsignal::signums::SIGKILL => "SIGKILL",
        procsignal::signums::SIGQUIT => "SIGQUIT",
        procsignal::signums::SIGTERM => "SIGTERM",
        procsignal::signums::SIGUSR1 => "SIGUSR1",
        procsignal::signums::SIGUSR2 => "SIGUSR2",
        _ => "(unknown)",
    }
}

/// signal_child for the postmaster-tracked aux singletons; per-child signal
/// delivery to backend threads is the pmchild redesign.
pub fn signal_child(pmchild: &PmChild, signal: i32) {
    report_internal(
        DEBUG3,
        format!(
            "sending signal {}/{} to {} process with pid {}",
            signal,
            pm_signame(signal),
            miscinit::GetBackendTypeDesc(pmchild.bkend_type),
            pmchild.pid
        ),
        3455,
        "signal_child",
    );
    // kill(pid, signal)'s thread rendering; C ignores kill() failures here.
    let _ = procsignal::SendThreadSignal(pmchild.pid, signal);
}

pub fn SignalChildren(signal: i32, target_mask: BackendTypeMask) -> bool {
    pmchild_seams::signal_children::call(signal, target_mask)
}

pub fn TerminateChildren(signal: i32) {
    SignalChildren(signal, btmask_all_except(&[BackendType::Logger]));
    if with_pm(|pm| pm.startup.is_some())
        && (signal == procsignal::signums::SIGQUIT || signal == procsignal::signums::SIGKILL || signal == procsignal::signums::SIGABRT)
    {
        with_pm(|pm| pm.startup_status = StartupStatusEnum::Signaled);
    }
}

pub(crate) fn HandleFatalError(
    reason: pmsignal::QuitSignalReason,
    consider_sigabrt: bool,
) -> PgResult<()> {
    debug_assert!(with_pm(|pm| !pm.fatal_error));
    debug_assert!(with_pm(|pm| pm.shutdown != ImmediateShutdown));

    launch_backend::wpool::flush_for_crash();

    pmsignal::SetQuitSignalReason(reason);

    let sigtosend = if consider_sigabrt && guc_tables::vars::send_abort_for_crash.read() {
        procsignal::signums::SIGABRT
    } else {
        procsignal::signums::SIGQUIT
    };
    TerminateChildren(sigtosend);

    with_pm(|pm| pm.fatal_error = true);

    match with_pm(|pm| pm.pm_state) {
        PMState::PM_INIT | PMState::PM_STARTUP => debug_assert!(false),
        PMState::PM_RECOVERY
        | PMState::PM_HOT_STANDBY
        | PMState::PM_RUN
        | PMState::PM_STOP_BACKENDS => UpdatePMState(PMState::PM_WAIT_BACKENDS),
        PMState::PM_WAIT_BACKENDS => {}
        PMState::PM_WAIT_XLOG_SHUTDOWN
        | PMState::PM_WAIT_XLOG_ARCHIVAL
        | PMState::PM_WAIT_CHECKPOINTER
        | PMState::PM_WAIT_IO_WORKERS => {
            ConfigurePostmasterWaitSet(false)?;
            UpdatePMState(PMState::PM_WAIT_DEAD_END);
        }
        PMState::PM_WAIT_DEAD_END | PMState::PM_NO_CHILDREN => {}
    }

    if with_pm(|pm| pm.abort_start_time) == 0 {
        with_pm(|pm| pm.abort_start_time = now_secs());
    }
    let _ = loc(2701, "HandleFatalError");
    Ok(())
}

pub fn process_pm_shutdown_request() -> PgResult<()> {
    report_internal(
        DEBUG2,
        "postmaster received shutdown request signal".into(),
        2078,
        "process_pm_shutdown_request",
    );

    crate::PENDING_PM_SHUTDOWN_REQUEST.store(false, Ordering::Release);

    let mode = if crate::PENDING_PM_IMMEDIATE_SHUTDOWN_REQUEST.swap(false, Ordering::AcqRel) {
        crate::PENDING_PM_FAST_SHUTDOWN_REQUEST.store(false, Ordering::Release);
        ImmediateShutdown
    } else if crate::PENDING_PM_FAST_SHUTDOWN_REQUEST.swap(false, Ordering::AcqRel) {
        FastShutdown
    } else {
        SmartShutdown
    };

    match mode {
        SmartShutdown => {
            if with_pm(|pm| pm.shutdown >= SmartShutdown) {
                return Ok(());
            }
            with_pm(|pm| pm.shutdown = SmartShutdown);
            report(LOG, "received smart shutdown request".into(), 2113, "process_pm_shutdown_request");

            miscinit::AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STOPPING)?;

            let pm_state = with_pm(|pm| pm.pm_state);
            if pm_state == PMState::PM_RUN || pm_state == PMState::PM_HOT_STANDBY {
                with_pm(|pm| pm.conns_allowed = false);
            } else if pm_state == PMState::PM_STARTUP || pm_state == PMState::PM_RECOVERY {
                UpdatePMState(PMState::PM_STOP_BACKENDS);
            }

            PostmasterStateMachine()?;
        }
        FastShutdown => {
            if with_pm(|pm| pm.shutdown >= FastShutdown) {
                return Ok(());
            }
            with_pm(|pm| pm.shutdown = FastShutdown);
            report(LOG, "received fast shutdown request".into(), 2153, "process_pm_shutdown_request");

            miscinit::AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STOPPING)?;

            let pm_state = with_pm(|pm| pm.pm_state);
            if pm_state == PMState::PM_STARTUP || pm_state == PMState::PM_RECOVERY {
                UpdatePMState(PMState::PM_STOP_BACKENDS);
            } else if pm_state == PMState::PM_RUN || pm_state == PMState::PM_HOT_STANDBY {
                report(LOG, "aborting any active transactions".into(), 2170, "process_pm_shutdown_request");
                UpdatePMState(PMState::PM_STOP_BACKENDS);
            }

            PostmasterStateMachine()?;
        }
        _ => {
            // ImmediateShutdown
            if with_pm(|pm| pm.shutdown >= ImmediateShutdown) {
                return Ok(());
            }
            with_pm(|pm| pm.shutdown = ImmediateShutdown);
            report(LOG, "received immediate shutdown request".into(), 2193, "process_pm_shutdown_request");

            miscinit::AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STOPPING)?;

            pmsignal::SetQuitSignalReason(pmsignal::QuitSignalReason::PMQUIT_FOR_STOP);
            // Immediate shutdown skips PM_STOP_BACKENDS: fence the
            // registry-invisible standing gang here (see the
            // PM_STOP_BACKENDS arm in PostmasterStateMachine).
            if postmaster_seams::rtgang_retire::is_installed() {
                postmaster_seams::rtgang_retire::call();
            }
            TerminateChildren(procsignal::signums::SIGQUIT);
            UpdatePMState(PMState::PM_WAIT_BACKENDS);

            with_pm(|pm| pm.abort_start_time = now_secs());

            PostmasterStateMachine()?;
        }
    }
    Ok(())
}

fn now_secs() -> i64 {
    // DST P2 (contract §1.2): crash-loop window stamps on pg_clock.
    pg_clock::wall_secs()
}

pub fn PostmasterStateMachine() -> PgResult<()> {
    if with_pm(|pm| {
        matches!(pm.pm_state, PMState::PM_RUN | PMState::PM_HOT_STANDBY) && !pm.conns_allowed
    }) && pmchild_seams::count_children::call(btmask(BackendType::Backend)) == 0
    {
        UpdatePMState(PMState::PM_STOP_BACKENDS);
    }

    if with_pm(|pm| {
        matches!(pm.pm_state, PMState::PM_STOP_BACKENDS | PMState::PM_WAIT_BACKENDS)
    }) {
        let mut target_mask = BTYPE_MASK_NONE;
        for t in [
            BackendType::Backend,
            BackendType::AutovacLauncher,
            BackendType::AutovacWorker,
            BackendType::BgWorker,
            BackendType::WalWriter,
            BackendType::BgWriter,
            BackendType::SlotsyncWorker,
            BackendType::WalSummarizer,
            BackendType::Startup,
            BackendType::WalReceiver,
        ] {
            target_mask = btmask_add(target_mask, t);
        }
        if with_pm(|pm| pm.fatal_error || pm.shutdown >= ImmediateShutdown) {
            for t in [
                BackendType::Checkpointer,
                BackendType::Archiver,
                BackendType::IoWorker,
                BackendType::WalSender,
            ] {
                target_mask = btmask_add(target_mask, t);
            }
        }

        if with_pm(|pm| pm.pm_state == PMState::PM_STOP_BACKENDS) {
            bgworker::ForgetUnstartedBackgroundWorkers();
            // Registry-invisible standing runtime executors: fence them
            // alongside the SIGTERM broadcast (they carry no pmchild slot,
            // so the mask below can never reach them). Parked ones exit
            // clean; exits poke PMSIGNAL_ADVANCE_STATE_MACHINE so the
            // quiescence gate below re-runs as they drain.
            if postmaster_seams::rtgang_retire::is_installed() {
                postmaster_seams::rtgang_retire::call();
            }
            SignalChildren(procsignal::signums::SIGTERM, target_mask);
            UpdatePMState(PMState::PM_WAIT_BACKENDS);
        }

        if pmchild_seams::count_children::call(target_mask) == 0
            && (!postmaster_seams::rtgang_live::is_installed()
                || postmaster_seams::rtgang_live::call() == 0)
        {
            if with_pm(|pm| pm.shutdown >= ImmediateShutdown || pm.fatal_error) {
                UpdatePMState(PMState::PM_WAIT_DEAD_END);
                ConfigurePostmasterWaitSet(false)?;
                SignalChildren(procsignal::signums::SIGQUIT, btmask(BackendType::DeadEndBackend));
            } else {
                debug_assert!(with_pm(|pm| pm.shutdown > crate::NoShutdown));
                if with_pm(|pm| pm.checkpointer.is_none()) {
                    let c = StartChildProcess(BackendType::Checkpointer);
                    with_pm(|pm| pm.checkpointer = c);
                }
                let checkpointer = with_pm(|pm| pm.checkpointer);
                match checkpointer {
                    Some(c) => {
                        signal_child(&c, procsignal::signums::SIGINT);
                        UpdatePMState(PMState::PM_WAIT_XLOG_SHUTDOWN);
                    }
                    None => {
                        HandleFatalError(pmsignal::QuitSignalReason::PMQUIT_FOR_CRASH, false)?;
                    }
                }
            }
        }
    }

    // PM_WAIT_XLOG_SHUTDOWN -> PM_WAIT_XLOG_ARCHIVAL happens in
    // process_pm_pmsignal (PMSIGNAL_XLOG_IS_SHUTDOWN).

    if with_pm(|pm| pm.pm_state == PMState::PM_WAIT_XLOG_ARCHIVAL)
        && pmchild_seams::count_children::call(btmask_all_except(&[
            BackendType::Checkpointer,
            BackendType::IoWorker,
            BackendType::Logger,
            BackendType::DeadEndBackend,
        ])) == 0
    {
        UpdatePMState(PMState::PM_WAIT_IO_WORKERS);
        SignalChildren(procsignal::signums::SIGUSR2, btmask(BackendType::IoWorker));
    }

    if with_pm(|pm| pm.pm_state == PMState::PM_WAIT_IO_WORKERS && pm.io_worker_count == 0) {
        UpdatePMState(PMState::PM_WAIT_CHECKPOINTER);
        let checkpointer = with_pm(|pm| pm.checkpointer);
        if let Some(c) = checkpointer {
            signal_child(&c, procsignal::signums::SIGUSR2);
        }
    }

    // PM_WAIT_CHECKPOINTER -> PM_WAIT_DEAD_END happens in process_pm_child_exit.

    if with_pm(|pm| pm.pm_state == PMState::PM_WAIT_DEAD_END)
        && pmchild_seams::count_children::call(btmask_all_except(&[BackendType::Logger])) == 0
    {
        UpdatePMState(PMState::PM_NO_CHILDREN);
    }

    if with_pm(|pm| pm.shutdown > crate::NoShutdown && pm.pm_state == PMState::PM_NO_CHILDREN) {
        if with_pm(|pm| pm.fatal_error) {
            report(LOG, "abnormal database system shutdown".into(), 3168, "PostmasterStateMachine");
            ExitPostmaster(1);
        } else {
            ExitPostmaster(0);
        }
    }

    if with_pm(|pm| pm.pm_state == PMState::PM_NO_CHILDREN) {
        if with_pm(|pm| pm.startup_status == StartupStatusEnum::Crashed) {
            report(LOG, "shutting down due to startup process failure".into(), 3190, "PostmasterStateMachine");
            ExitPostmaster(1);
        }
        if !guc_tables::vars::restart_after_crash.read() {
            report(LOG, "shutting down because \"restart_after_crash\" is off".into(), 3196, "PostmasterStateMachine");
            ExitPostmaster(1);
        }
    }

    if with_pm(|pm| pm.fatal_error && pm.pm_state == PMState::PM_NO_CHILDREN) {
        report(
            LOG,
            "all server processes terminated; reinitializing".into(),
            3205,
            "PostmasterStateMachine",
        );

        if guc_tables::vars::remove_temp_files_after_crash.read() {
            fd::RemovePgTempFiles()?;
        }
        bgworker::ResetBackgroundWorkerCrashTimes();

        ipc::shmem_exit(1)?;

        transam_xlog::LocalProcessControlFile(true)?;

        ipci::ResetShmemAfterCrash()?;
        // C reset_shared() re-runs BackgroundWorkerShmemInit: counts zeroed,
        // surviving registrations recopied into slots.
        bgworker::BackgroundWorkerShmemInit();
        if launcher_seams::apply_launcher_shmem_init::is_installed() {
            launcher_seams::apply_launcher_shmem_init::call();
        }

        UpdatePMState(PMState::PM_STARTUP);

        crate::serverloop::maybe_adjust_io_workers();

        let startup = StartChildProcess(BackendType::Startup);
        debug_assert!(startup.is_some());
        with_pm(|pm| {
            pm.startup = startup;
            pm.startup_status = StartupStatusEnum::Running;
            pm.abort_start_time = 0;
            // Crash-cycle children all drained (PM_NO_CHILDREN): disarm the
            // forced-exit floor before the reinitialized cluster comes up.
            pm.lethal_time = 0;
        });

        ConfigurePostmasterWaitSet(true)?;
    }

    Ok(())
}

pub fn ExitPostmaster(status: i32) -> ! {
    // pthread_is_threaded_np LOG: inverted by design under the thread model.
    ipc_seams::proc_exit::call(status, init_small::globals::MyProcPid())
}

/// StartChildProcess — start an aux child of the given type. Unported child
/// mains reach launch_backend's named panic.
pub fn StartChildProcess(child_type: BackendType) -> Option<PmChild> {
    let Some(child_slot) = pmchild_seams::assign_postmaster_child_slot::call(child_type) else {
        return None;
    };
    let pid =
        launch_backend::postmaster_child_launch(child_type, child_slot, StartupData::None, None);
    if pid < 0 {
        pmchild_seams::release_postmaster_child_slot::call(child_slot);
        report(
            LOG,
            format!("could not fork {} process", miscinit::GetBackendTypeDesc(child_type)),
            3960,
            "StartChildProcess",
        );
        if child_type == BackendType::Startup {
            ExitPostmaster(1);
        }
        return None;
    }
    pmchild_seams::set_child_pid::call(child_slot, pid);
    Some(PmChild { child_slot, bkend_type: child_type, pid })
}

pub fn StartSysLogger() {
    debug_assert!(with_pm(|pm| pm.syslogger.is_none()));
    let Some(child_slot) = pmchild_seams::assign_postmaster_child_slot::call(BackendType::Logger)
    else {
        panic!("no postmaster child slot available for syslogger");
    };
    // C: SysLogger_Start's FATALs longjmp out of the postmaster.
    let pid = match syslogger::SysLogger_Start(child_slot) {
        Ok(pid) => pid,
        Err(e) => {
            elog::emit_error_report_for(&e);
            ExitPostmaster(1);
        }
    };
    if pid == 0 {
        pmchild_seams::release_postmaster_child_slot::call(child_slot);
    } else {
        pmchild_seams::set_child_pid::call(child_slot, pid);
        with_pm(|pm| {
            pm.syslogger = Some(PmChild { child_slot, bkend_type: BackendType::Logger, pid })
        });
    }
}

/// StartBackgroundWorker(rw) (postmaster.c). Failure marks the entry crashed
/// so maybe_start_bgworkers retries later, per C.
fn StartBackgroundWorker(idx: usize) -> bool {
    debug_assert_eq!(bgworker::rw_pid(idx), 0);

    let Some(child_slot) = pmchild_seams::assign_postmaster_child_slot::call(BackendType::BgWorker)
    else {
        let _ = elog::ereport(types_error::LOG)
            .errcode(types_error::ERRCODE_CONFIGURATION_LIMIT_EXCEEDED)
            .errmsg("no slot available for new background worker process")
            .finish(loc(4139, "StartBackgroundWorker"));
        bgworker::set_rw_crashed_at(idx, timestamp_seams::get_current_timestamp::call());
        return false;
    };

    report_internal(
        DEBUG1,
        format!("starting background worker process \"{}\"", bgworker::rw_name(idx)),
        4153,
        "StartBackgroundWorker",
    );

    let shmem_slot = bgworker::rw_shmem_slot(idx);
    let startup_data = StartupData::BgWorker(types_startup::BgWorkerStartupData {
        slot: shmem_slot,
        generation: bgworker::slot_generation(shmem_slot),
    });
    let pid = launch_backend::postmaster_child_launch(
        BackendType::BgWorker,
        child_slot,
        startup_data,
        None,
    );
    if pid < 0 {
        report(LOG, "could not fork background worker process".into(), 4162, "StartBackgroundWorker");
        pmchild_seams::release_postmaster_child_slot::call(child_slot);
        bgworker::set_rw_crashed_at(idx, timestamp_seams::get_current_timestamp::call());
        return false;
    }

    bgworker::set_rw_pid(idx, pid);
    pmchild_seams::set_child_pid::call(child_slot, pid);
    bgworker::ReportBackgroundWorkerPID(idx);
    true
}

/// bgworker_should_start_now(start_time) (postmaster.c).
fn bgworker_should_start_now(start_time: bgworker::BgWorkerStartTime) -> bool {
    use bgworker::BgWorkerStartTime::*;
    match with_pm(|pm| pm.pm_state) {
        PMState::PM_NO_CHILDREN
        | PMState::PM_WAIT_CHECKPOINTER
        | PMState::PM_WAIT_DEAD_END
        | PMState::PM_WAIT_XLOG_ARCHIVAL
        | PMState::PM_WAIT_XLOG_SHUTDOWN
        | PMState::PM_WAIT_IO_WORKERS
        | PMState::PM_WAIT_BACKENDS
        | PMState::PM_STOP_BACKENDS => false,
        PMState::PM_RUN => matches!(start_time, RecoveryFinished | ConsistentState),
        PMState::PM_HOT_STANDBY => start_time == ConsistentState,
        PMState::PM_RECOVERY | PMState::PM_STARTUP | PMState::PM_INIT => {
            start_time == PostmasterStart
        }
    }
}

/// maybe_start_bgworkers (postmaster.c). Restart scheduling is unreachable
/// this phase: registration rejects bgw_restart_time >= 0.
pub fn maybe_start_bgworkers() {
    const MAX_BGWORKERS_TO_LAUNCH: i32 = 100;
    let mut num_launched = 0;

    if with_pm(|pm| pm.fatal_error) {
        with_pm(|pm| {
            pm.start_worker_needed = false;
            pm.have_crashed_worker = false;
        });
        return;
    }

    with_pm(|pm| {
        pm.start_worker_needed = false;
        pm.have_crashed_worker = false;
    });

    for idx in bgworker::registered_indexes() {
        if bgworker::rw_pid(idx) != 0 {
            continue;
        }

        if bgworker::rw_terminate(idx) {
            bgworker::ForgetBackgroundWorker(idx);
            continue;
        }

        if bgworker::rw_crashed_at(idx) != 0 {
            if bgworker::rw_restart_time(idx) == bgworker::BGW_NEVER_RESTART {
                let notify_pid = bgworker::rw_notify_pid(idx);
                bgworker::ForgetBackgroundWorker(idx);
                if notify_pid != 0 {
                    let _ = procsignal::SendThreadSignal(notify_pid, procsignal::signums::SIGUSR1);
                }
                continue;
            }
            // Not yet time to restart this worker: remember to check again
            // later (postmaster.c maybe_start_bgworkers, restart-interval arm).
            let now = timestamp_seams::get_current_timestamp::call();
            let restart_usecs = (bgworker::rw_restart_time(idx) as i64) * 1000 * 1000;
            if now - bgworker::rw_crashed_at(idx) < restart_usecs {
                with_pm(|pm| pm.have_crashed_worker = true);
                continue;
            }
        }

        if bgworker_should_start_now(bgworker::rw_start_time(idx)) {
            bgworker::set_rw_crashed_at(idx, 0);

            bgworker::gtrace("pm.spawn.begin");
            if !StartBackgroundWorker(idx) {
                with_pm(|pm| pm.start_worker_needed = true);
                return;
            }

            num_launched += 1;
            if num_launched >= MAX_BGWORKERS_TO_LAUNCH {
                with_pm(|pm| pm.start_worker_needed = true);
                return;
            }
        }
    }
}

pub fn StartAutovacuumWorker() {
    use types_startup::CacState;
    if canAcceptConnections(BackendType::AutovacWorker) == CacState::Ok {
        let c = StartChildProcess(BackendType::AutovacWorker);
        if c.is_none() {
            autovacuum_seams::autovac_worker_failed::call();
            with_pm(|pm| pm.avlauncher_needs_signal = true);
        }
    }
    let _ = loc(3990, "StartAutovacuumWorker");
}
