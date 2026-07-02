//! postmaster.c core — the boot half: PostmasterMain startup sequencing,
//! ServerLoop, backend spawn, and shutdown-signal handling, C order preserved.
//! Thread model per launch_backend: children are threads; signals reaching
//! the process land on the postmaster (the only installer of handlers), and
//! child-exit notification/child signaling is the pmchild unit's redesign
//! (loud seams here). The auth/bgworker/syslogger child matrix defers.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(clippy::result_large_err)]

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

use types_core::init::{BackendType, BACKEND_NUM_TYPES};
use types_core::pid_t;
use types_error::{ErrorLocation, PgResult, DEBUG2, LOG};
use types_storage::latch::LatchHandle;
use types_storage::waiteventset::WaitEventSetHandle;

pub mod main_entry;
pub mod serverloop;
pub mod statemachine;
#[cfg(test)]
mod tests;

pub use main_entry::PostmasterMain;

pub(crate) const SRC: &str = "src/backend/postmaster/postmaster.c";

pub(crate) fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new(SRC, line, func)
}

pub const NoShutdown: i32 = 0;
pub const SmartShutdown: i32 = 1;
pub const FastShutdown: i32 = 2;
pub const ImmediateShutdown: i32 = 3;

pub const SIGKILL_CHILDREN_AFTER_SECS: i64 = 5;
pub const MAXLISTEN: usize = 64;

// miscadmin.h lock-file line numbers + pg_ctl status strings.
pub const LOCK_FILE_LINE_SOCKET_DIR: i32 = 5;
pub const LOCK_FILE_LINE_LISTEN_ADDR: i32 = 6;
pub const LOCK_FILE_LINE_PM_STATUS: i32 = 8;
pub const PM_STATUS_STARTING: &str = "starting";
pub const PM_STATUS_STOPPING: &str = "stopping";
pub const PM_STATUS_READY: &str = "ready   ";
pub const PM_STATUS_STANDBY: &str = "standby ";

/// PMState (postmaster.c); ordering is load-bearing (`pmState < PM_STOP_BACKENDS`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PMState {
    PM_INIT = 0,
    PM_STARTUP,
    PM_RECOVERY,
    PM_HOT_STANDBY,
    PM_RUN,
    PM_STOP_BACKENDS,
    PM_WAIT_BACKENDS,
    PM_WAIT_XLOG_SHUTDOWN,
    PM_WAIT_XLOG_ARCHIVAL,
    PM_WAIT_IO_WORKERS,
    PM_WAIT_DEAD_END,
    PM_WAIT_CHECKPOINTER,
    PM_NO_CHILDREN,
}

pub(crate) fn pmstate_name(state: PMState) -> &'static str {
    match state {
        PMState::PM_INIT => "PM_INIT",
        PMState::PM_STARTUP => "PM_STARTUP",
        PMState::PM_RECOVERY => "PM_RECOVERY",
        PMState::PM_HOT_STANDBY => "PM_HOT_STANDBY",
        PMState::PM_RUN => "PM_RUN",
        PMState::PM_STOP_BACKENDS => "PM_STOP_BACKENDS",
        PMState::PM_WAIT_BACKENDS => "PM_WAIT_BACKENDS",
        PMState::PM_WAIT_XLOG_SHUTDOWN => "PM_WAIT_XLOG_SHUTDOWN",
        PMState::PM_WAIT_XLOG_ARCHIVAL => "PM_WAIT_XLOG_ARCHIVAL",
        PMState::PM_WAIT_IO_WORKERS => "PM_WAIT_IO_WORKERS",
        PMState::PM_WAIT_DEAD_END => "PM_WAIT_DEAD_END",
        PMState::PM_WAIT_CHECKPOINTER => "PM_WAIT_CHECKPOINTER",
        PMState::PM_NO_CHILDREN => "PM_NO_CHILDREN",
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartupStatusEnum {
    NotRunning,
    Running,
    Signaled,
    Crashed,
}

pub type BackendTypeMask = u32;

pub const BTYPE_MASK_NONE: BackendTypeMask = 0;
pub const BTYPE_MASK_ALL: BackendTypeMask = (1 << BACKEND_NUM_TYPES as u32) - 1;

pub fn btmask(t: BackendType) -> BackendTypeMask {
    1 << (t as u32)
}

pub fn btmask_add(mask: BackendTypeMask, t: BackendType) -> BackendTypeMask {
    mask | btmask(t)
}

pub fn btmask_all_except(ts: &[BackendType]) -> BackendTypeMask {
    let mut mask = BTYPE_MASK_ALL;
    for t in ts {
        mask &= !btmask(*t);
    }
    mask
}

pub fn btmask_contains(mask: BackendTypeMask, t: BackendType) -> bool {
    mask & btmask(t) != 0
}

#[derive(Clone, Copy, Debug)]
pub struct PmChild {
    pub child_slot: pmchild_seams::PmChildSlot,
    pub bkend_type: BackendType,
    pub pid: pid_t,
}

pub struct PostmasterState {
    pub pm_state: PMState,
    pub shutdown: i32,
    pub conns_allowed: bool,
    pub fatal_error: bool,
    pub abort_start_time: i64,
    pub reached_consistency: bool,
    pub startup_status: StartupStatusEnum,
    pub start_autovac_launcher: bool,
    pub avlauncher_needs_signal: bool,
    pub wal_receiver_requested: bool,
    pub io_worker_count: i32,
    pub listen_sockets: Vec<i32>,
    pub pm_wait_set: Option<WaitEventSetHandle>,
    pub checkpointer: Option<PmChild>,
    pub bgwriter: Option<PmChild>,
    pub startup: Option<PmChild>,
    pub walwriter: Option<PmChild>,
    pub autovac_launcher: Option<PmChild>,
    pub pgarch: Option<PmChild>,
    pub slotsync_worker: Option<PmChild>,
    pub walreceiver: Option<PmChild>,
    pub walsummarizer: Option<PmChild>,
    pub syslogger: Option<PmChild>,
}

impl PostmasterState {
    const fn new() -> Self {
        PostmasterState {
            pm_state: PMState::PM_INIT,
            shutdown: NoShutdown,
            conns_allowed: false,
            fatal_error: false,
            abort_start_time: 0,
            reached_consistency: false,
            startup_status: StartupStatusEnum::NotRunning,
            start_autovac_launcher: false,
            avlauncher_needs_signal: false,
            wal_receiver_requested: false,
            io_worker_count: 0,
            listen_sockets: Vec::new(),
            pm_wait_set: None,
            checkpointer: None,
            bgwriter: None,
            startup: None,
            walwriter: None,
            autovac_launcher: None,
            pgarch: None,
            slotsync_worker: None,
            walreceiver: None,
            walsummarizer: None,
            syslogger: None,
        }
    }
}

thread_local! {
    static PM: RefCell<PostmasterState> = const { RefCell::new(PostmasterState::new()) };
}

pub fn with_pm<R>(f: impl FnOnce(&mut PostmasterState) -> R) -> R {
    PM.with(|pm| f(&mut pm.borrow_mut()))
}

// C `volatile sig_atomic_t` pending flags: real signal handlers run on an
// arbitrary thread under the thread model, so these are process statics.
pub static PENDING_PM_SHUTDOWN_REQUEST: AtomicBool = AtomicBool::new(false);
pub static PENDING_PM_FAST_SHUTDOWN_REQUEST: AtomicBool = AtomicBool::new(false);
pub static PENDING_PM_IMMEDIATE_SHUTDOWN_REQUEST: AtomicBool = AtomicBool::new(false);
pub static PENDING_PM_RELOAD_REQUEST: AtomicBool = AtomicBool::new(false);
pub static PENDING_PM_CHILD_EXIT: AtomicBool = AtomicBool::new(false);
pub static PENDING_PM_PMSIGNAL: AtomicBool = AtomicBool::new(false);

static PM_LATCH: OnceLock<LatchHandle> = OnceLock::new();

pub(crate) fn publish_pm_latch(l: LatchHandle) {
    let _ = PM_LATCH.set(l);
}

pub(crate) fn set_pm_latch() {
    if let Some(l) = PM_LATCH.get() {
        latch::SetLatch(*l);
    }
}

pub fn handle_pm_pmsignal_signal(_sig: i32) {
    PENDING_PM_PMSIGNAL.store(true, Ordering::Release);
    set_pm_latch();
}

pub fn handle_pm_reload_request_signal(_sig: i32) {
    PENDING_PM_RELOAD_REQUEST.store(true, Ordering::Release);
    set_pm_latch();
}

pub fn handle_pm_shutdown_request_signal(sig: i32) {
    match sig {
        libc::SIGTERM => {
            PENDING_PM_SHUTDOWN_REQUEST.store(true, Ordering::Release);
        }
        libc::SIGINT => {
            PENDING_PM_FAST_SHUTDOWN_REQUEST.store(true, Ordering::Release);
            PENDING_PM_SHUTDOWN_REQUEST.store(true, Ordering::Release);
        }
        libc::SIGQUIT => {
            PENDING_PM_IMMEDIATE_SHUTDOWN_REQUEST.store(true, Ordering::Release);
            PENDING_PM_SHUTDOWN_REQUEST.store(true, Ordering::Release);
        }
        _ => {}
    }
    set_pm_latch();
}

pub fn handle_pm_child_exit_signal(_sig: i32) {
    PENDING_PM_CHILD_EXIT.store(true, Ordering::Release);
    set_pm_latch();
}

pub(crate) fn report(level: types_error::ErrorLevel, msg: String, line: i32, func: &'static str) {
    let _ = elog::ereport(level).errmsg(msg).finish(loc(line, func));
}

pub(crate) fn report_internal(
    level: types_error::ErrorLevel,
    msg: String,
    line: i32,
    func: &'static str,
) {
    let _ = elog::ereport(level).errmsg_internal(msg).finish(loc(line, func));
}

pub fn process_pm_reload_request() -> PgResult<()> {
    PENDING_PM_RELOAD_REQUEST.store(false, Ordering::Release);

    report_internal(
        DEBUG2,
        "postmaster received reload request signal".into(),
        1999,
        "process_pm_reload_request",
    );

    let shutdown = with_pm(|pm| pm.shutdown);
    if shutdown <= SmartShutdown {
        report(
            LOG,
            "received SIGHUP, reloading configuration files".into(),
            2004,
            "process_pm_reload_request",
        );
        guc_file::ProcessConfigFile(types_guc::GucContext::PGC_SIGHUP)?;
        pmchild_seams::signal_children::call(
            libc::SIGHUP,
            btmask_all_except(&[BackendType::DeadEndBackend]),
        );

        if !auth_seams::load_hba::call() {
            report(LOG, "pg_hba.conf was not reloaded".into(), 2010, "process_pm_reload_request");
        }
        if !auth_seams::load_ident::call() {
            report(LOG, "pg_ident.conf was not reloaded".into(), 2015, "process_pm_reload_request");
        }
    }
    Ok(())
}

pub fn process_pm_pmsignal() -> PgResult<()> {
    use pmsignal::PMSignalReason::*;

    PENDING_PM_PMSIGNAL.store(false, Ordering::Release);

    report_internal(
        DEBUG2,
        "postmaster received pmsignal signal".into(),
        3695,
        "process_pm_pmsignal",
    );

    if pmsignal::CheckPostmasterSignal(PMSIGNAL_RECOVERY_STARTED)
        && with_pm(|pm| pm.pm_state == PMState::PM_STARTUP && pm.shutdown == NoShutdown)
    {
        with_pm(|pm| {
            pm.fatal_error = false;
            pm.abort_start_time = 0;
            pm.reached_consistency = false;
        });

        if guc_tables::vars::XLogArchiveMode.read() >= 2 {
            let arch = statemachine::StartChildProcess(BackendType::Archiver);
            with_pm(|pm| pm.pgarch = arch);
        }
        if !guc_tables::vars::EnableHotStandby.read() {
            miscinit::AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_STANDBY)?;
        }

        statemachine::UpdatePMState(PMState::PM_RECOVERY);
    }

    if pmsignal::CheckPostmasterSignal(PMSIGNAL_RECOVERY_CONSISTENT)
        && with_pm(|pm| pm.pm_state == PMState::PM_RECOVERY && pm.shutdown == NoShutdown)
    {
        with_pm(|pm| pm.reached_consistency = true);
    }

    if pmsignal::CheckPostmasterSignal(PMSIGNAL_BEGIN_HOT_STANDBY)
        && with_pm(|pm| pm.pm_state == PMState::PM_RECOVERY && pm.shutdown == NoShutdown)
    {
        report(
            LOG,
            "database system is ready to accept read-only connections".into(),
            3745,
            "process_pm_pmsignal",
        );
        miscinit::AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_READY)?;
        statemachine::UpdatePMState(PMState::PM_HOT_STANDBY);
        with_pm(|pm| pm.conns_allowed = true);
    }

    if pmsignal::CheckPostmasterSignal(PMSIGNAL_BACKGROUND_WORKER_CHANGE) {
        panic!("process_pm_pmsignal: BackgroundWorkerStateChange unported (backend-postmaster-bgworker)");
    }

    let syslogger = with_pm(|pm| pm.syslogger);
    if let Some(syslogger) = syslogger {
        if syslogger_seams::check_logrotate_signal::call() {
            statemachine::signal_child(&syslogger, libc::SIGUSR1);
            syslogger_seams::remove_logrotate_signal_files::call();
        } else if pmsignal::CheckPostmasterSignal(PMSIGNAL_ROTATE_LOGFILE) {
            statemachine::signal_child(&syslogger, libc::SIGUSR1);
        }
    }

    if pmsignal::CheckPostmasterSignal(PMSIGNAL_START_AUTOVAC_LAUNCHER)
        && with_pm(|pm| pm.shutdown <= SmartShutdown && pm.pm_state < PMState::PM_STOP_BACKENDS)
    {
        with_pm(|pm| pm.start_autovac_launcher = true);
    }

    if pmsignal::CheckPostmasterSignal(PMSIGNAL_START_AUTOVAC_WORKER)
        && with_pm(|pm| pm.shutdown <= SmartShutdown && pm.pm_state < PMState::PM_STOP_BACKENDS)
    {
        statemachine::StartAutovacuumWorker();
    }

    if pmsignal::CheckPostmasterSignal(PMSIGNAL_START_WALRECEIVER) {
        with_pm(|pm| pm.wal_receiver_requested = true);
    }

    if pmsignal::CheckPostmasterSignal(PMSIGNAL_XLOG_IS_SHUTDOWN) {
        if with_pm(|pm| pm.pm_state == PMState::PM_WAIT_XLOG_SHUTDOWN) {
            debug_assert!(with_pm(|pm| pm.shutdown > NoShutdown));
            let pgarch = with_pm(|pm| pm.pgarch);
            if let Some(pgarch) = pgarch {
                statemachine::signal_child(&pgarch, libc::SIGUSR2);
            }
            pmchild_seams::signal_children::call(libc::SIGUSR2, btmask(BackendType::WalSender));
            statemachine::UpdatePMState(PMState::PM_WAIT_XLOG_ARCHIVAL);
        } else if with_pm(|pm| !pm.fatal_error && pm.shutdown != ImmediateShutdown) {
            panic!("process_pm_pmsignal: unexpected shutdown checkpoint; crash-restart needs the reaper (backend-postmaster-pmchild)");
        }
    }

    if pmsignal::CheckPostmasterSignal(PMSIGNAL_ADVANCE_STATE_MACHINE) {
        statemachine::PostmasterStateMachine()?;
    }

    Ok(())
}

/// process_pm_child_exit — the SIGCHLD reaper. No SIGCHLD exists under the
/// thread model; the pmchild unit owns the replacement exit channel.
pub fn process_pm_child_exit() -> PgResult<()> {
    PENDING_PM_CHILD_EXIT.store(false, Ordering::Release);
    panic!("process_pm_child_exit: child-exit reaping is the pmchild unit's thread-model redesign (backend-postmaster-pmchild)");
}

pub fn init_seams() {}
