//! postmaster.c core — the boot half: PostmasterMain startup sequencing,
//! ServerLoop, backend spawn, shutdown-signal handling, and the crash-restart
//! cycle for the catchable (caught-panic) crash class, C order preserved
//! (notes/crash-restart-design.md). Thread model per launch_backend: children
//! are threads; signals reaching the process land on the postmaster (the only
//! installer of handlers). The auth/bgworker/syslogger child matrix defers.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(clippy::result_large_err)]

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

use types_core::init::{BackendType, BACKEND_NUM_TYPES};
use types_core::pid_t;
use types_error::{ErrorLocation, PgResult, DEBUG1, DEBUG2, LOG};
use types_storage::latch::LatchHandle;
use types_storage::waiteventset::WaitEventSetHandle;

#[cfg(not(target_family = "wasm"))]
pub mod crash_signals;
// wasm32: no signals on WASI — a fatal fault is a wasm trap and the runtime
// itself reports it; there is no disposition to install or restore.
#[cfg(target_family = "wasm")]
pub mod crash_signals {
    pub fn install_crash_signal_reporter() {}
}
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
/// GL-DISCONNECT-WEDGE-1: grace after the SIGKILL broadcast before the
/// postmaster forces its own exit. C's SIGKILL is unconditional, so C's
/// PM_WAIT_BACKENDS always drains; the thread rendering (SendThreadKill)
/// lands at a drain point, and a thread wedged off its drain points (deep
/// non-latch wait, latch-deaf sleep) never takes it — without a floor an
/// IMMEDIATE shutdown hangs forever, a full outage whose only remediation
/// is kill -9 from outside. Process exit is the one unconditional kill a
/// single-process cluster has; immediate shutdown is already
/// crash-equivalent, so the next start runs crash recovery exactly as if
/// the OS had killed us.
pub const FORCED_EXIT_AFTER_LETHAL_SECS: i64 = 8;

/// GL-GANGWEDGE-1: bound on a NON-immediate shutdown that stops making
/// progress through the shutdown states.
///
/// C's fast/smart shutdown waits forever by design at every shutdown gate,
/// and that is sound in C because every child is a PROCESS in the
/// postmaster's registry: SIGTERM/SIGQUIT delivery is unconditional and the
/// child dies at its next CHECK_FOR_INTERRUPTS. This port has a second child
/// population C does not — the registry-invisible standing runtime executors
/// (rtgang/pool threads, folded into the PM_WAIT_BACKENDS quiescence gate
/// through the `rtgang_live` seam). They carry no pmchild slot, so NO SIGNAL
/// CAN REACH THEM; their only stop vector is the cooperative
/// `retire_for_shutdown` fence plus a condvar wake, which a thread wedged off
/// its fence-poll points never takes. C's unbounded wait grafted onto a
/// cooperative-only stop vector is not C parity — it is an unbounded outage,
/// and the operator escalation C relies on ("just send SIGQUIT") is not
/// available to an automated harness: the cold benchmark protocol issues 43
/// `pg_ctl stop` cycles per run and a single stall loses the run.
///
/// The bound is deliberately on STATE PROGRESS rather than on one named gate,
/// because the field sightings did not all wedge at the same gate: the
/// disconnect-side sighting wedged in PM_WAIT_BACKENDS, while the two
/// shutdown-side sightings wedged AFTER the shutdown checkpoint completed
/// (server log stops between "checkpoint complete" and the postmaster's
/// lock-file unlink), i.e. somewhere in the PM_WAIT_XLOG_ARCHIVAL ..
/// PM_WAIT_DEAD_END tail. A progress bound covers all of them, and covers the
/// next gate too.
///
/// On firing, the postmaster SELF-ESCALATES to immediate shutdown — exactly
/// the operator action C assumes — which flows into the already-audited kill
/// ladder (SIGKILL_CHILDREN_AFTER_SECS, then the GL-DISCONNECT-WEDGE-1
/// FORCED_EXIT_AFTER_LETHAL_SECS floor). The worst case becomes bounded and
/// loud instead of infinite and silent. Escalation forfeits the shutdown
/// checkpoint, so the next start runs crash recovery — strictly better than
/// never stopping at all, and exactly what an operator's kill -9 would have
/// cost anyway.
///
/// NOT bounded: PM_WAIT_XLOG_SHUTDOWN, where the shutdown checkpoint runs. A
/// large buffer pool can legitimately take minutes there, and unlike every
/// other shutdown gate it is doing work rather than waiting for a child to
/// notice a stop request.
pub const PM_SHUTDOWN_STALL_SECS: i64 = 60;

/// Escape hatch for the bound above (`PGRUST_PM_SHUTDOWN_STALL_SECS`), for
/// rigs that must hold a wedge open for a stack capture, and for an operator
/// who would rather keep a stalled shutdown alive for diagnosis than let it
/// escalate. `0` disables the watchdog and restores the unbounded pre-fix
/// wait. Not a product tuning knob: hanging forever is never correct, so the
/// default is the shipped behaviour.
pub fn pm_shutdown_stall_secs() -> i64 {
    static SECS: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *SECS.get_or_init(|| {
        std::env::var("PGRUST_PM_SHUTDOWN_STALL_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<i64>().ok())
            .filter(|v| *v >= 0)
            .unwrap_or(PM_SHUTDOWN_STALL_SECS)
    })
}

/// The unmistakable log marker a scored run greps for. Emitted at LOG when
/// the watchdog above fires. Fail fast and visibly beats hanging silently.
pub const WEDGE_MARKER: &str = "PGRUST-SHUTDOWN-WEDGE";

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

pub const MAX_IO_WORKERS_USIZE: usize = types_storage::storage::MAX_IO_WORKERS as usize;

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
    /// Time of the SIGKILL/SIGABRT broadcast to recalcitrant children; the
    /// forced-exit floor (serverloop) fires FORCED_EXIT_AFTER_LETHAL_SECS
    /// after it if children still have not drained. 0 = not sent.
    pub lethal_time: i64,
    /// GL-GANGWEDGE-1: wall-clock second of the last PMState change, and
    /// whether the shutdown-stall watchdog has already escalated for this
    /// shutdown (it fires once — after that the immediate-shutdown ladder
    /// owns the outcome). 0 = never stamped.
    pub pm_state_since: i64,
    pub stall_escalated: bool,
    pub reached_consistency: bool,
    pub startup_status: StartupStatusEnum,
    pub start_autovac_launcher: bool,
    pub avlauncher_needs_signal: bool,
    pub start_worker_needed: bool,
    pub have_crashed_worker: bool,
    pub wal_receiver_requested: bool,
    pub io_worker_count: i32,
    pub io_worker_children: [Option<PmChild>; MAX_IO_WORKERS_USIZE],
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
            lethal_time: 0,
            pm_state_since: 0,
            stall_escalated: false,
            reached_consistency: false,
            startup_status: StartupStatusEnum::NotRunning,
            start_autovac_launcher: false,
            avlauncher_needs_signal: false,
            start_worker_needed: true,
            have_crashed_worker: false,
            wal_receiver_requested: false,
            io_worker_count: 0,
            io_worker_children: [None; MAX_IO_WORKERS_USIZE],
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

// C reads/writes its PM-equivalent globals with no aliasing check, so a
// FATAL raised while a `with_pm` borrow is live (e.g. from deep inside
// listen_server_port's lock-file reclaim) and drained synchronously by
// proc_exit's on_proc_exit callbacks can re-enter here on the same thread.
// try_borrow_mut degrades to a no-op in that case instead of panicking the
// exit callback (the process is exiting anyway; the OS reclaims the fds).
pub fn try_with_pm<R>(f: impl FnOnce(&mut PostmasterState) -> R) -> Option<R> {
    PM.with(|pm| pm.try_borrow_mut().ok().map(|mut guard| f(&mut guard)))
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
        procsignal::signums::SIGTERM => {
            PENDING_PM_SHUTDOWN_REQUEST.store(true, Ordering::Release);
        }
        procsignal::signums::SIGINT => {
            PENDING_PM_FAST_SHUTDOWN_REQUEST.store(true, Ordering::Release);
            PENDING_PM_SHUTDOWN_REQUEST.store(true, Ordering::Release);
        }
        procsignal::signums::SIGQUIT => {
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

    // Parked standbys carry a pre-reload GUC snapshot; retire them so the
    // next maintain() respawns from the reloaded state.
    launch_backend::wpool::flush();

    let shutdown = with_pm(|pm| pm.shutdown);
    if shutdown <= SmartShutdown {
        report(
            LOG,
            "received SIGHUP, reloading configuration files".into(),
            2004,
            "process_pm_reload_request",
        );
        guc_file::ProcessConfigFile(types_guc::GucContext::PGC_SIGHUP)?;
        // Publish the post-reload BASE snapshot before children are told to
        // re-read: a child's own reload pass (and any reload-diff it runs)
        // then sees one atomic old-base -> new-base transition
        // (guc::layers, parallelism-redesign §2.4).
        guc::layers::ensure_base_current();
        pmchild_seams::signal_children::call(
            procsignal::signums::SIGHUP,
            btmask_all_except(&[BackendType::DeadEndBackend]),
        );
        if with_pm(|pm| pm.syslogger.is_some()) {
            syslogger::collector_kill(procsignal::signums::SIGHUP);
        }

        if !auth_seams::load_hba::call() {
            // translator: %s is a configuration file (C prints HbaFileName)
            report(
                LOG,
                format!(
                    "{} was not reloaded",
                    guc_tables::vars::HbaFileName.read().unwrap_or_default()
                ),
                2012,
                "process_pm_reload_request",
            );
        }
        if !auth_seams::load_ident::call() {
            report(
                LOG,
                format!(
                    "{} was not reloaded",
                    guc_tables::vars::IdentFileName.read().unwrap_or_default()
                ),
                2016,
                "process_pm_reload_request",
            );
        }

        if guc_tables::vars::EnableSSL.read() {
            if be_secure::secure_initialize(false)? == 0 {
                backend_startup::loaded_ssl::set(true);
            } else {
                report(LOG, "SSL configuration was not reloaded".into(), 2030, "process_pm_reload_request");
            }
        } else {
            be_secure::secure_destroy();
            backend_startup::loaded_ssl::set(false);
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
            pm.lethal_time = 0;
            pm.stall_escalated = false;
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
        bgworker::BackgroundWorkerStateChange(with_pm(|pm| pm.pm_state < PMState::PM_STOP_BACKENDS));
        with_pm(|pm| pm.start_worker_needed = true);
    }

    // signal_child(SIGUSR1) has no route to the slotless logger thread;
    // direct poke (see syslogger::collector_kill).
    if with_pm(|pm| pm.syslogger.is_some()) {
        if syslogger_seams::check_logrotate_signal::call() {
            syslogger::collector_kill(procsignal::signums::SIGUSR1);
            syslogger_seams::remove_logrotate_signal_files::call();
        } else if pmsignal::CheckPostmasterSignal(PMSIGNAL_ROTATE_LOGFILE) {
            syslogger::collector_kill(procsignal::signums::SIGUSR1);
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

    let mut request_state_update = false;

    if pmsignal::CheckPostmasterSignal(PMSIGNAL_XLOG_IS_SHUTDOWN) {
        request_state_update = true;
        if with_pm(|pm| pm.pm_state == PMState::PM_WAIT_XLOG_SHUTDOWN) {
            debug_assert!(with_pm(|pm| pm.shutdown > NoShutdown));
            let pgarch = with_pm(|pm| pm.pgarch);
            if let Some(pgarch) = pgarch {
                statemachine::signal_child(&pgarch, procsignal::signums::SIGUSR2);
            }
            pmchild_seams::signal_children::call(procsignal::signums::SIGUSR2, btmask(BackendType::WalSender));
            statemachine::UpdatePMState(PMState::PM_WAIT_XLOG_ARCHIVAL);
        } else if with_pm(|pm| !pm.fatal_error && pm.shutdown != ImmediateShutdown) {
            report(LOG, "WAL was shut down unexpectedly".into(), 3846, "process_pm_pmsignal");
            statemachine::HandleFatalError(
                pmsignal::QuitSignalReason::PMQUIT_FOR_CRASH,
                false,
            )?;
        }
    }

    if pmsignal::CheckPostmasterSignal(PMSIGNAL_ADVANCE_STATE_MACHINE) {
        request_state_update = true;
    }

    if request_state_update {
        statemachine::PostmasterStateMachine()?;
    }

    // pg_ctl promote: forward SIGUSR2 to the startup process while it is
    // still recovering (postmaster.c:3883-3895); startup unlinks the file.
    if with_pm(|pm| {
        pm.startup.is_some()
            && matches!(
                pm.pm_state,
                PMState::PM_STARTUP | PMState::PM_RECOVERY | PMState::PM_HOT_STANDBY
            )
    }) && xlogrecovery_seams::check_promote_signal::is_installed()
        && xlogrecovery_seams::check_promote_signal::call()
    {
        if let Some(startup) = with_pm(|pm| pm.startup) {
            statemachine::signal_child(&startup, procsignal::signums::SIGUSR2);
        }
    }

    Ok(())
}

// The waitpid channel's thread-model rendering: launch_backend announces a
// finished child thread through postmaster_seams; the queue is the zombie
// table process_pm_child_exit reaps. C wait-status encoding throughout.
static CHILD_EXIT_QUEUE: std::sync::Mutex<Vec<(pid_t, i32)>> = std::sync::Mutex::new(Vec::new());

fn announce_child_exit(pid: pid_t, exitstatus: i32) {
    CHILD_EXIT_QUEUE.lock().unwrap_or_else(|e| e.into_inner()).push((pid, exitstatus));
    handle_pm_child_exit_signal(0);
}

fn reap_one() -> Option<(pid_t, i32)> {
    let head = {
        let mut q = CHILD_EXIT_QUEUE.lock().unwrap_or_else(|e| e.into_inner());
        if q.is_empty() {
            None
        } else {
            Some(q.remove(0))
        }
    };
    // waitpid semantics: the child is reported dead only once its thread is
    // fully gone (TLS destructors included); leaders may free shared state
    // as soon as the bgworker slot clears (parallel worker-teardown race).
    if let Some((pid, _)) = head {
        launch_backend::join_announced_child(pid);
    }
    head
}

fn exit_status_code(exitstatus: i32) -> Option<i32> {
    // WIFEXITED / WEXITSTATUS.
    (exitstatus & 0x7f == 0).then_some((exitstatus >> 8) & 0xff)
}

fn log_child_exit(procname: &str, pid: pid_t, exitstatus: i32) {
    log_child_exit_at(LOG, procname, pid, exitstatus);
}

fn log_child_exit_at(level: types_error::ErrorLevel, procname: &str, pid: pid_t, exitstatus: i32) {
    match exit_status_code(exitstatus) {
        Some(code) => report(
            level,
            format!("{procname} (PID {pid}) exited with exit code {code}"),
            3830,
            "LogChildExit",
        ),
        None => report(
            level,
            format!("{procname} (PID {pid}) was terminated by signal {}", exitstatus & 0x7f),
            3844,
            "LogChildExit",
        ),
    }
}

/// process_pm_child_exit — the SIGCHLD reaper over the thread-exit queue.
pub fn process_pm_child_exit() -> PgResult<()> {
    PENDING_PM_CHILD_EXIT.store(false, Ordering::Release);

    report_internal(DEBUG2, "reaping dead processes".into(), 2240, "process_pm_child_exit");

    while let Some((pid, exitstatus)) = reap_one() {
        let status0 = exit_status_code(exitstatus) == Some(0);
        let status1 = exit_status_code(exitstatus) == Some(1);
        let status3 = exit_status_code(exitstatus) == Some(3);

        if with_pm(|pm| pm.startup.map(|c| c.pid)) == Some(pid) {
            let startup = with_pm(|pm| pm.startup.take()).expect("checked");
            pmchild_seams::release_postmaster_child_slot::call(startup.child_slot);

            if with_pm(|pm| pm.shutdown > NoShutdown) && (status0 || status1) {
                with_pm(|pm| pm.startup_status = StartupStatusEnum::NotRunning);
                statemachine::UpdatePMState(PMState::PM_WAIT_BACKENDS);
                continue;
            }

            if status3 {
                report(LOG, "shutdown at recovery target".into(), 2273, "process_pm_child_exit");
                with_pm(|pm| {
                    pm.startup_status = StartupStatusEnum::NotRunning;
                    pm.shutdown = pm.shutdown.max(SmartShutdown);
                });
                statemachine::TerminateChildren(procsignal::signums::SIGTERM);
                statemachine::UpdatePMState(PMState::PM_WAIT_BACKENDS);
                continue;
            }

            if with_pm(|pm| {
                pm.pm_state == PMState::PM_STARTUP
                    && pm.startup_status != StartupStatusEnum::Signaled
            }) && !status0
            {
                log_child_exit("startup process", pid, exitstatus);
                report(
                    LOG,
                    "aborting startup due to startup process failure".into(),
                    2292,
                    "process_pm_child_exit",
                );
                statemachine::ExitPostmaster(1);
            }

            if !status0 {
                if with_pm(|pm| pm.startup_status == StartupStatusEnum::Signaled) {
                    with_pm(|pm| pm.startup_status = StartupStatusEnum::NotRunning);
                    if with_pm(|pm| pm.pm_state == PMState::PM_STARTUP) {
                        statemachine::UpdatePMState(PMState::PM_WAIT_BACKENDS);
                    }
                } else {
                    with_pm(|pm| pm.startup_status = StartupStatusEnum::Crashed);
                }
                handle_child_crash("startup process", pid, exitstatus)?;
                continue;
            }

            with_pm(|pm| {
                pm.startup_status = StartupStatusEnum::NotRunning;
                pm.fatal_error = false;
                pm.abort_start_time = 0;
                pm.lethal_time = 0;
                pm.stall_escalated = false;
            });
            statemachine::UpdatePMState(PMState::PM_RUN);
            with_pm(|pm| pm.conns_allowed = true);

            with_pm(|pm| pm.start_worker_needed = true);

            report(
                LOG,
                "database system is ready to accept connections".into(),
                2345,
                "process_pm_child_exit",
            );
            miscinit::AddToDataDirLockFile(LOCK_FILE_LINE_PM_STATUS, PM_STATUS_READY)?;
            continue;
        }

        if with_pm(|pm| pm.bgwriter.map(|c| c.pid)) == Some(pid) {
            let bgwriter = with_pm(|pm| pm.bgwriter.take()).expect("checked");
            pmchild_seams::release_postmaster_child_slot::call(bgwriter.child_slot);
            if !status0 {
                handle_child_crash("background writer process", pid, exitstatus)?;
            }
            continue;
        }

        if with_pm(|pm| pm.checkpointer.map(|c| c.pid)) == Some(pid) {
            let checkpointer = with_pm(|pm| pm.checkpointer.take()).expect("checked");
            pmchild_seams::release_postmaster_child_slot::call(checkpointer.child_slot);
            if status0 && with_pm(|pm| pm.pm_state == PMState::PM_WAIT_CHECKPOINTER) {
                statemachine::UpdatePMState(PMState::PM_WAIT_DEAD_END);
                serverloop::ConfigurePostmasterWaitSet(false)?;
                statemachine::SignalChildren(
                    procsignal::signums::SIGTERM,
                    btmask_all_except(&[BackendType::Logger]),
                );
            } else {
                handle_child_crash("checkpointer process", pid, exitstatus)?;
            }
            continue;
        }

        if with_pm(|pm| pm.walwriter.map(|c| c.pid)) == Some(pid) {
            let walwriter = with_pm(|pm| pm.walwriter.take()).expect("checked");
            pmchild_seams::release_postmaster_child_slot::call(walwriter.child_slot);
            if !status0 {
                handle_child_crash("WAL writer process", pid, exitstatus)?;
            }
            continue;
        }

        if with_pm(|pm| pm.autovac_launcher.map(|c| c.pid)) == Some(pid) {
            let launcher = with_pm(|pm| pm.autovac_launcher.take()).expect("checked");
            pmchild_seams::release_postmaster_child_slot::call(launcher.child_slot);
            if !status0 {
                handle_child_crash("autovacuum launcher process", pid, exitstatus)?;
            }
            continue;
        }

        if with_pm(|pm| pm.walsummarizer.map(|c| c.pid)) == Some(pid) {
            let ws = with_pm(|pm| pm.walsummarizer.take()).expect("checked");
            pmchild_seams::release_postmaster_child_slot::call(ws.child_slot);
            if !status0 {
                handle_child_crash("WAL summarizer process", pid, exitstatus)?;
            }
            continue;
        }

        // C treats walreceiver exit status 0 or 1 as normal (FATAL exit = 1):
        // the startup process re-requests one when it still wants streaming.
        if with_pm(|pm| pm.walreceiver.map(|c| c.pid)) == Some(pid) {
            let wr = with_pm(|pm| pm.walreceiver.take()).expect("checked");
            pmchild_seams::release_postmaster_child_slot::call(wr.child_slot);
            if !(status0 || status1) {
                handle_child_crash("WAL receiver process", pid, exitstatus)?;
            }
            continue;
        }

        // C treats archiver exit status 0 or 1 as normal (FATAL exit = 1):
        // the main loop relaunches it to retry archiving remaining files.
        if with_pm(|pm| pm.pgarch.map(|c| c.pid)) == Some(pid) {
            let pgarch_child = with_pm(|pm| pm.pgarch.take()).expect("checked");
            pmchild_seams::release_postmaster_child_slot::call(pgarch_child.child_slot);
            if !(status0 || status1) {
                handle_child_crash("archiver process", pid, exitstatus)?;
            }
            continue;
        }

        // C process_pm_child_exit slot sync worker arm: status 0/1 is a
        // normal stop (config change, promotion); the main loop relaunches
        // it after SLOTSYNC_RESTART_INTERVAL_SEC when still applicable.
        if with_pm(|pm| pm.slotsync_worker.map(|c| c.pid)) == Some(pid) {
            let ss = with_pm(|pm| pm.slotsync_worker.take()).expect("checked");
            pmchild_seams::release_postmaster_child_slot::call(ss.child_slot);
            if !(status0 || status1) {
                handle_child_crash("slot sync worker process", pid, exitstatus)?;
            }
            continue;
        }

        if with_pm(|pm| pm.syslogger.map(|c| c.pid)) == Some(pid) {
            let logger = with_pm(|pm| pm.syslogger.take()).expect("checked");
            pmchild_seams::release_postmaster_child_slot::call(logger.child_slot);
            // C: for safety's sake, launch new logger *first*.
            if guc_tables::vars::Logging_collector.read() {
                statemachine::StartSysLogger();
            }
            if !status0 {
                log_child_exit("system logger process", pid, exitstatus);
            }
            continue;
        }

        match pmchild_seams::find_postmaster_child_by_pid::call(pid) {
            // C's CleanupBackend also owns autovacuum workers (B_AUTOVAC_WORKER
            // rides the backend list) and walsenders (B_WAL_SENDER — a backend
            // that switched type at START_REPLICATION; CleanupBackend treats it
            // exactly like a plain backend).
            Some((child_slot, btype))
                if matches!(
                    btype,
                    BackendType::Backend
                        | BackendType::DeadEndBackend
                        | BackendType::AutovacWorker
                        | BackendType::WalSender
                ) =>
            {
                pmchild_seams::release_postmaster_child_slot::call(child_slot);
                if !(status0 || status1) {
                    handle_child_crash("server process", pid, exitstatus)?;
                } else {
                    bgworker::BackgroundWorkerStopNotifications(pid);
                    // CleanupBackend's trailing LogChildExit(DEBUG2) — the TAP
                    // harness (Cluster.pm connect_ok/fails) waits on this line.
                    log_child_exit_at(
                        DEBUG2,
                        miscinit::GetBackendTypeDesc(btype),
                        pid,
                        exitstatus,
                    );
                }
            }
            Some((child_slot, BackendType::BgWorker)) => {
                // CleanupBackend's B_BG_WORKER half.
                let rw = bgworker::find_registered_worker_by_pid(pid);
                let procname = match rw {
                    Some(idx) => format!("background worker \"{}\"", bgworker::rw_type(idx)),
                    None => "background worker".to_string(),
                };
                pmchild_seams::release_postmaster_child_slot::call(child_slot);
                if !(status0 || status1) {
                    handle_child_crash(&procname, pid, exitstatus)?;
                } else {
                    bgworker::BackgroundWorkerStopNotifications(pid);
                    if let Some(idx) = rw {
                        if !status0 {
                            bgworker::set_rw_crashed_at(
                                idx,
                                timestamp_seams::get_current_timestamp::call(),
                            );
                        } else {
                            bgworker::set_rw_crashed_at(idx, 0);
                            bgworker::set_rw_terminate(idx, true);
                        }
                        bgworker::set_rw_pid(idx, 0);
                        bgworker::ReportBackgroundWorkerExit(idx);
                        log_child_exit_at(
                            if status0 { DEBUG1 } else { LOG },
                            &procname,
                            pid,
                            exitstatus,
                        );
                        with_pm(|pm| pm.have_crashed_worker = true);
                    }
                }
            }
            // C's "Was it an IO worker?" arm (maybe_reap_io_worker): exit 0/1
            // is normal; the liveness term (pm.io_worker_count) decrements
            // here and the pool is re-leveled, mirroring C's
            // maybe_adjust_io_workers-after-reap.
            Some((child_slot, BackendType::IoWorker)) => {
                pmchild_seams::release_postmaster_child_slot::call(child_slot);
                // maybe_reap_io_worker: free the pm slot + drop the count.
                with_pm(|pm| {
                    if let Some(i) = pm
                        .io_worker_children
                        .iter()
                        .position(|c| c.map(|c| c.pid) == Some(pid))
                    {
                        pm.io_worker_children[i] = None;
                    }
                    pm.io_worker_count -= 1;
                });
                if !(status0 || status1) {
                    handle_child_crash("io worker", pid, exitstatus)?;
                } else {
                    log_child_exit_at(DEBUG2, "io worker", pid, exitstatus);
                }
                serverloop::maybe_adjust_io_workers();
            }
            Some((_slot, btype)) => panic!(
                "process_pm_child_exit: reaper arm for {} unported (its owner extends the reaper when its main lands)",
                miscinit::GetBackendTypeDesc(btype)
            ),
            None => {
                log_child_exit("untracked child process", pid, exitstatus);
            }
        }
    }

    statemachine::PostmasterStateMachine()
}

/// HandleChildCrash. Covers the catchable crash class only (caught panics);
/// memory-safety violations are process-fatal by design
/// (notes/crash-restart-design.md).
fn handle_child_crash(procname: &str, pid: pid_t, exitstatus: i32) -> PgResult<()> {
    if with_pm(|pm| pm.fatal_error || pm.shutdown == ImmediateShutdown) {
        return Ok(());
    }

    log_child_exit(procname, pid, exitstatus);
    report(
        LOG,
        "terminating any other active server processes".into(),
        2804,
        "HandleChildCrash",
    );

    statemachine::HandleFatalError(pmsignal::QuitSignalReason::PMQUIT_FOR_CRASH, true)
}

/// pm_service_pending seam impl (SIMCORPUS): one postmaster service quantum
/// for harness boot threads that play the postmaster — the ServerLoop slice
/// a parallel-query corpus needs, in ServerLoop's order: child exits, then
/// the BACKGROUND_WORKER_CHANGE pmsignal arm, then deferred bgworker starts
/// (LaunchMissingBackgroundProcesses' maybe_start half). On the postmaster's
/// OWN thread-local PM state; with no shutdown/crash in flight the trailing
/// PostmasterStateMachine inside process_pm_child_exit is state-gated to a
/// no-op (PM_INIT), and maybe_start_bgworkers walks only pid-0 registered
/// entries — so this is exactly the ServerLoop's service arms and nothing
/// else.
fn pm_service_pending() {
    if PENDING_PM_CHILD_EXIT.load(Ordering::Acquire) {
        process_pm_child_exit().unwrap_or_else(|e| panic!("pm_service_pending: {e:?}"));
    }
    if PENDING_PM_PMSIGNAL.swap(false, Ordering::AcqRel)
        && pmsignal::CheckPostmasterSignal(pmsignal::PMSignalReason::PMSIGNAL_BACKGROUND_WORKER_CHANGE)
    {
        bgworker::BackgroundWorkerStateChange(with_pm(|pm| {
            pm.pm_state < PMState::PM_STOP_BACKENDS
        }));
        with_pm(|pm| pm.start_worker_needed = true);
    }
    if with_pm(|pm| pm.start_worker_needed || pm.have_crashed_worker) {
        statemachine::maybe_start_bgworkers();
    }
}

/// pm_promote_run seam impl (DST-MULTIBACKEND): the startup-exit promotion
/// for harness boot threads that play the postmaster — the arm of
/// process_pm_child_exit's startup case a PM_INIT surrogate never runs
/// (the surrogate owns recovery inside session 1's InitPostgres, so
/// "startup finished" is its state by construction once that session
/// completes). PostmasterMain parity first: claim the postmaster
/// environment (main_entry.rs sets it before any child spawns; the
/// standalone boot ladder never does), thread-local like every "C
/// per-process global" — session 1 already ran, and children capture it
/// at spawn through `Inherited`. Then the startup-exit trio: PM_RUN,
/// connections allowed (PostmasterStateMachine treats PM_RUN with
/// conns_allowed=false as the smart-shutdown STOP_BACKENDS trigger), and
/// the deferred-bgworker sweep request. With this state,
/// pm_service_pending's maybe_start_bgworkers arm can actually START a
/// pool-miss deferral (bgworker_should_start_now: ConsistentState needs
/// PM_RUN) through postmaster_child_launch (which asserts
/// IsPostmasterEnvironment) — the simcorpus §7 named boundary, closed.
fn pm_promote_run() {
    init_small::globals::SetIsPostmasterEnvironment(true);
    statemachine::UpdatePMState(PMState::PM_RUN);
    with_pm(|pm| {
        pm.conns_allowed = true;
        pm.start_worker_needed = true;
    });
}

pub fn init_seams() {
    postmaster_seams::announce_child_exit::set(announce_child_exit);
    postmaster_seams::bgworker_shmem_init::set(bgworker::BackgroundWorkerShmemInit);
    postmaster_seams::pm_promote_run::set(pm_promote_run);
    postmaster_seams::pm_service_pending::set(pm_service_pending);
    postmaster_seams::signal_postmaster_sigusr1::set(|| handle_pm_pmsignal_signal(procsignal::signums::SIGUSR1));
    postmaster_seams::signal_postmaster_sighup::set(|| handle_pm_reload_request_signal(procsignal::signums::SIGHUP));
    postmaster_seams::pg_start_time::set(main_entry::pg_start_time);
    postmaster_seams::set_pg_start_time::set(main_entry::set_pg_start_time);
}
