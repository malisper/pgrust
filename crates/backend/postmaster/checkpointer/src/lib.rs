//! checkpointer.c. Thread-model divergences: the shmem control block is a
//! process static; per-thread signal dispositions are not installed (the
//! postmaster owns process handlers) — the C SIGINT handler body is exported
//! as [`ReqShutdownXLOG`] for the per-thread delivery design (pmchild);
//! start_cv/done_cv storage belongs to the condition_variable unit, reached
//! through its seams (broadcasts skip while uninstalled: nothing can sleep).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering::Relaxed};
use std::sync::OnceLock;

use condition_variable_seams::CheckpointerCv;
use elog::{elog, ereport};
use init_small::globals as g;
use lwlock::{LWLockAcquire, LWLockRelease, LW_EXCLUSIVE};
use transam_xlog::{
    RecoveryInProgress, CHECKPOINT_CAUSE_TIME, CHECKPOINT_CAUSE_XLOG, CHECKPOINT_END_OF_RECOVERY,
    CHECKPOINT_IMMEDIATE, CHECKPOINT_REQUESTED, CHECKPOINT_WAIT,
};
use types_core::{XLogRecPtr, INVALID_PROC_NUMBER};
use types_error::{ErrorLocation, PgError, PgResult, DEBUG1, DEBUG2, ERROR, LOG};
use types_startup::StartupData;
use types_storage::storage::Spinlock;
use types_storage::sync::{FileTag, SyncRequestType};
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};
use types_storage::SyncCell;

#[cfg(test)]
mod tests;

const WRITES_PER_ABSORB: i32 = 1000;
const MAX_CHECKPOINT_REQUESTS: i32 = 10_000_000;
const MAX_SIGNAL_TRIES: i32 = 600;

// wait_event_names.txt ordering (Activity / IPC / Timeout sections).
const PG_WAIT_ACTIVITY: u32 = 0x0500_0000;
const PG_WAIT_IPC: u32 = 0x0800_0000;
const PG_WAIT_TIMEOUT: u32 = 0x0900_0000;
const WAIT_EVENT_CHECKPOINTER_MAIN: u32 = PG_WAIT_ACTIVITY + 4;
const WAIT_EVENT_CHECKPOINTER_SHUTDOWN: u32 = PG_WAIT_ACTIVITY + 5;
const WAIT_EVENT_CHECKPOINT_DONE: u32 = PG_WAIT_IPC + 11;
const WAIT_EVENT_CHECKPOINT_START: u32 = PG_WAIT_IPC + 12;
const WAIT_EVENT_CHECKPOINT_WRITE_DELAY: u32 = PG_WAIT_TIMEOUT + 1;

// CheckpointerCommLock offset in lwlocklist.h order (name pinned by test).
pub(crate) const CHECKPOINTER_COMM_LOCK_OFFSET: usize = 17;

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

thread_local! {
    static ABSORB_SCRATCH: std::cell::RefCell<Option<mcx::PgVec<'static, CheckpointerRequest>>> =
        const { std::cell::RefCell::new(None) };
    static CHECK_POINT_TIMEOUT: Cell<i32> = const { Cell::new(300) };
    static CHECK_POINT_WARNING: Cell<i32> = const { Cell::new(30) };

    static CKPT_ACTIVE: Cell<bool> = const { Cell::new(false) };
    static CKPT_START_TIME: Cell<i64> = const { Cell::new(0) };
    static CKPT_START_RECPTR: Cell<XLogRecPtr> = const { Cell::new(0) };
    static CKPT_CACHED_ELAPSED: Cell<f64> = const { Cell::new(0.0) };
    static LAST_CHECKPOINT_TIME: Cell<i64> = const { Cell::new(0) };
    static LAST_XLOG_SWITCH_TIME: Cell<i64> = const { Cell::new(0) };
    static ABSORB_COUNTER: Cell<i32> = const { Cell::new(WRITES_PER_ABSORB) };
    static FIRST_CALL_CKPT_DONE: Cell<i32> = const { Cell::new(0) };
}

// C `volatile sig_atomic_t ShutdownXLOGPending`; the per-thread delivery
// design may run the handler off-thread, hence a process atomic.
static SHUTDOWN_XLOG_PENDING: AtomicBool = AtomicBool::new(false);

pub fn CheckPointTimeout() -> i32 {
    CHECK_POINT_TIMEOUT.get()
}

// PendingCheckpointerStats bumps (C writes the pgstat_checkpointer.c extern
// struct directly); the counts land in pgstat's pending struct so
// pgstat_report_checkpointer flushes them.
mod pending_stats {
    macro_rules! bump_via_seam {
        ($name:ident) => {
            pub fn $name() {
                if pgstat_seams::$name::is_installed() {
                    pgstat_seams::$name::call();
                }
            }
        };
    }
    bump_via_seam!(pgstat_count_checkpointer_num_timed);
    bump_via_seam!(pgstat_count_checkpointer_num_requested);
    bump_via_seam!(pgstat_count_checkpointer_num_performed);
    bump_via_seam!(pgstat_count_checkpointer_restartpoints_timed);
    bump_via_seam!(pgstat_count_checkpointer_restartpoints_requested);
    bump_via_seam!(pgstat_count_checkpointer_restartpoints_performed);
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct CheckpointerRequest {
    req_type: SyncRequestType,
    ftag: FileTag,
}

struct CheckpointerShmemStruct {
    checkpointer_pid: AtomicI32,
    ckpt_lck: Spinlock,
    // ckpt_* mutations under ckpt_lck; unlocked ckpt_flags reads mirror C's
    // volatile single-word peeks.
    ckpt_started: AtomicI32,
    ckpt_done: AtomicI32,
    ckpt_failed: AtomicI32,
    ckpt_flags: AtomicI32,
    // num_requests/requests[] guarded by CheckpointerCommLock.
    num_requests: SyncCell<i32>,
    max_requests: i32,
    requests: &'static [SyncCell<CheckpointerRequest>],
}

static CHECKPOINTER_SHMEM: OnceLock<CheckpointerShmemStruct> = OnceLock::new();

fn shmem() -> &'static CheckpointerShmemStruct {
    CHECKPOINTER_SHMEM
        .get()
        .expect("CheckpointerShmem accessed before CheckpointerShmemInit")
}

fn spin_acquire(lock: &Spinlock) {
    if lock.tas() != 0 {
        let mut delay = s_lock_seams::SpinDelayStatus::new(file!(), line!() as i32, "ckpt_lck");
        while lock.tas_spin() != 0 {
            s_lock_seams::perform_spin_delay::call(&mut delay);
        }
        s_lock_seams::finish_spin_delay::call(&delay);
    }
}

fn with_ckpt_lck<R>(f: impl FnOnce(&CheckpointerShmemStruct) -> R) -> R {
    let cp = shmem();
    spin_acquire(&cp.ckpt_lck);
    let r = f(cp);
    cp.ckpt_lck.unlock();
    r
}

fn checkpointer_comm_lock() -> &'static lwlock::LWLock {
    lwlock::main_lock(CHECKPOINTER_COMM_LOCK_OFFSET)
}

pub fn CheckpointerShmemSize(nbuffers: i32) -> usize {
    let n = nbuffers.min(MAX_CHECKPOINT_REQUESTS).max(0) as usize;
    core::mem::size_of::<CheckpointerShmemStruct>()
        + n * core::mem::size_of::<SyncCell<CheckpointerRequest>>()
}

pub fn CheckpointerShmemInit(nbuffers: i32) {
    let max_requests = nbuffers.min(MAX_CHECKPOINT_REQUESTS).max(0);
    const {
        assert!(!core::mem::needs_drop::<SyncCell<CheckpointerRequest>>());
    }
    let zero = CheckpointerRequest {
        req_type: SyncRequestType::SYNC_REQUEST,
        ftag: FileTag::default(),
    };
    let requests: &'static [SyncCell<CheckpointerRequest>] = (0..max_requests)
        .map(|_| SyncCell::new(zero))
        .collect::<Vec<_>>()
        .leak();
    CHECKPOINTER_SHMEM
        .set(CheckpointerShmemStruct {
            checkpointer_pid: AtomicI32::new(0),
            ckpt_lck: Spinlock::new(),
            ckpt_started: AtomicI32::new(0),
            ckpt_done: AtomicI32::new(0),
            ckpt_failed: AtomicI32::new(0),
            ckpt_flags: AtomicI32::new(0),
            num_requests: SyncCell::new(0),
            max_requests,
            requests,
        })
        .unwrap_or_else(|_| panic!("CheckpointerShmemInit called twice"));
}

/// Crash-cycle reset in place to the CheckpointerShmemInit boot image
/// (notes/crash-restart-design.md); every child is dead, so the postmaster
/// thread has exclusive access. SHUTDOWN_XLOG_PENDING is a process static
/// under the thread model (C: per-process, reborn on fork) — reset with it.
pub fn CheckpointerShmemResetAfterCrash() {
    let cp = shmem();
    cp.ckpt_lck.unlock();
    cp.checkpointer_pid.store(0, Relaxed);
    cp.ckpt_started.store(0, Relaxed);
    cp.ckpt_done.store(0, Relaxed);
    cp.ckpt_failed.store(0, Relaxed);
    cp.ckpt_flags.store(0, Relaxed);
    cp.num_requests.set(0);
    let zero = CheckpointerRequest {
        req_type: SyncRequestType::SYNC_REQUEST,
        ftag: FileTag::default(),
    };
    for r in cp.requests {
        r.set(zero);
    }
    if condition_variable_seams::checkpointer_cvs_reset_after_crash::is_installed() {
        condition_variable_seams::checkpointer_cvs_reset_after_crash::call();
    }
    SHUTDOWN_XLOG_PENDING.store(false, Relaxed);
}

fn time_now() -> i64 {
    // DST P2 (contract §1.2): SystemTime -> pg_clock::wall_secs().
    pg_clock::wall_secs()
}

fn am_checkpointer() -> bool {
    miscinit::GetMyBackendType() == types_core::BackendType::Checkpointer
}

/// The C SIGINT handler body; the per-thread signal delivery design (pmchild)
/// is its caller.
pub fn ReqShutdownXLOG() {
    SHUTDOWN_XLOG_PENDING.store(true, Relaxed);
    if let Some(l) = g::MyLatch() {
        latch::SetLatch(l);
    }
}

fn fatal_exit(e: &PgError) -> ! {
    elog::emit_error_report_for(e);
    ipc::proc_exit(1, g::MyProcPid())
}

pub fn CheckpointerMain(startup_data: &StartupData) -> ! {
    debug_assert!(matches!(startup_data, StartupData::None));

    miscinit::SetMyBackendType(types_core::BackendType::Checkpointer);
    if let Err(e) = auxprocess::AuxiliaryProcessMainCommon() {
        fatal_exit(&e);
    }

    {
        use procsignal::ThreadSignalHandler::{Ignore, Simple};
        procsignal::pqsignal_thread(procsignal::signums::SIGHUP, Simple(interrupt::SignalHandlerForConfigReload));
        procsignal::pqsignal_thread(procsignal::signums::SIGINT, Simple(ReqShutdownXLOG));
        procsignal::pqsignal_thread(procsignal::signums::SIGTERM, Ignore);
        procsignal::pqsignal_thread(procsignal::signums::SIGALRM, Ignore);
        procsignal::pqsignal_thread(procsignal::signums::SIGPIPE, Ignore);
        procsignal::pqsignal_thread(
            procsignal::signums::SIGUSR2,
            Simple(interrupt::SignalHandlerForShutdownRequest),
        );
    }

    shmem().checkpointer_pid.store(g::MyProcPid(), Relaxed);

    let now0 = time_now();
    LAST_CHECKPOINT_TIME.set(now0);
    LAST_XLOG_SWITCH_TIME.set(now0);

    // pgstat unit unported => no stats subsystem exists to flush at shutdown;
    // the uninstalled skip is that empty flush.
    if pgstat_seams::pgstat_before_server_shutdown::is_installed() {
        ipc::before_shmem_exit(pgstat_before_server_shutdown_cb, datum::Datum::null())
            .unwrap_or_else(|e| fatal_exit(&e));
    }

    // The "Checkpointer" work context: nothing below allocates per-cycle, so
    // no dedicated arena is needed (C resets it only on error recovery).

    libpq_pqsignal::unblock_signals();

    if let Err(e) = UpdateSharedMemoryConfig() {
        fatal_exit(&e);
    }

    lmgr_proc::ProcGlobal()
        .checkpointerProc
        .store(g::MyProcNumber(), Relaxed);

    // sigsetjmp(PG_exception_stack) equivalent: an Err escaping the cycle
    // runs the minimal-abort cleanup, then re-enters after 1 s.
    loop {
        match checkpointer_main_loop() {
            Ok(()) => break,
            Err(err) => {
                abort_cleanup(&err);
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
    }

    elog::config::set_exit_on_any_error(true);

    if SHUTDOWN_XLOG_PENDING.load(Relaxed) {
        pending_stats::pgstat_count_checkpointer_num_requested();
        transam_xlog::ShutdownXLOG().unwrap_or_else(|e| fatal_exit(&e));
        report_checkpointer_stats();
        report_wal_stats(true);

        pmsignal::SendPostmasterSignal(pmsignal::PMSignalReason::PMSIGNAL_XLOG_IS_SHUTDOWN);
        SHUTDOWN_XLOG_PENDING.store(false, Relaxed);
    }

    loop {
        if let Some(l) = g::MyLatch() {
            latch::ResetLatch(l);
        }

        if let Err(e) = ProcessCheckpointerInterrupts() {
            fatal_exit(&e);
        }

        if interrupt::ShutdownRequestPending() {
            break;
        }

        let _ = latch::WaitLatch(
            g::MyLatch(),
            WL_LATCH_SET | WL_EXIT_ON_PM_DEATH,
            0,
            WAIT_EVENT_CHECKPOINTER_SHUTDOWN,
        );
    }

    ipc::proc_exit(0, g::MyProcPid())
}

fn pgstat_before_server_shutdown_cb(code: i32, _arg: datum::Datum) -> PgResult<()> {
    pgstat_seams::pgstat_before_server_shutdown::call(code)
}

fn broadcast(cv: CheckpointerCv) {
    if condition_variable_seams::checkpointer_cv_broadcast::is_installed() {
        condition_variable_seams::checkpointer_cv_broadcast::call(cv);
    }
}

fn abort_cleanup(err: &PgError) {
    g::SetInterruptHoldoffCount(0);
    g::SetCritSectionCount(0);
    g::HoldInterrupts();

    elog::emit_error_report_for(err);

    let _ = lwlock::LWLockReleaseAll();
    if condition_variable_seams::condition_variable_cancel_sleep::is_installed() {
        condition_variable_seams::condition_variable_cancel_sleep::call();
    }
    waitevent_seams::pgstat_report_wait_end::call();
    if aio_seams::pgaio_error_cleanup::is_installed() {
        aio_seams::pgaio_error_cleanup::call();
    }
    bufmgr::UnlockBuffers();
    let _ = resowner::ReleaseAuxProcessResources(false);
    bufmgr::AtEOXact_Buffers(false);
    let _ = smgr::AtEOXact_SMgr();
    let _ = fd::AtEOXact_Files(false);
    dynahash::AtEOXact_HashTables(false);

    if CKPT_ACTIVE.get() {
        with_ckpt_lck(|cp| {
            cp.ckpt_failed.fetch_add(1, Relaxed);
            cp.ckpt_done.store(cp.ckpt_started.load(Relaxed), Relaxed);
        });
        broadcast(CheckpointerCv::Done);
        CKPT_ACTIVE.set(false);
    }

    elog::FlushErrorState();
    g::ResumeInterrupts();
}

fn checkpointer_main_loop() -> PgResult<()> {
    loop {
        let mut do_checkpoint = false;
        let mut flags = 0i32;
        let mut chkpt_or_rstpt_requested = false;
        let mut chkpt_or_rstpt_timed = false;

        if let Some(l) = g::MyLatch() {
            latch::ResetLatch(l);
        }

        AbsorbSyncRequests()?;

        ProcessCheckpointerInterrupts()?;
        if SHUTDOWN_XLOG_PENDING.load(Relaxed) || interrupt::ShutdownRequestPending() {
            return Ok(());
        }

        if shmem().ckpt_flags.load(Relaxed) != 0 {
            do_checkpoint = true;
            chkpt_or_rstpt_requested = true;
        }

        let mut now = time_now();
        let mut elapsed_secs = now - LAST_CHECKPOINT_TIME.get();
        if elapsed_secs >= CheckPointTimeout() as i64 {
            if !do_checkpoint {
                chkpt_or_rstpt_timed = true;
            }
            do_checkpoint = true;
            flags |= CHECKPOINT_CAUSE_TIME;
        }

        if do_checkpoint {
            let mut do_restartpoint = RecoveryInProgress();

            with_ckpt_lck(|cp| {
                flags |= cp.ckpt_flags.load(Relaxed);
                cp.ckpt_flags.store(0, Relaxed);
                cp.ckpt_started.fetch_add(1, Relaxed);
            });
            broadcast(CheckpointerCv::Start);

            if flags & CHECKPOINT_END_OF_RECOVERY != 0 {
                do_restartpoint = false;
            }

            if chkpt_or_rstpt_timed {
                if do_restartpoint {
                    pending_stats::pgstat_count_checkpointer_restartpoints_timed();
                } else {
                    pending_stats::pgstat_count_checkpointer_num_timed();
                }
            }
            if chkpt_or_rstpt_requested {
                if do_restartpoint {
                    pending_stats::pgstat_count_checkpointer_restartpoints_requested();
                } else {
                    pending_stats::pgstat_count_checkpointer_num_requested();
                }
            }

            if !do_restartpoint
                && flags & CHECKPOINT_CAUSE_XLOG != 0
                && elapsed_secs < CHECK_POINT_WARNING.get() as i64
            {
                let plural = if elapsed_secs == 1 { "second" } else { "seconds" };
                ereport(LOG)
                    .errmsg(format!(
                        "checkpoints are occurring too frequently ({elapsed_secs} {plural} apart)"
                    ))
                    .errhint("Consider increasing the configuration parameter \"max_wal_size\".")
                    .finish(loc("CheckpointerMain"))?;
            }

            CKPT_ACTIVE.set(true);
            CKPT_START_RECPTR.set(if do_restartpoint {
                xlogrecovery_seams::get_xlog_replay_rec_ptr::call().0
            } else {
                transam_xlog::GetInsertRecPtr()
            });
            CKPT_START_TIME.set(now);
            CKPT_CACHED_ELAPSED.set(0.0);

            let ckpt_performed = if !do_restartpoint {
                transam_xlog::CreateCheckPoint(flags)?
            } else {
                transam_xlog::CreateRestartPoint(flags)?
            };

            smgr::smgrdestroyall()?;

            with_ckpt_lck(|cp| {
                cp.ckpt_done.store(cp.ckpt_started.load(Relaxed), Relaxed);
            });
            broadcast(CheckpointerCv::Done);

            if !do_restartpoint {
                LAST_CHECKPOINT_TIME.set(now);
                if ckpt_performed {
                    pending_stats::pgstat_count_checkpointer_num_performed();
                }
            } else if ckpt_performed {
                LAST_CHECKPOINT_TIME.set(now);
                pending_stats::pgstat_count_checkpointer_restartpoints_performed();
            } else {
                LAST_CHECKPOINT_TIME.set(now - CheckPointTimeout() as i64 + 15);
            }

            CKPT_ACTIVE.set(false);

            ProcessCheckpointerInterrupts()?;
            if SHUTDOWN_XLOG_PENDING.load(Relaxed) || interrupt::ShutdownRequestPending() {
                return Ok(());
            }
        }

        CheckArchiveTimeout()?;

        report_checkpointer_stats();
        report_wal_stats(true);

        if shmem().ckpt_flags.load(Relaxed) != 0 {
            continue;
        }

        now = time_now();
        elapsed_secs = now - LAST_CHECKPOINT_TIME.get();
        if elapsed_secs >= CheckPointTimeout() as i64 {
            continue;
        }
        let mut cur_timeout = CheckPointTimeout() as i64 - elapsed_secs;
        let archive_timeout = guc_tables::vars::XLogArchiveTimeout.read();
        if archive_timeout > 0 && !RecoveryInProgress() {
            elapsed_secs = now - LAST_XLOG_SWITCH_TIME.get();
            if elapsed_secs >= archive_timeout as i64 {
                continue;
            }
            cur_timeout = cur_timeout.min(archive_timeout as i64 - elapsed_secs);
        }

        latch::WaitLatch(
            g::MyLatch(),
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            cur_timeout * 1000,
            WAIT_EVENT_CHECKPOINTER_MAIN,
        )?;
    }
}

pub fn ProcessCheckpointerInterrupts() -> PgResult<()> {
    if procsignal_seams::proc_signal_barrier_pending::call() {
        procsignal_seams::process_proc_signal_barrier::call()?;
    }

    if interrupt::ConfigReloadPending() {
        interrupt::SetConfigReloadPending(false);
        guc_file::ProcessConfigFile(types_guc::GucContext::PGC_SIGHUP)?;
        UpdateSharedMemoryConfig()?;
    }

    // Flag owner (mcxt.c half) unported => the flag can never be set; the
    // uninstalled skip is C's false arm.
    if mcxt_seams::log_memory_context_pending::is_installed()
        && mcxt_seams::log_memory_context_pending::call()
    {
        mcxt_seams::process_log_memory_context_interrupt::call()?;
    }
    Ok(())
}

fn CheckArchiveTimeout() -> PgResult<()> {
    let archive_timeout = guc_tables::vars::XLogArchiveTimeout.read();
    if archive_timeout <= 0 || RecoveryInProgress() {
        return Ok(());
    }

    let now = time_now();
    if now - LAST_XLOG_SWITCH_TIME.get() < archive_timeout as i64 {
        return Ok(());
    }

    let (last_time, last_switch_lsn) = transam_xlog::GetLastSegSwitchData();
    LAST_XLOG_SWITCH_TIME.set(LAST_XLOG_SWITCH_TIME.get().max(last_time));

    if now - LAST_XLOG_SWITCH_TIME.get() >= archive_timeout as i64 {
        if transam_xlog::GetLastImportantRecPtr() > last_switch_lsn {
            let switchpoint = transam_xlog::RequestXLogSwitch(false)?;
            if transam_xlog::XLogSegmentOffset(switchpoint, transam_xlog::wal_segment_size()) != 0 {
                elog(
                    DEBUG1,
                    format!(
                        "write-ahead log switch forced (\"archive_timeout\"={archive_timeout})"
                    ),
                )?;
            }
        }
        LAST_XLOG_SWITCH_TIME.set(now);
    }
    Ok(())
}

fn ImmediateCheckpointRequested() -> bool {
    shmem().ckpt_flags.load(Relaxed) & CHECKPOINT_IMMEDIATE != 0
}

pub fn CheckpointWriteDelay(flags: i32, progress: f64) -> PgResult<()> {
    if !am_checkpointer() {
        return Ok(());
    }

    if flags & CHECKPOINT_IMMEDIATE == 0
        && !SHUTDOWN_XLOG_PENDING.load(Relaxed)
        && !interrupt::ShutdownRequestPending()
        && !ImmediateCheckpointRequested()
        && IsCheckpointOnSchedule(progress)
    {
        if interrupt::ConfigReloadPending() {
            interrupt::SetConfigReloadPending(false);
            guc_file::ProcessConfigFile(types_guc::GucContext::PGC_SIGHUP)?;
            UpdateSharedMemoryConfig()?;
        }

        AbsorbSyncRequests()?;
        ABSORB_COUNTER.set(WRITES_PER_ABSORB);

        CheckArchiveTimeout()?;

        report_checkpointer_stats();

        latch::WaitLatch(
            g::MyLatch(),
            WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_TIMEOUT,
            100,
            WAIT_EVENT_CHECKPOINT_WRITE_DELAY,
        )?;
        if let Some(l) = g::MyLatch() {
            latch::ResetLatch(l);
        }
    } else {
        ABSORB_COUNTER.set(ABSORB_COUNTER.get() - 1);
        if ABSORB_COUNTER.get() <= 0 {
            AbsorbSyncRequests()?;
            ABSORB_COUNTER.set(WRITES_PER_ABSORB);
        }
    }

    if procsignal_seams::proc_signal_barrier_pending::call() {
        procsignal_seams::process_proc_signal_barrier::call()?;
    }
    Ok(())
}

fn IsCheckpointOnSchedule(progress: f64) -> bool {
    debug_assert!(CKPT_ACTIVE.get());

    let progress = progress * guc_tables::vars::CheckPointCompletionTarget.read();

    if progress < CKPT_CACHED_ELAPSED.get() {
        return false;
    }

    let recptr = if RecoveryInProgress() {
        xlogrecovery_seams::get_xlog_replay_rec_ptr::call().0
    } else {
        transam_xlog::GetInsertRecPtr()
    };
    let elapsed_xlogs = (recptr.wrapping_sub(CKPT_START_RECPTR.get()) as f64
        / transam_xlog::wal_segment_size() as f64)
        / transam_xlog::CheckPointSegments() as f64;

    if progress < elapsed_xlogs {
        CKPT_CACHED_ELAPSED.set(elapsed_xlogs);
        return false;
    }

    // DST P2 (contract §1.2): only the raw read moves — IsCheckpointOnSchedule
    // keeps its wall-fraction semantics (CKPT_START_TIME is a wall stamp).
    let (now_sec, now_usec) = pg_clock::wall_timeval();
    let elapsed_time = ((now_sec - CKPT_START_TIME.get()) as f64
        + f64::from(now_usec) / 1_000_000.0)
        / CheckPointTimeout() as f64;

    if progress < elapsed_time {
        CKPT_CACHED_ELAPSED.set(elapsed_time);
        return false;
    }

    true
}

pub fn RequestCheckpoint(flags: i32) -> PgResult<()> {
    if !g::IsPostmasterEnvironment() {
        transam_xlog::CreateCheckPoint(flags | CHECKPOINT_IMMEDIATE)?;
        smgr::smgrdestroyall()?;
        return Ok(());
    }

    let (old_failed, old_started) = with_ckpt_lck(|cp| {
        let old_failed = cp.ckpt_failed.load(Relaxed);
        let old_started = cp.ckpt_started.load(Relaxed);
        cp.ckpt_flags.fetch_or(flags | CHECKPOINT_REQUESTED, Relaxed);
        (old_failed, old_started)
    });

    let mut ntries = 0;
    loop {
        let checkpointer_proc = lmgr_proc::ProcGlobal().checkpointerProc.load(Relaxed);
        if checkpointer_proc == INVALID_PROC_NUMBER {
            if ntries >= MAX_SIGNAL_TRIES || flags & CHECKPOINT_WAIT == 0 {
                let level = if flags & CHECKPOINT_WAIT != 0 { ERROR } else { LOG };
                elog(level, "could not notify checkpoint: checkpointer is not running")?;
                break;
            }
        } else {
            latch::SetLatch(types_storage::latch::LatchHandle::proc(checkpointer_proc));
            break;
        }

        postgres_seams::check_for_interrupts::call()?;
        std::thread::sleep(std::time::Duration::from_millis(100));
        ntries += 1;
    }

    if flags & CHECKPOINT_WAIT != 0 {
        condition_variable_seams::checkpointer_cv_prepare_to_sleep::call(CheckpointerCv::Start);
        let new_started = loop {
            let new_started = with_ckpt_lck(|cp| cp.ckpt_started.load(Relaxed));
            if new_started != old_started {
                break new_started;
            }
            condition_variable_seams::checkpointer_cv_sleep::call(
                CheckpointerCv::Start,
                WAIT_EVENT_CHECKPOINT_START,
            )?;
        };
        condition_variable_seams::condition_variable_cancel_sleep::call();

        condition_variable_seams::checkpointer_cv_prepare_to_sleep::call(CheckpointerCv::Done);
        let new_failed = loop {
            let (new_done, new_failed) =
                with_ckpt_lck(|cp| (cp.ckpt_done.load(Relaxed), cp.ckpt_failed.load(Relaxed)));
            if new_done.wrapping_sub(new_started) >= 0 {
                break new_failed;
            }
            condition_variable_seams::checkpointer_cv_sleep::call(
                CheckpointerCv::Done,
                WAIT_EVENT_CHECKPOINT_DONE,
            )?;
        };
        condition_variable_seams::condition_variable_cancel_sleep::call();

        if new_failed != old_failed {
            return ereport(ERROR)
                .errmsg("checkpoint request failed")
                .errhint("Consult recent messages in the server log for details.")
                .finish(loc("RequestCheckpoint"));
        }
    }
    Ok(())
}

pub fn ForwardSyncRequest(ftag: FileTag, req_type: SyncRequestType) -> PgResult<bool> {
    if !g::IsUnderPostmaster() {
        return Ok(false);
    }

    if am_checkpointer() {
        return elog(ERROR, "ForwardSyncRequest must not be called in checkpointer")
            .map(|_| false);
    }

    let cp = shmem();
    LWLockAcquire(checkpointer_comm_lock(), LW_EXCLUSIVE, g::MyProcNumber())?;

    if cp.checkpointer_pid.load(Relaxed) == 0
        || (cp.num_requests.get() >= cp.max_requests && !CompactCheckpointerRequestQueue()?)
    {
        LWLockRelease(checkpointer_comm_lock())?;
        return Ok(false);
    }

    let idx = cp.num_requests.get();
    cp.num_requests.set(idx + 1);
    cp.requests[idx as usize].set(CheckpointerRequest { req_type, ftag });

    let too_full = cp.num_requests.get() >= cp.max_requests / 2;

    LWLockRelease(checkpointer_comm_lock())?;

    if too_full {
        let checkpointer_proc = lmgr_proc::ProcGlobal().checkpointerProc.load(Relaxed);
        if checkpointer_proc != INVALID_PROC_NUMBER {
            latch::SetLatch(types_storage::latch::LatchHandle::proc(checkpointer_proc));
        }
    }
    Ok(true)
}

// Caller holds CheckpointerCommLock exclusively. Cold: queue-full fallback.
fn CompactCheckpointerRequestQueue() -> PgResult<bool> {
    if g::CritSectionCount() > 0 {
        return Ok(false);
    }

    let cp = shmem();
    let num_requests = cp.num_requests.get();
    let n = num_requests.max(0) as usize;

    let scratch = mcx::MemoryContext::new("CompactCheckpointerRequestQueue");
    let mut skip_slot: mcx::PgVec<'_, bool> = mcx::PgVec::new_in(scratch.mcx());
    if skip_slot.try_reserve(n).is_err() {
        return Ok(false);
    }
    skip_slot.resize(n, false);
    let mut htab: mcx::PgHashMap<'_, CheckpointerRequest, i32> =
        mcx::PgHashMap::new_in(scratch.mcx());
    let _ = htab.try_reserve(n);
    let mut num_skipped = 0;

    for i in 0..num_requests {
        if let Some(prev) = htab.insert(cp.requests[i as usize].get(), i) {
            skip_slot[prev as usize] = true;
            num_skipped += 1;
        }
    }

    if num_skipped == 0 {
        return Ok(false);
    }

    let mut preserve_count: i32 = 0;
    for i in 0..num_requests {
        if skip_slot[i as usize] {
            continue;
        }
        cp.requests[preserve_count as usize].set(cp.requests[i as usize].get());
        preserve_count += 1;
    }
    ereport(DEBUG1)
        .errmsg_internal(format!(
            "compacted fsync request queue from {num_requests} entries to {preserve_count} entries"
        ))
        .finish(loc("CompactCheckpointerRequestQueue"))?;
    cp.num_requests.set(preserve_count);
    Ok(true)
}

pub fn AbsorbSyncRequests() -> PgResult<()> {
    if !am_checkpointer() {
        return Ok(());
    }

    let cp = shmem();
    LWLockAcquire(checkpointer_comm_lock(), LW_EXCLUSIVE, g::MyProcNumber())?;

    let n = cp.num_requests.get();
    ABSORB_SCRATCH.with(|cell| -> PgResult<()> {
        let mut slot = cell.borrow_mut();
        let buf = slot.get_or_insert_with(|| {
            let cx: &'static mcx::MemoryContext =
                mcx::session_root("AbsorbSyncRequests scratch");
            // LIFO: empty the droppy TLS slot before its context is freed.
            mcx::register_session_cleanup(Box::new(|| {
                ABSORB_SCRATCH.with(|c| drop(c.borrow_mut().take()));
            }));
            mcx::PgVec::new_in(cx.mcx())
        });
        buf.clear();
        if buf.try_reserve(n.max(0) as usize).is_err() {
            panic!("out of memory absorbing fsync requests");
        }
        buf.extend((0..n).map(|i| cp.requests[i as usize].get()));

        g::StartCriticalSection();
        cp.num_requests.set(0);
        LWLockRelease(checkpointer_comm_lock())?;

        for req in buf.iter() {
            sync::RememberSyncRequest(&req.ftag, req.req_type)?;
        }
        g::EndCriticalSection();
        Ok(())
    })
}

fn UpdateSharedMemoryConfig() -> PgResult<()> {
    // SyncRepUpdateSyncStandbysDefined: WalSndCtl (the flag's shmem home)
    // cannot exist while walsender/syncrep are unported, and the
    // synchronous_standby_names GUC has no slot yet either, so the C call is
    // provably the leave-it-false no-op until that owner installs the seam.
    if syncrep_seams::sync_rep_update_sync_standbys_defined::is_installed() {
        syncrep_seams::sync_rep_update_sync_standbys_defined::call();
    }

    transam_xlog::UpdateFullPageWrites()?;

    ereport(DEBUG2)
        .errmsg_internal("checkpointer updated shared memory configuration values")
        .finish(loc("UpdateSharedMemoryConfig"))?;
    Ok(())
}

pub fn FirstCallSinceLastCheckpoint() -> bool {
    let new_done = with_ckpt_lck(|cp| cp.ckpt_done.load(Relaxed));
    let first = new_done != FIRST_CALL_CKPT_DONE.get();
    FIRST_CALL_CKPT_DONE.set(new_done);
    first
}

fn report_checkpointer_stats() {
    if pgstat_seams::pgstat_report_checkpointer::is_installed() {
        pgstat_seams::pgstat_report_checkpointer::call();
    }
}

fn report_wal_stats(force: bool) {
    if pgstat_seams::pgstat_report_wal::is_installed() {
        pgstat_seams::pgstat_report_wal::call(force);
    }
}

fn install_guc_backing() {
    use guc_tables::GucVarAccessors;
    guc_tables::vars::CheckPointTimeout.install(GucVarAccessors {
        get: CheckPointTimeout,
        set: |v| CHECK_POINT_TIMEOUT.set(v),
    });
    guc_tables::vars::CheckPointWarning.install(GucVarAccessors {
        get: || CHECK_POINT_WARNING.get(),
        set: |v| CHECK_POINT_WARNING.set(v),
    });
    // CheckPointCompletionTarget backing stays in transam_xlog (installed
    // there for CalculateCheckpointSegments before this unit landed).
}

pub fn init_seams() {
    install_guc_backing();
    checkpointer_seams::request_checkpoint::set(RequestCheckpoint);
    checkpointer_seams::forward_sync_request::set(ForwardSyncRequest);
    checkpointer_seams::checkpoint_write_delay::set(CheckpointWriteDelay);
    sync_seams::absorb_sync_requests::set(AbsorbSyncRequests);
}
