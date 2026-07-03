//! AutoVacLauncherMain. DIVERGENCE: the selection half reduces to its
//! provably-empty arms (no pgstat db entries exist; wraparound force arms
//! bound-checked in do_start_worker), so get_database_list's pg_database scan
//! is skipped and DatabaseList stays empty — C's outcome with no pgstat data.

use std::cell::Cell;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicI32};

use init_small::globals as g;
use mcx::MemoryContext;
use types_core::{
    BackendType, FirstNormalTransactionId, FullTransactionId, InvalidOid, Oid, OidIsValid,
    ProcessingMode, TransactionIdPrecedes,
};
use types_error::{PgError, PgResult, DEBUG1};
use types_guc::{GucContext, GucSource};
use types_startup::StartupData;
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

use crate::{autovacuum_max_workers, autovacuum_naptime, autovacuum_worker_slots, check_av_worker_gucs, AutoVacuumingActive};

const PG_WAIT_ACTIVITY: u32 = 0x0500_0000;
const WAIT_EVENT_AUTOVACUUM_MAIN: u32 = PG_WAIT_ACTIVITY + 1;
const MAX_AUTOVAC_SLEEPTIME_SECS: i64 = 300;
const MIN_AUTOVAC_SLEEPTIME_MS: i64 = 100;

// AutoVacuumShmemStruct reduced to launcher-visible fields; the free list
// is a count until AutoVacWorkerMain ports.
static AV_LAUNCHER_PID: AtomicI32 = AtomicI32::new(0);
static AV_SIGNAL_REBALANCE: AtomicBool = AtomicBool::new(false);
static AV_SIGNAL_FORK_FAILED: AtomicBool = AtomicBool::new(false);
static AV_FREE_WORKERS: AtomicI32 = AtomicI32::new(-1);

thread_local! {
    static GOT_SIGUSR2: Cell<bool> = const { Cell::new(false) };
}

fn avl_sigusr2_handler() {
    GOT_SIGUSR2.set(true);
    if let Some(l) = g::MyLatch() {
        latch::SetLatch(l);
    }
}

fn av_worker_available() -> bool {
    let free_slots = AV_FREE_WORKERS.load(Relaxed);
    let reserved_slots = (autovacuum_worker_slots() - autovacuum_max_workers()).max(0);
    free_slots > reserved_slots
}

fn fatal_exit(e: &PgError) -> ! {
    elog::emit_error_report_for(e);
    ipc::proc_exit(1, g::MyProcPid())
}

pub fn AutoVacLauncherMain(startup_data: &StartupData) -> ! {
    debug_assert!(matches!(startup_data, StartupData::None));

    miscinit::SetMyBackendType(BackendType::AutovacLauncher);

    if let Err(e) = elog::elog(DEBUG1, "autovacuum launcher started") {
        fatal_exit(&e);
    }

    let post_auth_delay = guc_tables::vars::PostAuthDelay.read();
    if post_auth_delay > 0 {
        std::thread::sleep(std::time::Duration::from_secs(post_auth_delay as u64));
    }

    {
        use procsignal::ThreadSignalHandler::{Fallible, Ignore, Simple};
        procsignal::pqsignal_thread(libc::SIGHUP, Simple(interrupt::SignalHandlerForConfigReload));
        procsignal::pqsignal_thread(libc::SIGINT, Simple(postgres::StatementCancelHandler));
        procsignal::pqsignal_thread(
            libc::SIGTERM,
            Simple(interrupt::SignalHandlerForShutdownRequest),
        );
        timeout_seams::initialize_timeouts::call();
        procsignal::pqsignal_thread(libc::SIGPIPE, Ignore);
        procsignal::pqsignal_thread(
            libc::SIGUSR1,
            Simple(procsignal::procsignal_sigusr1_handler),
        );
        procsignal::pqsignal_thread(libc::SIGUSR2, Simple(avl_sigusr2_handler));
        procsignal::pqsignal_thread(libc::SIGFPE, Fallible(postgres::FloatExceptionHandler));
        procsignal::pqsignal_thread(libc::SIGCHLD, Ignore);
    }

    let init = (|| -> PgResult<()> {
        lmgr_proc::InitProcess(BackendType::AutovacLauncher)?;
        postinit::BaseInit()?;
        let top = MemoryContext::new("AutoVacLauncherInit");
        postinit::InitPostgres(top.mcx(), None, InvalidOid, None, InvalidOid, 0, None)?;
        Ok(())
    })();
    if let Err(e) = init {
        fatal_exit(&e);
    }

    miscinit::SetProcessingMode(ProcessingMode::NormalProcessing);

    // sigsetjmp(local_sigjmp_buf) equivalent.
    let mut first = true;
    loop {
        if !first {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
        first = false;
        match launcher_body() {
            Ok(never) => match never {},
            Err(err) => abort_cleanup(&err),
        }
    }
}

enum Never {}

fn abort_cleanup(err: &PgError) {
    g::HoldInterrupts();

    let _ = timeout_seams::disable_all_timeouts::call(false);
    g::SetQueryCancelPending(false);

    elog::emit_error_report_for(err);

    xact::AbortCurrentTransaction()
        .unwrap_or_else(|e| panic!("AutoVacLauncherMain: AbortCurrentTransaction failed: {e:?}"));

    let _ = lwlock::LWLockReleaseAll();
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

    elog::FlushErrorState();
    g::ResumeInterrupts();

    if interrupt::ShutdownRequestPending() {
        AutoVacLauncherShutdown();
    }
}

fn launcher_body() -> PgResult<Never> {
    libpq_pqsignal::unblock_signals();

    // AutoVacuumShmemInit's free-list fill; sole writer while workers unported.
    AV_FREE_WORKERS.store(autovacuum_worker_slots(), Relaxed);

    guc::SetConfigOption("search_path", Some(""), GucContext::PGC_SUSET, GucSource::PGC_S_OVERRIDE)?;
    guc::SetConfigOption(
        "zero_damaged_pages",
        Some("false"),
        GucContext::PGC_SUSET,
        GucSource::PGC_S_OVERRIDE,
    )?;
    guc::SetConfigOption(
        "statement_timeout",
        Some("0"),
        GucContext::PGC_SUSET,
        GucSource::PGC_S_OVERRIDE,
    )?;
    guc::SetConfigOption(
        "transaction_timeout",
        Some("0"),
        GucContext::PGC_SUSET,
        GucSource::PGC_S_OVERRIDE,
    )?;
    guc::SetConfigOption(
        "lock_timeout",
        Some("0"),
        GucContext::PGC_SUSET,
        GucSource::PGC_S_OVERRIDE,
    )?;
    guc::SetConfigOption(
        "idle_in_transaction_session_timeout",
        Some("0"),
        GucContext::PGC_SUSET,
        GucSource::PGC_S_OVERRIDE,
    )?;
    guc::SetConfigOption(
        "default_transaction_isolation",
        Some("read committed"),
        GucContext::PGC_SUSET,
        GucSource::PGC_S_OVERRIDE,
    )?;
    guc::SetConfigOption(
        "stats_fetch_consistency",
        Some("none"),
        GucContext::PGC_SUSET,
        GucSource::PGC_S_OVERRIDE,
    )?;

    if !AutoVacuumingActive() {
        if !interrupt::ShutdownRequestPending() {
            do_start_worker();
        }
        ipc::proc_exit(0, g::MyProcPid());
    }

    AV_LAUNCHER_PID.store(g::MyProcPid(), Relaxed);

    rebuild_database_list();

    while !interrupt::ShutdownRequestPending() {
        let nap_ms = launcher_determine_sleep(av_worker_available());

        let _ = latch::WaitLatch(
            g::MyLatch(),
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            nap_ms,
            WAIT_EVENT_AUTOVACUUM_MAIN,
        )?;

        if let Some(l) = g::MyLatch() {
            latch::ResetLatch(l);
        }

        ProcessAutoVacLauncherInterrupts()?;

        if GOT_SIGUSR2.replace(false) {
            assert!(
                !AV_SIGNAL_REBALANCE.load(Relaxed),
                "AutoVacLauncherMain: AutoVacRebalance signaled with no worker \
                 pipeline (AutoVacWorkerMain unported, backend-postmaster-autovacuum)"
            );
            assert!(
                !AV_SIGNAL_FORK_FAILED.load(Relaxed),
                "AutoVacLauncherMain: AutoVacForkFailed signaled with no worker \
                 pipeline (StartAutovacuumWorker request path unported)"
            );
        }

        let can_launch = av_worker_available();

        if !can_launch {
            continue;
        }

        // Empty DatabaseList: C's arm launches right away (naptime-throttled).
        launch_worker();
    }

    AutoVacLauncherShutdown();
}

fn ProcessAutoVacLauncherInterrupts() -> PgResult<()> {
    if interrupt::ShutdownRequestPending() {
        AutoVacLauncherShutdown();
    }

    if interrupt::ConfigReloadPending() {
        let autovacuum_max_workers_prev = autovacuum_max_workers();

        interrupt::SetConfigReloadPending(false);
        guc_file_seams::process_config_file::call(GucContext::PGC_SIGHUP)?;

        if !AutoVacuumingActive() {
            AutoVacLauncherShutdown();
        }

        if autovacuum_max_workers_prev != autovacuum_max_workers() {
            check_av_worker_gucs();
        }

        rebuild_database_list();
    }

    if g::ProcSignalBarrierPending() {
        procsignal::ProcessProcSignalBarrier()?;
    }

    // Flag owner (mcxt.c half) unported => the flag can never be set.
    if mcxt_seams::log_memory_context_pending::is_installed()
        && mcxt_seams::log_memory_context_pending::call()
    {
        mcxt_seams::process_log_memory_context_interrupt::call()?;
    }

    sinval::ProcessCatchupInterrupt()?;

    Ok(())
}

fn AutoVacLauncherShutdown() -> ! {
    let _ = elog::elog(DEBUG1, "autovacuum launcher shutting down");
    AV_LAUNCHER_PID.store(0, Relaxed);
    ipc::proc_exit(0, g::MyProcPid())
}

// With DatabaseList always empty every arm is the naptime sleep, C clamps kept.
fn launcher_determine_sleep(_canlaunch: bool) -> i64 {
    let mut nap_ms = autovacuum_naptime() as i64 * 1000;
    if nap_ms <= 0 {
        nap_ms = MIN_AUTOVAC_SLEEPTIME_MS;
    }
    nap_ms.min(MAX_AUTOVAC_SLEEPTIME_SECS * 1000)
}

// The list C builds here is empty with no pgstat entries (module doc).
fn rebuild_database_list() {}

fn launch_worker() {
    let dbid = do_start_worker();
    // do_start_worker panics before returning a valid Oid.
    assert!(!OidIsValid(dbid), "launch_worker: unreachable");
}

fn do_start_worker() -> Oid {
    if !av_worker_available() {
        return InvalidOid;
    }

    let tv = varsup::TransamVariables();
    let recent_xid = FullTransactionId::from_u64(tv.nextXid.load(Relaxed)).xid();
    let mut xid_force_limit =
        recent_xid.wrapping_sub(g::autovacuum_freeze_max_age() as u32);
    if xid_force_limit < FirstNormalTransactionId {
        xid_force_limit = xid_force_limit.wrapping_sub(FirstNormalTransactionId);
    }
    // oldestXid = cluster-wide minimum datfrozenxid (SetTransactionIdLimit):
    // if it does not precede the force limit, no database's datfrozenxid can.
    let oldest_xid = tv.oldestXid.load(Relaxed);
    if TransactionIdPrecedes(oldest_xid, xid_force_limit) {
        panic!(
            "do_start_worker: a database is at xid-wraparound risk (oldestXid {oldest_xid} \
             precedes force limit {xid_force_limit}) — get_database_list + AutoVacWorkerMain \
             unported (backend-postmaster-autovacuum)"
        );
    }
    // Multixact force arm can't fire (multixact.c unported => datminmxid
    // frozen); the remaining C selection needs a pgstat db entry; none exist.
    InvalidOid
}
