//! AutoVacLauncherMain: thread-native launcher; the postmaster handshake
//! (av_startingWorker + PMSIGNAL_START_AUTOVAC_WORKER) is kept C-exact over
//! the process-global slots in shmem.rs.

use std::cell::{Cell, RefCell};
use std::sync::atomic::Ordering::Relaxed;

use init_small::globals as g;
use mcx::MemoryContext;
use types_core::xact::{MultiXactIdPrecedes, TransactionIdPrecedes};
use types_core::{
    BackendType, FirstNormalTransactionId, InvalidOid, MultiXactId, Oid, OidIsValid,
    ProcessingMode, TimestampTz, TransactionId,
};
use types_error::{PgError, PgResult, DEBUG1, WARNING};
use types_guc::{GucContext, GucSource};
use types_startup::StartupData;
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

use multixact::{FirstMultiXactId, MultiXactMemberFreezeThreshold, ReadNextMultiXactId};

use crate::cost::autovac_recalculate_workers_for_balance;
use crate::shmem::{self, AV_FORK_FAILED, AV_REBALANCE};
use crate::{autovacuum_max_workers, autovacuum_naptime, check_av_worker_gucs, AutoVacuumingActive};

const PG_WAIT_ACTIVITY: u32 = 0x0500_0000;
const WAIT_EVENT_AUTOVACUUM_MAIN: u32 = PG_WAIT_ACTIVITY + 1;
const MAX_AUTOVAC_SLEEPTIME_SECS: i64 = 300;
const MIN_AUTOVAC_SLEEPTIME_MS: i64 = 100;

#[derive(Clone, Copy)]
struct AvlDbase {
    adl_datid: Oid,
    adl_next_worker: TimestampTz,
    adl_score: i32,
}

#[derive(Clone, Copy)]
struct AvwDbase {
    adw_datid: Oid,
    adw_frozenxid: TransactionId,
    adw_minmulti: MultiXactId,
}

thread_local! {
    // C DatabaseList (launcher-private DatabaseListCxt): head first = most
    // distant adl_next_worker; the tail (last) is the soonest.
    static DATABASE_LIST: RefCell<Vec<AvlDbase>> = const { RefCell::new(Vec::new()) };
    static GOT_SIGUSR2: Cell<bool> = const { Cell::new(false) };
}

fn avl_sigusr2_handler() {
    GOT_SIGUSR2.set(true);
    if let Some(l) = g::MyLatch() {
        latch::SetLatch(l);
    }
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
        procsignal::pqsignal_thread(procsignal::signums::SIGHUP, Simple(interrupt::SignalHandlerForConfigReload));
        procsignal::pqsignal_thread(procsignal::signums::SIGINT, Simple(postgres::StatementCancelHandler));
        procsignal::pqsignal_thread(
            procsignal::signums::SIGTERM,
            Simple(interrupt::SignalHandlerForShutdownRequest),
        );
        timeout_seams::initialize_timeouts::call();
        procsignal::pqsignal_thread(procsignal::signums::SIGPIPE, Ignore);
        procsignal::pqsignal_thread(
            procsignal::signums::SIGUSR1,
            Simple(procsignal::procsignal_sigusr1_handler),
        );
        procsignal::pqsignal_thread(procsignal::signums::SIGUSR2, Simple(avl_sigusr2_handler));
        procsignal::pqsignal_thread(procsignal::signums::SIGFPE, Fallible(postgres::FloatExceptionHandler));
        procsignal::pqsignal_thread(procsignal::signums::SIGCHLD, Ignore);
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

    // sigsetjmp(local_sigjmp_buf) equivalent. Loud panics (unported callees
    // reached from rebuild_database_list / do_start_worker / catalog scans)
    // must convert to ERROR here, exactly like the worker's boundaries
    // (worker.rs AutoVacWorkerMain): an escaped panic reaches
    // launch_backend's SIGABRT mapping and the reaper's launcher arm treats
    // any non-zero exit as a crash -> HandleChildCrash cycles the whole
    // cluster, re-fired on every relaunch. proc_exit / elog-PANIC payloads
    // re-raise inside pg_error_from_panic and keep their semantics.
    let mut first = true;
    loop {
        if !first {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
        first = false;
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(launcher_body))
            .unwrap_or_else(|payload| {
                Err(Box::new(crate::worker::pg_error_from_panic(
                    payload,
                    "autovacuum launcher panicked",
                )))
            }) {
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
    // C guards this call ("this is probably dead code, but let's be safe:"):
    // the launcher is not an aux process and never creates the owner, and an
    // error raised outside any transaction reaches here with it null —
    // unguarded, the callee's !owner.is_null() assertion is a panic INSIDE
    // the recovery path, which escapes every catch and cycles the cluster.
    if !resowner::AuxProcessResourceOwner().is_null() {
        let _ = resowner::ReleaseAuxProcessResources(false);
    }
    bufmgr::AtEOXact_Buffers(false);
    let _ = smgr::AtEOXact_SMgr();
    let _ = fd::AtEOXact_Files(false);
    dynahash::AtEOXact_HashTables(false);

    // C deletes DatabaseListCxt in the error recovery block.
    DATABASE_LIST.with_borrow_mut(|l| l.clear());

    elog::FlushErrorState();
    g::ResumeInterrupts();

    if interrupt::ShutdownRequestPending() {
        AutoVacLauncherShutdown();
    }
}

fn launcher_body() -> PgResult<Never> {
    libpq_pqsignal::unblock_signals();

    shmem::shmem_init_once();

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

    // Emergency mode (autovacuum off, launcher started by wraparound signal):
    // start one worker and exit.
    if !AutoVacuumingActive() {
        if !interrupt::ShutdownRequestPending() {
            do_start_worker()?;
        }
        ipc::proc_exit(0, g::MyProcPid());
    }

    shmem::set_launcher_pid(g::MyProcPid());

    // Debug-only containment probe (autovacuum-e2e.sh probe 5): panic once
    // inside the launcher's sigsetjmp-equivalent region; the boundary in
    // AutoVacLauncherMain must convert it to a reported ERROR and retry —
    // never a cluster crash-restart.
    #[cfg(debug_assertions)]
    {
        // Process-global one-shot (NOT a thread_local: fires once per
        // postmaster lifetime so a relaunched launcher doesn't re-panic,
        // and the pinned session TLS census stays untouched).
        static LAUNCHER_PANIC_FIRED: std::sync::atomic::AtomicBool =
            std::sync::atomic::AtomicBool::new(false);
        if std::env::var("PGRUST_TEST_AUTOVAC_PANIC_LAUNCHER").is_ok()
            && !LAUNCHER_PANIC_FIRED.swap(true, Relaxed)
        {
            panic!("injected autovacuum launcher panic for containment test");
        }
    }

    rebuild_database_list(InvalidOid)?;

    while !interrupt::ShutdownRequestPending() {
        let nap_ms = launcher_determine_sleep(shmem::av_worker_available(), false)?;

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

        // A worker finished, or the postmaster failed to start one.
        if GOT_SIGUSR2.replace(false) {
            if shmem::get_av_signal(AV_REBALANCE) {
                let l = shmem::av_lock();
                autovac_recalculate_workers_for_balance(&l);
            }
            if shmem::get_av_signal(AV_FORK_FAILED) {
                std::thread::sleep(std::time::Duration::from_secs(1));
                pmsignal::SendPostmasterSignal(
                    pmsignal::PMSignalReason::PMSIGNAL_START_AUTOVAC_WORKER,
                );
                continue;
            }
        }

        let current_time = adt_timestamp::GetCurrentTimestamp();
        let mut can_launch;
        {
            let mut l = shmem::av_lock();
            can_launch = shmem::av_worker_available_locked(&l);

            if let Some(starting) = l.starting_worker {
                // A worker stuck in starting mode blocks new launches; reclaim
                // its slot after Min(naptime, 60)s.
                let waittime = autovacuum_naptime().min(60) * 1000;
                let launchtime = shmem::worker_launchtime(starting);
                if adt_timestamp::TimestampDifferenceExceeds(launchtime, current_time, waittime) {
                    shmem::worker_slots()[starting].reset();
                    l.free_workers.insert(0, starting);
                    l.starting_worker = None;
                    drop(l);
                    elog::ereport(WARNING)
                        .errmsg("autovacuum worker took too long to start; canceled")
                        .finish(loc("AutoVacLauncherMain"))?;
                } else {
                    can_launch = false;
                }
            }
        }

        if !can_launch {
            continue;
        }

        let list_empty = DATABASE_LIST.with_borrow(|l| l.is_empty());
        if list_empty {
            // Initial case: no database known to pgstats yet.
            launch_worker(current_time)?;
        } else {
            let next_worker = DATABASE_LIST.with_borrow(|l| l.last().unwrap().adl_next_worker);
            if adt_timestamp::TimestampDifferenceExceeds(next_worker, current_time, 0) {
                launch_worker(current_time)?;
            }
        }
    }

    AutoVacLauncherShutdown()
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

        rebuild_database_list(InvalidOid)?;
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
    shmem::set_launcher_pid(0);
    ipc::proc_exit(0, g::MyProcPid())
}

fn launcher_determine_sleep(canlaunch: bool, recursing: bool) -> PgResult<i64> {
    let (mut secs, mut usecs): (i64, i32);
    if !canlaunch {
        secs = autovacuum_naptime() as i64;
        usecs = 0;
    } else if let Some(next_worker) =
        DATABASE_LIST.with_borrow(|l| l.last().map(|d| d.adl_next_worker))
    {
        let current_time = adt_timestamp::GetCurrentTimestamp();
        (secs, usecs) = adt_timestamp::TimestampDifference(current_time, next_worker);
    } else {
        secs = autovacuum_naptime() as i64;
        usecs = 0;
    }

    // Exactly zero means an entry in the past: redistribute and retry once.
    if secs == 0 && usecs == 0 && !recursing {
        rebuild_database_list(InvalidOid)?;
        return launcher_determine_sleep(canlaunch, true);
    }

    if secs <= 0 && (usecs as i64) <= MIN_AUTOVAC_SLEEPTIME_MS * 1000 {
        secs = 0;
        usecs = (MIN_AUTOVAC_SLEEPTIME_MS * 1000) as i32;
    }
    if secs > MAX_AUTOVAC_SLEEPTIME_SECS {
        secs = MAX_AUTOVAC_SLEEPTIME_SECS;
    }
    Ok(secs * 1000 + (usecs / 1000) as i64)
}

fn rebuild_database_list(newdb: Oid) -> PgResult<()> {
    // Score-ordered insert set: newdb, then the current list, then all
    // pgstat-known databases.
    fn enter(dbary: &mut Vec<AvlDbase>, datid: Oid) {
        if !dbary.iter().any(|d| d.adl_datid == datid) {
            let score = dbary.len() as i32;
            dbary.push(AvlDbase { adl_datid: datid, adl_next_worker: 0, adl_score: score });
        }
    }
    let mut dbary: Vec<AvlDbase> = Vec::new();

    if OidIsValid(newdb) && pgstat::pgstat_fetch_stat_dbentry(newdb).is_some() {
        enter(&mut dbary, newdb);
    }

    let existing: Vec<Oid> =
        DATABASE_LIST.with_borrow(|l| l.iter().map(|d| d.adl_datid).collect());
    for datid in existing {
        if pgstat::pgstat_fetch_stat_dbentry(datid).is_none() {
            continue;
        }
        enter(&mut dbary, datid);
    }

    for avdb in get_database_list()? {
        if pgstat::pgstat_fetch_stat_dbentry(avdb.adw_datid).is_none() {
            continue;
        }
        enter(&mut dbary, avdb.adw_datid);
    }

    DATABASE_LIST.with_borrow_mut(|l| l.clear());
    let nelems = dbary.len() as i32;
    if nelems > 0 {
        dbary.sort_by_key(|d| d.adl_score);

        // C stores the float quotient into an int before the min-compare.
        let mut millis_increment =
            (1000.0 * autovacuum_naptime() as f64 / nelems as f64) as i64;
        if millis_increment <= MIN_AUTOVAC_SLEEPTIME_MS {
            millis_increment = (MIN_AUTOVAC_SLEEPTIME_MS as f64 * 1.1) as i64;
        }

        let mut current_time = adt_timestamp::GetCurrentTimestamp();
        DATABASE_LIST.with_borrow_mut(|l| {
            for db in dbary.iter_mut() {
                current_time += millis_increment * 1000;
                db.adl_next_worker = current_time;
                // Later elements go closer to the head.
                l.insert(0, *db);
            }
        });
    }
    Ok(())
}

// get_database_list: the launcher's only transaction; seqscan pg_database.
fn get_database_list() -> PgResult<Vec<AvwDbase>> {
    let mut dblist = Vec::new();

    xact::StartTransactionCommand()?;

    {
        let cx = MemoryContext::new("get_database_list");
        let mcx = cx.mcx();
        let rd = table::table_open(
            mcx,
            types_core::DATABASE_RELATION_ID,
            types_rel::lock::AccessShareLock,
        )?;
        let desc = rd.descr();
        let mut scan = genam::systable_beginscan(mcx, &rd, InvalidOid, false, None, &[])?;
        while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
            let att = |attnum: i32| -> datum::Datum {
                let mut isnull = false;
                // SAFETY: pg_database row under pg_database's descriptor;
                // fixed columns are never null.
                let d = unsafe { types_tuple::heap_getattr(tup, attnum, desc, &mut isnull) };
                debug_assert!(!isnull);
                d
            };
            // Skip invalid (interrupted-drop) databases; autovacuum can't
            // process them anyway.
            if att(pg_database::Anum_pg_database_datconnlimit).as_i32()
                == pg_database::DATCONNLIMIT_INVALID_DB
            {
                continue;
            }
            dblist.push(AvwDbase {
                adw_datid: att(pg_database::Anum_pg_database_oid).as_oid(),
                adw_frozenxid: att(pg_database::Anum_pg_database_datfrozenxid).as_u32(),
                adw_minmulti: att(pg_database::Anum_pg_database_datminmxid).as_u32(),
            });
        }
        genam::systable_endscan(mcx, scan)?;
        rd.close(types_rel::lock::AccessShareLock)?;
    }

    xact::CommitTransactionCommand()?;

    Ok(dblist)
}

fn do_start_worker() -> PgResult<Oid> {
    if !shmem::av_worker_available() {
        return Ok(InvalidOid);
    }

    let dblist = get_database_list()?;

    let recent_xid = varsup::ReadNextTransactionId()?;
    let mut xid_force_limit = recent_xid.wrapping_sub(g::autovacuum_freeze_max_age() as u32);
    if xid_force_limit < FirstNormalTransactionId {
        xid_force_limit = xid_force_limit.wrapping_sub(FirstNormalTransactionId);
    }

    let recent_multi = ReadNextMultiXactId()?;
    let mut multi_force_limit =
        recent_multi.wrapping_sub(MultiXactMemberFreezeThreshold()? as u32);
    if multi_force_limit < FirstMultiXactId {
        multi_force_limit = multi_force_limit.wrapping_sub(FirstMultiXactId);
    }

    let mut avdb: Option<&AvwDbase> = None;
    let mut avdb_last_autovac: TimestampTz = 0;
    let mut for_xid_wrap = false;
    let mut for_multi_wrap = false;
    let mut skipit = false;
    let current_time = adt_timestamp::GetCurrentTimestamp();

    for tmp in &dblist {
        if TransactionIdPrecedes(tmp.adw_frozenxid, xid_force_limit) {
            if avdb.is_none()
                || TransactionIdPrecedes(tmp.adw_frozenxid, avdb.unwrap().adw_frozenxid)
            {
                avdb = Some(tmp);
            }
            for_xid_wrap = true;
            continue;
        } else if for_xid_wrap {
            continue;
        } else if MultiXactIdPrecedes(tmp.adw_minmulti, multi_force_limit) {
            if avdb.is_none()
                || MultiXactIdPrecedes(tmp.adw_minmulti, avdb.unwrap().adw_minmulti)
            {
                avdb = Some(tmp);
            }
            for_multi_wrap = true;
            continue;
        } else if for_multi_wrap {
            continue;
        }

        let Some(entry) = pgstat::pgstat_fetch_stat_dbentry(tmp.adw_datid) else {
            continue;
        };

        // Skip databases scheduled within [now, now + naptime).
        skipit = false;
        let scheduled = DATABASE_LIST.with_borrow(|l| {
            l.iter()
                .find(|d| d.adl_datid == tmp.adw_datid)
                .map(|d| d.adl_next_worker)
        });
        if let Some(next_worker) = scheduled {
            if !adt_timestamp::TimestampDifferenceExceeds(next_worker, current_time, 0)
                && !adt_timestamp::TimestampDifferenceExceeds(
                    current_time,
                    next_worker,
                    autovacuum_naptime() * 1000,
                )
            {
                skipit = true;
            }
        }
        if skipit {
            continue;
        }

        if avdb.is_none() || entry.last_autovac_time < avdb_last_autovac {
            avdb = Some(tmp);
            avdb_last_autovac = entry.last_autovac_time;
        }
    }

    let mut retval = InvalidOid;
    if let Some(avdb) = avdb {
        {
            let mut l = shmem::av_lock();
            debug_assert!(!l.free_workers.is_empty());
            let worker = l.free_workers.remove(0);
            let slot = &shmem::worker_slots()[worker];
            slot.wi_dboid.store(avdb.adw_datid, Relaxed);
            slot.wi_proc_pid.store(0, Relaxed);
            slot.wi_launchtime
                .store(adt_timestamp::GetCurrentTimestamp(), Relaxed);
            l.starting_worker = Some(worker);
        }
        pmsignal::SendPostmasterSignal(pmsignal::PMSignalReason::PMSIGNAL_START_AUTOVAC_WORKER);
        retval = avdb.adw_datid;
    } else if skipit {
        // Everything on the list was skipped: it probably holds a dropped DB.
        rebuild_database_list(InvalidOid)?;
    }

    Ok(retval)
}

fn launch_worker(now: TimestampTz) -> PgResult<()> {
    let dbid = do_start_worker()?;
    if OidIsValid(dbid) {
        let new_next = now + (autovacuum_naptime() as i64 * 1000) * 1000;
        let found = DATABASE_LIST.with_borrow_mut(|l| {
            if let Some(idx) = l.iter().position(|d| d.adl_datid == dbid) {
                l[idx].adl_next_worker = new_next;
                let elem = l.remove(idx);
                l.insert(0, elem);
                true
            } else {
                false
            }
        });
        if !found {
            rebuild_database_list(dbid)?;
        }
    }
    Ok(())
}

// Postmaster-side: thread spawn for a worker failed.
pub fn AutoVacWorkerFailed() {
    shmem::set_av_signal(AV_FORK_FAILED);
}

#[cold]
#[inline(never)]
fn loc(routine: &'static str) -> types_error::ErrorLocation {
    types_error::ErrorLocation::new(file!(), line!() as i32, routine)
}
