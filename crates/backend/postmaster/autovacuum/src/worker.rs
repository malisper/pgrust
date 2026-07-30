//! AutoVacWorkerMain + do_autovacuum. Thread-native worker: the C fork+shmem
//! handshake becomes a thread spawn against the same process-global slots.

use std::cell::Cell;
use std::sync::atomic::Ordering::Relaxed;

use init_small::globals as g;
use mcx::{Mcx, MemoryContext, PgVec};
use tableam_vocab::{
    VacOptValue, VacuumParams, VACOPT_ANALYZE, VACOPT_PROCESS_MAIN, VACOPT_SKIP_DATABASE_STATS,
    VACOPT_SKIP_LOCKED, VACOPT_VACUUM,
};
use types_core::xact::{MultiXactIdPrecedes, TransactionIdIsNormal, TransactionIdPrecedes};
use types_core::{
    BackendType, FirstNormalTransactionId, InvalidOid, MultiXactId, Oid, OidIsValid,
    ProcessingMode, TransactionId, NAMESPACE_RELATION_ID, RELATION_RELATION_ID,
};
use types_error::{PgError, PgResult, DEBUG1, ERROR, FATAL, LOG, WARNING};
use types_nodes::parsenodes::{DropBehavior, VacuumRelation};
use types_nodes::{Node, NodeList};
use types_rel::lock::{AccessShareLock, AccessExclusiveLock};
use types_rel::pg_class::{RELKIND_MATVIEW, RELKIND_RELATION, RELKIND_TOASTVALUE};
use types_rel::reloptions::AutoVacOpts;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_startup::StartupData;
use types_storage::buf::{BufferAccessStrategy, BufferAccessStrategyType};

use catalog_namespace::TempNamespaceStatus;
use multixact::{
    FirstMultiXactId, MultiXactIdIsValid, MultiXactMemberFreezeThreshold, ReadNextMultiXactId,
};

use crate::cost::{autovac_recalculate_workers_for_balance, VacuumUpdateCosts};
use crate::shmem::{
    self, AVW_BRIN_SUMMARIZE_RANGE, AV_REBALANCE, AV_STORAGE_PARAM_COST_DELAY,
    AV_STORAGE_PARAM_COST_LIMIT, MY_WORKER_INFO, NUM_WORKITEMS,
};
use crate::{autovacuum_anl_scale, autovacuum_anl_thresh, autovacuum_multixact_freeze_max_age,
    autovacuum_vac_ins_scale, autovacuum_vac_ins_thresh, autovacuum_vac_max_thresh,
    autovacuum_vac_scale, autovacuum_vac_thresh, AutoVacuumingActive, Log_autovacuum_min_duration,
};

const StatisticRelationId: Oid = 2619;
const PERFORM_DELETION_INTERNAL: i32 = 0x0001;
const PERFORM_DELETION_QUIETLY: i32 = 0x0004;
const PERFORM_DELETION_SKIP_EXTENSIONS: i32 = 0x0010;

thread_local! {
    static RECENT_XID: Cell<TransactionId> = const { Cell::new(0) };
    static RECENT_MULTI: Cell<MultiXactId> = const { Cell::new(0) };
    static DEFAULT_FREEZE_MIN_AGE: Cell<i32> = const { Cell::new(0) };
    static DEFAULT_FREEZE_TABLE_AGE: Cell<i32> = const { Cell::new(0) };
    static DEFAULT_MULTIXACT_FREEZE_MIN_AGE: Cell<i32> = const { Cell::new(0) };
    static DEFAULT_MULTIXACT_FREEZE_TABLE_AGE: Cell<i32> = const { Cell::new(0) };
}

// proc_exit / PANIC payloads must keep unwinding (main_loop.rs precedent).
// Shared with the launcher (launcher.rs sigsetjmp-equivalent boundary).
pub(crate) fn pg_error_from_panic(
    payload: Box<dyn std::any::Any + Send>,
    fallback_msg: &str,
) -> PgError {
    if payload.is::<ipc::ProcExitThread>() || payload.is::<types_error::PanicExitThread>() {
        std::panic::resume_unwind(payload);
    }
    match ::types_error::pg_error_from_panic(payload) {
        Ok(e) => e,
        Err(payload) => {
            let msg = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| fallback_msg.to_string());
            PgError::new(ERROR, msg)
        }
    }
}

fn fatal_exit(e: &PgError) -> ! {
    elog::emit_error_report_for(e);
    ipc::proc_exit(1, g::MyProcPid())
}

pub fn AutoVacWorkerMain(startup_data: &StartupData) -> ! {
    debug_assert!(matches!(startup_data, StartupData::None));

    miscinit::SetMyBackendType(BackendType::AutovacWorker);

    {
        use procsignal::ThreadSignalHandler::{Fallible, Ignore, Simple};
        procsignal::pqsignal_thread(procsignal::signums::SIGHUP, Simple(interrupt::SignalHandlerForConfigReload));
        procsignal::pqsignal_thread(procsignal::signums::SIGINT, Simple(postgres::StatementCancelHandler));
        procsignal::pqsignal_thread(procsignal::signums::SIGTERM, Fallible(postgres::die));
        timeout_seams::initialize_timeouts::call();
        procsignal::pqsignal_thread(procsignal::signums::SIGPIPE, Ignore);
        procsignal::pqsignal_thread(procsignal::signums::SIGUSR1, Simple(procsignal::procsignal_sigusr1_handler));
        procsignal::pqsignal_thread(procsignal::signums::SIGUSR2, Ignore);
        procsignal::pqsignal_thread(procsignal::signums::SIGFPE, Fallible(postgres::FloatExceptionHandler));
        procsignal::pqsignal_thread(procsignal::signums::SIGCHLD, Ignore);
    }

    if let Err(e) = (|| -> PgResult<()> {
        lmgr_proc::InitProcess(BackendType::AutovacWorker)?;
        postinit::BaseInit()?;
        Ok(())
    })() {
        fatal_exit(&e);
    }

    // sigsetjmp equivalent: any error escaping the body is reported and the
    // worker exits 0 (C 1440-1457). Loud panics unwind as ERROR here — an
    // escaped panic reaches launch_backend's SIGABRT mapping and cycles the
    // whole cluster (postgres.c run_one_iteration precedent).
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(worker_body))
        .unwrap_or_else(|payload| Err(Box::new(pg_error_from_panic(payload, "autovacuum worker panicked"))))
    {
        Ok(()) => {}
        Err(e) => {
            g::HoldInterrupts();
            elog::emit_error_report_for(&e);
            ipc::proc_exit(0, g::MyProcPid());
        }
    }
    ipc::proc_exit(0, g::MyProcPid())
}

fn worker_body() -> PgResult<()> {
    use types_guc::{GucContext::PGC_SUSET, GucSource::PGC_S_OVERRIDE};

    libpq_pqsignal::unblock_signals();

    guc::SetConfigOption("search_path", Some(""), PGC_SUSET, PGC_S_OVERRIDE)?;
    guc::SetConfigOption("zero_damaged_pages", Some("false"), PGC_SUSET, PGC_S_OVERRIDE)?;
    guc::SetConfigOption("statement_timeout", Some("0"), PGC_SUSET, PGC_S_OVERRIDE)?;
    guc::SetConfigOption("transaction_timeout", Some("0"), PGC_SUSET, PGC_S_OVERRIDE)?;
    guc::SetConfigOption("lock_timeout", Some("0"), PGC_SUSET, PGC_S_OVERRIDE)?;
    guc::SetConfigOption(
        "idle_in_transaction_session_timeout",
        Some("0"),
        PGC_SUSET,
        PGC_S_OVERRIDE,
    )?;
    guc::SetConfigOption(
        "default_transaction_isolation",
        Some("read committed"),
        PGC_SUSET,
        PGC_S_OVERRIDE,
    )?;
    if guc_tables::vars::synchronous_commit.read()
        > guc_tables::consts::SYNCHRONOUS_COMMIT_LOCAL_FLUSH
    {
        guc::SetConfigOption("synchronous_commit", Some("local"), PGC_SUSET, PGC_S_OVERRIDE)?;
    }
    guc::SetConfigOption("stats_fetch_consistency", Some("none"), PGC_SUSET, PGC_S_OVERRIDE)?;

    let dbid = {
        let mut l = shmem::av_lock();
        if let Some(idx) = l.starting_worker {
            let slot = &shmem::worker_slots()[idx];
            MY_WORKER_INFO.set(Some(idx));
            let dbid = slot.wi_dboid.load(Relaxed);
            slot.wi_proc_pid.store(g::MyProcPid(), Relaxed);
            l.running_workers.insert(0, idx);
            l.starting_worker = None;
            drop(l);

            ipc::on_shmem_exit(free_worker_info_callback, 0);

            let launcherpid = shmem::launcher_pid();
            if launcherpid != 0 {
                let _ = procsignal::SendThreadSignal(launcherpid, procsignal::signums::SIGUSR2);
            }
            dbid
        } else {
            drop(l);
            elog::ereport(WARNING)
                .errmsg("autovacuum worker started without a worker entry")
                .finish(loc("AutoVacWorkerMain"))?;
            InvalidOid
        }
    };

    if OidIsValid(dbid) {
        // Before InitPostgres, so last_autovac_time advances even if the
        // connection attempt fails (C 1560-1566).
        pgstat::pgstat_report_autovac(dbid);

        let top = MemoryContext::new("AutoVacWorkerInit");
        postinit::InitPostgres(
            top.mcx(),
            None,
            dbid,
            None,
            InvalidOid,
            postinit::INIT_PG_OVERRIDE_ALLOW_CONNS,
            None,
        )?;
        miscinit::SetProcessingMode(ProcessingMode::NormalProcessing);
        elog::elog(DEBUG1, "autovacuum: processing database")?;

        let post_auth_delay = guc_tables::vars::PostAuthDelay.read();
        if post_auth_delay > 0 {
            std::thread::sleep(std::time::Duration::from_secs(post_auth_delay as u64));
        }

        RECENT_XID.set(varsup::ReadNextTransactionId()?);
        RECENT_MULTI.set(ReadNextMultiXactId()?);

        do_autovacuum()?;
    }
    Ok(())
}

fn free_worker_info_callback(_code: i32, _arg: usize) {
    FreeWorkerInfo();
}

fn FreeWorkerInfo() {
    let Some(idx) = MY_WORKER_INFO.get() else { return };
    let mut l = shmem::av_lock();

    // The launcher wake rides ProcKill via wake_autovacuum_launcher (C saves
    // AutovacuumLauncherPid here and kills from ProcKill).
    shmem::AUTOVACUUM_LAUNCHER_PID.set(shmem::launcher_pid());

    l.running_workers.retain(|&w| w != idx);
    shmem::worker_slots()[idx].reset();
    l.free_workers.insert(0, idx);
    MY_WORKER_INFO.set(None);

    shmem::set_av_signal(AV_REBALANCE);
}

struct AvClassRow {
    oid: Oid,
    relname: String,
    relnamespace: Oid,
    reltoastrelid: Oid,
    relpages: i32,
    reltuples: f32,
    relallfrozen: i32,
    relisshared: bool,
    relpersistence: u8,
    relkind: u8,
    relam: Oid,
    relfrozenxid: TransactionId,
    relminmxid: MultiXactId,
    avopts: Option<AutoVacOpts>,
}

fn decode_av_class_row(
    mcx: Mcx<'_>,
    desc: &types_tuple::TupleDescData<'_>,
    tup: &types_tuple::HeapTupleData<'_>,
) -> PgResult<AvClassRow> {
    let att = |attnum: i32| -> (datum::Datum, bool) {
        let mut isnull = false;
        // SAFETY: pg_class row under pg_class's descriptor.
        let d = unsafe { types_tuple::heap_getattr(tup, attnum, desc, &mut isnull) };
        (d, isnull)
    };
    let req = |attnum: i32| -> datum::Datum {
        let (d, isnull) = att(attnum);
        debug_assert!(!isnull);
        d
    };
    let relname = {
        let d = req(2);
        // SAFETY: NameData column: NAMEDATALEN readable bytes.
        let bytes = unsafe {
            std::slice::from_raw_parts(d.as_usize() as *const u8, 64)
        };
        let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        String::from_utf8_lossy(&bytes[..end]).into_owned()
    };
    let relkind = req(18).as_u8();
    let relam = req(7).as_oid();
    // Only the relkinds autovacuum considers carry AutoVacOpts (C extracts
    // after its relkind filters).
    let avopts = if matches!(relkind, RELKIND_RELATION | RELKIND_MATVIEW | RELKIND_TOASTVALUE) {
        let (opts_datum, opts_null) = att(33);
        extract_autovac_opts(mcx, relkind, relam, if opts_null { None } else { Some(opts_datum) })?
    } else {
        None
    };
    Ok(AvClassRow {
        oid: req(1).as_oid(),
        relname,
        relnamespace: req(3).as_oid(),
        reltoastrelid: req(14).as_oid(),
        relpages: req(10).as_i32(),
        reltuples: req(11).as_f32(),
        relallfrozen: req(13).as_i32(),
        relisshared: req(16).as_bool(),
        relpersistence: req(17).as_u8(),
        relkind,
        relam,
        relfrozenxid: req(30).as_u32(),
        relminmxid: req(31).as_u32(),
        avopts,
    })
}

fn extract_autovac_opts(
    mcx: Mcx<'_>,
    relkind: u8,
    relam: Oid,
    opts_datum: Option<datum::Datum>,
) -> PgResult<Option<AutoVacOpts>> {
    debug_assert!(matches!(relkind, RELKIND_RELATION | RELKIND_MATVIEW | RELKIND_TOASTVALUE));
    Ok(reloptions::extractRelOptions(mcx, relkind, relam, opts_datum)?
        .as_ref()
        .and_then(|o| o.std())
        .map(|s| s.autovacuum))
}

fn fetch_av_class_row(mcx: Mcx<'_>, relid: Oid) -> PgResult<Option<AvClassRow>> {
    let rd = table::table_open(mcx, RELATION_RELATION_ID, AccessShareLock)?;
    let desc = rd.descr();
    let mut key = ScanKeyData::empty();
    key.sk_attno = 1;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = datum::Datum::from_oid(relid);
    const ClassOidIndexId: Oid = 2662;
    let mut scan = genam::systable_beginscan(mcx, &rd, ClassOidIndexId, true, None, &[key])?;
    let row = match genam::systable_getnext(mcx, &mut scan)? {
        Some(tup) => Some(decode_av_class_row(mcx, desc, tup)?),
        None => None,
    };
    genam::systable_endscan(mcx, scan)?;
    rd.close(AccessShareLock)?;
    Ok(row)
}

struct AvRelation {
    ar_toastrelid: Oid,
    ar_hasrelopts: bool,
    ar_reloptions: AutoVacOpts,
}

struct AutovacTable {
    at_relid: Oid,
    at_params: VacuumParams,
    at_storage_param_vac_cost_delay: f64,
    at_storage_param_vac_cost_limit: i32,
    at_dobalance: bool,
    at_nspname: Oid,
}

pub fn do_autovacuum() -> PgResult<()> {
    let autovac = MemoryContext::new("Autovacuum worker");
    let mcx = autovac.mcx();

    xact::StartTransactionCommand()?;

    let effective_multixact_freeze_max_age = MultiXactMemberFreezeThreshold()?;

    {
        let scratch = MemoryContext::new("do_autovacuum dbform");
        let dbform = pg_database::search_database_syscache(scratch.mcx(), g::MyDatabaseId())?
            .ok_or_else(|| PgError::error("cache lookup failed for database"))?;
        if dbform.datistemplate || !dbform.datallowconn {
            DEFAULT_FREEZE_MIN_AGE.set(0);
            DEFAULT_FREEZE_TABLE_AGE.set(0);
            DEFAULT_MULTIXACT_FREEZE_MIN_AGE.set(0);
            DEFAULT_MULTIXACT_FREEZE_TABLE_AGE.set(0);
        } else {
            DEFAULT_FREEZE_MIN_AGE.set(guc_tables::vars::vacuum_freeze_min_age.read());
            DEFAULT_FREEZE_TABLE_AGE.set(guc_tables::vars::vacuum_freeze_table_age.read());
            DEFAULT_MULTIXACT_FREEZE_MIN_AGE
                .set(guc_tables::vars::vacuum_multixact_freeze_min_age.read());
            DEFAULT_MULTIXACT_FREEZE_TABLE_AGE
                .set(guc_tables::vars::vacuum_multixact_freeze_table_age.read());
        }
    }

    let mut table_oids: PgVec<'_, Oid> = PgVec::new_in(mcx);
    let mut orphan_oids: PgVec<'_, Oid> = PgVec::new_in(mcx);
    let mut table_toast_map: PgVec<'_, AvRelation> = PgVec::new_in(mcx);

    // Pass 1: relations and matviews; collect toast->main reloption mapping.
    {
        let scan_cx = MemoryContext::new("do_autovacuum pg_class scan");
        let smcx = scan_cx.mcx();
        let rd = table::table_open(smcx, RELATION_RELATION_ID, AccessShareLock)?;
        let desc = rd.descr();
        let mut scan = genam::systable_beginscan(smcx, &rd, InvalidOid, false, None, &[])?;
        while let Some(tup) = genam::systable_getnext(smcx, &mut scan)? {
            let row = decode_av_class_row(smcx, desc, tup)?;
            if row.relkind != RELKIND_RELATION && row.relkind != RELKIND_MATVIEW {
                continue;
            }
            if row.relpersistence == types_core::RELPERSISTENCE_TEMP {
                if catalog_namespace::checkTempNamespaceStatus(row.relnamespace)?
                    == TempNamespaceStatus::Idle
                {
                    orphan_oids.push(row.oid);
                }
                continue;
            }

            let tabentry = pgstat::pgstat_fetch_stat_tabentry_ext(row.relisshared, row.oid);
            let (dovacuum, doanalyze, _wraparound) = relation_needs_vacanalyze(
                row.oid,
                row.avopts.as_ref(),
                &row,
                tabentry.as_ref(),
                effective_multixact_freeze_max_age,
            );
            if dovacuum || doanalyze {
                table_oids.push(row.oid);
            }

            if OidIsValid(row.reltoastrelid)
                && !table_toast_map.iter().any(|h| h.ar_toastrelid == row.reltoastrelid)
            {
                table_toast_map.push(AvRelation {
                    ar_toastrelid: row.reltoastrelid,
                    ar_hasrelopts: row.avopts.is_some(),
                    ar_reloptions: row.avopts.unwrap_or(EMPTY_AV_OPTS),
                });
            }
        }
        genam::systable_endscan(smcx, scan)?;

        // Pass 2: toast tables, falling back to the main rel's reloptions.
        let mut scan = genam::systable_beginscan(smcx, &rd, InvalidOid, false, None, &[])?;
        while let Some(tup) = genam::systable_getnext(smcx, &mut scan)? {
            let row = decode_av_class_row(smcx, desc, tup)?;
            if row.relkind != RELKIND_TOASTVALUE {
                continue;
            }
            if row.relpersistence == types_core::RELPERSISTENCE_TEMP {
                continue;
            }
            let mut avopts = row.avopts;
            if avopts.is_none() {
                if let Some(h) = table_toast_map
                    .iter()
                    .find(|h| h.ar_toastrelid == row.oid && h.ar_hasrelopts)
                {
                    avopts = Some(h.ar_reloptions);
                }
            }
            let tabentry = pgstat::pgstat_fetch_stat_tabentry_ext(row.relisshared, row.oid);
            let (dovacuum, _doanalyze, _wraparound) = relation_needs_vacanalyze(
                row.oid,
                avopts.as_ref(),
                &row,
                tabentry.as_ref(),
                effective_multixact_freeze_max_age,
            );
            if dovacuum {
                table_oids.push(row.oid);
            }
        }
        genam::systable_endscan(smcx, scan)?;
        rd.close(AccessShareLock)?;
    }

    // Orphan temp tables: one transaction per drop.
    for &relid in orphan_oids.iter() {
        postgres_seams::check_for_interrupts::call()?;

        if !lmgr::ConditionalLockRelationOid(relid, AccessExclusiveLock)? {
            continue;
        }

        let recheck_cx = MemoryContext::new("orphan recheck");
        let Some(row) = fetch_av_class_row(recheck_cx.mcx(), relid)? else {
            lmgr::UnlockRelationOid(relid, AccessExclusiveLock)?;
            continue;
        };
        if !((row.relkind == RELKIND_RELATION || row.relkind == RELKIND_MATVIEW)
            && row.relpersistence == types_core::RELPERSISTENCE_TEMP)
        {
            lmgr::UnlockRelationOid(relid, AccessExclusiveLock)?;
            continue;
        }
        if catalog_namespace::checkTempNamespaceStatus(row.relnamespace)?
            != TempNamespaceStatus::Idle
        {
            lmgr::UnlockRelationOid(relid, AccessExclusiveLock)?;
            continue;
        }
        // Deadlock guard against an incoming backend's RemoveTempRelations.
        if !lmgr::ConditionalLockDatabaseObject(
            NAMESPACE_RELATION_ID,
            row.relnamespace,
            0,
            AccessShareLock,
        )? {
            lmgr::UnlockRelationOid(relid, AccessExclusiveLock)?;
            continue;
        }

        let datname = dbcommands_seams::get_database_name::call(g::MyDatabaseId())?
            .unwrap_or_default();
        let nspname = syscache_seams::pg_namespace_nspname::call(row.relnamespace)?
            .map(|n| String::from_utf8_lossy(n.name_str()).into_owned())
            .unwrap_or_default();
        elog::elog(
            LOG,
            format!(
                "autovacuum: dropping orphan temp table \"{}.{}.{}\"",
                datname, nspname, row.relname
            ),
        )?;

        let snapshot = snapmgr::GetTransactionSnapshot()?;
        snapmgr::PushActiveSnapshot(&snapshot)?;
        dependency_seams::perform_deletion::call(
            recheck_cx.mcx(),
            RELATION_RELATION_ID,
            relid,
            0,
            DropBehavior::DROP_CASCADE,
            PERFORM_DELETION_INTERNAL | PERFORM_DELETION_QUIETLY | PERFORM_DELETION_SKIP_EXTENSIONS,
        )?;
        snapmgr::PopActiveSnapshot()?;
        xact::CommitTransactionCommand()?;
        xact::StartTransactionCommand()?;
    }

    let bstrategy: BufferAccessStrategy = bufmgr::GetAccessStrategyWithSize(
        BufferAccessStrategyType::BasVacuum,
        g::VacuumBufferUsageLimit(),
    );

    let mut did_vacuum = false;
    let mut found_concurrent_worker = false;

    for &relid in table_oids.iter() {
        postgres_seams::check_for_interrupts::call()?;

        // Config changes apply per table; do NOT bail if autovacuum is now
        // disabled — this could be an anti-wraparound emergency worker.
        if interrupt::ConfigReloadPending() {
            interrupt::SetConfigReloadPending(false);
            guc_file_seams::process_config_file::call(types_guc::GucContext::PGC_SIGHUP)?;
        }

        let table_cx = MemoryContext::new("autovacuum per-table");
        let tmcx = table_cx.mcx();

        let Some(precheck) = fetch_av_class_row(tmcx, relid)? else {
            continue;
        };
        let isshared = precheck.relisshared;

        let my = MY_WORKER_INFO.get().expect("worker has a WorkerInfo slot");
        let slots = shmem::worker_slots();
        {
            let _sched = shmem::av_schedule_lock();
            {
                let l = shmem::av_lock();
                let mut skipit = false;
                for &widx in &l.running_workers {
                    if widx == my {
                        continue;
                    }
                    let w = &slots[widx];
                    if !w.wi_sharedrel.load(Relaxed)
                        && w.wi_dboid.load(Relaxed) != g::MyDatabaseId()
                    {
                        continue;
                    }
                    if w.wi_tableoid.load(Relaxed) == relid {
                        skipit = true;
                        found_concurrent_worker = true;
                        break;
                    }
                }
                drop(l);
                if skipit {
                    continue;
                }
            }
            slots[my].wi_tableoid.store(relid, Relaxed);
            slots[my].wi_sharedrel.store(isshared, Relaxed);
        }

        let tab = match table_recheck_autovac(
            tmcx,
            relid,
            &table_toast_map,
            effective_multixact_freeze_max_age,
        )? {
            Some(t) => t,
            None => {
                let _sched = shmem::av_schedule_lock();
                slots[my].wi_tableoid.store(InvalidOid, Relaxed);
                slots[my].wi_sharedrel.store(false, Relaxed);
                continue;
            }
        };

        AV_STORAGE_PARAM_COST_DELAY.set(tab.at_storage_param_vac_cost_delay);
        AV_STORAGE_PARAM_COST_LIMIT.set(tab.at_storage_param_vac_cost_limit);
        slots[my].wi_dobalance.store(tab.at_dobalance, Relaxed);
        {
            let l = shmem::av_lock();
            autovac_recalculate_workers_for_balance(&l);
        }
        VacuumUpdateCosts()?;

        // Refetch names just before vacuuming; a rel dropped since the recheck
        // is skipped (C's `goto deleted`, no did_vacuum).
        let relname = fetch_av_class_row(tmcx, tab.at_relid)?.map(|r| r.relname);
        let nspname = syscache_seams::pg_namespace_nspname::call(tab.at_nspname)?
            .map(|n| String::from_utf8_lossy(n.name_str()).into_owned());
        let datname = dbcommands_seams::get_database_name::call(g::MyDatabaseId())?;

        if let (Some(relname), Some(nspname), Some(datname)) = (relname, nspname, datname) {
            autovac_report_activity(&tab, &nspname, &relname);
            let vac_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                #[cfg(debug_assertions)]
                if std::env::var("PGRUST_TEST_AUTOVAC_PANIC_TABLE").as_deref() == Ok(relname.as_str()) {
                    panic!("injected autovacuum panic for containment test: {relname}");
                }
                autovacuum_do_vac_analyze(tmcx, &tab, bstrategy.clone())
            }))
            .unwrap_or_else(|payload| Err(Box::new(pg_error_from_panic(payload, "autovacuum worker panicked"))));
            match vac_result {
                Ok(()) => {
                    g::SetQueryCancelPending(false);
                }
                Err(mut err) => {
                    // C's PG_CATCH: adorn, report, abort, restart, continue.
                    // FATAL never reaches C's PG_CATCH (errfinish proc_exits).
                    if err.level() >= FATAL {
                        return Err(err);
                    }
                    g::HoldInterrupts();
                    let what = if tab.at_params.options & VACOPT_VACUUM != 0 {
                        "automatic vacuum of table"
                    } else {
                        "automatic analyze of table"
                    };
                    err.add_context_line(format!("{what} \"{datname}.{nspname}.{relname}\""));
                    elog::emit_error_report_for(&err);
                    xact::AbortOutOfAnyTransaction()?;
                    elog::FlushErrorState();
                    xact::StartTransactionCommand()?;
                    g::ResumeInterrupts();
                }
            }
            did_vacuum = true;
        }

        {
            let _sched = shmem::av_schedule_lock();
            slots[my].wi_tableoid.store(InvalidOid, Relaxed);
            slots[my].wi_sharedrel.store(false, Relaxed);
        }
        slots[my].wi_dobalance.store(true, Relaxed);
    }

    // Work items requested by backends. The only C type is BRIN autosummarize,
    // whose producer (AutoVacuumRequestWork caller) is the brin summarize lane.
    {
        let l = shmem::av_lock();
        for i in 0..NUM_WORKITEMS {
            let wi = &l.work_items[i];
            if wi.avw_used && !wi.avw_active && wi.avw_database == g::MyDatabaseId() {
                unported("perform_work_item: AVW_BRINSummarizeRange (brin autosummarize lane)");
            }
        }
    }

    if did_vacuum || !found_concurrent_worker {
        commands_vacuum::vac_update_datfrozenxid(mcx)?;
    }

    xact::CommitTransactionCommand()?;
    Ok(())
}

const EMPTY_AV_OPTS: AutoVacOpts = AutoVacOpts {
    enabled: true,
    vacuum_threshold: -1,
    vacuum_max_threshold: -2,
    vacuum_ins_threshold: -2,
    analyze_threshold: -1,
    vacuum_cost_limit: 0,
    freeze_min_age: -1,
    freeze_max_age: -1,
    freeze_table_age: -1,
    multixact_freeze_min_age: -1,
    multixact_freeze_max_age: -1,
    multixact_freeze_table_age: -1,
    log_min_duration: -1,
    vacuum_cost_delay: -1.0,
    vacuum_scale_factor: -1.0,
    vacuum_ins_scale_factor: -1.0,
    analyze_scale_factor: -1.0,
};

fn autovacuum_do_vac_analyze(
    mcx: Mcx<'_>,
    tab: &AutovacTable,
    bstrategy: BufferAccessStrategy,
) -> PgResult<()> {
    let mut n = Node::build::<VacuumRelation>(mcx)?;
    n.relation = None;
    n.oid = tab.at_relid;
    let rel = n.seal();
    let rel_list = NodeList::make1(mcx, rel)?;
    commands_vacuum::vacuum(mcx, &rel_list, &tab.at_params, bstrategy, true)
}

fn autovac_report_activity(tab: &AutovacTable, nspname: &str, relname: &str) {
    const MAX_AUTOVAC_ACTIV_LEN: usize = 64 * 2 + 56;
    let mut activity = if tab.at_params.options & VACOPT_VACUUM != 0 {
        if tab.at_params.options & VACOPT_ANALYZE != 0 {
            String::from("autovacuum: VACUUM ANALYZE")
        } else {
            String::from("autovacuum: VACUUM")
        }
    } else {
        String::from("autovacuum: ANALYZE")
    };
    let suffix = format!(
        " {}.{}{}",
        nspname,
        relname,
        if tab.at_params.is_wraparound { " (to prevent wraparound)" } else { "" }
    );
    let room = (MAX_AUTOVAC_ACTIV_LEN - 1).saturating_sub(activity.len());
    let mut end = suffix.len().min(room);
    while end > 0 && !suffix.is_char_boundary(end) {
        end -= 1;
    }
    activity.push_str(&suffix[..end]);
    backend_status_seams::pgstat_report_activity::call(
        backend_status_seams::BackendState::STATE_RUNNING,
        Some(&activity),
    );
}

fn table_recheck_autovac(
    mcx: Mcx<'_>,
    relid: Oid,
    table_toast_map: &[AvRelation],
    effective_multixact_freeze_max_age: i32,
) -> PgResult<Option<AutovacTable>> {
    let Some(row) = fetch_av_class_row(mcx, relid)? else {
        return Ok(None);
    };

    let mut avopts = row.avopts;
    if avopts.is_none() && row.relkind == RELKIND_TOASTVALUE {
        if let Some(h) = table_toast_map
            .iter()
            .find(|h| h.ar_toastrelid == relid && h.ar_hasrelopts)
        {
            avopts = Some(h.ar_reloptions);
        }
    }

    let tabentry = pgstat::pgstat_fetch_stat_tabentry_ext(row.relisshared, relid);
    let (dovacuum, mut doanalyze, wraparound) = relation_needs_vacanalyze(
        relid,
        avopts.as_ref(),
        &row,
        tabentry.as_ref(),
        effective_multixact_freeze_max_age,
    );
    if row.relkind == RELKIND_TOASTVALUE {
        doanalyze = false;
    }

    if !(dovacuum || doanalyze) {
        return Ok(None);
    }

    let av = avopts.as_ref();
    let log_min_duration = match av {
        Some(a) if a.log_min_duration >= 0 => a.log_min_duration,
        _ => Log_autovacuum_min_duration(),
    };
    let freeze_min_age = match av {
        Some(a) if a.freeze_min_age >= 0 => a.freeze_min_age,
        _ => DEFAULT_FREEZE_MIN_AGE.get(),
    };
    let freeze_table_age = match av {
        Some(a) if a.freeze_table_age >= 0 => a.freeze_table_age,
        _ => DEFAULT_FREEZE_TABLE_AGE.get(),
    };
    let multixact_freeze_min_age = match av {
        Some(a) if a.multixact_freeze_min_age >= 0 => a.multixact_freeze_min_age,
        _ => DEFAULT_MULTIXACT_FREEZE_MIN_AGE.get(),
    };
    let multixact_freeze_table_age = match av {
        Some(a) if a.multixact_freeze_table_age >= 0 => a.multixact_freeze_table_age,
        _ => DEFAULT_MULTIXACT_FREEZE_TABLE_AGE.get(),
    };

    let options = (if dovacuum {
        VACOPT_VACUUM | VACOPT_PROCESS_MAIN | VACOPT_SKIP_DATABASE_STATS
    } else {
        0
    }) | (if doanalyze { VACOPT_ANALYZE } else { 0 })
        | (if !wraparound { VACOPT_SKIP_LOCKED } else { 0 });

    let params = VacuumParams {
        options,
        freeze_min_age,
        freeze_table_age,
        multixact_freeze_min_age,
        multixact_freeze_table_age,
        is_wraparound: wraparound,
        log_min_duration,
        index_cleanup: VacOptValue::Unspecified,
        truncate: VacOptValue::Unspecified,
        toast_parent: InvalidOid,
        max_eager_freeze_failure_rate: guc_tables::vars::vacuum_max_eager_freeze_failure_rate
            .read(),
        nworkers: -1,
    };

    Ok(Some(AutovacTable {
        at_relid: relid,
        at_params: params,
        at_storage_param_vac_cost_limit: av.map(|a| a.vacuum_cost_limit).unwrap_or(0),
        at_storage_param_vac_cost_delay: av.map(|a| a.vacuum_cost_delay).unwrap_or(-1.0),
        at_dobalance: !av.is_some_and(|a| a.vacuum_cost_limit > 0 || a.vacuum_cost_delay >= 0.0),
        at_nspname: row.relnamespace,
    }))
}

fn relation_needs_vacanalyze(
    relid: Oid,
    relopts: Option<&AutoVacOpts>,
    row: &AvClassRow,
    tabentry: Option<&pgstat::PgStat_StatTabEntry>,
    effective_multixact_freeze_max_age: i32,
) -> (bool, bool, bool) {
    let vac_scale_factor = match relopts {
        Some(r) if r.vacuum_scale_factor >= 0.0 => r.vacuum_scale_factor as f32,
        _ => autovacuum_vac_scale() as f32,
    };
    let vac_base_thresh = match relopts {
        Some(r) if r.vacuum_threshold >= 0 => r.vacuum_threshold,
        _ => autovacuum_vac_thresh(),
    };
    // -1 disables the max threshold.
    let vac_max_thresh = match relopts {
        Some(r) if r.vacuum_max_threshold >= -1 => r.vacuum_max_threshold,
        _ => autovacuum_vac_max_thresh(),
    };
    let vac_ins_scale_factor = match relopts {
        Some(r) if r.vacuum_ins_scale_factor >= 0.0 => r.vacuum_ins_scale_factor as f32,
        _ => autovacuum_vac_ins_scale() as f32,
    };
    // -1 disables insert vacuums.
    let vac_ins_base_thresh = match relopts {
        Some(r) if r.vacuum_ins_threshold >= -1 => r.vacuum_ins_threshold,
        _ => autovacuum_vac_ins_thresh(),
    };
    let anl_scale_factor = match relopts {
        Some(r) if r.analyze_scale_factor >= 0.0 => r.analyze_scale_factor as f32,
        _ => autovacuum_anl_scale() as f32,
    };
    let anl_base_thresh = match relopts {
        Some(r) if r.analyze_threshold >= 0 => r.analyze_threshold,
        _ => autovacuum_anl_thresh(),
    };
    let freeze_max_age = match relopts {
        Some(r) if r.freeze_max_age >= 0 => {
            r.freeze_max_age.min(g::autovacuum_freeze_max_age())
        }
        _ => g::autovacuum_freeze_max_age(),
    };
    let multixact_freeze_max_age = match relopts {
        Some(r) if r.multixact_freeze_max_age >= 0 => {
            r.multixact_freeze_max_age.min(effective_multixact_freeze_max_age)
        }
        _ => effective_multixact_freeze_max_age,
    };
    let av_enabled = relopts.map(|r| r.enabled).unwrap_or(true);

    let mut xid_force_limit = RECENT_XID.get().wrapping_sub(freeze_max_age as u32);
    if xid_force_limit < FirstNormalTransactionId {
        xid_force_limit = xid_force_limit.wrapping_sub(FirstNormalTransactionId);
    }
    let mut force_vacuum = TransactionIdIsNormal(row.relfrozenxid)
        && TransactionIdPrecedes(row.relfrozenxid, xid_force_limit);
    if !force_vacuum {
        let mut multi_force_limit =
            RECENT_MULTI.get().wrapping_sub(multixact_freeze_max_age as u32);
        if multi_force_limit < FirstMultiXactId {
            multi_force_limit = multi_force_limit.wrapping_sub(FirstMultiXactId);
        }
        force_vacuum = MultiXactIdIsValid(row.relminmxid)
            && MultiXactIdPrecedes(row.relminmxid, multi_force_limit);
    }
    let wraparound = force_vacuum;

    if !av_enabled && !force_vacuum {
        return (false, false, wraparound);
    }

    let dovacuum;
    let doanalyze;
    if let Some(tabentry) = tabentry.filter(|_| AutoVacuumingActive()) {
        let vactuples = tabentry.dead_tuples as f32;
        let instuples = tabentry.ins_since_vacuum as f32;
        let anltuples = tabentry.mod_since_analyze as f32;

        let reltuples = if row.reltuples < 0.0 { 0.0 } else { row.reltuples };
        let mut pcnt_unfrozen = 1.0f32;
        if row.relpages > 0 && row.relallfrozen > 0 {
            let relallfrozen = row.relallfrozen.min(row.relpages);
            pcnt_unfrozen = 1.0 - (relallfrozen as f32 / row.relpages as f32);
        }

        let mut vacthresh = vac_base_thresh as f32 + vac_scale_factor * reltuples;
        if vac_max_thresh >= 0 && vacthresh > vac_max_thresh as f32 {
            vacthresh = vac_max_thresh as f32;
        }
        let vacinsthresh =
            vac_ins_base_thresh as f32 + vac_ins_scale_factor * reltuples * pcnt_unfrozen;
        let anlthresh = anl_base_thresh as f32 + anl_scale_factor * reltuples;

        dovacuum = force_vacuum
            || vactuples > vacthresh
            || (vac_ins_base_thresh >= 0 && instuples > vacinsthresh);
        doanalyze = anltuples > anlthresh;
    } else {
        dovacuum = force_vacuum;
        doanalyze = false;
    }

    let doanalyze = if relid == StatisticRelationId { false } else { doanalyze };
    (dovacuum, doanalyze, wraparound)
}

pub fn AutoVacuumRequestWork(av_type: i32, relation_id: Oid, blkno: types_core::BlockNumber) -> bool {
    debug_assert_eq!(av_type, AVW_BRIN_SUMMARIZE_RANGE);
    let mut l = shmem::av_lock();
    for i in 0..NUM_WORKITEMS {
        if l.work_items[i].avw_used {
            continue;
        }
        l.work_items[i] = shmem::WorkItem {
            avw_type: av_type,
            avw_used: true,
            avw_active: false,
            avw_database: g::MyDatabaseId(),
            avw_relation: relation_id,
            avw_block_number: blkno,
        };
        return true;
    }
    false
}

#[cold]
#[inline(never)]
fn unported(unit: &str) -> ! {
    panic!("unported callee reached from autovacuum.c: {unit}");
}

#[cold]
#[inline(never)]
fn loc(routine: &'static str) -> types_error::ErrorLocation {
    types_error::ErrorLocation::new(file!(), line!() as i32, routine)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(reltuples: f32, relpages: i32, relallfrozen: i32) -> AvClassRow {
        AvClassRow {
            oid: 50000,
            relname: String::from("t"),
            relnamespace: 2200,
            reltoastrelid: 0,
            relpages,
            reltuples,
            relallfrozen,
            relisshared: false,
            relpersistence: b'p',
            relkind: RELKIND_RELATION,
            relam: 2,
            relfrozenxid: 700,
            relminmxid: 1,
            avopts: None,
        }
    }

    fn entry(dead: i64, ins: i64, modd: i64) -> pgstat::PgStat_StatTabEntry {
        pgstat::PgStat_StatTabEntry {
            dead_tuples: dead,
            ins_since_vacuum: ins,
            mod_since_analyze: modd,
            ..Default::default()
        }
    }

    fn setup() {
        guc_tables::vars::pgstat_track_counts.install_if_absent(guc_tables::GucVarAccessors {
            get: || true,
            set: |_| {},
        });
        // Young cluster: age(relfrozenxid=700) < autovacuum_freeze_max_age.
        RECENT_XID.set(100_000_000);
        RECENT_MULTI.set(1);
    }

    fn decide(
        row: &AvClassRow,
        tab: Option<&pgstat::PgStat_StatTabEntry>,
        opts: Option<&AutoVacOpts>,
    ) -> (bool, bool, bool) {
        relation_needs_vacanalyze(row.oid, opts, row, tab, 400_000_000)
    }

    // Threshold matrix, byte-matching C's formulas at default GUCs:
    // vacthresh = 50 + 0.2*reltuples (cap 100000000), anlthresh = 50 + 0.1*t,
    // insthresh = 1000 + 0.2*t*pcnt_unfrozen.
    #[test]
    fn threshold_matrix() {
        setup();
        let r = row(1000.0, 10, 0);
        assert_eq!(decide(&r, Some(&entry(251, 0, 0)), None), (true, false, false));
        assert_eq!(decide(&r, Some(&entry(250, 0, 0)), None), (false, false, false));
        assert_eq!(decide(&r, Some(&entry(0, 0, 151)), None), (false, true, false));
        assert_eq!(decide(&r, Some(&entry(0, 0, 150)), None), (false, false, false));
        assert_eq!(decide(&r, Some(&entry(0, 1201, 0)), None), (true, false, false));
        assert_eq!(decide(&r, Some(&entry(0, 1200, 0)), None), (false, false, false));

        // Fully frozen table scales the insert threshold by pcnt_unfrozen = 0.
        let frozen = row(1000.0, 10, 10);
        assert_eq!(decide(&frozen, Some(&entry(0, 1001, 0)), None), (true, false, false));

        // vac_max_thresh cap (autovacuum_vacuum_max_threshold = 100000000):
        // uncapped vacthresh would be 2e8+50. Values f32-exact (C float4 math).
        let big = row(1e9, 1000, 0);
        assert_eq!(decide(&big, Some(&entry(200_000_000, 0, 0)), None), (true, false, false));
        assert_eq!(decide(&big, Some(&entry(50_000_000, 0, 0)), None), (false, false, false));

        // reltuples < 0 (never vacuumed) is treated as 0.
        let unk = row(-1.0, 10, 0);
        assert_eq!(decide(&unk, Some(&entry(51, 0, 0)), None), (true, false, false));

        // No pgstat entry: only the force arm can fire.
        assert_eq!(decide(&r, None, None), (false, false, false));
    }

    #[test]
    fn wraparound_force_overrides_disabled() {
        setup();
        RECENT_XID.set(250_000_000);
        // relfrozenxid 700 precedes 250M - 200M = 50M => force.
        let r = row(1000.0, 10, 0);
        let disabled = AutoVacOpts { enabled: false, ..EMPTY_AV_OPTS };
        assert_eq!(decide(&r, None, Some(&disabled)), (true, false, true));
        assert_eq!(decide(&r, Some(&entry(0, 0, 0)), None), (true, false, true));

        // Not at risk => reloption-disabled table is skipped entirely.
        RECENT_XID.set(300_000);
        assert_eq!(decide(&r, Some(&entry(9999, 0, 9999)), Some(&disabled)), (false, false, false));
    }

    #[test]
    fn per_table_reloptions_override() {
        setup();
        let r = row(1000.0, 10, 0);
        let opts = AutoVacOpts {
            vacuum_threshold: 10,
            vacuum_scale_factor: 0.0,
            analyze_threshold: 10,
            analyze_scale_factor: 0.0,
            ..EMPTY_AV_OPTS
        };
        assert_eq!(decide(&r, Some(&entry(11, 0, 11)), Some(&opts)), (true, true, false));
        assert_eq!(decide(&r, Some(&entry(10, 0, 10)), Some(&opts)), (false, false, false));
    }

    #[test]
    fn pg_statistic_never_analyzed() {
        setup();
        let mut r = row(1000.0, 10, 0);
        r.oid = StatisticRelationId;
        assert_eq!(decide(&r, Some(&entry(9999, 0, 9999)), None), (true, false, false));
    }
}
