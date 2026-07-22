// Gather/GatherMerge e2e: real worker threads run ParallelQueryMain, tuples
// return through the queues. Boot = substrate e2e (no postmaster thread,
// InvalidOid database) + the execmain-test syscache stubs. Plans are
// hand-built (planner emits none before phase 3); expectations match C 18.3
// debug_parallel_query-forced equivalents.
use std::sync::atomic::{AtomicI32, Ordering::Relaxed};
use std::sync::{Mutex, Once};

use ::datum::Datum;
use ::mcx::MemoryContext;
use ::tcop_dest::DestReceiver;
use ::types_dest::CommandDest;
use ::types_nodes::list::NodeList;
use ::types_nodes::node_tree::Node;
use ::types_nodes::nodes_enums::CmdType;
use ::types_nodes::plannodes::{Gather, GatherMerge, PlannedStmt, Result as ResultPlan, ValuesScan};
use ::types_nodes::primnodes::OUTER_VAR;
use ::types_core::InvalidOid;
use ::types_error::PgResult;
use ::types_portal::{ParamListHandle, QueryEnvHandle};
use ::types_scan::sdir::ForwardScanDirection;
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_startup::StartupData;
use ::types_tuple::{PgTypeShape, TYPALIGN_INT, TYPSTORAGE_PLAIN};

use init_small::globals as g;

const INT4OID: u32 = 23;
const INT4_LT: u32 = 97;
const INTEGER_BTREE_FAM: u32 = 1976;
const BTREE_AM: u32 = 403;
const F_BTINT4SORTSUPPORT: u32 = 3130;

fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

static NEXT_PID: AtomicI32 = AtomicI32::new(19000);

struct Watchdog(std::sync::Arc<std::sync::atomic::AtomicBool>);
impl Watchdog {
    fn arm(secs: u64, label: &'static str) -> Self {
        let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = std::sync::Arc::clone(&done);
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(secs));
            if !flag.load(Relaxed) {
                eprintln!("WATCHDOG: {label} still running after {secs}s — aborting");
                std::process::abort();
            }
        });
        Watchdog(done)
    }
}
impl Drop for Watchdog {
    fn drop(&mut self) {
        self.0.store(true, Relaxed);
    }
}

fn thread_guc_boot() {
    std::thread_local! {
        static ARMED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
    ARMED.with(|armed| {
        if !armed.get() {
            guc::store::initialize_guc_options().unwrap();
            armed.set(true);
        }
    });
}

fn thread_globals() {
    g::SetMaxConnections(16);
    g::set_max_worker_processes(2);
    g::SetMaxBackends(16 + 3 + 2 + 2 + 2);
    g::SetMyDatabaseId(InvalidOid);
    g::set_transaction_buffers(64);
    g::set_subtransaction_buffers(64);
    g::SetDataDir(DATA_DIR.get().unwrap());
    g::set_enableFsync(false);
}

fn stub_seams() {
    pg_sema_seams::pg_semaphore_create::set(|_| {});
    pg_sema_seams::pg_semaphore_reset::set(|_| {});
    pg_sema_seams::pg_semaphore_lock::set(|_| {});
    pg_sema_seams::pg_semaphore_unlock::set(|_| {});
    s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
    s_lock_seams::finish_spin_delay::set(|_| {});
    s_lock_seams::set_spins_per_delay::set(|_| {});
    s_lock_seams::update_spins_per_delay::set(|v| v);
    waitevent_seams::pgstat_set_wait_event_storage::set(|_| {});
    waitevent_seams::pgstat_report_wait_start::set(|_| {});
    waitevent_seams::pgstat_report_wait_end::set(|| {});
    waitevent_seams::pgstat_reset_wait_event_storage::set(|| {});
    ipc_seams::on_shmem_exit::set(|_, _| {});
    ipc_seams::before_shmem_exit::set(|_, _| Ok(()));
    deadlock_seams::init_dead_lock_checking::set(|| Ok(()));
    pmsignal_seams::register_postmaster_child_active::set(|| {});
    syncrep_seams::sync_rep_cleanup_at_proc_exit::set(|| {});
    condition_variable_seams::condition_variable_cancel_sleep::set(|| false);
    autovacuum_seams::wake_autovacuum_launcher::set(|| {});
    lock_seams::abort_strong_lock_acquire::set(|| {});
    lock_seams::get_awaited_lock_hashcode::set(|| None);
    lock_seams::lock_release_all::set(|_, _| lock::VirtualXactLockTableCleanup());
    lock_seams::lock_acquire_extended::set(|_, _, _, _, _, _| {
        Ok(types_storage::lock::LOCKACQUIRE_OK)
    });
    timeout_seams::disable_timeouts::set(|_| {});
    timeout_seams::initialize_timeouts::set(|| {});
    timeout_seams::register_timeout::set(|id, _| id);
    aio_seams::pgaio_closing_fd::set(|_| {});
    aio_seams::pgaio_init_backend::set(|| {});
    aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));
    aio_seams::at_eoxact_aio::set(|_| {});
    aio_seams::pgaio_error_cleanup::set(|| {});
    sync_seams::register_sync_request::set(|_, _, _| Ok(true));
    sync_seams::init_sync::set(|| Ok(()));
    slot_seams::replication_slot_initialize::set(|| Ok(()));
    sinval_seams::receive_shared_invalid_messages::set(|_, _| Ok(()));
    logical_worker_seams::at_eoxact_logical_rep_workers::set(|_| {});
    postgres_seams::check_for_interrupts::set(|| {
        if g::ParallelMessagePending() {
            parallel::ProcessParallelMessages()?;
        }
        Ok(())
    });

    timestamp_seams::get_current_timestamp::set(|| 777_000_000);
    trigger_seams::after_trigger_begin_xact::set(|| Ok(()));
    trigger_seams::after_trigger_end_xact::set(|_| Ok(()));
    trigger_seams::after_trigger_fire_deferred::set(|| Ok(()));
    async_seams::pre_commit_notify::set(|| Ok(()));
    async_seams::at_commit_notify::set(|| Ok(()));
    async_seams::at_abort_notify::set(|| {});
    tablecmds_seams::pre_commit_on_commit_actions::set(|| Ok(()));
    tablecmds_seams::at_eoxact_on_commit_actions::set(|_| {});
    spi_seams::at_eoxact_spi::set(|_| Ok(()));
    spi_seams::spi_inside_nonatomic_context::set(|| false);
    be_fsstubs_seams::at_eoxact_large_object::set(|_| Ok(()));
    namespace_seams::at_eoxact_namespace::set(|_, _| {});
    catalog_index_seams::reset_reindex_state::set(|_| {});
    catalog_storage_seams::smgr_get_pending_deletes::set(|mcx, _| Ok(mcx::PgVec::new_in(mcx)));
    catalog_storage_seams::smgr_do_pending_deletes::set(|_| Ok(()));
    catalog_storage_seams::smgr_do_pending_syncs::set(|_, _| Ok(()));
    multixact_seams::at_eoxact_multixact::set(|| {});
    multixact_seams::multi_xact_id_set_oldest_member::set(|| Ok(()));
    relcache_seams::at_eoxact_relation_cache::set(|_| Ok(()));
    relcache_seams::relation_cache_invalidate::set(|_| Ok(()));
    catcache_seams::reset_catalog_caches_ext::set(|_| Ok(()));
    typcache_seams::at_eoxact_type_cache::set(|| {});
    logical_seams::reset_logical_streaming_state::set(|| {});
    snapbuild_seams::snap_build_reset_exported_snapshot_state::set(|| {});
    origin_seams::replorigin_session_origin::set(|| types_core::InvalidRepOriginId);
    origin_seams::replorigin_session_origin_lsn::set(|| 0);
    origin_seams::replorigin_session_origin_timestamp::set(|| 0);
    origin_seams::set_replorigin_session_origin_timestamp::set(|_| {});
    commit_ts_seams::transaction_tree_set_commit_ts_data::set(|_, _, _, _| Ok(()));
    commit_ts_seams::extend_commit_ts::set(|_| Ok(()));
    syncrep_seams::sync_rep_wait_for_lsn::set(|_, _| Ok(()));
    backend_status_seams::pgstat_report_xact_timestamp::set(|_| {});
    backend_status_seams::pgstat_clear_backend_status_snapshot::set(|| {});
    backend_status_seams::pgstat_report_query_id::set(|_, _| {});
    backend_progress_seams::pgstat_progress_end_command::set(|| {});
    predicate_seams::pre_commit_check_for_serialization_failure::set(|| Ok(()));
    predicate_seams::release_predicate_locks::set(|_, _| Ok(()));
    predicate_seams::share_serializable_xact::set(|| 0);
    predicate_seams::attach_serializable_xact::set(|_| Ok(()));
    pmchild_seams::find_postmaster_child_by_pid::set(|pid| {
        Some((pid, types_core::BackendType::Backend))
    });
    {
        use std::sync::atomic::{AtomicI32 as AI, Ordering::Relaxed as R};
        static DPQ: AI = AI::new(0);
        guc_tables::vars::debug_parallel_query.install_if_absent(guc_tables::GucVarAccessors {
            get: || DPQ.load(R),
            set: |v| DPQ.store(v, R),
        });
    }
    // The executor-facing catalog stubs (execmain/src/tests.rs shapes).
    syscache_seams::lookup_pg_type_shape::set(|typid| {
        Ok((typid == INT4OID).then_some(PgTypeShape {
            typlen: 4,
            typbyval: true,
            typalign: TYPALIGN_INT,
            typstorage: TYPSTORAGE_PLAIN,
            typcollation: 0,
        }))
    });
    syscache_seams::lookup_pg_amop_members_by_operator::set(|mcx, opno| {
        let mut v = ::mcx::PgVec::new_in(mcx);
        assert_eq!(opno, INT4_LT, "unexpected amop probe");
        v.push(syscache_seams::PgAmopMemberShape {
            amopfamily: INTEGER_BTREE_FAM,
            amoplefttype: INT4OID,
            amoprighttype: INT4OID,
            amopstrategy: 1,
            amopmethod: BTREE_AM,
        });
        Ok(v)
    });
    syscache_seams::lookup_pg_amproc::set(|opfamily, left, right, procnum| {
        assert_eq!((opfamily, left, right, procnum), (INTEGER_BTREE_FAM, INT4OID, INT4OID, 2));
        Ok(F_BTINT4SORTSUPPORT)
    });
}

fn setup() {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        let dir = std::env::temp_dir().join(format!("pgrust_gather_e2e_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        for sub in ["global", "pg_wal", "pg_xact", "pg_subtrans"] {
            std::fs::create_dir_all(dir.join(sub)).unwrap();
        }
        std::env::set_current_dir(&dir).unwrap();
        let dir_str: &'static str = Box::leak(dir.to_str().unwrap().to_string().into_boxed_str());
        DATA_DIR.set(dir_str).unwrap();
        thread_globals();
        g::SetMyProcPid(1779);

        stub_seams();
        shmem::init_seams();
        fd::init_seams();
        guc_tables::init_seams();
        guc::init_seams();
        adt_bool::init_seams();
        adt_float::init_seams();
        transam_xlog::init_seams();
        xloginsert::init_seams();
        xlogutils::init_seams();
        heapam_visibility::init_seams();
        clog::init_seams();
        subtrans::init_seams();
        transam::init_seams();
        varsup::init_seams();
        xact::init_seams();
        snapmgr::init_seams();
        resowner::init_seams();
        procarray::init_seams();
        inval::init_seams();
        pgstat::init_seams();
        waiteventset::init_seams();
        latch::init_seams();
        miscinit::init_seams();
        combocid::init_seams();
        pg_enum::init_seams();
        parallel::init_seams();
        bgworker::init_seams();
        execmain::init_seams();
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
        thread_guc_boot();
        // init_small's GUC accessors write the process globals; the boot
        // defaults just clobbered ours — re-assert.
        thread_globals();

        lwlock::CreateLWLocks(false).unwrap();
        lmgr_proc::init_seams();
        lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
            autovacuum_worker_slots: 3,
            max_wal_senders: 2,
            max_prepared_xacts: 2,
            fastpath_lock_groups_per_backend: 1,
        });
        varsup::VarsupShmemInit();
        procarray::ProcArrayShmemInit();
        clog::CLOGShmemInit().unwrap();
        clog::BootStrapCLOG().unwrap();
        subtrans::SUBTRANSShmemInit().unwrap();
        subtrans::BootStrapSUBTRANS().unwrap();

        test_boot_control_file(dir_str);
        transam_xlog::ReadControlFile().unwrap();
        transam_xlog::XLOGShmemInit();
        boot_xlog_ctl();
        subtrans::StartupSUBTRANS(3).unwrap();

        pmsignal::PMSignalShmemInit(64);
        bgworker::BackgroundWorkerShmemInit();
        procsignal::ProcSignalShmemInit();
    });
    leader_thread_boot();
}

fn leader_thread_boot() {
    std::thread_local! {
        static ARMED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
    ARMED.with(|armed| {
        if armed.get() {
            return;
        }
        thread_guc_boot();
        thread_globals();
        g::SetMyProcPid(NEXT_PID.fetch_add(1, Relaxed));
        fd::InitFileAccess();
        waiteventset::InitializeWaitEventSupport().unwrap();
        miscinit::InitProcessLocalLatch();
        lmgr_proc::InitProcess(types_core::BackendType::Backend).unwrap();
        procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).unwrap();
        // Before InitializeLatchWaitSet so the cached set reserves the inert
        // postmaster-death position (this thread stays "under postmaster":
        // RegisterDynamicBackgroundWorker runs mid-ExecGather).
        g::SetIsUnderPostmaster(true);
        latch::InitializeLatchWaitSet().unwrap();
        procsignal::ProcSignalInit(&[]).unwrap();
        miscinit::SetAuthenticatedUserId(10);
        miscinit::SetSessionAuthorization(10, true).unwrap();
        armed.set(true);
    });
}

static DATA_DIR: std::sync::OnceLock<&'static str> = std::sync::OnceLock::new();

const SEG: usize = 16 * 1024 * 1024;

fn test_boot_control_file(dir: &str) {
    let mut cf = controldata_utils::ControlFileData::ZEROED;
    cf.system_identifier = 0x5544_3322_1100_BBCE;
    cf.pg_control_version = controldata_utils::PG_CONTROL_VERSION;
    cf.catalog_version_no = controldata_utils::CATALOG_VERSION_NO;
    cf.state = transam_xlog::DB_IN_PRODUCTION;
    cf.checkPoint = SEG as u64 + 40;
    cf.checkPointCopy.redo = SEG as u64 + 40;
    cf.checkPointCopy.ThisTimeLineID = 1;
    cf.checkPointCopy.PrevTimeLineID = 1;
    cf.checkPointCopy.nextXid = types_core::FullTransactionId::from_epoch_and_xid(0, 3);
    cf.unloggedLSN = transam_xlog::control_file::FirstNormalUnloggedLSN;
    cf.maxAlign = 8;
    cf.floatFormat = transam_xlog::control_file::FLOATFORMAT_VALUE;
    cf.blcksz = 8192;
    cf.relseg_size = 131072;
    cf.xlog_blcksz = 8192;
    cf.xlog_seg_size = SEG as u32;
    cf.nameDataLen = 64;
    cf.indexMaxKeys = 32;
    cf.toast_max_chunk_size = transam_xlog::control_file::TOAST_MAX_CHUNK_SIZE;
    cf.loblksize = 2048;
    cf.float8ByVal = true;
    cf.crc = controldata_utils::crc_of_image(&cf.to_disk_bytes());
    let mut image = vec![0u8; transam_xlog::control_file::PG_CONTROL_FILE_SIZE];
    image[..controldata_utils::SIZEOF_CONTROL_FILE_DATA].copy_from_slice(&cf.to_disk_bytes());
    std::fs::write(format!("{dir}/global/pg_control"), &image).unwrap();
}

fn boot_xlog_ctl() {
    use transam_xlog::XLogRecPtrToBytePos;
    let end_of_log = 2 * SEG as u64;
    let prev_rec = SEG as u64 + 40;
    let ctl = transam_xlog::ctl::XLogCtl();
    ctl.InsertTimeLineID.store(1, Relaxed);
    ctl.PrevTimeLineID.store(1, Relaxed);
    ctl.Insert.CurrBytePos.store(XLogRecPtrToBytePos(end_of_log), Relaxed);
    ctl.Insert.PrevBytePos.store(XLogRecPtrToBytePos(prev_rec), Relaxed);
    ctl.Insert.fullPageWrites.store(true, Relaxed);
    ctl.Insert.RedoRecPtr.store(prev_rec, Relaxed);
    ctl.RedoRecPtr.store(prev_rec, Relaxed);
    ctl.InitializedUpTo.store(end_of_log, Relaxed);
    ctl.logInsertResult.store(end_of_log, Relaxed);
    ctl.logWriteResult.store(end_of_log, Relaxed);
    ctl.logFlushResult.store(end_of_log, Relaxed);
    ctl.LogwrtRqstWrite.store(end_of_log, Relaxed);
    ctl.LogwrtRqstFlush.store(end_of_log, Relaxed);
    ctl.SharedRecoveryState.store(transam_xlog::RECOVERY_STATE_DONE, Relaxed);
    ctl.InstallXLogFileSegmentActive.store(true, Relaxed);
    xlogutils::set_in_recovery(false);
}

// The postmaster stand-in (substrate_e2e.rs shape).
fn launch_registered_workers() -> Vec<std::thread::JoinHandle<i32>> {
    bgworker::BackgroundWorkerStateChange(true);
    let mut joins = Vec::new();
    for idx in bgworker::registered_indexes() {
        if bgworker::rw_pid(idx) != 0 || bgworker::rw_terminate(idx) {
            continue;
        }
        let pid = NEXT_PID.fetch_add(1, Relaxed);
        let slot = bgworker::rw_shmem_slot(idx);
        let generation = bgworker::slot_generation(slot);
        bgworker::set_rw_pid(idx, pid);
        bgworker::ReportBackgroundWorkerPID(idx);
        let guc_snapshot = guc::store::capture_nondefault_variables();
        let handle = std::thread::Builder::new()
            .name(format!("pg:gather-e2e-worker:{pid}"))
            .spawn(move || {
                thread_globals();
                g::SetMyProcPid(pid);
                guc::store::initialize_guc_options_for_child(&guc_snapshot)
                    .and_then(|()| guc::store::restore_nondefault_variables(&guc_snapshot))
                    .unwrap();
                thread_globals();
                waiteventset::InitializeWaitEventSupport().unwrap();
                miscinit::InitProcessLocalLatch();
                latch::InitializeLatchWaitSet().unwrap();
                let sd = StartupData::BgWorker(types_startup::BgWorkerStartupData {
                    slot,
                    generation,
                });
                let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    bgworker::BackgroundWorkerMain(&sd)
                }))
                .unwrap_err();
                let code = payload.downcast_ref::<ipc::ProcExitThread>().map(|p| p.code);
                // proc_exit defers the exit-callback drain to the thread top
                // (run_child_task in the real server; this rig is that top).
                if let Some(code) = code {
                    let _ = ipc::run_deferred_exit_callbacks(code);
                }
                if lmgr_proc::MyProc().is_some() {
                    lmgr_proc::ProcKill(0, 0);
                }
                bgworker::ReportBackgroundWorkerExit(idx);
                code.unwrap_or(27)
            })
            .unwrap();
        joins.push(handle);
    }
    joins
}

fn begin_xact() {
    xact::SetCurrentStatementStartTimestamp();
    xact::StartTransactionCommand().unwrap();
    let snap = snapmgr::GetTransactionSnapshot().unwrap();
    snapmgr::PushActiveSnapshot(&snap).unwrap();
}

fn end_xact() {
    snapmgr::PopActiveSnapshot().unwrap();
    xact::CommitTransactionCommand().unwrap();
}

fn leaked_mcx() -> ::mcx::Mcx<'static> {
    let m: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("gather-e2e")));
    m.mcx()
}

fn mk_int4_const(mcx: ::mcx::Mcx<'_>, v: i32) -> Node<'_> {
    Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::from_i32(v), false, true).unwrap()
}

fn outer_var_tlist(mcx: ::mcx::Mcx<'_>) -> NodeList<'_> {
    let var = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
    let tle = Node::mk_target_entry(mcx, var, 1, Some("x"), false).unwrap();
    NodeList::make1(mcx, tle).unwrap()
}

fn result_const_plan(mcx: ::mcx::Mcx<'_>, v: i32, node_id: i32) -> Node<'_> {
    let tle = Node::mk_target_entry(mcx, mk_int4_const(mcx, v), 1, Some("x"), false).unwrap();
    let mut result = Node::build::<ResultPlan>(mcx).unwrap();
    result.plan.targetlist = NodeList::make1(mcx, tle).unwrap();
    result.plan.plan_node_id = node_id;
    result.plan.parallel_safe = true;
    result.seal()
}

fn gather_pstmt<'m>(
    mcx: ::mcx::Mcx<'m>,
    num_workers: i32,
    single_copy: bool,
    child: Node<'m>,
) -> &'m PlannedStmt<'m> {
    let mut gather = Node::build::<Gather>(mcx).unwrap();
    gather.plan.targetlist = outer_var_tlist(mcx);
    gather.plan.lefttree = Some(child);
    gather.plan.plan_node_id = 0;
    gather.num_workers = num_workers;
    gather.single_copy = single_copy;
    let plan_node = gather.seal();
    let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
    pstmt.commandType = CmdType::CMD_SELECT;
    pstmt.canSetTag = true;
    pstmt.parallelModeNeeded = true;
    pstmt.planTree = Some(plan_node);
    pstmt.seal_ref()
}

// One executor run through a Tuplestore receiver: returns (es_processed,
// attr-1 values in output order).
fn run_once(qd: types_portal::QueryDescHandle) -> PgResult<(u64, Vec<i32>)> {
    let store = tuplestore::Tuplestore::begin_heap(false, false, 1024);
    let h = tuplestore::hold::register(store);
    let mut dest = DestReceiver::Tuplestore(tstore_receiver::tstore_create_DR());
    tcop_dest::SetTuplestoreDestReceiverParams(&mut dest, h, false);
    execmain_seams::executor_run::call(qd, ForwardScanDirection, 0, &mut dest)?;
    let processed = execmain_seams::query_desc_es_processed::call(qd);
    let mcx = leaked_mcx();
    let desc = execmain_seams::query_desc_result_tupdesc::call(qd).unwrap();
    let mut slot = exectuples::make_tuple_table_slot(
        mcx,
        TupleSlotKind::MinimalTuple,
        Some(std::rc::Rc::clone(&desc)),
    );
    let mut values = Vec::new();
    let mut store = tuplestore::hold::take(h).unwrap();
    loop {
        let got = store.gettupleslot(true, false, &mut slot, mcx)?;
        if !got {
            break;
        }
        let mut isnull = false;
        let d = exectuples::slot_getattr(&mut slot, 1, &mut isnull);
        assert!(!isnull);
        values.push(d.as_i32());
    }
    debug_assert!(matches!(slot, SlotData::Minimal(_)));
    store.end();
    Ok((processed, values))
}

// Gather over Result(42): a non-parallel-aware child runs once in each
// participant. Requested 3 workers, only 2 bgworker slots — C's contract is
// to run with fewer; leader participates. Rows = 2 workers + leader = 3.
#[test]
fn gather_runs_child_in_workers_and_leader() {
    let _s = serial();
    let _w = Watchdog::arm(240, "gather_runs_child_in_workers_and_leader");
    setup();
    begin_xact();

    let mcx = leaked_mcx();
    let pstmt = gather_pstmt(mcx, 3, false, result_const_plan(mcx, 42, 1));

    let qd = execmain_seams::create_query_desc::call(
        pstmt,
        "select x (gather)",
        Some(snapmgr::GetActiveSnapshot()),
        None,
        CommandDest::None,
        ParamListHandle::NULL,
        QueryEnvHandle::NULL,
        0,
    )
    .unwrap();
    execmain_seams::executor_start::call(qd, 0).unwrap();

    // Workers launch inside the Gather's first execute; the stand-in
    // postmaster thread starts whatever gets registered while the leader
    // polls its queues.
    let poller = spawn_postmaster_standin();
    let (processed, mut values) = run_once(qd).unwrap();
    let joins = poller.join().unwrap();

    assert_eq!(processed, 3, "2 launched workers + participating leader");
    values.sort_unstable();
    assert_eq!(values, vec![42, 42, 42]);

    execmain_seams::executor_finish::call(qd).unwrap();
    execmain_seams::executor_end::call(qd).unwrap();
    execmain_seams::free_query_desc::call(qd);
    end_xact();
    for j in joins {
        assert_eq!(j.join().unwrap(), 0);
    }
    assert!(!parallel::ParallelContextActive());
}

// LaunchParallelWorkers registers with the (absent) postmaster; a helper
// thread plays maybe_start_bgworkers until the leader's run completes.
fn spawn_postmaster_standin() -> std::thread::JoinHandle<Vec<std::thread::JoinHandle<i32>>> {
    std::thread::spawn(|| {
        thread_globals();
        let mut joins = Vec::new();
        for _ in 0..600 {
            std::thread::sleep(std::time::Duration::from_millis(10));
            let mut new = launch_registered_workers();
            if !new.is_empty() {
                joins.append(&mut new);
                break;
            }
        }
        joins
    })
}

// Gather(single_copy, 1 worker): the leader never runs the plan; exactly the
// worker's row comes back.
#[test]
fn gather_single_copy_runs_only_in_worker() {
    let _s = serial();
    let _w = Watchdog::arm(240, "gather_single_copy_runs_only_in_worker");
    setup();
    begin_xact();

    let mcx = leaked_mcx();
    let pstmt = gather_pstmt(mcx, 1, true, result_const_plan(mcx, 7, 1));

    let qd = execmain_seams::create_query_desc::call(
        pstmt,
        "select x (gather single_copy)",
        Some(snapmgr::GetActiveSnapshot()),
        None,
        CommandDest::None,
        ParamListHandle::NULL,
        QueryEnvHandle::NULL,
        0,
    )
    .unwrap();
    execmain_seams::executor_start::call(qd, 0).unwrap();
    let poller = spawn_postmaster_standin();
    let (processed, values) = run_once(qd).unwrap();
    let joins = poller.join().unwrap();

    assert_eq!(processed, 1);
    assert_eq!(values, vec![7]);

    execmain_seams::executor_finish::call(qd).unwrap();
    execmain_seams::executor_end::call(qd).unwrap();
    execmain_seams::free_query_desc::call(qd);
    end_xact();
    for j in joins {
        assert_eq!(j.join().unwrap(), 0);
    }
}

// Fake-heap fixture (execmain/src/tests.rs scanfix shape) with frozen tuples:
// real heapam + real MVCC visibility run over hand-built pages, only the
// buffer/relation providers are stubbed.
mod heapfix {
    use core::ptr::NonNull;
    use std::collections::HashMap;
    use std::rc::Rc;
    use std::sync::Mutex;

    use ::mcx::{Mcx, PgVec};
    use ::types_core::{Buffer, Oid, BLCKSZ, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT};
    use ::types_rel::{
        FormData_pg_class, LockInfoData, LockRelId, Relation, RelationData, LOCKMODE,
        RELKIND_RELATION,
    };
    use ::types_storage::bufpage::{ItemIdData, SizeOfPageHeaderData, LP_NORMAL};
    use ::types_storage::RelFileLocatorBackend;
    use ::types_tuple::{
        CompactAttribute, FormData_pg_attribute, NameData, TupleDescData, HEAP_XMAX_INVALID,
        HEAP_XMIN_FROZEN, TYPALIGN_INT, TYPSTORAGE_PLAIN,
    };

    struct Fake {
        tables: HashMap<Oid, Vec<Buffer>>,
        pages: Vec<usize>,
        /// Per-relation (relpages, reltuples) — the fixture's ANALYZE:
        /// `register_table` records the true page/row counts so
        /// `fake_relation_open` serves an analyzed pg_class row (the funnel's
        /// emit-fraction FloorGuard fail-closes on reltuples <= 0).
        /// `register_table_unanalyzed` records reltuples = -1 (never analyzed)
        /// for the negative witness.
        stats: HashMap<Oid, (i32, f32)>,
    }

    static FAKE: Mutex<Option<Fake>> = Mutex::new(None);

    fn with_fake<R>(f: impl FnOnce(&mut Fake) -> R) -> R {
        let mut g = FAKE.lock().unwrap_or_else(|e| e.into_inner());
        f(g.get_or_insert_with(|| Fake {
            tables: HashMap::new(),
            pages: Vec::new(),
            stats: HashMap::new(),
        }))
    }

    pub fn install() {
        static INSTALLED: std::sync::Once = std::sync::Once::new();
        INSTALLED.call_once(|| {
            bufmgr_seams::read_buffer::set(|rel, block| {
                with_fake(|f| Ok(f.tables[&rel.rd_id][block as usize]))
            });
            bufmgr_seams::read_buffer_strategy::set(|rel, block, _strategy| {
                bufmgr_seams::read_buffer::call(rel, block)
            });
            bufmgr_seams::buffer_get_block_number::set(|buf| {
                with_fake(|f| {
                    for pages in f.tables.values() {
                        if let Some(i) = pages.iter().position(|b| *b == buf) {
                            return i as u32;
                        }
                    }
                    panic!("unknown buffer {buf}")
                })
            });
            bufmgr_seams::buffer_get_page::set(|buf| {
                let addr = with_fake(|f| f.pages[(buf - 1) as usize]);
                NonNull::new(addr as *mut u8).unwrap()
            });
            bufmgr_seams::release_buffer::set(|_| Ok(()));
            bufmgr_seams::incr_buffer_ref_count::set(|_| {});
            bufmgr_seams::lock_buffer::set(|_buf, _mode| Ok(()));
            bufmgr_seams::get_access_strategy::set(|_| None);
            bufmgr_seams::free_access_strategy::set(|_| {});
            bufmgr_seams::relation_get_number_of_blocks_in_fork::set(|rel, _fork| {
                with_fake(|f| Ok(f.tables[&rel.rd_id].len() as u32))
            });
            bufmgr_seams::relation_smgr_locator::set(|rel| RelFileLocatorBackend {
                locator: rel.rd_locator.get(),
                backend: rel.rd_backend,
            });

            predicate_seams::check_for_serializable_conflict_out_needed::set(|_r, _s| Ok(false));
            predicate_seams::predicate_lock_relation::set(|_r, _s| Ok(()));
            pruneheap_seams::heap_page_prune_opt::set(|_r, _b| Ok(()));

            aclchk_seams::pg_class_aclmask::set(|_objid, _roleid, mask, _how| Ok(mask));
            aclchk_seams::object_aclcheck::set(|_classid, _objid, _roleid, _mode| Ok(0));

            relation_seams::relation_open::set(fake_relation_open);
        });
    }

    // Frozen tuple image: raw xmin = FrozenTransactionId with the frozen hint
    // bits, so REAL MVCC visibility accepts it with no clog probe.
    fn tuple_image(val: i32) -> Vec<u8> {
        let mut img = vec![0u8; 28];
        img[0..4].copy_from_slice(&2u32.to_ne_bytes());
        img[18..20].copy_from_slice(&1u16.to_ne_bytes());
        img[20..22].copy_from_slice(&(HEAP_XMAX_INVALID | HEAP_XMIN_FROZEN).to_ne_bytes());
        img[22] = 24;
        img[24..28].copy_from_slice(&val.to_ne_bytes());
        img
    }

    #[repr(align(8))]
    struct TestPage([u8; BLCKSZ]);

    fn build_page(rows: &[i32]) -> Box<TestPage> {
        let mut page = Box::new(TestPage([0u8; BLCKSZ]));
        let n = rows.len();
        let lower = SizeOfPageHeaderData + n * 4;
        let mut upper = BLCKSZ;
        for (i, val) in rows.iter().enumerate() {
            let img = tuple_image(*val);
            upper = (upper - img.len()) & !7;
            page.0[upper..upper + img.len()].copy_from_slice(&img);
            let id = ItemIdData::new(upper as u16, LP_NORMAL, img.len() as u16);
            let off = SizeOfPageHeaderData + i * 4;
            // SAFETY: repr(transparent) over u32.
            let raw: u32 = unsafe { core::mem::transmute(id) };
            page.0[off..off + 4].copy_from_slice(&raw.to_ne_bytes());
        }
        page.0[12..14].copy_from_slice(&(lower as u16).to_ne_bytes());
        page.0[14..16].copy_from_slice(&(upper as u16).to_ne_bytes());
        page.0[16..18].copy_from_slice(&(BLCKSZ as u16).to_ne_bytes());
        page.0[18..20].copy_from_slice(&((BLCKSZ as u16) | 4).to_ne_bytes());
        page
    }

    /// Register an ANALYZED table: pg_class stats (relpages/reltuples) are
    /// populated from the actual fixture contents, exactly what ANALYZE
    /// leaves behind on a real table.
    pub fn register_table(relid: Oid, pages: &[&[i32]]) {
        let reltuples: usize = pages.iter().map(|p| p.len()).sum();
        register_table_impl(relid, pages, reltuples as f32);
    }

    /// Register a NEVER-ANALYZED table (reltuples = -1): the negative-witness
    /// fixture for stats-gated paths (the funnel FloorGuard must fail-close).
    pub fn register_table_unanalyzed(relid: Oid, pages: &[&[i32]]) {
        register_table_impl(relid, pages, -1.0);
    }

    fn register_table_impl(relid: Oid, pages: &[&[i32]], reltuples: f32) {
        with_fake(|f| {
            if f.tables.contains_key(&relid) {
                return;
            }
            let mut bufs = Vec::new();
            for rows in pages {
                let addr = Box::leak(build_page(rows)).0.as_mut_ptr() as usize;
                f.pages.push(addr);
                bufs.push(f.pages.len() as Buffer);
            }
            f.tables.insert(relid, bufs);
            f.stats.insert(relid, (pages.len() as i32, reltuples));
        });
    }

    fn int4_tupdesc<'mcx>(mcx: Mcx<'mcx>) -> Rc<TupleDescData<'mcx>> {
        let mut attrs = PgVec::new_in(mcx);
        let mut compact = PgVec::new_in(mcx);
        let att = FormData_pg_attribute {
            attnum: 1,
            atttypid: 23,
            atttypmod: -1,
            attlen: 4,
            attbyval: true,
            attalign: TYPALIGN_INT,
            attstorage: TYPSTORAGE_PLAIN,
            ..Default::default()
        };
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
        Rc::new(TupleDescData {
            natts: 1,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: -1,
            constr: None,
            compact_attrs: compact,
            attrs,
        })
    }

    fn fake_relation_open<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
        _lockmode: LOCKMODE,
    ) -> ::types_error::PgResult<Relation<'mcx>> {
        let mut relname = NameData::default();
        relname.namestrcpy("t");
        // The fixture's ANALYZE: registered tables serve their true
        // relpages/reltuples; unregistered (or register_table_unanalyzed)
        // relations stay never-analyzed (reltuples = -1).
        let (relpages, reltuples) =
            with_fake(|f| f.stats.get(&relid).copied().unwrap_or((0, -1.0)));
        let rd_rel = FormData_pg_class {
            relname,
            relnamespace: 2200,
            reltype: 0,
            relowner: 10,
            relam: tableam::HEAP_TABLE_AM_OID,
            relfilenode: relid,
            reltablespace: 0,
            relpages,
            reltuples,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relisshared: false,
            relpersistence: RELPERSISTENCE_PERMANENT,
            relkind: RELKIND_RELATION,
            relhassubclass: false,
            relrowsecurity: false,
            relispopulated: true,
            relreplident: b'd',
            relispartition: false,
            relfrozenxid: 3,
            relminmxid: 1,
        };
        let data = RelationData {
            rd_locator: Default::default(),
            rd_smgr: Default::default(),
            rd_id: relid,
            rd_backend: INVALID_PROC_NUMBER,
            rd_islocaltemp: false,
            rd_isvalid: std::cell::Cell::new(true),
            rd_createSubid: std::cell::Cell::new(0),
            rd_newRelfilelocatorSubid: std::cell::Cell::new(0),
            rd_firstRelfilelocatorSubid: std::cell::Cell::new(0),
            rd_droppedSubid: std::cell::Cell::new(0),
            rd_lockInfo: LockInfoData { lockRelId: LockRelId { relId: relid, dbId: 5 } },
            rd_rel,
            rd_att: int4_tupdesc(mcx),
            rd_index: None,
            rd_opcintype: PgVec::new_in(mcx),
            rd_opfamily: PgVec::new_in(mcx),
            rd_indoption: PgVec::new_in(mcx),
            rd_indcollation: PgVec::new_in(mcx),
            rd_options: None,
            // Off: no pgstat shmem in this harness (the zero-count batched
            // flush is a no-op).
            pgstat_enabled: std::cell::Cell::new(false),
            pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
            rd_amcache: Default::default(),
            rd_amcache_hash: Default::default(),
            rd_amcache_gin: Default::default(),
            rd_amcache_spgist: Default::default(),
            rd_support: PgVec::new_in(mcx),
            rd_supportinfo: Default::default(),
            rd_opcoptions: Default::default(),
            rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false,
            rd_hasrules: false,
        };
        Ok(Relation::open(data, None))
    }
}

fn seqscan_node(mcx: ::mcx::Mcx<'_>, parallel_aware: bool, node_id: i32) -> Node<'_> {
    use ::types_nodes::plannodes::{Plan, Scan, SeqScan};
    let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let tle = Node::mk_target_entry(mcx, var, 1, Some("a"), false).unwrap();
    let plan = Plan {
        targetlist: NodeList::make1(mcx, tle).unwrap(),
        plan_node_id: node_id,
        parallel_safe: true,
        parallel_aware,
        ..Default::default()
    };
    Node::mk(mcx, SeqScan { scan: Scan { plan, scanrelid: 1 }, cb_scan_cols: None }).unwrap()
}

fn seqscan_tables<'m>(
    mcx: ::mcx::Mcx<'m>,
    relid: u32,
    pstmt: &mut ::types_nodes::node_tree::NodeMut<'m, PlannedStmt<'m>>,
) {
    use ::types_nodes::bitmapset::Bitmapset;
    use ::types_nodes::parsenodes::{RTEKind, RTEPermissionInfo, RangeTblEntry};
    let rte = Node::mk(
        mcx,
        RangeTblEntry {
            rtekind: RTEKind::RTE_RELATION,
            relid,
            relkind: ::types_rel::RELKIND_RELATION,
            rellockmode: ::types_rel::AccessShareLock,
            perminfoindex: 1,
            inFromCl: true,
            ..Default::default()
        },
    )
    .unwrap();
    let perminfo = Node::mk(
        mcx,
        RTEPermissionInfo {
            relid,
            requiredPerms: 1 << 1, // ACL_SELECT
            ..Default::default()
        },
    )
    .unwrap();
    let mut unpruned = Bitmapset::empty();
    unpruned.add_member(mcx, 1).unwrap();
    pstmt.rtable = NodeList::make1(mcx, rte).unwrap();
    pstmt.permInfos = NodeList::make1(mcx, perminfo).unwrap();
    pstmt.unprunableRelids = unpruned;
}

fn run_pstmt(pstmt: &'static PlannedStmt<'static>, tag: &'static str, parallel: bool) -> (u64, Vec<i32>) {
    begin_xact();
    let qd = execmain_seams::create_query_desc::call(
        pstmt,
        tag,
        Some(snapmgr::GetActiveSnapshot()),
        None,
        CommandDest::None,
        ParamListHandle::NULL,
        QueryEnvHandle::NULL,
        0,
    )
    .unwrap();
    execmain_seams::executor_start::call(qd, 0).unwrap();
    let poller = parallel.then(spawn_postmaster_standin);
    let (processed, values) = run_once(qd).unwrap();
    let joins = poller.map(|p| p.join().unwrap()).unwrap_or_default();
    execmain_seams::executor_finish::call(qd).unwrap();
    execmain_seams::executor_end::call(qd).unwrap();
    execmain_seams::free_query_desc::call(qd);
    end_xact();
    for j in joins {
        assert_eq!(j.join().unwrap(), 0);
    }
    (processed, values)
}

// Gather over a parallel-aware SeqScan: the shared ParallelBlockTableScanDesc
// hands each multi-block chunk to exactly one participant (2 workers + the
// leader), so the union equals the serial scan with no duplicates — the
// opposite contract of the non-parallel-aware Result child above.
#[test]
fn gather_parallel_seqscan_matches_serial_scan() {
    let _s = serial();
    let _w = Watchdog::arm(240, "gather_parallel_seqscan_matches_serial_scan");
    setup();
    heapfix::install();
    const RELID: u32 = 91001;
    heapfix::register_table(
        RELID,
        &[&[1, 2, 3, 4, 5], &[6, 7, 8, 9, 10], &[11, 12, 13, 14, 15]],
    );

    let mcx = leaked_mcx();
    let mut serial_pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
    serial_pstmt.commandType = CmdType::CMD_SELECT;
    serial_pstmt.canSetTag = true;
    serial_pstmt.planTree = Some(seqscan_node(mcx, false, 0));
    seqscan_tables(mcx, RELID, &mut serial_pstmt);
    let serial_pstmt = serial_pstmt.seal_ref();
    let (serial_processed, mut serial_values) =
        run_pstmt(serial_pstmt, "select a (serial seqscan)", false);
    assert_eq!(serial_processed, 15);
    serial_values.sort_unstable();
    assert_eq!(serial_values, (1..=15).collect::<Vec<i32>>());

    let mcx = leaked_mcx();
    let mut gather = Node::build::<Gather>(mcx).unwrap();
    gather.plan.targetlist = outer_var_tlist(mcx);
    gather.plan.lefttree = Some(seqscan_node(mcx, true, 1));
    gather.plan.plan_node_id = 0;
    gather.num_workers = 2;
    gather.single_copy = false;
    let plan_node = gather.seal();
    let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
    pstmt.commandType = CmdType::CMD_SELECT;
    pstmt.canSetTag = true;
    pstmt.parallelModeNeeded = true;
    pstmt.planTree = Some(plan_node);
    seqscan_tables(mcx, RELID, &mut pstmt);
    let pstmt = pstmt.seal_ref();

    let (processed, mut values) = run_pstmt(pstmt, "select a (parallel seqscan)", true);
    assert_eq!(processed, 15, "each block scanned by exactly one participant");
    values.sort_unstable();
    assert_eq!(values, serial_values);
    assert!(!parallel::ParallelContextActive());
}

// GatherMerge over ValuesScan([1,3,5]): each participant contributes the same
// sorted stream; the binary-heap merge must interleave them into a globally
// sorted stream (C gather_merge_getnext).
#[test]
fn gather_merge_merges_sorted_streams() {
    let _s = serial();
    let _w = Watchdog::arm(240, "gather_merge_merges_sorted_streams");
    setup();
    begin_xact();

    let mcx = leaked_mcx();
    // VALUES (1),(3),(5) rte + scan.
    let mut rte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
    rte.rtekind = types_nodes::parsenodes::RTEKind::RTE_VALUES;
    let rte_node = rte.seal();
    let mut values_lists = NodeList::nil();
    for v in [1, 3, 5] {
        let row = Node::mk_list(mcx, NodeList::make1(mcx, mk_int4_const(mcx, v)).unwrap()).unwrap();
        values_lists.lappend(mcx, row).unwrap();
    }
    let mut vs = Node::build::<ValuesScan>(mcx).unwrap();
    vs.scan.scanrelid = 1;
    vs.scan.plan.plan_node_id = 1;
    vs.scan.plan.parallel_safe = true;
    vs.values_lists = values_lists;
    let var1 = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    vs.scan.plan.targetlist =
        NodeList::make1(mcx, Node::mk_target_entry(mcx, var1, 1, Some("x"), false).unwrap())
            .unwrap();
    let child = vs.seal();

    let mut gm = Node::build::<GatherMerge>(mcx).unwrap();
    gm.plan.targetlist = outer_var_tlist(mcx);
    gm.plan.lefttree = Some(child);
    gm.plan.plan_node_id = 0;
    gm.num_workers = 2;
    gm.numCols = 1;
    gm.sortColIdx = ::mcx::slice_borrow_in(mcx, &[1i16]).unwrap();
    gm.sortOperators = ::mcx::slice_borrow_in(mcx, &[INT4_LT]).unwrap();
    gm.collations = ::mcx::slice_borrow_in(mcx, &[0u32]).unwrap();
    gm.nullsFirst = ::mcx::slice_borrow_in(mcx, &[false]).unwrap();
    let plan_node = gm.seal();
    let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
    pstmt.commandType = CmdType::CMD_SELECT;
    pstmt.canSetTag = true;
    pstmt.parallelModeNeeded = true;
    pstmt.planTree = Some(plan_node);
    pstmt.rtable = NodeList::make1(mcx, rte_node).unwrap();
    let pstmt = pstmt.seal_ref();

    let qd = execmain_seams::create_query_desc::call(
        pstmt,
        "select x (gather merge)",
        Some(snapmgr::GetActiveSnapshot()),
        None,
        CommandDest::None,
        ParamListHandle::NULL,
        QueryEnvHandle::NULL,
        0,
    )
    .unwrap();
    execmain_seams::executor_start::call(qd, 0).unwrap();
    let poller = spawn_postmaster_standin();
    let (processed, values) = run_once(qd).unwrap();
    let joins = poller.join().unwrap();

    // 3 participants x [1,3,5], merge-sorted.
    assert_eq!(processed, 9);
    assert_eq!(values, vec![1, 1, 1, 3, 3, 3, 5, 5, 5]);

    execmain_seams::executor_finish::call(qd).unwrap();
    execmain_seams::executor_end::call(qd).unwrap();
    execmain_seams::free_query_desc::call(qd);
    end_xact();
    for j in joins {
        assert_eq!(j.join().unwrap(), 0);
    }
}

// ---------------------------------------------------------------------------
// World-B passthrough row-emit funnel — Stage-4 SQL smoke (gather-elimination
// Phase 2). ARMED only when the kill switch is set at process start:
//
//   PGRUST_RUNTIME_ROW_FUNNEL=1 cargo test -p execmain --test gather_e2e funnel_
//
// Unarmed (the default `cargo test` world) every test here SKIPS, so the
// default binary's behavior — and the pre-existing gather tests — stay
// byte-identical. The kill switch is a process-static OnceLock, so arming
// must happen at process start (the env prefix), never mid-process.
// ---------------------------------------------------------------------------

fn funnel_armed() -> bool {
    matches!(
        std::env::var("PGRUST_RUNTIME_ROW_FUNNEL").as_deref(),
        Ok("1") | Ok("on")
    )
}

/// Install the process-global runtime (the pool the hook's `runtime::global()`
/// gate requires). No pool threads are spawned: the passthrough RG is PINNED
/// (invisible to pool workers); the bgworkers drive it themselves through
/// external lanes, exactly as in production.
fn funnel_runtime_boot() {
    static BOOT: Once = Once::new();
    BOOT.call_once(|| {
        // 8 execution permits (not 2): external participants (bgworkers + the
        // leader-producer caller) each hold a permit across a step, and a
        // producer parked on a full ring HOLDS its permit (blocking_io_section
        // donates only on registered pool threads) — at permits <= gang size
        // the caller-leader starves behind parked producers (measured: the
        // leader-mode smoke ran 8.4s at 2 permits, ms at 8). Production pools
        // have `cores` permits, so the squeeze needs DOP ~ cores there; the
        // GL-FUNNEL-2 letter records it as a leader-mode admission bound.
        let rt = ::runtime::Runtime::new(::runtime::RuntimeConfig::new(8));
        ::runtime::install_global(rt);
    });
}

const BOOLOID: u32 = 16;

/// `Var(a) > Const(k)` int4 qual (opno 521 / int4gt proc 147).
fn funnel_qual_gt(mcx: ::mcx::Mcx<'_>, k: i32) -> NodeList<'_> {
    let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let op = Node::mk(
        mcx,
        ::types_nodes::primnodes::OpExpr {
            opno: 521,
            opfuncid: 147, // pg_proc int4gt
            opresulttype: BOOLOID,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args: NodeList::make2(mcx, var, mk_int4_const(mcx, k)).unwrap(),
            location: -1,
        },
    )
    .unwrap();
    NodeList::make1(mcx, op).unwrap()
}

/// `(10 / (a - k)) >= 0` — errors with division-by-zero exactly at a == k
/// (the worker-error-mid-scan injection).
fn funnel_qual_div_err(mcx: ::mcx::Mcx<'_>, k: i32) -> NodeList<'_> {
    let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let mi = Node::mk(
        mcx,
        ::types_nodes::primnodes::OpExpr {
            opno: 555,
            opfuncid: 181, // int4mi
            opresulttype: INT4OID,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args: NodeList::make2(mcx, var, mk_int4_const(mcx, k)).unwrap(),
            location: -1,
        },
    )
    .unwrap();
    let div = Node::mk(
        mcx,
        ::types_nodes::primnodes::OpExpr {
            opno: 528,
            opfuncid: 154, // int4div — raises on zero divisor
            opresulttype: INT4OID,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args: NodeList::make2(mcx, mk_int4_const(mcx, 10), mi).unwrap(),
            location: -1,
        },
    )
    .unwrap();
    let ge = Node::mk(
        mcx,
        ::types_nodes::primnodes::OpExpr {
            opno: 525,
            opfuncid: 150, // int4ge
            opresulttype: BOOLOID,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args: NodeList::make2(mcx, div, mk_int4_const(mcx, 0)).unwrap(),
            location: -1,
        },
    )
    .unwrap();
    NodeList::make1(mcx, ge).unwrap()
}

/// Bare (non-parallel-aware) SeqScan pstmt: `SELECT a FROM rel [WHERE qual]`.
/// parallel_safe so the funnel gate admits it; NOT parallel_aware — each
/// funnel worker positions its own scan over claimed morsel block ranges
/// (seq_scan_set_morsel_range), no shared parallel scan descriptor.
/// `plan_rows` is the planner's post-qual output estimate (what a real plan
/// carries after ANALYZE): the emit-fraction FloorGuard consults
/// plan_rows/reltuples, so armed engagement witnesses must pass an in-band
/// estimate and fail-closed witnesses an out-of-band (or zero) one.
fn funnel_seqscan_pstmt<'m>(
    mcx: ::mcx::Mcx<'m>,
    relid: u32,
    qual: Option<NodeList<'m>>,
    plan_rows: f64,
) -> &'m PlannedStmt<'m> {
    funnel_seqscan_pstmt_frag(mcx, relid, qual, plan_rows, false)
}

/// `funnel_seqscan_pstmt` with an explicit `parallel_aware` marker: `true`
/// builds a parallel-FRAGMENT shape (what a parallel worker's deserialized
/// plan carries — its `plan_rows` is the planner's PER-PARTICIPANT estimate,
/// divided by the parallel divisor at costing). The emit-band FloorGuard must
/// refuse fragment estimates categorically; see `floorguard_emit_band_admits`.
fn funnel_seqscan_pstmt_frag<'m>(
    mcx: ::mcx::Mcx<'m>,
    relid: u32,
    qual: Option<NodeList<'m>>,
    plan_rows: f64,
    parallel_aware: bool,
) -> &'m PlannedStmt<'m> {
    use ::types_nodes::plannodes::{Plan, Scan, SeqScan};
    let var = Node::mk_var(mcx, 1, 1, INT4OID, -1, 0, 0).unwrap();
    let tle = Node::mk_target_entry(mcx, var, 1, Some("a"), false).unwrap();
    let mut plan = Plan {
        targetlist: NodeList::make1(mcx, tle).unwrap(),
        plan_node_id: 0,
        parallel_safe: true,
        parallel_aware,
        plan_rows,
        ..Default::default()
    };
    if let Some(q) = qual {
        plan.qual = q;
    }
    let scan =
        Node::mk(mcx, SeqScan { scan: Scan { plan, scanrelid: 1 }, cb_scan_cols: None }).unwrap();
    let mut pstmt = Node::build::<PlannedStmt>(mcx).unwrap();
    pstmt.commandType = CmdType::CMD_SELECT;
    pstmt.canSetTag = true;
    pstmt.planTree = Some(scan);
    seqscan_tables(mcx, relid, &mut pstmt);
    pstmt.seal_ref()
}

/// `run_once` with an explicit executor count (0 = complete drain; N = the
/// count-limited/suspendable cadence the funnel must REFUSE).
fn funnel_run_once(
    qd: types_portal::QueryDescHandle,
    count: u64,
) -> PgResult<(u64, Vec<i32>)> {
    let store = tuplestore::Tuplestore::begin_heap(false, false, 1024);
    let h = tuplestore::hold::register(store);
    let mut dest = DestReceiver::Tuplestore(tstore_receiver::tstore_create_DR());
    tcop_dest::SetTuplestoreDestReceiverParams(&mut dest, h, false);
    execmain_seams::executor_run::call(qd, ForwardScanDirection, count, &mut dest)?;
    let processed = execmain_seams::query_desc_es_processed::call(qd);
    let mcx = leaked_mcx();
    let desc = execmain_seams::query_desc_result_tupdesc::call(qd).unwrap();
    let mut slot = exectuples::make_tuple_table_slot(
        mcx,
        TupleSlotKind::MinimalTuple,
        Some(std::rc::Rc::clone(&desc)),
    );
    let mut values = Vec::new();
    let mut store = tuplestore::hold::take(h).unwrap();
    loop {
        let got = store.gettupleslot(true, false, &mut slot, mcx)?;
        if !got {
            break;
        }
        let mut isnull = false;
        let d = exectuples::slot_getattr(&mut slot, 1, &mut isnull);
        assert!(!isnull);
        values.push(d.as_i32());
    }
    store.end();
    Ok((processed, values))
}

/// Postmaster stand-in with a stop flag: exits when it launched a worker
/// batch OR when the funnel run already completed (leader-producer mode can
/// finish the whole scan before the 10ms poll ever launches the gang — the
/// stock 600x10ms poller would then spin its full timeout for nothing).
fn spawn_postmaster_standin_stoppable(
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> std::thread::JoinHandle<Vec<std::thread::JoinHandle<i32>>> {
    std::thread::spawn(move || {
        thread_globals();
        let mut joins = Vec::new();
        for _ in 0..600 {
            std::thread::sleep(std::time::Duration::from_millis(10));
            let mut new = launch_registered_workers();
            if !new.is_empty() {
                joins.append(&mut new);
                break;
            }
            if stop.load(Relaxed) {
                break;
            }
        }
        joins
    })
}

/// Full leader-side run of a funnel-candidate pstmt. `poller` spawns the
/// postmaster stand-in (needed whenever the funnel may LaunchParallelWorkers).
/// Errors tear down via release + transaction abort (the session error path).
fn funnel_run_pstmt(
    pstmt: &'static PlannedStmt<'static>,
    tag: &'static str,
    count: u64,
    poller: bool,
) -> PgResult<(u64, Vec<i32>)> {
    begin_xact();
    let qd = execmain_seams::create_query_desc::call(
        pstmt,
        tag,
        Some(snapmgr::GetActiveSnapshot()),
        None,
        CommandDest::None,
        ParamListHandle::NULL,
        QueryEnvHandle::NULL,
        0,
    )
    .unwrap();
    execmain_seams::executor_start::call(qd, 0).unwrap();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let poller = poller.then(|| spawn_postmaster_standin_stoppable(std::sync::Arc::clone(&stop)));
    let r = funnel_run_once(qd, count);
    stop.store(true, Relaxed);
    let joins = poller.map(|p| p.join().unwrap()).unwrap_or_default();
    match r {
        Ok((processed, values)) => {
            execmain_seams::executor_finish::call(qd).unwrap();
            execmain_seams::executor_end::call(qd).unwrap();
            execmain_seams::free_query_desc::call(qd);
            end_xact();
            for j in joins {
                assert_eq!(j.join().unwrap(), 0);
            }
            Ok((processed, values))
        }
        Err(e) => {
            // Session error path: release the executor mid-run and abort the
            // transaction; workers exited through their own error paths (their
            // exit codes are not asserted — the leader error is the contract).
            execmain_seams::release_query_desc::call(qd);
            xact::AbortCurrentTransaction().unwrap();
            for j in joins {
                let _ = j.join();
            }
            Err(e)
        }
    }
}

// Byte-identity smoke: a multi-page passthrough SELECT a WHERE a > 300 runs
// once through the SERIAL loop (count-limited run — which also proves the
// portal-suspend refusal gate) and once through the FUNNEL (complete drain),
// and must produce the identical row multiset. Engagement is asserted
// POSITIVELY through the funnel counters — identical rows alone cannot tell
// the funnel from the serial loop.
#[test]
fn funnel_smoke_byte_identical_on_vs_off() {
    if !funnel_armed() {
        eprintln!("SKIP: funnel_smoke_byte_identical_on_vs_off (PGRUST_RUNTIME_ROW_FUNNEL unset)");
        return;
    }
    let _s = serial();
    let _w = Watchdog::arm(240, "funnel_smoke_byte_identical_on_vs_off");
    setup();
    heapfix::install();
    funnel_runtime_boot();

    // 600 pages x 100 rows = 60000 rows (values 1..=60000): enough granules
    // for the gang floor and — with the a>55000 qual — 5000 emitted rows,
    // enough to fill 1024-slot rings (real back-pressure + the mid-drive
    // leader wake). The qual keeps the shape INSIDE the GL-FUNNEL-4
    // FloorGuard emit band: plan_rows/reltuples = 5000/60000 = 8.3% <= 10%
    // (the fixture is registered ANALYZED, so reltuples is real).
    const RELID: u32 = 93001;
    let pages: Vec<Vec<i32>> = (0..600)
        .map(|p| ((p * 100 + 1)..=(p * 100 + 100)).collect())
        .collect();
    let page_refs: Vec<&[i32]> = pages.iter().map(|v| &v[..]).collect();
    heapfix::register_table(RELID, &page_refs);
    let expected: Vec<i32> = (55_001..=60_000).collect();

    // OFF-equivalent baseline: a count-limited run (count >> rows). The
    // count-limited gate refuses the funnel (the portal-suspend duplication
    // fix), so this is the SERIAL loop — engagement counters must not move.
    let (e0, c0) = execmain::funnel_engagements();
    let mcx = leaked_mcx();
    let pstmt_off =
        funnel_seqscan_pstmt(mcx, RELID, Some(funnel_qual_gt(mcx, 55_000)), 5000.0);
    let t0 = std::time::Instant::now();
    let (processed_off, mut values_off) =
        funnel_run_pstmt(pstmt_off, "select a where a>55000 (serial baseline)", 100_000, false)
            .unwrap();
    let serial_ms = t0.elapsed().as_millis();
    let (e1, c1) = execmain::funnel_engagements();
    assert_eq!((e1, c1), (e0, c0), "count-limited run must NOT engage the funnel");
    assert_eq!(processed_off, expected.len() as u64);
    values_off.sort_unstable();
    assert_eq!(values_off, expected);

    // ON: complete-drain run — the funnel engages, bgworkers produce, the
    // leader drains concurrently to the tuplestore dest.
    let mcx = leaked_mcx();
    let pstmt_on =
        funnel_seqscan_pstmt(mcx, RELID, Some(funnel_qual_gt(mcx, 55_000)), 5000.0);
    let t1 = std::time::Instant::now();
    let (processed_on, mut values_on) =
        funnel_run_pstmt(pstmt_on, "select a where a>55000 (funnel)", 0, true).unwrap();
    let funnel_ms = t1.elapsed().as_millis();
    let (e2, c2) = execmain::funnel_engagements();
    assert_eq!(e2, e1 + 1, "complete-drain run must engage the funnel");
    assert_eq!(c2, c1 + 1, "the funnel must complete the run (not fall back)");
    assert_eq!(processed_on, expected.len() as u64);
    values_on.sort_unstable();
    assert_eq!(values_on, expected, "funnel rows must equal the serial rows");
    eprintln!(
        "funnel smoke: rows={} serial_ms={serial_ms} funnel_ms={funnel_ms}",
        expected.len()
    );
    assert!(!parallel::ParallelContextActive());
}

// LIMIT / portal-suspend variant: a small count-limited run (the
// Execute(portal, max_rows) shape) must stay on the SERIAL path — the
// count-limited refusal gate — return exactly `count` rows in serial scan
// order, and not hang.
#[test]
fn funnel_limit_refusal_stays_serial_no_hang() {
    if !funnel_armed() {
        eprintln!(
            "SKIP: funnel_limit_refusal_stays_serial_no_hang (PGRUST_RUNTIME_ROW_FUNNEL unset)"
        );
        return;
    }
    let _s = serial();
    let _w = Watchdog::arm(120, "funnel_limit_refusal_stays_serial_no_hang");
    setup();
    heapfix::install();
    funnel_runtime_boot();

    const RELID: u32 = 93002;
    let pages: Vec<Vec<i32>> =
        (0..8).map(|p| ((p * 5 + 1)..=(p * 5 + 5)).collect()).collect();
    let page_refs: Vec<&[i32]> = pages.iter().map(|v| &v[..]).collect();
    heapfix::register_table(RELID, &page_refs);

    let (e0, _) = execmain::funnel_engagements();
    let mcx = leaked_mcx();
    let pstmt = funnel_seqscan_pstmt(mcx, RELID, None, 40.0);
    let (processed, values) =
        funnel_run_pstmt(pstmt, "select a limit-cadence (serial)", 5, false).unwrap();
    let (e1, _) = execmain::funnel_engagements();
    assert_eq!(e1, e0, "count-limited run must NOT engage the funnel");
    assert_eq!(processed, 5);
    // Serial scan order is deterministic: the first 5 heap rows.
    assert_eq!(values, vec![1, 2, 3, 4, 5]);
}

// Worker-error-mid-scan: a qual that raises division-by-zero at one mid-table
// row must surface as a query ERROR from the funnel run — never partial rows
// as success — and tear down without a hang.
#[test]
fn funnel_worker_error_mid_scan_surfaces() {
    if !funnel_armed() {
        eprintln!(
            "SKIP: funnel_worker_error_mid_scan_surfaces (PGRUST_RUNTIME_ROW_FUNNEL unset)"
        );
        return;
    }
    let _s = serial();
    let _w = Watchdog::arm(120, "funnel_worker_error_mid_scan_surfaces");
    setup();
    heapfix::install();
    funnel_runtime_boot();

    const RELID: u32 = 93003;
    let pages: Vec<Vec<i32>> =
        (0..12).map(|p| ((p * 10 + 1)..=(p * 10 + 10)).collect()).collect();
    let page_refs: Vec<&[i32]> = pages.iter().map(|v| &v[..]).collect();
    heapfix::register_table(RELID, &page_refs);

    let (e0, c0) = execmain::funnel_engagements();
    let mcx = leaked_mcx();
    // 10 / (a - 60) errors at a == 60 (page 6 of 12). plan_rows 10 of 120
    // analyzed reltuples = 8.3%, inside the FloorGuard band, so the ONLY
    // exit is the mid-scan error.
    let pstmt = funnel_seqscan_pstmt(mcx, RELID, Some(funnel_qual_div_err(mcx, 60)), 10.0);
    let r = funnel_run_pstmt(pstmt, "select a div-err (funnel)", 0, true);
    let (e1, c1) = execmain::funnel_engagements();
    assert_eq!(e1, e0 + 1, "the error run must have engaged the funnel");
    assert_eq!(c1, c0, "an errored run must NOT count as completed");
    let err = r.expect_err("division by zero must surface as a query error");
    eprintln!("funnel worker-error surfaced: {err}");
}

// NEGATIVE WITNESS (the emit-fraction FloorGuard's fail-closed contract): a
// shape that passes every other gate — qualed, in-band plan_rows, complete
// drain, enough granules — but scans a GENUINELY stats-less table
// (never-analyzed reltuples = -1) must REFUSE to the serial loop: engagement
// counters do not move and the rows are still byte-correct.
#[test]
fn funnel_statsless_table_fail_closes_to_serial() {
    if !funnel_armed() {
        eprintln!(
            "SKIP: funnel_statsless_table_fail_closes_to_serial (PGRUST_RUNTIME_ROW_FUNNEL unset)"
        );
        return;
    }
    let _s = serial();
    let _w = Watchdog::arm(120, "funnel_statsless_table_fail_closes_to_serial");
    setup();
    heapfix::install();
    funnel_runtime_boot();

    // 12 pages x 100 rows, registered UNANALYZED (reltuples = -1).
    const RELID: u32 = 93004;
    let pages: Vec<Vec<i32>> =
        (0..12).map(|p| ((p * 100 + 1)..=(p * 100 + 100)).collect()).collect();
    let page_refs: Vec<&[i32]> = pages.iter().map(|v| &v[..]).collect();
    heapfix::register_table_unanalyzed(RELID, &page_refs);
    let expected: Vec<i32> = (1101..=1200).collect();

    let (e0, c0) = execmain::funnel_engagements();
    let mcx = leaked_mcx();
    // In-band estimate (100/1200 = 8.3% WOULD pass if the table had stats):
    // the refusal below is attributable to missing stats alone.
    let pstmt = funnel_seqscan_pstmt(mcx, RELID, Some(funnel_qual_gt(mcx, 1100)), 100.0);
    let (processed, mut values) =
        funnel_run_pstmt(pstmt, "select a where a>1100 (stats-less)", 0, false).unwrap();
    let (e1, c1) = execmain::funnel_engagements();
    assert_eq!(
        (e1, c1),
        (e0, c0),
        "a stats-less table must fail-close to the serial loop (no engagement)"
    );
    assert_eq!(processed, expected.len() as u64);
    values.sort_unstable();
    assert_eq!(values, expected, "serial fallback rows must be byte-correct");
}

// In-parallel-mode refusal: a shape that passes every other gate must REFUSE
// when the session is already inside parallel machinery. This is the
// worker-side duplication regression: a legacy Gather WORKER re-enters
// execute_plan with a serial-shaped fragment (the serialized plan clears
// parallelModeNeeded), and an engaged funnel there would full-scan the
// relation through a private granule map — every participant emits the
// complete result, (workers+1)x rows at the destination — and nest a second
// parallel context whose lock-group join corrupts the in-flight membership.
// Workers always run inside parallel mode (StartParallelWorkerTransaction),
// so EnterParallelMode reproduces the worker-side gate condition exactly.
#[test]
fn funnel_refuses_inside_parallel_mode() {
    if !funnel_armed() {
        eprintln!("SKIP: funnel_refuses_inside_parallel_mode (PGRUST_RUNTIME_ROW_FUNNEL unset)");
        return;
    }
    let _s = serial();
    let _w = Watchdog::arm(120, "funnel_refuses_inside_parallel_mode");
    setup();
    heapfix::install();
    funnel_runtime_boot();

    // 60 pages x 100 rows, ANALYZED; qual a > 5700 emits 300/6000 = 5% —
    // inside the emit band, above the granule floor: every gate but the
    // parallel-mode one admits, so the refusal below is attributable to
    // parallel mode alone.
    const RELID: u32 = 93005;
    let pages: Vec<Vec<i32>> =
        (0..60).map(|p| ((p * 100 + 1)..=(p * 100 + 100)).collect()).collect();
    let page_refs: Vec<&[i32]> = pages.iter().map(|v| &v[..]).collect();
    heapfix::register_table(RELID, &page_refs);
    let expected: Vec<i32> = (5701..=6000).collect();

    // Leg 1 — inside parallel mode: complete-drain shape, counters must not
    // move, rows served byte-correct by the serial loop.
    let (e0, c0) = execmain::funnel_engagements();
    let mcx = leaked_mcx();
    let pstmt = funnel_seqscan_pstmt(mcx, RELID, Some(funnel_qual_gt(mcx, 5700)), 300.0);
    begin_xact();
    xact::EnterParallelMode();
    let qd = execmain_seams::create_query_desc::call(
        pstmt,
        "select a where a>5700 (in parallel mode)",
        Some(snapmgr::GetActiveSnapshot()),
        None,
        CommandDest::None,
        ParamListHandle::NULL,
        QueryEnvHandle::NULL,
        0,
    )
    .unwrap();
    execmain_seams::executor_start::call(qd, 0).unwrap();
    let (processed, mut values) = funnel_run_once(qd, 0).unwrap();
    execmain_seams::executor_finish::call(qd).unwrap();
    execmain_seams::executor_end::call(qd).unwrap();
    execmain_seams::free_query_desc::call(qd);
    xact::ExitParallelMode();
    end_xact();
    let (e1, c1) = execmain::funnel_engagements();
    assert_eq!(
        (e1, c1),
        (e0, c0),
        "a run inside parallel mode must NOT engage the funnel"
    );
    assert_eq!(processed, expected.len() as u64);
    values.sort_unstable();
    assert_eq!(values, expected, "serial fallback rows must be byte-correct");

    // Leg 2 — the identical shape OUTSIDE parallel mode engages (the refusal
    // above is not vacuous).
    let mcx = leaked_mcx();
    let pstmt_ctl = funnel_seqscan_pstmt(mcx, RELID, Some(funnel_qual_gt(mcx, 5700)), 300.0);
    let (processed_ctl, mut values_ctl) =
        funnel_run_pstmt(pstmt_ctl, "select a where a>5700 (control)", 0, true).unwrap();
    let (e2, c2) = execmain::funnel_engagements();
    assert_eq!(e2, e1 + 1, "the control leg must engage the funnel");
    assert_eq!(c2, c1 + 1, "the control leg must complete through the funnel");
    assert_eq!(processed_ctl, expected.len() as u64);
    values_ctl.sort_unstable();
    assert_eq!(values_ctl, expected);
}

// FLOORGUARD gate 3 scale-consistency witnesses (emit-band divisor fix):
// admission must track the TRUE emit fraction at every planned DOP. A
// parallel FRAGMENT's plan_rows is the planner's PER-PARTICIPANT estimate —
// divided at costing by the parallel divisor, w + max(0, 1 - 0.3*w) for w
// planned workers — so the pre-fix expression `plan_rows / reltuples <= band`
// read a 33%-emit qual as 8.25% at 4 planned workers (divisor 4.0) and
// ADMITTED it (the mis-admission enabler of the worker-side duplication bug);
// at 2 workers (divisor 2.4) the same qual read 13.75% and refused — the
// exact clean-vs-dirty DOP boundary observed. Post-fix the band refuses
// fragment estimates categorically (the divisor is not recoverable from the
// Plan node, so no exact true fraction exists to admit on) and prices
// whole-plan estimates unchanged. Every cell also asserts byte-correct rows
// through whichever engine served it.
#[test]
fn funnel_band_boundary_tracks_true_fraction_across_dop() {
    if !funnel_armed() {
        eprintln!(
            "SKIP: funnel_band_boundary_tracks_true_fraction_across_dop (PGRUST_RUNTIME_ROW_FUNNEL unset)"
        );
        return;
    }
    let _s = serial();
    let _w = Watchdog::arm(240, "funnel_band_boundary_tracks_true_fraction_across_dop");
    setup();
    heapfix::install();
    funnel_runtime_boot();

    // 600 pages x 100 rows = 60000 rows, ANALYZED (the smoke test's scan
    // scale — POD-CLASS SIZING, not taste: an engaged run must outlive the
    // postmaster stand-in's first 10ms launch tick. A shorter engaged run
    // lands in the pod doom band — longer than the tick, shorter than the
    // tick plus worker-thread startup — so the gang attaches only AFTER the
    // leader-producer finished and destroyed the parallel context, fails to
    // map the segment, and exits nonzero (benign late-worker in C; an
    // exit-code assert in this harness). Locally such a run ends before the
    // tick and nothing ever launches, which is why a 60-page variant passed
    // every local run and failed every fleet pod.
    const RELID: u32 = 93006;
    let pages: Vec<Vec<i32>> =
        (0..600).map(|p| ((p * 100 + 1)..=(p * 100 + 100)).collect()).collect();
    let page_refs: Vec<&[i32]> = pages.iter().map(|v| &v[..]).collect();
    heapfix::register_table(RELID, &page_refs);

    // (tag, qual threshold k — `a > k` truly emits 60000-k rows, plan_rows,
    //  parallel_aware, expect_engage). Fragment plan_rows = whole / divisor.
    let cells: &[(&'static str, i32, f64, bool, bool)] = &[
        // Planned DOP 1 (serial whole-plan shapes): admission tracks the
        // true fraction across the 10% boundary — 9.9% engages, 10.3%
        // refuses.
        ("dop1 true 9.9% whole-plan (engage)", 54060, 5940.0, false, true),
        ("dop1 true 10.3% whole-plan (refuse)", 53820, 6180.0, false, false),
        // Planned DOP 2 fragment, true 33% (divisor 2.4 -> apparent 13.75%):
        // refuse — pre-fix also refused; the clean side of the measured
        // boundary.
        ("dop2 fragment true 33% (refuse)", 40200, 8250.0, true, false),
        // Planned DOP 4 fragment, true 33% (divisor 4.0 -> apparent 8.25%,
        // INSIDE the band): RED WITNESS — the pre-fix expression admitted
        // this cell.
        ("dop4 fragment true 33% (refuse; red witness)", 40200, 4950.0, true, false),
        // Planned DOP 4 fragment, true 9.9% (apparent 2.475%): refuses too —
        // fragments fail closed even when the true fraction is in-band,
        // because the divisor is not recoverable from the Plan node.
        ("dop4 fragment true 9.9% (refuse; fail-closed)", 54060, 1485.0, true, false),
        // Positive control for the red-witness cell: the SAME 8.25% estimate
        // on a whole-plan shape engages — the fragment refusals above are
        // attributable to the per-participant marker alone, not any other
        // gate.
        ("whole-plan 8.25% estimate control (engage)", 40200, 4950.0, false, true),
    ];

    for &(tag, k, plan_rows, parallel_aware, expect_engage) in cells {
        let (e0, c0) = execmain::funnel_engagements();
        let mcx = leaked_mcx();
        let pstmt = funnel_seqscan_pstmt_frag(
            mcx,
            RELID,
            Some(funnel_qual_gt(mcx, k)),
            plan_rows,
            parallel_aware,
        );
        let expected: Vec<i32> = ((k + 1)..=60000).collect();
        // Poller only where a gang is expected (pod-class hardening, the
        // sibling tests' shape): a refuse-expected cell registers nothing,
        // so its poller could only ever launch strays racing a previous
        // cell's context teardown. A refuse cell that regresses into
        // engaging hangs awaiting a gang no stand-in launches and surfaces
        // as this test's named watchdog abort — still red, just not crisp.
        let (processed, mut values) = funnel_run_pstmt(pstmt, tag, 0, expect_engage).unwrap();
        let (e1, c1) = execmain::funnel_engagements();
        if expect_engage {
            assert_eq!(e1, e0 + 1, "{tag}: must engage the funnel");
            assert_eq!(c1, c0 + 1, "{tag}: must complete through the funnel");
        } else {
            assert_eq!((e1, c1), (e0, c0), "{tag}: must NOT engage the funnel");
        }
        assert_eq!(processed, expected.len() as u64, "{tag}: row count");
        values.sort_unstable();
        assert_eq!(values, expected, "{tag}: rows must be byte-correct");
    }
}
