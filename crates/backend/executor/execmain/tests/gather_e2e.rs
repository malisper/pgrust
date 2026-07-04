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
    aio_seams::pgaio_io_start_readv::set(|_, _, _| {});
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
        thread_globals();
        g::SetMyProcPid(NEXT_PID.fetch_add(1, Relaxed));
        thread_guc_boot();
        fd::InitFileAccess();
        waiteventset::InitializeWaitEventSupport().unwrap();
        miscinit::InitProcessLocalLatch();
        lmgr_proc::InitProcess(types_core::BackendType::Backend).unwrap();
        procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).unwrap();
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
        let mut joins = Vec::new();
        for _ in 0..600 {
            std::thread::sleep(std::time::Duration::from_millis(10));
            g::SetIsUnderPostmaster(true);
            let mut new = launch_registered_workers();
            g::SetIsUnderPostmaster(false);
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
