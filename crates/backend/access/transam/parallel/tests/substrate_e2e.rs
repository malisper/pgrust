// Substrate e2e: real worker threads through bgworker::BackgroundWorkerMain,
// tuples back through shm_mq/tqueue, C-shaped error rethrow. Harness deltas
// from a live server, each the narrowest available: no postmaster thread (the
// test drives the registered-worker launch/reap the way serverloop would),
// database_id InvalidOid (no catalog connect), hand-built MVCC active
// snapshot (Serialize/Restore is unit-proven in snapmgr).
use std::any::Any;
use std::sync::atomic::{AtomicI32, Ordering::Relaxed};
use std::sync::{Arc, Condvar, Mutex, Once};

use init_small::globals as g;
use types_core::{InvalidOid, INVALID_PROC_NUMBER};
use types_error::{PgError, PgResult, ERROR};
use types_startup::StartupData;

const N_TUPLES: usize = 500;

fn serial() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

static NEXT_PID: AtomicI32 = AtomicI32::new(9000);

// The fleet log filters captured test stdout; diagnostics ride the asserts.
static WORKER_LOG: Mutex<Vec<String>> = Mutex::new(Vec::new());
static BINDER_TARGET: Mutex<Option<Arc<parallel::ParallelShared>>> = Mutex::new(None);
static BINDER_REFUSALS: Mutex<Vec<(Arc<parallel::ParallelShared>, &'static str)>> =
    Mutex::new(Vec::new());
static BINDER_REPORT: (Mutex<Option<Result<(), String>>>, Condvar) =
    (Mutex::new(None), Condvar::new());

static TEST_RECORD_REGISTRIES: Mutex<
    Vec<(std::thread::ThreadId, typcache_seams::RecordRegistryHandle)>,
> = Mutex::new(Vec::new());

fn test_record_registry_handle() -> typcache_seams::RecordRegistryHandle {
    let thread = std::thread::current().id();
    let mut registries = TEST_RECORD_REGISTRIES.lock().unwrap_or_else(|error| error.into_inner());
    if let Some((_, registry)) = registries.iter().find(|(id, _)| *id == thread) {
        return Arc::clone(registry);
    }
    let registry = typcache_seams::RecordRegistryHandle::default();
    registries.push((thread, Arc::clone(&registry)));
    registry
}

fn install_test_record_registry(registry: typcache_seams::RecordRegistryHandle) {
    let thread = std::thread::current().id();
    let mut registries = TEST_RECORD_REGISTRIES.lock().unwrap_or_else(|error| error.into_inner());
    if let Some((_, current)) = registries.iter_mut().find(|(id, _)| *id == thread) {
        *current = registry;
    } else {
        registries.push((thread, registry));
    }
}

fn wlog(s: String) {
    WORKER_LOG.lock().unwrap_or_else(|e| e.into_inner()).push(s);
}

fn wlog_dump() -> String {
    WORKER_LOG.lock().unwrap_or_else(|e| e.into_inner()).join(" | ")
}

// A hang here otherwise burns the whole fleet job deadline.
struct Watchdog(Arc<std::sync::atomic::AtomicBool>);
impl Watchdog {
    fn arm(secs: u64, label: &'static str) -> Self {
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&done);
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

// IsUnderPostmaster is thread-local; scope it to registration so the latch
// wait paths never arm a postmaster-death watch (no postmaster pipe here).
fn launch_as_if_under_postmaster(
    pcxt: parallel::ParallelContextId,
) -> PgResult<i32> {
    g::SetIsUnderPostmaster(true);
    let r = parallel::LaunchParallelWorkers(pcxt);
    g::SetIsUnderPostmaster(false);
    r
}

// insert_select.rs's full-transaction rig, with real latch/waiteventset/
// miscinit/parallel/combocid seams where that test stubbed them (workers
// block on real latches here).
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
    postgres_seams::check_for_interrupts::set(|| Ok(()));

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
    typcache_seams::record_registry_handle::set(test_record_registry_handle);
    typcache_seams::install_record_registry::set(install_test_record_registry);
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
    backend_progress_seams::pgstat_progress_end_command::set(|| {});
    predicate_seams::pre_commit_check_for_serialization_failure::set(|| Ok(()));
    predicate_seams::release_predicate_locks::set(|_, _| Ok(()));
    predicate_seams::share_serializable_xact::set(|| 0);
    predicate_seams::attach_serializable_xact::set(|_| Ok(()));
    // Every launched worker thread is "postmaster child"-shaped here.
    pmchild_seams::find_postmaster_child_by_pid::set(|pid| {
        Some((pid, types_core::BackendType::Backend))
    });

    // The planner owns this accessor in production.
    {
        use std::sync::atomic::{AtomicI32, Ordering::Relaxed as R};
        static DPQ: AtomicI32 = AtomicI32::new(0);
        guc_tables::vars::debug_parallel_query.install_if_absent(guc_tables::GucVarAccessors {
            get: || DPQ.load(R),
            set: |v| DPQ.store(v, R),
        });
    }
}

fn setup() {
    static SETUP: Once = Once::new();
    SETUP.call_once(|| {
        let dir = std::env::temp_dir().join(format!("pgrust_par_e2e_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        for sub in ["global", "pg_wal", "pg_xact", "pg_subtrans"] {
            std::fs::create_dir_all(dir.join(sub)).unwrap();
        }
        std::env::set_current_dir(&dir).unwrap();
        let dir_str: &'static str =
            Box::leak(dir.to_str().unwrap().to_string().into_boxed_str());
        DATA_DIR.set(dir_str).unwrap();
        thread_globals();
        g::SetMyProcPid(779);

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
        assert!(transam_xlog::XLogInsertAllowed());

        pmsignal::PMSignalShmemInit(64);
        bgworker::BackgroundWorkerShmemInit();
        procsignal::ProcSignalShmemInit();
        parallel::register_parallel_worker_entrypoint("substrate_e2e_main", e2e_worker_main);
        parallel::register_parallel_worker_entrypoint("substrate_e2e_error", e2e_error_main);
        parallel::register_parallel_worker_entrypoint("substrate_e2e_noop", |_| Ok(()));
        parallel::register_parallel_post_task_park(query_task_binder_hook);
    });
    leader_thread_boot();
}

// Each #[test] runs on its own thread; give it a backend-shaped identity.
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
    cf.system_identifier = 0x5544_3322_1100_AACD;
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

struct E2eShared {
    queues: Vec<Arc<shm_mq::ShmMq>>,
    instr: Mutex<Vec<types_core::instrument::Instrumentation>>,
}

fn tuple_image(worker: usize, i: usize) -> Vec<u8> {
    (0..(16 + (i * 7) % 96))
        .map(|j| (worker * 131 + i * 31 + j) as u8)
        .collect()
}

fn e2e_worker_main(shared: &parallel::ParallelShared) -> PgResult<()> {
    let me = parallel::ParallelWorkerNumber() as usize;
    let private = shared.private().expect("e2e private missing");
    let e2e = private.downcast_ref::<E2eShared>().expect("e2e private type");
    let mq = Arc::clone(&e2e.queues[me]);
    mq.set_sender(g::MyProcNumber());
    let mut tx = shm_mq::shm_mq_attach(mq);
    let mut instr = types_core::instrument::Instrumentation::default();
    instrument::instr_init(&mut instr, 0);
    instrument::instr_start_node(&mut instr);
    let mut sent = 0f64;
    for i in 0..N_TUPLES {
        let img = tuple_image(me, i);
        if !tqueue::tqueue_send_bytes(&mut tx, &img)? {
            break;
        }
        sent += 1.0;
    }
    instrument::instr_stop_node(&mut instr, sent);
    instrument::instr_end_loop(&mut instr);
    e2e.instr.lock().unwrap_or_else(|e| e.into_inner())[me] = instr;
    Ok(())
}

fn e2e_error_main(_shared: &parallel::ParallelShared) -> PgResult<()> {
    wlog(format!("e2e_error_main reached in worker {}", parallel::ParallelWorkerNumber()));
    Err(Box::new(
        PgError::new(types_error::FATAL, "worker exploded on purpose")
            .with_sqlstate(types_error::ERRCODE_DIVISION_BY_ZERO)
            .with_context("inner worker frame"),
    ))
}

struct HelperState {
    identity: miscinit::SessionIdentityState,
    xact: types_core::SavedTransactionCharacteristics,
    xact_ts: types_core::TimestampTz,
    stmt_ts: types_core::TimestampTz,
    namespace: catalog_namespace::SessionNamespaceState,
    work_mem: i32,
    client: (Option<&'static str>, types_core::init::UserAuth),
    record_registry: typcache_seams::RecordRegistryHandle,
}

fn seed_helper_state() -> PgResult<HelperState> {
    guc::SetConfigOption(
        "work_mem",
        Some("8192"),
        types_guc::PGC_USERSET,
        types_guc::PGC_S_SESSION,
    )?;
    catalog_namespace::SetTempNamespaceState(700, 701);
    miscinit::set_client_connection_info(Some("scram:helper"), types_core::init::uaSCRAM);
    install_test_record_registry(Default::default());
    xact::SetXactIsoLevel(2);
    xact::SetXactReadOnly(true);
    xact::SetXactDeferrable(true);
    xact::SetParallelStartTimestamps(111_111, 222_222);
    Ok(capture_helper_state())
}

fn capture_helper_state() -> HelperState {
    HelperState {
        identity: miscinit::CaptureSessionIdentityState(),
        xact: xact::SaveTransactionCharacteristics(),
        xact_ts: xact::GetCurrentTransactionStartTimestamp(),
        stmt_ts: xact::GetCurrentStatementStartTimestamp(),
        namespace: catalog_namespace::CaptureSessionNamespaceState(),
        work_mem: g::work_mem(),
        client: miscinit::client_connection_info(),
        record_registry: test_record_registry_handle(),
    }
}

fn assert_helper_state(expected: &HelperState) -> PgResult<()> {
    assert_query_task_helper_clean()?;
    let actual = capture_helper_state();
    if actual.identity != expected.identity
        || actual.xact != expected.xact
        || actual.xact_ts != expected.xact_ts
        || actual.stmt_ts != expected.stmt_ts
        || actual.namespace != expected.namespace
        || actual.work_mem != expected.work_mem
        || actual.client != expected.client
        || !Arc::ptr_eq(&actual.record_registry, &expected.record_registry)
    {
        return Err(PgError::error("query-task helper state was not restored exactly").into());
    }
    Ok(())
}

fn panic_text(payload: &(dyn Any + Send)) -> Option<&str> {
    payload
        .downcast_ref::<&'static str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
}

fn query_task_binder_hook(_source: &parallel::ParallelShared) {
    let Some(target) = BINDER_TARGET.lock().unwrap_or_else(|e| e.into_inner()).take() else {
        return;
    };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        g::SetMyDatabaseId(target.database_id);
        lmgr_proc::GetPGProcByNumber(target.parallel_leader_proc_number)
            .databaseId
            .store(target.database_id, Relaxed);
        session::InitializeSession()?;
        assert_query_task_helper_clean()?;
        let helper_state = seed_helper_state()?;
        for _ in 0..3 {
            parallel::with_query_task_binding(&target, || {
                if miscinit::GetUserId() != 20
                    || !xact::IsTransactionOrTransactionBlock()
                    || !snapmgr::ActiveSnapshotSet()
                    || resowner::ResourceOwnerStateClean()
                {
                    return Err(PgError::error("query-task state was not fully bound").into());
                }
                Ok(())
            })?;
            assert_helper_state(&helper_state)?;
        }

        let sql_error = parallel::with_query_task_binding(&target, || {
            Err::<(), _>(PgError::new(ERROR, "binder SQL error").into())
        })
        .expect_err("SQL error must escape binding");
        if sql_error.message() != "binder SQL error" {
            return Err(PgError::error("query-task binder replaced the first SQL error").into());
        }
        assert_helper_state(&helper_state)?;

        let cancel = parallel::with_query_task_binding(&target, || {
            Err::<(), _>(
                PgError::new(ERROR, "binder cancellation")
                    .with_sqlstate(types_error::ERRCODE_QUERY_CANCELED)
                    .into(),
            )
        })
        .expect_err("cancellation must escape binding");
        if cancel.sqlstate() != types_error::ERRCODE_QUERY_CANCELED {
            return Err(PgError::error("query-task binder replaced cancellation").into());
        }
        assert_helper_state(&helper_state)?;

        parallel::with_query_task_binding(&target, || {
            let nested = parallel::with_query_task_binding(&target, || Ok(()))
                .expect_err("nested binding must be refused");
            if !nested.message().contains("nested") {
                return Err(PgError::error("nested binding refusal lost its reason").into());
            }
            Ok(())
        })?;
        assert_helper_state(&helper_state)?;

        let helper = lmgr_proc::GetPGProcByNumber(g::MyProcNumber());
        let leader = helper.lockGroupLeader.swap(INVALID_PROC_NUMBER, Relaxed);
        let cross_leader = parallel::with_query_task_binding(&target, || Ok(()))
            .expect_err("cross-leader binding must be refused");
        helper.lockGroupLeader.store(leader, Relaxed);
        if !cross_leader.message().contains("cross-leader") {
            return Err(PgError::error("cross-leader refusal lost its reason").into());
        }
        assert_helper_state(&helper_state)?;

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = parallel::with_query_task_binding(&target, || -> PgResult<()> {
                panic!("binder injected panic")
            });
        }));
        if panic.is_ok() {
            return Err(PgError::error("query-task binder swallowed panic").into());
        }
        assert_helper_state(&helper_state)?;

        for point in [
            parallel::QueryTaskFaultPoint::BindIdentity,
            parallel::QueryTaskFaultPoint::BindTransaction,
            parallel::QueryTaskFaultPoint::BindRelationMap,
            parallel::QueryTaskFaultPoint::BindTransactionSnapshot,
            parallel::QueryTaskFaultPoint::BindActiveSnapshot,
            parallel::QueryTaskFaultPoint::BindInvalidations,
            parallel::QueryTaskFaultPoint::BindGucs,
            parallel::QueryTaskFaultPoint::BindClient,
            parallel::QueryTaskFaultPoint::BindParallelMode,
        ] {
            parallel::set_query_task_fault(point, parallel::QueryTaskFaultAction::Error);
            let error = parallel::with_query_task_binding(&target, || Ok(()))
                .expect_err("partial bind fault must escape");
            if !error.message().contains("injected fault") {
                return Err(PgError::error("partial bind fault lost its reason").into());
            }
            assert_helper_state(&helper_state)?;
        }

        init_small::wretain::begin_task(true);
        parallel::set_query_task_fault(
            parallel::QueryTaskFaultPoint::BindGucs,
            parallel::QueryTaskFaultAction::Panic,
        );
        let bind_panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = parallel::with_query_task_binding(&target, || Ok(()));
        }))
        .expect_err("partial bind panic must escape");
        if panic_text(bind_panic.as_ref()) != Some("query-task injected panic at BindGucs")
            || init_small::wretain::candidate()
        {
            return Err(PgError::error("partial bind panic was not preserved and retired").into());
        }
        assert_helper_state(&helper_state)?;

        for point in [
            parallel::QueryTaskFaultPoint::FinishParallelMode,
            parallel::QueryTaskFaultPoint::FinishSnapshot,
            parallel::QueryTaskFaultPoint::FinishTransaction,
            parallel::QueryTaskFaultPoint::FinishSessionState,
            parallel::QueryTaskFaultPoint::FinishBoundary,
        ] {
            init_small::wretain::begin_task(true);
            parallel::set_query_task_fault(point, parallel::QueryTaskFaultAction::Error);
            let error = parallel::with_query_task_binding(&target, || Ok(()))
                .expect_err("cleanup fault must escape a successful body");
            if !error.message().contains("injected fault") || init_small::wretain::candidate() {
                return Err(PgError::error("cleanup fault did not retire the helper").into());
            }
            assert_helper_state(&helper_state)?;
        }

        init_small::wretain::begin_task(true);
        parallel::set_query_task_fault(
            parallel::QueryTaskFaultPoint::FinishSnapshot,
            parallel::QueryTaskFaultAction::Panic,
        );
        let cleanup_panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            parallel::with_query_task_binding(&target, || {
                Err::<(), _>(PgError::new(ERROR, "binder original error").into())
            })
        }));
        let original = match cleanup_panic {
            Ok(Err(error)) => error,
            _ => return Err(PgError::error("cleanup panic replaced the body error").into()),
        };
        if original.message() != "binder original error" || init_small::wretain::candidate() {
            return Err(PgError::error("cleanup panic did not preserve error and retire").into());
        }
        assert_helper_state(&helper_state)?;

        init_small::wretain::begin_task(true);
        parallel::set_query_task_fault(
            parallel::QueryTaskFaultPoint::FinishTransaction,
            parallel::QueryTaskFaultAction::Panic,
        );
        let double_panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = parallel::with_query_task_binding(&target, || -> PgResult<()> {
                panic!("binder original panic")
            });
        }))
        .expect_err("body panic must escape cleanup panic");
        if panic_text(double_panic.as_ref()) != Some("binder original panic")
            || init_small::wretain::candidate()
        {
            return Err(PgError::error("double panic did not preserve body panic and retire").into());
        }
        assert_helper_state(&helper_state)?;

        let refusals = BINDER_REFUSALS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
            .collect::<Vec<_>>();
        for (refused, needle) in refusals {
            let error = parallel::with_query_task_binding(&refused, || Ok(()))
                .expect_err("unsafe query-task target must be refused");
            if !error.message().contains(needle) {
                return Err(PgError::error(format!(
                    "query-task refusal expected {needle}, got {}",
                    error.message()
                ))
                .into());
            }
            assert_helper_state(&helper_state)?;
        }

        init_small::wretain::begin_task(true);
        let cleanup_fault = parallel::with_query_task_binding(&target, || {
            g::SetCritSectionCount(1);
            Err::<(), _>(PgError::new(ERROR, "binder first fault").into())
        })
        .expect_err("fault-injected cleanup must preserve the body error");
        if cleanup_fault.message() != "binder first fault" || init_small::wretain::candidate() {
            return Err(PgError::error("cleanup fault did not preserve error and retire helper").into());
        }
        g::SetCritSectionCount(0);
        assert_helper_state(&helper_state)?;
        Ok::<(), Box<PgError>>(())
    }));
    let report = match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(error.message().to_string()),
        Err(_) => Err("query-task binder hook panicked".to_string()),
    };
    *BINDER_REPORT.0.lock().unwrap_or_else(|e| e.into_inner()) = Some(report);
    BINDER_REPORT.1.notify_all();
}

fn assert_query_task_helper_clean() -> PgResult<()> {
    if miscinit::GetAuthenticatedUserId() != 10 || miscinit::GetUserId() != 10 {
        return Err(PgError::error("query-task helper boundary: identity was not restored").into());
    }
    if let Some(issue) = session::SessionEnvelopeBoundaryIssue() {
        return Err(PgError::error(format!("query-task helper boundary: {issue}")).into());
    }
    if xact::IsTransactionOrTransactionBlock() {
        return Err(PgError::error("query-task helper boundary: transaction is live").into());
    }
    if !snapmgr::SnapshotStateClean() {
        return Err(PgError::error("query-task helper boundary: snapshot state is live").into());
    }
    Ok(())
}

// The postmaster stand-in: what serverloop's maybe_start_bgworkers + the
// reaper do, driven synchronously by the test.
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
        wlog(format!("stand-in launch: idx={idx} slot={slot} gen={generation} pid={pid}"));
        bgworker::set_rw_pid(idx, pid);
        bgworker::ReportBackgroundWorkerPID(idx);
        // launch_backend's per-thread GUC boot (the postmaster snapshot).
        let guc_snapshot = guc::store::capture_nondefault_variables();
        let handle = std::thread::Builder::new()
            .name(format!("pg:parallel-e2e-worker:{pid}"))
            .spawn(move || {
                thread_globals();
                g::SetMyProcPid(pid);
                guc::store::initialize_guc_options_for_child(&guc_snapshot)
                    .and_then(|()| guc::store::restore_nondefault_variables(&guc_snapshot))
                    .unwrap();
                // fd::InitFileAccess is BaseInit's job inside BackgroundWorkerMain.
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
                if code.is_none() {
                    let msg = payload
                        .downcast_ref::<&str>()
                        .map(|s| s.to_string())
                        .or_else(|| payload.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "non-string panic".to_string());
                    wlog(format!("e2e worker {pid} died without proc_exit: {msg}"));
                } else {
                    wlog(format!("e2e worker {pid} proc_exit({})", code.unwrap()));
                }
                // on_shmem_exit is stubbed here; release the PGPROC by hand or
                // the 2-slot bgworker pool starves the next test's worker.
                if lmgr_proc::MyProc().is_some() {
                    lmgr_proc::ProcKill(0, 0);
                }
                // The reaper reports the exit no matter how the worker died —
                // otherwise a worker crash hangs the leader instead of raising
                // C's "parallel worker failed to initialize".
                bgworker::ReportBackgroundWorkerExit(idx);
                code.unwrap_or(27)
            })
            .unwrap();
        joins.push(handle);
    }
    joins
}

fn begin_parallel_ready_xact() {
    // start_xact_command's job (postgres.c); the worker-side xact assert
    // requires a real xact_start_timestamp.
    xact::SetCurrentStatementStartTimestamp();
    xact::StartTransactionCommand().unwrap();
    let snap = snapmgr::GetTransactionSnapshot().unwrap();
    snapmgr::PushActiveSnapshot(&snap).unwrap();
    xact::EnterParallelMode();
}

fn end_parallel_ready_xact() {
    xact::ExitParallelMode();
    snapmgr::PopActiveSnapshot().unwrap();
    xact::CommitTransactionCommand().unwrap();
}

#[test]
fn substrate_happy_path_with_launch_fewer() {
    let _s = serial();
    let _w = Watchdog::arm(180, "substrate_happy_path_with_launch_fewer");
    setup();

    g::SetMyDatabaseId(InvalidOid);
    begin_parallel_ready_xact();

    // Ask for 3; only 2 bgworker slots exist (max_worker_processes=2): the
    // third registration fails and C's contract is to run with fewer.
    let pcxt = parallel::CreateParallelContext("postgres", "substrate_e2e_main", 3).unwrap();
    parallel::InitializeParallelDSM(pcxt).unwrap();

    let leader_procno = g::MyProcNumber();
    let queues: Vec<Arc<shm_mq::ShmMq>> = (0..3)
        .map(|_| {
            let mq = shm_mq::shm_mq_create(tqueue::PARALLEL_TUPLE_QUEUE_SIZE);
            mq.set_receiver(leader_procno);
            mq
        })
        .collect();
    let mut readers: Vec<tqueue::TupleQueueReader> = queues
        .iter()
        .map(|mq| tqueue::TupleQueueReader::new(shm_mq::shm_mq_attach(Arc::clone(mq))))
        .collect();
    let e2e_shared = Arc::new(E2eShared {
        queues,
        instr: Mutex::new(vec![types_core::instrument::Instrumentation::default(); 3]),
    });
    parallel::set_private(pcxt, Arc::clone(&e2e_shared) as Arc<dyn Any + Send + Sync>);

    let launched = launch_as_if_under_postmaster(pcxt).unwrap();
    assert_eq!(launched, 2);
    assert_eq!(parallel::nworkers_launched(pcxt), 2);

    let joins = launch_registered_workers();
    assert_eq!(joins.len(), 2);

    parallel::WaitForParallelWorkersToAttach(pcxt).unwrap();

    let mut got: Vec<Vec<Vec<u8>>> = vec![Vec::new(), Vec::new()];
    let mut done = [false, false];
    while !(done[0] && done[1]) {
        let mut progressed = false;
        for w in 0..2 {
            if done[w] {
                continue;
            }
            let mut d = false;
            if let Some(bytes) = readers[w].next(true, &mut d).unwrap() {
                got[w].push(bytes.to_vec());
                progressed = true;
            }
            if d {
                done[w] = true;
                progressed = true;
            }
        }
        if !progressed {
            std::thread::yield_now();
        }
    }
    for (w, tuples) in got.iter().enumerate() {
        assert_eq!(tuples.len(), N_TUPLES, "worker {w} tuple count");
        for (i, t) in tuples.iter().enumerate() {
            assert_eq!(t, &tuple_image(w, i), "worker {w} tuple {i}");
        }
    }

    parallel::WaitForParallelWorkersToFinish(pcxt).unwrap();

    // ExecParallelRetrieveInstrumentation's shape: InstrAggNode of every
    // worker slot into the leader's node instrumentation.
    let mut leader_instr = types_core::instrument::Instrumentation::default();
    instrument::instr_init(&mut leader_instr, 0);
    for wi in e2e_shared.instr.lock().unwrap_or_else(|e| e.into_inner()).iter().take(2) {
        instrument::instr_agg_node(&mut leader_instr, wi);
    }
    assert_eq!(leader_instr.ntuples, (2 * N_TUPLES) as f64);
    assert_eq!(leader_instr.nloops, 2.0);

    parallel::DestroyParallelContext(pcxt).unwrap();
    end_parallel_ready_xact();

    for j in joins {
        assert_eq!(j.join().unwrap(), 0);
    }
    assert!(!parallel::ParallelContextActive());
}

#[test]
fn worker_error_rethrows_with_c_shape() {
    let _s = serial();
    let _w = Watchdog::arm(180, "worker_error_rethrows_with_c_shape");
    setup();

    g::SetMyDatabaseId(InvalidOid);
    begin_parallel_ready_xact();

    let pcxt = parallel::CreateParallelContext("postgres", "substrate_e2e_error", 1).unwrap();
    parallel::InitializeParallelDSM(pcxt).unwrap();
    let launched = launch_as_if_under_postmaster(pcxt).unwrap();
    assert_eq!(launched, 1);
    let joins = launch_registered_workers();

    let err = parallel::WaitForParallelWorkersToFinish(pcxt).unwrap_err();
    assert_eq!(
        err.message(),
        "worker exploded on purpose",
        "full error: {err:?}; worker log: {}",
        wlog_dump()
    );
    assert_eq!(err.sqlstate(), types_error::ERRCODE_DIVISION_BY_ZERO);
    assert_eq!(err.level, ERROR); // clamped from FATAL per C
    let ctx = err.context().unwrap();
    assert!(ctx.ends_with("parallel worker"), "context was: {ctx}");
    assert!(ctx.contains("inner worker frame"));

    parallel::DestroyParallelContext(pcxt).unwrap();
    end_parallel_ready_xact();
    for j in joins {
        assert_eq!(j.join().unwrap(), 1);
    }
}

#[test]
fn query_task_binder_restores_clean_helper_across_outcomes() {
    let _s = serial();
    let _w = Watchdog::arm(180, "query_task_binder_restores_clean_helper_across_outcomes");
    setup();

    g::SetMyDatabaseId(42);
    session::InitializeSession().unwrap();
    begin_parallel_ready_xact();
    let helper_identity = miscinit::CaptureSessionIdentityState();
    miscinit::ReplaceSessionIdentityState(miscinit::SessionIdentityState {
        authenticated_user_id: 10,
        session_user_id: 10,
        outer_user_id: 10,
        current_user_id: 20,
        system_user: helper_identity.system_user,
        session_user_is_superuser: true,
        security_restriction_context: 0,
        set_role_is_active: false,
    });
    let target = parallel::CreateParallelContext("postgres", "substrate_e2e_noop", 0).unwrap();
    parallel::InitializeParallelDSM(target).unwrap();
    miscinit::ReplaceSessionIdentityState(helper_identity);
    parallel::InstallQueryTaskBinding(target, parallel::QueryTaskBindingPolicy::default()).unwrap();
    *BINDER_TARGET.lock().unwrap_or_else(|e| e.into_inner()) =
        Some(parallel::shared_for(target));
    *BINDER_REPORT.0.lock().unwrap_or_else(|e| e.into_inner()) = None;

    let refusal_cases = [
        (
            42,
            parallel::QueryTaskBindingPolicy {
                has_params: true,
                ..Default::default()
            },
            "Params",
        ),
        (
            42,
            parallel::QueryTaskBindingPolicy {
                temp_state: true,
                ..Default::default()
            },
            "temporary",
        ),
        (
            42,
            parallel::QueryTaskBindingPolicy {
                serializable: true,
                ..Default::default()
            },
            "serializable",
        ),
        (
            42,
            parallel::QueryTaskBindingPolicy {
                pending_invalidations: true,
                ..Default::default()
            },
            "invalidations",
        ),
        (43, parallel::QueryTaskBindingPolicy::default(), "cross-database"),
    ];
    let mut refusal_contexts = Vec::new();
    for (database, policy, needle) in refusal_cases {
        g::SetMyDatabaseId(database);
        let context = parallel::CreateParallelContext("postgres", "substrate_e2e_noop", 0).unwrap();
        parallel::InitializeParallelDSM(context).unwrap();
        parallel::InstallQueryTaskBinding(context, policy).unwrap();
        BINDER_REFUSALS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((parallel::shared_for(context), needle));
        refusal_contexts.push(context);
    }

    g::SetMyDatabaseId(InvalidOid);
    let source = parallel::CreateParallelContext("postgres", "substrate_e2e_noop", 1).unwrap();
    parallel::InitializeParallelDSM(source).unwrap();
    assert_eq!(launch_as_if_under_postmaster(source).unwrap(), 1);
    let joins = launch_registered_workers();
    parallel::WaitForParallelWorkersToAttach(source).unwrap();

    let report = BINDER_REPORT.0.lock().unwrap_or_else(|e| e.into_inner());
    let (mut report, timeout) = BINDER_REPORT
        .1
        .wait_timeout_while(report, std::time::Duration::from_secs(120), |r| r.is_none())
        .unwrap_or_else(|e| e.into_inner());
    assert!(!timeout.timed_out(), "binder hook timed out; worker log: {}", wlog_dump());
    report.take().expect("binder hook omitted report").unwrap();
    drop(report);

    parallel::WaitForParallelWorkersToFinish(source).unwrap();
    parallel::DestroyParallelContext(source).unwrap();
    for context in refusal_contexts {
        parallel::DestroyParallelContext(context).unwrap();
    }
    parallel::DestroyParallelContext(target).unwrap();
    g::SetMyDatabaseId(42);
    end_parallel_ready_xact();
    for join in joins {
        assert_eq!(join.join().unwrap(), 0);
    }
}
