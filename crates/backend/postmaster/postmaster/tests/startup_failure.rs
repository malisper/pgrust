//! upstream affdb2dd5c67 (18.4): Fix orphaned processes when startup process
//! fails during PM_STARTUP. Since 7ff23c6d277d the checkpointer, bgwriter,
//! IO workers and early bgworkers are alive in PM_STARTUP, so a startup child
//! that exits FATAL there must not short-circuit to ExitPostmaster(1): it
//! takes the crash path (SIGQUIT fan-out, PM_WAIT_BACKENDS) and the
//! postmaster exits only from PM_NO_CHILDREN, with "shutting down due to
//! startup process failure". Same harness shape as tests/lifecycle.rs (full
//! ipci bringup over REAL shared state; one test per binary).

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc::channel;

use postmaster::{with_pm, PMState, PmChild, StartupStatusEnum};
use types_core::init::BackendType;

static SIGQUIT_SEEN: AtomicBool = AtomicBool::new(false);
static QUIT_REASON_SEEN: AtomicU32 = AtomicU32::new(u32::MAX);

// quickdie's observation point: the disposition reads the postmaster's reason.
fn observe_sigquit() {
    QUIT_REASON_SEEN.store(pmsignal::GetQuitSignalReason() as u32, Ordering::SeqCst);
    SIGQUIT_SEEN.store(true, Ordering::SeqCst);
}

fn write_valid_control_file(dir: &str) {
    std::fs::create_dir_all(format!("{dir}/global")).unwrap();
    // update_controlfile opens without O_CREAT (C parity).
    std::fs::write(format!("{dir}/global/pg_control"), []).unwrap();
    let mut cf = controldata_utils::ControlFileData::ZEROED;
    cf.pg_control_version = controldata_utils::PG_CONTROL_VERSION;
    cf.catalog_version_no = controldata_utils::CATALOG_VERSION_NO;
    cf.maxAlign = 8;
    cf.floatFormat = 1234567.0;
    cf.blcksz = types_core::BLCKSZ as u32;
    cf.relseg_size = types_storage::smgr::RELSEG_SIZE;
    cf.xlog_blcksz = transam_xlog::XLOG_BLCKSZ as u32;
    cf.xlog_seg_size = 16 * 1024 * 1024;
    cf.nameDataLen = types_core::NAMEDATALEN as u32;
    cf.indexMaxKeys = types_core::INDEX_MAX_KEYS as u32;
    cf.toast_max_chunk_size = 1996;
    cf.loblksize = types_storage::large_object::LOBLKSIZE as u32;
    cf.float8ByVal = true;
    controldata_utils::update_controlfile(dir, &mut cf, false).unwrap();
}

const STARTUP_PID: i32 = 9101;
const CHECKPOINTER_PID: i32 = 9102;

fn full_ipci_bringup() -> &'static str {
    guc_tables::init_seams();
    init_small::init_seams();
    transam_xlog::init_seams();
    shmem::init_seams();
    ipc::init_seams();
    ipci::init_seams();
    pmchild::init_seams();
    postmaster::init_seams();
    pgstat::init_seams();
    pg_prng::init_seams();
    s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
    s_lock_seams::finish_spin_delay::set(|_| {});
    pg_sema_seams::pg_semaphore_create::set(|_| {});
    xact_seams::is_in_parallel_mode::set(|| false);
    xact_seams::get_current_transaction_nest_level::set(|| 1);
    scalar_seams::parse_bool::set(|value| match value {
        "on" | "true" | "yes" | "1" => Some(true),
        "off" | "false" | "no" | "0" => Some(false),
        _ => None,
    });
    aclchk_seams::pg_parameter_aclcheck_set::set(|_, _| Ok(true));
    mbutils_seams::get_database_encoding::set(|| 6);
    file_seams::with_allocated_dir::set(|dirname, cb| {
        let mut ret = false;
        let Ok(entries) = std::fs::read_dir(dirname) else { return Ok(false) };
        for entry in entries {
            ret = cb(entry.unwrap().file_name().to_str().unwrap())?;
            if ret {
                break;
            }
        }
        Ok(ret)
    });
    backend_status_seams::backend_status_shmem_size::set(|| Ok(4096));
    backend_status_seams::backend_status_shmem_init::set(|| Ok(()));
    backend_status_seams::backend_status_shmem_reset_after_crash::set(|| {});
    {
        use std::sync::atomic::AtomicI32;
        use std::sync::atomic::Ordering::Relaxed;
        static AV_SLOTS: AtomicI32 = AtomicI32::new(16);
        static WAL_SENDERS: AtomicI32 = AtomicI32::new(10);
        static MAX_PREPARED: AtomicI32 = AtomicI32::new(0);
        static MAX_LOCKS: AtomicI32 = AtomicI32::new(64);
        guc_tables::vars::autovacuum_worker_slots.install(guc_tables::GucVarAccessors {
            get: || AV_SLOTS.load(Relaxed),
            set: |v| AV_SLOTS.store(v, Relaxed),
        });
        guc_tables::vars::max_wal_senders.install(guc_tables::GucVarAccessors {
            get: || WAL_SENDERS.load(Relaxed),
            set: |v| WAL_SENDERS.store(v, Relaxed),
        });
        guc_tables::vars::max_prepared_xacts.install(guc_tables::GucVarAccessors {
            get: || MAX_PREPARED.load(Relaxed),
            set: |v| MAX_PREPARED.store(v, Relaxed),
        });
        guc_tables::vars::max_locks_per_xact.install(guc_tables::GucVarAccessors {
            get: || MAX_LOCKS.load(Relaxed),
            set: |v| MAX_LOCKS.store(v, Relaxed),
        });
    }
    aio_core::init_seams();
    guc_tables::vars::io_max_combine_limit.install_if_absent(guc_tables::GucVarAccessors {
        get: || 16,
        set: |_| {},
    });

    guc::store::initialize_guc_options().unwrap();
    pg_prng::global_prng(|prng| prng.seed(42));

    init_small::globals::SetIsPostmasterEnvironment(true);
    init_small::globals::SetMaxConnections(10);
    init_small::globals::set_max_worker_processes(8);
    init_small::globals::SetNBuffers(16);
    init_small::globals::SetMaxBackends(
        10 + 16 + 8 + 10 + types_storage::storage::NUM_SPECIAL_WORKER_PROCS,
    );
    pmchild_seams::init_postmaster_child_slots::call();
    bgworker::BackgroundWorkerShmemInit();

    let dir = std::env::temp_dir().join(format!("pgrust-startup-failure-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let dir: &'static str = Box::leak(dir.to_str().unwrap().to_string().into_boxed_str());
    write_valid_control_file(dir);
    init_small::globals::SetDataDir(dir);

    ipci_seams::create_shared_memory_and_semaphores::call(1).unwrap();

    guc_tables::vars::io_max_concurrency.write(-1);
    guc_tables::vars::remove_temp_files_after_crash.write(false);

    waiteventset::InitializeWaitEventSupport().unwrap();
    miscinit::InitProcessLocalLatch();
    dir
}

#[test]
fn startup_failure_in_pm_startup_waits_for_live_children() {
    let _dir = full_ipci_bringup();

    // The children 7ff23c6d277d launches before the startup process: a live
    // checkpointer stands in for all of them. Slots first (pmchild), then the
    // postmaster's own PmChild records.
    let checkpointer_slot =
        pmchild_seams::assign_postmaster_child_slot::call(BackendType::Checkpointer).unwrap();
    pmchild_seams::set_child_pid::call(checkpointer_slot, CHECKPOINTER_PID);
    let startup_slot =
        pmchild_seams::assign_postmaster_child_slot::call(BackendType::Startup).unwrap();
    pmchild_seams::set_child_pid::call(startup_slot, STARTUP_PID);

    let (ready_tx, ready_rx) = channel();
    let (announce_tx, announce_rx) = channel::<()>();
    let checkpointer = std::thread::spawn(move || {
        init_small::globals::SetIsUnderPostmaster(true);
        init_small::globals::SetMyProcNumber(1);
        init_small::globals::SetMyProcPid(CHECKPOINTER_PID);
        procsignal::ProcSignalInit(&[]).unwrap();
        procsignal::pqsignal_thread(
            libc::SIGQUIT,
            procsignal::ThreadSignalHandler::Simple(observe_sigquit),
        );
        ready_tx.send(()).unwrap();
        while !SIGQUIT_SEEN.load(Ordering::SeqCst) {
            procsignal::DrainThreadSignals().unwrap();
            std::thread::yield_now();
        }
        // quickdie's thread rendering: exit code 2, announced to the reaper
        // only once the test asks for it, so the first reap is deterministic.
        announce_rx.recv().unwrap();
        postmaster_seams::announce_child_exit::call(CHECKPOINTER_PID, 2 << 8);
    });
    ready_rx.recv().unwrap();

    with_pm(|pm| {
        pm.pm_state = PMState::PM_STARTUP;
        pm.startup_status = StartupStatusEnum::Running;
        pm.startup = Some(PmChild {
            child_slot: startup_slot,
            bkend_type: BackendType::Startup,
            pid: STARTUP_PID,
        });
        pm.checkpointer = Some(PmChild {
            child_slot: checkpointer_slot,
            bkend_type: BackendType::Checkpointer,
            pid: CHECKPOINTER_PID,
        });
    });

    // The startup process exits FATAL (exit code 1) during PM_STARTUP.
    postmaster_seams::announce_child_exit::call(STARTUP_PID, 1 << 8);
    match catch_unwind(AssertUnwindSafe(postmaster::process_pm_child_exit)) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("process_pm_child_exit failed: {e:?}"),
        Err(payload) => {
            let code = payload.downcast_ref::<ipc::ProcExitThread>().map(|p| p.code);
            panic!(
                "pre-fix shortcut: the postmaster exited (ExitPostmaster, code {code:?}) \
                 with a live checkpointer instead of waiting for it"
            );
        }
    }

    assert_eq!(
        with_pm(|pm| pm.startup_status),
        StartupStatusEnum::Crashed,
        "a FATAL startup exit is catastrophic: STARTUP_CRASHED, never reinitialize"
    );
    assert!(with_pm(|pm| pm.fatal_error), "the startup failure runs HandleFatalError");
    assert_eq!(
        with_pm(|pm| pm.pm_state),
        PMState::PM_WAIT_BACKENDS,
        "HandleFatalError must route PM_STARTUP to PM_WAIT_BACKENDS (no assertion arm)"
    );
    assert!(with_pm(|pm| pm.startup.is_none()), "the startup child's record is reaped");

    // terminate-all: the live checkpointer observes SIGQUIT/PMQUIT_FOR_CRASH.
    announce_tx.send(()).unwrap();
    checkpointer.join().unwrap();
    assert!(
        SIGQUIT_SEEN.load(Ordering::SeqCst),
        "every child alive during PM_STARTUP is SIGQUIT'ed on startup failure"
    );
    assert_eq!(
        QUIT_REASON_SEEN.load(Ordering::SeqCst),
        pmsignal::QuitSignalReason::PMQUIT_FOR_CRASH as u32,
        "the checkpointer must see PMQUIT_FOR_CRASH at its quickdie point"
    );

    // With the last child reaped the state machine reaches PM_NO_CHILDREN and
    // only now exits: "shutting down due to startup process failure", status 1.
    match catch_unwind(AssertUnwindSafe(postmaster::process_pm_child_exit)) {
        Err(payload) => {
            let exit = payload
                .downcast_ref::<ipc::ProcExitThread>()
                .unwrap_or_else(|| panic!("expected ExitPostmaster's ProcExitThread unwind"));
            assert_eq!(exit.code, 1, "startup failure exits the postmaster with status 1");
        }
        Ok(r) => panic!("expected ExitPostmaster(1) from PM_NO_CHILDREN, got {r:?}"),
    }
    assert!(
        with_pm(|pm| pm.checkpointer.is_none()),
        "the checkpointer was reaped before the postmaster exited"
    );
}
