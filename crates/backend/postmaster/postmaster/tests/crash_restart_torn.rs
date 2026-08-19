//! Torn-shared-state arm of the catchable-crash choreography (upstream
//! issue #67, hardening item 2): a REAL panic unwinds a backend thread
//! while it HOLDS an LWLock mid-mutation of shared state. Locking is
//! C-style, not RAII — nothing releases on unwind — so the lock stays held
//! and the mutation stays half-applied, exactly the torn state C's quickdie
//! also leaves behind (C only ever risks *shared* memory this way; here it
//! is the whole address space, which is why the ladder must be exercised
//! against it). The reinit arm must clear both: LWLockResetAfterCrash
//! re-arms the held lock and the shmem reset walk restores the boot image.
//!
//! Own test binary (process-global shmem, like crash_restart.rs, whose
//! harness this clones; that test pins the orchestration with a fabricated
//! exit and hand-tampered state — this one drives the state through a real
//! unwind).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;

use postmaster::{with_pm, PMState, StartupStatusEnum};
use types_core::init::BackendType;

static SIGQUIT_SEEN: AtomicBool = AtomicBool::new(false);

fn observe_sigquit() {
    SIGQUIT_SEEN.store(true, Ordering::SeqCst);
}

fn write_valid_control_file(dir: &str) {
    std::fs::create_dir_all(format!("{dir}/global")).unwrap();
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

const VICTIM_PID: i32 = 9101;
const SIBLING_PID: i32 = 9102;

#[test]
fn panic_under_lwlock_mid_mutation_reinits_clean() {
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
    pg_prng::global_prng(|prng| prng.seed(43));

    init_small::globals::SetIsPostmasterEnvironment(true);
    init_small::globals::SetMaxConnections(10);
    init_small::globals::set_max_worker_processes(8);
    init_small::globals::SetNBuffers(16);
    init_small::globals::SetMaxBackends(
        10 + 16 + 8 + 10 + types_storage::storage::NUM_SPECIAL_WORKER_PROCS,
    );
    pmchild_seams::init_postmaster_child_slots::call();
    bgworker::BackgroundWorkerShmemInit();

    let dir = std::env::temp_dir().join(format!("pgrust-crash-torn-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let dir: &'static str = Box::leak(dir.to_str().unwrap().to_string().into_boxed_str());
    write_valid_control_file(dir);
    init_small::globals::SetDataDir(dir);

    ipci_seams::create_shared_memory_and_semaphores::call(1).unwrap();
    guc_tables::vars::remove_temp_files_after_crash.write(false);

    waiteventset::InitializeWaitEventSupport().unwrap();
    miscinit::InitProcessLocalLatch();

    let victim_slot =
        pmchild_seams::assign_postmaster_child_slot::call(BackendType::Backend).unwrap();
    pmchild_seams::set_child_pid::call(victim_slot, VICTIM_PID);
    let sibling_slot =
        pmchild_seams::assign_postmaster_child_slot::call(BackendType::Backend).unwrap();
    pmchild_seams::set_child_pid::call(sibling_slot, SIBLING_PID);

    // Sibling backend: parks at its quickdie observation point.
    let (ready_tx, ready_rx) = channel();
    let (announce_tx, announce_rx) = channel::<()>();
    let sibling = std::thread::spawn(move || {
        init_small::globals::SetIsUnderPostmaster(true);
        init_small::globals::SetMyProcNumber(1);
        init_small::globals::SetMyProcPid(SIBLING_PID);
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
        announce_rx.recv().unwrap();
        postmaster_seams::announce_child_exit::call(SIBLING_PID, 2 << 8);
    });
    ready_rx.recv().unwrap();

    with_pm(|pm| {
        pm.pm_state = PMState::PM_RUN;
        pm.conns_allowed = true;
    });

    // Victim backend: a REAL panic while HOLDING an LWLock, mid-mutation of
    // shared transam state. The unwind is caught (launch_backend's
    // catch_unwind contract) and translated into the synthetic crash-exit
    // announcement, exactly as the production wrapper does for the
    // catchable class.
    let lock0 = lwlock::main_lock(0);
    let victim = std::thread::spawn(move || {
        init_small::globals::SetIsUnderPostmaster(true);
        init_small::globals::SetMyProcNumber(0);
        init_small::globals::SetMyProcPid(VICTIM_PID);
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            lwlock::LWLockAcquire(lwlock::main_lock(0), lwlock::LW_EXCLUSIVE, 0).unwrap();
            // Half-applied shared mutation: the "torn" state at fault time.
            varsup::TransamVariables().nextOid.store(777, Ordering::Relaxed);
            panic!("torn-state crash: panic while holding an LWLock mid-mutation");
        }));
        assert!(unwound.is_err());
        postmaster_seams::announce_child_exit::call(VICTIM_PID, libc::SIGABRT);
    });
    victim.join().unwrap();

    // The torn state is real: C-style locking releases nothing on unwind,
    // and the mutation stuck. This is what the ladder must clear.
    assert_ne!(
        lock0.state.load(Ordering::Relaxed) & lwlock::LW_VAL_EXCLUSIVE,
        0,
        "the unwind must have left the LWLock held (no RAII release)"
    );
    assert_eq!(varsup::TransamVariables().nextOid.load(Ordering::Relaxed), 777);

    // Crash choreography: fatal fan-out, sibling quiesce, reinit ladder.
    postmaster::process_pm_child_exit().unwrap();
    assert!(with_pm(|pm| pm.fatal_error), "HandleFatalError must set fatal_error");
    assert_eq!(with_pm(|pm| pm.pm_state), PMState::PM_WAIT_BACKENDS);

    announce_tx.send(()).unwrap();
    sibling.join().unwrap();
    assert!(SIGQUIT_SEEN.load(Ordering::SeqCst), "sibling must observe SIGQUIT");

    postmaster::process_pm_child_exit().unwrap();

    // Post-reinit: the held lock is re-armed, the torn mutation is gone.
    assert_eq!(
        lock0.state.load(Ordering::Relaxed),
        lwlock::LW_FLAG_RELEASE_OK,
        "LWLockResetAfterCrash must clear the crash-held exclusive bit"
    );
    assert_eq!(
        varsup::TransamVariables().nextOid.load(Ordering::Relaxed),
        0,
        "the shmem reset walk must restore the boot image over the torn store"
    );
    assert_eq!(with_pm(|pm| pm.pm_state), PMState::PM_STARTUP);
    assert!(with_pm(|pm| pm.startup.is_some()));
    assert_eq!(with_pm(|pm| pm.startup_status), StartupStatusEnum::Running);

    // And the lock is USABLE again, not just bit-reset: a fresh
    // acquire/release cycle from this thread succeeds.
    lwlock::LWLockAcquire(lock0, lwlock::LW_EXCLUSIVE, 0).unwrap();
    lwlock::LWLockRelease(lock0).unwrap();
    assert_eq!(lock0.state.load(Ordering::Relaxed), lwlock::LW_FLAG_RELEASE_OK);
}
