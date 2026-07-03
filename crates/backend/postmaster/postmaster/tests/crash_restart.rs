//! Catchable-class crash choreography end to end: a fake backend crashes →
//! HandleChildCrash SIGQUITs the sibling (observed quickdie-style) → the
//! reaper drains → the reinit arm runs the reset walk and panics loudly at
//! the first non-resettable subsystem (notes/crash-restart-design.md).

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;

use postmaster::{with_pm, PMState};
use types_core::init::BackendType;

static SIGQUIT_SEEN: AtomicBool = AtomicBool::new(false);
static QUIT_REASON_SEEN: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(u32::MAX);

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

const VICTIM_PID: i32 = 9001;
const SIBLING_PID: i32 = 9002;

#[test]
fn crash_fans_out_sigquit_and_reinit_names_first_blocker() {
    guc_tables::init_seams();
    {
        use std::sync::atomic::AtomicI32;
        static WAL_SENDERS: AtomicI32 = AtomicI32::new(10);
        static AV_SLOTS: AtomicI32 = AtomicI32::new(16);
        guc_tables::vars::max_wal_senders.install(guc_tables::GucVarAccessors {
            get: || WAL_SENDERS.load(Ordering::Relaxed),
            set: |v| WAL_SENDERS.store(v, Ordering::Relaxed),
        });
        guc_tables::vars::autovacuum_worker_slots.install(guc_tables::GucVarAccessors {
            get: || AV_SLOTS.load(Ordering::Relaxed),
            set: |v| AV_SLOTS.store(v, Ordering::Relaxed),
        });
    }
    init_small::init_seams();
    transam_xlog::init_seams();
    shmem::init_seams();
    ipc::init_seams();
    pmchild::init_seams();
    postmaster::init_seams();
    s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
    s_lock_seams::finish_spin_delay::set(|_| {});
    pg_sema_seams::pg_semaphore_create::set(|_| {});

    init_small::globals::SetMaxConnections(10);
    init_small::globals::set_max_worker_processes(8);
    init_small::globals::SetMaxBackends(
        10 + 3 + 8 + 2 + types_storage::storage::NUM_SPECIAL_WORKER_PROCS,
    );
    pmchild_seams::init_postmaster_child_slots::call();
    lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
        autovacuum_worker_slots: 3,
        max_wal_senders: 2,
        max_prepared_xacts: 2,
        fastpath_lock_groups_per_backend: 1,
    });
    procsignal::ProcSignalShmemInit();
    pmsignal::PMSignalShmemInit(pmchild_seams::max_live_postmaster_children::call());
    lwlock::CreateLWLocks(false).unwrap();
    varsup::VarsupShmemInit();

    waiteventset::InitializeWaitEventSupport().unwrap();
    miscinit::InitProcessLocalLatch();

    let dir = std::env::temp_dir().join(format!("pgrust-crash-restart-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let dir: &'static str = Box::leak(dir.to_str().unwrap().to_string().into_boxed_str());
    write_valid_control_file(dir);
    init_small::globals::SetDataDir(dir);
    guc_tables::vars::remove_temp_files_after_crash.write(false);

    let victim_slot =
        pmchild_seams::assign_postmaster_child_slot::call(BackendType::Backend).unwrap();
    pmchild_seams::set_child_pid::call(victim_slot, VICTIM_PID);
    let sibling_slot =
        pmchild_seams::assign_postmaster_child_slot::call(BackendType::Backend).unwrap();
    pmchild_seams::set_child_pid::call(sibling_slot, SIBLING_PID);

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
        // quickdie's thread rendering: exit code 2, announced to the reaper —
        // gated so the first reap below stays deterministic.
        announce_rx.recv().unwrap();
        postmaster_seams::announce_child_exit::call(SIBLING_PID, 2 << 8);
    });
    ready_rx.recv().unwrap();

    with_pm(|pm| {
        pm.pm_state = PMState::PM_RUN;
        pm.conns_allowed = true;
    });

    postmaster_seams::announce_child_exit::call(VICTIM_PID, libc::SIGABRT);
    postmaster::process_pm_child_exit().unwrap();

    assert!(with_pm(|pm| pm.fatal_error), "HandleFatalError must set fatal_error");
    assert_eq!(with_pm(|pm| pm.pm_state), PMState::PM_WAIT_BACKENDS);

    announce_tx.send(()).unwrap();
    sibling.join().unwrap();
    assert!(SIGQUIT_SEEN.load(Ordering::SeqCst), "sibling must observe SIGQUIT");
    assert_eq!(
        QUIT_REASON_SEEN.load(Ordering::SeqCst),
        pmsignal::QuitSignalReason::PMQUIT_FOR_CRASH as u32,
        "sibling must see PMQUIT_FOR_CRASH at its quickdie point"
    );

    varsup::TransamVariables().nextOid.store(777, Ordering::Relaxed);
    let lock0 = lwlock::main_lock(0);
    lock0
        .state
        .store(lwlock::LW_FLAG_RELEASE_OK | 5, Ordering::Relaxed);

    let err = catch_unwind(AssertUnwindSafe(|| {
        postmaster::process_pm_child_exit().unwrap();
    }))
    .expect_err("reinit arm must panic at the first non-resettable subsystem");
    let msg = err
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| err.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_default();
    assert!(
        msg.contains("crash-restart reinit blocked") && msg.contains("transam_xlog"),
        "panic must name the blocker, got: {msg}"
    );

    assert_eq!(with_pm(|pm| pm.pm_state), PMState::PM_NO_CHILDREN);
    assert_eq!(
        varsup::TransamVariables().nextOid.load(Ordering::Relaxed),
        0,
        "VarsupShmemReset must restore the boot image before the blocker"
    );
    assert_eq!(
        lock0.state.load(Ordering::Relaxed),
        lwlock::LW_FLAG_RELEASE_OK,
        "LWLockResetAfterCrash must re-arm locks before the blocker"
    );
}
