//! Lifecycle test design (PR #1036) — the reaper half of the crash firewall,
//! over REAL shared state via the full ipci bringup (same harness as
//! `crash_restart.rs`):
//!
//! - D-1 / I-1 [P0]: a backend crash status (the B1 crash-class the
//!   `launch_backend::panic_payload_to_exit_status` seam produces for a raw
//!   panic — `SIGABRT`) drives the postmaster crash cycle: terminate-all
//!   (siblings SIGQUIT'd with `PMQUIT_FOR_CRASH`) + `ResetShmemAfterCrash` +
//!   re-enter `PM_STARTUP`, with the postmaster PID unchanged (no external
//!   supervisor restart — the C-parity "same postmaster recovered").
//! - D-5 [P1]: the negative control — a FATAL exit (status 1) reclaims only
//!   the failed backend's slot and triggers NO crash cycle; sibling sessions
//!   keep answering. This proves D-1's terminate-all is crash-SPECIFIC,
//!   exactly as C distinguishes exit(1) (CleanupBackend) from a signal death
//!   (HandleChildCrash).
//!
//! The exit statuses fed to the reaper are computed through the SAME
//! production status-map seam a real backend crash flows through
//! (`launch_backend::panic_payload_to_exit_status`), so this test is wired to
//! the actual firewall path, not to hand-picked magic numbers. C is the oracle
//! for both outcomes (documented in docs/testing/findings-lifecycle.md).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;

use postmaster::{with_pm, PMState, StartupStatusEnum};
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
const FATAL_PID: i32 = 9003;

/// The B1 crash-class exit status a real backend panic produces, computed
/// through the production seam (raw panic -> SIGABRT). This is precisely the
/// status a backend that called `crash_primitive::pgrust_test_backend_panic()`
/// hands the reaper.
fn crash_status_from_seam() -> i32 {
    let payload = std::panic::catch_unwind(|| panic!("test backend crash (B1 SIGABRT class)"))
        .expect_err("panic must unwind");
    launch_backend::panic_payload_to_exit_status(payload.as_ref())
}

/// The FATAL exit-status (exit code 1) a refused/failed backend hands the
/// reaper, via the same seam (ProcExitThread{code:1} -> 1<<8).
fn fatal_status_from_seam() -> i32 {
    launch_backend::panic_payload_to_exit_status(&ipc::ProcExitThread { code: 1 })
}

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
    bgworker::BackgroundWorkerShmemInit().expect("bgworker shmem init");

    let dir = std::env::temp_dir().join(format!("pgrust-lifecycle-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let dir: &'static str = Box::leak(dir.to_str().unwrap().to_string().into_boxed_str());
    write_valid_control_file(dir);
    init_small::globals::SetDataDir(dir);

    ipci_seams::create_shared_memory_and_semaphores::call(1).unwrap();

    guc_tables::vars::io_max_concurrency.write(-1);
    guc_tables::vars::remove_temp_files_after_crash.write(false);

    waiteventset::InitializeWaitEventSupport().unwrap();
    miscinit::InitProcessLocalLatch().expect("local latch");
    dir
}

#[test]
fn firewall_crash_terminates_all_and_recovers_but_fatal_spares_siblings() {
    let _dir = full_ipci_bringup();

    // Slots: a victim (will crash), a live sibling, and a backend that exits
    // FATAL (D-5 negative control).
    let victim_slot =
        pmchild_seams::assign_postmaster_child_slot::call(BackendType::Backend).unwrap();
    pmchild_seams::set_child_pid::call(victim_slot, VICTIM_PID);
    let sibling_slot =
        pmchild_seams::assign_postmaster_child_slot::call(BackendType::Backend).unwrap();
    pmchild_seams::set_child_pid::call(sibling_slot, SIBLING_PID);
    let fatal_slot =
        pmchild_seams::assign_postmaster_child_slot::call(BackendType::Backend).unwrap();
    pmchild_seams::set_child_pid::call(fatal_slot, FATAL_PID);

    // The sibling session: installs a SIGQUIT observer (quickdie's point) and
    // keeps serving until it sees SIGQUIT.
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
    let postmaster_pid_before = init_small::globals::PostmasterPid();

    // ---- D-5: FATAL exit (status 1) must NOT trigger a crash cycle ---------
    assert_eq!(fatal_status_from_seam(), 1 << 8, "FATAL seam status must be WIFEXITED(1)");
    postmaster_seams::announce_child_exit::call(FATAL_PID, fatal_status_from_seam());
    postmaster::process_pm_child_exit().unwrap();

    assert!(
        !with_pm(|pm| pm.fatal_error),
        "D-5: a FATAL exit(1) must NOT set fatal_error (no crash cycle)"
    );
    assert_eq!(
        with_pm(|pm| pm.pm_state),
        PMState::PM_RUN,
        "D-5: the postmaster stays in PM_RUN through a FATAL backend exit"
    );
    assert!(
        pmchild_seams::find_postmaster_child_by_pid::call(FATAL_PID).is_none(),
        "D-5: the FATAL backend's slot must be reclaimed"
    );
    assert!(
        !SIGQUIT_SEEN.load(Ordering::SeqCst),
        "D-5: sibling sessions keep answering — a FATAL exit never fans out SIGQUIT"
    );

    // ---- D-1 / I-1: a backend crash fans out SIGQUIT and reinitializes -----
    // Seam sanity: the two crash classes both map to non-clean statuses the
    // reaper treats as crashes (neither 0 nor 1).
    let crash_status = crash_status_from_seam();
    assert_eq!(crash_status, libc::SIGABRT, "raw backend panic -> SIGABRT crash class");
    let segv_status =
        launch_backend::panic_payload_to_exit_status(&ipc::KilledBySignal { signo: libc::SIGSEGV });
    assert_eq!(segv_status, libc::SIGSEGV, "SIGSEGV crash primitive -> WTERMSIG(SIGSEGV)");
    for (name, st) in [("SIGABRT", crash_status), ("SIGSEGV", segv_status)] {
        assert!(
            (st & 0x7f) != 0,
            "{name} status must be a signal death (crash class), not a clean/FATAL exit"
        );
    }

    postmaster_seams::announce_child_exit::call(VICTIM_PID, crash_status);
    postmaster::process_pm_child_exit().unwrap();

    assert!(with_pm(|pm| pm.fatal_error), "D-1: a crash must set fatal_error (HandleFatalError)");
    assert_eq!(
        with_pm(|pm| pm.pm_state),
        PMState::PM_WAIT_BACKENDS,
        "D-1: the crash cycle enters PM_WAIT_BACKENDS"
    );

    // terminate-all: the sibling must observe SIGQUIT/PMQUIT_FOR_CRASH.
    announce_tx.send(()).unwrap();
    sibling.join().unwrap();
    assert!(
        SIGQUIT_SEEN.load(Ordering::SeqCst),
        "D-1/I-1: EVERY sibling is terminated on a peer crash (a surviving sibling = FAIL)"
    );
    assert_eq!(
        QUIT_REASON_SEEN.load(Ordering::SeqCst),
        pmsignal::QuitSignalReason::PMQUIT_FOR_CRASH as u32,
        "sibling must see PMQUIT_FOR_CRASH at its quickdie point"
    );

    // Dirty two shared structures, then let the reinit arm run.
    varsup::TransamVariables().nextOid.store(777, Ordering::Relaxed);
    let lock0 = lwlock::main_lock(0);
    lock0.state.store(lwlock::LW_FLAG_RELEASE_OK | 5, Ordering::Relaxed);

    postmaster::process_pm_child_exit().unwrap();

    assert_eq!(
        varsup::TransamVariables().nextOid.load(Ordering::Relaxed),
        0,
        "ResetShmemAfterCrash must restore the boot image (VarsupShmemReset)"
    );
    assert_eq!(
        lock0.state.load(Ordering::Relaxed),
        lwlock::LW_FLAG_RELEASE_OK,
        "LWLockResetAfterCrash must re-arm locks"
    );
    assert_eq!(
        with_pm(|pm| pm.pm_state),
        PMState::PM_STARTUP,
        "D-1: recovery re-enters PM_STARTUP"
    );
    assert!(with_pm(|pm| pm.startup.is_some()), "a fresh startup child must be launched");
    assert_eq!(with_pm(|pm| pm.startup_status), StartupStatusEnum::Running);
    assert_eq!(
        init_small::globals::PostmasterPid(),
        postmaster_pid_before,
        "D-1: the SAME postmaster performs recovery — PID unchanged, no external restart"
    );
    // The real startup thread recovers against the scratch datadir in the
    // background; its eventual exit stays queued and unreaped here.
}
