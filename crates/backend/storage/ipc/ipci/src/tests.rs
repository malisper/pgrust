use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Once;

use super::*;

static BE_STATUS_INITS: AtomicUsize = AtomicUsize::new(0);
static BE_STATUS_RESETS: AtomicUsize = AtomicUsize::new(0);
static BARRIER_CV_RESETS: AtomicUsize = AtomicUsize::new(0);
static CHECKPOINTER_CV_RESETS: AtomicUsize = AtomicUsize::new(0);

// Recorded on_shmem_exit registry: lets the reset test replay shmem_exit(1)
// (LIFO) so the dsm control segment is torn down before the walk re-creates it.
static SHMEM_EXIT_CBS: std::sync::Mutex<Vec<(fn(i32, usize), usize)>> =
    std::sync::Mutex::new(Vec::new());

const MAX_LIVE_CHILDREN: i32 = 286;

fn install_test_gucs() {
    use std::sync::atomic::{AtomicI32, Ordering::Relaxed};
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

fn bringup() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        shmem::init_seams();
        pg_prng::init_seams();
        ipc_seams::on_shmem_exit::set(|cb, arg| {
            SHMEM_EXIT_CBS.lock().unwrap().push((cb, arg));
        });
        ipc_seams::proc_exit::set(|code, _pid| panic!("proc_exit({code})"));
        xact_seams::is_in_parallel_mode::set(|| false);
        xact_seams::get_current_transaction_nest_level::set(|| 1);
        guc_tables::init_seams();
        // commands_variable owns this accessor in production seams_init;
        // AioShmemSize reads it (this harness inits GUCs piecemeal).
        guc_tables::vars::io_max_combine_limit.install_if_absent(guc_tables::GucVarAccessors {
            get: || 16,
            set: |_| {},
        });
        pgstat::init_seams();
        init_small::init_seams();
        scalar_seams::parse_bool::set(|value| match value {
            "on" | "true" | "yes" | "1" => Some(true),
            "off" | "false" | "no" | "0" => Some(false),
            _ => None,
        });
        aclchk_seams::pg_parameter_aclcheck_set::set(|_, _| Ok(true));
        mbutils_seams::get_database_encoding::set(|| 6);
        pg_sema_seams::pg_semaphore_create::set(|_procno| {});
        pmchild_seams::max_live_postmaster_children::set(|| MAX_LIVE_CHILDREN);
        backend_status_seams::backend_status_shmem_size::set(|| Ok(4096));
        backend_status_seams::backend_status_shmem_init::set(|| {
            BE_STATUS_INITS.fetch_add(1, Ordering::Relaxed);
            Ok(())
        });
        backend_status_seams::backend_status_shmem_reset_after_crash::set(|| {
            BE_STATUS_RESETS.fetch_add(1, Ordering::Relaxed);
        });
        // AsyncShmemInit scans pg_notify/ at boot; no datadir in this test.
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
        condition_variable_seams::proc_signal_barrier_cvs_reset_after_crash::set(|| {
            BARRIER_CV_RESETS.fetch_add(1, Ordering::Relaxed);
        });
        condition_variable_seams::checkpointer_cvs_reset_after_crash::set(|| {
            CHECKPOINTER_CV_RESETS.fetch_add(1, Ordering::Relaxed);
        });
        transam_xlog::init_seams();
        install_test_gucs();
        init_seams();
    });
    guc::store::initialize_guc_options().unwrap();
    // Unseeded prng = xoroshiro zero fixed point; InitProcessGlobals seeds it.
    pg_prng::global_prng(|prng| prng.seed(42));
    g::SetNBuffers(16);
    g::SetMaxConnections(100);
    g::set_max_worker_processes(8);
    g::SetMaxBackends(100 + 16 + 8 + 10 + 2);
}

#[test]
fn create_shared_memory_and_semaphores_end_to_end() {
    bringup();

    ipci_seams::create_shared_memory_and_semaphores::call(4).unwrap();

    assert_eq!(BE_STATUS_INITS.load(Ordering::Relaxed), 1);
    assert_eq!(lmgr_proc::ProcGlobal().allProcs.len() > 0, true);
    pmsignal::MarkPostmasterChildSlotAssigned(1).unwrap();
    assert!(pmsignal::MarkPostmasterChildSlotUnassigned(1));

    ipci_seams::initialize_shmem_gucs::call(4).unwrap();
    let mb = guc::GetConfigOption("shared_memory_size", false, false)
        .unwrap()
        .unwrap();
    assert!(mb.parse::<u64>().unwrap() > 0);
    let semas = guc::GetConfigOption("num_os_semaphores", false, false)
        .unwrap()
        .unwrap();
    assert_eq!(
        semas.parse::<i32>().unwrap(),
        lmgr_proc::ProcGlobalSemas()
    );

    // Crash-cycle reset walk over the same live structures: dirty a probe per
    // reset family, replay shmem_exit(1) (LIFO — tears down the dsm control
    // segment), then assert the boot image is restored.
    varsup::TransamVariables().nextOid.store(777, Ordering::Relaxed);
    let lock0 = lwlock::main_lock(0);
    lock0
        .state
        .store(lwlock::LW_FLAG_RELEASE_OK | 5, Ordering::Relaxed);
    let cbs: Vec<_> = SHMEM_EXIT_CBS.lock().unwrap().drain(..).collect();
    for (cb, arg) in cbs.into_iter().rev() {
        cb(1, arg);
    }

    ResetShmemAfterCrash().unwrap();

    assert_eq!(varsup::TransamVariables().nextOid.load(Ordering::Relaxed), 0);
    assert_eq!(lock0.state.load(Ordering::Relaxed), lwlock::LW_FLAG_RELEASE_OK);
    assert_eq!(BE_STATUS_RESETS.load(Ordering::Relaxed), 1);
    assert_eq!(BARRIER_CV_RESETS.load(Ordering::Relaxed), 1);
    assert_eq!(CHECKPOINTER_CV_RESETS.load(Ordering::Relaxed), 1);
    pmsignal::MarkPostmasterChildSlotAssigned(1).unwrap();
    assert!(pmsignal::MarkPostmasterChildSlotUnassigned(1));
}

#[test]
fn calculate_shmem_size_rounds_and_counts_addin() {
    bringup();
    let cfg = proc_global_config(4);
    let (size, num_semas) = CalculateShmemSize(&cfg).unwrap();
    assert_eq!(size % 8192, 0);
    assert!(size > 100000);
    assert_eq!(num_semas, lmgr_proc::ProcGlobalSemas());

    RequestAddinShmemSpace(64 * 1024, true).unwrap();
    let (with_addin, _) = CalculateShmemSize(&cfg).unwrap();
    assert!(with_addin >= size + 64 * 1024 - 8192);
    assert_eq!(with_addin % 8192, 0);
    TOTAL_ADDIN_REQUEST.set(0);
}

#[test]
fn request_addin_outside_hook_is_fatal() {
    bringup();
    let err = std::panic::catch_unwind(|| RequestAddinShmemSpace(1, false))
        .expect_err("FATAL must not return");
    let msg = err.downcast_ref::<String>().cloned().unwrap_or_default();
    assert!(msg.contains("proc_exit(1)"), "got: {msg}");
}
