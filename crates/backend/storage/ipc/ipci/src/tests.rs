use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Once;

use super::*;

static BE_STATUS_INITS: AtomicUsize = AtomicUsize::new(0);

// C boot values: 2*(100+10) + 16 + 8 + 32 io-workers + 10 singletons.
const MAX_LIVE_CHILDREN: i32 = 286;

// Test accessors for GUC slots whose owner units are unported; C boot_vals.
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
        ipc_seams::on_shmem_exit::set(|_cb, _arg| {});
        ipc_seams::proc_exit::set(|code, _pid| panic!("proc_exit({code})"));
        xact_seams::is_in_parallel_mode::set(|| false);
        guc_tables::init_seams();
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
        transam_xlog::init_seams();
        install_test_gucs();
        init_seams();
    });
    guc::store::initialize_guc_options().unwrap();
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

    // Idempotent re-entry is not C behavior; second run must not be needed.
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
    // C elog(FATAL) does not return: it reaches proc_exit (stubbed to panic).
    let err = std::panic::catch_unwind(|| RequestAddinShmemSpace(1, false))
        .expect_err("FATAL must not return");
    let msg = err.downcast_ref::<String>().cloned().unwrap_or_default();
    assert!(msg.contains("proc_exit(1)"), "got: {msg}");
}
