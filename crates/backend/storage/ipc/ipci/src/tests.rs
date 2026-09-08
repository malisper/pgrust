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
        // GetHugePageSize reads /proc/meminfo through AllocateFile (fd desc
        // table keyed by the current subxact).
        xact_seams::get_current_sub_transaction_id::set(|| 1);
        guc_tables::init_seams();
        // commands_variable owns this accessor in production seams_init;
        // AioShmemSize reads it (this harness inits GUCs piecemeal).
        guc_tables::vars::io_max_combine_limit.install_if_absent(guc_tables::GucVarAccessors {
            get: || 16,
            set: |_| {},
        });
        // AioShmemSize resolves io_max_concurrency=-1 through
        // SetConfigOption(PGC_S_DYNAMIC_DEFAULT) (aio_init.c:117-133); the
        // GUC engine only stores into installed variables, so the AIO
        // accessors must be in place before the store is brought up (the
        // postmaster installs them via seams_init).
        aio_core::init_seams();
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
    // PGSharedMemoryCreate stats DataDir and walks the System V key space
    // from its inode (sysv_shmem.c:716, :764): give it a real directory.
    let dir = std::env::temp_dir().join(format!("pgrust-ipci-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    init_small::globals::SetDataDir(dir.to_str().unwrap());
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

// ipci.c:377-388: InitializeShmemGUCs asks GetHugePageSize (sysv_shmem.c:479)
// for the huge page size and, whenever it is non-zero, sets
// shared_memory_size_in_huge_pages = size_b / hp_size + 1. Under MAP_HUGETLB
// (Linux) the size is never zero: huge_page_size when set (0 in this harness),
// else /proc/meminfo's "Hugepagesize: N kB", else the 2MB fallback. Without
// MAP_HUGETLB the GUC keeps its -1 boot value.
#[test]
fn initialize_shmem_gucs_counts_huge_pages_like_c() {
    bringup();

    ipci_seams::initialize_shmem_gucs::call(4).unwrap();
    let (size_b, _) = CalculateShmemSize(&proc_global_config(4)).unwrap();
    let got = guc::GetConfigOption("shared_memory_size_in_huge_pages", false, false)
        .unwrap()
        .unwrap();

    let expected = if cfg!(any(target_os = "linux", target_os = "android")) {
        let mut hp_size: usize = 2 * 1024 * 1024;
        if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
            for line in meminfo.lines() {
                let Some(rest) = line.strip_prefix("Hugepagesize:") else { continue };
                let mut fields = rest.split_whitespace();
                if let (Some(n), Some(unit)) = (fields.next(), fields.next()) {
                    if unit.starts_with('k') {
                        if let Ok(kb) = n.parse::<usize>() {
                            hp_size = kb * 1024;
                            break;
                        }
                    }
                }
            }
        }
        (size_b / hp_size + 1).to_string()
    } else {
        "-1".to_string()
    };
    assert_eq!(got, expected, "shared_memory_size_in_huge_pages for size_b={size_b}");
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

// C 18.6 values for the terms CalculateShmemSize (ipci.c:116) used to omit,
// at this harness's configuration (MaxBackends 136 = 100 + 16 + 8 + 10 + 2,
// max_prepared_transactions 0, hot_standby on, max_wal_senders 10): the
// pg_shmem_allocations rows a C 18.6 server reports for the same structs
// ("Proc Array" 580, "KnownAssignedXids" 35360, "KnownAssignedXidsValid"
// 8840, "XLOG Recovery Ctl" 104, "Wal Sender Ctl" 1072, "BTree Vacuum
// State" 1644, "Shared Memory Stats" 315552) and posix_sema.c's
// PGSemaphoreShmemSize for ProcGlobalSemas = 136 + NUM_AUXILIARY_PROCS.
#[test]
fn shmem_size_terms_match_c_18_6_census() {
    bringup();
    assert_eq!(g::MaxBackends(), 136);
    // hot_standby boots on (transam_xlog's GUC install): Proc Array +
    // KnownAssignedXids + KnownAssignedXidsValid.
    assert_eq!(procarray::ProcArrayShmemSize(0).unwrap(), 580 + 35360 + 8840);
    assert_eq!(xlogrecovery::XLogRecoveryShmemSize(), 104);
    assert_eq!(walsender::WalSndShmemSize(10).unwrap(), 1072);
    assert_eq!(nbtree::BTreeShmemSize().unwrap(), 1644);
    assert_eq!(pgstat::shmem::StatsShmemSize().unwrap(), 315552);
    assert_eq!(lmgr_proc::ProcGlobalSemas(), 174);
    assert_eq!(pg_sema::PGSemaphoreShmemSize(174).unwrap(), 174 * 32);
    // BufferManagerShmemSize (buf_init.c:145) at this harness's NBuffers:
    // descriptors + cache-line pad, blocks + I/O-align pad, freelist.c's
    // estimate, I/O CVs + pad, checkpoint sort items.
    let nbuffers = g::NBuffers() as usize;
    let strategy = dynahash::hash_estimate_size((nbuffers + 128) as i64, 24) + 32;
    assert_eq!(
        bufmgr::BufferManagerShmemSize().unwrap(),
        nbuffers * 64 + 128 + 4096 + nbuffers * 8192 + strategy + nbuffers * 16 + 128 + nbuffers * 20
    );
}

#[test]
fn request_addin_outside_hook_is_fatal() {
    bringup();
    let err = std::panic::catch_unwind(|| RequestAddinShmemSpace(1, false))
        .expect_err("FATAL must not return");
    let msg = err.downcast_ref::<String>().cloned().unwrap_or_default();
    assert!(msg.contains("proc_exit(1)"), "got: {msg}");
}
