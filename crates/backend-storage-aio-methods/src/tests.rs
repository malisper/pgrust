//! Unit tests for the boot-critical AIO init + sync method slice.
//!
//! The seam slots and the live GUC store are process-global, so a single
//! `SERIAL` mutex serializes the tests. The init-small seams are installed once
//! (guarded by `installed()`) and read fixed values via process-global atomics.

use super::*;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Mutex;

static SERIAL: Mutex<()> = Mutex::new(());

static T_MAX_BACKENDS: AtomicI32 = AtomicI32::new(8);
static T_NBUFFERS: AtomicI32 = AtomicI32::new(1024);
static T_MY_PROC_NUMBER: AtomicI32 = AtomicI32::new(0);

/// Install the seams these tests rely on (once), build the live GUC store, and
/// set the per-test backend/buffer counts. Returns the serial guard.
fn setup(max_backends: i32, nbuffers: i32, my_proc_number: i32) -> std::sync::MutexGuard<'static, ()> {
    let guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());

    T_MAX_BACKENDS.store(max_backends, Ordering::SeqCst);
    T_NBUFFERS.store(nbuffers, Ordering::SeqCst);
    T_MY_PROC_NUMBER.store(my_proc_number, Ordering::SeqCst);

    if !backend_utils_init_small_seams::max_backends::is_installed() {
        backend_utils_init_small_seams::max_backends::set(|| T_MAX_BACKENDS.load(Ordering::SeqCst));
    }
    if !backend_utils_init_small_seams::nbuffers::is_installed() {
        backend_utils_init_small_seams::nbuffers::set(|| T_NBUFFERS.load(Ordering::SeqCst));
    }
    if !backend_utils_init_small_seams::my_proc_number::is_installed() {
        backend_utils_init_small_seams::my_proc_number::set(|| T_MY_PROC_NUMBER.load(Ordering::SeqCst));
    }
    if !backend_utils_init_small_seams::my_backend_type::is_installed() {
        backend_utils_init_small_seams::my_backend_type::set(|| types_core::init::BackendType::Backend);
    }
    if !backend_storage_ipc_shmem_seams::add_size::is_installed() {
        backend_storage_ipc_shmem_seams::add_size::set(|a, b| {
            a.checked_add(b).ok_or_else(|| oom_error("add_size"))
        });
    }
    if !backend_storage_ipc_shmem_seams::mul_size::is_installed() {
        backend_storage_ipc_shmem_seams::mul_size::set(|a, b| {
            a.checked_mul(b).ok_or_else(|| oom_error("mul_size"))
        });
    }

    // Build the live GUC store from boot values so get_int/get_enum resolve.
    backend_utils_misc_guc::initialize_guc_options();
    guard
}

/// Directly seed an int GUC's live value, bypassing the full `SetConfigOption`
/// machinery (which pulls in the parallel-mode / ACL seams). The live store is
/// exactly the C `*conf->variable` that `AioShmemInit` reads.
fn seed_int(name: &str, value: i32) {
    use backend_utils_misc_guc::registry::GucVariable;
    backend_utils_misc_guc::live::with_store_mut(|reg| {
        if let Some(GucVariable::Int(c)) = reg.find_option_mut(name) {
            c.value = Some(value);
        } else {
            panic!("int GUC {name} not found");
        }
    })
    .expect("live GUC store initialized");
}

/// Seed the io_method enum GUC's live value.
fn seed_enum(name: &str, value: i32) {
    use backend_utils_misc_guc::registry::GucVariable;
    backend_utils_misc_guc::live::with_store_mut(|reg| {
        if let Some(GucVariable::Enum(c)) = reg.find_option_mut(name) {
            c.value = Some(value);
        } else {
            panic!("enum GUC {name} not found");
        }
    })
    .expect("live GUC store initialized");
}

/// `AioChooseMaxConcurrency` clamps low (>=1) and high (<=64).
#[test]
fn choose_max_concurrency_clamps() {
    {
        let _g = setup(100, 1, 0);
        // 1 buffer / many backends => clamps up to 1.
        assert_eq!(AioChooseMaxConcurrency(), 1);
    }
    {
        let _g = setup(1, 1_000_000_000, 0);
        // huge buffers => clamps down to 64.
        assert_eq!(AioChooseMaxConcurrency(), 64);
    }
    {
        // max_backends=2 => 2 + NUM_AUXILIARY_PROCS aux; 400/(that) middling.
        let _g = setup(2, 400, 0);
        let aux = (2 + NUM_AUXILIARY_PROCS) as i32;
        assert_eq!(AioChooseMaxConcurrency(), core::cmp::min(core::cmp::max(400 / aux, 1), 64));
    }
}

/// The sync IO method: `needs_synchronous_execution` is always true, no shmem
/// contribution, and `submit` reproduces the C `elog(ERROR, ...)`.
#[test]
fn sync_method_ops_shape() {
    let _g = setup(8, 1024, 0);
    let ops = pgaio_sync_ops();
    assert!(!ops.wait_on_fd_before_close);
    assert!(ops.shmem_size.is_none());
    assert!(ops.shmem_init.is_none());
    assert!(ops.init_backend.is_none());
    let h = PgAioHandle::zeroed();
    assert!((ops.needs_synchronous_execution.unwrap())(&h));
    let err = (ops.submit.unwrap())(0).unwrap_err();
    assert!(err.message().contains("executed synchronously"));
}

/// `AioShmemSize` + `AioShmemInit` under `io_method = sync`: sizes the regions
/// and lays out the per-backend idle lists and handle fields exactly.
#[test]
fn shmem_init_builds_layout_under_sync() {
    let _g = setup(3, 1024, 0);
    seed_enum("io_method", IOMETHOD_SYNC);
    // io_max_concurrency boot value is -1; seed a positive value (as the
    // dynamic-default would). AioShmemSize's -1 path is exercised separately.
    seed_int("io_max_concurrency", 4);

    let imc = io_max_concurrency();
    let imcl = io_max_combine_limit();
    assert_eq!(imc, 4);

    let sz = AioShmemSize().expect("AioShmemSize under sync");
    assert!(sz > 0);

    AioShmemInit().expect("AioShmemInit under sync");

    let ctl = pgaio_ctl();
    let aio_procs = (3 + NUM_AUXILIARY_PROCS) as u32;
    assert_eq!(ctl.io_handle_count, aio_procs * imc as u32);
    assert_eq!(ctl.iovec_count, aio_procs * (imc * imcl) as u32);
    assert_eq!(ctl.backend_state.len(), aio_procs as usize);
    assert_eq!(ctl.io_handles.len(), ctl.io_handle_count as usize);
    assert_eq!(ctl.iovecs.lock().unwrap().len(), ctl.iovec_count as usize);
    assert_eq!(ctl.handle_data.lock().unwrap().len(), ctl.iovec_count as usize);

    let mut expected_off = 0u32;
    for (procno, bs_mutex) in ctl.backend_state.iter().enumerate() {
        let bs = bs_mutex.lock().unwrap();
        assert_eq!(bs.io_handle_off, procno as u32 * imc as u32);
        assert_eq!(bs.idle_ios.count, imc as u32);
        assert_eq!(bs.idle_ios.members.len(), imc as usize);
        assert_eq!(bs.in_flight_ios.count, 0);
        assert!(bs.staged_ios.iter().all(Option::is_none));
        for (i, &handle_index) in bs.idle_ios.members.iter().enumerate() {
            assert_eq!(handle_index, bs.io_handle_off as usize + i);
            let ioh = &ctl.io_handles[handle_index];
            assert_eq!(ioh.generation.load(core::sync::atomic::Ordering::Relaxed), 1);
            assert_eq!(ioh.owner_procno, procno as i32);
            let d = ioh.data();
            assert_eq!(d.distilled_result.status, PgAioResultStatus::Unknown);
            assert!(d.report_return.is_none());
            assert!(d.resowner.is_none());
            // iovec_off advances by io_max_combine_limit per handle, globally.
            assert_eq!(ioh.iovec_off, expected_off);
            expected_off += imcl as u32;
        }
    }

    // A second AioShmemInit attaches (found short-circuit), no rebuild/panic.
    AioShmemInit().expect("AioShmemInit attach");
}

/// `pgaio_init_backend` errors for an out-of-range / null PGPROC. The guards
/// return before touching `pgaio_ctl`, so these are independent of whether the
/// (process-global, set-once) control struct was built by another test.
#[test]
fn init_backend_guards() {
    // Null PGPROC (MyProcNumber == INVALID_PROC_NUMBER) => error.
    {
        let _g = setup(2, 1024, types_core::primitive::INVALID_PROC_NUMBER);
        let err = pgaio_init_backend().unwrap_err();
        assert!(err.message().contains("aio requires a normal PGPROC"));
    }
    // Out-of-range MyProcNumber (>= AioProcs()) => error.
    {
        let aio_procs = (2 + NUM_AUXILIARY_PROCS) as i32;
        let _g = setup(2, 1024, aio_procs);
        let err = pgaio_init_backend().unwrap_err();
        assert!(err.message().contains("aio requires a normal PGPROC"));
    }
}
