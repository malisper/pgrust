//! aio_init.c: shmem sizing/creation, backend attach, crash-cycle reset.

use std::sync::atomic::Ordering;

use init_small::globals as g;
use types_error::PgResult;
use types_storage::aio::{PgAioResult, PgAioResultStatus, PGAIO_OP_INVALID, PGAIO_TID_INVALID};
use types_storage::storage::NUM_AUXILIARY_PROCS;

use crate::{
    backend_slot, dclist_push_tail, ioh, AioCell, BackendData, Dclist, HandleData, ListNode,
    PgAioBackend, PgAioHandle, BACKENDS, BACKEND_COUNT, HANDLES, HANDLE_COUNT, HANDLE_DATA,
    IOVECS, MY_BACKEND, NO_HANDLE, PGAIO_HS_IDLE,
};

fn AioProcs() -> usize {
    // C: a ProcNumber can land on an IO worker, so the table covers
    // every PGPROC-owning slot.
    (g::MaxBackends() + NUM_AUXILIARY_PROCS) as usize
}

fn AioChooseMaxConcurrency() -> i32 {
    let max_backends = g::MaxBackends() + NUM_AUXILIARY_PROCS;
    let max_proportional_pins = g::NBuffers() / max_backends;
    max_proportional_pins.clamp(1, 64)
}

pub fn AioShmemSize() -> PgResult<usize> {
    // C resolves io_max_concurrency=-1 via SetConfigOption(PGC_S_DYNAMIC_DEFAULT)
    // Divergence: auto-tune stores via the var accessor, no guc-engine
    // round trip (pg_settings source shows "default" either way, as C).
    if crate::io_max_concurrency() == -1 {
        crate::IO_MAX_CONCURRENCY.store(AioChooseMaxConcurrency(), Ordering::Relaxed);
    }

    let procs = AioProcs();
    let imc = crate::io_max_concurrency() as usize;
    let per_backend_iovecs = imc * guc_tables::vars::io_max_combine_limit.read() as usize;

    let mut sz = shmem::add_size(0, std::mem::size_of::<PgAioBackend>() * procs)?;
    sz = shmem::add_size(sz, std::mem::size_of::<PgAioHandle>() * procs * imc)?;
    sz = shmem::add_size(sz, std::mem::size_of::<libc::iovec>() * procs * per_backend_iovecs)?;
    sz = shmem::add_size(sz, std::mem::size_of::<u64>() * procs * per_backend_iovecs)?;
    sz = shmem::add_size(sz, crate::method_worker::pgaio_worker_shmem_size())?;
    Ok(sz)
}

pub fn AioShmemInit() -> PgResult<()> {
    let procs = AioProcs();
    let imc = crate::io_max_concurrency() as usize;
    debug_assert!(imc > 0, "AioShmemSize must run first (io_max_concurrency auto-tune)");
    let combine = guc_tables::vars::io_max_combine_limit.read() as usize;
    let per_backend_iovecs = imc * combine;

    let (backends_raw, found_b) =
        shmem::ShmemInitStruct("AioBackend", std::mem::size_of::<PgAioBackend>() * procs)?;
    let (handles_raw, _) =
        shmem::ShmemInitStruct("AioHandle", std::mem::size_of::<PgAioHandle>() * procs * imc)?;
    let (iovecs_raw, _) = shmem::ShmemInitStruct(
        "AioHandleIOV",
        std::mem::size_of::<libc::iovec>() * procs * per_backend_iovecs,
    )?;
    let (handle_data_raw, _) = shmem::ShmemInitStruct(
        "AioHandleData",
        std::mem::size_of::<u64>() * procs * per_backend_iovecs,
    )?;

    if !found_b {
        let backends = backends_raw.cast::<PgAioBackend>();
        let handles = handles_raw.cast::<PgAioHandle>();
        let iovecs = iovecs_raw.cast::<libc::iovec>();
        let handle_data = handle_data_raw.cast::<u64>();

        let mut io_handle_off: u32 = 0;
        let mut iovec_off: u32 = 0;
        for procno in 0..procs {
            // SAFETY: fresh in-bounds allocation, placement-init each slot.
            unsafe {
                backends.add(procno).write(PgAioBackend {
                    io_handle_off,
                    b: AioCell::new(BackendData {
                        idle_ios: Dclist::new(),
                        in_flight_ios: Dclist::new(),
                        handed_out_io: NO_HANDLE,
                        in_batchmode: false,
                        num_staged_ios: 0,
                        staged_ios: [NO_HANDLE; types_storage::aio::PGAIO_SUBMIT_BATCH_SIZE],
                    }),
                });
            }
            for i in 0..imc {
                // SAFETY: as above.
                unsafe {
                    handles.add(io_handle_off as usize + i).write(PgAioHandle {
                        state: std::sync::atomic::AtomicU8::new(PGAIO_HS_IDLE),
                        flags: std::sync::atomic::AtomicU8::new(0),
                        owner_procno: procno as i32,
                        iovec_off,
                        generation: std::sync::atomic::AtomicU64::new(1),
                        result: std::sync::atomic::AtomicI32::new(0),
                        cv: condition_variable::ConditionVariable::new(),
                        d: AioCell::new(HandleData {
                            target: PGAIO_TID_INVALID,
                            op: PGAIO_OP_INVALID,
                            num_callbacks: 0,
                            callbacks: [0; 4],
                            callbacks_data: [0; 4],
                            handle_data_len: 0,
                            resowner: None,
                            report_return: std::ptr::null_mut(),
                            distilled_result: PgAioResult {
                                status: PgAioResultStatus::Unknown,
                                ..Default::default()
                            },
                            op_data: Default::default(),
                            target_data: Default::default(),
                        }),
                        node: AioCell::new(ListNode { prev: NO_HANDLE, next: NO_HANDLE }),
                    });
                }
                iovec_off += combine as u32;
            }
            io_handle_off += imc as u32;
        }

        // SAFETY: zero-fill of POD regions the kernel handed us.
        unsafe {
            std::ptr::write_bytes(iovecs, 0, procs * per_backend_iovecs);
            std::ptr::write_bytes(handle_data, 0, procs * per_backend_iovecs);
        }

        HANDLES.store(handles, Ordering::Relaxed);
        HANDLE_COUNT.store((procs * imc) as i64, Ordering::Relaxed);
        BACKENDS.store(backends, Ordering::Relaxed);
        BACKEND_COUNT.store(procs as i64, Ordering::Relaxed);
        IOVECS.store(iovecs, Ordering::Relaxed);
        HANDLE_DATA.store(handle_data, Ordering::Relaxed);

        for procno in 0..procs {
            let slot = backend_slot(procno as i32);
            // SAFETY: single-threaded boot.
            let b = unsafe { &mut *slot.b.get() };
            for i in 0..imc {
                dclist_push_tail(&mut b.idle_ios, slot.io_handle_off + i as u32);
            }
        }
    }

    crate::method_worker::pgaio_worker_shmem_init(!found_b)?;
    Ok(())
}

/// Crash-cycle in-place reset (ipci ResetShmemAfterCrash walk arm): all
pub fn AioShmemResetAfterCrash() -> PgResult<()> {
    if HANDLES.load(Ordering::Relaxed).is_null() {
        return Ok(());
    }
    let procs = BACKEND_COUNT.load(Ordering::Relaxed) as usize;
    // Table geometry from the STORED counts, never the live GUC: per-child
    // base-snapshot stamping can rewrite io_max_concurrency (=-1) after the
    // boot-time auto-tune (fleet crash-smoke finding, job -47a8).
    let imc = crate::handle_count() / procs;

    for procno in 0..procs {
        let slot = backend_slot(procno as i32);
        // SAFETY: crash reset is single-threaded (postmaster, children dead).
        let b = unsafe { &mut *slot.b.get() };
        b.idle_ios = Dclist::new();
        b.in_flight_ios = Dclist::new();
        b.handed_out_io = NO_HANDLE;
        b.in_batchmode = false;
        b.num_staged_ios = 0;

        for i in 0..imc {
            let index = slot.io_handle_off + i as u32;
            let h = ioh(index);
            // Wakeup lists live in PGPROC cvWaitLinks which ProcGlobalReset
            condition_variable::cv_reset_after_crash(&h.cv);
            h.flags.store(0, Ordering::Relaxed);
            h.result.store(0, Ordering::Relaxed);
            h.generation.fetch_add(1, Ordering::Relaxed);
            h.set_state(PGAIO_HS_IDLE);
            // SAFETY: single-threaded crash reset.
            let d = unsafe { h.data() };
            d.target = PGAIO_TID_INVALID;
            d.op = PGAIO_OP_INVALID;
            d.num_callbacks = 0;
            d.handle_data_len = 0;
            d.resowner = None;
            d.report_return = std::ptr::null_mut();
            d.distilled_result =
                PgAioResult { status: PgAioResultStatus::Unknown, ..Default::default() };
            dclist_push_tail(&mut b.idle_ios, index);
        }
    }

    crate::method_worker::pgaio_worker_shmem_reset_after_crash();
    Ok(())
}

pub fn pgaio_init_backend() {
    debug_assert!(MY_BACKEND.get().is_none());

    if miscinit::GetMyBackendType() == types_core::BackendType::IoWorker {
        return;
    }

    if lmgr_proc::MyProc().is_none() || g::MyProcNumber() as usize >= AioProcs() {
        panic!("aio requires a normal PGPROC");
    }

    MY_BACKEND.set(Some(g::MyProcNumber()));

    ipc_seams::before_shmem_exit::call(crate::handle::pgaio_shutdown, datum::Datum::from_usize(0))
        .expect("pgaio_init_backend: before_shmem_exit");
}
