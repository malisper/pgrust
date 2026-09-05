//! aio_init.c: shmem sizing/creation, backend attach, crash-cycle reset.

use std::sync::atomic::Ordering;

use elog::ereport;
use init_small::globals as g;
use types_error::{PgResult, ERROR};
use types_guc::{GucSource, PGC_POSTMASTER};
use types_storage::aio::{PgAioResult, PgAioResultStatus, PGAIO_OP_INVALID, PGAIO_TID_INVALID};
use types_storage::storage::NUM_AUXILIARY_PROCS;

use crate::handle::loc;
use crate::{
    backend_slot, dclist_push_tail, ioh, new_backend, new_handle, pgaio_ctl_opt, publish_ctl,
    Dclist, PgAioBackend, PgAioCtl, PgAioHandle, MY_BACKEND, NO_HANDLE, PGAIO_HS_IDLE,
};

fn AioCtlShmemSize() -> usize {
    // pgaio_ctl itself
    std::mem::size_of::<PgAioCtl>()
}

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
    // aio_init.c:117-133: resolve io_max_concurrency=-1 through the GUC engine
    // at PGC_S_DYNAMIC_DEFAULT so the record itself (SHOW, pg_settings
    // setting/reset_val, every child's bring-up copy) carries the tuned
    // value, not just this crate's accessor. If the DBA explicitly set
    // io_max_concurrency = -1 in the config file, PGC_S_DYNAMIC_DEFAULT
    // fails to override that and we must force the matter with
    // PGC_S_OVERRIDE.
    if crate::io_max_concurrency() == -1 {
        let buf = AioChooseMaxConcurrency().to_string();
        if guc::store::is_initialized() {
            guc::SetConfigOption(
                "io_max_concurrency",
                Some(&buf),
                PGC_POSTMASTER,
                GucSource::PGC_S_DYNAMIC_DEFAULT,
            )?;
            if crate::io_max_concurrency() == -1 {
                // failed to apply it?
                guc::SetConfigOption(
                    "io_max_concurrency",
                    Some(&buf),
                    PGC_POSTMASTER,
                    GucSource::PGC_S_OVERRIDE,
                )?;
            }
        } else {
            // Unit harnesses bring the AIO tables up without a GUC store
            // (no InitializeGUCOptions); the postmaster always has one.
            crate::IO_MAX_CONCURRENCY.store(AioChooseMaxConcurrency(), Ordering::Relaxed);
        }
    }

    let procs = AioProcs();
    let imc = crate::io_max_concurrency() as usize;
    let per_backend_iovecs = imc * guc_tables::vars::io_max_combine_limit.read() as usize;

    let mut sz = shmem::add_size(0, AioCtlShmemSize())?;
    sz = shmem::add_size(sz, std::mem::size_of::<PgAioBackend>() * procs)?;
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

    // aio_init.c:157: the control block is its own shmem-index entry.
    let (ctl_raw, found) = shmem::ShmemInitStruct("AioCtl", AioCtlShmemSize())?;
    let ctl = ctl_raw.cast::<PgAioCtl>();

    if !found {
        let (backends_raw, _) =
            shmem::ShmemInitStruct("AioBackend", std::mem::size_of::<PgAioBackend>() * procs)?;
        let (handles_raw, _) = shmem::ShmemInitStruct(
            "AioHandle",
            std::mem::size_of::<PgAioHandle>() * procs * imc,
        )?;
        let (iovecs_raw, _) = shmem::ShmemInitStruct(
            "AioHandleIOV",
            std::mem::size_of::<libc::iovec>() * procs * per_backend_iovecs,
        )?;
        let (handle_data_raw, _) = shmem::ShmemInitStruct(
            "AioHandleData",
            std::mem::size_of::<u64>() * procs * per_backend_iovecs,
        )?;

        let backends = backends_raw.cast::<PgAioBackend>();
        let handles = handles_raw.cast::<PgAioHandle>();
        let iovecs = iovecs_raw.cast::<libc::iovec>();
        let handle_data = handle_data_raw.cast::<u64>();

        let mut io_handle_off: u32 = 0;
        let mut iovec_off: u32 = 0;
        for procno in 0..procs {
            // SAFETY: fresh in-bounds allocation, placement-init each slot.
            unsafe {
                backends.add(procno).write(new_backend(io_handle_off));
            }
            for i in 0..imc {
                // SAFETY: as above.
                unsafe {
                    handles
                        .add(io_handle_off as usize + i)
                        .write(new_handle(procno as i32, iovec_off));
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

        // SAFETY: fresh in-bounds allocation; published (Release) below
        // before any other thread can reach it.
        unsafe {
            ctl.write(PgAioCtl {
                io_handle_count: (procs * imc) as u64,
                iovec_count: (procs * per_backend_iovecs) as u64,
                backend_count: procs as u64,
                backend_state: backends,
                io_handles: handles,
                iovecs,
                handle_data,
            });
        }
        publish_ctl(ctl);

        for procno in 0..procs {
            let slot = backend_slot(procno as i32);
            // SAFETY: single-threaded boot.
            let b = unsafe { &mut *slot.b.get() };
            for i in 0..imc {
                dclist_push_tail(&mut b.idle_ios, slot.io_handle_off + i as u32);
            }
        }
    } else {
        publish_ctl(ctl);
    }

    crate::method_worker::pgaio_worker_shmem_init(!found)?;
    Ok(())
}

/// Crash-cycle in-place reset (ipci ResetShmemAfterCrash walk arm): all
pub fn AioShmemResetAfterCrash() -> PgResult<()> {
    if pgaio_ctl_opt().is_none() {
        return Ok(());
    }
    let procs = crate::backend_count();
    // Table geometry from the STORED counts, never the live GUC: per-child
    // base-snapshot stamping can rewrite io_max_concurrency (=-1) after the
    // boot-time auto-tune (CI cluster crash-smoke finding, job -47a8).
    let imc = crate::io_handles_per_backend();

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

pub fn pgaio_init_backend() -> PgResult<()> {
    debug_assert!(MY_BACKEND.get().is_none());

    if miscinit::GetMyBackendType() == types_core::BackendType::IoWorker {
        return Ok(());
    }

    if lmgr_proc::MyProc().is_none() || g::MyProcNumber() as usize >= AioProcs() {
        // aio_init.c:227 elog(ERROR): a catchable XX000, not a thread panic.
        ereport(ERROR)
            .errmsg_internal("aio requires a normal PGPROC")
            .finish(loc("pgaio_init_backend"))?;
    }

    MY_BACKEND.set(Some(g::MyProcNumber()));

    ipc_seams::before_shmem_exit::call(crate::handle::pgaio_shutdown, datum::Datum::from_usize(0))
}
