#![allow(non_snake_case)]

use std::cell::Cell;

use init_small::globals as g;
use lmgr_proc::ProcGlobalConfig;
use types_error::{PgResult, DEBUG3, FATAL};
use types_guc::GucContext::PGC_INTERNAL;
use types_guc::GucSource::PGC_S_DYNAMIC_DEFAULT;
use types_storage::{PGShmemHeader, PGShmemMagic};

#[cfg(test)]
mod tests;

// C legs whose owner units are unported are absent from both drivers below;
// each owner allocates its shared structures when it lands, and until then its
// first entry point panics with its own name (no silent stub): dsm_registry,
// xlogprefetcher, xlogrecovery, commit_ts, multixact, twophase, bgworker,
// predicate, checkpointer, autovacuum, slots/origin/walsnd/walrcv/
// walsummarizer/pgarch/launcher/slotsync, nbtree vacuum-cycle, syncscan,
// async, pgstat shmem, custom wait events, injection points, aio. Segment
// mechanics (PGSharedMemoryCreate / InitShmemAccess / InitShmemAllocation /
// InitShmemIndex / PGReserveSemaphores / the shmem-index hash estimate /
// huge pages) have no thread-model counterpart: allocation is per-request
// (see the shmem crate doc) and semaphores are per-PGPROC in InitProcGlobal.
// AttachSharedMemoryStructs (EXEC_BACKEND) is likewise n/a: child threads
// share the address space, the fork()-inheritance arm of the C.

thread_local! {
    static TOTAL_ADDIN_REQUEST: Cell<usize> = const { Cell::new(0) };
}

// process_shmem_requests_in_progress (miscinit.c) arrives as a parameter, as
// fabled's port did: no ambient-global getter seams.
pub fn RequestAddinShmemSpace(
    size: usize,
    process_shmem_requests_in_progress: bool,
) -> PgResult<()> {
    if !process_shmem_requests_in_progress {
        elog::elog(
            FATAL,
            "cannot request additional shared memory outside shmem_request_hook",
        )?;
    }
    let total = shmem::add_size(TOTAL_ADDIN_REQUEST.get(), size)?;
    TOTAL_ADDIN_REQUEST.set(total);
    Ok(())
}

fn proc_global_config(fastpath_lock_groups_per_backend: i32) -> ProcGlobalConfig {
    ProcGlobalConfig {
        autovacuum_worker_slots: guc_tables::vars::autovacuum_worker_slots.read(),
        max_wal_senders: guc_tables::vars::max_wal_senders.read(),
        max_prepared_xacts: guc_tables::vars::max_prepared_xacts.read(),
        fastpath_lock_groups_per_backend,
    }
}

/// CalculateShmemSize: `(size, num_semaphores)`; the C out-parameter is always
/// returned. The sum covers the landed subsystems only (header comment), so it
/// undercounts C until the missing owners land — the consumers (the
/// shared_memory_size / num_os_semaphores GUCs) are informational here, with
/// no segment to size.
pub fn CalculateShmemSize(cfg: &ProcGlobalConfig) -> PgResult<(usize, i32)> {
    let num_semas = lmgr_proc::ProcGlobalSemas();

    let mut size: usize = 100000;
    size = shmem::add_size(size, dsm_core::dsm::dsm_estimate_size())?;
    size = shmem::add_size(size, lock::LockManagerShmemSize(cfg.max_prepared_xacts))?;
    size = shmem::add_size(size, lmgr_proc::ProcGlobalShmemSize(cfg)?)?;
    size = shmem::add_size(size, varsup::VarsupShmemSize())?;
    size = shmem::add_size(size, transam_xlog::XLOGShmemSize())?;
    size = shmem::add_size(size, clog::CLOGShmemSize())?;
    size = shmem::add_size(size, subtrans::SUBTRANSShmemSize())?;
    size = shmem::add_size(size, lwlock::LWLockShmemSize()?)?;
    size = shmem::add_size(size, backend_status_seams::backend_status_shmem_size::call()?)?;
    size = shmem::add_size(size, sinval::SharedInvalShmemSize()?)?;
    size = shmem::add_size(
        size,
        pmsignal::PMSignalShmemSize(pmchild_seams::max_live_postmaster_children::call())?,
    )?;
    size = shmem::add_size(size, procsignal::ProcSignalShmemSize()?)?;

    size = shmem::add_size(size, TOTAL_ADDIN_REQUEST.get())?;

    size = shmem::add_size(size, 8192 - (size % 8192))?;

    Ok((size, num_semas))
}

pub fn CreateSharedMemoryAndSemaphores(fastpath_lock_groups_per_backend: i32) -> PgResult<()> {
    debug_assert!(!g::IsUnderPostmaster());

    let cfg = proc_global_config(fastpath_lock_groups_per_backend);
    let (size, _num_semas) = CalculateShmemSize(&cfg)?;
    elog::elog(DEBUG3, format!("invoking IpcMemoryCreate(size={size})"))?;

    CreateOrAttachShmemStructs(&cfg)?;

    // The C shim is the segment header PGSharedMemoryCreate returned;
    // dsm_postmaster_startup stores the control-segment handle in it. With no
    // segment, ipci owns the header for the cluster lifetime.
    let shim = Box::leak(Box::new(PGShmemHeader {
        magic: PGShmemMagic,
        creatorPID: std::process::id() as _,
        totalsize: size,
        freeoffset: 0,
        dsm_control: 0,
        index: std::ptr::null_mut(),
        device: 0,
        inode: 0,
    }));
    dsm_core::dsm::dsm_postmaster_startup(shim)?;

    // shmem_startup_hook: preload libraries are unported; the miscinit
    // process_shared_preload_libraries seam panics before any hook could
    // have been installed.
    Ok(())
}

pub fn CreateOrAttachShmemStructs(cfg: &ProcGlobalConfig) -> PgResult<()> {
    lwlock::CreateLWLocks(g::IsUnderPostmaster())?;

    dsm_core::dsm::dsm_shmem_init()?;

    varsup::VarsupShmemInit();
    transam_xlog::XLOGShmemInit();
    clog::CLOGShmemInit()?;
    subtrans::SUBTRANSShmemInit()?;
    bufmgr::BufferManagerShmemInit()?;

    lock::LockManagerShmemInit(cfg.max_prepared_xacts)?;

    if !g::IsUnderPostmaster() {
        lmgr_proc::InitProcGlobal(cfg);
    }
    procarray::ProcArrayShmemInit();
    backend_status_seams::backend_status_shmem_init::call()?;

    sinval::SharedInvalShmemInit()?;

    pmsignal::PMSignalShmemInit(pmchild_seams::max_live_postmaster_children::call());
    procsignal::ProcSignalShmemInit();

    Ok(())
}

pub fn InitializeShmemGUCs(fastpath_lock_groups_per_backend: i32) -> PgResult<()> {
    let cfg = proc_global_config(fastpath_lock_groups_per_backend);
    let (size_b, num_semas) = CalculateShmemSize(&cfg)?;
    let size_mb = shmem::add_size(size_b, (1024 * 1024) - 1)? / (1024 * 1024);
    guc::SetConfigOption(
        "shared_memory_size",
        Some(&size_mb.to_string()),
        PGC_INTERNAL,
        PGC_S_DYNAMIC_DEFAULT,
    )?;

    // GetHugePageSize: no segment, no huge pages; the GUC keeps its -1
    // "not supported" boot value.

    guc::SetConfigOption(
        "num_os_semaphores",
        Some(&num_semas.to_string()),
        PGC_INTERNAL,
        PGC_S_DYNAMIC_DEFAULT,
    )?;
    Ok(())
}

pub fn init_seams() {
    ipci_seams::create_shared_memory_and_semaphores::set(CreateSharedMemoryAndSemaphores);
    ipci_seams::initialize_shmem_gucs::set(InitializeShmemGUCs);
}
