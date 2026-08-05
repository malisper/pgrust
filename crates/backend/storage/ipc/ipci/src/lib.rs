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

// Unported owners are absent from both drivers (each allocates its shmem when
// it lands; until then its first entry point panics with its own name); the
// full omission list is in CATALOG.tsv. Segment mechanics (PGSharedMemoryCreate
// / InitShmemAccess/Allocation/Index / PGReserveSemaphores / huge pages) and
// AttachSharedMemoryStructs (EXEC_BACKEND) have no thread-model counterpart:
// allocation is per-request (shmem crate doc), semaphores are per-PGPROC.

thread_local! {
    static TOTAL_ADDIN_REQUEST: Cell<usize> = const { Cell::new(0) };
}

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

/// Returns `(size, num_semaphores)`; sums landed subsystems only.
pub fn CalculateShmemSize(cfg: &ProcGlobalConfig) -> PgResult<(usize, i32)> {
    let num_semas = lmgr_proc::ProcGlobalSemas();

    let mut size: usize = 100000;
    size = shmem::add_size(size, dsm_core::dsm::dsm_estimate_size())?;
    size = shmem::add_size(size, dsm_registry::DSMRegistryShmemSize())?;
    size = shmem::add_size(size, lock::LockManagerShmemSize(cfg.max_prepared_xacts))?;
    size = shmem::add_size(size, predicate::PredicateLockShmemSize(cfg.max_prepared_xacts))?;
    size = shmem::add_size(size, lmgr_proc::ProcGlobalShmemSize(cfg)?)?;
    size = shmem::add_size(size, varsup::VarsupShmemSize())?;
    size = shmem::add_size(size, transam_xlog::XLOGShmemSize())?;
    size = shmem::add_size(size, xlogprefetcher::XLogPrefetchShmemSize())?;
    size = shmem::add_size(size, clog::CLOGShmemSize())?;
    size = shmem::add_size(size, commit_ts::CommitTsShmemSize())?;
    size = shmem::add_size(size, subtrans::SUBTRANSShmemSize())?;
    size = shmem::add_size(size, multixact::MultiXactShmemSize())?;
    size = shmem::add_size(size, twophase::TwoPhaseShmemSize())?;
    size = shmem::add_size(size, lwlock::LWLockShmemSize()?)?;
    size = shmem::add_size(size, backend_status_seams::backend_status_shmem_size::call()?)?;
    size = shmem::add_size(size, sinval::SharedInvalShmemSize()?)?;
    size = shmem::add_size(
        size,
        pmsignal::PMSignalShmemSize(pmchild_seams::max_live_postmaster_children::call())?,
    )?;
    size = shmem::add_size(size, procsignal::ProcSignalShmemSize()?)?;
    size = shmem::add_size(size, syncscan::SyncScanShmemSize())?;
    size = shmem::add_size(size, commands_async::AsyncShmemSize())?;
    size = shmem::add_size(size, waitevent::custom::WaitEventCustomShmemSize())?;
    size = shmem::add_size(size, aio_core::AioShmemSize()?)?;
    size = shmem::add_size(size, checkpointer::CheckpointerShmemSize(g::NBuffers()))?;
    size = shmem::add_size(size, slot::ReplicationSlotsShmemSize())?;
    size = shmem::add_size(size, walsummarizer::WalSummarizerShmemSize())?;
    size = shmem::add_size(size, pgarch::PgArchShmemSize())?;

    size = shmem::add_size(size, TOTAL_ADDIN_REQUEST.get())?;

    size = shmem::add_size(size, 8192 - (size % 8192))?;

    Ok((size, num_semas))
}

pub fn CreateSharedMemoryAndSemaphores(fastpath_lock_groups_per_backend: i32) -> PgResult<()> {
    debug_assert!(!g::IsUnderPostmaster());

    let cfg = proc_global_config(fastpath_lock_groups_per_backend);
    let (size, _num_semas) = CalculateShmemSize(&cfg)?;
    elog::elog(DEBUG3, format!("invoking IpcMemoryCreate(size={size})"))?;

    // C reports this from PGSharedMemoryCreate; thread-shared state never
    // mmaps with MAP_HUGETLB, so huge pages are always off (never "unknown").
    guc::SetConfigOption("huge_pages_status", Some("off"), PGC_INTERNAL, PGC_S_DYNAMIC_DEFAULT)?;

    CreateOrAttachShmemStructs(&cfg)?;

    // The C shim is PGSharedMemoryCreate's segment header; ipci owns it here.
    let shim = Box::leak(Box::new(PGShmemHeader {
        magic: PGShmemMagic,
        creatorPID: init_small::globals::process_id() as _,
        totalsize: size,
        freeoffset: 0,
        dsm_control: 0,
        index: std::ptr::null_mut(),
        device: 0,
        inode: 0,
    }));
    dsm_core::dsm::dsm_postmaster_startup(shim)?;

    // shmem_startup_hook: none can exist, preload libraries are unported.
    Ok(())
}

pub fn CreateOrAttachShmemStructs(cfg: &ProcGlobalConfig) -> PgResult<()> {
    lwlock::CreateLWLocks(g::IsUnderPostmaster())?;

    dsm_core::dsm::dsm_shmem_init()?;
    dsm_registry::DSMRegistryShmemInit();

    varsup::VarsupShmemInit();
    transam_xlog::XLOGShmemInit();
    xlogprefetcher::XLogPrefetchShmemInit();
    clog::CLOGShmemInit()?;
    commit_ts::CommitTsShmemInit()?;
    subtrans::SUBTRANSShmemInit()?;
    multixact::MultiXactShmemInit()?;
    bufmgr::BufferManagerShmemInit()?;

    lock::LockManagerShmemInit(cfg.max_prepared_xacts)?;
    predicate::PredicateLockShmemInit(cfg.max_prepared_xacts)?;

    if !g::IsUnderPostmaster() {
        lmgr_proc::InitProcGlobal(cfg);
    }
    procarray::ProcArrayShmemInit();
    backend_status_seams::backend_status_shmem_init::call()?;
    twophase::TwoPhaseShmemInit();

    sinval::SharedInvalShmemInit()?;

    pmsignal::PMSignalShmemInit(pmchild_seams::max_live_postmaster_children::call());
    procsignal::ProcSignalShmemInit();
    checkpointer::CheckpointerShmemInit(g::NBuffers());
    slot::ReplicationSlotsShmemInit();
    origin::ReplicationOriginShmemInit();
    walsummarizer::WalSummarizerShmemInit();
    walreceiverfuncs::WalRcvShmemInit();
    pgarch::PgArchShmemInit();
    syncscan::SyncScanShmemInit();
    commands_async::AsyncShmemInit()?;
    waitevent::custom::WaitEventCustomShmemInit()?;
    aio_core::AioShmemInit()?;

    Ok(())
}

/// Crash-cycle CreateSharedMemoryAndSemaphores: in-place resets to the
/// post-ShmemInit boot image, CreateOrAttachShmemStructs order. Caller:
/// postmaster thread, children dead, after `ipc::shmem_exit(1)`
/// (notes/crash-restart-design.md).
pub fn ResetShmemAfterCrash() -> PgResult<()> {
    lwlock::LWLockResetAfterCrash();

    if dsm_core::dsm::dsm_estimate_size() != 0 {
        panic!(
            "crash-restart reinit blocked: dsm main region has no reset surface \
             (min_dynamic_shared_memory > 0; storage-ipc-dsm)"
        );
    }

    dsm_registry::DSMRegistryShmemResetAfterCrash();

    varsup::VarsupShmemReset();
    transam_xlog::XLOGShmemResetAfterCrash();
    xlogprefetcher::XLogPrefetchShmemResetAfterCrash();
    clog::CLOGShmemResetAfterCrash();
    commit_ts::CommitTsShmemResetAfterCrash();
    subtrans::SUBTRANSShmemResetAfterCrash();
    multixact::MultiXactShmemResetAfterCrash();
    bufmgr::BufferManagerShmemResetAfterCrash();

    lock::LockManagerShmemResetAfterCrash();
    predicate::PredicateLockShmemResetAfterCrash();

    lmgr_proc::ProcGlobalResetAfterCrash();
    procarray::ProcArrayShmemResetAfterCrash();
    backend_status_seams::backend_status_shmem_reset_after_crash::call();
    twophase::TwoPhaseStateResetAfterCrash();

    sinval::SharedInvalShmemResetAfterCrash();

    pmsignal::PMSignalShmemResetAfterCrash();
    procsignal::ProcSignalShmemResetAfterCrash();
    checkpointer::CheckpointerShmemResetAfterCrash();
    slot::ReplicationSlotsShmemResetAfterCrash();
    origin::ReplicationOriginShmemResetAfterCrash();
    walsummarizer::WalSummarizerShmemResetAfterCrash();
    walreceiverfuncs::WalRcvShmemResetAfterCrash();
    pgarch::PgArchShmemResetAfterCrash();
    syncscan::SyncScanShmemResetAfterCrash();
    commands_async::AsyncShmemResetAfterCrash()?;
    waitevent::custom::WaitEventCustomShmemResetAfterCrash();
    aio_core::AioShmemResetAfterCrash()?;

    dsm_core::dsm::dsm_postmaster_startup_after_crash()
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

    // No segment, no huge pages: that GUC keeps its -1 boot value.

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
