//! The crash-reinit rendering of CreateSharedMemoryAndSemaphores: shared
//! structures are leaked process allocations, so "re-create" is reset in
//! place, per subsystem, in CreateOrAttachShmemStructs order. Sizes are
//! PGC_POSTMASTER-stable across the cycle; the startup process re-seeds
//! contents from pg_control/WAL. Inventory + rules:
//! notes/crash-restart-design.md. Lives here (not ipci) only while the ipci
//! lane is hot; the walk mirrors ipci::CreateOrAttachShmemStructs.

pub(crate) fn reset_shared_memory_after_crash() {
    lwlock::LWLockResetAfterCrash();

    if dsm_core::dsm::dsm_estimate_size() != 0 {
        panic!(
            "crash-restart reinit blocked: dsm main region has no reset surface \
             (min_dynamic_shared_memory > 0; storage-ipc-dsm)"
        );
    }

    varsup::VarsupShmemReset();

    panic!(
        "crash-restart reinit blocked: transam_xlog XLogCtl has no reset surface \
         (then, in init order: clog, subtrans, bufmgr, lock, lmgr_proc, procarray, \
         backend_status, sinval, pmsignal, procsignal, checkpointer, dsm control \
         segment — notes/crash-restart-design.md)"
    );
}
