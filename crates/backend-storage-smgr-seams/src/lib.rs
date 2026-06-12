//! Seam declarations for the `backend-storage-smgr-smgr` unit
//! (`storage/smgr/smgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `ProcessBarrierSmgrRelease()` (smgr.c) — close all smgr file handles
    /// for the PROCSIGNAL_BARRIER_SMGRRELEASE barrier. Returns true when the
    /// barrier was absorbed; an `ereport(ERROR)` from the file layer is the
    /// `Err` (the caller's PG_TRY in `ProcessProcSignalBarrier`).
    pub fn process_barrier_smgr_release() -> types_error::PgResult<bool>
);
