//! Seam declarations for the `backend-storage-smgr-smgr` unit
//! (`storage/smgr/smgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use types_error::PgResult;
use types_storage::RelFileLocator;

seam_core::seam!(
    /// `ProcessBarrierSmgrRelease()` (smgr.c) — close all smgr file handles
    /// for the PROCSIGNAL_BARRIER_SMGRRELEASE barrier. Returns true when the
    /// barrier was absorbed; an `ereport(ERROR)` from the file layer is the
    /// `Err` (the caller's PG_TRY in `ProcessProcSignalBarrier`).
    pub fn process_barrier_smgr_release() -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `smgrnblocks(smgropen(rlocator, backend), forknum)` (smgr.c): the
    /// number of blocks in the fork. `Err` carries the C `ereport(ERROR)`s
    /// from the underlying `mdnblocks` (e.g. `could not seek to end of file`).
    pub fn smgrnblocks(
        rlocator: types_storage::RelFileLocator,
        backend: types_core::primitive::ProcNumber,
        forknum: types_core::primitive::ForkNumber,
    ) -> types_error::PgResult<types_core::primitive::BlockNumber>
);

seam_core::seam!(
    /// `AtEOXact_SMgr()` — close transient SMgrRelation objects.
    pub fn at_eoxact_smgr()
);

seam_core::seam!(
    /// `DropRelationFiles(delrels, ndelrels, isRedo)` (md.c) — physically drop
    /// relation files during replay/commit application.
    pub fn drop_relation_files(delrels: &[RelFileLocator], is_redo: bool) -> PgResult<()>
);
