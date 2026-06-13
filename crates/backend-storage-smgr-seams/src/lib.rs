//! Seam declarations for the `backend-storage-smgr-smgr` unit
//! (`storage/smgr/smgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use types_error::PgResult;
use types_storage::{RelFileLocator, RelFileLocatorBackend};

seam_core::seam!(
    /// `smgrreleaserellocator(rlocator)` (smgr.c) — close (release) the smgr
    /// file handles for one relation file locator, the SMGR arm of
    /// `LocalExecuteInvalidationMessage`. Reachable file-layer
    /// `ereport(ERROR)`s are carried on `Err`.
    pub fn smgr_release_rellocator(rlocator: RelFileLocatorBackend) -> PgResult<()>
);

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

seam_core::seam!(
    /// `smgrexists(smgropen(rlocator, backend), forknum)` (smgr.c): does the
    /// fork's storage exist on disk? `Err` carries the file-layer
    /// `ereport(ERROR)`s reachable under `mdexists` (the implicit `smgropen`
    /// can also OOM-error creating the relation's smgr hash entry).
    pub fn smgrexists(
        rlocator: types_storage::RelFileLocator,
        backend: types_core::primitive::ProcNumber,
        forknum: types_core::primitive::ForkNumber,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `smgrdestroyall()` (smgr.c) — close and destroy all open
    /// `SMgrRelation` objects. Used by `XLogDropDatabase` during replay.
    pub fn smgrdestroyall() -> PgResult<()>
);

// --- backend-utils-init-postinit consumer (smgr.c) ---

seam_core::seam!(
    /// `smgrinit()` (smgr.c): initialize the storage-manager switch and
    /// register its shutdown hook. `Err` carries its `ereport` surface.
    pub fn smgrinit() -> types_error::PgResult<()>
);
