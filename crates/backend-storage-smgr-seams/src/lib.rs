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
    /// `smgropen(rlocator, backend)` (smgr.c) — ensure the SMgrRelation cache
    /// entry for this relation file locator exists in THIS backend's smgr cache
    /// (idempotent). The localbuf temp-flush path (`FlushLocalBuffer(bufHdr,
    /// NULL)` = `smgropen(BufTagGetRelFileLocator(&tag), MyProcNumber)`) needs
    /// the entry before `smgrwrite`, mirroring bufmgr's defensive `smgropen`
    /// before a `FlushBuffer` write. `Err` carries the file-layer
    /// `ereport(ERROR)`s.
    pub fn smgr_open(
        rlocator: RelFileLocator,
        backend: types_core::primitive::ProcNumber,
    ) -> PgResult<()>
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
    /// `RelationGetSmgr(rel)->smgr_cached_nblocks[forknum]` (smgr.h) — peek the
    /// cached block count for the fork WITHOUT forcing a kernel `lseek`.
    /// Returns `InvalidBlockNumber` when the count is not cached yet. Used by
    /// `fsm_does_block_exist` to avoid an `lseek` when the cached MAIN-fork
    /// size already proves the block exists. Pure read of smgr-owned state.
    pub fn smgr_cached_nblocks(
        rlocator: types_storage::RelFileLocator,
        backend: types_core::primitive::ProcNumber,
        forknum: types_core::primitive::ForkNumber,
    ) -> types_core::primitive::BlockNumber
);

seam_core::seam!(
    /// `smgrgettargblock(smgropen(rlocator, backend))` (smgr.c): the relation's
    /// current insertion target block, or `InvalidBlockNumber` when none is
    /// cached. This is what `RelationGetTargetBlock(rel)` reads off
    /// `rd_smgr->smgr_targblock`; an unopened smgr (C `rd_smgr == NULL`) yields
    /// `InvalidBlockNumber`. Pure read of smgr-owned state.
    pub fn smgrgettargblock(
        rlocator: types_storage::RelFileLocator,
        backend: types_core::primitive::ProcNumber,
    ) -> types_core::primitive::BlockNumber
);

seam_core::seam!(
    /// `smgrsettargblock(smgropen(rlocator, backend), targblock)` (smgr.c):
    /// record the relation's insertion target block hint. This is what
    /// `RelationSetTargetBlock(rel, blkno)` writes through
    /// `RelationGetSmgr(rel)->smgr_targblock`.
    pub fn smgrsettargblock(
        rlocator: types_storage::RelFileLocator,
        backend: types_core::primitive::ProcNumber,
        targblock: types_core::primitive::BlockNumber,
    ) -> types_error::PgResult<()>
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

seam_core::seam!(
    /// `smgrreleaseall()` (smgr.c) — release the OS resources (open file
    /// handles) used by *all* open `SMgrRelation` objects without destroying
    /// the objects themselves (pointers to them stay in active use). The
    /// relcache SI-overflow reset (`RelationCacheInvalidate`) calls this to
    /// close every relation's FDs. `void` in C; the file-layer close path does
    /// not `ereport(ERROR)` (failures are FATAL/LOG), so this is void here too.
    pub fn smgrreleaseall()
);

seam_core::seam!(
    /// `RelationCloseSmgr(relation)` (rel.h inline) — close the relation's smgr
    /// handle (`smgrunpin` + `smgrclose`, clearing `rd_smgr`). The owned
    /// relcache mirror carries no `rd_smgr` field, so the relcache caller
    /// routes the relation's `RelFileLocatorBackend` to its smgr owner. `void`
    /// in C (`smgrclose` is void).
    pub fn relation_close_smgr(rlocator: RelFileLocatorBackend)
);

// --- localbuf.c temp-relation I/O consumers (smgr.c) ---

seam_core::seam!(
    /// `smgrread(smgropen(rlocator, backend), forknum, blocknum, buffer)`
    /// (smgr.c) — read one block of a (temp) relation fork into `dst`
    /// (`BLCKSZ` bytes). `Err` carries the file-layer `ereport(ERROR)`s.
    pub fn smgr_read(
        rlocator: types_storage::RelFileLocator,
        backend: types_core::primitive::ProcNumber,
        forknum: types_core::primitive::ForkNumber,
        blocknum: types_core::primitive::BlockNumber,
        dst: &mut [u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `smgrwrite(smgropen(rlocator, backend), forknum, blocknum, buffer,
    /// false)` (smgr.c) — write one block of a (temp) relation fork from
    /// `src` (`BLCKSZ` bytes). `Err` carries the file-layer `ereport(ERROR)`s.
    pub fn smgr_write(
        rlocator: types_storage::RelFileLocator,
        backend: types_core::primitive::ProcNumber,
        forknum: types_core::primitive::ForkNumber,
        blocknum: types_core::primitive::BlockNumber,
        src: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `smgrzeroextend(smgropen(rlocator, backend), forknum, blocknum,
    /// nblocks, skipFsync)` (smgr.c) — extend a (temp) relation fork by
    /// `nblocks` all-zero blocks starting at `blocknum`. `Err` carries the
    /// file-layer `ereport(ERROR)`s.
    pub fn smgr_zeroextend(
        rlocator: types_storage::RelFileLocator,
        backend: types_core::primitive::ProcNumber,
        forknum: types_core::primitive::ForkNumber,
        blocknum: types_core::primitive::BlockNumber,
        nblocks: u32,
        skip_fsync: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `smgrprefetch(smgropen(rlocator, backend), forknum, blocknum, 1)`
    /// (smgr.c) — initiate an async read of one block; returns whether the
    /// prefetch facility is available. `Err` carries the file-layer
    /// `ereport(ERROR)`s.
    pub fn smgr_prefetch(
        rlocator: types_storage::RelFileLocator,
        backend: types_core::primitive::ProcNumber,
        forknum: types_core::primitive::ForkNumber,
        blocknum: types_core::primitive::BlockNumber,
    ) -> types_error::PgResult<bool>
);

// --- backend-utils-init-postinit consumer (smgr.c) ---

seam_core::seam!(
    /// `smgrinit()` (smgr.c): initialize the storage-manager switch and
    /// register its shutdown hook. `Err` carries its `ereport` surface.
    pub fn smgrinit() -> types_error::PgResult<()>
);
