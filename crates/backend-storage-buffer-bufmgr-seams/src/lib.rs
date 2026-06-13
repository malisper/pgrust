//! Seam declarations for the `backend-storage-buffer-bufmgr` unit
//! (`storage/buffer/bufmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! The `effective_io_concurrency` / `maintenance_io_concurrency` GUC globals
//! deliberately have no getter seams: per the no-ambient-global-seams rule,
//! consumers take the values as explicit parameters.

seam_core::seam!(
    /// `RelationGetNumberOfBlocksInFork(relation, forkNum)` (bufmgr.c): the
    /// current number of blocks in the relation fork (`smgrnblocks` under
    /// the covers — the `RelationGetNumberOfBlocks` macro is the
    /// `MAIN_FORKNUM` case). `Err` carries the smgr `ereport(ERROR)`s.
    pub fn relation_get_number_of_blocks_in_fork(
        relation: types_core::primitive::Oid,
        fork_num: types_core::primitive::ForkNumber,
    ) -> types_error::PgResult<types_core::primitive::BlockNumber>
);

seam_core::seam!(
    /// `HoldingBufferPinThatDelaysRecovery()` — does this backend hold the
    /// buffer pin the Startup process is waiting for?
    pub fn holding_buffer_pin_that_delays_recovery() -> bool
);

seam_core::seam!(
    /// `AtEOXact_Buffers(isCommit)` — sanity-check that all buffer pins were
    /// released (Assert-only in production builds).
    pub fn at_eoxact_buffers(is_commit: bool)
);

seam_core::seam!(
    /// `UnlockBuffers()` — release buffer content locks on the abort path.
    pub fn unlock_buffers()
);

seam_core::seam!(
    /// `PrefetchSharedBuffer(smgropen(rlocator, backend), forkNum, blockNum)`
    /// (bufmgr.c): initiate (or note as unnecessary) a prefetch of a shared
    /// buffer. The C function takes the `SMgrRelation` handle; smgropen is
    /// cached and cheap, so the seam takes the locator + backend pair like
    /// the flattened smgr seams. `Err` carries the buffer-table /
    /// `smgrprefetch` `ereport(ERROR)`s.
    pub fn prefetch_shared_buffer(
        rlocator: types_storage::RelFileLocator,
        backend: types_core::primitive::ProcNumber,
        fork_num: types_core::primitive::ForkNumber,
        block_num: types_core::primitive::BlockNumber,
    ) -> types_error::PgResult<types_storage::PrefetchBufferResult>
);

seam_core::seam!(
    /// `BufferGetBlockNumber(buffer)` (bufmgr.c): the block number the buffer
    /// currently holds. Pure read of a valid pinned buffer.
    pub fn buffer_get_block_number(
        buf: types_storage::storage::Buffer,
    ) -> types_core::primitive::BlockNumber
);

seam_core::seam!(
    /// `BufferGetPage(buffer)` (bufmgr.h): a snapshot copy of the buffer's
    /// page image in `mcx` (the consumer reads page-format fields off it).
    /// `Err` carries OOM.
    pub fn buffer_get_page<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        buf: types_storage::storage::Buffer,
    ) -> types_error::PgResult<mcx::PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `ReleaseBuffer(buffer)` (bufmgr.c): drop one pin on a buffer.
    pub fn release_buffer(buf: types_storage::storage::Buffer)
);

seam_core::seam!(
    /// `IncrBufferRefCount(buffer)` (bufmgr.c): bump the local pin count on a
    /// buffer the backend already has pinned.
    pub fn incr_buffer_ref_count(buf: types_storage::storage::Buffer)
);

seam_core::seam!(
    /// `MarkBufferDirtyHint(buffer, buffer_std = false)` (bufmgr.c): mark a
    /// buffer dirty for a non-WAL-logged hint-bit-style change (the nbtree
    /// cycle-id clear passes `buffer_std = false`).
    pub fn mark_buffer_dirty_hint(buf: types_storage::storage::Buffer)
);

seam_core::seam!(
    /// `ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, strategy)`
    /// (bufmgr.c): pin (reading in if needed) a block, using the VACUUM
    /// buffer-access strategy. `Err` carries the smgr read ereports.
    pub fn read_buffer_extended<'mcx>(
        rel: &types_rel::Relation<'mcx>,
        blkno: types_core::primitive::BlockNumber,
    ) -> types_error::PgResult<types_storage::storage::Buffer>
);
