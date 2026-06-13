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
    /// `BufferGetPage(buffer)` with write access (`storage/bufpage.h`): runs
    /// `f` over the buffer's live page bytes (`BLCKSZ`). The owner holds the
    /// buffer pin/content lock across the callback (the caller already holds
    /// the exclusive content lock), so reads and in-place writes both happen
    /// against the shared page — modelling C's direct `Page` pointer without
    /// handing out an aliasable `&'static mut`. The page is mutated in place;
    /// `f`'s `Err` (and any buffer-access `ereport`) propagates.
    pub fn with_buffer_page(
        buffer: types_storage::Buffer,
        f: &mut dyn FnMut(&mut [u8]) -> types_error::PgResult<()>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `MarkBufferDirty(buffer)` (bufmgr.c) — mark the buffer's contents as
    /// dirty. Called inside a critical section; the C path only `Assert`s,
    /// so the seam is infallible.
    pub fn mark_buffer_dirty(buffer: types_storage::Buffer)
);

seam_core::seam!(
    /// `UnlockReleaseBuffer(buffer)` (bufmgr.c) — release the buffer's content
    /// lock and pin. Infallible.
    pub fn unlock_release_buffer(buffer: types_storage::Buffer)
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

// --- backend-utils-init-postinit consumer (bufmgr.c) ---

seam_core::seam!(
    /// `InitBufferManagerAccess()` (bufmgr.c): initialize this backend's local
    /// buffer-manager structures and register its cleanup callback. `Err`
    /// carries its `ereport` surface.
    pub fn init_buffer_manager_access() -> types_error::PgResult<()>
);

// ---------------------------------------------------------------------------
// XLOG-replay buffer primitives consumed by xlogutils.c's redo fetchers.
// The relation Page lives behind the buffer-manager boundary; xlogutils
// crosses it by `Buffer` id rather than exposing a `Page` pointer.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// The buffer-acquisition body of `XLogReadBufferExtended` (xlogutils.c):
    /// the recent-buffer fast path, `smgropen`/`smgrcreate`/`smgrnblocks`, and
    /// the `ReadBufferWithoutRelcache` vs. `ExtendBufferedRelTo` branch — all
    /// of which are bufmgr/smgr operations. Returns the pinned buffer, or
    /// `InvalidBuffer` (0) for the RBM_NORMAL / RBM_NORMAL_NO_LOG missing-page
    /// case (the caller re-applies the in-crate `log_invalid_page`
    /// bookkeeping). `Err` carries the smgr/read `ereport(ERROR)`s.
    pub fn xlog_read_buffer_extended(
        rlocator: types_storage::RelFileLocator,
        forknum: types_core::primitive::ForkNumber,
        blkno: types_core::primitive::BlockNumber,
        mode: types_storage::ReadBufferMode,
        recent_buffer: types_storage::Buffer,
    ) -> types_error::PgResult<types_storage::Buffer>
);

seam_core::seam!(
    /// `PageIsNew(BufferGetPage(buffer))` (bufpage.h) — whether the buffer's
    /// page is all-zeroes (`pd_upper == 0`).
    pub fn page_is_new(buffer: types_storage::Buffer) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `PageSetLSN(BufferGetPage(buffer), lsn)` (bufpage.h) — stamp the page
    /// LSN.
    pub fn page_set_lsn(
        buffer: types_storage::Buffer,
        lsn: types_core::XLogRecPtr,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `PageGetLSN(BufferGetPage(buffer))` (bufpage.h) — the page LSN.
    pub fn page_get_lsn(
        buffer: types_storage::Buffer,
    ) -> types_error::PgResult<types_core::XLogRecPtr>
);

seam_core::seam!(
    /// `FlushOneBuffer(buffer)` (bufmgr.c) — write a single buffer to disk
    /// (used to keep unlogged-relation init forks in sync). `Err` carries the
    /// I/O `ereport(ERROR)`s.
    pub fn flush_one_buffer(buffer: types_storage::Buffer) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE)` (bufmgr.c).
    pub fn lock_buffer_exclusive(buffer: types_storage::Buffer) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `LockBufferForCleanup(buffer)` (bufmgr.c) — acquire a cleanup
    /// (super-exclusive) lock on the buffer.
    pub fn lock_buffer_for_cleanup(buffer: types_storage::Buffer) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `BufferManagerShmemSize()` (ipci.c `CalculateShmemSize` accumulator) — shared-memory
    /// bytes this subsystem needs. `Err` carries the `add_size`/`mul_size`
    /// overflow `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn buffer_manager_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `BufferManagerShmemInit()` (ipci.c `CreateOrAttachShmemStructs`) — allocate-or-attach
    /// this subsystem's shared-memory structures. `Err` carries the C
    /// out-of-shared-memory `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn buffer_manager_shmem_init() -> types_error::PgResult<()>
);
