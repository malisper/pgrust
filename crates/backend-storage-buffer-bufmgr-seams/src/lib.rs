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
    /// `MarkBufferDirtyHint(buffer, buffer_std)` (bufmgr.c): mark a buffer
    /// dirty for a non-WAL-logged hint-bit-style change. `buffer_std` is true
    /// for standard page-layout buffers (the heap-visibility hint-bit path) and
    /// false otherwise (e.g. the nbtree cycle-id clear, freespace map).
    pub fn mark_buffer_dirty_hint(buf: types_storage::storage::Buffer, buffer_std: bool)
);

seam_core::seam!(
    /// `BufferIsPermanent(buffer)` (bufmgr.c): is the buffer's relation
    /// WAL-logged (permanent), so hint-bit changes need LSN-interlock care?
    pub fn buffer_is_permanent(buf: types_storage::storage::Buffer) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `BufferGetLSNAtomic(buffer)` (bufmgr.c): atomically read the page LSN of
    /// a pinned buffer (takes the buffer header spinlock for shared buffers).
    pub fn buffer_get_lsn_atomic(
        buf: types_storage::storage::Buffer,
    ) -> types_error::PgResult<types_core::primitive::XLogRecPtr>
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

// ---------------------------------------------------------------------------
// Free Space Map page round-trip + buffer primitives (freespace.c/fsmpage.c
// consumer). The FSM page is `(FSMPage) PageGetContents(page)` of a buffer in
// the `FSM_FORKNUM`; the buffer manager owns the shared page, so the FSM
// algorithm reads the page body out as an owned `FSMPageData` and writes the
// mutated body back, bracketed by the lock seams exactly where C holds the
// content lock. No raw `Page` pointer crosses the boundary.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `(FSMPage) PageGetContents(BufferGetPage(buf))` materialized as an owned
    /// [`types_fsm::FSMPageData`] (fsm_internals.h). The caller holds the
    /// appropriate buffer content lock. `Err` carries OOM building the owned
    /// node array.
    pub fn fsm_buffer_get_page(
        buf: types_storage::Buffer,
    ) -> types_error::PgResult<types_fsm::FSMPageData>
);

seam_core::seam!(
    /// Store a mutated FSM page body back into `(FSMPage)
    /// PageGetContents(BufferGetPage(buf))` (the C in-place page mutation).
    /// The caller holds the exclusive content lock. `Err` carries OOM.
    pub fn fsm_buffer_set_page(
        buf: types_storage::Buffer,
        page: types_fsm::FSMPageData,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `BufferGetTag(buf, &rlocator, &forknum, &blknum)` (bufmgr.c) — the
    /// relation/fork/block this buffer currently holds, returned as one owned
    /// triple. Used by the FSM torn-page `DEBUG1` notice.
    pub fn buffer_get_tag(
        buf: types_storage::Buffer,
    ) -> types_error::PgResult<(
        types_storage::RelFileLocator,
        types_core::primitive::ForkNumber,
        types_core::primitive::BlockNumber,
    )>
);

seam_core::seam!(
    /// `LockBuffer(buffer, mode)` (bufmgr.c) — `mode` is one of the
    /// `BUFFER_LOCK_*` constants (`UNLOCK`/`SHARE`/`EXCLUSIVE`). `Err` carries
    /// the lock-manager `ereport(ERROR)`s.
    pub fn lock_buffer(
        buffer: types_storage::Buffer,
        mode: i32,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `PageInit(BufferGetPage(buf), BLCKSZ, 0)` (bufpage.c) — initialize a
    /// fresh (all-zero) FSM page's header. The caller holds the exclusive
    /// content lock. `Err` carries any page-init `ereport(ERROR)`.
    pub fn page_init(buf: types_storage::Buffer) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ReadBufferExtended(rel, forknum, blkno, RBM_ZERO_ON_ERROR, NULL)`
    /// (bufmgr.c) for the FSM fork — pin (reading in, zeroing a torn page) a
    /// block of the relation's `FSM_FORKNUM`. `Err` carries the smgr read
    /// `ereport(ERROR)`s.
    pub fn read_buffer_extended_fsm<'mcx>(
        rel: &types_rel::Relation<'mcx>,
        blkno: types_core::primitive::BlockNumber,
    ) -> types_error::PgResult<types_storage::Buffer>
);

seam_core::seam!(
    /// `ExtendBufferedRelTo(BMR_REL(rel), FSM_FORKNUM, NULL,
    /// EB_CREATE_FORK_IF_NEEDED | EB_CLEAR_SIZE_CACHE, fsm_nblocks,
    /// RBM_ZERO_ON_ERROR)` (bufmgr.c) — ensure the FSM fork is at least
    /// `fsm_nblocks` long, extending with all-zero pages, and pin the target
    /// block. `Err` carries the extension `ereport(ERROR)`s.
    pub fn extend_buffered_rel_to_fsm<'mcx>(
        rel: &types_rel::Relation<'mcx>,
        fsm_nblocks: types_core::primitive::BlockNumber,
    ) -> types_error::PgResult<types_storage::Buffer>
);

// ---------------------------------------------------------------------------
// Visibility-map fork buffer round-trip (visibilitymap.c `vm_readbuf` /
// `vm_extend` consumer). Same shape as the FSM-fork pair above: the visibility
// map is a separate fork (`VISIBILITYMAP_FORKNUM`) and the buffer manager owns
// the shared page, so the VM algorithm crosses the boundary by `Buffer` id.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `ReadBufferExtended(rel, VISIBILITYMAP_FORKNUM, blkno, RBM_ZERO_ON_ERROR,
    /// NULL)` (bufmgr.c) for the VM fork — pin (reading in, zeroing a torn page)
    /// a block of the relation's `VISIBILITYMAP_FORKNUM`. `Err` carries the smgr
    /// read `ereport(ERROR)`s.
    pub fn read_buffer_extended_vm<'mcx>(
        rel: &types_rel::Relation<'mcx>,
        blkno: types_core::primitive::BlockNumber,
    ) -> types_error::PgResult<types_storage::Buffer>
);

seam_core::seam!(
    /// `ExtendBufferedRelTo(BMR_REL(rel), VISIBILITYMAP_FORKNUM, NULL,
    /// EB_CREATE_FORK_IF_NEEDED | EB_CLEAR_SIZE_CACHE, vm_nblocks,
    /// RBM_ZERO_ON_ERROR)` (bufmgr.c) — ensure the VM fork is at least
    /// `vm_nblocks` long, extending with all-zero pages, and pin the target
    /// block. `Err` carries the extension `ereport(ERROR)`s.
    pub fn extend_buffered_rel_to_vm<'mcx>(
        rel: &types_rel::Relation<'mcx>,
        vm_nblocks: types_core::primitive::BlockNumber,
    ) -> types_error::PgResult<types_storage::Buffer>
);
