//! Seam declarations for the `backend-storage-buffer-bufmgr` unit
//! (`storage/buffer/bufmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! Most per-backend GUC globals are passed as explicit parameters
//! (no-ambient-global-seams rule). The exceptions are the ring-sizing knobs
//! (`io_combine_limit` / `effective_io_concurrency` / `get_pin_limit` /
//! `io_direct_data`): the `get_access_strategy(btype)` contract fixes its
//! signature to `btype` alone (it is the bufmgr boundary the buffer-support
//! ring builder crosses), so these process-global knobs that C's
//! `GetAccessStrategy`/`PrefetchLocalBuffer` read directly are reached through
//! getter seams here rather than threaded through a contract that cannot carry
//! them.


seam_core::seam!(
    /// `RelationGetNumberOfBlocksInFork(relation, forkNum)` (bufmgr.c): the
    /// current number of blocks in the relation fork. For a table-AM relation
    /// this is `table_relation_size(rel, fork) / BLCKSZ` (rounded up); for any
    /// other relation with storage it is `smgrnblocks(RelationGetSmgr(rel),
    /// fork)`. The `RelationGetNumberOfBlocks` macro is the `MAIN_FORKNUM` case.
    /// Takes the `&Relation` (the C `Relation`); the owner resolves the physical
    /// id and relkind off it directly. `Err` carries the smgr `ereport(ERROR)`s.
    pub fn relation_get_number_of_blocks_in_fork<'mcx>(
        relation: &types_rel::Relation<'mcx>,
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

seam_core::seam!(
    /// `ReadBufferExtended(rel, MAIN_FORKNUM, blkno, mode, strategy)` (bufmgr.c):
    /// pin (reading in / zeroing per `mode`) a MAIN_FORKNUM block with a runtime
    /// read-buffer mode and optional bulk-insert strategy. The hio.c
    /// `ReadBufferBI` path needs the full mode (`RBM_NORMAL` for re-reads,
    /// `RBM_ZERO_AND_LOCK` / `RBM_ZERO_AND_CLEANUP_LOCK` for the extend path) and
    /// `has_strategy` (`bistate->strategy != NULL`) on one call. `Err` carries
    /// the smgr read ereports.
    pub fn read_buffer_extended_mode<'mcx>(
        rel: &types_rel::Relation<'mcx>,
        blkno: types_core::primitive::BlockNumber,
        mode: types_storage::storage::ReadBufferMode,
        has_strategy: bool,
    ) -> types_error::PgResult<types_storage::storage::Buffer>
);

seam_core::seam!(
    /// `ReadBufferExtended(rel, forknum, blkno, RBM_NORMAL, NULL)` (bufmgr.c):
    /// pin (reading in if needed) a block of an explicit fork with no
    /// buffer-access strategy. Used by `log_newpage_range`, which logs an
    /// arbitrary fork. `Err` carries the smgr read ereports.
    pub fn read_buffer_extended_fork<'mcx>(
        rel: &types_rel::Relation<'mcx>,
        forknum: types_core::primitive::ForkNumber,
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

// ---------------------------------------------------------------------------
// Buffer-access strategy rings (freelist.c). The backend-private
// `BufferAccessStrategyData` ring is built by `get_access_strategy` and handed
// out BY POINTER (`BufferAccessStrategy` = `Rc<RefCell<BufferAccessStrategyData>>`
// / `None`), mirroring C's palloc'd object; consumers (heapam bulk insert, COPY,
// VACUUM, ...) thread that pointer. `None` is the C NULL strategy.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Additional buffer primitives consumed by the hash access method (hashpage.c /
// hashovfl.c / hashsearch.c / hashinsert.c). The hash AM threads `Relation`
// values and `Buffer` ids; the buffer manager owns the shared pages.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `ReadBuffer(rel, blkno)` (bufmgr.c) — pin (reading in if needed) the
    /// given block of the relation's MAIN_FORKNUM with the default RBM_NORMAL
    /// mode and no buffer-access strategy. `Err` carries the smgr read
    /// `ereport(ERROR)`s.
    pub fn read_buffer<'mcx>(
        rel: &types_rel::Relation<'mcx>,
        blkno: types_core::primitive::BlockNumber,
    ) -> types_error::PgResult<types_storage::storage::Buffer>
);

seam_core::seam!(
    /// `ReleaseAndReadBuffer(buffer, relation, blockNum)` (bufmgr.c) — combine
    /// `ReleaseBuffer` and `ReadBuffer`: if `buffer` is valid and already holds
    /// `blockNum` of `relation`'s MAIN_FORKNUM, return it as-is (saving a
    /// release+reacquire); otherwise unpin it (if valid) and read+pin the
    /// requested block. `InvalidBuffer` is accepted (behaves like `ReadBuffer`).
    /// The heap AM's `heapam_index_fetch_tuple` uses this to switch HOT-chain
    /// pages. `Err` carries the smgr read `ereport(ERROR)`s.
    pub fn release_and_read_buffer<'mcx>(
        buffer: types_storage::storage::Buffer,
        relation: &types_rel::Relation<'mcx>,
        block_num: types_core::primitive::BlockNumber,
    ) -> types_error::PgResult<types_storage::storage::Buffer>
);

seam_core::seam!(
    /// `ReadBufferExtended(rel, forkNum, blkno, RBM_ZERO_AND_LOCK, NULL)`
    /// (bufmgr.c) — pin a block, zeroing it and acquiring the exclusive content
    /// lock (used by `_hash_getinitbuf` / the existing-block branch of
    /// `_hash_getnewbuf`). `Err` carries the smgr read `ereport(ERROR)`s.
    pub fn read_buffer_zero_and_lock<'mcx>(
        rel: &types_rel::Relation<'mcx>,
        fork_num: types_core::primitive::ForkNumber,
        blkno: types_core::primitive::BlockNumber,
    ) -> types_error::PgResult<types_storage::storage::Buffer>
);

seam_core::seam!(
    /// `ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, bstrategy)`
    /// (bufmgr.c) — pin a block with an explicit buffer-access strategy (the
    /// VACUUM path: `_hash_getbuf_with_strategy`). A NULL (`None`) strategy
    /// behaves like the default. `Err` carries the smgr read `ereport(ERROR)`s.
    pub fn read_buffer_with_strategy<'mcx>(
        rel: &types_rel::Relation<'mcx>,
        blkno: types_core::primitive::BlockNumber,
        strategy: types_storage::buf::BufferAccessStrategy,
    ) -> types_error::PgResult<types_storage::storage::Buffer>
);

seam_core::seam!(
    /// `ExtendBufferedRel(BMR_REL(rel), forkNum, NULL, EB_LOCK_FIRST |
    /// EB_SKIP_EXTENSION_LOCK)` (bufmgr.c) — extend the relation fork by one
    /// block, returning the new write-locked, pinned buffer (the
    /// extend-the-EOF branch of `_hash_getnewbuf`). `Err` carries the extension
    /// `ereport(ERROR)`s.
    pub fn extend_buffered_rel<'mcx>(
        rel: &types_rel::Relation<'mcx>,
        fork_num: types_core::primitive::ForkNumber,
    ) -> types_error::PgResult<types_storage::storage::Buffer>
);

seam_core::seam!(
    /// `ExtendBufferedRelBy(BMR_REL(rel), MAIN_FORKNUM, strategy, EB_LOCK_FIRST,
    /// extend_by, victim_buffers, &extend_by)` (bufmgr.c) — the hio.c
    /// `RelationAddBlocks` multi-page extension: extend MAIN_FORKNUM by up to
    /// `extend_by` pages with the bulk-insert strategy (when `has_strategy`),
    /// returning the first new block (exclusive-locked via `EB_LOCK_FIRST`), the
    /// pinned victim buffers, and the actual extension count. `Err` carries the
    /// extension `ereport(ERROR)`s.
    pub fn extend_buffered_rel_by_main<'mcx>(
        rel: &types_rel::Relation<'mcx>,
        has_strategy: bool,
        extend_by: u32,
    ) -> types_error::PgResult<types_storage::buf::ExtendedRelation>
);

seam_core::seam!(
    /// `ConditionalLockBufferForCleanup(buffer)` (bufmgr.c) — try to acquire a
    /// cleanup (super-exclusive) lock without blocking; returns whether it was
    /// acquired. `Err` carries the lock-manager `ereport(ERROR)`s.
    pub fn conditional_lock_buffer_for_cleanup(
        buffer: types_storage::Buffer,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `ConditionalLockBuffer(buffer)` (bufmgr.c) — try to acquire the buffer's
    /// exclusive content lock without blocking; returns whether it was acquired.
    /// `Err` carries the lock-manager `ereport(ERROR)`s.
    pub fn conditional_lock_buffer(
        buffer: types_storage::Buffer,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `IsBufferCleanupOK(buffer)` (bufmgr.c) — does the caller already hold a
    /// cleanup-strength lock on the buffer (exclusive content lock + single
    /// pin)? `Err` carries the `Assert`-promoted error surface.
    pub fn is_buffer_cleanup_ok(buffer: types_storage::Buffer) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `log_newpage(&rel->rd_locator, forkNum, blkno, page, page_std)`
    /// (xloginsert.c) — emit an `XLOG_FPI` record for a freshly-initialized
    /// page image. Used by `_hash_init` (per-bucket) and `_hash_alloc_buckets`.
    /// The page image crosses as bytes (`BLCKSZ`). Returns the record's LSN.
    pub fn log_newpage(
        rlocator: types_storage::RelFileLocator,
        fork_num: types_core::primitive::ForkNumber,
        blkno: types_core::primitive::BlockNumber,
        page: &[u8],
        page_std: bool,
    ) -> types_error::PgResult<types_core::XLogRecPtr>
);

seam_core::seam!(
    /// `PageSetChecksumInplace(page, blkno); smgrextend(RelationGetSmgr(rel),
    /// MAIN_FORKNUM, blkno, page, skipFsync)` (bufmgr/smgr) — the
    /// `_hash_alloc_buckets` tail that stamps a checksum into the in-memory
    /// page image and writes it past the current EOF to keep smgr's idea of the
    /// relation length in sync. The page image crosses as bytes (`BLCKSZ`).
    pub fn smgr_extend_page(
        rlocator: types_storage::RelFileLocator,
        fork_num: types_core::primitive::ForkNumber,
        blkno: types_core::primitive::BlockNumber,
        page: &mut [u8],
        skip_fsync: bool,
    ) -> types_error::PgResult<()>
);

// ---------------------------------------------------------------------------
// Per-buffer header array primitives (buf_internals.h) consumed by freelist.c's
// clock sweep and the backend-private ring. The shmem-resident `BufferDesc`
// array (`BufferDescriptors`) is owned by the buffer manager (`buf_init.c`);
// freelist.c reaches it by `buf_id` (the inherited 0-based-index opacity).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `LockBufHdr(desc)` (bufmgr.c) — spin to acquire the buffer header's
    /// in-`state` spinlock bit (`BM_LOCKED`) and return the observed `state`
    /// word (with `BM_LOCKED` set). Infallible (spins).
    pub fn lock_buf_hdr(buf_id: i32) -> u32
);

seam_core::seam!(
    /// `UnlockBufHdr(desc, buf_state)` (buf_internals.h) — write `buf_state`
    /// back with `BM_LOCKED` cleared, releasing the header spinlock.
    pub fn unlock_buf_hdr(buf_id: i32, buf_state: u32)
);

seam_core::seam!(
    /// `GetBufferDescriptor(buf_id)->freeNext` (buf_internals.h) — read the
    /// freelist link of a buffer (protected by `buffer_strategy_lock`).
    pub fn buf_free_next(buf_id: i32) -> i32
);

seam_core::seam!(
    /// `GetBufferDescriptor(buf_id)->freeNext = value` (buf_internals.h) —
    /// write the freelist link of a buffer (protected by
    /// `buffer_strategy_lock`).
    pub fn set_buf_free_next(buf_id: i32, value: i32)
);

seam_core::seam!(
    /// `GetPinLimit()` (bufmgr.c) — the maximum number of buffers this backend
    /// could ever additionally pin, used to size a `BAS_BULKREAD` ring.
    pub fn get_pin_limit() -> i32
);

seam_core::seam!(
    /// `io_combine_limit` (GUC) — the maximum number of blocks a single I/O may
    /// combine, consulted when sizing a `BAS_BULKREAD` ring.
    pub fn io_combine_limit() -> i32
);

seam_core::seam!(
    /// `effective_io_concurrency` (GUC) — the configured degree of I/O
    /// concurrency, consulted when sizing a `BAS_BULKREAD` ring. May be 0.
    pub fn effective_io_concurrency() -> i32
);

seam_core::seam!(
    /// `(io_direct_flags & IO_DIRECT_DATA) != 0` (fd.c/bufmgr.h) — whether
    /// direct I/O is enabled for relation data, which disables prefetch in
    /// `PrefetchLocalBuffer`.
    pub fn io_direct_data() -> bool
);

seam_core::seam!(
    /// `maintenance_io_concurrency` (GUC) — the configured degree of I/O
    /// concurrency for maintenance work (VACUUM/CREATE INDEX), consulted by a
    /// `READ_STREAM_MAINTENANCE` stream. May be 0.
    pub fn maintenance_io_concurrency() -> i32
);

seam_core::seam!(
    /// `io_method == IOMETHOD_SYNC` (aio.c GUC) — whether the synchronous I/O
    /// method is in use, which enables read-ahead advice in a read stream.
    pub fn io_method_sync() -> bool
);

seam_core::seam!(
    /// `GetAccessStrategy(btype)` (freelist.c): allocate a ring buffer of the
    /// kind appropriate for `btype` and return its handle. `Err` carries the
    /// allocation `ereport(ERROR)` surface.
    pub fn get_access_strategy(
        btype: types_storage::buf::BufferAccessStrategyType,
    ) -> types_error::PgResult<types_storage::buf::BufferAccessStrategy>
);

seam_core::seam!(
    /// `FreeAccessStrategy(strategy)` (freelist.c): free a ring buffer
    /// previously obtained from `GetAccessStrategy`. The C path `pfree`s and
    /// only `Assert`s, so the seam is infallible. A NULL (`None`) strategy
    /// is a no-op in C; callers should not pass one.
    pub fn free_access_strategy(strategy: types_storage::buf::BufferAccessStrategy)
);

seam_core::seam!(
    /// `DropRelationBuffers(smgr_reln, forkNum, nforks, firstDelBlock)`
    /// (bufmgr.c) — drop from the shared buffer pool every buffer of the given
    /// relation that lies at or after `nblocks[i]` in fork `forknum[i]`, without
    /// writing the contents. `smgrtruncate` calls it before truncating on disk.
    /// The C `SMgrRelation` is flattened to its `RelFileLocatorBackend`. `Err`
    /// carries the buffer-pool `ereport(ERROR)`s.
    pub fn drop_relation_buffers(
        smgr_reln: types_storage::RelFileLocatorBackend,
        forknum: &[types_core::primitive::ForkNumber],
        nblocks: &[types_core::primitive::BlockNumber],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `DropRelationsAllBuffers(smgr_reln, nlocators)` (bufmgr.c) — drop every
    /// buffer of all the given relations from the shared pool without writing
    /// the contents. `smgrdounlinkall` calls it before unlinking on disk. The
    /// C `SMgrRelation *` array is flattened to a `RelFileLocatorBackend` slice.
    /// `Err` carries the buffer-pool `ereport(ERROR)`s.
    pub fn drop_relations_all_buffers(
        smgr_reln: &[types_storage::RelFileLocatorBackend],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `FlushRelationsAllBuffers(smgrs, nrels)` (bufmgr.c) — write every dirty
    /// buffer of all the given relations to the kernel (but do not fsync them).
    /// `smgrdosyncall` calls it before the per-fork immediate sync. The
    /// C `SMgrRelation *` array is flattened to a `RelFileLocatorBackend` slice.
    /// `Err` carries the write `ereport(ERROR)`s.
    pub fn flush_relations_all_buffers(
        smgrs: &[types_storage::RelFileLocatorBackend],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `FlushRelationBuffers(rel)` (bufmgr.c) — write every dirty buffer of the
    /// one relation out to the kernel (but do not fsync). `fill_seq_with_data`
    /// calls it for an unlogged sequence's freshly-written INIT fork.
    /// `void` in C; `Err` carries the write `ereport(ERROR)`s.
    pub fn flush_relation_buffers<'mcx>(
        rel: &types_rel::Relation<'mcx>,
    ) -> types_error::PgResult<()>
);

// ---------------------------------------------------------------------------
// ResourceOwner buffer-pin bookkeeping (bufmgr.c:244).
//
// In C `ResourceOwnerRememberBuffer` / `ResourceOwnerForgetBuffer` are bufmgr.c
// macros over the generic `ResourceOwnerRemember/Forget(owner, Int32GetDatum(b),
// &buffer_pin_resowner_desc)`, where `buffer_pin_resowner_desc` and its
// `ResOwnerReleaseBufferPin` callback are DEFINED IN bufmgr.c. The bufmgr core
// (pin/unpin/incr) consumes these to keep the current resource owner's pin list
// in sync; the resowner crate (backend-utils-resowner-resowner, still `todo`)
// installs them when it lands. Until then a call panics loudly (a real pin
// cannot be resource-owner-tracked before resowner ports — sanctioned
// panic-until-owner).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `ResourceOwnerRememberBuffer(CurrentResourceOwner, buffer)` (bufmgr.c) —
    /// record one buffer pin on the current resource owner so a transaction/
    /// portal abort can release the leaked pin. Infallible in C (the enlarge
    /// that may `ereport` is a separate, earlier call).
    pub fn remember_buffer(buffer: types_storage::storage::Buffer)
);

seam_core::seam!(
    /// `ResourceOwnerForgetBuffer(CurrentResourceOwner, buffer)` (bufmgr.c) —
    /// drop the record of one buffer pin from the current resource owner.
    /// Infallible in C.
    pub fn forget_buffer(buffer: types_storage::storage::Buffer)
);

seam_core::seam!(
    /// `ResourceOwnerEnlarge(CurrentResourceOwner)` (bufmgr.c) — ensure the
    /// current resource owner has room to remember one more buffer pin before
    /// the pin is taken (so the remember below cannot fail). `Err` carries the
    /// `ereport(ERROR)` on memory exhaustion.
    pub fn resowner_enlarge() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `pgBufferUsage.shared_blks_dirtied++` / `pgstat_count_buffer_dirtied`-
    /// style accounting (bufmgr.c) — note that this backend just dirtied a
    /// previously-clean shared buffer. Owned by the per-backend buffer-usage
    /// statistics (pgstat) when it ports; infallible.
    pub fn count_buffer_dirtied()
);

seam_core::seam!(
    /// `pgBufferUsage.shared_blks_written++` (bufmgr.c) — note that this backend
    /// just wrote one shared buffer to disk (the relation-extension path bumps it
    /// once per newly-added block). Owned by the per-backend buffer-usage
    /// statistics (pgstat) when it ports; infallible, stats-only.
    pub fn count_buffer_write()
);

seam_core::seam!(
    /// `pgstat_count_io_op_time(IOOBJECT_RELATION, io_context, IOOP_EXTEND,
    /// io_start, cnt, bytes)` (bufmgr.c) — record an I/O operation against the
    /// relation object: `cnt` extend operations totalling `bytes` bytes, plus the
    /// elapsed time when `track_io_timing` is on. The `pgstat_prepare_io_time` /
    /// `track_io_timing` start-timestamp dance is internal to the statistics
    /// subsystem; this seam collapses it to the post-operation accounting call,
    /// behaviour-neutral (stats only). Owned by pgstat when it ports; infallible.
    pub fn count_io_op_extend(cnt: u64, bytes: u64)
);

seam_core::seam!(
    /// `ResourceOwnerRememberBufferIO(CurrentResourceOwner, buffer)` (bufmgr.c) —
    /// record one in-progress buffer I/O on the current resource owner so a
    /// transaction/portal abort can clean up a buffer left mid-I/O. The buffer-IO
    /// `ResourceOwnerDesc` is defined in bufmgr.c; installed by resowner when it
    /// ports (panic-until-owner). Infallible in C (the enlarge that may `ereport`
    /// is the earlier `resowner_enlarge` call in `StartBufferIO`).
    pub fn remember_buffer_io(buffer: types_storage::storage::Buffer)
);

seam_core::seam!(
    /// `ResourceOwnerForgetBufferIO(CurrentResourceOwner, buffer)` (bufmgr.c) —
    /// drop the record of one in-progress buffer I/O from the current resource
    /// owner (`TerminateBufferIO` with `forget_owner`). Infallible in C.
    pub fn forget_buffer_io(buffer: types_storage::storage::Buffer)
);

// ---------------------------------------------------------------------------
// resowner release callbacks (bufmgr.c `ResOwnerReleaseBufferPin` /
// `ResOwnerReleaseBufferIO`). These are the `ReleaseResource` callbacks of the
// bufmgr-defined `buffer_pin_resowner_desc` / `buffer_io_resowner_desc`. The
// resowner owner crate holds the descriptors and invokes the callback when a
// leaked pin/IO is found during release; the body is bufmgr-internal
// (`UnpinBufferNoOwner` / `AbortBufferIO`), so it is installed by bufmgr.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `ResOwnerReleaseBufferPin(Datum res)` (bufmgr.c:6555) — release a leaked
    /// buffer pin without touching the (already-being-released) resource owner.
    /// `UnpinBufferNoOwner` / `UnpinLocalBufferNoOwner`. `Err` carries the
    /// `elog(ERROR, "bad buffer ID")`.
    pub fn release_buffer_pin(buffer: types_storage::storage::Buffer) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ResOwnerReleaseBufferIO(Datum res)` (bufmgr.c:6539) — abort a leaked
    /// in-progress buffer I/O (`AbortBufferIO`).
    pub fn release_buffer_io(buffer: types_storage::storage::Buffer) -> types_error::PgResult<()>
);

// ---------------------------------------------------------------------------
// LockBufferForCleanup recovery-conflict (InHotStandby) deep leg (bufmgr.c).
//
// In C the InHotStandby branch of LockBufferForCleanup performs a multi-step
// recovery-conflict wait: ps-display "waiting" suffix, deadlock-timeout
// LogRecoveryConflict logging, publishing the bufid the Startup process waits
// on (SetStartupBufferPinWaitBufId), the ResolveRecoveryConflictWithBufferPin
// alarm-and-park, and resetting the published bufid. Every step touches the
// startup/recovery subsystem (in_hot_standby / startup-buffer-pin-wait-bufid /
// the recovery-conflict resolver), which is not reachable from this core; the
// whole branch is bundled into the seams below, installed by the recovery owner
// when it ports (panic-until-owner — sanctioned deep standby leg).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `InHotStandby` (xlogutils.h) — is this backend serving queries while the
    /// server is in hot-standby recovery? Gates the recovery-conflict wait leg.
    pub fn in_hot_standby() -> bool
);

seam_core::seam!(
    /// `GetStartupBufferPinWaitBufId()` (proc.c) — the 0-based buffer id the
    /// Startup process published as the one it is waiting on, or `-1` if none.
    pub fn startup_buffer_pin_wait_buf_id() -> i32
);

seam_core::seam!(
    /// The whole `InHotStandby` recovery-conflict wait of
    /// `LockBufferForCleanup` (bufmgr.c:5751-5794): ps-display suffix on first
    /// wait, deadlock-timeout `LogRecoveryConflict`, publish-bufid,
    /// `ResolveRecoveryConflictWithBufferPin` alarm+park, reset bufid. Takes the
    /// loop-carried `(wait_start, waiting, logged_recovery_conflict)` and returns
    /// their updated values. `Err` carries the recovery-conflict
    /// `ereport(ERROR)` surface.
    pub fn lock_buffer_for_cleanup_recovery_wait_park(
        buffer: types_storage::storage::Buffer,
        wait_start: types_core::primitive::TimestampTz,
        waiting: bool,
        logged_recovery_conflict: bool,
    ) -> types_error::PgResult<(types_core::primitive::TimestampTz, bool, bool)>
);

seam_core::seam!(
    /// `LogRecoveryConflict(PROCSIG_RECOVERY_CONFLICT_BUFFERPIN, waitStart,
    /// GetCurrentTimestamp(), NULL, false)` (bufmgr.c:5725) — the
    /// "resolved after deadlock_timeout" recovery-conflict log emitted from
    /// `LockBufferForCleanup` once the cleanup lock is finally acquired. Only
    /// reachable after the park leg above set `logged_recovery_conflict`. `Err`
    /// carries its `ereport` surface.
    pub fn lock_buffer_for_cleanup_recovery_wait(
        buffer: types_storage::storage::Buffer,
        wait_start: types_core::primitive::TimestampTz,
        waiting: bool,
        logged_recovery_conflict: bool,
        resolved: bool,
    ) -> types_error::PgResult<()>
);

// ---------------------------------------------------------------------------
// Local-buffer (localbuf.c) dispatch consumed by the F1c content-lock surface.
//
// bufmgr.c's content-lock functions test `BufferIsLocal(buffer)` (a negative id)
// and dispatch to the local-buffer manager for the temp-relation pool. The local
// pool is owned by `backend-storage-buffer-support` (localbuf.c), but its ambient
// per-backend `LocalBufferManager` handle is not yet established, so these are
// bufmgr-OUTWARD seams installed by the local-buffer owner when that ambient
// handle lands (panic-until-owner — sanctioned, like the F1b pin dispatch).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `LocalRefCount[-buffer - 1]` (localbuf.c) — this backend's local pin count
    /// for a local (temp) buffer.
    pub fn local_ref_count(buffer: types_storage::storage::Buffer) -> types_error::PgResult<i32>
);

seam_core::seam!(
    /// `MarkLocalBufferDirty(buffer)` (localbuf.c) — mark a local (temp) buffer's
    /// contents dirty (the `BufferIsLocal` arm of `MarkBufferDirtyHint` /
    /// `MarkBufferDirty`).
    pub fn mark_local_buffer_dirty(
        buffer: types_storage::storage::Buffer,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `LocalBufferAlloc(smgr, forkNum, blockNum, foundPtr)` (localbuf.c) — the
    /// `RELPERSISTENCE_TEMP` arm of `PinBufferForBlock` (bufmgr.c:1148): find or
    /// allocate the given block in this backend's local (temp) buffer pool. The
    /// C `SMgrRelation` is flattened to its `RelFileLocatorBackend`. Returns the
    /// pinned (local) `Buffer` and `found = true` iff the block was already
    /// present. Installed by the local-buffer owner when its ambient per-backend
    /// `LocalBufferManager` handle lands (panic-until-owner — sanctioned, same
    /// posture as the F1c local-buffer pin dispatch). `Err` carries the localbuf
    /// `ereport(ERROR)`s.
    pub fn local_buffer_alloc(
        smgr_reln: types_storage::RelFileLocatorBackend,
        fork_num: types_core::primitive::ForkNumber,
        block_num: types_core::primitive::BlockNumber,
    ) -> types_error::PgResult<(types_storage::storage::Buffer, bool)>
);

seam_core::seam!(
    /// `PrefetchLocalBuffer(smgr, forkNum, blockNum)` (localbuf.c) — the
    /// `RELPERSISTENCE_TEMP` arm of `PrefetchBuffer` (bufmgr.c:665): initiate (or
    /// note as unnecessary) a prefetch of a block in this backend's local (temp)
    /// buffer pool. The C `SMgrRelation` is flattened to its
    /// `RelFileLocatorBackend`. Installed by the local-buffer owner
    /// (panic-until-owner). `Err` carries the localbuf `ereport(ERROR)`s.
    pub fn prefetch_local_buffer(
        smgr_reln: types_storage::RelFileLocatorBackend,
        fork_num: types_core::primitive::ForkNumber,
        block_num: types_core::primitive::BlockNumber,
    ) -> types_error::PgResult<types_storage::PrefetchBufferResult>
);

seam_core::seam!(
    /// `AtEOXact_LocalBuffers(isCommit)` (localbuf.c) — the local-buffer leg of
    /// `AtEOXact_Buffers` (bufmgr.c:3995): leak-check this backend's local (temp)
    /// buffer pins at end of transaction. Installed by the local-buffer owner
    /// when its ambient per-backend handle lands (panic-until-owner — sanctioned,
    /// same posture as the F1c local-buffer dispatch). `Err` carries the localbuf
    /// `ereport` surface.
    pub fn at_eoxact_local_buffers(is_commit: bool) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `AtProcExit_LocalBuffers()` (localbuf.c) — the local-buffer leg of
    /// `AtProcExit_Buffers` (bufmgr.c:4047): leak-check this backend's local
    /// (temp) buffer pins at backend exit. Installed by the local-buffer owner
    /// when its ambient per-backend handle lands (panic-until-owner). `Err`
    /// carries the localbuf `ereport` surface.
    pub fn at_proc_exit_local_buffers() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExtendBufferedRelLocal(bmr, fork, flags, extend_by, extend_upto, buffers,
    /// extended_by)` (localbuf.c) — the `RELPERSISTENCE_TEMP` arm of
    /// `ExtendBufferedRelCommon` (bufmgr.c:2580): extend a temp relation fork in
    /// this backend's local (temp) buffer pool by up to `extend_by` blocks,
    /// filling `buffers` with the pinned new (local) `Buffer`s and reporting the
    /// actual count extended. The C `BufferManagerRelation` is flattened to its
    /// `RelFileLocatorBackend` + relpersistence (already known to be TEMP). The
    /// extension `flags` (`EB_*` bits) cross as a bitmask. Returns
    /// `(first_block, extended_by)`. Installed by the local-buffer owner
    /// (panic-until-owner). `Err` carries the localbuf `ereport(ERROR)`s.
    pub fn extend_buffered_rel_local(
        smgr_reln: types_storage::RelFileLocatorBackend,
        fork_num: types_core::primitive::ForkNumber,
        flags: u32,
        extend_by: u32,
        extend_upto: types_core::primitive::BlockNumber,
        buffers: &mut [types_storage::storage::Buffer],
    ) -> types_error::PgResult<(types_core::primitive::BlockNumber, u32)>
);

// ---------------------------------------------------------------------------
// AIO engine handle accessors for the explicit multi-block read pipeline
// (`StartReadBuffers`/`WaitReadBuffers`/`AsyncReadBuffers`, buf_read.rs +
// buf_aio.rs). The buffer manager models the AIO-shaped read path IN-CRATE
// (the descriptor staging, the BM_IO_IN_PROGRESS interlock, the run splitting),
// but the actual pgaio handle lifecycle — `pgaio_io_acquire`, the
// `pgaio_io_register_callbacks` of the buffer-readv completion vtable, the
// `smgrstartreadv` submit, and the `pgaio_wref_wait` — lives in the AIO engine
// (backend-storage-aio-*). These seams cross that boundary by `Buffer`
// id/run + `PgAioWaitRef`; the aio-sync method stage installs them AFTER this
// F3 read layer lands (panic-until-owner — sanctioned, the single-block
// synchronous `ReadBuffer*` core does NOT touch them and stays fully live).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `pgaio_io_acquire(CurrentResourceOwner, &operation->io_return)`
    /// (aio.c) — acquire an AIO handle for one in-flight buffer read, returning
    /// its wait reference (`pgaio_io_get_wref`). The handle's `io_return` slot is
    /// the issuer-owned result the completion path writes back. `Err` carries the
    /// AIO `ereport(ERROR)` surface (handle exhaustion). Installed by the AIO
    /// engine; panics until then.
    pub fn pgaio_io_acquire() -> types_error::PgResult<types_storage::buf::PgAioWaitRef>
);

seam_core::seam!(
    /// `pgaio_io_register_callbacks(ioh, PGAIO_HCB_{SHARED,LOCAL}_BUFFER_READV,
    /// cb_data)` + `pgaio_io_set_handle_data_32(ioh, io_buffers, len)`
    /// (bufmgr.c `AsyncReadBuffers`) — bind the buffer-readv completion vtable
    /// (with `cb_data` == the `READ_BUFFERS_*` flag bitmask) to the acquired
    /// handle and record the run of 0-based buf_ids it covers. `is_temp` selects
    /// the LOCAL vs SHARED completion callback. `Err` carries the AIO
    /// `ereport(ERROR)` surface. Installed by the AIO engine; panics until then.
    pub fn pgaio_register_callbacks(
        wref: types_storage::buf::PgAioWaitRef,
        io_buffers: &[i32],
        flags: u8,
        synchronous: bool,
        is_temp: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `smgrstartreadv(ioh, operation->smgr, forknum, blocknum, BufferGetBlock(..),
    /// io_buffers_len)` (bufmgr.c `AsyncReadBuffers`) — submit the vectored read
    /// of `io_buffers_len` consecutive blocks starting at `blocknum` into the run
    /// previously registered on the handle `wref`. In IOMETHOD_SYNC the read
    /// happens inline and the shared completion callback runs before this
    /// returns, so on return the handle's `io_return` slot carries the actual
    /// blocks-read count + status. `Err` carries the smgr `ereport(ERROR)`
    /// surface. Installed by the AIO engine; panics until then.
    pub fn start_read_buffers(
        wref: types_storage::buf::PgAioWaitRef,
        rlocator: types_storage::RelFileLocatorBackend,
        forknum: types_core::primitive::ForkNumber,
        blocknum: types_core::primitive::BlockNumber,
        io_buffers_len: i32,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `pgaio_wref_wait(&operation->io_wref)` (bufmgr.c `WaitReadBuffers`) — block
    /// until the in-flight read referenced by `wref` completes, returning the
    /// completed `(result, status)` of the AIO operation: `result` is the actual
    /// number of blocks SMGR read (negative/zero for an error), `status` is the
    /// `PgAioResultStatus` (0=UNKNOWN, 1=OK, 2=PARTIAL, 3=WARNING, 4=ERROR — the
    /// completion path reports/raises on WARNING/ERROR). `Err` carries the AIO
    /// `ereport(ERROR)` surface. Installed by the AIO engine; panics until then.
    pub fn wait_read_buffers(
        wref: types_storage::buf::PgAioWaitRef,
    ) -> types_error::PgResult<(i32, u32)>
);

seam_core::seam!(
    /// `pgaio_wref_check_done(&operation->io_wref)` (bufmgr.c `WaitReadBuffers`) —
    /// whether the in-flight read referenced by `wref` has already completed
    /// (so `WaitReadBuffers` can skip the wait-time accounting). Installed by the
    /// AIO engine; panics until then.
    pub fn wref_check_done(
        wref: types_storage::buf::PgAioWaitRef,
    ) -> types_error::PgResult<bool>
);

// ---------------------------------------------------------------------------
// F5 (flush/drop) outward seams.
//
// The local-buffer drop arms of DropRelationBuffers/DropRelationsAllBuffers
// dispatch to localbuf.c (DropRelationLocalBuffers / DropRelationAllLocalBuffers
// on the per-backend LocalBufferManager), which the local-buffer owner installs
// (panic-until-owner — same posture as the F1 local-buffer pin dispatch).
//
// The checkpoint/bgwriter throttling + the strategy clock-sweep snapshot are
// the checkpointer/bgwriter subsystems' concern; BufferSync/BgBufferSync call
// out through these getters and pacing seams. The pure per-backend statistics
// counters (pgstat) are bumped through no-op-installable accounting seams, the
// same behaviour-neutral posture as F2's count_buffer_write/count_io_op_extend.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `DropRelationLocalBuffers(rlocator, forkNum, firstDelBlock)` (localbuf.c)
    /// — drop from this backend's local (temp) buffer pool all pages of the
    /// relation forks `forkNum[j]` with block number `>= firstDelBlock[j]`,
    /// without writing them. Installed by the local-buffer owner (panic-until-
    /// owner). `Err` carries the localbuf `ereport(ERROR)`s.
    pub fn drop_relation_local_buffers(
        rlocator: types_storage::RelFileLocator,
        forknum: &[types_core::primitive::ForkNumber],
        first_del_block: &[types_core::primitive::BlockNumber],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `DropRelationAllLocalBuffers(rlocator)` (localbuf.c) — drop from this
    /// backend's local (temp) buffer pool every page of all forks of the
    /// relation. Installed by the local-buffer owner (panic-until-owner). `Err`
    /// carries the localbuf `ereport(ERROR)`s.
    pub fn drop_relation_all_local_buffers(
        rlocator: types_storage::RelFileLocator,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `pgstat_report_checkpointer()` accumulation of `ckpt_bufs_written++`
    /// (bufmgr.c `BufferSync`) — note that the checkpoint wrote one shared
    /// buffer. Owned by the checkpoint statistics when it ports; infallible,
    /// stats-only.
    pub fn count_checkpoint_buffer_written()
);

seam_core::seam!(
    /// `PendingBgWriterStats.buf_alloc += recent_alloc` (bufmgr.c `BgBufferSync`)
    /// — report the buffer allocations the clock sweep observed since the last
    /// call. Owned by the bgwriter statistics when it ports; infallible.
    pub fn report_bgwriter_buf_alloc(recent_alloc: i32)
);

seam_core::seam!(
    /// `PendingBgWriterStats.maxwritten_clean++` (bufmgr.c `BgBufferSync`) — note
    /// that the LRU scan stopped early because it hit `bgwriter_lru_maxpages`.
    /// Owned by the bgwriter statistics when it ports; infallible.
    pub fn count_bgwriter_maxwritten_clean()
);

seam_core::seam!(
    /// `PendingBgWriterStats.buf_written_clean++` (bufmgr.c `BgBufferSync`) — note
    /// that the bgwriter wrote one clean (reusable) buffer. Owned by the bgwriter
    /// statistics when it ports; infallible.
    pub fn count_bgwriter_buffer_written_clean()
);

seam_core::seam!(
    /// `bgwriter_lru_maxpages` GUC (guc) — the max number of LRU pages the
    /// background writer flushes per round (≤ 0 disables the LRU scan).
    pub fn bgwriter_lru_maxpages() -> i32
);

seam_core::seam!(
    /// `bgwriter_lru_multiplier` GUC (guc) — the multiple of recent average
    /// allocations the bgwriter tries to keep clean ahead of the strategy point.
    pub fn bgwriter_lru_multiplier() -> f64
);

seam_core::seam!(
    /// `BgWriterDelay` GUC (guc) — the bgwriter's sleep between rounds, in
    /// milliseconds (used to pace the whole-pool minimum scan rate).
    pub fn bgwriter_delay() -> i32
);

seam_core::seam!(
    /// `checkpoint_flush_after` GUC (guc) — after how many buffer writes the
    /// checkpoint issues a kernel writeback hint (the writeback context's
    /// `max_pending`, in pages; 0 disables writeback control).
    pub fn checkpoint_flush_after() -> i32
);

seam_core::seam!(
    /// `bgwriter_flush_after` GUC (bufmgr.c `int bgwriter_flush_after`) — after
    /// how many buffer writes the background writer issues a kernel writeback
    /// hint (the bgwriter writeback context's `max_pending`, in pages; 0 disables
    /// writeback control). Owned by the GUC machinery when it ports.
    pub fn bgwriter_flush_after() -> i32
);

seam_core::seam!(
    /// `CheckpointWriteDelay(flags, progress)` (checkpointer.c) — throttle the
    /// checkpoint write rate to spread the I/O across the checkpoint interval,
    /// also servicing checkpointer requests / barrier events. `progress` is the
    /// fraction (0..1) of buffers processed so far. Owned by the checkpointer
    /// when it ports (panic-until-owner). `Err` carries the `ereport(ERROR)`
    /// surface (shutdown request).
    pub fn checkpoint_write_delay(
        flags: i32,
        progress: f64,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `DropDatabaseBuffers(dbid)` (bufmgr.c): drop every shared-buffer page
    /// that belongs to database `dbid` from the buffer pool — the `dropdb` /
    /// `dbase_redo` `XLOG_DBASE_DROP` cleanup. Owned by bufmgr (panic until it
    /// installs this).
    pub fn drop_database_buffers(dbid: types_core::Oid) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `FlushDatabaseBuffers(dbid)` (bufmgr.c): write out all dirty
    /// shared-buffer pages of database `dbid` (the
    /// `XLOG_DBASE_CREATE_FILE_COPY` "force source up-to-date for the copy"
    /// step in `dbase_redo`/`createdb`). Owned by bufmgr. `Err` carries the
    /// I/O `ereport(ERROR)` surface.
    pub fn flush_database_buffers(dbid: types_core::Oid) -> types_error::PgResult<()>
);
