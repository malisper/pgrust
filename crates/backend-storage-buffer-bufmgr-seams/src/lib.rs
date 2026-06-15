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
// Buffer-access strategy rings (freelist.c). The opaque
// `BufferAccessStrategyData` ring lives in the buffer manager; consumers
// (heapam bulk insert, COPY, VACUUM, ...) only thread the `BufferAccessStrategy`
// handle. `id == 0` is the C NULL strategy.
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
    /// VACUUM path: `_hash_getbuf_with_strategy`). A NULL (`id == 0`) strategy
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
    /// `ConditionalLockBufferForCleanup(buffer)` (bufmgr.c) — try to acquire a
    /// cleanup (super-exclusive) lock without blocking; returns whether it was
    /// acquired. `Err` carries the lock-manager `ereport(ERROR)`s.
    pub fn conditional_lock_buffer_for_cleanup(
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
    /// only `Assert`s, so the seam is infallible. A NULL (`id == 0`) strategy
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
