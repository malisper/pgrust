use types_core::{BlockNumber, ForkNumber};
use types_error::PgResult;
use types_storage::{RelFileLocatorBackend, WriteChunk};

seam_core::seam!(
    pub fn smgr_release_rel_locator(rlocator: RelFileLocatorBackend) -> PgResult<()>
);

seam_core::seam!(
    // smgrcreate(smgropen(rlocator), forknum, isRedo) (smgr.c); C's
    // SMgrRelation handle crosses as its locator key — the open-handle cache
    // lookup stays inside smgr.
    pub fn smgr_create(rlocator: RelFileLocatorBackend, forknum: ForkNumber, is_redo: bool) -> PgResult<()>
);

seam_core::seam!(
    // smgrnblocks(smgropen(rlocator), forknum) (smgr.c).
    pub fn smgr_nblocks(rlocator: RelFileLocatorBackend, forknum: ForkNumber) -> PgResult<BlockNumber>
);
seam_core::seam!(
    // smgrnblocks(RelationGetSmgr(rel), forknum): the rd_smgr pin is required —
    // unpinned entries die at AtEOXact_SMgr, closing fds (openat+close/query).
    pub fn rel_smgr_nblocks<'a, 'mcx>(
        rel: &'a types_rel::RelationData<'mcx>,
        forknum: ForkNumber,
    ) -> PgResult<BlockNumber>
);
seam_core::seam!(
    // smgrnblocks_cached (smgr.c): recovery-gated cache read.
    pub fn smgr_nblocks_cached(rlocator: RelFileLocatorBackend, forknum: ForkNumber) -> BlockNumber
);

seam_core::seam!(
    // smgrexists(smgropen(rlocator), forknum) (smgr.c).
    pub fn smgr_exists(rlocator: RelFileLocatorBackend, forknum: ForkNumber) -> PgResult<bool>
);

seam_core::seam!(
    // reln->smgr_cached_nblocks[forknum] raw field read; InvalidBlockNumber
    // when uncached (fsm/vm size checks trust it outside recovery).
    pub fn smgr_cached_nblocks(
        rlocator: RelFileLocatorBackend,
        forknum: ForkNumber,
    ) -> BlockNumber
);

seam_core::seam!(
    // reln->smgr_cached_nblocks[forknum] = value.
    pub fn smgr_set_cached_nblocks(
        rlocator: RelFileLocatorBackend,
        forknum: ForkNumber,
        value: BlockNumber,
    ) -> PgResult<()>
);

seam_core::seam!(
    // smgrdestroyall() (smgr.c).
    pub fn smgr_destroy_all() -> PgResult<()>
);

seam_core::seam!(
    // smgrprefetch(smgropen(rlocator), forknum, blocknum, nblocks) (smgr.c);
    // false = relation file gone (recovery) — advisory only.
    pub fn smgr_prefetch(
        rlocator: RelFileLocatorBackend,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        nblocks: i32,
    ) -> PgResult<bool>
);

seam_core::seam!(
    // smgrstartreadv (smgr.c aio read path) narrowed to one block landing in
    // shared buffer `buffer`; false = ring unavailable/full/file gone --
    // caller backs the IO out and falls back to advisory prefetch.
    pub fn smgr_start_buffer_read(
        rlocator: RelFileLocatorBackend,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        buffer: i32,
    ) -> PgResult<bool>
);

seam_core::seam!(
    // smgrreadv(smgropen(rlocator), forknum, blocknum, 1 iov) (smgr.c);
    // buffer is exactly BLCKSZ.
    pub fn smgr_read(
        rlocator: RelFileLocatorBackend,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        buffer: &mut [u8],
    ) -> PgResult<()>
);

seam_core::seam!(
    // smgrreadv(smgropen(rlocator), forknum, blocknum, n iovs) (smgr.c); each
    // buffer is exactly BLCKSZ and the run must not cross a RELSEG_SIZE
    // segment boundary (mdreadv ereports otherwise — callers cap like C's
    // smgrmaxcombine).
    pub fn smgr_readv(
        rlocator: RelFileLocatorBackend,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        buffers: &mut [&mut [u8]],
    ) -> PgResult<()>
);

seam_core::seam!(
    // ProcessBarrierSmgrRelease() (smgr.c); barrier processors may ereport.
    pub fn process_barrier_smgr_release() -> PgResult<bool>
);

seam_core::seam!(
    // smgrwrite(smgropen(rlocator), forknum, blocknum, buffer, skipFsync)
    // (smgr.c); buffer is exactly BLCKSZ. WriteChunk, not &[u8]: the buffer
    // pool's flush path holds only a SHARE content lock and hint-bit setters
    // mutate the same shared image under it, so an immutable slice would be
    // a false promise to the optimizer (types_storage::writechunk).
    pub fn smgr_write<'a>(
        rlocator: RelFileLocatorBackend,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        buffer: WriteChunk<'a>,
        skip_fsync: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    // smgrzeroextend(smgropen(rlocator), forknum, blocknum, nblocks, skipFsync)
    // (smgr.c).
    pub fn smgr_zeroextend(
        rlocator: RelFileLocatorBackend,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        nblocks: i32,
        skip_fsync: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    // smgrwriteback(smgropen(rlocator), forknum, blocknum, nblocks) (smgr.c).
    pub fn smgr_writeback(
        rlocator: RelFileLocatorBackend,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        nblocks: BlockNumber,
    ) -> PgResult<()>
);

// ---- process-global relation-size cache maintenance (md nblocks cache) ----
// No C counterpart: the cache is the single-address-space replacement for
// per-statement lseek(SEEK_END) size probes; these seams cover the file
// mutations that happen outside md's own entry points.

seam_core::seam!(
    // DROP/move DATABASE removes the database's file tree wholesale (rmtree,
    // not per-relation unlinks): purge every cached size keyed to it.
    pub fn smgr_nblocks_cache_purge_db(db: types_core::Oid) -> ()
);

seam_core::seam!(
    // The unlogged-relation reinit pass copies init forks over main forks at
    // the file level: drop the whole cache (startup-time, rare).
    pub fn smgr_nblocks_cache_clear() -> ()
);

seam_core::seam!(
    // A table AM that mutates its main-fork bytes by direct file io (the
    // columnar byte stream) declares so at writer open: the key is served by
    // the real lseek walk from then on.
    pub fn smgr_nblocks_cache_poison(
        rlocator: RelFileLocatorBackend,
        forknum: ForkNumber,
    ) -> ()
);

seam_core::seam!(
    // smgr_aio_reopen (smgr.c): re-resolve the IO's target through THIS
    // thread's smgr/md/vfd stack (worker execution; C's cross-process fd
    // invalidity applies to cross-thread vfd caches identically) and return
    // the raw fd for op_data. `offset` is asserted against the segment
    // resolution (C: Assert(off == od->read.offset)).
    pub fn aio_smgr_reopen(
        td: types_storage::aio::PgAioTargetData,
        op: u8,
        temp_procno: i32,
        offset: u64,
    ) -> PgResult<i32>
);

seam_core::seam!(
    // md_readv_complete (md.c): shared completion for PGAIO_HCB_MD_READV.
    pub fn aio_md_readv_complete(
        ioh: u32,
        prior_result: types_storage::aio::PgAioResult,
        cb_data: u8,
    ) -> types_storage::aio::PgAioResult
);

seam_core::seam!(
    // md_readv_report (md.c): raise/log a failed md readv at `elevel`.
    pub fn aio_md_readv_report(
        result: types_storage::aio::PgAioResult,
        td: types_storage::aio::PgAioTargetData,
        elevel: types_error::ErrorLevel,
    ) -> PgResult<()>
);

seam_core::seam!(
    // smgrstartreadv (smgr.c): start an asynchronous readv of `pages.len()`
    // consecutive blocks into the given pool pages, on the CURRENT handed-out
    // AIO handle (the fd.c FileStartReadV shape). Completion callbacks see
    // the result in blocks.
    pub fn smgr_startreadv(
        rlocator: RelFileLocatorBackend,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        pages: &[*mut u8],
    ) -> PgResult<()>
);
