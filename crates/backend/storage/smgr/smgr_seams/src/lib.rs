use types_core::{BlockNumber, ForkNumber};
use types_error::PgResult;
use types_storage::RelFileLocatorBackend;

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
    // (smgr.c); buffer is exactly BLCKSZ.
    pub fn smgr_write(
        rlocator: RelFileLocatorBackend,
        forknum: ForkNumber,
        blocknum: BlockNumber,
        buffer: &[u8],
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
