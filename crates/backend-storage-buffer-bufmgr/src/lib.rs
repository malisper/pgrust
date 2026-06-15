#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// `PgError` is a large owned `Err`; the un-boxed return is the project error
// contract, so accept `clippy::result_large_err` crate-wide.
#![allow(clippy::result_large_err)]

//! The shared buffer manager (`storage/buffer/bufmgr.c` + `buf_init.c`).
//!
//! F1a (this stage): the descriptor array, the page bytes, the per-buffer
//! content-lock and I/O-condvar arrays ([`mgr`]), the per-backend private pin
//! map ([`refcount`]), and the buffer-header spinlock primitives. F1a INSTALLS
//! the four header/freelist seams that unblock the buffer-support clock sweep:
//! `lock_buf_hdr` / `unlock_buf_hdr` / `buf_free_next` / `set_buf_free_next`.
//!
//! `Buffer` (an `i32`) and the descriptor / block arrays are inherited opacity:
//! a buffer is named by its 1-based id, never by a pointer. The per-buffer
//! content lock is a real [`backend_storage_lmgr_lwlock`] `LWLock` acquired
//! directly (no central content-lock seam). The pin/lock/mark/page primitives
//! and the higher-fan-in seams arrive in F1b-d; until then those seams stay
//! installed by NOBODY (panic-until-owner).

extern crate alloc;

#[path = "alloc.rs"]
mod bufalloc;
mod buf_drop;
mod buf_flush;
mod buf_lock;
mod eoxact;
mod extend;
mod mgr;
mod ops;
mod page;
mod read;
mod refcount;

pub use mgr::BufferManager;
pub use read::ReadOp;

use types_storage::storage::Buffer;

/// `LockBufHdr(GetBufferDescriptor(buf_id))` installed seam (buf_internals.h):
/// spin on the header `BM_LOCKED` bit, returning the observed state word (with
/// `BM_LOCKED` set). The buffer-support freelist clock sweep consumes it.
fn lock_buf_hdr(buf_id: i32) -> u32 {
    BufferManager::global_expect().lock_buf_hdr(buf_id as usize)
}

/// `UnlockBufHdr(desc, buf_state)` installed seam — write `buf_state` back with
/// `BM_LOCKED` cleared.
fn unlock_buf_hdr(buf_id: i32, buf_state: u32) {
    BufferManager::global_expect().unlock_buf_hdr(buf_id as usize, buf_state);
}

/// `GetBufferDescriptor(buf_id)->freeNext` installed seam.
fn buf_free_next(buf_id: i32) -> i32 {
    BufferManager::global_expect().free_next(buf_id)
}

/// `GetBufferDescriptor(buf_id)->freeNext = value` installed seam.
fn set_buf_free_next(buf_id: i32, value: i32) {
    BufferManager::global_expect().set_free_next(buf_id, value);
}

// --- F1b: pin / unpin / release / refcount seams (bufmgr.c) ---------------

/// `ReleaseBuffer(buffer)` installed seam (bufmgr.c) — drop one pin.
fn release_buffer(buf: Buffer) {
    BufferManager::global_expect()
        .ReleaseBuffer(buf)
        .expect("ReleaseBuffer: bad buffer ID");
}

/// `UnlockReleaseBuffer(buffer)` installed seam (bufmgr.c) — release the content
/// lock then the pin.
fn unlock_release_buffer(buffer: Buffer) {
    BufferManager::global_expect()
        .UnlockReleaseBuffer(buffer)
        .expect("UnlockReleaseBuffer: bad buffer ID or lock release failed");
}

/// `IncrBufferRefCount(buffer)` installed seam (bufmgr.c) — bump the local pin
/// count on an already-pinned buffer.
fn incr_buffer_ref_count(buf: Buffer) {
    BufferManager::global_expect()
        .IncrBufferRefCount(buf)
        .expect("IncrBufferRefCount: bad buffer ID or resource-owner enlarge failed");
}

/// `BufferIsPermanent(buffer)` installed seam (bufmgr.c) — is the buffer's
/// relation WAL-logged?
fn buffer_is_permanent(buf: Buffer) -> types_error::PgResult<bool> {
    BufferManager::global_expect().BufferIsPermanent(buf)
}

// --- F1c: content-lock + cleanup-lock + hint-dirty seams (bufmgr.c) -------

/// `LockBuffer(buffer, mode)` installed seam (bufmgr.c) — acquire/release the
/// buffer's content lock (direct lwlock dep).
fn lock_buffer(buffer: Buffer, mode: i32) -> types_error::PgResult<()> {
    BufferManager::global_expect().LockBuffer(buffer, mode)
}

/// `LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE)` installed seam (bufmgr.c).
fn lock_buffer_exclusive(buffer: Buffer) -> types_error::PgResult<()> {
    BufferManager::global_expect()
        .LockBuffer(buffer, types_storage::buf::BUFFER_LOCK_EXCLUSIVE)
}

/// `LockBufferForCleanup(buffer)` installed seam (bufmgr.c) — acquire a cleanup
/// (super-exclusive) lock.
fn lock_buffer_for_cleanup(buffer: Buffer) -> types_error::PgResult<()> {
    BufferManager::global_expect().LockBufferForCleanup(buffer)
}

/// `ConditionalLockBufferForCleanup(buffer)` installed seam (bufmgr.c) — try to
/// take a cleanup lock without blocking.
fn conditional_lock_buffer_for_cleanup(buffer: Buffer) -> types_error::PgResult<bool> {
    BufferManager::global_expect().ConditionalLockBufferForCleanup(buffer)
}

/// `ConditionalLockBuffer(buffer)` installed seam (bufmgr.c) — try to take the
/// buffer's exclusive content lock without blocking.
fn conditional_lock_buffer(buffer: Buffer) -> types_error::PgResult<bool> {
    BufferManager::global_expect().ConditionalLockBuffer(buffer)
}

/// `IsBufferCleanupOK(buffer)` installed seam (bufmgr.c) — does the already-held
/// exclusive lock happen to be a cleanup lock?
fn is_buffer_cleanup_ok(buffer: Buffer) -> types_error::PgResult<bool> {
    BufferManager::global_expect().IsBufferCleanupOK(buffer)
}

/// `MarkBufferDirtyHint(buffer, buffer_std)` installed seam (bufmgr.c) — mark a
/// buffer dirty for a hint-bit-only change. The seam contract is infallible
/// (the consumers call it bare); the rare `bad buffer ID` / WAL-FPI
/// `ereport(ERROR)` path becomes a loud panic here.
fn mark_buffer_dirty_hint(buf: Buffer, buffer_std: bool) {
    BufferManager::global_expect()
        .MarkBufferDirtyHint(buf, buffer_std)
        .expect("MarkBufferDirtyHint: bad buffer ID or WAL hint-FPI failed");
}

// --- F1d: mark-dirty + page access + BufferGet* accessor seams (bufmgr.c) --

/// `MarkBufferDirty(buffer)` installed seam (bufmgr.c) — mark the buffer's
/// contents dirty. Called inside a critical section; the C path only `Assert`s,
/// so the seam is infallible and the rare bad-ID / unpinned `ereport(ERROR)`
/// becomes a loud panic here.
fn mark_buffer_dirty(buffer: Buffer) {
    BufferManager::global_expect()
        .MarkBufferDirty(buffer)
        .expect("MarkBufferDirty: bad buffer ID or buffer not pinned");
}

/// `BufferGetPage(buffer)` write-access installed seam (bufpage.h): run `f` over
/// the buffer's live page bytes (`BLCKSZ`) under the caller's content lock.
fn with_buffer_page(
    buffer: Buffer,
    f: &mut dyn FnMut(&mut [u8]) -> types_error::PgResult<()>,
) -> types_error::PgResult<()> {
    BufferManager::global_expect().with_buffer_page(buffer, f)
}

/// `BufferGetBlockNumber(buffer)` installed seam (bufmgr.c). The C path only
/// `Assert`s the pin; a bad-ID becomes a loud panic.
fn buffer_get_block_number(buf: Buffer) -> types_core::primitive::BlockNumber {
    BufferManager::global_expect()
        .BufferGetBlockNumber(buf)
        .expect("BufferGetBlockNumber: bad buffer ID")
}

/// `BufferGetTag(buf, ...)` installed seam (bufmgr.c) — the relation/fork/block
/// this buffer currently holds.
fn buffer_get_tag(
    buf: Buffer,
) -> types_error::PgResult<(
    types_storage::RelFileLocator,
    types_core::primitive::ForkNumber,
    types_core::primitive::BlockNumber,
)> {
    BufferManager::global_expect().BufferGetTag(buf)
}

/// `BufferGetPage(buffer)` snapshot-copy installed seam (bufmgr.h) — an owned
/// copy of the buffer's page image in `mcx`.
fn buffer_get_page<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    buf: Buffer,
) -> types_error::PgResult<mcx::PgVec<'mcx, u8>> {
    BufferManager::global_expect().BufferGetPageOwned(mcx, buf)
}

/// `BufferGetLSNAtomic(buffer)` installed seam (bufmgr.c) — the page LSN read
/// under the header spinlock.
fn buffer_get_lsn_atomic(buf: Buffer) -> types_error::PgResult<types_core::primitive::XLogRecPtr> {
    BufferManager::global_expect().BufferGetLSNAtomic(buf)
}

/// `PageInit(BufferGetPage(buf), BLCKSZ, 0)` installed seam (bufpage.c).
fn page_init(buf: Buffer) -> types_error::PgResult<()> {
    BufferManager::global_expect().page_init(buf)
}

/// `PageSetLSN(BufferGetPage(buffer), lsn)` installed seam (bufpage.h).
fn page_set_lsn(buffer: Buffer, lsn: types_core::XLogRecPtr) -> types_error::PgResult<()> {
    BufferManager::global_expect().page_set_lsn(buffer, lsn)
}

/// `PageGetLSN(BufferGetPage(buffer))` installed seam (bufpage.h).
fn page_get_lsn(buffer: Buffer) -> types_error::PgResult<types_core::XLogRecPtr> {
    BufferManager::global_expect().page_get_lsn(buffer)
}

/// `PageIsNew(BufferGetPage(buffer))` installed seam (bufpage.h).
fn page_is_new(buffer: Buffer) -> types_error::PgResult<bool> {
    BufferManager::global_expect().page_is_new(buffer)
}

// --- F2b: relation-extension seams (bufmgr.c) -----------------------------

/// `EB_LOCK_FIRST` (bufmgr.h) — return the first extended block exclusively
/// locked.
const EB_LOCK_FIRST: u32 = 1 << 3;
/// `EB_SKIP_EXTENSION_LOCK` (bufmgr.h) — the caller already holds the
/// relation-extension lock.
const EB_SKIP_EXTENSION_LOCK: u32 = 1 << 0;
/// `EB_CREATE_FORK_IF_NEEDED` (bufmgr.h) — create the fork if absent.
const EB_CREATE_FORK_IF_NEEDED: u32 = 1 << 2;
/// `EB_CLEAR_SIZE_CACHE` (bufmgr.h) — invalidate the smgr size cache.
const EB_CLEAR_SIZE_CACHE: u32 = 1 << 4;

/// `ExtendBufferedRel(BMR_REL(rel), forkNum, NULL, EB_LOCK_FIRST |
/// EB_SKIP_EXTENSION_LOCK)` installed seam (bufmgr.c) — extend the relation fork
/// by one block, returning the new write-locked, pinned buffer (the
/// extend-the-EOF branch of `_hash_getnewbuf`).
fn extend_buffered_rel(
    rel: &types_rel::Relation,
    fork_num: types_core::primitive::ForkNumber,
) -> types_error::PgResult<Buffer> {
    BufferManager::global_expect().ExtendBufferedRel(
        rel,
        fork_num,
        false,
        EB_LOCK_FIRST | EB_SKIP_EXTENSION_LOCK,
    )
}

/// `ExtendBufferedRelTo(BMR_REL(rel), FSM_FORKNUM, NULL, EB_CREATE_FORK_IF_NEEDED
/// | EB_CLEAR_SIZE_CACHE, fsm_nblocks, RBM_ZERO_ON_ERROR)` installed seam
/// (bufmgr.c) — ensure the FSM fork is at least `fsm_nblocks` long and pin the
/// target block.
fn extend_buffered_rel_to_fsm(
    rel: &types_rel::Relation,
    fsm_nblocks: types_core::primitive::BlockNumber,
) -> types_error::PgResult<Buffer> {
    BufferManager::global_expect().ExtendBufferedRelTo(
        rel,
        types_core::primitive::ForkNumber::FSM_FORKNUM,
        false,
        EB_CREATE_FORK_IF_NEEDED | EB_CLEAR_SIZE_CACHE,
        fsm_nblocks,
        types_storage::storage::ReadBufferMode::ZeroOnError,
    )
}

/// `ExtendBufferedRelTo(BMR_REL(rel), VISIBILITYMAP_FORKNUM, NULL,
/// EB_CREATE_FORK_IF_NEEDED | EB_CLEAR_SIZE_CACHE, vm_nblocks,
/// RBM_ZERO_ON_ERROR)` installed seam (bufmgr.c) — ensure the VM fork is at
/// least `vm_nblocks` long and pin the target block.
fn extend_buffered_rel_to_vm(
    rel: &types_rel::Relation,
    vm_nblocks: types_core::primitive::BlockNumber,
) -> types_error::PgResult<Buffer> {
    BufferManager::global_expect().ExtendBufferedRelTo(
        rel,
        types_core::primitive::ForkNumber::VISIBILITYMAP_FORKNUM,
        false,
        EB_CREATE_FORK_IF_NEEDED | EB_CLEAR_SIZE_CACHE,
        vm_nblocks,
        types_storage::storage::ReadBufferMode::ZeroOnError,
    )
}

// --- F3: read-path seams (bufmgr.c) ---------------------------------------

/// `ReadBuffer(rel, blkno)` installed seam (bufmgr.c) — MAIN_FORKNUM, RBM_NORMAL,
/// no strategy.
fn read_buffer<'mcx>(
    rel: &types_rel::Relation<'mcx>,
    blkno: types_core::primitive::BlockNumber,
) -> types_error::PgResult<Buffer> {
    BufferManager::global_expect().ReadBuffer(rel, blkno)
}

/// `ReleaseAndReadBuffer(buffer, relation, blockNum)` installed seam (bufmgr.c):
/// MAIN_FORKNUM. If `buffer` is valid and already holds `blockNum` of
/// `relation`, return it as-is; else unpin (if valid) and `ReadBuffer`.
fn release_and_read_buffer<'mcx>(
    buffer: Buffer,
    relation: &types_rel::Relation<'mcx>,
    block_num: types_core::primitive::BlockNumber,
) -> types_error::PgResult<Buffer> {
    use types_storage::buf::BufferIsValid;
    let bm = BufferManager::global_expect();
    if BufferIsValid(buffer) {
        // we have pin, so it's ok to examine tag without spinlock.
        let (rlocator, fork_num, blk) = bm.BufferGetTag(buffer)?;
        if blk == block_num
            && rlocator == relation.rd_locator
            && fork_num == types_core::primitive::ForkNumber::MAIN_FORKNUM
        {
            return Ok(buffer);
        }
        bm.ReleaseBuffer(buffer)?;
    }
    bm.ReadBuffer(relation, block_num)
}

/// `ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, strategy)` installed
/// seam (bufmgr.c) — the VACUUM/bulk buffer-access-strategy read of the main
/// fork. The ring kind collapses to `has_strategy: true`.
fn read_buffer_extended<'mcx>(
    rel: &types_rel::Relation<'mcx>,
    blkno: types_core::primitive::BlockNumber,
) -> types_error::PgResult<Buffer> {
    BufferManager::global_expect().ReadBufferExtended(
        rel,
        types_core::primitive::ForkNumber::MAIN_FORKNUM,
        blkno,
        types_storage::storage::ReadBufferMode::Normal,
        true,
    )
}

/// `ReadBufferExtended(rel, forknum, blkno, RBM_NORMAL, NULL)` installed seam
/// (bufmgr.c) — an explicit fork, RBM_NORMAL, no strategy (log_newpage_range).
fn read_buffer_extended_fork<'mcx>(
    rel: &types_rel::Relation<'mcx>,
    forknum: types_core::primitive::ForkNumber,
    blkno: types_core::primitive::BlockNumber,
) -> types_error::PgResult<Buffer> {
    BufferManager::global_expect().ReadBufferExtended(
        rel,
        forknum,
        blkno,
        types_storage::storage::ReadBufferMode::Normal,
        false,
    )
}

/// `ReadBufferExtended(rel, forkNum, blkno, RBM_ZERO_AND_LOCK, NULL)` installed
/// seam (bufmgr.c) — `_hash_getinitbuf` / the existing-block branch of
/// `_hash_getnewbuf`.
fn read_buffer_zero_and_lock<'mcx>(
    rel: &types_rel::Relation<'mcx>,
    fork_num: types_core::primitive::ForkNumber,
    blkno: types_core::primitive::BlockNumber,
) -> types_error::PgResult<Buffer> {
    BufferManager::global_expect().ReadBufferExtended(
        rel,
        fork_num,
        blkno,
        types_storage::storage::ReadBufferMode::ZeroAndLock,
        false,
    )
}

/// `ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, bstrategy)` installed
/// seam (bufmgr.c) — an explicit buffer-access strategy (VACUUM:
/// `_hash_getbuf_with_strategy`). A NULL (`None`) strategy behaves like the
/// default.
fn read_buffer_with_strategy<'mcx>(
    rel: &types_rel::Relation<'mcx>,
    blkno: types_core::primitive::BlockNumber,
    strategy: types_storage::buf::BufferAccessStrategy,
) -> types_error::PgResult<Buffer> {
    let has_strategy = strategy.is_some();
    BufferManager::global_expect().ReadBufferExtended(
        rel,
        types_core::primitive::ForkNumber::MAIN_FORKNUM,
        blkno,
        types_storage::storage::ReadBufferMode::Normal,
        has_strategy,
    )
}

/// `ReadBufferExtended(rel, FSM_FORKNUM, blkno, RBM_ZERO_ON_ERROR, NULL)`
/// installed seam (bufmgr.c) — the FSM-fork read (`vm_readbuf` analog for FSM).
fn read_buffer_extended_fsm<'mcx>(
    rel: &types_rel::Relation<'mcx>,
    blkno: types_core::primitive::BlockNumber,
) -> types_error::PgResult<Buffer> {
    BufferManager::global_expect().ReadBufferExtended(
        rel,
        types_core::primitive::ForkNumber::FSM_FORKNUM,
        blkno,
        types_storage::storage::ReadBufferMode::ZeroOnError,
        false,
    )
}

/// `ReadBufferExtended(rel, VISIBILITYMAP_FORKNUM, blkno, RBM_ZERO_ON_ERROR,
/// NULL)` installed seam (bufmgr.c) — the VM-fork read (`vm_readbuf`).
fn read_buffer_extended_vm<'mcx>(
    rel: &types_rel::Relation<'mcx>,
    blkno: types_core::primitive::BlockNumber,
) -> types_error::PgResult<Buffer> {
    BufferManager::global_expect().ReadBufferExtended(
        rel,
        types_core::primitive::ForkNumber::VISIBILITYMAP_FORKNUM,
        blkno,
        types_storage::storage::ReadBufferMode::ZeroOnError,
        false,
    )
}

/// `PrefetchSharedBuffer(smgropen(rlocator, backend), forkNum, blockNum)`
/// installed seam (bufmgr.c).
fn prefetch_shared_buffer(
    rlocator: types_storage::RelFileLocator,
    backend: types_core::primitive::ProcNumber,
    fork_num: types_core::primitive::ForkNumber,
    block_num: types_core::primitive::BlockNumber,
) -> types_error::PgResult<types_storage::PrefetchBufferResult> {
    BufferManager::global_expect().PrefetchSharedBuffer(rlocator, backend, fork_num, block_num)
}

/// `XLogReadBufferExtended`'s buffer-acquisition body (xlogutils.c) — the
/// `ReadBufferWithoutRelcache` leg used by recovery redo fetchers. The
/// recent-buffer fast path + the `ExtendBufferedRelTo` missing-page branch are
/// the recovery-specific wrapping the xlogutils consumer re-applies; this seam
/// resolves the core `RBM_*` read of an already-extant block by locator, which
/// is the bufmgr/smgr operation. The RBM_NORMAL missing-page case (block beyond
/// EOF) surfaces as the read's own `Err` rather than `InvalidBuffer`, matching
/// the synchronous core's smgr read error.
fn xlog_read_buffer_extended(
    rlocator: types_storage::RelFileLocator,
    forknum: types_core::primitive::ForkNumber,
    blkno: types_core::primitive::BlockNumber,
    mode: types_storage::ReadBufferMode,
    recent_buffer: Buffer,
) -> types_error::PgResult<Buffer> {
    let bm = BufferManager::global_expect();
    // ReadRecentBuffer fast path (recovery passes the buffer it last saw).
    if recent_buffer != 0 && bm.ReadRecentBuffer(rlocator, forknum, blkno, recent_buffer)? {
        return Ok(recent_buffer);
    }
    // The relation is always treated as permanent for the redo read (recovery
    // replays WAL-logged changes); ReadBufferWithoutRelcache reads it in.
    bm.ReadBufferWithoutRelcache(rlocator, true, forknum, blkno, mode, false)
}

// --- F5: flush / drop seams (bufmgr.c) ------------------------------------

/// `FlushOneBuffer(buffer)` installed seam (bufmgr.c) — write a single pinned,
/// exclusive-locked buffer to storage (keeps an unlogged-relation init fork in
/// sync; the victim-flush in `GetVictimBuffer` also rides it).
fn flush_one_buffer(buffer: Buffer) -> types_error::PgResult<()> {
    BufferManager::global_expect().FlushOneBuffer(buffer)
}

/// `DropRelationBuffers(smgr_reln, forkNum, nforks, firstDelBlock)` installed
/// seam (bufmgr.c) — drop one relation's buffers at/after the per-fork
/// truncation point without writing them (`smgrtruncate`).
fn drop_relation_buffers(
    smgr_reln: types_storage::RelFileLocatorBackend,
    forknum: &[types_core::primitive::ForkNumber],
    nblocks: &[types_core::primitive::BlockNumber],
) -> types_error::PgResult<()> {
    BufferManager::global_expect().DropRelationBuffers(smgr_reln, forknum, nblocks)
}

/// `DropRelationsAllBuffers(smgr_reln, nlocators)` installed seam (bufmgr.c) —
/// drop every buffer of all the given relations without writing them
/// (`smgrdounlinkall`).
fn drop_relations_all_buffers(
    smgr_reln: &[types_storage::RelFileLocatorBackend],
) -> types_error::PgResult<()> {
    BufferManager::global_expect().DropRelationsAllBuffers(smgr_reln)
}

/// `FlushRelationsAllBuffers(smgrs, nrels)` installed seam (bufmgr.c) — write
/// every dirty buffer of all the given relations to the kernel
/// (`smgrdosyncall`). The C `SMgrRelation` array is flattened to a
/// `RelFileLocatorBackend` slice; this shared core flushes by the unbacked
/// relfilelocator (temp relations don't reach here).
fn flush_relations_all_buffers(
    smgrs: &[types_storage::RelFileLocatorBackend],
) -> types_error::PgResult<()> {
    let locators: alloc::vec::Vec<types_storage::RelFileLocator> =
        smgrs.iter().map(|s| s.locator).collect();
    BufferManager::global_expect().FlushRelationsAllBuffers(&locators)
}

// --- lifecycle + relation-size seams (bufmgr.c) ---------------------------

/// `UnlockBuffers()` installed seam (bufmgr.c) — release the in-progress
/// PIN_COUNT request on the abort/cleanup path (xact / standby consumers).
fn unlock_buffers() {
    BufferManager::global_expect().UnlockBuffers();
}

/// `HoldingBufferPinThatDelaysRecovery()` installed seam (bufmgr.c) — does this
/// backend hold the buffer pin the Startup process is waiting on?
fn holding_buffer_pin_that_delays_recovery() -> bool {
    BufferManager::global_expect().HoldingBufferPinThatDelaysRecovery()
}

/// `AtEOXact_Buffers(isCommit)` installed seam (bufmgr.c) — end-of-transaction
/// buffer-pin leak check (xact commit/abort consumer).
fn at_eoxact_buffers(is_commit: bool) {
    BufferManager::global_expect()
        .AtEOXact_Buffers(is_commit)
        .expect("AtEOXact_Buffers: buffer-pin leak");
}

/// `InitBufferManagerAccess()` installed seam (bufmgr.c) — set up this backend's
/// private pin map and register the process-exit cleanup (postinit consumer).
fn init_buffer_manager_access() -> types_error::PgResult<()> {
    BufferManager::global_expect().InitBufferManagerAccess()
}

/// `RelationGetNumberOfBlocksInFork(relation, forkNum)` installed seam
/// (bufmgr.c) — the current block count of a relation fork
/// (hash / nbtree / table-AM consumers).
fn relation_get_number_of_blocks_in_fork(
    relation: &types_rel::Relation<'_>,
    fork_num: types_core::primitive::ForkNumber,
) -> types_error::PgResult<types_core::primitive::BlockNumber> {
    BufferManager::global_expect().RelationGetNumberOfBlocksInFork(relation, fork_num)
}

/// `(FSMPage) PageGetContents(BufferGetPage(buf))` installed seam (bufmgr.c) —
/// materialise the FSM page body as an owned [`types_fsm::FSMPageData`]
/// (freespace consumer).
fn fsm_buffer_get_page(buf: Buffer) -> types_error::PgResult<types_fsm::FSMPageData> {
    BufferManager::global_expect().fsm_buffer_get_page(buf)
}

/// Store a mutated FSM page body back into the buffer's page (bufmgr.c)
/// installed seam (freespace consumer).
fn fsm_buffer_set_page(
    buf: Buffer,
    page: types_fsm::FSMPageData,
) -> types_error::PgResult<()> {
    BufferManager::global_expect().fsm_buffer_set_page(buf, page)
}

/// `PageSetChecksumInplace(page, blkno); smgrextend(RelationGetSmgr(rel),
/// forkNum, blkno, page, skipFsync)` installed seam (bufmgr/smgr) — the
/// `_hash_alloc_buckets` tail that stamps a checksum into the in-memory page and
/// writes it past the current EOF (hash consumer). smgr is a direct dep.
fn smgr_extend_page(
    rlocator: types_storage::RelFileLocator,
    fork_num: types_core::primitive::ForkNumber,
    blkno: types_core::primitive::BlockNumber,
    page: &mut [u8],
    skip_fsync: bool,
) -> types_error::PgResult<()> {
    // PageSetChecksumInplace(page, blkno).
    {
        let mut p = backend_storage_page::PageMut::new(page)
            .expect("smgr_extend_page: page is BLCKSZ");
        backend_storage_page::PageSetChecksumInplace(&mut p, blkno);
    }
    // smgrextend(RelationGetSmgr(rel), forkNum, blkno, page, skipFsync). The
    // page write is keyed by the unbacked RelFileLocatorBackend; the hash AM
    // only reaches this for permanent relations.
    backend_storage_smgr_smgr::smgrextend(
        types_storage::RelFileLocatorBackend {
            locator: rlocator,
            backend: types_core::primitive::INVALID_PROC_NUMBER,
        },
        fork_num,
        blkno,
        page,
        skip_fsync,
    )
}

/// Install this crate's inward seams. F1a installs the four header/freelist
/// seams that unblock the buffer-support freelist clock sweep; F1b installs the
/// pin/unpin/release/refcount seams (`release_buffer` / `unlock_release_buffer`
/// / `incr_buffer_ref_count` / `buffer_is_permanent`). The lock/mark/page seams
/// arrive in F1c-d.
pub fn init_seams() {
    backend_storage_buffer_bufmgr_seams::lock_buf_hdr::set(lock_buf_hdr);
    backend_storage_buffer_bufmgr_seams::unlock_buf_hdr::set(unlock_buf_hdr);
    backend_storage_buffer_bufmgr_seams::buf_free_next::set(buf_free_next);
    backend_storage_buffer_bufmgr_seams::set_buf_free_next::set(set_buf_free_next);
    // F1b
    backend_storage_buffer_bufmgr_seams::release_buffer::set(release_buffer);
    backend_storage_buffer_bufmgr_seams::unlock_release_buffer::set(unlock_release_buffer);
    backend_storage_buffer_bufmgr_seams::incr_buffer_ref_count::set(incr_buffer_ref_count);
    backend_storage_buffer_bufmgr_seams::buffer_is_permanent::set(buffer_is_permanent);
    // F1c
    backend_storage_buffer_bufmgr_seams::lock_buffer::set(lock_buffer);
    backend_storage_buffer_bufmgr_seams::lock_buffer_exclusive::set(lock_buffer_exclusive);
    backend_storage_buffer_bufmgr_seams::lock_buffer_for_cleanup::set(lock_buffer_for_cleanup);
    backend_storage_buffer_bufmgr_seams::conditional_lock_buffer_for_cleanup::set(
        conditional_lock_buffer_for_cleanup,
    );
    backend_storage_buffer_bufmgr_seams::conditional_lock_buffer::set(conditional_lock_buffer);
    backend_storage_buffer_bufmgr_seams::is_buffer_cleanup_ok::set(is_buffer_cleanup_ok);
    backend_storage_buffer_bufmgr_seams::mark_buffer_dirty_hint::set(mark_buffer_dirty_hint);
    // F1d
    backend_storage_buffer_bufmgr_seams::mark_buffer_dirty::set(mark_buffer_dirty);
    backend_storage_buffer_bufmgr_seams::with_buffer_page::set(with_buffer_page);
    backend_storage_buffer_bufmgr_seams::buffer_get_block_number::set(buffer_get_block_number);
    backend_storage_buffer_bufmgr_seams::buffer_get_tag::set(buffer_get_tag);
    backend_storage_buffer_bufmgr_seams::buffer_get_page::set(buffer_get_page);
    backend_storage_buffer_bufmgr_seams::buffer_get_lsn_atomic::set(buffer_get_lsn_atomic);
    backend_storage_buffer_bufmgr_seams::page_init::set(page_init);
    backend_storage_buffer_bufmgr_seams::page_set_lsn::set(page_set_lsn);
    backend_storage_buffer_bufmgr_seams::page_get_lsn::set(page_get_lsn);
    backend_storage_buffer_bufmgr_seams::page_is_new::set(page_is_new);
    // F2b: relation-extension entry points.
    backend_storage_buffer_bufmgr_seams::extend_buffered_rel::set(extend_buffered_rel);
    backend_storage_buffer_bufmgr_seams::extend_buffered_rel_to_fsm::set(extend_buffered_rel_to_fsm);
    backend_storage_buffer_bufmgr_seams::extend_buffered_rel_to_vm::set(extend_buffered_rel_to_vm);
    // F2b: the relation-extension I/O accounting (stats-only; no-op installs,
    // same posture as F1's `count_buffer_dirtied`, until pgstat ports).
    backend_storage_buffer_bufmgr_seams::count_buffer_write::set(|| {});
    backend_storage_buffer_bufmgr_seams::count_io_op_extend::set(|_cnt, _bytes| {});
    // F3: the read-path entry points (the synchronous single-block core; the
    // explicit StartReadBuffers/WaitReadBuffers pipeline is a public API on
    // BufferManager + rides the panic-until-owner aio-handle seams).
    backend_storage_buffer_bufmgr_seams::read_buffer::set(read_buffer);
    backend_storage_buffer_bufmgr_seams::release_and_read_buffer::set(release_and_read_buffer);
    backend_storage_buffer_bufmgr_seams::read_buffer_extended::set(read_buffer_extended);
    backend_storage_buffer_bufmgr_seams::read_buffer_extended_fork::set(read_buffer_extended_fork);
    backend_storage_buffer_bufmgr_seams::read_buffer_zero_and_lock::set(read_buffer_zero_and_lock);
    backend_storage_buffer_bufmgr_seams::read_buffer_with_strategy::set(read_buffer_with_strategy);
    backend_storage_buffer_bufmgr_seams::read_buffer_extended_fsm::set(read_buffer_extended_fsm);
    backend_storage_buffer_bufmgr_seams::read_buffer_extended_vm::set(read_buffer_extended_vm);
    backend_storage_buffer_bufmgr_seams::prefetch_shared_buffer::set(prefetch_shared_buffer);
    backend_storage_buffer_bufmgr_seams::xlog_read_buffer_extended::set(xlog_read_buffer_extended);
    // F5: flush / drop entry points (the in-crate write core + relation/db drop
    // + flush sweeps; the disk write rides the landed smgr).
    backend_storage_buffer_bufmgr_seams::flush_one_buffer::set(flush_one_buffer);
    backend_storage_buffer_bufmgr_seams::drop_relation_buffers::set(drop_relation_buffers);
    backend_storage_buffer_bufmgr_seams::drop_relations_all_buffers::set(drop_relations_all_buffers);
    backend_storage_buffer_bufmgr_seams::flush_relations_all_buffers::set(
        flush_relations_all_buffers,
    );
    // F5: the per-backend checkpoint/bgwriter statistics counters — no-op
    // installs (behaviour-neutral, same posture as F2's count_buffer_write /
    // count_io_op_extend until the pgstat owner ports).
    backend_storage_buffer_bufmgr_seams::count_checkpoint_buffer_written::set(|| {});
    backend_storage_buffer_bufmgr_seams::report_bgwriter_buf_alloc::set(|_| {});
    backend_storage_buffer_bufmgr_seams::count_bgwriter_maxwritten_clean::set(|| {});
    backend_storage_buffer_bufmgr_seams::count_bgwriter_buffer_written_clean::set(|| {});
    // Lifecycle + relation-size + FSM-page + smgr-extend seams: bufmgr OWNS all
    // of these (the local-buffer leg of AtEOXact/AtProcExit + the temp-relation
    // dispatch stay panic-until-owner outward seams installed by the local-buffer
    // owner; log_newpage is installed by its xloginsert owner).
    backend_storage_buffer_bufmgr_seams::unlock_buffers::set(unlock_buffers);
    backend_storage_buffer_bufmgr_seams::holding_buffer_pin_that_delays_recovery::set(
        holding_buffer_pin_that_delays_recovery,
    );
    backend_storage_buffer_bufmgr_seams::at_eoxact_buffers::set(at_eoxact_buffers);
    backend_storage_buffer_bufmgr_seams::init_buffer_manager_access::set(
        init_buffer_manager_access,
    );
    backend_storage_buffer_bufmgr_seams::relation_get_number_of_blocks_in_fork::set(
        relation_get_number_of_blocks_in_fork,
    );
    backend_storage_buffer_bufmgr_seams::fsm_buffer_get_page::set(fsm_buffer_get_page);
    backend_storage_buffer_bufmgr_seams::fsm_buffer_set_page::set(fsm_buffer_set_page);
    backend_storage_buffer_bufmgr_seams::smgr_extend_page::set(smgr_extend_page);
}
