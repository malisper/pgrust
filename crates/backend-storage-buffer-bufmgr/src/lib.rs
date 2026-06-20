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

mod buf_aio;
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

pub use buf_flush::{writeback_context_init, BgBufferSyncState, WritebackContext};
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

/// `ResOwnerReleaseBufferPin(res)` installed seam (bufmgr.c:6555) — release a
/// leaked buffer pin the resource owner found during release, without touching
/// the (already-being-released) owner.
fn release_buffer_pin(buf: Buffer) -> types_error::PgResult<()> {
    BufferManager::global_expect().ResOwnerReleaseBufferPin(buf)
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

/// `ExtendBufferedRel(BMR_REL(rel), MAIN_FORKNUM, NULL, EB_LOCK_FIRST)` installed
/// seam (bufmgr.c) — extend MAIN_FORKNUM by one block, returning the new
/// write-locked, pinned buffer. Unlike [`extend_buffered_rel`], the
/// relation-extension lock IS taken (no `EB_SKIP_EXTENSION_LOCK`); this is the
/// `_bt_allocbuf` / new-page nbtree variant.
fn extend_buffered_rel_locked(
    rel: &types_rel::Relation,
    fork_num: types_core::primitive::ForkNumber,
) -> types_error::PgResult<Buffer> {
    BufferManager::global_expect().ExtendBufferedRel(rel, fork_num, false, EB_LOCK_FIRST)
}

/// `ExtendBufferedRelBy(BMR_REL(rel), MAIN_FORKNUM, strategy, EB_LOCK_FIRST,
/// extend_by, victim_buffers, &extend_by)` installed seam (bufmgr.c) — hio.c's
/// `RelationAddBlocks` multi-page extension. `extend_by` is capped at
/// `MAX_BUFFERS_TO_EXTEND_BY` (64) by the caller, so the victim-buffer slice is
/// sized accordingly.
fn extend_buffered_rel_by_main(
    rel: &types_rel::Relation,
    has_strategy: bool,
    extend_by: u32,
) -> types_error::PgResult<types_storage::buf::ExtendedRelation> {
    // MAX_BUFFERS_TO_EXTEND_BY (hio.c) — the caller's hard cap on extend_by.
    const MAX_BUFFERS_TO_EXTEND_BY: usize = 64;
    let mut buffers = [Buffer::default(); MAX_BUFFERS_TO_EXTEND_BY];
    let mut extended_by: u32 = 0;
    let first_block = BufferManager::global_expect().ExtendBufferedRelBy(
        rel,
        types_core::primitive::ForkNumber::MAIN_FORKNUM,
        has_strategy,
        EB_LOCK_FIRST,
        extend_by,
        &mut buffers[..extend_by as usize],
        &mut extended_by,
    )?;
    Ok(types_storage::buf::ExtendedRelation {
        first_block,
        victim_buffers: buffers[..extended_by as usize].to_vec(),
        extended_by,
    })
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

/// `ReadBufferWithoutRelcache(rlocator, forkNum, blockNum, mode, strategy,
/// permanent)` installed seam (bufmgr.c) — read a block for a relation
/// identified only by its `RelFileLocator`. The createdb cross-database scan
/// reaches this.
fn read_buffer_without_relcache(
    rlocator: types_storage::RelFileLocator,
    forknum: types_core::primitive::ForkNumber,
    blocknum: types_core::primitive::BlockNumber,
    mode: types_storage::storage::ReadBufferMode,
    has_strategy: bool,
    permanent: bool,
) -> types_error::PgResult<Buffer> {
    let _ = has_strategy;
    BufferManager::global_expect().ReadBufferWithoutRelcache(
        rlocator, permanent, forknum, blocknum, mode, has_strategy,
    )
}

/// `RelationCopyStorageUsingBuffer(srclocator, dstlocator, forkNum, permanent)`
/// installed seam (bufmgr.c) — the buffered per-fork copy engine of
/// `CreateAndCopyRelationData` (createdb WAL_LOG strategy).
fn relation_copy_storage_using_buffer(
    srclocator: types_storage::RelFileLocator,
    dstlocator: types_storage::RelFileLocator,
    forknum: types_core::primitive::ForkNumber,
    permanent: bool,
) -> types_error::PgResult<()> {
    BufferManager::global_expect().RelationCopyStorageUsingBuffer(
        srclocator, dstlocator, forknum, permanent,
    )
}

/// `ReadBufferExtended(rel, MAIN_FORKNUM, blkno, mode, strategy)` (bufmgr.c) —
/// the runtime-mode form for hio.c's `ReadBufferBI`.
fn read_buffer_extended_mode<'mcx>(
    rel: &types_rel::Relation<'mcx>,
    blkno: types_core::primitive::BlockNumber,
    mode: types_storage::storage::ReadBufferMode,
    has_strategy: bool,
) -> types_error::PgResult<Buffer> {
    BufferManager::global_expect().ReadBufferExtended(
        rel,
        types_core::primitive::ForkNumber::MAIN_FORKNUM,
        blkno,
        mode,
        has_strategy,
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

/// `FlushRelationBuffers(rel)` installed seam (bufmgr.c) — flush every dirty
/// buffer of the one relation. The owned relcache mirror carries the relation's
/// `rd_locator`, which is all `FlushRelationBuffers` needs.
fn flush_relation_buffers(rel: &types_rel::Relation) -> types_error::PgResult<()> {
    BufferManager::global_expect().FlushRelationBuffers(&rel.rd_locator)
}

/// `FlushDatabaseBuffers(dbid)` installed seam (bufmgr.c:5304) — flush every
/// dirty buffer of one database.
fn flush_database_buffers(dbid: types_core::Oid) -> types_error::PgResult<()> {
    BufferManager::global_expect().FlushDatabaseBuffers(dbid)
}

/// `DropDatabaseBuffers(dbid)` installed seam (bufmgr.c:4888) — drop (without
/// writing) every shared-buffer page of one database, for `dropdb` /
/// `dbase_redo` XLOG_DBASE_DROP cleanup.
fn drop_database_buffers(dbid: types_core::Oid) -> types_error::PgResult<()> {
    BufferManager::global_expect().DropDatabaseBuffers(dbid)
}

/// `ResOwnerReleaseBufferIO(res)` installed seam (bufmgr.c:6539) — abort a
/// leaked in-progress buffer I/O (`AbortBufferIO`) the resource owner found
/// during release, without removing the I/O from the (being-released) owner.
fn release_buffer_io(buffer: types_storage::storage::Buffer) -> types_error::PgResult<()> {
    BufferManager::global_expect().abort_buffer_io(buffer)
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
    // The AIO buffer-readv completion callbacks + synchronous read syscall this
    // crate owns (bufmgr.c buffer_readv_complete / buffer_stage_common), installed
    // into the aio-completion seams the AIO engine dispatches through.
    buf_aio::init_seams();

    backend_storage_buffer_bufmgr_seams::lock_buf_hdr::set(lock_buf_hdr);
    backend_storage_buffer_bufmgr_seams::unlock_buf_hdr::set(unlock_buf_hdr);
    backend_storage_buffer_bufmgr_seams::buf_free_next::set(buf_free_next);
    backend_storage_buffer_bufmgr_seams::set_buf_free_next::set(set_buf_free_next);
    // F1b
    backend_storage_buffer_bufmgr_seams::release_buffer::set(release_buffer);
    backend_storage_buffer_bufmgr_seams::release_buffer_pin::set(release_buffer_pin);
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
    // `pgBufferUsage.shared_blks_dirtied++` (bufmgr.c:2989 / :5555), fired from
    // MarkBufferDirty / MarkBufferDirtyHint when a previously-clean shared buffer
    // is first dirtied. Stats-only; no-op install (same posture as the F2b
    // `count_buffer_write` / `count_io_op_extend` stubs below) until pgstat ports.
    backend_storage_buffer_bufmgr_seams::count_buffer_dirtied::set(|| {});
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
    backend_storage_buffer_bufmgr_seams::extend_buffered_rel_locked::set(extend_buffered_rel_locked);
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
    backend_storage_buffer_bufmgr_seams::read_buffer_extended_mode::set(read_buffer_extended_mode);
    backend_storage_buffer_bufmgr_seams::extend_buffered_rel_by_main::set(extend_buffered_rel_by_main);
    backend_storage_buffer_bufmgr_seams::read_buffer_extended_fork::set(read_buffer_extended_fork);
    backend_storage_buffer_bufmgr_seams::read_buffer_zero_and_lock::set(read_buffer_zero_and_lock);
    backend_storage_buffer_bufmgr_seams::read_buffer_with_strategy::set(read_buffer_with_strategy);
    backend_storage_buffer_bufmgr_seams::read_buffer_extended_fsm::set(read_buffer_extended_fsm);
    backend_storage_buffer_bufmgr_seams::read_buffer_extended_vm::set(read_buffer_extended_vm);
    backend_storage_buffer_bufmgr_seams::prefetch_shared_buffer::set(prefetch_shared_buffer);
    backend_storage_buffer_bufmgr_seams::xlog_read_buffer_extended::set(xlog_read_buffer_extended);
    backend_storage_buffer_bufmgr_seams::read_buffer_without_relcache::set(
        read_buffer_without_relcache,
    );
    backend_storage_buffer_bufmgr_seams::relation_copy_storage_using_buffer::set(
        relation_copy_storage_using_buffer,
    );
    // F5: flush / drop entry points (the in-crate write core + relation/db drop
    // + flush sweeps; the disk write rides the landed smgr).
    backend_storage_buffer_bufmgr_seams::flush_one_buffer::set(flush_one_buffer);
    backend_storage_buffer_bufmgr_seams::drop_relation_buffers::set(drop_relation_buffers);
    backend_storage_buffer_bufmgr_seams::drop_relations_all_buffers::set(drop_relations_all_buffers);
    backend_storage_buffer_bufmgr_seams::flush_relations_all_buffers::set(
        flush_relations_all_buffers,
    );
    backend_storage_buffer_bufmgr_seams::flush_relation_buffers::set(flush_relation_buffers);
    backend_storage_buffer_bufmgr_seams::flush_database_buffers::set(flush_database_buffers);
    backend_storage_buffer_bufmgr_seams::drop_database_buffers::set(drop_database_buffers);
    backend_storage_buffer_bufmgr_seams::release_buffer_io::set(release_buffer_io);
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
    // Shared-memory sizing + placement (buf_init.c) — the ipci.c
    // `CalculateShmemSize` / `CreateOrAttachShmemStructs` entry points.
    backend_storage_buffer_bufmgr_seams::buffer_manager_shmem_size::set(
        mgr::BufferManagerShmemSize,
    );
    backend_storage_buffer_bufmgr_seams::buffer_manager_shmem_init::set(
        mgr::BufferManagerShmemInit,
    );

    // GUC var accessors. Each of these globals is a plain process-global GUC
    // variable defined in bufmgr.c (none is read from the ControlFile), so the
    // faithful read is `vars::<name>.read()` off the live GUC slot. These getter
    // seams exist because the contract boundary the consumer crosses
    // (`GetAccessStrategy(btype)` ring sizing in buffer-support, the read_stream
    // builder in aio, and the bgwriter/checkpoint flush loops here) cannot carry
    // these process-global knobs as parameters.
    use backend_utils_misc_guc_tables::vars;
    // The effective `io_combine_limit` global (bufmgr.c) is the clamped value
    // `Min(io_combine_limit_guc, io_max_combine_limit)` maintained by the GUC
    // assign-hooks (variable.c). Read/write it through the dedicated effective
    // backing cell rather than the raw `io_combine_limit_guc` slot. It is seeded
    // to its boot default and updated by `set_io_combine_limit` whenever either
    // contributing GUC is assigned.
    backend_storage_buffer_bufmgr_seams::io_combine_limit::set(guc_vars::effective_io_combine_limit_get);
    backend_storage_buffer_bufmgr_seams::set_io_combine_limit::set(
        guc_vars::effective_io_combine_limit_set,
    );
    backend_storage_buffer_bufmgr_seams::effective_io_concurrency::set(|| {
        vars::effective_io_concurrency.read()
    });
    backend_storage_buffer_bufmgr_seams::maintenance_io_concurrency::set(|| {
        vars::maintenance_io_concurrency.read()
    });
    // `io_method == IOMETHOD_SYNC` (aio.c GUC) — read off the live `io_method`
    // enum slot (backed by aio-methods at boot), mirroring C's global compare.
    backend_storage_buffer_bufmgr_seams::io_method_sync::set(|| {
        vars::io_method.read() == backend_utils_misc_guc_tables::consts::IOMETHOD_SYNC
    });
    backend_storage_buffer_bufmgr_seams::bgwriter_lru_maxpages::set(|| {
        vars::bgwriter_lru_maxpages.read()
    });
    backend_storage_buffer_bufmgr_seams::bgwriter_lru_multiplier::set(|| {
        vars::bgwriter_lru_multiplier.read()
    });
    backend_storage_buffer_bufmgr_seams::checkpoint_flush_after::set(|| {
        vars::checkpoint_flush_after.read()
    });
    backend_storage_buffer_bufmgr_seams::bgwriter_flush_after::set(|| {
        vars::bgwriter_flush_after.read()
    });

    // --- lazy-vacuum driver's BufferGetPage(buffer)-over-page + buffer-read
    //     seams (vacuumlazy.c, re-signed off `Buffer`/`&Relation` in
    //     vacuumlazy-seams). The buffer manager owns the buffer→page mapping. ---
    use backend_access_heap_vacuumlazy_seams as vx;
    vx::read_buffer_extended::set(vac_read_buffer_extended);
    vx::prefetch_buffer::set(vac_prefetch_buffer);
    vx::check_buffer_is_pinned_once::set(|buffer| {
        BufferManager::global_expect().CheckBufferIsPinnedOnce(buffer)
    });
    vx::page_get_heap_free_space::set(|buffer| {
        BufferManager::global_expect().page_get_heap_free_space(buffer)
    });
    vx::page_is_new::set(|buffer| BufferManager::global_expect().page_is_new(buffer));
    vx::page_is_empty::set(|buffer| BufferManager::global_expect().page_is_empty(buffer));
    vx::page_is_all_visible::set(|buffer| {
        BufferManager::global_expect().page_is_all_visible(buffer)
    });
    vx::page_set_all_visible::set(|buffer| {
        BufferManager::global_expect().page_set_all_visible(buffer)
    });
    vx::page_clear_all_visible::set(|buffer| {
        BufferManager::global_expect().page_clear_all_visible(buffer)
    });
    vx::page_lsn_is_invalid::set(|buffer| {
        BufferManager::global_expect().page_lsn_is_invalid(buffer)
    });
    vx::page_get_max_offset_number::set(|buffer| {
        BufferManager::global_expect().page_get_max_offset_number(buffer)
    });
    vx::page_truncate_line_pointer_array::set(|buffer| {
        BufferManager::global_expect().page_truncate_line_pointer_array(buffer)
    });
    vx::page_item_id_state::set(|buffer, offnum| {
        BufferManager::global_expect().page_item_id_state(buffer, offnum)
    });
    vx::page_item_id_set_unused::set(|buffer, offnum| {
        BufferManager::global_expect().page_item_id_set_unused(buffer, offnum)
    });

    // Install the backing storage for the GUC int/real variables defined as
    // bufmgr.c globals (the `.read()` calls above resolve through these). The
    // GUC bootstrap writes each boot_val through the accessor at startup.
    guc_vars::install();
}

/// `ReadBufferExtended(rel, fork, blkno, RBM_NORMAL, bstrategy)` for the lazy
/// vacuum driver's read-stream block fetch (vacuumlazy-seams signature off
/// `&Relation` + `fork: i32` + an explicit strategy).
fn vac_read_buffer_extended<'mcx>(
    rel: &types_rel::Relation<'mcx>,
    fork: i32,
    blkno: types_core::primitive::BlockNumber,
    strategy: types_storage::buf::BufferAccessStrategy,
) -> types_error::PgResult<Buffer> {
    let forknum = types_core::primitive::ForkNumber::from_i32(fork)
        .expect("vacuumlazy read_buffer_extended: invalid fork number");
    BufferManager::global_expect().ReadBufferExtended(
        rel,
        forknum,
        blkno,
        types_storage::storage::ReadBufferMode::Normal,
        strategy.is_some(),
    )
}

/// `PrefetchBuffer(rel, fork, blkno)` for the lazy vacuum truncation pre-read.
fn vac_prefetch_buffer<'mcx>(
    rel: &types_rel::Relation<'mcx>,
    fork: i32,
    blkno: types_core::primitive::BlockNumber,
) -> types_error::PgResult<()> {
    let forknum = types_core::primitive::ForkNumber::from_i32(fork)
        .expect("vacuumlazy prefetch_buffer: invalid fork number");
    BufferManager::global_expect()
        .PrefetchBuffer(rel, forknum, blkno)
        .map(|_| ())
}

/// Backing storage for the GUC variables that are file-scope globals in
/// bufmgr.c (`effective_io_concurrency`, `maintenance_io_concurrency`,
/// `io_combine_limit`, `bgwriter_lru_maxpages`, `bgwriter_lru_multiplier`,
/// `checkpoint_flush_after`, `bgwriter_flush_after`). Each is a per-backend
/// `thread_local` cell exposed through `GucVarAccessors`, mirroring C's
/// `conf->variable` pointer into the global.
mod guc_vars {
    use backend_utils_misc_guc_tables::{vars, GucVarAccessors};
    use std::cell::Cell;

    macro_rules! int_guc {
        ($cell:ident, $get:ident, $set:ident, $default:expr) => {
            thread_local! {
                static $cell: Cell<i32> = const { Cell::new($default) };
            }
            fn $get() -> i32 {
                $cell.with(Cell::get)
            }
            fn $set(value: i32) {
                $cell.with(|c| c.set(value));
            }
        };
    }

    macro_rules! bool_guc {
        ($cell:ident, $get:ident, $set:ident, $default:expr) => {
            thread_local! {
                static $cell: Cell<bool> = const { Cell::new($default) };
            }
            fn $get() -> bool {
                $cell.with(Cell::get)
            }
            fn $set(value: bool) {
                $cell.with(|c| c.set(value));
            }
        };
    }

    int_guc!(EFFECTIVE_IO_CONCURRENCY, eff_get, eff_set, 16);
    int_guc!(MAINTENANCE_IO_CONCURRENCY, maint_get, maint_set, 16);
    // bufmgr.c keeps two distinct globals: `io_combine_limit_guc` is the raw GUC
    // variable backing the `io_combine_limit` setting, while `io_combine_limit`
    // is the *effective* derived value `Min(io_combine_limit_guc,
    // io_max_combine_limit)` maintained by the assign-hooks. They need separate
    // storage so the GUC framework's write-through of the raw setting does not
    // clobber the clamped effective value.
    int_guc!(IO_COMBINE_LIMIT_GUC, iocl_guc_get, iocl_guc_set, 16);
    int_guc!(IO_COMBINE_LIMIT, iocl_get, iocl_set, 16);
    int_guc!(BGWRITER_LRU_MAXPAGES, blm_get, blm_set, 100);
    int_guc!(CHECKPOINT_FLUSH_AFTER, cfa_get, cfa_set, 0);
    int_guc!(BGWRITER_FLUSH_AFTER, bfa_get, bfa_set, 0);
    // bufmgr.c: `int backend_flush_after = DEFAULT_BACKEND_FLUSH_AFTER` (0),
    // `int io_max_combine_limit = DEFAULT_IO_COMBINE_LIMIT` (128KB/BLCKSZ = 16).
    int_guc!(BACKEND_FLUSH_AFTER, bkfa_get, bkfa_set, 0);
    int_guc!(IO_MAX_COMBINE_LIMIT, iomcl_get, iomcl_set, 16);
    // bufmgr.c: `bool track_io_timing = false`, `bool zero_damaged_pages = false`.
    bool_guc!(TRACK_IO_TIMING, tit_get, tit_set, false);
    bool_guc!(ZERO_DAMAGED_PAGES, zdp_get, zdp_set, false);

    thread_local! {
        static BGWRITER_LRU_MULTIPLIER: Cell<f64> = const { Cell::new(2.0) };
    }
    fn blmul_get() -> f64 {
        BGWRITER_LRU_MULTIPLIER.with(Cell::get)
    }
    fn blmul_set(value: f64) {
        BGWRITER_LRU_MULTIPLIER.with(|c| c.set(value));
    }

    /// Read the effective `io_combine_limit` global (bufmgr.c) — the clamped
    /// `Min(io_combine_limit_guc, io_max_combine_limit)`.
    pub(super) fn effective_io_combine_limit_get() -> i32 {
        iocl_get()
    }
    /// Write the effective `io_combine_limit` global (bufmgr.c). Called by the
    /// GUC assign-hooks via the `set_io_combine_limit` seam.
    pub(super) fn effective_io_combine_limit_set(value: i32) {
        iocl_set(value);
    }

    pub(super) fn install() {
        vars::effective_io_concurrency.install(GucVarAccessors {
            get: eff_get,
            set: eff_set,
        });
        vars::maintenance_io_concurrency.install(GucVarAccessors {
            get: maint_get,
            set: maint_set,
        });
        vars::io_combine_limit_guc.install(GucVarAccessors {
            get: iocl_guc_get,
            set: iocl_guc_set,
        });
        vars::bgwriter_lru_maxpages.install(GucVarAccessors {
            get: blm_get,
            set: blm_set,
        });
        vars::checkpoint_flush_after.install(GucVarAccessors {
            get: cfa_get,
            set: cfa_set,
        });
        vars::bgwriter_flush_after.install(GucVarAccessors {
            get: bfa_get,
            set: bfa_set,
        });
        vars::bgwriter_lru_multiplier.install(GucVarAccessors {
            get: blmul_get,
            set: blmul_set,
        });
        vars::track_io_timing.install(GucVarAccessors {
            get: tit_get,
            set: tit_set,
        });
        vars::zero_damaged_pages.install(GucVarAccessors {
            get: zdp_get,
            set: zdp_set,
        });
        vars::backend_flush_after.install(GucVarAccessors {
            get: bkfa_get,
            set: bkfa_set,
        });
        vars::io_max_combine_limit.install(GucVarAccessors {
            get: iomcl_get,
            set: iomcl_set,
        });
    }
}
