#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// `PgError` is a large owned `Err`; the un-boxed return is the project error
// contract, so accept `clippy::result_large_err` crate-wide.
#![allow(clippy::result_large_err)]

//! Buffer-pool support — the buffer manager's helper modules:
//!
//!  * `buf_table.c` — the shared buffer lookup hash ([`buf_table`]),
//!  * `freelist.c` — the clock-sweep victim selection and free list
//!    ([`freelist`]) plus the backend-private `BufferAccessStrategy` ring
//!    ([`strategy`]),
//!  * `localbuf.c` — the temp-relation local buffer manager ([`localbuf`]).
//!
//! These provide the substrate the main buffer manager
//! (`backend-storage-buffer-bufmgr`) sits on.
//!
//! `buf_table.c` and `freelist.c`'s control block are SHARED-MEMORY
//! subsystems. The shared structs are modeled field-for-field as owned Rust
//! values with the real spinlock + atomic semantics; the SUBSTRATE they rest on
//! — the shared-memory allocator (`ShmemInitStruct`), the bufmgr-owned
//! per-buffer header array (`LockBufHdr`/`UnlockBufHdr`/`freeNext`), and the
//! bgwriter wakeup latch — is reached through the owners' seam crates, because
//! that infrastructure is not yet ported. The `buffer_strategy_lock` is a real
//! [`backend_storage_lmgr_s_lock::Spinlock`] (a direct dep). The in-crate
//! ALGORITHMS (clock sweep, open-addressing lookup/insert/delete, free-list
//! pop, ring policy, local clock sweep) stay in-crate.
//!
//! `localbuf.c` is BACKEND-LOCAL (temp-table buffers are never shared); its
//! pool is an owned [`localbuf::LocalBufferManager`] value, and its externals
//! are the temp-relation `smgr` I/O entry points (seamed).

extern crate alloc;

use alloc::rc::Rc;
use core::cell::RefCell;

use types_error::PgResult;
use types_storage::buf::{BufferAccessStrategy, BufferAccessStrategyType};

mod buf_table;
mod freelist;
mod localbuf;
mod strategy;

pub use buf_table::{buf_table_hash_code, buf_table_hash_partition, BufTable, BufTableShmemSize};
pub use freelist::{BufferStrategyControl, ClockSweep, StrategyShmemSize};
pub use localbuf::{check_temp_buffers, LocalBufferManager};
pub use strategy::{
    get_access_strategy_ring, get_access_strategy_with_size_ring, BufferAccessStrategyRing,
    FreeAccessStrategy,
};

// Re-export the shared signature types from types-storage so callers reach them
// through this crate's surface.
pub use types_storage::buf::{
    IOContext, LocalBufferLookupEnt, Victim, FREENEXT_END_OF_LIST, FREENEXT_NOT_IN_LIST,
};
pub use types_storage::PrefetchBufferResult;

/// `BUF_STATE_GET_REFCOUNT(buf_state)` (buf_internals.h).
#[inline]
fn buf_state_get_refcount(buf_state: u32) -> u32 {
    buf_state & types_storage::buf::BUF_REFCOUNT_MASK
}

/// `BUF_STATE_GET_USAGECOUNT(buf_state)` (buf_internals.h).
#[inline]
fn buf_state_get_usagecount(buf_state: u32) -> u32 {
    (buf_state & types_storage::buf::BUF_USAGECOUNT_MASK) / types_storage::buf::BUF_USAGECOUNT_ONE
}

// ---------------------------------------------------------------------------
// Backend-private `BufferAccessStrategy` ring.
//
// `BufferAccessStrategyData` is a backend-private object that C's
// `GetAccessStrategy` `palloc`s and hands BACK BY POINTER (`typedef struct
// BufferAccessStrategyData *BufferAccessStrategy`); callers hold it directly and
// mutate the ring through the pointer until `FreeAccessStrategy` `pfree`s it.
// The faithful Rust model of that single shared/mutated heap object is an
// `Rc<RefCell<_>>` (see the `BufferAccessStrategy` alias in types-storage); the
// C `NULL` (default, no-ring) strategy is `None`. There is no id-keyed lookup
// table — the handle IS the object.
// ---------------------------------------------------------------------------

/// `GetAccessStrategy(btype)` (freelist.c) installed inward seam. Builds the
/// ring (or `None` for the default/no-ring strategy) and returns it as the
/// by-pointer handle (`Rc<RefCell<_>>`), mirroring C's `palloc`'d object.
pub fn get_access_strategy(btype: BufferAccessStrategyType) -> PgResult<BufferAccessStrategy> {
    let nbuffers_total = backend_utils_init_small_seams::nbuffers::call();
    let ring = strategy::get_access_strategy_ring(btype, nbuffers_total)?;
    Ok(ring.map(|ring| Rc::new(RefCell::new(ring))))
}

/// `GetAccessStrategyWithSize(btype, ring_size_kb)` (freelist.c) — create a
/// `BufferAccessStrategy` whose ring holds `ring_size_kb / (BLCKSZ/1024)`
/// buffers. Returns the C `NULL` (`None`) strategy when `ring_size_kb` rounds
/// down to zero buffers. Used directly by `vacuum.c` / `vacuumparallel.c` to
/// size the VACUUM ring from `vacuum_buffer_usage_limit`.
pub fn get_access_strategy_with_size(
    btype: BufferAccessStrategyType,
    ring_size_kb: i32,
) -> PgResult<BufferAccessStrategy> {
    let nbuffers_total = backend_utils_init_small_seams::nbuffers::call();
    let ring = strategy::get_access_strategy_with_size_ring(btype, ring_size_kb, nbuffers_total)?;
    Ok(ring.map(|ring| Rc::new(RefCell::new(ring))))
}

/// `GetAccessStrategyBufferCount(strategy)` (freelist.c) — the number of buffers
/// in the ring; `0` for the C `NULL` (`None`) strategy, matching
/// `GetAccessStrategyWithSize` returning `NULL` at 0 size.
pub fn get_access_strategy_buffer_count(strategy: &BufferAccessStrategy) -> i32 {
    match strategy {
        None => 0,
        Some(s) => s.borrow().nbuffers,
    }
}

/// `FreeAccessStrategy(strategy)` (freelist.c) installed inward seam. A NULL
/// (`None`) strategy is a no-op (C's guard); otherwise dropping the handle frees
/// the ring once the last reference is gone (C's `pfree`).
pub fn free_access_strategy(strategy: BufferAccessStrategy) {
    drop(strategy);
}

// ---------------------------------------------------------------------------
// Local-buffer (localbuf.c) ambient manager + dispatch seams.
//
// localbuf.c's process-global file-statics (`LocalBufferDescriptors`,
// `LocalBufHash`, `LocalRefCount`, `NLocBuffer`, ...) are modeled by the
// per-backend ambient [`localbuf::LocalBufferManager`] published via
// `register_global`. C allocates these lazily on the FIRST temp-relation access
// (`if (LocalBufHash == NULL) InitLocalBuffers()`); we mirror that by
// constructing + publishing the manager on first demand
// ([`local_mgr_get_or_create`]) reading the `num_temp_buffers` GUC and
// `IsParallelWorker()` exactly where C's `InitLocalBuffers` does.
//
// The two end-of-life leak checks (`AtEOXact_LocalBuffers` /
// `AtProcExit_LocalBuffers`) must NOT force a manager into existence: C's
// `CheckForLocalBufferLeaks` is `if (LocalRefCount) { ... }` — a no-op for a
// backend that never touched a temp relation (LocalRefCount == NULL). So those
// two seams use [`localbuf::LocalBufferManager::global`] and no-op when absent.
// ---------------------------------------------------------------------------

/// Get this backend's ambient local-buffer manager, lazily constructing +
/// publishing it on first demand. Mirrors localbuf.c's
/// `if (LocalBufHash == NULL) InitLocalBuffers()` guard: the manager's arrays
/// are still allocated lazily by `InitLocalBuffers` inside the manager, but the
/// manager VALUE itself (which reads the `num_temp_buffers` GUC and
/// `IsParallelWorker()`) is created here.
fn local_mgr_get_or_create() -> &'static localbuf::LocalBufferManager {
    if let Some(mgr) = localbuf::LocalBufferManager::global() {
        return mgr;
    }
    let num_temp_buffers = backend_utils_misc_guc_tables::vars::num_temp_buffers.read();
    let is_parallel_worker = backend_access_transam_parallel::is_parallel_worker();
    localbuf::LocalBufferManager::new(num_temp_buffers, is_parallel_worker).register_global()
}

/// `LocalRefCount[-buffer - 1]` (localbuf.c) — this backend's local pin count.
fn local_ref_count(buffer: types_core::primitive::Buffer) -> PgResult<i32> {
    Ok(local_mgr_get_or_create().local_ref_count(buffer))
}

/// `MarkLocalBufferDirty(buffer)` (localbuf.c).
fn mark_local_buffer_dirty(buffer: types_core::primitive::Buffer) -> PgResult<()> {
    local_mgr_get_or_create().MarkLocalBufferDirty(buffer)
}

/// `UnpinLocalBuffer(buffer)` (localbuf.c).
fn unpin_local_buffer(buffer: types_core::primitive::Buffer) -> PgResult<()> {
    local_mgr_get_or_create().UnpinLocalBuffer(buffer)
}

/// `LocalRefCount[-buffer - 1]++` (localbuf.c, inline `IncrBufferRefCount` arm).
fn incr_local_buffer_ref_count(buffer: types_core::primitive::Buffer) -> PgResult<()> {
    local_mgr_get_or_create().IncrLocalBufferRefCount(buffer);
    Ok(())
}

/// `BufferGetBlockNumber` local arm (localbuf.c).
fn local_buffer_block_number(
    buffer: types_core::primitive::Buffer,
) -> PgResult<types_core::primitive::BlockNumber> {
    Ok(local_mgr_get_or_create().block_number(buffer))
}

/// `BufferGetTag` local arm (localbuf.c).
fn local_buffer_get_tag(
    buffer: types_core::primitive::Buffer,
) -> PgResult<(
    types_storage::RelFileLocator,
    types_core::primitive::ForkNumber,
    types_core::primitive::BlockNumber,
)> {
    let tag = local_mgr_get_or_create().buffer_tag(buffer);
    let rlocator = types_storage::RelFileLocator {
        spcOid: tag.spcOid,
        dbOid: tag.dbOid,
        relNumber: tag.relNumber,
    };
    Ok((rlocator, tag.forkNum, tag.blockNum))
}

/// `PageGetLSN(BufferGetPage(buffer))` local arm (`BufferGetLSNAtomic`).
fn local_buffer_get_lsn(
    buffer: types_core::primitive::Buffer,
) -> PgResult<types_core::primitive::XLogRecPtr> {
    local_mgr_get_or_create().with_block(buffer, |bytes| {
        let page = backend_storage_page::PageRef::new(bytes)?;
        Ok(backend_storage_page::PageGetLSN(&page))
    })
}

/// `LocalBufferAlloc(smgr, forkNum, blockNum, foundPtr)` (localbuf.c).
fn local_buffer_alloc(
    smgr_reln: types_storage::RelFileLocatorBackend,
    fork_num: types_core::primitive::ForkNumber,
    block_num: types_core::primitive::BlockNumber,
) -> PgResult<(types_storage::storage::Buffer, bool)> {
    local_mgr_get_or_create().LocalBufferAlloc(smgr_reln.locator, fork_num, block_num)
}

/// `PrefetchLocalBuffer(smgr, forkNum, blockNum)` (localbuf.c).
fn prefetch_local_buffer(
    smgr_reln: types_storage::RelFileLocatorBackend,
    fork_num: types_core::primitive::ForkNumber,
    block_num: types_core::primitive::BlockNumber,
) -> PgResult<types_storage::PrefetchBufferResult> {
    local_mgr_get_or_create().PrefetchLocalBuffer(smgr_reln.locator, fork_num, block_num)
}

/// `AtEOXact_LocalBuffers(isCommit)` (localbuf.c) — leak-check at end of
/// transaction. No-op when no manager exists (C's `if (LocalRefCount)` guard).
fn at_eoxact_local_buffers(is_commit: bool) -> PgResult<()> {
    match localbuf::LocalBufferManager::global() {
        Some(mgr) => mgr.AtEOXact_LocalBuffers(is_commit),
        None => Ok(()),
    }
}

/// `AtProcExit_LocalBuffers()` (localbuf.c) — leak-check at backend exit. No-op
/// when no manager exists (C's `if (LocalRefCount)` guard).
fn at_proc_exit_local_buffers() -> PgResult<()> {
    match localbuf::LocalBufferManager::global() {
        Some(mgr) => mgr.AtProcExit_LocalBuffers(),
        None => Ok(()),
    }
}

/// `ExtendBufferedRelLocal(...)` (localbuf.c).
fn extend_buffered_rel_local(
    smgr_reln: types_storage::RelFileLocatorBackend,
    fork_num: types_core::primitive::ForkNumber,
    flags: u32,
    extend_by: u32,
    extend_upto: types_core::primitive::BlockNumber,
    buffers: &mut [types_storage::storage::Buffer],
) -> PgResult<(types_core::primitive::BlockNumber, u32)> {
    let mut extended_by: u32 = 0;
    let first_block = local_mgr_get_or_create().ExtendBufferedRelLocal(
        smgr_reln.locator,
        fork_num,
        flags,
        extend_by,
        extend_upto,
        buffers,
        &mut extended_by,
    )?;
    Ok((first_block, extended_by))
}

/// `DropRelationLocalBuffers(rlocator, forkNum, firstDelBlock)` (localbuf.c).
fn drop_relation_local_buffers(
    rlocator: types_storage::RelFileLocator,
    forknum: &[types_core::primitive::ForkNumber],
    first_del_block: &[types_core::primitive::BlockNumber],
) -> PgResult<()> {
    local_mgr_get_or_create().DropRelationLocalBuffers(rlocator, forknum, first_del_block)
}

/// `DropRelationAllLocalBuffers(rlocator)` (localbuf.c).
fn drop_relation_all_local_buffers(rlocator: types_storage::RelFileLocator) -> PgResult<()> {
    local_mgr_get_or_create().DropRelationAllLocalBuffers(rlocator)
}

/// Install this crate's inward seams: the two `BufferAccessStrategy` bufmgr
/// seams, plus the local-buffer (localbuf.c) dispatch seams the shared buffer
/// manager calls on its `BufferIsLocal` branches and the transaction/proc-exit
/// cleanup legs. The per-buffer header / shmem / smgr / GUC / latch seams this
/// crate CONSUMES are installed by their own owners.
pub fn init_seams() {
    backend_storage_buffer_bufmgr_seams::get_access_strategy::set(get_access_strategy);
    backend_storage_buffer_bufmgr_seams::free_access_strategy::set(free_access_strategy);

    // Local-buffer dispatch declared in the bufmgr-seams crate (the
    // bufmgr-OUTWARD F1c/F5 local-buffer arms + the transaction/proc-exit
    // cleanup legs).
    backend_storage_buffer_bufmgr_seams::local_ref_count::set(local_ref_count);
    backend_storage_buffer_bufmgr_seams::mark_local_buffer_dirty::set(mark_local_buffer_dirty);
    backend_storage_buffer_bufmgr_seams::local_buffer_alloc::set(local_buffer_alloc);
    backend_storage_buffer_bufmgr_seams::prefetch_local_buffer::set(prefetch_local_buffer);
    backend_storage_buffer_bufmgr_seams::at_eoxact_local_buffers::set(at_eoxact_local_buffers);
    backend_storage_buffer_bufmgr_seams::at_proc_exit_local_buffers::set(at_proc_exit_local_buffers);
    backend_storage_buffer_bufmgr_seams::extend_buffered_rel_local::set(extend_buffered_rel_local);
    backend_storage_buffer_bufmgr_seams::drop_relation_local_buffers::set(
        drop_relation_local_buffers,
    );
    backend_storage_buffer_bufmgr_seams::drop_relation_all_local_buffers::set(
        drop_relation_all_local_buffers,
    );

    // Local-buffer dispatch declared in the support-seams crate (the
    // `BufferIsLocal` arms of ReleaseBuffer/IncrBufferRefCount/MarkBufferDirty/
    // BufferGetBlockNumber/BufferGetTag/BufferGetLSNAtomic).
    backend_storage_buffer_support_seams::mark_local_buffer_dirty::set(mark_local_buffer_dirty);
    backend_storage_buffer_support_seams::unpin_local_buffer::set(unpin_local_buffer);
    backend_storage_buffer_support_seams::incr_local_buffer_ref_count::set(
        incr_local_buffer_ref_count,
    );
    backend_storage_buffer_support_seams::local_buffer_block_number::set(local_buffer_block_number);
    backend_storage_buffer_support_seams::local_buffer_get_tag::set(local_buffer_get_tag);
    backend_storage_buffer_support_seams::local_buffer_get_lsn::set(local_buffer_get_lsn);
}

#[cfg(test)]
pub(crate) mod test_support;
