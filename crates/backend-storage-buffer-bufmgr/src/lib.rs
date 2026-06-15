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

mod buf_lock;
mod mgr;
mod ops;
mod refcount;

pub use mgr::BufferManager;

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
    backend_storage_buffer_bufmgr_seams::is_buffer_cleanup_ok::set(is_buffer_cleanup_ok);
    backend_storage_buffer_bufmgr_seams::mark_buffer_dirty_hint::set(mark_buffer_dirty_hint);
}
