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

mod mgr;
mod refcount;

pub use mgr::BufferManager;

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

/// Install this crate's inward seams. F1a installs the four
/// header/freelist seams that unblock the buffer-support freelist clock sweep;
/// the pin/lock/mark/page seams arrive in F1b-d.
pub fn init_seams() {
    backend_storage_buffer_bufmgr_seams::lock_buf_hdr::set(lock_buf_hdr);
    backend_storage_buffer_bufmgr_seams::unlock_buf_hdr::set(unlock_buf_hdr);
    backend_storage_buffer_bufmgr_seams::buf_free_next::set(buf_free_next);
    backend_storage_buffer_bufmgr_seams::set_buf_free_next::set(set_buf_free_next);
}
