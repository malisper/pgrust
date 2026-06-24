//! wasm-only `libc` shim for `backend-utils-error-fgram`.
//!
//! On `wasm64-unknown-unknown` the `libc` crate links but exposes none of the
//! POSIX errno constants or `malloc`/`free` this crate uses. The errno values
//! are needed only as *classification keys* — `errcode_for_file_access` /
//! `errcode_for_socket_access` map a `saved_errno` onto a SQLSTATE — so they
//! must be real `const`s (they appear in `match` arm patterns). We provide the
//! standard Linux-glibc numeric values, matching the values `libc` exposes on
//! the `x86_64-unknown-linux-gnu` build that is this port's ground truth.
//!
//! Each consumer module brings this in with
//! `#[cfg(target_family = "wasm")] use crate::libc_wasm as libc;`, which
//! shadows the extern-prelude `libc` for that module only.

#![allow(dead_code)]

// --- errno constants (Linux/glibc values) ---------------------------------
pub const EPERM: i32 = 1;
pub const ENOENT: i32 = 2;
pub const EIO: i32 = 5;
pub const ENOMEM: i32 = 12;
pub const EACCES: i32 = 13;
pub const EEXIST: i32 = 17;
pub const ENOTDIR: i32 = 20;
pub const EISDIR: i32 = 21;
pub const ENFILE: i32 = 23;
pub const EMFILE: i32 = 24;
pub const ENOSPC: i32 = 28;
pub const EPIPE: i32 = 32;
pub const ENAMETOOLONG: i32 = 36;
pub const ENOTEMPTY: i32 = 39;
pub const ETIMEDOUT: i32 = 110;
pub const ECONNRESET: i32 = 104;
pub const ECONNABORTED: i32 = 103;
pub const ENETDOWN: i32 = 100;
pub const ENETUNREACH: i32 = 101;
pub const ENETRESET: i32 = 102;
pub const EHOSTDOWN: i32 = 112;
pub const EHOSTUNREACH: i32 = 113;

// --- malloc / free --------------------------------------------------------
//
// `malloc_string`/`free_ptr` allocate a NUL-terminated C string here and free
// it here (the allocation never crosses to a real system `free`), so a
// self-consistent size-prefixed allocator over Rust's global allocator is a
// faithful stand-in. We stash the allocation size in a header word immediately
// before the returned pointer so `free` (which takes no size in C) can recover
// the layout.

use core::ffi::c_void;

const HEADER: usize = core::mem::size_of::<usize>();

/// `void *malloc(size_t size)` — allocate `size` bytes (8-byte aligned), or
/// return null on a zero request or allocation failure.
///
/// # Safety
/// Mirrors the C `malloc` contract: the caller owns the returned block and must
/// release it with [`free`].
pub unsafe fn malloc(size: usize) -> *mut c_void {
    if size == 0 {
        return core::ptr::null_mut();
    }
    let total = size + HEADER;
    let layout = match core::alloc::Layout::from_size_align(total, HEADER) {
        Ok(l) => l,
        Err(_) => return core::ptr::null_mut(),
    };
    // SAFETY: non-zero layout.
    let base = unsafe { std::alloc::alloc(layout) };
    if base.is_null() {
        return core::ptr::null_mut();
    }
    // SAFETY: `base` has room for the header word.
    unsafe { (base as *mut usize).write(total) };
    unsafe { base.add(HEADER) as *mut c_void }
}

/// `void free(void *ptr)` — release a block previously returned by [`malloc`].
/// A null pointer is a no-op, matching C.
///
/// # Safety
/// `ptr` must be null or a pointer returned by this module's [`malloc`].
pub unsafe fn free(ptr: *mut c_void) {
    if ptr.is_null() {
        return;
    }
    // SAFETY: recover the header written by `malloc`.
    let base = unsafe { (ptr as *mut u8).sub(HEADER) };
    let total = unsafe { (base as *mut usize).read() };
    let layout = core::alloc::Layout::from_size_align(total, HEADER)
        .expect("layout was valid at malloc time");
    // SAFETY: `base`/`layout` match the original allocation.
    unsafe { std::alloc::dealloc(base, layout) };
}
