//! Backend allocation provider for the snowball runtime.
//!
//! PostgreSQL's `src/include/snowball/header.h` (lines 47-65) redefines the C
//! `malloc`/`calloc`/`realloc`/`free` the libstemmer runtime uses to
//! `palloc`/`palloc0`/`repalloc`/`pfree`. The snowball runtime (`SN_env`
//! `symbol*` buffers) needs a *raw-address* allocator: its buffer header (the
//! `[capacity, length]` ints) lives at negative offsets before the returned
//! pointer, so the handle-based backend mmgr cannot be plugged in directly
//! (`backend-snowball-runtime/src/mem.rs` documents this).
//!
//! These buffers are short-lived scratch allocated and `pfree`d entirely within
//! a single `dsnowball_lexize` call (the runtime frees every buffer it creates),
//! so a raw `malloc`/`realloc`/`free`-backed provider is behaviorally identical
//! to the C palloc-into-CurrentMemoryContext path (it relies on explicit
//! `free`, not a context reset). We install it once at startup.

use core::ffi::c_void;

use runtime::mem::{install, AllocHooks};

unsafe fn p_palloc(size: usize) -> *mut c_void {
    // C `palloc(0)` returns a valid unique pointer; libc malloc(0) is
    // implementation-defined, so request at least 1 byte.
    let n = size.max(1);
    unsafe { libc::malloc(n) }
}

unsafe fn p_palloc0(size: usize) -> *mut c_void {
    let n = size.max(1);
    unsafe { libc::calloc(1, n) }
}

unsafe fn p_repalloc(ptr: *mut c_void, size: usize) -> *mut c_void {
    let n = size.max(1);
    unsafe { libc::realloc(ptr, n) }
}

unsafe fn p_pfree(ptr: *mut c_void) {
    // `backend-snowball-runtime::mem::pfree` already guards NULL before calling
    // this provider, but libc `free(NULL)` is a safe no-op regardless.
    unsafe { libc::free(ptr) }
}

/// Install the libc-backed raw allocator into the snowball runtime. Idempotent
/// (re-install overwrites the same pointers); call once during startup.
pub fn install_snowball_alloc() {
    // SAFETY: called during single-threaded backend startup, before any stemmer
    // runs; the libc primitives uphold the palloc-family raw-address contract.
    unsafe {
        install(AllocHooks {
            palloc: p_palloc,
            palloc0: p_palloc0,
            repalloc: p_repalloc,
            pfree: p_pfree,
        });
    }
}
