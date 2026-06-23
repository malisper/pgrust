//! Local allocation seam for the snowball runtime.
//!
//! The snowball C runtime (`src/backend/snowball/libstemmer/api.c`,
//! `src/backend/snowball/libstemmer/utilities.c`) manages its `symbol*`
//! buffers with a hand-rolled length/capacity header that lives in the bytes
//! immediately *before* the returned pointer (see [`crate::utilities`]). It
//! does so through the four C primitives `malloc`/`calloc`/`realloc`/`free`,
//! which PostgreSQL's `src/include/snowball/header.h` (lines 47-65) redefines to
//! `palloc`/`palloc0`/`repalloc`/`pfree` so the snowball code allocates inside a
//! backend memory context.
//!
//! In the idiomatic workspace the real allocator (`backend-utils-mmgr`) hands
//! out an `Allocation` *handle*, not a raw pointer with stable address that the
//! snowball header arithmetic requires. We therefore keep an in-crate seam: a
//! function-pointer table that a host installs with [`install`]. Until a host
//! installs a provider the seam loud-panics — it never fabricates an allocation
//! or silently returns null (that would corrupt the byte-for-byte algorithm).
//!
//! The C functions return `NULL` on allocation failure and the runtime contains
//! explicit `NULL` checks that are part of the algorithm. A provider may signal
//! failure by returning a null pointer; the runtime honours that control flow
//! exactly.

use core::ffi::c_void;

/// The set of raw allocation primitives the snowball runtime needs. A host
/// (PostgreSQL backend, or the in-crate test allocator) installs these so the
/// runtime can manage its hidden-header `symbol*` buffers.
///
/// Every pointer flowing through these primitives is a raw byte pointer whose
/// numeric address is load-bearing (the snowball header lives at negative
/// offsets), so a handle-based allocator cannot be plugged in directly; the
/// host must expose real `*mut` addresses.
#[derive(Copy, Clone)]
pub struct AllocHooks {
    /// `palloc(size)`: allocate `size` bytes; null on failure.
    pub palloc: unsafe fn(usize) -> *mut c_void,
    /// `palloc0(size)`: allocate `size` zeroed bytes; null on failure.
    pub palloc0: unsafe fn(usize) -> *mut c_void,
    /// `repalloc(ptr, size)`: grow/shrink a chunk; null on failure.
    ///
    /// # Safety
    /// `ptr` must be a live chunk from `palloc`/`palloc0`/`repalloc`.
    pub repalloc: unsafe fn(*mut c_void, usize) -> *mut c_void,
    /// `pfree(ptr)`: release a chunk from the palloc family.
    ///
    /// # Safety
    /// `ptr` must be a live chunk from `palloc`/`palloc0`/`repalloc`.
    pub pfree: unsafe fn(*mut c_void),
}

/// Installed allocation hooks. `None` until a host calls [`install`]; the seam
/// loud-panics on use until then.
static mut HOOKS: Option<AllocHooks> = None;

/// Install the host allocation primitives. Call once during single-threaded
/// startup, before any stemmer runs.
///
/// # Safety
/// Must be called before any concurrent use of the runtime; mutates a process
/// global. The supplied function pointers must implement the `palloc` family
/// contract over real raw addresses.
pub unsafe fn install(hooks: AllocHooks) {
    unsafe {
        HOOKS = Some(hooks);
    }
}

#[inline]
fn hooks() -> AllocHooks {
    // SAFETY: single-threaded backend startup installs the hooks before use;
    // reads after that are stable. We never expose a &mut to the static.
    match unsafe { HOOKS } {
        Some(h) => h,
        None => panic!(
            "backend-snowball-runtime: allocation seam not installed; \
             call runtime::mem::install() with the backend \
             palloc/palloc0/repalloc/pfree primitives before running a stemmer"
        ),
    }
}

/// `palloc(size)` — allocate `size` bytes in the current memory context.
/// Returns null on allocation failure, mirroring the C contract the snowball
/// runtime checks against. Loud-panics if no provider is installed.
#[inline]
pub fn palloc(size: usize) -> *mut c_void {
    let h = hooks();
    // SAFETY: provider upholds the palloc contract.
    unsafe { (h.palloc)(size) }
}

/// `palloc0(size)` — allocate `size` zero-initialised bytes. Null on failure.
#[inline]
pub fn palloc0(size: usize) -> *mut c_void {
    let h = hooks();
    // SAFETY: provider upholds the palloc0 contract.
    unsafe { (h.palloc0)(size) }
}

/// `repalloc(pointer, size)` — grow/shrink a palloc'd chunk; null on failure.
///
/// # Safety
/// `pointer` must be a live allocation from [`palloc`]/[`palloc0`]/this fn.
#[inline]
pub unsafe fn repalloc(pointer: *mut c_void, size: usize) -> *mut c_void {
    let h = hooks();
    // SAFETY: caller upholds liveness; provider upholds repalloc contract.
    unsafe { (h.repalloc)(pointer, size) }
}

/// `pfree(pointer)` — release a chunk obtained from the palloc family.
///
/// `api.c:46` calls `free(z->I)` unconditionally even when `z->I` is still NULL
/// (it is zero-initialised by `palloc0` and only assigned when `I_size != 0`).
/// In C this relies on `free(NULL)` being a no-op. The backend `pfree` does not
/// accept NULL, so — exactly as the faithful runtime does — we guard NULL here
/// to preserve that call-site control flow.
///
/// # Safety
/// `pointer` must be null or a live allocation from
/// [`palloc`]/[`palloc0`]/[`repalloc`].
#[inline]
pub unsafe fn pfree(pointer: *mut c_void) {
    if pointer.is_null() {
        return;
    }
    let h = hooks();
    // SAFETY: caller upholds liveness; provider upholds pfree contract.
    unsafe { (h.pfree)(pointer) }
}

// ---------------------------------------------------------------------------
// In-crate test allocator.
//
// The snowball buffer layout stores `[capacity, length]` ints *before* the
// returned pointer and reads/writes them at negative offsets, so the test
// allocator must hand out raw addresses with a recoverable original layout.
// We prefix every allocation with the requested size (as a `usize`) so that
// `repalloc`/`pfree` can reconstruct the `Layout`. This is test-only scaffolding
// and is never compiled into a shipping build.
// ---------------------------------------------------------------------------
#[cfg(test)]
pub(crate) mod test_alloc {
    use super::{install, AllocHooks};
    use core::ffi::c_void;

    extern crate alloc;
    use alloc::alloc::{alloc, alloc_zeroed, dealloc, realloc, Layout};

    // Reserve a header word large enough for `usize` with adequate alignment for
    // any `int`/pointer the runtime stores. `align_of::<usize>()` >= the
    // runtime's `int`/`symbol` alignment on all supported targets.
    const HDR: usize = core::mem::size_of::<usize>();

    #[inline]
    fn layout_for(total: usize) -> Layout {
        Layout::from_size_align(total, core::mem::align_of::<usize>()).expect("bad layout")
    }

    unsafe fn t_palloc(size: usize) -> *mut c_void {
        let total = HDR + size;
        let p = unsafe { alloc(layout_for(total)) };
        if p.is_null() {
            return core::ptr::null_mut();
        }
        unsafe { *(p as *mut usize) = size };
        unsafe { p.add(HDR) as *mut c_void }
    }

    unsafe fn t_palloc0(size: usize) -> *mut c_void {
        let total = HDR + size;
        let p = unsafe { alloc_zeroed(layout_for(total)) };
        if p.is_null() {
            return core::ptr::null_mut();
        }
        unsafe { *(p as *mut usize) = size };
        unsafe { p.add(HDR) as *mut c_void }
    }

    unsafe fn t_repalloc(ptr: *mut c_void, size: usize) -> *mut c_void {
        let base = unsafe { (ptr as *mut u8).sub(HDR) };
        let old_size = unsafe { *(base as *mut usize) };
        let old_total = HDR + old_size;
        let new_total = HDR + size;
        let np = unsafe { realloc(base, layout_for(old_total), new_total) };
        if np.is_null() {
            return core::ptr::null_mut();
        }
        unsafe { *(np as *mut usize) = size };
        unsafe { np.add(HDR) as *mut c_void }
    }

    unsafe fn t_pfree(ptr: *mut c_void) {
        let base = unsafe { (ptr as *mut u8).sub(HDR) };
        let size = unsafe { *(base as *mut usize) };
        unsafe { dealloc(base, layout_for(HDR + size)) };
    }

    /// Install the std-backed test allocator. Idempotent for the purposes of the
    /// tests (re-installing the same hooks is harmless).
    pub(crate) fn ensure_installed() {
        // SAFETY: tests run single-threaded by default for this module.
        unsafe {
            install(AllocHooks {
                palloc: t_palloc,
                palloc0: t_palloc0,
                repalloc: t_repalloc,
                pfree: t_pfree,
            });
        }
    }
}
