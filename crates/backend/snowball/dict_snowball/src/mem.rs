use core::ffi::c_void;
use std::alloc::{alloc, alloc_zeroed, dealloc, realloc, Layout};

// The snowball buffer layout stores [capacity, length] ints at negative
// offsets from the returned pointer, so addresses are load-bearing and the
// buffers cannot live in a handle-based arena; they go through the global
// allocator (mimalloc) with a size header so Layout is recoverable.
// DIVERGENCE: C pallocs these into dictCtx (bulk-freed); here SN_close_env
// frees them and a dictionary dropped without close leaks.
const HDR: usize = core::mem::size_of::<usize>();

#[inline]
fn layout_for(total: usize) -> Layout {
    Layout::from_size_align(total, core::mem::align_of::<usize>()).expect("snowball layout")
}

pub fn palloc(size: usize) -> *mut c_void {
    // SAFETY: layout has non-zero size (HDR > 0); header write is in-bounds.
    unsafe {
        let p = alloc(layout_for(HDR + size));
        if p.is_null() {
            return core::ptr::null_mut();
        }
        *(p as *mut usize) = size;
        p.add(HDR) as *mut c_void
    }
}

pub fn palloc0(size: usize) -> *mut c_void {
    // SAFETY: as palloc.
    unsafe {
        let p = alloc_zeroed(layout_for(HDR + size));
        if p.is_null() {
            return core::ptr::null_mut();
        }
        *(p as *mut usize) = size;
        p.add(HDR) as *mut c_void
    }
}

/// # Safety
/// `ptr` must be a live chunk from this module's palloc family.
pub unsafe fn repalloc(ptr: *mut c_void, size: usize) -> *mut c_void {
    unsafe {
        let base = (ptr as *mut u8).sub(HDR);
        let old_size = *(base as *mut usize);
        let np = realloc(base, layout_for(HDR + old_size), HDR + size);
        if np.is_null() {
            return core::ptr::null_mut();
        }
        *(np as *mut usize) = size;
        np.add(HDR) as *mut c_void
    }
}

/// # Safety
/// `ptr` must be null or a live chunk from this module's palloc family.
/// C api.c relies on free(NULL) being a no-op.
pub unsafe fn pfree(ptr: *mut c_void) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        let base = (ptr as *mut u8).sub(HDR);
        let size = *(base as *mut usize);
        dealloc(base, layout_for(HDR + size));
    }
}
