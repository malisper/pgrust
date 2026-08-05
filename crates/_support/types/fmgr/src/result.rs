use core::alloc::Layout;

use ::datum::{Datum, Varlena};
use ::mcx::{Allocator, Mcx, PgVec};
use ::types_error::PgResult;

// Results leak into the arming context and die at its reset (C's palloc ownership).
#[inline]
pub fn varlena_result(v: Varlena<'_>) -> Datum {
    let image = v.into_image();
    let d = Datum::from_usize(image.as_ptr() as usize);
    core::mem::forget(image);
    d
}

#[inline]
pub fn cstring_result(v: PgVec<'_, u8>) -> Datum {
    debug_assert_eq!(v.last(), Some(&0));
    let d = Datum::from_usize(v.as_ptr() as usize);
    core::mem::forget(v);
    d
}

/// Image copy into `mcx` at C's palloc alignment (8): the aligned-payload lane (numeric digits).
pub fn byref_result(mcx: Mcx<'_>, image: &[u8]) -> PgResult<Datum> {
    ::mcx::check_alloc_size(image.len())?;
    let layout = Layout::from_size_align(image.len(), 8).expect("byref_result layout");
    let dst: core::ptr::NonNull<u8> = mcx
        .allocate(layout)
        .map_err(|_| mcx.oom(layout.size()))?
        .cast();
    // SAFETY: fresh `image.len()`-byte allocation; `image` is a live slice.
    unsafe {
        core::ptr::copy_nonoverlapping(image.as_ptr(), dst.as_ptr(), image.len());
    }
    Ok(Datum::from_usize(dst.as_ptr() as usize))
}
