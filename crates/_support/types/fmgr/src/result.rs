use core::alloc::Layout;

use ::datum::{Datum, Varlena};
use ::mcx::{Allocator, Mcx, PgVec};
use ::types_error::PgResult;

// Shared body for registered internal-protocol builtins (selectivity
// estimators, AM/opclass support, window internals, RI trigger bodies, ...):
// pgrust routes their catalog-driven dispatch natively, and SQL cannot form
// `internal` arguments (C parity), so a call landing here is a dispatch bug.
#[cold]
#[inline(never)]
pub fn fc_internal_dispatch_only(
    flinfo: Option<&mut crate::fcinfo::FmgrInfo>,
    _fcinfo: &mut crate::fcinfo::FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let oid = flinfo.map_or(0, |f| f.fn_oid);
    panic!(
        "internal-protocol builtin (OID {oid}) called through fmgr; \
         pgrust dispatches it natively"
    );
}

// C AM handlers (gisthandler, bthandler, heap_tableam_handler, ...) palloc
// their AmRoutine and return it without inspecting the argument; pgrust
// dispatches the closed AM set natively, so an alias call through fmgr
// (CREATE FUNCTION ... AS 'gisthandler' LANGUAGE internal) gets an opaque
// non-null block, as C's IS NOT NULL / pointer result does.
pub fn fc_am_handler_stub(
    _flinfo: Option<&mut crate::fcinfo::FmgrInfo>,
    fcinfo: &mut crate::fcinfo::FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), &[0u8; 8])
}

// Results leak into the arming context and die at its reset (C's palloc ownership).
#[inline]
pub fn varlena_result(v: Varlena<'_>) -> Datum {
    let image = v.into_image();
    let d = Datum::from_usize(image.as_ptr() as usize);
    core::mem::forget(image);
    d
}

// C pallocs each cstring out-function result per row; the resolved FmgrInfo
// owns retained scratch instead (rule 7). The datum aliases it until the next
// call through the same FmgrInfo; sibling expression nodes own their own.
pub struct OutScratch(pub alloc::vec::Vec<u8>);

#[cold]
#[inline(never)]
fn no_flinfo(name: &str) -> ! {
    panic!("{name}: cstring result needs a resolved FmgrInfo's scratch; direct callers use the value core")
}

pub fn cstring_scratch(
    flinfo: Option<&mut crate::fcinfo::FmgrInfo>,
    name: &'static str,
    bytes: &[u8],
) -> Datum {
    let Some(flinfo) = flinfo else { no_flinfo(name) };
    if !flinfo.has_fn_extra() {
        flinfo.set_fn_extra(OutScratch(alloc::vec::Vec::new()));
    }
    let buf = &mut flinfo.fn_extra_mut::<OutScratch>().unwrap().0;
    buf.clear();
    buf.reserve(bytes.len() + 1);
    buf.extend_from_slice(bytes);
    buf.push(0);
    Datum::from_usize(buf.as_ptr() as usize)
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
