//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the
//! `multirangetypes.c` SQL-callable functions whose argument/result types are
//! expressible at the current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core (`typcache_io` / `operators` /
//! `setops_ordering_agg`), and writes back the result word / by-reference
//! payload. [`register_multirangetypes_builtins`] registers every row into the
//! fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch resolves
//! them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat`.
//!
//! # The by-reference `anymultirange` convention
//!
//! `multirange` (`anymultirange`) is a pass-by-reference (varlena) type, the
//! same shape as `anyrange`: a serialized [`MultirangeTypeP`] is a real in-memory
//! image whose header carries the multirange type's OID, and the value cores read
//! it as a `*const MultirangeType` handle obtained via `DatumGetMultirangeTypeP`
//! (which dereferences a `Datum` word as the varlena address). So a `multirange`
//! ARG arriving as `RefPayload::Varlena(bytes)` on the by-ref lane is first
//! materialized as a real varlena in a memory context, the by-value word is set
//! to its address, and the cores read it through `datum_get_multirange_type_p`.
//! A `multirange` RESULT (`make_*` returns a `MultirangeTypeP`) is read back at
//! its pointer (`VARSIZE`) and copied onto the by-ref result lane as `Varlena`.
//! The bytes carried are the COMPLETE `MultirangeType` varlena image INCLUDING
//! its 4-byte `VARHDRSZ` header, symmetric on the arg and result lanes — mirrors
//! the sibling `rangetypes` `fmgr_builtins` convention exactly.

use backend_utils_adt_rangetypes_seams as range_seams;
use backend_utils_fmgr_core::get_fn_expr_rettype;
use mcx::{Mcx, MemoryContext};
use types_cache::typcache::TypeCacheEntry;
use types_core::primitive::Oid;
use types_datum::Datum;
use types_error::PgResult;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, RefPayload};
use types_rangetypes::{MultirangeTypeP, RangeTypeP};

// ---------------------------------------------------------------------------
// Marshalling helpers (mirror rangetypes/fmgr_builtins).
// ---------------------------------------------------------------------------

/// `VARHDRSZ` (`c.h`) — `sizeof(int32)`.
const VARHDRSZ: usize = 4;

/// `VARSIZE_4B(ptr)` from a plain (4-byte-header, uncompressed) varlena.
///
/// # Safety
/// `ptr` must point at a valid plain 4B varlena header.
#[inline]
unsafe fn varsize_4b(ptr: *const u8) -> usize {
    let word = (ptr as *const u32).read_unaligned();
    ((word >> 2) & 0x3FFF_FFFF) as usize
}

/// Materialize a `MultirangeType` varlena image (full bytes, header and all) into
/// `mcx` MAXALIGN'd and return the `Datum` pointer word `DatumGetMultirangeTypeP`
/// dereferences.
fn mr_bytes_to_arg_word<'mcx>(mcx: Mcx<'mcx>, image: &[u8]) -> PgResult<Datum> {
    use core::alloc::Layout;
    use mcx::Allocator;
    mcx::check_alloc_size(image.len())?;
    let layout =
        Layout::from_size_align(image.len().max(1), 8).expect("valid MultirangeType image layout");
    let block = mcx.allocate(layout).map_err(|_| mcx.oom(image.len()))?;
    let dst = block.as_ptr() as *mut u8;
    // SAFETY: `dst` heads a freshly allocated image.len()-byte region.
    unsafe {
        core::ptr::copy_nonoverlapping(image.as_ptr(), dst, image.len());
    }
    Ok(Datum::from_usize(dst as usize))
}

/// Read the complete `MultirangeType` varlena image at a pointer word into an
/// owned `Vec<u8>` for the by-ref result lane.
///
/// # Safety
/// `word` must be the address of a plain 4B `MultirangeType` varlena living for
/// the duration of this read.
unsafe fn mr_word_to_result_bytes(word: Datum) -> Vec<u8> {
    let ptr = word.as_usize() as *const u8;
    debug_assert!(!ptr.is_null());
    let len = varsize_4b(ptr);
    debug_assert!(len >= VARHDRSZ);
    core::slice::from_raw_parts(ptr, len).to_vec()
}

/// A scratch / result context for the multirange ADT's `Mcx`-allocating cores.
fn scratch_mcx() -> MemoryContext {
    MemoryContext::new("multirangetypes fmgr scratch")
}

/// Raise a builtin's `ereport(ERROR)` through `invoke_pgfunction`'s `catch_unwind`.
fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
}

#[inline]
fn ok<T>(r: PgResult<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => raise(e),
    }
}

/// `PG_GETARG_MULTIRANGE_P(i)`: materialize arg `i`'s by-ref `Varlena` image into
/// `mcx` and detoast to a `MultirangeTypeP` (the cores' input form).
fn getarg_multirange<'mcx>(
    fcinfo: &FunctionCallInfoBaseData,
    mcx: Mcx<'mcx>,
    i: usize,
) -> MultirangeTypeP<'mcx> {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("multirange fn: by-ref `multirange` arg missing from by-ref lane");
    let word = ok(mr_bytes_to_arg_word(mcx, image));
    ok(crate::typcache_io::datum_get_multirange_type_p(mcx, word))
}

/// `PG_GETARG_CSTRING(i)`: the input cstring on the by-ref lane.
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("multirange fn: cstring arg missing from by-ref lane")
}

/// `PG_GETARG_INT32(i)` / `PG_GETARG_OID(i)`: the scalar word.
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo.arg(i).map(|nd| nd.value.as_oid()).unwrap_or(0)
}
fn arg_int32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).map(|nd| nd.value.as_i32()).unwrap_or(0)
}
fn arg_int64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).map(|nd| nd.value.as_i64()).unwrap_or(0)
}

/// The element RANGE type's typcache for a `multirange` value: the multirange
/// typcache's `->rngtype` (C: `multirange_get_typcache(...)->rngtype`).
fn rangetyp_of(mr: MultirangeTypeP<'_>) -> TypeCacheEntry {
    let mtc = ok(crate::typcache_io::multirange_get_typcache(mr.multirangetypid()));
    *mtc
        .rngtype
        .expect("multirange typcache has a range subtype")
}

/// Set a `multirange` result (read from its pointer word) on the by-ref lane.
fn ret_multirange(fcinfo: &mut FunctionCallInfoBaseData, mr: MultirangeTypeP<'_>) -> Datum {
    // SAFETY: `mr.ptr` is a plain MultirangeType varlena the core allocated in the
    // wrapper's scratch context, which lives until the wrapper returns.
    let bytes = unsafe { mr_word_to_result_bytes(Datum::from_usize(mr.ptr as usize)) };
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::null()
}

/// `PG_GETARG_RANGE_P(i)`: materialize arg `i`'s by-ref `Varlena` range image into
/// `mcx` and detoast to a `RangeTypeP`. A serialized `RangeType` is a varlena
/// image just like a multirange, so it crosses the by-ref lane the same way.
fn getarg_range<'mcx>(
    fcinfo: &FunctionCallInfoBaseData,
    mcx: Mcx<'mcx>,
    i: usize,
) -> RangeTypeP<'mcx> {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("multirange fn: by-ref `range` arg missing from by-ref lane");
    let word = ok(mr_bytes_to_arg_word(mcx, image));
    ok(range_seams::datum_get_range_type_p::call(mcx, word))
}

/// Set a `range` result (read from its pointer word) on the by-ref lane. A
/// serialized `RangeType` is a plain 4B varlena, read the same way as a multirange.
fn ret_range(fcinfo: &mut FunctionCallInfoBaseData, r: RangeTypeP<'_>) -> Datum {
    // SAFETY: `r.ptr` is a plain RangeType varlena the core allocated in the
    // wrapper's scratch context, which lives until the wrapper returns.
    let bytes = unsafe { mr_word_to_result_bytes(Datum::from_usize(r.ptr as usize)) };
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::null()
}

/// The element RANGE type's typcache for a `multirange` type OID: the multirange
/// typcache's `->rngtype` (C: `multirange_get_typcache(...)->rngtype`).
fn rangetyp_for_mltrng(mltrngtypid: Oid) -> TypeCacheEntry {
    let mtc = ok(crate::typcache_io::multirange_get_typcache(mltrngtypid));
    *mtc
        .rngtype
        .expect("multirange typcache has a range subtype")
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `multirange_in(cstring, oid, int4) -> anymultirange` (oid 4231).
fn fc_multirange_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0);
    let mltrngtypoid = arg_oid(fcinfo, 1);
    let typmod = arg_int32(fcinfo, 2);
    let m = scratch_mcx();
    let mr = ok(crate::typcache_io::multirange_in(m.mcx(), s, mltrngtypoid, typmod));
    ret_multirange(fcinfo, mr)
}

/// `multirange_out(anymultirange) -> cstring` (oid 4232).
fn fc_multirange_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    // `multirange_out` takes the raw `Datum` word and detoasts itself.
    let image = fcinfo
        .ref_arg(0)
        .and_then(|p| p.as_varlena())
        .expect("multirange_out arg 0 is a multirange");
    let word = ok(mr_bytes_to_arg_word(m.mcx(), image));
    let s = ok(crate::typcache_io::multirange_out(m.mcx(), word));
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::null()
}

/// `multirange_recv(internal, oid, int4) -> anymultirange` (oid 4233).
fn fc_multirange_recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // The wire buffer is the by-ref `Varlena` arg 0 (StringInfo message bytes).
    let buf_bytes = fcinfo
        .ref_arg(0)
        .and_then(|p| p.as_varlena())
        .expect("multirange_recv arg 0 is a StringInfo buffer")
        .to_vec();
    let mltrngtypoid = arg_oid(fcinfo, 1);
    let typmod = arg_int32(fcinfo, 2);
    let m = scratch_mcx();
    let mut cur: &[u8] = &buf_bytes;
    let mr = ok(crate::typcache_io::multirange_recv(
        m.mcx(),
        &mut cur,
        mltrngtypoid,
        typmod,
    ));
    ret_multirange(fcinfo, mr)
}

/// `multirange_send(anymultirange) -> bytea` (oid 4234).
fn fc_multirange_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let image = fcinfo
        .ref_arg(0)
        .and_then(|p| p.as_varlena())
        .expect("multirange_send arg 0 is a multirange");
    let word = ok(mr_bytes_to_arg_word(m.mcx(), image));
    let bytes = ok(crate::typcache_io::multirange_send(m.mcx(), word));
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::null()
}

/// `multirange_empty(anymultirange) -> bool` (oid 4237, `isempty`).
fn fc_multirange_empty(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mr = getarg_multirange(fcinfo, m.mcx(), 0);
    Datum::from_bool(ok(crate::operators::multirange_empty(mr)))
}

/// Body of a `(multirange, multirange) -> bool` comparator around a
/// `fn(&TypeCacheEntry, MultirangeTypeP, MultirangeTypeP) -> PgResult<bool>` core.
macro_rules! fc_mr_cmp_bool {
    ($fc:ident, $core:path) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let m = scratch_mcx();
            let a = getarg_multirange(fcinfo, m.mcx(), 0);
            let b = getarg_multirange(fcinfo, m.mcx(), 1);
            let rangetyp = rangetyp_of(a);
            Datum::from_bool(ok($core(&rangetyp, a, b)))
        }
    };
}

fc_mr_cmp_bool!(fc_multirange_eq, crate::operators::multirange_eq_internal);
fc_mr_cmp_bool!(fc_multirange_ne, crate::operators::multirange_ne_internal);
fc_mr_cmp_bool!(fc_multirange_lt, crate::setops_ordering_agg::multirange_lt);
fc_mr_cmp_bool!(fc_multirange_le, crate::setops_ordering_agg::multirange_le);
fc_mr_cmp_bool!(fc_multirange_gt, crate::setops_ordering_agg::multirange_gt);
fc_mr_cmp_bool!(fc_multirange_ge, crate::setops_ordering_agg::multirange_ge);

/// `multirange_cmp(anymultirange, anymultirange) -> int4` (oid 4273).
fn fc_multirange_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let a = getarg_multirange(fcinfo, m.mcx(), 0);
    let b = getarg_multirange(fcinfo, m.mcx(), 1);
    let rangetyp = rangetyp_of(a);
    Datum::from_i32(ok(crate::setops_ordering_agg::multirange_cmp(
        &rangetyp, a, b,
    )))
}

/// `hash_multirange(anymultirange) -> int4` (oid 4278).
fn fc_hash_multirange(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mr = getarg_multirange(fcinfo, m.mcx(), 0);
    let rangetyp = rangetyp_of(mr);
    // PG_RETURN_INT32 of a uint32 hash word (reinterpret).
    Datum::from_i32(ok(crate::setops_ordering_agg::hash_multirange(&rangetyp, mr)) as i32)
}

/// `hash_multirange_extended(anymultirange, int8) -> int8` (oid 4279).
fn fc_hash_multirange_extended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mr = getarg_multirange(fcinfo, m.mcx(), 0);
    let seed = arg_int64(fcinfo, 1) as u64;
    let rangetyp = rangetyp_of(mr);
    Datum::from_i64(ok(crate::setops_ordering_agg::hash_multirange_extended(
        &rangetyp, mr, seed,
    )) as i64)
}

// ---------------------------------------------------------------------------
// constructors (multirangetypes.c GENERIC FUNCTIONS). The result multirange
// type OID is read off `flinfo->fn_expr` (`get_fn_expr_rettype`), so a real
// planned call frame is required; the typcache's `->rngtype` is the element
// range type. C dispatches these polymorphically by return type.
// ---------------------------------------------------------------------------

/// `multirange_constructor0() -> anymultirange` (oid 4280 + per-type dups).
fn fc_multirange_constructor0(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mltrngtypoid = get_fn_expr_rettype(fcinfo.flinfo.as_deref());
    let rangetyp = rangetyp_for_mltrng(mltrngtypoid);
    let nargs = fcinfo.nargs() as i32;
    let mr = ok(crate::serialize_core::multirange_constructor0(
        m.mcx(),
        mltrngtypoid,
        &rangetyp,
        nargs,
    ));
    ret_multirange(fcinfo, mr)
}

/// `multirange_constructor1(anyrange) -> anymultirange` (oid 4281 + per-type
/// dups). The single member range arg crosses on the by-ref lane.
fn fc_multirange_constructor1(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mltrngtypoid = get_fn_expr_rettype(fcinfo.flinfo.as_deref());
    let rangetyp = rangetyp_for_mltrng(mltrngtypoid);
    let range_isnull = fcinfo.arg(0).map(|nd| nd.isnull).unwrap_or(true);
    // The range arg's by-value word: stage the by-ref image to a pointer word so
    // the kernel's `datum_get_range_type_p` resolves it (NULL word if absent).
    let range_word = match fcinfo.ref_arg(0).and_then(|p| p.as_varlena()) {
        Some(image) => ok(mr_bytes_to_arg_word(m.mcx(), image)),
        None => Datum::null(),
    };
    let mr = ok(crate::serialize_core::multirange_constructor1(
        m.mcx(),
        mltrngtypoid,
        &rangetyp,
        range_isnull,
        range_word,
    ));
    ret_multirange(fcinfo, mr)
}

/// `multirange_constructor2(variadic anyrange[]) -> anymultirange` (oid 4282 +
/// per-type dups). The member-range array arrives as the `anyrange[]` arg word
/// (a varlena image on the by-ref lane).
fn fc_multirange_constructor2(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mltrngtypoid = get_fn_expr_rettype(fcinfo.flinfo.as_deref());
    let rangetyp = rangetyp_for_mltrng(mltrngtypoid);
    let nargs = fcinfo.nargs() as i32;
    let array_isnull = fcinfo.arg(0).map(|nd| nd.isnull).unwrap_or(true);
    let array_word = match fcinfo.ref_arg(0).and_then(|p| p.as_varlena()) {
        Some(image) => ok(mr_bytes_to_arg_word(m.mcx(), image)),
        None => fcinfo.arg(0).map(|nd| nd.value).unwrap_or_else(Datum::null),
    };
    let mr = ok(crate::serialize_core::multirange_constructor2(
        m.mcx(),
        mltrngtypoid,
        &rangetyp,
        nargs,
        array_isnull,
        array_word,
    ));
    ret_multirange(fcinfo, mr)
}

// ---------------------------------------------------------------------------
// accessors (single multirange -> element / bool).
// ---------------------------------------------------------------------------

/// Body of a `multirange -> bool` accessor over a
/// `fn(&TypeCacheEntry, MultirangeTypeP) -> PgResult<bool>` core.
macro_rules! fc_mr_pred {
    ($fc:ident, $core:path) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let m = scratch_mcx();
            let mr = getarg_multirange(fcinfo, m.mcx(), 0);
            let rangetyp = rangetyp_of(mr);
            Datum::from_bool(ok($core(&rangetyp, mr)))
        }
    };
}

fc_mr_pred!(fc_multirange_lower_inc, crate::operators::multirange_lower_inc);
fc_mr_pred!(fc_multirange_upper_inc, crate::operators::multirange_upper_inc);
fc_mr_pred!(fc_multirange_lower_inf, crate::operators::multirange_lower_inf);
fc_mr_pred!(fc_multirange_upper_inf, crate::operators::multirange_upper_inf);

/// Body of a `multirange -> anyelement` accessor (`multirange_lower`/`_upper`).
/// A SQL-NULL (empty/unbounded) result sets `fcinfo->isnull`.
macro_rules! fc_mr_bound {
    ($fc:ident, $core:path) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let m = scratch_mcx();
            let mr = getarg_multirange(fcinfo, m.mcx(), 0);
            let rangetyp = rangetyp_of(mr);
            match ok($core(&rangetyp, mr)) {
                Some(d) => d,
                None => {
                    fcinfo.set_result_null(true);
                    Datum::null()
                }
            }
        }
    };
}

fc_mr_bound!(fc_multirange_lower, crate::operators::multirange_lower);
fc_mr_bound!(fc_multirange_upper, crate::operators::multirange_upper);

// ---------------------------------------------------------------------------
// element / range / multirange containment & position operators -> bool.
// The element-range typcache (`->rngtype`) keys every comparison; it is read off
// whichever arg is a multirange (its own header OID).
// ---------------------------------------------------------------------------

/// `multirange_contains_elem(anymultirange, anyelement) -> bool` (oid 4249).
fn fc_multirange_contains_elem(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mr = getarg_multirange(fcinfo, m.mcx(), 0);
    let val = fcinfo.arg(1).map(|nd| nd.value).unwrap_or_else(Datum::null);
    let rangetyp = rangetyp_of(mr);
    Datum::from_bool(ok(crate::operators::multirange_contains_elem_internal(
        &rangetyp, mr, val,
    )))
}

/// `elem_contained_by_multirange(anyelement, anymultirange) -> bool` (oid 4252).
fn fc_elem_contained_by_multirange(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let val = fcinfo.arg(0).map(|nd| nd.value).unwrap_or_else(Datum::null);
    let mr = getarg_multirange(fcinfo, m.mcx(), 1);
    let rangetyp = rangetyp_of(mr);
    Datum::from_bool(ok(crate::operators::multirange_contains_elem_internal(
        &rangetyp, mr, val,
    )))
}

/// `(multirange@0, range@1) -> bool` via `multirange_contains_range_internal`.
macro_rules! fc_mr_contains_range {
    ($fc:ident) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let m = scratch_mcx();
            let mr = getarg_multirange(fcinfo, m.mcx(), 0);
            let r = getarg_range(fcinfo, m.mcx(), 1);
            let rangetyp = rangetyp_of(mr);
            Datum::from_bool(ok(crate::operators::multirange_contains_range_internal(
                &rangetyp, mr, r,
            )))
        }
    };
}

// `multirange_contains_range(anymultirange, anyrange) -> bool` (oid 4250).
fc_mr_contains_range!(fc_multirange_contains_range);

/// `(range@0, multirange@1) -> bool` via `range_contains_multirange_internal`.
macro_rules! fc_range_contains_mr {
    ($fc:ident) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let m = scratch_mcx();
            let r = getarg_range(fcinfo, m.mcx(), 0);
            let mr = getarg_multirange(fcinfo, m.mcx(), 1);
            let rangetyp = rangetyp_of(mr);
            Datum::from_bool(ok(crate::operators::range_contains_multirange_internal(
                &rangetyp, r, mr,
            )))
        }
    };
}

// `range_contains_multirange(anyrange, anymultirange) -> bool` (oid 4541).
fc_range_contains_mr!(fc_range_contains_multirange);
/// `range_contained_by_multirange(anyrange, anymultirange) -> bool` (oid 4253):
/// C calls `multirange_contains_range_internal(rngtyp, mr, r)` (mr@1, r@0).
fn fc_range_contained_by_multirange(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let r = getarg_range(fcinfo, m.mcx(), 0);
    let mr = getarg_multirange(fcinfo, m.mcx(), 1);
    let rangetyp = rangetyp_of(mr);
    Datum::from_bool(ok(crate::operators::multirange_contains_range_internal(
        &rangetyp, mr, r,
    )))
}

/// `multirange_contained_by_range(anymultirange, anyrange) -> bool` (oid 4542):
/// C calls `range_contains_multirange_internal(rngtyp, r, mr)` (mr@0, r@1).
fn fc_multirange_contained_by_range(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mr = getarg_multirange(fcinfo, m.mcx(), 0);
    let r = getarg_range(fcinfo, m.mcx(), 1);
    let rangetyp = rangetyp_of(mr);
    Datum::from_bool(ok(crate::operators::range_contains_multirange_internal(
        &rangetyp, r, mr,
    )))
}

/// `multirange_contains_multirange(anymultirange, anymultirange) -> bool` (4251).
fn fc_multirange_contains_multirange(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mr1 = getarg_multirange(fcinfo, m.mcx(), 0);
    let mr2 = getarg_multirange(fcinfo, m.mcx(), 1);
    let rangetyp = rangetyp_of(mr1);
    Datum::from_bool(ok(
        crate::operators::multirange_contains_multirange_internal(&rangetyp, mr1, mr2),
    ))
}

/// `multirange_contained_by_multirange(anymultirange, anymultirange) -> bool`
/// (4254): C swaps the args into `multirange_contains_multirange_internal`.
fn fc_multirange_contained_by_multirange(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mr1 = getarg_multirange(fcinfo, m.mcx(), 0);
    let mr2 = getarg_multirange(fcinfo, m.mcx(), 1);
    let rangetyp = rangetyp_of(mr1);
    Datum::from_bool(ok(
        crate::operators::multirange_contains_multirange_internal(&rangetyp, mr2, mr1),
    ))
}

// --- overlaps --------------------------------------------------------------

/// `range_overlaps_multirange(anyrange, anymultirange) -> bool` (oid 4246).
fn fc_range_overlaps_multirange(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let r = getarg_range(fcinfo, m.mcx(), 0);
    let mr = getarg_multirange(fcinfo, m.mcx(), 1);
    let rangetyp = rangetyp_of(mr);
    Datum::from_bool(ok(crate::operators::range_overlaps_multirange_internal(
        &rangetyp, r, mr,
    )))
}

/// `multirange_overlaps_range(anymultirange, anyrange) -> bool` (oid 4247).
fn fc_multirange_overlaps_range(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mr = getarg_multirange(fcinfo, m.mcx(), 0);
    let r = getarg_range(fcinfo, m.mcx(), 1);
    let rangetyp = rangetyp_of(mr);
    Datum::from_bool(ok(crate::operators::range_overlaps_multirange_internal(
        &rangetyp, r, mr,
    )))
}

/// `multirange_overlaps_multirange(anymultirange, anymultirange) -> bool` (4248).
fn fc_multirange_overlaps_multirange(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mr1 = getarg_multirange(fcinfo, m.mcx(), 0);
    let mr2 = getarg_multirange(fcinfo, m.mcx(), 1);
    let rangetyp = rangetyp_of(mr1);
    Datum::from_bool(ok(
        crate::operators::multirange_overlaps_multirange_internal(&rangetyp, mr1, mr2),
    ))
}

// --- overleft / overright (range/multirange mixes) -------------------------

/// `range_overleft_multirange(anyrange, anymultirange) -> bool` (oid 4264).
fn fc_range_overleft_multirange(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let r = getarg_range(fcinfo, m.mcx(), 0);
    let mr = getarg_multirange(fcinfo, m.mcx(), 1);
    let rangetyp = rangetyp_of(mr);
    Datum::from_bool(ok(crate::operators::range_overleft_multirange_internal(
        &rangetyp, r, mr,
    )))
}

/// `multirange_overleft_range(anymultirange, anyrange) -> bool` (oid 4265).
fn fc_multirange_overleft_range(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mr = getarg_multirange(fcinfo, m.mcx(), 0);
    let r = getarg_range(fcinfo, m.mcx(), 1);
    let rangetyp = rangetyp_of(mr);
    Datum::from_bool(ok(crate::operators::multirange_overleft_range_internal(
        &rangetyp, mr, r,
    )))
}

/// `multirange_overleft_multirange(anymultirange, anymultirange) -> bool` (4266).
fn fc_multirange_overleft_multirange(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mr1 = getarg_multirange(fcinfo, m.mcx(), 0);
    let mr2 = getarg_multirange(fcinfo, m.mcx(), 1);
    let rangetyp = rangetyp_of(mr1);
    Datum::from_bool(ok(
        crate::operators::multirange_overleft_multirange_internal(&rangetyp, mr1, mr2),
    ))
}

/// `range_overright_multirange(anyrange, anymultirange) -> bool` (oid 4267).
fn fc_range_overright_multirange(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let r = getarg_range(fcinfo, m.mcx(), 0);
    let mr = getarg_multirange(fcinfo, m.mcx(), 1);
    let rangetyp = rangetyp_of(mr);
    Datum::from_bool(ok(crate::operators::range_overright_multirange_internal(
        &rangetyp, r, mr,
    )))
}

/// `multirange_overright_range(anymultirange, anyrange) -> bool` (oid 4268).
fn fc_multirange_overright_range(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mr = getarg_multirange(fcinfo, m.mcx(), 0);
    let r = getarg_range(fcinfo, m.mcx(), 1);
    let rangetyp = rangetyp_of(mr);
    Datum::from_bool(ok(crate::operators::multirange_overright_range_internal(
        &rangetyp, mr, r,
    )))
}

/// `multirange_overright_multirange(anymultirange, anymultirange) -> bool` (4269).
fn fc_multirange_overright_multirange(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mr1 = getarg_multirange(fcinfo, m.mcx(), 0);
    let mr2 = getarg_multirange(fcinfo, m.mcx(), 1);
    let rangetyp = rangetyp_of(mr1);
    Datum::from_bool(ok(
        crate::operators::multirange_overright_multirange_internal(&rangetyp, mr1, mr2),
    ))
}

// --- before / after (range/multirange mixes) -------------------------------

/// `range_before_multirange(anyrange, anymultirange) -> bool` (oid 4258).
fn fc_range_before_multirange(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let r = getarg_range(fcinfo, m.mcx(), 0);
    let mr = getarg_multirange(fcinfo, m.mcx(), 1);
    let rangetyp = rangetyp_of(mr);
    Datum::from_bool(ok(crate::operators::range_before_multirange_internal(
        &rangetyp, r, mr,
    )))
}

/// `multirange_before_range(anymultirange, anyrange) -> bool` (oid 4259): C
/// calls `range_after_multirange_internal(rngtyp, r, mr)` (r@1, mr@0).
fn fc_multirange_before_range(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mr = getarg_multirange(fcinfo, m.mcx(), 0);
    let r = getarg_range(fcinfo, m.mcx(), 1);
    let rangetyp = rangetyp_of(mr);
    Datum::from_bool(ok(crate::operators::range_after_multirange_internal(
        &rangetyp, r, mr,
    )))
}

/// `range_after_multirange(anyrange, anymultirange) -> bool` (oid 4261).
fn fc_range_after_multirange(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let r = getarg_range(fcinfo, m.mcx(), 0);
    let mr = getarg_multirange(fcinfo, m.mcx(), 1);
    let rangetyp = rangetyp_of(mr);
    Datum::from_bool(ok(crate::operators::range_after_multirange_internal(
        &rangetyp, r, mr,
    )))
}

/// `multirange_after_range(anymultirange, anyrange) -> bool` (oid 4262): C calls
/// `range_before_multirange_internal(rngtyp, r, mr)` (r@1, mr@0).
fn fc_multirange_after_range(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mr = getarg_multirange(fcinfo, m.mcx(), 0);
    let r = getarg_range(fcinfo, m.mcx(), 1);
    let rangetyp = rangetyp_of(mr);
    Datum::from_bool(ok(crate::operators::range_before_multirange_internal(
        &rangetyp, r, mr,
    )))
}

/// `multirange_before_multirange(anymultirange, anymultirange) -> bool` (4260).
fn fc_multirange_before_multirange(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mr1 = getarg_multirange(fcinfo, m.mcx(), 0);
    let mr2 = getarg_multirange(fcinfo, m.mcx(), 1);
    let rangetyp = rangetyp_of(mr1);
    Datum::from_bool(ok(
        crate::operators::multirange_before_multirange_internal(&rangetyp, mr1, mr2),
    ))
}

/// `multirange_after_multirange(anymultirange, anymultirange) -> bool` (4263): C
/// calls `multirange_before_multirange_internal(rngtyp, mr2, mr1)`.
fn fc_multirange_after_multirange(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mr1 = getarg_multirange(fcinfo, m.mcx(), 0);
    let mr2 = getarg_multirange(fcinfo, m.mcx(), 1);
    let rangetyp = rangetyp_of(mr1);
    Datum::from_bool(ok(
        crate::operators::multirange_before_multirange_internal(&rangetyp, mr2, mr1),
    ))
}

// --- adjacent --------------------------------------------------------------

/// `range_adjacent_multirange(anyrange, anymultirange) -> bool` (oid 4255).
fn fc_range_adjacent_multirange(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let r = getarg_range(fcinfo, m.mcx(), 0);
    let mr = getarg_multirange(fcinfo, m.mcx(), 1);
    let rangetyp = rangetyp_of(mr);
    Datum::from_bool(ok(crate::operators::range_adjacent_multirange_internal(
        &rangetyp, r, mr,
    )))
}

/// `multirange_adjacent_range(anymultirange, anyrange) -> bool` (oid 4257): C
/// calls `range_adjacent_multirange_internal(rngtyp, r, mr)` (r@1, mr@0).
fn fc_multirange_adjacent_range(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mr = getarg_multirange(fcinfo, m.mcx(), 0);
    let r = getarg_range(fcinfo, m.mcx(), 1);
    let rangetyp = rangetyp_of(mr);
    Datum::from_bool(ok(crate::operators::range_adjacent_multirange_internal(
        &rangetyp, r, mr,
    )))
}

/// `multirange_adjacent_multirange(anymultirange, anymultirange) -> bool` (4256).
fn fc_multirange_adjacent_multirange(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mr1 = getarg_multirange(fcinfo, m.mcx(), 0);
    let mr2 = getarg_multirange(fcinfo, m.mcx(), 1);
    let rangetyp = rangetyp_of(mr1);
    Datum::from_bool(ok(
        crate::operators::multirange_adjacent_multirange_internal(&rangetyp, mr1, mr2),
    ))
}

// ---------------------------------------------------------------------------
// set operations (multirange, multirange) -> multirange; range_merge -> range.
// ---------------------------------------------------------------------------

/// Body of a `(multirange, multirange) -> multirange` set op around a
/// `fn(Mcx, &TypeCacheEntry, MultirangeTypeP, MultirangeTypeP) -> PgResult<MultirangeTypeP>`.
macro_rules! fc_mr_setop {
    ($fc:ident, $core:path) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
            let m = scratch_mcx();
            let mr1 = getarg_multirange(fcinfo, m.mcx(), 0);
            let mr2 = getarg_multirange(fcinfo, m.mcx(), 1);
            let rangetyp = rangetyp_of(mr1);
            let out = ok($core(m.mcx(), &rangetyp, mr1, mr2));
            ret_multirange(fcinfo, out)
        }
    };
}

fc_mr_setop!(fc_multirange_union, crate::setops_ordering_agg::multirange_union);
fc_mr_setop!(fc_multirange_minus, crate::setops_ordering_agg::multirange_minus);
fc_mr_setop!(
    fc_multirange_intersect,
    crate::setops_ordering_agg::multirange_intersect
);

/// `range_merge_from_multirange(anymultirange) -> anyrange` (oid 4228).
fn fc_range_merge_from_multirange(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mr = getarg_multirange(fcinfo, m.mcx(), 0);
    let rangetyp = rangetyp_of(mr);
    let r = ok(crate::setops_ordering_agg::range_merge_from_multirange(
        m.mcx(),
        &rangetyp,
        mr,
    ));
    ret_range(fcinfo, r)
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    strict: bool,
    retset: bool,
    func: fn(&mut FunctionCallInfoBaseData) -> Datum,
) -> BuiltinFunction {
    BuiltinFunction {
        foid,
        name: name.to_string(),
        nargs,
        strict,
        retset,
        func: Some(func),
    }
}

/// Register the expressible `multirangetypes.c` builtins (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
/// OIDs/nargs/strict/retset transcribed exactly from `pg_proc.dat`; all are
/// `proisstrict => 't'` default and none `proretset`.
///
/// NOT registered here (genuinely keystone-gated, not this lever):
/// - `multirange_intersect_agg_transfn` / `range_agg_*` / `multirange_agg_transfn`
///   aggregate fns (4299/4300/4388/6225/6226): need the `AggCheckCallContext`
///   call-context channel (#324/#335), absent from the `types_fmgr` frame.
/// - `multirange_unnest` (1293): a set-returning function (`proretset`); the fmgr
///   boundary (`fn(&mut fcinfo) -> Datum`) cannot express the ValuePerCall SRF
///   protocol. Its kernel is fully ported; only the SRF surface is gated.
/// - `multirange_typanalyze` / `multirangesel` / the GiST support fns: take an
///   `internal` (`VacAttrStats`/`PlannerInfo`/`GISTENTRY`) executor-owned scratch
///   struct, not expressible on the by-ref boundary.
pub fn register_multirangetypes_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // I/O: cstring/internal/bytea <-> anymultirange.
        builtin(4231, "multirange_in", 3, true, false, fc_multirange_in),
        builtin(4232, "multirange_out", 1, true, false, fc_multirange_out),
        builtin(4233, "multirange_recv", 3, true, false, fc_multirange_recv),
        builtin(4234, "multirange_send", 1, true, false, fc_multirange_send),
        // constructors (polymorphic by return type via get_fn_expr_rettype). One
        // (oid) row per built-in multirange type, all sharing the same kernel.
        builtin(4280, "multirange_constructor0", 0, true, false, fc_multirange_constructor0),
        builtin(4281, "multirange_constructor1", 1, true, false, fc_multirange_constructor1),
        builtin(4282, "multirange_constructor2", 1, true, false, fc_multirange_constructor2),
        builtin(4283, "multirange_constructor0", 0, true, false, fc_multirange_constructor0),
        builtin(4284, "multirange_constructor1", 1, true, false, fc_multirange_constructor1),
        builtin(4285, "multirange_constructor2", 1, true, false, fc_multirange_constructor2),
        builtin(4286, "multirange_constructor0", 0, true, false, fc_multirange_constructor0),
        builtin(4287, "multirange_constructor1", 1, true, false, fc_multirange_constructor1),
        builtin(4288, "multirange_constructor2", 1, true, false, fc_multirange_constructor2),
        builtin(4289, "multirange_constructor0", 0, true, false, fc_multirange_constructor0),
        builtin(4290, "multirange_constructor1", 1, true, false, fc_multirange_constructor1),
        builtin(4291, "multirange_constructor2", 1, true, false, fc_multirange_constructor2),
        builtin(4292, "multirange_constructor0", 0, true, false, fc_multirange_constructor0),
        builtin(4293, "multirange_constructor1", 1, true, false, fc_multirange_constructor1),
        builtin(4294, "multirange_constructor2", 1, true, false, fc_multirange_constructor2),
        builtin(4295, "multirange_constructor0", 0, true, false, fc_multirange_constructor0),
        builtin(4296, "multirange_constructor1", 1, true, false, fc_multirange_constructor1),
        builtin(4297, "multirange_constructor2", 1, true, false, fc_multirange_constructor2),
        builtin(4298, "multirange_constructor1", 1, true, false, fc_multirange_constructor1),
        // accessors: bound value (anyelement) / inclusivity / infinity -> bool.
        builtin(4235, "multirange_lower", 1, true, false, fc_multirange_lower),
        builtin(4236, "multirange_upper", 1, true, false, fc_multirange_upper),
        builtin(4237, "multirange_empty", 1, true, false, fc_multirange_empty),
        builtin(4238, "multirange_lower_inc", 1, true, false, fc_multirange_lower_inc),
        builtin(4239, "multirange_upper_inc", 1, true, false, fc_multirange_upper_inc),
        builtin(4240, "multirange_lower_inf", 1, true, false, fc_multirange_lower_inf),
        builtin(4241, "multirange_upper_inf", 1, true, false, fc_multirange_upper_inf),
        // element / range / multirange containment -> bool.
        builtin(4249, "multirange_contains_elem", 2, true, false, fc_multirange_contains_elem),
        builtin(4252, "elem_contained_by_multirange", 2, true, false, fc_elem_contained_by_multirange),
        builtin(4250, "multirange_contains_range", 2, true, false, fc_multirange_contains_range),
        builtin(4541, "range_contains_multirange", 2, true, false, fc_range_contains_multirange),
        builtin(4253, "range_contained_by_multirange", 2, true, false, fc_range_contained_by_multirange),
        builtin(4542, "multirange_contained_by_range", 2, true, false, fc_multirange_contained_by_range),
        builtin(4251, "multirange_contains_multirange", 2, true, false, fc_multirange_contains_multirange),
        builtin(4254, "multirange_contained_by_multirange", 2, true, false, fc_multirange_contained_by_multirange),
        // overlaps -> bool.
        builtin(4246, "range_overlaps_multirange", 2, true, false, fc_range_overlaps_multirange),
        builtin(4247, "multirange_overlaps_range", 2, true, false, fc_multirange_overlaps_range),
        builtin(4248, "multirange_overlaps_multirange", 2, true, false, fc_multirange_overlaps_multirange),
        // overleft / overright -> bool.
        builtin(4264, "range_overleft_multirange", 2, true, false, fc_range_overleft_multirange),
        builtin(4265, "multirange_overleft_range", 2, true, false, fc_multirange_overleft_range),
        builtin(4266, "multirange_overleft_multirange", 2, true, false, fc_multirange_overleft_multirange),
        builtin(4267, "range_overright_multirange", 2, true, false, fc_range_overright_multirange),
        builtin(4268, "multirange_overright_range", 2, true, false, fc_multirange_overright_range),
        builtin(4269, "multirange_overright_multirange", 2, true, false, fc_multirange_overright_multirange),
        // before / after -> bool.
        builtin(4258, "range_before_multirange", 2, true, false, fc_range_before_multirange),
        builtin(4259, "multirange_before_range", 2, true, false, fc_multirange_before_range),
        builtin(4260, "multirange_before_multirange", 2, true, false, fc_multirange_before_multirange),
        builtin(4261, "range_after_multirange", 2, true, false, fc_range_after_multirange),
        builtin(4262, "multirange_after_range", 2, true, false, fc_multirange_after_range),
        builtin(4263, "multirange_after_multirange", 2, true, false, fc_multirange_after_multirange),
        // adjacent -> bool.
        builtin(4255, "range_adjacent_multirange", 2, true, false, fc_range_adjacent_multirange),
        builtin(4257, "multirange_adjacent_range", 2, true, false, fc_multirange_adjacent_range),
        builtin(4256, "multirange_adjacent_multirange", 2, true, false, fc_multirange_adjacent_multirange),
        // (multirange, multirange) -> bool (eq/ne/ordering).
        builtin(4244, "multirange_eq", 2, true, false, fc_multirange_eq),
        builtin(4245, "multirange_ne", 2, true, false, fc_multirange_ne),
        builtin(4274, "multirange_lt", 2, true, false, fc_multirange_lt),
        builtin(4275, "multirange_le", 2, true, false, fc_multirange_le),
        builtin(4276, "multirange_ge", 2, true, false, fc_multirange_ge),
        builtin(4277, "multirange_gt", 2, true, false, fc_multirange_gt),
        // (multirange, multirange) -> int4 (3-way compare).
        builtin(4273, "multirange_cmp", 2, true, false, fc_multirange_cmp),
        // set operations -> multirange / range.
        builtin(4270, "multirange_union", 2, true, false, fc_multirange_union),
        builtin(4271, "multirange_minus", 2, true, false, fc_multirange_minus),
        builtin(4272, "multirange_intersect", 2, true, false, fc_multirange_intersect),
        builtin(4228, "range_merge_from_multirange", 1, true, false, fc_range_merge_from_multirange),
        // hash -> int4 / int8.
        builtin(4278, "hash_multirange", 1, true, false, fc_hash_multirange),
        builtin(
            4279,
            "hash_multirange_extended",
            2,
            true,
            false,
            fc_hash_multirange_extended,
        ),
    ]);
}

// ===========================================================================
// End-to-end proof: a by-reference `anymultirange` builtin is genuinely callable
// through the fmgr registry. We install a synthetic `int4multirange` typcache
// seam (its `->rngtype` is the `int4range` typcache, whose `->rngelemtype` is
// `int4`), build empty multiranges via the real `make_empty_multirange`/
// `make_multirange` kernels, then drive the registered builtins BY OID through
// `fmgr_isbuiltin`. Empty-vs-empty equality/compare and `multirange_empty`
// short-circuit before any subtype compare proc.
// ===========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use types_datum::NullableDatum;

    const TEST_MR_OID: u32 = 4451; // pg_type int4multirange
    const TEST_RANGE_OID: u32 = 3904; // pg_type int4range
    const TEST_ELEM_OID: u32 = 23; // int4

    /// A synthetic `int4range` typcache (element int4, 4-byte by-value).
    fn int4_range_typcache() -> TypeCacheEntry {
        let mut elem = TypeCacheEntry::default();
        elem.type_id = TEST_ELEM_OID;
        elem.typlen = 4;
        elem.typbyval = true;
        elem.typalign = b'i' as i8;
        elem.typstorage = b'p' as i8;

        let mut rng = TypeCacheEntry::default();
        rng.type_id = TEST_RANGE_OID;
        rng.typlen = -1;
        rng.typbyval = false;
        rng.typalign = b'd' as i8;
        rng.typstorage = b'x' as i8;
        rng.rngelemtype = Some(Box::new(elem));
        rng
    }

    /// A synthetic `int4multirange` typcache whose `->rngtype` is the int4range.
    fn int4_multirange_typcache() -> TypeCacheEntry {
        let mut mr = TypeCacheEntry::default();
        mr.type_id = TEST_MR_OID;
        mr.typlen = -1;
        mr.typbyval = false;
        mr.typalign = b'd' as i8;
        mr.typstorage = b'x' as i8;
        mr.rngtype = Some(Box::new(int4_range_typcache()));
        mr
    }

    fn install_test_seams() {
        use backend_utils_cache_typcache_seams as ts;
        use backend_utils_fmgr_fmgr_seams as fs;
        // The multirange typcache lookup keyed by multirange OID; the range I/O
        // path also looks up the range OID. Dispatch on the requested OID.
        if !ts::lookup_type_cache_entry::is_installed() {
            ts::lookup_type_cache_entry::set(|type_id, _flags| {
                if type_id == TEST_RANGE_OID {
                    Ok(int4_range_typcache())
                } else {
                    Ok(int4_multirange_typcache())
                }
            });
        }
        // `make_multirange` canonicalizes member ranges, comparing bounds via
        // `range_cmp_bounds` -> `function_call2_coll`; the int4 stand-in cmp.
        if !fs::function_call2_coll::is_installed() {
            fs::function_call2_coll::set(|_fid, _coll, a, b| {
                Ok(Datum::from_i32((a.as_i32()).cmp(&b.as_i32()) as i32))
            });
        }
        // `range_cmp_bounds`/`range_cmp_bound_values` now compare element bounds
        // through the by-reference-capable `function_call2_coll_datum` lane (the
        // by-ref element fix); install the int4 stand-in over the canonical
        // `Datum` arg form (the int4 element is by-value -> the `ByVal` word).
        if !fs::function_call2_coll_datum::is_installed() {
            fs::function_call2_coll_datum::set(|mcx, _fid, _coll, a, b| {
                let av = a.as_usize() as i32;
                let bv = b.as_usize() as i32;
                let _ = mcx;
                Ok(types_tuple::backend_access_common_heaptuple::Datum::from_usize(
                    (av.cmp(&bv) as i32) as usize,
                ))
            });
        }
        register_multirangetypes_builtins();
    }

    /// Build an empty `int4multirange` image (the by-ref lane form) via the real
    /// `make_empty_multirange` kernel.
    fn empty_multirange_image() -> Vec<u8> {
        install_test_seams();
        let rng = int4_range_typcache();
        let m = MemoryContext::new("test empty multirange");
        let mr =
            crate::serialize_core::make_empty_multirange(m.mcx(), TEST_MR_OID, &rng).expect("empty mr");
        unsafe { mr_word_to_result_bytes(Datum::from_usize(mr.ptr as usize)) }
    }

    fn call_mr_cmp_bool(oid: u32, a: &[u8], b: &[u8]) -> bool {
        install_test_seams();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),
            NullableDatum::value(Datum::null()),
        ];
        fcinfo.ref_args = vec![
            Some(RefPayload::Varlena(a.to_vec())),
            Some(RefPayload::Varlena(b.to_vec())),
        ];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid).expect("builtin registered");
        (entry.func.unwrap())(&mut fcinfo).as_bool()
    }

    fn call_mr_cmp_i32(oid: u32, a: &[u8], b: &[u8]) -> i32 {
        install_test_seams();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo.args = vec![
            NullableDatum::value(Datum::null()),
            NullableDatum::value(Datum::null()),
        ];
        fcinfo.ref_args = vec![
            Some(RefPayload::Varlena(a.to_vec())),
            Some(RefPayload::Varlena(b.to_vec())),
        ];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid).expect("builtin registered");
        (entry.func.unwrap())(&mut fcinfo).as_i32()
    }

    fn call_mr_pred(oid: u32, a: &[u8]) -> bool {
        install_test_seams();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(a.to_vec()))];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid).expect("builtin registered");
        (entry.func.unwrap())(&mut fcinfo).as_bool()
    }

    #[test]
    fn multirange_builtins_are_registered() {
        register_multirangetypes_builtins();
        for oid in [
            // I/O + ordering + hash (original surface).
            4231u32, 4232, 4233, 4234, 4237, 4244, 4273, 4278, 4279,
            // constructors (one row per built-in multirange type).
            4280, 4281, 4282, 4298,
            // accessors.
            4235, 4236, 4238, 4239, 4240, 4241,
            // containment / overlap / position / adjacency.
            4249, 4252, 4250, 4541, 4253, 4542, 4251, 4254, 4246, 4247, 4248, 4264, 4265, 4266,
            4267, 4268, 4269, 4258, 4259, 4260, 4261, 4262, 4263, 4255, 4257, 4256,
            // set operations.
            4270, 4271, 4272, 4228,
        ] {
            assert!(
                backend_utils_fmgr_core::fmgr_isbuiltin(oid).is_some(),
                "multirange builtin {oid} should be registered"
            );
        }
    }

    /// THE PROOF: `multirange_eq`/`multirange_ne`/`multirange_cmp`/`isempty` over
    /// empty multiranges, entirely through the fmgr registry by OID with
    /// `anymultirange` args crossing on the by-reference lane.
    #[test]
    fn byref_multirange_empty_through_registry() {
        let e1 = empty_multirange_image();
        let e2 = empty_multirange_image();
        // multirange_eq (4244): empty == empty -> true; ne (4245) -> false.
        assert!(call_mr_cmp_bool(4244, &e1, &e2));
        assert!(!call_mr_cmp_bool(4245, &e1, &e2));
        // multirange_cmp (4273): empty <=> empty == 0; lt/gt false.
        assert_eq!(call_mr_cmp_i32(4273, &e1, &e2), 0);
        assert!(!call_mr_cmp_bool(4274, &e1, &e2)); // lt
        assert!(!call_mr_cmp_bool(4277, &e1, &e2)); // gt
                                                    // le/ge true.
        assert!(call_mr_cmp_bool(4275, &e1, &e2)); // le
        assert!(call_mr_cmp_bool(4276, &e1, &e2)); // ge
                                                   // isempty (4237): an empty multirange is empty.
        assert!(call_mr_pred(4237, &e1));
    }
}
