//! Family `range-fmgr-boundary`: the `PG_FUNCTION_ARGS` entry points.
//!
//! Each `*` mirrors one `Datum fn(PG_FUNCTION_ARGS)` from `rangetypes.c`,
//! marshalling `Datum` <-> typed args, then delegating to the kernel in the
//! relevant family. This layer is deliberately thin: no range logic lives here.
//!
//! Because every range allocation in C is charged to `CurrentMemoryContext` and
//! this repo carries no ambient context, each entry takes the result `Mcx<'mcx>`
//! as a parameter (the universal "allocating fns take `Mcx` + return `PgResult`"
//! rule) instead of the bare C `Datum fn(FunctionCallInfo)` signature. C's
//! `ereport(ERROR)`/`elog(ERROR)` longjmp surfaces as `Err(PgError)`; a SQL-NULL
//! result (`PG_RETURN_NULL`) sets `fcinfo->isnull` and returns `Datum(0)`.
//!
//! In-unit kernels (`range_io` / `range_repr_serialize` / `range_bounds_compare`
//! / `range_setops` / `range_canonical_subdiff_hash` / `range_planner_support`)
//! are called directly. Genuinely-external neighbors are reached through their owners
//! (`get_fn_expr_*` from fmgr-core, `text_to_cstring`/typcache via their seams),
//! or — where no compatible seam exists yet — by a loud owner-named panic
//! (`AggCheckCallContext`), the sanctioned seam-and-panic for an unported dep.

use ::fmgr_core::{get_fn_expr_argtype, get_fn_expr_rettype};
use ::mcx::Mcx;
use ::datum::Datum;
use ::types_error::{PgError, PgResult, ERRCODE_DATA_EXCEPTION};
use ::fmgr::FunctionCallInfoBaseData;
use ::types_rangetypes::{
    RangeBound, RangeType, RangeTypeP, RANGE_EMPTY, RANGE_LB_INC, RANGE_LB_INF, RANGE_UB_INC,
    RANGE_UB_INF,
};

use crate::range_bounds_compare::{
    range_after_internal, range_before_internal, range_adjacent_internal,
    range_contained_by_internal, range_contains_elem_internal, range_contains_internal,
    range_eq_internal, range_get_typcache, range_ne_internal, range_overlaps_internal,
    range_overleft_internal, range_overright_internal,
};
use crate::range_canonical_subdiff_hash::{
    daterange_canonical, hash_range as hash_range_kernel,
    hash_range_extended as hash_range_extended_kernel, int4range_canonical, int8range_canonical,
    range_cmp as range_cmp_kernel,
};
use crate::range_io::{get_range_io_data, IOFuncSelector};
use crate::range_io::{
    range_in as range_in_kernel, range_out as range_out_kernel, range_recv as range_recv_kernel,
    range_send as range_send_kernel,
};
// The `range-planner-support` kernels (`elem_contained_by_range_support` /
// `range_contains_elem_support`) now run over the REAL `(root, FuncExpr)`
// request and are reached through the value-typed `call_support_simplify`
// dispatch (wired in `seams-init`), not the fmgr-`Node*`-request boundary: the
// `OidFunctionCall1(prosupport, PointerGetDatum(&SupportRequestSimplify))`
// convention (a `Node*` argument carried in a `Datum` word) is not modeled on
// the ported fmgr surface, so the two support fns are NOT registered as fmgr
// builtins here (their `pg_proc` rows 6345/6346 stay on the
// `builtin_gap_baseline` allowlist until that fmgr-support protocol lands).
use crate::range_repr_serialize::{make_range, range_get_flags};
use crate::range_setops::{
    range_intersect_agg_transfn as range_intersect_agg_transfn_kernel,
    range_intersect_internal, range_minus_internal, range_union_internal,
};

// ---------------------------------------------------------------------------
// fmgr marshalling helpers (the `PG_*` macros over `fmgr` fcinfo).
// ---------------------------------------------------------------------------

/// `PG_GETARG_DATUM(n)` — the raw argument word.
fn getarg_datum(fcinfo: &FunctionCallInfoBaseData, n: usize) -> Datum {
    fcinfo.arg(n).map(|nd| nd.value).unwrap_or_else(Datum::null)
}

/// `PG_ARGISNULL(n)`.
fn argisnull(fcinfo: &FunctionCallInfoBaseData, n: usize) -> bool {
    fcinfo.arg(n).map(|nd| nd.isnull).unwrap_or(true)
}

/// Resolve the bare element word a `RangeBound.val` carrier must hold for the
/// `range_constructor{2,3}` element argument `n`, given the range element type.
///
/// C reads `PG_GETARG_DATUM(n)` directly: for a by-VALUE element (`int4` etc.)
/// that word *is* the value; for a by-REFERENCE element (`numeric`, `text`,
/// `tstzrange`'s `timestamptz` is by-value but `numeric` is by-ref) the word is
/// a `Pointer` to the value's varlena image in the caller's memory context, and
/// `range_serialize`'s `datum_compute_size`/`datum_write` dereference it.
///
/// On the owned fmgr boundary a by-reference argument's referent does NOT ride
/// the bare `args[n].value` word (which is only a placeholder) — it rides the
/// `ref_args` side channel as a `RefPayload::Varlena` image (the same lane
/// `text_to_cstring` reads the flags arg from). So for a by-reference element we
/// must MATERIALIZE that image into a real, MAXALIGN(8)-aligned varlena living
/// in `mcx` and return its pointer word, exactly the form `DatumGetPointer`
/// expects. For a by-value element the bare word crosses verbatim.
///
/// Mirrors `fmgr_builtins::range_bytes_to_arg_word` (the symmetric staging the
/// `fc_` wrapper does for whole-range arguments). Without this the bare
/// placeholder word flowed straight into `datum_write`, dereferencing a
/// non-pointer and faulting (SIGSEGV) for by-reference element ranges such as
/// `numrange`.
fn stage_elem_arg<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData,
    n: usize,
    elem_typbyval: bool,
) -> PgResult<Datum> {
    if elem_typbyval {
        // By-value element: the machine word IS the value.
        return Ok(getarg_datum(fcinfo, n));
    }
    // By-reference element: the referent image rides the by-ref side channel.
    // A composite subtype (e.g. `range(subtype = two_ints)`) arrives on the
    // `Composite` lane rather than `Varlena`; both carry the same flat
    // self-describing image, so accept either.
    match fcinfo.ref_arg(n).and_then(|p| p.as_byref_image()) {
        Some(image) => materialize_byref_word(mcx, image),
        // No by-ref payload present: fall back to the bare word. This covers a
        // fixed-length-by-reference element already passed as a live pointer (or
        // a NULL the caller guards), matching C's direct `PG_GETARG_DATUM`.
        None => Ok(getarg_datum(fcinfo, n)),
    }
}

/// Copy a by-reference value's varlena image into `mcx` as a MAXALIGN(8)-aligned
/// block and return the pointer `Datum` word (`PointerGetDatum`). The 8-byte
/// alignment matches what `palloc` hands a detoasted datum, which the element
/// type's by-ref accessors rely on.
fn materialize_byref_word<'mcx>(mcx: Mcx<'mcx>, image: &[u8]) -> PgResult<Datum> {
    use allocator_api2::alloc::Allocator;
    use core::alloc::Layout;
    ::mcx::check_alloc_size(image.len())?;
    let layout = Layout::from_size_align(image.len().max(1), 8)
        .expect("valid by-ref element image layout");
    let block = mcx.allocate(layout).map_err(|_| mcx.oom(image.len()))?;
    let dst = block.as_ptr() as *mut u8;
    // SAFETY: `dst` heads a freshly allocated image.len()-byte region.
    unsafe {
        core::ptr::copy_nonoverlapping(image.as_ptr(), dst, image.len());
    }
    Ok(Datum::from_usize(dst as usize))
}

/// The range element type's `typbyval`, read off the resolved range
/// `TypeCacheEntry`. A range type always has `rngelemtype` set
/// (`range_get_typcache` resolves `TYPECACHE_RANGE_INFO`).
fn elem_typbyval(typcache: &cache::typcache::TypeCacheEntry) -> bool {
    typcache
        .rngelemtype
        .as_ref()
        .map(|e| e.typbyval)
        .unwrap_or(true)
}

/// `PG_GETARG_RANGE_P(n)` — `DatumGetRangeTypeP(PG_GETARG_DATUM(n))`. The detoast
/// is the range ADT's own `datum_get_range_type_p` kernel (in `range_repr_serialize`).
fn getarg_range_p<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData,
    n: usize,
) -> PgResult<RangeTypeP<'mcx>> {
    // A by-reference range argument crosses the owned fmgr boundary through the
    // `ref_args` side channel (the way `range_constructor*` / const-folding hand
    // back a `RangeType` — `RefPayload::Varlena`), so the bare `args[n].value`
    // word is just a placeholder. Read the referent bytes when present; otherwise
    // fall back to the bare-word `Datum` (an on-disk / already-pointer datum the
    // detoast path resolves).
    if let Some(bytes) = fcinfo.ref_arg(n).and_then(|p| p.as_varlena()) {
        return crate::range_repr_serialize::range_p_from_varlena_bytes(mcx, bytes);
    }
    crate::range_repr_serialize::datum_get_range_type_p(mcx, getarg_datum(fcinfo, n))
}

/// `RangeTypeGetOid(r)` (rangetypes.h): the range type's own OID, read from the
/// only directly-readable header field. The handle's `ptr` is range-ADT-owned
/// detoasted memory produced by `datum_get_range_type_p`.
fn range_type_get_oid(r: RangeTypeP<'_>) -> types_core::primitive::Oid {
    // SAFETY: `r.ptr` points at a detoasted `RangeType` varlena the range ADT
    // produced and keeps alive for `'mcx`; the fixed header (and thus
    // `rangetypid`) is always present.
    r.rangetypid()
}

/// `PG_RETURN_RANGE_P(range)` — `PG_RETURN_POINTER(range)`.
///
/// C returns the bare `RangeType *` pointer word; the owned fmgr boundary cannot
/// hand back a raw pointer (its referent lives in the call's transient context
/// and would dangle past the call), so a by-reference range result crosses
/// through the `RefPayload::Varlena` side channel (the same lane `range_out` /
/// `range_send` and every other by-ref-returning builtin use). The returned
/// `Datum` is the null placeholder word; the caller reads the referent from
/// `fcinfo->ref_result`.
fn return_range_p(fcinfo: &mut FunctionCallInfoBaseData, range: RangeTypeP<'_>) -> Datum {
    fcinfo.set_ref_result(::fmgr::RefPayload::Varlena(
        crate::range_repr_serialize::range_to_varlena_bytes(range),
    ));
    Datum::null()
}

/// `PG_RETURN_NULL()` — set `fcinfo->isnull` and hand back the zero word.
fn return_null(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.set_result_null(true);
    Datum::null()
}

// ---------------------------------------------------------------------------
// I/O (range-io)
// ---------------------------------------------------------------------------

/// `range_in(PG_FUNCTION_ARGS)` (rangetypes.c:90).
pub fn range_in<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    // char *input_str = PG_GETARG_CSTRING(0);
    // Copy to an owned String so the cstring borrow does not alias the mutable
    // `escontext_mut()` borrow taken below.
    let input_str = fcinfo
        .ref_arg(0)
        .and_then(|p| p.as_cstring())
        .expect("range_in arg 0 is a cstring")
        .to_string();
    let rngtypoid = getarg_datum(fcinfo, 1).as_oid();
    let typmod = getarg_datum(fcinfo, 2).as_i32();

    // C: check_stack_depth() — recursion guard owned by miscadmin; the safe
    // port's call stack faults instead of an explicit depth check.

    let cache = get_range_io_data(rngtypoid, IOFuncSelector::Input)?;

    // C: Node *escontext = fcinfo->context; — forward the soft-error sink so a
    // recoverable parse / element-input error soft-fails (PG_RETURN_NULL) when a
    // caller such as `pg_input_is_valid` supplied an ErrorSaveContext.
    let escontext = fcinfo.escontext_mut();

    // get_range_io_data + element-input parsing + make_range live in the kernel,
    // which returns NULL (soft error) as the SQL NULL the entry surfaces.
    match range_in_kernel(mcx, &cache, &input_str, typmod, escontext)? {
        Some(range) => Ok(return_range_p(fcinfo, range)),
        // C: PG_RETURN_NULL() after a soft error.
        None => Ok(return_null(fcinfo)),
    }
}

/// `range_out(PG_FUNCTION_ARGS)` (rangetypes.c:139).
pub fn range_out<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let range = getarg_range_p(mcx, fcinfo, 0)?;
    let cache = get_range_io_data(range_type_get_oid(range), IOFuncSelector::Output)?;
    let output_str = range_out_kernel(mcx, &cache, range)?;
    // PG_RETURN_CSTRING(output_str): by-ref cstring result.
    fcinfo.set_ref_result(::fmgr::RefPayload::Cstring(output_str));
    Ok(Datum::null())
}

/// `range_recv(PG_FUNCTION_ARGS)` (rangetypes.c:179).
pub fn range_recv<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    // StringInfo buf = (StringInfo) PG_GETARG_POINTER(0): the wire buffer is the
    // by-ref varlena payload carrying the message bytes.
    let buf = fcinfo
        .ref_arg(0)
        .and_then(|p| p.as_varlena())
        .expect("range_recv arg 0 is a StringInfo buffer");
    let rngtypoid = getarg_datum(fcinfo, 1).as_oid();
    let typmod = getarg_datum(fcinfo, 2).as_i32();

    let cache = get_range_io_data(rngtypoid, IOFuncSelector::Receive)?;
    let range = range_recv_kernel(mcx, &cache, buf, typmod)?;
    Ok(return_range_p(fcinfo, range))
}

/// `range_send(PG_FUNCTION_ARGS)` (rangetypes.c:263).
pub fn range_send<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let range = getarg_range_p(mcx, fcinfo, 0)?;
    let cache = get_range_io_data(range_type_get_oid(range), IOFuncSelector::Send)?;
    let bytes = range_send_kernel(mcx, &cache, range)?;
    // PG_RETURN_BYTEA_P(...): by-ref bytea result.
    fcinfo.set_ref_result(::fmgr::RefPayload::Varlena(bytes));
    Ok(Datum::null())
}

// ---------------------------------------------------------------------------
// constructors / accessors
// ---------------------------------------------------------------------------

/// `range_constructor2(PG_FUNCTION_ARGS)` (rangetypes.c:379).
pub fn range_constructor2<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let rngtypid = get_fn_expr_rettype(fcinfo.flinfo.as_deref());

    let typcache = range_get_typcache(rngtypid)?;
    let elem_byval = elem_typbyval(&typcache);

    // C: lower.val = PG_GETARG_DATUM(0) (a by-ref element rides the ref lane).
    let arg1 = stage_elem_arg(mcx, fcinfo, 0, elem_byval)?;
    let arg2 = stage_elem_arg(mcx, fcinfo, 1, elem_byval)?;

    let lower = RangeBound {
        val: if argisnull(fcinfo, 0) { Datum::null() } else { arg1 },
        infinite: argisnull(fcinfo, 0),
        inclusive: true,
        lower: true,
    };
    let upper = RangeBound {
        val: if argisnull(fcinfo, 1) { Datum::null() } else { arg2 },
        infinite: argisnull(fcinfo, 1),
        inclusive: false,
        lower: false,
    };

    let range = make_range(mcx, &typcache, &lower, &upper, false)?;
    Ok(return_range_p(fcinfo, range))
}

/// `range_constructor3(PG_FUNCTION_ARGS)` (rangetypes.c:407).
pub fn range_constructor3<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let rngtypid = get_fn_expr_rettype(fcinfo.flinfo.as_deref());

    let typcache = range_get_typcache(rngtypid)?;
    let elem_byval = elem_typbyval(&typcache);

    if argisnull(fcinfo, 2) {
        return Err(PgError::error("range constructor flags argument must not be null")
            .with_sqlstate(ERRCODE_DATA_EXCEPTION));
    }

    // flags = range_parse_flags(text_to_cstring(PG_GETARG_TEXT_PP(2)));
    // `text` is pass-by-REFERENCE: `TextDatumGetCString` dereferences the arg
    // word as a `struct varlena *`. On the owned fmgr boundary the referent rides
    // the `ref_args` side channel (a const-folded `text` literal arrives as
    // `RefPayload::Varlena` with only a NULL placeholder on the by-value word), so
    // we must materialize it into a real pointer word before `text_to_cstring`
    // dereferences it — otherwise the NULL placeholder faults (SIGSEGV).
    let flags_word = stage_elem_arg(mcx, fcinfo, 2, false)?;
    let flags_str = varlena_seams::text_to_cstring::call(mcx, flags_word)?;
    let flags = crate::range_io::range_parse_flags(flags_str.as_str())?;

    // C: lower.val = PG_GETARG_DATUM(0) (a by-ref element rides the ref lane).
    let arg1 = stage_elem_arg(mcx, fcinfo, 0, elem_byval)?;
    let arg2 = stage_elem_arg(mcx, fcinfo, 1, elem_byval)?;

    let lower = RangeBound {
        val: if argisnull(fcinfo, 0) { Datum::null() } else { arg1 },
        infinite: argisnull(fcinfo, 0),
        inclusive: (flags & RANGE_LB_INC) != 0,
        lower: true,
    };
    let upper = RangeBound {
        val: if argisnull(fcinfo, 1) { Datum::null() } else { arg2 },
        infinite: argisnull(fcinfo, 1),
        inclusive: (flags & RANGE_UB_INC) != 0,
        lower: false,
    };

    let range = make_range(mcx, &typcache, &lower, &upper, false)?;
    Ok(return_range_p(fcinfo, range))
}

/// `PG_RETURN_DATUM(bound.val)` for a range *element* result (`range_lower`/
/// `range_upper`). C returns the element `Datum` directly: for a by-value
/// subtype the bare word; for a by-reference subtype a `Pointer` to the element
/// image, which on the owned fmgr boundary must instead ride the `ref_result`
/// side channel as a `RefPayload::Varlena` (a bare pointer word would dangle and
/// the caller's `ref_out_to_datum` would mis-read it). Reuses the cmp lane's
/// `elem_word_to_canon` (header-ful image) so a packed bound un-packs correctly.
fn return_elem<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
    typcache: &cache::typcache::TypeCacheEntry,
    val: Datum,
) -> PgResult<Datum> {
    let canon = crate::range_bounds_compare::elem_word_to_canon(mcx, typcache, val)?;
    match canon {
        types_tuple::heaptuple::Datum::ByVal(w) => Ok(Datum::from_usize(w)),
        types_tuple::heaptuple::Datum::ByRef(b) => {
            // A by-reference element image is a flat varlena; for a composite
            // (rowtype) subtype it is a `HeapTupleHeader` and must ride the
            // `Composite` lane so a downstream record consumer (`row_to_json`,
            // record I/O) sees it canonically as a composite Datum, not a plain
            // varlena. C makes no such distinction (a composite Datum is just a
            // varlena pointer); the lane tag is the port's own and must match.
            let elem_oid = typcache
                .rngelemtype
                .as_ref()
                .map(|e| e.type_id)
                .unwrap_or_default();
            let payload = if lsyscache_seams::type_is_rowtype::call(elem_oid)? {
                ::fmgr::RefPayload::Composite(b.as_slice().to_vec())
            } else {
                ::fmgr::RefPayload::Varlena(b.as_slice().to_vec())
            };
            fcinfo.set_ref_result(payload);
            Ok(Datum::null())
        }
        // A range element is only ever by-value or a varlena/fixed-len by-ref
        // (the `ByRef` arm above); the other canonical kinds never arise here.
        other => panic!("range element result: unexpected canonical kind {other:?}"),
    }
}

/// `range_lower(PG_FUNCTION_ARGS)` (rangetypes.c:448).
pub fn range_lower<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r1 = getarg_range_p(mcx, fcinfo, 0)?;
    let typcache = range_get_typcache(range_type_get_oid(r1))?;
    let (lower, _upper, empty) = crate::range_repr_serialize::range_deserialize(&typcache, r1)?;

    // Return NULL if there's no finite lower bound
    if empty || lower.infinite {
        return Ok(return_null(fcinfo));
    }
    return_elem(mcx, fcinfo, &typcache, lower.val)
}

/// `range_upper(PG_FUNCTION_ARGS)` (rangetypes.c:469).
pub fn range_upper<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r1 = getarg_range_p(mcx, fcinfo, 0)?;
    let typcache = range_get_typcache(range_type_get_oid(r1))?;
    let (_lower, upper, empty) = crate::range_repr_serialize::range_deserialize(&typcache, r1)?;

    if empty || upper.infinite {
        return Ok(return_null(fcinfo));
    }
    return_elem(mcx, fcinfo, &typcache, upper.val)
}

/// `range_empty(PG_FUNCTION_ARGS)` (rangetypes.c:493).
pub fn range_empty<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r1 = getarg_range_p(mcx, fcinfo, 0)?;
    let flags = range_get_flags(r1);
    Ok(Datum::from_bool(flags & RANGE_EMPTY != 0))
}

/// `range_lower_inc(PG_FUNCTION_ARGS)` (rangetypes.c:503).
pub fn range_lower_inc<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r1 = getarg_range_p(mcx, fcinfo, 0)?;
    let flags = range_get_flags(r1);
    Ok(Datum::from_bool(flags & RANGE_LB_INC != 0))
}

/// `range_upper_inc(PG_FUNCTION_ARGS)` (rangetypes.c:513).
pub fn range_upper_inc<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r1 = getarg_range_p(mcx, fcinfo, 0)?;
    let flags = range_get_flags(r1);
    Ok(Datum::from_bool(flags & RANGE_UB_INC != 0))
}

/// `range_lower_inf(PG_FUNCTION_ARGS)` (rangetypes.c:523).
pub fn range_lower_inf<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r1 = getarg_range_p(mcx, fcinfo, 0)?;
    let flags = range_get_flags(r1);
    Ok(Datum::from_bool(flags & RANGE_LB_INF != 0))
}

/// `range_upper_inf(PG_FUNCTION_ARGS)` (rangetypes.c:533).
pub fn range_upper_inf<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r1 = getarg_range_p(mcx, fcinfo, 0)?;
    let flags = range_get_flags(r1);
    Ok(Datum::from_bool(flags & RANGE_UB_INF != 0))
}

// ---------------------------------------------------------------------------
// element / predicate operators (range-bounds-compare)
// ---------------------------------------------------------------------------

/// `range_contains_elem(PG_FUNCTION_ARGS)` (rangetypes.c:546).
pub fn range_contains_elem<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r = getarg_range_p(mcx, fcinfo, 0)?;
    let typcache = range_get_typcache(range_type_get_oid(r))?;
    // C: `val = PG_GETARG_DATUM(1)` — for a by-reference element subtype that
    // word is a `Pointer` to the element image; on the owned fmgr boundary the
    // referent rides the `ref_args` side channel, so stage it the same way the
    // range constructors stage their element args (materializing the by-ref
    // image into `mcx` and handing back its pointer word).
    let val = stage_elem_arg(mcx, fcinfo, 1, elem_typbyval(&typcache))?;
    Ok(Datum::from_bool(range_contains_elem_internal(&typcache, r, val)?))
}

/// `elem_contained_by_range(PG_FUNCTION_ARGS)` (rangetypes.c:559).
pub fn elem_contained_by_range<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r = getarg_range_p(mcx, fcinfo, 1)?;
    let typcache = range_get_typcache(range_type_get_oid(r))?;
    // C: `val = PG_GETARG_DATUM(0)` — the element is arg 0; stage its by-ref
    // referent off the `ref_args` lane (see `range_contains_elem`).
    let val = stage_elem_arg(mcx, fcinfo, 0, elem_typbyval(&typcache))?;
    Ok(Datum::from_bool(range_contains_elem_internal(&typcache, r, val)?))
}

/// `range_eq(PG_FUNCTION_ARGS)` (rangetypes.c:607).
pub fn range_eq<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r1 = getarg_range_p(mcx, fcinfo, 0)?;
    let r2 = getarg_range_p(mcx, fcinfo, 1)?;
    let typcache = range_get_typcache(range_type_get_oid(r1))?;
    Ok(Datum::from_bool(range_eq_internal(&typcache, r1, r2)?))
}

/// `range_ne(PG_FUNCTION_ARGS)` (rangetypes.c:627).
pub fn range_ne<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r1 = getarg_range_p(mcx, fcinfo, 0)?;
    let r2 = getarg_range_p(mcx, fcinfo, 1)?;
    let typcache = range_get_typcache(range_type_get_oid(r1))?;
    Ok(Datum::from_bool(range_ne_internal(&typcache, r1, r2)?))
}

/// `range_contains(PG_FUNCTION_ARGS)` (rangetypes.c:640).
pub fn range_contains<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r1 = getarg_range_p(mcx, fcinfo, 0)?;
    let r2 = getarg_range_p(mcx, fcinfo, 1)?;
    let typcache = range_get_typcache(range_type_get_oid(r1))?;
    Ok(Datum::from_bool(range_contains_internal(&typcache, r1, r2)?))
}

/// `range_contained_by(PG_FUNCTION_ARGS)` (rangetypes.c:653).
pub fn range_contained_by<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r1 = getarg_range_p(mcx, fcinfo, 0)?;
    let r2 = getarg_range_p(mcx, fcinfo, 1)?;
    let typcache = range_get_typcache(range_type_get_oid(r1))?;
    Ok(Datum::from_bool(range_contained_by_internal(&typcache, r1, r2)?))
}

/// `range_before(PG_FUNCTION_ARGS)` (rangetypes.c:691).
pub fn range_before<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r1 = getarg_range_p(mcx, fcinfo, 0)?;
    let r2 = getarg_range_p(mcx, fcinfo, 1)?;
    let typcache = range_get_typcache(range_type_get_oid(r1))?;
    Ok(Datum::from_bool(range_before_internal(&typcache, r1, r2)?))
}

/// `range_after(PG_FUNCTION_ARGS)` (rangetypes.c:729).
pub fn range_after<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r1 = getarg_range_p(mcx, fcinfo, 0)?;
    let r2 = getarg_range_p(mcx, fcinfo, 1)?;
    let typcache = range_get_typcache(range_type_get_oid(r1))?;
    Ok(Datum::from_bool(range_after_internal(&typcache, r1, r2)?))
}

/// `range_adjacent(PG_FUNCTION_ARGS)` (rangetypes.c:830).
pub fn range_adjacent<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r1 = getarg_range_p(mcx, fcinfo, 0)?;
    let r2 = getarg_range_p(mcx, fcinfo, 1)?;
    let typcache = range_get_typcache(range_type_get_oid(r1))?;
    Ok(Datum::from_bool(range_adjacent_internal(mcx, &typcache, r1, r2)?))
}

/// `range_overlaps(PG_FUNCTION_ARGS)` (rangetypes.c:876).
pub fn range_overlaps<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r1 = getarg_range_p(mcx, fcinfo, 0)?;
    let r2 = getarg_range_p(mcx, fcinfo, 1)?;
    let typcache = range_get_typcache(range_type_get_oid(r1))?;
    Ok(Datum::from_bool(range_overlaps_internal(&typcache, r1, r2)?))
}

/// `range_overleft(PG_FUNCTION_ARGS)` (rangetypes.c:917).
pub fn range_overleft<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r1 = getarg_range_p(mcx, fcinfo, 0)?;
    let r2 = getarg_range_p(mcx, fcinfo, 1)?;
    let typcache = range_get_typcache(range_type_get_oid(r1))?;
    Ok(Datum::from_bool(range_overleft_internal(&typcache, r1, r2)?))
}

/// `range_overright(PG_FUNCTION_ARGS)` (rangetypes.c:958).
pub fn range_overright<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r1 = getarg_range_p(mcx, fcinfo, 0)?;
    let r2 = getarg_range_p(mcx, fcinfo, 1)?;
    let typcache = range_get_typcache(range_type_get_oid(r1))?;
    Ok(Datum::from_bool(range_overright_internal(&typcache, r1, r2)?))
}

// ---------------------------------------------------------------------------
// set operations (range-setops)
// ---------------------------------------------------------------------------

/// `range_minus(PG_FUNCTION_ARGS)` (rangetypes.c:974).
pub fn range_minus<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r1 = getarg_range_p(mcx, fcinfo, 0)?;
    let r2 = getarg_range_p(mcx, fcinfo, 1)?;

    // Different types should be prevented by ANYRANGE matching rules
    if range_type_get_oid(r1) != range_type_get_oid(r2) {
        return Err(PgError::error("range types do not match"));
    }

    let typcache = range_get_typcache(range_type_get_oid(r1))?;

    // C: ret = range_minus_internal(...); if (ret) PG_RETURN_RANGE_P(ret); else
    // PG_RETURN_NULL(). The kernel signals "no result" with a NULL `RangeType *`.
    let ret = range_minus_internal(mcx, &typcache, r1, r2)?;
    if ret.ptr.is_null() {
        Ok(return_null(fcinfo))
    } else {
        Ok(return_range_p(fcinfo, ret))
    }
}

/// `range_union(PG_FUNCTION_ARGS)` (rangetypes.c:1100).
pub fn range_union<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r1 = getarg_range_p(mcx, fcinfo, 0)?;
    let r2 = getarg_range_p(mcx, fcinfo, 1)?;
    let typcache = range_get_typcache(range_type_get_oid(r1))?;
    let ret = range_union_internal(mcx, &typcache, r1, r2, true)?;
    Ok(return_range_p(fcinfo, ret))
}

/// `range_merge(PG_FUNCTION_ARGS)` (rangetypes.c:1116).
pub fn range_merge<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r1 = getarg_range_p(mcx, fcinfo, 0)?;
    let r2 = getarg_range_p(mcx, fcinfo, 1)?;
    let typcache = range_get_typcache(range_type_get_oid(r1))?;
    // C: range_merge == range_union_internal(..., strict = false).
    let ret = range_union_internal(mcx, &typcache, r1, r2, false)?;
    Ok(return_range_p(fcinfo, ret))
}

/// `range_intersect(PG_FUNCTION_ARGS)` (rangetypes.c:1129).
pub fn range_intersect<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r1 = getarg_range_p(mcx, fcinfo, 0)?;
    let r2 = getarg_range_p(mcx, fcinfo, 1)?;

    // Different types should be prevented by ANYRANGE matching rules
    if range_type_get_oid(r1) != range_type_get_oid(r2) {
        return Err(PgError::error("range types do not match"));
    }

    let typcache = range_get_typcache(range_type_get_oid(r1))?;
    let ret = range_intersect_internal(mcx, &typcache, r1, r2)?;
    Ok(return_range_p(fcinfo, ret))
}

/// `range_intersect_agg_transfn(PG_FUNCTION_ARGS)` (rangetypes.c:1221).
pub fn range_intersect_agg_transfn<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    // if (!AggCheckCallContext(fcinfo, &aggContext))
    //     elog(ERROR, "range_intersect_agg_transfn called in non-aggregate context");
    //
    // `AggCheckCallContext` is owned by backend-executor-nodeAgg and exposed via
    // the installed `agg_check_call_context` seam (over the `fmgr` call
    // frame, whose aggstate back-reference the executor deposits before the
    // transfn runs).
    if !agg_check_call_context(fcinfo) {
        return Err(PgError::error(
            "range_intersect_agg_transfn called in non-aggregate context",
        ));
    }

    let rngtypoid = get_fn_expr_argtype(fcinfo.flinfo.as_deref(), 1);
    if !type_is_range(rngtypoid)? {
        return Err(PgError::error("range_intersect_agg must be called with a range"));
    }

    let typcache = range_get_typcache(rngtypoid)?;

    // strictness ensures these are non-null
    let result = getarg_range_p(mcx, fcinfo, 0)?;
    let current = getarg_range_p(mcx, fcinfo, 1)?;

    // The kernel models the running-intersection state machine; here both
    // operands are present (Some).
    let out = range_intersect_agg_transfn_kernel(mcx, &typcache, Some(result), Some(current))?;
    match out {
        Some(range) => Ok(return_range_p(fcinfo, range)),
        None => Ok(return_null(fcinfo)),
    }
}

/// `AggCheckCallContext(fcinfo, &aggContext) != 0` — whether the function is
/// being called as an aggregate transition/final function. Routes through the
/// installed `agg_check_call_context` nodeAgg seam (the executor deposits the
/// aggstate back-reference on the `fmgr` frame before the transfn runs);
/// `true` when the context code is `AGG_CONTEXT_AGGREGATE`.
fn agg_check_call_context(fcinfo: &FunctionCallInfoBaseData) -> bool {
    let (code, _aggcontext) =
        nodeAgg_aggapi_seams::agg_check_call_context::call(fcinfo);
    code == nodeAgg_aggapi_seams::AGG_CONTEXT_AGGREGATE
}

/// `type_is_range(typid)` — owned by utils/cache/lsyscache.c. Routes through
/// the owner's installed seam.
fn type_is_range(typid: types_core::primitive::Oid) -> PgResult<bool> {
    lsyscache_seams::type_is_range::call(typid)
}

// ---------------------------------------------------------------------------
// ordering / hash / sortsupport (range-canonical-subdiff-hash)
// ---------------------------------------------------------------------------

/// `range_cmp(PG_FUNCTION_ARGS)` (rangetypes.c:1251).
pub fn range_cmp<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r1 = getarg_range_p(mcx, fcinfo, 0)?;
    let r2 = getarg_range_p(mcx, fcinfo, 1)?;
    let typcache = range_get_typcache(range_type_get_oid(r1))?;
    // The kernel mirrors the empty-ordering + lower/upper bound comparison and
    // the PG_FREE_IF_COPY of the detoasted inputs.
    let cmp = range_cmp_kernel(&typcache, r1, r2)?;
    Ok(Datum::from_i32(cmp))
}

/// `range_lt(PG_FUNCTION_ARGS)` (rangetypes.c:1359).
pub fn range_lt<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let cmp = range_cmp(mcx, fcinfo)?.as_i32();
    Ok(Datum::from_bool(cmp < 0))
}

/// `range_le(PG_FUNCTION_ARGS)` (rangetypes.c:1367).
pub fn range_le<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let cmp = range_cmp(mcx, fcinfo)?.as_i32();
    Ok(Datum::from_bool(cmp <= 0))
}

/// `range_ge(PG_FUNCTION_ARGS)` (rangetypes.c:1375).
pub fn range_ge<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let cmp = range_cmp(mcx, fcinfo)?.as_i32();
    Ok(Datum::from_bool(cmp >= 0))
}

/// `range_gt(PG_FUNCTION_ARGS)` (rangetypes.c:1383).
pub fn range_gt<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let cmp = range_cmp(mcx, fcinfo)?.as_i32();
    Ok(Datum::from_bool(cmp > 0))
}

/// `hash_range(PG_FUNCTION_ARGS)` (rangetypes.c:1394).
pub fn hash_range<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r = getarg_range_p(mcx, fcinfo, 0)?;
    let typcache = range_get_typcache(range_type_get_oid(r))?;
    let result = hash_range_kernel(&typcache, r)?;
    // PG_RETURN_INT32(result): the uint32 hash returned as an int32 word.
    Ok(Datum::from_i32(result as i32))
}

/// `hash_range_extended(PG_FUNCTION_ARGS)` (rangetypes.c:1460).
pub fn hash_range_extended<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r = getarg_range_p(mcx, fcinfo, 0)?;
    let seed = getarg_datum(fcinfo, 1).as_usize() as u64;
    let typcache = range_get_typcache(range_type_get_oid(r))?;
    let result = hash_range_extended_kernel(&typcache, r, seed)?;
    // PG_RETURN_INT64(result): the uint64 hash returned as an int64 word.
    Ok(Datum::from_usize(result as usize))
}

/// `range_sortsupport(PG_FUNCTION_ARGS)` (rangetypes.c:1297).
pub fn range_sortsupport<'mcx>(
    _mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    // SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);
    // ssup->comparator = range_fast_cmp; ssup->ssup_extra = NULL;
    //
    // SortSupport is the executor-owned sort scratch; wiring the comparator
    // (`range_fast_cmp`) into a live SortSupport is the sortsupport neighbor's
    // job, reached over the owned call frame the `fmgr` boundary does not
    // carry. Route to the owner (loud panic until it lands).
    let _ = fcinfo;
    panic!(
        "sort_sortsupport::range_sortsupport: unported neighbor — \
         installing range_fast_cmp into a live SortSupport needs the executor call frame"
    );
}

// ---------------------------------------------------------------------------
// canonical / subdiff (range-canonical-subdiff-hash)
// ---------------------------------------------------------------------------

/// `int4range_canonical(PG_FUNCTION_ARGS)` (rangetypes.c:1531).
pub fn int4range_canonical_v1<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r = getarg_range_p(mcx, fcinfo, 0)?;
    let typcache = range_get_typcache(range_type_get_oid(r))?;
    // The kernel performs deserialize, the empty short-circuit (returns `r`), the
    // boundary +1/inclusivity normalization with the integer-out-of-range
    // overflow check (`ereturn(fcinfo->context, ...)`), and range_serialize.
    let escontext = fcinfo.escontext_mut();
    match int4range_canonical(mcx, &typcache, r, escontext)? {
        Some(out) => Ok(return_range_p(fcinfo, out)),
        None => Ok(return_null(fcinfo)),
    }
}

/// `int8range_canonical(PG_FUNCTION_ARGS)` (rangetypes.c:1574).
pub fn int8range_canonical_v1<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r = getarg_range_p(mcx, fcinfo, 0)?;
    let typcache = range_get_typcache(range_type_get_oid(r))?;
    let escontext = fcinfo.escontext_mut();
    match int8range_canonical(mcx, &typcache, r, escontext)? {
        Some(out) => Ok(return_range_p(fcinfo, out)),
        None => Ok(return_null(fcinfo)),
    }
}

/// `daterange_canonical(PG_FUNCTION_ARGS)` (rangetypes.c:1622).
pub fn daterange_canonical_v1<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r = getarg_range_p(mcx, fcinfo, 0)?;
    let typcache = range_get_typcache(range_type_get_oid(r))?;
    let escontext = fcinfo.escontext_mut();
    match daterange_canonical(mcx, &typcache, r, escontext)? {
        Some(out) => Ok(return_range_p(fcinfo, out)),
        None => Ok(return_null(fcinfo)),
    }
}

/// `int4range_subdiff(PG_FUNCTION_ARGS)` (rangetypes.c:1685).
pub fn int4range_subdiff<'mcx>(
    _mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let v1 = getarg_datum(fcinfo, 0).as_i32();
    let v2 = getarg_datum(fcinfo, 1).as_i32();
    Ok(float8_datum(crate::range_canonical_subdiff_hash::int4range_subdiff(v1, v2)))
}

/// `int8range_subdiff(PG_FUNCTION_ARGS)` (rangetypes.c:1693).
pub fn int8range_subdiff<'mcx>(
    _mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let v1 = getarg_datum(fcinfo, 0).as_usize() as i64;
    let v2 = getarg_datum(fcinfo, 1).as_usize() as i64;
    Ok(float8_datum(crate::range_canonical_subdiff_hash::int8range_subdiff(v1, v2)))
}

/// `numrange_subdiff(PG_FUNCTION_ARGS)` (rangetypes.c:1703).
pub fn numrange_subdiff<'mcx>(
    _mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let v1 = getarg_datum(fcinfo, 0);
    let v2 = getarg_datum(fcinfo, 1);
    Ok(float8_datum(crate::range_canonical_subdiff_hash::numrange_subdiff(v1, v2)?))
}

/// `daterange_subdiff(PG_FUNCTION_ARGS)` (rangetypes.c:1719).
pub fn daterange_subdiff<'mcx>(
    _mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let v1 = getarg_datum(fcinfo, 0).as_i32();
    let v2 = getarg_datum(fcinfo, 1).as_i32();
    Ok(float8_datum(crate::range_canonical_subdiff_hash::daterange_subdiff(v1, v2)))
}

/// `tsrange_subdiff(PG_FUNCTION_ARGS)` (rangetypes.c:1728).
pub fn tsrange_subdiff<'mcx>(
    _mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let v1 = getarg_datum(fcinfo, 0).as_usize() as i64;
    let v2 = getarg_datum(fcinfo, 1).as_usize() as i64;
    Ok(float8_datum(crate::range_canonical_subdiff_hash::tsrange_subdiff(v1, v2)))
}

/// `tstzrange_subdiff(PG_FUNCTION_ARGS)` (rangetypes.c:1739).
pub fn tstzrange_subdiff<'mcx>(
    _mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let v1 = getarg_datum(fcinfo, 0).as_usize() as i64;
    let v2 = getarg_datum(fcinfo, 1).as_usize() as i64;
    Ok(float8_datum(crate::range_canonical_subdiff_hash::tstzrange_subdiff(v1, v2)))
}

/// `PG_RETURN_FLOAT8(x)` — `Float8GetDatum` (the IEEE bit pattern in the word,
/// USE_FLOAT8_BYVAL).
fn float8_datum(x: f64) -> Datum {
    Datum::from_usize(x.to_bits() as usize)
}

// ---------------------------------------------------------------------------
// planner support (range-planner-support)
//
// `elem_contained_by_range_support` / `range_contains_elem_support` are NOT
// fmgr-boundary wrappers anymore: their support-request argument is a planner
// `Node *` (`SupportRequestSimplify`) carried in a `Datum` word, a convention
// not modeled on the ported fmgr surface. The simplification logic lives in
// `range_planner_support` over the real `(root, FuncExpr)` request and is
// reached through the value-typed `call_support_simplify` dispatch wired in
// `seams-init`.
// ---------------------------------------------------------------------------

// Silence the unused-import lint for `RangeType` (only its `rangetypid` field is
// reached, via the raw `*const RangeType` deref in `range_type_get_oid`).
#[allow(unused_imports)]
use RangeType as _RangeTypeHeader;
