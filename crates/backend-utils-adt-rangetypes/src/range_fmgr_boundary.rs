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
//! are called directly; they panic via their own `todo!()` until each family
//! lands. Genuinely-external neighbors are reached through their owners
//! (`get_fn_expr_*` from fmgr-core, `text_to_cstring`/typcache via their seams),
//! or — where no compatible seam exists yet — by a loud owner-named panic
//! (`AggCheckCallContext`), the sanctioned seam-and-panic for an unported dep.

use backend_utils_fmgr_core::{get_fn_expr_argtype, get_fn_expr_rettype};
use mcx::Mcx;
use types_datum::Datum;
use types_error::{PgError, PgResult, ERRCODE_DATA_EXCEPTION};
use types_fmgr::FunctionCallInfoBaseData;
use types_rangetypes::{
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
use crate::range_planner_support::{
    elem_contained_by_range_support as elem_contained_by_range_support_kernel,
    range_contains_elem_support as range_contains_elem_support_kernel, PlannerNode,
};
use crate::range_repr_serialize::{make_range, range_get_flags};
use crate::range_setops::{
    range_intersect_agg_transfn as range_intersect_agg_transfn_kernel,
    range_intersect_internal, range_minus_internal, range_union_internal,
};

// ---------------------------------------------------------------------------
// fmgr marshalling helpers (the `PG_*` macros over `types_fmgr` fcinfo).
// ---------------------------------------------------------------------------

/// `PG_GETARG_DATUM(n)` — the raw argument word.
fn getarg_datum(fcinfo: &FunctionCallInfoBaseData, n: usize) -> Datum {
    fcinfo.arg(n).map(|nd| nd.value).unwrap_or_else(Datum::null)
}

/// `PG_ARGISNULL(n)`.
fn argisnull(fcinfo: &FunctionCallInfoBaseData, n: usize) -> bool {
    fcinfo.arg(n).map(|nd| nd.isnull).unwrap_or(true)
}

/// `PG_GETARG_RANGE_P(n)` — `DatumGetRangeTypeP(PG_GETARG_DATUM(n))`. The detoast
/// is the range ADT's own `datum_get_range_type_p` kernel (in `range_repr_serialize`).
fn getarg_range_p<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData,
    n: usize,
) -> PgResult<RangeTypeP<'mcx>> {
    crate::range_repr_serialize::datum_get_range_type_p(mcx, getarg_datum(fcinfo, n))
}

/// `RangeTypeGetOid(r)` (rangetypes.h): the range type's own OID, read from the
/// only directly-readable header field. The handle's `ptr` is range-ADT-owned
/// detoasted memory produced by `datum_get_range_type_p`.
fn range_type_get_oid(r: RangeTypeP<'_>) -> types_core::primitive::Oid {
    // SAFETY: `r.ptr` points at a detoasted `RangeType` varlena the range ADT
    // produced and keeps alive for `'mcx`; the fixed header (and thus
    // `rangetypid`) is always present.
    unsafe { (*r.ptr).rangetypid }
}

/// `PG_RETURN_RANGE_P(range)` — the pointer word (`PointerGetDatum`).
fn return_range_p(range: RangeTypeP<'_>) -> Datum {
    Datum::from_usize(range.ptr as usize)
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
    let input_str = fcinfo
        .ref_arg(0)
        .and_then(|p| p.as_cstring())
        .expect("range_in arg 0 is a cstring");
    let rngtypoid = getarg_datum(fcinfo, 1).as_oid();
    let typmod = getarg_datum(fcinfo, 2).as_i32();

    // C: check_stack_depth() — recursion guard owned by miscadmin; the safe
    // port's call stack faults instead of an explicit depth check.

    let cache = get_range_io_data(rngtypoid, IOFuncSelector::Input)?;

    // get_range_io_data + element-input parsing + make_range live in the kernel,
    // which returns NULL (soft error) as the SQL NULL the entry surfaces.
    let range = range_in_kernel(mcx, &cache, input_str, typmod)?;
    Ok(return_range_p(range))
}

/// `range_out(PG_FUNCTION_ARGS)` (rangetypes.c:139).
pub fn range_out<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let range = getarg_range_p(mcx, fcinfo, 0)?;
    let cache = get_range_io_data(range_type_get_oid(range), IOFuncSelector::Output)?;
    let output_str = range_out_kernel(&cache, range)?;
    // PG_RETURN_CSTRING(output_str): by-ref cstring result.
    fcinfo.set_ref_result(types_fmgr::RefPayload::Cstring(output_str));
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
    Ok(return_range_p(range))
}

/// `range_send(PG_FUNCTION_ARGS)` (rangetypes.c:263).
pub fn range_send<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let range = getarg_range_p(mcx, fcinfo, 0)?;
    let cache = get_range_io_data(range_type_get_oid(range), IOFuncSelector::Send)?;
    let bytes = range_send_kernel(&cache, range)?;
    // PG_RETURN_BYTEA_P(...): by-ref bytea result.
    fcinfo.set_ref_result(types_fmgr::RefPayload::Varlena(bytes));
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
    let arg1 = getarg_datum(fcinfo, 0);
    let arg2 = getarg_datum(fcinfo, 1);
    let rngtypid = get_fn_expr_rettype(fcinfo.flinfo.as_deref());

    let typcache = range_get_typcache(rngtypid)?;

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
    Ok(return_range_p(range))
}

/// `range_constructor3(PG_FUNCTION_ARGS)` (rangetypes.c:407).
pub fn range_constructor3<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let arg1 = getarg_datum(fcinfo, 0);
    let arg2 = getarg_datum(fcinfo, 1);
    let rngtypid = get_fn_expr_rettype(fcinfo.flinfo.as_deref());

    let typcache = range_get_typcache(rngtypid)?;

    if argisnull(fcinfo, 2) {
        return Err(PgError::error("range constructor flags argument must not be null")
            .with_sqlstate(ERRCODE_DATA_EXCEPTION));
    }

    // flags = range_parse_flags(text_to_cstring(PG_GETARG_TEXT_PP(2)));
    // TextDatumGetCString detoasts the `text` arg word; the varlena image is
    // carried in the by-ref side channel, addressed by the arg Datum.
    let flags_str =
        backend_utils_adt_varlena_seams::text_to_cstring::call(mcx, getarg_datum(fcinfo, 2))?;
    let flags = crate::range_io::range_parse_flags(flags_str.as_str())?;

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
    Ok(return_range_p(range))
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
    Ok(lower.val)
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
    Ok(upper.val)
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
    let val = getarg_datum(fcinfo, 1);
    let typcache = range_get_typcache(range_type_get_oid(r))?;
    Ok(Datum::from_bool(range_contains_elem_internal(&typcache, r, val)?))
}

/// `elem_contained_by_range(PG_FUNCTION_ARGS)` (rangetypes.c:559).
pub fn elem_contained_by_range<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let val = getarg_datum(fcinfo, 0);
    let r = getarg_range_p(mcx, fcinfo, 1)?;
    let typcache = range_get_typcache(range_type_get_oid(r))?;
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
    Ok(Datum::from_bool(range_adjacent_internal(&typcache, r1, r2)?))
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
        Ok(return_range_p(ret))
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
    Ok(return_range_p(ret))
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
    Ok(return_range_p(ret))
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
    Ok(return_range_p(ret))
}

/// `range_intersect_agg_transfn(PG_FUNCTION_ARGS)` (rangetypes.c:1221).
pub fn range_intersect_agg_transfn<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    // if (!AggCheckCallContext(fcinfo, &aggContext))
    //     elog(ERROR, "range_intersect_agg_transfn called in non-aggregate context");
    //
    // AggCheckCallContext is owned by backend-executor-nodeAgg (over the owned
    // `types_nodes` call frame, not the `types_fmgr` one this boundary carries);
    // no compatible seam exists yet, so route through the owner with a loud
    // panic until that unit lands (seam-and-panic for an unported dep).
    if !agg_check_call_context(fcinfo) {
        return Err(PgError::error(
            "range_intersect_agg_transfn called in non-aggregate context",
        ));
    }

    let rngtypoid = get_fn_expr_argtype(fcinfo.flinfo.as_deref(), 1);
    if !type_is_range(rngtypoid) {
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
        Some(range) => Ok(return_range_p(range)),
        None => Ok(return_null(fcinfo)),
    }
}

/// `AggCheckCallContext(fcinfo, &aggContext)` — owned by backend-executor-nodeAgg.
/// The `types_fmgr` call frame this boundary carries holds no AggState
/// back-reference, so the check cannot be answered here; route to the owner
/// (loud panic until nodeAgg exposes a compatible seam).
fn agg_check_call_context(_fcinfo: &FunctionCallInfoBaseData) -> bool {
    panic!(
        "backend_executor_nodeAgg::AggCheckCallContext: unported neighbor — \
         range_intersect_agg_transfn needs the aggregate call-context check"
    );
}

/// `type_is_range(typid)` — owned by utils/cache/lsyscache.c. No seam exists
/// yet; route to the owner with a loud panic until it lands.
fn type_is_range(_typid: types_core::primitive::Oid) -> bool {
    panic!("backend_utils_cache_lsyscache::type_is_range: unported neighbor");
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
    // job, reached over the owned call frame the `types_fmgr` boundary does not
    // carry. Route to the owner (loud panic until it lands).
    let _ = fcinfo;
    panic!(
        "backend_utils_sort_sortsupport::range_sortsupport: unported neighbor — \
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
    // overflow check, and range_serialize.
    let out = int4range_canonical(mcx, &typcache, r)?;
    Ok(return_range_p(out))
}

/// `int8range_canonical(PG_FUNCTION_ARGS)` (rangetypes.c:1574).
pub fn int8range_canonical_v1<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r = getarg_range_p(mcx, fcinfo, 0)?;
    let typcache = range_get_typcache(range_type_get_oid(r))?;
    let out = int8range_canonical(mcx, &typcache, r)?;
    Ok(return_range_p(out))
}

/// `daterange_canonical(PG_FUNCTION_ARGS)` (rangetypes.c:1622).
pub fn daterange_canonical_v1<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let r = getarg_range_p(mcx, fcinfo, 0)?;
    let typcache = range_get_typcache(range_type_get_oid(r))?;
    let out = daterange_canonical(mcx, &typcache, r)?;
    Ok(return_range_p(out))
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
// ---------------------------------------------------------------------------

/// `elem_contained_by_range_support(PG_FUNCTION_ARGS)` (rangetypes.c:2251).
pub fn elem_contained_by_range_support<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    // Node *rawreq = (Node *) PG_GETARG_POINTER(0): the support request node is
    // a planner Node* (inherited opacity). The IsA(SupportRequestSimplify)
    // dispatch + find_simplified_clause live in the kernel.
    let rawreq = PlannerNode(getarg_datum(fcinfo, 0).as_usize() as u64);
    let ret = elem_contained_by_range_support_kernel(mcx, rawreq)?;
    Ok(Datum::from_usize(ret.0 as usize))
}

/// `range_contains_elem_support(PG_FUNCTION_ARGS)` (rangetypes.c:2277).
pub fn range_contains_elem_support<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let rawreq = PlannerNode(getarg_datum(fcinfo, 0).as_usize() as u64);
    let ret = range_contains_elem_support_kernel(mcx, rawreq)?;
    Ok(Datum::from_usize(ret.0 as usize))
}

// Silence the unused-import lint for `RangeType` (only its `rangetypid` field is
// reached, via the raw `*const RangeType` deref in `range_type_get_oid`).
#[allow(unused_imports)]
use RangeType as _RangeTypeHeader;
