use ::datum::Datum;
use ::lsyscache::IOFuncSelector;
use ::mcx::{Mcx, PgVec};
use ::types_core::{InvalidOid, Oid};
use ::types_error::{PgError, PgResult};
use ::types_fmgr::{
    byref_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};

use crate::io::{cached_range_io_data, range_parse_flags};
use crate::ops::{self, MinusResult, UnionResult};
use crate::{
    cached_range_info, make_range, range_deserialize, range_get_flags, range_serialize,
    range_type_oid, range_types_do_not_match, ElemInfo, RangeBound, RangeInfo, RANGE_EMPTY,
    RANGE_LB_INC, RANGE_LB_INF, RANGE_UB_INC, RANGE_UB_INF,
};

pub enum RangeArg<'m> {
    Borrowed(&'m [u8]),
    Owned(PgVec<'m, u8>),
}

impl core::ops::Deref for RangeArg<'_> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            RangeArg::Borrowed(s) => s,
            RangeArg::Owned(v) => v,
        }
    }
}

// PG_GETARG_RANGE_P / PG_GETARG_MULTIRANGE_P: full detoast to the 4-byte
// header form range_deserialize requires.
pub fn arg_range<'m>(fcinfo: &Fcinfo, i: usize, mcx: Mcx<'m>) -> PgResult<RangeArg<'m>> {
    // SAFETY: catalog args of these fns are non-null range/multirange varlenas.
    let p = unsafe { fcinfo.arg_ptr(i) };
    // SAFETY: live varlena header readable through its full VARSIZE_ANY.
    let total = unsafe { ::types_tuple::varatt::varsize_any(p) };
    // SAFETY: live varlena of `total` bytes.
    let raw = unsafe { core::slice::from_raw_parts(p, total) };
    if raw[0] & 0x03 == 0 {
        // SAFETY: the borrow lives as long as the argument datum (the call).
        Ok(RangeArg::Borrowed(unsafe { core::slice::from_raw_parts(p, total) }))
    } else {
        Ok(RangeArg::Owned(::detoast_seams::detoast_attr::call(mcx, raw)?))
    }
}

// Flinfo-less callers (the tuplesort comparison shim) memo here instead:
// C's range_fast_cmp caches the typcache entry in ssup_extra once per sort.
std::thread_local! {
    static SHIM_RI: core::cell::UnsafeCell<Option<core::mem::ManuallyDrop<RangeInfo>>> =
        const { core::cell::UnsafeCell::new(None) };
}

fn flinfo_ri<'f>(
    flinfo: Option<&'f mut FmgrInfo>,
    rngtypid: Oid,
) -> PgResult<&'f mut RangeInfo> {
    if let Some(fl) = flinfo {
        return cached_range_info(fl, rngtypid);
    }
    SHIM_RI.with(|c| {
        // SAFETY: single-threaded backend; the borrow ends before any path
        // that could re-enter this slot (element cmp fns never reach ranges).
        let slot = unsafe { &mut *c.get() };
        let stale = match slot {
            Some(ri) => ri.rngtypid != rngtypid,
            None => true,
        };
        if stale {
            let fresh = core::mem::ManuallyDrop::new(RangeInfo::lookup(rngtypid)?);
            if let Some(old) = slot.replace(fresh) {
                // SAFETY: the displaced memo has no outstanding borrow (the
                // slot borrow above is the only path in) and is never reused.
                drop(core::mem::ManuallyDrop::into_inner(old));
            }
        }
        // SAFETY: as above — the slot outlives the call and is not re-entered.
        Ok(unsafe { &mut *(&mut **slot.as_mut().unwrap() as *mut RangeInfo) })
    })
}

fn range_result(fcinfo: &Fcinfo, img: &[u8]) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), img)
}

pub fn fc_range_in(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let rngtypid = fcinfo.arg_oid(1);
    let typmod = fcinfo.arg_i32(2);
    let mcx = fcinfo.result_mcx();
    // SAFETY: catalog arg 0 of range_in is a non-null cstring.
    let input = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    // SAFETY: context, if set, is a live ErrorSaveNode armed for this call.
    let esc = unsafe { fcinfo.error_save_node() };
    let cache = cached_range_io_data(
        flinfo.expect("range_in: NULL flinfo"),
        rngtypid,
        IOFuncSelector::IOFunc_input,
    )?;
    match crate::io::range_in(mcx, cache, input, typmod, esc)? {
        Some(img) => byref_result(mcx, &img),
        None => Ok(Datum::from_usize(0)),
    }
}

pub fn fc_range_out(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let r = arg_range(fcinfo, 0, mcx)?;
    let cache = cached_range_io_data(
        flinfo.expect("range_out: NULL flinfo"),
        range_type_oid(&r),
        IOFuncSelector::IOFunc_output,
    )?;
    Ok(::types_fmgr::cstring_result(crate::io::range_out(mcx, cache, &r)?))
}

pub fn fc_range_recv(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let rngtypid = fcinfo.arg_oid(1);
    let typmod = fcinfo.arg_i32(2);
    let mcx = fcinfo.result_mcx();
    // SAFETY: arg 0 of a recv function is a live &mut StringInfo pointer.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let cache = cached_range_io_data(
        flinfo.expect("range_recv: NULL flinfo"),
        rngtypid,
        IOFuncSelector::IOFunc_receive,
    )?;
    let img = crate::io::range_recv(mcx, cache, buf, typmod)?;
    byref_result(mcx, &img)
}

pub fn fc_range_send(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let r = arg_range(fcinfo, 0, mcx)?;
    let cache = cached_range_io_data(
        flinfo.expect("range_send: NULL flinfo"),
        range_type_oid(&r),
        IOFuncSelector::IOFunc_send,
    )?;
    Ok(varlena_result(crate::io::range_send(mcx, cache, &r)?))
}

pub fn fc_range_constructor2(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("range constructor: NULL flinfo");
    let rngtypid = ::funcapi::get_fn_expr_rettype(flinfo);
    let mcx = fcinfo.result_mcx();
    let ri = cached_range_info(flinfo, rngtypid)?;
    let mut lower = RangeBound {
        val: if fcinfo.argisnull(0) { Datum::from_usize(0) } else { fcinfo.arg(0) },
        infinite: fcinfo.argisnull(0),
        inclusive: true,
        lower: true,
    };
    let mut upper = RangeBound {
        val: if fcinfo.argisnull(1) { Datum::from_usize(0) } else { fcinfo.arg(1) },
        infinite: fcinfo.argisnull(1),
        inclusive: false,
        lower: false,
    };
    let img = make_range(mcx, ri, &mut lower, &mut upper, false, None)?
        .expect("hard error path returns Some");
    byref_result(mcx, &img)
}

#[track_caller]
#[cold]
fn null_flags_arg() -> Box<PgError> {
    Box::new(
        PgError::error("range constructor flags argument must not be null")
            .with_sqlstate(::types_error::ERRCODE_DATA_EXCEPTION),
    )
}

pub fn fc_range_constructor3(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("range constructor: NULL flinfo");
    let rngtypid = ::funcapi::get_fn_expr_rettype(flinfo);
    let mcx = fcinfo.result_mcx();
    let ri = cached_range_info(flinfo, rngtypid)?;
    if fcinfo.argisnull(2) {
        return Err(null_flags_arg());
    }
    // SAFETY: arg 2 is a non-null text varlena.
    let flags_text = unsafe { fcinfo.arg_varlena_packed(2) }?;
    let flags = range_parse_flags(flags_text.data())?;
    let mut lower = RangeBound {
        val: if fcinfo.argisnull(0) { Datum::from_usize(0) } else { fcinfo.arg(0) },
        infinite: fcinfo.argisnull(0),
        inclusive: flags & RANGE_LB_INC != 0,
        lower: true,
    };
    let mut upper = RangeBound {
        val: if fcinfo.argisnull(1) { Datum::from_usize(0) } else { fcinfo.arg(1) },
        infinite: fcinfo.argisnull(1),
        inclusive: flags & RANGE_UB_INC != 0,
        lower: false,
    };
    let img = make_range(mcx, ri, &mut lower, &mut upper, false, None)?
        .expect("hard error path returns Some");
    byref_result(mcx, &img)
}

pub fn fc_range_lower(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let out = {
        let mcx = fcinfo.result_mcx();
        let r = arg_range(fcinfo, 0, mcx)?;
        let ri = flinfo_ri(flinfo, range_type_oid(&r))?;
        let (lower, _upper, empty) = range_deserialize(&ri.elem, &r);
        if empty || lower.infinite {
            None
        } else {
            Some(bound_datum_result(fcinfo, ri, lower.val)?)
        }
    };
    Ok(out.unwrap_or_else(|| fcinfo.return_null()))
}

pub fn fc_range_upper(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let out = {
        let mcx = fcinfo.result_mcx();
        let r = arg_range(fcinfo, 0, mcx)?;
        let ri = flinfo_ri(flinfo, range_type_oid(&r))?;
        let (_lower, upper, empty) = range_deserialize(&ri.elem, &r);
        if empty || upper.infinite {
            None
        } else {
            Some(bound_datum_result(fcinfo, ri, upper.val)?)
        }
    };
    Ok(out.unwrap_or_else(|| fcinfo.return_null()))
}

// A by-ref bound datum points into the (possibly detoasted-local) argument
// image; copy it into the result mcx before the image dies.
fn bound_datum_result(fcinfo: &Fcinfo, ri: &RangeInfo, val: Datum) -> PgResult<Datum> {
    if ri.elem.typbyval {
        return Ok(val);
    }
    let p = val.as_usize() as *const u8;
    let n = if ri.elem.typlen == -1 {
        // SAFETY: a byref bound datum is a live varlena inside the image.
        unsafe { ::types_tuple::varatt::varsize_any(p) }
    } else {
        ri.elem.typlen as usize
    };
    // SAFETY: live bound value of n bytes inside the argument image.
    byref_result(fcinfo.result_mcx(), unsafe { core::slice::from_raw_parts(p, n) })
}

macro_rules! fc_flag {
    ($($fc:ident: $bit:expr;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let mcx = fcinfo.result_mcx();
            let r = arg_range(fcinfo, 0, mcx)?;
            Ok(Datum::from_bool(range_get_flags(&r) & $bit != 0))
        }
    )*};
}

fc_flag! {
    fc_range_empty: RANGE_EMPTY;
    fc_range_lower_inc: RANGE_LB_INC;
    fc_range_upper_inc: RANGE_UB_INC;
    fc_range_lower_inf: RANGE_LB_INF;
    fc_range_upper_inf: RANGE_UB_INF;
}

pub fn fc_range_contains_elem(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let r = arg_range(fcinfo, 0, mcx)?;
    let val = fcinfo.arg(1);
    let ri = flinfo_ri(flinfo, range_type_oid(&r))?;
    Ok(Datum::from_bool(ops::range_contains_elem_internal(mcx, ri, &r, val)?))
}

pub fn fc_elem_contained_by_range(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let val = fcinfo.arg(0);
    let r = arg_range(fcinfo, 1, mcx)?;
    let ri = flinfo_ri(flinfo, range_type_oid(&r))?;
    Ok(Datum::from_bool(ops::range_contains_elem_internal(mcx, ri, &r, val)?))
}

macro_rules! fc_rr_bool {
    ($($fc:ident: $f:path;)*) => {$(
        pub fn $fc(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let mcx = fcinfo.result_mcx();
            let r1 = arg_range(fcinfo, 0, mcx)?;
            let r2 = arg_range(fcinfo, 1, mcx)?;
            let ri = flinfo_ri(flinfo, range_type_oid(&r1))?;
            Ok(Datum::from_bool($f(mcx, ri, &r1, &r2)?))
        }
    )*};
}

fc_rr_bool! {
    fc_range_eq: ops::range_eq_internal;
    fc_range_ne: ops::range_ne_internal;
    fc_range_contains: ops::range_contains_internal;
    fc_range_contained_by: ops::range_contained_by_internal;
    fc_range_before: ops::range_before_internal;
    fc_range_after: ops::range_after_internal;
    fc_range_overlaps: ops::range_overlaps_internal;
    fc_range_overleft: ops::range_overleft_internal;
    fc_range_overright: ops::range_overright_internal;
}

pub fn fc_range_adjacent(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let r1 = arg_range(fcinfo, 0, mcx)?;
    let r2 = arg_range(fcinfo, 1, mcx)?;
    let ri = flinfo_ri(flinfo, range_type_oid(&r1))?;
    Ok(Datum::from_bool(ops::range_adjacent_internal(mcx, ri, &r1, &r2)?))
}

pub fn fc_range_minus(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let r1 = arg_range(fcinfo, 0, mcx)?;
    let r2 = arg_range(fcinfo, 1, mcx)?;
    if range_type_oid(&r1) != range_type_oid(&r2) {
        return Err(range_types_do_not_match());
    }
    let ri = flinfo_ri(flinfo, range_type_oid(&r1))?;
    match ops::range_minus_internal(mcx, ri, &r1, &r2)? {
        MinusResult::Input1 => range_result(fcinfo, &r1),
        MinusResult::New(img) => range_result(fcinfo, &img),
    }
}

pub fn fc_range_union(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    range_union_common(flinfo, fcinfo, true)
}

pub fn fc_range_merge(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    range_union_common(flinfo, fcinfo, false)
}

fn range_union_common(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    strict: bool,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let r1 = arg_range(fcinfo, 0, mcx)?;
    let r2 = arg_range(fcinfo, 1, mcx)?;
    let ri = flinfo_ri(flinfo, range_type_oid(&r1))?;
    match ops::range_union_internal(mcx, ri, &r1, &r2, strict)? {
        UnionResult::Input1 => range_result(fcinfo, &r1),
        UnionResult::Input2 => range_result(fcinfo, &r2),
        UnionResult::New(img) => range_result(fcinfo, &img),
    }
}

pub fn fc_range_intersect(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let r1 = arg_range(fcinfo, 0, mcx)?;
    let r2 = arg_range(fcinfo, 1, mcx)?;
    if range_type_oid(&r1) != range_type_oid(&r2) {
        return Err(range_types_do_not_match());
    }
    let ri = flinfo_ri(flinfo, range_type_oid(&r1))?;
    let img = ops::range_intersect_internal(mcx, ri, &r1, &r2)?;
    range_result(fcinfo, &img)
}

#[track_caller]
#[cold]
fn non_aggregate_context(what: &str) -> Box<PgError> {
    Box::new(PgError::error(format!("{what} called in non-aggregate context")))
}

pub fn fc_range_intersect_agg_transfn(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: context, if set, is the evaltrans build's AggStateNode.
    if unsafe { fcinfo.agg_context() }.is_none() {
        return Err(non_aggregate_context("range_intersect_agg_transfn"));
    }
    let flinfo = flinfo.expect("range_intersect_agg_transfn: NULL flinfo");
    let rngtypoid = ::funcapi::get_fn_expr_argtype(Some(flinfo), 1);
    if ::lsyscache::get_range_subtype(rngtypoid)? == InvalidOid {
        return Err(Box::new(PgError::error("range_intersect_agg must be called with a range")));
    }
    let mcx = fcinfo.result_mcx();
    let r1 = arg_range(fcinfo, 0, mcx)?;
    let r2 = arg_range(fcinfo, 1, mcx)?;
    let ri = cached_range_info(flinfo, rngtypoid)?;
    let img = ops::range_intersect_internal(mcx, ri, &r1, &r2)?;
    range_result(fcinfo, &img)
}

pub fn fc_range_cmp(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let r1 = arg_range(fcinfo, 0, mcx)?;
    let r2 = arg_range(fcinfo, 1, mcx)?;
    let ri = flinfo_ri(flinfo, range_type_oid(&r1))?;
    Ok(Datum::from_i32(ops::range_cmp_internal(mcx, ri, &r1, &r2)?))
}

macro_rules! fc_cmp_op {
    ($($fc:ident: $op:tt;)*) => {$(
        pub fn $fc(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let mcx = fcinfo.result_mcx();
            let r1 = arg_range(fcinfo, 0, mcx)?;
            let r2 = arg_range(fcinfo, 1, mcx)?;
            let ri = flinfo_ri(flinfo, range_type_oid(&r1))?;
            Ok(Datum::from_bool(ops::range_cmp_internal(mcx, ri, &r1, &r2)? $op 0))
        }
    )*};
}

fc_cmp_op! {
    fc_range_lt: <;
    fc_range_le: <=;
    fc_range_ge: >=;
    fc_range_gt: >;
}

pub fn fc_hash_range(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let r = arg_range(fcinfo, 0, mcx)?;
    let ri = flinfo_ri(flinfo, range_type_oid(&r))?;
    Ok(Datum::from_i32(ops::hash_range_internal(mcx, ri, &r)? as i32))
}

pub fn fc_hash_range_extended(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let r = arg_range(fcinfo, 0, mcx)?;
    let seed = fcinfo.arg(1);
    let ri = flinfo_ri(flinfo, range_type_oid(&r))?;
    Ok(Datum::from_u64(ops::hash_range_extended_internal(mcx, ri, &r, seed)?))
}

// The canonical pg_proc entry points (make_range dispatches to the adjusters
// natively; these remain callable as SQL functions).
macro_rules! fc_canonical {
    ($($fc:ident: $adj:path;)*) => {$(
        pub fn $fc(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let mcx = fcinfo.result_mcx();
            let r = arg_range(fcinfo, 0, mcx)?;
            let ri = flinfo_ri(flinfo, range_type_oid(&r))?;
            let (mut lower, mut upper, empty) = range_deserialize(&ri.elem, &r);
            if empty {
                return range_result(fcinfo, &r);
            }
            // SAFETY: context, if set, is a live ErrorSaveNode armed for this call.
            let mut esc = unsafe { fcinfo.error_save_node() };
            if !$adj(&mut lower, &mut upper, esc.as_deref_mut().map(|n| &mut n.ctx))? {
                return Ok(Datum::from_usize(0));
            }
            match range_serialize(mcx, ri, &mut lower, &mut upper, false,
                esc.as_deref_mut().map(|n| &mut n.ctx))? {
                Some(img) => range_result(fcinfo, &img),
                None => Ok(Datum::from_usize(0)),
            }
        }
    )*};
}

fc_canonical! {
    fc_int4range_canonical: crate::canonical_adjust_i32;
    fc_int8range_canonical: crate::canonical_adjust_i64;
    fc_daterange_canonical: crate::canonical_adjust_date;
}

pub fn fc_int4range_subdiff(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_f64(fcinfo.arg_i32(0) as f64 - fcinfo.arg_i32(1) as f64))
}

pub fn fc_int8range_subdiff(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_f64(fcinfo.arg_i64(0) as f64 - fcinfo.arg_i64(1) as f64))
}

pub fn fc_daterange_subdiff(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_f64(fcinfo.arg_i32(0) as f64 - fcinfo.arg_i32(1) as f64))
}

const USECS_PER_SEC: f64 = 1_000_000.0;

pub fn fc_tsrange_subdiff(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_f64(
        (fcinfo.arg_i64(0) as f64 - fcinfo.arg_i64(1) as f64) / USECS_PER_SEC,
    ))
}

pub fn fc_tstzrange_subdiff(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_f64(
        (fcinfo.arg_i64(0) as f64 - fcinfo.arg_i64(1) as f64) / USECS_PER_SEC,
    ))
}

pub fn fc_numrange_subdiff(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let diff = ::types_fmgr::direct_function_call2_coll_in(
        ::adt_numeric::builtins::fc_numeric_sub,
        InvalidOid,
        fcinfo.result_mcx(),
        fcinfo.arg(0),
        fcinfo.arg(1),
    )?;
    ::types_fmgr::direct_function_call1_coll_in(
        ::adt_numeric::builtins::fc_numeric_float8,
        InvalidOid,
        fcinfo.result_mcx(),
        diff,
    )
}

pub fn fc_range_sortsupport(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    panic!("range_sortsupport not ported: sorts ride the cmp-proc shim on range_cmp");
}

// find_simplified_clause (rangetypes.c): rewrite `elem <@ const-range` /
// `const-range @> elem` into btree boundary comparisons. Returns 0 (C NULL)
// to keep the original clause when no rewrite applies.
fn range_elem_support(fcinfo: &mut Fcinfo, range_is_left: bool, name: &str) -> PgResult<Datum> {
    use ::types_nodes::{supportnodes::SupportRequestSimplify, NodeTag};
    let [a] = fcinfo.args_n::<1>();
    let p = a.value.as_usize() as *const NodeTag;
    // SAFETY: prosupport contract — arg points at a live tag-first node.
    if unsafe { *p } != NodeTag::T_SupportRequestSimplify {
        return Ok(Datum::from_usize(0));
    }
    // SAFETY: tag checked; the planner owns the request node for the call.
    let req = unsafe { &*(a.value.as_usize() as *const SupportRequestSimplify) };
    let fexpr = req
        .fcall
        .and_then(|n| n.as_func_expr())
        .unwrap_or_else(|| panic!("{name}: SupportRequestSimplify without a FuncExpr fcall"));
    assert_eq!(fexpr.args.len(), 2);
    let range_arg = fexpr.args.nth(if range_is_left { 0 } else { 1 });
    let elem_expr = fexpr.args.nth(if range_is_left { 1 } else { 0 });
    match range_arg.as_const() {
        Some(c) if !c.constisnull => {
            let mcx = req.mcx.unwrap_or_else(|| panic!("{name}: request carries an mcx"));
            match find_simplified_clause(mcx, c.constvalue, elem_expr)? {
                Some(node) => Ok(Datum::from_usize(node.as_raw().as_ptr() as usize)),
                None => Ok(Datum::from_usize(0)),
            }
        }
        _ => Ok(Datum::from_usize(0)),
    }
}

// find_simplified_clause (rangetypes.c); the volatile/subplan/cost gate for
// the two-bound case rides clauses_seams::expr_safe_to_evaluate_twice.
fn find_simplified_clause<'mcx>(
    mcx: Mcx<'mcx>,
    range_const: Datum,
    elem_expr: ::types_nodes::Node<'mcx>,
) -> PgResult<Option<::types_nodes::Node<'mcx>>> {
    use ::types_nodes::NodeTag;

    // DatumGetRangeTypeP: detoast into the request's (planner) context so
    // by-ref bound datums outlive the rewritten clause.
    let praw = range_const.as_usize() as *const u8;
    // SAFETY: a non-null range Const's value is a live varlena.
    let total = unsafe { ::types_tuple::varatt::varsize_any(praw) };
    // SAFETY: live varlena of `total` bytes.
    let raw = unsafe { core::slice::from_raw_parts(praw, total) };
    let range: &[u8] = if raw[0] & 0x03 == 0 {
        raw
    } else {
        ::detoast_seams::detoast_attr::call(mcx, raw)?.leak()
    };

    let entry = ::typcache::lookup_type_cache(range_type_oid(range), ::typcache::TYPECACHE_RANGE_INFO)?;
    let Some(elem_entry) = entry.rngelemtype() else {
        return Err(crate::not_a_range_type(range_type_oid(range)));
    };
    let elem_info = ElemInfo {
        typlen: elem_entry.typlen(),
        typbyval: elem_entry.typbyval(),
        typalign: elem_entry.typalign() as u8,
        typstorage: elem_entry.typstorage() as u8,
    };
    let (lower, upper, empty) = range_deserialize(&elem_info, range);

    if empty {
        // An empty range matches nothing.
        return Ok(Some(make_bool_const(mcx, false)?));
    }
    if lower.infinite && upper.infinite {
        // Infinite bounds on both sides match everything.
        return Ok(Some(make_bool_const(mcx, true)?));
    }

    if !lower.infinite && !upper.infinite {
        // The rewrite evaluates elemExpr twice; C declines volatile,
        // subplan-bearing, or >10*cpu_operator_cost elemExprs.
        if !::clauses_seams::expr_safe_to_evaluate_twice::call(elem_expr)? {
            return Ok(None);
        }
    }

    let opfamily = entry.rng_opfamily();
    let rng_collation = entry.rng_collation();
    let mut lower_expr = None;
    let mut upper_expr = None;
    if !lower.infinite {
        lower_expr =
            build_bound_expr(mcx, elem_expr, lower.val, true, lower.inclusive, &elem_entry, opfamily, rng_collation)?;
        if lower_expr.is_none() {
            return Ok(None);
        }
    }
    if !upper.infinite {
        // C copies the elemExpr for the second comparison; nodes here are
        // immutable arena shares, so the same node serves both OpExprs.
        upper_expr =
            build_bound_expr(mcx, elem_expr, upper.val, false, upper.inclusive, &elem_entry, opfamily, rng_collation)?;
        if upper_expr.is_none() {
            return Ok(None);
        }
    }

    match (lower_expr, upper_expr) {
        (Some(l), Some(u)) => Ok(Some(::types_nodes::Node::mk(
            mcx,
            ::types_nodes::primnodes::BoolExpr {
                boolop: ::types_nodes::primnodes::BoolExprType::AND_EXPR,
                args: ::types_nodes::NodeList::make2(mcx, l, u)?,
                location: -1,
            },
        )?)),
        (Some(l), None) => Ok(Some(l)),
        (None, Some(u)) => Ok(Some(u)),
        (None, None) => unreachable!("at least one bound is finite"),
    }
}

// build_bound_expr (rangetypes.c): (elemExpr <op> bound-val) via the range's
// btree opfamily member for the bound's strategy.
#[allow(clippy::too_many_arguments)]
fn build_bound_expr<'mcx>(
    mcx: Mcx<'mcx>,
    elem_expr: ::types_nodes::Node<'mcx>,
    val: Datum,
    is_lower_bound: bool,
    is_inclusive: bool,
    elem_entry: &::typcache::TypeCacheEntry,
    opfamily: Oid,
    rng_collation: Oid,
) -> PgResult<Option<::types_nodes::Node<'mcx>>> {
    use ::lsyscache::{BTGreaterStrategyNumber, BTLessStrategyNumber};
    // stratnum.h members lsyscache doesn't carry yet.
    const BTLessEqualStrategyNumber: i16 = 2;
    const BTGreaterEqualStrategyNumber: i16 = 4;
    let elem_type = elem_entry.type_id;
    let strategy = if is_lower_bound {
        if is_inclusive { BTGreaterEqualStrategyNumber } else { BTGreaterStrategyNumber }
    } else if is_inclusive {
        BTLessEqualStrategyNumber
    } else {
        BTLessStrategyNumber
    };
    let oproid = ::lsyscache::get_opfamily_member(opfamily, elem_type, elem_type, strategy)?;
    if oproid == InvalidOid {
        return Ok(None);
    }
    let const_expr = ::types_nodes::Node::mk(
        mcx,
        ::types_nodes::primnodes::Const {
            consttype: elem_type,
            consttypmod: -1,
            constcollid: elem_entry.typcollation(),
            constlen: elem_entry.typlen() as i32,
            constvalue: val,
            constisnull: false,
            constbyval: elem_entry.typbyval(),
            location: -1,
        },
    )?;
    Ok(Some(::types_nodes::Node::mk(
        mcx,
        ::types_nodes::primnodes::OpExpr {
            opno: oproid,
            opfuncid: ::lsyscache::get_opcode(oproid)?,
            opresulttype: ::types_core::catalog::BOOLOID,
            opretset: false,
            opcollid: InvalidOid,
            inputcollid: rng_collation,
            args: ::types_nodes::NodeList::make2(mcx, elem_expr, const_expr)?,
            location: -1,
        },
    )?))
}

// makeBoolConst (makefuncs.c), non-null leg.
fn make_bool_const<'mcx>(mcx: Mcx<'mcx>, value: bool) -> PgResult<::types_nodes::Node<'mcx>> {
    ::types_nodes::Node::mk(
        mcx,
        ::types_nodes::primnodes::Const {
            consttype: ::types_core::catalog::BOOLOID,
            consttypmod: -1,
            constcollid: InvalidOid,
            constlen: 1,
            constvalue: Datum::from_bool(value),
            constisnull: false,
            constbyval: true,
            location: -1,
        },
    )
}

pub fn fc_range_contains_elem_support(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    range_elem_support(fcinfo, true, "range_contains_elem_support")
}

pub fn fc_elem_contained_by_range_support(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    range_elem_support(fcinfo, false, "elem_contained_by_range_support")
}

const fn b(foid: Oid, name: &'static str, nargs: i16, strict: bool, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict, retset: false, func }
}

// pg_proc.dat rows for rangetypes.c, OID-ascending.
pub const RANGETYPES_BUILTINS: &[FmgrBuiltin] = &[
    b(3417, "hash_range_extended", 2, true, fc_hash_range_extended),
    b(3834, "range_in", 3, true, fc_range_in),
    b(3835, "range_out", 1, true, fc_range_out),
    // anyrange_out (pseudotypes.c) is `return range_out(fcinfo)`.
    b(3833, "anyrange_out", 1, true, fc_range_out),
    b(3836, "range_recv", 3, true, fc_range_recv),
    b(3837, "range_send", 1, true, fc_range_send),
    b(3840, "int4range", 2, false, fc_range_constructor2),
    b(3841, "int4range", 3, false, fc_range_constructor3),
    b(3844, "numrange", 2, false, fc_range_constructor2),
    b(3845, "numrange", 3, false, fc_range_constructor3),
    b(3848, "lower", 1, true, fc_range_lower),
    b(3849, "upper", 1, true, fc_range_upper),
    b(3850, "isempty", 1, true, fc_range_empty),
    b(3851, "lower_inc", 1, true, fc_range_lower_inc),
    b(3852, "upper_inc", 1, true, fc_range_upper_inc),
    b(3853, "lower_inf", 1, true, fc_range_lower_inf),
    b(3854, "upper_inf", 1, true, fc_range_upper_inf),
    b(3855, "range_eq", 2, true, fc_range_eq),
    b(3856, "range_ne", 2, true, fc_range_ne),
    b(3857, "range_overlaps", 2, true, fc_range_overlaps),
    b(3858, "range_contains_elem", 2, true, fc_range_contains_elem),
    b(3859, "range_contains", 2, true, fc_range_contains),
    b(3860, "elem_contained_by_range", 2, true, fc_elem_contained_by_range),
    b(3861, "range_contained_by", 2, true, fc_range_contained_by),
    b(3862, "range_adjacent", 2, true, fc_range_adjacent),
    b(3863, "range_before", 2, true, fc_range_before),
    b(3864, "range_after", 2, true, fc_range_after),
    b(3865, "range_overleft", 2, true, fc_range_overleft),
    b(3866, "range_overright", 2, true, fc_range_overright),
    b(3867, "range_union", 2, true, fc_range_union),
    b(3868, "range_intersect", 2, true, fc_range_intersect),
    b(3869, "range_minus", 2, true, fc_range_minus),
    b(3870, "range_cmp", 2, true, fc_range_cmp),
    b(3871, "range_lt", 2, true, fc_range_lt),
    b(3872, "range_le", 2, true, fc_range_le),
    b(3873, "range_ge", 2, true, fc_range_ge),
    b(3874, "range_gt", 2, true, fc_range_gt),
    b(3902, "hash_range", 1, true, fc_hash_range),
    b(3914, "int4range_canonical", 1, true, fc_int4range_canonical),
    b(3915, "daterange_canonical", 1, true, fc_daterange_canonical),
    b(3922, "int4range_subdiff", 2, true, fc_int4range_subdiff),
    b(3923, "int8range_subdiff", 2, true, fc_int8range_subdiff),
    b(3924, "numrange_subdiff", 2, true, fc_numrange_subdiff),
    b(3925, "daterange_subdiff", 2, true, fc_daterange_subdiff),
    b(3928, "int8range_canonical", 1, true, fc_int8range_canonical),
    b(3929, "tsrange_subdiff", 2, true, fc_tsrange_subdiff),
    b(3930, "tstzrange_subdiff", 2, true, fc_tstzrange_subdiff),
    b(3933, "tsrange", 2, false, fc_range_constructor2),
    b(3934, "tsrange", 3, false, fc_range_constructor3),
    b(3937, "tstzrange", 2, false, fc_range_constructor2),
    b(3938, "tstzrange", 3, false, fc_range_constructor3),
    b(3941, "daterange", 2, false, fc_range_constructor2),
    b(3942, "daterange", 3, false, fc_range_constructor3),
    b(3945, "int8range", 2, false, fc_range_constructor2),
    b(3946, "int8range", 3, false, fc_range_constructor3),
    b(4057, "range_merge", 2, true, fc_range_merge),
    b(4401, "range_intersect_agg_transfn", 2, true, fc_range_intersect_agg_transfn),
    b(6345, "range_contains_elem_support", 1, true, fc_range_contains_elem_support),
    b(6346, "elem_contained_by_range_support", 1, true, fc_elem_contained_by_range_support),
    b(6391, "range_sortsupport", 1, true, fc_range_sortsupport),
];
