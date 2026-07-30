use ::adt_rangetypes::builtins::{arg_range, RangeArg};
use ::adt_rangetypes::{range_is_empty, range_type_oid};
use ::datum::Datum;
use ::lsyscache::IOFuncSelector;
use ::mcx::{Mcx, PgVec};
use ::types_core::{InvalidOid, Oid};
use ::types_error::{
    PgError, PgResult, ERRCODE_CARDINALITY_VIOLATION, ERRCODE_NULL_VALUE_NOT_ALLOWED,
};
use ::types_fmgr::{
    byref_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};

use crate::io::cached_multirange_io_data;
use crate::{
    cached_multirange_info, leak_image, make_multirange, multirange_count,
    multirange_deserialize, multirange_get_bounds, multirange_get_range, multirange_is_empty,
    multirange_type_oid, multirange_types_do_not_match, MultirangeInfo,
};

// PG_GETARG_MULTIRANGE_P: same detoast contract as ranges.
fn arg_multirange<'m>(fcinfo: &Fcinfo, i: usize, mcx: Mcx<'m>) -> PgResult<RangeArg<'m>> {
    arg_range(fcinfo, i, mcx)
}

// Flinfo-less callers (the tuplesort comparison shim) memo here; see the
// rangetypes SHIM_RI note.
std::thread_local! {
    static SHIM_MI: core::cell::UnsafeCell<Option<core::mem::ManuallyDrop<MultirangeInfo>>> =
        const { core::cell::UnsafeCell::new(None) };
}

fn flinfo_mi<'f>(
    flinfo: Option<&'f mut FmgrInfo>,
    mltrngtypid: Oid,
) -> PgResult<&'f mut MultirangeInfo> {
    if let Some(fl) = flinfo {
        return cached_multirange_info(fl, mltrngtypid);
    }
    SHIM_MI.with(|c| {
        // SAFETY: single-threaded backend; not re-entered across the borrow.
        let slot = unsafe { &mut *c.get() };
        let stale = match slot {
            Some(mi) => mi.mltrngtypid != mltrngtypid,
            None => true,
        };
        if stale {
            let fresh = core::mem::ManuallyDrop::new(MultirangeInfo::lookup(mltrngtypid)?);
            if let Some(old) = slot.replace(fresh) {
                // SAFETY: the displaced memo has no outstanding borrow (the
                // slot borrow above is the only path in) and is never reused.
                drop(core::mem::ManuallyDrop::into_inner(old));
            }
        }
        // SAFETY: as above.
        Ok(unsafe { &mut *(&mut **slot.as_mut().unwrap() as *mut MultirangeInfo) })
    })
}

fn mr_result(fcinfo: &Fcinfo, img: &[u8]) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), img)
}

pub fn fc_multirange_in(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mltrngtypid = fcinfo.arg_oid(1);
    let typmod = fcinfo.arg_i32(2);
    let mcx = fcinfo.result_mcx();
    // SAFETY: catalog arg 0 of multirange_in is a non-null cstring.
    let input = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    // SAFETY: context, if set, is a live ErrorSaveNode armed for this call.
    let esc = unsafe { fcinfo.error_save_node() };
    let cache = cached_multirange_io_data(
        flinfo.expect("multirange_in: NULL flinfo"),
        mltrngtypid,
        IOFuncSelector::IOFunc_input,
    )?;
    match crate::io::multirange_in(mcx, cache, input, typmod, esc)? {
        Some(img) => byref_result(mcx, &img),
        None => Ok(Datum::from_usize(0)),
    }
}

pub fn fc_multirange_out(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mr = arg_multirange(fcinfo, 0, mcx)?;
    let cache = cached_multirange_io_data(
        flinfo.expect("multirange_out: NULL flinfo"),
        multirange_type_oid(&mr),
        IOFuncSelector::IOFunc_output,
    )?;
    Ok(::types_fmgr::cstring_result(crate::io::multirange_out(mcx, cache, &mr)?))
}

pub fn fc_multirange_recv(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mltrngtypid = fcinfo.arg_oid(1);
    let typmod = fcinfo.arg_i32(2);
    let mcx = fcinfo.result_mcx();
    // SAFETY: arg 0 of a recv function is a live &mut StringInfo pointer.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let cache = cached_multirange_io_data(
        flinfo.expect("multirange_recv: NULL flinfo"),
        mltrngtypid,
        IOFuncSelector::IOFunc_receive,
    )?;
    let img = crate::io::multirange_recv(mcx, cache, buf, typmod)?;
    byref_result(mcx, &img)
}

pub fn fc_multirange_send(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mr = arg_multirange(fcinfo, 0, mcx)?;
    let cache = cached_multirange_io_data(
        flinfo.expect("multirange_send: NULL flinfo"),
        multirange_type_oid(&mr),
        IOFuncSelector::IOFunc_send,
    )?;
    Ok(varlena_result(crate::io::multirange_send(mcx, cache, &mr)?))
}

#[track_caller]
#[cold]
fn null_member() -> Box<PgError> {
    Box::new(
        PgError::error("multirange values cannot contain null members")
            .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED),
    )
}

pub fn fc_multirange_constructor0(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("multirange constructor: NULL flinfo");
    let mltrngtypid = ::funcapi::get_fn_expr_rettype(flinfo);
    let mcx = fcinfo.result_mcx();
    let mi = cached_multirange_info(flinfo, mltrngtypid)?;
    let img = crate::make_empty_multirange(mcx, mltrngtypid, &mut mi.rng)?;
    mr_result(fcinfo, &img)
}

pub fn fc_multirange_constructor1(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("multirange constructor: NULL flinfo");
    let mltrngtypid = ::funcapi::get_fn_expr_rettype(flinfo);
    let mcx = fcinfo.result_mcx();
    if fcinfo.argisnull(0) {
        return Err(null_member());
    }
    let r = arg_range(fcinfo, 0, mcx)?;
    let mi = cached_multirange_info(flinfo, mltrngtypid)?;
    if range_type_oid(&r) != mi.rng.rngtypid {
        return Err(constructor_type_mismatch(range_type_oid(&r)));
    }
    let mut ranges: PgVec<'_, &[u8]> = ::mcx::vec_with_capacity_in(mcx, 1)?;
    ranges.push(&r[..]);
    let img = make_multirange(mcx, mltrngtypid, &mut mi.rng, &mut ranges)?;
    mr_result(fcinfo, &img)
}

#[track_caller]
#[cold]
fn constructor_type_mismatch(rngtypid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("type {rngtypid} does not match constructor type")))
}

#[track_caller]
#[cold]
fn multidimensional_array() -> Box<PgError> {
    Box::new(
        PgError::error("multiranges cannot be constructed from multidimensional arrays")
            .with_sqlstate(ERRCODE_CARDINALITY_VIOLATION),
    )
}

pub fn fc_multirange_constructor2(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("multirange constructor: NULL flinfo");
    let mltrngtypid = ::funcapi::get_fn_expr_rettype(flinfo);
    let mcx = fcinfo.result_mcx();
    if fcinfo.argisnull(0) {
        return Err(null_member());
    }
    // SAFETY: arg 0 is a non-null array varlena.
    let p = unsafe { fcinfo.arg_ptr(0) };
    // SAFETY: live varlena header readable through its full VARSIZE_ANY.
    let total = unsafe { ::types_tuple::varatt::varsize_any(p) };
    // SAFETY: live varlena of `total` bytes.
    let raw = unsafe { core::slice::from_raw_parts(p, total) };
    let array = ::detoast_seams::detoast_attr::call(mcx, raw)?;

    let mi = cached_multirange_info(flinfo, mltrngtypid)?;
    let ndim = ::arrayfuncs::foundation::arr_ndim(&array);
    if ndim > 1 {
        return Err(multidimensional_array());
    }
    let rngtypid = ::arrayfuncs::foundation::arr_elemtype(&array);
    if rngtypid != mi.rng.rngtypid {
        return Err(constructor_type_mismatch(rngtypid));
    }

    // ranges slices borrow the detoasted array image: array must stay live
    // past the make_multirange consumption (lifetimes laundered below).
    let mut ranges: PgVec<'_, &[u8]> = ::mcx::vec_with_capacity_in(mcx, 8)?;
    if ndim != 0 {
        let (elems, nulls) = ::arrayfuncs::deconstruct_array(
            mcx,
            &array,
            mi.rng.own_typlen as i32,
            mi.rng.own_typbyval,
            mi.rng.own_typalign,
            true,
        )?;
        for (i, d) in elems.iter().enumerate() {
            if nulls.get(i).copied().unwrap_or(false) {
                return Err(null_member());
            }
            let rp = d.as_usize() as *const u8;
            // Array members can be short-form; expand to the 4-byte form.
            let expanded = if unsafe { *rp } & 0x03 != 0 {
                // SAFETY: live varlena member inside the array image.
                let n = unsafe { ::types_tuple::varatt::varsize_any(rp) };
                // SAFETY: live varlena of n bytes.
                let raw = unsafe { core::slice::from_raw_parts(rp, n) };
                leak_image(::detoast_seams::detoast_attr::call(mcx, raw)?)
            } else {
                let n = ::adt_rangetypes::varsize_4b(rp);
                // SAFETY: live varlena of n bytes inside the array image.
                unsafe { core::slice::from_raw_parts(rp, n) }
            };
            ranges.push(expanded);
        }
    }
    let img = make_multirange(mcx, mltrngtypid, &mut mi.rng, &mut ranges)?;
    mr_result(fcinfo, &img)
}

pub fn fc_multirange_union(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mr1 = arg_multirange(fcinfo, 0, mcx)?;
    let mr2 = arg_multirange(fcinfo, 1, mcx)?;
    if multirange_is_empty(&mr1) {
        return mr_result(fcinfo, &mr2);
    }
    if multirange_is_empty(&mr2) {
        return mr_result(fcinfo, &mr1);
    }
    let mi = flinfo_mi(flinfo, multirange_type_oid(&mr1))?;
    let ranges1 = multirange_deserialize(mcx, &mi.rng, &mr1)?;
    let ranges2 = multirange_deserialize(mcx, &mi.rng, &mr2)?;
    let mut ranges3: PgVec<'_, &[u8]> =
        ::mcx::vec_with_capacity_in(mcx, ranges1.len() + ranges2.len())?;
    for r in ranges1.iter().chain(ranges2.iter()) {
        ranges3.push(*r);
    }
    let img = make_multirange(mcx, mi.mltrngtypid, &mut mi.rng, &mut ranges3)?;
    mr_result(fcinfo, &img)
}

pub fn fc_multirange_minus(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mr1 = arg_multirange(fcinfo, 0, mcx)?;
    let mr2 = arg_multirange(fcinfo, 1, mcx)?;
    let mltrngtypoid = multirange_type_oid(&mr1);
    let mi = flinfo_mi(flinfo, mltrngtypoid)?;
    if multirange_is_empty(&mr1) || multirange_is_empty(&mr2) {
        return mr_result(fcinfo, &mr1);
    }
    let ranges1 = multirange_deserialize(mcx, &mi.rng, &mr1)?;
    let ranges2 = multirange_deserialize(mcx, &mi.rng, &mr2)?;
    let img = crate::multirange_minus_internal(mcx, mltrngtypoid, &mut mi.rng, &ranges1, &ranges2)?;
    mr_result(fcinfo, &img)
}

pub fn fc_multirange_intersect(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mr1 = arg_multirange(fcinfo, 0, mcx)?;
    let mr2 = arg_multirange(fcinfo, 1, mcx)?;
    let mltrngtypoid = multirange_type_oid(&mr1);
    let mi = flinfo_mi(flinfo, mltrngtypoid)?;
    if multirange_is_empty(&mr1) || multirange_is_empty(&mr2) {
        let img = crate::make_empty_multirange(mcx, mltrngtypoid, &mut mi.rng)?;
        return mr_result(fcinfo, &img);
    }
    let ranges1 = multirange_deserialize(mcx, &mi.rng, &mr1)?;
    let ranges2 = multirange_deserialize(mcx, &mi.rng, &mr2)?;
    let img =
        crate::multirange_intersect_internal(mcx, mltrngtypoid, &mut mi.rng, &ranges1, &ranges2)?;
    mr_result(fcinfo, &img)
}

#[track_caller]
#[cold]
fn non_aggregate_context(what: &str) -> Box<PgError> {
    Box::new(PgError::error(format!("{what} called in non-aggregate context")))
}

pub fn fc_multirange_intersect_agg_transfn(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: context, if set, is the evaltrans build's AggStateNode.
    if unsafe { fcinfo.agg_context() }.is_none() {
        return Err(non_aggregate_context("multirange_intersect_agg_transfn"));
    }
    let flinfo = flinfo.expect("multirange_intersect_agg_transfn: NULL flinfo");
    let mltrngtypoid = ::funcapi::get_fn_expr_argtype(Some(flinfo), 1);
    if ::lsyscache::get_multirange_range(mltrngtypoid)? == InvalidOid {
        return Err(Box::new(PgError::error(
            "range_intersect_agg must be called with a multirange",
        )));
    }
    let mcx = fcinfo.result_mcx();
    let mr1 = arg_multirange(fcinfo, 0, mcx)?;
    let mr2 = arg_multirange(fcinfo, 1, mcx)?;
    let mi = cached_multirange_info(flinfo, mltrngtypoid)?;
    let ranges1 = multirange_deserialize(mcx, &mi.rng, &mr1)?;
    let ranges2 = multirange_deserialize(mcx, &mi.rng, &mr2)?;
    let img =
        crate::multirange_intersect_internal(mcx, mltrngtypoid, &mut mi.rng, &ranges1, &ranges2)?;
    mr_result(fcinfo, &img)
}

// The internal transvalue is a raw pointer to an aggcontext-placed
// ArrayBuildState (the fc_array_agg_array_transfn round-trip shape).
fn agg_state_slot<'a>(
    fcinfo: &Fcinfo,
    aggmcx: Mcx<'a>,
    element_type: Oid,
) -> PgResult<*mut ::datum::array_build::ArrayBuildState<'a>> {
    if fcinfo.argisnull(0) {
        let st = ::arrayfuncs::init_array_result(aggmcx, element_type, false)?;
        let layout = core::alloc::Layout::new::<::datum::array_build::ArrayBuildState<'a>>();
        let raw = ::mcx::Allocator::allocate(&aggmcx, layout)
            .map_err(|_| aggmcx.oom(layout.size()))?;
        let p: *mut ::datum::array_build::ArrayBuildState<'a> = raw.cast().as_ptr();
        // SAFETY: fresh aggcontext allocation of the exact layout; no drop
        // glue runs (PgVec fields are arena-plain).
        unsafe { p.write(st) };
        Ok(p)
    } else {
        Ok(fcinfo.arg(0).as_usize() as *mut ::datum::array_build::ArrayBuildState<'a>)
    }
}

pub fn fc_range_agg_transfn(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: context, if set, is the executor's live AggStateNode.
    let Some(aggmcx) = (unsafe { fcinfo.agg_context() }) else {
        return Err(non_aggregate_context("range_agg_transfn"));
    };
    let flinfo = flinfo.expect("range_agg_transfn: NULL flinfo");
    let rngtypoid = ::funcapi::get_fn_expr_argtype(Some(flinfo), 1);
    if !::lsyscache::type_is_range(rngtypoid)? {
        return Err(Box::new(PgError::error("range_agg must be called with a range")));
    }
    let stp = agg_state_slot(fcinfo, aggmcx, rngtypoid)?;
    if !fcinfo.argisnull(1) {
        // SAFETY: stp is the aggcontext-owned state; plain-data move in/out.
        unsafe {
            let st = stp.read();
            let st =
                ::arrayfuncs::accum_array_result(aggmcx, Some(st), fcinfo.arg(1), false, rngtypoid)?;
            stp.write(st);
        }
    }
    Ok(Datum::from_usize(stp as usize))
}

pub fn fc_range_agg_finalfn(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: context, if set, is the executor's live AggStateNode.
    if unsafe { fcinfo.agg_context() }.is_none() {
        return Err(non_aggregate_context("range_agg_finalfn"));
    }
    if fcinfo.argisnull(0) {
        return Ok(fcinfo.return_null());
    }
    let stp = fcinfo.arg(0).as_usize() as *const ::datum::array_build::ArrayBuildState<'_>;
    // SAFETY: transvalue points at the aggcontext-owned build state.
    let st = unsafe { &*stp };
    if st.nelems == 0 {
        return Ok(fcinfo.return_null());
    }
    let flinfo = flinfo.expect("range_agg_finalfn: NULL flinfo");
    // C reads the rettype off the faked finalfn fn_expr; the collected
    // element type's pg_range row names the same concrete multirange.
    let mltrngtypid = syscache_seams::lookup_pg_range_shape::call(st.element_type)?
        .unwrap_or_else(|| panic!("cache lookup failed for range type {}", st.element_type))
        .rngmultitypid;
    let mcx = fcinfo.result_mcx();
    let mi = cached_multirange_info(flinfo, mltrngtypid)?;
    let mut ranges: PgVec<'_, &[u8]> = ::mcx::vec_with_capacity_in(mcx, st.nelems as usize)?;
    for d in st.dvalues[..st.nelems as usize].iter() {
        let rp = d.as_usize() as *const u8;
        // Accumulated datums can be short-form; expand to the 4-byte form
        // (C DatumGetRangeTypeP).
        // SAFETY: live varlena copied into the agg state by accumArrayResult.
        let expanded = if unsafe { *rp } & 0x03 != 0 {
            let n = unsafe { ::types_tuple::varatt::varsize_any(rp) };
            // SAFETY: live varlena of n bytes.
            let raw = unsafe { core::slice::from_raw_parts(rp, n) };
            leak_image(::detoast_seams::detoast_attr::call(mcx, raw)?)
        } else {
            let n = ::adt_rangetypes::varsize_4b(rp);
            // SAFETY: live varlena of n bytes.
            unsafe { core::slice::from_raw_parts(rp, n) }
        };
        ranges.push(expanded);
    }
    let img = make_multirange(mcx, mltrngtypid, &mut mi.rng, &mut ranges)?;
    mr_result(fcinfo, &img)
}

pub fn fc_multirange_agg_transfn(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: context, if set, is the executor's live AggStateNode.
    let Some(aggmcx) = (unsafe { fcinfo.agg_context() }) else {
        return Err(non_aggregate_context("multirange_agg_transfn"));
    };
    let flinfo = flinfo.expect("multirange_agg_transfn: NULL flinfo");
    let mltrngtypoid = ::funcapi::get_fn_expr_argtype(Some(flinfo), 1);
    if !::lsyscache::type_is_multirange(mltrngtypoid)? {
        return Err(Box::new(PgError::error("range_agg must be called with a multirange")));
    }
    let mi = cached_multirange_info(flinfo, mltrngtypoid)?;
    let rngtypoid = mi.rng.rngtypid;
    let stp = agg_state_slot(fcinfo, aggmcx, rngtypoid)?;
    if !fcinfo.argisnull(1) {
        let mcx = fcinfo.result_mcx();
        let mr = arg_multirange(fcinfo, 1, mcx)?;
        let mut accum = |d: Datum| -> PgResult<()> {
            // SAFETY: stp is the aggcontext-owned state; plain-data move in/out.
            unsafe {
                let st = stp.read();
                let st =
                    ::arrayfuncs::accum_array_result(aggmcx, Some(st), d, false, rngtypoid)?;
                stp.write(st);
            }
            Ok(())
        };
        if multirange_is_empty(&mr) {
            // C adds an empty range so the result is empty, not null.
            let empty = ::adt_rangetypes::make_empty_range(mcx, &mut mi.rng)?;
            accum(Datum::from_usize(empty.as_ptr() as usize))?;
        } else {
            let ranges = multirange_deserialize(mcx, &mi.rng, &mr)?;
            for r in ranges.iter() {
                accum(Datum::from_usize(r.as_ptr() as usize))?;
            }
        }
    }
    Ok(Datum::from_usize(stp as usize))
}

pub fn fc_multirange_lower(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let out = {
        let mcx = fcinfo.result_mcx();
        let mr = arg_multirange(fcinfo, 0, mcx)?;
        if multirange_is_empty(&mr) {
            None
        } else {
            let mi = flinfo_mi(flinfo, multirange_type_oid(&mr))?;
            let (lower, _upper) = multirange_get_bounds(&mi.rng, &mr, 0);
            if !lower.infinite {
                Some(bound_datum_result(fcinfo, mi, lower.val)?)
            } else {
                None
            }
        }
    };
    Ok(out.unwrap_or_else(|| fcinfo.return_null()))
}

pub fn fc_multirange_upper(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let out = {
        let mcx = fcinfo.result_mcx();
        let mr = arg_multirange(fcinfo, 0, mcx)?;
        if multirange_is_empty(&mr) {
            None
        } else {
            let mi = flinfo_mi(flinfo, multirange_type_oid(&mr))?;
            let (_lower, upper) =
                multirange_get_bounds(&mi.rng, &mr, multirange_count(&mr) as usize - 1);
            if !upper.infinite {
                Some(bound_datum_result(fcinfo, mi, upper.val)?)
            } else {
                None
            }
        }
    };
    Ok(out.unwrap_or_else(|| fcinfo.return_null()))
}

fn bound_datum_result(fcinfo: &Fcinfo, mi: &MultirangeInfo, val: Datum) -> PgResult<Datum> {
    if mi.rng.elem.typbyval {
        return Ok(val);
    }
    let p = val.as_usize() as *const u8;
    let n = if mi.rng.elem.typlen == -1 {
        // SAFETY: a byref bound datum is a live varlena inside the image.
        unsafe { ::types_tuple::varatt::varsize_any(p) }
    } else {
        mi.rng.elem.typlen as usize
    };
    // SAFETY: live bound value of n bytes inside the argument image.
    byref_result(fcinfo.result_mcx(), unsafe { core::slice::from_raw_parts(p, n) })
}

pub fn fc_multirange_empty(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mr = arg_multirange(fcinfo, 0, mcx)?;
    Ok(Datum::from_bool(multirange_is_empty(&mr)))
}

macro_rules! fc_mr_bound_flag {
    ($($fc:ident: $first:expr, $field:ident;)*) => {$(
        pub fn $fc(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let mcx = fcinfo.result_mcx();
            let mr = arg_multirange(fcinfo, 0, mcx)?;
            if multirange_is_empty(&mr) {
                return Ok(Datum::from_bool(false));
            }
            let mi = flinfo_mi(flinfo, multirange_type_oid(&mr))?;
            let i = if $first { 0 } else { multirange_count(&mr) as usize - 1 };
            let (lower, upper) = multirange_get_bounds(&mi.rng, &mr, i);
            let b = if $first { lower } else { upper };
            Ok(Datum::from_bool(b.$field))
        }
    )*};
}

fc_mr_bound_flag! {
    fc_multirange_lower_inc: true, inclusive;
    fc_multirange_upper_inc: false, inclusive;
    fc_multirange_lower_inf: true, infinite;
    fc_multirange_upper_inf: false, infinite;
}

pub fn fc_multirange_contains_elem(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mr = arg_multirange(fcinfo, 0, mcx)?;
    let val = fcinfo.arg(1);
    let mi = flinfo_mi(flinfo, multirange_type_oid(&mr))?;
    Ok(Datum::from_bool(crate::multirange_contains_elem_internal(mcx, &mut mi.rng, &mr, val)?))
}

pub fn fc_elem_contained_by_multirange(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let val = fcinfo.arg(0);
    let mr = arg_multirange(fcinfo, 1, mcx)?;
    let mi = flinfo_mi(flinfo, multirange_type_oid(&mr))?;
    Ok(Datum::from_bool(crate::multirange_contains_elem_internal(mcx, &mut mi.rng, &mr, val)?))
}

// (multirange, range) and (range, multirange) argument shapes.
macro_rules! fc_mr_r {
    ($($fc:ident: $f:path, mr_first: $mr_first:expr;)*) => {$(
        pub fn $fc(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let mcx = fcinfo.result_mcx();
            let (mr_idx, r_idx) = if $mr_first { (0, 1) } else { (1, 0) };
            let mr = arg_multirange(fcinfo, mr_idx, mcx)?;
            let r = arg_range(fcinfo, r_idx, mcx)?;
            let mi = flinfo_mi(flinfo, multirange_type_oid(&mr))?;
            Ok(Datum::from_bool($f(mcx, &mut mi.rng, &r, &mr)?))
        }
    )*};
}

fc_mr_r! {
    fc_multirange_contains_range: multirange_contains_range_flip, mr_first: true;
    fc_range_contains_multirange: crate::range_contains_multirange_internal, mr_first: false;
    fc_range_contained_by_multirange: multirange_contains_range_flip, mr_first: false;
    fc_multirange_contained_by_range: crate::range_contains_multirange_internal, mr_first: true;
    fc_range_overlaps_multirange: crate::range_overlaps_multirange_internal, mr_first: false;
    fc_multirange_overlaps_range: crate::range_overlaps_multirange_internal, mr_first: true;
    fc_range_overleft_multirange: crate::range_overleft_multirange_internal, mr_first: false;
    fc_range_overright_multirange: crate::range_overright_multirange_internal, mr_first: false;
    fc_range_before_multirange: crate::range_before_multirange_internal, mr_first: false;
    fc_multirange_after_range: crate::range_before_multirange_internal, mr_first: true;
    fc_range_after_multirange: crate::range_after_multirange_internal, mr_first: false;
    fc_multirange_before_range: crate::range_after_multirange_internal, mr_first: true;
}

fn multirange_contains_range_flip(
    mcx: Mcx<'_>,
    rng: &mut ::adt_rangetypes::RangeInfo,
    r: &[u8],
    mr: &[u8],
) -> PgResult<bool> {
    crate::multirange_contains_range_internal(mcx, rng, mr, r)
}

pub fn fc_multirange_overleft_range(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mr = arg_multirange(fcinfo, 0, mcx)?;
    let r = arg_range(fcinfo, 1, mcx)?;
    if multirange_is_empty(&mr) || range_is_empty(&r) {
        return Ok(Datum::from_bool(false));
    }
    let mi = flinfo_mi(flinfo, multirange_type_oid(&mr))?;
    let rng = &mut mi.rng;
    let (_l1, upper1) = multirange_get_bounds(rng, &mr, multirange_count(&mr) as usize - 1);
    let (_l2, upper2, _e) = ::adt_rangetypes::range_deserialize(&rng.elem, &r);
    Ok(Datum::from_bool(::adt_rangetypes::range_cmp_bounds(mcx, rng, &upper1, &upper2)? <= 0))
}

pub fn fc_multirange_overright_range(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mr = arg_multirange(fcinfo, 0, mcx)?;
    let r = arg_range(fcinfo, 1, mcx)?;
    if multirange_is_empty(&mr) || range_is_empty(&r) {
        return Ok(Datum::from_bool(false));
    }
    let mi = flinfo_mi(flinfo, multirange_type_oid(&mr))?;
    let rng = &mut mi.rng;
    let (lower1, _u1) = multirange_get_bounds(rng, &mr, 0);
    let (lower2, _u2, _e) = ::adt_rangetypes::range_deserialize(&rng.elem, &r);
    Ok(Datum::from_bool(::adt_rangetypes::range_cmp_bounds(mcx, rng, &lower1, &lower2)? >= 0))
}

macro_rules! fc_mr_mr_bool {
    ($($fc:ident: $f:path $(, swap: $swap:expr)?;)*) => {$(
        pub fn $fc(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let mcx = fcinfo.result_mcx();
            let mr1 = arg_multirange(fcinfo, 0, mcx)?;
            let mr2 = arg_multirange(fcinfo, 1, mcx)?;
            let mi = flinfo_mi(flinfo, multirange_type_oid(&mr1))?;
            #[allow(unused_mut, unused_assignments)]
            let mut swapped = false;
            $(swapped = $swap;)?
            let (a, b): (&[u8], &[u8]) =
                if swapped { (&mr2, &mr1) } else { (&mr1, &mr2) };
            Ok(Datum::from_bool($f(mcx, &mut mi.rng, a, b)?))
        }
    )*};
}

fc_mr_mr_bool! {
    fc_multirange_eq: crate::multirange_eq_internal;
    fc_multirange_ne: multirange_ne_internal;
    fc_multirange_overlaps_multirange: crate::multirange_overlaps_multirange_internal;
    fc_multirange_contains_multirange: crate::multirange_contains_multirange_internal;
    fc_multirange_contained_by_multirange: crate::multirange_contains_multirange_internal, swap: true;
    fc_multirange_before_multirange: crate::multirange_before_multirange_internal;
    fc_multirange_after_multirange: crate::multirange_before_multirange_internal, swap: true;
}

fn multirange_ne_internal(
    mcx: Mcx<'_>,
    rng: &mut ::adt_rangetypes::RangeInfo,
    mr1: &[u8],
    mr2: &[u8],
) -> PgResult<bool> {
    Ok(!crate::multirange_eq_internal(mcx, rng, mr1, mr2)?)
}

pub fn fc_multirange_overleft_multirange(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mr1 = arg_multirange(fcinfo, 0, mcx)?;
    let mr2 = arg_multirange(fcinfo, 1, mcx)?;
    if multirange_is_empty(&mr1) || multirange_is_empty(&mr2) {
        return Ok(Datum::from_bool(false));
    }
    let mi = flinfo_mi(flinfo, multirange_type_oid(&mr1))?;
    let rng = &mut mi.rng;
    let (_l1, upper1) = multirange_get_bounds(rng, &mr1, multirange_count(&mr1) as usize - 1);
    let (_l2, upper2) = multirange_get_bounds(rng, &mr2, multirange_count(&mr2) as usize - 1);
    Ok(Datum::from_bool(::adt_rangetypes::range_cmp_bounds(mcx, rng, &upper1, &upper2)? <= 0))
}

pub fn fc_multirange_overright_multirange(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mr1 = arg_multirange(fcinfo, 0, mcx)?;
    let mr2 = arg_multirange(fcinfo, 1, mcx)?;
    if multirange_is_empty(&mr1) || multirange_is_empty(&mr2) {
        return Ok(Datum::from_bool(false));
    }
    let mi = flinfo_mi(flinfo, multirange_type_oid(&mr1))?;
    let rng = &mut mi.rng;
    let (lower1, _u1) = multirange_get_bounds(rng, &mr1, 0);
    let (lower2, _u2) = multirange_get_bounds(rng, &mr2, 0);
    Ok(Datum::from_bool(::adt_rangetypes::range_cmp_bounds(mcx, rng, &lower1, &lower2)? >= 0))
}

pub fn fc_range_adjacent_multirange(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let r = arg_range(fcinfo, 0, mcx)?;
    let mr = arg_multirange(fcinfo, 1, mcx)?;
    let mi = flinfo_mi(flinfo, multirange_type_oid(&mr))?;
    Ok(Datum::from_bool(crate::range_adjacent_multirange_internal(mcx, &mut mi.rng, &r, &mr)?))
}

pub fn fc_multirange_adjacent_range(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mr = arg_multirange(fcinfo, 0, mcx)?;
    let r = arg_range(fcinfo, 1, mcx)?;
    if range_is_empty(&r) || multirange_is_empty(&mr) {
        return Ok(Datum::from_bool(false));
    }
    let mi = flinfo_mi(flinfo, multirange_type_oid(&mr))?;
    Ok(Datum::from_bool(crate::range_adjacent_multirange_internal(mcx, &mut mi.rng, &r, &mr)?))
}

pub fn fc_multirange_adjacent_multirange(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mr1 = arg_multirange(fcinfo, 0, mcx)?;
    let mr2 = arg_multirange(fcinfo, 1, mcx)?;
    if multirange_is_empty(&mr1) || multirange_is_empty(&mr2) {
        return Ok(Datum::from_bool(false));
    }
    let mi = flinfo_mi(flinfo, multirange_type_oid(&mr1))?;
    let rng = &mut mi.rng;
    let range_count1 = multirange_count(&mr1) as usize;
    let range_count2 = multirange_count(&mr2) as usize;
    let (mut lower1, mut upper1) = multirange_get_bounds(rng, &mr1, range_count1 - 1);
    let (mut lower2, mut upper2) = multirange_get_bounds(rng, &mr2, 0);
    if ::adt_rangetypes::ops::bounds_adjacent(mcx, rng, upper1, lower2)? {
        return Ok(Datum::from_bool(true));
    }
    if range_count1 > 1 {
        (lower1, upper1) = multirange_get_bounds(rng, &mr1, 0);
    }
    if range_count2 > 1 {
        (lower2, upper2) = multirange_get_bounds(rng, &mr2, range_count2 - 1);
    }
    let _ = (lower2, upper1);
    if ::adt_rangetypes::ops::bounds_adjacent(mcx, rng, upper2, lower1)? {
        return Ok(Datum::from_bool(true));
    }
    Ok(Datum::from_bool(false))
}

pub fn fc_multirange_cmp(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mr1 = arg_multirange(fcinfo, 0, mcx)?;
    let mr2 = arg_multirange(fcinfo, 1, mcx)?;
    let mi = flinfo_mi(flinfo, multirange_type_oid(&mr1))?;
    Ok(Datum::from_i32(crate::multirange_cmp_internal(mcx, &mut mi.rng, &mr1, &mr2)?))
}

macro_rules! fc_mr_cmp_op {
    ($($fc:ident: $op:tt;)*) => {$(
        pub fn $fc(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let mcx = fcinfo.result_mcx();
            let mr1 = arg_multirange(fcinfo, 0, mcx)?;
            let mr2 = arg_multirange(fcinfo, 1, mcx)?;
            let mi = flinfo_mi(flinfo, multirange_type_oid(&mr1))?;
            Ok(Datum::from_bool(crate::multirange_cmp_internal(mcx, &mut mi.rng, &mr1, &mr2)? $op 0))
        }
    )*};
}

fc_mr_cmp_op! {
    fc_multirange_lt: <;
    fc_multirange_le: <=;
    fc_multirange_ge: >=;
    fc_multirange_gt: >;
}

pub fn fc_range_merge_from_multirange(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mr = arg_multirange(fcinfo, 0, mcx)?;
    let mi = flinfo_mi(flinfo, multirange_type_oid(&mr))?;
    let img = if multirange_is_empty(&mr) {
        ::adt_rangetypes::make_empty_range(mcx, &mut mi.rng)?
    } else if multirange_count(&mr) == 1 {
        multirange_get_range(mcx, &mi.rng, &mr, 0)?
    } else {
        crate::multirange_get_union_range(mcx, &mut mi.rng, &mr)?
    };
    mr_result(fcinfo, &img)
}

struct UnnestState {
    mr: Vec<u8>,
    rng: ::adt_rangetypes::RangeInfo,
    index: usize,
}

pub fn fc_multirange_unnest(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("multirange_unnest: NULL flinfo");
    if !flinfo.has_fn_extra() {
        // The SRF state owns a copy of the image (C detoasts into the
        // multi-call context); Vec is the user_fctx Box's own allocation.
        let state = {
            let mcx = fcinfo.result_mcx();
            let mr = arg_multirange(fcinfo, 0, mcx)?;
            let mi = MultirangeInfo::lookup(multirange_type_oid(&mr))?;
            UnnestState { mr: mr.to_vec(), rng: mi.rng, index: 0 }
        };
        let fctx = ::funcapi::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(Box::new(state));
    }
    let mcx = fcinfo.result_mcx();
    let state = ::funcapi::per_MultiFuncCall(flinfo)
        .user_fctx
        .as_mut()
        .expect("multirange_unnest: user_fctx set at first call")
        .downcast_mut::<UnnestState>()
        .expect("multirange_unnest: user_fctx is UnnestState");
    if state.index < multirange_count(&state.mr) as usize {
        let d = {
            let img = multirange_get_range(mcx, &state.rng, &state.mr, state.index)?;
            state.index += 1;
            byref_result(mcx, &img)?
        };
        Ok(::funcapi::srf_return_next(flinfo, fcinfo, d))
    } else {
        Ok(::funcapi::srf_return_done(flinfo, fcinfo))
    }
}

pub fn fc_hash_multirange(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mr = arg_multirange(fcinfo, 0, mcx)?;
    let mi = flinfo_mi(flinfo, multirange_type_oid(&mr))?;
    Ok(Datum::from_u32(crate::hash_multirange_internal(mcx, mi, &mr)?))
}

pub fn fc_hash_multirange_extended(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mr = arg_multirange(fcinfo, 0, mcx)?;
    let seed = fcinfo.arg(1);
    let mi = flinfo_mi(flinfo, multirange_type_oid(&mr))?;
    Ok(Datum::from_u64(crate::hash_multirange_extended_internal(mcx, mi, &mr, seed)?))
}

const fn b(
    foid: Oid,
    name: &'static str,
    nargs: i16,
    strict: bool,
    retset: bool,
    func: PGFunction,
) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict, retset, func }
}

// pg_proc.dat rows for multirangetypes.c, OID-ascending.
pub const MULTIRANGETYPES_BUILTINS: &[FmgrBuiltin] = &[
    b(1293, "unnest", 1, true, true, fc_multirange_unnest),
    b(4228, "range_merge", 1, true, false, fc_range_merge_from_multirange),
    b(4231, "multirange_in", 3, true, false, fc_multirange_in),
    b(4232, "multirange_out", 1, true, false, fc_multirange_out),
    // anymultirange_out (pseudotypes.c) is `return multirange_out(fcinfo)`.
    b(4230, "anymultirange_out", 1, true, false, fc_multirange_out),
    b(4233, "multirange_recv", 3, true, false, fc_multirange_recv),
    b(4234, "multirange_send", 1, true, false, fc_multirange_send),
    b(4235, "lower", 1, true, false, fc_multirange_lower),
    b(4236, "upper", 1, true, false, fc_multirange_upper),
    b(4237, "isempty", 1, true, false, fc_multirange_empty),
    b(4238, "lower_inc", 1, true, false, fc_multirange_lower_inc),
    b(4239, "upper_inc", 1, true, false, fc_multirange_upper_inc),
    b(4240, "lower_inf", 1, true, false, fc_multirange_lower_inf),
    b(4241, "upper_inf", 1, true, false, fc_multirange_upper_inf),
    b(4244, "multirange_eq", 2, true, false, fc_multirange_eq),
    b(4245, "multirange_ne", 2, true, false, fc_multirange_ne),
    b(4246, "range_overlaps_multirange", 2, true, false, fc_range_overlaps_multirange),
    b(4247, "multirange_overlaps_range", 2, true, false, fc_multirange_overlaps_range),
    b(4248, "multirange_overlaps_multirange", 2, true, false, fc_multirange_overlaps_multirange),
    b(4249, "multirange_contains_elem", 2, true, false, fc_multirange_contains_elem),
    b(4250, "multirange_contains_range", 2, true, false, fc_multirange_contains_range),
    b(4251, "multirange_contains_multirange", 2, true, false, fc_multirange_contains_multirange),
    b(4252, "elem_contained_by_multirange", 2, true, false, fc_elem_contained_by_multirange),
    b(4253, "range_contained_by_multirange", 2, true, false, fc_range_contained_by_multirange),
    b(
        4254,
        "multirange_contained_by_multirange",
        2,
        true,
        false,
        fc_multirange_contained_by_multirange,
    ),
    b(4255, "range_adjacent_multirange", 2, true, false, fc_range_adjacent_multirange),
    b(4256, "multirange_adjacent_multirange", 2, true, false, fc_multirange_adjacent_multirange),
    b(4257, "multirange_adjacent_range", 2, true, false, fc_multirange_adjacent_range),
    b(4258, "range_before_multirange", 2, true, false, fc_range_before_multirange),
    b(4259, "multirange_before_range", 2, true, false, fc_multirange_before_range),
    b(4260, "multirange_before_multirange", 2, true, false, fc_multirange_before_multirange),
    b(4261, "range_after_multirange", 2, true, false, fc_range_after_multirange),
    b(4262, "multirange_after_range", 2, true, false, fc_multirange_after_range),
    b(4263, "multirange_after_multirange", 2, true, false, fc_multirange_after_multirange),
    b(4264, "range_overleft_multirange", 2, true, false, fc_range_overleft_multirange),
    b(4265, "multirange_overleft_range", 2, true, false, fc_multirange_overleft_range),
    b(4266, "multirange_overleft_multirange", 2, true, false, fc_multirange_overleft_multirange),
    b(4267, "range_overright_multirange", 2, true, false, fc_range_overright_multirange),
    b(4268, "multirange_overright_range", 2, true, false, fc_multirange_overright_range),
    b(
        4269,
        "multirange_overright_multirange",
        2,
        true,
        false,
        fc_multirange_overright_multirange,
    ),
    b(4270, "multirange_union", 2, true, false, fc_multirange_union),
    b(4271, "multirange_minus", 2, true, false, fc_multirange_minus),
    b(4272, "multirange_intersect", 2, true, false, fc_multirange_intersect),
    b(4273, "multirange_cmp", 2, true, false, fc_multirange_cmp),
    b(4274, "multirange_lt", 2, true, false, fc_multirange_lt),
    b(4275, "multirange_le", 2, true, false, fc_multirange_le),
    b(4276, "multirange_ge", 2, true, false, fc_multirange_ge),
    b(4277, "multirange_gt", 2, true, false, fc_multirange_gt),
    b(4278, "hash_multirange", 1, true, false, fc_hash_multirange),
    b(4279, "hash_multirange_extended", 2, true, false, fc_hash_multirange_extended),
    b(4280, "int4multirange", 0, true, false, fc_multirange_constructor0),
    b(4281, "int4multirange", 1, true, false, fc_multirange_constructor1),
    b(4282, "int4multirange", 1, true, false, fc_multirange_constructor2),
    b(4283, "nummultirange", 0, true, false, fc_multirange_constructor0),
    b(4284, "nummultirange", 1, true, false, fc_multirange_constructor1),
    b(4285, "nummultirange", 1, true, false, fc_multirange_constructor2),
    b(4286, "tsmultirange", 0, true, false, fc_multirange_constructor0),
    b(4287, "tsmultirange", 1, true, false, fc_multirange_constructor1),
    b(4288, "tsmultirange", 1, true, false, fc_multirange_constructor2),
    b(4289, "tstzmultirange", 0, true, false, fc_multirange_constructor0),
    b(4290, "tstzmultirange", 1, true, false, fc_multirange_constructor1),
    b(4291, "tstzmultirange", 1, true, false, fc_multirange_constructor2),
    b(4292, "datemultirange", 0, true, false, fc_multirange_constructor0),
    b(4293, "datemultirange", 1, true, false, fc_multirange_constructor1),
    b(4294, "datemultirange", 1, true, false, fc_multirange_constructor2),
    b(4295, "int8multirange", 0, true, false, fc_multirange_constructor0),
    b(4296, "int8multirange", 1, true, false, fc_multirange_constructor1),
    b(4297, "int8multirange", 1, true, false, fc_multirange_constructor2),
    b(4298, "multirange", 1, true, false, fc_multirange_constructor1),
    b(4299, "range_agg_transfn", 2, false, false, fc_range_agg_transfn),
    b(4300, "range_agg_finalfn", 2, false, false, fc_range_agg_finalfn),
    b(4388, "multirange_intersect_agg_transfn", 2, true, false, fc_multirange_intersect_agg_transfn),
    b(4541, "range_contains_multirange", 2, true, false, fc_range_contains_multirange),
    b(4542, "multirange_contained_by_range", 2, true, false, fc_multirange_contained_by_range),
    b(6225, "multirange_agg_transfn", 2, false, false, fc_multirange_agg_transfn),
    b(6226, "multirange_agg_finalfn", 2, false, false, fc_range_agg_finalfn),
];
