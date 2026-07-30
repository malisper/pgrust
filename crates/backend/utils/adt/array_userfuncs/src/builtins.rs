use ::datum::Datum;
use ::mcx::{Mcx, PgVec};
use ::typcache::{TYPECACHE_EQ_OPR_FINFO, TYPECACHE_GT_OPR, TYPECACHE_LT_OPR};
use ::types_core::Oid;
use ::types_error::{
    PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_NULL_VALUE_NOT_ALLOWED, ERRCODE_UNDEFINED_FUNCTION,
    ERRCODE_UNDEFINED_OBJECT,
};
use ::types_fmgr::{
    byref_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

use crate::{
    accum_array_result_arr, array_append_internal, array_cat_internal, array_position_internal,
    array_positions_internal, array_prepend_internal, array_reverse_n, array_shuffle_n,
    array_sort_with, init_array_result_arr, make_array_result_arr, trim_array_internal, ElemMeta,
    PositionSearch, ARRAY_GT_OP, ARRAY_LT_OP, F_BTARRAYCMP,
};
use ::arrayfuncs::foundation::{arr_elemtype, read_dims_lbounds, varsize_any};
use ::datum::array_build::ArrayBuildStateArr;

fn arg_array_bytes<'m>(fcinfo: &Fcinfo, i: usize, mcx: Mcx<'m>) -> PgResult<PgVec<'m, u8>> {
    // SAFETY: arg i is a non-null array (varlena) datum, checked by the caller.
    let p = unsafe { fcinfo.arg_ptr(i) };
    let total = varsize_any(p);
    // SAFETY: a live varlena of `total` bytes.
    let raw = unsafe { core::slice::from_raw_parts(p, total) };
    ::detoast_seams::detoast_attr::call(mcx, raw)
}

fn elem_meta(element_type: Oid) -> PgResult<ElemMeta> {
    let (typlen, typbyval, typalign) = ::lsyscache::get_typlenbyvalalign(element_type)?;
    Ok(ElemMeta { element_type, typlen: typlen as i32, typbyval, typalign: typalign as u8 })
}

fn cached_elem_meta(flinfo: &mut FmgrInfo, element_type: Oid) -> PgResult<ElemMeta> {
    let need = match flinfo.fn_extra_ref::<ElemMeta>() {
        Some(m) => m.element_type != element_type,
        None => true,
    };
    if need {
        flinfo.set_fn_extra(elem_meta(element_type)?);
    }
    Ok(*flinfo.fn_extra_ref::<ElemMeta>().unwrap())
}

#[track_caller]
#[cold]
fn could_not_determine_input_type() -> Box<PgError> {
    Box::new(
        PgError::error("could not determine input data type")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

#[track_caller]
#[cold]
fn multidim_search_unsupported() -> Box<PgError> {
    Box::new(
        PgError::error("searching for elements in multidimensional arrays is not supported")
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

fn append_prepend_common(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    is_append: bool,
) -> PgResult<Datum> {
    let (arr_i, elem_i) = if is_append { (0, 1) } else { (1, 0) };
    let flinfo = flinfo.expect("array_append/prepend: NULL flinfo");
    // SAFETY: fcinfo.context, if an agg node, is the executor's live state.
    let mcx = match unsafe { fcinfo.agg_context() } {
        Some(m) => m,
        None => fcinfo.result_mcx(),
    };

    let (array, meta) = if !fcinfo.argisnull(arr_i) {
        let img = arg_array_bytes(fcinfo, arr_i, mcx)?;
        let meta = cached_elem_meta(flinfo, arr_elemtype(&img))?;
        (img, meta)
    } else {
        let arr_typeid = ::fmgr_seams::get_fn_expr_argtype::call(flinfo, arr_i as i16);
        if arr_typeid == 0 {
            return Err(could_not_determine_input_type());
        }
        let element_type = ::lsyscache::get_element_type(arr_typeid)?;
        if element_type == 0 {
            return Err(Box::new(
                PgError::error("input data type is not an array")
                    .with_sqlstate(ERRCODE_DATATYPE_MISMATCH),
            ));
        }
        let meta = cached_elem_meta(flinfo, element_type)?;
        (::arrayfuncs::construct::construct_empty_array(mcx, element_type)?, meta)
    };

    let elem_null = fcinfo.argisnull(elem_i);
    let _elem_copy;
    let elem = if elem_null {
        Datum::null()
    } else if meta.typlen == -1 {
        // C array_set_element detoasts a varlena replacement value.
        let img = arg_array_bytes(fcinfo, elem_i, mcx)?;
        let d = Datum::from_usize(img.as_ptr() as usize);
        _elem_copy = img;
        d
    } else {
        fcinfo.arg(elem_i)
    };

    let out = if is_append {
        array_append_internal(mcx, &array, elem, elem_null, &meta)?
    } else {
        array_prepend_internal(mcx, &array, elem, elem_null, &meta)?
    };
    byref_result(mcx, &out)
}

pub fn fc_array_append(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    append_prepend_common(flinfo, fcinfo, true)
}

pub fn fc_array_prepend(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    append_prepend_common(flinfo, fcinfo, false)
}

pub fn fc_array_append_support(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SupportRequestModifyInPlace is not in the support-node vocabulary yet;
    // C's fallthrough for every other request is a NULL pointer.
    Ok(Datum::from_usize(0))
}

pub fn fc_array_prepend_support(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_usize(0))
}

pub fn fc_array_cat(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    if fcinfo.argisnull(0) {
        if fcinfo.argisnull(1) {
            return Ok(fcinfo.return_null());
        }
        let mcx = fcinfo.result_mcx();
        let img = arg_array_bytes(fcinfo, 1, mcx)?;
        return byref_result(mcx, &img);
    }
    if fcinfo.argisnull(1) {
        let mcx = fcinfo.result_mcx();
        let img = arg_array_bytes(fcinfo, 0, mcx)?;
        return byref_result(mcx, &img);
    }
    let mcx = fcinfo.result_mcx();
    let v1 = arg_array_bytes(fcinfo, 0, mcx)?;
    let v2 = arg_array_bytes(fcinfo, 1, mcx)?;
    let out = array_cat_internal(mcx, &v1, &v2)?;
    byref_result(mcx, &out)
}

struct CmpMemo(FmgrInfo);

pub fn fc_array_larger(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    larger_smaller(flinfo, fcinfo, true)
}

pub fn fc_array_smaller(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    larger_smaller(flinfo, fcinfo, false)
}

fn larger_smaller(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    larger: bool,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("array_larger/smaller: NULL flinfo");
    if !flinfo.has_fn_extra() {
        flinfo.set_fn_extra(CmpMemo(::fmgr_seams::fmgr_info::call(F_BTARRAYCMP)?));
    }
    let memo = flinfo.fn_extra_mut::<CmpMemo>().unwrap();
    let mcx = fcinfo.result_mcx();
    let r = ::types_fmgr::function_call2_coll_in(
        &mut memo.0,
        fcinfo.get_collation(),
        mcx,
        fcinfo.arg(0),
        fcinfo.arg(1),
    )?
    .as_i32();
    let pick_first = if larger { r > 0 } else { r < 0 };
    Ok(if pick_first { fcinfo.arg(0) } else { fcinfo.arg(1) })
}

struct PosMemo {
    meta: ElemMeta,
    proc: FmgrInfo,
}

fn cached_pos_memo<'f>(
    flinfo: &'f mut FmgrInfo,
    element_type: Oid,
) -> PgResult<&'f mut PosMemo> {
    let need = match flinfo.fn_extra_ref::<PosMemo>() {
        Some(m) => m.meta.element_type != element_type,
        None => true,
    };
    if need {
        let meta = elem_meta(element_type)?;
        let e = ::typcache::lookup_type_cache(element_type, TYPECACHE_EQ_OPR_FINFO)?;
        let proc = e.eq_opr_finfo().clone();
        if proc.fn_oid == 0 {
            return Err(Box::new(
                PgError::error(format!(
                    "could not identify an equality operator for type {}",
                    ::format_type::format_type_be(element_type)?
                ))
                .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION),
            ));
        }
        flinfo.set_fn_extra(PosMemo { meta, proc });
    }
    Ok(flinfo.fn_extra_mut::<PosMemo>().unwrap())
}

fn position_compute(
    flinfo: &mut FmgrInfo,
    fcinfo: &Fcinfo,
    has_start: bool,
) -> PgResult<Option<i32>> {
    let mcx = fcinfo.result_mcx();
    let array = arg_array_bytes(fcinfo, 0, mcx)?;
    let (ndim, _dims, _lbs) = read_dims_lbounds(&array);
    if ndim > 1 {
        return Err(multidim_search_unsupported());
    }
    if ndim < 1 {
        return Ok(None);
    }

    let null_search = fcinfo.argisnull(1);
    if null_search && !::arrayfuncs::array_contains_nulls(&array) {
        return Ok(None);
    }
    let searched = if null_search { Datum::null() } else { fcinfo.arg(1) };

    let position_min = if has_start {
        if fcinfo.argisnull(2) {
            return Err(Box::new(
                PgError::error("initial position must not be null")
                    .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED),
            ));
        }
        Some(fcinfo.arg_i32(2))
    } else {
        None
    };

    let memo = cached_pos_memo(flinfo, arr_elemtype(&array))?;
    let s = PositionSearch {
        searched,
        null_search,
        collation: fcinfo.get_collation(),
        position_min,
    };
    array_position_internal(mcx, &array, &s, &memo.meta.clone(), &mut memo.proc)
}

fn position_common(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    has_start: bool,
) -> PgResult<Datum> {
    if fcinfo.argisnull(0) {
        return Ok(fcinfo.return_null());
    }
    let flinfo = flinfo.expect("array_position: NULL flinfo");
    match position_compute(flinfo, fcinfo, has_start)? {
        Some(pos) => Ok(Datum::from_i32(pos)),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_array_position(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    position_common(flinfo, fcinfo, false)
}

pub fn fc_array_position_start(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    position_common(flinfo, fcinfo, true)
}

pub fn fc_array_positions(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    if fcinfo.argisnull(0) {
        return Ok(fcinfo.return_null());
    }
    let flinfo = flinfo.expect("array_positions: NULL flinfo");
    let mcx = fcinfo.result_mcx();
    let array = arg_array_bytes(fcinfo, 0, mcx)?;
    let (ndim, _dims, _lbs) = read_dims_lbounds(&array);
    if ndim > 1 {
        return Err(multidim_search_unsupported());
    }
    let null_search = fcinfo.argisnull(1);
    if ndim < 1 || (null_search && !::arrayfuncs::array_contains_nulls(&array)) {
        let empty = ::arrayfuncs::construct::construct_empty_array(mcx, 23)?;
        return byref_result(mcx, &empty);
    }
    let searched = if null_search { Datum::null() } else { fcinfo.arg(1) };
    let memo = cached_pos_memo(flinfo, arr_elemtype(&array))?;
    let s = PositionSearch {
        searched,
        null_search,
        collation: fcinfo.get_collation(),
        position_min: None,
    };
    let out = array_positions_internal(mcx, &array, &s, &memo.meta.clone(), &mut memo.proc)?;
    byref_result(mcx, &out)
}

pub fn fc_array_agg_array_transfn(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("array_agg_array_transfn: NULL flinfo");
    let arg1_typeid = ::fmgr_seams::get_fn_expr_argtype::call(flinfo, 1);
    if arg1_typeid == 0 {
        return Err(could_not_determine_input_type());
    }
    // SAFETY: fcinfo.context is the executor's live AggStateNode.
    let Some(aggmcx) = (unsafe { fcinfo.agg_context() }) else {
        panic!("array_agg_array_transfn called in non-aggregate context");
    };

    let stp: *mut ArrayBuildStateArr<'_> = if fcinfo.argisnull(0) {
        let st = init_array_result_arr(aggmcx, arg1_typeid, 0)?;
        let layout = core::alloc::Layout::new::<ArrayBuildStateArr<'_>>();
        let raw = ::mcx::Allocator::allocate(&aggmcx, layout)
            .map_err(|_| aggmcx.oom(layout.size()))?;
        let p: *mut ArrayBuildStateArr<'_> = raw.cast().as_ptr();
        // SAFETY: fresh aggcontext allocation of the exact layout; no drop
        // glue runs (PgVec fields are arena-plain).
        unsafe { p.write(st) };
        p
    } else {
        fcinfo.arg(0).as_usize() as *mut ArrayBuildStateArr<'_>
    };

    let arg_img = if fcinfo.argisnull(1) {
        None
    } else {
        Some(arg_array_bytes(fcinfo, 1, aggmcx)?)
    };
    // SAFETY: stp is the aggcontext-owned state; plain-data move in/out.
    unsafe {
        let st = stp.read();
        let st = accum_array_result_arr(aggmcx, Some(st), arg_img.as_deref(), arg1_typeid)?;
        stp.write(st);
    }
    Ok(Datum::from_usize(stp as usize))
}

fn alloc_state_arr<'m>(
    mcx: ::mcx::Mcx<'m>,
    st: ArrayBuildStateArr<'m>,
) -> PgResult<*mut ArrayBuildStateArr<'m>> {
    let layout = core::alloc::Layout::new::<ArrayBuildStateArr<'_>>();
    let raw =
        ::mcx::Allocator::allocate(&mcx, layout).map_err(|_| mcx.oom(layout.size()))?;
    let p: *mut ArrayBuildStateArr<'m> = raw.cast().as_ptr();
    // SAFETY: fresh allocation of the exact layout; no drop glue runs
    // (PgVec fields are arena-plain).
    unsafe { p.write(st) };
    Ok(p)
}

pub fn fc_array_agg_array_combine(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: fcinfo.context is the executor's live AggStateNode.
    let Some(aggmcx) = (unsafe { fcinfo.agg_context() }) else {
        panic!("aggregate function called in non-aggregate context");
    };
    let state1 = if fcinfo.argisnull(0) {
        None
    } else {
        Some(fcinfo.arg(0).as_usize() as *mut ArrayBuildStateArr<'_>)
    };
    let state2 = if fcinfo.argisnull(1) {
        None
    } else {
        Some(fcinfo.arg(1).as_usize() as *const ArrayBuildStateArr<'_>)
    };
    // SAFETY: state pointers address live aggregate-owned build states.
    match (state1, state2) {
        (None, None) => Ok(fcinfo.return_null()),
        (Some(p1), None) => Ok(Datum::from_usize(p1 as usize)),
        (None, Some(p2)) => {
            let st = crate::clone_array_build_state_arr(aggmcx, unsafe { &*p2 })?;
            Ok(Datum::from_usize(alloc_state_arr(aggmcx, st)? as usize))
        }
        (Some(p1), Some(p2)) => {
            let s2 = unsafe { &*p2 };
            if s2.nitems > 0 {
                unsafe { crate::combine_array_build_state_arr(&mut *p1, s2)? };
            }
            Ok(Datum::from_usize(p1 as usize))
        }
    }
}

pub fn fc_array_agg_array_serialize(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: fcinfo.context is the executor's live AggStateNode.
    debug_assert!(unsafe { fcinfo.agg_context() }.is_some());
    let stp = fcinfo.arg(0).as_usize() as *const ArrayBuildStateArr<'_>;
    // SAFETY: transvalue points at the aggcontext-owned build state.
    let st = unsafe { &*stp };
    let mcx = fcinfo.result_mcx();
    let out = crate::serialize_array_build_state_arr(mcx, st)?;
    Ok(::types_fmgr::varlena_result(out))
}

pub fn fc_array_agg_array_deserialize(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: fcinfo.context is the executor's live AggStateNode.
    debug_assert!(unsafe { fcinfo.agg_context() }.is_some());
    // SAFETY: strict fn — arg 0 is a non-null live bytea.
    let sstate = unsafe { fcinfo.arg_varlena_packed(0) }?;
    // SAFETY: the executor's per-input context outlives the returned state's
    // consumption by the immediately-following combine call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let st = crate::deserialize_array_build_state_arr(mcx, sstate.data())?;
    Ok(Datum::from_usize(alloc_state_arr(mcx, st)? as usize))
}

pub fn fc_array_agg_array_finalfn(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    if fcinfo.argisnull(0) {
        return Ok(fcinfo.return_null());
    }
    let stp = fcinfo.arg(0).as_usize() as *const ArrayBuildStateArr<'_>;
    // SAFETY: transvalue points at the aggcontext-owned build state.
    let st = unsafe { &*stp };
    let mcx = fcinfo.result_mcx();
    let out = make_array_result_arr(mcx, st)?;
    byref_result(mcx, &out)
}

pub fn fc_trim_array(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("trim_array: NULL flinfo");
    let mcx = fcinfo.result_mcx();
    let v = arg_array_bytes(fcinfo, 0, mcx)?;
    let n = fcinfo.arg_i32(1);
    let meta = cached_elem_meta(flinfo, arr_elemtype(&v))?;
    let out = trim_array_internal(mcx, &v, n, meta.typlen, meta.typalign)?;
    byref_result(mcx, &out)
}

pub fn fc_array_shuffle(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("array_shuffle: NULL flinfo");
    let mcx = fcinfo.result_mcx();
    let array = arg_array_bytes(fcinfo, 0, mcx)?;
    let (ndim, dims, _lbs) = read_dims_lbounds(&array);
    if ndim < 1 || dims[0] < 2 {
        return byref_result(mcx, &array);
    }
    let meta = cached_elem_meta(flinfo, arr_elemtype(&array))?;
    let out = array_shuffle_n(mcx, &array, dims[0], true, &meta)?;
    byref_result(mcx, &out)
}

pub fn fc_array_sample(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("array_sample: NULL flinfo");
    let mcx = fcinfo.result_mcx();
    let array = arg_array_bytes(fcinfo, 0, mcx)?;
    let n = fcinfo.arg_i32(1);
    let (ndim, dims, _lbs) = read_dims_lbounds(&array);
    let nitem = if ndim < 1 { 0 } else { dims[0] };
    if n < 0 || n > nitem {
        return Err(Box::new(
            PgError::error(format!("sample size must be between 0 and {nitem}"))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    let meta = cached_elem_meta(flinfo, arr_elemtype(&array))?;
    let out = array_shuffle_n(mcx, &array, n, false, &meta)?;
    byref_result(mcx, &out)
}

pub fn fc_array_reverse(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("array_reverse: NULL flinfo");
    let mcx = fcinfo.result_mcx();
    let array = arg_array_bytes(fcinfo, 0, mcx)?;
    let (ndim, dims, _lbs) = read_dims_lbounds(&array);
    if ndim < 1 || dims[0] < 2 {
        return byref_result(mcx, &array);
    }
    let meta = cached_elem_meta(flinfo, arr_elemtype(&array))?;
    let out = array_reverse_n(mcx, &array, &meta)?;
    byref_result(mcx, &out)
}

struct SortMemo {
    meta: ElemMeta,
    lt: Oid,
    gt: Oid,
    typarray: Oid,
}

fn sort_common(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    descending: bool,
    nulls_first: bool,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("array_sort: NULL flinfo");
    let mcx = fcinfo.result_mcx();
    let array = arg_array_bytes(fcinfo, 0, mcx)?;
    let (ndim, dims, _lbs) = read_dims_lbounds(&array);
    if ndim < 1 || dims[0] < 2 {
        return byref_result(mcx, &array);
    }
    let elmtyp = arr_elemtype(&array);

    let need = match flinfo.fn_extra_ref::<SortMemo>() {
        Some(m) => m.meta.element_type != elmtyp,
        None => true,
    };
    if need {
        let e = ::typcache::lookup_type_cache(elmtyp, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR)?;
        let memo = SortMemo {
            meta: ElemMeta {
                element_type: elmtyp,
                typlen: e.typlen() as i32,
                typbyval: e.typbyval(),
                typalign: e.typalign() as u8,
            },
            lt: e.lt_opr(),
            gt: e.gt_opr(),
            typarray: e.typarray(),
        };
        flinfo.set_fn_extra(memo);
    }
    let m = flinfo.fn_extra_ref::<SortMemo>().unwrap();
    let (meta, lt, gt, typarray) = (m.meta, m.lt, m.gt, m.typarray);

    let (sort_typ, sort_opr, sub) = if ndim == 1 {
        (elmtyp, if descending { gt } else { lt }, None)
    } else {
        if typarray == 0 {
            return Err(Box::new(
                PgError::error(format!(
                    "could not find array type for data type {}",
                    ::format_type::format_type_be(elmtyp)?
                ))
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
            ));
        }
        (typarray, if descending { ARRAY_GT_OP } else { ARRAY_LT_OP }, Some(typarray))
    };
    if sort_opr == 0 {
        return Err(Box::new(
            PgError::error(format!(
                "could not identify a comparison function for type {}",
                ::format_type::format_type_be(elmtyp)?
            ))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }

    let collation = fcinfo.get_collation();
    let out = array_sort_with(mcx, &array, &meta, sub, |items| {
        ::tuplesort_seams::tuplesort_datums::call(
            mcx,
            sort_typ,
            sort_opr,
            collation,
            nulls_first,
            ::init_small::globals::work_mem(),
            items,
        )
    })?;
    byref_result(mcx, &out)
}

pub fn fc_array_sort(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    sort_common(flinfo, fcinfo, false, false)
}

pub fn fc_array_sort_order(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let descending = fcinfo.arg_bool(1);
    sort_common(flinfo, fcinfo, descending, descending)
}

pub fn fc_array_sort_order_nulls_first(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let descending = fcinfo.arg_bool(1);
    let nulls_first = fcinfo.arg_bool(2);
    sort_common(flinfo, fcinfo, descending, nulls_first)
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

const fn ns(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: false, retset: false, func }
}

pub const ARRAY_USERFUNCS_BUILTINS: &[FmgrBuiltin] = &[
    ns(378, "array_append", 2, fc_array_append),
    ns(379, "array_prepend", 2, fc_array_prepend),
    b(515, "array_larger", 2, fc_array_larger),
    b(516, "array_smaller", 2, fc_array_smaller),
    ns(3277, "array_position", 2, fc_array_position),
    ns(3278, "array_position_start", 3, fc_array_position_start),
    ns(3279, "array_positions", 2, fc_array_positions),
    ns(4051, "array_agg_array_transfn", 2, fc_array_agg_array_transfn),
    ns(4052, "array_agg_array_finalfn", 2, fc_array_agg_array_finalfn),
    b(6172, "trim_array", 2, fc_trim_array),
    b(6215, "array_shuffle", 1, fc_array_shuffle),
    b(6216, "array_sample", 2, fc_array_sample),
    ns(6296, "array_agg_array_combine", 2, fc_array_agg_array_combine),
    b(6297, "array_agg_array_serialize", 1, fc_array_agg_array_serialize),
    b(6298, "array_agg_array_deserialize", 2, fc_array_agg_array_deserialize),
    b(6378, "array_append_support", 1, fc_array_append_support),
    b(6379, "array_prepend_support", 1, fc_array_prepend_support),
    b(6381, "array_reverse", 1, fc_array_reverse),
    b(6388, "array_sort", 1, fc_array_sort),
    b(6389, "array_sort_order", 2, fc_array_sort_order),
    b(6390, "array_sort_order_nulls_first", 3, fc_array_sort_order_nulls_first),
];
