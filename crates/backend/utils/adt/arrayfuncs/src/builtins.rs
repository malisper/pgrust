use alloc::boxed::Box;

use ::datum::Datum;
use ::lsyscache::{get_type_io_data, IOFuncSelector};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult, ERRCODE_UNDEFINED_FUNCTION};
use ::types_fmgr::{
    byref_result, cstring_result, varlena_result, FmgrBuiltin, FmgrInfo,
    FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

use crate::foundation::varsize_any;
use ::mcx::vec_with_capacity_in;
use crate::io::{array_in, array_out, array_recv, array_send, ArrayIoMeta};

// Cached in FmgrInfo.fn_extra: resolved element I/O metadata + proc carrier,
// keyed by element_type (C's ArrayMetaState fn_extra memo).
struct ArrayMetaState {
    meta: ArrayIoMeta,
    proc: FmgrInfo,
}

fn build_meta(element_type: Oid, which: IOFuncSelector, binary: bool) -> PgResult<ArrayMetaState> {
    let io = get_type_io_data(element_type, which)?;
    if binary && io.func == 0 {
        let what = match which {
            IOFuncSelector::IOFunc_receive => "input",
            _ => "output",
        };
        return Err(Box::new(
            PgError::error(alloc::format!(
                "no binary {what} function available for type {element_type}"
            ))
            .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION),
        ));
    }
    let proc = ::fmgr_seams::fmgr_info::call(io.func)?;
    Ok(ArrayMetaState {
        meta: ArrayIoMeta {
            element_type,
            typlen: io.typlen as i32,
            typbyval: io.typbyval,
            typalign: io.typalign as u8,
            typdelim: io.typdelim as u8,
            typioparam: io.typioparam,
        },
        proc,
    })
}

// Populate/refresh the fn_extra memo for element_type; returns a &mut to it.
fn cached_meta<'f>(
    flinfo: &'f mut FmgrInfo,
    element_type: Oid,
    which: IOFuncSelector,
    binary: bool,
) -> PgResult<&'f mut ArrayMetaState> {
    let need = match flinfo.fn_extra_ref::<ArrayMetaState>() {
        Some(ams) => ams.meta.element_type != element_type,
        None => true,
    };
    if need {
        let ams = build_meta(element_type, which, binary)?;
        flinfo.set_fn_extra(ams);
    }
    Ok(flinfo.fn_extra_mut::<ArrayMetaState>().unwrap())
}

// Flatten an array-typed argument into an owned, MAXALIGN'd flat image.
pub(crate) fn arg_array_bytes<'mcx>(
    fcinfo: &Fcinfo,
    i: usize,
    mcx: ::mcx::Mcx<'mcx>,
) -> PgResult<::mcx::PgVec<'mcx, u8>> {
    // SAFETY: arg i is a non-null array (varlena) datum (strict function).
    let p = unsafe { fcinfo.arg_ptr(i) };
    let total = varsize_any(p);
    // SAFETY: a live varlena of `total` bytes.
    let raw = unsafe { core::slice::from_raw_parts(p, total) };
    ::detoast_seams::detoast_attr::call(mcx, raw)
}

pub fn fc_array_in(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of array_in is a non-null cstring.
    let s = unsafe { fcinfo.arg_cstring(0) };
    let string = s.to_str().map_err(|_| {
        Box::new(PgError::error("invalid UTF-8 in array literal"))
    })?;
    let element_type = fcinfo.arg(1).as_oid();
    let typmod = fcinfo.arg(2).as_i32();
    let mcx = fcinfo.result_mcx();
    // SAFETY: fcinfo.context, if set, is a live ErrorSaveNode armed for this call.
    let esc = unsafe { fcinfo.error_save_node() };

    let flinfo = flinfo.expect("array_in: NULL flinfo");
    let ams = cached_meta(flinfo, element_type, IOFuncSelector::IOFunc_input, false)?;
    match array_in(mcx, string, &ams.meta, &mut ams.proc, typmod, esc)? {
        Some(img) => byref_result(mcx, &img),
        None => Ok(Datum::null()),
    }
}

pub fn fc_array_out(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let array = arg_array_bytes(fcinfo, 0, mcx)?;
    let element_type = crate::foundation::arr_elemtype(&array);
    let flinfo = flinfo.expect("array_out: NULL flinfo");
    let ams = cached_meta(flinfo, element_type, IOFuncSelector::IOFunc_output, false)?;
    let out = array_out(mcx, &array, &ams.meta, &mut ams.proc)?;
    Ok(cstring_result(out))
}

pub fn fc_array_recv(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let spec_element_type = fcinfo.arg(1).as_oid();
    let typmod = fcinfo.arg(2).as_i32();
    let mcx = fcinfo.result_mcx();
    // SAFETY: arg 0 of a recv function is a live &mut StringInfo pointer.
    let buf = unsafe { &mut *(fcinfo.arg(0).as_usize() as *mut ::stringinfo::StringInfo<'_>) };
    let flinfo = flinfo.expect("array_recv: NULL flinfo");
    let ams = cached_meta(flinfo, spec_element_type, IOFuncSelector::IOFunc_receive, true)?;
    let img = array_recv(mcx, buf, &ams.meta, &mut ams.proc, typmod)?;
    byref_result(mcx, &img)
}

pub fn fc_array_send(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let array = arg_array_bytes(fcinfo, 0, mcx)?;
    let element_type = crate::foundation::arr_elemtype(&array);
    let flinfo = flinfo.expect("array_send: NULL flinfo");
    let ams = cached_meta(flinfo, element_type, IOFuncSelector::IOFunc_send, true)?;
    let out = array_send(mcx, &array, &ams.meta, &mut ams.proc)?;
    Ok(varlena_result(out))
}

// array_unnest (arrayfuncs.c): ValuePerCall SRF over a private copy of the
// detoasted array (C copies into multi_call_memory_ctx; the fn_extra Box is
// that lifetime here).
struct ArrayUnnestFctx {
    image: alloc::vec::Vec<u8>,
    nextelem: i32,
    numelems: i32,
    pos: usize,
    elmlen: i32,
    elmbyval: bool,
    elmalign: u8,
}

impl ArrayUnnestFctx {
    fn next(&mut self) -> Option<(Datum, bool)> {
        use crate::foundation::{att_addlength_pointer, att_align_nominal, fetch_att};
        if self.nextelem >= self.numelems {
            return None;
        }
        let offset = self.nextelem;
        self.nextelem += 1;
        let bo = crate::foundation::arr_nullbitmap_off(&self.image);
        if let Some(bo) = bo {
            let byte = self.image[bo + offset as usize / 8];
            if byte & (1 << (offset % 8)) == 0 {
                return Some((Datum::null(), true));
            }
        }
        let p = self.image[self.pos..].as_ptr();
        let d = fetch_att(p, self.elmbyval, self.elmlen);
        self.pos = att_addlength_pointer(self.pos, self.elmlen, p);
        self.pos = att_align_nominal(self.pos, self.elmalign);
        Some((d, false))
    }
}

pub fn fc_array_unnest(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("array_unnest: NULL flinfo");
    if !flinfo.has_fn_extra() {
        let mcx = fcinfo.result_mcx();
        let array = arg_array_bytes(fcinfo, 0, mcx)?;
        let elemtype = crate::foundation::arr_elemtype(&array);
        let (elmlen, elmbyval, elmalign) = ::lsyscache::get_typlenbyvalalign(elemtype)
            .map(|(l, b, a)| (l as i32, b, a as u8))?;
        let ndim = crate::foundation::arr_ndim(&array);
        let mut dims = [0i32; crate::foundation::MAXDIM];
        for i in 0..ndim as usize {
            dims[i] = crate::foundation::arr_dim(&array, i);
        }
        let numelems = ::arrayutils::array_get_n_items(ndim, &dims)?;
        let pos = crate::foundation::arr_data_offset(&array);
        let state = ArrayUnnestFctx {
            image: array.as_slice().to_vec(),
            nextelem: 0,
            numelems,
            pos,
            elmlen,
            elmbyval,
            elmalign,
        };
        let fctx = ::funcapi_srf::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(Box::new(state));
    }
    let next = ::funcapi_srf::per_MultiFuncCall(flinfo)
        .user_fctx
        .as_mut()
        .expect("array_unnest: user_fctx set at first call")
        .downcast_mut::<ArrayUnnestFctx>()
        .expect("array_unnest: user_fctx is ArrayUnnestFctx")
        .next();
    match next {
        Some((d, isnull)) => {
            let r = ::funcapi_srf::srf_return_next(flinfo, fcinfo, d);
            fcinfo.isnull = isnull;
            Ok(r)
        }
        None => Ok(::funcapi_srf::srf_return_done(flinfo, fcinfo)),
    }
}

// array_unnest_support (arrayfuncs.c): SupportRequestRows over the argument
// (Const array nitems / 1-D ArrayExpr length; anything else falls back).
pub fn fc_array_unnest_support(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let p = fcinfo.arg(0).as_usize() as *mut ();
    // SAFETY: prosupport contract — the internal arg points at a live
    // tag-first support-request node exclusively owned by this call.
    let Some(req) = (unsafe { ::types_nodes::supportnodes::support_request_rows_mut(p) }) else {
        return Ok(Datum::from_usize(0));
    };
    let Some(fe) = req.node.and_then(|n| n.as_func_expr()) else {
        return Ok(Datum::from_usize(0));
    };
    let Some(arg1) = fe.args.first() else {
        return Ok(Datum::from_usize(0));
    };
    let rows = if let Some(c) = arg1.as_const() {
        if c.constisnull {
            0.0
        } else {
            let ap = c.constvalue.as_usize() as *const u8;
            // SAFETY: non-null array Const addresses a live flat varlena image.
            let arr = unsafe { core::slice::from_raw_parts(ap, varsize_any(ap)) };
            let ndim = crate::foundation::arr_ndim(arr);
            let mut dims = [0i32; crate::foundation::MAXDIM];
            for i in 0..ndim as usize {
                dims[i] = crate::foundation::arr_dim(arr, i);
            }
            ::arrayutils::array_get_n_items(ndim, &dims)? as f64
        }
    } else if let Some(a) = arg1.as_array_expr() {
        if a.multidims {
            10.0
        } else {
            a.elements.len() as f64
        }
    } else {
        10.0
    };
    req.rows = rows;
    Ok(Datum::from_usize(p as usize))
}

// array_agg_transfn (array_userfuncs.c): transvalue is a pointer datum to an
// aggcontext-owned ArrayBuildState (INTERNAL transtype); the element type
// rides fn_expr (C get_fn_expr_argtype).
pub fn fc_array_agg_transfn(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    use ::datum::array_build::ArrayBuildState;

    let flinfo = flinfo.expect("array_agg_transfn: NULL flinfo");
    let arg1_typeid = fmgr_seams::get_fn_expr_argtype::call(flinfo, 1);
    if arg1_typeid == ::types_core::InvalidOid {
        return Err(Box::new(
            PgError::error("could not determine input data type")
                .with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    // SAFETY: fcinfo.context is the executor's live AggStateNode.
    let Some(aggmcx) = (unsafe { fcinfo.agg_context() }) else {
        panic!("array_agg_transfn called in non-aggregate context");
    };

    let stp: *mut ArrayBuildState<'_> = if fcinfo.args[0].isnull {
        let st = crate::build::init_array_result(aggmcx, arg1_typeid, false)?;
        let layout = core::alloc::Layout::new::<ArrayBuildState<'_>>();
        let raw = ::mcx::Allocator::allocate(&aggmcx, layout)
            .map_err(|_| aggmcx.oom(layout.size()))?;
        let p: *mut ArrayBuildState<'_> = raw.cast().as_ptr();
        // SAFETY: fresh aggcontext allocation of the exact layout; no drop
        // glue runs (PgVec fields are arena-plain — ForgetSafe).
        unsafe { p.write(st) };
        p
    } else {
        fcinfo.arg(0).as_usize() as *mut ArrayBuildState<'_>
    };

    let (elem, elem_null) = (fcinfo.args[1].value, fcinfo.args[1].isnull);
    let elem = if elem_null { Datum::null() } else { elem };
    // SAFETY: stp is the aggcontext-owned state; plain-data move in/out.
    unsafe {
        let st = stp.read();
        let st = crate::build::accum_array_result(aggmcx, Some(st), elem, elem_null, arg1_typeid)?;
        stp.write(st);
    }
    Ok(Datum::from_usize(stp as usize))
}

pub fn fc_array_agg_finalfn(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    use ::datum::array_build::ArrayBuildState;
    // SAFETY: fcinfo.context is the executor's live AggStateNode.
    debug_assert!(unsafe { fcinfo.agg_context() }.is_some());
    if fcinfo.args[0].isnull {
        return Ok(fcinfo.return_null());
    }
    let stp = fcinfo.arg(0).as_usize() as *const ArrayBuildState<'_>;
    // SAFETY: transvalue points at the aggcontext-owned build state.
    let st = unsafe { &*stp };
    let mcx = fcinfo.result_mcx();
    let dims = [st.nelems];
    let lbs = [1i32];
    let img = crate::build::make_md_array_result(mcx, st, 1, &dims, &lbs)?;
    byref_result(mcx, &img)
}

fn alloc_build_state<'m>(
    mcx: ::mcx::Mcx<'m>,
    st: ::datum::array_build::ArrayBuildState<'m>,
) -> PgResult<*mut ::datum::array_build::ArrayBuildState<'m>> {
    let layout = core::alloc::Layout::new::<::datum::array_build::ArrayBuildState<'_>>();
    let raw = ::mcx::Allocator::allocate(&mcx, layout).map_err(|_| mcx.oom(layout.size()))?;
    let p: *mut ::datum::array_build::ArrayBuildState<'m> = raw.cast().as_ptr();
    // SAFETY: fresh allocation of the exact layout; no drop glue runs (PgVec
    // fields are arena-plain).
    unsafe { p.write(st) };
    Ok(p)
}

pub fn fc_array_agg_combine(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    use ::datum::array_build::ArrayBuildState;
    // SAFETY: fcinfo.context is the executor's live AggStateNode.
    let Some(aggmcx) = (unsafe { fcinfo.agg_context() }) else {
        panic!("aggregate function called in non-aggregate context");
    };
    let state1 = if fcinfo.argisnull(0) {
        None
    } else {
        Some(fcinfo.arg(0).as_usize() as *mut ArrayBuildState<'_>)
    };
    let state2 = if fcinfo.argisnull(1) {
        None
    } else {
        Some(fcinfo.arg(1).as_usize() as *const ArrayBuildState<'_>)
    };
    // SAFETY: state pointers address live aggregate-owned build states.
    match (state1, state2) {
        (None, None) => Ok(fcinfo.return_null()),
        (Some(p1), None) => Ok(Datum::from_usize(p1 as usize)),
        (None, Some(p2)) => {
            let st = crate::build::array_agg_combine_clone(aggmcx, unsafe { &*p2 })?;
            Ok(Datum::from_usize(alloc_build_state(aggmcx, st)? as usize))
        }
        (Some(p1), Some(p2)) => {
            let s2 = unsafe { &*p2 };
            if s2.nelems > 0 {
                unsafe { crate::build::array_agg_combine_append(&mut *p1, s2)? };
            }
            Ok(Datum::from_usize(p1 as usize))
        }
    }
}

pub fn fc_array_agg_serialize(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    use ::datum::array_build::ArrayBuildState;
    // SAFETY: fcinfo.context is the executor's live AggStateNode.
    debug_assert!(unsafe { fcinfo.agg_context() }.is_some());
    // SAFETY: transvalue points at the aggcontext-owned build state.
    let st = unsafe { &*(fcinfo.arg(0).as_usize() as *const ArrayBuildState<'_>) };
    let mcx = fcinfo.result_mcx();
    let out = if st.typbyval {
        crate::build::array_agg_serialize_state(mcx, st, None)?
    } else {
        // C's SerialIOData fn_extra memo for the element typsend.
        let flinfo = flinfo.expect("array_agg_serialize: NULL flinfo");
        let ams = cached_meta(flinfo, st.element_type, IOFuncSelector::IOFunc_send, true)?;
        crate::build::array_agg_serialize_state(mcx, st, Some(&mut ams.proc))?
    };
    Ok(varlena_result(out))
}

pub fn fc_array_agg_deserialize(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: fcinfo.context is the executor's live AggStateNode.
    if unsafe { fcinfo.agg_context() }.is_none() {
        panic!("aggregate function called in non-aggregate context");
    }
    // SAFETY: strict fn — arg 0 is a non-null live bytea.
    let sstate = unsafe { fcinfo.arg_varlena_packed(0) }?;
    let payload = sstate.data();
    // SAFETY: the executor's per-input context outlives the returned state's
    // consumption by the immediately-following combine call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    // Wire layout: element_type(4) nelems(8) typlen(2) typbyval(1) — byte 14
    // decides whether C's DeserialIOData (typreceive) memo is needed.
    let st = if payload.len() >= 15 && payload[14] == 0 {
        let element_type = u32::from_be_bytes(payload[0..4].try_into().unwrap()) as Oid;
        let flinfo = flinfo.expect("array_agg_deserialize: NULL flinfo");
        let ams = cached_meta(flinfo, element_type, IOFuncSelector::IOFunc_receive, true)?;
        let typioparam = ams.meta.typioparam;
        crate::build::array_agg_deserialize_state(mcx, payload, Some((&mut ams.proc, typioparam)))?
    } else {
        crate::build::array_agg_deserialize_state(mcx, payload, None)?
    };
    Ok(Datum::from_usize(alloc_build_state(mcx, st)? as usize))
}

// C array_length (arrayfuncs.c).
pub fn fc_array_length(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (ndim, dims) = {
        let mcx = fcinfo.result_mcx();
        let array = arg_array_bytes(fcinfo, 0, mcx)?;
        let (ndim, dims, _lb) = crate::foundation::read_dims_lbounds(&array);
        (ndim, dims)
    };
    let reqdim = fcinfo.arg(1).as_i32();
    if ndim <= 0 || ndim > crate::foundation::MAXDIM as i32 || reqdim <= 0 || reqdim > ndim {
        return Ok(fcinfo.return_null());
    }
    Ok(Datum::from_i32(dims[(reqdim - 1) as usize]))
}

// C array_to_text_internal (varlena.c), hosted with the array machinery it
// consumes; null_string=None skips null elements.
fn array_to_text_common(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    null_string: Option<alloc::vec::Vec<u8>>,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let array = arg_array_bytes(fcinfo, 0, mcx)?;
    let sep: alloc::vec::Vec<u8> = {
        // SAFETY: arg 1 is a live text varlena (callers checked for NULL).
        let v = unsafe { fcinfo.arg_varlena_packed(1) }?;
        v.data().to_vec()
    };
    let element_type = crate::foundation::arr_elemtype(&array);
    let (ndim, dims, _lb) = crate::foundation::read_dims_lbounds(&array);
    let nitems = ::arrayutils::array_get_n_items(ndim, &dims)?;
    let mut out: alloc::vec::Vec<u8> = alloc::vec::Vec::new();
    if nitems > 0 {
        let flinfo = flinfo.expect("array_to_text: NULL flinfo");
        let ams = cached_meta(flinfo, element_type, IOFuncSelector::IOFunc_output, false)?;
        let (elems, nulls) = crate::construct::deconstruct_array(
            mcx,
            &array,
            ams.meta.typlen,
            ams.meta.typbyval,
            ams.meta.typalign,
            true,
        )?;
        let mut printed = false;
        for (i, &d) in elems.iter().enumerate() {
            if nulls[i] {
                if let Some(ns) = &null_string {
                    if printed {
                        out.extend_from_slice(&sep);
                    }
                    out.extend_from_slice(ns);
                    printed = true;
                }
                continue;
            }
            let v = crate::io::call1_armed(&mut ams.proc, mcx, d)?;
            // SAFETY: out fns return NUL-terminated cstrings.
            let cs = unsafe {
                core::ffi::CStr::from_ptr(v.as_usize() as *const core::ffi::c_char)
            };
            if printed {
                out.extend_from_slice(&sep);
            }
            out.extend_from_slice(cs.to_bytes());
            printed = true;
        }
    }
    let total = 4 + out.len();
    let mut img: ::mcx::PgVec<'_, u8> = vec_with_capacity_in(mcx, total)?;
    ::mcx::vec_append_bytes(&mut img, &(((total as u32) << 2)).to_ne_bytes())?;
    ::mcx::vec_append_bytes(&mut img, &out)?;
    byref_result(mcx, &img)
}

pub fn fc_array_to_text(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    array_to_text_common(flinfo, fcinfo, None)
}

pub fn fc_array_to_text_null(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    if fcinfo.argisnull(0) || fcinfo.argisnull(1) {
        return Ok(fcinfo.return_null());
    }
    let null_string = if !fcinfo.argisnull(2) {
        // SAFETY: arg 2 checked non-null; a live text varlena.
        Some(unsafe { fcinfo.arg_varlena_packed(2) }?.data().to_vec())
    } else {
        None
    };
    array_to_text_common(flinfo, fcinfo, null_string)
}

const fn srf(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: true, func }
}

const HASH_RECORD_OID: Oid = 6192;
const HASH_RECORD_EXTENDED_OID: Oid = 6193;


// C array_cat (array_userfuncs.c), hosted with the array machinery it
// consumes; catalog unit backend-utils-adt-array-user.
pub fn fc_array_cat(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    if fcinfo.argisnull(0) {
        if fcinfo.argisnull(1) {
            return Ok(fcinfo.return_null());
        }
        let v = arg_array_bytes(fcinfo, 1, mcx)?;
        return ::types_fmgr::byref_result(mcx, &v);
    }
    if fcinfo.argisnull(1) {
        let v = arg_array_bytes(fcinfo, 0, mcx)?;
        return ::types_fmgr::byref_result(mcx, &v);
    }
    let v1 = arg_array_bytes(fcinfo, 0, mcx)?;
    let v2 = arg_array_bytes(fcinfo, 1, mcx)?;

    use crate::foundation as f;
    let element_type1 = f::arr_elemtype(&v1);
    let element_type2 = f::arr_elemtype(&v2);
    if element_type1 != element_type2 {
        return Err(Box::new(
            PgError::error(alloc::format!("cannot concatenate incompatible arrays"))
                .with_detail(alloc::format!(
                    "Arrays with element types {} and {} are not compatible for concatenation.",
                    ::format_type::format_type_be(element_type1)?,
                    ::format_type::format_type_be(element_type2)?
                ))
                .with_sqlstate(::types_error::ERRCODE_DATATYPE_MISMATCH),
        ));
    }
    let element_type = element_type1;

    let (ndims1, dims1, lbs1) = f::read_dims_lbounds(&v1);
    let (ndims2, dims2, lbs2) = f::read_dims_lbounds(&v2);
    if ndims1 == 0 && ndims2 > 0 {
        return ::types_fmgr::byref_result(mcx, &v2);
    }
    if ndims2 == 0 {
        return ::types_fmgr::byref_result(mcx, &v1);
    }
    if ndims1 != ndims2 && ndims1 != ndims2 - 1 && ndims1 != ndims2 + 1 {
        return Err(cat_incompatible(&alloc::format!(
            "Arrays of {ndims1} and {ndims2} dimensions are not compatible for concatenation."
        )));
    }

    let nitems1 = ::arrayutils::array_get_n_items(ndims1, &dims1)?;
    let nitems2 = ::arrayutils::array_get_n_items(ndims2, &dims2)?;
    let ndatabytes1 = f::arr_size(&v1) - f::arr_data_offset(&v1);
    let ndatabytes2 = f::arr_size(&v2) - f::arr_data_offset(&v2);

    let mut dims = [0i32; f::MAXDIM];
    let mut lbs = [0i32; f::MAXDIM];
    let ndims;
    if ndims1 == ndims2 {
        ndims = ndims1;
        dims[0] = dims1[0] + dims2[0];
        lbs[0] = lbs1[0];
        for i in 1..ndims as usize {
            if dims1[i] != dims2[i] || lbs1[i] != lbs2[i] {
                return Err(cat_incompatible(
                    "Arrays with differing element dimensions are not compatible for \
                     concatenation.",
                ));
            }
            dims[i] = dims1[i];
            lbs[i] = lbs1[i];
        }
    } else if ndims1 == ndims2 - 1 {
        ndims = ndims2;
        dims[..ndims as usize].copy_from_slice(&dims2[..ndims as usize]);
        lbs[..ndims as usize].copy_from_slice(&lbs2[..ndims as usize]);
        dims[0] += 1;
        for i in 0..ndims1 as usize {
            if dims1[i] != dims[i + 1] || lbs1[i] != lbs[i + 1] {
                return Err(cat_incompatible(
                    "Arrays with differing dimensions are not compatible for concatenation.",
                ));
            }
        }
    } else {
        ndims = ndims1;
        dims[..ndims as usize].copy_from_slice(&dims1[..ndims as usize]);
        lbs[..ndims as usize].copy_from_slice(&lbs1[..ndims as usize]);
        dims[0] += 1;
        for i in 0..ndims2 as usize {
            if dims2[i] != dims[i + 1] || lbs2[i] != lbs[i + 1] {
                return Err(cat_incompatible(
                    "Arrays with differing dimensions are not compatible for concatenation.",
                ));
            }
        }
    }

    let nitems = ::arrayutils::array_get_n_items(ndims, &dims)?;
    ::arrayutils::array_check_bounds(ndims, &dims, &lbs)?;

    let ndatabytes = ndatabytes1 + ndatabytes2;
    let hasnull = f::arr_hasnull(&v1) || f::arr_hasnull(&v2);
    let (dataoffset, nbytes) = if hasnull {
        let d = f::arr_overhead_withnulls(ndims, nitems);
        (d as i32, ndatabytes + d)
    } else {
        (0i32, ndatabytes + f::arr_overhead_nonulls(ndims))
    };
    let mut out: ::mcx::PgVec<u8> = vec_with_capacity_in(mcx, nbytes)?;
    out.resize(nbytes, 0);
    crate::construct::write_header(&mut out, nbytes, ndims, dataoffset, element_type);
    crate::construct::write_dims_lbounds(&mut out, ndims, &dims, &lbs);
    let dstoff = f::arr_data_offset(&out);
    out[dstoff..dstoff + ndatabytes1]
        .copy_from_slice(&v1[f::arr_data_offset(&v1)..f::arr_data_offset(&v1) + ndatabytes1]);
    out[dstoff + ndatabytes1..dstoff + ndatabytes]
        .copy_from_slice(&v2[f::arr_data_offset(&v2)..f::arr_data_offset(&v2) + ndatabytes2]);
    if hasnull {
        let dest_bo = f::arr_nullbitmap_off(&out).expect("hasnull result has a bitmap");
        let src1 = f::arr_nullbitmap_off(&v1).map(|o| (&v1[..], o));
        let src2 = f::arr_nullbitmap_off(&v2).map(|o| (&v2[..], o));
        crate::element::array_bitmap_copy(&mut out, dest_bo, 0, src1, 0, nitems1);
        crate::element::array_bitmap_copy(&mut out, dest_bo, nitems1, src2, 0, nitems2);
    }
    ::types_fmgr::byref_result(mcx, &out)
}

#[track_caller]
#[cold]
#[inline(never)]
fn cat_incompatible(detail: &str) -> Box<PgError> {
    Box::new(
        PgError::error(alloc::format!("cannot concatenate incompatible arrays"))
            .with_detail(detail)
            .with_sqlstate(::types_error::ERRCODE_ARRAY_SUBSCRIPT_ERROR),
    )
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs,
        strict: true,
        retset: false,
        func,
    }
}

const fn agg(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: false, retset: false, func }
}

// Cached in FmgrInfo.fn_extra by the comparison family: the element type's
// resolved eq/cmp finfo + physical properties (C caches the typcache entry).
struct ElemCmpState {
    element_type: Oid,
    finfo: FmgrInfo,
    typlen: i32,
    typbyval: bool,
    typalign: u8,
}

fn fresh_elem_cmp(element_type: Oid, eq: bool) -> PgResult<ElemCmpState> {
    let flags = if eq {
        ::typcache::TYPECACHE_EQ_OPR_FINFO
    } else {
        ::typcache::TYPECACHE_CMP_PROC_FINFO
    };
    let entry = ::typcache::lookup_type_cache(element_type, flags)?;
    let finfo = if eq { entry.eq_opr_finfo().clone() } else { entry.cmp_proc_finfo().clone() };
    if finfo.fn_oid == 0 {
        let name = ::format_type::format_type_be(element_type)?;
        let what = if eq { "an equality operator" } else { "a comparison function" };
        return Err(Box::new(
            PgError::error(alloc::format!("could not identify {what} for type {name}"))
                .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION),
        ));
    }
    Ok(ElemCmpState {
        element_type,
        finfo,
        typlen: entry.typlen() as i32,
        typbyval: entry.typbyval(),
        typalign: entry.typalign() as u8,
    })
}

// flinfo-less callers exist (tuplesort's comparison shim carries no FmgrInfo);
// the per-call typcache probe replaces the fn_extra memo there.
fn resolve_elem_cmp<'f>(
    flinfo: Option<&'f mut FmgrInfo>,
    scratch: &'f mut Option<ElemCmpState>,
    element_type: Oid,
    eq: bool,
) -> PgResult<&'f mut ElemCmpState> {
    let Some(flinfo) = flinfo else {
        return Ok(scratch.insert(fresh_elem_cmp(element_type, eq)?));
    };
    let need = match flinfo.fn_extra_ref::<ElemCmpState>() {
        Some(s) => s.element_type != element_type,
        None => true,
    };
    if need {
        let st = fresh_elem_cmp(element_type, eq)?;
        flinfo.set_fn_extra(st);
    }
    Ok(flinfo.fn_extra_mut::<ElemCmpState>().unwrap())
}

#[cold]
#[inline(never)]
fn elem_type_mismatch() -> PgResult<Datum> {
    Err(Box::new(
        PgError::error("cannot compare arrays of different element types")
            .with_sqlstate(::types_error::ERRCODE_DATATYPE_MISMATCH),
    ))
}

fn array_eq_internal(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<bool> {
    // By-val result; detoast/deconstruct scratch dies with the call (C's
    // AARR_FREE_IF_COPY), so no armed result frame is required.
    let scratch = ::mcx::MemoryContext::new_bump("array_eq scratch");
    let mcx = scratch.mcx();
    let a1 = arg_array_bytes(fcinfo, 0, mcx)?;
    let a2 = arg_array_bytes(fcinfo, 1, mcx)?;
    let collation = fcinfo.fncollation;
    let element_type = crate::foundation::arr_elemtype(&a1);
    if element_type != crate::foundation::arr_elemtype(&a2) {
        elem_type_mismatch()?;
    }
    let (nd1, dims1, lbs1) = crate::foundation::read_dims_lbounds(&a1);
    let (nd2, dims2, lbs2) = crate::foundation::read_dims_lbounds(&a2);
    if nd1 != nd2
        || dims1[..nd1 as usize] != dims2[..nd1 as usize]
        || lbs1[..nd1 as usize] != lbs2[..nd1 as usize]
    {
        return Ok(false);
    }
    let mut shim_scratch = None;
    let st = resolve_elem_cmp(flinfo, &mut shim_scratch, element_type, true)?;
    let (vals1, nulls1) = crate::construct::deconstruct_array(
        mcx, &a1, st.typlen, st.typbyval, st.typalign, true,
    )?;
    let (vals2, nulls2) = crate::construct::deconstruct_array(
        mcx, &a2, st.typlen, st.typbyval, st.typalign, true,
    )?;
    for i in 0..vals1.len() {
        // Two NULLs are equal; NULL vs not-NULL is unequal (C array_eq).
        if nulls1[i] && nulls2[i] {
            continue;
        }
        if nulls1[i] || nulls2[i] {
            return Ok(false);
        }
        let r = ::types_fmgr::function_call2_coll_in(
            &mut st.finfo,
            collation,
            mcx,
            vals1[i],
            vals2[i],
        )?;
        if !r.as_bool() {
            return Ok(false);
        }
    }
    Ok(true)
}

fn array_cmp_internal(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<i32> {
    let scratch = ::mcx::MemoryContext::new_bump("array_cmp scratch");
    let mcx = scratch.mcx();
    let a1 = arg_array_bytes(fcinfo, 0, mcx)?;
    let a2 = arg_array_bytes(fcinfo, 1, mcx)?;
    let collation = fcinfo.fncollation;
    let element_type = crate::foundation::arr_elemtype(&a1);
    if element_type != crate::foundation::arr_elemtype(&a2) {
        elem_type_mismatch()?;
    }
    let (nd1, dims1, lbs1) = crate::foundation::read_dims_lbounds(&a1);
    let (nd2, dims2, lbs2) = crate::foundation::read_dims_lbounds(&a2);
    let nitems1 = ::arrayutils::array_get_n_items(nd1, &dims1)? as usize;
    let nitems2 = ::arrayutils::array_get_n_items(nd2, &dims2)? as usize;
    let mut shim_scratch = None;
    let st = resolve_elem_cmp(flinfo, &mut shim_scratch, element_type, false)?;
    let (vals1, nulls1) = crate::construct::deconstruct_array(
        mcx, &a1, st.typlen, st.typbyval, st.typalign, true,
    )?;
    let (vals2, nulls2) = crate::construct::deconstruct_array(
        mcx, &a2, st.typlen, st.typbyval, st.typalign, true,
    )?;
    for i in 0..nitems1.min(nitems2) {
        // Two NULLs are equal; NULL sorts above not-NULL (C array_cmp).
        if nulls1[i] && nulls2[i] {
            continue;
        }
        if nulls1[i] {
            return Ok(1);
        }
        if nulls2[i] {
            return Ok(-1);
        }
        let c = ::types_fmgr::function_call2_coll_in(
            &mut st.finfo,
            collation,
            mcx,
            vals1[i],
            vals2[i],
        )?
        .as_i32();
        if c < 0 {
            return Ok(-1);
        }
        if c > 0 {
            return Ok(1);
        }
    }
    if nitems1 != nitems2 {
        return Ok(if nitems1 < nitems2 { -1 } else { 1 });
    }
    if nd1 != nd2 {
        return Ok(if nd1 < nd2 { -1 } else { 1 });
    }
    for i in 0..nd1 as usize {
        if dims1[i] != dims2[i] {
            return Ok(if dims1[i] < dims2[i] { -1 } else { 1 });
        }
    }
    for i in 0..nd1 as usize {
        if lbs1[i] != lbs2[i] {
            return Ok(if lbs1[i] < lbs2[i] { -1 } else { 1 });
        }
    }
    Ok(0)
}

pub fn fc_array_eq(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(array_eq_internal(flinfo, fcinfo)?))
}

pub fn fc_array_ne(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(!array_eq_internal(flinfo, fcinfo)?))
}

pub fn fc_btarraycmp(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(array_cmp_internal(flinfo, fcinfo)?))
}

pub fn fc_array_lt(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(array_cmp_internal(flinfo, fcinfo)? < 0))
}

pub fn fc_array_gt(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(array_cmp_internal(flinfo, fcinfo)? > 0))
}

pub fn fc_array_le(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(array_cmp_internal(flinfo, fcinfo)? <= 0))
}

pub fn fc_array_ge(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(array_cmp_internal(flinfo, fcinfo)? >= 0))
}

// oidvectorrecv/oidvectorsend (oid.c): thin array_recv/array_send delegations;
// they live here to keep adt_scalar off the lsyscache dependency spine.
pub fn fc_oidvectorrecv(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of oidvectorrecv is internal (StringInfo).
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let mcx = fcinfo.result_mcx();
    let flinfo = flinfo.expect("oidvectorrecv: NULL flinfo");
    let ams = cached_meta(
        flinfo,
        ::types_core::catalog::OIDOID,
        IOFuncSelector::IOFunc_receive,
        true,
    )?;
    let img = array_recv(mcx, buf, &ams.meta, &mut ams.proc, -1)?;
    let ndim = i32::from_ne_bytes(img[4..8].try_into().unwrap());
    let dataoffset = i32::from_ne_bytes(img[8..12].try_into().unwrap());
    let elemtype = u32::from_ne_bytes(img[12..16].try_into().unwrap());
    if ndim != 1
        || dataoffset != 0
        || elemtype != ::types_core::catalog::OIDOID as u32
        || i32::from_ne_bytes(img[20..24].try_into().unwrap()) != 0
    {
        return Err(Box::new(
            PgError::error("invalid oidvector data")
                .with_sqlstate(::types_error::ERRCODE_INVALID_BINARY_REPRESENTATION),
        ));
    }
    byref_result(mcx, &img)
}

pub fn fc_oidvectorsend(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let array = arg_array_bytes(fcinfo, 0, mcx)?;
    let flinfo = flinfo.expect("oidvectorsend: NULL flinfo");
    let ams = cached_meta(
        flinfo,
        ::types_core::catalog::OIDOID,
        IOFuncSelector::IOFunc_send,
        true,
    )?;
    let out = array_send(mcx, &array, &ams.meta, &mut ams.proc)?;
    Ok(varlena_result(out))
}

// int2vectorrecv/int2vectorsend (int.c): same array_recv/array_send delegation
// shape as oidvector, with int.c's 1-D/0-based/no-null sanity checks.
pub fn fc_int2vectorrecv(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of int2vectorrecv is internal (StringInfo).
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let mcx = fcinfo.result_mcx();
    let flinfo = flinfo.expect("int2vectorrecv: NULL flinfo");
    let ams = cached_meta(
        flinfo,
        ::types_core::catalog::INT2OID,
        IOFuncSelector::IOFunc_receive,
        true,
    )?;
    let img = array_recv(mcx, buf, &ams.meta, &mut ams.proc, -1)?;
    let ndim = i32::from_ne_bytes(img[4..8].try_into().unwrap());
    let dataoffset = i32::from_ne_bytes(img[8..12].try_into().unwrap());
    let elemtype = u32::from_ne_bytes(img[12..16].try_into().unwrap());
    if ndim != 1
        || dataoffset != 0
        || elemtype != ::types_core::catalog::INT2OID as u32
        || i32::from_ne_bytes(img[20..24].try_into().unwrap()) != 0
    {
        return Err(Box::new(
            PgError::error("invalid int2vector data")
                .with_sqlstate(::types_error::ERRCODE_INVALID_BINARY_REPRESENTATION),
        ));
    }
    byref_result(mcx, &img)
}

pub fn fc_int2vectorsend(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let array = arg_array_bytes(fcinfo, 0, mcx)?;
    let flinfo = flinfo.expect("int2vectorsend: NULL flinfo");
    let ams = cached_meta(
        flinfo,
        ::types_core::catalog::INT2OID,
        IOFuncSelector::IOFunc_send,
        true,
    )?;
    let out = array_send(mcx, &array, &ams.meta, &mut ams.proc)?;
    Ok(varlena_result(out))
}

const fn nb(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: false, retset: false, func }
}

// pg_proc.dat rows for the generic array functions.
pub const ARRAYFUNCS_BUILTINS: &[FmgrBuiltin] = &[
    b(382, "btarraycmp", 2, fc_btarraycmp),
    b(390, "array_ne", 2, fc_array_ne),
    b(391, "array_lt", 2, fc_array_lt),
    b(392, "array_gt", 2, fc_array_gt),
    b(393, "array_le", 2, fc_array_le),
    b(396, "array_ge", 2, fc_array_ge),
    b(744, "array_eq", 2, fc_array_eq),
    b(750, "array_in", 3, fc_array_in),
    b(751, "array_out", 1, fc_array_out),
    // anyarray_out (pseudotypes.c) is `return array_out(fcinfo)`.
    b(2297, "anyarray_out", 1, fc_array_out),
    b(395, "array_to_text", 2, fc_array_to_text),
    nb(384, "array_to_text_null", 3, fc_array_to_text_null),
    b(2176, "array_length", 2, fc_array_length),
    b(383, "array_cat", 2, fc_array_cat),
    b(2400, "array_recv", 3, fc_array_recv),
    b(2401, "array_send", 1, fc_array_send),
    b(2410, "int2vectorrecv", 1, fc_int2vectorrecv),
    b(2411, "int2vectorsend", 1, fc_int2vectorsend),
    b(2420, "oidvectorrecv", 1, fc_oidvectorrecv),
    b(2421, "oidvectorsend", 1, fc_oidvectorsend),
    agg(2333, "array_agg_transfn", 2, fc_array_agg_transfn),
    agg(2334, "array_agg_finalfn", 2, fc_array_agg_finalfn),
    agg(6293, "array_agg_combine", 2, fc_array_agg_combine),
    b(6294, "array_agg_serialize", 1, fc_array_agg_serialize),
    b(6295, "array_agg_deserialize", 2, fc_array_agg_deserialize),
    srf(2331, "array_unnest", 1, fc_array_unnest),
    b(3996, "array_unnest_support", 1, fc_array_unnest_support),
    b(747, "array_dims", 1, crate::ops::fc_array_dims),
    b(748, "array_ndims", 1, crate::ops::fc_array_ndims),
    b(2091, "array_lower", 2, crate::ops::fc_array_lower),
    b(2092, "array_upper", 2, crate::ops::fc_array_upper),
    b(3179, "array_cardinality", 1, crate::ops::fc_array_cardinality),
    b(626, "hash_array", 1, crate::ops::fc_hash_array),
    b(782, "hash_array_extended", 2, crate::ops::fc_hash_array_extended),
    b(2747, "arrayoverlap", 2, crate::ops::fc_arrayoverlap),
    b(2748, "arraycontains", 2, crate::ops::fc_arraycontains),
    b(2749, "arraycontained", 2, crate::ops::fc_arraycontained),
    nb(3167, "array_remove", 2, crate::ops::fc_array_remove),
    nb(3168, "array_replace", 3, crate::ops::fc_array_replace),
    nb(1193, "array_fill", 2, crate::ops::fc_array_fill),
    nb(1286, "array_fill_with_lower_bounds", 3, crate::ops::fc_array_fill_with_lower_bounds),
    srf(1191, "generate_subscripts", 3, crate::ops::fc_generate_subscripts),
    srf(1192, "generate_subscripts_nodir", 2, crate::ops::fc_generate_subscripts),
    b(3218, "width_bucket_array", 2, crate::ops::fc_width_bucket_array),
];
