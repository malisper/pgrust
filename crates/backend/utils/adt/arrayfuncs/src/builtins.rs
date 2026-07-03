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
fn arg_array_bytes<'mcx>(
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

const fn srf(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: true, func }
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

// pg_proc.dat rows for the generic array I/O functions.
pub const ARRAYFUNCS_BUILTINS: &[FmgrBuiltin] = &[
    b(750, "array_in", 3, fc_array_in),
    b(751, "array_out", 1, fc_array_out),
    b(2400, "array_recv", 3, fc_array_recv),
    b(2401, "array_send", 1, fc_array_send),
    agg(2333, "array_agg_transfn", 2, fc_array_agg_transfn),
    agg(2334, "array_agg_finalfn", 2, fc_array_agg_finalfn),
    srf(2331, "array_unnest", 1, fc_array_unnest),
    b(3996, "array_unnest_support", 1, fc_array_unnest_support),
];
