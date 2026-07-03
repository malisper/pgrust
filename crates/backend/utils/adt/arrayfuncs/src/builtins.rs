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

// array_agg_transfn (array_userfuncs.c): transvalue is a pointer datum to an
// aggcontext-owned ArrayBuildState (INTERNAL transtype); the element type
// rides fn_expr (C get_fn_expr_argtype).
pub fn fc_array_agg_transfn(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    use ::datum::array_build::ArrayBuildState;

    let flinfo = flinfo.expect("array_agg_transfn: NULL flinfo");
    let arg1_typeid = flinfo
        .fn_expr
        .as_ref()
        .and_then(|e| e.downcast_ref::<::types_nodes::Node<'static>>())
        .and_then(|n| n.as_aggref())
        .and_then(|a| a.aggargtypes.first());
    let Some(arg1_typeid) = arg1_typeid else {
        return Err(Box::new(
            PgError::error("could not determine input data type")
                .with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    };
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
];
