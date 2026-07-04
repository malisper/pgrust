// fastpath.c: server side of the libpq PQfn Function-call protocol ('F').
#![allow(non_snake_case, non_upper_case_globals)]

use core::ffi::CStr;

use datum::Datum;
use elog::ereport;
use mcx::{Mcx, PgVec};
use stringinfo::StringInfo;
use types_core::{InvalidOid, Oid, FUNC_MAX_ARGS, NAMESPACE_RELATION_ID, PROCEDURE_RELATION_ID};
use types_error::{
    PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_BINARY_REPRESENTATION,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_PROTOCOL_VIOLATION, ERRCODE_UNDEFINED_FUNCTION,
    ERROR, LOG,
};
use types_fmgr::{FmgrInfo, LocalFcinfo, PackedVarlena};
use types_nodes::parsenodes::ObjectType;

const PqMsg_FunctionCallResponse: u8 = b'V';
const PROKIND_FUNCTION: i8 = b'f' as i8;

struct FpInfo {
    funcid: Oid,
    flinfo: FmgrInfo,
    namespace: Oid,
    rettype: Oid,
    argtypes: [Oid; FUNC_MAX_ARGS as usize],
    fname: String,
}

fn loc(line: i32, func: &'static str) -> types_error::ErrorLocation {
    types_error::ErrorLocation::new("fastpath.c", line, func)
}

fn send_function_result<'mcx>(
    mcx: Mcx<'mcx>,
    retval: Datum,
    isnull: bool,
    rettype: Oid,
    format: i16,
) -> PgResult<()> {
    let mut buf = pqformat::pq_beginmessage(mcx, PqMsg_FunctionCallResponse)?;

    if isnull {
        pqformat::pq_sendint32(&mut buf, (-1_i32) as u32)?;
    } else if format == 0 {
        let (typoutput, _typisvarlena) = lsyscache::typ::getTypeOutputInfo(rettype)?;
        let mut finfo = fmgr_seams::fmgr_info::call(typoutput)?;
        let out = types_fmgr::function_call1_coll_in(&mut finfo, InvalidOid, mcx, retval)?;
        // SAFETY: text output fns return a NUL-terminated cstring datum.
        let s = unsafe { CStr::from_ptr(out.as_usize() as *const core::ffi::c_char) }.to_bytes();
        pqformat::pq_sendcountedtext(&mut buf, s)?;
    } else if format == 1 {
        let (typsend, _typisvarlena) = lsyscache::typ::getTypeBinaryOutputInfo(rettype)?;
        let mut finfo = fmgr_seams::fmgr_info::call(typsend)?;
        let out = types_fmgr::send_function_call(&mut finfo, retval, mcx)?;
        // SAFETY: send fns return an untoasted bytea image.
        let v = unsafe { PackedVarlena::from_ptr(out.as_usize() as *const u8) };
        let data = v.data();
        pqformat::pq_sendint32(&mut buf, data.len() as u32)?;
        pqformat::pq_sendbytes(&mut buf, data)?;
    } else {
        return Err(unsupported_format_code(format as i32));
    }

    pqformat::pq_endmessage(buf)
}

#[cold]
fn unsupported_format_code(format: i32) -> Box<types_error::PgError> {
    ereport(ERROR)
        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        .errmsg(format!("unsupported format code: {format}"))
        .into_error()
        .into()
}

fn fetch_fp_info<'mcx>(mcx: Mcx<'mcx>, func_id: Oid) -> PgResult<FpInfo> {
    let Some(pp) = syscache_seams::lookup_pg_proc_shape::call(func_id)? else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!("function with OID {func_id} does not exist"))
            .into_error()
            .into());
    };
    let fname = match syscache_seams::pg_proc_proname::call(func_id)? {
        Some(name) => String::from_utf8_lossy(name.name_str()).into_owned(),
        None => String::new(),
    };

    // Reject pg_proc entries that are unsafe to call via fastpath.
    if pp.prokind != PROKIND_FUNCTION || pp.proretset {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot call function \"{fname}\" via fastpath interface"))
            .into_error()
            .into());
    }

    if pp.pronargs as i32 > FUNC_MAX_ARGS as i32 {
        return Err(ereport(ERROR)
            .errmsg_internal(format!(
                "function {fname} has more than {FUNC_MAX_ARGS} arguments"
            ))
            .into_error()
            .into());
    }

    let mut argtypes = [InvalidOid; FUNC_MAX_ARGS as usize];
    let Some((_rettype, sig_argtypes)) =
        syscache_seams::lookup_pg_proc_signature::call(mcx, func_id)?
    else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!("function with OID {func_id} does not exist"))
            .into_error()
            .into());
    };
    argtypes[..sig_argtypes.len()].copy_from_slice(&sig_argtypes);

    let flinfo = fmgr_seams::fmgr_info::call(func_id)?;

    Ok(FpInfo {
        funcid: func_id,
        flinfo,
        namespace: pp.pronamespace,
        rettype: pp.prorettype,
        argtypes,
        fname,
    })
}

/// HandleFunctionRequest: read the message, look up the function, parse the
/// arguments, invoke, send the result. Returns `was_logged`; the caller
/// (PostgresMain's 'F' arm) runs check_log_duration — a shape divergence from
/// C, which logs duration in-function via postgres.c statics.
pub fn HandleFunctionRequest<'mcx>(
    mcx: Mcx<'mcx>,
    msg_buf: &mut StringInfo<'mcx>,
) -> PgResult<bool> {
    let mut was_logged = false;

    if xact::IsAbortedTransactionBlockState() {
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_IN_FAILED_SQL_TRANSACTION)
            .errmsg(
                "current transaction is aborted, commands ignored until end of transaction block",
            )
            .into_error()
            .into());
    }

    // Set snapshot in case needed by the function or the datatype I/O.
    let snap = snapmgr::GetTransactionSnapshot()?;
    snapmgr::PushActiveSnapshot(&snap)?;

    let fid: Oid = pqformat::pq_getmsgint(msg_buf, 4)?;

    let mut fip = fetch_fp_info(mcx, fid)?;

    if guc_tables::backing::log_statement() == guc_tables::consts::LOGSTMT_ALL {
        ereport(LOG)
            .errmsg(format!(
                "fastpath function call: \"{}\" (OID {fid})",
                fip.fname
            ))
            .finish(loc(233, "HandleFunctionRequest"))?;
        was_logged = true;
    }

    // Check permission to access and call the function; no normal name lookup
    // happened, so check schema usage too.
    let aclresult = aclchk::object_aclcheck(
        NAMESPACE_RELATION_ID,
        fip.namespace,
        miscinit::GetUserId(),
        adt_acl::ACL_USAGE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        let nspname = lsyscache::get_namespace_name(mcx, fip.namespace)?;
        aclchk::aclcheck_error(
            aclresult,
            ObjectType::OBJECT_SCHEMA,
            nspname.as_ref().map_or("", |s| s.as_str()),
        )?;
    }

    let aclresult = aclchk::object_aclcheck(
        PROCEDURE_RELATION_ID,
        fid,
        miscinit::GetUserId(),
        adt_acl::ACL_EXECUTE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        let funcname = lsyscache::get_func_name(mcx, fid)?;
        aclchk::aclcheck_error(
            aclresult,
            ObjectType::OBJECT_FUNCTION,
            funcname.as_ref().map_or("", |s| s.as_str()),
        )?;
    }

    // Note: collation = InvalidOid, so collation-sensitive functions can't be
    // called this way.
    let mut fcinfo = LocalFcinfo::<{ FUNC_MAX_ARGS as usize }>::fresh(InvalidOid);
    // SAFETY: the message context outlives this call.
    unsafe { fcinfo.set_result_mcx(mcx) };

    let rformat = parse_fcall_arguments(mcx, msg_buf, &fip, &mut fcinfo)?;

    pqformat::pq_getmsgend(msg_buf)?;

    // If func is strict, must not call it for null args.
    let mut callit = true;
    if fip.flinfo.fn_strict && fcinfo.has_null_args() {
        callit = false;
    }

    let (retval, isnull) = if callit {
        let v = fip.flinfo.invoke(&mut fcinfo)?;
        (v, fcinfo.isnull)
    } else {
        (Datum::null(), true)
    };

    // At least one CHECK_FOR_INTERRUPTS per function call.
    postgres_seams::check_for_interrupts::call()?;

    send_function_result(mcx, retval, isnull, fip.rettype, rformat)?;

    snapmgr::PopActiveSnapshot()?;

    Ok(was_logged)
}

fn parse_fcall_arguments<'mcx>(
    mcx: Mcx<'mcx>,
    msg_buf: &mut StringInfo<'mcx>,
    fip: &FpInfo,
    fcinfo: &mut LocalFcinfo<{ FUNC_MAX_ARGS as usize }>,
) -> PgResult<i16> {
    let numAFormats = pqformat::pq_getmsgint(msg_buf, 2)? as i32;
    let mut aformats: PgVec<'mcx, i16> = PgVec::new_in(mcx);
    if numAFormats > 0 {
        aformats
            .try_reserve_exact(numAFormats as usize)
            .map_err(|_| mcx.oom(numAFormats as usize))?;
        for _ in 0..numAFormats {
            aformats.push(pqformat::pq_getmsgint(msg_buf, 2)? as i16);
        }
    }

    let nargs = pqformat::pq_getmsgint(msg_buf, 2)? as i32;

    if fip.flinfo.fn_nargs as i32 != nargs || nargs > FUNC_MAX_ARGS as i32 {
        let requires = fip.flinfo.fn_nargs;
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!(
                "function call message contains {nargs} arguments but function requires {requires}"
            ))
            .into_error()
            .into());
    }

    fcinfo.nargs = nargs as i16;

    if numAFormats > 1 && numAFormats != nargs {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!(
                "function call message contains {numAFormats} argument formats but {nargs} arguments"
            ))
            .into_error()
            .into());
    }

    for i in 0..nargs as usize {
        let argsize = pqformat::pq_getmsgint(msg_buf, 4)? as i32;
        let mut raw: Option<&[u8]> = None;
        if argsize == -1 {
            fcinfo.set_arg_null(i);
        } else {
            if argsize < 0 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg(format!(
                        "invalid argument size {argsize} in function call message"
                    ))
                    .into_error()
                    .into());
            }
            raw = Some(pqformat::pq_getmsgbytes(msg_buf, argsize as usize)?);
        }

        let aformat: i16 = if numAFormats > 1 {
            aformats[i]
        } else if numAFormats > 0 {
            aformats[0]
        } else {
            0
        };

        if aformat == 0 {
            let (typinput, typioparam) = lsyscache::typ::getTypeInputInfo(fip.argtypes[i])?;
            let pstring: Option<PgVec<'mcx, u8>> = match raw {
                None => None,
                Some(raw) => Some(client_to_server_cstring(mcx, raw)?),
            };
            let cstr = pstring.as_ref().map(|v| {
                CStr::from_bytes_with_nul(v).expect("client_to_server_cstring NUL-terminates")
            });
            let mut finfo = fmgr_seams::fmgr_info::call(typinput)?;
            let v = types_fmgr::input_function_call(&mut finfo, cstr, typioparam, -1, mcx)?;
            // C stores the value even for a NULL arg; isnull was set above.
            fcinfo.args[i].value = v;
        } else if aformat == 1 {
            let (typreceive, typioparam) =
                lsyscache::typ::getTypeBinaryInputInfo(fip.argtypes[i])?;
            let mut finfo = fmgr_seams::fmgr_info::call(typreceive)?;
            match raw {
                None => {
                    let v =
                        types_fmgr::receive_function_call(&mut finfo, None, typioparam, -1, mcx)?;
                    fcinfo.args[i].value = v;
                }
                Some(raw) => {
                    let mut abuf = StringInfo::with_capacity_in(mcx, raw.len() + 1)?;
                    abuf.append_bytes(raw)?;
                    let v = types_fmgr::receive_function_call(
                        &mut finfo,
                        Some(&mut abuf),
                        typioparam,
                        -1,
                        mcx,
                    )?;
                    // Trouble if it didn't eat the whole buffer.
                    if abuf.cursor != abuf.len() {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_INVALID_BINARY_REPRESENTATION)
                            .errmsg(format!(
                                "incorrect binary data format in function argument {}",
                                i + 1
                            ))
                            .into_error()
                            .into());
                    }
                    fcinfo.set_arg(i, v);
                }
            }
        } else {
            return Err(unsupported_format_code(aformat as i32));
        }
    }

    Ok(pqformat::pq_getmsgint(msg_buf, 2)? as i16)
}

fn client_to_server_cstring<'mcx>(mcx: Mcx<'mcx>, raw: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let mut v = match mbutils::pg_client_to_server(mcx, raw)? {
        Some(converted) => converted,
        None => {
            let mut v: PgVec<'mcx, u8> = PgVec::new_in(mcx);
            v.try_reserve_exact(raw.len() + 1)
                .map_err(|_| mcx.oom(raw.len() + 1))?;
            mcx::vec_append_bytes(&mut v, raw)?;
            v
        }
    };
    v.try_reserve_exact(1).map_err(|_| mcx.oom(1))?;
    v.push(0);
    Ok(v)
}
