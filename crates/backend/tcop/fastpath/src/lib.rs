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
    // C-parity fp_info field (fastpath.c); kept with the row image.
    #[allow(dead_code)]
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
            .with_funcname("fetch_fp_info")
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
            .with_funcname("fetch_fp_info")
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
    // fastpath.c:246 — the hook's ereport (ereport_on_violation) is the refusal.
    objectaccess::InvokeNamespaceSearchHook(fip.namespace, true)?;

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
    // fastpath.c:252
    objectaccess::InvokeFunctionExecuteHook(fid)?;

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
            // C parse_fcall_arguments: argsize != -1 ⇒ isnull=false before I/O.
            // LocalFcinfo::fresh starts every slot as NullableDatum::null().
            fcinfo.args[i].isnull = false;
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
            // The result may alias finfo's fn_extra scratch (fc_textin's
            // OutBuf, fc_namein's InScratch, ...), which is freed when `finfo`
            // drops at the end of this arm — long before the target function
            // runs, since HandleFunctionRequest invokes only after every
            // argument has been parsed. C is safe here because
            // OidInputFunctionCall pallocs its result in the surrounding
            // (message) context; mirror that by copying any by-ref datum into
            // `mcx` before `finfo` dies (the copy_param_datum discipline of
            // exec_bind_message).
            fcinfo.args[i].value = retain_arg_datum(mcx, v, raw.is_none(), fip.argtypes[i])?;
        } else if aformat == 1 {
            let (typreceive, typioparam) =
                lsyscache::typ::getTypeBinaryInputInfo(fip.argtypes[i])?;
            let mut finfo = fmgr_seams::fmgr_info::call(typreceive)?;
            match raw {
                None => {
                    let v =
                        types_fmgr::receive_function_call(&mut finfo, None, typioparam, -1, mcx)?;
                    fcinfo.args[i].value = retain_arg_datum(mcx, v, true, fip.argtypes[i])?;
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
                    // Every ported typreceive currently allocates its result in
                    // the caller's memory context, but that is an unenforced
                    // convention; copy here too so a future scratch-returning
                    // typreceive cannot reintroduce the use-after-free (finfo's
                    // scratch is freed when this arm ends, before invocation).
                    let v = retain_arg_datum(mcx, v, false, fip.argtypes[i])?;
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

/// Copy a by-ref input/receive-function result into `mcx` so it outlives the
/// per-argument `FmgrInfo` whose `fn_extra` scratch may back it (fc_textin's
/// OutBuf, fc_namein's InScratch, ...). By-value and NULL datums are returned
/// unchanged. This is `datumCopy` scoped to fastpath arguments — the same
/// discipline `exec_bind_message` uses via `copy_param_datum`, and the reason
/// C's palloc'd input-function results survive the drop of the stack-local
/// `FmgrInfo` used by `parse_fcall_arguments`.
fn retain_arg_datum<'mcx>(
    mcx: Mcx<'mcx>,
    value: Datum,
    is_null: bool,
    argtype: Oid,
) -> PgResult<Datum> {
    if is_null {
        return Ok(value);
    }
    let (typlen, typbyval) = lsyscache::typ::get_typlenbyval(argtype)?;
    if typbyval {
        return Ok(value);
    }
    datum_copy_in(mcx, value, typlen)
}

// datumCopy (datum.c) scoped to fastpath arguments — a mirror of
// exec_bind_message's datum_copy_in (extended_query.rs). By-ref sources are
// input/receive-function results (canonical 4B varlena today), but the -1 arm
// is C's VARSIZE_ANY so a future short/toast source copies, never misreads.
fn datum_copy_in<'mcx>(mcx: Mcx<'mcx>, value: Datum, typlen: i16) -> PgResult<Datum> {
    let p = value.as_usize() as *const u8;
    if p.is_null() {
        return Ok(Datum::null());
    }
    let size = match typlen {
        -1 => {
            // SAFETY: non-null by-ref varlena datum, readable for its
            // header-declared (VARSIZE_ANY) size.
            unsafe {
                let b0 = *p;
                if b0 == 0x01 {
                    2 + match *p.add(1) {
                        18 => 16,
                        1 => 8,
                        2 | 3 => panic!(
                            "datum_copy_in: expanded-object flatten (EOH_flatten_into) unported"
                        ),
                        tag => panic!("datum_copy_in: unknown vartag {tag}"),
                    }
                } else if b0 & 0x01 != 0 {
                    (b0 as usize >> 1) & 0x7F
                } else {
                    ::datum::VarlenaRef::from_ptr(p).varsize()
                }
            }
        }
        -2 => {
            let mut n = 0usize;
            // SAFETY: non-null NUL-terminated cstring datum.
            while unsafe { *p.add(n) } != 0 {
                n += 1;
            }
            n + 1
        }
        l => {
            debug_assert!(l > 0);
            l as usize
        }
    };
    // SAFETY: `size` bytes readable per the arms above.
    let src = unsafe { core::slice::from_raw_parts(p, size) };
    let out = mcx::slice_in(mcx, src)?;
    Ok(Datum::from_usize(out.leak().as_ptr() as usize))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;

    // A by-ref argument datum produced against a per-argument FmgrInfo scratch
    // must survive after that scratch is freed. retain_arg_datum copies it into
    // the surviving message context; the copy stays readable once the scratch
    // context is dropped, and it is a distinct allocation from the source.
    #[test]
    fn retain_arg_datum_copies_byref_across_scratch_drop() {
        let msg_ctx = MemoryContext::new("message");
        let msg = msg_ctx.mcx();

        let payload: [u8; 8] = [1, 2, 3, 4, 5, 6, 7, 8];
        let copied;
        {
            // Stand in for FmgrInfo fn_extra scratch: a fixed-length (typlen=8,
            // typbyval=false) by-ref datum living only for this block.
            let scratch_ctx = MemoryContext::new("fmgr-scratch");
            let scratch = mcx::slice_in(scratch_ctx.mcx(), &payload).unwrap();
            let d = Datum::from_usize(scratch.leak().as_ptr() as usize);

            // datum_copy_in exercised directly (retain_arg_datum's syscache
            // lookup needs a live catalog; the copy itself is the hazard fix).
            // typlen = 8 stands in for a fixed-length by-ref argument type.
            copied = datum_copy_in(msg, d, 8).unwrap();
            assert_ne!(copied.as_usize(), d.as_usize(), "copy must be a fresh chunk");
            // scratch_ctx drops here, freeing the source datum.
        }

        // SAFETY: `copied` lives in `msg`, which is still alive.
        let got = unsafe { core::slice::from_raw_parts(copied.as_usize() as *const u8, 8) };
        assert_eq!(got, &payload, "copied datum must retain its bytes");
    }
}

// fastpath.c:246/252: after the schema-usage and function-execute ACL checks,
// HandleFunctionRequest runs InvokeNamespaceSearchHook(fip->namespace, true)
// and InvokeFunctionExecuteHook(fid) so an object_access_hook consumer
// (sepgsql, audit modules) observes the fastpath invocation — and can refuse
// it — before the function body runs. Catalog access is faked at the syscache
// / fmgr seams; the ACL checks pass on the bootstrap-superuser short-circuit
// (aclchk.c pg_namespace_aclmask_ext / object_aclmask_ext), and the snapshot
// comes from the historic-snapshot slot so no procarray is needed.
#[cfg(test)]
mod oat_hook_tests {
    use super::*;
    use std::rc::Rc;
    use std::sync::{Mutex, Once};

    use mcx::MemoryContext;
    use objectaccess::{ObjectAccessArg, ObjectAccessType, OAT_FUNCTION_EXECUTE};
    use syscache_seams::PgProcShape;
    use types_core::BOOTSTRAP_SUPERUSERID;
    use types_fmgr::FunctionCallInfoBaseData;
    use types_snapshot::{SnapshotData, SnapshotType};
    use types_tuple::NameData;

    const FID: Oid = 61234;
    const NSP: Oid = 61235;
    const INT4OID: Oid = 23;

    static EVENTS: Mutex<Vec<String>> = Mutex::new(Vec::new());
    static SEAMS: Once = Once::new();

    fn log(event: String) {
        EVENTS.lock().unwrap().push(event);
    }

    fn drain() -> Vec<String> {
        std::mem::take(&mut *EVENTS.lock().unwrap())
    }

    // The function body: reached only once every pre-call step has passed.
    fn body(
        _flinfo: Option<&mut FmgrInfo>,
        _fcinfo: &mut FunctionCallInfoBaseData,
    ) -> PgResult<Datum> {
        log("BODY".to_string());
        Err(ereport(ERROR)
            .errmsg("fp witness: function body reached")
            .into_error()
            .into())
    }

    fn recording_hook(
        access: ObjectAccessType,
        class_id: Oid,
        object_id: Oid,
        sub_id: i32,
        arg: &mut ObjectAccessArg<'_>,
    ) -> PgResult<()> {
        let detail = match arg {
            ObjectAccessArg::NamespaceSearch(ns) => {
                format!(" ereport={}", ns.ereport_on_violation)
            }
            _ => String::new(),
        };
        log(format!("{access:?} class={class_id} obj={object_id} sub={sub_id}{detail}"));
        Ok(())
    }

    // sepgsql shape: the hook ereports on OAT_FUNCTION_EXECUTE.
    fn refusing_hook(
        access: ObjectAccessType,
        class_id: Oid,
        object_id: Oid,
        sub_id: i32,
        arg: &mut ObjectAccessArg<'_>,
    ) -> PgResult<()> {
        recording_hook(access, class_id, object_id, sub_id, arg)?;
        if access == OAT_FUNCTION_EXECUTE {
            return Err(ereport(ERROR)
                .errmsg("fp witness: hook refused")
                .into_error()
                .into());
        }
        Ok(())
    }

    fn proc_shape(funcid: Oid) -> PgResult<Option<PgProcShape>> {
        Ok((funcid == FID).then(|| PgProcShape {
            pronamespace: NSP,
            prorettype: INT4OID,
            provariadic: InvalidOid,
            prosupport: InvalidOid,
            prolang: 12,
            pronargs: 0,
            prokind: PROKIND_FUNCTION,
            provolatile: b'i' as i8,
            proparallel: b's' as i8,
            proretset: false,
            proisstrict: false,
            proleakproof: false,
            prosecdef: false,
            proconfig_isnull: true,
        }))
    }

    fn proc_name(funcid: Oid) -> PgResult<Option<NameData>> {
        let mut name = NameData { data: [0; 64] };
        name.namestrcpy("fp_oat_witness");
        Ok((funcid == FID).then_some(name))
    }

    fn proc_signature<'mcx>(
        mcx: Mcx<'mcx>,
        funcid: Oid,
    ) -> PgResult<Option<(Oid, PgVec<'mcx, Oid>)>> {
        Ok((funcid == FID).then(|| (INT4OID, PgVec::new_in(mcx))))
    }

    fn fmgr_info(funcid: Oid) -> PgResult<FmgrInfo> {
        Ok(FmgrInfo::new(body, funcid, 0, false, false))
    }

    fn install_seams() {
        SEAMS.call_once(|| {
            syscache_seams::lookup_pg_proc_shape::set(proc_shape);
            syscache_seams::pg_proc_proname::set(proc_name);
            syscache_seams::lookup_pg_proc_signature::set(proc_signature);
            fmgr_seams::fmgr_info::set(fmgr_info);
            xact_seams::get_current_transaction_nest_level::set(|| 1);
        });
    }

    // One 'F' message: fid, 0 argument formats, 0 arguments, text result.
    fn run_request() -> PgResult<bool> {
        let ctx = MemoryContext::new("fp-oat-msg");
        let mcx = ctx.mcx();
        let mut msg = StringInfo::new_in(mcx)?;
        msg.append_bytes(&FID.to_be_bytes())?;
        msg.append_bytes(&0u16.to_be_bytes())?;
        msg.append_bytes(&0u16.to_be_bytes())?;
        msg.append_bytes(&0u16.to_be_bytes())?;
        msg.cursor = 0;
        HandleFunctionRequest(mcx, &mut msg)
    }

    fn namespace_search_event() -> String {
        format!("OAT_NAMESPACE_SEARCH class={NAMESPACE_RELATION_ID} obj={NSP} sub=0 ereport=true")
    }

    fn function_execute_event() -> String {
        format!("OAT_FUNCTION_EXECUTE class={PROCEDURE_RELATION_ID} obj={FID} sub=0")
    }

    #[test]
    fn handle_function_request_runs_object_access_hooks_before_the_body() {
        install_seams();
        miscinit::SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, 0);
        let snap_ctx: &'static MemoryContext = mcx::session_root("fp-oat-snap");
        snapmgr::SetupHistoricSnapshot(
            Rc::new(SnapshotData::sentinel(snap_ctx.mcx(), SnapshotType::SNAPSHOT_MVCC)),
            None,
        );
        drain();

        // A recording hook sees OAT_NAMESPACE_SEARCH(namespace, ereport=true)
        // then OAT_FUNCTION_EXECUTE(fid), in fastpath.c order, before the body.
        let prev = objectaccess::set_object_access_hook(Some(recording_hook));
        assert!(prev.is_none(), "another hook is installed on this thread");
        let err = run_request().expect_err("the witness body always errors");
        assert_eq!(err.message(), "fp witness: function body reached");
        assert_eq!(
            drain(),
            vec![namespace_search_event(), function_execute_event(), "BODY".to_string()]
        );

        // A hook that ereports on OAT_FUNCTION_EXECUTE stops the call: the
        // hook's error is what HandleFunctionRequest returns and the body
        // never runs.
        objectaccess::set_object_access_hook(Some(refusing_hook));
        let err = run_request().expect_err("the refusing hook errors");
        assert_eq!(err.message(), "fp witness: hook refused");
        assert_eq!(drain(), vec![namespace_search_event(), function_execute_event()]);

        objectaccess::set_object_access_hook(None);
        snapmgr::TeardownHistoricSnapshot(false);
    }
}
