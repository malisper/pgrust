//! `fastpath.c` — routines to handle function requests from the frontend
//! (`src/backend/tcop/fastpath.c`, PostgreSQL 18.3). This is the server side
//! of `PQfn`, the libpq fast-path function-call interface (protocol symbol
//! "F").
//!
//! The request-handling spine is in-crate: every line of control flow, every
//! constant, every wire-protocol step ordering, every [`FpInfo`] field write
//! and every `ereport`/`elog` message + SQLSTATE is reproduced here. The C
//! `struct fp_info` is private to fastpath.c; here it is the crate-local
//! [`FpInfo`].
//!
//! Everything below the spine is owned by other subsystems and reached through
//! a direct dependency (libpq `pqformat`, which is a leaf) or the owner's seam
//! crate (the syscache `pg_proc` lookup, fmgr resolution/dispatch + type I/O,
//! ACL checks, the object-access hooks, transaction/snapshot management,
//! statement/duration logging, encoding conversion, and the per-call interrupt
//! check). A seam panics until its owner lands — correct mirror-PG behaviour.
//!
//! The message buffers are `mcx`-backed [`StringInfo`] values (mutated by
//! `&mut`, advancing their `cursor`); the per-argument scratch buffer is a
//! `StringInfo` charged to the same `mcx`. fastpath runs in `PostgresMain`'s
//! `MessageContext`, which is reset on return, so nothing here needs explicit
//! `pfree`.
//!
//! C function inventory:
//!   * `struct fp_info`        (fastpath.c:48)  — IN-CRATE owned state ([`FpInfo`])
//!   * `SendFunctionResult`    (fastpath.c:66)  — [`send_function_result`]
//!   * `fetch_fp_info`         (fastpath.c:118) — [`fetch_fp_info`]
//!   * `HandleFunctionRequest` (fastpath.c:188) — [`handle_function_request`] (public)
//!   * `parse_fcall_arguments` (fastpath.c:328) — [`parse_fcall_arguments`]

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use ::utils_error::ereport;
use ::mcx::Mcx;
use ::types_acl::acl::{AclResult, ACLCHECK_OK, ACL_EXECUTE, ACL_USAGE};
use ::types_core::catalog::{NAMESPACE_RELATION_ID, PROCEDURE_RELATION_ID};
use ::types_core::primitive::{InvalidOid, Oid, FUNC_MAX_ARGS};
use types_tuple::heaptuple::Datum as CanonDatum;
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_INVALID_BINARY_REPRESENTATION, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_IN_FAILED_SQL_TRANSACTION, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_OUT_OF_MEMORY,
    ERRCODE_PROTOCOL_VIOLATION, ERRCODE_UNDEFINED_FUNCTION, ERROR, LOG,
};
use ::nodes::parsenodes::ObjectType;
use ::stringinfo::StringInfo;

use transam_xact_seams as xact_seam;
use aclchk_seams as aclchk_seam;
use objectaccess_seams as objaccess_seam;
use pqformat as pqformat;
use postgres_seams as tcop_seam;
use lsyscache_seams as lsyscache_seam;
use syscache_seams as syscache_seam;
use fmgr_seams as fmgr_seam;
use miscinit_seams as miscinit_seam;
use mbutils_seams as mbutils_seam;
use snapmgr_seams as snapmgr_seam;

use ::types_core::fmgr::FmgrInfo;

/// `PqMsg_FunctionCallResponse` (`libpq/protocol.h`) — the message-type byte
/// of the fast-path function-call reply.
const PqMsg_FunctionCallResponse: u8 = b'V';

/// `PROKIND_FUNCTION` (`catalog/pg_proc.h`) — `prokind` for a plain function.
const PROKIND_FUNCTION: i8 = b'f' as i8;

/// `NAMEDATALEN` (`pg_config_manual.h`).
const NAMEDATALEN: usize = 64;

fn loc(line: i32, func: &'static str) -> ErrorLocation {
    ErrorLocation::new("fastpath.c", line, func)
}

/// `struct fp_info` (fastpath.c:48-56) — function and type info looked up by
/// [`fetch_fp_info`]; private to fastpath.c and built field-by-field here.
///
/// The validity of this structure is determined by whether `funcid` is OK;
/// [`fetch_fp_info`] clears `funcid` first and only sets it once a good struct
/// is ready to return (it can be interrupted by `ereport(ERROR)` at any point).
#[derive(Clone, Debug)]
pub struct FpInfo {
    /// `Oid funcid`.
    pub funcid: Oid,
    /// `FmgrInfo flinfo` — function lookup info for `funcid`.
    pub flinfo: FmgrInfo,
    /// `Oid namespace` — other stuff from `pg_proc`.
    pub namespace: Oid,
    /// `Oid rettype`.
    pub rettype: Oid,
    /// `Oid argtypes[FUNC_MAX_ARGS]` (only the first `pronargs` are written).
    pub argtypes: [Oid; FUNC_MAX_ARGS],
    /// `char fname[NAMEDATALEN]` rendered as the (NUL-trimmed) function name
    /// for logging / error messages.
    pub fname: String,
}

impl FpInfo {
    /// `MemSet(fip, 0, sizeof(struct fp_info))` (fastpath.c:134) — a fully
    /// zeroed `fp_info` (the `FmgrInfo` sub-object is likewise all-zero).
    fn zeroed() -> Self {
        Self {
            funcid: 0,
            flinfo: FmgrInfo::empty(),
            namespace: 0,
            rettype: 0,
            argtypes: [0; FUNC_MAX_ARGS],
            fname: String::new(),
        }
    }
}

/// `SendFunctionResult` (fastpath.c:66-110) — build and send the
/// `PqMsg_FunctionCallResponse` message for the (possibly null) return value in
/// the requested wire `format`.
pub fn send_function_result<'mcx>(
    mcx: Mcx<'mcx>,
    retval: &CanonDatum<'_>,
    isnull: bool,
    rettype: Oid,
    format: i16,
) -> PgResult<()> {
    // pq_beginmessage(&buf, PqMsg_FunctionCallResponse);
    let mut buf = pqformat::pq_beginmessage(mcx, PqMsg_FunctionCallResponse)?;

    if isnull {
        // pq_sendint32(&buf, -1);
        pqformat::pq_sendint32(&mut buf, (-1_i32) as u32)?;
    } else if format == 0 {
        // getTypeOutputInfo(rettype, &typoutput, &typisvarlena);
        let (typoutput, _typisvarlena) = lsyscache_seam::get_type_output_info::call(rettype)?;
        // outputstr = OidOutputFunctionCall(typoutput, retval);
        let outputstr = fmgr_seam::fastpath_output_function_call::call(mcx, typoutput, retval)?;
        // pq_sendcountedtext(&buf, outputstr, strlen(outputstr));
        pqformat::pq_sendcountedtext(&mut buf, &outputstr)?;
        // pfree(outputstr);  — the owned bytes are dropped at end of scope.
    } else if format == 1 {
        // getTypeBinaryOutputInfo(rettype, &typsend, &typisvarlena);
        let (typsend, _typisvarlena) = lsyscache_seam::get_type_binary_output_info::call(rettype)?;
        // outputbytes = OidSendFunctionCall(typsend, retval);
        // The seam returns exactly the VARSIZE(outputbytes) - VARHDRSZ payload.
        let outputbytes = fmgr_seam::fastpath_send_function_call::call(mcx, typsend, retval)?;
        // pq_sendint32(&buf, VARSIZE(outputbytes) - VARHDRSZ);
        pqformat::pq_sendint32(&mut buf, outputbytes.len() as u32)?;
        // pq_sendbytes(&buf, VARDATA(outputbytes), VARSIZE(outputbytes) - VARHDRSZ);
        pqformat::pq_sendbytes(&mut buf, &outputbytes)?;
        // pfree(outputbytes);  — the owned bytes are dropped at end of scope.
    } else {
        // ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
        //                 errmsg("unsupported format code: %d", format)));
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("unsupported format code: {format}"))
            .into_error());
    }

    // pq_endmessage(&buf);
    pqformat::pq_endmessage(buf)?;
    Ok(())
}

/// `fetch_fp_info` (fastpath.c:118-169) — perform the catalog lookups to load
/// an [`FpInfo`] for `func_id`. The owned `fip` is mutated in place, matching
/// the C `fetch_fp_info(func_id, fip)` out-parameter discipline.
pub fn fetch_fp_info<'mcx>(mcx: Mcx<'mcx>, func_id: Oid, fip: &mut FpInfo) -> PgResult<()> {
    /*
     * Since the validity of this structure is determined by whether the funcid
     * is OK, we clear the funcid here.  It must not be set to the correct value
     * until we are about to return with a good struct fp_info, since we can be
     * interrupted (i.e., with an ereport(ERROR, ...)) at any time.
     */
    // MemSet(fip, 0, sizeof(struct fp_info));
    *fip = FpInfo::zeroed();
    // fip->funcid = InvalidOid;
    fip.funcid = InvalidOid;

    // func_htp = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_id));
    // if (!HeapTupleIsValid(func_htp)) ereport(ERROR, ...);
    // pp = (Form_pg_proc) GETSTRUCT(func_htp);
    let pp = match syscache_seam::search_pg_proc_fastpath::call(mcx, func_id)? {
        Some(pp) => pp,
        None => {
            // ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
            //                 errmsg("function with OID %u does not exist", func_id)));
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_FUNCTION)
                .errmsg(format!("function with OID {func_id} does not exist"))
                .into_error());
        }
    };

    // /* reject pg_proc entries that are unsafe to call via fastpath */
    // if (pp->prokind != PROKIND_FUNCTION || pp->proretset)
    if pp.prokind != PROKIND_FUNCTION || pp.proretset {
        // ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        //   errmsg("cannot call function \"%s\" via fastpath interface",
        //          NameStr(pp->proname))));
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "cannot call function \"{}\" via fastpath interface",
                pp.proname.as_str()
            ))
            .into_error());
    }

    // /* watch out for catalog entries with more than FUNC_MAX_ARGS args */
    // if (pp->pronargs > FUNC_MAX_ARGS)
    if (pp.pronargs as i32) > FUNC_MAX_ARGS as i32 {
        // elog(ERROR, "function %s has more than %d arguments",
        //      NameStr(pp->proname), FUNC_MAX_ARGS);
        return Err(ereport(ERROR)
            .errmsg_internal(format!(
                "function {} has more than {FUNC_MAX_ARGS} arguments",
                pp.proname.as_str()
            ))
            .into_error());
    }

    // fip->namespace = pp->pronamespace;
    fip.namespace = pp.pronamespace;
    // fip->rettype = pp->prorettype;
    fip.rettype = pp.prorettype;
    // memcpy(fip->argtypes, pp->proargtypes.values, pp->pronargs * sizeof(Oid));
    let pronargs = pp.pronargs as usize;
    for i in 0..pronargs {
        fip.argtypes[i] = pp.proargtypes[i];
    }
    // strlcpy(fip->fname, NameStr(pp->proname), NAMEDATALEN);
    fip.fname = strlcpy_name(pp.proname.as_str());

    // ReleaseSysCache(func_htp);  — owned by the seam installer.

    // fmgr_info(func_id, &fip->flinfo);
    fip.flinfo = fmgr_seam::fmgr_info::call(mcx, func_id)?;

    // /* This must be last! */
    // fip->funcid = func_id;
    fip.funcid = func_id;
    Ok(())
}

/// `HandleFunctionRequest` (fastpath.c:188-320) — the public entry: read the
/// fastpath function-call message, look up the function, parse the arguments,
/// invoke the function, and send the result. `msg_buf` is positioned at the
/// function-call message body (passed by `&mut` so the sequential `pq_getmsg*`
/// reads advance its cursor).
pub fn handle_function_request<'mcx>(
    mcx: Mcx<'mcx>,
    msg_buf: &mut StringInfo<'mcx>,
) -> PgResult<()> {
    // LOCAL_FCINFO(fcinfo, FUNC_MAX_ARGS); -- the owned arg vector is built in
    // parse_fcall_arguments once we know nargs.
    let fid: Oid;
    let aclresult: AclResult;
    let rformat: i16;
    let retval: CanonDatum<'mcx>;
    // struct fp_info my_fp;  struct fp_info *fip;
    let mut fip = FpInfo::zeroed();
    let callit: bool;
    let mut was_logged = false;
    // bool fcinfo->isnull
    let isnull: bool;

    /*
     * We only accept COMMIT/ABORT if we are in an aborted transaction, and
     * COMMIT/ABORT cannot be executed through the fastpath interface.
     */
    // if (IsAbortedTransactionBlockState())
    if xact_seam::is_aborted_transaction_block_state::call() {
        // ereport(ERROR, (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
        //   errmsg("current transaction is aborted, "
        //          "commands ignored until end of transaction block")));
        return Err(ereport(ERROR)
            .errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION)
            .errmsg(
                "current transaction is aborted, \
                 commands ignored until end of transaction block",
            )
            .into_error());
    }

    /*
     * Now that we know we are in a valid transaction, set snapshot in case
     * needed by function itself or one of the datatype I/O routines.
     */
    // PushActiveSnapshot(GetTransactionSnapshot());
    snapmgr_seam::push_active_snapshot_transaction::call()?;

    // /* Begin parsing the buffer contents. */
    // fid = (Oid) pq_getmsgint(msgBuf, 4);  /* function oid */
    fid = pqformat::pq_getmsgint(msg_buf, 4)? as Oid;

    /*
     * There used to be a lame attempt at caching lookup info here. Now we just
     * do the lookups on every call.
     */
    // fip = &my_fp;
    // fetch_fp_info(fid, fip);
    fetch_fp_info(mcx, fid, &mut fip)?;

    // /* Log as soon as we have the function OID and name */
    // if (log_statement == LOGSTMT_ALL)
    if tcop_seam::log_statement_is_all::call() {
        // ereport(LOG, (errmsg("fastpath function call: \"%s\" (OID %u)",
        //                      fip->fname, fid)));
        ereport(LOG)
            .errmsg(format!(
                "fastpath function call: \"{}\" (OID {fid})",
                fip.fname
            ))
            .finish(loc(233, "HandleFunctionRequest"))?;
        // was_logged = true;
        was_logged = true;
    }

    /*
     * Check permission to access and call function.  Since we didn't go through
     * a normal name lookup, we need to check schema usage too.
     */
    // aclresult = object_aclcheck(NamespaceRelationId, fip->namespace,
    //                             GetUserId(), ACL_USAGE);
    aclresult = aclchk_seam::object_aclcheck::call(
        NAMESPACE_RELATION_ID,
        fip.namespace,
        miscinit_seam::get_user_id::call(),
        ACL_USAGE,
    )?;
    // if (aclresult != ACLCHECK_OK)
    if aclresult != ACLCHECK_OK {
        // aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(fip->namespace));
        let nspname = lsyscache_seam::get_namespace_name::call(mcx, fip.namespace)?
            .map(|s| s.as_str().to_string());
        aclchk_seam::aclcheck_error::call(aclresult, ObjectType::Schema, nspname)?;
    }
    // InvokeNamespaceSearchHook(fip->namespace, true);
    objaccess_seam::invoke_namespace_search_hook::call(fip.namespace, true)?;

    // aclresult = object_aclcheck(ProcedureRelationId, fid, GetUserId(),
    //                             ACL_EXECUTE);
    let aclresult = aclchk_seam::object_aclcheck::call(
        PROCEDURE_RELATION_ID,
        fid,
        miscinit_seam::get_user_id::call(),
        ACL_EXECUTE,
    )?;
    // if (aclresult != ACLCHECK_OK)
    if aclresult != ACLCHECK_OK {
        // aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(fid));
        let funcname =
            lsyscache_seam::get_func_name::call(mcx, fid)?.map(|s| s.as_str().to_string());
        aclchk_seam::aclcheck_error::call(aclresult, ObjectType::Function, funcname)?;
    }
    // InvokeFunctionExecuteHook(fid);
    objaccess_seam::invoke_function_execute_hook::call(fid)?;

    /*
     * Prepare function call info block and insert arguments.
     *
     * Note: for now we pass collation = InvalidOid, so collation-sensitive
     * functions can't be called this way.
     */
    // InitFunctionCallInfoData(*fcinfo, &fip->flinfo, 0, InvalidOid, NULL, NULL);
    // The owned arg vector and its null flags are populated by
    // parse_fcall_arguments; nargs is established there. Values cross the fmgr
    // boundary as the canonical `Datum<'mcx>` (a by-reference argument — the
    // `text` arg of `lo_import` — survives as `ByRef`); `args_null[i]` carries
    // `fcinfo->args[i].isnull` alongside.
    let mut args: Vec<CanonDatum<'mcx>> = Vec::new();
    let mut args_null: Vec<bool> = Vec::new();

    // rformat = parse_fcall_arguments(msgBuf, fip, fcinfo);
    rformat = parse_fcall_arguments(mcx, msg_buf, &fip, &mut args, &mut args_null)?;

    // /* Verify we reached the end of the message where expected. */
    // pq_getmsgend(msgBuf);
    pqformat::pq_getmsgend(msg_buf)?;

    /*
     * If func is strict, must not call it for null args.
     */
    // callit = true;
    callit = {
        let mut call = true;
        // if (fip->flinfo.fn_strict)
        if fip.flinfo.fn_strict {
            // for (i = 0; i < fcinfo->nargs; i++)
            for &arg_null in &args_null {
                // if (fcinfo->args[i].isnull) { callit = false; break; }
                if arg_null {
                    call = false;
                    break;
                }
            }
        }
        call
    };

    // if (callit)
    if callit {
        // /* Okay, do it ... */
        // retval = FunctionCallInvoke(fcinfo);
        let (v, callee_isnull) = fmgr_seam::fastpath_function_call_invoke::call(
            mcx,
            fip.funcid,
            InvalidOid,
            &args,
            &args_null,
        )?;
        retval = v;
        isnull = callee_isnull;
    } else {
        // fcinfo->isnull = true;
        isnull = true;
        // retval = (Datum) 0;
        retval = CanonDatum::ByVal(0);
    }

    // /* ensure we do at least one CHECK_FOR_INTERRUPTS per function call */
    // CHECK_FOR_INTERRUPTS();
    tcop_seam::check_for_interrupts::call()?;

    // SendFunctionResult(retval, fcinfo->isnull, fip->rettype, rformat);
    send_function_result(mcx, &retval, isnull, fip.rettype, rformat)?;

    // /* We no longer need the snapshot */
    // PopActiveSnapshot();
    snapmgr_seam::pop_active_snapshot::call()?;

    /*
     * Emit duration logging if appropriate.
     */
    // switch (check_log_duration(msec_str, was_logged))
    let (code, msec_str) = tcop_seam::check_log_duration::call(mcx, was_logged)?;
    match code {
        1 => {
            // ereport(LOG, (errmsg("duration: %s ms", msec_str)));
            ereport(LOG)
                .errmsg(format!("duration: {} ms", msec_str.as_str()))
                .finish(loc(312, "HandleFunctionRequest"))?;
        }
        2 => {
            // ereport(LOG, (errmsg("duration: %s ms  fastpath function call:
            //   \"%s\" (OID %u)", msec_str, fip->fname, fid)));
            ereport(LOG)
                .errmsg(format!(
                    "duration: {} ms  fastpath function call: \"{}\" (OID {fid})",
                    msec_str.as_str(),
                    fip.fname
                ))
                .finish(loc(316, "HandleFunctionRequest"))?;
        }
        _ => {}
    }

    Ok(())
}

/// `parse_fcall_arguments` (fastpath.c:328-458) — parse the argument values
/// from `msg_buf` into `args`, applying the requested per-argument formats;
/// returns the requested result format code.
pub fn parse_fcall_arguments<'mcx>(
    mcx: Mcx<'mcx>,
    msg_buf: &mut StringInfo<'mcx>,
    fip: &FpInfo,
    args: &mut Vec<CanonDatum<'mcx>>,
    args_null: &mut Vec<bool>,
) -> PgResult<i16> {
    let nargs: i32;
    let numAFormats: i32;
    // `aformats` is the C `palloc(numAFormats * sizeof(int16))`, empty when
    // `numAFormats <= 0` (C: the NULL pointer).
    let mut aformats: Vec<i16> = Vec::new();
    // StringInfoData abuf;
    let mut abuf = StringInfo::new_in(mcx);

    // /* Get the argument format codes */
    // numAFormats = pq_getmsgint(msgBuf, 2);
    // C's pq_getmsgint(2) returns an `unsigned int` (the uint16 zero-extended),
    // assigned to `int numAFormats` — so the value is 0..65535, never negative.
    numAFormats = pqformat::pq_getmsgint(msg_buf, 2)? as i32;
    // if (numAFormats > 0)
    if numAFormats > 0 {
        // aformats = (int16 *) palloc(numAFormats * sizeof(int16));
        aformats
            .try_reserve(numAFormats as usize)
            .map_err(|_| out_of_memory("function call argument formats"))?;
        // for (i = 0; i < numAFormats; i++)
        for _ in 0..numAFormats {
            // aformats[i] = pq_getmsgint(msgBuf, 2);
            aformats.push(pqformat::pq_getmsgint(msg_buf, 2)? as i16);
        }
    }

    // nargs = pq_getmsgint(msgBuf, 2);  /* # of arguments */
    // Zero-extended into C's `int nargs` (0..65535); the > FUNC_MAX_ARGS guard
    // below rejects anything beyond 100.
    nargs = pqformat::pq_getmsgint(msg_buf, 2)? as i32;

    // if (fip->flinfo.fn_nargs != nargs || nargs > FUNC_MAX_ARGS)
    if (fip.flinfo.fn_nargs as i32) != nargs || nargs > FUNC_MAX_ARGS as i32 {
        // ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
        //   errmsg("function call message contains %d arguments but function
        //           requires %d", nargs, fip->flinfo.fn_nargs)));
        let requires = fip.flinfo.fn_nargs;
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!(
                "function call message contains {nargs} arguments but function requires {requires}"
            ))
            .into_error());
    }

    // fcinfo->nargs = nargs;  — the owned arg vector is sized to nargs. Each
    // slot is the canonical `Datum<'mcx>` plus its `fcinfo->args[i].isnull`.
    args.try_reserve(nargs as usize)
        .map_err(|_| out_of_memory("function call arguments"))?;
    args_null
        .try_reserve(nargs as usize)
        .map_err(|_| out_of_memory("function call arguments"))?;
    args.clear();
    args_null.clear();
    for _ in 0..nargs {
        args.push(CanonDatum::ByVal(0));
        args_null.push(false);
    }

    // if (numAFormats > 1 && numAFormats != nargs)
    if numAFormats > 1 && numAFormats != nargs {
        // ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
        //   errmsg("function call message contains %d argument formats but %d
        //           arguments", numAFormats, nargs)));
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!(
                "function call message contains {numAFormats} argument formats but {nargs} arguments"
            ))
            .into_error());
    }

    // initStringInfo(&abuf);  — abuf is already empty.

    /*
     * Copy supplied arguments into arg vector.
     */
    // for (i = 0; i < nargs; ++i)
    for i in 0..nargs as usize {
        let argsize: i32;
        let aformat: i16;

        // argsize = pq_getmsgint(msgBuf, 4);
        argsize = pqformat::pq_getmsgint(msg_buf, 4)? as i32;
        // if (argsize == -1)
        if argsize == -1 {
            // fcinfo->args[i].isnull = true;
            args_null[i] = true;
        } else {
            // fcinfo->args[i].isnull = false;
            args_null[i] = false;
            // if (argsize < 0)
            if argsize < 0 {
                // ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                //   errmsg("invalid argument size %d in function call message",
                //          argsize)));
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg(format!(
                        "invalid argument size {argsize} in function call message"
                    ))
                    .into_error());
            }

            // /* Reset abuf to empty, and insert raw data into it */
            // resetStringInfo(&abuf);
            abuf.reset();
            // appendBinaryStringInfo(&abuf,
            //                        pq_getmsgbytes(msgBuf, argsize),
            //                        argsize);
            let raw = pqformat::pq_getmsgbytes(msg_buf, argsize as usize)?;
            abuf.data
                .try_reserve(raw.len())
                .map_err(|_| out_of_memory("function call argument data"))?;
            abuf.data.extend_from_slice(raw);
        }

        // if (numAFormats > 1) aformat = aformats[i];
        // else if (numAFormats > 0) aformat = aformats[0];
        // else aformat = 0;  /* default = text */
        if numAFormats > 1 {
            aformat = aformats[i];
        } else if numAFormats > 0 {
            aformat = aformats[0];
        } else {
            aformat = 0;
        }

        // if (aformat == 0)
        if aformat == 0 {
            // getTypeInputInfo(fip->argtypes[i], &typinput, &typioparam);
            let (typinput, typioparam) = lsyscache_seam::get_type_input_info::call(fip.argtypes[i])?;

            /*
             * Since stringinfo.c keeps a trailing null in place even for binary
             * data, the contents of abuf are a valid C string.  We have to do
             * encoding conversion before calling the typinput routine, though.
             */
            // if (argsize == -1) pstring = NULL;
            // else pstring = pg_client_to_server(abuf.data, argsize);
            //
            // The owned conversion buffer must outlive the borrow handed to the
            // input seam, so it is bound here. `None` from the conversion seam
            // is C's "no conversion needed" (the returned pointer == abuf.data),
            // so we fall back to the raw abuf bytes.
            let converted = if argsize == -1 {
                None
            } else {
                mbutils_seam::pg_client_to_server::call(mcx, abuf.as_bytes())?
            };
            let pstring: Option<&str> = if argsize == -1 {
                // C: pstring = NULL
                None
            } else {
                let bytes: &[u8] = match &converted {
                    Some(c) => c.as_slice(),
                    None => abuf.as_bytes(),
                };
                Some(bytes_as_str(bytes)?)
            };

            // fcinfo->args[i].value =
            //   OidInputFunctionCall(typinput, pstring, typioparam, -1);
            args[i] =
                fmgr_seam::fastpath_input_function_call::call(mcx, typinput, pstring, typioparam, -1)?;
            // /* Free result of encoding conversion, if any */ — owned buffer
            // dropped at end of iteration.
        } else if aformat == 1 {
            // getTypeBinaryInputInfo(fip->argtypes[i], &typreceive, &typioparam);
            let (typreceive, typioparam) =
                lsyscache_seam::get_type_binary_input_info::call(fip.argtypes[i])?;

            // if (argsize == -1) bufptr = NULL; else bufptr = &abuf;
            // fcinfo->args[i].value =
            //   OidReceiveFunctionCall(typreceive, bufptr, typioparam, -1);
            //
            // C's "Trouble if it didn't eat the whole buffer" check
            // (`abuf.cursor != abuf.len` → ERRCODE_INVALID_BINARY_REPRESENTATION
            // "incorrect binary data format in function argument %d") is enforced
            // inside the typed receive helper: the receive function reads the
            // supplied slice through its `StringInfo` and a trailing
            // `pq_getmsgend` fails if bytes remain — the same model `record_recv`
            // uses (the helper does not surface a separate bytes-consumed count).
            let value = if argsize == -1 {
                fmgr_seam::fastpath_receive_function_call::call(mcx, typreceive, None, typioparam, -1)?
            } else {
                fmgr_seam::fastpath_receive_function_call::call(
                    mcx,
                    typreceive,
                    Some(abuf.as_bytes()),
                    typioparam,
                    -1,
                )?
            };
            args[i] = value;
        } else {
            // ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            //   errmsg("unsupported format code: %d", aformat)));
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!("unsupported format code: {aformat}"))
                .into_error());
        }
    }

    // /* Return result format code */
    // return (int16) pq_getmsgint(msgBuf, 2);
    Ok(pqformat::pq_getmsgint(msg_buf, 2)? as i16)
}

/// This crate owns no inward seams: its sole public entry,
/// [`handle_function_request`], is called directly by `tcop/postgres.c`
/// (`PostgresMain`'s message loop), which depends on this crate, so no
/// cycle-breaking seam is needed. `init_seams()` is therefore empty.
pub fn init_seams() {}

/// `strlcpy(dst, NameStr(name), NAMEDATALEN)` (fastpath.c:159): copy the name
/// truncated to `NAMEDATALEN - 1` bytes. The catalog `proname` always fits, so
/// this is the identity in practice; the truncation is reproduced for fidelity.
fn strlcpy_name(name: &str) -> String {
    let max = NAMEDATALEN - 1;
    if name.len() <= max {
        name.to_string()
    } else {
        // Truncate on a char boundary at or below the byte limit.
        let mut end = max;
        while end > 0 && !name.is_char_boundary(end) {
            end -= 1;
        }
        name[..end].to_string()
    }
}

/// The abuf bytes, viewed as the valid C string `OidInputFunctionCall`
/// receives (stringinfo.c keeps a trailing NUL, so the C bytes are a valid C
/// string). A genuinely non-UTF-8 client image would be a `pg_client_to_server`
/// concern; here it surfaces as a protocol-violation error rather than a panic.
fn bytes_as_str(bytes: &[u8]) -> PgResult<&str> {
    core::str::from_utf8(bytes).map_err(|_| {
        ereport(ERROR)
            .errcode(ERRCODE_INVALID_BINARY_REPRESENTATION)
            .errmsg_internal("invalid byte sequence in function call argument".to_string())
            .into_error()
    })
}

/// `palloc` OOM (`ereport(ERROR, errcode(ERRCODE_OUT_OF_MEMORY), ...)`) for a
/// failed owned reservation — C's `palloc` raises `out of memory` on failure.
fn out_of_memory(what: &str) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_OUT_OF_MEMORY)
        .errmsg_internal(format!("out of memory allocating {what}"))
        .into_error()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strlcpy_name_passes_short_names_through() {
        assert_eq!(strlcpy_name("int4eq"), "int4eq");
        assert_eq!(strlcpy_name(""), "");
    }

    #[test]
    fn strlcpy_name_truncates_to_namedatalen_minus_one() {
        // A name longer than NAMEDATALEN-1 is truncated to NAMEDATALEN-1 bytes
        // (the catalog never produces these; reproduced for fidelity).
        let long = "a".repeat(100);
        let out = strlcpy_name(&long);
        assert_eq!(out.len(), NAMEDATALEN - 1);
        assert!(out.bytes().all(|b| b == b'a'));
    }

    #[test]
    fn fp_info_zeroed_is_all_zero() {
        let fp = FpInfo::zeroed();
        assert_eq!(fp.funcid, 0);
        assert_eq!(fp.namespace, 0);
        assert_eq!(fp.rettype, 0);
        assert!(fp.fname.is_empty());
        assert!(fp.argtypes.iter().all(|&a| a == 0));
        assert!(!fp.flinfo.fn_strict);
        assert_eq!(fp.flinfo.fn_nargs, 0);
    }

    #[test]
    fn bytes_as_str_rejects_invalid_utf8() {
        assert!(bytes_as_str(b"valid").is_ok());
        assert!(bytes_as_str(&[0xff, 0xfe]).is_err());
    }
}
