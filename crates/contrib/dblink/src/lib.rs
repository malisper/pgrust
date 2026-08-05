//! `contrib/dblink/dblink.c` — run queries on OTHER postgres servers from
//! within SQL. dblink is a libpq CLIENT embedded in the backend; here it rides
//! the in-tree wire client `pgclient` (crates/interfaces/pgclient, the seam
//! postgres_fdw will also use — docs/design/pgclient-seam.md).
//!
//! Per-backend connection state (C globals -> thread_locals; one backend = one
//! thread) lives in `registry`; local-catalog helpers in `catalog`; remote
//! result -> tuplestore in `materialize`; FDW option validation in `fdw`.

mod catalog;
mod fdw;
mod materialize;
mod registry;

use datum::Datum;
use elog::ereport;
use pgclient::{ExecStatus, PgConn, QueryResult};
use registry::RemoteConn;
use types_error::{
    make_sqlstate, ErrorLevel, ErrorLocation, PgError, PgResult, ERROR, NOTICE,
    ERRCODE_CONNECTION_FAILURE, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_CURSOR_NAME,
    ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED,
    ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_tuple::TupleDescData;

const LIBRARY: &str = "dblink";

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

fn throw<T>(r: PgResult<()>) -> PgResult<T> {
    match r {
        Ok(()) => unreachable!("throw with non-error"),
        Err(e) => Err(e),
    }
}

fn pchomp(s: &str) -> String {
    s.trim_end_matches('\n').to_string()
}

pub(crate) fn text_result(mcx: mcx::Mcx<'_>, s: &str) -> PgResult<Datum> {
    Ok(types_fmgr::varlena_result(varlena::cstring_to_text(mcx, s.as_bytes())?))
}

// C's `PG_GETARG_TEXT_PP` + text_to_cstring, as an owned String.
fn arg_text(fcinfo: &Fcinfo, i: usize) -> PgResult<String> {
    // SAFETY: strict text arg.
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    Ok(String::from_utf8_lossy(v.data()).into_owned())
}

fn arg_is_bool(flinfo: Option<&FmgrInfo>, i: usize) -> bool {
    funcapi::get_fn_expr_argtype(flinfo, i) == types_core::BOOLOID
}

// prepTuplestoreResult (dblink.c): every tuplestore-returning entry point
// arms materialize mode BEFORE touching the connection, so the fail=false and
// results-exhausted arms return an EMPTY set with setResult left None (C's
// "caller must fill these to return a non-empty result"). Without the arming,
// the executor's ValuePerCall arm stores the placeholder datum as a row —
// the fleet-gate r2/r3 abort at dblink_fetch(...,false) / dblink_get_result
// after cancel.
fn prep_tuplestore_result(fcinfo: &mut Fcinfo) -> PgResult<()> {
    let Some(rsi) = fcinfo.rsinfo_mut() else {
        return Err(Box::new(
            PgError::error("set-valued function called in context that cannot accept a set")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    };
    if rsi.allowedModes & types_fmgr::SFRM_Materialize == 0 {
        return Err(Box::new(
            PgError::error("materialize mode required, but it is not allowed in this context")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    rsi.returnMode = types_fmgr::SetFunctionReturnMode::Materialize;
    rsi.setResult = None;
    rsi.setDesc = None;
    Ok(())
}

pub(crate) fn single_text_tupdesc<'m>(mcx: mcx::Mcx<'m>, name: &str) -> PgResult<TupleDescData<'m>> {
    let mut td = tupdesc::CreateTemplateTupleDesc(mcx, 1)?;
    tupdesc::TupleDescInitEntry(&mut td, 1, Some(name), types_core::TEXTOID, -1, 0)?;
    Ok(td)
}

// dblink_res_error: re-raise the remote server's error locally (its SQLSTATE
// preserved, 08006 fallback), then add a dblink CONTEXT line. fail=false emits
// a NOTICE and returns Ok.
#[cold]
fn res_error(
    conn: &PgConn,
    conname: Option<&str>,
    res: &QueryResult,
    fail: bool,
    action: &str,
) -> PgResult<()> {
    let level: ErrorLevel = if fail { ERROR } else { NOTICE };
    let diag = res.diag.clone().unwrap_or_default();
    let sqlstate = if diag.sqlstate.len() == 5 {
        let b = diag.sqlstate.as_bytes();
        make_sqlstate([b[0], b[1], b[2], b[3], b[4]])
    } else {
        ERRCODE_CONNECTION_FAILURE
    };
    let mut primary = diag.primary.clone();
    if primary.is_empty() {
        primary = pchomp(&conn.error_message());
    }
    let dblink_ctx = match conname {
        Some(n) => format!("{action} on dblink connection named \"{n}\""),
        None => format!("{action} on unnamed dblink connection"),
    };
    let ctx = if diag.context.is_empty() {
        dblink_ctx
    } else {
        format!("{}\n{}", diag.context, dblink_ctx)
    };
    let mut b = ereport(level).errcode(sqlstate);
    b = if !primary.is_empty() {
        b.errmsg_internal(primary)
    } else {
        b.errmsg("could not obtain message string for remote error")
    };
    if !diag.detail.is_empty() {
        b = b.errdetail_internal(diag.detail.clone());
    }
    if !diag.hint.is_empty() {
        b = b.errhint(diag.hint.clone());
    }
    b = b.errcontext_msg(ctx);
    b.finish(loc("dblink_res_error"))
}

#[track_caller]
#[cold]
fn connect_failed(msg: &str) -> Box<PgError> {
    Box::new(
        PgError::error("could not establish connection")
            .with_sqlstate(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION)
            .with_detail(pchomp(msg)),
    )
}

// dblink_get_conn: name -> named conn (borrowed), else foreign-server or raw
// connstr -> a fresh TRANSIENT connection the caller must terminate.
enum ConnTarget {
    Named(String),
    Unnamed,
    Transient(PgConn),
}

fn dblink_get_conn(mcx: mcx::Mcx<'_>, conname_or_str: &str) -> PgResult<(ConnTarget, Option<String>)> {
    if registry::named_present(conname_or_str)? {
        let n = conname_or_str.to_string();
        return Ok((ConnTarget::Named(n.clone()), Some(n)));
    }
    let connstr = registry::get_connect_string(mcx, conname_or_str)?
        .unwrap_or_else(|| conname_or_str.to_string());
    registry::connstr_check(&connstr)?;
    let we = registry::we_get_conn()?;
    let enc = mbutils::GetDatabaseEncodingName();
    let conn = match pgclient::connect_db(&connstr, Some(enc), we)? {
        Ok(c) => c,
        Err(msg) => return Err(connect_failed(&msg)),
    };
    registry::security_check(&conn, &connstr)?;
    Ok((ConnTarget::Transient(conn), None))
}

// Run `f` against the connection identified by `target`. Transient conns are
// owned (mutated in place); named/unnamed re-enter the registry.
fn with_target<R>(target: &mut ConnTarget, f: impl FnOnce(&mut PgConn) -> R) -> PgResult<R> {
    match target {
        ConnTarget::Transient(c) => Ok(f(c)),
        ConnTarget::Named(name) => {
            if !registry::named_present(name)? {
                return Err(registry::conn_not_avail(Some(name)));
            }
            registry::with_named(name, |rc| f(&mut rc.expect("present").conn))
        }
        ConnTarget::Unnamed => registry::with_unnamed(|rc| match rc {
            Some(rc) => Ok(f(&mut rc.conn)),
            None => Err(registry::conn_not_avail(None)),
        }),
    }
}

fn with_target_ref<R>(target: &ConnTarget, f: impl FnOnce(&PgConn) -> R) -> PgResult<R> {
    match target {
        ConnTarget::Transient(c) => Ok(f(c)),
        ConnTarget::Named(name) => registry::with_named(name, |rc| f(&rc.expect("present").conn)),
        ConnTarget::Unnamed => registry::with_unnamed(|rc| match rc {
            Some(rc) => Ok(f(&rc.conn)),
            None => Err(registry::conn_not_avail(None)),
        }),
    }
}

fn terminate_transient(target: &mut ConnTarget) {
    if let ConnTarget::Transient(c) = target {
        c.terminate();
    }
}

// --- connection management ---

fn fc_dblink_connect(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let (connname, conname_or_str) = if fcinfo.nargs() == 2 {
        (Some(arg_text(fcinfo, 0)?), arg_text(fcinfo, 1)?)
    } else {
        (None, arg_text(fcinfo, 0)?)
    };
    let connstr = registry::get_connect_string(mcx, &conname_or_str)?.unwrap_or(conname_or_str);
    registry::connstr_check(&connstr)?;
    let we = registry::we_connect()?;
    let enc = mbutils::GetDatabaseEncodingName();
    let conn = match pgclient::connect_db(&connstr, Some(enc), we)? {
        Ok(c) => c,
        Err(msg) => return Err(connect_failed(&msg)),
    };
    if let Err(e) = registry::security_check(&conn, &connstr) {
        let mut conn = conn;
        conn.terminate();
        return Err(e);
    }
    match &connname {
        Some(name) => registry::create_named(name, conn)?,
        None => registry::set_unnamed(conn),
    }
    text_result(mcx, "OK")
}

fn fc_dblink_disconnect(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    if fcinfo.nargs() == 1 {
        let name = arg_text(fcinfo, 0)?;
        if !registry::named_present(&name)? {
            return Err(registry::conn_not_avail(Some(&name)));
        }
        registry::with_named(&name, |rc| {
            if let Some(rc) = rc {
                rc.conn.terminate();
            }
        })?;
        registry::delete_named(&name)?;
    } else {
        match registry::take_unnamed() {
            Some(mut rc) => rc.conn.terminate(),
            None => return Err(registry::conn_not_avail(None)),
        }
    }
    text_result(mcx, "OK")
}

fn fc_dblink_get_connections(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let names = registry::all_named_names();
    if names.is_empty() {
        // C: `PG_RETURN_NULL()` (dblink.c dblink_get_connections, the
        // `else` of `if (astate)`). A bare `Ok(Datum::null())` without
        // isnull=true hands the executor a "non-null" zero pointer datum —
        // detoasting it aborted the server mid-corpus (fleet gate
        // pgrust-fast-tests-601a9800c4-1784439279-28fc).
        fcinfo.isnull = true;
        return Ok(Datum::null());
    }
    let mut elems = Vec::with_capacity(names.len());
    for n in &names {
        elems.push(types_fmgr::varlena_result(varlena::cstring_to_text(mcx, n.as_bytes())?));
    }
    let image = arrayfuncs::construct_array(mcx, &elems, types_core::TEXTOID, -1, false, b'i')?;
    let ptr = image.as_ptr() as usize;
    core::mem::forget(image);
    Ok(Datum::from_usize(ptr))
}

fn fc_dblink_error_message(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let name = arg_text(fcinfo, 0)?;
    if !registry::named_present(&name)? {
        return Err(registry::conn_not_avail(Some(&name)));
    }
    let msg = registry::with_named(&name, |rc| rc.expect("present").conn.error_message())?;
    if msg.is_empty() {
        text_result(mcx, "OK")
    } else {
        text_result(mcx, &pchomp(&msg))
    }
}

fn fc_dblink_is_busy(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let name = arg_text(fcinfo, 0)?;
    if !registry::named_present(&name)? {
        return Err(registry::conn_not_avail(Some(&name)));
    }
    let busy = registry::with_named(&name, |rc| {
        let rc = rc.expect("present");
        rc.conn.consume_input();
        rc.conn.is_busy()
    })?;
    Ok(Datum::from_i32(if busy { 1 } else { 0 }))
}

fn fc_dblink_cancel_query(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let name = arg_text(fcinfo, 0)?;
    if !registry::named_present(&name)? {
        return Err(registry::conn_not_avail(Some(&name)));
    }
    let msg = registry::with_named(&name, |rc| rc.expect("present").conn.cancel(30_000))??;
    text_result(mcx, msg.as_deref().unwrap_or("OK"))
}

fn fc_dblink_send_query(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let name = arg_text(fcinfo, 0)?;
    let sql = arg_text(fcinfo, 1)?;
    if !registry::named_present(&name)? {
        return Err(registry::conn_not_avail(Some(&name)));
    }
    let (ok, errmsg) = registry::with_named(&name, |rc| {
        let rc = rc.expect("present");
        let ok = rc.conn.send_query(&sql);
        (ok, if ok { String::new() } else { rc.conn.error_message() })
    })?;
    if !ok {
        ereport(NOTICE)
            .errmsg(format!("could not send query: {}", pchomp(&errmsg)))
            .finish(loc("dblink_send_query"))?;
    }
    Ok(Datum::from_i32(if ok { 1 } else { 0 }))
}

// --- synchronous query / command ---

fn fc_dblink_record(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("dblink: resolved FmgrInfo required");
    prep_tuplestore_result(fcinfo)?; // C: dblink_record_internal's first act
    // SAFETY: executor arms es_query_cxt pre-call; outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let (sql, mut target, conname, fail) = parse_conn_sql_args(mcx, fcinfo, Some(flinfo))?;

    let fcinfo_ptr: *mut Fcinfo = fcinfo;
    // SAFETY: sink holds fcinfo only across exec_streaming, no aliasing.
    let mut sink = unsafe { materialize::TupleSink::new(mcx, flinfo, fcinfo_ptr) };
    let res = with_target(&mut target, |c| c.exec_streaming(&sql, &mut sink))?;
    let res = match res {
        Ok(r) => r,
        Err(e) => {
            // PG_CATCH parity: clear pending data so the connection stays
            // usable, then rethrow (interrupt or conversion error mid-stream).
            let _ = with_target(&mut target, |c| c.drain());
            terminate_transient(&mut target);
            return Err(e);
        }
    };

    let out = if res.status != ExecStatus::CommandOk && res.status != ExecStatus::TuplesOk {
        with_target(&mut target, |c| c.drain())?;
        with_target_ref(&target, |c| res_error(c, conname.as_deref(), &res, fail, "while executing query"))??;
        // fail=false: return the (empty) result the sink already holds.
        Ok(sink.finish(unsafe { &mut *fcinfo_ptr }))
    } else if res.status == ExecStatus::CommandOk {
        materialize::materialize_command_status(mcx, unsafe { &mut *fcinfo_ptr }, &res.cmd_tag)
    } else {
        Ok(sink.finish(unsafe { &mut *fcinfo_ptr }))
    };
    terminate_transient(&mut target);
    out
}

fn fc_dblink_exec(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let flinfo_ref = flinfo.map(|f| &*f);
    let (sql, mut target, conname, fail) = parse_conn_sql_args(mcx, fcinfo, flinfo_ref)?;

    let res = with_target(&mut target, |c| c.exec(&sql))?;
    let res = match res {
        Ok(r) => r,
        Err(e) => {
            terminate_transient(&mut target);
            return Err(e);
        }
    };
    let out = match res.status {
        ExecStatus::CommandOk => text_result(mcx, &res.cmd_tag),
        ExecStatus::TuplesOk => Err(Box::new(
            PgError::error("statement returning results not allowed")
                .with_sqlstate(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
        )),
        _ => {
            with_target_ref(&target, |c| {
                res_error(c, conname.as_deref(), &res, fail, "while executing command")
            })??;
            text_result(mcx, "ERROR")
        }
    };
    terminate_transient(&mut target);
    out
}

fn fc_dblink_get_result(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("dblink_get_result: resolved FmgrInfo required");
    prep_tuplestore_result(fcinfo)?; // C: dblink_record_internal's first act
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let name = arg_text(fcinfo, 0)?;
    // C: `bool fail = true;` then overridden only when PG_NARGS() == 2 —
    // the one-arg form ERRORS on a failed result (r2's cancel leg raised
    // NOTICE where C raises ERROR).
    let fail = if fcinfo.nargs() == 2 { fcinfo.arg_bool(1) } else { true };
    if !registry::named_present(&name)? {
        return Err(registry::conn_not_avail(Some(&name)));
    }
    let (res, gucs) = registry::with_named(&name, |rc| {
        let rc = rc.expect("present");
        let res = rc.conn.get_result();
        let gucs = materialize::RemoteIoGucs::capture(&rc.conn);
        (res, gucs)
    })?;
    let Some(res) = res? else {
        return Ok(Datum::from_usize(0)); // NULL: async results exhausted
    };
    if res.status != ExecStatus::CommandOk && res.status != ExecStatus::TuplesOk {
        registry::with_named(&name, |rc| {
            res_error(&rc.expect("present").conn, Some(&name), &res, fail, "while executing query")
        })??;
        return Ok(Datum::from_usize(0));
    }
    materialize::materialize_result(mcx, flinfo, fcinfo, &gucs, &res)
}

// --- cursor family ---

fn fc_dblink_open(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let flinfo_ref = flinfo.map(|f| &*f);
    let (conname, curname, sql, fail) = match fcinfo.nargs() {
        2 => (None, arg_text(fcinfo, 0)?, arg_text(fcinfo, 1)?, true),
        3 if arg_is_bool(flinfo_ref, 2) => {
            (None, arg_text(fcinfo, 0)?, arg_text(fcinfo, 1)?, fcinfo.arg_bool(2))
        }
        3 => (Some(arg_text(fcinfo, 0)?), arg_text(fcinfo, 1)?, arg_text(fcinfo, 2)?, true),
        _ => (Some(arg_text(fcinfo, 0)?), arg_text(fcinfo, 1)?, arg_text(fcinfo, 2)?, fcinfo.arg_bool(3)),
    };
    let ok = cursor_op(&conname, |rc| {
        // C: `if (PQtransactionStatus(conn) == PQTRANS_IDLE)` — the REMOTE
        // xact state, not our bookkeeping flag: when the user opened their
        // own transaction via dblink_exec('BEGIN'), C starts none and never
        // auto-COMMITs it (newXactForCursor stays false). Gating on the flag
        // double-BEGAN and then committed the user's transaction at close
        // (fleet gate r2: the myconn cursor-transactions corpus section).
        if rc.conn.transaction_status() == pgclient::TransactionStatus::Idle {
            let res = rc.conn.exec("BEGIN")?;
            if res.status != ExecStatus::CommandOk {
                return internal_err(&rc.conn, "begin error");
            }
            rc.new_xact_for_cursor = true;
            rc.open_cursor_count = 0;
        }
        // C: `if (rconn->newXactForCursor) (rconn->openCursorCount)++;`
        if rc.new_xact_for_cursor {
            rc.open_cursor_count += 1;
        }
        let res = rc.conn.exec(&format!("DECLARE {curname} CURSOR FOR {sql}"))?;
        if res.status != ExecStatus::CommandOk {
            res_error(&rc.conn, conname.as_deref(), &res, fail, &format!("while opening cursor \"{curname}\""))?;
            return Ok(false);
        }
        Ok(true)
    })?;
    text_result(mcx, if ok { "OK" } else { "ERROR" })
}

fn fc_dblink_close(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let flinfo_ref = flinfo.map(|f| &*f);
    let (conname, curname, fail) = match fcinfo.nargs() {
        1 => (None, arg_text(fcinfo, 0)?, true),
        2 if arg_is_bool(flinfo_ref, 1) => (None, arg_text(fcinfo, 0)?, fcinfo.arg_bool(1)),
        2 => (Some(arg_text(fcinfo, 0)?), arg_text(fcinfo, 1)?, true),
        _ => (Some(arg_text(fcinfo, 0)?), arg_text(fcinfo, 1)?, fcinfo.arg_bool(2)),
    };
    let ok = cursor_op(&conname, |rc| {
        let res = rc.conn.exec(&format!("CLOSE {curname}"))?;
        if res.status != ExecStatus::CommandOk {
            res_error(&rc.conn, conname.as_deref(), &res, fail, &format!("while closing cursor \"{curname}\""))?;
            return Ok(false);
        }
        if rc.new_xact_for_cursor {
            rc.open_cursor_count -= 1;
            if rc.open_cursor_count == 0 {
                rc.new_xact_for_cursor = false;
                let res = rc.conn.exec("COMMIT")?;
                if res.status != ExecStatus::CommandOk {
                    return internal_err(&rc.conn, "commit error");
                }
            }
        }
        Ok(true)
    })?;
    text_result(mcx, if ok { "OK" } else { "ERROR" })
}

fn fc_dblink_fetch(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("dblink_fetch: resolved FmgrInfo required");
    prep_tuplestore_result(fcinfo)?; // C: dblink_fetch's first act
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let flinfo_ref = Some(&*flinfo);
    let (conname, curname, howmany, fail) = match fcinfo.nargs() {
        4 => (Some(arg_text(fcinfo, 0)?), arg_text(fcinfo, 1)?, fcinfo.arg_i32(2), fcinfo.arg_bool(3)),
        3 if arg_is_bool(flinfo_ref, 2) => {
            (None, arg_text(fcinfo, 0)?, fcinfo.arg_i32(1), fcinfo.arg_bool(2))
        }
        3 => (Some(arg_text(fcinfo, 0)?), arg_text(fcinfo, 1)?, fcinfo.arg_i32(2), true),
        _ => (None, arg_text(fcinfo, 0)?, fcinfo.arg_i32(1), true),
    };
    if !conn_present(&conname)? {
        return Err(registry::conn_not_avail(conname.as_deref()));
    }
    let sql = format!("FETCH {howmany} FROM {curname}");
    let (res, gucs) = on_conn(&conname, |rc| {
        let res = rc.conn.exec(&sql);
        let gucs = materialize::RemoteIoGucs::capture(&rc.conn);
        (res, gucs)
    })?;
    let res = res?;
    if res.status != ExecStatus::CommandOk && res.status != ExecStatus::TuplesOk {
        on_conn(&conname, |rc| {
            res_error(&rc.conn, conname.as_deref(), &res, fail, &format!("while fetching from cursor \"{curname}\""))
        })??;
        return Ok(Datum::from_usize(0));
    }
    if res.status == ExecStatus::CommandOk {
        return throw(ereport(ERROR)
            .errcode(ERRCODE_INVALID_CURSOR_NAME)
            .errmsg(format!("cursor \"{curname}\" does not exist"))
            .finish(loc("dblink_fetch")));
    }
    materialize::materialize_result(mcx, flinfo, fcinfo, &gucs, &res)
}

fn fc_dblink_get_notify(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("dblink_get_notify: resolved FmgrInfo required");
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let conname = if fcinfo.nargs() == 1 { Some(arg_text(fcinfo, 0)?) } else { None };
    if !conn_present(&conname)? {
        return Err(registry::conn_not_avail(conname.as_deref()));
    }
    let notifies = on_conn(&conname, |rc| {
        rc.conn.consume_input();
        let mut out = Vec::new();
        while let Some(n) = rc.conn.next_notify() {
            out.push(n);
            rc.conn.consume_input();
        }
        out
    })?;

    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    for n in &notifies {
        let name = types_fmgr::varlena_result(varlena::cstring_to_text(mcx, n.channel.as_bytes())?);
        let pid = Datum::from_i32(n.be_pid);
        let extra = types_fmgr::varlena_result(varlena::cstring_to_text(mcx, n.extra.as_bytes())?);
        srf.putvalues(&[name, pid, extra], &[false, false, false])?;
    }
    Ok(srf.finish(fcinfo))
}

fn fc_dblink_current_query(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    adt_misc::fc_current_query(flinfo, fcinfo)
}

// --- shared dispatch helpers ---

fn parse_conn_sql_args(
    mcx: mcx::Mcx<'_>,
    fcinfo: &Fcinfo,
    flinfo: Option<&FmgrInfo>,
) -> PgResult<(String, ConnTarget, Option<String>, bool)> {
    match fcinfo.nargs() {
        3 => {
            let conname = arg_text(fcinfo, 0)?;
            let sql = arg_text(fcinfo, 1)?;
            let fail = fcinfo.arg_bool(2);
            let (t, cn) = dblink_get_conn(mcx, &conname)?;
            Ok((sql, t, cn, fail))
        }
        2 if arg_is_bool(flinfo, 1) => {
            Ok((arg_text(fcinfo, 0)?, ConnTarget::Unnamed, None, fcinfo.arg_bool(1)))
        }
        2 => {
            let conname = arg_text(fcinfo, 0)?;
            let sql = arg_text(fcinfo, 1)?;
            let (t, cn) = dblink_get_conn(mcx, &conname)?;
            Ok((sql, t, cn, true))
        }
        _ => Ok((arg_text(fcinfo, 0)?, ConnTarget::Unnamed, None, true)),
    }
}

fn conn_present(conname: &Option<String>) -> PgResult<bool> {
    match conname {
        Some(n) => registry::named_present(n),
        None => Ok(registry::unnamed_present()),
    }
}

fn on_conn<R>(conname: &Option<String>, f: impl FnOnce(&mut RemoteConn) -> R) -> PgResult<R> {
    match conname {
        Some(name) => registry::with_named(name, |rc| f(rc.expect("present"))),
        None => registry::with_unnamed(|rc| match rc {
            Some(rc) => Ok(f(rc)),
            None => Err(registry::conn_not_avail(None)),
        }),
    }
}

fn cursor_op(
    conname: &Option<String>,
    f: impl FnOnce(&mut RemoteConn) -> PgResult<bool>,
) -> PgResult<bool> {
    if !conn_present(conname)? {
        return Err(registry::conn_not_avail(conname.as_deref()));
    }
    on_conn(conname, f)?
}

#[cold]
fn internal_err(conn: &PgConn, what: &str) -> PgResult<bool> {
    throw(ereport(ERROR)
        .errmsg(format!("{what}: {}", pchomp(&conn.error_message())))
        .finish(loc("dblink")))
}

// --- fmgr registration ---

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "dblink_connect" => fc_dblink_connect,
        "dblink_disconnect" => fc_dblink_disconnect,
        "dblink_open" => fc_dblink_open,
        "dblink_close" => fc_dblink_close,
        "dblink_fetch" => fc_dblink_fetch,
        "dblink_record" => fc_dblink_record,
        "dblink_send_query" => fc_dblink_send_query,
        "dblink_get_result" => fc_dblink_get_result,
        "dblink_exec" => fc_dblink_exec,
        "dblink_get_pkey" => catalog::fc_dblink_get_pkey,
        "dblink_build_sql_insert" => catalog::fc_dblink_build_sql_insert,
        "dblink_build_sql_delete" => catalog::fc_dblink_build_sql_delete,
        "dblink_build_sql_update" => catalog::fc_dblink_build_sql_update,
        "dblink_current_query" => fc_dblink_current_query,
        "dblink_get_connections" => fc_dblink_get_connections,
        "dblink_is_busy" => fc_dblink_is_busy,
        "dblink_cancel_query" => fc_dblink_cancel_query,
        "dblink_error_message" => fc_dblink_error_message,
        "dblink_get_notify" => fc_dblink_get_notify,
        "dblink_fdw_validator" => fdw::fc_dblink_fdw_validator,
        _ => return None,
    })
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
}
