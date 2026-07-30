// exec_parse/bind/execute/describe_message — the extended-query protocol
// (postgres.c). Binary-format params decode through the typreceive fc ABI
// (types_fmgr::wire). Loud arm: BuildParamLogString
// (log_parameter_max_length_on_error != 0).
use core::cell::Cell;
use std::ffi::CStr;

use ::elog::ereport;
use ::datum::Datum;
use ::mcx::{Mcx, MemoryContext, PgString, PgVec};
use ::plancache::CachedPlanSourceHandle;
use ::stringinfo::StringInfo;
use ::types_core::{Oid, OidIsValid};
use ::types_dest::CommandDest;
use ::types_error::{
    PgResult, ERRCODE_INDETERMINATE_DATATYPE, ERRCODE_INVALID_BINARY_REPRESENTATION,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_IN_FAILED_SQL_TRANSACTION, ERRCODE_PROTOCOL_VIOLATION,
    ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_CURSOR, ERRCODE_UNDEFINED_PSTATEMENT, ERROR, LOG,
};
use ::types_nodes::nodes_enums::CmdType;
use ::types_nodes::parsenodes::Query;
use ::types_nodes::plannodes::PlannedStmt;
use ::types_nodes::rawnodes::RawStmt;
use ::types_nodes::NodeTag;
use ::types_portal::params::{ParamExternData, PARAM_FLAG_CONST};
use ::types_portal::{
    ParamListHandle, Portal, QueryCompletion, QueryEnvHandle, CMDTAG_UNKNOWN,
    CURSOR_OPT_PARALLEL_OK, FETCH_ALL,
};

use crate::simple_query::{
    check_log_duration, finish_xact_command, pg_parse_query,
    pg_rewrite_query, start_xact_command, IsTransactionExitStmt,
};
use crate::{check_for_interrupts, loc, ResetUsage, ShowUsage};

mod pqmsg {
    pub const PARSE_COMPLETE: u8 = b'1';
    pub const BIND_COMPLETE: u8 = b'2';
    pub const PORTAL_SUSPENDED: u8 = b's';
    pub const PARAMETER_DESCRIPTION: u8 = b't';
    pub const NO_DATA: u8 = b'n';
}

const UNKNOWNOID: Oid = 705;

thread_local! {
    static UNNAMED_STMT_PSRC: Cell<Option<CachedPlanSourceHandle>> = const { Cell::new(None) };
}

pub fn drop_unnamed_stmt() {
    // Cleared before the drop (C's dangling-pointer paranoia).
    if let Some(psrc) = UNNAMED_STMT_PSRC.take() {
        plancache::DropCachedPlan(psrc);
    }
}

fn log_statement_stats() -> bool {
    guc_tables::backing::log_statement_stats()
}

#[cold]
fn unnamed_stmt_missing() -> Box<types_error::PgError> {
    ereport(ERROR)
        .errcode(ERRCODE_UNDEFINED_PSTATEMENT)
        .errmsg("unnamed prepared statement does not exist")
        .into_error()
        .into()
}

fn lookup_plansource(stmt_name: &str) -> PgResult<CachedPlanSourceHandle> {
    if !stmt_name.is_empty() {
        let plansource = prepare_seams::fetch_prepared_statement_plansource::call(stmt_name, true)?
            .expect("throwError returned entry");
        Ok(plansource)
    } else {
        UNNAMED_STMT_PSRC.with(Cell::get).ok_or_else(unnamed_stmt_missing)
    }
}

#[cold]
fn aborted_xact_error() -> Box<types_error::PgError> {
    ereport(ERROR)
        .errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION)
        .errmsg("current transaction is aborted, commands ignored until end of transaction block")
        .into_error()
        .into()
}

pub fn pg_analyze_and_rewrite_varparams<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: &RawStmt<'mcx>,
    query_string: &str,
    param_types: &[Oid],
    query_env: QueryEnvHandle,
) -> PgResult<(PgVec<'mcx, Query<'mcx>>, PgVec<'mcx, Oid>)> {
    if guc_tables::backing::log_parser_stats() {
        ResetUsage();
    }

    let (query, resolved) = analyze_seams::parse_analyze_varparams::call(
        mcx,
        parsetree,
        query_string,
        param_types,
        query_env,
    )?;

    for (i, &ptype) in resolved.iter().enumerate() {
        if !OidIsValid(ptype) || ptype == UNKNOWNOID {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INDETERMINATE_DATATYPE)
                .errmsg(format!("could not determine data type of parameter ${}", i + 1))
                .into_error()
                .into());
        }
    }

    if guc_tables::backing::log_parser_stats() {
        ShowUsage("PARSE ANALYSIS STATISTICS")?;
    }

    Ok((pg_rewrite_query(mcx, query)?, resolved))
}

pub fn exec_parse_message<'mcx>(
    mcx: Mcx<'mcx>,
    query_string: &str,
    stmt_name: &str,
    param_types: &[Oid],
) -> PgResult<()> {
    let save_log_statement_stats = log_statement_stats();

    // C: `debug_query_string = query_string;` (exec_parse_message top),
    // cleared at the message's end — the scope's drop.
    let _debug_query = elog::debug_query_string_scope(query_string);

    backend_status_seams::pgstat_report_activity::call(
        backend_status_seams::BackendState::STATE_RUNNING,
        Some(query_string),
    );
    ps_status_seams::set_ps_display::call("PARSE");

    if save_log_statement_stats {
        ResetUsage();
    }

    start_xact_command()?;

    /*
     * C's named/unnamed context strategy (MessageContext + copy vs
     * unnamed_stmt_context reparenting) collapses onto the plancache's
     * per-source query arena: the re-parse goes straight into it for both
     * (prepare.c port precedent), so nothing is copied at CompleteCachedPlan.
     */
    let is_named = !stmt_name.is_empty();
    if !is_named {
        drop_unnamed_stmt();
    }

    let parsetree_list = pg_parse_query(mcx, query_string)?;

    if parsetree_list.len() > 1 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("cannot insert multiple commands into a prepared statement")
            .into_error()
            .into());
    }

    let psrc = if let Some(raw) = parsetree_list.first() {
        let stmt = raw.stmt.expect("RawStmt has a stmt");
        if xact::IsAbortedTransactionBlockState() && !IsTransactionExitStmt(Some(stmt)) {
            return Err(aborted_xact_error());
        }

        let tag = utility_seams::create_command_tag::call(stmt);
        let psrc = plancache::CreateCachedPlan(Some(raw), query_string, tag)?;

        let filled = fill_parse_plansource(psrc, raw, query_string, param_types);
        if let Err(e) = filled {
            // C leaves the transient plansource to xact-abort cleanup; the
            // registry has no abort hook, so reclaim here (prepare precedent).
            plancache::DropCachedPlan(psrc);
            return Err(e);
        }
        // Revalidation is plancache's fixedparams default on the retained raw
        // tree, with the resolved param types (C's parserSetup == NULL arm).
        psrc
    } else {
        /* Empty input string.  This is legal. */
        let psrc = plancache::CreateCachedPlan(None, query_string, CMDTAG_UNKNOWN)?;
        let qmcx = plancache::SourceQueryMcx(psrc);
        let complete = plancache::CompleteCachedPlan(
            psrc,
            PgVec::new_in(qmcx),
            param_types,
            CURSOR_OPT_PARALLEL_OK,
            true,
        );
        if let Err(e) = complete {
            plancache::DropCachedPlan(psrc);
            return Err(e);
        }
        psrc
    };

    check_for_interrupts()?;

    if is_named {
        if let Err(e) = prepare_seams::store_prepared_statement::call(stmt_name, psrc, false) {
            plancache::DropCachedPlan(psrc);
            return Err(e);
        }
    } else {
        plancache::SaveCachedPlan(psrc)?;
        UNNAMED_STMT_PSRC.with(|c| c.set(Some(psrc)));
    }

    xact::CommandCounterIncrement()?;

    if elog::config::where_to_send_output() == CommandDest::Remote {
        pqformat::pq_putemptymessage(pqmsg::PARSE_COMPLETE)?;
    }

    match check_log_duration(false) {
        (1, msec_str) => {
            ereport(LOG)
                .errmsg(format!("duration: {msec_str} ms"))
                .errhidestmt(true)
                .finish(loc(1596, "exec_parse_message"))?;
        }
        (2, msec_str) => {
            let name = if is_named { stmt_name } else { "<unnamed>" };
            ereport(LOG)
                .errmsg(format!("duration: {msec_str} ms  parse {name}: {query_string}"))
                .errhidestmt(true)
                .finish(loc(1601, "exec_parse_message"))?;
        }
        _ => {}
    }

    if save_log_statement_stats {
        ShowUsage("PARSE MESSAGE STATISTICS")?;
    }

    Ok(())
}

fn fill_parse_plansource(
    psrc: CachedPlanSourceHandle,
    raw: &RawStmt<'_>,
    query_string: &str,
    param_types: &[Oid],
) -> PgResult<()> {
    let mut snapshot_set = false;
    if analyze_seams::analyze_requires_snapshot::call(raw) {
        let snap = snapmgr::GetTransactionSnapshot()?;
        snapmgr::PushActiveSnapshot(&snap)?;
        snapshot_set = true;
    }

    let outcome = (|| -> PgResult<()> {
        // C analyzes the message-arena raw tree in place; here analysis
        // scribbles query-arena pointers into its input, so the plansource's
        // retained copy is copied once more into the query arena (no re-lex:
        // a second lex re-emits scanner warnings C doesn't).
        let qmcx = plancache::SourceQueryMcx(psrc);
        let reparsed = plancache::CachedPlanRawParseTreeCopy(qmcx, psrc)?
            .expect("created with a raw tree");

        let (query_list, resolved) = pg_analyze_and_rewrite_varparams(
            qmcx,
            reparsed,
            query_string,
            param_types,
            QueryEnvHandle::NULL,
        )?;

        plancache::CompleteCachedPlan(
            psrc,
            query_list,
            &resolved,
            CURSOR_OPT_PARALLEL_OK,
            true,
        )
    })();

    if snapshot_set {
        snapmgr::PopActiveSnapshot()?;
    }
    outcome
}

fn copy_param_datum<'mcx>(
    mcx: Mcx<'mcx>,
    value: Datum,
    is_null: bool,
    ptype: Oid,
) -> PgResult<Datum> {
    if is_null {
        return Ok(value);
    }
    let (typlen, typbyval) = lsyscache::typ::get_typlenbyval(ptype)?;
    if typbyval {
        return Ok(value);
    }
    datum_copy_in(mcx, value, typlen)
}

// datumCopy (datum.c) scoped to bind parameters; by-ref sources are
// input/receive-function results (canonical 4B today), but the -1 arm is
// C's VARSIZE_ANY so a future short/toast source copies, never misreads.
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


// bind_param_error_callback (postgres.c): CONTEXT for an error thrown while
// processing one bind parameter; the value is quoted and clipped to
// log_parameter_max_length_on_error bytes (<0 = unclipped), C's
// appendStringInfoStringQuoted shape (quote doubling, in-quote "..." when
// clipped, clip backed off to a character boundary).
#[cold]
#[inline(never)]
fn bind_param_error_context(
    mut err: Box<types_error::PgError>,
    portal_name: &str,
    paramno: usize,
    value: Option<&str>,
) -> Box<types_error::PgError> {
    let line = match value {
        Some(s) => {
            let maxlen = guc_tables::backing::log_parameter_max_length_on_error();
            let (clip, ellipsis) = if maxlen < 0 || maxlen as usize >= s.len() {
                (s, false)
            } else {
                let mut end = maxlen as usize;
                while end > 0 && !s.is_char_boundary(end) {
                    end -= 1;
                }
                (&s[..end], true)
            };
            let mut quoted = String::with_capacity(clip.len() + 8);
            quoted.push('\'');
            for c in clip.chars() {
                if c == '\'' {
                    quoted.push('\'');
                }
                quoted.push(c);
            }
            if ellipsis {
                quoted.push_str("...");
            }
            quoted.push('\'');
            if portal_name.is_empty() {
                format!("unnamed portal parameter ${} = {}", paramno + 1, quoted)
            } else {
                format!("portal \"{}\" parameter ${} = {}", portal_name, paramno + 1, quoted)
            }
        }
        None if portal_name.is_empty() => {
            format!("unnamed portal parameter ${}", paramno + 1)
        }
        None => format!("portal \"{}\" parameter ${}", portal_name, paramno + 1),
    };
    err.add_context_line(line);
    err
}

fn portal_mcx(portal: &Portal<'static>) -> Mcx<'static> {
    // SAFETY: portalContext is PgBox'd for address stability and outlives
    // every use of this Mcx (freed only in PortalDrop, which first frees the
    // params registry handle whose datums live here); the Ref is released
    // before use (pquery precedent).
    let ctx: &'static MemoryContext = unsafe {
        let p = portal.borrow();
        &*(&**p.portalContext.as_ref().expect("portal has portalContext")
            as *const MemoryContext)
    };
    ctx.mcx()
}

pub fn exec_bind_message<'mcx>(
    mcx: Mcx<'mcx>,
    input_message: &mut StringInfo<'mcx>,
) -> PgResult<()> {
    let portal_name = owned_msg_string(mcx, input_message)?;
    let stmt_name = owned_msg_string(mcx, input_message)?;

    let psrc = lookup_plansource(stmt_name.as_str())?;
    let query_string = plancache::CachedPlanQueryString(psrc);
    let save_log_statement_stats = log_statement_stats();

    // C: `debug_query_string = psrc->query_string;` (exec_bind_message).
    let _debug_query = elog::debug_query_string_scope(query_string);

    backend_status_seams::pgstat_report_activity::call(
        backend_status_seams::BackendState::STATE_RUNNING,
        Some(query_string),
    );
    for query in plancache::CachedPlanQueryList(psrc) {
        if query.queryId != 0 {
            backend_status_seams::pgstat_report_query_id::call(query.queryId, false);
            break;
        }
    }
    ps_status_seams::set_ps_display::call("BIND");

    if save_log_statement_stats {
        ResetUsage();
    }

    start_xact_command()?;
    crate::stmt_trace::probe("b.xact");

    let num_pformats = pqformat::pq_getmsgint(input_message, 2)? as usize;
    let mut pformats: PgVec<'mcx, i16> = PgVec::new_in(mcx);
    pformats.try_reserve_exact(num_pformats).map_err(|_| mcx.oom(num_pformats))?;
    for _ in 0..num_pformats {
        pformats.push(pqformat::pq_getmsgint(input_message, 2)? as i16);
    }

    let num_params = pqformat::pq_getmsgint(input_message, 2)? as usize;

    if num_pformats > 1 && num_pformats != num_params {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!(
                "bind message has {num_pformats} parameter formats but {num_params} parameters"
            ))
            .into_error()
            .into());
    }

    if num_params != plancache::CachedPlanNumParams(psrc) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!(
                "bind message supplies {} parameters, but prepared statement \"{}\" requires {}",
                num_params,
                stmt_name.as_str(),
                plancache::CachedPlanNumParams(psrc)
            ))
            .into_error()
            .into());
    }

    if xact::IsAbortedTransactionBlockState()
        && (!plancache::CachedPlanIsTransactionExitStmt(psrc) || num_params != 0)
    {
        return Err(aborted_xact_error());
    }

    let mut reused = false;
    let portal = if portal_name.is_empty() {
        // Portal retention: drop the previous unnamed portal first (it may
        // itself park), then take this statement's parked shell if one
        // exists. The retained execution is only reused after GetCachedPlan
        // below returns the shell's own still-valid generic plan.
        if let Some(existing) = portalmem::GetPortalByName(Some("")) {
            portalmem::PortalDrop(&existing, false)?;
        }
        match portalmem::TakeParkedPortal(types_portal::PlanSourceHandle(psrc.0))? {
            Some(shell) => {
                reused = true;
                shell
            }
            None => portalmem::CreatePortal("", true, true)?,
        }
    } else {
        portalmem::CreatePortal(portal_name.as_str(), false, false)?
    };

    let pmcx = portal_mcx(&portal);

    let mut snapshot_set = false;
    if num_params > 0 || plancache::CachedPlanRequiresSnapshot(psrc) {
        let snap = snapmgr::GetTransactionSnapshot()?;
        snapmgr::PushActiveSnapshot(&snap)?;
        snapshot_set = true;
    }

    let params = if num_params > 0 {
        let param_types = plancache::CachedPlanParamTypes(psrc);
        let mut params: PgVec<'static, ParamExternData> = PgVec::new_in(pmcx);
        params.try_reserve_exact(num_params).map_err(|_| pmcx.oom(num_params))?;

        for paramno in 0..num_params {
            // bind_param_error_callback (postgres.c): every error thrown while
            // processing this parameter carries a CONTEXT line; the textual
            // value rides only on the input-function call. Non-error reports
            // emitted inside input functions do not attach it (the propagation
            // pattern covers the ERROR path only).
            let pctx = |e: Box<types_error::PgError>| {
                bind_param_error_context(e, portal_name.as_str(), paramno, None)
            };
            let ptype = param_types[paramno];
            let plength =
                pqformat::pq_getmsgint(input_message, 4).map_err(pctx)? as i32;
            let is_null = plength == -1;

            let pformat: i16 = if num_pformats > 1 {
                pformats[paramno]
            } else if num_pformats > 0 {
                pformats[0]
            } else {
                0 /* default = text */
            };

            let pval = match pformat {
                0 => {
                    let (typinput, typioparam) =
                        lsyscache::typ::getTypeInputInfo(ptype).map_err(pctx)?;
                    let pstring = if is_null {
                        None
                    } else {
                        let raw = pqformat::pq_getmsgbytes(input_message, plength as usize)
                            .map_err(pctx)?;
                        Some(client_to_server_cstring(pmcx, raw).map_err(pctx)?)
                    };
                    if guc_tables::backing::log_parameter_max_length_on_error() != 0 {
                        panic!(
                            "exec_bind_message (postgres.c): knownTextValues/\
                             BuildParamLogString need the params-logging lane \
                             (log_parameter_max_length_on_error != 0)"
                        );
                    }
                    let cstr = pstring.as_ref().map(|v| {
                        CStr::from_bytes_with_nul(v)
                            .expect("client_to_server_cstring NUL-terminates")
                    });
                    let mut finfo = fmgr_seams::fmgr_info::call(typinput).map_err(pctx)?;
                    let v = types_fmgr::input_function_call(&mut finfo, cstr, typioparam, -1, pmcx)
                        .map_err(|e| {
                            let val = cstr.map(|c| String::from_utf8_lossy(c.to_bytes()).into_owned());
                            bind_param_error_context(
                                e,
                                portal_name.as_str(),
                                paramno,
                                val.as_deref(),
                            )
                        })?;
                    // The result may alias finfo's scratch (fc_textin's
                    // OutBuf), which dies with this arm — datumCopy into the
                    // portal context (C pallocs there directly).
                    copy_param_datum(pmcx, v, is_null, ptype).map_err(pctx)?
                }
                1 => {
                    let (typreceive, typioparam) =
                        lsyscache::typ::getTypeBinaryInputInfo(ptype).map_err(pctx)?;
                    let mut finfo = fmgr_seams::fmgr_info::call(typreceive).map_err(pctx)?;
                    if is_null {
                        let v = types_fmgr::receive_function_call(
                            &mut finfo, None, typioparam, -1, pmcx,
                        )
                        .map_err(pctx)?;
                        copy_param_datum(pmcx, v, is_null, ptype).map_err(pctx)?
                    } else {
                        let raw = pqformat::pq_getmsgbytes(input_message, plength as usize)
                            .map_err(pctx)?;
                        // C's initReadOnlyStringInfo aliases the message buffer;
                        // no read-only StringInfo here, so copy the param bytes.
                        let mut pbuf = StringInfo::with_capacity_in(pmcx, plength as usize + 1)?;
                        pbuf.append_bytes(raw)?;
                        let pval = types_fmgr::receive_function_call(
                            &mut finfo,
                            Some(&mut pbuf),
                            typioparam,
                            -1,
                            pmcx,
                        )
                        .map_err(pctx)?;
                        if pbuf.cursor != pbuf.len() {
                            return Err(pctx(Box::new(
                                ereport(ERROR)
                                    .errcode(ERRCODE_INVALID_BINARY_REPRESENTATION)
                                    .errmsg(format!(
                                        "incorrect binary data format in bind parameter {}",
                                        paramno + 1
                                    ))
                                    .into_error(),
                            ))
                            .into());
                        }
                        copy_param_datum(pmcx, pval, is_null, ptype).map_err(pctx)?
                    }
                }
                other => {
                    return Err(pctx(Box::new(
                        ereport(ERROR)
                            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                            .errmsg(format!("unsupported format code: {other}"))
                            .into_error(),
                    ))
                    .into());
                }
            };

            // PARAM_FLAG_CONST so custom plans use the value (C).
            params.push(ParamExternData {
                value: pval,
                isnull: is_null,
                pflags: PARAM_FLAG_CONST,
                ptype,
            });
        }

        let slice: &'static [ParamExternData] = params.leak();
        // SAFETY: the datums and the slice live in the portal context;
        // PortalDrop frees the handle before that context dies.
        let h = unsafe { types_portal::params::register(slice) };
        // Stored now so an error below reaches PortalDrop's registry cleanup.
        portal.borrow_mut().portalParams = h;
        h
    } else {
        ParamListHandle::NULL
    };

    let num_rformats = pqformat::pq_getmsgint(input_message, 2)? as usize;
    let mut rformats: PgVec<'mcx, i16> = PgVec::new_in(mcx);
    rformats.try_reserve_exact(num_rformats).map_err(|_| mcx.oom(num_rformats))?;
    for _ in 0..num_rformats {
        rformats.push(pqformat::pq_getmsgint(input_message, 2)? as i16);
    }

    pqformat::pq_getmsgend(input_message)?;
    crate::stmt_trace::probe("b.params");

    let cplan = plancache::GetCachedPlan(psrc, params, None, QueryEnvHandle::NULL)?;
    crate::stmt_trace::probe("b.plan");

    // Retained execution engages only against the shell's own generic plan:
    // any RevalidateCachedQuery invalidation (DDL, search_path, RLS
    // environment) replans into a fresh CachedPlan and misses this check.
    let retained = reused && portal.borrow().cplan == cplan;
    if retained {
        // The shell already pins this plan; drop the refcount just taken.
        plancache::ReleaseCachedPlan(cplan);
    } else {
        if reused {
            portalmem::ShedRetainedExecution(&portal);
        }
        let stmt_slice = plancache::CachedPlanStmtList(cplan);
        // SAFETY: the cplan refcount taken by GetCachedPlan pins stmt_slice until
        // PortalDrop releases it (which also frees this handle). NIL stays the
        // null handle (empty query string).
        let stmts = if stmt_slice.is_empty() {
            types_portal::StmtListHandle::NULL
        } else {
            unsafe { pquery::stmt_list::register(stmt_slice) }
        };
        // No fallible call between GetCachedPlan and PortalDefineQuery (C's
        // refcount-leak rule; the Copy stores in DefineQuery land first).
        portalmem::PortalDefineQuery(
            &portal,
            (!stmt_name.is_empty()).then(|| stmt_name.as_str()),
            query_string,
            plancache::CachedPlanCommandTag(psrc),
            stmts,
            cplan,
        )?;
        portal.borrow_mut().plansource = types_portal::PlanSourceHandle(psrc.0);
    }

    for stmt in plancache::CachedPlanStmtList(portal.borrow().cplan) {
        if stmt.planId != 0 {
            backend_status_seams::pgstat_report_plan_id::call(stmt.planId, false);
            break;
        }
    }

    if snapshot_set {
        snapmgr::PopActiveSnapshot()?;
    }

    if retained {
        pquery::PortalStartParked(&portal, params)?;
    } else {
        pquery::PortalStart(&portal, params, 0, None)?;
    }
    crate::stmt_trace::probe("b.portalstart");

    pquery::PortalSetResultFormat(&portal, &rformats)?;

    if elog::config::where_to_send_output() == CommandDest::Remote {
        pqformat::pq_putemptymessage(pqmsg::BIND_COMPLETE)?;
    }
    crate::stmt_trace::probe("b.done");

    match check_log_duration(false) {
        (1, msec_str) => {
            ereport(LOG)
                .errmsg(format!("duration: {msec_str} ms"))
                .errhidestmt(true)
                .finish(loc(2065, "exec_bind_message"))?;
        }
        (2, msec_str) => {
            let name = if stmt_name.is_empty() { "<unnamed>" } else { stmt_name.as_str() };
            let sep = if portal_name.is_empty() { "" } else { "/" };
            ereport(LOG)
                .errmsg(format!(
                    "duration: {msec_str} ms  bind {name}{sep}{}: {query_string}",
                    portal_name.as_str()
                ))
                .errhidestmt(true)
                .finish(loc(2070, "exec_bind_message"))?;
        }
        _ => {}
    }

    if save_log_statement_stats {
        ShowUsage("BIND MESSAGE STATISTICS")?;
    }

    Ok(())
}

pub(crate) fn owned_msg_string<'mcx>(
    mcx: Mcx<'mcx>,
    msg: &mut StringInfo<'_>,
) -> PgResult<PgString<'mcx>> {
    let s = pqformat::pq_getmsgstring(mcx, msg)?;
    // Non-UTF8 here means the string is in a non-UTF8 database encoding the
    // UTF-8 engine cannot honor (see non_utf8_query_error); C would carry
    // these bytes, so the honest report is 0A000, not a protocol violation.
    let text =
        core::str::from_utf8(s.as_bytes()).map_err(|_| crate::non_utf8_query_error())?;
    PgString::from_str_in(text, mcx).map_err(Into::into)
}

// pg_client_to_server + the trailing NUL C scribbles onto the message buffer.
fn client_to_server_cstring<'mcx>(
    mcx: Mcx<'mcx>,
    raw: &[u8],
) -> PgResult<PgVec<'mcx, u8>> {
    let mut v = match mbutils::pg_client_to_server(mcx, raw)? {
        Some(converted) => converted,
        None => {
            let mut v: PgVec<'mcx, u8> = PgVec::new_in(mcx);
            v.try_reserve_exact(raw.len() + 1).map_err(|_| mcx.oom(raw.len() + 1))?;
            mcx::vec_append_bytes(&mut v, raw)?;
            v
        }
    };
    v.try_reserve_exact(1).map_err(|_| mcx.oom(1))?;
    v.push(0);
    Ok(v)
}

pub fn exec_execute_message<'mcx>(
    mcx: Mcx<'mcx>,
    portal_name: &str,
    max_rows: i64,
) -> PgResult<()> {
    let mut dest = elog::config::where_to_send_output();
    if dest == CommandDest::Remote {
        dest = CommandDest::RemoteExecute;
    }

    let Some(portal) = portalmem::GetPortalByName(Some(portal_name)) else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_CURSOR)
            .errmsg(format!("portal \"{portal_name}\" does not exist"))
            .into_error()
            .into());
    };

    if portal.borrow().commandTag == CMDTAG_UNKNOWN {
        debug_assert!(portal.borrow().stmts.is_null());
        return tcop_dest::NullCommand(dest);
    }

    // sourceText/prepStmtName copied into MessageContext: the portal may be
    // destroyed during finish_xact_command (C's pstrdup pair).
    let (is_xact_command, source_text, prep_stmt_name) = {
        let p = portal.borrow();
        let stmts = p.stmts;
        let is_xact = !stmts.is_null()
            && pquery::stmt_list::with(stmts, IsTransactionStmtList);
        let src = PgString::from_str_in(
            p.sourceText.as_ref().map(|s| s.as_str()).unwrap_or(""),
            mcx,
        )?;
        let prep = PgString::from_str_in(
            p.prepStmtName.as_ref().map(|s| s.as_str()).unwrap_or("<unnamed>"),
            mcx,
        )?;
        (is_xact, src, prep)
    };
    let source_text = source_text.as_str();
    let prep_stmt_name = prep_stmt_name.as_str();

    // C: `debug_query_string = sourceText;` (exec_execute_message) — the
    // MessageContext copy above outlives this frame, as C's pstrdup does.
    let _debug_query = elog::debug_query_string_scope(source_text);

    backend_status_seams::pgstat_report_activity::call(
        backend_status_seams::BackendState::STATE_RUNNING,
        Some(source_text),
    );
    {
        let stmts = portal.borrow().stmts;
        if !stmts.is_null() {
            pquery::stmt_list::with(stmts, |stmts| {
                if let Some(stmt) = stmts.iter().find(|s| s.queryId.get() != 0) {
                    backend_status_seams::pgstat_report_query_id::call(stmt.queryId.get(), false);
                }
                if let Some(stmt) = stmts.iter().find(|s| s.planId != 0) {
                    backend_status_seams::pgstat_report_plan_id::call(stmt.planId, false);
                }
            });
        }
    }

    let command_tag = portal.borrow().commandTag;
    let (cmdtagname, _len) = cmdtag::GetCommandTagNameAndLen(command_tag);
    ps_status_seams::set_ps_display::call(cmdtagname);

    let save_log_statement_stats = log_statement_stats();
    if save_log_statement_stats {
        ResetUsage();
    }

    tcop_dest::BeginCommand(command_tag, dest);

    let mut receiver = tcop_dest::CreateDestReceiver(dest);
    if dest == CommandDest::RemoteExecute {
        tcop_dest::SetRemoteDestReceiverParams(&mut receiver, portal.clone());
    }

    start_xact_command()?;
    crate::stmt_trace::probe("e.xact");

    let execute_is_fetch = !portal.borrow().atStart;

    let mut was_logged = false;
    if check_log_statement_planned(&portal) {
        let verb = if execute_is_fetch { "execute fetch from" } else { "execute" };
        let sep = if portal_name.is_empty() { "" } else { "/" };
        ereport(LOG)
            .errmsg(format!("{verb} {prep_stmt_name}{sep}{portal_name}: {source_text}"))
            .errhidestmt(true)
            .finish(loc(2231, "exec_execute_message"))?;
        was_logged = true;
    }

    if xact::IsAbortedTransactionBlockState() {
        let stmts = portal.borrow().stmts;
        let exit_ok = !stmts.is_null()
            && pquery::stmt_list::with(stmts, IsTransactionExitStmtList);
        if !exit_ok {
            return Err(aborted_xact_error());
        }
    }

    check_for_interrupts()?;

    let max_rows = if max_rows <= 0 { FETCH_ALL } else { max_rows };

    let mut qc = QueryCompletion::default();
    let completed = pquery::PortalRun(
        &portal,
        max_rows,
        true, /* always top level */
        &mut receiver,
        None, /* altdest aliases dest, as in C */
        Some(&mut qc),
    )?;
    crate::stmt_trace::probe("e.run");

    receiver.destroy();

    if completed {
        if is_xact_command
            || (xact::MyXactFlags() & types_core::xact::XACT_FLAGS_NEEDIMMEDIATECOMMIT) != 0
        {
            finish_xact_command()?;
        } else {
            xact::CommandCounterIncrement()?;

            xact::OrMyXactFlags(types_core::xact::XACT_FLAGS_PIPELINING);

            crate::simple_query::disable_statement_timeout()?;
        }

        crate::stmt_trace::probe("e.commit");
        tcop_dest::EndCommand(&qc, dest, false)?;
    } else {
        if elog::config::where_to_send_output() == CommandDest::Remote {
            pqformat::pq_putemptymessage(pqmsg::PORTAL_SUSPENDED)?;
        }
        xact::OrMyXactFlags(types_core::xact::XACT_FLAGS_PIPELINING);
    }

    match check_log_duration(was_logged) {
        (1, msec_str) => {
            ereport(LOG)
                .errmsg(format!("duration: {msec_str} ms"))
                .errhidestmt(true)
                .finish(loc(2343, "exec_execute_message"))?;
        }
        (2, msec_str) => {
            let verb = if execute_is_fetch { "execute fetch from" } else { "execute" };
            let sep = if portal_name.is_empty() { "" } else { "/" };
            ereport(LOG)
                .errmsg(format!(
                    "duration: {msec_str} ms  {verb} {prep_stmt_name}{sep}{portal_name}: {source_text}"
                ))
                .errhidestmt(true)
                .finish(loc(2348, "exec_execute_message"))?;
        }
        _ => {}
    }

    if save_log_statement_stats {
        ShowUsage("EXECUTE MESSAGE STATISTICS")?;
    }

    Ok(())
}

// check_log_statement (postgres.c), PlannedStmt-list flavor; the per-stmt
// probe is GetCommandLogLevel's T_PlannedStmt arm inlined (no Node wrapper
// exists for a bare PlannedStmt).
fn check_log_statement_planned(portal: &Portal<'static>) -> bool {
    use guc_tables::consts::{LOGSTMT_ALL, LOGSTMT_NONE};
    let log_statement = guc_tables::backing::log_statement();

    if log_statement == LOGSTMT_NONE {
        return false;
    }
    if log_statement == LOGSTMT_ALL {
        return true;
    }

    let stmts = portal.borrow().stmts;
    if stmts.is_null() {
        return false;
    }
    pquery::stmt_list::with(stmts, |stmts| {
        stmts.iter().any(|stmt| planned_stmt_log_level(stmt) <= log_statement)
    })
}

fn planned_stmt_log_level(stmt: &PlannedStmt<'_>) -> i32 {
    use guc_tables::consts::{LOGSTMT_ALL, LOGSTMT_MOD};
    match stmt.commandType {
        CmdType::CMD_SELECT => LOGSTMT_ALL,
        CmdType::CMD_INSERT | CmdType::CMD_UPDATE | CmdType::CMD_DELETE | CmdType::CMD_MERGE => {
            LOGSTMT_MOD
        }
        CmdType::CMD_UTILITY => utility_seams::get_command_log_level::call(
            stmt.utilityStmt.expect("CMD_UTILITY stmt has utilityStmt"),
        ),
        _ => LOGSTMT_ALL,
    }
}

fn IsTransactionStmtList(pstmts: &[PlannedStmt<'_>]) -> bool {
    if pstmts.len() == 1 {
        let pstmt = &pstmts[0];
        return pstmt.commandType == CmdType::CMD_UTILITY
            && pstmt.utilityStmt.map(|u| u.node_tag()) == Some(NodeTag::T_TransactionStmt);
    }
    false
}

fn IsTransactionExitStmtList(pstmts: &[PlannedStmt<'_>]) -> bool {
    if pstmts.len() == 1 {
        let pstmt = &pstmts[0];
        return pstmt.commandType == CmdType::CMD_UTILITY
            && IsTransactionExitStmt(pstmt.utilityStmt);
    }
    false
}

pub fn exec_describe_statement_message<'mcx>(mcx: Mcx<'mcx>, stmt_name: &str) -> PgResult<()> {
    start_xact_command()?;

    let psrc = lookup_plansource(stmt_name)?;

    debug_assert!(plancache::CachedPlanFixedResult(psrc));

    let result_desc = plancache::CachedPlanResultDesc(psrc);

    if xact::IsAbortedTransactionBlockState() && result_desc.is_some() {
        return Err(aborted_xact_error());
    }

    if elog::config::where_to_send_output() != CommandDest::Remote {
        return Ok(()); /* can't actually do anything... */
    }

    let param_types = plancache::CachedPlanParamTypes(psrc);
    let mut buf = pqformat::pq_beginmessage(mcx, pqmsg::PARAMETER_DESCRIPTION)?;
    pqformat::pq_sendint16(&mut buf, param_types.len() as u16)?;
    for &ptype in param_types {
        pqformat::pq_sendint32(&mut buf, ptype)?;
    }
    pqformat::pq_endmessage(buf)?;

    match result_desc {
        Some(desc) => {
            let tlist = plancache::CachedPlanGetTargetList(mcx, psrc, QueryEnvHandle::NULL)?;
            let mut rbuf = StringInfo::new_in(mcx)?;
            printtup::SendRowDescriptionMessage(&mut rbuf, &desc, &tlist, None)
        }
        None => pqformat::pq_putemptymessage(pqmsg::NO_DATA),
    }
}

pub fn exec_describe_portal_message<'mcx>(mcx: Mcx<'mcx>, portal_name: &str) -> PgResult<()> {
    start_xact_command()?;

    let Some(portal) = portalmem::GetPortalByName(Some(portal_name)) else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_CURSOR)
            .errmsg(format!("portal \"{portal_name}\" does not exist"))
            .into_error()
            .into());
    };

    if xact::IsAbortedTransactionBlockState() && portal.borrow().tupDesc.is_some() {
        return Err(aborted_xact_error());
    }

    if elog::config::where_to_send_output() != CommandDest::Remote {
        return Ok(()); /* can't actually do anything... */
    }

    let tup_desc = portal.borrow().tupDesc.clone();
    match tup_desc {
        Some(desc) => {
            let p = portal.borrow();
            let tlist = pquery::FetchPortalTargetList(mcx, &p)?;
            let formats = (!p.formats.is_empty()).then_some(&p.formats[..]);
            let mut rbuf = StringInfo::new_in(mcx)?;
            printtup::SendRowDescriptionMessage(&mut rbuf, &desc, &tlist, formats)
        }
        None => pqformat::pq_putemptymessage(pqmsg::NO_DATA),
    }
}

/// The plan-cache probe used by tests: (num_generic_plans, num_custom_plans)
/// of the named or unnamed source.
pub fn plan_cache_counts(stmt_name: &str) -> PgResult<(i64, i64)> {
    Ok(plancache::CachedPlanCounts(lookup_plansource(stmt_name)?))
}

#[cfg(test)]
mod short_varlena_tests {
    use super::*;

    #[test]
    fn datum_copy_in_reads_any_header_form() {
        let ctx = mcx::MemoryContext::new("copy-any-test");
        let mcx = ctx.mcx();
        let short: [u8; 4] = [(4u8 << 1) | 0x01, b'a', b'b', b'c'];
        let out = datum_copy_in(mcx, Datum::from_usize(short.as_ptr() as usize), -1).unwrap();
        let p = out.as_usize() as *const u8;
        assert_eq!(unsafe { core::slice::from_raw_parts(p, 4) }, &short[..]);

        let mut long = ((4u32 + 3) << 2).to_ne_bytes().to_vec();
        long.extend_from_slice(b"abc");
        let out = datum_copy_in(mcx, Datum::from_usize(long.as_ptr() as usize), -1).unwrap();
        let p = out.as_usize() as *const u8;
        assert_eq!(unsafe { core::slice::from_raw_parts(p, 7) }, &long[..]);

        let mut ondisk = vec![0x01u8, 18];
        ondisk.extend_from_slice(&[0u8; 16]);
        let out = datum_copy_in(mcx, Datum::from_usize(ondisk.as_ptr() as usize), -1).unwrap();
        let p = out.as_usize() as *const u8;
        assert_eq!(unsafe { core::slice::from_raw_parts(p, 18) }, &ondisk[..]);
    }
}
