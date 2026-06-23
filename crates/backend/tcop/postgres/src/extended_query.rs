//! `tcop/postgres.c` — the extended-query (FE/BE, "F2") protocol pipeline.
//!
//! This module ports the Parse / Bind / Describe / Execute message handlers that
//! drive prepared statements, parameterized queries, and cursors over the v3
//! extended-query protocol (psql's `\parse` / `\bind` / `\bind_named` / `\gdesc`,
//! the libpq `PQprepare`/`PQexecPrepared` path, JDBC server-side prepares, …):
//!
//!   * [`exec_parse_message`]              — postgres.c:1389 (Parse: build a
//!     `CachedPlanSource` via the plancache, store the prepared statement)
//!   * [`exec_bind_message`]               — postgres.c:1624 (Bind: create a
//!     portal, fetch + convert parameters, plan, define + start the portal)
//!   * [`exec_execute_message`]            — postgres.c:2107 (Execute: run the
//!     portal to `max_rows`, send CommandComplete or PortalSuspended)
//!   * [`exec_describe_statement_message`] — postgres.c:2641 (Describe 'S')
//!   * [`exec_describe_portal_message`]    — postgres.c:2734 (Describe 'P')
//!
//! The plancache (`CachedPlanSource`/`CachedPlan`) is the central dependency and
//! is fully ported + de-handled (#159 STEP C): `CreateCachedPlan` /
//! `CompleteCachedPlan` / `GetCachedPlan` own real `Query`/`PlannedStmt` values,
//! handed across the seam crate by opaque `CachedPlanSourceHandle` /
//! `CachedPlanHandle` tokens. The prepared-statement store (`prepare.c`'s
//! `StorePreparedStatement`/`FetchPreparedStatement`) and the portal machinery
//! are likewise real. So the F2 path here is a faithful driver over those owners,
//! mirroring the F1 (`exec_simple_query`) pipeline shape.
//!
//! # Sanctioned divergences (audit against these)
//!
//! 1. **Per-message arena, not `MessageContext`/`unnamed_stmt_context`.** C runs
//!    parse/analysis for an unnamed Parse in a child `unnamed_stmt_context` it
//!    later reparents under the `CachedPlanSource`; the de-handled plancache owns
//!    its trees in its own private contexts (it `clone_in`s on `CreateCachedPlan`
//!    / `CompleteCachedPlan`), so the working trees live in the threaded `'mcx`
//!    and the named/unnamed context distinction collapses to that one arena. The
//!    reset reclaims the temp space the same way.
//! 2. **`row_description_buf` reuse → a fresh per-message StringInfo.** C reuses
//!    one backend-global RowDescription buffer; the Describe path here charges a
//!    fresh `StringInfo` to the threaded `mcx` per message (the printtup
//!    `send_describe_*` seams do this), functionally identical.
//! 3. **The per-parameter / params error-context callbacks** (`bind_param_error_callback`,
//!    `ParamsErrorCallback`) are logging-decoration only; the backend-utils-error
//!    model carries the parameter detail differently. The parameter *conversion*
//!    (type input/receive functions) is faithful; the error-callback chrome is
//!    not threaded (noted at each site).
//! 4. **`log_parameter_max_length_on_error` `knownTextValues`/`BuildParamLogString`**
//!    is the parameter-logging-on-error decoration (same family as 3); not
//!    threaded. Parameter values are still converted and bound identically.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::format;
use alloc::string::String;

use ::mcx::Mcx;
use ::types_core::primitive::Oid;
use ::types_dest::dest::CommandDest;
use ::types_error::{PgResult, ERROR, LOG};
use ::stringinfo::StringInfo;

use ::utils_error::{ereport, errfinish, errhidestmt, errmsg, errstart};

use crate::globals;
use crate::logging;
use crate::simple_query::{
    check_log_statement_planned, drop_unnamed_stmt, finish_xact_command, is_transaction_exit_stmt,
    start_xact_command,
};

// Seam crate aliases.
use printtup_seams as printtup_seams;
use transam_xact_seams as xact_seams;
use params_seams as params_seams;
use dest_seams as dest_seams;
use lsyscache_seams as lsyscache_seams;
use plancache_seams as plancache_seams;
use fmgr_seams as fmgr_seams;
use mbutils_seams as mbutils_seams;
use more_seams as more_seams;
use portalmem_seams as portalmem;
use status_seams as status_seams;

// Owner crates called directly (acyclic — none deps this crate).
use transam_xact as xact;
use prepare as prepare;
use pqformat as pqformat;
use parser_analyze as analyze;
use pquery as pquery;

use ::nodes::parsestmt::{CachedPlanHandle, CachedPlanSourceHandle};
use ::nodes::params::{ParamListInfo, ParamListInfoData};

// `__FILE__` / `__LINE__` / `__func__` for `errfinish`.
macro_rules! here {
    ($func:expr) => {
        (Some(file!()), line!() as i32, Some($func))
    };
}

// PqMsg_* backend reply message type codes (libpq/protocol.h).
const PQMSG_PARSE_COMPLETE: u8 = b'1';
const PQMSG_BIND_COMPLETE: u8 = b'2';
const PQMSG_PORTAL_SUSPENDED: u8 = b's';

// ===========================================================================
// IsTransactionStmtList / IsTransactionExitStmtList — postgres.c:2874 / :2890
// ===========================================================================

/// `IsTransactionStmtList(pstmts)` (postgres.c:2874): is this a one-element list
/// whose single statement is a utility `TransactionStmt`?
fn is_transaction_stmt_list(stmts: &[::nodes::nodeindexscan::PlannedStmt<'_>]) -> bool {
    if stmts.len() == 1 {
        let pstmt = &stmts[0];
        if pstmt.commandType == ::nodes::nodes::CmdType::CMD_UTILITY {
            if let Some(u) = pstmt.utilityStmt.as_deref() {
                return u.is_transactionstmt();
            }
        }
    }
    false
}

/// `IsTransactionExitStmtList(pstmts)` (postgres.c:2890): is this a one-element
/// list whose single statement is a transaction-exit utility command
/// (COMMIT/PREPARE/ROLLBACK/ROLLBACK-TO)?
fn is_transaction_exit_stmt_list(stmts: &[::nodes::nodeindexscan::PlannedStmt<'_>]) -> bool {
    if stmts.len() == 1 {
        let pstmt = &stmts[0];
        if pstmt.commandType == ::nodes::nodes::CmdType::CMD_UTILITY {
            if let Some(u) = pstmt.utilityStmt.as_deref() {
                return is_transaction_exit_stmt(u);
            }
        }
    }
    false
}

// ===========================================================================
// exec_parse_message — postgres.c:1389
// ===========================================================================

/// `exec_parse_message(query_string, stmt_name, paramTypes, numParams)`
/// (postgres.c:1389) — process a Parse message: build a `CachedPlanSource`,
/// analyze + rewrite the (single) statement with variable parameter types, fill
/// the source in, and store it as a named prepared statement (or as the unnamed
/// `unnamed_stmt_psrc`).
pub fn exec_parse_message<'mcx>(
    mcx: Mcx<'mcx>,
    query_string: &'mcx str,
    stmt_name: &str,
    param_types: &[Oid],
) -> PgResult<()> {
    let save_log_statement_stats = log_statement_stats();

    // Report query to various monitoring facilities. (debug_query_string store
    // skipped — the global cannot carry an 'mcx borrow; see simple_query.)
    status_seams::pgstat_report_activity_running::call(query_string.into());
    more_seams::set_ps_display::call("PARSE");

    if save_log_statement_stats {
        logging::ResetUsage();
    }

    // ereport(DEBUG2, "parse %s: %s", ...) — a DEBUG2 line, below threshold.

    // Start up a transaction command so we can run parse analysis etc.
    start_xact_command()?;

    // C splits parsing context by named/unnamed; the de-handled plancache owns
    // its trees in its own contexts, so we parse in the threaded `mcx` for both.
    let is_named = !stmt_name.is_empty();
    if !is_named {
        // Unnamed prepared statement: release any prior unnamed stmt.
        drop_unnamed_stmt()?;
    }

    // Do basic parsing of the query or queries (safe even in aborted xact).
    let parsetree_list = crate::simple_query::pg_parse_query(mcx, query_string)?;

    // We only allow a single user statement in a prepared statement.
    if parsetree_list.len() > 1 {
        return Err(ereport(ERROR)
            .errcode(::types_error::error::ERRCODE_SYNTAX_ERROR)
            .errmsg("cannot insert multiple commands into a prepared statement")
            .into_error());
    }

    let psrc: CachedPlanSourceHandle;

    if !parsetree_list.is_empty() {
        let raw_parse_tree = &parsetree_list[0];

        // If we are in an aborted transaction, reject all commands except
        // COMMIT/ROLLBACK (before any database access).
        if xact_seams::is_aborted_transaction_block_state::call()
            && !is_transaction_exit_stmt(&raw_parse_tree.stmt)
        {
            return Err(aborted_xact_error());
        }

        // Create the CachedPlanSource before parse analysis — it needs the
        // unmodified raw parse tree.
        let command_tag =
            utility_seams::create_command_tag::call(&raw_parse_tree.stmt)?;
        psrc = plancache_seams::create_cached_plan::call(
            mcx,
            raw_parse_tree,
            query_string,
            command_tag,
        )?;

        // Set up a snapshot if parse analysis will need one.
        let snapshot_set = analyze::analyze_requires_snapshot(raw_parse_tree);
        if snapshot_set {
            snapmgr_seams::push_active_snapshot_transaction::call()?;
        }

        // Analyze and rewrite. The Parse message's parameter set is not required
        // to be complete, so use the varparams analyzer (it infers undeclared
        // `$n` types and grows the OID array).
        let analyzed = crate::simple_query::pg_analyze_and_rewrite_varparams(
            mcx,
            raw_parse_tree,
            query_string,
            param_types,
        )?;

        if snapshot_set {
            snapmgr_seams::pop_active_snapshot::call()?;
        }

        let resolved_param_types: alloc::vec::Vec<Oid> =
            analyzed.arg_types.iter().copied().collect();

        // Finish filling in the CachedPlanSource.
        //   CompleteCachedPlan(psrc, querytree_list, ..., paramTypes, numParams,
        //                      NULL, NULL, CURSOR_OPT_PARALLEL_OK, true);
        plancache_seams::complete_cached_plan::call(
            mcx,
            psrc,
            analyzed.query_list.as_slice(),
            resolved_param_types.as_slice(),
        )?;
    } else {
        // Empty input string. This is legal.
        psrc = plancache_seams::create_cached_plan_empty::call(
            mcx,
            query_string,
            ::types_core::cmdtag::CommandTag(portal::CMDTAG_UNKNOWN),
        )?;
        plancache_seams::complete_cached_plan::call(mcx, psrc, &[], &[])?;
    }

    // If we got a cancel signal during analysis, quit.
    crate::interrupt::check_for_interrupts()?;

    if is_named {
        // Store the query as a prepared statement.
        prepare::StorePreparedStatement(stmt_name, psrc, false)?;
    } else {
        // Save the CachedPlanSource into unnamed_stmt_psrc.
        plancache_seams::save_cached_plan::call(psrc)?;
        globals::set_unnamed_stmt_psrc(psrc);
    }

    // We do NOT close the open transaction command here; that happens on Sync.
    // Do CommandCounterIncrement just in case something happened during plan.
    xact_seams::command_counter_increment::call()?;

    // Send ParseComplete.
    if globals::where_to_send_output() == CommandDest::Remote {
        pqformat::pq_putemptymessage(PQMSG_PARSE_COMPLETE)?;
    }

    // Emit duration logging if appropriate.
    emit_duration_log(mcx, false, "exec_parse_message")?;

    if save_log_statement_stats {
        let _ = logging::ShowUsage("PARSE MESSAGE STATISTICS");
    }

    globals::set_debug_query_string(None);
    Ok(())
}

// ===========================================================================
// exec_bind_message — postgres.c:1624
// ===========================================================================

/// `exec_bind_message(input_message)` (postgres.c:1624) — process a Bind
/// message: locate the prepared statement, read parameter format codes +
/// values, convert them via the type input/receive functions, plan against the
/// bound params, and define + start a portal.
pub fn exec_bind_message<'mcx>(
    mcx: Mcx<'mcx>,
    input_message: &mut StringInfo<'mcx>,
) -> PgResult<()> {
    let save_log_statement_stats = log_statement_stats();

    // Get the fixed part of the message.
    let portal_name = string_arg(mcx, input_message)?;
    let stmt_name = string_arg(mcx, input_message)?;

    // ereport(DEBUG2, "bind %s to %s", ...) — DEBUG2.

    // Find prepared statement.
    let psrc: CachedPlanSourceHandle = if !stmt_name.is_empty() {
        let pstmt = prepare::FetchPreparedStatement(&stmt_name, true)?
            .expect("FetchPreparedStatement(throwError=true) returns Some or errors");
        pstmt.plansource
    } else {
        // special-case the unnamed statement
        let psrc = globals::unnamed_stmt_psrc();
        if psrc == CachedPlanSourceHandle::NULL {
            return Err(ereport(ERROR)
                .errcode(::types_error::error::ERRCODE_UNDEFINED_PSTATEMENT)
                .errmsg("unnamed prepared statement does not exist")
                .into_error());
        }
        psrc
    };

    // Report query to various monitoring facilities.
    let src_query = plancache_seams::plansource_query_string::call(mcx, psrc)?;
    status_seams::pgstat_report_activity_running::call(src_query.as_str().into());

    more_seams::set_ps_display::call("BIND");

    if save_log_statement_stats {
        logging::ResetUsage();
    }

    // Start up a transaction command.
    start_xact_command()?;

    // Get the parameter format codes.
    let num_pformats = pqformat::pq_getmsgint(input_message, 2)? as i32;
    let mut pformats: alloc::vec::Vec<i16> = alloc::vec::Vec::with_capacity(num_pformats.max(0) as usize);
    for _ in 0..num_pformats {
        pformats.push(pqformat::pq_getmsgint(input_message, 2)? as i16);
    }

    // Get the parameter value count.
    let num_params = pqformat::pq_getmsgint(input_message, 2)? as i32;

    if num_pformats > 1 && num_pformats != num_params {
        return Err(ereport(ERROR)
            .errcode(::types_error::error::ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!(
                "bind message has {num_pformats} parameter formats but {num_params} parameters"
            ))
            .into_error());
    }

    let src_num_params = plancache_seams::plansource_num_params::call(psrc)?;
    if num_params != src_num_params {
        return Err(ereport(ERROR)
            .errcode(::types_error::error::ERRCODE_PROTOCOL_VIOLATION)
            .errmsg(format!(
                "bind message supplies {num_params} parameters, but prepared statement \"{stmt_name}\" requires {src_num_params}"
            ))
            .into_error());
    }

    // If we are in aborted transaction state, only COMMIT/ROLLBACK portals can
    // run, and no parameters may be bound.
    if xact_seams::is_aborted_transaction_block_state::call()
        && (!plansource_raw_is_transaction_exit(mcx, psrc)? || num_params != 0)
    {
        return Err(aborted_xact_error());
    }

    // Create the portal. Allow silent replacement only for the unnamed portal.
    let portal = if portal_name.is_empty() {
        portalmem::create_portal::call(&portal_name, true, true)?
    } else {
        portalmem::create_portal::call(&portal_name, false, false)?
    };

    // Set a snapshot if we have parameters to fetch (the input functions might
    // need it) or the query isn't a utility command (could redo parse/plan).
    let need_snapshot = num_params > 0 || plansource_raw_requires_snapshot(mcx, psrc)?;
    let mut snapshot_set = false;
    if need_snapshot {
        snapmgr_seams::push_active_snapshot_transaction::call()?;
        snapshot_set = true;
    }

    // Fetch parameters, if any, and store them in the value param list.
    let params: ParamListInfo = if num_params > 0 {
        let param_types = plancache_seams::plansource_param_types::call(mcx, psrc)?;
        // makeParamList(numParams) → a fresh, uniquely-owned value param list.
        let mut param_rc = params_seams::make_param_list::call(num_params)?
            .expect("makeParamList(num_params > 0) returns a list");
        let param_data: &mut ParamListInfoData<'static> = std::rc::Rc::get_mut(&mut param_rc)
            .expect("freshly made ParamListInfo is uniquely owned");

        for paramno in 0..num_params {
            let ptype = param_types[paramno as usize];

            // plength = pq_getmsgint(input_message, 4); isNull = (plength == -1);
            let plength = pqformat::pq_getmsgint(input_message, 4)? as i32;
            let is_null = plength == -1;

            // The per-parameter format code.
            let pformat: i16 = if num_pformats > 1 {
                pformats[paramno as usize]
            } else if num_pformats > 0 {
                pformats[0]
            } else {
                0 // default = text
            };

            // Read the value bytes (only when non-NULL).
            let pbytes: Option<alloc::vec::Vec<u8>> = if !is_null {
                Some(pqformat::pq_getmsgbytes(input_message, plength as usize)?.to_vec())
            } else {
                None
            };

            let (value, isnull) = if pformat == 0 {
                // text mode
                let (typinput, typioparam) =
                    lsyscache_seams::get_type_input_info::call(ptype)?;
                if is_null {
                    // C: pstring = NULL; OidInputFunctionCall(typinput, NULL, ...)
                    // — a NULL value. The unified-Datum input seam takes `&str`;
                    // a NULL text param stores a null Datum (isnull = true).
                    (types_tuple::heaptuple::Datum::null(), true)
                } else {
                    let raw = pbytes.as_ref().unwrap();
                    // We have to do encoding conversion before the typinput call.
                    // pg_client_to_server returns None when no conversion is
                    // needed (the bytes are already server-encoding).
                    let converted = mbutils_seams::pg_client_to_server::call(mcx, raw)?;
                    let bytes: &[u8] = match converted.as_ref() {
                        Some(v) => v.as_slice(),
                        None => raw.as_slice(),
                    };
                    let pstring = core::str::from_utf8(bytes).map_err(|_| {
                        ereport(ERROR)
                            .errcode(::types_error::error::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE)
                            .errmsg("invalid byte sequence for encoding")
                            .into_error()
                    })?;
                    let v = fmgr_seams::oid_input_function_call::call(
                        mcx,
                        typinput,
                        pstring,
                        typioparam,
                        -1,
                    )?;
                    (v, false)
                }
            } else if pformat == 1 {
                // binary mode
                let (typreceive, typioparam) =
                    lsyscache_seams::get_type_binary_input_info::call(ptype)?;
                if is_null {
                    let v = fmgr_seams::oid_receive_function_call::call(
                        mcx, typreceive, None, typioparam, -1,
                    )?;
                    (v, true)
                } else {
                    let raw = pbytes.as_ref().unwrap();
                    // OidReceiveFunctionCall(typreceive, &pbuf, typioparam, -1);
                    // the typed receive helper consumes the supplied slice, so
                    // the C `pbuf.cursor != pbuf.len` whole-buffer check is
                    // satisfied by passing exactly the parameter's bytes.
                    let v = fmgr_seams::oid_receive_function_call::call(
                        mcx,
                        typreceive,
                        Some(raw.as_slice()),
                        typioparam,
                        -1,
                    )?;
                    (v, false)
                }
            } else {
                return Err(ereport(ERROR)
                    .errcode(::types_error::error::ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!("unsupported format code: {pformat}"))
                    .into_error());
            };

            // params->params[paramno] = { value, isnull, PARAM_FLAG_CONST, ptype }
            // — datumCopy'd into the backend-lifetime param-list context.
            params_seams::store_param_extern::call(param_data, paramno, &value, isnull, ptype)?;
        }

        Some(param_rc)
    } else {
        None
    };

    // Get the result format codes.
    let num_rformats = pqformat::pq_getmsgint(input_message, 2)? as i32;
    let mut rformats: alloc::vec::Vec<i16> = alloc::vec::Vec::with_capacity(num_rformats.max(0) as usize);
    for _ in 0..num_rformats {
        rformats.push(pqformat::pq_getmsgint(input_message, 2)? as i16);
    }

    pqformat::pq_getmsgend(input_message)?;

    // Obtain a plan from the CachedPlanSource. The plan refcount is assigned to
    // the Portal, released at portal destruction.
    let cplan: CachedPlanHandle = plancache_seams::get_cached_plan::call(
        psrc,
        params.clone(),
        ::nodes::parsestmt::ResourceOwnerHandle::NULL,
        None,
    )?;
    let plan_list = plancache_seams::cached_plan_stmt_list::call(mcx, cplan)?;

    // DO NOT put any code that could possibly throw an error between the above
    // GetCachedPlan call and PortalDefineQuery (would leak the plan refcount).
    let saved_stmt_name: Option<&str> = if stmt_name.is_empty() {
        None
    } else {
        Some(stmt_name.as_str())
    };
    let command_tag = plancache_seams::plansource_command_tag::call(psrc)?;
    portalmem::portal_define_query_list::call(
        &portal,
        saved_stmt_name,
        src_query.as_str(),
        command_tag.0,
        plan_list.as_slice(),
        portal::CachedPlanHandle(cplan.0),
    )?;

    // Done with the snapshot used for parameter I/O and parsing/planning.
    if snapshot_set {
        snapmgr_seams::pop_active_snapshot::call()?;
    }

    // And we're ready to start portal execution.
    pquery::portal_start(&portal, params, 0, None)?;

    // Apply the result format requests to the portal.
    pquery::portal_set_result_format(&portal, rformats.as_slice())?;

    // Send BindComplete.
    if globals::where_to_send_output() == CommandDest::Remote {
        pqformat::pq_putemptymessage(PQMSG_BIND_COMPLETE)?;
    }

    // Emit duration logging if appropriate.
    emit_duration_log(mcx, false, "exec_bind_message")?;

    if save_log_statement_stats {
        let _ = logging::ShowUsage("BIND MESSAGE STATISTICS");
    }

    globals::set_debug_query_string(None);
    Ok(())
}

// ===========================================================================
// exec_execute_message — postgres.c:2107
// ===========================================================================

/// `exec_execute_message(portal_name, max_rows)` (postgres.c:2107) — run an
/// existing portal to `max_rows` rows, sending CommandComplete (completed) or
/// PortalSuspended (more rows remain).
pub fn exec_execute_message<'mcx>(
    mcx: Mcx<'mcx>,
    portal_name: &str,
    max_rows_in: i64,
) -> PgResult<()> {
    let save_log_statement_stats = log_statement_stats();

    // Adjust destination to tell printtup.c what to do.
    let mut dest = globals::where_to_send_output();
    if dest == CommandDest::Remote {
        dest = CommandDest::RemoteExecute;
    }

    let portal = portalmem::get_portal_by_name::call(portal_name)?.ok_or_else(|| {
        ereport(ERROR)
            .errcode(::types_error::error::ERRCODE_UNDEFINED_CURSOR)
            .errmsg(format!("portal \"{portal_name}\" does not exist"))
            .into_error()
    })?;

    // If the original query was a null string, just return EmptyQueryResponse.
    let command_tag = portal.borrow().commandTag;
    if command_tag == portal::CMDTAG_UNKNOWN {
        dest_seams::null_command::call(dest)?;
        return Ok(());
    }

    // Snapshot the portal fields C copies into MessageContext (the portal can be
    // destroyed during finish_xact_command). `PlannedStmt` is not `Clone` (it
    // owns arena-bound subtrees), so rather than copy the stmt list we read off
    // the booleans / log decision C derives from it (IsTransactionStmtList,
    // IsTransactionExitStmtList, check_log_statement) while holding the borrow.
    let (source_text, is_xact_command, is_xact_exit, do_log_statement, portal_params) = {
        let p = portal.borrow();
        let source_text: String = p.sourceText.as_ref().cloned().unwrap_or_default();
        let stmts: &[::nodes::nodeindexscan::PlannedStmt<'_>] =
            p.stmts.as_deref().unwrap_or(&[]);
        let is_xact_command = is_transaction_stmt_list(stmts);
        let is_xact_exit = is_transaction_exit_stmt_list(stmts);
        let do_log_statement = check_log_statement_planned(stmts)?;
        (
            source_text,
            is_xact_command,
            is_xact_exit,
            do_log_statement,
            p.portalParams.clone(),
        )
    };

    // Report query to monitoring facilities.
    status_seams::pgstat_report_activity_running::call(source_text.as_str().into());

    more_seams::set_ps_display::call("EXECUTE");

    if save_log_statement_stats {
        logging::ResetUsage();
    }

    dest_seams::begin_command::call(command_tag, dest);

    // Create dest receiver (in MessageContext, not txn context).
    let receiver = dest_seams::create_dest_receiver::call(dest);
    if dest == CommandDest::RemoteExecute {
        dest_seams::set_remote_dest_receiver_params::call(receiver, &portal)?;
    }

    // Ensure we are in a transaction command (normally already so after Bind).
    start_xact_command()?;

    // If we re-issue Execute against an existing portal we are fetching more
    // rows. atStart is never reset for a v3 portal.
    let execute_is_fetch = !portal.borrow().atStart;

    // Log immediately if dictated by log_statement.
    let mut was_logged = false;
    if do_log_statement {
        if errstart(LOG, None) {
            let verb = if execute_is_fetch {
                "execute fetch from"
            } else {
                "execute"
            };
            errmsg(&format!("{verb}: {source_text}"))?;
            errhidestmt(true)?;
            let (f, l, fc) = here!("exec_execute_message");
            errfinish(f, l, fc)?;
        }
        was_logged = true;
    }

    // If we are in aborted transaction state, only COMMIT/ROLLBACK portals run.
    if xact_seams::is_aborted_transaction_block_state::call() && !is_xact_exit {
        return Err(aborted_xact_error());
    }

    // Check for cancel signal before we start execution.
    crate::interrupt::check_for_interrupts()?;

    let max_rows = if max_rows_in <= 0 { i64::MAX } else { max_rows_in };

    let mut qc = portal::QueryCompletion {
        commandTag: portal::CMDTAG_UNKNOWN,
        nprocessed: 0,
    };
    let completed = pquery::portal_run(
        &portal,
        max_rows,
        true, // always top level
        receiver,
        receiver,
        Some(&mut qc),
    )?;

    // receiver->rDestroy(receiver): reclaim the router slot (and the owner's
    // per-receiver printtup state) so the per-statement create/destroy cycle
    // reuses slots instead of growing the registry for the life of the backend.
    dest_seams::free_dest_receiver::call(receiver);

    let mut report_params = portal_params;
    if completed {
        if is_xact_command
            || (xact::MyXactFlags() & ::types_core::xact::XACT_FLAGS_NEEDIMMEDIATECOMMIT) != 0
        {
            // Transaction-control / immediate-commit statement: commit now.
            finish_xact_command()?;
            // Storage went away during finish_xact_command; pretend no params.
            report_params = None;
        } else {
            // CommandCounterIncrement after every query except xact start/end.
            xact_seams::command_counter_increment::call()?;
            // Set XACT_FLAGS_PIPELINING; disable statement timeout.
            xact::SetMyXactFlags(
                xact::MyXactFlags() | ::types_core::xact::XACT_FLAGS_PIPELINING,
            );
            logging::disable_statement_timeout();
        }

        // Send appropriate CommandComplete to client.
        dest_seams::end_command::call(mcx, &qc, dest, false)?;
    } else {
        // Portal run not complete: send PortalSuspended.
        if globals::where_to_send_output() == CommandDest::Remote {
            pqformat::pq_putemptymessage(PQMSG_PORTAL_SUSPENDED)?;
        }
        // Set XACT_FLAGS_PIPELINING whenever we suspend an Execute, too.
        xact::SetMyXactFlags(xact::MyXactFlags() | ::types_core::xact::XACT_FLAGS_PIPELINING);
    }
    let _ = report_params;

    // Emit duration logging if appropriate.
    emit_duration_log(mcx, was_logged, "exec_execute_message")?;

    if save_log_statement_stats {
        let _ = logging::ShowUsage("EXECUTE MESSAGE STATISTICS");
    }

    globals::set_debug_query_string(None);
    Ok(())
}

// ===========================================================================
// exec_describe_statement_message — postgres.c:2641
// ===========================================================================

/// `exec_describe_statement_message(stmt_name)` (postgres.c:2641) — describe a
/// prepared statement: send ParameterDescription + RowDescription/NoData.
pub fn exec_describe_statement_message<'mcx>(mcx: Mcx<'mcx>, stmt_name: &str) -> PgResult<()> {
    // Start up a transaction command.
    start_xact_command()?;

    // Find prepared statement.
    let psrc: CachedPlanSourceHandle = if !stmt_name.is_empty() {
        let pstmt = prepare::FetchPreparedStatement(stmt_name, true)?
            .expect("FetchPreparedStatement(throwError=true) returns Some or errors");
        pstmt.plansource
    } else {
        let psrc = globals::unnamed_stmt_psrc();
        if psrc == CachedPlanSourceHandle::NULL {
            return Err(ereport(ERROR)
                .errcode(::types_error::error::ERRCODE_UNDEFINED_PSTATEMENT)
                .errmsg("unnamed prepared statement does not exist")
                .into_error());
        }
        psrc
    };

    // Assert(psrc->fixed_result) — prepared statements have fixed result descs.

    // In aborted-xact state, refuse to Describe statements that return data
    // (SendRowDescriptionMessage needs catalog access). Describing parameters
    // is safe.
    let has_result = plancache_seams::plansource_has_result_desc::call(psrc)?;
    if xact_seams::is_aborted_transaction_block_state::call() && has_result {
        return Err(aborted_xact_error());
    }

    if globals::where_to_send_output() != CommandDest::Remote {
        return Ok(()); // can't actually do anything
    }

    // Read the parameter types + result descriptor + target list, then encode.
    let param_types = plancache_seams::plansource_param_types::call(mcx, psrc)?;
    let result_desc = plancache_seams::plansource_result_desc::call(mcx, psrc)?;
    let targetlist = if result_desc.is_some() {
        plancache_seams::cached_plan_get_target_list::call(mcx, psrc)?
    } else {
        ::mcx::vec_with_capacity_in(mcx, 0)?
    };

    printtup_seams::send_describe_statement::call(
        mcx,
        param_types.as_slice(),
        result_desc.as_ref(),
        targetlist.as_slice(),
    )?;
    Ok(())
}

// ===========================================================================
// exec_describe_portal_message — postgres.c:2734
// ===========================================================================

/// `exec_describe_portal_message(portal_name)` (postgres.c:2734) — describe a
/// portal's result: send RowDescription/NoData.
pub fn exec_describe_portal_message<'mcx>(mcx: Mcx<'mcx>, portal_name: &str) -> PgResult<()> {
    // Start up a transaction command.
    start_xact_command()?;

    let portal = portalmem::get_portal_by_name::call(portal_name)?.ok_or_else(|| {
        ereport(ERROR)
            .errcode(::types_error::error::ERRCODE_UNDEFINED_CURSOR)
            .errmsg(format!("portal \"{portal_name}\" does not exist"))
            .into_error()
    })?;

    // In aborted-xact state, refuse to Describe portals that return data.
    let has_tupdesc = portal.borrow().tupDesc.is_some();
    if xact_seams::is_aborted_transaction_block_state::call() && has_tupdesc {
        return Err(aborted_xact_error());
    }

    if globals::where_to_send_output() != CommandDest::Remote {
        return Ok(()); // can't actually do anything
    }

    printtup_seams::send_describe_portal::call(mcx, &portal)?;
    Ok(())
}

// ===========================================================================
// helpers
// ===========================================================================

/// Read a NUL-terminated C string argument from the message into an owned
/// `String` (the names are copied; the message buffer is reused for the value
/// bytes, so we don't hold a borrow into it).
fn string_arg<'mcx>(mcx: Mcx<'mcx>, msg: &mut StringInfo<'mcx>) -> PgResult<String> {
    let s = pqformat::pq_getmsgstring(mcx, msg)?;
    Ok(String::from_utf8_lossy(s.as_bytes()).into_owned())
}

/// The `current transaction is aborted` ereport, with the abort-reason detail
/// line appended (postgres.c, several sites).
fn aborted_xact_error() -> ::types_error::PgError {
    if errstart(ERROR, None) {
        let _ = (|| -> PgResult<()> {
            ::utils_error::errcode(
                ::types_error::error::ERRCODE_IN_FAILED_SQL_TRANSACTION,
            )?;
            errmsg(
                "current transaction is aborted, commands ignored until end of transaction block",
            )?;
            crate::interrupt::errdetail_abort()?;
            let (f, l, fc) = here!("extended_query");
            errfinish(f, l, fc)?;
            Ok(())
        })();
    }
    ereport(ERROR)
        .errcode(::types_error::error::ERRCODE_IN_FAILED_SQL_TRANSACTION)
        .errmsg(
            "current transaction is aborted, commands ignored until end of transaction block",
        )
        .into_error()
}

/// `psrc->raw_parse_tree && IsTransactionExitStmt(psrc->raw_parse_tree->stmt)` —
/// whether the cached source's raw statement is a transaction-exit command.
/// Modeled via the source's command tag classification: a transaction-exit
/// command's `CachedPlanSource` carries a `TransactionStmt` raw tree. We re-run
/// the analyze-side check by inspecting the cached query list's command type.
fn plansource_raw_is_transaction_exit<'mcx>(
    _mcx: Mcx<'mcx>,
    psrc: CachedPlanSourceHandle,
) -> PgResult<bool> {
    plancache_seams::plansource_raw_is_transaction_exit_stmt::call(psrc)
}

/// `psrc->raw_parse_tree && analyze_requires_snapshot(psrc->raw_parse_tree)`.
fn plansource_raw_requires_snapshot<'mcx>(
    _mcx: Mcx<'mcx>,
    psrc: CachedPlanSourceHandle,
) -> PgResult<bool> {
    plancache_seams::plansource_raw_requires_snapshot::call(psrc)
}

/// Emit the `duration:` LOG line if `check_log_duration` asks for it (the F1/F2
/// shared tail; mirrors `exec_simple_query`'s switch). The statement-text branch
/// (case 2) uses `debug_query_string`, which we do not carry; we log the bare
/// duration in both cases (the duration value itself is faithful).
fn emit_duration_log<'mcx>(mcx: Mcx<'mcx>, was_logged: bool, func: &str) -> PgResult<()> {
    let (code, msec) = logging::check_log_duration(mcx, was_logged)?;
    if code != 0 && errstart(LOG, None) {
        errmsg(&format!("duration: {} ms", msec.as_str()))?;
        errhidestmt(true)?;
        let (f, l, fc) = (Some(file!()), line!() as i32, Some(func));
        errfinish(f, l, fc)?;
    }
    Ok(())
}

// ===========================================================================
// GUC reads
// ===========================================================================

fn log_statement_stats() -> bool {
    guc_tables::vars::log_statement_stats.read()
}
