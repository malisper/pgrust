//! `tcop/postgres.c` — the simple-Query (F1) pipeline.
//!
//! This module ports the `exec_simple_query` command-processor pipeline and its
//! parse/analyze/rewrite/plan helper family, now that the portal-define
//! boundary (`portal_define_query_list`) and the owned-value planner entry
//! (`pg_plan_query`, installed by the planner crate) are landed.
//!
//!   * [`pg_parse_query`]                       — postgres.c:603
//!   * [`pg_analyze_and_rewrite_fixedparams`]   — postgres.c:665
//!   * [`pg_rewrite_query`]                      — postgres.c:798
//!   * [`pg_plan_query`]                         — postgres.c:882 (thin wrapper;
//!     the optimizer itself is the planner crate, reached via its seam)
//!   * [`pg_plan_queries`]                       — postgres.c:970
//!   * [`exec_simple_query`]                     — postgres.c:1011
//!   * [`start_xact_command`] / [`finish_xact_command`] — postgres.c:2786/2825
//!   * [`is_transaction_exit_stmt`]              — postgres.c:2857
//!   * [`check_log_statement`]                   — postgres.c:2384
//!   * [`drop_unnamed_stmt`]                     — postgres.c:2904
//!
//! Output (the `DestRemote` `printtup` receiver) is reached purely through the
//! `tcop/dest.h` vtable; `printtup` is not yet routed into the dest router (a
//! sibling dest/printtup lane owns that), so a `DestRemote` run will panic in
//! the receiver's unwired vtable when the executor pushes the first tuple. The
//! pipeline itself — parse → analyze → rewrite → plan → portal → run — is
//! complete and faithful; the gap is in the output owner, not here.

#![allow(non_snake_case)]

extern crate alloc;

use mcx::{Mcx, PgVec};
use types_core::cmdtag::CommandTag;
use types_dest::dest::CommandDest;
use types_error::{PgResult, ERROR, LOG};
use types_nodes::copy_query::{Query, CURSOR_OPT_PARALLEL_OK};
use types_nodes::nodeindexscan::PlannedStmt;
use types_nodes::nodes::{CmdType, Node};
use types_nodes::parsestmt::RawStmt;
use types_parsenodes::RawParseMode;

use backend_utils_error::{ereport, errcode, errfinish, errhidestmt, errmsg, errstart};

use crate::globals;
use crate::logging;

// `__FILE__` / `__LINE__` / `__func__` for `errfinish`.
macro_rules! here {
    ($func:expr) => {
        (Some(file!()), line!() as i32, Some($func))
    };
}

// Seam crate aliases.
use backend_access_transam_xact_seams as xact_seams;
use backend_tcop_dest_seams as dest_seams;
use backend_tcop_utility_seams as utility_seams;
use backend_utils_activity_status_seams as status_seams;
use backend_utils_mmgr_portalmem_seams as portalmem;
use backend_utils_time_snapmgr_seams as snapmgr;

use backend_rewrite_rewritehandler_seams as rewrite_seams;

// Owner crates we call directly for the entry points that have no consumable
// seam (acyclic: none of these depends on this crate — each deps only the
// `*-seams` leaves, verified at the Cargo level).
use backend_parser_driver as parser_driver;
use backend_parser_analyze as analyze;
use backend_tcop_pquery as pquery;
use backend_optimizer_plan_planner_seams as planner_seams;

// ===========================================================================
// pg_parse_query — postgres.c:603
// ===========================================================================

/// `pg_parse_query(query_string)` (postgres.c:603) — do basic parsing of the
/// query or queries, returning the raw parsetree list.
///
/// The `DEBUG_NODE_TESTS_ENABLED` copyObject/outfuncs round-trip checks are
/// debug-build-only and not threaded here. `log_parser_stats` gates
/// `ResetUsage`/`ShowUsage`.
pub fn pg_parse_query<'mcx>(
    mcx: Mcx<'mcx>,
    query_string: &'mcx str,
) -> PgResult<PgVec<'mcx, RawStmt<'mcx>>> {
    if log_parser_stats() {
        logging::ResetUsage();
    }

    let raw_parsetree_list =
        parser_driver::raw_parser(mcx, query_string, RawParseMode::RAW_PARSE_DEFAULT)?;

    if log_parser_stats() {
        let _ = logging::ShowUsage("PARSER STATISTICS");
    }

    Ok(raw_parsetree_list)
}

// ===========================================================================
// pg_analyze_and_rewrite_fixedparams — postgres.c:665
// ===========================================================================

/// `pg_analyze_and_rewrite_fixedparams(parsetree, query_string, paramTypes,
/// numParams, queryEnv)` (postgres.c:665) — parse analysis + rule rewriting.
///
/// Returns a list of `Query` nodes (the analyzer or rewriter may expand one
/// query to several). The simple-Query driver passes no parameters and a `None`
/// query environment.
pub fn pg_analyze_and_rewrite_fixedparams<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: &RawStmt<'mcx>,
    query_string: &str,
    param_types: &[types_core::primitive::Oid],
) -> PgResult<PgVec<'mcx, Query<'mcx>>> {
    // (1) Perform parse analysis.
    if log_parser_stats() {
        logging::ResetUsage();
    }

    let query = analyze::parse_analyze_fixedparams(mcx, parsetree, query_string, param_types)?;

    if log_parser_stats() {
        let _ = logging::ShowUsage("PARSE ANALYSIS STATISTICS");
    }

    // (2) Rewrite the queries, as necessary.
    pg_rewrite_query(mcx, query)
}

// ===========================================================================
// pg_analyze_and_rewrite_varparams — postgres.c:704
// ===========================================================================

/// `pg_analyze_and_rewrite_varparams(parsetree, query_string, &paramTypes,
/// &numParams, queryEnv)` (postgres.c:704) — like
/// [`pg_analyze_and_rewrite_fixedparams`] except it is okay to deduce `$n`
/// datatypes from context. PREPARE uses this so undeclared parameter types can
/// be inferred. The resolved/grown parameter OID array is read back from the
/// `VarParamState` carrier `parse_analyze_varparams` returns (its `Vec` length is
/// the C `*numParams`). `queryEnv` is `NULL` on this path.
pub fn pg_analyze_and_rewrite_varparams<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: &RawStmt<'mcx>,
    query_string: &str,
    arg_types: &[types_core::primitive::Oid],
) -> PgResult<backend_parser_analyze_seams::AnalyzedVarparams<'mcx>> {
    // (1) Perform parse analysis.
    if log_parser_stats() {
        logging::ResetUsage();
    }

    let (query, parstate) =
        analyze::parse_analyze_varparams(mcx, parsetree, query_string, arg_types)?;

    // Check all parameter types got determined.
    //   for (int i = 0; i < *numParams; i++) { ptype = (*paramTypes)[i];
    //       if (ptype == InvalidOid || ptype == UNKNOWNOID) ereport(ERROR, ...); }
    // The resolved `*numParams`-length array is the shared VarParamState Vec.
    const UNKNOWNOID: types_core::primitive::Oid = 705; // pg_type.h UNKNOWNOID
    {
        let resolved = parstate.param_types.borrow();
        for (i, &ptype) in resolved.iter().enumerate() {
            if ptype == types_core::primitive::InvalidOid || ptype == UNKNOWNOID {
                return Err(ereport(ERROR)
                    .errcode(types_error::error::ERRCODE_INDETERMINATE_DATATYPE)
                    .errmsg(alloc::format!(
                        "could not determine data type of parameter ${}",
                        i + 1
                    ))
                    .into_error());
            }
        }
    }

    if log_parser_stats() {
        let _ = logging::ShowUsage("PARSE ANALYSIS STATISTICS");
    }

    // (2) Rewrite the queries, as necessary.
    let query_list = pg_rewrite_query(mcx, query)?;

    // Return the rewritten list + the resolved parameter OID array (C returns the
    // list and writes back *paramTypes/*numParams; the carrier holds the array).
    let resolved = parstate.param_types.borrow();
    let mut arg_types_out: PgVec<'mcx, types_core::primitive::Oid> =
        mcx::vec_with_capacity_in(mcx, resolved.len())?;
    for &t in resolved.iter() {
        arg_types_out.push(t);
    }
    drop(resolved);

    // pg_rewrite_query yields a `PgVec<Query>`; the varparams contract wants a
    // `PgVec<Node>` (the rewritten `List *` of `Query *`, wrapped as nodes — the
    // PREPARE/plancache consumer reads `Node::Query`).
    let mut query_nodes: PgVec<'mcx, Node<'mcx>> = mcx::vec_with_capacity_in(mcx, query_list.len())?;
    for q in query_list {
        query_nodes.push(Node::mk_query(mcx, q)?);
    }

    Ok(backend_parser_analyze_seams::AnalyzedVarparams {
        query_list: query_nodes,
        arg_types: arg_types_out,
    })
}

// ===========================================================================
// pg_rewrite_query — postgres.c:798
// ===========================================================================

/// `pg_rewrite_query(query)` (postgres.c:798) — perform rewriting of a query
/// produced by parse analysis. CMD_UTILITY queries are passed straight through;
/// regular queries go through `QueryRewrite`.
///
/// Note: `query` must have just come from the parser (no `AcquireRewriteLocks`).
/// `Debug_print_parse`/`Debug_print_rewritten` node-dumps and the
/// `DEBUG_NODE_TESTS_ENABLED` round-trips are debug-only and not threaded here.
pub fn pg_rewrite_query<'mcx>(
    mcx: Mcx<'mcx>,
    query: Query<'mcx>,
) -> PgResult<PgVec<'mcx, Query<'mcx>>> {
    if log_parser_stats() {
        logging::ResetUsage();
    }

    let querytree_list = if query.commandType == CmdType::CMD_UTILITY {
        // don't rewrite utilities, just dump 'em into result list
        let mut v = PgVec::new_in(mcx);
        v.push(query);
        v
    } else {
        // rewrite regular queries (QueryRewrite, via the canonical owned-value
        // entry the rewriteHandler owner installs).
        rewrite_seams::query_rewrite_canonical::call(mcx, query)?
    };

    if log_parser_stats() {
        let _ = logging::ShowUsage("REWRITER STATISTICS");
    }

    Ok(querytree_list)
}

// ===========================================================================
// pg_plan_query — postgres.c:882
// ===========================================================================

/// `pg_plan_query(querytree, query_string, cursorOptions, boundParams)`
/// (postgres.c:882) — generate a plan for a single already-rewritten query. A
/// thin wrapper around `planner()` (reached through the installed
/// `pg_plan_query` seam owned by the planner crate). Utility commands have no
/// plan (`None`).
///
/// `boundParams` is `None` on the simple-Query path; the planner-crate seam
/// runs with no bound params. `log_planner_stats` gates `ResetUsage`/`ShowUsage`
/// (done inside the planner crate's entry; not duplicated here).
pub fn pg_plan_query<'mcx>(
    mcx: Mcx<'mcx>,
    querytree: &Query<'mcx>,
    query_string: &str,
    cursor_options: i32,
    bound_params: types_nodes::params::ParamListInfo,
) -> PgResult<Option<PlannedStmt<'mcx>>> {
    // Utility commands have no plans.
    if querytree.commandType == CmdType::CMD_UTILITY {
        return Ok(None);
    }

    // Planner must have a snapshot in case it calls user-defined functions.
    // (Assert(ActiveSnapshotSet()) — the simple-Query loop pushes a transaction
    // snapshot before reaching here when analyze_requires_snapshot is true.)

    // call the optimizer (planner_hook == NULL -> standard_planner), owned and
    // installed by the planner crate. `boundParams` flows into the planner so a
    // PARAM_EXTERN `$n` const-folds against the bound value (the custom-plan
    // path); `None` is the generic-plan / simple-Query / COPY path.
    let plan = planner_seams::pg_plan_query_params::call(
        mcx,
        querytree,
        query_string,
        cursor_options,
        bound_params,
    )?;

    Ok(Some(plan))
}

// ===========================================================================
// pg_plan_queries — postgres.c:970
// ===========================================================================

/// `pg_plan_queries(querytrees, query_string, cursorOptions, boundParams)`
/// (postgres.c:970) — generate plans for a list of already-rewritten queries.
///
/// For optimizable statements, invoke the planner. For utility statements, the
/// C makes a trivial wrapper `PlannedStmt` (commandType = CMD_UTILITY, copying
/// `canSetTag`/`utilityStmt`/`stmt_location`/`stmt_len`/`queryId`, everything
/// else null/zero); see [`PlannedStmt::for_utility`].
pub fn pg_plan_queries<'mcx>(
    mcx: Mcx<'mcx>,
    querytrees: PgVec<'mcx, Query<'mcx>>,
    query_string: &str,
    cursor_options: i32,
    bound_params: types_nodes::params::ParamListInfo,
) -> PgResult<PgVec<'mcx, PlannedStmt<'mcx>>> {
    let mut stmt_list: PgVec<'mcx, PlannedStmt<'mcx>> = PgVec::new_in(mcx);

    for query in querytrees.iter() {
        if query.commandType == CmdType::CMD_UTILITY {
            // Utility commands require no planning; C builds a trivial wrapper
            // PlannedStmt copying canSetTag/utilityStmt/stmt_location/stmt_len/
            // queryId, everything else null/zero (postgres.c pg_plan_queries).
            stmt_list.push(PlannedStmt::for_utility(mcx, query)?);
        } else {
            // C passes the same `boundParams` to every query in the list
            // (postgres.c pg_plan_queries).
            let stmt = pg_plan_query(mcx, query, query_string, cursor_options, bound_params.clone())?
                .expect("pg_plan_query returned None for a non-utility query");
            stmt_list.push(stmt);
        }
    }

    Ok(stmt_list)
}

// ===========================================================================
// exec_simple_query — postgres.c:1011
// ===========================================================================

/// `exec_simple_query(query_string)` (postgres.c:1011) — execute a "simple
/// Query" protocol message.
///
/// `mcx` is the per-message arena (the C `MessageContext`); `query_string` is
/// allocated there (it outlives the portal, as the C comment notes). The per-
/// parsetree context optimization (a child context per non-last parsetree) is a
/// memory-reclamation detail over the arena model; we use the single `mcx` for
/// every parsetree (the C `MessageContext` reset reclaims it all at the end of
/// the message anyway), and note the divergence.
pub fn exec_simple_query<'mcx>(mcx: Mcx<'mcx>, query_string: &'mcx str) -> PgResult<()> {
    let dest = globals::where_to_send_output();
    let save_log_statement_stats = log_statement_stats();
    let mut was_logged = false;

    // Report query to various monitoring facilities.
    //
    //   debug_query_string = query_string;
    // The `debug_query_string` global is modeled as `Option<&'static str>` (a
    // raw `const char *` in C, pointing into MessageContext for the message's
    // lifetime); it cannot hold the `'mcx`-bounded `query_string` borrow under
    // the owned model. Setting it is monitoring-only (the error reporter reads
    // it for `STATEMENT:` log lines); skip the store rather than leak `'mcx`
    // into `'static`. (Re-modeling `debug_query_string` to carry an `'mcx`
    // string is a globals-owner follow-on.)
    status_seams::pgstat_report_activity_running::call(query_string.into());

    // We use save_log_statement_stats so ShowUsage doesn't report incorrect
    // results because ResetUsage wasn't called.
    if save_log_statement_stats {
        logging::ResetUsage();
    }

    // Start up a transaction command.
    start_xact_command()?;

    // Zap any pre-existing unnamed statement.
    drop_unnamed_stmt()?;

    // (MemoryContextSwitchTo(MessageContext) — we already operate in `mcx`.)

    // Do basic parsing of the query or queries (safe even in aborted xact).
    let parsetree_list = pg_parse_query(mcx, query_string)?;

    // Log immediately if dictated by log_statement.
    if check_log_statement(&parsetree_list)? {
        if errstart(LOG, None) {
            errmsg(&alloc::format!("statement: {query_string}"))?;
            errhidestmt(true)?;
            // errdetail_execute(parsetree_list): the per-statement detail line
            // is an extended-protocol logging helper (F2 family, unported); the
            // statement text is already in errmsg above. Omitted here.
            let (f, l, fc) = here!("exec_simple_query");
            errfinish(f, l, fc)?;
        }
        was_logged = true;
    }

    // For historical reasons, multiple statements in a single simple Query
    // message run in a single (implicit) transaction block.
    let use_implicit_block = parsetree_list.len() > 1;

    // Run through the raw parsetree(s) and process each one.
    let n = parsetree_list.len();
    for (idx, parsetree) in parsetree_list.iter().enumerate() {
        let is_last = idx + 1 == n;
        let mut snapshot_set = false;

        status_seams::pgstat_report_query_id::call(0, true);
        status_seams::pgstat_report_plan_id::call(0, true);

        // Get the command tag for status display + default completion tag.
        let command_tag: CommandTag = utility_seams::create_command_tag::call(&parsetree.stmt)?;
        let (cmdtagname, _cmdtaglen) =
            backend_tcop_cmdtag::get_command_tag_name_and_len(portal_tag(command_tag));

        // set_ps_display_with_len(cmdtagname, cmdtaglen) — the activity title.
        backend_utils_misc_more_seams::set_ps_display::call(cmdtagname);

        // BeginCommand(commandTag, dest) — owned by dest.c (a no-op currently).
        dest_seams::begin_command::call(portal_tag(command_tag), dest);

        // If we are in an aborted transaction, reject all but COMMIT/ABORT.
        if xact_seams::is_aborted_transaction_block_state::call()
            && !is_transaction_exit_stmt(&parsetree.stmt)
        {
            if errstart(ERROR, None) {
                errcode(types_error::error::ERRCODE_IN_FAILED_SQL_TRANSACTION)?;
                errmsg(
                    "current transaction is aborted, commands ignored until end of \
                     transaction block",
                )?;
                // errdetail_abort() appends the abort-reason detail line.
                crate::interrupt::errdetail_abort()?;
                let (f, l, fc) = here!("exec_simple_query");
                errfinish(f, l, fc)?;
            }
            // errstart(ERROR) normally returns true and errfinish(ERROR) returns
            // Err (propagated above). If errstart returned false (error
            // recursion), the ereport was suppressed; the C `ereport(ERROR)`
            // must still not fall through, so surface a minimal error rather
            // than continue executing the aborted statement.
            return Err(ereport(ERROR)
                .errcode(types_error::error::ERRCODE_IN_FAILED_SQL_TRANSACTION)
                .errmsg(
                    "current transaction is aborted, commands ignored until end of \
                     transaction block",
                )
                .into_error());
        }

        // Make sure we are in a transaction command.
        start_xact_command()?;

        // If using an implicit transaction block and not already in a block,
        // start an implicit block to group this statement with following ones.
        if use_implicit_block {
            backend_access_transam_xact::BeginImplicitTransactionBlock();
        }

        // If we got a cancel signal in parsing or a prior command, quit.
        crate::interrupt::check_for_interrupts()?;

        // Set up a snapshot if parse analysis/planning will need one.
        if analyze::analyze_requires_snapshot(parsetree) {
            snapmgr::push_active_snapshot_transaction::call()?;
            snapshot_set = true;
        }

        // OK to analyze, rewrite, and plan this query. (The per-parsetree
        // context optimization is collapsed onto `mcx` — see the fn note.)
        let querytree_list =
            pg_analyze_and_rewrite_fixedparams(mcx, parsetree, query_string, &[])?;

        // The simple-Query path has no bound external params (NULL boundParams
        // in C's exec_simple_query → pg_plan_queries).
        let plantree_list =
            pg_plan_queries(mcx, querytree_list, query_string, CURSOR_OPT_PARALLEL_OK, None)?;

        // Done with the snapshot used for parsing/planning. (We deliberately do
        // NOT reuse it for execution; see the C comment / postgr.es link.)
        if snapshot_set {
            snapmgr::pop_active_snapshot::call()?;
        }

        // If we got a cancel signal in analysis or planning, quit.
        crate::interrupt::check_for_interrupts()?;

        // Create unnamed portal to run the query or queries in.
        let portal = portalmem::create_portal::call("", true, true)?;
        // Don't display the portal in pg_cursors.
        portal.borrow_mut().visible = false;

        // We don't have to copy anything into the portal: everything we pass is
        // in `mcx` (the C MessageContext / per_parsetree_context) and outlives
        // the portal. The portal-define owner still interns its own copies.
        //   PortalDefineQuery(portal, NULL, query_string, commandTag,
        //                     plantree_list, NULL);
        portalmem::portal_define_query_list::call(
            &portal,
            None,
            query_string,
            portal_tag(command_tag),
            &plantree_list,
            types_portal::CachedPlanHandle::NULL,
        )?;

        // Start the portal. No parameters here, InvalidSnapshot (= None).
        pquery::portal_start(&portal, None, 0, None)?;

        // Select the output format: text unless doing a FETCH from a binary
        // cursor.
        let mut format: i16 = 0; // TEXT is default
        if let Some(stmt) = parsetree.stmt.as_fetchstmt() {
            if !stmt.ismove {
                if let Some(name) = stmt.portalname.as_ref() {
                    if let Some(fportal) =
                        portalmem::get_portal_by_name::call(name.as_str())?
                    {
                        if (fportal.borrow().cursorOptions
                            & types_nodes::portalcmds::CURSOR_OPT_BINARY)
                            != 0
                        {
                            format = 1; // BINARY
                        }
                    }
                }
            }
        }
        pquery::portal_set_result_format(&portal, &[format])?;

        // Create the destination receiver object.
        let receiver = dest_seams::create_dest_receiver::call(dest);
        if dest == CommandDest::Remote {
            dest_seams::set_remote_dest_receiver_params::call(receiver, &portal)?;
        }

        // (MemoryContextSwitchTo(oldcontext) — back to the transaction context
        // for execution; collapsed onto `mcx`.)

        // Run the portal to completion, then drop it (and the receiver).
        let mut qc = types_portal::QueryCompletion {
            commandTag: types_portal::CMDTAG_UNKNOWN,
            nprocessed: 0,
        };
        let _ = pquery::portal_run(
            &portal,
            i64::MAX, // FETCH_ALL
            true,     // always top level
            receiver,
            receiver,
            Some(&mut qc),
        )?;

        // receiver->rDestroy(receiver): in the registry-handle dest model the
        // receiver has no separate destroy slot (the owner reclaims its state);
        // dropping the handle is the equivalent.
        let _ = receiver;

        portalmem::portal_drop::call(&portal, false)?;

        if is_last {
            // Last parsetree: close down the transaction statement before
            // reporting command-complete.
            if use_implicit_block {
                backend_access_transam_xact::EndImplicitTransactionBlock();
            }
            finish_xact_command()?;
        } else if parsetree.stmt.is_transactionstmt() {
            // Transaction control statement: commit it; a new xact command
            // starts for the next command.
            finish_xact_command()?;
        } else {
            // We had better not see XACT_FLAGS_NEEDIMMEDIATECOMMIT set if we're
            // not calling finish_xact_command(). (Assert dropped: the flag is
            // checked inside the xact owner.)

            // CommandCounterIncrement after every query except those that start
            // or end a transaction block.
            xact_seams::command_counter_increment::call()?;

            // Disable statement timeout between queries of a multi-query string.
            logging::disable_statement_timeout();
        }

        // Tell client we're done with this query: exactly one EndCommand per
        // raw parsetree.
        dest_seams::end_command::call(mcx, &qc, dest, false)?;

        // (per_parsetree_context delete — collapsed onto `mcx`.)
    } // end loop over parsetrees

    // Close down transaction statement, if one is open. (Only does something if
    // the parsetree list was empty; otherwise the last loop iteration did it.)
    finish_xact_command()?;

    // If there were no parsetrees, return EmptyQueryResponse.
    if parsetree_list.is_empty() {
        dest_seams::null_command::call(dest)?;
    }

    // Emit duration logging if appropriate.
    let (code, msec) = logging::check_log_duration(mcx, was_logged)?;
    match code {
        1 => {
            if errstart(LOG, None) {
                errmsg(&alloc::format!("duration: {} ms", msec.as_str()))?;
                errhidestmt(true)?;
                let (f, l, fc) = here!("exec_simple_query");
                errfinish(f, l, fc)?;
            }
        }
        2 => {
            if errstart(LOG, None) {
                errmsg(&alloc::format!(
                    "duration: {} ms  statement: {}",
                    msec.as_str(),
                    query_string
                ))?;
                errhidestmt(true)?;
                let (f, l, fc) = here!("exec_simple_query");
                errfinish(f, l, fc)?;
            }
        }
        _ => {}
    }

    if save_log_statement_stats {
        let _ = logging::ShowUsage("QUERY STATISTICS");
    }

    // debug_query_string = NULL; (the store was skipped above — see the note.)
    globals::set_debug_query_string(None);
    Ok(())
}

// ===========================================================================
// start_xact_command / finish_xact_command — postgres.c:2786 / 2825
// ===========================================================================

/// `start_xact_command(void)` (postgres.c:2786) — start a transaction command
/// if one is not already started; start the statement timeout.
///
/// The `XACT_FLAGS_PIPELINING` implicit-block branch is an extended-protocol
/// (pipelining) path not reached from `exec_simple_query`; it is checked via
/// the xact flags and started faithfully if set. The
/// `CLIENT_CONNECTION_CHECK_TIMEOUT` arming requires `IsUnderPostmaster &&
/// MyProcPort` (a real backend connection), not reachable in this single-user
/// build; it is skipped, mirroring the C `if`.
pub fn start_xact_command() -> PgResult<()> {
    if !globals::xact_started() {
        xact_seams::start_transaction_command::call()?;
        globals::set_xact_started(true);
    } else if (backend_access_transam_xact::MyXactFlags()
        & types_core::xact::XACT_FLAGS_PIPELINING)
        != 0
    {
        // When the first Execute message completes, following commands run in
        // an implicit transaction block created via pipelining.
        backend_access_transam_xact::BeginImplicitTransactionBlock();
    }

    // Start statement timeout if necessary (does not reset an already-running
    // timeout).
    logging::enable_statement_timeout()?;

    // CLIENT_CONNECTION_CHECK_TIMEOUT arming requires IsUnderPostmaster &&
    // MyProcPort; not reachable here (single-user). Faithfully skipped.

    Ok(())
}

/// `finish_xact_command(void)` (postgres.c:2825) — cancel the statement timeout
/// and commit the transaction command if one is started.
///
/// The `MEMORY_CONTEXT_CHECKING` / `SHOW_MEMORY_STATS` blocks are debug-build
/// diagnostics, not threaded here.
pub fn finish_xact_command() -> PgResult<()> {
    // Cancel active statement timeout after each command.
    logging::disable_statement_timeout();

    if globals::xact_started() {
        xact_seams::commit_transaction_command::call()?;
        globals::set_xact_started(false);
    }

    Ok(())
}

// ===========================================================================
// IsTransactionExitStmt — postgres.c:2857
// ===========================================================================

/// `IsTransactionExitStmt(parsetree)` (postgres.c:2857) — is this a
/// COMMIT/PREPARE/ROLLBACK/ROLLBACK-TO statement (the ones allowed in
/// transaction-aborted state)?
pub fn is_transaction_exit_stmt(parsetree: &Node<'_>) -> bool {
    use types_nodes::ddlnodes::TransactionStmtKind;
    if let Some(stmt) = parsetree.as_transactionstmt() {
        matches!(
            stmt.kind,
            TransactionStmtKind::TRANS_STMT_COMMIT
                | TransactionStmtKind::TRANS_STMT_PREPARE
                | TransactionStmtKind::TRANS_STMT_ROLLBACK
                | TransactionStmtKind::TRANS_STMT_ROLLBACK_TO
        )
    } else {
        false
    }
}

// ===========================================================================
// check_log_statement — postgres.c:2384
// ===========================================================================

/// `check_log_statement(stmt_list)` (postgres.c:2384) — determine whether the
/// statement(s) should be logged per the `log_statement` GUC. The list is the
/// raw parsetrees (`RawStmt`); `GetCommandLogLevel` inspects each `stmt`.
pub fn check_log_statement(stmt_list: &PgVec<'_, RawStmt<'_>>) -> PgResult<bool> {
    use backend_utils_misc_guc_tables::consts::{LOGSTMT_ALL, LOGSTMT_NONE};
    let log_statement = log_statement_guc();

    if log_statement == LOGSTMT_NONE {
        return Ok(false);
    }
    if log_statement == LOGSTMT_ALL {
        return Ok(true);
    }

    // Else inspect the statement(s) to see whether to log.
    for raw in stmt_list.iter() {
        if utility_seams::get_command_log_level::call(&raw.stmt)? <= log_statement {
            return Ok(true);
        }
    }

    Ok(false)
}

/// `check_log_statement(stmt_list)` (postgres.c:2384) over a list of *planned*
/// statements (the extended-query Execute path, where `portal->stmts` is a
/// `PlannedStmt` list). Mirrors `GetCommandLogLevel`'s `T_PlannedStmt` case:
/// a `CMD_SELECT` plan is `LOGSTMT_ALL`, a modifying plan is `LOGSTMT_MOD`, a
/// utility plan defers to `GetCommandLogLevel(stmt->utilityStmt)`.
pub fn check_log_statement_planned(
    stmt_list: &[types_nodes::nodeindexscan::PlannedStmt<'_>],
) -> PgResult<bool> {
    use backend_utils_misc_guc_tables::consts::{LOGSTMT_ALL, LOGSTMT_MOD, LOGSTMT_NONE};
    let log_statement = log_statement_guc();

    if log_statement == LOGSTMT_NONE {
        return Ok(false);
    }
    if log_statement == LOGSTMT_ALL {
        return Ok(true);
    }

    for stmt in stmt_list.iter() {
        let lev = match stmt.commandType {
            CmdType::CMD_SELECT => LOGSTMT_ALL,
            CmdType::CMD_UPDATE
            | CmdType::CMD_INSERT
            | CmdType::CMD_DELETE
            | CmdType::CMD_MERGE => LOGSTMT_MOD,
            CmdType::CMD_UTILITY => match stmt.utilityStmt.as_deref() {
                Some(u) => utility_seams::get_command_log_level::call(u)?,
                None => LOGSTMT_ALL,
            },
            _ => LOGSTMT_ALL,
        };
        if lev <= log_statement {
            return Ok(true);
        }
    }

    Ok(false)
}

// ===========================================================================
// drop_unnamed_stmt — postgres.c:2904
// ===========================================================================

/// `drop_unnamed_stmt(void)` (postgres.c:2904) — release any existing unnamed
/// prepared statement (`DropCachedPlan(unnamed_stmt_psrc)`).
///
/// The `unnamed_stmt_psrc` backend global is owned by the extended-protocol
/// (F2) family, which is not ported; on the simple-Query path it is always
/// `NULL`, so this is a no-op. Mirror PG: nothing to drop.
pub fn drop_unnamed_stmt() -> PgResult<()> {
    // paranoia to avoid a dangling pointer in case of error
    let psrc = globals::unnamed_stmt_psrc();
    if psrc != types_nodes::parsestmt::CachedPlanSourceHandle::NULL {
        // Clear the global FIRST (C: unnamed_stmt_psrc = NULL; before the drop).
        globals::set_unnamed_stmt_psrc(types_nodes::parsestmt::CachedPlanSourceHandle::NULL);
        backend_utils_cache_plancache_seams::drop_cached_plan::call(psrc)?;
    }
    Ok(())
}

// ===========================================================================
// GUC reads (postgres.c-owned logging GUCs)
// ===========================================================================

fn log_parser_stats() -> bool {
    backend_utils_misc_guc_tables::vars::log_parser_stats.read()
}

fn log_statement_stats() -> bool {
    backend_utils_misc_guc_tables::vars::log_statement_stats.read()
}

fn log_statement_guc() -> i32 {
    backend_utils_misc_guc_tables::vars::log_statement.read()
}

// ===========================================================================
// CommandTag model bridge
// ===========================================================================

/// Bridge `types_core::cmdtag::CommandTag` (the newtype `CreateCommandTag`
/// returns) to `types_portal::CommandTag` (the bare `i32` the portal /
/// QueryCompletion / dest seams carry). They are two views of the same C
/// `CommandTag` enumerator.
fn portal_tag(tag: CommandTag) -> types_portal::CommandTag {
    tag.0
}
