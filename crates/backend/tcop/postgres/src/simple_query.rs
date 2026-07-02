// exec_simple_query + the pg_parse/analyze/rewrite/plan wrapper family
// (postgres.c). Parser/rewriter/planner are seams (lanes in flight).
use ::elog::ereport;
use ::mcx::{Mcx, PgVec};
use ::types_core::CommandTag;
use ::types_dest::CommandDest;
use ::types_error::{PgResult, ERRCODE_IN_FAILED_SQL_TRANSACTION, ERROR, LOG};
use ::types_nodes::node_tree::Node;
use ::types_nodes::nodes_enums::CmdType;
use ::types_nodes::parsenodes::Query;
use ::types_nodes::plannodes::PlannedStmt;
use ::types_nodes::rawnodes::RawStmt;
use ::types_nodes::NodeTag;
use ::types_portal::{
    CachedPlanHandle, ParamListHandle, QueryCompletion, QueryEnvHandle, CURSOR_OPT_PARALLEL_OK,
    FETCH_ALL,
};

use crate::{check_for_interrupts, loc, set_xact_started, xact_started, ResetUsage, ShowUsage};

fn log_parser_stats() -> bool {
    guc_tables::backing::log_parser_stats()
}
fn log_planner_stats() -> bool {
    guc_tables::backing::log_planner_stats()
}
fn log_statement_stats() -> bool {
    guc_tables::backing::log_statement_stats()
}

pub fn pg_parse_query<'mcx>(
    mcx: Mcx<'mcx>,
    query_string: &str,
) -> PgResult<PgVec<'mcx, RawStmt<'mcx>>> {
    if log_parser_stats() {
        ResetUsage();
    }

    let raw_parsetree_list =
        parser_seams::raw_parser::call(mcx, query_string, parser_seams::RawParseMode::RAW_PARSE_DEFAULT)?;

    if log_parser_stats() {
        ShowUsage("PARSER STATISTICS")?;
    }

    Ok(raw_parsetree_list)
}

pub fn pg_analyze_and_rewrite_fixedparams<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: &'a RawStmt<'mcx>,
    query_string: &'a str,
    param_types: &'a [types_core::Oid],
    query_env: QueryEnvHandle,
) -> PgResult<PgVec<'mcx, Query<'mcx>>> {
    if log_parser_stats() {
        ResetUsage();
    }

    let query = analyze_seams::parse_analyze_fixedparams::call(
        mcx,
        parsetree,
        query_string,
        param_types,
        query_env,
    )?;

    if log_parser_stats() {
        ShowUsage("PARSE ANALYSIS STATISTICS")?;
    }

    pg_rewrite_query(mcx, query)
}

pub fn pg_rewrite_query<'mcx>(mcx: Mcx<'mcx>, query: Query<'mcx>) -> PgResult<PgVec<'mcx, Query<'mcx>>> {
    if log_parser_stats() {
        ResetUsage();
    }

    let querytree_list = if query.commandType == CmdType::CMD_UTILITY {
        let mut v = PgVec::new_in(mcx);
        v.try_reserve_exact(1).map_err(|_| mcx.oom(1))?;
        v.push(query);
        v
    } else {
        rewrite_handler_seams::query_rewrite::call(mcx, query)?
    };

    if log_parser_stats() {
        ShowUsage("REWRITER STATISTICS")?;
    }

    Ok(querytree_list)
}

pub fn pg_plan_query<'mcx>(
    mcx: Mcx<'mcx>,
    querytree: Query<'mcx>,
    query_string: &str,
    cursor_options: i32,
    bound_params: ParamListHandle,
) -> PgResult<Option<PlannedStmt<'mcx>>> {
    if querytree.commandType == CmdType::CMD_UTILITY {
        return Ok(None);
    }

    debug_assert!(snapmgr::ActiveSnapshotSet());

    if log_planner_stats() {
        ResetUsage();
    }

    let plan =
        planner_seams::planner::call(mcx, querytree, query_string, cursor_options, bound_params)?;

    if log_planner_stats() {
        ShowUsage("PLANNER STATISTICS")?;
    }

    Ok(Some(plan))
}

pub fn pg_plan_queries<'mcx>(
    mcx: Mcx<'mcx>,
    querytrees: PgVec<'mcx, Query<'mcx>>,
    query_string: &str,
    cursor_options: i32,
    bound_params: ParamListHandle,
) -> PgResult<PgVec<'mcx, PlannedStmt<'mcx>>> {
    let mut stmt_list: PgVec<'mcx, PlannedStmt<'mcx>> = PgVec::new_in(mcx);
    stmt_list
        .try_reserve_exact(querytrees.len())
        .map_err(|_| mcx.oom(querytrees.len()))?;

    for query in querytrees {
        if query.commandType == CmdType::CMD_UTILITY {
            stmt_list.push(PlannedStmt {
                commandType: CmdType::CMD_UTILITY,
                canSetTag: query.canSetTag,
                utilityStmt: query.utilityStmt,
                stmt_location: query.stmt_location,
                stmt_len: query.stmt_len,
                queryId: query.queryId,
                ..PlannedStmt::default()
            });
        } else {
            let stmt =
                pg_plan_query(mcx, query, query_string, cursor_options, bound_params)?
                    .expect("pg_plan_query returned None for a non-utility query");
            stmt_list.push(stmt);
        }
    }

    Ok(stmt_list)
}

pub fn exec_simple_query<'mcx>(mcx: Mcx<'mcx>, query_string: &'mcx str) -> PgResult<()> {
    let dest = elog::config::where_to_send_output();
    let save_log_statement_stats = log_statement_stats();
    let mut was_logged = false;

    backend_status_seams::pgstat_report_activity::call(
        backend_status_seams::BackendState::STATE_RUNNING,
        Some(query_string),
    );

    if save_log_statement_stats {
        ResetUsage();
    }

    start_xact_command()?;

    drop_unnamed_stmt();


    let parsetree_list = pg_parse_query(mcx, query_string)?;

    if check_log_statement(&parsetree_list) {
        ereport(LOG)
            .errmsg(format!("statement: {query_string}"))
            .errhidestmt(true)
            .finish(loc(1069, "exec_simple_query"))?;
        was_logged = true;
    }

    let use_implicit_block = parsetree_list.len() > 1;

    let n = parsetree_list.len();
    for (idx, parsetree) in parsetree_list.iter().enumerate() {
        let is_last = idx + 1 == n;
        let mut snapshot_set = false;

        backend_status_seams::pgstat_report_query_id::call(0, true);
        backend_status_seams::pgstat_report_plan_id::call(0, true);

        let stmt = parsetree.stmt.expect("RawStmt has a stmt");

        let command_tag: CommandTag = utility_seams::create_command_tag::call(stmt);
        let (cmdtagname, _cmdtaglen) = cmdtag::GetCommandTagNameAndLen(command_tag);

        ps_status_seams::set_ps_display::call(cmdtagname);

        tcop_dest::BeginCommand(command_tag, dest);

        if xact::IsAbortedTransactionBlockState() && !IsTransactionExitStmt(Some(stmt)) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION)
                .errmsg("current transaction is aborted, commands ignored until end of transaction block")
                .into_error()
                .into());
        }

        start_xact_command()?;

        if use_implicit_block {
            xact::BeginImplicitTransactionBlock();
        }

        check_for_interrupts()?;

        if analyze_seams::analyze_requires_snapshot::call(parsetree) {
            let snap = snapmgr::GetTransactionSnapshot()?;
            snapmgr::PushActiveSnapshot(&snap)?;
            snapshot_set = true;
        }

        // C uses a per-parsetree child context for all but the last parsetree
        // so multi-statement strings free as they go; collapsed onto the
        // MessageContext arena (its reset reclaims everything per message).
        let querytree_list = pg_analyze_and_rewrite_fixedparams(
            mcx,
            parsetree,
            query_string,
            &[],
            QueryEnvHandle::NULL,
        )?;

        let plantree_list = pg_plan_queries(
            mcx,
            querytree_list,
            query_string,
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )?;

        if snapshot_set {
            snapmgr::PopActiveSnapshot()?;
        }

        check_for_interrupts()?;

        let portal = portalmem::CreatePortal("", true, true)?;
        portal.borrow_mut().visible = false;

        // SAFETY: `plantree_list` is arena-backed by `mcx` and neither moves
        // nor drops before `stmt_list::free(stmts)` / the next reset_all().
        let stmts = unsafe { pquery::stmt_list::register(&plantree_list) };
        portalmem::PortalDefineQuery(
            &portal,
            None,
            query_string,
            command_tag,
            stmts,
            CachedPlanHandle::NULL,
        )?;

        pquery::PortalStart(&portal, ParamListHandle::NULL, 0, None)?;

        /*
         * Output format: text unless FETCH from a binary cursor. The
         * FetchStmt payload lands with the grammar; reaching one here is
         * impossible until then.
         */
        let format: i16 = if stmt.node_tag() == NodeTag::T_FetchStmt {
            panic!(
                "exec_simple_query (postgres.c:1243): FETCH binary-cursor format \
                 probe needs the FetchStmt node (parser/portalcmds lanes)"
            );
        } else {
            0 /* TEXT is default */
        };
        pquery::PortalSetResultFormat(&portal, &[format])?;

        let mut receiver = tcop_dest::CreateDestReceiver(dest);
        if dest == CommandDest::Remote {
            tcop_dest::SetRemoteDestReceiverParams(&mut receiver, portal.clone());
        }

        let mut qc = QueryCompletion::default();
        let _ = pquery::PortalRun(
            &portal,
            FETCH_ALL,
            true, /* always top level */
            &mut receiver,
            None, /* altdest aliases dest, as in C */
            Some(&mut qc),
        )?;

        receiver.destroy();

        portalmem::PortalDrop(&portal, false)?;
        pquery::stmt_list::free(stmts);

        if is_last {
            if use_implicit_block {
                xact::EndImplicitTransactionBlock();
            }
            finish_xact_command()?;
        } else if stmt.node_tag() == NodeTag::T_TransactionStmt {
            finish_xact_command()?;
        } else {
            debug_assert!(
                (xact::MyXactFlags() & types_core::xact::XACT_FLAGS_NEEDIMMEDIATECOMMIT) == 0
            );

            xact::CommandCounterIncrement()?;

            disable_statement_timeout()?;
        }

        tcop_dest::EndCommand(&qc, dest, false)?;

        /* (per_parsetree_context delete: collapsed onto the arena.) */
    }

    finish_xact_command()?;

    if parsetree_list.is_empty() {
        tcop_dest::NullCommand(dest)?;
    }

    match check_log_duration(was_logged) {
        (1, msec_str) => {
            ereport(LOG)
                .errmsg(format!("duration: {msec_str} ms"))
                .errhidestmt(true)
                .finish(loc(1362, "exec_simple_query"))?;
        }
        (2, msec_str) => {
            ereport(LOG)
                .errmsg(format!("duration: {msec_str} ms  statement: {query_string}"))
                .errhidestmt(true)
                .finish(loc(1367, "exec_simple_query"))?;
        }
        _ => {}
    }

    if save_log_statement_stats {
        ShowUsage("QUERY STATISTICS")?;
    }


    Ok(())
}

pub fn start_xact_command() -> PgResult<()> {
    if !xact_started() {
        xact::StartTransactionCommand()?;
        set_xact_started(true);
    } else if (xact::MyXactFlags() & types_core::xact::XACT_FLAGS_PIPELINING) != 0 {
        xact::BeginImplicitTransactionBlock();
    }

    enable_statement_timeout()?;

    // CLIENT_CONNECTION_CHECK_TIMEOUT arming reads the
    // client_connection_check_interval GUC (boot default 0: disabled; the GUC
    // backing var lands with the guc lane).

    Ok(())
}

pub fn finish_xact_command() -> PgResult<()> {
    disable_statement_timeout()?;

    if xact_started() {
        xact::CommitTransactionCommand()?;
        set_xact_started(false);
    }

    Ok(())
}

pub(crate) fn IsTransactionExitStmt(parsetree: Option<Node<'_>>) -> bool {
    match parsetree {
        Some(node) if node.node_tag() == NodeTag::T_TransactionStmt => {
            panic!(
                "IsTransactionExitStmt (postgres.c:2857): TransactionStmt payload \
                 (kind) not yet in types_nodes (grammar lane)"
            )
        }
        _ => false,
    }
}


pub fn drop_unnamed_stmt() {}

fn check_log_statement(stmt_list: &PgVec<'_, RawStmt<'_>>) -> bool {
    use guc_tables::consts::{LOGSTMT_ALL, LOGSTMT_NONE};
    let log_statement = guc_tables::backing::log_statement();

    if log_statement == LOGSTMT_NONE {
        return false;
    }
    if log_statement == LOGSTMT_ALL {
        return true;
    }

    for raw in stmt_list.iter() {
        let Some(stmt) = raw.stmt else { continue };
        if utility_seams::get_command_log_level::call(stmt) <= log_statement {
            return true;
        }
    }

    false
}

// check_log_duration (postgres.c:2427). The log_min_duration_sample /
// log_statement_sample_rate refinement rides in_sample state the guc lane
// owns; with those GUCs at boot defaults (-1 / 1.0) this matches C.
fn check_log_duration(was_logged: bool) -> (i32, String) {
    let log_duration = guc_tables::backing::log_duration();
    let log_min = guc_tables::backing::log_min_duration_statement();
    if !log_duration && log_min < 0 {
        return (0, String::new());
    }

    let start = xact::GetCurrentStatementStartTimestamp();
    let now = crate::get_current_timestamp();
    let diff_us = now - start;
    let secs = diff_us / 1_000_000;
    let usecs = (diff_us % 1_000_000) as i64;
    let msecs = usecs / 1000;

    let exceeded_duration = log_min == 0
        || (log_min > 0 && (secs > i64::from(log_min) / 1000
            || secs * 1000 + msecs >= i64::from(log_min)));

    if exceeded_duration || log_duration {
        let msec_str = format!("{}.{:03}", secs * 1000 + msecs, usecs % 1000);
        if exceeded_duration && !was_logged {
            return (2, msec_str);
        }
        return (1, msec_str);
    }

    (0, String::new())
}

// enable_statement_timeout (postgres.c:5203). The StatementTimeout /
// TransactionTimeout GUC backing vars land with the guc lane; boot default 0
// takes the disable leg, as a fresh C backend does.
fn enable_statement_timeout() -> PgResult<()> {
    debug_assert!(xact_started());
    let statement_timeout = 0i32;
    if statement_timeout > 0 {
        if !timeout_seams::get_timeout_active::call(timeout_seams::STATEMENT_TIMEOUT) {
            timeout_seams::enable_timeout_after::call(
                timeout_seams::STATEMENT_TIMEOUT,
                statement_timeout,
            )?;
        }
    } else if timeout_seams::get_timeout_active::call(timeout_seams::STATEMENT_TIMEOUT) {
        timeout_seams::disable_timeout::call(timeout_seams::STATEMENT_TIMEOUT, false)?;
    }
    Ok(())
}

fn disable_statement_timeout() -> PgResult<()> {
    if timeout_seams::get_timeout_active::call(timeout_seams::STATEMENT_TIMEOUT) {
        timeout_seams::disable_timeout::call(timeout_seams::STATEMENT_TIMEOUT, false)?;
    }
    Ok(())
}
