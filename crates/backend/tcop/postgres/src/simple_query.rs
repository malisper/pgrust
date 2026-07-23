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
    querytree: &'mcx mut Query<'mcx>,
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

    // The analyzed Queries already live in arena memory; plan each in place
    // (C mutates the Query and shares the pointer) instead of moving it
    // through the call chain and copying it back into the arena at seal.
    let querytrees: &'mcx mut [Query<'mcx>] = querytrees.leak();
    for query in querytrees {
        if query.commandType == CmdType::CMD_UTILITY {
            stmt_list.push(PlannedStmt {
                commandType: CmdType::CMD_UTILITY,
                canSetTag: query.canSetTag,
                utilityStmt: query.utilityStmt,
                stmt_location: query.stmt_location,
                stmt_len: query.stmt_len,
                queryId: types_nodes::SyncCell::new(query.queryId),
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

// Crash-test injection worker (env-gated caller). Signals a TARGET vpid to
// die abruptly: `quit` = SIGQUIT quickdie (WARNING + 57P02 to peers, no proc
// cleanup), `kill` = SIGKILL-equivalent (no message, no cleanup). Superuser-
// gated, mirroring pg_signal_backend's contract. The delivery is async — the
// target dies at its next interrupt point and the postmaster runs the crash-
// restart cycle; this session returns a clean empty-query response first.
fn crash_backend_injection(args: &str) -> PgResult<()> {
    if !superuser_seams::superuser::call()? {
        return Err(ereport(ERROR)
            .errmsg("crash-backend injection requires superuser")
            .into_error()
            .into());
    }

    let mut parts = args.split_whitespace();
    let pid: i32 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| {
            ereport(ERROR)
                .errmsg("crash backend: expected 'pgrust: crash backend <vpid> quit|kill'")
                .into_error()
        })?;
    let mode = parts.next().unwrap_or("quit");

    let rc = match mode {
        "quit" => procsignal::SendThreadSignal(pid, crate::SIGQUIT),
        "kill" => procsignal::SendThreadKill(pid),
        other => {
            return Err(ereport(ERROR)
                .errmsg(format!("crash backend: unknown mode {other:?} (want quit|kill)"))
                .into_error()
                .into());
        }
    };
    if rc != 0 {
        return Err(ereport(ERROR)
            .errmsg(format!("PID {pid} is not a PostgreSQL backend process"))
            .into_error()
            .into());
    }

    tcop_dest::NullCommand(elog::config::where_to_send_output())
}

// GL-MEMWATCH-1: the deliberate context hog behind the developer GUC
// pgrust.memory_watchdog_test_hog. Allocates (and touches — RSS must grow)
// `mb` one-MB slabs into a session-lifetime "WatchdogTestHog" context and
// leaks them there: ACCOUNTED growth, the incident shape. Freed only at
// session teardown (the session_root Roots phase). Never reachable at the
// default 0.
#[cold]
#[inline(never)]
fn watchdog_test_hog(mb: usize) {
    use std::cell::Cell;
    thread_local! {
        static HOG_CTX: Cell<Option<&'static ::mcx::MemoryContext>> = const { Cell::new(None) };
    }
    let ctx = HOG_CTX.with(|c| match c.get() {
        Some(ctx) => ctx,
        None => {
            let ctx: &'static ::mcx::MemoryContext = ::mcx::session_root("WatchdogTestHog");
            c.set(Some(ctx));
            ctx
        }
    });
    for _ in 0..mb {
        let v: PgVec<'_, u8> = ::mcx::vec_from_elem_in(ctx.mcx(), 0xA5u8, 1 << 20);
        let _ = v.leak();
    }
}

pub fn exec_simple_query<'mcx>(mcx: Mcx<'mcx>, query_string: &'mcx str) -> PgResult<()> {
    // Crash-restart test injection: PANIC-class fault on demand, env-gated so
    // the surface is inert in production (notes/crash-restart-design.md).
    if query_string == "pgrust: inject panic"
        && std::env::var_os("PGRUST_CRASH_TEST").is_some()
    {
        ereport(types_error::PANIC)
            .errmsg("crash-restart test injection")
            .finish(loc(0, "exec_simple_query"))?;
    }

    // Crash-test injection: make a TARGET backend die as if externally
    // SIGQUIT/SIGKILL'd. TAP crash tests kill by OS pid; thread-model vpids
    // aren't OS-signalable, so the recovery rig routes pg_ctl kill here
    // (notes/crash-tap-kill-design.md). Env-gated: inert in production.
    if let Some(args) = query_string.strip_prefix("pgrust: crash backend ") {
        if std::env::var_os("PGRUST_CRASH_TEST").is_some() {
            return crash_backend_injection(args);
        }
    }

    // GL-MEMWATCH-1 test instrumentation (developer GUC, default 0 = one
    // TLS load + branch): each simple query leaks N MB into a named
    // session-lifetime context — the memory watchdog e2e's deliberate hog.
    if guc_tables::backing::pgrust_memory_watchdog_test_hog() > 0 {
        watchdog_test_hog(guc_tables::backing::pgrust_memory_watchdog_test_hog() as usize);
    }

    // C: `debug_query_string = query_string;` (exec_simple_query top); the
    // scope's drop is the tail `debug_query_string = NULL` plus the
    // sigsetjmp reset on the error path. current_query() reads it.
    let _debug_query = elog::debug_query_string_scope(query_string);

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
    crate::stmt_trace::probe("q.xact");

    drop_unnamed_stmt();


    let parsetree_list = pg_parse_query(mcx, query_string)?;
    crate::stmt_trace::probe("q.parse");

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
        crate::stmt_trace::probe("q.rewrite");

        let plantree_list = pg_plan_queries(
            mcx,
            querytree_list,
            query_string,
            CURSOR_OPT_PARALLEL_OK,
            ParamListHandle::NULL,
        )?;
        crate::stmt_trace::probe("q.plan");

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
        crate::stmt_trace::probe("q.portalstart");

        /* Output format: text unless FETCH from a binary cursor. */
        let format: i16 = match stmt.node_tag() {
            NodeTag::T_FetchStmt => {
                let fstmt = stmt.as_fetch_stmt().expect("T_FetchStmt");
                let binary = !fstmt.ismove
                    && portalmem::GetPortalByName(fstmt.portalname).is_some_and(|fportal| {
                        fportal.borrow().cursorOptions & types_portal::CURSOR_OPT_BINARY != 0
                    });
                i16::from(binary)
            }
            _ => 0, /* TEXT is default */
        };
        pquery::PortalSetResultFormat(&portal, &[format])?;

        let mut receiver = tcop_dest::CreateDestReceiver(dest);
        if dest == CommandDest::Remote {
            tcop_dest::SetRemoteDestReceiverParams(&mut receiver, portal.clone());
        }

        // GL-STMTTASK-1 (kill knob PGRUST_STMT_TASK, default OFF): arm the
        // statement-as-task executor hook for exactly THIS statement's
        // top-level portal run — the protocol-level half of the admission
        // envelope (simple protocol, single statement, wire dest, a raw
        // SELECT, normal non-subtransaction session state). The executor
        // hook owns the plan-shape gates and consumes the arm; the guard
        // disarms on every exit path so the arm can never leak past the
        // statement. Knob-OFF cost: one memoized bool read per statement.
        let _stmt_task_arm = postgres_seams::stmt_task_arm::arm_statement(
            n == 1
                && dest == CommandDest::Remote
                && stmt.node_tag() == NodeTag::T_SelectStmt
                && !xact::IsSubTransaction(),
        );

        let mut qc = QueryCompletion::default();
        let _ = pquery::PortalRun(
            &portal,
            FETCH_ALL,
            true, /* always top level */
            &mut receiver,
            None, /* altdest aliases dest, as in C */
            Some(&mut qc),
        )?;

        crate::stmt_trace::probe("q.run");
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
    crate::stmt_trace::probe("q.commit");

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

    // C postgres.c: start the dead-client check when configured. One-shot;
    // ProcessInterrupts re-arms it after every live check, so a parked
    // parallel leader keeps polling for the disconnect that would otherwise
    // have no cancel vector at all (GL-DISCONNECT-WEDGE-1).
    let interval = crate::client_connection_check_interval_ms();
    if interval > 0
        && init_small::globals::IsUnderPostmaster()
        && !timeout_seams::get_timeout_active::call(timeout_seams::CLIENT_CONNECTION_CHECK_TIMEOUT)
    {
        timeout_seams::enable_timeout_after::call(
            timeout_seams::CLIENT_CONNECTION_CHECK_TIMEOUT,
            interval,
        )?;
    }

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
    use types_nodes::parsenodes::TransactionStmtKind::*;
    match parsetree.and_then(|node| node.as_transaction_stmt()) {
        Some(stmt) => matches!(
            stmt.kind,
            TRANS_STMT_COMMIT | TRANS_STMT_PREPARE | TRANS_STMT_ROLLBACK | TRANS_STMT_ROLLBACK_TO
        ),
        None => false,
    }
}


use crate::extended_query::drop_unnamed_stmt;

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

// check_log_duration (postgres.c:2427), full C shape: log_min_duration_sample
// / log_statement_sample_rate decide an in_sample draw at the same decision
// point as C, and xact_is_sampled (drawn once per transaction from
// log_transaction_sample_rate at StartTransaction) forces logging for sampled
// transactions. (GL-GUCBATCH-1: the sampling refinement was a silent PARTIAL —
// SET accepted, never honored.)
pub(crate) fn check_log_duration(was_logged: bool) -> (i32, String) {
    let gucs = LogDurationGucs {
        log_duration: guc_tables::backing::log_duration(),
        log_min: guc_tables::backing::log_min_duration_statement(),
        log_min_sample: guc_tables::backing::log_min_duration_sample(),
        sample_rate: guc_tables::backing::log_statement_sample_rate(),
        xact_is_sampled: xact::xact_is_sampled(),
    };
    let diff_us =
        crate::get_current_timestamp() - xact::GetCurrentStatementStartTimestamp();
    check_log_duration_impl(was_logged, diff_us, &gucs, || {
        pg_prng::global_prng(pg_prng::PgPrng::next_f64)
    })
}

pub(crate) struct LogDurationGucs {
    pub(crate) log_duration: bool,
    pub(crate) log_min: i32,
    pub(crate) log_min_sample: i32,
    pub(crate) sample_rate: f64,
    pub(crate) xact_is_sampled: bool,
}

// The C decision body, GUC/clock/PRNG-free for the deterministic units.
pub(crate) fn check_log_duration_impl(
    was_logged: bool,
    diff_us: i64,
    g: &LogDurationGucs,
    draw: impl FnOnce() -> f64,
) -> (i32, String) {
    if !g.log_duration && g.log_min_sample < 0 && g.log_min < 0 && !g.xact_is_sampled {
        return (0, String::new());
    }

    let secs = diff_us / 1_000_000;
    let usecs = (diff_us % 1_000_000) as i64;
    let msecs = usecs / 1000;

    let exceeded_duration = g.log_min == 0
        || (g.log_min > 0 && (secs > i64::from(g.log_min) / 1000
            || secs * 1000 + msecs >= i64::from(g.log_min)));

    let exceeded_sample_duration = g.log_min_sample == 0
        || (g.log_min_sample > 0
            && (secs > i64::from(g.log_min_sample) / 1000
                || secs * 1000 + msecs >= i64::from(g.log_min_sample)));

    // C: no PRNG draw at rate 0 (never log) or rate 1 (always log).
    let mut in_sample = false;
    if exceeded_sample_duration {
        in_sample = g.sample_rate != 0.0 && (g.sample_rate == 1.0 || draw() <= g.sample_rate);
    }

    if exceeded_duration || in_sample || g.log_duration || g.xact_is_sampled {
        let msec_str = format!("{}.{:03}", secs * 1000 + msecs, usecs % 1000);
        if (exceeded_duration || in_sample || g.xact_is_sampled) && !was_logged {
            return (2, msec_str);
        }
        return (1, msec_str);
    }

    (0, String::new())
}

fn enable_statement_timeout() -> PgResult<()> {
    debug_assert!(xact_started());
    let statement_timeout = lmgr_proc::globals::StatementTimeout();
    let transaction_timeout = lmgr_proc::globals::TransactionTimeout();
    // An earlier-or-equal TRANSACTION_TIMEOUT subsumes the statement timer.
    if statement_timeout > 0
        && (statement_timeout < transaction_timeout || transaction_timeout == 0)
    {
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

pub(crate) fn disable_statement_timeout() -> PgResult<()> {
    if timeout_seams::get_timeout_active::call(timeout_seams::STATEMENT_TIMEOUT) {
        timeout_seams::disable_timeout::call(timeout_seams::STATEMENT_TIMEOUT, false)?;
    }
    Ok(())
}
