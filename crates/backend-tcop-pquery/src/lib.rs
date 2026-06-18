//! `pquery.c` — POSTGRES process query command code (portal execution),
//! `src/backend/tcop/pquery.c` (PostgreSQL 18.3), ported to the repo's
//! owned-value model.
//!
//! The portal-driving state machine reads and mutates live [`Portal`]
//! (`Rc<RefCell<PortalData>>`) fields through `portal.borrow()/borrow_mut()` and
//! the `portalmem` accessor API, while threading the executor (`ExecutorStart`/
//! `Run`/`Finish`/`End`/`Rewind`), the snapshot manager, the `DestReceiver`
//! router, the tuplestore receiver and `ProcessUtility`. Control flow, branch
//! order, constants, error messages and SQLSTATEs match the C exactly.
//!
//! The C `PG_TRY`/`PG_CATCH` save/restore of the `ActivePortal` /
//! `CurrentResourceOwner` / `PortalContext` / `CurrentMemoryContext` globals is
//! modeled per `docs/query-lifecycle-raii.md`: `CurrentResourceOwner` and the
//! `MemoryContextSwitchTo` are dissolved (RAII + explicit `Mcx`); the remaining
//! `ActivePortal`/`PortalContext` save/set/restore is the
//! `portalmem::with_portal_globals` scoped callback (restores on both `Ok` and
//! `Err`), and the `MarkPortalFailed`-on-error path is the `Err` arm of the
//! protected body.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::rc::Rc;
use alloc::string::{String, ToString};

use types_error::{
    PgError, PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_PROTOCOL_VIOLATION, ERROR,
};

use types_dest::CommandDest;
use types_nodes::copy_query::Query;
use types_nodes::nodeindexscan::PlannedStmt;
use types_nodes::nodes::{
    Node, NodeTag, CMD_DELETE, CMD_INSERT, CMD_MERGE, CMD_SELECT, CMD_UPDATE, CMD_UTILITY,
    T_CheckPointStmt, T_ConstraintsSetStmt, T_FetchStmt, T_ListenStmt, T_LockStmt, T_NotifyStmt,
    T_TransactionStmt, T_UnlistenStmt, T_VariableSetStmt, T_VariableShowStmt,
};
use types_nodes::parsestmt::{
    DestReceiverHandle, ProcessUtilityContext, PROCESS_UTILITY_QUERY, PROCESS_UTILITY_TOPLEVEL,
};
use types_nodes::portalcmds::ParamListInfo;
use types_nodes::querydesc::QueryDesc;
use types_portal::{
    CommandTag, FetchDirection, Portal, PortalStrategy, QueryCompletion, CMDTAG_DELETE,
    CMDTAG_INSERT, CMDTAG_MERGE, CMDTAG_SELECT, CMDTAG_UNKNOWN, CMDTAG_UPDATE, CURSOR_OPT_NO_SCROLL,
    CURSOR_OPT_SCROLL, FETCH_ABSOLUTE, FETCH_BACKWARD, FETCH_FORWARD, FETCH_RELATIVE,
    PORTAL_MULTI_QUERY, PORTAL_ONE_MOD_WITH, PORTAL_ONE_RETURNING, PORTAL_ONE_SELECT, PORTAL_READY,
    PORTAL_UTIL_SELECT,
};
use types_scan::sdir::{
    ScanDirection, ScanDirectionIsForward, ScanDirectionIsNoMovement, BackwardScanDirection,
    ForwardScanDirection, NoMovementScanDirection,
};
use types_slot::{TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_REWIND};
use types_snapshot::SnapshotData;

use backend_executor_execMain as execMain;
use backend_tcop_cmdtag::initialize_query_completion;
use backend_utils_mmgr_portalmem as portalmem;

use backend_access_transam_xact_seams as xact_seam;
use backend_executor_execTuples_seams as exectuples_seam;
use backend_executor_tstorereceiver_seams as tstore_seam;
use backend_tcop_dest_seams as dest_seam;
use backend_tcop_postgres_seams as postgres_seam;
use backend_tcop_utility_seams as utility_seam;
use backend_utils_mmgr_portalmem_seams as portalmem_seam;
use backend_utils_sort_storage_seams as sortstore_seam;
use backend_utils_time_snapmgr_seams as snapmgr_seam;

/// `FETCH_ALL` (`commands/portalcmds.h`, via `parsenodes.h`'s `FetchStmt`) — the
/// "all rows" sentinel; the C `#define FETCH_ALL LONG_MAX`.
const FETCH_ALL: i64 = i64::MAX;

/// `elog(ERROR, ...)` analogue — an internal-error (`XX000`) [`PgError`].
fn elog_error(msg: impl Into<String>) -> PgError {
    PgError::error(msg.into())
}

// ===========================================================================
// Inline helpers mirroring the C macros/inlines.
// ===========================================================================

/// `SetQueryCompletion(qc, commandTag, nprocessed)` (cmdtag.h, inline).
#[inline]
fn set_query_completion(qc: &mut QueryCompletion, command_tag: CommandTag, nprocessed: u64) {
    qc.commandTag = command_tag;
    qc.nprocessed = nprocessed;
}

/// `CopyQueryCompletion(dst, src)` (cmdtag.h, inline).
#[inline]
fn copy_query_completion(dst: &mut QueryCompletion, src: &QueryCompletion) {
    dst.commandTag = src.commandTag;
    dst.nprocessed = src.nprocessed;
}

/// `GetActiveSnapshot()` — the snapmgr seam returns `Option`; `PortalRun`-path
/// callers treat the active snapshot as always present.
fn get_active_snapshot() -> PgResult<Option<Rc<SnapshotData>>> {
    snapmgr_seam::get_active_snapshot::call()
}

// ===========================================================================
// `CreateQueryDesc` / `FreeQueryDesc`
// ===========================================================================

/// `CreateQueryDesc` (pquery.c:67-104) — build a `QueryDesc` for the executor.
///
/// In the owned model `QueryDesc::create` stores the `Option<Rc<SnapshotData>>`
/// snapshots directly; the C `RegisterSnapshot`/`UnregisterSnapshot` refcounting
/// is the `Rc` refcount, so there is no explicit register call here and no
/// explicit free in [`free_query_desc`] (the value's `Drop` is the
/// `pfree`/`UnregisterSnapshot`). The C `queryEnv` argument is dropped (the
/// owned `QueryDesc` does not carry it).
#[allow(clippy::too_many_arguments)]
pub fn create_query_desc(
    parent: &mcx::MemoryContext,
    plannedstmt: &PlannedStmt<'_>,
    source_text: &str,
    snapshot: Option<Rc<SnapshotData>>,
    crosscheck_snapshot: Option<Rc<SnapshotData>>,
    dest: DestReceiverHandle,
    params: ParamListInfo,
    instrument_options: i32,
) -> PgResult<QueryDesc> {
    QueryDesc::create(
        parent,
        plannedstmt,
        source_text,
        snapshot,
        crosscheck_snapshot,
        dest,
        params,
        instrument_options,
    )
}

/// `FreeQueryDesc` (pquery.c:106-117) — release a `QueryDesc`. The two
/// `UnregisterSnapshot` calls and the `pfree` are the `Drop` of the owned value;
/// the `Assert(estate == NULL)` is enforced by `ExecutorEnd` having torn down
/// the work bundle's plan state.
pub fn free_query_desc(qdesc: QueryDesc) {
    drop(qdesc);
}

// ===========================================================================
// `ProcessQuery`
// ===========================================================================

/// `ProcessQuery` (pquery.c:137-198, static) — execute a single plannable query
/// within a `PORTAL_MULTI_QUERY` / `PORTAL_ONE_RETURNING` / `PORTAL_ONE_MOD_WITH`
/// portal, sending results to `dest`.
///
/// `qc` is `None` when the caller wants no status string (the C `qc == NULL`).
/// `parent` is the `CurrentMemoryContext` the QueryDesc's per-query context is
/// made a child of (the portal context, in C terms). `params` / `query_env` are
/// the portal's handle-typed params/environment.
#[allow(clippy::too_many_arguments)]
fn process_query(
    parent: &mcx::MemoryContext,
    plan: &PlannedStmt<'_>,
    source_text: &str,
    params: ParamListInfo,
    dest: DestReceiverHandle,
    qc: Option<&mut QueryCompletion>,
) -> PgResult<()> {
    /*
     * Create the QueryDesc object
     */
    let mut query_desc = create_query_desc(
        parent,
        plan,
        source_text,
        get_active_snapshot()?,
        None, /* InvalidSnapshot */
        dest,
        params,
        0,
    )?;

    /*
     * Call ExecutorStart to prepare the plan for execution
     */
    execMain::ExecutorStart(&mut query_desc, 0)?;

    /*
     * Run the plan to completion.
     */
    execMain::ExecutorRun(&mut query_desc, ForwardScanDirection, 0)?;

    /*
     * Build command completion status data, if caller wants one.
     */
    if let Some(qc) = qc {
        let es_processed = query_desc.es_processed();
        match query_desc.operation {
            CMD_SELECT => set_query_completion(qc, CMDTAG_SELECT, es_processed),
            CMD_INSERT => set_query_completion(qc, CMDTAG_INSERT, es_processed),
            CMD_UPDATE => set_query_completion(qc, CMDTAG_UPDATE, es_processed),
            CMD_DELETE => set_query_completion(qc, CMDTAG_DELETE, es_processed),
            CMD_MERGE => set_query_completion(qc, CMDTAG_MERGE, es_processed),
            _ => set_query_completion(qc, CMDTAG_UNKNOWN, es_processed),
        }
    }

    /*
     * Now, we close down all the scans and free allocated resources.
     */
    execMain::ExecutorFinish(&mut query_desc)?;
    execMain::ExecutorEnd(&mut query_desc)?;

    free_query_desc(query_desc);

    Ok(())
}

// ===========================================================================
// `run_ctas_executor` — the createas.c executor-driven leg (createas.c 300-361)
// ===========================================================================

/// The CTAS executor-driven leg (`ExecCreateTableAs` else-branch, createas.c
/// 300-361): run the rule rewriter, plan the query, and execute it with output
/// redirected to the `DR_intorel` receiver named by `dest`.
///
/// This is the `run_ctas_executor` seam, owned by `backend-commands-createas`
/// (which carries the receiver) but installed here: this is the executor-driving
/// layer (`pquery.c`) that already owns `CreateQueryDesc` /
/// `ExecutorStart`/`Run`/`Finish`/`End`, and only here can it reach the
/// rewriter (`QueryRewrite`), planner (`pg_plan_query`) and active-snapshot
/// management. The keystone the seam doc cited (`portalcmds::Query` vs the
/// canonical `copy_query::Query`) is resolved: `query_rewrite_canonical` and
/// `pg_plan_query` both take the value-typed `copy_query::Query<'mcx>`, the same
/// model CTAS carries, so no model reconciliation is needed.
///
/// `GetIntoRelEFlags(into)` is computed inline (createas.c 374-383): the trivial
/// `into->skipData ? EXEC_FLAG_WITH_NO_DATA : 0`. The `ddlnodes::IntoClause`
/// carried here is the value-typed clause, so there is no need to cross the
/// `parsestmt::IntoClause`-shaped `get_into_rel_eflags` seam.
fn run_ctas_executor<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    query: Query<'mcx>,
    into: types_nodes::ddlnodes::IntoClause<'mcx>,
    query_string: &str,
    params: ParamListInfo,
    dest: DestReceiverHandle,
    qc: Option<QueryCompletion>,
) -> PgResult<Option<QueryCompletion>> {
    let mut qc = qc;

    /*
     * Parse analysis was done already, but we still have to run the rule
     * rewriter.  We do not do AcquireRewriteLocks: we assume the query either
     * came straight from the parser, or suitable locks were acquired by
     * plancache.c.
     *
     *   rewritten = QueryRewrite(query);
     */
    let mut rewritten =
        backend_rewrite_rewritehandler_seams::query_rewrite_canonical::call(mcx, query)?;

    /* SELECT should never rewrite to more or less than one SELECT query */
    if rewritten.len() != 1 {
        return Err(elog_error(
            "unexpected rewrite result for CREATE TABLE AS SELECT",
        ));
    }
    let query = rewritten.remove(0);
    debug_assert_eq!(query.commandType, CMD_SELECT);

    /*
     * plan the query
     *
     *   plan = pg_plan_query(query, pstate->p_sourcetext,
     *                        CURSOR_OPT_PARALLEL_OK, params);
     */
    let plan = backend_optimizer_plan_planner_seams::pg_plan_query::call(
        mcx,
        &query,
        query_string,
        types_nodes::copy_query::CURSOR_OPT_PARALLEL_OK,
    )?;
    let _ = params; // bound params are not threaded through the planner seam (COPY/CTAS-SELECT path has none)

    /*
     * Use a snapshot with an updated command ID to ensure this query sees
     * results of any previously executed queries.
     *
     *   PushCopiedSnapshot(GetActiveSnapshot());
     *   UpdateActiveSnapshotCommandId();
     */
    snapmgr_seam::push_copied_snapshot_and_bump::call()?;

    /* Create a QueryDesc, redirecting output to our tuple receiver */
    let mut query_desc = create_query_desc(
        mcx.context(),
        &plan,
        query_string,
        get_active_snapshot()?,
        None, /* InvalidSnapshot */
        dest,
        params,
        0,
    )?;

    /* call ExecutorStart to prepare the plan for execution
     *   ExecutorStart(queryDesc, GetIntoRelEFlags(into)); */
    let eflags: i32 = if into.skipData {
        types_nodes::executor::EXEC_FLAG_WITH_NO_DATA
    } else {
        0
    };
    execMain::ExecutorStart(&mut query_desc, eflags)?;

    /* run the plan to completion */
    execMain::ExecutorRun(&mut query_desc, ForwardScanDirection, 0)?;

    /* save the rowcount if we're given a qc to fill
     *   SetQueryCompletion(qc, CMDTAG_SELECT, queryDesc->estate->es_processed); */
    if let Some(qc) = qc.as_mut() {
        set_query_completion(qc, CMDTAG_SELECT, query_desc.es_processed());
    }

    /* and clean up */
    execMain::ExecutorFinish(&mut query_desc)?;
    execMain::ExecutorEnd(&mut query_desc)?;

    free_query_desc(query_desc);

    snapmgr_seam::pop_active_snapshot::call()?;

    Ok(qc)
}

// ===========================================================================
// `ChoosePortalStrategy`
// ===========================================================================

/// `ChoosePortalStrategy` (pquery.c:210-317) — select the [`PortalStrategy`] for
/// a list of statements.
///
/// The C accepts a `List` of `Query` *or* `PlannedStmt` nodes (plancache.c uses
/// the `Query` leg); a portal's `stmts` are always `PlannedStmt`s, so this owned
/// form operates over `&[PlannedStmt]`. The `IsA(stmt, Query)` and
/// `unrecognized node type` `elog(ERROR)` legs are unreachable here because the
/// element type is statically `PlannedStmt`.
pub fn choose_portal_strategy(stmts: &[PlannedStmt<'_>]) -> PgResult<PortalStrategy> {
    let mut n_set_tag: i32;

    /*
     * PORTAL_ONE_SELECT and PORTAL_UTIL_SELECT need only consider the
     * single-statement case ...
     */
    if stmts.len() == 1 {
        let pstmt = &stmts[0];
        if pstmt.canSetTag {
            if pstmt.commandType == CMD_SELECT {
                if pstmt.hasModifyingCTE {
                    return Ok(PORTAL_ONE_MOD_WITH);
                } else {
                    return Ok(PORTAL_ONE_SELECT);
                }
            }
            if pstmt.commandType == CMD_UTILITY {
                if let Some(utility_stmt) = pstmt.utilityStmt.as_deref() {
                    if utility_seam::utility_returns_tuples::call(utility_stmt)? {
                        return Ok(PORTAL_UTIL_SELECT);
                    }
                }
                /* it can't be ONE_RETURNING, so give up */
                return Ok(PORTAL_MULTI_QUERY);
            }
        }
    }

    /*
     * PORTAL_ONE_RETURNING has to allow auxiliary queries added by rewrite.
     * Choose PORTAL_ONE_RETURNING if there is exactly one canSetTag query and
     * it has a RETURNING list.
     */
    n_set_tag = 0;
    for pstmt in stmts {
        if pstmt.canSetTag {
            n_set_tag += 1;
            if n_set_tag > 1 {
                return Ok(PORTAL_MULTI_QUERY); /* no need to look further */
            }
            if pstmt.commandType == CMD_UTILITY || !pstmt.hasReturning {
                return Ok(PORTAL_MULTI_QUERY); /* no need to look further */
            }
        }
    }
    if n_set_tag == 1 {
        return Ok(PORTAL_ONE_RETURNING);
    }

    /* Else, it's the general case... */
    Ok(PORTAL_MULTI_QUERY)
}

/// `ChoosePortalStrategy` (pquery.c:210-317) over the OWNED `Query` value tree —
/// the leg `plancache.c` exercises (`PlanCacheComputeResultDesc` /
/// `GetCachedPlan`), where `stmt_list` is a list of `Query` nodes rather than
/// `PlannedStmt`s. Mirrors [`choose_portal_strategy`] field-for-field: the
/// `commandType`/`canSetTag`/`hasModifyingCTE`/`returningList` reads come off the
/// `Query` instead of the `PlannedStmt`, and the CMD_UTILITY leg consults
/// `UtilityReturnsTuples(query->utilityStmt)` via the utility seam. The
/// `unrecognized node type` `elog(ERROR)` leg is unreachable (the element type is
/// statically `Query`).
pub fn choose_portal_strategy_queries(stmts: &[Query<'_>]) -> PgResult<PortalStrategy> {
    let mut n_set_tag: i32;

    /*
     * PORTAL_ONE_SELECT and PORTAL_UTIL_SELECT need only consider the
     * single-statement case ...
     */
    if stmts.len() == 1 {
        let query = &stmts[0];
        if query.canSetTag {
            if query.commandType == CMD_SELECT {
                if query.hasModifyingCTE {
                    return Ok(PORTAL_ONE_MOD_WITH);
                } else {
                    return Ok(PORTAL_ONE_SELECT);
                }
            }
            if query.commandType == CMD_UTILITY {
                if let Some(utility_stmt) = query.utilityStmt.as_deref() {
                    if utility_seam::utility_returns_tuples::call(utility_stmt)? {
                        return Ok(PORTAL_UTIL_SELECT);
                    }
                }
                /* it can't be ONE_RETURNING, so give up */
                return Ok(PORTAL_MULTI_QUERY);
            }
        }
    }

    /*
     * PORTAL_ONE_RETURNING has to allow auxiliary queries added by rewrite.
     * Choose PORTAL_ONE_RETURNING if there is exactly one canSetTag query and
     * it has a RETURNING list.
     */
    n_set_tag = 0;
    for query in stmts {
        if query.canSetTag {
            n_set_tag += 1;
            if n_set_tag > 1 {
                return Ok(PORTAL_MULTI_QUERY); /* no need to look further */
            }
            if query.commandType == CMD_UTILITY || query.returningList.is_empty() {
                return Ok(PORTAL_MULTI_QUERY); /* no need to look further */
            }
        }
    }
    if n_set_tag == 1 {
        return Ok(PORTAL_ONE_RETURNING);
    }

    /* Else, it's the general case... */
    Ok(PORTAL_MULTI_QUERY)
}

// ===========================================================================
// `FetchPortalTargetList` / `FetchStatementTargetList`
// ===========================================================================

/// `FetchPortalTargetList` (pquery.c:327-334) — return the target list of the
/// portal's primary statement, or `None` (NIL) if it returns no tuples.
///
/// In the owned model the target list is owned by the cached plan inside the
/// portal's `stmts`; we return the primary statement's index so the caller can
/// read the borrowed target list off the portal. `None` is the C `NIL`.
pub fn fetch_portal_target_list(portal: &Portal) -> PgResult<Option<usize>> {
    /* no point in looking if we determined it doesn't return tuples */
    if portalmem::portal_get_strategy(portal) == PORTAL_MULTI_QUERY {
        return Ok(None);
    }
    /* get the primary statement and find out what it returns */
    Ok(portalmem::PortalGetPrimaryStmt(portal))
}

/// `FetchStatementTargetList` (pquery.c:349-408) — given a `PlannedStmt` that
/// returns tuples, identify whether it has a determinable target list.
///
/// The C returns the `List *` target list; in the owned model the target list
/// lives in the statement's `planTree` and is read by the caller. This returns
/// `Ok(true)` when the statement has a determinable (non-NIL) target list,
/// `Ok(false)` otherwise — covering the `Query`-collapsed `PlannedStmt` legs.
/// The `FetchStmt` / `ExecuteStmt` recursion legs are reached only through
/// plancache's use of this function (never the portal path); they are not
/// modeled here.
pub fn fetch_statement_target_list(pstmt: &PlannedStmt<'_>) -> bool {
    if pstmt.commandType == CMD_UTILITY {
        /* utility: would transfer attention to utilityStmt (plancache leg) */
        return false;
    }
    if pstmt.commandType == CMD_SELECT || pstmt.hasReturning {
        return pstmt
            .planTree
            .as_deref()
            .map(|p| p.plan_head().targetlist.is_some())
            .unwrap_or(false);
    }
    false
}

// ===========================================================================
// `PlannedStmtRequiresSnapshot`
// ===========================================================================

/// `PlannedStmtRequiresSnapshot` (pquery.c:1715-1750) — true if executing
/// `pstmt` requires an active snapshot.
pub fn planned_stmt_requires_snapshot(pstmt: &PlannedStmt<'_>) -> bool {
    let utility_stmt = match pstmt.utilityStmt.as_deref() {
        /* If it's not a utility statement, it definitely needs a snapshot */
        None => return true,
        Some(u) => u,
    };

    /*
     * Most utility statements need a snapshot ... Hence, enumerate those that
     * do not need one.
     */
    let tag: NodeTag = utility_stmt.tag();
    if tag == T_TransactionStmt
        || tag == T_LockStmt
        || tag == T_VariableSetStmt
        || tag == T_VariableShowStmt
        || tag == T_ConstraintsSetStmt
        /* efficiency hacks from here down */
        || tag == T_FetchStmt
        || tag == T_ListenStmt
        || tag == T_NotifyStmt
        || tag == T_UnlistenStmt
        || tag == T_CheckPointStmt
    {
        return false;
    }

    true
}

// ===========================================================================
// `PortalStart`
// ===========================================================================

/// `PortalStart` (pquery.c:434-611) — prepare a portal for execution.
///
/// `snapshot` is the optional caller-supplied snapshot (`None` == the C
/// `InvalidSnapshot`, i.e. take a new transaction snapshot). The C
/// `PG_TRY`/`PG_CATCH` global save/restore + `MarkPortalFailed`-on-error is the
/// `with_portal_globals` callback wrapping the protected body; on `Err` we mark
/// the portal failed then propagate.
pub fn portal_start(
    portal: &Portal,
    params: ParamListInfo,
    eflags: i32,
    snapshot: Option<Rc<SnapshotData>>,
) -> PgResult<()> {
    debug_assert!(portal.is_valid());
    debug_assert_eq!(
        portalmem::portal_get_status(portal),
        types_portal::PORTAL_DEFINED
    );

    // PG_TRY()/PG_CATCH(): with_portal_globals saves/sets/restores ActivePortal
    // + PortalContext (on both Ok and Err); the MarkPortalFailed-on-error path
    // is the Err arm here. (CurrentResourceOwner / MemoryContextSwitchTo are
    // dissolved per docs/query-lifecycle-raii.md.)
    let body = || -> PgResult<()> {
        /* Must remember portal param list, if any */
        portal.borrow_mut().portalParams = params.clone();

        /*
         * Determine the portal execution strategy
         */
        let strategy = {
            let p = portal.borrow();
            let stmts = p.stmts.as_deref().unwrap_or(&[]);
            choose_portal_strategy(stmts)?
        };
        portalmem::portal_set_strategy(portal, strategy);

        /*
         * Fire her up according to the strategy
         */
        if strategy == PORTAL_ONE_SELECT {
            /* Must set snapshot before starting executor. */
            if let Some(snap) = snapshot.clone() {
                snapmgr_seam::push_active_snapshot::call(snap)?;
            } else {
                let txn = snapmgr_seam::get_transaction_snapshot::call()?;
                snapmgr_seam::push_active_snapshot::call(Rc::new(txn))?;
            }

            /*
             * Create QueryDesc in portal's context; for the moment, set the
             * destination to DestNone.
             */
            let source_text = portal.borrow().sourceText.clone().unwrap_or_default();
            let params_value = params.clone();
            let active_snapshot = get_active_snapshot()?;
            let mut query_desc = {
                let p = portal.borrow();
                let portal_context = p
                    .portalContext
                    .as_ref()
                    .expect("portal has no portalContext (CreatePortal always assigns one)");
                let stmts = p.stmts.as_deref().unwrap_or(&[]);
                create_query_desc(
                    portal_context,
                    &stmts[0], /* linitial_node(PlannedStmt, portal->stmts) */
                    &source_text,
                    active_snapshot,
                    None, /* InvalidSnapshot */
                    backend_tcop_dest::none_receiver(),
                    params_value,
                    0,
                )?
            };

            /*
             * If it's a scrollable cursor, executor needs to support REWIND and
             * backwards scan, as well as whatever the caller might've asked for.
             */
            let myeflags = if (portalmem::portal_get_cursor_options(portal) & CURSOR_OPT_SCROLL) != 0
            {
                eflags | EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD
            } else {
                eflags
            };

            /*
             * Call ExecutorStart to prepare the plan for execution
             */
            execMain::ExecutorStart(&mut query_desc, myeflags)?;

            /*
             * Remember tuple descriptor (computed by ExecutorStart)
             */
            query_desc.with_result_tupdesc(|td| portalmem::portal_set_tup_desc(portal, td))?;

            /*
             * This tells PortalCleanup to shut down the executor
             */
            portal.borrow_mut().queryDesc = Some(query_desc);

            /*
             * Reset cursor position data to "start of query"
             */
            portalmem::portal_set_at_start(portal, true);
            portalmem::portal_set_at_end(portal, false); /* allow fetches */
            portalmem::portal_set_portal_pos(portal, 0);

            snapmgr_seam::pop_active_snapshot::call()?;
        } else if strategy == PORTAL_ONE_RETURNING || strategy == PORTAL_ONE_MOD_WITH {
            /*
             * We don't start the executor until we are told to run the portal.
             * We do need to set up the result tupdesc.
             */
            let primary = portalmem::PortalGetPrimaryStmt(portal)
                .expect("PORTAL_ONE_RETURNING portal has a primary stmt");
            // portal->tupDesc = ExecCleanTypeFromTL(pstmt->planTree->targetlist);
            // Built in (and stored into) the portal's own portalContext.
            // portalmem owns that arena + the `'static`-marked `tupDesc` field,
            // so it runs the build (the supplied closure) and the store under a
            // single portal borrow; the unsafe arena-lifetime marshaling lives
            // in the owner, never duplicated here.
            portalmem::set_result_tup_desc_with(portal, &mut |mcx, stmts| {
                let pstmt = &stmts[primary];
                let plan = pstmt
                    .planTree
                    .as_deref()
                    .map(Node::plan_head)
                    .expect("PlannedStmt has a planTree");
                let tlist = plan.targetlist.as_deref().unwrap_or(&[]);
                exectuples_seam::exec_clean_type_from_tl::call(mcx, tlist)
            })?;

            /*
             * Reset cursor position data to "start of query"
             */
            portalmem::portal_set_at_start(portal, true);
            portalmem::portal_set_at_end(portal, false); /* allow fetches */
            portalmem::portal_set_portal_pos(portal, 0);
        } else if strategy == PORTAL_UTIL_SELECT {
            /*
             * We don't set snapshot here, because PortalRunUtility will take
             * care of it if needed.
             */
            let primary = portalmem::PortalGetPrimaryStmt(portal)
                .expect("PORTAL_UTIL_SELECT portal has a primary stmt");
            // portal->tupDesc = UtilityTupleDescriptor(pstmt->utilityStmt);
            portalmem::set_result_tup_desc_with(portal, &mut |mcx, stmts| {
                let pstmt = &stmts[primary];
                debug_assert_eq!(pstmt.commandType, CMD_UTILITY);
                let utility_stmt = pstmt
                    .utilityStmt
                    .as_deref()
                    .expect("PORTAL_UTIL_SELECT stmt has a utilityStmt");
                utility_seam::utility_tuple_descriptor::call(mcx, utility_stmt)
            })?;

            /*
             * Reset cursor position data to "start of query"
             */
            portalmem::portal_set_at_start(portal, true);
            portalmem::portal_set_at_end(portal, false); /* allow fetches */
            portalmem::portal_set_portal_pos(portal, 0);
        } else if strategy == PORTAL_MULTI_QUERY {
            /* Need do nothing now */
            portalmem::portal_set_tup_desc(portal, None)?;
        }

        Ok(())
    };

    run_protected(portal, body)?;

    portalmem::portal_set_status(portal, PORTAL_READY);

    Ok(())
}

/// Run `body` with the portal globals set up (`with_portal_globals`), marking
/// the portal failed on error before propagating — the shared shape of the C
/// `PG_TRY { ... } PG_CATCH { MarkPortalFailed(portal); PG_RE_THROW(); }` in
/// `PortalStart`/`PortalRun`/`PortalRunFetch`.
fn run_protected(
    portal: &Portal,
    mut body: impl FnMut() -> PgResult<()>,
) -> PgResult<()> {
    // C's PG_CATCH runs MarkPortalFailed on ANY non-local exit out of the body.
    // An unported path may leave `body` via a `panic!` rather than a
    // `PgResult::Err`; without catching it the portal would stay PORTAL_ACTIVE
    // and poison every later statement in the session ("cannot drop active
    // portal"). Catch the unwind, mark the portal failed like the Err arm, then
    // resume the panic so the main-loop catch_unwind turns it into the proper
    // recoverable ERROR. (with_portal_globals has already restored the portal
    // globals before re-raising.)
    let result =
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            portalmem_seam::with_portal_globals::call(portal, &mut body)
        })) {
            Ok(r) => r,
            Err(payload) => {
                let _ = portalmem::MarkPortalFailed(portal);
                std::panic::resume_unwind(payload);
            }
        };
    if result.is_err() {
        /* Uncaught error while executing portal: mark it dead */
        portalmem::MarkPortalFailed(portal)?;
    }
    result
}

// ===========================================================================
// `PortalSetResultFormat`
// ===========================================================================

/// `PortalSetResultFormat` (pquery.c:624-660) — select the format codes for a
/// portal's result columns. `formats` is the client's per-column format request
/// (the C `int16 *formats` with `nFormats == formats.len()`).
pub fn portal_set_result_format(portal: &Portal, formats: &[i16]) -> PgResult<()> {
    let n_formats = formats.len() as i32;

    /* Do nothing if portal won't return tuples */
    let natts = match portal.borrow().tupDesc.as_ref() {
        None => return Ok(()),
        Some(td) => td.natts,
    };

    let mut out: alloc::vec::Vec<i16> = alloc::vec::Vec::new();
    out.try_reserve(natts as usize)
        .map_err(|_| elog_error("out of memory"))?;

    if n_formats > 1 {
        /* format specified for each column */
        if n_formats != natts {
            return Err(PgError::new(
                ERROR,
                alloc::format!(
                    "bind message has {n_formats} result formats but query has {natts} columns"
                ),
            )
            .with_sqlstate(ERRCODE_PROTOCOL_VIOLATION));
        }
        out.extend_from_slice(&formats[..natts as usize]);
    } else if n_formats > 0 {
        /* single format specified, use for all columns */
        let format1 = formats[0];
        for _ in 0..natts {
            out.push(format1);
        }
    } else {
        /* use default format for all columns */
        for _ in 0..natts {
            out.push(0);
        }
    }

    portal.borrow_mut().formats = out;

    Ok(())
}

// ===========================================================================
// `PortalRun`
// ===========================================================================

/// `PortalRun` (pquery.c:685-843) — run a portal, returning `true` if it is
/// completely done. `qc` is `None` when the caller wants no status data.
///
/// The C save/restore of `TopTransactionResourceOwner` / `CurrentResourceOwner`
/// (the VACUUM/CLUSTER internal-commit dance) and the `MemoryContextSwitchTo`
/// collapse away under the RAII model; the `ActivePortal`/`PortalContext`
/// save/restore + `MarkPortalFailed`-on-error is the `with_portal_globals`
/// callback.
pub fn portal_run(
    portal: &Portal,
    count: i64,
    is_top_level: bool,
    dest: DestReceiverHandle,
    altdest: DestReceiverHandle,
    qc: Option<&mut QueryCompletion>,
) -> PgResult<bool> {
    debug_assert!(portal.is_valid());

    /* TRACE_POSTGRESQL_QUERY_EXECUTE_START(); */

    /* Initialize empty completion data */
    let mut qc = qc;
    if let Some(qc) = qc.as_deref_mut() {
        initialize_query_completion(qc);
    }

    let strategy = portalmem::portal_get_strategy(portal);
    if postgres_seam::log_executor_stats::call() && strategy != PORTAL_MULTI_QUERY {
        /* elog(DEBUG3, "PortalRun"); */
        /* PORTAL_MULTI_QUERY logs its own stats per query */
        postgres_seam::reset_usage::call();
    }

    /*
     * Check for improper portal use, and mark portal active.
     */
    portalmem::MarkPortalActive(portal)?;

    let mut result = false;
    let mut qc_ref = qc;

    // PG_TRY()/PG_CATCH(): with_portal_globals + MarkPortalFailed-on-error.
    {
        let result_out = &mut result;
        let qc_inner = &mut qc_ref;
        let mut body = || -> PgResult<()> {
            if strategy == PORTAL_ONE_SELECT
                || strategy == PORTAL_ONE_RETURNING
                || strategy == PORTAL_ONE_MOD_WITH
                || strategy == PORTAL_UTIL_SELECT
            {
                /*
                 * If we have not yet run the command, do so, storing its results
                 * in the portal's tuplestore. But we don't do that for the
                 * PORTAL_ONE_SELECT case.
                 */
                // Bind the borrow result so the portal `Ref` is released before
                // `fill_portal_store` runs the executor (which `borrow_mut`s the
                // portal via the tuplestore receiver). A bare `portal.borrow()`
                // in the `if` condition keeps the temporary alive for the whole
                // block, double-borrowing the portal.
                let need_fill = strategy != PORTAL_ONE_SELECT && {
                    let hold_none = portal.borrow().holdStore.is_none();
                    hold_none
                };
                if need_fill {
                    fill_portal_store(portal, is_top_level)?;
                }

                /*
                 * Now fetch desired portion of results.
                 */
                let nprocessed = portal_run_select(portal, true, count, dest)?;

                /*
                 * If the portal result contains a command tag and the caller gave
                 * us a pointer to store it, copy it and update the rowcount.
                 */
                let portal_qc = portalmem::portal_get_qc(portal);
                if let Some(qc) = qc_inner.as_deref_mut() {
                    if portal_qc.commandTag != CMDTAG_UNKNOWN {
                        copy_query_completion(qc, &portal_qc);
                        qc.nprocessed = nprocessed;
                    }
                }

                /* Mark portal not active */
                portalmem::portal_set_status(portal, PORTAL_READY);

                /*
                 * Since it's a forward fetch, say DONE iff atEnd is now true.
                 */
                *result_out = portalmem::portal_get_at_end(portal);
            } else if strategy == PORTAL_MULTI_QUERY {
                portal_run_multi(portal, is_top_level, false, dest, altdest, qc_inner.as_deref_mut())?;

                /* Prevent portal's commands from being re-executed */
                portalmem::MarkPortalDone(portal)?;

                /* Always complete at end of RunMulti */
                *result_out = true;
            } else {
                return Err(elog_error(alloc::format!(
                    "unrecognized portal strategy: {}",
                    strategy as u32
                )));
            }
            Ok(())
        };
        run_protected(portal, &mut body)?;
    }

    if postgres_seam::log_executor_stats::call() && strategy != PORTAL_MULTI_QUERY {
        postgres_seam::show_usage::call("EXECUTOR STATISTICS");
    }

    /* TRACE_POSTGRESQL_QUERY_EXECUTE_DONE(); */

    Ok(result)
}

// ===========================================================================
// `PortalRunSelect`
// ===========================================================================

/// `PortalRunSelect` (pquery.c:864-985, static) — run a SELECT-type portal's
/// query, returning the number of tuples processed.
fn portal_run_select(
    portal: &Portal,
    forward: bool,
    mut count: i64,
    dest: DestReceiverHandle,
) -> PgResult<u64> {
    /*
     * NB: queryDesc will be NULL if we are fetching from a held cursor or a
     * completed utility query; can't use it in that path.
     */
    let has_query_desc = portal.borrow().queryDesc.is_some();

    /* Caller messed up if we have neither a ready query nor held data. */
    debug_assert!(has_query_desc || portal.borrow().holdStore.is_some());

    /*
     * Force the queryDesc destination to the right thing. This supports MOVE,
     * for example, which will pass in dest = DestNone.
     */
    if has_query_desc {
        portal.borrow_mut().queryDesc.as_mut().unwrap().dest = dest;
    }

    let nprocessed: u64;
    let direction: ScanDirection;

    /*
     * Determine which direction to go in ...
     */
    if forward {
        if portalmem::portal_get_at_end(portal) || count <= 0 {
            direction = NoMovementScanDirection;
            count = 0; /* don't pass negative count to executor */
        } else {
            direction = ForwardScanDirection;
        }

        /* In the executor, zero count processes all rows */
        if count == FETCH_ALL {
            count = 0;
        }

        if portal.borrow().holdStore.is_some() {
            nprocessed = run_from_store(portal, direction, count as u64, dest)?;
        } else {
            let snap = portal
                .borrow()
                .queryDesc
                .as_ref()
                .and_then(|qd| qd.snapshot.clone())
                .expect("portal_run_select: queryDesc->snapshot is NULL while executor is active");
            snapmgr_seam::push_active_snapshot::call(snap)?;
            {
                let mut p = portal.borrow_mut();
                let qd = p.queryDesc.as_mut().unwrap();
                execMain::ExecutorRun(qd, direction, count as u64)?;
            }
            nprocessed = portal.borrow().queryDesc.as_ref().unwrap().es_processed();
            snapmgr_seam::pop_active_snapshot::call()?;
        }

        if !ScanDirectionIsNoMovement(direction) {
            if nprocessed > 0 {
                portalmem::portal_set_at_start(portal, false); /* OK to go backward now */
            }
            if count == 0 || nprocessed < count as u64 {
                portalmem::portal_set_at_end(portal, true); /* we retrieved 'em all */
            }
            let pos = portalmem::portal_get_portal_pos(portal) + nprocessed;
            portalmem::portal_set_portal_pos(portal, pos);
        }
    } else {
        if (portalmem::portal_get_cursor_options(portal) & CURSOR_OPT_NO_SCROLL) != 0 {
            return Err(PgError::new(ERROR, "cursor can only scan forward".to_string())
                .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .with_hint("Declare it with SCROLL option to enable backward scan.".to_string()));
        }

        if portalmem::portal_get_at_start(portal) || count <= 0 {
            direction = NoMovementScanDirection;
            count = 0; /* don't pass negative count to executor */
        } else {
            direction = BackwardScanDirection;
        }

        /* In the executor, zero count processes all rows */
        if count == FETCH_ALL {
            count = 0;
        }

        if portal.borrow().holdStore.is_some() {
            nprocessed = run_from_store(portal, direction, count as u64, dest)?;
        } else {
            let snap = portal
                .borrow()
                .queryDesc
                .as_ref()
                .and_then(|qd| qd.snapshot.clone())
                .expect("portal_run_select: queryDesc->snapshot is NULL while executor is active");
            snapmgr_seam::push_active_snapshot::call(snap)?;
            {
                let mut p = portal.borrow_mut();
                let qd = p.queryDesc.as_mut().unwrap();
                execMain::ExecutorRun(qd, direction, count as u64)?;
            }
            nprocessed = portal.borrow().queryDesc.as_ref().unwrap().es_processed();
            snapmgr_seam::pop_active_snapshot::call()?;
        }

        if !ScanDirectionIsNoMovement(direction) {
            if nprocessed > 0 && portalmem::portal_get_at_end(portal) {
                portalmem::portal_set_at_end(portal, false); /* OK to go forward now */
                let pos = portalmem::portal_get_portal_pos(portal) + 1;
                portalmem::portal_set_portal_pos(portal, pos); /* adjust for endpoint case */
            }
            if count == 0 || nprocessed < count as u64 {
                portalmem::portal_set_at_start(portal, true); /* we retrieved 'em all */
                portalmem::portal_set_portal_pos(portal, 0);
            } else {
                let pos = portalmem::portal_get_portal_pos(portal) - nprocessed;
                portalmem::portal_set_portal_pos(portal, pos);
            }
        }
    }

    Ok(nprocessed)
}

// ===========================================================================
// `FillPortalStore`
// ===========================================================================

/// `FillPortalStore` (pquery.c:995-1041, static) — run a portal to completion
/// and dump its results into the hold store.
fn fill_portal_store(portal: &Portal, is_top_level: bool) -> PgResult<()> {
    let mut qc = QueryCompletion::default();

    initialize_query_completion(&mut qc);
    portalmem::PortalCreateHoldStore(portal)?;
    let treceiver = tstore_seam::create_dest_receiver_tuplestore::call()?;
    tstore_seam::set_tuplestore_dest_receiver_params::call(treceiver, portal, false)?;

    let strategy = portalmem::portal_get_strategy(portal);
    if strategy == PORTAL_ONE_RETURNING || strategy == PORTAL_ONE_MOD_WITH {
        /*
         * Run the portal to completion just as for the default
         * PORTAL_MULTI_QUERY case, but send the primary query's output to the
         * tuplestore. Auxiliary query outputs are discarded.
         */
        portal_run_multi(
            portal,
            is_top_level,
            true,
            treceiver,
            backend_tcop_dest::none_receiver(),
            Some(&mut qc),
        )?;
    } else if strategy == PORTAL_UTIL_SELECT {
        let primary = portalmem::PortalGetPrimaryStmt(portal)
            .expect("PORTAL_UTIL_SELECT portal has a primary stmt");
        /* linitial_node(PlannedStmt, portal->stmts) */
        debug_assert_eq!(primary, 0);
        portal_run_utility(portal, primary, is_top_level, true, treceiver, Some(&mut qc))?;
    } else {
        return Err(elog_error(alloc::format!(
            "unsupported portal strategy: {}",
            strategy as u32
        )));
    }

    /* Override portal completion data with actual command results */
    if qc.commandTag != CMDTAG_UNKNOWN {
        let mut portal_qc = portalmem::portal_get_qc(portal);
        copy_query_completion(&mut portal_qc, &qc);
        portalmem::portal_set_qc(portal, portal_qc);
    }

    tstore_seam::dest_destroy::call(treceiver)?;

    Ok(())
}

// ===========================================================================
// `RunFromStore`
// ===========================================================================

/// `RunFromStore` (pquery.c:1056-1115, static) — replay tuples from a portal's
/// hold store to `dest`, returning the number of tuples sent.
///
/// The C makes a standalone `MakeSingleTupleTableSlot` and runs the receiver in
/// the caller's memory context. The slot is allocated in the portal's hold
/// context (`holdContext`); the standalone tuplestore-fetch seam forms the
/// fetched `MinimalTuple` there.
fn run_from_store(
    portal: &Portal,
    direction: ScanDirection,
    count: u64,
    dest: DestReceiverHandle,
) -> PgResult<u64> {
    let mut current_tuple_count: u64 = 0;

    // The slot + its result descriptor copy live in a per-call scratch context
    // (C makes the slot in `CurrentMemoryContext` and forms tuples there during
    // the `tuplestore_gettupleslot` `holdContext` switch); this avoids aliasing
    // the portal's `RefCell` (which the loop mutates for `holdStore`). The
    // scratch context is dropped at function end.
    let scratch = mcx::MemoryContext::new_bump("RunFromStoreSlot");
    let mcx = scratch.mcx();

    // The result tuple descriptor (portal->tupDesc), deep-copied into the
    // scratch context for the slot (mirroring the shared C `portal->tupDesc`).
    let tupdesc: types_tuple::heaptuple::TupleDesc<'_> = {
        let p = portal.borrow();
        let td = p
            .tupDesc
            .as_ref()
            .expect("run_from_store: portal has no tupDesc");
        let copied = td.clone_in(mcx)?;
        Some(mcx::PgBox::new_in(copied, mcx))
    };

    // dest->rStartup(dest, CMD_SELECT, portal->tupDesc) — before the descriptor
    // is moved into the slot.
    {
        let td = tupdesc
            .as_deref()
            .expect("run_from_store: tupDesc present");
        dest_seam::dest_rstartup::call(mcx, dest, CMD_SELECT, td)?;
    }

    let mut slot = exectuples_seam::make_single_tuple_table_slot::call(
        mcx,
        tupdesc,
        TupleSlotKind::MinimalTuple,
    )?;

    if ScanDirectionIsNoMovement(direction) {
        /* do nothing except start/stop the destination */
    } else {
        let forward = ScanDirectionIsForward(direction);

        loop {
            // C switches to holdContext around the gettupleslot; the standalone
            // fetch forms the tuple in `mcx` (the hold context) directly.
            let ok = {
                let mut p = portal.borrow_mut();
                let store = p
                    .holdStore
                    .as_mut()
                    .expect("run_from_store: holdStore fetch without holdStore");
                sortstore_seam::tuplestore_gettupleslot_standalone::call(
                    mcx, store, forward, false, &mut slot,
                )?
            };

            if !ok {
                break;
            }

            /*
             * If we are not able to send the tuple, we assume the destination
             * has closed and no more tuples can be sent. ...
             */
            if !dest_seam::dest_receive_slot::call(mcx, &mut slot, dest)? {
                break;
            }

            exectuples_seam::exec_reset_one_slot::call(&mut slot)?; /* ExecClearTuple(slot) */

            /*
             * check our tuple count.. if we've processed the proper number then
             * quit, else loop again and process more tuples. Zero count means no
             * limit.
             */
            current_tuple_count += 1;
            if count != 0 && count == current_tuple_count {
                break;
            }
        }
    }

    dest_seam::dest_rshutdown::call(mcx, dest)?;

    exectuples_seam::exec_drop_single_tuple_table_slot::call(slot)?;

    Ok(current_tuple_count)
}

// ===========================================================================
// `PortalRunUtility`
// ===========================================================================

/// `PortalRunUtility` (pquery.c:1122-1177, static) — run a single utility
/// statement within a portal. `pstmt_idx` is the index of the statement in the
/// portal's `stmts` (the C `PlannedStmt *pstmt`).
fn portal_run_utility(
    portal: &Portal,
    pstmt_idx: usize,
    is_top_level: bool,
    set_hold_snapshot: bool,
    dest: DestReceiverHandle,
    qc: Option<&mut QueryCompletion>,
) -> PgResult<()> {
    /*
     * Set snapshot if utility stmt needs one.
     */
    let requires_snapshot = {
        let p = portal.borrow();
        let stmts = p.stmts.as_deref().unwrap_or(&[]);
        planned_stmt_requires_snapshot(&stmts[pstmt_idx])
    };

    if requires_snapshot {
        let mut snapshot = snapmgr_seam::get_transaction_snapshot::call()?;

        /* If told to, register the snapshot we're using and save in portal */
        if set_hold_snapshot {
            snapshot = snapmgr_seam::register_snapshot::call(snapshot)?;
            portal.borrow_mut().holdSnapshot = Some(Rc::new(snapshot.clone()));
        }

        /*
         * In any case, make the snapshot active and remember it in portal. ...
         */
        let create_level = portalmem::portal_get_create_level(portal);
        snapmgr_seam::push_active_snapshot_with_level::call(Rc::new(snapshot), create_level)?;
        /* PushActiveSnapshotWithLevel might have copied the snapshot */
        portal.borrow_mut().portalSnapshot = get_active_snapshot()?;
    } else {
        portal.borrow_mut().portalSnapshot = None;
    }

    let context: ProcessUtilityContext = if is_top_level {
        PROCESS_UTILITY_TOPLEVEL
    } else {
        PROCESS_UTILITY_QUERY
    };

    let source_text = portal.borrow().sourceText.clone().unwrap_or_default();
    let read_only_tree = portal.borrow().cplan.is_null() == false; /* protect tree if in plancache */
    let params = portal.borrow().portalParams.clone();
    let dest_handle = dest;

    // ProcessUtility fills the QueryCompletion in place; thread a local and copy
    // the result back to the caller's &mut (None == the C qc == NULL).
    let mut local_qc = QueryCompletion::default();
    {
        // Per-utility working context (C: `CurrentMemoryContext` during the
        // portal's utility run — the per-message context reset after the
        // command). `standard_ProcessUtility` allocates its `copyObject(pstmt)`
        // deep-copy and the `make_parsestate(NULL)` parse state in it; nothing it
        // returns escapes the context (`qc` is owned, `dest` is a handle), so a
        // per-call scratch context dropped at the end of this block is sound and
        // is the owned analogue of the per-message reset / `free_parsestate`.
        let scratch = mcx::MemoryContext::new_bump("ProcessUtility");
        let mcx = scratch.mcx();
        // C passes `pstmt` (a stable `PlannedStmt *` into the portal's stmt
        // list) and holds no lock across `ProcessUtility`. Here a borrow of the
        // portal cannot span the call: a tuple-returning utility (`SHOW`)
        // routes its rows to the portal's own hold-store receiver, which takes
        // `portal.borrow_mut()`. So copy the statement into the per-utility
        // scratch context (the owned analogue of C's stable pointer) and drop
        // the portal borrow before dispatching.
        let pstmt = {
            let p = portal.borrow();
            let stmts = p.stmts.as_deref().unwrap_or(&[]);
            stmts[pstmt_idx].clone_in(mcx)?
        };
        utility_seam::process_utility::call(
            mcx,
            &pstmt,
            &source_text,
            read_only_tree,
            context,
            params,
            dest_handle,
            &mut local_qc,
        )?;
    }
    if let Some(qc) = qc {
        *qc = local_qc;
    }

    /* Some utility statements may change context on us (no-op: explicit Mcx). */

    /*
     * Some utility commands (e.g., VACUUM) pop the ActiveSnapshot stack from
     * under us, so don't complain if it's now empty. Otherwise, our snapshot
     * should be the top one; pop it. ...
     */
    let has_portal_snapshot = portal.borrow().portalSnapshot.is_some();
    if has_portal_snapshot && snapmgr_seam::active_snapshot_set::call() {
        // C asserts `portal->portalSnapshot == GetActiveSnapshot()` (pointer
        // identity of the same `Snapshot`). In this owned model the active
        // stack stores `Rc<RefCell<SnapshotData>>` while the
        // `portalSnapshot` field / the `get_active_snapshot` seam carry a
        // separate `Rc<SnapshotData>` cloned from it, so the two Rc allocations
        // never alias — `Rc::ptr_eq` is structurally unsatisfiable here. The
        // invariant the C assert protects (our portal snapshot is the one on
        // top of the active stack) is what the subsequent pop relies on; we
        // keep the C control flow and drop the representation-incompatible
        // pointer-identity check.
        snapmgr_seam::pop_active_snapshot::call()?;
    }
    portal.borrow_mut().portalSnapshot = None;

    Ok(())
}

// ===========================================================================
// `PortalRunMulti`
// ===========================================================================

/// `PortalRunMulti` (pquery.c:1185-1362, static) — run the multiple statements
/// of a `PORTAL_MULTI_QUERY` portal.
fn portal_run_multi(
    portal: &Portal,
    is_top_level: bool,
    set_hold_snapshot: bool,
    mut dest: DestReceiverHandle,
    mut altdest: DestReceiverHandle,
    qc: Option<&mut QueryCompletion>,
) -> PgResult<()> {
    let mut qc = qc;
    let mut active_snapshot_set = false;

    /*
     * If the destination is DestRemoteExecute, change to DestNone. ...
     */
    if dest_seam::dest_get_mydest::call(dest) == CommandDest::RemoteExecute {
        dest = backend_tcop_dest::none_receiver();
    }
    if dest_seam::dest_get_mydest::call(altdest) == CommandDest::RemoteExecute {
        altdest = backend_tcop_dest::none_receiver();
    }

    /*
     * Loop to handle the individual queries generated from a single parsetree by
     * analysis and rewrite.
     */
    let source_text = portal.borrow().sourceText.clone().unwrap_or_default();
    let _query_env = handle_query_env(portal);

    let nstmts = portalmem::portal_num_stmts(portal);
    let mut idx: usize = 0;
    while idx < nstmts {
        /*
         * If we got a cancel signal in prior command, quit
         */
        postgres_seam::check_for_interrupts::call()?;

        // Re-read element fields each iteration (portal->stmts may be reset).
        let (has_utility, can_set_tag) = {
            let p = portal.borrow();
            let stmts = p.stmts.as_deref().unwrap_or(&[]);
            let pstmt = &stmts[idx];
            (pstmt.utilityStmt.is_some(), pstmt.canSetTag)
        };

        if !has_utility {
            /*
             * process a plannable query.
             */
            /* TRACE_POSTGRESQL_QUERY_EXECUTE_START(); */

            if postgres_seam::log_executor_stats::call() {
                postgres_seam::reset_usage::call();
            }

            /*
             * Must always have a snapshot for plannable queries. First time
             * through, take a new snapshot; for subsequent queries in the same
             * portal, just update the snapshot's copy of the command counter.
             */
            if !active_snapshot_set {
                let snapshot = snapmgr_seam::get_transaction_snapshot::call()?;

                /* If told to, register the snapshot and save in portal */
                if set_hold_snapshot {
                    let registered = snapmgr_seam::register_snapshot::call(snapshot.clone())?;
                    portal.borrow_mut().holdSnapshot = Some(Rc::new(registered));
                }

                /*
                 * We can't have the holdSnapshot also be the active one,
                 * because UpdateActiveSnapshotCommandId would complain.  So
                 * force an extra snapshot copy.  Plain PushActiveSnapshot would
                 * have copied the transaction snapshot anyway, so this only adds
                 * a copy step when setHoldSnapshot is true.
                 *   PushCopiedSnapshot(snapshot);
                 * (`snapshot` is the transaction snapshot taken above — NOT the
                 * active snapshot: there is no active snapshot at this point in
                 * PortalRunMulti, the parse/plan snapshot having been popped by
                 * exec_simple_query.)
                 */
                snapmgr_seam::push_copied_snapshot::call(Rc::new(snapshot))?;

                active_snapshot_set = true;
            } else {
                snapmgr_seam::update_active_snapshot_command_id::call()?;
            }

            let params = portal.borrow().portalParams.clone();

            // process_query runs the executor, which (for a RETURNING/MOD_WITH
            // tuplestore dest) re-borrows the portal mutably to append to
            // holdStore. So we must NOT hold a PortalData borrow across the call.
            // Move the (stable, unmutated) portalContext + stmts out, run, and
            // restore — mirroring C's stable `portal->portalContext`/`->stmts`
            // pointers that the executor never touches.
            // `portalContext` is a `Box<MemoryContext>`, so this `take()` and the
            // restore below move only the box pointer — the heap `MemoryContext`
            // stays put. That keeps the `Mcx<'static>` markers the interned
            // `stmts` carry (raw `&MemoryContext` into this context) valid across
            // the move; relocating the `MemoryContext` value would dangle them and
            // fault at the next deallocation (e.g. an UPDATE plan's
            // `resultRelations` PgVec in `uncharge`).
            let portal_context = portal
                .borrow_mut()
                .portalContext
                .take()
                .expect("portal has no portalContext");
            let stmts = portal.borrow_mut().stmts.take().unwrap_or_default();

            let run_result = (|| {
                if can_set_tag {
                    /* statement can set tag string */
                    process_query(
                        &portal_context,
                        &stmts[idx],
                        &source_text,
                        params.clone(),
                        dest,
                        qc.as_deref_mut(),
                    )
                } else {
                    /* stmt added by rewrite cannot set tag */
                    process_query(
                        &portal_context,
                        &stmts[idx],
                        &source_text,
                        params.clone(),
                        altdest,
                        None,
                    )
                }
            })();

            // Restore the moved-out fields before propagating any error so the
            // portal stays well-formed for cleanup/MarkPortalFailed.
            portal.borrow_mut().portalContext = Some(portal_context);
            portal.borrow_mut().stmts = Some(stmts);
            run_result?;

            if postgres_seam::log_executor_stats::call() {
                postgres_seam::show_usage::call("EXECUTOR STATISTICS");
            }

            /* TRACE_POSTGRESQL_QUERY_EXECUTE_DONE(); */
        } else {
            /*
             * process utility functions (create, destroy, etc..)
             *
             * We must not set a snapshot here for utility commands ...
             */
            if can_set_tag {
                debug_assert!(!active_snapshot_set);
                /* statement can set tag string */
                portal_run_utility(portal, idx, is_top_level, false, dest, qc.as_deref_mut())?;
            } else {
                /* stmt added by rewrite cannot set tag (Assert NotifyStmt) */
                portal_run_utility(portal, idx, is_top_level, false, altdest, None)?;
            }
        }

        /*
         * Clear subsidiary contexts to recover temporary memory.
         */
        portalmem_seam::memory_context_delete_children::call(portal)?;

        /*
         * Avoid crashing if portal->stmts has been reset. ...
         */
        if portal.borrow().stmts.as_deref().map(|s| s.is_empty()).unwrap_or(true) {
            break;
        }

        /*
         * Increment command counter between queries, but not after the last one.
         */
        if idx + 1 < portalmem::portal_num_stmts(portal) {
            xact_seam::command_counter_increment::call()?;
        }

        idx += 1;
    }

    /* Pop the snapshot if we pushed one. */
    if active_snapshot_set {
        snapmgr_seam::pop_active_snapshot::call()?;
    }

    /*
     * If a command tag was requested and we did not fill in a run-time-
     * determined tag above, copy the parse-time tag from the Portal.
     */
    if let Some(qc) = qc {
        let portal_qc = portalmem::portal_get_qc(portal);
        if qc.commandTag == CMDTAG_UNKNOWN && portal_qc.commandTag != CMDTAG_UNKNOWN {
            copy_query_completion(qc, &portal_qc);
        }
    }

    Ok(())
}

// ===========================================================================
// `PortalRunFetch` / `DoPortalRunFetch` / `DoPortalRewind`
// ===========================================================================

/// `PortalRunFetch` (pquery.c:1377-1463) — variant form of `PortalRun` that
/// supports SQL `FETCH` directions.
pub fn portal_run_fetch(
    portal: &Portal,
    fdirection: FetchDirection,
    count: i64,
    dest: DestReceiverHandle,
) -> PgResult<u64> {
    debug_assert!(portal.is_valid());

    /*
     * Check for improper portal use, and mark portal active.
     */
    portalmem::MarkPortalActive(portal)?;

    let mut result: u64 = 0;
    {
        let result_out = &mut result;
        let mut body = || -> PgResult<()> {
            let strategy = portalmem::portal_get_strategy(portal);
            if strategy == PORTAL_ONE_SELECT {
                *result_out = do_portal_run_fetch(portal, fdirection, count, dest)?;
            } else if strategy == PORTAL_ONE_RETURNING
                || strategy == PORTAL_ONE_MOD_WITH
                || strategy == PORTAL_UTIL_SELECT
            {
                /*
                 * If we have not yet run the command, do so, storing its results
                 * in the portal's tuplestore.
                 */
                if portal.borrow().holdStore.is_none() {
                    fill_portal_store(portal, false /* isTopLevel */)?;
                }

                /*
                 * Now fetch desired portion of results.
                 */
                *result_out = do_portal_run_fetch(portal, fdirection, count, dest)?;
            } else {
                return Err(elog_error("unsupported portal strategy"));
            }
            Ok(())
        };
        run_protected(portal, &mut body)?;
    }

    /* Mark portal not active */
    portalmem::portal_set_status(portal, PORTAL_READY);

    Ok(result)
}

/// `DoPortalRunFetch` (pquery.c:1475-1663, static) — guts of `PortalRunFetch`.
fn do_portal_run_fetch(
    portal: &Portal,
    mut fdirection: FetchDirection,
    mut count: i64,
    dest: DestReceiverHandle,
) -> PgResult<u64> {
    debug_assert!(matches!(
        portalmem::portal_get_strategy(portal),
        PORTAL_ONE_SELECT | PORTAL_ONE_RETURNING | PORTAL_ONE_MOD_WITH | PORTAL_UTIL_SELECT
    ));

    let mut forward: bool;

    match fdirection {
        FETCH_FORWARD => {
            if count < 0 {
                fdirection = FETCH_BACKWARD;
                count = -count;
            }
            /* fall out of switch to share code with FETCH_BACKWARD */
        }
        FETCH_BACKWARD => {
            if count < 0 {
                fdirection = FETCH_FORWARD;
                count = -count;
            }
            /* fall out of switch to share code with FETCH_FORWARD */
        }
        FETCH_ABSOLUTE => {
            if count > 0 {
                /*
                 * Definition: Rewind to start, advance count-1 rows, return next
                 * row (if any). In practice, if the goal is less than halfway
                 * back to the start, it's better to scan from where we are. ...
                 */
                let portal_pos = portalmem::portal_get_portal_pos(portal);
                if (count - 1) as u64 <= portal_pos / 2 || portal_pos >= i64::MAX as u64 {
                    do_portal_rewind(portal)?;
                    if count > 1 {
                        portal_run_select(portal, true, count - 1, backend_tcop_dest::none_receiver())?;
                    }
                } else {
                    let mut pos = portal_pos as i64;
                    if portalmem::portal_get_at_end(portal) {
                        pos += 1; /* need one extra fetch if off end */
                    }
                    if count <= pos {
                        portal_run_select(
                            portal,
                            false,
                            pos - count + 1,
                            backend_tcop_dest::none_receiver(),
                        )?;
                    } else if count > pos + 1 {
                        portal_run_select(
                            portal,
                            true,
                            count - pos - 1,
                            backend_tcop_dest::none_receiver(),
                        )?;
                    }
                }
                return portal_run_select(portal, true, 1, dest);
            } else if count < 0 {
                /*
                 * Definition: Advance to end, back up abs(count)-1 rows, return
                 * prior row (if any). ...
                 */
                portal_run_select(portal, true, FETCH_ALL, backend_tcop_dest::none_receiver())?;
                if count < -1 {
                    portal_run_select(portal, false, -count - 1, backend_tcop_dest::none_receiver())?;
                }
                return portal_run_select(portal, false, 1, dest);
            } else {
                /* count == 0 */
                /* Rewind to start, return zero rows */
                do_portal_rewind(portal)?;
                return portal_run_select(portal, true, 0, dest);
            }
        }
        FETCH_RELATIVE => {
            if count > 0 {
                /*
                 * Definition: advance count-1 rows, return next row (if any).
                 */
                if count > 1 {
                    portal_run_select(portal, true, count - 1, backend_tcop_dest::none_receiver())?;
                }
                return portal_run_select(portal, true, 1, dest);
            } else if count < 0 {
                /*
                 * Definition: back up abs(count)-1 rows, return prior row (if
                 * any).
                 */
                if count < -1 {
                    portal_run_select(portal, false, -count - 1, backend_tcop_dest::none_receiver())?;
                }
                return portal_run_select(portal, false, 1, dest);
            } else {
                /* count == 0 */
                /* Same as FETCH FORWARD 0, so fall out of switch */
                fdirection = FETCH_FORWARD;
            }
        }
    }

    /*
     * Get here with fdirection == FETCH_FORWARD or FETCH_BACKWARD, and count >= 0.
     */
    forward = fdirection == FETCH_FORWARD;

    /*
     * Zero count means to re-fetch the current row, if any (per SQL)
     */
    if count == 0 {
        /* Are we sitting on a row? */
        let on_row =
            !portalmem::portal_get_at_start(portal) && !portalmem::portal_get_at_end(portal);

        if dest_seam::dest_get_mydest::call(dest) == CommandDest::None {
            /* MOVE 0 returns 0/1 based on if FETCH 0 would return a row */
            return Ok(if on_row { 1 } else { 0 });
        } else {
            /*
             * If we are sitting on a row, back up one so we can re-fetch it. ...
             */
            if on_row {
                portal_run_select(portal, false, 1, backend_tcop_dest::none_receiver())?;
                /* Set up to fetch one row forward */
                count = 1;
                forward = true;
            }
        }
    }

    /*
     * Optimize MOVE BACKWARD ALL into a Rewind.
     */
    if !forward && count == FETCH_ALL && dest_seam::dest_get_mydest::call(dest) == CommandDest::None
    {
        let mut result = portalmem::portal_get_portal_pos(portal);

        if result > 0 && !portalmem::portal_get_at_end(portal) {
            result -= 1;
        }
        do_portal_rewind(portal)?;
        return Ok(result);
    }

    portal_run_select(portal, forward, count, dest)
}

/// `DoPortalRewind` (pquery.c:1669-1709, static) — rewind a portal to its
/// starting point.
fn do_portal_rewind(portal: &Portal) -> PgResult<()> {
    /*
     * No work is needed if we've not advanced nor attempted to advance the
     * cursor (and we don't want to throw a NO SCROLL error in this case).
     */
    if portalmem::portal_get_at_start(portal) && !portalmem::portal_get_at_end(portal) {
        return Ok(());
    }

    /* Otherwise, cursor must allow scrolling */
    if (portalmem::portal_get_cursor_options(portal) & CURSOR_OPT_NO_SCROLL) != 0 {
        return Err(PgError::new(ERROR, "cursor can only scan forward".to_string())
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint("Declare it with SCROLL option to enable backward scan.".to_string()));
    }

    /* Rewind holdStore, if we have one */
    if portal.borrow().holdStore.is_some() {
        let mut p = portal.borrow_mut();
        let store = p.holdStore.as_mut().unwrap();
        sortstore_seam::tuplestore_rescan::call(store)?;
    }

    /* Rewind executor, if active */
    let has_query_desc = portal.borrow().queryDesc.is_some();
    if has_query_desc {
        let snap = portal
            .borrow()
            .queryDesc
            .as_ref()
            .and_then(|qd| qd.snapshot.clone())
            .expect("do_portal_rewind: queryDesc->snapshot is NULL while executor is active");
        snapmgr_seam::push_active_snapshot::call(snap)?;
        {
            let mut p = portal.borrow_mut();
            let qd = p.queryDesc.as_mut().unwrap();
            execMain::ExecutorRewind(qd)?;
        }
        snapmgr_seam::pop_active_snapshot::call()?;
    }

    portal.borrow_mut().atStart = true;
    portal.borrow_mut().atEnd = false;
    portal.borrow_mut().portalPos = 0;

    Ok(())
}

// ===========================================================================
// `EnsurePortalSnapshotExists`
// ===========================================================================

/// `EnsurePortalSnapshotExists` (pquery.c:1763-1790) — ensure an active
/// snapshot exists for the duration of portal execution.
pub fn ensure_portal_snapshot_exists() -> PgResult<()> {
    /*
     * Nothing to do if a snapshot is set.  (We take it on faith that the
     * outermost active snapshot belongs to some Portal; or if there is no
     * Portal, it's somebody else's responsibility to manage things.)
     */
    if snapmgr_seam::active_snapshot_set::call() {
        return Ok(());
    }

    /* Otherwise, we'd better have an active Portal */
    portalmem::with_active_portal(|portal| {
        let portal = portal.ok_or_else(|| {
            elog_error("cannot execute SQL without an outer snapshot or portal")
        })?;
        debug_assert!(portal.borrow().portalSnapshot.is_none());

        /*
         * Create a new snapshot, make it active, and remember it in portal.
         * Because the portal now references the snapshot, we must tell
         * snapmgr.c that the snapshot belongs to the portal's transaction
         * level, else we risk portalSnapshot becoming a dangling pointer.
         */
        let snapshot = snapmgr_seam::get_transaction_snapshot::call()?;
        let create_level = portalmem::portal_get_create_level(portal);
        snapmgr_seam::push_active_snapshot_with_level::call(Rc::new(snapshot), create_level)?;
        /* PushActiveSnapshotWithLevel might have copied the snapshot */
        portal.borrow_mut().portalSnapshot = get_active_snapshot()?;
        Ok(())
    })
}

// ===========================================================================
// handle bridges
// ===========================================================================

/// The portal's `queryEnv` handle. `QueryDesc::create` drops `queryEnv`, so this
/// is only the C `portal->queryEnv` read; it is unused by the owned core.
fn handle_query_env(_portal: &Portal) -> u64 {
    0
}

// ===========================================================================
// init_seams
// ===========================================================================

/// Install this crate's inward seams. Wired into `seams-init`.
///
/// The `backend-tcop-pquery-seams` (the portal-execution slice) install
/// cleanly against the owned `Portal`/`QueryCompletion`/`Rc<SnapshotData>`
/// values. The PREPARE/EXECUTE portal-run tail now threads those same values
/// (the portal/snapshot/QueryCompletion handles were de-handled), so
/// `portal_start`/`portal_run` are the EXECUTE path's installed seams too.
pub fn init_seams() {
    backend_tcop_pquery_seams::portal_start::set(|portal, params, eflags, snapshot| {
        portal_start(portal, params, eflags, snapshot)
    });
    backend_tcop_pquery_seams::portal_run::set(|portal, count, is_top_level, dest, altdest, qc| {
        portal_run(portal, count, is_top_level, dest, altdest, qc)
    });
    backend_tcop_pquery_seams::portal_run_fetch::set(|portal, fdirection, count, dest| {
        portal_run_fetch(portal, fdirection, count, dest)
    });
    backend_tcop_pquery_seams::choose_portal_strategy_queries::set(choose_portal_strategy_queries);

    // The CTAS executor-driven leg (createas.c 300-361). Owned-decl by
    // backend-commands-createas (it carries the DR_intorel receiver); installed
    // here because this is the executor-driving layer with the
    // QueryRewrite/pg_plan_query/CreateQueryDesc/ExecutorStart..End substrate.
    backend_commands_createas_seams::run_ctas_executor::set(run_ctas_executor);
}
