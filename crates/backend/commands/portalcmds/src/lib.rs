//! Port of `backend/commands/portalcmds.c` — utility commands affecting
//! portals (SQL cursor commands `DECLARE CURSOR`, `FETCH`/`MOVE`, `CLOSE`),
//! plus the portal cleanup hook and holdable-portal-persistence hook
//! (PostgreSQL 18.3).
//!
//! The `Portal` is the shared open-handle owned by `utils/mmgr/portalmem.c`;
//! this unit reads/writes its consumed fields directly (mirroring the C raw
//! pointer) and calls portal/executor/planner operations through the owning
//! crates' seam crates. C's `MemoryContextSwitchTo(portal->portalContext)`
//! ambient switches become explicit allocations in the portal context's `Mcx`
//! (there is no ambient current context in this repo).

#![allow(non_snake_case)]

use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_CURSOR_NAME,
    ERRCODE_UNDEFINED_CURSOR, ERROR,
};

use ::nodes::nodes::CMD_SELECT;
use ::nodes::portalcmds::{
    DeclareCursorStmt, FetchStmt, ParamListInfo, ParseState, Query, CURSOR_OPT_HOLD,
    CURSOR_OPT_NO_SCROLL, CURSOR_OPT_SCROLL,
};
use ::nodes::nodes::{CmdType, Node};
use ::nodes::parsestmt::{DestReceiverHandle, ParseState as CanonParseState};
use portal::{
    CommandTag, FetchDirection, Portal, QueryCompletion, CMDTAG_FETCH, CMDTAG_MOVE, PORTAL_FAILED,
    PORTAL_ONE_SELECT, PORTAL_READY,
};
use snapshot::SnapshotData;

use tupdesc_seams as tupdesc;
use transam_xact_seams as xact;
use execMain_seams as executor;
use queryjumble_seams as queryjumble;
use parser_analyze_seams as analyze;
use rewritehandler_seams as rewrite;
use dest_seams as dest;
use postgres_seams as postgres;
use pquery_seams as pquery;
use miscinit_seams as miscinit;
use portalmem_seams as portalmem;
use resowner_seams as resowner;
use sort_storage_seams as sortstore;
use snapmgr_seams as snapmgr;
use tstorereceiver_seams as tstore;
use alloc::rc::Rc;

extern crate alloc;

/// Install every seam this crate owns. Wired from `seams-init`.
pub fn init_seams() {
    portalcmds_seams::perform_cursor_open::set(perform_cursor_open_seam);
    portalcmds_seams::perform_portal_fetch::set(perform_portal_fetch_seam);
    portalcmds_seams::perform_portal_close::set(perform_portal_close_seam);
    portalcmds_seams::portal_cleanup::set(PortalCleanup);
    portalcmds_seams::persist_holdable_portal::set(PersistHoldablePortal);

    // The `tcop/utility.c` dispatch (backend-tcop-utility) routes the portal /
    // cursor verbs and the FETCH returns-tuples predicate through its own
    // outward seams. All four are installed here over the *canonical*
    // `copy_query::Query` carried by the raw parse tree at dispatch time:
    // `PerformCursorOpen` / `PerformPortalFetch` decode the canonical
    // `Node<'mcx>` into the cursor/fetch fields and plan via the canonical
    // `query_rewrite_canonical` + `pg_plan_queries_value` seams (the same value
    // entries the simple-Query / plancache pipeline uses), so the trimmed
    // `portalcmds::Query` legacy path (and its uninstalled jumble/rewrite/plan
    // seams) is bypassed entirely on the dispatch spine.
    utility_out_seams::perform_cursor_open::set(perform_cursor_open_canon_arm);
    utility_out_seams::perform_portal_fetch::set(perform_portal_fetch_canon_arm);
    utility_out_seams::perform_portal_close::set(perform_portal_close_seam);
    utility_out_seams::fetch_stmt_portal_tupdesc::set(fetch_stmt_portal_tupdesc_arm);
    utility_out_seams::fetch_stmt_result_desc::set(fetch_stmt_result_desc_arm);
}

/// `UtilityReturnsTuples` FETCH leg — `GetPortalByName(name)->tupDesc != NULL`.
fn fetch_stmt_portal_tupdesc_arm(parsetree: &::nodes::nodes::Node) -> bool {
    let Some(stmt) = parsetree.as_fetchstmt() else {
        panic!("fetch_stmt_portal_tupdesc: parse tree is not a FetchStmt");
    };
    let Some(name) = stmt.portalname.as_deref() else {
        return false;
    };
    // portal = GetPortalByName(name); if (!PortalIsValid(portal)) return false;
    // return portal->tupDesc ? true : false;
    match portalmem::get_portal_by_name::call(name) {
        Ok(Some(portal)) => portal.borrow().tupDesc.is_some(),
        _ => false,
    }
}

/// `UtilityTupleDescriptor` FETCH leg —
/// `CreateTupleDescCopy(GetPortalByName(name)->tupDesc)` (utility.c:2186). MOVE
/// has no result descriptor (`None`); an invalid portal folds to `None`.
fn fetch_stmt_result_desc_arm<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    stmt: &::nodes::nodes::Node<'mcx>,
) -> types_tuple::heaptuple::TupleDesc<'mcx> {
    let Some(fstmt) = stmt.as_fetchstmt() else {
        panic!("fetch_stmt_result_desc: parse tree is not a FetchStmt");
    };
    // MOVE returns no tuples → no descriptor (the returns.rs predicate already
    // gates `ismove`, but mirror the C `if (stmt->ismove) return NULL;`).
    if fstmt.ismove {
        return None;
    }
    let Some(name) = fstmt.portalname.as_deref() else {
        return None;
    };
    // portal = GetPortalByName(name); if (!PortalIsValid(portal)) return NULL;
    // return CreateTupleDescCopy(portal->tupDesc);
    let portal = match portalmem::get_portal_by_name::call(name) {
        Ok(Some(p)) => p,
        _ => return None,
    };
    let p = portal.borrow();
    let src = p.tupDesc.as_ref()?;
    match tupdesc::create_tupledesc_copy::call(mcx, src) {
        Ok(copy) => Some(copy),
        Err(_) => None,
    }
}

// Thin marshal adapters for the inward seams (utility.c calls these).
fn perform_cursor_open_seam<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    pstate: &ParseState,
    cstmt: DeclareCursorStmt,
    params: ParamListInfo,
    is_top_level: bool,
) -> PgResult<()> {
    PerformCursorOpen(mcx, pstate, cstmt, params, is_top_level)
}
fn perform_portal_fetch_seam(
    stmt: &FetchStmt,
    dest: DestReceiverHandle,
    qc: Option<&mut QueryCompletion>,
) -> PgResult<()> {
    PerformPortalFetch(stmt, dest, qc)
}
fn perform_portal_close_seam(name: Option<&str>) -> PgResult<()> {
    PerformPortalClose(name)
}

// ===========================================================================
// Canonical dispatch bridge — DECLARE CURSOR / FETCH over the canonical
// `Node<'mcx>` / `copy_query::Query<'mcx>` carried by the raw parse tree.
//
// `tcop/utility.c` hands the cursor verbs the canonical arena-lifetimed
// `Node<'mcx>` (not the trimmed `portalcmds::Query`). These arms decode the
// node's cursor/fetch fields and drive `PerformCursorOpen` / `PerformPortalFetch`
// directly off the canonical query, planning via the canonical
// `query_rewrite_canonical` + `pg_plan_queries_value` value seams (the same
// entries `exec_simple_query` / plancache use). This is the Query-model bridge
// the seam-install note used to defer: the canonical Query *is* the model the
// rest of the plan/exec pipeline now consumes, so DECLARE CURSOR / FETCH plan
// the real query tree end-to-end rather than the empty trimmed `QueryPayload`.
// ===========================================================================

/// Dispatch arm for `T_DeclareCursorStmt` — decode the canonical `Node` and run
/// the canonical `PerformCursorOpen`.
fn perform_cursor_open_canon_arm<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    pstate: &mut CanonParseState<'mcx>,
    cstmt: &Node<'mcx>,
    params: ParamListInfo,
    is_top_level: bool,
) -> PgResult<()> {
    let Some(stmt) = cstmt.as_declarecursorstmt() else {
        panic!("perform_cursor_open: parse tree is not a DeclareCursorStmt");
    };
    PerformCursorOpenCanonical(mcx, pstate, stmt, params, is_top_level)
}

/// Dispatch arm for `T_FetchStmt` — decode the canonical `Node` and run the
/// canonical `PerformPortalFetch`.
fn perform_portal_fetch_canon_arm<'mcx>(
    _mcx: mcx::Mcx<'mcx>,
    stmt: &Node<'mcx>,
    dest: DestReceiverHandle,
    qc: Option<&mut QueryCompletion>,
) -> PgResult<()> {
    let Some(fstmt) = stmt.as_fetchstmt() else {
        panic!("perform_portal_fetch: parse tree is not a FetchStmt");
    };
    // The canonical `FetchStmt<'mcx>` and the runtime `PerformPortalFetch` agree
    // field-for-field (direction / howMany / portalname / ismove); reuse the
    // existing driver by projecting the canonical node into the trimmed
    // `FetchStmt` it consumes (all four fields are plain scalars / an owned
    // name copy — no query tree is involved in FETCH).
    let trimmed = FetchStmt {
        direction: map_canon_fetch_direction(fstmt.direction),
        howMany: fstmt.how_many,
        portalname: fstmt.portalname.as_ref().map(|s| s.as_str().into()),
        ismove: fstmt.ismove,
    };
    PerformPortalFetch(&trimmed, dest, qc)
}

/// Map the canonical (ddlnodes) `FetchDirection` onto the trimmed-model
/// `FetchDirection` the [`FetchStmt`] driver consumes. The two are the same
/// underlying enum (`crate::ddlnodes::FetchDirection`, re-exported by both
/// modules), so this is the identity — written explicitly to document the
/// crossing.
fn map_canon_fetch_direction(
    d: ::nodes::ddlnodes::FetchDirection,
) -> ::nodes::portalcmds::FetchDirection {
    d
}

/// `PerformCursorOpen` over the canonical `copy_query::Query<'mcx>` carried by
/// the raw parse tree (portalcmds.c lines 40-164). Mirrors [`PerformCursorOpen`]
/// but plans through the canonical value rewrite/plan seams instead of the
/// trimmed-Query legacy seams.
pub fn PerformCursorOpenCanonical<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    pstate: &CanonParseState<'mcx>,
    cstmt: &::nodes::ddlnodes::DeclareCursorStmt<'mcx>,
    params: ParamListInfo,
    is_top_level: bool,
) -> PgResult<()> {
    // Query *query = castNode(Query, cstmt->query);
    let query_node = cstmt
        .query
        .as_ref()
        .expect("PerformCursorOpen: DeclareCursorStmt->query is NULL");
    let query = query_node
        .as_query()
        .expect("PerformCursorOpen: cstmt->query is not a Query node")
        .clone_in(mcx)?;

    let portalname = cstmt.portalname.as_ref().map(|s| s.as_str());
    let options = cstmt.options;

    // Disallow empty-string cursor name (conflicts with protocol-level unnamed
    // portal).
    if portalname.is_none_or(str::is_empty) {
        return Err(PgError::new(ERROR, "invalid cursor name: must not be empty")
            .with_sqlstate(ERRCODE_INVALID_CURSOR_NAME));
    }
    let portalname = portalname.unwrap();

    // If this is a non-holdable cursor, we require that this statement has been
    // executed inside a transaction block (or else, it would have no
    // user-visible effect).
    if (options & CURSOR_OPT_HOLD) == 0 {
        xact::require_transaction_block::call(is_top_level, "DECLARE CURSOR")?;
    } else if miscinit::in_security_restricted_operation::call() {
        return Err(PgError::new(
            ERROR,
            "cannot create a cursor WITH HOLD within security-restricted operation",
        )
        .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE));
    }

    let p_sourcetext = pstate
        .p_sourcetext
        .as_ref()
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();

    // Query contained by DeclareCursor needs to be jumbled if requested.
    //   if (IsQueryIdEnabled()) jstate = JumbleQuery(query);
    //   if (post_parse_analyze_hook) (*post_parse_analyze_hook)(pstate, query, jstate);
    //
    // queryId jumbling is unported (queryId stays 0, jstate NULL) and the
    // post_parse_analyze_hook is NULL by default, so both C `if` guards fall
    // through — identical no-op handling to createas.c's `jumble_and_post_analyze`
    // and analyze.c's `run_post_parse_analyze_hook` (which the rest of the parse
    // pipeline relies on). The cursor query was already jumbled/hooked when it
    // was parse-analyzed as a sub-statement; re-running them here is only
    // observable under `compute_query_id`/an extension hook — a deferred leg
    // (DESIGN_DEBT: canonical jumble/hook for DECLARE CURSOR, gated on the
    // unported queryjumble body + a value-typed post-parse-analyze runner).

    // Parse analysis was done already, but we still have to run the rule
    // rewriter. We do not do AcquireRewriteLocks: we assume the query either
    // came straight from the parser, or suitable locks were acquired by
    // plancache.c.
    //   rewritten = QueryRewrite(query);
    let mut rewritten = rewrite::query_rewrite_canonical::call(mcx, query)?;

    // SELECT should never rewrite to more or less than one query.
    if rewritten.len() != 1 {
        return Err(PgError::new(ERROR, "non-SELECT statement in DECLARE CURSOR"));
    }

    // query = linitial_node(Query, rewritten);
    let query = rewritten.pop().unwrap();

    // if (query->commandType != CMD_SELECT)
    if query.commandType != CmdType::CMD_SELECT {
        return Err(PgError::new(ERROR, "non-SELECT statement in DECLARE CURSOR"));
    }

    // Plan the query, applying the specified options.
    //   plan = pg_plan_query(query, pstate->p_sourcetext, cstmt->options, params);
    let mut plans = postgres::pg_plan_queries_value::call(
        mcx,
        core::slice::from_ref(&query),
        &p_sourcetext,
        options,
        params.clone(),
    )?;
    debug_assert_eq!(plans.len(), 1);
    let plan = plans.pop().expect("pg_plan_queries_value yielded no plan");

    // The scroll decision below inspects `plan->rowMarks`/`plan->planTree`.
    let plan_row_marks_nil = plan.rowMarks.is_none();
    let plan_supports_backward = executor::exec_supports_backward_scan::call(&plan)?;

    // Create a portal and copy the plan and query string into its memory.
    let portal = portalmem::create_portal::call(portalname, false, false)?;

    //    PortalDefineQuery(portal, NULL, queryString, CMDTAG_SELECT,
    //                      list_make1(plan), NULL);  /* always a SELECT */
    portalmem::portal_define_query_select::call(&portal, &p_sourcetext, plan)?;

    // Also copy the outer portal's parameter list into the inner portal's
    // memory context.
    //   params = copyParamList(params);
    let params = portalmem::copy_param_list_into_portal::call(&portal, params)?;

    // Set up options for portal.
    //   portal->cursorOptions = cstmt->options;
    {
        let mut p = portal.borrow_mut();
        p.cursorOptions = options;
    }
    let cursor_options = portal.borrow().cursorOptions;
    if (cursor_options & (CURSOR_OPT_SCROLL | CURSOR_OPT_NO_SCROLL)) == 0 {
        // plan->rowMarks == NIL && ExecSupportsBackwardScan(plan->planTree)
        let mut p = portal.borrow_mut();
        if plan_row_marks_nil && plan_supports_backward {
            p.cursorOptions |= CURSOR_OPT_SCROLL;
        } else {
            p.cursorOptions |= CURSOR_OPT_NO_SCROLL;
        }
    }

    // Start execution, inserting parameters if any.
    //   PortalStart(portal, params, 0, GetActiveSnapshot());
    let active_snapshot = snapmgr::get_active_snapshot::call()?;
    pquery::portal_start::call(&portal, params, 0, active_snapshot)?;

    // Assert(portal->strategy == PORTAL_ONE_SELECT);
    debug_assert_eq!(portal.borrow().strategy, PORTAL_ONE_SELECT);

    // We're done; the query won't actually be run until PerformPortalFetch is
    // called.
    Ok(())
}

// ===========================================================================
// PerformCursorOpen — portalcmds.c lines 40-164
// ===========================================================================

/// `PerformCursorOpen` — execute SQL `DECLARE CURSOR`.
///
/// `mcx` is the caller's working (message) context, where the planned
/// statement is allocated before being copied into the portal's own context
/// (C runs in the message context until `MemoryContextSwitchTo(portalContext)`).
pub fn PerformCursorOpen<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    pstate: &ParseState,
    cstmt: DeclareCursorStmt,
    params: ParamListInfo,
    is_top_level: bool,
) -> PgResult<()> {
    // Query *query = castNode(Query, cstmt->query);
    let query: Query = match cstmt.query {
        Some(q) => *q,
        // castNode asserts the node is a Query; anything else is a caller bug.
        None => panic!("PerformCursorOpen: cstmt->query is not a Query node"),
    };

    let portalname = cstmt.portalname;
    let options = cstmt.options;

    // Disallow empty-string cursor name (conflicts with protocol-level unnamed
    // portal).
    if portalname.as_deref().is_none_or(str::is_empty) {
        return Err(PgError::new(ERROR, "invalid cursor name: must not be empty")
            .with_sqlstate(ERRCODE_INVALID_CURSOR_NAME));
    }
    let portalname = portalname.unwrap();

    // If this is a non-holdable cursor, we require that this statement has been
    // executed inside a transaction block (or else, it would have no
    // user-visible effect).
    if (options & CURSOR_OPT_HOLD) == 0 {
        xact::require_transaction_block::call(is_top_level, "DECLARE CURSOR")?;
    } else if miscinit::in_security_restricted_operation::call() {
        return Err(PgError::new(
            ERROR,
            "cannot create a cursor WITH HOLD within security-restricted operation",
        )
        .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE));
    }

    let p_sourcetext = pstate.p_sourcetext.clone().unwrap_or_default();

    // Query contained by DeclareCursor needs to be jumbled if requested.
    //   if (IsQueryIdEnabled()) jstate = JumbleQuery(query);
    //   if (post_parse_analyze_hook) (*post_parse_analyze_hook)(pstate, query, jstate);
    let jstate = if queryjumble::is_query_id_enabled::call() {
        Some(queryjumble::jumble_query::call(&query)?)
    } else {
        None
    };
    analyze::run_post_parse_analyze_hook::call(pstate, &query, jstate.as_ref())?;

    // Parse analysis was done already, but we still have to run the rule
    // rewriter.  We do not do AcquireRewriteLocks: we assume the query either
    // came straight from the parser, or suitable locks were acquired by
    // plancache.c.
    //   rewritten = QueryRewrite(query);
    let mut rewritten = rewrite::query_rewrite::call(mcx, query)?;

    // SELECT should never rewrite to more or less than one query.
    if rewritten.len() != 1 {
        return Err(PgError::new(ERROR, "non-SELECT statement in DECLARE CURSOR"));
    }

    // query = linitial_node(Query, rewritten);
    let query = rewritten.pop().unwrap();

    // if (query->commandType != CMD_SELECT)
    if query.commandType != CMD_SELECT {
        return Err(PgError::new(ERROR, "non-SELECT statement in DECLARE CURSOR"));
    }

    // Plan the query, applying the specified options.
    //   plan = pg_plan_query(query, pstate->p_sourcetext, cstmt->options, params);
    let plan =
        postgres::pg_plan_query::call(mcx, query, p_sourcetext.clone(), options, params.clone())?;

    // The scroll decision below inspects `plan->rowMarks`/`plan->planTree`. C
    // reads them off the portal-context copy, but copyObject preserves both, so
    // read them off the working-context plan now, before it is handed (and
    // copied) into the portal.
    let plan_row_marks_nil = plan.rowMarks.is_none();
    let plan_supports_backward = executor::exec_supports_backward_scan::call(&plan)?;

    // Create a portal and copy the plan and query string into its memory.
    let portal = portalmem::create_portal::call(&portalname, false, false)?;

    // C: oldContext = MemoryContextSwitchTo(portal->portalContext);
    //    plan = copyObject(plan);
    //    queryString = pstrdup(pstate->p_sourcetext);
    //    PortalDefineQuery(portal, NULL, queryString, CMDTAG_SELECT,
    //                      list_make1(plan), NULL);  /* always a SELECT */
    //    MemoryContextSwitchTo(oldContext);
    //
    // The copyObject/pstrdup-into-portal-context is done by the portal owner
    // (portalmem owns `portal->portalContext`): we hand it the working-context
    // plan and source text and it copies them into the portal's own context as
    // part of defining the query.
    portalmem::portal_define_query_select::call(&portal, &p_sourcetext, plan)?;

    // Also copy the outer portal's parameter list into the inner portal's
    // memory context (likewise done by the owner of the portal context).
    //   params = copyParamList(params);
    let params = portalmem::copy_param_list_into_portal::call(&portal, params)?;

    // Set up options for portal.  If the user didn't specify a SCROLL type,
    // allow or disallow scrolling based on whether it would require any
    // additional runtime overhead to do so.  Also disallow scrolling for FOR
    // UPDATE cursors.
    //   portal->cursorOptions = cstmt->options;
    {
        let mut p = portal.borrow_mut();
        p.cursorOptions = options;
    }
    let cursor_options = portal.borrow().cursorOptions;
    if (cursor_options & (CURSOR_OPT_SCROLL | CURSOR_OPT_NO_SCROLL)) == 0 {
        // plan->rowMarks == NIL && ExecSupportsBackwardScan(plan->planTree)
        let mut p = portal.borrow_mut();
        if plan_row_marks_nil && plan_supports_backward {
            p.cursorOptions |= CURSOR_OPT_SCROLL;
        } else {
            p.cursorOptions |= CURSOR_OPT_NO_SCROLL;
        }
    }

    // Start execution, inserting parameters if any.
    //   PortalStart(portal, params, 0, GetActiveSnapshot());
    let active_snapshot = snapmgr::get_active_snapshot::call()?;
    pquery::portal_start::call(&portal, params, 0, active_snapshot)?;

    // Assert(portal->strategy == PORTAL_ONE_SELECT);
    debug_assert_eq!(portal.borrow().strategy, PORTAL_ONE_SELECT);

    // We're done; the query won't actually be run until PerformPortalFetch is
    // called.
    Ok(())
}

// ===========================================================================
// PerformPortalFetch — portalcmds.c lines 166-217
// ===========================================================================

/// `PerformPortalFetch` — execute SQL `FETCH` or `MOVE`. `dest` is the
/// router-keyed [`DestReceiverHandle`] the dispatcher built.
pub fn PerformPortalFetch(
    stmt: &FetchStmt,
    mut dest: DestReceiverHandle,
    qc: Option<&mut QueryCompletion>,
) -> PgResult<()> {
    // Disallow empty-string cursor name.
    if stmt.portalname.as_deref().is_none_or(str::is_empty) {
        return Err(PgError::new(ERROR, "invalid cursor name: must not be empty")
            .with_sqlstate(ERRCODE_INVALID_CURSOR_NAME));
    }
    let portalname = stmt.portalname.as_deref().unwrap();

    // get the portal from the portal name.
    let Some(portal) = portalmem::get_portal_by_name::call(portalname)? else {
        return Err(
            PgError::new(ERROR, alloc::format!("cursor \"{portalname}\" does not exist"))
                .with_sqlstate(ERRCODE_UNDEFINED_CURSOR),
        );
    };

    // Adjust dest if needed.  MOVE wants destination DestNone.
    //   dest = CreateDestReceiver(DestNone);
    if stmt.ismove {
        dest = dest::create_dest_receiver::call(types_dest::CommandDest::None);
    }

    // nprocessed = PortalRunFetch(portal, stmt->direction, stmt->howMany, dest);
    let direction = map_fetch_direction(stmt.direction);
    let nprocessed = pquery::portal_run_fetch::call(&portal, direction, stmt.howMany, dest)?;

    // if (qc) SetQueryCompletion(qc, stmt->ismove ? CMDTAG_MOVE : CMDTAG_FETCH, nprocessed);
    if let Some(qc) = qc {
        let tag: CommandTag = if stmt.ismove { CMDTAG_MOVE } else { CMDTAG_FETCH };
        set_query_completion(qc, tag, nprocessed);
    }

    Ok(())
}

/// `SetQueryCompletion(qc, commandTag, nprocessed)` (`tcop/cmdtag.h`) — store
/// command completion data. Inline (the C is a trivial field set).
fn set_query_completion(qc: &mut QueryCompletion, command_tag: CommandTag, nprocessed: u64) {
    qc.commandTag = command_tag;
    qc.nprocessed = nprocessed;
}

/// Map the parser's `FetchDirection` (types-nodes) onto the portal-runtime
/// `FetchDirection` (types-portal); the enum is identical (parsenodes.h).
fn map_fetch_direction(d: ::nodes::portalcmds::FetchDirection) -> FetchDirection {
    use ::nodes::portalcmds::FetchDirection as N;
    match d {
        N::FETCH_FORWARD => FetchDirection::FETCH_FORWARD,
        N::FETCH_BACKWARD => FetchDirection::FETCH_BACKWARD,
        N::FETCH_ABSOLUTE => FetchDirection::FETCH_ABSOLUTE,
        N::FETCH_RELATIVE => FetchDirection::FETCH_RELATIVE,
    }
}

// ===========================================================================
// PerformPortalClose — portalcmds.c lines 219-260
// ===========================================================================

/// `PerformPortalClose` — close a cursor.  `name` `None` means `CLOSE ALL`.
pub fn PerformPortalClose(name: Option<&str>) -> PgResult<()> {
    // NULL means CLOSE ALL.
    let name = match name {
        None => {
            portalmem::portal_hash_table_delete_all::call()?;
            return Ok(());
        }
        Some(name) => name,
    };

    // Disallow empty-string cursor name.
    if name.is_empty() {
        return Err(PgError::new(ERROR, "invalid cursor name: must not be empty")
            .with_sqlstate(ERRCODE_INVALID_CURSOR_NAME));
    }

    // get the portal from the portal name.
    let Some(portal) = portalmem::get_portal_by_name::call(name)? else {
        return Err(
            PgError::new(ERROR, alloc::format!("cursor \"{name}\" does not exist"))
                .with_sqlstate(ERRCODE_UNDEFINED_CURSOR),
        );
    };

    // Note: PortalCleanup is called as a side-effect, if not already done.
    portalmem::portal_drop::call(&portal, false)?;

    Ok(())
}

// ===========================================================================
// PortalCleanup — portalcmds.c lines 262-315
// ===========================================================================

/// `PortalCleanup` — standard cleanup hook for portals when they are dropped.
pub fn PortalCleanup(portal: Portal) -> PgResult<()> {
    debug_assert!(portal.is_valid());

    // Shut down executor, if still running.  We skip this during error abort,
    // since other mechanisms will take care of releasing executor resources,
    // and we can't be sure that ExecutorEnd itself wouldn't fail.
    //
    //   queryDesc = portal->queryDesc;
    //   if (queryDesc) { portal->queryDesc = NULL; if (status != PORTAL_FAILED) {...} }
    let query_desc = portal.borrow_mut().queryDesc.take();
    if let Some(query_desc) = query_desc {
        // We must make the portal's resource owner current (C: save
        // CurrentResourceOwner, set it to portal->resowner, restore after).
        // The save/restore-global idiom is a scoped callback here.
        //
        // Unlike C — where `queryDesc` is a bare pointer that is simply left
        // dangling (leaked into the portal context) on the PORTAL_FAILED path —
        // this port `take()`s an *owned* QueryDesc, so when it goes out of scope
        // it is dropped, running `Relation::drop` → `RelationClose` for every
        // scan relation in `es_relations`. Those relcache pins were remembered
        // against the portal's resource owner (CurrentResourceOwner was the
        // portal owner during ExecutorStart/Run), so the matching forget MUST
        // also run under the portal owner — otherwise the forget lands on the
        // transaction owner and the pin is double-released by the abort-path
        // ResourceOwnerRelease of the portal owner (rd_refcnt underflows, and
        // the next CREATE INDEX/DROP sees a stale "in use" refcount). We
        // therefore drop the QueryDesc under the portal's resource owner on
        // BOTH paths; the PORTAL_FAILED path skips ExecutorFinish/End (matching
        // C's "skip during error abort"), running only the implicit Drop.
        let resowner = portal.borrow().resowner.clone();
        let is_failed = portal.borrow().status == PORTAL_FAILED;
        let mut query_desc = Some(query_desc);
        resowner::with_current_resource_owner::call(resowner, &mut || {
            let mut qd = query_desc.take().unwrap();
            if !is_failed {
                executor::executor_finish::call(&mut qd)?;
                executor::executor_end::call(&mut qd)?;
                executor::free_query_desc::call(qd)?;
            } else {
                // PORTAL_FAILED: do not run the executor again; just drop the
                // owned QueryDesc here so its relcache-pin forgets land on the
                // portal owner (now current). C leaks the pointer and relies on
                // the resowner abort to release the pins; in the owned model the
                // Drop releases them, but it must happen under this owner.
                drop(qd);
            }
            Ok(())
        })?;
    }

    Ok(())
}

// ===========================================================================
// PersistHoldablePortal — portalcmds.c lines 317-506
// ===========================================================================

/// `PersistHoldablePortal` — prepare the specified Portal for access outside of
/// the current transaction.  When this function returns, all future accesses to
/// the portal must be done via the Tuplestore (not by invoking the executor).
pub fn PersistHoldablePortal(portal: Portal) -> PgResult<()> {
    // If we're preserving a holdable portal, we had better be inside the
    // transaction that originally created it.
    debug_assert_ne!(portal.borrow().createSubid, 0);
    debug_assert!(portal.borrow().queryDesc.is_some());

    // Caller must have created the tuplestore already ... but not a snapshot.
    debug_assert!(portal.borrow().holdContext.is_some());
    debug_assert!(portal.borrow().holdStore.is_some());
    debug_assert!(portal.borrow().holdSnapshot.is_none());

    // Before closing down the executor, we must copy the tupdesc into long-term
    // memory, since it was created in executor memory.
    //   oldcxt = MemoryContextSwitchTo(portal->holdContext);
    //   portal->tupDesc = CreateTupleDescCopy(portal->tupDesc);
    //   MemoryContextSwitchTo(oldcxt);
    //
    // The copy targets `portal->holdContext`, owned by portalmem; it does the
    // `CreateTupleDescCopy` into that context and stores the result back on the
    // portal (the lifetime of the copy is the portal's hold context).
    portalmem::copy_tup_desc_into_hold_context::call(&portal)?;

    // Check for improper portal use, and mark portal active.
    portalmem::mark_portal_active::call(&portal)?;

    // C saves ActivePortal/CurrentResourceOwner/PortalContext, sets them for
    // the PG_TRY block, and restores them on both the success and the PG_CATCH
    // paths. Those save/set/restore-global idioms are scoped callbacks here
    // (ActivePortal+PortalContext via portalmem; CurrentResourceOwner via
    // resowner). The callback bodies run the executor; an error from the body
    // is the PG_CATCH path.
    //
    //   PG_TRY { ActivePortal=portal; CurrentResourceOwner=portal->resowner;
    //            PortalContext=portal->portalContext; ... }
    //   PG_CATCH { MarkPortalFailed(portal); restore; PG_RE_THROW(); }
    let resowner = portal.borrow().resowner.clone();
    let try_result = portalmem::with_portal_globals::call(&portal, &mut || {
        resowner::with_current_resource_owner::call(resowner.clone(), &mut || {
            persist_holdable_portal_try(&portal)
        })
    });

    if let Err(err) = try_result {
        // Uncaught error while executing portal — mark it dead. (Globals are
        // already restored by the scoped callbacks unwinding.)
        portalmem::mark_portal_failed::call(&portal)?;
        // PG_RE_THROW();
        return Err(err);
    }

    // Mark portal not active.
    portal.borrow_mut().status = PORTAL_READY;

    snapmgr::pop_active_snapshot::call()?;

    // We can now release any subsidiary memory of the portal's context; we'll
    // never use it again.
    portalmem::memory_context_delete_children::call(&portal)?;

    Ok(())
}

/// The body of `PersistHoldablePortal`'s `PG_TRY()` block (portalcmds.c lines
/// 369-473), run with the portal's ActivePortal/PortalContext/resource owner
/// installed by the enclosing scoped callbacks. Returning `Err` is the
/// `PG_CATCH` path.
fn persist_holdable_portal_try(portal: &Portal) -> PgResult<()> {
    // ScanDirection direction = ForwardScanDirection;
    let mut direction = types_scan::sdir::ScanDirection::ForwardScanDirection;

    // PushActiveSnapshot(queryDesc->snapshot);
    let snapshot: Rc<SnapshotData> = portal
        .borrow()
        .queryDesc
        .as_ref()
        .unwrap()
        .snapshot
        .clone()
        .expect("PersistHoldablePortal: queryDesc->snapshot is non-NULL in C");
    snapmgr::push_active_snapshot::call(snapshot)?;

    // If the portal is marked scrollable, store the entire result set in the
    // tuplestore so subsequent backward FETCHs can be processed.  Otherwise
    // store only the not-yet-fetched rows.
    if (portal.borrow().cursorOptions & CURSOR_OPT_SCROLL) != 0 {
        let mut p = portal.borrow_mut();
        let qd = p.queryDesc.as_mut().unwrap();
        executor::executor_rewind::call(qd)?;
    } else if portal.borrow().atEnd {
        // If we already reached end-of-query, set the direction to NoMovement
        // to avoid trying to fetch any tuples.  We'll still set up an empty
        // tuplestore, though, to keep this from being a special case later.
        direction = types_scan::sdir::ScanDirection::NoMovementScanDirection;
    }

    // Change the destination to output to the tuplestore.  We tell the
    // tuplestore receiver to detoast all data passed through it; this makes it
    // safe to not keep a snapshot associated with the data.
    //   queryDesc->dest = CreateDestReceiver(DestTuplestore);
    //   SetTuplestoreDestReceiverParams(queryDesc->dest, portal->holdStore,
    //                                   portal->holdContext, true, NULL, NULL);
    let dest = tstore::create_dest_receiver_tuplestore::call()?;
    portal.borrow_mut().queryDesc.as_mut().unwrap().dest = dest;
    tstore::set_tuplestore_dest_receiver_params::call(dest, portal, true)?;

    // Fetch the result set into the tuplestore.
    //
    // C drives the executor through `portal->queryDesc` (a raw pointer), so the
    // portal struct is never borrow-locked while `ExecutorRun` runs. In the
    // owned model the executor re-enters the portal subsystem on this thread:
    // the tuplestore DestReceiver's per-row callback does `portal.borrow_mut()`
    // to append to `portal->holdStore`. Holding a `borrow_mut()` across the run
    // would make that a double-borrow panic (a panic during the executor ->
    // backend kill). So move `queryDesc` out for the duration of the call and
    // restore it afterwards; a Drop guard restores it even if the run unwinds
    // (the moved field must always be put back, exactly as C's raw pointer is
    // always valid). This mirrors pquery's `with_portal_query_desc`.
    {
        struct Restore<'p> {
            portal: &'p Portal,
            qd: Option<::nodes::querydesc::QueryDesc>,
        }
        impl Drop for Restore<'_> {
            fn drop(&mut self) {
                if let Some(qd) = self.qd.take() {
                    self.portal.borrow_mut().queryDesc = Some(qd);
                }
            }
        }
        let taken = portal
            .borrow_mut()
            .queryDesc
            .take()
            .expect("persist_holdable_portal_try: queryDesc is NULL while executor is active");
        let mut guard = Restore {
            portal,
            qd: Some(taken),
        };
        executor::executor_run::call(guard.qd.as_mut().unwrap(), direction, 0)?;
    }

    //   queryDesc->dest->rDestroy(queryDesc->dest);  queryDesc->dest = NULL;
    let dest = portal.borrow().queryDesc.as_ref().unwrap().dest;
    if dest != DestReceiverHandle::NULL {
        tstore::dest_destroy::call(dest)?;
    }
    portal.borrow_mut().queryDesc.as_mut().unwrap().dest = DestReceiverHandle::NULL;

    // Now shut down the inner executor.
    //   portal->queryDesc = NULL;   /* prevent double shutdown */
    let mut query_desc = portal.borrow_mut().queryDesc.take().unwrap();
    executor::executor_finish::call(&mut query_desc)?;
    executor::executor_end::call(&mut query_desc)?;
    executor::free_query_desc::call(query_desc)?;

    // Set the position in the result set.  MemoryContextSwitchTo(holdContext)
    // (no ambient context here; the tuplestore ops act on portal->holdStore).
    let at_end = portal.borrow().atEnd;
    if at_end {
        // Just force the tuplestore forward to its end.  The size of the skip
        // request here is arbitrary.
        let mut p = portal.borrow_mut();
        let store = p.holdStore.as_mut().unwrap();
        while sortstore::tuplestore_skiptuples::call(store, 1_000_000, true)? {
            /* continue */
        }
    } else {
        {
            let mut p = portal.borrow_mut();
            let store = p.holdStore.as_mut().unwrap();
            sortstore::tuplestore_rescan::call(store)?;
        }

        // In the no-scroll case, the start of the tuplestore is exactly where
        // we want to be, so no repositioning is wanted.
        if (portal.borrow().cursorOptions & CURSOR_OPT_SCROLL) != 0 {
            let portal_pos = portal.borrow().portalPos as i64;
            let mut p = portal.borrow_mut();
            let store = p.holdStore.as_mut().unwrap();
            if !sortstore::tuplestore_skiptuples::call(store, portal_pos, true)? {
                return Err(PgError::new(ERROR, "unexpected end of tuple stream"));
            }
        }
    }

    Ok(())
}
