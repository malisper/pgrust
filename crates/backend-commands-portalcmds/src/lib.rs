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

use types_nodes::nodes::CMD_SELECT;
use types_nodes::portalcmds::{
    DeclareCursorStmt, FetchStmt, ParamListInfo, ParseState, Query, CURSOR_OPT_HOLD,
    CURSOR_OPT_NO_SCROLL, CURSOR_OPT_SCROLL,
};
use types_portal::{
    CommandTag, DestReceiver, FetchDirection, Portal, QueryCompletion, CMDTAG_FETCH, CMDTAG_MOVE,
    PORTAL_FAILED, PORTAL_ONE_SELECT, PORTAL_READY,
};
use types_snapshot::SnapshotData;

use backend_access_transam_xact_seams as xact;
use backend_executor_execMain_seams as executor;
use backend_nodes_queryjumble_seams as queryjumble;
use backend_parser_analyze_seams as analyze;
use backend_rewrite_rewritehandler_seams as rewrite;
use backend_tcop_postgres_seams as postgres;
use backend_tcop_pquery_seams as pquery;
use backend_utils_init_miscinit_seams as miscinit;
use backend_utils_mmgr_portalmem_seams as portalmem;
use backend_utils_resowner_resowner_seams as resowner;
use backend_utils_sort_storage_seams as sortstore;
use backend_utils_time_snapmgr_seams as snapmgr;
use backend_executor_tstorereceiver_seams as tstore;
use alloc::rc::Rc;

extern crate alloc;

/// Install every seam this crate owns. Wired from `seams-init`.
pub fn init_seams() {
    backend_commands_portalcmds_seams::perform_cursor_open::set(perform_cursor_open_seam);
    backend_commands_portalcmds_seams::perform_portal_fetch::set(perform_portal_fetch_seam);
    backend_commands_portalcmds_seams::perform_portal_close::set(perform_portal_close_seam);
    backend_commands_portalcmds_seams::portal_cleanup::set(PortalCleanup);
    backend_commands_portalcmds_seams::persist_holdable_portal::set(PersistHoldablePortal);
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
    dest: DestReceiver,
    qc: Option<&mut QueryCompletion>,
) -> PgResult<()> {
    PerformPortalFetch(stmt, dest, qc)
}
fn perform_portal_close_seam(name: Option<&str>) -> PgResult<()> {
    PerformPortalClose(name)
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

/// `PerformPortalFetch` — execute SQL `FETCH` or `MOVE`.
pub fn PerformPortalFetch(
    stmt: &FetchStmt,
    mut dest: DestReceiver,
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
    if stmt.ismove {
        dest = DestReceiver::new(types_dest::CommandDest::None);
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
fn map_fetch_direction(d: types_nodes::portalcmds::FetchDirection) -> FetchDirection {
    use types_nodes::portalcmds::FetchDirection as N;
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
        if portal.borrow().status != PORTAL_FAILED {
            // We must make the portal's resource owner current (C: save
            // CurrentResourceOwner, set it to portal->resowner, restore after).
            // The save/restore-global idiom is a scoped callback here.
            let resowner = portal.borrow().resowner.clone();
            let mut query_desc = Some(query_desc);
            resowner::with_current_resource_owner::call(resowner, &mut || {
                let mut qd = query_desc.take().unwrap();
                executor::executor_finish::call(&mut qd)?;
                executor::executor_end::call(&mut qd)?;
                executor::free_query_desc::call(qd)?;
                Ok(())
            })?;
        }
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
    portal.borrow_mut().queryDesc.as_mut().unwrap().dest = Some(dest);
    tstore::set_tuplestore_dest_receiver_params::call(dest, portal, true)?;

    // Fetch the result set into the tuplestore.
    {
        let mut p = portal.borrow_mut();
        let qd = p.queryDesc.as_mut().unwrap();
        executor::executor_run::call(qd, direction, 0)?;
    }

    //   queryDesc->dest->rDestroy(queryDesc->dest);  queryDesc->dest = NULL;
    let dest = portal.borrow().queryDesc.as_ref().unwrap().dest;
    if let Some(dest) = dest {
        tstore::dest_destroy::call(dest)?;
    }
    portal.borrow_mut().queryDesc.as_mut().unwrap().dest = None;

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
