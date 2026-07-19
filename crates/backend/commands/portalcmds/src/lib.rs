// portalcmds.c — SQL cursor commands (DECLARE/FETCH/MOVE/CLOSE) + the
// standard portal cleanup hook.
#![allow(non_snake_case)]

use ::elog::ereport;
use ::mcx::{Mcx, MemoryContext, PgBox};
use ::types_error::{
    PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_CURSOR_NAME,
    ERRCODE_UNDEFINED_CURSOR, ERROR,
};
use ::types_nodes::nodes_enums::CmdType;
use ::types_nodes::parsenodes::{DeclareCursorStmt, FetchStmt, Query};
use ::types_nodes::plannodes::PlannedStmt;
use ::types_dest::CommandDest;
use ::types_portal::{
    CachedPlanHandle, ParamListHandle, Portal, QueryCompletion, QueryDescHandle, CMDTAG_FETCH,
    CMDTAG_MOVE, CMDTAG_SELECT, CURSOR_OPT_HOLD, CURSOR_OPT_NO_SCROLL, CURSOR_OPT_SCROLL,
    PORTAL_FAILED, PORTAL_ONE_SELECT, PORTAL_READY,
};
use ::types_scan::sdir::{ForwardScanDirection, NoMovementScanDirection};

use ::tcop_dest::DestReceiver;

#[cfg(test)]
mod tests;

pub fn init_seams() {
    portalcmds_seams::portal_cleanup::set(PortalCleanup);
    portalcmds_seams::persist_holdable_portal::set(PersistHoldablePortal);
}

pub fn PerformCursorOpen(
    _mcx: Mcx<'_>,
    cstmt: &DeclareCursorStmt<'_>,
    stmt_text: &str,
    source_text: &str,
    params: ParamListHandle,
    is_top_level: bool,
) -> PgResult<()> {
    let name = match cstmt.portalname {
        Some(n) if !n.is_empty() => n,
        _ => return Err(empty_cursor_name()),
    };

    if cstmt.options & CURSOR_OPT_HOLD == 0 {
        xact::RequireTransactionBlock(is_top_level, "DECLARE CURSOR")?;
    } else if miscinit::InSecurityRestrictedOperation() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("cannot create a cursor WITH HOLD within security-restricted operation")
            .into_error()
            .into());
    }

    // C copies the finished plan into portalContext (portalcmds.c:109); node
    // deep-copy is unported, so the plan is DERIVED inside a portal-owned
    // arena instead: re-parse this DECLARE's own statement text and run
    // analyze/rewrite/plan with the arena's Mcx. Identical text under the
    // same snapshot yields the identical plan; the analysis re-run is the
    // once-per-DECLARE cost of the missing copyObject. C's error order is
    // preserved (rewrite/plan errors fire before CreatePortal's 42P03).
    // The re-analysis divergence needs the param TYPES for $n: take them from
    // the live outer param list — they are the types the outer analysis
    // resolved (C skips this: it receives the analyzed query and only copies
    // the VALUES, below). Identical text + identical types + same snapshot
    // yields the identical query.
    let param_types: Vec<_> = if params.is_null() {
        Vec::new()
    } else {
        types_portal::params::with(params, |src| src.iter().map(|p| p.ptype).collect())
    };

    let plan_ctx = Box::new(MemoryContext::new_bump("PortalPlanContext"));
    // SAFETY: the Box gives the context a stable address; PortalDrop reclaims
    // it only after the stmts registry handle below is released.
    let pctx: &'static MemoryContext = unsafe { &*(&*plan_ctx as *const MemoryContext) };
    let pmcx = pctx.mcx();

    let raw = postgres::pg_parse_query(pmcx, stmt_text)?;
    assert!(raw.len() == 1, "DECLARE statement slice re-parsed to {} statements", raw.len());
    let queries = postgres::pg_analyze_and_rewrite_fixedparams(
        pmcx,
        &raw[0],
        stmt_text,
        &param_types,
        types_portal::QueryEnvHandle::NULL,
    )?;
    assert!(queries.len() == 1, "DECLARE analysis yielded {} queries", queries.len());
    let util = queries.into_iter().next().expect("len == 1");
    let cstmt_node = util
        .utilityStmt
        .filter(|n| n.node_tag() == types_nodes::NodeTag::T_DeclareCursorStmt)
        .expect("re-parsed DECLARE slice is a DeclareCursorStmt");
    // SAFETY: the re-parsed tree is single-owner here; the Query is consumed
    // exactly as C's QueryRewrite consumes its argument.
    let query_node = unsafe {
        cstmt_node.with_mut::<DeclareCursorStmt, _>(|d| d.query.take())
    }
    .flatten()
    .ok_or_else(non_select_in_declare)?;
    // SAFETY: as above; no derived refs are live.
    let mut query = unsafe { query_node.with_mut::<Query, _>(core::mem::take) }
        .ok_or_else(non_select_in_declare)?;

    // C jumbles the DECLARE's contained query at entry; the re-parsed tree is
    // identical, so the queryId matches. post_parse_analyze_hook: no plugin
    // surface exists.
    if queryjumble::IsQueryIdEnabled() {
        queryjumble::JumbleQueryDiscard(pmcx, &mut query)?;
    }

    let rewritten = rewrite_handler_seams::query_rewrite::call(pmcx, query)?;
    if rewritten.len() != 1 {
        return Err(non_select_in_declare());
    }
    let query = rewritten.into_iter().next().expect("len == 1");
    if query.commandType != CmdType::CMD_SELECT {
        return Err(non_select_in_declare());
    }

    let plan = postgres::pg_plan_query(pmcx, mcx::leak_in(mcx::alloc_in(pmcx, query)?), source_text, cstmt.options, params)?
        .expect("planner output for a SELECT");

    let portal = portalmem::CreatePortal(name, false, false)?;

    let plan: &'static PlannedStmt<'static> = ::mcx::leak_in(PgBox::new_in(plan, pmcx));
    // SAFETY: `plan` lives in plan_ctx, which the portal owns until PortalDrop
    // (which releases this handle first).
    let stmts = unsafe { pquery::stmt_list::register(core::slice::from_ref(plan)) };

    if let Err(e) = portalmem::PortalDefineQuery(
        &portal,
        None,
        source_text,
        CMDTAG_SELECT,
        stmts,
        CachedPlanHandle::NULL,
    ) {
        pquery::stmt_list::free(stmts);
        return Err(e);
    }

    portalmem::PortalAttachPlanContext(&portal, plan_ctx);

    // C: params = copyParamList(params) into portalContext (portalcmds.c):
    // the copy (values in plan_ctx) outlives the outer execution that owns
    // the incoming list — FETCH runs after the declaring call returned.
    let params = if params.is_null() {
        params
    } else {
        let copied =
            types_portal::params::with(params, |src| nodes_params::copy_param_list(pmcx, src))?;
        let copied: &'static [types_portal::params::ParamExternData] = copied.leak();
        // SAFETY: the slice and its by-ref datums live in plan_ctx, which the
        // portal owns until PortalDrop; PortalDrop frees the handle first.
        let h = unsafe { types_portal::params::register(copied) };
        // Stored now so an error before PortalStart reaches PortalDrop's
        // registry cleanup (extended_query precedent).
        portal.borrow_mut().portalParams = h;
        h
    };

    {
        let mut p = portal.borrow_mut();
        p.cursorOptions = cstmt.options;
        if p.cursorOptions & (CURSOR_OPT_SCROLL | CURSOR_OPT_NO_SCROLL) == 0 {
            // C's default-scrollability probe (portalcmds.c PerformCursorOpen).
            // A POLICY oracle since the backward-execution wave (B10): it
            // decides which cursors accept FETCH BACKWARD by default (C
            // parity); the reads themselves are store-served.
            if plan.rowMarks.is_nil()
                && execmain::plan_implicit_scroll_ok(plan.planTree)
            {
                p.cursorOptions |= CURSOR_OPT_SCROLL;
            } else {
                p.cursorOptions |= CURSOR_OPT_NO_SCROLL;
            }
        }
    }

    pquery::PortalStart(&portal, params, 0, Some(snapmgr::GetActiveSnapshot()))?;

    debug_assert_eq!(portal.borrow().strategy, PORTAL_ONE_SELECT);

    Ok(())
}

pub fn PerformPortalFetch(
    stmt: &FetchStmt<'_>,
    dest: &mut DestReceiver<'_>,
    qc: Option<&mut QueryCompletion>,
) -> PgResult<()> {
    let name = match stmt.portalname {
        Some(n) if !n.is_empty() => n,
        _ => return Err(empty_cursor_name()),
    };

    let Some(portal) = portalmem::GetPortalByName(Some(name)) else {
        return Err(undefined_cursor(name));
    };

    // C: MOVE swaps dest for None_Receiver.
    let nprocessed = if stmt.ismove {
        let mut none = DestReceiver::DoNothing;
        pquery::PortalRunFetch(&portal, stmt.direction, stmt.howMany, &mut none)?
    } else {
        pquery::PortalRunFetch(&portal, stmt.direction, stmt.howMany, dest)?
    };

    if let Some(qc) = qc {
        qc.commandTag = if stmt.ismove { CMDTAG_MOVE } else { CMDTAG_FETCH };
        qc.nprocessed = nprocessed;
    }
    Ok(())
}

pub fn PerformPortalClose(name: Option<&str>) -> PgResult<()> {
    // NULL means CLOSE ALL.
    let Some(name) = name else {
        return portalmem::PortalHashTableDeleteAll();
    };

    if name.is_empty() {
        return Err(empty_cursor_name());
    }

    let Some(portal) = portalmem::GetPortalByName(Some(name)) else {
        return Err(undefined_cursor(name));
    };

    // PortalDrop runs PortalCleanup and releases the stmts registry handle.
    portalmem::PortalDrop(&portal, false)
}

pub fn PortalCleanup(portal: &Portal<'static>) -> PgResult<()> {
    let (query_desc, failed) = {
        let mut p = portal.borrow_mut();
        // Reset queryDesc first so an error below cannot shut down twice.
        (
            core::mem::replace(&mut p.queryDesc, QueryDescHandle::NULL),
            p.status == PORTAL_FAILED,
        )
    };
    if query_desc.is_null() {
        return Ok(());
    }
    // Both arms need CurrentResourceOwner = portal->resowner (portalcmds.c:279):
    // ExecutorEnd unregisters es_snapshot from it; the failed arm's guard drops
    // (pins, relation closers) forget from it, not the abort-time owner.
    let save_owner = resowner_seams::current_resource_owner::call();
    let portal_owner = portal.borrow().resowner;
    if !portal_owner.is_null() {
        resowner_seams::set_current_resource_owner::call(portal_owner);
    }
    let result = if failed {
        // C leaves the QueryDesc to die with the abort cleanup; the registry
        // entry is owning, so release it here (execmain audit E-4 precedent).
        execmain_seams::release_query_desc::call(query_desc);
        Ok(())
    } else {
        (|| -> PgResult<()> {
            execmain_seams::executor_finish::call(query_desc)?;
            execmain_seams::executor_end::call(query_desc)?;
            execmain_seams::free_query_desc::call(query_desc);
            Ok(())
        })()
    };
    resowner_seams::set_current_resource_owner::call(save_owner);
    result
}

pub fn PersistHoldablePortal(portal: &Portal<'static>) -> PgResult<()> {
    let query_desc = portal.borrow().queryDesc;
    assert!(!query_desc.is_null(), "PersistHoldablePortal: portal has no queryDesc");
    debug_assert!(!portal.borrow().holdStore.is_null());
    debug_assert!(portal.borrow().holdSnapshot.is_none());
    // C copies tupDesc into holdContext before ExecutorEnd; the portal's Rc
    // keeps the desc alive past FreeQueryDesc here.

    portalmem::MarkPortalActive(portal)?;

    pquery::run_protected(portal, false, || -> PgResult<()> {
        let snap = execmain_seams::query_desc_snapshot::call(query_desc)
            .expect("queryDesc->snapshot set while executor is active");
        snapmgr::PushActiveSnapshot(&snap)?;

        let scroll = portal.borrow().cursorOptions & CURSOR_OPT_SCROLL != 0;
        // WS-CA wave-10 (contract §2.4 arm 1): a store-armed SCROLL+HOLD
        // portal's store already IS the holdStore — detoasted, interXact,
        // holdContext-resident since first fill. Persist = resume the fill
        // from the high-water mark to EOF; NEVER rewind (§5 D2: the fetched
        // prefix is kept where C re-executes from scratch — same bytes for
        // stable queries by snapshot identity). Teardown + repositioning
        // below are shared with the C-shape arms verbatim.
        let hold_store = portal.borrow().holdStore;
        if portal.borrow().cursorStoreArmed {
            debug_assert!(scroll);
            pquery::fill_portal_store_to(portal, 0)?;
            // Auto-held portals (plpgsql pin + intra-procedure COMMIT, the
            // HoldPinnedPortals class): the filled store is the
            // transaction-scoped cursorStore — copy it into the fresh
            // holdStore (detoasting) and drop it. No-op when the store
            // already IS the holdStore (DECLARE'd WITH HOLD).
            pquery::cursor_store_persist_into_hold(portal)?;
        } else {
            // SCROLL stores the whole result (rewind first); no-scroll stores
            // only the not-yet-fetched rows, and NoMovement if already at end
            // (not all plan nodes tolerate another fetch after returning
            // NULL).
            let direction = if scroll {
                execmain_seams::executor_rewind::call(query_desc)?;
                ForwardScanDirection
            } else if portal.borrow().atEnd {
                NoMovementScanDirection
            } else {
                ForwardScanDirection
            };

            // detoast=true: the stored rows must not depend on the snapshot.
            let mut treceiver = tcop_dest::CreateDestReceiver(CommandDest::Tuplestore);
            tcop_dest::SetTuplestoreDestReceiverParams(&mut treceiver, hold_store, true);
            execmain_seams::executor_run::call(query_desc, direction, 0, &mut treceiver)?;
            treceiver.destroy();
        }

        portal.borrow_mut().queryDesc = QueryDescHandle::NULL;
        let mut qd_owner = pquery::QueryDescOwner(query_desc);
        execmain_seams::executor_finish::call(query_desc)?;
        execmain_seams::executor_end::call(query_desc)?;
        qd_owner.disarm();
        execmain_seams::free_query_desc::call(query_desc);

        let (at_end, portal_pos) = {
            let p = portal.borrow();
            (p.atEnd, p.portalPos)
        };
        if at_end {
            while tuplestore_hold_seams::tuplestore_skiptuples::call(hold_store, 1_000_000, true)? {}
        } else {
            tuplestore_hold_seams::tuplestore_rescan::call(hold_store)?;
            // No-scroll: the store starts at the not-yet-fetched rows already.
            if scroll
                && !tuplestore_hold_seams::tuplestore_skiptuples::call(
                    hold_store,
                    portal_pos as i64,
                    true,
                )?
            {
                return Err(ereport(ERROR)
                    .errmsg_internal("unexpected end of tuple stream")
                    .into_error()
                    .into());
            }
        }
        Ok(())
    })?;

    portal.borrow_mut().status = PORTAL_READY;

    snapmgr::PopActiveSnapshot()?;

    // C: MemoryContextDeleteChildren(portal->portalContext) — the plan arena
    // (planContext) is still referenced by the stmts registry handle, so it is
    // retained until PortalDrop.
    Ok(())
}

#[cold]
#[inline(never)]
fn empty_cursor_name() -> Box<types_error::PgError> {
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_INVALID_CURSOR_NAME)
            .errmsg("invalid cursor name: must not be empty")
            .into_error(),
    )
}

#[cold]
#[inline(never)]
fn undefined_cursor(name: &str) -> Box<types_error::PgError> {
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_CURSOR)
            .errmsg(format!("cursor \"{name}\" does not exist"))
            .into_error(),
    )
}

#[cold]
#[inline(never)]
fn non_select_in_declare() -> Box<types_error::PgError> {
    Box::new(
        ereport(ERROR)
            .errmsg_internal("non-SELECT statement in DECLARE CURSOR")
            .into_error(),
    )
}
