// portalcmds.c — SQL cursor commands (DECLARE/FETCH/MOVE/CLOSE) + the
// standard portal cleanup hook.
#![allow(non_snake_case)]

use ::elog::ereport;
use ::mcx::{Mcx, PgBox};
use ::types_error::{
    PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_CURSOR_NAME,
    ERRCODE_UNDEFINED_CURSOR, ERROR,
};
use ::types_nodes::nodes_enums::CmdType;
use ::types_nodes::parsenodes::{DeclareCursorStmt, FetchStmt, Query};
use ::types_nodes::plannodes::PlannedStmt;
use ::types_portal::{
    CachedPlanHandle, ParamListHandle, Portal, QueryCompletion, QueryDescHandle, CMDTAG_FETCH,
    CMDTAG_MOVE, CMDTAG_SELECT, CURSOR_OPT_HOLD, CURSOR_OPT_NO_SCROLL, CURSOR_OPT_SCROLL,
    PORTAL_FAILED, PORTAL_ONE_SELECT,
};

use ::tcop_dest::DestReceiver;

#[cfg(test)]
mod tests;

pub fn init_seams() {
    portalcmds_seams::portal_cleanup::set(PortalCleanup);
    portalcmds_seams::persist_holdable_portal::set(PersistHoldablePortal);
}

pub fn PerformCursorOpen<'mcx>(
    mcx: Mcx<'mcx>,
    cstmt: &DeclareCursorStmt<'mcx>,
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

    // C: JumbleQuery + post_parse_analyze_hook — compute_query_id is off at
    // boot and no hook surface exists.

    let query_node = cstmt.query.expect("DECLARE CURSOR has a query");
    // SAFETY: the statement tree is single-owner at utility dispatch and the
    // Query is consumed exactly as C's QueryRewrite consumes its argument; no
    // derived refs are live.
    let query = unsafe { query_node.with_mut::<Query, _>(core::mem::take) }
        .ok_or_else(non_select_in_declare)?;

    let rewritten = rewrite_handler_seams::query_rewrite::call(mcx, query)?;
    if rewritten.len() != 1 {
        return Err(non_select_in_declare());
    }
    let query = rewritten.into_iter().next().expect("len == 1");
    if query.commandType != CmdType::CMD_SELECT {
        return Err(non_select_in_declare());
    }

    let plan = postgres::pg_plan_query(mcx, query, source_text, cstmt.options, params)?
        .expect("planner output for a SELECT");

    let portal = portalmem::CreatePortal(name, false, false)?;

    // C copies the plan + query string into portalContext (portalcmds.c:109);
    // node clone_in is unported (plancache lane), so the plan stays in the
    // caller's arena under stmt_list::register's liveness contract — every
    // caller today (the DECLARE grammar is unported) holds the statement
    // arena for the portal's life; a violation is a stale-handle panic.
    let plan: &'mcx PlannedStmt<'mcx> = ::mcx::leak_in(PgBox::new_in(plan, mcx));
    // SAFETY: `plan` is arena-backed by `mcx`; see the retention note above.
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

    // C: params = copyParamList(params) into portalContext — a handle copy
    // (real param lists loud-panic upstream).

    {
        let mut p = portal.borrow_mut();
        p.cursorOptions = cstmt.options;
        if p.cursorOptions & (CURSOR_OPT_SCROLL | CURSOR_OPT_NO_SCROLL) == 0 {
            if plan.rowMarks.is_nil()
                && execmain::exec_supports_backward_scan(plan.planTree)
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

    // PortalCleanup runs as a side-effect, if not already done; PortalDrop
    // also releases the stmts registry handle (C frees it with portalContext).
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
    if failed {
        // C leaves the QueryDesc to die with the abort cleanup; the registry
        // entry is owning, so release it here (execmain audit E-4 precedent).
        execmain_seams::release_query_desc::call(query_desc);
        return Ok(());
    }
    // C: CurrentResourceOwner switched to portal->resowner around the
    // shutdown; resources are RAII-owned here.
    execmain_seams::executor_finish::call(query_desc)?;
    execmain_seams::executor_end::call(query_desc)?;
    execmain_seams::free_query_desc::call(query_desc);
    Ok(())
}

pub fn PersistHoldablePortal(_portal: &Portal<'static>) -> PgResult<()> {
    panic!(
        "PersistHoldablePortal (portalcmds.c): WITH HOLD persist-at-commit \
         unported — unit backend-commands-portalcmds holdable lane"
    );
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
