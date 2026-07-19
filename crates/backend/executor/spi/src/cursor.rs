// SPI cursor surface (spi.c): SPI_cursor_open_with_paramlist / _fetch /
// _close — the plpgsql FOR-IN-query lane. The portal keeps the CachedPlan
// refcount even for unsaved plans (C copies the stmt list into the portal
// context instead).
use datum::Datum;
use tcop_dest::CreateDestReceiver;
use types_dest::CommandDest;
use types_error::{PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_CURSOR_DEFINITION};
use types_portal::params::{ParamExternData, PARAM_FLAG_CONST};
use types_portal::{
    FetchDirection, ParamListHandle, Portal, QueryEnvHandle, StmtListHandle,
    CURSOR_OPT_NO_SCROLL, CURSOR_OPT_SCROLL,
};

use elog::ereport;

use crate::plan::{single_source, SpiPlanPtr};
use crate::{
    set_spi_processed, set_spi_tuptable, with_current, TuptabHandle, _SPI_begin_call,
    _SPI_end_call,
};

pub struct SpiCursor {
    pub portal: Portal<'static>,
    stmts: StmtListHandle,
}

impl SpiCursor {
    // Re-wrap an already-open portal (the portal owns its stmts handle;
    // PortalDrop frees it).
    pub fn from_portal(portal: Portal<'static>) -> SpiCursor {
        SpiCursor { portal, stmts: StmtListHandle::NULL }
    }
}

// C SPI_cursor_open_internal's copyParamList into portal->portalContext: the
// param array must survive _SPI_end_call's exec-context reset, so it gets
// portal lifetime. Storing the handle in portalParams immediately makes
// PortalDrop (close or abort-time cleanup) the single free path.
fn cursor_params(
    portal: &Portal<'static>,
    argtypes: &[types_core::Oid],
    values: &[Datum],
    nulls: &[bool],
) -> PgResult<ParamListHandle> {
    if argtypes.is_empty() {
        return Ok(ParamListHandle::NULL);
    }
    // SAFETY: portalContext is PgBox'd for address stability and outlives this
    // call (freed only in PortalDrop, after release_portal_registry_handles).
    let ctx: &mcx::MemoryContext = unsafe {
        let p = portal.borrow();
        &*(&**p.portalContext.as_ref().expect("portal has portalContext")
            as *const mcx::MemoryContext)
    };
    let mcx = ctx.mcx();
    let mut v = mcx::vec_with_capacity_in(mcx, argtypes.len())?;
    for i in 0..argtypes.len() {
        v.push(ParamExternData {
            value: values[i],
            isnull: nulls[i],
            pflags: PARAM_FLAG_CONST,
            ptype: argtypes[i],
        });
    }
    let slice = mcx::vec_borrow_in(mcx, v)?;
    // SAFETY: slice lives in portalContext, which PortalDrop deletes only
    // after release_portal_registry_handles frees the handle.
    let params = unsafe { types_portal::params::register(slice) };
    portal.borrow_mut().portalParams = params;
    Ok(params)
}

// SPI_cursor_open_internal (spi.c); the paramlist arrives as values/nulls
// against the plan's argtypes (C _SPI_convert_params shape).
pub fn SPI_cursor_open(
    name: Option<&str>,
    ptr: SpiPlanPtr,
    values: &[Datum],
    nulls: &[bool],
    read_only: bool,
) -> PgResult<SpiCursor> {
    let Some(state) = crate::plan::state_snapshot(ptr) else {
        panic!("SPI_cursor_open: invalid plan");
    };
    if single_source(ptr).is_none() {
        return Err(ereport(types_error::ERROR)
            .errcode(ERRCODE_INVALID_CURSOR_DEFINITION)
            .errmsg("cannot open multi-query plan as cursor")
            .into_error()
            .into());
    }
    let (psrc, cursor_options) = single_source(ptr).expect("checked");
    if plancache::CachedPlanResultDesc(psrc).is_none() {
        let tag = plancache::CachedPlanCommandTag(psrc);
        let cmdname = if tag == types_portal::CMDTAG_SELECT {
            "SELECT INTO"
        } else {
            cmdtag::GetCommandTagName(tag)
        };
        return Err(ereport(types_error::ERROR)
            .errcode(ERRCODE_INVALID_CURSOR_DEFINITION)
            .errmsg(format!("cannot open {cmdname} query as cursor"))
            .into_error()
            .into());
    }

    let res = _SPI_begin_call(true);
    if res < 0 {
        panic!("SPI_cursor_open called while not connected");
    }
    set_spi_processed(0);
    set_spi_tuptable(None);
    with_current(|c| {
        c.processed = 0;
        c.tuptable = None;
    });

    let result = (|| -> PgResult<SpiCursor> {
        let portal = match name {
            None | Some("") => portalmem::CreateNewPortal()?,
            Some(n) => portalmem::CreatePortal(n, false, false)?,
        };

        let params = cursor_params(&portal, &state.argtypes, values, nulls)?;

        let query_string = plancache::CachedPlanQueryString(psrc);
        let cplan = plancache::GetCachedPlan(psrc, params, None, crate::current_query_env())?;
        let stmt_slice = plancache::CachedPlanStmtList(cplan);
        // SAFETY: the cplan refcount taken by GetCachedPlan pins stmt_slice
        // until PortalDrop releases it (prepare.c precedent).
        let stmts = unsafe { pquery::stmt_list::register(stmt_slice) };
        portalmem::PortalDefineQuery(
            &portal,
            None,
            query_string,
            plancache::CachedPlanCommandTag(psrc),
            stmts,
            cplan,
        )?;

        {
            let mut p = portal.borrow_mut();
            p.cursorOptions = cursor_options;
            if p.cursorOptions & (CURSOR_OPT_SCROLL | CURSOR_OPT_NO_SCROLL) == 0 {
                // C's default-scrollability probe (spi.c SPI_cursor_open_internal).
                // The probe is a POLICY oracle since the backward-execution
                // wave (B10): it decides which cursors accept FETCH BACKWARD
                // by default (C parity); the reads themselves are store-served.
                if stmt_slice.len() == 1
                    && stmt_slice[0].commandType != types_nodes::nodes_enums::CmdType::CMD_UTILITY
                    && stmt_slice[0].rowMarks.is_nil()
                    && execmain::plan_implicit_scroll_ok(stmt_slice[0].planTree)
                {
                    p.cursorOptions |= CURSOR_OPT_SCROLL;
                } else {
                    p.cursorOptions |= CURSOR_OPT_NO_SCROLL;
                }
            }
            p.queryEnv = crate::current_query_env();
        }

        if read_only {
            for stmt in stmt_slice {
                if !utility::CommandIsReadOnly(stmt) {
                    let name = cmdtag::GetCommandTagName(crate::execute::command_tag_of(stmt));
                    return Err(ereport(types_error::ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg(format!("{name} is not allowed in a non-volatile function"))
                        .into_error()
                        .into());
                }
            }
        }

        let snapshot = if read_only {
            snapmgr::GetActiveSnapshot()
        } else {
            xact::CommandCounterIncrement()?;
            snapmgr::GetTransactionSnapshot()?
        };

        pquery::PortalStart(&portal, params, 0, Some(snapshot))?;

        Ok(SpiCursor { portal, stmts })
    })();

    _SPI_end_call(true);
    result
}

// _SPI_cursor_operation + SPI_cursor_fetch (spi.c).
pub fn SPI_cursor_fetch(cursor: &SpiCursor, forward: bool, count: i64) -> PgResult<()> {
    let res = _SPI_begin_call(true);
    if res < 0 {
        panic!("SPI cursor operation called while not connected");
    }
    set_spi_processed(0);
    set_spi_tuptable(None);
    with_current(|c| {
        c.processed = 0;
        c.tuptable = None;
    });

    let result = (|| -> PgResult<()> {
        let mut dest = CreateDestReceiver(CommandDest::Spi);
        let direction =
            if forward { FetchDirection::FETCH_FORWARD } else { FetchDirection::FETCH_BACKWARD };
        let nfetched = pquery::PortalRunFetch(&cursor.portal, direction, count, &mut dest)?;
        with_current(|c| c.processed = nfetched);
        let (processed, tuptable) =
            with_current(|c| (c.processed, c.tuptable.take())).expect("connected");
        set_spi_processed(processed);
        set_spi_tuptable(tuptable.map(TuptabHandle));
        Ok(())
    })();

    _SPI_end_call(true);
    result
}

// SPI_cursor_close (spi.c).
pub fn SPI_cursor_close(cursor: SpiCursor) -> PgResult<()> {
    portalmem::PortalDrop(&cursor.portal, false)?;
    pquery::stmt_list::free(cursor.stmts);
    Ok(())
}

// SPI_cursor_find (spi.c): portal lookup by name.
pub fn SPI_cursor_find_portal(name: &str) -> Option<Portal<'static>> {
    portalmem::GetPortalByName(Some(name))
}

// SPI_cursor_close by portal (PortalDrop frees the portal-held stmts handle).
pub fn SPI_cursor_close_portal(portal: &Portal<'static>) -> PgResult<()> {
    portalmem::PortalDrop(portal, false)
}

fn cursor_operation(
    portal: &Portal<'static>,
    direction: FetchDirection,
    count: i64,
    fetch: bool,
) -> PgResult<()> {
    let res = _SPI_begin_call(true);
    if res < 0 {
        panic!("SPI cursor operation called while not connected");
    }
    set_spi_processed(0);
    set_spi_tuptable(None);
    with_current(|c| {
        c.processed = 0;
        c.tuptable = None;
    });

    let result = (|| -> PgResult<()> {
        let nfetched = if fetch {
            let mut dest = CreateDestReceiver(CommandDest::Spi);
            pquery::PortalRunFetch(portal, direction, count, &mut dest)?
        } else {
            let mut dest = CreateDestReceiver(CommandDest::None);
            pquery::PortalRunFetch(portal, direction, count, &mut dest)?
        };
        with_current(|c| c.processed = nfetched);
        let (processed, tuptable) =
            with_current(|c| (c.processed, c.tuptable.take())).expect("connected");
        set_spi_processed(processed);
        set_spi_tuptable(tuptable.map(TuptabHandle));
        Ok(())
    })();

    _SPI_end_call(true);
    result
}

// SPI_scroll_cursor_fetch (spi.c).
pub fn SPI_scroll_cursor_fetch(
    portal: &Portal<'static>,
    direction: FetchDirection,
    count: i64,
) -> PgResult<()> {
    cursor_operation(portal, direction, count, true)
}

// SPI_scroll_cursor_move (spi.c).
pub fn SPI_scroll_cursor_move(
    portal: &Portal<'static>,
    direction: FetchDirection,
    count: i64,
) -> PgResult<()> {
    cursor_operation(portal, direction, count, false)
}

// SPI_cursor_parse_open (spi.c): one-shot parse/plan of `src`, then the
// portal-open tail; the SpiPlan is freed after the portal pins the cplan.
pub fn SPI_cursor_open_extended(
    name: Option<&str>,
    src: &str,
    argtypes: &[types_core::Oid],
    values: &[Datum],
    nulls: &[bool],
    read_only: bool,
    cursor_options: i32,
) -> PgResult<SpiCursor> {
    let plan = crate::plan::SPI_prepare_cursor(src, argtypes, cursor_options)?;
    let result = SPI_cursor_open(name, plan, values, nulls, read_only);
    crate::plan::SPI_freeplan(plan);
    result
}
