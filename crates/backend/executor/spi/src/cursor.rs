// SPI cursor surface (spi.c): SPI_cursor_open_with_paramlist / _fetch /
// _close — the plpgsql FOR-IN-query lane. Unspecified scrollability maps to
// NO_SCROLL (C consults ExecSupportsBackwardScan; forward-only callers are
// unaffected). The portal keeps the CachedPlan refcount even for unsaved
// plans (C copies the stmt list into the portal context instead).
use datum::Datum;
use tcop_dest::CreateDestReceiver;
use types_dest::CommandDest;
use types_error::{PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_CURSOR_DEFINITION};
use types_portal::{
    FetchDirection, ParamListHandle, Portal, QueryEnvHandle, StmtListHandle,
    CURSOR_OPT_NO_SCROLL, CURSOR_OPT_SCROLL,
};

use elog::ereport;

use crate::execute::convert_params;
use crate::plan::{single_source, SpiPlanPtr};
use crate::{
    set_spi_processed, set_spi_tuptable, with_current, TuptabHandle, _SPI_begin_call,
    _SPI_end_call,
};

pub struct SpiCursor {
    pub portal: Portal<'static>,
    stmts: StmtListHandle,
    params: ParamListHandle,
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

        let params = convert_params(&state.argtypes, values, nulls)?;

        let query_string = plancache::CachedPlanQueryString(psrc);
        let cplan = plancache::GetCachedPlan(psrc, params, None, QueryEnvHandle::NULL)?;
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
                p.cursorOptions |= CURSOR_OPT_NO_SCROLL;
            }
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

        Ok(SpiCursor { portal, stmts, params })
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
    if !cursor.params.is_null() {
        types_portal::params::free(cursor.params);
    }
    Ok(())
}
