//! The SPI **cursor** surface (`spi.c`: `SPI_cursor_open_with_paramlist` /
//! `SPI_cursor_open_internal`, `SPI_cursor_find`, `SPI_cursor_fetch` /
//! `SPI_cursor_move` / `SPI_scroll_cursor_fetch` / `SPI_scroll_cursor_move`,
//! `SPI_cursor_close`, and the shared `_SPI_cursor_operation`).
//!
//! These open a real [`Portal`] (portalmem) wrapping a planned statement so the
//! caller can fetch incrementally rather than materializing every row up front.
//! The Portal outlives the SPI call — it lives in the portal memory context
//! (portalmem owns the `Rc<RefCell<PortalData>>`), owning its own
//! `QueryDesc`/`EState` across fetches.
//!
//! # Owned model
//!
//! C threads a `SPIPlanPtr` (a `_SPI_plan` holding a `CachedPlanSource` list).
//! The PL/pgSQL consumer here does not hold a long-lived `SpiPlanPtr`; instead
//! the open entry-point builds the cached-plan source from the embedded query
//! text + the PL/pgSQL parse state (the same prepare pipeline as
//! [`crate::execsql`]), `GetCachedPlan`s it, and hands the planned statements
//! into the portal. For the unsaved one-shot source the statements are copied
//! into the portal's own context (portalmem's `portal_define_query_list` does
//! the copyObject) and the source is dropped — the portal then depends only on
//! its own context, exactly like C's `!plan->saved` branch of
//! `SPI_cursor_open_internal`.

use mcx::{MemoryContext, Mcx};
use types_error::{PgError, PgResult, ERROR};
use ::nodes::nodes::CmdType;
use ::nodes::parsestmt::{CachedPlanHandle as SeamCachedPlanHandle, PlpgsqlExprParseState};
use ::parsenodes::RawParseMode;
use portal::{CachedPlanHandle as PortalCachedPlanHandle, FetchDirection, Portal};
use ::types_resowner::ResourceOwner;

use crate::dest_spi::{create_spi_dest_receiver, take_spi_raw_result, RawCol};
use crate::eval::EvalParamValue;
use crate::execsql::{
    build_dyn_param_list, build_param_list, prepare_dynexecute_plan, prepare_execsql_plan,
    ExecsqlColumn,
};

use execMain as execmain;
use dest_seams as dest_seams;
use pquery as pquery;
use cache_plancache as plancache;
use plancache_seams as plancache_seams;
use portalmem_seams as portalmem;
use snapmgr_seams as snapmgr;

// CURSOR_OPT_* (parsenodes.h): the scroll / no-scroll decision bits.
use ::nodes::portalcmds::{CURSOR_OPT_NO_SCROLL, CURSOR_OPT_SCROLL};

type SourceHandle = u64;

/// `elog(ERROR, msg)` — an internal SPI cursor error.
fn cursor_error(msg: impl Into<String>) -> PgError {
    utils_error::ereport(ERROR)
        .errmsg_internal(msg.into())
        .into_error()
}

/// The result of a cursor fetch/move: the SPI return code, the row count, and —
/// for a fetch (not a move) — every fetched row's raw columns (`SPI_tuptable`).
/// A move returns no rows (the C `None_Receiver` path).
pub struct CursorFetchResult {
    pub processed: u64,
    /// The fetched rows (empty for a `MOVE`). Each row is a vector of its
    /// columns in the cursor's result-descriptor order.
    pub rows: Vec<Vec<ExecsqlColumn>>,
}

/// `SPI_cursor_open_with_paramlist(name, plan, params, read_only)` →
/// `SPI_cursor_open_internal`, specialized to the PL/pgSQL `OPEN` path: the
/// cursor plan is built from the embedded `query` (with the PL/pgSQL parser
/// hooks, so barewords resolve to `$dno+1` Params) and the bound estate datums
/// resolved by `resolve`. Returns the open portal's name (C's
/// `portal->name`).
///
/// `curname` is the explicit cursor name (`None`/empty → a generated
/// nonconflicting name, C's `CreateNewPortal`). `cursor_options` carries the
/// `CURSOR_OPT_*` flags from the cursor declaration.
pub fn spi_cursor_open_plpgsql(
    curname: Option<&str>,
    query: &str,
    parsemode: RawParseMode,
    parse_state: PlpgsqlExprParseState,
    cursor_options: i32,
    read_only: bool,
    resolve: &mut dyn FnMut(i32) -> PgResult<EvalParamValue>,
) -> PgResult<String> {
    // The prepare work lives in a private context (C's `_SPI_current->procCxt`);
    // the completed CachedPlanSource owns its own context in the plancache
    // registry, and the portal owns its copy of the planned statements, so this
    // working arena can be dropped on return.
    let cxt = MemoryContext::new("SPI Cursor Prepare");
    let mcx = cxt.mcx();
    let interned = leak_str_in(mcx, query)?;

    // _SPI_prepare_plan: CreateCachedPlan + parse_analyze_plpgsql_expr +
    // QueryRewrite + CompleteCachedPlan. The resulting source is the single
    // `plancache_list` entry of C's transient `_SPI_plan`.
    let source = prepare_execsql_plan(mcx, interned, parsemode, parse_state.clone())?;

    // Build a value ParamListInfo from the referenced estate datums (C's
    // _SPI_convert_params / copyParamList of the bound paramLI). The portal
    // owner copies it into the portal context.
    let result = build_param_list(&parse_state, resolve)
        .and_then(|param_li| open_internal(mcx, curname, source, cursor_options, read_only, param_li));

    // The source is the one-shot (unsaved) plan; once the portal owns its own
    // copy of the statements (or on error before that), drop it. C's
    // `!plan->saved` branch `ReleaseCachedPlan`s the cplan and the transient
    // `_SPI_plan` is discarded with `_SPI_end_call`.
    let _ = plancache::DropCachedPlan(source);

    result
}

/// `SPI_cursor_open_internal` (spi.c): check the plan is a cursor plan, create
/// the portal, define its query from the cached plan, set the scroll options,
/// copy in the params, and `PortalStart`. Returns the portal name.
fn open_internal(
    mcx: Mcx<'_>,
    curname: Option<&str>,
    source: SourceHandle,
    cursor_options: i32,
    read_only: bool,
    param_li: ::nodes::params::ParamListInfo,
) -> PgResult<String> {
    // SPI_is_cursor_plan(plan): the plan must return tuples (plansource->
    // resultDesc != NULL). A SELECT INTO / DML / utility cannot be a cursor.
    let source_h = ::nodes::parsestmt::CachedPlanSourceHandle(source);
    if !plancache_seams::plansource_has_result_desc::call(source_h)? {
        // C gives a tag-specific message ("cannot open %s query as cursor",
        // GetCommandTagName(...)). The cmdtag name table is not a dependency
        // here; a non-tuple-returning plan as a cursor is the only case.
        return Err(utils_error::ereport(ERROR)
            .errcode(::types_error::ERRCODE_INVALID_CURSOR_DEFINITION)
            .errmsg("cannot open non-SELECT query as cursor")
            .into_error());
    }

    // Create the portal. C: name NULL/empty → CreateNewPortal (a generated
    // nonconflicting name); else CreatePortal(name, false, false) (error if a
    // portal of the same name already exists).
    let portal: Portal = match curname {
        Some(name) if !name.is_empty() => portalmem::create_portal::call(name, false, false)?,
        _ => portalmem::create_new_portal::call()?,
    };

    // Replan if needed and materialize the planned statements (C: GetCachedPlan
    // + cplan->stmt_list). For an unsaved one-shot source the portal must not
    // depend on the cplan, so we hand the (copied-into-portal-context)
    // statements with a NULL cplan handle — portalmem's
    // portal_define_query_list does the copyObject into the portal context.
    let cplan = plancache::GetCachedPlan(source, param_li.clone(), ResourceOwner::NULL, None)?;
    let stmt_list = plancache_seams::cached_plan_stmt_list::call(mcx, SeamCachedPlanHandle(cplan))?;
    // plansource->commandTag — for a cursor (tuple-returning) plan this is
    // CMDTAG_SELECT. `::portal::CommandTag` is the bare i32; unwrap the
    // newtype the seam returns.
    let command_tag: ::portal::CommandTag =
        plancache_seams::plansource_command_tag::call(source_h)?.0;

    // Inspect the (single) statement for the scroll decision and the read-only
    // check before handing it off — copyObject preserves rowMarks/planTree, so
    // reading them off the working-context plan now matches C reading them off
    // the portal copy.
    let stmts: &[::nodes::nodeindexscan::PlannedStmt] = &stmt_list;
    let single_select = stmts.len() == 1
        && stmts[0].commandType != CmdType::CMD_UTILITY;
    let row_marks_nil = single_select && stmts[0].rowMarks.is_none();
    let supports_backward = if single_select {
        execmain::exec_supports_backward_scan(&stmts[0])?
    } else {
        false
    };

    // If told to be read-only, check for read-only queries (C: foreach stmt,
    // CommandIsReadOnly). A cursor plan is a single tuple-returning statement;
    // a plain SELECT without row marks is read-only. A SELECT FOR UPDATE/SHARE
    // (rowMarks present) is not read-only.
    if read_only && single_select && !row_marks_nil {
        return Err(cursor_error(
            "SELECT FOR UPDATE/SHARE is not allowed in a non-volatile function",
        ));
    }

    // PortalDefineQuery(portal, NULL, query_string, commandTag, stmt_list,
    // cplan). The portal owns its copy of the statements; the unsaved cplan is
    // dropped by the caller, so the handle stored on the portal is NULL.
    portalmem::portal_define_query_list::call(
        &portal,
        None,
        // query_string: the source text (diagnostic). The plancache source
        // owns the canonical string; copy it into the portal via the seam.
        plancache_seams::plansource_query_string::call(mcx, source_h)?.as_str(),
        command_tag,
        stmts,
        PortalCachedPlanHandle::NULL,
    )?;

    // The portal now owns the statements; release our cplan refcount.
    let _ = plancache::ReleaseCachedPlan(cplan, ResourceOwner::NULL);

    // Set up the portal scroll options (C: portal->cursorOptions =
    // plan->cursor_options; default SCROLL decision like PerformCursorOpen).
    {
        let mut p = portal.borrow_mut();
        p.cursorOptions = cursor_options;
        if (p.cursorOptions & (CURSOR_OPT_SCROLL | CURSOR_OPT_NO_SCROLL)) == 0 {
            if single_select && row_marks_nil && supports_backward {
                p.cursorOptions |= CURSOR_OPT_SCROLL;
            } else {
                p.cursorOptions |= CURSOR_OPT_NO_SCROLL;
            }
        }
    }

    // Disallow SCROLL with SELECT FOR UPDATE.
    if (portal.borrow().cursorOptions & CURSOR_OPT_SCROLL) != 0
        && single_select
        && !row_marks_nil
    {
        return Err(cursor_error(
            "DECLARE SCROLL CURSOR ... FOR UPDATE/SHARE is not supported",
        ));
    }

    // Copy the params into the portal context (C: copyParamList after
    // MemoryContextSwitchTo(portal->portalContext)).
    let portal_params = portalmem::copy_param_list_into_portal::call(&portal, param_li)?;

    // Set up the snapshot. C: read_only → GetActiveSnapshot; else
    // CommandCounterIncrement + GetTransactionSnapshot.
    let snapshot = if read_only {
        snapmgr::get_active_snapshot::call()?
    } else {
        transam_xact_seams::command_counter_increment::call()?;
        Some(std::rc::Rc::new(snapmgr::get_transaction_snapshot::call()?))
    };

    // Start portal execution, inserting parameters if any.
    pquery::portal_start(&portal, portal_params, 0, snapshot)?;

    // Assert(portal->strategy != PORTAL_MULTI_QUERY).
    debug_assert_ne!(
        portal.borrow().strategy,
        ::portal::PORTAL_MULTI_QUERY
    );

    let name = portal.borrow().name.clone();
    Ok(name)
}

/// `SPI_cursor_parse_open(name, src, options)` (spi.c) → `SPI_cursor_open_internal`,
/// specialized to the PL/pgSQL `OPEN ... FOR EXECUTE` path
/// (`exec_dynquery_with_params`): prepare the already-rendered dynamic query
/// string `query` as a top-level statement (`RAW_PARSE_DEFAULT`, with the
/// `USING`-param types as fixed parameters), open a real portal with the given
/// `cursor_options`, and return the portal name. `params` are the already
/// evaluated `USING` values.
pub fn spi_cursor_parse_open(
    curname: Option<&str>,
    query: &str,
    params: &[EvalParamValue],
    cursor_options: i32,
    read_only: bool,
) -> PgResult<String> {
    let cxt = MemoryContext::new("SPI Cursor Parse Open");
    let mcx = cxt.mcx();
    let interned = leak_str_in(mcx, query)?;

    // _SPI_prepare_plan with the USING param types as fixed parameters (no
    // PL/pgSQL parser hooks); C's SPI_cursor_parse_open uses RAW_PARSE_DEFAULT.
    let param_types: Vec<types_core::Oid> = params.iter().map(|p| p.typeid).collect();
    let source = prepare_dynexecute_plan(mcx, interned, &param_types)?;

    // Build the value ParamListInfo directly from the evaluated USING values.
    let result = build_dyn_param_list(params)
        .and_then(|param_li| open_internal(mcx, curname, source, cursor_options, read_only, param_li));

    // The one-shot (unsaved) source: once the portal owns its copy of the
    // statements (or on error before that), drop it.
    let _ = plancache::DropCachedPlan(source);

    result
}

/// `SPI_cursor_find(name)` — does a cursor of this name currently exist?
/// (C `GetPortalByName(name) != NULL`.)
pub fn spi_cursor_find(name: &str) -> PgResult<bool> {
    Ok(portalmem::get_portal_by_name::call(name)?.is_some())
}

/// `SPI_scroll_cursor_fetch` / `SPI_scroll_cursor_move` →
/// `_SPI_cursor_operation(portal, direction, count, dest)`: find the cursor by
/// name, run `PortalRunFetch` in the given direction, and (for a fetch) return
/// every fetched row's raw columns. A move uses the `None` receiver and returns
/// no rows (C's `None_Receiver`).
pub fn spi_cursor_fetch_move(
    name: &str,
    direction: FetchDirection,
    count: i64,
    is_move: bool,
) -> PgResult<CursorFetchResult> {
    // GetPortalByName(name); if invalid → elog(ERROR).
    let portal = portalmem::get_portal_by_name::call(name)?
        .ok_or_else(|| cursor_error("invalid portal in SPI cursor operation"))?;

    if is_move {
        // SPI_cursor_move: None_Receiver (DestNone). No rows are collected; the
        // portal advances and reports the count.
        let dest = dest_seams::create_dest_receiver::call(types_dest::CommandDest::None);
        let processed = pquery::portal_run_fetch(&portal, direction, count, dest)?;
        return Ok(CursorFetchResult {
            processed,
            rows: Vec::new(),
        });
    }

    // SPI_cursor_fetch: CreateDestReceiver(DestSPI). PortalRunFetch pushes the
    // fetched tuples into the receiver; take_spi_raw_result reads them out.
    let receiver = create_spi_dest_receiver();
    let processed = pquery::portal_run_fetch(&portal, direction, count, receiver)?;

    let (columns, raw_rows) = take_spi_raw_result(receiver);

    // Project each raw row into the ExecsqlColumn shape (per-column type OID +
    // name from the result descriptor), exactly like the execsql collect path.
    let project_row = |row: &Vec<RawCol>| -> Vec<ExecsqlColumn> {
        row.iter()
            .enumerate()
            .map(|(i, col): (usize, &RawCol)| ExecsqlColumn {
                value: col.value,
                isnull: col.isnull,
                typeid: columns
                    .get(i)
                    .map(|c| c.typeid)
                    .unwrap_or(types_core::InvalidOid),
                name: columns.get(i).map(|c| c.name.clone()).unwrap_or_default(),
                byref: col.byref.clone(),
            })
            .collect()
    };

    let rows: Vec<Vec<ExecsqlColumn>> = raw_rows.iter().map(project_row).collect();

    Ok(CursorFetchResult { processed, rows })
}

/// `SPI_cursor_close(portal)` — close (drop) the named cursor. C:
/// `if (!PortalIsValid(portal)) elog(ERROR, …); PortalDrop(portal, false);`.
pub fn spi_cursor_close_by_name(name: &str) -> PgResult<()> {
    let portal = portalmem::get_portal_by_name::call(name)?
        .ok_or_else(|| cursor_error("invalid portal in SPI cursor operation"))?;
    portalmem::portal_drop::call(&portal, false)
}

/// Copy `s` into the arena and return a `&'mcx str`.
fn leak_str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = s.as_bytes();
    let mut v: ::mcx::PgVec<'mcx, u8> = ::mcx::PgVec::new_in(mcx);
    v.try_reserve(bytes.len()).map_err(|_| mcx.oom(bytes.len()))?;
    v.extend_from_slice(bytes);
    let leaked: &'mcx [u8] = allocator_api2::boxed::Box::leak(v.into_boxed_slice());
    Ok(core::str::from_utf8(leaked).expect("valid utf8 from &str"))
}
