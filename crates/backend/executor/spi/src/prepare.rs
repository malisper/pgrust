//! The prepared-plan SPI surface (`spi.c`: `SPI_prepare` / `SPI_prepare_cursor`
//! / `_SPI_prepare_plan`, `SPI_keepplan` / `SPI_freeplan` /
//! `SPI_plan_is_valid`, `SPI_execute_plan` / `SPI_execute_snapshot` /
//! `_SPI_execute_plan` / `_SPI_pquery`), specialized to the parameterized
//! queries the RI trigger procs (`ri_triggers.c`) and PL/pgSQL issue.
//!
//! # Owned model of `_SPI_plan`
//!
//! C's `SPIPlanPtr` (`struct _SPI_plan *`) holds a `plancache_list` of
//! `CachedPlanSource`s, the declared `argtypes`/`nargs`, the `magic`/`saved`
//! flags and the `plancxt` the plan lives in. Here the saved-plan carrier is a
//! [`SpiPlan`] kept in a backend-lifetime thread-local registry keyed by the
//! `SpiPlanPtr` token; it records the (single) completed `CachedPlanSource`
//! handle the plancache owner already keeps alive in its own registry, plus the
//! `argtypes`. RI never prepares a multi-statement plan, so the
//! `plancache_list` is a single source (the general N-source case is the
//! follow-up multi-statement leg).
//!
//! # Pipeline (faithful to `_SPI_prepare_plan` + `_SPI_execute_plan`)
//!
//!   raw_parser(src) →
//!   per raw stmt: CreateCachedPlan + pg_analyze_and_rewrite_fixedparams
//!     (parse_analyze_fixedparams with the declared `$n` argtypes → QueryRewrite)
//!     + CompleteCachedPlan(argtypes, nargs) →
//!   SPI_keepplan: SaveCachedPlan →
//!   SPI_execute_snapshot: build a value `ParamListInfo` from the bound datums →
//!     GetCachedPlan(paramLI) → push the test/crosscheck snapshot →
//!     per PlannedStmt: CreateQueryDesc(params) + ExecutorStart/Run/Finish/End
//!       to a DestSPI receiver (`_SPI_pquery`) →
//!     collect rows + set SPI_processed + map the SPI result code.

use core::cell::RefCell;

use ::utils_error::ereport;
use mcx::{MemoryContext, Mcx, PgVec};
use ::types_core::Oid;
use types_error::{PgResult, ERROR, ERRCODE_SYNTAX_ERROR};
use ::nodes::nodeindexscan::PlannedStmt;
use ::nodes::nodes::CmdType;
use ::nodes::params::{ParamExternData, ParamListInfo, ParamListInfoData, PARAM_FLAG_CONST};
use ::nodes::parsestmt::CachedPlanHandle;
use ::types_resowner::ResourceOwner;
use types_ri_triggers::{ResultColumn, SpiExecResult, SpiPlanPtr};
use ::types_tuple::Datum;
use ::types_xml::SpiResult;

use crate::backbone::{set_spi_processed, set_spi_result};
use crate::dest_spi::{create_spi_dest_receiver, take_spi_result};
use crate::result_code::{
    SPI_ERROR_OPUNKNOWN, SPI_OK_DELETE, SPI_OK_DELETE_RETURNING, SPI_OK_INSERT,
    SPI_OK_INSERT_RETURNING, SPI_OK_SELECT, SPI_OK_UPDATE, SPI_OK_UPDATE_RETURNING,
    SPI_OK_UTILITY,
};

use execMain as execmain;
use cache_plancache as plancache;
use plancache_seams as plancache_seams;
use transam_xact_seams as xact;
use snapmgr_seams as snapmgr;

// The plancache owner's pub fns operate on its bare `u64` source/plan handle
// aliases; we hold the bare handle and wrap only when calling a seam.
pub(crate) type SourceHandle = u64;

/// `plan->plancache_list` and `plan->saved` for the cursor driver
/// ([`crate::exec`]): the prepared plan's `CachedPlanSource` handles plus
/// whether `SPI_keepplan` has been applied. `Err` for an invalid handle (the
/// C `plan == NULL || plan->magic != _SPI_PLAN_MAGIC` guard).
pub(crate) fn plan_sources(plan: SpiPlanPtr) -> PgResult<(Vec<SourceHandle>, bool)> {
    with_plan(plan, |p| (p.plancache_list.clone(), p.saved))
}

/// The owned `_SPI_plan` carrier (one per prepared/saved SPI plan).
struct SpiPlan {
    /// The completed `CachedPlanSource` handles (`plancache_list`). RI uses a
    /// single source; the field is a `Vec` so the multi-statement leg slots in.
    plancache_list: Vec<SourceHandle>,
    /// `argtypes` — the declared `$n` parameter types.
    argtypes: Vec<Oid>,
    /// `saved` — has `SPI_keepplan`/`SaveCachedPlan` been applied?
    saved: bool,
    /// The last SELECT's collected result rows, retained so `spi_first_row`
    /// (the RI violation reporter's `SPI_tuptable->vals[0]` read) can render the
    /// first tuple's columns after the execute returned.
    last_result: Option<SpiResult>,
}

thread_local! {
    /// The saved-plan registry: `SpiPlanPtr.0 - 1` indexes into it (0 is the C
    /// NULL `SPIPlanPtr` sentinel). A plan is removed by `SPI_freeplan`.
    static PLANS: RefCell<Vec<Option<SpiPlan>>> = const { RefCell::new(Vec::new()) };

    /// `SPI_tuptable` — the most-recently-executed query's result. C overwrites
    /// this global on every execute, so the RI violation reporter's
    /// `SPI_tuptable->vals[0]` read sees the *last* query's rows, not some other
    /// retained plan's. (Scanning every plan's `last_result` instead would pick a
    /// stale prior plan when several were executed — e.g. a per-row FK trigger
    /// check before the bulk `RI_Initial_Check`.)
    static LAST_SPI_RESULT: RefCell<Option<SpiResult>> = const { RefCell::new(None) };
}

/// Record the most-recent execute's result as the current `SPI_tuptable`.
fn set_last_spi_result(result: Option<SpiResult>) {
    LAST_SPI_RESULT.with(|r| *r.borrow_mut() = result);
}

fn plan_register(plan: SpiPlan) -> SpiPlanPtr {
    PLANS.with(|r| {
        let mut reg = r.borrow_mut();
        if let Some(i) = reg.iter().position(Option::is_none) {
            reg[i] = Some(plan);
            SpiPlanPtr((i + 1) as u64)
        } else {
            reg.push(Some(plan));
            SpiPlanPtr(reg.len() as u64)
        }
    })
}

fn with_plan<R>(p: SpiPlanPtr, f: impl FnOnce(&mut SpiPlan) -> R) -> PgResult<R> {
    PLANS.with(|r| {
        let mut reg = r.borrow_mut();
        match reg.get_mut((p.0 - 1) as usize).and_then(Option::as_mut) {
            Some(plan) => Ok(f(plan)),
            None => Err(ereport(ERROR)
                .errmsg_internal("SPI plan handle is invalid")
                .into_error()),
        }
    })
}

/// Copy `s` into the arena and return a `&'mcx str` (the parser / executor want
/// the query text rooted in the working context, like C's `MessageContext`-
/// resident query string).
fn leak_str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = s.as_bytes();
    let mut v: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    v.try_reserve(bytes.len()).map_err(|_| mcx.oom(bytes.len()))?;
    v.extend_from_slice(bytes);
    let leaked: &'mcx [u8] = allocator_api2::boxed::Box::leak(v.into_boxed_slice());
    Ok(core::str::from_utf8(leaked).expect("valid utf8 from &str"))
}

/// `SPI_prepare(src, nargs, argtypes)` → `SPI_prepare_cursor(src, …, 0)` →
/// `_SPI_prepare_plan`. Parse, analyze (with the declared `$n` argtypes), and
/// rewrite each raw statement into a completed (non-saved) `CachedPlanSource`,
/// and register the resulting `_SPI_plan`.
///
/// Returns `Ok(None)` (C `return NULL; SPI_result = SPI_ERROR_*`) on a parse /
/// analysis error, exactly like C's `SPI_prepare`, which swallows the error into
/// `SPI_result` and returns NULL. The connection check is the caller's job
/// (`SPI_prepare` requires an active SPI connection; RI always `SPI_connect`s
/// first).
pub fn spi_prepare(querystr: &[u8], argtypes: &[Oid]) -> PgResult<Option<SpiPlanPtr>> {
    let src = match core::str::from_utf8(querystr) {
        Ok(s) => s,
        Err(_) => {
            set_spi_result(SPI_ERROR_OPUNKNOWN);
            return Ok(None);
        }
    };

    // The prepare work lives in a private context (C's `_SPI_current->procCxt`
    // for the temporary parse trees); the completed CachedPlanSources own their
    // own contexts in the plancache registry, so the working arena can be
    // dropped on return.
    let cxt = MemoryContext::new("SPI Prepare");
    let mcx = cxt.mcx();
    let interned = leak_str_in(mcx, src)?;

    match prepare_plan(mcx, interned, argtypes) {
        Ok(plan) => Ok(Some(plan_register(plan))),
        Err(_e) => {
            // `SPI_prepare` translates the analysis error into SPI_result and
            // returns NULL rather than propagating (spi.c). RI's caller then
            // turns the NULL into its own "SPI_prepare returned <code>" error.
            set_spi_result(SPI_ERROR_OPUNKNOWN);
            Ok(None)
        }
    }
}

/// `_SPI_prepare_plan(src, plan)`: raw_parser → per raw stmt CreateCachedPlan +
/// parse_analyze_fixedparams + QueryRewrite + CompleteCachedPlan. Returns the
/// (non-saved) `_SPI_plan` carrier.
fn prepare_plan<'mcx>(
    mcx: Mcx<'mcx>,
    query: &'mcx str,
    argtypes: &[Oid],
) -> PgResult<SpiPlan> {
    let raw_list = driver::raw_parser(
        mcx,
        query,
        parsenodes::RawParseMode::RAW_PARSE_DEFAULT,
    )?;

    let mut plancache_list: Vec<SourceHandle> = Vec::with_capacity(raw_list.len());

    for parsetree in raw_list.iter() {
        // CreateCachedPlan(parsetree, src, CreateCommandTag(stmt)).
        let command_tag = utility_seams::create_command_tag::call(&parsetree.stmt)?;
        let plansource = plancache::CreateCachedPlan(parsetree, query, command_tag)?;

        // pg_analyze_and_rewrite_fixedparams: parse analysis with the declared
        // `$n` argtypes, then rewrite.
        let query_node =
            parser_analyze::parse_analyze_fixedparams(mcx, parsetree, query, argtypes)?;
        let querytree_list =
            rewritehandler::QueryRewrite(mcx, query_node)?;

        // CompleteCachedPlan(plansource, stmt_list, NULL, argtypes, nargs,
        //                    NULL, NULL, cursor_options, false).
        plancache::CompleteCachedPlan(
            plansource,
            &querytree_list,
            argtypes,
            argtypes.len() as i32,
            false, // has_parser_setup
            ::nodes::copy_query::CURSOR_OPT_PARALLEL_OK,
            false, // fixed_result
        )?;

        plancache_list.push(plansource);
    }

    if plancache_list.is_empty() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("SPI_prepare: empty query")
            .into_error());
    }

    Ok(SpiPlan {
        plancache_list,
        argtypes: argtypes.to_vec(),
        saved: false,
        last_result: None,
    })
}

/// `SPI_keepplan(plan)`: make the prepared plan persist across the SPI
/// connection by `SaveCachedPlan`ing every source and flipping `saved`.
pub fn spi_keepplan(plan: SpiPlanPtr) -> PgResult<()> {
    let sources = with_plan(plan, |p| p.plancache_list.clone())?;
    for src in sources {
        plancache::SaveCachedPlan(src)?;
    }
    with_plan(plan, |p| p.saved = true)?;
    Ok(())
}

/// `SPI_freeplan(plan)`: drop the saved plan and free its sources.
pub fn spi_freeplan(plan: SpiPlanPtr) -> PgResult<()> {
    let taken = PLANS.with(|r| {
        r.borrow_mut()
            .get_mut((plan.0 - 1) as usize)
            .and_then(Option::take)
    });
    match taken {
        Some(p) => {
            for src in p.plancache_list {
                let _ = plancache::DropCachedPlan(src);
            }
            Ok(())
        }
        None => Err(ereport(ERROR)
            .errmsg_internal("SPI_freeplan: invalid plan handle")
            .into_error()),
    }
}

/// `SPI_plan_is_valid(plan)`: a saved SPI plan is always revalidated lazily on
/// `GetCachedPlan`, so a live carrier is valid (C `CachedPlanIsValid` returns
/// true once the source is revalidatable). A missing handle is not valid.
pub fn spi_plan_is_valid(plan: SpiPlanPtr) -> bool {
    PLANS.with(|r| {
        r.borrow()
            .get((plan.0 - 1) as usize)
            .map(Option::is_some)
            .unwrap_or(false)
    })
}

/// `SPI_execute_snapshot(plan, Values, Nulls, snapshot, crosscheck_snapshot,
/// read_only, fire_triggers, tcount)` → `_SPI_execute_plan`. The RI execution
/// variant: run each cached statement of `plan` with the bound parameter datums
/// under the supplied snapshot, collecting rows into the SPI tuple table and
/// reporting `(SPI return code, SPI_processed)`.
#[allow(clippy::too_many_arguments)]
pub fn spi_execute_snapshot<'mcx>(
    plan: SpiPlanPtr,
    vals: &[Datum<'mcx>],
    nulls: &[bool],
    snapshot: Option<snapshot::SnapshotData>,
    crosscheck: Option<snapshot::SnapshotData>,
    _read_only: bool,
    fire_triggers: bool,
    tcount: i64,
) -> PgResult<SpiExecResult> {
    let (sources, argtypes) =
        with_plan(plan, |p| (p.plancache_list.clone(), p.argtypes.clone()))?;

    // _SPI_convert_params: build the value ParamListInfo from the bound datums.
    let param_li = convert_params(&argtypes, vals, nulls)?;

    // The execution working data lives in a private arena (C's
    // `_SPI_current->execCxt`); results are rendered to owned Strings here.
    let cxt = MemoryContext::new("SPI Exec");
    let mcx = cxt.mcx();

    let mut my_res: i32 = 0;
    let mut my_processed: u64 = 0;
    let mut last_result: Option<SpiResult> = None;

    for &source in sources.iter() {
        let (res, processed, result) = execute_one_source(
            mcx, source, &param_li, &snapshot, &crosscheck, _read_only, fire_triggers, tcount,
        )?;
        my_res = res;
        my_processed = processed;
        if result.is_some() {
            last_result = result;
        }
        if my_res < 0 {
            break;
        }
    }

    set_spi_processed(my_processed);
    set_spi_result(my_res);
    set_last_spi_result(last_result.clone());
    with_plan(plan, |p| p.last_result = last_result)?;

    Ok(SpiExecResult {
        code: my_res,
        processed: my_processed,
    })
}

/// `_SPI_execute_plan` for a single saved `CachedPlanSource`: GetCachedPlan with
/// the bound params, push the snapshot, run each PlannedStmt to a DestSPI
/// receiver (`_SPI_pquery`), and map the SPI result code.
fn execute_one_source<'mcx>(
    mcx: Mcx<'mcx>,
    source: SourceHandle,
    param_li: &ParamListInfo,
    snapshot: &Option<snapshot::SnapshotData>,
    crosscheck: &Option<snapshot::SnapshotData>,
    read_only: bool,
    fire_triggers: bool,
    tcount: i64,
) -> PgResult<(i32, u64, Option<SpiResult>)> {
    // _SPI_execute_plan: when no caller snapshot is given, RI's read-only check
    // still needs the transaction snapshot to plan/run under; with a caller
    // snapshot (the serializable detect-new-rows path) we push exactly that.
    let pushed = match snapshot {
        Some(s) => {
            snapmgr::push_active_snapshot::call(std::rc::Rc::new(s.clone()))?;
            true
        }
        None => {
            snapmgr::push_active_snapshot_transaction::call()?;
            true
        }
    };

    let out = run_cached(
        mcx,
        source,
        param_li,
        crosscheck,
        read_only,
        pushed,
        fire_triggers,
        tcount,
    );

    if pushed {
        let _ = snapmgr::pop_active_snapshot::call();
    }
    out
}

fn run_cached<'mcx>(
    mcx: Mcx<'mcx>,
    source: SourceHandle,
    param_li: &ParamListInfo,
    crosscheck: &Option<snapshot::SnapshotData>,
    read_only: bool,
    pushed_active_snap: bool,
    fire_triggers: bool,
    tcount: i64,
) -> PgResult<(i32, u64, Option<SpiResult>)> {
    let cplan = plancache::GetCachedPlan(source, param_li.clone(), ResourceOwner::NULL, None)?;
    let stmt_list = plancache_seams::cached_plan_stmt_list::call(mcx, CachedPlanHandle(cplan))?;

    let mut res: i32 = 0;
    let mut processed: u64 = 0;
    let mut last_result: Option<SpiResult> = None;

    for stmt in stmt_list.iter() {
        // _SPI_execute_plan: in non-read-only mode, advance the command counter
        // before each command and update the active snapshot's command id, so a
        // statement sees the effects of the prior commands in this same SPI call
        // (e.g. a cascaded FK UPDATE's re-fired RI check must see the row the
        // outer UPDATE just changed). Skipped when the snapshot isn't ours.
        if !read_only && pushed_active_snap {
            xact::command_counter_increment::call()?;
            snapmgr::update_active_snapshot_command_id::call()?;
        }
        let (code, n, result) =
            run_one_stmt(stmt, param_li, crosscheck, fire_triggers, tcount)?;
        res = code;
        processed = n;
        if result.is_some() {
            last_result = result;
        }
    }

    let _ = plancache::ReleaseCachedPlan(cplan, ResourceOwner::NULL);
    Ok((res, processed, last_result))
}

/// `_SPI_pquery` for a single PlannedStmt: classify the result code from the
/// command type, then for a row-returning statement run it through the executor
/// to a DestSPI receiver. For the FK/RI path this is the SELECT check (`SELECT 1
/// FROM pk … FOR KEY SHARE`) and the cascade DML.
fn run_one_stmt<'mcx>(
    stmt: &PlannedStmt<'mcx>,
    param_li: &ParamListInfo,
    crosscheck: &Option<snapshot::SnapshotData>,
    fire_triggers: bool,
    tcount: i64,
) -> PgResult<(i32, u64, Option<SpiResult>)> {
    let operation = stmt.commandType;

    // _SPI_pquery: derive res from the command type (canSetTag statement).
    let res = match operation {
        CmdType::CMD_SELECT => SPI_OK_SELECT,
        CmdType::CMD_INSERT => {
            if stmt.hasReturning {
                SPI_OK_INSERT_RETURNING
            } else {
                SPI_OK_INSERT
            }
        }
        CmdType::CMD_DELETE => {
            if stmt.hasReturning {
                SPI_OK_DELETE_RETURNING
            } else {
                SPI_OK_DELETE
            }
        }
        CmdType::CMD_UPDATE => {
            if stmt.hasReturning {
                SPI_OK_UPDATE_RETURNING
            } else {
                SPI_OK_UPDATE
            }
        }
        CmdType::CMD_UTILITY => SPI_OK_UTILITY,
        _ => SPI_ERROR_OPUNKNOWN,
    };

    if res == SPI_ERROR_OPUNKNOWN {
        return Ok((res, 0, None));
    }

    // Utility statements in the RI/keepplan path do not occur (RI prepares only
    // SELECT / INSERT / UPDATE / DELETE). A utility would go through
    // ProcessUtility (the non-SELECT utility leg); guard it as decomp-blocked.
    if operation == CmdType::CMD_UTILITY {
        return Err(ereport(ERROR)
            .errmsg_internal(
                "SPI_execute_plan: utility statement not supported on the prepared-plan path",
            )
            .into_error());
    }

    let receiver = create_spi_dest_receiver();
    let snap = snapmgr::get_active_snapshot::call()?;
    let crosscheck_snap = crosscheck.as_ref().map(|s| std::rc::Rc::new(s.clone()));

    let parent = MemoryContext::new("SPI QueryDesc parent");
    let mut qdesc = execmain::CreateQueryDesc(
        &parent,
        stmt,
        "", // source text (diagnostic only)
        snap,
        crosscheck_snap,
        receiver,
        param_li.clone(),
        0, // instrument_options
    )?;

    // _SPI_pquery: select execution options. fire_triggers => eflags = 0
    // (run-to-completion); otherwise EXEC_FLAG_SKIP_TRIGGERS so the RI/FK
    // cascade sub-statement does NOT open its own after-trigger query level —
    // its queued AFTER events land at the outer query level and are picked up
    // by the AfterTriggerEndQuery re-drive loop (and share its transition
    // tuplestore). (executor.h: EXEC_FLAG_SKIP_TRIGGERS = 0x0020.)
    const EXEC_FLAG_SKIP_TRIGGERS: i32 = 0x0020;
    let eflags = if fire_triggers {
        0
    } else {
        EXEC_FLAG_SKIP_TRIGGERS
    };
    execmain::ExecutorStart(&mut qdesc, eflags)?;
    let count: u64 = if tcount <= 0 { u64::MAX } else { tcount as u64 };
    execmain::ExecutorRun(&mut qdesc, types_scan::sdir::ForwardScanDirection, count)?;
    let processed = qdesc.es_processed();
    execmain::ExecutorFinish(&mut qdesc)?;
    execmain::ExecutorEnd(&mut qdesc)?;
    execmain::FreeQueryDesc(qdesc)?;

    let result = take_spi_result(receiver);
    Ok((res, processed, Some(result)))
}

/// `_SPI_convert_params(nargs, argtypes, Values, Nulls)` (spi.c): build a value
/// `ParamListInfo` from the bound argument datums. Each by-reference datum is
/// cloned into the long-lived param context so the params outlive the caller's
/// working `Mcx` (C `palloc`s into the executor context for the call).
fn convert_params(
    argtypes: &[Oid],
    vals: &[Datum<'_>],
    nulls: &[bool],
) -> PgResult<ParamListInfo> {
    if argtypes.is_empty() {
        return Ok(None);
    }
    debug_assert_eq!(vals.len(), argtypes.len());
    debug_assert_eq!(nulls.len(), argtypes.len());

    // The params must be `'static` (the long-lived param-context convention);
    // clone the datums into a fresh leaked backend-lifetime context. The context
    // is intentionally leaked: the param list is consumed within this one
    // execute (the executor reads `params[i]`), and the leak is bounded by the
    // tiny key datums of one RI check — the faithful analogue of C palloc'ing
    // the converted params into the executor context that the SPI op resets.
    let ctx: &'static MemoryContext =
        allocator_api2::boxed::Box::leak(allocator_api2::boxed::Box::new(MemoryContext::new(
            "SPI Params",
        )));
    let pmcx: Mcx<'static> = ctx.mcx();

    let mut params: Vec<ParamExternData<'static>> = Vec::with_capacity(argtypes.len());
    for i in 0..argtypes.len() {
        let isnull = nulls[i];
        let value: Datum<'static> = if isnull {
            Datum::null()
        } else {
            vals[i].clone_in(pmcx)?
        };
        params.push(ParamExternData {
            value,
            isnull,
            pflags: PARAM_FLAG_CONST,
            ptype: argtypes[i],
        });
    }

    Ok(Some(std::rc::Rc::new(ParamListInfoData {
        param_fetch: false,
        param_fetch_arg: None,
        param_compile: false,
        param_compile_arg: None,
        parser_setup: false,
        parser_setup_arg: None,
        param_values_str: None,
        num_params: argtypes.len() as i32,
        params,
    })))
}

/// `spi_first_row_columns(attnums)`: render the named columns of the first row
/// of the last execute's SPI tuple table (`SPI_tuptable->vals[0]`) — the
/// `ri_ReportViolation` "key (a)=(99) is not present" detail. The values were
/// already rendered to text by the DestSPI receiver; this picks the requested
/// attnums (1-based) out of the first collected row.
pub fn spi_first_row_columns<'mcx>(
    mcx: Mcx<'mcx>,
    attnums: &[i16],
) -> PgResult<PgVec<'mcx, ResultColumn<'mcx>>> {
    // Read the most-recently-executed query's first row from `SPI_tuptable`
    // (C reads `SPI_tuptable->vals[0]`). Using the global last-result — rather
    // than scanning every retained plan — is essential when several queries ran
    // before the report (e.g. a per-row FK trigger check ahead of the bulk
    // `RI_Initial_Check`), since the scan would pick a stale prior plan's rows.
    let row = LAST_SPI_RESULT.with(|r| {
        r.borrow()
            .as_ref()
            .map(|res| (res.columns.clone(), res.rows.first().cloned()))
    });

    let mut out: PgVec<'mcx, ResultColumn<'mcx>> = PgVec::new_in(mcx);

    let (columns, first_row) = match row {
        Some((cols, Some(r))) => (cols, r),
        _ => return Ok(out),
    };

    for &att in attnums {
        let idx = (att - 1) as usize;
        let name = columns
            .get(idx)
            .map(|c| c.name.as_bytes().to_vec())
            .unwrap_or_default();
        let mut namebuf: PgVec<'mcx, u8> = PgVec::new_in(mcx);
        namebuf.extend_from_slice(&name);

        let value = match first_row.get(idx) {
            Some(Some(s)) => Some(::mcx::PgString::from_str_in(s, mcx)?),
            _ => None,
        };
        out.push(ResultColumn {
            name: namebuf,
            value,
        });
    }
    Ok(out)
}
