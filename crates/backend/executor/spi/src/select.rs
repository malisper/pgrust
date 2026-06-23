//! The value-returning SELECT execution path (`spi.c`: `SPI_execute` /
//! `_SPI_prepare_oneshot_plan` / `_SPI_execute_plan` / `_SPI_pquery`,
//! specialized to a read-only single-result SELECT producing the consumer-facing
//! [`SpiResult`]).
//!
//! This is the core the xml `query_to_xml` / `table_to_xml` family and the
//! descriptor reads bottom out on (`spi_execute_select`, `spi_query_tupdesc`).
//! It runs the faithful pipeline:
//!
//!   `SPI_connect()` →
//!   raw_parser (`_SPI_prepare_oneshot_plan`) →
//!   CreateOneShotCachedPlan + parse_analyze + CompleteCachedPlan →
//!   GetCachedPlan (`_SPI_execute_plan`) →
//!   per PlannedStmt: CreateQueryDesc + ExecutorStart/Run/Finish/End
//!     to a DestSPI receiver (`_SPI_pquery`) →
//!   collect the rendered rows →
//!   `SPI_finish()`.
//!
//! # Scope (faithful structural boundary)
//!
//! Only the **plain read-only SELECT** path is wired, which is exactly what the
//! xml/tsvector consumers issue (catalog SELECTs). A statement that is not a
//! plain SELECT — DML, utility, `SELECT … FOR UPDATE/SHARE`, a modifying CTE, or
//! a parallel plan — reaches the executor driver's `#167 F0d` guard-panic in
//! `standard_ExecutorStart` / `ExecutePlan` (`backend-executor-execMain`), which
//! is the genuinely-unported substrate boundary for the non-SELECT executor.
//! Multi-statement / utility dispatch (`ProcessUtility`, the CTAS/COPY row-count
//! special cases) is the non-SELECT leg and is not reached for these consumers.

use utils_error::ereport;
use mcx::{MemoryContext, Mcx, PgVec};
use types_error::{PgResult, ERROR};
use nodes::nodeindexscan::PlannedStmt;
use nodes::parsestmt::CachedPlanHandle;
use types_resowner::ResourceOwner;
use types_xml::{SpiColumn, SpiResult};

// The plancache owner's pub fns operate on its bare `u64` source/plan handle
// aliases; the plancache *seams* take the `nodes` newtype handles. We
// hold the bare handle and wrap when calling a seam.
type SourceHandle = u64;

use crate::backbone::{set_spi_processed, SPI_connect, SPI_finish};
use crate::dest_spi::{create_spi_dest_receiver, take_spi_result};

use execMain as execmain;
use cache_plancache as plancache;
use plancache_seams as plancache_seams;
use snapmgr_seams as snapmgr;

const SCRATCH_CXT: &str = "SPI Exec";

/// Copy `bytes` into the arena and return a `&'mcx str` (the executor / parser
/// API wants the query text rooted in the working context, like C's
/// `MessageContext`-resident query string).
fn leak_str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = s.as_bytes();
    let mut v: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    v.try_reserve(bytes.len()).map_err(|_| mcx.oom(bytes.len()))?;
    v.extend_from_slice(bytes);
    let leaked: &'mcx [u8] = allocator_api2::boxed::Box::leak(v.into_boxed_slice());
    // The input is already a valid &str, so this never fails.
    Ok(core::str::from_utf8(leaked).expect("valid utf8 from &str"))
}

/// Parse `query` into a single one-shot `CachedPlanSource`, run parse analysis +
/// rewrite, and `CompleteCachedPlan` it (`_SPI_prepare_oneshot_plan` + the
/// one-shot analysis leg of `_SPI_execute_plan`). Returns the completed handle.
fn prepare_oneshot_select<'mcx>(
    mcx: Mcx<'mcx>,
    query: &'mcx str,
) -> PgResult<SourceHandle> {
    // raw_parser(src, RAW_PARSE_DEFAULT).
    let raw_list = driver::raw_parser(
        mcx,
        query,
        parsenodes::RawParseMode::RAW_PARSE_DEFAULT,
    )?;

    // The value-SELECT consumers pass a single SELECT. A query producing zero or
    // several raw statements is rejected here (the multi-statement SPI leg goes
    // through ProcessUtility / the non-SELECT executor, unported).
    if raw_list.len() != 1 {
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_SYNTAX_ERROR)
            .errmsg("SPI_execute expected a single query")
            .into_error());
    }
    let parsetree = &raw_list[0];

    // CreateOneShotCachedPlan(parsetree, src, CreateCommandTag(stmt)).
    let command_tag = utility_seams::create_command_tag::call(&parsetree.stmt)?;
    let plansource = plancache::CreateOneShotCachedPlan(parsetree, query, command_tag)?;

    // One-shot: parse analysis now (the `plan->oneshot` branch of
    // _SPI_execute_plan). No parserSetup, no parameters.
    let query_node =
        parser_analyze::parse_analyze_fixedparams(mcx, parsetree, query, &[])?;
    let mut querytree_list: PgVec<'mcx, nodes::copy_query::Query<'mcx>> = PgVec::new_in(mcx);
    querytree_list.push(query_node);

    // CompleteCachedPlan(plansource, stmt_list, NULL, argtypes, nargs,
    //                    NULL, NULL, cursor_options, false).
    plancache::CompleteCachedPlan(
        plansource,
        &querytree_list,
        &[],   // param_types
        0,     // num_params
        false, // has_parser_setup
        nodes::copy_query::CURSOR_OPT_PARALLEL_OK,
        false, // fixed_result
    )?;

    Ok(plansource)
}

/// `SPI_connect(); SPI_execute(query, true, 0); …; SPI_finish();` returning the
/// rendered [`SpiResult`] (`spi_execute_select` seam body). Read-only, single
/// SELECT (the only shape the xml/tsvector consumers issue).
pub fn spi_execute_select(query: &str) -> PgResult<SpiResult> {
    SPI_connect()?;
    let result = run_select(query);
    SPI_finish()?;
    result
}

/// `spi_query_tupdesc(query)` — prepare the query and report the result tuple
/// descriptor (`SPI_tuptable->tupdesc` shape) without fetching rows.
pub fn spi_query_tupdesc(query: &str) -> PgResult<Vec<SpiColumn>> {
    SPI_connect()?;
    let result = run_query_tupdesc(query);
    SPI_finish()?;
    result
}

/// Core of [`spi_execute_select`] between connect/finish.
fn run_select(query: &str) -> PgResult<SpiResult> {
    // The SPI executor context (`_SPI_execmem`): a private arena for the
    // parse/plan/execute working data; results are rendered to owned `String`s
    // and the arena is dropped on return (C reclaims execCxt at _SPI_end_call).
    let cxt = MemoryContext::new(SCRATCH_CXT);
    let mcx = cxt.mcx();

    let interned = leak_str_in(mcx, query)?;
    let plansource = prepare_oneshot_select(mcx, interned)?;

    // SELECT always needs an active snapshot for planning + execution (the
    // `snapshot == InvalidSnapshot && requires snapshot` path of
    // _SPI_execute_plan: PushActiveSnapshot(GetTransactionSnapshot())).
    snapmgr::push_active_snapshot_transaction::call()?;
    let exec = run_cached_select(mcx, plansource);
    let _ = snapmgr::pop_active_snapshot::call();

    let _ = plancache::DropCachedPlan(plansource);
    exec
}

/// Run a completed one-shot SELECT `plansource`: GetCachedPlan, then run each
/// PlannedStmt through the executor to a DestSPI receiver (`_SPI_execute_plan`
/// SELECT leg + `_SPI_pquery`).
fn run_cached_select<'mcx>(mcx: Mcx<'mcx>, plansource: SourceHandle) -> PgResult<SpiResult> {
    let cplan = plancache::GetCachedPlan(
        plansource,
        None,
        ResourceOwner::NULL,
        None,
    )?;
    let stmt_list =
        plancache_seams::cached_plan_stmt_list::call(mcx, CachedPlanHandle(cplan))?;

    let mut last: Option<SpiResult> = None;
    for stmt in stmt_list.iter() {
        last = Some(run_one_select_stmt(stmt)?);
    }
    let _ = plancache::ReleaseCachedPlan(cplan, ResourceOwner::NULL);

    let result = last.unwrap_or_default();
    set_spi_processed(result.rows.len() as u64);
    Ok(result)
}

/// `_SPI_pquery` for a single SELECT `stmt`: CreateQueryDesc to a fresh DestSPI
/// receiver, ExecutorStart/Run/Finish/End, then take the collected rows.
fn run_one_select_stmt<'mcx>(stmt: &PlannedStmt<'mcx>) -> PgResult<SpiResult> {
    let receiver = create_spi_dest_receiver();

    // snap = GetActiveSnapshot() (pushed by the caller).
    let snap = snapmgr::get_active_snapshot::call()?;

    let parent = MemoryContext::new("SPI QueryDesc parent");
    let mut qdesc = execmain::CreateQueryDesc(
        &parent,
        stmt,
        "", // source text (diagnostic only)
        snap,
        None, // crosscheck snapshot
        receiver,
        None,
        0, // instrument_options
    )?;

    // _SPI_pquery: fire_triggers=true -> eflags = 0.
    execmain::ExecutorStart(&mut qdesc, 0)?;
    execmain::ExecutorRun(&mut qdesc, types_scan::sdir::ForwardScanDirection, u64::MAX)?;
    execmain::ExecutorFinish(&mut qdesc)?;
    execmain::ExecutorEnd(&mut qdesc)?;
    execmain::FreeQueryDesc(qdesc)?;

    Ok(take_spi_result(receiver))
}

/// Core of [`spi_query_tupdesc`]: prepare the SELECT and read its result
/// descriptor via the plancache `plansource_result_desc` seam.
fn run_query_tupdesc(query: &str) -> PgResult<Vec<SpiColumn>> {
    let cxt = MemoryContext::new(SCRATCH_CXT);
    let mcx = cxt.mcx();
    let interned = leak_str_in(mcx, query)?;
    let plansource = prepare_oneshot_select(mcx, interned)?;

    snapmgr::push_active_snapshot_transaction::call()?;
    let desc = plancache_seams::plansource_result_desc::call(
        mcx,
        nodes::parsestmt::CachedPlanSourceHandle(plansource),
    );
    let _ = snapmgr::pop_active_snapshot::call();
    let _ = plancache::DropCachedPlan(plansource);

    let desc = desc?;
    let mut cols: Vec<SpiColumn> = Vec::new();
    if let Some(td) = desc {
        for i in 0..td.natts {
            let attr = td.attr(i as usize);
            cols.push(SpiColumn {
                name: String::from_utf8_lossy(attr.attname.name_str()).into_owned(),
                typeid: attr.atttypid,
                is_dropped: attr.attisdropped,
            });
        }
    }
    Ok(cols)
}
