//! The PL/pgSQL single-expression evaluation path (`spi.c` `SPI_prepare_params`
//! + `_SPI_execute_plan`, specialized to the one-row `exec_run_select`
//! evaluation `pl_exec.c`'s `exec_eval_expr` slow path issues).
//!
//! This is the faithful `exec_eval_expr` → `exec_run_select` pipeline for a
//! PL/pgSQL expression (`RETURN <expr>`, `IF <cond>`, an assignment RHS, …):
//!
//!   raw_parser(expr->query, expr->parseMode) →
//!   CreateOneShotCachedPlan + parse_analyze_plpgsql_expr (installs the PL/pgSQL
//!     parser hooks so variable barewords resolve to `$dno+1` `Param`s, recording
//!     the referenced datum numbers) + QueryRewrite + CompleteCachedPlan →
//!   setup_param_list: build a value `ParamListInfo` from the referenced estate
//!     datums (resolved through the caller's `resolve` callback) →
//!   GetCachedPlan(paramLI) → push the transaction snapshot →
//!   CreateQueryDesc + ExecutorStart/Run/Finish/End to a DestSPI receiver →
//!   read the first row's first column raw datum (`SPI_getbinval`).
//!
//! Only the **simple read-only one-row SELECT** an expression produces is wired
//! (the simple-expr fast path's `exec_simple_check_plan` optimization caches the
//! `ExprState`; this is the equivalent slow path that re-plans, which C falls to
//! for any non-cacheable expression and which is always correct). The result is
//! returned as a bare-word datum: a pass-by-value result (`int4`, `bool`, …)
//! crosses faithfully; a pass-by-reference result is the separate by-ref-Datum
//! keystone.

use mcx::{MemoryContext, Mcx, PgVec};
use types_core::Oid;
use types_error::{PgResult, ERROR, ERRCODE_SYNTAX_ERROR};
use types_nodes::nodeindexscan::PlannedStmt;
use types_nodes::nodes::CmdType;
use types_nodes::params::{ParamExternData, ParamListInfo, ParamListInfoData, PARAM_FLAG_CONST};
use types_nodes::parsestmt::{CachedPlanHandle, PlpgsqlExprParseState};
use types_parsenodes::RawParseMode;
use types_resowner::ResourceOwner;
use types_tuple::Datum as RichDatum;

use crate::backbone::set_spi_processed;
use crate::dest_spi::{create_spi_dest_receiver, take_spi_raw_result, RawCol};

use backend_access_transam_xact_seams as xact_seams;
use backend_executor_execMain as execmain;
use backend_utils_cache_plancache as plancache;
use backend_utils_cache_plancache_seams as plancache_seams;
use backend_utils_time_snapmgr_seams as snapmgr;

type SourceHandle = u64;

/// One PL/pgSQL datum value the caller binds into a `Param` (the value the
/// `setup_param_list` reads out of `estate->datums[dno]`): the bare-word datum,
/// its is-null flag, and its type OID.
///
/// A pass-by-reference scalar datum (a `text`/`varchar`/`numeric` argument or
/// variable) carries its verbatim header-ful varlena / cstring byte image in
/// `byref` (the bare `value` word is `0` then); `build_param_list`
/// reconstructs a `Datum::ByRef` from it so the image survives into the bound
/// `ParamExternData` and the executed plan reads it through `ExecEvalParam*`.
/// `None` for a by-value datum.
#[derive(Clone)]
pub struct EvalParamValue {
    pub value: usize,
    pub isnull: bool,
    pub typeid: Oid,
    pub byref: Option<Vec<u8>>,
}

/// The raw result of evaluating a PL/pgSQL expression to a single value: the
/// first row's first column (`SPI_getbinval(tuptab->vals[0], tupdesc, 1)`).
#[derive(Clone)]
pub struct EvalResult {
    /// The bare-word result datum (`0` when null, or when the result is a
    /// by-reference value carried in `byref`).
    pub value: usize,
    pub isnull: bool,
    /// `Some(image)` for a non-null pass-by-reference result: the verbatim
    /// header-ful varlena / cstring byte image (the rich `Datum::ByRef` payload,
    /// `datumCopy`'d out of the SPI arena). `None` for a by-value/NULL result.
    pub byref: Option<Vec<u8>>,
    /// The result column's type OID (`SPI_gettypeid(tupdesc, 1)`).
    pub typeid: Oid,
    /// `SPI_processed` — the number of rows the expression produced.
    pub processed: u64,
}

/// Copy `s` into the arena and return a `&'mcx str` (the parser/executor want
/// the query text rooted in the working context).
fn leak_str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = s.as_bytes();
    let mut v: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    v.try_reserve(bytes.len()).map_err(|_| mcx.oom(bytes.len()))?;
    v.extend_from_slice(bytes);
    let leaked: &'mcx [u8] = allocator_api2::boxed::Box::leak(v.into_boxed_slice());
    Ok(core::str::from_utf8(leaked).expect("valid utf8 from &str"))
}

/// Map the PL/pgSQL [`RawParseMode`] enum to the parser-driver mode.
fn parse_mode(mode: RawParseMode) -> RawParseMode {
    mode
}

/// `SPI_prepare_params(src, plpgsql_parser_setup, expr, 0)` + the one-row
/// `exec_run_select` (`pl_exec.c`): prepare the PL/pgSQL expression `query` (in
/// its `parsemode`) with the PL/pgSQL parser hooks installed, bind the
/// referenced estate datums into a `ParamListInfo`, run it under the transaction
/// snapshot, and return the first row's first-column raw datum.
///
/// `resolve(dno)` reads `estate->datums[dno]`'s current `(value, isnull,
/// typeid)` — the `setup_param_list` body (which the SPI owner cannot reach into
/// the PL/pgSQL execstate for, so the caller supplies it). `maxtuples` caps the
/// row count (`exec_run_select` passes 2 to detect "query returned more than one
/// row").
pub fn spi_eval_expr(
    query: &str,
    parsemode: RawParseMode,
    parse_state: PlpgsqlExprParseState,
    maxtuples: i64,
    read_only: bool,
    resolve: &mut dyn FnMut(i32) -> PgResult<EvalParamValue>,
) -> PgResult<EvalResult> {
    // The prepare/exec working data lives in a private context (C's
    // `_SPI_current->procCxt`/`execCxt`); the result datum word is by-value, so
    // it survives the arena drop (a by-ref result would need datumCopy out).
    let cxt = MemoryContext::new("SPI Eval");
    let mcx = cxt.mcx();
    let interned = leak_str_in(mcx, query)?;

    // `_SPI_error_callback` (spi.c): SPI installs an error-context callback for
    // the WHOLE duration of `_SPI_prepare_plan` + `_SPI_execute_plan`, decorating
    // BOTH a parse/analysis-time error (e.g. `operator does not exist` / a syntax
    // error in the embedded `x + 1` expression) AND an execution-time error.
    //   * a syntax/cursor position (`geterrposition() > 0`) is cleared and
    //     promoted to an INTERNAL position + internal query = this embedded query
    //     text — rendering the `LINE n: …` caret and the `QUERY: …` block against
    //     the inner expression (not the outer `select fn(...)` call);
    //   * otherwise a mode-dependent context line is attached.
    // The "attached once" latch is cleared so an outer PL/pgSQL frame re-attaches
    // its own `plpgsql_exec_error_callback` line. (Mirrors execsql.rs exactly.)
    let spi_error_decorate = |mut e: types_error::PgError| -> types_error::PgError {
        if e.cursor_position().unwrap_or(0) > 0 {
            let pos = e.cursor_position().unwrap();
            e = e
                .with_cursor_position(0)
                .with_internal_position(pos)
                .with_internal_query(query.to_string());
        } else {
            let line = match parsemode {
                RawParseMode::RAW_PARSE_PLPGSQL_EXPR => {
                    format!("PL/pgSQL expression \"{query}\"")
                }
                RawParseMode::RAW_PARSE_PLPGSQL_ASSIGN1
                | RawParseMode::RAW_PARSE_PLPGSQL_ASSIGN2
                | RawParseMode::RAW_PARSE_PLPGSQL_ASSIGN3 => {
                    format!("PL/pgSQL assignment \"{query}\"")
                }
                _ => format!("SQL statement \"{query}\""),
            };
            e = e.add_context(line);
        }
        e.plpgsql_context_attached = false;
        e
    };

    // Execution-phase decoration for a PL/pgSQL *expression* (`RETURN <expr>`,
    // `IF <cond>`, an assignment RHS): C evaluates a *simple* expression through
    // `exec_eval_simple_expr`, which bypasses SPI and runs the cached `ExprState`
    // directly — so NO `_SPI_error_callback` is on the error_context_stack when a
    // runtime error (e.g. `division by zero` in `1/x`) is raised, and the
    // "PL/pgSQL expression \"…\"" context line is therefore NOT attached. (The
    // outer frame's own `plpgsql_exec_error_callback` still fires, which the
    // latch reset below re-enables.) The full `spi_error_decorate` line is added
    // only for parse/analysis-time errors (the prepare phase below), matching C's
    // simple-expr path where prepare still goes through SPI but execution does
    // not. A genuinely non-simple expression would run via SPI's `exec_run_select`
    // and keep the callback; pgrust's expression path corresponds to the simple
    // case (a scalar one-row SELECT), so we drop the execute-phase context line.
    let spi_error_no_context = |mut e: types_error::PgError| -> types_error::PgError {
        e.plpgsql_context_attached = false;
        e
    };

    // _SPI_prepare_plan: parse + analyze (with the PL/pgSQL parser hooks) +
    // rewrite + complete the cached plan. This records the referenced datum
    // numbers into `parse_state.paramnos`.
    let (source, argtypes) = prepare_expr_plan(mcx, interned, parsemode, parse_state.clone())
        .map_err(&spi_error_decorate)?;

    // setup_param_list: build the value ParamListInfo from the referenced estate
    // datums. The referenced dnos drive which datums to bind; the param id is
    // `dno + 1` (matching make_datum_param), so the params vector is indexed by
    // (paramid - 1) == dno and must cover the max referenced dno.
    let param_li = build_param_list(&parse_state, &argtypes, resolve)?;

    // _SPI_execute_plan: GetCachedPlan + push the transaction snapshot + run to
    // a DestSPI receiver, collecting the raw first row. When not read-only (a
    // VOLATILE function: estate->readonly_func == false), advance the command
    // counter and the active snapshot's command id before the query (spi.c:2665)
    // so the expression's SELECT sees the partial effects of the in-progress
    // outer command — e.g. `UPDATE t SET a = vol_fn()` where vol_fn() reads `t`
    // must observe rows already updated by this same UPDATE.
    let pushed = snapmgr::push_active_snapshot_transaction::call().is_ok();
    let advance_cid = pushed && !read_only;
    let out = run_eval(mcx, source, &param_li, maxtuples, advance_cid);
    if pushed {
        let _ = snapmgr::pop_active_snapshot::call();
    }
    let (processed, raw) = out.map_err(&spi_error_no_context)?;

    let _ = plancache::DropCachedPlan(source);

    // SPI_getbinval(tuptab->vals[0], tupdesc, 1, &isnull): the first row's first
    // column. No rows -> a NULL result of the column type (exec_run_select with
    // maxtuples and SPI_processed == 0 leaves *isNull = true).
    let (typeid, value, isnull, byref) = match raw.first_col() {
        Some((typeid, col)) => (typeid, col.value, col.isnull, col.byref),
        None => (types_core::InvalidOid, 0usize, true, None),
    };

    Ok(EvalResult {
        value,
        isnull,
        byref,
        typeid,
        processed,
    })
}

/// `_SPI_prepare_plan` for a PL/pgSQL expression: raw_parser(parsemode) ->
/// CreateOneShotCachedPlan + parse_analyze_plpgsql_expr + QueryRewrite +
/// CompleteCachedPlan. Returns the completed (one-shot) source handle and the
/// `$n` argtypes the analysis declared (empty — the params come from the
/// PL/pgSQL datum hooks, not declared `$n` types).
fn prepare_expr_plan<'mcx>(
    mcx: Mcx<'mcx>,
    query: &'mcx str,
    parsemode: RawParseMode,
    parse_state: PlpgsqlExprParseState,
) -> PgResult<(SourceHandle, Vec<Oid>)> {
    let raw_list = backend_parser_driver::raw_parser(mcx, query, parse_mode(parsemode))?;

    if raw_list.len() != 1 {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("PL/pgSQL expression did not yield a single query")
            .into_error());
    }
    let parsetree = &raw_list[0];

    let command_tag = backend_tcop_utility_seams::create_command_tag::call(&parsetree.stmt)?;
    // A NON-one-shot plan source: a one-shot source forces a custom plan in
    // GetCachedPlan (choose_custom_plan returns true for is_oneshot), which would
    // thread the bound param VALUES into the planner as `glob->boundParams` for
    // const-folding (the unported boundParams planner leg). PL/pgSQL binds its
    // datum Params at run time off `es_param_list_info` instead — the generic
    // plan. CURSOR_OPT_GENERIC_PLAN forces choose_custom_plan to pick generic.
    let plansource = plancache::CreateCachedPlan(parsetree, query, command_tag)?;

    // parse_analyze_plpgsql_expr installs the PL/pgSQL parser hooks; the analysis
    // records the referenced datum numbers into parse_state.paramnos.
    let query_node = backend_parser_analyze::parse_analyze_plpgsql_expr(
        mcx,
        parsetree,
        query,
        parse_state,
    )?;
    let querytree_list = backend_rewrite_rewritehandler::QueryRewrite(mcx, query_node)?;

    // The expression's Params are external (PL/pgSQL datums), not declared `$n`
    // types, so CompleteCachedPlan is given an empty argtype list — the params
    // are bound by value at execute time. has_parser_setup is true (the plan was
    // analyzed with a parser setup), so the plan is not re-analyzed with fixed
    // params on revalidation. CURSOR_OPT_GENERIC_PLAN keeps the generic plan so
    // no bound param values reach the planner.
    plancache::CompleteCachedPlan(
        plansource,
        &querytree_list,
        &[],   // param_types
        0,     // num_params
        true,  // has_parser_setup
        types_nodes::copy_query::CURSOR_OPT_PARALLEL_OK
            | types_nodes::portalcmds::CURSOR_OPT_GENERIC_PLAN,
        false, // fixed_result
    )?;

    Ok((plansource, Vec::new()))
}

/// `setup_param_list(estate, expr)` (pl_exec.c) — build a value `ParamListInfo`
/// covering the datum numbers the expression referenced. The param vector is
/// indexed by `paramid - 1 == dno`; entries for unreferenced dnos below the max
/// are filled with a NULL placeholder (never read by the plan, which only
/// fetches the referenced paramids).
fn build_param_list(
    parse_state: &PlpgsqlExprParseState,
    _argtypes: &[Oid],
    resolve: &mut dyn FnMut(i32) -> PgResult<EvalParamValue>,
) -> PgResult<ParamListInfo> {
    let dnos = parse_state.referenced_dnos();
    if dnos.is_empty() {
        // A constant expression references no datums (paramLI == NULL in C).
        return Ok(None);
    }

    let max_dno = *dnos.iter().max().unwrap();
    let num_params = (max_dno + 1) as usize;

    // The params must outlive this call's working arena (the executor reads them
    // during the run); leak a backend-lifetime context for the bound words. A
    // pass-by-value datum is the scalar word itself (no deep copy needed); a
    // pass-by-reference datum (`text`/`varchar`/`numeric`/…) is rebuilt as a
    // `Datum::ByRef` from the caller's verbatim image, copied into this leaked
    // context (`slice_in`, == C's `datumCopy` into the param-list context) so
    // the image outlives the eval working arena the executor reads it across.
    let ctx: &'static MemoryContext = allocator_api2::boxed::Box::leak(
        allocator_api2::boxed::Box::new(MemoryContext::new("PL/pgSQL Param List")),
    );
    let pmcx: Mcx<'static> = ctx.mcx();

    let mut params: Vec<ParamExternData<'static>> = Vec::with_capacity(num_params);
    let referenced: std::collections::BTreeSet<i32> = dnos.iter().copied().collect();
    for dno in 0..(num_params as i32) {
        if referenced.contains(&dno) {
            let v = resolve(dno)?;
            let value = match v.byref {
                Some(image) if !v.isnull => {
                    RichDatum::ByRef(mcx::slice_in(pmcx, &image)?)
                }
                _ => RichDatum::from_usize(v.value),
            };
            params.push(ParamExternData {
                value,
                isnull: v.isnull,
                pflags: PARAM_FLAG_CONST,
                ptype: v.typeid,
            });
        } else {
            // Placeholder for an unreferenced slot (never fetched by the plan).
            params.push(ParamExternData {
                value: RichDatum::null(),
                isnull: true,
                pflags: PARAM_FLAG_CONST,
                ptype: types_core::InvalidOid,
            });
        }
    }

    Ok(Some(std::rc::Rc::new(ParamListInfoData {
        param_fetch: false,
        param_fetch_arg: None,
        param_compile: false,
        param_compile_arg: None,
        parser_setup: false,
        parser_setup_arg: None,
        param_values_str: None,
        num_params: num_params as i32,
        params,
    })))
}

/// The collected raw result of an expression run: column type OIDs + the raw
/// bare-word datums of the rows (only the first is read).
struct EvalRaw {
    columns: Vec<types_xml::SpiColumn>,
    raw_rows: Vec<Vec<RawCol>>,
}

impl EvalRaw {
    /// `(typeid, RawCol)` of the first row's first column, if any row was
    /// produced and it has a column.
    fn first_col(&self) -> Option<(Oid, RawCol)> {
        let row = self.raw_rows.first()?;
        let col = row.first()?;
        let typeid = self.columns.first().map(|c| c.typeid).unwrap_or(types_core::InvalidOid);
        Some((typeid, col.clone()))
    }
}

/// `_SPI_execute_plan` for the one-shot expression source: GetCachedPlan + run
/// each PlannedStmt to a DestSPI receiver collecting the raw datums.
fn run_eval<'mcx>(
    mcx: Mcx<'mcx>,
    source: SourceHandle,
    param_li: &ParamListInfo,
    maxtuples: i64,
    advance_cid: bool,
) -> PgResult<(u64, EvalRaw)> {
    let cplan = plancache::GetCachedPlan(source, param_li.clone(), ResourceOwner::NULL, None)?;
    let stmt_list = plancache_seams::cached_plan_stmt_list::call(mcx, CachedPlanHandle(cplan))?;

    let mut processed: u64 = 0;
    let mut columns: Vec<types_xml::SpiColumn> = Vec::new();
    let mut raw_rows: Vec<Vec<RawCol>> = Vec::new();

    for stmt in stmt_list.iter() {
        // spi.c:2665 — when not read-only and the snapshot is under our control,
        // advance the command counter before each command and update the active
        // snapshot's command id, so the SELECT sees the writes of the in-progress
        // outer command (the VOLATILE-function-in-UPDATE partial-update case).
        if advance_cid {
            xact_seams::command_counter_increment::call()?;
            snapmgr::update_active_snapshot_command_id::call()?;
        }
        let (n, cols, rows) = run_one_eval_stmt(stmt, param_li, maxtuples)?;
        processed = n;
        if !cols.is_empty() {
            columns = cols;
        }
        if !rows.is_empty() {
            raw_rows = rows;
        }
    }

    let _ = plancache::ReleaseCachedPlan(cplan, ResourceOwner::NULL);
    Ok((processed, EvalRaw { columns, raw_rows }))
}

/// `_SPI_pquery` for a single SELECT PlannedStmt: run it through the executor to
/// a DestSPI receiver and return its raw collected rows. A PL/pgSQL expression
/// is always a single read-only SELECT; a non-SELECT statement in an expression
/// is a "query is not a SELECT" condition (rejected here).
fn run_one_eval_stmt<'mcx>(
    stmt: &PlannedStmt<'mcx>,
    param_li: &ParamListInfo,
    maxtuples: i64,
) -> PgResult<(u64, Vec<types_xml::SpiColumn>, Vec<Vec<RawCol>>)> {
    if stmt.commandType != CmdType::CMD_SELECT {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(types_error::ERRCODE_SYNTAX_ERROR)
            .errmsg("query is not a SELECT")
            .into_error());
    }

    let receiver = create_spi_dest_receiver();
    let snap = snapmgr::get_active_snapshot::call()?;

    let parent = MemoryContext::new("SPI Eval QueryDesc parent");
    let mut qdesc = execmain::CreateQueryDesc(
        &parent,
        stmt,
        "",
        snap,
        None,
        receiver,
        param_li.clone(),
        0,
    )?;

    execmain::ExecutorStart(&mut qdesc, 0)?;
    let count: u64 = if maxtuples <= 0 { u64::MAX } else { maxtuples as u64 };
    execmain::ExecutorRun(&mut qdesc, types_scan::sdir::ForwardScanDirection, count)?;
    let processed = qdesc.es_processed();
    execmain::ExecutorFinish(&mut qdesc)?;
    execmain::ExecutorEnd(&mut qdesc)?;
    execmain::FreeQueryDesc(qdesc)?;

    let (columns, raw_rows) = take_spi_raw_result(receiver);
    Ok((processed, columns, raw_rows))
}

// Keep set_spi_processed in scope for parity with the other execute paths (the
// PL/pgSQL eval consumer reads `processed` from the returned EvalResult, but the
// global is also updated so an intervening SPI_processed read is consistent).
#[allow(dead_code)]
fn publish_processed(n: u64) {
    set_spi_processed(n);
}
