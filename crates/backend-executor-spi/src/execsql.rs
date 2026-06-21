//! The PL/pgSQL embedded-SQL execution path (`pl_exec.c`'s `exec_stmt_execsql`
//! core: `exec_prepare_plan` + `setup_param_list` +
//! `SPI_execute_plan_with_paramlist`, specialized to the statement an
//! `INSERT`/`UPDATE`/`DELETE`/plain-`SELECT` body issues, with the bound
//! PL/pgSQL datum Params).
//!
//! This reuses the same prepare pipeline as the single-expression evaluator
//! ([`crate::eval`]): raw_parser(parseMode) + parse_analyze_plpgsql_expr (which
//! installs the PL/pgSQL parser hooks so variable barewords resolve to `$dno+1`
//! `Param`s) + QueryRewrite + CompleteCachedPlan, then GetCachedPlan with a
//! value `ParamListInfo` built from the referenced estate datums. Unlike the
//! evaluator, it runs **any** command type (SELECT / INSERT / UPDATE / DELETE),
//! classifies the SPI result code from the planned command, and — for `INTO` —
//! returns the first result row's raw columns.
//!
//! Pass-by-value result words cross faithfully; a pass-by-reference INTO column
//! is the separate by-ref-Datum keystone (the rich `Datum` payload carries it,
//! the bare-word channel does not).

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
use crate::eval::EvalParamValue;
use crate::result_code::*;

use backend_executor_execMain as execmain;
use backend_utils_cache_plancache as plancache;
use backend_utils_cache_plancache_seams as plancache_seams;
use backend_access_transam_xact_seams as xact_seams;
use backend_utils_time_snapmgr_seams as snapmgr;

type SourceHandle = u64;

/// One column of the first INTO result row: the bare-word datum, its is-null
/// flag, and the source column type OID (`SPI_gettypeid`). The typmod is not
/// carried by the SPI column descriptor, so the consumer casts at `-1` (matching
/// the single-expression evaluator's `rettypmod`).
///
/// A pass-by-reference column (a `text`/`varchar`/`numeric` value) carries its
/// verbatim header-ful varlena / cstring byte image in `byref` (the bare `value`
/// word is `0` then), `datumCopy`'d out of the receiver arena so it survives to
/// the INTO store; `None` for a by-value column.
#[derive(Clone)]
pub struct ExecsqlColumn {
    pub value: usize,
    pub isnull: bool,
    pub typeid: Oid,
    /// `SPI_fname(tupdesc, i+1)` — the result column name (for a record INTO
    /// target, this is the field name `r.<name>` resolves against).
    pub name: String,
    pub byref: Option<Vec<u8>>,
}

/// The raw result of running an embedded SQL statement: the SPI result code, the
/// row count, whether a tuple table was produced, and the first result row's
/// columns (for `INTO`).
pub struct ExecsqlResult {
    pub code: i32,
    pub processed: u64,
    pub returned_tuptable: bool,
    pub first_row: Vec<ExecsqlColumn>,
    /// All result rows' columns (for the FOR-loop / RETURN QUERY iteration path,
    /// `exec_run_select` + `exec_for_query`). Populated only when `collect_all`
    /// was requested; otherwise empty (the INTO path reads `first_row`). The
    /// materialize-all analogue of `pl_exec.c`'s portal-fetch loop: C's
    /// `exec_for_query` fetches batches of 50 rows from a held portal, but the
    /// observable iteration (every row, in order) is identical.
    pub all_rows: Vec<Vec<ExecsqlColumn>>,
}

/// Copy `s` into the arena and return a `&'mcx str`.
fn leak_str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = s.as_bytes();
    let mut v: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    v.try_reserve(bytes.len()).map_err(|_| mcx.oom(bytes.len()))?;
    v.extend_from_slice(bytes);
    let leaked: &'mcx [u8] = allocator_api2::boxed::Box::leak(v.into_boxed_slice());
    Ok(core::str::from_utf8(leaked).expect("valid utf8 from &str"))
}

/// `exec_stmt_execsql` core: prepare `query` (with the PL/pgSQL parser hooks),
/// bind the referenced datums from `resolve`, run it, and return the SPI code +
/// row count (+ first row for INTO). `tcount` caps the row count (1/2 for INTO,
/// 0 = run to completion). `_read_only` mirrors `estate->readonly_func` (the
/// snapshot push is unconditional here, as for the evaluator).
pub fn spi_execsql(
    query: &str,
    parsemode: RawParseMode,
    parse_state: PlpgsqlExprParseState,
    _read_only: bool,
    into: bool,
    tcount: i64,
    resolve: &mut dyn FnMut(i32) -> PgResult<EvalParamValue>,
) -> PgResult<ExecsqlResult> {
    spi_execsql_inner(query, parsemode, parse_state, _read_only, into, false, tcount, resolve)
}

/// `exec_run_select` materialize-all path (`pl_exec.c`): run `query`, collecting
/// **every** result row's columns into `all_rows` for the FOR-loop /
/// RETURN QUERY iteration (`exec_for_query`). Equivalent to the INTO path but
/// without the row cap, keeping all rows rather than only the first.
pub fn spi_execsql_collect(
    query: &str,
    parsemode: RawParseMode,
    parse_state: PlpgsqlExprParseState,
    read_only: bool,
    resolve: &mut dyn FnMut(i32) -> PgResult<EvalParamValue>,
) -> PgResult<ExecsqlResult> {
    spi_execsql_inner(query, parsemode, parse_state, read_only, false, true, 0, resolve)
}

/// `exec_stmt_dynexecute` / `exec_dynquery_with_params` core (`pl_exec.c`): run a
/// **dynamic** query string `query` (the runtime text after the `EXECUTE`
/// keyword) as a one-shot, optionally with `USING` parameters.
///
/// Unlike [`spi_execsql`], the query text is not a compiled PL/pgSQL expression:
/// it is analyzed with NO PL/pgSQL parser hooks (a bareword does *not* resolve
/// to a variable — only `$n` `USING` placeholders are substituted). The `USING`
/// parameters arrive already evaluated in `params` (param id `$i+1`); their type
/// OIDs drive `setup_parse_fixed_parameters` so the analyzer knows each
/// placeholder's type (C: `SPI_execute_extended` / `SPI_cursor_parse_open` with a
/// `ParamListInfo`, which feeds `fixed_paramref_hook`). `into` collects the first
/// row; `collect_all` collects every row (FOR-IN-EXECUTE); `tcount` caps the row
/// count (0 = run to completion). All command types — `SELECT`, DML, and utility
/// (DDL) — run, mirroring C's `SPI_execute_extended` switch.
pub fn spi_execsql_dynamic(
    query: &str,
    params: &[EvalParamValue],
    read_only: bool,
    into: bool,
    collect_all: bool,
    tcount: i64,
) -> PgResult<ExecsqlResult> {
    let cxt = MemoryContext::new("SPI Dynexecute");
    let mcx = cxt.mcx();
    let interned = leak_str_in(mcx, query)?;

    // _SPI_prepare_plan with fixed parameter types (the USING params' type OIDs),
    // no PL/pgSQL parser hooks. C's `_SPI_prepare_oneshot` for a dynamic query
    // analyzes with `parse_analyze_fixedparams`.
    let param_types: Vec<Oid> = params.iter().map(|p| p.typeid).collect();
    let source = prepare_dynexecute_plan(mcx, interned, &param_types)?;

    // Build the value ParamListInfo directly from the evaluated USING values
    // (C `exec_eval_using_params` -> the paramLI fed to SPI_execute_extended).
    let param_li = build_dyn_param_list(params)?;

    // _SPI_execute_plan: GetCachedPlan + push the transaction snapshot + run.
    let pushed = snapmgr::push_active_snapshot_transaction::call().is_ok();
    let advance_cid = pushed && !read_only;
    let out = run_execsql(mcx, source, &param_li, into, collect_all, tcount, advance_cid);
    if pushed {
        let _ = snapmgr::pop_active_snapshot::call();
    }
    let result = out;

    let _ = plancache::DropCachedPlan(source);

    let result = result?;
    set_spi_processed(result.processed);
    Ok(result)
}

/// `_SPI_prepare_plan` for a dynamic query string: raw_parser(default) ->
/// CreateCachedPlan + `parse_analyze_fixedparams` (the USING param types, no
/// PL/pgSQL hooks) + QueryRewrite + CompleteCachedPlan. Returns the completed
/// source handle.
pub(crate) fn prepare_dynexecute_plan<'mcx>(
    mcx: Mcx<'mcx>,
    query: &'mcx str,
    param_types: &[Oid],
) -> PgResult<SourceHandle> {
    // A dynamic EXECUTE string is parsed as a complete top-level statement.
    let raw_list = backend_parser_driver::raw_parser(mcx, query, RawParseMode::RAW_PARSE_DEFAULT)?;

    if raw_list.len() != 1 {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("cannot insert multiple commands into a prepared statement")
            .into_error());
    }
    let parsetree = &raw_list[0];

    let command_tag = backend_tcop_utility_seams::create_command_tag::call(&parsetree.stmt)?;
    let plansource = plancache::CreateCachedPlan(parsetree, query, command_tag)?;

    let query_node = backend_parser_analyze::parse_analyze_fixedparams(
        mcx,
        parsetree,
        query,
        param_types,
    )?;
    let querytree_list = backend_rewrite_rewritehandler::QueryRewrite(mcx, query_node)?;

    plancache::CompleteCachedPlan(
        plansource,
        &querytree_list,
        param_types,
        param_types.len() as i32,
        false, // has_parser_setup (fixed param types, not a parser hook)
        types_nodes::copy_query::CURSOR_OPT_PARALLEL_OK
            | types_nodes::portalcmds::CURSOR_OPT_GENERIC_PLAN,
        false, // fixed_result
    )?;

    Ok(plansource)
}

/// `exec_eval_using_params` result -> `ParamListInfo` (`pl_exec.c`): build the
/// value param list from the already-evaluated USING values, param id `$i+1`
/// (index `i`). Returns `None` when there are no params (C's NIL fast path).
pub(crate) fn build_dyn_param_list(params: &[EvalParamValue]) -> PgResult<ParamListInfo> {
    if params.is_empty() {
        return Ok(None);
    }

    let num_params = params.len();

    let ctx: &'static MemoryContext = allocator_api2::boxed::Box::leak(
        allocator_api2::boxed::Box::new(MemoryContext::new("PL/pgSQL Dyn Param List")),
    );
    let pmcx: Mcx<'static> = ctx.mcx();

    let mut plist: Vec<ParamExternData<'static>> = Vec::with_capacity(num_params);
    for p in params {
        // A pass-by-reference value (text/varchar/numeric/...) carries its
        // header-ful byte image in `byref`; rebuild it as a live `Datum::ByRef`
        // rooted in the param-list context.
        let value = match &p.byref {
            Some(bytes) if !p.isnull => RichDatum::from_byref_bytes_in(pmcx, bytes)?,
            _ => RichDatum::from_usize(p.value),
        };
        plist.push(ParamExternData {
            value,
            isnull: p.isnull,
            // exec_eval_using_params always marks USING params PARAM_FLAG_CONST
            // (they are only used with one-shot plans).
            pflags: PARAM_FLAG_CONST,
            ptype: p.typeid,
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
        num_params: num_params as i32,
        params: plist,
    })))
}

#[allow(clippy::too_many_arguments)]
fn spi_execsql_inner(
    query: &str,
    parsemode: RawParseMode,
    parse_state: PlpgsqlExprParseState,
    _read_only: bool,
    into: bool,
    collect_all: bool,
    tcount: i64,
    resolve: &mut dyn FnMut(i32) -> PgResult<EvalParamValue>,
) -> PgResult<ExecsqlResult> {
    let cxt = MemoryContext::new("SPI Execsql");
    let mcx = cxt.mcx();
    let interned = leak_str_in(mcx, query)?;

    // _SPI_prepare_plan with the PL/pgSQL parser setup.
    let source = prepare_execsql_plan(mcx, interned, parsemode, parse_state.clone())?;

    // setup_param_list: build the value ParamListInfo from the referenced datums.
    let param_li = build_param_list(&parse_state, resolve)?;

    // _SPI_execute_plan: GetCachedPlan + push the transaction snapshot + run.
    let pushed = snapmgr::push_active_snapshot_transaction::call().is_ok();
    // Advance the command counter before each command and update the snapshot
    // when not read-only and the snapshot is under our control (spi.c:2665) — so
    // a later statement in the same function/trigger sees the writes of an
    // earlier one (and a fired AFTER trigger sees the triggering statement's
    // effects). Skipped for a read-only function (it makes no writes to see).
    let advance_cid = pushed && !_read_only;
    let out = run_execsql(mcx, source, &param_li, into, collect_all, tcount, advance_cid);
    if pushed {
        let _ = snapmgr::pop_active_snapshot::call();
    }
    let result = out;

    let _ = plancache::DropCachedPlan(source);

    let result = result?;
    set_spi_processed(result.processed);
    Ok(result)
}

/// `_SPI_prepare_plan` for an embedded SQL statement: raw_parser(parsemode) ->
/// CreateCachedPlan + parse_analyze_plpgsql_expr + QueryRewrite +
/// CompleteCachedPlan. Returns the completed source handle.
pub(crate) fn prepare_execsql_plan<'mcx>(
    mcx: Mcx<'mcx>,
    query: &'mcx str,
    parsemode: RawParseMode,
    parse_state: PlpgsqlExprParseState,
) -> PgResult<SourceHandle> {
    let raw_list = backend_parser_driver::raw_parser(mcx, query, parsemode)?;

    if raw_list.len() != 1 {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("PL/pgSQL embedded SQL did not yield a single query")
            .into_error());
    }
    let parsetree = &raw_list[0];

    let command_tag = backend_tcop_utility_seams::create_command_tag::call(&parsetree.stmt)?;
    let plansource = plancache::CreateCachedPlan(parsetree, query, command_tag)?;

    let query_node =
        backend_parser_analyze::parse_analyze_plpgsql_expr(mcx, parsetree, query, parse_state)?;
    let querytree_list = backend_rewrite_rewritehandler::QueryRewrite(mcx, query_node)?;

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

    Ok(plansource)
}

/// `setup_param_list(estate, expr)`: build a value `ParamListInfo` covering the
/// referenced datum numbers (param id `dno+1`, vector indexed by `dno`).
pub(crate) fn build_param_list(
    parse_state: &PlpgsqlExprParseState,
    resolve: &mut dyn FnMut(i32) -> PgResult<EvalParamValue>,
) -> PgResult<ParamListInfo> {
    let dnos = parse_state.referenced_dnos();
    if dnos.is_empty() {
        return Ok(None);
    }

    let max_dno = *dnos.iter().max().unwrap();
    let num_params = (max_dno + 1) as usize;

    let ctx: &'static MemoryContext = allocator_api2::boxed::Box::leak(
        allocator_api2::boxed::Box::new(MemoryContext::new("PL/pgSQL Param List")),
    );
    let pmcx: Mcx<'static> = ctx.mcx();

    let mut params: Vec<ParamExternData<'static>> = Vec::with_capacity(num_params);
    let referenced: std::collections::BTreeSet<i32> = dnos.iter().copied().collect();
    for dno in 0..(num_params as i32) {
        if referenced.contains(&dno) {
            let v = resolve(dno)?;
            // A pass-by-reference value (varchar/text/name/numeric/...) carries
            // its header-ful byte image in `byref`; rebuild it as a live
            // `Datum::ByRef` rooted in the param-list context. The bare `value`
            // word is only meaningful for by-value types (byref == None).
            let value = match &v.byref {
                Some(bytes) if !v.isnull => RichDatum::from_byref_bytes_in(pmcx, bytes)?,
                _ => RichDatum::from_usize(v.value),
            };
            params.push(ParamExternData {
                value,
                isnull: v.isnull,
                pflags: PARAM_FLAG_CONST,
                ptype: v.typeid,
            });
        } else {
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

/// `_SPI_execute_plan`: GetCachedPlan, run each PlannedStmt, return the last
/// statement's classification + row count (+ first row for INTO).
fn run_execsql<'mcx>(
    mcx: Mcx<'mcx>,
    source: SourceHandle,
    param_li: &ParamListInfo,
    into: bool,
    collect_all: bool,
    tcount: i64,
    advance_cid: bool,
) -> PgResult<ExecsqlResult> {
    let cplan = plancache::GetCachedPlan(source, param_li.clone(), ResourceOwner::NULL, None)?;
    let stmt_list = plancache_seams::cached_plan_stmt_list::call(mcx, CachedPlanHandle(cplan))?;

    let mut code: i32 = SPI_OK_SELECT;
    let mut processed: u64 = 0;
    let mut returned_tuptable = false;
    let mut first_row: Vec<ExecsqlColumn> = Vec::new();
    let mut all_rows: Vec<Vec<ExecsqlColumn>> = Vec::new();

    for stmt in stmt_list.iter() {
        // spi.c:2665 — advance the command counter before each command and update
        // the active snapshot's command id, so writes of an earlier command (or
        // the statement that queued a now-firing AFTER trigger) are visible.
        if advance_cid {
            xact_seams::command_counter_increment::call()?;
            snapmgr::update_active_snapshot_command_id::call()?;
        }
        let (c, n, tt, row, rows) =
            run_one_execsql_stmt(stmt, param_li, into, collect_all, tcount)?;
        code = c;
        processed = n;
        returned_tuptable = tt;
        if !row.is_empty() {
            first_row = row;
        }
        if !rows.is_empty() {
            all_rows = rows;
        }
    }

    let _ = plancache::ReleaseCachedPlan(cplan, ResourceOwner::NULL);
    Ok(ExecsqlResult {
        code,
        processed,
        returned_tuptable,
        first_row,
        all_rows,
    })
}

/// `_SPI_pquery` for one PlannedStmt: classify the SPI code from the command
/// type, run it through the executor to a DestSPI receiver, and collect the
/// first row's raw columns (for INTO).
#[allow(clippy::type_complexity)]
fn run_one_execsql_stmt<'mcx>(
    stmt: &PlannedStmt<'mcx>,
    param_li: &ParamListInfo,
    into: bool,
    collect_all: bool,
    tcount: i64,
) -> PgResult<(i32, u64, bool, Vec<ExecsqlColumn>, Vec<Vec<ExecsqlColumn>>)> {
    let operation = stmt.commandType;

    // _SPI_pquery: derive the SPI result code from the planned command type.
    let code = match operation {
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
        CmdType::CMD_MERGE => {
            if stmt.hasReturning {
                SPI_OK_MERGE_RETURNING
            } else {
                SPI_OK_MERGE
            }
        }
        CmdType::CMD_UTILITY => SPI_OK_UTILITY,
        _ => SPI_ERROR_OPUNKNOWN,
    };

    if code == SPI_ERROR_OPUNKNOWN {
        return Ok((code, 0, false, Vec::new(), Vec::new()));
    }

    // A utility statement (e.g. EXPLAIN, or an embedded CREATE/DROP inside a
    // PL/pgSQL function body) goes through ProcessUtility, not the executor
    // (C `_SPI_execute_plan`: `if (stmt->utilityStmt != NULL)` →
    // `ProcessUtility(stmt, …, dest, &qc)`). A row-returning utility (EXPLAIN /
    // SHOW / a PORTAL_UTIL_SELECT) writes its tuples into the SPI dest receiver
    // exactly as a SELECT would, so the FOR-loop / RETURN QUERY / INTO callers
    // collect them the same way.
    if operation == CmdType::CMD_UTILITY {
        let receiver = create_spi_dest_receiver();

        // The per-utility working context (C: CurrentMemoryContext during the
        // portal's utility run). standard_ProcessUtility's readOnlyTree
        // copyObject + make_parsestate allocations live here; the context is
        // dropped when this returns. Nothing the dispatch returns escapes it
        // (qc is owned, the rows are copied out of the receiver below).
        let ucx = MemoryContext::new("SPI Execsql ProcessUtility");
        let ucx_mcx = ucx.mcx();
        let mut qc = types_portal::QueryCompletion::default();

        // Copy the PlannedStmt into the per-utility scratch context so the
        // statement and the working `mcx` share its lifetime (the owned analogue
        // of C's stable `PlannedStmt *`); a row-returning utility routes its rows
        // to the SPI receiver, never back into the caller's plan storage.
        let pstmt = stmt.clone_in(ucx_mcx)?;

        // C passes PROCESS_UTILITY_QUERY (a plain function-body call is the
        // atomic context).
        backend_tcop_utility_seams::process_utility::call(
            ucx_mcx,
            &pstmt,
            "",
            true, // readOnlyTree: protect the plancache's node tree (C true)
            types_nodes::parsestmt::ProcessUtilityContext::PROCESS_UTILITY_QUERY,
            param_li.clone(),
            receiver,
            &mut qc,
        )?;

        let (columns, raw_rows) = take_spi_raw_result(receiver);

        // C: `res = SPI_OK_UTILITY;` and `_SPI_current->processed =
        // _SPI_current->tuptable->numvals` when the utility returned tuples.
        let processed = raw_rows.len() as u64;

        // A row-returning utility (EXPLAIN/SHOW/etc.) leaves a tuple table, just
        // like a SELECT; classify it as row-returning so the callers collect.
        let returns_rows = !columns.is_empty();
        let returned_tuptable = returns_rows;

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

        let first_row: Vec<ExecsqlColumn> = if into && returns_rows {
            match raw_rows.first() {
                Some(row) => project_row(row),
                None => Vec::new(),
            }
        } else {
            Vec::new()
        };
        let all_rows: Vec<Vec<ExecsqlColumn>> = if collect_all && returns_rows {
            raw_rows.iter().map(project_row).collect()
        } else {
            Vec::new()
        };

        return Ok((
            SPI_OK_UTILITY,
            processed,
            returned_tuptable,
            first_row,
            all_rows,
        ));
    }

    // A row-returning statement (SELECT, or DML RETURNING) collects into a
    // DestSPI receiver; a plain DML statement still runs to a (throwaway)
    // receiver and reports es_processed.
    let receiver = create_spi_dest_receiver();
    let snap = snapmgr::get_active_snapshot::call()?;

    let parent = MemoryContext::new("SPI Execsql QueryDesc parent");
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
    let count: u64 = if tcount <= 0 { u64::MAX } else { tcount as u64 };
    execmain::ExecutorRun(&mut qdesc, types_scan::sdir::ForwardScanDirection, count)?;
    let processed = qdesc.es_processed();
    execmain::ExecutorFinish(&mut qdesc)?;
    execmain::ExecutorEnd(&mut qdesc)?;
    execmain::FreeQueryDesc(qdesc)?;

    let (columns, raw_rows) = take_spi_raw_result(receiver);

    // A tuple table is produced when the statement returns rows (SELECT, or DML
    // with RETURNING). `SPI_tuptable != NULL` in C corresponds to a row-returning
    // operation; a plain DML statement leaves SPI_tuptable NULL.
    let returns_rows = matches!(operation, CmdType::CMD_SELECT) || stmt.hasReturning;
    let returned_tuptable = returns_rows;

    // Project one raw row's columns into the ExecsqlColumn shape (the per-column
    // type OID + name come from the tuple descriptor).
    let project_row = |row: &Vec<RawCol>| -> Vec<ExecsqlColumn> {
        row.iter()
            .enumerate()
            .map(|(i, col): (usize, &RawCol)| ExecsqlColumn {
                value: col.value,
                isnull: col.isnull,
                typeid: columns.get(i).map(|c| c.typeid).unwrap_or(types_core::InvalidOid),
                name: columns.get(i).map(|c| c.name.clone()).unwrap_or_default(),
                byref: col.byref.clone(),
            })
            .collect()
    };

    // For INTO, hand back the first row's raw columns.
    let first_row: Vec<ExecsqlColumn> = if into && returns_rows {
        match raw_rows.first() {
            Some(row) => project_row(row),
            None => Vec::new(),
        }
    } else {
        Vec::new()
    };

    // For the FOR-loop / RETURN QUERY iteration (`exec_for_query`), hand back
    // every result row's columns (the materialize-all analogue of C's portal
    // fetch loop).
    let all_rows: Vec<Vec<ExecsqlColumn>> = if collect_all && returns_rows {
        raw_rows.iter().map(project_row).collect()
    } else {
        Vec::new()
    };

    Ok((code, processed, returned_tuptable, first_row, all_rows))
}
