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
    let out = run_execsql(mcx, source, &param_li, into, tcount, advance_cid);
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
fn prepare_execsql_plan<'mcx>(
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
fn build_param_list(
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
    tcount: i64,
    advance_cid: bool,
) -> PgResult<ExecsqlResult> {
    let cplan = plancache::GetCachedPlan(source, param_li.clone(), ResourceOwner::NULL, None)?;
    let stmt_list = plancache_seams::cached_plan_stmt_list::call(mcx, CachedPlanHandle(cplan))?;

    let mut code: i32 = SPI_OK_SELECT;
    let mut processed: u64 = 0;
    let mut returned_tuptable = false;
    let mut first_row: Vec<ExecsqlColumn> = Vec::new();

    for stmt in stmt_list.iter() {
        // spi.c:2665 — advance the command counter before each command and update
        // the active snapshot's command id, so writes of an earlier command (or
        // the statement that queued a now-firing AFTER trigger) are visible.
        if advance_cid {
            xact_seams::command_counter_increment::call()?;
            snapmgr::update_active_snapshot_command_id::call()?;
        }
        let (c, n, tt, row) = run_one_execsql_stmt(stmt, param_li, into, tcount)?;
        code = c;
        processed = n;
        returned_tuptable = tt;
        if !row.is_empty() {
            first_row = row;
        }
    }

    let _ = plancache::ReleaseCachedPlan(cplan, ResourceOwner::NULL);
    Ok(ExecsqlResult {
        code,
        processed,
        returned_tuptable,
        first_row,
    })
}

/// `_SPI_pquery` for one PlannedStmt: classify the SPI code from the command
/// type, run it through the executor to a DestSPI receiver, and collect the
/// first row's raw columns (for INTO).
fn run_one_execsql_stmt<'mcx>(
    stmt: &PlannedStmt<'mcx>,
    param_li: &ParamListInfo,
    into: bool,
    tcount: i64,
) -> PgResult<(i32, u64, bool, Vec<ExecsqlColumn>)> {
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
        CmdType::CMD_UTILITY => SPI_OK_UTILITY,
        _ => SPI_ERROR_OPUNKNOWN,
    };

    if code == SPI_ERROR_OPUNKNOWN {
        return Ok((code, 0, false, Vec::new()));
    }

    // A utility statement goes through ProcessUtility (the non-SELECT utility
    // leg). PL/pgSQL embedded utility statements (e.g. CREATE TABLE inside a
    // function body) reach the utility executor, which is the separate ported
    // ProcessUtility slow path; for the DML/SELECT shapes here it does not occur.
    if operation == CmdType::CMD_UTILITY {
        return Err(backend_utils_error::ereport(ERROR)
            .errmsg_internal(
                "PL/pgSQL embedded utility statement not supported on the prepared-plan path",
            )
            .into_error());
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

    // For INTO, hand back the first row's raw columns.
    let first_row: Vec<ExecsqlColumn> = if into && returns_rows {
        match raw_rows.first() {
            Some(row) => row
                .iter()
                .enumerate()
                .map(|(i, col): (usize, &RawCol)| ExecsqlColumn {
                    value: col.value,
                    isnull: col.isnull,
                    typeid: columns.get(i).map(|c| c.typeid).unwrap_or(types_core::InvalidOid),
                    name: columns.get(i).map(|c| c.name.clone()).unwrap_or_default(),
                    byref: col.byref.clone(),
                })
                .collect(),
            None => Vec::new(),
        }
    } else {
        Vec::new()
    };

    Ok((code, processed, returned_tuptable, first_row))
}
