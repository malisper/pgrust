// prepare.c. prepared_queries is a PgFxHashMap in a leaked backend-lifetime
// context (C's per-backend dynahash under CacheMemoryContext). Loud arms:
// declared argtypes / $n parameters (varparams + EvaluateParams lanes),
// EXPLAIN EXECUTE, CREATE TABLE AS EXECUTE, pg_prepared_statement SRF.
#![allow(non_snake_case)]

use core::cell::RefCell;
use std::rc::Rc;

use elog::ereport;
use mcx::{Mcx, MemoryContext, PgHashMap, PgString};
use plancache::CachedPlanSourceHandle;
use tcop_dest::DestReceiver;
use types_core::TimestampTz;
use types_error::{
    PgResult, ERRCODE_DUPLICATE_PSTATEMENT, ERRCODE_INVALID_PSTATEMENT_DEFINITION,
    ERRCODE_UNDEFINED_PSTATEMENT, ERROR,
};
use types_nodes::parsenodes::{DeallocateStmt, ExecuteStmt, PrepareStmt};
use types_nodes::rawnodes::RawStmt;
use types_core::ParseLoc;
use types_portal::{ParamListHandle, QueryCompletion, QueryEnvHandle, CURSOR_OPT_PARALLEL_OK, FETCH_ALL};

pub fn init_seams() {
    prepare_seams::store_prepared_statement::set(StorePreparedStatement);
    prepare_seams::fetch_prepared_statement_plansource::set(|stmt_name, throw_error| {
        Ok(FetchPreparedStatement(stmt_name, throw_error)?.map(|p| p.plansource))
    });
    prepare_seams::drop_prepared_statement::set(DropPreparedStatement);
}

#[derive(Clone, Copy)]
pub struct PreparedStatement {
    pub plansource: CachedPlanSourceHandle,
    pub from_sql: bool,
    pub prepare_time: TimestampTz,
}

struct QueryTable {
    mcx: Mcx<'static>,
    map: PgHashMap<'static, PgString<'static>, PreparedStatement>,
}

thread_local! {
    static PREPARED_QUERIES: RefCell<Option<QueryTable>> = const { RefCell::new(None) };
}

fn with_table<R>(f: impl FnOnce(&mut QueryTable) -> R) -> R {
    PREPARED_QUERIES.with(|t| {
        let mut t = t.borrow_mut();
        let table = t.get_or_insert_with(|| {
            let mcx = Box::leak(Box::new(MemoryContext::new("PreparedQueries"))).mcx();
            QueryTable { mcx, map: PgHashMap::with_capacity_in(32, mcx) }
        });
        f(table)
    })
}

pub fn PrepareQuery(
    source_text: &str,
    stmt: &PrepareStmt<'_>,
    stmt_location: ParseLoc,
    stmt_len: ParseLoc,
) -> PgResult<()> {
    let name = match stmt.name {
        Some(n) if !n.is_empty() => n,
        _ => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PSTATEMENT_DEFINITION)
                .errmsg("invalid statement name: must not be empty")
                .into_error()
                .into())
        }
    };

    let query = stmt.query.expect("PREPARE has a query");
    let rawstmt = RawStmt { stmt: Some(query), stmt_location, stmt_len };
    let tag = utility_seams::create_command_tag::call(query);
    let plansource = plancache::CreateCachedPlan(Some(&rawstmt), source_text, tag)?;

    let filled = fill_plansource(plansource, source_text, stmt_location);
    if let Err(e) = filled {
        // C leaves the transient plansource to transaction-abort cleanup; the
        // registry has no abort hook yet, so reclaim it here.
        plancache::DropCachedPlan(plansource);
        return Err(e);
    }

    let stored = StorePreparedStatement(name, plansource, true);
    if let Err(e) = stored {
        plancache::DropCachedPlan(plansource);
        return Err(e);
    }
    Ok(())
}

fn fill_plansource(
    plansource: CachedPlanSourceHandle,
    source_text: &str,
    stmt_location: ParseLoc,
) -> PgResult<()> {
    // C copyObject-retains the message-arena raw tree; here the statement is
    // re-parsed into the plansource's query arena (once per PREPARE, cold).
    let qmcx = plancache::SourceQueryMcx(plansource);
    let raw_list = parser_seams::raw_parser::call(
        qmcx,
        source_text,
        parser_seams::RawParseMode::RAW_PARSE_DEFAULT,
    )?;
    let reparsed = raw_list
        .iter()
        .find(|r| {
            r.stmt_location == stmt_location
                && r.stmt.map(|s| s.node_tag()) == Some(types_nodes::NodeTag::T_PrepareStmt)
        })
        .and_then(|r| r.stmt)
        .and_then(|s| s.as_prepare_stmt())
        .expect("re-parse reproduces the PREPARE statement");
    let inner = RawStmt {
        stmt: Some(reparsed.query.expect("PREPARE has a query")),
        stmt_location,
        stmt_len: 0,
    };

    let mut argtypes: mcx::PgVec<'_, types_core::Oid> =
        mcx::vec_with_capacity_in(qmcx, reparsed.argtypes.len())?;
    for tn_node in reparsed.argtypes.iter() {
        let tn = tn_node.as_type_name().expect("PREPARE argtypes are TypeNames");
        argtypes.push(parse_utilcmd::typenameTypeIdAndMod(qmcx, tn)?.0);
    }

    let (query_list, resolved) = postgres::pg_analyze_and_rewrite_varparams(
        qmcx,
        &inner,
        source_text,
        &argtypes,
        QueryEnvHandle::NULL,
    )?;

    plancache::CompleteCachedPlan(plansource, query_list, &resolved, CURSOR_OPT_PARALLEL_OK, true)
}

pub fn ExecuteQuery<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &ExecuteStmt<'mcx>,
    source_text: &str,
    // C threads the caller's params into the EState for nested references;
    // evaluate_expr has no binding, so they are unused here (loud in interp).
    _params: ParamListHandle,
    dest: &mut DestReceiver<'mcx>,
    qc: Option<&mut QueryCompletion>,
) -> PgResult<()> {
    let name = stmt.name.expect("EXECUTE has a name");
    let entry = FetchPreparedStatement(name, true)?.expect("throwError returned entry");

    if !plancache::CachedPlanFixedResult(entry.plansource) {
        return Err(ereport(ERROR)
            .errmsg("EXECUTE does not support variable-result cached plans")
            .into_error()
            .into());
    }

    let param_li = if plancache::CachedPlanNumParams(entry.plansource) > 0 {
        EvaluateParams(mcx, &entry, name, &stmt.params, source_text)?
    } else {
        ParamListHandle::NULL
    };

    let portal = portalmem::CreateNewPortal()?;
    portal.borrow_mut().visible = false;

    let query_string = plancache::CachedPlanQueryString(entry.plansource);
    let cplan = plancache::GetCachedPlan(entry.plansource, param_li, None, QueryEnvHandle::NULL)?;
    let stmt_slice = plancache::CachedPlanStmtList(cplan);
    // SAFETY: the cplan refcount taken by GetCachedPlan pins stmt_slice until
    // PortalDrop releases it; the handle is freed right after.
    let stmts = unsafe { pquery::stmt_list::register(stmt_slice) };
    // No fallible call between GetCachedPlan and PortalDefineQuery (C's
    // refcount-leak rule).
    portalmem::PortalDefineQuery(
        &portal,
        None,
        query_string,
        plancache::CachedPlanCommandTag(entry.plansource),
        stmts,
        cplan,
    )?;

    pquery::PortalStart(&portal, param_li, 0, Some(snapmgr::GetActiveSnapshot()))?;

    let _ = pquery::PortalRun(&portal, FETCH_ALL, false, dest, None, qc)?;

    portalmem::PortalDrop(&portal, false)?;
    pquery::stmt_list::free(stmts);
    types_portal::params::free(param_li);

    Ok(())
}

// EvaluateParams (prepare.c). Divergences: expression evaluation rides
// execexpr::evaluate_expr (no EState), so a parameter expression that itself
// references an outer $n has no binding and fails loudly in the interpreter.
fn EvaluateParams<'mcx>(
    mcx: Mcx<'mcx>,
    entry: &PreparedStatement,
    stmt_name: &str,
    params_list: &types_nodes::NodeList<'mcx>,
    source_text: &str,
) -> PgResult<ParamListHandle> {
    let param_types = plancache::CachedPlanParamTypes(entry.plansource);
    let num_params = param_types.len();
    let nparams = params_list.len();

    if nparams != num_params {
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_SYNTAX_ERROR)
            .errmsg(format!(
                "wrong number of parameters for prepared statement \"{stmt_name}\""
            ))
            .errdetail(format!("Expected {num_params} parameters but got {nparams}."))
            .into_error()
            .into());
    }
    if num_params == 0 {
        return Ok(ParamListHandle::NULL);
    }

    let mut pstate = parser_small1::make_parsestate(mcx, None);
    pstate.p_sourcetext = Some(mcx::slice_in(mcx, source_text.as_bytes())?.leak());

    let mut out: mcx::PgVec<'mcx, types_portal::params::ParamExternData> =
        mcx::vec_with_capacity_in(mcx, num_params)?;
    for (i, raw) in params_list.iter().enumerate() {
        let expected_type_id = param_types[i];
        let expr = parse_expr::transformExpr(
            mcx,
            &mut pstate,
            raw,
            parser_small1::ParseExprKind::EXPR_KIND_EXECUTE_PARAMETER,
        )?;
        let given_type_id = parse_expr::expr_type(expr);
        let coerced = coerce::coerce_to_target_type(
            mcx,
            &pstate,
            expr,
            given_type_id,
            expected_type_id,
            -1,
            coerce::CoercionContext::COERCION_ASSIGNMENT,
            types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        )?;
        let Some(coerced) = coerced else {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!(
                    "parameter ${} of type {} cannot be coerced to the expected type {}",
                    i + 1,
                    format_type::format_type_be(given_type_id)?,
                    format_type::format_type_be(expected_type_id)?,
                ))
                .errhint("You will need to rewrite or cast the expression.")
                .errposition(parser_small1::parser_errposition(
                    &pstate,
                    parse_expr::expr_location(expr),
                    mbutils::GetDatabaseEncoding(),
                ))
                .into_error()
                .into());
        };
        parse_collate::assign_expr_collations(mcx, &pstate, coerced)?;

        let evaluated = execexpr::evaluate_expr(
            mcx,
            coerced,
            parse_expr::expr_type(coerced),
            parse_expr::expr_typmod(coerced),
            parse_expr::expr_collation(coerced),
        )?;
        let c = evaluated.as_const().expect("evaluate_expr returns a Const");
        out.push(types_portal::params::ParamExternData {
            value: c.constvalue,
            isnull: c.constisnull,
            pflags: types_portal::params::PARAM_FLAG_CONST,
            ptype: expected_type_id,
        });
    }
    parser_small1::free_parsestate(pstate)?;

    // SAFETY: the slice is mcx-leaked (statement lifetime); ExecuteQuery
    // frees the handle after PortalDrop, inside that lifetime.
    Ok(unsafe { types_portal::params::register(out.leak()) })
}

pub fn StorePreparedStatement(
    stmt_name: &str,
    plansource: CachedPlanSourceHandle,
    from_sql: bool,
) -> PgResult<()> {
    let cur_ts = xact::GetCurrentStatementStartTimestamp();
    let inserted = with_table(|t| -> PgResult<bool> {
        if t.map.contains_key(stmt_name) {
            return Ok(false);
        }
        let key = PgString::from_str_in(stmt_name, t.mcx)?;
        t.map.insert(key, PreparedStatement { plansource, from_sql, prepare_time: cur_ts });
        Ok(true)
    })?;
    if !inserted {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_PSTATEMENT)
            .errmsg(format!("prepared statement \"{stmt_name}\" already exists"))
            .into_error()
            .into());
    }
    plancache::SaveCachedPlan(plansource)
}

pub fn FetchPreparedStatement(
    stmt_name: &str,
    throw_error: bool,
) -> PgResult<Option<PreparedStatement>> {
    let entry = with_table(|t| t.map.get(stmt_name).copied());
    if entry.is_none() && throw_error {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_PSTATEMENT)
            .errmsg(format!("prepared statement \"{stmt_name}\" does not exist"))
            .into_error()
            .into());
    }
    Ok(entry)
}

// Fixed-result plans never change their tupdesc, so no revalidation (C).
pub fn FetchPreparedStatementResultDesc(
    stmt: &PreparedStatement,
) -> Option<Rc<types_tuple::TupleDescData<'static>>> {
    debug_assert!(plancache::CachedPlanFixedResult(stmt.plansource));
    plancache::CachedPlanResultDesc(stmt.plansource)
}

pub fn FetchPreparedStatementTargetList(_stmt: &PreparedStatement) -> ! {
    panic!(
        "FetchPreparedStatementTargetList (prepare.c): CachedPlanGetTargetList is the \
         protocol Describe lane"
    );
}

pub fn DeallocateQuery(stmt: &DeallocateStmt<'_>) -> PgResult<()> {
    match stmt.name {
        Some(name) => DropPreparedStatement(name, true),
        None => {
            DropAllPreparedStatements();
            Ok(())
        }
    }
}

pub fn DropPreparedStatement(stmt_name: &str, show_error: bool) -> PgResult<()> {
    let entry = FetchPreparedStatement(stmt_name, show_error)?;
    if let Some(entry) = entry {
        plancache::DropCachedPlan(entry.plansource);
        with_table(|t| t.map.remove(stmt_name));
    }
    Ok(())
}

pub fn DropAllPreparedStatements() {
    with_table(|t| {
        for (_, entry) in t.map.drain() {
            plancache::DropCachedPlan(entry.plansource);
        }
    });
}

// The plan renderer is injected by the explain crate (a direct dep here would
// cycle: explain deps prepare for this entry point). Called once per cached
// PlannedStmt with (pstmt, prepared query string, evaluated params,
// planduration, is_last).
pub fn ExplainExecuteQuery<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &ExecuteStmt<'mcx>,
    source_text: &str,
    _params: ParamListHandle,
    query_env: QueryEnvHandle,
    explain_one_plan: &mut dyn FnMut(
        &'static types_nodes::plannodes::PlannedStmt<'static>,
        &'static str,
        ParamListHandle,
        core::time::Duration,
        bool,
    ) -> PgResult<()>,
) -> PgResult<()> {
    let planstart = std::time::Instant::now();

    let name = stmt.name.expect("EXECUTE has a name");
    let entry = FetchPreparedStatement(name, true)?.expect("throwError returned entry");

    if !plancache::CachedPlanFixedResult(entry.plansource) {
        return Err(ereport(ERROR)
            .errmsg("EXPLAIN EXECUTE does not support variable-result cached plans")
            .into_error()
            .into());
    }
    let query_string = plancache::CachedPlanQueryString(entry.plansource);

    let param_li = if plancache::CachedPlanNumParams(entry.plansource) > 0 {
        EvaluateParams(mcx, &entry, name, &stmt.params, source_text)?
    } else {
        ParamListHandle::NULL
    };

    let cplan = plancache::GetCachedPlan(entry.plansource, param_li, None, query_env)?;
    let planduration = planstart.elapsed();

    let stmts = plancache::CachedPlanStmtList(cplan);
    let last = stmts.len().saturating_sub(1);
    let mut result = Ok(());
    for (i, pstmt) in stmts.iter().enumerate() {
        if pstmt.commandType == types_nodes::nodes_enums::CmdType::CMD_UTILITY {
            panic!(
                "ExplainExecuteQuery (prepare.c): utility statement in cached plan \
                 list (rules lane)"
            );
        }
        result = explain_one_plan(pstmt, query_string, param_li, planduration, i == last);
        if result.is_err() {
            break;
        }
    }
    plancache::ReleaseCachedPlan(cplan);
    types_portal::params::free(param_li);
    result
}

pub fn pg_prepared_statement() -> ! {
    panic!("pg_prepared_statement (prepare.c): SRF machinery is the funcapi lane");
}
