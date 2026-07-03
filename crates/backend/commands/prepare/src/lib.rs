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

    let filled = fill_plansource(plansource, source_text, stmt, stmt_location);
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
    stmt: &PrepareStmt<'_>,
    stmt_location: ParseLoc,
) -> PgResult<()> {
    if !stmt.argtypes.is_nil() {
        panic!(
            "PrepareQuery (prepare.c): declared argtypes need typenameTypeId + \
             pg_analyze_and_rewrite_varparams (varparams lane)"
        );
    }
    if has_dollar_param(source_text) {
        panic!(
            "PrepareQuery (prepare.c): $n parameters need \
             pg_analyze_and_rewrite_varparams (varparams lane)"
        );
    }

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

    let query_list = postgres::pg_analyze_and_rewrite_fixedparams(
        qmcx,
        &inner,
        source_text,
        &[],
        QueryEnvHandle::NULL,
    )?;

    plancache::CompleteCachedPlan(plansource, query_list, &[], CURSOR_OPT_PARALLEL_OK, true)
}

fn has_dollar_param(text: &str) -> bool {
    let b = text.as_bytes();
    (1..b.len()).any(|i| b[i - 1] == b'$' && b[i].is_ascii_digit())
}

pub fn ExecuteQuery<'mcx>(
    stmt: &ExecuteStmt<'_>,
    params: ParamListHandle,
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

    if plancache::CachedPlanNumParams(entry.plansource) > 0 {
        panic!(
            "ExecuteQuery (prepare.c): EvaluateParams needs transformExpr/\
             coerce_to_target_type + a real ParamListInfo (execute-params lane)"
        );
    }

    let portal = portalmem::CreateNewPortal()?;
    portal.borrow_mut().visible = false;

    let query_string = plancache::CachedPlanQueryString(entry.plansource);
    let cplan = plancache::GetCachedPlan(entry.plansource, params, None, QueryEnvHandle::NULL)?;
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

    pquery::PortalStart(&portal, params, 0, Some(snapmgr::GetActiveSnapshot()))?;

    let _ = pquery::PortalRun(&portal, FETCH_ALL, false, dest, None, qc)?;

    portalmem::PortalDrop(&portal, false)?;
    pquery::stmt_list::free(stmts);

    Ok(())
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

pub fn ExplainExecuteQuery() -> ! {
    panic!("ExplainExecuteQuery (prepare.c): EXPLAIN EXECUTE is the explain lane");
}

pub fn pg_prepared_statement() -> ! {
    panic!("pg_prepared_statement (prepare.c): SRF machinery is the funcapi lane");
}
