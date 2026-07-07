// prepare.c. prepared_queries is a real dynahash HTAB (HASH_STRINGS,
// NAMEDATALEN keys) because pg_prepared_statements emits rows in hash_seq
// order — byte parity with C requires C's iteration order. Loud arm:
// $n parameters inside EXECUTE parameter expressions (no EState binding).
#![allow(non_snake_case)]

use core::cell::Cell;
use core::mem::size_of;
use std::rc::Rc;

use datum::Datum;
use elog::ereport;
use mcx::Mcx;
use plancache::CachedPlanSourceHandle;
use tcop_dest::DestReceiver;
use types_core::{Oid, ParseLoc, TimestampTz};
use types_error::{
    PgResult, ERRCODE_DUPLICATE_PSTATEMENT, ERRCODE_INVALID_PSTATEMENT_DEFINITION,
    ERRCODE_UNDEFINED_PSTATEMENT, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_fmgr::{varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use types_hash::hsearch::{
    HASHCTL, HASH_ELEM, HASH_ENTER, HASH_FIND, HASH_REMOVE, HASH_SEQ_STATUS, HASH_STRINGS, HTAB,
};
use types_nodes::parsenodes::{DeallocateStmt, ExecuteStmt, PrepareStmt};
use types_nodes::rawnodes::{IntoClause, RawStmt};
use types_portal::{ParamListHandle, QueryCompletion, QueryEnvHandle, CURSOR_OPT_PARALLEL_OK, FETCH_ALL};

pub fn init_seams() {
    prepare_seams::store_prepared_statement::set(StorePreparedStatement);
    prepare_seams::fetch_prepared_statement_plansource::set(|stmt_name, throw_error| {
        Ok(FetchPreparedStatement(stmt_name, throw_error)?.map(|p| p.plansource))
    });
    prepare_seams::drop_prepared_statement::set(DropPreparedStatement);
}

pub const PREPARE_BUILTINS: &[FmgrBuiltin] = &[FmgrBuiltin {
    foid: 2510,
    name: "pg_prepared_statement",
    nargs: 0,
    strict: true,
    retset: true,
    func: pg_prepared_statement,
}];

#[derive(Clone, Copy)]
pub struct PreparedStatement {
    pub plansource: CachedPlanSourceHandle,
    pub from_sql: bool,
    pub prepare_time: TimestampTz,
}

const NAMEDATALEN: usize = types_core::fmgr::NAMEDATALEN as usize;

#[repr(C)]
struct PreparedStatementEntry {
    stmt_name: [u8; NAMEDATALEN],
    plansource: CachedPlanSourceHandle,
    from_sql: bool,
    prepare_time: TimestampTz,
}

thread_local! {
    static PREPARED_QUERIES: Cell<*mut HTAB> = const { Cell::new(core::ptr::null_mut()) };
}

fn query_hash_table() -> PgResult<*mut HTAB> {
    let hashp = PREPARED_QUERIES.with(Cell::get);
    if !hashp.is_null() {
        return Ok(hashp);
    }
    let mut info = HASHCTL::default();
    info.keysize = NAMEDATALEN;
    info.entrysize = size_of::<PreparedStatementEntry>();
    let hashp = dynahash::hash_create("Prepared Queries", 32, &info, HASH_ELEM | HASH_STRINGS)?;
    PREPARED_QUERIES.with(|c| c.set(hashp));
    Ok(hashp)
}

fn key_buf(name: &str) -> [u8; NAMEDATALEN] {
    let mut key = [0u8; NAMEDATALEN];
    let n = name.len().min(NAMEDATALEN - 1);
    key[..n].copy_from_slice(&name.as_bytes()[..n]);
    key
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

    let filled = fill_plansource(plansource, source_text, stmt);
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
    // Revalidation is plancache's fixedparams default on the retained inner
    // query tree, with the resolved param types (C's parserSetup == NULL arm).
    Ok(())
}

fn fill_plansource(
    plansource: CachedPlanSourceHandle,
    source_text: &str,
    stmt: &PrepareStmt<'_>,
) -> PgResult<()> {
    // C analyzes the message-arena inner tree in place; here analysis
    // scribbles query-arena pointers into its input, so the plansource's
    // retained copy is copied once more into the query arena (no re-lex: a
    // second lex re-emits scanner warnings C doesn't).
    let qmcx = plancache::SourceQueryMcx(plansource);
    let inner = plancache::CachedPlanRawParseTreeCopy(qmcx, plansource)?
        .expect("created with a raw tree");

    let mut pstate = parser_small1::make_parsestate(qmcx, None);
    pstate.p_sourcetext = Some(mcx::slice_in(qmcx, source_text.as_bytes())?.leak());
    let mut argtypes: mcx::PgVec<'_, types_core::Oid> =
        mcx::vec_with_capacity_in(qmcx, stmt.argtypes.len())?;
    for tn_node in stmt.argtypes.iter() {
        let tn = tn_node.as_type_name().expect("PREPARE argtypes are TypeNames");
        argtypes.push(parse_utilcmd::typenameTypeId(qmcx, Some(&pstate), tn)?);
    }
    parser_small1::free_parsestate(pstate)?;

    let (query_list, resolved) = postgres::pg_analyze_and_rewrite_varparams(
        qmcx,
        inner,
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
    into_clause: Option<&IntoClause<'mcx>>,
    dest: &mut DestReceiver<'mcx>,
    qc: Option<&mut QueryCompletion>,
) -> PgResult<()> {
    let name = stmt.name.expect("EXECUTE has a name");
    let entry = FetchPreparedStatement(name, true)?.expect("throwError returned entry");
    let info = plancache::CachedPlanSourceExecInfo(entry.plansource);

    if !info.fixed_result {
        return Err(ereport(ERROR)
            .errmsg("EXECUTE does not support variable-result cached plans")
            .into_error()
            .into());
    }

    let param_li = if info.num_params > 0 {
        EvaluateParams(mcx, &entry, name, &stmt.params, source_text)?
    } else {
        ParamListHandle::NULL
    };

    let portal = portalmem::CreateNewPortal()?;
    portal.borrow_mut().visible = false;

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
        info.query_string,
        info.commandTag,
        stmts,
        cplan,
    )?;

    // CREATE TABLE AS EXECUTE: C insists the prepared statement is a plain
    // SELECT (INSERT ... RETURNING etc. stay unsupported upstream too).
    let (eflags, count) = match into_clause {
        Some(into) => {
            let is_select = stmt_slice.len() == 1
                && stmt_slice[0].commandType == types_nodes::nodes_enums::CmdType::CMD_SELECT;
            if !is_select {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg("prepared statement is not a SELECT")
                    .into_error()
                    .into());
            }
            let eflags = createas_seams::get_into_rel_eflags::call(into.skipData);
            (eflags, if into.skipData { 0 } else { FETCH_ALL })
        }
        None => (0, FETCH_ALL),
    };

    pquery::PortalStart(&portal, param_li, eflags, Some(snapmgr::GetActiveSnapshot()))?;

    let _ = pquery::PortalRun(&portal, count, false, dest, None, qc)?;

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
    let hashp = query_hash_table()?;
    let key = key_buf(stmt_name);
    let mut found = false;
    let entry = dynahash::hash_search(hashp, key.as_ptr(), HASH_ENTER, Some(&mut found))?;
    if found {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_PSTATEMENT)
            .errmsg(format!("prepared statement \"{stmt_name}\" already exists"))
            .into_error()
            .into());
    }
    // SAFETY: dynahash returned a live PreparedStatementEntry-sized slot;
    // keycopy already wrote stmt_name.
    unsafe {
        let e = entry as *mut PreparedStatementEntry;
        (*e).plansource = plansource;
        (*e).from_sql = from_sql;
        (*e).prepare_time = cur_ts;
    }
    plancache::SaveCachedPlan(plansource)
}

pub fn FetchPreparedStatement(
    stmt_name: &str,
    throw_error: bool,
) -> PgResult<Option<PreparedStatement>> {
    let hashp = PREPARED_QUERIES.with(Cell::get);
    let entry = if hashp.is_null() {
        None
    } else {
        let key = key_buf(stmt_name);
        let entry = dynahash::hash_search(hashp, key.as_ptr(), HASH_FIND, None)?;
        // SAFETY: a non-null hit is a live PreparedStatementEntry.
        unsafe {
            (entry as *const PreparedStatementEntry).as_ref().map(|e| PreparedStatement {
                plansource: e.plansource,
                from_sql: e.from_sql,
                prepare_time: e.prepare_time,
            })
        }
    };
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

pub fn FetchPreparedStatementTargetList<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &PreparedStatement,
) -> PgResult<mcx::PgVec<'mcx, pquery::TargetEntrySummary>> {
    plancache::CachedPlanGetTargetList(mcx, stmt.plansource, QueryEnvHandle::NULL)
}

pub fn DeallocateQuery(stmt: &DeallocateStmt<'_>) -> PgResult<()> {
    match stmt.name {
        Some(name) => DropPreparedStatement(name, true),
        None => DropAllPreparedStatements(),
    }
}

pub fn DropPreparedStatement(stmt_name: &str, show_error: bool) -> PgResult<()> {
    let entry = FetchPreparedStatement(stmt_name, show_error)?;
    if let Some(entry) = entry {
        plancache::DropCachedPlan(entry.plansource);
        let key = key_buf(stmt_name);
        dynahash::hash_search(PREPARED_QUERIES.with(Cell::get), key.as_ptr(), HASH_REMOVE, None)?;
    }
    Ok(())
}

pub fn DropAllPreparedStatements() -> PgResult<()> {
    let hashp = PREPARED_QUERIES.with(Cell::get);
    if hashp.is_null() {
        return Ok(());
    }
    let mut seq = HASH_SEQ_STATUS::default();
    dynahash::hash_seq_init(&mut seq, hashp)?;
    loop {
        let entry = dynahash::hash_seq_search(&mut seq)?;
        if entry.is_null() {
            break;
        }
        // SAFETY: live entry from the seq scan; dynahash supports removal of
        // the current element mid-scan (C does exactly this).
        let e = unsafe { &*(entry as *const PreparedStatementEntry) };
        plancache::DropCachedPlan(e.plansource);
        dynahash::hash_search(hashp, e.stmt_name.as_ptr(), HASH_REMOVE, None)?;
    }
    Ok(())
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

pub fn pg_prepared_statement(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_prepared_statement: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    debug_assert_eq!(srf.tupdesc.natts, 8);

    let hashp = PREPARED_QUERIES.with(Cell::get);
    if !hashp.is_null() {
        let mut seq = HASH_SEQ_STATUS::default();
        dynahash::hash_seq_init(&mut seq, hashp)?;
        loop {
            let entry = dynahash::hash_seq_search(&mut seq)?;
            if entry.is_null() {
                break;
            }
            // SAFETY: live entry from the seq scan.
            let e = unsafe { &*(entry as *const PreparedStatementEntry) };
            let name_len =
                e.stmt_name.iter().position(|&b| b == 0).unwrap_or(NAMEDATALEN);
            let query_string = plancache::CachedPlanQueryString(e.plansource);
            let param_types = plancache::CachedPlanParamTypes(e.plansource);
            let (n_generic, n_custom) = plancache::CachedPlanCounts(e.plansource);
            let mut nulls = [false; 8];
            let result_types = match plancache::CachedPlanResultDesc(e.plansource) {
                Some(desc) => {
                    let natts = desc.natts as usize;
                    let mut oids: mcx::PgVec<'_, Oid> = mcx::vec_with_capacity_in(mcx, natts)?;
                    for i in 0..natts {
                        oids.push(desc.attr(i).atttypid);
                    }
                    build_regtype_array(mcx, &oids)?
                }
                None => {
                    nulls[4] = true;
                    Datum::from_usize(0)
                }
            };
            let values = [
                varlena_result(varlena::cstring_to_text(mcx, &e.stmt_name[..name_len])?),
                varlena_result(varlena::cstring_to_text(mcx, query_string.as_bytes())?),
                Datum::from_i64(e.prepare_time),
                build_regtype_array(mcx, param_types)?,
                result_types,
                Datum::from_bool(e.from_sql),
                Datum::from_i64(n_generic),
                Datum::from_i64(n_custom),
            ];
            srf.putvalues(&values, &nulls)?;
        }
    }
    Ok(srf.finish(fcinfo))
}

// An empty set of types yields a zero-element array, not NULL (C).
fn build_regtype_array<'mcx>(mcx: Mcx<'mcx>, types: &[Oid]) -> PgResult<Datum> {
    let mut elems: mcx::PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(mcx, types.len())?;
    for &t in types {
        elems.push(Datum::from_oid(t));
    }
    let arr = arrayfuncs::construct::construct_array(
        mcx,
        &elems,
        types_core::catalog::REGTYPEOID,
        4,
        true,
        b'i',
    )?;
    Ok(Datum::from_usize(arr.leak().as_ptr() as usize))
}
