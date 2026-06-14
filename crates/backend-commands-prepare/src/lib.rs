//! Port of `backend/commands/prepare.c` — preparable SQL statements via
//! `PREPARE`, `EXECUTE`, `DEALLOCATE`, the storage of prepared statements
//! accessed by the extended FE/BE query protocol, and the
//! `EXPLAIN EXECUTE` / `pg_prepared_statement` accessors (PostgreSQL 18.3).
//!
//! Every public and static function from prepare.c is present:
//! `PrepareQuery`, `ExecuteQuery`, `EvaluateParams` (static),
//! `InitQueryHashTable` (static), `StorePreparedStatement`,
//! `FetchPreparedStatement`, `FetchPreparedStatementResultDesc`,
//! `FetchPreparedStatementTargetList`, `DeallocateQuery`,
//! `DropPreparedStatement`, `DropAllPreparedStatements`, `ExplainExecuteQuery`,
//! `pg_prepared_statement`, `build_regtype_array` (static).
//!
//! ## Owned in-crate: the per-backend `prepared_queries` hash table
//!
//! prepare.c owns `static HTAB *prepared_queries` (a per-backend dynahash keyed
//! by `stmt_name[NAMEDATALEN]`). It is a per-backend C global, so it is modelled
//! as a `thread_local!` `RefCell<Option<HashMap<String, PreparedStatement>>>`
//! (AGENTS.md "Backend-global state"); `None` mirrors the `NULL` sentinel the C
//! lazily replaces in `InitQueryHashTable`. The dynahash `HASH_STRINGS` key copy
//! (`strlcpy(dest, src, NAMEDATALEN)`, truncated to `NAMEDATALEN-1`) is mirrored
//! in [`hash_key`] so an over-long statement name collides identically.
//!
//! ## Outward calls go through each owner's `-seams` crate
//!
//! prepare.c is a thin driver over the plan cache, parse analysis + rewriter,
//! the parser type resolver / coercion / collation / eval machinery, the
//! executor + portal machinery, the EXPLAIN printer, the snapshot / resource
//! owner / memory-context machinery, the timestamp source, the createas helper,
//! and the SRF / `Datum` value layer. Each call crosses the owning unit's seam
//! crate and panics loudly until that owner lands. The live `CachedPlanSource` /
//! `CachedPlan` / `EState` / `Portal` / `ParamListInfo` / `ResourceOwner` /
//! `DestReceiver` / `QueryCompletion` / `MemoryContext` values are carried as
//! the opaque handle newtypes in `types_nodes::parsestmt`, owned by the
//! not-yet-ported owners (inherited opacity, docs/types.md rule 6).

#![allow(non_snake_case)]

use std::cell::RefCell;
use std::collections::HashMap;

use mcx::Mcx;
use types_core::{Oid, TimestampTz};
// Canonical migration-target value type (the `Datum<'mcx>` enum). The SRF value
// layer builds these via the `from_*` / `null` codec methods; they are lowered
// to the still-shim-typed `types_datum::Datum` only at the audited seam edges
// (`materialized_srf_putvalues` / `construct_array_builtin`), whose owning units
// have not yet advanced their contract off the bare-word newtype.
use types_tuple::backend_access_common_heaptuple::Datum;
use types_error::{
    PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_DUPLICATE_PSTATEMENT,
    ERRCODE_INVALID_PSTATEMENT_DEFINITION, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_PSTATEMENT,
    ERRCODE_WRONG_OBJECT_TYPE,
};

use types_nodes::nodes::{CmdType, Node};
use types_nodes::primnodes::Expr;
use types_nodes::EStateData;
use types_nodes::parsestmt::{
    CachedPlanHandle, CachedPlanSourceHandle, CommandTag, DeallocateStmt, DestReceiverHandle,
    ExecuteStmt, ExplainState, IntoClause, ParamListInfoHandle, ParseState,
    PortalHandle, PrepareStmt, PreparedStatement, QueryCompletionHandle, RawStmt,
    ResourceOwnerHandle,
};

use backend_access_common_tupdesc_seams as tupdesc_seam;
use backend_access_transam_xact_seams as xact_seam;
use backend_commands_createas_seams as createas_seam;
use backend_commands_explain_seams as explain_seam;
use backend_executor_execExpr_seams as execexpr_seam;
use backend_nodes_params_seams as params_seam;
use backend_parser_analyze_seams as analyze_seam;
use backend_parser_parse_expr_seams as parseexpr_seam;
use backend_parser_parse_type_seams as parsetype_seam;
use backend_tcop_pquery_pre_seams as pquery_seam;
use backend_tcop_utility_seams as utility_seam;
use backend_utils_adt_arrayfuncs_seams as arrayfuncs_seam;
use backend_utils_adt_format_type_seams as formattype_seam;
use backend_utils_adt_varlena_seams as varlena_seam;
use backend_utils_cache_plancache_seams as plancache_seam;
use backend_utils_fmgr_funcapi_seams as funcapi_seam;
use backend_utils_mmgr_mcxt_seams as mcxt_seam;
use backend_utils_mmgr_portalmem_pre_seams as portal_seam;
use backend_utils_resowner_resowner_seams as resowner_seam;
use backend_utils_time_snapmgr_pre_seams as snapmgr_seam;

/// `#define NAMEDATALEN 64` (`c.h`) — the dynahash key width.
const NAMEDATALEN: usize = 64;

/// `#define FETCH_ALL LONG_MAX` (`nodes/parsenodes.h`); the portal-run count
/// (C `long`) is carried as `i64` (LP64 backends).
const FETCH_ALL: i64 = i64::MAX;

/// `#define CURSOR_OPT_PARALLEL_OK 0x0800` (`nodes/parsenodes.h`).
const CURSOR_OPT_PARALLEL_OK: i32 = 0x0800;

/// `#define PARAM_FLAG_CONST 0x0001` (`nodes/params.h`).
const PARAM_FLAG_CONST: u16 = 0x0001;

/// `#define REGTYPEOID 2206` (`catalog/pg_type.dat`) — the `regtype` type OID.
const REGTYPEOID: Oid = 2206;

// ---------------------------------------------------------------------------
// The per-backend prepared-statement hash table (prepare.c: `static HTAB *`).
// ---------------------------------------------------------------------------

thread_local! {
    /// `static HTAB *prepared_queries = NULL;` — `None` means the hash table
    /// has not been created yet (so it cannot be storing anything).
    static PREPARED_QUERIES: RefCell<Option<HashMap<String, PreparedStatement>>> =
        const { RefCell::new(None) };
}

/// dynahash keys a `HASH_STRINGS` table of `NAMEDATALEN`-byte keys by copying
/// the C string with `strlcpy(dest, src, NAMEDATALEN)` — truncated to
/// `NAMEDATALEN-1` bytes. Mirror that so over-long names collide exactly.
fn hash_key(stmt_name: &str) -> String {
    let max = NAMEDATALEN - 1;
    if stmt_name.len() <= max {
        stmt_name.to_owned()
    } else {
        let mut end = max;
        while end > 0 && !stmt_name.is_char_boundary(end) {
            end -= 1;
        }
        stmt_name[..end].to_owned()
    }
}

// ===========================================================================
// PrepareQuery — prepare.c:58
// ===========================================================================

/// Implements the `PREPARE` utility statement.
pub fn PrepareQuery<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'mcx>,
    stmt: PrepareStmt<'mcx>,
    stmt_location: i32,
    stmt_len: i32,
) -> PgResult<()> {
    // if (!stmt->name || stmt->name[0] == '\0')
    //     ereport(ERROR, ERRCODE_INVALID_PSTATEMENT_DEFINITION, ...);
    let name: String = match &stmt.name {
        Some(n) if !n.as_str().is_empty() => n.as_str().to_owned(),
        _ => {
            return Err(PgError::error("invalid statement name: must not be empty")
                .with_sqlstate(ERRCODE_INVALID_PSTATEMENT_DEFINITION));
        }
    };

    // Need to wrap the contained statement in a RawStmt node to pass it to
    // parse analysis. The wrapped query is also read by CreateCommandTag, so
    // we keep both the raw node and its clone (C aliases the same `stmt->query`
    // pointer at lines 82 and 91).
    //
    //   rawstmt = makeNode(RawStmt);
    //   rawstmt->stmt = stmt->query; rawstmt->stmt_location/len = ...;
    let query: &Node<'mcx> = match &stmt.query {
        Some(q) => q,
        // C dereferences stmt->query unconditionally; a missing query is a
        // grammar bug.
        None => panic!("PrepareQuery: PrepareStmt::query is missing"),
    };
    let raw_stmt = make_raw_stmt(mcx, query, stmt_location, stmt_len)?;

    // Create the CachedPlanSource before parse analysis, since it needs to see
    // the unmodified raw parse tree.
    //
    //   plansource = CreateCachedPlan(rawstmt, pstate->p_sourcetext,
    //                                 CreateCommandTag(stmt->query));
    let p_sourcetext: &str = pstate
        .p_sourcetext
        .as_ref()
        .map(|s| s.as_str())
        .unwrap_or("");
    let command_tag: CommandTag = utility_seam::create_command_tag::call(query)?;
    let plansource: CachedPlanSourceHandle =
        plancache_seam::create_cached_plan::call(mcx, &raw_stmt, p_sourcetext, command_tag)?;

    // Transform list of TypeNames to array of type OIDs.
    //
    //   nargs = list_length(stmt->argtypes);
    //   if (nargs) { argtypes = palloc_array(Oid, nargs);
    //       foreach(l, stmt->argtypes) argtypes[i++] = typenameTypeId(pstate, tn); }
    let nargs = stmt.argtypes.len();
    let mut argtypes: mcx::PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, nargs)?;
    if nargs != 0 {
        for tn in stmt.argtypes.iter() {
            // C: typenameTypeId(pstate, tn). main's seam mirrors PostgreSQL's
            // own typenameTypeId(NULL, typeName) entry point: it reads only the
            // TypeName, so pstate (its sole field here, p_sourcetext) is not
            // threaded across the seam.
            let toid = parsetype_seam::typename_type_id::call(tn)?;
            argtypes.push(toid);
        }
    }

    // Analyze using these parameter types (deducing unknown ones from context)
    // and rewrite; the result may grow/replace argtypes.
    //
    //   query_list = pg_analyze_and_rewrite_varparams(rawstmt, pstate->p_sourcetext,
    //                                                  &argtypes, &nargs, NULL);
    let analyzed = analyze_seam::analyze_and_rewrite_varparams::call(
        mcx,
        &raw_stmt,
        p_sourcetext,
        argtypes.as_slice(),
    )?;

    // Finish filling in the CachedPlanSource.
    //
    //   CompleteCachedPlan(plansource, query_list, NULL, argtypes, nargs,
    //                      NULL, NULL, CURSOR_OPT_PARALLEL_OK, true);
    let _ = CURSOR_OPT_PARALLEL_OK; // fixed cursor option + fixed_result=true are baked into the seam
    plancache_seam::complete_cached_plan::call(
        mcx,
        plansource,
        analyzed.query_list.as_slice(),
        analyzed.arg_types.as_slice(),
    )?;

    // Save the results. StorePreparedStatement(stmt->name, plansource, true);
    StorePreparedStatement(&name, plansource, true)?;

    Ok(())
}

// ===========================================================================
// ExecuteQuery — prepare.c:149
// ===========================================================================

/// `ExecuteQuery` — implement the `EXECUTE` utility statement. A non-`None`
/// `into_clause` selects the `CREATE TABLE ... AS EXECUTE` path.
pub fn ExecuteQuery<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'mcx>,
    stmt: ExecuteStmt<'mcx>,
    into_clause: Option<&IntoClause<'mcx>>,
    params: ParamListInfoHandle,
    dest: DestReceiverHandle,
    qc: QueryCompletionHandle,
) -> PgResult<()> {
    // ParamListInfo paramLI = NULL; EState *estate = NULL;
    let mut param_li: ParamListInfoHandle = ParamListInfoHandle::NULL;
    let mut estate: Option<mcx::PgBox<'mcx, EStateData<'mcx>>> = None;
    let eflags: i32;
    let count: i64;

    // entry = FetchPreparedStatement(stmt->name, true);
    let name: &str = stmt.name.as_ref().map(|s| s.as_str()).unwrap_or("");
    let entry = FetchPreparedStatement(name, true)?
        .expect("FetchPreparedStatement(throwError=true) returns Some or errors");

    // if (!entry->plansource->fixed_result)
    //     elog(ERROR, "EXECUTE does not support variable-result cached plans");
    if !plancache_seam::plansource_fixed_result::call(entry.plansource)? {
        return Err(PgError::error(
            "EXECUTE does not support variable-result cached plans",
        ));
    }

    // if (entry->plansource->num_params > 0) {
    //     estate = CreateExecutorState(); estate->es_param_list_info = params;
    //     paramLI = EvaluateParams(pstate, entry, stmt->params, estate); }
    if plancache_seam::plansource_num_params::call(entry.plansource)? > 0 {
        let mut es = execexpr_seam::create_executor_state::call(mcx)?;
        es.es_param_list_info = params;
        param_li = EvaluateParams(mcx, pstate, &entry, &stmt.params, &mut es)?;
        estate = Some(es);
    }

    // portal = CreateNewPortal(); portal->visible = false;
    let portal: PortalHandle = portal_seam::create_new_portal::call()?;
    portal_seam::portal_set_visible::call(&portal, false)?;

    // query_string = MemoryContextStrdup(portal->portalContext,
    //                                    entry->plansource->query_string);
    let portal_context = portal_seam::portal_get_portal_context::call(&portal)?;
    let plan_query_string = plancache_seam::plansource_query_string::call(mcx, entry.plansource)?;
    let query_string =
        mcxt_seam::memory_context_strdup::call(portal_context, plan_query_string.as_str())?;

    // cplan = GetCachedPlan(entry->plansource, paramLI, NULL, NULL);
    // plan_list = cplan->stmt_list;
    let cplan: CachedPlanHandle = plancache_seam::get_cached_plan::call(
        entry.plansource,
        param_li,
        ResourceOwnerHandle::NULL,
        None,
    )?;
    let plan_list = plancache_seam::cached_plan_stmt_list::call(mcx, cplan)?;

    // DO NOT add any logic that could possibly throw an error between
    // GetCachedPlan and PortalDefineQuery, or you'll leak the plan refcount.
    //
    //   PortalDefineQuery(portal, NULL, query_string, entry->plansource->commandTag,
    //                     plan_list, cplan);
    let command_tag = plancache_seam::plansource_command_tag::call(entry.plansource)?;
    portal_seam::portal_define_query::call(
        &portal,
        query_string.as_str(),
        command_tag,
        plan_list.as_slice(),
        cplan,
    )?;

    // For CREATE TABLE ... AS EXECUTE, verify the statement produces tuples
    // (a plain SELECT) and set the proper eflags / fetch count.
    if let Some(into) = into_clause {
        // if (list_length(plan_list) != 1) ereport(ERROR, ... "not a SELECT");
        if plan_list.len() != 1 {
            return Err(PgError::error("prepared statement is not a SELECT")
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE));
        }
        // pstmt = linitial_node(PlannedStmt, plan_list);
        // if (pstmt->commandType != CMD_SELECT) ereport(ERROR, ... "not a SELECT");
        let pstmt = &plan_list[0];
        if pstmt.commandType != CmdType::CMD_SELECT {
            return Err(PgError::error("prepared statement is not a SELECT")
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE));
        }

        // eflags = GetIntoRelEFlags(intoClause);
        eflags = createas_seam::get_into_rel_eflags::call(into)?;

        // if (intoClause->skipData) count = 0; else count = FETCH_ALL;
        if into.skipData {
            count = 0;
        } else {
            count = FETCH_ALL;
        }
    } else {
        // Plain old EXECUTE.
        eflags = 0;
        count = FETCH_ALL;
    }

    // PortalStart(portal, paramLI, eflags, GetActiveSnapshot());
    let active_snapshot = snapmgr_seam::get_active_snapshot::call();
    pquery_seam::portal_start::call(&portal, param_li, eflags, active_snapshot)?;

    // (void) PortalRun(portal, count, false, dest, dest, qc);
    pquery_seam::portal_run::call(&portal, count, dest, qc)?;

    // PortalDrop(portal, false);
    portal_seam::portal_drop::call(&portal)?;

    // if (estate) FreeExecutorState(estate);
    if let Some(es) = estate {
        execexpr_seam::free_executor_state::call(es)?;
    }

    // No need to pfree other memory, MemoryContext will be reset.
    Ok(())
}

// ===========================================================================
// EvaluateParams — prepare.c:280 (static)
// ===========================================================================

/// `EvaluateParams` — evaluate a list of EXECUTE parameters into a
/// `ParamListInfo` (carried as a handle), or `ParamListInfoHandle::NULL` when
/// there are none.
fn EvaluateParams<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'mcx>,
    pstmt: &PreparedStatement,
    params: &[mcx::PgBox<'mcx, Node<'mcx>>],
    estate: &mut EStateData<'mcx>,
) -> PgResult<ParamListInfoHandle> {
    // Oid *param_types = pstmt->plansource->param_types;
    // int num_params = pstmt->plansource->num_params;
    // int nparams = list_length(params);
    let param_types = plancache_seam::plansource_param_types::call(mcx, pstmt.plansource)?;
    let num_params: i32 = plancache_seam::plansource_num_params::call(pstmt.plansource)?;
    let nparams: i32 = params.len() as i32;

    // if (nparams != num_params) ereport(ERROR, ERRCODE_SYNTAX_ERROR, ...);
    if nparams != num_params {
        return Err(PgError::error(format!(
            "wrong number of parameters for prepared statement \"{}\"",
            pstmt.stmt_name
        ))
        .with_sqlstate(ERRCODE_SYNTAX_ERROR)
        .with_detail(format!(
            "Expected {num_params} parameters but got {nparams}."
        )));
    }

    // if (num_params == 0) return NULL;
    if num_params == 0 {
        return Ok(ParamListInfoHandle::NULL);
    }

    // params = copyObject(params); — the parser scribbles on its input, so it
    // copies first.
    //
    // foreach(l, params) { expr = transformExpr(...); given = exprType(expr);
    //     expr = coerce_to_target_type(...); if (!expr) ereport(...);
    //     assign_expr_collations(...); lfirst(l) = expr; }
    //
    // In the owned model the per-parameter analysis seam takes the raw parser
    // node and returns the finished `Expr`; we collect them into the working
    // `Expr` list (`lfirst(l) = expr`) handed to `ExecPrepareExprList`.
    let p_sourcetext: &str = pstate
        .p_sourcetext
        .as_ref()
        .map(|s| s.as_str())
        .unwrap_or("");
    let mut params_work: mcx::PgVec<'mcx, Expr> = mcx::vec_with_capacity_in(mcx, num_params as usize)?;
    let mut i: i32 = 0;
    while i < num_params {
        let expected_type_id = param_types[i as usize];
        let res = parseexpr_seam::analyze_one_exec_param::call(
            mcx,
            p_sourcetext,
            &params[i as usize],
            i,
            expected_type_id,
        )?;

        if res.coercion_failed {
            let given_name = formattype_seam::format_type_be::call(mcx, res.given_type_id)?;
            let expected_name = formattype_seam::format_type_be::call(mcx, expected_type_id)?;
            let cursor = parseexpr_seam::parser_errposition::call(p_sourcetext, res.expr_location)?;
            return Err(PgError::error(format!(
                "parameter ${} of type {} cannot be coerced to the expected type {}",
                i + 1,
                given_name.as_str(),
                expected_name.as_str()
            ))
            .with_sqlstate(ERRCODE_DATATYPE_MISMATCH)
            .with_hint("You will need to rewrite or cast the expression.")
            .with_cursor_position(cursor));
        }

        // lfirst(l) = expr;
        let expr = res
            .expr
            .expect("analyze_one_exec_param returns Some expr when coercion succeeds");
        params_work.push((*expr).clone());
        i += 1;
    }

    // exprstates = ExecPrepareExprList(params, estate);
    let exprstates =
        execexpr_seam::exec_prepare_expr_list::call(params_work.as_slice(), estate)?;

    // paramLI = makeParamList(num_params);
    let param_li = params_seam::make_param_list::call(num_params)?;
    let _ = PARAM_FLAG_CONST; // pflags = PARAM_FLAG_CONST is set inside the eval seam

    // foreach(l, exprstates) { ParamExternData *prm = &paramLI->params[i];
    //     prm->ptype = param_types[i]; prm->pflags = PARAM_FLAG_CONST;
    //     prm->value = ExecEvalExprSwitchContext(n, GetPerTupleExprContext(estate),
    //                                            &prm->isnull); i++; }
    let mut i: i32 = 0;
    while i < num_params {
        execexpr_seam::eval_exec_param_into_list::call(
            param_li,
            &exprstates[i as usize],
            i,
            param_types[i as usize],
            estate,
        )?;
        i += 1;
    }

    Ok(param_li)
}

// ===========================================================================
// InitQueryHashTable — prepare.c:371 (static)
// ===========================================================================

/// Initialize query hash table upon first use
/// (`hash_create("Prepared Queries", 32, ...)`).
fn InitQueryHashTable() {
    PREPARED_QUERIES.with(|tbl| {
        let mut tbl = tbl.borrow_mut();
        if tbl.is_none() {
            *tbl = Some(HashMap::with_capacity(32));
        }
    });
}

// ===========================================================================
// StorePreparedStatement — prepare.c:391
// ===========================================================================

/// Store a query's data in the hash table under the specified key.
pub fn StorePreparedStatement(
    stmt_name: &str,
    plansource: CachedPlanSourceHandle,
    from_sql: bool,
) -> PgResult<()> {
    // TimestampTz cur_ts = GetCurrentStatementStartTimestamp();
    let cur_ts: TimestampTz = xact_seam::get_current_statement_start_timestamp::call();

    // if (!prepared_queries) InitQueryHashTable();
    InitQueryHashTable();

    // entry = hash_search(prepared_queries, stmt_name, HASH_ENTER, &found);
    // if (found) ereport(ERROR, ERRCODE_DUPLICATE_PSTATEMENT, ...);
    let key = hash_key(stmt_name);
    let found = PREPARED_QUERIES.with(|tbl| {
        let tbl = tbl.borrow();
        tbl.as_ref().is_some_and(|m| m.contains_key(&key))
    });
    if found {
        return Err(
            PgError::error(format!("prepared statement \"{stmt_name}\" already exists"))
                .with_sqlstate(ERRCODE_DUPLICATE_PSTATEMENT),
        );
    }

    // entry->plansource = plansource; entry->from_sql = from_sql;
    // entry->prepare_time = cur_ts;
    let entry = PreparedStatement {
        stmt_name: key.clone(),
        plansource,
        from_sql,
        prepare_time: cur_ts,
    };
    PREPARED_QUERIES.with(|tbl| {
        tbl.borrow_mut()
            .as_mut()
            .expect("InitQueryHashTable ran")
            .insert(key, entry);
    });

    // SaveCachedPlan(plansource);
    plancache_seam::save_cached_plan::call(plansource)?;

    Ok(())
}

// ===========================================================================
// FetchPreparedStatement — prepare.c:433
// ===========================================================================

/// Lookup an existing query in the hash table. Throws `ereport(ERROR)` when
/// `throw_error` and the entry is missing, else returns `Ok(None)`.
pub fn FetchPreparedStatement(
    stmt_name: &str,
    throw_error: bool,
) -> PgResult<Option<PreparedStatement>> {
    let key = hash_key(stmt_name);
    let entry = PREPARED_QUERIES.with(|tbl| {
        let tbl = tbl.borrow();
        tbl.as_ref().and_then(|m| m.get(&key).cloned())
    });

    // if (!entry && throwError) ereport(ERROR, ERRCODE_UNDEFINED_PSTATEMENT, ...);
    if entry.is_none() && throw_error {
        return Err(
            PgError::error(format!("prepared statement \"{stmt_name}\" does not exist"))
                .with_sqlstate(ERRCODE_UNDEFINED_PSTATEMENT),
        );
    }

    Ok(entry)
}

// ===========================================================================
// FetchPreparedStatementResultDesc — prepare.c:465
// ===========================================================================

/// Determine the result tupledesc a prepared statement produces, copied into
/// `mcx`; `None` if execution returns no tuples.
pub fn FetchPreparedStatementResultDesc<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &PreparedStatement,
) -> PgResult<Option<types_tuple::heaptuple::TupleDescData<'mcx>>> {
    // Assert(stmt->plansource->fixed_result);
    debug_assert!(plancache_seam::plansource_fixed_result::call(stmt.plansource)?);

    // if (stmt->plansource->resultDesc)
    //     return CreateTupleDescCopy(stmt->plansource->resultDesc); else return NULL;
    match plancache_seam::plansource_result_desc::call(mcx, stmt.plansource)? {
        Some(result_desc) => Ok(Some(tupdesc_seam::create_tuple_desc_copy::call(
            mcx,
            &result_desc,
        )?)),
        None => Ok(None),
    }
}

// ===========================================================================
// FetchPreparedStatementTargetList — prepare.c:488
// ===========================================================================

/// Extract a prepared statement's query targetlist (copied into `mcx`); the
/// empty `Vec` is the C `NIL`.
pub fn FetchPreparedStatementTargetList<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &PreparedStatement,
) -> PgResult<mcx::PgVec<'mcx, Node<'mcx>>> {
    // tlist = CachedPlanGetTargetList(stmt->plansource, NULL);
    // return copyObject(tlist);  — the seam returns an owned independent copy.
    plancache_seam::cached_plan_get_target_list::call(mcx, stmt.plansource)
}

// ===========================================================================
// DeallocateQuery — prepare.c:504
// ===========================================================================

/// Implements the `DEALLOCATE` utility statement.
pub fn DeallocateQuery<'mcx>(stmt: &DeallocateStmt<'mcx>) -> PgResult<()> {
    // if (stmt->name) DropPreparedStatement(stmt->name, true);
    // else DropAllPreparedStatements();
    match stmt.name.as_ref().map(|s| s.as_str()) {
        Some(name) => DropPreparedStatement(name, true),
        None => DropAllPreparedStatements(),
    }
}

// ===========================================================================
// DropPreparedStatement — prepare.c:518
// ===========================================================================

/// Internal version of `DEALLOCATE`. If `show_error` is false, dropping a
/// nonexistent statement is a no-op.
pub fn DropPreparedStatement(stmt_name: &str, show_error: bool) -> PgResult<()> {
    // entry = FetchPreparedStatement(stmt_name, showError);
    let entry = FetchPreparedStatement(stmt_name, show_error)?;

    // if (entry) { DropCachedPlan(entry->plansource);
    //              hash_search(prepared_queries, entry->stmt_name, HASH_REMOVE, NULL); }
    if let Some(entry) = entry {
        plancache_seam::drop_cached_plan::call(entry.plansource)?;
        PREPARED_QUERIES.with(|tbl| {
            if let Some(m) = tbl.borrow_mut().as_mut() {
                m.remove(&entry.stmt_name);
            }
        });
    }

    Ok(())
}

// ===========================================================================
// DropAllPreparedStatements — prepare.c:540
// ===========================================================================

/// Drop all cached statements.
pub fn DropAllPreparedStatements() -> PgResult<()> {
    // if (!prepared_queries) return;  — when the table was never created, the
    // collected snapshot is empty and the loop body never runs.
    let entries: Vec<PreparedStatement> = PREPARED_QUERIES.with(|tbl| match tbl.borrow().as_ref() {
        None => Vec::new(),
        Some(m) => m.values().cloned().collect(),
    });

    // hash_seq_init(&seq, prepared_queries);
    // while ((entry = hash_seq_search(&seq)) != NULL) {
    //     DropCachedPlan(entry->plansource);
    //     hash_search(prepared_queries, entry->stmt_name, HASH_REMOVE, NULL); }
    for entry in entries {
        plancache_seam::drop_cached_plan::call(entry.plansource)?;
        PREPARED_QUERIES.with(|tbl| {
            if let Some(m) = tbl.borrow_mut().as_mut() {
                m.remove(&entry.stmt_name);
            }
        });
    }

    Ok(())
}

// ===========================================================================
// ExplainExecuteQuery — prepare.c:570
// ===========================================================================

/// Implements the `EXPLAIN EXECUTE` utility statement. `into` is `None` unless
/// doing `EXPLAIN CREATE TABLE AS EXECUTE`.
pub fn ExplainExecuteQuery<'mcx>(
    mcx: Mcx<'mcx>,
    execstmt: ExecuteStmt<'mcx>,
    into: Option<&IntoClause<'mcx>>,
    es: &mut ExplainState<'mcx>,
    pstate: &ParseState<'mcx>,
    params: ParamListInfoHandle,
) -> PgResult<()> {
    // ParamListInfo paramLI = NULL; EState *estate = NULL;
    let mut param_li: ParamListInfoHandle = ParamListInfoHandle::NULL;
    let mut estate: Option<mcx::PgBox<'mcx, EStateData<'mcx>>> = None;

    // if (es->memory) { create+switch planner ctx } if (es->buffers) snapshot
    // pgBufferUsage; INSTR_TIME_SET_CURRENT(planstart);
    let bk = explain_seam::explain_execute_begin::call(&*es)?;

    // entry = FetchPreparedStatement(execstmt->name, true);
    let name: &str = execstmt.name.as_ref().map(|s| s.as_str()).unwrap_or("");
    let entry = FetchPreparedStatement(name, true)?
        .expect("FetchPreparedStatement(throwError=true) returns Some or errors");

    // if (!entry->plansource->fixed_result)
    //     elog(ERROR, "EXPLAIN EXECUTE does not support variable-result cached plans");
    if !plancache_seam::plansource_fixed_result::call(entry.plansource)? {
        return Err(PgError::error(
            "EXPLAIN EXECUTE does not support variable-result cached plans",
        ));
    }

    // query_string = entry->plansource->query_string;
    let query_string = plancache_seam::plansource_query_string::call(mcx, entry.plansource)?;

    // if (entry->plansource->num_params) {
    //     pstate_params = make_parsestate(NULL);
    //     pstate_params->p_sourcetext = pstate->p_sourcetext;
    //     estate = CreateExecutorState(); estate->es_param_list_info = params;
    //     paramLI = EvaluateParams(pstate_params, entry, execstmt->params, estate); }
    //
    // EvaluateParams only consults p_sourcetext, and C copies pstate's into the
    // throwaway pstate_params, so we pass `pstate` straight through.
    if plancache_seam::plansource_num_params::call(entry.plansource)? != 0 {
        let mut es_state = execexpr_seam::create_executor_state::call(mcx)?;
        es_state.es_param_list_info = params;
        param_li = EvaluateParams(mcx, pstate, &entry, &execstmt.params, &mut es_state)?;
        estate = Some(es_state);
    }

    // cplan = GetCachedPlan(entry->plansource, paramLI, CurrentResourceOwner,
    //                       pstate->p_queryEnv);
    let owner = resowner_seam::current_resource_owner::call()?;
    let query_env = pstate.p_queryEnv.as_deref();
    let cplan: CachedPlanHandle =
        plancache_seam::get_cached_plan::call(entry.plansource, param_li, owner, query_env)?;

    // INSTR_TIME_SET_CURRENT(planduration); INSTR_TIME_SUBTRACT(planduration, planstart);
    explain_seam::explain_planduration::call(bk)?;

    let es_memory = es.memory;
    let es_buffers = es.buffers;

    // if (es->memory) { MemoryContextSwitchTo(saved_ctx);
    //     MemoryContextMemConsumed(planner_ctx, &mem_counters); }
    if es_memory {
        explain_seam::explain_memory_accounting::call(bk)?;
    }

    // if (es->buffers) { memset(&bufusage, 0, ...);
    //     BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &bufusage_start); }
    if es_buffers {
        explain_seam::explain_buffer_accounting::call(bk)?;
    }

    // plan_list = cplan->stmt_list;
    let plan_list = plancache_seam::cached_plan_stmt_list::call(mcx, cplan)?;

    // foreach(p, plan_list) {
    //     if (pstmt->commandType != CMD_UTILITY)
    //         ExplainOnePlan(pstmt, into, es, query_string, paramLI, pstate->p_queryEnv,
    //                        &planduration, bufusage?, mem_counters?);
    //     else ExplainOneUtility(pstmt->utilityStmt, into, es, pstate, paramLI);
    //     if (lnext(plan_list, p) != NULL) ExplainSeparatePlans(es); }
    let n = plan_list.len();
    let p_sourcetext: &str = pstate
        .p_sourcetext
        .as_ref()
        .map(|s| s.as_str())
        .unwrap_or("");
    for idx in 0..n {
        let pstmt = &plan_list[idx];
        if pstmt.commandType != CmdType::CMD_UTILITY {
            explain_seam::explain_one_plan::call(
                pstmt,
                into,
                &mut *es,
                query_string.as_str(),
                param_li,
                query_env,
                bk,
                es_buffers,
                es_memory,
            )?;
        } else {
            // C dereferences pstmt->utilityStmt unconditionally for a
            // CMD_UTILITY PlannedStmt; a missing node is a planner bug.
            let utility_stmt = pstmt
                .utilityStmt
                .as_deref()
                .expect("ExplainExecuteQuery: CMD_UTILITY PlannedStmt without utilityStmt");
            explain_seam::explain_one_utility::call(
                utility_stmt,
                into,
                &mut *es,
                p_sourcetext,
                query_env,
                param_li,
            )?;
        }

        // No need for CommandCounterIncrement, as ExplainOnePlan did it.
        if idx + 1 < n {
            explain_seam::explain_separate_plans::call(&mut *es)?;
        }
    }

    // if (estate) FreeExecutorState(estate);
    if let Some(es_state) = estate {
        execexpr_seam::free_executor_state::call(es_state)?;
    }

    // ReleaseCachedPlan(cplan, CurrentResourceOwner);
    plancache_seam::release_cached_plan::call(cplan, owner)?;

    Ok(())
}

// ===========================================================================
// pg_prepared_statement / build_regtype_array — prepare.c:684 / 746
// ===========================================================================

/// This set-returning function reads all the prepared statements and returns a
/// set of (name, statement, prepare_time, param_types, from_sql, generic_plans,
/// custom_plans). Returns `(Datum) 0`.
pub fn pg_prepared_statement<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut types_nodes::fmgr::FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    // We put all tuples into a tuplestore in one scan of the hashtable.
    //
    //   InitMaterializedSRF(fcinfo, 0);
    funcapi_seam::InitMaterializedSRF::call(fcinfo, 0)?;

    // hash table might be uninitialized.
    if PREPARED_QUERIES.with(|tbl| tbl.borrow().is_some()) {
        // Take a stable snapshot of the entries (mirroring the single
        // hash_seq_search scan).
        let entries: Vec<PreparedStatement> = PREPARED_QUERIES.with(|tbl| {
            tbl.borrow()
                .as_ref()
                .map(|m| m.values().cloned().collect())
                .unwrap_or_default()
        });

        let rsinfo = fcinfo
            .resultinfo
            .as_mut()
            .expect("InitMaterializedSRF set fcinfo->resultinfo");

        for prep_stmt in &entries {
            // result_desc = prep_stmt->plansource->resultDesc;
            let result_desc =
                plancache_seam::plansource_result_desc::call(mcx, prep_stmt.plansource)?;

            // values[8], nulls[8] = {0}
            let mut values: [Datum<'mcx>; 8] = std::array::from_fn(|_| Datum::null());
            let mut nulls = [false; 8];

            // values[0] = CStringGetTextDatum(prep_stmt->stmt_name);
            // `cstring_to_text` is varlena-owned and still hands back the
            // bare-word `text` pointer (shim contract not yet advanced); carry
            // it as the canonical enum's by-value pointer word at this edge.
            values[0] = Datum::from_usize(
                varlena_seam::cstring_to_text::call(mcx, &prep_stmt.stmt_name)?.as_usize(),
            );
            // values[1] = CStringGetTextDatum(prep_stmt->plansource->query_string);
            let qs = plancache_seam::plansource_query_string::call(mcx, prep_stmt.plansource)?;
            values[1] =
                Datum::from_usize(varlena_seam::cstring_to_text::call(mcx, qs.as_str())?.as_usize());
            // values[2] = TimestampTzGetDatum(prep_stmt->prepare_time);
            values[2] = Datum::from_i64(prep_stmt.prepare_time);
            // values[3] = build_regtype_array(param_types, num_params);
            let param_types =
                plancache_seam::plansource_param_types::call(mcx, prep_stmt.plansource)?;
            values[3] = build_regtype_array(mcx, param_types.as_slice())?;

            // if (result_desc) { build result_types regtype[]; } else nulls[4] = true;
            match &result_desc {
                Some(desc) => {
                    // result_types = palloc_array(Oid, natts);
                    // for i in 0..natts: result_types[i] = TupleDescAttr(desc, i)->atttypid;
                    let natts = desc.attrs.len();
                    let mut result_types: mcx::PgVec<'mcx, Oid> =
                        mcx::vec_with_capacity_in(mcx, natts)?;
                    for i in 0..natts {
                        result_types.push(desc.attr(i).atttypid);
                    }
                    values[4] = build_regtype_array(mcx, result_types.as_slice())?;
                }
                None => {
                    nulls[4] = true;
                }
            }

            // values[5] = BoolGetDatum(prep_stmt->from_sql);
            values[5] = Datum::from_bool(prep_stmt.from_sql);
            // values[6] = Int64GetDatumFast(num_generic_plans);
            values[6] = Datum::from_i64(
                plancache_seam::plansource_num_generic_plans::call(prep_stmt.plansource)?,
            );
            // values[7] = Int64GetDatumFast(num_custom_plans);
            values[7] = Datum::from_i64(
                plancache_seam::plansource_num_custom_plans::call(prep_stmt.plansource)?,
            );

            // tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
            // The funcapi seam now takes the canonical unified value directly
            // (the Datum-unification keystone flipped this edge).
            funcapi_seam::materialized_srf_putvalues::call(rsinfo, &values, &nulls)?;
        }
    }

    // return (Datum) 0;
    Ok(Datum::null())
}

/// `build_regtype_array(param_types, num_params)` — a one-dimensional `regtype`
/// array `Datum` from a C array of Oids. An empty array is a zero-element
/// array, not NULL.
fn build_regtype_array<'mcx>(mcx: Mcx<'mcx>, param_types: &[Oid]) -> PgResult<Datum<'mcx>> {
    // tmp_ary = palloc_array(Datum, num_params);
    // for i in 0..num_params: tmp_ary[i] = ObjectIdGetDatum(param_types[i]);
    // The element words are built as canonical by-value `regtype` oids; they are
    // lowered to the still-shim-typed `construct_array_builtin` contract at this
    // audited array-build edge (arrayfuncs has not advanced off the bare-word
    // newtype).
    let mut tmp_ary: mcx::PgVec<'mcx, types_datum::Datum> =
        mcx::vec_with_capacity_in(mcx, param_types.len())?;
    for &t in param_types {
        tmp_ary.push(types_datum::Datum::from_usize(Datum::from_oid(t).as_usize()));
    }

    // result = construct_array_builtin(tmp_ary, num_params, REGTYPEOID);
    // return PointerGetDatum(result);
    let arr = arrayfuncs_seam::construct_array_builtin::call(mcx, tmp_ary.as_slice(), REGTYPEOID)?;
    Ok(Datum::from_usize(arr.as_usize()))
}

// ===========================================================================
// Internal helpers
// ===========================================================================

/// `makeNode(RawStmt)` (prepare.c:81-84) — build the `RawStmt` wrapper in
/// `mcx`, cloning the contained query into it (`rawstmt->stmt = stmt->query`;
/// C aliases the same `stmt->query` pointer into both the RawStmt and
/// `CreateCommandTag`) and recording the `stmt_location` / `stmt_len`
/// source-text span. The wrapper is threaded into both `CreateCachedPlan`
/// (which stores the raw tree, span included) and
/// `pg_analyze_and_rewrite_varparams`.
fn make_raw_stmt<'mcx>(
    mcx: Mcx<'mcx>,
    query: &Node<'mcx>,
    stmt_location: i32,
    stmt_len: i32,
) -> PgResult<RawStmt<'mcx>> {
    Ok(RawStmt {
        stmt: mcx::alloc_in(mcx, query.clone_in(mcx)?)?,
        stmt_location,
        stmt_len,
    })
}
