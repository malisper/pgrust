//! CALL family of `backend/commands/functioncmds.c`.
//!
//! `ExecuteCallStmt` (CALL proc resolution, fcinfo setup, atomic snapshot
//! push/pop, result-tuple dispatch) and `CallStmtResultDesc` (polymorphic
//! output-argument tuple descriptor).
//!
//! Both bodies operate on the live, arena-lifetimed `types_nodes` CALL node:
//! `transformCallStmt` (analyze.c, ported in backend-parser-analyze) has already
//! populated `stmt->funcexpr` (a real `FuncExpr` node) and `stmt->outargs` (the
//! transformed OUT/INOUT expression nodes), so the rich call tree is carried
//! end-to-end and nothing here is opaque. The executor pieces these bodies need
//! (executor state, expr compile/eval, fmgr invoke, the record tuple sender)
//! cross seams to their real owners.

#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_nodes::nodes::Node;
use types_nodes::parsestmt::DestReceiverHandle;
use types_nodes::portalcmds::ParamListInfo;
use types_nodes::primnodes::{Expr, FuncExpr};
use types_tuple::heaptuple::TupleDesc;
use types_tuple::Datum;

use types_acl::acl::ACLCHECK_OK;
use types_acl::ACL_EXECUTE;
use types_error::{ERRCODE_TOO_MANY_ARGUMENTS, ERROR};
use backend_utils_error::ereport;

/// `RECORDOID` (pg_type.h).
const RECORDOID: Oid = 2249;
/// `VOIDOID` (pg_type.h).
const VOIDOID: Oid = 2278;
/// `FUNC_MAX_ARGS` (pg_config_manual.h).
const FUNC_MAX_ARGS: usize = 100;
/// `T_CallContext` (nodetags.h).
const T_CALL_CONTEXT: u32 = 332;

/// Read `stmt->funcexpr` off the live CALL node and `castNode(FuncExpr, .)` it.
fn callstmt_funcexpr<'a, 'mcx>(stmt_node: &'a Node<'mcx>) -> PgResult<&'a FuncExpr> {
    let stmt = stmt_node.expect_callstmt();
    let fexpr_node = stmt
        .funcexpr
        .as_deref()
        .ok_or_else(|| PgError::error("ExecuteCallStmt: stmt->funcexpr is NULL"))?;
    match fexpr_node.as_expr().and_then(|e| e.as_funcexpr()) {
        Some(f) => Ok(f),
        None => Err(PgError::error(
            "ExecuteCallStmt: stmt->funcexpr is not a FuncExpr",
        )),
    }
}

// ===========================================================================
// ExecuteCallStmt (functioncmds.c:2206)
// ===========================================================================

/// `ExecuteCallStmt(stmt, params, atomic, dest)` (functioncmds.c:2206).
///
/// Resolves the procedure off `stmt->funcexpr`, checks EXECUTE privilege,
/// forces atomicity when the procedure has `proconfig`/`prosecdef`, evaluates
/// the (input) argument expressions in a throwaway executor state, invokes the
/// procedure with a `CallContext` carrying `atomic`, and — for a RECORD-result
/// procedure (OUT/INOUT params) — sends the returned record to `dest`.
///
/// Faithful to the non-transaction-control path. A procedure that runs
/// COMMIT/ROLLBACK requires `atomic == false`; the `CallContext.atomic` flag is
/// threaded to the language handler (so plpgsql's nonatomic demux is correct),
/// and the in-procedure transaction-control machinery is the language handler's
/// own (pl_exec.c) concern.
pub fn ExecuteCallStmt<'mcx>(
    mcx: Mcx<'mcx>,
    stmt_node: &Node<'mcx>,
    params: ParamListInfo,
    atomic: bool,
    dest: DestReceiverHandle,
) -> PgResult<()> {
    let fexpr = callstmt_funcexpr(stmt_node)?;
    let funcid = fexpr.funcid;
    let funcresulttype = fexpr.funcresulttype;
    let inputcollid = fexpr.inputcollid;

    /*
     * object_aclcheck(ProcedureRelationId, fexpr->funcid, GetUserId(),
     * ACL_EXECUTE) — and aclcheck_error(OBJECT_PROCEDURE, ...) on failure.
     */
    let user_id = backend_commands_functioncmds_seams::get_user_id::call()?;
    let aclresult =
        backend_commands_functioncmds_seams::proc_aclcheck::call(funcid, user_id, ACL_EXECUTE)?;
    if aclresult != ACLCHECK_OK {
        let name =
            backend_commands_functioncmds_seams::get_func_name::call(funcid)?.unwrap_or_default();
        backend_commands_functioncmds_seams::aclcheck_error_function::call(aclresult, name)?;
    }

    /*
     * Prep the context object we'll pass to the procedure. If proconfig is set
     * (GUC stacking) or the procedure is SECURITY DEFINER, transaction commands
     * can't be allowed, so force atomic.
     */
    let proc = backend_optimizer_util_clauses_seams::get_func_form::call(funcid)?;
    let mut call_atomic = atomic;
    if !proc.proconfig_isnull {
        call_atomic = true;
    }
    if proc.prosecdef {
        call_atomic = true;
    }

    /* safety check; see ExecInitFunc() */
    let nargs = fexpr.args.len();
    if nargs > FUNC_MAX_ARGS {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_TOO_MANY_ARGUMENTS)
            .errmsg(format!(
                "cannot pass more than {FUNC_MAX_ARGS} arguments to a procedure"
            ))
            .into_error());
    }

    /* InvokeFunctionExecuteHook(fexpr->funcid). */
    backend_catalog_objectaccess_seams::invoke_function_execute_hook::call(funcid)?;

    /*
     * Evaluate procedure arguments inside a throwaway execution context. Note
     * we can't free this context till the procedure returns.
     *
     * estate->es_param_list_info = params;
     */
    let mut estate = backend_executor_execUtils_seams::create_executor_state::call(mcx)?;
    estate.es_param_list_info = params;
    let econtext = backend_executor_execUtils_seams::create_expr_context::call(&mut estate)?;

    /*
     * If we're called in non-atomic context, also ensure the argument
     * expressions run with an up-to-date snapshot.
     * PushActiveSnapshot(GetTransactionSnapshot()).
     */
    if !atomic {
        backend_utils_time_snapmgr_seams::push_active_snapshot_transaction::call()?;
    }

    let mut argvals: Vec<Datum<'mcx>> = Vec::with_capacity(nargs);
    let mut argnulls: Vec<bool> = Vec::with_capacity(nargs);
    for arg in fexpr.args.iter() {
        let mut exprstate =
            backend_executor_execExpr_seams::exec_prepare_expr::call(arg, &mut estate)?;
        let (val, isnull) =
            backend_executor_execExprInterp_seams::exec_eval_expr_switch_context::call(
                &mut exprstate,
                econtext,
                &mut estate,
            )?;
        argvals.push(val);
        argnulls.push(isnull);
    }

    /* Get rid of temporary snapshot for arguments, if we made one. */
    if !atomic {
        backend_utils_time_snapmgr_seams::pop_active_snapshot::call()?;
    }

    /*
     * Here we actually call the procedure.
     *
     * C builds the fcinfo with `fcinfo->context = (Node *) callcontext` so the
     * language handler's `IsA(fcinfo->context, CallContext)` demux fires and
     * reads `callcontext->atomic`. The by-OID fmgr dispatch builds the callee
     * frame itself, so the CallContext tag + atomic flag ride the RAII
     * thread-local channel (CallContextTagGuard::install_call), exactly as the
     * trigger dispatcher stamps T_TriggerData. `fexpr` is the call node
     * `fmgr_info_set_expr` stamps (carried erased as the polymorphic fn_expr).
     */
    let fn_expr = clone_funcexpr_erased(mcx, fexpr)?;
    let retval;
    let isnull;
    {
        let _ctx_guard =
            types_fmgr::fmgr::CallContextTagGuard::install_call(T_CALL_CONTEXT, call_atomic);
        let (r, n) = backend_utils_fmgr_fmgr_seams::function_call_invoke_datum::call(
            mcx,
            funcid,
            inputcollid,
            &argvals,
            &argnulls,
            Some(fn_expr),
        )?;
        retval = r;
        isnull = n;
    }

    /* Handle the procedure's outputs. */
    if funcresulttype == VOIDOID {
        /* do nothing */
    } else if funcresulttype == RECORDOID {
        /* send tuple to client */
        if isnull {
            return Err(PgError::error("procedure returned null record"));
        }

        /*
         * Ensure there's an active snapshot whilst we execute whatever's
         * involved here.
         */
        backend_tcop_pquery_seams::ensure_portal_snapshot_exists::call()?;

        /*
         * td = DatumGetHeapTupleHeader(retval); tupType/tupTypmod from the
         * record header; retdesc = lookup_rowtype_tupdesc(tupType, tupTypmod);
         * deform the returned record into columns. `deform_record_datum`
         * performs the header decode + rowtype lookup + deform as one step.
         */
        let (tup_type, tup_typmod, values, nulls) =
            backend_executor_execTuples_seams::deform_record_datum::call(mcx, retval)?;
        let retdesc = backend_utils_cache_typcache_seams::lookup_rowtype_tupdesc::call(
            mcx, tup_type, tup_typmod,
        )?;
        let retdesc_opt: TupleDesc<'mcx> = Some(retdesc);

        /*
         * begin_tup_output_tupdesc(dest, retdesc, &TTSOpsVirtual): the owned
         * do_tup_output stores the deformed columns as a virtual tuple, so the
         * output slot is a virtual slot.
         */
        let mut tstate = backend_executor_execTuples_seams::begin_tup_output_tupdesc::call(
            mcx,
            dest,
            retdesc_opt,
            types_nodes::TupleSlotKind::Virtual,
        )?;
        backend_executor_execTuples_seams::do_tup_output::call(mcx, &mut tstate, &values, &nulls)?;
        backend_executor_execTuples_seams::end_tup_output::call(mcx, tstate)?;
    } else {
        return Err(PgError::error(format!(
            "unexpected result type for procedure: {funcresulttype}"
        )));
    }

    /* FreeExecutorState(estate). */
    backend_executor_execUtils_seams::free_executor_state::call(estate)?;
    Ok(())
}

/// Build the polymorphic `fn_expr` handle C stamps with
/// `fmgr_info_set_expr((Node *) fexpr, &flinfo)`: a deep clone of the FuncExpr
/// erased into an [`FnExprErased`] (the `expr_type`/polymorphic-resolution
/// readers downcast it back to `Expr`).
fn clone_funcexpr_erased<'mcx>(
    mcx: Mcx<'mcx>,
    fexpr: &FuncExpr,
) -> PgResult<types_core::fmgr::FnExprErased> {
    let cloned = Expr::FuncExpr(fexpr.clone()).clone_in(mcx)?;
    Ok(types_core::fmgr::FnExprErased::new(cloned))
}

// ===========================================================================
// CallStmtResultDesc (functioncmds.c:2383)
// ===========================================================================

/// `CallStmtResultDesc(stmt)` (functioncmds.c:2383).
///
/// Builds the result `TupleDesc` for a CALL from the procedure's declared
/// OUT/INOUT params (`build_function_result_tupdesc_t`), then re-types each
/// column from `stmt->outargs[i]` via `exprType` to get the right concrete
/// types in polymorphic cases. `None` when there are no OUT args.
pub fn CallStmtResultDesc<'mcx>(mcx: Mcx<'mcx>, stmt_node: &Node<'mcx>) -> PgResult<TupleDesc<'mcx>> {
    let fexpr = callstmt_funcexpr(stmt_node)?;
    let funcid = fexpr.funcid;

    let tupdesc =
        backend_utils_fmgr_funcapi_seams::build_function_result_tupdesc_t::call(mcx, funcid)?;

    /*
     * The result of build_function_result_tupdesc_t has the right column names
     * but the declared output-argument types, wrong in polymorphic cases. Get
     * the correct types from stmt->outargs. tupdesc is None if no outargs.
     */
    let Some(mut td) = tupdesc else {
        return Ok(None);
    };

    let stmt = stmt_node.expect_callstmt();
    let natts = td.natts as usize;
    debug_assert_eq!(natts, stmt.outargs.len());

    for i in 0..natts {
        let attname = String::from_utf8_lossy(td.attr(i).attname.name_str()).into_owned();
        let outarg = stmt
            .outargs
            .get(i)
            .ok_or_else(|| PgError::error("CallStmtResultDesc: outargs/tupdesc length mismatch"))?;
        let outarg_expr = outarg.as_expr();
        let typ = backend_nodes_core::nodefuncs::expr_type(outarg_expr)?;
        backend_access_common_tupdesc_seams::tuple_desc_init_entry::call(
            &mut td,
            (i + 1) as i16,
            Some(&attname),
            typ,
            -1,
            0,
        )?;
    }

    Ok(Some(td))
}

// ===========================================================================
// Seam adapters (backend_tcop_utility_out_seams::{execute_call_stmt,
// call_stmt_result_desc}).
// ===========================================================================

/// Seam adapter for `backend_tcop_utility_out_seams::execute_call_stmt`. The
/// live-portal `params`/`dest` are threaded straight through; the CALL node is
/// the live `T_CallStmt` parse tree.
pub fn execute_call_stmt_seam<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &Node<'mcx>,
    params: ParamListInfo,
    atomic: bool,
    dest: DestReceiverHandle,
) -> PgResult<()> {
    ExecuteCallStmt(mcx, stmt, params, atomic, dest)
}

/// Seam adapter for `backend_tcop_utility_out_seams::call_stmt_result_desc`.
pub fn call_stmt_result_desc_seam<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &Node<'mcx>,
) -> PgResult<TupleDesc<'mcx>> {
    CallStmtResultDesc(mcx, stmt)
}
