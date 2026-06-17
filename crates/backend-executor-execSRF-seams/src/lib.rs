//! Seam declarations for the `backend-executor-execSRF` unit
//! (`executor/execSRF.c`): the set-returning-function evaluation entry points
//! consumed by `nodeProjectSet.c` (and `nodeFunctionscan.c`).
//!
//! The owning unit (execSRF.c) installs these from its `init_seams()` when it
//! lands; until then a call panics loudly. The owned model threads
//! `&mut EStateData` explicitly in place of the C `PlanState.state`
//! back-pointer, and addresses the per-node `ExprContext` by [`EcxtId`].

#![allow(non_snake_case)]

seam_core::seam!(
    /// `ExecInitFunctionResultSet(expr, econtext, parent)` (execSRF.c): build
    /// the [`SetExprState`](types_nodes::execexpr::SetExprState) for a
    /// set-returning `FuncExpr`/`OpExpr` in a targetlist — compiling its
    /// argument expressions (`ExecInitExprList`) and looking up the target
    /// function (`init_sexpr`). The `econtext` is the id of the node's
    /// per-node `ExprContext` (the C `econtext->ecxt_per_query_memory` charges
    /// the long-lived state); `parent` is the lent plan-state. The compiled
    /// state is allocated in the per-query context; fallible on OOM and on a
    /// non-`FuncExpr`/`OpExpr` node (the C
    /// `elog(ERROR, "unrecognized node type")`).
    pub fn exec_init_function_result_set<'mcx>(
        expr: &types_nodes::primnodes::Expr,
        econtext: types_nodes::EcxtId,
        parent: &mut types_nodes::execnodes::PlanStateData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<
        mcx::PgBox<'mcx, types_nodes::execexpr::SetExprState<'mcx>>,
    >
);

seam_core::seam!(
    /// `ExecMakeFunctionResultSet(fcache, econtext, argContext, &isNull,
    /// &isDone)` (execSRF.c): evaluate the SRF's arguments and call the
    /// function, returning one result row's `(Datum, isNull, isDone)`. Must be
    /// called in a short-lived (per-tuple) context; `arg_context` must live
    /// until the row series is exhausted (`isDone` reaches `ExprEndResult` /
    /// `ExprSingleResult`). `fcache` is mutated across a value-per-call series
    /// (`setArgsValid`, the tuplestore, the cached `FmgrInfo`/`fcinfo`).
    /// Fallible on `ereport(ERROR)` from the function or argument evaluation.
    ///
    /// The function-produced result word crosses as the canonical unified value
    /// [`Datum`](types_tuple::backend_access_common_heaptuple::Datum):
    /// a `ByVal` scalar word for a pass-by-value return type, or the
    /// materialized `ByRef` payload bytes otherwise (the C `Datum` result of
    /// `FunctionCallInvoke` / the dematerialized tuplestore row).
    pub fn exec_make_function_result_set<'mcx>(
        fcache: &mut types_nodes::execexpr::SetExprState<'mcx>,
        econtext: types_nodes::EcxtId,
        arg_context: &mcx::MemoryContext,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<(
        types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
        bool,
        types_nodes::execexpr::ExprDoneCond,
    )>
);

seam_core::seam!(
    /// `ExecInitTableFunctionResult(expr, econtext, parent)` (execSRF.c): build
    /// the [`SetExprState`](types_nodes::execexpr::SetExprState) for a
    /// set-returning function in a range-table function (a `FunctionScan` /
    /// `ROWS FROM` function). Like [`exec_init_function_result_set`] but for the
    /// table-function (materialize-mode) flavour: `funcReturnsSet` is left
    /// `false` and `init_sexpr` runs lazily on the first
    /// `ExecMakeTableFunctionResult` call. `econtext` is the id of the node's
    /// per-node `ExprContext`; `parent` is the lent plan-state. The compiled
    /// state is allocated in the per-query context; fallible on OOM.
    pub fn exec_init_table_function_result<'mcx>(
        expr: &types_nodes::primnodes::Expr,
        econtext: types_nodes::EcxtId,
        parent: &mut types_nodes::execnodes::PlanStateData<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<
        mcx::PgBox<'mcx, types_nodes::execexpr::SetExprState<'mcx>>,
    >
);

seam_core::seam!(
    /// `ExecMakeTableFunctionResult(setexpr, econtext, argContext, expectedDesc,
    /// randomAccess)` (execSRF.c): evaluate a set-returning function appearing
    /// in a range-table function and return the materialized result rows in a
    /// [`Tuplestorestate`](types_nodes::funcapi::Tuplestorestate). This is the
    /// value-per-call / materialize-mode SRF execution loop that reads back the
    /// live [`ReturnSetInfo`](types_nodes::funcapi::ReturnSetInfo) the callee
    /// mutates (`returnMode`/`isDone`/`setResult`/`setDesc`); `expectedDesc` is
    /// the descriptor the caller expects, `arg_context` the (per-one-call
    /// lifetime) context arguments are evaluated in, `random_access` requests a
    /// rewindable tuplestore (the C `node->eflags & EXEC_FLAG_BACKWARD`).
    /// Fallible on `ereport(ERROR)` from the function or argument evaluation.
    ///
    /// K2 BLOCKED: the owning unit `execSRF.c` is not yet ported (the frame-based
    /// SRF invoke seam threading a live `&mut ReturnSetInfo` through by-OID
    /// `PGFunction` dispatch — the #327 dual-fcinfo-home keystone). Until it
    /// lands and installs this seam, a call panics loudly. See the memory note
    /// `execSRF-blocked-on-resultinfo-srf-callconv-keystone.md` (#349 K2).
    pub fn exec_make_table_function_result<'mcx>(
        setexpr: &mut types_nodes::execexpr::SetExprState<'mcx>,
        econtext: types_nodes::EcxtId,
        arg_context: &mut mcx::MemoryContext,
        expected_desc: &types_tuple::heaptuple::TupleDescData<'mcx>,
        random_access: bool,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_nodes::funcapi::Tuplestorestate<'mcx>>>
);
