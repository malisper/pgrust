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
    pub fn exec_make_function_result_set<'mcx>(
        fcache: &mut types_nodes::execexpr::SetExprState<'mcx>,
        econtext: types_nodes::EcxtId,
        arg_context: &mcx::MemoryContext,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<(
        types_datum::Datum,
        bool,
        types_nodes::execexpr::ExprDoneCond,
    )>
);
