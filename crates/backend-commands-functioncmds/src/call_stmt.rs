//! CALL family of `backend/commands/functioncmds.c` (Family 4).
//!
//! `ExecuteCallStmt` (CALL proc resolution, fcinfo setup, atomic snapshot
//! push/pop, result-tuple dispatch) and `CallStmtResultDesc` (polymorphic
//! output-argument tuple descriptor).
//!
//! DECOMP STATUS — both bodies are genuine unported-owner work, not
//! functioncmds' own logic, so they cross seams to their *real* owners (the
//! re-home this decomp performs — they no longer live in this crate's
//! `*-seams` crate):
//!
//!   * `ExecuteCallStmt` — re-homed to `backend-executor-execMain-seams`
//!     (`execute_call_stmt`). The entire body operates on the unported planner
//!     expression node `stmt->funcexpr` (`FuncExpr.funcid` / `args` /
//!     `inputcollid` / `funcresulttype` — not modelled by the layered node
//!     tree; `CallStmt.funcexpr` is opaque here), the unported execExpr
//!     evaluation (`ExecPrepareExpr` / `ExecEvalExprSwitchContext`), the fmgr
//!     CALL invocation (`fmgr_info` / `InitFunctionCallInfoData` /
//!     `FunctionCallInvoke`), the snapmgr non-atomic snapshot push/pop, the
//!     funcapi/typcache RECORD result handling, and — crucially — the *runtime*
//!     `ParamListInfo params` / `DestReceiver *dest`, which are live portal
//!     values that are NOT part of the owned CALL parse tree. The executor's
//!     CALL runtime owns all of this; the owner threads `params`/`dest` from the
//!     live portal.
//!   * `CallStmtResultDesc` — re-homed to `backend-nodes-core-seams`
//!     (`call_stmt_result_desc`), the `backend-nodes-core` owner. The function is
//!     keyed entirely by the unported
//!     `fexpr->funcid` (`CallStmt.funcexpr` is opaque here — the layered node
//!     tree does not model `FuncExpr`), runs `build_function_result_tupdesc_t`
//!     over the PROCOID tuple, then per-column re-types from `stmt->outargs` via
//!     `exprType` — all nodeFuncs/nodes-core expression-tree territory, not
//!     functioncmds logic.

use mcx::Mcx;
use types_error::PgResult;
use types_parsenodes::CallStmt;
use types_tuple::TupleDesc;

use backend_executor_execMain_seams as exec_seam;
use backend_nodes_core_seams as nodefuncs_seam;

// ===========================================================================
// ExecuteCallStmt (functioncmds.c:2206)
// ===========================================================================

/// `ExecuteCallStmt(stmt, params, atomic, dest)` (functioncmds.c:2206).
///
/// The body is owned by the executor's CALL runtime (see module docs); it
/// crosses the `backend-executor-execMain` seam, which additionally threads the
/// live-portal `params`/`dest` not carried by the owned `CallStmt`.
pub fn ExecuteCallStmt(stmt: &CallStmt, atomic: bool) -> PgResult<()> {
    exec_seam::execute_call_stmt::call(stmt.clone(), atomic)
}

// ===========================================================================
// CallStmtResultDesc (functioncmds.c:2383)
// ===========================================================================

/// `CallStmtResultDesc(stmt)` (functioncmds.c:2383).
///
/// The body is nodeFuncs/nodes-core expression-tree machinery (see module
/// docs); it crosses the `backend-nodes-nodeFuncs` seam.
pub fn CallStmtResultDesc<'mcx>(mcx: Mcx<'mcx>, stmt: &CallStmt) -> PgResult<TupleDesc<'mcx>> {
    nodefuncs_seam::call_stmt_result_desc::call(mcx, stmt.clone())
}
