//! CALL family of `backend/commands/functioncmds.c` (Family 4).
//!
//! `ExecuteCallStmt` (CALL proc resolution, fcinfo setup, atomic snapshot
//! push/pop, result-tuple dispatch) and `CallStmtResultDesc` (polymorphic
//! output-argument tuple descriptor).
//!
//! DECOMP STATUS — the genuine remaining work for this unit. Both bodies are
//! currently delegated to the `execute_call_stmt` / `call_stmt_result_desc`
//! seams; the in-crate port is blocked on FuncExpr field access + a CallContext
//! carrier node + fcinfo/FmgrInfo ABI in the layered `types-*` stack and on
//! seams to genuinely-unported owners: the executor (CreateExecutorState,
//! CreateExprContext, ExecPrepareExpr, ExecEvalExprSwitchContext,
//! FreeExecutorState, begin/end_tup_output, ExecStoreHeapTuple), fmgr
//! (fmgr_info, InitFunctionCallInfoData, FunctionCallInvoke), snapmgr
//! (Push/PopActiveSnapshot, GetTransactionSnapshot, EnsurePortalSnapshotExists),
//! typcache/funcapi (lookup_rowtype_tupdesc, build_function_result_tupdesc_t),
//! and pgstat (pgstat_init/end_function_usage). All ARE merged in this repo, so
//! the in-crate port wires real seams rather than re-stubbing.

use backend_commands_functioncmds_seams as seam;
use mcx::Mcx;
use types_error::PgResult;
use types_parsenodes::CallStmt;
use types_tuple::TupleDesc;

// ===========================================================================
// ExecuteCallStmt (functioncmds.c:2205)
// ===========================================================================

/// `ExecuteCallStmt(stmt, params, atomic, dest)` (functioncmds.c:2205).
///
/// TODO(decomp Family 4): port the body in-crate — ACL_EXECUTE check, syscache
/// PROCOID fetch + proconfig/prosecdef -> force-atomic, FUNC_MAX_ARGS guard,
/// fmgr_info + InitFunctionCallInfoData, executor-state arg eval under a pushed
/// snapshot when non-atomic, FunctionCallInvoke with pgstat usage, then the
/// VOID / RECORD (tuple-to-dest) / unexpected-type result dispatch.
pub fn ExecuteCallStmt(stmt: &CallStmt, atomic: bool) -> PgResult<()> {
    seam::execute_call_stmt::call(stmt.clone(), atomic)
}

// ===========================================================================
// CallStmtResultDesc (functioncmds.c:2382)
// ===========================================================================

/// `CallStmtResultDesc(stmt)` (functioncmds.c:2382).
///
/// TODO(decomp Family 4): port the body in-crate — syscache PROCOID fetch,
/// build_function_result_tupdesc_t, then re-type each output column from
/// `stmt->outargs` via `exprType` (typmod -1, default collation).
pub fn CallStmtResultDesc<'mcx>(mcx: Mcx<'mcx>, stmt: &CallStmt) -> PgResult<TupleDesc<'mcx>> {
    seam::call_stmt_result_desc::call(mcx, stmt.clone())
}
