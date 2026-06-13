//! `backend-executor-execExprInterp` — the expression-evaluation interpreter
//! (`src/backend/executor/execExprInterp.c`).
//!
//! This unit owns the *interpreter*: the engine that walks the
//! [`ExprEvalStep`] program a compiled [`ExprState`] carries
//! (`ExecReadyInterpretedExpr` / `ExecInterpExpr`), plus all the per-opcode
//! evaluation routines and the aggregation-transition helpers. The expression
//! *compiler* that emits the step program is the sibling unit `execExpr.c`
//! (`backend-executor-execExpr`).
//!
//! Scaffold: every C function has a real, C-faithful signature with a
//! `todo!("decomp")` body. Bodies are filled per-family in parallel.
//!
//! Family split (one module per coherent group; see CATALOG notes):
//! - [`dispatch`]  — interpreter core: ready/dispatch/step-op/validity checks.
//! - [`interp_loop`] — the ~1800-line `ExecInterpExpr` step-program walk, split
//!   out of `dispatch` to keep each family module's functions reviewable.
//! - [`justs`]     — the `ExecJust*` fast-path evaluators + their Impl helpers.
//! - [`eval_scalar`]    — scalar ops: func/param/coerce/SQLValue/SAOP/sysvar/constraints.
//! - [`eval_composite`] — row/record ops: Row/RowNull/FieldSelect/FieldStore/ConvertRowtype/WholeRow/MinMax.
//! - [`eval_array`]     — ArrayExpr / ArrayCoerce.
//! - [`eval_json_xml`]  — Xml / Json constructor / IsJson / JsonExprPath / Json coercion.
//! - [`eval_misc`]      — SubPlan / GroupingFunc / MergeSupportFunc.
//! - [`eval_agg`]       — aggregate-transition + presorted-distinct helpers.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

pub mod dispatch;
pub mod eval_agg;
pub mod eval_array;
pub mod eval_composite;
pub mod eval_json_xml;
pub mod eval_misc;
pub mod eval_scalar;
pub mod interp_loop;
pub mod justs;
pub mod saophash;

/// Install this unit's seams. The interpreter owns
/// `backend-executor-execExprInterp-seams`: `exec_ready_interpreted_expr`
/// (`ExecReadyInterpretedExpr`) and `exec_eval_expr_switch_context`
/// (`ExecEvalExprSwitchContext` / the `ExecInterpExpr` dispatch loop). `execExpr`
/// (the compiler) and the executor nodes reach the interpreter through these.
///
/// `exec_ready_interpreted_expr` takes `&mut ExprState` in both the seam and
/// [`dispatch::ExecReadyInterpretedExpr`], so it is installed directly.
///
/// `exec_eval_expr_switch_context` is a tracked contract divergence: the seam
/// declares `state: &ExprState` (the C `ExecEvalExprSwitchContext` macro reads
/// `state->evalfunc`), but the owned dispatch entry
/// [`dispatch::ExecInterpExprStillValid`] needs `&mut ExprState` because the
/// still-valid first-call check and the `ExecJust*` / `ExecInterpExpr` evalfuncs
/// mutate per-eval scratch. Reconciling the shared-vs-mut mismatch is the
/// seam-contract-reconcile lane's job (DESIGN_DEBT +
/// `CONTRACT_RECONCILE_PENDING`); until then this seam stays seam-and-panic on
/// the still-uninstalled `&ExprState` surface.
pub fn init_seams() {
    backend_executor_execExprInterp_seams::exec_ready_interpreted_expr::set(
        dispatch::ExecReadyInterpretedExpr,
    );
}
