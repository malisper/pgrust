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

/// Install this unit's seams. The interpreter is reached through the
/// `ExprState::evalfunc` entry point that the compiler installs, not through a
/// seam of its own, so there is nothing to install yet — kept as the
/// single wiring slot per the seams-init convention.
pub fn init_seams() {}
