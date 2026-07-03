// backend-executor-execExpr + backend-executor-execExprInterp (execExpr.c
// compile-to-steps + execExprInterp.c). Ported step families: DONE_RETURN/
// DONE_NO_RETURN, INNER/OUTER/SCAN_FETCHSOME, INNER/OUTER/SCAN_VAR,
// ASSIGN_*_VAR, ASSIGN_TMP[_MAKE_RO], CONST, FUNCEXPR[_STRICT[_1|_2]], QUAL.
// Deferred families (loud-panic at compile): WHOLEROW, SYSVAR, OLD/NEW
// (RETURNING), PARAM_* (ParamListInfo), BOOL_AND/OR/NOT + JUMP_* + NULLTEST +
// BOOLTEST + CASE/COALESCE (BoolExpr/NullTest/CaseExpr node families
// unported), FUSAGE, SQLVALUEFUNCTION, row/array/subscript/domain/hash/
// json/xml/agg/window/subplan sets.
#![allow(clippy::too_many_arguments)]

extern crate alloc;

mod compile;
mod interp;
mod steps;
#[cfg(test)]
mod tests;

pub use compile::{
    exec_build_projection_info, exec_init_expr, exec_init_qual, expr_type, INDEX_VAR, INNER_VAR,
    OUTER_VAR,
};
pub use interp::{exec_eval_expr, exec_project, exec_qual, EvalSlots};
pub use steps::{CmpOp, ExprState, Kernel, OutRef, SlotSrc, Step};
