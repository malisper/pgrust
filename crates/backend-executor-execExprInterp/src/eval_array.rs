//! Array-opcode evaluators (`execExprInterp.c`): ArrayExpr construction and
//! ArrayCoerce element-wise coercion.

use types_error::PgResult;
use types_nodes::execexpr::ExprState;
use types_nodes::execnodes::EcxtId;
use types_nodes::EStateData;

/// `ExecEvalArrayExpr(ExprState *state, ExprEvalStep *op)` — build an array
/// Datum from the per-element results of an ArrayExpr.
pub fn ExecEvalArrayExpr<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalArrayCoerce(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — coerce each element of an array to a new type.
pub fn ExecEvalArrayCoerce<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}
