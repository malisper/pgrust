//! Miscellaneous opcode evaluators (`execExprInterp.c`): SubPlan invocation,
//! GROUPING() and MERGE support functions.

use types_error::PgResult;
use types_nodes::execexpr::ExprState;
use types_nodes::execnodes::EcxtId;
use types_nodes::EStateData;

/// `ExecEvalSubPlan(ExprState *state, ExprEvalStep *op, ExprContext *econtext)`
/// — evaluate a SubPlan/AlternativeSubPlan (delegates to nodeSubplan's
/// `ExecSubPlan` through that owner's seam).
pub fn ExecEvalSubPlan<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalGroupingFunc(ExprState *state, ExprEvalStep *op)` — evaluate a
/// GROUPING() expression against the current grouping set.
pub fn ExecEvalGroupingFunc<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalMergeSupportFunc(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — evaluate MERGE_ACTION() within a MERGE command.
pub fn ExecEvalMergeSupportFunc<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}
