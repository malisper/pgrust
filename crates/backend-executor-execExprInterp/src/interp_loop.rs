//! The main interpreter loop (`ExecInterpExpr`, `execExprInterp.c` lines
//! 460–2287). Split out of [`crate::dispatch`] because the single C function is
//! ~1800 lines — far past the per-function size that keeps a family module
//! reviewable. The surrounding interpreter core (ready/still-valid/slot checks)
//! stays in `dispatch`; this module holds only the step-program walk and writes
//! its result through the same owned-step conventions documented there.

use types_datum::Datum;
use types_error::PgResult;
use types_nodes::execexpr::ExprState;
use types_nodes::execnodes::EcxtId;
use types_nodes::EStateData;

/// `ExecInterpExpr(ExprState *state, ExprContext *econtext, bool *isnull)` —
/// the main interpreter: walk the step program and return the result datum and
/// its null flag.
pub fn ExecInterpExpr<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    todo!("decomp")
}
