//! Interpreter core (`execExprInterp.c`): make a compiled `ExprState` ready,
//! the main step dispatch loop, the still-valid revalidation path, and the
//! slot-compatibility / cached-rowtype helpers.
//!
//! Owned-model conventions shared by every per-opcode evaluator in this crate:
//! - A step is addressed by its index `op` into `state.steps`; bodies access
//!   the instruction via `state.steps[op]` and write the result through the
//!   step's [`ResultSlot`]. Passing the index (not `&mut ExprEvalStep`) avoids
//!   aliasing the `ExprState` and the step at once, which C does through raw
//!   pointers.
//! - `econtext` is an [`EcxtId`] into the EState's expr-context pool; the
//!   interpreter reads its linked tuples and runs in its per-tuple memory.
//! - Evaluation can `ereport(ERROR)`, so evaluators return [`PgResult`].

use types_datum::Datum;
use types_core::primitive::Oid;
use types_error::PgResult;
use types_nodes::execexpr::{ExprEvalOp, ExprEvalStep, ExprState};
use types_nodes::execnodes::EcxtId;
use types_nodes::executor::TupleTableSlot;
use types_nodes::EStateData;
use types_tuple::heaptuple::TupleDescData;

/// `ExecReadyInterpretedExpr(ExprState *state)` — finalize a compiled
/// expression: pick the specialized `ExecJust*` evalfunc when the step pattern
/// matches one, else the general `ExecInterpExpr`, and run interpreter setup.
pub fn ExecReadyInterpretedExpr(state: &mut ExprState<'_>) -> PgResult<()> {
    todo!("decomp")
}

// `ExecInterpExpr` — the ~1800-line main interpreter loop — lives in its own
// [`crate::interp_loop`] module (re-exported below) so this family module stays
// reviewable.
pub use crate::interp_loop::ExecInterpExpr;

/// `ExecInterpExprStillValid(ExprState *state, ExprContext *econtext,
/// bool *isNull)` — the evalfunc installed when compiled state must be
/// revalidated before each first use; checks then dispatches to the real
/// evalfunc.
pub fn ExecInterpExprStillValid<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum, bool)> {
    todo!("decomp")
}

/// `CheckExprStillValid(ExprState *state, ExprContext *econtext)` — verify the
/// slot types referenced by the compiled steps still match the econtext's
/// current tuples.
pub fn CheckExprStillValid<'mcx>(
    state: &ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `CheckVarSlotCompatibility(TupleTableSlot *slot, int attnum, Oid vartype)` —
/// assert a Var's slot/attno/type expectation against the actual slot.
pub fn CheckVarSlotCompatibility(
    slot: &TupleTableSlot,
    attnum: i32,
    vartype: Oid,
) -> PgResult<()> {
    todo!("decomp")
}

/// `CheckOpSlotCompatibility(ExprEvalStep *op, TupleTableSlot *slot)` — assert
/// a FETCHSOME step's cached descriptor/kind matches the slot it will read.
pub fn CheckOpSlotCompatibility(op: &ExprEvalStep<'_>, slot: &TupleTableSlot) -> PgResult<()> {
    todo!("decomp")
}

/// `get_cached_rowtype(Oid type_id, int32 typmod, ExprEvalRowtypeCache *cache,
/// ExprContext *econtext)` — look up and cache the `TupleDesc` for a composite
/// type referenced by a step.
pub fn get_cached_rowtype<'mcx>(
    type_id: Oid,
    typmod: i32,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<&'mcx TupleDescData<'mcx>> {
    todo!("decomp")
}

/// `ExecInitInterpreter(void)` — one-time interpreter initialization (the
/// computed-goto dispatch table in C; a no-op-equivalent setup in the port).
pub fn ExecInitInterpreter() {
    todo!("decomp")
}

/// `ExecEvalStepOp(ExprState *state, ExprEvalStep *op)` — recover the
/// `ExprEvalOp` of a step (used by JIT / debugging; reverses the computed-goto
/// opcode overlay).
pub fn ExecEvalStepOp(state: &ExprState<'_>, op: &ExprEvalStep<'_>) -> ExprEvalOp {
    todo!("decomp")
}

/// `dispatch_compare_ptr(const void *a, const void *b)` — qsort comparator used
/// when building the computed-goto dispatch table.
pub fn dispatch_compare_ptr(a: usize, b: usize) -> core::cmp::Ordering {
    todo!("decomp")
}
