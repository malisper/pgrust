//! Scalar-opcode evaluators (`execExprInterp.c`): function calls with usage
//! tracking, PARAM nodes, I/O coercion, SQLValueFunction, CurrentOf,
//! NextValue, system Vars, constraint checks, and the (hashed) ScalarArrayOp
//! machinery.
//!
//! Step evaluators address their instruction by index `op` into `state.steps`
//! and write the result through that step's `ResultSlot`; they return
//! `PgResult<()>` (evaluation can `ereport`). See [`crate::dispatch`] for the
//! shared owned-model conventions.

use types_datum::Datum;
use types_error::PgResult;
use types_nodes::execexpr::ExprState;
use types_nodes::execnodes::EcxtId;
use types_nodes::EStateData;

/// `ExecEvalFuncExprFusage(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — call a (non-strict) function, tracking usage stats.
pub fn ExecEvalFuncExprFusage<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalFuncExprStrictFusage(...)` — call a strict function with usage stats
/// (NULL argument short-circuits to NULL).
pub fn ExecEvalFuncExprStrictFusage<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalParamExec(ExprState *state, ExprEvalStep *op, ExprContext *econtext)`
/// — fetch a PARAM_EXEC value from the econtext's param-exec array.
pub fn ExecEvalParamExec<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalParamExtern(...)` — fetch a PARAM_EXTERN value from the param list.
pub fn ExecEvalParamExtern<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalParamSet(...)` — store a value into a PARAM_EXEC slot.
pub fn ExecEvalParamSet<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalCoerceViaIOSafe(ExprState *state, ExprEvalStep *op)` — output-then-
/// input I/O coercion with soft-error handling.
pub fn ExecEvalCoerceViaIOSafe<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalSQLValueFunction(ExprState *state, ExprEvalStep *op)` — evaluate
/// CURRENT_DATE / CURRENT_USER / etc.
pub fn ExecEvalSQLValueFunction<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalCurrentOfExpr(ExprState *state, ExprEvalStep *op)` — CURRENT OF
/// cursor reference (always errors at runtime; resolved by the scan node).
pub fn ExecEvalCurrentOfExpr<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalNextValueExpr(ExprState *state, ExprEvalStep *op)` — evaluate a
/// column DEFAULT nextval() during COPY/INSERT.
pub fn ExecEvalNextValueExpr<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalConstraintNotNull(ExprState *state, ExprEvalStep *op)` — domain
/// NOT NULL constraint check.
pub fn ExecEvalConstraintNotNull<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalConstraintCheck(ExprState *state, ExprEvalStep *op)` — single domain
/// CHECK constraint evaluation.
pub fn ExecEvalConstraintCheck<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalSysVar(ExprState *state, ExprEvalStep *op, ExprContext *econtext,
/// TupleTableSlot *slot)` — fetch a system attribute (ctid, xmin, ...).
pub fn ExecEvalSysVar<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    slot: types_nodes::SlotId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalScalarArrayOp(ExprState *state, ExprEvalStep *op)` — `x op ANY/ALL
/// (array)` by linear scan over the array elements.
pub fn ExecEvalScalarArrayOp<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalHashedScalarArrayOp(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — `x = ANY (array)` via a built hash table.
pub fn ExecEvalHashedScalarArrayOp<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `saop_element_hash(struct saophash_hash *tb, Datum key)` — hash one array
/// element for the hashed-SAOP table (simplehash callback).
pub fn saop_element_hash(key: Datum) -> u32 {
    todo!("decomp")
}

/// `saop_hash_element_match(struct saophash_hash *tb, Datum key1, Datum key2)`
/// — equality callback for the hashed-SAOP table.
pub fn saop_hash_element_match(key1: Datum, key2: Datum) -> bool {
    todo!("decomp")
}
