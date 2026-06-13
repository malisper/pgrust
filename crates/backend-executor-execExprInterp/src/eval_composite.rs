//! Row / composite-value opcode evaluators (`execExprInterp.c`): RowExpr,
//! NullTest on rows, FieldSelect, FieldStore deform/form, ConvertRowtype,
//! WholeRowVar, and GREATEST/LEAST (MinMax).

use types_error::PgResult;
use types_nodes::execexpr::ExprState;
use types_nodes::execnodes::EcxtId;
use types_nodes::EStateData;

/// `ExecEvalRowNull(ExprState *state, ExprEvalStep *op, ExprContext *econtext)`
/// — `IS NULL` test on a row value.
pub fn ExecEvalRowNull<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalRowNotNull(...)` — `IS NOT NULL` test on a row value.
pub fn ExecEvalRowNotNull<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalRowNullInt(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext, bool checkisnull)` — shared body for the row
/// null/not-null tests.
pub fn ExecEvalRowNullInt<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    checkisnull: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalRow(ExprState *state, ExprEvalStep *op)` — build a composite Datum
/// from the per-column results of a RowExpr.
pub fn ExecEvalRow<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalMinMax(ExprState *state, ExprEvalStep *op)` — GREATEST / LEAST.
pub fn ExecEvalMinMax<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalFieldSelect(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — extract one field from a composite value.
pub fn ExecEvalFieldSelect<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalFieldStoreDeForm(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — deform the composite value a FieldStore updates.
pub fn ExecEvalFieldStoreDeForm<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalFieldStoreForm(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — re-form the composite value after field updates.
pub fn ExecEvalFieldStoreForm<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalConvertRowtype(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — coerce a composite value to another rowtype.
pub fn ExecEvalConvertRowtype<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalWholeRowVar(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — materialize a whole-row Var as a composite Datum.
pub fn ExecEvalWholeRowVar<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}
