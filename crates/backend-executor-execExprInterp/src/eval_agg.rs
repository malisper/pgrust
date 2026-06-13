//! Aggregate-transition helpers (`execExprInterp.c`): the inlined transition
//! machinery the AGG_* opcodes call (`ExecInterpExpr` dispatches to these),
//! plus the presorted-distinct filters. These operate on the nodeAgg-owned
//! `AggState` / per-trans / per-group state (boundary slice in
//! [`types_nodes::nodeagg`]).
//!
//! `op` is the step index into `state.steps`; the helpers also receive the
//! `AggState` they transition. Returns `PgResult<()>` (transition functions
//! and datum copies can `ereport`).

use types_error::PgResult;
use types_nodes::execexpr::ExprState;
use types_nodes::nodeagg::{AggStateData as AggState, AggStatePerGroupData};
use types_nodes::EStateData;

/// `ExecAggInitGroup(AggState *aggstate, AggStatePerTrans pertrans,
/// AggStatePerGroup pergroup, ExprContext *aggcontext)` — initialize a group's
/// transition value from the first input row (strict-init transition).
pub fn ExecAggInitGroup<'mcx>(
    aggstate: &mut AggState<'mcx>,
    pertrans: usize,
    pergroup: &mut AggStatePerGroupData,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecAggCopyTransValue(AggState *aggstate, AggStatePerTrans pertrans,
/// Datum newValue, bool newValueIsNull, Datum oldValue, bool oldValueIsNull)` —
/// copy a new transition value into the aggregate context and free the old.
pub fn ExecAggCopyTransValue<'mcx>(
    aggstate: &mut AggState<'mcx>,
    pertrans: usize,
    new_value: types_datum::Datum,
    new_value_is_null: bool,
    old_value: types_datum::Datum,
    old_value_is_null: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<types_datum::Datum> {
    todo!("decomp")
}

/// `ExecEvalPreOrderedDistinctSingle(AggState *aggstate,
/// AggStatePerTrans pertrans)` — single-column DISTINCT filter over presorted
/// input; returns whether the current value is distinct from the last.
pub fn ExecEvalPreOrderedDistinctSingle<'mcx>(
    aggstate: &mut AggState<'mcx>,
    pertrans: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    todo!("decomp")
}

/// `ExecEvalPreOrderedDistinctMulti(AggState *aggstate,
/// AggStatePerTrans pertrans)` — multi-column DISTINCT filter over presorted
/// input.
pub fn ExecEvalPreOrderedDistinctMulti<'mcx>(
    aggstate: &mut AggState<'mcx>,
    pertrans: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    todo!("decomp")
}

/// `ExecEvalAggOrderedTransDatum(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — tuplesort-feed a single ORDER BY / DISTINCT
/// aggregate input datum.
pub fn ExecEvalAggOrderedTransDatum<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: types_nodes::execnodes::EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalAggOrderedTransTuple(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — tuplesort-feed a multi-column aggregate input row.
pub fn ExecEvalAggOrderedTransTuple<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: types_nodes::execnodes::EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecAggPlainTransByVal(AggState *aggstate, AggStatePerTrans pertrans,
/// AggStatePerGroup pergroup, ExprContext *aggcontext, int setno)` — pass-by-
/// value plain transition (inlined fast path for the AGG_PLAIN_TRANS opcodes).
pub fn ExecAggPlainTransByVal<'mcx>(
    aggstate: &mut AggState<'mcx>,
    pertrans: usize,
    pergroup: &mut AggStatePerGroupData,
    aggcontext: types_nodes::execnodes::EcxtId,
    setno: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecAggPlainTransByRef(...)` — pass-by-reference plain transition.
pub fn ExecAggPlainTransByRef<'mcx>(
    aggstate: &mut AggState<'mcx>,
    pertrans: usize,
    pergroup: &mut AggStatePerGroupData,
    aggcontext: types_nodes::execnodes::EcxtId,
    setno: i32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}
