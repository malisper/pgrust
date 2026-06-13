//! XML and JSON opcode evaluators (`execExprInterp.c`): XmlExpr, the SQL/JSON
//! constructors and predicates, JSON_VALUE/JSON_QUERY/JSON_EXISTS path
//! evaluation, and the JSON coercion steps.

use mcx::{Mcx, PgString};
use types_error::PgResult;
use types_nodes::execexpr::ExprState;
use types_nodes::execnodes::EcxtId;
use types_nodes::EStateData;

/// `ExecEvalXmlExpr(ExprState *state, ExprEvalStep *op)` — evaluate an
/// XMLELEMENT / XMLFOREST / XMLPARSE / etc. expression.
pub fn ExecEvalXmlExpr<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalJsonConstructor(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — JSON / JSONB object/array constructor.
pub fn ExecEvalJsonConstructor<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalJsonIsPredicate(ExprState *state, ExprEvalStep *op)` —
/// `IS JSON [VALUE|OBJECT|ARRAY|SCALAR]` predicate.
pub fn ExecEvalJsonIsPredicate<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `ExecEvalJsonExprPath(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — run a jsonpath for JSON_VALUE/QUERY/EXISTS,
/// choosing the success/error/empty coercion jump.
pub fn ExecEvalJsonExprPath<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<i32> {
    todo!("decomp")
}

/// `ExecGetJsonValueItemString(JsonbValue *item, bool *resnull)` — render a
/// scalar jsonb item as its text form. Allocates the result string.
///
/// The `JsonbValue` argument is owned by the jsonb adt unit (not yet ported);
/// the JSON-family body agent defines the trimmed real type when filling this
/// in. Until then the parameter is the raw jsonb `Datum` the caller holds.
pub fn ExecGetJsonValueItemString<'mcx>(
    mcx: Mcx<'mcx>,
    item: types_datum::Datum,
) -> PgResult<(Option<PgString<'mcx>>, bool)> {
    todo!("decomp")
}

/// `ExecEvalJsonCoercion(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — coerce a JSON path result to the output type.
pub fn ExecEvalJsonCoercion<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}

/// `GetJsonBehaviorValueString(JsonBehavior *behavior)` — text of an ON
/// ERROR / ON EMPTY behavior for error messages. Allocates the string.
///
/// The `JsonBehavior` node is owned by the parsenodes/SQL-JSON unit (not yet
/// ported); the JSON-family body agent reads it off the step's compiled
/// payload, so the helper takes the owning `ExprState` + step index here.
pub fn GetJsonBehaviorValueString<'mcx>(
    mcx: Mcx<'mcx>,
    state: &ExprState<'mcx>,
    op: usize,
) -> PgResult<PgString<'mcx>> {
    todo!("decomp")
}

/// `ExecEvalJsonCoercionFinish(ExprState *state, ExprEvalStep *op)` — finalize
/// a JSON coercion that needed a sub-expression evaluation.
pub fn ExecEvalJsonCoercionFinish<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    todo!("decomp")
}
