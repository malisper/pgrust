//! `execExpr-json` family — SQL/JSON expression init.
//!
//! Owns `ExecInitJsonExpr` and `ExecInitJsonCoercion` (the `EEOP_JSONEXPR_PATH`
//! / `EEOP_JSONEXPR_COERCION` / `EEOP_JSON_CONSTRUCTOR` / `EEOP_IS_JSON` step
//! emission). These are reached from the core `ExecInitExprRec` switch (a
//! `JsonExpr` / `JsonConstructorExpr` / `JsonIsPredicate` arm dispatches here),
//! not yet through a dedicated cross-unit seam — the JSON support types
//! (`JsonExpr` / `JsonReturning` parse nodes, `JsonExprState` /
//! `JsonPathVariable` / `ErrorSaveContext` runtime state) live in unported
//! units, and the bodies drive the core family's still-unported static emission
//! helpers `ExecInitExprRec` / `ExprEvalPushStep`. Per "Mirror PG and panic",
//! each entry below carries the full C step-emission sequence as a structural
//! comment and a loud seam-and-panic body until those owners land (it is neither
//! a silent stub nor an invented opaque stand-in). No seam is installed from
//! this family yet; it exists so the JSON emission code has its home when the
//! spine starts emitting steps.

use types_error::PgResult;
use types_nodes::execexpr::ExprState;
use types_nodes::{EStateData, EcxtId};

/// `ExecInitJsonExpr(jsexpr, state, resv, resnull, scratch)` (execExpr.c) —
/// emit the `EEOP_JSONEXPR_PATH` + coercion steps for a `JsonExpr`.
///
/// Called from the `ExecInitExprRec` `T_JsonExpr` arm (execExpr-core). The
/// `JsonExpr` node and `JsonExprState` are JSON-unit-owned; parked until those
/// types land.
pub(crate) fn exec_init_json_expr<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = (state, econtext, estate);
    // ExecInitJsonExpr(jsexpr, state, resv, resnull, scratch) (execExpr.c):
    //
    //   jsestate = palloc0(sizeof(JsonExprState));
    //   returning_domain = get_typtype(jsexpr->returning->typid) == TYPTYPE_DOMAIN;
    //   Assert(jsexpr->on_error != NULL);
    //   jsestate->jsexpr = jsexpr;
    //   ExecInitExprRec(jsexpr->formatted_expr, ...);  // -> jsestate->formatted_expr
    //   <EEOP_JUMP_IF_NULL on formatted_expr.isnull, target patched below>
    //   ExecInitExprRec(jsexpr->path_spec, ...);       // -> jsestate->pathspec
    //   <EEOP_JUMP_IF_NULL on pathspec.isnull, target patched below>
    //   forboth(passing_values, passing_names): build JsonPathVariable, ExecInitExprRec
    //   <EEOP_JSONEXPR_PATH with jsestate>
    //   patch jumps_return_null -> here; <EEOP_CONST NULL>
    //   escontext = on_error->btype != JSON_BEHAVIOR_ERROR ? &jsestate->escontext : NULL;
    //   if (use_json_coercion) ExecInitJsonCoercion(...);
    //   else if (use_io_coercion) build the input-function FunctionCallInfo (3 args);
    //   if (jump_eval_coercion >= 0 && escontext) <EEOP_JSONEXPR_COERCION_FINISH>;
    //   ON ERROR / ON EMPTY behaviour expression steps (each: JUMP_IF_NOT_TRUE,
    //     ExecInitExprRec(behavior->expr), optional ExecInitJsonCoercion + COERCION_FINISH,
    //     JUMP to end); patch jumps_to_end -> end; jsestate->jump_end = steps_len.
    //
    // This body consumes the `JsonExpr` / `JsonReturning` parse nodes and the
    // `JsonExprState` / `JsonPathVariable` / `ErrorSaveContext` runtime structs,
    // none of which are ported yet (primnodes.h JSON nodes + the execExpr.h JSON
    // state structs are owned by still-unported units), and it drives the core
    // family's static emission helpers `ExecInitExprRec` / `ExprEvalPushStep`,
    // which are likewise unported in this scaffold. The signature above does not
    // yet carry `jsexpr` / `resv` / `resnull` / `scratch` for the same reason.
    // Per "Mirror PG and panic", this is a loud seam-and-panic until those owners
    // land (the JSON node/state types and the core emission spine) — not a silent
    // stub and not an invented opaque stand-in.
    panic!(
        "execExpr-json: ExecInitJsonExpr — JsonExpr/JsonReturning/JsonExprState \
         node+state types (primnodes.h / execExpr.h JSON) and the core-family \
         ExecInitExprRec/ExprEvalPushStep emission spine are not yet ported"
    )
}

/// `ExecInitJsonCoercion(...)` (execExpr.c) — emit one `EEOP_JSONEXPR_COERCION`
/// step for a JSON output coercion.
pub(crate) fn exec_init_json_coercion<'mcx>(
    state: &mut ExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = (state, econtext, estate);
    // ExecInitJsonCoercion(state, returning, escontext, omit_quotes, exists_coerce,
    //                      resv, resnull) (execExpr.c):
    //
    //   ExprEvalStep scratch = {0};
    //   scratch.opcode = EEOP_JSONEXPR_COERCION;
    //   scratch.resvalue = resv; scratch.resnull = resnull;
    //   scratch.d.jsonexpr_coercion.targettype = returning->typid;
    //   scratch.d.jsonexpr_coercion.targettypmod = returning->typmod;
    //   scratch.d.jsonexpr_coercion.json_coercion_cache = NULL;
    //   scratch.d.jsonexpr_coercion.escontext = escontext;
    //   scratch.d.jsonexpr_coercion.omit_quotes = omit_quotes;
    //   scratch.d.jsonexpr_coercion.exists_coerce = exists_coerce;
    //   scratch.d.jsonexpr_coercion.exists_cast_to_int =
    //       exists_coerce && getBaseType(returning->typid) == INT4OID;
    //   scratch.d.jsonexpr_coercion.exists_check_domain =
    //       exists_coerce && DomainHasConstraints(returning->typid);
    //   ExprEvalPushStep(state, &scratch);
    //
    // Consumes the unported `JsonReturning` node and `ErrorSaveContext`, calls the
    // unported lsyscache helpers `getBaseType` / `DomainHasConstraints` through
    // their owner seams, and drives the unported core-family `ExprEvalPushStep`.
    // The signature does not yet carry `returning` / `escontext` / `omit_quotes`
    // / `exists_coerce` / `resv` / `resnull` for the same reason. Loud
    // seam-and-panic until those owners land.
    panic!(
        "execExpr-json: ExecInitJsonCoercion — JsonReturning/ErrorSaveContext types, \
         the getBaseType/DomainHasConstraints lsyscache seams, and the core-family \
         ExprEvalPushStep emission spine are not yet ported"
    )
}
