//! `execExpr-json` family — SQL/JSON expression init.
//!
//! Owns `ExecInitJsonExpr` and `ExecInitJsonCoercion` (the `EEOP_JSONEXPR_PATH`
//! / `EEOP_JSONEXPR_COERCION` / `EEOP_JSONEXPR_COERCION_FINISH` step emission).
//! These are reached from the core `ExecInitExprRec` switch (the `T_JsonExpr`
//! arm dispatches here), not through a dedicated cross-unit seam.
//!
//! Porting status — the parse-node side is now present (the keystone Expr-enum
//! expansion carries faithful `JsonExpr` / `JsonReturning` / `JsonBehavior`
//! primnodes), but TWO genuine cross-unit dependencies are still unported and
//! block a faithful body:
//!
//!  1. The runtime-state structs `JsonExprState` / `JsonPathVariable` /
//!     `ErrorSaveContext` (execExpr.h SQL/JSON state group) do not exist in the
//!     repo. `ExecInitJsonExpr`'s whole job is `jsestate =
//!     palloc0(sizeof(JsonExprState))` followed by populating
//!     `jsestate->{formatted_expr,pathspec,args,escontext,jump_*}` and stashing
//!     the pointer in `EEOP_JSONEXPR_PATH`'s `d.jsonexpr.jsestate`. The keystone
//!     deliberately parks that member as `ExprEvalStepData::JsonExpr { jsestate:
//!     usize }` (opaque address) because the owning state unit has not landed —
//!     fabricating the struct here would *introduce* opacity / own the wrong
//!     unit's type, which the porting rules forbid.
//!  2. The core family's `ExecInitExprRec` recursion into *distinct* result
//!     cells (`&jsestate->formatted_expr.value`, `&jsestate->pathspec.value`,
//!     `&var->value`, the ON ERROR / ON EMPTY behavior expressions). The core
//!     spine's `exec_init_expr_rec` is private to `execExpr_core`, is not
//!     exported, and explicitly panics for any node whose compilation needs a
//!     distinct (non-`state->resvalue`) output cell — the result-cell arena that
//!     would back those distinct cells is not landed for this case.
//!
//! Per "Mirror PG and panic", each entry below carries the full C step-emission
//! sequence as a structural comment and a loud seam-and-panic body until those
//! two owners land (it is neither a silent stub nor an invented opaque
//! stand-in). The genuine downstream cross-unit callees (`get_typtype`,
//! `getTypeInputInfo` + `fmgr_info` for the IO-coercion path, `getBaseType` /
//! `DomainHasConstraints` for the coercion-step flags) will route through their
//! owner seams once the two blockers above are resolved. No seam is installed
//! from this family yet; it exists so the JSON emission code has its home when
//! the spine starts emitting steps.

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
    // The `JsonExpr` / `JsonReturning` / `JsonBehavior` parse nodes are now
    // present (keystone Expr-enum expansion), but this body still requires two
    // genuinely unported cross-unit dependencies: (1) the `JsonExprState` /
    // `JsonPathVariable` / `ErrorSaveContext` runtime-state structs (execExpr.h
    // SQL/JSON state group, owned by a still-unported unit; the keystone parks
    // the back-pointer as `ExprEvalStepData::JsonExpr { jsestate: usize }`), and
    // (2) the core family's `ExecInitExprRec` recursion into *distinct* result
    // cells (`&jsestate->formatted_expr.value`, the PASSING `&var->value`, the ON
    // ERROR / ON EMPTY behavior expressions) — `exec_init_expr_rec` is private to
    // `execExpr_core` and panics for any distinct-cell output target because the
    // result-cell arena is not landed for that case. The signature above does not
    // yet carry `jsexpr` / `resv` / `resnull` / `scratch` for the same reason.
    // Per "Mirror PG and panic", this is a loud seam-and-panic until those two
    // owners land — not a silent stub and not an invented opaque stand-in.
    panic!(
        "execExpr-json: ExecInitJsonExpr — the JsonExprState/JsonPathVariable/\
         ErrorSaveContext runtime-state types (execExpr.h SQL/JSON state group) \
         and the core-family ExecInitExprRec distinct-result-cell emission spine \
         are not yet ported"
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
    // The step-build/push body is otherwise own-logic (`ExprEvalPushStep` is
    // ported in `execExpr_core`), but it depends on the `ErrorSaveContext`
    // `escontext` threaded in from the caller's still-unported `JsonExprState`,
    // and on the `getBaseType` / `DomainHasConstraints` lsyscache helpers routed
    // through their owner seams. Its only caller is `ExecInitJsonExpr`, which
    // cannot produce `returning` / `escontext` / `resv` / `resnull` until the
    // `JsonExprState` runtime-state group lands, so the signature does not yet
    // carry them. Loud seam-and-panic until those owners land.
    panic!(
        "execExpr-json: ExecInitJsonCoercion — the ErrorSaveContext escontext \
         (threaded from the unported JsonExprState) and the getBaseType/\
         DomainHasConstraints lsyscache owner seams are not yet ported"
    )
}
