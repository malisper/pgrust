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
    // ExecInitJsonExpr(jsexpr, state, resv, resnull, scratch) (execExpr.c) —
    // line-for-line C step-emission sequence (c2rust execExpr.rs:10369-10675):
    //
    //   jsestate = palloc0(sizeof(JsonExprState));
    //   returning_domain = get_typtype(jsexpr->returning->typid) == TYPTYPE_DOMAIN;
    //   jsestate->jsexpr = jsexpr;
    //   // --- formatted_expr: query target, recursed into jsestate->formatted_expr ---
    //   ExecInitExprRec(jsexpr->formatted_expr, state,
    //                   &jsestate->formatted_expr.value, &jsestate->formatted_expr.isnull);
    //   jumps_return_null = lappend_int(jumps_return_null, state->steps_len);
    //   scratch.opcode = EEOP_JUMP_IF_NULL;
    //   scratch.resnull = &jsestate->formatted_expr.isnull;
    //   scratch.d.jump.jumpdone = -1;            ExprEvalPushStep(state, scratch);
    //   // --- path_spec: jsonpath, recursed into jsestate->pathspec ---
    //   ExecInitExprRec(jsexpr->path_spec, state,
    //                   &jsestate->pathspec.value, &jsestate->pathspec.isnull);
    //   jumps_return_null = lappend_int(jumps_return_null, state->steps_len);
    //   scratch.opcode = EEOP_JUMP_IF_NULL;
    //   scratch.resnull = &jsestate->pathspec.isnull;
    //   scratch.d.jump.jumpdone = -1;            ExprEvalPushStep(state, scratch);
    //   // --- PASSING args: forboth(passing_values, passing_names) ---
    //   jsestate->args = NIL;
    //   forboth(argexprlc, jsexpr->passing_values, argnamelc, jsexpr->passing_names) {
    //       argexpr = lfirst(argexprlc); argname = lfirst(argnamelc);
    //       var = palloc(sizeof(JsonPathVariable));
    //       var->name = argname->sval; var->namelen = strlen(var->name);
    //       var->typid = exprType(argexpr); var->typmod = exprTypmod(argexpr);
    //       ExecInitExprRec(argexpr, state, &var->value, &var->isnull);
    //       jsestate->args = lappend(jsestate->args, var);
    //   }
    //   // --- the JSONEXPR_PATH step itself ---
    //   scratch.opcode = EEOP_JSONEXPR_PATH;
    //   scratch.resvalue = resv; scratch.resnull = resnull;
    //   scratch.d.jsonexpr.jsestate = jsestate;  ExprEvalPushStep(state, scratch);
    //   // patch every jumps_return_null target -> here (steps_len)
    //   foreach(lc, jumps_return_null)
    //       state->steps[lfirst_int(lc)].d.jump.jumpdone = state->steps_len;
    //   scratch.opcode = EEOP_CONST;             // return SQL NULL on a null input
    //   scratch.resvalue = resv; scratch.resnull = resnull;
    //   scratch.d.constval.value = (Datum) 0; scratch.d.constval.isnull = true;
    //   ExprEvalPushStep(state, scratch);
    //   // --- escontext: &jsestate->escontext unless ON ERROR is ERROR ---
    //   escontext = (on_error->btype != JSON_BEHAVIOR_ERROR) ? &jsestate->escontext : NULL;
    //   jsestate->escontext.type = T_ErrorSaveContext;
    //   jsestate->jump_eval_coercion = -1;
    //   if (jsexpr->use_json_coercion) {
    //       jsestate->jump_eval_coercion = state->steps_len;
    //       ExecInitJsonCoercion(state, jsexpr->returning, escontext,
    //                            jsexpr->omit_quotes,
    //                            jsexpr->op == JSON_EXISTS_OP, resv, resnull);
    //   } else if (jsexpr->use_io_coercion) {
    //       getTypeInputInfo(jsexpr->returning->typid, &typinput, &typioparam);
    //       finfo  = palloc0(sizeof(FmgrInfo));
    //       fcinfo = palloc0(SizeForFunctionCallInfo(3));
    //       fmgr_info(typinput, finfo);
    //       finfo->fn_expr = (Node *) jsexpr->returning;
    //       fcinfo->flinfo = finfo; fcinfo->context = NULL; fcinfo->resultinfo = NULL;
    //       fcinfo->fncollation = InvalidOid; fcinfo->isnull = false; fcinfo->nargs = 3;
    //       fcinfo->args[1].value = ObjectIdGetDatum(typioparam); fcinfo->args[1].isnull = false;
    //       fcinfo->args[2].value = Int32GetDatum(jsexpr->returning->typmod);
    //       fcinfo->args[2].isnull = false;
    //       fcinfo->context = (Node *) escontext;
    //       jsestate->input_fcinfo = fcinfo;
    //   }
    //   if (jsestate->jump_eval_coercion >= 0 && escontext) {
    //       scratch.opcode = EEOP_JSONEXPR_COERCION_FINISH;
    //       scratch.d.jsonexpr.jsestate = jsestate;  ExprEvalPushStep(state, scratch);
    //   }
    //   // --- ON ERROR / ON EMPTY behaviour expressions ---
    //   jsestate->jump_error = jsestate->jump_empty = -1;
    //   if (on_error->btype != JSON_BEHAVIOR_ERROR &&
    //       (!(IsA(on_error->expr, Const) && ((Const *) on_error->expr)->constisnull)
    //        || returning_domain)) {
    //       jsestate->jump_error = state->steps_len;
    //       jumps_to_end = lappend_int(jumps_to_end, state->steps_len);
    //       scratch.opcode = EEOP_JUMP_IF_NOT_TRUE;
    //       scratch.resvalue = &jsestate->error.value; scratch.resnull = &jsestate->error.isnull;
    //       scratch.d.jump.jumpdone = -1;        ExprEvalPushStep(state, scratch);
    //       saved = state->escontext; state->escontext = escontext;
    //       ExecInitExprRec(on_error->expr, state, resv, resnull);
    //       state->escontext = saved;
    //       if (on_error->coerce)
    //           ExecInitJsonCoercion(state, jsexpr->returning, escontext,
    //                                jsexpr->omit_quotes, false, resv, resnull);
    //       if (on_error->coerce || IsA(on_error->expr, CoerceViaIO)
    //                            || IsA(on_error->expr, CoerceToDomain)) {
    //           scratch.opcode = EEOP_JSONEXPR_COERCION_FINISH;
    //           scratch.resvalue = resv; scratch.resnull = resnull;
    //           scratch.d.jsonexpr.jsestate = jsestate;  ExprEvalPushStep(state, scratch);
    //       }
    //       jumps_to_end = lappend_int(jumps_to_end, state->steps_len);
    //       scratch.opcode = EEOP_JUMP; scratch.d.jump.jumpdone = -1;
    //       ExprEvalPushStep(state, scratch);
    //   }
    //   if (on_empty != NULL && on_empty->btype != JSON_BEHAVIOR_ERROR &&
    //       (!(IsA(on_empty->expr, Const) && ((Const *) on_empty->expr)->constisnull)
    //        || returning_domain)) {
    //       jsestate->jump_empty = state->steps_len;
    //       jumps_to_end = lappend_int(jumps_to_end, state->steps_len);
    //       scratch.opcode = EEOP_JUMP_IF_NOT_TRUE;
    //       scratch.resvalue = &jsestate->empty.value; scratch.resnull = &jsestate->empty.isnull;
    //       scratch.d.jump.jumpdone = -1;        ExprEvalPushStep(state, scratch);
    //       saved = state->escontext; state->escontext = escontext;
    //       ExecInitExprRec(on_empty->expr, state, resv, resnull);
    //       state->escontext = saved;
    //       if (on_empty->coerce)
    //           ExecInitJsonCoercion(state, jsexpr->returning, escontext,
    //                                jsexpr->omit_quotes, false, resv, resnull);
    //       if (on_empty->coerce || IsA(on_empty->expr, CoerceViaIO)
    //                            || IsA(on_empty->expr, CoerceToDomain)) {
    //           scratch.opcode = EEOP_JSONEXPR_COERCION_FINISH;
    //           scratch.resvalue = resv; scratch.resnull = resnull;
    //           scratch.d.jsonexpr.jsestate = jsestate;  ExprEvalPushStep(state, scratch);
    //       }
    //   }
    //   foreach(lc, jumps_to_end)              // patch all JUMP/JUMP_IF_NOT_TRUE -> end
    //       state->steps[lfirst_int(lc)].d.jump.jumpdone = state->steps_len;
    //   jsestate->jump_end = state->steps_len;
    //
    // The `JsonExpr` / `JsonReturning` / `JsonBehavior` parse nodes are now
    // present (keystone Expr-enum expansion), but this body still requires two
    // genuinely unported cross-unit dependencies: (1) the `JsonExprState` /
    // `JsonPathVariable` / `ErrorSaveContext` runtime-state structs (execExpr.h
    // SQL/JSON state group, owned by a still-unported unit; the keystone parks
    // the back-pointer as `ExprEvalStepData::JsonExpr { jsestate: usize }`), and
    // (2) the core family's `ExecInitExprRec` recursion into *distinct* result
    // cells (`&jsestate->formatted_expr.value`, `&jsestate->pathspec.value`, the
    // PASSING `&var->value`, the ON ERROR / ON EMPTY behavior expressions) —
    // `exec_init_expr_rec` and the `new_result_cell` arena allocator are private
    // (`fn`, not `pub(crate)`) to `execExpr_core` with no cross-family entry
    // point, so a sibling family cannot allocate a distinct output cell nor drive
    // the recursion (the `execExpr_func_subscript` family records the identical
    // blocker). The signature above does not yet carry `jsexpr` / `resv` /
    // `resnull` / `scratch` for the same reason.
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
