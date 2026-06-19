//! `execExpr-json` family — SQL/JSON expression init (`ExecInitJsonExpr` /
//! `ExecInitJsonCoercion`, execExpr.c).
//!
//! Owns the `EEOP_JSONEXPR_PATH` / `EEOP_JSONEXPR_COERCION` /
//! `EEOP_JSONEXPR_COERCION_FINISH` step emission for a `JsonExpr`
//! (`JSON_VALUE`/`JSON_QUERY`/`JSON_EXISTS`). Reached from the core
//! `ExecInitExprRec` `T_JsonExpr` arm.

use mcx::Mcx;
use types_error::PgResult;
use types_nodes::execexpr::{
    ExprEvalOp, ExprEvalStep, ExprEvalStepData, ExprState, JsonExprState, JsonExprStateId,
    JsonPathVariableState, ResultCellId,
};
use types_nodes::primnodes::{
    etag, Expr, JsonBehaviorType, JsonExpr, JsonExprOp, JsonReturning, XmlExpr,
};
use types_tuple::backend_access_common_heaptuple::Datum as DatumV;

use crate::execExpr_core::{expr_eval_push_step, exec_init_expr_rec, new_result_cell};

/// `TYPTYPE_DOMAIN` (catalog/pg_type.h).
const TYPTYPE_DOMAIN: u8 = b'd';

/// Allocate a fresh [`JsonExprState`] in `state`'s arena and return its id.
fn new_json_state<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut ExprState<'mcx>,
    js: JsonExprState<'mcx>,
) -> PgResult<JsonExprStateId> {
    if state.json_states.states.is_none() {
        state.json_states.states = Some(mcx::vec_with_capacity_in(mcx, 1)?);
    }
    let states = state.json_states.states.as_mut().unwrap();
    let id = JsonExprStateId(states.len() as u32);
    states
        .try_reserve(1)
        .map_err(|_| mcx.oom(core::mem::size_of::<JsonExprState>()))?;
    states.push(js);
    Ok(id)
}

/// `ExecInitJsonExpr(jsexpr, state, resv, resnull, scratch)` (execExpr.c:4748) —
/// push the steps to evaluate a `JsonExpr` and its subsidiary expressions.
pub(crate) fn exec_init_json_expr<'mcx>(
    mcx: Mcx<'mcx>,
    jsexpr: &JsonExpr,
    state: &mut ExprState<'mcx>,
    resv: ResultCellId,
) -> PgResult<()> {
    let returning = jsexpr
        .returning
        .as_ref()
        .expect("JsonExpr.returning must be present");
    let on_error = jsexpr
        .on_error
        .as_ref()
        .expect("JsonExpr.on_error must be present (Assert)");

    // returning_domain = get_typtype(jsexpr->returning->typid) == TYPTYPE_DOMAIN;
    let returning_domain =
        backend_utils_cache_lsyscache_seams::get_typtype::call(returning.typid)? == TYPTYPE_DOMAIN;

    // Allocate distinct result cells for the formatted_expr / pathspec values
    // (the C &jsestate->formatted_expr.value / &jsestate->pathspec.value
    // aliasing targets) and for the error / empty flags (the C
    // &jsestate->error.value / &jsestate->empty.value the JUMP_IF_NOT_TRUE
    // steps read and ExecEvalJsonExprPath writes).
    let formatted_expr_cell = new_result_cell(mcx, state)?;
    let pathspec_cell = new_result_cell(mcx, state)?;
    let error_cell = new_result_cell(mcx, state)?;
    let empty_cell = new_result_cell(mcx, state)?;

    // Reserve the jsestate slot up-front so its id is stable while we emit
    // steps; it is filled in (jump_* / args / input_fcinfo) below.
    let jsestate_id = new_json_state(
        mcx,
        state,
        JsonExprState {
            jsexpr: jsexpr.clone(),
            formatted_expr_cell,
            pathspec_cell,
            args: mcx::vec_with_capacity_in(mcx, jsexpr.passing_values.len())?,
            error_cell,
            empty_cell,
            jump_empty: -1,
            jump_error: -1,
            jump_eval_coercion: -1,
            jump_end: -1,
            input_fcinfo: None,
            escontext: types_error::SoftErrorContext::default(),
        },
    )?;

    let mut jumps_return_null: Vec<usize> = Vec::new();
    let mut jumps_to_end: Vec<usize> = Vec::new();

    // Evaluate formatted_expr into jsestate->formatted_expr.
    let formatted_expr = jsexpr
        .formatted_expr
        .as_deref()
        .expect("JsonExpr.formatted_expr present");
    exec_init_expr_rec(mcx, formatted_expr, state, formatted_expr_cell)?;

    // JUMP to return NULL if formatted_expr is NULL.
    jumps_return_null.push(state.steps_len as usize);
    let scratch = ExprEvalStep {
        opcode: ExprEvalOp::EEOP_JUMP_IF_NULL,
        resvalue: formatted_expr_cell,
        resnull: formatted_expr_cell,
        d: ExprEvalStepData::Jump { jumpdone: -1 },
    };
    expr_eval_push_step(mcx, state, scratch)?;

    // Evaluate pathspec into jsestate->pathspec.
    let path_spec = jsexpr
        .path_spec
        .as_deref()
        .expect("JsonExpr.path_spec present");
    exec_init_expr_rec(mcx, path_spec, state, pathspec_cell)?;

    // JUMP to return NULL if path_spec is NULL.
    jumps_return_null.push(state.steps_len as usize);
    let scratch = ExprEvalStep {
        opcode: ExprEvalOp::EEOP_JUMP_IF_NULL,
        resvalue: pathspec_cell,
        resnull: pathspec_cell,
        d: ExprEvalStepData::Jump { jumpdone: -1 },
    };
    expr_eval_push_step(mcx, state, scratch)?;

    // Steps to compute PASSING args:
    //   forboth(argexprlc, passing_values, argnamelc, passing_names) { ... }
    let nargs = core::cmp::min(jsexpr.passing_values.len(), jsexpr.passing_names.len());
    for i in 0..nargs {
        let argexpr = &jsexpr.passing_values[i];
        let argname = &jsexpr.passing_names[i];
        // var->typid = exprType(argexpr); var->typmod = exprTypmod(argexpr);
        let ti = backend_nodes_nodeFuncs_seams::expr_type_info::call(argexpr)?;
        let value_cell = new_result_cell(mcx, state)?;
        // ExecInitExprRec(argexpr, state, &var->value, &var->isnull);
        exec_init_expr_rec(mcx, argexpr, state, value_cell)?;

        let var = JsonPathVariableState {
            name: String::from(argname.as_str()),
            typid: ti.typid,
            typmod: ti.typmod,
            value_cell,
        };
        let states = state.json_states.states.as_mut().unwrap();
        states[jsestate_id.0 as usize]
            .args
            .try_reserve(1)
            .map_err(|_| mcx.oom(core::mem::size_of::<JsonPathVariableState>()))?;
        states[jsestate_id.0 as usize].args.push(var);
    }

    // Step for jsonpath evaluation; see ExecEvalJsonExprPath().
    let scratch = ExprEvalStep {
        opcode: ExprEvalOp::EEOP_JSONEXPR_PATH,
        resvalue: resv,
        resnull: resv,
        d: ExprEvalStepData::JsonExpr {
            jsestate: jsestate_id,
        },
    };
    expr_eval_push_step(mcx, state, scratch)?;

    // Patch the return-NULL jumps to here, then a CONST NULL step.
    let here = state.steps_len;
    patch_jumps(state, &jumps_return_null, here);
    let scratch = ExprEvalStep {
        opcode: ExprEvalOp::EEOP_CONST,
        resvalue: resv,
        resnull: resv,
        d: ExprEvalStepData::ConstVal {
            value: DatumV::null(),
            isnull: true,
        },
    };
    expr_eval_push_step(mcx, state, scratch)?;

    // escontext = on_error->btype != JSON_BEHAVIOR_ERROR ? &jsestate->escontext : NULL;
    let escontext_present = on_error.btype != JsonBehaviorType::JSON_BEHAVIOR_ERROR;
    let escontext_id = if escontext_present {
        Some(jsestate_id)
    } else {
        None
    };
    // jsestate->escontext.type = T_ErrorSaveContext;  — the owned SoftErrorContext
    // carries no NodeTag; default-constructed above.

    // Steps to coerce the result computed by EEOP_JSONEXPR_PATH (or the NULL
    // returned on NULL input).
    if jsexpr.use_json_coercion {
        let jec = state.steps_len;
        set_jump(state, jsestate_id, JumpField::EvalCoercion, jec);
        exec_init_json_coercion(
            mcx,
            state,
            returning,
            escontext_id,
            jsexpr.omit_quotes,
            jsexpr.op == JsonExprOp::JSON_EXISTS_OP,
            resv,
        )?;
    } else if jsexpr.use_io_coercion {
        // Build the RETURNING type's input-function FunctionCallInfo (3 args).
        // getTypeInputInfo(returning->typid, &typinput, &typioparam);
        let (typinput, typioparam) =
            backend_utils_cache_lsyscache_seams::get_type_input_info::call(returning.typid)?;
        let finfo = backend_utils_fmgr_fmgr_seams::fmgr_info::call(mcx, typinput)?;
        // InitFunctionCallInfoData(*fcinfo, finfo, 3, InvalidOid, NULL, escontext);
        let fcinfo = mcx::alloc_in(
            mcx,
            types_nodes::fmgr::FunctionCallInfoBaseData {
                flinfo: Some(finfo),
                context: None,
                resultinfo: None,
                fncollation: types_core::InvalidOid,
                isnull: false,
                nargs: 3,
                args: vec![
                    // args[0] — the (cstring) value, filled at eval.
                    types_datum::NullableDatum {
                        value: types_datum::Datum::from_usize(0),
                        isnull: false,
                    },
                    // args[1] — typioparam (constant).
                    types_datum::NullableDatum {
                        value: types_datum::Datum::from_oid(typioparam),
                        isnull: false,
                    },
                    // args[2] — returning->typmod (constant).
                    types_datum::NullableDatum {
                        value: types_datum::Datum::from_i32(returning.typmod),
                        isnull: false,
                    },
                ],
                ..Default::default()
            },
        )?;
        let states = state.json_states.states.as_mut().unwrap();
        states[jsestate_id.0 as usize].input_fcinfo = Some(fcinfo);
    }

    // Add a COERCION_FINISH step if a coercion was emitted and we capture
    // errors softly.
    let jump_eval_coercion = json_jump(state, jsestate_id, JumpField::EvalCoercion);
    if jump_eval_coercion >= 0 && escontext_present {
        let scratch = ExprEvalStep {
            opcode: ExprEvalOp::EEOP_JSONEXPR_COERCION_FINISH,
            resvalue: resv,
            resnull: resv,
            d: ExprEvalStepData::JsonExpr {
                jsestate: jsestate_id,
            },
        };
        expr_eval_push_step(mcx, state, scratch)?;
    }

    // ON ERROR expression steps.
    if on_error.btype != JsonBehaviorType::JSON_BEHAVIOR_ERROR
        && (!on_behavior_is_null_const(on_error) || returning_domain)
    {
        let je = state.steps_len;
        set_jump(state, jsestate_id, JumpField::Error, je);

        // JUMP to end if false (skip the ON ERROR expression).
        jumps_to_end.push(state.steps_len as usize);
        let scratch = ExprEvalStep {
            opcode: ExprEvalOp::EEOP_JUMP_IF_NOT_TRUE,
            resvalue: error_cell,
            resnull: error_cell,
            d: ExprEvalStepData::Jump { jumpdone: -1 },
        };
        expr_eval_push_step(mcx, state, scratch)?;

        // Evaluate the ON ERROR expression, softly.
        let on_error_expr = on_error.expr.as_deref().expect("on_error.expr present");
        let saved = state.escontext;
        state.escontext = escontext_id;
        exec_init_expr_rec(mcx, on_error_expr, state, resv)?;
        state.escontext = saved;

        if on_error.coerce {
            exec_init_json_coercion(
                mcx, state, returning, escontext_id, jsexpr.omit_quotes, false, resv,
            )?;
        }
        if on_error.coerce || is_coerce_via_io(on_error_expr) || is_coerce_to_domain(on_error_expr)
        {
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_JSONEXPR_COERCION_FINISH,
                resvalue: resv,
                resnull: resv,
                d: ExprEvalStepData::JsonExpr {
                    jsestate: jsestate_id,
                },
            };
            expr_eval_push_step(mcx, state, scratch)?;
        }

        // JUMP to end (skip the ON EMPTY steps).
        jumps_to_end.push(state.steps_len as usize);
        let scratch = ExprEvalStep {
            opcode: ExprEvalOp::EEOP_JUMP,
            resvalue: resv,
            resnull: resv,
            d: ExprEvalStepData::Jump { jumpdone: -1 },
        };
        expr_eval_push_step(mcx, state, scratch)?;
    }

    // ON EMPTY expression steps.
    if let Some(on_empty) = jsexpr.on_empty.as_ref() {
        if on_empty.btype != JsonBehaviorType::JSON_BEHAVIOR_ERROR
            && (!on_behavior_is_null_const(on_empty) || returning_domain)
        {
            let je = state.steps_len;
            set_jump(state, jsestate_id, JumpField::Empty, je);

            jumps_to_end.push(state.steps_len as usize);
            let scratch = ExprEvalStep {
                opcode: ExprEvalOp::EEOP_JUMP_IF_NOT_TRUE,
                resvalue: empty_cell,
                resnull: empty_cell,
                d: ExprEvalStepData::Jump { jumpdone: -1 },
            };
            expr_eval_push_step(mcx, state, scratch)?;

            let on_empty_expr = on_empty.expr.as_deref().expect("on_empty.expr present");
            let saved = state.escontext;
            state.escontext = escontext_id;
            exec_init_expr_rec(mcx, on_empty_expr, state, resv)?;
            state.escontext = saved;

            if on_empty.coerce {
                exec_init_json_coercion(
                    mcx, state, returning, escontext_id, jsexpr.omit_quotes, false, resv,
                )?;
            }
            if on_empty.coerce
                || is_coerce_via_io(on_empty_expr)
                || is_coerce_to_domain(on_empty_expr)
            {
                let scratch = ExprEvalStep {
                    opcode: ExprEvalOp::EEOP_JSONEXPR_COERCION_FINISH,
                    resvalue: resv,
                    resnull: resv,
                    d: ExprEvalStepData::JsonExpr {
                        jsestate: jsestate_id,
                    },
                };
                expr_eval_push_step(mcx, state, scratch)?;
            }
        }
    }

    // Patch all jumps_to_end and record jump_end.
    let end = state.steps_len;
    patch_jumps(state, &jumps_to_end, end);
    set_jump(state, jsestate_id, JumpField::End, end);

    Ok(())
}

/// `ExecInitJsonCoercion(state, returning, escontext, omit_quotes,
/// exists_coerce, resv, resnull)` (execExpr.c:5051) — emit one
/// `EEOP_JSONEXPR_COERCION` step.
fn exec_init_json_coercion<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut ExprState<'mcx>,
    returning: &JsonReturning,
    escontext_id: Option<JsonExprStateId>,
    omit_quotes: bool,
    exists_coerce: bool,
    resv: ResultCellId,
) -> PgResult<()> {
    // exists_cast_to_int = exists_coerce && getBaseType(returning->typid) == INT4OID;
    let exists_cast_to_int = exists_coerce
        && backend_utils_cache_lsyscache_seams::get_base_type::call(returning.typid)?
            == types_core::catalog::INT4OID;
    // exists_check_domain = exists_coerce && DomainHasConstraints(returning->typid);
    let exists_check_domain = exists_coerce
        && backend_utils_cache_typcache_seams::domain_has_constraints::call(returning.typid)?;

    // scratch.d.jsonexpr_coercion.json_coercion_cache = NULL; — allocate a fresh
    // persistent ColumnIOData cache slot (the interpreter fills it lazily on
    // first eval and reuses it across rows).
    let json_coercion_cache = new_json_coercion_cache(mcx, state)?;

    let scratch = ExprEvalStep {
        opcode: ExprEvalOp::EEOP_JSONEXPR_COERCION,
        resvalue: resv,
        resnull: resv,
        d: ExprEvalStepData::JsonExprCoercion {
            targettype: returning.typid,
            targettypmod: returning.typmod,
            omit_quotes,
            exists_coerce,
            exists_cast_to_int,
            exists_check_domain,
            json_coercion_cache,
            jsestate: escontext_id,
        },
    };
    expr_eval_push_step(mcx, state, scratch)
}

/// Allocate a fresh (zeroed `ColumnIOData`) coercion cache in `state`'s arena.
fn new_json_coercion_cache<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut ExprState<'mcx>,
) -> PgResult<types_nodes::execexpr::JsonCoercionCacheId> {
    if state.json_coercion_caches.caches.is_none() {
        state.json_coercion_caches.caches = Some(mcx::vec_with_capacity_in(mcx, 1)?);
    }
    let caches = state.json_coercion_caches.caches.as_mut().unwrap();
    let id = types_nodes::execexpr::JsonCoercionCacheId(caches.len() as u32);
    caches
        .try_reserve(1)
        .map_err(|_| mcx.oom(core::mem::size_of::<types_nodes::execexpr::JsonCoercionCache>()))?;
    caches.push(types_nodes::execexpr::JsonCoercionCache::default());
    Ok(id)
}

// --- helpers -----------------------------------------------------------------

/// Which `jump_*` field of a [`JsonExprState`] to set.
enum JumpField {
    Empty,
    Error,
    EvalCoercion,
    End,
}

fn set_jump<'mcx>(
    state: &mut ExprState<'mcx>,
    id: JsonExprStateId,
    field: JumpField,
    value: i32,
) {
    let js = &mut state.json_states.states.as_mut().unwrap()[id.0 as usize];
    match field {
        JumpField::Empty => js.jump_empty = value,
        JumpField::Error => js.jump_error = value,
        JumpField::EvalCoercion => js.jump_eval_coercion = value,
        JumpField::End => js.jump_end = value,
    }
}

fn json_jump<'mcx>(state: &ExprState<'mcx>, id: JsonExprStateId, field: JumpField) -> i32 {
    let js = &state.json_states.states.as_ref().unwrap()[id.0 as usize];
    match field {
        JumpField::Empty => js.jump_empty,
        JumpField::Error => js.jump_error,
        JumpField::EvalCoercion => js.jump_eval_coercion,
        JumpField::End => js.jump_end,
    }
}

/// Patch each step in `targets` to jump to `dest`.
fn patch_jumps<'mcx>(state: &mut ExprState<'mcx>, targets: &[usize], dest: i32) {
    let steps = state.steps.as_mut().unwrap();
    for &t in targets {
        if let ExprEvalStepData::Jump { jumpdone } = &mut steps[t].d {
            *jumpdone = dest;
        }
    }
}

/// C: `IsA(behavior->expr, Const) && ((Const *) behavior->expr)->constisnull`.
fn on_behavior_is_null_const(behavior: &types_nodes::primnodes::JsonBehavior) -> bool {
    match behavior.expr.as_deref() {
        Some(e) if e.expr_tag() == etag::T_Const => e.expect_const().constisnull,
        _ => false,
    }
}

fn is_coerce_via_io(expr: &Expr) -> bool {
    expr.expr_tag() == etag::T_CoerceViaIO
}

fn is_coerce_to_domain(expr: &Expr) -> bool {
    expr.expr_tag() == etag::T_CoerceToDomain
}

/// `ExecInitExprRec` `T_XmlExpr` arm (execExpr.c:2640) — push the steps to
/// evaluate an `XmlExpr` (XMLCONCAT / XMLELEMENT / XMLFOREST / XMLPARSE / XMLPI /
/// XMLROOT / XMLSERIALIZE / IS DOCUMENT) and its subsidiary expressions.
///
/// C parks `Datum*`/`bool*` scratch buffers and `ExecInitExprRec`'s each
/// named/positional argument into them; here each sub-step writes its own
/// result cell, and the step carries the cell ids the evaluator reads.
pub(crate) fn exec_init_xml_expr<'mcx>(
    mcx: Mcx<'mcx>,
    xexpr: &XmlExpr,
    state: &mut ExprState<'mcx>,
    resv: ResultCellId,
) -> PgResult<()> {
    let nnamed = xexpr.named_args.len();
    let nargs = xexpr.args.len();

    // Compile each named arg into a fresh cell, recording its cell id and its
    // exprType (needed for the XMLFOREST/XMLELEMENT value mapping).
    let (named_arg_cells, named_arg_types) = if nnamed != 0 {
        let mut cells = mcx::vec_with_capacity_in(mcx, nnamed)?;
        let mut types = mcx::vec_with_capacity_in(mcx, nnamed)?;
        for e in &xexpr.named_args {
            let cell = new_result_cell(mcx, state)?;
            let ti = backend_nodes_nodeFuncs_seams::expr_type_info::call(e)?;
            exec_init_expr_rec(mcx, e, state, cell)?;
            cells.push(cell);
            types.push(ti.typid);
        }
        (Some(cells), Some(types))
    } else {
        (None, None)
    };

    // Compile each positional arg into a fresh cell.
    let arg_cells = if nargs != 0 {
        let mut cells = mcx::vec_with_capacity_in(mcx, nargs)?;
        for e in &xexpr.args {
            let cell = new_result_cell(mcx, state)?;
            exec_init_expr_rec(mcx, e, state, cell)?;
            cells.push(cell);
        }
        Some(cells)
    } else {
        None
    };

    let scratch = ExprEvalStep {
        opcode: ExprEvalOp::EEOP_XMLEXPR,
        resvalue: resv,
        resnull: resv,
        d: ExprEvalStepData::XmlExpr {
            xexpr: xexpr.clone(),
            named_arg_cells,
            named_arg_types,
            arg_cells,
        },
    };
    expr_eval_push_step(mcx, state, scratch)
}

/// `ExecInitExprRec` `T_JsonValueExpr` arm (execExpr.c) — a bare `JsonValueExpr`
/// at exec-init time recurses into its `raw_expr` then its `formatted_expr` (the
/// later result overwrites the earlier; the cooked form always has both set, the
/// formatted one being the value that survives). We mirror that: recurse into
/// `raw_expr` if present, then into `formatted_expr` if present (so its value is
/// the one left in `resv`).
pub(crate) fn exec_init_json_value_expr<'mcx>(
    mcx: Mcx<'mcx>,
    jve: &types_nodes::primnodes::JsonValueExpr,
    state: &mut ExprState<'mcx>,
    resv: ResultCellId,
) -> PgResult<()> {
    if let Some(raw) = jve.raw_expr.as_deref() {
        exec_init_expr_rec(mcx, raw, state, resv)?;
    }
    if let Some(formatted) = jve.formatted_expr.as_deref() {
        exec_init_expr_rec(mcx, formatted, state, resv)?;
    }
    Ok(())
}

/// `ExecInitExprRec` `T_JsonConstructorExpr` arm (execExpr.c) — push the steps
/// for a SQL/JSON constructor (`JSON_OBJECT`/`JSON_ARRAY`/`JSON_SCALAR`/...).
///
/// Mirrors the C: if `ctor->func` is set the whole thing is a plain function
/// call (recurse into it); the PARSE-non-unique / SERIALIZE shortcuts use the
/// first argument's value directly; otherwise build a
/// [`JsonConstructorExprState`] carrier (per-arg `arg_types`, `Const` args
/// pre-filled and non-`Const` args compiled into result cells), push the
/// `EEOP_JSON_CONSTRUCTOR` step, then handle the RETURNING-type `coercion` (with
/// innermost caseval pointed at `resv`).
pub(crate) fn exec_init_json_constructor<'mcx>(
    mcx: Mcx<'mcx>,
    ctor: &types_nodes::primnodes::JsonConstructorExpr,
    state: &mut ExprState<'mcx>,
    resv: ResultCellId,
) -> PgResult<()> {
    use types_nodes::execexpr::{JsonArgTypeCache, JsonConstructorExprState};
    use types_nodes::primnodes::{JsonConstructorType, JsonFormatType};

    if let Some(func) = ctor.func.as_deref() {
        // The whole constructor is a plain function call (e.g. an aggregate's
        // finalfn wrapper). Recurse into it.
        return exec_init_expr_rec(mcx, func, state, resv);
    }

    if (ctor.r#type == JsonConstructorType::JSCTOR_JSON_PARSE && !ctor.unique)
        || ctor.r#type == JsonConstructorType::JSCTOR_JSON_SERIALIZE
    {
        // Use the value of the first argument as result.
        let first = ctor
            .args
            .first()
            .expect("JSCTOR_JSON_PARSE/SERIALIZE must have at least one arg");
        exec_init_expr_rec(mcx, first, state, resv)?;
    } else {
        let nargs = ctor.args.len();

        let is_jsonb = ctor
            .returning
            .as_ref()
            .and_then(|r| r.format.as_ref())
            .map(|f| f.format_type == JsonFormatType::JS_FORMAT_JSONB)
            .unwrap_or(false);

        let mut arg_values: mcx::PgVec<'mcx, DatumV<'mcx>> =
            mcx::vec_with_capacity_in(mcx, nargs)?;
        let mut arg_nulls: mcx::PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, nargs)?;
        let mut arg_types: mcx::PgVec<'mcx, types_core::primitive::Oid> =
            mcx::vec_with_capacity_in(mcx, nargs)?;
        let mut arg_cells: mcx::PgVec<'mcx, Option<ResultCellId>> =
            mcx::vec_with_capacity_in(mcx, nargs)?;

        for arg in &ctor.args {
            let typid = backend_nodes_nodeFuncs_seams::expr_type_info::call(arg)?.typid;
            arg_types.push(typid);

            if arg.expr_tag() == etag::T_Const {
                // Don't evaluate const arguments every round.
                let con = arg.expect_const();
                arg_values.push(con.constvalue.clone_in(mcx)?);
                arg_nulls.push(con.constisnull);
                arg_cells.push(None);
            } else {
                // Each non-const arg evaluates into its own result cell; the
                // interpreter gathers it into arg_values/arg_nulls per row.
                let cell = new_result_cell(mcx, state)?;
                arg_values.push(DatumV::null());
                arg_nulls.push(false);
                exec_init_expr_rec(mcx, arg, state, cell)?;
                arg_cells.push(Some(cell));
            }
        }

        // Prepare type cache for datum_to_json[b]() (JSCTOR_JSON_SCALAR only).
        let arg_type_cache = if ctor.r#type == JsonConstructorType::JSCTOR_JSON_SCALAR {
            // json_categorize_type has no executor-facing seam yet, so the
            // scalar constructor's per-arg category/outfuncid cache cannot be
            // built. The eval path raises FEATURE_NOT_SUPPORTED for SCALAR; we
            // leave the cache empty here (the eval never reads it).
            let mut cache: mcx::PgVec<'mcx, JsonArgTypeCache> =
                mcx::vec_with_capacity_in(mcx, nargs)?;
            for _ in 0..nargs {
                cache.push(JsonArgTypeCache::default());
            }
            Some(cache)
        } else {
            None
        };

        let jcstate = JsonConstructorExprState {
            arg_values: Some(arg_values),
            arg_nulls: Some(arg_nulls),
            arg_types: Some(arg_types),
            arg_type_cache,
            nargs: nargs as i32,
            arg_cells: Some(arg_cells),
            ctor_type: ctor.r#type,
            is_jsonb,
            absent_on_null: ctor.absent_on_null,
            unique: ctor.unique,
        };

        let scratch = ExprEvalStep {
            opcode: ExprEvalOp::EEOP_JSON_CONSTRUCTOR,
            resvalue: resv,
            resnull: resv,
            d: ExprEvalStepData::JsonConstructor {
                jcstate: Some(mcx::alloc_in(mcx, jcstate)?),
            },
        };
        expr_eval_push_step(mcx, state, scratch)?;
    }

    // RETURNING-type coercion: recurse with innermost caseval pointed at resv
    // (the owned model carries value+null in the single resv cell).
    if let Some(coercion) = ctor.coercion.as_deref() {
        let save = state.innermost_caseval;
        state.innermost_caseval = Some(resv);
        exec_init_expr_rec(mcx, coercion, state, resv)?;
        state.innermost_caseval = save;
    }

    Ok(())
}

/// `ExecInitExprRec` `T_JsonIsPredicate` arm (execExpr.c) — recurse into the
/// subject expression (writing `resv`), then push the `EEOP_IS_JSON` step. The
/// step carries the projected `item_type`/`unique_keys` and the subject's type
/// OID (the C reads `pred->item_type`, `pred->unique_keys`, and
/// `exprType(pred->expr)`).
pub(crate) fn exec_init_json_is_predicate<'mcx>(
    mcx: Mcx<'mcx>,
    pred: &types_nodes::primnodes::JsonIsPredicate,
    state: &mut ExprState<'mcx>,
    resv: ResultCellId,
) -> PgResult<()> {
    let expr = pred
        .expr
        .as_deref()
        .expect("JsonIsPredicate.expr must be present");
    exec_init_expr_rec(mcx, expr, state, resv)?;

    let arg_type = backend_nodes_nodeFuncs_seams::expr_type_info::call(expr)?.typid;

    let scratch = ExprEvalStep {
        opcode: ExprEvalOp::EEOP_IS_JSON,
        resvalue: resv,
        resnull: resv,
        d: ExprEvalStepData::IsJson {
            item_type: pred.item_type,
            unique_keys: pred.unique_keys,
            arg_type,
        },
    };
    expr_eval_push_step(mcx, state, scratch)
}
