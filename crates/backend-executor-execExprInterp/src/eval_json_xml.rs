//! XML and JSON opcode evaluators (`execExprInterp.c`): XmlExpr, the SQL/JSON
//! constructors and predicates, JSON_VALUE/JSON_QUERY/JSON_EXISTS path
//! evaluation, and the JSON coercion steps.
//!
//! The `JsonExpr` path / coercion family (`EEOP_JSONEXPR_PATH`,
//! `EEOP_JSONEXPR_COERCION`, `EEOP_JSONEXPR_COERCION_FINISH`) is ported over the
//! real `JsonPathExists`/`Query`/`Value` workers and `json_populate_type`
//! coercion. `XmlExpr` / `JsonConstructor` / `IsJson` remain panics: their
//! parse-node back-pointers are still parked and their xml.c / json.c workers
//! have no executor-facing seam yet.

use mcx::{Mcx, PgString};
use types_error::{PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_NO_SQL_JSON_ITEM};
use types_nodes::execexpr::{
    ExprEvalStepData, ExprState, JsonCoercionCacheId, JsonExprStateId, ResultCellId,
};
use types_nodes::execnodes::EcxtId;
use types_nodes::EStateData;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::{JSONBOID, JSONOID};

use backend_utils_adt_jsonpath_exec::{
    JsonPathExists, JsonPathQuery, JsonPathValue, JsonPathVariable, JsonPathVars,
    JsonWrapper as PathJsonWrapper,
};
use types_jsonb::backend_utils_adt_jsonb_util::{JsonbValue, JsonbValueData};
use types_nodes::primnodes::{JsonBehaviorType, JsonExprOp, JsonWrapper};

use crate::interp_loop::{read_cell, write_cell};

/// `ExecEvalXmlExpr(ExprState *state, ExprEvalStep *op)` — evaluate an
/// XMLELEMENT / XMLFOREST / XMLPARSE / etc. expression. Still parked (the
/// `XmlExpr` primnode and xml.c workers are unported).
pub fn ExecEvalXmlExpr<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = (state, op, estate);
    panic!(
        "execExprInterp: EEOP_XMLEXPR — the XmlExpr primnode (op->d.xmlexpr.xexpr, \
         parked as ExprEvalStepData::XmlExpr {{ xexpr: usize }}) and the xml.c \
         workers it dispatches to (xmlconcat/xmlelement/xmlparse/xmlpi/xmlroot/\
         xmltotext_with_options/xml_is_document/map_sql_value_to_xml_value) are \
         not yet ported — no executor-facing xml seam exists"
    )
}

/// `ExecEvalJsonConstructor` — JSON / JSONB object/array constructor. Still
/// parked (the `JsonConstructorExpr` back-pointer and the json.c/jsonb.c
/// constructor workers are unported).
pub fn ExecEvalJsonConstructor<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = (state, op, econtext, estate);
    panic!(
        "execExprInterp: EEOP_JSON_CONSTRUCTOR — the JsonConstructorExpr node \
         (jcstate->constructor) is parked off JsonConstructorExprState, and the \
         json.c/jsonb.c constructor workers have no executor-facing seam — not \
         yet ported"
    )
}

/// `ExecEvalJsonIsPredicate` — `IS JSON [VALUE|OBJECT|ARRAY|SCALAR]` predicate.
/// Still parked (the `JsonIsPredicate` node is unported).
pub fn ExecEvalJsonIsPredicate<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = (state, op, estate);
    panic!(
        "execExprInterp: EEOP_IS_JSON — the JsonIsPredicate node \
         (op->d.is_json.pred, parked as ExprEvalStepData::IsJson {{ pred: usize }}) \
         and the json.c/jsonb.c probes have no executor-facing seam — not yet ported"
    )
}

/// Map the primnodes `JsonWrapper` to the jsonpath-exec worker's `JsonWrapper`.
fn map_wrapper(w: JsonWrapper) -> PathJsonWrapper {
    match w {
        JsonWrapper::JSW_UNSPEC => PathJsonWrapper::JSW_UNSPEC,
        JsonWrapper::JSW_NONE => PathJsonWrapper::JSW_NONE,
        JsonWrapper::JSW_CONDITIONAL => PathJsonWrapper::JSW_CONDITIONAL,
        JsonWrapper::JSW_UNCONDITIONAL => PathJsonWrapper::JSW_UNCONDITIONAL,
    }
}

/// `ExecEvalJsonExprPath(ExprState *state, ExprEvalStep *op, ExprContext
/// *econtext)` (execExprInterp.c:4835) — run a jsonpath for JSON_VALUE / QUERY /
/// EXISTS, choosing the success / error / empty / coercion jump. Returns the
/// next step address.
pub fn ExecEvalJsonExprPath<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<i32> {
    let _ = econtext;
    let mcx = estate.es_query_cxt;

    // jsestate = op->d.jsonexpr.jsestate;
    let jsestate_id = match &state.steps.as_ref().unwrap()[op].d {
        ExprEvalStepData::JsonExpr { jsestate } => *jsestate,
        _ => unreachable!("EEOP_JSONEXPR_PATH: payload is not JsonExpr"),
    };
    let resv = state.steps.as_ref().unwrap()[op].resvalue;
    let resnull = state.steps.as_ref().unwrap()[op].resnull;

    // Snapshot the immutable jsestate fields the evaluation needs (jsexpr op /
    // wrapper / column_name / behavior, the cell ids, the jump targets).
    let js = &state.json_states.states.as_ref().unwrap()[jsestate_id.0 as usize];
    let jsexpr = js.jsexpr.clone();
    let formatted_expr_cell = js.formatted_expr_cell;
    let pathspec_cell = js.pathspec_cell;
    let error_cell = js.error_cell;
    let empty_cell = js.empty_cell;
    let jump_empty = js.jump_empty;
    let jump_error = js.jump_error;
    let jump_eval_coercion = js.jump_eval_coercion;
    let jump_end = js.jump_end;
    let returning = jsexpr
        .returning
        .as_ref()
        .expect("JsonExpr.returning present");
    let on_error = jsexpr.on_error.as_ref().expect("JsonExpr.on_error present");

    let throw_error = on_error.btype == JsonBehaviorType::JSON_BEHAVIOR_ERROR;
    let suppress_errors = !throw_error;
    let mut error = false;
    let mut empty = false;

    // item = jsestate->formatted_expr.value;  path = DatumGetJsonPathP(pathspec.value);
    let item = read_cell(state, formatted_expr_cell).0;
    let path = read_cell(state, pathspec_cell).0;
    let item_bytes = item.as_ref_bytes().to_vec();
    let path_bytes = path.as_ref_bytes().to_vec();

    // Build the PASSING-variable list from jsestate->args.
    let vars = build_path_vars(state, jsestate_id)?;

    // Reset error/empty cells and the soft-error context for this row.
    write_cell(state, error_cell, Datum::from_bool(false), false);
    write_cell(state, empty_cell, Datum::from_bool(false), false);
    {
        let js = &mut state.json_states.states.as_mut().unwrap()[jsestate_id.0 as usize];
        js.escontext = types_error::SoftErrorContext::default();
    }

    let column_name = jsexpr.column_name.as_deref();
    let mut val_string: Option<PgString<'mcx>> = None;

    match jsexpr.op {
        JsonExprOp::JSON_EXISTS_OP => {
            let r = JsonPathExists(mcx, &item_bytes, &path_bytes, suppress_errors, &vars)?;
            error = r.error;
            if !error {
                write_cell(state, resv, Datum::from_bool(r.matched), false);
                if resnull != resv {
                    write_cell(state, resnull, Datum::from_bool(r.matched), false);
                }
            }
        }
        JsonExprOp::JSON_QUERY_OP => {
            let r = JsonPathQuery(
                mcx,
                &item_bytes,
                &path_bytes,
                map_wrapper(jsexpr.wrapper),
                suppress_errors,
                &vars,
                column_name,
            )?;
            error = r.error;
            empty = r.empty;
            match r.value {
                Some(bytes) => {
                    let v = mcx::slice_in(mcx, &bytes)?;
                    write_cell(state, resv, Datum::ByRef(v), false);
                }
                None => write_cell(state, resv, Datum::null(), true),
            }
        }
        JsonExprOp::JSON_VALUE_OP => {
            let r = JsonPathValue(
                mcx,
                &item_bytes,
                &path_bytes,
                suppress_errors,
                &vars,
                column_name,
            )?;
            error = r.error;
            empty = r.empty;
            match r.value {
                None => write_cell(state, resv, Datum::null(), true),
                Some(jbv) => {
                    if !error && !empty {
                        let rettypid = returning.typid;
                        if rettypid == JSONOID || rettypid == JSONBOID {
                            // jsonb_out(JsonbValueToJsonb(jbv))
                            let jb =
                                backend_utils_adt_jsonb_util::JsonbValueToJsonb(mcx, &jbv)?;
                            let s = backend_utils_adt_jsonb::jsonb_out(mcx, &jb)?;
                            val_string = Some(pgstring_from_bytes(mcx, s.as_slice())?);
                        } else if jsexpr.use_json_coercion {
                            let jb =
                                backend_utils_adt_jsonb_util::JsonbValueToJsonb(mcx, &jbv)?;
                            let v = mcx::slice_in(mcx, jb.as_slice())?;
                            write_cell(state, resv, Datum::ByRef(v), false);
                        } else {
                            let (s, is_null) = exec_get_json_value_item_string(mcx, &jbv)?;
                            if is_null {
                                write_cell(state, resv, Datum::null(), true);
                            }
                            val_string = s;
                            if !jsexpr.use_io_coercion {
                                // *op->resvalue = DirectFunctionCall1(textin,
                                //     CStringGetDatum(val_string));
                                if let Some(vs) = val_string.as_ref() {
                                    let t = backend_utils_adt_varlena_seams::cstring_to_text_v::call(mcx, vs.as_str())?;
                                    write_cell(state, resv, t, false);
                                }
                            }
                        }
                    }
                }
            }
        }
        // JSON_TABLE_OP can't happen here.
        _ => {
            return Err(PgError::error(format!(
                "unrecognized SQL/JSON expression op {}",
                jsexpr.op as i32
            )));
        }
    }

    // Coerce the result by calling the RETURNING type's input function.
    let resnull_now = read_cell(state, resv).1;
    if !resnull_now && jsexpr.use_io_coercion {
        // C: fcinfo = jsestate->input_fcinfo; args[0] = val_string; the second
        // and third args (typioparam / typmod) were preloaded at compile. The
        // owned input-function call re-resolves by OID, so read the preloaded
        // fn_oid / typioparam / returning typmod off the stored input_fcinfo.
        let vs = val_string
            .as_ref()
            .expect("use_io_coercion: val_string must be set")
            .to_string();
        let js = &state.json_states.states.as_ref().unwrap()[jsestate_id.0 as usize];
        let fcinfo = js
            .input_fcinfo
            .as_ref()
            .expect("use_io_coercion: input_fcinfo must be set");
        let fn_oid = fcinfo
            .flinfo
            .as_ref()
            .expect("input_fcinfo flinfo present")
            .fn_oid;
        let typioparam =
            types_core::primitive::Oid::from(fcinfo.args[1].value.as_usize() as u32);
        let typmod = fcinfo.args[2].value.as_usize() as i32;
        // C threads jsestate->escontext for soft IO-coercion errors; the owned
        // hard-error input_function_call seam (escontext == NULL) is used here,
        // so a malformed value raises hard rather than steering ON ERROR. This
        // is the one IO-coercion narrowing (no soft InputFunctionCallSafe seam).
        let coerced = backend_utils_fmgr_fmgr_seams::input_function_call::call(
            mcx,
            fn_oid,
            Some(&vs),
            typioparam,
            typmod,
        )?;
        write_cell(state, resv, coerced, false);
    }

    // Handle ON EMPTY.
    if empty {
        write_cell(state, resv, Datum::null(), true);
        if jsexpr.on_empty.is_some() {
            let on_empty = jsexpr.on_empty.as_ref().unwrap();
            if on_empty.btype != JsonBehaviorType::JSON_BEHAVIOR_ERROR {
                write_cell(state, empty_cell, Datum::from_bool(true), false);
                let js = &mut state.json_states.states.as_mut().unwrap()[jsestate_id.0 as usize];
                js.escontext = types_error::SoftErrorContext::new(true);
                return Ok(if jump_empty >= 0 { jump_empty } else { jump_end });
            }
        } else if on_error.btype != JsonBehaviorType::JSON_BEHAVIOR_ERROR {
            write_cell(state, error_cell, Datum::from_bool(true), false);
            let js = &mut state.json_states.states.as_mut().unwrap()[jsestate_id.0 as usize];
            js.escontext = types_error::SoftErrorContext::new(true);
            return Ok(if jump_error >= 0 { jump_error } else { jump_end });
        }

        return Err(no_sql_json_item_error(column_name));
    }

    // Handle ON ERROR (not reached when the behavior is ERROR — already thrown).
    if error {
        write_cell(state, resv, Datum::null(), true);
        write_cell(state, error_cell, Datum::from_bool(true), false);
        let js = &mut state.json_states.states.as_mut().unwrap()[jsestate_id.0 as usize];
        js.escontext = types_error::SoftErrorContext::new(true);
        return Ok(if jump_error >= 0 { jump_error } else { jump_end });
    }

    Ok(if jump_eval_coercion >= 0 {
        jump_eval_coercion
    } else {
        jump_end
    })
}

/// Build the `JsonPathVars` from a jsestate's compiled PASSING args, gathering
/// each arg's value out of its result cell.
fn build_path_vars<'mcx>(
    state: &ExprState<'mcx>,
    jsestate_id: JsonExprStateId,
) -> PgResult<JsonPathVars> {
    let js = &state.json_states.states.as_ref().unwrap()[jsestate_id.0 as usize];
    if js.args.is_empty() {
        return Ok(JsonPathVars::None);
    }
    let mut vars = Vec::with_capacity(js.args.len());
    // Collect (name, typid, typmod, value_cell) first to drop the jsestate borrow.
    let specs: Vec<(Vec<u8>, u32, i32, ResultCellId)> = js
        .args
        .iter()
        .map(|v| {
            (
                v.name.as_bytes().to_vec(),
                v.typid,
                v.typmod,
                v.value_cell,
            )
        })
        .collect();
    for (name, typid, typmod, cell) in specs {
        let (val, isnull) = read_cell(state, cell);
        // JsonPathVariable.value is a bare-word types_datum::Datum. A by-value
        // arg maps directly; a by-reference arg cannot be carried by the bare
        // word — that is the genuine by-ref-Datum substrate gap (and the
        // json_item_from_datum seam that would consume it is itself
        // uninstalled), so it loud-panics here.
        let word = match val {
            Datum::ByVal(w) => w,
            _ => panic!(
                "execExprInterp: ExecEvalJsonExprPath PASSING variable {:?} has a by-reference \
                 value; the bare-word JsonPathVariable.value carrier and the json_item_from_datum \
                 detoast seam for varlena PASSING args are not yet landed",
                String::from_utf8_lossy(&name)
            ),
        };
        vars.push(JsonPathVariable {
            name,
            typid,
            typmod,
            value: types_datum::Datum::from_usize(word),
            isnull,
        });
    }
    Ok(JsonPathVars::List(vars))
}

/// `ExecGetJsonValueItemString(JsonbValue *item, bool *resnull)`
/// (execExprInterp.c:5037) — render a scalar jsonb item as its text form.
/// Returns `(string, is_null)`.
fn exec_get_json_value_item_string<'mcx>(
    mcx: Mcx<'mcx>,
    item: &JsonbValue,
) -> PgResult<(Option<PgString<'mcx>>, bool)> {
    match &item.val {
        JsonbValueData::Null => Ok((None, true)),
        JsonbValueData::String(bytes) => Ok((Some(pgstring_from_bytes(mcx, bytes)?), false)),
        JsonbValueData::Numeric(num) => {
            let s = backend_utils_adt_numeric::io::numeric_out(mcx, num)?;
            Ok((Some(pgstring_from_bytes(mcx, s.as_bytes())?), false))
        }
        JsonbValueData::Bool(b) => {
            // DirectFunctionCall1(boolout, ...): "t" / "f".
            let s = if *b { "t" } else { "f" };
            Ok((Some(pgstring_from_bytes(mcx, s.as_bytes())?), false))
        }
        JsonbValueData::Array { .. } | JsonbValueData::Object(_) | JsonbValueData::Binary { .. } => {
            // jsonb_out(JsonbValueToJsonb(item))
            let jb = backend_utils_adt_jsonb_util::JsonbValueToJsonb(mcx, item)?;
            let s = backend_utils_adt_jsonb::jsonb_out(mcx, &jb)?;
            Ok((Some(pgstring_from_bytes(mcx, s.as_slice())?), false))
        }
        JsonbValueData::Datetime(_) => {
            // The per-type datetime *_out casts (date_out/time_out/timetz_out/
            // timestamp_out/timestamptz_out via DirectFunctionCall1) live in the
            // datetime adt unit, not threaded into the interpreter; JSON_VALUE of
            // a jsonb datetime scalar to text is the one narrow arm not yet
            // reachable here.
            panic!(
                "execExprInterp: ExecGetJsonValueItemString — the jbvDatetime arm needs the \
                 date_out/time_out/timetz_out/timestamp_out/timestamptz_out casts \
                 (backend-utils-adt-datetime), not yet threaded into the interpreter"
            )
        }
    }
}

/// `ExecEvalJsonCoercion(ExprState *state, ExprEvalStep *op, ExprContext
/// *econtext)` (execExprInterp.c:5112) — coerce a JSON path result to the
/// output type.
pub fn ExecEvalJsonCoercion<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = econtext;
    let mcx = estate.es_query_cxt;

    let (
        targettype,
        targettypmod,
        omit_quotes,
        exists_coerce,
        exists_cast_to_int,
        exists_check_domain,
        cache_id,
        escontext_id,
    ) = match &state.steps.as_ref().unwrap()[op].d {
        ExprEvalStepData::JsonExprCoercion {
            targettype,
            targettypmod,
            omit_quotes,
            exists_coerce,
            exists_cast_to_int,
            exists_check_domain,
            json_coercion_cache,
            jsestate,
        } => (
            *targettype,
            *targettypmod,
            *omit_quotes,
            *exists_coerce,
            *exists_cast_to_int,
            *exists_check_domain,
            *json_coercion_cache,
            *jsestate,
        ),
        _ => unreachable!("EEOP_JSONEXPR_COERCION: payload is not JsonExprCoercion"),
    };
    let resv = state.steps.as_ref().unwrap()[op].resvalue;

    if exists_coerce {
        if exists_cast_to_int {
            // Check domain constraints if any (domain_check_safe). Not yet
            // threaded; only reached for JSON_EXISTS RETURNING a domain over int
            // with constraints.
            if exists_check_domain {
                panic!(
                    "execExprInterp: EEOP_JSONEXPR_COERCION — the exists_check_domain branch \
                     needs domain_check_safe (utils/adt/domains.c), not yet threaded into the \
                     interpreter"
                );
            }
            // *op->resvalue = DirectFunctionCall1(bool_int4, *op->resvalue);
            let (v, n) = read_cell(state, resv);
            let b = if n { false } else { v.as_bool() };
            write_cell(state, resv, Datum::from_i32(if b { 1 } else { 0 }), n);
            return Ok(());
        }

        // *op->resvalue = DirectFunctionCall1(jsonb_in, "true"/"false")
        let (v, n) = read_cell(state, resv);
        let truth = if n { false } else { v.as_bool() };
        let s: &[u8] = if truth { b"true" } else { b"false" };
        let jb = backend_utils_adt_jsonb::jsonb_in(mcx, s)?;
        write_cell(state, resv, Datum::ByRef(mcx::slice_in(mcx, jb.as_slice())?), false);
    }

    // *op->resvalue = json_populate_type(*op->resvalue, JSONBOID, targettype,
    //     targettypmod, &json_coercion_cache, per_query_memory, op->resnull,
    //     omit_quotes, escontext);
    let (val, mut isnull) = read_cell(state, resv);
    let json_val = if isnull { Vec::new() } else { val.as_ref_bytes().to_vec() };

    // Take the persistent cache and the soft-error sink out so the borrows do
    // not alias `state` while json_populate_type runs, then restore them.
    let mut cache = take_coercion_cache(state, cache_id);
    let mut escontext = escontext_id.map(|id| {
        core::mem::take(&mut state.json_states.states.as_mut().unwrap()[id.0 as usize].escontext)
    });

    let result = backend_utils_adt_jsonfuncs::populate::json_populate_type(
        mcx,
        &json_val,
        JSONBOID,
        targettype,
        targettypmod,
        &mut cache,
        &mut isnull,
        omit_quotes,
        escontext.as_mut(),
    );

    // Restore the cache and the soft-error sink regardless of outcome.
    put_coercion_cache(state, cache_id, cache);
    if let (Some(id), Some(ec)) = (escontext_id, escontext) {
        state.json_states.states.as_mut().unwrap()[id.0 as usize].escontext = ec;
    }

    let datum = result?;
    write_cell(state, resv, datum, isnull);
    Ok(())
}

/// `GetJsonBehaviorValueString(JsonBehavior *behavior)` (execExprInterp.c:5164)
/// — the text of an ON ERROR / ON EMPTY behavior, for error messages.
fn get_json_behavior_value_string(btype: JsonBehaviorType) -> &'static str {
    // Order must match JsonBehaviorType.
    const NAMES: [&str; 9] = [
        "NULL",
        "ERROR",
        "EMPTY",
        "TRUE",
        "FALSE",
        "UNKNOWN",
        "EMPTY ARRAY",
        "EMPTY OBJECT",
        "DEFAULT",
    ];
    NAMES[btype as usize]
}

/// `ExecEvalJsonCoercionFinish(ExprState *state, ExprEvalStep *op)`
/// (execExprInterp.c:5192) — finalize a JSON coercion that needed a
/// sub-expression evaluation, rethrowing a soft coercion error.
pub fn ExecEvalJsonCoercionFinish<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = estate;
    let jsestate_id = match &state.steps.as_ref().unwrap()[op].d {
        ExprEvalStepData::JsonExpr { jsestate } => *jsestate,
        _ => unreachable!("EEOP_JSONEXPR_COERCION_FINISH: payload is not JsonExpr"),
    };
    let resv = state.steps.as_ref().unwrap()[op].resvalue;

    let js = &state.json_states.states.as_ref().unwrap()[jsestate_id.0 as usize];
    if js.escontext.error_occurred() {
        let error_set = read_cell(state, js.error_cell).0.as_bool();
        let empty_set = read_cell(state, js.empty_cell).0.as_bool();
        let js = &state.json_states.states.as_ref().unwrap()[jsestate_id.0 as usize];
        let jsexpr = &js.jsexpr;
        let detail = js
            .escontext
            .error()
            .map(|e| e.message().to_string())
            .unwrap_or_default();

        if error_set {
            let clause = jsexpr
                .on_error
                .as_ref()
                .map(|b| get_json_behavior_value_string(b.btype))
                .unwrap_or("ON ERROR");
            return Err(coercion_error("ON ERROR", clause, &detail));
        } else if empty_set {
            let clause = jsexpr
                .on_empty
                .as_ref()
                .map(|b| get_json_behavior_value_string(b.btype))
                .unwrap_or("ON EMPTY");
            return Err(coercion_error("ON EMPTY", clause, &detail));
        }

        // Reset for next use: resvalue NULL, error TRUE, escontext cleared.
        let error_cell = state.json_states.states.as_ref().unwrap()[jsestate_id.0 as usize].error_cell;
        write_cell(state, resv, Datum::null(), true);
        write_cell(state, error_cell, Datum::from_bool(true), false);
        let js = &mut state.json_states.states.as_mut().unwrap()[jsestate_id.0 as usize];
        js.escontext = types_error::SoftErrorContext::default();
    }
    Ok(())
}

// --- helpers -----------------------------------------------------------------

/// Build a `PgString` from raw bytes (C `pstrdup` of a rendered cstring).
fn pgstring_from_bytes<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> PgResult<PgString<'mcx>> {
    PgString::from_str_in(&String::from_utf8_lossy(bytes), mcx)
}

/// Take the persistent coercion cache out of the arena (replacing it with a
/// fresh default so the slot is restored after the call).
fn take_coercion_cache<'mcx>(
    state: &mut ExprState<'mcx>,
    id: JsonCoercionCacheId,
) -> types_nodes::execexpr::JsonCoercionCache<'mcx> {
    let caches = state
        .json_coercion_caches
        .caches
        .as_mut()
        .expect("coercion-cache arena allocated at compile");
    core::mem::take(&mut caches[id.0 as usize])
}

/// Restore a coercion cache taken by [`take_coercion_cache`].
fn put_coercion_cache<'mcx>(
    state: &mut ExprState<'mcx>,
    id: JsonCoercionCacheId,
    cache: types_nodes::execexpr::JsonCoercionCache<'mcx>,
) {
    let caches = state.json_coercion_caches.caches.as_mut().unwrap();
    caches[id.0 as usize] = cache;
}

fn no_sql_json_item_error(column_name: Option<&str>) -> PgError {
    let msg = match column_name {
        Some(c) => format!(
            "no SQL/JSON item found for specified path of column \"{}\"",
            c
        ),
        None => "no SQL/JSON item found for specified path".to_string(),
    };
    PgError::error(msg).with_sqlstate(ERRCODE_NO_SQL_JSON_ITEM)
}

fn coercion_error(clause: &str, behavior: &str, detail: &str) -> PgError {
    let e = PgError::error(format!(
        "could not coerce {} expression ({}) to the RETURNING type",
        clause,
        behavior
    ))
    .with_sqlstate(ERRCODE_DATATYPE_MISMATCH);
    if detail.is_empty() {
        e
    } else {
        e.with_detail(detail.to_string())
    }
}
