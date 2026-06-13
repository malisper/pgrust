//! XML and JSON opcode evaluators (`execExprInterp.c`): XmlExpr, the SQL/JSON
//! constructors and predicates, JSON_VALUE/JSON_QUERY/JSON_EXISTS path
//! evaluation, and the JSON coercion steps.
//!
//! Porting status — every handler in this family reads a step payload that the
//! COMPILER (`backend-executor-execExpr`) cannot yet emit, and calls runtime
//! workers that no seam exposes. Both blockers are *genuinely unported owners*,
//! not gaps in the interpreter's own logic:
//!
//!  1. The step-data node/state back-pointers these opcodes branch on are parked
//!     by the keystone F0 model as opaque addresses, because their owning units
//!     have not landed:
//!       - `EEOP_XMLEXPR`            → `ExprEvalStepData::XmlExpr { xexpr: usize }`
//!         — the `XmlExpr` primnode the switch reads (`xexpr->op`,
//!         `xexpr->args`, `xexpr->named_args`, `xexpr->arg_names`,
//!         `xexpr->xmloption`, `xexpr->name`, `xexpr->indent`) is parked.
//!       - `EEOP_JSON_CONSTRUCTOR`   → `ExprEvalStepData::JsonConstructor { jcstate }`.
//!         The `JsonConstructorExprState` workspace arrays ARE carried, but its
//!         `constructor` back-pointer (`JsonConstructorExpr *ctor`) — whose
//!         `ctor->type` / `ctor->returning->format->format_type` /
//!         `ctor->absent_on_null` / `ctor->unique` drive the whole switch — is
//!         NOT a field of the ported state (parked until the SQL/JSON primnode
//!         + JsonReturning/JsonFormat unit lands).
//!       - `EEOP_IS_JSON`            → `ExprEvalStepData::IsJson { pred: usize }`
//!         — the `JsonIsPredicate` node (`pred->expr`, `pred->item_type`,
//!         `pred->unique_keys`) is parked.
//!       - `EEOP_JSONEXPR_PATH` /
//!         `EEOP_JSONEXPR_COERCION_FINISH`
//!                                   → `ExprEvalStepData::JsonExpr { jsestate: usize }`
//!         — the entire `JsonExprState` runtime-state group (`JsonExpr`,
//!         `JsonPathVariable`, `ErrorSaveContext`, the `jump_*` targets,
//!         `input_fcinfo`, `formatted_expr`/`pathspec`/`args`/`error`/`empty`)
//!         is parked. The companion compiler entry `exec_init_json_expr`
//!         (`execExpr_json.rs`) records the identical blocker and also panics:
//!         the interpreter cannot read a payload the compiler never writes.
//!       - `EEOP_JSONEXPR_COERCION`  → `ExprEvalStepData::JsonExprCoercion { .. }`.
//!         The scalar flags (`targettype`/`targettypmod`/`omit_quotes`/
//!         `exists_*`) ARE carried, but the `escontext` (`ErrorSaveContext *`)
//!         and `json_coercion_cache` it threads are parked opaque addresses
//!         owned by the same unported JSON-state unit.
//!
//!  2. The actual evaluation workers have no seam this unit can call (the crate
//!     depends only on the execExpr / execTuples / nodeSubplan / fmgr seams):
//!       - xml.c: `xmlconcat`, `xmlelement`, `xmlparse`, `xmlpi`, `xmlroot`,
//!         `xmltotext_with_options`, `xml_is_document`,
//!         `map_sql_value_to_xml_value` (only `escape_xml` is seamed today).
//!       - json.c / jsonb.c: `json_build_array_worker`,
//!         `jsonb_build_array_worker`, `json_build_object_worker`,
//!         `jsonb_build_object_worker`, `datum_to_json`, `datum_to_jsonb`,
//!         `json_validate`, `jsonb_from_text`, `json_get_first_token`,
//!         `JsonbValueToJsonb`, `JB_ROOT_IS_*` (the unit `backend-utils-adt-json`
//!         exists but exposes no executor-facing seam).
//!       - jsonpath_exec.c: `JsonPathExists`, `JsonPathQuery`, `JsonPathValue`,
//!         `json_populate_type`, `DatumGetJsonPathP` (unported unit, no seam).
//!       - the `DirectFunctionCall1` output/cast targets (`numeric_out`,
//!         `boolout`, `date_out`, `time_out`, `timetz_out`, `timestamp_out`,
//!         `timestamptz_out`, `jsonb_out`, `jsonb_in`, `textin`, `bool_int4`)
//!         and `domain_check_safe` — their owning adt units are unported.
//!
//! Per "Mirror PG and panic", each handler below carries the full faithful
//! `execExprInterp.c` logic as a structural comment and a loud seam-and-panic
//! body naming the unported owners — it is neither a silent stub nor an invented
//! opaque stand-in, and it is not `todo!()` (this family's own interpreter logic
//! is complete: every branch, every field read, every worker call is mirrored).
//! These bodies become real the moment the parked node/state types and the
//! xml/json/jsonpath worker seams land, exactly as the compiler-side
//! `exec_init_json_expr` does.

use mcx::{Mcx, PgString};
use types_error::PgResult;
use types_nodes::execexpr::ExprState;
use types_nodes::execnodes::EcxtId;
use types_nodes::EStateData;

/// `ExecEvalXmlExpr(ExprState *state, ExprEvalStep *op)` — evaluate an
/// XMLELEMENT / XMLFOREST / XMLPARSE / etc. expression.
///
/// C logic (execExprInterp.c:4441-4651):
///
///   XmlExpr *xexpr = op->d.xmlexpr.xexpr;
///   Datum    value;
///   *op->resnull = true;          /* until we get a result */
///   *op->resvalue = (Datum) 0;
///   switch (xexpr->op)
///   {
///     case IS_XMLCONCAT:
///       Datum *argvalue = op->d.xmlexpr.argvalue;
///       bool  *argnull  = op->d.xmlexpr.argnull;
///       List  *values = NIL;
///       for (int i = 0; i < list_length(xexpr->args); i++)
///         if (!argnull[i]) values = lappend(values, DatumGetPointer(argvalue[i]));
///       if (values != NIL) { *op->resvalue = PointerGetDatum(xmlconcat(values));
///                            *op->resnull = false; }
///       break;
///     case IS_XMLFOREST:
///       Datum *argvalue = op->d.xmlexpr.named_argvalue;
///       bool  *argnull  = op->d.xmlexpr.named_argnull;
///       StringInfoData buf; initStringInfo(&buf); i = 0;
///       forboth(lc, xexpr->named_args, lc2, xexpr->arg_names) {
///         Expr *e = lfirst(lc); char *argname = strVal(lfirst(lc2));
///         if (!argnull[i]) {
///           value = argvalue[i];
///           appendStringInfo(&buf, "<%s>%s</%s>", argname,
///             map_sql_value_to_xml_value(value, exprType((Node*)e), true), argname);
///           *op->resnull = false; }
///         i++; }
///       if (!*op->resnull) { text *result = cstring_to_text_with_len(buf.data, buf.len);
///                            *op->resvalue = PointerGetDatum(result); }
///       pfree(buf.data); break;
///     case IS_XMLELEMENT:
///       *op->resvalue = PointerGetDatum(xmlelement(xexpr,
///         op->d.xmlexpr.named_argvalue, op->d.xmlexpr.named_argnull,
///         op->d.xmlexpr.argvalue, op->d.xmlexpr.argnull));
///       *op->resnull = false; break;
///     case IS_XMLPARSE:
///       Datum *argvalue = op->d.xmlexpr.argvalue; bool *argnull = op->d.xmlexpr.argnull;
///       Assert(list_length(xexpr->args) == 2);
///       if (argnull[0]) return; data = DatumGetTextPP(argvalue[0]);
///       if (argnull[1]) return; preserve_whitespace = DatumGetBool(argvalue[1]);
///       *op->resvalue = PointerGetDatum(xmlparse(data, xexpr->xmloption, preserve_whitespace));
///       *op->resnull = false; break;
///     case IS_XMLPI:
///       Assert(list_length(xexpr->args) <= 1);
///       if (xexpr->args) { isnull = op->d.xmlexpr.argnull[0];
///                          arg = isnull ? NULL : DatumGetTextPP(op->d.xmlexpr.argvalue[0]); }
///       else { arg = NULL; isnull = false; }
///       *op->resvalue = PointerGetDatum(xmlpi(xexpr->name, arg, isnull, op->resnull)); break;
///     case IS_XMLROOT:
///       Assert(list_length(xexpr->args) == 3);
///       if (argnull[0]) return; data = DatumGetXmlP(argvalue[0]);
///       version = argnull[1] ? NULL : DatumGetTextPP(argvalue[1]);
///       Assert(!argnull[2]); standalone = DatumGetInt32(argvalue[2]);
///       *op->resvalue = PointerGetDatum(xmlroot(data, version, standalone));
///       *op->resnull = false; break;
///     case IS_XMLSERIALIZE:
///       Assert(list_length(xexpr->args) == 1);
///       if (argnull[0]) return; value = argvalue[0];
///       *op->resvalue = PointerGetDatum(xmltotext_with_options(DatumGetXmlP(value),
///         xexpr->xmloption, xexpr->indent)); *op->resnull = false; break;
///     case IS_DOCUMENT:
///       Assert(list_length(xexpr->args) == 1);
///       if (argnull[0]) return; value = argvalue[0];
///       *op->resvalue = BoolGetDatum(xml_is_document(DatumGetXmlP(value)));
///       *op->resnull = false; break;
///     default: elog(ERROR, "unrecognized XML operation");
///   }
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

/// `ExecEvalJsonConstructor(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — JSON / JSONB object/array constructor.
///
/// C logic (execExprInterp.c:4656-4729):
///
///   JsonConstructorExprState *jcstate = op->d.json_constructor.jcstate;
///   JsonConstructorExpr *ctor = jcstate->constructor;
///   bool is_jsonb = ctor->returning->format->format_type == JS_FORMAT_JSONB;
///   bool isnull = false;
///   if (ctor->type == JSCTOR_JSON_ARRAY)
///     res = (is_jsonb ? jsonb_build_array_worker : json_build_array_worker)
///             (jcstate->nargs, jcstate->arg_values, jcstate->arg_nulls,
///              jcstate->arg_types, ctor->absent_on_null);
///   else if (ctor->type == JSCTOR_JSON_OBJECT)
///     res = (is_jsonb ? jsonb_build_object_worker : json_build_object_worker)
///             (jcstate->nargs, jcstate->arg_values, jcstate->arg_nulls,
///              jcstate->arg_types, ctor->absent_on_null, ctor->unique);
///   else if (ctor->type == JSCTOR_JSON_SCALAR) {
///     if (jcstate->arg_nulls[0]) { res = (Datum)0; isnull = true; }
///     else { value = jcstate->arg_values[0];
///            outfuncid = jcstate->arg_type_cache[0].outfuncid;
///            category = jcstate->arg_type_cache[0].category;
///            res = is_jsonb ? datum_to_jsonb(value, category, outfuncid)
///                           : datum_to_json(value, category, outfuncid); } }
///   else if (ctor->type == JSCTOR_JSON_PARSE) {
///     if (jcstate->arg_nulls[0]) { res = (Datum)0; isnull = true; }
///     else { value = jcstate->arg_values[0]; js = DatumGetTextP(value);
///            if (is_jsonb) res = jsonb_from_text(js, true);
///            else { (void) json_validate(js, true, true); res = value; } } }
///   else elog(ERROR, "invalid JsonConstructorExpr type %d", ctor->type);
///   *op->resvalue = res; *op->resnull = isnull;
pub fn ExecEvalJsonConstructor<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = (state, op, econtext, estate);
    panic!(
        "execExprInterp: EEOP_JSON_CONSTRUCTOR — the JsonConstructorExpr node \
         (jcstate->constructor, whose type/returning->format/absent_on_null/\
         unique drive the switch) is parked off JsonConstructorExprState, and \
         the json.c/jsonb.c constructor workers (json[b]_build_array_worker, \
         json[b]_build_object_worker, datum_to_json[b], jsonb_from_text, \
         json_validate) have no executor-facing seam — not yet ported"
    )
}

/// `ExecEvalJsonIsPredicate(ExprState *state, ExprEvalStep *op)` —
/// `IS JSON [VALUE|OBJECT|ARRAY|SCALAR]` predicate.
///
/// C logic (execExprInterp.c:4734-4817):
///
///   JsonIsPredicate *pred = op->d.is_json.pred;
///   Datum js = *op->resvalue;
///   if (*op->resnull) { *op->resvalue = BoolGetDatum(false); return; }
///   exprtype = exprType(pred->expr);
///   if (exprtype == TEXTOID || exprtype == JSONOID) {
///     text *json = DatumGetTextP(js);
///     if (pred->item_type == JS_TYPE_ANY) res = true;
///     else switch (json_get_first_token(json, false)) {
///       case JSON_TOKEN_OBJECT_START: res = pred->item_type == JS_TYPE_OBJECT; break;
///       case JSON_TOKEN_ARRAY_START:  res = pred->item_type == JS_TYPE_ARRAY;  break;
///       case JSON_TOKEN_STRING: case JSON_TOKEN_NUMBER: case JSON_TOKEN_TRUE:
///       case JSON_TOKEN_FALSE:  case JSON_TOKEN_NULL:
///         res = pred->item_type == JS_TYPE_SCALAR; break;
///       default: res = false; break; }
///     if (res && (pred->unique_keys || exprtype == TEXTOID))
///       res = json_validate(json, pred->unique_keys, false);
///   } else if (exprtype == JSONBOID) {
///     if (pred->item_type == JS_TYPE_ANY) res = true;
///     else { Jsonb *jb = DatumGetJsonbP(js);
///       switch (pred->item_type) {
///         case JS_TYPE_OBJECT: res = JB_ROOT_IS_OBJECT(jb); break;
///         case JS_TYPE_ARRAY:  res = JB_ROOT_IS_ARRAY(jb) && !JB_ROOT_IS_SCALAR(jb); break;
///         case JS_TYPE_SCALAR: res = JB_ROOT_IS_ARRAY(jb) && JB_ROOT_IS_SCALAR(jb); break;
///         default: res = false; break; } }
///   } else res = false;
///   *op->resvalue = BoolGetDatum(res);
pub fn ExecEvalJsonIsPredicate<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = (state, op, estate);
    panic!(
        "execExprInterp: EEOP_IS_JSON — the JsonIsPredicate node \
         (op->d.is_json.pred, parked as ExprEvalStepData::IsJson {{ pred: usize }}; \
         supplies pred->expr/item_type/unique_keys) and the json.c/jsonb.c \
         probes (exprType, json_get_first_token, json_validate, the JB_ROOT_IS_* \
         macros) have no executor-facing seam — not yet ported"
    )
}

/// `ExecEvalJsonExprPath(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — run a jsonpath for JSON_VALUE/QUERY/EXISTS,
/// choosing the success/error/empty coercion jump. Returns the next step
/// address (one of jump_error/jump_empty/jump_eval_coercion/jump_end).
///
/// C logic (execExprInterp.c:4834-5029):
///
///   JsonExprState *jsestate = op->d.jsonexpr.jsestate;
///   JsonExpr *jsexpr = jsestate->jsexpr;
///   bool throw_error = jsexpr->on_error->btype == JSON_BEHAVIOR_ERROR;
///   bool error = false, empty = false;
///   int jump_eval_coercion = jsestate->jump_eval_coercion;
///   char *val_string = NULL;
///   item = jsestate->formatted_expr.value;
///   path = DatumGetJsonPathP(jsestate->pathspec.value);
///   memset(&jsestate->error, 0, sizeof(NullableDatum));
///   memset(&jsestate->empty, 0, sizeof(NullableDatum));
///   if (jsestate->escontext.details_wanted) {
///     jsestate->escontext.error_data = NULL; jsestate->escontext.details_wanted = false; }
///   jsestate->escontext.error_occurred = false;
///   switch (jsexpr->op) {
///     case JSON_EXISTS_OP:
///       exists = JsonPathExists(item, path, !throw_error ? &error : NULL, jsestate->args);
///       if (!error) { *op->resnull = false; *op->resvalue = BoolGetDatum(exists); } break;
///     case JSON_QUERY_OP:
///       *op->resvalue = JsonPathQuery(item, path, jsexpr->wrapper, &empty,
///         !throw_error ? &error : NULL, jsestate->args, jsexpr->column_name);
///       *op->resnull = (DatumGetPointer(*op->resvalue) == NULL); break;
///     case JSON_VALUE_OP:
///       jbv = JsonPathValue(item, path, &empty, !throw_error ? &error : NULL,
///         jsestate->args, jsexpr->column_name);
///       if (jbv == NULL) { *op->resvalue = (Datum)0; *op->resnull = true; }
///       else if (!error && !empty) {
///         if (returning->typid == JSONOID || JSONBOID)
///           val_string = DatumGetCString(DirectFunctionCall1(jsonb_out,
///             JsonbPGetDatum(JsonbValueToJsonb(jbv))));
///         else if (jsexpr->use_json_coercion) {
///           *op->resvalue = JsonbPGetDatum(JsonbValueToJsonb(jbv)); *op->resnull = false; }
///         else { val_string = ExecGetJsonValueItemString(jbv, op->resnull);
///                if (!jsexpr->use_io_coercion)
///                  *op->resvalue = DirectFunctionCall1(textin, CStringGetDatum(val_string)); } }
///       break;
///     default: elog(ERROR, "unrecognized SQL/JSON expression op %d", jsexpr->op); return false; }
///   if (!*op->resnull && jsexpr->use_io_coercion) {
///     fcinfo = jsestate->input_fcinfo;
///     fcinfo->args[0].value = PointerGetDatum(val_string); fcinfo->args[0].isnull = *op->resnull;
///     fcinfo->isnull = false; *op->resvalue = FunctionCallInvoke(fcinfo);
///     if (SOFT_ERROR_OCCURRED(&jsestate->escontext)) error = true; }
///   if (empty) {
///     *op->resvalue = (Datum)0; *op->resnull = true;
///     if (jsexpr->on_empty) { if (on_empty->btype != JSON_BEHAVIOR_ERROR) {
///         jsestate->empty.value = BoolGetDatum(true);
///         jsestate->escontext.error_occurred = false; jsestate->escontext.details_wanted = true;
///         return jsestate->jump_empty >= 0 ? jsestate->jump_empty : jsestate->jump_end; } }
///     else if (on_error->btype != JSON_BEHAVIOR_ERROR) {
///         jsestate->error.value = BoolGetDatum(true);
///         jsestate->escontext.error_occurred = false; jsestate->escontext.details_wanted = true;
///         return jsestate->jump_error >= 0 ? jsestate->jump_error : jsestate->jump_end; }
///     ereport(ERROR, errcode(ERRCODE_NO_SQL_JSON_ITEM), "no SQL/JSON item found ..."); }
///   if (error) {
///     *op->resvalue = (Datum)0; *op->resnull = true; jsestate->error.value = BoolGetDatum(true);
///     jsestate->escontext.error_occurred = false; jsestate->escontext.details_wanted = true;
///     return jsestate->jump_error >= 0 ? jsestate->jump_error : jsestate->jump_end; }
///   return jump_eval_coercion >= 0 ? jump_eval_coercion : jsestate->jump_end;
pub fn ExecEvalJsonExprPath<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<i32> {
    let _ = (state, op, econtext, estate);
    panic!(
        "execExprInterp: EEOP_JSONEXPR_PATH — the whole JsonExprState runtime-state \
         group (op->d.jsonexpr.jsestate, parked as ExprEvalStepData::JsonExpr \
         {{ jsestate: usize }}: jsexpr/formatted_expr/pathspec/args/escontext/\
         jump_*/input_fcinfo/error/empty) is not ported — its companion compiler \
         emitter exec_init_json_expr (execExpr_json.rs) panics for the same \
         reason — and the jsonpath_exec.c workers (JsonPathExists/Query/Value, \
         DatumGetJsonPathP, JsonbValueToJsonb) have no executor-facing seam"
    )
}

/// `ExecGetJsonValueItemString(JsonbValue *item, bool *resnull)` — render a
/// scalar jsonb item as its text form. Allocates the result string.
///
/// The `JsonbValue` argument is owned by the jsonb adt unit (the real type is
/// `types_jsonb::...::JsonbValue`); this helper's only caller,
/// `ExecEvalJsonExprPath`, is itself gated behind the unported `JsonExprState`,
/// so the trimmed real type is not threaded here yet — the parameter remains the
/// raw jsonb `Datum` the (still-parked) caller would hold.
///
/// C logic (execExprInterp.c:5036-5102):
///
///   *resnull = false;
///   switch (item->type) {
///     case jbvNull:    *resnull = true; return NULL;
///     case jbvString:  str = palloc(len+1); memcpy(str, val, len); str[len]=0; return str;
///     case jbvNumeric: return DatumGetCString(DirectFunctionCall1(numeric_out,
///                               NumericGetDatum(item->val.numeric)));
///     case jbvBool:    return DatumGetCString(DirectFunctionCall1(boolout,
///                               BoolGetDatum(item->val.boolean)));
///     case jbvDatetime: switch (item->val.datetime.typid) {
///       case DATEOID:        return DatumGetCString(DirectFunctionCall1(date_out, ...));
///       case TIMEOID:        return DatumGetCString(DirectFunctionCall1(time_out, ...));
///       case TIMETZOID:      return DatumGetCString(DirectFunctionCall1(timetz_out, ...));
///       case TIMESTAMPOID:   return DatumGetCString(DirectFunctionCall1(timestamp_out, ...));
///       case TIMESTAMPTZOID: return DatumGetCString(DirectFunctionCall1(timestamptz_out, ...));
///       default: elog(ERROR, "unexpected jsonb datetime type oid %u", typid); }
///     case jbvArray: case jbvObject: case jbvBinary:
///       return DatumGetCString(DirectFunctionCall1(jsonb_out,
///         JsonbPGetDatum(JsonbValueToJsonb(item))));
///     default: elog(ERROR, "unexpected jsonb value type %d", item->type); }
///   Assert(false); *resnull = true; return NULL;
pub fn ExecGetJsonValueItemString<'mcx>(
    mcx: Mcx<'mcx>,
    item: types_datum::Datum,
) -> PgResult<(Option<PgString<'mcx>>, bool)> {
    let _ = (mcx, item);
    panic!(
        "execExprInterp: ExecGetJsonValueItemString — the per-type *_out cast \
         targets (numeric_out/boolout/date_out/time_out/timetz_out/timestamp_out/\
         timestamptz_out/jsonb_out via DirectFunctionCall1) and JsonbValueToJsonb \
         have no executor-facing seam; its sole caller ExecEvalJsonExprPath is \
         gated behind the unported JsonExprState — not yet ported"
    )
}

/// `ExecEvalJsonCoercion(ExprState *state, ExprEvalStep *op,
/// ExprContext *econtext)` — coerce a JSON path result to the output type.
///
/// C logic (execExprInterp.c:5111-5161):
///
///   ErrorSaveContext *escontext = op->d.jsonexpr_coercion.escontext;
///   if (op->d.jsonexpr_coercion.exists_coerce) {
///     if (op->d.jsonexpr_coercion.exists_cast_to_int) {
///       if (op->d.jsonexpr_coercion.exists_check_domain &&
///           !domain_check_safe(*op->resvalue, *op->resnull,
///                              op->d.jsonexpr_coercion.targettype,
///                              &op->d.jsonexpr_coercion.json_coercion_cache,
///                              econtext->ecxt_per_query_memory, (Node*)escontext)) {
///         *op->resnull = true; *op->resvalue = (Datum)0; }
///       else *op->resvalue = DirectFunctionCall1(bool_int4, *op->resvalue);
///       return; }
///     *op->resvalue = DirectFunctionCall1(jsonb_in,
///       DatumGetBool(*op->resvalue) ? CStringGetDatum("true") : CStringGetDatum("false")); }
///   *op->resvalue = json_populate_type(*op->resvalue, JSONBOID,
///     op->d.jsonexpr_coercion.targettype, op->d.jsonexpr_coercion.targettypmod,
///     &op->d.jsonexpr_coercion.json_coercion_cache, econtext->ecxt_per_query_memory,
///     op->resnull, op->d.jsonexpr_coercion.omit_quotes, (Node*)escontext);
pub fn ExecEvalJsonCoercion<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = (state, op, econtext, estate);
    panic!(
        "execExprInterp: EEOP_JSONEXPR_COERCION — the ErrorSaveContext escontext \
         and json_coercion_cache (op->d.jsonexpr_coercion, parked opaque off the \
         unported JsonExprState unit) and the coercion workers (json_populate_type, \
         domain_check_safe, bool_int4/jsonb_in via DirectFunctionCall1) have no \
         executor-facing seam — not yet ported"
    )
}

/// `GetJsonBehaviorValueString(JsonBehavior *behavior)` — text of an ON
/// ERROR / ON EMPTY behavior for error messages. Allocates the string.
///
/// The `JsonBehavior` node is owned by the parsenodes/SQL-JSON unit (not yet
/// ported); the helper reads it off the step's compiled `JsonExprState` payload,
/// which is parked, so it takes the owning `ExprState` + step index here.
///
/// C logic (execExprInterp.c:5163-5184):
///
///   const char *behavior_names[] = { "NULL","ERROR","EMPTY","TRUE","FALSE",
///                                    "UNKNOWN","EMPTY ARRAY","EMPTY OBJECT","DEFAULT" };
///   return pstrdup(behavior_names[behavior->btype]);
pub fn GetJsonBehaviorValueString<'mcx>(
    mcx: Mcx<'mcx>,
    state: &ExprState<'mcx>,
    op: usize,
) -> PgResult<PgString<'mcx>> {
    let _ = (mcx, state, op);
    panic!(
        "execExprInterp: GetJsonBehaviorValueString — the JsonBehavior node \
         (read off the parked JsonExprState->jsexpr->on_error/on_empty; supplies \
         behavior->btype) is not ported — the JSON SQL/JSON parsenode + state \
         unit has not landed"
    )
}

/// `ExecEvalJsonCoercionFinish(ExprState *state, ExprEvalStep *op)` — finalize
/// a JSON coercion that needed a sub-expression evaluation.
///
/// C logic (execExprInterp.c:5191-5233):
///
///   JsonExprState *jsestate = op->d.jsonexpr.jsestate;
///   if (SOFT_ERROR_OCCURRED(&jsestate->escontext)) {
///     if (DatumGetBool(jsestate->error.value))
///       ereport(ERROR, errcode(ERRCODE_DATATYPE_MISMATCH),
///         errmsg("could not coerce %s expression (%s) to the RETURNING type",
///                "ON ERROR", GetJsonBehaviorValueString(jsestate->jsexpr->on_error)),
///         errdetail("%s", jsestate->escontext.error_data->message));
///     else if (DatumGetBool(jsestate->empty.value))
///       ereport(ERROR, errcode(ERRCODE_DATATYPE_MISMATCH),
///         errmsg("could not coerce %s expression (%s) to the RETURNING type",
///                "ON EMPTY", GetJsonBehaviorValueString(jsestate->jsexpr->on_empty)),
///         errdetail("%s", jsestate->escontext.error_data->message));
///     *op->resvalue = (Datum)0; *op->resnull = true;
///     jsestate->error.value = BoolGetDatum(true);
///     jsestate->escontext.error_occurred = false; jsestate->escontext.details_wanted = true; }
pub fn ExecEvalJsonCoercionFinish<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = (state, op, estate);
    panic!(
        "execExprInterp: EEOP_JSONEXPR_COERCION_FINISH — the JsonExprState \
         runtime-state group (op->d.jsonexpr.jsestate, parked as \
         ExprEvalStepData::JsonExpr {{ jsestate: usize }}: escontext/error/empty/\
         jsexpr->on_error/on_empty) is not ported — same blocker as its compiler \
         emitter exec_init_json_expr (execExpr_json.rs)"
    )
}
