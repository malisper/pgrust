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
use types_error::{
    PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_NO_SQL_JSON_ITEM,
};
use types_nodes::execexpr::{
    ExprEvalStepData, ExprState, JsonCoercionCacheId, JsonExprStateId, ResultCellId,
};
use types_nodes::execnodes::EcxtId;
use types_nodes::EStateData;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::{JSONBOID, JSONOID, TEXTOID};

use backend_utils_adt_jsonpath_exec::{
    JsonPathExists, JsonPathQuery, JsonPathValue, JsonPathVariable, JsonPathVars,
    JsonWrapper as PathJsonWrapper,
};
use types_jsonb::backend_utils_adt_jsonb_util::{JsonbValue, JsonbValueData};
use types_nodes::primnodes::{JsonBehaviorType, JsonExprOp, JsonWrapper, XmlExprOp};

use crate::interp_loop::{read_cell, write_cell};

/// `ExecEvalXmlExpr(ExprState *state, ExprEvalStep *op)` (execExprInterp.c:4000)
/// — evaluate an XMLCONCAT / XMLFOREST / XMLELEMENT / XMLPARSE / XMLPI / XMLROOT
/// / XMLSERIALIZE / IS DOCUMENT expression. The arguments were compiled into
/// result cells by `exec_init_xml_expr`; this gathers them and dispatches to the
/// xml.c value workers in `backend_utils_adt_xml`.
pub fn ExecEvalXmlExpr<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;

    // Snapshot the step payload (the node + arg cell ids) so the per-arg
    // read_cell()s do not alias the &mut state borrow.
    let (xexpr, named_arg_cells, named_arg_types, arg_cells) =
        match &state.steps.as_ref().unwrap()[op].d {
            ExprEvalStepData::XmlExpr {
                xexpr,
                named_arg_cells,
                named_arg_types,
                arg_cells,
            } => (
                xexpr.clone(),
                named_arg_cells
                    .as_ref()
                    .map(|v| v.iter().copied().collect::<Vec<_>>())
                    .unwrap_or_default(),
                named_arg_types
                    .as_ref()
                    .map(|v| v.iter().copied().collect::<Vec<_>>())
                    .unwrap_or_default(),
                arg_cells
                    .as_ref()
                    .map(|v| v.iter().copied().collect::<Vec<_>>())
                    .unwrap_or_default(),
            ),
            _ => unreachable!("EEOP_XMLEXPR: payload is not XmlExpr"),
        };
    let resv = state.steps.as_ref().unwrap()[op].resvalue;
    let resnull = state.steps.as_ref().unwrap()[op].resnull;

    // *op->resnull = true; *op->resvalue = (Datum) 0;
    write_cell(state, resv, Datum::null(), true);
    if resnull != resv {
        write_cell(state, resnull, Datum::from_bool(true), false);
    }

    match xexpr.op {
        XmlExprOp::IS_XMLCONCAT => {
            // values = list of non-null arg payloads (all xml-typed).
            let mut values: Vec<Vec<u8>> = Vec::new();
            for &cell in &arg_cells {
                let (v, isnull) = read_cell(state, cell);
                if !isnull {
                    values.push(v.as_ref_bytes().to_vec());
                }
            }
            if !values.is_empty() {
                let out = backend_utils_adt_xml::xmlconcat(&values)?;
                write_xmltype(state, resv, resnull, mcx, &out)?;
            }
        }
        XmlExprOp::IS_XMLFOREST => {
            // forboth(named_args, arg_names): <name>map_sql_value_to_xml_value(...)</name>
            let mut buf: Vec<u8> = Vec::new();
            let mut any = false;
            for i in 0..named_arg_cells.len() {
                let (v, isnull) = read_cell(state, named_arg_cells[i]);
                if !isnull {
                    let argname = &xexpr.arg_names[i];
                    let mapped = map_named_value(v, named_arg_types[i])?;
                    buf.extend_from_slice(b"<");
                    buf.extend_from_slice(argname.as_bytes());
                    buf.extend_from_slice(b">");
                    buf.extend_from_slice(mapped.as_bytes());
                    buf.extend_from_slice(b"</");
                    buf.extend_from_slice(argname.as_bytes());
                    buf.extend_from_slice(b">");
                    any = true;
                }
            }
            if any {
                // cstring_to_text_with_len — store as a plain text/xml varlena.
                write_xmltype(state, resv, resnull, mcx, &buf)?;
            }
        }
        XmlExprOp::IS_XMLELEMENT => {
            // named_args -> (attr-name, mapped-value-or-NULL); args -> content.
            let mut named: Vec<(String, Option<String>)> = Vec::new();
            for i in 0..named_arg_cells.len() {
                let (v, isnull) = read_cell(state, named_arg_cells[i]);
                let mapped = if isnull {
                    None
                } else {
                    Some(map_named_value(v, named_arg_types[i])?)
                };
                named.push((xexpr.arg_names[i].clone(), mapped));
            }
            // The C xmlelement walks xexpr->args and maps each via
            // map_sql_value_to_xml_value(argvalue[i], exprType(e), true) into
            // content; a NULL arg is skipped.
            let mut content: Vec<String> = Vec::new();
            for i in 0..arg_cells.len() {
                let (v, isnull) = read_cell(state, arg_cells[i]);
                if !isnull {
                    let typid = expr_type_of_arg(&xexpr, i)?;
                    content.push(map_named_value(v, typid)?);
                }
            }
            let name = xexpr.name.as_deref().unwrap_or("");
            let out = backend_utils_adt_xml::xmlelement(name, &named, &content)?;
            write_xmltype(state, resv, resnull, mcx, &out)?;
        }
        XmlExprOp::IS_XMLPARSE => {
            // args known to be (text, bool).
            let (v0, n0) = read_cell(state, arg_cells[0]);
            if n0 {
                return Ok(());
            }
            let data = v0.as_ref_bytes().to_vec();
            let (v1, n1) = read_cell(state, arg_cells[1]);
            if n1 {
                return Ok(());
            }
            let preserve_whitespace = v1.as_bool();
            let out = backend_utils_adt_xml::xmlparse(&data, xexpr.xmloption, preserve_whitespace)?;
            write_xmltype(state, resv, resnull, mcx, &out)?;
        }
        XmlExprOp::IS_XMLPI => {
            // optional argument known to be text.
            let (arg, isnull): (Option<Vec<u8>>, bool) = if !arg_cells.is_empty() {
                let (v, n) = read_cell(state, arg_cells[0]);
                if n {
                    (None, true)
                } else {
                    (Some(v.as_ref_bytes().to_vec()), false)
                }
            } else {
                (None, false)
            };
            let target = xexpr.name.as_deref().unwrap_or("");
            let (out, is_null) =
                backend_utils_adt_xml::xmlpi(target, arg.as_deref(), isnull)?;
            match out {
                Some(bytes) if !is_null => write_xmltype(state, resv, resnull, mcx, &bytes)?,
                _ => { /* result stays NULL */ }
            }
        }
        XmlExprOp::IS_XMLROOT => {
            // args known to be (xml, text, int).
            let (v0, n0) = read_cell(state, arg_cells[0]);
            if n0 {
                return Ok(());
            }
            let data = v0.as_ref_bytes().to_vec();
            let (v1, n1) = read_cell(state, arg_cells[1]);
            let version = if n1 { None } else { Some(v1.as_ref_bytes().to_vec()) };
            let (v2, _n2) = read_cell(state, arg_cells[2]); // always present
            let standalone = xml_standalone_from_i32(v2.as_usize() as i32);
            let out =
                backend_utils_adt_xml::xmlroot(&data, version.as_deref(), standalone)?;
            write_xmltype(state, resv, resnull, mcx, &out)?;
        }
        XmlExprOp::IS_XMLSERIALIZE => {
            // argument type known to be xml.
            let (v0, n0) = read_cell(state, arg_cells[0]);
            if n0 {
                return Ok(());
            }
            let data = v0.as_ref_bytes().to_vec();
            let out = backend_utils_adt_xml::xmltotext_with_options(
                &data,
                xexpr.xmloption,
                xexpr.indent,
            )?;
            write_xmltype(state, resv, resnull, mcx, &out)?;
        }
        XmlExprOp::IS_DOCUMENT => {
            // optional argument known to be xml.
            let (v0, n0) = read_cell(state, arg_cells[0]);
            if n0 {
                return Ok(());
            }
            let data = v0.as_ref_bytes().to_vec();
            let is_doc = backend_utils_adt_xml::xml_is_document(&data)?;
            write_cell(state, resv, Datum::from_bool(is_doc), false);
            if resnull != resv {
                write_cell(state, resnull, Datum::from_bool(false), false);
            }
        }
    }
    Ok(())
}

/// Write a freshly built xml/text payload into the result cell as a
/// by-reference Datum (the C `PointerGetDatum(result)` of a `cstring_to_text` /
/// `stringinfo_to_xmltype` value), clearing the NULL flag.
///
/// In this model `Datum::ByRef` carries the *header-less* detoasted payload (see
/// `cstring_to_text_with_len`, which returns the bare bytes via `slice_in`), so
/// the xml.c worker's payload is stored verbatim.
fn write_xmltype<'mcx>(
    state: &mut ExprState<'mcx>,
    resv: ResultCellId,
    resnull: ResultCellId,
    mcx: Mcx<'mcx>,
    bytes: &[u8],
) -> PgResult<()> {
    let payload = mcx::slice_in(mcx, bytes)?;
    write_cell(state, resv, Datum::ByRef(payload), false);
    if resnull != resv {
        write_cell(state, resnull, Datum::from_bool(false), false);
    }
    Ok(())
}

/// Map a SQL value Datum (read out of a result cell) through xml.c's
/// `map_sql_value_to_xml_value(value, typid, true)`.
///
/// The xml.c worker takes a bare-word `types_datum::Datum` and, for the native
/// text path, calls `OidOutputFunctionCall(typeOut, value)` through a seam that
/// receives the machine-word Datum bits. A by-value SQL value (bool / int /
/// date / timestamp) maps directly. A by-reference (varlena) value cannot be
/// carried by the bare machine word — that is the genuine by-ref-Datum substrate
/// gap (the same one ExecEvalJsonExprPath PASSING vars hit): the output-function
/// seam needs a stable pointer the inline-bytes `Datum::ByRef` model does not
/// supply. Such a value loud-panics rather than passing a bogus pointer.
fn map_named_value<'mcx>(
    value: Datum<'mcx>,
    typid: types_core::primitive::Oid,
) -> PgResult<String> {
    match value {
        Datum::ByVal(word) => backend_utils_adt_xml::map_sql_value_to_xml_value(
            types_datum::Datum::from_usize(word),
            typid,
            true,
        ),
        _ => panic!(
            "execExprInterp: ExecEvalXmlExpr — XMLELEMENT/XMLFOREST mapping of a by-reference \
             (varlena) value of type oid {} needs map_sql_value_to_xml_value to reach the \
             OidOutputFunctionCall / detoast seam, but the inline-bytes Datum::ByRef carrier \
             cannot supply the stable machine-word pointer those seams consume — the by-ref \
             Datum substrate for XML value mapping is not yet landed",
            typid
        ),
    }
}

/// `exprType(xexpr->args[i])` for an XMLELEMENT content argument. Recovered from
/// the compiled named-arg-type list is not available for positional args, so we
/// re-derive from the node via the nodeFuncs seam.
fn expr_type_of_arg(
    xexpr: &types_nodes::primnodes::XmlExpr,
    i: usize,
) -> PgResult<types_core::primitive::Oid> {
    let e = &xexpr.args[i];
    Ok(backend_nodes_nodeFuncs_seams::expr_type_info::call(e)?.typid)
}

/// Decode the `int` standalone argument of IS_XMLROOT (a `XmlStandaloneType`).
fn xml_standalone_from_i32(v: i32) -> backend_utils_adt_xml::XmlStandaloneType {
    use backend_utils_adt_xml::XmlStandaloneType as X;
    match v {
        0 => X::XML_STANDALONE_YES,
        1 => X::XML_STANDALONE_NO,
        2 => X::XML_STANDALONE_NO_VALUE,
        _ => X::XML_STANDALONE_OMITTED,
    }
}

/// `ExecEvalJsonConstructor(state, op, econtext)` (execExprInterp.c:4657) —
/// JSON / JSONB object/array constructor. The argument sub-expressions were
/// compiled into result cells by `exec_init_json_constructor`; this gathers them
/// into the carrier's `arg_values`/`arg_nulls`, then dispatches to the json.c /
/// jsonb.c build-object / build-array workers and wraps the serialized bytes
/// into a by-reference Datum.
pub fn ExecEvalJsonConstructor<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    use types_nodes::primnodes::JsonConstructorType as Ct;
    let _ = econtext;
    let mcx = estate.es_query_cxt;

    // Snapshot the carrier scalars + arg cells (so per-arg read_cell() doesn't
    // alias the &mut state borrow held below).
    let (ctor_type, is_jsonb, absent_on_null, unique, nargs, arg_cells, mut arg_values, mut arg_nulls, arg_types) =
        match &state.steps.as_ref().unwrap()[op].d {
            ExprEvalStepData::JsonConstructor { jcstate } => {
                let jc = jcstate
                    .as_ref()
                    .expect("EEOP_JSON_CONSTRUCTOR: jcstate present");
                (
                    jc.ctor_type,
                    jc.is_jsonb,
                    jc.absent_on_null,
                    jc.unique,
                    jc.nargs as usize,
                    jc.arg_cells
                        .as_ref()
                        .map(|v| v.iter().copied().collect::<Vec<_>>())
                        .unwrap_or_default(),
                    jc.arg_values
                        .as_ref()
                        .map(|v| v.iter().cloned().collect::<Vec<_>>())
                        .unwrap_or_default(),
                    jc.arg_nulls
                        .as_ref()
                        .map(|v| v.iter().copied().collect::<Vec<_>>())
                        .unwrap_or_default(),
                    jc.arg_types
                        .as_ref()
                        .map(|v| v.iter().copied().collect::<Vec<_>>())
                        .unwrap_or_default(),
                )
            }
            _ => unreachable!("EEOP_JSON_CONSTRUCTOR: payload is not JsonConstructor"),
        };
    let resv = state.steps.as_ref().unwrap()[op].resvalue;
    let resnull = state.steps.as_ref().unwrap()[op].resnull;

    // Gather the non-const args (the C `ExecInitExprRec`'d the live values into
    // jcstate->arg_values[i]; the owned model reads them back out of cells).
    for i in 0..nargs {
        if let Some(cell) = arg_cells.get(i).copied().flatten() {
            let (v, isnull) = read_cell(state, cell);
            arg_values[i] = v;
            arg_nulls[i] = isnull;
        }
    }

    let bytes: Vec<u8> = match ctor_type {
        Ct::JSCTOR_JSON_ARRAY => {
            if is_jsonb {
                backend_utils_adt_jsonb::jsonb_build_array_worker(
                    mcx, &arg_values, &arg_nulls, &arg_types, absent_on_null,
                )?
            } else {
                backend_utils_adt_json::json_build_array_worker(
                    mcx, &arg_values, &arg_nulls, &arg_types, absent_on_null,
                )?
            }
            .as_slice()
            .to_vec()
        }
        Ct::JSCTOR_JSON_OBJECT => {
            if is_jsonb {
                backend_utils_adt_jsonb::jsonb_build_object_worker(
                    mcx, &arg_values, &arg_nulls, &arg_types, absent_on_null, unique,
                )?
            } else {
                backend_utils_adt_json::json_build_object_worker(
                    mcx, &arg_values, &arg_nulls, &arg_types, absent_on_null, unique,
                )?
            }
            .as_slice()
            .to_vec()
        }
        Ct::JSCTOR_JSON_SCALAR => {
            // datum_to_json[b](value, category, outfuncid) needs the per-arg
            // json_categorize_type cache, which has no executor-facing seam.
            return Err(PgError::error(
                "JSON_SCALAR constructor (JSCTOR_JSON_SCALAR) is not yet supported: \
                 datum_to_json/datum_to_jsonb and json_categorize_type have no \
                 executor-facing seam",
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
        }
        Ct::JSCTOR_JSON_PARSE => {
            // JSON(text) / jsonb_from_text — only reachable here for the JSONB
            // `unique` case (text non-unique is shortcut at init). jsonb_from_text
            // has no executor-facing seam.
            return Err(PgError::error(
                "JSON PARSE constructor (JSCTOR_JSON_PARSE) is not yet supported: \
                 jsonb_from_text has no executor-facing seam",
            )
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
        }
        other => {
            return Err(PgError::error(format!(
                "invalid JsonConstructorExpr type {:?}",
                other
            )));
        }
    };

    // Wrap the serialized bytes into a by-reference Datum. The `json`/`jsonb`
    // by-ref convention in this port carries the FULL varlena image (the leading
    // VARHDRSZ length word + payload), matching the fmgr boundary
    // (`backend-utils-adt-json::fmgr_builtins` strips VARHDRSZ on the way in).
    //   * `jsonb_*_worker` returns `JsonbValueToJsonb`, already a full varlena.
    //   * `json_*_worker` returns the header-less json text payload, so frame it
    //     into a text varlena (C: the json output function's `cstring_to_text`).
    let image: mcx::PgVec<'mcx, u8> = if is_jsonb {
        // `jsonb_*_worker` returns `JsonbValueToJsonb`, already a full varlena.
        mcx::slice_in(mcx, &bytes)?
    } else {
        // `json_*_worker` returns the header-less json text payload; build the
        // text varlena image (4-byte length word + payload), mirroring C's
        // `cstring_to_text` (which is what the json output function returns).
        const VARHDRSZ: usize = 4;
        let total = bytes.len() + VARHDRSZ;
        let mut out = mcx::vec_with_capacity_in(mcx, total)?;
        out.extend_from_slice(&types_datum::varlena::set_varsize_4b(total));
        out.extend_from_slice(&bytes);
        out
    };
    let payload = mcx::slice_in(mcx, image.as_slice())?;
    write_cell(state, resv, Datum::ByRef(payload), false);
    if resnull != resv {
        write_cell(state, resnull, Datum::from_bool(false), false);
    }
    Ok(())
}

/// `ExecEvalJsonIsPredicate(state, op)` (execExprInterp.c:4733) —
/// `IS JSON [VALUE|OBJECT|ARRAY|SCALAR]` predicate. The subject value is already
/// in the result cell (compiled by `exec_init_json_is_predicate`); this reads it
/// and validates per the subject type (text/json via the json lexer, jsonb via
/// the root container header).
pub fn ExecEvalJsonIsPredicate<'mcx>(
    state: &mut ExprState<'mcx>,
    op: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    use types_jsonb::jsonb::{
        json_container_is_array, json_container_is_object, json_container_is_scalar,
    };
    use types_nodes::primnodes::JsonValueType as Jt;
    let _ = estate;

    let (item_type, unique_keys, arg_type) = match &state.steps.as_ref().unwrap()[op].d {
        ExprEvalStepData::IsJson {
            item_type,
            unique_keys,
            arg_type,
        } => (*item_type, *unique_keys, *arg_type),
        _ => unreachable!("EEOP_IS_JSON: payload is not IsJson"),
    };
    let resv = state.steps.as_ref().unwrap()[op].resvalue;
    let resnull = state.steps.as_ref().unwrap()[op].resnull;

    let (js, js_null) = read_cell(state, resv);
    if js_null {
        write_cell(state, resv, Datum::from_bool(false), false);
        if resnull != resv {
            write_cell(state, resnull, Datum::from_bool(false), false);
        }
        return Ok(());
    }

    let res: bool = if arg_type == TEXTOID || arg_type == JSONOID {
        // A text/json `ByRef` value is the FULL varlena image (4-byte VARHDRSZ
        // length word + payload); C's ExecEvalJsonIsPredicate detoasts and lexes
        // `VARDATA_ANY` / `VARSIZE_ANY_EXHDR` — the header-stripped payload. Skip
        // the VARHDRSZ word so `json_get_first_token` / `json_validate` see the
        // JSON text, not the length bytes.
        const VARHDRSZ: usize = 4;
        let img = js.as_ref_bytes();
        let json = if img.len() >= VARHDRSZ { &img[VARHDRSZ..] } else { img };
        let mut res = if item_type == Jt::JS_TYPE_ANY {
            true
        } else {
            // Mirror json_get_first_token: the first non-whitespace token
            // discriminates object `{` / array `[` / scalar.
            match first_json_token_kind(json) {
                FirstTok::ObjectStart => item_type == Jt::JS_TYPE_OBJECT,
                FirstTok::ArrayStart => item_type == Jt::JS_TYPE_ARRAY,
                FirstTok::Scalar => item_type == Jt::JS_TYPE_SCALAR,
                FirstTok::Invalid => false,
            }
        };
        // Full parse only for uniqueness check or text validation.
        if res && (unique_keys || arg_type == TEXTOID) {
            res = backend_utils_adt_json::json_validate(json, unique_keys, false)?;
        }
        res
    } else if arg_type == JSONBOID {
        if item_type == Jt::JS_TYPE_ANY {
            true
        } else {
            // The jsonb ByRef image is the full varlena (incl. VARHDRSZ); the
            // root container header u32 follows it (native order).
            let img = js.as_ref_bytes();
            let header = jsonb_root_header(img);
            match item_type {
                Jt::JS_TYPE_OBJECT => json_container_is_object(header),
                Jt::JS_TYPE_ARRAY => {
                    json_container_is_array(header) && !json_container_is_scalar(header)
                }
                Jt::JS_TYPE_SCALAR => {
                    json_container_is_array(header) && json_container_is_scalar(header)
                }
                Jt::JS_TYPE_ANY => true,
            }
        }
    } else {
        false
    };

    write_cell(state, resv, Datum::from_bool(res), false);
    if resnull != resv {
        write_cell(state, resnull, Datum::from_bool(false), false);
    }
    Ok(())
}

/// Return the `jsonpath` image in the form the `executeJsonPath` cores expect
/// (`jsonpath_header` reads `[4..8]`, `jsonpath_data` reads `[8..]`).
///
/// The `jsonpath` by-ref lane (and a `jsonpath` `Const`'s `ByRef` value) carries
/// the full `jsonpath` varlena image behind ONE extra leading `VARHDRSZ` word —
/// the canonical-`ByRef` → ABI bridge frames a pass-by-reference value that way,
/// so the on-the-wire bytes are `[outer VARHDRSZ][inner VARHDRSZ][version word]
/// [flattened nodes]`. Strip the single leading `VARHDRSZ` to recover the real
/// full `jsonpath` varlena the cores slice into — identical to the jsonpath
/// fmgr boundary's `arg_jsonpath_image` (jsonpath_exec `fmgr_builtins.rs`).
///
/// Verified against the `jsonpath` for `$.a`: `[136, 0,0,0,  120,0,0,0,
/// 1,0,0,128, …]` = `[outer len 34<<2][inner len][version|LAX]`; after stripping
/// the outer length word the cores read `version|LAX` at `[4..8]`.
fn normalize_jsonpath_image(image: &[u8]) -> Vec<u8> {
    const VARHDRSZ: usize = 4;
    if image.len() >= VARHDRSZ {
        image[VARHDRSZ..].to_vec()
    } else {
        image.to_vec()
    }
}

/// `JB_ROOT_*` header word of a jsonb varlena image: the `u32` immediately after
/// the 4-byte varlena length header (`&jb->root.header`, native byte order).
fn jsonb_root_header(image: &[u8]) -> u32 {
    const VARHDRSZ: usize = 4;
    if image.len() < VARHDRSZ + 4 {
        return 0;
    }
    let b = &image[VARHDRSZ..VARHDRSZ + 4];
    u32::from_ne_bytes([b[0], b[1], b[2], b[3]])
}

/// Result of the cheap first-token probe (the discriminating subset of
/// `json_get_first_token`).
enum FirstTok {
    ObjectStart,
    ArrayStart,
    Scalar,
    Invalid,
}

/// First non-whitespace JSON token kind, sufficient to discriminate
/// object/array/scalar for `IS JSON [OBJECT|ARRAY|SCALAR]` (full validity is
/// confirmed separately by `json_validate`). Mirrors `json_get_first_token`'s
/// classification: `{` -> object, `[` -> array, a valid scalar lead char ->
/// scalar, empty/garbage -> invalid.
fn first_json_token_kind(bytes: &[u8]) -> FirstTok {
    let s = bytes;
    let mut i = 0;
    while i < s.len() && (s[i] == b' ' || s[i] == b'\t' || s[i] == b'\n' || s[i] == b'\r') {
        i += 1;
    }
    if i >= s.len() {
        return FirstTok::Invalid;
    }
    match s[i] {
        b'{' => FirstTok::ObjectStart,
        b'[' => FirstTok::ArrayStart,
        b'"' | b'-' | b'0'..=b'9' | b't' | b'f' | b'n' => FirstTok::Scalar,
        _ => FirstTok::Invalid,
    }
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
    // `executeJsonPath`/`jspInit` consume the FULL `jsonpath` varlena image
    // (`jsonpath_header` reads `[4..8]`, `jsonpath_data` reads `[8..]`), i.e.
    // `[VARHDRSZ length word][version word][nodes]`. A `jsonpath` `Const`
    // const-folded from `jsonpath_in` carries only the logical payload
    // (`[version word][nodes]`, no leading `VARHDRSZ`); frame it back to the
    // full varlena the cores expect when the leading length word is absent.
    let path_bytes = normalize_jsonpath_image(path.as_ref_bytes());

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
        // DirectFunctionCall1: no soft escontext, so this never soft-fails.
        let jb = backend_utils_adt_jsonb::jsonb_in(mcx, s, None)?
            .expect("jsonb_in without escontext never soft-fails");
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
