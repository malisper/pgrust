//! Array-elements SRFs (jsonfuncs.c:2208-2463): `jsonb_array_elements` /
//! `jsonb_array_elements_text` plus `elements_worker_jsonb`, and
//! `json_array_elements` / `json_array_elements_text` plus `elements_worker`
//! and its `ElementsState` SAX callbacks (`elements_array_element_start`,
//! `elements_array_element_end`, `elements_object_start`, `elements_scalar`).
//!
//! Each is a single-column materialize-mode SRF (one output column, `jsonb`
//! or `text`). Per the repo SRF convention (cf. `keys.rs`) the fmgr entry
//! points run `InitMaterializedSRF(fcinfo, MAT_SRF_BLESS)` + per-row
//! `materialized_srf_putvalues`; the document bytes arrive through the
//! `srf_arg_varlena_bytes` seam. The workers collect the element rows; a
//! row value of `None` is a SQL NULL (text mode `jbvNull`).

use core::cell::RefCell;

use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::vec::Vec;

use ::utils_error::ereport;
use ::mcx::Mcx;
use ::types_error::error::{ERRCODE_INVALID_PARAMETER_VALUE, ERROR};
use ::types_error::PgResult;
use ::types_json::{JsonLexContext, JsonParseErrorType, JsonSemAction, JsonTokenType};
use types_jsonb::jsonb_util::JsonbValue;
use ::types_jsonb::jsonb::{
    jbvType, json_container_is_array, json_container_is_scalar, JsonbIteratorToken,
};
use ::nodes::fmgr::FunctionCallInfoBaseData;
use ::types_tuple::Datum;

use ::jsonb_util::{JsonbIteratorInit, JsonbIteratorNext, JsonbValueToJsonb};
use funcapi_seams as funcapi;

/// One emitted element row's column value: `None` is a SQL NULL, `Some(bytes)`
/// is the raw payload bytes — text (no varlena header, wrapped by
/// `bytes_to_varlena_v`) or a full `jsonb` varlena (`JsonbValueToJsonb`).
pub type ElementRow = Option<Vec<u8>>;

/// Read the leading `JsonbContainer.header` word from the root container bytes.
#[inline]
fn container_header(root: &[u8]) -> u32 {
    u32::from_ne_bytes([root[0], root[1], root[2], root[3]])
}

// ===========================================================================
// jsonb_array_elements / jsonb_array_elements_text (jsonfuncs.c:2207-2293).
// ===========================================================================

/// Core of `elements_worker_jsonb` (jsonfuncs.c:2219-2293): walk the binary
/// jsonb array root, emitting one element row per `WJB_ELEM`. `jb` is the full
/// `jsonb` varlena; the root container starts after the varlena header.
fn elements_worker_jsonb(
    mcx: Mcx<'_>,
    jb: &[u8],
    funcname: &str,
    as_text: bool,
) -> PgResult<Vec<ElementRow>> {
    let _ = funcname;
    let root = crate::common::vardata_any(jb);
    let header = container_header(root);

    // if (JB_ROOT_IS_SCALAR(jb)) ... "cannot extract elements from a scalar"
    if json_container_is_scalar(header) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("cannot extract elements from a scalar")
            .into_error());
    }
    // else if (!JB_ROOT_IS_ARRAY(jb)) ... "cannot extract elements from an object"
    else if !json_container_is_array(header) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("cannot extract elements from an object")
            .into_error());
    }

    let mut rows: Vec<ElementRow> = Vec::new();

    // it = JsonbIteratorInit(mcx, &jb->root);
    let mut it = JsonbIteratorInit(mcx, root);
    let mut v = JsonbValue::null();
    let mut skip_nested = false;

    // while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE)
    loop {
        let r = JsonbIteratorNext(&mut it, &mut v, skip_nested)?;
        if r == JsonbIteratorToken::WJB_DONE {
            break;
        }

        skip_nested = true;

        if r == JsonbIteratorToken::WJB_ELEM {
            if as_text {
                if v.typ == jbvType::jbvNull {
                    // a json null is an sql null in text mode
                    rows.push(None);
                } else {
                    // values[0] = PointerGetDatum(JsonbValueAsText(&v));
                    let text = crate::common::JsonbValueAsText(mcx, &v)?;
                    // JsonbValueAsText only returns None for jbvNull (handled above).
                    let bytes = text
                        .expect("JsonbValueAsText returns Some for non-null scalar")
                        .to_vec();
                    rows.push(Some(bytes));
                }
            } else {
                // Not in text mode, just return the Jsonb.
                let val = JsonbValueToJsonb(mcx, &v)?;
                rows.push(Some(val.to_vec()));
            }
        }
    }

    Ok(rows)
}

/// `jsonb_array_elements(PG_FUNCTION_ARGS)` (jsonfuncs.c:2207).
pub fn jsonb_array_elements<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    jsonb_array_elements_impl(mcx, fcinfo, "jsonb_array_elements", false)
}

/// `jsonb_array_elements_text(PG_FUNCTION_ARGS)` (jsonfuncs.c:2213).
pub fn jsonb_array_elements_text<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    jsonb_array_elements_impl(mcx, fcinfo, "jsonb_array_elements_text", true)
}

/// Shared fmgr body for the two jsonb entry points (jsonfuncs.c:2219-2293).
fn jsonb_array_elements_impl<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    funcname: &str,
    as_text: bool,
) -> PgResult<Datum<'mcx>> {
    // Jsonb *jb = PG_GETARG_JSONB_P(0);
    let jb = funcapi::srf_arg_varlena_bytes::call(mcx, fcinfo, 0)?;

    // (the scalar/object root checks happen inside the worker, mirroring C
    //  which performs them before InitMaterializedSRF.)
    let rows = elements_worker_jsonb(mcx, &jb, funcname, as_text)?;

    // InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC | MAT_SRF_BLESS);
    // A single-column (`jsonb`/`text`) SRF returns `SETOF jsonb`/`SETOF text`,
    // a SCALAR result type; `get_call_result_type` would reject it ("return
    // type must be a row type"), so C blesses the executor-supplied
    // `expectedDesc` (the 1-column `value` descriptor) instead.
    funcapi::InitMaterializedSRF::call(
        fcinfo,
        ::nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC | ::nodes::funcapi::MAT_SRF_BLESS,
    )?;

    put_element_rows(mcx, fcinfo, &rows, as_text)?;

    // PG_RETURN_NULL();
    Ok(Datum::null())
}

// ===========================================================================
// json_array_elements / json_array_elements_text (jsonfuncs.c:2295-2347).
// ===========================================================================

/// `ElementsState` (jsonfuncs.c:122): per-parse state for the json (text)
/// array-elements SRF.
///
/// The C struct's `lex` is the parser's concern (each callback receives the
/// live `&JsonLexContext`); `tuple_store`/`ret_tdesc`/`tmp_cxt` are the
/// materialize-SRF tuplestore's concern, so the collected rows accumulate in
/// `rows` and are put after the parse. `result_start` is a byte offset into the
/// lexer input.
struct ElementsState {
    /// Collected element rows (`None` is a SQL NULL value).
    rows: Vec<ElementRow>,
    /// `const char *function_name`.
    function_name: Box<str>,
    /// `bool normalize_results` (== `as_text`).
    normalize_results: bool,
    /// `bool next_scalar`.
    next_scalar: bool,
    /// `const char *result_start`: byte offset of the element start in the
    /// lexer input.
    result_start: usize,
    /// `char *normalized_scalar`: the de-escaped scalar token bytes.
    normalized_scalar: Vec<u8>,
}

/// `elements_array_element_start` (jsonfuncs.c:2349-2369).
fn elements_array_element_start(state: &mut ElementsState, lex: &JsonLexContext) {
    // save a pointer to where the value starts
    if lex.lex_level == 1 {
        // next_scalar will be reset in the array_element_end handler ...
        if state.normalize_results && lex.token_type == JsonTokenType::JSON_TOKEN_STRING {
            state.next_scalar = true;
        } else {
            state.result_start = lex.token_start;
        }
    }
}

/// `elements_array_element_end` (jsonfuncs.c:2371-2415).
fn elements_array_element_end(state: &mut ElementsState, lex: &JsonLexContext, isnull: bool) {
    // skip over nested objects
    if lex.lex_level != 1 {
        return;
    }

    if isnull && state.normalize_results {
        // nulls[0] = true; values[0] = (Datum) NULL;
        state.rows.push(None);
    } else if state.next_scalar {
        // values[0] = CStringGetTextDatum(_state->normalized_scalar);
        state
            .rows
            .push(Some(core::mem::take(&mut state.normalized_scalar)));
        state.next_scalar = false;
    } else {
        // len = prev_token_terminator - result_start;
        // val = cstring_to_text_with_len(result_start, len);
        let bytes = lex.input[state.result_start..lex.prev_token_terminator].to_vec();
        state.rows.push(Some(bytes));
    }
}

/// `elements_object_start` (jsonfuncs.c:2417-2430).
fn elements_object_start(state: &ElementsState, lex: &JsonLexContext) -> PgResult<()> {
    // json structure check
    if lex.lex_level == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(alloc::format!(
                "cannot call {} on a non-array",
                state.function_name
            ))
            .into_error());
    }
    Ok(())
}

/// `elements_scalar` (jsonfuncs.c:2432-2449).
fn elements_scalar(state: &mut ElementsState, lex: &JsonLexContext, token: &[u8]) -> PgResult<()> {
    // json structure check
    if lex.lex_level == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(alloc::format!(
                "cannot call {} on a scalar",
                state.function_name
            ))
            .into_error());
    }

    // supply de-escaped value if required
    if state.next_scalar {
        state.normalized_scalar = token.to_vec();
    }

    Ok(())
}

/// Core of `elements_worker` (jsonfuncs.c:2307-2347): parse the json text with
/// the `elements_*` SAX callbacks, collecting the array element rows.
pub fn elements_worker(json: &[u8], funcname: &str, as_text: bool) -> PgResult<Vec<ElementRow>> {
    // state = palloc0(sizeof(ElementsState)); sem = palloc0(sizeof(JsonSemAction));
    let state = Rc::new(RefCell::new(ElementsState {
        rows: Vec::new(),
        function_name: funcname.into(),
        normalize_results: as_text,
        next_scalar: false,
        result_start: 0,
        normalized_scalar: Vec::new(),
    }));
    let mut sem = JsonSemAction::default();

    // sem->object_start = elements_object_start;
    {
        let state = Rc::clone(&state);
        sem.object_start = Some(Box::new(move |lex: &JsonLexContext| {
            elements_object_start(&state.borrow(), lex)
        }));
    }
    // sem->scalar = elements_scalar;
    {
        let state = Rc::clone(&state);
        sem.scalar = Some(Box::new(
            move |lex: &JsonLexContext, token: &[u8], _tokentype: JsonTokenType| {
                elements_scalar(&mut state.borrow_mut(), lex, token)
            },
        ));
    }
    // sem->array_element_start = elements_array_element_start;
    {
        let state = Rc::clone(&state);
        sem.array_element_start = Some(Box::new(move |lex: &JsonLexContext, _isnull: bool| {
            elements_array_element_start(&mut state.borrow_mut(), lex);
            Ok(())
        }));
    }
    // sem->array_element_end = elements_array_element_end;
    {
        let state = Rc::clone(&state);
        sem.array_element_end = Some(Box::new(move |lex: &JsonLexContext, isnull: bool| {
            elements_array_element_end(&mut state.borrow_mut(), lex, isnull);
            Ok(())
        }));
    }

    // makeJsonLexContext(&lex, json, as_text); pg_parse_json_or_ereport(&lex, sem);
    // elements only needs escaped strings when as_text.
    let encoding = jsonapi_seams::get_database_encoding::call();
    let result = jsonapi_seams::pg_parse_json::call(json, encoding, as_text, &mut sem)?;
    if result != JsonParseErrorType::JSON_SUCCESS {
        jsonapi_seams::errsave_error::call(result, json, as_text, None)?;
        unreachable!("errsave_error with no escontext raises");
    }

    let rows = core::mem::take(&mut state.borrow_mut().rows);
    Ok(rows)
}

/// `json_array_elements(PG_FUNCTION_ARGS)` (jsonfuncs.c:2295).
pub fn json_array_elements<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    json_array_elements_impl(mcx, fcinfo, "json_array_elements", false)
}

/// `json_array_elements_text(PG_FUNCTION_ARGS)` (jsonfuncs.c:2301).
pub fn json_array_elements_text<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    json_array_elements_impl(mcx, fcinfo, "json_array_elements_text", true)
}

/// Shared fmgr body for the two json (text) entry points (jsonfuncs.c:2307-2347).
fn json_array_elements_impl<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    funcname: &str,
    as_text: bool,
) -> PgResult<Datum<'mcx>> {
    // text *json = PG_GETARG_TEXT_PP(0);
    // The seam yields the header-ful varlena image; the json (text) document is
    // its VARDATA (skip the 4-byte length word), as C reads via VARDATA_ANY.
    let json_image = funcapi::srf_arg_varlena_bytes::call(mcx, fcinfo, 0)?;
    let json = crate::common::vardata_any(&json_image);

    // InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC | MAT_SRF_BLESS);
    // Single-column SRF: bless the executor-supplied 1-column `expectedDesc`
    // (a `SETOF json`/`text` SCALAR result type is not a row type).
    funcapi::InitMaterializedSRF::call(
        fcinfo,
        ::nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC | ::nodes::funcapi::MAT_SRF_BLESS,
    )?;

    let rows = elements_worker(json, funcname, as_text)?;

    put_element_rows(mcx, fcinfo, &rows, as_text)?;

    // PG_RETURN_NULL();
    Ok(Datum::null())
}

// ===========================================================================
// Shared row emission.
// ===========================================================================

/// Emit the collected element rows into the materialize-SRF tuplestore, one
/// single-column tuple per row. `as_text` selects the column type: `text`
/// (built through `bytes_to_varlena_v`) or `jsonb` (a full varlena, wrapped
/// directly as `Datum::ByRef`). A `None` row is a SQL NULL value.
fn put_element_rows<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    rows: &[ElementRow],
    as_text: bool,
) -> PgResult<()> {
    for row in rows {
        let (values, nulls): ([Datum<'mcx>; 1], [bool; 1]) = match row {
            None => ([Datum::null()], [true]),
            Some(bytes) => {
                let datum = if as_text {
                    varlena_seams::bytes_to_varlena_v::call(mcx, bytes)?
                } else {
                    // jsonb: bytes already a full varlena (VARHDRSZ + payload).
                    let mut v = ::mcx::vec_with_capacity_in::<u8>(mcx, bytes.len())?;
                    v.extend_from_slice(bytes);
                    Datum::ByRef(v)
                };
                ([datum], [false])
            }
        };
        let rsi = fcinfo
            .resultinfo
            .as_mut()
            .expect("InitMaterializedSRF set fcinfo->resultinfo");
        funcapi::materialized_srf_putvalues::call(rsi, &values, &nulls)?;
    }
    Ok(())
}

extern crate alloc;
