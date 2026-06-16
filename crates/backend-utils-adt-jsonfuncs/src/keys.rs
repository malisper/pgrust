//! Object-keys SRFs (jsonfuncs.c:55-62, 567-836): `jsonb_object_keys` /
//! `json_object_keys` plus the `OkeysState` collector and its three SAX
//! callbacks (`okeys_object_field_start`, `okeys_array_start`, `okeys_scalar`).
//!
//! Both are set-returning functions. Per the repo SRF convention (cf.
//! `backend-backup-walsummaryfuncs::pg_available_wal_summaries`), the fmgr
//! entry points run `InitMaterializedSRF` + `materialized_srf_putvalues` over
//! the owned call frame; the document bytes come from the first varlena arg
//! through the funcapi `srf_arg_varlena_bytes` seam (the bare-word -> varlena
//! detoast boundary is fmgr-owned). The actual key-collection logic lives in
//! `*_worker(&[u8])` cores: the jsonb path walks the on-disk tree through the
//! landed `jsonb_util.c` iterator API; the json (text) path drives the three
//! `okeys_*` SAX callbacks over the `common/jsonapi.c` parser through the
//! `common-jsonapi-seams::pg_parse_json` SAX-driver seam.

use core::cell::RefCell;

use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::vec::Vec;

use backend_utils_error::ereport;
use mcx::Mcx;
use types_error::error::{ERRCODE_INVALID_PARAMETER_VALUE, ERROR};
use types_error::PgResult;
use types_json::{JsonLexContext, JsonParseErrorType, JsonSemAction, JsonTokenType};
use types_jsonb::backend_utils_adt_jsonb_util::{JsonbValue, JsonbValueData};
use types_jsonb::jsonb::{
    json_container_is_array, json_container_is_scalar, json_container_size, JsonbIteratorToken,
    VARHDRSZ,
};
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::Datum;

use backend_utils_adt_jsonb_util::{JsonbIteratorInit, JsonbIteratorNext};
use backend_utils_fmgr_funcapi_seams as funcapi;

/// `OkeysState` (jsonfuncs.c:55): collects the top-level object keys for the
/// json (text) `json_object_keys` SRF.
///
/// The C struct also carries `lex` (the shared lexer the callbacks read
/// `lex_level` off of), `result_size` (the `palloc`'d capacity), and
/// `sent_count` (the per-call SRF cursor). Here the parse driver hands the live
/// `&JsonLexContext` to each callback, the capacity is `Vec`'s own bookkeeping,
/// and the per-call cursor is the materialize-SRF tuplestore's concern — so only
/// the collected `result` remains. Each entry is the raw field-name bytes
/// (`pstrdup(fname)`).
#[derive(Default)]
struct OkeysState {
    result: Vec<Vec<u8>>,
}

/// Read the leading `JsonbContainer.header` word from the root container bytes.
#[inline]
fn container_header(root: &[u8]) -> u32 {
    u32::from_ne_bytes([root[0], root[1], root[2], root[3]])
}

// ===========================================================================
// jsonb_object_keys (jsonfuncs.c:567).
// ===========================================================================

/// Core of `jsonb_object_keys` (jsonfuncs.c:573-622): collect the stored-order
/// keys of a jsonb object. `jb` is the full `jsonb` varlena; the root container
/// starts after the varlena header (`&jb[VARHDRSZ..]`). Raises
/// `ERRCODE_INVALID_PARAMETER_VALUE` on a scalar or array root.
fn jsonb_object_keys_worker(jb: &[u8]) -> PgResult<Vec<Vec<u8>>> {
    let root = &jb[VARHDRSZ..];
    let header = container_header(root);

    // if (JB_ROOT_IS_SCALAR(jb)) ereport(... "cannot call %s on a scalar" ...)
    if json_container_is_scalar(header) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(alloc::format!("cannot call {} on a scalar", "jsonb_object_keys"))
            .into_error());
    }
    // else if (JB_ROOT_IS_ARRAY(jb)) ereport(... "cannot call %s on an array" ...)
    else if json_container_is_array(header) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(alloc::format!("cannot call {} on an array", "jsonb_object_keys"))
            .into_error());
    }

    // state->result_size = JB_ROOT_COUNT(jb);
    // state->result = palloc(state->result_size * sizeof(char *));
    let result_size = json_container_size(header) as usize;
    let mut result: Vec<Vec<u8>> = Vec::with_capacity(result_size);

    // it = JsonbIteratorInit(&jb->root);
    let mut it = JsonbIteratorInit(root);
    let mut v = JsonbValue::null();
    let mut skip_nested = false;

    // while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE)
    loop {
        let r = JsonbIteratorNext(&mut it, &mut v, skip_nested)?;
        if r == JsonbIteratorToken::WJB_DONE {
            break;
        }

        skip_nested = true;

        // if (r == WJB_KEY) — copy out the key bytes (the C palloc'd
        // NUL-terminated cstr that CStringGetTextDatum later consumes).
        if r == JsonbIteratorToken::WJB_KEY {
            if let JsonbValueData::String(ref s) = v.val {
                result.push(s.clone());
            }
        }
    }

    Ok(result)
}

/// `jsonb_object_keys(PG_FUNCTION_ARGS)` (jsonfuncs.c:568) — materialize-mode
/// SRF returning the set of keys of a jsonb object as `text`. Returns the SRF
/// null word `(Datum) 0`.
pub fn jsonb_object_keys<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    // InitMaterializedSRF(fcinfo, 0);
    funcapi::InitMaterializedSRF::call(fcinfo, 0)?;
    let nulls: [bool; 1] = [false];

    // Jsonb *jb = PG_GETARG_JSONB_P(0);
    let jb = funcapi::srf_arg_varlena_bytes::call(mcx, fcinfo, 0)?;

    let keys = jsonb_object_keys_worker(&jb)?;

    // For each key: SRF_RETURN_NEXT(funcctx, CStringGetTextDatum(nxt)).
    for key in &keys {
        let values: [Datum<'mcx>; 1] = [text_datum_from_bytes(mcx, key)?];
        let rsi = fcinfo
            .resultinfo
            .as_mut()
            .expect("InitMaterializedSRF set fcinfo->resultinfo");
        funcapi::materialized_srf_putvalues::call(rsi, &values, &nulls)?;
    }

    Ok(Datum::null())
}

// ===========================================================================
// okeys_* SAX callbacks (jsonfuncs.c:785-836).
// ===========================================================================

/// `okeys_object_field_start` (jsonfuncs.c:785): at the start of each object
/// field, if at the top-level object (`lex_level == 1`), save a copy of the
/// field name.
fn okeys_object_field_start(state: &mut OkeysState, lex: &JsonLexContext, fname: &[u8]) {
    // only collecting keys for the top level object
    if lex.lex_level != 1 {
        return;
    }

    // state->result[state->result_count++] = pstrdup(fname);
    // (the C result-array doubling/repalloc is Vec's own growth.)
    state.result.push(fname.to_vec());
}

/// `okeys_array_start` (jsonfuncs.c:808): the top level must be a json object,
/// so an array at depth 0 is an error.
fn okeys_array_start(lex: &JsonLexContext) -> PgResult<JsonParseErrorType> {
    // top level must be a json object
    if lex.lex_level == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(alloc::format!("cannot call {} on an array", "json_object_keys"))
            .into_error());
    }

    Ok(JsonParseErrorType::JSON_SUCCESS)
}

/// `okeys_scalar` (jsonfuncs.c:823): the top level must be a json object, so a
/// scalar at depth 0 is an error.
fn okeys_scalar(lex: &JsonLexContext) -> PgResult<JsonParseErrorType> {
    // top level must be a json object
    if lex.lex_level == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(alloc::format!("cannot call {} on a scalar", "json_object_keys"))
            .into_error());
    }

    Ok(JsonParseErrorType::JSON_SUCCESS)
}

// ===========================================================================
// json_object_keys (jsonfuncs.c:731).
// ===========================================================================

/// Core of `json_object_keys` (jsonfuncs.c:737-769): collect the document-order
/// keys of a json (text) object by parsing `json` with the `okeys_*` SAX
/// callbacks. `json` is the text payload bytes.
fn json_object_keys_worker(json: &[u8]) -> PgResult<Vec<Vec<u8>>> {
    // state = palloc(sizeof(OkeysState)); sem = palloc0(sizeof(JsonSemAction));
    let state = Rc::new(RefCell::new(OkeysState::default()));
    let mut sem = JsonSemAction::default();

    // sem->array_start = okeys_array_start;
    sem.array_start = Some(Box::new(|lex: &JsonLexContext| {
        okeys_array_start(lex).map(|_| ())
    }));
    // sem->scalar = okeys_scalar;
    sem.scalar = Some(Box::new(
        |lex: &JsonLexContext, _token: &[u8], _tokentype: JsonTokenType| {
            okeys_scalar(lex).map(|_| ())
        },
    ));
    // sem->object_field_start = okeys_object_field_start;
    {
        let state = Rc::clone(&state);
        sem.object_field_start = Some(Box::new(
            move |lex: &JsonLexContext, fname: &[u8], _isnull: bool| {
                okeys_object_field_start(&mut state.borrow_mut(), lex, fname);
                Ok(())
            },
        ));
    }

    // makeJsonLexContext(&lex, json, true); pg_parse_json_or_ereport(&lex, sem);
    let encoding = common_jsonapi_seams::get_database_encoding::call();
    let result = common_jsonapi_seams::pg_parse_json::call(json, encoding, true, &mut sem)?;
    if result != JsonParseErrorType::JSON_SUCCESS {
        // pg_parse_json_or_ereport: a parse failure raises through json_errsave_error.
        common_jsonapi_seams::errsave_error::call(result, json)?;
        unreachable!("errsave_error with no escontext raises");
    }

    // keys are now in state->result
    let keys = core::mem::take(&mut state.borrow_mut().result);
    Ok(keys)
}

/// `json_object_keys(PG_FUNCTION_ARGS)` (jsonfuncs.c:732) — materialize-mode SRF
/// returning the set of keys of a json (text) object as `text`. Returns the SRF
/// null word `(Datum) 0`.
pub fn json_object_keys<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    // InitMaterializedSRF(fcinfo, 0);
    funcapi::InitMaterializedSRF::call(fcinfo, 0)?;
    let nulls: [bool; 1] = [false];

    // text *json = PG_GETARG_TEXT_PP(0);
    let json = funcapi::srf_arg_varlena_bytes::call(mcx, fcinfo, 0)?;

    let keys = json_object_keys_worker(&json)?;

    // For each key: SRF_RETURN_NEXT(funcctx, CStringGetTextDatum(nxt)).
    for key in &keys {
        let values: [Datum<'mcx>; 1] = [text_datum_from_bytes(mcx, key)?];
        let rsi = fcinfo
            .resultinfo
            .as_mut()
            .expect("InitMaterializedSRF set fcinfo->resultinfo");
        funcapi::materialized_srf_putvalues::call(rsi, &values, &nulls)?;
    }

    Ok(Datum::null())
}

/// `CStringGetTextDatum(nxt)` (jsonfuncs.c:631/779): build a `text` Datum from a
/// key's raw bytes. C uses `cstring_to_text` over server-encoded bytes (no
/// UTF-8 validation), so the byte-faithful varlena builder is used.
fn text_datum_from_bytes<'mcx>(mcx: Mcx<'mcx>, key: &[u8]) -> PgResult<Datum<'mcx>> {
    backend_utils_adt_varlena_seams::bytes_to_varlena_v::call(mcx, key)
}
