//! `json[b]_each[_text]` set-returning functions (jsonfuncs.c:1940-2207):
//! decompose a json/jsonb object into `(key, value)` rows.
//!
//! These are 2-column materialize-mode SRFs `(key text, value json[b]-or-text)`.
//! Per the repo SRF convention (cf. `keys.rs` / `pg_available_wal_summaries`),
//! the fmgr entry points run `InitMaterializedSRF(fcinfo, MAT_SRF_BLESS)` +
//! `materialized_srf_putvalues` over the owned call frame; the document bytes
//! come from the first varlena arg through the funcapi `srf_arg_varlena_bytes`
//! seam.
//!
//! The jsonb (binary) path [`each_worker_jsonb`] walks the on-disk object
//! through the landed `jsonb_util.c` iterator API. The json (text) path
//! [`each_worker`] drives the four `each_*` SAX callbacks over the
//! `common/jsonapi.c` parser through the `common-jsonapi-seams::pg_parse_json`
//! SAX-driver seam, closing over an [`EachState`].

extern crate alloc;

use core::cell::RefCell;

use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::vec::Vec;

use ::utils_error::ereport;
use ::mcx::Mcx;
use ::types_error::error::{ERRCODE_INVALID_PARAMETER_VALUE, ERROR};
use ::types_error::PgResult;
use ::types_json::{JsonLexContext, JsonParseErrorType, JsonTokenType};
use types_jsonb::jsonb_util::{JsonbValue, JsonbValueData};
use ::types_jsonb::jsonb::{jbvType, json_container_is_object, JsonbIteratorToken};
use ::nodes::fmgr::FunctionCallInfoBaseData;
use ::nodes::funcapi::MAT_SRF_BLESS;
use ::types_tuple::Datum;

use ::jsonb_util::{JsonbIteratorInit, JsonbIteratorNext, JsonbValueToJsonb};
use funcapi_seams as funcapi;

use crate::common::JsonbValueAsText;

// ===========================================================================
// Row output model.
//
// json_each / jsonb_each emit `(key text, value json[b])`; the *_text variants
// emit `(key text, value text)`. A worker hands back the ordered rows; the SRF
// entry marshals each into the 2-column `values`/`nulls` tuplestore frame.
// ===========================================================================

/// One `(key, value)` row produced by an `each` worker. `key` is the object
/// key as text bytes; `value` is the value column.
struct EachRow {
    key: Vec<u8>,
    value: EachValue,
}

/// The `values[1]`/`nulls[1]` value column of an [`EachRow`].
enum EachValue {
    /// `nulls[1] = true` — the SQL NULL value (text mode over a json `null`).
    Null,
    /// `as_text`: the value rendered as text (a `text` column).
    Text(Vec<u8>),
    /// not text mode: the value re-serialised as a `jsonb` varlena (a `jsonb`
    /// column).
    Jsonb(Vec<u8>),
}

// ===========================================================================
// SQL entry points (jsonfuncs.c:1949-1971).
// ===========================================================================

/// `json_each` (jsonfuncs.c:1950): `each_worker(fcinfo, false)`.
pub fn json_each<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    each_worker(mcx, fcinfo, false)
}

/// `jsonb_each` (jsonfuncs.c:1956): `each_worker_jsonb(fcinfo, "jsonb_each",
/// false)`.
pub fn jsonb_each<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    each_worker_jsonb(mcx, fcinfo, "jsonb_each", false)
}

/// `json_each_text` (jsonfuncs.c:1962): `each_worker(fcinfo, true)`.
pub fn json_each_text<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    each_worker(mcx, fcinfo, true)
}

/// `jsonb_each_text` (jsonfuncs.c:1968): `each_worker_jsonb(fcinfo,
/// "jsonb_each_text", true)`.
pub fn jsonb_each_text<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    each_worker_jsonb(mcx, fcinfo, "jsonb_each_text", true)
}

// ===========================================================================
// Row -> column-Datum marshalling.
// ===========================================================================

/// Emit one collected [`EachRow`] into the SRF tuplestore as a 2-column
/// `(key text, value ...)` row (the C `tuplestore_putvalues`/`heap_form_tuple`
/// over `values[2]`/`nulls[2]`).
fn put_each_row<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    row: &EachRow,
) -> PgResult<()> {
    // values[0] = the key as a text Datum (CStringGetTextDatum / cstring_to_text*).
    let key_datum = varlena_seams::bytes_to_varlena_v::call(mcx, &row.key)?;

    // values[1] / nulls[1].
    let (val_datum, val_null) = match &row.value {
        EachValue::Null => (Datum::null(), true),
        EachValue::Text(bytes) => (
            varlena_seams::bytes_to_varlena_v::call(mcx, bytes)?,
            false,
        ),
        EachValue::Jsonb(bytes) => {
            // JsonbValueToJsonb already produced a full jsonb varlena; it is a
            // pass-by-reference value -> Datum::ByRef of those bytes.
            let mut v = ::mcx::vec_with_capacity_in::<u8>(mcx, bytes.len())?;
            v.extend_from_slice(bytes);
            (Datum::ByRef(v), false)
        }
    };

    let values: [Datum<'mcx>; 2] = [key_datum, val_datum];
    let nulls: [bool; 2] = [false, val_null];

    let rsi = fcinfo
        .resultinfo
        .as_mut()
        .expect("InitMaterializedSRF set fcinfo->resultinfo");
    funcapi::materialized_srf_putvalues::call(rsi, &values, &nulls)
}

// ===========================================================================
// each_worker_jsonb — the jsonb (binary) worker (jsonfuncs.c:1973-2054).
// ===========================================================================

/// `each_worker_jsonb` (jsonfuncs.c:1974): decompose a jsonb object into
/// `(key, value)` rows. `funcname` is reproduced in the non-object error.
fn each_worker_jsonb<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    funcname: &str,
    as_text: bool,
) -> PgResult<Datum<'mcx>> {
    // Jsonb *jb = PG_GETARG_JSONB_P(0);
    let jb = funcapi::srf_arg_varlena_bytes::call(mcx, fcinfo, 0)?;
    let root = crate::common::vardata_any(&jb);
    let header = u32::from_ne_bytes([root[0], root[1], root[2], root[3]]);

    // if (!JB_ROOT_IS_OBJECT(jb)) ereport(... "cannot call %s on a non-object" ...)
    if !json_container_is_object(header) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(alloc::format!("cannot call {} on a non-object", funcname))
            .into_error());
    }

    // rsi = (ReturnSetInfo *) fcinfo->resultinfo;
    // InitMaterializedSRF(fcinfo, MAT_SRF_BLESS);
    funcapi::InitMaterializedSRF::call(fcinfo, MAT_SRF_BLESS)?;

    // tmp_cxt = AllocSetContextCreate(...): the per-tuple temp context is an
    // allocation detail with no observable result difference.

    let mut skip_nested = false;

    // it = JsonbIteratorInit(mcx, &jb->root);
    let mut it = JsonbIteratorInit(mcx, root);
    let mut v = JsonbValue::null();

    // while ((r = JsonbIteratorNext(&it, &v, skipNested)) != WJB_DONE)
    loop {
        let r = JsonbIteratorNext(&mut it, &mut v, skip_nested)?;
        if r == JsonbIteratorToken::WJB_DONE {
            break;
        }

        skip_nested = true;

        if r == JsonbIteratorToken::WJB_KEY {
            // key = cstring_to_text_with_len(v.val.string.val, v.val.string.len);
            let key = match v.val {
                JsonbValueData::String(ref s) => s.clone(),
                _ => Vec::new(),
            };

            // The next thing the iterator fetches should be the value, no
            // matter what shape it is.
            // r = JsonbIteratorNext(&it, &v, skipNested); Assert(r != WJB_DONE);
            let r2 = JsonbIteratorNext(&mut it, &mut v, skip_nested)?;
            debug_assert!(r2 != JsonbIteratorToken::WJB_DONE);

            // values[0] = PointerGetDatum(key);
            let value = if as_text {
                if v.typ == jbvType::jbvNull {
                    // a json null is an sql null in text mode
                    // nulls[1] = true; values[1] = (Datum) NULL;
                    EachValue::Null
                } else {
                    // values[1] = PointerGetDatum(JsonbValueAsText(&v));
                    match JsonbValueAsText(mcx, &v)? {
                        Some(t) => EachValue::Text(t[..].to_vec()),
                        None => EachValue::Null,
                    }
                }
            } else {
                // Not in text mode, just return the Jsonb.
                // Jsonb *val = JsonbValueToJsonb(&v); values[1] = PointerGetDatum(val);
                let val = JsonbValueToJsonb(mcx, &v)?;
                EachValue::Jsonb(val[..].to_vec())
            };

            // tuplestore_putvalues(rsi->setResult, rsi->setDesc, values, nulls);
            let row = EachRow { key, value };
            put_each_row(mcx, fcinfo, &row)?;
        }
    }

    // MemoryContextDelete(tmp_cxt); PG_RETURN_NULL();
    Ok(Datum::null())
}

// ===========================================================================
// each_worker — the json (text) worker (jsonfuncs.c:2057-2094).
// ===========================================================================

/// `EachState` (jsonfuncs.c:109): SAX-callback state for the json (text)
/// `each_worker`.
///
/// The C struct also carries `lex` (the lexer handed to each callback here),
/// `tuple_store`/`ret_tdesc` (the SRF output, marshalled by the entry point
/// from `result`), and `tmp_cxt` (the per-tuple temp context, an allocation
/// detail). The remaining fields are the cross-callback running state.
#[derive(Default)]
struct EachState {
    /// The `(key, value)` rows produced so far, in document order.
    result: Vec<EachRow>,
    /// `const char *result_start`: offset into the lexer input where the
    /// current top-level value begins (set in `each_object_field_start`).
    result_start: usize,
    /// `bool normalize_results`: the `as_text` flag (de-escape string scalars).
    normalize_results: bool,
    /// `bool next_scalar`: the current top-level value is a string scalar whose
    /// de-escaped form must be used.
    next_scalar: bool,
    /// `char *normalized_scalar`: the de-escaped scalar token captured by
    /// `each_scalar` for the next `each_object_field_end`.
    normalized_scalar: Vec<u8>,
}

/// `each_object_field_start` (jsonfuncs.c:2098): record where each top-level
/// value starts.
fn each_object_field_start(state: &mut EachState, lex: &JsonLexContext) {
    // save a pointer to where the value starts
    if lex.lex_level == 1 {
        // next_scalar will be reset in the object_field_end handler, and since
        // we know the value is a scalar there is no danger of it being on while
        // recursing down the tree.
        if state.normalize_results && lex.token_type == JsonTokenType::JSON_TOKEN_STRING {
            state.next_scalar = true;
        } else {
            state.result_start = lex.token_start;
        }
    }
}

/// `each_object_field_end` (jsonfuncs.c:2120): emit one `(fname, value)` row.
fn each_object_field_end(state: &mut EachState, lex: &JsonLexContext, fname: &[u8], isnull: bool) {
    // skip over nested objects
    if lex.lex_level != 1 {
        return;
    }

    // use the tmp context so we can clean up after each tuple is done — an
    // allocation detail with no observable result difference.

    // values[0] = CStringGetTextDatum(fname);
    let key = fname.to_vec();

    let value = if isnull && state.normalize_results {
        // nulls[1] = true; values[1] = (Datum) 0;
        EachValue::Null
    } else if state.next_scalar {
        // values[1] = CStringGetTextDatum(_state->normalized_scalar);
        // _state->next_scalar = false;
        state.next_scalar = false;
        EachValue::Text(core::mem::take(&mut state.normalized_scalar))
    } else {
        // len = _state->lex->prev_token_terminator - _state->result_start;
        // val = cstring_to_text_with_len(_state->result_start, len);
        // values[1] = PointerGetDatum(val);
        let len = lex.prev_token_terminator - state.result_start;
        EachValue::Text(lex.input[state.result_start..state.result_start + len].to_vec())
    };

    // tuple = heap_form_tuple(_state->ret_tdesc, values, nulls);
    // tuplestore_puttuple(_state->tuple_store, tuple);
    state.result.push(EachRow { key, value });
}

/// `each_array_start` (jsonfuncs.c:2168): a top-level array is not an object.
fn each_array_start(lex: &JsonLexContext) -> PgResult<()> {
    // json structure check
    if lex.lex_level == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("cannot deconstruct an array as an object")
            .into_error());
    }
    Ok(())
}

/// `each_scalar` (jsonfuncs.c:2182): a top-level scalar is not an object;
/// supply the de-escaped value when one is required.
fn each_scalar(state: &mut EachState, lex: &JsonLexContext, token: &[u8]) -> PgResult<()> {
    // json structure check
    if lex.lex_level == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("cannot deconstruct a scalar")
            .into_error());
    }

    // supply de-escaped value if required
    if state.next_scalar {
        state.normalized_scalar = token.to_vec();
    }
    Ok(())
}

/// `each_worker` (jsonfuncs.c:2058): decompose a json (text) object into
/// `(key, value)` rows by driving the `each_*` SAX callbacks, then marshalling
/// the produced rows into the materialize-mode SRF tuplestore.
fn each_worker<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    as_text: bool,
) -> PgResult<Datum<'mcx>> {
    // text *json = PG_GETARG_TEXT_PP(0);
    // The seam yields the header-ful varlena image; the json (text) document is
    // its VARDATA (skip the 4-byte length word), as C reads via VARDATA_ANY.
    let json_image = funcapi::srf_arg_varlena_bytes::call(mcx, fcinfo, 0)?;
    let json = crate::common::vardata_any(&json_image);

    // state = palloc0(sizeof(EachState)); sem = palloc0(sizeof(JsonSemAction));
    let state = Rc::new(RefCell::new(EachState::default()));

    // rsi = (ReturnSetInfo *) fcinfo->resultinfo;
    // InitMaterializedSRF(fcinfo, MAT_SRF_BLESS);
    // state->tuple_store = rsi->setResult; state->ret_tdesc = rsi->setDesc;
    funcapi::InitMaterializedSRF::call(fcinfo, MAT_SRF_BLESS)?;

    // state->normalize_results = as_text; state->next_scalar = false;
    state.borrow_mut().normalize_results = as_text;

    let mut sem = ::types_json::JsonSemAction::default();

    // sem->array_start = each_array_start;
    sem.array_start = Some(Box::new(|lex: &JsonLexContext| each_array_start(lex)));
    // sem->scalar = each_scalar;
    {
        let state = Rc::clone(&state);
        sem.scalar = Some(Box::new(
            move |lex: &JsonLexContext, token: &[u8], _tokentype: JsonTokenType| {
                each_scalar(&mut state.borrow_mut(), lex, token)
            },
        ));
    }
    // sem->object_field_start = each_object_field_start;
    {
        let state = Rc::clone(&state);
        sem.object_field_start = Some(Box::new(
            move |lex: &JsonLexContext, _fname: &[u8], _isnull: bool| {
                each_object_field_start(&mut state.borrow_mut(), lex);
                Ok(())
            },
        ));
    }
    // sem->object_field_end = each_object_field_end;
    {
        let state = Rc::clone(&state);
        sem.object_field_end = Some(Box::new(
            move |lex: &JsonLexContext, fname: &[u8], isnull: bool| {
                each_object_field_end(&mut state.borrow_mut(), lex, fname, isnull);
                Ok(())
            },
        ));
    }

    // state->lex = makeJsonLexContext(&lex, json, /*need_escapes=*/ true);
    // pg_parse_json_or_ereport(&lex, sem);
    let encoding = jsonapi_seams::get_database_encoding::call();
    let result = jsonapi_seams::pg_parse_json::call(json, encoding, true, &mut sem)?;
    if result != JsonParseErrorType::JSON_SUCCESS {
        // pg_parse_json_or_ereport: a parse failure raises through json_errsave_error.
        jsonapi_seams::errsave_error::call(result, json, true, None)?;
        unreachable!("errsave_error with no escontext raises");
    }

    // The SAX callbacks (which borrow `state`) have run; drop them so the rows
    // can be moved out, then marshal each into the SRF tuplestore.
    drop(sem);
    let rows = core::mem::take(&mut state.borrow_mut().result);
    for row in &rows {
        put_each_row(mcx, fcinfo, row)?;
    }

    // MemoryContextDelete(state->tmp_cxt); freeJsonLexContext(&lex);
    // PG_RETURN_NULL();
    Ok(Datum::null())
}
