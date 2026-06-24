//! GIN value iteration + string-value transform (jsonfuncs.c:5663-5969).
//!
//! Ports `iterate_jsonb_values` / `iterate_json_values` (and their
//! `iterate_values_*` SAX callbacks), and `transform_jsonb_string_values` /
//! `transform_json_string_values` (and the `transform_string_values_*` SAX
//! callbacks).
//!
//! The C callbacks take a `void *state` plus a `JsonIterateStringValuesAction`
//! / `JsonTransformStringValuesAction` function pointer. Here the per-value
//! action is modelled as a Rust closure (`&mut dyn FnMut`) supplied by the
//! caller — the C `void *state` is folded into the closure's captures, exactly
//! as the rest of this crate's SAX code does. The text-path SAX callbacks close
//! over the working state through `Rc<RefCell<_>>` (matching `strip.rs` /
//! `length.rs`), and `pg_parse_json` is driven through the
//! `common-jsonapi-seams` seam.
//!
//! ## Action mapping (C -> repo)
//!
//! * `JsonIterateStringValuesAction action(void *state, char *val, int len)`
//!   becomes `action: &mut dyn FnMut(&[u8]) -> PgResult<()>`: the element value
//!   bytes (`val[..len]`) are handed to the closure.
//! * `JsonTransformStringValuesAction transform_action(void *state, char *val,
//!   int len) -> text *` becomes `transform_action: &mut dyn FnMut(Mcx, &[u8])
//!   -> PgResult<PgVec<u8>>`: the closure receives the string value bytes and
//!   returns the replacement `text` bytes.

use core::cell::RefCell;

use alloc::boxed::Box;
use alloc::rc::Rc;

use ::mcx::{Mcx, PgVec};
use ::types_error::PgResult;

use ::types_json::{JsonLexContext, JsonParseErrorType, JsonSemAction, JsonTokenType};
use types_jsonb::jsonb_util::{JsonbValue, JsonbValueData};
use ::types_jsonb::jsonb::{jbvType, JsonbIteratorToken};

use ::adt_json::{escape_json, escape_json_with_len};
use ::jsonb_util::{
    JsonbIteratorInit, JsonbIteratorNext, JsonbValueToJsonb, pushJsonbValue,
};
use ::adt_numeric::io::numeric_out;

use crate::common::{jtiBool, jtiKey, jtiNumeric, jtiString};

// ===========================================================================
// iterate_jsonb_values — the jsonb (binary) GIN value iterator
// (jsonfuncs.c:5663-5725).
// ===========================================================================

/// `iterate_jsonb_values` (jsonfuncs.c:5663): iterate over jsonb values or
/// elements, selected by `flags`, and pass each (together with the iteration
/// state captured by `action`) to the `JsonIterateStringValuesAction`.
///
/// `jb` is the full jsonb varlena; the root container starts at `VARHDRSZ`.
pub fn iterate_jsonb_values<'mcx>(
    mcx: Mcx<'mcx>,
    jb: &[u8],
    flags: u32,
    action: &mut dyn FnMut(&[u8]) -> PgResult<()>,
) -> PgResult<()> {
    // it = JsonbIteratorInit(mcx, &jb->root);
    let mut it = JsonbIteratorInit(mcx, crate::common::vardata_any(jb));

    // Just recursively iterating over jsonb and call callback on all
    // corresponding elements.
    let mut v = JsonbValue::null();
    // while ((type = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
    loop {
        let typ = JsonbIteratorNext(&mut it, &mut v, false)?;
        if typ == JsonbIteratorToken::WJB_DONE {
            break;
        }

        if typ == JsonbIteratorToken::WJB_KEY {
            // if (flags & jtiKey) action(state, v.val.string.val, v.val.string.len);
            if flags & jtiKey != 0 {
                if let JsonbValueData::String(s) = &v.val {
                    action(s)?;
                }
            }
            continue;
        } else if !(typ == JsonbIteratorToken::WJB_VALUE || typ == JsonbIteratorToken::WJB_ELEM) {
            // do not call callback for composite JsonbValue
            continue;
        }

        // JsonbValue is a value of object or element of array
        match v.typ {
            jbvType::jbvString => {
                // if (flags & jtiString) action(state, v.val.string.val, v.val.string.len);
                if flags & jtiString != 0 {
                    if let JsonbValueData::String(s) = &v.val {
                        action(s)?;
                    }
                }
            }
            jbvType::jbvNumeric => {
                if flags & jtiNumeric != 0 {
                    if let JsonbValueData::Numeric(num) = &v.val {
                        // val = DatumGetCString(DirectFunctionCall1(numeric_out,
                        //     NumericGetDatum(v.val.numeric)));
                        // action(state, val, strlen(val)); pfree(val);
                        let val = numeric_out(mcx, num)?;
                        action(val.as_bytes())?;
                    }
                }
            }
            jbvType::jbvBool => {
                if flags & jtiBool != 0 {
                    if let JsonbValueData::Bool(b) = &v.val {
                        // if (v.val.boolean) action(state, "true", 4);
                        // else action(state, "false", 5);
                        if *b {
                            action(b"true")?;
                        } else {
                            action(b"false")?;
                        }
                    }
                }
            }
            _ => {
                // do not call callback for composite JsonbValue
            }
        }
    }

    Ok(())
}

// ===========================================================================
// iterate_json_values — the json (text) GIN value iterator
// (jsonfuncs.c:5731-5797).
// ===========================================================================

/// `IterateJsonStringValuesState` (jsonfuncs.c:65): SAX-callback state for
/// `iterate_json_values`.
///
/// The C struct also carries `lex` (owned by the parser here), and the
/// `action`/`action_state` pair (the `action` closure is threaded into each
/// callback explicitly), so the only cross-callback state that remains is the
/// selection `flags`.
struct IterateJsonStringValuesState {
    /// `uint32 flags`: which kinds of json values to iterate.
    flags: u32,
}

/// `iterate_json_values` (jsonfuncs.c:5731): iterate over json (text) values
/// and elements, selected by `flags`, passing each to `action`.
pub fn iterate_json_values<'mcx>(
    json: &[u8],
    flags: u32,
    action: &mut dyn FnMut(&[u8]) -> PgResult<()>,
) -> PgResult<()> {
    // state = palloc0(...); state->flags = flags; (lex/action threaded below)
    let state = Rc::new(RefCell::new(IterateJsonStringValuesState { flags }));
    // The action closure is shared across the callbacks via Rc<RefCell<_>>.
    let action: Rc<RefCell<&mut dyn FnMut(&[u8]) -> PgResult<()>>> =
        Rc::new(RefCell::new(action));

    // sem = palloc0(sizeof(JsonSemAction));
    let mut sem = JsonSemAction::default();

    // sem->scalar = iterate_values_scalar;
    {
        let state = Rc::clone(&state);
        let action = Rc::clone(&action);
        sem.scalar = Some(Box::new(
            move |_lex: &JsonLexContext, token: &[u8], tokentype: JsonTokenType| {
                iterate_values_scalar(
                    &state.borrow(),
                    &mut **action.borrow_mut(),
                    token,
                    tokentype,
                )
                .map(|_| ())
            },
        ));
    }
    // sem->object_field_start = iterate_values_object_field_start;
    {
        let state = Rc::clone(&state);
        let action = Rc::clone(&action);
        sem.object_field_start = Some(Box::new(
            move |_lex: &JsonLexContext, fname: &[u8], isnull: bool| {
                iterate_values_object_field_start(
                    &state.borrow(),
                    &mut **action.borrow_mut(),
                    fname,
                    isnull,
                )
                .map(|_| ())
            },
        ));
    }

    // makeJsonLexContext(&lex, json, true): need_escapes = true.
    // pg_parse_json_or_ereport(&lex, sem);
    let encoding = jsonapi_seams::get_database_encoding::call();
    let result = jsonapi_seams::pg_parse_json::call(json, encoding, true, &mut sem)?;
    if result != JsonParseErrorType::JSON_SUCCESS {
        // pg_parse_json_or_ereport: a parse failure raises through json_errsave_error.
        jsonapi_seams::errsave_error::call(result, json, true, None)?;
        unreachable!("errsave_error with no escontext raises");
    }

    drop(sem);
    Ok(())
}

/// `iterate_values_scalar` (jsonfuncs.c:5757): invoke `action` for scalar
/// values of the selected kinds.
fn iterate_values_scalar(
    state: &IterateJsonStringValuesState,
    action: &mut dyn FnMut(&[u8]) -> PgResult<()>,
    token: &[u8],
    tokentype: JsonTokenType,
) -> PgResult<JsonParseErrorType> {
    match tokentype {
        JsonTokenType::JSON_TOKEN_STRING => {
            if state.flags & jtiString != 0 {
                // _state->action(_state->action_state, token, strlen(token));
                action(token)?;
            }
        }
        JsonTokenType::JSON_TOKEN_NUMBER => {
            if state.flags & jtiNumeric != 0 {
                action(token)?;
            }
        }
        JsonTokenType::JSON_TOKEN_TRUE | JsonTokenType::JSON_TOKEN_FALSE => {
            if state.flags & jtiBool != 0 {
                action(token)?;
            }
        }
        _ => {
            // do not call callback for any other token
        }
    }

    Ok(JsonParseErrorType::JSON_SUCCESS)
}

/// `iterate_values_object_field_start` (jsonfuncs.c:5785): invoke `action` for
/// object keys when `jtiKey` is set.
fn iterate_values_object_field_start(
    state: &IterateJsonStringValuesState,
    action: &mut dyn FnMut(&[u8]) -> PgResult<()>,
    fname: &[u8],
    _isnull: bool,
) -> PgResult<JsonParseErrorType> {
    if state.flags & jtiKey != 0 {
        // char *val = pstrdup(fname); _state->action(_state->action_state, val, strlen(val));
        action(fname)?;
    }

    Ok(JsonParseErrorType::JSON_SUCCESS)
}

// ===========================================================================
// transform_jsonb_string_values — the jsonb (binary) string-value transform
// (jsonfuncs.c:5805-5843).
// ===========================================================================

/// `r < WJB_BEGIN_ARRAY` (jsonb.h token ordering): true for WJB_KEY /
/// WJB_VALUE / WJB_ELEM, the tokens that carry a `JsonbValue` payload. Mirrors
/// the C `type < WJB_BEGIN_ARRAY ? &v : NULL` discriminant comparison.
#[inline]
fn token_lt_begin_array(typ: JsonbIteratorToken) -> bool {
    matches!(
        typ,
        JsonbIteratorToken::WJB_KEY
            | JsonbIteratorToken::WJB_VALUE
            | JsonbIteratorToken::WJB_ELEM
    )
}

/// `transform_jsonb_string_values` (jsonfuncs.c:5805): iterate over `jsonb` and
/// apply `transform_action` to every string value or element, returning a copy
/// of the original jsonb (the full result varlena) with the transformed values.
///
/// `jsonb` is the full jsonb varlena; the root container starts at `VARHDRSZ`.
pub fn transform_jsonb_string_values<'mcx>(
    mcx: Mcx<'mcx>,
    jsonb: &'mcx [u8],
    transform_action: &mut dyn FnMut(Mcx<'mcx>, &[u8]) -> PgResult<PgVec<'mcx, u8>>,
) -> PgResult<PgVec<'mcx, u8>> {
    // JsonbValue v, *res = NULL; JsonbParseState *st = NULL; bool is_scalar;
    let mut res: Option<JsonbValue<'mcx>> = None;
    let mut st: Option<alloc::boxed::Box<types_jsonb::jsonb_util::JsonbParseState<'mcx>>> = None;

    // it = JsonbIteratorInit(mcx, &jsonb->root);
    let mut it = JsonbIteratorInit(mcx, crate::common::vardata_any(jsonb));
    // is_scalar = it->isScalar;
    let is_scalar = it.as_ref().map(|i| i.is_scalar).unwrap_or(false);

    let mut v = JsonbValue::null();
    // while ((type = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
    loop {
        let typ = JsonbIteratorNext(&mut it, &mut v, false)?;
        if typ == JsonbIteratorToken::WJB_DONE {
            break;
        }

        if (typ == JsonbIteratorToken::WJB_VALUE || typ == JsonbIteratorToken::WJB_ELEM)
            && v.typ == jbvType::jbvString
        {
            // out = transform_action(action_state, v.val.string.val, v.val.string.len);
            let out = match &v.val {
                JsonbValueData::String(s) => transform_action(mcx, s)?,
                _ => unreachable!("WJB_VALUE/WJB_ELEM jbvString carried a non-string payload"),
            };
            // out is probably not toasted, but let's be sure:
            //   out = pg_detoast_datum_packed(out);
            //   v.val.string.val = VARDATA_ANY(out);
            //   v.val.string.len = VARSIZE_ANY_EXHDR(out);
            // Detoasting an owned byte buffer is a no-op; VARDATA_ANY /
            // VARSIZE_ANY_EXHDR are just the bytes and their length.
            v.val = JsonbValueData::String(::mcx::slice_borrow_in(mcx, &out)?);
            // res = pushJsonbValue(&st, type, type < WJB_BEGIN_ARRAY ? &v : NULL);
            // (here type is WJB_VALUE or WJB_ELEM, both < WJB_BEGIN_ARRAY)
            res = pushJsonbValue(
                mcx,
                &mut st,
                typ,
                if token_lt_begin_array(typ) {
                    Some(&v)
                } else {
                    None
                },
            )?;
        } else {
            // res = pushJsonbValue(&st, type, (type == WJB_KEY ||
            //     type == WJB_VALUE || type == WJB_ELEM) ? &v : NULL);
            let pass = matches!(
                typ,
                JsonbIteratorToken::WJB_KEY
                    | JsonbIteratorToken::WJB_VALUE
                    | JsonbIteratorToken::WJB_ELEM
            );
            res = pushJsonbValue(mcx, &mut st, typ, if pass { Some(&v) } else { None })?;
        }
    }

    // if (res->type == jbvArray) res->val.array.rawScalar = is_scalar;
    let mut res = res.expect("transform_jsonb_string_values: pushJsonbValue produced no result");
    if res.typ == jbvType::jbvArray {
        if let JsonbValueData::Array { raw_scalar, .. } = &mut res.val {
            *raw_scalar = is_scalar;
        }
    }

    // return JsonbValueToJsonb(res);
    JsonbValueToJsonb(mcx, &res)
}

// ===========================================================================
// transform_json_string_values — the json (text) string-value transform
// (jsonfuncs.c:5852-5969).
// ===========================================================================

/// `TransformJsonStringValuesState` (jsonfuncs.c:76): SAX-callback state for
/// `transform_json_string_values`.
///
/// The C struct also carries `lex` (owned by the parser here) and the
/// `action`/`action_state` pair (threaded into the callbacks explicitly). The
/// resulting json text accumulates in `strval` (the C `StringInfo`), here a
/// `PgVec<u8>` charged to the call's mcx arena.
struct TransformJsonStringValuesState<'mcx> {
    /// `StringInfo strval`: the resulting json being built.
    strval: PgVec<'mcx, u8>,
}

/// `transform_json_string_values` (jsonfuncs.c:5852): iterate over json (text)
/// and apply `transform_action` to every string value or element, returning a
/// copy of the original json (its text bytes) with the transformed values.
pub fn transform_json_string_values<'mcx>(
    mcx: Mcx<'mcx>,
    json: &[u8],
    transform_action: &mut dyn FnMut(Mcx<'mcx>, &[u8]) -> PgResult<PgVec<'mcx, u8>>,
) -> PgResult<PgVec<'mcx, u8>> {
    // state = palloc0(...); state->strval = makeStringInfo();
    let state = Rc::new(RefCell::new(TransformJsonStringValuesState {
        strval: PgVec::new_in(mcx),
    }));
    // The transform action is shared across the callbacks via Rc<RefCell<_>>.
    let action: Rc<RefCell<&mut dyn FnMut(Mcx<'mcx>, &[u8]) -> PgResult<PgVec<'mcx, u8>>>> =
        Rc::new(RefCell::new(transform_action));

    // sem = palloc0(sizeof(JsonSemAction));
    let mut sem = JsonSemAction::default();

    // sem->object_start = transform_string_values_object_start;
    {
        let state = Rc::clone(&state);
        sem.object_start = Some(Box::new(move |_lex: &JsonLexContext| {
            transform_string_values_object_start(&mut state.borrow_mut()).map(|_| ())
        }));
    }
    // sem->object_end = transform_string_values_object_end;
    {
        let state = Rc::clone(&state);
        sem.object_end = Some(Box::new(move |_lex: &JsonLexContext| {
            transform_string_values_object_end(&mut state.borrow_mut()).map(|_| ())
        }));
    }
    // sem->array_start = transform_string_values_array_start;
    {
        let state = Rc::clone(&state);
        sem.array_start = Some(Box::new(move |_lex: &JsonLexContext| {
            transform_string_values_array_start(&mut state.borrow_mut()).map(|_| ())
        }));
    }
    // sem->array_end = transform_string_values_array_end;
    {
        let state = Rc::clone(&state);
        sem.array_end = Some(Box::new(move |_lex: &JsonLexContext| {
            transform_string_values_array_end(&mut state.borrow_mut()).map(|_| ())
        }));
    }
    // sem->scalar = transform_string_values_scalar;
    {
        let state = Rc::clone(&state);
        let action = Rc::clone(&action);
        sem.scalar = Some(Box::new(
            move |_lex: &JsonLexContext, token: &[u8], tokentype: JsonTokenType| {
                transform_string_values_scalar(
                    mcx,
                    &mut state.borrow_mut(),
                    &mut **action.borrow_mut(),
                    token,
                    tokentype,
                )
                .map(|_| ())
            },
        ));
    }
    // sem->array_element_start = transform_string_values_array_element_start;
    {
        let state = Rc::clone(&state);
        sem.array_element_start = Some(Box::new(move |_lex: &JsonLexContext, isnull: bool| {
            transform_string_values_array_element_start(&mut state.borrow_mut(), isnull)
                .map(|_| ())
        }));
    }
    // sem->object_field_start = transform_string_values_object_field_start;
    {
        let state = Rc::clone(&state);
        sem.object_field_start = Some(Box::new(
            move |_lex: &JsonLexContext, fname: &[u8], isnull: bool| {
                transform_string_values_object_field_start(&mut state.borrow_mut(), fname, isnull)
                    .map(|_| ())
            },
        ));
    }

    // makeJsonLexContext(&lex, json, true): need_escapes = true.
    // pg_parse_json_or_ereport(&lex, sem);
    let encoding = jsonapi_seams::get_database_encoding::call();
    let result = jsonapi_seams::pg_parse_json::call(json, encoding, true, &mut sem)?;
    if result != JsonParseErrorType::JSON_SUCCESS {
        jsonapi_seams::errsave_error::call(result, json, true, None)?;
        unreachable!("errsave_error with no escontext raises");
    }

    drop(sem);
    drop(action);

    // return cstring_to_text_with_len(state->strval->data, state->strval->len);
    let state = Rc::try_unwrap(state)
        .map(RefCell::into_inner)
        .unwrap_or_else(|_| unreachable!("state still shared after pg_parse_json returned"));
    Ok(state.strval)
}

/// `transform_string_values_object_start` (jsonfuncs.c:5886).
fn transform_string_values_object_start(
    state: &mut TransformJsonStringValuesState,
) -> PgResult<JsonParseErrorType> {
    // appendStringInfoCharMacro(_state->strval, '{');
    state.strval.push(b'{');
    Ok(JsonParseErrorType::JSON_SUCCESS)
}

/// `transform_string_values_object_end` (jsonfuncs.c:5896).
fn transform_string_values_object_end(
    state: &mut TransformJsonStringValuesState,
) -> PgResult<JsonParseErrorType> {
    // appendStringInfoCharMacro(_state->strval, '}');
    state.strval.push(b'}');
    Ok(JsonParseErrorType::JSON_SUCCESS)
}

/// `transform_string_values_array_start` (jsonfuncs.c:5906).
fn transform_string_values_array_start(
    state: &mut TransformJsonStringValuesState,
) -> PgResult<JsonParseErrorType> {
    // appendStringInfoCharMacro(_state->strval, '[');
    state.strval.push(b'[');
    Ok(JsonParseErrorType::JSON_SUCCESS)
}

/// `transform_string_values_array_end` (jsonfuncs.c:5916).
fn transform_string_values_array_end(
    state: &mut TransformJsonStringValuesState,
) -> PgResult<JsonParseErrorType> {
    // appendStringInfoCharMacro(_state->strval, ']');
    state.strval.push(b']');
    Ok(JsonParseErrorType::JSON_SUCCESS)
}

/// `transform_string_values_object_field_start` (jsonfuncs.c:5926).
fn transform_string_values_object_field_start(
    state: &mut TransformJsonStringValuesState,
    fname: &[u8],
    _isnull: bool,
) -> PgResult<JsonParseErrorType> {
    // if (_state->strval->data[_state->strval->len - 1] != '{')
    //     appendStringInfoCharMacro(_state->strval, ',');
    if state.strval[state.strval.len() - 1] != b'{' {
        state.strval.push(b',');
    }

    // Unfortunately we don't have the quoted and escaped string any more, so we
    // have to re-escape it.
    // escape_json(_state->strval, fname);
    escape_json(&mut state.strval, fname)?;
    // appendStringInfoCharMacro(_state->strval, ':');
    state.strval.push(b':');

    Ok(JsonParseErrorType::JSON_SUCCESS)
}

/// `transform_string_values_array_element_start` (jsonfuncs.c:5944).
fn transform_string_values_array_element_start(
    state: &mut TransformJsonStringValuesState,
    _isnull: bool,
) -> PgResult<JsonParseErrorType> {
    // if (_state->strval->data[_state->strval->len - 1] != '[')
    //     appendStringInfoCharMacro(_state->strval, ',');
    if state.strval[state.strval.len() - 1] != b'[' {
        state.strval.push(b',');
    }
    Ok(JsonParseErrorType::JSON_SUCCESS)
}

/// `transform_string_values_scalar` (jsonfuncs.c:5955).
fn transform_string_values_scalar<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut TransformJsonStringValuesState<'mcx>,
    action: &mut dyn FnMut(Mcx<'mcx>, &[u8]) -> PgResult<PgVec<'mcx, u8>>,
    token: &[u8],
    tokentype: JsonTokenType,
) -> PgResult<JsonParseErrorType> {
    if tokentype == JsonTokenType::JSON_TOKEN_STRING {
        // text *out = _state->action(_state->action_state, token, strlen(token));
        let out = action(mcx, token)?;
        // escape_json_text(_state->strval, out): detoast (a no-op for owned
        // bytes) then escape over the value's bytes.
        escape_json_with_len(&mut state.strval, &out)?;
    } else {
        // appendStringInfoString(_state->strval, token);
        state.strval.extend_from_slice(token);
    }

    Ok(JsonParseErrorType::JSON_SUCCESS)
}
