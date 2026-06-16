//! `json_strip_nulls` / `jsonb_strip_nulls` and their semantic actions
//! (jsonfuncs.c:283-290, 4389-4599).
//!
//! `json_strip_nulls` walks the json *text* with the SAX parser, re-emitting it
//! verbatim except for null object fields (and, optionally, null array
//! elements). `jsonb_strip_nulls` does the same over the binary jsonb tree via
//! the `jsonb_util.c` iterator/parse-state API.

use core::cell::RefCell;

use alloc::boxed::Box;
use alloc::rc::Rc;

use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_json::{JsonLexContext, JsonParseErrorType, JsonSemAction, JsonTokenType};
use types_jsonb::backend_utils_adt_jsonb_util::JsonbValue;
use types_jsonb::jsonb::{jbvType, json_container_is_scalar, JsonbIteratorToken, VARHDRSZ};

use backend_utils_adt_json::escape_json;
use backend_utils_adt_jsonb_util::{
    JsonbIteratorInit, JsonbIteratorNext, JsonbValueToJsonb, pushJsonbValue,
};

// ===========================================================================
// StripnullState + the sn_* SAX callbacks (jsonfuncs.c:283-290, 4389-4499).
// ===========================================================================

/// `StripnullState` (jsonfuncs.c:284): per-parse state for `json_strip_nulls`.
///
/// The C struct's `JsonLexContext *lex` is the parser's concern here; the
/// `StringInfo strval` output buffer becomes a shared `PgVec<u8>` charged to the
/// call's mcx arena (shared across the SAX callback closures via `Rc<RefCell>`).
struct StripnullState<'mcx> {
    /// `StringInfo strval`: the re-emitted json text built so far.
    strval: PgVec<'mcx, u8>,
    /// `bool skip_next_null`: set by a null field/element start, consumed by the
    /// very next scalar action.
    skip_next_null: bool,
    /// `bool strip_in_arrays`: whether null array elements are stripped too.
    strip_in_arrays: bool,
}

/// `sn_object_start` (jsonfuncs.c:4390): emit `{`.
fn sn_object_start(state: &mut StripnullState) -> PgResult<()> {
    // appendStringInfoCharMacro(_state->strval, '{');
    state.strval.push('{' as u8);
    Ok(())
}

/// `sn_object_end` (jsonfuncs.c:4400): emit `}`.
fn sn_object_end(state: &mut StripnullState) -> PgResult<()> {
    // appendStringInfoCharMacro(_state->strval, '}');
    state.strval.push('}' as u8);
    Ok(())
}

/// `sn_array_start` (jsonfuncs.c:4410): emit `[`.
fn sn_array_start(state: &mut StripnullState) -> PgResult<()> {
    // appendStringInfoCharMacro(_state->strval, '[');
    state.strval.push('[' as u8);
    Ok(())
}

/// `sn_array_end` (jsonfuncs.c:4420): emit `]`.
fn sn_array_end(state: &mut StripnullState) -> PgResult<()> {
    // appendStringInfoCharMacro(_state->strval, ']');
    state.strval.push(']' as u8);
    Ok(())
}

/// `sn_object_field_start` (jsonfuncs.c:4430).
fn sn_object_field_start(state: &mut StripnullState, fname: &[u8], isnull: bool) -> PgResult<()> {
    if isnull {
        // The next thing must be a scalar or isnull couldn't be true, so there
        // is no danger of this state being carried down into a nested object or
        // array. The flag will be reset in the scalar action.
        state.skip_next_null = true;
        return Ok(());
    }

    // if (_state->strval->data[_state->strval->len - 1] != '{')
    //     appendStringInfoCharMacro(_state->strval, ',');
    if state.strval[state.strval.len() - 1] != '{' as u8 {
        state.strval.push(',' as u8);
    }

    // Unfortunately we don't have the quoted and escaped string any more, so we
    // have to re-escape it.
    // escape_json(_state->strval, fname);
    escape_json(&mut state.strval, fname)?;

    // appendStringInfoCharMacro(_state->strval, ':');
    state.strval.push(':' as u8);

    Ok(())
}

/// `sn_array_element_start` (jsonfuncs.c:4460).
fn sn_array_element_start(state: &mut StripnullState, isnull: bool) -> PgResult<()> {
    // If strip_in_arrays is enabled and this is a null, mark it for skipping.
    if isnull && state.strip_in_arrays {
        state.skip_next_null = true;
        return Ok(());
    }

    // Only add a comma if this is not the first valid element.
    // if (_state->strval->len > 0 && _state->strval->data[_state->strval->len - 1] != '[')
    if state.strval.len() > 0 && state.strval[state.strval.len() - 1] != '[' as u8 {
        state.strval.push(',' as u8);
    }

    Ok(())
}

/// `sn_scalar` (jsonfuncs.c:4482).
fn sn_scalar(state: &mut StripnullState, token: &[u8], tokentype: JsonTokenType) -> PgResult<()> {
    if state.skip_next_null {
        // Assert(tokentype == JSON_TOKEN_NULL);
        debug_assert_eq!(tokentype, JsonTokenType::JSON_TOKEN_NULL);
        state.skip_next_null = false;
        return Ok(());
    }

    if tokentype == JsonTokenType::JSON_TOKEN_STRING {
        // escape_json(_state->strval, token);
        escape_json(&mut state.strval, token)?;
    } else {
        // appendStringInfoString(_state->strval, token);
        state.strval.extend_from_slice(token);
    }

    Ok(())
}

/// `json_strip_nulls(json[, strip_in_arrays]) -> json` (jsonfuncs.c:4504).
///
/// In C `strip_in_arrays` defaults to `false` and is overridden by the second
/// argument when `PG_NARGS() == 2`; that fmgr argument unwrap lives at the fmgr
/// boundary, so the resolved flag is passed in here. Returns the re-emitted json
/// text bytes (`cstring_to_text_with_len(strval->data, strval->len)`).
pub fn json_strip_nulls<'mcx>(
    mcx: Mcx<'mcx>,
    json: &[u8],
    strip_in_arrays: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    // state = palloc0(sizeof(StripnullState));
    // state->lex = makeJsonLexContext(&lex, json, true);
    // state->strval = makeStringInfo();
    // state->skip_next_null = false;
    // state->strip_in_arrays = strip_in_arrays;
    let state = Rc::new(RefCell::new(StripnullState {
        strval: PgVec::new_in(mcx),
        skip_next_null: false,
        strip_in_arrays,
    }));

    // sem = palloc0(sizeof(JsonSemAction));
    let mut sem = JsonSemAction::default();

    // sem->object_start = sn_object_start;
    {
        let state = Rc::clone(&state);
        sem.object_start = Some(Box::new(move |_lex: &JsonLexContext| {
            sn_object_start(&mut state.borrow_mut())
        }));
    }
    // sem->object_end = sn_object_end;
    {
        let state = Rc::clone(&state);
        sem.object_end = Some(Box::new(move |_lex: &JsonLexContext| {
            sn_object_end(&mut state.borrow_mut())
        }));
    }
    // sem->array_start = sn_array_start;
    {
        let state = Rc::clone(&state);
        sem.array_start = Some(Box::new(move |_lex: &JsonLexContext| {
            sn_array_start(&mut state.borrow_mut())
        }));
    }
    // sem->array_end = sn_array_end;
    {
        let state = Rc::clone(&state);
        sem.array_end = Some(Box::new(move |_lex: &JsonLexContext| {
            sn_array_end(&mut state.borrow_mut())
        }));
    }
    // sem->scalar = sn_scalar;
    {
        let state = Rc::clone(&state);
        sem.scalar = Some(Box::new(
            move |_lex: &JsonLexContext, token: &[u8], tokentype: JsonTokenType| {
                sn_scalar(&mut state.borrow_mut(), token, tokentype)
            },
        ));
    }
    // sem->array_element_start = sn_array_element_start;
    {
        let state = Rc::clone(&state);
        sem.array_element_start = Some(Box::new(move |_lex: &JsonLexContext, isnull: bool| {
            sn_array_element_start(&mut state.borrow_mut(), isnull)
        }));
    }
    // sem->object_field_start = sn_object_field_start;
    {
        let state = Rc::clone(&state);
        sem.object_field_start = Some(Box::new(
            move |_lex: &JsonLexContext, fname: &[u8], isnull: bool| {
                sn_object_field_start(&mut state.borrow_mut(), fname, isnull)
            },
        ));
    }

    // pg_parse_json_or_ereport(&lex, sem);
    // makeJsonLexContext(&lex, json, true): need_escapes = true (strip needs the
    // raw token text).
    let encoding = common_jsonapi_seams::get_database_encoding::call();
    let result = common_jsonapi_seams::pg_parse_json::call(json, encoding, true, &mut sem)?;
    if result != JsonParseErrorType::JSON_SUCCESS {
        // pg_parse_json_or_ereport: a parse failure raises through json_errsave_error.
        common_jsonapi_seams::errsave_error::call(result, json)?;
        unreachable!("errsave_error with no escontext raises");
    }

    // drop the callback closures so the only Rc strong ref is ours.
    drop(sem);

    // PG_RETURN_TEXT_P(cstring_to_text_with_len(state->strval->data, state->strval->len));
    let state = Rc::try_unwrap(state)
        .map(RefCell::into_inner)
        .unwrap_or_else(|rc| rc.borrow().clone_strval_panic());
    Ok(state.strval)
}

impl<'mcx> StripnullState<'mcx> {
    /// Unreachable fallback: after the parse driver returns and `sem` is dropped,
    /// no other `Rc` strong ref to the state can survive, so `Rc::try_unwrap`
    /// always succeeds.
    fn clone_strval_panic(&self) -> Self {
        unreachable!("StripnullState still shared after pg_parse_json returned");
    }
}

// ===========================================================================
// jsonb_strip_nulls — the jsonb (binary) entry point (jsonfuncs.c:4540).
// ===========================================================================

/// `jsonb_strip_nulls(jsonb[, strip_in_arrays]) -> jsonb` (jsonfuncs.c:4540).
///
/// In C `strip_in_arrays` defaults to `false` and is overridden by the second
/// argument when `PG_NARGS() == 2`; the fmgr argument unwrap lives at the fmgr
/// boundary, so the resolved flag is passed in here. `jb` is the full jsonb
/// varlena; the result is the full result varlena.
pub fn jsonb_strip_nulls<'mcx>(
    mcx: Mcx<'mcx>,
    jb: &[u8],
    strip_in_arrays: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    let root = &jb[VARHDRSZ..];
    let header = u32::from_ne_bytes([root[0], root[1], root[2], root[3]]);

    // if (JB_ROOT_IS_SCALAR(jb)) PG_RETURN_POINTER(jb);
    if json_container_is_scalar(header) {
        let mut out = PgVec::new_in(mcx);
        out.extend_from_slice(jb);
        return Ok(out);
    }

    // it = JsonbIteratorInit(&jb->root);
    let mut it = JsonbIteratorInit(root);

    // JsonbParseState *parseState = NULL;
    let mut parse_state = None;
    // JsonbValue *res = NULL;
    let mut res: Option<JsonbValue> = None;
    // JsonbValue v, k;
    let mut v = JsonbValue::null();
    // JsonbValue k;  (stashed pending key)
    let mut k = JsonbValue::null();
    // bool last_was_key = false;
    let mut last_was_key = false;

    // while ((type = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
    loop {
        let typ = JsonbIteratorNext(&mut it, &mut v, false)?;
        if typ == JsonbIteratorToken::WJB_DONE {
            break;
        }

        // Assert(!(type == WJB_KEY && last_was_key));
        debug_assert!(!(typ == JsonbIteratorToken::WJB_KEY && last_was_key));

        if typ == JsonbIteratorToken::WJB_KEY {
            // stash the key until we know if it has a null value
            k = v.clone();
            last_was_key = true;
            continue;
        }

        if last_was_key {
            // if the last element was a key this one can't be
            last_was_key = false;

            // skip this field if value is null
            // if (type == WJB_VALUE && v.type == jbvNull) continue;
            if typ == JsonbIteratorToken::WJB_VALUE && v.typ == jbvType::jbvNull {
                continue;
            }

            // otherwise, do a delayed push of the key
            // (void) pushJsonbValue(&parseState, WJB_KEY, &k);
            pushJsonbValue(&mut parse_state, JsonbIteratorToken::WJB_KEY, Some(&k))?;
        }

        // if strip_in_arrays is set, also skip null array elements
        // if (strip_in_arrays) if (type == WJB_ELEM && v.type == jbvNull) continue;
        if strip_in_arrays && typ == JsonbIteratorToken::WJB_ELEM && v.typ == jbvType::jbvNull {
            continue;
        }

        // if (type == WJB_VALUE || type == WJB_ELEM)
        //     res = pushJsonbValue(&parseState, type, &v);
        // else
        //     res = pushJsonbValue(&parseState, type, NULL);
        res = if typ == JsonbIteratorToken::WJB_VALUE || typ == JsonbIteratorToken::WJB_ELEM {
            pushJsonbValue(&mut parse_state, typ, Some(&v))?
        } else {
            pushJsonbValue(&mut parse_state, typ, None)?
        };
    }

    // Assert(res != NULL);
    let res = res.expect("jsonb_strip_nulls: pushJsonbValue produced no result");

    // PG_RETURN_POINTER(JsonbValueToJsonb(res));
    JsonbValueToJsonb(mcx, &res)
}
