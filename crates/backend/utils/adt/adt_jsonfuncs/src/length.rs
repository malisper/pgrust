//! Array length (jsonfuncs.c:1851-1937): `json_array_length` /
//! `jsonb_array_length` plus the `json_array_length` SAX callbacks
//! `alen_object_start` / `alen_scalar` / `alen_array_element_start`.
//!
//! The json (text) entry point drives the `common/jsonapi.c` recursive-descent
//! parser through the `common-jsonapi-seams::pg_parse_json` SAX-driver seam over
//! a real [`JsonSemAction`] callback table closing over an [`AlenState`]. The
//! jsonb (binary) entry point reads the root container header directly.

use core::cell::RefCell;

use ::utils_error::ereport;
use ::types_error::error::{ERRCODE_INVALID_PARAMETER_VALUE, ERROR};
use ::types_error::PgResult;

use ::types_json::{JsonLexContext, JsonParseErrorType, JsonSemAction};
use ::types_jsonb::jsonb::{json_container_is_array, json_container_is_scalar, json_container_size};

use alloc::boxed::Box;
use alloc::rc::Rc;

// ===========================================================================
// AlenState (jsonfuncs.c:102) — state for json_array_length.
//
// C holds `JsonLexContext *lex` + `int count`; the callbacks read
// `lex->lex_level` off that back-pointer. Here the parse driver hands the live
// `&JsonLexContext` to each callback, so the shared state carries only `count`.
// ===========================================================================

#[derive(Default)]
struct AlenState {
    count: i32,
}

// ===========================================================================
// JB_ROOT_* helpers (jsonb.h macros over the root JsonbContainer header word).
//
// `jb` is the root container bytes (the header word begins at offset 0), which
// is exactly what `JB_ROOT_*` reads from `&jb->root`; the fmgr boundary strips
// the varlena header, as the other jsonb entry points do.
// ===========================================================================

/// Read the leading `JsonbContainer.header` word from the root bytes.
#[inline]
fn container_header(root: &[u8]) -> u32 {
    u32::from_ne_bytes([root[0], root[1], root[2], root[3]])
}

// ===========================================================================
// jsonb_array_length (jsonfuncs.c:1877).
// ===========================================================================

/// `jsonb_array_length(jsonb) -> int` (jsonfuncs.c:1878). `jb` is the full
/// `jsonb` varlena; the root container starts after the varlena header (the
/// `&jsonb[VARHDRSZ..]` convention the other jsonb entry points use).
pub fn jsonb_array_length(jb: &[u8]) -> PgResult<i32> {
    let header = container_header(crate::common::vardata_any(jb));

    // JB_ROOT_IS_SCALAR(jb)
    if json_container_is_scalar(header) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("cannot get array length of a scalar")
            .into_error());
    }
    // !JB_ROOT_IS_ARRAY(jb)
    else if !json_container_is_array(header) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("cannot get array length of a non-array")
            .into_error());
    }

    // PG_RETURN_INT32(JB_ROOT_COUNT(jb))
    Ok(json_container_size(header) as i32)
}

// ===========================================================================
// alen_* SAX callbacks (jsonfuncs.c:1899-1937).
//
// These next two checks ensure that the json is an array (since it can't be
// a scalar or an object).
// ===========================================================================

/// `alen_object_start` (jsonfuncs.c:1899).
fn alen_object_start(lex: &JsonLexContext) -> PgResult<JsonParseErrorType> {
    // json structure check
    if lex.lex_level == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("cannot get array length of a non-array")
            .into_error());
    }

    Ok(JsonParseErrorType::JSON_SUCCESS)
}

/// `alen_scalar` (jsonfuncs.c:1913).
fn alen_scalar(lex: &JsonLexContext) -> PgResult<JsonParseErrorType> {
    // json structure check
    if lex.lex_level == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("cannot get array length of a scalar")
            .into_error());
    }

    Ok(JsonParseErrorType::JSON_SUCCESS)
}

/// `alen_array_element_start` (jsonfuncs.c:1927).
fn alen_array_element_start(state: &mut AlenState, lex: &JsonLexContext) {
    // just count up all the level 1 elements
    if lex.lex_level == 1 {
        state.count += 1;
    }
}

// ===========================================================================
// json_array_length (jsonfuncs.c:1851).
// ===========================================================================

/// `json_array_length(json) -> int` (jsonfuncs.c:1851). `json` is the text
/// payload bytes (VARDATA already stripped by the fmgr boundary).
pub fn json_array_length(json: &[u8]) -> PgResult<i32> {
    // state = palloc0(sizeof(AlenState));  (count = 0)
    let state = Rc::new(RefCell::new(AlenState::default()));

    // sem = palloc0(sizeof(JsonSemAction));
    // sem->object_start = alen_object_start;
    // sem->scalar = alen_scalar;
    // sem->array_element_start = alen_array_element_start;
    let mut sem = JsonSemAction::default();
    sem.object_start = Some(Box::new(|lex: &JsonLexContext| {
        alen_object_start(lex).map(|_| ())
    }));
    sem.scalar = Some(Box::new(
        |lex: &JsonLexContext, _token: &[u8], _tokentype| alen_scalar(lex).map(|_| ()),
    ));
    {
        let state = Rc::clone(&state);
        sem.array_element_start = Some(Box::new(move |lex: &JsonLexContext, _isnull: bool| {
            alen_array_element_start(&mut state.borrow_mut(), lex);
            Ok(())
        }));
    }

    // makeJsonLexContext(&lex, json, false); pg_parse_json_or_ereport(lex, sem);
    let encoding = jsonapi_seams::get_database_encoding::call();
    let result = jsonapi_seams::pg_parse_json::call(json, encoding, false, &mut sem)?;
    if result != JsonParseErrorType::JSON_SUCCESS {
        // pg_parse_json_or_ereport: a parse failure raises through json_errsave_error.
        jsonapi_seams::errsave_error::call(result, json, false, None)?;
    }

    // PG_RETURN_INT32(state->count)
    let count = state.borrow().count;
    Ok(count)
}
