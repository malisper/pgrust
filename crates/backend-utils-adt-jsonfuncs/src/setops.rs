//! Jsonb mutation operators (jsonfuncs.c:1679-1799, 4607-5663).
//!
//! The SQL entry points `jsonb_pretty`, `jsonb_concat`, `jsonb_delete`,
//! `jsonb_delete_array`, `jsonb_delete_idx`, `jsonb_set`, `jsonb_set_lax`,
//! `jsonb_delete_path`, `jsonb_insert`; the element setter `jsonb_set_element`;
//! the tree-rewrite helpers `IteratorConcat`, `push_null_elements`, `push_path`,
//! `setPath`, `setPathObject`, `setPathArray`; and `parse_jsonb_index_flags`.
//!
//! These are the jsonb (binary) mutation paths. Every one walks the on-disk
//! container through the landed `jsonb_util` value API
//! (`JsonbIteratorInit`/`JsonbIteratorNext`/`pushJsonbValue`) and serialises the
//! result with `JsonbValueToJsonb`. Control flow, branch order, casts and
//! integer widths are 1:1 with the C.
//!
//! Faithful adaptation to this worktree's boundaries: the `PG_FUNCTION_ARGS` /
//! `deconstruct_array_builtin` / `text`-decode glue is the fmgr/array boundary
//! (the project-wide systemic deferral). A `jsonb` argument arrives as its full
//! varlena bytes (`&[u8]`, header + root container); the container fed to the
//! iterator / root-header reads is `&jb[VARHDRSZ..]` (mirrors
//! `backend-utils-adt-jsonb`). A `text[]` path/keys argument arrives already
//! deconstructed as `&[Option<Vec<u8>>]` (one entry per element, `None` for an
//! SQL NULL element, mirroring the C `path_nulls[i]` / detoasted
//! `path_elems[i]`).

use alloc::boxed::Box;
use alloc::format;
use alloc::vec::Vec;

use mcx::{Mcx, PgVec};
use types_error::error::{
    ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_INTERNAL_ERROR, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NULL_VALUE_NOT_ALLOWED,
};
use types_error::{PgError, PgResult};

use types_jsonb::backend_utils_adt_jsonb_util::{
    JsonbIterator, JsonbParseState, JsonbValue, JsonbValueData,
};
use types_jsonb::jsonb::{
    jbvType, json_container_is_object, json_container_is_scalar, json_container_size,
    JsonbIteratorToken, VARHDRSZ,
};

use backend_utils_adt_jsonb::JsonbToCStringIndent;
use backend_utils_adt_jsonb_util::{
    JsonbIteratorInit, JsonbIteratorNext, JsonbToJsonbValue, JsonbValueToJsonb, pushJsonbValue,
};
use backend_utils_misc_stack_depth_seams as stack_depth_seams;

// ===========================================================================
// JB_PATH_* op_type bits (jsonfuncs.c:44-52) + jti* flags (jsonfuncs.c:39-43).
// Plain `u32` masks, matching the C `int op_type` / `uint32 flags`.
// ===========================================================================

/// `JB_PATH_CREATE` (jsonfuncs.c:44).
const JB_PATH_CREATE: u32 = 0x0001;
/// `JB_PATH_DELETE` (jsonfuncs.c:45).
const JB_PATH_DELETE: u32 = 0x0002;
/// `JB_PATH_REPLACE` (jsonfuncs.c:46).
const JB_PATH_REPLACE: u32 = 0x0004;
/// `JB_PATH_INSERT_BEFORE` (jsonfuncs.c:47).
const JB_PATH_INSERT_BEFORE: u32 = 0x0008;
/// `JB_PATH_INSERT_AFTER` (jsonfuncs.c:48).
const JB_PATH_INSERT_AFTER: u32 = 0x0010;
/// `JB_PATH_CREATE_OR_INSERT` (jsonfuncs.c:49-50).
const JB_PATH_CREATE_OR_INSERT: u32 = JB_PATH_INSERT_BEFORE | JB_PATH_INSERT_AFTER | JB_PATH_CREATE;
/// `JB_PATH_FILL_GAPS` (jsonfuncs.c:51).
const JB_PATH_FILL_GAPS: u32 = 0x0020;
/// `JB_PATH_CONSISTENT_POSITION` (jsonfuncs.c:52).
const JB_PATH_CONSISTENT_POSITION: u32 = 0x0040;

/// `jtiKey` (jsonfuncs.c: `enum JsonToIndex`).
pub const jtiKey: u32 = 0x01;
/// `jtiString`.
pub const jtiString: u32 = 0x02;
/// `jtiNumeric`.
pub const jtiNumeric: u32 = 0x04;
/// `jtiBool`.
pub const jtiBool: u32 = 0x08;
/// `jtiAll`.
pub const jtiAll: u32 = 0x0F;

// ===========================================================================
// Small helpers tying the repo's owned model to the C idioms.
// ===========================================================================

/// Read the root container header word from the full varlena (`&jb[VARHDRSZ..]`
/// first word). Mirrors `backend-utils-adt-jsonb::container_header`.
#[inline]
fn root_header(jb: &[u8]) -> u32 {
    let c = &jb[VARHDRSZ..];
    u32::from_ne_bytes([c[0], c[1], c[2], c[3]])
}

/// `JB_ROOT_COUNT(jb)` (jsonb.h:219).
#[inline]
fn jb_root_count(jb: &[u8]) -> u32 {
    json_container_size(root_header(jb))
}

/// `JB_ROOT_IS_SCALAR(jb)` (jsonb.h:220).
#[inline]
fn jb_root_is_scalar(jb: &[u8]) -> bool {
    json_container_is_scalar(root_header(jb))
}

/// `JB_ROOT_IS_OBJECT(jb)` (jsonb.h:221).
#[inline]
fn jb_root_is_object(jb: &[u8]) -> bool {
    json_container_is_object(root_header(jb))
}

/// `pg_abs_s32(a)` (common/int.h:221): the absolute value of an `int32` as a
/// `uint32`, widening through `int64` so `INT_MIN` does not overflow.
#[inline]
fn pg_abs_s32(a: i32) -> u32 {
    (a as i64).unsigned_abs() as u32
}

/// `r < WJB_BEGIN_ARRAY`: whether a token carries a `JsonbValue` payload when
/// pushed (`WJB_DONE`/`WJB_KEY`/`WJB_VALUE`/`WJB_ELEM` do; the `WJB_BEGIN_*` /
/// `WJB_END_*` container tokens do not).
#[inline]
fn token_carries_value(r: JsonbIteratorToken) -> bool {
    use JsonbIteratorToken::*;
    matches!(r, WJB_DONE | WJB_KEY | WJB_VALUE | WJB_ELEM)
}

/// A fresh `jbvNull` value (the C `JsonbValue v; v.type = jbvNull;` scratch, and
/// the `NULL` placeholder `jsonb_delete_path` passes to `setPath`).
#[inline]
fn jbv_null() -> JsonbValue {
    JsonbValue {
        typ: jbvType::jbvNull,
        val: JsonbValueData::Null,
    }
}

/// A `jbvString` value over the given bytes.
#[inline]
fn jbv_string(bytes: Vec<u8>) -> JsonbValue {
    JsonbValue {
        typ: jbvType::jbvString,
        val: JsonbValueData::String(bytes),
    }
}

/// `strtoint(s, &endptr, 10)` with full consumption + `errno`/`endptr` checks:
/// parse the whole byte string as a base-10 `int32`, returning `None` on the C
/// `badp == c || *badp != '\0' || errno != 0` failure. `strtol` skips leading
/// C-locale whitespace; match all six bytes.
fn parse_full_i32(s: &[u8]) -> Option<i32> {
    let t = core::str::from_utf8(s).ok()?;
    let trimmed = t.trim_start_matches([' ', '\t', '\n', '\x0B', '\x0C', '\r']);
    if trimmed.is_empty() {
        return None;
    }
    trimmed.parse::<i32>().ok()
}

/// `pg_strncasecmp(a, b, n) == 0` with the lengths already checked equal.
#[inline]
fn eq_ci(a: &[u8], b: &[u8]) -> bool {
    a.eq_ignore_ascii_case(b)
}

/// `elog(ERROR, msg)`: an internal error (XX000).
fn elog_error(msg: impl Into<alloc::string::String>) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

// Shared error shapes (used by more than one entry point).

fn wrong_number_of_array_subscripts() -> PgError {
    PgError::error("wrong number of array subscripts").with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
}

fn cannot_delete_from_scalar() -> PgError {
    PgError::error("cannot delete from scalar").with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

fn cannot_set_path_in_scalar() -> PgError {
    PgError::error("cannot set path in scalar").with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

/// `PG_RETURN_JSONB_P(JsonbValueToJsonb(res))`: serialise the built result value
/// to a `jsonb` varlena (full header + container).
#[inline]
fn value_to_jsonb<'mcx>(mcx: Mcx<'mcx>, res: &JsonbValue) -> PgResult<PgVec<'mcx, u8>> {
    JsonbValueToJsonb(mcx, res)
}

/// `PG_RETURN_JSONB_P(in)`: return an input `jsonb` unchanged. The full varlena
/// bytes are copied into `mcx` (the C returns the same datum it received).
#[inline]
fn return_jsonb<'mcx>(mcx: Mcx<'mcx>, jb: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let mut out = mcx::vec_with_capacity_in(mcx, jb.len())?;
    out.extend_from_slice(jb);
    Ok(out)
}

/// `pushJsonbValue(&state, seq, jbval)` over the owned parse-state box; threads
/// the state in place (the C `*state` write-back) and returns the produced
/// container value when `seq` closes the outermost container, else `None`.
#[inline]
fn push(
    state: &mut Option<Box<JsonbParseState>>,
    seq: JsonbIteratorToken,
    jbval: Option<&JsonbValue>,
) -> PgResult<Option<JsonbValue>> {
    pushJsonbValue(state, seq, jbval)
}

/// `JsonbIteratorNext(it, &v, skip)` returning the owned token + scratch value;
/// `it` is the owned iterator box, threaded in place (the C `*it` write-back).
#[inline]
fn iter_next(
    it: &mut Option<Box<JsonbIterator>>,
    skip_nested: bool,
) -> PgResult<(JsonbIteratorToken, JsonbValue)> {
    let mut v = jbv_null();
    let r = JsonbIteratorNext(it, &mut v, skip_nested)?;
    Ok((r, v))
}

/// `JsonbToJsonbValue(newjsonb, &newval)` (jsonb_util.c): wrap a full-varlena
/// `jsonb` as a `jbvBinary` value over its root container.
#[inline]
fn jsonb_to_jsonb_value(jb: &[u8]) -> PgResult<JsonbValue> {
    let mut v = jbv_null();
    JsonbToJsonbValue(jb, &mut v)?;
    Ok(v)
}

/// `v.type == jbvString && keylen == v.val.string.len && memcmp(...) == 0`.
#[inline]
fn string_is(v: &JsonbValue, key: &[u8]) -> bool {
    matches!(&v.val, JsonbValueData::String(s) if s.as_slice() == key)
}

/// `v.val.string.len` for a `jbvString` value (else 0; callers guard on type).
#[inline]
fn string_len(v: &JsonbValue) -> usize {
    match &v.val {
        JsonbValueData::String(s) => s.len(),
        _ => 0,
    }
}

/// `(int) r`: the iterator token discriminant the C `elog` prints (matches the
/// `JsonbIteratorToken` enum order in jsonb.h).
#[inline]
fn token_as_int(r: JsonbIteratorToken) -> i32 {
    use JsonbIteratorToken::*;
    match r {
        WJB_DONE => 0,
        WJB_KEY => 1,
        WJB_VALUE => 2,
        WJB_ELEM => 3,
        WJB_BEGIN_ARRAY => 4,
        WJB_END_ARRAY => 5,
        WJB_BEGIN_OBJECT => 6,
        WJB_END_OBJECT => 7,
    }
}

// ===========================================================================
// jsonb mutation: pretty / concat / delete (jsonfuncs.c:4607-4862)
// ===========================================================================

/// `jsonb_pretty` (jsonfuncs.c:4607): pretty-printed text for the jsonb.
///
/// Returns the text bytes (the C `cstring_to_text_with_len(str->data, str->len)`
/// payload; the varlena wrapping is the fmgr boundary).
pub fn jsonb_pretty<'mcx>(mcx: Mcx<'mcx>, jb: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // JsonbToCStringIndent(str, &jb->root, VARSIZE(jb));
    JsonbToCStringIndent(mcx, &jb[VARHDRSZ..], jb.len() as i32)
}

/// `jsonb_concat` (jsonfuncs.c:4623): the `||` operator.
pub fn jsonb_concat<'mcx>(mcx: Mcx<'mcx>, jb1: &[u8], jb2: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // If one of the jsonb is empty, just return the other if it's not scalar and
    // both are of the same kind. If it's a scalar or they are of different kinds
    // we need to perform the concatenation even if one is empty.
    if jb_root_is_object(jb1) == jb_root_is_object(jb2) {
        if jb_root_count(jb1) == 0 && !jb_root_is_scalar(jb2) {
            return return_jsonb(mcx, jb2);
        } else if jb_root_count(jb2) == 0 && !jb_root_is_scalar(jb1) {
            return return_jsonb(mcx, jb1);
        }
    }

    let mut it1 = JsonbIteratorInit(&jb1[VARHDRSZ..]);
    let mut it2 = JsonbIteratorInit(&jb2[VARHDRSZ..]);
    let mut state: Option<Box<JsonbParseState>> = None;

    let res = iterator_concat(&mut it1, &mut it2, &mut state)?;

    // Assert(res != NULL);
    value_to_jsonb(mcx, &res)
}

/// `jsonb_delete` (jsonfuncs.c:4664): delete a key/element (jsonb, text).
///
/// `key` is the `text` payload bytes (the C `VARDATA_ANY(key)`/`VARSIZE_ANY_EXHDR`).
pub fn jsonb_delete<'mcx>(mcx: Mcx<'mcx>, jb: &[u8], key: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let keylen = key.len();

    if jb_root_is_scalar(jb) {
        return Err(cannot_delete_from_scalar());
    }

    if jb_root_count(jb) == 0 {
        return return_jsonb(mcx, jb);
    }

    let mut it = JsonbIteratorInit(&jb[VARHDRSZ..]);
    let mut state: Option<Box<JsonbParseState>> = None;
    let mut res: Option<JsonbValue> = None;
    let mut skip_nested = false;

    loop {
        let (r, v) = iter_next(&mut it, skip_nested)?;
        if r == JsonbIteratorToken::WJB_DONE {
            break;
        }
        skip_nested = true;

        if (r == JsonbIteratorToken::WJB_ELEM || r == JsonbIteratorToken::WJB_KEY)
            && v.typ == jbvType::jbvString
            && keylen == string_len(&v)
            && string_is(&v, key)
        {
            // skip corresponding value as well
            if r == JsonbIteratorToken::WJB_KEY {
                let _ = iter_next(&mut it, true)?;
            }
            continue;
        }

        res = push(
            &mut state,
            r,
            if token_carries_value(r) { Some(&v) } else { None },
        )?;
    }

    value_to_jsonb(mcx, &res.ok_or_else(|| elog_error("jsonb_delete: empty result"))?)
}

/// `jsonb_delete_array` (jsonfuncs.c:4717): delete several keys (jsonb, text[]).
///
/// `keys` is the already-deconstructed `text[]` (one entry per element, `None`
/// for an SQL NULL key); `keys_ndim` is `ARR_NDIM(keys)`.
pub fn jsonb_delete_array<'mcx>(
    mcx: Mcx<'mcx>,
    jb: &[u8],
    keys: &[Option<Vec<u8>>],
    keys_ndim: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    if keys_ndim > 1 {
        return Err(wrong_number_of_array_subscripts());
    }

    if jb_root_is_scalar(jb) {
        return Err(cannot_delete_from_scalar());
    }

    if jb_root_count(jb) == 0 {
        return return_jsonb(mcx, jb);
    }

    // deconstruct_array_builtin(keys, TEXTOID, ...) is the fmgr/array boundary;
    // `keys` arrives already deconstructed. keys_len == keys.len().
    if keys.is_empty() {
        return return_jsonb(mcx, jb);
    }

    let mut it = JsonbIteratorInit(&jb[VARHDRSZ..]);
    let mut state: Option<Box<JsonbParseState>> = None;
    let mut res: Option<JsonbValue> = None;
    let mut skip_nested = false;

    loop {
        let (r, v) = iter_next(&mut it, skip_nested)?;
        if r == JsonbIteratorToken::WJB_DONE {
            break;
        }
        skip_nested = true;

        if (r == JsonbIteratorToken::WJB_ELEM || r == JsonbIteratorToken::WJB_KEY)
            && v.typ == jbvType::jbvString
        {
            let mut found = false;

            for k in keys {
                // if (keys_nulls[i]) continue;
                let keyptr = match k {
                    None => continue,
                    Some(b) => b.as_slice(),
                };
                // We rely on the array elements not being toasted.
                let keylen = keyptr.len();
                if keylen == string_len(&v) && string_is(&v, keyptr) {
                    found = true;
                    break;
                }
            }
            if found {
                // skip corresponding value as well
                if r == JsonbIteratorToken::WJB_KEY {
                    let _ = iter_next(&mut it, true)?;
                }
                continue;
            }
        }

        res = push(
            &mut state,
            r,
            if token_carries_value(r) { Some(&v) } else { None },
        )?;
    }

    value_to_jsonb(
        mcx,
        &res.ok_or_else(|| elog_error("jsonb_delete_array: empty result"))?,
    )
}

/// `jsonb_delete_idx` (jsonfuncs.c:4804): delete an array element by index.
/// Negative `idx` counts back from the end.
pub fn jsonb_delete_idx<'mcx>(mcx: Mcx<'mcx>, jb: &[u8], idx: i32) -> PgResult<PgVec<'mcx, u8>> {
    if jb_root_is_scalar(jb) {
        return Err(cannot_delete_from_scalar());
    }

    if jb_root_is_object(jb) {
        return Err(PgError::error("cannot delete from object using integer index")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    if jb_root_count(jb) == 0 {
        return return_jsonb(mcx, jb);
    }

    let mut it = JsonbIteratorInit(&jb[VARHDRSZ..]);
    let mut state: Option<Box<JsonbParseState>> = None;

    let (r, v) = iter_next(&mut it, false)?;
    debug_assert_eq!(r, JsonbIteratorToken::WJB_BEGIN_ARRAY);
    let n: u32 = match &v.val {
        JsonbValueData::Array { elems, .. } => elems.len() as u32,
        _ => 0,
    };

    let mut idx = idx;
    if idx < 0 {
        if pg_abs_s32(idx) > n {
            idx = n as i32;
        } else {
            idx = n.wrapping_add(idx as u32) as i32;
        }
    }

    // C: `if (idx >= n)` — `idx` is `int`, `n` is `uint32`, so the comparison is
    // unsigned (the `idx < 0` branch above mapped negatives to a non-negative
    // value or to `n`).
    if idx as u32 >= n {
        return return_jsonb(mcx, jb);
    }

    push(&mut state, r, None)?;

    let mut i: u32 = 0;
    let mut res: Option<JsonbValue> = None;
    loop {
        let (r, v) = iter_next(&mut it, true)?;
        if r == JsonbIteratorToken::WJB_DONE {
            break;
        }
        if r == JsonbIteratorToken::WJB_ELEM {
            let cur = i;
            i = i.wrapping_add(1);
            if cur == idx as u32 {
                continue;
            }
        }
        res = push(
            &mut state,
            r,
            if token_carries_value(r) { Some(&v) } else { None },
        )?;
    }

    value_to_jsonb(
        mcx,
        &res.ok_or_else(|| elog_error("jsonb_delete_idx: empty result"))?,
    )
}

// ===========================================================================
// jsonb mutation: set / set_lax / delete_path / insert / set_element
// (jsonfuncs.c:1679, 4868-5066)
// ===========================================================================

/// `jsonb_set_element` (jsonfuncs.c:1679): jsonb subscripting assignment.
///
/// `path` is the non-NULL `Datum *path` (in C `path_nulls` is `palloc0`'d, all
/// false), so each element here is a plain byte string. `newval` is the value to
/// assign.
pub fn jsonb_set_element<'mcx>(
    mcx: Mcx<'mcx>,
    jb: &[u8],
    path: &[Vec<u8>],
    newval: &JsonbValue,
) -> PgResult<PgVec<'mcx, u8>> {
    let path_len = path.len() as i32;
    // bool *path_nulls = palloc0(path_len * sizeof(bool)); — all false.
    let path_elems: Vec<Option<&[u8]>> = path.iter().map(|b| Some(b.as_slice())).collect();
    let path_nulls: Vec<bool> = vec_false(path.len());

    let mut newval = newval.clone();
    // if (newval->type == jbvArray && newval->val.array.rawScalar)
    //     *newval = newval->val.array.elems[0];
    if newval.typ == jbvType::jbvArray {
        if let JsonbValueData::Array {
            elems,
            raw_scalar: true,
        } = &newval.val
        {
            newval = elems[0].clone();
        }
    }

    let mut it = JsonbIteratorInit(&jb[VARHDRSZ..]);
    let mut state: Option<Box<JsonbParseState>> = None;

    let res = set_path(
        &mut it,
        &path_elems,
        &path_nulls,
        path_len,
        &mut state,
        0,
        &newval,
        JB_PATH_CREATE | JB_PATH_FILL_GAPS | JB_PATH_CONSISTENT_POSITION,
    )?;

    value_to_jsonb(mcx, &res)
}

/// `jsonb_set` (jsonfuncs.c:4868): `jsonb_set(jsonb, text[], jsonb, boolean)`.
///
/// `path` is the already-deconstructed `text[]`; `path_ndim` is `ARR_NDIM(path)`.
pub fn jsonb_set<'mcx>(
    mcx: Mcx<'mcx>,
    jb: &[u8],
    path: &[Option<Vec<u8>>],
    path_ndim: i32,
    newjsonb: &[u8],
    create: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    // JsonbToJsonbValue(newjsonb, &newval);
    let newval = jsonb_to_jsonb_value(newjsonb)?;
    jsonb_set_with_value(mcx, jb, path, path_ndim, &newval, create)
}

/// The body of [`jsonb_set`] after `newjsonb` has been converted to a
/// `JsonbValue`. Shared with the `jsonb_set_lax` `"use_json_null"` path, which
/// substitutes a different `newval` (the C re-invokes `jsonb_set` after
/// rewriting `fcinfo->args[2]`).
fn jsonb_set_with_value<'mcx>(
    mcx: Mcx<'mcx>,
    jb: &[u8],
    path: &[Option<Vec<u8>>],
    path_ndim: i32,
    newval: &JsonbValue,
    create: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    let path_len = path.len() as i32;

    if path_ndim > 1 {
        return Err(wrong_number_of_array_subscripts());
    }

    if jb_root_is_scalar(jb) {
        return Err(cannot_set_path_in_scalar());
    }

    if jb_root_count(jb) == 0 && !create {
        return return_jsonb(mcx, jb);
    }

    // deconstruct_array_builtin(...) is the fmgr boundary; path_len == path.len().
    if path.is_empty() {
        return return_jsonb(mcx, jb);
    }

    let path_elems = path_elems_of(path);
    let path_nulls = path_nulls_of(path);
    let mut it = JsonbIteratorInit(&jb[VARHDRSZ..]);
    let mut st: Option<Box<JsonbParseState>> = None;

    let res = set_path(
        &mut it,
        &path_elems,
        &path_nulls,
        path_len,
        &mut st,
        0,
        newval,
        if create { JB_PATH_CREATE } else { JB_PATH_REPLACE },
    )?;

    value_to_jsonb(mcx, &res)
}

/// `jsonb_set_lax` (jsonfuncs.c:4917):
/// `jsonb_set_lax(jsonb, text[], jsonb, boolean, text)`.
///
/// The fmgr calling convention (`fcinfo`) is the seamed boundary; the SQL
/// arguments are passed explicitly. `arg0`/`path`/`create` being `None`
/// reproduces the C `PG_ARGISNULL(0|1|3)` early `PG_RETURN_NULL()`.
/// `newjsonb == None` is the SQL-NULL new value (the case that triggers the lax
/// handling); `handle_null == None` is the SQL-NULL `null_value_treatment`.
/// Returns `Ok(None)` for the SQL-NULL result.
///
/// For the `"use_json_null"` arm, `json_null` supplies the value the C builds via
/// `DirectFunctionCall1(jsonb_in, "null")` (the `jsonb` varlena for the literal
/// `null`); the `jsonb_in` parse is the fmgr boundary.
pub fn jsonb_set_lax<'mcx>(
    mcx: Mcx<'mcx>,
    arg0: Option<&[u8]>,
    path: Option<(&[Option<Vec<u8>>], i32)>,
    newjsonb: Option<&[u8]>,
    create: Option<bool>,
    handle_null: Option<&[u8]>,
    json_null: &[u8],
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(3)) PG_RETURN_NULL();
    let (in_jb, (path_elems, path_ndim), create) = match (arg0, path, create) {
        (Some(in_jb), Some(path), Some(create)) => (in_jb, path, create),
        _ => return Ok(None),
    };

    // could happen if they pass in an explicit NULL
    let handle_val = match handle_null {
        None => {
            return Err(PgError::error("null_value_treatment must be \"delete_key\", \"return_target\", \"use_json_null\", or \"raise_exception\"")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
        }
        Some(h) => h,
    };

    // if the new value isn't an SQL NULL just call jsonb_set
    if let Some(newjsonb) = newjsonb {
        return jsonb_set(mcx, in_jb, path_elems, path_ndim, newjsonb, create).map(Some);
    }

    if handle_val == b"raise_exception" {
        Err(PgError::error("JSON value must not be null")
            .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED)
            .with_detail(
                "Exception was raised because null_value_treatment is \"raise_exception\".",
            )
            .with_hint(
                "To avoid, either change the null_value_treatment argument or ensure that an SQL NULL is not passed.",
            ))
    } else if handle_val == b"use_json_null" {
        // newval = DirectFunctionCall1(jsonb_in, CStringGetDatum("null"));
        // fcinfo->args[2].value = newval; ... return jsonb_set(fcinfo);
        let newval = jsonb_to_jsonb_value(json_null)?;
        jsonb_set_with_value(mcx, in_jb, path_elems, path_ndim, &newval, create).map(Some)
    } else if handle_val == b"delete_key" {
        jsonb_delete_path(mcx, in_jb, path_elems, path_ndim).map(Some)
    } else if handle_val == b"return_target" {
        // Jsonb *in = PG_GETARG_JSONB_P(0); PG_RETURN_JSONB_P(in);
        return_jsonb(mcx, in_jb).map(Some)
    } else {
        Err(PgError::error("null_value_treatment must be \"delete_key\", \"return_target\", \"use_json_null\", or \"raise_exception\"")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE))
    }
}

/// `jsonb_delete_path` (jsonfuncs.c:4984): delete the value at a path.
pub fn jsonb_delete_path<'mcx>(
    mcx: Mcx<'mcx>,
    jb: &[u8],
    path: &[Option<Vec<u8>>],
    path_ndim: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let path_len = path.len() as i32;

    if path_ndim > 1 {
        return Err(wrong_number_of_array_subscripts());
    }

    if jb_root_is_scalar(jb) {
        return Err(PgError::error("cannot delete path in scalar")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    if jb_root_count(jb) == 0 {
        return return_jsonb(mcx, jb);
    }

    if path.is_empty() {
        return return_jsonb(mcx, jb);
    }

    let path_elems = path_elems_of(path);
    let path_nulls = path_nulls_of(path);
    let mut it = JsonbIteratorInit(&jb[VARHDRSZ..]);
    let mut st: Option<Box<JsonbParseState>> = None;

    // setPath with newval == NULL: JB_PATH_DELETE never dereferences newval, so a
    // jbvNull placeholder reproduces the C `NULL` argument.
    let nullval = jbv_null();
    let res = set_path(
        &mut it,
        &path_elems,
        &path_nulls,
        path_len,
        &mut st,
        0,
        &nullval,
        JB_PATH_DELETE,
    )?;

    value_to_jsonb(mcx, &res)
}

/// `jsonb_insert` (jsonfuncs.c:5027): insert a value before/after a path target.
pub fn jsonb_insert<'mcx>(
    mcx: Mcx<'mcx>,
    jb: &[u8],
    path: &[Option<Vec<u8>>],
    path_ndim: i32,
    newjsonb: &[u8],
    after: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    let path_len = path.len() as i32;
    // JsonbToJsonbValue(newjsonb, &newval);
    let newval = jsonb_to_jsonb_value(newjsonb)?;

    if path_ndim > 1 {
        return Err(wrong_number_of_array_subscripts());
    }

    if jb_root_is_scalar(jb) {
        return Err(cannot_set_path_in_scalar());
    }

    if path.is_empty() {
        return return_jsonb(mcx, jb);
    }

    let path_elems = path_elems_of(path);
    let path_nulls = path_nulls_of(path);
    let mut it = JsonbIteratorInit(&jb[VARHDRSZ..]);
    let mut st: Option<Box<JsonbParseState>> = None;

    let res = set_path(
        &mut it,
        &path_elems,
        &path_nulls,
        path_len,
        &mut st,
        0,
        &newval,
        if after {
            JB_PATH_INSERT_AFTER
        } else {
            JB_PATH_INSERT_BEFORE
        },
    )?;

    value_to_jsonb(mcx, &res)
}

// ===========================================================================
// IteratorConcat / setPath family (jsonfuncs.c:1702-1799, 5076-5588)
// ===========================================================================

/// `push_null_elements` (jsonfuncs.c:1702): push `num` `jbvNull` array elements.
fn push_null_elements(ps: &mut Option<Box<JsonbParseState>>, num: i32) -> PgResult<()> {
    let null = jbv_null();
    let mut num = num;
    while num > 0 {
        num -= 1;
        push(ps, JsonbIteratorToken::WJB_ELEM, Some(&null))?;
    }
    Ok(())
}

/// `IteratorConcat` (jsonfuncs.c:5076): merge two jsonb iterators into a parse
/// state and return the resulting value. Logic copied 1:1 from the C (which
/// itself follows hstore), including the object || object fast append.
fn iterator_concat(
    it1: &mut Option<Box<JsonbIterator>>,
    it2: &mut Option<Box<JsonbIterator>>,
    state: &mut Option<Box<JsonbParseState>>,
) -> PgResult<JsonbValue> {
    use JsonbIteratorToken::*;
    let mut res: Option<JsonbValue> = None;

    // JsonbIteratorNext reports raw scalars as single-element arrays, so we only
    // need to consider "object" and "array" cases here.
    let (rk1, _v1) = iter_next(it1, false)?;
    let (rk2, _v2) = iter_next(it2, false)?;

    if rk1 == WJB_BEGIN_OBJECT && rk2 == WJB_BEGIN_OBJECT {
        // Both inputs are objects.
        //
        // Append all the tokens from v1 to res, except last WJB_END_OBJECT
        // (because res will not be finished yet).
        push(state, rk1, None)?;
        loop {
            let (r1, v1) = iter_next(it1, true)?;
            if r1 == WJB_END_OBJECT {
                break;
            }
            push(state, r1, Some(&v1))?;
        }

        // Append all the tokens from v2 to res, including last WJB_END_OBJECT
        // (the concatenation will be completed). Any duplicate keys will
        // automatically override the value from the first object.
        loop {
            let (r2, v2) = iter_next(it2, true)?;
            if r2 == WJB_DONE {
                break;
            }
            res = push(state, r2, if r2 != WJB_END_OBJECT { Some(&v2) } else { None })?;
        }
    } else if rk1 == WJB_BEGIN_ARRAY && rk2 == WJB_BEGIN_ARRAY {
        // Both inputs are arrays.
        push(state, rk1, None)?;

        loop {
            let (r1, v1) = iter_next(it1, true)?;
            if r1 == WJB_END_ARRAY {
                break;
            }
            debug_assert_eq!(r1, WJB_ELEM);
            push(state, r1, Some(&v1))?;
        }

        loop {
            let (r2, v2) = iter_next(it2, true)?;
            if r2 == WJB_END_ARRAY {
                break;
            }
            debug_assert_eq!(r2, WJB_ELEM);
            push(state, WJB_ELEM, Some(&v2))?;
        }

        res = push(state, WJB_END_ARRAY, None /* signal to sort */)?;
    } else if rk1 == WJB_BEGIN_OBJECT {
        // We have object || array.
        debug_assert_eq!(rk2, WJB_BEGIN_ARRAY);

        push(state, WJB_BEGIN_ARRAY, None)?;

        push(state, WJB_BEGIN_OBJECT, None)?;
        loop {
            let (r1, v1) = iter_next(it1, true)?;
            if r1 == WJB_DONE {
                break;
            }
            push(state, r1, if r1 != WJB_END_OBJECT { Some(&v1) } else { None })?;
        }

        loop {
            let (r2, v2) = iter_next(it2, true)?;
            if r2 == WJB_DONE {
                break;
            }
            res = push(state, r2, if r2 != WJB_END_ARRAY { Some(&v2) } else { None })?;
        }
    } else {
        // We have array || object.
        debug_assert_eq!(rk1, WJB_BEGIN_ARRAY);
        debug_assert_eq!(rk2, WJB_BEGIN_OBJECT);

        push(state, WJB_BEGIN_ARRAY, None)?;

        loop {
            let (r1, v1) = iter_next(it1, true)?;
            if r1 == WJB_END_ARRAY {
                break;
            }
            push(state, r1, Some(&v1))?;
        }

        push(state, WJB_BEGIN_OBJECT, None)?;
        loop {
            let (r2, v2) = iter_next(it2, true)?;
            if r2 == WJB_DONE {
                break;
            }
            push(state, r2, if r2 != WJB_END_OBJECT { Some(&v2) } else { None })?;
        }

        res = push(state, WJB_END_ARRAY, None)?;
    }

    res.ok_or_else(|| elog_error("IteratorConcat produced no value"))
}

/// `push_path` (jsonfuncs.c:1721): build nested empty objects/arrays for a path
/// suffix and assign `newval` at the end. E.g. the path `[a][0][b]` with the new
/// value `1` produces `{a: [{b: 1}]}`.
fn push_path(
    st: &mut Option<Box<JsonbParseState>>,
    level: i32,
    path_elems: &[Option<&[u8]>],
    path_nulls: &[bool],
    path_len: i32,
    newval: &JsonbValue,
) -> PgResult<()> {
    // tpath contains the expected type of an empty jsonb created at each level
    // higher or equal to the current one, either jbvObject or jbvArray. It
    // contains only the path slice from level to the end, so the access index
    // must be normalized by level.
    let span = (path_len - level) as usize;
    let mut tpath: Vec<jbvType> = alloc::vec![jbvType::jbvNull; span];

    // Create the first part of the chain with beginning tokens. For the current
    // level WJB_BEGIN_OBJECT/WJB_BEGIN_ARRAY was already created, so start with
    // the next one.
    let mut i = level + 1;
    while i < path_len {
        if path_nulls[i as usize] {
            break;
        }

        // Try to convert to an integer to find out the expected type, object or
        // array.
        let c = path_elems[i as usize].unwrap_or(&[]);
        match parse_full_i32(c) {
            None => {
                // text, an object is expected
                let newkey = jbv_string(c.to_vec());
                push(st, JsonbIteratorToken::WJB_BEGIN_OBJECT, None)?;
                push(st, JsonbIteratorToken::WJB_KEY, Some(&newkey))?;
                tpath[(i - level) as usize] = jbvType::jbvObject;
            }
            Some(lindex) => {
                // integer, an array is expected
                push(st, JsonbIteratorToken::WJB_BEGIN_ARRAY, None)?;
                push_null_elements(st, lindex)?;
                tpath[(i - level) as usize] = jbvType::jbvArray;
            }
        }
        i += 1;
    }

    // Insert an actual value for either an object or array.
    if tpath[((path_len - level) - 1) as usize] == jbvType::jbvArray {
        push(st, JsonbIteratorToken::WJB_ELEM, Some(newval))?;
    } else {
        push(st, JsonbIteratorToken::WJB_VALUE, Some(newval))?;
    }

    // Close everything up to the last but one level. The last one is closed
    // outside of this function.
    let mut i = path_len - 1;
    while i > level {
        if path_nulls[i as usize] {
            break;
        }
        if tpath[(i - level) as usize] == jbvType::jbvObject {
            push(st, JsonbIteratorToken::WJB_END_OBJECT, None)?;
        } else {
            push(st, JsonbIteratorToken::WJB_END_ARRAY, None)?;
        }
        i -= 1;
    }

    Ok(())
}

/// `setPath` (jsonfuncs.c:5204): recursively walk an iterator, applying a path
/// operation (`JB_PATH_*`). Object/array sub-cases delegate to
/// [`set_path_object`] / [`set_path_array`].
///
/// All path elements before the last must already exist whatever bits in
/// `op_type` are set, or nothing is done.
#[allow(clippy::too_many_arguments)]
fn set_path(
    it: &mut Option<Box<JsonbIterator>>,
    path_elems: &[Option<&[u8]>],
    path_nulls: &[bool],
    path_len: i32,
    st: &mut Option<Box<JsonbParseState>>,
    level: i32,
    newval: &JsonbValue,
    op_type: u32,
) -> PgResult<JsonbValue> {
    stack_depth_seams::check_stack_depth::call()?;

    if path_nulls[level as usize] {
        return Err(PgError::error(format!(
            "path element at position {} is null",
            level + 1
        ))
        .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED));
    }

    let (r, v) = iter_next(it, false)?;

    let res = match r {
        JsonbIteratorToken::WJB_BEGIN_ARRAY => {
            let (nelems, raw_scalar) = match &v.val {
                JsonbValueData::Array { elems, raw_scalar } => {
                    (elems.len() as u32, *raw_scalar)
                }
                _ => (0, false),
            };

            // If instructed complain about attempts to replace within a raw
            // scalar value. This happens even when current level is equal to
            // path_len, because the last path key should also correspond to an
            // object or an array, not raw scalar.
            //
            // C: `(level <= path_len - 1)`.
            if (op_type & JB_PATH_FILL_GAPS) != 0 && level <= path_len - 1 && raw_scalar {
                return Err(PgError::error("cannot replace existing key")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                    .with_detail(
                        "The path assumes key is a composite object, but it is a scalar value.",
                    ));
            }

            push(st, r, None)?;
            set_path_array(
                it, path_elems, path_nulls, path_len, st, level, newval, nelems, op_type,
            )?;
            let (r2, _v2) = iter_next(it, false)?;
            debug_assert_eq!(r2, JsonbIteratorToken::WJB_END_ARRAY);
            push(st, r2, None)?
        }
        JsonbIteratorToken::WJB_BEGIN_OBJECT => {
            let npairs = match &v.val {
                JsonbValueData::Object(pairs) => pairs.len() as u32,
                _ => 0,
            };
            push(st, r, None)?;
            set_path_object(
                it, path_elems, path_nulls, path_len, st, level, newval, npairs, op_type,
            )?;
            let (r2, _v2) = iter_next(it, true)?;
            debug_assert_eq!(r2, JsonbIteratorToken::WJB_END_OBJECT);
            push(st, r2, None)?
        }
        JsonbIteratorToken::WJB_ELEM | JsonbIteratorToken::WJB_VALUE => {
            // If instructed complain about attempts to replace within a scalar
            // value. This happens even when current level is equal to path_len,
            // because the last path key should also correspond to an object or an
            // array, not an element or value.
            //
            // C: `(level <= path_len - 1)`.
            if (op_type & JB_PATH_FILL_GAPS) != 0 && level <= path_len - 1 {
                return Err(PgError::error("cannot replace existing key")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                    .with_detail(
                        "The path assumes key is a composite object, but it is a scalar value.",
                    ));
            }
            push(st, r, Some(&v))?
        }
        other => {
            return Err(elog_error(format!(
                "unrecognized iterator result: {}",
                token_as_int(other)
            )));
        }
    };

    res.ok_or_else(|| elog_error("setPath produced no value"))
}

/// Walk over a nested container (`WJB_BEGIN_*`..matching `WJB_END_*`), copying
/// every token through to the builder. Mirrors the `walking_level` loops shared
/// by `setPathObject`/`setPathArray`.
fn copy_nested_container(
    it: &mut Option<Box<JsonbIterator>>,
    st: &mut Option<Box<JsonbParseState>>,
) -> PgResult<()> {
    let mut walking_level = 1;
    while walking_level != 0 {
        let (r, v) = iter_next(it, false)?;

        if r == JsonbIteratorToken::WJB_BEGIN_ARRAY || r == JsonbIteratorToken::WJB_BEGIN_OBJECT {
            walking_level += 1;
        }
        if r == JsonbIteratorToken::WJB_END_ARRAY || r == JsonbIteratorToken::WJB_END_OBJECT {
            walking_level -= 1;
        }

        push(
            st,
            r,
            if token_carries_value(r) { Some(&v) } else { None },
        )?;
    }
    Ok(())
}

/// `setPathObject` (jsonfuncs.c:5286): object walker for [`set_path`].
#[allow(clippy::too_many_arguments)]
fn set_path_object(
    it: &mut Option<Box<JsonbIterator>>,
    path_elems: &[Option<&[u8]>],
    path_nulls: &[bool],
    path_len: i32,
    st: &mut Option<Box<JsonbParseState>>,
    level: i32,
    newval: &JsonbValue,
    npairs: u32,
    op_type: u32,
) -> PgResult<()> {
    let mut done = false;
    let mut pathelem: Option<&[u8]> = None;

    if level >= path_len || path_nulls[level as usize] {
        done = true;
    } else {
        // The path Datum could be toasted, in which case we must detoast it; the
        // detoast is the fmgr boundary's concern (path arrives decoded).
        pathelem = Some(path_elems[level as usize].unwrap_or(&[]));
    }

    // empty object is a special case for create
    if npairs == 0 && (op_type & JB_PATH_CREATE_OR_INSERT) != 0 && level == path_len - 1 {
        let pe = pathelem.expect("pathelem set when not done");
        let newkey = jbv_string(pe.to_vec());
        push(st, JsonbIteratorToken::WJB_KEY, Some(&newkey))?;
        push(st, JsonbIteratorToken::WJB_VALUE, Some(newval))?;
    }

    for i in 0..npairs {
        let (mut r, k) = iter_next(it, true)?;
        debug_assert_eq!(r, JsonbIteratorToken::WJB_KEY);

        // k.val.string.len == VARSIZE_ANY_EXHDR(pathelem) &&
        //   memcmp(k.val.string.val, VARDATA_ANY(pathelem), k.val.string.len) == 0
        let key_matches = !done
            && match (&k.val, pathelem) {
                (JsonbValueData::String(ks), Some(pe)) => {
                    ks.len() == pe.len() && ks.as_slice() == pe
                }
                _ => false,
            };

        if key_matches {
            done = true;

            if level == path_len - 1 {
                // called from jsonb_insert(), it forbids redefining an existing
                // value
                if (op_type & (JB_PATH_INSERT_BEFORE | JB_PATH_INSERT_AFTER)) != 0 {
                    return Err(PgError::error("cannot replace existing key")
                        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                        .with_hint("Try using the function jsonb_set to replace key value."));
                }

                // skip value
                let _ = iter_next(it, true)?;
                if (op_type & JB_PATH_DELETE) == 0 {
                    push(st, JsonbIteratorToken::WJB_KEY, Some(&k))?;
                    push(st, JsonbIteratorToken::WJB_VALUE, Some(newval))?;
                }
            } else {
                push(st, r, Some(&k))?;
                set_path(
                    it,
                    path_elems,
                    path_nulls,
                    path_len,
                    st,
                    level + 1,
                    newval,
                    op_type,
                )?;
            }
        } else {
            if (op_type & JB_PATH_CREATE_OR_INSERT) != 0
                && !done
                && level == path_len - 1
                && i == npairs - 1
            {
                let pe = pathelem.expect("pathelem set when not done");
                let newkey = jbv_string(pe.to_vec());
                push(st, JsonbIteratorToken::WJB_KEY, Some(&newkey))?;
                push(st, JsonbIteratorToken::WJB_VALUE, Some(newval))?;
            }

            push(st, r, Some(&k))?;
            let (r3, v) = iter_next(it, false)?;
            r = r3;
            push(
                st,
                r,
                if token_carries_value(r) { Some(&v) } else { None },
            )?;
            if r == JsonbIteratorToken::WJB_BEGIN_ARRAY || r == JsonbIteratorToken::WJB_BEGIN_OBJECT
            {
                copy_nested_container(it, st)?;
            }
        }
    }

    // If we got here there are only a few possibilities:
    // - no target path was found, and an open object with some keys/values was
    //   pushed into the state
    // - an object is empty, only WJB_BEGIN_OBJECT is pushed
    //
    // In both cases if instructed to create the path when not present, generate
    // the whole chain of empty objects and insert the new value there.
    if !done && (op_type & JB_PATH_FILL_GAPS) != 0 && level < path_len - 1 {
        let pe = pathelem.expect("pathelem set when not done");
        let newkey = jbv_string(pe.to_vec());
        push(st, JsonbIteratorToken::WJB_KEY, Some(&newkey))?;
        push_path(st, level, path_elems, path_nulls, path_len, newval)?;
        // Result is closed with WJB_END_OBJECT outside of this function.
    }

    Ok(())
}

/// `setPathArray` (jsonfuncs.c:5425): array walker for [`set_path`].
#[allow(clippy::too_many_arguments)]
fn set_path_array(
    it: &mut Option<Box<JsonbIterator>>,
    path_elems: &[Option<&[u8]>],
    path_nulls: &[bool],
    path_len: i32,
    st: &mut Option<Box<JsonbParseState>>,
    level: i32,
    newval: &JsonbValue,
    nelems: u32,
    op_type: u32,
) -> PgResult<()> {
    let mut done = false;

    // pick correct index
    let mut idx: i32;
    if level < path_len && !path_nulls[level as usize] {
        let c = path_elems[level as usize].unwrap_or(&[]);
        match parse_full_i32(c) {
            Some(n) => idx = n,
            None => {
                return Err(PgError::error(format!(
                    "path element at position {} is not an integer: \"{}\"",
                    level + 1,
                    String::from_utf8_lossy(c)
                ))
                .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION));
            }
        }
    } else {
        idx = nelems as i32;
    }

    if idx < 0 {
        if pg_abs_s32(idx) > nelems {
            // If asked to keep elements position consistent, it's not allowed to
            // prepend the array.
            if (op_type & JB_PATH_CONSISTENT_POSITION) != 0 {
                return Err(PgError::error(format!(
                    "path element at position {} is out of range: {}",
                    level + 1,
                    idx
                ))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            } else {
                idx = i32::MIN;
            }
        } else {
            idx = nelems.wrapping_add(idx as u32) as i32;
        }
    }

    // Filling the gaps means there are no limits on the positive index, we can
    // set any element. Otherwise limit the index by nelems.
    if (op_type & JB_PATH_FILL_GAPS) == 0 && idx > 0 && idx as u32 > nelems {
        idx = nelems as i32;
    }

    // if we're creating, and idx == INT_MIN, we prepend the new value to the
    // array also if the array is empty - in which case we don't really care what
    // the idx value is
    if (idx == i32::MIN || nelems == 0)
        && level == path_len - 1
        && (op_type & JB_PATH_CREATE_OR_INSERT) != 0
    {
        // Assert(newval != NULL);
        if (op_type & JB_PATH_FILL_GAPS) != 0 && nelems == 0 && idx > 0 {
            push_null_elements(st, idx)?;
        }
        push(st, JsonbIteratorToken::WJB_ELEM, Some(newval))?;
        done = true;
    }

    // iterate over the array elements
    for i in 0..nelems as i32 {
        if i == idx && level < path_len {
            done = true;

            if level == path_len - 1 {
                // skip
                let (r, v) = iter_next(it, true)?;

                if (op_type & (JB_PATH_INSERT_BEFORE | JB_PATH_CREATE)) != 0 {
                    push(st, JsonbIteratorToken::WJB_ELEM, Some(newval))?;
                }

                // We should keep the current value only in case of
                // JB_PATH_INSERT_BEFORE or JB_PATH_INSERT_AFTER because otherwise
                // it should be deleted or replaced.
                if (op_type & (JB_PATH_INSERT_AFTER | JB_PATH_INSERT_BEFORE)) != 0 {
                    push(st, r, Some(&v))?;
                }

                if (op_type & (JB_PATH_INSERT_AFTER | JB_PATH_REPLACE)) != 0 {
                    push(st, JsonbIteratorToken::WJB_ELEM, Some(newval))?;
                }
            } else {
                set_path(
                    it,
                    path_elems,
                    path_nulls,
                    path_len,
                    st,
                    level + 1,
                    newval,
                    op_type,
                )?;
            }
        } else {
            let (r, v) = iter_next(it, false)?;

            push(
                st,
                r,
                if token_carries_value(r) { Some(&v) } else { None },
            )?;

            if r == JsonbIteratorToken::WJB_BEGIN_ARRAY || r == JsonbIteratorToken::WJB_BEGIN_OBJECT
            {
                copy_nested_container(it, st)?;
            }
        }
    }

    if (op_type & JB_PATH_CREATE_OR_INSERT) != 0 && !done && level == path_len - 1 {
        // If asked to fill the gaps, idx could be bigger than nelems, so prepend
        // the new element with nulls if that's the case.
        if (op_type & JB_PATH_FILL_GAPS) != 0 && idx as u32 > nelems {
            push_null_elements(st, idx - nelems as i32)?;
        }
        push(st, JsonbIteratorToken::WJB_ELEM, Some(newval))?;
        done = true;
    }

    // If we got here there are only a few possibilities:
    // - no target path was found, and an open array with some keys/values was
    //   pushed into the state
    // - an array is empty, only WJB_BEGIN_ARRAY is pushed
    //
    // In both cases if instructed to create the path when not present, generate
    // the whole chain of empty objects and insert the new value there.
    if !done && (op_type & JB_PATH_FILL_GAPS) != 0 && level < path_len - 1 {
        if idx > 0 {
            push_null_elements(st, idx - nelems as i32)?;
        }
        push_path(st, level, path_elems, path_nulls, path_len, newval)?;
        // Result is closed with WJB_END_OBJECT outside of this function.
    }

    Ok(())
}

// ===========================================================================
// parse_jsonb_index_flags (jsonfuncs.c:5596)
// ===========================================================================

/// `parse_jsonb_index_flags` (jsonfuncs.c:5596): parse the flag array that
/// describes which jsonb value kinds to iterate in `iterate_json(b)_values`,
/// into the `jti*` bitmask. The information is presented in jsonb format.
pub fn parse_jsonb_index_flags(jb: &[u8]) -> PgResult<u32> {
    let mut it = JsonbIteratorInit(&jb[VARHDRSZ..]);
    let mut flags: u32 = 0;

    let (mut typ, _v) = iter_next(&mut it, false)?;

    // We iterate over the array (a scalar internally is represented as an array,
    // so we accept it too) to check all its elements. Flag names are chosen the
    // same as jsonb_typeof uses.
    if typ != JsonbIteratorToken::WJB_BEGIN_ARRAY {
        return Err(PgError::error("wrong flag type, only arrays and scalars are allowed")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    loop {
        let (t, v) = iter_next(&mut it, false)?;
        typ = t;
        if typ != JsonbIteratorToken::WJB_ELEM {
            break;
        }

        if v.typ != jbvType::jbvString {
            return Err(PgError::error("flag array element is not a string")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .with_hint(
                    "Possible values are: \"string\", \"numeric\", \"boolean\", \"key\", and \"all\".",
                ));
        }

        let s = match &v.val {
            JsonbValueData::String(s) => s.as_slice(),
            _ => unreachable!("jbvString payload"),
        };

        if s.len() == 3 && eq_ci(s, b"all") {
            flags |= jtiAll;
        } else if s.len() == 3 && eq_ci(s, b"key") {
            flags |= jtiKey;
        } else if s.len() == 6 && eq_ci(s, b"string") {
            flags |= jtiString;
        } else if s.len() == 7 && eq_ci(s, b"numeric") {
            flags |= jtiNumeric;
        } else if s.len() == 7 && eq_ci(s, b"boolean") {
            flags |= jtiBool;
        } else {
            // errmsg("wrong flag in flag array: \"%s\"", pnstrdup(val, len))
            let bad = String::from_utf8_lossy(s).into_owned();
            return Err(PgError::error(format!("wrong flag in flag array: \"{bad}\""))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .with_hint(
                    "Possible values are: \"string\", \"numeric\", \"boolean\", \"key\", and \"all\".",
                ));
        }
    }

    // expect end of array now
    if typ != JsonbIteratorToken::WJB_END_ARRAY {
        return Err(elog_error("unexpected end of flag array"));
    }

    // get final WJB_DONE and free iterator
    let (typ, _v) = iter_next(&mut it, false)?;
    if typ != JsonbIteratorToken::WJB_DONE {
        return Err(elog_error("unexpected end of flag array"));
    }

    Ok(flags)
}

// ===========================================================================
// Local helpers tied to the deconstructed-path boundary.
// ===========================================================================

/// The `path_elems[]` byte-slice view of a deconstructed path (the C detoasted
/// `path_elems[i]`); a NULL element views as an empty slice (never read, since
/// `path_nulls[i]` guards every access).
fn path_elems_of(path: &[Option<Vec<u8>>]) -> Vec<Option<&[u8]>> {
    path.iter().map(|e| e.as_deref()).collect()
}

/// The `path_nulls[]` companion array: `true` for an SQL NULL element.
fn path_nulls_of(path: &[Option<Vec<u8>>]) -> Vec<bool> {
    path.iter().map(|e| e.is_none()).collect()
}

/// `palloc0(n * sizeof(bool))`: an all-false `path_nulls` vector of length `n`.
fn vec_false(n: usize) -> Vec<bool> {
    alloc::vec![false; n]
}
