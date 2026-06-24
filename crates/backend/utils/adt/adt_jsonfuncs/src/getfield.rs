//! Field / element / path extraction (jsonfuncs.c:838-1678): the backings for
//! the `->`, `->>`, `#>`, `#>>` operators and the `json[b]_extract_path[_text]`
//! functions.
//!
//! Ports `json[b]_object_field[_text]`, `json[b]_array_element[_text]`,
//! `json[b]_extract_path[_text]`, `get_path_all`, `get_worker`, the nine
//! `get_*` SAX callbacks (the `GetState` path-matching state machine),
//! `get_jsonb_path_all` and `jsonb_get_element`.
//!
//! The jsonb (binary) surface walks the on-disk tree directly through the
//! landed `jsonb_util.c` API (`getKeyJsonValueFromContainer`,
//! `getIthJsonbValueFromContainer`, `JsonbValueToJsonb`). The json (text)
//! surface drives the `common/jsonapi.c` recursive-descent parser through the
//! `common-jsonapi-seams::pg_parse_json` SAX-driver seam over a real
//! [`JsonSemAction`] callback table closing over a shared [`GetState`]
//! (`Rc<RefCell<..>>`), exactly as `keys.rs` / `length.rs` do.
//!
//! `JsonbValueAsText` lives in `crate::common`; it is imported, not re-ported.

#![allow(non_snake_case)]

use core::cell::RefCell;

use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::vec::Vec;

use ::utils_error::ereport;
use ::mcx::{Mcx, PgVec};
use ::types_error::error::ERROR;
use ::types_error::PgResult;
use ::types_json::{JsonLexContext, JsonParseErrorType, JsonSemAction, JsonTokenType};
use types_jsonb::jsonb_util::{JsonbValue, JsonbValueData};
use ::types_jsonb::jsonb::{
    is_a_jsonb_scalar, jbvType, json_container_is_array, json_container_is_object,
    json_container_is_scalar, json_container_size,
};

use ::adt_jsonb::JsonbToCString;
use ::jsonb_util::{
    getIthJsonbValueFromContainer, getKeyJsonValueFromContainer, JsonbValueToJsonb,
};

use crate::common::JsonbValueAsText;

// ===========================================================================
// Small helpers over the root JsonbContainer header word.
// ===========================================================================

/// Read the leading `JsonbContainer.header` word from a root container's bytes.
#[inline]
fn container_header(root: &[u8]) -> u32 {
    u32::from_ne_bytes([root[0], root[1], root[2], root[3]])
}

use crate::common::vardata_any;

/// `JB_ROOT_COUNT(jb)` (jsonb.h:219) — over the full varlena.
#[inline]
fn jb_root_count(jb: &[u8]) -> u32 {
    json_container_size(container_header(vardata_any(jb)))
}

/// `JB_ROOT_IS_OBJECT(jb)` (jsonb.h:221).
#[inline]
fn jb_root_is_object(jb: &[u8]) -> bool {
    json_container_is_object(container_header(vardata_any(jb)))
}

/// `JB_ROOT_IS_ARRAY(jb)` (jsonb.h:222).
#[inline]
fn jb_root_is_array(jb: &[u8]) -> bool {
    json_container_is_array(container_header(vardata_any(jb)))
}

/// `JB_ROOT_IS_SCALAR(jb)` (jsonb.h:220).
#[inline]
fn jb_root_is_scalar(jb: &[u8]) -> bool {
    json_container_is_scalar(container_header(vardata_any(jb)))
}

/// `pg_abs_s32(a)` (common/int.h): `|a|` as a `uint32`, widening through `int64`
/// so `INT_MIN` does not overflow.
#[inline]
fn pg_abs_s32(a: i32) -> u32 {
    (a as i64).unsigned_abs() as u32
}

/// `VARSIZE(jb)` (postgres.h): the total varlena byte size, used only as the
/// `JsonbToCString` length *estimate*.
#[inline]
fn varsize_jsonb(jb: &[u8]) -> i32 {
    jb.len() as i32
}

// ===========================================================================
// jsonb (binary) field / element accessors (jsonfuncs.c:861-1006).
// ===========================================================================

/// `jsonb_object_field` (jsonfuncs.c:862): `jb -> key`.
pub fn jsonb_object_field<'mcx>(
    mcx: Mcx<'mcx>,
    jb: &'mcx [u8],
    key: &[u8],
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // if (!JB_ROOT_IS_OBJECT(jb)) PG_RETURN_NULL();
    if !jb_root_is_object(jb) {
        return Ok(None);
    }

    // v = getKeyJsonValueFromContainer(&jb->root, VARDATA_ANY(key),
    //                                  VARSIZE_ANY_EXHDR(key), &vbuf);
    let v = getKeyJsonValueFromContainer(vardata_any(jb), key)?;

    // if (v != NULL) PG_RETURN_JSONB_P(JsonbValueToJsonb(v));
    match v {
        Some(v) => Ok(Some(JsonbValueToJsonb(mcx, &v)?)),
        None => Ok(None),
    }
}

/// `jsonb_object_field_text` (jsonfuncs.c:900): `jb ->> key`.
pub fn jsonb_object_field_text<'mcx>(
    mcx: Mcx<'mcx>,
    jb: &'mcx [u8],
    key: &[u8],
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // if (!JB_ROOT_IS_OBJECT(jb)) PG_RETURN_NULL();
    if !jb_root_is_object(jb) {
        return Ok(None);
    }

    let v = getKeyJsonValueFromContainer(vardata_any(jb), key)?;

    // if (v != NULL && v->type != jbvNull) PG_RETURN_TEXT_P(JsonbValueAsText(v));
    match v {
        Some(v) if v.typ != jbvType::jbvNull => JsonbValueAsText(mcx, &v),
        _ => Ok(None),
    }
}

/// `jsonb_array_element` (jsonfuncs.c:937): `jb -> element`.
pub fn jsonb_array_element<'mcx>(
    mcx: Mcx<'mcx>,
    jb: &'mcx [u8],
    element: i32,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // if (!JB_ROOT_IS_ARRAY(jb)) PG_RETURN_NULL();
    if !jb_root_is_array(jb) {
        return Ok(None);
    }

    let mut element = element;
    // Handle negative subscript.
    if element < 0 {
        let nelements = jb_root_count(jb);
        // if (pg_abs_s32(element) > nelements) PG_RETURN_NULL();
        if pg_abs_s32(element) > nelements {
            return Ok(None);
        } else {
            // element += nelements;
            element += nelements as i32;
        }
    }

    // v = getIthJsonbValueFromContainer(&jb->root, element);
    let v = getIthJsonbValueFromContainer(vardata_any(jb), element as u32)?;
    match v {
        Some(v) => Ok(Some(JsonbValueToJsonb(mcx, &v)?)),
        None => Ok(None),
    }
}

/// `jsonb_array_element_text` (jsonfuncs.c:980): `jb ->> element`.
pub fn jsonb_array_element_text<'mcx>(
    mcx: Mcx<'mcx>,
    jb: &'mcx [u8],
    element: i32,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // if (!JB_ROOT_IS_ARRAY(jb)) PG_RETURN_NULL();
    if !jb_root_is_array(jb) {
        return Ok(None);
    }

    let mut element = element;
    if element < 0 {
        let nelements = jb_root_count(jb);
        if pg_abs_s32(element) > nelements {
            return Ok(None);
        } else {
            element += nelements as i32;
        }
    }

    let v = getIthJsonbValueFromContainer(vardata_any(jb), element as u32)?;

    // if (v != NULL && v->type != jbvNull) PG_RETURN_TEXT_P(JsonbValueAsText(v));
    match v {
        Some(v) if v.typ != jbvType::jbvNull => JsonbValueAsText(mcx, &v),
        _ => Ok(None),
    }
}

// ===========================================================================
// GetState (jsonfuncs.c:86): the json getter SAX state.
//
// C carries a `JsonLexContext *lex` back-pointer the callbacks read positions
// off of; here the parse driver hands the live `&JsonLexContext` to each
// callback, so `lex` is not a field. `result_start`, a `const char *` into the
// immutable input in C, is a byte offset into `lex.input` here (`None` == the C
// NULL). `tresult` is captured text bytes (`None` == NULL).
// ===========================================================================

#[derive(Default)]
struct GetState {
    /// `text *tresult`: the captured value text (`None` == NULL).
    tresult: Option<Vec<u8>>,
    /// `const char *result_start`: offset into `lex.input` of the start of the
    /// value being captured (`None` == NULL).
    result_start: Option<usize>,
    /// `bool normalize_results`: the `_as_text` variant flag.
    normalize_results: bool,
    /// `bool next_scalar`: tell `get_scalar` to capture the next scalar.
    next_scalar: bool,
    /// `int npath`: length of each path-related array.
    npath: i32,
    /// `char **path_names`: field name(s) being sought (`None` for the whole
    /// array; inner `None` == "don't match a field at this level").
    path_names: Option<Vec<Option<Vec<u8>>>>,
    /// `int *path_indexes`: array index(es) being sought (`None` for the whole
    /// object).
    path_indexes: Option<Vec<i32>>,
    /// `bool *pathok`: is the path matched to the current depth?
    pathok: Vec<bool>,
    /// `int *array_cur_index`: current element index at each path level.
    array_cur_index: Vec<i32>,
}

// ===========================================================================
// json (text) field / element / path getters (jsonfuncs.c:846-1023).
// ===========================================================================

/// `json_object_field` (jsonfuncs.c:846): `json -> fname`.
pub fn json_object_field<'mcx>(
    mcx: Mcx<'mcx>,
    json: &[u8],
    fname: &[u8],
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // fnamestr = text_to_cstring(fname);
    // result = get_worker(json, &fnamestr, NULL, 1, false);
    let tpath: Vec<Option<Vec<u8>>> = alloc::vec![Some(fname.to_vec())];
    get_worker(mcx, json, Some(&tpath), None, 1, false)
}

/// `json_object_field_text` (jsonfuncs.c:884): `json ->> fname`.
pub fn json_object_field_text<'mcx>(
    mcx: Mcx<'mcx>,
    json: &[u8],
    fname: &[u8],
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let tpath: Vec<Option<Vec<u8>>> = alloc::vec![Some(fname.to_vec())];
    get_worker(mcx, json, Some(&tpath), None, 1, true)
}

/// `json_array_element` (jsonfuncs.c:922): `json -> element`.
pub fn json_array_element<'mcx>(
    mcx: Mcx<'mcx>,
    json: &[u8],
    element: i32,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // result = get_worker(json, NULL, &element, 1, false);
    let ipath: Vec<i32> = alloc::vec![element];
    get_worker(mcx, json, None, Some(ipath), 1, false)
}

/// `json_array_element_text` (jsonfuncs.c:965): `json ->> element`.
pub fn json_array_element_text<'mcx>(
    mcx: Mcx<'mcx>,
    json: &[u8],
    element: i32,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let ipath: Vec<i32> = alloc::vec![element];
    get_worker(mcx, json, None, Some(ipath), 1, true)
}

/// `json_extract_path` (jsonfuncs.c:1009): `json #> path`.
pub fn json_extract_path<'mcx>(
    mcx: Mcx<'mcx>,
    json: &[u8],
    path: &[Option<Vec<u8>>],
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    get_path_all(mcx, json, path, false)
}

/// `json_extract_path_text` (jsonfuncs.c:1015): `json #>> path`.
pub fn json_extract_path_text<'mcx>(
    mcx: Mcx<'mcx>,
    json: &[u8],
    path: &[Option<Vec<u8>>],
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    get_path_all(mcx, json, path, true)
}

/// `get_path_all` (jsonfuncs.c:1023): common routine for the extract_path
/// functions.
///
/// `path` is the already-deconstructed `text[]` (one `Option<Vec<u8>>` per
/// element, `None` == SQL NULL element); `array_contains_nulls` +
/// `deconstruct_array_builtin` are the fmgr/array boundary's concern.
fn get_path_all<'mcx>(
    mcx: Mcx<'mcx>,
    json: &[u8],
    path: &[Option<Vec<u8>>],
    as_text: bool,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // If the array contains any null elements, return NULL, on the grounds that
    // you'd have gotten NULL if any RHS value were NULL in a nested series of
    // applications of the -> operator. (Note: because we also return NULL for
    // error cases such as no-such-field, this is true regardless of the contents
    // of the rest of the array.)
    //
    // if (array_contains_nulls(path)) PG_RETURN_NULL();
    if path.iter().any(|e| e.is_none()) {
        return Ok(None);
    }

    let npath = path.len() as i32;

    // tpath = palloc(npath * sizeof(char *)); ipath = palloc(npath * sizeof(int));
    let mut tpath: Vec<Option<Vec<u8>>> = Vec::with_capacity(npath as usize);
    let mut ipath: Vec<i32> = Vec::with_capacity(npath as usize);

    for elem in path {
        // Assert(!pathnulls[i]); tpath[i] = TextDatumGetCString(pathtext[i]);
        let s = elem
            .as_ref()
            .expect("array_contains_nulls already checked");
        tpath.push(Some(s.clone()));

        // we have no idea at this stage what structure the document is so just
        // convert anything in the path that we can to an integer and set all the
        // other integers to INT_MIN which will never match.
        //
        // if (*tpath[i] != '\0') { ind = strtoint(tpath[i], &endptr, 10);
        //     if (endptr == tpath[i] || *endptr != '\0' || errno != 0)
        //         ipath[i] = INT_MIN; else ipath[i] = ind; }
        // else ipath[i] = INT_MIN;
        if !s.is_empty() {
            match parse_full_i32(s) {
                Some(ind) => ipath.push(ind),
                None => ipath.push(i32::MIN),
            }
        } else {
            ipath.push(i32::MIN);
        }
    }

    // result = get_worker(json, tpath, ipath, npath, as_text);
    get_worker(mcx, json, Some(&tpath), Some(ipath), npath, as_text)
}

// ===========================================================================
// get_worker + the nine get_* SAX callbacks (jsonfuncs.c:1102-1485).
// ===========================================================================

/// `get_worker` (jsonfuncs.c:1102): common worker for all the json getter
/// functions.
///
/// `tpath` is the field name(s) to extract (or `None`); `ipath` is the array
/// index(es) (or `None`, taken by value so the negative-subscript pre-pass in
/// `get_array_start` can mutate it); `npath` is the path length;
/// `normalize_results` de-escapes string/null scalars (the `_as_text` variants).
///
/// Drives the `common/jsonapi.c` parser through the `pg_parse_json` SAX-driver
/// seam over a real `JsonSemAction` table closing over a shared `GetState`
/// (`Rc<RefCell<..>>`), exactly as the other json-text workers do.
fn get_worker<'mcx>(
    mcx: Mcx<'mcx>,
    json: &[u8],
    tpath: Option<&[Option<Vec<u8>>]>,
    ipath: Option<Vec<i32>>,
    npath: i32,
    normalize_results: bool,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    debug_assert!(npath >= 0);

    // state = palloc0(sizeof(GetState));
    // state->pathok = palloc0(sizeof(bool) * npath);
    // state->array_cur_index = palloc(sizeof(int) * npath);
    let path_names = tpath.map(|t| t.to_vec());
    let path_indexes = ipath;
    let state = Rc::new(RefCell::new(GetState {
        tresult: None,
        result_start: None,
        normalize_results,
        next_scalar: false,
        npath,
        path_names,
        path_indexes,
        pathok: alloc::vec![false; npath.max(0) as usize],
        array_cur_index: alloc::vec![0; npath.max(0) as usize],
    }));

    // if (npath > 0) state->pathok[0] = true;
    if npath > 0 {
        state.borrow_mut().pathok[0] = true;
    }

    // sem = palloc0(sizeof(JsonSemAction)); sem->semstate = state;
    let mut sem = JsonSemAction::default();

    // Not all variants need all the semantic routines. Only set the ones that
    // are actually needed for maximum efficiency.
    //
    // sem->scalar = get_scalar;
    {
        let state = Rc::clone(&state);
        sem.scalar = Some(Box::new(
            move |lex: &JsonLexContext, token: &[u8], tokentype: JsonTokenType| {
                get_scalar(&mut state.borrow_mut(), lex, token, tokentype)
            },
        ));
    }
    // if (npath == 0) { sem->object_start/object_end/array_start/array_end = ...; }
    if npath == 0 {
        {
            let state = Rc::clone(&state);
            sem.object_start =
                Some(Box::new(move |lex: &JsonLexContext| {
                    get_object_start(&mut state.borrow_mut(), lex)
                }));
        }
        {
            let state = Rc::clone(&state);
            sem.object_end =
                Some(Box::new(move |lex: &JsonLexContext| {
                    get_object_end(&mut state.borrow_mut(), lex)
                }));
        }
        {
            let state = Rc::clone(&state);
            let json_owned = json.to_vec();
            sem.array_start = Some(Box::new(move |lex: &JsonLexContext| {
                get_array_start(&mut state.borrow_mut(), lex, &json_owned)
            }));
        }
        {
            let state = Rc::clone(&state);
            sem.array_end =
                Some(Box::new(move |lex: &JsonLexContext| {
                    get_array_end(&mut state.borrow_mut(), lex)
                }));
        }
    }
    // if (tpath != NULL) { sem->object_field_start/end = ...; }
    if tpath.is_some() {
        {
            let state = Rc::clone(&state);
            sem.object_field_start = Some(Box::new(
                move |lex: &JsonLexContext, fname: &[u8], _isnull: bool| {
                    get_object_field_start(&mut state.borrow_mut(), lex, fname)
                },
            ));
        }
        {
            let state = Rc::clone(&state);
            sem.object_field_end = Some(Box::new(
                move |lex: &JsonLexContext, fname: &[u8], isnull: bool| {
                    get_object_field_end(&mut state.borrow_mut(), lex, fname, isnull)
                },
            ));
        }
    }
    // if (ipath != NULL) { sem->array_start = get_array_start;
    //                      sem->array_element_start/end = ...; }
    if state.borrow().path_indexes.is_some() {
        {
            let state = Rc::clone(&state);
            let json_owned = json.to_vec();
            sem.array_start = Some(Box::new(move |lex: &JsonLexContext| {
                get_array_start(&mut state.borrow_mut(), lex, &json_owned)
            }));
        }
        {
            let state = Rc::clone(&state);
            sem.array_element_start =
                Some(Box::new(move |lex: &JsonLexContext, isnull: bool| {
                    get_array_element_start(&mut state.borrow_mut(), lex, isnull)
                }));
        }
        {
            let state = Rc::clone(&state);
            sem.array_element_end =
                Some(Box::new(move |lex: &JsonLexContext, isnull: bool| {
                    get_array_element_end(&mut state.borrow_mut(), lex, isnull)
                }));
        }
    }

    // makeJsonLexContext(NULL, json, true); pg_parse_json_or_ereport(lex, sem);
    let encoding = jsonapi_seams::get_database_encoding::call();
    let result = jsonapi_seams::pg_parse_json::call(json, encoding, true, &mut sem)?;
    if result != JsonParseErrorType::JSON_SUCCESS {
        // pg_parse_json_or_ereport: a parse failure raises through json_errsave_error.
        jsonapi_seams::errsave_error::call(result, json, true, None)?;
        unreachable!("errsave_error with no escontext raises");
    }

    // return state->tresult;  (charge the result text to the caller's context)
    let tresult = core::mem::take(&mut state.borrow_mut().tresult);
    match tresult {
        Some(bytes) => Ok(Some(slice_to_pgvec(mcx, &bytes)?)),
        None => Ok(None),
    }
}

/// `get_object_start` (jsonfuncs.c:1159).
fn get_object_start(state: &mut GetState, lex: &JsonLexContext) -> PgResult<()> {
    let lex_level = lex.lex_level;

    if lex_level == 0 && state.npath == 0 {
        // Special case: we should match the entire object. We only need this at
        // outermost level because at nested levels the match will have been
        // started by the outer field or array element callback.
        state.result_start = Some(lex.token_start);
    }

    Ok(())
}

/// `get_object_end` (jsonfuncs.c:1178).
fn get_object_end(state: &mut GetState, lex: &JsonLexContext) -> PgResult<()> {
    let lex_level = lex.lex_level;

    if lex_level == 0 && state.npath == 0 {
        // Special case: return the entire object.
        let start = state.result_start.expect("result_start set in get_object_start");
        // len = lex->prev_token_terminator - start;
        let len = lex.prev_token_terminator - start;
        state.tresult = Some(cstring_to_text_with_len(&lex.input, start, len));
    }

    Ok(())
}

/// `get_object_field_start` (jsonfuncs.c:1196).
fn get_object_field_start(
    state: &mut GetState,
    lex: &JsonLexContext,
    fname: &[u8],
) -> PgResult<()> {
    let mut get_next = false;
    let lex_level = lex.lex_level;

    // if (lex_level <= npath && pathok[lex_level - 1] && path_names != NULL &&
    //     path_names[lex_level - 1] != NULL &&
    //     strcmp(fname, path_names[lex_level - 1]) == 0)
    if lex_level <= state.npath
        && state.pathok[(lex_level - 1) as usize]
        && path_name_matches(&state.path_names, lex_level - 1, fname)
    {
        if lex_level < state.npath {
            // if not at end of path just mark path ok
            state.pathok[lex_level as usize] = true;
        } else {
            // end of path, so we want this value
            get_next = true;
        }
    }

    if get_next {
        // this object overrides any previous matching object
        state.tresult = None;
        state.result_start = None;

        if state.normalize_results && lex.token_type == JsonTokenType::JSON_TOKEN_STRING {
            // for as_text variants, tell get_scalar to set it for us
            state.next_scalar = true;
        } else {
            // for non-as_text variants, just note the json starting point
            state.result_start = Some(lex.token_start);
        }
    }

    Ok(())
}

/// `get_object_field_end` (jsonfuncs.c:1243).
fn get_object_field_end(
    state: &mut GetState,
    lex: &JsonLexContext,
    fname: &[u8],
    isnull: bool,
) -> PgResult<()> {
    let mut get_last = false;
    let lex_level = lex.lex_level;

    // same tests as in get_object_field_start
    if lex_level <= state.npath
        && state.pathok[(lex_level - 1) as usize]
        && path_name_matches(&state.path_names, lex_level - 1, fname)
    {
        if lex_level < state.npath {
            // done with this field so reset pathok
            state.pathok[lex_level as usize] = false;
        } else {
            // end of path, so we want this value
            get_last = true;
        }
    }

    // for as_text scalar case, our work is already done
    if get_last && state.result_start.is_some() {
        // make a text object from the string from the previously noted json
        // start up to the end of the previous token (the lexer is by now ahead
        // of us on whatever came after what we're interested in).
        if isnull && state.normalize_results {
            state.tresult = None;
        } else {
            let start = state.result_start.expect("checked is_some");
            let len = lex.prev_token_terminator - start;
            state.tresult = Some(cstring_to_text_with_len(&lex.input, start, len));
        }

        // this should be unnecessary but let's do it for cleanliness:
        state.result_start = None;
    }

    Ok(())
}

/// `get_array_start` (jsonfuncs.c:1294).
///
/// `json` is the full input the parse was started over; the C
/// `json_count_array_elements(_state->lex, ...)` lookahead is reached through
/// the `(json, encoding)`-shaped seam, the only parse-driver counting boundary
/// available in this worktree.
fn get_array_start(state: &mut GetState, lex: &JsonLexContext, json: &[u8]) -> PgResult<()> {
    let lex_level = lex.lex_level;

    if lex_level < state.npath {
        // Initialize counting of elements in this array.
        state.array_cur_index[lex_level as usize] = -1;

        // INT_MIN value is reserved to represent invalid subscript.
        let path_idx = state
            .path_indexes
            .as_ref()
            .expect("ipath set when npath>0")[lex_level as usize];
        if path_idx < 0 && path_idx != i32::MIN {
            // Negative subscript -- convert to positive-wise subscript.
            // error = json_count_array_elements(lex, &nelements);
            // if (error != JSON_SUCCESS) json_errsave_error(error, lex, NULL);
            let encoding = jsonapi_seams::get_database_encoding::call();
            let nelements = jsonapi_seams::json_count_array_elements::call(json, encoding)?;

            // if (-path_indexes[lex_level] <= nelements)
            //     path_indexes[lex_level] += nelements;
            if -path_idx <= nelements {
                state
                    .path_indexes
                    .as_mut()
                    .expect("ipath set")[lex_level as usize] += nelements;
            }
        }
    } else if lex_level == 0 && state.npath == 0 {
        // Special case: we should match the entire array. We only need this at
        // the outermost level because at nested levels the match will have been
        // started by the outer field or array element callback.
        state.result_start = Some(lex.token_start);
    }

    Ok(())
}

/// `get_array_end` (jsonfuncs.c:1334).
fn get_array_end(state: &mut GetState, lex: &JsonLexContext) -> PgResult<()> {
    let lex_level = lex.lex_level;

    if lex_level == 0 && state.npath == 0 {
        // Special case: return the entire array.
        let start = state.result_start.expect("result_start set in get_array_start");
        let len = lex.prev_token_terminator - start;
        state.tresult = Some(cstring_to_text_with_len(&lex.input, start, len));
    }

    Ok(())
}

/// `get_array_element_start` (jsonfuncs.c:1352).
fn get_array_element_start(
    state: &mut GetState,
    lex: &JsonLexContext,
    _isnull: bool,
) -> PgResult<()> {
    let mut get_next = false;
    let lex_level = lex.lex_level;

    // Update array element counter.
    // if (lex_level <= npath) array_cur_index[lex_level - 1]++;
    if lex_level <= state.npath {
        state.array_cur_index[(lex_level - 1) as usize] += 1;
    }

    // if (lex_level <= npath && pathok[lex_level - 1] && path_indexes != NULL &&
    //     array_cur_index[lex_level - 1] == path_indexes[lex_level - 1])
    if lex_level <= state.npath
        && state.pathok[(lex_level - 1) as usize]
        && state.path_indexes.is_some()
        && state.array_cur_index[(lex_level - 1) as usize]
            == state.path_indexes.as_ref().expect("ipath set")[(lex_level - 1) as usize]
    {
        if lex_level < state.npath {
            // if not at end of path just mark path ok
            state.pathok[lex_level as usize] = true;
        } else {
            // end of path, so we want this value
            get_next = true;
        }
    }

    // same logic as for objects
    if get_next {
        state.tresult = None;
        state.result_start = None;

        if state.normalize_results && lex.token_type == JsonTokenType::JSON_TOKEN_STRING {
            state.next_scalar = true;
        } else {
            state.result_start = Some(lex.token_start);
        }
    }

    Ok(())
}

/// `get_array_element_end` (jsonfuncs.c:1400).
fn get_array_element_end(
    state: &mut GetState,
    lex: &JsonLexContext,
    isnull: bool,
) -> PgResult<()> {
    let mut get_last = false;
    let lex_level = lex.lex_level;

    // same tests as in get_array_element_start
    if lex_level <= state.npath
        && state.pathok[(lex_level - 1) as usize]
        && state.path_indexes.is_some()
        && state.array_cur_index[(lex_level - 1) as usize]
            == state.path_indexes.as_ref().expect("ipath set")[(lex_level - 1) as usize]
    {
        if lex_level < state.npath {
            // done with this element so reset pathok
            state.pathok[lex_level as usize] = false;
        } else {
            // end of path, so we want this value
            get_last = true;
        }
    }

    // same logic as for objects
    if get_last && state.result_start.is_some() {
        if isnull && state.normalize_results {
            state.tresult = None;
        } else {
            let start = state.result_start.expect("checked is_some");
            let len = lex.prev_token_terminator - start;
            state.tresult = Some(cstring_to_text_with_len(&lex.input, start, len));
        }

        state.result_start = None;
    }

    Ok(())
}

/// `get_scalar` (jsonfuncs.c:1444).
fn get_scalar(
    state: &mut GetState,
    lex: &JsonLexContext,
    token: &[u8],
    tokentype: JsonTokenType,
) -> PgResult<()> {
    let lex_level = lex.lex_level;

    // Check for whole-object match.
    if lex_level == 0 && state.npath == 0 {
        if state.normalize_results && tokentype == JsonTokenType::JSON_TOKEN_STRING {
            // we want the de-escaped string
            state.next_scalar = true;
        } else if state.normalize_results && tokentype == JsonTokenType::JSON_TOKEN_NULL {
            state.tresult = None;
        } else {
            // This is a bit hokey: we will suppress whitespace after the scalar
            // token, but not whitespace before it. Probably not worth doing our
            // own space-skipping to avoid that.
            //
            // start = lex->input (offset 0); len = lex->prev_token_terminator - start;
            let len = lex.prev_token_terminator; // - 0
            state.tresult = Some(cstring_to_text_with_len(&lex.input, 0, len));
        }
    }

    if state.next_scalar {
        // a de-escaped text value is wanted, so supply it.
        // tresult = cstring_to_text(token);
        state.tresult = Some(token.to_vec());
        // make sure the next call to get_scalar doesn't overwrite it
        state.next_scalar = false;
    }

    Ok(())
}

// ===========================================================================
// jsonb path extraction (jsonfuncs.c:1488-1676).
//
// The C `Datum` return + `*isnull` out-parameter is modelled as
// `PgResult<Option<PgVec<u8>>>`: `Ok(None)` is the SQL NULL (`*isnull = true`);
// for the non-text variants the `Some` payload is the serialised jsonb varlena
// (`JsonbValueToJsonb` or, for the empty-path special case, the input `jb`); for
// the text variants it is the rendered text bytes.
// ===========================================================================

/// `jsonb_extract_path` (jsonfuncs.c:1488): `jb #> path`.
pub fn jsonb_extract_path<'mcx>(
    mcx: Mcx<'mcx>,
    jb: &'mcx [u8],
    path: &[Option<Vec<u8>>],
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    get_jsonb_path_all(mcx, jb, path, false)
}

/// `jsonb_extract_path_text` (jsonfuncs.c:1494): `jb #>> path`.
pub fn jsonb_extract_path_text<'mcx>(
    mcx: Mcx<'mcx>,
    jb: &'mcx [u8],
    path: &[Option<Vec<u8>>],
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    get_jsonb_path_all(mcx, jb, path, true)
}

/// `get_jsonb_path_all` (jsonfuncs.c:1499): common routine for the
/// `jsonb_extract_path[_text]` functions.
fn get_jsonb_path_all<'mcx>(
    mcx: Mcx<'mcx>,
    jb: &'mcx [u8],
    path: &[Option<Vec<u8>>],
    as_text: bool,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // If the array contains any null elements, return NULL ... (see get_path_all).
    // if (array_contains_nulls(path)) PG_RETURN_NULL();
    if path.iter().any(|e| e.is_none()) {
        return Ok(None);
    }

    // res = jsonb_get_element(jb, pathtext, npath, &isnull, as_text);
    // if (isnull) PG_RETURN_NULL(); else PG_RETURN_DATUM(res);
    let path_bytes: Vec<&[u8]> = path
        .iter()
        .map(|e| e.as_ref().expect("array_contains_nulls already checked").as_slice())
        .collect();

    jsonb_get_element(mcx, jb, &path_bytes, as_text)
}

/// `jsonb_get_element` (jsonfuncs.c:1531): the core jsonb path walk used by
/// `jsonb_extract_path[_text]` (and the subscripting executor).
///
/// `path` is the deconstructed `text[]` as one byte slice per element. Returns
/// `Ok(None)` for the C `*isnull = true` path.
pub fn jsonb_get_element<'mcx>(
    mcx: Mcx<'mcx>,
    jb: &'mcx [u8],
    path: &[&[u8]],
    as_text: bool,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let npath = path.len() as i32;
    // JsonbContainer *container = &jb->root;  (a borrow into the 'mcx document)
    let mut container: &'mcx [u8] = vardata_any(jb);
    let mut jbvp: Option<JsonbValue<'mcx>> = None;
    let mut have_object = false;
    let mut have_array = false;

    // *isnull = false;  (modelled by returning a non-None variant)

    // Identify whether we have object, array, or scalar at top-level.
    if jb_root_is_object(jb) {
        have_object = true;
    } else if jb_root_is_array(jb) && !jb_root_is_scalar(jb) {
        have_array = true;
    } else {
        // Assert(JB_ROOT_IS_ARRAY(jb) && JB_ROOT_IS_SCALAR(jb));
        // Extract the scalar value, if it is what we'll return.
        if npath <= 0 {
            jbvp = getIthJsonbValueFromContainer(container, 0)?;
        }
    }

    // If the array is empty, return the entire LHS object, on the grounds that we
    // should do zero field or element extractions. For the non-scalar case we can
    // just hand back the object without much work. For the scalar case, fall
    // through and deal with the value below the loop.
    //
    // if (npath <= 0 && jbvp == NULL) { ... }
    if npath <= 0 && jbvp.is_none() {
        if as_text {
            // return PointerGetDatum(cstring_to_text(JsonbToCString(NULL,
            //                          container, VARSIZE(jb))));
            return Ok(Some(JsonbToCString(mcx, container, varsize_jsonb(jb))?));
        } else {
            // not text mode - just hand back the jsonb
            return Ok(Some(slice_to_pgvec(mcx, jb)?));
        }
    }

    for i in 0..npath as usize {
        if have_object {
            // text *subscr = DatumGetTextPP(path[i]);
            // jbvp = getKeyJsonValueFromContainer(container, VARDATA_ANY(subscr),
            //                                     VARSIZE_ANY_EXHDR(subscr), NULL);
            let subscr = path[i];
            jbvp = getKeyJsonValueFromContainer(container, subscr)?;
        } else if have_array {
            // char *indextext = TextDatumGetCString(path[i]);
            let indextext = path[i];
            // errno = 0; lindex = strtoint(indextext, &endptr, 10);
            // if (endptr == indextext || *endptr != '\0' || errno != 0)
            //     { *isnull = true; return PointerGetDatum(NULL); }
            let lindex = match parse_full_i32(indextext) {
                Some(v) => v,
                None => return Ok(None),
            };

            let index: u32 = if lindex >= 0 {
                // index = (uint32) lindex;
                lindex as u32
            } else {
                // Handle negative subscript.
                // Container must be array, but make sure.
                // if (!JsonContainerIsArray(container)) elog(ERROR, "not a jsonb array");
                if !json_container_is_array(container_header(container)) {
                    return Err(elog_error("not a jsonb array"));
                }
                // nelements = JsonContainerSize(container);
                let nelements = json_container_size(container_header(container));
                // if (lindex == INT_MIN || -lindex > nelements)
                //     { *isnull = true; return PointerGetDatum(NULL); }
                // else index = nelements + lindex;
                if lindex == i32::MIN || pg_abs_s32(lindex) > nelements {
                    return Ok(None);
                } else {
                    (nelements as i64 + lindex as i64) as u32
                }
            };

            // jbvp = getIthJsonbValueFromContainer(container, index);
            jbvp = getIthJsonbValueFromContainer(container, index)?;
        } else {
            // scalar, extraction yields a null
            // *isnull = true; return PointerGetDatum(NULL);
            return Ok(None);
        }

        // if (jbvp == NULL) { *isnull = true; return PointerGetDatum(NULL); }
        // else if (i == npath - 1) break;
        let current_type = match &jbvp {
            None => return Ok(None),
            Some(v) => v.typ,
        };

        if i == npath as usize - 1 {
            break;
        }

        // if (jbvp->type == jbvBinary) { container = jbvp->val.binary.data; ... }
        // else { Assert(IsAJsonbScalar(jbvp)); have_object = false; have_array = false; }
        if current_type == jbvType::jbvBinary {
            // container = jbvp->val.binary.data;  (binary.data is &'mcx, Copy)
            let data: &'mcx [u8] = match &jbvp.as_ref().expect("jbvp must be set").val {
                JsonbValueData::Binary { data, .. } => data,
                _ => unreachable!("jbvBinary payload"),
            };
            container = data;
            have_object = json_container_is_object(container_header(container));
            have_array = json_container_is_array(container_header(container));
            // Assert(!JsonContainerIsScalar(container));
            debug_assert!(!json_container_is_scalar(container_header(container)));
        } else {
            // Assert(IsAJsonbScalar(jbvp));
            debug_assert!(is_a_jsonb_scalar(current_type));
            have_object = false;
            have_array = false;
        }
    }

    let final_v = jbvp.expect("loop guarantees jbvp is set on break");

    if as_text {
        // if (jbvp->type == jbvNull) { *isnull = true; return PointerGetDatum(NULL); }
        if final_v.typ == jbvType::jbvNull {
            return Ok(None);
        }
        // return PointerGetDatum(JsonbValueAsText(jbvp));
        JsonbValueAsText(mcx, &final_v)
    } else {
        // Jsonb *res = JsonbValueToJsonb(jbvp); PG_RETURN_JSONB_P(res);
        Ok(Some(JsonbValueToJsonb(mcx, &final_v)?))
    }
}

// ===========================================================================
// Local helpers (mirrors of inline C idioms used above).
// ===========================================================================

/// `cstring_to_text_with_len(start, len)` over the lexer's input buffer: the C
/// pointer arithmetic `cstring_to_text_with_len(p, q - p)` becomes a slice copy
/// of `input[start .. start + len]`.
#[inline]
fn cstring_to_text_with_len(input: &[u8], start: usize, len: usize) -> Vec<u8> {
    input[start..start + len].to_vec()
}

/// Copy `src` into a `PgVec` charged to the caller's context (the C
/// `cstring_to_text*` palloc that yields the returned `text`).
fn slice_to_pgvec<'mcx>(mcx: Mcx<'mcx>, src: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let mut out = ::mcx::vec_with_capacity_in::<u8>(mcx, src.len())?;
    out.extend_from_slice(src);
    Ok(out)
}

/// `strcmp(fname, path_names[idx]) == 0`, also covering the C guards
/// `path_names != NULL && path_names[idx] != NULL` (jsonfuncs.c:1205-1207).
fn path_name_matches(path_names: &Option<Vec<Option<Vec<u8>>>>, idx: i32, fname: &[u8]) -> bool {
    match path_names {
        None => false,
        Some(names) => match &names[idx as usize] {
            None => false,
            Some(name) => &name[..] == fname,
        },
    }
}

/// `strtoint(s, &endptr, 10)` with full consumption + `errno`/`endptr` checks:
/// parse the whole byte string as a base-10 `int32`, returning `None` for an
/// empty/partial/overflowing token (the C `endptr == s || *endptr != '\0' ||
/// errno != 0` failure that maps to `INT_MIN`/`*isnull = true`).
fn parse_full_i32(s: &[u8]) -> Option<i32> {
    // C strtol skips leading C-locale isspace: space, tab, newline, vertical
    // tab, form feed, carriage return.
    let t = core::str::from_utf8(s).ok()?;
    let t = t.trim_start_matches([' ', '\t', '\n', '\x0B', '\x0C', '\r']);
    // strtol allows leading whitespace, then the callers require the remainder
    // to be empty (`*endptr == '\0'`).
    t.parse::<i32>().ok().filter(|_| !t.is_empty())
}

/// `elog(ERROR, msg)`: an internal error with no SQLSTATE override.
fn elog_error(msg: &str) -> ::types_error::PgError {
    ereport(ERROR).errmsg_internal(msg).into_error()
}
