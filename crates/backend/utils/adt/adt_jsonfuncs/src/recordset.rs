//! `json[b]_populate_recordset` / `json[b]_to_recordset` set-returning
//! functions (jsonfuncs.c:3973-4212): build a *set* of composite values (one
//! tuple per top-level array element) from a json/jsonb array of objects.
//!
//! All four SQL entry points funnel into [`populate_recordset_worker`], which
//! (mirroring `populate.rs`'s `populate_record_worker`) resolves the result row
//! type, reads the optional record argument, then drives one
//! [`populate_recordset_record`] per array element — the json (text) path
//! through the seven `populate_recordset_*` SAX callbacks over `common/jsonapi.c`,
//! the jsonb (binary) path through the landed `jsonb_util.c` iterator.
//!
//! Each produced tuple comes from the sibling `populate.rs`
//! [`populate_record`]; rather than C's `tuplestore_puttuple`, the repo
//! materialize-mode SRF path deforms the formed tuple back to its
//! `(values, nulls)` columns and appends them through the funcapi
//! `materialized_srf_putvalues` seam (the same path `each.rs` / `keys.rs` use).
//!
//! As in `populate.rs`, the trimmed owned call frame has no `fn_extra` slot, so
//! the per-query [`PopulateRecordCacheLocal`] is rebuilt each call (a
//! behaviour-preserving loss of cross-call memoization — every field's IO
//! metadata is re-derived, exactly as the C first-call path does).

#![allow(non_snake_case)]

extern crate alloc;

use core::cell::RefCell;

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::rc::Rc;
use alloc::vec::Vec;

use mcx::Mcx;

use utils_error::ereport;
use types_error::error::{ERRCODE_INVALID_PARAMETER_VALUE, ERROR};
use types_error::{PgError, PgResult};

use types_json::{JsonLexContext, JsonParseErrorType, JsonSemAction, JsonTokenType};
use types_jsonb::jsonb_util::{JsonbValue, JsonbValueData};
use types_jsonb::jsonb::{
    jbvType, json_container_is_array, json_container_is_object, json_container_is_scalar,
    JsonbIteratorToken,
};
use types_jsonfuncs::{
    ColumnIOUnion, JsonHashEntry, TypeCat, NAMEDATALEN,
};
use ::nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::heaptuple::{HeapTupleHeaderGetTypMod, HeapTupleHeaderGetTypeId, RECORDOID};
use types_tuple::heaptuple::FormedTuple;
use types_tuple::Datum;

use heaptuple::{heap_deform_tuple, HeapTupleGetDatum};
use jsonb_util::{JsonbIteratorInit, JsonbIteratorNext};
use funcapi_seams as funcapi;

use crate::populate::{
    get_record_type_from_argument, get_record_type_from_query, populate_record,
    update_cached_tupdesc, JsObjectW, PopulateRecordCacheLocal,
};

// ===========================================================================
// SQL entry points (jsonfuncs.c:3973-3999).
// ===========================================================================

/// `jsonb_populate_recordset(PG_FUNCTION_ARGS)` (jsonfuncs.c:3974):
/// `populate_recordset_worker(fcinfo, "jsonb_populate_recordset", false, true)`.
pub fn jsonb_populate_recordset<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &'mcx mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    populate_recordset_worker(mcx, fcinfo, "jsonb_populate_recordset", false, true)
}

/// `jsonb_to_recordset(PG_FUNCTION_ARGS)` (jsonfuncs.c:3981):
/// `populate_recordset_worker(fcinfo, "jsonb_to_recordset", false, false)`.
pub fn jsonb_to_recordset<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &'mcx mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    populate_recordset_worker(mcx, fcinfo, "jsonb_to_recordset", false, false)
}

/// `json_populate_recordset(PG_FUNCTION_ARGS)` (jsonfuncs.c:3988):
/// `populate_recordset_worker(fcinfo, "json_populate_recordset", true, true)`.
pub fn json_populate_recordset<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &'mcx mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    populate_recordset_worker(mcx, fcinfo, "json_populate_recordset", true, true)
}

/// `json_to_recordset(PG_FUNCTION_ARGS)` (jsonfuncs.c:3995):
/// `populate_recordset_worker(fcinfo, "json_to_recordset", true, false)`.
pub fn json_to_recordset<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &'mcx mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    populate_recordset_worker(mcx, fcinfo, "json_to_recordset", true, false)
}

// ===========================================================================
// PopulateRecordsetState (jsonfuncs.c:245-256).
//
// In C the SAX callbacks are driven over `void *state`; here the shared state is
// an `Rc<RefCell<PopulateRecordsetState>>` closed over by each closure. The C
// `lex` is handed to each callback by the parse driver; `tuple_store` /
// `ret_tdesc` are the SRF output, marshalled out of `state` by the entry point
// (so the produced tuples are collected into `result`). `cache` is borrowed
// mutably here (held outside the state and threaded into
// `populate_recordset_record`).
// ===========================================================================

/// C: `struct PopulateRecordsetState` (jsonfuncs.c:245), SAX-callback running
/// state for the json (text) path.
struct PopulateRecordsetState<'mcx> {
    /// `const char *function_name`.
    function_name: &'static str,
    /// `HTAB *json_hash` — the field map for the current level-1 object
    /// (`None` == C NULL: not inside a level-1 object).
    json_hash: Option<BTreeMap<Vec<u8>, JsonHashEntry>>,
    /// `char *saved_scalar` — the most recent level-2 scalar token.
    saved_scalar: Option<Vec<u8>>,
    /// `const char *save_json_start` — offset of the start of the current
    /// subobject text (`None` == scalar).
    save_json_start: Option<usize>,
    /// `JsonTokenType saved_token_type`.
    saved_token_type: JsonTokenType,
    /// `HeapTupleHeader rec` — the optional seed record argument
    /// (`json{b}_populate_recordset`); `None` for the `to_recordset` variants.
    rec: Option<FormedTuple<'mcx>>,
    /// The formed tuples produced so far, in document order (the C
    /// `tuplestore_puttuple` stream).
    result: Vec<FormedTuple<'mcx>>,
    /// Any error raised by `populate_recordset_record` (deferred out of the
    /// `&mut self` SAX callbacks so it can propagate from the worker).
    pending_error: Option<PgError>,
}

// ===========================================================================
// populate_recordset_record (jsonfuncs.c:4002).
// ===========================================================================

/// `populate_recordset_record` (jsonfuncs.c:4002): build one tuple from the
/// current object `obj` and append it to the result stream.
///
/// C builds a `HeapTupleData` around the formed header and calls
/// `tuplestore_puttuple`; here the formed tuple is collected and later deformed
/// + appended through `materialized_srf_putvalues` (see
/// [`put_recordset_tuple`]).
fn populate_recordset_record<'mcx>(
    mcx: Mcx<'mcx>,
    cache: &mut PopulateRecordCacheLocal<'mcx>,
    rec: Option<&FormedTuple<'mcx>>,
    obj: &JsObjectW<'_>,
    result: &mut Vec<FormedTuple<'mcx>>,
) -> PgResult<()> {
    // acquire/update cached tuple descriptor
    let typcat = cache.c.typcat;
    let argtype = cache.argtype;
    let io = match &mut cache.c.io {
        ColumnIOUnion::Composite(c) => c,
        _ => {
            return Err(ereport(ERROR)
                .errmsg_internal("populate_recordset_record: cache.c is not composite")
                .into_error())
        }
    };
    update_cached_tupdesc(mcx, io)?;

    // replace record fields from json
    // tuphead = populate_record(tupdesc, &record_io, state->rec, fn_mcxt, obj, NULL);
    let tupdesc_box = io
        .tupdesc
        .as_ref()
        .expect("tupdesc cached")
        .clone_in(mcx)?;
    let record_io = io.record_io.take();
    let (tuple, new_record_io) =
        populate_record(mcx, &tupdesc_box, record_io, rec, obj, None)?;
    io.record_io = new_record_io;

    // if it's domain over composite, check domain constraints
    // domain_check_safe(HeapTupleHeaderGetDatum(tuphead), false, argtype, ..., NULL);
    if typcat == TypeCat::CompositeDomain {
        let datum = HeapTupleGetDatum(mcx, &tuple, &tupdesc_box)?;
        // C passes escontext = NULL, so a violation raises (no soft capture).
        misc2::domains::domain_check(mcx, &datum, false, argtype)?;
    }

    // ok, save into tuplestore (collected here; deformed + put by the worker)
    result.push(tuple);
    Ok(())
}

// ===========================================================================
// populate_recordset_worker (jsonfuncs.c:4041).
// ===========================================================================

/// `populate_recordset_worker` (jsonfuncs.c:4041): common worker for
/// `json{b}_populate_recordset()` and `json{b}_to_recordset()`.
fn populate_recordset_worker<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &'mcx mut FunctionCallInfoBaseData<'mcx>,
    funcname: &'static str,
    is_json: bool,
    have_record_arg: bool,
) -> PgResult<Datum<'mcx>> {
    let json_arg_num = if have_record_arg { 1 } else { 0 };

    // First time through (no fn_extra cache in the trimmed frame): identify the
    // input/result record type. (See PopulateRecordCacheLocal.)
    let mut cache = PopulateRecordCacheLocal {
        argtype: types_core::InvalidOid,
        c: types_jsonfuncs::ColumnIOData::default(),
    };

    // The result-type resolution + arg reads need a `'mcx` shared view of the
    // call frame (the `get_record_type_from_query` -> `get_call_result_type` ->
    // `fn_oid_and_expr` chain hands back a `&'mcx` `fn_expr` node that lives in
    // the call arena), while the final SRF output re-borrows the frame mutably.
    //
    // SAFETY: `fcinfo_ref` aliases `*fcinfo` as a shared `'mcx` view used ONLY
    // for the read-only resolution + argument reads below; it is dead before the
    // `&mut fcinfo` SRF output at the end of this function (the resolution +
    // reads produce owned values — the cache, the `rec` `FormedTuple`, the input
    // bytes — that do not borrow the frame), so the shared and exclusive
    // accesses never overlap in time. This mirrors the same trimmed-call-frame
    // boundary `funcapi::InitMaterializedSRF` itself resolves with this idiom.
    let fcinfo_ref: &'mcx FunctionCallInfoBaseData<'mcx> =
        unsafe { &*(fcinfo as *const FunctionCallInfoBaseData<'mcx>) };

    if have_record_arg {
        get_record_type_from_argument(mcx, fcinfo_ref, funcname, &mut cache)?;
    } else {
        get_record_type_from_query(mcx, fcinfo_ref, funcname, &mut cache)?;
    }

    // Collect record arg if we have one.
    let rec: Option<FormedTuple<'mcx>>;
    if !have_record_arg {
        rec = None; // it's json{b}_to_recordset()
    } else if !fmgr_seams::pg_argisnull::call(fcinfo_ref, 0) {
        // rec = PG_GETARG_HEAPTUPLEHEADER(0);
        let r = funcapi::srf_arg_record::call(mcx, fcinfo_ref, 0)?;

        // When declared arg type is RECORD, identify actual record type from
        // the tuple itself.
        if cache.argtype == RECORDOID {
            if let ColumnIOUnion::Composite(c) = &mut cache.c.io {
                c.base_typid = HeapTupleHeaderGetTypeId(r.tuple.t_data.as_ref().unwrap());
                c.base_typmod = HeapTupleHeaderGetTypMod(r.tuple.t_data.as_ref().unwrap());
            }
        }
        rec = Some(r);
    } else {
        rec = None;
        // When declared arg type is RECORD, identify from the calling query.
        if cache.argtype == RECORDOID {
            get_record_type_from_query(mcx, fcinfo_ref, funcname, &mut cache)?;
            // This can't change argtype, which is important for next time.
            debug_assert!(cache.argtype == RECORDOID);
        }
    }

    // Forcibly update the cached tupdesc, to ensure we have the right tupdesc to
    // return even if the JSON contains no rows. (C jsonfuncs.c:4117 — done
    // before the tuplestore is created, after the null-json short-circuit; we
    // hoist it ahead of the null check so the materialize-mode `ReturnSetInfo`
    // can be set up with the resolved descriptor in all cases — see below.)
    {
        let io = match &mut cache.c.io {
            ColumnIOUnion::Composite(c) => c,
            _ => {
                return Err(ereport(ERROR)
                    .errmsg_internal("populate_recordset_worker: cache.c is not composite")
                    .into_error())
            }
        };
        update_cached_tupdesc(mcx, io)?;
    }

    // The result row type is the INPUT-RECORD's cached composite tupdesc (C
    // jsonfuncs.c:4204-4205: `rsi->setDesc = CreateTupleDescCopy(
    // cache->c.io.composite.tupdesc)`), NOT the query's column-def-list. Set up
    // the materialize-mode `ReturnSetInfo` here — the rsinfo validity /
    // SFRM_Materialize checks, the `tuplestore_begin_heap`, `returnMode`, and
    // `setResult`/`setDesc` block C runs by hand (4051-4066, 4127-4132,
    // 4204-4205). Crucially this does NOT route through
    // `InitMaterializedSRF`/`get_call_result_type`, which would reject the
    // RECORD result type ("return type must be a row type") that
    // `json_populate_recordset(row(...), ...)` and the column-definition-list
    // forms legitimately use — the row type comes from the resolved cache, not
    // from the call result type.
    let setdesc_box = {
        let io = match &cache.c.io {
            ColumnIOUnion::Composite(c) => c,
            _ => unreachable!("cache.c is composite"),
        };
        io.tupdesc.as_ref().expect("tupdesc cached").clone_in(mcx)?
    };
    funcapi::init_materialized_srf_with_desc::call(
        fcinfo,
        Some(mcx::alloc_in(mcx, setdesc_box)?),
    )?;

    // if the json is null send back an empty set (the materialize-mode store is
    // already initialized above, so this yields a valid empty result).
    if fmgr_seams::pg_argisnull::call(fcinfo_ref, json_arg_num) {
        return Ok(Datum::null());
    }

    // The formed tuples produced by each level-1 object / array element. (C
    // streams these straight into state->tuple_store via tuplestore_puttuple;
    // the repo materialize path deforms + putvalues them after the walk.)
    let mut result: Vec<FormedTuple<'mcx>> = Vec::new();

    if is_json {
        // text *json = PG_GETARG_TEXT_PP(json_arg_num);
        // The seam yields the header-ful varlena image; the json (text) document
        // is its VARDATA (skip the 4-byte length word), as C reads via
        // VARDATA_ANY.
        let json_image = funcapi::srf_arg_varlena_bytes::call(mcx, fcinfo_ref, json_arg_num)?;
        let json = crate::common::vardata_any(&json_image);

        let state = Rc::new(RefCell::new(PopulateRecordsetState {
            function_name: funcname,
            json_hash: None,
            saved_scalar: None,
            save_json_start: None,
            saved_token_type: JsonTokenType::JSON_TOKEN_INVALID,
            rec,
            result: Vec::new(),
            pending_error: None,
        }));

        // The cache is shared into the object_end callback (where
        // populate_recordset_record runs). RefCell because the SAX callbacks
        // and the cache reference are distinct borrow regions.
        let cache_cell = Rc::new(RefCell::new(cache));

        let mut sem = JsonSemAction::default();
        // sem->array_start = populate_recordset_array_start;
        sem.array_start = Some(Box::new(|_lex: &JsonLexContext| Ok(())));
        // sem->array_element_start = populate_recordset_array_element_start;
        {
            let state = Rc::clone(&state);
            sem.array_element_start =
                Some(Box::new(move |lex: &JsonLexContext, _isnull: bool| {
                    populate_recordset_array_element_start(&mut state.borrow_mut(), lex)
                }));
        }
        // sem->scalar = populate_recordset_scalar;
        {
            let state = Rc::clone(&state);
            sem.scalar = Some(Box::new(
                move |lex: &JsonLexContext, token: &[u8], _tokentype: JsonTokenType| {
                    populate_recordset_scalar(&mut state.borrow_mut(), lex, token)
                },
            ));
        }
        // sem->object_field_start = populate_recordset_object_field_start;
        {
            let state = Rc::clone(&state);
            sem.object_field_start = Some(Box::new(
                move |lex: &JsonLexContext, _fname: &[u8], _isnull: bool| {
                    populate_recordset_object_field_start(&mut state.borrow_mut(), lex);
                    Ok(())
                },
            ));
        }
        // sem->object_field_end = populate_recordset_object_field_end;
        {
            let state = Rc::clone(&state);
            sem.object_field_end = Some(Box::new(
                move |lex: &JsonLexContext, fname: &[u8], isnull: bool| {
                    populate_recordset_object_field_end(&mut state.borrow_mut(), lex, fname, isnull);
                    Ok(())
                },
            ));
        }
        // sem->object_start = populate_recordset_object_start;
        {
            let state = Rc::clone(&state);
            sem.object_start = Some(Box::new(move |lex: &JsonLexContext| {
                populate_recordset_object_start(&mut state.borrow_mut(), lex)
            }));
        }
        // sem->object_end = populate_recordset_object_end;
        {
            let state = Rc::clone(&state);
            let cache_cell = Rc::clone(&cache_cell);
            sem.object_end = Some(Box::new(move |lex: &JsonLexContext| {
                let mut st = state.borrow_mut();
                let mut cache = cache_cell.borrow_mut();
                populate_recordset_object_end(mcx, &mut st, &mut cache, lex)
            }));
        }

        // pg_parse_json_or_ereport(&lex, sem);
        let encoding = jsonapi_seams::get_database_encoding::call();
        let parse = jsonapi_seams::pg_parse_json::call(json, encoding, true, &mut sem);
        drop(sem);

        let state = Rc::try_unwrap(state)
            .ok()
            .expect("all SAX closures dropped")
            .into_inner();
        cache = Rc::try_unwrap(cache_cell)
            .ok()
            .expect("cache closure dropped")
            .into_inner();

        // A populate_recordset_record error deferred out of object_end.
        if let Some(e) = state.pending_error {
            return Err(e);
        }

        match parse {
            Ok(JsonParseErrorType::JSON_SUCCESS) => {}
            Ok(res) => {
                // pg_parse_json_or_ereport: a parse failure raises.
                jsonapi_seams::errsave_error::call(res, json, true, None)?;
                unreachable!("errsave_error with no escontext raises");
            }
            Err(e) => return Err(e),
        }

        result = state.result;
    } else {
        // Jsonb *jb = PG_GETARG_JSONB_P(json_arg_num);
        let jb = funcapi::srf_arg_varlena_bytes::call(mcx, fcinfo_ref, json_arg_num)?;
        let root = crate::common::vardata_any(&jb);
        let header = u32::from_ne_bytes([root[0], root[1], root[2], root[3]]);

        // if (JB_ROOT_IS_SCALAR(jb) || !JB_ROOT_IS_ARRAY(jb)) ereport("cannot call %s on a non-array")
        if json_container_is_scalar(header) || !json_container_is_array(header) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(alloc::format!("cannot call {} on a non-array", funcname))
                .into_error());
        }

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

            if r == JsonbIteratorToken::WJB_ELEM {
                // if (v.type != jbvBinary || !JsonContainerIsObject(v.val.binary.data))
                //     ereport("argument of %s must be an array of objects")
                let cont: &[u8] = match (v.typ, &v.val) {
                    (jbvType::jbvBinary, JsonbValueData::Binary { data, .. })
                        if json_container_is_object(u32::from_ne_bytes([
                            data[0], data[1], data[2], data[3],
                        ])) =>
                    {
                        data
                    }
                    _ => {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                            .errmsg(alloc::format!(
                                "argument of {} must be an array of objects",
                                funcname
                            ))
                            .into_error());
                    }
                };

                // obj.is_json = false; obj.val.jsonb_cont = v.val.binary.data;
                let obj = JsObjectW::JsonbCont(Some(cont));
                populate_recordset_record(mcx, &mut cache, rec.as_ref(), &obj, &mut result)?;
            }
        }
    }

    // C jsonfuncs.c:4204-4205 (`rsi->setResult`/`rsi->setDesc`) is already done:
    // the materialize-mode store and `setDesc` (the INPUT-RECORD's cached
    // composite tupdesc) were set up by `init_materialized_srf_with_desc` above.
    // We still need a local copy of that descriptor to deform the formed tuples
    // against the type they were built with (no value crosses a
    // by-value/by-reference boundary). When the supplied record's type
    // disagrees with the query's column definitions, the executor's
    // `tupledesc_match(expectedDesc, setDesc)` (execSRF) raises
    // "function return row and query-specified return row do not match" — the
    // user-facing error for the wrong-record-type negative cases.
    let tupdesc_box = {
        let io = match &cache.c.io {
            ColumnIOUnion::Composite(c) => c,
            _ => unreachable!("cache.c is composite"),
        };
        io.tupdesc.as_ref().expect("tupdesc cached").clone_in(mcx)?
    };

    for tuple in &result {
        put_recordset_tuple(mcx, fcinfo, &tupdesc_box, tuple)?;
    }

    // PG_RETURN_NULL();
    Ok(Datum::null())
}

/// Append one collected tuple to the materialize-mode SRF output. C streams the
/// formed `HeapTupleHeader` straight into the tuplestore via
/// `tuplestore_puttuple`; the repo materialize path takes `(values, nulls)`, so
/// the tuple is deformed first (a behaviour-preserving round-trip — the same
/// columns end up in the same `setDesc` row).
fn put_recordset_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
    tupdesc: &types_tuple::heaptuple::TupleDescData<'mcx>,
    tuple: &FormedTuple<'mcx>,
) -> PgResult<()> {
    let deformed = heap_deform_tuple(mcx, &tuple.tuple, tupdesc, &tuple.data)?;
    let mut values: Vec<Datum<'mcx>> = Vec::with_capacity(deformed.len());
    let mut nulls: Vec<bool> = Vec::with_capacity(deformed.len());
    for (d, n) in deformed.into_iter() {
        values.push(d);
        nulls.push(n);
    }

    let rsi = fcinfo
        .resultinfo
        .as_mut()
        .expect("InitMaterializedSRF set fcinfo->resultinfo");
    funcapi::materialized_srf_putvalues::call(rsi, &values, &nulls)
}

// ===========================================================================
// The seven populate_recordset_* SAX callbacks (jsonfuncs.c:4214-4378).
// ===========================================================================

/// `populate_recordset_object_start` (jsonfuncs.c:4214).
fn populate_recordset_object_start(
    state: &mut PopulateRecordsetState<'_>,
    lex: &JsonLexContext,
) -> PgResult<()> {
    let lex_level = lex.lex_level;

    // Reject object at top level: we must have an array at level 0.
    if lex_level == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(alloc::format!("cannot call {} on an object", state.function_name))
            .into_error());
    }

    // Nested objects require no special processing.
    if lex_level > 1 {
        return Ok(());
    }

    // Object at level 1: set up a new hash table for this object.
    state.json_hash = Some(BTreeMap::new());

    Ok(())
}

/// `populate_recordset_object_end` (jsonfuncs.c:4244).
fn populate_recordset_object_end<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut PopulateRecordsetState<'mcx>,
    cache: &mut PopulateRecordCacheLocal<'mcx>,
    lex: &JsonLexContext,
) -> PgResult<()> {
    // Nested objects require no special processing.
    if lex.lex_level > 1 {
        return Ok(());
    }

    // obj.is_json = true; obj.val.json_hash = _state->json_hash;
    let json_hash = state.json_hash.take();
    let obj = JsObjectW::JsonHash(json_hash);

    // Split-borrow the running fields so `rec` (shared) and `result` (mutable)
    // can be passed together.
    let PopulateRecordsetState {
        rec,
        result,
        pending_error,
        ..
    } = state;

    // Construct and return a tuple based on this level-1 object. A raised error
    // is deferred into the state so it propagates from the worker (the parse
    // driver swallows a non-Ok into a soft/SAX-failed return otherwise).
    if let Err(e) = populate_recordset_record(mcx, cache, rec.as_ref(), &obj, result) {
        *pending_error = Some(e.clone());
        return Err(e);
    }

    // Done with hash for this object: json_hash = NULL (already taken above).
    Ok(())
}

/// `populate_recordset_array_element_start` (jsonfuncs.c:4267).
fn populate_recordset_array_element_start(
    state: &mut PopulateRecordsetState<'_>,
    lex: &JsonLexContext,
) -> PgResult<()> {
    if lex.lex_level == 1 && lex.token_type != JsonTokenType::JSON_TOKEN_OBJECT_START {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(alloc::format!(
                "argument of {} must be an array of objects",
                state.function_name
            ))
            .into_error());
    }
    Ok(())
}

/// `populate_recordset_scalar` (jsonfuncs.c:4289).
fn populate_recordset_scalar(
    state: &mut PopulateRecordsetState<'_>,
    lex: &JsonLexContext,
    token: &[u8],
) -> PgResult<()> {
    if lex.lex_level == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(alloc::format!("cannot call {} on a scalar", state.function_name))
            .into_error());
    }

    if lex.lex_level == 2 {
        state.saved_scalar = Some(token.to_vec());
    }

    Ok(())
}

/// `populate_recordset_object_field_start` (jsonfuncs.c:4306).
fn populate_recordset_object_field_start(
    state: &mut PopulateRecordsetState<'_>,
    lex: &JsonLexContext,
) {
    if lex.lex_level > 2 {
        return;
    }

    state.saved_token_type = lex.token_type;

    if lex.token_type == JsonTokenType::JSON_TOKEN_ARRAY_START
        || lex.token_type == JsonTokenType::JSON_TOKEN_OBJECT_START
    {
        state.save_json_start = Some(lex.token_start);
    } else {
        state.save_json_start = None;
    }
}

/// `populate_recordset_object_field_end` (jsonfuncs.c:4329).
fn populate_recordset_object_field_end(
    state: &mut PopulateRecordsetState<'_>,
    lex: &JsonLexContext,
    fname: &[u8],
    isnull: bool,
) {
    // Ignore nested fields.
    if lex.lex_level > 2 {
        return;
    }

    // Ignore field names >= NAMEDATALEN — they can't match a record field.
    if fname.len() >= NAMEDATALEN {
        return;
    }

    // hashentry = hash_search(json_hash, fname, HASH_ENTER, &found);
    // (found being true == a duplicate; a later field overrides the earlier.)
    let saved_token_type = state.saved_token_type;
    debug_assert!(isnull == (saved_token_type == JsonTokenType::JSON_TOKEN_NULL));

    let val = if let Some(start) = state.save_json_start {
        // len = prev_token_terminator - save_json_start; copy out the subobject.
        let len = lex.prev_token_terminator - start;
        Some(lex.input[start..start + len].to_vec())
    } else {
        // must have had a scalar instead
        state.saved_scalar.clone()
    };

    if let Some(hash) = state.json_hash.as_mut() {
        hash.insert(
            fname.to_vec(),
            JsonHashEntry {
                val,
                type_: saved_token_type,
            },
        );
    }
}
