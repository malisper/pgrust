//! `json[b]_populate_record` / `json[b]_to_record` machinery (jsonfuncs.c:
//! 2464-3960): recursively build a SQL composite value (a `record`) from a
//! json/jsonb document, threading the per-query type-IO metadata cache.
//!
//! The five SQL entry points (`jsonb_populate_record`,
//! `jsonb_populate_record_valid`, `jsonb_to_record`, `json_populate_record`,
//! `json_to_record`) funnel into [`populate_record_worker`], which resolves the
//! result row type, wraps the json/jsonb input in a [`JsValue`], and drives the
//! recursive [`populate_composite`] / [`populate_record`] / [`populate_array`] /
//! [`populate_scalar`] / [`populate_domain`] descent.
//!
//! The text (json) object path decomposes the document into a field hash via
//! [`get_json_object_as_hash`] (the four `hash_*` SAX callbacks over the
//! `common/jsonapi.c` parser); the binary (jsonb) path walks the on-disk
//! container directly through the landed `jsonb_util.c` API. Type input
//! conversion goes through the fmgr input-function seam; arrays are built with
//! the `arrayfuncs.c` `ArrayBuildState` primitives; composite tuple descriptors
//! come from the typcache.
//!
//! The C `JsObject` union holds a `JsonbContainer *` for the binary path; the
//! repo `jsonb_util` field-lookup (`getKeyJsonValueFromContainer`) consumes
//! container *bytes*, so the working object carrier here ([`JsObjectW`]) holds
//! the container as a `&[u8]` slice rather than `types_jsonfuncs::JsObject`'s
//! `Box<JsonbContainer>`.

#![allow(non_snake_case)]

extern crate alloc;

use core::cell::RefCell;

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::rc::Rc;
use alloc::string::String;
use alloc::vec::Vec;

use mcx::Mcx;

use backend_utils_error::ereport;
use types_error::error::{
    ERRCODE_DATATYPE_MISMATCH, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERROR,
};
use types_error::{PgError, PgResult, SoftErrorContext};

use types_json::{JsonLexContext, JsonParseErrorType, JsonSemAction, JsonTokenType};
use types_jsonb::backend_utils_adt_jsonb_util::{JsonbValue, JsonbValueData};
use types_jsonb::jsonb::{
    is_a_jsonb_scalar, jbvType, json_container_is_array, json_container_is_object,
    json_container_is_scalar, json_container_size, JsonbIteratorToken, VARHDRSZ,
};
use types_jsonfuncs::{
    ArrayIOData, CachedFmgrInfo, ColumnIOData, ColumnIOUnion, CompositeIOData, DomainIOData,
    JsValue, JsonHashEntry, RecordIOData, ScalarIOData, TypeCat, NAMEDATALEN,
};
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_nodes::funcapi::TypeFuncClass;
use types_tuple::heaptuple::{
    HeapTupleHeaderGetTypMod, HeapTupleHeaderGetTypeId, TupleDescData, RECORDOID,
};
use types_tuple::backend_access_common_heaptuple::FormedTuple;
use types_tuple::Datum;

use types_fmgr::boundary::FmgrOut;

use backend_access_common_heaptuple::{heap_deform_tuple, heap_form_tuple, HeapTupleGetDatum};
use backend_utils_adt_arrayfuncs::construct::{
    accum_array_result, init_array_result, make_md_array_result,
};
use backend_utils_adt_json::escape_json_with_len;
use backend_utils_adt_jsonb::{JsonbToCString, JsonbUnquote};
use backend_utils_adt_jsonb_util::{
    getKeyJsonValueFromContainer, JsonbIteratorInit, JsonbIteratorNext, JsonbValueToJsonb,
};
use backend_utils_adt_numeric::io::numeric_out;
use backend_utils_cache_lsyscache::type_ as lsyscache_type;
use backend_utils_cache_typcache::lookup_rowtype_tupdesc;
use backend_utils_fmgr_core::{fmgr_info, input_function_call_safe_typed};
use backend_utils_fmgr_funcapi_seams as funcapi;
use types_datum::array_build::ArrayBuildState;

/// `TYPTYPE_DOMAIN` (`pg_type.h`).
const TYPTYPE_DOMAIN: u8 = b'd';
/// `TYPTYPE_COMPOSITE` (`pg_type.h`).
const TYPTYPE_COMPOSITE: u8 = b'c';
/// `JSONOID` (`pg_type.h`).
const JSONOID: u32 = 114;
/// `JSONBOID` (`pg_type.h`).
const JSONBOID: u32 = 3802;

// ===========================================================================
// JsObjectW — the working json/jsonb object carrier (jsonfuncs.c:309 JsObject).
// ===========================================================================

/// C: `struct JsObject` (jsonfuncs.c:309), working form. The binary arm borrows
/// the container bytes; the text arm owns its field map.
pub(crate) enum JsObjectW<'a> {
    /// `HTAB *json_hash` — the field-name -> entry map the text path builds.
    /// `None` is the C NULL pointer (a non-object / failed parse).
    JsonHash(Option<BTreeMap<Vec<u8>, JsonHashEntry>>),
    /// `JsonbContainer *jsonb_cont` — the binary object container bytes.
    /// `None` is the C NULL pointer.
    JsonbCont(Option<&'a [u8]>),
}

impl JsObjectW<'_> {
    /// `JsObjectIsEmpty(jso)` (jsonfuncs.c:329).
    fn is_empty(&self) -> bool {
        match self {
            JsObjectW::JsonHash(h) => match h {
                None => true,
                Some(map) => map.is_empty(),
            },
            JsObjectW::JsonbCont(c) => match c {
                None => true,
                Some(bytes) => json_container_size(container_header(bytes)) == 0,
            },
        }
    }
}

/// Read the leading `JsonbContainer.header` word from container bytes.
#[inline]
fn container_header(c: &[u8]) -> u32 {
    u32::from_ne_bytes([c[0], c[1], c[2], c[3]])
}

/// `JsValueIsNull(jsv)` (jsonfuncs.c:320).
fn js_value_is_null(jsv: &JsValue) -> bool {
    match jsv {
        JsValue::Json { str, type_ } => str.is_none() || *type_ == JsonTokenType::JSON_TOKEN_NULL,
        JsValue::Jsonb(jbv) => match jbv {
            None => true,
            Some(v) => v.typ == jbvType::jbvNull,
        },
    }
}

/// `JsValueIsString(jsv)` (jsonfuncs.c:325).
fn js_value_is_string(jsv: &JsValue) -> bool {
    match jsv {
        JsValue::Json { type_, .. } => *type_ == JsonTokenType::JSON_TOKEN_STRING,
        JsValue::Jsonb(jbv) => match jbv {
            None => false,
            Some(v) => v.typ == jbvType::jbvString,
        },
    }
}

/// `SOFT_ERROR_OCCURRED(escontext)` — whether the soft-error context recorded an
/// error.
fn soft_error_occurred(escontext: &Option<&mut SoftErrorContext>) -> bool {
    match escontext {
        Some(ctx) => ctx.error_occurred(),
        None => false,
    }
}

/// `errsave(escontext, ...)`: if a soft-error context is present, record the
/// error softly and return `Ok(())`; otherwise raise it. (C's `errsave` saves
/// into the `ErrorSaveContext` when one is supplied, else `ereport(ERROR)`.)
fn errsave(escontext: &mut Option<&mut SoftErrorContext>, err: PgError) -> PgResult<()> {
    match escontext {
        Some(ctx) => {
            ctx.save(err);
            Ok(())
        }
        None => Err(err),
    }
}

/// `ereturn(escontext, dummy, ...)`: same soft/hard split as [`errsave`]; the
/// caller supplies the `false` return on the soft branch.
fn ereturn(escontext: &mut Option<&mut SoftErrorContext>, err: PgError) -> PgResult<()> {
    errsave(escontext, err)
}

/// Convert an fmgr [`FmgrOut`] boundary result into the canonical [`Datum`].
fn fmgr_out_to_datum<'mcx>(mcx: Mcx<'mcx>, out: FmgrOut<'mcx>) -> PgResult<Datum<'mcx>> {
    match out {
        FmgrOut::ByVal(d) => Ok(d),
        FmgrOut::Ref(payload) => match payload {
            types_fmgr::boundary::RefPayload::Varlena(b) => {
                let mut v = mcx::vec_with_capacity_in::<u8>(mcx, b.len())?;
                v.extend_from_slice(&b);
                Ok(Datum::ByRef(v))
            }
            types_fmgr::boundary::RefPayload::Cstring(s) => Ok(Datum::Cstring(s)),
            types_fmgr::boundary::RefPayload::Expanded(eo) => Ok(Datum::Expanded(eo)),
        },
    }
}

/// Bridge the canonical [`Datum`] to the bare-word `types_datum::Datum` the
/// `arrayfuncs.c` `ArrayBuildState` primitives consume. A by-value scalar
/// carries its machine word; a by-reference element cannot be addressed as a
/// bare word in the owned model (the arrayfuncs byref-element path resolves the
/// payload through the unported detoast seam and panics loudly there — the same
/// divergence the whole `arrayfuncs` crate already carries, not introduced
/// here), so it is forwarded as a zero word.
fn datum_to_bare_word(d: Datum<'_>) -> types_datum::datum::Datum {
    match d {
        Datum::ByVal(w) => types_datum::datum::Datum::from_usize(w),
        // By-reference: the bare word would be a pointer into palloc'd bytes;
        // arrayfuncs' accum copies through the detoast seam keyed off the word.
        _ => types_datum::datum::Datum::from_usize(0),
    }
}

// ===========================================================================
// PopulateArrayContext (jsonfuncs.c:259).
// ===========================================================================

/// C: `struct PopulateArrayContext` (jsonfuncs.c:259). `astate` is the owned
/// [`ArrayBuildState`]; the build/cache contexts collapse to the caller `Mcx`.
struct PopulateArrayContext<'mcx, 'io, 'es> {
    /// `ArrayBuildState *astate` — the array build state (owned).
    astate: Option<ArrayBuildState>,
    /// `ArrayIOData *aio` — element metadata cache.
    aio: &'io mut ArrayIOData<'mcx>,
    /// `MemoryContext mcxt` — the cache memory context (and `acxt`).
    mcxt: Mcx<'mcx>,
    /// `const char *colname` — for diagnostics only.
    colname: Option<Vec<u8>>,
    /// `int *dims` — dimensions (`-1` == unknown).
    dims: Vec<i32>,
    /// `int *sizes` — current dimension counters.
    sizes: Vec<i32>,
    /// `int ndims` — number of dimensions.
    ndims: i32,
    /// `Node *escontext` — soft-error handling.
    escontext: Option<&'es mut SoftErrorContext>,
}

// ===========================================================================
// Diagnostics + dimension helpers (jsonfuncs.c:2510-2641).
// ===========================================================================

/// `populate_array_report_expected_array` (jsonfuncs.c:2510).
fn populate_array_report_expected_array(
    ctx: &mut PopulateArrayContext<'_, '_, '_>,
    ndim: i32,
) -> PgResult<()> {
    if ndim <= 0 {
        let err = if let Some(colname) = &ctx.colname {
            ereport(ERROR)
                .errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
                .errmsg("expected JSON array")
                .errhint(alloc::format!(
                    "See the value of key \"{}\".",
                    String::from_utf8_lossy(colname)
                ))
                .into_error()
        } else {
            ereport(ERROR)
                .errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
                .errmsg("expected JSON array")
                .into_error()
        };
        return errsave(&mut ctx.escontext, err);
    }

    debug_assert!(ctx.ndims > 0 && ndim < ctx.ndims);
    let mut indices = String::new();
    for i in 0..ndim as usize {
        indices.push_str(&alloc::format!("[{}]", ctx.sizes[i]));
    }

    let err = if let Some(colname) = &ctx.colname {
        ereport(ERROR)
            .errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
            .errmsg("expected JSON array")
            .errhint(alloc::format!(
                "See the array element {} of key \"{}\".",
                indices,
                String::from_utf8_lossy(colname)
            ))
            .into_error()
    } else {
        ereport(ERROR)
            .errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
            .errmsg("expected JSON array")
            .errhint(alloc::format!("See the array element {}.", indices))
            .into_error()
    };
    errsave(&mut ctx.escontext, err)
}

/// `populate_array_assign_ndims` (jsonfuncs.c:2560). `Ok(false)` == soft error.
fn populate_array_assign_ndims(
    ctx: &mut PopulateArrayContext<'_, '_, '_>,
    ndims: i32,
) -> PgResult<bool> {
    debug_assert!(ctx.ndims <= 0);

    if ndims <= 0 {
        populate_array_report_expected_array(ctx, ndims)?;
        return Ok(false);
    }

    ctx.ndims = ndims;
    ctx.dims = alloc::vec![-1; ndims as usize];
    ctx.sizes = alloc::vec![0; ndims as usize];

    Ok(true)
}

/// `populate_array_check_dimension` (jsonfuncs.c:2590). `Ok(false)` == soft.
fn populate_array_check_dimension(
    ctx: &mut PopulateArrayContext<'_, '_, '_>,
    ndim: i32,
) -> PgResult<bool> {
    let dim = ctx.sizes[ndim as usize];

    if ctx.dims[ndim as usize] == -1 {
        ctx.dims[ndim as usize] = dim;
    } else if ctx.dims[ndim as usize] != dim {
        let err = ereport(ERROR)
            .errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
            .errmsg("malformed JSON array")
            .errdetail("Multidimensional arrays must have sub-arrays with matching dimensions.")
            .into_error();
        ereturn(&mut ctx.escontext, err)?;
        return Ok(false);
    }

    ctx.sizes[ndim as usize] = 0;

    if ndim > 0 {
        ctx.sizes[(ndim - 1) as usize] += 1;
    }

    Ok(true)
}

/// `populate_array_element` (jsonfuncs.c:2618). `Ok(false)` == soft error.
fn populate_array_element(
    ctx: &mut PopulateArrayContext<'_, '_, '_>,
    ndim: i32,
    jsv: &JsValue,
) -> PgResult<bool> {
    let mut element_isnull = false;
    let element_type = ctx.aio.element_type;
    let element_typmod = ctx.aio.element_typmod;
    let mcxt = ctx.mcxt;

    let element = populate_record_field(
        mcxt,
        &mut ctx.aio.element_info,
        element_type,
        element_typmod,
        None,
        &Datum::null(),
        jsv,
        &mut element_isnull,
        ctx.escontext.as_deref_mut(),
        false,
    )?;

    if soft_error_occurred(&ctx.escontext) {
        return Ok(false);
    }

    // accumArrayResult(ctx->astate, element, element_isnull, element_type, acxt);
    let astate = ctx.astate.take();
    let dword = datum_to_bare_word(element);
    ctx.astate = Some(accum_array_result(
        mcxt,
        astate,
        dword,
        element_isnull,
        element_type,
    )?);

    debug_assert!(ndim > 0);
    ctx.sizes[(ndim - 1) as usize] += 1;

    Ok(true)
}

// ===========================================================================
// PopulateArrayState + the json (text) array SAX callbacks
// (jsonfuncs.c:273-2781).
//
// The C `PopulateArrayState` carries a back-pointer to the shared
// `PopulateArrayContext` and the per-element running state; here the context is
// shared as `Rc<RefCell<..>>` across the SAX closures, and the element scan
// state is the remaining fields.
// ===========================================================================

/// C: `struct PopulateArrayState` (jsonfuncs.c:273). `lex` is handed to each
/// callback by the parse driver; `ctx` is the shared context.
struct PopulateArrayState<'mcx, 'io, 'es> {
    ctx: PopulateArrayContext<'mcx, 'io, 'es>,
    /// `const char *element_start` — offset of the current element start into
    /// the lexer input (`None` == C NULL).
    element_start: Option<usize>,
    /// `char *element_scalar` — the current scalar element token (`None` == C
    /// NULL).
    element_scalar: Option<Vec<u8>>,
    /// `JsonTokenType element_type`.
    element_type: JsonTokenType,
}

/// `populate_array_object_start` (jsonfuncs.c:2645).
fn populate_array_object_start(
    state: &mut PopulateArrayState<'_, '_, '_>,
    lex: &JsonLexContext,
) -> PgResult<JsonParseErrorType> {
    let ndim = lex.lex_level;

    if state.ctx.ndims <= 0 {
        if !populate_array_assign_ndims(&mut state.ctx, ndim)? {
            return Ok(JsonParseErrorType::JSON_SEM_ACTION_FAILED);
        }
    } else if ndim < state.ctx.ndims {
        populate_array_report_expected_array(&mut state.ctx, ndim)?;
        return Ok(JsonParseErrorType::JSON_SEM_ACTION_FAILED);
    }

    Ok(JsonParseErrorType::JSON_SUCCESS)
}

/// `populate_array_array_end` (jsonfuncs.c:2668).
fn populate_array_array_end(
    state: &mut PopulateArrayState<'_, '_, '_>,
    lex: &JsonLexContext,
) -> PgResult<JsonParseErrorType> {
    let ndim = lex.lex_level;

    if state.ctx.ndims <= 0 {
        if !populate_array_assign_ndims(&mut state.ctx, ndim + 1)? {
            return Ok(JsonParseErrorType::JSON_SEM_ACTION_FAILED);
        }
    }

    if ndim < state.ctx.ndims {
        if !populate_array_check_dimension(&mut state.ctx, ndim)? {
            return Ok(JsonParseErrorType::JSON_SEM_ACTION_FAILED);
        }
    }

    Ok(JsonParseErrorType::JSON_SUCCESS)
}

/// `populate_array_element_start` (jsonfuncs.c:2692).
fn populate_array_element_start(
    state: &mut PopulateArrayState<'_, '_, '_>,
    lex: &JsonLexContext,
    _isnull: bool,
) -> PgResult<JsonParseErrorType> {
    let ndim = lex.lex_level;

    if state.ctx.ndims <= 0 || ndim == state.ctx.ndims {
        // remember current array element start
        state.element_start = Some(lex.token_start);
        state.element_type = lex.token_type;
        state.element_scalar = None;
    }

    Ok(JsonParseErrorType::JSON_SUCCESS)
}

/// `populate_array_element_end` (jsonfuncs.c:2710).
fn populate_array_element_end(
    state: &mut PopulateArrayState<'_, '_, '_>,
    lex: &JsonLexContext,
    isnull: bool,
) -> PgResult<JsonParseErrorType> {
    let ndim = lex.lex_level;

    debug_assert!(state.ctx.ndims > 0);

    if ndim == state.ctx.ndims {
        let jsv = if isnull {
            debug_assert!(state.element_type == JsonTokenType::JSON_TOKEN_NULL);
            // jsv.val.json.str = NULL; len = 0;
            JsValue::Json {
                str: None,
                type_: state.element_type,
            }
        } else if let Some(scalar) = &state.element_scalar {
            // jsv.val.json.str = element_scalar; len = -1 (null-terminated)
            JsValue::Json {
                str: Some(scalar.clone()),
                type_: state.element_type,
            }
        } else {
            // jsv.val.json.str = element_start;
            // len = prev_token_terminator - element_start
            let start = state.element_start.expect("element_start set");
            let len = lex.prev_token_terminator - start;
            JsValue::Json {
                str: Some(lex.input[start..start + len].to_vec()),
                type_: state.element_type,
            }
        };

        if !populate_array_element(&mut state.ctx, ndim, &jsv)? {
            return Ok(JsonParseErrorType::JSON_SEM_ACTION_FAILED);
        }
    }

    Ok(JsonParseErrorType::JSON_SUCCESS)
}

/// `populate_array_scalar` (jsonfuncs.c:2753).
fn populate_array_scalar(
    state: &mut PopulateArrayState<'_, '_, '_>,
    lex: &JsonLexContext,
    token: &[u8],
    tokentype: JsonTokenType,
) -> PgResult<JsonParseErrorType> {
    let ndim = lex.lex_level;

    if state.ctx.ndims <= 0 {
        if !populate_array_assign_ndims(&mut state.ctx, ndim)? {
            return Ok(JsonParseErrorType::JSON_SEM_ACTION_FAILED);
        }
    } else if ndim < state.ctx.ndims {
        populate_array_report_expected_array(&mut state.ctx, ndim)?;
        return Ok(JsonParseErrorType::JSON_SEM_ACTION_FAILED);
    }

    if ndim == state.ctx.ndims {
        // remember the scalar element token
        state.element_scalar = Some(token.to_vec());
        debug_assert!(state.element_type == tokentype);
    }

    Ok(JsonParseErrorType::JSON_SUCCESS)
}

/// `populate_array_json` (jsonfuncs.c:2789): parse a json array and populate the
/// array, driving the five `populate_array_*` SAX callbacks. Returns the
/// (mutated) context. `Ok(false)` == a soft error occurred.
fn populate_array_json<'mcx, 'io, 'es>(
    ctx: PopulateArrayContext<'mcx, 'io, 'es>,
    json: &[u8],
) -> PgResult<(PopulateArrayContext<'mcx, 'io, 'es>, bool)> {
    let state = Rc::new(RefCell::new(PopulateArrayState {
        ctx,
        element_start: None,
        element_scalar: None,
        element_type: JsonTokenType::JSON_TOKEN_INVALID,
    }));

    let mut sem = JsonSemAction::default();
    {
        let state = Rc::clone(&state);
        sem.object_start = Some(Box::new(move |lex: &JsonLexContext| {
            populate_array_object_start(&mut state.borrow_mut(), lex).map(|_| ())
        }));
    }
    {
        let state = Rc::clone(&state);
        sem.array_end = Some(Box::new(move |lex: &JsonLexContext| {
            populate_array_array_end(&mut state.borrow_mut(), lex).map(|_| ())
        }));
    }
    {
        let state = Rc::clone(&state);
        sem.array_element_start = Some(Box::new(move |lex: &JsonLexContext, isnull: bool| {
            populate_array_element_start(&mut state.borrow_mut(), lex, isnull).map(|_| ())
        }));
    }
    {
        let state = Rc::clone(&state);
        sem.array_element_end = Some(Box::new(move |lex: &JsonLexContext, isnull: bool| {
            populate_array_element_end(&mut state.borrow_mut(), lex, isnull).map(|_| ())
        }));
    }
    {
        let state = Rc::clone(&state);
        sem.scalar = Some(Box::new(
            move |lex: &JsonLexContext, token: &[u8], tokentype: JsonTokenType| {
                populate_array_scalar(&mut state.borrow_mut(), lex, token, tokentype).map(|_| ())
            },
        ));
    }

    // pg_parse_json_or_errsave(state.lex, &sem, ctx->escontext):
    // A parse failure or a softly-reported SAX failure both surface as a
    // recorded soft error; the result-Ok path means parse succeeded.
    let encoding = common_jsonapi_seams::get_database_encoding::call();
    let parse = common_jsonapi_seams::pg_parse_json::call(json, encoding, true, &mut sem);
    drop(sem);

    let mut state = Rc::try_unwrap(state)
        .ok()
        .expect("all SAX closures dropped")
        .into_inner();

    match parse {
        Ok(JsonParseErrorType::JSON_SUCCESS) => {
            debug_assert!(state.ctx.ndims > 0 && !state.ctx.dims.is_empty());
        }
        Ok(result) => {
            // pg_parse_json_or_errsave reports the parse failure into escontext.
            common_jsonapi_seams::errsave_error::call(result, json)
                .or_else(|e| errsave(&mut state.ctx.escontext, e))?;
        }
        Err(e) => {
            // A SAX callback raised; if soft-error mode it is already recorded,
            // else propagate.
            if !soft_error_occurred(&state.ctx.escontext) {
                return Err(e);
            }
        }
    }

    let occurred = soft_error_occurred(&state.ctx.escontext);
    Ok((state.ctx, !occurred))
}

/// `populate_array_dim_jsonb` (jsonfuncs.c:2825): iterate recursively through
/// jsonb sub-array elements and accumulate result. Returns `Ok(false)` on a
/// soft-error early-out.
fn populate_array_dim_jsonb(
    ctx: &mut PopulateArrayContext<'_, '_, '_>,
    jbv: &JsonbValue,
    ndim: i32,
) -> PgResult<bool> {
    // JsonbContainer *jbc = jbv->val.binary.data;
    backend_utils_misc_stack_depth_seams::check_stack_depth::call()?;

    // Even scalars can end up here thanks to ExecEvalJsonCoercion().
    // if (jbv->type != jbvBinary || !JsonContainerIsArray(jbc) ||
    //     JsonContainerIsScalar(jbc))
    let jbc: &[u8] = match (jbv.typ, &jbv.val) {
        (jbvType::jbvBinary, JsonbValueData::Binary { data, .. }) => data,
        _ => {
            populate_array_report_expected_array(ctx, ndim - 1)?;
            return Ok(false);
        }
    };
    if !json_container_is_array(container_header(jbc)) || json_container_is_scalar(container_header(jbc))
    {
        populate_array_report_expected_array(ctx, ndim - 1)?;
        return Ok(false);
    }

    // it = JsonbIteratorInit(jbc);
    let mut it = JsonbIteratorInit(jbc);
    let mut val = JsonbValue::null();

    // tok = JsonbIteratorNext(&it, &val, true); Assert(tok == WJB_BEGIN_ARRAY);
    let tok = JsonbIteratorNext(&mut it, &mut val, true)?;
    debug_assert!(tok == JsonbIteratorToken::WJB_BEGIN_ARRAY);

    // tok = JsonbIteratorNext(&it, &val, true);
    let mut tok = JsonbIteratorNext(&mut it, &mut val, true)?;

    // If ndims unknown and (end of array, or first child not an array), assign now.
    let first_child_not_array = tok == JsonbIteratorToken::WJB_ELEM
        && match (val.typ, &val.val) {
            (jbvType::jbvBinary, JsonbValueData::Binary { data, .. }) => {
                !json_container_is_array(container_header(data))
            }
            _ => true,
        };
    if ctx.ndims <= 0 && (tok == JsonbIteratorToken::WJB_END_ARRAY || first_child_not_array) {
        if !populate_array_assign_ndims(ctx, ndim)? {
            return Ok(false);
        }
    }

    // process all the array elements
    while tok == JsonbIteratorToken::WJB_ELEM {
        // Recurse only if dimensions still unknown or not the innermost dim.
        if ctx.ndims > 0 && ndim >= ctx.ndims {
            let jsv = JsValue::Jsonb(Some(Box::new(val.clone())));
            if !populate_array_element(ctx, ndim, &jsv)? {
                return Ok(false);
            }
        } else {
            // populate child sub-array
            if !populate_array_dim_jsonb(ctx, &val, ndim + 1)? {
                return Ok(false);
            }

            debug_assert!(ctx.ndims > 0 && !ctx.dims.is_empty());

            if !populate_array_check_dimension(ctx, ndim)? {
                return Ok(false);
            }
        }

        tok = JsonbIteratorNext(&mut it, &mut val, true)?;
    }

    debug_assert!(tok == JsonbIteratorToken::WJB_END_ARRAY);

    // free iterator, iterating until WJB_DONE
    let tok = JsonbIteratorNext(&mut it, &mut val, true)?;
    debug_assert!(tok == JsonbIteratorToken::WJB_DONE && it.is_none());

    Ok(true)
}

/// `populate_array` (jsonfuncs.c:2915): recursively populate an array from
/// json/jsonb. `*isnull` is set true on a soft-error report.
fn populate_array<'mcx>(
    mcx: Mcx<'mcx>,
    aio: &mut ArrayIOData<'mcx>,
    colname: Option<&[u8]>,
    jsv: &JsValue,
    isnull: &mut bool,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Datum<'mcx>> {
    let element_type = aio.element_type;
    // ctx.astate = initArrayResult(aio->element_type, ctx.acxt, true);
    let astate = init_array_result(element_type, true)?;

    let mut ctx = PopulateArrayContext {
        astate: Some(astate),
        aio,
        mcxt: mcx,
        colname: colname.map(|c| c.to_vec()),
        ndims: 0, // unknown yet
        dims: Vec::new(),
        sizes: Vec::new(),
        escontext: escontext.as_deref_mut(),
    };

    match jsv {
        JsValue::Json { str, .. } => {
            // populate_array_json(&ctx, json, len >= 0 ? len : strlen(json))
            let json = str.as_deref().unwrap_or(&[]);
            let (returned_ctx, ok) = populate_array_json(ctx, json)?;
            ctx = returned_ctx;
            if !ok {
                *isnull = true;
                return Ok(Datum::null());
            }
        }
        JsValue::Jsonb(jbv) => {
            let jbv = jbv.as_deref().expect("jsonb value present");
            if !populate_array_dim_jsonb(&mut ctx, jbv, 1)? {
                *isnull = true;
                return Ok(Datum::null());
            }
            // ctx.dims[0] = ctx.sizes[0];
            ctx.dims[0] = ctx.sizes[0];
        }
    }

    debug_assert!(ctx.ndims > 0);

    // lbs = palloc(sizeof(int) * ndims); for (i) lbs[i] = 1;
    let lbs: Vec<i32> = alloc::vec![1; ctx.ndims as usize];

    // result = makeMdArrayResult(ctx.astate, ctx.ndims, ctx.dims, lbs, acxt, true);
    let astate = ctx.astate.take().expect("astate present");
    let result = make_md_array_result(mcx, &astate, ctx.ndims, &ctx.dims, &lbs)?;

    *isnull = false;
    Ok(Datum::ByRef(result))
}

// ===========================================================================
// JsValueToJsObject + the composite / scalar / domain populate cluster
// (jsonfuncs.c:2982-3247).
// ===========================================================================

/// `JsValueToJsObject` (jsonfuncs.c:2982): convert a [`JsValue`] into a
/// [`JsObjectW`]. Returns `Ok(false)` when a soft error was reported.
fn js_value_to_js_object<'a>(
    jsv: &'a JsValue,
    escontext: &mut Option<&mut SoftErrorContext>,
) -> PgResult<(JsObjectW<'a>, bool)> {
    match jsv {
        JsValue::Json { str, .. } => {
            // jso->val.json_hash = get_json_object_as_hash(json, len, "populate_composite", escontext);
            let json = str.as_deref().unwrap_or(&[]);
            let hash = get_json_object_as_hash(json, "populate_composite", escontext)?;
            let occurred = soft_error_occurred(escontext);
            debug_assert!(hash.is_some() || occurred);
            Ok((JsObjectW::JsonHash(hash), !occurred))
        }
        JsValue::Jsonb(jbv) => {
            let jbv = jbv.as_deref().expect("jsonb value present");
            // if (jbv->type == jbvBinary && JsonContainerIsObject(jbv->val.binary.data))
            if let (jbvType::jbvBinary, JsonbValueData::Binary { data, .. }) = (jbv.typ, &jbv.val) {
                if json_container_is_object(container_header(data)) {
                    return Ok((JsObjectW::JsonbCont(Some(data)), !soft_error_occurred(escontext)));
                }
            }

            // is_scalar = IsAJsonbScalar(jbv) || (jbvBinary && JsonContainerIsScalar(data));
            let is_scalar = is_a_jsonb_scalar(jbv.typ)
                || match (jbv.typ, &jbv.val) {
                    (jbvType::jbvBinary, JsonbValueData::Binary { data, .. }) => {
                        json_container_is_scalar(container_header(data))
                    }
                    _ => false,
                };
            let err = ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(if is_scalar {
                    "cannot call populate_composite on a scalar"
                } else {
                    "cannot call populate_composite on an array"
                })
                .into_error();
            errsave(escontext, err)?;
            Ok((JsObjectW::JsonbCont(None), !soft_error_occurred(escontext)))
        }
    }
}

/// `update_cached_tupdesc` (jsonfuncs.c:3029): acquire/update the cached tuple
/// descriptor for a composite type. The repo `lookup_rowtype_tupdesc` already
/// hands back an owned, constraint-stripped copy (the C
/// `CreateTupleDescCopy` of the typcache descriptor), so it is cached directly.
pub(crate) fn update_cached_tupdesc<'mcx>(
    mcx: Mcx<'mcx>,
    io: &mut CompositeIOData<'mcx>,
) -> PgResult<()> {
    let stale = match &io.tupdesc {
        None => true,
        Some(td) => td.tdtypeid != io.base_typid || td.tdtypmod != io.base_typmod,
    };
    if stale {
        // tupdesc = lookup_rowtype_tupdesc(base_typid, base_typmod);
        // io->tupdesc = CreateTupleDescCopy(tupdesc);
        let td = lookup_rowtype_tupdesc(mcx, io.base_typid, io.base_typmod)?;
        io.tupdesc = Some(td);
    }
    Ok(())
}

/// `populate_composite` (jsonfuncs.c:3057): recursively populate a composite
/// (row type) value from json/jsonb.
fn populate_composite<'mcx>(
    mcx: Mcx<'mcx>,
    io: &mut CompositeIOData<'mcx>,
    typid: u32,
    _colname: Option<&[u8]>,
    defaultval: Option<&FormedTuple<'mcx>>,
    jsv: &JsValue,
    isnull: &mut bool,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Datum<'mcx>> {
    // acquire/update cached tuple descriptor
    update_cached_tupdesc(mcx, io)?;

    let result;

    if *isnull {
        result = Datum::null();
    } else {
        // prepare input value
        let (jso, ok) = js_value_to_js_object(jsv, &mut escontext)?;
        if !ok {
            *isnull = true;
            return Ok(Datum::null());
        }

        // populate resulting record tuple
        let tupdesc_box = io.tupdesc.as_ref().expect("tupdesc cached").clone_in(mcx)?;
        let record_io = io.record_io.take();
        let (tuple, new_record_io) = populate_record(
            mcx,
            &tupdesc_box,
            record_io,
            defaultval,
            &jso,
            escontext.as_deref_mut(),
        )?;
        io.record_io = new_record_io;

        if soft_error_occurred(&escontext) {
            *isnull = true;
            return Ok(Datum::null());
        }
        // result = HeapTupleHeaderGetDatum(tuple);
        result = HeapTupleGetDatum(mcx, &tuple, &tupdesc_box)?;
    }

    // If it's domain over composite, check domain constraints.
    // if (typid != io->base_typid && typid != RECORDOID)
    if typid != io.base_typid && typid != RECORDOID {
        if !domain_check_safe(mcx, &result, *isnull, typid, &mut escontext)? {
            *isnull = true;
            return Ok(Datum::null());
        }
    }

    Ok(result)
}

/// `populate_scalar` (jsonfuncs.c:3124): populate a non-null scalar value from a
/// json/jsonb value through the type input function.
fn populate_scalar<'mcx>(
    mcx: Mcx<'mcx>,
    io: &ScalarIOData,
    typid: u32,
    typmod: i32,
    jsv: &JsValue,
    isnull: &mut bool,
    mut escontext: Option<&mut SoftErrorContext>,
    omit_quotes: bool,
) -> PgResult<Datum<'mcx>> {
    // The input string the type input function will consume (None == NULL).
    let str_opt: Option<Vec<u8>>;

    match jsv {
        JsValue::Json { str, type_ } => {
            // json = jsv->val.json.str; Assert(json);
            let json = str.as_deref().expect("populate_scalar: json str non-null");

            if (typid == JSONOID || typid == JSONBOID)
                && *type_ == JsonTokenType::JSON_TOKEN_STRING
            {
                // make string into a valid JSON literal
                let mut buf = mcx::vec_with_capacity_in::<u8>(mcx, json.len() + 2)?;
                // The json text carrier has no explicit length (NUL-terminated
                // model); C escape_json stops at NUL, escape_json_with_len uses
                // len. Here the byte slice is the exact payload, so use the
                // length-aware form (equivalent observable output).
                escape_json_with_len(&mut buf, json)?;
                str_opt = Some(buf.as_slice().to_vec());
            } else {
                // NUL-terminated / verbatim copy of the bytes.
                str_opt = Some(json.to_vec());
            }
        }
        JsValue::Jsonb(jbv) => {
            let jbv = jbv.as_deref().expect("jsonb value present");

            if jbv.typ == jbvType::jbvString && omit_quotes {
                // str = pnstrdup(string.val, string.len)
                str_opt = Some(jsonb_string_bytes(jbv));
            } else if typid == JSONBOID {
                // Jsonb *jsonb = JsonbValueToJsonb(jbv); return JsonbPGetDatum(jsonb);
                let jsonb = JsonbValueToJsonb(mcx, jbv)?;
                return Ok(Datum::ByRef(jsonb));
            } else if typid == JSONOID && jbv.typ != jbvType::jbvBinary {
                // Convert scalar jsonb to json string, preserving quotes.
                // Jsonb *jsonb = JsonbValueToJsonb(jbv);
                // str = JsonbToCString(NULL, &jsonb->root, VARSIZE(jsonb));
                let jsonb = JsonbValueToJsonb(mcx, jbv)?;
                let root = &jsonb[VARHDRSZ..];
                let s = JsonbToCString(mcx, root, jsonb.len() as i32)?;
                str_opt = Some(s.as_slice().to_vec());
            } else if jbv.typ == jbvType::jbvString {
                str_opt = Some(jsonb_string_bytes(jbv));
            } else if jbv.typ == jbvType::jbvBool {
                let b = matches!(jbv.val, JsonbValueData::Bool(true));
                str_opt = Some(if b { b"true".to_vec() } else { b"false".to_vec() });
            } else if jbv.typ == jbvType::jbvNumeric {
                // str = DatumGetCString(DirectFunctionCall1(numeric_out, numeric));
                let num = match &jbv.val {
                    JsonbValueData::Numeric(n) => n.as_slice(),
                    _ => &[],
                };
                str_opt = Some(numeric_out(mcx, num)?.into_bytes());
            } else if jbv.typ == jbvType::jbvBinary {
                // str = JsonbToCString(NULL, binary.data, binary.len);
                let (data, len) = match &jbv.val {
                    JsonbValueData::Binary { data, len, .. } => (data.as_slice(), *len),
                    _ => (&[][..], 0),
                };
                let s = JsonbToCString(mcx, data, len)?;
                str_opt = Some(s.as_slice().to_vec());
            } else {
                return Err(ereport(ERROR)
                    .errmsg_internal(alloc::format!("unrecognized jsonb type: {}", jbv.typ as i32))
                    .into_error());
            }
        }
    }

    // if (!InputFunctionCallSafe(&io->typiofunc, str, io->typioparam, typmod,
    //                            escontext, &res)) { res = 0; *isnull = true; }
    let cached = io
        .typiofunc
        .as_ref()
        .expect("populate_scalar: scalar_io.typiofunc resolved");
    let str_utf8: Option<String> = match &str_opt {
        Some(b) => Some(String::from_utf8_lossy(b).into_owned()),
        None => None,
    };
    let out = input_function_call_safe_typed(
        mcx,
        &cached.resolution,
        cached.finfo.clone(),
        str_utf8.as_deref(),
        io.typioparam,
        typmod,
        escontext.as_deref_mut(),
    )?;

    match out {
        Some(fmgr_out) => fmgr_out_to_datum(mcx, fmgr_out),
        None => {
            // a soft error was saved
            *isnull = true;
            Ok(Datum::null())
        }
    }
}

/// `pnstrdup(jbv->val.string.val, jbv->val.string.len)` — the jsonb string
/// payload bytes.
fn jsonb_string_bytes(jbv: &JsonbValue) -> Vec<u8> {
    match &jbv.val {
        JsonbValueData::String(s) => s.clone(),
        _ => Vec::new(),
    }
}

/// `populate_domain` (jsonfuncs.c:3216).
fn populate_domain<'mcx>(
    mcx: Mcx<'mcx>,
    io: &mut DomainIOData<'mcx>,
    typid: u32,
    colname: Option<&[u8]>,
    jsv: &JsValue,
    isnull: &mut bool,
    mut escontext: Option<&mut SoftErrorContext>,
    omit_quotes: bool,
) -> PgResult<Datum<'mcx>> {
    let res;

    if *isnull {
        res = Datum::null();
    } else {
        // res = populate_record_field(io->base_io, base_typid, base_typmod,
        //          colname, mcxt, PointerGetDatum(NULL), jsv, isnull,
        //          escontext, omit_quotes);
        let base_typid = io.base_typid;
        let base_typmod = io.base_typmod;
        res = populate_record_field(
            mcx,
            &mut io.base_io,
            base_typid,
            base_typmod,
            colname,
            &Datum::null(),
            jsv,
            isnull,
            escontext.as_deref_mut(),
            omit_quotes,
        )?;
        debug_assert!(!*isnull || soft_error_occurred(&escontext));
    }

    if !domain_check_safe(mcx, &res, *isnull, typid, &mut escontext)? {
        *isnull = true;
        return Ok(Datum::null());
    }

    Ok(res)
}

/// `domain_check_safe(value, isnull, domainType, extra, mcxt, escontext)`
/// (domains.c) — the soft-error-capable domain constraint check. The repo
/// `domain_check` raises hard (no escontext threading; the `extra` memoization
/// handle is absent), so the soft semantics are reconstructed here: with an
/// escontext, capture the raised error into it and return `false`; without one,
/// propagate. Returns `Ok(true)` when the value satisfied the domain.
fn domain_check_safe<'mcx>(
    mcx: Mcx<'mcx>,
    value: &Datum<'mcx>,
    isnull: bool,
    domain_type: u32,
    escontext: &mut Option<&mut SoftErrorContext>,
) -> PgResult<bool> {
    match backend_utils_adt_misc2::domains::domain_check(mcx, value, isnull, domain_type) {
        Ok(()) => Ok(true),
        Err(e) => match escontext {
            Some(ctx) => {
                ctx.save(e);
                Ok(false)
            }
            None => Err(e),
        },
    }
}

// ===========================================================================
// prepare_column_cache + populate_record_field (jsonfuncs.c:3249-3473).
// ===========================================================================

/// `prepare_column_cache` (jsonfuncs.c:3250): prepare the column metadata cache
/// for the given type. `need_scalar` forces scalar_io lookup even for
/// non-scalars (the json-string hack in `populate_record_field`).
pub(crate) fn prepare_column_cache<'mcx>(
    mcx: Mcx<'mcx>,
    column: &mut ColumnIOData<'mcx>,
    typid: u32,
    typmod: i32,
    mut need_scalar: bool,
) -> PgResult<()> {
    column.typid = typid;
    column.typmod = typmod;

    // tup = SearchSysCache1(TYPEOID, typid); type = GETSTRUCT(tup);
    // type->typtype:
    let typtype = lsyscache_type::get_typtype(typid)?;

    if typtype == TYPTYPE_DOMAIN {
        // Move directly to the bottom base type; domain_check covers the stack.
        // base_typid = getBaseTypeAndTypmod(typid, &base_typmod);
        let (base_typid, base_typmod) = lsyscache_type::get_base_type_and_typmod(typid)?;
        if lsyscache_type::get_typtype(base_typid)? == TYPTYPE_COMPOSITE {
            // domain over composite has its own code path
            column.typcat = TypeCat::CompositeDomain;
            column.io = ColumnIOUnion::Composite(CompositeIOData {
                record_io: None,
                tupdesc: None,
                base_typid,
                base_typmod,
                domain_info: None,
            });
        } else {
            // domain over anything else
            column.typcat = TypeCat::Domain;
            column.io = ColumnIOUnion::Domain(DomainIOData {
                base_typid,
                base_typmod,
                base_io: Box::new(ColumnIOData::default()),
                domain_info: None,
            });
        }
    } else if typtype == TYPTYPE_COMPOSITE || typid == RECORDOID {
        column.typcat = TypeCat::Composite;
        column.io = ColumnIOUnion::Composite(CompositeIOData {
            record_io: None,
            tupdesc: None,
            base_typid: typid,
            base_typmod: typmod,
            domain_info: None,
        });
    } else if is_true_array_type(typid)? {
        // IsTrueArrayType(type): typelem valid && typlen == -1.
        column.typcat = TypeCat::Array;
        let element_type = lsyscache_type::get_element_type(typid)?.unwrap_or(0);
        column.io = ColumnIOUnion::Array(ArrayIOData {
            element_info: Box::new(ColumnIOData::default()),
            element_type,
            // array element typemod stored in attribute's typmod
            element_typmod: typmod,
        });
    } else {
        column.typcat = TypeCat::Scalar;
        need_scalar = true;
    }

    // caller can force scalar_io lookup even for non-scalars
    if need_scalar {
        // getTypeInputInfo(typid, &typioproc, &column->scalar_io.typioparam);
        let (typioproc, typioparam) = lsyscache_type::get_type_input_info(typid)?;
        column.scalar_io.typioparam = typioparam;
        // fmgr_info_cxt(typioproc, &column->scalar_io.typiofunc, mcxt);
        let r = fmgr_info(mcx, typioproc)?;
        column.scalar_io.typiofunc = Some(CachedFmgrInfo {
            finfo: r.finfo,
            resolution: r.resolution,
        });
    }

    Ok(())
}

/// `IsTrueArrayType(typeForm)` (lsyscache.h): a true array type has a valid
/// element type and varlena length (`typelem != 0 && typlen == -1`). The repo
/// `get_element_type` returns the element type only for a varlena array
/// (`typlen == -1`), exactly the `IsTrueArrayType` predicate.
fn is_true_array_type(typid: u32) -> PgResult<bool> {
    Ok(lsyscache_type::get_element_type(typid)?.is_some())
}

/// `populate_record_field` (jsonfuncs.c:3405): recursively populate a record
/// field or an array element from a json/jsonb value.
#[allow(clippy::too_many_arguments)]
fn populate_record_field<'mcx>(
    mcx: Mcx<'mcx>,
    col: &mut ColumnIOData<'mcx>,
    typid: u32,
    typmod: i32,
    colname: Option<&[u8]>,
    defaultval: &Datum<'mcx>,
    jsv: &JsValue,
    isnull: &mut bool,
    mut escontext: Option<&mut SoftErrorContext>,
    omit_scalar_quotes: bool,
) -> PgResult<Datum<'mcx>> {
    backend_utils_misc_stack_depth_seams::check_stack_depth::call()?;

    // Force lookup of scalar_io so the json string hack below works.
    if col.typid != typid || col.typmod != typmod {
        prepare_column_cache(mcx, col, typid, typmod, true)?;
    }

    *isnull = js_value_is_null(jsv);

    let mut typcat = col.typcat;

    // try to convert json string to a non-scalar type through input function
    if js_value_is_string(jsv)
        && (typcat == TypeCat::Array
            || typcat == TypeCat::Composite
            || typcat == TypeCat::CompositeDomain)
    {
        typcat = TypeCat::Scalar;
    }

    // we must perform domain checks for NULLs, otherwise exit immediately
    if *isnull && typcat != TypeCat::Domain && typcat != TypeCat::CompositeDomain {
        return Ok(Datum::null());
    }

    match typcat {
        TypeCat::Scalar => populate_scalar(
            mcx,
            &col.scalar_io,
            typid,
            typmod,
            jsv,
            isnull,
            escontext.as_deref_mut(),
            omit_scalar_quotes,
        ),
        TypeCat::Array => {
            let io = match &mut col.io {
                ColumnIOUnion::Array(a) => a,
                _ => unreachable!("typcat ARRAY implies io.array"),
            };
            populate_array(mcx, io, colname, jsv, isnull, escontext.as_deref_mut())
        }
        TypeCat::Composite | TypeCat::CompositeDomain => {
            // defaultval is a composite Datum; decode it back to a FormedTuple
            // only when it carries a pointer (DatumGetPointer != NULL).
            let default_tuple = match defaultval {
                Datum::ByVal(0) => None,
                d => Some(datum_get_heap_tuple_header(mcx, d)?),
            };
            let io = match &mut col.io {
                ColumnIOUnion::Composite(c) => c,
                _ => unreachable!("typcat COMPOSITE implies io.composite"),
            };
            populate_composite(
                mcx,
                io,
                typid,
                colname,
                default_tuple.as_ref(),
                jsv,
                isnull,
                escontext.as_deref_mut(),
            )
        }
        TypeCat::Domain => {
            let io = match &mut col.io {
                ColumnIOUnion::Domain(d) => d,
                _ => unreachable!("typcat DOMAIN implies io.domain"),
            };
            populate_domain(
                mcx,
                io,
                typid,
                colname,
                jsv,
                isnull,
                escontext.as_deref_mut(),
                omit_scalar_quotes,
            )
        }
    }
}

/// `DatumGetHeapTupleHeader(defaultval)` (htup_details.h): decode a composite
/// Datum back into a [`FormedTuple`].
fn datum_get_heap_tuple_header<'mcx>(
    mcx: Mcx<'mcx>,
    d: &Datum<'mcx>,
) -> PgResult<FormedTuple<'mcx>> {
    backend_access_common_heaptuple::DatumGetHeapTupleHeader(mcx, d)
}

// ===========================================================================
// JsObjectGetField + populate_record (jsonfuncs.c:3491-3628).
// ===========================================================================

/// `JsObjectGetField` (jsonfuncs.c:3491): look up `field` in the object,
/// producing the per-field [`JsValue`]. Returns `(jsv, found)`.
fn js_object_get_field(obj: &JsObjectW<'_>, field: &[u8]) -> PgResult<(JsValue, bool)> {
    match obj {
        JsObjectW::JsonHash(hash) => {
            // hashentry = hash_search(json_hash, field, HASH_FIND, NULL);
            let entry = match hash {
                Some(map) => map.get(field),
                None => None,
            };
            // type = hashentry ? hashentry->type : JSON_TOKEN_NULL;
            let type_ = entry
                .map(|e| e.type_)
                .unwrap_or(JsonTokenType::JSON_TOKEN_NULL);
            // str = type == JSON_TOKEN_NULL ? NULL : hashentry->val;
            let str = if type_ == JsonTokenType::JSON_TOKEN_NULL {
                None
            } else {
                entry.and_then(|e| e.val.clone())
            };
            // len = str ? -1 : 0;  (carried implicitly by Option<Vec<u8>>)
            Ok((JsValue::Json { str, type_ }, entry.is_some()))
        }
        JsObjectW::JsonbCont(cont) => {
            // jsv->val.jsonb = !cont ? NULL :
            //     getKeyJsonValueFromContainer(cont, field, strlen(field), NULL);
            let jbv = match cont {
                None => None,
                Some(bytes) => getKeyJsonValueFromContainer(bytes, field)?.map(Box::new),
            };
            let found = jbv.is_some();
            Ok((JsValue::Jsonb(jbv), found))
        }
    }
}

/// `populate_record` (jsonfuncs.c:3519): populate a record tuple from a
/// json/jsonb object. Returns the formed tuple plus the (allocated/updated)
/// `RecordIOData` cache to thread back into the [`CompositeIOData`].
pub(crate) fn populate_record<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: &TupleDescData<'mcx>,
    record_p: Option<Box<RecordIOData<'mcx>>>,
    defaultval: Option<&FormedTuple<'mcx>>,
    obj: &JsObjectW<'_>,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<(FormedTuple<'mcx>, Option<Box<RecordIOData<'mcx>>>)> {
    let ncolumns = tupdesc.natts;

    // If the input json is empty, skip the rest only if passed a non-null record.
    if let Some(default) = defaultval {
        if obj.is_empty() {
            // return defaultval;
            return Ok((default.clone_in(mcx)?, record_p));
        }
    }

    // (re)allocate metadata cache
    let mut record = match record_p {
        Some(r) if r.ncolumns == ncolumns => r,
        _ => allocate_record_info(ncolumns),
    };

    // invalidate metadata cache if the record type has changed
    if record.record_type != tupdesc.tdtypeid || record.record_typmod != tupdesc.tdtypmod {
        record = allocate_record_info(ncolumns);
        record.record_type = tupdesc.tdtypeid;
        record.record_typmod = tupdesc.tdtypmod;
        record.ncolumns = ncolumns;
    }

    // values = palloc(ncolumns); nulls = palloc(ncolumns);
    let mut values: Vec<Datum<'mcx>> = Vec::with_capacity(ncolumns as usize);
    let mut nulls: Vec<bool> = Vec::with_capacity(ncolumns as usize);

    if let Some(default) = defaultval {
        // Build a temporary HeapTuple control structure and deform it.
        // heap_deform_tuple(&tuple, tupdesc, values, nulls);
        let deformed = heap_deform_tuple(mcx, &default.tuple, tupdesc, &default.data)?;
        for (d, n) in deformed.into_iter() {
            values.push(d);
            nulls.push(n);
        }
    } else {
        for _ in 0..ncolumns {
            values.push(Datum::null());
            nulls.push(true);
        }
    }

    for i in 0..ncolumns as usize {
        let att = tupdesc.attr(i);
        let colname = att.attname.name_str().to_vec();
        let atttypid = att.atttypid;
        let atttypmod = att.atttypmod;
        let attisdropped = att.attisdropped;

        // Ignore dropped columns in datatype
        if attisdropped {
            nulls[i] = true;
            continue;
        }

        let (field, found) = js_object_get_field(obj, &colname)?;

        // If passed a non-null record and the key wasn't found, keep existing.
        if defaultval.is_some() && !found {
            continue;
        }

        let default_for_field = if nulls[i] {
            Datum::null()
        } else {
            values[i].clone_in(mcx)?
        };

        let mut isnull = nulls[i];
        let v = populate_record_field(
            mcx,
            &mut record.columns[i],
            atttypid,
            atttypmod,
            Some(&colname),
            &default_for_field,
            &field,
            &mut isnull,
            escontext.as_deref_mut(),
            false,
        )?;
        values[i] = v;
        nulls[i] = isnull;
    }

    // res = heap_form_tuple(tupdesc, values, nulls);
    let res = heap_form_tuple(mcx, tupdesc, &values, &nulls)
        .map_err(|e| PgError::error(alloc::format!("heap_form_tuple failed: {e:?}")))?;

    Ok((res, Some(record)))
}

/// `allocate_record_info` (jsonfuncs.c:3475): allocate a zeroed `RecordIOData`
/// with `ncolumns` column caches.
fn allocate_record_info<'mcx>(ncolumns: i32) -> Box<RecordIOData<'mcx>> {
    let mut columns = Vec::with_capacity(ncolumns.max(0) as usize);
    for _ in 0..ncolumns.max(0) {
        columns.push(ColumnIOData::default());
    }
    Box::new(RecordIOData {
        record_type: types_core::InvalidOid,
        record_typmod: 0,
        ncolumns,
        columns,
    })
}

// ===========================================================================
// get_json_object_as_hash + the four hash_* SAX callbacks
// (jsonfuncs.c:3810-3960).
// ===========================================================================

/// C: `struct JHashState` (jsonfuncs.c:136): the `get_json_object_as_hash`
/// SAX-callback state. `lex` is handed to each callback by the parse driver; the
/// hash is built into the owned map.
struct JHashState {
    function_name: &'static str,
    hash: BTreeMap<Vec<u8>, JsonHashEntry>,
    /// `char *saved_scalar` — the most recent top-level scalar token.
    saved_scalar: Option<Vec<u8>>,
    /// `const char *save_json_start` — offset of the start of the current
    /// subobject text (`None` == scalar).
    save_json_start: Option<usize>,
    /// `JsonTokenType saved_token_type`.
    saved_token_type: JsonTokenType,
}

/// `get_json_object_as_hash` (jsonfuncs.c:3810): decompose a json object into a
/// field hash. Returns `None` (and a recorded soft error) on a parse failure.
pub(crate) fn get_json_object_as_hash(
    json: &[u8],
    funcname: &'static str,
    escontext: &mut Option<&mut SoftErrorContext>,
) -> PgResult<Option<BTreeMap<Vec<u8>, JsonHashEntry>>> {
    let state = Rc::new(RefCell::new(JHashState {
        function_name: funcname,
        hash: BTreeMap::new(),
        saved_scalar: None,
        save_json_start: None,
        saved_token_type: JsonTokenType::JSON_TOKEN_INVALID,
    }));

    let mut sem = JsonSemAction::default();
    {
        let state = Rc::clone(&state);
        sem.array_start = Some(Box::new(move |lex: &JsonLexContext| {
            hash_array_start(&state.borrow(), lex)
        }));
    }
    {
        let state = Rc::clone(&state);
        sem.scalar = Some(Box::new(
            move |lex: &JsonLexContext, token: &[u8], tokentype: JsonTokenType| {
                hash_scalar(&mut state.borrow_mut(), lex, token, tokentype)
            },
        ));
    }
    {
        let state = Rc::clone(&state);
        sem.object_field_start = Some(Box::new(
            move |lex: &JsonLexContext, _fname: &[u8], _isnull: bool| {
                hash_object_field_start(&mut state.borrow_mut(), lex)
            },
        ));
    }
    {
        let state = Rc::clone(&state);
        sem.object_field_end = Some(Box::new(
            move |lex: &JsonLexContext, fname: &[u8], isnull: bool| {
                hash_object_field_end(&mut state.borrow_mut(), lex, fname, isnull)
            },
        ));
    }

    // if (!pg_parse_json_or_errsave(state->lex, sem, escontext)) { tab = NULL; }
    let encoding = common_jsonapi_seams::get_database_encoding::call();
    let parse = common_jsonapi_seams::pg_parse_json::call(json, encoding, true, &mut sem);
    drop(sem);

    let state = Rc::try_unwrap(state)
        .ok()
        .expect("all SAX closures dropped")
        .into_inner();

    match parse {
        Ok(JsonParseErrorType::JSON_SUCCESS) => Ok(Some(state.hash)),
        Ok(result) => {
            // parse failure reported into escontext (or raised if none).
            common_jsonapi_seams::errsave_error::call(result, json)
                .or_else(|e| errsave(escontext, e))?;
            Ok(None)
        }
        Err(e) => {
            // A hash_* callback raised (hard error: array/scalar at top level).
            Err(e)
        }
    }
}

/// `hash_object_field_start` (jsonfuncs.c:3852).
fn hash_object_field_start(
    state: &mut JHashState,
    lex: &JsonLexContext,
) -> PgResult<()> {
    if lex.lex_level > 1 {
        return Ok(());
    }

    // remember token type
    state.saved_token_type = lex.token_type;

    if lex.token_type == JsonTokenType::JSON_TOKEN_ARRAY_START
        || lex.token_type == JsonTokenType::JSON_TOKEN_OBJECT_START
    {
        // remember start position of the whole text of the subobject
        state.save_json_start = Some(lex.token_start);
    } else {
        // must be a scalar
        state.save_json_start = None;
    }

    Ok(())
}

/// `hash_object_field_end` (jsonfuncs.c:3878).
fn hash_object_field_end(
    state: &mut JHashState,
    lex: &JsonLexContext,
    fname: &[u8],
    isnull: bool,
) -> PgResult<()> {
    // Ignore nested fields.
    if lex.lex_level > 1 {
        return Ok(());
    }

    // Ignore field names >= NAMEDATALEN — they can't match a record field.
    if fname.len() >= NAMEDATALEN {
        return Ok(());
    }

    // hashentry = hash_search(hash, fname, HASH_ENTER, &found);
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

    state.hash.insert(
        fname.to_vec(),
        JsonHashEntry {
            val,
            type_: saved_token_type,
        },
    );

    Ok(())
}

/// `hash_array_start` (jsonfuncs.c:3929): a top-level array is an error.
fn hash_array_start(state: &JHashState, lex: &JsonLexContext) -> PgResult<()> {
    if lex.lex_level == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(alloc::format!("cannot call {} on an array", state.function_name))
            .into_error());
    }
    Ok(())
}

/// `hash_scalar` (jsonfuncs.c:3942).
fn hash_scalar(
    state: &mut JHashState,
    lex: &JsonLexContext,
    token: &[u8],
    tokentype: JsonTokenType,
) -> PgResult<()> {
    if lex.lex_level == 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(alloc::format!("cannot call {} on a scalar", state.function_name))
            .into_error());
    }

    if lex.lex_level == 1 {
        state.saved_scalar = Some(token.to_vec());
        debug_assert!(state.saved_token_type == tokentype);
    }

    Ok(())
}

// ===========================================================================
// json_populate_type + the worker + type resolution + SQL entry points
// (jsonfuncs.c:3344-3801).
// ===========================================================================

/// The per-query cache `populate_record_worker` threads through
/// `fcinfo->flinfo->fn_extra`. The trimmed owned `FunctionCallInfoBaseData` has
/// no `fn_extra` slot, so the cache is rebuilt each call (a behaviour-preserving
/// loss of the cross-call memoization — every field's IO metadata is re-derived,
/// exactly as the C first-call path does). It is materialized here as a local.
pub(crate) struct PopulateRecordCacheLocal<'mcx> {
    /// `Oid argtype`.
    pub(crate) argtype: u32,
    /// `ColumnIOData c`.
    pub(crate) c: ColumnIOData<'mcx>,
}

/// `get_record_type_from_argument` (jsonfuncs.c:3635): result type = first
/// argument's declared type (unless it's `null::record`, handled later).
pub(crate) fn get_record_type_from_argument<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData<'mcx>,
    funcname: &str,
    cache: &mut PopulateRecordCacheLocal<'mcx>,
) -> PgResult<()> {
    // cache->argtype = get_fn_expr_argtype(fcinfo->flinfo, 0);
    cache.argtype = backend_utils_fmgr_fmgr_seams::get_fn_expr_argtype::call(fcinfo, 0);
    // prepare_column_cache(&cache->c, argtype, -1, fn_mcxt, false);
    prepare_column_cache(mcx, &mut cache.c, cache.argtype, -1, false)?;
    if cache.c.typcat != TypeCat::Composite && cache.c.typcat != TypeCat::CompositeDomain {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(alloc::format!(
                "first argument of {} must be a row type",
                funcname
            ))
            .into_error());
    }
    Ok(())
}

/// `get_record_type_from_query` (jsonfuncs.c:3661): result type is specified by
/// the calling query (`get_call_result_type`).
pub(crate) fn get_record_type_from_query<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &'mcx FunctionCallInfoBaseData<'mcx>,
    funcname: &str,
    cache: &mut PopulateRecordCacheLocal<'mcx>,
) -> PgResult<()> {
    // if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) ereport;
    let resolved = backend_utils_fmgr_funcapi::result_type::get_call_result_type(mcx, fcinfo)?;
    if resolved.class != Some(TypeFuncClass::Composite) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(alloc::format!(
                "could not determine row type for result of {}",
                funcname
            ))
            .errhint(
                "Provide a non-null record argument, or call the function in the FROM clause using a column definition list.",
            )
            .into_error());
    }

    let tupdesc = resolved
        .result_tuple_desc
        .expect("TYPEFUNC_COMPOSITE has a tupdesc");
    cache.argtype = tupdesc.tdtypeid;

    // Save identified tupdesc into cache->c.io.composite (CreateTupleDescCopy).
    let base_typid = tupdesc.tdtypeid;
    let base_typmod = tupdesc.tdtypmod;
    cache.c.io = ColumnIOUnion::Composite(CompositeIOData {
        record_io: None,
        tupdesc: Some(tupdesc),
        base_typid,
        base_typmod,
        domain_info: None,
    });
    Ok(())
}

/// `populate_record_worker` (jsonfuncs.c:3698): common worker for
/// `json[b]_populate_record` / `json[b]_to_record`.
fn populate_record_worker<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &'mcx mut FunctionCallInfoBaseData<'mcx>,
    funcname: &str,
    is_json: bool,
    have_record_arg: bool,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Datum<'mcx>> {
    let json_arg_num = if have_record_arg { 1 } else { 0 };

    // First time through (no fn_extra cache in the trimmed frame): identify the
    // input/result record type. (See PopulateRecordCacheLocal.)
    let mut cache = PopulateRecordCacheLocal {
        argtype: types_core::InvalidOid,
        c: ColumnIOData::default(),
    };

    // get_record_type_from_query borrows fcinfo as &'mcx; the rest of the
    // function only needs shared reads (arg null flags / bytes through seams)
    // and the mutable resultinfo is untouched here, so reborrow immutably.
    let fcinfo_ref: &'mcx FunctionCallInfoBaseData<'mcx> = fcinfo;

    if have_record_arg {
        get_record_type_from_argument(mcx, fcinfo_ref, funcname, &mut cache)?;
    } else {
        get_record_type_from_query(mcx, fcinfo_ref, funcname, &mut cache)?;
    }

    // Collect record arg if we have one.
    let rec: Option<FormedTuple<'mcx>>;
    if !have_record_arg {
        rec = None; // json{b}_to_record()
    } else if !backend_utils_fmgr_fmgr_seams::pg_argisnull::call(fcinfo_ref, 0) {
        // rec = PG_GETARG_HEAPTUPLEHEADER(0);
        let r = funcapi::srf_arg_record::call(mcx, fcinfo_ref, 0)?;

        // When declared arg type is RECORD, identify the actual record type.
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
            debug_assert!(cache.argtype == RECORDOID);
        }
    }

    // If no JSON argument, return the record (if any) unchanged.
    if backend_utils_fmgr_fmgr_seams::pg_argisnull::call(fcinfo_ref, json_arg_num) {
        return match rec {
            Some(r) => {
                // PG_RETURN_POINTER(rec): hand the composite datum back.
                let tupdesc = composite_tupdesc(&cache.c)?;
                HeapTupleGetDatum(mcx, &r, tupdesc)
            }
            None => Ok(Datum::null()),
        };
    }

    // Build the JsValue from the input.
    let jsv: JsValue = if is_json {
        // text *json = PG_GETARG_TEXT_PP(json_arg_num);
        let json = funcapi::srf_arg_varlena_bytes::call(mcx, fcinfo_ref, json_arg_num)?;
        JsValue::Json {
            str: Some(json.as_slice().to_vec()),
            // type not used in populate_composite()
            type_: JsonTokenType::JSON_TOKEN_INVALID,
        }
    } else {
        // Jsonb *jb = PG_GETARG_JSONB_P(json_arg_num);
        // jbv.type = jbvBinary; jbv.val.binary.data = &jb->root;
        // jbv.val.binary.len = VARSIZE(jb) - VARHDRSZ;
        let jb = funcapi::srf_arg_varlena_bytes::call(mcx, fcinfo_ref, json_arg_num)?;
        let root = &jb[VARHDRSZ..];
        let jbv = JsonbValue {
            typ: jbvType::jbvBinary,
            val: JsonbValueData::Binary {
                len: (jb.len() - VARHDRSZ) as i32,
                data: root.to_vec(),
                offset: 0,
            },
        };
        JsValue::Jsonb(Some(Box::new(jbv)))
    };

    let mut isnull = false;
    // rettuple = populate_composite(&cache->c.io.composite, cache->argtype,
    //              NULL, fnmcxt, rec, &jsv, &isnull, escontext);
    let argtype = cache.argtype;
    let io = match &mut cache.c.io {
        ColumnIOUnion::Composite(c) => c,
        _ => {
            return Err(ereport(ERROR)
                .errmsg_internal("populate_record_worker: cache.c is not composite")
                .into_error())
        }
    };
    let rettuple = populate_composite(
        mcx,
        io,
        argtype,
        None,
        rec.as_ref(),
        &jsv,
        &mut isnull,
        escontext.as_deref_mut(),
    )?;
    debug_assert!(!isnull || soft_error_occurred(&escontext));

    Ok(rettuple)
}

/// Borrow the cached composite `TupleDesc` out of the worker's column cache.
fn composite_tupdesc<'a, 'mcx>(
    c: &'a ColumnIOData<'mcx>,
) -> PgResult<&'a TupleDescData<'mcx>> {
    match &c.io {
        ColumnIOUnion::Composite(comp) => Ok(comp
            .tupdesc
            .as_ref()
            .expect("composite tupdesc cached")
            .as_ref()),
        _ => Err(ereport(ERROR)
            .errmsg_internal("composite_tupdesc: not a composite cache")
            .into_error()),
    }
}

// ---------------------------------------------------------------------------
// SQL entry points (jsonfuncs.c:2464-2506).
// ---------------------------------------------------------------------------

/// `jsonb_populate_record(PG_FUNCTION_ARGS)` (jsonfuncs.c:2464).
pub fn jsonb_populate_record<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &'mcx mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    populate_record_worker(mcx, fcinfo, "jsonb_populate_record", false, true, None)
}

/// `jsonb_populate_record_valid(PG_FUNCTION_ARGS)` (jsonfuncs.c:2477): returns
/// `false` if `json_populate_record` encounters an error, `true` otherwise.
pub fn jsonb_populate_record_valid<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &'mcx mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    // ErrorSaveContext escontext = {T_ErrorSaveContext};
    let mut escontext = SoftErrorContext::new(false);
    let _ = populate_record_worker(
        mcx,
        fcinfo,
        "jsonb_populate_record",
        false,
        true,
        Some(&mut escontext),
    )?;
    // return BoolGetDatum(!escontext.error_occurred);
    Ok(Datum::from_bool(!escontext.error_occurred()))
}

/// `jsonb_to_record(PG_FUNCTION_ARGS)` (jsonfuncs.c:2488).
pub fn jsonb_to_record<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &'mcx mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    populate_record_worker(mcx, fcinfo, "jsonb_to_record", false, false, None)
}

/// `json_populate_record(PG_FUNCTION_ARGS)` (jsonfuncs.c:2495).
pub fn json_populate_record<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &'mcx mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    populate_record_worker(mcx, fcinfo, "json_populate_record", true, true, None)
}

/// `json_to_record(PG_FUNCTION_ARGS)` (jsonfuncs.c:2502).
pub fn json_to_record<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &'mcx mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    populate_record_worker(mcx, fcinfo, "json_to_record", true, false, None)
}

/// `json_populate_type(json_val, json_type, typid, typmod, cache, mcxt, isnull,
/// omit_quotes, escontext)` (jsonfuncs.c:3344): populate and return a value of
/// the specified type from a json/jsonb `Datum`. Exposed for the SQL/JSON
/// coercion path (`ExecEvalJsonCoercion`).
///
/// The C `void **cache` (a `ColumnIOData *` first-call-allocated and reused) is
/// threaded explicitly here as an in/out [`ColumnIOData`]. `json_val` arrives
/// pre-decoded as the json/jsonb payload bytes (the fmgr `DatumGetTextPP` /
/// `DatumGetJsonbP` detoast is the caller's boundary).
#[allow(clippy::too_many_arguments)]
pub fn json_populate_type<'mcx>(
    mcx: Mcx<'mcx>,
    json_val: &[u8],
    json_type: u32,
    typid: u32,
    typmod: i32,
    cache: &mut ColumnIOData<'mcx>,
    isnull: &mut bool,
    omit_quotes: bool,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Datum<'mcx>> {
    let is_json = json_type == JSONOID;

    let jsv: JsValue = if *isnull {
        if is_json {
            JsValue::Json {
                str: None,
                type_: JsonTokenType::JSON_TOKEN_INVALID,
            }
        } else {
            JsValue::Jsonb(None)
        }
    } else if is_json {
        // text *json = DatumGetTextPP(json_val);
        // jsv.val.json.str = VARDATA_ANY(json); len = VARSIZE_ANY_EXHDR(json);
        JsValue::Json {
            str: Some(json_val.to_vec()),
            type_: JsonTokenType::JSON_TOKEN_INVALID,
        }
    } else {
        // Jsonb *jsonb = DatumGetJsonbP(json_val);
        if omit_quotes {
            // char *str = JsonbUnquote(jsonb); jbv.type = jbvString; ...
            let s = JsonbUnquote(mcx, json_val)?;
            let bytes = s.as_slice().to_vec();
            JsValue::Jsonb(Some(Box::new(JsonbValue {
                typ: jbvType::jbvString,
                val: JsonbValueData::String(bytes),
            })))
        } else {
            // jbv.type = jbvBinary; data = &jsonb->root; len = VARSIZE - VARHDRSZ;
            let root = &json_val[VARHDRSZ..];
            JsValue::Jsonb(Some(Box::new(JsonbValue {
                typ: jbvType::jbvBinary,
                val: JsonbValueData::Binary {
                    len: (json_val.len() - VARHDRSZ) as i32,
                    data: root.to_vec(),
                    offset: 0,
                },
            })))
        }
    };

    // if (*cache == NULL) *cache = MemoryContextAllocZero(mcxt, sizeof(ColumnIOData));
    // (the caller passes an owned ColumnIOData::default() the first time.)
    populate_record_field(
        mcx,
        cache,
        typid,
        typmod,
        None,
        &Datum::null(),
        &jsv,
        isnull,
        escontext,
        omit_quotes,
    )
}
