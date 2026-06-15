//! Idiomatic port of PostgreSQL's `json.c` — the text `json` data type and its
//! builder / aggregate / escape machinery.
//!
//! Mirrors `postgres-18.3/src/backend/utils/adt/json.c` (PostgreSQL 18.3).
//!
//! The text `json` type is *itself* just a validated UTF-8 varlena — there is
//! no on-disk binary tree (that is `jsonb`). Therefore this crate defines no
//! on-disk struct; the SQL-facing functions operate on the logical *content
//! bytes*, exactly as the C code treats the `json` value as raw text; the
//! varlena framing is the caller's (fmgr wrapper's) responsibility.
//!
//! # Buffer model
//!
//! `json.c` builds results in a `StringInfo` allocated in the current memory
//! context. The faithful analog here is a [`mcx::PgVec`]`<u8>` (the
//! context-charged byte spine, == `StringInfoData.data`); the per-call
//! [`mcx::Mcx`] is its allocator. Every `appendStringInfo*` becomes a fallible
//! append against that spine, surfacing OOM / over-`MaxAllocSize` as a
//! recoverable [`PgError`] rather than aborting.
//!
//! # Seams
//!
//! Everything structural (escaping, the object/array builders over
//! pre-classified inputs, the unique-key check, the aggregate state machine) is
//! ported in-crate. The genuinely-external work is seamed:
//!  * the JSON lexer/parser in `src/common/jsonapi.c` (`common-jsonapi-seams`);
//!  * type classification + the fmgr output/cast functions + array/composite
//!    deconstruction (`backend-utils-adt-jsonfuncs-seams`, anchored by
//!    `json_categorize_type` in the cycle-partner `jsonfuncs.c`);
//!  * `JsonEncodeDateTime`, the datetime subsystem's field-conversion +
//!    `Encode*` machinery (`backend-utils-adt-timestamp-seams`).

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use alloc::string::String;

use mcx::{Mcx, PgString, PgVec, MAX_ALLOC_SIZE};
use types_core::Oid;
// The canonical unified value type (`types_tuple::Datum<'mcx>`, the ByVal/ByRef
// enum — the faithful idiomatic substitute for C's `Datum`).
//
// `json.c` is a pure value *relay*: every value it touches arrives already
// extracted at the executor/fmgr boundary and flows straight back out across a
// type-classification / output-function / datetime-encode seam. Those consumed
// seam contracts — `backend-utils-adt-jsonfuncs-seams` (`output_function_call`,
// `cast_function_call`, `text_datum_bytes`, `deconstruct_array`,
// `walk_composite`) and `backend-utils-adt-timestamp-seams`
// (`json_encode_datetime`), plus the `json_encode_datetime` decl this crate
// owns in `backend-utils-adt-json-seams`, and the `ArrayForJson` /
// `CompositeFieldForJson` value carriers in `types-json` — all already speak the
// canonical `Datum<'mcx>` (by reference). So every value here is carried as the
// canonical enum and forwarded unchanged across those edges; there is no
// in-crate scalar-word logic (no `DatumGet*`/`*GetDatum` forges, no pointer
// forges, no datum pointer-token registry, no `_as_datum` forges, no deprecated
// bare-word `datum_*` `_v` seam variants). The lone codec read (`DatumGetBool`)
// is the canonical `Datum::as_bool` accessor. The bare-word
// `types_datum::Datum(usize)` shim is no longer referenced internally.
use types_tuple::Datum;
use types_error::error::{
    ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE, ERRCODE_INTERNAL_ERROR,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_NULL_VALUE_NOT_ALLOWED, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};
use types_error::{PgError, PgResult};
use types_json::{JsonParseErrorType, JsonTokenType, JsonTypeCategory};
use types_tuple::heaptuple::{DATEOID, TIMESTAMPOID, TIMESTAMPTZOID};

use common_jsonapi_seams as jsonapi;
use common_hashfn_seams as hashfn;
use backend_utils_adt_jsonfuncs_seams as catalog_fmgr;

pub use types_json::{
    ArrayForJson, CompositeFieldForJson, JsonParseErrorType as JsonParseError,
    JsonTokenType as JsonToken, JsonTypeCategory as JsonType,
};

/// C: `PROVOLATILE_IMMUTABLE` (`'i'`) from `catalog/pg_proc.h`.
pub const PROVOLATILE_IMMUTABLE: u8 = b'i';

/// C: `InvalidOid`.
pub const InvalidOid: Oid = 0;

// ---------------------------------------------------------------------------
// Charged working-buffer helpers. Mirror PostgreSQL's `StringInfo`
// (`appendBinaryStringInfo` / `appendStringInfoChar`): every byte appended to a
// json result grows the context-charged spine fallibly, like `enlargeStringInfo`
// reserving in the current context.
// ---------------------------------------------------------------------------

/// Out-of-memory / over-limit error (a failed `palloc` / `!AllocSizeIsValid`).
fn alloc_failure() -> PgError {
    PgError::error("out of memory").with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
}

/// `appendBinaryStringInfo(buf, src, len)` — append `src`, validating the total
/// requested capacity against `MaxAllocSize` and charging the buffer's context.
fn buf_extend(buf: &mut PgVec<'_, u8>, src: &[u8]) -> PgResult<()> {
    let want = buf.len().checked_add(src.len()).filter(|&n| n <= MAX_ALLOC_SIZE);
    if want.is_none() {
        return Err(alloc_failure());
    }
    buf.try_reserve(src.len()).map_err(|_| alloc_failure())?;
    buf.extend_from_slice(src);
    Ok(())
}

/// `appendStringInfoChar(buf, c)` — append one byte.
#[inline]
fn buf_push(buf: &mut PgVec<'_, u8>, c: u8) -> PgResult<()> {
    buf.try_reserve(1).map_err(|_| alloc_failure())?;
    buf.push(c);
    Ok(())
}

/// Run a one-shot builder against a fresh context-charged byte spine.
fn build<'mcx, F>(mcx: Mcx<'mcx>, f: F) -> PgResult<PgVec<'mcx, u8>>
where
    F: FnOnce(&mut PgVec<'mcx, u8>) -> PgResult<()>,
{
    let mut buf = PgVec::new_in(mcx);
    f(&mut buf)?;
    Ok(buf)
}

// ===========================================================================
// Input / output. (json.c:106-167)
// ===========================================================================

/// C: `json_in(PG_FUNCTION_ARGS)` (json.c:106).
///
/// `json` is the input cstring's bytes; the internal representation is the same
/// as text, so on success the validated bytes are returned unchanged. A
/// swallowed soft error surfaces as `Ok(None)`; a hard error as `Err`.
pub fn json_in<'mcx>(mcx: Mcx<'mcx>, json: &[u8]) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let result = jsonapi::parse_validate::call(json);
    if result != JsonParseErrorType::JSON_SUCCESS {
        jsonapi::errsave_error::call(result, json)?;
        return Ok(None);
    }
    Ok(Some(mcx::slice_in(mcx, json)?))
}

/// C: `json_out(PG_FUNCTION_ARGS)` (json.c:125) — a `json` value is its own
/// text, so output is the (detoasted) content bytes verbatim.
pub fn json_out<'mcx>(mcx: Mcx<'mcx>, json: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    mcx::slice_in(mcx, json)
}

/// C: `json_send(PG_FUNCTION_ARGS)` (json.c:137) — binary send is the text
/// bytes wrapped by `pq_begintypsend`/`pq_endtypsend`. We return the body the
/// wire layer frames; the body is the value's content bytes.
pub fn json_send<'mcx>(mcx: Mcx<'mcx>, json: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    mcx::slice_in(mcx, json)
}

/// C: `json_recv(PG_FUNCTION_ARGS)` (json.c:151) — read the message text and
/// validate it; the stored representation is the same text.
pub fn json_recv<'mcx>(mcx: Mcx<'mcx>, str: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let result = jsonapi::parse_validate::call(str);
    if result != JsonParseErrorType::JSON_SUCCESS {
        jsonapi::errsave_error::call(result, str)?;
        // pg_parse_json_or_ereport never returns on failure.
        return Err(unreached_soft_error());
    }
    mcx::slice_in(mcx, str)
}

// ===========================================================================
// datum_to_json_internal and friends. (json.c:178-302)
// ===========================================================================

/// C: `datum_to_json_internal(Datum val, bool is_null, StringInfo result,
/// JsonTypeCategory tcategory, Oid outfuncoid, bool key_scalar)` (json.c:178).
///
/// Appends the JSON text for `val` onto `result`. Array/composite rendering and
/// the fmgr output/cast functions are reached through the seams.
pub fn datum_to_json_internal<'mcx>(
    val: &Datum<'mcx>,
    is_null: bool,
    result: &mut PgVec<'_, u8>,
    tcategory: JsonTypeCategory,
    outfuncoid: Oid,
    key_scalar: bool,
) -> PgResult<()> {
    use JsonTypeCategory::*;

    // C: check_stack_depth() — array/composite rendering recurses, so guard the
    // execution stack (ERRCODE_STATEMENT_TOO_COMPLEX) before descending.
    backend_utils_misc_stack_depth_seams::check_stack_depth::call()?;

    // callers are expected to ensure that null keys are not passed in
    debug_assert!(!(key_scalar && is_null));

    if is_null {
        buf_extend(result, b"null")?;
        return Ok(());
    }

    if key_scalar
        && (tcategory == JSONTYPE_ARRAY
            || tcategory == JSONTYPE_COMPOSITE
            || tcategory == JSONTYPE_JSON
            || tcategory == JSONTYPE_CAST)
    {
        return Err(
            PgError::error("key value must be scalar, not array, composite, or json")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        );
    }

    match tcategory {
        JSONTYPE_ARRAY => {
            array_to_json_internal(val, result, false)?;
        }
        JSONTYPE_COMPOSITE => {
            composite_to_json(val, result, false)?;
        }
        JSONTYPE_BOOL => {
            if key_scalar {
                buf_push(result, b'"')?;
            }
            if val.as_bool() {
                buf_extend(result, b"true")?;
            } else {
                buf_extend(result, b"false")?;
            }
            if key_scalar {
                buf_push(result, b'"')?;
            }
        }
        JSONTYPE_NUMERIC => {
            let outputstr = catalog_fmgr::output_function_call::call(outfuncoid, val)?;

            // Don't quote a non-key if it's a valid JSON number (i.e., not
            // "Infinity", "-Infinity", or "NaN"). We open-code the validation:
            // a valid number starts with a digit, or '-' followed by a digit.
            let is_number = match outputstr.first() {
                Some(&c0) if c0.is_ascii_digit() => true,
                Some(&b'-') => matches!(outputstr.get(1), Some(c1) if c1.is_ascii_digit()),
                _ => false,
            };
            if !key_scalar && is_number {
                buf_extend(result, &outputstr)?;
            } else {
                buf_push(result, b'"')?;
                buf_extend(result, &outputstr)?;
                buf_push(result, b'"')?;
            }
        }
        JSONTYPE_DATE => {
            let buf = JsonEncodeDateTime(val, DATEOID, None)?;
            buf_push(result, b'"')?;
            buf_extend(result, buf.as_bytes())?;
            buf_push(result, b'"')?;
        }
        JSONTYPE_TIMESTAMP => {
            let buf = JsonEncodeDateTime(val, TIMESTAMPOID, None)?;
            buf_push(result, b'"')?;
            buf_extend(result, buf.as_bytes())?;
            buf_push(result, b'"')?;
        }
        JSONTYPE_TIMESTAMPTZ => {
            let buf = JsonEncodeDateTime(val, TIMESTAMPTZOID, None)?;
            buf_push(result, b'"')?;
            buf_extend(result, buf.as_bytes())?;
            buf_push(result, b'"')?;
        }
        JSONTYPE_JSON => {
            // JSON and JSONB output will already be escaped.
            let outputstr = catalog_fmgr::output_function_call::call(outfuncoid, val)?;
            buf_extend(result, &outputstr)?;
        }
        JSONTYPE_CAST => {
            // outfuncoid refers to a cast function, not an output function.
            let jsontext = catalog_fmgr::cast_function_call::call(outfuncoid, val)?;
            buf_extend(result, &jsontext)?;
        }
        // C's `switch` has explicit cases above and a `default:` covering
        // JSONTYPE_JSONB / JSONTYPE_NULL / JSONTYPE_OTHER. JSONTYPE_JSONB and
        // JSONTYPE_NULL are unreachable in json.c flows (is_jsonb=false never
        // yields JSONTYPE_JSONB; a null Datum is early-returned above).
        JSONTYPE_NULL | JSONTYPE_JSONB | JSONTYPE_OTHER => {
            // special-case text types to save useless palloc/memcpy cycles
            if catalog_fmgr::is_text_output_func::call(outfuncoid) {
                let txt = catalog_fmgr::text_datum_bytes::call(val)?;
                escape_json_with_len(result, &txt)?;
            } else {
                let outputstr = catalog_fmgr::output_function_call::call(outfuncoid, val)?;
                escape_json(result, &outputstr)?;
            }
        }
    }

    Ok(())
}

/// C: `JsonEncodeDateTime(char *buf, Datum value, Oid typid, const int *tzp)`
/// (json.c:309).
///
/// Encodes a datetime Datum into ISO format (forcing XSD date style), returning
/// the formatted string. `tzp`, if `Some`, is the time-zone offset in seconds
/// for `timestamptz`. The body is entirely the datetime subsystem's field
/// conversions + `Encode*` routines, reached through the seam.
pub fn JsonEncodeDateTime<'mcx>(
    value: &Datum<'mcx>,
    typid: Oid,
    tzp: Option<i32>,
) -> PgResult<String> {
    // The actual datetime field-conversion owner (`timestamp.c`) is unported;
    // its `json_encode_datetime` seam now carries the canonical
    // `types_tuple::Datum<'mcx>` (by reference), so forward the value unchanged.
    backend_utils_adt_timestamp_seams::json_encode_datetime::call(value, typid, tzp)
}

/// C: `array_dim_to_json(StringInfo result, int dim, int ndims, int *dims,
/// Datum *vals, bool *nulls, int *valcount, JsonTypeCategory tcategory, Oid
/// outfuncoid, bool use_line_feeds)` (json.c:430).
///
/// Process a single dimension of an array, recursing into inner dimensions.
/// `valcount` is advanced as innermost values are consumed.
pub fn array_dim_to_json<'mcx>(
    result: &mut PgVec<'_, u8>,
    dim: usize,
    ndims: usize,
    dims: &[i32],
    vals: &[Datum<'mcx>],
    nulls: &[bool],
    valcount: &mut usize,
    tcategory: JsonTypeCategory,
    outfuncoid: Oid,
    use_line_feeds: bool,
) -> PgResult<()> {
    debug_assert!(dim < ndims);

    let sep: &[u8] = if use_line_feeds { b",\n " } else { b"," };

    buf_push(result, b'[')?;

    let mut i = 1;
    while i <= dims[dim] {
        if i > 1 {
            buf_extend(result, sep)?;
        }

        if dim + 1 == ndims {
            datum_to_json_internal(
                &vals[*valcount],
                nulls[*valcount],
                result,
                tcategory,
                outfuncoid,
                false,
            )?;
            *valcount += 1;
        } else {
            // Do we want line feeds on inner dimensions of arrays? For now we'll
            // say no.
            array_dim_to_json(
                result,
                dim + 1,
                ndims,
                dims,
                vals,
                nulls,
                valcount,
                tcategory,
                outfuncoid,
                false,
            )?;
        }

        i += 1;
    }

    buf_push(result, b']')?;
    Ok(())
}

/// C: `array_to_json_internal(Datum array, StringInfo result, bool
/// use_line_feeds)` (json.c:473). Turn an array into JSON.
pub fn array_to_json_internal<'mcx>(
    array: &Datum<'mcx>,
    result: &mut PgVec<'_, u8>,
    use_line_feeds: bool,
) -> PgResult<()> {
    let arr = catalog_fmgr::deconstruct_array::call(array)?;
    // `deconstruct_array` yields the canonical `Datum<'mcx>` element model
    // (`ArrayForJson.elements`); `array_dim_to_json` / `datum_to_json_internal`
    // drive the per-element `OutputFunctionCall` directly off the canonical
    // value (each by-value element rides `ByVal`, each detoasted by-reference
    // element rides `ByRef`), so no scalar-word collapse is needed.

    // nitems = ArrayGetNItems(ndim, dim). The overflow guard
    // (ArrayGetNItemsSafe) is enforced by the seam (it owns
    // ArrayGetNItems/deconstruct_array); here we recompute the product only to
    // drive the `nitems <= 0` early-return.
    let nitems: i64 = if arr.ndim <= 0 {
        0
    } else {
        let mut n: i64 = 1;
        for &d in &arr.dims {
            n *= d as i64;
        }
        n
    };

    if nitems <= 0 {
        buf_extend(result, b"[]")?;
        return Ok(());
    }

    let mut count = 0usize;
    array_dim_to_json(
        result,
        0,
        arr.ndim as usize,
        &arr.dims,
        &arr.elements,
        &arr.nulls,
        &mut count,
        arr.element_tcategory,
        arr.element_outfuncoid,
        use_line_feeds,
    )
}

/// C: `composite_to_json(Datum composite, StringInfo result, bool
/// use_line_feeds)` (json.c:520). Turn a composite / record into JSON.
pub fn composite_to_json<'mcx>(
    composite: &Datum<'mcx>,
    result: &mut PgVec<'_, u8>,
    use_line_feeds: bool,
) -> PgResult<()> {
    let mut needsep = false;
    // precalculate the separator (avoids strlen in C).
    let sep: &[u8] = if use_line_feeds { b",\n " } else { b"," };

    let fields = catalog_fmgr::walk_composite::call(composite)?;

    buf_push(result, b'{')?;

    for field in &fields {
        // (att->attisdropped fields are already filtered out by walk_composite.)
        if needsep {
            buf_extend(result, sep)?;
        }
        needsep = true;

        escape_json(result, &field.attname)?;
        buf_push(result, b':')?;

        datum_to_json_internal(
            // `walk_composite` yields the canonical `Datum<'mcx>` per attribute
            // (`CompositeFieldForJson.val`); forward it directly.
            &field.val,
            field.is_null,
            result,
            field.tcategory,
            field.outfuncoid,
            false,
        )?;
    }

    buf_push(result, b'}')?;
    Ok(())
}

/// C: `add_json(Datum val, bool is_null, StringInfo result, Oid val_type, bool
/// key_scalar)` (json.c:601). Thin wrapper around `datum_to_json` that
/// classifies `val_type` first.
pub fn add_json<'mcx>(
    val: &Datum<'mcx>,
    is_null: bool,
    result: &mut PgVec<'_, u8>,
    val_type: Oid,
    key_scalar: bool,
) -> PgResult<()> {
    if val_type == InvalidOid {
        return Err(PgError::error("could not determine input data type")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    let (tcategory, outfuncoid) = if is_null {
        (JsonTypeCategory::JSONTYPE_NULL, InvalidOid)
    } else {
        catalog_fmgr::categorize_type::call(val_type)?
    };

    datum_to_json_internal(val, is_null, result, tcategory, outfuncoid, key_scalar)
}

/// C: `to_json_is_immutable(Oid typoid)` (json.c:699).
pub fn to_json_is_immutable(typoid: Oid) -> PgResult<bool> {
    use JsonTypeCategory::*;

    let (tcategory, outfuncoid) = catalog_fmgr::categorize_type::call(typoid)?;

    match tcategory {
        JSONTYPE_BOOL | JSONTYPE_JSON | JSONTYPE_JSONB | JSONTYPE_NULL => Ok(true),

        JSONTYPE_DATE | JSONTYPE_TIMESTAMP | JSONTYPE_TIMESTAMPTZ => Ok(false),

        JSONTYPE_ARRAY => Ok(false), // TODO recurse into elements

        JSONTYPE_COMPOSITE => Ok(false), // TODO recurse into fields

        JSONTYPE_NUMERIC | JSONTYPE_CAST | JSONTYPE_OTHER => {
            Ok(catalog_fmgr::func_volatile::call(outfuncoid)? == PROVOLATILE_IMMUTABLE)
        }
    }
}

/// C: `datum_to_json(Datum val, JsonTypeCategory tcategory, Oid outfuncoid)`
/// (json.c:762). Turn a Datum into JSON text (returned as content bytes).
pub fn datum_to_json<'mcx>(
    mcx: Mcx<'mcx>,
    val: &Datum<'mcx>,
    tcategory: JsonTypeCategory,
    outfuncoid: Oid,
) -> PgResult<PgVec<'mcx, u8>> {
    build(mcx, |buf| {
        datum_to_json_internal(val, false, buf, tcategory, outfuncoid, false)
    })
}

// ===========================================================================
// SQL entry points for whole-Datum rendering. (json.c:629-755)
//
// The fmgr arg marshaling (`PG_GETARG_DATUM`, `get_fn_expr_argtype`) is the
// executor boundary; these take the already-extracted Datum (and the resolved
// argument type for to_json) and return the json text content bytes.
// ===========================================================================

/// C: `array_to_json(PG_FUNCTION_ARGS)` (json.c:629).
pub fn array_to_json<'mcx>(mcx: Mcx<'mcx>, array: &Datum<'mcx>) -> PgResult<PgVec<'mcx, u8>> {
    build(mcx, |buf| array_to_json_internal(array, buf, false))
}

/// C: `array_to_json_pretty(PG_FUNCTION_ARGS)` (json.c:645).
pub fn array_to_json_pretty<'mcx>(
    mcx: Mcx<'mcx>,
    array: &Datum<'mcx>,
    use_line_feeds: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    build(mcx, |buf| array_to_json_internal(array, buf, use_line_feeds))
}

/// C: `row_to_json(PG_FUNCTION_ARGS)` (json.c:662).
pub fn row_to_json<'mcx>(mcx: Mcx<'mcx>, array: &Datum<'mcx>) -> PgResult<PgVec<'mcx, u8>> {
    build(mcx, |buf| composite_to_json(array, buf, false))
}

/// C: `row_to_json_pretty(PG_FUNCTION_ARGS)` (json.c:678).
pub fn row_to_json_pretty<'mcx>(
    mcx: Mcx<'mcx>,
    array: &Datum<'mcx>,
    use_line_feeds: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    build(mcx, |buf| composite_to_json(array, buf, use_line_feeds))
}

/// C: `to_json(PG_FUNCTION_ARGS)` (json.c:738).
///
/// `val_type` is `get_fn_expr_argtype(fcinfo->flinfo, 0)` (executor boundary).
pub fn to_json<'mcx>(mcx: Mcx<'mcx>, val: &Datum<'mcx>, val_type: Oid) -> PgResult<PgVec<'mcx, u8>> {
    if val_type == InvalidOid {
        return Err(PgError::error("could not determine input data type")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    let (tcategory, outfuncoid) = catalog_fmgr::categorize_type::call(val_type)?;

    datum_to_json(mcx, val, tcategory, outfuncoid)
}

// ===========================================================================
// Unique-key check support. (json.c:898-994)
//
// The C code uses a dynahash table keyed by (object_id, key_len, key). The hash
// and match functions are ported verbatim; we keep the entries in a Vec because
// the only operation is "insert if not present" (HASH_ENTER + found), which is
// what `json_unique_check_key` performs, and the table never outlives a single
// object/build/parse.
// ===========================================================================

/// C: `JsonUniqueHashEntry` (json.c:44).
#[derive(Clone, Debug)]
struct JsonUniqueHashEntry {
    key: alloc::vec::Vec<u8>,
    key_len: i32,
    object_id: i32,
}

/// C: `JsonUniqueCheckState` — hash table for key names (json.c:41).
#[derive(Debug, Default)]
pub struct JsonUniqueCheckState {
    entries: alloc::vec::Vec<JsonUniqueHashEntry>,
}

/// C: `JsonUniqueBuilderState` (json.c:69).
#[derive(Debug, Default)]
pub struct JsonUniqueBuilderState {
    /// C: `JsonUniqueCheckState check`.
    pub check: JsonUniqueCheckState,
    /// C: `StringInfoData skipped_keys` — the throwaway buffer, lazily
    /// initialized (None == `skipped_keys.data == NULL`).
    skipped_keys: Option<alloc::vec::Vec<u8>>,
}

/// C: `json_unique_hash(const void *key, Size keysize)` (json.c:899).
fn json_unique_hash(entry: &JsonUniqueHashEntry) -> u32 {
    let mut hash = hashfn::hash_bytes_uint32::call(entry.object_id as u32);
    hash ^= hashfn::tag_hash::call(&entry.key[..entry.key_len as usize], entry.key_len as usize);
    hash
}

/// C: `json_unique_hash_match(const void *key1, const void *key2, Size
/// keysize)` (json.c:910).
fn json_unique_hash_match(entry1: &JsonUniqueHashEntry, entry2: &JsonUniqueHashEntry) -> i32 {
    if entry1.object_id != entry2.object_id {
        return if entry1.object_id > entry2.object_id { 1 } else { -1 };
    }

    if entry1.key_len != entry2.key_len {
        return if entry1.key_len > entry2.key_len { 1 } else { -1 };
    }

    // strncmp(entry1->key, entry2->key, entry1->key_len)
    let n = entry1.key_len as usize;
    let a = &entry1.key[..n];
    let b = &entry2.key[..n.min(entry2.key.len())];
    match a.cmp(b) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    }
}

/// C: `json_unique_check_init(JsonUniqueCheckState *cxt)` (json.c:931).
pub fn json_unique_check_init(cxt: &mut JsonUniqueCheckState) {
    cxt.entries.clear();
}

/// C: `json_unique_builder_init(JsonUniqueBuilderState *cxt)` (json.c:949).
pub fn json_unique_builder_init(cxt: &mut JsonUniqueBuilderState) {
    json_unique_check_init(&mut cxt.check);
    cxt.skipped_keys = None;
}

/// C: `json_unique_check_key(JsonUniqueCheckState *cxt, const char *key, int
/// object_id)` (json.c:957). Returns `true` if the key was *not* already present
/// (i.e. HASH_ENTER reported `!found`).
pub fn json_unique_check_key(cxt: &mut JsonUniqueCheckState, key: &[u8], object_id: i32) -> bool {
    let entry = JsonUniqueHashEntry {
        key: key.to_vec(),
        key_len: key.len() as i32,
        object_id,
    };

    // hash_search(*cxt, &entry, HASH_ENTER, &found): the hash + match functions
    // give the same equality semantics as the dynahash probe.
    let probe_hash = json_unique_hash(&entry);
    let found = cxt.entries.iter().any(|existing| {
        json_unique_hash(existing) == probe_hash && json_unique_hash_match(existing, &entry) == 0
    });

    if !found {
        cxt.entries.push(entry);
    }

    !found
}

/// C: `json_unique_builder_get_throwawaybuf(JsonUniqueBuilderState *cxt)`
/// (json.c:977). On-demand init (and reset) of a throwaway buffer for reading
/// skipped (NULL-valued) keys.
pub fn json_unique_builder_get_throwawaybuf(
    cxt: &mut JsonUniqueBuilderState,
) -> &mut alloc::vec::Vec<u8> {
    match cxt.skipped_keys {
        None => {
            cxt.skipped_keys = Some(alloc::vec::Vec::new());
        }
        Some(ref mut buf) => {
            buf.clear();
        }
    }
    cxt.skipped_keys.as_mut().unwrap()
}

// ===========================================================================
// Unique-key check semantic actions, driven by the parser. (json.c:51-66,
// 1753-1808)
//
// The recursive-descent parse loop is in `src/common/jsonapi.c` (seamed); these
// three callbacks are pure and live in-crate. The seam's
// `parse_validate_unique` runs the parser and invokes these on the shared
// `JsonUniqueParsingState` exactly as `pg_parse_json` would call the
// `JsonSemAction` hooks.
// ===========================================================================

/// C: `JsonUniqueParsingState` (json.c:59); the linked `JsonUniqueStackEntry`
/// (json.c:52) list is a LIFO `Vec` of `object_id`s.
#[derive(Debug, Default)]
pub struct JsonUniqueParsingState {
    /// C: `JsonUniqueCheckState check`.
    pub check: JsonUniqueCheckState,
    /// C: the `JsonUniqueStackEntry *stack` linked list, as a LIFO stack of
    /// object ids (`entry->object_id`).
    pub stack: alloc::vec::Vec<i32>,
    /// C: `int id_counter`.
    pub id_counter: i32,
    /// C: `bool unique`.
    pub unique: bool,
}

impl JsonUniqueParsingState {
    /// Initialize as `json_validate` does.
    pub fn new() -> Self {
        let mut s = Self {
            check: JsonUniqueCheckState::default(),
            stack: alloc::vec::Vec::new(),
            id_counter: 0,
            unique: true,
        };
        json_unique_check_init(&mut s.check);
        s
    }
}

/// C: `json_unique_object_start(void *_state)` (json.c:1753).
pub fn json_unique_object_start(state: &mut JsonUniqueParsingState) -> JsonParseErrorType {
    if !state.unique {
        return JsonParseErrorType::JSON_SUCCESS;
    }

    // push object entry to stack
    let object_id = state.id_counter;
    state.id_counter += 1;
    state.stack.push(object_id);

    JsonParseErrorType::JSON_SUCCESS
}

/// C: `json_unique_object_end(void *_state)` (json.c:1771).
pub fn json_unique_object_end(state: &mut JsonUniqueParsingState) -> JsonParseErrorType {
    if !state.unique {
        return JsonParseErrorType::JSON_SUCCESS;
    }

    state.stack.pop(); // pop object from stack
    JsonParseErrorType::JSON_SUCCESS
}

/// C: `json_unique_object_field_start(void *_state, char *field, bool isnull)`
/// (json.c:1786). `field` is the (NUL-free) key name bytes.
pub fn json_unique_object_field_start(
    state: &mut JsonUniqueParsingState,
    field: &[u8],
    _isnull: bool,
) -> JsonParseErrorType {
    if !state.unique {
        return JsonParseErrorType::JSON_SUCCESS;
    }

    // find key collision in the current object
    let object_id = *state.stack.last().expect("field start within an object");
    if json_unique_check_key(&mut state.check, field, object_id) {
        return JsonParseErrorType::JSON_SUCCESS;
    }

    state.unique = false;

    // pop all objects entries
    state.stack.clear();

    JsonParseErrorType::JSON_SUCCESS
}

// ===========================================================================
// JSON aggregates. (json.c:778-896, 1001-1201)
//
// The fmgr/nodeAgg argument marshaling and `AggCheckCallContext` are the
// executor boundary; the worker logic below takes already-extracted typed
// inputs and the persistent `JsonAggState` directly so the array assembly /
// uniqueness logic is byte-for-byte faithful.
// ===========================================================================

/// C: `JsonAggState` (json.c:78) — the "internal" transition value. Its
/// `StringInfo str` is the context-charged byte spine tied to the aggregate's
/// memory context (`'mcx`).
#[derive(Debug)]
pub struct JsonAggState<'mcx> {
    /// C: `StringInfo str`.
    pub str: PgVec<'mcx, u8>,
    /// C: `JsonTypeCategory key_category`.
    pub key_category: Option<JsonTypeCategory>,
    /// C: `Oid key_output_func`.
    pub key_output_func: Oid,
    /// C: `JsonTypeCategory val_category`.
    pub val_category: Option<JsonTypeCategory>,
    /// C: `Oid val_output_func`.
    pub val_output_func: Oid,
    /// C: `JsonUniqueBuilderState unique_check`.
    pub unique_check: JsonUniqueBuilderState,
}

impl<'mcx> JsonAggState<'mcx> {
    /// A fresh state with an empty buffer in `mcx`.
    fn new(mcx: Mcx<'mcx>) -> Self {
        JsonAggState {
            str: PgVec::new_in(mcx),
            key_category: None,
            key_output_func: InvalidOid,
            val_category: None,
            val_output_func: InvalidOid,
            unique_check: JsonUniqueBuilderState::default(),
        }
    }
}

/// C: `json_agg_transfn_worker(FunctionCallInfo fcinfo, bool absent_on_null)`
/// (json.c:778).
///
/// `state` is the persistent transition value (`None` on the first call, where
/// `arg_type` must be the resolved type of arg 1). Returns the (possibly newly
/// created) state.
pub fn json_agg_transfn_worker<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<JsonAggState<'mcx>>,
    arg_type: Oid,
    val: &Datum<'mcx>,
    val_is_null: bool,
    absent_on_null: bool,
) -> PgResult<JsonAggState<'mcx>> {
    let first_call = state.is_none();
    let mut state = match state {
        None => {
            if arg_type == InvalidOid {
                return Err(PgError::error("could not determine input data type")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }

            let mut s = JsonAggState::new(mcx);
            buf_push(&mut s.str, b'[')?;
            let (cat, out) = catalog_fmgr::categorize_type::call(arg_type)?;
            s.val_category = Some(cat);
            s.val_output_func = out;
            s
        }
        Some(s) => s,
    };

    if absent_on_null && val_is_null {
        return Ok(state);
    }

    if state.str.len() > 1 {
        buf_extend(&mut state.str, b", ")?;
    }

    // fast path for NULLs
    if val_is_null {
        datum_to_json_internal(
            &Datum::null(),
            true,
            &mut state.str,
            JsonTypeCategory::JSONTYPE_NULL,
            InvalidOid,
            false,
        )?;
        return Ok(state);
    }

    // add some whitespace if structured type and not first item
    if !first_call
        && state.str.len() > 1
        && (state.val_category == Some(JsonTypeCategory::JSONTYPE_ARRAY)
            || state.val_category == Some(JsonTypeCategory::JSONTYPE_COMPOSITE))
    {
        buf_extend(&mut state.str, b"\n ")?;
    }

    let val_category = state.val_category.ok_or_else(|| {
        PgError::error("json_agg_transfn_worker: val_category is not set on a non-first call")
    })?;
    let val_output_func = state.val_output_func;
    datum_to_json_internal(val, false, &mut state.str, val_category, val_output_func, false)?;

    Ok(state)
}

/// C: `json_agg_transfn(PG_FUNCTION_ARGS)` (json.c:861).
pub fn json_agg_transfn<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<JsonAggState<'mcx>>,
    arg_type: Oid,
    val: &Datum<'mcx>,
    val_is_null: bool,
) -> PgResult<JsonAggState<'mcx>> {
    json_agg_transfn_worker(mcx, state, arg_type, val, val_is_null, false)
}

/// C: `json_agg_strict_transfn(PG_FUNCTION_ARGS)` (json.c:870).
pub fn json_agg_strict_transfn<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<JsonAggState<'mcx>>,
    arg_type: Oid,
    val: &Datum<'mcx>,
    val_is_null: bool,
) -> PgResult<JsonAggState<'mcx>> {
    json_agg_transfn_worker(mcx, state, arg_type, val, val_is_null, true)
}

/// C: `json_agg_finalfn(PG_FUNCTION_ARGS)` (json.c:879). Returns `None` for the
/// no-rows case (`PG_RETURN_NULL`), else the array text with `]` appended.
pub fn json_agg_finalfn<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<&JsonAggState<'_>>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let state = match state {
        None => return Ok(None),
        Some(s) => s,
    };
    Ok(Some(catenate_stringinfo_string(mcx, state.str.as_slice(), b"]")?))
}

/// C: `json_object_agg_transfn_worker(FunctionCallInfo fcinfo, bool
/// absent_on_null, bool unique_keys)` (json.c:1001).
pub fn json_object_agg_transfn_worker<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<JsonAggState<'mcx>>,
    key_arg_type: Oid,
    val_arg_type: Oid,
    key: &Datum<'mcx>,
    key_is_null: bool,
    val: &Datum<'mcx>,
    val_is_null: bool,
    absent_on_null: bool,
    unique_keys: bool,
) -> PgResult<JsonAggState<'mcx>> {
    let mut state = match state {
        None => {
            let mut s = JsonAggState::new(mcx);
            if unique_keys {
                json_unique_builder_init(&mut s.unique_check);
            }

            if key_arg_type == InvalidOid {
                return Err(PgError::error("could not determine data type for argument 1")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
            let (kcat, kout) = catalog_fmgr::categorize_type::call(key_arg_type)?;
            s.key_category = Some(kcat);
            s.key_output_func = kout;

            if val_arg_type == InvalidOid {
                return Err(PgError::error("could not determine data type for argument 2")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
            }
            let (vcat, vout) = catalog_fmgr::categorize_type::call(val_arg_type)?;
            s.val_category = Some(vcat);
            s.val_output_func = vout;

            buf_extend(&mut s.str, b"{ ")?;
            s
        }
        Some(s) => s,
    };

    if key_is_null {
        return Err(PgError::error("null value not allowed for object key")
            .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED));
    }

    // Skip null values if absent_on_null
    let skip = absent_on_null && val_is_null;

    let key_category = state.key_category.expect("key_category set on first call");
    let key_output_func = state.key_output_func;

    // Render the key into the throwaway buffer (skipped) or state.str.
    let key_bytes: alloc::vec::Vec<u8>;
    if skip {
        // We got a NULL value and we're not storing those; if we're not testing
        // key uniqueness, we're done. Otherwise, use the throwaway buffer.
        if !unique_keys {
            return Ok(state);
        }

        let mut out = PgVec::new_in(mcx);
        datum_to_json_internal(key, false, &mut out, key_category, key_output_func, true)?;
        key_bytes = out.as_slice().to_vec();
    } else {
        // Append comma delimiter only if we have output some fields after "{ ".
        if state.str.len() > 2 {
            buf_extend(&mut state.str, b", ")?;
        }
        let key_offset = state.str.len();
        datum_to_json_internal(key, false, &mut state.str, key_category, key_output_func, true)?;
        key_bytes = state.str[key_offset..].to_vec();
    }

    if unique_keys {
        // check key uniqueness after appending (copy the key first)
        if !json_unique_check_key(&mut state.unique_check.check, &key_bytes, 0) {
            return Err(PgError::error(alloc::format!(
                "duplicate JSON object key value: {}",
                String::from_utf8_lossy(&key_bytes)
            ))
            .with_sqlstate(ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE));
        }

        if skip {
            return Ok(state);
        }
    }

    buf_extend(&mut state.str, b" : ")?;

    let null_arg = Datum::null();
    let arg = if val_is_null { &null_arg } else { val };
    let val_category = state.val_category.expect("val_category set on first call");
    let val_output_func = state.val_output_func;
    datum_to_json_internal(arg, val_is_null, &mut state.str, val_category, val_output_func, false)?;

    Ok(state)
}

/// C: `json_object_agg_transfn(PG_FUNCTION_ARGS)` (json.c:1150).
pub fn json_object_agg_transfn<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<JsonAggState<'mcx>>,
    key_arg_type: Oid,
    val_arg_type: Oid,
    key: &Datum<'mcx>,
    key_is_null: bool,
    val: &Datum<'mcx>,
    val_is_null: bool,
) -> PgResult<JsonAggState<'mcx>> {
    json_object_agg_transfn_worker(
        mcx, state, key_arg_type, val_arg_type, key, key_is_null, val, val_is_null, false, false,
    )
}

/// C: `json_object_agg_strict_transfn(PG_FUNCTION_ARGS)` (json.c:1159).
pub fn json_object_agg_strict_transfn<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<JsonAggState<'mcx>>,
    key_arg_type: Oid,
    val_arg_type: Oid,
    key: &Datum<'mcx>,
    key_is_null: bool,
    val: &Datum<'mcx>,
    val_is_null: bool,
) -> PgResult<JsonAggState<'mcx>> {
    json_object_agg_transfn_worker(
        mcx, state, key_arg_type, val_arg_type, key, key_is_null, val, val_is_null, true, false,
    )
}

/// C: `json_object_agg_unique_transfn(PG_FUNCTION_ARGS)` (json.c:1168).
pub fn json_object_agg_unique_transfn<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<JsonAggState<'mcx>>,
    key_arg_type: Oid,
    val_arg_type: Oid,
    key: &Datum<'mcx>,
    key_is_null: bool,
    val: &Datum<'mcx>,
    val_is_null: bool,
) -> PgResult<JsonAggState<'mcx>> {
    json_object_agg_transfn_worker(
        mcx, state, key_arg_type, val_arg_type, key, key_is_null, val, val_is_null, false, true,
    )
}

/// C: `json_object_agg_unique_strict_transfn(PG_FUNCTION_ARGS)` (json.c:1177).
pub fn json_object_agg_unique_strict_transfn<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<JsonAggState<'mcx>>,
    key_arg_type: Oid,
    val_arg_type: Oid,
    key: &Datum<'mcx>,
    key_is_null: bool,
    val: &Datum<'mcx>,
    val_is_null: bool,
) -> PgResult<JsonAggState<'mcx>> {
    json_object_agg_transfn_worker(
        mcx, state, key_arg_type, val_arg_type, key, key_is_null, val, val_is_null, true, true,
    )
}

/// C: `json_object_agg_finalfn(PG_FUNCTION_ARGS)` (json.c:1186). Returns `None`
/// for the no-rows case, else the object text with " }" appended.
pub fn json_object_agg_finalfn<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<&JsonAggState<'_>>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let state = match state {
        None => return Ok(None),
        Some(s) => s,
    };
    Ok(Some(catenate_stringinfo_string(mcx, state.str.as_slice(), b" }")?))
}

/// C: `catenate_stringinfo_string(StringInfo buffer, const char *addon)`
/// (json.c:1208). Return the buffer's contents plus a trailing string.
pub fn catenate_stringinfo_string<'mcx>(
    mcx: Mcx<'mcx>,
    buffer: &[u8],
    addon: &[u8],
) -> PgResult<PgVec<'mcx, u8>> {
    build(mcx, |out| {
        buf_extend(out, buffer)?;
        buf_extend(out, addon)
    })
}

// ===========================================================================
// json_build_object / json_build_array. (json.c:1223-1397)
// ===========================================================================

/// C: `json_build_object_worker(int nargs, const Datum *args, const bool
/// *nulls, const Oid *types, bool absent_on_null, bool unique_keys)`
/// (json.c:1223).
pub fn json_build_object_worker<'mcx>(
    mcx: Mcx<'mcx>,
    args: &[Datum<'mcx>],
    nulls: &[bool],
    types: &[Oid],
    absent_on_null: bool,
    unique_keys: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    let nargs = args.len();

    if !nargs.is_multiple_of(2) {
        return Err(PgError::error("argument list must have even number of elements")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .with_hint(
                "The arguments of json_build_object() must consist of alternating keys and values.",
            ));
    }

    build(mcx, |result| {
        let mut sep: &[u8] = b"";

        buf_push(result, b'{')?;

        let mut unique_check = JsonUniqueBuilderState::default();
        if unique_keys {
            json_unique_builder_init(&mut unique_check);
        }

        let mut i = 0;
        while i < nargs {
            // Skip null values if absent_on_null
            let skip = absent_on_null && nulls[i + 1];

            // process key
            if nulls[i] {
                return Err(PgError::error("null value not allowed for object key")
                    .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED));
            }

            let key_bytes: alloc::vec::Vec<u8>;
            if skip {
                // If key uniqueness check is needed we must save skipped keys.
                if !unique_keys {
                    i += 2;
                    continue;
                }

                // C uses a throwaway StringInfo to hold the key bytes just long
                // enough to copy them for the uniqueness check.
                let mut out = PgVec::new_in(mcx);
                add_json(&args[i], false, &mut out, types[i], true)?;
                key_bytes = out.as_slice().to_vec();
            } else {
                buf_extend(result, sep)?;
                sep = b", ";
                let key_offset = result.len();
                add_json(&args[i], false, result, types[i], true)?;
                key_bytes = result[key_offset..].to_vec();
            }

            if unique_keys {
                // check key uniqueness after key appending (copy the key first)
                if !json_unique_check_key(&mut unique_check.check, &key_bytes, 0) {
                    return Err(PgError::error(alloc::format!(
                        "duplicate JSON object key value: {}",
                        String::from_utf8_lossy(&key_bytes)
                    ))
                    .with_sqlstate(ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE));
                }

                if skip {
                    i += 2;
                    continue;
                }
            }

            buf_extend(result, b" : ")?;

            // process value
            add_json(&args[i + 1], nulls[i + 1], result, types[i + 1], false)?;

            i += 2;
        }

        buf_push(result, b'}')
    })
}

/// C: `json_build_object(PG_FUNCTION_ARGS)` (json.c:1317).
///
/// `extract_variadic_args` is the executor boundary; `args`/`nulls`/`types` are
/// the already-extracted variadic arguments. Returns `None` for the SQL-NULL
/// case (C `nargs < 0` -> `PG_RETURN_NULL`), modeled as `extracted == None`.
pub fn json_build_object<'mcx>(
    mcx: Mcx<'mcx>,
    extracted: Option<(&[Datum<'mcx>], &[bool], &[Oid])>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    match extracted {
        None => Ok(None),
        Some((args, nulls, types)) => {
            Ok(Some(json_build_object_worker(mcx, args, nulls, types, false, false)?))
        }
    }
}

/// C: `json_build_object_noargs(PG_FUNCTION_ARGS)` (json.c:1337).
pub fn json_build_object_noargs<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, u8>> {
    mcx::slice_in(mcx, b"{}")
}

/// C: `json_build_array_worker(int nargs, const Datum *args, const bool *nulls,
/// const Oid *types, bool absent_on_null)` (json.c:1343).
pub fn json_build_array_worker<'mcx>(
    mcx: Mcx<'mcx>,
    args: &[Datum<'mcx>],
    nulls: &[bool],
    types: &[Oid],
    absent_on_null: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    let nargs = args.len();
    build(mcx, |result| {
        let mut sep: &[u8] = b"";

        buf_push(result, b'[')?;

        for i in 0..nargs {
            if absent_on_null && nulls[i] {
                continue;
            }

            buf_extend(result, sep)?;
            sep = b", ";
            add_json(&args[i], nulls[i], result, types[i], false)?;
        }

        buf_push(result, b']')
    })
}

/// C: `json_build_array(PG_FUNCTION_ARGS)` (json.c:1373).
pub fn json_build_array<'mcx>(
    mcx: Mcx<'mcx>,
    extracted: Option<(&[Datum<'mcx>], &[bool], &[Oid])>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    match extracted {
        None => Ok(None),
        Some((args, nulls, types)) => {
            Ok(Some(json_build_array_worker(mcx, args, nulls, types, false)?))
        }
    }
}

/// C: `json_build_array_noargs(PG_FUNCTION_ARGS)` (json.c:1393).
pub fn json_build_array_noargs<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, u8>> {
    mcx::slice_in(mcx, b"[]")
}

// ===========================================================================
// json_object(text[]) / json_object(text[], text[]). (json.c:1405-1555)
//
// These work purely on already-deconstructed text element Datums (the
// element-byte payloads) plus null flags. `ndims`/`dims` match the C checks.
// ===========================================================================

/// C: `json_object(PG_FUNCTION_ARGS)` (json.c:1405) — one- or two-dimensional
/// text array of alternating key/value pairs.
pub fn json_object<'mcx>(
    mcx: Mcx<'mcx>,
    ndims: i32,
    dims: &[i32],
    in_datums: &[&[u8]],
    in_nulls: &[bool],
) -> PgResult<PgVec<'mcx, u8>> {
    match ndims {
        0 => {
            return mcx::slice_in(mcx, b"{}");
        }
        1 => {
            if dims[0] % 2 != 0 {
                return Err(PgError::error("array must have even number of elements")
                    .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
            }
        }
        2 => {
            if dims[1] != 2 {
                return Err(PgError::error("array must have two columns")
                    .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
            }
        }
        _ => {
            return Err(PgError::error("wrong number of array subscripts")
                .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
        }
    }

    let in_count = in_datums.len();
    let count = in_count / 2;

    build(mcx, |result| {
        buf_push(result, b'{')?;

        for i in 0..count {
            if in_nulls[i * 2] {
                return Err(PgError::error("null value not allowed for object key")
                    .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED));
            }

            if i > 0 {
                buf_extend(result, b", ")?;
            }
            escape_json_text(result, in_datums[i * 2])?;
            buf_extend(result, b" : ")?;
            if in_nulls[i * 2 + 1] {
                buf_extend(result, b"null")?;
            } else {
                escape_json_text(result, in_datums[i * 2 + 1])?;
            }
        }

        buf_push(result, b'}')
    })
}

/// C: `json_object_two_arg(PG_FUNCTION_ARGS)` (json.c:1489) — separate key and
/// value text arrays.
pub fn json_object_two_arg<'mcx>(
    mcx: Mcx<'mcx>,
    nkdims: i32,
    nvdims: i32,
    key_datums: &[&[u8]],
    key_nulls: &[bool],
    val_datums: &[&[u8]],
    val_nulls: &[bool],
) -> PgResult<PgVec<'mcx, u8>> {
    if nkdims > 1 || nkdims != nvdims {
        return Err(PgError::error("wrong number of array subscripts")
            .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
    }

    if nkdims == 0 {
        return mcx::slice_in(mcx, b"{}");
    }

    let key_count = key_datums.len();
    let val_count = val_datums.len();

    if key_count != val_count {
        return Err(PgError::error("mismatched array dimensions")
            .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR));
    }

    build(mcx, |result| {
        buf_push(result, b'{')?;

        for i in 0..key_count {
            if key_nulls[i] {
                return Err(PgError::error("null value not allowed for object key")
                    .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED));
            }

            if i > 0 {
                buf_extend(result, b", ")?;
            }
            escape_json_text(result, key_datums[i])?;
            buf_extend(result, b" : ")?;
            if val_nulls[i] {
                buf_extend(result, b"null")?;
            } else {
                escape_json_text(result, val_datums[i])?;
            }
        }

        buf_push(result, b'}')
    })
}

// ===========================================================================
// Escaping. (json.c:1561-1750)
// ===========================================================================

/// C: `escape_json_char(StringInfo buf, char c)` (json.c:1561). Append one
/// character, escaping JSON metacharacters and control characters.
#[inline]
pub fn escape_json_char(buf: &mut PgVec<'_, u8>, c: u8) -> PgResult<()> {
    match c {
        b'\x08' => buf_extend(buf, b"\\b"),
        b'\x0c' => buf_extend(buf, b"\\f"),
        b'\n' => buf_extend(buf, b"\\n"),
        b'\r' => buf_extend(buf, b"\\r"),
        b'\t' => buf_extend(buf, b"\\t"),
        b'"' => buf_extend(buf, b"\\\""),
        b'\\' => buf_extend(buf, b"\\\\"),
        _ => {
            if c < b' ' {
                // appendStringInfo(buf, "\\u%04x", (int) c)
                let mut tmp = [b'\\', b'u', b'0', b'0', 0, 0];
                const HEX: &[u8; 16] = b"0123456789abcdef";
                tmp[4] = HEX[((c >> 4) & 0xF) as usize];
                tmp[5] = HEX[(c & 0xF) as usize];
                buf_extend(buf, &tmp)
            } else {
                buf_push(buf, c)
            }
        }
    }
}

/// C: `escape_json(StringInfo buf, const char *str)` (json.c:1601). Produce a
/// JSON string literal from a NUL-terminated cstring (stops at the first NUL).
pub fn escape_json(buf: &mut PgVec<'_, u8>, str: &[u8]) -> PgResult<()> {
    buf_push(buf, b'"')?;

    // for (; *str != '\0'; str++): the C loop stops at the first NUL byte.
    for &c in str {
        if c == 0 {
            break;
        }
        escape_json_char(buf, c)?;
    }

    buf_push(buf, b'"')
}

/// C: `escape_json_with_len(StringInfo buf, const char *str, int len)`
/// (json.c:1630). Produce a JSON string literal from possibly-not-NUL-
/// terminated bytes.
///
/// The C version uses a SIMD fast path to flush runs of "safe" bytes; the
/// observable output is identical to escaping every byte, so we port the safe
/// scalar equivalent (find runs with no metacharacter/control byte, flush them
/// wholesale, then per-byte escape). The C `enlargeStringInfo(buf, len + 2)`
/// reservation is made fallibly here against `MaxAllocSize`.
pub fn escape_json_with_len(buf: &mut PgVec<'_, u8>, str: &[u8]) -> PgResult<()> {
    let len = str.len();

    // Validate the data-derived capacity (plus two for the quotes) against
    // MaxAllocSize before growing the spine.
    if len.checked_add(2).filter(|&n| n <= MAX_ALLOC_SIZE).is_none() {
        return Err(alloc_failure());
    }

    buf_push(buf, b'"')?;

    // Equivalent to the vector-search-then-per-byte loop: walk forward, copying
    // maximal runs of bytes that need no escaping, escaping the rest one byte at
    // a time. A byte needs escaping iff it is <= 0x1F, '"', or '\\'.
    let mut copypos = 0;
    let mut i = 0;
    while i < len {
        let c = str[i];
        if c <= 0x1F || c == b'"' || c == b'\\' {
            if copypos < i {
                buf_extend(buf, &str[copypos..i])?;
            }
            escape_json_char(buf, c)?;
            i += 1;
            copypos = i;
        } else {
            i += 1;
        }
    }
    if copypos < len {
        buf_extend(buf, &str[copypos..len])?;
    }

    buf_push(buf, b'"')
}

/// C: `escape_json_text(StringInfo buf, const text *txt)` (json.c:1736).
///
/// `txt` is the detoasted text payload bytes (`VARDATA_ANY`); we escape them
/// with [`escape_json_with_len`]. Detoasting is the caller's responsibility (it
/// owns the varlena), matching the content-byte interface.
pub fn escape_json_text(buf: &mut PgVec<'_, u8>, txt: &[u8]) -> PgResult<()> {
    escape_json_with_len(buf, txt)
}

// ===========================================================================
// json_validate / json_typeof. (json.c:1753-1913)
// ===========================================================================

/// C: `json_validate(text *json, bool check_unique_keys, bool throw_error)`
/// (json.c:1811).
pub fn json_validate(json: &[u8], check_unique_keys: bool, throw_error: bool) -> PgResult<bool> {
    let (result, unique) = if check_unique_keys {
        jsonapi::parse_validate_unique::call(json)
    } else {
        (jsonapi::parse_validate::call(json), true)
    };

    if result != JsonParseErrorType::JSON_SUCCESS {
        if throw_error {
            jsonapi::errsave_error::call(result, json)?;
        }
        return Ok(false); // invalid json
    }

    if check_unique_keys && !unique {
        if throw_error {
            return Err(PgError::error("duplicate JSON object key value")
                .with_sqlstate(ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE));
        }
        return Ok(false); // not unique keys
    }

    Ok(true) // ok
}

/// C: `json_typeof(PG_FUNCTION_ARGS)` (json.c:1873). Returns the type of the
/// outermost JSON value.
pub fn json_typeof(json: &[u8]) -> PgResult<&'static str> {
    use JsonTokenType::*;

    let (result, token_type) = jsonapi::lex_first_token::call(json);
    if result != JsonParseErrorType::JSON_SUCCESS {
        jsonapi::errsave_error::call(result, json)?;
        // json_errsave_error(..., NULL) does not return on a hard error.
        return Err(unreached_soft_error());
    }

    let type_str = match token_type {
        JSON_TOKEN_OBJECT_START => "object",
        JSON_TOKEN_ARRAY_START => "array",
        JSON_TOKEN_STRING => "string",
        JSON_TOKEN_NUMBER => "number",
        JSON_TOKEN_TRUE | JSON_TOKEN_FALSE => "boolean",
        JSON_TOKEN_NULL => "null",
        other => {
            return Err(PgError::error(alloc::format!(
                "unexpected json token: {}",
                token_type_int(other)
            ))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR));
        }
    };

    Ok(type_str)
}

/// The C `lex.token_type` integer, used only in the unreachable
/// `elog(ERROR, "unexpected json token: %d", ...)` diagnostic.
fn token_type_int(t: JsonTokenType) -> i32 {
    use JsonTokenType::*;
    match t {
        JSON_TOKEN_INVALID => 0,
        JSON_TOKEN_STRING => 1,
        JSON_TOKEN_NUMBER => 2,
        JSON_TOKEN_OBJECT_START => 3,
        JSON_TOKEN_OBJECT_END => 4,
        JSON_TOKEN_ARRAY_START => 5,
        JSON_TOKEN_ARRAY_END => 6,
        JSON_TOKEN_COMMA => 7,
        JSON_TOKEN_COLON => 8,
        JSON_TOKEN_TRUE => 9,
        JSON_TOKEN_FALSE => 10,
        JSON_TOKEN_NULL => 11,
        JSON_TOKEN_END => 12,
    }
}

/// Internal-invariant guard for the hard-error parser paths (`json_recv`,
/// `json_typeof`). In C, `pg_parse_json_or_ereport` and
/// `json_errsave_error(..., NULL)` never return on a non-success result; if a
/// misbehaving provider returned `Ok(())` here, surface the broken invariant as
/// XX000 rather than continuing with invalid JSON.
fn unreached_soft_error() -> PgError {
    PgError::error("json backend errsave returned without raising on a hard-error path")
        .with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

// ===========================================================================
// Seam installation.
// ===========================================================================

/// `escape_json` provider for the inward seam (`backend-utils-adt-json-seams`).
/// The seam hands a UTF-8 `&str` (the C cstring) and a `PgString` sink. We
/// escape into a temporary byte spine (the in-crate `escape_json`) and push the
/// escaped, valid-UTF-8 result onto the sink.
fn escape_json_into_pgstring(buf: &mut PgString<'_>, str: &str) -> PgResult<()> {
    let mcx = buf.allocator();
    let mut tmp = PgVec::new_in(mcx);
    escape_json(&mut tmp, str.as_bytes())?;
    // Escape output is valid UTF-8 (input is &str; escapes are ASCII).
    let s = core::str::from_utf8(&tmp).map_err(|_| {
        PgError::error("escape_json produced invalid UTF-8").with_sqlstate(ERRCODE_INTERNAL_ERROR)
    })?;
    buf.try_push_str(s)
}

/// `escape_json_with_len` provider for the inward seam.
fn escape_json_with_len_into_pgstring(buf: &mut PgString<'_>, str: &[u8]) -> PgResult<()> {
    let mcx = buf.allocator();
    let mut tmp = PgVec::new_in(mcx);
    escape_json_with_len(&mut tmp, str)?;
    let s = core::str::from_utf8(&tmp).map_err(|_| {
        PgError::error("escape_json produced invalid UTF-8").with_sqlstate(ERRCODE_INTERNAL_ERROR)
    })?;
    buf.try_push_str(s)
}

/// Install every seam this crate owns (`backend-utils-adt-json-seams`). Only
/// `set()` calls; called once from `seams-init::init_all()`.
pub fn init_seams() {
    backend_utils_adt_json_seams::escape_json::set(escape_json_into_pgstring);
    backend_utils_adt_json_seams::escape_json_with_len::set(escape_json_with_len_into_pgstring);
    backend_utils_adt_json_seams::json_encode_datetime::set(JsonEncodeDateTime);
}

#[cfg(test)]
mod tests;
