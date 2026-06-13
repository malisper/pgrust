//! Signature types for `backend-utils-adt-json` — the parser/category
//! vocabulary `json.c` shares with `src/common/jsonapi.h`,
//! `src/include/utils/jsonfuncs.h`, and the deconstructed inputs the seams hand
//! back for the in-crate object/array assembly.
//!
//! These are pure value types (no raw pointers): the json crate ports the
//! structural assembly (escaping, builders, the unique-key check, the aggregate
//! state machine) in-crate and reaches the genuinely-external parser/catalog/
//! fmgr/datetime work through seams, exchanging these owned values.

#![no_std]
#![allow(non_camel_case_types)]

extern crate alloc;

use alloc::vec::Vec;

use types_core::Oid;
use types_datum::Datum;

/// C: `JsonTokenType` (`src/common/jsonapi.h`). The discriminant order matches
/// the C enum (the integer is observable only in `json_typeof`'s otherwise-
/// unreachable `elog(ERROR, "unexpected json token: %d", ...)` diagnostic).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonTokenType {
    JSON_TOKEN_INVALID,
    JSON_TOKEN_STRING,
    JSON_TOKEN_NUMBER,
    JSON_TOKEN_OBJECT_START,
    JSON_TOKEN_OBJECT_END,
    JSON_TOKEN_ARRAY_START,
    JSON_TOKEN_ARRAY_END,
    JSON_TOKEN_COMMA,
    JSON_TOKEN_COLON,
    JSON_TOKEN_TRUE,
    JSON_TOKEN_FALSE,
    JSON_TOKEN_NULL,
    JSON_TOKEN_END,
}

/// C: `JsonParseErrorType` (`src/common/jsonapi.h`). The discriminant order
/// matches the C enum. `JSON_SUCCESS` is the only "ok" value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonParseErrorType {
    JSON_SUCCESS,
    JSON_INCOMPLETE,
    JSON_INVALID_LEXER_TYPE,
    JSON_NESTING_TOO_DEEP,
    JSON_ESCAPING_INVALID,
    JSON_ESCAPING_REQUIRED,
    JSON_EXPECTED_ARRAY_FIRST,
    JSON_EXPECTED_ARRAY_NEXT,
    JSON_EXPECTED_COLON,
    JSON_EXPECTED_END,
    JSON_EXPECTED_JSON,
    JSON_EXPECTED_MORE,
    JSON_EXPECTED_OBJECT_FIRST,
    JSON_EXPECTED_OBJECT_NEXT,
    JSON_EXPECTED_STRING,
    JSON_INVALID_TOKEN,
    JSON_OUT_OF_MEMORY,
    JSON_UNICODE_CODE_POINT_ZERO,
    JSON_UNICODE_ESCAPE_FORMAT,
    JSON_UNICODE_HIGH_ESCAPE,
    JSON_UNICODE_UNTRANSLATABLE,
    JSON_UNICODE_HIGH_SURROGATE,
    JSON_UNICODE_LOW_SURROGATE,
    JSON_SEM_ACTION_FAILED,
}

/// C: `JsonTypeCategory` (`src/include/utils/jsonfuncs.h`) — how a Datum's type
/// maps to a JSON rendering. Shared with `jsonb`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonTypeCategory {
    JSONTYPE_NULL,
    JSONTYPE_BOOL,
    JSONTYPE_NUMERIC,
    JSONTYPE_DATE,
    JSONTYPE_TIMESTAMP,
    JSONTYPE_TIMESTAMPTZ,
    JSONTYPE_JSON,
    JSONTYPE_JSONB,
    JSONTYPE_ARRAY,
    JSONTYPE_COMPOSITE,
    JSONTYPE_CAST,
    JSONTYPE_OTHER,
}

/// The deconstructed array handed back from the `deconstruct_array` seam for the
/// in-crate `array_to_json_internal` assembly. This is the catalog/`array.c`
/// half (`get_typlenbyvalalign` + `json_categorize_type(element_type, ...)` +
/// `deconstruct_array`); the structural `[ ... ]` assembly stays in the crate.
#[derive(Clone, Debug)]
pub struct ArrayForJson {
    /// C: `ARR_NDIM(v)`.
    pub ndim: i32,
    /// C: `ARR_DIMS(v)` — one entry per dimension.
    pub dims: Vec<i32>,
    /// C: `deconstruct_array` output Datums (row-major).
    pub elements: Vec<Datum>,
    /// C: `deconstruct_array` null flags (row-major).
    pub nulls: Vec<bool>,
    /// C: `json_categorize_type(element_type, false, &tcategory, ...)`.
    pub element_tcategory: JsonTypeCategory,
    /// C: `json_categorize_type(... &outfuncoid)`.
    pub element_outfuncoid: Oid,
}

/// One composite attribute handed back from the `walk_composite` seam (the
/// catalog half of `composite_to_json`: `lookup_rowtype_tupdesc`, the
/// per-attribute `heap_getattr`, and the per-attribute `json_categorize_type`).
/// Dropped attributes are already filtered out (matching the C
/// `if (att->attisdropped) continue;`).
#[derive(Clone, Debug)]
pub struct CompositeFieldForJson {
    /// C: `NameStr(att->attname)` — the attribute name bytes (no trailing NUL).
    pub attname: Vec<u8>,
    /// C: `heap_getattr(...)` value Datum (meaningless if `is_null`).
    pub val: Datum,
    /// C: the `heap_getattr` `isnull` out-flag.
    pub is_null: bool,
    /// C: `json_categorize_type(att->atttypid, ...)` (or `JSONTYPE_NULL` if the
    /// attribute is null).
    pub tcategory: JsonTypeCategory,
    /// C: the matching `outfuncoid` (or `InvalidOid` if null).
    pub outfuncoid: Oid,
}
