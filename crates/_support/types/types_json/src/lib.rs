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

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::any::Any;

use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_tuple::Datum;

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
pub struct ArrayForJson<'mcx> {
    /// C: `ARR_NDIM(v)`.
    pub ndim: i32,
    /// C: `ARR_DIMS(v)` — one entry per dimension.
    pub dims: Vec<i32>,
    /// C: `deconstruct_array` output Datums (row-major).
    pub elements: Vec<Datum<'mcx>>,
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
pub struct CompositeFieldForJson<'mcx> {
    /// C: `NameStr(att->attname)` — the attribute name bytes (no trailing NUL).
    pub attname: Vec<u8>,
    /// C: `heap_getattr(...)` value Datum (meaningless if `is_null`).
    pub val: Datum<'mcx>,
    /// C: the `heap_getattr` `isnull` out-flag.
    pub is_null: bool,
    /// C: `json_categorize_type(att->atttypid, ...)` (or `JSONTYPE_NULL` if the
    /// attribute is null).
    pub tcategory: JsonTypeCategory,
    /// C: the matching `outfuncoid` (or `InvalidOid` if null).
    pub outfuncoid: Oid,
}

// ---------------------------------------------------------------------------
// The JSON lexer state + the SAX callback table (`src/common/jsonapi.h`).
//
// `JsonLexContext` is the lexer's running state, observed by the SAX callbacks
// the json-text entry points install. In C the position fields are
// `const char *` into the immutable input buffer; here the buffer is held once
// as `input` and positions are byte offsets into it — bounds-checkable while
// faithful to the pointer arithmetic the callbacks perform. The lexer is owned
// by `common/jsonapi.c`; until that lands, `pg_parse_json` (the SAX driver) is
// reached through `common-jsonapi-seams` and panics. The driver writes these
// fields as it advances and hands the live `&JsonLexContext` to each callback.
// ---------------------------------------------------------------------------

/// C: `struct JsonLexContext` (`src/common/jsonapi.h`). The lexer's running
/// state; the SAX callbacks read `token_type`/`lex_level`/`line_number`/the
/// position offsets off the live context the parse driver threads to them.
#[derive(Clone, Debug, Default)]
pub struct JsonLexContext {
    /// `char *input`: the JSON text being lexed (the whole buffer).
    pub input: Vec<u8>,
    /// `int input_length`: length of `input`.
    pub input_length: usize,
    /// `int input_encoding`: server encoding of `input`.
    pub input_encoding: i32,
    /// `JsonTokenType token_type`: the current token's type.
    pub token_type: JsonTokenType,
    /// `int lex_level`: current nesting depth (0 at top level).
    pub lex_level: i32,
    /// `char *token_start`: offset of the start of the current token.
    pub token_start: usize,
    /// `char *token_terminator`: offset one past the current token.
    pub token_terminator: usize,
    /// `char *prev_token_terminator`: offset one past the previous token.
    pub prev_token_terminator: usize,
    /// `int line_number`: 1-based line number of the current token.
    pub line_number: i32,
    /// `char *line_start`: offset of the start of the current line.
    pub line_start: usize,
}

impl Default for JsonTokenType {
    fn default() -> Self {
        JsonTokenType::JSON_TOKEN_INVALID
    }
}

impl JsonLexContext {
    /// The byte at `off`, or the C NUL terminator (0) at/past the end.
    #[inline]
    pub fn byte_at(&self, off: usize) -> u8 {
        self.input.get(off).copied().unwrap_or(0)
    }
}

/// C: `JsonSemAction` (`src/common/jsonapi.h`) — the table of semantic-action
/// callbacks `pg_parse_json` invokes as it walks the input. In C the callbacks
/// are bare function pointers plus a `void *semstate`; here each is an owned
/// boxed closure that captures the caller's state directly (the idiomatic
/// substitute for the `void *semstate` + fn-pointer pair), so no separate
/// state pointer is needed. The driver invokes whichever callbacks are
/// `Some`; a `None` slot is the C `NULL` action (a no-op the driver skips,
/// exactly as `pg_parse_json` does).
///
/// The callbacks mirror the C `json_struct_action` / `json_ofield_action` /
/// `json_aelem_action` / `json_scalar_action` signatures: structural ones take
/// the live `&JsonLexContext`; object-field ones add the field-name bytes and
/// its `isnull` flag; array-element ones add the `isnull` flag; the scalar one
/// adds the token bytes and its `JsonTokenType`. Every callback returns
/// `PgResult<()>` — `Ok` is the C `JSON_SUCCESS`, `Err` is a raised
/// `ereport(ERROR)` (the C callbacks that ereport map to this, and the
/// `JSON_SEM_ACTION_FAILED` soft path is carried inside the `Err`).
pub type JsonStructAction<'a> = Box<dyn FnMut(&JsonLexContext) -> PgResult<()> + 'a>;
pub type JsonOfieldAction<'a> = Box<dyn FnMut(&JsonLexContext, &[u8], bool) -> PgResult<()> + 'a>;
pub type JsonAelemAction<'a> = Box<dyn FnMut(&JsonLexContext, bool) -> PgResult<()> + 'a>;
pub type JsonScalarAction<'a> =
    Box<dyn FnMut(&JsonLexContext, &[u8], JsonTokenType) -> PgResult<()> + 'a>;

/// C: `JsonSemAction` (`src/common/jsonapi.h`). All fields default to `None`
/// (the C "null semantic action").
#[derive(Default)]
pub struct JsonSemAction<'a> {
    /// `json_struct_action object_start`.
    pub object_start: Option<JsonStructAction<'a>>,
    /// `json_struct_action object_end`.
    pub object_end: Option<JsonStructAction<'a>>,
    /// `json_struct_action array_start`.
    pub array_start: Option<JsonStructAction<'a>>,
    /// `json_struct_action array_end`.
    pub array_end: Option<JsonStructAction<'a>>,
    /// `json_ofield_action object_field_start`.
    pub object_field_start: Option<JsonOfieldAction<'a>>,
    /// `json_ofield_action object_field_end`.
    pub object_field_end: Option<JsonOfieldAction<'a>>,
    /// `json_aelem_action array_element_start`.
    pub array_element_start: Option<JsonAelemAction<'a>>,
    /// `json_aelem_action array_element_end`.
    pub array_element_end: Option<JsonAelemAction<'a>>,
    /// `json_scalar_action scalar`.
    pub scalar: Option<JsonScalarAction<'a>>,
}

/// Marker bound used by `JsObject`'s json-hash carrier so the populate machinery
/// can hold an opaque hash without naming the dynahash crate.
pub trait JsonHashTable: Any {}
