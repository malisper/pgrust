//! On-disk / Datum representation for the PostgreSQL `jsonpath` type.
//!
//! Lifted from `postgres-18.3/src/include/utils/jsonpath.h`: the on-disk
//! `JsonPath` varlena, the `JsonPathItemType` node-type enum (which becomes part
//! of the on-disk representation -- order must never change), the version/flag
//! header bits, and the XQuery regex-mode flags.

use alloc::vec::Vec;

use types_jsonb::jsonb::jbvType;

/// `VARHDRSZ`, the varlena length-header size in bytes.
pub use types_datum::VARHDRSZ;

// ---------------------------------------------------------------------------
// JsonPath: the top-level on-disk datum (jsonpath.h).
// ---------------------------------------------------------------------------

/// The on-disk `jsonpath` datum: a varlena header, a `uint32` version/flags
/// word, and a flexible `data` byte array holding the flattened expression.
#[derive(Clone, Debug)]
pub struct JsonPath {
    /// Varlena length header.  Do not touch directly.
    pub vl_len_: i32,
    /// Version and flags (see [`JSONPATH_VERSION`] / [`JSONPATH_LAX`]).
    pub header: u32,
    /// Flexible array member (`char data[]`) holding the flattened nodes.
    pub data: Vec<u8>,
}

/// Current jsonpath on-disk version.
pub const JSONPATH_VERSION: u32 = 0x01;
/// Lax-mode flag bit in `JsonPath.header`.
pub const JSONPATH_LAX: u32 = 0x8000_0000;
/// `JSONPATH_HDRSZ`: `offsetof(JsonPath, data)` -- the varlena header plus the
/// `uint32` version/flags word (4 + 4 = 8 bytes).
pub const JSONPATH_HDRSZ: usize = 8;

// ---------------------------------------------------------------------------
// XQuery regex mode flags for the LIKE_REGEX predicate (jsonpath.h).
// ---------------------------------------------------------------------------

pub const JSP_REGEX_ICASE: u32 = 0x01; // i flag, case insensitive
pub const JSP_REGEX_DOTALL: u32 = 0x02; // s flag, dot matches newline
pub const JSP_REGEX_MLINE: u32 = 0x04; // m flag, ^/$ match at newlines
pub const JSP_REGEX_WSPACE: u32 = 0x08; // x flag, ignore whitespace in pattern
pub const JSP_REGEX_QUOTE: u32 = 0x10; // q flag, no special characters

// ---------------------------------------------------------------------------
// JsonPathItemType: node types of a jsonpath expression (jsonpath.h).
//
// These become part of the on-disk representation; to preserve
// pg_upgradability, the order must not change and new values are added at the
// end.  The first four share their discriminants with jbvType (jpiNull ==
// jbvNull, etc.); the rest follow in sequence.
// ---------------------------------------------------------------------------

/// `enum JsonPathItemType`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum JsonPathItemType {
    jpiNull = jbvType::jbvNull as isize,       // NULL literal
    jpiString = jbvType::jbvString as isize,   // string literal
    jpiNumeric = jbvType::jbvNumeric as isize, // numeric literal
    jpiBool = jbvType::jbvBool as isize,       // boolean literal: TRUE or FALSE
    jpiAnd,                                    // predicate && predicate
    jpiOr,                                     // predicate || predicate
    jpiNot,                                    // ! predicate
    jpiIsUnknown,                              // (predicate) IS UNKNOWN
    jpiEqual,                                  // expr == expr
    jpiNotEqual,                               // expr != expr
    jpiLess,                                   // expr < expr
    jpiGreater,                                // expr > expr
    jpiLessOrEqual,                            // expr <= expr
    jpiGreaterOrEqual,                         // expr >= expr
    jpiAdd,                                    // expr + expr
    jpiSub,                                    // expr - expr
    jpiMul,                                    // expr * expr
    jpiDiv,                                    // expr / expr
    jpiMod,                                    // expr % expr
    jpiPlus,                                   // + expr
    jpiMinus,                                  // - expr
    jpiAnyArray,                               // [*]
    jpiAnyKey,                                 // .*
    jpiIndexArray,                             // [subscript, ...]
    jpiAny,                                    // .**
    jpiKey,                                    // .key
    jpiCurrent,                                // @
    jpiRoot,                                   // $
    jpiVariable,                               // $variable
    jpiFilter,                                 // ? (predicate)
    jpiExists,                                 // EXISTS (expr) predicate
    jpiType,                                   // .type() item method
    jpiSize,                                   // .size() item method
    jpiAbs,                                    // .abs() item method
    jpiFloor,                                  // .floor() item method
    jpiCeiling,                                // .ceiling() item method
    jpiDouble,                                 // .double() item method
    jpiDatetime,                               // .datetime() item method
    jpiKeyValue,                               // .keyvalue() item method
    jpiSubscript,                              // array subscript: 'expr' or 'expr TO expr'
    jpiLast,                                   // LAST array subscript
    jpiStartsWith,                             // STARTS WITH predicate
    jpiLikeRegex,                              // LIKE_REGEX predicate
    jpiBigint,                                 // .bigint() item method
    jpiBoolean,                                // .boolean() item method
    jpiDate,                                   // .date() item method
    jpiDecimal,                                // .decimal() item method
    jpiInteger,                                // .integer() item method
    jpiNumber,                                 // .number() item method
    jpiStringFunc,                             // .string() item method
    jpiTime,                                   // .time() item method
    jpiTimeTz,                                 // .time_tz() item method
    jpiTimestamp,                              // .timestamp() item method
    jpiTimestampTz,                            // .timestamp_tz() item method
}

/// `jspIsScalar`: a scalar literal (jpiNull..=jpiBool).
#[inline]
pub const fn jsp_is_scalar(ty: JsonPathItemType) -> bool {
    (ty as i32) >= (JsonPathItemType::jpiNull as i32)
        && (ty as i32) <= (JsonPathItemType::jpiBool as i32)
}
