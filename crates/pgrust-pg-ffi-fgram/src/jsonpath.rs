//! On-disk / Datum ABI for the PostgreSQL `jsonpath` type.
//!
//! Lifted byte-for-byte from `postgres-18.3/src/include/utils/jsonpath.h`: the
//! on-disk `JsonPath` varlena, the `JsonPathItemType` node-type enum (which
//! becomes part of the on-disk representation -- order must never change), the
//! version/flag header bits, and the XQuery regex-mode flags.  `#[repr(C)]`
//! layout is verified at compile time by the const-assert gates below.
//!
//! There is NO `extern "C"` here.  In-memory working types (`JsonPathItem`,
//! `JsonPathParseItem`, `JsonPathParseResult`, `JsonPathVariable`, ...) are
//! idiomatic Rust types that live in the `backend-utils-adt-jsonpath` crate,
//! since they are never stored on disk and never cross a C ABI boundary.

#![allow(non_upper_case_globals)]

use core::mem::{align_of, offset_of, size_of};

use crate::jsonb::jbvType;

/// `VARHDRSZ`, the varlena length-header size in bytes.
pub use crate::VARHDRSZ;

// ---------------------------------------------------------------------------
// JsonPath: the top-level on-disk datum (jsonpath.h).
// ---------------------------------------------------------------------------

/// The on-disk `jsonpath` datum: a varlena header, a `uint32` version/flags
/// word, and a flexible `data` byte array holding the flattened expression.
#[derive(Copy, Clone)]
#[repr(C)]
pub struct JsonPath {
    /// Varlena length header.  Do not touch directly.
    pub vl_len_: i32,
    /// Version and flags (see [`JSONPATH_VERSION`] / [`JSONPATH_LAX`]).
    pub header: u32,
    /// Flexible array member (`char data[]`) holding the flattened nodes.
    pub data: [u8; 0],
}

/// Current jsonpath on-disk version.
pub const JSONPATH_VERSION: u32 = 0x01;
/// Lax-mode flag bit in `JsonPath.header`.
pub const JSONPATH_LAX: u32 = 0x8000_0000;
/// `JSONPATH_HDRSZ`: `offsetof(JsonPath, data)`.
pub const JSONPATH_HDRSZ: usize = offset_of!(JsonPath, data);

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
#[repr(i32)]
pub enum JsonPathItemType {
    jpiNull = jbvType::jbvNull as i32,       // NULL literal
    jpiString = jbvType::jbvString as i32,   // string literal
    jpiNumeric = jbvType::jbvNumeric as i32, // numeric literal
    jpiBool = jbvType::jbvBool as i32,       // boolean literal: TRUE or FALSE
    jpiAnd,                                  // predicate && predicate
    jpiOr,                                   // predicate || predicate
    jpiNot,                                  // ! predicate
    jpiIsUnknown,                            // (predicate) IS UNKNOWN
    jpiEqual,                                // expr == expr
    jpiNotEqual,                             // expr != expr
    jpiLess,                                 // expr < expr
    jpiGreater,                              // expr > expr
    jpiLessOrEqual,                          // expr <= expr
    jpiGreaterOrEqual,                       // expr >= expr
    jpiAdd,                                  // expr + expr
    jpiSub,                                  // expr - expr
    jpiMul,                                  // expr * expr
    jpiDiv,                                  // expr / expr
    jpiMod,                                  // expr % expr
    jpiPlus,                                 // + expr
    jpiMinus,                                // - expr
    jpiAnyArray,                             // [*]
    jpiAnyKey,                               // .*
    jpiIndexArray,                           // [subscript, ...]
    jpiAny,                                  // .**
    jpiKey,                                  // .key
    jpiCurrent,                              // @
    jpiRoot,                                 // $
    jpiVariable,                             // $variable
    jpiFilter,                               // ? (predicate)
    jpiExists,                               // EXISTS (expr) predicate
    jpiType,                                 // .type() item method
    jpiSize,                                 // .size() item method
    jpiAbs,                                  // .abs() item method
    jpiFloor,                                // .floor() item method
    jpiCeiling,                              // .ceiling() item method
    jpiDouble,                               // .double() item method
    jpiDatetime,                             // .datetime() item method
    jpiKeyValue,                             // .keyvalue() item method
    jpiSubscript,                            // array subscript: 'expr' or 'expr TO expr'
    jpiLast,                                 // LAST array subscript
    jpiStartsWith,                           // STARTS WITH predicate
    jpiLikeRegex,                            // LIKE_REGEX predicate
    jpiBigint,                               // .bigint() item method
    jpiBoolean,                              // .boolean() item method
    jpiDate,                                 // .date() item method
    jpiDecimal,                              // .decimal() item method
    jpiInteger,                              // .integer() item method
    jpiNumber,                               // .number() item method
    jpiStringFunc,                           // .string() item method
    jpiTime,                                 // .time() item method
    jpiTimeTz,                               // .time_tz() item method
    jpiTimestamp,                            // .timestamp() item method
    jpiTimestampTz,                          // .timestamp_tz() item method
}

/// `jspIsScalar`: a scalar literal (jpiNull..=jpiBool).
#[inline]
pub const fn jsp_is_scalar(ty: JsonPathItemType) -> bool {
    (ty as i32) >= (JsonPathItemType::jpiNull as i32)
        && (ty as i32) <= (JsonPathItemType::jpiBool as i32)
}

// ---------------------------------------------------------------------------
// Compile-time layout gates.  JsonPath is on-disk ABI; the JsonPathItemType
// discriminants are part of the on-disk node stream.
// ---------------------------------------------------------------------------

const _: () = {
    // JsonPath: int32 varlena header + uint32 header + flexible byte array.
    assert!(size_of::<JsonPath>() == 8);
    assert!(align_of::<JsonPath>() == 4);
    assert!(offset_of!(JsonPath, vl_len_) == 0);
    assert!(offset_of!(JsonPath, header) == 4);
    assert!(offset_of!(JsonPath, data) == 8);
    assert!(JSONPATH_HDRSZ == 8);

    // The shared-with-jbvType discriminants must line up.
    assert!(JsonPathItemType::jpiNull as i32 == jbvType::jbvNull as i32);
    assert!(JsonPathItemType::jpiString as i32 == jbvType::jbvString as i32);
    assert!(JsonPathItemType::jpiNumeric as i32 == jbvType::jbvNumeric as i32);
    assert!(JsonPathItemType::jpiBool as i32 == jbvType::jbvBool as i32);
    // The first sequential value follows jpiBool (== jbvBool == 3).
    assert!(JsonPathItemType::jpiAnd as i32 == 4);
    // The last value pins the tail of the on-disk enum.  jpiBool == jbvBool ==
    // 3, jpiAnd == 4, and there are 50 sequential values jpiAnd..=jpiTimestampTz
    // (49 steps), so jpiTimestampTz == 4 + 49 == 53.
    assert!(JsonPathItemType::jpiTimestampTz as i32 == 53);
};
