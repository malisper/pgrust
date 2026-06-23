//! In-memory working types of `jsonpath.c` — `JsonPathParseItem`,
//! `JsonPathParseResult`, `JsonPathParseValue`, `JsonPathSubscript`, and
//! `JsonPathVariable`. They are never stored on disk and never cross a C ABI
//! boundary, so they are modelled as idiomatic owned Rust types.
//!
//! They live in this vocabulary crate (rather than in the owning crate)
//! because the grammar/scanner parser seam names `JsonPathParseResult` in its
//! signature, and a seam crate may depend only on `types-*`.

use alloc::boxed::Box;
use alloc::vec::Vec;

use crate::jsonpath::JsonPathItemType;

/// A `numeric` value carried opaquely (C: `Numeric`). Holds the complete
/// on-disk varlena bytes (length header included).
pub type JsonPathNumeric = Vec<u8>;

/// The scanner's literal-text buffer / token semantic value
/// (C: `struct JsonPathString { char *val; int len; int total; }`, declared in
/// `jsonpath_internal.h`). The grammar/scanner seam names it in helper
/// signatures (`makeItemString`, `makeItemKey`, ...), so it lives in this
/// vocabulary crate. `val` carries the decoded bytes (the C buffer may include
/// a trailing NUL written by `addchar(false,'\0')`); `len` is the meaningful
/// length excluding any terminator; `total` is the allocated capacity flex
/// tracks for the doubling `resizeString` strategy.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JsonPathString {
    pub val: Vec<u8>,
    pub len: i32,
    pub total: i32,
}

impl JsonPathString {
    /// The meaningful bytes (C: `val[0 .. len]`), excluding any trailing NUL.
    pub fn bytes(&self) -> &[u8] {
        &self.val[..self.len as usize]
    }
}

/// Parse-tree node (C: `struct JsonPathParseItem`). Produced by the
/// grammar/scanner seam, flattened by `flattenJsonPathParseItem`.
#[derive(Clone, Debug)]
pub struct JsonPathParseItem {
    /// Node type (C: `JsonPathItemType type`).
    pub typ: JsonPathItemType,
    /// Next in path (C: `JsonPathParseItem *next`).
    pub next: Option<Box<JsonPathParseItem>>,
    /// The tagged value payload (C: the `value` union).
    pub value: JsonPathParseValue,
}

/// The payload union of a [`JsonPathParseItem`] (C: `JsonPathParseItem.value`).
#[derive(Clone, Debug)]
pub enum JsonPathParseValue {
    /// No payload (e.g. `jpiRoot`, `jpiCurrent`, `jpiAnyArray`).
    None,
    /// Binary operator operands (C: `value.args`).
    Args {
        left: Option<Box<JsonPathParseItem>>,
        right: Option<Box<JsonPathParseItem>>,
    },
    /// Unary operand (C: `value.arg`).
    Arg(Option<Box<JsonPathParseItem>>),
    /// `jpiIndexArray` index list (C: `value.array`).
    Array(Vec<JsonPathSubscript>),
    /// `jpiAny` levels (C: `value.anybounds`).
    AnyBounds { first: u32, last: u32 },
    /// `jpiLikeRegex` (C: `value.like_regex`).
    LikeRegex {
        expr: Option<Box<JsonPathParseItem>>,
        pattern: Vec<u8>,
        flags: u32,
    },
    /// `jpiNumeric` scalar (C: `value.numeric`) — full on-disk varlena bytes.
    Numeric(JsonPathNumeric),
    /// `jpiBool` scalar (C: `value.boolean`).
    Boolean(bool),
    /// `jpiString`/`jpiKey`/`jpiVariable` scalar (C: `value.string`).
    String(Vec<u8>),
}

/// One `from`/`to` subscript pair in a `jpiIndexArray` (C: anonymous struct).
#[derive(Clone, Debug)]
pub struct JsonPathSubscript {
    pub from: Option<Box<JsonPathParseItem>>,
    pub to: Option<Box<JsonPathParseItem>>,
}

/// Result of parsing a jsonpath string (C: `struct JsonPathParseResult`).
#[derive(Clone, Debug)]
pub struct JsonPathParseResult {
    pub expr: Option<Box<JsonPathParseItem>>,
    pub lax: bool,
}

/// An external variable passed into the jsonpath executor
/// (C: `struct JsonPathVariable`).
#[derive(Clone, Debug)]
pub struct JsonPathVariable {
    pub name: Vec<u8>,
    pub namelen: i32,
    pub typid: u32,
    pub typmod: i32,
    /// `Datum`, carried opaquely as a machine word.
    pub value: usize,
    pub isnull: bool,
}
