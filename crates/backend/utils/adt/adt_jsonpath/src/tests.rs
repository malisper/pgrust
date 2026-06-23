//! Unit tests for the parts of `jsonpath.c` that do not require the (unported)
//! grammar/scanner parser seam: the regex-flag conversion, the operation-name /
//! priority tables, and a flatten -> reader/print round-trip over a manually
//! built parse tree.

use super::*;
use alloc::boxed::Box;
use alloc::vec;
use mcx::MemoryContext;

fn pi(typ: JsonPathItemType, value: JsonPathParseValue) -> JsonPathParseItem {
    JsonPathParseItem {
        typ,
        next: None,
        value,
    }
}

#[test]
fn convert_regex_flags_default_advanced() {
    // No flags -> REG_ADVANCED | REG_NLSTOP (dotall off by default).
    use regex_core::regex_consts::{REG_ADVANCED, REG_NLSTOP};
    assert_eq!(jspConvertRegexFlags(0).unwrap(), REG_ADVANCED | REG_NLSTOP);
}

#[test]
fn convert_regex_flags_icase_dotall_mline() {
    use regex_core::regex_consts::{REG_ADVANCED, REG_ICASE, REG_NLANCH};
    let xflags = JSP_REGEX_ICASE | JSP_REGEX_DOTALL | JSP_REGEX_MLINE;
    // dotall set -> no REG_NLSTOP; mline -> REG_NLANCH; icase -> REG_ICASE.
    assert_eq!(
        jspConvertRegexFlags(xflags).unwrap(),
        REG_ADVANCED | REG_ICASE | REG_NLANCH
    );
}

#[test]
fn convert_regex_flags_quote_overrides() {
    use regex_core::regex_consts::{REG_ADVANCED, REG_QUOTE};
    // q clears REG_ADVANCED and sets REG_QUOTE; m/s/x are ignored.
    let xflags = JSP_REGEX_QUOTE | JSP_REGEX_MLINE | JSP_REGEX_DOTALL;
    let cflags = jspConvertRegexFlags(xflags).unwrap();
    assert_eq!(cflags & REG_ADVANCED, 0);
    assert_ne!(cflags & REG_QUOTE, 0);
}

#[test]
fn convert_regex_flags_wspace_unimplemented() {
    let err = jspConvertRegexFlags(JSP_REGEX_WSPACE).unwrap_err();
    assert!(err.message().contains("expanded regular expressions"));
}

#[test]
fn operation_name_and_priority() {
    assert_eq!(jspOperationName(jpiAnd).unwrap(), "&&");
    assert_eq!(jspOperationName(jpiStartsWith).unwrap(), "starts with");
    assert_eq!(jspOperationName(jpiLikeRegex).unwrap(), "like_regex");
    assert!(jspOperationName(jpiNull).is_err());

    assert!(operationPriority(jpiOr) < operationPriority(jpiAnd));
    assert!(operationPriority(jpiAnd) < operationPriority(jpiEqual));
    assert!(operationPriority(jpiMul) > operationPriority(jpiAdd));
    assert_eq!(operationPriority(jpiKey), 6);
}

/// Build `$.foo` (root then a key access), flatten it, then read it back with
/// the reader API and verify the on-disk structure round-trips. No parser
/// needed: we construct the parse tree directly.
#[test]
fn flatten_and_read_root_key() {
    let ctx = MemoryContext::new("jsonpath-test");
    let mcx = ctx.mcx();

    // root -> key "foo"
    let key = pi(jpiKey, JsonPathParseValue::String(b"foo".to_vec()));
    let mut root = pi(jpiRoot, JsonPathParseValue::None);
    root.next = Some(Box::new(key));

    let mut buf: PgVec<'_, u8> = PgVec::new_in(mcx);
    buf_zeros(&mut buf, JSONPATH_HDRSZ).unwrap();
    let ok = flattenJsonPathParseItem(&mut buf, None, None, &root, 0, false).unwrap();
    assert!(ok);

    // Set header: version, lax (default).
    let total = buf.len();
    set_varsize(&mut buf, total);
    let header = JSONPATH_VERSION | JSONPATH_LAX;
    buf[4..8].copy_from_slice(&header.to_ne_bytes());

    // Read the root node back.
    let v = jspInit(&buf);
    assert_eq!(v.typ, jpiRoot);
    assert!(jspHasNext(&v));
    let next = jspGetNext(&v).unwrap();
    assert_eq!(next.typ, jpiKey);
    assert_eq!(jspGetString(&next), b"foo");

    assert!(jsonpath_is_lax(&buf));
}

/// Round-trip a numeric-and-bool index/literal structure through flatten and
/// the reader (no printer, which needs numeric_out's full machinery installed).
#[test]
fn flatten_bool_literal() {
    let ctx = MemoryContext::new("jsonpath-test");
    let mcx = ctx.mcx();

    let item = pi(jpiBool, JsonPathParseValue::Boolean(true));

    let mut buf: PgVec<'_, u8> = PgVec::new_in(mcx);
    buf_zeros(&mut buf, JSONPATH_HDRSZ).unwrap();
    flattenJsonPathParseItem(&mut buf, None, None, &item, 0, false).unwrap();
    let total = buf.len();
    set_varsize(&mut buf, total);
    buf[4..8].copy_from_slice(&JSONPATH_VERSION.to_ne_bytes());

    let v = jspInit(&buf);
    assert_eq!(v.typ, jpiBool);
    assert!(jspGetBool(&v));
}

/// `@` outside a filter is a hard syntax error (root nesting level 0).
#[test]
fn flatten_current_at_root_errors() {
    let ctx = MemoryContext::new("jsonpath-test");
    let mcx = ctx.mcx();

    let item = pi(jpiCurrent, JsonPathParseValue::None);
    let mut buf: PgVec<'_, u8> = PgVec::new_in(mcx);
    buf_zeros(&mut buf, JSONPATH_HDRSZ).unwrap();
    let err = flattenJsonPathParseItem(&mut buf, None, None, &item, 0, false).unwrap_err();
    assert!(err.message().contains("not allowed in root expressions"));
}

/// `LAST` outside an array subscript is a hard syntax error.
#[test]
fn flatten_last_outside_subscript_errors() {
    let ctx = MemoryContext::new("jsonpath-test");
    let mcx = ctx.mcx();

    let item = pi(jpiLast, JsonPathParseValue::None);
    let mut buf: PgVec<'_, u8> = PgVec::new_in(mcx);
    buf_zeros(&mut buf, JSONPATH_HDRSZ).unwrap();
    let err = flattenJsonPathParseItem(&mut buf, None, None, &item, 0, false).unwrap_err();
    assert!(err.message().contains("LAST is allowed only in array subscripts"));
}

#[test]
fn jsp_is_scalar_bounds() {
    assert!(jsp_is_scalar(jpiNull));
    assert!(jsp_is_scalar(jpiBool));
    assert!(!jsp_is_scalar(jpiKey));
    assert!(!jsp_is_scalar(jpiRoot));
}

#[test]
fn index_array_subscript_roundtrip() {
    let ctx = MemoryContext::new("jsonpath-test");
    let mcx = ctx.mcx();

    // [ <num 0> ] -- one subscript, no "to".
    let from = pi(
        jpiNumeric,
        JsonPathParseValue::Numeric(make_numeric_zero()),
    );
    let arr = pi(
        jpiIndexArray,
        JsonPathParseValue::Array(vec![JsonPathSubscript {
            from: Some(Box::new(from)),
            to: None,
        }]),
    );

    let mut buf: PgVec<'_, u8> = PgVec::new_in(mcx);
    buf_zeros(&mut buf, JSONPATH_HDRSZ).unwrap();
    flattenJsonPathParseItem(&mut buf, None, None, &arr, 0, false).unwrap();
    let total = buf.len();
    set_varsize(&mut buf, total);
    buf[4..8].copy_from_slice(&JSONPATH_VERSION.to_ne_bytes());

    let v = jspInit(&buf);
    assert_eq!(v.typ, jpiIndexArray);
    assert_eq!(v.content.array.nelems, 1);
    let (sub_from, sub_to) = jspGetArraySubscript(&v, 0);
    assert_eq!(sub_from.typ, jpiNumeric);
    assert!(sub_to.is_none());
}

/// A minimal on-disk uncompressed-varlena `numeric` for the value 0: a 4-byte
/// header (length) followed by the 2-byte `n_header` weight/sign/dscale word,
/// no digits. We only need the bytes to round-trip through flatten + the reader
/// length math (we never call numeric_out in these tests).
fn make_numeric_zero() -> Vec<u8> {
    // VARSIZE = 4 (header) + 2 (n_short header) = 6 bytes.
    let len: u32 = 6;
    let word: u32 = if cfg!(target_endian = "big") {
        len & 0x3FFF_FFFF
    } else {
        len << 2
    };
    let mut v = word.to_ne_bytes().to_vec();
    // NUMERIC_SHORT, weight 0, sign 0, dscale 0 -> a 16-bit header of 0x8000.
    v.extend_from_slice(&0x8000u16.to_ne_bytes());
    v
}
