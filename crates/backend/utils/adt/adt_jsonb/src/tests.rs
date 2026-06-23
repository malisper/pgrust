//! Unit tests for the SQL-facing jsonb layer.
//!
//! The parser/Datum operations are seamed (a global function-pointer slot
//! installed once at startup), so these tests exercise the pure paths
//! (rendering, typeof, casts, unquote, the text[] object builders, the numeric
//! scalar bridge, the `jsonb_in_*` semantic actions, indented rendering) over
//! jsonb bytes assembled directly through the jsonb_util push API — no backend
//! provider required. The jsonb_util serialization engine's externals
//! (`check_stack_depth`, `numeric_eq`/`numeric_cmp`, `varstr_cmp`, the byte-hash
//! primitives) plus this crate's `numeric_int4` cast seam are installed once
//! behind a `std::sync::Once` (the seam slots are process-global).

extern crate std;

use std::string::String;
use std::sync::Once;
use std::vec;
use std::vec::Vec;

use super::*;
use jsonb_util as jbu;
use ::mcx::MemoryContext;
use ::types_error::error::ERRCODE_INVALID_PARAMETER_VALUE;
use JsonbIteratorToken::*;

static INSTALL: Once = Once::new();

/// Install the externals' seams with faithful, deterministic implementations.
/// Shared with the `fmgr_builtins::tests` module (one process-global `Once`)
/// so both test modules install the same slots exactly once.
pub(crate) fn install_seams() {
    INSTALL.call_once(|| {
        stack_depth_seams::check_stack_depth::set(|| Ok(()));
        numeric_seams::numeric_eq::set(|a, b| a == b);
        numeric_seams::numeric_cmp::set(|a, b| match a.cmp(b) {
            core::cmp::Ordering::Less => -1,
            core::cmp::Ordering::Equal => 0,
            core::cmp::Ordering::Greater => 1,
        });
        varlena_seams::varstr_cmp::set(|a, b, _coll| {
            Ok(match a.cmp(b) {
                core::cmp::Ordering::Less => -1,
                core::cmp::Ordering::Equal => 0,
                core::cmp::Ordering::Greater => 1,
            })
        });
        hashfn_seams::hash_bytes::set(|k| {
            let mut h: u32 = 0x811c_9dc5;
            for &b in k {
                h ^= b as u32;
                h = h.wrapping_mul(0x0100_0193);
            }
            h
        });
        hashfn_seams::hash_bytes_extended::set(|k, seed| {
            let mut h: u64 = 0xcbf2_9ce4_8422_2325 ^ seed;
            for &b in k {
                h ^= b as u64;
                h = h.wrapping_mul(0x0000_0100_0000_01b3);
            }
            h
        });
        hashfn_seams::hash_bytes_uint32_extended::set(|k, seed| {
            (k as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ seed
        });
        // The numeric->int4 cast (jsonb_int4): decode the on-disk numeric to a
        // float8 (the in-repo numeric crate provides this) and round-trip to
        // i32. Faithful for the small integral values these tests use.
        jsonb_seams::numeric_int4::set(|num| {
            let f = adt_numeric::convert::numeric_to_float8(num)?;
            Ok(f as i32)
        });
    });
}

fn jstring(s: &str) -> JsonbValue {
    JsonbValue {
        typ: jbvType::jbvString,
        val: JsonbValueData::String(s.as_bytes().to_vec()),
    }
}

fn jbool(b: bool) -> JsonbValue {
    JsonbValue {
        typ: jbvType::jbvBool,
        val: JsonbValueData::Bool(b),
    }
}

/// Serialize a `JsonbValue` to owned on-disk bytes via a per-call context.
fn to_bytes(v: &JsonbValue) -> Vec<u8> {
    install_seams();
    let ctx = MemoryContext::new("jsonb.test.to_bytes");
    let buf = jbu::JsonbValueToJsonb(ctx.mcx(), v).unwrap();
    buf.as_slice().to_vec()
}

/// Build an on-disk jsonb array `["a", true, null]`.
fn build_array() -> Vec<u8> {
    install_seams();
    let ctx = MemoryContext::new("jsonb.test.build_array");
    let mut ps: Option<Box<jbu::JsonbParseState>> = None;
    jbu::pushJsonbValue(&mut ps, WJB_BEGIN_ARRAY, None).unwrap();
    jbu::pushJsonbValue(&mut ps, WJB_ELEM, Some(&jstring("a"))).unwrap();
    jbu::pushJsonbValue(&mut ps, WJB_ELEM, Some(&jbool(true))).unwrap();
    jbu::pushJsonbValue(&mut ps, WJB_ELEM, Some(&JsonbValue::null())).unwrap();
    let res = jbu::pushJsonbValue(&mut ps, WJB_END_ARRAY, None).unwrap().unwrap();
    let buf = jbu::JsonbValueToJsonb(ctx.mcx(), &res).unwrap();
    buf.as_slice().to_vec()
}

/// Build an on-disk jsonb object `{"k":"v"}`.
fn build_object() -> Vec<u8> {
    install_seams();
    let ctx = MemoryContext::new("jsonb.test.build_object");
    let mut ps: Option<Box<jbu::JsonbParseState>> = None;
    jbu::pushJsonbValue(&mut ps, WJB_BEGIN_OBJECT, None).unwrap();
    jbu::pushJsonbValue(&mut ps, WJB_KEY, Some(&jstring("k"))).unwrap();
    jbu::pushJsonbValue(&mut ps, WJB_VALUE, Some(&jstring("v"))).unwrap();
    let res = jbu::pushJsonbValue(&mut ps, WJB_END_OBJECT, None).unwrap().unwrap();
    let buf = jbu::JsonbValueToJsonb(ctx.mcx(), &res).unwrap();
    buf.as_slice().to_vec()
}

fn build_scalar_string(s: &str) -> Vec<u8> {
    to_bytes(&jstring(s))
}

fn build_scalar_bool(b: bool) -> Vec<u8> {
    to_bytes(&jbool(b))
}

fn out(jb: &[u8]) -> String {
    let ctx = MemoryContext::new("jsonb.test.out");
    let buf = jsonb_out(ctx.mcx(), jb).unwrap();
    String::from_utf8(buf.as_slice().to_vec()).unwrap()
}

#[test]
fn out_array() {
    assert_eq!(out(&build_array()), r#"["a", true, null]"#);
}

#[test]
fn out_object() {
    assert_eq!(out(&build_object()), r#"{"k": "v"}"#);
}

#[test]
fn out_escapes_string() {
    assert_eq!(out(&build_scalar_string("a\"b\\c\n")), r#""a\"b\\c\n""#);
}

#[test]
fn typeof_variants() {
    assert_eq!(jsonb_typeof(&build_array()).unwrap(), "array");
    assert_eq!(jsonb_typeof(&build_object()).unwrap(), "object");
    assert_eq!(jsonb_typeof(&build_scalar_string("x")).unwrap(), "string");
    assert_eq!(jsonb_typeof(&build_scalar_bool(true)).unwrap(), "boolean");
    assert_eq!(jsonb_typeof(&to_bytes(&JsonbValue::null())).unwrap(), "null");
}

#[test]
fn bool_cast() {
    assert_eq!(jsonb_bool(&build_scalar_bool(true)).unwrap(), Some(true));
    assert_eq!(jsonb_bool(&build_scalar_bool(false)).unwrap(), Some(false));
    // jbvNull yields SQL NULL (None).
    assert_eq!(jsonb_bool(&to_bytes(&JsonbValue::null())).unwrap(), None);
    // wrong type -> cannot cast.
    let err = jsonb_bool(&build_scalar_string("x")).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
}

#[test]
fn unquote_string() {
    let ctx = MemoryContext::new("u");
    let jb = build_scalar_string("hello");
    assert_eq!(JsonbUnquote(ctx.mcx(), &jb).unwrap().as_slice(), b"hello");
}

#[test]
fn unquote_container_renders() {
    let ctx = MemoryContext::new("u");
    let jb = build_object();
    assert_eq!(
        String::from_utf8(JsonbUnquote(ctx.mcx(), &jb).unwrap().as_slice().to_vec()).unwrap(),
        r#"{"k": "v"}"#
    );
}

#[test]
fn object_builder_pairs() {
    let ctx = MemoryContext::new("o");
    // jsonb_object(['a','1','b','2']) (1-D, 4 elements) -> {"a":"1","b":"2"}.
    let datums = vec![
        Some(b"a".to_vec()),
        Some(b"1".to_vec()),
        Some(b"b".to_vec()),
        Some(b"2".to_vec()),
    ];
    let jb = jsonb_object(ctx.mcx(), 1, &[4], &datums).unwrap();
    assert_eq!(out(jb.as_slice()), r#"{"a": "1", "b": "2"}"#);
}

#[test]
fn object_builder_odd_errors() {
    let ctx = MemoryContext::new("o");
    let datums = vec![Some(b"a".to_vec()), Some(b"1".to_vec()), Some(b"b".to_vec())];
    let err = jsonb_object(ctx.mcx(), 1, &[3], &datums).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_ARRAY_SUBSCRIPT_ERROR);
}

#[test]
fn object_two_arg_builder() {
    let ctx = MemoryContext::new("o");
    let keys = vec![Some(b"k1".to_vec()), Some(b"k2".to_vec())];
    let vals = vec![Some(b"v1".to_vec()), None];
    let jb = jsonb_object_two_arg(ctx.mcx(), 1, 1, &keys, &vals).unwrap();
    assert_eq!(out(jb.as_slice()), r#"{"k1": "v1", "k2": null}"#);
}

#[test]
fn object_two_arg_null_key_errors() {
    let ctx = MemoryContext::new("o");
    let keys = vec![None];
    let vals = vec![Some(b"v".to_vec())];
    let err = jsonb_object_two_arg(ctx.mcx(), 1, 1, &keys, &vals).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_NULL_VALUE_NOT_ALLOWED);
}

#[test]
fn build_array_noargs_empty() {
    install_seams();
    let ctx = MemoryContext::new("a");
    let jb = jsonb_build_array_noargs(ctx.mcx()).unwrap();
    assert_eq!(out(jb.as_slice()), "[]");
}

#[test]
fn build_object_noargs_empty() {
    install_seams();
    let ctx = MemoryContext::new("o");
    let jb = jsonb_build_object_noargs(ctx.mcx()).unwrap();
    assert_eq!(out(jb.as_slice()), "{}");
}

#[test]
fn recv_unsupported_version() {
    let ctx = MemoryContext::new("r");
    let err = jsonb_recv(ctx.mcx(), &[2u8, b'1']).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INTERNAL_ERROR);
}

#[test]
fn recv_empty_errors() {
    let ctx = MemoryContext::new("r");
    let err = jsonb_recv(ctx.mcx(), &[]).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_PROTOCOL_VIOLATION);
}

#[test]
fn check_string_len_limit() {
    assert_eq!(checkStringLen(0, None).unwrap(), true);
    assert_eq!(checkStringLen(JENTRY_OFFLENMASK, None).unwrap(), true);
    let err = checkStringLen(JENTRY_OFFLENMASK + 1, None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_PROGRAM_LIMIT_EXCEEDED);
}

/// Drive the `jsonb_in_*` semantic actions to assemble `{"a": 1, "b": [true]}`
/// (the parser would call exactly these), then render the assembled bytes —
/// exercising the numeric scalar path through `numeric_in`.
#[test]
fn semantic_actions_assemble_object_with_number() {
    install_seams();
    let ctx = MemoryContext::new("s");
    let mut st = JsonbInState::default();

    jsonb_in_object_start(&mut st).unwrap();
    jsonb_in_object_field_start(&mut st, b"a", None).unwrap();
    jsonb_in_scalar(ctx.mcx(), &mut st, Some(b"1"), JsonTokenType::JSON_TOKEN_NUMBER, None).unwrap();
    jsonb_in_object_field_start(&mut st, b"b", None).unwrap();
    jsonb_in_array_start(&mut st).unwrap();
    jsonb_in_scalar(ctx.mcx(), &mut st, None, JsonTokenType::JSON_TOKEN_TRUE, None).unwrap();
    jsonb_in_array_end(&mut st).unwrap();
    jsonb_in_object_end(&mut st).unwrap();

    let bytes = jbu::JsonbValueToJsonb(ctx.mcx(), st.res.as_ref().unwrap())
        .unwrap()
        .as_slice()
        .to_vec();
    assert_eq!(out(&bytes), r#"{"a": 1, "b": [true]}"#);
    assert_eq!(jsonb_typeof(&bytes).unwrap(), "object");
}

/// A bare top-level scalar number assembles as a raw-scalar jsonb that renders
/// without brackets and casts to numeric/int.
#[test]
fn semantic_actions_top_level_scalar_number() {
    install_seams();
    let ctx = MemoryContext::new("s");
    let mut st = JsonbInState::default();
    jsonb_in_scalar(ctx.mcx(), &mut st, Some(b"42"), JsonTokenType::JSON_TOKEN_NUMBER, None).unwrap();
    let bytes = jbu::JsonbValueToJsonb(ctx.mcx(), st.res.as_ref().unwrap())
        .unwrap()
        .as_slice()
        .to_vec();
    assert_eq!(out(&bytes), "42");
    assert_eq!(jsonb_int4(&bytes).unwrap(), Some(42));
    assert_eq!(jsonb_typeof(&bytes).unwrap(), "number");
}

/// Indented rendering exercises the `add_indent` / redo-switch paths.
#[test]
fn out_indent_object() {
    install_seams();
    let ctx = MemoryContext::new("i");
    let jb = build_object();
    let rendered = JsonbToCStringIndent(ctx.mcx(), &jb[VARHDRSZ..], jb.len() as i32).unwrap();
    assert_eq!(
        String::from_utf8(rendered.as_slice().to_vec()).unwrap(),
        "{\n    \"k\": \"v\"\n}"
    );
}

/// `jsonb_send` frames the rendered text with a leading version byte.
#[test]
fn send_prefixes_version_byte() {
    install_seams();
    let ctx = MemoryContext::new("send");
    let jb = build_scalar_string("x");
    let sent = jsonb_send(ctx.mcx(), &jb).unwrap();
    assert_eq!(sent[0], 1u8);
    assert_eq!(&sent.as_slice()[1..], br#""x""#);
}
