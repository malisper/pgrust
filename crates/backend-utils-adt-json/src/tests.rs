//! Tests for the pure (non-seamed) `json.c` cores — escaping, the unique-key
//! check, the structural builders, the aggregate state machine, the unique
//! semantic actions — plus the seam-driven validation / typeof / datum-render
//! paths exercised through installed test seams.
//!
//! Seam providers are plain `fn` pointers installed via the owning seam
//! crates' `set(...)`. Each test that depends on a seam installs it; the
//! `set`-twice panic means we install at most once per process, so the
//! cross-owner seams are installed under a `Once`.

use super::*;

use mcx::MemoryContext;
use std::sync::Once;

// The bare-word `Datum` (`types_datum::Datum`, re-aliased `ScalarWord` in the
// crate body) is the seam-contract currency these tests feed into the
// `output_function_call` / aggregate / build relay paths. It is still the type
// every consumed json/jsonfuncs/timestamp seam speaks, so the test fixtures
// construct it directly (forced seam-contract residual).
use types_datum::Datum;

use types_json::{JsonParseErrorType as PErr, JsonTokenType as Tok, JsonTypeCategory as Cat};

fn ctx() -> MemoryContext {
    MemoryContext::new("json-test")
}

// ---------------------------------------------------------------------------
// escape_json / escape_json_with_len / escape_json_char.
// ---------------------------------------------------------------------------

fn escaped(s: &[u8]) -> String {
    let c = ctx();
    let mut buf = PgVec::new_in(c.mcx());
    escape_json(&mut buf, s).unwrap();
    String::from_utf8(buf.as_slice().to_vec()).unwrap()
}

fn escaped_len(s: &[u8]) -> String {
    let c = ctx();
    let mut buf = PgVec::new_in(c.mcx());
    escape_json_with_len(&mut buf, s).unwrap();
    String::from_utf8(buf.as_slice().to_vec()).unwrap()
}

#[test]
fn escape_json_quotes_and_metachars() {
    assert_eq!(escaped(b"abc"), "\"abc\"");
    assert_eq!(escaped(b"a\"b"), "\"a\\\"b\"");
    assert_eq!(escaped(b"a\\b"), "\"a\\\\b\"");
    assert_eq!(escaped(b"a\nb\tc"), "\"a\\nb\\tc\"");
    assert_eq!(escaped(b"\x08\x0c\r"), "\"\\b\\f\\r\"");
}

#[test]
fn escape_json_control_chars_use_u_escape() {
    assert_eq!(escaped(b"\x01"), "\"\\u0001\"");
    assert_eq!(escaped(b"\x1f"), "\"\\u001f\"");
}

#[test]
fn escape_json_stops_at_nul() {
    assert_eq!(escaped(b"ab\0cd"), "\"ab\"");
}

#[test]
fn escape_json_with_len_matches_escape_json_for_nul_free() {
    let s = b"hello \"world\"\n\tdone";
    assert_eq!(escaped_len(s), escaped(s));
}

#[test]
fn escape_json_with_len_keeps_embedded_nul() {
    // escape_json_with_len does NOT stop at NUL (it has a length).
    let out = escaped_len(b"ab\0cd");
    assert_eq!(out, "\"ab\\u0000cd\"");
}

// ---------------------------------------------------------------------------
// json_object / json_object_two_arg (pure, no seams).
// ---------------------------------------------------------------------------

fn s(out: &PgVec<'_, u8>) -> String {
    String::from_utf8(out.as_slice().to_vec()).unwrap()
}

#[test]
fn json_object_one_dim() {
    let c = ctx();
    let datums: [&[u8]; 4] = [b"a", b"1", b"b", b"2"];
    let nulls = [false, false, false, false];
    let out = json_object(c.mcx(), 1, &[4], &datums, &nulls).unwrap();
    assert_eq!(s(&out), "{\"a\" : \"1\", \"b\" : \"2\"}");
}

#[test]
fn json_object_empty() {
    let c = ctx();
    let out = json_object(c.mcx(), 0, &[], &[], &[]).unwrap();
    assert_eq!(s(&out), "{}");
}

#[test]
fn json_object_odd_elements_errors() {
    let c = ctx();
    let datums: [&[u8]; 3] = [b"a", b"1", b"b"];
    let nulls = [false, false, false];
    assert!(json_object(c.mcx(), 1, &[3], &datums, &nulls).is_err());
}

#[test]
fn json_object_null_value_renders_null() {
    let c = ctx();
    let datums: [&[u8]; 2] = [b"a", b""];
    let nulls = [false, true];
    let out = json_object(c.mcx(), 1, &[2], &datums, &nulls).unwrap();
    assert_eq!(s(&out), "{\"a\" : null}");
}

#[test]
fn json_object_two_arg_basic() {
    let c = ctx();
    let keys: [&[u8]; 2] = [b"k1", b"k2"];
    let vals: [&[u8]; 2] = [b"v1", b"v2"];
    let kn = [false, false];
    let vn = [false, false];
    let out = json_object_two_arg(c.mcx(), 1, 1, &keys, &kn, &vals, &vn).unwrap();
    assert_eq!(s(&out), "{\"k1\" : \"v1\", \"k2\" : \"v2\"}");
}

// ---------------------------------------------------------------------------
// Unique-key check (needs the common-hashfn seams installed).
// ---------------------------------------------------------------------------

static HASHFN_INIT: Once = Once::new();

fn install_hashfn() {
    HASHFN_INIT.call_once(|| {
        // Deterministic test hashes: order-insensitive sums. The exact match
        // function (object_id, key_len, bytes) carries correctness; the hash is
        // only a fast-path filter, so any consistent function is sufficient.
        common_hashfn_seams::hash_bytes_uint32::set(|k: u32| k.wrapping_mul(2654435761));
        common_hashfn_seams::tag_hash::set(|key: &[u8], _sz: usize| {
            let mut h: u32 = 0;
            for &b in key {
                h = h.wrapping_mul(31).wrapping_add(b as u32);
            }
            h
        });
    });
}

#[test]
fn unique_check_detects_dups_within_object() {
    install_hashfn();
    let mut st = JsonUniqueCheckState::default();
    json_unique_check_init(&mut st);
    assert!(json_unique_check_key(&mut st, b"a", 0));
    assert!(json_unique_check_key(&mut st, b"b", 0));
    // duplicate "a" in same object => false (already present).
    assert!(!json_unique_check_key(&mut st, b"a", 0));
    // "a" in a different object id is fine.
    assert!(json_unique_check_key(&mut st, b"a", 1));
}

#[test]
fn unique_parsing_actions_track_objects() {
    install_hashfn();
    let mut st = JsonUniqueParsingState::new();
    assert_eq!(json_unique_object_start(&mut st), PErr::JSON_SUCCESS);
    assert_eq!(json_unique_object_field_start(&mut st, b"a", false), PErr::JSON_SUCCESS);
    // duplicate key in the same object marks not-unique (but still SUCCESS).
    assert_eq!(json_unique_object_field_start(&mut st, b"a", false), PErr::JSON_SUCCESS);
    assert!(!st.unique);
}

// ---------------------------------------------------------------------------
// Aggregates + datum rendering, driven by an installed categorize/output seam.
// ---------------------------------------------------------------------------

static CATALOG_INIT: Once = Once::new();

fn install_catalog() {
    CATALOG_INIT.call_once(|| {
        // Treat every type as JSONTYPE_NUMERIC and render the Datum's integer
        // value via the "output function" (which just stringifies it).
        backend_utils_adt_jsonfuncs_seams::categorize_type::set(|_typoid| {
            Ok((Cat::JSONTYPE_NUMERIC, 42))
        });
        backend_utils_adt_jsonfuncs_seams::output_function_call::set(|_oid, val: Datum| {
            Ok(alloc::format!("{}", val.as_i32()).into_bytes())
        });
        // datum_to_json_internal (post-audit) guards recursion with
        // check_stack_depth; install a no-op so the builder paths under test
        // do not panic on the uninstalled seam.
        backend_utils_misc_stack_depth_seams::check_stack_depth::set(|| Ok(()));
    });
}

#[test]
fn json_agg_builds_array() {
    install_catalog();
    let c = ctx();
    let mut state: Option<JsonAggState> = None;
    state = Some(json_agg_transfn(c.mcx(), state, 23, Datum::from_i32(1), false).unwrap());
    state = Some(
        json_agg_transfn(c.mcx(), state, 23, Datum::from_i32(2), false).unwrap(),
    );
    let out = json_agg_finalfn(c.mcx(), state.as_ref()).unwrap().unwrap();
    assert_eq!(s(&out), "[1, 2]");
}

#[test]
fn json_agg_finalfn_none_for_no_rows() {
    let c = ctx();
    let out = json_agg_finalfn(c.mcx(), None).unwrap();
    assert!(out.is_none());
}

#[test]
fn json_object_agg_builds_object() {
    install_catalog();
    let c = ctx();
    let mut state: Option<JsonAggState> = None;
    state = Some(
        json_object_agg_transfn(
            c.mcx(),
            state,
            23,
            23,
            Datum::from_i32(1),
            false,
            Datum::from_i32(10),
            false,
        )
        .unwrap(),
    );
    let out = json_object_agg_finalfn(c.mcx(), state.as_ref()).unwrap().unwrap();
    // key is rendered as a quoted scalar (key_scalar=true): "1" : 10
    assert_eq!(s(&out), "{ \"1\" : 10 }");
}

#[test]
fn json_object_agg_null_key_errors() {
    install_catalog();
    let c = ctx();
    let r = json_object_agg_transfn(
        c.mcx(),
        None,
        23,
        23,
        Datum::null(),
        true,
        Datum::from_i32(1),
        false,
    );
    assert!(r.is_err());
}

#[test]
fn json_build_object_even_args() {
    install_catalog();
    let c = ctx();
    let args = [Datum::from_i32(1), Datum::from_i32(2)];
    let nulls = [false, false];
    let types = [23u32, 23u32];
    let out = json_build_object_worker(c.mcx(), &args, &nulls, &types, false, false).unwrap();
    // key rendered as quoted scalar, value as number.
    assert_eq!(s(&out), "{\"1\" : 2}");
}

#[test]
fn json_build_object_odd_args_errors() {
    let c = ctx();
    let args = [Datum::from_i32(1)];
    let nulls = [false];
    let types = [23u32];
    assert!(json_build_object_worker(c.mcx(), &args, &nulls, &types, false, false).is_err());
}

#[test]
fn json_build_array_basic() {
    install_catalog();
    let c = ctx();
    let args = [Datum::from_i32(1), Datum::from_i32(2)];
    let nulls = [false, false];
    let types = [23u32, 23u32];
    let out = json_build_array_worker(c.mcx(), &args, &nulls, &types, false).unwrap();
    assert_eq!(s(&out), "[1, 2]");
}

#[test]
fn json_build_array_absent_on_null_skips() {
    install_catalog();
    let c = ctx();
    let args = [Datum::from_i32(1), Datum::null(), Datum::from_i32(3)];
    let nulls = [false, true, false];
    let types = [23u32, 23u32, 23u32];
    let out = json_build_array_worker(c.mcx(), &args, &nulls, &types, true).unwrap();
    assert_eq!(s(&out), "[1, 3]");
}

#[test]
fn build_noargs() {
    let c = ctx();
    assert_eq!(s(&json_build_object_noargs(c.mcx()).unwrap()), "{}");
    assert_eq!(s(&json_build_array_noargs(c.mcx()).unwrap()), "[]");
}

// ---------------------------------------------------------------------------
// json_validate / json_typeof via installed jsonapi seams.
// ---------------------------------------------------------------------------

static JSONAPI_INIT: Once = Once::new();

fn install_jsonapi() {
    JSONAPI_INIT.call_once(|| {
        // Trivial validator: nonempty input is valid; "{}" is unique.
        common_jsonapi_seams::parse_validate::set(|json: &[u8]| {
            if json.is_empty() {
                PErr::JSON_INVALID_TOKEN
            } else {
                PErr::JSON_SUCCESS
            }
        });
        common_jsonapi_seams::parse_validate_unique::set(|json: &[u8]| {
            (
                if json.is_empty() { PErr::JSON_INVALID_TOKEN } else { PErr::JSON_SUCCESS },
                true,
            )
        });
        common_jsonapi_seams::lex_first_token::set(|json: &[u8]| {
            let t = match json.first() {
                Some(b'{') => Tok::JSON_TOKEN_OBJECT_START,
                Some(b'[') => Tok::JSON_TOKEN_ARRAY_START,
                Some(b'"') => Tok::JSON_TOKEN_STRING,
                Some(b't') | Some(b'f') => Tok::JSON_TOKEN_TRUE,
                Some(b'n') => Tok::JSON_TOKEN_NULL,
                Some(c) if c.is_ascii_digit() => Tok::JSON_TOKEN_NUMBER,
                _ => Tok::JSON_TOKEN_INVALID,
            };
            (PErr::JSON_SUCCESS, t)
        });
        common_jsonapi_seams::errsave_error::set(|_e, _json| {
            Err(PgError::error("invalid input syntax for type json"))
        });
    });
}

#[test]
fn json_validate_ok_and_bad() {
    install_jsonapi();
    assert!(json_validate(b"{}", false, false).unwrap());
    assert!(!json_validate(b"", false, false).unwrap());
    assert!(json_validate(b"", false, true).is_err());
}

#[test]
fn json_typeof_categories() {
    install_jsonapi();
    assert_eq!(json_typeof(b"{}").unwrap(), "object");
    assert_eq!(json_typeof(b"[1]").unwrap(), "array");
    assert_eq!(json_typeof(b"\"x\"").unwrap(), "string");
    assert_eq!(json_typeof(b"42").unwrap(), "number");
    assert_eq!(json_typeof(b"true").unwrap(), "boolean");
    assert_eq!(json_typeof(b"null").unwrap(), "null");
}

#[test]
fn json_in_roundtrips_valid() {
    install_jsonapi();
    let c = ctx();
    let out = json_in(c.mcx(), b"{}").unwrap().unwrap();
    assert_eq!(s(&out), "{}");
}
