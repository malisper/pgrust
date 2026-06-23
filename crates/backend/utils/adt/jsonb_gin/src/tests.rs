//! Unit tests for the `jsonb_gin` port.
//!
//! These exercise the GIN key encoding (`make_text_key` / `make_scalar_key`),
//! the `jsonb_ops` / `jsonb_path_ops` entry extraction over real on-disk jsonb
//! built with the sibling `jsonb_util` engine, the
//! consistency / triconsistency strategy logic, and `numeric_normalize`. The
//! jsonpath expression-tree evaluator is tested directly via
//! [`execute_jsp_gin_node`] over hand-built nodes.

extern crate std;

use std::string::ToString;
use std::sync::Once;
use std::vec;
use std::vec::Vec;

use super::*;
use ::jsonb_util::{JsonbPair, JsonbValueToJsonb};
use ::mcx::MemoryContext;

/// Install the sibling-crate seams the jsonb engine and our hashing reach when
/// these tests serialize / iterate / hash real on-disk jsonb.
fn install_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        stack_depth_seams::check_stack_depth::set(|| Ok(()));
        // Real CityHash primitives so make_text_key's overlength hashing and
        // JsonbHashScalarValue produce stable values.
        hashfn_seams::hash_bytes::set(hashfn::hash_bytes);
        hashfn_seams::hash_bytes_extended::set(hashfn::hash_bytes_extended);
        hashfn_seams::hash_bytes_uint32_extended::set(
            hashfn::hash_bytes_uint32_extended,
        );
    });
}

/// Build the on-disk root container bytes (C: `&jb->root`) from a JsonbValue.
fn root_of(mcx: Mcx<'_>, v: &JsonbValue) -> Vec<u8> {
    install_seams();
    let bytes = JsonbValueToJsonb(mcx, v).unwrap();
    bytes[VARHDRSZ..].to_vec()
}

fn jstr(s: &str) -> JsonbValue {
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

fn jnull() -> JsonbValue {
    JsonbValue::null()
}

fn jobject(pairs: Vec<(&str, JsonbValue)>) -> JsonbValue {
    let pairs = pairs
        .into_iter()
        .enumerate()
        .map(|(i, (k, val))| JsonbPair {
            key: jstr(k),
            value: val,
            order: i as u32,
        })
        .collect();
    JsonbValue {
        typ: jbvType::jbvObject,
        val: JsonbValueData::Object(pairs),
    }
}

fn jarray(elems: Vec<JsonbValue>) -> JsonbValue {
    JsonbValue {
        typ: jbvType::jbvArray,
        val: JsonbValueData::Array {
            elems,
            raw_scalar: false,
        },
    }
}

// ---------------------------------------------------------------------------
// make_text_key / make_scalar_key
// ---------------------------------------------------------------------------

#[test]
fn text_key_layout() {
    install_seams();
    let key = make_text_key(JGINFLAG_KEY, b"foo");
    // VARHDRSZ + 1 (flag) + 3 (payload).
    assert_eq!(key.len(), VARHDRSZ + 1 + 3);
    assert_eq!(key[VARHDRSZ], JGINFLAG_KEY);
    assert_eq!(&key[VARHDRSZ + 1..], b"foo");
    // No hashing for short text.
    assert_eq!(key[VARHDRSZ] & JGINFLAG_HASHED, 0);
}

#[test]
fn text_key_hashes_overlength() {
    install_seams();
    let long = vec![b'x'; (JGIN_MAXLENGTH as usize) + 1];
    let key = make_text_key(JGINFLAG_STR, &long);
    // Hashed: flag has the HASHED bit, payload is the 8-char hex hash.
    assert_ne!(key[VARHDRSZ] & JGINFLAG_HASHED, 0);
    assert_eq!(key[VARHDRSZ] & 0x0f, JGINFLAG_STR);
    assert_eq!(key.len(), VARHDRSZ + 1 + 8);
}

#[test]
fn scalar_key_types() {
    let ctx = MemoryContext::new("jsonb_gin.test.scalar_key");
    install_seams();
    let mcx = ctx.mcx();

    let s = make_scalar_key(mcx, &jstr("k"), true).unwrap();
    assert_eq!(s[VARHDRSZ], JGINFLAG_KEY);
    let s = make_scalar_key(mcx, &jstr("k"), false).unwrap();
    assert_eq!(s[VARHDRSZ], JGINFLAG_STR);
    let b = make_scalar_key(mcx, &jbool(true), false).unwrap();
    assert_eq!(b[VARHDRSZ], JGINFLAG_BOOL);
    assert_eq!(&b[VARHDRSZ + 1..], b"t");
    let n = make_scalar_key(mcx, &jnull(), false).unwrap();
    assert_eq!(n[VARHDRSZ], JGINFLAG_NULL);
    assert_eq!(n.len(), VARHDRSZ + 1);
}

// ---------------------------------------------------------------------------
// gin_extract_jsonb (jsonb_ops extractValue)
// ---------------------------------------------------------------------------

#[test]
fn extract_jsonb_empty_object_has_no_keys() {
    let ctx = MemoryContext::new("jsonb_gin.test.empty");
    let mcx = ctx.mcx();
    let root = root_of(mcx, &jobject(vec![]));
    let entries = gin_extract_jsonb(mcx, &root).unwrap();
    assert!(entries.is_empty());
}

#[test]
fn extract_jsonb_object_keys_and_values() {
    let ctx = MemoryContext::new("jsonb_gin.test.obj");
    let mcx = ctx.mcx();
    let root = root_of(mcx, &jobject(vec![("a", jstr("x")), ("b", jbool(false))]));
    let entries = gin_extract_jsonb(mcx, &root).unwrap();
    // Two keys (KEY flag) + one string value (STR flag) + one bool value
    // (BOOL flag).
    let flags: Vec<u8> = entries.iter().map(|e| e[VARHDRSZ]).collect();
    assert_eq!(flags.iter().filter(|&&f| f == JGINFLAG_KEY).count(), 2);
    assert_eq!(flags.iter().filter(|&&f| f == JGINFLAG_STR).count(), 1);
    assert_eq!(flags.iter().filter(|&&f| f == JGINFLAG_BOOL).count(), 1);
}

#[test]
fn extract_jsonb_string_array_elements_are_keys() {
    let ctx = MemoryContext::new("jsonb_gin.test.arr");
    let mcx = ctx.mcx();
    // String array elements are treated as keys (see jsonb.h); a bool element
    // is a value.
    let root = root_of(mcx, &jarray(vec![jstr("s"), jbool(true)]));
    let entries = gin_extract_jsonb(mcx, &root).unwrap();
    let flags: Vec<u8> = entries.iter().map(|e| e[VARHDRSZ]).collect();
    assert!(flags.contains(&JGINFLAG_KEY)); // the string element
    assert!(flags.contains(&JGINFLAG_BOOL)); // the bool element
}

// ---------------------------------------------------------------------------
// gin_extract_jsonb_path (jsonb_path_ops extractValue)
// ---------------------------------------------------------------------------

#[test]
fn extract_jsonb_path_emits_uint32_hashes() {
    let ctx = MemoryContext::new("jsonb_gin.test.path");
    let mcx = ctx.mcx();
    let root = root_of(mcx, &jobject(vec![("a", jstr("x")), ("b", jstr("y"))]));
    let entries = gin_extract_jsonb_path(&root).unwrap();
    // One hash entry per value; each is a 4-byte uint32 Datum.
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().all(|e| e.len() == 4));
    // {"a":"x"} and {"b":"x"} must differ — the key folds into the hash.
    let root2 = root_of(mcx, &jobject(vec![("a", jstr("x"))]));
    let root3 = root_of(mcx, &jobject(vec![("b", jstr("x"))]));
    let e2 = gin_extract_jsonb_path(&root2).unwrap();
    let e3 = gin_extract_jsonb_path(&root3).unwrap();
    assert_ne!(e2[0], e3[0]);
}

#[test]
fn extract_jsonb_path_empty_is_empty() {
    let ctx = MemoryContext::new("jsonb_gin.test.path_empty");
    let mcx = ctx.mcx();
    let root = root_of(mcx, &jobject(vec![]));
    assert!(gin_extract_jsonb_path(&root).unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// gin_compare_jsonb
// ---------------------------------------------------------------------------

#[test]
fn compare_is_byte_order() {
    assert_eq!(gin_compare_jsonb(b"a", b"a"), 0);
    assert_eq!(gin_compare_jsonb(b"a", b"b"), -1);
    assert_eq!(gin_compare_jsonb(b"b", b"a"), 1);
    assert_eq!(gin_compare_jsonb(b"a", b"ab"), -1);
}

// ---------------------------------------------------------------------------
// extractQuery strategies
// ---------------------------------------------------------------------------

#[test]
fn extract_query_exists_is_single_key() {
    let ctx = MemoryContext::new("jsonb_gin.test.q_exists");
    let mcx = ctx.mcx();
    let out =
        gin_extract_jsonb_query(mcx, GinJsonbQuery::Exists(b"foo"), JsonbExistsStrategyNumber)
            .unwrap();
    assert_eq!(out.entries.len(), 1);
    assert_eq!(out.entries[0][VARHDRSZ], JGINFLAG_KEY);
    assert!(!out.search_mode_all);
}

#[test]
fn extract_query_contains_empty_forces_full_scan() {
    let ctx = MemoryContext::new("jsonb_gin.test.q_contains_empty");
    let mcx = ctx.mcx();
    let root = root_of(mcx, &jobject(vec![]));
    let out = gin_extract_jsonb_query(
        mcx,
        GinJsonbQuery::Contains(&root),
        JsonbContainsStrategyNumber,
    )
    .unwrap();
    assert!(out.entries.is_empty());
    assert!(out.search_mode_all);
}

#[test]
fn extract_query_exists_all_empty_array_forces_full_scan() {
    let ctx = MemoryContext::new("jsonb_gin.test.q_existsall_empty");
    let mcx = ctx.mcx();
    let empty: [Option<&[u8]>; 0] = [];
    let out = gin_extract_jsonb_query(
        mcx,
        GinJsonbQuery::ExistsArray(&empty),
        JsonbExistsAllStrategyNumber,
    )
    .unwrap();
    assert!(out.entries.is_empty());
    assert!(out.search_mode_all);
}

#[test]
fn extract_query_exists_any_ignores_nulls() {
    let ctx = MemoryContext::new("jsonb_gin.test.q_existsany");
    let mcx = ctx.mcx();
    let elems: [Option<&[u8]>; 3] = [Some(b"a" as &[u8]), None, Some(b"b" as &[u8])];
    let out = gin_extract_jsonb_query(
        mcx,
        GinJsonbQuery::ExistsArray(&elems),
        JsonbExistsAnyStrategyNumber,
    )
    .unwrap();
    assert_eq!(out.entries.len(), 2);
    assert!(!out.search_mode_all);
}

// ---------------------------------------------------------------------------
// consistent / triconsistent strategy logic
// ---------------------------------------------------------------------------

#[test]
fn consistent_contains_requires_all_keys() {
    let (res, recheck) =
        gin_consistent_jsonb(&[true, true], JsonbContainsStrategyNumber, 2, None).unwrap();
    assert!(res);
    assert!(recheck);
    let (res, _) =
        gin_consistent_jsonb(&[true, false], JsonbContainsStrategyNumber, 2, None).unwrap();
    assert!(!res);
}

#[test]
fn consistent_exists_always_true_recheck() {
    let (res, recheck) =
        gin_consistent_jsonb(&[false], JsonbExistsStrategyNumber, 1, None).unwrap();
    assert!(res);
    assert!(recheck);
}

#[test]
fn triconsistent_contains_false_if_any_absent() {
    let res =
        gin_triconsistent_jsonb(&[GIN_TRUE, GIN_FALSE], JsonbContainsStrategyNumber, 2, None)
            .unwrap();
    assert_eq!(res, GIN_FALSE);
    let res =
        gin_triconsistent_jsonb(&[GIN_TRUE, GIN_MAYBE], JsonbContainsStrategyNumber, 2, None)
            .unwrap();
    assert_eq!(res, GIN_MAYBE); // never GIN_TRUE
}

#[test]
fn triconsistent_exists_any_maybe_if_one_present() {
    let res =
        gin_triconsistent_jsonb(&[GIN_FALSE, GIN_MAYBE], JsonbExistsAnyStrategyNumber, 2, None)
            .unwrap();
    assert_eq!(res, GIN_MAYBE);
    let res =
        gin_triconsistent_jsonb(&[GIN_FALSE, GIN_FALSE], JsonbExistsAnyStrategyNumber, 2, None)
            .unwrap();
    assert_eq!(res, GIN_FALSE);
}

#[test]
fn path_consistent_contains_requires_all_keys() {
    let (res, _) =
        gin_consistent_jsonb_path(&[true, false], JsonbContainsStrategyNumber, 2, None).unwrap();
    assert!(!res);
    let (res, recheck) =
        gin_consistent_jsonb_path(&[true, true], JsonbContainsStrategyNumber, 2, None).unwrap();
    assert!(res);
    assert!(recheck);
}

#[test]
fn unrecognized_strategy_errors() {
    assert!(gin_consistent_jsonb(&[], 999, 0, None).is_err());
    assert!(gin_triconsistent_jsonb(&[], 999, 0, None).is_err());
    assert!(gin_consistent_jsonb_path(&[], 999, 0, None).is_err());
}

// ---------------------------------------------------------------------------
// execute_jsp_gin_node (the jsonpath expression-tree evaluator)
// ---------------------------------------------------------------------------

#[test]
fn execute_and_or_logic() {
    // AND(entry0, entry1)
    let and_node = JsonPathGinNode::Logic {
        and: true,
        args: vec![
            JsonPathGinNode::EntryIndex(0),
            JsonPathGinNode::EntryIndex(1),
        ],
    };
    assert_eq!(
        execute_jsp_gin_node(&and_node, &[GIN_TRUE, GIN_TRUE]).unwrap(),
        GIN_TRUE
    );
    assert_eq!(
        execute_jsp_gin_node(&and_node, &[GIN_TRUE, GIN_FALSE]).unwrap(),
        GIN_FALSE
    );
    assert_eq!(
        execute_jsp_gin_node(&and_node, &[GIN_TRUE, GIN_MAYBE]).unwrap(),
        GIN_MAYBE
    );

    // OR(entry0, entry1)
    let or_node = JsonPathGinNode::Logic {
        and: false,
        args: vec![
            JsonPathGinNode::EntryIndex(0),
            JsonPathGinNode::EntryIndex(1),
        ],
    };
    assert_eq!(
        execute_jsp_gin_node(&or_node, &[GIN_FALSE, GIN_TRUE]).unwrap(),
        GIN_TRUE
    );
    assert_eq!(
        execute_jsp_gin_node(&or_node, &[GIN_FALSE, GIN_FALSE]).unwrap(),
        GIN_FALSE
    );
    assert_eq!(
        execute_jsp_gin_node(&or_node, &[GIN_FALSE, GIN_MAYBE]).unwrap(),
        GIN_MAYBE
    );
}

#[test]
fn execute_unemitted_entry_errors() {
    let node = JsonPathGinNode::EntryDatum(vec![1, 2, 3]);
    assert!(execute_jsp_gin_node(&node, &[GIN_TRUE]).is_err());
}

#[test]
fn emit_replaces_datums_with_indices() {
    install_seams();
    let mut node = JsonPathGinNode::Logic {
        and: true,
        args: vec![
            JsonPathGinNode::EntryDatum(vec![10]),
            JsonPathGinNode::EntryDatum(vec![20]),
        ],
    };
    let mut entries = GinEntries::default();
    emit_jsp_gin_entries(&mut node, &mut entries).unwrap();
    assert_eq!(entries.buf, vec![vec![10u8], vec![20u8]]);
    match &node {
        JsonPathGinNode::Logic { args, .. } => {
            assert!(matches!(args[0], JsonPathGinNode::EntryIndex(0)));
            assert!(matches!(args[1], JsonPathGinNode::EntryIndex(1)));
        }
        _ => panic!("expected Logic node"),
    }
}

// ---------------------------------------------------------------------------
// numeric_normalize
// ---------------------------------------------------------------------------

#[test]
fn numeric_normalize_strips_trailing_zeroes() {
    let ctx = MemoryContext::new("jsonb_gin.test.numnorm");
    let mcx = ctx.mcx();
    // Build the on-disk numeric for 1.50 -> normalized "1.5"; for 100 -> "100".
    use ::adt_numeric::convert::make_result;
    use ::adt_numeric::io::set_var_from_str;

    let render = |s: &str| -> std::string::String {
        let (v, _) = set_var_from_str(mcx, s, 0).unwrap();
        let disk = make_result(mcx, &v).unwrap();
        numeric_normalize(mcx, &disk).unwrap()
    };

    assert_eq!(render("1.50"), "1.5".to_string());
    assert_eq!(render("100"), "100".to_string());
    assert_eq!(render("10.00"), "10".to_string());
}
