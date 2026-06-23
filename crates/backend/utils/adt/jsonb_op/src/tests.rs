//! Unit tests for the jsonb_op operator layer.
//!
//! Each test builds a `JsonbValue` tree, serializes it to on-disk bytes via the
//! sibling engine's `JsonbValueToJsonb`, slices off the varlena header to get
//! the root container bytes the operators consume, then exercises the operator.
//!
//! The genuine externals reached across the seam boundary (the recursion guard,
//! numeric/string comparison, the byte-hash primitives, and
//! `deconstruct_text_array`) are installed with faithful deterministic
//! implementations behind a process-global `Once`.

extern crate std;

use std::sync::Once;
use std::vec::Vec;

use super::*;
use jsonb_util::{JsonbValueToJsonb, VARHDRSZ};
use tsvector_ext_seams::ArrayElem;

static INSTALL: Once = Once::new();

fn install_seams() {
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
            let mut h: u32 = 0x811c9dc5;
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
        // `deconstruct_text_array` over a tiny private wire format produced by
        // `text_array` below: u32 count, then per element a u8 null-flag and a
        // u32 length + that many value bytes.
        array_more_seams::deconstruct_text_array::set(|arr| {
            let mut out: Vec<ArrayElem> = Vec::new();
            let mut p = 0usize;
            let count = u32::from_ne_bytes([arr[0], arr[1], arr[2], arr[3]]);
            p += 4;
            for _ in 0..count {
                let is_null = arr[p] != 0;
                p += 1;
                let len =
                    u32::from_ne_bytes([arr[p], arr[p + 1], arr[p + 2], arr[p + 3]]) as usize;
                p += 4;
                let value = arr[p..p + len].to_vec();
                p += len;
                out.push(ArrayElem { value, is_null });
            }
            Ok(out)
        });
    });
}

/// Encode a `text[]` for the test `deconstruct_text_array` seam above.
fn text_array(elems: &[Option<&str>]) -> Vec<u8> {
    let mut v: Vec<u8> = Vec::new();
    v.extend_from_slice(&(elems.len() as u32).to_ne_bytes());
    for e in elems {
        match e {
            None => {
                v.push(1);
                v.extend_from_slice(&0u32.to_ne_bytes());
            }
            Some(s) => {
                v.push(0);
                v.extend_from_slice(&(s.len() as u32).to_ne_bytes());
                v.extend_from_slice(s.as_bytes());
            }
        }
    }
    v
}

fn jstring(s: &str) -> JsonbValue {
    JsonbValue {
        typ: jbvType::jbvString,
        val: JsonbValueData::String(s.as_bytes().to_vec()),
    }
}

fn jobj(pairs: &[(&str, JsonbValue)]) -> JsonbValue {
    use jsonb_util::JsonbPair;
    let mut ps: Vec<JsonbPair> = Vec::new();
    for (i, (k, v)) in pairs.iter().enumerate() {
        ps.push(JsonbPair {
            key: JsonbValue {
                typ: jbvType::jbvString,
                val: JsonbValueData::String(k.as_bytes().to_vec()),
            },
            value: v.clone(),
            order: i as u32,
        });
    }
    JsonbValue {
        typ: jbvType::jbvObject,
        val: JsonbValueData::Object(ps),
    }
}

fn jarr(elems: &[JsonbValue]) -> JsonbValue {
    JsonbValue {
        typ: jbvType::jbvArray,
        val: JsonbValueData::Array {
            elems: elems.to_vec(),
            raw_scalar: false,
        },
    }
}

/// Serialize and return the root container bytes (after the varlena header).
fn root(v: &JsonbValue) -> Vec<u8> {
    let ctx = mcx::MemoryContext::new("jsonb_op.test");
    let buf = JsonbValueToJsonb(ctx.mcx(), v).unwrap();
    buf.as_slice()[VARHDRSZ..].to_vec()
}

#[test]
fn exists_object_key() {
    install_seams();
    let o = root(&jobj(&[("a", jstring("x")), ("b", jstring("y"))]));
    assert!(jsonb_exists(&o, b"a").unwrap());
    assert!(jsonb_exists(&o, b"b").unwrap());
    assert!(!jsonb_exists(&o, b"c").unwrap());
}

#[test]
fn exists_array_string_element() {
    install_seams();
    let a = root(&jarr(&[jstring("foo"), jstring("bar")]));
    assert!(jsonb_exists(&a, b"foo").unwrap());
    assert!(!jsonb_exists(&a, b"baz").unwrap());
}

#[test]
fn exists_any_all() {
    install_seams();
    let o = root(&jobj(&[("a", jstring("1")), ("b", jstring("2"))]));

    let any_hit = text_array(&[None, Some("z"), Some("b")]);
    assert!(jsonb_exists_any(&o, &any_hit).unwrap());

    let any_miss = text_array(&[Some("x"), Some("y")]);
    assert!(!jsonb_exists_any(&o, &any_miss).unwrap());

    let all_hit = text_array(&[Some("a"), Some("b"), None]);
    assert!(jsonb_exists_all(&o, &all_hit).unwrap());

    let all_miss = text_array(&[Some("a"), Some("c")]);
    assert!(!jsonb_exists_all(&o, &all_miss).unwrap());

    // All-null array vacuously satisfies exists_all and fails exists_any.
    let all_null = text_array(&[None, None]);
    assert!(jsonb_exists_all(&o, &all_null).unwrap());
    assert!(!jsonb_exists_any(&o, &all_null).unwrap());
}

#[test]
fn contains_and_contained() {
    install_seams();
    let big = root(&jobj(&[("a", jstring("1")), ("b", jstring("2"))]));
    let sub = root(&jobj(&[("a", jstring("1"))]));
    let miss = root(&jobj(&[("a", jstring("9"))]));

    assert!(jsonb_contains(&big, &sub).unwrap());
    assert!(!jsonb_contains(&big, &miss).unwrap());
    assert!(!jsonb_contains(&sub, &big).unwrap());

    // contained is the commutator: contained(tmpl, val) == contains(val, tmpl).
    assert!(jsonb_contained(&sub, &big).unwrap());
    assert!(!jsonb_contained(&big, &sub).unwrap());

    // Object vs array root: type mismatch short-circuits false.
    let arr = root(&jarr(&[jstring("a")]));
    assert!(!jsonb_contains(&big, &arr).unwrap());
}

#[test]
fn btree_comparators() {
    install_seams();
    let a = root(&jobj(&[("a", jstring("1"))]));
    let b = root(&jobj(&[("a", jstring("1"))]));
    let c = root(&jobj(&[("a", jstring("2"))]));

    assert!(jsonb_eq(&a, &b).unwrap());
    assert!(!jsonb_ne(&a, &b).unwrap());
    assert!(jsonb_le(&a, &b).unwrap());
    assert!(jsonb_ge(&a, &b).unwrap());
    assert!(!jsonb_lt(&a, &b).unwrap());
    assert!(!jsonb_gt(&a, &b).unwrap());
    assert_eq!(jsonb_cmp(&a, &b).unwrap(), 0);

    assert!(jsonb_ne(&a, &c).unwrap());
    assert!(jsonb_lt(&a, &c).unwrap());
    assert!(jsonb_gt(&c, &a).unwrap());
    assert!(jsonb_cmp(&a, &c).unwrap() < 0);
    assert!(jsonb_cmp(&c, &a).unwrap() > 0);
}

#[test]
fn hash_equal_for_equal_values_and_empty() {
    install_seams();
    let a = root(&jobj(&[("a", jstring("1")), ("b", jstring("2"))]));
    let b = root(&jobj(&[("a", jstring("1")), ("b", jstring("2"))]));
    let c = root(&jobj(&[("a", jstring("1")), ("b", jstring("9"))]));

    assert_eq!(jsonb_hash(&a).unwrap(), jsonb_hash(&b).unwrap());
    assert_ne!(jsonb_hash(&a).unwrap(), jsonb_hash(&c).unwrap());

    assert_eq!(
        jsonb_hash_extended(&a, 42).unwrap(),
        jsonb_hash_extended(&b, 42).unwrap()
    );
    assert_ne!(
        jsonb_hash_extended(&a, 42).unwrap(),
        jsonb_hash_extended(&a, 7).unwrap()
    );

    // Empty array hashes to 0 / seed.
    let empty = root(&jarr(&[]));
    assert_eq!(jsonb_hash(&empty).unwrap(), 0);
    assert_eq!(jsonb_hash_extended(&empty, 99).unwrap(), 99);
}
