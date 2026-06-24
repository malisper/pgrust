//! Unit tests for the jsonb_util convert/iterate substrate.
//!
//! Build a `JsonbValue` tree, serialize it to on-disk bytes, then iterate it
//! back and check token order / shapes / containment / ordering / hashing.
//!
//! The genuine externals (`numeric_eq`/`numeric_cmp`, `varstr_cmp`,
//! `json_encode_datetime`, `check_stack_depth`, the byte-hash primitives) live
//! in sibling subsystems reached across the per-owner seam boundary, so each
//! test installs a faithful seam implementation via the owning crate's seam
//! `set(...)` (once, behind a `std::sync::Once`, since the seam slots are
//! process-global).  The installed `numeric_*` / `varstr_cmp` providers compute
//! the same answer the real backend would for the byte-identical / collation-C
//! inputs these tests use, so the crate logic under test is exercised
//! end-to-end.

extern crate std;

use std::sync::Once;
use std::vec;
use std::vec::Vec;

use super::*;

static INSTALL: Once = Once::new();

/// Install the externals' seams with faithful, deterministic implementations.
fn install_seams() {
    INSTALL.call_once(|| {
        // Recursion guard: never overflows in these small tests.
        stack_depth_seams::check_stack_depth::set(|| Ok(()));
        // numeric equality / comparison over the on-disk numeric bytes.  These
        // tests use byte-identical numerics, so a byte comparison is the same
        // answer numeric_eq / numeric_cmp would give.
        numeric_seams::numeric_eq::set(|a, b| a == b);
        numeric_seams::numeric_cmp::set(|a, b| match a.cmp(b) {
            core::cmp::Ordering::Less => -1,
            core::cmp::Ordering::Equal => 0,
            core::cmp::Ordering::Greater => 1,
        });
        // varstr_cmp at the database default collation.  In a unit context the
        // default collation is C/POSIX, so the byte comparison is the faithful
        // answer (this is the `collate_is_c` reduction varstr_cmp performs).
        varlena_seams::varstr_cmp::set(|a, b, _coll| {
            Ok(match a.cmp(b) {
                core::cmp::Ordering::Less => -1,
                core::cmp::Ordering::Equal => 0,
                core::cmp::Ordering::Greater => 1,
            })
        });
        // Byte-hash primitives over the digit/string runs.  These mirror
        // common/hashfn.c; a deterministic FNV-1a is faithful enough for the
        // equal-hashes-for-equal-values property the jsonb hash tests assert.
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
    });
}

/// Serialize a `JsonbValue` to owned on-disk bytes, copying out of a per-call
/// memory context (the test analog of the C `palloc`'d return value).
fn to_jsonb(mcx: mcx::Mcx<'_>, v: &JsonbValue) -> Vec<u8> {
    let buf = JsonbValueToJsonb(mcx, v).unwrap();
    buf.as_slice().to_vec()
}

/// Intern owned bytes into the arena, yielding the `&'mcx [u8]` the on-disk
/// container reader needs (the test analog of `pg_detoast_datum` into `mcx`).
fn borrow_in<'mcx>(mcx: mcx::Mcx<'mcx>, bytes: &[u8]) -> &'mcx [u8] {
    ::mcx::slice_borrow_in(mcx, bytes).unwrap()
}

fn jstring(s: &str) -> JsonbValue<'_> {
    JsonbValue {
        typ: jbvType::jbvString,
        val: JsonbValueData::String(s.as_bytes()),
    }
}

fn jbool<'mcx>(b: bool) -> JsonbValue<'mcx> {
    JsonbValue {
        typ: jbvType::jbvBool,
        val: JsonbValueData::Bool(b),
    }
}

fn jnull<'mcx>() -> JsonbValue<'mcx> {
    JsonbValue {
        typ: jbvType::jbvNull,
        val: JsonbValueData::Null,
    }
}

/// Build the on-disk `numeric` varlena bytes for a small non-negative integer
/// `n` (0 <= n < 10000), in PostgreSQL short format: a 4-byte varlena header, a
/// 2-byte short header word (`NUMERIC_SHORT`, dscale 0, weight 0), and a single
/// base-10000 `i16` digit.  These bytes are exactly what `jbvNumeric` carries.
fn build_numeric_small(n: i16) -> Vec<u8> {
    assert!((0..10000).contains(&n));
    let mut v: Vec<u8> = Vec::new();
    let total = VARHDRSZ + 2 + 2; // varhdr + short hdr + one digit
    // varlena header: SET_VARSIZE -> (len << 2), native byte order.
    v.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    // short numeric header word: NUMERIC_SHORT, dscale=0, weight=0.
    v.extend_from_slice(&types_numeric::NUMERIC_SHORT.to_ne_bytes());
    // single digit.
    v.extend_from_slice(&n.to_ne_bytes());
    v
}

fn jnumeric<'mcx>(mcx: mcx::Mcx<'mcx>, n: i16) -> JsonbValue<'mcx> {
    JsonbValue {
        typ: jbvType::jbvNumeric,
        val: JsonbValueData::Numeric(borrow_in(mcx, &build_numeric_small(n))),
    }
}

/// Build `["a", true, null]` via the push API and round-trip it.
#[test]
fn array_roundtrip() {
    install_seams();
    let ctx = mcx::MemoryContext::new("jsonb_util.test");
    let mcx = ctx.mcx();
    let mut pstate: Option<Box<JsonbParseState>> = None;
    pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_BEGIN_ARRAY, None).unwrap();
    pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_ELEM, Some(&jstring("a"))).unwrap();
    pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_ELEM, Some(&jbool(true))).unwrap();
    pushJsonbValue(
        mcx, &mut pstate,
        JsonbIteratorToken::WJB_ELEM,
        Some(&JsonbValue::null()),
    )
    .unwrap();
    let res = pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_END_ARRAY, None)
        .unwrap()
        .unwrap();

    let bytes = to_jsonb(mcx, &res);

    // Iterate the on-disk container (skip the varlena header).
    let mut it = JsonbIteratorInit(mcx, borrow_in(mcx, &bytes[VARHDRSZ..]));
    let mut v = JsonbValue::null();
    let mut toks = Vec::new();
    let mut strings = Vec::new();
    loop {
        let t = JsonbIteratorNext(&mut it, &mut v, false).unwrap();
        if t == JsonbIteratorToken::WJB_DONE {
            break;
        }
        toks.push(t);
        if let JsonbValueData::String(s) = &v.val {
            strings.push(std::string::String::from_utf8(s.to_vec()).unwrap());
        }
    }
    use JsonbIteratorToken::*;
    assert_eq!(
        toks,
        vec![WJB_BEGIN_ARRAY, WJB_ELEM, WJB_ELEM, WJB_ELEM, WJB_END_ARRAY]
    );
    assert_eq!(strings, vec!["a".to_string()]);
}

/// Build `{"k1":"v1","k2":false}` and round-trip key/value order.
#[test]
fn object_roundtrip() {
    install_seams();
    let ctx = mcx::MemoryContext::new("jsonb_util.test");
    let mcx = ctx.mcx();
    let mut pstate: Option<Box<JsonbParseState>> = None;
    pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_BEGIN_OBJECT, None).unwrap();
    pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_KEY, Some(&jstring("k1"))).unwrap();
    pushJsonbValue(
        mcx, &mut pstate,
        JsonbIteratorToken::WJB_VALUE,
        Some(&jstring("v1")),
    )
    .unwrap();
    pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_KEY, Some(&jstring("k2"))).unwrap();
    pushJsonbValue(
        mcx, &mut pstate,
        JsonbIteratorToken::WJB_VALUE,
        Some(&jbool(false)),
    )
    .unwrap();
    let res = pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_END_OBJECT, None)
        .unwrap()
        .unwrap();

    let bytes = to_jsonb(mcx, &res);

    let mut it = JsonbIteratorInit(mcx, borrow_in(mcx, &bytes[VARHDRSZ..]));
    let mut v = JsonbValue::null();
    let mut keys = Vec::new();
    use JsonbIteratorToken::*;
    loop {
        let t = JsonbIteratorNext(&mut it, &mut v, false).unwrap();
        if t == WJB_DONE {
            break;
        }
        if t == WJB_KEY {
            if let JsonbValueData::String(s) = &v.val {
                keys.push(std::string::String::from_utf8(s.to_vec()).unwrap());
            }
        }
    }
    assert_eq!(keys, vec!["k1".to_string(), "k2".to_string()]);
}

/// A raw scalar (string) round-trips through the scalar pseudo-array path.
#[test]
fn scalar_roundtrip() {
    install_seams();
    let ctx = mcx::MemoryContext::new("jsonb_util.test");
    let mcx = ctx.mcx();
    let v = jstring("hello");
    let bytes = to_jsonb(mcx, &v);
    // Root must be a raw-scalar array.
    let header = container_header(&bytes[VARHDRSZ..]);
    assert!(json_container_is_scalar(header));
    assert!(json_container_is_array(header));

    // Extract the scalar back via iteration with skip_nested.
    let mut it = JsonbIteratorInit(mcx, borrow_in(mcx, &bytes[VARHDRSZ..]));
    let mut tmp = JsonbValue::null();
    use JsonbIteratorToken::*;
    assert_eq!(
        JsonbIteratorNext(&mut it, &mut tmp, true).unwrap(),
        WJB_BEGIN_ARRAY
    );
    let mut elem = JsonbValue::null();
    assert_eq!(JsonbIteratorNext(&mut it, &mut elem, true).unwrap(), WJB_ELEM);
    assert_eq!(elem.typ, jbvType::jbvString);
    if let JsonbValueData::String(s) = &elem.val {
        assert_eq!(s, b"hello");
    }
}

/// Build an object `{k1:v1, k2:false}` on disk and look keys up by binary
/// search through `getKeyJsonValueFromContainer`.
#[test]
fn object_key_lookup() {
    install_seams();
    let ctx = mcx::MemoryContext::new("jsonb_util.test");
    let mcx = ctx.mcx();
    let mut pstate: Option<Box<JsonbParseState>> = None;
    pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_BEGIN_OBJECT, None).unwrap();
    pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_KEY, Some(&jstring("k1"))).unwrap();
    pushJsonbValue(
        mcx, &mut pstate,
        JsonbIteratorToken::WJB_VALUE,
        Some(&jstring("v1")),
    )
    .unwrap();
    pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_KEY, Some(&jstring("k2"))).unwrap();
    pushJsonbValue(
        mcx, &mut pstate,
        JsonbIteratorToken::WJB_VALUE,
        Some(&jbool(false)),
    )
    .unwrap();
    let res = pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_END_OBJECT, None)
        .unwrap()
        .unwrap();
    let bytes = to_jsonb(mcx, &res);
    let container = &bytes[VARHDRSZ..];

    let v1 = getKeyJsonValueFromContainer(container, b"k1")
        .unwrap()
        .unwrap();
    assert_eq!(v1.typ, jbvType::jbvString);
    assert!(matches!(&v1.val, JsonbValueData::String(s) if s == b"v1"));

    let v2 = getKeyJsonValueFromContainer(container, b"k2")
        .unwrap()
        .unwrap();
    assert_eq!(v2.typ, jbvType::jbvBool);
    assert!(matches!(&v2.val, JsonbValueData::Bool(false)));

    assert!(getKeyJsonValueFromContainer(container, b"missing")
        .unwrap()
        .is_none());

    // findJsonbValueFromContainer with JB_FOBJECT routes to the key lookup.
    let found = findJsonbValueFromContainer(container, JB_FOBJECT, &jstring("k1"))
        .unwrap()
        .unwrap();
    assert!(matches!(&found.val, JsonbValueData::String(s) if s == b"v1"));
}

/// Build `["a","b","c"]` and fetch elements by index, and search for a scalar.
#[test]
fn array_index_and_find() {
    install_seams();
    let ctx = mcx::MemoryContext::new("jsonb_util.test");
    let mcx = ctx.mcx();
    let mut pstate: Option<Box<JsonbParseState>> = None;
    pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_BEGIN_ARRAY, None).unwrap();
    for s in ["a", "b", "c"] {
        pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_ELEM, Some(&jstring(s))).unwrap();
    }
    let res = pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_END_ARRAY, None)
        .unwrap()
        .unwrap();
    let bytes = to_jsonb(mcx, &res);
    let container = &bytes[VARHDRSZ..];

    let e1 = getIthJsonbValueFromContainer(container, 1).unwrap().unwrap();
    assert!(matches!(&e1.val, JsonbValueData::String(s) if s == b"b"));
    assert!(getIthJsonbValueFromContainer(container, 3)
        .unwrap()
        .is_none());

    // findJsonbValueFromContainer over the array finds "c".
    assert!(findJsonbValueFromContainer(container, JB_FARRAY, &jstring("c"))
        .unwrap()
        .is_some());
    assert!(findJsonbValueFromContainer(container, JB_FARRAY, &jstring("z"))
        .unwrap()
        .is_none());
}

/// A tiny jsonb literal model used to assemble on-disk bytes for golden tests.
enum J {
    Null,
    Bool(bool),
    Num(i16),
    Str(&'static str),
    Arr(Vec<J>),
    Obj(Vec<(&'static str, J)>),
}

/// Push a `J` literal into the parse state, then materialize the on-disk bytes.
fn build(mcx: mcx::Mcx<'_>, j: &J) -> Vec<u8> {
    fn jvalue<'mcx>(mcx: mcx::Mcx<'mcx>, j: &J) -> JsonbValue<'mcx> {
        match j {
            J::Null => JsonbValue::null(),
            J::Bool(b) => jbool(*b),
            J::Str(s) => jstring(s),
            J::Num(n) => jnumeric(mcx, *n),
            J::Arr(elems) => {
                let mut pstate: Option<Box<JsonbParseState>> = None;
                pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_BEGIN_ARRAY, None).unwrap();
                for e in elems {
                    let ev = jvalue(mcx, e);
                    pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_ELEM, Some(&ev)).unwrap();
                }
                pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_END_ARRAY, None)
                    .unwrap()
                    .unwrap()
            }
            J::Obj(pairs) => {
                let mut pstate: Option<Box<JsonbParseState>> = None;
                pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_BEGIN_OBJECT, None).unwrap();
                for (k, v) in pairs {
                    let kv = jstring(k);
                    let vv = jvalue(mcx, v);
                    pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_KEY, Some(&kv)).unwrap();
                    pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_VALUE, Some(&vv)).unwrap();
                }
                pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_END_OBJECT, None)
                    .unwrap()
                    .unwrap()
            }
        }
    }

    to_jsonb(mcx, &jvalue(mcx, j))
}

/// `compareJsonbContainers` orders arrays by element count, then element-wise.
#[test]
fn compare_containers() {
    install_seams();
    let ctx = mcx::MemoryContext::new("jsonb_util.test");
    let mcx = ctx.mcx();
    let a = build(mcx, &J::Arr(vec![J::Num(1), J::Num(2)]));
    let b = build(mcx, &J::Arr(vec![J::Num(1), J::Num(2)]));
    let c = build(mcx, &J::Arr(vec![J::Num(1), J::Num(2), J::Num(3)]));
    let d = build(mcx, &J::Arr(vec![J::Num(1), J::Num(3)]));

    assert_eq!(
        compareJsonbContainers(mcx, borrow_in(mcx, &a[VARHDRSZ..]), borrow_in(mcx, &b[VARHDRSZ..])).unwrap(),
        0
    );
    // Fewer elements sorts less.
    assert!(compareJsonbContainers(mcx, borrow_in(mcx, &a[VARHDRSZ..]), borrow_in(mcx, &c[VARHDRSZ..])).unwrap() < 0);
    assert!(compareJsonbContainers(mcx, borrow_in(mcx, &c[VARHDRSZ..]), borrow_in(mcx, &a[VARHDRSZ..])).unwrap() > 0);
    // Same count, 2 < 3 numerically.
    assert!(compareJsonbContainers(mcx, borrow_in(mcx, &a[VARHDRSZ..]), borrow_in(mcx, &d[VARHDRSZ..])).unwrap() < 0);
}

/// `JsonbDeepContains`: `{a:1,b:2}` contains `{a:1}` but not `{a:9}`.
#[test]
fn deep_contains_object() {
    install_seams();
    let ctx = mcx::MemoryContext::new("jsonb_util.test");
    let mcx = ctx.mcx();
    let obj = |pairs: &[(&'static str, bool)]| -> Vec<u8> {
        build(mcx, &J::Obj(
            pairs.iter().map(|(k, v)| (*k, J::Bool(*v))).collect(),
        ))
    };

    let big = obj(&[("a", true), ("b", false)]);
    let sub = obj(&[("a", true)]);
    let nope = obj(&[("a", false)]);

    let mut iv = JsonbIteratorInit(mcx, borrow_in(mcx, &big[VARHDRSZ..]));
    let mut ic = JsonbIteratorInit(mcx, borrow_in(mcx, &sub[VARHDRSZ..]));
    assert!(JsonbDeepContains(mcx, &mut iv, &mut ic).unwrap());

    let mut iv = JsonbIteratorInit(mcx, borrow_in(mcx, &big[VARHDRSZ..]));
    let mut ic = JsonbIteratorInit(mcx, borrow_in(mcx, &nope[VARHDRSZ..]));
    assert!(!JsonbDeepContains(mcx, &mut iv, &mut ic).unwrap());

    // A smaller lhs cannot contain a larger rhs.
    let mut iv = JsonbIteratorInit(mcx, borrow_in(mcx, &sub[VARHDRSZ..]));
    let mut ic = JsonbIteratorInit(mcx, borrow_in(mcx, &big[VARHDRSZ..]));
    assert!(!JsonbDeepContains(mcx, &mut iv, &mut ic).unwrap());
}

/// `JsonbDeepContains` over arrays: the lhs array must contain every rhs element.
#[test]
fn deep_contains_array() {
    install_seams();
    let ctx = mcx::MemoryContext::new("jsonb_util.test");
    let mcx = ctx.mcx();
    let arr = |bools: &[bool]| -> Vec<u8> {
        build(mcx, &J::Arr(bools.iter().map(|b| J::Bool(*b)).collect()))
    };

    let big = arr(&[true, false]);
    let sub = arr(&[true]);
    let only_missing = arr(&[true, true, true]); // still all "true", contained

    let mut iv = JsonbIteratorInit(mcx, borrow_in(mcx, &big[VARHDRSZ..]));
    let mut ic = JsonbIteratorInit(mcx, borrow_in(mcx, &sub[VARHDRSZ..]));
    assert!(JsonbDeepContains(mcx, &mut iv, &mut ic).unwrap());

    let mut iv = JsonbIteratorInit(mcx, borrow_in(mcx, &big[VARHDRSZ..]));
    let mut ic = JsonbIteratorInit(mcx, borrow_in(mcx, &only_missing[VARHDRSZ..]));
    assert!(JsonbDeepContains(mcx, &mut iv, &mut ic).unwrap());

    // sub does NOT contain `false`.
    let mut iv = JsonbIteratorInit(mcx, borrow_in(mcx, &sub[VARHDRSZ..]));
    let mut ic = JsonbIteratorInit(mcx, borrow_in(mcx, &big[VARHDRSZ..]));
    assert!(!JsonbDeepContains(mcx, &mut iv, &mut ic).unwrap());
}

/// Golden: nested array self-containment and the raw-scalar/array asymmetry.
#[test]
fn golden_containment() {
    install_seams();
    let ctx = mcx::MemoryContext::new("jsonb_util.test");
    let mcx = ctx.mcx();
    let contains = |val_bytes: &[u8], tmpl_bytes: &[u8]| -> bool {
        let val = borrow_in(mcx, &val_bytes[VARHDRSZ..]);
        let tmpl = borrow_in(mcx, &tmpl_bytes[VARHDRSZ..]);
        let val_is_obj = json_container_is_object(container_header(val));
        let tmpl_is_obj = json_container_is_object(container_header(tmpl));
        if val_is_obj != tmpl_is_obj {
            return false;
        }
        let mut it1 = JsonbIteratorInit(mcx, val);
        let mut it2 = JsonbIteratorInit(mcx, tmpl);
        JsonbDeepContains(mcx, &mut it1, &mut it2).unwrap()
    };

    // Array containment (jsonb.out): [1,2] @> [1,2,2], [1,1,2] @> [1,2,2].
    let a12 = build(mcx, &J::Arr(vec![J::Num(1), J::Num(2)]));
    let a122 = build(mcx, &J::Arr(vec![J::Num(1), J::Num(2), J::Num(2)]));
    let a112 = build(mcx, &J::Arr(vec![J::Num(1), J::Num(1), J::Num(2)]));
    assert!(contains(&a12, &a122));
    assert!(contains(&a112, &a122));
    let aa12 = build(mcx, &J::Arr(vec![J::Arr(vec![J::Num(1), J::Num(2)])]));
    let aa122 = build(mcx, &J::Arr(vec![J::Arr(vec![J::Num(1), J::Num(2), J::Num(2)])]));
    assert!(contains(&aa12, &aa122)); // [[1,2]] @> [[1,2,2]]

    // Scalar / raw-scalar containment.
    let s5 = build(mcx, &J::Num(5));
    let arr5 = build(mcx, &J::Arr(vec![J::Num(5)]));
    assert!(contains(&arr5, &arr5)); // [5] @> [5]
    assert!(contains(&s5, &s5)); // 5 @> 5
    assert!(contains(&arr5, &s5)); // [5] @> 5
    assert!(!contains(&s5, &arr5)); // 5 @> [5] -> f (raw scalar can't contain array)

    // {"tags":["qu"]} is NOT contained in {"name":"Bob","tags":["enim","qui"]}.
    let bob = build(mcx, &J::Obj(vec![
        ("name", J::Str("Bob")),
        ("tags", J::Arr(vec![J::Str("enim"), J::Str("qui")])),
    ]));
    let tags_qu = build(mcx, &J::Obj(vec![("tags", J::Arr(vec![J::Str("qu")]))]));
    assert!(!contains(&bob, &tags_qu));
}

/// Golden: the documented btree ordering (json.sgml):
///   Object > Array > Boolean > Number > String > null
/// plus n-pairs / n-elements rules and the empty-array-vs-null exception.
#[test]
fn golden_btree_ordering() {
    install_seams();
    let ctx = mcx::MemoryContext::new("jsonb_util.test");
    let mcx = ctx.mcx();
    let cmp = |a: &[u8], b: &[u8]| -> i32 {
        compareJsonbContainers(mcx, borrow_in(mcx, &a[VARHDRSZ..]), borrow_in(mcx, &b[VARHDRSZ..])).unwrap()
    };

    let obj = build(mcx, &J::Obj(vec![("k", J::Num(1))]));
    let arr = build(mcx, &J::Arr(vec![J::Num(1), J::Num(2)])); // non-empty
    let boolean = build(mcx, &J::Bool(true));
    let num = build(mcx, &J::Num(1));
    let strv = build(mcx, &J::Str("a"));
    let nul = build(mcx, &J::Null);

    assert!(cmp(&obj, &arr) > 0, "Object > Array");
    assert!(cmp(&arr, &boolean) > 0, "Array > Boolean");
    assert!(cmp(&boolean, &num) > 0, "Boolean > Number");
    assert!(cmp(&num, &strv) > 0, "Number > String");
    assert!(cmp(&strv, &nul) > 0, "String > null");

    // Object with n pairs > object with n-1 pairs.
    let o1 = build(mcx, &J::Obj(vec![("a", J::Num(1))]));
    let o2 = build(mcx, &J::Obj(vec![("a", J::Num(1)), ("b", J::Num(2))]));
    assert!(cmp(&o2, &o1) > 0, "object n > object n-1");

    // Array with n elements > array with n-1 elements.
    let a1 = build(mcx, &J::Arr(vec![J::Num(1)]));
    let a2 = build(mcx, &J::Arr(vec![J::Num(1), J::Num(2)]));
    assert!(cmp(&a2, &a1) > 0, "array n > array n-1");

    // Equal numeric documents compare 0.
    let e1 = build(mcx, &J::Arr(vec![J::Num(3), J::Num(4)]));
    let e2 = build(mcx, &J::Arr(vec![J::Num(3), J::Num(4)]));
    assert_eq!(cmp(&e1, &e2), 0, "equal numeric docs compare 0");

    // Historical exception: an empty top-level array sorts LESS than null.
    let empty_arr = build(mcx, &J::Arr(vec![]));
    assert!(cmp(&empty_arr, &nul) < 0, "empty top-level array < null");
}

/// Hashing: known scalar constants and equal-scalar hash equality.
#[test]
fn hash_scalar_values() {
    install_seams();
    let ctx = mcx::MemoryContext::new("jsonb_util.test");
    let mcx = ctx.mcx();
    // null contributes 0x01.
    let mut h: u32 = 0;
    JsonbHashScalarValue(&JsonbValue::null(), &mut h).unwrap();
    assert_eq!(h, 0x01); // rotate_left(0,1) ^ 0x01

    // bool true contributes 0x02, false 0x04.
    let mut ht: u32 = 0;
    JsonbHashScalarValue(&jbool(true), &mut ht).unwrap();
    assert_eq!(ht, 0x02);
    let mut hf: u32 = 0;
    JsonbHashScalarValue(&jbool(false), &mut hf).unwrap();
    assert_eq!(hf, 0x04);

    // Equal strings hash equally.
    let mut h1: u32 = 0;
    let mut h2: u32 = 0;
    JsonbHashScalarValue(&jstring("hello"), &mut h1).unwrap();
    JsonbHashScalarValue(&jstring("hello"), &mut h2).unwrap();
    assert_eq!(h1, h2);

    // Equal numerics hash equally.
    let mut hn1: u32 = 0;
    let mut hn2: u32 = 0;
    JsonbHashScalarValue(&jnumeric(mcx, 7), &mut hn1).unwrap();
    JsonbHashScalarValue(&jnumeric(mcx, 7), &mut hn2).unwrap();
    assert_eq!(hn1, hn2);

    // Extended hash, zero seed, bool path matches the 32-bit constants.
    let mut he: u64 = 0;
    JsonbHashScalarValueExtended(&jbool(true), &mut he, 0).unwrap();
    assert_eq!(he, 0x02);
}

/// Numeric scalar round-trip + equality/compare through the numeric seams.
#[test]
fn numeric_scalar_roundtrip() {
    install_seams();
    let ctx = mcx::MemoryContext::new("jsonb_util.test");
    let mcx = ctx.mcx();
    let v = jnumeric(mcx, 42);

    // Equal numerics compare/equal as equal.
    assert!(equalsJsonbScalarValue(&v, &jnumeric(mcx, 42)).unwrap());
    assert_eq!(compareJsonbScalarValue(&v, &jnumeric(mcx, 42)).unwrap(), 0);

    // Serialize a numeric scalar and read it back identically.
    let bytes = to_jsonb(mcx, &v);
    let mut it = JsonbIteratorInit(mcx, borrow_in(mcx, &bytes[VARHDRSZ..]));
    let mut tmp = JsonbValue::null();
    use JsonbIteratorToken::*;
    assert_eq!(
        JsonbIteratorNext(&mut it, &mut tmp, true).unwrap(),
        WJB_BEGIN_ARRAY
    );
    let mut elem = JsonbValue::null();
    assert_eq!(JsonbIteratorNext(&mut it, &mut elem, true).unwrap(), WJB_ELEM);
    assert_eq!(elem.typ, jbvType::jbvNumeric);
    assert_eq!(jsonb_numeric_bytes(&elem), build_numeric_small(42).as_slice());
}

/// Duplicate keys with `unique_keys` raises the duplicate-key SQLSTATE.
#[test]
fn duplicate_key_errors() {
    install_seams();
    let ctx = mcx::MemoryContext::new("jsonb_util.test");
    let mcx = ctx.mcx();
    let mut pstate: Option<Box<JsonbParseState>> = None;
    pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_BEGIN_OBJECT, None).unwrap();
    pstate.as_mut().unwrap().unique_keys = true;
    pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_KEY, Some(&jstring("k"))).unwrap();
    pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_VALUE, Some(&jbool(true))).unwrap();
    pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_KEY, Some(&jstring("k"))).unwrap();
    pushJsonbValue(
        mcx, &mut pstate,
        JsonbIteratorToken::WJB_VALUE,
        Some(&jbool(false)),
    )
    .unwrap();
    let err = pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_END_OBJECT, None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE);
}

/// Non-string scalar arms (bool/null) are collation-independent and compare
/// without touching the collation seam.
#[test]
fn compare_nonstring_scalars() {
    install_seams();
    assert_eq!(compareJsonbScalarValue(&jnull(), &jnull()).unwrap(), 0);
    assert_eq!(
        compareJsonbScalarValue(&jbool(true), &jbool(false)).unwrap(),
        1
    );
    assert_eq!(
        compareJsonbScalarValue(&jbool(false), &jbool(true)).unwrap(),
        -1
    );
    assert_eq!(
        compareJsonbScalarValue(&jbool(true), &jbool(true)).unwrap(),
        0
    );
}

/// The jbvString arm routes through the `varstr_cmp_collation` seam.
#[test]
fn compare_string_scalar_uses_collation_seam() {
    install_seams();
    // Installed seam is a C-collation byte compare; "ab" < "ac".
    assert!(compareJsonbScalarValue(&jstring("ab"), &jstring("ac")).unwrap() < 0);
    assert!(compareJsonbScalarValue(&jstring("ac"), &jstring("ab")).unwrap() > 0);
    assert_eq!(
        compareJsonbScalarValue(&jstring("ab"), &jstring("ab")).unwrap(),
        0
    );
}

// ---------------------------------------------------------------------------
// MemoryContext charge gates.
//
// `convertToJsonb` allocates its working buffer (the on-disk serialization
// `StringInfo` analog) and the returned varlena in the caller's `Mcx`.  The
// `PgVec` releases its charge on drop, so dropping the result returns the
// owning context's counter to zero on every path.
// ---------------------------------------------------------------------------

/// Success path: the result is charged to the caller's context while alive, and
/// dropping it returns the counter to zero.
#[test]
fn charge_gate_convert_success_path_released() {
    install_seams();
    let ctx = mcx::MemoryContext::new("charge-gate-ok");
    let mcx = ctx.mcx();
    // A non-scalar value (object), so it goes through convertToJsonb directly.
    let v = {
        let mut pstate: Option<Box<JsonbParseState>> = None;
        pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_BEGIN_OBJECT, None).unwrap();
        pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_KEY, Some(&jstring("k"))).unwrap();
        pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_VALUE, Some(&jstring("v"))).unwrap();
        pushJsonbValue(mcx, &mut pstate, JsonbIteratorToken::WJB_END_OBJECT, None)
            .unwrap()
            .unwrap()
    };

    {
        let buf = JsonbValueToJsonb(mcx, &v).unwrap();
        assert!(ctx.used() > 0, "the result is charged while alive");
        assert!(buf.len() > 0);
    }
    assert_eq!(ctx.used(), 0, "no charge may leak after the result is dropped");
}

/// Error path: a nested `jbvBinary` element inside an array hits the
/// "unknown type of jsonb container to convert" arm; the partially-built buffer
/// is dropped, leaving the counter at zero.
#[test]
fn charge_gate_convert_error_path_released() {
    install_seams();
    let ctx = mcx::MemoryContext::new("charge-gate-err");
    let mcx = ctx.mcx();
    // [ <jbvBinary> ] -- the binary element is neither scalar nor a fresh
    // array/object value, so converting it errors deterministically.
    let mut elems = ::mcx::vec_with_capacity_in(mcx, 1).unwrap();
    elems.push(JsonbValue {
        typ: jbvType::jbvBinary,
        val: JsonbValueData::Binary {
            len: 0,
            data: &[],
            offset: 0,
        },
    });
    let bad = JsonbValue {
        typ: jbvType::jbvArray,
        val: JsonbValueData::Array {
            elems,
            raw_scalar: false,
        },
    };

    let err = JsonbValueToJsonb(mcx, &bad).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INTERNAL_ERROR);
    assert_eq!(ctx.used(), 0, "no charge may leak after an error teardown");
}

/// The public `convertToJsonb` entry point (used via `JsonbValueToJsonb`)
/// round-trips a raw scalar through its serialization and back.
#[test]
fn convert_public_roundtrips_clean() {
    install_seams();
    let ctx = mcx::MemoryContext::new("jsonb_util.test");
    let mcx = ctx.mcx();
    let v = jstring("hello"); // raw scalar -> scalar pseudo-array -> convertToJsonb
    let bytes = to_jsonb(mcx, &v);
    let mut it = JsonbIteratorInit(mcx, borrow_in(mcx, &bytes[VARHDRSZ..]));
    let mut tmp = JsonbValue::null();
    use JsonbIteratorToken::*;
    assert_eq!(
        JsonbIteratorNext(&mut it, &mut tmp, true).unwrap(),
        WJB_BEGIN_ARRAY
    );
    let mut elem = JsonbValue::null();
    assert_eq!(JsonbIteratorNext(&mut it, &mut elem, true).unwrap(), WJB_ELEM);
    assert!(matches!(&elem.val, JsonbValueData::String(s) if s == b"hello"));
}
