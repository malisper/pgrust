//! [sqe-bpchar] InBytes encoding + eval law (unit grain; the server-side
//! pad law is gated end-to-end by scripts/sqe-bpchar-pad-e2e.sh against
//! real PostgreSQL 18.6).

use sqe::ir::{decode_in_needles, encode_in_needles, VarOp, VarPredTerm};
use sqe::typmeta::{bpchar_declared_chars, TypMeta, COLLATION_C};

fn term(needles: Vec<Vec<u8>>) -> VarPredTerm {
    let ty = TypMeta { oid: sqe::typmeta::oids::BPCHAR, ..TypMeta::TEXT_C };
    VarPredTerm::new(3, VarOp::InBytes, encode_in_needles(needles), ty)
}

#[test]
fn encode_is_canonical_sorted_deduped() {
    // List order and duplicates never change the image — the
    // fingerprint identity law (two spellings share a verdict plane).
    let a = encode_in_needles(vec![b"ab ".to_vec(), b"a  ".to_vec(), b"ab ".to_vec()]);
    let b = encode_in_needles(vec![b"a  ".to_vec(), b"ab ".to_vec()]);
    assert_eq!(a, b);
    let imgs: Vec<&[u8]> = decode_in_needles(&a).collect();
    assert_eq!(imgs, vec![b"a  ".as_slice(), b"ab ".as_slice()]);
}

#[test]
fn decode_roundtrips_empty_and_binary() {
    // Empty image (bpchar '' pads to blanks upstream, but the codec
    // itself must carry any byte string), sub-space bytes, multibyte.
    let needles = vec![Vec::new(), b"a\x01 ".to_vec(), "é ".as_bytes().to_vec()];
    let enc = encode_in_needles(needles.clone());
    let mut back: Vec<Vec<u8>> = decode_in_needles(&enc).map(|s| s.to_vec()).collect();
    back.sort();
    let mut want = needles;
    want.sort();
    want.dedup();
    assert_eq!(back, want);
}

#[test]
fn eval_is_exact_byte_membership() {
    let t = term(vec![b"ab ".to_vec(), b"zz ".to_vec()]);
    assert!(t.eval(b"ab "));
    assert!(t.eval(b"zz "));
    // The pad law's whole point: the UNPADDED spelling is NOT a member —
    // lowering must pad before authoring, never the eval.
    assert!(!t.eval(b"ab"));
    assert!(!t.eval(b"ab  "));
    assert!(!t.eval(b""));
    // 3VL: NULL never passes.
    assert!(!t.eval_v(b"ab ", false));
}

#[test]
fn eval_never_matches_partial_prefixes() {
    // A needle must match whole — no prefix confusion across the
    // length-prefixed concatenation ("ab" + "cd" never matches "abcd").
    let t = term(vec![b"ab".to_vec(), b"cd".to_vec()]);
    assert!(!t.eval(b"abcd"));
    assert!(t.eval(b"ab"));
    assert!(t.eval(b"cd"));
}

#[test]
fn declared_chars_law() {
    // atttypmod = n + VARHDRSZ; −1 (bare) and sub-minimum stay None.
    assert_eq!(bpchar_declared_chars(-1), None);
    assert_eq!(bpchar_declared_chars(4), None);
    assert_eq!(bpchar_declared_chars(5), Some(1));
    assert_eq!(bpchar_declared_chars(29), Some(25));
}
