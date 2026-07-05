use std::collections::HashMap;

use ::adt_tsvector_core::layout::{ts_compare_string, TsVecBuilder, WordEntryPos};
use ::mcx::MemoryContext;

use crate::{parse_weight, ts_accum};

fn wep(pos: u16, weight: u16) -> WordEntryPos {
    pos | (weight << 14)
}

fn tsvec(words: &[(&str, &[WordEntryPos])]) -> Vec<u8> {
    let ctx = MemoryContext::new("ts-stat-test");
    let mcx = ctx.mcx();
    let mut b = TsVecBuilder::with_capacity(mcx, words.len(), 64).unwrap();
    for (w, p) in words {
        b.push(w.as_bytes(), p).unwrap();
    }
    let img = b.finish(mcx).unwrap().to_vec();
    img
}

#[test]
fn parse_weight_maps_abcd_and_ignores_others() {
    assert_eq!(parse_weight(b"a"), 1 << 3);
    assert_eq!(parse_weight(b"Bd"), (1 << 2) | 1);
    assert_eq!(parse_weight(b"xyz"), 0);
    assert_eq!(parse_weight(b"AbCd"), 0b1111);
}

#[test]
fn ts_accum_counts_docs_and_entries() {
    let img1 = tsvec(&[("cat", &[wep(1, 0), wep(5, 0)]), ("dog", &[])]);
    let img2 = tsvec(&[("cat", &[wep(2, 0)])]);
    let mut acc: HashMap<Vec<u8>, (i32, i32)> = HashMap::new();
    ts_accum(&mut acc, 0, &img1);
    ts_accum(&mut acc, 0, &img2);
    assert_eq!(acc[b"cat".as_slice()], (2, 3));
    assert_eq!(acc[b"dog".as_slice()], (1, 1));
}

#[test]
fn ts_accum_weight_filter_drops_posless_and_unweighted() {
    // weight A only: entries without positions contribute 0 (skipped);
    // positions carrying other weights are not counted.
    let img = tsvec(&[
        ("apos", &[wep(1, 3), wep(2, 0)]),
        ("bare", &[]),
        ("dpos", &[wep(1, 0)]),
    ]);
    let mut acc: HashMap<Vec<u8>, (i32, i32)> = HashMap::new();
    ts_accum(&mut acc, 1 << 3, &img);
    assert_eq!(acc[b"apos".as_slice()], (1, 1));
    assert!(!acc.contains_key(b"bare".as_slice()));
    assert!(!acc.contains_key(b"dpos".as_slice()));
}

#[test]
fn output_order_is_descending_tscompare() {
    // The C StatEntry tree walks greater-first (greater keys go left).
    let mut rows = vec![
        (b"a".to_vec(), 1, 1),
        (b"ab".to_vec(), 1, 1),
        (b"b".to_vec(), 1, 1),
    ];
    rows.sort_unstable_by(|a, b| ts_compare_string(&b.0, &a.0, false).cmp(&0));
    let words: Vec<&[u8]> = rows.iter().map(|r| r.0.as_slice()).collect();
    assert_eq!(words, vec![b"b".as_slice(), b"ab".as_slice(), b"a".as_slice()]);
}
