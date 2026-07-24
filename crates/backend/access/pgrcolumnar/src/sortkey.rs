//! load-r2 L3: fixed-width memcmp sort keys for the parallel load-sort
//! pipeline.
//!
//! `encode_sort_key` maps one row's presort/cluster key datums to a byte
//! string whose unsigned lexicographic (memcmp) order equals the
//! `CbIngestSort` comparator order (signed-int order per kind — the
//! sign-flipped big-endian trick). The identity chain this preserves:
//! memcmp key order == datum comparator order == the frozen-bank GNU
//! `LC_ALL=C` recipe order.
//!
//! GL-LOADDET-1 CORRECTION: this header used to claim "zero datum ties" as
//! part of that measured chain. It is FALSE on the analytics key set, and it
//! was load-bearing for the wrong reason — a tie-free key pins the output
//! permutation all by itself, so every byte-identity gate built on a tie-free
//! fixture passed while the load was in fact non-deterministic on tied keys.
//! Key ties are ordinary; what makes them safe is the deterministic run
//! partition + input-major merge order + arrival-order batch sort
//! (`PGRUST_PARALLEL_COPY_SORT_DETERMINISTIC`), NOT their absence.
//!
//! Int-class kinds only (fixed width). `TextC` keys have no fixed-width
//! encoding here — the parallel load-sort refuses them at admission
//! (serial presort handles them through the tuplesort path).

use ::datum::Datum;
pub use ::tuplesort_seams::CbSortKeyKind;

/// Encoded byte width of one key column; None for variable-width kinds.
pub fn key_col_width(kind: CbSortKeyKind) -> Option<usize> {
    match kind {
        CbSortKeyKind::Int16 => Some(2),
        CbSortKeyKind::Int32 => Some(4),
        CbSortKeyKind::Int64 => Some(8),
        CbSortKeyKind::TextC => None,
    }
}

/// Total fixed key width for a key set; None if any column is variable.
pub fn fixed_key_width(keys: &[(u16, CbSortKeyKind)]) -> Option<usize> {
    keys.iter().map(|&(_, k)| key_col_width(k)).sum()
}

/// Append the row's memcmp sort key to `out`.
///
/// Caller guarantees non-null key datums (pgrcolumnar refuses NULLs before any
/// row reaches a sorter) and int-class kinds (`fixed_key_width` is Some).
#[inline]
pub fn encode_sort_key(keys: &[(u16, CbSortKeyKind)], values: &[Datum], out: &mut Vec<u8>) {
    for &(c, kind) in keys {
        let c = c as usize;
        match kind {
            CbSortKeyKind::Int16 => {
                out.extend_from_slice(&((values[c].as_i16() as u16) ^ 0x8000).to_be_bytes())
            }
            CbSortKeyKind::Int32 => {
                out.extend_from_slice(&((values[c].as_i32() as u32) ^ 0x8000_0000).to_be_bytes())
            }
            CbSortKeyKind::Int64 => out.extend_from_slice(
                &((values[c].as_i64() as u64) ^ 0x8000_0000_0000_0000).to_be_bytes(),
            ),
            CbSortKeyKind::TextC => {
                unreachable!("TextC keys are refused at parallel load-sort admission")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc1(kind: CbSortKeyKind, v: Datum) -> Vec<u8> {
        let mut out = Vec::new();
        encode_sort_key(&[(0, kind)], &[v], &mut out);
        out
    }

    // memcmp order of the encoding == signed order of the value, per kind,
    // across the full signed range including the extremes.
    #[test]
    fn single_key_order_matches_signed_order() {
        let i16s: Vec<i16> = vec![i16::MIN, i16::MIN + 1, -1, 0, 1, 42, i16::MAX - 1, i16::MAX];
        for a in &i16s {
            for b in &i16s {
                assert_eq!(
                    enc1(CbSortKeyKind::Int16, Datum::from_i16(*a))
                        .cmp(&enc1(CbSortKeyKind::Int16, Datum::from_i16(*b))),
                    a.cmp(b),
                    "i16 {a} vs {b}"
                );
            }
        }
        let i32s: Vec<i32> = vec![i32::MIN, i32::MIN + 1, -7, 0, 7, i32::MAX - 1, i32::MAX];
        for a in &i32s {
            for b in &i32s {
                assert_eq!(
                    enc1(CbSortKeyKind::Int32, Datum::from_i32(*a))
                        .cmp(&enc1(CbSortKeyKind::Int32, Datum::from_i32(*b))),
                    a.cmp(b),
                    "i32 {a} vs {b}"
                );
            }
        }
        let i64s: Vec<i64> =
            vec![i64::MIN, i64::MIN + 1, -2461439046089301801, -1, 0, 1, i64::MAX - 1, i64::MAX];
        for a in &i64s {
            for b in &i64s {
                assert_eq!(
                    enc1(CbSortKeyKind::Int64, Datum::from_i64(*a))
                        .cmp(&enc1(CbSortKeyKind::Int64, Datum::from_i64(*b))),
                    a.cmp(b),
                    "i64 {a} vs {b}"
                );
            }
        }
    }

    // Multi-key: memcmp of the concatenated encoding == lexicographic tuple
    // order (the hits 5-key shape: i32, i32, i64, i64, i64).
    #[test]
    fn multi_key_order_is_lexicographic() {
        let keys = [
            (0u16, CbSortKeyKind::Int32),
            (1, CbSortKeyKind::Int32),
            (2, CbSortKeyKind::Int64),
            (3, CbSortKeyKind::Int64),
            (4, CbSortKeyKind::Int64),
        ];
        assert_eq!(fixed_key_width(&keys), Some(32));
        // Deterministic pseudo-random tuples incl. sign boundaries.
        let mut x: u64 = 0x9e3779b97f4a7c15;
        let mut step = || {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            x
        };
        let mut tuples: Vec<(i32, i32, i64, i64, i64)> = (0..512)
            .map(|_| {
                (
                    (step() as i32) % 1000,
                    step() as i32,
                    step() as i64,
                    (step() as i64) % 3,
                    step() as i64,
                )
            })
            .collect();
        tuples.push((i32::MIN, i32::MAX, i64::MIN, i64::MAX, 0));
        tuples.push((0, 0, 0, 0, 0));

        let enc = |t: &(i32, i32, i64, i64, i64)| {
            let vals = [
                Datum::from_i32(t.0),
                Datum::from_i32(t.1),
                Datum::from_i64(t.2),
                Datum::from_i64(t.3),
                Datum::from_i64(t.4),
            ];
            let mut out = Vec::with_capacity(32);
            encode_sort_key(&keys, &vals, &mut out);
            assert_eq!(out.len(), 32);
            out
        };

        let mut by_key: Vec<_> = tuples.clone();
        by_key.sort_by(|a, b| enc(a).cmp(&enc(b)));
        let mut by_tuple = tuples;
        by_tuple.sort();
        assert_eq!(by_key, by_tuple);
    }

    #[test]
    fn text_keys_have_no_fixed_width() {
        assert_eq!(key_col_width(CbSortKeyKind::TextC), None);
        assert_eq!(
            fixed_key_width(&[(0, CbSortKeyKind::Int64), (1, CbSortKeyKind::TextC)]),
            None
        );
    }
}
