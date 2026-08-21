//! Granule-grain predicate byte-mask + masked-fold kernels: monomorphic
//! per (CmpOp × word face), branchless bodies over contiguous datum
//! windows — the autovectorizable form of the term-major filter (same
//! conjunct order, same 3VL; a mask byte is exactly 0 or 1). Word faces
//! only: byte-ref faces (Fixed/Varlena/PackedNumeric) and float keys stay
//! on the caller's scalar residual, which guards `word_key` behind
//! validity (raw-parts safety on NULL slots).

use crate::bank::Face;
use crate::ir::{CmpOp, PredTerm};
use crate::stencils::fused_filter_agg::ColAcc;

#[inline(always)]
fn lanes(
    d: &[u64],
    mask: &mut [u8],
    first: bool,
    ext: impl Fn(u64) -> i64 + Copy,
    cmp: impl Fn(i64) -> bool + Copy,
) {
    if first {
        for (m, &x) in mask.iter_mut().zip(d) {
            *m = cmp(ext(x)) as u8;
        }
    } else {
        for (m, &x) in mask.iter_mut().zip(d) {
            *m &= cmp(ext(x)) as u8;
        }
    }
}

/// AND term `t` into `mask` over the datum window `d` (first term writes).
/// Returns false when the face has no vector embed — the caller runs its
/// guarded scalar residual instead. Values are compared UNCONDITIONALLY
/// (NULL slots carry initialized garbage words; integer extension of
/// garbage is well-defined) — the caller ANDs the validity plane after.
pub fn mask_term(t: &PredTerm, face: Face, d: &[u64], mask: &mut [u8], first: bool) -> bool {
    debug_assert_eq!(d.len(), mask.len());
    macro_rules! ops {
        ($ext:expr) => {{
            let (lo, hi) = (t.lo, t.hi);
            match t.op {
                CmpOp::Eq => lanes(d, mask, first, $ext, move |v| v == lo),
                CmpOp::Ne => lanes(d, mask, first, $ext, move |v| v != lo),
                // One-sided ranges (the common `>`/`<=` lowering) halve
                // the compare work.
                CmpOp::Between if lo == i64::MIN => lanes(d, mask, first, $ext, move |v| v <= hi),
                CmpOp::Between if hi == i64::MAX => lanes(d, mask, first, $ext, move |v| v >= lo),
                CmpOp::Between => lanes(d, mask, first, $ext, move |v| v >= lo && v <= hi),
                CmpOp::In2 => lanes(d, mask, first, $ext, move |v| v == lo || v == hi),
            }
        }};
    }
    match face {
        Face::SignedWord(1) => ops!(|x: u64| x as i8 as i64),
        Face::SignedWord(2) => ops!(|x: u64| x as i16 as i64),
        Face::SignedWord(4) => ops!(|x: u64| x as i32 as i64),
        Face::SignedWord(8) => ops!(|x: u64| x as i64),
        Face::UnsignedWord(1) => ops!(|x: u64| x as u8 as i64),
        Face::UnsignedWord(2) => ops!(|x: u64| x as u16 as i64),
        Face::UnsignedWord(4) => ops!(|x: u64| x as u32 as i64),
        Face::Bool => ops!(|x: u64| (x != 0) as i64),
        _ => return false,
    }
    true
}

/// Survivor count of a 0/1 mask.
#[inline]
pub fn mask_count(mask: &[u8]) -> usize {
    mask.iter().map(|&m| m as usize).sum()
}

/// Stable selection build from a 0/1 mask (rows `rlo + i`), branchless
/// write-ahead compaction — bit-identical to filtering term-major.
pub fn mask_sel(mask: &[u8], rlo: usize, sel: &mut Vec<u16>) {
    sel.clear();
    sel.resize(mask.len(), 0);
    let mut w = 0usize;
    for (i, &m) in mask.iter().enumerate() {
        sel[w] = (rlo + i) as u16;
        w += m as usize;
    }
    sel.truncate(w);
}

/// Faces the masked/dense folds serve: |word_key| < 2^32, so an i64
/// partial over one granule (rows <= 2^16, the u16 selection law) cannot
/// overflow — the i128 widening at merge is exact, and every reduction
/// below is associative+commutative, so the answer is byte-identical to
/// the row-order selection fold.
#[inline(always)]
pub fn foldable(face: Face) -> bool {
    matches!(
        face,
        Face::SignedWord(1)
            | Face::SignedWord(2)
            | Face::SignedWord(4)
            | Face::UnsignedWord(1)
            | Face::UnsignedWord(2)
            | Face::UnsignedWord(4)
            | Face::Bool
    )
}

#[inline(always)]
fn fold_run(
    d: &[u64],
    mask: Option<&[u8]>,
    acc: &mut ColAcc,
    ext: impl Fn(u64) -> i64 + Copy,
) {
    let mut n = 0u64;
    let mut sum = 0i64;
    let (mut mn, mut mx, mut ba, mut bo) = (i64::MAX, i64::MIN, -1i64, 0i64);
    match mask {
        None => {
            for &x in d {
                let v = ext(x);
                sum += v;
                mn = mn.min(v);
                mx = mx.max(v);
                ba &= v;
                bo |= v;
            }
            n = d.len() as u64;
        }
        Some(mask) => {
            for (&m, &x) in mask.iter().zip(d) {
                let v = ext(x);
                let keep = m != 0;
                n += keep as u64;
                sum += if keep { v } else { 0 };
                mn = mn.min(if keep { v } else { i64::MAX });
                mx = mx.max(if keep { v } else { i64::MIN });
                ba &= if keep { v } else { -1 };
                bo |= if keep { v } else { 0 };
            }
        }
    }
    if n > 0 {
        acc.n += n;
        acc.sum += sum as i128;
        acc.min = Some(acc.min.map_or(mn, |a| a.min(mn)));
        acc.max = Some(acc.max.map_or(mx, |a| a.max(mx)));
        acc.band = Some(acc.band.map_or(ba, |a| a & ba));
        acc.bor = Some(acc.bor.map_or(bo, |a| a | bo));
    }
}

/// Branchless fold of the mask's survivors into `acc` (Σx² excluded —
/// callers with a variance leg keep the selection fold). `mask = None`
/// folds every row (the zone-proven all-pass arm).
pub(crate) fn fold_word(face: Face, d: &[u64], mask: Option<&[u8]>, acc: &mut ColAcc) {
    debug_assert!(foldable(face));
    debug_assert!(d.len() <= 1 << 16);
    match face {
        Face::SignedWord(1) => fold_run(d, mask, acc, |x: u64| x as i8 as i64),
        Face::SignedWord(2) => fold_run(d, mask, acc, |x: u64| x as i16 as i64),
        Face::SignedWord(4) => fold_run(d, mask, acc, |x: u64| x as i32 as i64),
        Face::UnsignedWord(1) => fold_run(d, mask, acc, |x: u64| x as u8 as i64),
        Face::UnsignedWord(2) => fold_run(d, mask, acc, |x: u64| x as u16 as i64),
        Face::UnsignedWord(4) => fold_run(d, mask, acc, |x: u64| x as u32 as i64),
        Face::Bool => fold_run(d, mask, acc, |x: u64| (x != 0) as i64),
        _ => unreachable!("fold_word: gated by foldable()"),
    }
}
