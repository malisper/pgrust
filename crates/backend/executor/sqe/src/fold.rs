//! The single scatter-fold law (P1-1 typed currency, the lx4 adopt-target:
//! lanev4 lx4_kernel/src/interp.rs `scatter_cell_fold` + lx4_pipe
//! grouped.rs:1370-1431). ONE home for per-row aggregate cell math —
//! `fold_row(op, cell, word, valid)` — shared by every null-threaded
//! stencil body and consulted (as spec, not as code) by the rig oracle's
//! independent control flow. No fold math is ever written twice.
//!
//! 3VL law: a NULL row folds NOTHING (the strict-transition rule);
//! Min/Max answers are Option (`cell.valid == 0` = no row folded = SQL
//! NULL); Sum/Avg carry {sum, count} so SUM-over-empty→NULL and
//! AVG = sum / count(nonnull) are decidable at ANSWER time, never at
//! fold time. `valid: bool` is the 3VL entry point: NOT NULL lanes pass
//! a hoisted `true` (constant-folds to the PoC loop — law 11: the common
//! case costs nothing).
//!
//! Face key laws (the type-face closure, census 2026-08-17): every
//! admitted non-varlena face maps its datum into an ORDER-PRESERVING i64
//! `word` before the fold, so the cell math stays one law:
//!   - signed words: sign-extend (`stencils::sx`);
//!   - unsigned words (oid/xid/"char" class, width <= 4): zero-extend —
//!     nonneg i64, unsigned order preserved;
//!   - float4/float8: `f64_key` (PG float_cmp semantics: NaN greatest,
//!     all NaNs equal, -0 == 0), inverted by `f64_from_key` at answer;
//!   - bool: the 0/1 datum;
//!   - Fixed{16} (uuid): NOT word-foldable — memcmp byte folds
//!     (`fold_min_bytes`/`fold_max_bytes`).

/// The closed physical fold-op set (the lx4 `AggFoldOp` vocabulary).
/// CountStar/CountDistinct/MinBytes carry no cell fold (they ride row
/// counts, element sets, and byte folds respectively).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggFoldOp {
    /// COUNT(col): count non-null rows.
    CountCol,
    Sum,
    Min,
    Max,
    /// The variance decomposition lane {count, sum, sum-of-squares}:
    /// `a` = Σx (exact i128), `a2` = Σx² (exact i128), `b` = n. The
    /// finisher (PG's closed form N·Σx² − (Σx)²) runs at ANSWER time —
    /// never in the fold. Overflow law: Σx² stays exact in i128 iff the
    /// input domain is witnessed |x| <= 2^31−1 (int2/int4 by type; int8
    /// only under an exact stats witness — the planner's admission gate);
    /// then Σx² <= n·2^62 < 2^127 for any n < 2^64 rows.
    SumSq,
    /// Bitwise AND across non-null words (identity element !0 carried as
    /// the `valid` flag, never a sentinel: all-NULL answers NULL).
    BitAnd,
    /// Bitwise OR across non-null words (identity element 0 via `valid`).
    BitOr,
}

/// The uniform accumulator cell: `a` = sum (i128 exact) or the
/// min/max/bit word; `a2` = the SumSq lane's Σx² (0 for every other op);
/// `b` = the non-null fold count; `valid` != 0 iff any row folded (the
/// Min/Max/Bit Option-ness, state not debug).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AccumCell {
    pub a: i128,
    pub a2: i128,
    pub b: i64,
    pub valid: u8,
}

/// THE per-row scatter-fold law (verbatim lx4 semantics). A NULL row
/// folds nothing; Sum/SumSq are exact i128 in-cell math; Min/Max compare
/// the order-preserving i64 word and record `valid`; BitAnd/BitOr fold
/// the raw word bits under the same `valid` law.
#[inline(always)]
pub fn scatter_cell_fold(op: AggFoldOp, cell: &mut AccumCell, word: i64, valid: bool) {
    if !valid {
        return;
    }
    match op {
        AggFoldOp::CountCol => cell.b += 1,
        AggFoldOp::Sum => {
            cell.a += word as i128;
            cell.b += 1;
        }
        AggFoldOp::SumSq => {
            let w = word as i128;
            cell.a += w;
            cell.a2 += w * w;
            cell.b += 1;
        }
        AggFoldOp::Min => {
            if cell.valid == 0 || word < cell.a as i64 {
                cell.a = word as i128;
            }
            cell.valid = 1;
            cell.b += 1;
        }
        AggFoldOp::Max => {
            if cell.valid == 0 || word > cell.a as i64 {
                cell.a = word as i128;
            }
            cell.valid = 1;
            cell.b += 1;
        }
        AggFoldOp::BitAnd => {
            cell.a = if cell.valid == 0 { word } else { cell.a as i64 & word } as i128;
            cell.valid = 1;
            cell.b += 1;
        }
        AggFoldOp::BitOr => {
            cell.a = if cell.valid == 0 { word } else { cell.a as i64 | word } as i128;
            cell.valid = 1;
            cell.b += 1;
        }
    }
}

/// The fold-op of an IR aggregate (None = the leg carries no word-cell
/// fold: CountStar rides row counts, CountDistinct the element sets,
/// MinBytes/EmitMatches the byte stores). The four variance/stddev ops
/// share ONE decomposition lane (SumSq); their finishers differ only at
/// the answer seam.
pub fn fold_op_of(op: crate::ir::AggOp) -> Option<AggFoldOp> {
    use crate::ir::AggOp;
    match op {
        AggOp::Sum | AggOp::SumShifted => Some(AggFoldOp::Sum),
        AggOp::Avg | AggOp::AvgLen | AggOp::AvgCharLen => Some(AggFoldOp::Sum),
        AggOp::Min => Some(AggFoldOp::Min),
        AggOp::Max => Some(AggFoldOp::Max),
        AggOp::VarSamp | AggOp::VarPop | AggOp::StddevSamp | AggOp::StddevPop => {
            Some(AggFoldOp::SumSq)
        }
        AggOp::BitAnd => Some(AggFoldOp::BitAnd),
        AggOp::BitOr => Some(AggFoldOp::BitOr),
        AggOp::CountStar | AggOp::CountDistinct | AggOp::EmitMatches | AggOp::MinBytes => None,
        // [aggqual] distinct folds ride the pair-dedup machinery, never
        // a per-row cell fold.
        AggOp::SumDistinct | AggOp::AvgDistinct => None,
        // [sortgrp v1] the tier-2 order-sensitive/holistic ops have NO
        // cell fold — they ride the SortGrouped run-boundary walk.
        AggOp::StringAgg
        | AggOp::ArrayAgg
        | AggOp::ArrayAggDistinct
        | AggOp::PercentileDisc
        | AggOp::PercentileCont
        | AggOp::Mode => None,
    }
}

/// Cell merge (combine grain): the same law applied cell-to-cell.
#[inline]
pub fn combine_cell_fold(op: AggFoldOp, into: &mut AccumCell, from: &AccumCell) {
    match op {
        AggFoldOp::CountCol => into.b += from.b,
        AggFoldOp::Sum => {
            into.a += from.a;
            into.b += from.b;
        }
        AggFoldOp::SumSq => {
            into.a += from.a;
            into.a2 += from.a2;
            into.b += from.b;
        }
        AggFoldOp::Min | AggFoldOp::Max | AggFoldOp::BitAnd | AggFoldOp::BitOr => {
            if from.valid != 0 {
                scatter_cell_fold(op, into, from.a as i64, true);
                into.b += from.b - 1;
            }
        }
    }
}

/// Min/Max answer: `None` = no row folded = SQL NULL (the empty-domain
/// law by construction — never a sentinel).
#[inline]
pub fn minmax_answer(cell: &AccumCell) -> Option<i64> {
    (cell.valid != 0).then(|| cell.a as i64)
}

// ---------------------------------------------------------------------------
// face word-key laws (order-preserving i64 embeds per storage face)
// ---------------------------------------------------------------------------

/// PG float8 total order as an order-preserving i64 key: -0 == 0 and all
/// NaNs are equal and GREATER than everything (float8_cmp semantics), so
/// both are canonicalized before the IEEE bit trick.
#[inline(always)]
pub fn f64_key(f: f64) -> i64 {
    let f = if f.is_nan() {
        f64::NAN // one canonical NaN: all NaNs group/compare equal
    } else if f == 0.0 {
        0.0 // -0.0 == 0.0
    } else {
        f
    };
    let b = f.to_bits();
    // order-preserving u64: negatives reverse, positives offset above.
    let u = if b >> 63 != 0 { !b } else { b | (1u64 << 63) };
    (u ^ (1u64 << 63)) as i64
}

/// Inverse of `f64_key` (canonical values round-trip; that is all the
/// answer layer ever holds).
#[inline(always)]
pub fn f64_from_key(k: i64) -> f64 {
    let u = (k as u64) ^ (1u64 << 63);
    let b = if u >> 63 != 0 { u & !(1u64 << 63) } else { !u };
    f64::from_bits(b)
}

/// float4 datum (raw bits in the low 32) -> the f64 key domain. f32->f64
/// widening is exact and order-preserving, so float4 rides the same law.
#[inline(always)]
pub fn f32_key(datum: u64) -> i64 {
    f64_key(f32::from_bits(datum as u32) as f64)
}

/// Byte-image min fold (Fixed faces: uuid memcmp order).
#[inline]
pub fn fold_min_bytes(acc: &mut Option<Vec<u8>>, b: &[u8]) {
    if acc.as_deref().map(|a| b < a).unwrap_or(true) {
        *acc = Some(b.to_vec());
    }
}

#[inline]
pub fn fold_max_bytes(acc: &mut Option<Vec<u8>>, b: &[u8]) {
    if acc.as_deref().map(|a| b > a).unwrap_or(true) {
        *acc = Some(b.to_vec());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_rows_fold_nothing() {
        let mut c = AccumCell::default();
        scatter_cell_fold(AggFoldOp::Min, &mut c, 5, false);
        scatter_cell_fold(AggFoldOp::Sum, &mut c, 5, false);
        assert_eq!(minmax_answer(&c), None);
        assert_eq!(c.b, 0);
    }

    #[test]
    fn min_max_option_law() {
        let mut c = AccumCell::default();
        for v in [3i64, -7, 12] {
            scatter_cell_fold(AggFoldOp::Min, &mut c, v, true);
        }
        assert_eq!(minmax_answer(&c), Some(-7));
        assert_eq!(c.b, 3);
        let mut m = AccumCell::default();
        for v in [3i64, -7, 12] {
            scatter_cell_fold(AggFoldOp::Max, &mut m, v, true);
        }
        assert_eq!(minmax_answer(&m), Some(12));
    }

    #[test]
    fn f64_key_pg_order() {
        // PG float8 order: -inf < -1 < -0 == 0 < 1 < +inf < NaN == NaN.
        let vals = [f64::NEG_INFINITY, -1.0, -f64::MIN_POSITIVE, 0.0, 1.0, f64::INFINITY];
        let keys: Vec<i64> = vals.iter().map(|&f| f64_key(f)).collect();
        for w in keys.windows(2) {
            assert!(w[0] < w[1], "order violated: {keys:?}");
        }
        assert_eq!(f64_key(-0.0), f64_key(0.0));
        assert_eq!(f64_key(f64::NAN), f64_key(-f64::NAN));
        assert!(f64_key(f64::NAN) > f64_key(f64::INFINITY));
        // round trips (canonical values)
        for &f in &vals {
            assert_eq!(f64_from_key(f64_key(f)).to_bits(), f.to_bits());
        }
        assert!(f64_from_key(f64_key(f64::NAN)).is_nan());
    }

    #[test]
    fn sumsq_lane_exact() {
        let mut c = AccumCell::default();
        for v in [3i64, -4, 0, 5] {
            scatter_cell_fold(AggFoldOp::SumSq, &mut c, v, true);
        }
        // NULL folds nothing (n stays the non-null count).
        scatter_cell_fold(AggFoldOp::SumSq, &mut c, 999, false);
        assert_eq!((c.b, c.a, c.a2), (4, 4, 50));
        // Single row: n=1 — the samp finishers' NULL row, decidable at answer.
        let mut one = AccumCell::default();
        scatter_cell_fold(AggFoldOp::SumSq, &mut one, 7, true);
        assert_eq!((one.b, one.a, one.a2), (1, 7, 49));
    }

    #[test]
    fn sumsq_overflow_witness_bound() {
        // The admission law's edge: |x| = 2^31 - 1 (the witnessed int8
        // bound / int4 domain edge) squares exactly in i128.
        let b = (1i64 << 31) - 1;
        let mut c = AccumCell::default();
        for v in [b, -b, b] {
            scatter_cell_fold(AggFoldOp::SumSq, &mut c, v, true);
        }
        assert_eq!(c.a2, 3 * (b as i128) * (b as i128));
        assert_eq!(c.a, b as i128);
        // int2/int4 extremes.
        let mut m = AccumCell::default();
        for v in [i32::MIN as i64, i32::MAX as i64, i16::MIN as i64] {
            scatter_cell_fold(AggFoldOp::SumSq, &mut m, v, true);
        }
        assert_eq!(
            m.a2,
            (i32::MIN as i128).pow(2) + (i32::MAX as i128).pow(2) + (i16::MIN as i128).pow(2)
        );
    }

    #[test]
    fn bit_fold_laws() {
        // all-NULL → no fold → answer NULL via valid.
        let mut c = AccumCell::default();
        scatter_cell_fold(AggFoldOp::BitAnd, &mut c, 0xFF, false);
        assert_eq!(minmax_answer(&c), None);
        // single row is its own answer (identity via valid, no sentinel).
        let mut c1 = AccumCell::default();
        scatter_cell_fold(AggFoldOp::BitAnd, &mut c1, 0b1010, true);
        assert_eq!(minmax_answer(&c1), Some(0b1010));
        // AND / OR over a mix including negatives (sign-extended words:
        // bitwise ops are width-local, upper bits are sign copies).
        let rows = [0b1100i64, 0b1010, -1];
        let (mut ca, mut co) = (AccumCell::default(), AccumCell::default());
        for &v in &rows {
            scatter_cell_fold(AggFoldOp::BitAnd, &mut ca, v, true);
            scatter_cell_fold(AggFoldOp::BitOr, &mut co, v, true);
        }
        assert_eq!(minmax_answer(&ca), Some(0b1000));
        assert_eq!(minmax_answer(&co), Some(-1));
        assert_eq!(ca.b, 3);
    }

    #[test]
    fn bool_via_minmax_word_lane() {
        // bool_and = MIN over 0/1; bool_or = MAX over 0/1; empty → NULL.
        let rows = [1i64, 0, 1];
        let (mut mn, mut mx) = (AccumCell::default(), AccumCell::default());
        for &v in &rows {
            scatter_cell_fold(AggFoldOp::Min, &mut mn, v, true);
            scatter_cell_fold(AggFoldOp::Max, &mut mx, v, true);
        }
        assert_eq!(minmax_answer(&mn), Some(0)); // bool_and = f
        assert_eq!(minmax_answer(&mx), Some(1)); // bool_or = t
        assert_eq!(minmax_answer(&AccumCell::default()), None);
    }

    #[test]
    fn combine_matches_scatter() {
        for op in [
            AggFoldOp::CountCol,
            AggFoldOp::Sum,
            AggFoldOp::Min,
            AggFoldOp::Max,
            AggFoldOp::SumSq,
            AggFoldOp::BitAnd,
            AggFoldOp::BitOr,
        ] {
            let rows = [4i64, -2, 9, 9, 0];
            let mut whole = AccumCell::default();
            for &v in &rows {
                scatter_cell_fold(op, &mut whole, v, true);
            }
            let mut left = AccumCell::default();
            let mut right = AccumCell::default();
            for &v in &rows[..2] {
                scatter_cell_fold(op, &mut left, v, true);
            }
            for &v in &rows[2..] {
                scatter_cell_fold(op, &mut right, v, true);
            }
            combine_cell_fold(op, &mut left, &right);
            assert_eq!(left, whole, "op {op:?}");
        }
    }
}
