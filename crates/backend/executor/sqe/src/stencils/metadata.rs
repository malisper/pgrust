//! metadata-answer stencil: the query is answered from the manifest/stats
//! plane, zero data reads (answer-plane substitution, tier-1 rewrite).
//!
//! [famB M1] The full flat-stats AggSpec vocabulary:
//! per-part §8.1 stats records answer sums / nonzero counts / exact-key
//! min-max; any part without the witness falls back to ONE fused decode
//! pass that reproduces the stats semantics (signed-sum law — the hot-shape
//! UserID lesson). A single stats-answerable predicate term (hot-shape `<> 0`)
//! folds through the same records; rows come from the manifest.
//!
//! P1-1 fixes of record (currency-insertion.md §4): `ColFacts` min/max
//! are Option — the PoC's `i64::MAX/MIN` sentinels would have RENDERED on
//! an all-empty domain. MIN/MAX/SUM/AVG over zero rows are NULL through
//! the answer validity leg. Render law rides the AggSpec's TypMeta at the
//! render seam, never the op variant (MinDate/AvgExact are dead).
//!
//! 3VL + face closure (engine-currency lane):
//!   - every fact is NONNULL-based: SUM/AVG fold non-null rows only and
//!     AVG divides by count(nonnull), SUM over zero non-null rows is
//!     NULL, the `= 0` count is `nonnull - nonzero` (NULL is not zero);
//!   - the decode fallback consults the validity face (AllValid fast
//!     path: the specialized null-free signed loop is byte-identical to
//!     the PoC's — law 11);
//!   - min/max fold in the FACE's order-preserving word-key domain
//!     (fold.rs laws: unsigned zero-extend, float f64_key with PG NaN
//!     order, bool 0/1) and un-embed at the answer seam; Fixed faces
//!     (uuid) fold min/max by memcmp over the byte images.
//!   - the stats fast path serves SIGNED word faces only (min_key/
//!     max_key/sum_i128 are signed-domain facts); other faces take the
//!     decode fallback.

use crate::answer::{AnswerCol, AnswerSet, BytesBuild, ColData, Validity};
use crate::bank::Face;
use crate::engine::SqeCtx;
use crate::fold::{f64_from_key, fold_max_bytes, fold_min_bytes};
use crate::ir::{AggOp, CmpOp, PlanNode};
use crate::kernels_f123::part_stats;
use crate::scan::{open_cursor, GranValid, Scratch};
use pgrc2_format::meta::KeyKind;

/// Per-column facts folded across parts (stats plane or scan fallback).
/// min/max are None until a NON-NULL row is seen — the empty-domain NULL
/// law by construction. Word facts live in the face's order-preserving
/// key domain; bmin/bmax carry Fixed-face byte images.
struct ColFacts {
    sum: i128,
    /// Σx² over non-null rows (folded only when the agg set wants it —
    /// the overflow law is the planner's admission witness).
    sumsq: i128,
    nonzero: u64,
    nonnull: u64,
    min: Option<i64>,
    max: Option<i64>,
    /// bit_and/bit_or folds (None until a non-null row folds — the
    /// all-NULL answer is NULL, never an identity-element sentinel).
    band: Option<i64>,
    bor: Option<i64>,
    bmin: Option<Vec<u8>>,
    bmax: Option<Vec<u8>>,
}

impl ColFacts {
    fn empty() -> ColFacts {
        ColFacts {
            sum: 0,
            sumsq: 0,
            nonzero: 0,
            nonnull: 0,
            min: None,
            max: None,
            band: None,
            bor: None,
            bmin: None,
            bmax: None,
        }
    }
    fn merge(&mut self, o: ColFacts) {
        self.sum += o.sum;
        self.sumsq += o.sumsq;
        self.nonzero += o.nonzero;
        self.nonnull += o.nonnull;
        self.min = match (self.min, o.min) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        self.max = match (self.max, o.max) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
        self.band = match (self.band, o.band) {
            (Some(a), Some(b)) => Some(a & b),
            (a, b) => a.or(b),
        };
        self.bor = match (self.bor, o.bor) {
            (Some(a), Some(b)) => Some(a | b),
            (a, b) => a.or(b),
        };
        if let Some(b) = o.bmin {
            fold_min_bytes(&mut self.bmin, &b);
        }
        if let Some(b) = o.bmax {
            fold_max_bytes(&mut self.bmax, &b);
        }
    }
}

/// Which facts one column's agg legs consume (decides the stats-vs-
/// decode election per part: sumsq/bit facts are decode-only).
#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct ColWant {
    minmax: bool,
    /// Σx² wanted (variance family). NEVER folded unwanted: the overflow
    /// law holds only for planner-witnessed input domains.
    sq: bool,
    /// bit_and/bit_or folds wanted.
    bits: bool,
}

impl ColWant {
    fn of(op: AggOp) -> ColWant {
        ColWant {
            minmax: matches!(op, AggOp::Min | AggOp::Max),
            sq: matches!(
                op,
                AggOp::VarSamp | AggOp::VarPop | AggOp::StddevSamp | AggOp::StddevPop
            ),
            bits: matches!(op, AggOp::BitAnd | AggOp::BitOr),
        }
    }
    fn or(self, o: ColWant) -> ColWant {
        ColWant {
            minmax: self.minmax || o.minmax,
            sq: self.sq || o.sq,
            bits: self.bits || o.bits,
        }
    }
}

/// Fold one column across all parts, PART-PARALLEL (the serial arm —
/// `PGRUST_SQE_META_PAR=0` — was the measured-settled control: 511
/// Stats-section preads one at a time on the driving thread were the
/// whole hot-shape cold wall).
fn col_facts(ctx: &SqeCtx, attno: u32, want: ColWant) -> ColFacts {
    let bank = ctx.bank;
    let face = bank.face(attno);
    let per = crate::engine::par_parts(ctx.faces.cfg.threads, bank.parts.len(), |pi| {
        part_facts(bank, pi, attno, want, face)
    });
    let mut f = ColFacts::empty();
    for p in per {
        f.merge(p);
    }
    f
}

/// One part's facts: stats fast path (signed word faces), decode
/// fallback (signed-sum law; validity-aware; face word keys) otherwise.
/// Σx² and bit facts are never in the stats records — wanting them
/// forces the decode pass.
fn part_facts(
    bank: &crate::bank::Bank,
    pi: usize,
    attno: u32,
    want: ColWant,
    face: Face,
) -> ColFacts {
    let mut f = ColFacts::empty();
    let rec = if matches!(face, Face::SignedWord(_)) && !want.sq && !want.bits {
        part_stats(bank, pi, attno)
    } else {
        None // min_key/max_key/sum_i128 are signed-domain facts
    };
    let stats_ok = match &rec {
        Some(r) => !want.minmax || r.key_kind == KeyKind::Exact.as_u8(),
        None => false,
    };
    if stats_ok {
        let r = rec.unwrap();
        f.sum += r.sum_i128;
        f.nonnull += r.nonnull as u64;
        f.nonzero += r.nonnull as u64 - r.zero_count;
        if want.minmax && r.nonnull > 0 {
            f.min = Some(r.min_key);
            f.max = Some(r.max_key);
        }
        return f;
    }
    let mut s = Scratch::new();
    let mut cur = open_cursor(bank, pi, attno);
    for g in 0..cur.granule_count() {
        let rows = cur.rows_in_granule(g) as usize;
        if rows == 0 {
            continue;
        }
        let gv = s.validity(&mut cur, g, rows);
        let d = s.decode_full(&mut cur, g, rows);
        // Alias the datum window so `s.row_valid` stays borrowable (the
        // stencil-wide raw-parts pattern; validity words and datums are
        // disjoint Scratch planes).
        let d: &[u64] = unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) };
        match (face, gv) {
            // The PoC's null-free signed fast loop, byte-identical (law
            // 11: the reference-workload shape pays nothing for the machinery).
            (Face::SignedWord(_), GranValid::AllValid) if !want.sq && !want.bits => {
                let (mut lo, mut hi, mut neg, mut nz) = (0u64, 0u64, 0u64, 0u64);
                let (mut mn, mut mx) = (i64::MAX, i64::MIN);
                for &x in d {
                    lo += x & 0xFFFF_FFFF;
                    hi += x >> 32;
                    neg += x >> 63;
                    nz += (x != 0) as u64;
                    let v = x as i64;
                    mn = mn.min(v);
                    mx = mx.max(v);
                }
                f.sum += lo as i128 + ((hi as i128) << 32) - ((neg as i128) << 64);
                f.nonzero += nz;
                f.nonnull += rows as u64;
                f.merge(ColFacts { min: Some(mn), max: Some(mx), ..ColFacts::empty() });
            }
            (Face::Fixed(len), gv) => {
                let all = gv.all_valid();
                for (r, &x) in d.iter().enumerate() {
                    if !all && !s.row_valid(r) {
                        continue;
                    }
                    let b =
                        unsafe { std::slice::from_raw_parts(x as *const u8, len as usize) };
                    f.nonnull += 1;
                    fold_min_bytes(&mut f.bmin, b);
                    fold_max_bytes(&mut f.bmax, b);
                }
            }
            (Face::Varlena, _) => panic!("metadata_answer: varlena facts (lowering bug)"),
            // General word-face loop (null-aware; face word-key embed;
            // cell math through THE fold law — fold.rs scatter_cell_fold).
            (face, gv) => {
                use crate::fold::{minmax_answer, scatter_cell_fold, AccumCell, AggFoldOp};
                let all = gv.all_valid();
                let (mut cs, mut cn, mut cx) =
                    (AccumCell::default(), AccumCell::default(), AccumCell::default());
                let (mut ca, mut co) = (AccumCell::default(), AccumCell::default());
                let mut nz = 0u64;
                for (r, &x) in d.iter().enumerate() {
                    let valid = all || s.row_valid(r);
                    if !valid {
                        continue;
                    }
                    let k = face.word_key(x);
                    // Σx² only when a variance leg wants it: the i128
                    // exactness law holds only for the planner-witnessed
                    // domains (never fold it speculatively).
                    if want.sq {
                        scatter_cell_fold(AggFoldOp::SumSq, &mut cs, k, true);
                    } else {
                        scatter_cell_fold(AggFoldOp::Sum, &mut cs, k, true);
                    }
                    scatter_cell_fold(AggFoldOp::Min, &mut cn, k, true);
                    scatter_cell_fold(AggFoldOp::Max, &mut cx, k, true);
                    if want.bits {
                        scatter_cell_fold(AggFoldOp::BitAnd, &mut ca, k, true);
                        scatter_cell_fold(AggFoldOp::BitOr, &mut co, k, true);
                    }
                    nz += (k != 0) as u64;
                }
                f.sum += cs.a;
                f.sumsq += cs.a2;
                f.nonzero += nz;
                f.nonnull += cs.b as u64;
                f.merge(ColFacts {
                    min: minmax_answer(&cn),
                    max: minmax_answer(&cx),
                    band: minmax_answer(&ca),
                    bor: minmax_answer(&co),
                    ..ColFacts::empty()
                });
            }
        }
    }
    f
}

pub fn run_metadata_answer(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    let bank = ctx.bank;
    let n: u64 = bank.manifest.parts.iter().map(|p| p.rows).sum();

    // Stats-answerable predicate: exactly one `= 0` / `<> 0`
    // term. The lowering refuses anything else (Refuse::
    // MetadataPredNotStatsAnswerable); this assert is the internal
    // invariant, not a reachable-by-plan-shape error.
    let pred_term = node.pred.as_ref().map(|p| {
        assert!(
            p.var_terms.is_empty() && p.terms.len() == 1 && p.terms[0].lo == 0,
            "metadata_answer: predicate not stats-answerable (lowering bug)"
        );
        &p.terms[0]
    });

    // Fold facts once per distinct column mentioned by the agg list.
    let mut cols: Vec<(u32, ColWant)> = Vec::new();
    let mut note = |c: u32, w: ColWant| match cols.iter_mut().find(|(cc, _)| *cc == c) {
        Some(e) => e.1 = e.1.or(w),
        None => cols.push((c, w)),
    };
    for a in &node.agg {
        if let Some(c) = a.col {
            note(c, ColWant::of(a.op));
        }
    }
    if let Some(t) = pred_term {
        note(t.col, ColWant::default());
    }
    let facts: Vec<(u32, ColFacts)> = cols
        .iter()
        .map(|&(c, w)| (c, col_facts(ctx, c, w)))
        .collect();
    let get = |c: u32| -> &ColFacts { &facts.iter().find(|(cc, _)| *cc == c).unwrap().1 };

    // Un-embed a word-key min/max answer per the column's face (the ONE
    // place a key leaves the fold domain).
    let word_answer = |a: &crate::ir::AggSpec, key: Option<i64>| -> AnswerCol {
        let c = a.col.unwrap();
        match bank.face(c) {
            Face::F32 | Face::F64 => AnswerCol {
                ty: a.out,
                data: ColData::F64(vec![key.map(f64_from_key).unwrap_or(0.0)]),
                validity: if key.is_some() {
                    Validity::AllValid
                } else {
                    Validity::Mask(vec![false])
                },
            },
            _ => AnswerCol::i64s_opt(a.out, vec![key]),
        }
    };
    let bytes_answer = |a: &crate::ir::AggSpec, b: &Option<Vec<u8>>| -> AnswerCol {
        let mut bb = BytesBuild::new();
        bb.push(b.as_deref().unwrap_or(b""));
        let mut c = bb.finish(a.out);
        if b.is_none() {
            c.validity = Validity::Mask(vec![false]);
        }
        c
    };

    let out_cols: Vec<AnswerCol> = node
        .agg
        .iter()
        .map(|a| {
            let cf = a.col.map(&get);
            match a.op {
                AggOp::CountStar => {
                    let c = match pred_term {
                        None => n,
                        Some(t) => {
                            let f = get(t.col);
                            match t.op {
                                CmpOp::Ne => f.nonzero,
                                // `= 0`: zero-valued NON-NULL rows (NULL
                                // is not zero — the 3VL count law).
                                CmpOp::Eq => f.nonnull - f.nonzero,
                                _ => unreachable!("gated above"),
                            }
                        }
                    };
                    AnswerCol::i64s(a.out, vec![c as i64])
                }
                // SUM folds non-null rows; over zero of them it is NULL.
                AggOp::Sum => {
                    let f = cf.unwrap();
                    let mut c = AnswerCol::i128s(a.out, vec![f.sum]);
                    if f.nonnull == 0 {
                        c.validity = Validity::Mask(vec![false]);
                    }
                    c
                }
                AggOp::SumShifted => {
                    // SUM(col + k): NULL + k is NULL — shift by the
                    // NON-NULL count.
                    let f = cf.unwrap();
                    // k is i64 bits in the u64 field (negative shifts
                    // author two's-complement — read back signed).
                    let v = f.sum + (a.k as i64 as i128) * (f.nonnull as i128);
                    let mut c = AnswerCol::i128s(a.out, vec![v]);
                    if f.nonnull == 0 {
                        c.validity = Validity::Mask(vec![false]);
                    }
                    c
                }
                // AVG = sum / count(nonnull) (PG strict-agg law); the
                // ratios ctor NULLs the count-0 row.
                AggOp::Avg => AnswerCol::ratios(
                    a.out,
                    vec![(cf.unwrap().sum, cf.unwrap().nonnull as i64)],
                    a.avg_exact(),
                ),
                AggOp::Min => {
                    let f = cf.unwrap();
                    if matches!(bank.face(a.col.unwrap()), Face::Fixed(_)) {
                        bytes_answer(a, &f.bmin)
                    } else {
                        word_answer(a, f.min)
                    }
                }
                AggOp::Max => {
                    let f = cf.unwrap();
                    if matches!(bank.face(a.col.unwrap()), Face::Fixed(_)) {
                        bytes_answer(a, &f.bmax)
                    } else {
                        word_answer(a, f.max)
                    }
                }
                // Variance family: the exact {n, Σx, Σx²} triple; the
                // finisher (and its n==0 / n<=1 NULL laws) runs at the
                // answer/render seam.
                AggOp::VarSamp | AggOp::VarPop | AggOp::StddevSamp | AggOp::StddevPop => {
                    let f = cf.unwrap();
                    let kind = crate::answer::MomentKind::of_op(a.op).expect("moment op");
                    crate::answer::AnswerCol::moments(
                        a.out,
                        kind,
                        vec![(f.nonnull as i64, f.sum, f.sumsq)],
                    )
                }
                AggOp::BitAnd => AnswerCol::i64s_opt(a.out, vec![cf.unwrap().band]),
                AggOp::BitOr => AnswerCol::i64s_opt(a.out, vec![cf.unwrap().bor]),
                other => panic!("metadata_answer: unsupported agg {other:?} (lowering bug)"),
            }
        })
        .collect();
    AnswerSet::from_cols(out_cols)
}
