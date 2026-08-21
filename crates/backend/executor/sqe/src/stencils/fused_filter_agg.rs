//! fused filter-agg stencil (SWAR/zone-skip family): predicate + aggregate
//! fused in one pass, zone/stats skipping ahead of every decode. Two agg
//! modes, both parametric:
//!   - grouped CountStar over a DENSE key domain (hot-shape shape): part-owned
//!     pool walk, per-part stats consult in-timing, fused nonzero SWAR
//!     count when the encoding face supports it, dense count array sized
//!     by the stats-elected domain. Condcache class is NEGATIVE here.
//!   - EmitMatches (hot-shape shape): flat-SMA zone consult, survivor decode,
//!     emit matching values; parallelism elected from the live-unit
//!     count (§1.6). Cold fills verdicts as the fused loop runs, warm
//!     replays them in parallel with decode_sel.

use crate::answer::{AnswerCol, AnswerSet};
use crate::engine::{CVerdict, SqeCtx};
use crate::exec::{publish_cache, replay_cache};
use crate::fused::FusedInts;
use crate::ir::*;
use crate::planner::{agg_tier, elect_threads, AggTier};
use crate::scan::{CurCache, Scratch};
use crate::statsview::PartStats;
use crate::stencils::{col_width, sx};
use crate::typmeta::TypMeta;
use pgrc2_format::meta::{KeyKind, StatsRecord, STATSF_COMPUTED};
use std::cell::RefCell;

/// Per-worker persistent scratch (the pool-lifecycle discipline): decode
/// arena + cursor cache + dense count slice survive across reps; counts
/// are cleared, never freed.
struct GScratch {
    col: u32,
    scr: Scratch,
    cc: CurCache,
    counts: Vec<u32>,
}

thread_local! {
    // tls-dtor: plain-data — held type audited 2026-08-19: no Drop beyond plain collections/dealloc.
    static GS: RefCell<Option<GScratch>> = const { RefCell::new(None) };
}

fn with_gscratch<R>(col: u32, dom: usize, f: impl FnOnce(&mut GScratch) -> R) -> R {
    GS.with(|cell| {
        let mut cell = cell.borrow_mut();
        let stale = match cell.as_ref() {
            Some(s) => s.col != col,
            None => true,
        };
        if stale {
            if let Some(old) = cell.take() {
                crate::scan::scratch_park(old.scr);
            }
            *cell = Some(GScratch {
                col,
                scr: crate::scan::scratch_fetch(),
                cc: CurCache::new(col),
                counts: Vec::new(),
            });
        }
        let s = cell.as_mut().unwrap();
        if s.counts.len() != dom {
            s.counts = vec![0u32; dom];
        } else {
            s.counts.fill(0);
        }
        f(s)
    })
}

pub fn run_fused_filter_agg(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    // Rung B: the ungrouped filtered scalar fold (count/sum/avg/min/max
    // under an int-conjunct predicate and/or [aggqual] per-agg FILTER
    // conjuncts). EmitMatches keeps its own arm.
    if node.params.group_cols.is_empty()
        && !node.agg.iter().any(|a| a.op == AggOp::EmitMatches)
    {
        return filtered_fold(ctx, node);
    }
    let pred = node.pred.as_ref().expect("fused_filter_agg: predicate-bearing family");
    assert_eq!(pred.terms.len(), 1, "M0: fused filter-agg takes one term");
    match node.agg[0].op {
        AggOp::CountStar if !node.params.group_cols.is_empty() => {
            grouped_count(ctx, node, &pred.terms[0])
        }
        AggOp::EmitMatches => emit_matches(ctx, node, &pred.terms[0]),
        other => panic!("fused_filter_agg: unsupported agg {other:?} (M0)"),
    }
}

// ---------------------------------------------------------------------------
// ungrouped filtered scalar fold (rung B): predicate conjunction + strict
// scalar aggregates in one fused pass. 3VL by the fold law: a NULL in any
// predicate column fails the row (eval_v), a NULL agg input among the
// survivors is skipped (strict aggs); zero survivors render count 0 and
// NULL sum/avg/min/max through the answer validity leg. Zone skip per
// term over the flat SMA faces ahead of every decode.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Default)]
pub(crate) struct ColAcc {
    pub(crate) sum: i128,
    /// Σx², folded only when a variance leg reads this column (`sq`
    /// hoisted per column): the i128 exactness law holds only for
    /// planner-witnessed input domains — never folded speculatively.
    pub(crate) sumsq: i128,
    pub(crate) n: u64,
    pub(crate) min: Option<i64>,
    pub(crate) max: Option<i64>,
    pub(crate) band: Option<i64>,
    pub(crate) bor: Option<i64>,
}

impl ColAcc {
    pub(crate) fn fold(&mut self, k: i64, sq: bool) {
        self.sum += k as i128;
        if sq {
            self.sumsq += (k as i128) * (k as i128);
        }
        self.n += 1;
        self.min = Some(self.min.map_or(k, |m| m.min(k)));
        self.max = Some(self.max.map_or(k, |m| m.max(k)));
        self.band = Some(self.band.map_or(k, |m| m & k));
        self.bor = Some(self.bor.map_or(k, |m| m | k));
    }
    pub(crate) fn merge(&mut self, o: &ColAcc) {
        self.sum += o.sum;
        self.sumsq += o.sumsq;
        self.n += o.n;
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
    }
}

fn filtered_fold(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    use crate::answer::{ColData, Validity};
    use crate::bank::Face;
    use crate::fold::f64_from_key;

    let (bank, pool) = (ctx.bank, ctx.pool);
    static NO_TERMS: Vec<PredTerm> = Vec::new();
    static NO_VTERMS: Vec<VarPredTerm> = Vec::new();
    let (terms, vterms): (&Vec<PredTerm>, &Vec<VarPredTerm>) = match node.pred.as_ref() {
        Some(p) => (&p.terms, &p.var_terms),
        None => (&NO_TERMS, &NO_VTERMS),
    };
    // [aggqual] per-agg FILTER conjuncts: (leg index, its terms).
    let flt_legs: Vec<(usize, &[PredTerm])> = node
        .params
        .agg_filters
        .iter()
        .enumerate()
        .filter_map(|(ai, f)| f.as_ref().map(|p| (ai, p.terms.as_slice())))
        .collect();
    assert!(
        !terms.is_empty() || !vterms.is_empty() || !flt_legs.is_empty(),
        "filtered_fold: needs a term (lowering gate)"
    );

    // Distinct decode set: predicate columns, then agg inputs.
    let mut dcols: Vec<u32> = Vec::new();
    let idx_of = |dcols: &mut Vec<u32>, c: u32| -> usize {
        match dcols.iter().position(|&x| x == c) {
            Some(i) => i,
            None => {
                dcols.push(c);
                dcols.len() - 1
            }
        }
    };
    let term_di: Vec<usize> = terms.iter().map(|t| idx_of(&mut dcols, t.col)).collect();
    let vterm_di: Vec<usize> = vterms.iter().map(|t| idx_of(&mut dcols, t.col)).collect();
    let agg_di: Vec<Option<usize>> =
        node.agg.iter().map(|a| a.col.map(|c| idx_of(&mut dcols, c))).collect();
    // [P4-1] fused-arithmetic legs: (leg, di of a, di of b, shape). The
    // shape dispatch happens ONCE per granule (the de-interpretation
    // law); the per-row word is the proven-no-overflow i64 evaluation.
    let expr_legs: Vec<(usize, usize, usize, Option<usize>, crate::ir::FoldExpr)> = node
        .agg
        .iter()
        .enumerate()
        .filter_map(|(ai, a)| {
            a.expr.map(|e| {
                let da = agg_di[ai].expect("expr leg has a primary column");
                let db = idx_of(&mut dcols, e.col2());
                let dc = e.col3().map(|c| idx_of(&mut dcols, c));
                (ai, da, db, dc, e)
            })
        })
        .collect();
    // [aggqual] per-leg filter term decode indices (aligned with
    // flt_legs; the columns join the decode set only — filter terms
    // evaluate on statement survivors, never at frame grain).
    let flt_dis: Vec<Vec<usize>> = flt_legs
        .iter()
        .map(|(_, ts)| ts.iter().map(|t| idx_of(&mut dcols, t.col)).collect())
        .collect();
    let npred_dcols = {
        let mut n = 0usize;
        for &di in term_di.iter().chain(&vterm_di) {
            n = n.max(di + 1);
        }
        n
    };
    let faces_v: Vec<crate::bank::Face> = dcols.iter().map(|&c| bank.face(c)).collect();
    // Distinct agg decode-column indices (fold once per column, legs
    // read their column's facts). Expr legs fold per-LEG cells instead —
    // their columns join the DECODE set only.
    // [aggqual] a FILTER-bearing leg never joins the shared per-column
    // fold — it folds from ITS OWN survivor selection into a per-leg
    // cell.
    let has_flt = |ai: usize| flt_legs.iter().any(|&(i, _)| i == ai);
    let mut fold_dis: Vec<usize> = node
        .agg
        .iter()
        .enumerate()
        .zip(&agg_di)
        .filter(|((ai, a), _)| a.expr.is_none() && !has_flt(*ai))
        .filter_map(|(_, di)| *di)
        .collect();
    fold_dis.sort_unstable();
    fold_dis.dedup();
    let mut decode_dis: Vec<usize> = fold_dis.clone();
    for &(_, da, db, dc, _) in &expr_legs {
        decode_dis.push(da);
        decode_dis.push(db);
        if let Some(dc) = dc {
            decode_dis.push(dc);
        }
    }
    for (fi, &(ai, _)) in flt_legs.iter().enumerate() {
        if let Some(di) = agg_di[ai] {
            decode_dis.push(di);
        }
        decode_dis.extend(&flt_dis[fi]);
    }
    decode_dis.sort_unstable();
    decode_dis.dedup();
    // Per-column Σx² election (hoisted out of the row loop).
    let needs_sq: Vec<bool> = (0..dcols.len())
        .map(|di| {
            node.agg.iter().zip(&agg_di).any(|(a, adi)| {
                *adi == Some(di)
                    && matches!(
                        a.op,
                        AggOp::VarSamp | AggOp::VarPop | AggOp::StddevSamp | AggOp::StddevPop
                    )
            })
        })
        .collect();
    let needs_sq = &needs_sq;

    let units = ctx.faces.walk(bank, dcols[0]);
    let smas: Vec<_> = terms.iter().map(|t| ctx.faces.sma(bank, t.col)).collect();
    // Null-freedom per term (cached metadata fact), hoisted: the
    // zone-all-pass arm is sound only over proven-nonnull columns
    // (zone min/max are facts about the NON-NULL values).
    let term_nullfree: Vec<bool> = terms.iter().map(|t| bank.null_free(t.col)).collect();
    let term_nullfree = &term_nullfree;
    // [psma-consume] §8.2 candidate-slice faces per conjunct.
    let psmas: Vec<_> = terms.iter().map(|t| ctx.faces.psma(bank, t.col)).collect();
    let psmas = &psmas;
    // first unit index of each part (granules are part-ordered).
    let mut part_units: Vec<(usize, usize)> = vec![(0, 0); bank.parts.len()];
    {
        let mut i = 0usize;
        while i < units.len() {
            let pi = units[i].0;
            let s = i;
            while i < units.len() && units[i].0 == pi {
                i += 1;
            }
            part_units[pi] = (s, i);
        }
    }

    struct WState {
        survivors: u64,
        accs: Vec<ColAcc>,
        /// [P4-1] per-LEG cells for fused-arithmetic inputs.
        eaccs: Vec<ColAcc>,
        /// [aggqual] per-FILTERED-LEG cells + survivor counts (aligned
        /// with flt_legs) and the leg selection scratch.
        faccs: Vec<ColAcc>,
        fcnt: Vec<u64>,
        fsel: Vec<u16>,
        cols: Vec<(crate::scan::Scratch, CurCache)>,
        sel: Vec<u16>,
        mask: Vec<u8>,
    }
    // [sqe-topn-gap] Claim at part-SUBDIVIDED grain: a shallow part plane
    // (parts << 4·width, the small-bank regime) starves the claim-depth
    // guard down to n/4 workers; the deepened plane engages full width.
    // The fold merge is order-independent (i128 sums, min/max, counts),
    // so the answer never depends on the claim schedule.
    let chunks = crate::pool::part_claim_chunks(&part_units, pool.threads());
    let chunks = &chunks;
    // Per-run worker state rides the depot: decode arenas fetch reset
    // from the worker's parked set and park back at engagement end
    // (run_finish) — the warm-engagement bill is the wake, not the init.
    let states = pool.run_finish(
        chunks.len(),
        |_| WState {
            survivors: 0,
            accs: vec![ColAcc::default(); dcols.len()],
            eaccs: vec![ColAcc::default(); expr_legs.len()],
            faccs: vec![ColAcc::default(); flt_legs.len()],
            fcnt: vec![0u64; flt_legs.len()],
            fsel: Vec::new(),
            cols: dcols
                .iter()
                .map(|&c| (crate::scan::scratch_fetch(), CurCache::new(c)))
                .collect(),
            sel: Vec::new(),
            mask: Vec::new(),
        },
        |s: &mut WState, ci| {
            let (pi, u0, u1) = chunks[ci];
            for ui in u0..u1 {
                let (_, g, rows32, _) = units[ui];
                let rows = rows32 as usize;
                if rows == 0 {
                    continue;
                }
                // Zone skip: the granule is dead when ANY conjunct's SMA
                // zone excludes it (NULL rows never pass eval_v anyway).
                if terms
                    .iter()
                    .enumerate()
                    .any(|(ti, t)| !t.zone_may_pass(smas[ti].mins[ui], smas[ti].maxs[ui]))
                {
                    continue;
                }
                // Zone-proven all-pass (every term, null-freedom held):
                // every row survives — predicate decode, PSMA probe and
                // the mask are skipped; folds run dense. Vacuously true
                // with no statement terms ([aggqual]-only), matching the
                // full-range seed below.
                let all_pass = vterms.is_empty()
                    && terms.iter().enumerate().all(|(ti, t)| {
                        term_nullfree[ti] && t.zone_all_pass(smas[ti].mins[ui], smas[ti].maxs[ui])
                    });
                let mut slices: Vec<Option<(&[u64], bool)>> = vec![None; dcols.len()];
                let (rlo, rhi, matched);
                if all_pass {
                    (rlo, rhi, matched) = (0, rows, rows);
                } else {
                    // [psma-consume] Zone said maybe: seed the window from
                    // the intersection of the conjuncts' PSMA candidate
                    // slices (one probe per conjunct per granule). Rows
                    // outside the window never enter the mask — the
                    // term-major residual stays mandatory inside it.
                    let w = terms.iter().enumerate().fold((0usize, rows), |w, (ti, t)| {
                        crate::psmaface::narrow(
                            w,
                            psmas[ti].as_ref().and_then(|pf| {
                                pf.slice(pi, g, rows32, smas[ti].mins[ui], smas[ti].maxs[ui], t)
                            }),
                        )
                    });
                    (rlo, rhi) = w;
                    if rlo >= rhi {
                        continue;
                    }
                    // Decode predicate columns; alias the datum windows so
                    // the per-column validity planes stay borrowable (the
                    // stencil-wide raw-parts pattern; planes are disjoint
                    // per Scratch).
                    for di in 0..npred_dcols {
                        let (scr, cc) = &mut s.cols[di];
                        let cur = cc.get(bank, pi);
                        let gv = scr.validity(cur, g, rows);
                        let d = scr.decode_full(cur, g, rows);
                        let d: &[u64] =
                            unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) };
                        slices[di] = Some((d, gv.all_valid()));
                    }
                    // [psma-consume, oracle] slice-complement emptiness gate.
                    #[cfg(feature = "oracle")]
                    for (ti, t) in terms.iter().enumerate() {
                        if let Some(sl) = psmas[ti].as_ref().and_then(|pf| {
                            pf.slice(pi, g, rows32, smas[ti].mins[ui], smas[ti].maxs[ui], t)
                        }) {
                            let di = term_di[ti];
                            let (d, allv) = slices[di].expect("pred col decoded");
                            let face = faces_v[di];
                            let scr = &s.cols[di].0;
                            crate::psmaface::oracle_check_complement(
                                t,
                                sl.0 as usize,
                                (sl.1 as usize).min(rows),
                                rows,
                                |r| allv || scr.row_valid(r),
                                |r| face.word_key(d[r]),
                            );
                        }
                    }
                    // Term-major MASK (R2, vector form): each conjunct's
                    // CmpOp × face matches ONCE per (granule, term) and a
                    // monomorphic branchless byte loop ANDs into the
                    // granule mask — same conjunct order and 3VL as the
                    // selection compaction (kernels_pred law: a NULL row's
                    // mask byte is 0; word faces compare unconditionally,
                    // byte-ref faces stay on the validity-guarded scalar
                    // residual).
                    let win = rhi - rlo;
                    if s.mask.len() < win {
                        s.mask.resize(win, 0);
                    }
                    {
                        let WState { cols, mask, .. } = &mut *s;
                        let mwin = &mut mask[..win];
                        for (ti, t) in terms.iter().enumerate() {
                            let di = term_di[ti];
                            let (d, allv) = slices[di].expect("pred col decoded");
                            let face = faces_v[di];
                            let first = ti == 0;
                            if crate::kernels_pred::mask_term(t, face, &d[rlo..rhi], mwin, first)
                            {
                                if !allv {
                                    let scr = &cols[di].0;
                                    for (i, m) in mwin.iter_mut().enumerate() {
                                        *m &= scr.row_valid(rlo + i) as u8;
                                    }
                                }
                            } else {
                                let scr = &cols[di].0;
                                for (i, m) in mwin.iter_mut().enumerate() {
                                    let r = rlo + i;
                                    let keep = ((allv || scr.row_valid(r))
                                        && t.eval(face.word_key(d[r])))
                                        as u8;
                                    if first {
                                        *m = keep;
                                    } else {
                                        *m &= keep;
                                    }
                                }
                            }
                        }
                        // Varlena conjuncts: 3VL payload eval over the
                        // surviving mask (NULL never passes).
                        for (vi, t) in vterms.iter().enumerate() {
                            let di = vterm_di[vi];
                            let (d, allv) = slices[di].expect("pred col decoded");
                            let face = faces_v[di];
                            let first = terms.is_empty() && vi == 0;
                            let scr = &cols[di].0;
                            for (i, m) in mwin.iter_mut().enumerate() {
                                if !first && *m == 0 {
                                    continue;
                                }
                                let r = rlo + i;
                                let keep = ((allv || scr.row_valid(r))
                                    && t.eval(unsafe { crate::scan::byte_payload(face, d[r]) }))
                                    as u8;
                                *m = keep;
                            }
                        }
                        matched = crate::kernels_pred::mask_count(mwin);
                    }
                }
                if matched == 0 {
                    continue;
                }
                s.survivors += matched as u64;
                // Decode agg-only columns for granules with survivors.
                for &di in &decode_dis {
                    if slices[di].is_none() {
                        let (scr, cc) = &mut s.cols[di];
                        let cur = cc.get(bank, pi);
                        let gv = scr.validity(cur, g, rows);
                        let d = scr.decode_full(cur, g, rows);
                        let d: &[u64] =
                            unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) };
                        slices[di] = Some((d, gv.all_valid()));
                    }
                }
                // The selection is now a residual currency: built once per
                // granule, only when a scalar-fallback fold or the
                // expr/[aggqual] legs consume it.
                let mut sel_built = false;
                macro_rules! ensure_sel {
                    () => {
                        if !sel_built {
                            if all_pass {
                                s.sel.clear();
                                s.sel.extend((0..rows).map(|r| r as u16));
                            } else {
                                crate::kernels_pred::mask_sel(
                                    &s.mask[..rhi - rlo],
                                    rlo,
                                    &mut s.sel,
                                );
                            }
                            sel_built = true;
                        }
                    };
                }
                // Strict fold: NULL agg inputs among survivors are skipped.
                // Null-free word columns without a Σx² leg fold branchless
                // from the mask (dense on the all-pass arm) — exact by the
                // fold's associativity (kernels_pred::fold_word law); the
                // sparse/nullable/Σx² residual keeps the selection walk.
                for &di in &fold_dis {
                    let (d, allv) = slices[di].expect("agg col decoded");
                    let sq = needs_sq[di];
                    let face = faces_v[di];
                    if !sq
                        && allv
                        && crate::kernels_pred::foldable(face)
                        && (all_pass || matched * 4 >= rhi - rlo)
                    {
                        let mask = if all_pass { None } else { Some(&s.mask[..rhi - rlo]) };
                        crate::kernels_pred::fold_word(face, &d[rlo..rhi], mask, &mut s.accs[di]);
                        continue;
                    }
                    ensure_sel!();
                    for &r16 in &s.sel {
                        let r = r16 as usize;
                        if allv || s.cols[di].0.row_valid(r) {
                            s.accs[di].fold(face.word_key(d[r]), sq);
                        }
                    }
                }
                if !expr_legs.is_empty() || !flt_legs.is_empty() {
                    ensure_sel!();
                }
                let _ = sel_built;
                // [P4-1] fused-arithmetic legs: shape dispatch at granule
                // grain, one monomorphic loop per shape. NULL law: a NULL
                // in EITHER operand skips the row (strict transition over
                // the composed input). The i64 arithmetic is proven
                // non-overflowing by the planner's admission witness.
                {
                    let WState { eaccs, cols, sel, .. } = s;
                    for (ei, &(_, da, db, dc, e)) in expr_legs.iter().enumerate() {
                        let (dav, a_allv) = slices[da].expect("expr col a decoded");
                        let (dbv, b_allv) = slices[db].expect("expr col b decoded");
                        let (fa, fb) = (faces_v[da], faces_v[db]);
                        let acc = &mut eaccs[ei];
                        let valid = |r: usize| {
                            (a_allv || cols[da].0.row_valid(r))
                                && (b_allv || cols[db].0.row_valid(r))
                        };
                        match e {
                            crate::ir::FoldExpr::MulCC { .. } => {
                                for &r16 in sel.iter() {
                                    let r = r16 as usize;
                                    if valid(r) {
                                        acc.fold(
                                            fa.word_key(dav[r]) * fb.word_key(dbv[r]),
                                            false,
                                        );
                                    }
                                }
                            }
                            crate::ir::FoldExpr::MulKSub { k, .. } => {
                                for &r16 in sel.iter() {
                                    let r = r16 as usize;
                                    if valid(r) {
                                        acc.fold(
                                            fa.word_key(dav[r]) * (k - fb.word_key(dbv[r])),
                                            false,
                                        );
                                    }
                                }
                            }
                            // [scale-alg] packed mantissa products: the
                            // planner's scale witness proved every
                            // per-row (and intermediate) product fits
                            // i64 — the mantissa math below is PG's
                            // exact numeric product at the fused scale.
                            crate::ir::FoldExpr::PackedMulK { k, sub, .. } => {
                                let sgn = if sub { -1i64 } else { 1i64 };
                                for &r16 in sel.iter() {
                                    let r = r16 as usize;
                                    if valid(r) {
                                        acc.fold(
                                            fa.word_key(dav[r])
                                                * (k + sgn * fb.word_key(dbv[r])),
                                            false,
                                        );
                                    }
                                }
                            }
                            crate::ir::FoldExpr::PackedMulK2 { k1, sub1, k2, sub2, .. } => {
                                let dc = dc.expect("PackedMulK2 stages a third column");
                                let (dcv, c_allv) = slices[dc].expect("expr col c decoded");
                                let fc = faces_v[dc];
                                let s1 = if sub1 { -1i64 } else { 1i64 };
                                let s2 = if sub2 { -1i64 } else { 1i64 };
                                for &r16 in sel.iter() {
                                    let r = r16 as usize;
                                    if valid(r) && (c_allv || cols[dc].0.row_valid(r)) {
                                        acc.fold(
                                            fa.word_key(dav[r])
                                                * (k1 + s1 * fb.word_key(dbv[r]))
                                                * (k2 + s2 * fc.word_key(dcv[r])),
                                            false,
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
                // [aggqual] per-leg FILTER folds: seed the leg's
                // selection from the STATEMENT survivors, run its
                // conjuncts term-major over that buffer (same
                // filter_sel law, same 3VL — only TRUE passes), then
                // strict-fold the survivors into the leg's own cell.
                // count(*) FILTER counts the leg's survivors.
                {
                    let WState { faccs, fcnt, fsel, cols, sel, .. } = s;
                    for (fi, &(ai, fterms)) in flt_legs.iter().enumerate() {
                        fsel.clear();
                        fsel.extend_from_slice(sel);
                        for (ti, t) in fterms.iter().enumerate() {
                            let di = flt_dis[fi][ti];
                            let (d, allv) = slices[di].expect("filter col decoded");
                            let face = faces_v[di];
                            if allv {
                                t.filter_sel(fsel, |_| true, |r| face.word_key(d[r]));
                            } else {
                                let scr = &cols[di].0;
                                t.filter_sel(
                                    fsel,
                                    |r| scr.row_valid(r),
                                    |r| face.word_key(d[r]),
                                );
                            }
                        }
                        fcnt[fi] += fsel.len() as u64;
                        if let Some(di) = agg_di[ai] {
                            let (d, allv) = slices[di].expect("agg col decoded");
                            let sq = needs_sq[di]
                                || matches!(
                                    node.agg[ai].op,
                                    AggOp::VarSamp
                                        | AggOp::VarPop
                                        | AggOp::StddevSamp
                                        | AggOp::StddevPop
                                );
                            let acc = &mut faccs[fi];
                            let face = faces_v[di];
                            for &r16 in fsel.iter() {
                                let r = r16 as usize;
                                if allv || cols[di].0.row_valid(r) {
                                    acc.fold(face.word_key(d[r]), sq);
                                }
                            }
                        }
                    }
                }
            }
        },
        |mut s: WState| {
            for (scr, _) in s.cols.drain(..) {
                crate::scan::scratch_park(scr);
            }
            (s.survivors, s.accs, s.eaccs, s.faccs, s.fcnt)
        },
    );
    let mut count = 0u64;
    let mut accs = vec![ColAcc::default(); dcols.len()];
    let mut eaccs = vec![ColAcc::default(); expr_legs.len()];
    let mut faccs = vec![ColAcc::default(); flt_legs.len()];
    let mut fcnts = vec![0u64; flt_legs.len()];
    for (survivors, s_accs, s_eaccs, s_faccs, s_fcnt) in &states {
        count += survivors;
        for (a, o) in accs.iter_mut().zip(s_accs) {
            a.merge(o);
        }
        for (a, o) in eaccs.iter_mut().zip(s_eaccs) {
            a.merge(o);
        }
        for (a, o) in faccs.iter_mut().zip(s_faccs) {
            a.merge(o);
        }
        for (a, o) in fcnts.iter_mut().zip(s_fcnt) {
            *a += o;
        }
    }
    // Leg index -> its expr cell (answer currency).
    let eacc_of = |ai: usize| -> Option<&ColAcc> {
        expr_legs.iter().position(|&(li, ..)| li == ai).map(|ei| &eaccs[ei])
    };
    // [aggqual] leg index -> its FILTER cell / survivor count.
    let flt_of = |ai: usize| -> Option<usize> {
        flt_legs.iter().position(|&(li, _)| li == ai)
    };

    // Un-embed a word-key min/max per the column's face (the metadata
    // stencil's answer law; Fixed/varlena faces refused at lowering).
    let word_answer = |a: &AggSpec, di: usize, key: Option<i64>| -> AnswerCol {
        match faces_v[di] {
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
    let out_cols: Vec<AnswerCol> = node
        .agg
        .iter()
        .enumerate()
        .map(|(ai, a)| match a.op {
            AggOp::CountStar => {
                // [aggqual] count(*) FILTER answers the LEG's survivor
                // count; the plain leg keeps the statement count.
                let c = flt_of(ai).map(|fi| fcnts[fi]).unwrap_or(count);
                AnswerCol::i64s(a.out, vec![c as i64])
            }
            AggOp::Sum => {
                // [P4-1] expr legs read their per-leg cell; [aggqual]
                // filtered legs their filter cell.
                let f = flt_of(ai)
                    .map(|fi| &faccs[fi])
                    .or_else(|| eacc_of(ai))
                    .unwrap_or_else(|| &accs[agg_di[ai].expect("sum has a column")]);
                let mut c = AnswerCol::i128s(a.out, vec![f.sum]);
                if f.n == 0 {
                    c.validity = Validity::Mask(vec![false]);
                }
                c
            }
            AggOp::SumShifted => {
                // SUM(col + k) = Σcol + k·n over NON-NULL survivors
                // (NULL + k is NULL — the strict law).
                let f = &accs[agg_di[ai].expect("sum-shifted has a column")];
                let v = f.sum + (a.k as i64 as i128) * (f.n as i128);
                let mut c = AnswerCol::i128s(a.out, vec![v]);
                if f.n == 0 {
                    c.validity = Validity::Mask(vec![false]);
                }
                c
            }
            AggOp::Avg => {
                let f = flt_of(ai)
                    .map(|fi| &faccs[fi])
                    .or_else(|| eacc_of(ai))
                    .unwrap_or_else(|| &accs[agg_di[ai].expect("avg has a column")]);
                // Expr legs always render exact: the fused product sums
                // past 2^53 well inside witnessed domains.
                let exact = a.avg_exact() || a.expr.is_some();
                AnswerCol::ratios(a.out, vec![(f.sum, f.n as i64)], exact)
            }
            AggOp::Min => {
                let di = agg_di[ai].expect("min has a column");
                let f = flt_of(ai).map(|fi| &faccs[fi]).unwrap_or(&accs[di]);
                word_answer(a, di, f.min)
            }
            AggOp::Max => {
                let di = agg_di[ai].expect("max has a column");
                let f = flt_of(ai).map(|fi| &faccs[fi]).unwrap_or(&accs[di]);
                word_answer(a, di, f.max)
            }
            AggOp::VarSamp | AggOp::VarPop | AggOp::StddevSamp | AggOp::StddevPop => {
                let f = flt_of(ai)
                    .map(|fi| &faccs[fi])
                    .unwrap_or(&accs[agg_di[ai].expect("var/stddev has a column")]);
                let kind = crate::answer::MomentKind::of_op(a.op).expect("moment op");
                crate::answer::AnswerCol::moments(
                    a.out,
                    kind,
                    vec![(f.n as i64, f.sum, f.sumsq)],
                )
            }
            AggOp::BitAnd => {
                let f = flt_of(ai)
                    .map(|fi| &faccs[fi])
                    .unwrap_or(&accs[agg_di[ai].expect("bit col")]);
                AnswerCol::i64s_opt(a.out, vec![f.band])
            }
            AggOp::BitOr => {
                let f = flt_of(ai)
                    .map(|fi| &faccs[fi])
                    .unwrap_or(&accs[agg_di[ai].expect("bit col")]);
                AnswerCol::i64s_opt(a.out, vec![f.bor])
            }
            other => panic!("filtered_fold: unsupported agg {other:?} (lowering gate)"),
        })
        .collect();
    AnswerSet::from_cols(out_cols)
}

/// Can this granule contain a passing row, per its stats record? Generic:
/// the exact zone refuses the term, or (COMPUTED) every value is zero and
/// the term excludes zero.
#[inline]
fn stats_skip(t: &PredTerm, r: &StatsRecord, rows: u64) -> bool {
    if r.key_kind == KeyKind::Exact.as_u8() && !t.zone_may_pass(r.min_key, r.max_key) {
        return true;
    }
    r.flags & STATSF_COMPUTED != 0 && r.zero_count == rows && !t.eval(0)
}

// ---------------------------------------------------------------------------
// grouped CountStar over a dense domain (parametrized from the hot-shape elected
// kernel zoneskip_swar_part_pool)
// ---------------------------------------------------------------------------

fn grouped_count(ctx: &SqeCtx, node: &PlanNode, term: &PredTerm) -> AnswerSet {
    let (bank, pool) = (ctx.bank, ctx.pool);
    let col = node.cols[0];
    assert_eq!(node.params.group_cols, vec![col], "M0: group key == filtered col");
    let width = col_width(bank, col);
    // Stats face: standing face — never re-opened inside the timed region.
    let sv = ctx.faces.stats(bank, col);
    let AggTier::Dense { lo: dom_lo, hi: dom_hi } =
        agg_tier(&sv, node.params.slot_bytes, node.params.l2_bytes, pool.threads())
    else {
        panic!("M0: fused grouped count needs the dense agg tier (stats refused exact domain)")
    };
    let dom = (dom_hi - dom_lo + 1) as usize;
    let units = ctx.faces.walk(bank, col);
    // first unit index of each part (granules are part-ordered).
    let mut part_first = vec![usize::MAX; bank.parts.len()];
    for (i, &(pi, _, _, _)) in units.iter().enumerate() {
        if part_first[pi] == usize::MAX {
            part_first[pi] = i;
        }
    }
    // SWAR fused path only when the term is `<> 0` (the nonzero-count
    // face), the domain fits i16, AND the column is proven null-free —
    // the word face counts raw lanes, so a validity stream disqualifies
    // it (3VL law; the null-free proof is a cached metadata fact).
    let swar = term.op == CmpOp::Ne
        && term.lo == 0
        && dom_lo >= i16::MIN as i64
        && dom_hi <= i16::MAX as i64
        && bank.null_free(col);

    // Per-worker PERSISTENT scratch (thread_local): the pool workers own
    // their decode arenas + count slices across reps.
    let states = pool.run(
        bank.parts.len(),
        |_| vec![0u64; dom],
        |acc: &mut Vec<u64>, pi| {
            let first = part_first[pi];
            if first == usize::MAX {
                return;
            }
            with_gscratch(col, dom, |s| {
                let gc = bank.manifest.parts[pi].granule_count;
                // Honest-hot: the stats consult is INSIDE the timed
                // region, part-owned (the hot-shape elected shape).
                let ps = PartStats::open(bank, pi, col);
                if let Some(ps) = &ps {
                    if let Some(r) = ps.part() {
                        let prow: u64 =
                            (0..gc).map(|g| units[first + g as usize].2 as u64).sum();
                        if stats_skip(term, &r, prow) {
                            return;
                        }
                    }
                }
                let mut fi: Option<Option<FusedInts>> = None;
                for g in 0..gc {
                    let ui = first + g as usize;
                    let rows = units[ui].2 as usize;
                    let skip = ps
                        .as_ref()
                        .and_then(|ps| ps.granule(g))
                        .map(|r| stats_skip(term, &r, rows as u64))
                        .unwrap_or(false);
                    if skip {
                        continue;
                    }
                    if swar {
                        let f = fi.get_or_insert_with(|| FusedInts::open(bank, pi, col));
                        if let Some(f) = f {
                            f.count_nonzero_i16(g, rows, dom_lo, &mut s.counts);
                            continue;
                        }
                    }
                    let (fb, cc, counts) = (&mut s.scr, &mut s.cc, &mut s.counts);
                    let cur = cc.get(bank, pi);
                    let gv = fb.validity(cur, g, rows);
                    let d = fb.decode_full(cur, g, rows);
                    let d: &[u64] = unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) };
                    if gv.all_valid() {
                        // NOT NULL fast path: the PoC loop, byte-identical.
                        for &x in d {
                            let v = sx(x, width);
                            if term.eval(v) {
                                counts[(v - dom_lo) as usize] += 1;
                            }
                        }
                    } else {
                        // 3VL: NULL never passes (eval_v); NULL rows never
                        // touch the dense domain (WHERE filters them).
                        for (r, &x) in d.iter().enumerate() {
                            let v = sx(x, width);
                            if term.eval_v(v, fb.row_valid(r)) {
                                counts[(v - dom_lo) as usize] += 1;
                            }
                        }
                    }
                }
                for i in 0..dom {
                    acc[i] += s.counts[i] as u64;
                    s.counts[i] = 0;
                }
            });
        },
    );
    let mut counts = vec![0u64; dom];
    for st in &states {
        for i in 0..dom {
            counts[i] += st[i];
        }
    }
    render_dense(&counts, dom_lo, node)
}

/// Typed dense-count emit: (key, count) columns in final row order —
/// ordering/limit applied here (the one ordering law), text only at the
/// render seam.
fn render_dense(counts: &[u64], dom_lo: i64, node: &PlanNode) -> AnswerSet {
    let params = &node.params;
    let mut rows: Vec<(i64, u64)> = counts
        .iter()
        .enumerate()
        .filter(|&(_, &c)| c != 0)
        .map(|(i, &c)| (dom_lo + i as i64, c))
        .collect();
    match params.order {
        OrderBy::CountDesc => {
            rows.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)))
        }
        OrderBy::KeyAsc => rows.sort_unstable_by(|a, b| a.0.cmp(&b.0)),
        OrderBy::None => {}
        other => panic!("fused_filter_agg render: unsupported order {other:?}"),
    }
    let key_ty = node.ty_of(node.params.group_cols[0]);
    let window: Vec<(i64, u64)> =
        rows.into_iter().skip(params.offset).take(params.limit).collect();
    let keys: Vec<i64> = window.iter().map(|&(k, _)| k).collect();
    let cnts: Vec<i64> = window.iter().map(|&(_, c)| c as i64).collect();
    AnswerSet::from_cols(vec![
        AnswerCol::i64s(key_ty, keys),
        AnswerCol::i64s(TypMeta::INT8, cnts),
    ])
}

// ---------------------------------------------------------------------------
// EmitMatches (parametrized from the hot-shape elected kernel sma_serial +
// the condcache replay discipline)
// ---------------------------------------------------------------------------

fn emit_matches(ctx: &SqeCtx, node: &PlanNode, term: &PredTerm) -> AnswerSet {
    let (bank, pool) = (ctx.bank, ctx.pool);
    let col = node.cols[0];
    let width = col_width(bank, col);
    let units = ctx.faces.walk(bank, col);
    let class = node.params.goal.claim_class.unwrap_or(ClaimClass::SkipDominated);

    if let Some(cache) = replay_cache(ctx, node) {
        // REPLAY: parallel over live granules, decode_sel for Rows
        // (serial replay was a measured 6-19x loss); the claim count
        // itself is elected from the live-unit count.
        let live: Vec<usize> = (0..units.len())
            .filter(|&i| !matches!(cache.v[i], CVerdict::Skip))
            .collect();
        let rows_est: u64 = live.iter().map(|&i| units[i].2 as u64).sum();
        let t = elect_threads(class, live.len(), rows_est, pool.threads());
        let work = |s: &mut (Scratch, CurCache, Vec<(usize, Vec<i64>)>), k: usize| {
            let ui = live[k];
            let (pi, g, rows, _) = units[ui];
            let (scr, cc, out) = (&mut s.0, &mut s.1, &mut s.2);
            let mut vals = Vec::new();
            match &cache.v[ui] {
                CVerdict::Skip => {}
                CVerdict::Rows(sel) => {
                    for &x in scr.decode_sel(cc.get(bank, pi), g, sel) {
                        vals.push(sx(x, width));
                    }
                }
                CVerdict::AllPass => {
                    for &x in scr.decode_full(cc.get(bank, pi), g, rows as usize) {
                        vals.push(sx(x, width));
                    }
                }
                CVerdict::Bitmap(w) => {
                    let sel: Vec<u16> = (0..rows as usize)
                        .filter(|&r| w[r >> 6] >> (r & 63) & 1 != 0)
                        .map(|r| r as u16)
                        .collect();
                    for &x in scr.decode_sel(cc.get(bank, pi), g, &sel) {
                        vals.push(sx(x, width));
                    }
                }
            }
            out.push((ui, vals));
        };
        let mut per_unit: Vec<(usize, Vec<i64>)> = if t <= 1 {
            let mut s = (crate::scan::scratch_fetch(), CurCache::new(col), Vec::new());
            for k in 0..live.len() {
                work(&mut s, k);
            }
            crate::scan::scratch_park(s.0);
            s.2
        } else {
            pool.run_finish(
                live.len(),
                |_| (crate::scan::scratch_fetch(), CurCache::new(col), Vec::new()),
                work,
                |s| {
                    crate::scan::scratch_park(s.0);
                    s.2
                },
            )
            .into_iter()
            .flatten()
            .collect()
        };
        per_unit.sort_unstable_by_key(|&(ui, _)| ui);
        return render_emit(per_unit, node);
    }

    // COLD/HONEST: flat-SMA zone consult (standing face), survivor decode,
    // fused emit; verdicts recorded as the loop runs (fill-on-cold is a
    // byproduct, not a second pass). The elected shape at low survivor
    // counts is SERIAL (the hot-shape parallelism election).
    let sma = ctx.faces.sma(bank, col);
    // [psma-consume] §8.2 candidate-slice face (None = kill switch /
    // uncovered column; the fused loop scans full granules then).
    let pface = ctx.faces.psma(bank, col);
    let pface = &pface;
    let scan: Vec<usize> = (0..units.len())
        .filter(|&i| term.zone_may_pass(sma.mins[i], sma.maxs[i]))
        .collect();
    let rows_est: u64 = scan.iter().map(|&i| units[i].2 as u64).sum();
    let t = elect_threads(class, scan.len(), rows_est, pool.threads());
    let mut verdicts: Vec<CVerdict> = (0..units.len()).map(|_| CVerdict::Skip).collect();
    let work = |s: &mut (Scratch, CurCache, Vec<(usize, Vec<i64>, CVerdict)>), k: usize| {
        let ui = scan[k];
        let (pi, g, rows, _) = units[ui];
        let rows = rows as usize;
        let (scr, cc, out) = (&mut s.0, &mut s.1, &mut s.2);
        let cur = cc.get(bank, pi);
        let gv = scr.validity(cur, g, rows);
        let d = scr.decode_full(cur, g, rows);
        let d: &[u64] = unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) };
        let all_valid = gv.all_valid();
        // [psma-consume] Zone said maybe: one candidate-slice probe per
        // granule; the fused count/gather loops run inside the window
        // only (rows outside are excluded without evaluation).
        let (rlo, rhi) = crate::psmaface::narrow(
            (0, rows),
            pface.as_ref().and_then(|pf| {
                pf.slice(pi, g, rows as u32, sma.mins[ui], sma.maxs[ui], term)
            }),
        );
        // [psma-consume, oracle] slice-complement emptiness gate.
        #[cfg(feature = "oracle")]
        crate::psmaface::oracle_check_complement(
            term,
            rlo,
            rhi,
            rows,
            |r| all_valid || scr.row_valid(r),
            |r| sx(d[r], width),
        );
        // Branchless count first (most scanned granules have zero
        // survivors); survivor lists are built only when hits exist.
        // NOT NULL lanes keep the PoC loop (all_valid hoisted).
        let mut hits = 0u64;
        if all_valid {
            for &x in &d[rlo..rhi] {
                hits += term.eval(sx(x, width)) as u64;
            }
        } else {
            for (r, &x) in d[rlo..rhi].iter().enumerate().map(|(i, x)| (rlo + i, x)) {
                hits += term.eval_v(sx(x, width), scr.row_valid(r)) as u64;
            }
        }
        let mut vals = Vec::new();
        let mut sel: Vec<u16> = Vec::new();
        if hits > 0 {
            for (r, &x) in d[rlo..rhi].iter().enumerate().map(|(i, x)| (rlo + i, x)) {
                let v = sx(x, width);
                if term.eval_v(v, all_valid || scr.row_valid(r)) {
                    vals.push(v);
                    sel.push(r as u16);
                }
            }
        }
        // A granule with any NULL can never encode AllPass: sel excludes
        // NULL rows, so `sel.len() == rows` is unreachable there — the
        // encode law stays sound under 3VL.
        out.push((ui, vals, CVerdict::encode(sel, rows)));
    };
    let results: Vec<(usize, Vec<i64>, CVerdict)> = if t <= 1 {
        let mut s = (crate::scan::scratch_fetch(), CurCache::new(col), Vec::new());
        for k in 0..scan.len() {
            work(&mut s, k);
        }
        crate::scan::scratch_park(s.0);
        s.2
    } else {
        pool.run_finish(
            scan.len(),
            |_| (crate::scan::scratch_fetch(), CurCache::new(col), Vec::new()),
            work,
            |s| {
                crate::scan::scratch_park(s.0);
                s.2
            },
        )
        .into_iter()
        .flatten()
        .collect()
    };
    let mut per_unit: Vec<(usize, Vec<i64>)> = Vec::with_capacity(results.len());
    let mut survivors = 0u64;
    for (ui, vals, verdict) in results {
        survivors += vals.len() as u64;
        verdicts[ui] = verdict;
        per_unit.push((ui, vals));
    }
    publish_cache(ctx, node, units.clone(), verdicts, survivors);
    per_unit.sort_unstable_by_key(|&(ui, _)| ui);
    render_emit(per_unit, node)
}

/// Typed emit: one column of matching values + the row-count trailer the
/// answers of record carry.
fn render_emit(per_unit: Vec<(usize, Vec<i64>)>, node: &PlanNode) -> AnswerSet {
    let hits: u64 = per_unit.iter().map(|(_, v)| v.len() as u64).sum();
    let mut vals: Vec<i64> = Vec::with_capacity(hits as usize);
    for (_, v) in &per_unit {
        vals.extend_from_slice(v);
    }
    let ty = node.agg[0].out;
    let mut a = AnswerSet::from_cols(vec![AnswerCol::i64s(ty, vals)]);
    a.note = Some(crate::render::footer_rows(hits));
    a
}
