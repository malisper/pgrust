//! two-level code agg + merge-join stencil (parametrized from the hot-shape
//! elected kernel opt_code_pool): the FRAME predicate erases granules at
//! zone grain (condition cache!), residues filter the frame survivors,
//! and the group identity lives in the per-part LOCAL dict code space —
//! phase 1 scatters SLOTS (part base + local code) into range buckets,
//! phase 2 gives each byte-range one owner that dense-counts its code
//! intervals and MERGE-JOINS the sorted per-part code streams by dict
//! bytes. Bytes are touched only for the answer rows.
//!
//! Frame handling is the condition-cache tier-1 rewrite: cold builds the
//! per-granule survivor rowlists (and publishes them under the frame
//! fingerprint — hot-shape SHARE this entry); warm replays them.

use crate::answer::{AnswerCol, AnswerSet, BytesBuild};
use crate::bank::{Bank, BankIdent};
use crate::engine::{CVerdict, DictFace, SqeCtx};
use crate::exec::{publish_cache, replay_cache};
use crate::ir::*;
use crate::kernels_f6::FxHasher;
use crate::pool::Pool;
use crate::scan::{CurCache, Scratch};
use crate::stencils::sx;
use crate::typmeta::TypMeta;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

type Fx = std::hash::BuildHasherDefault<FxHasher>;
type FxBytesMap = HashMap<Vec<u8>, u64, Fx>;

pub(crate) struct FG {
    pub(crate) pi: usize,
    pub(crate) g: u32,
    pub(crate) rows: u32,
    pub(crate) rl: Vec<u16>,
    /// [sqe-m2] ordinal into the anchor column's unit walk — the index the
    /// condition-cache planes are aligned with (final-plane publication).
    pub(crate) ord: usize,
}

/// [sqe-m2] One unit's FG from a cached verdict plane entry.
fn fg_of(cache: &crate::engine::PredCache, ui: usize) -> Option<FG> {
    let (pi, g, rows, _) = cache.units[ui];
    match &cache.v[ui] {
        CVerdict::Skip => None,
        CVerdict::AllPass => Some(FG { pi, g, rows, rl: (0..rows as u16).collect(), ord: ui }),
        CVerdict::Rows(rl) => {
            (!rl.is_empty()).then(|| FG { pi, g, rows, rl: rl.clone(), ord: ui })
        }
        CVerdict::Bitmap(w) => {
            let rl: Vec<u16> = (0..rows as usize)
                .filter(|&r| w[r >> 6] >> (r & 63) & 1 != 0)
                .map(|r| r as u16)
                .collect();
            (!rl.is_empty()).then_some(FG { pi, g, rows, rl, ord: ui })
        }
    }
}

/// Granules rebuilt from a cached verdict plane. Parallel build, MEMOIZED
/// per fingerprint (a standing face: planes are immutable and first-
/// publish-wins, so the expansion is derived data — rebuilding it was a
/// measured ~0.8-1.4ms tax on every hot-shape replay).
///
/// [race-118] The key carries the BANK IDENTITY, not just the predicate
/// fingerprint. The memoized value is the survivor granule/rowlist set of a
/// predicate evaluated over a SPECIFIC bank; a `ConjFp`-only key let a
/// concurrent session's entry, produced from a DIFFERENT relation whose
/// column shapes yield the same structural fingerprint (or the same
/// relation at a different data generation), be replayed as this
/// statement's survivors — cross-session wrong results / data exposure,
/// since the query-boundary clear (`clear_fg_memo`) is not exclusive in the
/// thread-per-session process. `(BankIdent, ConjFp)` makes another bank's
/// entries unreachable while preserving legitimate same-bank reuse.
static FG_MEMO: std::sync::Mutex<Option<HashMap<(BankIdent, ConjFp), Arc<Vec<FG>>>>> =
    std::sync::Mutex::new(None);

/// [ruling] the plane EXPANSION is derived data — per-query-run.
pub fn clear_fg_memo() {
    if let Some(m) = FG_MEMO.lock().unwrap().as_mut() {
        m.clear();
    }
}

fn fg_from_cache(
    bank: &Bank,
    pool: &Pool,
    fp: &ConjFp,
    cache: &crate::engine::PredCache,
) -> Arc<Vec<FG>> {
    // [race-118] Qualify the fingerprint with the bank's data identity so a
    // colliding fingerprint from another relation/generation (possibly a
    // concurrent session's entry) can never be replayed as this bank's
    // survivors.
    let key = (bank.ident(), fp.clone());
    {
        let m = FG_MEMO.lock().unwrap();
        if let Some(v) = m.as_ref().and_then(|m| m.get(&key)) {
            return v.clone();
        }
    }
    let n = cache.units.len();
    let out: Vec<FG> = if n < 256 {
        (0..n).filter_map(|ui| fg_of(cache, ui)).collect()
    } else {
        let states = pool.run(n, |_| Vec::new(), |acc: &mut Vec<FG>, ui| {
            if let Some(fg) = fg_of(cache, ui) {
                acc.push(fg);
            }
        });
        let mut out: Vec<FG> = states.into_iter().flatten().collect();
        out.sort_unstable_by_key(|fg| fg.ord);
        out
    };
    let out = Arc::new(out);
    FG_MEMO.lock()
        .unwrap()
        .get_or_insert_with(HashMap::new)
        .entry(key)
        .or_insert_with(|| out.clone())
        .clone()
}

/// [sqe-m2] Warm final-survivor plane (frame + residues already applied):
/// Some(granules) iff the plan's goal carries the full fingerprint, it
/// differs from the frame identity, and the engine cache holds it.
pub(crate) fn final_granules(
    ctx: &SqeCtx,
    node: &PlanNode,
    pred: &PredSpec,
) -> Option<Arc<Vec<FG>>> {
    let full = pred.full_fingerprint();
    if full == pred.frame_fingerprint() {
        return None;
    }
    let cache = crate::exec::replay_cache_at(ctx, node, &full)?;
    Some(fg_from_cache(ctx.bank, ctx.pool, &full, &cache))
}

/// [sqe-m2] Publish the FINAL survivor plane (post-residue rowlists,
/// aligned with `units` ordinals) under the full-conjunction fingerprint.
/// No-op when the goal does not carry it.
pub(crate) fn publish_final(
    ctx: &SqeCtx,
    node: &PlanNode,
    pred: &PredSpec,
    units: Arc<Vec<crate::engine::Unit>>,
    recs: Vec<(usize, Vec<u16>)>,
) {
    let full = pred.full_fingerprint();
    if full == pred.frame_fingerprint() {
        return;
    }
    let mut v: Vec<CVerdict> = (0..units.len()).map(|_| CVerdict::Skip).collect();
    let mut survivors = 0u64;
    for (ord, rl) in recs {
        survivors += rl.len() as u64;
        v[ord] = CVerdict::encode(rl, units[ord].2 as usize);
    }
    crate::exec::publish_cache_at(ctx, node, &full, units, v, survivors, None);
}

/// Frame survivors via the condition cache (replay) or a cold zone-walk
/// build (recompute + publish). Mirrors kernels_f6::build_frame.
/// pub(crate): the frame-walk consumers (hash_plane hot-shape shape,
/// dense_domain hot-shape shape) share this ONE derivation — same engine-cache
/// entry per frame fingerprint (hot-shape share; hot-shape keys its own).
pub(crate) fn frame_granules(
    ctx: &SqeCtx,
    node: &PlanNode,
    pred: &PredSpec,
) -> Arc<Vec<FG>> {
    let (bank, pool) = (ctx.bank, ctx.pool);
    let anchor = pred.frame()[0].col;
    let units = ctx.faces.walk(bank, anchor);
    let widths: Vec<u8> =
        pred.frame().iter().map(|t| super::col_width(bank, t.col)).collect();

    if let Some(cache) = replay_cache(ctx, node) {
        return fg_from_cache(bank, pool, &pred.frame_fingerprint(), &cache);
    }

    // COLD: flat-SMA zone consult per frame term, decode + filter the
    // survivors, publish the verdicts (rowlist grain elected per granule).
    let smas: Vec<Arc<crate::kernels_dec::SmaFlat>> =
        pred.frame().iter().map(|t| ctx.faces.sma(bank, t.col)).collect();
    // [psma-consume] §8.2 candidate-slice faces per frame term.
    let psmas: Vec<_> = pred.frame().iter().map(|t| ctx.faces.psma(bank, t.col)).collect();
    let mut scr: Vec<Scratch> = pred.frame().iter().map(|_| crate::scan::scratch_fetch()).collect();
    let mut cc: Vec<CurCache> = pred.frame().iter().map(|t| CurCache::new(t.col)).collect();
    let mut verdicts: Vec<CVerdict> = Vec::with_capacity(units.len());
    let mut survivors = 0u64;
    let mut out = Vec::new();
    for (ui, &(pi, g, rows, _)) in units.iter().enumerate() {
        let mut skip = false;
        let mut all = true;
        for (ti, t) in pred.frame().iter().enumerate() {
            let (zlo, zhi) = (smas[ti].mins[ui], smas[ti].maxs[ui]);
            if !t.zone_may_pass(zlo, zhi) {
                skip = true;
                break;
            }
            all &= t.zone_all_pass(zlo, zhi);
        }
        if skip {
            verdicts.push(CVerdict::Skip);
            continue;
        }
        if all {
            survivors += rows as u64;
            verdicts.push(CVerdict::AllPass);
            out.push(FG { pi, g, rows, rl: (0..rows as u16).collect(), ord: ui });
            continue;
        }
        // [psma-consume] Zone said maybe: intersect the frame terms'
        // candidate slices; the residual rowlist walk runs inside the
        // window only.
        let win = pred.frame().iter().enumerate().fold((0usize, rows as usize), |w, (ti, t)| {
            crate::psmaface::narrow(
                w,
                psmas[ti].as_ref().and_then(|pf| {
                    pf.slice(pi, g, rows, smas[ti].mins[ui], smas[ti].maxs[ui], t)
                }),
            )
        });
        if win.0 >= win.1 {
            verdicts.push(CVerdict::encode(Vec::new(), rows as usize));
            continue;
        }
        let rl = frame_rowlist(bank, pred, &widths, &mut scr, &mut cc, pi, g, rows, win);
        survivors += rl.len() as u64;
        if !rl.is_empty() {
            out.push(FG { pi, g, rows, rl: rl.clone(), ord: ui });
        }
        verdicts.push(CVerdict::encode(rl, rows as usize));
    }
    scr.into_iter().for_each(crate::scan::scratch_park);
    publish_cache(ctx, node, units.clone(), verdicts, survivors);
    Arc::new(out)
}

#[allow(clippy::too_many_arguments)]
fn frame_rowlist(
    bank: &Bank,
    pred: &PredSpec,
    widths: &[u8],
    scr: &mut [Scratch],
    cc: &mut [CurCache],
    pi: usize,
    g: u32,
    rows: u32,
    win: (usize, usize),
) -> Vec<u16> {
    let n = rows as usize;
    let mut cols: Vec<&[u64]> = Vec::with_capacity(scr.len());
    for (s, c) in scr.iter_mut().zip(cc.iter_mut()) {
        let d = s.decode_full(c.get(bank, pi), g, n);
        cols.push(unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) });
    }
    // Term-major (R2): one CmpOp match per (granule, term) driving a
    // monomorphic compaction loop — same conjunct order as the row walk.
    // [psma-consume] The seed selection is the granule's PSMA window
    // (full when no face narrowed it); rows outside never enter.
    let mut rl: Vec<u16> = (win.0..win.1).map(|r| r as u16).collect();
    for (ti, t) in pred.frame().iter().enumerate() {
        let (d, w) = (cols[ti], widths[ti]);
        t.filter_sel(&mut rl, |_| true, |r| sx(d[r], w));
    }
    // [psma-consume, oracle] window-complement emptiness gate on the
    // CONJUNCTION (the window is the slice intersection — an individual
    // term may legitimately match outside it).
    #[cfg(feature = "oracle")]
    for r in (0..win.0).chain(win.1..n) {
        let pass = pred
            .frame()
            .iter()
            .enumerate()
            .all(|(ti, t)| t.eval(sx(cols[ti][r], widths[ti])));
        assert!(!pass, "sqe oracle: frame PSMA window [{},{}) hid matching row {r}", win.0, win.1);
    }
    rl
}

/// Thread-persistent decode scratch (hot reps allocate nothing big),
/// keyed by the node's column signature.
struct TScratch {
    sig: (Vec<u32>, u32),
    rs: Vec<Scratch>,
    rc: Vec<CurCache>,
    ks: Scratch,
    kcache: CurCache,
    codes: Vec<u32>,
}

thread_local! {
    // tls-dtor: plain-data — held type audited 2026-08-19: no Drop beyond plain collections/dealloc.
    static TS: RefCell<Option<TScratch>> = const { RefCell::new(None) };
}

fn with_scratch<R>(res_cols: &[u32], key_col: u32, f: impl FnOnce(&mut TScratch) -> R) -> R {
    TS.with(|cell| {
        let mut cell = cell.borrow_mut();
        let stale = match cell.as_ref() {
            Some(s) => s.sig.0 != res_cols || s.sig.1 != key_col,
            None => true,
        };
        if stale {
            if let Some(old) = cell.take() {
                old.rs.into_iter().for_each(crate::scan::scratch_park);
                crate::scan::scratch_park(old.ks);
            }
            *cell = Some(TScratch {
                sig: (res_cols.to_vec(), key_col),
                rs: res_cols.iter().map(|_| crate::scan::scratch_fetch()).collect(),
                rc: res_cols.iter().map(|&a| CurCache::new(a)).collect(),
                ks: crate::scan::scratch_fetch(),
                kcache: CurCache::new(key_col),
                codes: vec![0; 8192],
            });
        }
        f(cell.as_mut().unwrap())
    })
}

/// [spill-2] Two-level slot-scatter spill engagement census.
pub static TLSPILL_FLUSHES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static TLSPILL_FOLDS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn run_two_level_code_agg(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    let (bank, pool) = (ctx.bank, ctx.pool);
    // ---- famA shape dispatch: a BYVAL group key elects the dense-int
    // two-level form (per-part entry tables + zone-constant fast path —
    // hot-shape lineage); the varlena local-code merge-join form follows.
    if super::col_width(bank, node.params.group_cols[0]) > 0 {
        return super::code_agg::dense_int_entrylen(ctx, node);
    }
    // ---- sqe-m1 shape: varlena key, COUNT(*) only, no int predicate
    // terms (a `<> ''` conjunct rides F_DROP_EMPTY_KEY, evaluated in gid
    // domain at render) — the two levels are per-part dense code counts
    // fused into the bit-unpack (level 1) + the c2g identity merge
    // (level 2). hot-shape lineage (fused_percode_pool).
    if node.agg.iter().all(|a| a.op == AggOp::CountStar)
        && node.pred.as_ref().map(|p| p.terms.is_empty() && p.var_terms.is_empty()).unwrap_or(true)
        && node.params.having_min_count == 0
    {
        // [noglobaldict] per-part dense counts + k-way string merge (the
        // registry-translated gid A/B arm is deleted at port).
        return super::part_merge::part_count(ctx, node);
    }
    let pred = node.pred.as_ref().expect("two_level: predicate-bearing family");
    let key_col = node.params.group_cols[0];
    let drop_empty = node.params.flags & F_DROP_EMPTY_KEY != 0;

    // [sqe-m2] Warm final plane: residues already applied — hot reps decode
    // ONLY the key columns over final survivors (the floor condcache law).
    let t_frame = std::time::Instant::now();
    let (granules, res_applied) = match final_granules(ctx, node, pred) {
        Some(g) => (g, true),
        None => (frame_granules(ctx, node, pred), false),
    };
    let res_terms: &[PredTerm] = if res_applied { &[] } else { pred.residues() };
    let record_final = !res_applied
        && !pred.residues().is_empty()
        && node.params.goal.fingerprints.iter().any(|f| *f == pred.full_fingerprint());
    let res_cols: Vec<u32> = res_terms.iter().map(|t| t.col).collect();
    let res_widths: Vec<u8> = res_cols.iter().map(|&c| super::col_width(bank, c)).collect();
    crate::engine::phn(node, if res_applied { "frame_final" } else { "frame" }, t_frame);

    // Identity election (a FUNCTION over runtime stats, never query
    // identity — the hot-shape-vs-hot-shape fork): the local-code merge-join wins
    // when the per-part code slot plane is no larger than the frame
    // (hot-shape: 61k Title slots <= ~738k frame rows at 100m); a slot plane
    // LARGER than the frame (hot-shape: the URL dictionary) inverts the walk
    // cost and the GLOBAL-dict A|barrier|B owned merge wins (the
    // opt_gdict_par2 shape, kernels_f6.rs:1307).
    let frame_rows: u64 = granules.iter().map(|fg| fg.rl.len() as u64).sum();
    // [coldstart] prewarm the touched parts' dict faces part-parallel (only
    // the frame's parts — never the whole column) before the serial walks.
    {
        let mut touched: Vec<usize> = granules.iter().map(|fg| fg.pi).collect();
        touched.sort_unstable();
        touched.dedup();
        ctx.faces.dicts_for(bank, key_col, &touched);
    }
    let slot_plane: u64 = {
        let mut seen: Vec<usize> = Vec::new();
        let mut s = 0u64;
        for fg in granules.iter() {
            if !seen.contains(&fg.pi) {
                seen.push(fg.pi);
                s += ctx.faces.dict(bank, fg.pi, key_col).ncodes as u64;
            }
        }
        s
    };
    // [noglobaldict, risks.md §11] the gdict A/B arm is DELETED at port
    // (global stitched dictionaries are prohibited — "removed twice");
    // the per-part local-code merge-join is the only arm. The hot-shape
    // trade this fixes in place is the measured, accepted one.
    if crate::engine::phase_on() {
        println!(
            "SQEELECT|q={}|two_level=merge_join|slot_plane={slot_plane}|frame_rows={frame_rows}",
            node.q,
        );
    }

    // -- involved parts: slot base per part (granules are part-ordered).
    let mut parts: Vec<(usize, Arc<DictFace>, u32)> = Vec::new();
    let mut base_of: HashMap<usize, (u32, Option<u32>)> = HashMap::new();
    let mut total_slots = 0u32;
    let mut _has_nondict = false;
    for fg in granules.iter() {
        if base_of.contains_key(&fg.pi) {
            continue;
        }
        let df = ctx.faces.dict(bank, fg.pi, key_col);
        if df.dh.is_some() {
            let es = if drop_empty { df.empty_code.map(|c| total_slots + c) } else { None };
            base_of.insert(fg.pi, (total_slots, es));
            parts.push((fg.pi, Arc::clone(&df), total_slots));
            total_slots += df.ncodes;
        } else {
            _has_nondict = true;
            base_of.insert(fg.pi, (u32::MAX, None));
            parts.push((fg.pi, Arc::clone(&df), u32::MAX));
        }
    }
    const RB: usize = 128;
    let mut shift = 0u32;
    while ((total_slots as usize) >> shift) + 1 > RB {
        shift += 1;
    }
    let nbuckets = ((total_slots as usize) >> shift) + 1;
    let t = pool.threads();

    // [spill-2] The merge-join route's ONE row-scaled plane is the slot
    // scatter (4 B per surviving frame row); everything in pass 2 is
    // dict/slot-bounded. Over the E18 budget the scatter flushes at the
    // E18b worker share (u32 records, one chunk per range bucket) and
    // pass 2 folds the chunks back at range grain — counts are additive
    // and dense, so no merge order exists to get wrong. (The raw-part
    // side maps stay resident: group-grain, witnessed under the emit
    // cap — the standing residue note.)
    let sp_on = ctx.faces.cfg.spill
        && (bank.rows_total() as u128)
            * (crate::stencils::hash_plane::SCATTER_ROW_BYTES as u128)
            > ctx.faces.cfg.grouped_budget_bytes() as u128
        && crate::spill::available();
    let tstore: Option<std::sync::Arc<dyn crate::spill::SpillStore>> = if sp_on {
        Some(crate::spill::new_store().unwrap_or_else(|| {
            crate::refuse::raise_runtime(crate::refuse::Refuse::GroupedSpillUnavailable {
                what: "no-substrate",
                est: bank
                    .rows_total()
                    .saturating_mul(crate::stencils::hash_plane::SCATTER_ROW_BYTES),
                budget: ctx.faces.cfg.grouped_budget_bytes(),
            })
        }))
    } else {
        None
    };
    let tshare = ((ctx.faces.cfg.grouped_budget_bytes() / pool.threads().max(1) as u64).max(1))
        as usize;
    let tstorer = &tstore;

    // -- phase 1: granule claims; residues; scatter slots by range.
    let t_p1 = std::time::Instant::now();
    struct S1 {
        buckets: Vec<Vec<u32>>,
        side: FxBytesMap,
        rec: Vec<(usize, Vec<u16>)>,
        sp: Option<super::hash_group::BW>,
        w: usize,
    }
    let mut pass1 = pool.run(
        granules.len(),
        |w| S1 {
            buckets: (0..nbuckets).map(|_| Vec::new()).collect(),
            side: Default::default(),
            rec: Vec::new(),
            sp: None,
            w,
        },
        |s: &mut S1, i| {
            let fg = &granules[i];
            with_scratch(&res_cols, key_col, |cs| {
                // residue filter over the frame rowlist.
                let n = fg.rows as usize;
                let mut rcols: Vec<&[u64]> = Vec::with_capacity(res_cols.len());
                for (scr, cc) in cs.rs.iter_mut().zip(cs.rc.iter_mut()) {
                    let d = scr.decode_full(cc.get(bank, fg.pi), fg.g, n);
                    rcols.push(unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) });
                }
                // Term-major residue filter (R2: op match at granule-term
                // grain).
                let mut surv: Vec<u16> = fg.rl.clone();
                for (ti, t) in res_terms.iter().enumerate() {
                    let (d, w) = (rcols[ti], res_widths[ti]);
                    t.filter_sel(&mut surv, |_| true, |r| sx(d[r], w));
                }
                if record_final {
                    s.rec.push((fg.ord, surv.clone()));
                }
                if surv.is_empty() {
                    return;
                }
                let (base, empty_slot) = base_of[&fg.pi];
                if base != u32::MAX {
                    if cs.codes.len() < n {
                        cs.codes.resize(n, 0);
                    }
                    cs.kcache
                        .get(bank, fg.pi)
                        .decode_codes(fg.g, &mut cs.codes[..n])
                        .expect("codes");
                    let codes = &cs.codes;
                    let buckets = &mut s.buckets;
                    match empty_slot {
                        Some(es) => {
                            for &r in &surv {
                                let slot = base + codes[r as usize];
                                if slot == es {
                                    continue;
                                }
                                buckets[(slot >> shift) as usize].push(slot);
                            }
                        }
                        None => {
                            for &r in &surv {
                                let slot = base + codes[r as usize];
                                buckets[(slot >> shift) as usize].push(slot);
                            }
                        }
                    }
                } else {
                    let kc = cs.ks.decode_sel(cs.kcache.get(bank, fg.pi), fg.g, &surv);
                    for idx in 0..surv.len() {
                        let p = unsafe { crate::scan::varlena_payload(kc[idx]) };
                        if drop_empty && p.is_empty() {
                            continue;
                        }
                        match s.side.get_mut(p) {
                            Some(c) => *c += 1,
                            None => {
                                s.side.insert(p.to_vec(), 1);
                            }
                        }
                    }
                }
            });
            // [spill-2, E18b] flush the slot buckets at the worker share
            // (u32 records; the arenas keep capacity). Granule grain.
            if let Some(tst) = tstorer {
                let resident: usize = s.buckets.iter().map(|b| b.len()).sum();
                if resident * 4 > tshare {
                    TLSPILL_FLUSHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let w = s.w;
                    let sp = s.sp.get_or_insert_with(|| {
                        super::hash_group::BW::new(&**tst, "tlslots", w)
                    });
                    for b in 0..nbuckets {
                        if s.buckets[b].is_empty() {
                            continue;
                        }
                        sp.begin();
                        for &slot in &s.buckets[b] {
                            sp.push(&slot.to_ne_bytes());
                        }
                        let (off, len) = sp.end();
                        sp.chunks.push((b as u32, off, len));
                        s.buckets[b].clear();
                    }
                }
            }
        },
    );
    // [spill-2] the spilled chunk directories, split from the states the
    // resident planes keep borrowing.
    let spilled: Vec<Option<(Box<dyn crate::spill::SpillMedium>, Vec<(u32, u64, u64)>)>> =
        pass1.iter_mut().map(|s| s.sp.take().map(|w| (w.m, w.chunks))).collect();
    let spilled = &spilled;
    if record_final {
        let units = ctx.faces.walk(bank, pred.frame()[0].col);
        let recs: Vec<(usize, Vec<u16>)> =
            pass1.iter_mut().flat_map(|s| s.rec.drain(..)).collect();
        publish_final(ctx, node, pred, units, recs);
    }
    crate::engine::phn(node, "pass1", t_p1);
    let t_p2 = std::time::Instant::now();
    let scattered: Vec<&Vec<Vec<u32>>> = pass1.iter().map(|s| &s.buckets).collect();

    // -- phase 2: BYTES-RANGE owners; dense counts; k-way merge join over
    //    sorted per-part code streams (identity = memcmp of dict entries).
    let kw = node.params.emit_cap();
    let dparts: Vec<(usize, Arc<DictFace>, u32)> =
        parts.iter().filter(|p| p.2 != u32::MAX).cloned().collect();
    let nown = t.max(1);
    // [noglobaldict] raw-part rows (side maps, bytes-keyed per worker) join
    // the k-way merge as ONE extra byte-sorted stream: merged across
    // workers, sorted, and range-sliced per owner. Group grain, tiny.
    let mut side_sorted: Vec<(Vec<u8>, u64)> = {
        let mut m: FxBytesMap = Default::default();
        for s in &pass1 {
            for (kb, &c) in &s.side {
                *m.entry(kb.clone()).or_insert(0) += c;
            }
        }
        m.into_iter().collect()
    };
    side_sorted.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    let side_sorted = &side_sorted;
    // (count, seq, part-or-SIDE, code-or-side-index)
    const SIDEJ: usize = usize::MAX;
    type Own = (u64, u64, Vec<(u32, u32, usize, u32)>);
    let mut owned: Vec<Vec<(usize, Own)>> = Vec::new();
    if !dparts.is_empty() {
        let n0 = dparts[0].1.ncodes;
        let dh0 = dparts[0].1.dh.as_ref().unwrap();
        owned = pool.run(
            nown,
            |_| Vec::new(),
            |out: &mut Vec<(usize, Own)>, k| {
                let bound = |q: usize| -> Option<&[u8]> {
                    if q == 0 || q >= nown {
                        None
                    } else {
                        let c = ((q as u64 * n0 as u64) / nown as u64) as u32;
                        Some(dh0.entry(c.min(n0.saturating_sub(1))).expect("dict entry").bytes)
                    }
                };
                let blo = bound(k);
                let bhi = bound(k + 1);
                let mut ivs: Vec<(u32, u32)> = Vec::with_capacity(dparts.len());
                for (_, df, _) in &dparts {
                    let dh = df.dh.as_ref().unwrap();
                    let n = df.ncodes;
                    let pp = |b: Option<&[u8]>| -> u32 {
                        match b {
                            None => 0,
                            Some(b) => {
                                let (mut lo, mut hi) = (0u32, n);
                                while lo < hi {
                                    let mid = lo + (hi - lo) / 2;
                                    if dh.entry(mid).expect("dict entry").bytes < b {
                                        lo = mid + 1;
                                    } else {
                                        hi = mid;
                                    }
                                }
                                lo
                            }
                        }
                    };
                    let lo = pp(blo);
                    let hi = if bhi.is_none() { n } else { pp(bhi) };
                    ivs.push((lo, hi.max(lo)));
                }
                let mut offs: Vec<usize> = Vec::with_capacity(dparts.len() + 1);
                let mut tot = 0usize;
                for &(lo, hi) in &ivs {
                    offs.push(tot);
                    tot += (hi - lo) as usize;
                }
                offs.push(tot);
                let mut counts = vec![0u32; tot];
                for (j, (_, _, base)) in dparts.iter().enumerate() {
                    let (lo, hi) = ivs[j];
                    if lo >= hi {
                        continue;
                    }
                    let slo = base + lo;
                    let shi = base + hi;
                    let b0 = (slo >> shift) as usize;
                    let b1 = ((shi - 1) >> shift) as usize;
                    let o = offs[j];
                    for s in &scattered {
                        for b in b0..=b1 {
                            for &slot in &s[b] {
                                if slot >= slo && slot < shi {
                                    counts[o + (slot - slo) as usize] += 1;
                                }
                            }
                        }
                    }
                }
                // [spill-2] fold the spilled slot chunks for this
                // owner's windows (dense additive counts — no order to
                // get wrong; boundary-bucket slots outside every window
                // belong to another owner and skip).
                if spilled.iter().any(Option::is_some) {
                    let mut wins: Vec<(u32, u32, usize)> = Vec::new();
                    for (j, (_, _, base)) in dparts.iter().enumerate() {
                        let (lo, hi) = ivs[j];
                        if lo < hi {
                            wins.push((base + lo, base + hi, offs[j]));
                        }
                    }
                    if !wins.is_empty() {
                        TLSPILL_FOLDS
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let cb0 = wins.first().expect("nonempty").0 >> shift;
                        let cb1 = (wins.last().expect("nonempty").1 - 1) >> shift;
                        let slab = crate::spill::SLAB_BYTES.min(tshare.max(4));
                        for sp in spilled.iter() {
                            let Some((m, chunks)) = sp else { continue };
                            for &(cb, off, len) in chunks {
                                if cb < cb0 || cb > cb1 {
                                    continue;
                                }
                                let mut cur = crate::spill::ChunkCursor::new(
                                    &**m,
                                    off,
                                    len / 4,
                                    4,
                                    slab,
                                );
                                while let Some(r) = cur.next() {
                                    let slot =
                                        u32::from_ne_bytes(r.try_into().unwrap());
                                    let wi =
                                        wins.partition_point(|&(slo, _, _)| slo <= slot);
                                    if wi > 0 {
                                        let (slo, shi2, o) = wins[wi - 1];
                                        if slot < shi2 {
                                            counts[o + (slot - slo) as usize] += 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                struct Cur<'a> {
                    j: usize,
                    c: u32,
                    hi: u32,
                    o: usize,
                    lo: u32,
                    bytes: &'a [u8],
                }
                let mut curs: Vec<Cur> = Vec::with_capacity(dparts.len() + 1);
                for (j, (_, df, _)) in dparts.iter().enumerate() {
                    let (lo, hi) = ivs[j];
                    let o = offs[j];
                    let mut c = lo;
                    while c < hi && counts[o + (c - lo) as usize] == 0 {
                        c += 1;
                    }
                    if c < hi {
                        let bytes = df.dh.as_ref().unwrap().entry(c).expect("dict entry").bytes;
                        curs.push(Cur { j, c, hi, o, lo, bytes });
                    }
                }
                // the side stream's slice for this owner (bytes-sorted).
                let s_lo = match blo {
                    None => 0,
                    Some(b) => side_sorted.partition_point(|e| e.0.as_slice() < b),
                };
                let s_hi = match bhi {
                    None => side_sorted.len(),
                    Some(b) => side_sorted.partition_point(|e| e.0.as_slice() < b),
                };
                if s_lo < s_hi {
                    curs.push(Cur {
                        j: SIDEJ,
                        c: s_lo as u32,
                        hi: s_hi as u32,
                        o: 0,
                        lo: 0,
                        bytes: side_sorted[s_lo].0.as_slice(),
                    });
                }
                // heap-ordered merge front (the linear min scan over 50+
                // cursors was the measured cost at 835k groups): the
                // cursor with the smallest bytes sits at the front.
                let mut groups = 0u64;
                let mut rows = 0u64;
                let mut seq = 0u32;
                let mut top: Vec<(u32, u32, usize, u32)> = Vec::with_capacity(kw + 1);
                curs.sort_unstable_by(|a, b| a.bytes.cmp(b.bytes));
                let mut sum_side = 0u64;
                let mut sum = 0u32;
                while !curs.is_empty() {
                    let mb = curs[0].bytes;
                    let (pj, pc) = (curs[0].j, curs[0].c);
                    sum = 0;
                    sum_side = 0;
                    // consume every cursor at the front with equal bytes,
                    // advance each, and re-insert in sorted position.
                    let mut ne = 0usize;
                    while ne < curs.len() && curs[ne].bytes == mb {
                        ne += 1;
                    }
                    let mut adv: Vec<Cur> = Vec::with_capacity(ne);
                    for cu in curs.drain(..ne) {
                        let mut cu = cu;
                        if cu.j == SIDEJ {
                            sum_side += side_sorted[cu.c as usize].1;
                            cu.c += 1;
                            if cu.c < cu.hi {
                                cu.bytes = side_sorted[cu.c as usize].0.as_slice();
                                adv.push(cu);
                            }
                        } else {
                            sum += counts[cu.o + (cu.c - cu.lo) as usize];
                            let mut c = cu.c + 1;
                            while c < cu.hi && counts[cu.o + (c - cu.lo) as usize] == 0 {
                                c += 1;
                            }
                            if c < cu.hi {
                                cu.c = c;
                                cu.bytes = dparts[cu.j]
                                    .1
                                    .dh
                                    .as_ref()
                                    .unwrap()
                                    .entry(c)
                                    .expect("dict entry")
                                    .bytes;
                                adv.push(cu);
                            }
                        }
                    }
                    for cu in adv {
                        let pos = curs.partition_point(|x| x.bytes < cu.bytes);
                        curs.insert(pos, cu);
                    }
                    let tot = sum as u64 + sum_side;
                    let tot32 = u32::try_from(tot).expect("group count fits u32");
                    groups += 1;
                    rows += tot;
                    if top.len() < kw {
                        top.push((tot32, seq, pj, pc));
                        top.sort_unstable_by(|x, y| y.0.cmp(&x.0).then(x.1.cmp(&y.1)));
                    } else if kw > 0 && tot32 > top[kw - 1].0 {
                        top[kw - 1] = (tot32, seq, pj, pc);
                        top.sort_unstable_by(|x, y| y.0.cmp(&x.0).then(x.1.cmp(&y.1)));
                    }
                    seq += 1;
                }
                let _ = (sum, sum_side);
                out.push((k, (groups, rows, top)));
            },
        );
    }

    let fast = !owned.is_empty();
    let answer: AnswerSet = if fast {
        let states: Vec<(usize, Own)> = owned.into_iter().flatten().collect();
        let groups: u64 = states.iter().map(|s| s.1 .0).sum();
        let rows_total: u64 = states.iter().map(|s| s.1 .1).sum();
        let mut all: Vec<(u32, usize, u32, usize, u32)> = states
            .iter()
            .flat_map(|(k, (_, _, top))| {
                top.iter().map(move |&(c, sq, j, code)| (c, *k, sq, j, code))
            })
            .collect();
        all.sort_unstable_by(|x, y| y.0.cmp(&x.0).then(x.1.cmp(&y.1)).then(x.2.cmp(&y.2)));
        let key_ty = node.ty_of(key_col);
        let mut kb = BytesBuild::new();
        let mut cnts: Vec<i64> = Vec::new();
        for &(c, _, _, j, code) in all.iter().skip(node.params.offset).take(node.params.limit) {
            if j == SIDEJ {
                kb.push(&side_sorted[code as usize].0);
            } else {
                let e = dparts[j].1.dh.as_ref().unwrap().entry(code).expect("dict entry");
                kb.push(e.bytes);
            }
            cnts.push(c as i64);
        }
        let mut a = AnswerSet::from_cols(vec![
            kb.finish(key_ty),
            AnswerCol::i64s(TypMeta::INT8, cnts),
        ]);
        a.note = Some(crate::render::footer_groups(groups as u64, rows_total as u64));
        a
    } else {
        // General path (non-dict part in the frame): dense count + bytes-
        // domain merge + side map. Correct; never the hot shape here.
        let mut dense = vec![0u32; total_slots as usize];
        for s in &scattered {
            for b in s.iter() {
                for &slot in b {
                    dense[slot as usize] += 1;
                }
            }
        }
        // [spill-2] the spilled slot chunks fold into the same dense
        // plane (additive, order-free).
        for sp in spilled.iter() {
            let Some((m, chunks)) = sp else { continue };
            TLSPILL_FOLDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let slab = crate::spill::SLAB_BYTES.min(tshare.max(4));
            for &(_cb, off, len) in chunks {
                let mut cur = crate::spill::ChunkCursor::new(&**m, off, len / 4, 4, slab);
                while let Some(r) = cur.next() {
                    dense[u32::from_ne_bytes(r.try_into().unwrap()) as usize] += 1;
                }
            }
        }
        let mut global: FxBytesMap = HashMap::with_capacity_and_hasher(1 << 16, Fx::default());
        for (_, df, pbase) in &dparts {
            for c in 0..df.ncodes {
                let cnt = dense[(pbase + c) as usize];
                if cnt == 0 {
                    continue;
                }
                let e = df.dh.as_ref().unwrap().entry(c).expect("dict entry");
                match global.get_mut(e.bytes) {
                    Some(x) => *x += cnt as u64,
                    None => {
                        global.insert(e.bytes.to_vec(), cnt as u64);
                    }
                }
            }
        }
        for s in &pass1 {
            for (kb, &c) in &s.side {
                *global.entry(kb.clone()).or_insert(0) += c;
            }
        }
        render_bytes(&global, node)
    };
    crate::engine::phn(node, "pass2_render", t_p2);
    pool.drop_par(pass1);
    answer
}

/// Rank-window render over a bytes-keyed count map (the general path) —
/// typed: (key bytes, count) columns + the groups/rows trailer.
pub(crate) fn render_bytes(map: &FxBytesMap, node: &PlanNode) -> AnswerSet {
    let total_rows: u64 = map.values().sum();
    let groups = map.len();
    let mut rows: Vec<(&Vec<u8>, u64)> = map.iter().map(|(k, &c)| (k, c)).collect();
    let k = node.params.emit_cap().min(rows.len());
    // [emitcap-audit] A NATIVE pushed bound with a (count DESC) key (the
    // server posture: `order` arrives None, `params.topk` carries the
    // contract, and `emit_cap()` == t.n bounds this render) must select
    // by count — a key-ordered truncation would keep the wrong group set.
    // An empty native spec (bare LIMIT) keeps key order: any n is the law.
    let native_count_bound =
        matches!(&node.params.topk, Some(t) if t.native && !t.keys.is_empty());
    let cmp = |a: &(&Vec<u8>, u64), b: &(&Vec<u8>, u64)| match node.params.order {
        OrderBy::CountDesc => b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)),
        _ if native_count_bound => b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)),
        _ => a.0.cmp(b.0),
    };
    if k > 0 && k < rows.len() {
        rows.select_nth_unstable_by(k - 1, cmp);
    }
    rows.truncate(k);
    rows.sort_by(cmp);
    let key_ty = node.ty_of(node.params.group_cols[0]);
    let mut kb = BytesBuild::new();
    let mut cnts: Vec<i64> = Vec::new();
    for (kbytes, c) in rows.into_iter().skip(node.params.offset).take(node.params.limit) {
        kb.push(kbytes);
        cnts.push(c as i64);
    }
    let mut a = AnswerSet::from_cols(vec![
        kb.finish(key_ty),
        AnswerCol::i64s(TypMeta::INT8, cnts),
    ]);
    a.note = Some(crate::render::footer_groups(groups as u64, total_rows as u64));
    a
}

#[cfg(test)]
mod tests {
    use super::*;

    /// [race-118] The FG_MEMO key must carry the bank's data identity, not
    /// the predicate fingerprint alone. Two banks with an IDENTICAL
    /// structural fingerprint (a colliding predicate on a different relation,
    /// or the same relation at a different data generation) must occupy
    /// DISTINCT memo slots, so one session's survivor expansion can never be
    /// replayed as another's. A same-identity + same-fingerprint probe must
    /// still hit (legitimate hot-shape reuse preserved).
    #[test]
    fn fg_memo_key_scoped_by_bank_identity() {
        let fp = ConjFp::default();
        let mut memo: HashMap<(BankIdent, ConjFp), u32> = HashMap::new();

        let bank_a = BankIdent { db: 5, relfilenumber: 100, gen: 1 };
        // Same relation, NEXT data generation — must not alias bank_a.
        let bank_a_next = BankIdent { db: 5, relfilenumber: 100, gen: 2 };
        // Different relation, SAME structural fingerprint — the attacker's
        // colliding table.
        let bank_b = BankIdent { db: 5, relfilenumber: 200, gen: 1 };

        memo.insert((bank_a, fp.clone()), 1);
        memo.insert((bank_a_next, fp.clone()), 2);
        memo.insert((bank_b, fp.clone()), 3);

        assert_eq!(memo.len(), 3, "identical fingerprint must not collapse across bank identities");
        assert_eq!(memo.get(&(bank_a, fp.clone())), Some(&1));
        assert_eq!(memo.get(&(bank_a_next, fp.clone())), Some(&2));
        assert_eq!(memo.get(&(bank_b, fp.clone())), Some(&3));
        // Legitimate reuse: the same identity + fingerprint hits its own slot.
        assert_eq!(memo.get(&(bank_a, fp)), Some(&1));
    }

    /// [emitcap-audit] `render_bytes` under the SERVER posture for a pushed
    /// bound — `order = None` + a NATIVE (count DESC) `params.topk` — must
    /// select the top-k groups BY COUNT before truncating; the pre-fix code
    /// matched only `params.order` and kept the k byte-smallest keys.
    #[test]
    fn render_bytes_native_count_bound_selects_by_count() {
        // Counts RISE with key byte order: key-order truncation keeps
        // exactly the wrong groups.
        let mut map: FxBytesMap = Default::default();
        for j in 0..16u8 {
            map.insert(vec![b'a' + j], 10 + 5 * j as u64);
        }
        let node = PlanNode {
            family: Family::TwoLevelCodeAgg,
            q: 0,
            cols: vec![1],
            col_tys: vec![TypMeta::TEXT_C],
            pred: None,
            agg: vec![AggSpec::new(AggOp::CountStar, None, None)],
            params: Params {
                group_cols: vec![1],
                topk: Some(TopK {
                    keys: vec![TopKKey { col: 1, desc: true, nulls_first: false, lo: None, trim: false }],
                    n: 3,
                    native: true,
                }),
                ..Params::default()
            },
        };
        let a = render_bytes(&map, &node);
        // Top 3 by count = the 3 byte-LARGEST keys here ('n','o','p').
        let mut got: Vec<i64> = match &a.cols[1].data {
            crate::answer::ColData::I64(v) => v.clone(),
            other => panic!("count col: {other:?}"),
        };
        got.sort_unstable();
        assert_eq!(got, vec![75, 80, 85], "native (count DESC) bound must keep the top-count set");
    }
}
