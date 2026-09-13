//! famA hash-plane owned-group SHAPES beyond the packed-2-byval form:
//! int keys, int+gid pairs, 128-bit string-hash keys, and the
//! dense-domain + distinct-leg form. Routed from
//! `hash_plane::run_hash_plane_owned_group` by key SHAPE (column widths +
//! agg set — schema/stats facts, never query identity). All elections are
//! functions over the standing faces; nothing opens StatsView or walks
//! granule geometry inside the timed region.

use crate::answer::{AnswerCol, AnswerSet, BytesBuild, ColData, Validity};
use crate::bank::Bank;
use crate::drivers::SelOrder;
use crate::engine::SqeCtx;
use crate::grouped::{hash64, radix_of, Cnt128, RADIX_P};
use crate::ir::*;
use crate::kernels_g::ColState;
use crate::kernels_u::{user_count_l2p, Arena, Tbl64, U1};
use crate::planner::{partition_count, run_collapse_witness};
use crate::scan::{CurCache, Scratch};
use crate::stencils::{col_width, sx};
use crate::typmeta::TypMeta;

fn sel_order(o: OrderBy) -> SelOrder {
    match o {
        // None = order-free plan (server rung C); any output order is legal.
        OrderBy::CountDesc | OrderBy::None => SelOrder::CountDescKeyAsc,
        OrderBy::KeyAsc => SelOrder::KeyAsc,
        other => panic!("hash_group: unsupported order {other:?}"),
    }
}

/// Sorted-insert selection cost grows with k: larger pushed bounds ride
/// collector mode + the answer-boundary trim instead.
const NATIVE_TOPK_MAX: usize = 1 << 16;

/// usize::MAX = no LIMIT: collect ALL groups (Sel128 collector mode).
fn topk(node: &PlanNode) -> usize {
    if let Some(t) = &node.params.topk {
        return if t.native && t.n <= NATIVE_TOPK_MAX { t.n } else { usize::MAX };
    }
    if node.params.limit == usize::MAX {
        return usize::MAX;
    }
    node.params.offset + node.params.limit
}

/// ndv estimate from the stats face, dict-census fallback (varlena cols).
fn ndv_face(ctx: &SqeCtx, attno: u32) -> usize {
    let bank = ctx.bank;
    let n = ctx.faces.stats(bank, attno).ndv_est_sum() as usize;
    if n > 0 {
        return n;
    }
    let census: usize =
        ctx.faces.dicts_all(bank, attno).iter().map(|df| df.ncodes as usize).sum();
    census.max(1 << 16)
}

/// Typed grouped-row sink: key_exprs then aggs in plan order, one
/// AnswerSet column per emitted field (the render_line ~11-site kill,
/// abi-kill-list.md §3). `key_val(col)` resolves a byval key element;
/// `key_text` the varlena element.
enum OutCol {
    I(TypMeta, Vec<i64>),
    B(TypMeta, BytesBuild),
    R(TypMeta, Vec<(i128, i64)>),
}

struct LineSink {
    cols: Vec<OutCol>,
    exprs: Vec<KeyExpr>,
}

impl LineSink {
    fn new(bank: &Bank, node: &PlanNode) -> LineSink {
        let exprs: Vec<KeyExpr> = if node.params.key_exprs.is_empty() {
            node.params.group_cols.iter().map(|&c| KeyExpr::Col(c)).collect()
        } else {
            node.params.key_exprs.clone()
        };
        let mut cols: Vec<OutCol> = Vec::new();
        for e in &exprs {
            match e {
                KeyExpr::Col(c) => {
                    if col_width(bank, *c) == 0 {
                        cols.push(OutCol::B(node.ty_of(*c), BytesBuild::new()));
                    } else {
                        cols.push(OutCol::I(node.ty_of(*c), Vec::new()));
                    }
                }
                KeyExpr::Const1 => cols.push(OutCol::I(TypMeta::INT4, Vec::new())),
                KeyExpr::MinusConst(c, _) => cols.push(OutCol::I(node.ty_of(*c), Vec::new())),
                KeyExpr::Minute(_) => {
                    panic!("hash_group LineSink: Minute keys render in their own shape")
                }
                other => panic!("hash_group LineSink: unsupported key expr {other:?}"),
            }
        }
        for a in &node.agg {
            match a.op {
                AggOp::CountStar | AggOp::CountDistinct => {
                    cols.push(OutCol::I(TypMeta::INT8, Vec::new()))
                }
                AggOp::Sum | AggOp::SumDistinct => cols.push(OutCol::I(a.out, Vec::new())),
                AggOp::Avg | AggOp::AvgDistinct => cols.push(OutCol::R(a.out, Vec::new())),
                other => panic!("hash_group LineSink: unsupported agg {other:?}"),
            }
        }
        LineSink { cols, exprs }
    }

    #[allow(clippy::too_many_arguments)]
    fn push(
        &mut self,
        node: &PlanNode,
        key_val: &dyn Fn(u32) -> i64,
        key_text: Option<&[u8]>,
        cnt: u64,
        sums: &dyn Fn(usize) -> u64,
        distinct: u64,
        dsum: i64,
    ) {
        let mut ci = 0usize;
        for e in &self.exprs.clone() {
            match (e, &mut self.cols[ci]) {
                (KeyExpr::Col(c), OutCol::B(_, b)) => {
                    let _ = c;
                    b.push(key_text.expect("text key element"));
                }
                (KeyExpr::Col(c), OutCol::I(_, v)) => v.push(key_val(*c)),
                (KeyExpr::Const1, OutCol::I(_, v)) => v.push(1),
                (KeyExpr::MinusConst(c, k), OutCol::I(_, v)) => v.push(key_val(*c) - k),
                _ => unreachable!("LineSink shape mismatch"),
            }
            ci += 1;
        }
        for (i, a) in node.agg.iter().enumerate() {
            match (&a.op, &mut self.cols[ci]) {
                (AggOp::CountStar, OutCol::I(_, v)) => v.push(cnt as i64),
                (AggOp::CountDistinct, OutCol::I(_, v)) => v.push(distinct as i64),
                (AggOp::Sum, OutCol::I(_, v)) => v.push(sums(i) as i64),
                (AggOp::Avg, OutCol::R(_, v)) => v.push((sums(i) as i64 as i128, cnt as i64)),
                // [aggqual] distinct folds: the pair-dedup's first-seen
                // sum + the distinct count (avg = dsum / distinct).
                (AggOp::SumDistinct, OutCol::I(_, v)) => v.push(dsum),
                (AggOp::AvgDistinct, OutCol::R(_, v)) => {
                    v.push((dsum as i128, distinct as i64))
                }
                _ => unreachable!("LineSink agg mismatch"),
            }
            ci += 1;
        }
    }

    /// The one-varlena-key NULL group (text128): a NULL key cell plus
    /// the CountStar legs; every other lane is outside this arm.
    fn push_null_key(&mut self, node: &PlanNode, cnt: u64) {
        match &mut self.cols[0] {
            OutCol::B(_, b) => b.push_null(),
            _ => unreachable!("LineSink null key on a non-varlena lane"),
        }
        for (i, a) in node.agg.iter().enumerate() {
            match (&a.op, &mut self.cols[1 + i]) {
                (AggOp::CountStar, OutCol::I(_, v)) => v.push(cnt as i64),
                _ => unreachable!("LineSink null-key agg mismatch"),
            }
        }
    }

    fn finish(self) -> AnswerSet {
        let cols: Vec<AnswerCol> = self
            .cols
            .into_iter()
            .map(|c| match c {
                OutCol::I(ty, v) => AnswerCol::i64s(ty, v),
                OutCol::B(ty, b) => b.finish(ty),
                OutCol::R(ty, pairs) => AnswerCol {
                    ty,
                    data: ColData::Ratio { pairs, exact: false },
                    validity: Validity::AllValid,
                },
            })
            .collect();
        AnswerSet::from_cols(cols)
    }
}

// ---------------------------------------------------------------------------
// int key, COUNT(*): user_count_l2p IS the parametric kernel —
// this shape is its election + render harness.
// ---------------------------------------------------------------------------

pub fn int_key(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    let (bank, pool) = (ctx.bank, ctx.pool);
    assert!(node.agg.iter().all(|a| a.op == AggOp::CountStar));
    let attno = node.params.group_cols[0];
    let units = ctx.faces.walk(bank, attno);
    let ndv = ndv_face(ctx, attno);
    let rc = run_collapse_witness(bank, attno, &units, ctx.faces.cfg.threads);
    let t = pool.threads();
    let order = sel_order(node.params.order);
    thread_local! {
        // [persist-rehome] KEYED BY POOL WIDTH: U1 states park plain
        // buckets only now (decode scratches ride the worker depot;
        // cursors are fetched per engagement and NEVER at rest — the
        // former attno key existed because parked ColState cursors were
        // column-bound, the cross-query arena-reuse rig failure, and it
        // still let a SAME-attno query on a different bank resurface the
        // old bank's cursor). A width change re-sizes both arenas (the
        // old cell indexed slots out of bounds on a wider pool).
        // tls-dtor: plain-data — arenas park Vec buckets only; cursors are never at rest here.
        static ARENAS: std::cell::RefCell<Option<(usize, Arena<U1>, Arena<Tbl64>)>> =
            const { std::cell::RefCell::new(None) };
    }
    let out = ARENAS.with(|cell| {
        let mut cell = cell.borrow_mut();
        let stale = match cell.as_ref() {
            Some((w, _, _)) => *w != t,
            None => true,
        };
        if stale {
            *cell = Some((t, Arena::new(t), Arena::new(t)));
        }
        let ar: Option<(&Arena<U1>, &Arena<Tbl64>)> = cell.as_ref().map(|(_, a, b)| (a, b));
        user_count_l2p(
            bank,
            pool,
            &units,
            attno,
            ndv,
            topk(node),
            order,
            node.params.l2_bytes,
            rc,
            ar,
        )
    });
    assert_eq!(out.rows_counted, bank.rows_total(), "q{}: rows must cover the bank", node.q);
    // [cap-retire] finalize answer-bytes law: the tuned count-only arm
    // is reachable by cap-retired (unwitnessed / over-cap) shapes, so
    // its answer plane prices under the same exact law before the render
    // sink materializes (the internal top vector is this arm's staging
    // plane — exact element bytes, counted rows).
    let stage = out.top.rows.first().map(std::mem::size_of_val).unwrap_or(0);
    super::hash_plane::check_answer_budget(ctx, node, out.top.rows.len() as u64, stage, false);
    let w = col_width(bank, attno);
    let mut sink = LineSink::new(bank, node);
    for &(k, c) in out.top.rows.iter().skip(node.params.offset) {
        sink.push(node, &|_| sx(k as u64, w), None, c, &|_| 0, 0, 0);
    }
    sink.finish()
}

// ---------------------------------------------------------------------------
// (int, gid) pair, COUNT(*): pair_count_l2p + the shared
// registry face (build_par — gid assignment identical to the oracle's).
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// 128-bit string-hash key, COUNT(*): dict parts pre-aggregate at
// ENTRY granularity; non-dict rows scatter (hash, locator); owners fold on
// the hash plane and resolve bytes lazily at the top-k admission boundary.
// ---------------------------------------------------------------------------

pub fn text128(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    use crate::fp::entry_fp128;
    let (bank, pool) = (ctx.bank, ctx.pool);
    assert!(node.agg.iter().all(|a| a.op == AggOp::CountStar));
    let attno = node.params.group_cols[0];
    let entries = ndv_face(ctx, attno);
    // hash-plane slot ≈ 32B, ×2 load ⇒ 64B/entry (the measured law inputs).
    let p = partition_count(entries, 32, node.params.l2_bytes, ctx.pool.threads());
    let pbits = p.trailing_zeros();
    let k = topk(node);
    // [fpcache] pass1 folds dict entries on the SAME Faces-homed cached
    // fp plane as the fp-combine (one cache per column, never a second).
    let pf = pm::dict_faces(ctx, attno);
    let t_fpb = std::time::Instant::now();
    let fps = pm::build_fps_cached(ctx, &pf, attno);
    crate::engine::phn(node, "fp_build", t_fpb);
    let fpr: &[Vec<u128>] = &fps;
    const ROW_BIT: u32 = 0x8000_0000;
    struct S {
        su: Scratch,
        codes: Vec<u32>,
        counts: Vec<u32>,
        buckets: Vec<Vec<(u64, u64, u32, u32, u32)>>,
        nulls: u64,
    }
    // [sqe-m2] Parked scatter buckets (~hundreds of MB per rep otherwise
    // re-faulted fresh — the parked-arena law applied here).
    // [spill-2, shrink law] formerly UNCAPPED — scatter-arena class cap
    // (the persist-rehome PARK32 precedent).
    static PARKB: crate::stencils::statepark::StatePark<Vec<Vec<(u64, u64, u32, u32, u32)>>> = crate::stencils::statepark::StatePark::new(256 << 20);
    let t_p1 = std::time::Instant::now();
    let pass1 = pool.run_finish(
        bank.parts.len(),
        |_| {
            let mut buckets = PARKB.fetch().unwrap_or_default();
            if buckets.len() != p {
                buckets = (0..p).map(|_| Vec::new()).collect();
            } else {
                buckets.iter_mut().for_each(|b| b.clear());
            }
            S {
                su: crate::scan::scratch_fetch(),
                codes: vec![0; 8192],
                counts: Vec::new(),
                buckets,
                nulls: 0,
            }
        },
        |s, pi| {
            let df = ctx.faces.dict(bank, pi, attno);
            if df.dh.is_some() {
                let mut cur = crate::scan::open_cursor(bank, pi, attno);
                let n = df.ncodes as usize;
                s.counts.clear();
                s.counts.resize(n, 0);
                for g in 0..cur.granule_count() {
                    let rows = cur.rows_in_granule(g) as usize;
                    if s.codes.len() < rows {
                        s.codes.resize(rows, 0);
                    }
                    cur.decode_codes(g, &mut s.codes[..rows]).expect("codes");
                    // [json-rung2] 3VL key law: NULL rows (placeholder
                    // codes under the validity stream) fold into the one
                    // NULL group; AllValid granules keep the bare loop.
                    if s.su.validity(&mut cur, g, rows).all_valid() {
                        for r in 0..rows {
                            s.counts[s.codes[r] as usize] += 1;
                        }
                    } else {
                        for r in 0..rows {
                            if s.su.row_valid(r) {
                                s.counts[s.codes[r] as usize] += 1;
                            } else {
                                s.nulls += 1;
                            }
                        }
                    }
                }
                let mut census_n = 0u64;
                let fpp = &fpr[pi];
                for c in 0..n {
                    let cnt = s.counts[c];
                    if cnt != 0 {
                        census_n += 1;
                        let h = fpp[c];
                        let (h1, h2) = ((h >> 64) as u64, h as u64);
                        s.buckets[(h1 >> (64 - pbits)) as usize]
                            .push((h1, h2, cnt, pi as u32, c as u32));
                    }
                }
                crate::engine::census_entries(census_n);
            } else {
                let mut cu = crate::scan::open_cursor(bank, pi, attno);
                for g in 0..cu.granule_count() {
                    let rows = cu.rows_in_granule(g) as usize;
                    let all_valid = s.su.validity(&mut cu, g, rows).all_valid();
                    s.su.decode_full(&mut cu, g, rows);
                    for r in 0..rows {
                        if !all_valid && !s.su.row_valid(r) {
                            s.nulls += 1;
                            continue;
                        }
                        let x = s.su.datums[r];
                        let pl = unsafe { crate::scan::varlena_payload(x) };
                        let h = entry_fp128(pl);
                        let (h1, h2) = ((h >> 64) as u64, h as u64);
                        s.buckets[(h1 >> (64 - pbits)) as usize].push((
                            h1,
                            h2,
                            1,
                            ROW_BIT | pi as u32,
                            (g << 16) | r as u32,
                        ));
                    }
                }
            }
        },
        |s| {
            crate::scan::scratch_park(s.su);
            (s.buckets, s.nulls)
        },
    );
    crate::engine::phn(node, "pass1", t_p1);
    let t_p2 = std::time::Instant::now();
    let null_total: u64 = pass1.iter().map(|s| s.1).sum();
    let mut pass1v: Vec<Vec<Vec<(u64, u64, u32, u32, u32)>>> =
        pass1.into_iter().map(|s| s.0).collect();
    let pass1 = &pass1v;
    // [sqe-m2] Per-WORKER owner table, grow-only, parked across reps (the
    // M2 profile: pass2 at ~155ms was dominated by a fresh alloc + zero
    // per PARTITION — ~GBs of page churn per rep); dict handles cached
    // per worker (faces() is a mutex — per-candidate locks serialized the
    // admission boundary).
    #[derive(Clone, Copy, Default)]
    struct FS {
        h1: u64,
        h2: u64,
        cnt: u32,
        pi: u32,
        code: u32,
    }
    // [spill-2, shrink law] formerly UNCAPPED — table class cap (the
    // PARKOA precedent).
    static PARKFS: crate::stencils::statepark::StatePark<Vec<FS>> = crate::stencils::statepark::StatePark::new(64 << 20);
    type P2State = (
        Vec<(Vec<u8>, u64)>,
        Scratch,
        CurCache,
        Vec<FS>,
        std::collections::HashMap<u32, std::sync::Arc<crate::engine::DictFace>>,
    );
    let owned = pool.run_finish(
        p,
        |_| -> P2State {
            (
                Vec::new(),
                crate::scan::scratch_fetch(),
                CurCache::new(attno),
                PARKFS.fetch().unwrap_or_default(),
                std::collections::HashMap::new(),
            )
        },
        |(out, rs, rc, slots, dhc): &mut P2State, part| {
            let n: usize = pass1.iter().map(|s| s[part].len()).sum();
            let cap = (n * 2).next_power_of_two().max(64);
            let mask = cap - 1;
            if slots.len() < cap {
                slots.resize(cap, FS::default());
            }
            for e in slots[..cap].iter_mut() {
                e.cnt = 0;
            }
            let slots = &mut slots[..cap];
            for s in pass1.iter() {
                for &(h1, h2, c, pi, code) in &s[part] {
                    let mut i = (h1 as usize) & mask;
                    loop {
                        let e = &mut slots[i];
                        if e.cnt == 0 {
                            *e = FS { h1, h2, cnt: c, pi, code };
                            break;
                        }
                        if e.h1 == h1 && e.h2 == h2 {
                            e.cnt += c;
                            break;
                        }
                        i = (i + 1) & mask;
                    }
                }
            }
            // partition top-k (cnt desc, bytes asc); lazy byte resolve.
            let mut top: Vec<(Vec<u8>, u64)> = Vec::new();
            for fs in slots.iter() {
                let cnt = fs.cnt as u64;
                if cnt == 0 {
                    continue;
                }
                if top.len() == k && cnt < top.last().unwrap().1 {
                    continue;
                }
                let bytes: Vec<u8> = if fs.pi & ROW_BIT != 0 {
                    let (g, r) = (fs.code >> 16, (fs.code & 0xFFFF) as u16);
                    let d = rs.decode_sel(rc.get(bank, (fs.pi & !ROW_BIT) as usize), g, &[r]);
                    unsafe { crate::scan::varlena_payload(d[0]) }.to_vec()
                } else {
                    let df = dhc
                        .entry(fs.pi)
                        .or_insert_with(|| ctx.faces.dict(bank, fs.pi as usize, attno));
                    df.dh.as_ref().unwrap().entry(fs.code).expect("dict entry").bytes.to_vec()
                };
                if k == usize::MAX {
                    // Collector mode (no selection bound): every group
                    // survives — append raw; the global sort below the
                    // partition loop settles the order once. The sorted
                    // insert is O(len) per group — quadratic at full-set
                    // grain.
                    top.push((bytes, cnt));
                    continue;
                }
                if top.len() == k {
                    let w = top.last().unwrap();
                    if cnt < w.1 || (cnt == w.1 && bytes >= w.0) {
                        continue;
                    }
                    top.pop();
                }
                let pos = top
                    .binary_search_by(|pr| cnt.cmp(&pr.1).then_with(|| pr.0.as_slice().cmp(&bytes)))
                    .unwrap_or_else(|q| q);
                top.insert(pos, (bytes, cnt));
            }
            out.extend(top);
        },
        |s| {
            crate::scan::scratch_park(s.1);
            (s.0, s.3)
        },
    );
    let mut all: Vec<(Vec<u8>, u64)> = Vec::new();
    for (out, slots) in owned {
        all.extend(out);
        let b = crate::stencils::statepark::vec_bytes(&slots);
        PARKFS.park(slots, b);
    }
    for b in pass1v.drain(..) {
        let bytes = crate::stencils::statepark::nested_bytes(&b);
        PARKB.park(b, bytes);
    }
    crate::engine::phn(node, "pass2", t_p2);
    pm::park_fps(fps);
    all.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    all.truncate(k);
    // The NULL group joins at the global cut: one group, NULLS-greatest
    // under the (count DESC, key ASC) order (the byval pack's convention).
    let mut rows: Vec<(Option<&[u8]>, u64)> =
        all.iter().map(|(b, c)| (Some(b.as_slice()), *c)).collect();
    if null_total > 0 {
        let pos = rows.partition_point(|r| r.1 >= null_total);
        rows.insert(pos, (None, null_total));
        rows.truncate(k);
    }
    let mut sink = LineSink::new(bank, node);
    for &(bytes, c) in rows.iter().skip(node.params.offset) {
        match bytes {
            Some(b) => sink.push(node, &|_| 0, Some(b), c, &|_| 0, 0, 0),
            None => sink.push_null_key(node, c),
        }
    }
    sink.finish()
}

// ---------------------------------------------------------------------------
// dense-domain group + distinct leg: domain bounds from the stats
// face; per-worker dense SoA agg arrays (vector-add merge); the
// COUNT(DISTINCT) leg rides the distinct pipeline (run-collapsed pair
// scatter -> owned dedupe -> dense-slice merge).
// ---------------------------------------------------------------------------

/// [spill-2] Distinct pair-plane spill engagement census.
pub static DSPILL_FLUSHES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static DSPILL_DRAINS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static DSPILL_MERGES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn dense_distinct(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    let (bank, pool) = (ctx.bank, ctx.pool);
    let a_key = node.params.group_cols[0];
    // [spill-2] pair-plane spill (spill-design.md §6): the O(rows)
    // (group, distinct-value) pair scatter + the owner dedupe tables
    // flush/drain/merge at the E18b shares; the dense per-group arrays
    // stay resident (priced under the budget by the classification).
    let sp_on = crate::stencils::hash_plane::distinct_spill_engaged(bank, ctx.faces, node);
    let dstore: Option<std::sync::Arc<dyn crate::spill::SpillStore>> = if sp_on {
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
    let dshare = ((ctx.faces.cfg.grouped_budget_bytes() / pool.threads().max(1) as u64).max(1))
        as usize;
    // The fused-HAVING answer path never routes here (server admission
    // refuses the pair; this form's emit carries no group filter).
    assert!(node.params.having.is_none(), "dense_distinct: fused HAVING unsupported");
    let (lo, dn) = {
        let sv = ctx.faces.stats(bank, a_key);
        let (lo, hi) = sv.minmax_exact().expect("dense_distinct: exact stats bounds");
        let range = (hi as i128 - lo as i128) + 1;
        // One authority with the server admission gate.
        let cap = crate::planner::DENSE_DISTINCT_DOMAIN as i128;
        assert!(range > 0 && range <= cap, "dense_distinct: domain not dense-safe");
        (lo, range as usize)
    };
    let units = ctx.faces.walk(bank, a_key);
    let mut sum_specs: Vec<(u32, u8)> = Vec::new(); // (attno, width)
    let mut distinct_attno: Option<u32> = None;
    // [aggqual] any sum/avg DISTINCT leg arms the first-seen value fold
    // (one shared distinct column — the admission contract).
    let mut dsum_armed = false;
    for a in &node.agg {
        match a.op {
            AggOp::CountStar => {}
            AggOp::Sum | AggOp::Avg => {
                let c = a.col.expect("sum/avg col");
                sum_specs.push((c, col_width(bank, c)));
            }
            AggOp::CountDistinct | AggOp::SumDistinct | AggOp::AvgDistinct => {
                assert!(
                    distinct_attno.is_none() || distinct_attno == a.col,
                    "dense_distinct: one shared distinct column (admission gap)"
                );
                distinct_attno = a.col;
                if matches!(a.op, AggOp::SumDistinct | AggOp::AvgDistinct) {
                    dsum_armed = true;
                }
            }
            other => panic!("dense_distinct: unsupported agg {other:?}"),
        }
    }
    let dw = distinct_attno.map(|c| col_width(bank, c)).unwrap_or(0);
    let nsum = sum_specs.len();
    struct S {
        ck: ColState,
        cs: Vec<ColState>,
        cd: Option<ColState>,
        cnt: Vec<u64>,
        sums: Vec<Vec<u64>>,
        buckets: Vec<Vec<u128>>,
        /// [spill-2] pair-plane spill lanes (None on the resident arm).
        sp: Option<BW>,
        resident: usize,
        w: usize,
    }
    let dstorer = &dstore;
    let t_p1 = std::time::Instant::now();
    let pass1 = pool.run_finish(
        units.len(),
        |w| S {
            ck: ColState::fetch(a_key),
            cs: sum_specs.iter().map(|&(a, _)| ColState::fetch(a)).collect(),
            cd: distinct_attno.map(ColState::fetch),
            cnt: vec![0; dn],
            sums: (0..nsum).map(|_| vec![0u64; dn]).collect(),
            buckets: if distinct_attno.is_some() {
                (0..RADIX_P).map(|_| Vec::new()).collect()
            } else {
                Vec::new()
            },
            sp: None,
            resident: 0,
            w,
        },
        |st, i| {
            let (pi, g, rows, _) = units[i];
            let rows = rows as usize;
            let dk = st.ck.dec(bank, pi, g, rows);
            let dk: &[u64] = unsafe { std::slice::from_raw_parts(dk.as_ptr(), dk.len()) };
            let mut scols: Vec<&[u64]> = Vec::with_capacity(nsum);
            for c in st.cs.iter_mut() {
                let d = c.dec(bank, pi, g, rows);
                scols.push(unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) });
            }
            let dd: Option<&[u64]> = st.cd.as_mut().map(|c| {
                let d = c.dec(bank, pi, g, rows);
                unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) }
            });
            let mut prev: u128 = u128::MAX;
            for r in 0..rows {
                let k = (dk[r] as i64 - lo) as usize;
                st.cnt[k] += 1;
                for (a, &(_, w)) in sum_specs.iter().enumerate() {
                    st.sums[a][k] = st.sums[a][k].wrapping_add(sx(scols[a][r], w) as u64);
                }
                if let Some(du) = dd {
                    let pair = ((k as u128) << 64) | du[r] as u128;
                    if pair != prev {
                        st.buckets[radix_of(hash64(du[r]))].push(pair);
                        st.resident += 1;
                        prev = pair;
                    }
                }
            }
            // [spill-2, E18b] flush the pair buckets at the worker share
            // (16 B/pair records, one chunk per radix partition; the
            // arena keeps capacity — the flush bounds resident bytes).
            if let Some(dst) = dstorer {
                if st.resident * 16 > dshare {
                    DSPILL_FLUSHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let w = st.w;
                    let sp = st.sp.get_or_insert_with(|| BW::new(&**dst, "dpair-scatter", w));
                    for b in 0..RADIX_P {
                        if st.buckets[b].is_empty() {
                            continue;
                        }
                        sp.begin();
                        for &pr in &st.buckets[b] {
                            sp.push(&pr.to_ne_bytes());
                        }
                        let (off, len) = sp.end();
                        sp.chunks.push((b as u32, off, len));
                        st.buckets[b].clear();
                    }
                    st.resident = 0;
                }
            }
        },
        |s| {
            s.ck.park();
            s.cs.into_iter().for_each(ColState::park);
            if let Some(c) = s.cd {
                c.park();
            }
            (s.cnt, s.sums, s.buckets, s.sp.map(|w| (w.m, w.chunks)))
        },
    );
    crate::engine::phn(node, "pass1", t_p1);
    let t_p2 = std::time::Instant::now();
    let mut distinct = vec![0u64; dn];
    let mut dsums = vec![0u64; if dsum_armed { dn } else { 0 }];
    if distinct_attno.is_some() {
        // [sqe-m2] Parked per-worker seen-tables: the fresh Cnt128 per
        // radix partition was ~hundreds of MB of page churn per rep (the
        // distinct_merge phase at ~150ms).
        // [spill-2, shrink law] formerly UNCAPPED — table class cap.
        static PARKC: crate::stencils::statepark::StatePark<Cnt128> = crate::stencils::statepark::StatePark::new(64 << 20);
        let scattered: Vec<&Vec<Vec<u128>>> = pass1.iter().map(|p| &p.2).collect();
        let chunked: Vec<&Option<(Box<dyn crate::spill::SpillMedium>, Vec<(u32, u64, u64)>)>> =
            pass1.iter().map(|p| &p.3).collect();
        // [spill-2, E18b] the owner dedupe-table cap (entries): a Cnt128
        // at 2x-load slots is ~40 B/entry; the 128 floor is the drain
        // grain. Bind only on the spill arm.
        let cap_entries = (dshare / 40).max(128);
        let owned = pool.run(
            RADIX_P,
            |w| {
                (
                    vec![0u32; dn],
                    vec![0u64; if dsum_armed { dn } else { 0 }],
                    PARKC.fetch().unwrap_or_else(|| Cnt128::new(16)),
                    w,
                    None::<BW>,
                )
            },
            |st: &mut (Vec<u32>, Vec<u64>, Cnt128, usize, Option<BW>), p| {
                let (counts, sums, seen, w, rw) =
                    (&mut st.0, &mut st.1, &mut st.2, st.3, &mut st.4);
                let n: usize = scattered.iter().map(|b| b[p].len()).sum();
                // One emit per DISTINCT pair — the fold both paths share.
                macro_rules! emit {
                    ($pair:expr) => {{
                        let pair: u128 = $pair;
                        counts[(pair >> 64) as usize] += 1;
                        // [aggqual] first-seen: the distinct VALUE folds
                        // once (u64 wrapping lanes under the witnessed
                        // i64-provable bound).
                        if dsum_armed {
                            sums[(pair >> 64) as usize] = sums[(pair >> 64) as usize]
                                .wrapping_add(sx(pair as u64, dw) as u64);
                        }
                    }};
                }
                if dstore.is_none() {
                    // Resident arm: exactly the pre-spill fold.
                    seen.reset(n.max(16));
                    for b in &scattered {
                        for &pair in &b[p] {
                            if seen.add(pair, 1) {
                                emit!(pair);
                            }
                        }
                    }
                    return;
                }
                // [spill-2] spill arm: capped dedupe table draining
                // key-sorted pair runs; spilled chunks fold first
                // (sequential reads), then the resident residue.
                seen.reset(cap_entries.min(n.max(16)));
                let mut runs: Vec<(u64, u64)> = Vec::new(); // (off, nrec)
                let mut drain = |seen: &mut Cnt128,
                                 rw: &mut Option<BW>,
                                 runs: &mut Vec<(u64, u64)>| {
                    DSPILL_DRAINS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let mut keys: Vec<u128> = Vec::with_capacity(seen.len);
                    for i in 0..seen.cap() {
                        if seen.cnt[i] != 0 {
                            keys.push(seen.keys[i]);
                        }
                    }
                    keys.sort_unstable();
                    let bw = rw.get_or_insert_with(|| {
                        BW::new(&**dstore.as_ref().expect("spill arm"), "dpair-runs", w)
                    });
                    bw.begin();
                    for k in &keys {
                        bw.push(&k.to_ne_bytes());
                    }
                    let (off, _len) = bw.end();
                    runs.push((off, keys.len() as u64));
                    seen.reset(cap_entries);
                };
                for ch in &chunked {
                    if let Some((m, chunks)) = ch {
                        for &(cb, off, len) in chunks {
                            if cb as usize != p {
                                continue;
                            }
                            let slab = crate::spill::SLAB_BYTES.min(dshare.max(16));
                            let mut cur =
                                crate::spill::ChunkCursor::new(&**m, off, len / 16, 16, slab);
                            while let Some(r) = cur.next() {
                                let pair = u128::from_ne_bytes(r.try_into().unwrap());
                                if seen.len >= cap_entries {
                                    drain(seen, rw, &mut runs);
                                }
                                seen.add(pair, 1);
                            }
                        }
                    }
                }
                for b in &scattered {
                    for &pair in &b[p] {
                        if seen.len >= cap_entries {
                            drain(seen, rw, &mut runs);
                        }
                        seen.add(pair, 1);
                    }
                }
                if runs.is_empty() {
                    // never drained: the table IS the distinct set.
                    for i in 0..seen.cap() {
                        if seen.cnt[i] != 0 {
                            emit!(seen.keys[i]);
                        }
                    }
                    return;
                }
                // K-way dedupe merge: sorted runs + the sorted residue.
                DSPILL_MERGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                use std::cmp::Reverse;
                use std::collections::BinaryHeap;
                let mut mem: Vec<u128> = Vec::with_capacity(seen.len);
                for i in 0..seen.cap() {
                    if seen.cnt[i] != 0 {
                        mem.push(seen.keys[i]);
                    }
                }
                mem.sort_unstable();
                let m = &*rw.as_ref().expect("runs imply a run file").m;
                let nrun = runs.len();
                let slab = (dshare / (nrun + 1)).clamp(16, crate::spill::SLAB_BYTES);
                let mut curs: Vec<crate::spill::ChunkCursor> = runs
                    .iter()
                    .map(|&(off, g)| crate::spill::ChunkCursor::new(m, off, g, 16, slab))
                    .collect();
                let mut mi = 0usize;
                let mut heads: Vec<Option<u128>> = Vec::with_capacity(nrun + 1);
                let mut heap: BinaryHeap<Reverse<(u128, usize)>> =
                    BinaryHeap::with_capacity(nrun + 1);
                for (i, c) in curs.iter_mut().enumerate() {
                    let h = c.next().map(|r| u128::from_ne_bytes(r.try_into().unwrap()));
                    if let Some(k) = h {
                        heap.push(Reverse((k, i)));
                    }
                    heads.push(h);
                }
                let mh = (mi < mem.len()).then(|| {
                    let k = mem[mi];
                    mi += 1;
                    k
                });
                if let Some(k) = mh {
                    heap.push(Reverse((k, nrun)));
                }
                heads.push(mh);
                while let Some(Reverse((key, src))) = heap.pop() {
                    let mut adv = |heads: &mut Vec<Option<u128>>,
                                   heap: &mut BinaryHeap<Reverse<(u128, usize)>>,
                                   s: usize| {
                        let nx = if s < nrun {
                            curs[s].next().map(|r| u128::from_ne_bytes(r.try_into().unwrap()))
                        } else if mi < mem.len() {
                            let k = mem[mi];
                            mi += 1;
                            Some(k)
                        } else {
                            None
                        };
                        if let Some(k) = nx {
                            heap.push(Reverse((k, s)));
                        }
                        heads[s] = nx;
                    };
                    adv(&mut heads, &mut heap, src);
                    // dedupe: swallow every equal key — one emit per
                    // DISTINCT pair, run boundaries invisible.
                    while let Some(&Reverse((k2, s2))) = heap.peek() {
                        if k2 != key {
                            break;
                        }
                        heap.pop();
                        adv(&mut heads, &mut heap, s2);
                    }
                    emit!(key);
                }
            },
        );
        for (c, sm, seen, _, _) in owned {
            let b = seen.keys.capacity() * 16 + seen.cnt.capacity() * 4;
            PARKC.park(seen, b);
            for i in 0..dn {
                distinct[i] += c[i] as u64;
            }
            if dsum_armed {
                for i in 0..dn {
                    dsums[i] = dsums[i].wrapping_add(sm[i]);
                }
            }
        }
    }
    let mut cnt = vec![0u64; dn];
    let mut sums: Vec<Vec<u64>> = (0..nsum).map(|_| vec![0u64; dn]).collect();
    for p in &pass1 {
        for i in 0..dn {
            cnt[i] += p.0[i];
        }
        for a in 0..nsum {
            for i in 0..dn {
                sums[a][i] = sums[a][i].wrapping_add(p.1[a][i]);
            }
        }
    }
    pool.drop_par(pass1);
    crate::engine::phn(node, "distinct_merge", t_p2);
    let covered: u64 = cnt.iter().sum();
    assert_eq!(covered, bank.rows_total(), "q{}: dense counts must cover the bank", node.q);
    // map agg index -> sums slot.
    let mut slot_of = vec![usize::MAX; node.agg.len()];
    let mut si = 0usize;
    for (i, a) in node.agg.iter().enumerate() {
        if matches!(a.op, AggOp::Sum | AggOp::Avg) {
            slot_of[i] = si;
            si += 1;
        }
    }
    let mut idx: Vec<usize> = (0..dn).filter(|&i| cnt[i] > 0).collect();
    // [spill-3] a pure count-distinct set (the re-elected shape) pins
    // the pipeline's own (distinct DESC, key ASC); mixes keep count.
    let pure_cd = node.agg.iter().all(|a| a.op == AggOp::CountDistinct);
    let ord = if pure_cd { &distinct } else { &cnt };
    idx.sort_unstable_by(|&a, &b| ord[b].cmp(&ord[a]).then_with(|| a.cmp(&b)));
    let k = topk(node);
    idx.truncate(k);
    let w = col_width(bank, a_key);
    let mut sink = LineSink::new(bank, node);
    for &i in idx.iter().skip(node.params.offset) {
        let keyv = (i as i64 + lo) as u64;
        sink.push(node, &|_| sx(keyv, w), None, cnt[i], &|ai| sums[slot_of[ai]][i], distinct[i], if dsum_armed { dsums[i] as i64 } else { 0 });
    }
    sink.finish()
}

// ---------------------------------------------------------------------------
// (int, gid) pair with a drop-empty key filter (sqe-m1 lineage):
// the packed key fits u64 — (gid << 8*w_int) | int — so the shape rides
// drivers::count_owned_64 (the elected cnt_owned64_pool driver). Empty-gid
// rows are dropped in the fill (the `<> ''` conjunct evaluated in gid
// domain: one integer compare post-translation).
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// [sqe-m4] (gid, gid) pair — TWO varlena group keys, a key shape with no
// reference-workload precedent. Identity: one merge-built text registry per
// column (gid ASC == bytes ASC), keys packed (gidA << b) | gidB with b
// ELECTED from the second registry's census (the key-pack law — widths
// from ngids, never constants). SelOrder::CountDescKeyAsc over the pack
// is then exactly (count DESC, bytesA ASC, bytesB ASC) — the canonical
// tie. `<> ''` conjuncts on key columns drop in gid domain (one integer
// compare), the drop-empty lineage.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// derived-minute packed key (sqe-m1 lineage): (ord << (6+gb)) |
// (minute << gb) | gid — the ordinal remap of the wide int key is an
// ORDER-PRESERVING standing face (kernels_g2::ord_remap_memo), so the u64
// pack sorts exactly like the canonical (int, minute, text) key. Bit
// widths are ELECTED from ngids/ndv (the plan-flagged scale hazard);
// asserts are the guard.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Per-part shapes (the only arms — risks.md §11 deleted the registry/
// globaldict A/B twins at port): keys are PART-SCOPED codes
// (part_merge::psk; side keys for raw parts); byte-equal groups re-join
// across parts at the combine — string compares at merge boundaries,
// never per row.
// ---------------------------------------------------------------------------

use crate::grouped::Cnt128 as PmCnt128;
use crate::scan::varlena_payload;
use crate::stencils::part_merge as pm;

/// Selection entry for the gid-order-emulating shapes: (count, int lane,
/// reg_ord hash, bytes). The old arm's order was over packed (lane, gid)
/// keys; gid ASC == (reg_ord, bytes) ASC (part_merge::reg_ord), so this
/// tuple reproduces it exactly.
type GidOrdEnt = (u64, u64, u64, Vec<u8>);

/// candidate-before-entry under the shape's order: KeyAsc = (lane ASC,
/// gid-order ASC); CountDescKeyAsc = (count DESC, lane ASC, gid-order ASC).
#[inline]
fn gidord_before(key_asc: bool, c: u64, a: u64, h: u64, b: &[u8], e: &GidOrdEnt) -> bool {
    if !key_asc && c != e.0 {
        return c > e.0;
    }
    if a != e.1 {
        return a < e.1;
    }
    if h != e.2 {
        return h < e.2;
    }
    b < e.3.as_slice()
}

/// Top-k insert — bytes (and their reg_ord hash) fetched only when a
/// candidate survives the count/lane short-circuits.
fn topk_gidord(
    top: &mut Vec<GidOrdEnt>,
    kw: usize,
    key_asc: bool,
    c: u64,
    a: u64,
    key: u64,
    kb: &pm::KeyBytes,
) {
    if kw == 0 {
        return;
    }
    if kw == usize::MAX {
        // Collector mode (no selection bound): every group survives, so
        // append raw and settle the order ONCE at the global merge
        // (`gidord_finish`). The sorted-insert path below is O(len) per
        // candidate — quadratic at full-set grain.
        let b = kb.bytes(key);
        top.push((c, a, pm::reg_ord(b), b.to_vec()));
        return;
    }
    if top.len() == kw {
        let last = top.last().unwrap();
        // cheap rejects that need no bytes
        if key_asc {
            if a > last.1 {
                return;
            }
        } else {
            if c < last.0 {
                return;
            }
            if c == last.0 && a > last.1 {
                return;
            }
        }
        let b = kb.bytes(key);
        let h = pm::reg_ord(b);
        if !gidord_before(key_asc, c, a, h, b, last) {
            return;
        }
        top.pop();
        let pos = top
            .partition_point(|e| !gidord_before(key_asc, c, a, h, b, e));
        top.insert(pos, (c, a, h, b.to_vec()));
        return;
    }
    let b = kb.bytes(key);
    let h = pm::reg_ord(b);
    let pos = top.partition_point(|e| !gidord_before(key_asc, c, a, h, b, e));
    top.insert(pos, (c, a, h, b.to_vec()));
}

/// Global merge of per-owner GidOrdEnt selections.
fn gidord_finish(mut all: Vec<GidOrdEnt>, key_asc: bool, kw: usize) -> Vec<GidOrdEnt> {
    all.sort_unstable_by(|x, y| {
        let o = if key_asc {
            std::cmp::Ordering::Equal
        } else {
            y.0.cmp(&x.0)
        };
        o.then_with(|| x.1.cmp(&y.1))
            .then_with(|| x.2.cmp(&y.2))
            .then_with(|| x.3.cmp(&y.3))
    });
    all.truncate(kw);
    all
}

/// [fpcombine] (int, text) COUNT(*) on the fingerprint plane:
/// pass1 mixes the part's ENTRY fingerprint (L2-resident per-part array)
/// with the int lane into a 128-bit group key AT SCATTER TIME; owners fold
/// on 128-bit equality — no cross-part closure, no group sort, no byte
/// touches until the top-k admission boundary.
fn int_gid_fp(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    use crate::grouped::hash128;
    let (bank, pool) = (ctx.bank, ctx.pool);
    let (a_int, a_text) = (node.params.group_cols[0], node.params.group_cols[1]);
    pm::assert_psk_fits(bank);
    let pf = pm::dict_faces(ctx, a_text);
    let t_fpb = std::time::Instant::now();
    let fps = pm::build_fps_cached(ctx, &pf, a_text);
    crate::engine::phn(node, "fp_build", t_fpb);
    let units = ctx.faces.walk(bank, a_text);
    let ndv = ndv_face(ctx, a_int) + ndv_face(ctx, a_text);
    let p = partition_count(ndv.max(1), node.params.slot_bytes, node.params.l2_bytes, ctx.pool.threads());
    let shift = 64 - p.trailing_zeros();
    // 128-bit (v, entry) group key: entry fingerprint XOR a two-lane mix
    // of v (independent finishers). Same 128-bit-equality-as-identity
    // convention as text128.
    #[inline(always)]
    fn vmix(v: u64) -> u128 {
        ((hash64(v) as u128) << 64) | hash64(v ^ 0xD6E8_FEB8_6659_FD93) as u128
    }
    struct S {
        ci: ColState,
        kc: CurCache,
        ks: Scratch,
        codes: Vec<u32>,
        buckets: Vec<Vec<(u128, u64, u64)>>, // (gk, v, key<<20|cnt)
        intern: pm::Intern,
    }
    struct SK {
        buckets: Vec<Vec<(u128, u64, u64)>>,
        intern: pm::Intern,
    }
    let pf2 = &pf;
    let fpr: &[Vec<u128>] = &fps;
    // [spill-2, shrink law] formerly UNCAPPED and column-keyed — the
    // parked states are plain data buckets, cleared/re-armed on fetch
    // (query-agnostic by the park law); scatter-arena class cap.
    static PARKIGF: crate::stencils::statepark::StatePark<Vec<Vec<(u128, u64, u64)>>> = crate::stencils::statepark::StatePark::new(256 << 20);
    let parked: std::sync::Mutex<Vec<Vec<Vec<(u128, u64, u64)>>>> =
        std::sync::Mutex::new(PARKIGF.fetch_up_to(pool.threads()));
    let t_p1 = std::time::Instant::now();
    let pass1 = pool.run_finish(
        units.len(),
        |t| {
            let mut buckets = parked.lock().unwrap().pop().unwrap_or_default();
            if buckets.len() != p {
                buckets = (0..p).map(|_| Vec::new()).collect();
            } else {
                buckets.iter_mut().for_each(|b| b.clear());
            }
            S {
                ci: ColState::fetch(a_int),
                kc: CurCache::new(a_text),
                ks: crate::scan::scratch_fetch(),
                codes: vec![0; 8192],
                buckets,
                intern: pm::Intern::new(t),
            }
        },
        |s: &mut S, i| {
            let (pi, g, rows, _) = units[i];
            let rows = rows as usize;
            let du = s.ci.dec(bank, pi, g, rows);
            let du: &[u64] = unsafe { std::slice::from_raw_parts(du.as_ptr(), du.len()) };
            if pf2[pi].dh.is_some() {
                if s.codes.len() < rows {
                    s.codes.resize(rows, 0);
                }
                s.kc.get(bank, pi).decode_codes(g, &mut s.codes[..rows]).expect("codes");
                let codes = &s.codes;
                let fpp = &fpr[pi];
                let mut r = 0usize;
                while r < rows {
                    let (v, c) = (du[r], codes[r]);
                    let mut j = r + 1;
                    while j < rows && du[j] == v && codes[j] == c {
                        j += 1;
                    }
                    // one hash64(v) serves both the bucket and vmix's hi
                    // lane (must stay byte-identical to vmix()).
                    let h = hash64(v);
                    let gk = fpp[c as usize]
                        ^ (((h as u128) << 64) | hash64(v ^ 0xD6E8_FEB8_6659_FD93) as u128);
                    s.buckets[(h >> shift) as usize].push((
                        gk,
                        v,
                        (pm::psk(pi, c) << 20) | (j - r) as u64,
                    ));
                    r = j;
                }
            } else {
                let d = s.ks.decode_full(s.kc.get(bank, pi), g, rows);
                let d: &[u64] = unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) };
                let mut prev: Option<(u64, u64)> = None;
                let mut prev_fp: u128 = 0;
                for r in 0..rows {
                    let pl = unsafe { varlena_payload(d[r]) };
                    let key = s.intern.key(pl);
                    let v = du[r];
                    if prev == Some((v, key)) {
                        if let Some(last) = s.buckets[(hash64(v) >> shift) as usize].last_mut() {
                            last.2 += 1;
                            continue;
                        }
                    }
                    if prev.map(|(_, k)| k) != Some(key) {
                        prev_fp = crate::fp::entry_fp128(pl);
                    }
                    let gk = prev_fp ^ vmix(v);
                    s.buckets[(hash64(v) >> shift) as usize].push((gk, v, (key << 20) | 1));
                    prev = Some((v, key));
                }
            }
        },
        |s| {
            s.ci.park();
            crate::scan::scratch_park(s.ks);
            SK { buckets: s.buckets, intern: s.intern }
        },
    );
    crate::engine::phn(node, "pass1", t_p1);
    let t_p2 = std::time::Instant::now();
    let kb = pm::KeyBytes {
        pf: &pf,
        interns: pm::interns_by_slot(pool.threads(), pass1.iter().map(|s| &s.intern)),
    };
    let kbr = &kb;
    let scattered: Vec<&SK> = pass1.iter().collect();
    let kw = topk(node);
    let key_asc = node.params.order == OrderBy::KeyAsc;
    // owner slot: (gk, v, key, cnt) — open-addressed on hash128(gk).
    type FpPairSlot = (u128, u64, u64, u64);
    static PARKIGFT: crate::stencils::statepark::StatePark<Vec<FpPairSlot>> = crate::stencils::statepark::StatePark::new(64 << 20);
    let owned = pool.run(
        p,
        |_| {
            (
                Vec::new(),
                0u64,
                0u64,
                PARKIGFT.fetch().unwrap_or_default(),
            )
        },
        |(top, rowsc, groups, slots): &mut (Vec<GidOrdEnt>, u64, u64, Vec<FpPairSlot>),
         part| {
            let n: usize = scattered.iter().map(|s| s.buckets[part].len()).sum();
            if n == 0 {
                return;
            }
            let cap = (n * 2).next_power_of_two().max(16);
            let mask = cap - 1;
            if slots.len() < cap {
                slots.resize(cap, (0, 0, 0, 0));
            }
            for e in slots[..cap].iter_mut() {
                e.3 = 0;
            }
            let tbl = &mut slots[..cap];
            for s in &scattered {
                for &(gk, v, kc) in &s.buckets[part] {
                    let (key, c) = (kc >> 20, kc & ((1 << 20) - 1));
                    let mut i = (hash128(gk) as usize) & mask;
                    loop {
                        let e = &mut tbl[i];
                        if e.3 == 0 {
                            *e = (gk, v, key, c);
                            break;
                        }
                        if e.0 == gk {
                            e.3 += c;
                            break;
                        }
                        i = (i + 1) & mask;
                    }
                }
            }
            for e in tbl.iter() {
                if e.3 != 0 {
                    *rowsc += e.3;
                    *groups += 1;
                    topk_gidord(top, kw, key_asc, e.3, e.1, e.2, kbr);
                }
            }
        },
    );
    let rows_counted: u64 = owned.iter().map(|o| o.1).sum();
    assert_eq!(rows_counted, bank.rows_total(), "q{}: rows must cover the bank", node.q);
    let mut tops: Vec<GidOrdEnt> = Vec::new();
    for (o, _, _, slots) in owned {
        tops.extend(o);
        let b = crate::stencils::statepark::vec_bytes(&slots);
        PARKIGFT.park(slots, b);
    }
    let all = gidord_finish(tops, key_asc, kw);
    crate::engine::phn(node, "pass2", t_p2);
    let w = col_width(bank, a_int);
    let mut sink = LineSink::new(bank, node);
    for (c, v, _, b) in all.iter().skip(node.params.offset) {
        sink.push(node, &|_| sx(*v, w), Some(b), *c, &|_| 0, 0, 0);
    }
    let out = sink.finish();
    {
        let mut v: Vec<Vec<Vec<(u128, u64, u64)>>> = parked.into_inner().unwrap();
        for st in pass1 {
            v.push(st.buckets);
        }
        for b in v {
            let bytes = crate::stencils::statepark::nested_bytes(&b);
            PARKIGF.park(b, bytes);
        }
    }
    pm::park_fps(fps);
    out
}

/// [fpcombine] (int, text) COUNT(*) with the empty-key drop in ONE
/// fold stage: pass1 mixes the entry fingerprint with the (masked) int
/// lane at scatter time; owners fold on 128-bit equality and keep the
/// tie-inclusive count-bar top-k. The per-partition exact (psk, int)
/// pre-fold and the string-keyed stage-2 both disappear.
fn int_gid_filtered_fp(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    use crate::grouped::hash128;
    let (bank, pool) = (ctx.bank, ctx.pool);
    assert!(node.agg.iter().all(|a| a.op == AggOp::CountStar));
    let (a_int, a_text) = (node.params.group_cols[0], node.params.group_cols[1]);
    let wi = col_width(bank, a_int);
    let lo_mask = if wi >= 8 { u64::MAX } else { (1u64 << (8 * wi as u32)) - 1 };
    pm::assert_psk_fits(bank);
    let pf = pm::dict_faces(ctx, a_text);
    let t_fpb = std::time::Instant::now();
    let fps = pm::build_fps_cached(ctx, &pf, a_text);
    crate::engine::phn(node, "fp_build", t_fpb);
    let units = ctx.faces.walk(bank, a_text);
    let ndv = ndv_face(ctx, a_text);
    let p = partition_count(ndv.max(1), node.params.slot_bytes, node.params.l2_bytes, ctx.pool.threads());
    let shift = 64 - p.trailing_zeros();
    #[inline(always)]
    fn vmix(v: u64) -> u128 {
        ((hash64(v) as u128) << 64) | hash64(v ^ 0xD6E8_FEB8_6659_FD93) as u128
    }
    struct S {
        ci: ColState,
        kc: CurCache,
        ks: Scratch,
        codes: Vec<u32>,
        buckets: Vec<Vec<(u128, u64, u64)>>, // (gk, v, key<<20|cnt)
        intern: pm::Intern,
    }
    struct SK {
        buckets: Vec<Vec<(u128, u64, u64)>>,
        intern: pm::Intern,
    }
    let pf2 = &pf;
    let fpr: &[Vec<u128>] = &fps;
    // [spill-2, shrink law] formerly UNCAPPED and column-keyed (see
    // PARKIGF): scatter-arena class cap, query-agnostic.
    static PARKIGFF: crate::stencils::statepark::StatePark<Vec<Vec<(u128, u64, u64)>>> = crate::stencils::statepark::StatePark::new(256 << 20);
    let parked: std::sync::Mutex<Vec<Vec<Vec<(u128, u64, u64)>>>> =
        std::sync::Mutex::new(PARKIGFF.fetch_up_to(pool.threads()));
    let t_p1 = std::time::Instant::now();
    let pass1 = pool.run_finish(
        units.len(),
        |t| {
            let mut buckets = parked.lock().unwrap().pop().unwrap_or_default();
            if buckets.len() != p {
                buckets = (0..p).map(|_| Vec::new()).collect();
            } else {
                buckets.iter_mut().for_each(|b| b.clear());
            }
            S {
                ci: ColState::fetch(a_int),
                kc: CurCache::new(a_text),
                ks: crate::scan::scratch_fetch(),
                codes: vec![0; 8192],
                buckets,
                intern: pm::Intern::new(t),
            }
        },
        |s: &mut S, i| {
            let (pi, g, rows, _) = units[i];
            let rows = rows as usize;
            let du = s.ci.dec(bank, pi, g, rows);
            let du: &[u64] = unsafe { std::slice::from_raw_parts(du.as_ptr(), du.len()) };
            if pf2[pi].dh.is_some() {
                if s.codes.len() < rows {
                    s.codes.resize(rows, 0);
                }
                s.kc.get(bank, pi).decode_codes(g, &mut s.codes[..rows]).expect("codes");
                let empty = pf2[pi].empty_code;
                let codes = &s.codes;
                let fpp = &fpr[pi];
                let mut r = 0usize;
                while r < rows {
                    let (v, c) = (du[r] & lo_mask, codes[r]);
                    let mut j = r + 1;
                    while j < rows && (du[j] & lo_mask) == v && codes[j] == c {
                        j += 1;
                    }
                    if Some(c) != empty {
                        let gk = fpp[c as usize] ^ vmix(v);
                        s.buckets[(hash128(gk) >> shift) as usize].push((
                            gk,
                            v,
                            (pm::psk(pi, c) << 20) | (j - r) as u64,
                        ));
                    }
                    r = j;
                }
            } else {
                let d = s.ks.decode_full(s.kc.get(bank, pi), g, rows);
                let d: &[u64] = unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) };
                let mut prev_key: Option<u64> = None;
                let mut prev_fp: u128 = 0;
                for r in 0..rows {
                    let pl = unsafe { varlena_payload(d[r]) };
                    if pl.is_empty() {
                        continue;
                    }
                    let key = s.intern.key(pl);
                    if prev_key != Some(key) {
                        prev_fp = crate::fp::entry_fp128(pl);
                        prev_key = Some(key);
                    }
                    let v = du[r] & lo_mask;
                    let gk = prev_fp ^ vmix(v);
                    s.buckets[(hash128(gk) >> shift) as usize].push((gk, v, (key << 20) | 1));
                }
            }
        },
        |s| {
            s.ci.park();
            crate::scan::scratch_park(s.ks);
            SK { buckets: s.buckets, intern: s.intern }
        },
    );
    crate::engine::phn(node, "pass1", t_p1);
    let t_p2 = std::time::Instant::now();
    let kb = pm::KeyBytes {
        pf: &pf,
        interns: pm::interns_by_slot(pool.threads(), pass1.iter().map(|s| &s.intern)),
    };
    let kbr = &kb;
    let scattered: Vec<&SK> = pass1.iter().collect();
    let kw = topk(node);
    type FpPairSlot = (u128, u64, u64, u64);
    static PARKIGFFT: crate::stencils::statepark::StatePark<Vec<FpPairSlot>> = crate::stencils::statepark::StatePark::new(64 << 20);
    let owned = pool.run(
        p,
        |_| (Vec::new(), PARKIGFFT.fetch().unwrap_or_default()),
        |(out, slots): &mut (Vec<pm::Frag>, Vec<FpPairSlot>), part| {
            let n: usize = scattered.iter().map(|s| s.buckets[part].len()).sum();
            if n == 0 {
                return;
            }
            let cap = (n * 2).next_power_of_two().max(16);
            let mask = cap - 1;
            if slots.len() < cap {
                slots.resize(cap, (0, 0, 0, 0));
            }
            for e in slots[..cap].iter_mut() {
                e.3 = 0;
            }
            let tbl = &mut slots[..cap];
            for s in &scattered {
                for &(gk, v, kc) in &s.buckets[part] {
                    let (key, c) = (kc >> 20, kc & ((1 << 20) - 1));
                    let mut i = (hash128(gk) as usize) & mask;
                    loop {
                        let e = &mut tbl[i];
                        if e.3 == 0 {
                            *e = (gk, v, key, c);
                            break;
                        }
                        if e.0 == gk {
                            e.3 += c;
                            break;
                        }
                        i = (i + 1) & mask;
                    }
                }
            }
            // tie-inclusive count bar per partition (groups are whole).
            let mut all: Vec<pm::Frag> = Vec::new();
            for e in tbl.iter() {
                if e.3 != 0 {
                    all.push((e.2, e.1, e.3)); // (key, v, cnt)
                }
            }
            if kw > 0 && all.len() > kw {
                let (_, nth, _) = all.select_nth_unstable_by(kw - 1, |x, y| y.2.cmp(&x.2));
                let bar = nth.2;
                all.retain(|e| e.2 >= bar);
            }
            out.extend(all);
        },
    );
    let mut cands: Vec<pm::Frag> = Vec::new();
    for (o, slots) in owned {
        cands.extend(o);
        let b = crate::stencils::statepark::vec_bytes(&slots);
        PARKIGFFT.park(slots, b);
    }
    // (count DESC, gid-order ASC, int ASC) — gid-order = (reg_ord, bytes).
    let mut all: Vec<(u64, u64, &[u8], u64)> = cands
        .iter()
        .map(|&(key, v, c)| {
            let b = kbr.bytes(key);
            (c, pm::reg_ord(b), b, v)
        })
        .collect();
    all.sort_unstable_by(|x, y| {
        y.0.cmp(&x.0)
            .then_with(|| x.1.cmp(&y.1))
            .then_with(|| x.2.cmp(y.2))
            .then_with(|| x.3.cmp(&y.3))
    });
    all.truncate(kw);
    crate::engine::phn(node, "pass2", t_p2);
    let mut sink = LineSink::new(bank, node);
    for (c, _, b, v) in all.iter().skip(node.params.offset) {
        sink.push(node, &|_| sx(*v, wi), Some(b), *c, &|_| 0, 0, 0);
    }
    let out = sink.finish();
    {
        let mut v: Vec<Vec<Vec<(u128, u64, u64)>>> = parked.into_inner().unwrap();
        for st in pass1 {
            v.push(st.buckets);
        }
        for b in v {
            let bytes = crate::stencils::statepark::nested_bytes(&b);
            PARKIGFF.park(b, bytes);
        }
    }
    pm::park_fps(fps);
    out
}

/// (int, text) COUNT(*). Scatter (v, part-scoped key) by
/// the INT hash (a group's rows share v, so every fragment of a group
/// meets in one owner); owners fold exact (v, key) counts, then close the
/// cross-part boundary per v-run with byte compares.
pub fn int_gid(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    if ctx.faces.cfg.fpcombine {
        return int_gid_fp(ctx, node);
    }
    let (bank, pool) = (ctx.bank, ctx.pool);
    let (a_int, a_text) = (node.params.group_cols[0], node.params.group_cols[1]);
    pm::assert_psk_fits(bank);
    let pf = pm::dict_faces(ctx, a_text);
    let units = ctx.faces.walk(bank, a_text);
    let ndv = ndv_face(ctx, a_int) + ndv_face(ctx, a_text);
    let p = partition_count(ndv.max(1), node.params.slot_bytes, node.params.l2_bytes, ctx.pool.threads());
    let shift = 64 - p.trailing_zeros();
    struct S {
        ci: ColState,
        kc: CurCache,
        ks: Scratch,
        codes: Vec<u32>,
        buckets: Vec<Vec<(u64, u64, u32)>>,
        intern: pm::Intern,
    }
    struct SK {
        buckets: Vec<Vec<(u64, u64, u32)>>,
        intern: pm::Intern,
    }
    let pf2 = &pf;
    // [sqe-m2 arena law] scatter arenas parked across reps, keyed by the
    // column pair (states carry column-bound cursors).
    // [spill-2, shrink law] formerly UNCAPPED and column-keyed (see
    // PARKIGF): scatter-arena class cap, query-agnostic.
    static PARKIG: crate::stencils::statepark::StatePark<Vec<Vec<(u64, u64, u32)>>> = crate::stencils::statepark::StatePark::new(256 << 20);
    let parked: std::sync::Mutex<Vec<Vec<Vec<(u64, u64, u32)>>>> =
        std::sync::Mutex::new(PARKIG.fetch_up_to(pool.threads()));
    let t_p1 = std::time::Instant::now();
    let pass1 = pool.run_finish(
        units.len(),
        |t| {
            let mut buckets = parked.lock().unwrap().pop().unwrap_or_default();
            if buckets.len() != p {
                buckets = (0..p).map(|_| Vec::new()).collect();
            } else {
                buckets.iter_mut().for_each(|b| b.clear());
            }
            S {
                ci: ColState::fetch(a_int),
                kc: CurCache::new(a_text),
                ks: crate::scan::scratch_fetch(),
                codes: vec![0; 8192],
                buckets,
                intern: pm::Intern::new(t),
            }
        },
        |s: &mut S, i| {
            let (pi, g, rows, _) = units[i];
            let rows = rows as usize;
            let du = s.ci.dec(bank, pi, g, rows);
            let du: &[u64] = unsafe { std::slice::from_raw_parts(du.as_ptr(), du.len()) };
            if pf2[pi].dh.is_some() {
                if s.codes.len() < rows {
                    s.codes.resize(rows, 0);
                }
                s.kc.get(bank, pi).decode_codes(g, &mut s.codes[..rows]).expect("codes");
                let codes = &s.codes;
                let mut r = 0usize;
                while r < rows {
                    let (v, c) = (du[r], codes[r]);
                    let mut j = r + 1;
                    while j < rows && du[j] == v && codes[j] == c {
                        j += 1;
                    }
                    s.buckets[(hash64(v) >> shift) as usize].push((
                        v,
                        pm::psk(pi, c),
                        (j - r) as u32,
                    ));
                    r = j;
                }
            } else {
                let d = s.ks.decode_full(s.kc.get(bank, pi), g, rows);
                let d: &[u64] = unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) };
                let mut prev: Option<(u64, u64)> = None;
                for r in 0..rows {
                    let pl = unsafe { varlena_payload(d[r]) };
                    let key = s.intern.key(pl);
                    let v = du[r];
                    if prev == Some((v, key)) {
                        if let Some(last) =
                            s.buckets[(hash64(v) >> shift) as usize].last_mut()
                        {
                            last.2 += 1;
                            continue;
                        }
                    }
                    s.buckets[(hash64(v) >> shift) as usize].push((v, key, 1));
                    prev = Some((v, key));
                }
            }
        },
        |s| {
            s.ci.park();
            crate::scan::scratch_park(s.ks);
            SK { buckets: s.buckets, intern: s.intern }
        },
    );
    crate::engine::phn(node, "pass1", t_p1);
    let t_p2 = std::time::Instant::now();
    let kb = pm::KeyBytes {
        pf: &pf,
        interns: pm::interns_by_slot(pool.threads(), pass1.iter().map(|s| &s.intern)),
    };
    let kbr = &kb;
    let scattered: Vec<&SK> = pass1.iter().collect();
    let kw = topk(node);
    let key_asc = node.params.order == OrderBy::KeyAsc;
    static PARKIGT: crate::stencils::statepark::StatePark<PmCnt128> = crate::stencils::statepark::StatePark::new(64 << 20);
    let owned = pool.run(
        p,
        |_| {
            (
                Vec::new(),
                0u64,
                0u64,
                PARKIGT.fetch().unwrap_or_else(|| PmCnt128::new(16)),
            )
        },
        |(top, rowsc, groups, tbl): &mut (Vec<GidOrdEnt>, u64, u64, PmCnt128),
         part| {
            let n: usize = scattered.iter().map(|s| s.buckets[part].len()).sum();
            if n == 0 {
                return;
            }
            tbl.reset(n.max(16));
            for s in &scattered {
                for &(v, key, c) in &s.buckets[part] {
                    tbl.add(((v as u128) << 41) | key as u128, c);
                }
            }
            // per-v closure (the int lane is HIGH-CARD here — UserID —
            // so cross-part byte compares are rare; stage-2's hash of
            // every fragment's bytes measured 4x worse at 100m).
            let mut all: Vec<(u128, u64)> = Vec::with_capacity(tbl.len);
            for i in 0..tbl.cap() {
                if tbl.cnt[i] != 0 {
                    all.push((tbl.keys[i], tbl.cnt[i] as u64));
                }
            }
            all.sort_unstable_by_key(|e| e.0);
            let mut i = 0usize;
            let mut reps: Vec<(u64, u64)> = Vec::new();
            while i < all.len() {
                let v = (all[i].0 >> 41) as u64;
                reps.clear();
                let mut j = i;
                while j < all.len() && (all[j].0 >> 41) as u64 == v {
                    let key = (all[j].0 & ((1u128 << 41) - 1)) as u64;
                    let c = all[j].1;
                    *rowsc += c;
                    match reps.iter_mut().find(|(q, _)| kbr.same(*q, key)) {
                        Some((_, rc)) => *rc += c,
                        None => reps.push((key, c)),
                    }
                    j += 1;
                }
                *groups += reps.len() as u64;
                for &(key, c) in reps.iter() {
                    topk_gidord(top, kw, key_asc, c, v, key, kbr);
                }
                i = j;
            }
        },
    );
    let rows_counted: u64 = owned.iter().map(|o| o.1).sum();
    assert_eq!(rows_counted, bank.rows_total(), "q{}: rows must cover the bank", node.q);
    let mut tops: Vec<GidOrdEnt> = Vec::new();
    for (o, _, _, tbl) in owned {
        tops.extend(o);
        let b = tbl.keys.capacity() * 16 + tbl.cnt.capacity() * 4;
        PARKIGT.park(tbl, b);
    }
    let all = gidord_finish(tops, key_asc, kw);
    crate::engine::phn(node, "pass2", t_p2);
    let w = col_width(bank, a_int);
    let mut sink = LineSink::new(bank, node);
    for (c, v, _, b) in all.iter().skip(node.params.offset) {
        sink.push(node, &|_| sx(*v, w), Some(b), *c, &|_| 0, 0, 0);
    }
    let out = sink.finish();
    {
        let mut v: Vec<Vec<Vec<(u64, u64, u32)>>> = parked.into_inner().unwrap();
        for st in pass1 {
            v.push(st.buckets);
        }
        for b in v {
            let bytes = crate::stencils::statepark::nested_bytes(&b);
            PARKIG.park(b, bytes);
        }
    }
    out
}

/// (int, text) COUNT(*) with the empty-key drop. Fragments
/// (psk, int) fold exactly per hash partition (no bytes), then the
/// stage-2 string-keyed hash_combine re-joins byte-equal groups across
/// parts; final order (count DESC, gid-order ASC, int ASC).
pub fn int_gid_filtered(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    if ctx.faces.cfg.fpcombine {
        return int_gid_filtered_fp(ctx, node);
    }
    let (bank, pool) = (ctx.bank, ctx.pool);
    assert!(node.agg.iter().all(|a| a.op == AggOp::CountStar));
    let (a_int, a_text) = (node.params.group_cols[0], node.params.group_cols[1]);
    let wi = col_width(bank, a_int);
    let lo_mask = if wi >= 8 { u64::MAX } else { (1u64 << (8 * wi as u32)) - 1 };
    pm::assert_psk_fits(bank);
    let pf = pm::dict_faces(ctx, a_text);
    let units = ctx.faces.walk(bank, a_text);
    let ndv = ndv_face(ctx, a_text);
    let p = partition_count(ndv.max(1), node.params.slot_bytes, node.params.l2_bytes, ctx.pool.threads());
    let shift = 64 - p.trailing_zeros();
    struct S {
        ci: ColState,
        kc: CurCache,
        ks: Scratch,
        codes: Vec<u32>,
        buckets: Vec<Vec<(u64, u64, u32)>>, // (key, int, cnt)
        intern: pm::Intern,
    }
    struct SK {
        buckets: Vec<Vec<(u64, u64, u32)>>,
        intern: pm::Intern,
    }
    let pf2 = &pf;
    let t_p1 = std::time::Instant::now();
    let pass1 = pool.run_finish(
        units.len(),
        |t| S {
            ci: ColState::fetch(a_int),
            kc: CurCache::new(a_text),
            ks: crate::scan::scratch_fetch(),
            codes: vec![0; 8192],
            buckets: (0..p).map(|_| Vec::new()).collect(),
            intern: pm::Intern::new(t),
        },
        |s: &mut S, i| {
            let (pi, g, rows, _) = units[i];
            let rows = rows as usize;
            let du = s.ci.dec(bank, pi, g, rows);
            let du: &[u64] = unsafe { std::slice::from_raw_parts(du.as_ptr(), du.len()) };
            if pf2[pi].dh.is_some() {
                if s.codes.len() < rows {
                    s.codes.resize(rows, 0);
                }
                s.kc.get(bank, pi).decode_codes(g, &mut s.codes[..rows]).expect("codes");
                let empty = pf2[pi].empty_code;
                let codes = &s.codes;
                let mut r = 0usize;
                while r < rows {
                    let (v, c) = (du[r] & lo_mask, codes[r]);
                    let mut j = r + 1;
                    while j < rows && (du[j] & lo_mask) == v && codes[j] == c {
                        j += 1;
                    }
                    if Some(c) != empty {
                        let key = pm::psk(pi, c);
                        s.buckets[(hash64(key ^ v.rotate_left(32)) >> shift) as usize]
                            .push((key, v, (j - r) as u32));
                    }
                    r = j;
                }
            } else {
                let d = s.ks.decode_full(s.kc.get(bank, pi), g, rows);
                let d: &[u64] = unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) };
                for r in 0..rows {
                    let pl = unsafe { varlena_payload(d[r]) };
                    if pl.is_empty() {
                        continue;
                    }
                    let key = s.intern.key(pl);
                    let v = du[r] & lo_mask;
                    s.buckets[(hash64(key ^ v.rotate_left(32)) >> shift) as usize]
                        .push((key, v, 1));
                }
            }
        },
        |s| {
            s.ci.park();
            crate::scan::scratch_park(s.ks);
            SK { buckets: s.buckets, intern: s.intern }
        },
    );
    crate::engine::phn(node, "pass1", t_p1);
    let t_p2 = std::time::Instant::now();
    let kb = pm::KeyBytes {
        pf: &pf,
        interns: pm::interns_by_slot(pool.threads(), pass1.iter().map(|s| &s.intern)),
    };
    let kbr = &kb;
    let scattered: Vec<&SK> = pass1.iter().collect();
    let kw = topk(node);
    let owned = pool.run(
        p,
        |_| (Vec::new(), PmCnt128::new(16)),
        |(out, tbl): &mut (Vec<pm::Frag>, PmCnt128), part| {
            let n: usize = scattered.iter().map(|s| s.buckets[part].len()).sum();
            if n == 0 {
                return;
            }
            tbl.reset(n.max(16));
            for s in &scattered {
                for &(key, v, c) in &s.buckets[part] {
                    tbl.add(((key as u128) << 64) | v as u128, c);
                }
            }
            for i in 0..tbl.cap() {
                if tbl.cnt[i] != 0 {
                    out.push(((tbl.keys[i] >> 64) as u64, tbl.keys[i] as u64, tbl.cnt[i] as u64));
                }
            }
        },
    );
    let frags: Vec<Vec<pm::Frag>> = owned.into_iter().map(|o| o.0).collect();
    let (cands, _, _) = pm::hash_combine(pool, &frags, kbr, kw);
    // (count DESC, gid-order ASC, int ASC) — gid-order = (reg_ord, bytes).
    let mut all: Vec<(u64, u64, &[u8], u64)> = cands
        .iter()
        .map(|&(c, key, v)| {
            let b = kbr.bytes(key);
            (c, pm::reg_ord(b), b, v)
        })
        .collect();
    all.sort_unstable_by(|x, y| {
        y.0.cmp(&x.0)
            .then_with(|| x.1.cmp(&y.1))
            .then_with(|| x.2.cmp(y.2))
            .then_with(|| x.3.cmp(&y.3))
    });
    all.truncate(kw);
    crate::engine::phn(node, "pass2", t_p2);
    let mut sink = LineSink::new(bank, node);
    for (c, _, b, v) in all.iter().skip(node.params.offset) {
        sink.push(node, &|_| sx(*v, wi), Some(b), *c, &|_| 0, 0, 0);
    }
    let out = sink.finish();
    pool.drop_par(pass1);
    out
}

/// (int, minute, text) COUNT(*). The ord/minute lanes stay in
/// the ord-remap face (an ORDER-PRESERVING int face, untouched); the text
/// lane carries part-scoped codes. Scatter by (ord, minute) hash — a
/// group's rows share both — owners close the cross-part boundary per
/// (ord, minute) run.
pub fn ord_pack(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    use crate::kernels_g2::{minute_of, ord_remap_memo};
    let (bank, pool) = (ctx.bank, ctx.pool);
    assert!(node.agg.iter().all(|a| a.op == AggOp::CountStar));
    let a_time = node
        .params
        .key_exprs
        .iter()
        .find_map(|e| match e {
            KeyExpr::Minute(col) => Some(*col),
            _ => None,
        })
        .expect("ord_pack: Minute key element");
    let a_int = *node
        .params
        .group_cols
        .iter()
        .find(|&&c| c != a_time && col_width(bank, c) > 0)
        .expect("ord_pack: byval key col");
    let a_text = *node
        .params
        .group_cols
        .iter()
        .find(|&&c| col_width(bank, c) == 0)
        .expect("ord_pack: varlena key col");
    pm::assert_psk_fits(bank);
    let pf = pm::dict_faces(ctx, a_text);
    let units = ctx.faces.walk(bank, a_text);
    let orm = ord_remap_memo(ctx.faces, bank, a_int, pool);
    let ndv_hint =
        (ctx.faces.stats(bank, a_int).ndv_est_sum() as usize).max(1) * 4;
    let p = partition_count(ndv_hint, node.params.slot_bytes, node.params.l2_bytes, ctx.pool.threads());
    let shift = 64 - p.trailing_zeros();
    struct S {
        ct: ColState,
        kc: CurCache,
        ks: Scratch,
        codes: Vec<u32>,
        buckets: Vec<Vec<(u64, u64, u32)>>, // (om, key, cnt)
        intern: pm::Intern,
    }
    struct SK {
        buckets: Vec<Vec<(u64, u64, u32)>>,
        intern: pm::Intern,
    }
    let pf2 = &pf;
    let orm2 = &orm;
    // [spill-2, shrink law] formerly UNCAPPED and column-keyed (see
    // PARKIGF): scatter-arena class cap, query-agnostic.
    static PARKOP: crate::stencils::statepark::StatePark<Vec<Vec<(u64, u64, u32)>>> = crate::stencils::statepark::StatePark::new(256 << 20);
    let parked: std::sync::Mutex<Vec<Vec<Vec<(u64, u64, u32)>>>> =
        std::sync::Mutex::new(PARKOP.fetch_up_to(pool.threads()));
    let t_p1 = std::time::Instant::now();
    let pass1 = pool.run_finish(
        units.len(),
        |t| {
            let mut buckets = parked.lock().unwrap().pop().unwrap_or_default();
            if buckets.len() != p {
                buckets = (0..p).map(|_| Vec::new()).collect();
            } else {
                buckets.iter_mut().for_each(|b| b.clear());
            }
            S {
                ct: ColState::fetch(a_time),
                kc: CurCache::new(a_text),
                ks: crate::scan::scratch_fetch(),
                codes: vec![0; 8192],
                buckets,
                intern: pm::Intern::new(t),
            }
        },
        |s: &mut S, i| {
            let (pi, g, rows, base) = units[i];
            let (rows, base) = (rows as usize, base as usize);
            let dt = s.ct.dec(bank, pi, g, rows).to_vec();
            let ords = &orm2.ords[base..base + rows];
            if pf2[pi].dh.is_some() {
                if s.codes.len() < rows {
                    s.codes.resize(rows, 0);
                }
                s.kc.get(bank, pi).decode_codes(g, &mut s.codes[..rows]).expect("codes");
                let codes = &s.codes;
                let mut prev: Option<(u64, u64)> = None;
                for r in 0..rows {
                    let om = ((ords[r] as u64) << 6) | minute_of(dt[r] as i64);
                    let key = pm::psk(pi, codes[r]);
                    if prev == Some((om, key)) {
                        if let Some(last) =
                            s.buckets[(hash64(om) >> shift) as usize].last_mut()
                        {
                            last.2 += 1;
                            continue;
                        }
                    }
                    s.buckets[(hash64(om) >> shift) as usize].push((om, key, 1));
                    prev = Some((om, key));
                }
            } else {
                let d = s.ks.decode_full(s.kc.get(bank, pi), g, rows);
                let d: &[u64] = unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) };
                for r in 0..rows {
                    let om = ((ords[r] as u64) << 6) | minute_of(dt[r] as i64);
                    let pl = unsafe { varlena_payload(d[r]) };
                    let key = s.intern.key(pl);
                    s.buckets[(hash64(om) >> shift) as usize].push((om, key, 1));
                }
            }
        },
        |s| {
            s.ct.park();
            crate::scan::scratch_park(s.ks);
            SK { buckets: s.buckets, intern: s.intern }
        },
    );
    crate::engine::phn(node, "pass1", t_p1);
    let t_p2 = std::time::Instant::now();
    let kb = pm::KeyBytes {
        pf: &pf,
        interns: pm::interns_by_slot(pool.threads(), pass1.iter().map(|s| &s.intern)),
    };
    let kbr = &kb;
    let scattered: Vec<&SK> = pass1.iter().collect();
    let kw = topk(node);
    let key_asc = node.params.order == OrderBy::KeyAsc;
    static PARKOPT: crate::stencils::statepark::StatePark<PmCnt128> = crate::stencils::statepark::StatePark::new(64 << 20);
    let owned = pool.run(
        p,
        |_| {
            (
                Vec::new(),
                0u64,
                PARKOPT.fetch().unwrap_or_else(|| PmCnt128::new(16)),
            )
        },
        |(top, rowsc, tbl): &mut (Vec<GidOrdEnt>, u64, PmCnt128), part| {
            let n: usize = scattered.iter().map(|s| s.buckets[part].len()).sum();
            if n == 0 {
                return;
            }
            tbl.reset(n.max(16));
            for s in &scattered {
                for &(om, key, c) in &s.buckets[part] {
                    tbl.add(((om as u128) << 41) | key as u128, c);
                }
            }
            // per-om closure — (ord, minute) is high-card; see int_gid.
            let mut all: Vec<(u128, u64)> = Vec::with_capacity(tbl.len);
            for i in 0..tbl.cap() {
                if tbl.cnt[i] != 0 {
                    all.push((tbl.keys[i], tbl.cnt[i] as u64));
                }
            }
            all.sort_unstable_by_key(|e| e.0);
            let mut i = 0usize;
            let mut reps: Vec<(u64, u64)> = Vec::new();
            while i < all.len() {
                let om = (all[i].0 >> 41) as u64;
                reps.clear();
                let mut j = i;
                while j < all.len() && (all[j].0 >> 41) as u64 == om {
                    let key = (all[j].0 & ((1u128 << 41) - 1)) as u64;
                    let c = all[j].1;
                    *rowsc += c;
                    match reps.iter_mut().find(|(q, _)| kbr.same(*q, key)) {
                        Some((_, rc)) => *rc += c,
                        None => reps.push((key, c)),
                    }
                    j += 1;
                }
                for &(key, c) in reps.iter() {
                    topk_gidord(top, kw, key_asc, c, om, key, kbr);
                }
                i = j;
            }
        },
    );
    let rows_counted: u64 = owned.iter().map(|o| o.1).sum();
    assert_eq!(rows_counted, bank.rows_total(), "q{}: rows must cover the bank", node.q);
    let mut tops: Vec<GidOrdEnt> = Vec::new();
    {
        for (o, _, tbl) in owned {
            tops.extend(o);
            let b = tbl.keys.capacity() * 16 + tbl.cnt.capacity() * 4;
            PARKOPT.park(tbl, b);
        }
    }
    let all = gidord_finish(tops, key_asc, kw);
    crate::engine::phn(node, "pass2", t_p2);
    let wi = col_width(bank, a_int);
    // Typed emit in key_exprs order: the int key lane, the minute bucket
    // (a plain small-int projection — INT8 render), the text lane, aggs.
    let mut out_cols: Vec<OutCol> = Vec::new();
    for e in &node.params.key_exprs {
        match e {
            KeyExpr::Col(cc) if *cc == a_int => out_cols.push(OutCol::I(node.ty_of(a_int), Vec::new())),
            KeyExpr::Minute(_) => out_cols.push(OutCol::I(TypMeta::INT8, Vec::new())),
            KeyExpr::Col(cc) => out_cols.push(OutCol::B(node.ty_of(*cc), BytesBuild::new())),
            other => panic!("ord_pack render: unsupported key expr {other:?}"),
        }
    }
    for a in &node.agg {
        match a.op {
            AggOp::CountStar => out_cols.push(OutCol::I(TypMeta::INT8, Vec::new())),
            other => panic!("ord_pack render: unsupported agg {other:?}"),
        }
    }
    for (c, om, _, b) in all.iter().skip(node.params.offset) {
        let mut ci = 0usize;
        for e in &node.params.key_exprs {
            match (e, &mut out_cols[ci]) {
                (KeyExpr::Col(cc), OutCol::I(_, v)) if *cc == a_int => {
                    v.push(sx(orm.vals[(om >> 6) as usize], wi))
                }
                (KeyExpr::Minute(_), OutCol::I(_, v)) => v.push((om & 0x3F) as i64),
                (KeyExpr::Col(_), OutCol::B(_, bb)) => bb.push(b),
                _ => unreachable!("ord_pack emit shape"),
            }
            ci += 1;
        }
        match &mut out_cols[ci] {
            OutCol::I(_, v) => v.push(*c as i64),
            _ => unreachable!(),
        }
    }
    let out = AnswerSet::from_cols(
        out_cols
            .into_iter()
            .map(|c| match c {
                OutCol::I(ty, v) => AnswerCol::i64s(ty, v),
                OutCol::B(ty, b) => b.finish(ty),
                OutCol::R(ty, pairs) => AnswerCol {
                    ty,
                    data: ColData::Ratio { pairs, exact: false },
                    validity: Validity::AllValid,
                },
            })
            .collect(),
    );
    {
        let mut v: Vec<Vec<Vec<(u64, u64, u32)>>> = parked.into_inner().unwrap();
        for st in pass1 {
            v.push(st.buckets);
        }
        for b in v {
            let bytes = crate::stencils::statepark::nested_bytes(&b);
            PARKOP.park(b, bytes);
        }
    }
    out
}

/// (text, text) COUNT(*) — the unseen gid-pair shape. No reference-workload query
/// exercises it, so the per-part arm optimizes for correctness: part-
/// scoped key pairs fold exactly, then ONE string-keyed combine at group
/// grain re-joins byte-equal pairs.
pub fn gid_pair(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    let (bank, pool) = (ctx.bank, ctx.pool);
    assert!(node.agg.iter().all(|a| a.op == AggOp::CountStar));
    let (a0, a1) = (node.params.group_cols[0], node.params.group_cols[1]);
    pm::assert_psk_fits(bank);
    let pf0 = pm::dict_faces(ctx, a0);
    let pf1 = pm::dict_faces(ctx, a1);
    let drop0 = node.params.ne_empty_cols.contains(&a0);
    let drop1 = node.params.ne_empty_cols.contains(&a1);
    let units = ctx.faces.walk(bank, a0);
    struct S {
        kc0: CurCache,
        kc1: CurCache,
        ks0: Scratch,
        ks1: Scratch,
        codes0: Vec<u32>,
        codes1: Vec<u32>,
        buckets: Vec<Vec<(u128, u32)>>,
        in0: pm::Intern,
        in1: pm::Intern,
    }
    struct SK {
        buckets: Vec<Vec<(u128, u32)>>,
        in0: pm::Intern,
        in1: pm::Intern,
    }
    let (pf0r, pf1r) = (&pf0, &pf1);
    let pass1 = pool.run_finish(
        units.len(),
        |t| S {
            kc0: CurCache::new(a0),
            kc1: CurCache::new(a1),
            ks0: crate::scan::scratch_fetch(),
            ks1: crate::scan::scratch_fetch(),
            codes0: vec![0; 8192],
            codes1: vec![0; 8192],
            buckets: (0..RADIX_P).map(|_| Vec::new()).collect(),
            in0: pm::Intern::new(t),
            in1: pm::Intern::new(t),
        },
        |s: &mut S, i| {
            let (pi, g, rows, _) = units[i];
            let rows = rows as usize;
            // key lane 0
            let d0: Vec<u64> = if pf0r[pi].dh.is_some() {
                if s.codes0.len() < rows {
                    s.codes0.resize(rows, 0);
                }
                s.kc0.get(bank, pi).decode_codes(g, &mut s.codes0[..rows]).expect("codes");
                let empty = pf0r[pi].empty_code;
                s.codes0[..rows]
                    .iter()
                    .map(|&c| {
                        if drop0 && Some(c) == empty {
                            u64::MAX
                        } else {
                            pm::psk(pi, c)
                        }
                    })
                    .collect()
            } else {
                let d = s.ks0.decode_full(s.kc0.get(bank, pi), g, rows);
                let d: &[u64] = unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) };
                d.iter()
                    .map(|&x| {
                        let pl = unsafe { varlena_payload(x) };
                        if drop0 && pl.is_empty() {
                            u64::MAX
                        } else {
                            s.in0.key(pl)
                        }
                    })
                    .collect()
            };
            let d1: Vec<u64> = if pf1r[pi].dh.is_some() {
                if s.codes1.len() < rows {
                    s.codes1.resize(rows, 0);
                }
                s.kc1.get(bank, pi).decode_codes(g, &mut s.codes1[..rows]).expect("codes");
                let empty = pf1r[pi].empty_code;
                s.codes1[..rows]
                    .iter()
                    .map(|&c| {
                        if drop1 && Some(c) == empty {
                            u64::MAX
                        } else {
                            pm::psk(pi, c)
                        }
                    })
                    .collect()
            } else {
                let d = s.ks1.decode_full(s.kc1.get(bank, pi), g, rows);
                let d: &[u64] = unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) };
                d.iter()
                    .map(|&x| {
                        let pl = unsafe { varlena_payload(x) };
                        if drop1 && pl.is_empty() {
                            u64::MAX
                        } else {
                            s.in1.key(pl)
                        }
                    })
                    .collect()
            };
            for r in 0..rows {
                if d0[r] == u64::MAX || d1[r] == u64::MAX {
                    continue;
                }
                let key = ((d0[r] as u128) << 41) | d1[r] as u128;
                s.buckets[radix_of(hash64(d0[r] ^ d1[r].rotate_left(21)))]
                    .push((key, 1));
            }
        },
        |s| {
            crate::scan::scratch_park(s.ks0);
            crate::scan::scratch_park(s.ks1);
            SK { buckets: s.buckets, in0: s.in0, in1: s.in1 }
        },
    );
    let kb0 = pm::KeyBytes {
        pf: &pf0,
        interns: pm::interns_by_slot(pool.threads(), pass1.iter().map(|s| &s.in0)),
    };
    let kb1 = pm::KeyBytes {
        pf: &pf1,
        interns: pm::interns_by_slot(pool.threads(), pass1.iter().map(|s| &s.in1)),
    };
    let scattered: Vec<&SK> = pass1.iter().collect();
    let owned = pool.run(
        RADIX_P,
        |_| (Vec::new(), PmCnt128::new(16)),
        |(out, tbl): &mut (Vec<(u128, u64)>, PmCnt128), part| {
            let n: usize = scattered.iter().map(|s| s.buckets[part].len()).sum();
            if n == 0 {
                return;
            }
            tbl.reset(n.max(16));
            for s in &scattered {
                for &(key, c) in &s.buckets[part] {
                    tbl.add(key, c);
                }
            }
            for i in 0..tbl.cap() {
                if tbl.cnt[i] != 0 {
                    out.push((tbl.keys[i], tbl.cnt[i] as u64));
                }
            }
        },
    );
    // String-keyed combine at group grain (correctness-first: unseen-only).
    type Fx2 = std::hash::BuildHasherDefault<crate::kernels_f6::FxHasher>;
    let mut merged: std::collections::HashMap<(Vec<u8>, Vec<u8>), u64, Fx2> =
        Default::default();
    for (out, _) in &owned {
        for &(key, c) in out {
            let k0 = (key >> 41) as u64;
            let k1 = (key & ((1u128 << 41) - 1)) as u64;
            *merged
                .entry((kb0.bytes(k0).to_vec(), kb1.bytes(k1).to_vec()))
                .or_insert(0) += c;
        }
    }
    let mut rows: Vec<((Vec<u8>, Vec<u8>), u64)> = merged.into_iter().collect();
    rows.sort_unstable_by(|a, b| {
        b.1.cmp(&a.1).then_with(|| a.0 .0.cmp(&b.0 .0)).then_with(|| a.0 .1.cmp(&b.0 .1))
    });
    let k = topk(node);
    rows.truncate(k);
    let mut kb0o = BytesBuild::new();
    let mut kb1o = BytesBuild::new();
    let mut cnts: Vec<i64> = Vec::new();
    for ((b0, b1), c) in rows.iter().skip(node.params.offset) {
        kb0o.push(b0);
        kb1o.push(b1);
        cnts.push(*c as i64);
    }
    AnswerSet::from_cols(vec![
        kb0o.finish(node.ty_of(a0)),
        kb1o.finish(node.ty_of(a1)),
        AnswerCol::i64s(TypMeta::INT8, cnts),
    ])
}

/// [tpch-wave-3] (text, text) grouped folds: gid-pair keys carrying
/// exact AccumCell payloads, word-term predicates, the fused shapes;
/// ONE byte-keyed combine re-joins part-scoped keys.
pub fn gid_pair_cells(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    use crate::fold::{
        combine_cell_fold, fold_op_of, scatter_cell_fold, AccumCell, AggFoldOp,
    };
    use crate::grouped::Cells128;
    let (bank, pool) = (ctx.bank, ctx.pool);
    debug_assert!(node.params.having.is_none(), "having is the seam's here");
    let (a0, a1) = (node.params.group_cols[0], node.params.group_cols[1]);
    pm::assert_psk_fits(bank);
    let pf0 = pm::dict_faces(ctx, a0);
    let pf1 = pm::dict_faces(ctx, a1);
    let drop0 = node.params.ne_empty_cols.contains(&a0);
    let drop1 = node.params.ne_empty_cols.contains(&a1);
    let units = ctx.faces.walk(bank, a0);
    let na = node.agg.len();

    fn ci_of(cols: &mut Vec<u32>, c: u32) -> usize {
        cols.iter().position(|&x| x == c).unwrap_or_else(|| {
            cols.push(c);
            cols.len() - 1
        })
    }
    struct CLane {
        ai: usize,
        op: AggFoldOp,
        cis: [usize; 3],
        ex: Option<FoldExpr>,
    }
    let mut cols: Vec<u32> = Vec::new();
    let mut lanes: Vec<CLane> = Vec::new();
    for (ai, a) in node.agg.iter().enumerate() {
        let Some(op) = fold_op_of(a.op) else { continue };
        let c = a.col.expect("planner: fold aggs carry an input column");
        let mut cis = [usize::MAX; 3];
        cis[0] = ci_of(&mut cols, c);
        if let Some(c2) = a.col2() {
            cis[1] = ci_of(&mut cols, c2);
        }
        if let Some(c3) = a.col3() {
            cis[2] = ci_of(&mut cols, c3);
        }
        lanes.push(CLane { ai, op, cis, ex: a.expr });
    }
    let pred_terms: &[PredTerm] =
        node.pred.as_ref().map(|p| p.terms.as_slice()).unwrap_or(&[]);
    debug_assert!(
        node.pred
            .as_ref()
            .map(|p| p.var_terms.is_empty() && p.col_terms.is_empty())
            .unwrap_or(true),
        "cells arm admits word conjuncts only"
    );
    let pred_cis: Vec<usize> =
        pred_terms.iter().map(|t| ci_of(&mut cols, t.col)).collect();
    let faces_c: Vec<crate::bank::Face> = cols.iter().map(|&c| bank.face(c)).collect();
    let col_nf: Vec<bool> = cols.iter().map(|&c| bank.null_free(c)).collect();
    let ncols = cols.len();
    let (colsr, lanesr, faces_cr, col_nfr, pred_cisr) =
        (&cols, &lanes, &faces_c, &col_nf, &pred_cis);
    let (pf0r, pf1r) = (&pf0, &pf1);

    struct S {
        kc0: CurCache,
        kc1: CurCache,
        ks0: Scratch,
        ks1: Scratch,
        codes0: Vec<u32>,
        codes1: Vec<u32>,
        in0: pm::Intern,
        in1: pm::Intern,
        scr: Vec<Scratch>,
        cc: Vec<CurCache>,
        tab: Cells128,
        slots: Vec<u32>,
        live: Vec<bool>,
        msel: Vec<u16>,
    }
    let pass1 = pool.run_finish(
        units.len(),
        |t| S {
            kc0: CurCache::new(a0),
            kc1: CurCache::new(a1),
            ks0: crate::scan::scratch_fetch(),
            ks1: crate::scan::scratch_fetch(),
            codes0: vec![0; 8192],
            codes1: vec![0; 8192],
            in0: pm::Intern::new(t),
            in1: pm::Intern::new(t),
            scr: (0..ncols).map(|_| crate::scan::scratch_fetch()).collect(),
            cc: colsr.iter().map(|&a| CurCache::new(a)).collect(),
            tab: Cells128::new(1024, na),
            slots: Vec::new(),
            live: Vec::new(),
            msel: Vec::new(),
        },
        |s: &mut S, i| {
            let (pi, g, rows, _) = units[i];
            let rows = rows as usize;
            // gid_pair's key decode law verbatim, both lanes.
            let d0: Vec<u64> = if pf0r[pi].dh.is_some() {
                if s.codes0.len() < rows {
                    s.codes0.resize(rows, 0);
                }
                s.kc0.get(bank, pi).decode_codes(g, &mut s.codes0[..rows]).expect("codes");
                let empty = pf0r[pi].empty_code;
                s.codes0[..rows]
                    .iter()
                    .map(|&c| {
                        if drop0 && Some(c) == empty {
                            u64::MAX
                        } else {
                            pm::psk(pi, c)
                        }
                    })
                    .collect()
            } else {
                let d = s.ks0.decode_full(s.kc0.get(bank, pi), g, rows);
                let d: &[u64] = unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) };
                d.iter()
                    .map(|&x| {
                        let pl = unsafe { varlena_payload(x) };
                        if drop0 && pl.is_empty() {
                            u64::MAX
                        } else {
                            s.in0.key(pl)
                        }
                    })
                    .collect()
            };
            let d1: Vec<u64> = if pf1r[pi].dh.is_some() {
                if s.codes1.len() < rows {
                    s.codes1.resize(rows, 0);
                }
                s.kc1.get(bank, pi).decode_codes(g, &mut s.codes1[..rows]).expect("codes");
                let empty = pf1r[pi].empty_code;
                s.codes1[..rows]
                    .iter()
                    .map(|&c| {
                        if drop1 && Some(c) == empty {
                            u64::MAX
                        } else {
                            pm::psk(pi, c)
                        }
                    })
                    .collect()
            } else {
                let d = s.ks1.decode_full(s.kc1.get(bank, pi), g, rows);
                let d: &[u64] = unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) };
                d.iter()
                    .map(|&x| {
                        let pl = unsafe { varlena_payload(x) };
                        if drop1 && pl.is_empty() {
                            u64::MAX
                        } else {
                            s.in1.key(pl)
                        }
                    })
                    .collect()
            };
            let mut gvs: Vec<crate::scan::GranValid> = Vec::with_capacity(ncols);
            let mut ds: Vec<&[u64]> = Vec::with_capacity(ncols);
            for (ci, (scr, cc)) in s.scr.iter_mut().zip(s.cc.iter_mut()).enumerate() {
                let cur = cc.get(bank, pi);
                let need_v = !col_nfr[ci];
                gvs.push(if need_v {
                    scr.validity(cur, g, rows)
                } else {
                    crate::scan::GranValid::AllValid
                });
                let d = scr.decode_full(cur, g, rows);
                ds.push(unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) });
            }
            // Pass A: survivors + key pack + touch.
            debug_assert!(rows <= 1 << 16, "granule exceeds the u16 selection law");
            let S { scr, tab, slots, live, msel, .. } = s;
            msel.clear();
            msel.extend((0..rows).map(|r| r as u16));
            for (ti, t) in pred_terms.iter().enumerate() {
                let ci = pred_cisr[ti];
                let d = ds[ci];
                let face = faces_cr[ci];
                if col_nfr[ci] || gvs[ci].all_valid() {
                    t.filter_sel(msel, |_| true, |r| face.word_key(d[r]));
                } else {
                    let sc = &scr[ci];
                    t.filter_sel(msel, |r| sc.row_valid(r), |r| face.word_key(d[r]));
                }
            }
            if live.len() < rows {
                live.resize(rows, false);
            }
            live[..rows].fill(false);
            for &r16 in msel.iter() {
                live[r16 as usize] = true;
            }
            tab.reserve_batch(rows);
            if slots.len() < rows {
                slots.resize(rows, 0);
            }
            for r in 0..rows {
                if !live[r] {
                    continue;
                }
                if d0[r] == u64::MAX || d1[r] == u64::MAX {
                    live[r] = false;
                    continue;
                }
                let key = ((d0[r] as u128) << 41) | d1[r] as u128;
                slots[r] = tab.touch(key, 1) as u32;
            }
            // Pass B: one shape dispatch per (granule, lane); strict 3VL.
            for l in lanesr {
                let da = ds[l.cis[0]];
                let fa = faces_cr[l.cis[0]];
                let a_allv = col_nfr[l.cis[0]] || gvs[l.cis[0]].all_valid();
                let cells = &mut tab.cells[..];
                match l.ex {
                    None => {
                        for r in 0..rows {
                            if !live[r] {
                                continue;
                            }
                            let ok = a_allv || scr[l.cis[0]].row_valid(r);
                            let w = if ok { fa.word_key(da[r]) } else { 0 };
                            scatter_cell_fold(
                                l.op,
                                &mut cells[slots[r] as usize * na + l.ai],
                                w,
                                ok,
                            );
                        }
                    }
                    Some(e) => {
                        let cb = l.cis[1];
                        let db = ds[cb];
                        let fb = faces_cr[cb];
                        let b_allv = col_nfr[cb] || gvs[cb].all_valid();
                        match e {
                            // a·b is the (k=0, sgn=+1) point of a·(k±b).
                            FoldExpr::MulCC { .. }
                            | FoldExpr::MulKSub { .. }
                            | FoldExpr::PackedMulK { .. } => {
                                let (k, sgn) = match e {
                                    FoldExpr::MulCC { .. } => (0i64, 1i64),
                                    FoldExpr::MulKSub { k, .. } => (k, -1),
                                    FoldExpr::PackedMulK { k, sub, .. } => {
                                        (k, if sub { -1 } else { 1 })
                                    }
                                    FoldExpr::PackedMulK2 { .. } => unreachable!(),
                                };
                                for r in 0..rows {
                                    if !live[r] {
                                        continue;
                                    }
                                    let ok = (a_allv || scr[l.cis[0]].row_valid(r))
                                        && (b_allv || scr[cb].row_valid(r));
                                    if !ok {
                                        continue;
                                    }
                                    let w = fa.word_key(da[r])
                                        * (k + sgn * fb.word_key(db[r]));
                                    scatter_cell_fold(
                                        l.op,
                                        &mut cells[slots[r] as usize * na + l.ai],
                                        w,
                                        true,
                                    );
                                }
                            }
                            FoldExpr::PackedMulK2 { k1, sub1, k2, sub2, .. } => {
                                let cc3 = l.cis[2];
                                let dc = ds[cc3];
                                let fc = faces_cr[cc3];
                                let c_allv = col_nfr[cc3] || gvs[cc3].all_valid();
                                let s1 = if sub1 { -1i64 } else { 1i64 };
                                let s2 = if sub2 { -1i64 } else { 1i64 };
                                for r in 0..rows {
                                    if !live[r] {
                                        continue;
                                    }
                                    let ok = (a_allv || scr[l.cis[0]].row_valid(r))
                                        && (b_allv || scr[cb].row_valid(r))
                                        && (c_allv || scr[cc3].row_valid(r));
                                    if !ok {
                                        continue;
                                    }
                                    let w = fa.word_key(da[r])
                                        * (k1 + s1 * fb.word_key(db[r]))
                                        * (k2 + s2 * fc.word_key(dc[r]));
                                    scatter_cell_fold(
                                        l.op,
                                        &mut cells[slots[r] as usize * na + l.ai],
                                        w,
                                        true,
                                    );
                                }
                            }
                        }
                    }
                }
            }
        },
        |s| {
            crate::scan::scratch_park(s.ks0);
            crate::scan::scratch_park(s.ks1);
            s.scr.into_iter().for_each(crate::scan::scratch_park);
            (s.tab, s.in0, s.in1)
        },
    );
    let kb0 = pm::KeyBytes {
        pf: &pf0,
        interns: pm::interns_by_slot(pool.threads(), pass1.iter().map(|(_, i0, _)| i0)),
    };
    let kb1 = pm::KeyBytes {
        pf: &pf1,
        interns: pm::interns_by_slot(pool.threads(), pass1.iter().map(|(_, _, i1)| i1)),
    };
    type Fx2 = std::hash::BuildHasherDefault<crate::kernels_f6::FxHasher>;
    let mut merged: std::collections::HashMap<
        (Vec<u8>, Vec<u8>),
        (u64, Vec<AccumCell>),
        Fx2,
    > = Default::default();
    for (tab, _, _) in &pass1 {
        tab.for_each(|key, cnt, cells| {
            let k0 = (key >> 41) as u64;
            let k1 = (key & ((1u128 << 41) - 1)) as u64;
            let e = merged
                .entry((kb0.bytes(k0).to_vec(), kb1.bytes(k1).to_vec()))
                .or_insert_with(|| (0, vec![AccumCell::default(); na]));
            e.0 += cnt;
            for l in &lanes {
                combine_cell_fold(l.op, &mut e.1[l.ai], &cells[l.ai]);
            }
        });
    }
    let mut rows_v: Vec<((Vec<u8>, Vec<u8>), u64, Vec<AccumCell>)> =
        merged.into_iter().map(|(k, (c, cl))| (k, c, cl)).collect();
    rows_v.sort_unstable_by(|a, b| {
        b.1.cmp(&a.1).then_with(|| a.0 .0.cmp(&b.0 .0)).then_with(|| a.0 .1.cmp(&b.0 .1))
    });
    let k = topk(node);
    rows_v.truncate(k);
    let window: Vec<&((Vec<u8>, Vec<u8>), u64, Vec<AccumCell>)> =
        rows_v.iter().skip(node.params.offset).collect();
    let mut kb0o = BytesBuild::new();
    let mut kb1o = BytesBuild::new();
    for r in &window {
        kb0o.push(&r.0 .0);
        kb1o.push(&r.0 .1);
    }
    let mut cols_out: Vec<AnswerCol> = Vec::with_capacity(2 + na);
    cols_out.push(kb0o.finish(node.ty_of(a0)));
    cols_out.push(kb1o.finish(node.ty_of(a1)));
    for (ai, a) in node.agg.iter().enumerate() {
        cols_out.push(match a.op {
            AggOp::CountStar => AnswerCol::i64s(
                TypMeta::INT8,
                window.iter().map(|r| r.1 as i64).collect(),
            ),
            AggOp::Sum => {
                let mask: Vec<bool> = window.iter().map(|r| r.2[ai].b > 0).collect();
                let validity = if mask.iter().all(|&x| x) {
                    Validity::AllValid
                } else {
                    Validity::Mask(mask)
                };
                AnswerCol {
                    ty: a.out,
                    data: ColData::I128(window.iter().map(|r| r.2[ai].a).collect()),
                    validity,
                }
            }
            AggOp::Avg => AnswerCol::ratios(
                a.out,
                window.iter().map(|r| (r.2[ai].a, r.2[ai].b)).collect(),
                a.avg_exact(),
            ),
            AggOp::Min | AggOp::Max => AnswerCol::i64s_opt(
                a.out,
                window.iter().map(|r| crate::fold::minmax_answer(&r.2[ai])).collect(),
            ),
            other => panic!("gid_pair_cells: unsupported agg {other:?} (admission gap)"),
        });
    }
    AnswerSet::from_cols(cols_out)
}

// ---------------------------------------------------------------------------
// [spill-2] The byte-key grouped spill arm (spill-design.md §6 residue
// rung 1: the hash_group tuned text arms). Serves the varlena-key
// CountStar vocabulary — text128 ([0]), int_gid/int_gid_filtered
// ([w,0], with/without the empty-key drop), gid_pair ([0,0]) — under the
// SAME three moves as the SoA arm: account, flush-at-share, merge-at-
// finalize. Elected by `hash_plane::bytes_spill_engaged` (over the E18
// budget, or cap-retire-relied shapes: unwitnessed/over-cap group
// counts). Answers are byte-identical to the tuned arms: the fold is the
// same integer count law and the final sort replicates each shape's
// exact tie order (text128 (count DESC, bytes ASC); int_gid (count DESC,
// int ASC, gid-order ASC) / KeyAsc (int ASC, gid-order ASC); filtered
// (count DESC, gid-order ASC, int ASC); gid_pair (count DESC, bytes0
// ASC, bytes1 ASC) — gid-order = (reg_ord, bytes), part_merge law).
//
// Currency note ("mind the fp/dict currency"): dict parts stay in code
// currency through the granule loop (decode_codes + run collapse) and
// resolve entry BYTES only once per collapsed run at scatter; runs on
// disk are BYTE-KEY records — identity across parts/runs is byte
// equality itself, never a fingerprint — so the merge combine can never
// split or alias a group. Record layout (R2: one compile-time layout for
// the arm): [cnt u32][v u64][la u32][lb u32][bytesA][bytesB]; scatter
// records and sorted-run records share it (runs carry cnt u64 — see
// RREC_HDR).
// ---------------------------------------------------------------------------

/// Byte-key spill engagement census (rig gates prove the legs ran).
pub static BSPILL_FLUSHES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static BSPILL_DRAINS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static BSPILL_MERGES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Scatter record header: [cnt u32][v u64][la u32][lb u32].
const SREC_HDR: usize = 20;
/// Sorted-run record header: [cnt u64][v u64][la u32][lb u32].
const RREC_HDR: usize = 24;
/// Owner-table slot cost (open-addressed, 2x load headroom priced in).
const BSLOT_BYTES: usize = std::mem::size_of::<BSlot>() * 2;

/// The statement's content hash — the ONE partition/probe law for the
/// scatter, the owner table and the (re)probe of spilled records.
#[inline]
fn bkey_hash(v: u64, a: &[u8], b: &[u8]) -> u64 {
    use crate::grouped::hash_bytes;
    hash_bytes(a)
        ^ hash64(v ^ 0x9E37_79B9_7F4A_7C15)
        ^ if b.is_empty() { 0 } else { hash_bytes(b).rotate_left(21) }
}

/// One collected group at the answer boundary.
struct BRow {
    c: u64,
    v: u64,
    /// gid-order accelerator (`pm::reg_ord(a)`), cached at collection.
    ro: u64,
    a: Vec<u8>,
    b: Vec<u8>,
}

/// The per-shape answer order (the tuned arm each shape replaces).
#[derive(Clone, Copy, PartialEq)]
enum BShape {
    /// text128: (count DESC, bytes ASC)
    Text,
    /// int_gid: (count DESC, v ASC, reg_ord ASC, bytes ASC); KeyAsc drops
    /// the count leg (gidord_before law).
    IntGid { key_asc: bool },
    /// int_gid_filtered: (count DESC, reg_ord ASC, bytes ASC, v ASC)
    IntGidFiltered,
    /// gid_pair: (count DESC, bytes0 ASC, bytes1 ASC)
    GidPair,
}

fn bshape_before(sh: BShape, x: &BRow, y: &BRow) -> bool {
    let o = match sh {
        BShape::Text => y.c.cmp(&x.c).then_with(|| x.a.cmp(&y.a)),
        BShape::IntGid { key_asc } => {
            let c = if key_asc { std::cmp::Ordering::Equal } else { y.c.cmp(&x.c) };
            c.then_with(|| x.v.cmp(&y.v))
                .then_with(|| x.ro.cmp(&y.ro))
                .then_with(|| x.a.cmp(&y.a))
        }
        BShape::IntGidFiltered => y
            .c
            .cmp(&x.c)
            .then_with(|| x.ro.cmp(&y.ro))
            .then_with(|| x.a.cmp(&y.a))
            .then_with(|| x.v.cmp(&y.v)),
        BShape::GidPair => {
            y.c.cmp(&x.c).then_with(|| x.a.cmp(&y.a)).then_with(|| x.b.cmp(&y.b))
        }
    };
    o == std::cmp::Ordering::Less
}

/// Per-worker byte-arena spill writer: one private file, chunks indexed
/// `(partition, offset, byte_len)` — the byte-key twin of the SoA arm's
/// SpillW (varlen records make the chunk a byte extent, not a row count).
pub(crate) struct BW {
    pub(crate) m: Box<dyn crate::spill::SpillMedium>,
    pub(crate) chunks: Vec<(u32, u64, u64)>,
    buf: Vec<u8>,
    pending: Option<(u64, u64)>, // (chunk start offset, bytes so far)
}

impl BW {
    pub(crate) fn new(store: &dyn crate::spill::SpillStore, purpose: &'static str, w: usize) -> BW {
        let m = store
            .file(purpose, w)
            .unwrap_or_else(|e| crate::spill::io_fail("create", e));
        BW { m, chunks: Vec::new(), buf: Vec::new(), pending: None }
    }
    pub(crate) fn begin(&mut self) {
        self.buf.clear();
        self.pending = None;
    }
    pub(crate) fn push(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
        if self.buf.len() >= crate::spill::SLAB_BYTES {
            self.flush_slab();
        }
    }
    pub(crate) fn end(&mut self) -> (u64, u64) {
        self.flush_slab();
        self.pending.take().expect("spill chunk must not be empty")
    }
    fn flush_slab(&mut self) {
        if self.buf.is_empty() {
            return;
        }
        let off = self
            .m
            .append(&self.buf)
            .unwrap_or_else(|e| crate::spill::io_fail("append", e));
        match &mut self.pending {
            None => self.pending = Some((off, self.buf.len() as u64)),
            Some((_, n)) => *n += self.buf.len() as u64,
        }
        self.buf.clear();
    }
}

/// Owner-table slot: open-addressed on the content hash, bytes in the
/// owner's arena. `cnt == 0` = empty.
#[derive(Clone, Copy, Default)]
struct BSlot {
    h: u64,
    cnt: u64,
    v: u64,
    off: u64,
    la: u32,
    lb: u32,
}

/// The owner's bounded fold table (E18b share): open-addressed slots +
/// byte arena; `add` folds, the caller drains when `accounted()` would
/// cross the share.
struct BTab {
    slots: Vec<BSlot>,
    mask: usize,
    len: usize,
    arena: Vec<u8>,
}

impl BTab {
    fn new() -> BTab {
        BTab { slots: vec![BSlot::default(); 256], mask: 255, len: 0, arena: Vec::new() }
    }
    fn reset(&mut self) {
        self.slots.iter_mut().for_each(|s| s.cnt = 0);
        self.len = 0;
        self.arena.clear();
    }
    fn accounted(&self) -> usize {
        self.slots.len() * std::mem::size_of::<BSlot>() + self.arena.len()
    }
    /// Fold `(v, a, b) += cnt`; returns the bytes the insert ADDED (0 on
    /// a pure fold — the caller's accounting trigger).
    fn add(&mut self, h: u64, v: u64, a: &[u8], b: &[u8], cnt: u64) -> usize {
        // Grow at 1/2 load (the caller prices growth via accounted()).
        if self.len * 2 >= self.slots.len() {
            self.grow();
        }
        let mut i = (h as usize) & self.mask;
        loop {
            let s = self.slots[i];
            if s.cnt == 0 {
                let off = self.arena.len() as u64;
                self.arena.extend_from_slice(a);
                self.arena.extend_from_slice(b);
                self.slots[i] = BSlot { h, cnt, v, off, la: a.len() as u32, lb: b.len() as u32 };
                self.len += 1;
                return a.len() + b.len();
            }
            if s.h == h && s.v == v && s.la == a.len() as u32 && s.lb == b.len() as u32 {
                let o = s.off as usize;
                let (al, bl) = (s.la as usize, s.lb as usize);
                if &self.arena[o..o + al] == a && &self.arena[o + al..o + al + bl] == b {
                    self.slots[i].cnt += cnt;
                    return 0;
                }
            }
            i = (i + 1) & self.mask;
        }
    }
    #[cold]
    fn grow(&mut self) {
        let ncap = self.slots.len() * 2;
        let mut ns = vec![BSlot::default(); ncap];
        let nmask = ncap - 1;
        for s in &self.slots {
            if s.cnt == 0 {
                continue;
            }
            let mut i = (s.h as usize) & nmask;
            while ns[i].cnt != 0 {
                i = (i + 1) & nmask;
            }
            ns[i] = *s;
        }
        self.slots = ns;
        self.mask = nmask;
    }
    fn key<'a>(&'a self, s: &BSlot) -> (&'a [u8], &'a [u8]) {
        let o = s.off as usize;
        let (al, bl) = (s.la as usize, s.lb as usize);
        (&self.arena[o..o + al], &self.arena[o + al..o + al + bl])
    }
}

/// Canonical run/merge key order over the group key tuple — one total
/// order for drain sorts, the resident stream and the k-way combine
/// (NEVER the answer order; that is `bshape_before` at the selection).
#[inline]
fn bkey_ord(x: &BRec, y: &BRec) -> std::cmp::Ordering {
    x.1.cmp(&y.1).then_with(|| x.2.cmp(&y.2)).then_with(|| x.0.cmp(&y.0))
}

/// One decoded run/stream record: `(v, bytesA, bytesB, cnt)`.
type BRec = (u64, Vec<u8>, Vec<u8>, u64);

/// Decode the next sorted-run record, or `None` at run end.
fn brun_next(cur: &mut crate::spill::ByteCursor) -> Option<BRec> {
    let h: [u8; RREC_HDR] = cur.take(RREC_HDR)?.try_into().unwrap();
    let c = u64::from_ne_bytes(h[..8].try_into().unwrap());
    let v = u64::from_ne_bytes(h[8..16].try_into().unwrap());
    let la = u32::from_ne_bytes(h[16..20].try_into().unwrap()) as usize;
    let lb = u32::from_ne_bytes(h[20..24].try_into().unwrap()) as usize;
    let ab = cur.take(la + lb).expect("run record bytes");
    Some((v, ab[..la].to_vec(), ab[la..].to_vec(), c))
}

/// Drain the owner's table as one key-sorted BYTE-KEY run and re-arm it
/// (spill-design.md §3.2, byte-key form): rows sort by the canonical
/// `bkey_ord`, values ride untouched — the merge combine is the count
/// fold's own `+`, so run boundaries cannot change any answer byte.
#[cold]
fn btab_drain(
    tab: &mut BTab,
    rw: &mut Option<BW>,
    runs: &mut Vec<(u64, u64, u64)>,
    store: &dyn crate::spill::SpillStore,
    w: usize,
) {
    BSPILL_DRAINS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut rows: Vec<BRec> = Vec::with_capacity(tab.len);
    for s in &tab.slots {
        if s.cnt != 0 {
            let (a, b) = tab.key(s);
            rows.push((s.v, a.to_vec(), b.to_vec(), s.cnt));
        }
    }
    rows.sort_unstable_by(bkey_ord);
    let bw = rw.get_or_insert_with(|| BW::new(store, "bruns", w));
    bw.begin();
    let n = rows.len() as u64;
    for (v, a, b, c) in rows {
        let mut hdr = [0u8; RREC_HDR];
        hdr[..8].copy_from_slice(&c.to_ne_bytes());
        hdr[8..16].copy_from_slice(&v.to_ne_bytes());
        hdr[16..20].copy_from_slice(&(a.len() as u32).to_ne_bytes());
        hdr[20..24].copy_from_slice(&(b.len() as u32).to_ne_bytes());
        bw.push(&hdr);
        bw.push(&a);
        bw.push(&b);
    }
    let (off, len) = bw.end();
    runs.push((off, len, n));
    tab.reset();
}

/// Fold one record into the bounded owner table (E18b): if the insert
/// could cross the owner share (slot-growth headroom + the new bytes),
/// the table drains as a run FIRST. The 128-live-entry floor is the
/// drain grain (a degenerate share still folds instead of thrashing).
#[allow(clippy::too_many_arguments)]
fn btab_fold(
    tab: &mut BTab,
    rw: &mut Option<BW>,
    runs: &mut Vec<(u64, u64, u64)>,
    store: &dyn crate::spill::SpillStore,
    w: usize,
    share: usize,
    cnt: u64,
    v: u64,
    a: &[u8],
    b: &[u8],
) {
    if tab.len >= 128 && tab.accounted() + BSLOT_BYTES + a.len() + b.len() > share {
        btab_drain(tab, rw, runs, store, w);
    }
    tab.add(bkey_hash(v, a, b), v, a, b, cnt);
}

/// Per-owner answer-boundary sink: collector when `kw == MAX` (the
/// answer law prices it), sorted-insert top-k otherwise (O(k) plane,
/// the E17b posture — bounded answers never price staging).
fn bsel_push(out: &mut Vec<BRow>, kw: usize, shape: BShape, row: BRow) {
    if kw == usize::MAX {
        out.push(row);
        return;
    }
    if out.len() == kw {
        let last = out.last().expect("kw > 0 by the topk law");
        if !bshape_before(shape, &row, last) {
            return;
        }
        out.pop();
    }
    let pos = out.partition_point(|e| !bshape_before(shape, &row, e));
    out.insert(pos, row);
}

pub fn byte_spill(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    let (bank, pool) = (ctx.bank, ctx.pool);
    assert!(
        !node.agg.is_empty() && node.agg.iter().all(|a| a.op == AggOp::CountStar),
        "byte_spill: CountStar-only vocabulary (spill_class gap)"
    );
    let g = &node.params.group_cols;
    let widths: Vec<u8> = g.iter().map(|&c| col_width(bank, c)).collect();
    let drop_flag = node.params.flags & F_DROP_EMPTY_KEY != 0;
    // Layout election (mirrors the tuned dispatch exactly).
    let (shape, a_int, t0, t1): (BShape, Option<u32>, u32, Option<u32>) =
        match widths.as_slice() {
            [0] => (BShape::Text, None, g[0], None),
            [w, 0] if *w > 0 && drop_flag => (BShape::IntGidFiltered, Some(g[0]), g[1], None),
            [w, 0] if *w > 0 => (
                BShape::IntGid { key_asc: node.params.order == OrderBy::KeyAsc },
                Some(g[0]),
                g[1],
                None,
            ),
            [0, 0] => (BShape::GidPair, None, g[0], Some(g[1])),
            other => panic!("byte_spill: unroutable key layout {other:?}"),
        };
    // Empty-key drops, per the tuned arms' laws: the filtered [w,0] arm
    // drops empty text keys; gid_pair drops per ne_empty_cols membership.
    let drop_a = matches!(shape, BShape::IntGidFiltered)
        || (matches!(shape, BShape::GidPair) && node.params.ne_empty_cols.contains(&t0));
    let drop_b = t1.map(|c| node.params.ne_empty_cols.contains(&c)).unwrap_or(false);
    // int lane mask: the filtered arm folds/emits the int key at its
    // STORED width (the tuned lo_mask law); the plain int_gid arm rides
    // the decoded word untouched.
    let wi = a_int.map(|c| col_width(bank, c)).unwrap_or(0);
    let lo_mask = if wi >= 8 || wi == 0 { u64::MAX } else { (1u64 << (8 * wi as u32)) - 1 };
    let mask_v = matches!(shape, BShape::IntGidFiltered);

    let pf0 = pm::dict_faces(ctx, t0);
    let pf1 = t1.map(|c| pm::dict_faces(ctx, c));
    let units = ctx.faces.walk(bank, t0);
    let ndv = ndv_face(ctx, t0)
        + a_int.map(|c| ndv_face(ctx, c)).unwrap_or(0)
        + t1.map(|c| ndv_face(ctx, c)).unwrap_or(0);
    let p = partition_count(ndv.max(1), 32, node.params.l2_bytes, pool.threads());
    let shift = 64 - p.trailing_zeros();
    let t = pool.threads();
    let budget = ctx.faces.cfg.grouped_budget_bytes();
    let share = ((budget / t.max(1) as u64).max(1)) as usize;
    // The statement's spill namespace: engagement implies a registered
    // factory; a factory that cannot mint a store RIGHT NOW is the typed
    // no-substrate refusal through the runtime seam (fail-closed —
    // never a stumble mid-I/O).
    let store: std::sync::Arc<dyn crate::spill::SpillStore> = crate::spill::new_store()
        .unwrap_or_else(|| {
            crate::refuse::raise_runtime(crate::refuse::Refuse::GroupedSpillUnavailable {
                what: "no-substrate",
                est: bank
                    .rows_total()
                    .saturating_mul(crate::stencils::hash_plane::SCATTER_ROW_BYTES),
                budget,
            })
        });

    struct S {
        ci: Option<ColState>,
        kc0: CurCache,
        ks0: Scratch,
        codes0: Vec<u32>,
        kc1: Option<CurCache>,
        ks1: Scratch,
        codes1: Vec<u32>,
        /// Per-partition scatter byte arenas (varlen records).
        buckets: Vec<Vec<u8>>,
        resident: usize,
        sp: Option<BW>,
        w: usize,
    }
    struct SK {
        buckets: Vec<Vec<u8>>,
        sp: Option<(Box<dyn crate::spill::SpillMedium>, Vec<(u32, u64, u64)>)>,
    }
    let (pf0r, pf1r) = (&pf0, &pf1);
    let storer = &store;
    let t_p1 = std::time::Instant::now();
    let pass1 = pool.run_finish(
        units.len(),
        |w| S {
            ci: a_int.map(ColState::fetch),
            kc0: CurCache::new(t0),
            ks0: crate::scan::scratch_fetch(),
            codes0: vec![0; 8192],
            kc1: t1.map(CurCache::new),
            ks1: crate::scan::scratch_fetch(),
            codes1: vec![0; 8192],
            buckets: (0..p).map(|_| Vec::new()).collect(),
            resident: 0,
            sp: None,
            w,
        },
        |s: &mut S, i| {
            let (pi, gr, rows, _) = units[i];
            let rows = rows as usize;
            let dv: Option<&[u64]> = s.ci.as_mut().map(|c| {
                let d = c.dec(bank, pi, gr, rows);
                unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) }
            });
            // Text lanes: dict parts stay in CODE currency through the
            // granule (run collapse compares codes); raw parts decode
            // payload slices. Bytes resolve once per collapsed run.
            let dict0 = pf0r[pi].dh.is_some();
            let raw0: &[u64] = if dict0 {
                if s.codes0.len() < rows {
                    s.codes0.resize(rows, 0);
                }
                s.kc0.get(bank, pi).decode_codes(gr, &mut s.codes0[..rows]).expect("codes");
                &[]
            } else {
                let d = s.ks0.decode_full(s.kc0.get(bank, pi), gr, rows);
                unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) }
            };
            let dict1 = t1.is_some() && pf1r.as_ref().expect("pair faces")[pi].dh.is_some();
            let raw1: &[u64] = if let Some(kc1) = s.kc1.as_mut() {
                if dict1 {
                    if s.codes1.len() < rows {
                        s.codes1.resize(rows, 0);
                    }
                    kc1.get(bank, pi).decode_codes(gr, &mut s.codes1[..rows]).expect("codes");
                    &[]
                } else {
                    let d = s.ks1.decode_full(kc1.get(bank, pi), gr, rows);
                    unsafe { std::slice::from_raw_parts(d.as_ptr(), d.len()) }
                }
            } else {
                &[]
            };
            let mut r = 0usize;
            while r < rows {
                let v = dv.map(|d| if mask_v { d[r] & lo_mask } else { d[r] }).unwrap_or(0);
                let c0 = if dict0 { s.codes0[r] } else { 0 };
                let c1 = if dict1 { s.codes1[r] } else { 0 };
                let p0: &[u8] = if dict0 { &[] } else { unsafe { varlena_payload(raw0[r]) } };
                let p1: &[u8] = if t1.is_some() && !dict1 {
                    unsafe { varlena_payload(raw1[r]) }
                } else {
                    &[]
                };
                let mut j = r + 1;
                while j < rows {
                    let vj = dv.map(|d| if mask_v { d[j] & lo_mask } else { d[j] }).unwrap_or(0);
                    if vj != v {
                        break;
                    }
                    let same0 = if dict0 {
                        s.codes0[j] == c0
                    } else {
                        (unsafe { varlena_payload(raw0[j]) }) == p0
                    };
                    if !same0 {
                        break;
                    }
                    let same1 = if t1.is_none() {
                        true
                    } else if dict1 {
                        s.codes1[j] == c1
                    } else {
                        (unsafe { varlena_payload(raw1[j]) }) == p1
                    };
                    if !same1 {
                        break;
                    }
                    j += 1;
                }
                let cnt = (j - r) as u32;
                r = j;
                let ba: &[u8] = if dict0 { pm::face_bytes(pf0r, pi, c0) } else { p0 };
                let bb: &[u8] = if t1.is_none() {
                    &[]
                } else if dict1 {
                    pm::face_bytes(pf1r.as_ref().expect("pair faces"), pi, c1)
                } else {
                    p1
                };
                if (drop_a && ba.is_empty()) || (drop_b && bb.is_empty()) {
                    continue;
                }
                let h = bkey_hash(v, ba, bb);
                let bkt = &mut s.buckets[(h >> shift) as usize];
                let mut hdr = [0u8; SREC_HDR];
                hdr[..4].copy_from_slice(&cnt.to_ne_bytes());
                hdr[4..12].copy_from_slice(&v.to_ne_bytes());
                hdr[12..16].copy_from_slice(&(ba.len() as u32).to_ne_bytes());
                hdr[16..20].copy_from_slice(&(bb.len() as u32).to_ne_bytes());
                bkt.extend_from_slice(&hdr);
                bkt.extend_from_slice(ba);
                bkt.extend_from_slice(bb);
                s.resident += SREC_HDR + ba.len() + bb.len();
            }
            // [E18b] flush at the worker share: every bucket becomes one
            // chunk on the worker's private spill file (capacity kept —
            // the flush is what bounds resident bytes). Granule grain.
            if s.resident > share {
                BSPILL_FLUSHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let w = s.w;
                let sp = s.sp.get_or_insert_with(|| BW::new(&**storer, "bscatter", w));
                for b in 0..p {
                    if s.buckets[b].is_empty() {
                        continue;
                    }
                    sp.begin();
                    sp.push(&s.buckets[b]);
                    let (off, len) = sp.end();
                    sp.chunks.push((b as u32, off, len));
                    s.buckets[b].clear();
                }
                s.resident = 0;
            }
        },
        |s| {
            crate::scan::scratch_park(s.ks0);
            crate::scan::scratch_park(s.ks1);
            if let Some(ci) = s.ci {
                ci.park();
            }
            SK { buckets: s.buckets, sp: s.sp.map(|w| (w.m, w.chunks)) }
        },
    );
    crate::engine::phn(node, "pass1", t_p1);

    // ---- pass 2: one owner per partition, bounded table + byte-key runs.
    let t_p2 = std::time::Instant::now();
    let scattered: Vec<&SK> = pass1.iter().collect();
    let kw = topk(node);
    let chunk_slab = crate::spill::SLAB_BYTES.min(share.max(SREC_HDR));
    // Answer-plane exact account (unbounded answers only — the E17 law):
    // staging (BRow + key bytes) + render lanes (key bytes at the
    // BytesBuild plane + i64 words for the int/count lanes), counted per
    // collected group, never estimated.
    let ans_bytes = std::sync::atomic::AtomicU64::new(0);
    let render_word_lanes = (a_int.is_some() as u64) + node.agg.len() as u64;
    let store2 = &store;
    let ansb = &ans_bytes;
    let owned = pool.run(
        p,
        |w| {
            (
                Vec::<BRow>::new(),
                BTab::new(),
                w,
                None::<BW>,
                Vec::<(u64, u64, u64)>::new(),
            )
        },
        |st: &mut (Vec<BRow>, BTab, usize, Option<BW>, Vec<(u64, u64, u64)>), part| {
            let (out, tab, w, rw, runs) = (&mut st.0, &mut st.1, st.2, &mut st.3, &mut st.4);
            tab.reset();
            runs.clear();
            // Spilled chunks first (sequential reads), then the resident
            // bucket residue — arrival order is erased by the byte-key
            // fold + the answer sort either way.
            for sk in &scattered {
                if let Some((m, chunks)) = &sk.sp {
                    for &(cb, off, len) in chunks {
                        if cb as usize != part {
                            continue;
                        }
                        let mut cur = crate::spill::ByteCursor::new(&**m, off, len, chunk_slab);
                        loop {
                            let Some(hd) = cur.take(SREC_HDR) else { break };
                            let hd: [u8; SREC_HDR] = hd.try_into().unwrap();
                            let cnt = u32::from_ne_bytes(hd[..4].try_into().unwrap()) as u64;
                            let v = u64::from_ne_bytes(hd[4..12].try_into().unwrap());
                            let la = u32::from_ne_bytes(hd[12..16].try_into().unwrap()) as usize;
                            let lb = u32::from_ne_bytes(hd[16..20].try_into().unwrap()) as usize;
                            let ab = cur.take(la + lb).expect("scatter record bytes").to_vec();
                            btab_fold(
                                tab, rw, runs, &**store2, w, share, cnt, v, &ab[..la], &ab[la..],
                            );
                        }
                    }
                }
            }
            for sk in &scattered {
                let arena = &sk.buckets[part];
                let mut o = 0usize;
                while o < arena.len() {
                    let cnt = u32::from_ne_bytes(arena[o..o + 4].try_into().unwrap()) as u64;
                    let v = u64::from_ne_bytes(arena[o + 4..o + 12].try_into().unwrap());
                    let la =
                        u32::from_ne_bytes(arena[o + 12..o + 16].try_into().unwrap()) as usize;
                    let lb =
                        u32::from_ne_bytes(arena[o + 16..o + 20].try_into().unwrap()) as usize;
                    let sb = o + SREC_HDR;
                    btab_fold(
                        tab,
                        rw,
                        runs,
                        &**store2,
                        w,
                        share,
                        cnt,
                        v,
                        &arena[sb..sb + la],
                        &arena[sb + la..sb + la + lb],
                    );
                    o = sb + la + lb;
                }
            }
            let account = |row: &BRow| {
                (std::mem::size_of::<BRow>() + row.a.len() + row.b.len()) as u64
                    + (row.a.len() + row.b.len()) as u64
                    + 8 * render_word_lanes
            };
            if runs.is_empty() {
                for s in &tab.slots {
                    if s.cnt != 0 {
                        let (a, b) = tab.key(s);
                        let ro = pm::reg_ord(a);
                        let row = BRow { c: s.cnt, v: s.v, ro, a: a.to_vec(), b: b.to_vec() };
                        if kw == usize::MAX {
                            ansb.fetch_add(account(&row), std::sync::atomic::Ordering::Relaxed);
                        }
                        bsel_push(out, kw, shape, row);
                    }
                }
                return;
            }
            BSPILL_MERGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // Resident stream: extract + sort by the canonical key order.
            let mut mem: Vec<BRec> = Vec::with_capacity(tab.len);
            for s in &tab.slots {
                if s.cnt != 0 {
                    let (a, b) = tab.key(s);
                    mem.push((s.v, a.to_vec(), b.to_vec(), s.cnt));
                }
            }
            mem.sort_unstable_by(|x, y| {
                x.1.cmp(&y.1).then_with(|| x.2.cmp(&y.2)).then_with(|| x.0.cmp(&y.0))
            });
            let m = &*rw.as_ref().expect("runs imply a run file").m;
            let nrun = runs.len();
            let slab = (share / (nrun + 1)).clamp(RREC_HDR, crate::spill::SLAB_BYTES);
            let mut curs: Vec<crate::spill::ByteCursor> = runs
                .iter()
                .map(|&(off, len, _)| crate::spill::ByteCursor::new(m, off, len, slab))
                .collect();
            // Stream heads: runs 0..nrun, the resident stream at nrun.
            let mut mi = 0usize;
            let mut heads: Vec<Option<BRec>> = Vec::with_capacity(nrun + 1);
            for c in curs.iter_mut() {
                heads.push(brun_next(c));
            }
            heads.push((mi < mem.len()).then(|| {
                let r = std::mem::take(&mut mem[mi]);
                mi += 1;
                r
            }));
            // K-way combine by linear min-scan (streams are few — runs +
            // 1; the byte keys make heap keys allocation-heavy for no
            // asymptotic win at this fan-in).
            loop {
                let mut min: Option<usize> = None;
                for i in 0..heads.len() {
                    if heads[i].is_none() {
                        continue;
                    }
                    match min {
                        None => min = Some(i),
                        Some(mj) => {
                            let (hi, hm) = (
                                heads[i].as_ref().expect("checked"),
                                heads[mj].as_ref().expect("min head"),
                            );
                            let o = hi
                                .1
                                .cmp(&hm.1)
                                .then_with(|| hi.2.cmp(&hm.2))
                                .then_with(|| hi.0.cmp(&hm.0));
                            if o == std::cmp::Ordering::Less {
                                min = Some(i);
                            }
                        }
                    }
                }
                let Some(mj) = min else { break };
                let (v, a, b, mut c) = heads[mj].take().expect("min head");
                let mut adv = |heads: &mut Vec<Option<BRec>>, s: usize| {
                    heads[s] = if s < nrun {
                        brun_next(&mut curs[s])
                    } else if mi < mem.len() {
                        let r = std::mem::take(&mut mem[mi]);
                        mi += 1;
                        Some(r)
                    } else {
                        None
                    };
                };
                adv(&mut heads, mj);
                // Combine every equal-key head (byte equality IS the
                // arm's identity — a group cannot split or alias).
                loop {
                    let mut hit: Option<usize> = None;
                    for (i, h) in heads.iter().enumerate() {
                        if let Some(hr) = h {
                            if hr.0 == v && hr.1 == a && hr.2 == b {
                                hit = Some(i);
                                break;
                            }
                        }
                    }
                    let Some(h2) = hit else { break };
                    c += heads[h2].as_ref().expect("hit head").3;
                    adv(&mut heads, h2);
                }
                let ro = pm::reg_ord(&a);
                let row = BRow { c, v, ro, a, b };
                if kw == usize::MAX {
                    ansb.fetch_add(account(&row), std::sync::atomic::Ordering::Relaxed);
                }
                bsel_push(out, kw, shape, row);
            }
        },
    );
    // The exact answer-bytes law (spill-design.md §3.4, this arm's
    // counted account) — refuse typed BEFORE flattening the answer plane.
    if kw == usize::MAX {
        let got = ans_bytes.load(std::sync::atomic::Ordering::Relaxed);
        let ab = ctx.faces.cfg.answer_budget_bytes();
        if got > ab {
            crate::refuse::raise_runtime(crate::refuse::Refuse::GroupAnswerOverBudget {
                got,
                budget: ab,
            });
        }
    }
    let mut all: Vec<BRow> = Vec::new();
    for st in owned {
        all.extend(st.0);
    }
    drop(scattered);
    pool.drop_par(pass1);
    all.sort_unstable_by(|x, y| {
        if bshape_before(shape, x, y) {
            std::cmp::Ordering::Less
        } else if bshape_before(shape, y, x) {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    });
    if kw != usize::MAX {
        all.truncate(kw);
    }
    crate::engine::phn(node, "pass2", t_p2);
    // Render: the tuned arm's exact emit per shape.
    match shape {
        BShape::GidPair => {
            let mut kb0o = BytesBuild::new();
            let mut kb1o = BytesBuild::new();
            let mut cnts: Vec<i64> = Vec::new();
            for r in all.iter().skip(node.params.offset) {
                kb0o.push(&r.a);
                kb1o.push(&r.b);
                cnts.push(r.c as i64);
            }
            AnswerSet::from_cols(vec![
                kb0o.finish(node.ty_of(t0)),
                kb1o.finish(node.ty_of(t1.expect("gid pair"))),
                AnswerCol::i64s(TypMeta::INT8, cnts),
            ])
        }
        _ => {
            let mut sink = LineSink::new(bank, node);
            for r in all.iter().skip(node.params.offset) {
                sink.push(node, &|_| sx(r.v, wi), Some(&r.a), r.c, &|_| 0, 0, 0);
            }
            sink.finish()
        }
    }
}

// ---------------------------------------------------------------------------
// [q3334 textgroup] fp128-first spilled combine for the k-bounded single
// text key band ([0] widths, CountStar) — the ClickBench q33/q34 class.
//
// WHY: `bytes_spill_engaged` fires whenever `rows × SCATTER_ROW_BYTES`
// crosses the E18 width-scaled budget (64 MiB × pool width). On a
// 16-wide box a 100m-row bank prices at 2.8 GB against a 1 GiB budget,
// so `SELECT URL, COUNT(*) … GROUP BY URL ORDER BY c DESC LIMIT 10`
// leaves the tuned text128 arm and rides `byte_spill` — which scatters
// the FULL key payload per collapsed run (20 B header + the key bytes;
// URL-class keys average 60-80 B, ~3.6× the 28 B the planner priced) and
// refolds it with a per-record heap alloc + byte-key memcmp fold. The
// wave-3 c6a.4xlarge submission cell measured that route at ~25 s warm
// where the 64-wide tax rig (4 GiB budget → tuned text128) answers the
// same bank in ~70 ms.
//
// THE ARM: text128's own fp128-first two-pass (the partmerge fp-identity
// convention — part_merge.rs: collision odds ~1e-20 at 100m NDV, the
// standing text128 identity), with the ONE change that pass-1 scatter
// buckets flush to spill chunks at the E18b worker share instead of
// resting whole. Records stay the priced FIXED 28 B
// (h1 u64 | h2 u64 | cnt u32 | part u32 | code u32) — dict parts fold to
// entry grain first, raw parts carry a (granule, row) locator — and key
// BYTES resolve lazily at the top-k admission boundary only (dict entry
// or a decode_sel of the surviving row), exactly the tuned arm's law.
//
// ELECTION FLOORS (documented arithmetic; every miss falls back to
// byte_spill — behavior unchanged):
//   - k-bounded: `topk(node) != usize::MAX` (a native pushed bound
//     ≤ NATIVE_TOPK_MAX), further floored so the per-partition sorted-
//     insert selection stays L2-resident: the insert memmove moves
//     ~40 B/entry ((Vec<u8>, u64) pairs), so `kw × 40 ≤ l2_bytes`
//     (26,214 at the 1 MiB default; q33/q34 run at kw = 10).
//   - pass-2 owner residency: one partition's records stream back and
//     fold into an open-addressed table. Worst case (all-raw parts, no
//     adjacency) is 1 record/row: `ceil(rows/p)` records at
//     28 B (stream) + 2 × 32 B (table slots at ≤ 1/2 load) = 92 B each;
//     with a 2× partition-skew allowance the price is
//     `ceil(rows/p) × 184 ≤ share` (the E18 worker allowance,
//     budget/width). At 100m rows, p = 2048, share = 64 MiB: 48.9k ×
//     184 = 9.0 MB — in by 7×.
//
// Kill switch: PGRUST_SQE_TEXT128_SPILL=0 restores the byte_spill route
// (read per statement — no process-cached arm, the A/B twin law).
// ---------------------------------------------------------------------------

/// [q3334 textgroup] engagement census: statements served by the arm /
/// pass-1 scatter flushes (the tests' no-vacuous-green gate).
pub static TSPILL_ENGAGED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static TSPILL_FLUSHES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The election law above, as a function over the standing faces.
pub(crate) fn text128_spill_elects(ctx: &SqeCtx, node: &PlanNode) -> bool {
    if std::env::var("PGRUST_SQE_TEXT128_SPILL").is_ok_and(|v| v == "0") {
        return false;
    }
    let bank = ctx.bank;
    let g = &node.params.group_cols;
    if g.len() != 1 || col_width(bank, g[0]) != 0 {
        return false;
    }
    if !node.agg.iter().all(|a| a.op == AggOp::CountStar) {
        return false;
    }
    let kw = topk(node);
    if kw == usize::MAX || kw.saturating_mul(40) > node.params.l2_bytes {
        return false;
    }
    let attno = g[0];
    let entries = ndv_face(ctx, attno);
    let p = partition_count(entries, 32, node.params.l2_bytes, ctx.pool.threads());
    let share = ctx.faces.cfg.grouped_budget_bytes() / ctx.pool.threads().max(1) as u64;
    bank.rows_total().div_ceil(p as u64).saturating_mul(184) <= share
}

/// Fold one partition's 28 B fp128 records and run the tuned admission:
/// (count DESC, bytes ASC) top-k with LAZY byte resolve — `resolve`
/// fires only for candidates that pass the count gate (the text128
/// admission law verbatim). Factored for the pm_tests combine-law rig.
fn t128s_partition_top(
    recs: &[(u64, u64, u32, u32, u32)],
    k: usize,
    resolve: &mut dyn FnMut(u32, u32) -> Vec<u8>,
) -> Vec<(Vec<u8>, u64)> {
    #[derive(Clone, Copy, Default)]
    struct FS {
        h1: u64,
        h2: u64,
        cnt: u32,
        pl: u32,
        code: u32,
    }
    let cap = (recs.len() * 2).next_power_of_two().max(64);
    let mask = cap - 1;
    let mut slots = vec![FS::default(); cap];
    for &(h1, h2, c, pl, code) in recs {
        let mut i = (h1 as usize) & mask;
        loop {
            let e = &mut slots[i];
            if e.cnt == 0 {
                *e = FS { h1, h2, cnt: c, pl, code };
                break;
            }
            if e.h1 == h1 && e.h2 == h2 {
                e.cnt += c;
                break;
            }
            i = (i + 1) & mask;
        }
    }
    // partition top-k (cnt desc, bytes asc); lazy byte resolve.
    let mut top: Vec<(Vec<u8>, u64)> = Vec::new();
    for fs in slots.iter() {
        let cnt = fs.cnt as u64;
        if cnt == 0 {
            continue;
        }
        if top.len() == k && cnt < top.last().expect("full selection").1 {
            continue;
        }
        let bytes = resolve(fs.pl, fs.code);
        if top.len() == k {
            let w = top.last().expect("full selection");
            if cnt < w.1 || (cnt == w.1 && bytes >= w.0) {
                continue;
            }
            top.pop();
        }
        let pos = top
            .binary_search_by(|pr| cnt.cmp(&pr.1).then_with(|| pr.0.as_slice().cmp(&bytes)))
            .unwrap_or_else(|q| q);
        top.insert(pos, (bytes, cnt));
    }
    top
}

pub fn text128_spill(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    use crate::fp::entry_fp128;
    let (bank, pool) = (ctx.bank, ctx.pool);
    assert!(node.agg.iter().all(|a| a.op == AggOp::CountStar));
    let attno = node.params.group_cols[0];
    let k = topk(node);
    assert!(k != usize::MAX, "text128_spill: election is k-bounded");
    let entries = ndv_face(ctx, attno);
    let p = partition_count(entries, 32, node.params.l2_bytes, pool.threads());
    let pbits = p.trailing_zeros();
    let t = pool.threads();
    let budget = ctx.faces.cfg.grouped_budget_bytes();
    let share = ((budget / t.max(1) as u64).max(1)) as usize;
    TSPILL_ENGAGED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    // Engagement implies a registered factory (fail-closed, the byte_spill law).
    let store: std::sync::Arc<dyn crate::spill::SpillStore> = crate::spill::new_store()
        .unwrap_or_else(|| {
            crate::refuse::raise_runtime(crate::refuse::Refuse::GroupedSpillUnavailable {
                what: "no-substrate",
                est: bank
                    .rows_total()
                    .saturating_mul(crate::stencils::hash_plane::SCATTER_ROW_BYTES),
                budget,
            })
        });
    let pf = pm::dict_faces(ctx, attno);
    let t_fpb = std::time::Instant::now();
    let fps = pm::build_fps_cached(ctx, &pf, attno);
    crate::engine::phn(node, "fp_build", t_fpb);
    let fpr: &[Vec<u128>] = &fps;
    const ROW_BIT: u32 = 0x8000_0000;
    const REC: usize = 28;
    #[inline]
    fn push_rec(buf: &mut Vec<u8>, h1: u64, h2: u64, cnt: u32, pl: u32, code: u32) {
        buf.extend_from_slice(&h1.to_ne_bytes());
        buf.extend_from_slice(&h2.to_ne_bytes());
        buf.extend_from_slice(&cnt.to_ne_bytes());
        buf.extend_from_slice(&pl.to_ne_bytes());
        buf.extend_from_slice(&code.to_ne_bytes());
    }
    struct S {
        su: Scratch,
        codes: Vec<u32>,
        counts: Vec<u32>,
        buckets: Vec<Vec<u8>>,
        resident: usize,
        sp: Option<BW>,
        w: usize,
        nulls: u64,
    }
    struct SK {
        buckets: Vec<Vec<u8>>,
        sp: Option<(Box<dyn crate::spill::SpillMedium>, Vec<(u32, u64, u64)>)>,
        nulls: u64,
    }
    let (pfr, storer) = (&pf, &store);
    let t_p1 = std::time::Instant::now();
    let pass1 = pool.run_finish(
        bank.parts.len(),
        |w| S {
            su: crate::scan::scratch_fetch(),
            codes: vec![0; 8192],
            counts: Vec::new(),
            buckets: (0..p).map(|_| Vec::new()).collect(),
            resident: 0,
            sp: None,
            w,
            nulls: 0,
        },
        |s, pi| {
            // [E18b] flush at the worker share: every bucket becomes one
            // chunk on the worker's private spill file (28 B fp128
            // records — the priced form, never key payloads).
            let flush_if_over = |s: &mut S| {
                if s.resident <= share {
                    return;
                }
                TSPILL_FLUSHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let w = s.w;
                let sp = s.sp.get_or_insert_with(|| BW::new(&**storer, "t128scatter", w));
                for b in 0..p {
                    if s.buckets[b].is_empty() {
                        continue;
                    }
                    sp.begin();
                    sp.push(&s.buckets[b]);
                    let (off, len) = sp.end();
                    sp.chunks.push((b as u32, off, len));
                    s.buckets[b].clear();
                }
                s.resident = 0;
            };
            let df = &pfr[pi];
            if df.dh.is_some() {
                let mut cur = crate::scan::open_cursor(bank, pi, attno);
                let n = df.ncodes as usize;
                s.counts.clear();
                s.counts.resize(n, 0);
                for g in 0..cur.granule_count() {
                    let rows = cur.rows_in_granule(g) as usize;
                    if s.codes.len() < rows {
                        s.codes.resize(rows, 0);
                    }
                    cur.decode_codes(g, &mut s.codes[..rows]).expect("codes");
                    // 3VL key law (the text128 body verbatim): NULL rows
                    // fold into the one NULL group.
                    if s.su.validity(&mut cur, g, rows).all_valid() {
                        for r in 0..rows {
                            s.counts[s.codes[r] as usize] += 1;
                        }
                    } else {
                        for r in 0..rows {
                            if s.su.row_valid(r) {
                                s.counts[s.codes[r] as usize] += 1;
                            } else {
                                s.nulls += 1;
                            }
                        }
                    }
                }
                let mut census_n = 0u64;
                let fpp = &fpr[pi];
                for c in 0..n {
                    let cnt = s.counts[c];
                    if cnt != 0 {
                        census_n += 1;
                        let h = fpp[c];
                        let (h1, h2) = ((h >> 64) as u64, h as u64);
                        let b = (h1 >> (64 - pbits)) as usize;
                        push_rec(&mut s.buckets[b], h1, h2, cnt, pi as u32, c as u32);
                        s.resident += REC;
                    }
                }
                crate::engine::census_entries(census_n);
                flush_if_over(s);
            } else {
                let mut cu = crate::scan::open_cursor(bank, pi, attno);
                for g in 0..cu.granule_count() {
                    let rows = cu.rows_in_granule(g) as usize;
                    let all_valid = s.su.validity(&mut cu, g, rows).all_valid();
                    s.su.decode_full(&mut cu, g, rows);
                    for r in 0..rows {
                        if !all_valid && !s.su.row_valid(r) {
                            s.nulls += 1;
                            continue;
                        }
                        let x = s.su.datums[r];
                        let pl = unsafe { crate::scan::varlena_payload(x) };
                        let h = entry_fp128(pl);
                        let (h1, h2) = ((h >> 64) as u64, h as u64);
                        let b = (h1 >> (64 - pbits)) as usize;
                        push_rec(
                            &mut s.buckets[b],
                            h1,
                            h2,
                            1,
                            ROW_BIT | pi as u32,
                            (g << 16) | r as u32,
                        );
                        s.resident += REC;
                    }
                    flush_if_over(s);
                }
            }
        },
        |s| {
            crate::scan::scratch_park(s.su);
            SK { buckets: s.buckets, sp: s.sp.map(|w| (w.m, w.chunks)), nulls: s.nulls }
        },
    );
    crate::engine::phn(node, "pass1", t_p1);
    let t_p2 = std::time::Instant::now();
    let null_total: u64 = pass1.iter().map(|s| s.nulls).sum();
    let scattered: Vec<&SK> = pass1.iter().collect();
    let chunk_slab = crate::spill::SLAB_BYTES.min(share.max(REC));
    type P2State = (Vec<(Vec<u8>, u64)>, Scratch, CurCache, Vec<(u64, u64, u32, u32, u32)>);
    let owned = pool.run_finish(
        p,
        |_| -> P2State {
            (Vec::new(), crate::scan::scratch_fetch(), CurCache::new(attno), Vec::new())
        },
        |(out, rs, rc, recs): &mut P2State, part| {
            recs.clear();
            let take_rec = |b: &[u8]| -> (u64, u64, u32, u32, u32) {
                (
                    u64::from_ne_bytes(b[..8].try_into().expect("rec h1")),
                    u64::from_ne_bytes(b[8..16].try_into().expect("rec h2")),
                    u32::from_ne_bytes(b[16..20].try_into().expect("rec cnt")),
                    u32::from_ne_bytes(b[20..24].try_into().expect("rec part")),
                    u32::from_ne_bytes(b[24..28].try_into().expect("rec code")),
                )
            };
            // Spilled chunks first (sequential reads), then the resident
            // residue — arrival order is erased by the fp128 fold.
            for sk in &scattered {
                if let Some((m, chunks)) = &sk.sp {
                    for &(cb, off, len) in chunks {
                        if cb as usize != part {
                            continue;
                        }
                        let mut cur = crate::spill::ByteCursor::new(&**m, off, len, chunk_slab);
                        while let Some(b) = cur.take(REC) {
                            recs.push(take_rec(b));
                        }
                    }
                }
            }
            for sk in &scattered {
                let arena = &sk.buckets[part];
                for b in arena.chunks_exact(REC) {
                    recs.push(take_rec(b));
                }
            }
            let mut resolve = |pl: u32, code: u32| -> Vec<u8> {
                if pl & ROW_BIT != 0 {
                    let (g, r) = (code >> 16, (code & 0xFFFF) as u16);
                    let d = rs.decode_sel(rc.get(bank, (pl & !ROW_BIT) as usize), g, &[r]);
                    unsafe { crate::scan::varlena_payload(d[0]) }.to_vec()
                } else {
                    pm::face_bytes(pfr, pl as usize, code).to_vec()
                }
            };
            out.extend(t128s_partition_top(recs, k, &mut resolve));
        },
        |s| {
            crate::scan::scratch_park(s.1);
            s.0
        },
    );
    let mut all: Vec<(Vec<u8>, u64)> = Vec::new();
    for out in owned {
        all.extend(out);
    }
    drop(scattered);
    pool.drop_par(pass1);
    crate::engine::phn(node, "pass2", t_p2);
    pm::park_fps(fps);
    all.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    all.truncate(k);
    // The NULL group joins at the global cut (the text128 body verbatim).
    let mut rows: Vec<(Option<&[u8]>, u64)> =
        all.iter().map(|(b, c)| (Some(b.as_slice()), *c)).collect();
    if null_total > 0 {
        let pos = rows.partition_point(|r| r.1 >= null_total);
        rows.insert(pos, (None, null_total));
        rows.truncate(k);
    }
    let mut sink = LineSink::new(bank, node);
    for &(bytes, c) in rows.iter().skip(node.params.offset) {
        match bytes {
            Some(b) => sink.push(node, &|_| 0, Some(b), c, &|_| 0, 0, 0),
            None => sink.push_null_key(node, c),
        }
    }
    sink.finish()
}

// ---------------------------------------------------------------------------
// [q3334 textgroup] combine-law tests (in-file mod — the pm_tests idiom)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod pm_tests {
    use super::t128s_partition_top;

    /// Deterministic value stream (no dev-dep; splitmix-class).
    fn rng(seed: &mut u64) -> u64 {
        *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let mut x = *seed;
        x ^= x >> 33;
        x = x.wrapping_mul(0xff51afd7ed558ccd);
        x ^ (x >> 33)
    }

    /// High-NDV text keys, URL-shaped: a shared scheme/host prefix and a
    /// long distinct tail (the q33/q34 key class — most groups are
    /// singletons, a heavy head repeats).
    fn key_of(j: u64) -> Vec<u8> {
        format!("http://example.test/page/{j:07}?ref=abcdefgh").into_bytes()
    }

    /// Row stream over `ndv` distinct keys: key 0 is the heavy head
    /// (~1/4 of rows), the rest spread uniformly (mostly singletons once
    /// `rows` ≈ `ndv`).
    fn keys(rows: u64, ndv: u64, seed: &mut u64) -> Vec<u64> {
        (0..rows)
            .map(|_| {
                let r = rng(seed);
                if r % 4 == 0 {
                    0
                } else {
                    1 + (r >> 8) % (ndv - 1)
                }
            })
            .collect()
    }

    /// Scatter the stream into fp128 records exactly as pass 1 does for
    /// raw parts: one record/row, cnt = 1, the locator carries the key
    /// ordinal (the tests' resolve map). Splitting the stream across
    /// `parts` reproduces the multi-worker arrival mix.
    fn records(ks: &[u64]) -> Vec<(u64, u64, u32, u32, u32)> {
        ks.iter()
            .map(|&j| {
                let h = crate::fp::entry_fp128(&key_of(j));
                ((h >> 64) as u64, h as u64, 1u32, (j >> 32) as u32, j as u32)
            })
            .collect()
    }

    fn loc_key(pl: u32, code: u32) -> Vec<u8> {
        key_of(((pl as u64) << 32) | code as u64)
    }

    /// The scalar reference law: exact count fold by KEY BYTES, then the
    /// (count DESC, bytes ASC) cut.
    fn oracle(ks: &[u64], k: usize) -> Vec<(Vec<u8>, u64)> {
        let mut m: std::collections::BTreeMap<Vec<u8>, u64> = Default::default();
        for &j in ks {
            *m.entry(key_of(j)).or_insert(0) += 1;
        }
        let mut v: Vec<(Vec<u8>, u64)> = m.into_iter().collect();
        v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        v.truncate(k);
        v
    }

    /// Identity: the fp128 fold + lazy-resolve selection answers the
    /// scalar oracle exactly — high-NDV singleton-dominated streams,
    /// pre-folded dict-grain records, and tie storms.
    #[test]
    fn fold_select_vs_oracle() {
        let mut seed = 0x51_7cc1;
        for (rows, ndv, k) in [(20_000u64, 8_000u64, 10usize), (5_000, 4_999, 25), (3_000, 8, 3)]
        {
            let ks = keys(rows, ndv, &mut seed);
            let recs = records(&ks);
            let mut top =
                t128s_partition_top(&recs, k, &mut |pl, code| loc_key(pl, code));
            top.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            assert_eq!(top, oracle(&ks, k), "rows={rows} ndv={ndv} k={k}");
        }
    }

    /// Pre-folded (dict-grain) records combine with raw per-row records
    /// of the same keys — the mixed dict/raw part arrival.
    #[test]
    fn fold_select_mixed_grain() {
        let mut seed = 0xfeed;
        let ks = keys(8_000, 3_000, &mut seed);
        let mut recs = records(&ks[..4_000]);
        // second half arrives pre-folded per key (entry grain)
        let mut m: std::collections::BTreeMap<u64, u32> = Default::default();
        for &j in &ks[4_000..] {
            *m.entry(j).or_insert(0) += 1;
        }
        for (&j, &c) in &m {
            let h = crate::fp::entry_fp128(&key_of(j));
            recs.push(((h >> 64) as u64, h as u64, c, (j >> 32) as u32, j as u32));
        }
        let mut top = t128s_partition_top(&recs, 12, &mut |pl, code| loc_key(pl, code));
        top.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        assert_eq!(top, oracle(&ks, 12));
    }

    /// The lazy-resolve law: byte resolution touches candidates that
    /// pass the count gate only — with a separated count head the
    /// resolve census stays ~k-proportional, never group-proportional
    /// (the ~25 s byte-materialization class this arm retires).
    #[test]
    fn lazy_resolve_bounded() {
        // 64 heavy keys (counts 1000+) over 50k singletons.
        let mut ks: Vec<u64> = Vec::new();
        for j in 0..64u64 {
            ks.extend(std::iter::repeat(j).take(1_000 + j as usize));
        }
        ks.extend(64..50_064u64);
        let recs = records(&ks);
        let mut resolves = 0u64;
        let k = 10;
        let top = t128s_partition_top(&recs, k, &mut |pl, code| {
            resolves += 1;
            loc_key(pl, code)
        });
        assert_eq!(top.len(), k);
        // Selection admits by count before any byte touch: once the
        // standing k-th count clears the singleton mass, singletons skip
        // WITHOUT resolving (the byte_spill contrast materializes every
        // group's bytes up front). Table order is hash-random but the
        // stream is seedless-deterministic, so the census is a fixed
        // number — the law gates it well under the group count.
        let groups = 64 + 50_000u64;
        assert!(
            resolves < groups / 2,
            "lazy resolve law: {resolves} resolves for k={k} over {groups} groups"
        );
        // and the cut is the true top-10 by count (keys 54..=63)
        assert_eq!(top.last().expect("k rows").1, 1_054);
    }
}
