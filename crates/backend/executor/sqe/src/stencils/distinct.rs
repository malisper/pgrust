//! famA distinct pipeline (hot-shape lineage): scatter → owned dedupe
//! → slice merge. Shape dispatch is schema/stats-driven (column widths of
//! the group/distinct columns), never query identity:
//!   - no group, byval distinct col  → run-collapsed key scatter + owned
//!     key+1 open tables under the partition law;
//!   - no group, varlena distinct col → per-part code presence bitmaps
//!     through the fused unpack, entry-grain hash-plane union;
//!   - [byval, varlena] group        → sparse packed-group pair distinct
//!: owner Cnt128 seen-set → Cnt64 group counts, O(groups) merge;
//!   - [varlena] group               → gid-range-owned pair distinct with
//!     elected selective decode (hot-shape; drivers::pair_distinct_gid_owned).

use crate::answer::{AnswerCol, AnswerSet, BytesBuild};
use crate::engine::SqeCtx;
use crate::ir::*;
use crate::kernels_g::ColState;
use crate::planner::{partition_count, run_collapse_witness};
use crate::scan::{CurCache, Scratch};
use crate::stencils::{col_width, sx};
use crate::typmeta::TypMeta;
use crate::grouped::hash64;

pub fn run_distinct_pipeline(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    let bank = ctx.bank;
    let d_col = node
        .agg
        .iter()
        .find(|a| {
            matches!(a.op, AggOp::CountDistinct | AggOp::SumDistinct | AggOp::AvgDistinct)
        })
        .and_then(|a| a.col)
        .expect("distinct pipeline: distinct-class agg");
    let gw: Vec<u8> = node.params.group_cols.iter().map(|&c| col_width(bank, c)).collect();
    // [sqe-m4] int-conjunct predicate route (COUNT(DISTINCT text) WHERE
    // <int frame>): the distinct pipeline composed with the SMA plane.
    let has_int_pred = node
        .pred
        .as_ref()
        .map(|p| !p.terms.is_empty())
        .unwrap_or(false);
    if has_int_pred {
        match gw.as_slice() {
            [] if col_width(bank, d_col) == 0 => {
                return filtered_text_set(ctx, node, d_col)
            }
            other => panic!("distinct pipeline: predicate route unsupported for shape {other:?}"),
        }
    }
    // [spill-3] over-budget SET shapes ride the spill arms.
    let sp_on = set_spill_engaged(bank, ctx.faces, node);
    match gw.as_slice() {
        [] if col_width(bank, d_col) > 0 && sp_on => int_set_spill(ctx, node, d_col),
        [] if col_width(bank, d_col) > 0 => int_set(ctx, node, d_col),
        [] if sp_on => text_set_spill(ctx, node, d_col),
        [] => text_set(ctx, node, d_col),
        [w, 0] if *w > 0 => packed_grouped(ctx, node, d_col),
        [0] => gid_grouped(ctx, node, d_col),
        // [spill-4] over-budget or non-dense-domain [w] shapes ride the
        // grouped pair-spill arm; the dense resident arm is unchanged.
        [w] if *w > 0 && pair_spill_engaged(bank, ctx.faces, node) => {
            int_grouped_spill(ctx, node, d_col)
        }
        [w] if *w > 0 => int_grouped(ctx, node, d_col),
        other => panic!("distinct pipeline: unsupported group shape {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// [spill-3] The SET routes' spill arms (spill-design.md §6): account,
// flush-at-share, drain key-sorted runs, dedupe-merge — in each
// route's own identity currency; scalar answers need no answer law.
// ---------------------------------------------------------------------------

/// Set-route spill engagement census (rig gates prove the legs ran).
pub static SSPILL_FLUSHES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static SSPILL_DRAINS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static SSPILL_MERGES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The ONE classification `check_distinct_budget` and the dispatch
/// read: exactly the `int_set`/`text_set` vocabulary — the filtered
/// route and the byte-key grouped routes stay §6 residue (the [w]
/// grouped route rides `pair_spill_serves`).
pub fn set_spill_serves(bank: &crate::bank::Bank, node: &PlanNode) -> bool {
    if !node.params.group_cols.is_empty()
        || !node.params.key_exprs.is_empty()
        || node.params.having.is_some()
        || node.params.agg_filters.iter().any(Option::is_some)
        || !node.params.ne_empty_cols.is_empty()
        || node
            .pred
            .as_ref()
            .is_some_and(|p| !p.terms.is_empty() || !p.var_terms.is_empty())
        || node.agg.is_empty()
    {
        return false;
    }
    let mut shared: Option<u32> = None;
    let mut all_count = true;
    for a in &node.agg {
        match a.op {
            AggOp::CountDistinct => {}
            AggOp::SumDistinct | AggOp::AvgDistinct => all_count = false,
            _ => return false,
        }
        let Some(c) = a.col else { return false };
        if shared.is_some() && shared != Some(c) {
            return false;
        }
        shared = Some(c);
    }
    let Some(d) = shared else { return false };
    col_width(bank, d) > 0 || all_count
}

/// [spill-4] Grouped pair-spill engagement census.
pub static PSPILL_FLUSHES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static PSPILL_DRAINS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static PSPILL_MERGES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// [spill-4] The [w]-grouped route's spill vocabulary — the ONE
/// authority the planner laws and the dispatch read: one byval group
/// key, one COUNT(DISTINCT byval), nothing else on the node.
pub fn pair_spill_serves(bank: &crate::bank::Bank, node: &PlanNode) -> bool {
    node.params.key_exprs.is_empty()
        && node.params.having.is_none()
        && node.params.having_min_count == 0
        && node.params.agg_filters.iter().all(Option::is_none)
        && node.params.ne_empty_cols.is_empty()
        && node
            .pred
            .as_ref()
            .map_or(true, |p| p.terms.is_empty() && p.var_terms.is_empty())
        && matches!(node.params.group_cols.as_slice(),
            [gk] if col_width(bank, *gk) > 0)
        && matches!(node.agg.as_slice(),
            [a] if a.op == AggOp::CountDistinct
                && a.col.is_some_and(|c| col_width(bank, c) > 0))
}

/// The resident [w] arm's domain law (its own assert authority).
pub fn int_grouped_dense(
    bank: &crate::bank::Bank,
    faces: &crate::engine::Faces,
    node: &PlanNode,
) -> bool {
    let a_g = node.params.group_cols[0];
    faces
        .stats(bank, a_g)
        .minmax_exact()
        .map_or(false, |(lo, hi)| lo >= 0 && hi < 1 << 20)
}

/// [spill-4] One engagement trigger for the grouped pair arm: armed
/// spill, a substrate, the served vocabulary, and either the E18
/// trigger or a domain the resident arm cannot hold.
pub fn pair_spill_engaged(
    bank: &crate::bank::Bank,
    faces: &crate::engine::Faces,
    node: &PlanNode,
) -> bool {
    faces.cfg.spill
        && crate::spill::available()
        && pair_spill_serves(bank, node)
        && ((bank.rows_total() as u128)
            * (crate::stencils::hash_plane::SCATTER_ROW_BYTES as u128)
            > faces.cfg.grouped_budget_bytes() as u128
            || !int_grouped_dense(bank, faces, node))
}

/// One engagement trigger for the planner law and the dispatch.
pub fn set_spill_engaged(
    bank: &crate::bank::Bank,
    faces: &crate::engine::Faces,
    node: &PlanNode,
) -> bool {
    faces.cfg.spill
        && (bank.rows_total() as u128)
            * (crate::stencils::hash_plane::SCATTER_ROW_BYTES as u128)
            > faces.cfg.grouped_budget_bytes() as u128
        && set_spill_serves(bank, node)
        && crate::spill::available()
}

/// The statement's spill store, or the typed no-substrate refusal.
fn set_store(ctx: &SqeCtx) -> std::sync::Arc<dyn crate::spill::SpillStore> {
    crate::spill::new_store().unwrap_or_else(|| {
        crate::refuse::raise_runtime(crate::refuse::Refuse::GroupedSpillUnavailable {
            what: "no-substrate",
            est: ctx
                .bank
                .rows_total()
                .saturating_mul(crate::stencils::hash_plane::SCATTER_ROW_BYTES),
            budget: ctx.faces.cfg.grouped_budget_bytes(),
        })
    })
}

/// One answer column per leg over the ONE dedup set (both arms).
fn set_render(node: &PlanNode, n: u64, dsum: i128) -> AnswerSet {
    let cols_out: Vec<AnswerCol> = node
        .agg
        .iter()
        .map(|a| match a.op {
            AggOp::CountDistinct => AnswerCol::i64s(TypMeta::INT8, vec![n as i64]),
            AggOp::SumDistinct => {
                let mut c = AnswerCol::i128s(a.out, vec![dsum]);
                if n == 0 {
                    c.validity = crate::answer::Validity::Mask(vec![false]);
                }
                c
            }
            AggOp::AvgDistinct => {
                let exact = a.in_ty.map(|t| t.width == 8).unwrap_or(false);
                AnswerCol::ratios(a.out, vec![(dsum, n as i64)], exact)
            }
            other => panic!("distinct set: unsupported agg {other:?} (admission gap)"),
        })
        .collect();
    AnswerSet::from_cols(cols_out)
}

/// int_set's spill arm: 8 B u64 key records; one count (and one
/// first-seen value fold) per distinct key at the dedupe merge.
fn int_set_spill(ctx: &SqeCtx, node: &PlanNode, a_k: u32) -> AnswerSet {
    use crate::stencils::hash_group::BW;
    let (bank, pool) = (ctx.bank, ctx.pool);
    let units = ctx.faces.walk(bank, a_k);
    let sv = ctx.faces.stats(bank, a_k);
    let max_ok =
        (0..bank.parts.len()).all(|pi| sv.part(pi).map_or(true, |r| (r.max_key as u64) != u64::MAX));
    assert!(max_ok, "distinct int_set: key+1 encoding unsound");
    let ndv = (sv.ndv_est_sum() as usize).max(1 << 16);
    let p = partition_count(ndv, node.params.slot_bytes, node.params.l2_bytes, pool.threads());
    let shift = 64 - p.trailing_zeros();
    let rc = run_collapse_witness(bank, a_k, &units, ctx.faces.cfg.threads);
    let share =
        ((ctx.faces.cfg.grouped_budget_bytes() / pool.threads().max(1) as u64).max(1)) as usize;
    let store = set_store(ctx);
    let storer = &store;
    struct S {
        su: Scratch,
        cur: CurCache,
        bk: Vec<Vec<u64>>,
        resident: usize,
        sp: Option<BW>,
        w: usize,
    }
    let pass1 = pool.run_finish(
        units.len(),
        |w| S {
            su: crate::scan::scratch_fetch(),
            cur: CurCache::new(a_k),
            bk: (0..p).map(|_| Vec::new()).collect(),
            resident: 0,
            sp: None,
            w,
        },
        |s: &mut S, i| {
            let (pi, g, rows, _) = units[i];
            let d = s.su.decode_full(s.cur.get(bank, pi), g, rows as usize);
            let mut pushed = 0usize;
            if rc {
                let mut prev = u64::MAX;
                for &x in d {
                    if x != prev {
                        prev = x;
                        s.bk[(hash64(x) >> shift) as usize].push(x);
                        pushed += 1;
                    }
                }
            } else {
                for &x in d {
                    s.bk[(hash64(x) >> shift) as usize].push(x);
                }
                pushed = d.len();
            }
            s.resident += pushed * 8;
            if s.resident > share {
                SSPILL_FLUSHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let w = s.w;
                let sp = s.sp.get_or_insert_with(|| BW::new(&**storer, "iset-scatter", w));
                for b in 0..p {
                    if s.bk[b].is_empty() {
                        continue;
                    }
                    sp.begin();
                    for &k in &s.bk[b] {
                        sp.push(&k.to_ne_bytes());
                    }
                    let (off, len) = sp.end();
                    sp.chunks.push((b as u32, off, len));
                    s.bk[b].clear();
                }
                s.resident = 0;
            }
        },
        |s| {
            crate::scan::scratch_park(s.su);
            (s.bk, s.sp.map(|w| (w.m, w.chunks)))
        },
    );
    let dsum_armed = node
        .agg
        .iter()
        .any(|a| matches!(a.op, AggOp::SumDistinct | AggOp::AvgDistinct));
    let dw = col_width(bank, a_k);
    let cap_entries = (share / 16).max(128);
    let slots = (cap_entries * 2).next_power_of_two();
    let chunk_slab = crate::spill::SLAB_BYTES.min(share.max(8));
    let store2 = &store;
    let counts = pool.run(
        p,
        |w| (0u64, 0i128, vec![0u64; slots], 0usize, None::<BW>, w),
        |st: &mut (u64, i128, Vec<u64>, usize, Option<BW>, usize), part| {
            let (n, sum, tbl, len, rw, w) =
                (&mut st.0, &mut st.1, &mut st.2, &mut st.3, &mut st.4, st.5);
            tbl.fill(0);
            *len = 0;
            let mask = slots - 1;
            let mut runs: Vec<(u64, u64)> = Vec::new();
            let mut drain = |tbl: &mut Vec<u64>, len: &mut usize, rw: &mut Option<BW>,
                             runs: &mut Vec<(u64, u64)>| {
                SSPILL_DRAINS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let mut keys: Vec<u64> = Vec::with_capacity(*len);
                for &e in tbl.iter() {
                    if e != 0 {
                        keys.push(e.wrapping_sub(1));
                    }
                }
                keys.sort_unstable();
                let bw = rw.get_or_insert_with(|| {
                    BW::new(&**store2, "iset-runs", w)
                });
                bw.begin();
                for k in &keys {
                    bw.push(&k.to_ne_bytes());
                }
                let (off, _len) = bw.end();
                runs.push((off, keys.len() as u64));
                tbl.fill(0);
                *len = 0;
            };
            let mut insert = |k: u64, tbl: &mut Vec<u64>, len: &mut usize,
                              rw: &mut Option<BW>, runs: &mut Vec<(u64, u64)>| {
                let enc = k.wrapping_add(1);
                loop {
                    let mut slot = (hash64(k) as usize) & mask;
                    loop {
                        let cur = tbl[slot];
                        if cur == 0 {
                            if *len >= cap_entries {
                                break;
                            }
                            tbl[slot] = enc;
                            *len += 1;
                            return;
                        }
                        if cur == enc {
                            return;
                        }
                        slot = (slot + 1) & mask;
                    }
                    drain(tbl, len, rw, runs);
                }
            };
            for s in pass1.iter() {
                if let Some((m, chunks)) = &s.1 {
                    for &(cb, off, blen) in chunks {
                        if cb as usize != part {
                            continue;
                        }
                        let mut cur =
                            crate::spill::ChunkCursor::new(&**m, off, blen / 8, 8, chunk_slab);
                        while let Some(r) = cur.next() {
                            let k = u64::from_ne_bytes(r.try_into().unwrap());
                            insert(k, tbl, len, rw, &mut runs);
                        }
                    }
                }
            }
            for s in pass1.iter() {
                for &k in &s.0[part] {
                    insert(k, tbl, len, rw, &mut runs);
                }
            }
            if runs.is_empty() {
                *n += *len as u64;
                if dsum_armed {
                    for &e in tbl.iter() {
                        if e != 0 {
                            *sum += sx(e.wrapping_sub(1), dw) as i128;
                        }
                    }
                }
                return;
            }
            SSPILL_MERGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            use std::cmp::Reverse;
            use std::collections::BinaryHeap;
            let mut mem: Vec<u64> = Vec::with_capacity(*len);
            for &e in tbl.iter() {
                if e != 0 {
                    mem.push(e.wrapping_sub(1));
                }
            }
            mem.sort_unstable();
            let m = &*rw.as_ref().expect("runs imply a run file").m;
            let nrun = runs.len();
            let slab = (share / (nrun + 1)).clamp(8, crate::spill::SLAB_BYTES);
            let mut curs: Vec<crate::spill::ChunkCursor> = runs
                .iter()
                .map(|&(off, g)| crate::spill::ChunkCursor::new(m, off, g, 8, slab))
                .collect();
            let mut mi = 0usize;
            let mut heap: BinaryHeap<Reverse<(u64, usize)>> = BinaryHeap::with_capacity(nrun + 1);
            for (i, c) in curs.iter_mut().enumerate() {
                if let Some(r) = c.next() {
                    heap.push(Reverse((u64::from_ne_bytes(r.try_into().unwrap()), i)));
                }
            }
            if mi < mem.len() {
                heap.push(Reverse((mem[mi], nrun)));
                mi += 1;
            }
            while let Some(Reverse((key, src))) = heap.pop() {
                let mut adv = |heap: &mut BinaryHeap<Reverse<(u64, usize)>>, s: usize| {
                    if s < nrun {
                        if let Some(r) = curs[s].next() {
                            heap.push(Reverse((u64::from_ne_bytes(r.try_into().unwrap()), s)));
                        }
                    } else if mi < mem.len() {
                        heap.push(Reverse((mem[mi], nrun)));
                        mi += 1;
                    }
                };
                adv(&mut heap, src);
                while let Some(&Reverse((k2, s2))) = heap.peek() {
                    if k2 != key {
                        break;
                    }
                    heap.pop();
                    adv(&mut heap, s2);
                }
                *n += 1;
                if dsum_armed {
                    *sum += sx(key, dw) as i128;
                }
            }
        },
    );
    let n: u64 = counts.iter().map(|c| c.0).sum();
    let dsum: i128 = counts.iter().map(|c| c.1).sum();
    drop(counts);
    pool.drop_par(pass1);
    set_render(node, n, dsum)
}

/// text_set's spill arm: 16 B fp128 records, the resident union's own
/// identity currency; dict presence bitmaps stay dict-bounded.
fn text_set_spill(ctx: &SqeCtx, node: &PlanNode, a_k: u32) -> AnswerSet {
    use crate::fp::entry_fp128;
    use crate::grouped::Cnt128;
    use crate::stencils::hash_group::BW;
    use crate::stencils::part_merge as pm;
    let (bank, pool) = (ctx.bank, ctx.pool);
    let entries = {
        let n = ctx.faces.stats(bank, a_k).ndv_est_sum() as usize;
        if n > 0 {
            n
        } else {
            ctx.faces
                .dicts_all(bank, a_k)
                .iter()
                .map(|df| df.ncodes as usize)
                .sum::<usize>()
                .max(1 << 16)
        }
    };
    let p = partition_count(entries, 16, node.params.l2_bytes, pool.threads());
    let pbits = p.trailing_zeros();
    let pf = pm::dict_faces(ctx, a_k);
    let t_fpb = std::time::Instant::now();
    let fps = pm::build_fps_cached(ctx, &pf, a_k);
    crate::engine::phn(node, "fp_build", t_fpb);
    let fpr: &[Vec<u128>] = &fps;
    let share =
        ((ctx.faces.cfg.grouped_budget_bytes() / pool.threads().max(1) as u64).max(1)) as usize;
    let store = set_store(ctx);
    let storer = &store;
    struct S {
        su: Scratch,
        codes: Vec<u32>,
        buckets: Vec<Vec<u128>>,
        resident: usize,
        sp: Option<BW>,
        w: usize,
    }
    let flush = |s: &mut S, p: usize| {
        SSPILL_FLUSHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let w = s.w;
        let sp = s.sp.get_or_insert_with(|| BW::new(&**storer, "tset-scatter", w));
        for b in 0..p {
            if s.buckets[b].is_empty() {
                continue;
            }
            sp.begin();
            for &h in &s.buckets[b] {
                sp.push(&h.to_ne_bytes());
            }
            let (off, len) = sp.end();
            sp.chunks.push((b as u32, off, len));
            s.buckets[b].clear();
        }
        s.resident = 0;
    };
    let pass1 = pool.run_finish(
        bank.parts.len(),
        |w| S {
            su: crate::scan::scratch_fetch(),
            codes: vec![0; 8192],
            buckets: (0..p).map(|_| Vec::new()).collect(),
            resident: 0,
            sp: None,
            w,
        },
        |s, pi| {
            let df = ctx.faces.dict(bank, pi, a_k);
            if df.dh.is_some() {
                let n = df.ncodes as usize;
                let mut bits = vec![0u64; n.div_ceil(64)];
                let fc = crate::fused::FusedCodes::open(bank, pi, a_k);
                let mut cur = crate::scan::open_cursor(bank, pi, a_k);
                for g in 0..cur.granule_count() {
                    let rows = cur.rows_in_granule(g) as usize;
                    match &fc {
                        Some(fc) => fc.fold(g, rows, |_, c| {
                            bits[(c >> 6) as usize] |= 1u64 << (c & 63);
                        }),
                        None => {
                            if s.codes.len() < rows {
                                s.codes.resize(rows, 0);
                            }
                            cur.decode_codes(g, &mut s.codes[..rows]).expect("codes");
                            for &c in &s.codes[..rows] {
                                bits[(c >> 6) as usize] |= 1u64 << (c & 63);
                            }
                        }
                    }
                }
                let fpp = &fpr[pi];
                for c in 0..n {
                    if bits[c >> 6] & (1u64 << (c & 63)) != 0 {
                        let h = fpp[c];
                        s.buckets[(h >> (128 - pbits)) as usize].push(h);
                        s.resident += 16;
                        if s.resident > share {
                            flush(s, p);
                        }
                    }
                }
            } else {
                // Byte-accounted local dedupe; the owner union dedupes across drains.
                let mut cu = crate::scan::open_cursor(bank, pi, a_k);
                let mut local: std::collections::HashSet<Vec<u8>> = Default::default();
                let mut lbytes = 0usize;
                for g in 0..cu.granule_count() {
                    let rows = cu.rows_in_granule(g) as usize;
                    let d = s.su.decode_full(&mut cu, g, rows);
                    for &x in d {
                        let pl = unsafe { crate::scan::varlena_payload(x) };
                        if !local.contains(pl) {
                            lbytes += pl.len() + 48;
                            local.insert(pl.to_vec());
                        }
                    }
                    if lbytes * 2 > share {
                        for kk in local.drain() {
                            let h = entry_fp128(&kk);
                            s.buckets[(h >> (128 - pbits)) as usize].push(h);
                            s.resident += 16;
                        }
                        lbytes = 0;
                        if s.resident * 2 > share {
                            flush(s, p);
                        }
                    }
                }
                for kk in local {
                    let h = entry_fp128(&kk);
                    s.buckets[(h >> (128 - pbits)) as usize].push(h);
                    s.resident += 16;
                    if s.resident > share {
                        flush(s, p);
                    }
                }
            }
        },
        |s: S| {
            crate::scan::scratch_park(s.su);
            (s.buckets, s.sp.map(|w| (w.m, w.chunks)))
        },
    );
    let cap_entries = (share / 40).max(128);
    let chunk_slab = crate::spill::SLAB_BYTES.min(share.max(16));
    let store2 = &store;
    let owned = pool.run(
        p,
        |w| (0u64, w, None::<BW>),
        |st: &mut (u64, usize, Option<BW>), part| {
            let (distinct, w, rw) = (&mut st.0, st.1, &mut st.2);
            let n: usize = pass1.iter().map(|s| s.0[part].len()).sum();
            let mut seen = Cnt128::new(cap_entries.min(n.max(16)));
            let mut runs: Vec<(u64, u64)> = Vec::new();
            let mut drain = |seen: &mut Cnt128, rw: &mut Option<BW>,
                             runs: &mut Vec<(u64, u64)>| {
                SSPILL_DRAINS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let mut keys: Vec<u128> = Vec::with_capacity(seen.len);
                for i in 0..seen.cap() {
                    if seen.cnt[i] != 0 {
                        keys.push(seen.keys[i]);
                    }
                }
                keys.sort_unstable();
                let bw = rw.get_or_insert_with(|| BW::new(&**store2, "tset-runs", w));
                bw.begin();
                for k in &keys {
                    bw.push(&k.to_ne_bytes());
                }
                let (off, _len) = bw.end();
                runs.push((off, keys.len() as u64));
                seen.reset(cap_entries);
            };
            for s in pass1.iter() {
                if let Some((m, chunks)) = &s.1 {
                    for &(cb, off, blen) in chunks {
                        if cb as usize != part {
                            continue;
                        }
                        let mut cur =
                            crate::spill::ChunkCursor::new(&**m, off, blen / 16, 16, chunk_slab);
                        while let Some(r) = cur.next() {
                            let h = u128::from_ne_bytes(r.try_into().unwrap());
                            if seen.len >= cap_entries {
                                drain(&mut seen, rw, &mut runs);
                            }
                            seen.add(h, 1);
                        }
                    }
                }
            }
            for s in pass1.iter() {
                for &h in &s.0[part] {
                    if seen.len >= cap_entries {
                        drain(&mut seen, rw, &mut runs);
                    }
                    seen.add(h, 1);
                }
            }
            if runs.is_empty() {
                *distinct += seen.len as u64;
                return;
            }
            SSPILL_MERGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
            let slab = (share / (nrun + 1)).clamp(16, crate::spill::SLAB_BYTES);
            let mut curs: Vec<crate::spill::ChunkCursor> = runs
                .iter()
                .map(|&(off, g)| crate::spill::ChunkCursor::new(m, off, g, 16, slab))
                .collect();
            let mut mi = 0usize;
            let mut heap: BinaryHeap<Reverse<(u128, usize)>> = BinaryHeap::with_capacity(nrun + 1);
            for (i, c) in curs.iter_mut().enumerate() {
                if let Some(r) = c.next() {
                    heap.push(Reverse((u128::from_ne_bytes(r.try_into().unwrap()), i)));
                }
            }
            if mi < mem.len() {
                heap.push(Reverse((mem[mi], nrun)));
                mi += 1;
            }
            while let Some(Reverse((key, src))) = heap.pop() {
                let mut adv = |heap: &mut BinaryHeap<Reverse<(u128, usize)>>, s: usize| {
                    if s < nrun {
                        if let Some(r) = curs[s].next() {
                            heap.push(Reverse((u128::from_ne_bytes(r.try_into().unwrap()), s)));
                        }
                    } else if mi < mem.len() {
                        heap.push(Reverse((mem[mi], nrun)));
                        mi += 1;
                    }
                };
                adv(&mut heap, src);
                while let Some(&Reverse((k2, s2))) = heap.peek() {
                    if k2 != key {
                        break;
                    }
                    heap.pop();
                    adv(&mut heap, s2);
                }
                *distinct += 1;
            }
        },
    );
    let total: u64 = owned.iter().map(|o| o.0).sum();
    drop(owned);
    pool.drop_par(pass1);
    pm::park_fps(fps);
    AnswerSet::from_cols(vec![AnswerCol::i64s(TypMeta::INT8, vec![total as i64])])
}

// ---------------------------------------------------------------------------
// grouped pair distinct, dense int group (hot-shape lineage, sqe-m1): run-collapsed
// (group, distinct) pair scatter -> drivers::pair_distinct_owned (owner
// Cnt128 seen-set, first-insert bumps per-worker DENSE counts[group];
// merge = O(groups) vector add). Domain bound is a stats election.
// ---------------------------------------------------------------------------

fn int_grouped(ctx: &SqeCtx, node: &PlanNode, a_d: u32) -> AnswerSet {
    use crate::drivers::pair_distinct_owned;
    let (bank, pool) = (ctx.bank, ctx.pool);
    let a_g = node.params.group_cols[0];
    let dense = {
        let sv = ctx.faces.stats(bank, a_g);
        let (lo, hi) = sv.minmax_exact().expect("int_grouped: exact stats bounds");
        assert!(lo >= 0 && hi < 1 << 20, "int_grouped: domain not dense-safe: {lo}..{hi}");
        (hi + 1) as usize
    };
    let units = ctx.faces.walk(bank, a_g);
    // Run-collapse: correctness-free (the owner table still dedupes across
    // runs); elected by the distinct column's sortedness witness — pair
    // runs require the fine key to cluster.
    let rc = run_collapse_witness(bank, a_d, &units, ctx.faces.cfg.threads);
    let mask = if col_width(bank, a_g) < 8 {
        (1u64 << (8 * col_width(bank, a_g) as u32)) - 1
    } else {
        u64::MAX
    };
    let init = |_| (ColState::new(a_g), ColState::new(a_d));
    let units2 = &units;
    let fill = move |st: &mut (ColState, ColState), i: usize, out: &mut Vec<(u64, u64)>| {
        let (pi, g, rows, _) = units2[i];
        let rows = rows as usize;
        let dg = st.0.dec(bank, pi, g, rows);
        let dg: &[u64] = unsafe { std::slice::from_raw_parts(dg.as_ptr(), dg.len()) };
        let dd = st.1.dec(bank, pi, g, rows);
        let dd: &[u64] = unsafe { std::slice::from_raw_parts(dd.as_ptr(), dd.len()) };
        if rc {
            let mut prev: Option<(u64, u64)> = None;
            for r in 0..rows {
                let pair = (dg[r] & mask, dd[r]);
                if prev != Some(pair) {
                    out.push(pair);
                    prev = Some(pair);
                }
            }
        } else {
            for r in 0..rows {
                out.push((dg[r] & mask, dd[r]));
            }
        }
    };
    let out = pair_distinct_owned(pool, &units, dense, init, fill);
    let w = col_width(bank, a_g);
    let mut rows = out.groups;
    rows.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let k = node.params.emit_cap();
    rows.truncate(k);
    let window: Vec<&(u64, u64)> = rows.iter().skip(node.params.offset).collect();
    AnswerSet::from_cols(vec![
        AnswerCol::i64s(node.ty_of(a_g), window.iter().map(|&&(g, _)| sx(g, w)).collect()),
        AnswerCol::i64s(TypMeta::INT8, window.iter().map(|&&(_, c)| c as i64).collect()),
    ])
}

/// [spill-4] The [w] grouped route's pair-spill arm: 16 B (masked key,
/// value) records, key-partitioned; per-partition dedupe tables drain
/// sorted u128 runs; the k-way dedupe merge counts one per distinct
/// pair and feeds a capped (count DESC, key ASC) selector — the
/// resident arm's exact tie order on any served domain.
fn int_grouped_spill(ctx: &SqeCtx, node: &PlanNode, a_d: u32) -> AnswerSet {
    use crate::stencils::hash_group::BW;
    let (bank, pool) = (ctx.bank, ctx.pool);
    let a_g = node.params.group_cols[0];
    let wg = col_width(bank, a_g);
    let units = ctx.faces.walk(bank, a_g);
    let mask = if wg < 8 { (1u64 << (8 * wg as u32)) - 1 } else { u64::MAX };
    let p = (pool.threads().max(1) * 4).next_power_of_two();
    let shift = 64 - p.trailing_zeros();
    let rc = run_collapse_witness(bank, a_d, &units, ctx.faces.cfg.threads);
    let share =
        ((ctx.faces.cfg.grouped_budget_bytes() / pool.threads().max(1) as u64).max(4096)) as usize;
    let store = set_store(ctx);
    let storer = &store;
    let units2 = &units;
    struct S1 {
        g: ColState,
        d: ColState,
        bk: Vec<Vec<(u64, u64)>>,
        resident: usize,
        sp: Option<BW>,
        w: usize,
    }
    let pass1 = pool.run_finish(
        units.len(),
        |w| S1 {
            g: ColState::new(a_g),
            d: ColState::new(a_d),
            bk: (0..p).map(|_| Vec::new()).collect(),
            resident: 0,
            sp: None,
            w,
        },
        |s: &mut S1, i| {
            let (pi, gr, rows, _) = units2[i];
            let rows = rows as usize;
            let dg = s.g.dec(bank, pi, gr, rows);
            let dg: &[u64] = unsafe { std::slice::from_raw_parts(dg.as_ptr(), dg.len()) };
            let dd = s.d.dec(bank, pi, gr, rows);
            let dd: &[u64] = unsafe { std::slice::from_raw_parts(dd.as_ptr(), dd.len()) };
            let mut pushed = 0usize;
            if rc {
                let mut prev: Option<(u64, u64)> = None;
                for r in 0..rows {
                    let pair = (dg[r] & mask, dd[r]);
                    if prev != Some(pair) {
                        prev = Some(pair);
                        s.bk[(hash64(pair.0) >> shift) as usize].push(pair);
                        pushed += 1;
                    }
                }
            } else {
                for r in 0..rows {
                    s.bk[(hash64(dg[r] & mask) >> shift) as usize].push((dg[r] & mask, dd[r]));
                }
                pushed = rows;
            }
            s.resident += pushed * 16;
            if s.resident > share {
                PSPILL_FLUSHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let w = s.w;
                let sp = s.sp.get_or_insert_with(|| BW::new(&**storer, "igrp-scatter", w));
                for b in 0..p {
                    if s.bk[b].is_empty() {
                        continue;
                    }
                    sp.begin();
                    for &(k, v) in &s.bk[b] {
                        sp.push(&k.to_ne_bytes());
                        sp.push(&v.to_ne_bytes());
                    }
                    let (off, len) = sp.end();
                    sp.chunks.push((b as u32, off, len));
                    s.bk[b].clear();
                }
                s.resident = 0;
            }
        },
        |s| (s.bk, s.sp.map(|w| (w.m, w.chunks))),
    );
    let cap_entries = (share / 16).max(128);
    let slots = (cap_entries * 2).next_power_of_two();
    let chunk_slab = crate::spill::SLAB_BYTES.min(share.max(16));
    let kcap = node.params.emit_cap();
    let store2 = &store;
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;
    type Rank = (u64, Reverse<i64>);
    let parts = pool.run(
        p,
        |w| {
            (
                vec![0u128; slots],
                vec![0u64; slots / 64 + 1],
                0usize,
                None::<BW>,
                Vec::<(u64, i64)>::new(),
                w,
            )
        },
        |st: &mut (Vec<u128>, Vec<u64>, usize, Option<BW>, Vec<(u64, i64)>, usize), part| {
            let (tbl, bits, len, rw, out, w) =
                (&mut st.0, &mut st.1, &mut st.2, &mut st.3, &mut st.4, st.5);
            tbl.fill(0);
            bits.fill(0);
            *len = 0;
            let mask2 = slots - 1;
            let mut runs: Vec<(u64, u64)> = Vec::new();
            let mut drain = |tbl: &mut Vec<u128>, bits: &mut Vec<u64>, len: &mut usize,
                             rw: &mut Option<BW>, runs: &mut Vec<(u64, u64)>| {
                PSPILL_DRAINS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let mut es: Vec<u128> = Vec::with_capacity(*len);
                for (i, &e) in tbl.iter().enumerate() {
                    if bits[i / 64] & (1u64 << (i % 64)) != 0 {
                        es.push(e);
                    }
                }
                es.sort_unstable();
                let bw = rw.get_or_insert_with(|| BW::new(&**store2, "igrp-runs", w));
                bw.begin();
                for e in &es {
                    bw.push(&e.to_ne_bytes());
                }
                let (off, _l) = bw.end();
                runs.push((off, es.len() as u64));
                tbl.fill(0);
                bits.fill(0);
                *len = 0;
            };
            let mut insert = |k: u64, v: u64, tbl: &mut Vec<u128>, bits: &mut Vec<u64>,
                              len: &mut usize, rw: &mut Option<BW>,
                              runs: &mut Vec<(u64, u64)>| {
                let e = ((k as u128) << 64) | v as u128;
                let h = hash64(k ^ hash64(v));
                loop {
                    let mut slot = (h as usize) & mask2;
                    loop {
                        if bits[slot / 64] & (1u64 << (slot % 64)) == 0 {
                            if *len >= cap_entries {
                                break;
                            }
                            bits[slot / 64] |= 1u64 << (slot % 64);
                            tbl[slot] = e;
                            *len += 1;
                            return;
                        }
                        if tbl[slot] == e {
                            return;
                        }
                        slot = (slot + 1) & mask2;
                    }
                    drain(tbl, bits, len, rw, runs);
                }
            };
            for s in pass1.iter() {
                if let Some((m, chunks)) = &s.1 {
                    for &(cb, off, blen) in chunks {
                        if cb as usize != part {
                            continue;
                        }
                        let mut cur =
                            crate::spill::ChunkCursor::new(&**m, off, blen / 16, 16, chunk_slab);
                        while let Some(r) = cur.next() {
                            let k = u64::from_ne_bytes(r[..8].try_into().unwrap());
                            let v = u64::from_ne_bytes(r[8..].try_into().unwrap());
                            insert(k, v, tbl, bits, len, rw, &mut runs);
                        }
                    }
                }
            }
            for s in pass1.iter() {
                for &(k, v) in &s.0[part] {
                    insert(k, v, tbl, bits, len, rw, &mut runs);
                }
            }
            let mut keep: BinaryHeap<Reverse<Rank>> = BinaryHeap::new();
            let mut emit = |key: u64, cnt: u64, keep: &mut BinaryHeap<Reverse<Rank>>| {
                let r: Rank = (cnt, Reverse(sx(key, wg)));
                if keep.len() < kcap {
                    keep.push(Reverse(r));
                } else if keep.peek().is_some_and(|worst| r > worst.0) {
                    keep.pop();
                    keep.push(Reverse(r));
                }
            };
            if runs.is_empty() {
                let mut es: Vec<u128> = Vec::with_capacity(*len);
                for (i, &e) in tbl.iter().enumerate() {
                    if bits[i / 64] & (1u64 << (i % 64)) != 0 {
                        es.push(e);
                    }
                }
                es.sort_unstable();
                let mut cur: Option<(u64, u64)> = None;
                for &e in &es {
                    let key = (e >> 64) as u64;
                    match &mut cur {
                        Some((k0, c)) if *k0 == key => *c += 1,
                        _ => {
                            if let Some((k0, c)) = cur {
                                emit(k0, c, &mut keep);
                            }
                            cur = Some((key, 1));
                        }
                    }
                }
                if let Some((k0, c)) = cur {
                    emit(k0, c, &mut keep);
                }
            } else {
                PSPILL_MERGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let mut mem: Vec<u128> = Vec::with_capacity(*len);
                for (i, &e) in tbl.iter().enumerate() {
                    if bits[i / 64] & (1u64 << (i % 64)) != 0 {
                        mem.push(e);
                    }
                }
                mem.sort_unstable();
                let m = &*rw.as_ref().expect("runs imply a run file").m;
                let nrun = runs.len();
                let slab = (share / (nrun + 1)).clamp(16, crate::spill::SLAB_BYTES);
                let mut curs: Vec<crate::spill::ChunkCursor> = runs
                    .iter()
                    .map(|&(off, g)| crate::spill::ChunkCursor::new(m, off, g, 16, slab))
                    .collect();
                let mut mi = 0usize;
                let mut heap: BinaryHeap<Reverse<(u128, usize)>> =
                    BinaryHeap::with_capacity(nrun + 1);
                for (i, c) in curs.iter_mut().enumerate() {
                    if let Some(r) = c.next() {
                        heap.push(Reverse((u128::from_ne_bytes(r.try_into().unwrap()), i)));
                    }
                }
                if mi < mem.len() {
                    heap.push(Reverse((mem[mi], nrun)));
                    mi += 1;
                }
                let mut cur: Option<(u64, u64)> = None;
                while let Some(Reverse((e, src))) = heap.pop() {
                    let mut adv = |heap: &mut BinaryHeap<Reverse<(u128, usize)>>, s: usize| {
                        if s < nrun {
                            if let Some(r) = curs[s].next() {
                                heap.push(Reverse((
                                    u128::from_ne_bytes(r.try_into().unwrap()),
                                    s,
                                )));
                            }
                        } else if mi < mem.len() {
                            heap.push(Reverse((mem[mi], nrun)));
                            mi += 1;
                        }
                    };
                    adv(&mut heap, src);
                    while let Some(&Reverse((e2, s2))) = heap.peek() {
                        if e2 != e {
                            break;
                        }
                        heap.pop();
                        adv(&mut heap, s2);
                    }
                    let key = (e >> 64) as u64;
                    match &mut cur {
                        Some((k0, c)) if *k0 == key => *c += 1,
                        _ => {
                            if let Some((k0, c)) = cur {
                                emit(k0, c, &mut keep);
                            }
                            cur = Some((key, 1));
                        }
                    }
                }
                if let Some((k0, c)) = cur {
                    emit(k0, c, &mut keep);
                }
            }
            out.extend(keep.into_iter().map(|Reverse((c, Reverse(k)))| (c, k)));
        },
    );
    let mut rows: Vec<(u64, i64)> =
        parts.iter().flat_map(|st| st.4.iter().copied()).collect();
    drop(parts);
    pool.drop_par(pass1);
    rows.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    rows.truncate(kcap);
    let window: Vec<&(u64, i64)> = rows.iter().skip(node.params.offset).collect();
    AnswerSet::from_cols(vec![
        AnswerCol::i64s(node.ty_of(a_g), window.iter().map(|&&(_, k)| k).collect()),
        AnswerCol::i64s(TypMeta::INT8, window.iter().map(|&&(c, _)| c as i64).collect()),
    ])
}

// ---------------------------------------------------------------------------
// key-only int set
// ---------------------------------------------------------------------------

fn int_set(ctx: &SqeCtx, node: &PlanNode, a_k: u32) -> AnswerSet {
    let (bank, pool) = (ctx.bank, ctx.pool);
    let units = ctx.faces.walk(bank, a_k);
    let sv = ctx.faces.stats(bank, a_k);
    // key+1 sentinel soundness (hot-shape law): exact max must not be u64::MAX.
    let max_ok =
        (0..bank.parts.len()).all(|pi| sv.part(pi).map_or(true, |r| (r.max_key as u64) != u64::MAX));
    assert!(max_ok, "distinct int_set: key+1 encoding unsound");
    let ndv = (sv.ndv_est_sum() as usize).max(1 << 16);
    let p = partition_count(ndv, node.params.slot_bytes, node.params.l2_bytes, ctx.pool.threads());
    let shift = 64 - p.trailing_zeros();
    let rc = run_collapse_witness(bank, a_k, &units, ctx.faces.cfg.threads);
    let tp = pool.threads();
    // [spill-3, shrink law] formerly UNCAPPED caller-TLS parks —
    // rehomed on the capped query-agnostic StatePark (scatter-arena /
    // table classes); data-only, reshaped at fetch, cursors never park.
    static PARK1: crate::stencils::statepark::StatePark<Vec<Vec<u64>>> =
        crate::stencils::statepark::StatePark::new(256 << 20);
    static PARK2: crate::stencils::statepark::StatePark<Vec<u64>> =
        crate::stencils::statepark::StatePark::new(64 << 20);
    let per_bucket = (bank.rows_total() as usize / tp.max(1) / p / 2).max(64);
    let mut pass1 = pool.run_finish(
        units.len(),
        |_| {
            let parked = PARK1.fetch();
            let mut s = (
                crate::scan::scratch_fetch(),
                CurCache::new(a_k),
                parked.unwrap_or_default(),
                0u64,
            );
            if s.2.len() != p {
                s.2 = (0..p).map(|_| Vec::with_capacity(per_bucket)).collect();
            } else {
                for b in &mut s.2 {
                    b.clear();
                }
            }
            s
        },
        |s: &mut (Scratch, CurCache, Vec<Vec<u64>>, u64), i| {
            let (pi, g, rows, _) = units[i];
            let d = s.0.decode_full(s.1.get(bank, pi), g, rows as usize);
            let bk = &mut s.2;
            if rc {
                let mut prev = u64::MAX;
                for &x in d {
                    if x != prev {
                        prev = x;
                        bk[(hash64(x) >> shift) as usize].push(x);
                    }
                }
            } else {
                for &x in d {
                    bk[(hash64(x) >> shift) as usize].push(x);
                }
            }
            s.3 += rows as u64;
        },
        |s| {
            crate::scan::scratch_park(s.0);
            (s.2, s.3)
        },
    );
    let scattered: Vec<&Vec<Vec<u64>>> = pass1.iter().map(|s| &s.0).collect();
    // [aggqual] any sum/avg DISTINCT leg arms the first-insert value
    // fold (exact i128 — each distinct element folds exactly once).
    let dsum_armed = node
        .agg
        .iter()
        .any(|a| matches!(a.op, AggOp::SumDistinct | AggOp::AvgDistinct));
    let dw = col_width(bank, a_k);
    let counts = pool.run(
        p,
        |_| (PARK2.fetch().unwrap_or_default(), 0u64, 0i128),
        |(tbl, acc, sum), part| {
            let n: usize = scattered.iter().map(|b| b[part].len()).sum();
            let cap = (n * 2).next_power_of_two().max(1024);
            if tbl.len() < cap {
                tbl.resize(cap, 0);
            }
            let tbl = &mut tbl[..cap];
            tbl.fill(0);
            let mask = cap - 1;
            let mut cnt = 0u64;
            for b in &scattered {
                for &k in &b[part] {
                    let enc = k.wrapping_add(1);
                    let mut slot = (hash64(k) as usize) & mask;
                    loop {
                        let cur = tbl[slot];
                        if cur == 0 {
                            tbl[slot] = enc;
                            cnt += 1;
                            if dsum_armed {
                                *sum += sx(k, dw) as i128;
                            }
                            break;
                        }
                        if cur == enc {
                            break;
                        }
                        slot = (slot + 1) & mask;
                    }
                }
            }
            *acc += cnt;
        },
    );
    let n: u64 = counts.iter().map(|c| c.1).sum();
    let dsum: i128 = counts.iter().map(|c| c.2).sum();
    drop(scattered);
    for s in pass1.drain(..) {
        let b = s.0.iter().map(|v| v.capacity() * 8).sum::<usize>() + s.0.capacity() * 24;
        PARK1.park(s.0, b);
    }
    for c in counts.into_iter() {
        let b = c.0.capacity() * 8;
        PARK2.park(c.0, b);
    }
    // One answer column per leg (every leg reads the ONE dedup set —
    // the admission's shared-column contract). Empty set: count 0,
    // sum/avg NULL through the validity leg.
    let cols_out: Vec<AnswerCol> = node
        .agg
        .iter()
        .map(|a| match a.op {
            AggOp::CountDistinct => AnswerCol::i64s(TypMeta::INT8, vec![n as i64]),
            AggOp::SumDistinct => {
                let mut c = AnswerCol::i128s(a.out, vec![dsum]);
                if n == 0 {
                    c.validity = crate::answer::Validity::Mask(vec![false]);
                }
                c
            }
            AggOp::AvgDistinct => {
                let exact = a.in_ty.map(|t| t.width == 8).unwrap_or(false);
                AnswerCol::ratios(a.out, vec![(dsum, n as i64)], exact)
            }
            other => panic!("distinct int_set: unsupported agg {other:?} (admission gap)"),
        })
        .collect();
    AnswerSet::from_cols(cols_out)
}

// ---------------------------------------------------------------------------
// key-only text set: the set never materializes strings.
// ---------------------------------------------------------------------------

fn text_set(ctx: &SqeCtx, node: &PlanNode, a_k: u32) -> AnswerSet {
    use crate::fp::entry_fp128;
    use crate::stencils::part_merge as pm;
    let (bank, pool) = (ctx.bank, ctx.pool);
    let entries = {
        let n = ctx.faces.stats(bank, a_k).ndv_est_sum() as usize;
        if n > 0 {
            n
        } else {
            ctx.faces
                .dicts_all(bank, a_k)
                .iter()
                .map(|df| df.ncodes as usize)
                .sum::<usize>()
                .max(1 << 16)
        }
    };
    // union slot: u128 hash at ≤1/2 load ⇒ 16B slot cost input.
    let p = partition_count(entries, 16, node.params.l2_bytes, ctx.pool.threads());
    let pbits = p.trailing_zeros();
    // [fpcache] the SAME Faces-homed cached fp plane as the fp-combine
    // (one cache per column, never a second).
    let pf = pm::dict_faces(ctx, a_k);
    let t_fpb = std::time::Instant::now();
    let fps = pm::build_fps_cached(ctx, &pf, a_k);
    crate::engine::phn(node, "fp_build", t_fpb);
    let fpr: &[Vec<u128>] = &fps;
    struct S {
        su: Scratch,
        codes: Vec<u32>,
        buckets: Vec<Vec<u128>>,
    }
    let pass1 = pool.run_finish(
        bank.parts.len(),
        |_| S {
            su: crate::scan::scratch_fetch(),
            codes: vec![0; 8192],
            buckets: (0..p).map(|_| Vec::new()).collect(),
        },
        |s, pi| {
            let df = ctx.faces.dict(bank, pi, a_k);
            if df.dh.is_some() {
                let n = df.ncodes as usize;
                let mut bits = vec![0u64; n.div_ceil(64)];
                let fc = crate::fused::FusedCodes::open(bank, pi, a_k);
                let mut cur = crate::scan::open_cursor(bank, pi, a_k);
                for g in 0..cur.granule_count() {
                    let rows = cur.rows_in_granule(g) as usize;
                    match &fc {
                        Some(fc) => fc.fold(g, rows, |_, c| {
                            bits[(c >> 6) as usize] |= 1u64 << (c & 63);
                        }),
                        None => {
                            if s.codes.len() < rows {
                                s.codes.resize(rows, 0);
                            }
                            cur.decode_codes(g, &mut s.codes[..rows]).expect("codes");
                            for &c in &s.codes[..rows] {
                                bits[(c >> 6) as usize] |= 1u64 << (c & 63);
                            }
                        }
                    }
                }
                let fpp = &fpr[pi];
                for c in 0..n {
                    if bits[c >> 6] & (1u64 << (c & 63)) != 0 {
                        let h = fpp[c];
                        s.buckets[(h >> (128 - pbits)) as usize].push(h);
                    }
                }
            } else {
                let mut cu = crate::scan::open_cursor(bank, pi, a_k);
                let mut local: std::collections::HashSet<Vec<u8>> = Default::default();
                for g in 0..cu.granule_count() {
                    let rows = cu.rows_in_granule(g) as usize;
                    let d = s.su.decode_full(&mut cu, g, rows);
                    for &x in d {
                        let pl = unsafe { crate::scan::varlena_payload(x) };
                        if !local.contains(pl) {
                            local.insert(pl.to_vec());
                        }
                    }
                }
                for kk in local {
                    let h = entry_fp128(&kk);
                    s.buckets[(h >> (128 - pbits)) as usize].push(h);
                }
            }
        },
        // Worker-side finish: the decode arena parks on THIS worker's
        // depot; only the scattered hash buckets cross back.
        |s: S| {
            crate::scan::scratch_park(s.su);
            s.buckets
        },
    );
    let owned = pool.run(
        p,
        |_| 0u64,
        |distinct: &mut u64, part| {
            let n: usize = pass1.iter().map(|s| s[part].len()).sum();
            let cap = (n * 2).next_power_of_two().max(16);
            let mask = cap - 1;
            let mut th: Vec<u128> = vec![0; cap];
            let mut occ: Vec<bool> = vec![false; cap];
            for s in pass1.iter() {
                for &h in &s[part] {
                    let mut i = (h as usize) & mask;
                    loop {
                        if !occ[i] {
                            occ[i] = true;
                            th[i] = h;
                            *distinct += 1;
                            break;
                        }
                        if th[i] == h {
                            break;
                        }
                        i = (i + 1) & mask;
                    }
                }
            }
        },
    );
    let total: u64 = owned.iter().sum();
    pm::park_fps(fps);
    AnswerSet::from_cols(vec![AnswerCol::i64s(TypMeta::INT8, vec![total as i64])])
}

// ---------------------------------------------------------------------------
// grouped pair distinct, gid-dense group: drivers::pair_distinct_
// gid_owned under the gid-range law; selective decode elected when the
// group is a single filtered varlena column (the distinct column is then
// the only other decode — decode_sel scales it with survivors).
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// [noglobaldict] hot-shape shape without the merge registry: the pair-distinct
// engine over part-scoped codes, then dense per-part distinct counts and
// the k-way string merge for the (distinct DESC, bytes ASC) top-k.
// ---------------------------------------------------------------------------

fn gid_grouped(ctx: &SqeCtx, node: &PlanNode, a_d: u32) -> AnswerSet {
    use crate::stencils::part_merge as pm;
    let pool = ctx.pool;
    let a_t = node.params.group_cols[0];
    let drop_empty = node.params.flags & F_DROP_EMPTY_KEY != 0;
    if ctx.faces.cfg.fpcombine {
        // [fpcombine] fingerprint pair engine: no closure, no hit sorts,
        // fragments carry their fingerprints into the combine.
        let t_fill = std::time::Instant::now();
        let (mut runs, side_raw) =
            pm::pair_distinct_fp(ctx, node.q, a_t, a_d, drop_empty);
        crate::engine::phn(node, "scatter_own", t_fill);
        let t_m = std::time::Instant::now();
        let mut side_intern = pm::Intern::new(0);
        let mut sf: Vec<pm::FpFrag> = Vec::new();
        for ((b, _), c) in &side_raw {
            sf.push((crate::fp::entry_fp128(b), side_intern.key(b), *c));
        }
        runs.push(sf);
        let pf = pm::dict_faces(ctx, a_t);
        let kb = pm::KeyBytes { pf: &pf, interns: vec![side_intern.tab.as_slice()] };
        let k = node.params.emit_cap();
        let cands = pm::fp_combine_pre(pool, &runs, k);
        let mut top: Vec<(u64, &[u8])> =
            cands.iter().map(|&(c, key, _)| (c, kb.bytes(key))).collect();
        top.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1)));
        top.truncate(k);
        crate::engine::phn(node, "merge_render", t_m);
        let mut kbuild = BytesBuild::new();
        let mut cnts: Vec<i64> = Vec::new();
        for (c, b) in top.iter().skip(node.params.offset) {
            kbuild.push(b);
            cnts.push(*c as i64);
        }
        return AnswerSet::from_cols(vec![
            kbuild.finish(node.ty_of(a_t)),
            AnswerCol::i64s(TypMeta::INT8, cnts),
        ]);
    }
    let t_fill = std::time::Instant::now();
    let (mut runs, side_raw) =
        pm::pair_distinct_partlocal(ctx, a_t, None, a_d, drop_empty);
    crate::engine::phn(node, "scatter_own", t_fill);
    let t_m = std::time::Instant::now();
    // The per-owner (psk, _, distinct) runs ARE the fragments: stage-2
    // hash-combine at fragment grain (drop_empty already applied at fill).
    let mut side_intern = pm::Intern::new(0);
    let mut sf: Vec<pm::Frag> = Vec::new();
    for ((b, _), c) in &side_raw {
        sf.push((side_intern.key(b), 0, *c));
    }
    runs.push(sf);
    let pf = pm::dict_faces(ctx, a_t);
    let kb = pm::KeyBytes { pf: &pf, interns: vec![side_intern.tab.as_slice()] };
    let k = node.params.emit_cap();
    let (cands, _, _) = pm::hash_combine(pool, &runs, &kb, k);
    let mut top: Vec<(u64, &[u8])> = cands.iter().map(|&(c, key, _)| (c, kb.bytes(key))).collect();
    top.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1)));
    top.truncate(k);
    crate::engine::phn(node, "merge_render", t_m);
    let mut kbuild = BytesBuild::new();
    let mut cnts: Vec<i64> = Vec::new();
    for (c, b) in top.iter().skip(node.params.offset) {
        kbuild.push(b);
        cnts.push(*c as i64);
    }
    AnswerSet::from_cols(vec![
        kbuild.finish(node.ty_of(a_t)),
        AnswerCol::i64s(TypMeta::INT8, cnts),
    ])
}

// ---------------------------------------------------------------------------
// grouped pair distinct, sparse packed group: (gid << w_int_bits) |
// int; owner Cnt128 seen-set -> Cnt64 group counts; O(groups) merge.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// [noglobaldict] hot-shape shape without the registry: the pair-distinct engine
// with the int lane folded into pair identity; the group census is TINY
// (sparse packed group), so the string-keyed combine is a plain map fold.
// ---------------------------------------------------------------------------

fn packed_grouped(ctx: &SqeCtx, node: &PlanNode, a_d: u32) -> AnswerSet {
    use crate::stencils::part_merge as pm;
    let bank = ctx.bank;
    let (a_i, a_t) = (node.params.group_cols[0], node.params.group_cols[1]);
    let wi = col_width(bank, a_i);
    let lo_mask = if wi >= 8 { u64::MAX } else { (1u64 << (8 * wi as u32)) - 1 };
    assert!(8 * wi as u32 <= 16, "packed_grouped: int lane must fit the 23-bit aux");
    let drop_empty = node.params.flags & F_DROP_EMPTY_KEY != 0;
    let (runs, side_raw) =
        pm::pair_distinct_partlocal(ctx, a_t, Some((a_i, lo_mask)), a_d, drop_empty);
    // String-keyed combine at group grain (sparse census: a map fold).
    let pf = pm::dict_faces(ctx, a_t);
    type Fx2 = std::hash::BuildHasherDefault<crate::kernels_f6::FxHasher>;
    let mut merged: std::collections::HashMap<(Vec<u8>, u64), u64, Fx2> = Default::default();
    for r in &runs {
        for &(key, a, c) in r {
            let b = pm::face_bytes(&pf, pm::psk_part(key), pm::psk_code(key));
            *merged.entry((b.to_vec(), a)).or_insert(0) += c;
        }
    }
    for ((b, a), c) in side_raw {
        *merged.entry((b, a)).or_insert(0) += c;
    }
    let mut rows: Vec<((Vec<u8>, u64), u64)> = merged.into_iter().collect();
    // (distinct DESC, int-key ASC in its signed domain, key bytes ASC).
    rows.sort_unstable_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| sx(a.0 .1, wi).cmp(&sx(b.0 .1, wi)))
            .then_with(|| a.0 .0.cmp(&b.0 .0))
    });
    let k = node.params.emit_cap();
    rows.truncate(k);
    let mut ints: Vec<i64> = Vec::new();
    let mut kbuild = BytesBuild::new();
    let mut cnts: Vec<i64> = Vec::new();
    for ((b, a), c) in rows.iter().skip(node.params.offset) {
        ints.push(sx(*a, wi));
        kbuild.push(b);
        cnts.push(*c as i64);
    }
    AnswerSet::from_cols(vec![
        AnswerCol::i64s(node.ty_of(a_i), ints),
        kbuild.finish(node.ty_of(a_t)),
        AnswerCol::i64s(TypMeta::INT8, cnts),
    ])
}

// ---------------------------------------------------------------------------
// [sqe-m4] filtered text set: COUNT(DISTINCT varlena) under an int-conjunct
// predicate — the distinct pipeline COMPOSED with the SMA plane (a shape no
// reference-workload query exercises: hot-shape is unfiltered). Per granule the flat SMA
// faces of the predicate columns classify Skip / AllPass / Partial
// (~2 compares per granule per conjunct); AllPass granules fold codes into
// the per-part presence bitmap without touching the predicate lanes;
// Partial granules decode the predicate columns and test row-wise. The
// entry-grain hash-plane union is the hot-shape pipeline unchanged.
// ---------------------------------------------------------------------------

fn filtered_text_set(ctx: &SqeCtx, node: &PlanNode, a_k: u32) -> AnswerSet {
    use crate::fp::entry_fp128;
    use crate::stencils::part_merge as pm;
    let (bank, pool) = (ctx.bank, ctx.pool);
    let pred = node.pred.as_ref().expect("filtered_text_set: pred");
    assert!(pred.var_terms.is_empty(), "var-term distinct is a gap");
    let terms: Vec<PredTerm> = pred.terms.clone();
    let twidths: Vec<u8> = terms.iter().map(|t| col_width(bank, t.col)).collect();
    // standing faces: per-term granule walk + flat SMA, plus the part
    // start offset so (pi, g) indexes align across attnos (granules are
    // row-aligned across columns; asserted via the walk lengths).
    let units = ctx.faces.walk(bank, a_k);
    let smas: Vec<std::sync::Arc<crate::kernels_dec::SmaFlat>> =
        terms.iter().map(|t| ctx.faces.sma(bank, t.col)).collect();
    // [psma-consume] §8.2 candidate-slice faces per conjunct (None under
    // the kill switch / uncovered column — consult degrades).
    let psmas: Vec<_> = terms.iter().map(|t| ctx.faces.psma(bank, t.col)).collect();
    for t in &terms {
        assert_eq!(
            ctx.faces.walk(bank, t.col).len(),
            units.len(),
            "granule walks must align across columns"
        );
    }
    let mut part_start: Vec<usize> = vec![usize::MAX; bank.parts.len()];
    for (i, &(pi, g, _, _)) in units.iter().enumerate() {
        if g == 0 {
            part_start[pi] = i;
        }
    }
    let entries = {
        let n = ctx.faces.stats(bank, a_k).ndv_est_sum() as usize;
        if n > 0 {
            n
        } else {
            ctx.faces
                .dicts_all(bank, a_k)
                .iter()
                .map(|df| df.ncodes as usize)
                .sum::<usize>()
                .max(1 << 16)
        }
    };
    let p = crate::planner::partition_count(entries, 16, node.params.l2_bytes, ctx.pool.threads());
    let pbits = p.trailing_zeros();
    // [fpcache] same cached fp plane as text_set.
    let pf = pm::dict_faces(ctx, a_k);
    let t_fpb = std::time::Instant::now();
    let fps = pm::build_fps_cached(ctx, &pf, a_k);
    crate::engine::phn(node, "fp_build", t_fpb);
    let fpr: &[Vec<u128>] = &fps;
    struct S {
        su: Scratch,
        codes: Vec<u32>,
        pscr: Vec<Scratch>,
        buckets: Vec<Vec<u128>>,
        zskip: u64,
        zall: u64,
        zpart: u64,
    }
    let terms2 = &terms;
    let smas2 = &smas;
    let psmas2 = &psmas;
    let tw = &twidths;
    let pstart = &part_start;
    let pass1 = pool.run_finish(
        bank.parts.len(),
        |_| S {
            su: crate::scan::scratch_fetch(),
            codes: vec![0; 8192],
            pscr: terms2.iter().map(|_| crate::scan::scratch_fetch()).collect(),
            buckets: (0..p).map(|_| Vec::new()).collect(),
            zskip: 0,
            zall: 0,
            zpart: 0,
        },
        |s, pi| {
            let base = pstart[pi];
            let df = ctx.faces.dict(bank, pi, a_k);
            let mut pcurs: Vec<_> =
                terms2.iter().map(|t| crate::scan::open_cursor(bank, pi, t.col)).collect();
            // granule classification via the SMA faces
            #[derive(PartialEq)]
            enum Z {
                Skip,
                All,
                Part,
            }
            let zone_of = |gi: usize| -> Z {
                let mut all = true;
                for (ti, sm) in smas2.iter().enumerate() {
                    let (lo, hi) = (sm.mins[base + gi], sm.maxs[base + gi]);
                    if !terms2[ti].zone_may_pass(lo, hi) {
                        return Z::Skip;
                    }
                    if !terms2[ti].zone_all_pass(lo, hi) {
                        all = false;
                    }
                }
                if all {
                    Z::All
                } else {
                    Z::Part
                }
            };
            if df.dh.is_some() {
                let n = df.ncodes as usize;
                let mut bits = vec![0u64; n.div_ceil(64)];
                let mut cur = crate::scan::open_cursor(bank, pi, a_k);
                for g in 0..cur.granule_count() {
                    let rows = cur.rows_in_granule(g) as usize;
                    match zone_of(g as usize) {
                        Z::Skip => {
                            s.zskip += 1;
                            continue;
                        }
                        Z::All => {
                            s.zall += 1;
                            if s.codes.len() < rows {
                                s.codes.resize(rows, 0);
                            }
                            cur.decode_codes(g, &mut s.codes[..rows]).expect("codes");
                            for &c in &s.codes[..rows] {
                                bits[(c >> 6) as usize] |= 1u64 << (c & 63);
                            }
                        }
                        Z::Part => {
                            // [psma-consume] Zone said maybe: intersect the
                            // conjuncts' candidate slices (one probe per
                            // selective-class conjunct, never per-row); an
                            // empty window skips the granule before any
                            // decode.
                            let win =
                                terms2.iter().enumerate().fold((0usize, rows), |w, (ti, t)| {
                                    crate::psmaface::narrow(
                                        w,
                                        psmas2[ti].as_ref().and_then(|pf| {
                                            pf.slice(
                                                pi,
                                                g,
                                                rows as u32,
                                                smas2[ti].mins[base + g as usize],
                                                smas2[ti].maxs[base + g as usize],
                                                t,
                                            )
                                        }),
                                    )
                                });
                            if win.0 >= win.1 {
                                s.zskip += 1;
                                continue;
                            }
                            s.zpart += 1;
                            if s.codes.len() < rows {
                                s.codes.resize(rows, 0);
                            }
                            cur.decode_codes(g, &mut s.codes[..rows]).expect("codes");
                            let mut pvals: Vec<&[u64]> = Vec::with_capacity(terms2.len());
                            for (ti, pc) in pcurs.iter_mut().enumerate() {
                                let d = s.pscr[ti].decode_full(pc, g, rows);
                                pvals.push(unsafe {
                                    std::slice::from_raw_parts(d.as_ptr(), d.len())
                                });
                            }
                            // [psma-consume, oracle] slice-complement
                            // emptiness gate.
                            #[cfg(feature = "oracle")]
                            for (ti, t) in terms2.iter().enumerate() {
                                if let Some(sl) = psmas2[ti].as_ref().and_then(|pf| {
                                    pf.slice(
                                        pi,
                                        g,
                                        rows as u32,
                                        smas2[ti].mins[base + g as usize],
                                        smas2[ti].maxs[base + g as usize],
                                        t,
                                    )
                                }) {
                                    let (d, w) = (pvals[ti], tw[ti]);
                                    crate::psmaface::oracle_check_complement(
                                        t,
                                        sl.0 as usize,
                                        (sl.1 as usize).min(rows),
                                        rows,
                                        |_| true,
                                        |r| sx(d[r], w),
                                    );
                                }
                            }
                            // Term-major filter (R2: op match at
                            // granule-term grain); the seed selection is the
                            // granule's PSMA window (full when no face
                            // narrowed it) — rows outside never enter.
                            let mut sel: Vec<u16> =
                                (win.0..win.1).map(|r| r as u16).collect();
                            for (ti, t) in terms2.iter().enumerate() {
                                let (d, w) = (pvals[ti], tw[ti]);
                                t.filter_sel(&mut sel, |_| true, |r| sx(d[r], w));
                            }
                            for &r in &sel {
                                let c = s.codes[r as usize];
                                bits[(c >> 6) as usize] |= 1u64 << (c & 63);
                            }
                        }
                    }
                }
                let fpp = &fpr[pi];
                for c in 0..n {
                    if bits[c >> 6] & (1u64 << (c & 63)) != 0 {
                        let h = fpp[c];
                        s.buckets[(h >> (128 - pbits)) as usize].push(h);
                    }
                }
            } else {
                // raw part: decode both lanes, test row-wise, dedupe local.
                let mut cu = crate::scan::open_cursor(bank, pi, a_k);
                let mut local: std::collections::HashSet<Vec<u8>> = Default::default();
                for g in 0..cu.granule_count() {
                    let rows = cu.rows_in_granule(g) as usize;
                    if zone_of(g as usize) == Z::Skip {
                        s.zskip += 1;
                        continue;
                    }
                    // [psma-consume] Zone said maybe: intersect the
                    // conjuncts' candidate slices (one probe per
                    // selective-class conjunct, never per-row); an empty
                    // window skips the granule before any decode.
                    let win = terms2.iter().enumerate().fold((0usize, rows), |w, (ti, t)| {
                        crate::psmaface::narrow(
                            w,
                            psmas2[ti].as_ref().and_then(|pf| {
                                pf.slice(
                                    pi,
                                    g,
                                    rows as u32,
                                    smas2[ti].mins[base + g as usize],
                                    smas2[ti].maxs[base + g as usize],
                                    t,
                                )
                            }),
                        )
                    });
                    if win.0 >= win.1 {
                        s.zskip += 1;
                        continue;
                    }
                    let mut pvals: Vec<Vec<u64>> = Vec::with_capacity(terms2.len());
                    for (ti, pc) in pcurs.iter_mut().enumerate() {
                        pvals.push(s.pscr[ti].decode_full(pc, g, rows).to_vec());
                    }
                    let d = s.su.decode_full(&mut cu, g, rows);
                    // [psma-consume, oracle] slice-complement emptiness gate.
                    #[cfg(feature = "oracle")]
                    for (ti, t) in terms2.iter().enumerate() {
                        if let Some(sl) = psmas2[ti].as_ref().and_then(|pf| {
                            pf.slice(
                                pi,
                                g,
                                rows as u32,
                                smas2[ti].mins[base + g as usize],
                                smas2[ti].maxs[base + g as usize],
                                t,
                            )
                        }) {
                            let (pd, w) = (&pvals[ti][..], tw[ti]);
                            crate::psmaface::oracle_check_complement(
                                t,
                                sl.0 as usize,
                                (sl.1 as usize).min(rows),
                                rows,
                                |_| true,
                                |r| sx(pd[r], w),
                            );
                        }
                    }
                    // Term-major filter (R2: op match at granule-term
                    // grain); the seed selection is the granule's PSMA
                    // window — rows outside never enter.
                    let mut sel: Vec<u16> = (win.0..win.1).map(|r| r as u16).collect();
                    for (ti, t) in terms2.iter().enumerate() {
                        let (pd, w) = (&pvals[ti][..], tw[ti]);
                        t.filter_sel(&mut sel, |_| true, |r| sx(pd[r], w));
                    }
                    for &r in &sel {
                        let pl = unsafe { crate::scan::varlena_payload(d[r as usize]) };
                        if !local.contains(pl) {
                            local.insert(pl.to_vec());
                        }
                    }
                }
                for kk in local {
                    let h = entry_fp128(&kk);
                    s.buckets[(h >> (128 - pbits)) as usize].push(h);
                }
            }
        },
        // Worker-side finish: decode arenas (key + predicate lanes) park
        // on THIS worker's depot; buckets and the zone census cross back.
        |mut s: S| {
            crate::scan::scratch_park(s.su);
            for sc in s.pscr.drain(..) {
                crate::scan::scratch_park(sc);
            }
            (s.buckets, s.zskip, s.zall, s.zpart)
        },
    );
    let (_zs, _za, _zp): (u64, u64, u64) = pass1
        .iter()
        .fold((0, 0, 0), |(a, b, c), s| (a + s.1, b + s.2, c + s.3));
    let owned = pool.run(
        p,
        |_| 0u64,
        |distinct: &mut u64, part| {
            let n: usize = pass1.iter().map(|s| s.0[part].len()).sum();
            let cap = (n * 2).next_power_of_two().max(16);
            let mask = cap - 1;
            let mut th: Vec<u128> = vec![0; cap];
            let mut occ: Vec<bool> = vec![false; cap];
            for s in pass1.iter() {
                for &h in &s.0[part] {
                    let mut i = (h as usize) & mask;
                    loop {
                        if !occ[i] {
                            occ[i] = true;
                            th[i] = h;
                            *distinct += 1;
                            break;
                        }
                        if th[i] == h {
                            break;
                        }
                        i = (i + 1) & mask;
                    }
                }
            }
        },
    );
    let total: u64 = owned.iter().sum();
    pm::park_fps(fps);
    AnswerSet::from_cols(vec![AnswerCol::i64s(TypMeta::INT8, vec![total as i64])])
}
