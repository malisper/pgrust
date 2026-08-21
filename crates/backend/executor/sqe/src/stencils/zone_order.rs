//! zone-order walk / early-bound top-k stencil (ZoneOrderWalk family):
//! `SELECT <val> WHERE <residues> ORDER BY <order> LIMIT k`, the ORDER BY
//! solved by a standing order structure instead of a sort:
//!
//!  - DICT-HEAD walk (order col == the emitted text col): the byte-rank
//!    dictionary IS the order domain — per-part ≤k smallest passing
//!    entries are the only candidates; the row loop counts candidate
//!    codes integer-only, pool-parallel over parts (the hot-shape winner: 96T
//!    beats 48T — survivor granules are DRAM-bound unpack). Raw parts
//!    fall back to a pruned ordered map (per-part encoding election).
//!  - FLAT-SMA ZONE WALK (integer order col): the STANDING SMA face
//!    (faces().sma — built once per bank open, the M0 hot-shape lesson) gives
//!    granule min keys; argmin seed → k-th bound → collect+sort granules
//!    with min ≤ bound → tightening early-exit walk. SERIAL: the
//!    bounded-walk claim law (the ron hot-shape election: survivor work ~1ms;
//!    sma_zone_par admits 6x the granules — negative).
//!
//! `col <> ''` arrives via Params.ne_empty_cols (dict part: excluded
//! code, index-only when byte-rank sorted; raw part: byte test). The
//! secondary sort key (ORDER BY a, b) is OrderBy data: value bytes join
//! the comparison, fetched only at the boundary.
//!
//! NO condcache legs: an early-bound walk visits a handful of granules
//! chosen by the bound — a granule-verdict replay removes no decode the
//! walk still needs (the hot-shape condcache-deviation finding, generalized).

use std::collections::BTreeMap;

use crate::answer::{AnswerCol, AnswerSet, BytesBuild};
use crate::bank::Bank;
use crate::engine::{DictFace, SqeCtx, Unit};
use crate::ir::{OrderBy, PlanNode};
use crate::kernels_f4f5::prune_phrase_map;
use crate::scan::{open_cursor, varlena_payload, CurCache, Scratch};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Bounded sorted winner buffer (k ≈ 10: a sorted Vec beats a heap).
// ---------------------------------------------------------------------------

struct Top<T: Ord + Clone> {
    k: usize,
    v: Vec<T>,
}

impl<T: Ord + Clone> Top<T> {
    fn new(k: usize) -> Top<T> {
        Top { k, v: Vec::with_capacity(k + 1) }
    }
    #[inline(always)]
    fn worst(&self) -> Option<&T> {
        if self.v.len() == self.k {
            self.v.last()
        } else {
            None
        }
    }
    #[inline(always)]
    fn insert(&mut self, cand: T) {
        if self.v.len() == self.k {
            if &cand >= self.v.last().unwrap() {
                return;
            }
            self.v.pop();
        }
        let pos = self.v.binary_search(&cand).unwrap_or_else(|p| p);
        self.v.insert(pos, cand);
    }
}

/// Winner when the ORDER BY is the key alone. Field order = the floor
/// kernel's W24 derive-Ord tuple (key, grow, pi, g, rowin, code) — the
/// tie-break law byte-identity rides on this ordering.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct WKey {
    key: i64,
    grow: u64,
    pi: u32,
    g: u32,
    rowin: u16,
    /// Dict code of the value (u32::MAX = raw part: hydrate via decode_sel).
    code: u32,
}

/// Winner when the ORDER BY is (key, value bytes) — the floor's W26 law.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct WKeyVal {
    key: i64,
    bytes: Vec<u8>,
    grow: u64,
}

// ---------------------------------------------------------------------------
// value-column predicate plan per part (ne_empty residue)
// ---------------------------------------------------------------------------

/// The excluded dict code for `<> ''` (u32::MAX = none). Byte-rank
/// sorted dicts answer index-only ('' can only be code 0 — the standing
/// DictFace carries that probe); unsorted dicts scan entry lengths once.
fn empty_excl(face: &DictFace, ne_empty: bool) -> u32 {
    if !ne_empty {
        return u32::MAX;
    }
    if let Some(c) = face.empty_code {
        return c;
    }
    if let Some(dh) = &face.dh {
        if !dh.byte_rank_sorted() {
            for c in 0..face.ncodes {
                if dh.byte_len_only(c).expect("dict len") == 0 {
                    return c;
                }
            }
        }
    }
    u32::MAX
}

// ---------------------------------------------------------------------------
// flat-SMA zone walk (integer order key), serial
// ---------------------------------------------------------------------------

/// Argmin granule over the flat mins (ties → lowest id; the i64::MIN
/// sentinel of non-exact granules sorts first = must-visit).
#[inline(always)]
fn argmin(mins: &[i64]) -> usize {
    let mut best = 0usize;
    let mut bv = i64::MAX;
    for (i, &m) in mins.iter().enumerate() {
        if m < bv {
            bv = m;
            best = i;
        }
    }
    best
}

/// Every other granule with min ≤ bound, ascending by (min, id).
fn candidates(mins: &[i64], seed: usize, bound: i64) -> Vec<(i64, u32)> {
    let mut c: Vec<(i64, u32)> = Vec::new();
    for (i, &m) in mins.iter().enumerate() {
        if m <= bound && i != seed {
            c.push((m, i as u32));
        }
    }
    c.sort_unstable();
    c
}

/// Serial walker state (persistent across visits within one execution).
struct Walker {
    val: u32,
    ne_empty: bool,
    so: Scratch,
    sv: Scratch,
    co: CurCache,
    cv: CurCache,
    codes: Vec<u32>,
    pass: Vec<u32>,
    faces: Vec<Option<Arc<DictFace>>>,
    excl: Vec<u32>,
}

impl Walker {
    /// Depot-riding constructor (the scratch-init discipline): decode
    /// arenas come reset from the calling thread's depot; cursors and
    /// dict-face caches are per-engagement — never parked.
    fn fetch(nparts: usize, order: u32, val: u32, ne_empty: bool) -> Walker {
        Walker {
            val,
            ne_empty,
            so: crate::scan::scratch_fetch(),
            sv: crate::scan::scratch_fetch(),
            co: CurCache::new(order),
            cv: CurCache::new(val),
            codes: vec![0; 8192],
            pass: Vec::with_capacity(256),
            faces: (0..nparts).map(|_| None).collect(),
            excl: vec![u32::MAX; nparts],
        }
    }

    /// Park the decode arenas back on the calling thread's depot;
    /// everything else drops.
    fn park(self) {
        crate::scan::scratch_park(self.so);
        crate::scan::scratch_park(self.sv);
    }

    /// Visit one granule; `by_val` = the ORDER BY carries the value bytes
    /// as a secondary key (OrderBy data, not query identity).
    fn visit(
        &mut self,
        bank: &Bank,
        faces: &crate::engine::Faces,
        units: &[Unit],
        ui: usize,
        by_val: bool,
        top_key: &mut Top<WKey>,
        top_kv: &mut Top<WKeyVal>,
    ) {
        let (pi, g, rows32, base) = units[ui];
        let rows = rows32 as usize;
        if self.faces[pi].is_none() {
            let f = faces.dict(bank, pi, self.val);
            self.excl[pi] = empty_excl(&f, self.ne_empty);
            self.faces[pi] = Some(f);
        }
        let face = self.faces[pi].as_ref().unwrap().clone();
        let keys = self.so.decode_full(self.co.get(bank, pi), g, rows);
        let mut bound = if by_val {
            top_kv.worst().map(|w| w.key).unwrap_or(i64::MAX)
        } else {
            top_key.worst().map(|w| w.key).unwrap_or(i64::MAX)
        };
        match &face.dh {
            Some(dh) => {
                // Key-first ordinal gather: the codes lane is decoded only
                // when a row beats the bound (the hot-shape pred_order lever).
                self.pass.clear();
                for (r, &e) in keys.iter().enumerate() {
                    if (e as i64) <= bound {
                        self.pass.push(r as u32);
                    }
                }
                if self.pass.is_empty() {
                    return;
                }
                if self.codes.len() < rows {
                    self.codes.resize(rows, 0);
                }
                self.cv
                    .get(bank, pi)
                    .decode_codes(g, &mut self.codes[..rows])
                    .expect("codes");
                let e = self.excl[pi];
                let codes = &self.codes[..rows];
                for &r32 in &self.pass {
                    let r = r32 as usize;
                    let kv = keys[r] as i64;
                    // Re-check: the bound tightens as insertions land.
                    if kv > bound || codes[r] == e {
                        continue;
                    }
                    if by_val {
                        let b = dh.entry(codes[r]).expect("dict entry").bytes;
                        top_kv.insert(WKeyVal { key: kv, bytes: b.to_vec(), grow: base + r as u64 });
                        bound = top_kv.worst().map(|w| w.key).unwrap_or(i64::MAX);
                    } else {
                        top_key.insert(WKey {
                            key: kv,
                            grow: base + r as u64,
                            pi: pi as u32,
                            g,
                            rowin: r as u16,
                            code: codes[r],
                        });
                        bound = top_key.worst().map(|w| w.key).unwrap_or(i64::MAX);
                    }
                }
            }
            None => {
                let d = self.sv.decode_full(self.cv.get(bank, pi), g, rows);
                for (r, &x) in d.iter().enumerate() {
                    let kv = keys[r] as i64;
                    if kv > bound {
                        continue;
                    }
                    let b = unsafe { varlena_payload(x) };
                    if self.ne_empty && b.is_empty() {
                        continue;
                    }
                    if by_val {
                        top_kv.insert(WKeyVal { key: kv, bytes: b.to_vec(), grow: base + r as u64 });
                        bound = top_kv.worst().map(|w| w.key).unwrap_or(i64::MAX);
                    } else {
                        top_key.insert(WKey {
                            key: kv,
                            grow: base + r as u64,
                            pi: pi as u32,
                            g,
                            rowin: r as u16,
                            code: u32::MAX,
                        });
                        bound = top_key.worst().map(|w| w.key).unwrap_or(i64::MAX);
                    }
                }
            }
        }
    }
}

/// Hydrate key-only winners: dict winners resolve their code (no row
/// decode — "hydration = 10 dict-entry lookups"); raw winners decode_sel
/// one row. Render "{key}\t{esc(bytes)}" = the floor's hydrate24 law.
fn hydrate_key(ctx: &SqeCtx, node: &PlanNode, order: u32, val: u32, ws: &[WKey]) -> AnswerSet {
    let bank = ctx.bank;
    let mut keys: Vec<i64> = Vec::with_capacity(ws.len());
    let mut vals = BytesBuild::new();
    let mut s: Option<Scratch> = None;
    for w in ws {
        keys.push(w.key);
        if w.code != u32::MAX {
            let face = ctx.faces.dict(bank, w.pi as usize, val);
            let dh = face.dh.as_ref().expect("winner code from dict part");
            vals.push(dh.entry(w.code).expect("dict entry").bytes);
        } else {
            let sc = s.get_or_insert_with(crate::scan::scratch_fetch);
            let mut cur = open_cursor(bank, w.pi as usize, val);
            let d = sc.decode_sel(&mut cur, w.g, &[w.rowin]);
            vals.push(unsafe { varlena_payload(d[0]) });
        }
    }
    if let Some(sc) = s {
        crate::scan::scratch_park(sc);
    }
    let _ = node;
    AnswerSet::from_cols(vec![
        AnswerCol::i64s(bank.typ(order), keys),
        vals.finish(bank.typ(val)),
    ])
}

fn zone_walk(
    ctx: &SqeCtx,
    node: &PlanNode,
    order: u32,
    by_val: bool,
    ne_empty: bool,
) -> AnswerSet {
    let (bank, pool) = (ctx.bank, ctx.pool);
    let val = node.cols[0];
    let k = node.params.limit;
    // Standing faces: granule walk + flat SMA, built once per query run
    // (never inside the timed region — the M0 hot-shape stats-face lesson).
    let units = ctx.faces.walk(bank, order);
    let sma = ctx.faces.sma(bank, order);
    let mins = &sma.mins;
    let nparts = bank.parts.len();
    let mut w = Walker::fetch(nparts, order, val, ne_empty);
    let mut top_key = Top::<WKey>::new(k);
    let mut top_kv = Top::<WKeyVal>::new(k);
    let seed = argmin(mins);
    w.visit(bank, ctx.faces, &units, seed, by_val, &mut top_key, &mut top_kv);
    let bound = if by_val {
        top_kv.worst().map(|x| x.key).unwrap_or(i64::MAX)
    } else {
        top_key.worst().map(|x| x.key).unwrap_or(i64::MAX)
    };
    let cands = candidates(mins, seed, bound);
    // [sqe-m4] FALLBACK ELECTION: on an order column UNCORRELATED with
    // bank order the k-th bound erases nothing — the candidate set is
    // ~every granule and the "early-bound" walk degenerates to a serial
    // full scan. The §1.6 2D threshold (survivor work in rows vs the
    // measured serial cutoff band) elects the pool-parallel scan top-k
    // instead; per-worker k-slot buffers under the same (key, grow) total
    // order merge exactly. A function of SMA candidate stats + the serial
    // law — never of the query.
    let cand_rows: u64 = cands.iter().map(|&(_, ui)| units[ui as usize].2 as u64).sum();
    let t = crate::planner::elect_threads(
        crate::ir::ClaimClass::SkipDominated,
        cands.len(),
        cand_rows,
        pool.threads(),
    );
    if crate::engine::phase_on() {
        println!(
            "SQEELECT|q={}|zone_walk|cands={}/{}|cand_rows={}|elected={}",
            node.q,
            cands.len(),
            units.len(),
            cand_rows,
            if t <= 1 { "serial_bound_walk" } else { "parallel_scan_topk" }
        );
    }
    if t > 1 {
        w.park();
        return scan_topk(ctx, node, order, by_val, ne_empty);
    }
    for &(m, ui) in &cands {
        let cur_bound = if by_val {
            top_kv.worst().map(|x| x.key)
        } else {
            top_key.worst().map(|x| x.key)
        };
        if let Some(b) = cur_bound {
            if b < m {
                break;
            }
        }
        w.visit(bank, ctx.faces, &units, ui as usize, by_val, &mut top_key, &mut top_kv);
    }
    w.park();
    if by_val {
        emit_keyval(ctx, node, order, val, &top_kv.v)
    } else {
        hydrate_key(ctx, node, order, val, &top_key.v)
    }
}

/// Typed emit for the (key, value-bytes) winners.
fn emit_keyval(ctx: &SqeCtx, node: &PlanNode, order: u32, val: u32, ws: &[WKeyVal]) -> AnswerSet {
    let _ = node;
    let mut keys: Vec<i64> = Vec::with_capacity(ws.len());
    let mut vals = BytesBuild::new();
    for x in ws {
        keys.push(x.key);
        vals.push(&x.bytes);
    }
    AnswerSet::from_cols(vec![
        AnswerCol::i64s(ctx.bank.typ(order), keys),
        vals.finish(ctx.bank.typ(val)),
    ])
}

/// [sqe-m4] Pool-parallel scan top-k: every granule visited, per-worker
/// Walker + k-slot buffers under the SAME (key, grow, …) total order as
/// the serial walk (the tie law rides the WKey/WKeyVal Ord), merged by
/// reinsertion — winners are identical to the serial walk's by the total
/// order, whatever the visit schedule. Zone skipping still applies per
/// worker via the tightening per-worker bound.
fn scan_topk(
    ctx: &SqeCtx,
    node: &PlanNode,
    order: u32,
    by_val: bool,
    ne_empty: bool,
) -> AnswerSet {
    let (bank, pool) = (ctx.bank, ctx.pool);
    let val = node.cols[0];
    let k = node.params.limit;
    let units = ctx.faces.walk(bank, order);
    let nparts = bank.parts.len();
    let units2 = &units;
    let states = pool.run_finish(
        units.len(),
        |_| {
            (
                Walker::fetch(nparts, order, val, ne_empty),
                Top::<WKey>::new(k),
                Top::<WKeyVal>::new(k),
            )
        },
        |(w, tk, tkv), ui| {
            w.visit(bank, ctx.faces, units2, ui, by_val, tk, tkv);
        },
        // Worker-side finish: the walker's arenas park on THIS worker's
        // depot; only the k-slot winners cross back.
        |(w, tk, tkv)| {
            w.park();
            (tk, tkv)
        },
    );
    let mut top_key = Top::<WKey>::new(k);
    let mut top_kv = Top::<WKeyVal>::new(k);
    for (tk, tkv) in states {
        for x in tk.v {
            top_key.insert(x);
        }
        for x in tkv.v {
            top_kv.insert(x);
        }
    }
    if by_val {
        emit_keyval(ctx, node, order, val, &top_kv.v)
    } else {
        hydrate_key(ctx, node, order, val, &top_key.v)
    }
}

// ---------------------------------------------------------------------------
// dict-head walk (order column IS the emitted text column), pool-parallel
// ---------------------------------------------------------------------------

fn dict_head(ctx: &SqeCtx, node: &PlanNode, ne_empty: bool) -> AnswerSet {
    let (bank, pool) = (ctx.bank, ctx.pool);
    let val = node.cols[0];
    let k = node.params.limit as u64;
    let states = pool.run_finish(
        bank.parts.len(),
        |_| (BTreeMap::<Vec<u8>, u64>::new(), crate::scan::scratch_fetch(), vec![0u32; 8192]),
        |(m, sp, codes), pi| {
            // Per-part election (a function of the part's encoding): the
            // dict plane solves string order; raw parts scan into the map.
            let face = ctx.faces.dict(bank, pi, val);
            if let Some(dh) = &face.dh {
                let n = face.ncodes;
                let excl = empty_excl(&face, ne_empty);
                // ≤k smallest passing entries per part = the candidates.
                let mut cand: Vec<(Vec<u8>, u32)> = Vec::new();
                for c in 0..n {
                    if c == excl {
                        continue;
                    }
                    let b = dh.entry(c).expect("dict entry").bytes;
                    if ne_empty && b.is_empty() {
                        continue;
                    }
                    if cand.len() == k as usize && b >= cand.last().unwrap().0.as_slice() {
                        continue;
                    }
                    let pos = cand
                        .binary_search_by(|(kb, _)| kb.as_slice().cmp(b))
                        .unwrap_or_else(|p| p);
                    cand.insert(pos, (b.to_vec(), c));
                    cand.truncate(k as usize);
                }
                let mut is_cand = vec![false; n as usize];
                for (_, c) in &cand {
                    is_cand[*c as usize] = true;
                }
                let mut counts = vec![0u64; cand.len().max(1)];
                let mut cur = open_cursor(bank, pi, val);
                for g in 0..cur.granule_count() {
                    let rows = cur.rows_in_granule(g) as usize;
                    if codes.len() < rows {
                        codes.resize(rows, 0);
                    }
                    cur.decode_codes(g, &mut codes[..rows]).expect("codes");
                    for r in 0..rows {
                        let c = codes[r];
                        if is_cand[c as usize] {
                            let ci = cand.iter().position(|(_, cc)| *cc == c).unwrap();
                            counts[ci] += 1;
                        }
                    }
                }
                for (ci, (kb, _)) in cand.iter().enumerate() {
                    if counts[ci] > 0 {
                        *m.entry(kb.clone()).or_insert(0) += counts[ci];
                    }
                }
            } else {
                let mut cur = open_cursor(bank, pi, val);
                for g in 0..cur.granule_count() {
                    let rows = cur.rows_in_granule(g) as usize;
                    let d = sp.decode_full(&mut cur, g, rows);
                    for &x in d {
                        let b = unsafe { varlena_payload(x) };
                        if !(ne_empty && b.is_empty()) {
                            *m.entry(b.to_vec()).or_insert(0) += 1;
                        }
                    }
                }
            }
            prune_phrase_map(m, k);
        },
        |s| {
            crate::scan::scratch_park(s.1);
            s.0
        },
    );
    let mut m: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
    for pm in states {
        for (kb, c) in pm {
            *m.entry(kb).or_insert(0) += c;
        }
    }
    prune_phrase_map(&mut m, k);
    // Typed emit of the phrase-map window (the phrase_map_emit render
    // site, killed): one row per surviving instance up to k.
    let mut vals = BytesBuild::new();
    let mut left = k;
    'outer: for (kb, &c) in m.iter() {
        for _ in 0..c.min(left) {
            vals.push(kb);
            left -= 1;
            if left == 0 {
                break 'outer;
            }
        }
    }
    AnswerSet::from_cols(vec![vals.finish(bank.typ(val))])
}

// ---------------------------------------------------------------------------
// stencil entry (contract free function)
// ---------------------------------------------------------------------------

pub fn run_zone_order_walk(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    let val = node.cols[0];
    let ne_empty = node.params.ne_empty_cols.contains(&val);
    let (order, by_val) = match node.params.order {
        OrderBy::ColAsc(c) => (c, false),
        OrderBy::ColThenColAsc(a, b) => {
            assert_eq!(b, val, "secondary order key must be the emitted column");
            (a, true)
        }
        other => panic!("zone_order_walk: unsupported order {other:?}"),
    };
    if order == val {
        // ELECTION: ordering by the emitted column itself ⇒ the byte-rank
        // dictionary (where published) IS the order domain — dict-head,
        // pool-parallel over parts (the hot-shape claim finding: full width).
        dict_head(ctx, node, ne_empty)
    } else {
        // Integer order key ⇒ standing-SMA zone-order early-bound walk,
        // SERIAL when the bound prunes (bounded-walk claim law; parallel
        // admission is negative on clustered keys), pool-parallel scan
        // top-k when the SMA candidate census proves the bound is inert
        // (uncorrelated order columns — the sqe-m4 fallback election).
        zone_walk(ctx, node, order, by_val, ne_empty)
    }
}
