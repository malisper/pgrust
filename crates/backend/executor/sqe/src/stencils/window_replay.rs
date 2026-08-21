//! WINDOW-REPLAY stencil: the condition-cache
//! family. Two lanes, both fully parametric (no query identity anywhere):
//!
//!   VAR lane (PredSpec.var_terms non-empty — the verdict-bitmap shapes):
//!   entry-grain dict verdict bitwords for the first varlena conjunct
//!   (VerdictWords; honest arm recomputes them, elected cold record uses
//!   the standing face), fused fold_ptr code scan, later varlena conjuncts
//!   staged via decode_sel row bytes. The FINAL survivor plane is
//!   published to the ENGINE condition cache under the canonical frame
//!   fingerprint (hot-shape share "c14:contains:google"); hot-shape plans
//!   also publish a CODE SIDECAR (survivor codes of the driving column,
//!   u32::MAX = not dict-resolved) so MIN(url) replays from dict entry
//!   bytes without re-touching the URL lane.
//!
//!   FRAME lane (int-only predicates — hot-shape/40/41): the (CounterID =,
//!   EventDate BETWEEN) frame pair consumed through the shared-frame
//!   standing memo; remaining conjuncts are residues. The published plane
//!   is the FRAME rowlists (the "hot-shape share one entry" law); replay
//!   evaluates residues + keys with decode_sel over plane survivors.
//!
//! Consume epilogue by node shape: scalar count / grouped text key with
//! MinBytes-CountStar-CountDistinct in agg order / row top-k by an order
//! column + SELECT * hydrate (+ "-- predicate matches" trailer) / packed
//! int-or-text frame keys with the rank window and "-- groups= rows="
//! trailer. All render laws are byte-copies of the elected kernels
//! (q21_render / q22_render / q23_push4 / kernels_f6 run_spec).
//!
//! [q45 partmerge] The var-lane Grouped COMBINE (render_var) carries a
//! partition law (q45-highndv-decomposition.md §5/§6): worker maps
//! scatter by pure key hash to P owners (E7 sizing, E2-form measured
//! floors), plus an element-grain arm for small-G/large-distinct-set
//! shapes (q2-class). `PGRUST_SQE_GROUP_PARTMERGE=0` restores the
//! serial combine.

use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;

use crate::answer::{AnswerCol, AnswerSet, BytesBuild, ColData, Validity};
use crate::bank::Bank;
use crate::engine::{CVerdict, CacheSidecar, SqeCtx, Unit};
use crate::exec::replay_cache;
use crate::ir::*;
use crate::kernels::{hydrate_winners, Winner};
use crate::kernels_dec::{FcCache, VerdictWords};
use crate::kernels_f6::shared_frame;
use crate::planner::elect_threads;
use crate::scan::{varlena_payload, CurCache, Scratch};
use crate::stencils::{col_width, sx};
use crate::typmeta::TypMeta;

// ---------------------------------------------------------------------------
// small shared machinery
// ---------------------------------------------------------------------------

type FxMap = HashMap<Vec<u8>, u64, BuildHasherDefault<crate::kernels_f6::FxHasher>>;
type FxSet64 = std::collections::HashSet<u64, BuildHasherDefault<crate::kernels_f6::FxHasher>>;
/// [json-rung2] Varlena distinct sets: the 128-bit entry fingerprint
/// (the text128 grouping identity) per distinct payload.
type FxSet128 = std::collections::HashSet<u128, BuildHasherDefault<crate::kernels_f6::FxHasher>>;
type FxGMap = HashMap<Vec<u8>, GAcc, BuildHasherDefault<crate::kernels_f6::FxHasher>>;

// ---------------------------------------------------------------------------
// [spill-5] the VAR-lane fp128 distinct plane's memory law
// ---------------------------------------------------------------------------

/// Charged bytes per resident fp128 entry — one authority, both laws.
pub const DSET128_ENTRY_BYTES: u64 = 32;

pub static WRSPILL_SCATTERS: AtomicU64 = AtomicU64::new(0);
pub static WRSPILL_MERGES: AtomicU64 = AtomicU64::new(0);

// ---------------------------------------------------------------------------
// [q45 partmerge] partition-parallel grouped combine
// (docs/design/sqe/q45-highndv-decomposition.md §5/§6 — the E-gap: the
// var-lane Grouped combine had no election or partitioning law)
// ---------------------------------------------------------------------------

/// Census witnesses (the WRSPILL idiom: pub statics the rig and tests
/// read; last engagement wins the `store`d rows, `_ENGAGED` counts).
/// `dedup_factor` is derived: `PARTMERGE_ENTRIES_IN / PARTMERGE_GROUPS_OUT`
/// of the same engagement (q4 of record: ~6m / 1.23m = 4.9x). Walls ride
/// both the statics (ns) and the SQEPHASE plane (`engine::phn` rows
/// `pm_scatter` / `pm_owner` / `pm_setscatter` / `pm_setowner` /
/// `pm_bound`).
pub static PARTMERGE_ENGAGED: AtomicU64 = AtomicU64::new(0);
pub static PARTMERGE_PARTITIONS: AtomicU64 = AtomicU64::new(0);
pub static PARTMERGE_ENTRIES_IN: AtomicU64 = AtomicU64::new(0);
pub static PARTMERGE_GROUPS_OUT: AtomicU64 = AtomicU64::new(0);
pub static PARTMERGE_SCATTER_NS: AtomicU64 = AtomicU64::new(0);
pub static PARTMERGE_OWNER_NS: AtomicU64 = AtomicU64::new(0);
pub static PARTMERGE_BOUND_NS: AtomicU64 = AtomicU64::new(0);
pub static PARTMERGE_SET_ENGAGED: AtomicU64 = AtomicU64::new(0);
pub static PARTMERGE_SET_SHARDS: AtomicU64 = AtomicU64::new(0);
pub static PARTMERGE_SET_ELEMS_IN: AtomicU64 = AtomicU64::new(0);
pub static PARTMERGE_SETS_NS: AtomicU64 = AtomicU64::new(0);

/// Kill switch (A/B + born-red hygiene): `PGRUST_SQE_GROUP_PARTMERGE=0`
/// restores the serial render_var combine exactly. Default ON.
fn partmerge_on() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_SQE_GROUP_PARTMERGE").map_or(true, |v| v != "0"))
}

/// Pure key-bytes shard (determinism law: a function of the key bytes
/// and P only — answer order is shard-independent). Bits 40.. of the
/// fold hash, disjoint from the bucket-index bits the FxGMap probe
/// consumes (hashbrown: low bits + top-7 tag), so one map bucket chain
/// never pins a whole shard.
#[inline]
fn pm_shard(key: &[u8], p: usize) -> usize {
    use std::hash::Hasher;
    debug_assert!(p.is_power_of_two());
    let mut h = crate::kernels_f6::FxHasher::default();
    h.write(key);
    (h.finish() >> 40) as usize & (p - 1)
}

/// Element shard for the distinct-set grain — same law over the
/// element's LE bytes (raw u64 ids are not uniform; fp128 values are,
/// but one law keeps the shard fn pure and family-blind).
#[inline]
fn pm_shard64(x: u64, p: usize) -> usize {
    pm_shard(&x.to_le_bytes(), p)
}

#[inline]
fn pm_shard128(x: u128, p: usize) -> usize {
    pm_shard(&x.to_le_bytes(), p)
}

/// [q45 §5] E2-form election floor at COMBINE grain. The scatter→owner
/// combine pays two pool generations (2·setup(w), the E2 claim-plane
/// law) and touches every entry twice (scatter move + owner merge); the
/// serial combine touches each once at c_merge. Serial iff
/// `E·c < 2E·c/w + 2·setup(w)` ⇒ `E* = 2·setup(w)·w / (c·(w−2))`.
/// c_merge = 350 ns/entry — the q45 doc's measured band (§3: 32B hash +
/// 2–3 dependent DRAM misses + GAcc merge). At w=64: E* ≈ 12k entries,
/// so q3 (E = 64×72) and every q1-class shape stay serial while q4/q5
/// (E ≈ 6m) partition. The partition COUNT then comes from the E7 law
/// (`planner::partition_count`, floor/cap per cost_params).
fn pm_cutoff_entries(width: usize) -> u64 {
    const MERGE_ENTRY_NS: u64 = 350;
    if width <= 2 {
        return u64::MAX;
    }
    let setup = crate::cost_params::target().claim_setup_ns(width) as u128;
    let w = width as u128;
    u64::try_from(2 * setup * w / (MERGE_ENTRY_NS as u128 * (w - 2))).unwrap_or(u64::MAX)
}

/// The same E2 form at SET-ELEMENT grain (the q2-class distinct-set
/// unions: small G, million-entry per-group sets). c_union = 100
/// ns/element (set probe + insert into the growing global set). At
/// w=64: S* ≈ 42k elements.
fn pm_cutoff_set_elems(width: usize) -> u64 {
    const UNION_ELEM_NS: u64 = 100;
    if width <= 2 {
        return u64::MAX;
    }
    let setup = crate::cost_params::target().claim_setup_ns(width) as u128;
    let w = width as u128;
    u64::try_from(2 * setup * w / (UNION_ELEM_NS as u128 * (w - 2))).unwrap_or(u64::MAX)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PmArm {
    Serial,
    Group,
    Sets,
}

/// Combine-arm election over MEASURED sizes (the doc's ruling: "E_est
/// from the per-worker map sizes — measured, not assumed"): `entries` =
/// Σ worker map lens (O(nstates)); `set_elems` is consulted only under
/// the entry floor, where the walk is O(E) over a small E. Spilled
/// engagements stay serial: the spill-run merge keys into the single
/// resident map and its wall is IO-bound anyway.
fn pm_elect(
    on: bool,
    nstates: usize,
    width: usize,
    has_spill: bool,
    entries: u64,
    set_elems: impl FnOnce() -> u64,
) -> PmArm {
    if !on || has_spill || nstates <= 1 || width <= 2 {
        return PmArm::Serial;
    }
    if entries >= pm_cutoff_entries(width) {
        return PmArm::Group;
    }
    if set_elems() >= pm_cutoff_set_elems(width) {
        return PmArm::Sets;
    }
    PmArm::Serial
}

/// Grouped VAR shape with a varlena COUNT(DISTINCT) leg (one authority).
pub fn distinct_fp_serves(bank: &Bank, node: &PlanNode) -> bool {
    node.pred.as_ref().is_some_and(|p| !p.var_terms.is_empty())
        && matches!(shape_of(node), Shape::Grouped { .. })
        && node.agg.iter().any(|a| {
            matches!(a.op, AggOp::CountDistinct)
                && a.col.is_some_and(|c| col_width(bank, c) == 0)
        })
}

struct WrSpill {
    store: Arc<dyn crate::spill::SpillStore>,
    wid: AtomicUsize,
}

/// Per-worker meters vs the E18b share: armed workers drain-at-share;
/// unarmed, `over` latches — freeze, then typed refusal at finalize.
struct WrGauge {
    share: u64,
    bytes: AtomicU64,
    over: AtomicBool,
    spill: Option<WrSpill>,
}

fn wr_gauge(ctx: &SqeCtx, node: &PlanNode) -> Option<Arc<WrGauge>> {
    if !distinct_fp_serves(ctx.bank, node) {
        return None;
    }
    let cfg = &ctx.faces.cfg;
    let share = (cfg.grouped_budget_bytes() / ctx.pool.threads().max(1) as u64).max(4096);
    let spill = cfg
        .spill
        .then(crate::spill::new_store)
        .flatten()
        .map(|store| WrSpill { store, wid: AtomicUsize::new(0) });
    Some(Arc::new(WrGauge { share, bytes: AtomicU64::new(0), over: AtomicBool::new(false), spill }))
}

/// Per-(group, leg) sorted 16 B fp128 runs; identity rides the registry.
fn drain_wr_sets(s: &mut WState, g: &WrGauge) {
    if s.dres == 0 {
        return;
    }
    WRSPILL_SCATTERS.fetch_add(1, Relaxed);
    let sp = g.spill.as_ref().expect("drain implies the armed store");
    let bw = s.dspill.get_or_insert_with(|| {
        let w = sp.wid.fetch_add(1, Relaxed);
        crate::stencils::hash_group::BW::new(&*sp.store, "wrdset-runs", w)
    });
    let druns = &mut s.druns;
    let mut drained = 0u64;
    let mut one = |skey: &[u8], acc: &mut GAcc| {
        for (ui, set) in acc.tusers.iter_mut().enumerate() {
            if set.is_empty() {
                continue;
            }
            let mut v: Vec<u128> = std::mem::take(set).into_iter().collect();
            v.sort_unstable();
            bw.begin();
            for e in &v {
                bw.push(&e.to_ne_bytes());
            }
            let (off, _len) = bw.end();
            druns.push((skey.to_vec(), ui as u8, off, v.len() as u64));
            drained += v.len() as u64;
        }
    };
    let mut kb: Vec<u8> = Vec::with_capacity(64);
    for (k, acc) in s.gmap.iter_mut() {
        kb.clear();
        kb.push(0);
        kb.extend_from_slice(k);
        one(&kb, acc);
    }
    one(&[1], &mut s.gnull);
    for (b, acc) in s.gnullb.iter_mut().enumerate() {
        one(&[2, b as u8], acc);
    }
    s.dres = 0;
    g.bytes.fetch_sub(drained * DSET128_ENTRY_BYTES, Relaxed);
}

/// K-way dedupe-merge of one (group, leg) — the jdset finalize law.
fn merge_wr_runs(
    media: &[crate::stencils::hash_group::BW],
    runs: &[(Vec<u8>, u8, usize, u64, u64)],
    resident: FxSet128,
    share: u64,
) -> i64 {
    WRSPILL_MERGES.fetch_add(1, Relaxed);
    let mut mem: Vec<u128> = resident.into_iter().collect();
    mem.sort_unstable();
    let nrun = runs.len();
    let slab = ((share as usize) / (nrun + 1)).clamp(16, crate::spill::SLAB_BYTES);
    let mut curs: Vec<crate::spill::ChunkCursor> = runs
        .iter()
        .map(|r| crate::spill::ChunkCursor::new(&*media[r.2].m, r.3, r.4, 16, slab))
        .collect();
    let rec = |b: &[u8]| u128::from_ne_bytes(b.try_into().expect("16 B fp128 record"));
    let mut heap: std::collections::BinaryHeap<std::cmp::Reverse<(u128, usize)>> =
        std::collections::BinaryHeap::with_capacity(nrun + 1);
    for (si, c) in curs.iter_mut().enumerate() {
        if let Some(b) = c.next() {
            heap.push(std::cmp::Reverse((rec(b), si)));
        }
    }
    let mut mi = 0usize;
    if mi < mem.len() {
        heap.push(std::cmp::Reverse((mem[mi], nrun)));
        mi += 1;
    }
    let mut n = 0i64;
    while let Some(std::cmp::Reverse((e, src))) = heap.pop() {
        let mut adv = |heap: &mut std::collections::BinaryHeap<std::cmp::Reverse<(u128, usize)>>,
                       s: usize| {
            if s < nrun {
                if let Some(b) = curs[s].next() {
                    heap.push(std::cmp::Reverse((rec(b), s)));
                }
            } else if mi < mem.len() {
                heap.push(std::cmp::Reverse((mem[mi], nrun)));
                mi += 1;
            }
        };
        adv(&mut heap, src);
        while let Some(&std::cmp::Reverse((e2, s2))) = heap.peek() {
            if e2 != e {
                break;
            }
            heap.pop();
            adv(&mut heap, s2);
        }
        n += 1;
    }
    n
}

/// The finalize-merged spilled count, else the resident sets.
fn dcount(g: &GAcc, ui: usize) -> i64 {
    match g.tcnt.get(ui).copied().flatten() {
        Some(c) => c,
        None => (g.users[ui].len() + g.tusers[ui].len()) as i64,
    }
}

#[inline(always)]
fn bitw(w: &[u64], code: u32) -> bool {
    (w[(code >> 6) as usize] >> (code & 63)) & 1 != 0
}

fn topk_insert(w: &mut Vec<Winner>, cand: Winner, k: usize) {
    if w.len() < k {
        w.push(cand);
        w.sort();
    } else if cand < w[k - 1] {
        w[k - 1] = cand;
        w.sort();
    }
}

/// Survivor row ordinals of one plane verdict (AllPass expands).
fn verdict_rows<'a>(v: &'a CVerdict, rows: u32, buf: &'a mut Vec<u16>) -> Option<&'a [u16]> {
    match v {
        CVerdict::Skip => None,
        CVerdict::AllPass => {
            buf.clear();
            buf.extend(0..rows as u16);
            Some(buf)
        }
        CVerdict::Rows(r) => Some(r),
        CVerdict::Bitmap(w) => {
            buf.clear();
            for r in 0..rows as usize {
                if w[r >> 6] >> (r & 63) & 1 != 0 {
                    buf.push(r as u16);
                }
            }
            Some(buf)
        }
    }
}

// ---------------------------------------------------------------------------
// shapes (derived from node content)
// ---------------------------------------------------------------------------

enum Shape {
    Count,
    /// GROUP BY one varlena key (+ an hour bucket); aggs in node.agg order.
    Grouped { key: u32, bucket: Option<Bucket> },
    /// ORDER BY <order> ASC LIMIT k over survivor rows, SELECT * hydrate.
    RowTopK { order: u32 },
    /// Packed multi-field key, COUNT(*), rank window with offset.
    FrameGroup,
}

/// The hour-bucket key beside the text key (`first` = answer column 0).
#[derive(Clone, Copy)]
struct Bucket {
    col: u32,
    off_s: i64,
    first: bool,
}

fn shape_of(node: &PlanNode) -> Shape {
    let has_var = node.pred.as_ref().is_some_and(|p| !p.var_terms.is_empty());
    if has_var {
        match node.params.key_exprs.as_slice() {
            [KeyExpr::Col(t), KeyExpr::HourBucket { col, off_s }] => {
                let bucket = Some(Bucket { col: *col, off_s: *off_s, first: false });
                return Shape::Grouped { key: *t, bucket };
            }
            [KeyExpr::HourBucket { col, off_s }, KeyExpr::Col(t)] => {
                let bucket = Some(Bucket { col: *col, off_s: *off_s, first: true });
                return Shape::Grouped { key: *t, bucket };
            }
            [] => {}
            other => panic!("window var lane: unsupported key exprs {other:?}"),
        }
        if let Some(&key) = node.params.group_cols.first() {
            return Shape::Grouped { key, bucket: None };
        }
        if node.agg.is_empty() {
            if let OrderBy::ColAsc(order) = node.params.order {
                return Shape::RowTopK { order };
            }
        }
        return Shape::Count;
    }
    Shape::FrameGroup
}

/// Key field typing for the frame lane, derived from the schema (width;
/// the date shim mirrors planner::is_date_col — catalog data, not query).
#[derive(Clone, Copy)]
enum KKind {
    I16,
    I32,
    Date,
    I64,
    Text,
}

fn kkind(bank: &Bank, col: u32) -> KKind {
    // Typed catalog consult (the PoC's "eventdate" name sniff is dead —
    // currency-insertion.md §2).
    let is_date = bank.typ(col).oid == crate::typmeta::oids::DATE;
    match col_width(bank, col) {
        0 => KKind::Text,
        2 => KKind::I16,
        4 => {
            if is_date {
                KKind::Date
            } else {
                KKind::I32
            }
        }
        _ => KKind::I64,
    }
}

fn key_bytes(kinds: &[KKind], cols: &[&[u64]], i: usize, out: &mut Vec<u8>) {
    out.clear();
    for (j, k) in kinds.iter().enumerate() {
        let d = cols[j][i];
        match k {
            KKind::I16 => out.extend_from_slice(&(sx(d, 2) as i16).to_le_bytes()),
            KKind::I32 | KKind::Date => out.extend_from_slice(&(sx(d, 4) as i32).to_le_bytes()),
            KKind::I64 => out.extend_from_slice(&d.to_le_bytes()),
            KKind::Text => {
                assert_eq!(j, kinds.len() - 1, "text key field must be last");
                out.extend_from_slice(unsafe { varlena_payload(d) });
            }
        }
    }
}

/// [sqe-m2] Packability of a frame key tuple: every field fixed-width and
/// the LE byte layout fits u128. Returns the total byte width.
fn pack_spec(kinds: &[KKind]) -> Option<usize> {
    let mut w = 0usize;
    for k in kinds {
        w += match k {
            KKind::I16 => 2,
            KKind::I32 | KKind::Date => 4,
            KKind::I64 => 8,
            KKind::Text => return None,
        };
    }
    (w <= 16).then_some(w)
}

/// The u128 whose LE bytes are exactly `key_bytes`'s layout — the byte
/// tie-order at render is unchanged.
#[inline(always)]
fn key_u128(kinds: &[KKind], cols: &[&[u64]], i: usize) -> u128 {
    let mut k = 0u128;
    let mut off = 0u32;
    for (j, kk) in kinds.iter().enumerate() {
        let d = cols[j][i];
        match kk {
            KKind::I16 => {
                k |= ((sx(d, 2) as i16 as u16) as u128) << (8 * off);
                off += 2;
            }
            KKind::I32 | KKind::Date => {
                k |= ((sx(d, 4) as i32 as u32) as u128) << (8 * off);
                off += 4;
            }
            KKind::I64 => {
                k |= (d as u128) << (8 * off);
                off += 8;
            }
            KKind::Text => unreachable!("packed key has no text field"),
        }
    }
    k
}

/// Fold one granule's survivor keys into the worker maps (packed-int or
/// byte-keyed, elected once per plan by `pack_spec`).
fn fold_group_keys(s: &mut WState, kinds: &[KKind], packed: bool, krefs: &[&[u64]], n: usize) {
    if packed {
        for r in 0..n {
            *s.imap.entry(key_u128(kinds, krefs, r)).or_insert(0) += 1;
        }
    } else {
        let mut kbuf = std::mem::take(&mut s.kbuf);
        for r in 0..n {
            key_bytes(kinds, krefs, r, &mut kbuf);
            match s.fmap.get_mut(kbuf.as_slice()) {
                Some(c) => *c += 1,
                None => {
                    s.fmap.insert(kbuf.clone(), 1);
                }
            }
        }
        s.kbuf = kbuf;
    }
}

/// Typed key emit: split a packed key byte image into per-field typed
/// answer columns (I16/I32 as ints, Date as a DATE-typed i64 lane — the
/// date render lives in the render seam — trailing Text as bytes).
struct KeyCols {
    ints: Vec<(usize, TypMeta, Vec<i64>)>, // (field index, ty, values)
    text: Option<(usize, TypMeta, BytesBuild)>,
    nfields: usize,
}

impl KeyCols {
    fn new(bank: &Bank, kcols: &[u32], kinds: &[KKind]) -> KeyCols {
        let mut ints = Vec::new();
        let mut text = None;
        for (j, kk) in kinds.iter().enumerate() {
            let ty = bank.typ(kcols[j]);
            match kk {
                KKind::Text => text = Some((j, ty, BytesBuild::new())),
                _ => ints.push((j, ty, Vec::new())),
            }
        }
        KeyCols { ints, text, nfields: kinds.len() }
    }
    fn push_key(&mut self, kinds: &[KKind], k: &[u8]) {
        let mut off = 0usize;
        let mut ii = 0usize;
        for (j, kk) in kinds.iter().enumerate() {
            match kk {
                KKind::I16 => {
                    self.ints[ii].2.push(i16::from_le_bytes([k[off], k[off + 1]]) as i64);
                    ii += 1;
                    off += 2;
                }
                KKind::I32 | KKind::Date => {
                    self.ints[ii]
                        .2
                        .push(i32::from_le_bytes(k[off..off + 4].try_into().unwrap()) as i64);
                    ii += 1;
                    off += 4;
                }
                KKind::I64 => {
                    self.ints[ii]
                        .2
                        .push(i64::from_le_bytes(k[off..off + 8].try_into().unwrap()));
                    ii += 1;
                    off += 8;
                }
                KKind::Text => {
                    debug_assert_eq!(j, kinds.len() - 1);
                    self.text.as_mut().unwrap().2.push(&k[off..]);
                    off = k.len();
                }
            }
        }
    }
    /// Finish into field-ordered answer columns + the count column.
    fn finish(self, cnts: Vec<i64>) -> AnswerSet {
        let mut slots: Vec<Option<AnswerCol>> = (0..self.nfields).map(|_| None).collect();
        for (j, ty, v) in self.ints {
            slots[j] = Some(AnswerCol::i64s(ty, v));
        }
        if let Some((j, ty, b)) = self.text {
            slots[j] = Some(b.finish(ty));
        }
        let mut cols: Vec<AnswerCol> = slots.into_iter().map(|c| c.unwrap()).collect();
        cols.push(AnswerCol::i64s(TypMeta::INT8, cnts));
        AnswerSet::from_cols(cols)
    }
}

/// [sqe-m2] Rank-window render straight off the packed-int group table
/// (single-state serial replays): no per-group byte materialization until
/// the k winners. Byte-identical order: LE-byte slices compare exactly as
/// `render_frame`'s key bytes.
fn render_frame_packed(
    bank: &Bank,
    node: &PlanNode,
    kcols: &[u32],
    kinds: &[KKind],
    w: usize,
    imap: &HashMap<u128, u64, BuildHasherDefault<crate::kernels_f6::FxHasher>>,
) -> AnswerSet {
    let total_rows: u64 = imap.values().sum();
    let groups = imap.len();
    let mut rows: Vec<(u128, u64)> = imap.iter().map(|(&k, &c)| (k, c)).collect();
    let k = (node.params.offset + node.params.limit).min(rows.len());
    let cmp = |a: &(u128, u64), b: &(u128, u64)| {
        b.1.cmp(&a.1)
            .then_with(|| a.0.to_le_bytes()[..w].cmp(&b.0.to_le_bytes()[..w]))
    };
    if k > 0 && k < rows.len() {
        rows.select_nth_unstable_by(k - 1, cmp);
    }
    rows.truncate(k);
    rows.sort_by(cmp);
    let mut kc = KeyCols::new(bank, kcols, kinds);
    let mut cnts: Vec<i64> = Vec::new();
    for (kb, c) in rows.into_iter().skip(node.params.offset).take(node.params.limit) {
        kc.push_key(kinds, &kb.to_le_bytes()[..w]);
        cnts.push(c as i64);
    }
    let mut a = kc.finish(cnts);
    a.note = Some(crate::render::footer_groups(groups as u64, total_rows as u64));
    a
}

/// Rank-window render (kernels_f6 law): COUNT DESC, packed-key-bytes ASC
/// tie; select_nth head; offset..offset+limit; groups/rows trailer.
fn render_frame(
    bank: &Bank,
    node: &PlanNode,
    kcols: &[u32],
    kinds: &[KKind],
    map: &FxMap,
) -> AnswerSet {
    let total_rows: u64 = map.values().sum();
    let groups = map.len();
    let mut rows: Vec<(&Vec<u8>, u64)> = map.iter().map(|(k, &c)| (k, c)).collect();
    let k = (node.params.offset + node.params.limit).min(rows.len());
    let cmp = |a: &(&Vec<u8>, u64), b: &(&Vec<u8>, u64)| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0));
    if k > 0 && k < rows.len() {
        rows.select_nth_unstable_by(k - 1, cmp);
    }
    rows.truncate(k);
    rows.sort_by(cmp);
    let mut kc = KeyCols::new(bank, kcols, kinds);
    let mut cnts: Vec<i64> = Vec::new();
    for (kb, c) in rows.into_iter().skip(node.params.offset).take(node.params.limit) {
        kc.push_key(kinds, kb);
        cnts.push(c as i64);
    }
    let mut a = kc.finish(cnts);
    a.note = Some(crate::render::footer_groups(groups as u64, total_rows as u64));
    a
}

// ---------------------------------------------------------------------------
// worker state
// ---------------------------------------------------------------------------

struct ColIO {
    attno: u32,
    scr: Scratch,
    cur: CurCache,
}

impl ColIO {
    fn new(attno: u32) -> ColIO {
        // [persist-rehome] scratch rides the worker depot (this was the
        // scratch-depot doc's documented window_replay follow-up).
        ColIO { attno, scr: crate::scan::scratch_fetch(), cur: CurCache::new(attno) }
    }
}

/// Engagement-end park: decode scratches to the executing thread's depot
/// (worker-side under `run_finish`, caller-side on the serial arms);
/// cursors DROP here — never at rest across statements.
fn park_ios(ios: &mut Vec<ColIO>) {
    for io in ios.drain(..) {
        crate::scan::scratch_park(io.scr);
    }
}

/// Per-group accumulator (grouped shape): count + byte-mins (agg order) +
/// distinct sets (agg order) + word fold cells (agg order).
struct GAcc {
    c: u64,
    mins: Vec<Vec<u8>>,
    users: Vec<FxSet64>,
    /// Per CountDistinct leg (aligned with `users`): the varlena-input
    /// set; exactly one of the pair is populated per leg.
    tusers: Vec<FxSet128>,
    /// Per MIN/MAX/SUM word leg (agg order): exact i128 cell, None = NULL.
    words: Vec<Option<i128>>,
    /// [spill-5] per CountDistinct leg: finalize-merged spilled count.
    tcnt: Vec<Option<i64>>,
}

impl GAcc {
    fn empty() -> GAcc {
        GAcc {
            c: 0,
            mins: Vec::new(),
            users: Vec::new(),
            tusers: Vec::new(),
            words: Vec::new(),
            tcnt: Vec::new(),
        }
    }
    fn fresh(nmins: usize, ndist: usize, nwords: usize) -> GAcc {
        GAcc {
            c: 0,
            mins: vec![Vec::new(); nmins],
            users: vec![FxSet64::default(); ndist],
            tusers: vec![FxSet128::default(); ndist],
            words: vec![None; nwords],
            tcnt: vec![None; ndist],
        }
    }
}

#[inline(always)]
fn is_word_fold(op: AggOp) -> bool {
    matches!(op, AggOp::Min | AggOp::Max | AggOp::Sum)
}

#[inline(always)]
fn fold_word(cell: &mut Option<i128>, op: AggOp, x: i128) {
    *cell = Some(match (*cell, op) {
        (None, _) => x,
        (Some(c), AggOp::Min) => c.min(x),
        (Some(c), AggOp::Max) => c.max(x),
        (Some(c), _) => c + x,
    });
}

struct WState {
    drv_fc: FcCache,
    codes: Vec<u32>,
    sel: Vec<u16>,
    sel2: Vec<u16>,
    rowbuf: Vec<u16>,
    kbuf: Vec<u8>,
    ios: Vec<ColIO>,
    nmatch: u64,
    winners: Vec<Winner>,
    gmap: FxGMap,
    fmap: FxMap,
    /// [sqe-m2] packed-int frame keys (all-fixed-width key tuples <= 16B):
    /// grouping never leaves the integer domain (the opt_int law); bytes
    /// are minted once per GROUP at render, not once per row.
    imap: HashMap<u128, u64, BuildHasherDefault<crate::kernels_f6::FxHasher>>,
    /// [json-rung2] The NULL-key group of the var-lane Grouped shape
    /// (3VL: NULL keys group once); `c == 0` = no NULL key seen.
    gnull: GAcc,
    /// NULL text key per hour bucket (empty until a NULL key is seen).
    gnullb: Vec<GAcc>,
    // cold-record capture (unit ordinal -> verdict/codes)
    rec: Vec<(usize, CVerdict, Vec<u32>)>,
    // [spill-5] fp128 distinct-plane meter + per-worker run registry
    gauge: Option<Arc<WrGauge>>,
    dres: u64,
    dspill: Option<crate::stencils::hash_group::BW>,
    druns: Vec<(Vec<u8>, u8, u64, u64)>,
}

impl WState {
    fn new(driving: u32, cols: &[u32], gauge: Option<Arc<WrGauge>>) -> WState {
        let mut ios: Vec<ColIO> = vec![ColIO::new(driving)];
        for &a in cols {
            if !ios.iter().any(|io| io.attno == a) {
                ios.push(ColIO::new(a));
            }
        }
        WState {
            drv_fc: FcCache::new(driving),
            codes: vec![0; 8192],
            sel: Vec::new(),
            sel2: Vec::new(),
            rowbuf: Vec::new(),
            kbuf: Vec::new(),
            ios,
            nmatch: 0,
            winners: Vec::new(),
            gmap: FxGMap::default(),
            fmap: FxMap::default(),
            imap: HashMap::default(),
            gnull: GAcc::empty(),
            gnullb: Vec::new(),
            rec: Vec::new(),
            gauge,
            dres: 0,
            dspill: None,
            druns: Vec::new(),
        }
    }
    fn io(&mut self, attno: u32) -> &mut ColIO {
        self.ios.iter_mut().find(|io| io.attno == attno).expect("column io")
    }
}

// ---------------------------------------------------------------------------
// VAR lane
// ---------------------------------------------------------------------------

/// Columns the epilogue + staged residues touch (beyond the driving col).
fn other_cols(node: &PlanNode, shape: &Shape) -> Vec<u32> {
    let pred = node.pred.as_ref().unwrap();
    let mut v: Vec<u32> = Vec::new();
    let mut push = |a: u32| {
        if !v.contains(&a) {
            v.push(a);
        }
    };
    for t in pred.var_terms.iter().skip(1) {
        push(t.col);
    }
    // [sqe-m4] int conjuncts composed with the var lane (zone prune +
    // decode_sel residue filter) need their columns' IO too.
    for t in pred.frame().iter().chain(pred.residues()) {
        push(t.col);
    }
    match shape {
        Shape::Count => {}
        Shape::Grouped { key, bucket } => {
            push(*key);
            if let Some(b) = bucket {
                push(b.col);
            }
            for a in &node.agg {
                if let Some(c) = a.col {
                    push(c);
                }
            }
        }
        Shape::RowTopK { order } => push(*order),
        Shape::FrameGroup => unreachable!(),
    }
    let driving = pred.var_terms[0].col;
    v.retain(|&a| a != driving);
    v
}

/// Stage-1 verdict-bitword survivor selection for one granule. When
/// `keep_codes`, survivor codes land in s.codes[..sel.len()] (or MAX).
fn stage1_select(
    bank: &Bank,
    vw: &VerdictWords,
    t1: &VarPredTerm,
    s: &mut WState,
    pi: usize,
    g: u32,
    rows: usize,
    keep_codes: bool,
) {
    s.sel.clear();
    match vw.part_words(pi) {
        Some(w) => match s.drv_fc.get(bank, pi) {
            Some(fc) => {
                let selr = &mut s.sel;
                if keep_codes {
                    if s.codes.len() < rows {
                        s.codes.resize(rows, 0);
                    }
                    let codes = &mut s.codes;
                    let mut k = 0usize;
                    fc.fold_ptr(g, rows, |r, code| {
                        if bitw(w, code) {
                            selr.push(r as u16);
                            codes[k] = code;
                            k += 1;
                        }
                    });
                } else {
                    fc.fold_ptr(g, rows, |r, code| {
                        if bitw(w, code) {
                            selr.push(r as u16);
                        }
                    });
                }
            }
            None => {
                if s.codes.len() < rows {
                    s.codes.resize(rows, 0);
                }
                let cur = s.ios[0].cur.get(bank, pi);
                cur.decode_codes(g, &mut s.codes[..rows]).expect("codes");
                let mut k = 0usize;
                for r in 0..rows {
                    let code = s.codes[r];
                    if bitw(w, code) {
                        s.sel.push(r as u16);
                        if keep_codes {
                            s.codes[k] = code;
                            k += 1;
                        }
                    }
                }
            }
        },
        None => {
            // Scalar fallback: part does not publish a dict. NULL cells
            // are unspecified bytes — validity gates every payload read.
            let io = &mut s.ios[0];
            let all_valid = io.scr.validity(io.cur.get(bank, pi), g, rows).all_valid();
            io.scr.decode_full(io.cur.get(bank, pi), g, rows);
            let mut k = 0usize;
            for r in 0..rows {
                if !all_valid && !io.scr.row_valid(r) {
                    continue;
                }
                let x = io.scr.datums[r];
                if t1.eval(unsafe { varlena_payload(x) }) {
                    s.sel.push(r as u16);
                    if keep_codes {
                        if s.codes.len() <= k {
                            s.codes.resize(k + 1, u32::MAX);
                        }
                        s.codes[k] = u32::MAX;
                        k += 1;
                    }
                }
            }
        }
    }
}

/// Later varlena conjuncts on stage-1 survivors (decode_sel row bytes),
/// compacting the kept-codes lane in lockstep when present.
fn var_residue_filter(
    bank: &Bank,
    terms: &[VarPredTerm],
    s: &mut WState,
    pi: usize,
    g: u32,
    keep_codes: bool,
) {
    for t in terms.iter().skip(1) {
        if s.sel.is_empty() {
            return;
        }
        let sel = std::mem::take(&mut s.sel);
        let mut keep = std::mem::take(&mut s.sel2);
        keep.clear();
        let mut kk = 0usize;
        {
            let io = s.ios.iter_mut().find(|io| io.attno == t.col).expect("residue io");
            let cur = io.cur.get(bank, pi);
            let rows = cur.rows_in_granule(g) as usize;
            let all_valid = io.scr.validity(cur, g, rows).all_valid();
            io.scr.decode_sel(io.cur.get(bank, pi), g, &sel);
            for (k, &r) in sel.iter().enumerate() {
                if !all_valid && !io.scr.row_valid(r as usize) {
                    continue;
                }
                if t.eval(unsafe { varlena_payload(io.scr.datums[k]) }) {
                    keep.push(r);
                    if keep_codes {
                        s.codes[kk] = s.codes[k];
                        kk += 1;
                    }
                }
            }
        }
        s.sel = keep;
        s.sel2 = sel;
    }
}

/// [json-rung2] The strict-operator veto on the DRIVING column: every
/// var-lane op answers NULL (never TRUE) on a NULL input, so stage-1
/// survivors whose driving cell is NULL (placeholder codes under the
/// entry verdict) leave the set — row grain, before the plane is
/// recorded (a recorded plane is the FINAL 3VL survivor set). Residue
/// terms veto inline at their decode. Null-free columns never enter.
fn null_veto(
    bank: &Bank,
    terms: &[VarPredTerm],
    s: &mut WState,
    pi: usize,
    g: u32,
    rows: usize,
    keep_codes: bool,
) {
    for t in terms {
        if s.sel.is_empty() {
            return;
        }
        if bank.null_free(t.col) {
            continue;
        }
        let io = s.ios.iter_mut().find(|io| io.attno == t.col).expect("veto io");
        if io.scr.validity(io.cur.get(bank, pi), g, rows).all_valid() {
            continue;
        }
        let mut w = 0usize;
        for k in 0..s.sel.len() {
            if io.scr.row_valid(s.sel[k] as usize) {
                s.sel[w] = s.sel[k];
                if keep_codes {
                    s.codes[w] = s.codes[k];
                }
                w += 1;
            }
        }
        s.sel.truncate(w);
    }
}

/// One `decode_sel` + one validity map per LANE per granule: the survivor
/// image is cached by column, so every leg over a shared lane reads the
/// same decode (two folds over one word lane, the bucket beside a fold).
fn decode_lane(
    lanes: &mut Vec<(u32, Vec<u64>, Option<Vec<bool>>)>,
    s: &mut WState,
    bank: &Bank,
    pi: usize,
    g: u32,
    sel: &[u16],
    c: u32,
) -> usize {
    if let Some(i) = lanes.iter().position(|l| l.0 == c) {
        return i;
    }
    let io = s.io(c);
    let cur = io.cur.get(bank, pi);
    let rows = cur.rows_in_granule(g) as usize;
    let all_valid = io.scr.validity(cur, g, rows).all_valid();
    let v = (!all_valid).then(|| sel.iter().map(|&r| io.scr.row_valid(r as usize)).collect());
    let d = io.scr.decode_sel(io.cur.get(bank, pi), g, sel).to_vec();
    lanes.push((c, d, v));
    lanes.len() - 1
}

/// Fold final survivors of one granule into the shape accumulators.
/// `codes`: survivor codes of the sidecar column (empty = none).
fn consume_var(
    ctx: &SqeCtx,
    node: &PlanNode,
    shape: &Shape,
    s: &mut WState,
    pi: usize,
    g: u32,
    base: u64,
    sidecar_col: Option<u32>,
    codes: &[u32],
) {
    let bank = ctx.bank;
    if s.sel.is_empty() {
        return;
    }
    s.nmatch += s.sel.len() as u64;
    match shape {
        Shape::Count => {}
        Shape::RowTopK { order } => {
            let sel = std::mem::take(&mut s.sel);
            {
                let io = s.io(*order);
                let d = io.scr.decode_sel(io.cur.get(bank, pi), g, &sel).to_vec();
                for (k, &r) in sel.iter().enumerate() {
                    topk_insert(
                        &mut s.winners,
                        Winner {
                            etime: d[k] as i64,
                            grow: base + r as u64,
                            pi: pi as u32,
                            g,
                            rowin: r,
                        },
                        node.params.limit,
                    );
                }
            }
            s.sel = sel;
        }
        Shape::Grouped { key, bucket } => {
            let sel = std::mem::take(&mut s.sel);
            // [json-rung2] key validity (None = every survivor's key is
            // non-NULL — the null-free fast path); NULL keys group once.
            let mut lanes: Vec<(u32, Vec<u64>, Option<Vec<bool>>)> = Vec::new();
            let ki = decode_lane(&mut lanes, s, bank, pi, g, &sel, *key);
            let db: Option<Vec<u8>> = bucket.map(|b| {
                let i = decode_lane(&mut lanes, s, bank, pi, g, &sel, b.col);
                lanes[i].1.iter().map(|&x| hour_bucket(x as i64, b.off_s)).collect()
            });
            // Per MinBytes agg: either dict-entry bytes via the sidecar
            // codes (col == sidecar col, all codes resolved) or row bytes.
            // Non-sidecar legs hold lane-cache indices.
            let mut dmins: Vec<Option<usize>> = Vec::new();
            let mut ddist: Vec<(usize, bool)> = Vec::new();
            let mut dwords: Vec<(usize, u8, AggOp)> = Vec::new();
            let use_sidecar = |c: u32| {
                sidecar_col == Some(c) && !codes.is_empty() && codes.iter().all(|&x| x != u32::MAX)
            };
            for a in &node.agg {
                match a.op {
                    AggOp::MinBytes => {
                        let c = a.col.unwrap();
                        if use_sidecar(c) {
                            dmins.push(None);
                        } else {
                            dmins.push(Some(decode_lane(&mut lanes, s, bank, pi, g, &sel, c)));
                        }
                    }
                    AggOp::CountDistinct => {
                        let c = a.col.unwrap();
                        let text = col_width(bank, c) == 0;
                        ddist.push((decode_lane(&mut lanes, s, bank, pi, g, &sel, c), text));
                    }
                    op if is_word_fold(op) => {
                        let c = a.col.unwrap();
                        let w = col_width(bank, c);
                        dwords.push((decode_lane(&mut lanes, s, bank, pi, g, &sel, c), w, op));
                    }
                    _ => {}
                }
            }
            let dict = if dmins.iter().any(|d| d.is_none()) {
                sidecar_col.map(|c| ctx.faces.dict(bank, pi, c))
            } else {
                None
            };
            let dfrozen = s.gauge.as_ref().is_some_and(|g| g.over.load(Relaxed));
            let dfresh = std::cell::Cell::new(0u64);
            // Per-row fold body, shared by both probe arms below.
            let fold = |e: &mut GAcc, k: usize| {
                e.c += 1;
                let seed = e.c == 1;
                for (mi, dm) in dmins.iter().enumerate() {
                    let b: &[u8] = match dm {
                        Some(i) => unsafe { varlena_payload(lanes[*i].1[k]) },
                        None => {
                            let dh = dict.as_ref().unwrap().dh.as_ref().unwrap();
                            dh.entry(codes[k]).expect("dict entry").bytes
                        }
                    };
                    if seed || b < e.mins[mi].as_slice() {
                        e.mins[mi] = b.to_vec();
                    }
                }
                for (ui, (di, text)) in ddist.iter().enumerate() {
                    if *text {
                        if !dfrozen
                            && e.tusers[ui].insert(crate::fp::entry_fp128(unsafe {
                                varlena_payload(lanes[*di].1[k])
                            }))
                        {
                            dfresh.set(dfresh.get() + 1);
                        }
                    } else {
                        e.users[ui].insert(lanes[*di].1[k]);
                    }
                }
                for (wi, (di, w, op)) in dwords.iter().enumerate() {
                    let (_, d, v) = &lanes[*di];
                    if v.as_ref().is_some_and(|v| !v[k]) {
                        continue;
                    }
                    fold_word(&mut e.words[wi], *op, sx(d[k], *w) as i128);
                }
            };
            let fresh = || GAcc::fresh(dmins.len(), ddist.len(), dwords.len());
            let mut kbuf = std::mem::take(&mut s.kbuf);
            let (_, dg, kvalid) = &lanes[ki];
            for k in 0..sel.len() {
                let b = db.as_ref().map(|v| v[k]);
                if kvalid.as_ref().is_some_and(|v| !v[k]) {
                    // `key <> ''` is NULL on a NULL key: the row is out.
                    if node.params.flags & F_DROP_EMPTY_KEY != 0 {
                        continue;
                    }
                    match b {
                        None => {
                            if s.gnull.c == 0 {
                                s.gnull = fresh();
                            }
                            fold(&mut s.gnull, k);
                        }
                        Some(b) => {
                            if s.gnullb.is_empty() {
                                s.gnullb = (0..24).map(|_| fresh()).collect();
                            }
                            fold(&mut s.gnullb[b as usize], k);
                        }
                    }
                    continue;
                }
                let p = unsafe { varlena_payload(dg[k]) };
                if node.params.flags & F_DROP_EMPTY_KEY != 0 && p.is_empty() {
                    continue;
                }
                let p: &[u8] = match b {
                    None => p,
                    Some(b) => {
                        kbuf.clear();
                        kbuf.extend_from_slice(p);
                        kbuf.push(b);
                        &kbuf
                    }
                };
                // Probe with the BORROWED key first (fold_group_keys
                // idiom): the key bytes are minted once per GROUP, never
                // once per surviving row.
                match s.gmap.get_mut(p) {
                    Some(e) => fold(e, k),
                    None => fold(s.gmap.entry(p.to_vec()).or_insert_with(fresh), k),
                }
            }
            s.kbuf = kbuf;
            s.sel = sel;
            if let Some(g) = s.gauge.clone() {
                let fresh = dfresh.get();
                if fresh > 0 {
                    s.dres += fresh;
                    g.bytes.fetch_add(fresh * DSET128_ENTRY_BYTES, Relaxed);
                }
                if s.dres * DSET128_ENTRY_BYTES > g.share {
                    if g.spill.is_some() {
                        drain_wr_sets(s, &g);
                    } else {
                        g.over.store(true, Relaxed);
                    }
                }
            }
        }
        Shape::FrameGroup => unreachable!(),
    }
}

/// One worker-local accumulator merged into the global cell (the render
/// combine law — commutative/associative min/count/word-fold/set-union).
/// `take_sets` = the serial law (distinct sets union here); the Sets arm
/// passes false and receives the source sets back for the sharded
/// parallel union.
fn merge_gacc(
    e: &mut GAcc,
    a: GAcc,
    wops: &[AggOp],
    take_sets: bool,
) -> Option<(Vec<FxSet64>, Vec<FxSet128>)> {
    e.c += a.c;
    for (mi, m) in a.mins.into_iter().enumerate() {
        if m.as_slice() < e.mins[mi].as_slice() {
            e.mins[mi] = m;
        }
    }
    for (wi, w) in a.words.into_iter().enumerate() {
        if let Some(x) = w {
            fold_word(&mut e.words[wi], wops[wi], x);
        }
    }
    if take_sets {
        for (ui, u) in a.users.into_iter().enumerate() {
            e.users[ui].extend(u);
        }
        for (ui, u) in a.tusers.into_iter().enumerate() {
            e.tusers[ui].extend(u);
        }
        None
    } else {
        Some((a.users, a.tusers))
    }
}

/// `merge_gacc` under the empty-cell convention (`c == 0` = never seen).
fn merge_gacc_opt(e: &mut GAcc, a: GAcc, wops: &[AggOp]) {
    if a.c == 0 {
        return;
    }
    if e.c == 0 {
        *e = a;
    } else {
        merge_gacc(e, a, wops, true);
    }
}

/// [q45 changes 1+2] The scatter→owner grouped combine (hash_group's
/// pass1/pass2 grammar at COMBINE grain): worker maps drain into P
/// key-shard buckets (pure `pm_shard` — owner key spaces are disjoint by
/// construction), owners merge their shard across all sources and build
/// partition-local group rows; `bound` (the per-owner top-k under the
/// TOTAL select_groups order) runs inside the owner, so the leader sees
/// ≤ P·k candidates. Returns (rows in partition order, groups_out);
/// emit order is canonicalized downstream (select_groups /
/// sort_count_rows — the identity law both arms share).
fn pm_combine(
    pool: &crate::pool::Pool,
    node: Option<&PlanNode>,
    wmaps: Vec<FxGMap>,
    p: usize,
    wops: &[AggOp],
    has_bucket: bool,
    bound: Option<&(dyn Fn(&mut Vec<GRow>) + Sync)>,
) -> (Vec<GRow>, u64) {
    use std::sync::Mutex;
    let t0 = std::time::Instant::now();
    let nsrc = wmaps.len();
    let cells: Vec<Mutex<Option<FxGMap>>> =
        wmaps.into_iter().map(|m| Mutex::new(Some(m))).collect();
    let scat: Vec<Vec<Vec<Vec<(Vec<u8>, GAcc)>>>> = pool.run(
        cells.len(),
        |_| Vec::new(),
        |out: &mut Vec<Vec<Vec<(Vec<u8>, GAcc)>>>, i| {
            let m = cells[i].lock().unwrap().take().expect("one taker per source map");
            let mut b: Vec<Vec<(Vec<u8>, GAcc)>> = (0..p).map(|_| Vec::new()).collect();
            for (k, a) in m {
                b[pm_shard(&k, p)].push((k, a));
            }
            out.push(b);
        },
    );
    // Transpose to per-partition source-bucket lists (pointer moves only).
    let mut parts: Vec<Vec<Vec<(Vec<u8>, GAcc)>>> =
        (0..p).map(|_| Vec::with_capacity(nsrc)).collect();
    for w in scat {
        for bs in w {
            for (pi, b) in bs.into_iter().enumerate() {
                if !b.is_empty() {
                    parts[pi].push(b);
                }
            }
        }
    }
    PARTMERGE_SCATTER_NS.store(t0.elapsed().as_nanos() as u64, Relaxed);
    if let Some(n) = node {
        crate::engine::phn(n, "pm_scatter", t0);
    }
    let t1 = std::time::Instant::now();
    let pcells: Vec<Mutex<Option<Vec<Vec<(Vec<u8>, GAcc)>>>>> =
        parts.into_iter().map(|x| Mutex::new(Some(x))).collect();
    let groups_out = AtomicU64::new(0);
    let owned: Vec<Vec<(usize, Vec<GRow>)>> = pool.run(
        p,
        |_| Vec::new(),
        |out: &mut Vec<(usize, Vec<GRow>)>, part| {
            let srcs = pcells[part].lock().unwrap().take().expect("one owner per partition");
            let n: usize = srcs.iter().map(|b| b.len()).sum();
            if n == 0 {
                return;
            }
            let mut m = FxGMap::with_capacity_and_hasher(n, Default::default());
            for b in srcs {
                for (k, a) in b {
                    match m.get_mut(&k) {
                        Some(e) => {
                            merge_gacc(e, a, wops, true);
                        }
                        None => {
                            m.insert(k, a);
                        }
                    }
                }
            }
            groups_out.fetch_add(m.len() as u64, Relaxed);
            let mut rows: Vec<GRow> = m
                .into_iter()
                .map(|(mut k, acc)| {
                    let b = if has_bucket { k.pop().expect("bucket byte") } else { 0 };
                    GRow { key: Some(k), b, acc }
                })
                .collect();
            if let Some(f) = bound {
                f(&mut rows);
            }
            out.push((part, rows));
        },
    );
    PARTMERGE_OWNER_NS.store(t1.elapsed().as_nanos() as u64, Relaxed);
    if let Some(n) = node {
        crate::engine::phn(n, "pm_owner", t1);
    }
    let mut all: Vec<(usize, Vec<GRow>)> = owned.into_iter().flatten().collect();
    all.sort_by_key(|x| x.0);
    (all.into_iter().flat_map(|x| x.1).collect(), groups_out.load(Relaxed))
}

/// One (group, distinct-leg)'s source sets for the sharded union.
#[derive(Default)]
struct PmSetItem {
    sets64: Vec<FxSet64>,
    sets128: Vec<FxSet128>,
}

/// [q45 bonus] The distinct-set combine at ELEMENT grain (q2-class:
/// small G, million-entry per-group sets — group-grain partitioning
/// cannot spread ONE group's union). Per source set, elements scatter
/// into P element-shard buckets (pure `pm_shard64/128`); owners dedupe
/// each (item, shard) column independently — shard element spaces are
/// disjoint, so per-item distinct = Σ shard distincts, exactly the
/// serial union's len(). The u64-id and fp128 families count separately,
/// mirroring dcount's `users.len() + tusers.len()`.
fn pm_union_sets(
    pool: &crate::pool::Pool,
    node: Option<&PlanNode>,
    items: Vec<PmSetItem>,
    p: usize,
) -> Vec<i64> {
    use std::sync::Mutex;
    struct B {
        b64: Vec<Vec<u64>>,
        b128: Vec<Vec<u128>>,
    }
    if items.is_empty() {
        return Vec::new();
    }
    let t0 = std::time::Instant::now();
    let nitems = items.len();
    let cells: Vec<Mutex<Option<PmSetItem>>> =
        items.into_iter().map(|it| Mutex::new(Some(it))).collect();
    let scat: Vec<Vec<(usize, B)>> = pool.run(
        cells.len(),
        |_| Vec::new(),
        |out: &mut Vec<(usize, B)>, i| {
            let it = cells[i].lock().unwrap().take().expect("one taker per item");
            let mut b = B {
                b64: (0..p).map(|_| Vec::new()).collect(),
                b128: (0..p).map(|_| Vec::new()).collect(),
            };
            for s in it.sets64 {
                for x in s {
                    b.b64[pm_shard64(x, p)].push(x);
                }
            }
            for s in it.sets128 {
                for x in s {
                    b.b128[pm_shard128(x, p)].push(x);
                }
            }
            out.push((i, b));
        },
    );
    let mut byitem: Vec<Option<B>> = (0..nitems).map(|_| None).collect();
    for w in scat {
        for (i, b) in w {
            byitem[i] = Some(b);
        }
    }
    let byitem: Vec<B> = byitem.into_iter().map(|x| x.expect("scattered item")).collect();
    if let Some(n) = node {
        crate::engine::phn(n, "pm_setscatter", t0);
    }
    let t1 = std::time::Instant::now();
    let owned: Vec<Vec<(usize, i64)>> = pool.run(
        p,
        |_| Vec::new(),
        |out: &mut Vec<(usize, i64)>, part| {
            let mut d64 = FxSet64::default();
            let mut d128 = FxSet128::default();
            for (i, b) in byitem.iter().enumerate() {
                if b.b64[part].is_empty() && b.b128[part].is_empty() {
                    continue;
                }
                d64.clear();
                d128.clear();
                d64.extend(b.b64[part].iter().copied());
                d128.extend(b.b128[part].iter().copied());
                out.push((i, (d64.len() + d128.len()) as i64));
            }
        },
    );
    let mut totals = vec![0i64; nitems];
    for w in owned {
        for (i, n) in w {
            totals[i] += n;
        }
    }
    if let Some(n) = node {
        crate::engine::phn(n, "pm_setowner", t1);
    }
    PARTMERGE_SETS_NS.store(t0.elapsed().as_nanos() as u64, Relaxed);
    totals
}

/// The no-topk grouped answer order (count desc, key bytes
/// NULL-greatest, bucket) — already TOTAL over groups; shared by the
/// serial and partitioned arms (identity law).
fn sort_count_rows(rows: &mut Vec<GRow>, limit: usize) {
    rows.sort_by(|a, b| {
        b.acc
            .c
            .cmp(&a.acc.c)
            .then_with(|| cmp_key_null_greatest(&a.key, &b.key))
            .then_with(|| a.b.cmp(&b.b))
    });
    rows.truncate(limit);
}

fn render_var(ctx: &SqeCtx, node: &PlanNode, shape: &Shape, states: Vec<WState>) -> AnswerSet {
    let bank = ctx.bank;
    match shape {
        Shape::Count => {
            let n: u64 = states.iter().map(|s| s.nmatch).sum();
            AnswerSet::from_cols(vec![AnswerCol::i64s(TypMeta::INT8, vec![n as i64])])
        }
        Shape::RowTopK { .. } => {
            let mut winners: Vec<Winner> = Vec::new();
            let mut nmatch = 0u64;
            for st in &states {
                nmatch += st.nmatch;
                for &w in &st.winners {
                    topk_insert(&mut winners, w, node.params.limit);
                }
            }
            let mut a = hydrate_winners(bank, &winners);
            a.head_note = Some(crate::render::head_matches(nmatch));
            a
        }
        Shape::Grouped { key, bucket } => {
            let gauge = states.iter().find_map(|s| s.gauge.clone());
            if let Some(g) = &gauge {
                if g.over.load(Relaxed) {
                    crate::refuse::raise_runtime(crate::refuse::Refuse::GroupedSpillUnavailable {
                        what: "no-substrate",
                        est: g.bytes.load(Relaxed),
                        budget: g.share,
                    });
                }
            }
            let mut media: Vec<crate::stencils::hash_group::BW> = Vec::new();
            let mut spruns: Vec<(Vec<u8>, u8, usize, u64, u64)> = Vec::new();
            let (key, bucket) = (*key, *bucket);
            let wops: Vec<AggOp> =
                node.agg.iter().map(|a| a.op).filter(|&op| is_word_fold(op)).collect();
            let has_dist = node.agg.iter().any(|a| matches!(a.op, AggOp::CountDistinct));
            // [q45 partmerge] The always-serial pieces leave the worker
            // states first: spill run registries and the NULL-key groups
            // (≤ 25 accs per worker — never combine-bound).
            let mut gnull = GAcc::empty();
            let mut gnullb: Vec<GAcc> = Vec::new();
            let mut wmaps: Vec<FxGMap> = Vec::with_capacity(states.len());
            for mut st in states {
                if let Some(bw) = st.dspill.take() {
                    let mi = media.len();
                    for (k, leg, off, n) in st.druns.drain(..) {
                        spruns.push((k, leg, mi, off, n));
                    }
                    media.push(bw);
                }
                merge_gacc_opt(&mut gnull, std::mem::replace(&mut st.gnull, GAcc::empty()), &wops);
                if !st.gnullb.is_empty() {
                    if gnullb.is_empty() {
                        gnullb = std::mem::take(&mut st.gnullb);
                    } else {
                        for (e, a) in gnullb.iter_mut().zip(st.gnullb.drain(..)) {
                            merge_gacc_opt(e, a, &wops);
                        }
                    }
                }
                wmaps.push(std::mem::take(&mut st.gmap));
            }
            let width = ctx.pool.threads();
            let entries: u64 = wmaps.iter().map(|m| m.len() as u64).sum();
            let arm =
                pm_elect(partmerge_on(), wmaps.len(), width, !spruns.is_empty(), entries, || {
                    if !has_dist {
                        return 0;
                    }
                    wmaps
                        .iter()
                        .flat_map(|m| m.values())
                        .map(|a| {
                            a.users.iter().map(|s| s.len() as u64).sum::<u64>()
                                + a.tusers.iter().map(|s| s.len() as u64).sum::<u64>()
                        })
                        .sum()
                });
            let keyspec = KeySpec {
                nkeys: 1 + usize::from(bucket.is_some()),
                bucket_first: bucket.is_some_and(|b| b.first),
            };
            let tk = node.params.topk.as_ref().filter(|t| !t.native);
            let mut map: FxGMap = FxGMap::default();
            let mut grows: Option<Vec<GRow>> = None;
            match arm {
                PmArm::Group => {
                    PARTMERGE_ENGAGED.fetch_add(1, Relaxed);
                    // E7 sizing: entries is a sound UPPER bound on G
                    // (every group appears in ≥ 1 worker map).
                    let p = crate::planner::partition_count(
                        entries as usize,
                        node.params.slot_bytes,
                        node.params.l2_bytes,
                        width,
                    );
                    let limit = node.params.limit;
                    let bound: Box<dyn Fn(&mut Vec<GRow>) + Sync + '_> = match tk {
                        Some(t) => {
                            Box::new(move |r: &mut Vec<GRow>| select_groups(r, node, &keyspec, t))
                        }
                        None => Box::new(move |r: &mut Vec<GRow>| sort_count_rows(r, limit)),
                    };
                    let (r, groups_out) = pm_combine(
                        ctx.pool,
                        Some(node),
                        wmaps,
                        p,
                        &wops,
                        bucket.is_some(),
                        Some(&*bound),
                    );
                    PARTMERGE_PARTITIONS.store(p as u64, Relaxed);
                    PARTMERGE_ENTRIES_IN.store(entries, Relaxed);
                    PARTMERGE_GROUPS_OUT.store(groups_out, Relaxed);
                    grows = Some(r);
                }
                PmArm::Sets => {
                    PARTMERGE_SET_ENGAGED.fetch_add(1, Relaxed);
                    // Serial scalar merge; distinct sets DEFER (one
                    // PmSetItem per (group, leg)) to the sharded union.
                    let mut pend: HashMap<
                        Vec<u8>,
                        Vec<PmSetItem>,
                        BuildHasherDefault<crate::kernels_f6::FxHasher>,
                    > = HashMap::default();
                    for m in wmaps {
                        for (k, a) in m {
                            match map.get_mut(&k) {
                                None => {
                                    map.insert(k, a);
                                }
                                Some(e) => {
                                    let (us, ts) =
                                        merge_gacc(e, a, &wops, false).expect("deferred sets");
                                    let pv = pend.entry(k).or_insert_with(|| {
                                        (0..us.len()).map(|_| PmSetItem::default()).collect()
                                    });
                                    for (li, s) in us.into_iter().enumerate() {
                                        if !s.is_empty() {
                                            pv[li].sets64.push(s);
                                        }
                                    }
                                    for (li, s) in ts.into_iter().enumerate() {
                                        if !s.is_empty() {
                                            pv[li].sets128.push(s);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    let mut idx: Vec<(Vec<u8>, usize)> = Vec::new();
                    let mut items: Vec<PmSetItem> = Vec::new();
                    let mut elems = 0u64;
                    for (k, legs) in pend {
                        let acc = map.get_mut(&k).expect("pend key is resident");
                        for (li, mut it) in legs.into_iter().enumerate() {
                            if it.sets64.is_empty() && it.sets128.is_empty() {
                                continue;
                            }
                            // The resident sets join the union and leave
                            // the acc — tcnt becomes this leg's authority
                            // (dcount's existing override channel).
                            let r64 = std::mem::take(&mut acc.users[li]);
                            if !r64.is_empty() {
                                it.sets64.push(r64);
                            }
                            let r128 = std::mem::take(&mut acc.tusers[li]);
                            if !r128.is_empty() {
                                it.sets128.push(r128);
                            }
                            elems += it.sets64.iter().map(|s| s.len() as u64).sum::<u64>()
                                + it.sets128.iter().map(|s| s.len() as u64).sum::<u64>();
                            idx.push((k.clone(), li));
                            items.push(it);
                        }
                    }
                    let p = crate::planner::partition_count(
                        elems.max(1) as usize,
                        16,
                        node.params.l2_bytes,
                        width,
                    );
                    let counts = pm_union_sets(ctx.pool, Some(node), items, p);
                    for ((k, li), n) in idx.into_iter().zip(counts) {
                        map.get_mut(&k).expect("pend key is resident").tcnt[li] = Some(n);
                    }
                    PARTMERGE_SET_SHARDS.store(p as u64, Relaxed);
                    PARTMERGE_SET_ELEMS_IN.store(elems, Relaxed);
                }
                PmArm::Serial => {
                    for m in wmaps {
                        for (k, a) in m {
                            match map.get_mut(&k) {
                                None => {
                                    map.insert(k, a);
                                }
                                Some(e) => {
                                    merge_gacc(e, a, &wops, true);
                                }
                            }
                        }
                    }
                }
            }
            if !spruns.is_empty() {
                let g = gauge.as_ref().expect("runs imply the gauge");
                spruns.sort();
                let mut i = 0usize;
                while i < spruns.len() {
                    let mut j = i + 1;
                    while j < spruns.len()
                        && spruns[j].0 == spruns[i].0
                        && spruns[j].1 == spruns[i].1
                    {
                        j += 1;
                    }
                    let (skey, leg) = (&spruns[i].0, spruns[i].1 as usize);
                    let acc: &mut GAcc = match skey[0] {
                        0 => map.get_mut(&skey[1..]).expect("spilled group is resident"),
                        1 => &mut gnull,
                        _ => &mut gnullb[skey[1] as usize],
                    };
                    let resident = std::mem::take(&mut acc.tusers[leg]);
                    acc.tcnt[leg] = Some(merge_wr_runs(&media, &spruns[i..j], resident, g.share));
                    i = j;
                }
            }
            let mut rows: Vec<GRow> = match grows {
                Some(r) => r,
                None => map
                    .into_iter()
                    .map(|(mut k, acc)| {
                        let b = if bucket.is_some() { k.pop().expect("bucket byte") } else { 0 };
                        GRow { key: Some(k), b, acc }
                    })
                    .collect(),
            };
            if gnull.c != 0 {
                rows.push(GRow { key: None, b: 0, acc: gnull });
            }
            for (b, acc) in gnullb.into_iter().enumerate() {
                if acc.c != 0 {
                    rows.push(GRow { key: None, b: b as u8, acc });
                }
            }
            let t_bound = std::time::Instant::now();
            match tk {
                Some(t) => select_groups(&mut rows, node, &keyspec, t),
                None => sort_count_rows(&mut rows, node.params.limit),
            }
            if arm != PmArm::Serial {
                PARTMERGE_BOUND_NS.store(t_bound.elapsed().as_nanos() as u64, Relaxed);
                crate::engine::phn(node, "pm_bound", t_bound);
            }
            let mut kb = BytesBuild::new();
            let mut hb: Vec<i64> = Vec::new();
            let mut aggs: Vec<AggOut> = node
                .agg
                .iter()
                .map(|a| match a.op {
                    AggOp::MinBytes => AggOut::B(a.out, BytesBuild::new()),
                    AggOp::CountStar | AggOp::CountDistinct => AggOut::I(Vec::new()),
                    op if is_word_fold(op) => AggOut::W(a.out, op, Vec::new()),
                    other => panic!("window grouped agg {other:?}"),
                })
                .collect();
            for row in rows.iter() {
                kb.push_opt(row.key.as_deref());
                hb.push(row.b as i64);
                let gacc = &row.acc;
                let (mut mi, mut ui, mut wi) = (0usize, 0usize, 0usize);
                for (i, a) in node.agg.iter().enumerate() {
                    match (&a.op, &mut aggs[i]) {
                        (AggOp::MinBytes, AggOut::B(_, b)) => {
                            b.push(&gacc.mins[mi]);
                            mi += 1;
                        }
                        (AggOp::CountStar, AggOut::I(v)) => v.push(gacc.c as i64),
                        (AggOp::CountDistinct, AggOut::I(v)) => {
                            v.push(dcount(gacc, ui));
                            ui += 1;
                        }
                        (_, AggOut::W(_, _, v)) => {
                            v.push(gacc.words[wi]);
                            wi += 1;
                        }
                        _ => unreachable!(),
                    }
                }
            }
            let kcol = kb.finish(bank.typ(key));
            let mut cols: Vec<AnswerCol> = match bucket {
                None => vec![kcol],
                Some(b) => {
                    let hcol = AnswerCol::i64s(TypMeta::INT8, hb);
                    if b.first { vec![hcol, kcol] } else { vec![kcol, hcol] }
                }
            };
            for ag in aggs {
                cols.push(match ag {
                    AggOut::I(v) => AnswerCol::i64s(TypMeta::INT8, v),
                    AggOut::B(ty, b) => b.finish(ty),
                    AggOut::W(ty, AggOp::Sum, v) => {
                        let mask: Vec<bool> = v.iter().map(|x| x.is_some()).collect();
                        let validity = if mask.iter().all(|&b| b) {
                            Validity::AllValid
                        } else {
                            Validity::Mask(mask)
                        };
                        let data = ColData::I128(v.into_iter().map(|x| x.unwrap_or(0)).collect());
                        AnswerCol { ty, data, validity }
                    }
                    AggOut::W(ty, _, v) => {
                        AnswerCol::i64s_opt(ty, v.into_iter().map(|x| x.map(|x| x as i64)).collect())
                    }
                });
            }
            AnswerSet::from_cols(cols)
        }
        Shape::FrameGroup => unreachable!(),
    }
}

/// Typed grouped-agg output lanes for the var-lane Grouped shape.
enum AggOut {
    I(Vec<i64>),
    B(TypMeta, BytesBuild),
    W(TypMeta, AggOp, Vec<Option<i128>>),
}

fn cmp_key_null_greatest(a: &Option<Vec<u8>>, b: &Option<Vec<u8>>) -> std::cmp::Ordering {
    match (a, b) {
        (Some(x), Some(y)) => x.cmp(y),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

enum GCell<'a> {
    Null,
    I(i128),
    B(&'a [u8]),
}

struct GRow {
    key: Option<Vec<u8>>,
    b: u8,
    acc: GAcc,
}

#[derive(Clone, Copy)]
struct KeySpec {
    nkeys: usize,
    bucket_first: bool,
}

/// Answer column `col` of a group row (key columns first, then agg i).
fn group_cell<'a>(row: &'a GRow, ks: KeySpec, lanes: &[(AggOp, usize)], col: usize) -> GCell<'a> {
    if col < ks.nkeys {
        let is_bucket = ks.nkeys == 2 && (col == 0) == ks.bucket_first;
        if is_bucket {
            return GCell::I(row.b as i128);
        }
        return match &row.key {
            Some(k) => GCell::B(k),
            None => GCell::Null,
        };
    }
    let (op, j) = lanes[col - ks.nkeys];
    let g = &row.acc;
    match op {
        AggOp::CountStar => GCell::I(g.c as i128),
        AggOp::CountDistinct => GCell::I(dcount(g, j) as i128),
        AggOp::MinBytes => GCell::B(&g.mins[j]),
        _ => match g.words[j] {
            Some(x) => GCell::I(x),
            None => GCell::Null,
        },
    }
}

fn key_cell<'a>(row: &'a GRow, ks: KeySpec, lanes: &[(AggOp, usize)], k: &TopKKey) -> GCell<'a> {
    let Some(lo) = k.lo else { return group_cell(row, ks, lanes, k.col as usize) };
    match (group_cell(row, ks, lanes, k.col as usize), group_cell(row, ks, lanes, lo as usize)) {
        (GCell::I(x), GCell::I(y)) => GCell::I(x - y),
        _ => GCell::Null,
    }
}

/// Top-`n` groups under the pushed keys (`answer::cmp_rows`' law),
/// selected off the group table. [q45 partmerge, identity law] The cmp
/// is made TOTAL (ties settle on the group identity) and the window
/// emits IN that order — the canonical ordering both combine arms
/// share, and the property that lets the partitioned arm bound
/// per-owner (top-n of a union = top-n of the per-partition top-n's
/// under a total order).
fn select_groups(rows: &mut Vec<GRow>, node: &PlanNode, ks: &KeySpec, t: &TopK) {
    use std::cmp::Ordering;
    let n = t.n.min(rows.len());
    let mut lanes: Vec<(AggOp, usize)> = Vec::with_capacity(node.agg.len());
    let (mut mi, mut ui, mut wi) = (0usize, 0usize, 0usize);
    let next = |c: &mut usize| {
        *c += 1;
        *c - 1
    };
    for a in &node.agg {
        let idx = match a.op {
            AggOp::CountStar => 0,
            AggOp::MinBytes => next(&mut mi),
            AggOp::CountDistinct => next(&mut ui),
            _ => next(&mut wi),
        };
        lanes.push((a.op, idx));
    }
    let cmp = |a: &GRow, b: &GRow| -> Ordering {
        for k in &t.keys {
            let (ca, cb) = (key_cell(a, *ks, &lanes, k), key_cell(b, *ks, &lanes, k));
            let ord = match (ca, cb) {
                (GCell::Null, GCell::Null) => Ordering::Equal,
                (GCell::Null, _) => {
                    if k.nulls_first { Ordering::Less } else { Ordering::Greater }
                }
                (_, GCell::Null) => {
                    if k.nulls_first { Ordering::Greater } else { Ordering::Less }
                }
                (GCell::I(x), GCell::I(y)) => {
                    let o = x.cmp(&y);
                    if k.desc { o.reverse() } else { o }
                }
                (GCell::B(x), GCell::B(y)) => {
                    let o = x.cmp(y);
                    if k.desc { o.reverse() } else { o }
                }
                _ => unreachable!("group cell class mismatch (bug)"),
            };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        // [q45 partmerge] the pushed keys need not be total over groups;
        // ties settle on the group identity (key bytes NULL-greatest,
        // then bucket) — pure per-row, so the selected SET and the
        // emitted order are independent of combine input order.
        cmp_key_null_greatest(&a.key, &b.key).then_with(|| a.b.cmp(&b.b))
    };
    if n > 0 && n < rows.len() {
        rows.select_nth_unstable_by(n - 1, |a, b| cmp(a, b));
    }
    rows.truncate(n);
    // Canonical emit order (identity law; n is answer-sized).
    rows.sort_by(|a, b| cmp(a, b));
}

/// The sidecar column: a MinBytes agg over the DRIVING varlena column
/// (hot-shape MIN(URL)-from-dict-entry law) — codes ride the cache plane.
fn sidecar_col_of(node: &PlanNode) -> Option<u32> {
    let pred = node.pred.as_ref()?;
    let driving = pred.var_terms.first()?.col;
    node.agg
        .iter()
        .any(|a| a.op == AggOp::MinBytes && a.col == Some(driving))
        .then_some(driving)
}

/// One full var-lane scan: stage-1 verdict + staged residues + consume,
/// capturing verdicts (+ sidecar codes) for publication when `record`.
///
/// [sqe-m4] Int conjuncts (frame + residues) compose with the var lane:
/// the SMA faces classify each granule first (all-fail => Skip before the
/// verdict fold — the zone x verdict interplay); granules the zone cannot
/// decide decode_sel their predicate columns over the stage-1 survivors.
fn scan_var(
    ctx: &SqeCtx,
    node: &PlanNode,
    shape: &Shape,
    vw: &VerdictWords,
    units: &[Unit],
    record: bool,
    gauge: &Option<Arc<WrGauge>>,
) -> Vec<WState> {
    let (bank, pool) = (ctx.bank, ctx.pool);
    let pred = node.pred.as_ref().unwrap();
    let terms = &pred.var_terms;
    let others = other_cols(node, shape);
    let driving = terms[0].col;
    let sc = sidecar_col_of(node);
    let keep_codes = sc.is_some();
    let iterms: Vec<PredTerm> =
        pred.frame().iter().chain(pred.residues()).cloned().collect();
    let iwidths: Vec<u8> = iterms.iter().map(|t| col_width(bank, t.col)).collect();
    let smas: Vec<std::sync::Arc<crate::kernels_dec::SmaFlat>> =
        iterms.iter().map(|t| ctx.faces.sma(bank, t.col)).collect();
    // [psma-consume] §8.2 candidate-slice faces per int conjunct (None
    // under the kill switch / uncovered column — consult degrades).
    let psmas: Vec<_> = iterms.iter().map(|t| ctx.faces.psma(bank, t.col)).collect();
    for t in &iterms {
        assert_eq!(
            ctx.faces.walk(bank, t.col).len(),
            units.len(),
            "granule walks must align across columns"
        );
    }
    let iterms = &iterms;
    let iwidths = &iwidths;
    let smas = &smas;
    let psmas = &psmas;
    let states = pool.run_finish(
        units.len(),
        |_| WState::new(driving, &others, gauge.clone()),
        |s, i| {
            let (pi, g, rows32, base) = units[i];
            // zone consult for the int conjuncts (Skip / AllPass / Partial)
            let mut zone_partial: Vec<usize> = Vec::new();
            for (ti, t) in iterms.iter().enumerate() {
                let (lo, hi) = (smas[ti].mins[i], smas[ti].maxs[i]);
                if !t.zone_may_pass(lo, hi) {
                    if record {
                        s.rec.push((i, CVerdict::Skip, Vec::new()));
                    }
                    return;
                }
                if !t.zone_all_pass(lo, hi) {
                    zone_partial.push(ti);
                }
            }
            // [psma-consume] Zone said maybe: intersect the undecided int
            // conjuncts' candidate slices at granule grain (one probe per
            // zone-partial term, never per-row); an empty window proves
            // the conjunction empty before the var lane decodes anything.
            let win = zone_partial.iter().fold((0usize, rows32 as usize), |w, &ti| {
                crate::psmaface::narrow(
                    w,
                    psmas[ti].as_ref().and_then(|pf| {
                        pf.slice(pi, g, rows32, smas[ti].mins[i], smas[ti].maxs[i], &iterms[ti])
                    }),
                )
            });
            if win.0 >= win.1 {
                if record {
                    s.rec.push((i, CVerdict::encode(Vec::new(), rows32 as usize), Vec::new()));
                }
                return;
            }
            // [psma-consume, oracle] slice-complement emptiness gate over
            // the surviving granules (the scan_serve convention: the
            // empty-skip fires before any decode, checked granules pay a
            // full conjunct-column decode — oracle builds only).
            #[cfg(feature = "oracle")]
            for &ti in &zone_partial {
                let t = &iterms[ti];
                if let Some(sl) = psmas[ti].as_ref().and_then(|pf| {
                    pf.slice(pi, g, rows32, smas[ti].mins[i], smas[ti].maxs[i], t)
                }) {
                    let mut cur = crate::scan::open_cursor(bank, pi, t.col);
                    let mut scr = crate::scan::scratch_fetch();
                    let d = scr.decode_full(&mut cur, g, rows32 as usize).to_vec();
                    crate::scan::scratch_park(scr);
                    crate::psmaface::oracle_check_complement(
                        t,
                        sl.0 as usize,
                        (sl.1 as usize).min(rows32 as usize),
                        rows32 as usize,
                        |_| true,
                        |r| sx(d[r], iwidths[ti]),
                    );
                }
            }
            stage1_select(bank, vw, &terms[0], s, pi, g, rows32 as usize, keep_codes);
            var_residue_filter(bank, terms, s, pi, g, keep_codes);
            null_veto(bank, &terms[..1], s, pi, g, rows32 as usize, keep_codes);
            // [psma-consume] Stage-1 survivors outside the PSMA window
            // cannot satisfy the int conjunction — drop them (with their
            // sidecar codes, position-aligned) before any decode_sel.
            if win.0 > 0 || win.1 < rows32 as usize {
                let mut w = 0usize;
                for k in 0..s.sel.len() {
                    let r = s.sel[k] as usize;
                    if r >= win.0 && r < win.1 {
                        s.sel[w] = s.sel[k];
                        if keep_codes {
                            s.codes[w] = s.codes[k];
                        }
                        w += 1;
                    }
                }
                s.sel.truncate(w);
            }
            // int conjunct filter over the survivors (decode_sel per col)
            for &ti in &zone_partial {
                if s.sel.is_empty() {
                    break;
                }
                let t = &iterms[ti];
                let sel = std::mem::take(&mut s.sel);
                let mut keep = std::mem::take(&mut s.sel2);
                keep.clear();
                let mut kk = 0usize;
                {
                    let io = s.ios.iter_mut().find(|io| io.attno == t.col).expect("int io");
                    let d = io.scr.decode_sel(io.cur.get(bank, pi), g, &sel);
                    for (k, &r) in sel.iter().enumerate() {
                        if t.eval(sx(d[k], iwidths[ti])) {
                            keep.push(r);
                            if keep_codes {
                                s.codes[kk] = s.codes[k];
                                kk += 1;
                            }
                        }
                    }
                }
                s.sel = keep;
                s.sel2 = sel;
            }
            let codes: Vec<u32> =
                if keep_codes { s.codes[..s.sel.len()].to_vec() } else { Vec::new() };
            consume_var(ctx, node, shape, s, pi, g, base, sc, &codes);
            if record {
                let v = CVerdict::encode(s.sel.clone(), rows32 as usize);
                s.rec.push((i, v, codes));
            }
        },
        |mut s| {
            park_ios(&mut s.ios);
            s
        },
    );
    states
}

/// Publish the recorded plane (+ sidecar) from scan states under `fp`
/// (the FULL conjunction identity when int terms compose with the var
/// lane; the shared frame identity otherwise — they coincide when the
/// predicate is pure-var).
fn publish_var(ctx: &SqeCtx, node: &PlanNode, fp: &ConjFp, units: std::sync::Arc<Vec<Unit>>, states: &mut [WState]) {
    let n = units.len();
    let mut v: Vec<CVerdict> = (0..n).map(|_| CVerdict::Skip).collect();
    let mut codes: Vec<Vec<u32>> = vec![Vec::new(); n];
    let mut survivors = 0u64;
    let mut any_codes = false;
    for s in states.iter_mut() {
        for (i, cv, cd) in s.rec.drain(..) {
            survivors += match &cv {
                CVerdict::Skip => 0,
                CVerdict::AllPass => units[i].2 as u64,
                CVerdict::Rows(r) => r.len() as u64,
                CVerdict::Bitmap(w) => w.iter().map(|x| x.count_ones() as u64).sum(),
            };
            if !cd.is_empty() {
                any_codes = true;
                codes[i] = cd;
            }
            v[i] = cv;
        }
    }
    let sidecar = any_codes.then_some(CacheSidecar::Codes(codes));
    crate::exec::publish_cache_at(ctx, node, fp, units, v, survivors, sidecar);
}

/// Warm replay over the cached plane: decode_sel only the consume set.
fn replay_var(
    ctx: &SqeCtx,
    node: &PlanNode,
    shape: &Shape,
    cache: &crate::engine::PredCache,
    gauge: &Option<Arc<WrGauge>>,
) -> AnswerSet {
    let pool = ctx.pool;
    // [sqe-m2] Count shape: the plane's exact survivor total IS the answer
    // (no residues exist in the var lane by construction) — the floor's
    // 12us hot-shape replay never touches data, and neither should this.
    if matches!(shape, Shape::Count) {
        return AnswerSet::from_cols(vec![AnswerCol::i64s(
            TypMeta::INT8,
            vec![cache.survivors as i64],
        )]);
    }
    let pred = node.pred.as_ref().unwrap();
    let driving = pred.var_terms[0].col;
    let others = other_cols(node, shape);
    let sc = sidecar_col_of(node);
    let live: Vec<usize> = (0..cache.units.len())
        .filter(|&i| !matches!(cache.v[i], CVerdict::Skip))
        .collect();
    // [sqe-m2] Survivor-work estimate = the plane's exact survivor total
    // (var-lane planes are FINAL) — the granule-rows sum overestimated by
    // ~10x and elected 96T on serial-sized work (hot-shape replay tax).
    let rows_est: u64 = cache.survivors;
    let t = elect_threads(
        node.params.goal.claim_class.unwrap_or(ClaimClass::SkipDominated),
        live.len(),
        rows_est,
        pool.threads(),
    );
    let scodes: Option<&Vec<Vec<u32>>> = match &cache.sidecar {
        Some(CacheSidecar::Codes(c)) => Some(c),
        None => None,
    };
    let work = |s: &mut WState, k: usize| {
        let i = live[k];
        let (pi, g, rows32, base) = cache.units[i];
        let mut buf = std::mem::take(&mut s.rowbuf);
        if let Some(rows) = verdict_rows(&cache.v[i], rows32, &mut buf) {
            s.sel.clear();
            s.sel.extend_from_slice(rows);
            let codes: &[u32] = scodes.map(|c| c[i].as_slice()).unwrap_or(&[]);
            consume_var(ctx, node, shape, s, pi, g, base, sc, codes);
        }
        s.rowbuf = buf;
    };
    let states = if t <= 1 {
        // The §1.6 serial election: survivor work under the cutoff band.
        let mut s = WState::new(driving, &others, gauge.clone());
        for k in 0..live.len() {
            work(&mut s, k);
        }
        park_ios(&mut s.ios);
        vec![s]
    } else {
        pool.run_finish(
            live.len(),
            |_| WState::new(driving, &others, gauge.clone()),
            work,
            |mut s| {
                park_ios(&mut s.ios);
                s
            },
        )
    };
    render_var(ctx, node, shape, states)
}

// ---------------------------------------------------------------------------
// FRAME lane
// ---------------------------------------------------------------------------

/// Locate the shared-frame pair (Eq counter, Between date) in the frame
/// terms; every other conjunct (frame or residue) evaluates row-grain.
fn frame_pair<'a>(pred: &'a PredSpec) -> (Option<(&'a PredTerm, &'a PredTerm)>, Vec<&'a PredTerm>) {
    let mut eqs: Option<&PredTerm> = None;
    let mut rng: Option<&PredTerm> = None;
    let mut rest: Vec<&PredTerm> = Vec::new();
    for t in pred.frame() {
        match t.op {
            CmpOp::Eq if eqs.is_none() && t.lo != 0 => eqs = Some(t),
            CmpOp::Between if rng.is_none() => rng = Some(t),
            _ => rest.push(t),
        }
    }
    rest.extend(pred.residues());
    match (eqs, rng) {
        (Some(e), Some(r)) => (Some((e, r)), rest),
        _ => {
            let mut all: Vec<&PredTerm> = pred.frame().iter().collect();
            all.extend(pred.residues());
            (None, all)
        }
    }
}

/// Evaluate residue conjuncts for the rows of `sel` over decoded columns
/// (`cols[j]` is gathered per sel POSITION). Term-major (R2): filter the
/// position list — one CmpOp match per (granule, term) driving a
/// monomorphic compaction loop — then map surviving positions back to
/// row ids. Same conjunct order as the row-major walk.
fn eval_residues(
    res: &[&PredTerm],
    widths: &[u8],
    cols: &[&[u64]],
    sel: &[u16],
    out: &mut Vec<u16>,
) {
    out.clear();
    out.extend((0..sel.len()).map(|k| k as u16));
    for (j, t) in res.iter().enumerate() {
        let (d, w) = (cols[j], widths[j]);
        t.filter_sel(out, |_| true, |k| sx(d[k], w));
    }
    for k in out.iter_mut() {
        *k = sel[*k as usize];
    }
}

fn run_frame_lane(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    let (bank, pool) = (ctx.bank, ctx.pool);
    let t_all = std::time::Instant::now();
    let pred = node.pred.as_ref().expect("frame lane pred");
    let (pair, residues) = frame_pair(pred);
    let kcols: Vec<u32> = node.params.group_cols.clone();
    let kinds: Vec<KKind> = kcols.iter().map(|&c| kkind(bank, c)).collect();
    let packed_w = pack_spec(&kinds);
    let packed = packed_w.is_some();

    // Merge worker maps (packed-int lane rebuilds its group bytes ONCE per
    // group here) and render under the kernels_f6 rank-window law. The
    // first byte-keyed map is MOVED, not re-hashed.
    let finish = |states: Vec<(WState, Vec<ColIO>)>| -> FxMap {
        let mut map = FxMap::default();
        let w = packed_w.unwrap_or(0);
        for (mut s, _) in states {
            if map.is_empty() {
                map = std::mem::take(&mut s.fmap);
            } else {
                for (k, c) in s.fmap.drain() {
                    *map.entry(k).or_insert(0) += c;
                }
            }
            for (ik, c) in s.imap.drain() {
                *map.entry(ik.to_le_bytes()[..w].to_vec()).or_insert(0) += c;
            }
        }
        map
    };

    // [sqe-m2] WARM, final plane first (frame+residues applied): hot reps
    // decode ONLY key columns over final survivors — the floor condcache
    // law. Falls back to the shared FRAME plane (residues re-evaluated).
    let full_fp = pred.full_fingerprint();
    let (cache, resid_applied) = match crate::exec::replay_cache_at(ctx, node, &full_fp) {
        Some(c) => (Some(c), true),
        None => (replay_cache(ctx, node), false),
    };
    if let Some(cache) = cache {
        let t_w = std::time::Instant::now();
        let residues_eff: Vec<&PredTerm> =
            if resid_applied { Vec::new() } else { residues.clone() };
        // [sqe-q2426] Full-plane recurrence from the WARM frame arm: the
        // residue pass just computed the FINAL survivor rowlists — the
        // exact payload the cold path publishes under the full
        // fingerprint. Without this, a query whose shared FRAME plane was
        // already resident at its first execution (a sibling hot-shape
        // query published it) never runs cold again, so its full plane
        // never witnesses recurrence and every hot execution re-evaluates
        // residues + re-decodes keys over the whole frame. Publication
        // admission (populate law, density cap, goal membership) is
        // publish_cache_at's, unchanged.
        // Attempt cap: the populate law admits at the second touch, so a
        // plane still unpublished after a few attempts is density-refused
        // (stable per plane) — stop paying the payload collection.
        let record_full = !resid_applied
            && !residues.is_empty()
            && node.params.goal.fingerprints.iter().any(|f| *f == full_fp)
            && ctx.faces.touch_count(&full_fp) < 6;
        let rcols: Vec<u32> = residues_eff.iter().map(|t| t.col).collect();
        let rwidths: Vec<u8> = rcols.iter().map(|&c| col_width(bank, c)).collect();
        let live: Vec<usize> = (0..cache.units.len())
            .filter(|&i| !matches!(cache.v[i], CVerdict::Skip))
            .collect();
        // [sqe-m2] §1.6 serial gate for the replay: the floor's elected
        // condcache replay is SERIAL at these survivor volumes — the pool
        // generation + per-worker init costs more than the work. rows_est
        // is the plane's survivor total when residues are pre-applied.
        let rows_est = if resid_applied {
            cache.survivors
        } else {
            live.iter().map(|&i| cache.units[i].2 as u64).sum()
        };
        let t = elect_threads(
            node.params.goal.claim_class.unwrap_or(ClaimClass::SkipDominated),
            live.len(),
            rows_est,
            pool.threads(),
        );
        let work = |s: &mut WState, rios: &mut Vec<ColIO>, k: usize| {
            let i = live[k];
            let (pi, g, rows32, _) = cache.units[i];
            let mut buf = std::mem::take(&mut s.rowbuf);
            let Some(rows) = verdict_rows(&cache.v[i], rows32, &mut buf) else {
                s.rowbuf = buf;
                return;
            };
            let mut surv = std::mem::take(&mut s.sel);
            if residues_eff.is_empty() {
                surv.clear();
                surv.extend_from_slice(rows);
            } else {
                let rvals: Vec<Vec<u64>> = rios
                    .iter_mut()
                    .map(|io| io.scr.decode_sel(io.cur.get(bank, pi), g, rows).to_vec())
                    .collect();
                let rrefs: Vec<&[u64]> = rvals.iter().map(|v| v.as_slice()).collect();
                eval_residues(&residues_eff, &rwidths, &rrefs, rows, &mut surv);
            }
            if !surv.is_empty() {
                let kvals: Vec<Vec<u64>> = kcols
                    .iter()
                    .map(|&c| {
                        let io = s.io(c);
                        io.scr.decode_sel(io.cur.get(bank, pi), g, &surv).to_vec()
                    })
                    .collect();
                let krefs: Vec<&[u64]> = kvals.iter().map(|v| v.as_slice()).collect();
                fold_group_keys(s, &kinds, packed, &krefs, surv.len());
            }
            if record_full {
                s.rec.push((i, CVerdict::encode(surv.clone(), rows32 as usize), Vec::new()));
            }
            s.sel = surv;
            s.rowbuf = buf;
        };
        let mk = || {
            (
                WState::new(kcols.first().copied().unwrap_or(0), &kcols, None),
                rcols.iter().map(|&c| ColIO::new(c)).collect::<Vec<ColIO>>(),
            )
        };
        let states = if t <= 1 {
            let (mut s, mut rios) = mk();
            for k in 0..live.len() {
                work(&mut s, &mut rios, k);
            }
            park_ios(&mut s.ios);
            park_ios(&mut rios);
            vec![(s, rios)]
        } else {
            pool.run_finish(
                live.len(),
                |_| mk(),
                |(s, rios), k| work(s, rios, k),
                |(mut s, mut rios)| {
                    park_ios(&mut s.ios);
                    park_ios(&mut rios);
                    (s, rios)
                },
            )
        };
        let mut states = states;
        if record_full {
            let recs: Vec<(usize, CVerdict, Vec<u32>)> =
                states.iter_mut().flat_map(|(s, _)| s.rec.drain(..)).collect();
            let mut fv: Vec<CVerdict> =
                (0..cache.units.len()).map(|_| CVerdict::Skip).collect();
            let mut fsurvivors = 0u64;
            for (i, cv, _) in recs {
                fsurvivors += match &cv {
                    CVerdict::Skip => 0,
                    CVerdict::AllPass => cache.units[i].2 as u64,
                    CVerdict::Rows(rl) => rl.len() as u64,
                    CVerdict::Bitmap(w) => w.iter().map(|x| x.count_ones() as u64).sum(),
                };
                fv[i] = cv;
            }
            crate::exec::publish_cache_at(
                ctx,
                node,
                &full_fp,
                cache.units.clone(),
                fv,
                fsurvivors,
                None,
            );
        }
        crate::engine::phn(
            node,
            if resid_applied { "warm_scan" } else { "warm_frame_scan" },
            t_w,
        );
        let t_m = std::time::Instant::now();
        // Single-state packed replay: render straight off the int table —
        // no byte-map materialization at all.
        let out = if packed && states.len() == 1 && states[0].0.fmap.is_empty() {
            let out = render_frame_packed(
                bank,
                node,
                &kcols,
                &kinds,
                packed_w.unwrap(),
                &states[0].0.imap,
            );
            crate::engine::phn(node, "render_packed", t_m);
            out
        } else {
            let map = finish(states);
            crate::engine::phn(node, "warm_merge", t_m);
            let t_r = std::time::Instant::now();
            let out = render_frame(bank, node, &kcols, &kinds, &map);
            crate::engine::phn(node, "render", t_r);
            out
        };
        crate::engine::phn(node, "lane_total", t_all);
        return out;
    }

    // COLD/HONEST: shared-frame memo (standing face) or full walk; residue
    // filter + key fold fused; the FRAME rowlists published as the shared
    // plane AND the residue-final rowlists under the full fingerprint.
    let t_c = std::time::Instant::now();
    let rcols: Vec<u32> = residues.iter().map(|t| t.col).collect();
    let rwidths: Vec<u8> = rcols.iter().map(|&c| col_width(bank, c)).collect();
    let (e, r) = pair.expect("frame lane needs the (counter, date-range) pair");
    let frame = shared_frame(ctx.faces, bank, e.col, r.col, e.lo, r.lo, r.hi);
    let units = ctx.faces.walk(bank, e.col);
    let record_final = !residues.is_empty()
        && node.params.goal.fingerprints.iter().any(|f| *f == full_fp);
    let states = pool.run_finish(
        frame.granules.len(),
        |_| {
            (
                WState::new(kcols.first().copied().unwrap_or(0), &kcols, None),
                rcols.iter().map(|&c| ColIO::new(c)).collect::<Vec<ColIO>>(),
            )
        },
        |(s, rios), i| {
            let fg = &frame.granules[i];
            let rvals: Vec<Vec<u64>> = rios
                .iter_mut()
                .map(|io| io.scr.decode_sel(io.cur.get(bank, fg.pi), fg.g, &fg.rowlist).to_vec())
                .collect();
            let rrefs: Vec<&[u64]> = rvals.iter().map(|v| v.as_slice()).collect();
            let mut surv = std::mem::take(&mut s.sel);
            eval_residues(&residues, &rwidths, &rrefs, &fg.rowlist, &mut surv);
            if !surv.is_empty() {
                let kvals: Vec<Vec<u64>> = kcols
                    .iter()
                    .map(|&c| {
                        let io = s.io(c);
                        io.scr.decode_sel(io.cur.get(bank, fg.pi), fg.g, &surv).to_vec()
                    })
                    .collect();
                let krefs: Vec<&[u64]> = kvals.iter().map(|v| v.as_slice()).collect();
                fold_group_keys(s, &kinds, packed, &krefs, surv.len());
            }
            // publication payload: the FRAME rowlist (shared hot-shape law)
            // + the residue-final rowlist (this plan's full identity).
            s.rec.push((fg.ord, CVerdict::encode(fg.rowlist.clone(), fg.rows as usize), Vec::new()));
            if record_final {
                let fv = CVerdict::encode(surv.clone(), fg.rows as usize);
                s.rec.push((usize::MAX - fg.ord, fv, Vec::new()));
            }
            s.sel = surv;
        },
        |(mut s, mut rios)| {
            park_ios(&mut s.ios);
            park_ios(&mut rios);
            (s, rios)
        },
    );
    let mut v: Vec<CVerdict> = (0..units.len()).map(|_| CVerdict::Skip).collect();
    let mut fv: Vec<CVerdict> = if record_final {
        (0..units.len()).map(|_| CVerdict::Skip).collect()
    } else {
        Vec::new()
    };
    let mut survivors = 0u64;
    let mut fsurvivors = 0u64;
    let mut states = states;
    let recs: Vec<(usize, CVerdict, Vec<u32>)> =
        states.iter_mut().flat_map(|(s, _)| s.rec.drain(..)).collect();
    let map = finish(states);
    {
        for (ord, cv, _) in recs {
            let (slot, tally, target) = if ord > units.len() {
                (usize::MAX - ord, &mut fsurvivors, &mut fv)
            } else {
                (ord, &mut survivors, &mut v)
            };
            *tally += match &cv {
                CVerdict::Skip => 0,
                CVerdict::AllPass => units[slot].2 as u64,
                CVerdict::Rows(rl) => rl.len() as u64,
                CVerdict::Bitmap(w) => w.iter().map(|x| x.count_ones() as u64).sum(),
            };
            target[slot] = cv;
        }
    }
    if record_final {
        crate::exec::publish_cache_at(ctx, node, &full_fp, units.clone(), fv, fsurvivors, None);
    }
    crate::exec::publish_cache(ctx, node, units, v, survivors);
    crate::engine::phn(node, "cold", t_c);
    render_frame(bank, node, &kcols, &kinds, &map)
}

// ---------------------------------------------------------------------------
// entry point
// ---------------------------------------------------------------------------

pub fn run_window_replay(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    let (bank, pool) = (ctx.bank, ctx.pool);
    let shape = shape_of(node);
    if matches!(shape, Shape::FrameGroup) {
        return run_frame_lane(ctx, node);
    }
    let pred = node.pred.as_ref().expect("window replay needs a pred");
    let gauge = wr_gauge(ctx, node);
    // [sqe-m4] the cached plane's identity: pure-var predicates publish
    // under the shared frame fingerprint (hot-shape share one entry);
    // var+int compositions store the FINAL survivors, whose identity is
    // the full conjunction.
    let has_int = !(pred.frame().is_empty() && pred.residues().is_empty());
    let plane_fp =
        if has_int { pred.full_fingerprint() } else { pred.frame_fingerprint() };
    if let Some(cache) = crate::exec::replay_cache_at(ctx, node, &plane_fp) {
        return replay_var(ctx, node, &shape, &cache, &gauge);
    }
    let units = ctx.faces.walk(bank, pred.var_terms[0].col);
    let honest = node.params.flags & F_HONEST != 0;
    // [R6d] Guard-lowered row grain: an EMPTY verdict face (ncodes = 0
    // for every part) routes stage-1 through the scalar per-row fallback
    // for every part — the exhaustive row plane, same survivors, same
    // published plane identity. No dict payload is touched.
    if node.params.flags & F_ROW_GRAIN != 0 {
        let vw = VerdictWords::from_ncodes(
            pred.var_terms[0].col,
            &vec![0u32; bank.parts.len()],
        );
        let mut states = scan_var(ctx, node, &shape, &vw, &units, true, &gauge);
        publish_var(ctx, node, &plane_fp, units.clone(), &mut states);
        return render_var(ctx, node, &shape, states);
    }
    // Honest arm: recompute the dict-entry verdict bitwords per call (the
    // study's honest-twin law: the verdict WORK re-runs; the shared dict
    // handles serve both arms — the per-worker-opens arm was the
    // measured-settled control, deleted at port); elected cold record
    // reuses the standing face (built once per query run).
    let owned_vw;
    let arc_vw;
    let vw: &VerdictWords = if honest {
        let t1 = pred.var_terms[0].clone();
        let faces = ctx.faces.dicts_all(bank, pred.var_terms[0].col);
        let nc: Vec<u32> = faces.iter().map(|d| d.ncodes).collect();
        let mut w = VerdictWords::from_ncodes(pred.var_terms[0].col, &nc);
        let dhs: Vec<Option<std::sync::Arc<pgrc2_read::dicthandle::DictHandle>>> =
            faces.iter().map(|d| d.dh.clone()).collect();
        w.compute_with(&dhs, pool.threads(), move |b| t1.eval(b));
        owned_vw = w;
        &owned_vw
    } else {
        let t1 = pred.var_terms[0].clone();
        arc_vw = ctx.faces.verdict_words(
            bank,
            pred.var_terms[0].col,
            &pred.var_terms[0],
            pool.threads(),
            move |b| t1.eval(b),
        );
        &arc_vw
    };
    let mut states = scan_var(ctx, node, &shape, vw, &units, true, &gauge);
    publish_var(ctx, node, &plane_fp, units.clone(), &mut states);
    render_var(ctx, node, &shape, states)
}

// ---------------------------------------------------------------------------
// [q45 partmerge] combine-law tests (in-file mod — the cost_params idiom)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod pm_tests {
    use super::*;

    fn pool() -> crate::pool::Pool {
        crate::pool::Pool::new(4)
    }

    /// Deterministic value stream (no dev-dep; splitmix-class).
    fn rng(seed: &mut u64) -> u64 {
        *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let mut x = *seed;
        x ^= x >> 33;
        x = x.wrapping_mul(0xff51afd7ed558ccd);
        x ^ (x >> 33)
    }

    /// `collide` = hash-collision-heavy keys: a long shared prefix and a
    /// low-entropy tail keep many distinct keys in few FxHasher images'
    /// neighborhoods and few shards at small P.
    fn key_of(r: u64, collide: bool) -> Vec<u8> {
        let mut k = if collide { vec![0xAB; 24] } else { Vec::new() };
        k.extend_from_slice(&(r % 50_000).to_le_bytes());
        k
    }

    /// Synthetic worker maps: random overlapping keys with min-bytes,
    /// count, u64/fp128 distinct-set and min/sum word payloads.
    fn synth_states(nw: usize, per: usize, collide: bool, seed: u64) -> Vec<FxGMap> {
        let mut s = seed;
        (0..nw)
            .map(|_| {
                let mut m = FxGMap::default();
                for _ in 0..per {
                    let r = rng(&mut s);
                    let e = m.entry(key_of(r, collide)).or_insert_with(|| GAcc::fresh(1, 1, 2));
                    e.c += 1;
                    let v = rng(&mut s);
                    let b = v.to_le_bytes().to_vec();
                    if e.c == 1 || b < e.mins[0] {
                        e.mins[0] = b;
                    }
                    e.users[0].insert(v % 977);
                    e.tusers[0].insert(((v as u128) << 64) | r as u128);
                    fold_word(&mut e.words[0], AggOp::Min, (v % 100_000) as i128);
                    fold_word(&mut e.words[1], AggOp::Sum, (v % 1000) as i128);
                }
                m
            })
            .collect()
    }

    /// The serial combine of record (render_var's Serial arm law).
    fn serial_combine(maps: Vec<FxGMap>, wops: &[AggOp]) -> FxGMap {
        let mut map = FxGMap::default();
        for m in maps {
            for (k, a) in m {
                match map.get_mut(&k) {
                    None => {
                        map.insert(k, a);
                    }
                    Some(e) => {
                        merge_gacc(e, a, wops, true);
                    }
                }
            }
        }
        map
    }

    fn gacc_eq(a: &GAcc, b: &GAcc) -> bool {
        let s64 = |s: &FxSet64| {
            let mut v: Vec<u64> = s.iter().copied().collect();
            v.sort_unstable();
            v
        };
        let s128 = |s: &FxSet128| {
            let mut v: Vec<u128> = s.iter().copied().collect();
            v.sort_unstable();
            v
        };
        a.c == b.c
            && a.mins == b.mins
            && a.words == b.words
            && a.tcnt == b.tcnt
            && a.users.len() == b.users.len()
            && a.users.iter().zip(&b.users).all(|(x, y)| s64(x) == s64(y))
            && a.tusers.len() == b.tusers.len()
            && a.tusers.iter().zip(&b.tusers).all(|(x, y)| s128(x) == s128(y))
    }

    #[test]
    fn pm_group_combine_matches_serial() {
        let pool = pool();
        let wops = [AggOp::Min, AggOp::Sum];
        for (p, collide) in [(4usize, true), (64, false), (1, true)] {
            let maps = synth_states(8, 4000, collide, 42);
            let ser = serial_combine(synth_states(8, 4000, collide, 42), &wops);
            let (rows, gout) = pm_combine(&pool, None, maps, p, &wops, false, None);
            assert_eq!(gout as usize, ser.len());
            assert_eq!(rows.len(), ser.len());
            for r in &rows {
                let k = r.key.as_ref().unwrap();
                let e = ser.get(k.as_slice()).expect("group present in the serial result");
                assert!(gacc_eq(&r.acc, e), "acc mismatch at p={p} collide={collide}");
                assert_eq!(r.b, 0);
            }
        }
    }

    #[test]
    fn pm_group_combine_pops_bucket_byte() {
        let pool = pool();
        let wops = [AggOp::Min, AggOp::Sum];
        let mk = || {
            synth_states(3, 800, false, 7)
                .into_iter()
                .map(|m| {
                    // append an hour-bucket byte, the fold-time key law
                    m.into_iter()
                        .map(|(mut k, a)| {
                            let b = (k[0] % 24) as u8;
                            k.push(b);
                            (k, a)
                        })
                        .collect::<FxGMap>()
                })
                .collect::<Vec<_>>()
        };
        let ser = serial_combine(mk(), &wops);
        let (rows, _) = pm_combine(&pool, None, mk(), 8, &wops, true, None);
        assert_eq!(rows.len(), ser.len());
        for r in &rows {
            let mut full = r.key.clone().unwrap();
            full.push(r.b);
            let e = ser.get(full.as_slice()).expect("group present");
            assert!(gacc_eq(&r.acc, e));
        }
    }

    #[test]
    fn pm_union_sets_matches_serial() {
        let pool = pool();
        let mut seed = 9u64;
        for p in [2usize, 256] {
            let mut items = Vec::new();
            let mut want = Vec::new();
            for it in 0..5usize {
                let (mut sets64, mut sets128) = (Vec::new(), Vec::new());
                let (mut u, mut t) = (FxSet64::default(), FxSet128::default());
                for _ in 0..8 {
                    let (mut s64, mut s128) = (FxSet64::default(), FxSet128::default());
                    for _ in 0..(200 + it * 100) {
                        let v = rng(&mut seed) % 1500; // heavy cross-source overlap
                        s64.insert(v);
                        u.insert(v);
                        // collision-heavy: every element shares its high
                        // 64 bits — the shard fn must still spread them.
                        let f = (0xDEAD_BEEFu128 << 64) | v as u128;
                        s128.insert(f);
                        t.insert(f);
                    }
                    sets64.push(s64);
                    sets128.push(s128);
                }
                want.push((u.len() + t.len()) as i64);
                items.push(PmSetItem { sets64, sets128 });
            }
            assert_eq!(pm_union_sets(&pool, None, items, p), want, "p={p}");
        }
    }

    #[test]
    fn pm_floor_election() {
        let w = 64;
        let ec = pm_cutoff_entries(w);
        let sc = pm_cutoff_set_elems(w);
        // The derived E2-form floors at the c8g pool width of record:
        // q3-class (64×72 entries) stays serial, q4-class (~6m) partitions.
        assert!(ec > 64 * 72, "q3 must stay serial (ec={ec})");
        assert!(ec < 6_000_000, "q4 must partition (ec={ec})");
        assert_eq!(pm_elect(true, 64, w, false, ec, || 0), PmArm::Group);
        // Under the entry floor, q2-class set mass elects the set grain.
        assert_eq!(pm_elect(true, 64, w, false, 64 * 27, || sc), PmArm::Sets);
        // Small everything stays serial (q1/q3-class shapes untaxed).
        assert_eq!(pm_elect(true, 64, w, false, 64 * 72, || 0), PmArm::Serial);
        // Spilled engagements stay serial (run merge keys the one map).
        assert_eq!(pm_elect(true, 64, w, true, ec, || sc), PmArm::Serial);
        // Degenerate pools have no parallel body to win.
        assert_eq!(pm_elect(true, 1, w, false, ec, || sc), PmArm::Serial);
        assert_eq!(pm_elect(true, 64, 2, false, ec, || sc), PmArm::Serial);
    }

    #[test]
    fn pm_kill_switch_elects_serial() {
        // PGRUST_SQE_GROUP_PARTMERGE=0 feeds `on = false` — every input
        // lands Serial, which IS the pre-landing combine verbatim.
        assert_eq!(pm_elect(false, 64, 64, false, u64::MAX, || u64::MAX), PmArm::Serial);
    }
}
