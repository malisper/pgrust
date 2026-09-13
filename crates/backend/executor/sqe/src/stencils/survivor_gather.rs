//! survivor-gather stencil (hot-shape shape, SurvivorGather family): fused
//! filter → survivor gather → partition-owned fold, parametrized entirely
//! from the PlanNode (plans/clickbench.ron hot-shape — the lane-hot-shape
//! `gather_owned` winner, kernels_f4f5.rs:2737/3257, refactored generic).
//! Pipeline per part (pool, part-grain claim):
//!
//!   1. survivor list — the varlena `<> ''` frame term lowered per
//!      ENGINE-PLAN: dict parts compare fused-unpacked CODES against the
//!      empty-string code, the code zone answering AllPass/Skip before
//!      any unpack; non-dict parts fall back to hydrated payload tests.
//!      The SWAR walk is consulted and REFUSED (text lane, no int width —
//!      `swar_elected`), so the code-compare fold is the elected form.
//!   2. payload completion for SURVIVORS ONLY via the WordCol gather face
//!      (standing face; library decode_sel fallback per refused encoding).
//!   3. scatter under the L2 partition law (P from ndv_est stats — never
//!      a constant), partition-owned sentinel-table fold, zero merge;
//!      global top-(limit) under (count DESC, key ASC).
//!
//! Key layout is PLAN DATA: params.group_cols pack low-to-high from the
//! last column at their byval widths; the total bit width ELECTS u64 vs
//! u128 (a physical attribute, not a new stencil — hot-shape wider key is
//! this same code monomorphized).
//!
//! Condition-cache wiring (engine-owned, exec::replay_cache/publish_cache):
//! the honest/cold arm records the final survivor plane under
//! "c40:ne:empty" as a side effect of its fused loop; the elected arm
//! replays it PARALLEL, gathering survivor ordinals only.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::answer::{AnswerCol, AnswerSet, ColData, Validity};
use crate::bank::Bank;
use crate::engine::{CVerdict, PredCache, SqeCtx, Unit};
use crate::exec::{publish_cache, replay_cache};
use crate::fused::{FusedCodes, WordCol};
use crate::ir::{AggOp, PlanNode, VarOp};
use crate::kernels_f123::part_stats;
use crate::kernels_f4f5::{empty_filter, EmptyFilter};
use crate::scan::{varlena_payload, CurCache, Scratch};
use crate::stencils::col_width;
use crate::typmeta::TypMeta;

// ---------------------------------------------------------------------------
// Packed key abstraction (u64 / u128 — elected by total layout bits)
// ---------------------------------------------------------------------------

trait EKey: Copy + Eq + Ord + Send + Sync + std::hash::Hash + 'static {
    const MAXV: Self;
    const BITS: u32;
    fn from64(x: u64) -> Self;
    fn shl(self, s: u32) -> Self;
    fn or(self, o: Self) -> Self;
    /// Extract the field at (shift, bits) back to a u64.
    fn field(self, shift: u32, bits: u32) -> u64;
    fn hash64(self) -> u64;
}

impl EKey for u64 {
    const MAXV: u64 = u64::MAX;
    const BITS: u32 = 64;
    #[inline(always)]
    fn from64(x: u64) -> u64 {
        x
    }
    #[inline(always)]
    fn shl(self, s: u32) -> u64 {
        self << s
    }
    #[inline(always)]
    fn or(self, o: u64) -> u64 {
        self | o
    }
    #[inline(always)]
    fn field(self, shift: u32, bits: u32) -> u64 {
        let x = self >> shift;
        if bits >= 64 {
            x
        } else {
            x & ((1u64 << bits) - 1)
        }
    }
    #[inline(always)]
    fn hash64(self) -> u64 {
        self.wrapping_mul(0xD6E8_FEB8_6659_FD93).rotate_right(29)
    }
}

impl EKey for u128 {
    const MAXV: u128 = u128::MAX;
    const BITS: u32 = 128;
    #[inline(always)]
    fn from64(x: u64) -> u128 {
        x as u128
    }
    #[inline(always)]
    fn shl(self, s: u32) -> u128 {
        self << s
    }
    #[inline(always)]
    fn or(self, o: u128) -> u128 {
        self | o
    }
    #[inline(always)]
    fn field(self, shift: u32, bits: u32) -> u64 {
        let x = self >> shift;
        if bits >= 64 {
            x as u64
        } else {
            (x as u64) & ((1u64 << bits) - 1)
        }
    }
    #[inline(always)]
    fn hash64(self) -> u64 {
        let x = (self as u64) ^ ((self >> 64) as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        x.wrapping_mul(0xD6E8_FEB8_6659_FD93)
    }
}

/// Resolved layout: per-field (attno, bits, shift). Field order is the
/// plan's group order; the FIRST field owns the highest bits (the
/// handwritten hot-shape packing). Bits come from the column's byval width
/// (schema data, not query data).
struct Layout {
    fields: Vec<(u32, u32, u32)>, // (attno, bits, shift)
    total_bits: u32,
}

fn layout(bank: &Bank, group_cols: &[u32]) -> Layout {
    let mut fields = Vec::with_capacity(group_cols.len());
    let mut shift = 0u32;
    for &a in group_cols.iter().rev() {
        let w = col_width(bank, a);
        assert!(w > 0, "survivor_gather: varlena group keys take the bytes-domain family");
        let b = 8 * w as u32;
        fields.push((a, b, shift));
        shift += b;
    }
    fields.reverse();
    Layout { fields, total_bits: shift }
}

/// Width-derived key sign-fold (P1-1 fix of the survivor_gather.rs:145
/// WatchID render bug class: the PoC rendered w8 fields UNSIGNED, which
/// matched only because the data carries no negative WatchIDs — TypMeta
/// says int8 is signed, so the typed lane folds signed at every width;
/// byte-identical on the banks of record, correct on negatives).
#[inline(always)]
fn field_fold(unsigned: bool, bits: u32, x: u64) -> i64 {
    if unsigned {
        return x as i64;
    }
    match bits {
        16 => x as u16 as i16 as i64,
        32 => x as u32 as i32 as i64,
        _ => x as i64,
    }
}

// ---------------------------------------------------------------------------
// Elections (functions over stats — never constants)
// ---------------------------------------------------------------------------

/// L2 partition law for the gather family: the hot-shape-origin law shape
/// (planner::partition_count) at this family's measured budget — 64B/key
/// scatter slots against a 2MiB partition budget. [ruling 3] the upper
/// clamp is `cost_params::gather_partition_cap(width)` — "the pass-2
/// owner grain saturates the pool there" (the old `.min(2048)` comment)
/// as the formula it was describing: next_pow2(16·width), 2048 at the
/// 96-wide rig geometry (ron hot-shape P=1024 / hot-shape P=2048
/// measured cells). ndv_est from the standing stats faces.
fn l2_partitions(ctx: &SqeCtx, lay: &Layout) -> usize {
    let ndv_est = lay
        .fields
        .iter()
        .map(|&(a, _, _)| ctx.faces.stats(ctx.bank, a).ndv_est_sum())
        .max()
        .unwrap_or(0) as usize;
    let w = ctx.pool.threads();
    crate::planner::partition_count(ndv_est, 32, 2 * 1024 * 1024, w)
        .min(crate::cost_params::target().gather_partition_cap(w))
}

/// SWAR eligibility (ENGINE-PLAN predicate-lowering ladder): elect the
/// SWAR walk only when the lane is an int of ≤2 bytes AND expected
/// survivor density is below one per SWAR word. Text predicates (no int
/// lane) always refuse — consulted here so the lowering trace is visible.
fn swar_elected(lane_bytes: Option<u32>, survivor_frac_est: f64) -> bool {
    match lane_bytes {
        Some(b) if b <= 2 => survivor_frac_est < b as f64 / 8.0,
        _ => false,
    }
}

/// Payload-packing election from the stats plane: the (sum, avg) pair
/// packs into one u32 iff the sum column is a 0/1 flag and the avg column
/// fits 31 bits, both proven by part records over EVERY part.
static PACKABLE_MEMO: pgsync::Mutex<Option<HashMap<(crate::bank::BankIdent, u32, u32), bool>>> =
    pgsync::Mutex::new(None);

/// [ruling] per-query-run.
pub fn clear_memo() {
    if let Some(m) = PACKABLE_MEMO.lock().unwrap().as_mut() {
        m.clear();
    }
}

fn payload_packable(bank: &Bank, sum_col: u32, avg_col: u32) -> bool {
    // [sqe-m2] Memoized within a query run: the per-part stats sweep (511
    // parts x 2 columns) was a measured ~2.8ms of prep INSIDE the timed
    // region on every rep. [ruling] cleared per query run.
    // The proof is a per-part stats property of ONE bank: keyed by its
    // identity so a concurrent session's relation never answers for this one.
    let key = (bank.ident(), sum_col, avg_col);
    if let Some(&v) = PACKABLE_MEMO.lock().unwrap_or_else(|e| e.into_inner()).as_ref().and_then(|m| m.get(&key)) {
        return v;
    }
    let v = payload_packable_uncached(bank, sum_col, avg_col);
    PACKABLE_MEMO.lock()
        .unwrap_or_else(|e| e.into_inner())
        .get_or_insert_with(HashMap::new)
        .insert(key, v);
    v
}

fn payload_packable_uncached(bank: &Bank, sum_col: u32, avg_col: u32) -> bool {
    for pi in 0..bank.parts.len() {
        let (Some(rs), Some(ra)) = (part_stats(bank, pi, sum_col), part_stats(bank, pi, avg_col))
        else {
            return false;
        };
        let exact = pgrc2_format::meta::KeyKind::Exact.as_u8();
        if rs.key_kind != exact || ra.key_kind != exact {
            return false;
        }
        if rs.min_key < 0 || rs.max_key > 1 || ra.min_key < 0 || ra.max_key >= (1 << 31) {
            return false;
        }
    }
    true
}

// ---------------------------------------------------------------------------
// The stencil
// ---------------------------------------------------------------------------

/// Per-thread pass-1 state (allocations persist across reps via the
/// PERSIST slots; contents are truncated per call — the honest law).
struct GState<K> {
    keys: Vec<Vec<K>>,
    pays: Vec<Vec<u32>>,
    sel: Vec<u16>,
    cols: Vec<Vec<u64>>,
    fb: Option<Box<(Scratch, Vec<Scratch>)>>,
    wide: HashMap<K, (u64, u64, u64)>,
    survivors: u64,
    bad_key: u64,
    verdicts: Vec<(usize, CVerdict)>,
}

/// Sentinel-keyed presized owner table (the OaSel shape).
struct OwnTab<K: EKey> {
    keys: Vec<K>,
    cnt: Vec<u32>,
    sr: Vec<u32>,
    sw: Vec<u64>,
    mask: usize,
}

impl<K: EKey> OwnTab<K> {
    fn new(entries: usize) -> OwnTab<K> {
        let cap = (entries * 2).next_power_of_two().max(16);
        OwnTab {
            keys: vec![K::MAXV; cap],
            cnt: vec![0; cap],
            sr: vec![0; cap],
            sw: vec![0; cap],
            mask: cap - 1,
        }
    }
    fn reset(&mut self, entries: usize) {
        let cap = (entries * 2).next_power_of_two().max(16);
        if cap > self.keys.len() {
            self.keys.resize(cap, K::MAXV);
            self.cnt.resize(cap, 0);
            self.sr.resize(cap, 0);
            self.sw.resize(cap, 0);
        }
        self.keys[..cap].fill(K::MAXV);
        self.mask = cap - 1;
    }
    #[inline(always)]
    fn add(&mut self, key: K, sr: u32, sw: u64) {
        let mut slot = (key.hash64() as usize) & self.mask;
        loop {
            let k = self.keys[slot];
            if k == key {
                self.cnt[slot] += 1;
                self.sr[slot] += sr;
                self.sw[slot] += sw;
                return;
            }
            if k == K::MAXV {
                self.keys[slot] = key;
                self.cnt[slot] = 1;
                self.sr[slot] = sr;
                self.sw[slot] = sw;
                return;
            }
            slot = (slot + 1) & self.mask;
        }
    }
}

/// Bounded top-k insert under (count DESC, key ASC).
fn topk_insert<K: EKey>(top: &mut Vec<(K, u64, u64, u64)>, cand: (K, u64, u64, u64), k: usize) {
    if top.len() == k {
        let w = top.last().unwrap();
        if (cand.1, std::cmp::Reverse(cand.0)) <= (w.1, std::cmp::Reverse(w.0)) {
            return;
        }
        top.pop();
    }
    let pos = top
        .binary_search_by(|p| (cand.1, std::cmp::Reverse(cand.0)).cmp(&(p.1, std::cmp::Reverse(p.0))))
        .unwrap_or_else(|p| p);
    top.insert(pos, cand);
}

/// Rep-persistent allocation slots [persist-rehome]: capacity-only,
/// QUERY-AGNOSTIC, capped parks (statepark.rs) — the former per-query
/// `Persist` map was an unbounded static keyed by `node.q` whose
/// allocations survived every statement outside any memory law. Only
/// allocations ever survived (every call truncates and recomputes, the
/// study law), so the query key bought nothing the size-arm at fetch
/// does not; the cap bounds the park; contents are cleared at BOTH park
/// and fetch (the depot's both-ends reset law). The fallback decode
/// scratches (`GState::fb`) no longer rest here at all — they park on
/// the worker scratch depot at pass-1 finish. Cursors were never parked
/// (rebuilt per engagement) and still are not.
use crate::stencils::statepark::{nested_bytes, vec_bytes, StatePark};

const SG_PARK_CAP: usize = 256 << 20;
/// [q30-regress] Width-priced per-state budget for the shrink-law floor:
/// the park exists to retain `t` pass-1 states, so its effective cap is
/// at least `t x` this budget (CI cluster q30/q31 grain: keys+pays scatter +
/// the 8192-cell column windows land 2-4MB/state at width 64; 8MB
/// covers the doubling overshoot). A flat 256MB cap sits exactly at the
/// q30-class state mass at CI cluster width — parks ratchet capacity across
/// statements (capacity-only reuse never shrinks), overflow the cap,
/// drop, re-grow: the sawtooth behind the v6.1 q30 bimodal cell (and
/// the same purge/recommit class convicted for q32 in 9cd3be8b29b —
/// mimalloc purges the dropped arenas, every next execute re-commits
/// and re-faults them).
const SG_STATE_BUDGET: usize = 8 << 20;
// [sqe-park-knobs parity] caps env-overridable (MB) for the allocator
// trade-study grid; defaults unchanged.
static PARK_ST64: StatePark<GState<u64>> =
    StatePark::new_env(SG_PARK_CAP, "PGRUST_SQE_PARKSG_MB");
static PARK_ST128: StatePark<GState<u128>> =
    StatePark::new_env(SG_PARK_CAP, "PGRUST_SQE_PARKSG_MB");
static PARK_TAB64: StatePark<OwnTab<u64>> =
    StatePark::new_env(SG_PARK_CAP, "PGRUST_SQE_PARKSG_MB");
static PARK_TAB128: StatePark<OwnTab<u128>> =
    StatePark::new_env(SG_PARK_CAP, "PGRUST_SQE_PARKSG_MB");

fn state_park<K: EKey>() -> &'static StatePark<GState<K>> {
    // Monomorphized accessor: the two widths own separate parks
    // (K is exactly u64 or u128, checked by K::BITS).
    unsafe {
        if K::BITS == 64 {
            std::mem::transmute::<&'static StatePark<GState<u64>>, &'static StatePark<GState<K>>>(
                &PARK_ST64,
            )
        } else {
            std::mem::transmute::<&'static StatePark<GState<u128>>, &'static StatePark<GState<K>>>(
                &PARK_ST128,
            )
        }
    }
}

fn tab_park<K: EKey>() -> &'static StatePark<OwnTab<K>> {
    unsafe {
        if K::BITS == 64 {
            std::mem::transmute::<&'static StatePark<OwnTab<u64>>, &'static StatePark<OwnTab<K>>>(
                &PARK_TAB64,
            )
        } else {
            std::mem::transmute::<&'static StatePark<OwnTab<u128>>, &'static StatePark<OwnTab<K>>>(
                &PARK_TAB128,
            )
        }
    }
}

/// Parked heap footprint of a GState (fb never rests here — asserted).
fn gstate_bytes<K: EKey>(s: &GState<K>) -> usize {
    debug_assert!(s.fb.is_none(), "fb scratches park on the depot, not here");
    nested_bytes(&s.keys)
        + nested_bytes(&s.pays)
        + vec_bytes(&s.sel)
        + nested_bytes(&s.cols)
        + s.wide.capacity() * (std::mem::size_of::<K>() + 24 + 16)
        + s.verdicts.capacity() * std::mem::size_of::<(usize, CVerdict)>()
}

fn owntab_bytes<K: EKey>(t: &OwnTab<K>) -> usize {
    vec_bytes(&t.keys) + vec_bytes(&t.cnt) + vec_bytes(&t.sr) + vec_bytes(&t.sw)
}

/// Both-ends reset: no statement's contents at rest in the park.
fn gstate_clear<K: EKey>(s: &mut GState<K>) {
    for b in &mut s.keys {
        b.clear();
    }
    for b in &mut s.pays {
        b.clear();
    }
    s.wide.clear();
    s.survivors = 0;
    s.bad_key = 0;
    s.verdicts.clear();
}

#[allow(clippy::too_many_arguments)]
fn engine<K: EKey>(
    ctx: &SqeCtx,
    node: &PlanNode,
    lay: &Layout,
    filt_col: u32,
    sum_col: u32,
    avg_col: u32,
    replay: Option<&PredCache>,
) -> AnswerSet {
    let (bank, pool) = (ctx.bank, ctx.pool);
    let t_all = std::time::Instant::now();
    let t = pool.threads();
    let units: Arc<Vec<Unit>> = ctx.faces.walk(bank, filt_col);
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
    let p = l2_partitions(ctx, lay);
    let shift = 64 - p.trailing_zeros();
    let packed = payload_packable(bank, sum_col, avg_col);
    // SWAR election consulted (and refused: text lane, no int width) —
    // the lowering trace stays visible on the rig.
    let _swar = swar_elected(None, 0.0);
    let ncols = lay.fields.len() + 2;
    let a_cols: Vec<u32> = lay
        .fields
        .iter()
        .map(|&(a, _, _)| a)
        .chain([sum_col, avg_col])
        .collect();
    let limit = match &node.params.topk {
        // Pushed bound: the (count DESC, key ASC) bounded insert serves
        // native specs; non-native bounds ride full collection and trim
        // at the answer boundary.
        Some(t) if t.native => t.n,
        Some(_) => usize::MAX,
        None => node.params.limit.min(1 << 20),
    };
    let publish = replay.is_none();

    if let Some(pc) = replay {
        assert_eq!(pc.units.len(), units.len(), "condcache plane walk drift");
    }

    // [q30-regress] width-priced shrink-law floors: the parks must be
    // allowed to retain what this engagement will hand back — `t`
    // states at the family's per-state budget (see SG_STATE_BUDGET).
    state_park::<K>().arm_floor(t * SG_STATE_BUDGET);
    tab_park::<K>().arm_floor(t * SG_STATE_BUDGET);

    // Fetch capacity-only parked states (query-agnostic; sizes are
    // armed in the init closure below, exactly as before).
    let state_slots: Vec<Mutex<Option<GState<K>>>> = {
        let mut v = state_park::<K>().fetch_up_to(t);
        (0..t).map(|_| Mutex::new(v.pop())).collect()
    };
    let tab_slots: Vec<Mutex<Option<OwnTab<K>>>> = {
        let mut v = tab_park::<K>().fetch_up_to(t);
        (0..t).map(|_| Mutex::new(v.pop())).collect()
    };

    // ---- pass 1: filter → survivor gather → scatter (part-grain) ----------
    let t_p1 = std::time::Instant::now();
    let pass1 = pool.run_finish(
        bank.parts.len(),
        |w| match state_slots[w].lock().unwrap().take() {
            Some(mut s) => {
                // Both-ends reset (park cleared too) + size arm: parked
                // states are query-agnostic now, so the partition/column
                // geometry must be re-armed, not assumed.
                gstate_clear(&mut s);
                if s.keys.len() != p {
                    s.keys = (0..p).map(|_| Vec::with_capacity(64)).collect();
                    s.pays = (0..p).map(|_| Vec::with_capacity(64)).collect();
                }
                if s.cols.len() != ncols {
                    s.cols = (0..ncols).map(|_| vec![0u64; 8192]).collect();
                }
                s
            }
            None => GState::<K> {
                keys: (0..p).map(|_| Vec::with_capacity(64)).collect(),
                pays: (0..p).map(|_| Vec::with_capacity(64)).collect(),
                sel: vec![0u16; 8192],
                cols: (0..ncols).map(|_| vec![0u64; 8192]).collect(),
                fb: None,
                wide: HashMap::new(),
                survivors: 0,
                bad_key: 0,
                verdicts: Vec::new(),
            },
        },
        |s, pi| {
            let (u0, u1) = part_units[pi];
            if u0 == u1 {
                return;
            }
            let filt = empty_filter(bank, pi, filt_col);
            let fc = match filt {
                EmptyFilter::Dict(_) => FusedCodes::open(bank, pi, filt_col),
                EmptyFilter::Raw => None,
            };
            let wc: Vec<Arc<Option<WordCol>>> = a_cols
                .iter()
                .map(|&a| ctx.faces.word_col(bank, pi, a))
                .collect();
            let need_fb = (replay.is_none() && fc.is_none())
                || wc.iter().any(|w| w.is_none());
            let mut curs: Option<(CurCache, Vec<CurCache>)> = if need_fb {
                if s.fb.is_none() {
                    s.fb = Some(Box::new((
                        crate::scan::scratch_fetch(),
                        (0..ncols).map(|_| crate::scan::scratch_fetch()).collect(),
                    )));
                }
                Some((
                    CurCache::new(filt_col),
                    a_cols.iter().map(|&a| CurCache::new(a)).collect(),
                ))
            } else {
                None
            };
            for u in u0..u1 {
                let (_, g, rows32, _) = units[u];
                let rows = rows32 as usize;
                // -- survivor list ------------------------------------------
                let n = match (&replay, &filt, &fc) {
                    (Some(pc), _, _) => match &pc.v[u] {
                        CVerdict::Skip => 0,
                        CVerdict::AllPass => rows,
                        CVerdict::Rows(r) => {
                            s.sel[..r.len()].copy_from_slice(r);
                            r.len()
                        }
                        CVerdict::Bitmap(w) => {
                            let mut n = 0usize;
                            for r in 0..rows {
                                if w[r >> 6] >> (r & 63) & 1 != 0 {
                                    s.sel[n] = r as u16;
                                    n += 1;
                                }
                            }
                            n
                        }
                    },
                    (None, EmptyFilter::Dict(empty), Some(fc)) => {
                        let e = empty.unwrap_or(u32::MAX);
                        let (base, w) = fc.code_zone(g);
                        let hi = if w >= 32 {
                            u64::MAX
                        } else {
                            base as u64 + (1u64 << w)
                        };
                        if (e as u64) < base as u64 || (e as u64) >= hi {
                            rows
                        } else if w == 0 {
                            0
                        } else {
                            let sel = &mut s.sel[..rows];
                            let mut n = 0usize;
                            fc.fold(g, rows, |r, c| {
                                sel[n] = r as u16;
                                n += (c != e) as usize;
                            });
                            n
                        }
                    }
                    _ => {
                        let (cp, _) = curs.as_mut().unwrap();
                        let (sp, _) = &mut **s.fb.as_mut().unwrap();
                        let d = sp.decode_full(cp.get(bank, pi), g, rows);
                        let mut n = 0usize;
                        for (r, &x) in d.iter().enumerate() {
                            if !unsafe { varlena_payload(x) }.is_empty() {
                                s.sel[n] = r as u16;
                                n += 1;
                            }
                        }
                        n
                    }
                };
                // Fill-on-cold: the verdict this fused loop computed is
                // the cache entry (§1.7 encoding election in CVerdict).
                if publish {
                    let v = if n == 0 {
                        CVerdict::Skip
                    } else if n == rows {
                        CVerdict::AllPass
                    } else {
                        CVerdict::encode(s.sel[..n].to_vec(), rows)
                    };
                    s.verdicts.push((u, v));
                }
                if n == 0 {
                    continue;
                }
                let all = n == rows;
                s.survivors += n as u64;
                // -- payload completion (survivors only) ---------------------
                for c in 0..ncols {
                    match wc[c].as_ref() {
                        Some(w) => {
                            if all {
                                w.full(g, rows, &mut s.cols[c]);
                            } else {
                                w.gather(g, rows, &s.sel[..n], &mut s.cols[c]);
                            }
                        }
                        None => {
                            let (_, cc) = curs.as_mut().unwrap();
                            let (_, fbs) = &mut **s.fb.as_mut().unwrap();
                            if all {
                                let d = fbs[c].decode_full(cc[c].get(bank, pi), g, rows);
                                s.cols[c][..rows].copy_from_slice(d);
                            } else {
                                let d = fbs[c].decode_sel(cc[c].get(bank, pi), g, &s.sel[..n]);
                                s.cols[c][..n].copy_from_slice(d);
                            }
                        }
                    }
                }
                // -- scatter / fold ------------------------------------------
                // Two-loop variant split on the loop-invariant `packed`
                // election (R2: no per-row branch on a granule constant).
                let nf = lay.fields.len();
                let GState { keys, pays, cols, wide, bad_key, .. } = s;
                let pack = |i: usize| -> K {
                    let mut key = K::from64(0);
                    for (fi, &(_, bits, sh)) in lay.fields.iter().enumerate() {
                        let v = cols[fi][i];
                        let masked = if bits >= 64 { v } else { v & ((1u64 << bits) - 1) };
                        key = key.or(K::from64(masked).shl(sh));
                    }
                    key
                };
                if packed {
                    for i in 0..n {
                        let key = pack(i);
                        *bad_key |= (key == K::MAXV) as u64;
                        let b = (key.hash64() >> shift) as usize;
                        let pay =
                            ((cols[nf][i] as u32) << 31) | (cols[nf + 1][i] as u32 & 0x7FFF_FFFF);
                        keys[b].push(key);
                        pays[b].push(pay);
                    }
                } else {
                    for i in 0..n {
                        let e = wide.entry(pack(i)).or_insert((0, 0, 0));
                        e.0 += 1;
                        e.1 += cols[nf][i];
                        e.2 += cols[nf + 1][i];
                    }
                }
            }
        },
        // Worker-side finish: fallback decode scratches park on THIS
        // worker's depot (reset gate inside scratch_park); the cursors
        // in `curs` were per-engagement locals and are already gone.
        |mut s| {
            if let Some(fb) = s.fb.take() {
                let (a, bs) = *fb;
                crate::scan::scratch_park(a);
                bs.into_iter().for_each(crate::scan::scratch_park);
            }
            s
        },
    );
    assert!(
        pass1.iter().all(|s| s.bad_key == 0),
        "survivor_gather: a packed key collided with the sentinel"
    );

    // Publish the plane the cold loop just computed (engine cache;
    // no-op when the goal carries no fingerprints).
    if publish {
        let mut verdicts: Vec<CVerdict> = (0..units.len()).map(|_| CVerdict::Skip).collect();
        let mut survivors = 0u64;
        for s in &pass1 {
            survivors += s.survivors;
        }
        for s in &pass1 {
            for (u, v) in &s.verdicts {
                verdicts[*u] = match v {
                    CVerdict::Skip => CVerdict::Skip,
                    CVerdict::AllPass => CVerdict::AllPass,
                    CVerdict::Rows(r) => CVerdict::Rows(r.clone()),
                    CVerdict::Bitmap(w) => CVerdict::Bitmap(w.clone()),
                };
            }
        }
        publish_cache(ctx, node, units.clone(), verdicts, survivors);
    }

    // ---- pass 2: partition-owned fold, zero merge --------------------------
    crate::engine::phn(node, "pass1", t_p1);
    let t_p2 = std::time::Instant::now();
    let mut all: Vec<(K, u64, u64, u64)> = if packed {
        let states: Vec<&GState<K>> = pass1.iter().collect();
        let owned = pool.run(
            p,
            |w| (Vec::new(), tab_slots[w].lock().unwrap().take(), w),
            |(out, tab, _): &mut (Vec<(K, u64, u64, u64)>, Option<OwnTab<K>>, usize), part| {
                let n: usize = states.iter().map(|s| s.keys[part].len()).sum();
                if n == 0 {
                    return;
                }
                let tb = match tab {
                    Some(tb) => {
                        tb.reset(n);
                        tb
                    }
                    None => {
                        *tab = Some(OwnTab::new(n));
                        tab.as_mut().unwrap()
                    }
                };
                for s in &states {
                    let keys = &s.keys[part];
                    let pays = &s.pays[part];
                    for i in 0..keys.len() {
                        let pay = pays[i];
                        tb.add(keys[i], pay >> 31, (pay & 0x7FFF_FFFF) as u64);
                    }
                }
                let mut top: Vec<(K, u64, u64, u64)> = Vec::new();
                for sl in 0..=tb.mask {
                    if tb.keys[sl] == K::MAXV {
                        continue;
                    }
                    topk_insert(
                        &mut top,
                        (tb.keys[sl], tb.cnt[sl] as u64, tb.sr[sl] as u64, tb.sw[sl]),
                        limit,
                    );
                }
                out.extend(top);
            },
        );
        let mut all = Vec::new();
        for (out, tab, w) in owned {
            *tab_slots[w].lock().unwrap() = tab;
            all.extend(out);
        }
        all
    } else {
        // Wide fallback (payload not packable by stats): serial merge of
        // the per-thread maps — correct, slower; FINDING reports it ran.
        let mut m: HashMap<K, (u64, u64, u64)> = HashMap::new();
        for s in &pass1 {
            for (k, v) in &s.wide {
                let e = m.entry(*k).or_insert((0, 0, 0));
                e.0 += v.0;
                e.1 += v.1;
                e.2 += v.2;
            }
        }
        let mut top: Vec<(K, u64, u64, u64)> = Vec::new();
        for (k, (c, sr, sw)) in m {
            topk_insert(&mut top, (k, c, sr, sw), limit);
        }
        top
    };
    crate::engine::phn(node, "pass2", t_p2);
    let t_e = std::time::Instant::now();
    // Top-k selection before the exact sort (the union is p x limit wide).
    if all.len() > limit && limit > 0 {
        all.select_nth_unstable_by(limit - 1, |a, b| {
            b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0))
        });
        all.truncate(limit);
    }
    all.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    all.truncate(limit);

    // Hand allocations back to the capped parks: contents cleared at
    // park (both-ends reset — no statement data at rest; `sel`/`cols`
    // stay capacity-law windows, always written before read), byte-
    // priced admission, overflow drops eagerly.
    {
        let sp = state_park::<K>();
        for mut s in pass1 {
            gstate_clear(&mut s);
            let b = gstate_bytes(&s);
            // [sqe-park-knobs parity] plain-data advisor: keys/pays/
            // sel/cols are scalar buffers — lazy-releasable under
            // OVERCAP=advise. `wide` (HashMap) and `verdicts`
            // (pointer-bearing) are never advised.
            sp.park_with(s, b, |s| {
                use crate::stencils::statepark::lazyfree::{advise_nested, advise_vec};
                advise_nested(&s.keys);
                advise_nested(&s.pays);
                advise_vec(&s.sel);
                advise_nested(&s.cols);
            });
        }
        let tp = tab_park::<K>();
        for slot in tab_slots {
            if let Some(tab) = slot.into_inner().unwrap() {
                let b = owntab_bytes(&tab);
                tp.park_with(tab, b, |t| {
                    use crate::stencils::statepark::lazyfree::advise_vec;
                    advise_vec(&t.keys);
                    advise_vec(&t.cnt);
                    advise_vec(&t.sr);
                    advise_vec(&t.sw);
                });
            }
        }
    }

    // ---- typed emit: key fields in plan order, then the agg tail -----------
    let mut key_out: Vec<Vec<i64>> = lay.fields.iter().map(|_| Vec::new()).collect();
    let mut cnts: Vec<i64> = Vec::new();
    let mut sums: Vec<i128> = Vec::new();
    let mut avgs: Vec<(i128, i64)> = Vec::new();
    let unsigned: Vec<bool> = lay
        .fields
        .iter()
        .map(|&(a, _, _)| crate::typmeta::is_unsigned_word(bank.typ(a).oid))
        .collect();
    for &(k, c, sr, sw) in all.iter() {
        for (fi, &(_, bits, sh)) in lay.fields.iter().enumerate() {
            key_out[fi].push(field_fold(unsigned[fi], bits, k.field(sh, bits)));
        }
        cnts.push(c as i64);
        sums.push(sr as i128);
        avgs.push((sw as i128, c as i64));
    }
    let mut cols_out: Vec<AnswerCol> = Vec::new();
    for (fi, &(a, _, _)) in lay.fields.iter().enumerate() {
        cols_out.push(AnswerCol::i64s(bank.typ(a), std::mem::take(&mut key_out[fi])));
    }
    for a in &node.agg {
        match a.op {
            AggOp::CountStar => {
                cols_out.push(AnswerCol::i64s(TypMeta::INT8, cnts.clone()))
            }
            AggOp::Sum => cols_out.push(AnswerCol::i128s(a.out, sums.clone())),
            AggOp::Avg => cols_out.push(AnswerCol {
                ty: a.out,
                data: ColData::Ratio { pairs: avgs.clone(), exact: false },
                validity: Validity::AllValid,
            }),
            other => panic!("survivor_gather: unsupported agg {other:?}"),
        }
    }
    let out = AnswerSet::from_cols(cols_out);
    crate::engine::phn(node, "epilogue", t_e);
    crate::engine::phn(node, "total", t_all);
    out
}

/// Resolve the node's shape (filter column from the NeEmpty frame term,
/// sum/avg agg columns from the agg list).
fn shape(node: &PlanNode) -> (u32, u32, u32) {
    let pred = node.pred.as_ref().expect("survivor_gather: predicate-bearing family");
    assert!(pred.terms.is_empty(), "survivor_gather: int residues not in this shape");
    assert_eq!(pred.var_terms.len(), 1, "survivor_gather: one varlena frame term");
    let vt = &pred.var_terms[0];
    assert_eq!(vt.op, VarOp::NeEmpty, "survivor_gather: NeEmpty frame");
    let mut sum_col = None;
    let mut avg_col = None;
    for a in &node.agg {
        match a.op {
            AggOp::CountStar => {}
            AggOp::Sum => sum_col = Some(a.col.unwrap()),
            AggOp::Avg => avg_col = Some(a.col.unwrap()),
            other => panic!("survivor_gather: unsupported agg {other:?}"),
        }
    }
    (
        vt.col,
        sum_col.expect("survivor_gather: needs a SUM agg"),
        avg_col.expect("survivor_gather: needs an AVG agg"),
    )
}

/// Contract entry. Honest/cold arm: recompute the fused filter + gather,
/// publishing the survivor plane as a side effect. Elected arm: replay
/// the engine-cached plane (parallel, survivor ordinals only).
pub fn run_survivor_gather(ctx: &SqeCtx, node: &PlanNode) -> AnswerSet {
    let (filt_col, sum_col, avg_col) = shape(node);
    let lay = layout(ctx.bank, &node.params.group_cols);
    let cache = replay_cache(ctx, node);
    if lay.total_bits <= 64 {
        engine::<u64>(ctx, node, &lay, filt_col, sum_col, avg_col, cache.as_deref())
    } else {
        engine::<u128>(ctx, node, &lay, filt_col, sum_col, avg_col, cache.as_deref())
    }
}

#[cfg(test)]
mod tests {
    use super::field_fold;

    #[test]
    fn unsigned_word_keys_zero_extend() {
        assert_eq!(field_fold(true, 32, 0xFFFF_FFFE), 4_294_967_294);
        assert_eq!(field_fold(true, 32, 0x8000_0001), 2_147_483_649);
        assert_eq!(field_fold(false, 32, 0xFFFF_FFFE), -2);
        assert_eq!(field_fold(false, 16, 0xFFFF), -1);
        assert_eq!(field_fold(true, 8, 0xFF), 255);
    }
}
