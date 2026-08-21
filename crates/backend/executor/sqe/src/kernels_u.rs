//! [R3 hot-shape lane, 2026-08-15] The UserID family under the L2 PARTITION
//! LAW — the high-NDV int8 key (17.6M distinct users at 100m) that no
//! dictionary covers and whose 641MB ordinal remap collapsed 9× at scale.
//!
//! Shape (all variants here): partition-OWNED, zero merge.
//!   pass 1  decode → hash64(user) → scatter into P per-thread SoA
//!           buckets (presized from rows/threads/P — no Vec growth);
//!           P = next_pow2(ndv_est × bytes_per_group / L2) clamped
//!           [256, 8192], COMPUTED FROM BANK STATS (hot-shape law).
//!   pass 2  one owner per partition; the count table is sized from
//!           ndv_est / P (NOT from the partition's key count, which is
//!           multiplicity× larger and is what blew the old 256-way tables
//!           to 12MB each) and is REUSED across the partitions a worker
//!           claims (reset = in-L2 memset; no fresh pages per partition).
//!   levers  run-collapse (the bank is (CounterID, EventDate, UserID,…)
//!           sorted: equal-user runs fold into one (key, cnt) entry) and
//!           the per-worker ARENA (scatter buckets + table kept across
//!           reps: what a persistent executor's worker scratch looks like).
//! (Port note: the hot-shape rig variant ladders stay in the reference
//! tree; the tuned l2p drivers below are the engine's closure.)

use crate::bank::Bank;
use crate::drivers::{Sel128, SelOrder, Unit};
use crate::grouped::{hash64, GidStream, TextReg};
use crate::kernels_g::ColState;
use crate::pool::Pool;
use std::sync::Mutex;

/// The hot-shape L2 budget (measured winner on the metal box); tables sized to
/// sit under it are L2-resident on both Graviton (2MB L2) and x86.
pub const L2_BUDGET: usize = 512 * 1024;

/// P = next_pow2(ndv_est × bytes_per_group / l2). [ruling 3] clamp floor
/// is width-derived (cost_params::partition_floor — the historical 256
/// was next_pow2(2·96) frozen); ceiling is the per-target scatter cap.
pub fn l2_partition_count(
    ndv_est: usize,
    bytes_per_group: usize,
    l2_bytes: usize,
    width: usize,
) -> usize {
    let cp = crate::cost_params::target();
    let p = (ndv_est.max(1) * bytes_per_group).div_ceil(l2_bytes.max(1));
    p.next_power_of_two().clamp(cp.partition_floor(width), cp.scatter_partition_cap)
}

// ---------------------------------------------------------------------------
// AoS count table: 16B slots (u64 key, u32 cnt, pad) — ONE cache line per
// probe (the SoA Cnt64 touches keys[] and cnt[]); cnt==0 is the empty
// sentinel (no key value is reserved — UserID may be any u64). Reused
// across partitions: `reset` keeps the capacity.
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct Slot {
    key: u64,
    cnt: u32,
    _pad: u32,
}

pub struct Tbl64 {
    slots: Vec<Slot>,
    mask: usize,
    pub len: usize,
}

impl Tbl64 {
    /// cap = next_pow2(2 × expected groups) ≥ 1024 (≤ 1/2 load at the
    /// estimate; the grow path exists for underestimates, never planned).
    pub fn new(expect_groups: usize) -> Tbl64 {
        let cap = (expect_groups * 2).next_power_of_two().max(1024);
        Tbl64 {
            slots: vec![Slot::default(); cap],
            mask: cap - 1,
            len: 0,
        }
    }
    pub fn cap(&self) -> usize {
        self.mask + 1
    }
    #[inline]
    pub fn reset(&mut self) {
        self.slots.fill(Slot::default());
        self.len = 0;
    }
    #[inline(always)]
    pub fn add(&mut self, h: u64, key: u64, by: u32) {
        if self.len * 4 >= (self.mask + 1) * 3 {
            self.grow();
        }
        let mut s = (h as usize) & self.mask;
        loop {
            let e = &mut self.slots[s];
            if e.cnt == 0 {
                e.key = key;
                e.cnt = by;
                self.len += 1;
                return;
            }
            if e.key == key {
                e.cnt += by;
                return;
            }
            s = (s + 1) & self.mask;
        }
    }
    fn grow(&mut self) {
        let old = std::mem::replace(&mut self.slots, vec![Slot::default(); (self.mask + 1) * 2]);
        self.mask = self.slots.len() - 1;
        self.len = 0;
        for e in old {
            if e.cnt != 0 {
                self.add(hash64(e.key), e.key, e.cnt);
            }
        }
    }
    #[inline]
    pub fn for_each(&self, mut f: impl FnMut(u64, u32)) {
        for e in &self.slots {
            if e.cnt != 0 {
                f(e.key, e.cnt);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// per-worker arena: state slots keyed by pool worker index, handed back
// after each rep so buckets/tables keep their (already faulted-in) pages.
// ---------------------------------------------------------------------------

pub struct Arena<T> {
    slots: Vec<Mutex<Option<T>>>,
}

impl<T> Arena<T> {
    pub fn new(threads: usize) -> Arena<T> {
        Arena {
            slots: (0..threads).map(|_| Mutex::new(None)).collect(),
        }
    }
    pub fn take(&self, t: usize) -> Option<T> {
        self.slots[t].lock().unwrap().take()
    }
    pub fn put(&self, t: usize, v: T) {
        *self.slots[t].lock().unwrap() = Some(v);
    }
}

// ---------------------------------------------------------------------------
// hot-shape: SELECT UserID, COUNT(*) FROM hits GROUP BY UserID
//      ORDER BY COUNT(*) DESC LIMIT 10;
// ---------------------------------------------------------------------------

/// pass-1 state: the decode cursor + P SoA buckets (cnts empty unless
/// run-collapsing).
pub struct U1 {
    /// Per-engagement decode state [persist-rehome]: fetched fresh from
    /// the worker depot in init and parked at finish — NEVER at rest in
    /// the arena (a parked CurCache keyed only by part index can
    /// resurface another bank's or column's stream; the distinct.rs
    /// wrong-answer class).
    cs: Option<ColState>,
    keys: Vec<Vec<u64>>,
    cnts: Vec<Vec<u32>>,
    touched: u64,
}

pub struct UserCountOut {
    pub top: Sel128,
    pub rows_counted: u64,
    pub touched: u64,
    pub groups: u64,
    /// scattered entries (== rows unless run-collapsed)
    pub entries: u64,
    pub p: usize,
    pub cap: usize,
}

/// The tuned partition-owned COUNT over UserID.
#[allow(clippy::too_many_arguments)]
pub fn user_count_l2p(
    bank: &Bank,
    pool: &Pool,
    units: &[Unit],
    a_u: u32,
    ndv_est: usize,
    // topk = the top-k width (the PoC hardcoded the hot-shape LIMIT 10; the
    // plan's offset+limit is the law now).
    topk: usize,
    order: SelOrder,
    l2_bytes: usize,
    run_collapse: bool,
    arena: Option<(&Arena<U1>, &Arena<Tbl64>)>,
) -> UserCountOut {
    let t = pool.threads();
    let rows_total = bank.rows_total() as usize;
    // 16B slot × 2 (≤1/2 load) = 32B per group.
    let p = l2_partition_count(ndv_est, 32, l2_bytes, pool.threads());
    let shift = 64 - p.trailing_zeros();
    let per_bucket = (rows_total / t.max(1) / p) * 5 / 4 + 16;
    let est_part = ndv_est / p + 1;
    let fresh = |_t: usize| U1 {
        cs: Some(ColState::fetch(a_u)),
        keys: (0..p).map(|_| Vec::with_capacity(per_bucket)).collect(),
        cnts: if run_collapse {
            (0..p).map(|_| Vec::with_capacity(per_bucket)).collect()
        } else {
            Vec::new()
        },
        touched: 0,
    };
    let pass1 = pool.run_finish(
        units.len(),
        |ti| match arena {
            Some((a1, _)) => match a1.take(ti) {
                Some(mut u) if u.keys.len() == p && u.cnts.len() == if run_collapse { p } else { 0 } => {
                    for v in &mut u.keys {
                        v.clear();
                    }
                    for v in &mut u.cnts {
                        v.clear();
                    }
                    u.touched = 0;
                    debug_assert!(u.cs.is_none(), "cursors must never rest in the arena");
                    u.cs = Some(ColState::fetch(a_u));
                    u
                }
                _ => fresh(ti),
            },
            None => fresh(ti),
        },
        |u, i| {
            let (pi, g, rows, _) = units[i];
            let d = u.cs.as_mut().unwrap().dec(bank, pi, g, rows as usize);
            if run_collapse {
                // fold equal-key runs: one (key, run_len) entry per run.
                let n = d.len();
                let mut i = 0usize;
                while i < n {
                    let v = d[i];
                    let mut j = i + 1;
                    while j < n && d[j] == v {
                        j += 1;
                    }
                    let b = (hash64(v) >> shift) as usize;
                    u.keys[b].push(v);
                    u.cnts[b].push((j - i) as u32);
                    i = j;
                }
            } else {
                for &v in d {
                    u.keys[(hash64(v) >> shift) as usize].push(v);
                }
            }
            u.touched += rows as u64;
        },
        // Worker-side finish: scratch to THIS worker's depot, cursor
        // drops — the arena parks plain buckets only.
        |mut u| {
            if let Some(cs) = u.cs.take() {
                cs.park();
            }
            u
        },
    );
    let touched: u64 = pass1.iter().map(|u| u.touched).sum();
    let entries: u64 = pass1.iter().map(|u| u.keys.iter().map(|v| v.len() as u64).sum::<u64>()).sum();
    let scattered: Vec<&U1> = pass1.iter().collect();
    let owned = pool.run(
        p,
        |ti| {
            let tbl = match arena {
                Some((_, a2)) => match a2.take(ti) {
                    Some(tb) if tb.cap() >= (est_part * 2).next_power_of_two().max(1024) => tb,
                    _ => Tbl64::new(est_part),
                },
                None => Tbl64::new(est_part),
            };
            (Sel128::new(topk, order), 0u64, 0u64, tbl)
        },
        |(sel, rows_counted, groups, tbl), part| {
            tbl.reset();
            for u in &scattered {
                let ks = &u.keys[part];
                if run_collapse {
                    let cs = &u.cnts[part];
                    for i in 0..ks.len() {
                        let k = ks[i];
                        tbl.add(hash64(k), k, cs[i]);
                    }
                } else {
                    for &k in ks {
                        tbl.add(hash64(k), k, 1);
                    }
                }
            }
            *groups += tbl.len as u64;
            tbl.for_each(|key, cnt| {
                *rows_counted += cnt as u64;
                sel.consider(key as u128, cnt as u64);
            });
        },
    );
    let cap = owned.iter().map(|o| o.3.cap()).max().unwrap_or(0);
    let mut top = Sel128::new(topk, order);
    let mut rows_counted = 0u64;
    let mut groups = 0u64;
    for (s, rc, g, _) in &owned {
        top.absorb(s);
        rows_counted += rc;
        groups += g;
    }
    if let Some((a1, a2)) = arena {
        for (ti, u) in pass1.into_iter().enumerate() {
            a1.put(ti, u);
        }
        for (ti, o) in owned.into_iter().enumerate() {
            a2.put(ti, o.3);
        }
    }
    UserCountOut {
        top,
        rows_counted,
        touched,
        groups,
        entries,
        p,
        cap,
    }
}

// ---------------------------------------------------------------------------
// hot-shape: SELECT UserID, SearchPhrase, COUNT(*) FROM hits
//      GROUP BY UserID, SearchPhrase LIMIT 10   (canonical: packed key ASC)
// Same law, pair keys: ONE SoA stream (user u64, gid u32[, run cnt u32])
// partitioned by hash64(user) — the empty phrase rides the same stream as
// gid = empty_gid (no u128 anywhere; the old split arm's u128 currency for
// the phrase 14% is gone). Pass-2 slot: (user u64, gid u32, cnt u32) = 16B,
// one line per probe; table sized from the partition's ENTRY count (a
// hard upper bound on its groups; run-collapsed entries sit within ~1.5×
// of groups) and reused across a worker's partitions.
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct PSlot {
    key: u64,
    gid: u32,
    cnt: u32,
}

pub struct TblPair {
    slots: Vec<PSlot>,
    mask: usize,
    pub len: usize,
}

#[inline(always)]
fn pair_slot_hash(h_user: u64, gid: u32) -> u64 {
    // h_user's LOW bits are free (the partition took the top bits); mix
    // the gid in multiplicatively so equal-user groups spread.
    h_user ^ (gid as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

impl TblPair {
    pub fn new(expect_groups: usize) -> TblPair {
        let cap = (expect_groups * 2).next_power_of_two().max(1024);
        TblPair {
            slots: vec![PSlot::default(); cap],
            mask: cap - 1,
            len: 0,
        }
    }
    pub fn cap(&self) -> usize {
        self.mask + 1
    }
    /// Reset for a partition expecting `expect_groups`: grows the capacity
    /// if needed (kept afterwards), then clears.
    #[inline]
    pub fn reset_for(&mut self, expect_groups: usize) {
        let need = (expect_groups * 2).next_power_of_two().max(1024);
        if need > self.cap() {
            self.slots = vec![PSlot::default(); need];
            self.mask = need - 1;
        } else {
            self.slots.fill(PSlot::default());
        }
        self.len = 0;
    }
    #[inline(always)]
    pub fn add(&mut self, h_user: u64, key: u64, gid: u32, by: u32) {
        if self.len * 4 >= (self.mask + 1) * 3 {
            self.grow();
        }
        let mut s = (pair_slot_hash(h_user, gid) as usize) & self.mask;
        loop {
            let e = &mut self.slots[s];
            if e.cnt == 0 {
                e.key = key;
                e.gid = gid;
                e.cnt = by;
                self.len += 1;
                return;
            }
            if e.key == key && e.gid == gid {
                e.cnt += by;
                return;
            }
            s = (s + 1) & self.mask;
        }
    }
    fn grow(&mut self) {
        let old = std::mem::replace(&mut self.slots, vec![PSlot::default(); (self.mask + 1) * 2]);
        self.mask = self.slots.len() - 1;
        self.len = 0;
        for e in old {
            if e.cnt != 0 {
                self.add(hash64(e.key), e.key, e.gid, e.cnt);
            }
        }
    }
    #[inline]
    pub fn for_each(&self, mut f: impl FnMut(u64, u32, u32)) {
        for e in &self.slots {
            if e.cnt != 0 {
                f(e.key, e.gid, e.cnt);
            }
        }
    }
}

/// hot-shape pass-1 state: user cursor + gid stream + P SoA buckets.
pub struct P1 {
    /// Per-engagement decode state [join-depot]: fetched fresh from the
    /// worker depot in init and parked at finish — NEVER at rest in the
    /// arena (a parked CurCache keyed only by part index can resurface
    /// another bank's or column's stream; the distinct.rs wrong-answer
    /// class).
    cu: Option<ColState>,
    gs: Option<GidStream>,
    keys: Vec<Vec<u64>>,
    gids: Vec<Vec<u32>>,
    cnts: Vec<Vec<u32>>,
    touched: u64,
}

/// The tuned partition-owned COUNT over (UserID, SearchPhrase-gid).
#[allow(clippy::too_many_arguments)]
pub fn pair_count_l2p(
    bank: &Bank,
    pool: &Pool,
    units: &[Unit],
    a_u: u32,
    a_s: u32,
    reg: &TextReg,
    ndv_est: usize,
    topk: usize,
    order: SelOrder,
    l2_bytes: usize,
    run_collapse: bool,
    arena: Option<(&Arena<P1>, &Arena<TblPair>)>,
) -> UserCountOut {
    let t = pool.threads();
    let rows_total = bank.rows_total() as usize;
    let p = l2_partition_count(ndv_est, 32, l2_bytes, pool.threads());
    let shift = 64 - p.trailing_zeros();
    let per_bucket = (rows_total / t.max(1) / p) * 5 / 4 + 16;
    let fresh = |_t: usize| P1 {
        cu: Some(ColState::fetch(a_u)),
        gs: Some(GidStream::fetch(a_s)),
        keys: (0..p).map(|_| Vec::with_capacity(per_bucket)).collect(),
        gids: (0..p).map(|_| Vec::with_capacity(per_bucket)).collect(),
        cnts: if run_collapse {
            (0..p).map(|_| Vec::with_capacity(per_bucket)).collect()
        } else {
            Vec::new()
        },
        touched: 0,
    };
    let pass1 = pool.run_finish(
        units.len(),
        |ti| match arena {
            Some((a1, _)) => match a1.take(ti) {
                Some(mut u) if u.keys.len() == p && u.cnts.len() == if run_collapse { p } else { 0 } => {
                    for v in &mut u.keys {
                        v.clear();
                    }
                    for v in &mut u.gids {
                        v.clear();
                    }
                    for v in &mut u.cnts {
                        v.clear();
                    }
                    u.touched = 0;
                    debug_assert!(
                        u.cu.is_none() && u.gs.is_none(),
                        "cursors must never rest in the arena"
                    );
                    u.cu = Some(ColState::fetch(a_u));
                    u.gs = Some(GidStream::fetch(a_s));
                    u
                }
                _ => fresh(ti),
            },
            None => fresh(ti),
        },
        |u, i| {
            let (pi, g, rows, _) = units[i];
            let rows = rows as usize;
            let du = u.cu.as_mut().unwrap().dec(bank, pi, g, rows);
            let gids = u.gs.as_mut().unwrap().granule(bank, reg, pi, g, rows);
            if run_collapse {
                let mut r = 0usize;
                while r < rows {
                    let (v, gd) = (du[r], gids[r]);
                    let mut j = r + 1;
                    while j < rows && du[j] == v && gids[j] == gd {
                        j += 1;
                    }
                    let b = (hash64(v) >> shift) as usize;
                    u.keys[b].push(v);
                    u.gids[b].push(gd);
                    u.cnts[b].push((j - r) as u32);
                    r = j;
                }
            } else {
                for r in 0..rows {
                    let v = du[r];
                    let b = (hash64(v) >> shift) as usize;
                    u.keys[b].push(v);
                    u.gids[b].push(gids[r]);
                }
            }
            u.touched += 2 * rows as u64;
        },
        // Worker-side finish: scratches to THIS worker's depot, cursors
        // drop — the arena parks plain buckets only.
        |mut u| {
            if let Some(cs) = u.cu.take() {
                cs.park();
            }
            if let Some(gs) = u.gs.take() {
                gs.park();
            }
            u
        },
    );
    let touched: u64 = pass1.iter().map(|u| u.touched).sum();
    let entries: u64 = pass1.iter().map(|u| u.keys.iter().map(|v| v.len() as u64).sum::<u64>()).sum();
    let scattered: Vec<&P1> = pass1.iter().collect();
    let owned = pool.run(
        p,
        |ti| {
            let tbl = match arena {
                Some((_, a2)) => a2.take(ti).unwrap_or_else(|| TblPair::new(1024)),
                None => TblPair::new(1024),
            };
            (Sel128::new(topk, order), 0u64, 0u64, tbl)
        },
        |(sel, rows_counted, groups, tbl), part| {
            let n: usize = scattered.iter().map(|u| u.keys[part].len()).sum();
            tbl.reset_for(n);
            for u in &scattered {
                let ks = &u.keys[part];
                let gs = &u.gids[part];
                if run_collapse {
                    let cs = &u.cnts[part];
                    for i in 0..ks.len() {
                        let k = ks[i];
                        tbl.add(hash64(k), k, gs[i], cs[i]);
                    }
                } else {
                    for i in 0..ks.len() {
                        let k = ks[i];
                        tbl.add(hash64(k), k, gs[i], 1);
                    }
                }
            }
            *groups += tbl.len as u64;
            tbl.for_each(|key, gid, cnt| {
                *rows_counted += cnt as u64;
                sel.consider(((key as u128) << 32) | gid as u128, cnt as u64);
            });
        },
    );
    let cap = owned.iter().map(|o| o.3.cap()).max().unwrap_or(0);
    let mut top = Sel128::new(topk, order);
    let mut rows_counted = 0u64;
    let mut groups = 0u64;
    for (s, rc, g, _) in &owned {
        top.absorb(s);
        rows_counted += rc;
        groups += g;
    }
    if let Some((a1, a2)) = arena {
        for (ti, u) in pass1.into_iter().enumerate() {
            a1.put(ti, u);
        }
        for (ti, o) in owned.into_iter().enumerate() {
            a2.put(ti, o.3);
        }
    }
    UserCountOut {
        top,
        rows_counted,
        touched,
        groups,
        entries,
        p,
        cap,
    }
}
