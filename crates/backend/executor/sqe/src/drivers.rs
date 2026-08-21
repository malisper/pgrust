//! The two combine shapes of the grouped band, as reusable drivers over the
//! persistent pool:
//!
//! - `*_owned`: radix partition by key/user hash in pass 1 (per-thread
//!   bucket vecs, no sharing), pass 2 gives each partition exactly ONE
//!   owner — no merge exists anywhere (the phase-1 merge-tax lesson made the
//!   default shape).
//! - `*_merge`: thread-local tables folded serially at the end — kept to
//!   MEASURE the merge tax per kernel, never the recommended shape at high
//!   NDV.
//!
//! Units are granule-grain (pi, g, rows, base) claims from
//! `scan::granule_walk`; `fill` is the kernel's decode step.

use crate::grouped::{hash128, hash64, radix_of, Cnt128, Cnt64, RADIX_P};
use crate::pool::Pool;

pub type Unit = (usize, u32, u32, u64);

// ---------------------------------------------------------------------------
// top-k selection that DISTRIBUTES: a per-partition top-k under a TOTAL
// order merges into the global top-k because partitions own disjoint keys.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SelOrder {
    /// (count DESC, key ASC) — the canonical ORDER BY c DESC rendering.
    CountDescKeyAsc,
    /// (key ASC) — the canonical no-ORDER-BY LIMIT rendering.
    KeyAsc,
}

#[derive(Clone)]
pub struct Sel128 {
    pub rows: Vec<(u128, u64)>,
    k: usize,
    order: SelOrder,
}

impl Sel128 {
    /// k == usize::MAX = collector mode: keep every key, UNORDERED (no
    /// caller relies on order without a finite k).
    pub fn new(k: usize, order: SelOrder) -> Sel128 {
        Sel128 {
            rows: Vec::with_capacity(k.saturating_add(1).min(4096)),
            k,
            order,
        }
    }
    #[inline]
    fn before(&self, a: (u128, u64), b: (u128, u64)) -> bool {
        match self.order {
            SelOrder::CountDescKeyAsc => a.1 > b.1 || (a.1 == b.1 && a.0 < b.0),
            SelOrder::KeyAsc => a.0 < b.0,
        }
    }
    #[inline]
    pub fn consider(&mut self, key: u128, cnt: u64) {
        if self.k == usize::MAX {
            self.rows.push((key, cnt));
            return;
        }
        if self.rows.len() == self.k {
            let last = *self.rows.last().unwrap();
            if !self.before((key, cnt), last) {
                return;
            }
            self.rows.pop();
        }
        let mut i = self.rows.len();
        self.rows.push((key, cnt));
        while i > 0 && self.before(self.rows[i], self.rows[i - 1]) {
            self.rows.swap(i, i - 1);
            i -= 1;
        }
    }
    pub fn absorb(&mut self, other: &Sel128) {
        for &(k, c) in &other.rows {
            self.consider(k, c);
        }
    }
}

// ---------------------------------------------------------------------------
// COUNT(*) grouping, u128 keys
// ---------------------------------------------------------------------------

pub struct CountOut {
    pub top: Sel128,
    pub groups: u64,
    pub rows_counted: u64,
    pub touched: u64,
}

/// Partition-owned COUNT grouping. `fill(state, unit_idx, out)` decodes one
/// granule into packed keys (post-filter). Returns global top-k + totals.
pub fn count_owned_128<S: Send>(
    pool: &Pool,
    units: &[Unit],
    k: usize,
    order: SelOrder,
    expect_ndv: usize,
    init: impl Fn(usize) -> S + Sync,
    fill: impl Fn(&mut S, usize, &mut Vec<u128>) + Sync,
) -> CountOut {
    let t = pool.threads();
    struct P1<S> {
        st: S,
        buckets: Vec<Vec<u128>>,
        keybuf: Vec<u128>,
        touched: u64,
    }
    let pass1 = pool.run(
        units.len(),
        |ti| P1 {
            st: init(ti),
            buckets: (0..RADIX_P).map(|_| Vec::new()).collect(),
            keybuf: Vec::with_capacity(8192),
            touched: 0,
        },
        |p, i| {
            p.keybuf.clear();
            fill(&mut p.st, i, &mut p.keybuf);
            p.touched += units[i].2 as u64;
            for &key in &p.keybuf {
                p.buckets[radix_of(hash128(key))].push(key);
            }
        },
    );
    let touched: u64 = pass1.iter().map(|p| p.touched).sum();
    let scattered: Vec<&Vec<Vec<u128>>> = pass1.iter().map(|p| &p.buckets).collect();
    let _ = t;
    let per_part_expect = (expect_ndv / RADIX_P).max(16);
    let owned = pool.run(
        RADIX_P,
        |_| (Sel128::new(k, order), 0u64, 0u64),
        |(sel, groups, rows_counted), p| {
            let n: usize = scattered.iter().map(|b| b[p].len()).sum();
            let mut tbl = Cnt128::new(n.max(per_part_expect));
            for b in &scattered {
                for &key in &b[p] {
                    tbl.add(key, 1);
                }
            }
            *groups += tbl.len as u64;
            for s in 0..tbl.keys.len() {
                if tbl.cnt[s] != 0 {
                    *rows_counted += tbl.cnt[s] as u64;
                    sel.consider(tbl.keys[s], tbl.cnt[s] as u64);
                }
            }
        },
    );
    let mut top = Sel128::new(k, order);
    let mut groups = 0u64;
    let mut rows_counted = 0u64;
    for (s, g, rc) in &owned {
        top.absorb(s);
        groups += g;
        rows_counted += rc;
    }
    CountOut {
        top,
        groups,
        rows_counted,
        touched,
    }
}

/// Thread-local tables + serial merge — the merge-tax measurement arm.
pub fn count_merge_128<S: Send>(
    pool: &Pool,
    units: &[Unit],
    k: usize,
    order: SelOrder,
    expect_ndv: usize,
    init: impl Fn(usize) -> S + Sync,
    fill: impl Fn(&mut S, usize, &mut Vec<u128>) + Sync,
) -> CountOut {
    let t = pool.threads();
    let pass1 = pool.run(
        units.len(),
        |ti| (init(ti), Cnt128::new(expect_ndv / t + 16), Vec::with_capacity(8192), 0u64),
        |(st, tbl, keybuf, touched): &mut (S, Cnt128, Vec<u128>, u64), i| {
            keybuf.clear();
            fill(st, i, keybuf);
            *touched += units[i].2 as u64;
            for &key in keybuf.iter() {
                tbl.add(key, 1);
            }
        },
    );
    let touched: u64 = pass1.iter().map(|p| p.3).sum();
    // Serial merge into the largest table (the phase-1 merge-fix idiom).
    let mut tables: Vec<Cnt128> = pass1.into_iter().map(|p| p.1).collect();
    let big = tables
        .iter()
        .enumerate()
        .max_by_key(|(_, tb)| tb.len)
        .map(|(i, _)| i)
        .unwrap();
    let mut global = tables.swap_remove(big);
    for tb in tables {
        for s in 0..tb.keys.len() {
            if tb.cnt[s] != 0 {
                global.add(tb.keys[s], tb.cnt[s]);
            }
        }
    }
    let mut top = Sel128::new(k, order);
    let mut groups = 0u64;
    let mut rows_counted = 0u64;
    for s in 0..global.keys.len() {
        if global.cnt[s] != 0 {
            groups += 1;
            rows_counted += global.cnt[s] as u64;
            top.consider(global.keys[s], global.cnt[s] as u64);
        }
    }
    CountOut {
        top,
        groups,
        rows_counted,
        touched,
    }
}

// ---------------------------------------------------------------------------
// COUNT(DISTINCT user) per group — pair-distinct drivers
// ---------------------------------------------------------------------------

pub struct DistinctOut {
    /// (group_key, distinct_users) — the FULL group list (group NDV in this
    /// band's distinct kernels is small: regions, phone models, phrases).
    pub groups: Vec<(u64, u64)>,
    pub touched: u64,
    pub pairs_distinct: u64,
}

/// Partition-owned pair distinct: pass 1 scatters (group u64, user u64)
/// pairs radix-partitioned by hash(user) — every (group,user) pair lands in
/// exactly one partition; pass 2 dedupes per partition and counts first
/// insertions per group into per-WORKER dense arrays (dense_groups) that
/// vector-add at the end (a small merge that is O(groups), never O(pairs)).
pub fn pair_distinct_owned<S: Send>(
    pool: &Pool,
    units: &[Unit],
    dense_groups: usize,
    init: impl Fn(usize) -> S + Sync,
    fill: impl Fn(&mut S, usize, &mut Vec<(u64, u64)>) + Sync,
) -> DistinctOut {
    use crate::stencils::statepark::{arm_buckets, nested_bytes, scatter_park_on, vec_bytes, StatePark};
    // [p2-phase-widening] the pass-1 pair-scatter arena and the pass-2
    // seen/counts states were the last fresh-per-exec allocations in the
    // pair-distinct band (q8's flat ~9% served widening — the largest
    // per-exec scratch of the violator set); park them like their
    // part_merge siblings (PARKPDF/PARKPDT). Scatter-arena class cap /
    // table class cap; over-cap admission rides the ruled OVERCAP law.
    static PARKPO: StatePark<(Vec<Vec<u128>>, Vec<(u64, u64)>)> = StatePark::new(256 << 20);
    static PARKPOT: StatePark<(Cnt128, Vec<u32>)> = StatePark::new(64 << 20);
    let pass1 = pool.run(
        units.len(),
        |ti| {
            let (buckets, mut pairbuf) = if scatter_park_on() { PARKPO.fetch() } else { None }
                .unwrap_or_else(|| (Vec::new(), Vec::with_capacity(8192)));
            pairbuf.clear();
            (init(ti), arm_buckets(buckets, RADIX_P), pairbuf, 0u64)
        },
        |(st, buckets, pairbuf, touched): &mut (S, Vec<Vec<u128>>, Vec<(u64, u64)>, u64), i| {
            pairbuf.clear();
            fill(st, i, pairbuf);
            *touched += units[i].2 as u64;
            for &(gk, user) in pairbuf.iter() {
                let pair = ((gk as u128) << 64) | user as u128;
                buckets[radix_of(hash64(user))].push(pair);
            }
        },
    );
    let touched: u64 = pass1.iter().map(|p| p.3).sum();
    let scattered: Vec<&Vec<Vec<u128>>> = pass1.iter().map(|p| &p.1).collect();
    let owned = pool.run(
        RADIX_P,
        |_| {
            let (seen, mut counts) = if scatter_park_on() { PARKPOT.fetch() } else { None }
                .unwrap_or_else(|| (Cnt128::new(16), Vec::new()));
            counts.clear();
            counts.resize(dense_groups, 0);
            (counts, 0u64, seen)
        },
        |(counts, pairs, seen): &mut (Vec<u32>, u64, Cnt128), p| {
            let n: usize = scattered.iter().map(|b| b[p].len()).sum();
            // per-partition dedupe scope, exactly the fresh-table law
            // (pairs land in one partition; reset keeps scopes disjoint).
            seen.reset(n.max(16));
            for b in &scattered {
                for &pair in &b[p] {
                    if seen.add(pair, 1) {
                        counts[(pair >> 64) as usize] += 1;
                        *pairs += 1;
                    }
                }
            }
        },
    );
    let mut dense = vec![0u64; dense_groups];
    let mut pairs_distinct = 0u64;
    for (c, pr, _) in &owned {
        pairs_distinct += pr;
        for (i, &v) in c.iter().enumerate() {
            dense[i] += v as u64;
        }
    }
    if scatter_park_on() {
        for (counts, _, seen) in owned {
            let b = seen.keys.capacity() * 16 + seen.cnt.capacity() * 4 + vec_bytes(&counts);
            PARKPOT.park((seen, counts), b);
        }
        for (_, buckets, pairbuf, _) in pass1 {
            let b = nested_bytes(&buckets) + vec_bytes(&pairbuf);
            PARKPO.park((buckets, pairbuf), b);
        }
    }
    let groups: Vec<(u64, u64)> = dense
        .into_iter()
        .enumerate()
        .filter(|&(_, c)| c > 0)
        .map(|(g, c)| (g as u64, c))
        .collect();
    DistinctOut {
        groups,
        touched,
        pairs_distinct,
    }
}

/// Thread-local pair tables + serial merge (the merge-tax arm): local
/// dedupe shrinks the pair stream, but every locally-distinct pair is
/// re-inserted into ONE global table on one thread at the end.
pub fn pair_distinct_merge<S: Send>(
    pool: &Pool,
    units: &[Unit],
    dense_groups: usize,
    init: impl Fn(usize) -> S + Sync,
    fill: impl Fn(&mut S, usize, &mut Vec<(u64, u64)>) + Sync,
) -> DistinctOut {
    let pass1 = pool.run(
        units.len(),
        |ti| (init(ti), Cnt128::new(1 << 16), Vec::with_capacity(8192), 0u64),
        |(st, tbl, pairbuf, touched): &mut (S, Cnt128, Vec<(u64, u64)>, u64), i| {
            pairbuf.clear();
            fill(st, i, pairbuf);
            *touched += units[i].2 as u64;
            for &(gk, user) in pairbuf.iter() {
                tbl.add(((gk as u128) << 64) | user as u128, 1);
            }
        },
    );
    let touched: u64 = pass1.iter().map(|p| p.3).sum();
    let mut tables: Vec<Cnt128> = pass1.into_iter().map(|p| p.1).collect();
    let big = tables
        .iter()
        .enumerate()
        .max_by_key(|(_, tb)| tb.len)
        .map(|(i, _)| i)
        .unwrap();
    let mut global = tables.swap_remove(big);
    let mut counts = vec![0u64; dense_groups];
    let mut pairs_distinct = 0u64;
    // The kept table's pairs are already distinct — count them first.
    for s in 0..global.keys.len() {
        if global.cnt[s] != 0 {
            counts[(global.keys[s] >> 64) as usize] += 1;
            pairs_distinct += 1;
        }
    }
    for tb in tables {
        for s in 0..tb.keys.len() {
            if tb.cnt[s] != 0 && global.add(tb.keys[s], tb.cnt[s]) {
                counts[(tb.keys[s] >> 64) as usize] += 1;
                pairs_distinct += 1;
            }
        }
    }
    let groups: Vec<(u64, u64)> = counts
        .into_iter()
        .enumerate()
        .filter(|&(_, c)| c > 0)
        .map(|(g, c)| (g as u64, c))
        .collect();
    DistinctOut {
        groups,
        touched,
        pairs_distinct,
    }
}

/// Sort-based pair distinct (the pair-distinct shape study): pass 1 scatters pairs by
/// user hash exactly like `pair_distinct_owned`; pass 2 owners CONCATENATE
/// + SORT their partition and count unique transitions — no hash table at
/// all in pass 2.
pub fn pair_distinct_sort<S: Send>(
    pool: &Pool,
    units: &[Unit],
    dense_groups: usize,
    init: impl Fn(usize) -> S + Sync,
    fill: impl Fn(&mut S, usize, &mut Vec<(u64, u64)>) + Sync,
) -> DistinctOut {
    pair_distinct_sort_p(pool, units, dense_groups, 8, init, fill)
}

/// `pair_distinct_sort` with a caller-chosen partition-count exponent
/// (P = 2^pbits, bucket = top pbits of hash(user)) — the L2 partition law
/// applied to the pair-sort merge: P sized so each owner's concatenated
/// sort buffer is cache-resident, computed from bank row counts at run
/// time, never compiled in.
pub fn pair_distinct_sort_p<S: Send>(
    pool: &Pool,
    units: &[Unit],
    dense_groups: usize,
    pbits: u32,
    init: impl Fn(usize) -> S + Sync,
    fill: impl Fn(&mut S, usize, &mut Vec<(u64, u64)>) + Sync,
) -> DistinctOut {
    let p = 1usize << pbits;
    let pass1 = pool.run(
        units.len(),
        |ti| {
            (
                init(ti),
                (0..p).map(|_| Vec::new()).collect::<Vec<Vec<u128>>>(),
                Vec::with_capacity(8192),
                0u64,
            )
        },
        |(st, buckets, pairbuf, touched): &mut (S, Vec<Vec<u128>>, Vec<(u64, u64)>, u64), i| {
            pairbuf.clear();
            fill(st, i, pairbuf);
            *touched += units[i].2 as u64;
            for &(gk, user) in pairbuf.iter() {
                buckets[(hash64(user) >> (64 - pbits)) as usize]
                    .push(((gk as u128) << 64) | user as u128);
            }
        },
    );
    let touched: u64 = pass1.iter().map(|p| p.3).sum();
    let scattered: Vec<&Vec<Vec<u128>>> = pass1.iter().map(|p| &p.1).collect();
    let owned = pool.run(
        p,
        |_| (vec![0u32; dense_groups], 0u64, Vec::new()),
        |(counts, pairs, buf): &mut (Vec<u32>, u64, Vec<u128>), p| {
            buf.clear();
            for b in &scattered {
                buf.extend_from_slice(&b[p]);
            }
            buf.sort_unstable();
            let mut prev: Option<u128> = None;
            for &pair in buf.iter() {
                if prev != Some(pair) {
                    counts[(pair >> 64) as usize] += 1;
                    *pairs += 1;
                    prev = Some(pair);
                }
            }
        },
    );
    let mut dense = vec![0u64; dense_groups];
    let mut pairs_distinct = 0u64;
    for (c, pr, _) in &owned {
        pairs_distinct += pr;
        for (i, &v) in c.iter().enumerate() {
            dense[i] += v as u64;
        }
    }
    let groups: Vec<(u64, u64)> = dense
        .into_iter()
        .enumerate()
        .filter(|&(_, c)| c > 0)
        .map(|(g, c)| (g as u64, c))
        .collect();
    DistinctOut {
        groups,
        touched,
        pairs_distinct,
    }
}

/// [r3b] GID-RANGE-owned pair distinct (the 100m pair-distinct shape): pass 1
/// scatters (gid, user) pairs by gid RANGE (P ranges under the L2 law,
/// computed by the caller from bank counts); pass 2 gives each range one
/// owner that sorts its pairs and counts distinct users per gid into a
/// dense slice of width ngids/P — ONE dense array in total, sliced across
/// owners, instead of one full ngids array PER WORKER (which at 100m /
/// 96 threads / ~6M gids is 2.3 GB of zero-fill plus a 96-way vector
/// merge — the measured merge-bound tail). Owners keep count-only
/// top-10s; the caller's render needs only groups with count >= the
/// global 10th count, which is what `groups` returns (every group at or
/// above the boundary — exact for (count DESC, key ASC) top-10, ties
/// included).
pub fn pair_distinct_gid_owned<S: Send>(
    pool: &Pool,
    units: &[Unit],
    dense_groups: usize,
    pbits: u32,
    init: impl Fn(usize) -> S + Sync,
    fill: impl Fn(&mut S, usize, &mut Vec<(u64, u64)>) + Sync,
) -> DistinctOut {
    let p = 1usize << pbits;
    // range width: gids per owner (power of two so the bucket is a shift)
    let mut shift = 0u32;
    while (dense_groups >> shift) + 1 > p {
        shift += 1;
    }
    let width = 1usize << shift;
    let nb = (dense_groups >> shift) + 1;
    let pass1 = pool.run(
        units.len(),
        |ti| {
            (
                init(ti),
                (0..nb).map(|_| Vec::new()).collect::<Vec<Vec<u128>>>(),
                Vec::with_capacity(8192),
                0u64,
            )
        },
        |(st, buckets, pairbuf, touched): &mut (S, Vec<Vec<u128>>, Vec<(u64, u64)>, u64), i| {
            pairbuf.clear();
            fill(st, i, pairbuf);
            *touched += units[i].2 as u64;
            for &(gk, user) in pairbuf.iter() {
                buckets[(gk >> shift) as usize].push(((gk as u128) << 64) | user as u128);
            }
        },
    );
    let touched: u64 = pass1.iter().map(|p| p.3).sum();
    let scattered: Vec<&Vec<Vec<u128>>> = pass1.iter().map(|p| &p.1).collect();
    // owners: (bucket, dense slice, distinct pairs, top-10 counts)
    let owned = pool.run(
        nb,
        |_| (Vec::new(), Vec::new()),
        |(out, buf): &mut (Vec<(usize, Vec<u32>, u64, Vec<u32>)>, Vec<u128>), b| {
            buf.clear();
            for s in &scattered {
                buf.extend_from_slice(&s[b]);
            }
            if buf.is_empty() {
                return;
            }
            buf.sort_unstable();
            let base = (b << shift) as u64;
            let mut counts = vec![0u32; width];
            let mut pairs = 0u64;
            let mut prev: Option<u128> = None;
            for &pair in buf.iter() {
                if prev != Some(pair) {
                    counts[((pair >> 64) - base as u128) as usize] += 1;
                    pairs += 1;
                    prev = Some(pair);
                }
            }
            let mut top: Vec<u32> = Vec::with_capacity(11);
            for &c in &counts {
                if c != 0 {
                    if top.len() < 10 {
                        top.push(c);
                        top.sort_unstable_by(|a, b| b.cmp(a));
                    } else if c > top[9] {
                        top[9] = c;
                        top.sort_unstable_by(|a, b| b.cmp(a));
                    }
                }
            }
            out.push((b, counts, pairs, top));
        },
    );
    let states: Vec<(usize, Vec<u32>, u64, Vec<u32>)> =
        owned.into_iter().flat_map(|o| o.0).collect();
    let pairs_distinct: u64 = states.iter().map(|s| s.2).sum();
    let mut all_tops: Vec<u32> = states.iter().flat_map(|s| s.3.iter().copied()).collect();
    all_tops.sort_unstable_by(|a, b| b.cmp(a));
    let cb = if all_tops.len() >= 10 { all_tops[9] } else { 0 };
    let mut groups: Vec<(u64, u64)> = Vec::new();
    for (b, counts, _, _) in &states {
        let base = (*b << shift) as u64;
        for (i, &c) in counts.iter().enumerate() {
            if c != 0 && c >= cb {
                groups.push((base + i as u64, c as u64));
            }
        }
    }
    DistinctOut {
        groups,
        touched,
        pairs_distinct,
    }
}

// ---------------------------------------------------------------------------
// COUNT(*) grouping, u64 keys (half the key traffic of the u128 arm — the
// u64-key push after the Umbra-target directive)
// ---------------------------------------------------------------------------

pub fn count_owned_64<S: Send>(
    pool: &Pool,
    units: &[Unit],
    k: usize,
    order: SelOrder,
    expect_ndv: usize,
    init: impl Fn(usize) -> S + Sync,
    fill: impl Fn(&mut S, usize, &mut Vec<u64>) + Sync,
) -> CountOut {
    struct P1<S> {
        st: S,
        buckets: Vec<Vec<u64>>,
        keybuf: Vec<u64>,
        touched: u64,
    }
    let pass1 = pool.run(
        units.len(),
        |ti| P1 {
            st: init(ti),
            buckets: (0..RADIX_P).map(|_| Vec::new()).collect(),
            keybuf: Vec::with_capacity(8192),
            touched: 0,
        },
        |p, i| {
            p.keybuf.clear();
            fill(&mut p.st, i, &mut p.keybuf);
            p.touched += units[i].2 as u64;
            for &key in &p.keybuf {
                p.buckets[radix_of(hash64(key))].push(key);
            }
        },
    );
    let touched: u64 = pass1.iter().map(|p| p.touched).sum();
    let scattered: Vec<&Vec<Vec<u64>>> = pass1.iter().map(|p| &p.buckets).collect();
    let per_part_expect = (expect_ndv / RADIX_P).max(16);
    let owned = pool.run(
        RADIX_P,
        |_| (Sel128::new(k, order), 0u64, 0u64),
        |(sel, groups, rows_counted), p| {
            let n: usize = scattered.iter().map(|b| b[p].len()).sum();
            let mut tbl = Cnt64::new(n.max(per_part_expect));
            for b in &scattered {
                for &key in &b[p] {
                    tbl.add(key, 1);
                }
            }
            *groups += tbl.len as u64;
            for s in 0..tbl.keys.len() {
                if tbl.cnt[s] != 0 {
                    *rows_counted += tbl.cnt[s] as u64;
                    sel.consider(tbl.keys[s] as u128, tbl.cnt[s] as u64);
                }
            }
        },
    );
    let mut top = Sel128::new(k, order);
    let mut groups = 0u64;
    let mut rows_counted = 0u64;
    for (s, g, rc) in &owned {
        top.absorb(s);
        groups += g;
        rows_counted += rc;
    }
    CountOut {
        top,
        groups,
        rows_counted,
        touched,
    }
}

pub fn count_merge_64<S: Send>(
    pool: &Pool,
    units: &[Unit],
    k: usize,
    order: SelOrder,
    expect_ndv: usize,
    init: impl Fn(usize) -> S + Sync,
    fill: impl Fn(&mut S, usize, &mut Vec<u64>) + Sync,
) -> CountOut {
    let t = pool.threads();
    let pass1 = pool.run(
        units.len(),
        |ti| (init(ti), Cnt64::new(expect_ndv / t + 16), Vec::with_capacity(8192), 0u64),
        |(st, tbl, keybuf, touched): &mut (S, Cnt64, Vec<u64>, u64), i| {
            keybuf.clear();
            fill(st, i, keybuf);
            *touched += units[i].2 as u64;
            for &key in keybuf.iter() {
                tbl.add(key, 1);
            }
        },
    );
    let touched: u64 = pass1.iter().map(|p| p.3).sum();
    let mut tables: Vec<Cnt64> = pass1.into_iter().map(|p| p.1).collect();
    let big = tables
        .iter()
        .enumerate()
        .max_by_key(|(_, tb)| tb.len)
        .map(|(i, _)| i)
        .unwrap();
    let mut global = tables.swap_remove(big);
    for tb in tables {
        for s in 0..tb.keys.len() {
            if tb.cnt[s] != 0 {
                global.add(tb.keys[s], tb.cnt[s]);
            }
        }
    }
    let mut top = Sel128::new(k, order);
    let mut groups = 0u64;
    let mut rows_counted = 0u64;
    for s in 0..global.keys.len() {
        if global.cnt[s] != 0 {
            groups += 1;
            rows_counted += global.cnt[s] as u64;
            top.consider(global.keys[s] as u128, global.cnt[s] as u64);
        }
    }
    CountOut {
        top,
        groups,
        rows_counted,
        touched,
    }
}

#[cfg(test)]
mod pd_park_tests {
    use super::*;

    /// [p2-phase-widening] pair_distinct_owned through the parked
    /// scatter/seen path: consecutive executions must be byte-identical,
    /// including a SMALLER second shape (guards the capacity-only laws —
    /// counts clear+resize, per-partition seen.reset, arm_buckets — a
    /// stale parked state must never leak pairs or counts forward).
    #[test]
    fn pair_distinct_owned_park_reuse_identity() {
        let pool = Pool::new(4);
        let run = |nunits: usize, dense: usize, stride: u64| {
            let units: Vec<Unit> = (0..nunits).map(|i| (0, i as u32, 10, 0)).collect();
            let mut out = pair_distinct_owned(
                &pool,
                &units,
                dense,
                |_| (),
                |_, i, buf| {
                    for r in 0..10u64 {
                        // duplicated pairs across units exercise the dedupe
                        buf.push(((r % dense as u64), (i as u64 * stride + r) % 7));
                    }
                },
            );
            out.groups.sort_unstable();
            out
        };
        let a1 = run(16, 8, 3);
        let a2 = run(16, 8, 3);
        assert_eq!(a1.groups, a2.groups, "parked rerun diverged (groups)");
        assert_eq!(a1.pairs_distinct, a2.pairs_distinct, "parked rerun diverged (pairs)");
        assert_eq!(a1.touched, a2.touched);
        // shrink: fewer units, smaller dense domain, different pair mix —
        // must equal its own fresh recompute despite larger parked states
        let b1 = run(4, 3, 5);
        let b2 = run(4, 3, 5);
        assert_eq!(b1.groups, b2.groups, "post-shrink parked rerun diverged");
        assert_eq!(b1.pairs_distinct, b2.pairs_distinct);
    }
}
