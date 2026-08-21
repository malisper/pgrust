//! Handwritten, hand-optimal TPC-H floors (Q3/Q9/Q18 from the original
//! study; Q1/Q5/Q10/Q12/Q14 from the sqe-tpch-floor2 extension —
//! tpch-floor-2.md) against the pgrc2 bank faces — the code a perfect engine would emit for these plans: hand-
//! fused scan+filter+build/probe loops, no IR, no election, no
//! materialized join rows. Parallelism = the engine's own idiom
//! (`par_range` dynamic granule claiming, thread-local state merged by the
//! caller). These numbers are the P4-2 join-stencil bars.
//!
//! Known floor headroom (recorded, not taken): the two text filter columns
//! (c_mktsegment, p_name) are compared through decoded payload bytes, not
//! dict codes — both scans are <2% of their query's rows, so the delta is
//! noise at SF1; the SF100 study should take the dict-code compare.

use super::*;
use crate::bank::Bank;
use crate::scan::{granule_walk, par_range, varlena_payload, CurCache, Scratch};
use std::sync::atomic::{AtomicI32, Ordering};

pub struct TpchBanks {
    pub customer: Bank,
    pub orders: Bank,
    pub lineitem: Bank,
    pub part: Bank,
    pub partsupp: Bank,
    pub supplier: Bank,
    pub nation: Bank,
    pub region: Bank,
}

impl TpchBanks {
    pub fn open(root: &str) -> TpchBanks {
        TpchBanks {
            customer: open_table(root, "customer"),
            orders: open_table(root, "orders"),
            lineitem: open_table(root, "lineitem"),
            part: open_table(root, "part"),
            partsupp: open_table(root, "partsupp"),
            supplier: open_table(root, "supplier"),
            nation: open_table(root, "nation"),
            region: open_table(root, "region"),
        }
    }
}

/// Decoded varlena cell -> owned String (render-side only, never hot).
#[inline(always)]
fn txt(v: u64) -> String {
    String::from_utf8(unsafe { varlena_payload(v) }.to_vec()).unwrap()
}

#[inline(always)]
fn sx4(d: u64) -> i64 {
    d as u32 as i32 as i64
}

#[inline(always)]
fn hash64(k: u64) -> u64 {
    let mut z = k.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Open-addressing i64-key map with typed payload; key 0 reserved EMPTY
/// (every TPC-H key is >= 1). Fixed capacity (caller sizes 2x).
struct OaMap<V: Copy> {
    mask: usize,
    keys: Vec<i64>,
    vals: Vec<V>,
}

impl<V: Copy + Default> OaMap<V> {
    fn with_capacity(n: usize) -> OaMap<V> {
        let cap = (n.max(8) * 2).next_power_of_two();
        OaMap { mask: cap - 1, keys: vec![0; cap], vals: vec![V::default(); cap] }
    }
    #[inline(always)]
    fn insert(&mut self, k: i64, v: V) {
        debug_assert!(k != 0);
        let mut i = hash64(k as u64) as usize & self.mask;
        loop {
            if self.keys[i] == 0 {
                self.keys[i] = k;
                self.vals[i] = v;
                return;
            }
            if self.keys[i] == k {
                self.vals[i] = v;
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
    /// Slot of `k`, or usize::MAX.
    #[inline(always)]
    fn slot(&self, k: i64) -> usize {
        let mut i = hash64(k as u64) as usize & self.mask;
        loop {
            let kk = self.keys[i];
            if kk == k {
                return i;
            }
            if kk == 0 {
                return usize::MAX;
            }
            i = (i + 1) & self.mask;
        }
    }
}

/// Per-thread column reader: cursor cache + scratch, granule decode.
struct Col {
    cur: CurCache,
    scr: Scratch,
}

impl Col {
    fn new(attno: u32) -> Col {
        Col { cur: CurCache::new(attno), scr: Scratch::new() }
    }
    #[inline(always)]
    fn gran<'s>(&'s mut self, bank: &Bank, pi: usize, g: u32, rows: u32) -> &'s [u64] {
        let cur = self.cur.get(bank, pi);
        self.scr.decode_full(cur, g, rows as usize)
    }
}

// ---------------------------------------------------------------------------
// Q3 — shipping priority. customer(seg) ⋈ orders(date<) ⋈ lineitem(date>),
// group by orderkey, top-10 (revenue desc, o_orderdate asc, orderkey asc).
// ---------------------------------------------------------------------------

pub fn q3(b: &TpchBanks, threads: usize) -> Vec<String> {
    let cutoff = q3_date() as i64;

    // 1. BUILDING custkey bitset (customer scan, hand-fused filter).
    let ccount = b.customer.rows_total() as usize;
    let words = ccount / 64 + 2;
    let cunits = granule_walk(&b.customer, 1);
    let bitsets = par_range(
        cunits.len(),
        threads,
        |_| (Col::new(1), Col::new(7), vec![0u64; words]),
        |(ck, seg, bits), ui| {
            let (pi, g, rows, _) = cunits[ui];
            let segs = seg.gran(&b.customer, pi, g, rows);
            // Copy custkeys out before the second decode touches scratch?
            // No: two Cols carry independent scratches — decode order free.
            let keys = ck.gran(&b.customer, pi, g, rows);
            for r in 0..rows as usize {
                let p = unsafe { varlena_payload(segs[r]) };
                if p == Q3_SEGMENT {
                    let k = sx4(keys[r]) as usize;
                    bits[k >> 6] |= 1 << (k & 63);
                }
            }
        },
    );
    let mut bits = vec![0u64; words];
    for (_, _, tb) in &bitsets {
        for (a, v) in bits.iter_mut().zip(tb) {
            *a |= v;
        }
    }

    // 2. orders scan: o_orderdate < cutoff AND bit(custkey) -> map
    //    orderkey -> (orderdate, shippriority).
    let ounits = granule_walk(&b.orders, 1);
    let parts: Vec<Vec<(i64, i32, i32)>> = par_range(
        ounits.len(),
        threads,
        |_| (Col::new(1), Col::new(2), Col::new(5), Col::new(8), Vec::new()),
        |(ok, ck, od, pr, out): &mut (_, _, _, _, Vec<(i64, i32, i32)>), ui| {
            let (pi, g, rows, _) = ounits[ui];
            let dates = od.gran(&b.orders, pi, g, rows);
            let custs = ck.gran(&b.orders, pi, g, rows);
            let keys = ok.gran(&b.orders, pi, g, rows);
            let prios = pr.gran(&b.orders, pi, g, rows);
            for r in 0..rows as usize {
                let d = sx4(dates[r]);
                let c = sx4(custs[r]) as usize;
                if d < cutoff && bits[c >> 6] >> (c & 63) & 1 != 0 {
                    out.push((keys[r] as i64, d as i32, sx4(prios[r]) as i32));
                }
            }
        },
    )
    .into_iter()
    .map(|s| s.4)
    .collect();
    let n: usize = parts.iter().map(|v| v.len()).sum();
    let mut map: OaMap<(i32, i32)> = OaMap::with_capacity(n);
    for v in &parts {
        for &(k, d, p) in v {
            map.insert(k, (d, p));
        }
    }

    // 3. lineitem scan: l_shipdate > cutoff, probe, fold revenue
    //    (extprice * (100 - disc), scale-4) into per-thread slot arrays.
    let lunits = granule_walk(&b.lineitem, 1);
    let cap = map.mask + 1;
    let revs = par_range(
        lunits.len(),
        threads,
        |_| (Col::new(1), Col::new(6), Col::new(7), Col::new(11), vec![0i64; cap]),
        |(ok, ep, di, sd, rev): &mut (_, _, _, _, Vec<i64>), ui| {
            let (pi, g, rows, _) = lunits[ui];
            let ships = sd.gran(&b.lineitem, pi, g, rows);
            let keys = ok.gran(&b.lineitem, pi, g, rows);
            let exts = ep.gran(&b.lineitem, pi, g, rows);
            let discs = di.gran(&b.lineitem, pi, g, rows);
            for r in 0..rows as usize {
                if sx4(ships[r]) > cutoff {
                    let s = map.slot(keys[r] as i64);
                    if s != usize::MAX {
                        rev[s] += exts[r] as i64 * (100 - sx4(discs[r]));
                    }
                }
            }
        },
    );

    // 4. merge + top-10 (revenue desc, orderdate asc, orderkey asc).
    let mut top: Vec<(i64, i32, i64, i32)> = Vec::with_capacity(11); // (-rev, date, okey, prio)
    let mut total = vec![0i64; cap];
    for (_, _, _, _, r) in &revs {
        for (a, v) in total.iter_mut().zip(r) {
            *a += v;
        }
    }
    for s in 0..cap {
        if total[s] > 0 {
            let cand = (-total[s], map.vals[s].0, map.keys[s], map.vals[s].1);
            if top.len() < 10 {
                top.push(cand);
                top.sort();
            } else if cand < top[9] {
                top[9] = cand;
                top.sort();
            }
        }
    }
    top.iter()
        .map(|&(nrev, d, k, p)| {
            format!("{k}\t{}\t{}\t{p}", fmt_money4(-nrev), fmt_date(d))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Q9 — product-type profit. part(name ~ green) ⋈ partsupp ⋈ supplier ⋈
// nation, lineitem ⋈ orders(year); group (nation, year), order nation asc,
// year desc. amount = ext*(1-disc) - supplycost*qty (scale-4).
// ---------------------------------------------------------------------------

#[inline(always)]
fn contains(hay: &[u8], needle: &[u8]) -> bool {
    hay.len() >= needle.len()
        && hay.windows(needle.len()).any(|w| w == needle)
}

pub fn q9(b: &TpchBanks, threads: usize) -> Vec<String> {
    // 1. green partkey bitset.
    let pcount = b.part.rows_total() as usize;
    let words = pcount / 64 + 2;
    let punits = granule_walk(&b.part, 1);
    let bitsets = par_range(
        punits.len(),
        threads,
        |_| (Col::new(1), Col::new(2), vec![0u64; words]),
        |(pk, pn, bits), ui| {
            let (pi, g, rows, _) = punits[ui];
            let names = pn.gran(&b.part, pi, g, rows);
            let keys = pk.gran(&b.part, pi, g, rows);
            for r in 0..rows as usize {
                let p = unsafe { varlena_payload(names[r]) };
                if contains(p, Q9_COLOR) {
                    let k = sx4(keys[r]) as usize;
                    bits[k >> 6] |= 1 << (k & 63);
                }
            }
        },
    );
    let mut bits = vec![0u64; words];
    for (_, _, tb) in &bitsets {
        for (a, v) in bits.iter_mut().zip(tb) {
            *a |= v;
        }
    }

    // 2. supplier -> nation direct array (suppkey dense).
    let scount = b.supplier.rows_total() as usize;
    let mut s_nation = vec![0u8; scount + 1];
    {
        let sunits = granule_walk(&b.supplier, 1);
        let mut sk = Col::new(1);
        let mut nk = Col::new(4);
        for &(pi, g, rows, _) in &sunits {
            let nats = nk.gran(&b.supplier, pi, g, rows);
            let keys = sk.gran(&b.supplier, pi, g, rows);
            for r in 0..rows as usize {
                s_nation[sx4(keys[r]) as usize] = sx4(nats[r]) as u8;
            }
        }
    }

    // 3. partsupp: green parts only -> map (partkey<<20|suppkey) -> cost.
    //    (suppkey < 2^20 through SF100 — 10k * 100.)
    let psunits = granule_walk(&b.partsupp, 1);
    let pparts: Vec<Vec<(i64, i32)>> = par_range(
        psunits.len(),
        threads,
        |_| (Col::new(1), Col::new(2), Col::new(4), Vec::new()),
        |(pk, sk, sc, out): &mut (_, _, _, Vec<(i64, i32)>), ui| {
            let (pi, g, rows, _) = psunits[ui];
            let parts_ = pk.gran(&b.partsupp, pi, g, rows);
            let supps = sk.gran(&b.partsupp, pi, g, rows);
            let costs = sc.gran(&b.partsupp, pi, g, rows);
            for r in 0..rows as usize {
                let p = sx4(parts_[r]) as usize;
                if bits[p >> 6] >> (p & 63) & 1 != 0 {
                    out.push(((p as i64) << 20 | sx4(supps[r]), sx4(costs[r]) as i32));
                }
            }
        },
    )
    .into_iter()
    .map(|s| s.3)
    .collect();
    let nps: usize = pparts.iter().map(|v| v.len()).sum();
    let mut ps_cost: OaMap<i32> = OaMap::with_capacity(nps);
    for v in &pparts {
        for &(k, c) in v {
            ps_cost.insert(k, c);
        }
    }

    // 4. orders: orderkey -> year, direct array over the sparse key domain
    //    (bound = stats max; each key written once, so plain u8 stores
    //    behind an atomic view are race-free by disjointness).
    let okey_max = super::gen::orderkey_of(b.orders.rows_total() - 1) as usize;
    let year_arr: Vec<std::sync::atomic::AtomicU8> =
        (0..=okey_max).map(|_| std::sync::atomic::AtomicU8::new(0)).collect();
    let ounits = granule_walk(&b.orders, 1);
    par_range(
        ounits.len(),
        threads,
        |_| (Col::new(1), Col::new(5)),
        |(ok, od), ui| {
            let (pi, g, rows, _) = ounits[ui];
            let dates = od.gran(&b.orders, pi, g, rows);
            let keys = ok.gran(&b.orders, pi, g, rows);
            for r in 0..rows as usize {
                let y = date_year(sx4(dates[r]) as i32) - 1991; // 1..=7
                year_arr[keys[r] as usize].store(y as u8, Ordering::Relaxed);
            }
        },
    );

    // 5. lineitem: the 5-way probe pipeline, fold into [nation][year].
    const NY: usize = 25 * 8;
    let lunits = granule_walk(&b.lineitem, 1);
    let grids = par_range(
        lunits.len(),
        threads,
        |_| {
            (
                Col::new(1),
                Col::new(2),
                Col::new(3),
                Col::new(5),
                Col::new(6),
                Col::new(7),
                vec![(0i64, 0u32); NY],
            )
        },
        |(ok, pk, sk, qt, ep, di, grid): &mut (_, _, _, _, _, _, Vec<(i64, u32)>), ui| {
            let (pi, g, rows, _) = lunits[ui];
            let parts_ = pk.gran(&b.lineitem, pi, g, rows);
            let supps = sk.gran(&b.lineitem, pi, g, rows);
            let keys = ok.gran(&b.lineitem, pi, g, rows);
            let qtys = qt.gran(&b.lineitem, pi, g, rows);
            let exts = ep.gran(&b.lineitem, pi, g, rows);
            let discs = di.gran(&b.lineitem, pi, g, rows);
            for r in 0..rows as usize {
                let p = sx4(parts_[r]) as usize;
                if bits[p >> 6] >> (p & 63) & 1 != 0 {
                    let s = sx4(supps[r]);
                    let cost =
                        ps_cost.vals[ps_cost.slot((p as i64) << 20 | s)] as i64;
                    let y = year_arr[keys[r] as usize].load(Ordering::Relaxed) as usize;
                    // scale-4: ext_c*(100-disc)  -  cost_c * qty_c
                    // (qty_c = qty*100, so cost_c*qty_c is already s4).
                    let amount =
                        exts[r] as i64 * (100 - sx4(discs[r])) - cost * sx4(qtys[r]);
                    let nat = s_nation[s as usize] as usize;
                    let cell = &mut grid[nat * 8 + y];
                    cell.0 += amount;
                    cell.1 += 1;
                }
            }
        },
    );
    let mut grid = vec![(0i64, 0u32); NY];
    for s in &grids {
        for (a, v) in grid.iter_mut().zip(&s.6) {
            a.0 += v.0;
            a.1 += v.1;
        }
    }

    // 6. render: nation name asc, year desc.
    let mut names: Vec<(String, usize)> = Vec::with_capacity(25);
    {
        let nunits = granule_walk(&b.nation, 1);
        let mut kc = Col::new(1);
        let mut nc = Col::new(2);
        for &(pi, g, rows, _) in &nunits {
            let nm = nc.gran(&b.nation, pi, g, rows);
            let ks = kc.gran(&b.nation, pi, g, rows);
            for r in 0..rows as usize {
                let s = String::from_utf8(unsafe { varlena_payload(nm[r]) }.to_vec()).unwrap();
                names.push((s, sx4(ks[r]) as usize));
            }
        }
    }
    names.sort();
    let mut out = Vec::new();
    for (name, nk) in &names {
        for y in (1..=7usize).rev() {
            let (v, cnt) = grid[nk * 8 + y];
            if cnt > 0 {
                out.push(format!("{name}\t{}\t{}", 1991 + y, fmt_money4(v)));
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Q18 — large-volume customer. HAVING sum(l_quantity) > 300 over lineitem,
// join back orders + customer, top-100 (o_totalprice desc, o_orderdate asc).
// ---------------------------------------------------------------------------

pub fn q18(b: &TpchBanks, threads: usize) -> Vec<String> {
    // 1. sum(l_quantity) per orderkey — shared atomic array over the key
    //    domain (each key touched <= 7 times; contention negligible).
    let okey_max = super::gen::orderkey_of(b.orders.rows_total() - 1) as usize;
    let qty: Vec<AtomicI32> = (0..=okey_max).map(|_| AtomicI32::new(0)).collect();
    let lunits = granule_walk(&b.lineitem, 1);
    par_range(
        lunits.len(),
        threads,
        |_| (Col::new(1), Col::new(5)),
        |(ok, qt), ui| {
            let (pi, g, rows, _) = lunits[ui];
            let qtys = qt.gran(&b.lineitem, pi, g, rows);
            let keys = ok.gran(&b.lineitem, pi, g, rows);
            for r in 0..rows as usize {
                qty[keys[r] as usize].fetch_add(sx4(qtys[r]) as i32, Ordering::Relaxed);
            }
        },
    );

    // 2. qualifying orderkeys (parallel array sweep).
    let chunks = threads * 16;
    let step = okey_max / chunks + 1;
    let quals: Vec<Vec<i64>> = par_range(
        chunks,
        threads,
        |_| Vec::new(),
        |out: &mut Vec<i64>, c| {
            let lo = c * step;
            let hi = ((c + 1) * step).min(okey_max + 1);
            for k in lo..hi {
                if qty[k].load(Ordering::Relaxed) as i64 > Q18_QTY_C {
                    out.push(k as i64);
                }
            }
        },
    );
    let mut hot: Vec<i64> = quals.into_iter().flatten().collect();
    hot.sort_unstable();
    let mut hotset: OaMap<()> = OaMap::with_capacity(hot.len().max(1));
    for &k in &hot {
        hotset.insert(k, ());
    }

    // 3. orders scan: membership probe on the tiny hot set; late-decode
    //    (decode_sel) custkey/date/totalprice for hit rows only.
    let ounits = granule_walk(&b.orders, 1);
    let orows: Vec<Vec<(i64, i64, i32, i64)>> = par_range(
        ounits.len(),
        threads,
        |_| (Col::new(1), CurCache::new(2), CurCache::new(5), CurCache::new(4), Scratch::new(), Vec::new()),
        |(ok, ck, od, tp, scr, out): &mut (
            Col,
            CurCache,
            CurCache,
            CurCache,
            Scratch,
            Vec<(i64, i64, i32, i64)>,
        ),
         ui| {
            let (pi, g, rows, _) = ounits[ui];
            let keys = ok.gran(&b.orders, pi, g, rows);
            let mut sel: Vec<u16> = Vec::new();
            let mut skeys: Vec<i64> = Vec::new();
            for r in 0..rows as usize {
                let k = keys[r] as i64;
                if hotset.slot(k) != usize::MAX {
                    sel.push(r as u16);
                    skeys.push(k);
                }
            }
            if sel.is_empty() {
                return;
            }
            let custs: Vec<u64> =
                scr.decode_sel(ck.get(&b.orders, pi), g, &sel).to_vec();
            let dates: Vec<u64> =
                scr.decode_sel(od.get(&b.orders, pi), g, &sel).to_vec();
            let totals: Vec<u64> =
                scr.decode_sel(tp.get(&b.orders, pi), g, &sel).to_vec();
            for i in 0..sel.len() {
                out.push((
                    skeys[i],
                    sx4(custs[i]),
                    sx4(dates[i]) as i32,
                    totals[i] as i64,
                ));
            }
        },
    )
    .into_iter()
    .map(|s| s.5)
    .collect();
    let mut rows: Vec<(i64, i64, i32, i64)> = orows.into_iter().flatten().collect();

    // 4. c_name for the needed custkeys (late-decode over customer).
    let mut ckset: OaMap<()> = OaMap::with_capacity(rows.len().max(1));
    for &(_, c, _, _) in &rows {
        ckset.insert(c, ());
    }
    let cunits = granule_walk(&b.customer, 1);
    let cnames: Vec<Vec<(i64, String)>> = par_range(
        cunits.len(),
        threads,
        |_| (Col::new(1), CurCache::new(2), Scratch::new(), Vec::new()),
        |(ck, nm, scr, out): &mut (Col, CurCache, Scratch, Vec<(i64, String)>), ui| {
            let (pi, g, rows, _) = cunits[ui];
            let keys = ck.gran(&b.customer, pi, g, rows);
            let mut sel: Vec<u16> = Vec::new();
            let mut skeys: Vec<i64> = Vec::new();
            for r in 0..rows as usize {
                let k = sx4(keys[r]);
                if ckset.slot(k) != usize::MAX {
                    sel.push(r as u16);
                    skeys.push(k);
                }
            }
            if sel.is_empty() {
                return;
            }
            let names = scr.decode_sel(nm.get(&b.customer, pi), g, &sel);
            for i in 0..sel.len() {
                let s =
                    String::from_utf8(unsafe { varlena_payload(names[i]) }.to_vec()).unwrap();
                out.push((skeys[i], s));
            }
        },
    )
    .into_iter()
    .map(|s| s.3)
    .collect();
    let mut namemap: OaMap<u32> = OaMap::with_capacity(rows.len().max(1));
    let flat: Vec<(i64, String)> = cnames.into_iter().flatten().collect();
    for (i, (k, _)) in flat.iter().enumerate() {
        namemap.insert(*k, i as u32);
    }

    // 5. top-100 (o_totalprice desc, o_orderdate asc, custkey, orderkey).
    rows.sort_by_key(|&(k, c, d, t)| (-t, d, c, k));
    rows.truncate(100);
    rows.iter()
        .map(|&(k, c, d, t)| {
            let name = &flat[namemap.vals[namemap.slot(c)] as usize].1;
            format!(
                "{name}\t{c}\t{k}\t{}\t{}\t{}",
                fmt_date(d),
                fmt_money2(t),
                fmt_money2(qty[k as usize].load(Ordering::Relaxed) as i64)
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Q1 — pricing summary. One lineitem pass, l_shipdate <= cutoff, group by
// (l_returnflag, l_linestatus): tiny fixed domain -> per-thread direct
// grid indexed by the two flag bytes (no hashing). Scales: qty/base s2,
// disc_price s4, charge s6, averages via the shared round-half-up law.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Default)]
struct Q1Cell {
    qty: i64,
    base: i64,
    disc_price: i64,
    charge: i64,
    disc: i64,
    cnt: i64,
}

pub fn q1(b: &TpchBanks, threads: usize) -> Vec<String> {
    let cutoff = q1_date() as i64;
    // idx = (returnflag & 31) << 1 | (linestatus & 1): 'F'(0x46) even,
    // 'O'(0x4F) odd; ascending idx IS (rf asc, ls asc) — the ORDER BY.
    const CELLS: usize = 64;
    let lunits = granule_walk(&b.lineitem, 1);
    let grids = par_range(
        lunits.len(),
        threads,
        |_| {
            (
                Col::new(5),
                Col::new(6),
                Col::new(7),
                Col::new(8),
                Col::new(9),
                Col::new(10),
                Col::new(11),
                vec![Q1Cell::default(); CELLS],
            )
        },
        |(qt, ep, di, tx, rf, ls, sd, grid): &mut (_, _, _, _, _, _, _, Vec<Q1Cell>), ui| {
            let (pi, g, rows, _) = lunits[ui];
            let ships = sd.gran(&b.lineitem, pi, g, rows);
            let qtys = qt.gran(&b.lineitem, pi, g, rows);
            let exts = ep.gran(&b.lineitem, pi, g, rows);
            let discs = di.gran(&b.lineitem, pi, g, rows);
            let taxes = tx.gran(&b.lineitem, pi, g, rows);
            let rfs = rf.gran(&b.lineitem, pi, g, rows);
            let lss = ls.gran(&b.lineitem, pi, g, rows);
            for r in 0..rows as usize {
                if sx4(ships[r]) <= cutoff {
                    let rfb = unsafe { varlena_payload(rfs[r]) }[0] as usize;
                    let lsb = unsafe { varlena_payload(lss[r]) }[0] as usize;
                    let ext = exts[r] as i64;
                    let d = sx4(discs[r]);
                    let t = sx4(taxes[r]);
                    let c = &mut grid[(rfb & 31) << 1 | (lsb & 1)];
                    c.qty += sx4(qtys[r]);
                    c.base += ext;
                    c.disc_price += ext * (100 - d);
                    c.charge += ext * (100 - d) * (100 + t);
                    c.disc += d;
                    c.cnt += 1;
                }
            }
        },
    );
    let mut grid = vec![Q1Cell::default(); CELLS];
    for s in &grids {
        for (a, v) in grid.iter_mut().zip(&s.7) {
            a.qty += v.qty;
            a.base += v.base;
            a.disc_price += v.disc_price;
            a.charge += v.charge;
            a.disc += v.disc;
            a.cnt += v.cnt;
        }
    }
    let mut out = Vec::new();
    for idx in 0..CELLS {
        let c = &grid[idx];
        if c.cnt == 0 {
            continue;
        }
        let rfc = (0x40 | (idx >> 1)) as u8 as char;
        let lsc = if idx & 1 == 1 { 'O' } else { 'F' };
        out.push(format!(
            "{rfc}\t{lsc}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            fmt_money2(c.qty),
            fmt_money2(c.base),
            fmt_money4(c.disc_price),
            fmt_money6(c.charge),
            fmt_avg6(c.qty, c.cnt, 2),
            fmt_avg6(c.base, c.cnt, 2),
            fmt_avg6(c.disc, c.cnt, 2),
            c.cnt
        ));
    }
    out
}

// ---------------------------------------------------------------------------
// Q12 — shipmode priority. orders scan -> orderkey -> priority class
// direct array over the sparse key domain (Q9's relaxed-store idiom);
// one lineitem pass with the three date preds + mode filter folding a
// per-thread [mode][class] grid. Group key domain is 2 — no hash state.
// ---------------------------------------------------------------------------

pub fn q12(b: &TpchBanks, threads: usize) -> Vec<String> {
    let (lo, hi) = q12_dates();
    let (lo, hi) = (lo as i64, hi as i64);

    // 1. orderkey -> priority class (2 = '1-URGENT'/'2-HIGH', 1 = low);
    //    single-writer-per-key relaxed stores.
    let okey_max = super::gen::orderkey_of(b.orders.rows_total() - 1) as usize;
    let pri: Vec<std::sync::atomic::AtomicU8> =
        (0..=okey_max).map(|_| std::sync::atomic::AtomicU8::new(0)).collect();
    let ounits = granule_walk(&b.orders, 1);
    par_range(
        ounits.len(),
        threads,
        |_| (Col::new(1), Col::new(6)),
        |(ok, op), ui| {
            let (pi, g, rows, _) = ounits[ui];
            let pris = op.gran(&b.orders, pi, g, rows);
            let keys = ok.gran(&b.orders, pi, g, rows);
            for r in 0..rows as usize {
                let p0 = unsafe { varlena_payload(pris[r]) }[0];
                let cls = if p0 == b'1' || p0 == b'2' { 2u8 } else { 1u8 };
                pri[keys[r] as usize].store(cls, Ordering::Relaxed);
            }
        },
    );

    // 2. lineitem pass: receipt window first (cheapest cut), then the
    //    commit/ship correlation preds, then the mode compare.
    let lunits = granule_walk(&b.lineitem, 1);
    let counts = par_range(
        lunits.len(),
        threads,
        |_| (Col::new(1), Col::new(11), Col::new(12), Col::new(13), Col::new(15), [[0i64; 2]; 2]),
        |(ok, sd, cd, rd, sm, cnt): &mut (_, _, _, _, _, [[i64; 2]; 2]), ui| {
            let (pi, g, rows, _) = lunits[ui];
            let receipts = rd.gran(&b.lineitem, pi, g, rows);
            let commits = cd.gran(&b.lineitem, pi, g, rows);
            let ships = sd.gran(&b.lineitem, pi, g, rows);
            let modes = sm.gran(&b.lineitem, pi, g, rows);
            let keys = ok.gran(&b.lineitem, pi, g, rows);
            for r in 0..rows as usize {
                let rc = sx4(receipts[r]);
                if rc < lo || rc >= hi {
                    continue;
                }
                let cm = sx4(commits[r]);
                if cm >= rc || sx4(ships[r]) >= cm {
                    continue;
                }
                let m = unsafe { varlena_payload(modes[r]) };
                let mi = if m == Q12_MODE_A {
                    0usize
                } else if m == Q12_MODE_B {
                    1
                } else {
                    continue;
                };
                let high = pri[keys[r] as usize].load(Ordering::Relaxed) == 2;
                cnt[mi][(!high) as usize] += 1;
            }
        },
    );
    let mut cnt = [[0i64; 2]; 2];
    for s in &counts {
        for m in 0..2 {
            for c in 0..2 {
                cnt[m][c] += s.5[m][c];
            }
        }
    }
    let mut out = Vec::new();
    for (mi, name) in [(0usize, "MAIL"), (1, "SHIP")] {
        let (h, l) = (cnt[mi][0], cnt[mi][1]);
        if h + l > 0 {
            out.push(format!("{name}\t{h}\t{l}"));
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Q14 — promo revenue. part scan -> PROMO partkey bitset (prefix compare
// on decoded p_type payload — the Q9 bitset idiom); one lineitem pass
// with the shipdate window folding (promo, total) s4 sums; the single
// answer cell renders through the shared percentage law.
// ---------------------------------------------------------------------------

pub fn q14(b: &TpchBanks, threads: usize) -> Vec<String> {
    let (lo, hi) = q14_dates();
    let (lo, hi) = (lo as i64, hi as i64);

    // 1. PROMO partkey bitset.
    let pcount = b.part.rows_total() as usize;
    let words = pcount / 64 + 2;
    let punits = granule_walk(&b.part, 1);
    let bitsets = par_range(
        punits.len(),
        threads,
        |_| (Col::new(1), Col::new(5), vec![0u64; words]),
        |(pk, pt, bits), ui| {
            let (pi, g, rows, _) = punits[ui];
            let types = pt.gran(&b.part, pi, g, rows);
            let keys = pk.gran(&b.part, pi, g, rows);
            for r in 0..rows as usize {
                let p = unsafe { varlena_payload(types[r]) };
                if p.starts_with(Q14_PROMO) {
                    let k = sx4(keys[r]) as usize;
                    bits[k >> 6] |= 1 << (k & 63);
                }
            }
        },
    );
    let mut bits = vec![0u64; words];
    for (_, _, tb) in &bitsets {
        for (a, v) in bits.iter_mut().zip(tb) {
            *a |= v;
        }
    }

    // 2. lineitem pass: shipdate window; fold (promo, total) revenue.
    let lunits = granule_walk(&b.lineitem, 1);
    let sums = par_range(
        lunits.len(),
        threads,
        |_| (Col::new(2), Col::new(6), Col::new(7), Col::new(11), (0i64, 0i64)),
        |(pk, ep, di, sd, acc): &mut (_, _, _, _, (i64, i64)), ui| {
            let (pi, g, rows, _) = lunits[ui];
            let ships = sd.gran(&b.lineitem, pi, g, rows);
            let parts_ = pk.gran(&b.lineitem, pi, g, rows);
            let exts = ep.gran(&b.lineitem, pi, g, rows);
            let discs = di.gran(&b.lineitem, pi, g, rows);
            for r in 0..rows as usize {
                let s = sx4(ships[r]);
                if s >= lo && s < hi {
                    let rev = exts[r] as i64 * (100 - sx4(discs[r]));
                    acc.1 += rev;
                    let p = sx4(parts_[r]) as usize;
                    if bits[p >> 6] >> (p & 63) & 1 != 0 {
                        acc.0 += rev;
                    }
                }
            }
        },
    );
    let (mut promo, mut total) = (0i64, 0i64);
    for (_, _, _, _, (p, t)) in &sums {
        promo += *p;
        total += *t;
    }
    if total == 0 {
        return Vec::new(); // SQL answers NULL at degenerate SF; canon = no row.
    }
    vec![fmt_pct6(promo, total)]
}

// ---------------------------------------------------------------------------
// Q5 — local supplier volume. region/nation (tiny scalar scans) ->
// in-region membership; customer -> in-region nation direct array;
// supplier -> nation direct array; orders scan (date window + member
// customer) -> map orderkey -> customer nation; ONE lineitem pass
// probing the map and matching supplier nation, fold per-nation s4
// revenue. Order: revenue desc (tie: n_name asc — canonical law).
// ---------------------------------------------------------------------------

pub fn q5(b: &TpchBanks, threads: usize) -> Vec<String> {
    let (lo, hi) = q5_dates();
    let (lo, hi) = (lo as i64, hi as i64);

    // 1. ASIA regionkey + nation names/membership (scalar; 5 + 25 rows).
    let mut region_key = -1i64;
    {
        let runits = granule_walk(&b.region, 1);
        let mut kc = Col::new(1);
        let mut nc = Col::new(2);
        for &(pi, g, rows, _) in &runits {
            let nm = nc.gran(&b.region, pi, g, rows);
            let ks = kc.gran(&b.region, pi, g, rows);
            for r in 0..rows as usize {
                if unsafe { varlena_payload(nm[r]) } == Q5_REGION {
                    region_key = sx4(ks[r]);
                }
            }
        }
    }
    let mut nat_name: Vec<String> = (0..25).map(|_| String::new()).collect();
    let mut in_region = [false; 25];
    {
        let nunits = granule_walk(&b.nation, 1);
        let mut kc = Col::new(1);
        let mut nc = Col::new(2);
        let mut rc = Col::new(3);
        for &(pi, g, rows, _) in &nunits {
            let nm = nc.gran(&b.nation, pi, g, rows);
            let rks = rc.gran(&b.nation, pi, g, rows);
            let ks = kc.gran(&b.nation, pi, g, rows);
            for r in 0..rows as usize {
                let k = sx4(ks[r]) as usize;
                nat_name[k] = txt(nm[r]);
                in_region[k] = sx4(rks[r]) == region_key;
            }
        }
    }

    // 2. customer -> in-region nation + 1 (0 = out), dense direct array.
    let ccount = b.customer.rows_total() as usize;
    let mut c_nat = vec![0u8; ccount + 1];
    {
        let cunits = granule_walk(&b.customer, 1);
        let mut kc = Col::new(1);
        let mut nc = Col::new(4);
        for &(pi, g, rows, _) in &cunits {
            let nats = nc.gran(&b.customer, pi, g, rows);
            let keys = kc.gran(&b.customer, pi, g, rows);
            for r in 0..rows as usize {
                let n = sx4(nats[r]) as usize;
                if in_region[n] {
                    c_nat[sx4(keys[r]) as usize] = n as u8 + 1;
                }
            }
        }
    }

    // 3. supplier -> nation direct array (the Q9 idiom).
    let scount = b.supplier.rows_total() as usize;
    let mut s_nation = vec![0u8; scount + 1];
    {
        let sunits = granule_walk(&b.supplier, 1);
        let mut sk = Col::new(1);
        let mut nk = Col::new(4);
        for &(pi, g, rows, _) in &sunits {
            let nats = nk.gran(&b.supplier, pi, g, rows);
            let keys = sk.gran(&b.supplier, pi, g, rows);
            for r in 0..rows as usize {
                s_nation[sx4(keys[r]) as usize] = sx4(nats[r]) as u8;
            }
        }
    }

    // 4. orders scan: date window + in-region customer -> orderkey -> nation.
    let ounits = granule_walk(&b.orders, 1);
    let parts: Vec<Vec<(i64, u8)>> = par_range(
        ounits.len(),
        threads,
        |_| (Col::new(1), Col::new(2), Col::new(5), Vec::new()),
        |(ok, ck, od, out): &mut (_, _, _, Vec<(i64, u8)>), ui| {
            let (pi, g, rows, _) = ounits[ui];
            let dates = od.gran(&b.orders, pi, g, rows);
            let custs = ck.gran(&b.orders, pi, g, rows);
            let keys = ok.gran(&b.orders, pi, g, rows);
            for r in 0..rows as usize {
                let d = sx4(dates[r]);
                if d < lo || d >= hi {
                    continue;
                }
                let n = c_nat[sx4(custs[r]) as usize];
                if n != 0 {
                    out.push((keys[r] as i64, n - 1));
                }
            }
        },
    )
    .into_iter()
    .map(|s| s.3)
    .collect();
    let n: usize = parts.iter().map(|v| v.len()).sum();
    let mut omap: OaMap<u8> = OaMap::with_capacity(n.max(1));
    for v in &parts {
        for &(k, nat) in v {
            omap.insert(k, nat);
        }
    }

    // 5. lineitem pass: probe orders map, match supplier nation, fold.
    let lunits = granule_walk(&b.lineitem, 1);
    let grids = par_range(
        lunits.len(),
        threads,
        |_| (Col::new(1), Col::new(3), Col::new(6), Col::new(7), vec![(0i64, 0u32); 25]),
        |(ok, sk, ep, di, grid): &mut (_, _, _, _, Vec<(i64, u32)>), ui| {
            let (pi, g, rows, _) = lunits[ui];
            let keys = ok.gran(&b.lineitem, pi, g, rows);
            let supps = sk.gran(&b.lineitem, pi, g, rows);
            let exts = ep.gran(&b.lineitem, pi, g, rows);
            let discs = di.gran(&b.lineitem, pi, g, rows);
            for r in 0..rows as usize {
                let s = omap.slot(keys[r] as i64);
                if s == usize::MAX {
                    continue;
                }
                let nat = omap.vals[s];
                if s_nation[sx4(supps[r]) as usize] == nat {
                    let cell = &mut grid[nat as usize];
                    cell.0 += exts[r] as i64 * (100 - sx4(discs[r]));
                    cell.1 += 1;
                }
            }
        },
    );
    let mut grid = vec![(0i64, 0u32); 25];
    for s in &grids {
        for (a, v) in grid.iter_mut().zip(&s.4) {
            a.0 += v.0;
            a.1 += v.1;
        }
    }

    // 6. render: revenue desc, n_name asc on ties.
    let mut rows: Vec<(i64, &str)> = (0..25)
        .filter(|&nk| grid[nk].1 > 0)
        .map(|nk| (-grid[nk].0, nat_name[nk].as_str()))
        .collect();
    rows.sort();
    rows.iter().map(|&(nrev, name)| format!("{name}\t{}", fmt_money4(-nrev))).collect()
}

// ---------------------------------------------------------------------------
// Q10 — returned items. orders scan (3-month window) -> map orderkey ->
// custkey; one lineitem pass (l_returnflag = 'R') folding s4 revenue
// into a custkey-indexed DIRECT array (dense group domain — the §6.6
// election the floor prices); top-20 (revenue desc, custkey asc); then
// two needle late-decodes (customer fields via decode_sel, nation
// names) — the Q18 composition lesson.
// ---------------------------------------------------------------------------

pub fn q10(b: &TpchBanks, threads: usize) -> Vec<String> {
    let (lo, hi) = q10_dates();
    let (lo, hi) = (lo as i64, hi as i64);

    // 1. orders in window -> orderkey -> custkey.
    let ounits = granule_walk(&b.orders, 1);
    let parts: Vec<Vec<(i64, u32)>> = par_range(
        ounits.len(),
        threads,
        |_| (Col::new(1), Col::new(2), Col::new(5), Vec::new()),
        |(ok, ck, od, out): &mut (_, _, _, Vec<(i64, u32)>), ui| {
            let (pi, g, rows, _) = ounits[ui];
            let dates = od.gran(&b.orders, pi, g, rows);
            let custs = ck.gran(&b.orders, pi, g, rows);
            let keys = ok.gran(&b.orders, pi, g, rows);
            for r in 0..rows as usize {
                let d = sx4(dates[r]);
                if d >= lo && d < hi {
                    out.push((keys[r] as i64, sx4(custs[r]) as u32));
                }
            }
        },
    )
    .into_iter()
    .map(|s| s.3)
    .collect();
    let n: usize = parts.iter().map(|v| v.len()).sum();
    let mut omap: OaMap<u32> = OaMap::with_capacity(n.max(1));
    for v in &parts {
        for &(k, c) in v {
            omap.insert(k, c);
        }
    }

    // 2. lineitem pass: 'R' rows, probe, fold into per-thread custkey
    //    direct arrays (dense domain beats hash grouped state).
    let ccount = b.customer.rows_total() as usize;
    let lunits = granule_walk(&b.lineitem, 1);
    let revs = par_range(
        lunits.len(),
        threads,
        |_| (Col::new(1), Col::new(6), Col::new(7), Col::new(9), vec![0i64; ccount + 1]),
        |(ok, ep, di, rf, rev): &mut (_, _, _, _, Vec<i64>), ui| {
            let (pi, g, rows, _) = lunits[ui];
            let keys = ok.gran(&b.lineitem, pi, g, rows);
            let flags = rf.gran(&b.lineitem, pi, g, rows);
            let exts = ep.gran(&b.lineitem, pi, g, rows);
            let discs = di.gran(&b.lineitem, pi, g, rows);
            for r in 0..rows as usize {
                if unsafe { varlena_payload(flags[r]) }[0] != b'R' {
                    continue;
                }
                let s = omap.slot(keys[r] as i64);
                if s != usize::MAX {
                    rev[omap.vals[s] as usize] += exts[r] as i64 * (100 - sx4(discs[r]));
                }
            }
        },
    );
    let mut total = vec![0i64; ccount + 1];
    for s in &revs {
        for (a, v) in total.iter_mut().zip(&s.4) {
            *a += v;
        }
    }

    // 3. top-20 (revenue desc, custkey asc) insertion select.
    let mut top: Vec<(i64, u32)> = Vec::with_capacity(21);
    for ck in 1..=ccount {
        if total[ck] > 0 {
            let cand = (-total[ck], ck as u32);
            if top.len() < 20 {
                top.push(cand);
                top.sort();
            } else if cand < top[19] {
                top[19] = cand;
                top.sort();
            }
        }
    }

    // 4. late-decode customer fields for the <=20 hit custkeys. Each
    //    varlena decode_sel is materialized to owned Strings BEFORE the
    //    next decode reuses the scratch.
    let mut ckset: OaMap<()> = OaMap::with_capacity(top.len().max(1));
    for &(_, ck) in &top {
        ckset.insert(ck as i64, ());
    }
    let cunits = granule_walk(&b.customer, 1);
    type CustRow = (i64, String, String, u8, String, i64, String);
    let fields: Vec<Vec<CustRow>> = par_range(
        cunits.len(),
        threads,
        |_| {
            (
                Col::new(1),
                CurCache::new(2),
                CurCache::new(3),
                CurCache::new(4),
                CurCache::new(5),
                CurCache::new(6),
                CurCache::new(8),
                Scratch::new(),
                Vec::new(),
            )
        },
        |(kc, nm, ad, nk, ph, ab, cm, scr, out): &mut (
            Col,
            CurCache,
            CurCache,
            CurCache,
            CurCache,
            CurCache,
            CurCache,
            Scratch,
            Vec<CustRow>,
        ),
         ui| {
            let (pi, g, rows, _) = cunits[ui];
            let keys = kc.gran(&b.customer, pi, g, rows);
            let mut sel: Vec<u16> = Vec::new();
            let mut skeys: Vec<i64> = Vec::new();
            for r in 0..rows as usize {
                let k = sx4(keys[r]);
                if ckset.slot(k) != usize::MAX {
                    sel.push(r as u16);
                    skeys.push(k);
                }
            }
            if sel.is_empty() {
                return;
            }
            let names: Vec<String> =
                scr.decode_sel(nm.get(&b.customer, pi), g, &sel).iter().map(|&v| txt(v)).collect();
            let addrs: Vec<String> =
                scr.decode_sel(ad.get(&b.customer, pi), g, &sel).iter().map(|&v| txt(v)).collect();
            let nats: Vec<u8> = scr
                .decode_sel(nk.get(&b.customer, pi), g, &sel)
                .iter()
                .map(|&v| sx4(v) as u8)
                .collect();
            let phones: Vec<String> =
                scr.decode_sel(ph.get(&b.customer, pi), g, &sel).iter().map(|&v| txt(v)).collect();
            let bals: Vec<i64> =
                scr.decode_sel(ab.get(&b.customer, pi), g, &sel).iter().map(|&v| v as i64).collect();
            let comms: Vec<String> =
                scr.decode_sel(cm.get(&b.customer, pi), g, &sel).iter().map(|&v| txt(v)).collect();
            for i in 0..sel.len() {
                out.push((
                    skeys[i],
                    names[i].clone(),
                    addrs[i].clone(),
                    nats[i],
                    phones[i].clone(),
                    bals[i],
                    comms[i].clone(),
                ));
            }
        },
    )
    .into_iter()
    .map(|s| s.8)
    .collect();
    let flat: Vec<CustRow> = fields.into_iter().flatten().collect();
    let mut fmap: OaMap<u32> = OaMap::with_capacity(flat.len().max(1));
    for (i, f) in flat.iter().enumerate() {
        fmap.insert(f.0, i as u32);
    }

    // 5. nation names (scalar; 25 rows) + render.
    let mut nat_name: Vec<String> = (0..25).map(|_| String::new()).collect();
    {
        let nunits = granule_walk(&b.nation, 1);
        let mut kc = Col::new(1);
        let mut nc = Col::new(2);
        for &(pi, g, rows, _) in &nunits {
            let nm = nc.gran(&b.nation, pi, g, rows);
            let ks = kc.gran(&b.nation, pi, g, rows);
            for r in 0..rows as usize {
                nat_name[sx4(ks[r]) as usize] = txt(nm[r]);
            }
        }
    }
    top.iter()
        .map(|&(nrev, ck)| {
            let f = &flat[fmap.vals[fmap.slot(ck as i64)] as usize];
            format!(
                "{ck}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                f.1,
                fmt_money4(-nrev),
                fmt_money2(f.5),
                nat_name[f.3 as usize],
                f.2,
                f.4,
                f.6
            )
        })
        .collect()
}
