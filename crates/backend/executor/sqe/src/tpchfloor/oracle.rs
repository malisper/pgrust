//! Independent scalar reference for Q3/Q9/Q18 over the IN-MEMORY dataset
//! (never the banks; never the floors' data structures): row-at-a-time
//! loops + std BTreeMap/HashMap. The tiny-SF nested-loop oracle (a third
//! implementation, genuinely O(n*m)) lives in tests/tpch_floor_identity.rs.
//! Shares ONLY the render seam (mod.rs fmt_*) with the floors — byte-law.

use super::gen::{
    address, c_name, comment, p_name, phone, Tpch, MODES, NATIONS, REGIONS, SEGMENTS, TYPES,
};
use super::*;
use std::collections::{BTreeMap, HashMap};

pub fn q3(d: &Tpch) -> Vec<String> {
    let cutoff = q3_date();
    let mut building: Vec<bool> = vec![false; d.cust.n as usize + 1];
    for i in 0..d.cust.n as usize {
        building[i + 1] = SEGMENTS[d.cust.segment[i] as usize].as_bytes() == Q3_SEGMENT;
    }
    let mut okinfo: HashMap<i64, (i32, i32)> = HashMap::new();
    for i in 0..d.ord.orderkey.len() {
        if d.ord.orderdate[i] < cutoff && building[d.ord.custkey[i] as usize] {
            okinfo.insert(d.ord.orderkey[i], (d.ord.orderdate[i], 0));
        }
    }
    let mut rev: BTreeMap<i64, i64> = BTreeMap::new();
    for i in 0..d.li.orderkey.len() {
        if d.li.shipdate[i] > cutoff {
            if okinfo.contains_key(&d.li.orderkey[i]) {
                *rev.entry(d.li.orderkey[i]).or_insert(0) +=
                    d.li.extprice_c[i] * (100 - d.li.discount[i] as i64);
            }
        }
    }
    let mut rows: Vec<(i64, i32, i64, i32)> = rev
        .iter()
        .map(|(&k, &r)| {
            let (dte, pri) = okinfo[&k];
            (-r, dte, k, pri)
        })
        .collect();
    rows.sort();
    rows.truncate(10);
    rows.iter()
        .map(|&(nr, dte, k, p)| format!("{k}\t{}\t{}\t{p}", fmt_money4(-nr), fmt_date(dte)))
        .collect()
}

pub fn q9(d: &Tpch) -> Vec<String> {
    let green: Vec<bool> = (0..d.part.n as usize)
        .map(|i| {
            let name = p_name(d.part.name_words[i]);
            name.as_bytes()
                .windows(Q9_COLOR.len())
                .any(|w| w == Q9_COLOR)
        })
        .collect();
    let mut cost: HashMap<(u32, u32), i64> = HashMap::new();
    for i in 0..d.ps.partkey.len() {
        cost.insert((d.ps.partkey[i], d.ps.suppkey[i]), d.ps.supplycost_c[i] as i64);
    }
    let mut oyear: HashMap<i64, i32> = HashMap::new();
    for i in 0..d.ord.orderkey.len() {
        oyear.insert(d.ord.orderkey[i], date_year(d.ord.orderdate[i]));
    }
    // (nation name, -year) -> (sum, count)
    let mut grid: BTreeMap<(&'static str, i32), (i64, u64)> = BTreeMap::new();
    for i in 0..d.li.orderkey.len() {
        let pk = d.li.partkey[i];
        if !green[pk as usize - 1] {
            continue;
        }
        let sk = d.li.suppkey[i];
        let c = cost[&(pk, sk)];
        let y = oyear[&d.li.orderkey[i]];
        let amount = d.li.extprice_c[i] * (100 - d.li.discount[i] as i64)
            - c * d.li.quantity_c[i] as i64;
        let nat = NATIONS[d.supp.nationkey[sk as usize - 1] as usize].0;
        let e = grid.entry((nat, -y)).or_insert((0, 0));
        e.0 += amount;
        e.1 += 1;
    }
    grid.iter()
        .map(|(&(nat, ny), &(sum, _))| format!("{nat}\t{}\t{}", -ny, fmt_money4(sum)))
        .collect()
}

pub fn q18(d: &Tpch) -> Vec<String> {
    let mut qty: HashMap<i64, i64> = HashMap::new();
    for i in 0..d.li.orderkey.len() {
        *qty.entry(d.li.orderkey[i]).or_insert(0) += d.li.quantity_c[i] as i64;
    }
    let mut rows: Vec<(i64, i32, i64, i64, i64)> = Vec::new(); // (-total, date, custkey, okey, qty)
    for i in 0..d.ord.orderkey.len() {
        let k = d.ord.orderkey[i];
        if let Some(&q) = qty.get(&k) {
            if q > Q18_QTY_C {
                rows.push((
                    -d.ord.totalprice[i],
                    d.ord.orderdate[i],
                    d.ord.custkey[i] as i64,
                    k,
                    q,
                ));
            }
        }
    }
    rows.sort();
    rows.truncate(100);
    rows.iter()
        .map(|&(nt, dte, c, k, q)| {
            format!(
                "{}\t{c}\t{k}\t{}\t{}\t{}",
                c_name(c as u32),
                fmt_date(dte),
                fmt_money2(-nt),
                fmt_money2(q)
            )
        })
        .collect()
}

pub fn q1(d: &Tpch) -> Vec<String> {
    let cutoff = q1_date();
    // (returnflag, linestatus) byte order == the query's ORDER BY.
    let mut grid: BTreeMap<(u8, u8), (i64, i64, i64, i64, i64, i64)> = BTreeMap::new();
    for i in 0..d.li.orderkey.len() {
        if d.li.shipdate[i] > cutoff {
            continue;
        }
        let ext = d.li.extprice_c[i];
        let disc = d.li.discount[i] as i64;
        let tax = d.li.tax[i] as i64;
        let e = grid
            .entry((d.li.returnflag[i], d.li.linestatus[i]))
            .or_insert((0, 0, 0, 0, 0, 0));
        e.0 += d.li.quantity_c[i] as i64;
        e.1 += ext;
        e.2 += ext * (100 - disc);
        e.3 += ext * (100 - disc) * (100 + tax);
        e.4 += disc;
        e.5 += 1;
    }
    grid.iter()
        .map(|(&(rf, ls), &(q, bp, dp, ch, dc, n))| {
            format!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{n}",
                rf as char,
                ls as char,
                fmt_money2(q),
                fmt_money2(bp),
                fmt_money4(dp),
                fmt_money6(ch),
                fmt_avg6(q, n, 2),
                fmt_avg6(bp, n, 2),
                fmt_avg6(dc, n, 2)
            )
        })
        .collect()
}

pub fn q5(d: &Tpch) -> Vec<String> {
    let (lo, hi) = q5_dates();
    let region = REGIONS.iter().position(|r| r.as_bytes() == Q5_REGION).unwrap() as u8;
    let mut omap: HashMap<i64, u8> = HashMap::new();
    for i in 0..d.ord.orderkey.len() {
        let od = d.ord.orderdate[i];
        if od < lo || od >= hi {
            continue;
        }
        let nk = d.cust.nationkey[d.ord.custkey[i] as usize - 1];
        if NATIONS[nk as usize].1 == region {
            omap.insert(d.ord.orderkey[i], nk);
        }
    }
    let mut grid: HashMap<u8, i64> = HashMap::new();
    for i in 0..d.li.orderkey.len() {
        if let Some(&nk) = omap.get(&d.li.orderkey[i]) {
            if d.supp.nationkey[d.li.suppkey[i] as usize - 1] == nk {
                *grid.entry(nk).or_insert(0) +=
                    d.li.extprice_c[i] * (100 - d.li.discount[i] as i64);
            }
        }
    }
    let mut rows: Vec<(i64, &str)> =
        grid.iter().map(|(&nk, &s)| (-s, NATIONS[nk as usize].0)).collect();
    rows.sort();
    rows.iter().map(|&(ns, name)| format!("{name}\t{}", fmt_money4(-ns))).collect()
}

pub fn q10(d: &Tpch) -> Vec<String> {
    let (lo, hi) = q10_dates();
    let mut omap: HashMap<i64, u32> = HashMap::new();
    for i in 0..d.ord.orderkey.len() {
        let od = d.ord.orderdate[i];
        if od >= lo && od < hi {
            omap.insert(d.ord.orderkey[i], d.ord.custkey[i]);
        }
    }
    let mut rev: HashMap<u32, i64> = HashMap::new();
    for i in 0..d.li.orderkey.len() {
        if d.li.returnflag[i] != b'R' {
            continue;
        }
        if let Some(&ck) = omap.get(&d.li.orderkey[i]) {
            *rev.entry(ck).or_insert(0) += d.li.extprice_c[i] * (100 - d.li.discount[i] as i64);
        }
    }
    let mut rows: Vec<(i64, u32)> = rev.iter().map(|(&ck, &r)| (-r, ck)).collect();
    rows.sort();
    rows.truncate(20);
    rows.iter()
        .map(|&(nr, ck)| {
            let i = ck as usize - 1;
            let nk = d.cust.nationkey[i];
            format!(
                "{ck}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                c_name(ck),
                fmt_money4(-nr),
                fmt_money2(d.cust.acctbal[i] as i64),
                NATIONS[nk as usize].0,
                address('c', ck),
                phone(nk, ck),
                comment('c', i as u64)
            )
        })
        .collect()
}

pub fn q12(d: &Tpch) -> Vec<String> {
    let (lo, hi) = q12_dates();
    let mut pri: HashMap<i64, u8> = HashMap::new();
    for i in 0..d.ord.orderkey.len() {
        pri.insert(d.ord.orderkey[i], d.ord.priority[i]);
    }
    let mut grid: BTreeMap<&'static str, (i64, i64)> = BTreeMap::new();
    for i in 0..d.li.orderkey.len() {
        let m = MODES[d.li.mode[i] as usize];
        if m.as_bytes() != Q12_MODE_A && m.as_bytes() != Q12_MODE_B {
            continue;
        }
        let rc = d.li.receiptdate[i];
        if rc < lo || rc >= hi {
            continue;
        }
        if d.li.commitdate[i] >= rc || d.li.shipdate[i] >= d.li.commitdate[i] {
            continue;
        }
        let high = pri[&d.li.orderkey[i]] <= 1; // '1-URGENT' | '2-HIGH'
        let e = grid.entry(m).or_insert((0, 0));
        if high {
            e.0 += 1;
        } else {
            e.1 += 1;
        }
    }
    grid.iter().map(|(&m, &(h, l))| format!("{m}\t{h}\t{l}")).collect()
}

pub fn q14(d: &Tpch) -> Vec<String> {
    let (lo, hi) = q14_dates();
    let (mut promo, mut total) = (0i64, 0i64);
    for i in 0..d.li.orderkey.len() {
        let s = d.li.shipdate[i];
        if s < lo || s >= hi {
            continue;
        }
        let rev = d.li.extprice_c[i] * (100 - d.li.discount[i] as i64);
        total += rev;
        let ty = TYPES[d.part.ptype[d.li.partkey[i] as usize - 1] as usize];
        if ty.as_bytes().starts_with(Q14_PROMO) {
            promo += rev;
        }
    }
    if total == 0 {
        return Vec::new(); // SQL answers NULL at degenerate SF; canon = no row.
    }
    vec![fmt_pct6(promo, total)]
}
