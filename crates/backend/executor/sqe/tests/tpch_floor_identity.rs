//! P4-2a byte-law gate: the handwritten Q1/Q3/Q5/Q9/Q10/Q12/Q14/Q18
//! floors (over banks
//! sealed through the real writer) vs TWO independent references —
//! (1) a genuine nested-loop oracle (this file, row-at-a-time, no hash
//! tables) at tiny SF, and (2) the BTree scalar reference
//! (tpchfloor::oracle) at a multi-part SF. Identity is rendered-line
//! byte equality through the ONE render seam.

use sqe::tpchfloor::gen::{
    self, address, c_name, comment, p_name, phone, Tpch, MODES, NATIONS, REGIONS, SEGMENTS, TYPES,
};
use sqe::tpchfloor::{
    date_year, floors, fmt_avg6, fmt_date, fmt_money2, fmt_money4, fmt_money6, fmt_pct6, ingest,
    oracle, q10_dates, q12_dates, q14_dates, q1_date, q3_date, q5_dates, Q18_QTY_C, Q3_SEGMENT,
    Q9_COLOR,
};

fn tmpdir(tag: &str) -> String {
    let d = std::env::temp_dir().join(format!("sqe-tpch-floor-{tag}-{}", std::process::id()));
    let s = d.to_str().unwrap().to_string();
    if d.exists() {
        std::fs::remove_dir_all(&d).unwrap();
    }
    s
}

// ---------------------------------------------------------------------------
// The nested-loop oracle: O(n*m) loops, zero shared machinery with the
// floors (the join_identity oracle discipline).
// ---------------------------------------------------------------------------

fn nested_q3(d: &Tpch) -> Vec<String> {
    let cutoff = q3_date();
    let mut groups: Vec<(i64, i32, i32, i64)> = Vec::new(); // okey, date, prio, rev
    for oi in 0..d.ord.orderkey.len() {
        if d.ord.orderdate[oi] >= cutoff {
            continue;
        }
        // find the customer by scanning (nested loop).
        let mut seg_ok = false;
        for ci in 0..d.cust.n as usize {
            if ci as u32 + 1 == d.ord.custkey[oi] {
                seg_ok = SEGMENTS[d.cust.segment[ci] as usize].as_bytes() == Q3_SEGMENT;
                break;
            }
        }
        if !seg_ok {
            continue;
        }
        let mut rev = 0i64;
        let mut any = false;
        for li in 0..d.li.orderkey.len() {
            if d.li.orderkey[li] == d.ord.orderkey[oi] && d.li.shipdate[li] > cutoff {
                rev += d.li.extprice_c[li] * (100 - d.li.discount[li] as i64);
                any = true;
            }
        }
        if any {
            groups.push((d.ord.orderkey[oi], d.ord.orderdate[oi], 0, rev));
        }
    }
    groups.sort_by_key(|&(k, dt, _, r)| (-r, dt, k));
    groups.truncate(10);
    groups
        .iter()
        .map(|&(k, dt, p, r)| format!("{k}\t{}\t{}\t{p}", fmt_money4(r), fmt_date(dt)))
        .collect()
}

fn nested_q9(d: &Tpch) -> Vec<String> {
    // (nation, year) -> sum; discovered pairs kept in insertion-free sorted render.
    let mut sums = vec![0i64; 25 * 16];
    let mut hit = vec![false; 25 * 16];
    for li in 0..d.li.orderkey.len() {
        let pk = d.li.partkey[li];
        let name = p_name(d.part.name_words[pk as usize - 1]);
        if !name
            .as_bytes()
            .windows(Q9_COLOR.len())
            .any(|w| w == Q9_COLOR)
        {
            continue;
        }
        let mut cost = None;
        for pi in 0..d.ps.partkey.len() {
            if d.ps.partkey[pi] == pk && d.ps.suppkey[pi] == d.li.suppkey[li] {
                cost = Some(d.ps.supplycost_c[pi] as i64);
                break;
            }
        }
        let cost = cost.expect("partsupp row");
        let mut year = 0;
        for oi in 0..d.ord.orderkey.len() {
            if d.ord.orderkey[oi] == d.li.orderkey[li] {
                year = date_year(d.ord.orderdate[oi]);
                break;
            }
        }
        let nat = d.supp.nationkey[d.li.suppkey[li] as usize - 1] as usize;
        let amount = d.li.extprice_c[li] * (100 - d.li.discount[li] as i64)
            - cost * d.li.quantity_c[li] as i64;
        let slot = nat * 16 + (year - 1991) as usize;
        sums[slot] += amount;
        hit[slot] = true;
    }
    let mut order: Vec<(&str, usize)> =
        (0..25).map(|n| (NATIONS[n].0, n)).collect();
    order.sort();
    let mut out = Vec::new();
    for (name, n) in order {
        for y in (1..16).rev() {
            if hit[n * 16 + y] {
                out.push(format!("{name}\t{}\t{}", 1991 + y, fmt_money4(sums[n * 16 + y])));
            }
        }
    }
    out
}

fn nested_q18(d: &Tpch) -> Vec<String> {
    let mut rows: Vec<(i64, i32, i64, i64, i64)> = Vec::new();
    for oi in 0..d.ord.orderkey.len() {
        let mut q = 0i64;
        for li in 0..d.li.orderkey.len() {
            if d.li.orderkey[li] == d.ord.orderkey[oi] {
                q += d.li.quantity_c[li] as i64;
            }
        }
        if q > Q18_QTY_C {
            rows.push((
                -d.ord.totalprice[oi],
                d.ord.orderdate[oi],
                d.ord.custkey[oi] as i64,
                d.ord.orderkey[oi],
                q,
            ));
        }
    }
    rows.sort();
    rows.truncate(100);
    rows.iter()
        .map(|&(nt, dt, c, k, q)| {
            format!(
                "{}\t{c}\t{k}\t{}\t{}\t{}",
                c_name(c as u32),
                fmt_date(dt),
                fmt_money2(-nt),
                fmt_money2(q)
            )
        })
        .collect()
}

fn nested_q1(d: &Tpch) -> Vec<String> {
    let cutoff = q1_date();
    // Discovered groups, linear-searched — no maps.
    let mut keys: Vec<(u8, u8)> = Vec::new();
    let mut cells: Vec<[i64; 6]> = Vec::new();
    for i in 0..d.li.orderkey.len() {
        if d.li.shipdate[i] > cutoff {
            continue;
        }
        let k = (d.li.returnflag[i], d.li.linestatus[i]);
        let gi = match keys.iter().position(|&x| x == k) {
            Some(g) => g,
            None => {
                keys.push(k);
                cells.push([0; 6]);
                keys.len() - 1
            }
        };
        let ext = d.li.extprice_c[i];
        let disc = d.li.discount[i] as i64;
        let tax = d.li.tax[i] as i64;
        let c = &mut cells[gi];
        c[0] += d.li.quantity_c[i] as i64;
        c[1] += ext;
        c[2] += ext * (100 - disc);
        c[3] += ext * (100 - disc) * (100 + tax);
        c[4] += disc;
        c[5] += 1;
    }
    let mut order: Vec<usize> = (0..keys.len()).collect();
    order.sort_by_key(|&g| keys[g]);
    order
        .iter()
        .map(|&g| {
            let (rf, ls) = keys[g];
            let c = cells[g];
            format!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                rf as char,
                ls as char,
                fmt_money2(c[0]),
                fmt_money2(c[1]),
                fmt_money4(c[2]),
                fmt_money6(c[3]),
                fmt_avg6(c[0], c[5], 2),
                fmt_avg6(c[1], c[5], 2),
                fmt_avg6(c[4], c[5], 2),
                c[5]
            )
        })
        .collect()
}

fn nested_q5(d: &Tpch) -> Vec<String> {
    let (lo, hi) = q5_dates();
    let region = REGIONS.iter().position(|&r| r == "ASIA").unwrap() as u8;
    let mut revs = [0i64; 25];
    let mut hit = [false; 25];
    for i in 0..d.li.orderkey.len() {
        // find the order by scanning (nested loop).
        for oi in 0..d.ord.orderkey.len() {
            if d.ord.orderkey[oi] != d.li.orderkey[i] {
                continue;
            }
            let od = d.ord.orderdate[oi];
            if od >= lo && od < hi {
                // find the customer by scanning.
                for ci in 0..d.cust.n as usize {
                    if ci as u32 + 1 != d.ord.custkey[oi] {
                        continue;
                    }
                    let cn = d.cust.nationkey[ci];
                    let sn = d.supp.nationkey[d.li.suppkey[i] as usize - 1];
                    if cn == sn && NATIONS[cn as usize].1 == region {
                        revs[cn as usize] +=
                            d.li.extprice_c[i] * (100 - d.li.discount[i] as i64);
                        hit[cn as usize] = true;
                    }
                    break;
                }
            }
            break;
        }
    }
    let mut rows: Vec<(i64, &str)> =
        (0..25).filter(|&n| hit[n]).map(|n| (-revs[n], NATIONS[n].0)).collect();
    rows.sort();
    rows.iter().map(|&(nr, name)| format!("{name}\t{}", fmt_money4(-nr))).collect()
}

fn nested_q10(d: &Tpch) -> Vec<String> {
    let (lo, hi) = q10_dates();
    let mut rev = vec![0i64; d.cust.n as usize + 1];
    for i in 0..d.li.orderkey.len() {
        if d.li.returnflag[i] != b'R' {
            continue;
        }
        for oi in 0..d.ord.orderkey.len() {
            if d.ord.orderkey[oi] == d.li.orderkey[i] {
                let od = d.ord.orderdate[oi];
                if od >= lo && od < hi {
                    rev[d.ord.custkey[oi] as usize] +=
                        d.li.extprice_c[i] * (100 - d.li.discount[i] as i64);
                }
                break;
            }
        }
    }
    let mut rows: Vec<(i64, u32)> = (1..=d.cust.n as usize)
        .filter(|&c| rev[c] > 0)
        .map(|c| (-rev[c], c as u32))
        .collect();
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

fn nested_q12(d: &Tpch) -> Vec<String> {
    let (lo, hi) = q12_dates();
    let mut mail = (0i64, 0i64);
    let mut ship = (0i64, 0i64);
    for i in 0..d.li.orderkey.len() {
        let m = MODES[d.li.mode[i] as usize];
        let cell = if m == "MAIL" {
            &mut mail
        } else if m == "SHIP" {
            &mut ship
        } else {
            continue;
        };
        let rc = d.li.receiptdate[i];
        if rc < lo || rc >= hi {
            continue;
        }
        if !(d.li.commitdate[i] < rc && d.li.shipdate[i] < d.li.commitdate[i]) {
            continue;
        }
        // find the order by scanning (nested loop).
        let mut high = false;
        for oi in 0..d.ord.orderkey.len() {
            if d.ord.orderkey[oi] == d.li.orderkey[i] {
                high = d.ord.priority[oi] <= 1;
                break;
            }
        }
        if high {
            cell.0 += 1;
        } else {
            cell.1 += 1;
        }
    }
    let mut out = Vec::new();
    for (name, c) in [("MAIL", mail), ("SHIP", ship)] {
        if c.0 + c.1 > 0 {
            out.push(format!("{name}\t{}\t{}", c.0, c.1));
        }
    }
    out
}

fn nested_q14(d: &Tpch) -> Vec<String> {
    let (lo, hi) = q14_dates();
    let (mut promo, mut total) = (0i64, 0i64);
    for i in 0..d.li.orderkey.len() {
        let s = d.li.shipdate[i];
        if s < lo || s >= hi {
            continue;
        }
        let rev = d.li.extprice_c[i] * (100 - d.li.discount[i] as i64);
        total += rev;
        // find the part by scanning (nested loop).
        for pi in 0..d.part.n as usize {
            if pi as u32 + 1 == d.li.partkey[i] {
                if TYPES[d.part.ptype[pi] as usize].starts_with("PROMO") {
                    promo += rev;
                }
                break;
            }
        }
    }
    if total == 0 {
        return Vec::new();
    }
    vec![fmt_pct6(promo, total)]
}

// ---------------------------------------------------------------------------

#[test]
fn floors_vs_nested_loop_tiny() {
    let d = gen::generate(0.001);
    let root = tmpdir("nested");
    ingest::ingest_all(&root, &d, 8192);
    let b = floors::TpchBanks::open(&root);
    assert_eq!(floors::q1(&b, 3), nested_q1(&d), "q1 floor vs nested-loop oracle");
    assert_eq!(floors::q3(&b, 3), nested_q3(&d), "q3 floor vs nested-loop oracle");
    assert_eq!(floors::q5(&b, 3), nested_q5(&d), "q5 floor vs nested-loop oracle");
    assert_eq!(floors::q9(&b, 3), nested_q9(&d), "q9 floor vs nested-loop oracle");
    assert_eq!(floors::q10(&b, 3), nested_q10(&d), "q10 floor vs nested-loop oracle");
    assert_eq!(floors::q12(&b, 3), nested_q12(&d), "q12 floor vs nested-loop oracle");
    assert_eq!(floors::q14(&b, 3), nested_q14(&d), "q14 floor vs nested-loop oracle");
    assert_eq!(floors::q18(&b, 3), nested_q18(&d), "q18 floor vs nested-loop oracle");
    // The BTree reference must agree with the nested loops too (oracle
    // cross-check, same data).
    assert_eq!(oracle::q1(&d), nested_q1(&d));
    assert_eq!(oracle::q3(&d), nested_q3(&d));
    assert_eq!(oracle::q5(&d), nested_q5(&d));
    assert_eq!(oracle::q9(&d), nested_q9(&d));
    assert_eq!(oracle::q10(&d), nested_q10(&d));
    assert_eq!(oracle::q12(&d), nested_q12(&d));
    assert_eq!(oracle::q14(&d), nested_q14(&d));
    assert_eq!(oracle::q18(&d), nested_q18(&d));
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn floors_vs_reference_multipart() {
    // sf 0.02: lineitem ~120k rows over ~15 parts (part boundaries + the
    // dynamic-claim merge paths exercised).
    let d = gen::generate(0.02);
    let root = tmpdir("multipart");
    ingest::ingest_all(&root, &d, 8192);
    let b = floors::TpchBanks::open(&root);
    assert!(b.lineitem.parts.len() > 4, "want a multi-part lineitem bank");
    assert_eq!(floors::q1(&b, 5), oracle::q1(&d), "q1 floor vs reference");
    assert_eq!(floors::q3(&b, 5), oracle::q3(&d), "q3 floor vs reference");
    assert_eq!(floors::q5(&b, 5), oracle::q5(&d), "q5 floor vs reference");
    assert_eq!(floors::q9(&b, 5), oracle::q9(&d), "q9 floor vs reference");
    assert_eq!(floors::q10(&b, 5), oracle::q10(&d), "q10 floor vs reference");
    assert_eq!(floors::q12(&b, 5), oracle::q12(&d), "q12 floor vs reference");
    assert_eq!(floors::q14(&b, 5), oracle::q14(&d), "q14 floor vs reference");
    assert_eq!(floors::q18(&b, 5), oracle::q18(&d), "q18 floor vs reference");
    // Answers are non-degenerate (q18's HAVING > 300 can legitimately be
    // empty at toy SF — its identity is still asserted above; q5 keeps
    // whatever nations survive the c_nat = s_nat coincidence).
    assert_eq!(floors::q3(&b, 5).len(), 10);
    assert!(!floors::q9(&b, 5).is_empty());
    assert_eq!(floors::q1(&b, 5).len(), 4, "all four rf/ls groups at sf0.02");
    assert!(!floors::q5(&b, 5).is_empty());
    assert_eq!(floors::q10(&b, 5).len(), 20);
    assert_eq!(floors::q12(&b, 5).len(), 2, "MAIL and SHIP groups");
    assert_eq!(floors::q14(&b, 5).len(), 1);
    std::fs::remove_dir_all(&root).ok();
}
