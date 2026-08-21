//! P4-2a TPC-H floor rig.
//!
//!   sqe_tpch_floor <root> --gen [--sf 1]            generate + ingest banks
//!   sqe_tpch_floor <root> --run [--reps 5] [--oracle --sf 1] [--threads N]
//!   sqe_tpch_floor <root> --served [--sf 1]         served-path probe (what
//!                                                   the P4-2 phase-1 seam
//!                                                   serves / refuses today)
//!   sqe_tpch_floor <root> --tbl <dir> [--sf 1]      emit dbgen-style .tbl
//!                                                   (the row-engine bar)
//!
//! Timing recipe: bank open untimed; rep 1 = first-touch (page cache as
//! found — LOCAL INDICATIVE, not the CI cluster OFFICIAL recipe), hot = min of
//! later reps.

use sqe::tpchfloor::{floors, gen, ingest, oracle};
use sqe::tpchfloor::{fmt_date, fmt_money2};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("usage: sqe_tpch_floor <root> [--gen|--run|--served|--tbl <dir>] [--sf F] [--reps N] [--oracle] [--threads N]");
        std::process::exit(2);
    }
    let root = args[0].clone();
    let (mut do_gen, mut do_run, mut do_served, mut do_oracle) = (false, false, false, false);
    let mut do_show = false;
    let mut tbl_dir: Option<String> = None;
    let mut sf = 1.0f64;
    let mut reps = 5usize;
    let mut threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--gen" => do_gen = true,
            "--run" => do_run = true,
            "--served" => do_served = true,
            "--oracle" => do_oracle = true,
            "--show" => do_show = true,
            "--tbl" => {
                i += 1;
                tbl_dir = Some(args[i].clone());
            }
            "--sf" => {
                i += 1;
                sf = args[i].parse().unwrap();
            }
            "--reps" => {
                i += 1;
                reps = args[i].parse().unwrap();
            }
            "--threads" => {
                i += 1;
                threads = args[i].parse().unwrap();
            }
            other => {
                eprintln!("sqe_tpch_floor: unknown flag {other}");
                std::process::exit(2);
            }
        }
        i += 1;
    }

    if do_gen {
        let t0 = std::time::Instant::now();
        let d = gen::generate(sf);
        eprintln!(
            "GEN|sf={sf}|orders={}|lineitem={}|ms={:.0}",
            d.ord.orderkey.len(),
            d.li.orderkey.len(),
            t0.elapsed().as_secs_f64() * 1e3
        );
        let part_rows = if d.li.orderkey.len() > 1_000_000 { 262_144 } else { 8_192 };
        ingest::ingest_all(&root, &d, part_rows);
    }

    if let Some(dir) = tbl_dir {
        let d = gen::generate(sf);
        emit_tbl(&dir, &d);
    }

    if do_run {
        let t0 = std::time::Instant::now();
        let b = floors::TpchBanks::open(&root);
        eprintln!(
            "BANKS|lineitem_rows={}|orders_rows={}|open_ms={:.0}|threads={threads}",
            b.lineitem.rows_total(),
            b.orders.rows_total(),
            t0.elapsed().as_secs_f64() * 1e3
        );
        let want: Option<std::collections::HashMap<&str, Vec<String>>> = if do_oracle {
            let t0 = std::time::Instant::now();
            let d = gen::generate(sf);
            let m = std::collections::HashMap::from([
                ("q1", oracle::q1(&d)),
                ("q3", oracle::q3(&d)),
                ("q5", oracle::q5(&d)),
                ("q9", oracle::q9(&d)),
                ("q10", oracle::q10(&d)),
                ("q12", oracle::q12(&d)),
                ("q14", oracle::q14(&d)),
                ("q18", oracle::q18(&d)),
            ]);
            eprintln!("ORACLE|ms={:.0}", t0.elapsed().as_secs_f64() * 1e3);
            Some(m)
        } else {
            None
        };
        let mut ok = true;
        for (name, f) in [
            ("q1", floors::q1 as fn(&floors::TpchBanks, usize) -> Vec<String>),
            ("q3", floors::q3),
            ("q5", floors::q5),
            ("q9", floors::q9),
            ("q10", floors::q10),
            ("q12", floors::q12),
            ("q14", floors::q14),
            ("q18", floors::q18),
        ] {
            let mut got: Vec<String> = Vec::new();
            for rep in 1..=reps {
                let t0 = std::time::Instant::now();
                got = f(&b, threads);
                println!(
                    "FLOOR|{name}|rep={rep}|ms={:.2}|rows={}",
                    t0.elapsed().as_secs_f64() * 1e3,
                    got.len()
                );
            }
            if do_show {
                for l in &got {
                    println!("ROW|{name}|{l}");
                }
            }
            if let Some(m) = &want {
                let exp = &m[name];
                let same = &got == exp;
                println!("VERIFY|{name}|identical={same}|rows={}", exp.len());
                if !same {
                    for (a, b2) in got.iter().zip(exp.iter()) {
                        if a != b2 {
                            eprintln!("  floor : {a}\n  oracle: {b2}");
                            break;
                        }
                    }
                    ok = false;
                }
            }
        }
        if !ok {
            std::process::exit(1);
        }
    }

    if do_served {
        served_probe(&root, sf, threads, reps);
        if do_oracle {
            // Independent expectation for the two served shapes.
            let d = gen::generate(sf);
            let cutoff = sqe::tpchfloor::q3_date();
            let mut odate = std::collections::HashMap::new();
            for i in 0..d.ord.orderkey.len() {
                odate.insert(d.ord.orderkey[i], d.ord.orderdate[i]);
            }
            let (mut sum, mut cnt) = (0i64, 0i64);
            for i in 0..d.li.orderkey.len() {
                if d.li.shipdate[i] > cutoff && odate[&d.li.orderkey[i]] < cutoff {
                    sum += d.li.extprice_c[i];
                    cnt += 1;
                }
            }
            println!("EXPECT|q3-core-agg|sum_cents={sum}|count={cnt}");
            println!("EXPECT|q18-grouped-count|groups={}", d.ord.orderkey.len());
        }
    }
}

/// What the P4-2 phase-1 join seam can serve of the Q3/Q9/Q18 shapes
/// TODAY — measured where admitted, typed refusal recorded where not.
fn served_probe(root: &str, sf: f64, threads: usize, reps: usize) {
    use sqe::bank::Bank;
    use sqe::engine::{Engine, SqeConfig};
    use sqe::ir::{CmpOp, PredSpec, PredTerm};
    use sqe::joins::{join_agg_node, run_hash_join_agg, JoinAggOp, JoinAggReq, JoinKey, JoinOut, JoinSide, JoinType};
    use sqe::render::to_lines;
    use sqe::tpchfloor::{open_table, q3_date};

    let cfg = SqeConfig { threads, ..SqeConfig::default() };
    let eo = Engine::new(open_table(root, "orders"), cfg.clone());
    let el = Engine::new(open_table(root, "lineitem"), cfg.clone());
    let cutoff = q3_date() as i64;
    let dt = |b: &Bank, c: u32| b.typ(c);

    // (a) Q3 two-table core: orders(o_orderdate<X) ⋈ lineitem(l_shipdate>X),
    //     ungrouped sum(l_extendedprice) + count(*).
    let bpred = PredSpec::all(vec![PredTerm::new(
        5,
        CmpOp::Between,
        i64::MIN + 1,
        cutoff - 1,
        dt(&eo.bank, 5),
    )]);
    let ppred = PredSpec::all(vec![PredTerm::new(
        11,
        CmpOp::Between,
        cutoff + 1,
        i64::MAX - 1,
        dt(&el.bank, 11),
    )]);
    match join_agg_node(
        &eo.bank,
        &el.bank, &[],
        900,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        vec![],
        Some(bpred.clone()),
        Some(ppred.clone()), Vec::new(), vec![
            (JoinAggOp::Sum, Some(JoinOut { side: JoinSide::Probe, col: 6 })),
            (JoinAggOp::CountStar, None),
        ].into_iter().map(|(op, input)| JoinAggReq::col(op, input)).collect(), Vec::from_iter(None),
        eo.bank.rows_total()) {
        Ok(anode) => {
            for rep in 1..=reps {
                let t0 = std::time::Instant::now();
                let ans = run_hash_join_agg(&eo.ctx(), &el.ctx(), &[], &anode).expect("served run");
                println!(
                    "SERVED|q3-core-agg|rep={rep}|ms={:.2}|ans={}",
                    t0.elapsed().as_secs_f64() * 1e3,
                    to_lines(&ans).join(",")
                );
            }
        }
        Err(e) => println!("SERVED|q3-core-agg|REFUSED|{e}"),
    }

    // (b) Q3 real goal: grouped sum by l_orderkey — expect the typed
    //     count-only refusal (the phase-2 charter item).
    match join_agg_node(
        &eo.bank,
        &el.bank, &[],
        901,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        vec![],
        Some(bpred.clone()),
        Some(ppred), Vec::new(), vec![(JoinAggOp::Sum, Some(JoinOut { side: JoinSide::Probe, col: 6 }))].into_iter().map(|(op, input)| JoinAggReq::col(op, input)).collect(), Vec::from_iter(Some(JoinOut { side: JoinSide::Probe, col: 1 })),
        eo.bank.rows_total()) {
        Ok(anode) => {
            // Grouped SUM serves since sqe-grpfold; keep the leg honest.
            let t0 = std::time::Instant::now();
            let ans = run_hash_join_agg(&eo.ctx(), &el.ctx(), &[], &anode).expect("served run");
            println!(
                "SERVED|q3-grouped-sum|ms={:.2}|rows={}",
                t0.elapsed().as_secs_f64() * 1e3,
                ans.nrows
            );
        }
        Err(e) => println!("SERVED|q3-grouped-sum|REFUSED|{e}"),
    }

    // (c) Q18 core: grouped count(*) by l_orderkey (the shape the seam
    //     serves); the real Q18 needs grouped SUM(l_quantity) — refusal.
    match join_agg_node(
        &eo.bank,
        &el.bank, &[],
        902,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        vec![],
        None,
        None, Vec::new(), vec![(JoinAggOp::CountStar, None)].into_iter().map(|(op, input)| JoinAggReq::col(op, input)).collect(), Vec::from_iter(Some(JoinOut { side: JoinSide::Probe, col: 1 })),
        eo.bank.rows_total()) {
        Ok(anode) => {
            for rep in 1..=reps {
                let t0 = std::time::Instant::now();
                let ans = run_hash_join_agg(&eo.ctx(), &el.ctx(), &[], &anode).expect("served run");
                println!(
                    "SERVED|q18-grouped-count|rep={rep}|ms={:.2}|rows={}",
                    t0.elapsed().as_secs_f64() * 1e3,
                    ans.nrows
                );
            }
        }
        Err(e) => println!("SERVED|q18-grouped-count|REFUSED|{e}"),
    }
    match join_agg_node(
        &eo.bank,
        &el.bank, &[],
        903,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        vec![],
        None,
        None, Vec::new(), vec![(JoinAggOp::Sum, Some(JoinOut { side: JoinSide::Probe, col: 5 }))].into_iter().map(|(op, input)| JoinAggReq::col(op, input)).collect(), Vec::from_iter(Some(JoinOut { side: JoinSide::Probe, col: 1 })),
        eo.bank.rows_total()) {
        Ok(anode) => {
            let t0 = std::time::Instant::now();
            let ans = run_hash_join_agg(&eo.ctx(), &el.ctx(), &[], &anode).expect("served run");
            println!(
                "SERVED|q18-grouped-sum-qty|ms={:.2}|rows={}",
                t0.elapsed().as_secs_f64() * 1e3,
                ans.nrows
            );
        }
        Err(e) => println!("SERVED|q18-grouped-sum-qty|REFUSED|{e}"),
    }

    // (d) [sqe-q9arith] Q9 two-way core: partsupp ⋈ lineitem on
    //     (partkey, suppkey) — the fused-arith fold shapes the join
    //     vocabulary now reaches. Cost term sum(ps_supplycost*l_quantity)
    //     is the int4 mul (w=4: witnessed product ≤ 100_000·5_000 =
    //     5.0e8 < 2^31−1); the revenue form
    //     sum(l_extendedprice*(100−l_discount)) is the int84 mul (w=8,
    //     inner int4 sub wi=4, witnessed (100−disc) ∈ [90,100]).
    //     Ungrouped answers are verified against the in-memory generator;
    //     the grouped-by-l_suppkey twin reports shape + time. The FULL
    //     Q9 (part name LIKE, supplier→nation, order year) still refuses:
    //     5 relations exceed the 2-way + dim-stage tree.
    {
        use sqe::joins::JoinArith;
        let eps = Engine::new(open_table(root, "partsupp"), cfg.clone());
        let keys2 = vec![
            JoinKey { build_col: 1, probe_col: 2 },
            JoinKey { build_col: 2, probe_col: 3 },
        ];
        let cost_leg = JoinAggReq {
            op: JoinAggOp::Sum,
            input: Some(JoinOut { side: JoinSide::Build, col: 4 }),
            input2: Some(JoinOut { side: JoinSide::Probe, col: 5 }),
            arith: Some(JoinArith::MulCC { w: 4 }),
            case: None,
        };
        let rev_leg = JoinAggReq {
            op: JoinAggOp::Sum,
            input: Some(JoinOut { side: JoinSide::Probe, col: 6 }),
            input2: Some(JoinOut { side: JoinSide::Probe, col: 7 }),
            arith: Some(JoinArith::MulKSub { k: 100, w: 8, wi: 4 }),
            case: None,
        };
        match join_agg_node(
            &eps.bank, &el.bank, &[], 904, JoinType::Inner, keys2.clone(), vec![],
            None, None, Vec::new(),
            vec![cost_leg.clone(), rev_leg.clone(), JoinAggReq::col(JoinAggOp::CountStar, None)],
            Vec::new(), eps.bank.rows_total(),
        ) {
            Ok(anode) => {
                let mut got = String::new();
                for rep in 1..=reps {
                    let t0 = std::time::Instant::now();
                    let ans =
                        run_hash_join_agg(&eps.ctx(), &el.ctx(), &[], &anode).expect("served run");
                    got = to_lines(&ans).join(",");
                    println!(
                        "SERVED|q9-2way-core-agg|rep={rep}|ms={:.2}|ans={got}",
                        t0.elapsed().as_secs_f64() * 1e3,
                    );
                }
                // Independent expectation from the generator (scalar law).
                let d = sqe::tpchfloor::gen::generate(sf);
                let mut cost = std::collections::HashMap::new();
                for i in 0..d.ps.partkey.len() {
                    cost.insert((d.ps.partkey[i], d.ps.suppkey[i]), d.ps.supplycost_c[i] as i64);
                }
                let (mut ec, mut er) = (0i128, 0i128);
                let n = d.li.orderkey.len();
                for i in 0..n {
                    ec += (cost[&(d.li.partkey[i], d.li.suppkey[i])]
                        * d.li.quantity_c[i] as i64) as i128;
                    er += (d.li.extprice_c[i] * (100 - d.li.discount[i] as i64)) as i128;
                }
                let expect = format!("{ec}\t{er}\t{n}");
                println!(
                    "VERIFY|q9-2way-core-agg|identical={}|expect={expect}",
                    got == expect
                );
                if got != expect {
                    std::process::exit(1);
                }
            }
            Err(e) => println!("SERVED|q9-2way-core-agg|REFUSED|{e}"),
        }
        match join_agg_node(
            &eps.bank, &el.bank, &[], 905, JoinType::Inner, keys2, vec![],
            None, None, Vec::new(),
            vec![cost_leg, rev_leg],
            vec![JoinOut { side: JoinSide::Probe, col: 3 }], eps.bank.rows_total(),
        ) {
            Ok(anode) => {
                for rep in 1..=reps {
                    let t0 = std::time::Instant::now();
                    let ans =
                        run_hash_join_agg(&eps.ctx(), &el.ctx(), &[], &anode).expect("served run");
                    println!(
                        "SERVED|q9-2way-grouped-suppkey|rep={rep}|ms={:.2}|rows={}",
                        t0.elapsed().as_secs_f64() * 1e3,
                        ans.nrows
                    );
                }
            }
            Err(e) => println!("SERVED|q9-2way-grouped-suppkey|REFUSED|{e}"),
        }
    }
    println!("SERVED|q9-full|REFUSED|five-relation-tree: part-name filter + supplier/nation + order-year exceed the 2-way + dim-stage vocabulary");
}

/// dbgen-style pipe-delimited .tbl emit (the stock-PG heap-engine bar).
fn emit_tbl(dir: &str, d: &gen::Tpch) {
    use gen::*;
    use std::io::Write;
    std::fs::create_dir_all(dir).expect("mkdir tbl");
    let f = |name: &str| {
        std::io::BufWriter::new(std::fs::File::create(format!("{dir}/{name}.tbl")).unwrap())
    };
    let m2 = fmt_money2;
    {
        let mut w = f("customer");
        for i in 0..d.cust.n as usize {
            let ck = i as u32 + 1;
            let nk = d.cust.nationkey[i];
            writeln!(
                w,
                "{ck}|{}|{}|{nk}|{}|{}|{}|{}|",
                c_name(ck),
                address('c', ck),
                phone(nk, ck),
                m2(d.cust.acctbal[i] as i64),
                SEGMENTS[d.cust.segment[i] as usize],
                comment('c', i as u64)
            )
            .unwrap();
        }
    }
    {
        let mut w = f("orders");
        for i in 0..d.ord.orderkey.len() {
            writeln!(
                w,
                "{}|{}|{}|{}|{}|{}|{}|0|{}|",
                d.ord.orderkey[i],
                d.ord.custkey[i],
                d.ord.status[i] as char,
                m2(d.ord.totalprice[i]),
                fmt_date(d.ord.orderdate[i]),
                PRIORITIES[d.ord.priority[i] as usize],
                clerk_name(d.ord.clerk[i]),
                comment('o', i as u64)
            )
            .unwrap();
        }
    }
    {
        let mut w = f("lineitem");
        for i in 0..d.li.orderkey.len() {
            writeln!(
                w,
                "{}|{}|{}|{}|{}|{}|0.{:02}|0.{:02}|{}|{}|{}|{}|{}|{}|{}|",
                d.li.orderkey[i],
                d.li.partkey[i],
                d.li.suppkey[i],
                d.li.linenumber[i],
                d.li.quantity_c[i] / 100,
                m2(d.li.extprice_c[i]),
                d.li.discount[i],
                d.li.tax[i],
                d.li.returnflag[i] as char,
                d.li.linestatus[i] as char,
                fmt_date(d.li.shipdate[i]),
                fmt_date(d.li.commitdate[i]),
                fmt_date(d.li.receiptdate[i]),
                INSTRUCT[d.li.instruct[i] as usize],
                format!("{}|{}", MODES[d.li.mode[i] as usize], comment('l', i as u64)),
            )
            .unwrap();
        }
    }
    {
        let mut w = f("part");
        for i in 0..d.part.n as usize {
            let pk = i as u32 + 1;
            writeln!(
                w,
                "{pk}|{}|Manufacturer#{}|Brand#{}|{}|{}|{}|{}|{}|",
                p_name(d.part.name_words[i]),
                d.part.mfgr[i],
                d.part.brand[i],
                TYPES[d.part.ptype[i] as usize],
                d.part.size[i],
                CONTAINERS[d.part.container[i] as usize],
                m2(d.part.retail_c[i] as i64),
                comment('p', i as u64)
            )
            .unwrap();
        }
    }
    {
        let mut w = f("partsupp");
        for i in 0..d.ps.partkey.len() {
            writeln!(
                w,
                "{}|{}|{}|{}|{}|",
                d.ps.partkey[i],
                d.ps.suppkey[i],
                d.ps.availqty[i],
                m2(d.ps.supplycost_c[i] as i64),
                comment('s', i as u64)
            )
            .unwrap();
        }
    }
    {
        let mut w = f("supplier");
        for i in 0..d.supp.n as usize {
            let sk = i as u32 + 1;
            let nk = d.supp.nationkey[i];
            writeln!(
                w,
                "{sk}|{}|{}|{nk}|{}|{}|{}|",
                s_name(sk),
                address('s', sk),
                phone(nk, sk),
                m2(d.supp.acctbal[i] as i64),
                comment('u', i as u64)
            )
            .unwrap();
        }
    }
    {
        let mut w = f("nation");
        for (k, (name, region)) in NATIONS.iter().enumerate() {
            writeln!(w, "{k}|{name}|{region}|{}|", comment('n', k as u64)).unwrap();
        }
    }
    {
        let mut w = f("region");
        for (k, name) in REGIONS.iter().enumerate() {
            writeln!(w, "{k}|{name}|{}|", comment('r', k as u64)).unwrap();
        }
    }
    eprintln!("TBL|dir={dir}");
}
