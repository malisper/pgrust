//! Width-ladder cell (election-inputs-law.md §4.2): MEASURE the pool's
//! parallel-engagement cost so cost_params' claim_setup_(base,per_worker)
//! stop carrying the width-96-solved historical values.
//!
//! Two #[ignore] harnesses (profiling pushes only; this lane changes NO
//! defaults — the evidence rides docs/design/sqe/width-ladder-cell.md):
//!
//!   pool_engage_ladder — the E2 quantity in isolation: time Pool::run
//!     with w trivial claims (work ~ 0), so t == the claim-plane setup:
//!     publish + wake-all + w claims + done-report. Cold split: the
//!     FIRST engagement of a freshly spawned pool (boot fan-out) vs the
//!     steady parked-warm state (median of many).
//!
//!   fold_width_ladder — the same quantity through the REAL fused fold
//!     (FusedFilterAgg / filtered_fold, the rung-B ungrouped scalar fold)
//!     over sealed pgrc2 banks spanning N well below and above today's
//!     21M-row width-96 cutoff. filtered_fold engages the full pool
//!     unconditionally (no elect_threads gate on this arm), so
//!     SqeConfig.threads IS the forced width. Per width, fit
//!     t(w, N) = setup(w) + N/(rate·w); across widths, fit
//!     setup(w) = base + per_worker·w — the two cost_params values.
//!
//! Run:
//!   SQE_WLADDER_DIR=<dir> cargo test --release --features rig \
//!     --test width_ladder -- --ignored --nocapture
//! Knobs: SQE_WLADDER_WIDTHS=1,2,4,...  SQE_WLADDER_ROWS=100000,...
//!        SQE_WLADDER_REPS=<n>  SQE_WLADDER_COLD=<n fresh engines/pools>

#![cfg(feature = "rig")]

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::engine::{Engine, PopulatePolicy, SqeConfig};
use sqe::planner::{plan_from_ap, AAgg, AFamily, APlan, APred, AValExpr};
use sqe::pool::Pool;
use sqe::typmeta::TypMeta;

// ---------------------------------------------------------------------------
// knobs
// ---------------------------------------------------------------------------

fn env_list(name: &str, default: &[u64]) -> Vec<u64> {
    std::env::var(name)
        .ok()
        .map(|s| s.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .filter(|v: &Vec<u64>| !v.is_empty())
        .unwrap_or_else(|| default.to_vec())
}

fn env_n(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

/// Width ladder: powers of two up to available parallelism, plus the
/// machine width itself when it is not a power of two.
fn widths() -> Vec<usize> {
    if let Ok(s) = std::env::var("SQE_WLADDER_WIDTHS") {
        let v: Vec<usize> = s.split(',').filter_map(|x| x.trim().parse().ok()).collect();
        if !v.is_empty() {
            return v;
        }
    }
    let avail = sqe::rig::thread_count();
    let mut v = Vec::new();
    let mut w = 1usize;
    while w <= avail {
        v.push(w);
        w *= 2;
    }
    if *v.last().unwrap() != avail {
        v.push(avail);
    }
    v
}

fn median_ns(mut xs: Vec<u64>) -> u64 {
    xs.sort_unstable();
    xs[xs.len() / 2]
}

fn pct_ns(xs: &[u64], p: usize) -> u64 {
    let mut v = xs.to_vec();
    v.sort_unstable();
    v[(v.len() - 1) * p / 100]
}

// ---------------------------------------------------------------------------
// 1. pool_engage_ladder — the pure claim-plane setup cost
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn pool_engage_ladder() {
    let warm_reps = env_n("SQE_WLADDER_REPS", 3000);
    let cold_pools = env_n("SQE_WLADDER_COLD", 30);
    println!("POOLHDR|host_threads={}|warm_reps={warm_reps}|cold_pools={cold_pools}", sqe::rig::thread_count());
    for &w in &widths() {
        // ---- cold: fresh pool, time its first (and second) engagement.
        let mut first = Vec::with_capacity(cold_pools);
        let mut second = Vec::with_capacity(cold_pools);
        for _ in 0..cold_pools {
            let p = Pool::new(w);
            let t0 = std::time::Instant::now();
            let _ = p.run(w, |_| (), |_: &mut (), _| {});
            first.push(t0.elapsed().as_nanos() as u64);
            let t1 = std::time::Instant::now();
            let _ = p.run(w, |_| (), |_: &mut (), _| {});
            second.push(t1.elapsed().as_nanos() as u64);
        }
        // ---- warm: one pool, steady state.
        let p = Pool::new(w);
        for _ in 0..64 {
            let _ = p.run(w, |_| (), |_: &mut (), _| {});
        }
        let mut warm = Vec::with_capacity(warm_reps);
        for _ in 0..warm_reps {
            let t0 = std::time::Instant::now();
            let _ = p.run(w, |_| (), |_: &mut (), _| {});
            warm.push(t0.elapsed().as_nanos() as u64);
        }
        println!(
            "POOL|w={w}|cold1_p50_ns={}|cold1_p90_ns={}|cold2_p50_ns={}|warm_p50_ns={}|warm_p10_ns={}|warm_p90_ns={}",
            median_ns(first.clone()),
            pct_ns(&first, 90),
            median_ns(second),
            median_ns(warm.clone()),
            pct_ns(&warm, 10),
            pct_ns(&warm, 90),
        );
    }
}

// ---------------------------------------------------------------------------
// 2. fold_width_ladder — the same cost through the real fused fold
// ---------------------------------------------------------------------------

fn wcol(attno: u32) -> ColSchema {
    ColSchema {
        attno,
        class: StorageClass::ByvalWord { width: 8, signed: true },
        typlen: 8,
        typbyval: true,
        typalign: b'd',
        collation_class: CollationClass::C,
        semantics: TypeSemantics::SignedInt,
    }
}

/// Two int8 lanes, never zero: col1 the predicate lane, col2 the agg lane.
fn row(i: u64) -> (u64, u64) {
    let a = i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 33; // 31-bit, nonneg
    let b = i.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 33;
    (a | 1, b | 1)
}

/// Seal via the real writer + publish path (part cap 16384 rows so the
/// part-grain claim plane has depth even at the small-N rungs).
fn seal_fold_bank(dir: &str, rows: u64) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let schema = vec![wcol(1), wcol(2)];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        5,
        779,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 16384, max_bytes: u64::MAX, cut_granule_rows: 1024 },
    )
    .expect("open writer");
    for i in 0..rows {
        let (a, b) = row(i);
        let datums = [RawDatum::Word(a), RawDatum::Word(b)];
        let sources: [&dyn pgrc2_write::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.append_row(&datums, &mut kit.ext, &mut env).expect("append");
    }
    let probe = Probe::new(TxnVerdict::Committed);
    {
        let sources: [&dyn pgrc2_write::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.finish(&mut env).expect("finish");
    }
    w.publish(&mut vfs, &probe).expect("publish");
}

fn fold_engine(dir: &str, threads: usize) -> Engine {
    let schema = vec![ColMeta::new(1, "p", TypMeta::INT8), ColMeta::new(2, "x", TypMeta::INT8)];
    let bank = Bank::open(dir, schema, &OpenOpts { bankstats: false, threads: 1 });
    Engine::new(
        bank,
        SqeConfig { threads, populate: PopulatePolicy::Never, ..SqeConfig::default() },
    )
}

/// SELECT count(*), sum(x) WHERE p <> 0 — every row passes (both lanes
/// sealed odd), so the fold runs at full decode+eval rate: the E1 rate
/// and the E2 setup are the only terms in the timing.
fn fold_node(engine: &Engine, q: u32) -> sqe::ir::PlanNode {
    let ap = APlan {
        q,
        family: AFamily::FusedFilterAgg,
        sortagg_keys: Vec::new(),
        win: None,
        tags: Vec::new(),
        cols: vec![1, 2],
        pred: Some(APred::NeZero { col: 1, fp: None }),
        group: Vec::new(),
        agg: vec![AAgg::CountStar, AAgg::Sum { e: AValExpr::Col(2) }],
        order: None,
        agg_filters: Vec::new(),
        having: None,
        params: Vec::new(),
        fingerprints: Vec::new(),
        kernel_oracle: String::new(),
        notes: "width_ladder".into(),
        flags: Vec::new(),
    };
    plan_from_ap(&engine.bank, &engine.faces, &ap).expect("plan")
}

#[test]
#[ignore]
fn fold_width_ladder() {
    let base = std::env::var("SQE_WLADDER_DIR").unwrap_or_else(|_| {
        std::env::temp_dir().join("sqe_width_ladder").to_str().unwrap().into()
    });
    let rows_ladder = env_list(
        "SQE_WLADDER_ROWS",
        &[100_000, 300_000, 1_000_000, 3_000_000, 10_000_000, 30_000_000],
    );
    let reps = env_n("SQE_WLADDER_REPS", 15);
    let cold_engines = env_n("SQE_WLADDER_COLD", 5);
    println!(
        "FOLDHDR|host_threads={}|reps={reps}|cold_engines={cold_engines}",
        sqe::rig::thread_count()
    );
    for &rows in &rows_ladder {
        let dir = format!("{base}/n{rows}");
        let marker = format!("{dir}/.sealed");
        if !std::path::Path::new(&marker).exists() {
            let _ = std::fs::remove_dir_all(&dir);
            let t0 = std::time::Instant::now();
            seal_fold_bank(&dir, rows);
            std::fs::write(&marker, b"ok").unwrap();
            println!("SEAL|rows={rows}|ms={:.0}", t0.elapsed().as_secs_f64() * 1e3);
        }
        for &w in &widths() {
            // ---- cold-first-engagement: fresh engine (fresh pool), faces
            // prewarmed OUTSIDE the pool so the timed run is the fold +
            // the pool's boot fan-out, not face builds.
            let mut cold = Vec::with_capacity(cold_engines);
            for q in 0..cold_engines as u32 {
                let engine = fold_engine(&dir, w);
                let node = fold_node(&engine, 100 + q);
                let _ = engine.faces.walk(&engine.bank, 1);
                let _ = engine.faces.sma(&engine.bank, 1);
                let _ = engine.bank.face(1);
                let _ = engine.bank.face(2);
                let t0 = std::time::Instant::now();
                let a = engine.run(&node);
                cold.push(t0.elapsed().as_nanos() as u64);
                assert_eq!(a.nrows, 1, "one fold row");
            }
            // ---- warm: one engine, parked pool, steady state.
            let engine = fold_engine(&dir, w);
            let node = fold_node(&engine, 200);
            for _ in 0..3 {
                let _ = engine.run(&node);
            }
            let mut warm = Vec::with_capacity(reps);
            for _ in 0..reps {
                let t0 = std::time::Instant::now();
                let _ = engine.run(&node);
                warm.push(t0.elapsed().as_nanos() as u64);
            }
            println!(
                "FOLD|rows={rows}|w={w}|parts={}|warm_p50_ns={}|warm_p10_ns={}|warm_p90_ns={}|cold_p50_ns={}",
                engine.bank.parts.len(),
                median_ns(warm.clone()),
                pct_ns(&warm, 10),
                pct_ns(&warm, 90),
                median_ns(cold),
            );
        }
    }
}
