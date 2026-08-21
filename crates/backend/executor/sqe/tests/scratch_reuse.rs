//! Scratch-depot gates (the scratch-init lane): per-run per-worker decode
//! arenas are POOL-RESIDENT now — `pool.run_finish` parks them on the
//! worker thread at engagement end and the init closures fetch+reset
//! instead of faulting fresh 4 MiB arenas (the warm E2 finding: init, not
//! the wake, dominated the warm per-worker bill).
//!
//! Laws gated here:
//!   1. warm-engagement zero-alloc — after warmup, an engagement of the
//!      fused fold performs NO fresh Scratch allocation (the depot serves
//!      every worker), and answers stay identical rep over rep.
//!   2. no cross-statement leakage — a canceled statement's scratch never
//!      surfaces in the next statement's answer: cancel mid-run, then
//!      re-run and demand the identical answer a fresh engine gives.
//!      (The depot resets at BOTH park and fetch; unit twins live in
//!      scan.rs::tests.)
//!   3. width currency — the depot lives in worker-thread TLS, so a pool
//!      rebuild on a width change retires it with the threads; answers
//!      across widths stay identical (width-independence law).

#![cfg(feature = "rig")]

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::engine::{Engine, PopulatePolicy, SqeConfig};
use sqe::planner::{
    plan_from_ap, AAgg, AFamily, AKeyExpr, AOrderBy, AOrderSpec, APlan, APred, AValExpr,
};
use sqe::typmeta::TypMeta;

/// The fresh-scratch counter is PROCESS-GLOBAL: any sibling test
/// allocating during the counted window is a false depot-miss. Every
/// test in this binary serializes on this lock (the counter-window
/// hygiene the harness's default parallel runner needs).

fn digest(a: &sqe::answer::AnswerSet) -> String {
    sqe::render::to_lines(a).join("\n")
}

/// Counter-mutex discipline: `scratch_fresh_count` is process-global, so
/// every test that reads a count window OR engages an engine (fresh
/// engagements bump the counter from other test threads) serializes on
/// this lock — a zero-fresh window must be exclusively owned.
static FRESH_WINDOW: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn fresh_window() -> std::sync::MutexGuard<'static, ()> {
    FRESH_WINDOW.lock().unwrap_or_else(|p| p.into_inner())
}

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

fn row(i: u64) -> (u64, u64) {
    let a = i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 33;
    let b = i.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 33;
    (a | 1, b | 1)
}

fn seal_bank(dir: &str, rows: u64) {
    if std::path::Path::new(&format!("{dir}/.sealed")).exists() {
        return;
    }
    let _ = std::fs::remove_dir_all(dir);
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
    std::fs::write(format!("{dir}/.sealed"), b"ok").unwrap();
}

fn engine(dir: &str, threads: usize) -> Engine {
    let schema = vec![ColMeta::new(1, "p", TypMeta::INT8), ColMeta::new(2, "x", TypMeta::INT8)];
    let bank = Bank::open(dir, schema, &OpenOpts { bankstats: false, threads: 1 });
    Engine::new(
        bank,
        SqeConfig { threads, populate: PopulatePolicy::Never, ..SqeConfig::default() },
    )
}

fn fold_node(e: &Engine, q: u32) -> sqe::ir::PlanNode {
    let ap = APlan {
        win: None,
        q,
        family: AFamily::FusedFilterAgg,
        tags: Vec::new(),
        cols: vec![1, 2],
        pred: Some(APred::NeZero { col: 1, fp: None }),
        group: Vec::new(),
        agg: vec![AAgg::CountStar, AAgg::Sum { e: AValExpr::Col(2) }],
        order: None,
        agg_filters: Vec::new(),
        having: None,
        sortagg_keys: Vec::new(),
        params: Vec::new(),
        fingerprints: Vec::new(),
        kernel_oracle: String::new(),
        notes: "scratch_reuse".into(),
        flags: Vec::new(),
    };
    plan_from_ap(&e.bank, &e.faces, &ap).expect("plan")
}

/// Serialize this binary's engine-running tests. The zero-fresh gate
/// reads a PROCESS-WIDE counter (`scratch_fresh_count`), and every
/// sibling test's engine allocates legitimately-fresh Scratch on its own
/// cold worker threads — under the parallel harness those allocations
/// land inside the gate's measurement window and read as depot misses
/// (born-flaky at the depot landing, near-deterministic once the
/// cross-bank leg added two more engines). One lock, taken by every test
/// here; poison-immune so a failed sibling can't mask the gate.
fn counter_gate() -> std::sync::MutexGuard<'static, ()> {
    // One suite-wide lock ([sqe-join-depot] merge): the counter window
    // and the fresh-engagement window are the SAME contamination class.
    FRESH_WINDOW.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn bank_dir(rows: u64) -> String {
    let d = std::env::temp_dir()
        .join(format!("sqe_scratch_reuse_n{rows}"))
        .to_str()
        .unwrap()
        .to_string();
    seal_bank(&d, rows);
    d
}

/// Wide-column warm cell (#[ignore], A/B via SQE_SCRATCH_DEPOT=0/1): the
/// fold decodes 12 columns, so a warm engagement's init bill is 12
/// arenas per worker per run without the depot.
#[test]
#[ignore]
fn wide_fold_warm_cell() {
    let _gate = counter_gate();
    const NC: u32 = 12;
    let dir = std::env::temp_dir().join("sqe_scratch_wide12").to_str().unwrap().to_string();
    if !std::path::Path::new(&format!("{dir}/.sealed")).exists() {
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("mkdir");
        let mut vfs = RealVfs;
        let mut kit = Kit::new();
        let schema: Vec<ColSchema> = (1..=NC).map(wcol).collect();
        let mut w = TableWriter::open(
            dir.clone(),
            schema,
            1663,
            5,
            779,
            TxnStamp { fxid: 100, cid: 1 },
            &Default::default(),
            PartCutPolicy { max_rows: 16384, max_bytes: u64::MAX, cut_granule_rows: 1024 },
        )
        .expect("open writer");
        for i in 0..1_000_000u64 {
            let datums: Vec<RawDatum> = (0..NC as u64)
                .map(|c| RawDatum::Word(row(i.wrapping_add(c * 0x9E37)).0))
                .collect();
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
        std::fs::write(format!("{dir}/.sealed"), b"ok").unwrap();
    }
    let schema: Vec<ColMeta> =
        (1..=NC).map(|a| ColMeta::new(a, "c", TypMeta::INT8)).collect();
    let bank = Bank::open(&dir, schema, &OpenOpts { bankstats: false, threads: 1 });
    let threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8);
    let e = Engine::new(
        bank,
        SqeConfig { threads, populate: PopulatePolicy::Never, ..SqeConfig::default() },
    );
    let ap = APlan {
        win: None,
        q: 9,
        family: AFamily::FusedFilterAgg,
        tags: Vec::new(),
        cols: (1..=NC).collect(),
        pred: Some(APred::NeZero { col: 1, fp: None }),
        group: Vec::new(),
        agg: std::iter::once(AAgg::CountStar)
            .chain((2..=NC).map(|c| AAgg::Sum { e: AValExpr::Col(c) }))
            .collect(),
        order: None,
        agg_filters: Vec::new(),
        having: None,
        sortagg_keys: Vec::new(),
        params: Vec::new(),
        fingerprints: Vec::new(),
        kernel_oracle: String::new(),
        notes: "wide12".into(),
        flags: Vec::new(),
    };
    let node = plan_from_ap(&e.bank, &e.faces, &ap).expect("plan");
    for _ in 0..5 {
        let _ = e.run(&node);
    }
    let mut xs: Vec<u64> = (0..41)
        .map(|_| {
            let t0 = std::time::Instant::now();
            let _ = e.run(&node);
            t0.elapsed().as_nanos() as u64
        })
        .collect();
    xs.sort_unstable();
    println!(
        "WIDE12|threads={threads}|depot={}|warm_p50_ns={}|warm_p10_ns={}",
        std::env::var("SQE_SCRATCH_DEPOT").unwrap_or_else(|_| "1".into()),
        xs[xs.len() / 2],
        xs[(xs.len() - 1) / 10],
    );
}

/// Law 1: warm engagements allocate ZERO fresh Scratch — the depot serves
/// every worker — and the answer is bit-stable rep over rep.
#[test]
fn warm_engagement_zero_fresh_scratch() {
    let _gate = counter_gate();
    let dir = bank_dir(400_000);
    let e = engine(&dir, 8);
    let node = fold_node(&e, 1);
    let first = digest(&e.run(&node));
    let warm2 = digest(&e.run(&node));
    assert_eq!(first, warm2, "warm rep answer drift");
    let before = sqe::scan::scratch_fresh_count();
    for _ in 0..5 {
        let a = digest(&e.run(&node));
        assert_eq!(a, first, "warm rep answer drift");
    }
    let after = sqe::scan::scratch_fresh_count();
    assert_eq!(
        after - before,
        0,
        "warm engagements allocated fresh Scratch (depot miss)"
    );
}

/// Law 2: a canceled statement's scratch must not surface in the next —
/// cancel a run mid-flight, then demand the next run's answer match a
/// fresh engine exactly.
#[test]
fn canceled_statement_scratch_never_surfaces() {
    // NOTE: counter_gate() below IS the fresh window (the [sqe-join-depot]
    // merge unified the locks); taking fresh_window() here too was a
    // guaranteed self-deadlock (non-reentrant Mutex, same static).
    fn fire_fast() -> Option<sqe::cancel::Payload> {
        // The first poll anywhere in the run fires: the claim loops die
        // mid-statement with partial decode state in every engaged
        // worker's scratch (the done-wait polls on its 10ms quantum, so
        // the ~19ms w=2 fold below cancels mid-run).
        Some(Box::new("canceled-by-gate"))
    }
    // `_w` above already holds THE suite window (counter_gate is the
    // same mutex since the [sqe-join-depot] merge — a second lock here
    // self-deadlocks: the drain-tip battery hang).
    let dir = bank_dir(10_000_000);
    let e = engine(&dir, 2);
    let node = fold_node(&e, 2);
    let want = digest(&e.run(&node)); // warm the pool + depot honestly
    let canceled = {
        let _g = sqe::cancel::arm(fire_fast);
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = e.run(&node);
        }))
    };
    assert!(canceled.is_err(), "gate poll did not cancel the run");
    // Next statement on the same pool: identical to the pre-cancel truth
    // and to a fresh engine over the same bank.
    let after = digest(&e.run(&node));
    assert_eq!(after, want, "post-cancel answer drift on the warm pool");
    let fresh = engine(&dir, 2);
    let fnode = fold_node(&fresh, 2);
    assert_eq!(digest(&fresh.run(&fnode)), want, "fresh-engine oracle drift");
}

/// [persist-rehome] Law 4: the rehomed state parks are QUERY-AGNOSTIC and
/// BANK-AGNOSTIC by construction — a parked pass-1 state carries only
/// capacity (contents cleared at both park and fetch; cursors never
/// parked). Gate: interleave two different banks at two different pool
/// widths and three distinct query ids over the same process-global
/// parks; every answer must equal its own fresh-engine oracle. Under the
/// old per-query persistence maps this pattern is exactly the stale-reuse
/// identity leg (same q against different banks; cursor keyed by part
/// index resurfacing the other bank's stream).
#[test]
fn parked_state_cross_bank_cross_query_identity() {
    let _gate = counter_gate();
    fn group_node(e: &Engine, q: u32) -> sqe::ir::PlanNode {
        let ap = APlan {
            win: None,
            q,
            family: AFamily::HashPlaneOwnedGroup,
            tags: Vec::new(),
            cols: vec![2],
            pred: None,
            group: vec![AKeyExpr::Col(2)],
            agg: vec![AAgg::CountStar],
            order: Some(AOrderSpec { by: AOrderBy::CountDesc, limit: Some(7), offset: 0 }),
            agg_filters: Vec::new(),
            having: None,
            sortagg_keys: Vec::new(),
            params: Vec::new(),
            fingerprints: Vec::new(),
            kernel_oracle: String::new(),
            notes: "persist_rehome_cross".into(),
            flags: Vec::new(),
        };
        plan_from_ap(&e.bank, &e.faces, &ap).expect("plan")
    }
    // Two banks with different data and different widths (different
    // partition geometry p — the size-arm leg of the park law).
    let d1 = bank_dir(300_000);
    let d2 = bank_dir(500_000);
    let o1 = {
        let e = engine(&d1, 3);
        digest(&e.run(&group_node(&e, 21)))
    };
    let o2 = {
        let e = engine(&d2, 8);
        digest(&e.run(&group_node(&e, 21)))
    };
    let e1 = engine(&d1, 3);
    let e2 = engine(&d2, 8);
    for q in [21u32, 22, 23] {
        assert_eq!(
            digest(&e1.run(&group_node(&e1, q))),
            o1,
            "bank1 q{q}: parked state leaked across bank/query"
        );
        assert_eq!(
            digest(&e2.run(&group_node(&e2, q))),
            o2,
            "bank2 q{q}: parked state leaked across bank/query"
        );
    }
}

/// Law 3: width changes rebuild the pool and the depots go with the
/// worker threads — answers stay width-independent before and after.
#[test]
fn width_rebuild_keeps_answers() {
    let _gate = counter_gate();
    // Own bank (row count is the bank key): seals race under the
    // parallel test harness.
    let dir = bank_dir(400_128);
    let mut want: Option<String> = None;
    for &w in &[1usize, 4, 8, 3] {
        let e = engine(&dir, w);
        let node = fold_node(&e, 3);
        for _ in 0..3 {
            let d = digest(&e.run(&node));
            match &want {
                Some(x) => assert_eq!(&d, x, "width {w} answer drift"),
                None => want = Some(d),
            }
        }
        // Engine drop joins the pool: worker TLS depots retire here.
    }
}

// ---------------------------------------------------------------------------
// [join-depot] the zero-fresh-warm law through a JOIN engagement: the
// dim-build, build-pass and probe SideDecodes plus every converted
// stencil ride the worker depots — a warm 3-way join-agg statement
// allocates ZERO fresh Scratch.
// ---------------------------------------------------------------------------

fn join3_fixture() -> (Engine, Engine, Engine) {
    // Three banks over the shared row generator: build/probe/dim key
    // col1 agree on the common index prefix, so every stage sees real
    // matches (dim expansion, build scatter, probe chains all engage).
    let bd = bank_dir(150_000);
    let pd = bank_dir(250_000);
    let dd = bank_dir(60_000);
    (engine(&bd, 4), engine(&pd, 4), engine(&dd, 4))
}

fn join3_agg_node(be: &Engine, pe: &Engine, de: &Engine) -> sqe::joins::JoinAggNode {
    use sqe::joins::{DimKey, DimSrc, DimStage, JoinAggOp, JoinAggReq, JoinKey, JoinOut, JoinSide, JoinType};
    sqe::joins::join_agg_node(
        &be.bank,
        &pe.bank,
        &[&de.bank],
        41,
        JoinType::Inner,
        vec![JoinKey { build_col: 1, probe_col: 1 }],
        Vec::new(),
        None,
        None,
        vec![DimStage {
            keys: vec![DimKey { dim_col: 1, build_col: 1, src: DimSrc::Build }],
            pred: None,
            text_eqs: Vec::new(),
            rows_hint: 0,
        }],
        vec![
            JoinAggReq::col(JoinAggOp::CountStar, None),
            JoinAggReq::col(
                JoinAggOp::Sum,
                Some(JoinOut { side: JoinSide::Probe, col: 2 }),
            ),
            JoinAggReq::col(
                JoinAggOp::Sum,
                Some(JoinOut { side: JoinSide::Dim(0), col: 2 }),
            ),
        ],
        Vec::new(),
        be.bank.rows_total(),
    )
    .expect("construct 3-way join-agg")
}

fn run_join3(be: &Engine, pe: &Engine, de: &Engine, n: &sqe::joins::JoinAggNode) -> String {
    digest(
        &sqe::joins::run_hash_join_agg(&be.ctx(), &pe.ctx(), &[&de.ctx()], n)
            .expect("join-agg serves"),
    )
}

/// Law 1 through a join: after warmup, a 3-way join-agg engagement (dim
/// build stage + build pass + probe pass) allocates ZERO fresh Scratch
/// and its answer is bit-stable rep over rep.
#[test]
fn warm_join3_engagement_zero_fresh_scratch() {
    let _w = fresh_window();
    let (be, pe, de) = join3_fixture();
    let node = join3_agg_node(&be, &pe, &de);
    let first = run_join3(&be, &pe, &de, &node);
    let warm2 = run_join3(&be, &pe, &de, &node);
    assert_eq!(first, warm2, "warm join rep answer drift");
    let before = sqe::scan::scratch_fresh_count();
    for _ in 0..5 {
        let a = run_join3(&be, &pe, &de, &node);
        assert_eq!(a, first, "warm join rep answer drift");
    }
    let after = sqe::scan::scratch_fresh_count();
    assert_eq!(
        after - before,
        0,
        "warm join engagements allocated fresh Scratch (depot miss)"
    );
}

/// Law 2 through a join: a canceled join statement's scratch never
/// surfaces — cancel mid-run, then the next run must equal the pre-cancel
/// truth AND a fresh-engine oracle over the same banks.
#[test]
fn canceled_join_statement_scratch_never_surfaces() {
    let _w = fresh_window();
    fn fire_fast() -> Option<sqe::cancel::Payload> {
        Some(Box::new("canceled-by-join-gate"))
    }
    let (be, pe, de) = join3_fixture();
    let node = join3_agg_node(&be, &pe, &de);
    let want = run_join3(&be, &pe, &de, &node); // warm pools + depots honestly
    let canceled = {
        let _g = sqe::cancel::arm(fire_fast);
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = run_join3(&be, &pe, &de, &node);
        }))
    };
    assert!(canceled.is_err(), "gate poll did not cancel the join run");
    let after = run_join3(&be, &pe, &de, &node);
    assert_eq!(after, want, "post-cancel join answer drift on the warm pools");
    let (fb, fp, fd) = join3_fixture();
    let fnode = join3_agg_node(&fb, &fp, &fd);
    assert_eq!(run_join3(&fb, &fp, &fd, &fnode), want, "fresh-engine join oracle drift");
}
