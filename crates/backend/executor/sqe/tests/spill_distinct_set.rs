//! [spill-3] DistinctPipeline spill gates (spill-design.md §6):
//!
//!   - identity spill-forced vs the resident twin on the SET routes
//!     (int_set 8 B key records incl. sum/avg DISTINCT first-seen
//!     folds; text_set fp128 records, dict + raw lanes), plus the
//!     scalar SQL oracle;
//!   - engagement counters: the forced legs must FLUSH, DRAIN and
//!     MERGE (no vacuous green), and rerun byte-identically;
//!   - the budget-aware cross-family re-election: a grouped pure
//!     count-distinct in the dense-distinct spill vocabulary lands on
//!     the hash plane over budget (EXACT-ORDER identity vs the
//!     resident DistinctPipeline twin — the pipeline's own tie order),
//!     engages the pair-plane spill, and flips the former
//!     no-spill-arm refusal to served; under the kill switch and
//!     under the dense-array floor the election is byte-stable;
//!   - the SET routes' admission flip: over-budget ungrouped distinct
//!     admits (spill-served) where v2 refused `no-spill-arm`; grouped
//!     text-key distinct stays the typed refusal.

#![cfg(feature = "rig")]

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::dict::TextSemantics;
use pgrc2_write::elect::{CodecCandidates, ColumnPosture, DictPolicy};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{img_4b_u, Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::engine::{Engine, SqeConfig};
use sqe::ir::Family;
use sqe::planner::check_server_grouped;
use sqe::refuse::Refuse;
use sqe::render::to_lines;
use sqe::rig::lower::{lower, SelExpr, SqlAgg, SqlExpr, SqlQuery};
use sqe::rig::oracle::run_oracle;
use sqe::typmeta::TypMeta;

const ROWS: u64 = 60_000;

fn wcol(attno: u32, width: u8, typlen: i16) -> ColSchema {
    ColSchema {
        attno,
        class: StorageClass::ByvalWord { width, signed: true },
        typlen,
        typbyval: true,
        typalign: if width == 8 { b'd' } else { b'i' },
        collation_class: CollationClass::C,
        semantics: TypeSemantics::SignedInt,
    }
}

fn tcol(attno: u32) -> ColSchema {
    ColSchema {
        attno,
        class: StorageClass::VarlenaVerbatim,
        typlen: -1,
        typbyval: false,
        typalign: b'i',
        collation_class: CollationClass::C,
        semantics: TypeSemantics::TextCollated,
    }
}

/// The row law. Columns:
///   1 v  int8  key in [0, 50)              (dense-safe group key)
///   2 d  int8  ~20k distinct, >= 0         (int_set / pair distinct)
///   3 t  text  DICT, ~30k distinct         (text_set dict lane)
///   4 r  text  RAW (no dict), 5k distinct  (text_set raw lane)
///   5 sk int8  in [-100, 900)              (witnessed NON-dense key —
///                                           the [spill-4] pair arm)
fn row(i: u64) -> (i64, i64, String, String, i64) {
    let v = ((i * 7919) % 50) as i64;
    let d = ((i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 17) % 20_000) as i64;
    let tj = (i.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 7) % 30_000;
    let t = format!("t_{tj:05}");
    let r = format!("r_{:04}", (i.wrapping_mul(0xD6E8_FEB8_6659_FD93) >> 9) % 5_000);
    let sk = (i % 1000) as i64 - 100;
    (v, d, t, r, sk)
}

fn seal_bank(dir: &str) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let dict = |ndv_cap: u64| ColumnPosture {
        dict: Some(DictPolicy { ndv_cap, exec_ok: true, sem: TextSemantics::Utf8Chars }),
        ..Default::default()
    };
    let cands = CodecCandidates::new(ColumnPosture::default()).with_column(3, 0, dict(1 << 16));
    let resolver = pgrc2_write::seal::CodecResolver;
    let schema = vec![wcol(1, 8, 8), wcol(2, 8, 8), tcol(3), tcol(4), wcol(5, 8, 8)];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        7,
        797,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 4096, max_bytes: u64::MAX, cut_granule_rows: 1024 },
    )
    .expect("open writer");
    for i in 0..ROWS {
        let (v, d, t, r, sk) = row(i);
        let (ti, ri) = (img_4b_u(t.as_bytes()), img_4b_u(r.as_bytes()));
        let datums = [
            RawDatum::Word(v as u64),
            RawDatum::Word(d as u64),
            RawDatum::Bytes(&ti),
            RawDatum::Bytes(&ri),
            RawDatum::Word(sk as u64),
        ];
        let sources: [&dyn pgrc2_write::elect::CandidateSource; 1] = [&cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.append_row(&datums, &mut kit.ext, &mut env).expect("append");
    }
    let probe = Probe::new(TxnVerdict::Committed);
    {
        let sources: [&dyn pgrc2_write::elect::CandidateSource; 1] = [&cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.finish(&mut env).expect("finish");
    }
    w.publish(&mut vfs, &probe).expect("publish");
}

fn schema_meta() -> Vec<ColMeta> {
    vec![
        ColMeta::new(1, "v", TypMeta::INT8),
        ColMeta::new(2, "d", TypMeta::INT8),
        ColMeta::new(3, "t", TypMeta::TEXT_C),
        ColMeta::new(4, "r", TypMeta::TEXT_C),
        ColMeta::new(5, "sk", TypMeta::INT8),
    ]
}

fn bank_dir() -> String {
    let dir = std::env::temp_dir().join(format!("sqe_dsetspill_{}", std::process::id()));
    let d = dir.to_str().unwrap().to_string();
    static SEALED: std::sync::Once = std::sync::Once::new();
    SEALED.call_once(|| {
        let _ = std::fs::remove_dir_all(&d);
        seal_bank(&d);
    });
    d
}

/// `budget`: `None` = the E18 default law (never crossed at this bank —
/// the resident twin); `Some(bytes)` = the forced-low spill arm.
fn engine(budget: Option<u64>, spill: bool) -> Engine {
    sqe::spill::register_std_store();
    let bank = Bank::open(&bank_dir(), schema_meta(), &OpenOpts { bankstats: false, threads: 0 });
    Engine::new(
        bank,
        SqeConfig { threads: 3, spill, grouped_budget_override: budget, ..SqeConfig::default() },
    )
}

fn run_sql(eng: &Engine, q: u32, sql: &str) -> (Family, Vec<String>) {
    let node = lower(&eng.ctx(), q, sql, "dset").expect("lower");
    (node.family, to_lines(&eng.run(&node)))
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

fn oracle_lines(eng: &Engine, group: &[u32], dcol: u32) -> Vec<String> {
    let mut sel: Vec<(SelExpr, Option<String>)> =
        group.iter().map(|&c| (SelExpr::Expr(SqlExpr::Col(c)), None)).collect();
    sel.push((SelExpr::Agg(SqlAgg::CountDistinct(SqlExpr::Col(dcol))), None));
    let ast = SqlQuery {
        select: sel,
        preds: Vec::new(),
        group: group.iter().map(|&c| SqlExpr::Col(c)).collect(),
        order: Vec::new(),
        limit: None,
        offset: 0,
        having_count_gt: None,
    };
    sorted(to_lines(&run_oracle(&eng.bank, &ast)))
}

// ---------------------------------------------------------------------------
// SET routes: spill-forced vs resident twin, oracle, engagement
// ---------------------------------------------------------------------------

#[test]
fn set_spill_vs_resident_identity() {
    let twin = engine(None, true);
    let legs: [(&str, u32); 3] = [
        ("SELECT count(DISTINCT d) FROM sp;", 2),
        ("SELECT count(DISTINCT t) FROM sp;", 3),
        ("SELECT count(DISTINCT r) FROM sp;", 4),
    ];
    for budget in [4096u64, 1 << 16, 1 << 20] {
        let sp = engine(Some(budget), true);
        for (q, (sql, dcol)) in legs.iter().enumerate() {
            let (fam_a, a) = run_sql(&sp, 300 + q as u32, sql);
            let (fam_b, b) = run_sql(&twin, 310 + q as u32, sql);
            assert_eq!(fam_a, Family::DistinctPipeline, "set legs stay in-family: {sql}");
            assert_eq!(fam_b, Family::DistinctPipeline);
            assert_eq!(a, b, "set-spill identity: budget={budget} {sql}");
            assert_eq!(a, oracle_lines(&sp, &[], *dcol), "set-spill vs oracle: {sql}");
        }
    }
}

/// sum/avg DISTINCT legs on the byval set (first-seen value fold across
/// drains/merges), through the authored-plan seam.
#[test]
fn set_spill_sum_distinct_identity() {
    use sqe::planner::{plan_from_ap, AAgg, AFamily, AKeyExpr, APlan, AValExpr};
    let mk = |eng: &Engine| -> Vec<String> {
        let ap = APlan {
            sortagg_keys: Vec::new(),
            win: None,
            q: 0,
            family: AFamily::DistinctPipeline,
            tags: Vec::new(),
            cols: Vec::new(),
            pred: None,
            group: Vec::<u32>::new().into_iter().map(AKeyExpr::Col).collect(),
            agg: vec![
                AAgg::CountDistinct { e: AValExpr::Col(2) },
                AAgg::SumDistinct { e: AValExpr::Col(2) },
                AAgg::AvgDistinct { e: AValExpr::Col(2) },
            ],
            agg_filters: Vec::new(),
            order: None,
            having: None,
            params: Vec::new(),
            fingerprints: Vec::new(),
            kernel_oracle: String::new(),
            notes: String::new(),
            flags: Vec::new(),
        };
        let node = plan_from_ap(&eng.bank, &eng.faces, &ap).expect("lower");
        to_lines(&eng.run(&node))
    };
    let twin = engine(None, true);
    for budget in [4096u64, 1 << 16] {
        let sp = engine(Some(budget), true);
        assert_eq!(mk(&sp), mk(&twin), "sum/avg-distinct set identity: budget={budget}");
    }
}

#[test]
fn set_spill_engagement_counters() {
    use sqe::stencils::distinct::{SSPILL_DRAINS, SSPILL_FLUSHES, SSPILL_MERGES};
    use std::sync::atomic::Ordering::Relaxed;
    let sp = engine(Some(4096), true);
    for (q, sql) in [
        (330u32, "SELECT count(DISTINCT d) FROM sp;"),
        (331, "SELECT count(DISTINCT t) FROM sp;"),
    ] {
        let (f0, d0, m0) = (
            SSPILL_FLUSHES.load(Relaxed),
            SSPILL_DRAINS.load(Relaxed),
            SSPILL_MERGES.load(Relaxed),
        );
        let _ = run_sql(&sp, q, sql);
        assert!(SSPILL_FLUSHES.load(Relaxed) > f0, "pass-1 scatter spill engaged: {sql}");
        assert!(SSPILL_DRAINS.load(Relaxed) > d0, "owner-table drain engaged: {sql}");
        assert!(SSPILL_MERGES.load(Relaxed) > m0, "dedupe merge engaged: {sql}");
    }
}

#[test]
fn set_spill_rerun_determinism() {
    let sp = engine(Some(4096), true);
    for (q, sql) in [
        (340u32, "SELECT count(DISTINCT d) FROM sp;"),
        (341, "SELECT count(DISTINCT r) FROM sp;"),
    ] {
        let a = run_sql(&sp, q, sql).1;
        let b = run_sql(&sp, q, sql).1;
        assert_eq!(a, b, "set-spill rerun determinism: {sql}");
    }
}

// ---------------------------------------------------------------------------
// [spill-3] budget-aware cross-family re-election
// ---------------------------------------------------------------------------

const REELECT_SQL: &str = "SELECT v, count(DISTINCT d) FROM sp GROUP BY v;";

/// Over budget with the dense arrays priced UNDER it, the grouped pure
/// count-distinct re-elects onto the hash plane, engages the pair-plane
/// spill, and answers EXACT-ORDER identical to the resident
/// DistinctPipeline twin (the pipeline's own (distinct DESC, key ASC)).
#[test]
fn reelect_grouped_count_distinct_identity() {
    use sqe::stencils::hash_group::{DSPILL_DRAINS, DSPILL_FLUSHES, DSPILL_MERGES};
    use std::sync::atomic::Ordering::Relaxed;
    let twin = engine(None, true);
    let (fam_t, b) = run_sql(&twin, 350, REELECT_SQL);
    assert_eq!(fam_t, Family::DistinctPipeline, "under budget the election is unchanged");
    let sp = engine(Some(1 << 16), true);
    let (f0, d0, m0) = (
        DSPILL_FLUSHES.load(Relaxed),
        DSPILL_DRAINS.load(Relaxed),
        DSPILL_MERGES.load(Relaxed),
    );
    let (fam_s, a) = run_sql(&sp, 351, REELECT_SQL);
    assert_eq!(fam_s, Family::HashPlaneOwnedGroup, "over budget the family re-elects");
    assert!(DSPILL_FLUSHES.load(Relaxed) > f0, "pair scatter spill engaged");
    assert!(DSPILL_DRAINS.load(Relaxed) > d0, "dedupe-table drain engaged");
    assert!(DSPILL_MERGES.load(Relaxed) > m0, "pair run dedupe-merge engaged");
    assert_eq!(a, b, "re-elected arm answers the pipeline's exact order");
    assert_eq!(sorted(a), oracle_lines(&sp, &[1], 2), "re-elected arm vs oracle");
    // Kill switch: the legacy election, verbatim.
    let legacy = engine(Some(1 << 16), false);
    let (fam_l, c) = run_sql(&legacy, 352, REELECT_SQL);
    assert_eq!(fam_l, Family::DistinctPipeline, "kill switch keeps the election");
    assert_eq!(c, b, "legacy arm identity");
}

/// The refusal flip and its floors: the re-elected shape ADMITS over
/// budget; under the dense-array floor it stays DistinctPipeline and
/// keeps the typed no-spill-arm refusal; grouped text-key distinct and
/// the SET routes carry their own v3 verdicts.
#[test]
fn distinct_budget_verdicts() {
    // Re-elected: served.
    let sp = engine(Some(1 << 16), true);
    let node = lower(&sp.ctx(), 360, REELECT_SQL, "dset").expect("lower");
    assert_eq!(node.family, Family::HashPlaneOwnedGroup);
    check_server_grouped(&sp.bank, &sp.faces, &node)
        .expect("re-elected dense-safe count-distinct admits over budget");
    // Under the dense-array floor: no re-election — [spill-4] the
    // grouped pair arm ADMITS where v3 kept the no-spill-arm refusal.
    let tiny = engine(Some(1 << 10), true);
    let node = lower(&tiny.ctx(), 361, REELECT_SQL, "dset").expect("lower");
    assert_eq!(node.family, Family::DistinctPipeline, "dense floor blocks the re-election");
    check_server_grouped(&tiny.bank, &tiny.faces, &node)
        .expect("the grouped pair arm admits under the dense floor");
    // SET routes: over budget the former no-spill-arm refusal ADMITS.
    for (q, sql) in [
        (362u32, "SELECT count(DISTINCT d) FROM sp;"),
        (363, "SELECT count(DISTINCT t) FROM sp;"),
    ] {
        let node = lower(&tiny.ctx(), q, sql, "dset").expect("lower");
        check_server_grouped(&tiny.bank, &tiny.faces, &node)
            .unwrap_or_else(|e| panic!("set route must admit over budget ({sql}): {e:?}"));
    }
    // Grouped text-key distinct: still residue, still typed.
    let node = lower(&tiny.ctx(), 364, "SELECT t, count(DISTINCT d) FROM sp GROUP BY t;", "dset")
        .expect("lower");
    match check_server_grouped(&tiny.bank, &tiny.faces, &node) {
        Err(Refuse::GroupedSpillUnavailable { what: "no-spill-arm", .. }) => {}
        other => panic!("grouped text-key distinct stays residue, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// [spill-4] the grouped pair-spill arm
// ---------------------------------------------------------------------------

const SKQ: &str = "SELECT sk, count(DISTINCT d) FROM sp GROUP BY sk;";

/// The [w]-grouped pair arm: the dense-floor no-spill-arm refusal flips
/// to served (EXACT-ORDER identity vs the resident twin — the arm pins
/// (count DESC, key ASC)); the witnessed NON-dense key (formerly the
/// grouped-distinct-domain shape refusal) serves across budgets and vs
/// the oracle; the engagement counters prove the three moves ran; the
/// kill switch restores the domain refusal.
#[test]
fn grouped_pair_spill_identity() {
    use sqe::stencils::distinct::{PSPILL_DRAINS, PSPILL_FLUSHES, PSPILL_MERGES};
    use std::sync::atomic::Ordering::Relaxed;
    let twin = engine(None, true);
    let (fam_t, b) = run_sql(&twin, 370, REELECT_SQL);
    assert_eq!(fam_t, Family::DistinctPipeline, "under budget the resident arm holds");
    let tiny = engine(Some(1 << 10), true);
    let (f0, d0, m0) = (
        PSPILL_FLUSHES.load(Relaxed),
        PSPILL_DRAINS.load(Relaxed),
        PSPILL_MERGES.load(Relaxed),
    );
    let (fam_s, a) = run_sql(&tiny, 371, REELECT_SQL);
    assert_eq!(fam_s, Family::DistinctPipeline, "dense floor blocks the re-election");
    assert!(PSPILL_FLUSHES.load(Relaxed) > f0, "pair scatter spill engaged");
    assert!(PSPILL_DRAINS.load(Relaxed) > d0, "pair table drain engaged");
    assert!(PSPILL_MERGES.load(Relaxed) > m0, "pair run dedupe-merge engaged");
    assert_eq!(a, b, "pair-spill arm answers the resident arm's exact order");
    // The witnessed non-dense key: served across budgets, oracle-checked.
    let (fam_n, n1) = run_sql(&twin, 372, SKQ);
    assert_eq!(fam_n, Family::DistinctPipeline, "non-dense key stays in-family");
    let (_, n2) = run_sql(&tiny, 373, SKQ);
    assert_eq!(n1, n2, "non-dense pair identity across budgets");
    assert_eq!(sorted(n1), oracle_lines(&twin, &[5], 2), "non-dense pair vs oracle");
    // Rerun determinism on the spilled leg.
    let (_, a2) = run_sql(&tiny, 374, REELECT_SQL);
    assert_eq!(a2, a, "pair-spill rerun determinism");
    // Kill switch: the domain refusal returns (the legacy arm cannot
    // hold a non-dense domain).
    let legacy = engine(Some(1 << 10), false);
    let node = lower(&legacy.ctx(), 375, SKQ, "dset").expect("lower");
    match check_server_grouped(&legacy.bank, &legacy.faces, &node) {
        Err(Refuse::GroupServeUnsupported { what: "grouped-distinct-domain" }) => {}
        other => panic!("kill switch restores the domain refusal, got {other:?}"),
    }
}
