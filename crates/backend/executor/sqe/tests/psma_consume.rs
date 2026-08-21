//! PSMA-consumption gates (§8.2 → predicate evaluation): identity vs the
//! naive oracle with the face ON and OFF (the PGRUST_SQE_PSMA A/B, driven
//! through `SqeConfig.psma` directly — env is process-global), boundary
//! values at slice edges, all-match/none-match granules, NULL handling,
//! and the oracle-mode complement check riding every consult (the rig
//! feature implies `oracle`, so a lying slice would panic these runs).
//! A forged-block gate at face grain pins the paranoia mode itself:
//! `psma_candidates_eq` over a block whose entry HIDES rows is exactly
//! the wrong-answer hazard the residual cannot catch — the oracle
//! complement check must panic on it.

#![cfg(feature = "rig")]

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::engine::{Engine, SqeConfig};
use sqe::planner::{plan_from_ap, AAgg, AFamily, APlan, APred, AValExpr};
use sqe::render::to_lines;
use sqe::rig::lower::{COp, Lit, SelExpr, SqlAgg, SqlExpr, SqlPred, SqlQuery};
use sqe::rig::oracle::run_oracle;
use sqe::typmeta::TypMeta;

const ROWS: u64 = 40_000;
const GRANULE: u64 = 1024;

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

/// Row law (granule-aware so slice edges are craftable):
///  - k (int8): `g*10_000 + r*3` where g = granule ordinal, r = row in
///    granule — within-granule span 3*(GRANULE-1), so PSMA arms on every
///    granule; k ≡ 0 (mod 3) relative to the granule base, leaving
///    in-zone gaps (zone-maybe, none-match).
///  - v (int4, NULLABLE): NULL every 11th row, else `i % 50`.
///  - w (int4): constant per granule (`i / GRANULE`) — kmin == kmax, the
///    UNARMED lane (a constant granule prunes by zone alone).
fn row(i: u64) -> (i64, Option<i64>, i64) {
    let g = i / GRANULE;
    let r = i % GRANULE;
    let k = (g * 10_000 + r * 3) as i64;
    let v = (i % 11 != 0).then_some((i % 50) as i64);
    let w = (i / GRANULE) as i64;
    (k, v, w)
}

fn seal_bank(dir: &str) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let schema = vec![wcol(1, 8, 8), wcol(2, 4, 4), wcol(3, 4, 4)];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        5,
        793,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy {
            max_rows: 8192,
            max_bytes: u64::MAX,
            cut_granule_rows: GRANULE as u32,
        },
    )
    .expect("open writer");
    for i in 0..ROWS {
        let (k, v, wv) = row(i);
        let datums = [
            RawDatum::Word(k as u64),
            v.map(|x| RawDatum::Word(x as u64)).unwrap_or(RawDatum::Null),
            RawDatum::Word(wv as u64),
        ];
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

fn open_engine(dir: &str, psma: bool) -> Engine {
    let schema = vec![
        ColMeta::new(1, "k", TypMeta::INT8),
        ColMeta::new(2, "v", TypMeta::INT4),
        ColMeta::new(3, "w", TypMeta::INT4),
    ];
    let bank = Bank::open(dir, schema, &OpenOpts { bankstats: false, threads: 0 });
    Engine::new(bank, SqeConfig { threads: 2, psma, ..SqeConfig::default() })
}

fn ap(q: u32, family: AFamily) -> APlan {
    APlan {
        agg_filters: Vec::new(),
        sortagg_keys: Vec::new(),
        win: None,
        q,
        family,
        tags: Vec::new(),
        cols: Vec::new(),
        pred: None,
        group: Vec::new(),
        agg: Vec::new(),
        order: None,
        having: None,
        params: Vec::new(),
        fingerprints: Vec::new(),
        kernel_oracle: String::new(),
        notes: "psma".into(),
        flags: Vec::new(),
    }
}

/// One shape through BOTH engine arms (psma on/off) and the oracle: the
/// three renderings must agree byte-for-byte.
fn assert_ab_identity(on: &Engine, off: &Engine, p: &APlan, ast: &SqlQuery, what: &str) {
    let node_on = plan_from_ap(&on.bank, &on.faces, p).expect("plan (psma on)");
    let node_off = plan_from_ap(&off.bank, &off.faces, p).expect("plan (psma off)");
    // EmitMatches-class row-count trailers are engine-only notes: strip
    // them so the rendered-bytes compare is the ANSWER (the rig currency).
    let mut r_on = on.run(&node_on);
    let mut r_off = off.run(&node_off);
    (r_on.note, r_off.note) = (None, None);
    let a_on = to_lines(&r_on);
    let a_off = to_lines(&r_off);
    let want = to_lines(&run_oracle(&on.bank, ast));
    assert_eq!(want, a_on, "{what}: oracle vs engine (psma ON)");
    assert_eq!(want, a_off, "{what}: oracle vs engine (psma OFF)");
    assert_eq!(a_on, a_off, "{what}: psma A/B identity");
}

fn fold_plan(q: u32, pred: APred) -> APlan {
    let mut p = ap(q, AFamily::FusedFilterAgg);
    p.cols = vec![1, 2, 3];
    p.pred = Some(pred);
    p.agg = vec![
        AAgg::CountStar,
        AAgg::Sum { e: AValExpr::Col(1) },
        AAgg::Min { e: AValExpr::Col(1) },
        AAgg::Max { e: AValExpr::Col(1) },
    ];
    p
}

fn eq(col: u32, val: i64) -> SqlPred {
    SqlPred::Cmp { col, op: COp::Eq, val: Lit::Num(val.to_string()) }
}

fn between(col: u32, lo: i64, hi: i64) -> Vec<SqlPred> {
    vec![
        SqlPred::Cmp { col, op: COp::Ge, val: Lit::Num(lo.to_string()) },
        SqlPred::Cmp { col, op: COp::Le, val: Lit::Num(hi.to_string()) },
    ]
}

fn fold_ast(preds: Vec<SqlPred>) -> SqlQuery {
    SqlQuery {
        select: vec![
            (SelExpr::Agg(SqlAgg::CountStar), None),
            (SelExpr::Agg(SqlAgg::Sum(SqlExpr::Col(1))), None),
            (SelExpr::Agg(SqlAgg::Min(SqlExpr::Col(1))), None),
            (SelExpr::Agg(SqlAgg::Max(SqlExpr::Col(1))), None),
        ],
        preds,
        group: Vec::new(),
        order: Vec::new(),
        limit: None,
        offset: 0,
        having_count_gt: None,
    }
}

#[test]
fn psma_ab_identity_and_boundaries() {
    let dir = std::env::temp_dir().join(format!("sqe_psma_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);
    let on = open_engine(&dir, true);
    let off = open_engine(&dir, false);
    assert_eq!(on.bank.rows_total(), ROWS);
    assert!(on.bank.parts.len() > 1, "want a multi-part bank");
    // Premise: the sealer armed PSMA on k (the consumption lane's whole
    // point — a bank sealed without PSMA gates nothing).
    assert!(
        on.faces.psma(&on.bank, 1).is_some(),
        "test premise: Psma sections sealed for col k"
    );
    assert!(
        off.faces.psma(&off.bank, 1).is_none(),
        "kill switch: psma=false must refuse the face"
    );

    // Slice-edge boundaries on granule 0 of part 0: k spans [0, 3069]
    // there — probe the exact zone min (row 0), the exact zone max
    // (row 1023), and an in-zone GAP (zone-maybe, none-match).
    for (q, val, what) in [
        (10, 0i64, "eq at granule zone MIN (slice lo edge)"),
        (11, 3 * (GRANULE as i64 - 1), "eq at granule zone MAX (slice hi edge)"),
        (12, 5, "eq in-zone gap (zone-maybe, none-match granule)"),
        (13, 10_000, "eq at a later part's granule base"),
    ] {
        assert_ab_identity(
            &on,
            &off,
            &fold_plan(q, APred::Eq { col: 1, val: val.to_string(), fp: None }),
            &fold_ast(vec![eq(1, val)]),
            what,
        );
    }

    // Range shapes: a tight in-granule range, a whole-granule cover
    // (all-match), and a cross-granule span.
    for (q, lo, hi, what) in [
        (20, 30i64, 90i64, "tight in-granule between"),
        (21, 0, 4_000, "whole-granule cover (all-match granule)"),
        (22, 2_000, 12_000, "cross-granule between"),
        (23, 4_000, 9_000, "between over the inter-granule key gap (none-match)"),
    ] {
        assert_ab_identity(
            &on,
            &off,
            &fold_plan(
                q,
                APred::Range { col: 1, lo: lo.to_string(), hi: hi.to_string(), fp: None },
            ),
            &fold_ast(between(1, lo, hi)),
            what,
        );
    }

    // In2 (two-probe union slice): both values, straddling granules.
    assert_ab_identity(
        &on,
        &off,
        &fold_plan(24, APred::In { col: 1, vals: vec!["30".into(), "10030".into()], fp: None }),
        &fold_ast(vec![SqlPred::In { col: 1, vals: vec!["30".into(), "10030".into()] }]),
        "in2 (union of eq slices)",
    );

    // NULL handling: eq over the NULLABLE column v (NULL rows are outside
    // every PSMA entry — excluded without evaluation, exactly 3VL).
    assert_ab_identity(
        &on,
        &off,
        &fold_plan(30, APred::Eq { col: 2, val: "7".into(), fp: None }),
        &fold_ast(vec![eq(2, 7)]),
        "eq on nullable column (NULL rows excluded by slice AND by 3VL)",
    );

    // Unarmed lane: w is constant per granule (kmin == kmax ⇒ the sealer
    // does not arm) — the consult must degrade to the full scan.
    assert_ab_identity(
        &on,
        &off,
        &fold_plan(31, APred::Eq { col: 3, val: "3".into(), fp: None }),
        &fold_ast(vec![eq(3, 3)]),
        "constant-granule column (PSMA unarmed, zone-only pruning)",
    );

    // Conjunction: the intersected window (k tight-range ∧ v eq).
    assert_ab_identity(
        &on,
        &off,
        &fold_plan(
            32,
            APred::And {
                legs: vec![
                    APred::Range { col: 1, lo: "10030".into(), hi: "10600".into(), fp: None },
                    APred::Eq { col: 2, val: "7".into(), fp: None },
                ],
            },
        ),
        &fold_ast({
            let mut ps = between(1, 10_030, 10_600);
            ps.push(eq(2, 7));
            ps
        }),
        "conjunction (intersected PSMA windows)",
    );

    // EmitMatches (the fused eq-selective serve): SELECT k WHERE k = c.
    {
        let mut p = ap(40, AFamily::FusedFilterAgg);
        p.cols = vec![1];
        p.pred = Some(APred::Eq { col: 1, val: "20480".into(), fp: None });
        p.agg = vec![AAgg::Emit { e: AValExpr::Col(1) }];
        let ast = SqlQuery {
            select: vec![(SelExpr::Expr(SqlExpr::Col(1)), None)],
            preds: vec![eq(1, 20_480)],
            group: Vec::new(),
            order: Vec::new(),
            limit: None,
            offset: 0,
            having_count_gt: None,
        };
        assert_ab_identity(&on, &off, &p, &ast, "emit-matches eq (scan-serve class)");
    }

    // ScanServe pass 1: multi-column row serve under the eq conjunct.
    {
        let mut p = ap(41, AFamily::ScanServe);
        p.cols = vec![1, 2, 3];
        p.pred = Some(APred::Range { col: 1, lo: "20481".into(), hi: "20520".into(), fp: None });
        p.agg = vec![
            AAgg::Emit { e: AValExpr::Col(1) },
            AAgg::Emit { e: AValExpr::Col(2) },
            AAgg::Emit { e: AValExpr::Col(3) },
        ];
        let ast = SqlQuery {
            select: vec![
                (SelExpr::Expr(SqlExpr::Col(1)), None),
                (SelExpr::Expr(SqlExpr::Col(2)), None),
                (SelExpr::Expr(SqlExpr::Col(3)), None),
            ],
            preds: between(1, 20_481, 20_520),
            group: Vec::new(),
            order: Vec::new(),
            limit: None,
            offset: 0,
            having_count_gt: None,
        };
        assert_ab_identity(&on, &off, &p, &ast, "scan-serve pass 1 (psma-seeded)");
    }
}

/// Perf probe for the eq-selective class (run manually:
/// `cargo test --release -p sqe --features rig --test psma_consume -- --ignored --nocapture`).
/// Data Blocks shape: every granule = a dense low cluster + a far outlier
/// tail, so zone maps NEVER prune (every zone spans the whole domain) but
/// PSMA proves the vast middle empty per granule. Reports hot-rep medians
/// psma ON vs OFF for (a) the PSMA-empty eq (granule decode skipped
/// entirely) and (b) an in-cluster eq (slice-narrowed residual).
#[test]
#[ignore = "perf probe, not a gate"]
fn psma_eq_selective_perf_probe() {
    const PROWS: u64 = 2_000_000;
    let dir = std::env::temp_dir().join(format!("sqe_psma_perf_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    // Seal: k = low cluster [0,960) per granule, tail at 16M + r.
    {
        std::fs::create_dir_all(&dir).expect("mkdir");
        let mut vfs = RealVfs;
        let mut kit = Kit::new();
        let schema = vec![wcol(1, 8, 8), wcol(2, 8, 8)];
        let mut w = TableWriter::open(
            dir.clone(),
            schema,
            1663,
            5,
            794,
            TxnStamp { fxid: 100, cid: 1 },
            &Default::default(),
            PartCutPolicy {
                max_rows: 65536,
                max_bytes: u64::MAX,
                cut_granule_rows: GRANULE as u32,
            },
        )
        .expect("open writer");
        for i in 0..PROWS {
            let r = i % GRANULE;
            let k = if r < 960 { r as i64 } else { 16_000_000 + r as i64 };
            let datums = [RawDatum::Word(k as u64), RawDatum::Word(i)];
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
    let schema = vec![ColMeta::new(1, "k", TypMeta::INT8), ColMeta::new(2, "p", TypMeta::INT8)];
    let mk = |psma: bool| {
        let bank = Bank::open(&dir, schema.clone(), &OpenOpts { bankstats: false, threads: 0 });
        Engine::new(
            bank,
            SqeConfig {
                threads: 4,
                psma,
                populate: sqe::engine::PopulatePolicy::Never,
                ..SqeConfig::default()
            },
        )
    };
    let on = mk(true);
    let off = mk(false);
    let shapes: [(&str, i64); 2] =
        [("psma-empty eq (zone-maybe everywhere)", 8_000_000), ("in-cluster eq", 500)];
    for (what, val) in shapes {
        let mut p = fold_plan(90, APred::Eq { col: 1, val: val.to_string(), fp: None });
        p.cols = vec![1, 2];
        p.agg = vec![AAgg::CountStar, AAgg::Sum { e: AValExpr::Col(2) }];
        let time = |e: &Engine| -> (f64, String) {
            let node = plan_from_ap(&e.bank, &e.faces, &p).expect("plan");
            let mut best = f64::MAX;
            let mut lines = String::new();
            for _ in 0..7 {
                let t0 = std::time::Instant::now();
                let a = e.run(&node);
                let dt = t0.elapsed().as_secs_f64() * 1e3;
                best = best.min(dt);
                lines = to_lines(&a).join("|");
            }
            (best, lines)
        };
        let (t_on, a_on) = time(&on);
        let (t_off, a_off) = time(&off);
        assert_eq!(a_on, a_off, "{what}: A/B identity");
        println!(
            "PSMA-PERF|{what}|on={t_on:.3}ms|off={t_off:.3}ms|ratio={:.2}x|answer={a_on}",
            t_off / t_on
        );
    }
}

// ---------------------------------------------------------------------------
// [psma-2] follow-up consumers: window_serve, sort_grouped, distinct
// (filtered_text_set, dict + raw postures), window_replay (scan_var).
// Same gate law as above: psma ON/OFF byte-equality per consumer over
// slice-edge / gap / cross-granule / nullable shapes, an independent
// reference leg (run_oracle where the rig speaks the shape, a
// row-law-derived hand oracle for the window/sortagg shapes it cannot),
// and the oracle-mode complement check riding every consult.
// ---------------------------------------------------------------------------

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

/// The five-column row law: cols 1-3 exactly as `row` (k armed / v
/// nullable / w constant-per-granule), plus
///  - t (text, DICT posture, null-free): `d{i % 23:02}` — the
///    filtered-distinct dict path and the window_replay var lane.
///  - u (text, VERBATIM, null-free): `u{i % 37:03}` — the
///    filtered-distinct raw path and the string_agg input.
fn row5(i: u64) -> (i64, Option<i64>, i64, String, String) {
    let (k, v, w) = row(i);
    (k, v, w, format!("d{:02}", i % 23), format!("u{:03}", i % 37))
}

fn seal_bank5(dir: &str) {
    use pgrc2_write::dict::TextSemantics;
    use pgrc2_write::elect::{CodecCandidates, ColumnPosture, DictPolicy};
    use pgrc2_write::testkit::img_4b_u;
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    // Dict posture on t only; u stays verbatim (both distinct paths).
    let cands = CodecCandidates::new(ColumnPosture::default()).with_column(
        4,
        0,
        ColumnPosture {
            dict: Some(DictPolicy {
                ndv_cap: 1 << 16,
                exec_ok: true,
                sem: TextSemantics::Utf8Chars,
            }),
            ..Default::default()
        },
    );
    let resolver = pgrc2_write::seal::CodecResolver;
    let schema = vec![wcol(1, 8, 8), wcol(2, 4, 4), wcol(3, 4, 4), tcol(4), tcol(5)];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        5,
        795,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy {
            max_rows: 8192,
            max_bytes: u64::MAX,
            cut_granule_rows: GRANULE as u32,
        },
    )
    .expect("open writer");
    for i in 0..ROWS {
        let (k, v, wv, t, u) = row5(i);
        let t_img = img_4b_u(t.as_bytes());
        let u_img = img_4b_u(u.as_bytes());
        let datums = [
            RawDatum::Word(k as u64),
            v.map(|x| RawDatum::Word(x as u64)).unwrap_or(RawDatum::Null),
            RawDatum::Word(wv as u64),
            RawDatum::Bytes(&t_img),
            RawDatum::Bytes(&u_img),
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
    w.publish(&mut vfs, &Probe::new(TxnVerdict::Committed)).expect("publish");
}

fn open_engine5(dir: &str, psma: bool) -> Engine {
    let schema = vec![
        ColMeta::new(1, "k", TypMeta::INT8),
        ColMeta::new(2, "v", TypMeta::INT4),
        ColMeta::new(3, "w", TypMeta::INT4),
        ColMeta::new(4, "t", TypMeta::TEXT_C),
        ColMeta::new(5, "u", TypMeta::TEXT_C),
    ];
    let bank = Bank::open(dir, schema, &OpenOpts { bankstats: false, threads: 0 });
    Engine::new(bank, SqeConfig { threads: 2, psma, ..SqeConfig::default() })
}

/// Run one authored plan through both arms; gate byte-equality; return
/// the answer lines (notes stripped — the rendered ANSWER is the
/// currency) for the caller's reference-leg assert.
fn ab_lines(on: &Engine, off: &Engine, p: &APlan, what: &str) -> Vec<String> {
    let node_on = plan_from_ap(&on.bank, &on.faces, p).expect("plan (psma on)");
    let node_off = plan_from_ap(&off.bank, &off.faces, p).expect("plan (psma off)");
    let mut r_on = on.run(&node_on);
    let mut r_off = off.run(&node_off);
    (r_on.note, r_off.note) = (None, None);
    (r_on.head_note, r_off.head_note) = (None, None);
    let a_on = to_lines(&r_on);
    let a_off = to_lines(&r_off);
    assert_eq!(a_on, a_off, "{what}: psma A/B identity");
    a_on
}

/// The survivor set of an int-conjunct predicate over the row law,
/// ordered by k (globally unique — tie-free by construction).
fn survivors(pred: impl Fn(i64, Option<i64>) -> bool) -> Vec<u64> {
    let mut v: Vec<u64> = (0..ROWS)
        .filter(|&i| {
            let (k, vv, _) = row(i);
            pred(k, vv)
        })
        .collect();
    v.sort_by_key(|&i| row(i).0);
    v
}

#[test]
fn psma_followup_consumers() {
    use sqe::planner::{ASortKey, AWinFunc, AWinOp, AWindow};
    use sqe::rig::lower::{lower, parse_sql};

    let dir = std::env::temp_dir().join(format!("sqe_psma2_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank5(&dir);
    let on = open_engine5(&dir, true);
    let off = open_engine5(&dir, false);
    assert!(on.faces.psma(&on.bank, 1).is_some(), "test premise: Psma sealed for k");
    assert!(off.faces.psma(&off.bank, 1).is_none(), "kill switch honored");

    // The predicate shapes every consumer walks (slice edges, in-zone
    // gap, cross-granule span, inter-granule none-match, nullable 3VL
    // conjunction). (APred, row-law closure) pairs.
    let eq_k = |val: i64| APred::Eq { col: 1, val: val.to_string(), fp: None };
    let rng_k =
        |lo: i64, hi: i64| APred::Range { col: 1, lo: lo.to_string(), hi: hi.to_string(), fp: None };

    // ---- window_serve: emit k, row_number() + sum(v) OVER (ORDER BY k),
    // one partition (empty PARTITION BY) so the answer order is the
    // window order — hand-derivable from the row law.
    {
        let win_plan = |q: u32, pred: APred| -> APlan {
            let mut p = ap(q, AFamily::WindowServe);
            p.cols = vec![1, 2];
            p.pred = Some(pred);
            p.win = Some(AWindow { run_conds: Vec::new(), chain: None,
                part_cols: Vec::new(),
                order: vec![ASortKey { col: 1, desc: false, nulls_first: false }],
                funcs: vec![
                    AWinFunc { op: AWinOp::RowNumber, col: None, off: None },
                    AWinFunc { op: AWinOp::Sum, col: Some(2), off: None },
                ],
                emit: vec![1],
                frame: None,
            });
            p
        };
        // Hand oracle: default frame + unique order keys = the running
        // NULL-skipping sum; empty-so-far sum renders NULL (empty field).
        let expect = |pred: &dyn Fn(i64, Option<i64>) -> bool| -> Vec<String> {
            let mut sum: Option<i64> = None;
            survivors(pred)
                .iter()
                .enumerate()
                .map(|(n, &i)| {
                    let (k, v, _) = row(i);
                    if let Some(x) = v {
                        sum = Some(sum.unwrap_or(0) + x);
                    }
                    let s = sum.map(|x| x.to_string()).unwrap_or_default();
                    format!("{k}\t{}\t{s}", n + 1)
                })
                .collect()
        };
        for (q, p, f, what) in [
            (
                100u32,
                win_plan(100, eq_k(20_000)),
                Box::new(|k: i64, _: Option<i64>| k == 20_000) as Box<dyn Fn(i64, Option<i64>) -> bool>,
                "winserve eq at granule zone MIN (slice lo edge)",
            ),
            (
                101,
                win_plan(101, eq_k(20_005)),
                Box::new(|k, _| k == 20_005),
                "winserve eq in-zone gap (zone-maybe, none-match)",
            ),
            (
                102,
                win_plan(102, rng_k(2_000, 12_000)),
                Box::new(|k, _| (2_000..=12_000).contains(&k)),
                "winserve cross-granule between",
            ),
            (
                103,
                win_plan(
                    103,
                    APred::And { legs: vec![rng_k(20_000, 22_000), APred::Eq { col: 2, val: "7".into(), fp: None }] },
                ),
                Box::new(|k, v| (20_000..=22_000).contains(&k) && v == Some(7)),
                "winserve nullable 3VL conjunction (v NULL never passes)",
            ),
        ] {
            let _ = q;
            let got = ab_lines(&on, &off, &p, what);
            assert_eq!(expect(&*f), got, "{what}: hand-oracle identity");
        }
    }

    // ---- sort_grouped: ungrouped string_agg(u, ',' ORDER BY k) — one
    // answer row, hand-derivable (u is null-free; ORDER BY k is total).
    {
        let sg_plan = |q: u32, pred: APred| -> APlan {
            let mut p = ap(q, AFamily::SortGrouped);
            p.cols = vec![1, 5];
            p.pred = Some(pred);
            p.sortagg_keys = vec![ASortKey { col: 1, desc: false, nulls_first: false }];
            p.agg = vec![AAgg::StringAgg { col: 5, delim: Some(b",".to_vec()) }];
            p
        };
        let expect = |pred: &dyn Fn(i64, Option<i64>) -> bool| -> Vec<String> {
            let vals: Vec<String> =
                survivors(pred).iter().map(|&i| row5(i).4).collect();
            vec![vals.join(",")]
        };
        for (p, f, what) in [
            (
                sg_plan(110, eq_k(0)),
                Some(Box::new(|k: i64, _: Option<i64>| k == 0)
                    as Box<dyn Fn(i64, Option<i64>) -> bool>),
                "sortgrp eq at global zone MIN (slice lo edge)",
            ),
            (
                sg_plan(111, eq_k(3 * (GRANULE as i64 - 1))),
                Some(Box::new(|k, _| k == 3 * (GRANULE as i64 - 1))),
                "sortgrp eq at granule zone MAX (slice hi edge)",
            ),
            // The empty answer's ungrouped-NULL render is the engine's
            // (identical on both arms); the hand leg asserts survivors=0.
            (sg_plan(112, rng_k(4_000, 9_000)), None, "sortgrp inter-granule gap (none-match)"),
            (
                sg_plan(113, rng_k(2_000, 12_000)),
                Some(Box::new(|k, _| (2_000..=12_000).contains(&k))),
                "sortgrp cross-granule between",
            ),
            (
                sg_plan(
                    114,
                    APred::And { legs: vec![rng_k(20_000, 22_000), APred::Eq { col: 2, val: "7".into(), fp: None }] },
                ),
                Some(Box::new(|k, v| (20_000..=22_000).contains(&k) && v == Some(7))),
                "sortgrp nullable 3VL conjunction",
            ),
        ] {
            let got = ab_lines(&on, &off, &p, what);
            if let Some(f) = f {
                assert_eq!(expect(&*f), got, "{what}: hand-oracle identity");
            }
        }
    }

    // ---- distinct (filtered_text_set): COUNT(DISTINCT text) WHERE
    // <int conjunct> — t drives the dict path, u the raw path; the rig
    // oracle speaks this shape whole.
    {
        let d_plan = |q: u32, dcol: u32, pred: APred| -> APlan {
            let mut p = ap(q, AFamily::DistinctPipeline);
            p.cols = vec![dcol, 1];
            p.pred = Some(pred);
            p.agg = vec![AAgg::CountDistinct { e: AValExpr::Col(dcol) }];
            p
        };
        let d_ast = |dcol: u32, preds: Vec<SqlPred>| -> SqlQuery {
            SqlQuery {
                select: vec![(SelExpr::Agg(SqlAgg::CountDistinct(SqlExpr::Col(dcol))), None)],
                preds,
                group: Vec::new(),
                order: Vec::new(),
                limit: None,
                offset: 0,
                having_count_gt: None,
            }
        };
        for dcol in [4u32, 5] {
            let posture = if dcol == 4 { "dict" } else { "raw" };
            for (q, pred, preds, what) in [
                (120u32, eq_k(0), vec![eq(1, 0)], "eq at zone MIN (slice lo edge)"),
                (121, eq_k(5), vec![eq(1, 5)], "eq in-zone gap (none-match)"),
                (
                    122,
                    rng_k(2_000, 12_000),
                    between(1, 2_000, 12_000),
                    "cross-granule between",
                ),
                (
                    123,
                    rng_k(4_000, 9_000),
                    between(1, 4_000, 9_000),
                    "inter-granule gap (none-match)",
                ),
            ] {
                assert_ab_identity(
                    &on,
                    &off,
                    &d_plan(q + if dcol == 5 { 10 } else { 0 }, dcol, pred),
                    &d_ast(dcol, preds),
                    &format!("distinct {posture}: {what}"),
                );
            }
        }
        // Nullable conjunct: the distinct predicate route keeps the
        // typed 3VL refusal (no validity thread) — pin it, never a
        // wrong answer.
        let p = d_plan(140, 5, APred::Eq { col: 2, val: "7".into(), fp: None });
        assert!(
            plan_from_ap(&on.bank, &on.faces, &p).is_err(),
            "distinct: nullable conjunct must refuse typed"
        );
    }

    // ---- window_replay (scan_var): the var lane composed with int
    // conjuncts, lowered from SQL (real election), oracle-gated whole;
    // three reps walk compute / populate / replay on the ON arm.
    {
        let wr = |q: u32, sql: &str, what: &str| {
            let node_on = lower(&on.ctx(), q, sql, what).expect("lower (psma on)");
            let node_off = lower(&off.ctx(), q, sql, what).expect("lower (psma off)");
            assert_eq!(node_on.family, sqe::ir::Family::WindowReplay, "{what}: family");
            let ast = parse_sql(&on.bank, sql).expect("parse");
            let want = to_lines(&run_oracle(&on.bank, &ast));
            for rep in 0..3 {
                assert_eq!(want, to_lines(&on.run(&node_on)), "{what}: psma ON rep {rep}");
            }
            let a_off = to_lines(&off.run(&node_off));
            assert_eq!(want, a_off, "{what}: psma OFF");
        };
        wr(
            150,
            "SELECT COUNT(*) FROM hits WHERE t LIKE 'd0%' AND k >= 0 AND k <= 90;",
            "winreplay like + eq-class range at zone MIN (slice lo edge)",
        );
        wr(
            151,
            "SELECT COUNT(*) FROM hits WHERE t LIKE 'd0%' AND k >= 4 AND k <= 5;",
            "winreplay like + in-zone gap (PSMA-empty granules)",
        );
        wr(
            152,
            "SELECT COUNT(*) FROM hits WHERE t LIKE 'd1%' AND k >= 2000 AND k <= 12000;",
            "winreplay like + cross-granule between",
        );
        wr(
            153,
            "SELECT COUNT(*) FROM hits WHERE t LIKE 'd1%' AND k >= 4000 AND k <= 9000;",
            "winreplay like + inter-granule gap (none-match)",
        );
        wr(
            154,
            "SELECT u, COUNT(*), MIN(t) FROM hits WHERE t LIKE 'd1%' AND k >= 2000 AND k <= 12000 GROUP BY u ORDER BY COUNT(*) DESC LIMIT 10;",
            "winreplay grouped minbytes sidecar + int conjunct (codes stay sel-aligned)",
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Perf probe for the follow-up consumers (run manually:
/// `cargo test --release -p sqe --features rig --test psma_consume -- --ignored --nocapture`).
/// Data Blocks shape as the fold probe above: every granule = dense low
/// cluster + far outlier tail (zones never prune; PSMA proves the vast
/// middle empty per granule). One selective shape per consumer, hot-rep
/// best psma ON vs OFF — directional only, never a gate.
#[test]
#[ignore = "perf probe, not a gate"]
fn psma_followup_perf_probe() {
    use sqe::planner::{ASortKey, AWinFunc, AWinOp, AWindow};
    use sqe::rig::lower::lower;

    const PROWS: u64 = 1_000_000;
    let dir = std::env::temp_dir().join(format!("sqe_psma2_perf_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    {
        use pgrc2_write::dict::TextSemantics;
        use pgrc2_write::elect::{CodecCandidates, ColumnPosture, DictPolicy};
        use pgrc2_write::testkit::img_4b_u;
        std::fs::create_dir_all(&dir).expect("mkdir");
        let mut vfs = RealVfs;
        let mut kit = Kit::new();
        let cands = CodecCandidates::new(ColumnPosture::default()).with_column(
            3,
            0,
            ColumnPosture {
                dict: Some(DictPolicy {
                    ndv_cap: 1 << 16,
                    exec_ok: true,
                    sem: TextSemantics::Utf8Chars,
                }),
                ..Default::default()
            },
        );
        let resolver = pgrc2_write::seal::CodecResolver;
        let schema = vec![wcol(1, 8, 8), wcol(2, 8, 8), tcol(3)];
        let mut w = TableWriter::open(
            dir.clone(),
            schema,
            1663,
            5,
            796,
            TxnStamp { fxid: 100, cid: 1 },
            &Default::default(),
            PartCutPolicy {
                max_rows: 65536,
                max_bytes: u64::MAX,
                cut_granule_rows: GRANULE as u32,
            },
        )
        .expect("open writer");
        for i in 0..PROWS {
            let r = i % GRANULE;
            let k = if r < 960 { r as i64 } else { 16_000_000 + r as i64 };
            let t = format!("d{:02}", i % 97);
            let t_img = img_4b_u(t.as_bytes());
            let datums =
                [RawDatum::Word(k as u64), RawDatum::Word(i), RawDatum::Bytes(&t_img)];
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
        w.publish(&mut vfs, &Probe::new(TxnVerdict::Committed)).expect("publish");
    }
    let mk = |psma: bool| {
        let schema = vec![
            ColMeta::new(1, "k", TypMeta::INT8),
            ColMeta::new(2, "p", TypMeta::INT8),
            ColMeta::new(3, "t", TypMeta::TEXT_C),
        ];
        let bank = Bank::open(&dir, schema, &OpenOpts { bankstats: false, threads: 0 });
        Engine::new(
            bank,
            SqeConfig {
                threads: 4,
                psma,
                populate: sqe::engine::PopulatePolicy::Never,
                ..SqeConfig::default()
            },
        )
    };
    let on = mk(true);
    let off = mk(false);
    // PSMA-empty eq: k = 8M sits between every granule's cluster and its
    // outlier tail — zone-maybe EVERYWHERE, PSMA proves each granule empty.
    let pred = || APred::Eq { col: 1, val: "8000000".into(), fp: None };
    let time = |e: &Engine, p: &APlan| -> (f64, String) {
        let node = plan_from_ap(&e.bank, &e.faces, p).expect("plan");
        let mut best = f64::MAX;
        let mut lines = String::new();
        for _ in 0..7 {
            let t0 = std::time::Instant::now();
            let mut a = e.run(&node);
            (a.note, a.head_note) = (None, None);
            best = best.min(t0.elapsed().as_secs_f64() * 1e3);
            lines = to_lines(&a).join("|");
        }
        (best, lines)
    };
    let mut report = |what: &str, p: &APlan| {
        let (t_on, a_on) = time(&on, p);
        let (t_off, a_off) = time(&off, p);
        assert_eq!(a_on, a_off, "{what}: A/B identity");
        println!(
            "PSMA2-PERF|{what}|on={t_on:.3}ms|off={t_off:.3}ms|ratio={:.2}x",
            t_off / t_on
        );
    };
    {
        let mut p = ap(200, AFamily::WindowServe);
        p.cols = vec![1, 2];
        p.pred = Some(pred());
        p.win = Some(AWindow { run_conds: Vec::new(), chain: None,
            part_cols: Vec::new(),
            order: vec![ASortKey { col: 2, desc: false, nulls_first: false }],
            funcs: vec![AWinFunc { op: AWinOp::RowNumber, col: None, off: None }],
            emit: vec![2],
            frame: None,
        });
        report("window_serve psma-empty eq", &p);
    }
    {
        let mut p = ap(201, AFamily::SortGrouped);
        p.cols = vec![1, 2];
        p.pred = Some(pred());
        p.sortagg_keys = vec![ASortKey { col: 2, desc: false, nulls_first: false }];
        p.agg = vec![AAgg::PercentileDisc { col: 2, frac: 0.5 }];
        report("sort_grouped psma-empty eq", &p);
    }
    {
        let mut p = ap(202, AFamily::DistinctPipeline);
        p.cols = vec![3, 1];
        p.pred = Some(pred());
        p.agg = vec![AAgg::CountDistinct { e: AValExpr::Col(3) }];
        report("distinct psma-empty eq", &p);
    }
    {
        let sql = "SELECT COUNT(*) FROM hits WHERE t LIKE 'd1%' AND k = 8000000;";
        let node_on = lower(&on.ctx(), 203, sql, "perf").expect("lower");
        let node_off = lower(&off.ctx(), 203, sql, "perf").expect("lower");
        assert_eq!(node_on.family, sqe::ir::Family::WindowReplay);
        let run = |e: &Engine, n: &sqe::ir::PlanNode| -> (f64, String) {
            let mut best = f64::MAX;
            let mut lines = String::new();
            for _ in 0..7 {
                let t0 = std::time::Instant::now();
                let a = e.run(n);
                best = best.min(t0.elapsed().as_secs_f64() * 1e3);
                lines = to_lines(&a).join("|");
            }
            (best, lines)
        };
        let (t_on, a_on) = run(&on, &node_on);
        let (t_off, a_off) = run(&off, &node_off);
        assert_eq!(a_on, a_off, "window_replay: A/B identity");
        println!(
            "PSMA2-PERF|window_replay like + psma-empty eq|on={t_on:.3}ms|off={t_off:.3}ms|ratio={:.2}x",
            t_off / t_on
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Forged-block gate at face grain: a well-formed PSMA block whose entry
/// LIES (hides candidate rows) is the wrong-answer hazard the residual
/// cannot catch — the oracle complement check is the paranoia mode that
/// catches it, and this pins that it actually fires.
#[test]
fn forged_psma_block_is_caught_by_oracle_complement() {
    use pgrc2_meta::psma::{psma_candidates_eq, psma_index, psma_shift, PsmaAcc};
    use sqe::ir::{CmpOp, PredTerm};
    use sqe::typmeta::TypMeta;

    // Honest granule: keys 0..64, one row per key.
    let (kmin, kmax) = (0i64, 63i64);
    let shift = psma_shift(kmin, kmax);
    let mut acc = PsmaAcc::default();
    for r in 0..64u32 {
        acc.observe(psma_index(kmin, shift, r as i64), r);
    }
    let mut honest = Vec::new();
    acc.encode_into(&mut honest);

    // Forge: rebuild WITHOUT observing rows 32..64 — the block now hides
    // them while staying wire-well-formed (a lying section, not a
    // truncated one; checksums would pass on such bytes).
    let mut acc2 = PsmaAcc::default();
    for r in 0..32u32 {
        acc2.observe(psma_index(kmin, shift, r as i64), r);
    }
    let mut forged = Vec::new();
    acc2.encode_into(&mut forged);

    let term = PredTerm::new(1, CmpOp::Eq, 40, 0, TypMeta::INT8);
    let words: Vec<i64> = (0..64).collect();

    // Honest slice admits row 40 — complement check passes.
    let (lo, hi) = psma_candidates_eq(&honest, kmin, kmax, 40).expect("probe");
    assert!((lo as i64..hi as i64).contains(&40), "honest slice covers the match");
    sqe::psmaface::oracle_check_complement(
        &term,
        lo as usize,
        hi as usize,
        64,
        |_| true,
        |r| words[r],
    );

    // Forged slice excludes row 40 — the paranoia mode must panic.
    let (flo, fhi) = psma_candidates_eq(&forged, kmin, kmax, 40).expect("probe");
    assert!(
        !(flo as i64..fhi as i64).contains(&40),
        "forge premise: the lying block hides the matching row"
    );
    let got = std::panic::catch_unwind(|| {
        sqe::psmaface::oracle_check_complement(
            &term,
            flo as usize,
            fhi as usize,
            64,
            |_| true,
            |r| words[r],
        );
    });
    assert!(got.is_err(), "oracle complement check must catch a lying PSMA block");
}
