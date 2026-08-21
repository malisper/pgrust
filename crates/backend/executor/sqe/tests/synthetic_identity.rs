//! Synthetic-bank identity gate: real pgrc2_write banks, engine arms vs
//! oracle through the one render seam.

#![cfg(feature = "rig")]

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{img_4b_u, Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::answer::Validity;
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::engine::{Engine, SqeConfig};
use sqe::ir::PlanNode;
use sqe::planner::{plan_from_ap, AAgg, AFamily, AKeyExpr, AOrderBy, AOrderSpec, APlan, AValExpr};
use sqe::render::to_lines;
use sqe::rig::lower::{OKey, SelExpr, SqlAgg, SqlExpr, SqlQuery};
use sqe::rig::oracle::run_oracle;
use sqe::typmeta::TypMeta;

const ROWS: u64 = 20_000;

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

/// Deterministic NULL-free row content.
fn row(i: u64) -> (i64, i64, String) {
    let k = ((i.wrapping_mul(0x9E37_79B9_7F4A_7C15)) >> 17) as i64 % 1000;
    let g = (i % 16) as i64;
    let s = format!("s{:02}", i % 23);
    (k.abs(), g, s)
}

/// Seal via the real writer + publish path.
fn seal_bank(dir: &str) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let schema = vec![wcol(1, 8, 8), wcol(2, 4, 4), tcol(3)];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        5,
        777,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 4096, max_bytes: u64::MAX, cut_granule_rows: 1024 },
    )
    .expect("open writer");
    for i in 0..ROWS {
        let (k, g, s) = row(i);
        let img = img_4b_u(s.as_bytes());
        let datums = [
            RawDatum::Word(k as u64),
            RawDatum::Word(g as u64),
            RawDatum::Bytes(&img),
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

fn open_engine(dir: &str) -> Engine {
    let schema = vec![
        ColMeta::new(1, "k", TypMeta::INT8),
        ColMeta::new(2, "g", TypMeta::INT4),
        ColMeta::new(3, "s", TypMeta::TEXT_C),
    ];
    let bank = Bank::open(dir, schema, &OpenOpts { bankstats: false, threads: 1 });
    Engine::new(bank, SqeConfig { threads: 2, ..SqeConfig::default() })
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
        notes: "synthetic".into(),
        flags: Vec::new(),
    }
}

fn sel_agg(a: SqlAgg) -> (SelExpr, Option<String>) {
    (SelExpr::Agg(a), None)
}

/// Engine arms vs oracle, rendered.
fn assert_identity(engine: &Engine, node: &PlanNode, ast: &SqlQuery, what: &str) {
    let (answer, _, _) = sqe::rig::run_arms(engine, node);
    let want = run_oracle(&engine.bank, ast);
    // Rendered bytes through the ONE seam — the rig identity currency.
    // (EmitMatches-class trailers are engine-only notes; none here.)
    assert_eq!(to_lines(&want), to_lines(&answer), "{what}: oracle vs engine (rendered)");
}

#[test]
fn synthetic_bank_engine_vs_oracle_identity() {
    let dir = std::env::temp_dir().join(format!("sqe_synth_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);
    let engine = open_engine(&dir);
    assert_eq!(engine.bank.rows_total(), ROWS);
    assert!(engine.bank.parts.len() > 1, "want a multi-part bank");

    // P1: metadata answer — COUNT(*), SUM(k), MIN(k), MAX(k), AVG(k).
    {
        let mut p = ap(0, AFamily::MetadataAnswer);
        p.cols = vec![1];
        p.agg = vec![
            AAgg::CountStar,
            AAgg::Sum { e: AValExpr::Col(1) },
            AAgg::Min { e: AValExpr::Col(1) },
            AAgg::Max { e: AValExpr::Col(1) },
            AAgg::Avg { e: AValExpr::Col(1) },
        ];
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        let ast = SqlQuery {
            select: vec![
                sel_agg(SqlAgg::CountStar),
                sel_agg(SqlAgg::Sum(SqlExpr::Col(1))),
                sel_agg(SqlAgg::Min(SqlExpr::Col(1))),
                sel_agg(SqlAgg::Max(SqlExpr::Col(1))),
                sel_agg(SqlAgg::Avg(SqlExpr::Col(1))),
            ],
            preds: Vec::new(),
            group: Vec::new(),
            order: Vec::new(),
            limit: None,
            offset: 0,
            having_count_gt: None,
        };
        assert_identity(&engine, &node, &ast, "metadata");
    }

    // P2: hash-plane int key — SELECT g, COUNT(*) GROUP BY g
    {
        let mut p = ap(1, AFamily::HashPlaneOwnedGroup);
        p.cols = vec![2];
        p.group = vec![AKeyExpr::Col(2)];
        p.agg = vec![AAgg::CountStar];
        p.order = Some(AOrderSpec { by: AOrderBy::CountDesc, limit: Some(5), offset: 0 });
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        let ast = SqlQuery {
            select: vec![(SelExpr::Expr(SqlExpr::Col(2)), None), sel_agg(SqlAgg::CountStar)],
            preds: Vec::new(),
            group: vec![SqlExpr::Col(2)],
            order: vec![(OKey::AggRef(1), true)],
            limit: Some(5),
            offset: 0,
            having_count_gt: None,
        };
        assert_identity(&engine, &node, &ast, "hash_int_key");
    }

    // P3: distinct pipeline — SELECT COUNT(DISTINCT k).
    {
        let mut p = ap(2, AFamily::DistinctPipeline);
        p.cols = vec![1];
        p.agg = vec![AAgg::CountDistinct { e: AValExpr::Col(1) }];
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        let ast = SqlQuery {
            select: vec![sel_agg(SqlAgg::CountDistinct(SqlExpr::Col(1)))],
            preds: Vec::new(),
            group: Vec::new(),
            order: Vec::new(),
            limit: None,
            offset: 0,
            having_count_gt: None,
        };
        assert_identity(&engine, &node, &ast, "distinct_int_set");
    }

    // P4: dense-domain group + distinct leg — SELECT g, COUNT(*),
    {
        let mut p = ap(3, AFamily::HashPlaneOwnedGroup);
        p.cols = vec![2, 1];
        p.group = vec![AKeyExpr::Col(2)];
        p.agg = vec![AAgg::CountStar, AAgg::CountDistinct { e: AValExpr::Col(1) }];
        p.order = Some(AOrderSpec { by: AOrderBy::CountDesc, limit: Some(5), offset: 0 });
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        let ast = SqlQuery {
            select: vec![
                (SelExpr::Expr(SqlExpr::Col(2)), None),
                sel_agg(SqlAgg::CountStar),
                sel_agg(SqlAgg::CountDistinct(SqlExpr::Col(1))),
            ],
            preds: Vec::new(),
            group: vec![SqlExpr::Col(2)],
            order: vec![(OKey::AggRef(1), true)],
            limit: Some(5),
            offset: 0,
            having_count_gt: None,
        };
        assert_identity(&engine, &node, &ast, "dense_distinct");
    }

    // P5: two-level text group — SELECT s, COUNT(*) GROUP BY s
    {
        let mut p = ap(4, AFamily::TwoLevelCodeAgg);
        p.cols = vec![3];
        p.group = vec![AKeyExpr::Col(3)];
        p.agg = vec![AAgg::CountStar];
        p.order = Some(AOrderSpec { by: AOrderBy::CountDesc, limit: Some(5), offset: 0 });
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        let ast = SqlQuery {
            select: vec![(SelExpr::Expr(SqlExpr::Col(3)), None), sel_agg(SqlAgg::CountStar)],
            preds: Vec::new(),
            group: vec![SqlExpr::Col(3)],
            order: vec![(OKey::AggRef(1), true)],
            limit: Some(5),
            offset: 0,
            having_count_gt: None,
        };
        assert_identity(&engine, &node, &ast, "text_group_count");
    }

    // P6 (rung B): ungrouped filtered scalar fold — COUNT/SUM/MIN/MAX/AVG
    {
        use sqe::planner::APred;
        use sqe::rig::lower::{COp, Lit, SqlPred};
        let mut p = ap(6, AFamily::FusedFilterAgg);
        p.cols = vec![1, 2];
        p.pred = Some(APred::And {
            legs: vec![
                APred::Range { col: 2, lo: "8".into(), hi: "9223372036854775807".into(), fp: None },
                APred::Ne { col: 1, val: "500".into(), fp: None },
            ],
        });
        p.agg = vec![
            AAgg::CountStar,
            AAgg::Sum { e: AValExpr::Col(1) },
            AAgg::Min { e: AValExpr::Col(1) },
            AAgg::Max { e: AValExpr::Col(1) },
            AAgg::Avg { e: AValExpr::Col(1) },
        ];
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        let ast = SqlQuery {
            select: vec![
                sel_agg(SqlAgg::CountStar),
                sel_agg(SqlAgg::Sum(SqlExpr::Col(1))),
                sel_agg(SqlAgg::Min(SqlExpr::Col(1))),
                sel_agg(SqlAgg::Max(SqlExpr::Col(1))),
                sel_agg(SqlAgg::Avg(SqlExpr::Col(1))),
            ],
            preds: vec![
                SqlPred::Cmp { col: 2, op: COp::Gt, val: Lit::Num("7".into()) },
                SqlPred::Cmp { col: 1, op: COp::Ne, val: Lit::Num("500".into()) },
            ],
            group: Vec::new(),
            order: Vec::new(),
            limit: None,
            offset: 0,
            having_count_gt: None,
        };
        assert_identity(&engine, &node, &ast, "filtered_scalar_fold");
    }

    // P7 (rung B): zero survivors — count 0, strict aggs NULL, through
    {
        use sqe::planner::APred;
        use sqe::rig::lower::{COp, Lit, SqlPred};
        let mut p = ap(7, AFamily::FusedFilterAgg);
        p.cols = vec![1];
        p.pred = Some(APred::Eq { col: 1, val: "-424242".into(), fp: None });
        p.agg = vec![
            AAgg::CountStar,
            AAgg::Sum { e: AValExpr::Col(1) },
            AAgg::Min { e: AValExpr::Col(1) },
        ];
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        let (answer, _, _) = sqe::rig::run_arms(&engine, &node);
        assert_eq!(to_lines(&answer), vec!["0\t\t".to_string()], "empty fold: count 0, NULLs");
        let ast = SqlQuery {
            select: vec![
                sel_agg(SqlAgg::CountStar),
                sel_agg(SqlAgg::Sum(SqlExpr::Col(1))),
                sel_agg(SqlAgg::Min(SqlExpr::Col(1))),
            ],
            preds: vec![SqlPred::Cmp {
                col: 1,
                op: COp::Eq,
                val: Lit::Num("-424242".into()),
            }],
            group: Vec::new(),
            order: Vec::new(),
            limit: None,
            offset: 0,
            having_count_gt: None,
        };
        assert_identity(&engine, &node, &ast, "filtered_scalar_fold_empty");
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// MIN/MAX over an empty set is NULL, never 0.
#[test]
fn min_over_empty_renders_null() {
    let dir = std::env::temp_dir().join(format!("sqe_synth_mm_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);
    let engine = open_engine(&dir);
    let ast = SqlQuery {
        select: vec![
            sel_agg(SqlAgg::Min(SqlExpr::Col(1))),
            sel_agg(SqlAgg::Max(SqlExpr::Col(1))),
            sel_agg(SqlAgg::CountStar),
        ],
        preds: vec![sqe::rig::lower::SqlPred::Cmp {
            col: 1,
            op: sqe::rig::lower::COp::Eq,
            val: sqe::rig::lower::Lit::Num("-424242".into()),
        }],
        group: Vec::new(),
        order: Vec::new(),
        limit: None,
        offset: 0,
        having_count_gt: None,
    };
    let want = run_oracle(&engine.bank, &ast);
    assert!(matches!(want.cols[0].validity, Validity::Mask(ref m) if m == &vec![false]));
    assert!(matches!(want.cols[1].validity, Validity::Mask(ref m) if m == &vec![false]));
    assert_eq!(to_lines(&want), vec!["\t\t0".to_string()]);
    let _ = std::fs::remove_dir_all(&dir);
}

// 3VL: the nullable-bank identity gate (scalar NULLs only).

/// Nullable rows: k NULL every 7th, g every 5th, s every 11th.
fn nrow(i: u64) -> (Option<i64>, Option<i64>, Option<String>) {
    let k = ((i.wrapping_mul(0x9E37_79B9_7F4A_7C15)) >> 17) as i64 % 1000;
    let k = if i % 7 == 3 { None } else { Some(k.abs()) };
    let g = if i % 5 == 1 { None } else { Some((i % 16) as i64) };
    let s = if i % 11 == 7 { None } else { Some(format!("s{:02}", i % 23)) };
    (k, g, s)
}

fn seal_nullable_bank(dir: &str) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let schema = vec![wcol(1, 8, 8), wcol(2, 4, 4), tcol(3)];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        5,
        779,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 4096, max_bytes: u64::MAX, cut_granule_rows: 1024 },
    )
    .expect("open writer");
    for i in 0..ROWS {
        let (k, g, s) = nrow(i);
        let img = s.as_ref().map(|s| img_4b_u(s.as_bytes()));
        let datums = [
            k.map(|v| RawDatum::Word(v as u64)).unwrap_or(RawDatum::Null),
            g.map(|v| RawDatum::Word(v as u64)).unwrap_or(RawDatum::Null),
            img.as_ref().map(|b| RawDatum::Bytes(b)).unwrap_or(RawDatum::Null),
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

#[test]
fn nullable_bank_engine_vs_oracle_identity() {
    use sqe::planner::plan_from_ap;
    use sqe::rig::lower::{COp, Lit, SqlPred};
    let dir = std::env::temp_dir().join(format!("sqe_synth_null_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_nullable_bank(&dir);
    let engine = open_engine(&dir);
    assert!(!engine.bank.null_free(1), "k must carry a real validity stream");
    assert!(!engine.bank.null_free(2), "g must carry a real validity stream");

    // N1: metadata aggs over nullable data — COUNT(*) counts every row;
    {
        let mut p = ap(10, AFamily::MetadataAnswer);
        p.cols = vec![1];
        p.agg = vec![
            AAgg::CountStar,
            AAgg::Sum { e: AValExpr::Col(1) },
            AAgg::Min { e: AValExpr::Col(1) },
            AAgg::Max { e: AValExpr::Col(1) },
            AAgg::Avg { e: AValExpr::Col(1) },
        ];
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        let ast = SqlQuery {
            select: vec![
                sel_agg(SqlAgg::CountStar),
                sel_agg(SqlAgg::Sum(SqlExpr::Col(1))),
                sel_agg(SqlAgg::Min(SqlExpr::Col(1))),
                sel_agg(SqlAgg::Max(SqlExpr::Col(1))),
                sel_agg(SqlAgg::Avg(SqlExpr::Col(1))),
            ],
            preds: Vec::new(),
            group: Vec::new(),
            order: Vec::new(),
            limit: None,
            offset: 0,
            having_count_gt: None,
        };
        assert_identity(&engine, &node, &ast, "nullable_metadata");
    }

    // N2: NULL is its own group (PG semantics) — nullable int group key
    {
        let mut p = ap(11, AFamily::HashPlaneOwnedGroup);
        p.cols = vec![2];
        p.group = vec![AKeyExpr::Col(2)];
        p.agg = vec![AAgg::CountStar];
        p.order = Some(AOrderSpec { by: AOrderBy::CountDesc, limit: Some(20), offset: 0 });
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        let (answer, _, _) = sqe::rig::run_arms(&engine, &node);
        assert!(
            matches!(&answer.cols[0].validity, Validity::Mask(m) if m.iter().any(|&v| !v)),
            "the NULL group key must reach the answer's validity leg"
        );
        let ast = SqlQuery {
            select: vec![(SelExpr::Expr(SqlExpr::Col(2)), None), sel_agg(SqlAgg::CountStar)],
            preds: Vec::new(),
            group: vec![SqlExpr::Col(2)],
            order: vec![(OKey::AggRef(1), true)],
            limit: Some(20),
            offset: 0,
            having_count_gt: None,
        };
        assert_identity(&engine, &node, &ast, "nullable_group_key");
    }

    // N2b: TWO-column nullable key — the interleaved [n0][v0][n1][v1]
    {
        let mut p = ap(16, AFamily::HashPlaneOwnedGroup);
        p.cols = vec![2, 1];
        p.group = vec![AKeyExpr::Col(2), AKeyExpr::Col(1)];
        p.agg = vec![AAgg::CountStar];
        p.order = Some(AOrderSpec { by: AOrderBy::CountDesc, limit: Some(50), offset: 0 });
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        let ast = SqlQuery {
            select: vec![
                (SelExpr::Expr(SqlExpr::Col(2)), None),
                (SelExpr::Expr(SqlExpr::Col(1)), None),
                sel_agg(SqlAgg::CountStar),
            ],
            preds: Vec::new(),
            group: vec![SqlExpr::Col(2), SqlExpr::Col(1)],
            order: vec![(OKey::AggRef(2), true)],
            limit: Some(50),
            offset: 0,
            having_count_gt: None,
        };
        assert_identity(&engine, &node, &ast, "nullable_pair_key");
    }

    // N2c [sqe-grpfold]: grouped word-key FOLDS — SELECT g, COUNT(*),
    // SUM(k), MIN(k), MAX(k), AVG(k) GROUP BY g. NULL group key is its
    // own group; NULL fold inputs fold NOTHING (SUM/MIN/MAX over an
    // all-NULL group answer NULL; AVG divides by count(nonnull)).
    {
        let mut p = ap(17, AFamily::HashPlaneOwnedGroup);
        p.cols = vec![2, 1];
        p.group = vec![AKeyExpr::Col(2)];
        p.agg = vec![
            AAgg::CountStar,
            AAgg::Sum { e: AValExpr::Col(1) },
            AAgg::Min { e: AValExpr::Col(1) },
            AAgg::Max { e: AValExpr::Col(1) },
            AAgg::Avg { e: AValExpr::Col(1) },
        ];
        p.order = Some(AOrderSpec { by: AOrderBy::CountDesc, limit: Some(20), offset: 0 });
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan grouped folds");
        let ast = SqlQuery {
            select: vec![
                (SelExpr::Expr(SqlExpr::Col(2)), None),
                sel_agg(SqlAgg::CountStar),
                sel_agg(SqlAgg::Sum(SqlExpr::Col(1))),
                sel_agg(SqlAgg::Min(SqlExpr::Col(1))),
                sel_agg(SqlAgg::Max(SqlExpr::Col(1))),
                sel_agg(SqlAgg::Avg(SqlExpr::Col(1))),
            ],
            preds: Vec::new(),
            group: vec![SqlExpr::Col(2)],
            order: vec![(OKey::AggRef(1), true)],
            limit: Some(20),
            offset: 0,
            having_count_gt: None,
        };
        assert_identity(&engine, &node, &ast, "nullable_grouped_folds");
    }

    // N2d [sqe-grpfold]: TWO-column nullable key + folds (the
    // interleaved [n0][v0][n1][v1] pack under the Cells128 route).
    {
        let mut p = ap(18, AFamily::HashPlaneOwnedGroup);
        p.cols = vec![2, 1];
        p.group = vec![AKeyExpr::Col(2), AKeyExpr::Col(1)];
        p.agg = vec![
            AAgg::CountStar,
            AAgg::Sum { e: AValExpr::Col(1) },
            AAgg::Min { e: AValExpr::Col(1) },
        ];
        p.order = Some(AOrderSpec { by: AOrderBy::CountDesc, limit: Some(50), offset: 0 });
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan pair folds");
        let ast = SqlQuery {
            select: vec![
                (SelExpr::Expr(SqlExpr::Col(2)), None),
                (SelExpr::Expr(SqlExpr::Col(1)), None),
                sel_agg(SqlAgg::CountStar),
                sel_agg(SqlAgg::Sum(SqlExpr::Col(1))),
                sel_agg(SqlAgg::Min(SqlExpr::Col(1))),
            ],
            preds: Vec::new(),
            group: vec![SqlExpr::Col(2), SqlExpr::Col(1)],
            order: vec![(OKey::AggRef(2), true)],
            limit: Some(50),
            offset: 0,
            having_count_gt: None,
        };
        assert_identity(&engine, &node, &ast, "nullable_pair_key_folds");
    }

    // N3: 3VL WHERE over a nullable column — `g <> 1` must NOT pass NULL
    {
        let mut p = ap(12, AFamily::FusedFilterAgg);
        p.cols = vec![2];
        p.pred = Some(sqe::planner::APred::Eq { col: 2, val: "3".into(), fp: None });
        p.group = vec![AKeyExpr::Col(2)];
        p.agg = vec![AAgg::CountStar];
        p.order = Some(AOrderSpec { by: AOrderBy::CountDesc, limit: Some(5), offset: 0 });
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        let ast = SqlQuery {
            select: vec![(SelExpr::Expr(SqlExpr::Col(2)), None), sel_agg(SqlAgg::CountStar)],
            preds: vec![SqlPred::Cmp { col: 2, op: COp::Eq, val: Lit::Num("3".into()) }],
            group: vec![SqlExpr::Col(2)],
            order: vec![(OKey::AggRef(1), true)],
            limit: Some(5),
            offset: 0,
            having_count_gt: None,
        };
        assert_identity(&engine, &node, &ast, "nullable_pred_group");
    }

    // N4: filtered-agg-over-empty regression at ENGINE grain — a
    {
        let mut p = ap(13, AFamily::FusedFilterAgg);
        p.cols = vec![2];
        p.pred = Some(sqe::planner::APred::Eq { col: 2, val: "-42".into(), fp: None });
        p.group = vec![AKeyExpr::Col(2)];
        p.agg = vec![AAgg::CountStar];
        p.order = Some(AOrderSpec { by: AOrderBy::CountDesc, limit: Some(5), offset: 0 });
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        let ast = SqlQuery {
            select: vec![(SelExpr::Expr(SqlExpr::Col(2)), None), sel_agg(SqlAgg::CountStar)],
            preds: vec![SqlPred::Cmp { col: 2, op: COp::Eq, val: Lit::Num("-42".into()) }],
            group: vec![SqlExpr::Col(2)],
            order: vec![(OKey::AggRef(1), true)],
            limit: Some(5),
            offset: 0,
            having_count_gt: None,
        };
        assert_identity(&engine, &node, &ast, "filtered_group_over_empty");
    }

    // N5: filtered-agg-over-empty → NULL (the oracle-witnessed scalar
    {
        let ast = SqlQuery {
            select: vec![
                sel_agg(SqlAgg::Min(SqlExpr::Col(1))),
                sel_agg(SqlAgg::Max(SqlExpr::Col(1))),
                sel_agg(SqlAgg::Sum(SqlExpr::Col(1))),
                sel_agg(SqlAgg::Avg(SqlExpr::Col(1))),
                sel_agg(SqlAgg::CountStar),
            ],
            preds: vec![SqlPred::Cmp {
                col: 1,
                op: COp::Eq,
                val: Lit::Num("-424242".into()),
            }],
            group: Vec::new(),
            order: Vec::new(),
            limit: None,
            offset: 0,
            having_count_gt: None,
        };
        let want = run_oracle(&engine.bank, &ast);
        assert_eq!(to_lines(&want), vec!["\t\t\t\t0".to_string()]);
    }

    // N6 (rung B): UNGROUPED filtered scalar fold over NULLABLE data —
    {
        let mut p = ap(17, AFamily::FusedFilterAgg);
        p.cols = vec![1, 2];
        p.pred = Some(sqe::planner::APred::Ne { col: 2, val: "3".into(), fp: None });
        p.agg = vec![
            AAgg::CountStar,
            AAgg::Sum { e: AValExpr::Col(1) },
            AAgg::Min { e: AValExpr::Col(1) },
            AAgg::Max { e: AValExpr::Col(1) },
            AAgg::Avg { e: AValExpr::Col(1) },
        ];
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        let ast = SqlQuery {
            select: vec![
                sel_agg(SqlAgg::CountStar),
                sel_agg(SqlAgg::Sum(SqlExpr::Col(1))),
                sel_agg(SqlAgg::Min(SqlExpr::Col(1))),
                sel_agg(SqlAgg::Max(SqlExpr::Col(1))),
                sel_agg(SqlAgg::Avg(SqlExpr::Col(1))),
            ],
            preds: vec![SqlPred::Cmp { col: 2, op: COp::Ne, val: Lit::Num("3".into()) }],
            group: Vec::new(),
            order: Vec::new(),
            limit: None,
            offset: 0,
            having_count_gt: None,
        };
        assert_identity(&engine, &node, &ast, "nullable_filtered_scalar_fold");
    }

    // R1: dict lanes carry a zero-null proof — a nullable dict text
    {
        let mut p = ap(14, AFamily::TwoLevelCodeAgg);
        p.cols = vec![3];
        p.group = vec![AKeyExpr::Col(3)];
        p.agg = vec![AAgg::CountStar];
        p.order = Some(AOrderSpec { by: AOrderBy::CountDesc, limit: Some(5), offset: 0 });
        let any_dict =
            (0..engine.bank.parts.len()).any(|pi| sqe::scan::is_dict(&engine.bank, pi, 3));
        match plan_from_ap(&engine.bank, &engine.faces, &p) {
            Err(sqe::refuse::Refuse::NullableDict { attno: 3 }) => {
                assert!(any_dict, "NullableDict requires a dict part")
            }
            Err(sqe::refuse::Refuse::NullableUnsupported { attno: 3, .. }) => {
                assert!(!any_dict, "dict parts must refuse with the dict cause")
            }
            other => panic!("nullable text group key must refuse (typed), got {other:?}"),
        }
    }

    // R2: a nullable column on a family whose fold is not yet
    {
        let mut p = ap(15, AFamily::DistinctPipeline);
        p.cols = vec![1];
        p.agg = vec![AAgg::CountDistinct { e: AValExpr::Col(1) }];
        match plan_from_ap(&engine.bank, &engine.faces, &p) {
            Err(sqe::refuse::Refuse::NullableUnsupported { attno: 1, .. }) => {}
            other => panic!("nullable distinct input must refuse, got {other:?}"),
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

// [sqe-grpfold] i64-overflow-adjacent grouped folds: signed domain with
// |k| near i64::MAX — per-group SUMs leave the i64 domain (exact i128
// cells), MIN/MAX sit at the extremes, NULLs thread 3VL.

fn seal_wide_bank(dir: &str) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let schema = vec![wcol(1, 8, 8), wcol(2, 4, 4), tcol(3)];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        5,
        781,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 2048, max_bytes: u64::MAX, cut_granule_rows: 512 },
    )
    .expect("open writer");
    let big = i64::MAX - 3;
    for i in 0..6000u64 {
        let k = if i % 13 == 0 {
            None
        } else if i % 2 == 0 {
            Some(big - (i % 7) as i64)
        } else {
            Some(-big + (i % 5) as i64)
        };
        let g = (i % 8) as i64;
        let s = format!("w{}", i % 5);
        let img = img_4b_u(s.as_bytes());
        let datums = [
            k.map(|v| RawDatum::Word(v as u64)).unwrap_or(RawDatum::Null),
            RawDatum::Word(g as u64),
            RawDatum::Bytes(&img),
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

#[test]
fn grouped_fold_wide_domain_identity() {
    let dir = std::env::temp_dir().join(format!("sqe_synth_wide_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_wide_bank(&dir);
    let engine = open_engine(&dir);

    // W1: full fold vocabulary, overflow-adjacent SUM (i128 exact).
    {
        let mut p = ap(40, AFamily::HashPlaneOwnedGroup);
        p.cols = vec![2, 1];
        p.group = vec![AKeyExpr::Col(2)];
        p.agg = vec![
            AAgg::CountStar,
            AAgg::Sum { e: AValExpr::Col(1) },
            AAgg::Min { e: AValExpr::Col(1) },
            AAgg::Max { e: AValExpr::Col(1) },
            AAgg::Avg { e: AValExpr::Col(1) },
        ];
        p.order = Some(AOrderSpec { by: AOrderBy::CountDesc, limit: Some(10), offset: 0 });
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan wide folds");
        let ast = SqlQuery {
            select: vec![
                (SelExpr::Expr(SqlExpr::Col(2)), None),
                sel_agg(SqlAgg::CountStar),
                sel_agg(SqlAgg::Sum(SqlExpr::Col(1))),
                sel_agg(SqlAgg::Min(SqlExpr::Col(1))),
                sel_agg(SqlAgg::Max(SqlExpr::Col(1))),
                sel_agg(SqlAgg::Avg(SqlExpr::Col(1))),
            ],
            preds: Vec::new(),
            group: vec![SqlExpr::Col(2)],
            order: vec![(OKey::AggRef(1), true)],
            limit: Some(10),
            offset: 0,
            having_count_gt: None,
        };
        assert_identity(&engine, &node, &ast, "wide_grouped_folds");
    }

    // W2: multiple SUM legs (leaves the single-payload fast lanes).
    {
        let mut p = ap(41, AFamily::HashPlaneOwnedGroup);
        p.cols = vec![2, 1];
        p.group = vec![AKeyExpr::Col(2)];
        p.agg = vec![
            AAgg::CountStar,
            AAgg::Sum { e: AValExpr::Col(1) },
            AAgg::Sum { e: AValExpr::Col(2) },
            AAgg::Max { e: AValExpr::Col(2) },
        ];
        p.order = Some(AOrderSpec { by: AOrderBy::CountDesc, limit: Some(10), offset: 0 });
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan multi-sum");
        let ast = SqlQuery {
            select: vec![
                (SelExpr::Expr(SqlExpr::Col(2)), None),
                sel_agg(SqlAgg::CountStar),
                sel_agg(SqlAgg::Sum(SqlExpr::Col(1))),
                sel_agg(SqlAgg::Sum(SqlExpr::Col(2))),
                sel_agg(SqlAgg::Max(SqlExpr::Col(2))),
            ],
            preds: Vec::new(),
            group: vec![SqlExpr::Col(2)],
            order: vec![(OKey::AggRef(1), true)],
            limit: Some(10),
            offset: 0,
            having_count_gt: None,
        };
        assert_identity(&engine, &node, &ast, "wide_multi_sum");
    }

    // W3: varlena fold input on the byval grouped arm stays a TYPED
    // refusal (no word embed — never a lossy fold).
    {
        let mut p = ap(42, AFamily::HashPlaneOwnedGroup);
        p.cols = vec![2, 3];
        p.group = vec![AKeyExpr::Col(2)];
        p.agg = vec![AAgg::CountStar, AAgg::Min { e: AValExpr::Col(3) }];
        match plan_from_ap(&engine.bank, &engine.faces, &p) {
            Err(e) => {
                let s = e.to_string();
                assert!(
                    s.contains("min-max-face") || s.contains("grouped-fold-face"),
                    "unexpected refusal: {s}"
                );
            }
            Ok(_) => panic!("varlena grouped fold input must refuse (typed)"),
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

// [fpcombine] fp-on identity arms (unique masses => order-proof).

/// 25 groups; group j: 40*(j+1) rows, j+1 distinct k, round-robin
/// across parts.
fn seal_fp_bank(dir: &str) {
    use pgrc2_write::dict::TextSemantics;
    use pgrc2_write::elect::{CodecCandidates, ColumnPosture, DictPolicy};
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    // dict posture on the text column (the fp plane serves DICT parts)
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
    let schema = vec![wcol(1, 8, 8), wcol(2, 4, 4), tcol(3)];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        5,
        781,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 4096, max_bytes: u64::MAX, cut_granule_rows: 1024 },
    )
    .expect("open writer");
    const NG: u64 = 25;
    let sname = |j: u64| if j == 24 { String::new() } else { format!("t{j:02}") };
    let mut left: Vec<u64> = (0..NG).map(|j| 40 * (j + 1)).collect();
    let mut emitted: Vec<u64> = vec![0; NG as usize];
    let mut remaining: u64 = left.iter().sum();
    while remaining > 0 {
        for j in 0..NG {
            if left[j as usize] == 0 {
                continue;
            }
            let s = sname(j);
            let k = 1000 * j + (emitted[j as usize] % (j + 1)); // j+1 distinct
            let g = j % 5;
            let img = img_4b_u(s.as_bytes());
            let datums = [
                RawDatum::Word(k),
                RawDatum::Word(g),
                RawDatum::Bytes(&img),
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
            left[j as usize] -= 1;
            emitted[j as usize] += 1;
            remaining -= 1;
        }
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

fn fp_engine(dir: &str, fpcache: bool) -> Engine {
    fp_engine_arm(dir, true, fpcache)
}

/// Explicit fp opt-out arm constructor.
fn fp_engine_arm(dir: &str, fpcombine: bool, fpcache: bool) -> Engine {
    let schema = vec![
        ColMeta::new(1, "k", TypMeta::INT8),
        ColMeta::new(2, "g", TypMeta::INT4),
        ColMeta::new(3, "s", TypMeta::TEXT_C),
    ];
    let bank = Bank::open(dir, schema, &OpenOpts { bankstats: false, threads: 1 });
    Engine::new(
        bank,
        SqeConfig { threads: 2, fpcombine, fpcache, ..SqeConfig::default() },
    )
}

fn fp_cases(engine: &Engine, reps: usize) {
    use sqe::rig::lower::{COp, Lit, SqlPred};
    // F1: part_count -> fp_combine (GROUP BY s, COUNT(*)).
    {
        let mut p = ap(20, AFamily::TwoLevelCodeAgg);
        p.cols = vec![3];
        p.group = vec![AKeyExpr::Col(3)];
        p.agg = vec![AAgg::CountStar];
        p.order = Some(AOrderSpec { by: AOrderBy::CountDesc, limit: Some(100), offset: 0 });
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        let ast = SqlQuery {
            select: vec![(SelExpr::Expr(SqlExpr::Col(3)), None), sel_agg(SqlAgg::CountStar)],
            preds: Vec::new(),
            group: vec![SqlExpr::Col(3)],
            order: vec![(OKey::AggRef(1), true)],
            limit: Some(100),
            offset: 0,
            having_count_gt: None,
        };
        for _ in 0..reps {
            assert_identity(engine, &node, &ast, "fp_part_count");
        }
    }
    // F2: gid_grouped -> pair_distinct_fp + fp_combine_pre
    {
        let mut p = ap(21, AFamily::DistinctPipeline);
        p.cols = vec![3, 1];
        p.group = vec![AKeyExpr::Col(3)];
        p.agg = vec![AAgg::CountDistinct { e: AValExpr::Col(1) }];
        p.order = Some(AOrderSpec { by: AOrderBy::CountDesc, limit: Some(100), offset: 0 });
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        let ast = SqlQuery {
            select: vec![(SelExpr::Expr(SqlExpr::Col(3)), None), sel_agg(SqlAgg::CountDistinct(SqlExpr::Col(1)))],
            preds: Vec::new(),
            group: vec![SqlExpr::Col(3)],
            order: vec![(OKey::AggRef(1), true)],
            limit: Some(100),
            offset: 0,
            having_count_gt: None,
        };
        for _ in 0..reps {
            assert_identity(engine, &node, &ast, "fp_gid_grouped");
        }
    }
    // F2b: the drop-empty leg of pair_distinct_fp (s <> '').
    {
        let mut p = ap(22, AFamily::DistinctPipeline);
        p.cols = vec![3, 1];
        p.pred = Some(sqe::planner::APred::NeEmpty { col: 3, fp: None });
        p.group = vec![AKeyExpr::Col(3)];
        p.agg = vec![AAgg::CountDistinct { e: AValExpr::Col(1) }];
        p.order = Some(AOrderSpec { by: AOrderBy::CountDesc, limit: Some(100), offset: 0 });
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        let ast = SqlQuery {
            select: vec![(SelExpr::Expr(SqlExpr::Col(3)), None), sel_agg(SqlAgg::CountDistinct(SqlExpr::Col(1)))],
            preds: vec![SqlPred::Cmp { col: 3, op: COp::Ne, val: Lit::Str(String::new()) }],
            group: vec![SqlExpr::Col(3)],
            order: vec![(OKey::AggRef(1), true)],
            limit: Some(100),
            offset: 0,
            having_count_gt: None,
        };
        for _ in 0..reps {
            assert_identity(engine, &node, &ast, "fp_gid_grouped_drop_empty");
        }
    }
    // F3: int_gid_fp (GROUP BY g, s COUNT(*)).
    {
        let mut p = ap(23, AFamily::HashPlaneOwnedGroup);
        p.cols = vec![2, 3];
        p.group = vec![AKeyExpr::Col(2), AKeyExpr::Col(3)];
        p.agg = vec![AAgg::CountStar];
        p.order = Some(AOrderSpec { by: AOrderBy::CountDesc, limit: Some(100), offset: 0 });
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        let ast = SqlQuery {
            select: vec![
                (SelExpr::Expr(SqlExpr::Col(2)), None),
                (SelExpr::Expr(SqlExpr::Col(3)), None),
                sel_agg(SqlAgg::CountStar),
            ],
            preds: Vec::new(),
            group: vec![SqlExpr::Col(2), SqlExpr::Col(3)],
            order: vec![(OKey::AggRef(2), true)],
            limit: Some(100),
            offset: 0,
            having_count_gt: None,
        };
        for _ in 0..reps {
            assert_identity(engine, &node, &ast, "fp_int_gid");
        }
    }
    // F4: int_gid_filtered_fp (GROUP BY g, s COUNT(*) WHERE s <> '').
    {
        let mut p = ap(24, AFamily::HashPlaneOwnedGroup);
        p.cols = vec![2, 3];
        p.pred = Some(sqe::planner::APred::NeEmpty { col: 3, fp: None });
        p.group = vec![AKeyExpr::Col(2), AKeyExpr::Col(3)];
        p.agg = vec![AAgg::CountStar];
        p.order = Some(AOrderSpec { by: AOrderBy::CountDesc, limit: Some(100), offset: 0 });
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        assert!(
            node.params.flags & sqe::ir::F_DROP_EMPTY_KEY != 0,
            "s <> '' on a group col must lower to F_DROP_EMPTY_KEY"
        );
        let ast = SqlQuery {
            select: vec![
                (SelExpr::Expr(SqlExpr::Col(2)), None),
                (SelExpr::Expr(SqlExpr::Col(3)), None),
                sel_agg(SqlAgg::CountStar),
            ],
            preds: vec![SqlPred::Cmp { col: 3, op: COp::Ne, val: Lit::Str(String::new()) }],
            group: vec![SqlExpr::Col(2), SqlExpr::Col(3)],
            order: vec![(OKey::AggRef(2), true)],
            limit: Some(100),
            offset: 0,
            having_count_gt: None,
        };
        for _ in 0..reps {
            assert_identity(engine, &node, &ast, "fp_int_gid_filtered");
        }
    }
}

#[test]
fn fpcombine_engine_vs_oracle_identity() {
    let dir = std::env::temp_dir().join(format!("sqe_synth_fp_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_fp_bank(&dir);
    {
        let engine = fp_engine(&dir, false);
        assert!(engine.bank.parts.len() > 1, "want a multi-part bank");
        assert!(
            (0..engine.bank.parts.len()).any(|pi| sqe::scan::is_dict(&engine.bank, pi, 3)),
            "fp arms need dict parts on the group column"
        );
        fp_cases(&engine, 1);
    }
    {
        let engine = fp_engine(&dir, true);
        fp_cases(&engine, 3);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// The explicit fpcombine=false/fpcache=false arm.
#[test]
fn nonfp_arm_engine_vs_oracle_identity() {
    let dir = std::env::temp_dir().join(format!("sqe_synth_nonfp_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_fp_bank(&dir);
    let engine = fp_engine_arm(&dir, false, false);
    assert!(!engine.faces.cfg.fpcombine && !engine.faces.cfg.fpcache, "explicit opt-out arm");
    fp_cases(&engine, 2);
    let _ = std::fs::remove_dir_all(&dir);
}

/// [fpcache] The three entry-grain identity folds riding the cached fp
/// plane (text128, text_set, filtered_text_set): cached and uncached arms
/// must render byte-identical answers, every execution oracle-gated.
fn fold_shapes(engine: &Engine) -> Vec<(&'static str, PlanNode, SqlQuery)> {
    use sqe::rig::lower::{COp, Lit, SqlPred};
    let mut out = Vec::new();
    // T1 text128: GROUP BY s COUNT(*) (single varlena key => [0] shape).
    {
        let mut p = ap(30, AFamily::HashPlaneOwnedGroup);
        p.cols = vec![3];
        p.group = vec![AKeyExpr::Col(3)];
        p.agg = vec![AAgg::CountStar];
        p.order = Some(AOrderSpec { by: AOrderBy::CountDesc, limit: Some(100), offset: 0 });
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        let ast = SqlQuery {
            select: vec![(SelExpr::Expr(SqlExpr::Col(3)), None), sel_agg(SqlAgg::CountStar)],
            preds: Vec::new(),
            group: vec![SqlExpr::Col(3)],
            order: vec![(OKey::AggRef(1), true)],
            limit: Some(100),
            offset: 0,
            having_count_gt: None,
        };
        out.push(("fold_text128", node, ast));
    }
    // T2 text_set: COUNT(DISTINCT s), no group (the q5 shape).
    {
        let mut p = ap(31, AFamily::DistinctPipeline);
        p.cols = vec![3];
        p.agg = vec![AAgg::CountDistinct { e: AValExpr::Col(3) }];
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        let ast = SqlQuery {
            select: vec![sel_agg(SqlAgg::CountDistinct(SqlExpr::Col(3)))],
            preds: Vec::new(),
            group: Vec::new(),
            order: Vec::new(),
            limit: None,
            offset: 0,
            having_count_gt: None,
        };
        out.push(("fold_text_set", node, ast));
    }
    // T3 filtered_text_set: COUNT(DISTINCT s) WHERE g BETWEEN 1 AND 3.
    {
        let mut p = ap(32, AFamily::DistinctPipeline);
        p.cols = vec![3, 2];
        p.pred = Some(sqe::planner::APred::Range {
            col: 2,
            lo: "1".into(),
            hi: "3".into(),
            fp: None,
        });
        p.agg = vec![AAgg::CountDistinct { e: AValExpr::Col(3) }];
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        let ast = SqlQuery {
            select: vec![sel_agg(SqlAgg::CountDistinct(SqlExpr::Col(3)))],
            preds: vec![
                SqlPred::Cmp { col: 2, op: COp::Ge, val: Lit::Num("1".into()) },
                SqlPred::Cmp { col: 2, op: COp::Le, val: Lit::Num("3".into()) },
            ],
            group: Vec::new(),
            order: Vec::new(),
            limit: None,
            offset: 0,
            having_count_gt: None,
        };
        out.push(("fold_filtered_text_set", node, ast));
    }
    out
}

#[test]
fn fpcache_identity_folds_cached_vs_uncached() {
    let dir = std::env::temp_dir().join(format!("sqe_synth_fpfold_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_fp_bank(&dir);
    // Uncached arm (fpcache=false): build_fps every run — the baseline
    // rendering AND an oracle identity gate.
    let baseline: Vec<(String, Vec<String>)> = {
        let engine = fp_engine(&dir, false);
        assert!(
            (0..engine.bank.parts.len()).any(|pi| sqe::scan::is_dict(&engine.bank, pi, 3)),
            "identity folds need dict parts on the text column"
        );
        fold_shapes(&engine)
            .into_iter()
            .map(|(what, node, ast)| {
                let (answer, _, _) = sqe::rig::run_arms(&engine, &node);
                let want = run_oracle(&engine.bank, &ast);
                let lines = to_lines(&answer);
                assert_eq!(to_lines(&want), lines, "{what}: oracle vs engine (uncached)");
                (what.to_string(), lines)
            })
            .collect()
    };
    // Cached arm (fpcache=true): three executions per shape = compute (no
    // store) / compute + store / replay — each must match the ORACLE and
    // the UNCACHED rendering exactly.
    {
        let engine = fp_engine(&dir, true);
        for (i, (what, node, ast)) in fold_shapes(&engine).into_iter().enumerate() {
            let want = to_lines(&run_oracle(&engine.bank, &ast));
            assert_eq!(want, baseline[i].1, "{what}: baseline bookkeeping");
            for exec in ["compute", "store", "replay"] {
                let (answer, _, _) = sqe::rig::run_arms(&engine, &node);
                assert_eq!(
                    want,
                    to_lines(&answer),
                    "{what}: cached ({exec}) vs uncached/oracle"
                );
            }
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// SIGNED-flag law: word class from the SEALED bank, not TypMeta.
#[test]
fn unsigned_word_class_reconciles_from_the_sealed_bank() {
    let dir = format!(
        "{}/sqe-unsigned-{}",
        std::env::temp_dir().display(),
        std::process::id()
    );
    std::fs::create_dir_all(&dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let schema = vec![ColSchema {
        attno: 1,
        class: StorageClass::ByvalWord { width: 4, signed: false },
        typlen: 4,
        typbyval: true,
        typalign: b'i',
        collation_class: CollationClass::C,
        semantics: TypeSemantics::UnsignedInt,
    }];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        5,
        778,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 4096, max_bytes: u64::MAX, cut_granule_rows: 1024 },
    )
    .expect("open writer");
    for v in [1u64, 0x8000_0001, 7] {
        let datums = [RawDatum::Word(v)];
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
    w.publish(&mut vfs, &Probe::new(TxnVerdict::Committed)).expect("publish");

    let cols = vec![ColMeta::new(1, "o", TypMeta::INT4)];
    assert!(cols[0].class.signed(), "placeholder derives signed from int4 (precondition)");
    let bank = Bank::open(&dir, cols, &OpenOpts { bankstats: false, threads: 1 });
    assert_eq!(
        bank.schema[0].class,
        StorageClass::ByvalWord { width: 4, signed: false },
        "the engine's class must be the writer's, not TypMeta-derived"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// [P1-1 gate-close] Family-election table lock (the PLANDIFF alignment):
/// the SQL/plan-tree election must elect what the RON plans of record
/// elect for the four divergence shapes (q9, q30/q31, q38) without
/// disturbing the neighbors (q8-class, q12/q36-class).
#[test]
fn family_election_matches_the_ron_table() {
    use sqe::family::{elect_family, ShapeFacts};
    use sqe::planner::AFamily::*;
    let dir = std::env::temp_dir().join(format!("sqe_synth_fam_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);
    let engine = open_engine(&dir);
    let (bank, faces) = (&engine.bank, &engine.faces);
    let base = ShapeFacts { n_aggs: 1, aggs_all_count_star: true, ..ShapeFacts::default() };
    // q9 shape: int group key, agg MIX carrying one COUNT(DISTINCT) ->
    // hash plane (the RON election), never the distinct pipeline.
    let q9 = ShapeFacts {
        group: vec![1],
        has_count_distinct: true,
        n_count_distinct: 1,
        n_aggs: 4,
        aggs_all_count_star: false,
        ..base.clone()
    };
    assert_eq!(elect_family(bank, faces, &q9, "t").unwrap(), HashPlaneOwnedGroup);
    // q8/q10/q13 shape: the WHOLE agg set is COUNT(DISTINCT) -> distinct
    // pipeline.
    let q8 = ShapeFacts {
        group: vec![1],
        has_count_distinct: true,
        n_count_distinct: 1,
        n_aggs: 1,
        aggs_all_count_star: false,
        ..base.clone()
    };
    assert_eq!(elect_family(bank, faces, &q8, "t").unwrap(), DistinctPipeline);
    // q30/q31 shape: int keys + `<> ''` residue on a NON-key varlena
    // column -> survivor gather (carries the ne-empty goal fingerprint).
    let q30 = ShapeFacts {
        group: vec![1, 2],
        ne_empty_cols: vec![3],
        n_aggs: 3,
        aggs_all_count_star: false,
        npreds: 1,
        ..base.clone()
    };
    assert_eq!(elect_family(bank, faces, &q30, "t").unwrap(), SurvivorGather);
    // q36/q37/q12 shape: text group key filtered `<> ''` -> two-level.
    let q36 = ShapeFacts {
        group: vec![3],
        ne_empty_cols: vec![3],
        frame_pair: true,
        npreds: 3,
        ..base.clone()
    };
    assert_eq!(elect_family(bank, faces, &q36, "t").unwrap(), TwoLevelCodeAgg);
    // q38 shape: text key, frame pair, residues, NO key filter -> the
    // window-replay frame lane (the RON election), never two-level.
    let q38 = ShapeFacts { group: vec![3], frame_pair: true, npreds: 5, ..base.clone() };
    assert_eq!(elect_family(bank, faces, &q38, "t").unwrap(), WindowReplay);
    // q40/q41 shape: int keys + frame pair -> window-replay frame lane.
    let q40 = ShapeFacts { group: vec![1, 2], frame_pair: true, npreds: 4, ..base.clone() };
    assert_eq!(elect_family(bank, faces, &q40, "t").unwrap(), WindowReplay);
    // q32-class: unfiltered text group-by -> hash plane.
    let q32 = ShapeFacts { group: vec![3], ..base };
    assert_eq!(elect_family(bank, faces, &q32, "t").unwrap(), HashPlaneOwnedGroup);
    let _ = std::fs::remove_dir_all(&dir);
}

// [oracle-hardening] gate-(b) oracle-competence regressions.

/// Marker key 777777 exists ONLY in the tail part (q19 row-miss class).
fn seal_tail_bank(dir: &str) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let schema = vec![wcol(1, 8, 8), wcol(2, 4, 4), tcol(3)];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        5,
        783,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 4096, max_bytes: u64::MAX, cut_granule_rows: 1024 },
    )
    .expect("open writer");
    for i in 0..ROWS {
        let (mut k, g, s) = row(i);
        if i >= ROWS - 50 {
            k = 777_777; // tail part only (last 50 of 20_000 rows)
        }
        let img = img_4b_u(s.as_bytes());
        let datums = [
            RawDatum::Word(k as u64),
            RawDatum::Word(g as u64),
            RawDatum::Bytes(&img),
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

fn sel_col(c: u32) -> (SelExpr, Option<String>) {
    (SelExpr::Expr(SqlExpr::Col(c)), None)
}

#[test]
fn oracle_rowfetch_finds_tail_part_only_key() {
    use sqe::rig::lower::{COp, Lit, SqlPred};
    use sqe::rig::oracle::run_oracle_full;
    let dir = std::env::temp_dir().join(format!("sqe_synth_tail_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_tail_bank(&dir);
    let engine = open_engine(&dir);
    assert!(engine.bank.parts.len() > 1, "want a multi-part bank");
    let ast = SqlQuery {
        select: vec![sel_col(1)],
        preds: vec![SqlPred::Cmp {
            col: 1,
            op: COp::Eq,
            val: Lit::Num("777777".into()),
        }],
        group: Vec::new(),
        order: Vec::new(),
        limit: None,
        offset: 0,
        having_count_gt: None,
    };
    let out = run_oracle_full(&engine.bank, &ast);
    assert_eq!(out.matches, 50, "tail-part rows must be scanned");
    assert_eq!(out.ans.nrows, 50);
    assert_eq!(to_lines(&out.ans), vec!["777777".to_string(); 50]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn oracle_zero_column_scan_counts_from_bank_structure() {
    use sqe::rig::oracle::run_oracle_full;
    let dir = std::env::temp_dir().join(format!("sqe_synth_q0_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);
    let engine = open_engine(&dir);
    let ast = SqlQuery {
        select: vec![sel_agg(SqlAgg::CountStar)],
        preds: Vec::new(),
        group: Vec::new(),
        order: Vec::new(),
        limit: None,
        offset: 0,
        having_count_gt: None,
    };
    let out = run_oracle_full(&engine.bank, &ast);
    assert_eq!(to_lines(&out.ans), vec![format!("{ROWS}")]);
    assert_eq!(out.matches, ROWS);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn oracle_text_min_max_memcmp() {
    use sqe::rig::oracle::run_oracle;
    let dir = std::env::temp_dir().join(format!("sqe_synth_tmm_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);
    let engine = open_engine(&dir);
    let ast = SqlQuery {
        select: vec![
            sel_agg(SqlAgg::Min(SqlExpr::Col(3))),
            sel_agg(SqlAgg::Max(SqlExpr::Col(3))),
            sel_agg(SqlAgg::CountStar),
        ],
        preds: Vec::new(),
        group: Vec::new(),
        order: Vec::new(),
        limit: None,
        offset: 0,
        having_count_gt: None,
    };
    let want = run_oracle(&engine.bank, &ast);
    assert_eq!(to_lines(&want), vec![format!("s00\ts22\t{ROWS}")]);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Grouped sort law: ORDER BY the ACTUAL aggregate.
#[test]
fn oracle_grouped_sort_by_actual_aggregate() {
    use sqe::rig::lower::OKey;
    use sqe::rig::oracle::run_oracle_full;
    let dir = std::env::temp_dir().join(format!("sqe_synth_sort_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_fp_bank(&dir);
    let engine = fp_engine(&dir, false);
    let mk = |agg: SqlAgg| SqlQuery {
        select: vec![sel_col(3), (SelExpr::Agg(agg), Some("u".into()))],
        preds: Vec::new(),
        group: vec![SqlExpr::Col(3)],
        order: vec![(OKey::AggRef(1), true)],
        limit: Some(5),
        offset: 0,
        having_count_gt: None,
    };
    let out = run_oracle_full(&engine.bank, &mk(SqlAgg::CountDistinct(SqlExpr::Col(1))));
    let vals: Vec<i64> = to_lines(&out.ans)
        .iter()
        .map(|l| l.rsplit('\t').next().unwrap().parse().unwrap())
        .collect();
    assert_eq!(vals, vec![25, 24, 23, 22, 21], "sort key must be COUNT(DISTINCT)");
    let out = run_oracle_full(&engine.bank, &mk(SqlAgg::Sum(SqlExpr::Col(1))));
    let sums: Vec<i128> = to_lines(&out.ans)
        .iter()
        .map(|l| l.rsplit('\t').next().unwrap().parse().unwrap())
        .collect();
    assert!(sums.windows(2).all(|w| w[0] > w[1]), "SUM order non-monotonic: {sums:?}");
    let out = run_oracle_full(&engine.bank, &mk(SqlAgg::Avg(SqlExpr::Col(1))));
    let avgs: Vec<f64> = to_lines(&out.ans)
        .iter()
        .map(|l| l.rsplit('\t').next().unwrap().parse().unwrap())
        .collect();
    assert!(avgs.windows(2).all(|w| w[0] > w[1]), "AVG order non-monotonic: {avgs:?}");
    let mut q = mk(SqlAgg::CountStar);
    q.having_count_gt = Some(500);
    let out = run_oracle_full(&engine.bank, &q);
    assert_eq!(out.groups_total, Some(13));
    assert_eq!(out.groups_prehaving, Some(25));
    let _ = std::fs::remove_dir_all(&dir);
}

/// The rig's canonicalization laws (set / membership / proj / footer).
#[test]
fn canonicalization_laws() {
    use sqe::rig::lower::OKey;
    use sqe::rig::oracle::run_oracle_full;
    use sqe::rig::oracle_check;
    let dir = std::env::temp_dir().join(format!("sqe_synth_laws_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);
    let engine = open_engine(&dir);

    // (a) SET law: 16 equal-count groups, one tie tier cut by LIMIT 5.
    let mut p = ap(30, AFamily::HashPlaneOwnedGroup);
    p.cols = vec![2];
    p.group = vec![AKeyExpr::Col(2)];
    p.agg = vec![AAgg::CountStar];
    p.order = Some(AOrderSpec { by: AOrderBy::CountDesc, limit: Some(5), offset: 0 });
    let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
    let (answer, _, _) = sqe::rig::run_arms(&engine, &node);
    let ast = SqlQuery {
        select: vec![sel_col(2), sel_agg(SqlAgg::CountStar)],
        preds: Vec::new(),
        group: vec![SqlExpr::Col(2)],
        order: vec![(OKey::AggRef(1), true)],
        limit: Some(5),
        offset: 0,
        having_count_gt: None,
    };
    let out = run_oracle_full(&engine.bank, &ast);
    let v = oracle_check(&ast, &out, &answer);
    assert!(v.ok, "set law failed: {:?} {:?}", v.law, v.detail);
    assert_eq!(v.law, "set", "a full tie tier under LIMIT must elect the set law");

    // (b) MEMBERSHIP law: LIMIT without ORDER BY.
    let ast_m = SqlQuery {
        select: vec![sel_col(2), sel_agg(SqlAgg::CountStar)],
        preds: Vec::new(),
        group: vec![SqlExpr::Col(2)],
        order: Vec::new(),
        limit: Some(5),
        offset: 0,
        having_count_gt: None,
    };
    let out_m = run_oracle_full(&engine.bank, &ast_m);
    let full = out_m.full.as_ref().unwrap();
    let mut sim = full.clone();
    let keep: Vec<usize> = (full.nrows - 5..full.nrows).collect();
    for col in sim.cols.iter_mut() {
        if let sqe::answer::ColData::I64(v) = &mut col.data {
            *v = keep.iter().map(|&i| v[i]).collect();
        }
    }
    sim.nrows = 5;
    let v = oracle_check(&ast_m, &out_m, &sim);
    assert!(v.ok, "membership law failed: {:?} {:?}", v.law, v.detail);
    assert_eq!(v.law, "membership");
    let mut bad = sim.clone();
    if let sqe::answer::ColData::I64(v) = &mut bad.cols[1].data {
        v[0] += 1; // a count that exists nowhere
    }
    let v = oracle_check(&ast_m, &out_m, &bad);
    assert!(!v.ok, "fabricated member must fail the membership law");

    // (c) PROJECTION-SUBSET law + row-fetch footer verification.
    use sqe::rig::lower::{COp, Lit, SqlPred};
    let ast_p = SqlQuery {
        select: vec![sel_col(1)],
        preds: vec![SqlPred::Cmp { col: 2, op: COp::Eq, val: Lit::Num("3".into()) }],
        group: Vec::new(),
        order: Vec::new(),
        limit: None,
        offset: 0,
        having_count_gt: None,
    };
    let out_p = run_oracle_full(&engine.bank, &ast_p);
    let mut sim = out_p.ans.clone();
    sim.cols.insert(
        0,
        sqe::answer::AnswerCol::i64s(TypMeta::INT8, vec![0; sim.nrows]),
    );
    sim.note = Some(sqe::render::footer_rows(out_p.matches));
    let v = oracle_check(&ast_p, &out_p, &sim);
    assert!(v.ok, "proj law failed: {:?} {:?}", v.law, v.detail);
    assert_eq!(v.law, "proj+byte");
    assert_eq!(v.note, "rows");
    let mut bad = sim.clone();
    bad.note = Some(sqe::render::footer_rows(out_p.matches + 1));
    assert!(!oracle_check(&ast_p, &out_p, &bad).ok, "wrong footer must fail");

    // (d) grouped footer witness
    let mut sim = out_m.ans.clone();
    sim.note = Some(sqe::render::footer_groups(
        out_m.groups_total.unwrap(),
        out_m.rows_folded.unwrap(),
    ));
    let v = oracle_check(&ast_m, &out_m, &sim);
    assert!(v.ok, "grouped footer failed: {:?} {:?}", v.law, v.detail);
    assert_eq!(v.note, "groups");

    let _ = std::fs::remove_dir_all(&dir);
}
