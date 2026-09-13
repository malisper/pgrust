//! AVG over a signed column whose group sums net negative, next to a
//! distinct leg (the dense_distinct LineSink route): the wrapping u64 sum
//! lane must be reinterpreted as i64 before widening, or a negative sum
//! renders as 2^64 + sum.

#![cfg(feature = "rig")]

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::engine::{Engine, SqeConfig};
use sqe::planner::{plan_from_ap, AAgg, AFamily, AKeyExpr, AOrderBy, AOrderSpec, APlan, AValExpr};
use sqe::render::to_lines;
use sqe::rig::lower::{OKey, SelExpr, SqlAgg, SqlExpr, SqlQuery};
use sqe::rig::oracle::run_oracle;
use sqe::typmeta::TypMeta;

const ROWS: u64 = 6_000;

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

fn row(i: u64) -> (i64, i64) {
    let g = (i % 4) as i64;
    let k = match i % 4 {
        0 => -101_196,
        1 => -5 - (i % 3) as i64,
        2 => 42,
        _ => 7 - (i % 11) as i64,
    };
    (k, g)
}

fn seal_bank(dir: &str) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let schema = vec![wcol(1, 8, 8), wcol(2, 4, 4)];
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
    for i in 0..ROWS {
        let (k, g) = row(i);
        let datums = [RawDatum::Word(k as u64), RawDatum::Word(g as u64)];
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
        notes: "avg_signed_sum".into(),
        flags: Vec::new(),
    }
}

fn sel_agg(a: SqlAgg) -> (SelExpr, Option<String>) {
    (SelExpr::Agg(a), None)
}

#[test]
fn avg_with_distinct_leg_renders_negative_group_sums() {
    let dir = std::env::temp_dir().join(format!("sqe_avg_signed_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);
    let schema = vec![ColMeta::new(1, "k", TypMeta::INT8), ColMeta::new(2, "g", TypMeta::INT4)];
    let bank = Bank::open(&dir, schema, &OpenOpts { bankstats: false, threads: 1 });
    let engine = Engine::new(bank, SqeConfig { threads: 2, ..SqeConfig::default() });
    assert_eq!(engine.bank.rows_total(), ROWS);

    let mut p = ap(0, AFamily::HashPlaneOwnedGroup);
    p.cols = vec![2, 1];
    p.group = vec![AKeyExpr::Col(2)];
    p.agg = vec![
        AAgg::CountStar,
        AAgg::Avg { e: AValExpr::Col(1) },
        AAgg::CountDistinct { e: AValExpr::Col(1) },
    ];
    p.order = Some(AOrderSpec { by: AOrderBy::CountDesc, limit: Some(10), offset: 0 });
    let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
    let ast = SqlQuery {
        select: vec![
            (SelExpr::Expr(SqlExpr::Col(2)), None),
            sel_agg(SqlAgg::CountStar),
            sel_agg(SqlAgg::Avg(SqlExpr::Col(1))),
            sel_agg(SqlAgg::CountDistinct(SqlExpr::Col(1))),
        ],
        preds: Vec::new(),
        group: vec![SqlExpr::Col(2)],
        order: vec![(OKey::AggRef(1), true)],
        limit: Some(10),
        offset: 0,
        having_count_gt: None,
    };
    let (answer, _, _) = sqe::rig::run_arms(&engine, &node);
    let want = run_oracle(&engine.bank, &ast);
    let got = to_lines(&answer);
    assert_eq!(to_lines(&want), got, "avg_signed_sum: oracle vs engine (rendered)");
    assert!(got.iter().any(|l| l.contains("\t-101196.")), "negative avg rendered: {got:?}");
    let _ = std::fs::remove_dir_all(&dir);
}
