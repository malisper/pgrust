//! Grouped text-key identity + NDV-scaling checks: real pgrc2_write banks,
//! server-shaped plans (order None, no engine-side limit — the full grouped
//! answer, sliced by the caller), engine arms vs the scalar oracle.
//!
//! The `ndv_sweep_bench` harness (#[ignore]) seals banks whose varlena key
//! spans a controlled distinct-count ladder and times the grouped shapes
//! against their byval-only twins.

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
use sqe::ir::PlanNode;
use sqe::planner::{plan_from_ap, AAgg, AFamily, AKeyExpr, APlan};
use sqe::render::to_lines;
use sqe::rig::lower::{SelExpr, SqlAgg, SqlExpr, SqlQuery};
use sqe::rig::oracle::run_oracle;
use sqe::typmeta::TypMeta;

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

/// Row content: a near-unique wide int lane (lane NDV ~= rows) plus a
/// varlena key + its byval twin drawn from `text_ndv` distinct values in
/// scattered order.
fn row(i: u64, text_ndv: u64) -> (u64, u64, String) {
    let lane = (i.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 1) as u64;
    let j = (i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 13) % text_ndv;
    (lane, j, format!("phrase_{j:07}"))
}

/// Seal via the real writer + publish path; dict posture on the varlena
/// column (per-part cap — the served banks' shape).
fn seal_grp_bank(dir: &str, rows: u64, text_ndv: u64) {
    std::fs::create_dir_all(dir).expect("mkdir");
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
    for i in 0..rows {
        let (lane, j, s) = row(i, text_ndv);
        let img = img_4b_u(s.as_bytes());
        let datums = [RawDatum::Word(lane), RawDatum::Word(j), RawDatum::Bytes(&img)];
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

fn grp_engine(dir: &str, threads: usize, fpcombine: bool) -> Engine {
    let schema = vec![
        ColMeta::new(1, "lane", TypMeta::INT8),
        ColMeta::new(2, "twin", TypMeta::INT4),
        ColMeta::new(3, "s", TypMeta::TEXT_C),
    ];
    let bank = Bank::open(dir, schema, &OpenOpts { bankstats: false, threads: 1 });
    Engine::new(bank, SqeConfig { threads, fpcombine, ..SqeConfig::default() })
}

/// The server shape: grouped COUNT(*), order None, NO engine-side limit
/// (the caller slices the full answer).
fn full_set_node(engine: &Engine, q: u32, keys: &[u32]) -> PlanNode {
    let ap = APlan {
        sortagg_keys: Vec::new(),
        win: None,
        q,
        family: AFamily::HashPlaneOwnedGroup,
        tags: Vec::new(),
        cols: keys.to_vec(),
        pred: None,
        group: keys.iter().map(|&c| AKeyExpr::Col(c)).collect(),
        agg: vec![AAgg::CountStar],
        order: None,
        agg_filters: Vec::new(),
        having: None,
        params: Vec::new(),
        fingerprints: Vec::new(),
        kernel_oracle: String::new(),
        notes: "txtgrp".into(),
        flags: Vec::new(),
    };
    plan_from_ap(&engine.bank, &engine.faces, &ap).expect("plan")
}

fn sorted_lines(a: &sqe::answer::AnswerSet) -> Vec<String> {
    let mut v = to_lines(a);
    v.sort();
    v
}

/// Full-set identity at high varlena-key NDV (the regression class: every
/// group flows through the selection/collection path, none is limited
/// away). Engine fp and non-fp arms vs the scalar oracle, set-compared.
#[test]
fn high_ndv_text_grouped_full_set_identity() {
    let dir = std::env::temp_dir().join(format!("sqe_txtgrp_id_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    const ROWS: u64 = 60_000;
    const NDV: u64 = 30_000;
    seal_grp_bank(&dir, ROWS, NDV);
    let ast = SqlQuery {
        select: vec![
            (SelExpr::Expr(SqlExpr::Col(1)), None),
            (SelExpr::Expr(SqlExpr::Col(3)), None),
            (SelExpr::Agg(SqlAgg::CountStar), None),
        ],
        preds: Vec::new(),
        group: vec![SqlExpr::Col(1), SqlExpr::Col(3)],
        order: Vec::new(),
        limit: None,
        offset: 0,
        having_count_gt: None,
    };
    let ast_t = SqlQuery {
        select: vec![
            (SelExpr::Expr(SqlExpr::Col(3)), None),
            (SelExpr::Agg(SqlAgg::CountStar), None),
        ],
        preds: Vec::new(),
        group: vec![SqlExpr::Col(3)],
        order: Vec::new(),
        limit: None,
        offset: 0,
        having_count_gt: None,
    };
    for fp in [true, false] {
        let engine = grp_engine(&dir, 3, fp);
        assert!(engine.bank.parts.len() > 1, "want a multi-part bank");
        // int + varlena key, full set.
        let node = full_set_node(&engine, 90, &[1, 3]);
        let a = engine.run(&node);
        let want = run_oracle(&engine.bank, &ast);
        assert_eq!(
            sorted_lines(&want),
            sorted_lines(&a),
            "fp={fp}: full-set (int, text) grouped identity"
        );
        // varlena key alone, full set.
        let node = full_set_node(&engine, 91, &[3]);
        let a = engine.run(&node);
        let want = run_oracle(&engine.bank, &ast_t);
        assert_eq!(
            sorted_lines(&want),
            sorted_lines(&a),
            "fp={fp}: full-set text grouped identity"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// NDV-scaling harness (profiling pushes only):
///   SQE_TXTGRP_BENCH_DIR=<dir> cargo test --release --features rig \
///     --test txtgrp_ndv -- --ignored --nocapture ndv_sweep_bench
/// Seals one bank per NDV point (reused across runs when the dir is kept)
/// and times the grouped shapes: (int, text) fp/non-fp, text alone, and
/// the (int, int) byval twin as control.
#[test]
#[ignore]
fn ndv_sweep_bench() {
    let base = std::env::var("SQE_TXTGRP_BENCH_DIR")
        .unwrap_or_else(|_| std::env::temp_dir().join("sqe_txtgrp_bench").to_str().unwrap().into());
    const ROWS: u64 = 2_000_000;
    let reps = 3usize;
    let threads = sqe::rig::thread_count();
    for &ndv in &[1_000u64, 100_000, 1_000_000] {
        let dir = format!("{base}/ndv{ndv}");
        let marker = format!("{dir}/.sealed");
        if !std::path::Path::new(&marker).exists() {
            let _ = std::fs::remove_dir_all(&dir);
            let t0 = std::time::Instant::now();
            seal_grp_bank(&dir, ROWS, ndv);
            std::fs::write(&marker, b"ok").unwrap();
            println!("SEAL|ndv={ndv}|ms={:.0}", t0.elapsed().as_secs_f64() * 1e3);
        }
        for fp in [true, false] {
            let engine = grp_engine(&dir, threads, fp);
            let dicts = (0..engine.bank.parts.len())
                .filter(|&pi| sqe::scan::is_dict(&engine.bank, pi, 3))
                .count();
            let mut arms: Vec<(&str, PlanNode)> = vec![
                ("int_text", full_set_node(&engine, 92, &[1, 3])),
                ("text_only", full_set_node(&engine, 93, &[3])),
            ];
            if fp {
                arms.push(("int_int", full_set_node(&engine, 94, &[1, 2])));
            }
            for (name, node) in arms {
                let mut best = f64::MAX;
                let mut groups = 0usize;
                for _ in 0..reps {
                    let t0 = std::time::Instant::now();
                    let a = engine.run(&node);
                    best = best.min(t0.elapsed().as_secs_f64() * 1e3);
                    groups = a.cols.first().map(|c| c.data.len()).unwrap_or(0);
                }
                println!(
                    "BENCH|rows={ROWS}|ndv={ndv}|fp={fp}|dicts={dicts}/{}|{name}|groups={groups}|best_ms={best:.1}",
                    engine.bank.parts.len()
                );
            }
        }
    }
}
