//! [oracle, ruling Q4 2026-08-18] Structural verification of
//! fingerprint-keyed cache hits — the forged-encoder-omission gate.
//!
//! Production keeps hash-as-identity (the ConjFp/Fingerprint key);
//! oracle/CI builds store the canonical structural key alongside every
//! fingerprint-keyed cache entry and compare it on every hit. This test
//! FORGES an encoder omission — two semantically different predicates
//! deliberately minted to the same fingerprint (the test hook: the term
//! constant is changed while the term's fp field is left stale, exactly
//! what an fp encoder that omitted the constant would produce) — and
//! asserts the hit-path verification panics loudly. It also proves the
//! genuine replay path stays panic-free (no false positives).

#![cfg(all(feature = "rig", feature = "oracle"))]

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::answer::ColData;
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::engine::{Engine, PopulatePolicy, SqeConfig};
use sqe::planner::{plan_from_ap, AAgg, AFamily, APlan, APred, AValExpr};
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

fn row(i: u64) -> (i64, i64) {
    let k = ((i.wrapping_mul(0x9E37_79B9_7F4A_7C15)) >> 17) as i64 % 1000;
    let g = (i % 16) as i64;
    (k.abs(), g)
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
        779,
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
}

fn open_engine(dir: &str) -> Engine {
    let schema = vec![ColMeta::new(1, "k", TypMeta::INT8), ColMeta::new(2, "g", TypMeta::INT4)];
    let bank = Bank::open(dir, schema, &OpenOpts { bankstats: false, threads: 1 });
    Engine::new(
        bank,
        SqeConfig {
            threads: 2,
            // populate on the FIRST execution so the second run replays
            // (the second-touch rung would need three runs — same paths).
            populate: PopulatePolicy::First,
            ..SqeConfig::default()
        },
    )
}

fn emitted(a: &sqe::answer::AnswerSet) -> Vec<i64> {
    match &a.cols[0].data {
        ColData::I64(v) => v.clone(),
        other => panic!("emit answer must be I64, got {other:?}"),
    }
}

#[test]
fn forged_encoder_omission_panics_on_cache_hit() {
    let dir = std::env::temp_dir().join(format!("sqe_fpverify_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);
    let engine = open_engine(&dir);
    assert_eq!(engine.bank.rows_total(), ROWS);

    // SELECT k WHERE g BETWEEN 3 AND 9 (the EmitMatches shape — the
    // condcache-replaying arm of FusedFilterAgg): the selection IS the
    // answer, so the plan carries a POSITIVE condcache goal (the frame
    // fingerprint) and publishes/replays through the verdict plane.
    let mut p = APlan {
        sortagg_keys: Vec::new(),
        win: None,
        q: 900,
        family: AFamily::FusedFilterAgg,
        tags: Vec::new(),
        cols: vec![2],
        pred: None,
        group: Vec::new(),
        agg: vec![AAgg::Emit { e: AValExpr::Col(2) }],
        order: None,
        agg_filters: Vec::new(),
        having: None,
        params: Vec::new(),
        fingerprints: Vec::new(),
        kernel_oracle: String::new(),
        notes: "fp_verify".into(),
        flags: Vec::new(),
    };
    p.pred = Some(APred::Range { col: 2, lo: "3".into(), hi: "9".into(), fp: None });
    let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
    let pred = node.pred.as_ref().expect("pred");
    let fp = pred.frame_fingerprint();
    assert!(
        node.params.goal.fingerprints.iter().any(|f| *f == fp),
        "test premise: the plan must carry a positive condcache goal"
    );

    // Run 1 publishes (PopulatePolicy::First); run 2 replays. The genuine
    // replay must verify CLEAN — no false positives on honest hits.
    let want = emitted(&engine.run(&node));
    assert!(!want.is_empty(), "test premise: survivors exist");
    assert!(engine.faces.verdicts_available(&fp), "run 1 must publish the plane");
    let replayed = emitted(&engine.run(&node));
    assert_eq!(want, replayed, "genuine replay answers identically");

    // FORGE the encoder omission: a semantically different predicate
    // (hi 9 -> 5) whose fingerprint is deliberately left at the published
    // plane's identity — byte-for-byte what an fp encoder that omitted
    // the range constant would mint.
    let mut forged = node.clone();
    {
        let fpred = forged.pred.as_mut().expect("pred");
        fpred.terms[0].hi = 5; // semantic change...
        // ...and NO fingerprint change (the omission): same ConjFp.
        assert_eq!(fpred.frame_fingerprint(), fp, "forge premise: identical fingerprint");
    }
    let got = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = engine.run(&forged);
    }));
    let err = got.expect_err("oracle verify must panic on the forged hit");
    let msg = err
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| err.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_default();
    assert!(
        msg.contains("fingerprint collision or encoder omission"),
        "panic must name the failure class, got: {msg}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
