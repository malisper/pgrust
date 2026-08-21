//! [sqe-q2426] Pins for the q24-26/q38-41 strict-gate diagnosis:
//!
//!  1. Range-conjunct canonicalization: `c >= lo AND c <= hi` authored as
//!     TWO half-open Range conjuncts (the server lowering's form) must
//!     lower to the SAME predicate identity (frame + full fingerprints)
//!     and the same answer as the single closed Range twin (the rig's
//!     authored form) — the hot-shape condcache planes are shared across
//!     both authoring paths.
//!
//!  2. Warm-arm full-plane recurrence: a frame-lane plan whose shared
//!     FRAME plane is already resident at its first execution (published
//!     by a sibling hot-shape plan) must still converge to full-plane
//!     replay — the warm frame arm publishes the residue-final survivors
//!     it just computed. The regression this pins: the full plane's
//!     recurrence witness only ever bumping on the COLD path, so the
//!     sibling-warmed plan re-evaluates residues + re-decodes keys over
//!     the whole frame on EVERY hot execution (the q38/q40/q41 cells).

#![cfg(feature = "rig")]

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::engine::{Engine, SqeConfig};
use sqe::ir::PlanNode;
use sqe::planner::{plan_from_ap, AAgg, AFamily, AKeyExpr, AOrderBy, AOrderSpec, APlan, APred};
use sqe::render::to_lines;
use sqe::typmeta::TypMeta;

const ROWS: u64 = 30_000;

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

/// Row law: `a` unique; `b` a counter-like eq column (62 in the leading
/// eighth of the file — the eq term erases granules); `c` monotone (the
/// range term erases granules); `s` in {0,1} scattered (a residue that
/// never prunes); `r` a small grouping domain.
fn row(i: u64) -> (i64, i64, i64, i64, i64) {
    let a = 9_000_000_000 + i as i64;
    let b = if i < ROWS / 8 { 62 } else { 100 + (i % 50) as i64 };
    let c = i as i64;
    let s = (i % 3 == 0) as i64;
    let r = (i % 40) as i64;
    (a, b, c, s, r)
}

fn seal_bank(dir: &str) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let schema = vec![wcol(1, 8, 8), wcol(2, 4, 4), wcol(3, 4, 4), wcol(4, 2, 2), wcol(5, 4, 4)];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        5,
        791,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 8192, max_bytes: u64::MAX, cut_granule_rows: 1024 },
    )
    .expect("open writer");
    for i in 0..ROWS {
        let (a, b, c, s, r) = row(i);
        let datums = [
            RawDatum::Word(a as u64),
            RawDatum::Word(b as u64),
            RawDatum::Word(c as u64),
            RawDatum::Word(s as u64),
            RawDatum::Word(r as u64),
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
        ColMeta::new(1, "a", TypMeta::INT8),
        ColMeta::new(2, "b", TypMeta::INT4),
        ColMeta::new(3, "c", TypMeta::INT4),
        ColMeta::new(4, "s", TypMeta::INT2),
        ColMeta::new(5, "r", TypMeta::INT4),
    ];
    let bank = Bank::open(dir, schema, &OpenOpts { bankstats: true, threads: 0 });
    Engine::new(bank, SqeConfig { threads: 2, ..SqeConfig::default() })
}

/// A frame-lane grouped plan: (b = 62, c in range) frame + `s = 0`
/// residue, grouped by `r`, COUNT(*) DESC LIMIT 10.
fn frame_node(engine: &Engine, pred: APred) -> PlanNode {
    let p = APlan {
        agg_filters: Vec::new(),
        sortagg_keys: Vec::new(),
        win: None,
        q: 0,
        family: AFamily::WindowReplay,
        tags: Vec::new(),
        cols: vec![5, 2, 3, 4],
        pred: Some(pred),
        group: vec![AKeyExpr::Col(5)],
        agg: vec![AAgg::CountStar],
        order: Some(AOrderSpec { by: AOrderBy::CountDesc, limit: Some(10), offset: 0 }),
        having: None,
        params: Vec::new(),
        fingerprints: Vec::new(),
        kernel_oracle: String::new(),
        notes: "q2426-frame".into(),
        flags: Vec::new(),
    };
    plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan")
}

fn and(legs: Vec<APred>) -> APred {
    APred::And { legs }
}

fn rng(col: u32, lo: i64, hi: i64) -> APred {
    APred::Range { col, lo: lo.to_string(), hi: hi.to_string(), fp: None }
}

fn eq(col: u32, v: i64) -> APred {
    APred::Eq { col, val: v.to_string(), fp: None }
}

#[test]
fn split_range_conjuncts_share_the_closed_range_identity() {
    let dir = std::env::temp_dir().join(format!("sqe_q2426_fp_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);
    let engine = open_engine(&dir);

    let (lo, hi) = (100i64, 2000i64);
    // The server lowering's form: two half-open ranges.
    let split = frame_node(
        &engine,
        and(vec![
            eq(2, 62),
            rng(3, lo, i64::MAX),
            rng(3, i64::MIN, hi),
            APred::EqZero { col: 4, fp: None },
        ]),
    );
    // The rig's authored form: one closed range.
    let closed = frame_node(
        &engine,
        and(vec![eq(2, 62), rng(3, lo, hi), APred::EqZero { col: 4, fp: None }]),
    );

    let sp = split.pred.as_ref().expect("pred");
    let cp = closed.pred.as_ref().expect("pred");
    assert_eq!(
        sp.frame_fingerprint(),
        cp.frame_fingerprint(),
        "split ranges must mint the closed-range FRAME identity"
    );
    assert_eq!(
        sp.full_fingerprint(),
        cp.full_fingerprint(),
        "split ranges must mint the closed-range FULL identity"
    );
    assert_eq!(
        to_lines(&engine.run(&split)),
        to_lines(&engine.run(&closed)),
        "byte-identical answers across the two authoring forms"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn warm_frame_arm_publishes_the_full_plane() {
    let dir = std::env::temp_dir().join(format!("sqe_q2426_warm_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);
    let engine = open_engine(&dir);

    let (lo, hi) = (100i64, 2000i64);
    let frame = vec![eq(2, 62), rng(3, lo, hi)];
    // Sibling hot-shape plan (different residue): its two executions
    // publish the SHARED FRAME plane (compute-first, populate-on-second).
    let mut sib = frame.clone();
    sib.push(APred::NeZero { col: 4, fp: None });
    let sibling = frame_node(&engine, and(sib));
    let a_sib1 = engine.run(&sibling);
    let a_sib2 = engine.run(&sibling);
    assert_eq!(to_lines(&a_sib1), to_lines(&a_sib2), "sibling stable");
    let frame_fp = sibling.pred.as_ref().unwrap().frame_fingerprint();
    assert!(
        engine.faces.cond_get(&frame_fp).is_some(),
        "the shared frame plane is resident after the sibling's reps"
    );

    // The subject plan first executes WITH the frame already resident:
    // every later execution rides the warm frame arm — which must still
    // converge to full-plane replay by publishing from the warm arm.
    let mut subj = frame.clone();
    subj.push(APred::EqZero { col: 4, fp: None });
    let node = frame_node(&engine, and(subj));
    let full_fp = node.pred.as_ref().unwrap().full_fingerprint();
    assert_ne!(full_fp, frame_fp, "residue present: full != frame");

    let a1 = engine.run(&node); // cold (full-plane touch 1)
    let a2 = engine.run(&node); // warm frame arm -> publishes full plane
    assert!(
        engine.faces.cond_get(&full_fp).is_some(),
        "the warm frame arm must publish the residue-final plane \
         (without it, a sibling-warmed plan re-evaluates residues forever)"
    );
    let a3 = engine.run(&node); // full-plane replay
    assert_eq!(to_lines(&a1), to_lines(&a2), "cold == warm-frame");
    assert_eq!(to_lines(&a1), to_lines(&a3), "cold == full-replay");

    let _ = std::fs::remove_dir_all(&dir);
}
