//! [q28-serve] DerivedKeyFold k-bounded serving law — the engine half of
//! the seam's q28 admission (clickbench-serving-closure.md charter memo):
//!
//!   RED  — an UNWITNESSED derived-key grouped shape (verbatim text
//!          parts: no dict, no key-count witness) refuses
//!          `GroupCountUnwitnessed { "no-witness" }` without a pushed
//!          bound (the r14 refusal world, pinned verbatim);
//!   GREEN — the same node with a non-native `topk` (n <= GROUP_ROW_CAP)
//!          admits through the k_bounded exception, and execution under
//!          the fused `having_min_count` law answers EXACTLY the oracle:
//!          HAVING filters BEFORE the bound (survivors-only selection),
//!          the AVG order key compares by i128 cross-multiplication
//!          (never a rendered f64), boundary ties break by canonical
//!          key bytes ASC — the rig oracle's tie law.
//!
//! The generalized `params.having` comparator is NOT implemented by the
//! stencil; admission must refuse it fail-closed (also pinned).

#![cfg(feature = "rig")]

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{img_4b_u, Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::engine::{Engine, SqeConfig};
use sqe::ir::{PlanNode, TopK, TopKKey};
use sqe::kernels::referer_key;
use sqe::planner::{
    check_server_grouped, plan_from_ap, AAgg, AFamily, AKeyExpr, APlan, APred, AValExpr,
};
use sqe::refuse::Refuse;
use sqe::render::to_lines;
use sqe::typmeta::TypMeta;
use std::collections::HashMap;

const ROWS: u64 = 40_000;
const K: usize = 5;
/// Strict `COUNT(*) > HMIN` (the stencil's `having_min_count` law).
const HMIN: u64 = 500;

/// Per-host path pad: strictly separates the AVG order, EXCEPT the two
/// engineered exact-ratio ties — {h18,h19} (fully inside the top-K
/// window) and {h14,h15} (crossing the K=5 boundary: the canonical-key
/// ASC tie-break must keep h14 and cut h15).
fn pad(j: u64) -> usize {
    match j {
        19 => 36, // == pad(18)
        15 => 28, // == pad(14)
        j => (j as usize) * 2,
    }
}

/// Row law (m = i % 40):
///   m == 0        -> ''            (dropped by the NeEmpty residue)
///   m in 1..=20   -> host j=m-1    (~1000 rows each: HAVING survivors)
///   m == 21, i<200-> decoy host    (5 rows, LONGEST avg: HAVING must
///                                   filter it BEFORE the top-K bound)
///   otherwise     -> no-match text (unique -> singleton groups; the
///                                   derivation returns the input)
fn row_text(i: u64) -> String {
    match i % 40 {
        0 => String::new(),
        m @ 1..=20 => {
            let j = m - 1;
            format!("https://www.h{:02}.example/{}{:06}", j, "x".repeat(pad(j)), i)
        }
        21 if i < 200 => format!("https://www.zz-decoy.example/{}{:06}", "y".repeat(300), i),
        _ => format!("nomatch-{i:07}"),
    }
}

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

/// Seal via the real writer WITHOUT any dict posture: every part is
/// verbatim varlena, so `Witness::key_count` has no witness — exactly
/// the 100m-hits Referer admission regime the charter names.
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
        797,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 4096, max_bytes: u64::MAX, cut_granule_rows: 1024 },
    )
    .expect("open writer");
    for i in 0..ROWS {
        let s = row_text(i);
        let img = img_4b_u(s.as_bytes());
        let datums = [RawDatum::Word(i), RawDatum::Word(i % 7), RawDatum::Bytes(&img)];
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
        ColMeta::new(3, "r", TypMeta::TEXT_C),
    ];
    let bank = Bank::open(dir, schema, &OpenOpts { bankstats: false, threads: 1 });
    Engine::new(bank, SqeConfig { threads: 4, ..SqeConfig::default() })
}

/// The server-lowered q28 node shape: order None (the shell owns final
/// order), full limit, aggs AVG(octet_length) / COUNT(*) / MIN(bytes).
fn q28_node(engine: &Engine) -> PlanNode {
    let ap = APlan {
        sortagg_keys: Vec::new(),
        win: None,
        q: 28,
        family: AFamily::DerivedKeyFold,
        tags: Vec::new(),
        cols: vec![3],
        pred: Some(APred::NeEmpty { col: 3, fp: None }),
        group: vec![AKeyExpr::HostRegex { col: 3 }],
        agg: vec![
            AAgg::Avg { e: AValExpr::OctetLength { col: 3 } },
            AAgg::CountStar,
            AAgg::MinBytes { e: AValExpr::Col(3) },
        ],
        order: None,
        agg_filters: Vec::new(),
        having: None,
        params: Vec::new(),
        fingerprints: Vec::new(),
        kernel_oracle: String::new(),
        notes: "derived-topk-bound".into(),
        flags: Vec::new(),
    };
    plan_from_ap(&engine.bank, &engine.faces, &ap).expect("plan")
}

/// The seam's [q28-serve] down-pass: (avg DESC, key-bytes ASC) non-native
/// bound + the fused strict COUNT(*) HAVING. Answer columns: 0 = derived
/// key, 1 = AVG ratio, 2 = COUNT, 3 = MIN bytes.
fn push_bound(node: &mut PlanNode) {
    node.params.topk = Some(TopK {
        keys: vec![
            TopKKey { col: 1, desc: true, nulls_first: true, lo: None, trim: false },
            TopKKey { col: 0, desc: false, nulls_first: false, lo: None, trim: false },
        ],
        n: K,
        native: false,
    });
    node.params.having_min_count = HMIN;
}

/// Scalar oracle over the same row law: fold every non-empty row, filter
/// `count > HMIN`, order by exact cross-multiplied AVG desc then key
/// bytes asc, keep the top K keys.
fn oracle_top_keys() -> Vec<Vec<u8>> {
    let mut m: HashMap<Vec<u8>, (u64, u64)> = HashMap::new();
    for i in 0..ROWS {
        let s = row_text(i);
        if s.is_empty() {
            continue;
        }
        let k = referer_key(s.as_bytes()).to_vec();
        let e = m.entry(k).or_insert((0, 0));
        e.0 += 1;
        e.1 += s.len() as u64;
    }
    let mut rows: Vec<(Vec<u8>, u64, u64)> = m
        .into_iter()
        .filter(|(_, (c, _))| *c > HMIN)
        .map(|(k, (c, s))| (k, c, s))
        .collect();
    rows.sort_by(|a, b| {
        (b.2 as u128 * a.1 as u128)
            .cmp(&(a.2 as u128 * b.1 as u128))
            .then_with(|| a.0.cmp(&b.0))
    });
    rows.into_iter().take(K).map(|(k, _, _)| k).collect()
}

#[test]
fn derived_key_k_bounded_admission_and_identity() {
    let dir = std::env::temp_dir().join(format!("sqe_dkftopk_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);
    let engine = open_engine(&dir);

    // RED (pinned): no pushed bound -> the witness gate refuses verbatim.
    let unbounded = q28_node(&engine);
    match check_server_grouped(&engine.bank, &engine.faces, &unbounded) {
        Err(Refuse::GroupCountUnwitnessed { what: "no-witness" }) => {}
        other => panic!("unwitnessed full-answer shape must refuse no-witness, got {other:?}"),
    }

    // GREEN: the k-bounded down-pass admits without a group-count witness.
    let mut bounded = q28_node(&engine);
    push_bound(&mut bounded);
    check_server_grouped(&engine.bank, &engine.faces, &bounded)
        .expect("k-bounded derived-key shape must admit");

    // Fail-closed: the generalized `having` comparator is not the
    // stencil's law — admission must refuse it, never ignore it.
    let mut general = q28_node(&engine);
    push_bound(&mut general);
    general.params.having_min_count = 0;
    general.params.having = Some(sqe::ir::HavingCmp {
        agg: 1,
        op: sqe::ir::HvOp::Gt,
        rhs: HMIN as i64,
    });
    assert!(
        check_server_grouped(&engine.bank, &engine.faces, &general).is_err(),
        "generalized having must refuse fail-closed on DerivedKeyFold"
    );

    // Execution identity: HAVING-before-topk + exact-AVG order + the
    // canonical-key tie law. The kept K lines must be exactly the
    // oracle's keys, drawn line-identical from the full survivor set.
    let ctx = engine.ctx();
    let a_bound = sqe::exec::run_node(&ctx, &bounded);
    assert_eq!(a_bound.nrows, K, "bounded answer keeps exactly K survivors");

    // Full survivor twin (fused HAVING, no bound): line identity source.
    let mut survivors = q28_node(&engine);
    survivors.params.having_min_count = HMIN;
    let a_surv = sqe::exec::run_node(&ctx, &survivors);
    assert_eq!(a_surv.nrows, 20, "20 hosts survive COUNT(*) > HMIN");

    let rows_only = |a: &sqe::answer::AnswerSet| -> Vec<String> {
        to_lines(a).into_iter().filter(|l| l.contains('\t')).collect()
    };
    let full_lines: Vec<String> = rows_only(&a_surv);
    let mut got: Vec<String> = rows_only(&a_bound);
    got.sort();
    let keys = oracle_top_keys();
    assert_eq!(keys.len(), K);
    let mut want: Vec<String> = Vec::new();
    for k in &keys {
        let ks = String::from_utf8_lossy(k).into_owned();
        let line = full_lines
            .iter()
            .find(|l| l.split('\t').next() == Some(ks.as_str()))
            .unwrap_or_else(|| panic!("oracle key {ks} missing from survivor set"))
            .clone();
        want.push(line);
    }
    want.sort();
    assert_eq!(got, want, "bounded window == oracle top-K (HAVING-first, exact-AVG, tie law)");

    // The boundary tie law in the concrete: h14 kept, h15 cut.
    let joined = got.join("\n");
    assert!(joined.contains("h14.example"), "tie boundary keeps the ASC key (h14)");
    assert!(!joined.contains("h15.example"), "tie boundary cuts h15");
    // HAVING-before-topk in the concrete: the longest-avg decoy is out.
    assert!(!joined.contains("zz-decoy"), "under-HMIN decoy must not ride the top-K window");

    let _ = std::fs::remove_dir_all(&dir);
}
