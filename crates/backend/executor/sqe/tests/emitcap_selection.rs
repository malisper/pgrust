//! Emit-cap truncation guard [emitcap-audit]: a NATIVE pushed bound with a
//! (count DESC) key must never truncate an answer BEFORE the (count DESC,
//! key ASC) selection — for every grouped family, a forced tiny bound over
//! an ADVERSARIAL bank (group counts rise with key order, so a key-order
//! truncation keeps exactly the WRONG groups) must keep the same group SET
//! as the unbounded fold + the answer-boundary selection
//! (`answer::apply_topk`), byte-identically at the render seam.
//!
//! The landmine this pins (filed by the stmt-constant lane, notes/
//! stmt-constant-lane.md @9267c83bd39): `code_agg::dense_int_entrylen`
//! rendered under `match params.order` and truncated to `emit_cap()` with
//! NO selection when `order = None` — the exact server posture (the seam
//! lowers every grouped ORDER/LIMIT with `ap.order = None` + a native
//! `params.topk`), so any admission widening onto that arm would have
//! returned the bottom-of-domain keys as the "top k". Same class in
//! `two_level::render_bytes` (the no-dict fallback), pinned by a unit test
//! beside that function.

#![cfg(feature = "rig")]

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{img_4b_u, Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::answer::apply_topk;
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::engine::{Engine, SqeConfig};
use sqe::ir::{PlanNode, TopK, TopKKey};
use sqe::planner::{plan_from_ap, AAgg, AFamily, AKeyExpr, APlan, APred, AValExpr};
use sqe::render::to_lines;
use sqe::typmeta::TypMeta;

/// Group census: key j in 0..NK, count(j) = 50 + 37*j — counts are UNIQUE
/// and STRICTLY RISING with the key, so the top-k-by-count set and the
/// first-k-in-key-order set are disjoint for small k (the adversarial law).
const NK: u64 = 24;
const K: usize = 3;

fn cnt_of(j: u64) -> u64 {
    50 + 37 * j
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

/// One logical row of group j at within-group occurrence `occ`:
///   k = j (INT4, dense 0..NK — the entrylen arm's stats-exact domain)
///   g = j % 5 (INT2 second byval key; (k,g) groups == k groups)
///   s = per-group text, byte order ASC with j, length varies with j
///   d = occ % (10 + 3j)  (distinct census 10+3j: unique, rising with j)
///   r = 1000 + (occ % 600)
fn row(j: u64, occ: u64) -> (i64, i64, String, i64, i64) {
    let s = format!("t{:02}{}", j, "x".repeat((j % 7) as usize));
    (j as i64, (j % 5) as i64, s, (occ % (10 + 3 * j)) as i64, 1_000 + (occ % 600) as i64)
}

fn seal_bank(dir: &str) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let schema = vec![wcol(1, 4, 4), wcol(2, 2, 2), tcol(3), wcol(4, 8, 8), wcol(5, 4, 4)];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        5,
        791,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 4096, max_bytes: u64::MAX, cut_granule_rows: 1024 },
    )
    .expect("open writer");
    // Adversarial insertion order: a deterministic odd-stride permutation
    // over the concatenated group runs — every part sees a key mixture.
    let mut flat: Vec<(u64, u64)> = Vec::new();
    for j in 0..NK {
        for occ in 0..cnt_of(j) {
            flat.push((j, occ));
        }
    }
    let n = flat.len() as u64;
    for i in 0..n {
        let (j, occ) = flat[((i.wrapping_mul(2_654_435_761)) % n) as usize];
        let (k, g, s, d, r) = row(j, occ);
        let img = img_4b_u(s.as_bytes());
        let datums = [
            RawDatum::Word(k as u64),
            RawDatum::Word(g as u64),
            RawDatum::Bytes(&img),
            RawDatum::Word(d as u64),
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
        ColMeta::new(1, "k", TypMeta::INT4),
        ColMeta::new(2, "g", TypMeta::INT2),
        ColMeta::new(3, "s", TypMeta::TEXT_C),
        ColMeta::new(4, "d", TypMeta::INT8),
        ColMeta::new(5, "r", TypMeta::INT4),
    ];
    let bank = Bank::open(dir, schema, &OpenOpts { bankstats: true, threads: 0 });
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
        notes: "emitcap-selection".into(),
        flags: Vec::new(),
    }
}

/// The one guard law, per family:
///   got  = engine.run(node + native (count DESC) topk)      — server posture
///   want = engine.run(node unbounded) + `apply_topk` (same spec) — oracle
/// Compared as SETS (sorted rendered lines): row order within the kept set
/// belongs to the server emitter; the SET is the stencil's contract.
fn gate(engine: &Engine, node: &PlanNode, count_col: u32, label: &str) {
    let spec = TopK {
        keys: vec![TopKKey { col: count_col, desc: true, nulls_first: false, lo: None, trim: false }],
        n: K,
        native: true,
    };

    let mut want = engine.run(node);
    assert!(
        want.nrows as u64 >= NK,
        "{label}: unbounded twin must see every group (got {})",
        want.nrows
    );
    apply_topk(&mut want, &spec);
    let mut want = to_lines(&want);
    want.sort();

    let mut bounded = node.clone();
    bounded.params.topk = Some(spec);
    let a = engine.run(&bounded);
    assert_eq!(a.nrows, K, "{label}: bounded answer must keep exactly the top {K}");
    let mut got = to_lines(&a);
    got.sort();

    assert_eq!(got, want, "{label}: native bound kept the wrong group SET");
}

#[test]
fn native_count_bound_selects_before_truncation() {
    let dir = std::env::temp_dir().join(format!("sqe_emitcap_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);
    let engine = open_engine(&dir);
    assert!(engine.bank.parts.len() > 1, "want a multi-part bank");

    // 1) TwoLevelCodeAgg / dense-int entrylen arm (code_agg) — THE filed
    //    landmine: order=None render truncated to emit_cap with no
    //    selection. Answer cols [k, avg_len, count] -> count key col 2.
    {
        let mut p = ap(900, AFamily::TwoLevelCodeAgg);
        p.cols = vec![1, 3];
        p.group = vec![AKeyExpr::Col(1)];
        p.agg = vec![
            AAgg::Avg { e: AValExpr::OctetLength { col: 3 } },
            AAgg::CountStar,
        ];
        p.pred = Some(APred::NeEmpty { col: 3, fp: None });
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan entrylen");
        gate(&engine, &node, 2, "code_agg dense_int entrylen");
    }

    // 2) TwoLevelCodeAgg / varlena COUNT(*) (part_merge::part_count route).
    //    Answer cols [s, count] -> count key col 1.
    {
        let mut p = ap(901, AFamily::TwoLevelCodeAgg);
        p.cols = vec![3];
        p.group = vec![AKeyExpr::Col(3)];
        p.agg = vec![AAgg::CountStar];
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan part_count");
        gate(&engine, &node, 1, "two_level part_count");
    }

    // 3) HashPlaneOwnedGroup, byval pair keys + payload lanes.
    //    Answer cols [k, g, count, sum, avg] -> count key col 2.
    {
        let mut p = ap(902, AFamily::HashPlaneOwnedGroup);
        p.cols = vec![1, 2, 4, 5];
        p.group = vec![AKeyExpr::Col(1), AKeyExpr::Col(2)];
        p.agg = vec![
            AAgg::CountStar,
            AAgg::Sum { e: AValExpr::Col(4) },
            AAgg::Avg { e: AValExpr::Col(5) },
        ];
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan hash_plane");
        gate(&engine, &node, 2, "hash_plane owned-group");
    }

    // 4) DistinctPipeline: text key, COUNT(DISTINCT d) — the distinct
    //    census (10+3j) is unique and rises with the key.
    //    Answer cols [s, count] -> count key col 1.
    {
        let mut p = ap(903, AFamily::DistinctPipeline);
        p.cols = vec![3, 4];
        p.group = vec![AKeyExpr::Col(3)];
        p.agg = vec![AAgg::CountDistinct { e: AValExpr::Col(4) }];
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan distinct");
        gate(&engine, &node, 1, "distinct pipeline gid_grouped");
    }

    // 5) SurvivorGather: byval pair keys, one NeEmpty varlena frame term,
    //    COUNT + SUM + AVG. Answer cols [k, g, count, sum, avg] -> col 2.
    {
        let mut p = ap(904, AFamily::SurvivorGather);
        p.cols = vec![1, 2, 3, 4, 5];
        p.group = vec![AKeyExpr::Col(1), AKeyExpr::Col(2)];
        p.agg = vec![
            AAgg::CountStar,
            AAgg::Sum { e: AValExpr::Col(4) },
            AAgg::Avg { e: AValExpr::Col(5) },
        ];
        p.pred = Some(APred::NeEmpty { col: 3, fp: None });
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan gather");
        gate(&engine, &node, 2, "survivor_gather");
    }

    let _ = std::fs::remove_dir_all(&dir);
}
