//! [sqe-topn-gap] Identity pins for the part-subdivided claim plane and
//! the ScanServe keyed-bound short-circuit (the p72 ledger's worst
//! columnar cells: top-n 13.7x / cmp-fold 6.7x / filtered-fold 5.5x —
//! all one mechanism, a 10-part bank claimed at part grain engaging
//! `10/4 = 2` workers under the claim-depth guard, plus the keyed scan's
//! per-row candidate materialization).
//!
//!  1. ScanServe pushed top-k: the winner SET is unique under cmp_cand's
//!     strict total order ((ui, row) tiebreak), so it must be identical
//!     across pool widths (1 = serial oracle schedule, 2, 8) and equal
//!     to the brute-force oracle over the row law — heavy first-key ties
//!     included (the tiebreak leg is load-bearing).
//!  2. FusedFilterAgg ungrouped filtered fold: the chunked claim plane's
//!     per-worker partial folds must merge to the same rendered answer
//!     at every width (i128 exactness — order independence is the law).

#![cfg(feature = "rig")]

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::engine::{Engine, SqeConfig};
use sqe::ir::{TopK, TopKKey};
use sqe::planner::{plan_from_ap, AAgg, AFamily, APlan, APred, AValExpr};
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

/// Row law: `a` unique; `b` a small tie-heavy order domain (i % 7 — the
/// top-k bound must discriminate mostly by tiebreak); `c` a filter
/// column correlated with file order (zone-skip lanes stay live).
fn row(i: u64) -> (i64, i64, i64) {
    (9_000_000_000 + i as i64, (i % 7) as i64, i as i64)
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
        // 30k rows / 4096-row parts = 8 parts: the SHALLOW claim regime
        // (parts < 4·width for every multi-worker pool below).
        PartCutPolicy { max_rows: 4096, max_bytes: u64::MAX, cut_granule_rows: 1024 },
    )
    .expect("open writer");
    for i in 0..ROWS {
        let (a, b, c) = row(i);
        let datums =
            [RawDatum::Word(a as u64), RawDatum::Word(b as u64), RawDatum::Word(c as u64)];
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

fn open_engine(dir: &str, threads: usize) -> Engine {
    let schema = vec![
        ColMeta::new(1, "a", TypMeta::INT8),
        ColMeta::new(2, "b", TypMeta::INT4),
        ColMeta::new(3, "c", TypMeta::INT4),
    ];
    let bank = Bank::open(dir, schema, &OpenOpts { bankstats: true, threads: 0 });
    Engine::new(bank, SqeConfig { threads, ..SqeConfig::default() })
}

fn ap(q: u32, family: AFamily) -> APlan {
    APlan {
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
        agg_filters: Vec::new(),
        params: Vec::new(),
        fingerprints: Vec::new(),
        kernel_oracle: String::new(),
        notes: "topn-claim-plane".into(),
        flags: Vec::new(),
    }
}

fn sorted_lines(e: &Engine, node: &sqe::ir::PlanNode) -> Vec<String> {
    let a = e.run(node);
    let mut v = to_lines(&a);
    v.sort();
    v
}

#[test]
fn claim_plane_identity_across_widths() {
    let dir = format!(
        "{}/sqe-topn-claim-plane-{}",
        std::env::temp_dir().display(),
        std::process::id()
    );
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);

    // --- ScanServe keyed pushed bound: SELECT a, b ORDER BY b, a LIMIT k
    // (the p72 topn cell's shape; b tie-heavy so the (ui, row) tiebreak
    // leg of the bound short-circuit is exercised on ~every row).
    let mut p = ap(1, AFamily::ScanServe);
    p.cols = vec![1, 2];
    p.agg = vec![AAgg::Emit { e: AValExpr::Col(1) }, AAgg::Emit { e: AValExpr::Col(2) }];
    let widths = [1usize, 2, 8];
    for k in [10usize, 100, 300] {
        let mut per_width: Vec<Vec<String>> = Vec::new();
        for &t in &widths {
            let e = open_engine(&dir, t);
            let mut node = plan_from_ap(&e.bank, &e.faces, &p).expect("plan scan-serve");
            node.params.topk = Some(TopK {
                keys: vec![
                    TopKKey { col: 1, desc: false, nulls_first: false, lo: None, trim: false },
                    TopKKey { col: 0, desc: false, nulls_first: false, lo: None, trim: false },
                ],
                n: k,
                native: true,
            });
            per_width.push(sorted_lines(&e, &node));
        }
        // Brute-force oracle: sort by (b, a), take k, render "a\tb".
        let mut all: Vec<(i64, i64)> = (0..ROWS).map(|i| (row(i).1, row(i).0)).collect();
        all.sort_unstable();
        let mut oracle: Vec<String> =
            all[..k].iter().map(|&(b, a)| format!("{a}\t{b}")).collect();
        oracle.sort();
        for (wi, got) in per_width.iter().enumerate() {
            assert_eq!(got, &oracle, "scan-serve top-{k} at width {}", widths[wi]);
        }
    }

    // --- FusedFilterAgg filtered fold: SELECT count(*), sum(a) WHERE
    // b > 1 AND c <= 20000 (two conjuncts; the cmp-fold cell's shape).
    let mut p = ap(2, AFamily::FusedFilterAgg);
    p.cols = vec![1, 2, 3];
    p.pred = Some(APred::And {
        legs: vec![
            APred::Range { col: 2, lo: "2".into(), hi: "6".into(), fp: None },
            APred::Range { col: 3, lo: "0".into(), hi: "20000".into(), fp: None },
        ],
    });
    p.agg = vec![AAgg::CountStar, AAgg::Sum { e: AValExpr::Col(1) }];
    let mut per_width: Vec<Vec<String>> = Vec::new();
    for &t in &widths {
        let e = open_engine(&dir, t);
        let node = plan_from_ap(&e.bank, &e.faces, &p).expect("plan filtered fold");
        per_width.push(sorted_lines(&e, &node));
    }
    let (mut cnt, mut sum) = (0i64, 0i128);
    for i in 0..ROWS {
        let (a, b, c) = row(i);
        if (2..=6).contains(&b) && (0..=20_000).contains(&c) {
            cnt += 1;
            sum += a as i128;
        }
    }
    assert!(cnt > 0);
    let oracle = vec![format!("{cnt}\t{sum}")];
    for (wi, got) in per_width.iter().enumerate() {
        assert_eq!(got, &oracle, "filtered fold at width {}", widths[wi]);
    }

    let _ = std::fs::remove_dir_all(&dir);
}
