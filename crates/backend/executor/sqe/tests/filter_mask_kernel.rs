//! [sqe-filter-simd] Identity pins for the filtered-fold mask kernels
//! (kernels_pred): the byte-mask conjunction + branchless masked/dense
//! folds must render byte-identically to the brute row-law oracle at
//! every pool width, across every arm the granule body can take —
//! zone-proven all-pass (dense fold, predicate decode skipped), mixed
//! mask granules (vector compare + AND), the sparse selectivity gate
//! (selection-walk residual), NULL-bearing predicate and agg columns
//! (validity AND + strict scalar residual), and the w8 fold fallback.

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

/// Row law: `a` unique w8 (fold fallback leg), `b` a small tie-heavy
/// domain (selectivity control; negative values exercise the signed
/// extension), `c` file-ordered (zone all-pass granules exist for every
/// one-sided range), `n` NULL every 5th row.
fn row(i: u64) -> (i64, i64, i64, Option<i64>) {
    let n = if i % 5 == 0 { None } else { Some((i as i64 * 13 % 1000) - 500) };
    (9_000_000_000 + i as i64, (i % 7) as i64 - 3, i as i64, n)
}

fn seal_bank(dir: &str) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let schema = vec![wcol(1, 8, 8), wcol(2, 4, 4), wcol(3, 4, 4), wcol(4, 4, 4)];
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
        let (a, b, c, n) = row(i);
        let datums = [
            RawDatum::Word(a as u64),
            RawDatum::Word(b as u64),
            RawDatum::Word(c as u64),
            n.map_or(RawDatum::Null, |v| RawDatum::Word(v as u64)),
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

fn open_engine(dir: &str, threads: usize) -> Engine {
    let schema = vec![
        ColMeta::new(1, "a", TypMeta::INT8),
        ColMeta::new(2, "b", TypMeta::INT4),
        ColMeta::new(3, "c", TypMeta::INT4),
        ColMeta::new(4, "n", TypMeta::INT4),
    ];
    let bank = Bank::open(dir, schema, &OpenOpts { bankstats: true, threads: 0 });
    Engine::new(bank, SqeConfig { threads, ..SqeConfig::default() })
}

fn ap(q: u32, pred: APred, agg: Vec<AAgg>, cols: Vec<u32>) -> APlan {
    APlan {
        sortagg_keys: Vec::new(),
        win: None,
        q,
        family: AFamily::FusedFilterAgg,
        tags: Vec::new(),
        cols,
        pred: Some(pred),
        group: Vec::new(),
        agg,
        order: None,
        having: None,
        agg_filters: Vec::new(),
        params: Vec::new(),
        fingerprints: Vec::new(),
        kernel_oracle: String::new(),
        notes: "filter-mask-kernel".into(),
        flags: Vec::new(),
    }
}

fn run_widths(dir: &str, p: &APlan) -> Vec<(usize, Vec<String>)> {
    [1usize, 2, 8]
        .iter()
        .map(|&t| {
            let e = open_engine(dir, t);
            let node = plan_from_ap(&e.bank, &e.faces, p).expect("plan filtered fold");
            let mut v = to_lines(&e.run(&node));
            v.sort();
            (t, v)
        })
        .collect()
}

fn assert_all(dir: &str, p: &APlan, oracle: Vec<String>, tag: &str) {
    for (t, got) in run_widths(dir, p) {
        assert_eq!(got, oracle, "{tag} at width {t}");
    }
}

#[test]
fn filter_mask_identity_across_widths() {
    let dir = format!(
        "{}/sqe-filter-mask-kernel-{}",
        std::env::temp_dir().display(),
        std::process::id()
    );
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);

    // One-sided range on the file-ordered column: granules wholly above
    // the cut take the zone-all-pass dense arm, the boundary granule the
    // mask arm, granules below zone-skip. w4 legs fold branchless; the
    // w8 sum/min/max legs walk the selection residual.
    let p = ap(
        1,
        APred::Range { col: 3, lo: "5000".into(), hi: i64::MAX.to_string(), fp: None },
        vec![
            AAgg::CountStar,
            AAgg::Sum { e: AValExpr::Col(2) },
            AAgg::Min { e: AValExpr::Col(2) },
            AAgg::Max { e: AValExpr::Col(2) },
            AAgg::Sum { e: AValExpr::Col(1) },
            AAgg::Min { e: AValExpr::Col(1) },
            AAgg::Max { e: AValExpr::Col(1) },
        ],
        vec![1, 2, 3],
    );
    let (mut cnt, mut sb, mut sa) = (0i64, 0i128, 0i128);
    let (mut mnb, mut mxb, mut mna, mut mxa) = (i64::MAX, i64::MIN, i64::MAX, i64::MIN);
    for i in 0..ROWS {
        let (a, b, c, _) = row(i);
        if c >= 5000 {
            cnt += 1;
            sb += b as i128;
            sa += a as i128;
            mnb = mnb.min(b);
            mxb = mxb.max(b);
            mna = mna.min(a);
            mxa = mxa.max(a);
        }
    }
    assert!(cnt > 0);
    assert_all(&dir, &p, vec![format!("{cnt}\t{sb}\t{mnb}\t{mxb}\t{sa}\t{mna}\t{mxa}")], "all-pass+mask fold");

    // Two-conjunct mask (the cmp-fold cell's shape) + bit folds; never
    // all-pass (b's zone straddles both cuts in every granule).
    let p = ap(
        2,
        APred::And {
            legs: vec![
                APred::Range { col: 2, lo: "-1".into(), hi: "3".into(), fp: None },
                APred::Range { col: 3, lo: "0".into(), hi: "20000".into(), fp: None },
            ],
        },
        vec![
            AAgg::CountStar,
            AAgg::Sum { e: AValExpr::Col(2) },
            AAgg::BitAnd { e: AValExpr::Col(2) },
            AAgg::BitOr { e: AValExpr::Col(2) },
        ],
        vec![2, 3],
    );
    let (mut cnt, mut sb, mut ba, mut bo) = (0i64, 0i128, -1i64, 0i64);
    for i in 0..ROWS {
        let (_, b, c, _) = row(i);
        if (-1..=3).contains(&b) && (0..=20_000).contains(&c) {
            cnt += 1;
            sb += b as i128;
            ba &= b;
            bo |= b;
        }
    }
    assert!(cnt > 0);
    assert_all(&dir, &p, vec![format!("{cnt}\t{sb}\t{ba}\t{bo}")], "conjunct mask + bit folds");

    // Sparse survivor band (~14%): the selectivity gate elects the
    // selection-walk fold over the masked fold.
    let p = ap(
        3,
        APred::Eq { col: 2, val: "3".into(), fp: None },
        vec![AAgg::CountStar, AAgg::Sum { e: AValExpr::Col(2) }],
        vec![2],
    );
    let (mut cnt, mut sb) = (0i64, 0i128);
    for i in 0..ROWS {
        let (_, b, _, _) = row(i);
        if b == 3 {
            cnt += 1;
            sb += b as i128;
        }
    }
    assert!(cnt > 0);
    assert_all(&dir, &p, vec![format!("{cnt}\t{sb}")], "sparse gate");

    // NULL-bearing predicate + agg column: the validity plane ANDs into
    // the mask (a NULL row never passes — 3VL) and the nullable agg leg
    // takes the strict selection residual.
    let p = ap(
        4,
        APred::Range { col: 4, lo: "-100".into(), hi: "400".into(), fp: None },
        vec![
            AAgg::CountStar,
            AAgg::Sum { e: AValExpr::Col(4) },
            AAgg::Min { e: AValExpr::Col(4) },
            AAgg::Max { e: AValExpr::Col(4) },
            AAgg::Sum { e: AValExpr::Col(2) },
        ],
        vec![2, 4],
    );
    let (mut cnt, mut sn, mut sb) = (0i64, 0i128, 0i128);
    let (mut mnn, mut mxn) = (i64::MAX, i64::MIN);
    for i in 0..ROWS {
        let (_, b, _, n) = row(i);
        if let Some(n) = n {
            if (-100..=400).contains(&n) {
                cnt += 1;
                sn += n as i128;
                sb += b as i128;
                mnn = mnn.min(n);
                mxn = mxn.max(n);
            }
        }
    }
    assert!(cnt > 0);
    assert_all(&dir, &p, vec![format!("{cnt}\t{sn}\t{mnn}\t{mxn}\t{sb}")], "null 3VL mask");

    let _ = std::fs::remove_dir_all(&dir);
}
