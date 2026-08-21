//! [P4-1] Fused-arithmetic fold-input identity gate: the closed expr
//! vocabulary (col±k → the SumShifted answer algebra; a*b / a*(k−b) →
//! per-leg fused fold cells) over real sealed banks — engine answers vs
//! an independent scalar oracle recomputed from the row law, the 3VL
//! either-operand-NULL law, the empty-survivor law, and the overflow
//! admission witness (typed refusals, never a wrapped fold — see
//! ir::FoldExpr's overflow law).

#![cfg(feature = "rig")]

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::answer::{AnswerSet, ColData};
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::engine::{Engine, SqeConfig};
use sqe::planner::{plan_from_ap, AAgg, AFamily, APlan, APred, AValExpr};
use sqe::refuse::Refuse;
use sqe::typmeta::TypMeta;

const ROWS: u64 = 12_000;

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

/// The row law. Columns:
///   1 a   int4 NULLABLE (i%11==4 → NULL), −1000..=1000
///   2 d   int4 null-free 0..=100 (the discount lane)
///   3 f   int4 null-free 0..=199 (the filter lane)
///   4 c8  int8 null-free 0..=9_999_999 (the cents lane — exact-domain
///         witnessed int8)
///   5 big int8 null-free ±2^40 (outside every mul witness — the
///         refusal lane)
///   6 n2  int4 NULLABLE (i%7==2 → NULL, a DIFFERENT phase than `a` —
///         the either-operand-NULL law)
fn row(i: u64) -> (Option<i64>, i64, i64, i64, i64, Option<i64>) {
    let a = (i % 11 != 4).then(|| ((i.wrapping_mul(0x9E37_79B9)) >> 7) as i64 % 2001 - 1000);
    let d = ((i.wrapping_mul(37)) % 101) as i64;
    let f = ((i.wrapping_mul(0x517c_c1b7)) >> 5) as i64 % 200;
    let c8 = ((i.wrapping_mul(0x2545_F491)) >> 3) as i64 % 10_000_000;
    let big = if i % 2 == 0 { 1i64 << 40 } else { -(1i64 << 40) } + (i as i64 % 97);
    let n2 = (i % 7 != 2).then(|| ((i.wrapping_mul(151)) % 501) as i64 - 250);
    (a, d, f, c8, big, n2)
}

fn seal_bank(dir: &str) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let schema = vec![
        wcol(1, 4, 4),
        wcol(2, 4, 4),
        wcol(3, 4, 4),
        wcol(4, 8, 8),
        wcol(5, 8, 8),
        wcol(6, 4, 4),
    ];
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
    for i in 0..ROWS {
        let (a, d, f, c8, big, n2) = row(i);
        let datums = [
            a.map(|v| RawDatum::Word(v as u64)).unwrap_or(RawDatum::Null),
            RawDatum::Word(d as u64),
            RawDatum::Word(f as u64),
            RawDatum::Word(c8 as u64),
            RawDatum::Word(big as u64),
            n2.map(|v| RawDatum::Word(v as u64)).unwrap_or(RawDatum::Null),
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
        ColMeta::new(1, "a", TypMeta::INT4),
        ColMeta::new(2, "d", TypMeta::INT4),
        ColMeta::new(3, "f", TypMeta::INT4),
        ColMeta::new(4, "c8", TypMeta::INT8),
        ColMeta::new(5, "big", TypMeta::INT8),
        ColMeta::new(6, "n2", TypMeta::INT4),
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
        notes: "arith".into(),
        flags: Vec::new(),
    }
}

/// Scalar oracle cell: strict fold of an Option<i64> stream.
#[derive(Default, Clone, Copy)]
struct OCell {
    n: i64,
    sum: i128,
    rows: i64,
}

impl OCell {
    fn fold(&mut self, v: Option<i64>) {
        self.rows += 1;
        if let Some(v) = v {
            self.n += 1;
            self.sum += v as i128;
        }
    }
}

fn cell_i64(a: &AnswerSet, ci: usize, r: usize) -> (bool, i64) {
    let c = &a.cols[ci];
    let v = match &c.data {
        ColData::I64(v) => v[r],
        other => panic!("want I64 col, got {other:?}"),
    };
    (c.validity.is_valid(r), v)
}

/// Pull a SUM answer cell as (valid, i128) whatever lane it rendered in.
fn cell_sum(a: &AnswerSet, ci: usize, r: usize) -> (bool, i128) {
    let c = &a.cols[ci];
    let v = match &c.data {
        ColData::I64(v) => v[r] as i128,
        ColData::I128(v) => v[r],
        other => panic!("want int sum col, got {other:?}"),
    };
    (c.validity.is_valid(r), v)
}

/// Pull an AVG answer cell as the exact (sum, n) ratio pair.
fn cell_ratio(a: &AnswerSet, ci: usize, r: usize) -> (bool, i128, i64) {
    let c = &a.cols[ci];
    match &c.data {
        ColData::Ratio { pairs, .. } => {
            let (s, n) = pairs[r];
            (c.validity.is_valid(r), s, n)
        }
        other => panic!("want Ratios col, got {other:?}"),
    }
}

#[test]
fn fused_arith_fold_vs_scalar_oracle() {
    let dir = std::env::temp_dir().join(format!("sqe_arith_fused_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);
    let engine = open_engine(&dir);
    assert!(!engine.bank.null_free(1), "a must carry a real validity stream");
    assert!(engine.bank.parts.len() > 1, "want a multi-part bank (merge law in play)");

    // SELECT count(*), sum(a*d), avg(a*d), sum(a*(100-d)), avg(a*(100-d)),
    //        sum(c8*(100-d)), sum(a+7), sum(a-3), sum(a*n2)
    // FROM t WHERE f > 100 — every mul leg a fused per-leg cell, the ±k
    // legs the SumShifted answer algebra, a*n2 the either-NULL law.
    let mut p = ap(0, AFamily::FusedFilterAgg);
    p.cols = vec![1, 2, 3, 4, 6];
    p.pred = Some(APred::Range { col: 3, lo: "101".into(), hi: "199".into(), fp: None });
    p.agg = vec![
        AAgg::CountStar,
        AAgg::Sum { e: AValExpr::MulCC { a: 1, b: 2, w: 4 } },
        AAgg::Avg { e: AValExpr::MulCC { a: 1, b: 2, w: 4 } },
        AAgg::Sum { e: AValExpr::MulKSub { a: 1, k: 100, b: 2, w: 4, wi: 4 } },
        AAgg::Avg { e: AValExpr::MulKSub { a: 1, k: 100, b: 2, w: 4, wi: 4 } },
        AAgg::Sum { e: AValExpr::MulKSub { a: 4, k: 100, b: 2, w: 8, wi: 4 } },
        AAgg::Sum { e: AValExpr::AddK { col: 1, k: 7, w: 4 } },
        AAgg::Sum { e: AValExpr::AddK { col: 1, k: -3, w: 4 } },
        AAgg::Sum { e: AValExpr::MulCC { a: 1, b: 6, w: 4 } },
    ];
    let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan fused arith");
    let (answer, _, _) = sqe::rig::run_arms(&engine, &node);
    assert_eq!(answer.nrows, 1);

    // Scalar oracle from the row law (strict 3VL: any NULL operand
    // makes the row's input NULL).
    let (mut o_ad, mut o_aksub, mut o_c8, mut o_a, mut o_an2) = (
        OCell::default(),
        OCell::default(),
        OCell::default(),
        OCell::default(),
        OCell::default(),
    );
    let mut survivors = 0i64;
    for i in 0..ROWS {
        let (a, d, f, c8, _, n2) = row(i);
        if !(101..=199).contains(&f) {
            continue;
        }
        survivors += 1;
        o_ad.fold(a.map(|a| a * d));
        o_aksub.fold(a.map(|a| a * (100 - d)));
        o_c8.fold(Some(c8 * (100 - d)));
        o_a.fold(a);
        o_an2.fold(match (a, n2) {
            (Some(a), Some(n2)) => Some(a * n2),
            _ => None,
        });
    }
    assert!(survivors > 0, "filter must keep rows");
    assert!(o_ad.n < survivors, "NULL rows must be in play");

    assert_eq!(cell_i64(&answer, 0, 0), (true, survivors), "count(*)");
    assert_eq!(cell_sum(&answer, 1, 0), (true, o_ad.sum), "sum(a*d)");
    assert_eq!(cell_ratio(&answer, 2, 0), (true, o_ad.sum, o_ad.n), "avg(a*d)");
    assert_eq!(cell_sum(&answer, 3, 0), (true, o_aksub.sum), "sum(a*(100-d))");
    assert_eq!(cell_ratio(&answer, 4, 0), (true, o_aksub.sum, o_aksub.n), "avg(a*(100-d))");
    assert_eq!(cell_sum(&answer, 5, 0), (true, o_c8.sum), "sum(c8*(100-d))");
    assert_eq!(cell_sum(&answer, 6, 0), (true, o_a.sum + 7 * o_a.n as i128), "sum(a+7)");
    assert_eq!(cell_sum(&answer, 7, 0), (true, o_a.sum - 3 * o_a.n as i128), "sum(a-3)");
    assert_eq!(cell_sum(&answer, 8, 0), (true, o_an2.sum), "sum(a*n2) either-NULL law");

    // Empty survivor set: strict expr folds answer NULL, count 0.
    {
        let mut p = ap(1, AFamily::FusedFilterAgg);
        p.cols = vec![1, 2, 3];
        p.pred = Some(APred::Range { col: 3, lo: "500".into(), hi: "600".into(), fp: None });
        p.agg = vec![
            AAgg::CountStar,
            AAgg::Sum { e: AValExpr::MulCC { a: 1, b: 2, w: 4 } },
            AAgg::Sum { e: AValExpr::AddK { col: 1, k: 7, w: 4 } },
        ];
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan empty");
        let (answer, _, _) = sqe::rig::run_arms(&engine, &node);
        assert_eq!(cell_i64(&answer, 0, 0), (true, 0));
        assert!(!answer.cols[1].validity.is_valid(0), "empty sum(a*d) is NULL");
        assert!(!answer.cols[2].validity.is_valid(0), "empty sum(a+7) is NULL");
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn metadata_sum_shifted_battery_vs_oracle() {
    let dir = std::env::temp_dir().join(format!("sqe_arith_meta_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);
    let engine = open_engine(&dir);

    // The q29 shape: SUM(a+0), SUM(a+1), … (no predicate — the metadata
    // answer algebra Σ(a+k) = Σa + k·nonnull), plus the AddK spelling.
    let mut p = ap(2, AFamily::MetadataAnswer);
    p.cols = vec![1];
    p.agg = vec![
        AAgg::SumShifted { col: 1, k0: 0, n: 3 },
        AAgg::Sum { e: AValExpr::AddK { col: 1, k: 42, w: 4 } },
        AAgg::Sum { e: AValExpr::AddK { col: 1, k: -5, w: 4 } },
    ];
    let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan metadata addk");
    assert_eq!(node.agg.len(), 5, "battery expands per shift");
    let (answer, _, _) = sqe::rig::run_arms(&engine, &node);
    assert_eq!(answer.nrows, 1);

    let mut o = OCell::default();
    for i in 0..ROWS {
        o.fold(row(i).0);
    }
    for (ci, k) in [(0usize, 0i128), (1, 1), (2, 2), (3, 42), (4, -5)] {
        assert_eq!(cell_sum(&answer, ci, 0), (true, o.sum + k * o.n as i128), "sum(a+{k})");
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn arith_overflow_witness_refuses_typed() {
    let dir = std::env::temp_dir().join(format!("sqe_arith_wit_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);
    let engine = open_engine(&dir);

    let refuses = |agg: AAgg, why: &str| {
        let mut p = ap(3, AFamily::FusedFilterAgg);
        p.cols = vec![1, 2, 3, 5];
        p.pred = Some(APred::Range { col: 3, lo: "0".into(), hi: "199".into(), fp: None });
        p.agg = vec![agg];
        match plan_from_ap(&engine.bank, &engine.faces, &p) {
            Err(Refuse::AggUnsupported { what }) if what == why => {}
            other => panic!("want typed refusal {why}, got {other:?}"),
        }
    };

    // big*big: ±2^40 squared blows past i64 — refuse, never wrap.
    refuses(
        AAgg::Sum { e: AValExpr::MulCC { a: 5, b: 5, w: 8 } },
        "agg-arith-overflow-unwitnessed",
    );
    // a*d with the op result CLAIMED int2: the domain (±10^5) cannot fit.
    refuses(
        AAgg::Sum { e: AValExpr::MulCC { a: 1, b: 2, w: 2 } },
        "agg-arith-overflow-unwitnessed",
    );
    // big + i64::MAX: the shift escapes int8.
    refuses(
        AAgg::Sum { e: AValExpr::AddK { col: 5, k: i64::MAX, w: 8 } },
        "agg-arith-overflow-unwitnessed",
    );
    // a*(k−d) with k−d escaping the INNER op's int4 result type
    // (k − d underflows i32 for d > 0; the OUTER int8 op is roomy —
    // exactly the inner-op proof obligation).
    refuses(
        AAgg::Sum { e: AValExpr::MulKSub { a: 1, k: -(i32::MAX as i64), b: 2, w: 8, wi: 4 } },
        "agg-arith-overflow-unwitnessed",
    );
    // AVG(col+k) has no engine spelling — typed shape refusal.
    refuses(AAgg::Avg { e: AValExpr::AddK { col: 1, k: 1, w: 4 } }, "agg-expr-shape");

    // Expr legs outside their execution home (metadata family) refuse.
    {
        let mut p = ap(4, AFamily::MetadataAnswer);
        p.cols = vec![1, 2];
        p.agg = vec![AAgg::Sum { e: AValExpr::MulCC { a: 1, b: 2, w: 4 } }];
        match plan_from_ap(&engine.bank, &engine.faces, &p) {
            Err(Refuse::AggUnsupported { what: "agg-arith-family" }) => {}
            other => panic!("want agg-arith-family refusal, got {other:?}"),
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// The rig SQL front end drives the same lane: parse → elect → lower →
/// execute, answers vs the independent row-law oracle re-derivation.
#[test]
fn sql_lowered_arith_vs_oracle() {
    let dir = std::env::temp_dir().join(format!("sqe_arith_sql_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);
    let engine = open_engine(&dir);
    let ctx = engine.ctx();

    // Fused shape (predicate → FusedFilterAgg).
    let node = sqe::rig::lower::lower(
        &ctx,
        900,
        "SELECT COUNT(*), SUM(a * d), SUM(a * (100 - d)), AVG(a * d), SUM(a + 7) FROM hits WHERE f >= 101 AND f <= 199;",
        "arith-test",
    )
    .expect("sql arith lowers");
    let (answer, _, _) = sqe::rig::run_arms(&engine, &node);
    let (mut o_ad, mut o_ak, mut o_a) = (OCell::default(), OCell::default(), OCell::default());
    let mut survivors = 0i64;
    for i in 0..ROWS {
        let (a, d, f, ..) = row(i);
        if f <= 100 {
            continue;
        }
        survivors += 1;
        o_ad.fold(a.map(|a| a * d));
        o_ak.fold(a.map(|a| a * (100 - d)));
        o_a.fold(a);
    }
    assert_eq!(cell_i64(&answer, 0, 0), (true, survivors));
    assert_eq!(cell_sum(&answer, 1, 0), (true, o_ad.sum));
    assert_eq!(cell_sum(&answer, 2, 0), (true, o_ak.sum));
    assert_eq!(cell_ratio(&answer, 3, 0), (true, o_ad.sum, o_ad.n));
    assert_eq!(cell_sum(&answer, 4, 0), (true, o_a.sum + 7 * o_a.n as i128));

    // The q29 shape (no predicate → MetadataAnswer via the AddK
    // stats-answerable law).
    let node = sqe::rig::lower::lower(
        &ctx,
        901,
        "SELECT SUM(a + 1), SUM(a + 2), SUM(a - 4) FROM hits;",
        "arith-test",
    )
    .expect("sql addk battery lowers");
    assert_eq!(node.family, sqe::ir::Family::MetadataAnswer, "q29 shape rides metadata");
    let (answer, _, _) = sqe::rig::run_arms(&engine, &node);
    let mut o = OCell::default();
    for i in 0..ROWS {
        o.fold(row(i).0);
    }
    for (ci, k) in [(0usize, 1i128), (1, 2), (2, -4)] {
        assert_eq!(cell_sum(&answer, ci, 0), (true, o.sum + k * o.n as i128), "sum(a{k:+})");
    }

    // Outside the vocabulary: general nesting stays a lowering gap.
    assert!(
        sqe::rig::lower::lower(&ctx, 902, "SELECT SUM((a + 1) * (d + 2)) FROM hits;", "arith-test")
            .is_err(),
        "general nesting refuses"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
