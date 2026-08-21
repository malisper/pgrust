//! [sqe-genagg] General-aggregate identity gate: the algebraic/
//! distributive batch (variance family, bit_and/bit_or, bool via the
//! Min/Max word lane) over real sealed banks — engine answers vs an
//! independent scalar oracle recomputed from the row law, plus the
//! render seam's numeric text against live C-postgres 18.6 psql output
//! (values captured from a real 18.6 install; see each case's comment).

#![cfg(feature = "rig")]

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{bool_col, Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::answer::{AnswerSet, ColData, MomentKind, Validity};
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::engine::{Engine, SqeConfig};
use sqe::planner::{plan_from_ap, AAgg, AFamily, AKeyExpr, APlan, APred, AValExpr};
use sqe::refuse::Refuse;
use sqe::render::moments_str;
use sqe::typmeta::TypMeta;

const ROWS: u64 = 12_000;
const NGROUPS: i64 = 7;

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
///   1 k  int4 NULLABLE — group 5 is all-NULL; group 6 has EXACTLY ONE
///        non-null row (the N=1 samp-NULL law); else a signed value.
///   2 g  int4 group key 0..6 (null-free)
///   3 w  int2 (null-free, signed — the bit-fold lane)
///   4 b  bool (null-free)
///   5 k8 int8 small signed domain (null-free — the witnessed-int8 lane)
///   6 big int8 huge domain (null-free — the overflow-witness refusal)
fn row(i: u64) -> (Option<i64>, i64, i64, bool, i64, i64) {
    let g = (i % NGROUPS as u64) as i64;
    let k = if g == 5 {
        None
    } else if g == 6 {
        (i == 6).then_some(7)
    } else if i % 11 == 4 {
        None
    } else {
        Some(((i.wrapping_mul(0x9E37_79B9)) >> 7) as i64 % 2001 - 1000)
    };
    let w = ((i.wrapping_mul(37)) % 200) as i64 - 100;
    let b = i % 3 == 0;
    let k8 = ((i.wrapping_mul(0x517c_c1b7)) >> 9) as i64 % 1999 - 999;
    let big = if i % 2 == 0 { 1i64 << 40 } else { -(1i64 << 40) } + (i as i64 % 97);
    (k, g, w, b, k8, big)
}

fn seal_bank(dir: &str) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let schema = vec![
        wcol(1, 4, 4),
        wcol(2, 4, 4),
        wcol(3, 2, 2),
        bool_col(4),
        wcol(5, 8, 8),
        wcol(6, 8, 8),
    ];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        5,
        790,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 4096, max_bytes: u64::MAX, cut_granule_rows: 1024 },
    )
    .expect("open writer");
    for i in 0..ROWS {
        let (k, g, wv, b, k8, big) = row(i);
        let datums = [
            k.map(|v| RawDatum::Word(v as u64)).unwrap_or(RawDatum::Null),
            RawDatum::Word(g as u64),
            RawDatum::Word(wv as u64),
            RawDatum::Word(b as u64),
            RawDatum::Word(k8 as u64),
            RawDatum::Word(big as u64),
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
        ColMeta::new(2, "g", TypMeta::INT4),
        ColMeta::new(3, "w", TypMeta::INT2),
        ColMeta::new(4, "b", TypMeta::BOOL),
        ColMeta::new(5, "k8", TypMeta::INT8),
        ColMeta::new(6, "big", TypMeta::INT8),
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
        notes: "genagg".into(),
        flags: Vec::new(),
    }
}

/// The independent scalar oracle: fold one value stream naively.
#[derive(Default, Clone)]
struct Oracle {
    n: i64,
    sum: i128,
    sumsq: i128,
    band: Option<i64>,
    bor: Option<i64>,
    min: Option<i64>,
    max: Option<i64>,
    rows: i64,
}

impl Oracle {
    fn fold(&mut self, v: Option<i64>) {
        self.rows += 1;
        let Some(v) = v else { return };
        self.n += 1;
        self.sum += v as i128;
        self.sumsq += (v as i128) * (v as i128);
        self.band = Some(self.band.map_or(v, |a| a & v));
        self.bor = Some(self.bor.map_or(v, |a| a | v));
        self.min = Some(self.min.map_or(v, |a| a.min(v)));
        self.max = Some(self.max.map_or(v, |a| a.max(v)));
    }
}

/// Pull row `r` of an answer column as (valid, i64-or-0).
fn cell_i64(a: &AnswerSet, ci: usize, r: usize) -> (bool, i64) {
    let c = &a.cols[ci];
    let v = match &c.data {
        ColData::I64(v) => v[r],
        other => panic!("want I64 col, got {other:?}"),
    };
    (c.validity.is_valid(r), v)
}

/// Pull row `r` of a Moments column as (valid, kind, (n, sum, sumsq)).
fn cell_moments(a: &AnswerSet, ci: usize, r: usize) -> (bool, MomentKind, (i64, i128, i128)) {
    let c = &a.cols[ci];
    match &c.data {
        ColData::Moments { kind, trips } => (c.validity.is_valid(r), *kind, trips[r]),
        other => panic!("want Moments col, got {other:?}"),
    }
}

#[test]
fn grouped_genagg_cells_vs_scalar_oracle() {
    let dir = std::env::temp_dir().join(format!("sqe_genagg_grp_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);
    let engine = open_engine(&dir);
    assert!(!engine.bank.null_free(1), "k must carry a real validity stream");
    assert!(engine.bank.parts.len() > 1, "want a multi-part bank (merge law in play)");

    // SELECT g, count(*), var_samp(k), var_pop(k), stddev_samp(k),
    //        stddev_pop(k), bit_and(w), bit_or(w), bool_and(b) [Min],
    //        bool_or(b) [Max] GROUP BY g — the Cells128 foundation route.
    let mut p = ap(0, AFamily::HashPlaneOwnedGroup);
    p.cols = vec![2, 1, 3, 4];
    p.group = vec![AKeyExpr::Col(2)];
    p.agg = vec![
        AAgg::CountStar,
        AAgg::VarSamp { e: AValExpr::Col(1) },
        AAgg::VarPop { e: AValExpr::Col(1) },
        AAgg::StddevSamp { e: AValExpr::Col(1) },
        AAgg::StddevPop { e: AValExpr::Col(1) },
        AAgg::BitAnd { e: AValExpr::Col(3) },
        AAgg::BitOr { e: AValExpr::Col(3) },
        AAgg::Min { e: AValExpr::Col(4) },
        AAgg::Max { e: AValExpr::Col(4) },
    ];
    let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
    let (answer, _, _) = sqe::rig::run_arms(&engine, &node);
    assert_eq!(answer.nrows, NGROUPS as usize);

    // Scalar oracle per group from the row law.
    let mut ok: Vec<Oracle> = vec![Oracle::default(); NGROUPS as usize];
    let mut ow: Vec<Oracle> = vec![Oracle::default(); NGROUPS as usize];
    let mut ob: Vec<Oracle> = vec![Oracle::default(); NGROUPS as usize];
    for i in 0..ROWS {
        let (k, g, w, b, ..) = row(i);
        ok[g as usize].fold(k);
        ow[g as usize].fold(Some(w));
        ob[g as usize].fold(Some(b as i64));
    }

    for r in 0..answer.nrows {
        let (_, g) = cell_i64(&answer, 0, r);
        let g = g as usize;
        let (_, cnt) = cell_i64(&answer, 1, r);
        assert_eq!(cnt, ok[g].rows, "group {g} count(*)");
        for (ci, kind, sample) in [
            (2, MomentKind::VarSamp, true),
            (3, MomentKind::VarPop, false),
            (4, MomentKind::StddevSamp, true),
            (5, MomentKind::StddevPop, false),
        ] {
            let (valid, k2, (n, s, sq)) = cell_moments(&answer, ci, r);
            assert_eq!(k2, kind);
            assert_eq!((n, s, sq), (ok[g].n, ok[g].sum, ok[g].sumsq), "group {g} {kind:?}");
            let want_valid = if sample { ok[g].n > 1 } else { ok[g].n > 0 };
            assert_eq!(valid, want_valid, "group {g} {kind:?} NULL law");
        }
        // Group 5 (all-NULL k): every finisher NULL. Group 6 (n=1): samp
        // NULL, pop valid.
        if g == 5 {
            assert!(!cell_moments(&answer, 2, r).0 && !cell_moments(&answer, 3, r).0);
        }
        if g == 6 {
            let (vs, _, (n, ..)) = cell_moments(&answer, 2, r);
            assert_eq!((vs, n), (false, 1), "N=1 var_samp is NULL");
            assert!(cell_moments(&answer, 3, r).0, "N=1 var_pop is 0, not NULL");
        }
        let (va, band) = cell_i64(&answer, 6, r);
        let (vo, bor) = cell_i64(&answer, 7, r);
        assert!(va && vo);
        assert_eq!((band, bor), (ow[g].band.unwrap(), ow[g].bor.unwrap()), "group {g} bits");
        let (_, ba) = cell_i64(&answer, 8, r);
        let (_, bo) = cell_i64(&answer, 9, r);
        assert_eq!((ba, bo), (ob[g].min.unwrap(), ob[g].max.unwrap()), "group {g} bool");
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ungrouped_genagg_scalar_fold_and_metadata() {
    let dir = std::env::temp_dir().join(format!("sqe_genagg_ung_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);
    let engine = open_engine(&dir);

    let aggs = || {
        vec![
            AAgg::CountStar,
            AAgg::VarSamp { e: AValExpr::Col(1) },
            AAgg::StddevPop { e: AValExpr::Col(1) },
            AAgg::StddevSamp { e: AValExpr::Col(5) }, // witnessed int8 lane
            AAgg::BitAnd { e: AValExpr::Col(3) },
            AAgg::BitOr { e: AValExpr::Col(3) },
            AAgg::Min { e: AValExpr::Col(4) },
            AAgg::Max { e: AValExpr::Col(4) },
        ]
    };
    let check = |answer: &AnswerSet, filter: &dyn Fn(u64) -> bool, what: &str| {
        let (mut okk, mut ok8, mut oww, mut obb) =
            (Oracle::default(), Oracle::default(), Oracle::default(), Oracle::default());
        for i in 0..ROWS {
            if !filter(i) {
                continue;
            }
            let (k, _, w, b, k8, _) = row(i);
            okk.fold(k);
            ok8.fold(Some(k8));
            oww.fold(Some(w));
            obb.fold(Some(b as i64));
        }
        assert_eq!(answer.nrows, 1);
        assert_eq!(cell_i64(answer, 0, 0).1, okk.rows, "{what} count");
        let (v1, k1, t1) = cell_moments(answer, 1, 0);
        assert_eq!((k1, t1), (MomentKind::VarSamp, (okk.n, okk.sum, okk.sumsq)), "{what}");
        assert_eq!(v1, okk.n > 1);
        let (_, k2, t2) = cell_moments(answer, 2, 0);
        assert_eq!((k2, t2), (MomentKind::StddevPop, (okk.n, okk.sum, okk.sumsq)), "{what}");
        let (_, k3, t3) = cell_moments(answer, 3, 0);
        assert_eq!((k3, t3), (MomentKind::StddevSamp, (ok8.n, ok8.sum, ok8.sumsq)), "{what}");
        assert_eq!(cell_i64(answer, 4, 0), (true, oww.band.unwrap()), "{what} bit_and");
        assert_eq!(cell_i64(answer, 5, 0), (true, oww.bor.unwrap()), "{what} bit_or");
        assert_eq!(cell_i64(answer, 6, 0), (true, obb.min.unwrap()), "{what} bool_and");
        assert_eq!(cell_i64(answer, 7, 0), (true, obb.max.unwrap()), "{what} bool_or");
    };

    // A) MetadataAnswer (no predicate): the decode-fallback facts.
    {
        let mut p = ap(1, AFamily::MetadataAnswer);
        p.cols = vec![1, 3, 4, 5];
        p.agg = aggs();
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan metadata");
        let (answer, _, _) = sqe::rig::run_arms(&engine, &node);
        check(&answer, &|_| true, "metadata");
    }

    // B) FusedFilterAgg scalar fold under a real predicate (w BETWEEN
    //    -50 AND 49 — survivors only; strict 3VL folds).
    {
        let mut p = ap(2, AFamily::FusedFilterAgg);
        p.cols = vec![1, 3, 4, 5];
        p.pred = Some(APred::Range {
            col: 3,
            lo: "-50".into(),
            hi: "49".into(),
            fp: None,
        });
        p.agg = aggs();
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan filtered");
        let (answer, _, _) = sqe::rig::run_arms(&engine, &node);
        let in_range = |i: u64| {
            let (_, _, w, ..) = row(i);
            (-50..=49).contains(&w)
        };
        check(&answer, &in_range, "filtered");
    }

    // C) Empty survivor set: strict aggs NULL, count 0 (validity leg).
    {
        let mut p = ap(3, AFamily::FusedFilterAgg);
        p.cols = vec![1, 3, 4, 5];
        p.pred = Some(APred::Range { col: 3, lo: "500".into(), hi: "600".into(), fp: None });
        p.agg = aggs();
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan empty");
        let (answer, _, _) = sqe::rig::run_arms(&engine, &node);
        assert_eq!(cell_i64(&answer, 0, 0), (true, 0));
        for ci in 1..=3 {
            assert!(!answer.cols[ci].validity.is_valid(0), "empty fold col {ci} is NULL");
        }
        for ci in 4..=7 {
            assert!(!answer.cols[ci].validity.is_valid(0), "empty fold col {ci} is NULL");
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn int8_variance_overflow_witness_refuses() {
    let dir = std::env::temp_dir().join(format!("sqe_genagg_wit_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_bank(&dir);
    let engine = open_engine(&dir);

    // `big` spans ±2^40 — outside the ±(2^31−1) exactness band: every
    // variance op over it must refuse TYPED, never fold wrapped.
    for a in [
        AAgg::VarSamp { e: AValExpr::Col(6) },
        AAgg::VarPop { e: AValExpr::Col(6) },
        AAgg::StddevSamp { e: AValExpr::Col(6) },
        AAgg::StddevPop { e: AValExpr::Col(6) },
    ] {
        let mut p = ap(4, AFamily::MetadataAnswer);
        p.cols = vec![6];
        p.agg = vec![a];
        match plan_from_ap(&engine.bank, &engine.faces, &p) {
            Err(Refuse::AggUnsupported { what: "var-stddev-int8-domain-unwitnessed" }) => {}
            other => panic!("unwitnessed int8 variance must refuse typed, got {other:?}"),
        }
    }
    // SUM over the same column stays served (exact i128 — no sumsq law).
    {
        let mut p = ap(5, AFamily::MetadataAnswer);
        p.cols = vec![6];
        p.agg = vec![AAgg::Sum { e: AValExpr::Col(6) }];
        plan_from_ap(&engine.bank, &engine.faces, &p).expect("sum(big) serves");
    }
    // The witnessed int8 lane (small exact domain) serves.
    {
        let mut p = ap(6, AFamily::MetadataAnswer);
        p.cols = vec![5];
        p.agg = vec![AAgg::VarSamp { e: AValExpr::Col(5) }];
        plan_from_ap(&engine.bank, &engine.faces, &p).expect("var_samp(k8) serves");
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Render spot-checks vs LIVE C-postgres 18.6 psql output (captured
/// 2026-08-18 from /home/dev/Downloads/pg-bug-hunting/pg-install,
/// PostgreSQL 18.6 aarch64; queries in each comment). The engine's
/// finisher IS the ported numeric_poly_stddev_internal — these pin the
/// end-to-end scale/rounding against the reference server's text.
#[test]
fn moments_render_matches_live_pg_18_6() {
    // SELECT var_samp(x), var_pop(x), stddev_samp(x), stddev_pop(x)
    //   FROM (VALUES (3),(-4),(0),(5)) v(x);
    // -> 15.3333333333333333|11.5000000000000000|3.9157800414902435|3.3911649915626341
    assert_eq!(moments_str(MomentKind::VarSamp, 4, 4, 50), "15.3333333333333333");
    assert_eq!(moments_str(MomentKind::VarPop, 4, 4, 50), "11.5000000000000000");
    assert_eq!(moments_str(MomentKind::StddevSamp, 4, 4, 50), "3.9157800414902435");
    assert_eq!(moments_str(MomentKind::StddevPop, 4, 4, 50), "3.3911649915626341");
    // SELECT var_pop(x), stddev_pop(x) FROM (VALUES (7)) v(x); -> 0|0
    assert_eq!(moments_str(MomentKind::VarPop, 1, 7, 49), "0");
    assert_eq!(moments_str(MomentKind::StddevPop, 1, 7, 49), "0");
    // SELECT stddev_samp(x) FROM (VALUES (1),(1),(1)) v(x); -> 0
    assert_eq!(moments_str(MomentKind::StddevSamp, 3, 3, 3), "0");
    // SELECT stddev_samp(x), var_pop(x)
    //   FROM (VALUES (2147483647),(-2147483648),(12345)) v(x);
    // -> 2147483648|3074457344220472031
    let (a, b) = (2147483647i128, -2147483648i128);
    let sq = a * a + b * b + 12345 * 12345;
    assert_eq!(moments_str(MomentKind::StddevSamp, 3, 12344, sq), "2147483648");
    assert_eq!(moments_str(MomentKind::VarPop, 3, 12344, sq), "3074457344220472031");
    // int8 lane, witnessed domain: SELECT stddev_samp(x::int8),
    //   var_samp(x::int8) FROM (VALUES (2147483647),(-2147483647),(999999999)) v(x);
    // -> 2223739946|4945019346799087276
    let s: i128 = 999_999_999;
    let sq = a * a + a * a + s * s;
    assert_eq!(moments_str(MomentKind::StddevSamp, 3, s, sq), "2223739946");
    assert_eq!(moments_str(MomentKind::VarSamp, 3, s, sq), "4945019346799087276");
    // Historical aliases share the finisher: SELECT variance(x), stddev(x)
    //   FROM (VALUES (1),(2),(3),(4)) v(x); -> 1.6666666666666667|1.2909944487358056
    assert_eq!(moments_str(MomentKind::VarSamp, 4, 10, 30), "1.6666666666666667");
    assert_eq!(moments_str(MomentKind::StddevSamp, 4, 10, 30), "1.2909944487358056");

    // The all-NULL / N=1 NULL laws render through the validity leg, not
    // the finisher: an AnswerCol::moments masks them.
    let col = sqe::answer::AnswerCol::moments(
        TypMeta::NUMERIC,
        MomentKind::VarSamp,
        vec![(0, 0, 0), (1, 7, 49), (2, 3, 5)],
    );
    match &col.validity {
        Validity::Mask(m) => assert_eq!(m.as_slice(), &[false, false, true]),
        v => panic!("want mask, got {v:?}"),
    }
    let pop = sqe::answer::AnswerCol::moments(
        TypMeta::NUMERIC,
        MomentKind::StddevPop,
        vec![(0, 0, 0), (1, 7, 49)],
    );
    match &pop.validity {
        Validity::Mask(m) => assert_eq!(m.as_slice(), &[false, true]),
        v => panic!("want mask, got {v:?}"),
    }
}
