//! Type-face closure gate (engine-currency lane; census 2026-08-17 face

#![cfg(feature = "rig")]

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::answer::{ColData, Validity};
use sqe::bank::{Bank, ColMeta, Face, OpenOpts};
use sqe::engine::{Engine, SqeConfig};
use sqe::planner::{plan_from_ap, AAgg, AFamily, APlan, APred, AValExpr};
use sqe::render::to_lines;
use sqe::rig::lower::{SelExpr, SqlAgg, SqlExpr, SqlQuery};
use sqe::rig::oracle::run_oracle;
use sqe::typmeta::TypMeta;

const ROWS: u64 = 5_000;

fn col(attno: u32, class: StorageClass, typlen: i16, byval: bool, sem: TypeSemantics) -> ColSchema {
    ColSchema {
        attno,
        class,
        typlen,
        typbyval: byval,
        typalign: if typlen == 8 { b'd' } else { b'i' },
        collation_class: CollationClass::C,
        semantics: sem,
    }
}

/// Row content: b alternates with a bias; f carries negatives, -0.0, and
fn frow(i: u64) -> (bool, f64, u32, [u8; 16]) {
    let b = i % 3 == 0;
    let f = if i % 611 == 17 {
        f64::NAN
    } else if i % 97 == 5 {
        -0.0
    } else {
        ((i as i64 % 401) - 200) as f64 * 1.5
    };
    let u = if i % 53 == 9 { 0x8000_0000u32 + (i as u32 % 1000) } else { i as u32 % 90_000 };
    let mut id = [0u8; 16];
    let h = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    id[..8].copy_from_slice(&h.to_be_bytes());
    id[8..].copy_from_slice(&i.to_be_bytes());
    (b, f, u, id)
}

fn seal_faces_bank(dir: &str) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let schema = vec![
        col(1, StorageClass::Bool, 1, true, TypeSemantics::Bool),
        col(2, StorageClass::F64, 8, true, TypeSemantics::Float),
        col(
            3,
            StorageClass::ByvalWord { width: 4, signed: false },
            4,
            true,
            TypeSemantics::UnsignedInt,
        ),
        col(4, StorageClass::Fixed { len: 16 }, 16, false, TypeSemantics::MemcmpOrdered),
    ];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        5,
        780,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 2048, max_bytes: u64::MAX, cut_granule_rows: 1024 },
    )
    .expect("open writer");
    for i in 0..ROWS {
        let (b, f, u, id) = frow(i);
        let datums = [
            RawDatum::Word(b as u64),
            RawDatum::Word(f.to_bits()),
            RawDatum::Word(u as u64),
            RawDatum::Bytes(&id),
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
    let schema = vec![
        ColMeta::new(1, "b", TypMeta::BOOL),
        ColMeta::new(2, "f", TypMeta::FLOAT8),
        ColMeta::new(3, "u", TypMeta::OID),
        ColMeta::new(4, "id", TypMeta::UUID),
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
        notes: "faces".into(),
        flags: Vec::new(),
    }
}

fn sel_agg(a: SqlAgg) -> (SelExpr, Option<String>) {
    (SelExpr::Agg(a), None)
}

#[test]
fn face_gaps_engine_vs_oracle() {
    let dir = std::env::temp_dir().join(format!("sqe_faces_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal_faces_bank(&dir);
    let engine = open_engine(&dir);
    assert_eq!(engine.bank.rows_total(), ROWS);

    // The reconciled faces are the writer's, not TypMeta guesses.
    assert_eq!(engine.bank.face(1), Face::Bool);
    assert_eq!(engine.bank.face(2), Face::F64);
    assert_eq!(engine.bank.face(3), Face::UnsignedWord(4));
    assert_eq!(engine.bank.face(4), Face::Fixed(16));

    // F1: float8 MIN/MAX with a NaN present — PG order: NaN is GREATEST,
    {
        let mut p = ap(20, AFamily::MetadataAnswer);
        p.cols = vec![2];
        p.agg = vec![AAgg::Min { e: AValExpr::Col(2) }, AAgg::Max { e: AValExpr::Col(2) }];
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        let answer = engine.run(&node);
        let ast = SqlQuery {
            select: vec![sel_agg(SqlAgg::Min(SqlExpr::Col(2))), sel_agg(SqlAgg::Max(SqlExpr::Col(2)))],
            preds: Vec::new(),
            group: Vec::new(),
            order: Vec::new(),
            limit: None,
            offset: 0,
            having_count_gt: None,
        };
        let want = run_oracle(&engine.bank, &ast);
        assert_eq!(to_lines(&want), to_lines(&answer), "float min/max vs oracle");
        match (&answer.cols[0].data, &answer.cols[1].data) {
            (ColData::F64(mn), ColData::F64(mx)) => {
                assert_eq!(mn[0], -300.0, "MIN skips NaN (NaN is greatest)");
                assert!(mx[0].is_nan(), "MAX must be NaN when any NaN exists");
            }
            other => panic!("float answers must be F64 columns, got {other:?}"),
        }
    }

    // F2: unsigned high bit — MAX is the 0x8000_00xx class read UNSIGNED
    {
        let mut p = ap(21, AFamily::MetadataAnswer);
        p.cols = vec![3];
        p.agg = vec![AAgg::Min { e: AValExpr::Col(3) }, AAgg::Max { e: AValExpr::Col(3) }];
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        let answer = engine.run(&node);
        let ast = SqlQuery {
            select: vec![sel_agg(SqlAgg::Min(SqlExpr::Col(3))), sel_agg(SqlAgg::Max(SqlExpr::Col(3)))],
            preds: Vec::new(),
            group: Vec::new(),
            order: Vec::new(),
            limit: None,
            offset: 0,
            having_count_gt: None,
        };
        let want = run_oracle(&engine.bank, &ast);
        assert_eq!(to_lines(&want), to_lines(&answer), "unsigned min/max vs oracle");
        match (&answer.cols[0].data, &answer.cols[1].data) {
            (ColData::I64(mn), ColData::I64(mx)) => {
                assert!(mn[0] >= 0, "unsigned min is nonneg, got {}", mn[0]);
                assert!(
                    mx[0] >= 0x8000_0000,
                    "unsigned max must keep the high bit unsigned, got {}",
                    mx[0]
                );
            }
            other => panic!("unsigned answers must be I64 columns, got {other:?}"),
        }
    }

    // F3: uuid MIN/MAX — memcmp order over the 16-byte images, typed
    {
        let mut p = ap(22, AFamily::MetadataAnswer);
        p.cols = vec![4];
        p.agg = vec![AAgg::Min { e: AValExpr::Col(4) }, AAgg::Max { e: AValExpr::Col(4) }];
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        let answer = engine.run(&node);
        let ast = SqlQuery {
            select: vec![sel_agg(SqlAgg::Min(SqlExpr::Col(4))), sel_agg(SqlAgg::Max(SqlExpr::Col(4)))],
            preds: Vec::new(),
            group: Vec::new(),
            order: Vec::new(),
            limit: None,
            offset: 0,
            having_count_gt: None,
        };
        let want = run_oracle(&engine.bank, &ast);
        assert_eq!(to_lines(&want), to_lines(&answer), "uuid min/max vs oracle");
        // The memcmp winners, computed a third way (plain scan fold).
        let (mut mn, mut mx): (Option<[u8; 16]>, Option<[u8; 16]>) = (None, None);
        for i in 0..ROWS {
            let (_, _, _, id) = frow(i);
            if mn.map(|m| id < m).unwrap_or(true) {
                mn = Some(id);
            }
            if mx.map(|m| id > m).unwrap_or(true) {
                mx = Some(id);
            }
        }
        assert_eq!(answer.cols[0].data.bytes_at(0), mn.unwrap());
        assert_eq!(answer.cols[1].data.bytes_at(0), mx.unwrap());
        assert!(matches!(answer.cols[0].validity, Validity::AllValid));
    }

    // F4: bool face — COUNT(*) WHERE b <> 0 (the stats-answerable pred
    {
        let mut p = ap(23, AFamily::MetadataAnswer);
        p.cols = vec![1];
        p.pred = Some(APred::NeZero { col: 1, fp: None });
        p.agg = vec![AAgg::CountStar];
        let node = plan_from_ap(&engine.bank, &engine.faces, &p).expect("plan");
        let answer = engine.run(&node);
        let truth: i64 = (0..ROWS).filter(|&i| frow(i).0).count() as i64;
        match &answer.cols[0].data {
            ColData::I64(v) => assert_eq!(v[0], truth, "bool nonzero count"),
            other => panic!("count must be I64, got {other:?}"),
        }
        let ast = SqlQuery {
            select: vec![sel_agg(SqlAgg::CountStar)],
            preds: vec![sqe::rig::lower::SqlPred::Cmp {
                col: 1,
                op: sqe::rig::lower::COp::Ne,
                val: sqe::rig::lower::Lit::Num("0".into()),
            }],
            group: Vec::new(),
            order: Vec::new(),
            limit: None,
            offset: 0,
            having_count_gt: None,
        };
        let want = run_oracle(&engine.bank, &ast);
        assert_eq!(to_lines(&want), to_lines(&answer), "bool count vs oracle");
    }

    // F5: face refusals are TYPED — SUM over a float face and a float
    {
        let mut p = ap(24, AFamily::MetadataAnswer);
        p.cols = vec![2];
        p.agg = vec![AAgg::Sum { e: AValExpr::Col(2) }];
        match plan_from_ap(&engine.bank, &engine.faces, &p) {
            Err(sqe::refuse::Refuse::FaceUnsupported { attno: 2, .. }) => {}
            other => panic!("SUM over float face must refuse, got {other:?}"),
        }
        let mut p = ap(25, AFamily::MetadataAnswer);
        p.cols = vec![2];
        p.pred = Some(APred::NeZero { col: 2, fp: None });
        p.agg = vec![AAgg::CountStar];
        match plan_from_ap(&engine.bank, &engine.faces, &p) {
            Err(sqe::refuse::Refuse::FaceUnsupported { attno: 2, .. }) => {}
            other => panic!("float int-pred must refuse, got {other:?}"),
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}
