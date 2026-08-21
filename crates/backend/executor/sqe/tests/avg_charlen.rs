//! [q27-charlen] AVG(length(text)) — the CHARACTER-count fold (AvgCharLen)
//! the official ClickBench queries demand (`length(URL)` = textlen, chars
//! not bytes; the submission cell nulled Q28/Q29 with expr/func-expr
//! fp=780ab43e9055aea4 / d2bfb3536bf9dc33 because only octet_length was
//! vocabulary). Multibyte text makes the byte and char kernels answer
//! DIFFERENTLY — this gate pins the char answer to a scalar oracle and
//! pins the two kernels apart, on both the dict entrylen route and the
//! hydrated fallback of the q27 two-level stencil.

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
use sqe::planner::{plan_from_ap, AAgg, AFamily, AKeyExpr, APlan, APred, AValExpr};
use sqe::render::to_lines;
use sqe::typmeta::TypMeta;

const ROWS: u64 = 60_000;
const KEYS: u64 = 40;

fn wcol(attno: u32) -> ColSchema {
    ColSchema {
        attno,
        class: StorageClass::ByvalWord { width: 4, signed: true },
        typlen: 4,
        typbyval: true,
        typalign: b'i',
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

/// Row law: key 0..KEYS; text mixes ASCII, multibyte (cyrillic — 2 bytes
/// per char) and EMPTY values (the `<> ''` gate must exclude them from
/// the count on both kernels).
fn row(i: u64) -> (u64, String) {
    let k = (i.wrapping_mul(0x9E37_79B9) >> 7) % KEYS;
    let s = match i % 5 {
        0 => String::new(),
        1 => format!("plain_{}", i % 97),
        2 => format!("путь_{}", i % 53),
        3 => "смешанный/mixed".to_string(),
        _ => format!("u{}", i % 31),
    };
    (k, s)
}

fn seal(dir: &str, dict: bool) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let posture = if dict {
        ColumnPosture {
            dict: Some(DictPolicy {
                ndv_cap: 1 << 16,
                exec_ok: true,
                sem: TextSemantics::Utf8Chars,
            }),
            ..Default::default()
        }
    } else {
        ColumnPosture::default()
    };
    let cands = CodecCandidates::new(ColumnPosture::default()).with_column(2, 0, posture);
    let resolver = pgrc2_write::seal::CodecResolver;
    let schema = vec![wcol(1), tcol(2)];
    let mut w = TableWriter::open(
        dir.to_string(),
        schema,
        1663,
        5,
        785,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: 4096, max_bytes: u64::MAX, cut_granule_rows: 1024 },
    )
    .expect("open writer");
    for i in 0..ROWS {
        let (k, s) = row(i);
        let img = img_4b_u(s.as_bytes());
        let datums = [RawDatum::Word(k), RawDatum::Bytes(&img)];
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
    w.publish(&mut vfs, &Probe::new(TxnVerdict::Committed)).expect("publish");
}

fn engine(dir: &str) -> Engine {
    let schema =
        vec![ColMeta::new(1, "k", TypMeta::INT4), ColMeta::new(2, "s", TypMeta::TEXT_C)];
    let bank = Bank::open(dir, schema, &OpenOpts { bankstats: false, threads: 1 });
    Engine::new(bank, SqeConfig { threads: 3, ..SqeConfig::default() })
}

/// The q27 shape: GROUP BY k, AVG(len(s)), COUNT(*) WHERE s <> ''.
fn q27_lines(eng: &Engine, e: AValExpr) -> Vec<String> {
    let ap = APlan {
        q: 27,
        family: AFamily::TwoLevelCodeAgg,
        tags: Vec::new(),
        cols: vec![1, 2],
        pred: Some(APred::NeEmpty { col: 2, fp: None }),
        group: vec![AKeyExpr::Col(1)],
        agg: vec![AAgg::Avg { e }, AAgg::CountStar],
        order: None,
        sortagg_keys: Vec::new(),
        win: None,
        agg_filters: Vec::new(),
        having: None,
        params: Vec::new(),
        fingerprints: Vec::new(),
        kernel_oracle: String::new(),
        notes: "charlen gate".into(),
        flags: Vec::new(),
    };
    let node = plan_from_ap(&eng.bank, &eng.faces, &ap).expect("lower");
    let mut v = to_lines(&eng.run(&node));
    v.sort();
    v
}

/// Scalar oracle: per-key (sum, count) over non-empty values with the
/// given length law, rendered nothing — compared as exact ratios.
fn oracle(len_of: impl Fn(&str) -> u64) -> Vec<(u64, u64, u64)> {
    let mut acc = vec![(0u64, 0u64); KEYS as usize];
    for i in 0..ROWS {
        let (k, s) = row(i);
        if s.is_empty() {
            continue;
        }
        acc[k as usize].0 += len_of(&s);
        acc[k as usize].1 += 1;
    }
    (0..KEYS).map(|k| (k, acc[k as usize].0, acc[k as usize].1)).collect()
}

fn run_case(dict: bool) {
    let dir = std::env::temp_dir()
        .join(format!("sqe_charlen_{}_{}", dict as u8, std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    seal(&dir, dict);
    let eng = engine(&dir);

    let chars = q27_lines(&eng, AValExpr::CharLength { col: 2 });
    let bytes = q27_lines(&eng, AValExpr::OctetLength { col: 2 });
    assert_ne!(
        chars, bytes,
        "multibyte text must split the char and byte kernels (dict={dict})"
    );

    // char oracle: chars(s) counts Unicode scalars; bytes differ on the
    // cyrillic rows. The engine renders avg as an exact numeric ratio —
    // recompute the same ratio text via a second engine run is overkill;
    // instead pin the SUMS through a COUNT-weighted reconstruction: the
    // grouped answer lines are "<k>|<avg>|<count>"; avg * count must
    // equal the oracle sum as an exact rational — verified by rendering
    // the oracle through the same to_lines law is not available here, so
    // compare against the byte-kernel delta: sum_chars = sum_bytes -
    // (extra continuation bytes), a per-key exact quantity.
    let och = oracle(|s| s.chars().count() as u64);
    let oby = oracle(|s| s.len() as u64);
    assert_ne!(och, oby, "row law must actually contain multibyte text");
    // Cross-check totals via a keyless reconstruction: parse counts from
    // the answer lines and confirm the char answer moves exactly by the
    // oracle's char/byte ratio per key (string-compare the avg fields).
    for (cl, bl) in chars.iter().zip(bytes.iter()) {
        let cf: Vec<&str> = cl.split('\t').collect();
        let bf: Vec<&str> = bl.split('\t').collect();
        assert_eq!(cf[0], bf[0], "key alignment");
        assert_eq!(cf[2], bf[2], "counts agree across kernels");
        let k: usize = cf[0].parse().expect("key");
        let cnt: u64 = cf[2].parse().expect("count");
        assert_eq!(cnt, och[k].2, "count vs oracle");
        // exact-avg check: avg field must equal oracle sum / count for
        // each kernel (numeric render: compare via f64 at 1e-9 — counts
        // are ~1500, sums < 2^24, exact in f64).
        let ca: f64 = cf[1].parse().expect("char avg");
        let ba: f64 = bf[1].parse().expect("byte avg");
        let want_c = och[k].1 as f64 / och[k].2 as f64;
        let want_b = oby[k].1 as f64 / oby[k].2 as f64;
        assert!((ca - want_c).abs() < 1e-6, "char avg k={k}: got {ca}, want {want_c}");
        assert!((ba - want_b).abs() < 1e-6, "byte avg k={k}: got {ba}, want {want_b}");
    }
}

/// Dict route: the entry-length tables must carry CHAR counts (the dict
/// index char_len field) under AvgCharLen.
#[test]
fn avg_charlen_dict_entrylen_route() {
    run_case(true);
}

/// Hydrated route (no dict): the per-payload lead-byte walk.
#[test]
fn avg_charlen_hydrated_route() {
    run_case(false);
}
