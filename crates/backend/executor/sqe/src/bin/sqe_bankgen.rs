//! sqe_bankgen <bankdir> --rows N [--part-rows N] [--part-bytes MB]
//! Seal a synthetic hits-schema bank (105 columns) through the REAL
//! writer at CI-like part geometry — the cold-open diagnosis bank.
//! Scratch tooling (rig grain), never engine code.

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{img_4b_u, Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("usage: sqe_bankgen <bankdir> --rows N [--part-rows N] [--part-bytes MB]");
        std::process::exit(2);
    }
    let dir = args[0].clone();
    let mut rows: u64 = 1_000_000;
    let mut part_rows: u64 = 1 << 20;
    let mut part_bytes: u64 = 256 << 20;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--rows" => {
                i += 1;
                rows = args[i].parse().unwrap();
            }
            "--part-rows" => {
                i += 1;
                part_rows = args[i].parse().unwrap();
            }
            "--part-bytes" => {
                i += 1;
                part_bytes = args[i].parse::<u64>().unwrap() << 20;
            }
            other => panic!("unknown flag {other}"),
        }
        i += 1;
    }
    let cats = sqe::rig::hits_schema();
    let schema: Vec<ColSchema> = cats
        .iter()
        .map(|c| {
            if c.typ.is_varlena() {
                ColSchema {
                    attno: c.attno,
                    class: StorageClass::VarlenaVerbatim,
                    typlen: -1,
                    typbyval: false,
                    typalign: b'i',
                    collation_class: CollationClass::C,
                    semantics: TypeSemantics::TextCollated,
                }
            } else {
                ColSchema {
                    attno: c.attno,
                    class: StorageClass::ByvalWord { width: c.typ.width as u8, signed: true },
                    typlen: c.typ.width as i16,
                    typbyval: true,
                    typalign: if c.typ.width == 8 { b'd' } else { b'i' },
                    collation_class: CollationClass::C,
                    semantics: TypeSemantics::SignedInt,
                }
            }
        })
        .collect();
    if std::path::Path::new(&dir).exists() {
        std::fs::remove_dir_all(&dir).expect("clear bank dir");
    }
    std::fs::create_dir_all(&dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let mut w = TableWriter::open(
        dir.clone(),
        schema,
        1663,
        5,
        777,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy { max_rows: part_rows, max_bytes: part_bytes, cut_granule_rows: 8192 },
    )
    .expect("open writer");
    let t0 = std::time::Instant::now();
    // Column content classes by attno position: mix of low-cardinality
    // dict text, URL-like higher-cardinality text, and word columns with
    // varied domains — a hits-shaped byte diet, not hits itself.
    for r in 0..rows {
        let mut imgs: Vec<Option<Vec<u8>>> = vec![None; cats.len()];
        let mut words: Vec<u64> = vec![0; cats.len()];
        for (ci, c) in cats.iter().enumerate() {
            let h = mix(r, c.attno as u64);
            if c.typ.is_varlena() {
                let s = match c.attno % 5 {
                    // URL-ish high-cardinality strings.
                    0 => format!("http://example.test/{}/{}", c.attno, h % (rows / 8 + 1)),
                    // Mid-cardinality tokens.
                    1 => format!("tok{}_{}", c.attno, h % 4096),
                    // Low-cardinality dict darlings.
                    2 => format!("v{}", h % 23),
                    // Empty-heavy.
                    3 => {
                        if h % 4 == 0 {
                            String::new()
                        } else {
                            format!("s{}", h % 100)
                        }
                    }
                    _ => format!("phrase {} {} {}", h % 17, h % 29, h % 51),
                };
                imgs[ci] = Some(img_4b_u(s.as_bytes()));
            } else {
                let dom: u64 = match c.attno % 7 {
                    0 => 2,
                    1 => 100,
                    2 => 1 << 16,
                    3 => rows / 4 + 1,
                    4 => 1 << 31,
                    5 => 16,
                    _ => 1000,
                };
                let mut v = (h % dom) as i64;
                if c.typ.width == 2 {
                    v %= 1 << 15;
                }
                if c.typ.width == 4 {
                    v %= 1 << 31;
                }
                words[ci] = v as u64;
            }
        }
        let datums: Vec<RawDatum> = cats
            .iter()
            .enumerate()
            .map(|(ci, c)| {
                if c.typ.is_varlena() {
                    RawDatum::Bytes(imgs[ci].as_ref().unwrap())
                } else {
                    RawDatum::Word(words[ci])
                }
            })
            .collect();
        let sources: [&dyn pgrc2_write::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.append_row(&datums, &mut kit.ext, &mut env).expect("append");
        if r % 1_000_000 == 999_999 {
            eprintln!("rows={} elapsed_s={:.0}", r + 1, t0.elapsed().as_secs_f64());
        }
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
    let probe = Probe::new(TxnVerdict::Committed);
    w.publish(&mut vfs, &probe).expect("publish");
    eprintln!("SEALED|dir={dir}|rows={rows}|s={:.0}", t0.elapsed().as_secs_f64());
}

fn mix(a: u64, b: u64) -> u64 {
    let mut x = a
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(b.wrapping_mul(0xC2B2_AE3D_27D4_EB4F));
    x ^= x >> 29;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 32;
    x
}
