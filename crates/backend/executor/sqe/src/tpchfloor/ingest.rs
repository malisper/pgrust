//! Seal the generated TPC-H tables into pgrc2 banks through the REAL
//! writer (election, part cut, publish — the join_identity seal idiom).
//! One bank directory per table under `<root>/<table>`.

use super::gen::*;
use super::{fmt_money2, TABLES};
use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{img_4b_u, Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;

/// One ingest cell (the study's RawDatum currency: words + 4B-U text
/// images ONLY — the NUMERIC-image ingest gap is recorded in mod.rs).
pub enum Cell {
    W(i64),
    T(String),
}

fn wcol(attno: u32, width: u8) -> ColSchema {
    ColSchema {
        attno,
        class: StorageClass::ByvalWord { width, signed: true },
        typlen: width as i16,
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

/// Writer-side ColSchema derived from the read-side catalog stand-in.
fn write_schema(table: &str) -> Vec<ColSchema> {
    super::schema_of(table)
        .iter()
        .map(|c| {
            if c.typ.is_varlena() {
                tcol(c.attno)
            } else {
                wcol(c.attno, c.typ.width as u8)
            }
        })
        .collect()
}

/// Seal `nrows` rows produced by `row_fn` into `<dir>` (fresh directory).
fn seal_rows(
    dir: &str,
    table: &str,
    nrows: u64,
    part_rows: u32,
    row_fn: impl Fn(u64, &mut Vec<Cell>),
) {
    if std::path::Path::new(dir).exists() {
        std::fs::remove_dir_all(dir).expect("clear bank dir");
    }
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let mut w = TableWriter::open(
        dir.to_string(),
        write_schema(table),
        1663,
        5,
        777,
        TxnStamp { fxid: 100, cid: 1 },
        &Default::default(),
        PartCutPolicy {
            max_rows: part_rows as u64,
            max_bytes: u64::MAX,
            cut_granule_rows: 8192,
        },
    )
    .expect("open writer");
    let mut cells: Vec<Cell> = Vec::new();
    let mut imgs: Vec<Option<Vec<u8>>> = Vec::new();
    for r in 0..nrows {
        cells.clear();
        row_fn(r, &mut cells);
        imgs.clear();
        for c in &cells {
            imgs.push(match c {
                Cell::T(s) => Some(img_4b_u(s.as_bytes())),
                Cell::W(_) => None,
            });
        }
        let datums: Vec<RawDatum> = cells
            .iter()
            .zip(imgs.iter())
            .map(|(c, img)| match c {
                Cell::W(v) => RawDatum::Word(*v as u64),
                Cell::T(_) => RawDatum::Bytes(img.as_ref().unwrap()),
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
}

/// Ingest every table of `d` under `<root>/<table>`. `part_rows` is the
/// part-cut geometry (262144 for SF1; small for tests).
pub fn ingest_all(root: &str, d: &Tpch, part_rows: u32) {
    for &tname in TABLES.iter() {
        let dir = format!("{root}/{tname}");
        let t0 = std::time::Instant::now();
        match tname {
            "customer" => seal_rows(&dir, tname, d.cust.n as u64, part_rows, |r, out| {
                let i = r as usize;
                let ck = i as u32 + 1;
                let nk = d.cust.nationkey[i];
                out.push(Cell::W(ck as i64));
                out.push(Cell::T(c_name(ck)));
                out.push(Cell::T(address('c', ck)));
                out.push(Cell::W(nk as i64));
                out.push(Cell::T(phone(nk, ck)));
                out.push(Cell::W(d.cust.acctbal[i] as i64));
                out.push(Cell::T(SEGMENTS[d.cust.segment[i] as usize].to_string()));
                out.push(Cell::T(comment('c', r)));
            }),
            "orders" => seal_rows(&dir, tname, d.ord.orderkey.len() as u64, part_rows, |r, out| {
                let i = r as usize;
                out.push(Cell::W(d.ord.orderkey[i]));
                out.push(Cell::W(d.ord.custkey[i] as i64));
                out.push(Cell::T((d.ord.status[i] as char).to_string()));
                out.push(Cell::W(d.ord.totalprice[i]));
                out.push(Cell::W(d.ord.orderdate[i] as i64));
                out.push(Cell::T(PRIORITIES[d.ord.priority[i] as usize].to_string()));
                out.push(Cell::T(clerk_name(d.ord.clerk[i])));
                out.push(Cell::W(0));
                out.push(Cell::T(comment('o', r)));
            }),
            "lineitem" => seal_rows(&dir, tname, d.li.orderkey.len() as u64, part_rows, |r, out| {
                let i = r as usize;
                out.push(Cell::W(d.li.orderkey[i]));
                out.push(Cell::W(d.li.partkey[i] as i64));
                out.push(Cell::W(d.li.suppkey[i] as i64));
                out.push(Cell::W(d.li.linenumber[i] as i64));
                out.push(Cell::W(d.li.quantity_c[i] as i64));
                out.push(Cell::W(d.li.extprice_c[i]));
                out.push(Cell::W(d.li.discount[i] as i64));
                out.push(Cell::W(d.li.tax[i] as i64));
                out.push(Cell::T((d.li.returnflag[i] as char).to_string()));
                out.push(Cell::T((d.li.linestatus[i] as char).to_string()));
                out.push(Cell::W(d.li.shipdate[i] as i64));
                out.push(Cell::W(d.li.commitdate[i] as i64));
                out.push(Cell::W(d.li.receiptdate[i] as i64));
                out.push(Cell::T(INSTRUCT[d.li.instruct[i] as usize].to_string()));
                out.push(Cell::T(MODES[d.li.mode[i] as usize].to_string()));
                out.push(Cell::T(comment('l', r)));
            }),
            "part" => seal_rows(&dir, tname, d.part.n as u64, part_rows, |r, out| {
                let i = r as usize;
                let pk = i as u32 + 1;
                out.push(Cell::W(pk as i64));
                out.push(Cell::T(p_name(d.part.name_words[i])));
                out.push(Cell::T(format!("Manufacturer#{}", d.part.mfgr[i])));
                out.push(Cell::T(format!("Brand#{}", d.part.brand[i])));
                out.push(Cell::T(TYPES[d.part.ptype[i] as usize].to_string()));
                out.push(Cell::W(d.part.size[i] as i64));
                out.push(Cell::T(CONTAINERS[d.part.container[i] as usize].to_string()));
                out.push(Cell::W(d.part.retail_c[i] as i64));
                out.push(Cell::T(comment('p', r)));
            }),
            "partsupp" => seal_rows(&dir, tname, d.ps.partkey.len() as u64, part_rows, |r, out| {
                let i = r as usize;
                out.push(Cell::W(d.ps.partkey[i] as i64));
                out.push(Cell::W(d.ps.suppkey[i] as i64));
                out.push(Cell::W(d.ps.availqty[i] as i64));
                out.push(Cell::W(d.ps.supplycost_c[i] as i64));
                out.push(Cell::T(comment('s', r)));
            }),
            "supplier" => seal_rows(&dir, tname, d.supp.n as u64, part_rows, |r, out| {
                let i = r as usize;
                let sk = i as u32 + 1;
                let nk = d.supp.nationkey[i];
                out.push(Cell::W(sk as i64));
                out.push(Cell::T(s_name(sk)));
                out.push(Cell::T(address('s', sk)));
                out.push(Cell::W(nk as i64));
                out.push(Cell::T(phone(nk, sk)));
                out.push(Cell::W(d.supp.acctbal[i] as i64));
                out.push(Cell::T(comment('u', r)));
            }),
            "nation" => seal_rows(&dir, tname, 25, part_rows, |r, out| {
                let (name, region) = NATIONS[r as usize];
                out.push(Cell::W(r as i64));
                out.push(Cell::T(name.to_string()));
                out.push(Cell::W(region as i64));
                out.push(Cell::T(comment('n', r)));
            }),
            "region" => seal_rows(&dir, tname, 5, part_rows, |r, out| {
                out.push(Cell::W(r as i64));
                out.push(Cell::T(REGIONS[r as usize].to_string()));
                out.push(Cell::T(comment('r', r)));
            }),
            other => panic!("unknown table {other}"),
        }
        eprintln!("INGEST|{tname}|ms={:.0}", t0.elapsed().as_secs_f64() * 1e3);
    }
    // The ONE render-law self-check: money formatting is shared with the
    // oracle; assert the seam once per ingest.
    debug_assert_eq!(fmt_money2(-5), "-0.05");
}
