//! [bulkentries] Fingerprint-plane arm identity: the bulk sequential
//! entry cursor and the per-code point walk must produce BIT-IDENTICAL
//! fp planes over a mixed dictionary (empty entry, short/long payloads,
//! multiple parts, ranges spanning several frames). Real pgrc2_write
//! banks, both `build_fps_arm` arms compared exhaustively.

#![cfg(feature = "rig")]

use std::sync::Arc;

use pgrc2_format::class::{CollationClass, ColSchema, StorageClass, TypeSemantics};
use pgrc2_write::dict::TextSemantics;
use pgrc2_write::elect::{CodecCandidates, ColumnPosture, DictPolicy};
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::publish::TxnVerdict;
use pgrc2_write::testkit::{img_4b_u, Kit, Probe};
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::RealVfs;
use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::engine::DictFace;
use sqe::pool::Pool;
use sqe::stencils::part_merge::build_fps_arm;
use sqe::typmeta::TypMeta;

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

/// Mixed-payload value ladder: the empty string, 1-byte, mid, and long
/// (multi-hundred-byte) entries, scattered across rows.
fn val(j: u64) -> String {
    match j % 4 {
        0 if j == 0 => String::new(),
        0 => format!("k{j}"),
        1 => format!("key_{j:06}"),
        2 => format!("{}_{j:06}", "m".repeat(40)),
        _ => format!("{}_{j:06}", "long".repeat(64)),
    }
}

fn seal_bank(dir: &str, rows: u64, ndv: u64) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let mut vfs = RealVfs;
    let mut kit = Kit::new();
    let cands = CodecCandidates::new(ColumnPosture::default()).with_column(
        1,
        0,
        ColumnPosture {
            dict: Some(DictPolicy {
                ndv_cap: 1 << 16,
                exec_ok: true,
                sem: TextSemantics::Utf8Chars,
            }),
            ..Default::default()
        },
    );
    let resolver = pgrc2_write::seal::CodecResolver;
    let schema = vec![tcol(1)];
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
    for i in 0..rows {
        let j = (i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 13) % ndv;
        let s = val(j);
        let img = img_4b_u(s.as_bytes());
        let datums = [RawDatum::Bytes(&img)];
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
    let probe = Probe::new(TxnVerdict::Committed);
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
    w.publish(&mut vfs, &probe).expect("publish");
}

#[test]
fn bulk_and_point_walk_fp_planes_are_bit_identical() {
    let dir = std::env::temp_dir().join(format!("sqe_dictfp_id_{}", std::process::id()));
    let dir = dir.to_str().unwrap().to_string();
    let _ = std::fs::remove_dir_all(&dir);
    // Multiple 4096-row parts; per-part ncodes spans several 1024-entry
    // frames, so bulk refills cross frame boundaries mid-range.
    const ROWS: u64 = 20_000;
    const NDV: u64 = 6_000;
    seal_bank(&dir, ROWS, NDV);
    let schema = vec![ColMeta::new(1, "s", TypMeta::TEXT_C)];
    let bank = Bank::open(&dir, schema, &OpenOpts { bankstats: false, threads: 0 });
    assert!(bank.parts.len() > 1, "multi-part bank expected");
    let mut pf: Vec<Arc<DictFace>> = Vec::new();
    for pi in 0..bank.parts.len() {
        assert!(sqe::scan::is_dict(&bank, pi, 1), "dict posture expected on every part");
        let dh = sqe::scan::dict_handle(&bank, pi, 1);
        let n = dh.ncodes();
        assert!(n > 1024, "part dict must span several frames (got {n})");
        pf.push(Arc::new(DictFace { dh: Some(Arc::new(dh)), ncodes: n, empty_code: None }));
    }
    let pool = Pool::new(4);
    let point = build_fps_arm(&pool, &pf, false);
    let bulk = build_fps_arm(&pool, &pf, true);
    assert_eq!(point.len(), bulk.len());
    for (pi, (a, b)) in point.iter().zip(bulk.iter()).enumerate() {
        assert_eq!(a.len(), b.len(), "part {pi} plane length");
        for (c, (x, y)) in a.iter().zip(b.iter()).enumerate() {
            assert_eq!(x, y, "part {pi} code {c}: fp mismatch across arms");
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}
