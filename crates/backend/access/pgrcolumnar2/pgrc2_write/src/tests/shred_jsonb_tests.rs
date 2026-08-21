//! TY-3 JsonbShred writer wiring (lanev4; OD-4 two-arm election): the
//! adt_jsonb_shred-backed [`JsonbShredSource`] derives typed lanes for
//! shreddable jsonb columns at seal, the parent's election witness records
//! the STRUCTURAL JsonbShred election, and non-shreddable shapes fall to
//! the text plane. Corpus-shaped feeds: flat homogeneous objects
//! (jb_flat_homog: `jsonb_shred` MUST), scalar roots (text plane), all-null
//! (jb_allnull: `verbatim` MUST). The image lane stays the read truth and
//! round-trips byte-exactly (dual-store law, O-4).

use super::*;
use crate::shred_jsonb::JsonbShredSource;
use mcx::MemoryContext;
use pgrc2_format::abi::{ByteArena, DecodeOut, ValidityVerdict};
use pgrc2_format::enc::EncodingId;
use std::sync::Arc;
use std::sync::Once;

fn setup() {
    let _ = mbutils::SetDatabaseEncoding(wchar::PG_UTF8);
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        mbutils::init_seams();
    });
}

/// jb_flat_homog-shaped docs: flat homogeneous objects, uniform-dscale
/// numerics — the shred MUST arm.
fn flat_doc(i: u64) -> String {
    format!(
        r#"{{"active":{},"id":{},"name":"tok{}","score":{}.25}}"#,
        i % 2 == 0,
        i & 0x7FFF_FFFF,
        i % 97,
        i % 1000
    )
}

/// jb_scalar_roots-shaped docs: non-object roots — nothing electable.
fn scalar_doc(i: u64) -> String {
    match i % 5 {
        0 => "1".to_string(),
        1 => "\"x\"".to_string(),
        2 => "true".to_string(),
        3 => "null".to_string(),
        _ => "12.5".to_string(),
    }
}

/// Feed `docs` through jsonb_in and the writer with the real shred source;
/// return (part bytes, witness encoding, original images).
fn seal_jsonb(docs: &[Option<String>]) -> (Vec<u8>, u16, Vec<Option<Vec<u8>>>) {
    setup();
    let ctx = MemoryContext::new("shred-jsonb-test");
    let mcx = ctx.mcx();
    let images: Vec<Option<Vec<u8>>> = docs
        .iter()
        .map(|d| {
            d.as_ref().map(|d| {
                adt_jsonb::io::jsonb_in(mcx, d.as_bytes(), None)
                    .unwrap_or_else(|e| panic!("jsonb_in {d:?}: {}", e.message()))
                    .expect("hard path")[..]
                    .to_vec()
            })
        })
        .collect();

    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let mut shred = JsonbShredSource::new().with_column(1);
    let mut w = open_writer(vec![text_col(1)], stamp(41, 1));
    for img in &images {
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut shred,
            shred_opts: &kit.opts,
        };
        let d = match img {
            None => RawDatum::Null,
            Some(b) => RawDatum::Bytes(b),
        };
        w.append_row(&[d], &mut kit.ext, &mut env).expect("append");
    }
    let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
    let mut env = SealEnv {
        vfs: &mut vfs,
        sources: &sources,
        resolver: &kit.resolver,
        shred: &mut shred,
        shred_opts: &kit.opts,
    };
    w.finish(&mut env).expect("finish");
    let probe = Probe::new(TxnVerdict::InProgress).set(41, TxnVerdict::Committed);
    w.publish(&mut vfs, &probe).expect("publish");
    let witness = w
        .seal_reports()
        .iter()
        .flat_map(|r| r.elections.iter())
        .find(|e| e.attno == 1 && e.path_ord == 0)
        .copied()
        .expect("witness");
    let bytes = vfs
        .read_full(&format!("{DIR}/part-0.pgrc2"))
        .expect("part bytes");
    (bytes, witness.encoding, images)
}

#[test]
fn flat_homogeneous_jsonb_records_the_shred_election_with_real_lanes() {
    let docs: Vec<Option<String>> = (0..400u64)
        .map(|i| if i % 13 == 5 { None } else { Some(flat_doc(i)) })
        .collect();
    let (bytes, witness, images) = seal_jsonb(&docs);
    // The corpus assertion surface: jb_flat_homog MUST report jsonb_shred.
    assert_eq!(witness, EncodingId::JsonbShred.as_u16());

    let pv = PartView { bytes };
    // The PathTable maps the derived lanes (flat_doc has 4 scalar paths;
    // election order = canonical path order).
    let pt = pv
        .section_bytes(SectionKind::PathTable, 0, 0)
        .expect("PathTable");
    let paths = crate::shred::decode_path_table(pt).expect("paths");
    assert_eq!(
        paths,
        vec![
            "id".to_string(),
            "name".to_string(),
            "score".to_string(),
            "active".to_string(),
        ],
        "canonical (length-then-bytes) path order"
    );
    // Typed lanes ride as path_ord != 0 substreams, row-aligned.
    for ord in 1..=4u32 {
        let (lane, _) = pv.stream(1, ord, StreamRole::Values).expect("lane entry");
        assert_eq!(lane.values, 400, "lane {ord} row-aligned");
    }
    // The RULED scale slot (2026-08-14): NUMERIC lanes carry their
    // chunk-shared scale in aux32 under STREAMF_LANE_SCALE — "id" (ints,
    // scale 0) and "score" (uniform 2-decimal, scale 2); non-numeric
    // lanes ("name" text, "active" bool) never set the flag.
    use pgrc2_format::part::STREAMF_LANE_SCALE;
    let (id_lane, _) = pv.stream(1, 1, StreamRole::Values).expect("id lane");
    assert_ne!(id_lane.flags & STREAMF_LANE_SCALE, 0, "id lane carries the slot");
    assert_eq!(id_lane.aux32, 0, "id lane scale 0 (the int class)");
    let (score_lane, _) = pv.stream(1, 3, StreamRole::Values).expect("score lane");
    assert_ne!(score_lane.flags & STREAMF_LANE_SCALE, 0, "score lane carries the slot");
    assert_eq!(score_lane.aux32, 2, "score lane scale 2");
    for ord in [2u32, 4] {
        let (l, _) = pv.stream(1, ord, StreamRole::Values).expect("lane");
        assert_eq!(l.flags & STREAMF_LANE_SCALE, 0, "non-numeric lane {ord} flagless");
    }
    // The IMAGE lane keeps its ordinary text-plane encoding ON DISK — the
    // structural id never lands in a stream entry (spec §4).
    let (parent, _) = pv.stream(1, 0, StreamRole::Values).expect("image lane");
    assert_eq!(parent.encoding, EncodingId::Verbatim.as_u16());

    // Image-lane round-trip through the real reader: byte-exact documents.
    let part = Arc::new(
        pgrc2_read::openpart::OpenPart::open(
            Box::new(pgrc2_read::io::MemPartIo::new(pv.bytes.clone(), 7, 901)),
            &pgrc2_read::openpart::PartExpect::none(),
        )
        .expect("open"),
    );
    let binding = pgrc2_read::cursor::reference_binding_leaked();
    let mut cur =
        pgrc2_read::cursor::StreamCursor::open(Arc::clone(&part), binding, 1, 0).expect("cursor");
    let mut row = 0usize;
    for g in 0..cur.granule_count() {
        let rows_g = cur.rows_in_granule(g) as usize;
        let mut datums = vec![0u64; rows_g];
        // 8-aligned arena backing (abi.rs §19.4 base-alignment law).
        let mut arena_buf = ArenaBuf::new(1 << 20);
        let mut out = DecodeOut {
            datums: &mut datums,
            arena: ByteArena::new(arena_buf.bytes_mut()),
        };
        cur.decode_full(g, &mut out).expect("decode");
        let mut vwords = vec![0u64; rows_g.div_ceil(64).max(1)];
        let verdict = cur.validity(g, &mut vwords).expect("validity");
        for r in 0..rows_g {
            let valid = match verdict {
                ValidityVerdict::AllValid => true,
                ValidityVerdict::Mixed { .. } => (vwords[r / 64] >> (r % 64)) & 1 == 1,
            };
            match (&images[row], valid) {
                (None, v) => assert!(!v, "row {row} must be null"),
                (Some(img), v) => {
                    assert!(v, "row {row} must be valid");
                    let d = datums[r];
                    let hdr = u32::from_le_bytes(unsafe { *(d as *const [u8; 4]) });
                    let total = (hdr >> 2) as usize;
                    let got =
                        unsafe { std::slice::from_raw_parts(d as *const u8, total) };
                    assert_eq!(got, img.as_slice(), "row {row} image byte-exact");
                }
            }
            row += 1;
        }
    }
    assert_eq!(row, 400);
}

#[test]
fn scalar_roots_fall_to_the_text_plane() {
    let docs: Vec<Option<String>> = (0..300u64).map(|i| Some(scalar_doc(i))).collect();
    let (bytes, witness, _) = seal_jsonb(&docs);
    assert_eq!(witness, EncodingId::Verbatim.as_u16(), "text plane");
    let pv = PartView { bytes };
    assert!(
        pv.section_bytes(SectionKind::PathTable, 0, 0).is_none(),
        "no lanes derived"
    );
}

#[test]
fn all_null_jsonb_stays_on_text_plane() {
    let docs: Vec<Option<String>> = (0..200u64).map(|_| None).collect();
    let (bytes, witness, _) = seal_jsonb(&docs);
    // jb_allnull: NO shred lanes; the TEXT PLANE carries it. The plane's
    // election is CONST's ALL_NULL record (the exact-stat election — the
    // same adjudication as pgrc2_write tests/election.rs
    // all_null_column_elects_const_all_null; QA corpus v1.1 widened the
    // manifest cell to {const, verbatim} accordingly).
    assert_eq!(
        witness,
        EncodingId::Const.as_u16(),
        "jb_allnull: text plane via CONST ALL_NULL (never a shred witness)"
    );
    let pv = PartView { bytes };
    assert!(pv.section_bytes(SectionKind::PathTable, 0, 0).is_none());
}

/// The M3-I provider face (`ShredProvider for JsonbShredSource`): a
/// permuted-claim parallel seal over a shreddable jsonb column is
/// byte-identical to the serial seal with the SAME registered source —
/// the crate-grain twin of the QA battery's `l2.dirsha.{jsonb_family,
/// jb_widerow}` cells (formerly recorded SKIPs: no provider existed that
/// minted the jsonb source per part-assembly task).
#[test]
fn parallel_jsonb_shred_parts_byte_identical_to_serial() {
    setup();
    const CHUNK: u32 = 128;
    const N: u64 = 600; // 4 full chunks + tail; max_rows 256 ⇒ 3 parts
    // The #560 partition law: chunk grain == cut granule (the engine
    // refuses anything else), and the SERIAL side must run the same policy
    // for the partitions to coincide. 3 parts ⇒ the provider mints a fresh
    // source per part-assembly task, non-vacuously.
    let policy = crate::writer::PartCutPolicy {
        max_rows: 256,
        max_bytes: u64::MAX,
        cut_granule_rows: CHUNK,
    };
    let ctx = MemoryContext::new("shred-jsonb-par-test");
    let mcx = ctx.mcx();
    let images: Vec<Option<Vec<u8>>> = (0..N)
        .map(|i| {
            if i % 13 == 5 {
                None
            } else {
                let d = flat_doc(i);
                Some(
                    adt_jsonb::io::jsonb_in(mcx, d.as_bytes(), None)
                        .unwrap_or_else(|e| panic!("jsonb_in: {}", e.message()))
                        .expect("hard path")[..]
                        .to_vec(),
                )
            }
        })
        .collect();

    // Serial: the registered source through the one frozen seal face.
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let mut shred = JsonbShredSource::new().with_column(1);
    let mut w = open_writer_policy(vec![text_col(1)], stamp(41, 0), policy);
    for img in &images {
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut shred,
            shred_opts: &kit.opts,
        };
        let d = match img {
            None => RawDatum::Null,
            Some(b) => RawDatum::Bytes(b),
        };
        w.append_row(&[d], &mut kit.ext, &mut env).expect("append");
    }
    {
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut shred,
            shred_opts: &kit.opts,
        };
        w.finish(&mut env).expect("finish");
    }
    let serial: Vec<Vec<u8>> = w
        .sealed_parts()
        .iter()
        .map(|p| vfs.read_full(&format!("{DIR}/{}", p.tmp_name)).expect("serial tmp"))
        .collect();

    // Parallel: the SAME source minted per part-assembly task through the
    // provider face, chunks claimed in REVERSE order.
    let shared = shared_mem_with_dir();
    let mut providers = par_providers(&shared);
    providers.shred = Arc::new(JsonbShredSource::new().with_column(1));
    let schema = vec![text_col(1)];
    let spec = crate::seal::PartSpec {
        spc: SPC,
        db: DB,
        relfilenumber: RELFILENUMBER,
        schema_fingerprint: pgrc2_format::ident::schema_fingerprint(&schema),
    };
    let engine = crate::par::ParEngine::new(
        providers,
        schema,
        spec,
        DIR.to_string(),
        41,
        policy,
        crate::par::ParIngestOpts {
            chunk_rows: CHUNK,
            max_chunks_in_flight: 1024,
            max_parts_in_flight: 1024,
        },
        0,
    )
    .expect("engine");
    let mut chunks: Vec<crate::par::RowChunk> = Vec::new();
    let mut cur = crate::par::RowChunk::new(1, CHUNK);
    for img in &images {
        let d = match img {
            None => RawDatum::Null,
            Some(b) => RawDatum::Bytes(b),
        };
        cur.push_row(&[d]).expect("capture");
        if cur.rows() >= CHUNK {
            chunks.push(std::mem::replace(&mut cur, crate::par::RowChunk::new(1, CHUNK)));
        }
    }
    if cur.rows() > 0 {
        chunks.push(cur);
    }
    let total = chunks.len() as u64;
    let n = chunks.len();
    for (id, ch) in chunks.into_iter().enumerate() {
        engine.publish_chunk(ch, id as u64).expect("publish");
    }
    for id in (0..n).rev() {
        engine.run_chunk(id % 3, id as u64);
    }
    engine.close_input(total);
    let sealed = engine.collect().expect("collect");
    assert!(
        sealed
            .iter()
            .flat_map(|(_, r)| r.elections.iter())
            .any(|e| e.encoding == EncodingId::JsonbShred.as_u16()),
        "parallel seal must witness the JsonbShred election (non-vacuous)"
    );
    let par: Vec<Vec<u8>> = sealed
        .iter()
        .map(|(p, _)| {
            shared.with(|v| v.read_full(&format!("{DIR}/{}", p.tmp_name)).expect("par tmp"))
        })
        .collect();
    assert_eq!(serial.len(), 3, "serial partition: 256+256+88");
    assert_eq!(serial.len(), par.len(), "part partition identical");
    for (i, (a, b)) in serial.iter().zip(&par).enumerate() {
        assert!(a == b, "part {i}: serial vs parallel bytes diverge");
    }
}
