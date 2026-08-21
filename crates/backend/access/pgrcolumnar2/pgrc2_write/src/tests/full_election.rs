//! Full-registry election wiring (A-lane amendment M3-A2): the goldens for
//! elected outcomes on representative columns, and the cross-crate proofs
//! that a full-election part READS BACK through `pgrc2_read` + the codec
//! registry — including the dict width-byte resolution seam (M3-G gap 1):
//! the value stream's §6.3 width byte carries the max code width, the raw
//! entry fields can NOT resolve a kernel (tooth), and the normalized
//! `stream_kernel_key` path decodes byte-exact (the fix, proven end-to-end).

use super::*;
use crate::elect::{CodecCandidates, ColumnPosture, DictPolicy};
use crate::dict::TextSemantics;
use crate::publish::TxnVerdict;
use crate::seal::CodecResolver;
use pgrc2_format::abi::{datum_canonical_bytes, ByteArena, DecodeOut, KernelKey};
use pgrc2_format::enc::{EncodingId, Wrapper};
use pgrc2_format::part::{StreamSectionHdr, STREAMF_DICT_EXEC};
use pgrc2_format::{FormatError, FormatResult};
use pgrc2_read::io::MemPartIo;
use pgrc2_read::{CodecBinding, OpenPart, PartExpect, SectionUnwrapper, StreamCursor};
use std::sync::Arc;

fn splitmix(seed: &mut u64) -> u64 {
    *seed = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *seed;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// The LZ4 section unwrapper adapter (the lx_source `pgrc_codec_binding`
/// shape, rebuilt here because dep direction keeps it out of pgrc2_codec).
struct Lz4Unwrap;

impl SectionUnwrapper for Lz4Unwrap {
    fn wrapper(&self) -> Wrapper {
        Wrapper::Lz4
    }
    fn unwrap_section(&self, _hdr: &StreamSectionHdr, section: &[u8]) -> FormatResult<Vec<u8>> {
        let mut out = Vec::new();
        pgrc2_codec::wrapper::unwrap_section(section, &mut out)?;
        Ok(out)
    }
}

static LZ4_UNWRAP: Lz4Unwrap = Lz4Unwrap;

/// The Zstd twin (the CMP-A slot-fill; same codec entry point).
struct ZstdUnwrap;

impl SectionUnwrapper for ZstdUnwrap {
    fn wrapper(&self) -> Wrapper {
        Wrapper::Zstd
    }
    fn unwrap_section(&self, _hdr: &StreamSectionHdr, section: &[u8]) -> FormatResult<Vec<u8>> {
        let mut out = Vec::new();
        pgrc2_codec::wrapper::unwrap_section(section, &mut out)?;
        Ok(out)
    }
}

static ZSTD_UNWRAP: ZstdUnwrap = ZstdUnwrap;

fn codec_binding() -> CodecBinding<'static> {
    static UNWRAPPERS: [&dyn SectionUnwrapper; 2] = [&LZ4_UNWRAP, &ZSTD_UNWRAP];
    CodecBinding {
        registry: pgrc2_codec::registry(),
        unwrappers: &UNWRAPPERS,
    }
}

/// Open a published part's bytes through the REAL reader.
fn open_part(vfs: &mut MemVfs, name: &str) -> Arc<OpenPart> {
    let bytes = vfs.read_full(&format!("{DIR}/{name}")).expect("part bytes");
    let io = MemPartIo::new(bytes, 4242, 7);
    Arc::new(OpenPart::open(Box::new(io), &PartExpect::none()).expect("open part"))
}

/// Decode granule `g` fully; return (datums, arena) with the arena kept
/// alive so pointer datums stay valid.
fn decode_granule(cur: &mut StreamCursor<'_>, g: u32) -> (Vec<u64>, Vec<u8>) {
    let n = cur.values_in_granule(g).expect("values") as usize;
    let mut datums = vec![0u64; n];
    let mut arena_buf = vec![0u8; 4 << 20];
    let wrote = {
        let mut out = DecodeOut {
            datums: &mut datums,
            arena: ByteArena::new(&mut arena_buf),
        };
        cur.decode_full(g, &mut out).expect("decode_full")
    };
    assert_eq!(wrote as usize, n);
    (datums, arena_buf)
}

fn assert_varlena_datum(datum: u64, want_payload: &[u8]) {
    let mut scratch = [0u8; 8];
    // SAFETY: decode arena is live in the caller.
    let got = unsafe {
        datum_canonical_bytes(StorageClass::VarlenaVerbatim, datum, &mut scratch)
    }
    .expect("canonical bytes");
    assert_eq!(got, want_payload, "varlena payload mismatch");
}

struct FullKit {
    cands: CodecCandidates,
    resolver: CodecResolver,
    shred: crate::shred::NoShred,
    opts: ShredOptions,
    ext: crate::ingest::NoExternalDetoast,
}

impl FullKit {
    fn new(cands: CodecCandidates) -> FullKit {
        FullKit {
            cands,
            resolver: CodecResolver,
            shred: crate::shred::NoShred,
            opts: ShredOptions::default(),
            ext: crate::ingest::NoExternalDetoast,
        }
    }
}

fn f64_col(attno: u32) -> ColSchema {
    ColSchema {
        attno,
        class: StorageClass::F64,
        typlen: 8,
        typbyval: true,
        typalign: b'd',
        collation_class: CollationClass::C,
        semantics: TypeSemantics::Float,
    }
}

/// Decimal string with exactly `scale` fraction digits (the codec suite's
/// shape; `numeric_in` yields dscale == scale).
fn decimal_string(mant: i64, scale: i32) -> String {
    let neg = mant < 0;
    let mut a = mant.unsigned_abs().to_string();
    let s = scale as usize;
    if a.len() <= s {
        a = format!("{}{a}", "0".repeat(s + 1 - a.len()));
    }
    let dot = a.len() - s;
    let body = if s == 0 {
        a
    } else {
        format!("{}.{}", &a[..dot], &a[dot..])
    };
    if neg {
        format!("-{body}")
    } else {
        body
    }
}

fn publish(w: &mut TableWriter, vfs: &mut MemVfs, kit: &mut FullKit, fxid: u64) {
    let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
    let mut env = SealEnv {
        vfs,
        sources: &sources,
        resolver: &kit.resolver,
        shred: &mut kit.shred,
        shred_opts: &kit.opts,
    };
    w.finish(&mut env).expect("finish");
    let probe = Probe::new(TxnVerdict::InProgress).set(fxid, TxnVerdict::Committed);
    w.publish(vfs, &probe).expect("publish");
}

// ---------------------------------------------------------------------------
// election goldens on representative columns
// ---------------------------------------------------------------------------

#[test]
fn full_registry_election_goldens() {
    let mut vfs = mem_with_dir();
    let cands = CodecCandidates::new(ColumnPosture::default())
        .with_column(
            4,
            0,
            ColumnPosture {
                dict: Some(DictPolicy {
                    ndv_cap: 10_000,
                    exec_ok: true,
                    sem: TextSemantics::BytesOnly,
                }),
                ..ColumnPosture::default()
            },
        )
        .with_column(
            5,
            0,
            ColumnPosture {
                numeric: true,
                ..ColumnPosture::default()
            },
        );
    let mut kit = FullKit::new(cands);
    let schema = vec![
        int8_col(1),
        f64_col(2),
        bool_col(3),
        text_col(4),
        text_col(5), // numeric images ride the varlena class
        text_col(6), // incompressible bytea-like payloads
    ];
    let mut w = open_writer(schema, stamp(91, 1));
    let mut seed = 0xE1EC_7104_u64;
    let rows = 10_000u64;
    for i in 0..rows {
        let text_img = img_4b_u(format!("k{:03}", i % 97).as_bytes());
        let num_s = decimal_string((i % 5000) as i64 - 2500, 2);
        let num_img = adt_numeric::io::numeric_in(&num_s, -1, None)
            .expect("parse")
            .expect("non-soft")
            .as_bytes()
            .to_vec();
        let mut rand_payload = [0u8; 32];
        for chunk in rand_payload.chunks_mut(8) {
            chunk.copy_from_slice(&splitmix(&mut seed).to_le_bytes());
        }
        let rand_img = img_4b_u(&rand_payload);
        let images = [text_img, num_img, rand_img];
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        let row = [
            RawDatum::Word(i),                                     // 1: seq int8
            RawDatum::Word(((i % 1000) as f64 / 100.0).to_bits()), // 2: decimal f64
            RawDatum::Word((i % 3 == 0) as u64),                   // 3: bool
            RawDatum::Bytes(&images[0]),                           // 4: dict text
            RawDatum::Bytes(&images[1]),                           // 5: numeric
            RawDatum::Bytes(&images[2]),                           // 6: incompressible
        ];
        w.append_row(&row, &mut kit.ext, &mut env).expect("append");
    }
    publish(&mut w, &mut vfs, &mut kit, 91);

    // The elected outcomes, GOLDEN (attno order).
    let r = &w.seal_reports()[0];
    let encodings: Vec<(u32, u16)> = r
        .elections
        .iter()
        .map(|e| (e.attno, e.encoding))
        .collect();
    assert_eq!(
        encodings,
        vec![
            (1, EncodingId::ByteFor.as_u16()),
            (2, EncodingId::Alp.as_u16()),
            (3, EncodingId::BoolBitmap.as_u16()),
            (4, EncodingId::DictCodes.as_u16()),
            (5, EncodingId::PackedNumeric.as_u16()),
            (6, EncodingId::Verbatim.as_u16()),
        ],
        "elected encodings diverged from the golden"
    );
    // Every winner cleared the ≥10% gate; the demotion kept the baseline.
    for e in &r.elections {
        if e.encoding == EncodingId::Verbatim.as_u16() {
            assert_eq!(e.chosen_len, e.baseline_len);
        } else {
            assert!(e.chosen_len * 10 <= e.baseline_len * 9, "gate breached");
        }
    }
    // verify ran: 6 value streams × 2 granules.
    assert_eq!(r.granules_verified, 12);

    // Stream-entry facts, GOLDEN (the §6.3 vocabulary the reader consumes).
    let pv = PartView::open(&mut vfs, "part-0.pgrc2");
    let (e1, _) = pv.stream(1, 0, StreamRole::Values).expect("col1");
    assert_eq!((e1.encoding, e1.width), (EncodingId::ByteFor.as_u16(), 2));
    let (e2, _) = pv.stream(2, 0, StreamRole::Values).expect("col2");
    assert_eq!((e2.encoding, e2.width), (EncodingId::Alp.as_u16(), 0));
    let (e3, _) = pv.stream(3, 0, StreamRole::Values).expect("col3");
    assert_eq!((e3.encoding, e3.width), (EncodingId::BoolBitmap.as_u16(), 0));
    let (e4, _) = pv.stream(4, 0, StreamRole::Values).expect("col4");
    // 97 NDV bit-packs at 7; the pgrc21-widths byte-align default
    // (PGRUST_PGRC2_CODE_WIDTH_BYTE, ON since 2026-08-16) rounds the
    // granule code width up to the byte boundary — golden is 8.
    assert_eq!((e4.encoding, e4.width), (EncodingId::DictCodes.as_u16(), 8));
    assert_ne!(e4.flags & STREAMF_DICT_EXEC, 0, "zero-null dict lane must publish");
    let (e5, _) = pv.stream(5, 0, StreamRole::Values).expect("col5");
    assert_eq!(
        (e5.encoding, e5.width, e5.aux32),
        (EncodingId::PackedNumeric.as_u16(), 2, 2),
        "PACKED_NUMERIC width/scale golden"
    );
    let (e6, _) = pv.stream(6, 0, StreamRole::Values).expect("col6");
    assert_eq!((e6.encoding, e6.width), (EncodingId::Verbatim.as_u16(), 0));
    // Dict companion streams exist, DICT_CODES-stamped. 97 entries = one
    // dict frame, so the SB-7 frame-cut payload extent table has exactly
    // one extent here (multi-frame geometry is pinned by
    // `dict_payload_frame_extents_cut_and_read_back`).
    let (di, di_ext) = pv.stream(4, 0, StreamRole::DictIndex).expect("DictIndex");
    let (dp, dp_ext) = pv.stream(4, 0, StreamRole::DictPayload).expect("DictPayload");
    assert_eq!(di.encoding, EncodingId::DictCodes.as_u16());
    assert_eq!(dp.encoding, EncodingId::DictCodes.as_u16());
    assert_eq!((di.values, dp.values), (97, 97));
    assert_eq!((di_ext.len(), dp_ext.len()), (1, 1), "one frame = one extent");
}

#[test]
fn dict_above_cap_demotes_to_verbatim() {
    let mut vfs = mem_with_dir();
    let cands = CodecCandidates::new(ColumnPosture {
        dict: Some(DictPolicy {
            ndv_cap: 10, // 97 distinct values > 10
            exec_ok: true,
            sem: TextSemantics::BytesOnly,
        }),
        ..ColumnPosture::default()
    });
    let mut kit = FullKit::new(cands);
    let mut w = open_writer(vec![text_col(1)], stamp(92, 1));
    for i in 0..9_000u64 {
        let img = img_4b_u(format!("k{:03}", i % 97).as_bytes());
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.append_row(&[RawDatum::Bytes(&img)], &mut kit.ext, &mut env)
            .expect("append");
    }
    publish(&mut w, &mut vfs, &mut kit, 92);
    let r = &w.seal_reports()[0];
    assert_eq!(r.elections[0].encoding, EncodingId::Verbatim.as_u16());
    let pv = PartView::open(&mut vfs, "part-0.pgrc2");
    assert!(pv.stream(1, 0, StreamRole::DictIndex).is_none());
}

// ---------------------------------------------------------------------------
// FSST-UNLOCK teeth: the Utf8Chars posture is a CLAIM the election verifies
// per part — verified, never trusted.
// ---------------------------------------------------------------------------

/// A verified Utf8Chars claim on a dict-loser (cap-breach, url-shaped,
/// FSST-compressible) column reaches the SB-4 FSST arm and elects it,
/// with NO degrade note.
#[test]
fn utf8_verified_claim_reaches_the_fsst_arm() {
    let mut vfs = mem_with_dir();
    let cands = CodecCandidates::new(ColumnPosture {
        dict: Some(DictPolicy {
            ndv_cap: 10, // near-unique url class: cap breach demotes dict
            exec_ok: true,
            sem: TextSemantics::Utf8Chars,
        }),
        ..ColumnPosture::default()
    });
    let mut kit = FullKit::new(cands);
    let mut w = open_writer(vec![text_col(1)], stamp(93, 1));
    for i in 0..9_000u64 {
        // Valid UTF-8 (multibyte included), near-unique, heavily
        // FSST-compressible (long shared structure).
        let v = format!(
            "https://static.example-cdn.example/assets/catalog/item-{i:07}/image-größe-large.jpeg"
        );
        let img = img_4b_u(v.as_bytes());
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.append_row(&[RawDatum::Bytes(&img)], &mut kit.ext, &mut env)
            .expect("append");
    }
    publish(&mut w, &mut vfs, &mut kit, 93);
    let r = &w.seal_reports()[0];
    assert_eq!(
        r.elections[0].encoding,
        EncodingId::Fsst.as_u16(),
        "verified utf8 claim + dict cap breach must reach the FSST arm"
    );
    assert!(r.notes.is_empty(), "no degrade note on a verified claim");
}

/// The SAME shape with ONE invalid-UTF-8 payload: the claim fails byte
/// verification, the stream runs BytesOnly (FSST stays gated — verbatim),
/// ingest does NOT fail, and the typed degrade note rides the report.
#[test]
fn utf8_claim_degrade_is_typed_and_keeps_fsst_gated() {
    let mut vfs = mem_with_dir();
    let cands = CodecCandidates::new(ColumnPosture {
        dict: Some(DictPolicy {
            ndv_cap: 10,
            exec_ok: true,
            sem: TextSemantics::Utf8Chars,
        }),
        ..ColumnPosture::default()
    });
    let mut kit = FullKit::new(cands);
    let mut w = open_writer(vec![text_col(1)], stamp(94, 1));
    for i in 0..9_000u64 {
        // Row 4321 carries an ill-formed sequence (a lone continuation
        // byte) — ClickHouse-String-shaped bytes under a UTF8 annotation.
        let v = if i == 4_321 {
            let mut b = format!("https://static.example-cdn.example/assets/catalog/item-{i:07}/")
                .into_bytes();
            b.push(0xBF);
            b
        } else {
            format!(
                "https://static.example-cdn.example/assets/catalog/item-{i:07}/image-large.jpeg"
            )
            .into_bytes()
        };
        let img = img_4b_u(&v);
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.append_row(&[RawDatum::Bytes(&img)], &mut kit.ext, &mut env)
            .expect("append never fails on invalid utf8 under a claim");
    }
    publish(&mut w, &mut vfs, &mut kit, 94);
    let r = &w.seal_reports()[0];
    assert_eq!(
        r.elections[0].encoding,
        EncodingId::Verbatim.as_u16(),
        "degraded claim runs BytesOnly: FSST stays gated, dict cap-breaches, verbatim wins"
    );
    assert_eq!(
        r.notes,
        vec![crate::elect::SealNote::Utf8ClaimDegraded {
            attno: 1,
            path_ord: 0
        }],
        "the degrade is a typed census fact"
    );
}

// ---------------------------------------------------------------------------
// THE dict round-trip proof (M3-G gap 1, closed): writer emits a real dict
// stream with a NONZERO §6.3 width byte → the raw-field key cannot resolve
// (tooth) → the reader's normalized path resolves, decode_codes returns the
// exact global codes, decode_full materializes the original strings.
// ---------------------------------------------------------------------------

#[test]
fn dict_stream_reads_back_through_pgrc2_read() {
    let mut vfs = mem_with_dir();
    let cands = CodecCandidates::new(ColumnPosture {
        dict: Some(DictPolicy {
            ndv_cap: 100_000,
            exec_ok: true,
            sem: TextSemantics::BytesOnly,
        }),
        ..ColumnPosture::default()
    });
    let mut kit = FullKit::new(cands);
    let mut w = open_writer(vec![text_col(1)], stamp(93, 1));
    let rows = 10_000u64;
    let payload_of = |i: u64| format!("v{:04}", i % 211);
    for i in 0..rows {
        let img = img_4b_u(payload_of(i).as_bytes());
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.append_row(&[RawDatum::Bytes(&img)], &mut kit.ext, &mut env)
            .expect("append");
    }
    publish(&mut w, &mut vfs, &mut kit, 93);

    let part = open_part(&mut vfs, "part-0.pgrc2");
    let binding = codec_binding();
    let mut cur = StreamCursor::open(Arc::clone(&part), &binding, 1, 0).expect("cursor");
    let entry = cur.entry();
    assert_eq!(entry.encoding, EncodingId::DictCodes.as_u16());
    // 211 codes ⇒ max code width 8: the §6.3 width byte is REAL (nonzero) —
    // this stream is exactly the shape the raw-field key could not resolve.
    assert_eq!(entry.width, 8, "max code width stats fact");
    assert_ne!(entry.flags & STREAMF_DICT_EXEC, 0);

    // TOOTH (born-RED shape): the raw entry fields do NOT resolve a kernel.
    let raw = KernelKey {
        encoding: entry.encoding,
        class: entry.class,
        width: entry.width,
    };
    assert!(
        matches!(
            pgrc2_codec::registry().resolve(raw),
            Err(FormatError::KernelMissing { .. })
        ),
        "raw-field resolution must refuse — the normalization is load-bearing"
    );
    // The normalization maps the same fields onto the registered key.
    let norm = pgrc2_format::enc::stream_kernel_key(entry.encoding, entry.class, entry.width)
        .expect("normalize");
    assert_eq!(norm.width, 0);
    assert!(pgrc2_codec::registry().resolve(norm).is_ok());

    // decode_codes: exact global codes (byte-rank order of "v0000".."v0210"
    // == numeric order, so code == i % 211).
    let gcount = cur.granule_count();
    let mut row_base = 0u64;
    for g in 0..gcount {
        let n = cur.values_in_granule(g).expect("values");
        let mut codes = vec![0u32; n as usize];
        let wrote = cur.decode_codes(g, &mut codes).expect("decode_codes");
        assert_eq!(wrote, n);
        for r in 0..n as u64 {
            assert_eq!(
                codes[r as usize] as u64,
                (row_base + r) % 211,
                "global code mismatch at row {}",
                row_base + r
            );
        }
        // decode_full: materialized values byte-identical to the input.
        let (datums, _arena) = decode_granule(&mut cur, g);
        for r in 0..n as u64 {
            assert_varlena_datum(datums[r as usize], payload_of(row_base + r).as_bytes());
        }
        row_base += n as u64;
    }
    assert_eq!(row_base, rows);
}

// ---------------------------------------------------------------------------
// SB-7 (M3-L2): frame-boundary dict extents. UNWRAPPED DictPayload extent
// tables are cut at dict-frame boundaries (per-frame CRC, extent 0 carries
// the header, the tail extent the frame table) and read back through the
// reader's multi-extent assembly; a WRAPPED payload stays single-extent
// whole-section (P-6) with ONE ZSTD BLOCK PER FRAME inside the image.
// ---------------------------------------------------------------------------

/// 60 incompressible bytes per distinct id (splitmix64 stream): long
/// enough that the periodic 4-byte varlena headers (the one repeating
/// pattern in the payload region) stay under the >=20% wrapper gate.
fn sb7_random_payload(i: u64) -> [u8; 60] {
    let mut z = i.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut out = [0u8; 60];
    for c in out.chunks_mut(8) {
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        let b = z.to_le_bytes();
        let n = c.len();
        c.copy_from_slice(&b[..n]);
    }
    out
}

#[test]
fn dict_payload_frame_extents_cut_and_read_back() {
    let mut vfs = mem_with_dir();
    let cands = CodecCandidates::new(ColumnPosture {
        dict: Some(DictPolicy {
            ndv_cap: 100_000,
            exec_ok: true,
            sem: TextSemantics::BytesOnly,
        }),
        ..ColumnPosture::default()
    });
    let mut kit = FullKit::new(cands);
    let mut w = open_writer(vec![text_col(1)], stamp(94, 1));
    let distinct = 2_500u64; // 3 dict frames: 1024 + 1024 + 452
    let rows = 10_000u64;
    for i in 0..rows {
        let payload = sb7_random_payload(i % distinct);
        let img = img_4b_u(&payload);
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.append_row(&[RawDatum::Bytes(&img)], &mut kit.ext, &mut env)
            .expect("append");
    }
    publish(&mut w, &mut vfs, &mut kit, 94);

    // Geometry leg: incompressible payloads refuse the wrapper (SB-2 gate),
    // so the UNWRAPPED extent table is cut at frame boundaries.
    let pv = PartView::open(&mut vfs, "part-0.pgrc2");
    let (dp, dp_ext) = pv.stream(1, 0, StreamRole::DictPayload).expect("DictPayload");
    assert_eq!(
        dp.wrapper,
        0,
        "random payloads must refuse the wrapper (the unwrapped-extents leg needs it)"
    );
    assert_eq!(dp_ext.len(), 3, "ceil(2500/1024) frame extents");
    assert_eq!(
        dp_ext.iter().map(|e| e.values).collect::<Vec<_>>(),
        vec![1024, 1024, 452],
        "entries per frame"
    );
    for (f, e) in dp_ext.iter().enumerate() {
        assert_eq!(e.granule_start, f as u32, "extent {f} carries its frame ordinal");
        assert_eq!(e.granule_count, 1);
    }
    for wpair in dp_ext.windows(2) {
        assert_eq!(
            wpair[0].file_off + wpair[0].len,
            wpair[1].file_off,
            "frame extents tile the stored section contiguously"
        );
    }
    // The index stays single-extent (the small always-resident half).
    let (_, di_ext) = pv.stream(1, 0, StreamRole::DictIndex).expect("DictIndex");
    assert_eq!(di_ext.len(), 1);

    // Read-back leg: the reader ASSEMBLES the multi-extent payload (per-
    // extent CRC validation is the frame-grain fault) and materializes the
    // exact input strings through the dict.
    let part = open_part(&mut vfs, "part-0.pgrc2");
    let binding = codec_binding();
    let mut cur = StreamCursor::open(Arc::clone(&part), &binding, 1, 0).expect("cursor");
    assert_eq!(cur.entry().encoding, EncodingId::DictCodes.as_u16());
    let n = cur.values_in_granule(0).expect("values");
    let (datums, _arena) = decode_granule(&mut cur, 0);
    for r in 0..n as u64 {
        assert_varlena_datum(datums[r as usize], &sb7_random_payload(r % distinct));
    }
}

// ---------------------------------------------------------------------------
// Wrapper election (CMP-A shape): offered on EVERY election (O-CMP-3(a)),
// both arms priced exactly under the ≥20% wrapper law (O-CMP-4(a)), the
// smaller image elected; wrapped streams read back byte-exact through the
// reader's unwrap path with decompress-once residency (O-CMP-5(a)); a
// binding without the unwrapper refuses BEFORE any payload fault.
// ---------------------------------------------------------------------------

#[test]
fn wrapped_stream_reads_back_and_refuses_without_unwrapper() {
    let mut vfs = mem_with_dir();
    // DEFAULT posture: the wrapper offer no longer needs `cold` —
    // O-CMP-3(a)'s widening is exactly what this fixture witnesses.
    let cands = CodecCandidates::new(ColumnPosture::default());
    let mut kit = FullKit::new(cands);
    let mut w = open_writer(vec![text_col(1)], stamp(94, 1));
    let rows = 10_000u64;
    let payload_of = |i: u64| format!("{}{}", "pad-".repeat(15), i % 5);
    for i in 0..rows {
        let img = img_4b_u(payload_of(i).as_bytes());
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.append_row(&[RawDatum::Bytes(&img)], &mut kit.ext, &mut env)
            .expect("append");
    }
    publish(&mut w, &mut vfs, &mut kit, 94);

    let pv = PartView::open(&mut vfs, "part-0.pgrc2");
    let (e, _) = pv.stream(1, 0, StreamRole::Values).expect("col1");
    assert_eq!(e.encoding, EncodingId::Verbatim.as_u16());
    // Both arms clear the ≥20% law on this shape; zstd's exact image is
    // smaller, so the deterministic ranking elects it.
    assert_eq!(e.wrapper, Wrapper::Zstd.as_u8(), "the wrapper election engaged");

    // Reads back byte-exact through the unwrap path.
    let part = open_part(&mut vfs, "part-0.pgrc2");
    let binding = codec_binding();
    let mut cur = StreamCursor::open(Arc::clone(&part), &binding, 1, 0).expect("cursor");
    let mut row_base = 0u64;
    for g in 0..cur.granule_count() {
        let n = cur.values_in_granule(g).expect("values");
        let (datums, _arena) = decode_granule(&mut cur, g);
        for r in 0..n as u64 {
            assert_varlena_datum(datums[r as usize], payload_of(row_base + r).as_bytes());
        }
        row_base += n as u64;
    }
    assert_eq!(row_base, rows);

    // O-CMP-5(a) residency: the unwrapped image is cached on the PART —
    // a SECOND cursor over the same extents faults nothing new (the fault
    // log is the decompress-once witness; a cache hit records nothing).
    let faults_after_first = part.faults().len();
    let resident_after_first = part.resident();
    let mut cur2 = StreamCursor::open(Arc::clone(&part), &binding, 1, 0).expect("cursor 2");
    let (_datums, _arena) = decode_granule(&mut cur2, 0);
    assert_eq!(
        part.faults().len(),
        faults_after_first,
        "second cursor must hit the unwrapped-image cache (decompress once per residency)"
    );
    assert_eq!(
        part.resident(),
        resident_after_first,
        "a cache hit must not grow residency"
    );

    // TOOTH: no unwrapper registered ⇒ typed refusal at cursor open.
    let bare = CodecBinding {
        registry: pgrc2_codec::registry(),
        unwrappers: &[],
    };
    let part2 = open_part(&mut vfs, "part-0.pgrc2");
    assert!(
        StreamCursor::open(part2, &bare, 1, 0).is_err(),
        "wrapped stream without an unwrapper must refuse at open"
    );
    // TOOTH: an LZ4-only binding is equally refused for a Zstd stream —
    // the old-binary posture (§8 refusal-before-fault, arm-specific).
    let lz4_only = CodecBinding {
        registry: pgrc2_codec::registry(),
        unwrappers: &[&LZ4_UNWRAP],
    };
    let part3 = open_part(&mut vfs, "part-0.pgrc2");
    let err = StreamCursor::open(part3, &lz4_only, 1, 0).expect_err("lz4-only binding");
    assert!(
        matches!(
            err,
            pgrc2_read::ReadError::Format(FormatError::WrapperUnsupported { wrapper: 2 })
        ),
        "a binding without the Zstd unwrapper must refuse WrapperUnsupported, got {err:?}"
    );
}

#[test]
fn hot_fixed_width_stream_wraps_and_noise_does_not() {
    // The O-CMP-3(a) widening witness at seal grain, with its negative
    // control: a compressible HOT int8 ByteFor stream (a shape the old
    // cold-only policy never offered) wraps; a full-entropy int8 stream
    // (the incompressible guard) stays unwrapped.
    let mut vfs = mem_with_dir();
    let cands = CodecCandidates::new(ColumnPosture::default());
    let mut kit = FullKit::new(cands);
    let mut w = open_writer(vec![int8_col(1), int8_col(2)], stamp(95, 1));
    let mut seed = 0x77AB_u64;
    let rows = 10_000u64;
    for i in 0..rows {
        let noise = splitmix(&mut seed);
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.append_row(
            &[RawDatum::Word(i), RawDatum::Word(noise)],
            &mut kit.ext,
            &mut env,
        )
        .expect("append");
    }
    publish(&mut w, &mut vfs, &mut kit, 95);

    let pv = PartView::open(&mut vfs, "part-0.pgrc2");
    let (e1, _) = pv.stream(1, 0, StreamRole::Values).expect("col1");
    assert_eq!(e1.encoding, EncodingId::ByteFor.as_u16(), "seq int8 stays ByteFor");
    assert_eq!(
        e1.wrapper,
        Wrapper::Zstd.as_u8(),
        "hot compressible ByteFor stream must take the widened wrapper offer"
    );
    let (e2, _) = pv.stream(2, 0, StreamRole::Values).expect("col2");
    assert_eq!(e2.encoding, EncodingId::Verbatim.as_u16(), "noise demotes to verbatim");
    assert_eq!(
        e2.wrapper,
        Wrapper::None.as_u8(),
        "full-entropy stream must fail the ≥20% wrapper law (negative control)"
    );

    // Byte-exact read-back of the wrapped hot stream.
    let part = open_part(&mut vfs, "part-0.pgrc2");
    let binding = codec_binding();
    let mut cur = StreamCursor::open(Arc::clone(&part), &binding, 1, 0).expect("cursor");
    let mut row_base = 0u64;
    for g in 0..cur.granule_count() {
        let n = cur.values_in_granule(g).expect("values");
        let (datums, _arena) = decode_granule(&mut cur, g);
        for r in 0..n as u64 {
            assert_eq!(datums[r as usize], row_base + r, "wrapped ByteFor decode");
        }
        row_base += n as u64;
    }
    assert_eq!(row_base, rows);
}

/// Delegating source that pins `offer_wrapper = false` — the A/B control
/// for the #597 operand law below.
struct NoWrapSource(CodecCandidates);

impl crate::elect::CandidateSource for NoWrapSource {
    fn propose(&self, _input: &crate::elect::FullElectInput<'_>) -> Vec<crate::elect::Candidate> {
        Vec::new()
    }
    fn elect_full(
        &self,
        input: &crate::elect::FullElectInput<'_>,
    ) -> Option<crate::WriteResult<crate::elect::FullElection>> {
        self.0.elect_full(input).map(|r| {
            r.map(|mut f| {
                f.offer_wrapper = false;
                f
            })
        })
    }
}

#[test]
fn wrapped_parts_are_byte_identical_across_runs() {
    // The byte-identical-parts law extended to wrapped parts: part bytes
    // are a pure function of (schema, elections, input row partition) —
    // which requires the zstd arm itself to be deterministic at a pinned
    // level + library version. Two independent runs over identical input
    // must produce byte-identical part files, wrapped streams included.
    let run = || -> Vec<u8> {
        let mut vfs = mem_with_dir();
        let cands = CodecCandidates::new(ColumnPosture::default());
        let mut kit = FullKit::new(cands);
        let mut w = open_writer(vec![text_col(1), int8_col(2)], stamp(97, 1));
        let payload_of = |i: u64| format!("{}{}", "pad-".repeat(15), i % 5);
        for i in 0..10_000u64 {
            let img = img_4b_u(payload_of(i).as_bytes());
            let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
            let mut env = SealEnv {
                vfs: &mut vfs,
                sources: &sources,
                resolver: &kit.resolver,
                shred: &mut kit.shred,
                shred_opts: &kit.opts,
            };
            w.append_row(&[RawDatum::Bytes(&img), RawDatum::Word(i)], &mut kit.ext, &mut env)
                .expect("append");
        }
        publish(&mut w, &mut vfs, &mut kit, 97);
        // Sanity: the fixture actually wrapped (else the pin is vacuous).
        let pv = PartView::open(&mut vfs, "part-0.pgrc2");
        let (e, _) = pv.stream(1, 0, StreamRole::Values).expect("col1");
        assert_ne!(e.wrapper, Wrapper::None.as_u8(), "fixture must wrap");
        vfs.read_full(&format!("{DIR}/part-0.pgrc2")).expect("part bytes")
    };
    assert_eq!(run(), run(), "wrapped part bytes diverged across identical runs");
}

#[test]
fn two_witness_null_law_holds_on_wrapped_parts() {
    // The seal's two-witness nonnull cross-check runs on the UNWRAPPED
    // emitted bytes and must be unperturbed by a downstream wrapper
    // election: a NULL-carrying compressible column wraps AND banks its
    // crosschecks, and the wrapped part reads back with nulls intact.
    let mut vfs = mem_with_dir();
    let cands = CodecCandidates::new(ColumnPosture::default());
    let mut kit = FullKit::new(cands);
    let mut w = open_writer(vec![text_col(1)], stamp(98, 1));
    let rows = 10_000u64;
    let payload_of = |i: u64| format!("{}{}", "pad-".repeat(15), i % 5);
    for i in 0..rows {
        let img = img_4b_u(payload_of(i).as_bytes());
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        let row = if i % 7 == 0 {
            [RawDatum::Null]
        } else {
            [RawDatum::Bytes(&img)]
        };
        w.append_row(&row, &mut kit.ext, &mut env).expect("append");
    }
    publish(&mut w, &mut vfs, &mut kit, 98);
    let r = &w.seal_reports()[0];
    assert!(r.nonnull_crosschecks > 0, "two-witness checks must have RUN");

    let pv = PartView::open(&mut vfs, "part-0.pgrc2");
    let (e, _) = pv.stream(1, 0, StreamRole::Values).expect("col1");
    assert_ne!(e.wrapper, Wrapper::None.as_u8(), "fixture must wrap");
    pv.stream(1, 0, StreamRole::Validity).expect("validity stream exists");

    // Read back: the validity mask agrees with the null pattern and valid
    // rows decode byte-exactly through the unwrap path.
    let part = open_part(&mut vfs, "part-0.pgrc2");
    let binding = codec_binding();
    let mut cur = StreamCursor::open(Arc::clone(&part), &binding, 1, 0).expect("cursor");
    let mut row_base = 0u64;
    for g in 0..cur.granule_count() {
        let n = cur.values_in_granule(g).expect("values");
        let mut mask = vec![0u64; (n as usize).div_ceil(64)];
        cur.validity(g, &mut mask).expect("validity");
        let (datums, _arena) = decode_granule(&mut cur, g);
        for r in 0..n as u64 {
            let i = row_base + r;
            let valid = (mask[(r / 64) as usize] >> (r % 64)) & 1 != 0;
            assert_eq!(valid, i % 7 != 0, "validity mismatch at row {i}");
            if valid {
                assert_varlena_datum(datums[r as usize], payload_of(i).as_bytes());
            }
        }
        row_base += n as u64;
    }
    assert_eq!(row_base, rows);
}

#[test]
fn wrapping_never_moves_a_cut_boundary() {
    // The #597 should_cut operand law, RELEASE-effective on wrapped parts:
    // `bytes` is the pre-encode `ColBuffer::approx_bytes` sum, wrapping
    // happens strictly downstream at seal — so a wrapper-ON run and a
    // wrapper-OFF run over identical input must cut IDENTICAL partitions
    // (same part count, same per-part row counts), while their part bytes
    // legally differ.
    let policy = PartCutPolicy {
        max_rows: 1 << 20,
        max_bytes: 640 << 10, // byte-budget governs on this corpus
        ..PartCutPolicy::default()
    };
    let payload_of = |i: u64| format!("{}{}", "pad-".repeat(15), i % 5);
    let run = |wrap: bool| -> (Vec<u64>, bool) {
        let mut vfs = mem_with_dir();
        let on = CodecCandidates::new(ColumnPosture::default());
        let off = NoWrapSource(CodecCandidates::new(ColumnPosture::default()));
        let mut kit = FullKit::new(CodecCandidates::new(ColumnPosture::default()));
        let mut w = open_writer_policy(vec![text_col(1)], stamp(96, 1), policy);
        for i in 0..24_000u64 {
            let img = img_4b_u(payload_of(i).as_bytes());
            let sources: [&dyn crate::elect::CandidateSource; 1] =
                if wrap { [&on] } else { [&off] };
            let mut env = SealEnv {
                vfs: &mut vfs,
                sources: &sources,
                resolver: &kit.resolver,
                shred: &mut kit.shred,
                shred_opts: &kit.opts,
            };
            w.append_row(&[RawDatum::Bytes(&img)], &mut kit.ext, &mut env)
                .expect("append");
        }
        {
            let sources: [&dyn crate::elect::CandidateSource; 1] =
                if wrap { [&on] } else { [&off] };
            let mut env = SealEnv {
                vfs: &mut vfs,
                sources: &sources,
                resolver: &kit.resolver,
                shred: &mut kit.shred,
                shred_opts: &kit.opts,
            };
            w.finish(&mut env).expect("finish");
        }
        let rows: Vec<u64> = w.sealed_parts().iter().map(|p| p.rows).collect();
        // Whether ANY stream in the run actually wrapped (arm sanity).
        let mut any_wrapped = false;
        for seq in 0..w.sealed_parts().len() as u32 {
            let name = pgrc2_format::dirlayout::temp_file_name(96, seq);
            let bytes = vfs.read_full(&format!("{DIR}/{name}")).expect("tmp bytes");
            let io = MemPartIo::new(bytes, 4242, 7);
            let part = OpenPart::open(Box::new(io), &PartExpect::none()).expect("open");
            let dir = part.stream_directory().expect("dir");
            if let Some(ps) = dir.lookup(1, 0, StreamRole::Values) {
                if ps.entry.wrapper != Wrapper::None.as_u8() {
                    any_wrapped = true;
                }
            }
        }
        (rows, any_wrapped)
    };
    let (rows_on, wrapped_on) = run(true);
    let (rows_off, wrapped_off) = run(false);
    assert!(rows_on.len() > 1, "the fixture must actually cut (else vacuous)");
    assert!(wrapped_on, "the ON arm must actually wrap (else vacuous)");
    assert!(!wrapped_off, "the OFF control must stay unwrapped");
    assert_eq!(
        rows_on, rows_off,
        "wrapping moved a cut boundary — the #597 operand law is breached"
    );
}

// ---------------------------------------------------------------------------
// The v1 single-extent overflow law (spec §6.8, amended): a multi-band part
// with oversize values in EVERY band emits ONE overflow extent, and the
// merged reader (which refuses multi-extent overflow) decodes it.
// ---------------------------------------------------------------------------

#[test]
fn multi_band_overflow_single_extent_reads_back() {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new(); // legacy stats-only sources: the fallback path
    let mut w = open_writer(vec![text_col(1)], stamp(95, 1));
    let rows = 70_000u64; // 2 bands (65,536 + 4,464), 9 granules
    let big_a = "A".repeat(40_000);
    let big_b = "B".repeat(40_000);
    let payload_of = |i: u64| -> String {
        if i == 100 {
            big_a.clone()
        } else if i == 66_000 {
            big_b.clone()
        } else {
            format!("s{}", i % 41)
        }
    };
    for i in 0..rows {
        let img = img_4b_u(payload_of(i).as_bytes());
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.append_row(&[RawDatum::Bytes(&img)], &mut kit.ext, &mut env)
            .expect("append");
    }
    let probe = Probe::new(TxnVerdict::InProgress).set(95, TxnVerdict::Committed);
    finish_and_publish(&mut w, &mut vfs, &mut kit, &probe);

    let pv = PartView::open(&mut vfs, "part-0.pgrc2");
    let (oe, oext) = pv.stream(1, 0, StreamRole::Overflow).expect("overflow stream");
    assert_eq!(oext.len(), 1, "v1 single-extent law: one overflow extent");
    assert_eq!(oe.values, 2, "both bands' oversize values in the one region");

    // The merged reader decodes BOTH bands' oversize rows.
    let part = open_part(&mut vfs, "part-0.pgrc2");
    let binding = codec_binding();
    let mut cur = StreamCursor::open(Arc::clone(&part), &binding, 1, 0).expect("cursor");
    // Row 100 lives in granule 0; row 66,000 in granule 8 (row 464 there).
    let (datums0, _a0) = decode_granule(&mut cur, 0);
    assert_varlena_datum(datums0[100], big_a.as_bytes());
    let (datums8, _a8) = decode_granule(&mut cur, 8);
    assert_varlena_datum(datums8[464], big_b.as_bytes());
}

// ---------------------------------------------------------------------------
// CMP-B election widening (docs/design/pgrc2-compression.md §3 CMP-B row,
// ruled 2026-08-10 with O-CMP-1..8): (1) the cold/size STORAGE posture
// admits DELTA_FOR as a size candidate exactly where its exact bytes win;
// (2) the wrapper offer widens to the byte-run stream CLASS (overflow /
// dict index / dict payload) under the same ≥20% wrapper law, decompressed
// once per part residency, with typed refusal before any payload fault.
// ---------------------------------------------------------------------------

static TEST_UNWRAPPERS: [&dyn SectionUnwrapper; 2] = [&LZ4_UNWRAP, &ZSTD_UNWRAP];

/// The §9.2 timestamp shape at unit scale: µs-since-epoch steps defeat
/// byte-aligned FOR (per-frame range > 4 bytes ⇒ width 8 ⇒ BelowWinGate ⇒
/// VERBATIM), while DELTA_FOR's per-frame zigzag deltas are 3 bytes wide.
/// Born-RED pair: the default (non-storage) posture ships VERBATIM — the
/// gate CMP-B flips; the storage posture elects DELTA_FOR; answers are
/// byte-identical either way (elections change layout, never answers).
#[test]
fn deltafor_size_arm_elects_under_storage_posture_where_it_wins() {
    let base: u64 = 1_600_000_000_000_000;
    let step: u64 = 5_000_000; // 5s in µs: frame range ≈ 5.1e9 > u32
    let value_of = |i: u64| base + i * step;
    let rows = 20_000u64;
    let run = |cold: bool| -> (u16, Vec<u8>) {
        let mut vfs = mem_with_dir();
        let posture = ColumnPosture {
            cold,
            ..ColumnPosture::default()
        };
        let cands = CodecCandidates::new(posture);
        let mut kit = FullKit::new(cands);
        let mut w = open_writer(vec![int8_col(1)], stamp(99, 1));
        for i in 0..rows {
            let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
            let mut env = SealEnv {
                vfs: &mut vfs,
                sources: &sources,
                resolver: &kit.resolver,
                shred: &mut kit.shred,
                shred_opts: &kit.opts,
            };
            w.append_row(&[RawDatum::Word(value_of(i))], &mut kit.ext, &mut env)
                .expect("append");
        }
        publish(&mut w, &mut vfs, &mut kit, 99);
        let pv = PartView::open(&mut vfs, "part-0.pgrc2");
        let (e, _) = pv.stream(1, 0, StreamRole::Values).expect("col1");

        // Decode-backed answers for the identity cross-check.
        let part = open_part(&mut vfs, "part-0.pgrc2");
        let binding = codec_binding();
        let mut cur = StreamCursor::open(Arc::clone(&part), &binding, 1, 0).expect("cursor");
        let mut all: Vec<u8> = Vec::new();
        let mut row_base = 0u64;
        for g in 0..cur.granule_count() {
            let n = cur.values_in_granule(g).expect("values");
            let (datums, _arena) = decode_granule(&mut cur, g);
            for r in 0..n as u64 {
                assert_eq!(
                    datums[r as usize],
                    value_of(row_base + r),
                    "decode mismatch at row {}",
                    row_base + r
                );
                all.extend_from_slice(&datums[r as usize].to_le_bytes());
            }
            row_base += n as u64;
        }
        assert_eq!(row_base, rows);
        (e.encoding, all)
    };
    let (enc_default, vals_default) = run(false);
    let (enc_storage, vals_storage) = run(true);
    // v4 delta (ledger SB-3): the control arm's demotion WAS the v3 gap —
    // widths {1,2,4,8} forced width 8 and the >=10% gate failed. With the
    // completed {1..8} ladder this per-frame range (~5.1e9, 33 bits) elects
    // ByteFor at width 5 — the exact CMP-B recovery this shape existed to
    // demonstrate. The storage-posture contrast below is unchanged:
    // DELTA_FOR still displaces plain FOR as the size candidate.
    assert_eq!(
        enc_default,
        EncodingId::ByteFor.as_u16(),
        "control arm must elect the widened FOR ladder (SB-3; was Verbatim in v3)"
    );
    // The widening: DELTA_FOR competes as a size candidate and wins.
    assert_eq!(
        enc_storage,
        EncodingId::DeltaFor.as_u16(),
        "storage posture must elect DELTA_FOR where it wins"
    );
    // Elections change layout, never answers.
    assert_eq!(vals_default, vals_storage, "answers moved across postures");
}

/// Dict payload/index take the byte-run wrapper offer: a compressible dict
/// payload wraps (≥20% law), reads back byte-exact through cursor AND
/// DictHandle, decompresses once per part residency, and refuses TYPED at
/// open without the unwrapper (before any payload fault). The negative
/// control (incompressible dict payload) stays unwrapped.
#[test]
fn wrapped_dict_sections_read_back_with_residency_and_refusal() {
    let mut vfs = mem_with_dir();
    let dict_posture = ColumnPosture {
        dict: Some(DictPolicy {
            ndv_cap: 100_000,
            exec_ok: true,
            sem: TextSemantics::BytesOnly,
        }),
        ..ColumnPosture::default()
    };
    let cands = CodecCandidates::new(ColumnPosture::default())
        .with_column(1, 0, dict_posture)
        .with_column(2, 0, dict_posture);
    let mut kit = FullKit::new(cands);
    let mut w = open_writer(vec![text_col(1), text_col(2)], stamp(101, 1));
    let rows = 10_000u64;
    // Col 1: compressible dict payload (shared prefix + padding), with a
    // pseudo-random VALUE order so the code stream itself stays unwrapped
    // (the companion-refusal tooth below must bite on the DICT stream, not
    // the values stream).
    let comp_of = |i: u64| {
        let mut s = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        s ^= s >> 29;
        format!("shared-prefix-{:03}-padpadpadpadpadpadpadpad", s % 211)
    };
    // Col 2: incompressible dict payload (random 32-B hex-free bytes).
    let mut noise_entries: Vec<Vec<u8>> = Vec::new();
    let mut seed = 0xD1C7_u64;
    for _ in 0..211 {
        let mut e = [0u8; 32];
        for chunk in e.chunks_mut(8) {
            chunk.copy_from_slice(&splitmix(&mut seed).to_le_bytes());
        }
        noise_entries.push(e.to_vec());
    }
    for i in 0..rows {
        let img1 = img_4b_u(comp_of(i).as_bytes());
        let img2 = img_4b_u(&noise_entries[(i % 211) as usize]);
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.append_row(
            &[RawDatum::Bytes(&img1), RawDatum::Bytes(&img2)],
            &mut kit.ext,
            &mut env,
        )
        .expect("append");
    }
    publish(&mut w, &mut vfs, &mut kit, 101);

    let pv = PartView::open(&mut vfs, "part-0.pgrc2");
    let (v1, _) = pv.stream(1, 0, StreamRole::Values).expect("col1 values");
    assert_eq!(v1.encoding, EncodingId::DictCodes.as_u16(), "col1 dict-elects");
    assert_eq!(
        v1.wrapper,
        Wrapper::None.as_u8(),
        "fixture contract: col1 CODE stream must stay unwrapped (random code order)"
    );
    let (dp1, _) = pv.stream(1, 0, StreamRole::DictPayload).expect("col1 payload");
    assert_ne!(
        dp1.wrapper,
        Wrapper::None.as_u8(),
        "compressible dict PAYLOAD must take the byte-run wrapper offer"
    );
    let (dp2, _) = pv.stream(2, 0, StreamRole::DictPayload).expect("col2 payload");
    assert_eq!(
        dp2.wrapper,
        Wrapper::None.as_u8(),
        "incompressible dict payload must fail the ≥20% law (negative control)"
    );

    // Byte-exact read-back through the cursor face (dict materialization
    // reads the wrapped payload region through the unwrap path).
    let part = open_part(&mut vfs, "part-0.pgrc2");
    let binding = codec_binding();
    let mut cur = StreamCursor::open(Arc::clone(&part), &binding, 1, 0).expect("cursor");
    let mut row_base = 0u64;
    for g in 0..cur.granule_count() {
        let n = cur.values_in_granule(g).expect("values");
        let (datums, _arena) = decode_granule(&mut cur, g);
        for r in 0..n as u64 {
            assert_varlena_datum(datums[r as usize], comp_of(row_base + r).as_bytes());
        }
        row_base += n as u64;
    }
    assert_eq!(row_base, rows);

    // O-CMP-5(a) residency on the byte-run class: a SECOND cursor faults
    // nothing new and grows nothing — the unwrapped dict region is cached
    // on the part (this is also the StrView §7b stability argument: the
    // rebuilt region is part-resident for the part's life).
    let faults_after_first = part.faults().len();
    let resident_after_first = part.resident();
    let mut cur2 = StreamCursor::open(Arc::clone(&part), &binding, 1, 0).expect("cursor 2");
    let (_d, _a) = decode_granule(&mut cur2, 0);
    assert_eq!(part.faults().len(), faults_after_first, "decompress once per residency");
    assert_eq!(part.resident(), resident_after_first, "cache hit must not grow residency");

    // DictHandle face over the wrapped payload: entries materialize.
    let h = pgrc2_read::dicthandle::DictHandle::open(
        Arc::clone(&part),
        None,
        &TEST_UNWRAPPERS,
        1,
        0,
    )
    .expect("dict handle");
    assert_eq!(h.ncodes(), 211);
    for code in 0..h.ncodes() {
        let e = h.entry(code).expect("entry");
        assert!(e.bytes.starts_with(b"shared-prefix-"), "entry payload shape");
    }

    // TOOTH (refusal-before-fault, the old-binary posture): a binding
    // without unwrappers refuses TYPED at cursor open on the COMPANION
    // dict stream (the values stream is unwrapped by fixture contract),
    // and DictHandle::open refuses the same way.
    let bare = CodecBinding {
        registry: pgrc2_codec::registry(),
        unwrappers: &[],
    };
    let part2 = open_part(&mut vfs, "part-0.pgrc2");
    let err = StreamCursor::open(Arc::clone(&part2), &bare, 1, 0).expect_err("bare binding");
    assert!(
        matches!(
            err,
            pgrc2_read::ReadError::Format(FormatError::WrapperUnsupported { .. })
        ),
        "wrapped dict stream without an unwrapper must refuse typed at open, got {err:?}"
    );
    let herr = match pgrc2_read::dicthandle::DictHandle::open(part2, None, &[], 1, 0) {
        Ok(_) => panic!("bare dict handle must refuse"),
        Err(e) => e,
    };
    assert!(
        matches!(
            herr,
            pgrc2_read::ReadError::Format(FormatError::WrapperUnsupported { .. })
        ),
        "DictHandle over a wrapped dict stream without an unwrapper must refuse typed, got {herr:?}"
    );
}

/// The overflow stream takes the byte-run wrapper offer: compressible
/// oversize values wrap the ONE overflow extent; the reader rebuilds the
/// region and materializes the oversize rows byte-exactly.
#[test]
fn wrapped_overflow_region_reads_back() {
    let mut vfs = mem_with_dir();
    let cands = CodecCandidates::new(ColumnPosture::default());
    let mut kit = FullKit::new(cands);
    let mut w = open_writer(vec![text_col(1)], stamp(102, 1));
    let rows = 9_000u64;
    let big = "Z".repeat(40_000); // ≥ OVERSIZE_THRESHOLD, highly compressible
    let payload_of = |i: u64| -> String {
        if i == 7 || i == 8_500 {
            big.clone()
        } else {
            format!("s{}", i % 41)
        }
    };
    for i in 0..rows {
        let img = img_4b_u(payload_of(i).as_bytes());
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.append_row(&[RawDatum::Bytes(&img)], &mut kit.ext, &mut env)
            .expect("append");
    }
    publish(&mut w, &mut vfs, &mut kit, 102);

    let pv = PartView::open(&mut vfs, "part-0.pgrc2");
    let (oe, oext) = pv.stream(1, 0, StreamRole::Overflow).expect("overflow stream");
    assert_eq!(oext.len(), 1, "v1 single-extent law preserved");
    assert_eq!(oe.values, 2);
    assert_ne!(
        oe.wrapper,
        Wrapper::None.as_u8(),
        "compressible overflow region must take the byte-run wrapper offer"
    );

    let part = open_part(&mut vfs, "part-0.pgrc2");
    let binding = codec_binding();
    let mut cur = StreamCursor::open(Arc::clone(&part), &binding, 1, 0).expect("cursor");
    let (d0, _a0) = decode_granule(&mut cur, 0);
    assert_varlena_datum(d0[7], big.as_bytes());
    let g = 8_500 / 8_192;
    let (dg, _ag) = decode_granule(&mut cur, g);
    assert_varlena_datum(dg[(8_500 % 8_192) as usize], big.as_bytes());
}

/// Corrupt tooth on the widened class: a flipped byte inside a WRAPPED dict
/// payload extent refuses TYPED (CRC first — the wrapper CRC law covers the
/// wrapped bytes on disk), never a panic, never silent wrong bytes.
#[test]
fn corrupt_wrapped_dict_payload_refuses_typed() {
    let mut vfs = mem_with_dir();
    let dict_posture = ColumnPosture {
        dict: Some(DictPolicy {
            ndv_cap: 100_000,
            exec_ok: true,
            sem: TextSemantics::BytesOnly,
        }),
        ..ColumnPosture::default()
    };
    let cands = CodecCandidates::new(ColumnPosture::default()).with_column(1, 0, dict_posture);
    let mut kit = FullKit::new(cands);
    let mut w = open_writer(vec![text_col(1)], stamp(103, 1));
    let comp_of = |i: u64| {
        let mut s = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        s ^= s >> 29;
        format!("shared-prefix-{:03}-padpadpadpadpadpadpadpad", s % 211)
    };
    for i in 0..10_000u64 {
        let img = img_4b_u(comp_of(i).as_bytes());
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.append_row(&[RawDatum::Bytes(&img)], &mut kit.ext, &mut env)
            .expect("append");
    }
    publish(&mut w, &mut vfs, &mut kit, 103);

    let pv = PartView::open(&mut vfs, "part-0.pgrc2");
    let (dp, dpx) = pv.stream(1, 0, StreamRole::DictPayload).expect("payload");
    assert_ne!(dp.wrapper, Wrapper::None.as_u8(), "fixture must wrap");
    let rec = dpx[0];

    let mut bytes = vfs
        .read_full(&format!("{DIR}/part-0.pgrc2"))
        .expect("part bytes");
    let mid = rec.file_off as usize + (rec.len as usize) / 2;
    bytes[mid] ^= 0x40;
    let io = MemPartIo::new(bytes, 4242, 7);
    let part = Arc::new(OpenPart::open(Box::new(io), &PartExpect::none()).expect("open part"));
    let binding = codec_binding();
    let mut cur = StreamCursor::open(Arc::clone(&part), &binding, 1, 0).expect("cursor opens");
    let n = cur.values_in_granule(0).expect("values") as usize;
    let mut datums = vec![0u64; n];
    let mut arena_buf = vec![0u8; 4 << 20];
    let res = {
        let mut out = DecodeOut {
            datums: &mut datums,
            arena: ByteArena::new(&mut arena_buf),
        };
        cur.decode_full(0, &mut out)
    };
    assert!(res.is_err(), "corrupt wrapped dict payload must refuse, not decode");
}

// ---------------------------------------------------------------------------
// DICT-DEDUP: seal-grain distribution equivalence (born-RED charter)
// ---------------------------------------------------------------------------

/// The dedup's end-to-end law at SEALED-BYTES grain: on a dict-elected
/// column the Stats-sidecar distribution is served from the dict build's
/// counted set (the feed), and it must equal what the #971 accumulator
/// computes over the same non-null payload sequence — byte-for-byte, long
/// values (dict-legal, list-ineligible) and nulls included. RED if the
/// feed path drifts from the accumulator in either content or gating.
#[test]
fn dict_elected_sidecar_distribution_matches_accumulator_reference() {
    let mut vfs = mem_with_dir();
    let cands = CodecCandidates::new(ColumnPosture::default()).with_column(
        1,
        0,
        ColumnPosture {
            dict: Some(DictPolicy {
                ndv_cap: 10_000,
                exec_ok: true,
                sem: TextSemantics::Utf8Chars,
            }),
            ..ColumnPosture::default()
        },
    );
    let mut kit = FullKit::new(cands);
    let mut w = open_writer(vec![text_col(1)], stamp(97, 1));
    let rows = 2_000u64;
    let long_val = vec![b'L'; 300]; // > STATS_MCV_VALUE_MAX: counted, list-ineligible
    let mut payloads: Vec<Option<Vec<u8>>> = Vec::new();
    for i in 0..rows {
        let p = if i % 17 == 0 {
            None
        } else if i % 501 == 0 {
            Some(long_val.clone())
        } else {
            Some(format!("k{:03}", i % 97).into_bytes())
        };
        payloads.push(p);
    }
    for p in &payloads {
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        let img;
        let row = match p {
            Some(bytes) => {
                img = img_4b_u(bytes);
                [RawDatum::Bytes(&img)]
            }
            None => [RawDatum::Null],
        };
        w.append_row(&row, &mut kit.ext, &mut env).expect("append");
    }
    {
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.finish(&mut env).expect("finish");
    }
    // The dict MUST have elected, or this test proves nothing.
    let r = &w.seal_reports()[0];
    let e1 = r.elections.iter().find(|e| e.attno == 1).expect("attno 1");
    assert_eq!(
        e1.encoding,
        EncodingId::DictCodes.as_u16(),
        "corpus failed to dict-elect — the feed path was not exercised"
    );
    // The sealed sidecar payload (a seal byproduct, captured pre-publish).
    let payload = &w.sealed_parts()[0].stats_payload;
    let cols = pgrc2_format::sidecar::decode_stats_payload(payload).expect("payload decodes");
    let got = cols
        .iter()
        .find(|(a, p, _)| (*a, *p) == (1, 0))
        .map(|(_, _, d)| d)
        .expect("dict column sketch");
    // The #971 accumulator reference over the same non-null sequence.
    let mut reference = pgrc2_meta::sketch::DistAcc::default();
    for p in payloads.iter().flatten() {
        reference.observe(p);
    }
    assert_eq!(
        *got,
        reference.finalize(),
        "feed-served sidecar distribution diverged from the accumulator reference"
    );
}
