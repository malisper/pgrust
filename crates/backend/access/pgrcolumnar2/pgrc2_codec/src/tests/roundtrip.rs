//! Per-encoding bit-exact round-trips on adversarial + fuzz corpora (§5
//! M3-C: "the election quadruple's encode-side verify"). Every stream here
//! is built through the production driver with verify ON, then decoded
//! AGAIN through the registry and compared row-by-row — two independent
//! passes over the same bytes. The ALP suite additionally runs the vendored
//! reader as a differential oracle on the exact stored frames.

use super::*;
use crate::alpc::{AlpEncoder, AlpF32Encoder};
use crate::arraydual::{assemble_array_datums, elect_array_split, ArrayElemFacts};
use crate::boolbm::BoolBitmapEncoder;
use crate::bytefor::{granule_min_width, ByteForEncoder};
use crate::deltafor::DeltaForEncoder;
use crate::dictcodes::DictCodesEncoder;
use crate::ffor::FforEncoder;
use crate::packednum::PackedNumericEncoder;
use crate::wrapper::{unwrap_section, wrap_section};
use pgrc2_format::abi::{ByteArena, EncodeInput};
use pgrc2_format::class::{StorageClass, CLASS_VARLENA};
use pgrc2_format::dict::DictSections;
use pgrc2_format::enc::EncodingId;
use pgrc2_format::wire::put_varlena_entry;

// ---------------------------------------------------------------------------
// int corpora (shared shapes live in tests/mod.rs)
// ---------------------------------------------------------------------------

fn int_corpus(shape: u32, rows: u32, seed: &mut u64) -> GranuleData {
    roundtrip_int_corpus(shape, rows, seed)
}

fn signed_for(shape: u32) -> bool {
    shape % 6 == 3 || shape % 2 == 1
}

fn assert_int_roundtrip(built: &Built, granules: &[&GranuleData], class: StorageClass) {
    for (g, gd) in granules.iter().enumerate() {
        let (datums, _arena) = dec_full(built, g as u32);
        let input = gd.input(class);
        for r in 0..gd.rows {
            if !input.valid(r) {
                continue;
            }
            assert_eq!(
                datums[r as usize], gd.datums[r as usize],
                "granule {g} row {r}"
            );
        }
    }
}

#[test]
fn byte_for_roundtrips_across_shapes() {
    let mut seed = 0xB4_u64;
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    for shape in 0..12 {
        // Geometry law: only the LAST granule of an extent may be short.
        let last = [8192u32, 1500, 300, 777][shape as usize % 4];
        let gs: Vec<GranuleData> = [8192u32, 8192, last]
            .iter()
            .map(|&rows| int_corpus(shape, rows, &mut seed))
            .collect();
        let signed = signed_for(shape);
        let inputs: Vec<EncodeInput<'_>> = gs.iter().map(|g| g.input(class)).collect();
        let width = inputs
            .iter()
            .map(|g| granule_min_width(g, signed))
            .max()
            .unwrap();
        let mut enc = ByteForEncoder::new_bytefor(8, width, signed);
        let built = build_stream(&mut enc, &inputs, 0, signed);
        assert_int_roundtrip(&built, &gs.iter().collect::<Vec<_>>(), class);
    }
}

#[test]
fn delta_for_roundtrips_across_shapes() {
    let mut seed = 0xDF_u64;
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    for shape in 0..8 {
        let last = [8192u32, 700, 2049][shape as usize % 3];
        let gs: Vec<GranuleData> = [8192u32, last]
            .iter()
            .map(|&rows| int_corpus(shape, rows, &mut seed))
            .collect();
        let inputs: Vec<EncodeInput<'_>> = gs.iter().map(|g| g.input(class)).collect();
        let mut enc = DeltaForEncoder::default();
        let built = build_stream(&mut enc, &inputs, 0, true);
        assert_int_roundtrip(&built, &gs.iter().collect::<Vec<_>>(), class);
    }
}

#[test]
fn ffor_roundtrips_across_shapes() {
    let mut seed = 0xFF0_u64;
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    for shape in 0..8 {
        let last = [8192u32, 1025, 64][shape as usize % 3];
        let gs: Vec<GranuleData> = [8192u32, last]
            .iter()
            .map(|&rows| int_corpus(shape, rows, &mut seed))
            .collect();
        let signed = signed_for(shape);
        let inputs: Vec<EncodeInput<'_>> = gs.iter().map(|g| g.input(class)).collect();
        let mut enc = FforEncoder { signed, carry: None };
        let built = build_stream(&mut enc, &inputs, 0, signed);
        assert_int_roundtrip(&built, &gs.iter().collect::<Vec<_>>(), class);
    }
}

// ---------------------------------------------------------------------------
// floats: ALP family, bit-exact incl. NaN payloads; vendored oracle
// ---------------------------------------------------------------------------

fn float_corpus(shape: u32, rows: u32, seed: &mut u64) -> GranuleData {
    roundtrip_float_corpus(shape, rows, seed)
}

#[test]
fn alp_roundtrips_bit_exact_with_oracle_differential() {
    let mut seed = 0xA1B_u64;
    for shape in 0..12 {
        let last = [8192u32, 3000, 1024, 100][shape as usize % 4];
        let gs: Vec<GranuleData> = [8192u32, last]
            .iter()
            .map(|&rows| float_corpus(shape, rows, &mut seed))
            .collect();
        let inputs: Vec<EncodeInput<'_>> = gs.iter().map(|g| g.input(StorageClass::F64)).collect();
        for encoding in [EncodingId::Alp, EncodingId::AlpRd] {
            let mut enc = AlpEncoder { encoding, carry: None };
            let built = build_stream(&mut enc, &inputs, 0, false);
            for (g, gd) in gs.iter().enumerate() {
                let (datums, _arena) = dec_full(&built, g as u32);
                for r in 0..gd.rows as usize {
                    if !gd.input(StorageClass::F64).valid(r as u32) {
                        continue;
                    }
                    assert_eq!(
                        datums[r], gd.datums[r],
                        "shape {shape} granule {g} row {r}: float bits diverged"
                    );
                }
                // Vendored oracle differential on the exact stored frame.
                let ft = built.frame_table.as_deref().expect("alp frame table");
                let section = built.section();
                let hdr = pgrc2_format::part::StreamSectionHdr::decode(section).expect("hdr");
                let payload = crate::section::payload_region(&hdr, section).expect("payload");
                let fs = ft[g] as usize;
                let fe = ft.get(g + 1).map(|&x| x as usize).unwrap_or(payload.len());
                let mut oracle: Vec<u64> = Vec::new();
                alp::granule::decode_frame_words(&payload[fs..fe], &mut oracle)
                    .expect("vendored oracle decodes our frame");
                // The oracle sees placeholder values in null slots; compare
                // ALL slots (placeholders are deterministic 0.0-encodings).
                let (mine, _a) = dec_full(&built, g as u32);
                for r in 0..gd.rows as usize {
                    if gd.input(StorageClass::F64).valid(r as u32) {
                        assert_eq!(mine[r], oracle[r], "oracle diverged at row {r}");
                    }
                }
            }
        }
    }
}

#[test]
fn byte_for_sb3_widths_roundtrip_and_exact_size() {
    // The SB-3 widening: every new byte-aligned width (3/5/6/7) — plus the
    // pow2 sanity pairs — proven end-to-end at its boundary range. The
    // planted min/max inside frame 0 make the stream width deterministic;
    // the size check is independent arithmetic over the frozen §6.4/§6.10
    // geometry (header + per-frame ref + rows×w + frame table), never the
    // encoder's own accounting.
    let mut seed = 0x3B_u64;
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    let cases: [(u64, u8); 8] = [
        (0xFF, 1),
        (0x100, 2),
        (0xFF_FFFF, 3),
        (0x100_0000, 4),
        (0xFF_FFFF_FFFF, 5),
        (0x100_0000_0000, 6),
        (0xFF_FFFF_FFFF_FFFF, 7),
        (0x100_0000_0000_0000, 8),
    ];
    for &(range, want_w) in &cases {
        let rows = 2048u32;
        let base = 5_000_000_000u64;
        let mut datums: Vec<u64> = (0..rows)
            .map(|_| base + splitmix(&mut seed) % (range + 1))
            .collect();
        datums[0] = base; // plant the exact min ...
        datums[1] = base + range; // ... and max inside frame 0
        let gd = GranuleData {
            rows,
            datums,
            validity: None,
        };
        let inputs = [gd.input(class)];
        let w = granule_min_width(&inputs[0], true);
        assert_eq!(w, want_w, "range {range:#x} must elect width {want_w}");
        let mut enc = ByteForEncoder::new_bytefor(8, w, true);
        let built = build_stream(&mut enc, &inputs, 0, true);
        assert_int_roundtrip(&built, &[&gd], class);
        let frames = rows.div_ceil(1024) as usize;
        let expect = 32 + frames * 8 + rows as usize * w as usize + frames * 4;
        assert_eq!(built.section().len(), expect, "width {w}: exact section bytes");
    }
}

// ---------------------------------------------------------------------------
// bool bitmap
// ---------------------------------------------------------------------------

#[test]
fn bool_bitmap_roundtrips() {
    let mut seed = 0xB001_u64;
    for shape in 0..6 {
        let last = [8192u32, 5, 1027][shape as usize % 3];
        let gs: Vec<GranuleData> = [8192u32, last]
            .iter()
            .map(|&rows| {
                let validity = validity_pattern(shape, rows, &mut seed);
                let datums = (0..rows).map(|_| splitmix(&mut seed) % 2).collect();
                GranuleData {
                    rows,
                    datums,
                    validity,
                }
            })
            .collect();
        let inputs: Vec<EncodeInput<'_>> = gs.iter().map(|g| g.input(StorageClass::Bool)).collect();
        let mut enc = BoolBitmapEncoder;
        let built = build_stream(&mut enc, &inputs, 0, false);
        for (g, gd) in gs.iter().enumerate() {
            let (datums, _arena) = dec_full(&built, g as u32);
            for r in 0..gd.rows as usize {
                if gd.input(StorageClass::Bool).valid(r as u32) {
                    assert_eq!(datums[r], gd.datums[r] % 2, "granule {g} row {r}");
                }
            }
        }
    }
}

#[test]
fn alp_f32_roundtrips_bit_exact_with_oracle_differential() {
    // The SB-5 arm under the same two-pass law as the f64 suite: build
    // through the production driver (verify ON), decode again through the
    // registry and compare bits against the INPUT, then run the vendored
    // granule32 reader as a differential oracle on the exact stored frames.
    let mut seed = 0xA32_u64;
    for shape in 0..10 {
        let last = [8192u32, 3000, 1024, 100][shape as usize % 4];
        let gs: Vec<GranuleData> = [8192u32, last]
            .iter()
            .map(|&rows| roundtrip_f32_corpus(shape, rows, &mut seed))
            .collect();
        let inputs: Vec<EncodeInput<'_>> = gs.iter().map(|g| g.input(StorageClass::F32)).collect();
        let mut enc = AlpF32Encoder::default();
        let built = build_stream(&mut enc, &inputs, 0, false);
        for (g, gd) in gs.iter().enumerate() {
            let (datums, _arena) = dec_full(&built, g as u32);
            for r in 0..gd.rows as usize {
                if !gd.input(StorageClass::F32).valid(r as u32) {
                    continue;
                }
                assert_eq!(
                    datums[r], gd.datums[r],
                    "shape {shape} granule {g} row {r}: f32 bits diverged"
                );
            }
            // Vendored oracle differential on the exact stored frame.
            let ft = built.frame_table.as_deref().expect("alp f32 frame table");
            let section = built.section();
            let hdr = pgrc2_format::part::StreamSectionHdr::decode(section).expect("hdr");
            let payload = crate::section::payload_region(&hdr, section).expect("payload");
            let fs = ft[g] as usize;
            let fe = ft.get(g + 1).map(|&x| x as usize).unwrap_or(payload.len());
            let mut oracle: Vec<u32> = Vec::new();
            alp::granule32::decode_frame_bits32(&payload[fs..fe], &mut oracle)
                .expect("vendored oracle decodes our frame");
            for r in 0..gd.rows as usize {
                if gd.input(StorageClass::F32).valid(r as u32) {
                    assert_eq!(
                        datums[r], oracle[r] as u64,
                        "f32 oracle diverged at row {r}"
                    );
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// fsst (ENC 12 first-class; SB-4/OD-5)
// ---------------------------------------------------------------------------

/// One owned varlena image (4B-U header + payload).
pub fn varlena_image(payload: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(4 + payload.len());
    b.extend_from_slice(
        &pgrc2_format::wire::varlena_header_4b_u(payload.len() as u32).to_le_bytes(),
    );
    b.extend_from_slice(payload);
    b
}

/// Deterministic url-shaped payload (the SB-4 target class: templated
/// scheme/host/path segments around high-entropy ids — the corpus
/// `t_url` shape).
pub fn url_shaped_payload(seed: &mut u64) -> Vec<u8> {
    format!(
        "https://svc{}.example.com/api/v2/{}/{}?trace={:016x}",
        splitmix(seed) % 40,
        ["orders", "users", "items", "events"][(splitmix(seed) % 4) as usize],
        splitmix(seed) & 0xFFFF_FFFF,
        splitmix(seed)
    )
    .into_bytes()
}

/// Deterministic near-unique 16-char word token (the corpus `t_belowwin`
/// shape: 38-char alphabet a-z 0-9 _ -).
pub fn word_token_payload(seed: &mut u64) -> Vec<u8> {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789_-";
    (0..16)
        .map(|_| ALPHABET[(splitmix(seed) % ALPHABET.len() as u64) as usize])
        .collect()
}

/// Deterministic log-shaped payload (the corpus `t_log` shape).
pub fn log_shaped_payload(seed: &mut u64) -> Vec<u8> {
    format!(
        "2026-08-{:02}T{:02}:{:02}:{:02}Z {} worker-{} request {:016x} took {}ms",
        1 + splitmix(seed) % 28,
        splitmix(seed) % 24,
        splitmix(seed) % 60,
        splitmix(seed) % 60,
        ["INFO", "WARN", "ERROR", "DEBUG"][(splitmix(seed) % 4) as usize],
        splitmix(seed) % 64,
        splitmix(seed),
        splitmix(seed) % 5000
    )
    .into_bytes()
}

/// FSST value fixture: one granule's owned payloads + images + input data.
pub struct FsstGranule {
    pub payloads: Vec<Vec<u8>>,
    images: Vec<Vec<u8>>,
    pub rows: u32,
    pub validity: Option<Vec<u64>>,
}

impl FsstGranule {
    pub fn new(rows: u32, shape: u32, seed: &mut u64) -> FsstGranule {
        let validity = validity_pattern(shape, rows, seed);
        let payloads: Vec<Vec<u8>> = (0..rows)
            .map(|r| match r % 7 {
                // Empty strings and all-escape binary values ride along
                // with the url tokens (the boundary inputs the task pins).
                0 => Vec::new(),
                1 => (0..9u32)
                    .map(|_| 0x80u8 | (splitmix(seed) as u8 & 0x7F))
                    .collect(),
                _ => url_shaped_payload(seed),
            })
            .collect();
        let images: Vec<Vec<u8>> = payloads.iter().map(|p| varlena_image(p)).collect();
        FsstGranule {
            payloads,
            images,
            rows,
            validity,
        }
    }

    pub fn data(&self) -> GranuleData {
        GranuleData {
            rows: self.rows,
            datums: self.images.iter().map(|b| b.as_ptr() as u64).collect(),
            validity: self.validity.clone(),
        }
    }
}

#[test]
fn fsst_roundtrips_byte_exact() {
    let mut seed = 0xF557_u64;
    for (shape, last) in [(0u32, 8192u32), (1, 700), (2, 33)] {
        let fixtures: Vec<FsstGranule> = [8192u32, last]
            .iter()
            .map(|&rows| FsstGranule::new(rows, shape, &mut seed))
            .collect();
        let gs: Vec<GranuleData> = fixtures.iter().map(|f| f.data()).collect();
        let inputs: Vec<EncodeInput<'_>> = gs
            .iter()
            .map(|g| g.input(StorageClass::VarlenaVerbatim))
            .collect();
        // Per-(column,part) table over every payload (writer grain).
        let sample: Vec<&[u8]> = fixtures
            .iter()
            .flat_map(|f| f.payloads.iter().map(|p| p.as_slice()))
            .collect();
        let table = crate::fsst::FsstSymbolTable::build(&sample);
        let mut enc = crate::fsst::FsstEncoder::new(table);
        // verify=ON: the production driver already round-trips every
        // granule; the loop below is the independent second pass comparing
        // decoded images against the INPUT payloads.
        let built = build_stream(&mut enc, &inputs, 0, false);
        for (g, fx) in fixtures.iter().enumerate() {
            let (datums, _arena) = dec_full(&built, g as u32);
            let gd = fx.data();
            let input = gd.input(StorageClass::VarlenaVerbatim);
            for r in 0..fx.rows as usize {
                if !input.valid(r as u32) {
                    continue;
                }
                // SAFETY: arena-backed varlena datum built by the kernel.
                let got = unsafe { crate::section::varlena_payload(datums[r]).expect("shape") };
                assert_eq!(
                    got,
                    fx.payloads[r].as_slice(),
                    "shape {shape} granule {g} row {r}: fsst bytes diverged"
                );
            }
        }
    }
}

#[test]
fn fsst_grain_ladder_granules_address_by_ordinal() {
    // The wave-5 seal defect in miniature (t_belowwin sealed at a
    // non-default SB-10 grain): granule 1 of a 4096-row-grain part lives
    // at frame ordinal 1, NOT at index 8 of the 8192-keyed `frame_base`
    // closed form. FSST is a granule-framed family — addressing must be
    // the granule ORDINAL, so every ladder grain below round-trips.
    // Under per-1024-row framing + closed-form addressing, every one of
    // these shapes dies at granule 1 (wrong values or out-of-range frame)
    // inside build_stream's mandatory verify. The oracle is the INPUT
    // (decode-and-compare), plus the survivor face on the LAST granule.
    let mut seed = 0xBE10_u64;
    for grain_rows in [4096u32, 2048, 1024] {
        let granules = 3usize;
        let payloads: Vec<Vec<Vec<u8>>> = (0..granules)
            .map(|_| {
                (0..grain_rows)
                    .map(|_| word_token_payload(&mut seed))
                    .collect()
            })
            .collect();
        let images: Vec<Vec<Vec<u8>>> = payloads
            .iter()
            .map(|g| g.iter().map(|p| varlena_image(p)).collect())
            .collect();
        let gs: Vec<GranuleData> = images
            .iter()
            .map(|g| GranuleData {
                rows: grain_rows,
                datums: g.iter().map(|b| b.as_ptr() as u64).collect(),
                validity: None,
            })
            .collect();
        let inputs: Vec<EncodeInput<'_>> = gs
            .iter()
            .map(|g| g.input(StorageClass::VarlenaVerbatim))
            .collect();
        // Per-(column,part) table over every payload (writer grain).
        let sample: Vec<&[u8]> = payloads.iter().flatten().map(|p| p.as_slice()).collect();
        let table = crate::fsst::FsstSymbolTable::build(&sample);
        let mut enc = crate::fsst::FsstEncoder::new(table);
        let built = build_stream(&mut enc, &inputs, 0, false);
        for (g, gp) in payloads.iter().enumerate() {
            let (datums, _arena) = dec_full(&built, g as u32);
            for (r, want) in gp.iter().enumerate() {
                // SAFETY: arena-backed varlena datum built by the kernel.
                let got = unsafe { crate::section::varlena_payload(datums[r]).expect("shape") };
                assert_eq!(
                    got,
                    want.as_slice(),
                    "grain {grain_rows} granule {g} row {r}: wrong value (ordinal addressing)"
                );
            }
        }
        // Survivor face on the LAST granule (the ordinal-addressing edge).
        let sel_rows: Vec<u16> = vec![0, 1, (grain_rows - 1) as u16];
        let ctx = built.ctx(granules as u32 - 1);
        let (sel, _sa) = dec_sel_ctx(&ctx, &sel_rows);
        for (i, &r) in sel_rows.iter().enumerate() {
            // SAFETY: arena-backed varlena datum built by the kernel.
            let got = unsafe { crate::section::varlena_payload(sel[i]).expect("shape") };
            assert_eq!(
                got,
                payloads[granules - 1][r as usize].as_slice(),
                "grain {grain_rows} sel row {r}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// dict codes (global codes; §6.11 frozen blocks)
// ---------------------------------------------------------------------------

/// Byte-rank-sorted dict fixture: entries "k000000".."k{n-1}" zero-padded so
/// byte order == numeric order; returns (index bytes, payload bytes) in
/// 8-aligned buffers (the `SegBuf` substrate model — the zero-copy gather
/// emits pointers into these, and the §7b pins check absolute alignment).
pub fn dict_fixture(n: u32) -> (super::Aligned8, super::Aligned8) {
    let mut index = Vec::new();
    let mut payload = Vec::new();
    for i in 0..n {
        let s = format!("k{i:06}");
        let off = put_varlena_entry(&mut payload, s.as_bytes());
        pgrc2_format::dict::DictIndexEntry {
            payload_off: off as u32,
            byte_len: s.len() as u32,
            char_field: s.len() as u32,
        }
        .encode_into(&mut index);
    }
    (
        super::Aligned8::from_bytes(&index),
        super::Aligned8::from_bytes(&payload),
    )
}

#[test]
fn dict_codes_roundtrip_codes_and_materialization() {
    let mut seed = 0xD1C7_u64;
    let ndv = 3000u32;
    let (index, payload) = dict_fixture(ndv);
    for shape in 0..6 {
        let last = [8192u32, 1024, 77][shape as usize % 3];
        let gs: Vec<GranuleData> = [8192u32, last]
            .iter()
            .map(|&rows| {
                let validity = validity_pattern(shape, rows, &mut seed);
                // Cluster codes so per-granule ranges stay narrow sometimes.
                let base = (splitmix(&mut seed) % 2000) as u64;
                let datums = (0..rows)
                    .map(|_| base + splitmix(&mut seed) % ((shape as u64 % 4) * 250 + 1))
                    .collect();
                GranuleData {
                    rows,
                    datums,
                    validity,
                }
            })
            .collect();
        let inputs: Vec<EncodeInput<'_>> = gs
            .iter()
            .map(|g| g.input(StorageClass::VarlenaVerbatim))
            .collect();
        let mut enc = DictCodesEncoder {
            class: CLASS_VARLENA,
            max_width: 32,
            byte_align: false,
        };
        let built = build_stream(&mut enc, &inputs, 0, false);
        let dict = DictSections {
            index: &index[..],
            payload: &payload[..],
            entry_count: ndv,
            charlen_form: pgrc2_format::dict::DictCharLenForm::Absolute,
        };
        let vt = crate::registry()
            .resolve(built.key())
            .expect("dict kernel resolves");
        for (g, gd) in gs.iter().enumerate() {
            let mut ctx = built.ctx(g as u32);
            ctx.dict = Some(dict);
            // Codes face.
            let mut codes = vec![0u32; gd.rows as usize];
            let n = (vt.decode_codes)(&ctx, &mut codes).expect("decode_codes");
            assert_eq!(n, gd.rows);
            for r in 0..gd.rows as usize {
                if gd.input(StorageClass::VarlenaVerbatim).valid(r as u32) {
                    assert_eq!(codes[r] as u64, gd.datums[r], "granule {g} row {r} code");
                }
            }
            // Materialization face: images must equal the dict entries.
            let mut datums = vec![0u64; gd.rows as usize];
            let mut arena_buf = vec![0u8; 1 << 20];
            let mut out = pgrc2_format::abi::DecodeOut {
                datums: &mut datums,
                arena: ByteArena::new(&mut arena_buf),
            };
            (vt.decode_full)(&ctx, &mut out).expect("decode_full");
            for r in 0..gd.rows as usize {
                if !gd.input(StorageClass::VarlenaVerbatim).valid(r as u32) {
                    continue;
                }
                let want = format!("k{:06}", gd.datums[r]);
                // SAFETY: arena-backed varlena datum built by the kernel.
                let got = unsafe { crate::section::varlena_payload(datums[r]).expect("shape") };
                assert_eq!(got, want.as_bytes(), "granule {g} row {r} image");
            }
            // Dict handle face.
            let layout = (vt.dict_handle)(&ctx).expect("dict_handle");
            assert_eq!(layout.entry_count, ndv);
            assert_eq!(layout.frame_entries, 1024);
        }
    }
}

/// pgrc21-widths: byte-aligned code widths are a strict subset of the
/// frozen §6.11 envelope — a `byte_align: true` stream decodes to the SAME
/// codes as the bit-exact stream, its priced facts round to byte
/// multiples, and the priced bytes match what the encoder seals.
#[test]
fn dict_codes_byte_align_same_codes() {
    let mut seed = 0xBA17_u64;
    for shape in 0..6 {
        let last = [8192u32, 1024, 77][shape as usize % 3];
        let gs: Vec<GranuleData> = [8192u32, last]
            .iter()
            .map(|&rows| {
                let validity = validity_pattern(shape, rows, &mut seed);
                let base = (splitmix(&mut seed) % 2000) as u64;
                // Range dial: per-shape spread hits widths on both sides of
                // a byte boundary (0, <8, 8..16, wide).
                let spread = [1u64, 3, 200, 5000, 70000, 300000][shape as usize];
                let datums = (0..rows)
                    .map(|_| base + splitmix(&mut seed) % spread)
                    .collect();
                GranuleData {
                    rows,
                    datums,
                    validity,
                }
            })
            .collect();
        let inputs: Vec<EncodeInput<'_>> = gs
            .iter()
            .map(|g| g.input(StorageClass::VarlenaVerbatim))
            .collect();
        for input in &inputs {
            let (bytes_bit, w_bit) = crate::dictcodes::granule_block_facts(input, false);
            let (bytes_byte, w_byte) = crate::dictcodes::granule_block_facts(input, true);
            assert_eq!(w_byte % 8, 0, "byte posture width is byte-aligned");
            assert_eq!(w_byte, w_bit.div_ceil(8) * 8, "rounded up, never down");
            assert!(bytes_byte >= bytes_bit);
        }
        let mut arms = Vec::new();
        for byte_align in [false, true] {
            let mut enc = DictCodesEncoder {
                class: CLASS_VARLENA,
                max_width: 32,
                byte_align,
            };
            let built = build_stream(&mut enc, &inputs, 0, false);
            let vt = crate::registry()
                .resolve(built.key())
                .expect("dict kernel resolves");
            let mut all: Vec<Vec<u32>> = Vec::new();
            for (g, gd) in gs.iter().enumerate() {
                let ctx = built.ctx(g as u32);
                let mut codes = vec![0u32; gd.rows as usize];
                let n = (vt.decode_codes)(&ctx, &mut codes).expect("decode_codes");
                assert_eq!(n, gd.rows);
                all.push(codes);
            }
            arms.push(all);
        }
        assert_eq!(arms[0], arms[1], "shape {shape}: codes identical across postures");
    }
}

// ---------------------------------------------------------------------------
// packed numeric (vendored A6b semantics; byte-identical reconstruction)
// ---------------------------------------------------------------------------

fn numeric_images(rows: u32, scale: i32, seed: &mut u64) -> Vec<adt_numeric::NumericImage> {
    (0..rows)
        .map(|_| {
            let mant = (splitmix(seed) % 10_000_000) as i64 - 5_000_000;
            let s = super::electiontests::decimal_string(mant, scale);
            adt_numeric::io::numeric_in(&s, -1, None)
                .expect("parse")
                .expect("non-soft")
        })
        .collect()
}

#[test]
fn packed_numeric_reconstruction_is_byte_identical() {
    let mut seed = 0xA6B_u64;
    for scale in [0i32, 2, 7] {
        let rows = 2048u32;
        let images: Vec<Vec<adt_numeric::NumericImage>> = (0..1)
            .map(|_| numeric_images(rows, scale, &mut seed))
            .collect();
        let gs: Vec<GranuleData> = images
            .iter()
            .map(|imgs| {
                let mut vseed = seed;
                GranuleData {
                    rows,
                    datums: imgs.iter().map(|i| i.as_bytes().as_ptr() as u64).collect(),
                    validity: validity_pattern(1, rows, &mut vseed),
                }
            })
            .collect();
        let inputs: Vec<EncodeInput<'_>> = gs
            .iter()
            .map(|g| g.input(StorageClass::VarlenaVerbatim))
            .collect();
        // Width from the mantissa arm (the election's job; here direct).
        let election = crate::election::elect_numeric(&inputs, usize::MAX / 2).expect("elect");
        let crate::election::Election::Elected {
            encoding: EncodingId::PackedNumeric,
            width,
            aux32,
            ..
        } = election
        else {
            panic!("uniform-scale corpus must elect packed numeric: {election:?}");
        };
        assert_eq!(aux32 as i32, scale);
        let mut enc = PackedNumericEncoder::new(scale, width);
        let built = build_stream(&mut enc, &inputs, 0, true);
        for (g, imgs) in images.iter().enumerate() {
            let (datums, _arena) = dec_full(&built, g as u32);
            let input = gs[g].input(StorageClass::VarlenaVerbatim);
            for r in 0..rows as usize {
                if !input.valid(r as u32) {
                    continue;
                }
                // SAFETY: arena-backed varlena datum built by the kernel.
                let got = unsafe { crate::section::varlena_payload(datums[r]).expect("shape") };
                assert_eq!(
                    got,
                    &imgs[r].as_bytes()[4..],
                    "granule {g} row {r}: numeric bytes diverged"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// array dual (structural: split + child encode + assemble)
// ---------------------------------------------------------------------------

/// Hand-built 1-D int4 array image (payload form PG produces: ndim 1,
/// dataoffset 0, lbound 1) — or the ndim-0 empty form.
pub fn int4_array_image(elems: &[i32]) -> Vec<u8> {
    const INT4_OID: u32 = 23;
    let payload_len = if elems.is_empty() {
        12
    } else {
        20 + elems.len() * 4
    };
    let mut b = Vec::with_capacity(4 + payload_len);
    b.extend_from_slice(&pgrc2_format::wire::varlena_header_4b_u(payload_len as u32).to_le_bytes());
    if elems.is_empty() {
        b.extend_from_slice(&0i32.to_le_bytes());
        b.extend_from_slice(&0i32.to_le_bytes());
        b.extend_from_slice(&INT4_OID.to_le_bytes());
    } else {
        b.extend_from_slice(&1i32.to_le_bytes());
        b.extend_from_slice(&0i32.to_le_bytes());
        b.extend_from_slice(&INT4_OID.to_le_bytes());
        b.extend_from_slice(&(elems.len() as i32).to_le_bytes());
        b.extend_from_slice(&1i32.to_le_bytes());
        for e in elems {
            b.extend_from_slice(&e.to_le_bytes());
        }
    }
    b
}

#[test]
fn array_dual_split_encode_assemble_is_byte_identical() {
    let mut seed = 0xA9_u64;
    let facts = ArrayElemFacts {
        elemtype: 23,
        elem_len: 4,
    };
    let rows = 1200u32;
    let arrays: Vec<Vec<u8>> = (0..rows)
        .map(|_| {
            let n = (splitmix(&mut seed) % 7) as usize;
            let elems: Vec<i32> = (0..n).map(|_| splitmix(&mut seed) as i32).collect();
            int4_array_image(&elems)
        })
        .collect();
    let mut vseed = seed;
    let validity = validity_pattern(1, rows, &mut vseed);
    let mut datums: Vec<u64> = arrays.iter().map(|a| a.as_ptr() as u64).collect();
    // Null rows must not carry stale pointers into the split.
    if let Some(v) = &validity {
        for r in 0..rows {
            if v[(r / 64) as usize] >> (r % 64) & 1 == 0 {
                datums[r as usize] = 0;
            }
        }
    }
    let gd = GranuleData {
        rows,
        datums,
        validity,
    };
    let input = gd.input(StorageClass::VarlenaVerbatim);
    let split = elect_array_split(&input, facts)
        .expect("split ok")
        .expect("shape accepted");

    // Encode the two substreams through the ordinary int kernels.
    let sizes_gd = GranuleData {
        rows,
        datums: split.sizes.clone(),
        validity: None, // sizes are row-aligned with 0 for null rows
    };
    let sclass = StorageClass::ByvalWord {
        width: 8,
        signed: false,
    };
    let s_in = [sizes_gd.input(sclass)];
    let s_width = granule_min_width(&s_in[0], false);
    let mut s_enc = ByteForEncoder::new_bytefor(8, s_width, false);
    let s_built = build_stream(&mut s_enc, &s_in, 0, false);
    let (dec_sizes, _sa) = dec_full(&s_built, 0);

    let elems_gd = GranuleData {
        rows: split.elems.len() as u32,
        datums: split.elems.clone(),
        validity: None,
    };
    let e_in = [elems_gd.input(sclass)];
    let e_width = granule_min_width(&e_in[0], false);
    let mut e_enc = ByteForEncoder::new_bytefor(8, e_width, false);
    let e_built = build_stream(&mut e_enc, &e_in, 0, false);
    let (dec_elems, _ea) = dec_full(&e_built, 0);

    // Assemble and byte-compare.
    let mut arena_buf = vec![0u8; crate::arraydual::assembled_arena_bytes(facts, &dec_sizes) + 64];
    let mut arena = ByteArena::new(&mut arena_buf);
    let mut out = vec![0u64; rows as usize];
    let validity2 = gd.validity.clone();
    assemble_array_datums(
        facts,
        &dec_sizes,
        &dec_elems,
        |r| match &validity2 {
            None => true,
            Some(w) => w[(r / 64) as usize] >> (r % 64) & 1 == 1,
        },
        &mut out,
        &mut arena,
    )
    .expect("assemble");
    for r in 0..rows as usize {
        if !input.valid(r as u32) {
            continue;
        }
        // SAFETY: arena-backed varlena datum.
        let got = unsafe { crate::section::varlena_payload(out[r]).expect("shape") };
        assert_eq!(got, &arrays[r][4..], "row {r}: array bytes diverged");
        assert_eq!(out[r] % 8, 0, "row {r}: array datum not 8-aligned");
    }
}

#[test]
fn array_dual_refuses_foreign_shapes() {
    let facts = ArrayElemFacts {
        elemtype: 23,
        elem_len: 4,
    };
    // Wrong elemtype refuses.
    let img = int4_array_image(&[1, 2, 3]);
    let datums = [img.as_ptr() as u64];
    let input = EncodeInput {
        class: StorageClass::VarlenaVerbatim,
        rows: 1,
        datums: &datums,
        validity: None,
    };
    let wrong = ArrayElemFacts {
        elemtype: 20,
        elem_len: 8,
    };
    assert!(elect_array_split(&input, wrong).expect("ok").is_none());
    // 2-D refuses.
    let mut twod = int4_array_image(&[1, 2, 3, 9]);
    twod[4..8].copy_from_slice(&2i32.to_le_bytes());
    let datums2 = [twod.as_ptr() as u64];
    let input2 = EncodeInput {
        class: StorageClass::VarlenaVerbatim,
        rows: 1,
        datums: &datums2,
        validity: None,
    };
    assert!(elect_array_split(&input2, facts).expect("ok").is_none());
    // lbound != 1 refuses.
    let mut lb = int4_array_image(&[1, 2, 3]);
    lb[20..24].copy_from_slice(&0i32.to_le_bytes());
    let datums3 = [lb.as_ptr() as u64];
    let input3 = EncodeInput {
        class: StorageClass::VarlenaVerbatim,
        rows: 1,
        datums: &datums3,
        validity: None,
    };
    assert!(elect_array_split(&input3, facts).expect("ok").is_none());
}

// ---------------------------------------------------------------------------
// wrapper: wrapped sections decode to the same values
// ---------------------------------------------------------------------------

#[test]
fn zstd_wrapped_section_unwraps_to_identical_decode() {
    // The CMP-A arm under the same law as LZ4: wrap → unwrap → every
    // granule decodes byte-identically to the unwrapped original.
    let mut seed = 0x117B_u64;
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    let gs: Vec<GranuleData> = (0..3).map(|_| int_corpus(5, 8192, &mut seed)).collect();
    let inputs: Vec<EncodeInput<'_>> = gs.iter().map(|g| g.input(class)).collect();
    let width = inputs
        .iter()
        .map(|g| granule_min_width(g, true))
        .max()
        .unwrap();
    let mut enc = ByteForEncoder::new_bytefor(8, width, true);
    let built = build_stream(&mut enc, &inputs, 0, true);

    let mut wrapped = Vec::new();
    wrap_section(
        built.section(),
        &built.build.granule_payload_ends,
        pgrc2_format::enc::Wrapper::Zstd,
        &mut wrapped,
    )
    .expect("wrap");
    let mut scratch = Vec::new();
    unwrap_section(&wrapped, &mut scratch).expect("unwrap");

    // The rebuilt image is the unwrapped section verbatim (stronger than
    // value equality: the unwrap twin law is byte-exact end to end).
    assert_eq!(
        scratch,
        built.section(),
        "zstd unwrap must rebuild the unwrapped section byte-exactly"
    );

    let hdr = pgrc2_format::part::StreamSectionHdr::decode(&scratch).expect("hdr");
    let ft = hdr.frame_table(&scratch).expect("ft");
    for (g, gd) in gs.iter().enumerate() {
        let (orig, _oa) = dec_full(&built, g as u32);
        let ctx = built.ctx_with(g as u32, built.key(), &scratch, ft.as_deref());
        let (twin, _ta) = dec_full_ctx(&ctx);
        let input = gd.input(class);
        for r in 0..gd.rows as usize {
            if input.valid(r as u32) {
                assert_eq!(
                    orig[r], twin[r],
                    "granule {g} row {r} diverged after zstd unwrap"
                );
            }
        }
    }
}

#[test]
fn lz4_wrapped_section_unwraps_to_identical_decode() {
    let mut seed = 0x117A_u64;
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    let gs: Vec<GranuleData> = (0..3).map(|_| int_corpus(5, 8192, &mut seed)).collect();
    let inputs: Vec<EncodeInput<'_>> = gs.iter().map(|g| g.input(class)).collect();
    let width = inputs
        .iter()
        .map(|g| granule_min_width(g, true))
        .max()
        .unwrap();
    let mut enc = ByteForEncoder::new_bytefor(8, width, true);
    let built = build_stream(&mut enc, &inputs, 0, true);

    let mut wrapped = Vec::new();
    wrap_section(
        built.section(),
        &built.build.granule_payload_ends,
        pgrc2_format::enc::Wrapper::Lz4,
        &mut wrapped,
    )
    .expect("wrap");
    let mut scratch = Vec::new();
    unwrap_section(&wrapped, &mut scratch).expect("unwrap");

    // Decode every granule from the unwrapped twin; values must match the
    // original decode exactly.
    let hdr = pgrc2_format::part::StreamSectionHdr::decode(&scratch).expect("hdr");
    let ft = hdr.frame_table(&scratch).expect("ft");
    for (g, gd) in gs.iter().enumerate() {
        let (orig, _oa) = dec_full(&built, g as u32);
        let ctx = built.ctx_with(g as u32, built.key(), &scratch, ft.as_deref());
        let (twin, _ta) = dec_full_ctx(&ctx);
        let input = gd.input(class);
        for r in 0..gd.rows as usize {
            if input.valid(r as u32) {
                assert_eq!(
                    orig[r], twin[r],
                    "granule {g} row {r} diverged after unwrap"
                );
            }
        }
    }
}
