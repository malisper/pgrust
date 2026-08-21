//! The six-face ABI proof (M3-A exit slice: "reference Verbatim/Const codec
//! round-trips through the full six-face ABI"): encode through the frozen
//! encode faces → decode through every decode face → byte-exact comparison,
//! for every storage class, with nulls, selections, multi-granule extents,
//! and the oversize overflow region. Also pins the StrView §7b law (decoded
//! varlena datums are varlena-shaped, 8-aligned) and the
//! `decode_sel ≡ decode_full ∘ select` property at reference grain.

use crate::abi::{
    verify_roundtrip, ByteArena, CodecRegistry, DecodeOut, EncodeInput, GranuleEncoder, KernelCtx,
    KernelKey, Selection, ValidityVerdict,
};
use crate::class::{ClassHint, StorageClass};
use crate::enc::{EncodingId, Wrapper};
use crate::geom::{GRANULE_ROWS, OVERSIZE_THRESHOLD};
use crate::meta::{MetaAnswer, MetaProbe, Sortedness};
use crate::part::{OverflowSink, StreamSectionHdr, StreamSectionWriter};
use crate::verbatim::{encode_validity_bitmap, reference_vtables, ConstEncoder, VerbatimEncoder};
use crate::wire::varlena_4b_u_payload_len;
use crate::FormatResult;

/// One encoded stream extent + its side artifacts.
struct EncodedStream {
    section: Vec<u8>,
    overflow: Vec<u8>,
    frame_table: Option<Vec<u32>>,
    /// Per-granule (rows, validity words) copies for ctx construction.
    granules: Vec<(u32, Option<Vec<u64>>)>,
    key: KernelKey,
    flags: u16,
    fixed_len: u32,
}

fn encode_stream(
    enc: &mut dyn GranuleEncoder,
    granules: &[EncodeInput<'_>],
) -> FormatResult<EncodedStream> {
    let key = enc.key();
    let mut section = Vec::new();
    let mut overflow = Vec::new();
    {
        let mut w =
            StreamSectionWriter::begin(&mut section, key.encoding, key.width, Wrapper::None)?;
        let mut ovf = OverflowSink::new(&mut overflow);
        for g in granules {
            enc.encode_granule(g, &mut w, &mut ovf)?;
        }
        enc.finish_stream(&mut w)?;
        w.finish(false)?;
    }
    let hdr = StreamSectionHdr::decode(&section)?;
    let frame_table = hdr.frame_table(&section)?;
    let mut gmeta = Vec::new();
    let mut fixed_len = 0;
    for g in granules {
        if let StorageClass::Fixed { len } = g.class {
            fixed_len = len;
        }
        gmeta.push((g.rows, g.validity.map(|v| v.to_vec())));
    }
    let flags = if granules.first().map(|g| g.class.signed()).unwrap_or(false) {
        crate::part::STREAMF_SIGNED
    } else {
        0
    };
    Ok(EncodedStream {
        section,
        overflow,
        frame_table,
        granules: gmeta,
        key,
        flags,
        fixed_len,
    })
}

impl EncodedStream {
    fn ctx<'a>(&'a self, g: u32, validity_bitmap: Option<&'a [u8]>) -> KernelCtx<'a> {
        let rows = self.granules[g as usize].0;
        KernelCtx {
            key: self.key,
            flags: self.flags,
            fixed_len: self.fixed_len,
            bytes: &self.section,
            frame_table: self.frame_table.as_deref(),
            granule: g,
            granule_in_extent: g,
            rows,
            values: rows,
            validity_bytes: validity_bitmap,
            overflow: if self.overflow.is_empty() {
                None
            } else {
                Some(&self.overflow)
            },
            dict: None,
        }
    }
}

fn registry() -> CodecRegistry {
    // Static registry over the reference vtables (leaked once per test
    // process — test scaffolding, not product allocation policy).
    let leaked: &'static [&'static crate::abi::CodecVtable] =
        Box::leak(Box::new(reference_vtables()));
    CodecRegistry::new(leaked)
}

/// Decode granule `g` full and compare datums against expectations through
/// the canonical-bytes lens (null rows skipped — validity is the truth).
fn assert_full_roundtrip(
    reg: &CodecRegistry,
    s: &EncodedStream,
    g: u32,
    input: &EncodeInput<'_>,
    validity_bitmap: Option<&[u8]>,
) {
    let vt = reg.resolve(s.key).expect("kernel resolves");
    let ctx = s.ctx(g, validity_bitmap);
    let mut datums = vec![0u64; input.rows as usize];
    let mut arena_buf = vec![0u8; 512 * 1024];
    let mut out = DecodeOut {
        datums: &mut datums,
        arena: ByteArena::new(&mut arena_buf),
    };
    let n = (vt.decode_full)(&ctx, &mut out).expect("decode_full");
    assert_eq!(n, input.rows);
    for r in 0..input.rows {
        if !input.valid(r) {
            continue;
        }
        let mut s_in = [0u8; 8];
        let mut s_out = [0u8; 8];
        let a = unsafe {
            crate::abi::datum_canonical_bytes(input.class, input.datums[r as usize], &mut s_in)
        }
        .expect("input canon");
        let b = unsafe {
            crate::abi::datum_canonical_bytes(input.class, out.datums[r as usize], &mut s_out)
        }
        .expect("output canon");
        assert_eq!(a, b, "row {r} mismatched");
        // StrView §7b pin: pointer-class outputs are 8-aligned and (for
        // varlena) carry a valid 4B-U header.
        if matches!(input.class, StorageClass::VarlenaVerbatim) {
            let ptr = out.datums[r as usize];
            assert_eq!(ptr % 8, 0, "varlena output not 8-aligned (spec §19.4)");
            let header = u32::from_le_bytes(unsafe {
                core::slice::from_raw_parts(ptr as *const u8, 4)
                    .try_into()
                    .expect("len 4")
            });
            varlena_4b_u_payload_len(header, "pin").expect("varlena-shaped output (StrView §7b)");
        }
    }
}

/// decode_sel ≡ decode_full ∘ select at reference grain.
fn assert_sel_matches_full(
    reg: &CodecRegistry,
    s: &EncodedStream,
    g: u32,
    input: &EncodeInput<'_>,
    validity_bitmap: Option<&[u8]>,
    sel_rows: &[u16],
) {
    let vt = reg.resolve(s.key).expect("kernel resolves");
    let ctx = s.ctx(g, validity_bitmap);
    let sel = Selection { rows: sel_rows };
    sel.validate(input.rows).expect("selection valid");
    let mut d_full = vec![0u64; input.rows as usize];
    let mut a_full = vec![0u8; 512 * 1024];
    let mut out_full = DecodeOut {
        datums: &mut d_full,
        arena: ByteArena::new(&mut a_full),
    };
    (vt.decode_full)(&ctx, &mut out_full).expect("decode_full");
    let mut d_sel = vec![0u64; sel_rows.len()];
    let mut a_sel = vec![0u8; 512 * 1024];
    let mut out_sel = DecodeOut {
        datums: &mut d_sel,
        arena: ByteArena::new(&mut a_sel),
    };
    let n = (vt.decode_sel)(&ctx, &sel, &mut out_sel).expect("decode_sel");
    assert_eq!(n as usize, sel_rows.len());
    for (i, &r) in sel_rows.iter().enumerate() {
        if !input.valid(r as u32) {
            continue;
        }
        let mut s_a = [0u8; 8];
        let mut s_b = [0u8; 8];
        let a = unsafe {
            crate::abi::datum_canonical_bytes(input.class, out_full.datums[r as usize], &mut s_a)
        }
        .expect("full canon");
        let b =
            unsafe { crate::abi::datum_canonical_bytes(input.class, out_sel.datums[i], &mut s_b) }
                .expect("sel canon");
        assert_eq!(a, b, "decode_sel diverged from decode_full at row {r}");
    }
}

fn validity_words(valid: &[bool]) -> Vec<u64> {
    let mut words = vec![0u64; valid.len().div_ceil(64)];
    for (i, &v) in valid.iter().enumerate() {
        if v {
            words[i / 64] |= 1 << (i % 64);
        }
    }
    words
}

// ---------------------------------------------------------------------------
// byval / float / bool
// ---------------------------------------------------------------------------

#[test]
fn verbatim_byval_all_widths_roundtrip() {
    let reg = registry();
    for (width, signed) in [
        (1u8, true),
        (2, true),
        (4, true),
        (8, true),
        (4, false),
        (8, false),
    ] {
        let class = StorageClass::ByvalWord { width, signed };
        let rows: u32 = 2500;
        let datums: Vec<u64> = (0..rows as u64)
            .map(|i| {
                let v = i.wrapping_mul(0x9E37_79B9).wrapping_sub(1000);
                let masked = if width == 8 {
                    v
                } else {
                    v & ((1u64 << (width * 8)) - 1)
                };
                crate::verbatim::extend_word(masked, width, signed)
            })
            .collect();
        let input = EncodeInput {
            class,
            rows,
            datums: &datums,
            validity: None,
        };
        let mut enc = VerbatimEncoder { class };
        let s = encode_stream(&mut enc, &[input]).expect("encodes");
        let input = EncodeInput {
            class,
            rows,
            datums: &datums,
            validity: None,
        };
        assert_full_roundtrip(&reg, &s, 0, &input, None);
        assert_sel_matches_full(&reg, &s, 0, &input, None, &[0, 1, 17, 999, 2499]);
    }
}

#[test]
fn verbatim_floats_bit_exact_roundtrip() {
    let reg = registry();
    // f64 incl. NaN payloads, -0.0, infinities: BIT exactness.
    let vals: [f64; 6] = [0.0, -0.0, f64::NAN, f64::INFINITY, -1.5e300, 3.25];
    let datums: Vec<u64> = vals.iter().map(|v| v.to_bits()).collect();
    let input = EncodeInput {
        class: StorageClass::F64,
        rows: 6,
        datums: &datums,
        validity: None,
    };
    let mut enc = VerbatimEncoder {
        class: StorageClass::F64,
    };
    let s = encode_stream(&mut enc, &[input]).expect("encodes");
    let input = EncodeInput {
        class: StorageClass::F64,
        rows: 6,
        datums: &datums,
        validity: None,
    };
    assert_full_roundtrip(&reg, &s, 0, &input, None);

    let vals32: [f32; 4] = [0.0, -0.0, f32::NAN, 1.5];
    let datums32: Vec<u64> = vals32.iter().map(|v| v.to_bits() as u64).collect();
    let input = EncodeInput {
        class: StorageClass::F32,
        rows: 4,
        datums: &datums32,
        validity: None,
    };
    let mut enc = VerbatimEncoder {
        class: StorageClass::F32,
    };
    let s = encode_stream(&mut enc, &[input]).expect("encodes");
    let input = EncodeInput {
        class: StorageClass::F32,
        rows: 4,
        datums: &datums32,
        validity: None,
    };
    assert_full_roundtrip(&reg, &s, 0, &input, None);
}

#[test]
fn verbatim_bool_roundtrip() {
    let reg = registry();
    let datums: Vec<u64> = (0..100).map(|i| (i % 3 == 0) as u64).collect();
    let input = EncodeInput {
        class: StorageClass::Bool,
        rows: 100,
        datums: &datums,
        validity: None,
    };
    let mut enc = VerbatimEncoder {
        class: StorageClass::Bool,
    };
    let s = encode_stream(&mut enc, &[input]).expect("encodes");
    let input = EncodeInput {
        class: StorageClass::Bool,
        rows: 100,
        datums: &datums,
        validity: None,
    };
    assert_full_roundtrip(&reg, &s, 0, &input, None);
}

// ---------------------------------------------------------------------------
// fixed / varlena (+nulls, +overflow, +multi-granule)
// ---------------------------------------------------------------------------

#[test]
fn verbatim_fixed16_with_nulls_roundtrip() {
    let reg = registry();
    let rows: u32 = 300;
    let images: Vec<[u8; 16]> = (0..rows)
        .map(|i| {
            let mut b = [0u8; 16];
            b[..4].copy_from_slice(&i.to_le_bytes());
            b[15] = 0xAB;
            b
        })
        .collect();
    let valid: Vec<bool> = (0..rows).map(|i| i % 7 != 3).collect();
    let vwords = validity_words(&valid);
    let datums: Vec<u64> = images.iter().map(|b| b.as_ptr() as u64).collect();
    let class = StorageClass::Fixed { len: 16 };
    let input = EncodeInput {
        class,
        rows,
        datums: &datums,
        validity: Some(&vwords),
    };
    let mut enc = VerbatimEncoder { class };
    let s = encode_stream(&mut enc, &[input]).expect("encodes");
    let mut bitmap = Vec::new();
    encode_validity_bitmap(Some(&vwords), rows, &mut bitmap);
    let input = EncodeInput {
        class,
        rows,
        datums: &datums,
        validity: Some(&vwords),
    };
    assert_full_roundtrip(&reg, &s, 0, &input, Some(&bitmap));
    assert_sel_matches_full(&reg, &s, 0, &input, Some(&bitmap), &[3, 4, 10, 299]);
}

#[test]
fn verbatim_varlena_multi_granule_overflow_roundtrip() {
    let reg = registry();
    // Two granules + a short tail granule; strings of every interesting
    // length class incl. ONE oversize value (the overflow region, spec
    // §6.8) and interspersed nulls.
    let g_rows: [u32; 3] = [GRANULE_ROWS, GRANULE_ROWS, 1500];
    let mut all_strings: Vec<Vec<Vec<u8>>> = Vec::new();
    let mut all_valid: Vec<Vec<bool>> = Vec::new();
    for (gi, &rows) in g_rows.iter().enumerate() {
        let mut strings = Vec::with_capacity(rows as usize);
        let mut valid = Vec::with_capacity(rows as usize);
        for r in 0..rows {
            let is_null = (r + gi as u32) % 11 == 5;
            valid.push(!is_null);
            let s: Vec<u8> = match r % 5 {
                0 => Vec::new(),
                1 => b"short".to_vec(),
                2 => vec![b'x'; 13],
                3 => {
                    let mut v = format!("row-{gi}-{r}-").into_bytes();
                    v.resize(100, b'y');
                    v
                }
                _ => vec![b'z'; 40],
            };
            strings.push(s);
        }
        // One oversize value per first granule.
        if gi == 0 {
            strings[77] = vec![b'O'; OVERSIZE_THRESHOLD as usize + 123];
            valid[77] = true;
        }
        all_strings.push(strings);
        all_valid.push(valid);
    }
    // Build varlena-shaped input images (EncodeInput contract).
    let images: Vec<Vec<Vec<u8>>> = all_strings
        .iter()
        .map(|g| {
            g.iter()
                .map(|s| {
                    let mut img = crate::wire::varlena_header_4b_u(s.len() as u32)
                        .to_le_bytes()
                        .to_vec();
                    img.extend_from_slice(s);
                    img
                })
                .collect()
        })
        .collect();
    let class = StorageClass::VarlenaVerbatim;
    let mut enc = VerbatimEncoder { class };
    let datum_vecs: Vec<Vec<u64>> = images
        .iter()
        .map(|g| g.iter().map(|img| img.as_ptr() as u64).collect())
        .collect();
    let vword_vecs: Vec<Vec<u64>> = all_valid.iter().map(|v| validity_words(v)).collect();
    let inputs: Vec<EncodeInput<'_>> = (0..3)
        .map(|g| EncodeInput {
            class,
            rows: g_rows[g],
            datums: &datum_vecs[g],
            validity: Some(&vword_vecs[g]),
        })
        .collect();
    let s = encode_stream(&mut enc, &inputs).expect("encodes");
    assert!(
        !s.overflow.is_empty(),
        "the oversize value must have used the overflow stream"
    );
    for g in 0..3u32 {
        let mut bitmap = Vec::new();
        encode_validity_bitmap(
            Some(&vword_vecs[g as usize]),
            g_rows[g as usize],
            &mut bitmap,
        );
        let input = EncodeInput {
            class,
            rows: g_rows[g as usize],
            datums: &datum_vecs[g as usize],
            validity: Some(&vword_vecs[g as usize]),
        };
        assert_full_roundtrip(&reg, &s, g, &input, Some(&bitmap));
        assert_sel_matches_full(
            &reg,
            &s,
            g,
            &input,
            Some(&bitmap),
            &[0, 5, 76, 77, 78, 1400],
        );
    }
}

// ---------------------------------------------------------------------------
// const
// ---------------------------------------------------------------------------

#[test]
fn const_byval_and_varlena_roundtrip() {
    let reg = registry();
    // Byval const with interspersed nulls.
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    let rows: u32 = 1000;
    let datums: Vec<u64> = vec![0xFFFF_FFFF_FFFF_FF85; rows as usize]; // -123 sign-extended
    let valid: Vec<bool> = (0..rows).map(|i| i % 4 != 1).collect();
    let vwords = validity_words(&valid);
    let mut enc = ConstEncoder::new(class);
    let input = EncodeInput {
        class,
        rows,
        datums: &datums,
        validity: Some(&vwords),
    };
    let s = encode_stream(&mut enc, &[input]).expect("encodes");
    let mut bitmap = Vec::new();
    encode_validity_bitmap(Some(&vwords), rows, &mut bitmap);
    let input = EncodeInput {
        class,
        rows,
        datums: &datums,
        validity: Some(&vwords),
    };
    assert_full_roundtrip(&reg, &s, 0, &input, Some(&bitmap));
    assert_sel_matches_full(&reg, &s, 0, &input, Some(&bitmap), &[1, 2, 999]);

    // Varlena const.
    let img = {
        let payload = b"the-constant";
        let mut v = crate::wire::varlena_header_4b_u(payload.len() as u32)
            .to_le_bytes()
            .to_vec();
        v.extend_from_slice(payload);
        v
    };
    let class = StorageClass::VarlenaVerbatim;
    let datums: Vec<u64> = vec![img.as_ptr() as u64; 200];
    let mut enc = ConstEncoder::new(class);
    let input = EncodeInput {
        class,
        rows: 200,
        datums: &datums,
        validity: None,
    };
    let s = encode_stream(&mut enc, &[input]).expect("encodes");
    let input = EncodeInput {
        class,
        rows: 200,
        datums: &datums,
        validity: None,
    };
    assert_full_roundtrip(&reg, &s, 0, &input, None);

    // Sortedness answers Constant through the meta_probe face.
    let vt = reg.resolve(s.key).expect("resolves");
    let ctx = s.ctx(0, None);
    assert_eq!(
        (vt.meta_probe)(&ctx, &MetaProbe::Sortedness).expect("probe"),
        MetaAnswer::Sorted(Sortedness::Constant)
    );
}

#[test]
fn const_all_null_extent_roundtrip() {
    let reg = registry();
    let class = StorageClass::ByvalWord {
        width: 4,
        signed: true,
    };
    let rows: u32 = 100;
    let datums = vec![0u64; rows as usize];
    let vwords = vec![0u64; 2]; // all null
    let mut enc = ConstEncoder::new(class);
    let input = EncodeInput {
        class,
        rows,
        datums: &datums,
        validity: Some(&vwords),
    };
    let s = encode_stream(&mut enc, &[input]).expect("encodes");
    let mut bitmap = Vec::new();
    encode_validity_bitmap(Some(&vwords), rows, &mut bitmap);
    // NonNullCount == 0 via the meta face (validity is the truth).
    let vt = reg.resolve(s.key).expect("resolves");
    let ctx = s.ctx(0, Some(&bitmap));
    assert_eq!(
        (vt.meta_probe)(&ctx, &MetaProbe::NonNullCount).expect("probe"),
        MetaAnswer::Count(0)
    );
    // decode_full succeeds (datum contents unspecified for null rows).
    let mut datums_out = vec![1u64; rows as usize];
    let mut arena = vec![0u8; 1024];
    let mut out = DecodeOut {
        datums: &mut datums_out,
        arena: ByteArena::new(&mut arena),
    };
    assert_eq!((vt.decode_full)(&ctx, &mut out).expect("decode"), rows);
}

// ---------------------------------------------------------------------------
// validity face + meta face + verify_roundtrip teeth
// ---------------------------------------------------------------------------

#[test]
fn validity_face_short_circuit_and_popcount() {
    let reg = registry();
    let class = StorageClass::ByvalWord {
        width: 4,
        signed: false,
    };
    let rows: u32 = 130;
    let datums: Vec<u64> = (0..rows as u64).collect();
    let mut enc = VerbatimEncoder { class };
    let input = EncodeInput {
        class,
        rows,
        datums: &datums,
        validity: None,
    };
    let s = encode_stream(&mut enc, &[input]).expect("encodes");
    let vt = reg.resolve(s.key).expect("resolves");
    // AllValid short-circuit: out untouched.
    let ctx = s.ctx(0, None);
    let mut out = [0xAAu64; 3];
    assert_eq!(
        (vt.validity)(&ctx, &mut out).expect("validity"),
        ValidityVerdict::AllValid
    );
    assert_eq!(out, [0xAA; 3]);
    // Mixed: exact popcount + masked tail.
    let valid: Vec<bool> = (0..rows).map(|i| i % 3 == 0).collect();
    let vwords = validity_words(&valid);
    let mut bitmap = Vec::new();
    encode_validity_bitmap(Some(&vwords), rows, &mut bitmap);
    let ctx = s.ctx(0, Some(&bitmap));
    let mut out = [0u64; 3];
    let expected_nonnull = valid.iter().filter(|&&v| v).count() as u32;
    assert_eq!(
        (vt.validity)(&ctx, &mut out).expect("validity"),
        ValidityVerdict::Mixed {
            nonnull: expected_nonnull
        }
    );
    assert_eq!(out[0], vwords[0]);
    assert_eq!(
        (vt.meta_probe)(&ctx, &MetaProbe::NonNullCount).expect("probe"),
        MetaAnswer::Count(expected_nonnull as u64)
    );
    assert_eq!(
        (vt.meta_probe)(&ctx, &MetaProbe::RowCount).expect("probe"),
        MetaAnswer::Count(rows as u64)
    );
}

#[test]
fn verify_roundtrip_passes_and_catches_seeded_corruption() {
    let reg = registry();
    let class = StorageClass::ByvalWord {
        width: 2,
        signed: false,
    };
    let rows: u32 = 512;
    let datums: Vec<u64> = (0..rows as u64).map(|i| i * 3 % 65_536).collect();
    let mut enc = VerbatimEncoder { class };
    let input = EncodeInput {
        class,
        rows,
        datums: &datums,
        validity: None,
    };
    let mut s = encode_stream(&mut enc, &[input]).expect("encodes");
    let vt = reg.resolve(s.key).expect("resolves");
    let input = EncodeInput {
        class,
        rows,
        datums: &datums,
        validity: None,
    };
    let mut scratch = vec![0u64; rows as usize];
    let mut arena = vec![0u8; 4096];
    verify_roundtrip(vt, &s.ctx(0, None), &input, &mut scratch, &mut arena)
        .expect("clean round-trip verifies");
    // Seeded corruption: flip one payload byte — the verifier must fire
    // (the encode-side election-quadruple tooth).
    let idx = crate::part::STREAM_SECTION_HDR_LEN + 100;
    s.section[idx] ^= 0xFF;
    let err = verify_roundtrip(vt, &s.ctx(0, None), &input, &mut scratch, &mut arena)
        .expect_err("corruption must fail verification");
    assert_eq!(
        err,
        crate::FormatError::EncodeContract {
            detail: "round-trip value mismatch"
        }
    );
}

// ---------------------------------------------------------------------------
// class derivation sanity used across the suite
// ---------------------------------------------------------------------------

#[test]
fn storage_class_derivation_matrix() {
    use crate::class::{CLASS_BOOL, CLASS_BYVAL, CLASS_F32, CLASS_F64, CLASS_FIXED, CLASS_VARLENA};
    let d = |l, b, a, h| StorageClass::derive(l, b, a, h).expect("derives");
    assert_eq!(d(8, true, b'd', ClassHint::Signed).id(), CLASS_BYVAL);
    assert_eq!(d(4, true, b'i', ClassHint::Float).id(), CLASS_F32);
    assert_eq!(d(8, true, b'd', ClassHint::Float).id(), CLASS_F64);
    assert_eq!(d(1, true, b'c', ClassHint::Bool).id(), CLASS_BOOL);
    assert_eq!(d(16, false, b'c', ClassHint::None).id(), CLASS_FIXED);
    assert_eq!(d(-1, false, b'i', ClassHint::None).id(), CLASS_VARLENA);
    // Encoding id of the reference codec keys.
    assert_eq!(EncodingId::Verbatim.as_u16(), 0);
    assert_eq!(EncodingId::Const.as_u16(), 1);
}

// ---------------------------------------------------------------------------
// FlatStats (pgrc2.1 §2.1): SoA transposition round-trips against the
// contributing §8.1 records; alignment law holds on an 8-aligned base.
// ---------------------------------------------------------------------------

#[test]
fn flatstats_roundtrip_and_meet_laws() {
    use crate::meta::{
        flatstats_encode, flatstats_section_len, FlatStatsRef, KeyKind, StatsRecord,
        FLATSTATSF_ALL_ASCII, FLATSTATSF_COMPUTED, STATSF_ALL_ASCII, STATSF_COMPUTED,
    };
    let mk = |min: i64, max: i64, sum: i128, zc: u64, nn: u32, kk: KeyKind, fl: u16| {
        let mut r = StatsRecord::absent();
        r.min_key = min;
        r.max_key = max;
        r.sum_i128 = sum;
        r.zero_count = zc;
        r.nonnull = nn;
        r.key_kind = kk.as_u8();
        r.flags = fl;
        r
    };
    let g = vec![
        mk(-5, 9, 40, 2, 100, KeyKind::Exact, STATSF_COMPUTED | STATSF_ALL_ASCII),
        mk(3, 77, -12, 0, 99, KeyKind::Exact, STATSF_COMPUTED),
    ];
    let part = mk(-5, 77, 28, 2, 199, KeyKind::Exact, STATSF_COMPUTED);
    let body = flatstats_encode(&g, &part);
    assert_eq!(body.len(), flatstats_section_len(2));
    // 8-aligned base (Vec<u8> is not guaranteed): copy into a u64 buffer.
    let mut words = vec![0u64; body.len().div_ceil(8)];
    let aligned = unsafe {
        core::slice::from_raw_parts_mut(words.as_mut_ptr() as *mut u8, body.len())
    };
    aligned.copy_from_slice(&body);
    let f = FlatStatsRef::new(aligned).expect("parses");
    assert_eq!(f.n, 3);
    // Flag MEET: ALL_ASCII not on every record => cleared; COMPUTED kept.
    assert_eq!(f.flags & FLATSTATSF_COMPUTED, FLATSTATSF_COMPUTED);
    assert_eq!(f.flags & FLATSTATSF_ALL_ASCII, 0);
    assert_eq!(f.key_kind, KeyKind::Exact.as_u8());
    let mins = f.mins().expect("aligned");
    let maxs = f.maxs().expect("aligned");
    let zcs = f.zero_counts().expect("aligned");
    let nns = f.nonnulls().expect("aligned");
    assert_eq!(mins, &[-5, 3, -5]);
    assert_eq!(maxs, &[9, 77, 77]);
    assert_eq!(zcs, &[2, 0, 2]);
    assert_eq!(nns, &[100, 99, 199]);
    assert_eq!(f.sum_at(0), 40);
    assert_eq!(f.sum_at(1), -12);
    assert_eq!(f.sum_at(2), 28);
    // key_kind MEET: mixed kinds degrade to Absent.
    let mut g2 = g.clone();
    g2[1].key_kind = KeyKind::Coarse.as_u8();
    let body2 = flatstats_encode(&g2, &part);
    let f2 = FlatStatsRef::new(&body2);
    // (unaligned Vec base is fine for header parse)
    assert_eq!(f2.expect("parses").key_kind, KeyKind::Absent.as_u8());
}

#[test]
fn partdigest_roundtrip() {
    use crate::meta::{PartDigest, PARTDIGESTF_NDV_EXACT, PARTDIGESTF_ZERO_COMPUTED, PARTDIGEST_LEN};
    let d = PartDigest {
        flags: PARTDIGESTF_NDV_EXACT | PARTDIGESTF_ZERO_COMPUTED,
        ndv: 2_500_042,
        zero_count: 77,
    };
    let b = d.encode();
    assert_eq!(b.len(), PARTDIGEST_LEN);
    assert_eq!(PartDigest::decode(&b).expect("decodes"), d);
    assert_eq!(d.ndv_exact(), Some(2_500_042));
    let nd = PartDigest { flags: 0, ndv: 0, zero_count: 5 };
    assert_eq!(PartDigest::decode(&nd.encode()).expect("decodes").ndv_exact(), None);
}
