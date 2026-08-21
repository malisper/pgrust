//! Decode correctness THROUGH the dispatch layer (§5 M3-F +
//! non-negotiables): full decode vs expected canonical bytes for every
//! storage class over both reference encodings, nulls, the ≥32 KiB overflow
//! region, multi-granule + multi-extent streams, child (gcount) streams; and
//! the `decode_sel ≡ decode_full ∘ select` property under seeded permuted
//! selections — the kernel-level gate re-proven through the cursor because
//! the cursor builds every ctx.

use pgrc2_format::abi::{ByteArena, DecodeOut, Selection, ValidityVerdict};
use pgrc2_format::class::StorageClass;
use pgrc2_format::geom::OVERSIZE_THRESHOLD;
use pgrc2_format::meta::{MetaAnswer, MetaProbe};
use pgrc2_format::part::StreamRole;
use pgrc2_format::wire::varlena_4b_u_payload_len;

use crate::cursor::{reference_binding_leaked, StreamCursor};
use crate::testpart::{build_part, seq_i64_col, CellValue, ColSpec, PartSpec, RefEncoding};
use crate::ReadError;

use super::{open_built, ArenaBuf, XorShift};

/// Compare one decoded granule (already in `out`) against expectations
/// through the canonical-bytes lens; null rows skipped (validity is the only
/// truth); §7b pins on varlena outputs.
fn assert_granule(
    class: StorageClass,
    datums: &[u64],
    valid: &[bool],
    canon: &[Vec<u8>],
    base: usize,
    n: usize,
) {
    for i in 0..n {
        let r = base + i;
        if !valid[r] {
            continue;
        }
        let mut scratch = [0u8; 8];
        // SAFETY: pointer-class outputs point into the live decode arena.
        let got = unsafe {
            pgrc2_format::abi::datum_canonical_bytes(class, datums[i], &mut scratch)
        }
        .expect("canonical bytes");
        assert_eq!(got, &canon[r][..], "row {r} canonical mismatch");
        if matches!(class, StorageClass::VarlenaVerbatim) {
            let ptr = datums[i];
            assert_eq!(ptr % 8, 0, "varlena out not 8-aligned (spec §19.4)");
            // SAFETY: decode arena outlives this check.
            let header = u32::from_le_bytes(unsafe {
                core::slice::from_raw_parts(ptr as *const u8, 4)
                    .try_into()
                    .expect("len 4")
            });
            varlena_4b_u_payload_len(header, "§7b pin").expect("varlena-shaped output");
        }
    }
}

fn roundtrip_col(b: &crate::testpart::BuiltPart, attno: u32, role: StreamRole) {
    let part = open_built(b, 900 + attno as u64, 1);
    let binding = reference_binding_leaked();
    let mut cur =
        StreamCursor::open_role(part, binding, attno, 0, role).expect("cursor open");
    let exp = b
        .expected
        .iter()
        .find(|e| e.attno == attno && e.path_ord == 0)
        .expect("expected col");
    let mut rng = XorShift(0x5EED_0001 ^ attno as u64);
    let mut base = 0usize;
    for g in 0..cur.granule_count() {
        let n = cur.values_in_granule(g).expect("values") as usize;
        // decode_full
        let mut datums = vec![0u64; n.max(1)];
        let mut ab = ArenaBuf::new(4 << 20);
        let mut out = DecodeOut {
            datums: &mut datums,
            arena: ByteArena::new(ab.bytes_mut()),
        };
        let wrote = cur.decode_full(g, &mut out).expect("decode_full");
        assert_eq!(wrote as usize, n, "granule {g} row count");
        assert_granule(exp.class, out.datums, &exp.valid, &exp.canon, base, n);

        // decode_sel ≡ decode_full ∘ select under permuted selections.
        for sel_case in 0..4 {
            let rows: Vec<u16> = match sel_case {
                0 => Vec::new(),
                1 => (0..n as u16).collect(),
                2 => (0..n as u16).filter(|r| r % 3 == 0).collect(),
                _ => {
                    let mut picked: Vec<u16> = (0..n as u16)
                        .filter(|_| rng.below(4) == 0)
                        .collect();
                    picked.dedup();
                    picked
                }
            };
            let sel = Selection { rows: &rows };
            let mut sdat = vec![0u64; rows.len().max(1)];
            let mut sab = ArenaBuf::new(4 << 20);
            let mut sout = DecodeOut {
                datums: &mut sdat,
                arena: ByteArena::new(sab.bytes_mut()),
            };
            let m = cur.decode_sel(g, &sel, &mut sout).expect("decode_sel");
            assert_eq!(m as usize, rows.len());
            for (i, &r) in rows.iter().enumerate() {
                let gr = base + r as usize;
                if !exp.valid[gr] {
                    continue;
                }
                let mut s1 = [0u8; 8];
                let mut s2 = [0u8; 8];
                // SAFETY: both arenas live.
                let a = unsafe {
                    pgrc2_format::abi::datum_canonical_bytes(
                        exp.class,
                        out.datums[r as usize],
                        &mut s1,
                    )
                }
                .expect("full canon");
                let bcanon = unsafe {
                    pgrc2_format::abi::datum_canonical_bytes(exp.class, sout.datums[i], &mut s2)
                }
                .expect("sel canon");
                assert_eq!(a, bcanon, "decode_sel diverged at granule {g} row {r}");
            }
        }

        // validity face vs expected popcount (reader-grain two-witness).
        let words = n.div_ceil(64);
        let mut mask = vec![0u64; words.max(1)];
        let verdict = cur.validity(g, &mut mask).expect("validity face");
        let expected_nonnull = exp.valid[base..base + n].iter().filter(|&&v| v).count() as u32;
        match verdict {
            ValidityVerdict::AllValid => assert_eq!(expected_nonnull as usize, n),
            ValidityVerdict::Mixed { nonnull } => {
                assert_eq!(nonnull, expected_nonnull, "granule {g} nonnull witness");
                let pop: u32 = mask[..words].iter().map(|w| w.count_ones()).sum();
                assert_eq!(pop, expected_nonnull, "granule {g} mask popcount");
            }
        }

        // meta_probe passthrough.
        match cur.meta_probe(g, &MetaProbe::NonNullCount).expect("meta") {
            MetaAnswer::Count(c) => assert_eq!(c, expected_nonnull as u64),
            other => panic!("unexpected meta answer {other:?}"),
        }
        base += n;
    }
}

#[test]
fn byval_i64_multi_granule() {
    let b = build_part(&PartSpec::new(20_000, vec![seq_i64_col(1, 20_000)]));
    roundtrip_col(&b, 1, StreamRole::Values);
}

#[test]
fn byval_i16_signed_negative() {
    let rows = 9_000u64;
    let values = (0..rows)
        .map(|r| Some(CellValue::Word((-(r as i64) - 1) as u64)))
        .collect();
    let col = ColSpec::new(
        1,
        StorageClass::ByvalWord {
            width: 2,
            signed: true,
        },
        values,
    );
    let b = build_part(&PartSpec::new(rows, vec![col]));
    roundtrip_col(&b, 1, StreamRole::Values);
}

#[test]
fn floats_bit_exact_including_nan() {
    let rows = 5_000u64;
    let f64s = (0..rows)
        .map(|r| {
            let v = match r % 5 {
                0 => f64::NAN.to_bits(),
                1 => (-0.0f64).to_bits(),
                2 => f64::INFINITY.to_bits(),
                _ => (r as f64 * 1.5e-3).to_bits(),
            };
            Some(CellValue::Word(v))
        })
        .collect();
    let c64 = ColSpec::new(1, StorageClass::F64, f64s);
    let f32s = (0..rows)
        .map(|r| Some(CellValue::Word(f32::to_bits(r as f32 / 7.0) as u64)))
        .collect();
    let c32 = ColSpec::new(2, StorageClass::F32, f32s);
    let b = build_part(&PartSpec::new(rows, vec![c64, c32]));
    roundtrip_col(&b, 1, StreamRole::Values);
    roundtrip_col(&b, 2, StreamRole::Values);
}

#[test]
fn bool_and_fixed16_with_nulls() {
    let rows = 10_000u64;
    let bools = (0..rows)
        .map(|r| {
            if r % 11 == 0 {
                None
            } else {
                Some(CellValue::Word((r % 2 == 0) as u64))
            }
        })
        .collect();
    let cb = ColSpec::new(1, StorageClass::Bool, bools);
    let fixed = (0..rows)
        .map(|r| {
            if r % 7 == 3 {
                None
            } else {
                let mut img = vec![0u8; 16];
                img[..8].copy_from_slice(&r.to_le_bytes());
                img[8..].copy_from_slice(&(!r).to_le_bytes());
                Some(CellValue::Bytes(img))
            }
        })
        .collect();
    let cf = ColSpec::new(2, StorageClass::Fixed { len: 16 }, fixed);
    let b = build_part(&PartSpec::new(rows, vec![cb, cf]));
    roundtrip_col(&b, 1, StreamRole::Values);
    roundtrip_col(&b, 2, StreamRole::Values);
}

#[test]
fn varlena_nulls_overflow_multi_extent() {
    let rows = 20_000u64;
    let values = (0..rows)
        .map(|r| {
            if r % 13 == 5 {
                return None;
            }
            if r % 4_001 == 7 {
                // ≥ 32 KiB ⇒ overflow region (spec §6.8).
                return Some(CellValue::Bytes(vec![
                    (r % 251) as u8;
                    OVERSIZE_THRESHOLD as usize + 17
                ]));
            }
            Some(CellValue::Bytes(
                format!("row-{r}-{}", "y".repeat((r % 23) as usize)).into_bytes(),
            ))
        })
        .collect();
    let mut col = ColSpec::new(1, StorageClass::VarlenaVerbatim, values);
    col.extent_cuts = vec![2]; // two extents: granules [0,2) and [2,3)
    let b = build_part(&PartSpec::new(rows, vec![col]));
    roundtrip_col(&b, 1, StreamRole::Values);
}

#[test]
fn const_encoding_word_and_varlena_and_all_null() {
    let rows = 12_000u64;
    let mut cw = ColSpec::new(
        1,
        StorageClass::ByvalWord {
            width: 4,
            signed: false,
        },
        (0..rows).map(|_| Some(CellValue::Word(424_242))).collect(),
    );
    cw.encoding = RefEncoding::Const;
    let mut cv = ColSpec::new(
        2,
        StorageClass::VarlenaVerbatim,
        (0..rows)
            .map(|r| {
                if r % 2 == 0 {
                    Some(CellValue::Bytes(b"the-constant".to_vec()))
                } else {
                    None
                }
            })
            .collect(),
    );
    cv.encoding = RefEncoding::Const;
    let mut cn = ColSpec::new(
        3,
        StorageClass::ByvalWord {
            width: 8,
            signed: true,
        },
        (0..rows).map(|_| None).collect(),
    );
    cn.encoding = RefEncoding::Const;
    let b = build_part(&PartSpec::new(rows, vec![cw, cv, cn]));
    roundtrip_col(&b, 1, StreamRole::Values);
    roundtrip_col(&b, 2, StreamRole::Values);
    roundtrip_col(&b, 3, StreamRole::Values);
}

#[test]
fn child_stream_gcount_values() {
    // A ChildValues stream (sizes-of-sizes currency, spec §6.5): per-granule
    // value counts differ from rows; the cursor must serve values from the
    // gcount table and kernels address children without touching the parent.
    let rows = 20_000u64; // 3 granules
    let gcounts = vec![100u32, 0, 57];
    let total: u64 = gcounts.iter().map(|&v| v as u64).sum();
    let values = (0..total)
        .map(|v| {
            if v % 9 == 2 {
                None
            } else {
                Some(CellValue::Word(v.wrapping_mul(31)))
            }
        })
        .collect();
    let mut col = ColSpec::new(
        1,
        StorageClass::ByvalWord {
            width: 8,
            signed: false,
        },
        values,
    );
    col.child_gcounts = Some(gcounts.clone());
    let b = build_part(&PartSpec::new(rows, vec![col]));
    let part = open_built(&b, 950, 1);
    let binding = reference_binding_leaked();
    let mut cur = StreamCursor::open_role(part, binding, 1, 0, StreamRole::ChildValues)
        .expect("child cursor");
    for (g, &want) in gcounts.iter().enumerate() {
        assert_eq!(
            cur.values_in_granule(g as u32).expect("values"),
            want,
            "granule {g} child values"
        );
    }
    roundtrip_col(&b, 1, StreamRole::ChildValues);
}

#[test]
fn selection_validation_is_typed() {
    let b = build_part(&PartSpec::new(9_000, vec![seq_i64_col(1, 9_000)]));
    let part = open_built(&b, 951, 1);
    let binding = reference_binding_leaked();
    let mut cur = StreamCursor::open(part, binding, 1, 0).expect("cursor");
    let rows = cur.values_in_granule(0).expect("values");
    // Out-of-range ordinal.
    let bad = [rows as u16];
    let mut d = vec![0u64; 1];
    let mut ab = ArenaBuf::new(1 << 16);
    let mut out = DecodeOut {
        datums: &mut d,
        arena: ByteArena::new(ab.bytes_mut()),
    };
    let e = cur
        .decode_sel(0, &Selection { rows: &bad }, &mut out)
        .expect_err("oob selection");
    assert!(matches!(e, ReadError::Format(_)), "typed: {e}");
    // Non-ascending.
    let bad2 = [5u16, 5u16];
    let mut d2 = vec![0u64; 2];
    let mut ab2 = ArenaBuf::new(1 << 16);
    let mut out2 = DecodeOut {
        datums: &mut d2,
        arena: ByteArena::new(ab2.bytes_mut()),
    };
    let e2 = cur
        .decode_sel(0, &Selection { rows: &bad2 }, &mut out2)
        .expect_err("unordered selection");
    assert!(matches!(e2, ReadError::Format(_)), "typed: {e2}");
}
