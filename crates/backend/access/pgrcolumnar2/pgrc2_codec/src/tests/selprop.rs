//! `decode_sel ≡ decode_full ∘ select` under permuted selections (§5 M3-C
//! slice; the ABI's late-materialization law, spec §19.3): for every codec,
//! for many deterministic selections (empty, singleton, dense, sparse,
//! boundary rows), the survivor-only face must produce exactly the
//! composition of full decode and selection — canonical-bytes equality.

use super::*;
use crate::alpc::AlpEncoder;
use crate::boolbm::BoolBitmapEncoder;
use crate::bytefor::{granule_min_width, ByteForEncoder};
use crate::deltafor::DeltaForEncoder;
use crate::dictcodes::DictCodesEncoder;
use crate::ffor::FforEncoder;
use pgrc2_format::abi::{ByteArena, DecodeOut, Selection};
use pgrc2_format::class::{StorageClass, CLASS_VARLENA};
use pgrc2_format::enc::EncodingId;

fn selections(rows: u32, seed: &mut u64) -> Vec<Vec<u16>> {
    let mut sels: Vec<Vec<u16>> = vec![
        vec![],
        vec![0],
        vec![(rows - 1) as u16],
        (0..rows as u16).collect(),
        random_selection(rows, 3, seed),
        random_selection(rows, 17, seed),
        random_selection(rows, 101, seed),
    ];
    // Frame-boundary adversaries.
    let mut edges: Vec<u16> = Vec::new();
    for f in 0..rows.div_ceil(1024) {
        for d in [0i64, 1, 1023] {
            let r = f as i64 * 1024 + d;
            if r >= 0 && (r as u32) < rows {
                edges.push(r as u16);
            }
        }
    }
    edges.sort_unstable();
    edges.dedup();
    sels.push(edges);
    sels
}

fn assert_sel_property(b: &Built, g: u32, class: StorageClass, seed: &mut u64) {
    let rows = b.granules[g as usize].0;
    let (full, _fa) = dec_full(b, g);
    for sel_rows in selections(rows, seed) {
        let ctx = b.ctx(g);
        let (sel, _sa) = dec_sel_ctx(&ctx, &sel_rows);
        for (i, &r) in sel_rows.iter().enumerate() {
            let valid = match &b.granules[g as usize].1 {
                None => true,
                Some(bits) => bits[r as usize / 8] >> (r % 8) & 1 == 1,
            };
            if !valid {
                continue;
            }
            assert!(
                canon_eq(class, full[r as usize], sel[i]),
                "decode_sel diverged from decode_full at row {r}"
            );
        }
    }
}

#[test]
fn byte_for_sel_property() {
    let mut seed = 0x5E1_u64;
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    for shape in 0..4 {
        let rows = [8192u32, 1500, 4097, 100][shape as usize];
        let gd = super::roundtrip_int_corpus(shape, rows, &mut seed);
        let inputs = [gd.input(class)];
        let width = granule_min_width(&inputs[0], true);
        let mut enc = ByteForEncoder::new_bytefor(8, width, true);
        let built = build_stream(&mut enc, &inputs, 0, true);
        assert_sel_property(&built, 0, class, &mut seed);
    }
}

#[test]
fn delta_for_sel_property() {
    let mut seed = 0x5E2_u64;
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    let gd = super::roundtrip_int_corpus(4, 8192, &mut seed);
    let inputs = [gd.input(class)];
    let mut enc = DeltaForEncoder::default();
    let built = build_stream(&mut enc, &inputs, 0, true);
    assert_sel_property(&built, 0, class, &mut seed);
}

#[test]
fn ffor_sel_property() {
    let mut seed = 0x5E3_u64;
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    let gd = super::roundtrip_int_corpus(1, 8192, &mut seed);
    let inputs = [gd.input(class)];
    let mut enc = FforEncoder { signed: true, carry: None };
    let built = build_stream(&mut enc, &inputs, 0, true);
    assert_sel_property(&built, 0, class, &mut seed);
}

#[test]
fn alp_sel_property() {
    let mut seed = 0x5E4_u64;
    for shape in [0u32, 1, 3] {
        let gd = super::roundtrip_float_corpus(shape, 8192, &mut seed);
        let inputs = [gd.input(StorageClass::F64)];
        let mut enc = AlpEncoder {
            encoding: EncodingId::Alp,
            carry: None,
        };
        let built = build_stream(&mut enc, &inputs, 0, false);
        assert_sel_property(&built, 0, StorageClass::F64, &mut seed);
    }
}

#[test]
fn alp_f32_sel_property() {
    let mut seed = 0x5E7_u64;
    for shape in [0u32, 1, 2] {
        let gd = super::roundtrip_f32_corpus(shape, 8192, &mut seed);
        let inputs = [gd.input(StorageClass::F32)];
        let mut enc = crate::alpc::AlpF32Encoder::default();
        let built = build_stream(&mut enc, &inputs, 0, false);
        assert_sel_property(&built, 0, StorageClass::F32, &mut seed);
    }
}

#[test]
fn fsst_sel_property() {
    let mut seed = 0x5E8_u64;
    for shape in [0u32, 1] {
        let fx = super::roundtrip::FsstGranule::new(8192, shape, &mut seed);
        let gd = fx.data();
        let inputs = [gd.input(StorageClass::VarlenaVerbatim)];
        let sample: Vec<&[u8]> = fx.payloads.iter().map(|p| p.as_slice()).collect();
        let table = crate::fsst::FsstSymbolTable::build(&sample);
        let mut enc = crate::fsst::FsstEncoder::new(table);
        let built = build_stream(&mut enc, &inputs, 0, false);
        assert_sel_property(&built, 0, StorageClass::VarlenaVerbatim, &mut seed);
    }
}

#[test]
fn byte_for_sb3_width_sel_property() {
    // The new widths' survivor-only face composes exactly (width 5/6 —
    // the µs-timestamp shapes).
    let mut seed = 0x5E9_u64;
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    for range in [0xFF_FFFFu64, 0xFF_FFFF_FFFF, 0x3FF_FFFF_FFFF] {
        let rows = 8192u32;
        let mut datums: Vec<u64> = (0..rows)
            .map(|_| splitmix(&mut seed) % (range + 1))
            .collect();
        datums[0] = 0;
        datums[1] = range;
        let gd = GranuleData {
            rows,
            datums,
            validity: None,
        };
        let inputs = [gd.input(class)];
        let width = granule_min_width(&inputs[0], true);
        let mut enc = ByteForEncoder::new_bytefor(8, width, true);
        let built = build_stream(&mut enc, &inputs, 0, true);
        assert_sel_property(&built, 0, class, &mut seed);
    }
}

#[test]
fn bool_bitmap_sel_property() {
    let mut seed = 0x5E5_u64;
    let rows = 8192u32;
    let validity = validity_pattern(1, rows, &mut seed);
    let datums = (0..rows).map(|_| splitmix(&mut seed) % 2).collect();
    let gd = GranuleData {
        rows,
        datums,
        validity,
    };
    let inputs = [gd.input(StorageClass::Bool)];
    let mut enc = BoolBitmapEncoder;
    let built = build_stream(&mut enc, &inputs, 0, false);
    assert_sel_property(&built, 0, StorageClass::Bool, &mut seed);
}

#[test]
fn dict_codes_sel_property() {
    let mut seed = 0x5E6_u64;
    let ndv = 500u32;
    let (index, payload) = super::roundtrip::dict_fixture(ndv);
    let rows = 8192u32;
    let validity = validity_pattern(1, rows, &mut seed);
    let datums = (0..rows)
        .map(|_| splitmix(&mut seed) % ndv as u64)
        .collect();
    let gd = GranuleData {
        rows,
        datums,
        validity,
    };
    let inputs = [gd.input(StorageClass::VarlenaVerbatim)];
    let mut enc = DictCodesEncoder {
        class: CLASS_VARLENA,
        max_width: 32,
        byte_align: false,
    };
    let built = build_stream(&mut enc, &inputs, 0, false);
    let dict = pgrc2_format::dict::DictSections {
        index: &index[..],
        payload: &payload[..],
        entry_count: ndv,
        charlen_form: pgrc2_format::dict::DictCharLenForm::Absolute,
    };
    let vt = crate::registry().resolve(built.key()).expect("resolves");
    let mut ctx = built.ctx(0);
    ctx.dict = Some(dict);
    let (full, _fa) = dec_full_ctx(&ctx);
    for sel_rows in selections(rows, &mut seed) {
        let mut datums_sel = vec![0u64; sel_rows.len()];
        let mut arena_buf = vec![0u8; 1 << 20];
        let sel = Selection { rows: &sel_rows };
        let mut out = DecodeOut {
            datums: &mut datums_sel,
            arena: ByteArena::new(&mut arena_buf),
        };
        (vt.decode_sel)(&ctx, &sel, &mut out).expect("decode_sel");
        for (i, &r) in sel_rows.iter().enumerate() {
            let valid = match &built.granules[0].1 {
                None => true,
                Some(bits) => bits[r as usize / 8] >> (r % 8) & 1 == 1,
            };
            if !valid {
                continue;
            }
            assert!(
                canon_eq(
                    StorageClass::VarlenaVerbatim,
                    full[r as usize],
                    out.datums[i]
                ),
                "dict decode_sel diverged at row {r}"
            );
        }
    }
}
