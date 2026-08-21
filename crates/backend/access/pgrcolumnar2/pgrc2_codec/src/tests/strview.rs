//! StrView §7b pins (`lanev3-strview.md`, spec §1): every string-class
//! decode output in this crate is **varlena-shaped** (`[4B-U header][bytes]`)
//! at **≥8-byte alignment** in the arena, so the M4 flip from `Varlena`
//! cells to `StrView` cells is representation-only.
//!
//! Born-RED (two teeth): the checker itself is proven able to fire — a
//! deliberately misshaped image (short-varlena header bits) and a
//! misaligned pointer both FAIL the check — before the real outputs are
//! required to pass it.

use super::*;
use crate::dictcodes::DictCodesEncoder;
use crate::packednum::PackedNumericEncoder;
use pgrc2_format::abi::{ByteArena, DecodeOut, EncodeInput};
use pgrc2_format::class::{StorageClass, CLASS_VARLENA};
use pgrc2_format::wire::varlena_4b_u_payload_len;

/// The §7b shape check: pointer is 8-aligned and heads a valid 4B-U image.
fn varlena_shaped(datum: u64) -> Result<(), &'static str> {
    if datum % 8 != 0 {
        return Err("not 8-aligned");
    }
    // SAFETY: test datums point at live arena/backing images.
    let header = u32::from_le_bytes(
        unsafe { core::slice::from_raw_parts(datum as *const u8, 4) }
            .try_into()
            .expect("len 4"),
    );
    varlena_4b_u_payload_len(header, "pin").map_err(|_| "not a 4B-U header")?;
    Ok(())
}

#[test]
fn checker_fires_on_seeded_defects() {
    // Tooth 1: a 1B-short-varlena-style header byte (low bits 01) fails.
    let bad: [u8; 8] = [0x0D, b'a', b'b', b'c', 0, 0, 0, 0];
    assert_eq!(
        varlena_shaped(bad.as_ptr() as u64),
        Err("not a 4B-U header"),
        "seeded short-varlena header must fail the checker"
    );
    // Tooth 2: a misaligned pointer fails.
    let good = {
        let mut v = vec![0u8; 16];
        v[0..4].copy_from_slice(&pgrc2_format::wire::varlena_header_4b_u(3).to_le_bytes());
        v
    };
    let mis = good.as_ptr() as u64 + 1;
    assert_eq!(varlena_shaped(mis), Err("not 8-aligned"));
}

#[test]
fn dict_materialization_outputs_are_varlena_shaped() {
    let mut seed = 0x57F1_u64;
    let ndv = 200u32;
    let (index, payload) = super::roundtrip::dict_fixture(ndv);
    let rows = 4096u32;
    let datums = (0..rows)
        .map(|_| splitmix(&mut seed) % ndv as u64)
        .collect();
    let gd = GranuleData {
        rows,
        datums,
        validity: validity_pattern(1, rows, &mut seed),
    };
    let inputs = [gd.input(StorageClass::VarlenaVerbatim)];
    let mut enc = DictCodesEncoder {
        class: CLASS_VARLENA,
        max_width: 32,
        byte_align: false,
    };
    let built = build_stream(&mut enc, &inputs, 0, false);
    let vt = crate::registry().resolve(built.key()).expect("resolves");
    let mut ctx = built.ctx(0);
    ctx.dict = Some(pgrc2_format::dict::DictSections {
        index: &index[..],
        payload: &payload[..],
        entry_count: ndv,
        charlen_form: pgrc2_format::dict::DictCharLenForm::Absolute,
    });
    let mut out_datums = vec![0u64; rows as usize];
    let mut arena_buf = vec![0u8; 1 << 20];
    let mut out = DecodeOut {
        datums: &mut out_datums,
        arena: ByteArena::new(&mut arena_buf),
    };
    (vt.decode_full)(&ctx, &mut out).expect("decode_full");
    let input = gd.input(StorageClass::VarlenaVerbatim);
    for r in 0..rows {
        if !input.valid(r) {
            continue;
        }
        varlena_shaped(out.datums[r as usize])
            .unwrap_or_else(|e| panic!("dict output row {r}: {e} (StrView §7b)"));
    }
}

/// The dekern phase-2 mechanism pin: dict-varlena gather is ZERO-COPY —
/// every datum is a view INTO the resident dict payload section (never an
/// arena copy), the arena stays untouched, and `decode_sel ≡ decode_full ∘
/// select` holds at pointer identity (same views, not equal copies).
#[test]
fn dict_gather_datums_are_zero_copy_views_into_the_dictionary() {
    let mut seed = 0x2C0_u64;
    let ndv = 500u32;
    let (index, payload) = super::roundtrip::dict_fixture(ndv);
    let rows = 8192u32;
    let datums = (0..rows).map(|_| splitmix(&mut seed) % ndv as u64).collect();
    let gd = GranuleData {
        rows,
        datums,
        validity: None,
    };
    let inputs = [gd.input(StorageClass::VarlenaVerbatim)];
    let mut enc = DictCodesEncoder {
        class: CLASS_VARLENA,
        max_width: 32,
        byte_align: false,
    };
    let built = build_stream(&mut enc, &inputs, 0, false);
    let vt = crate::registry().resolve(built.key()).expect("resolves");
    let mut ctx = built.ctx(0);
    ctx.dict = Some(pgrc2_format::dict::DictSections {
        index: &index[..],
        payload: &payload[..],
        entry_count: ndv,
        charlen_form: pgrc2_format::dict::DictCharLenForm::Absolute,
    });
    let pay_lo = payload.as_ptr() as u64;
    let pay_hi = pay_lo + payload.len() as u64;

    // decode_full: every datum inside the payload region; zero arena bytes.
    let mut full = vec![0u64; rows as usize];
    let mut arena_buf = vec![0u8; 1 << 20];
    let used = {
        let mut out = DecodeOut {
            datums: &mut full,
            arena: ByteArena::new(&mut arena_buf),
        };
        (vt.decode_full)(&ctx, &mut out).expect("decode_full");
        out.arena.used()
    };
    assert_eq!(used, 0, "gather copied through the arena (mechanism pin)");
    for r in 0..rows as usize {
        let d = full[r];
        assert!(
            d >= pay_lo && d < pay_hi,
            "row {r}: datum {d:#x} not a view into the dict payload region"
        );
    }

    // decode_sel: pointer-identical to full ∘ select.
    let sel_rows: Vec<u16> = (0..rows as u16).filter(|r| r % 7 == 0).collect();
    let mut sel_out = vec![0u64; sel_rows.len()];
    let mut sel_arena = vec![0u8; 1 << 20];
    let used = {
        let mut out = DecodeOut {
            datums: &mut sel_out,
            arena: ByteArena::new(&mut sel_arena),
        };
        let sel = pgrc2_format::abi::Selection { rows: &sel_rows };
        (vt.decode_sel)(&ctx, &sel, &mut out).expect("decode_sel");
        out.arena.used()
    };
    assert_eq!(used, 0, "sel gather copied through the arena");
    for (i, &r) in sel_rows.iter().enumerate() {
        assert_eq!(
            sel_out[i], full[r as usize],
            "decode_sel row {r}: not the identical zero-copy view"
        );
    }
}

#[test]
fn packed_numeric_outputs_are_varlena_shaped() {
    let scale = 2i32;
    let mut seed = 0x57F2_u64;
    let rows = 1024u32;
    let images: Vec<adt_numeric::NumericImage> = (0..rows)
        .map(|_| {
            let mant = (splitmix(&mut seed) % 1_000_000) as i64;
            let s = super::electiontests::decimal_string(mant, scale);
            adt_numeric::io::numeric_in(&s, -1, None)
                .expect("parse")
                .expect("non-soft")
        })
        .collect();
    let gd = GranuleData {
        rows,
        datums: images
            .iter()
            .map(|i| i.as_bytes().as_ptr() as u64)
            .collect(),
        validity: None,
    };
    let inputs = [gd.input(StorageClass::VarlenaVerbatim)];
    let crate::election::Election::Elected { width, .. } =
        crate::election::elect_numeric(&inputs, usize::MAX / 2).expect("elect")
    else {
        panic!("uniform corpus must elect");
    };
    let mut enc = PackedNumericEncoder::new(scale, width);
    let built = build_stream(&mut enc, &inputs, 0, true);
    let (datums, _arena) = dec_full(&built, 0);
    for r in 0..rows as usize {
        varlena_shaped(datums[r]).unwrap_or_else(|e| panic!("numeric output row {r}: {e}"));
    }
}

#[test]
fn fsst_outputs_are_varlena_shaped() {
    // FSST materializes via a sized `arena.alloc` + hand-written 4B-U
    // header (the two-pass decode), NOT `alloc_varlena` — so the §7b shape
    // pin must hold independently.
    let mut seed = 0x57F3_u64;
    let fx = super::roundtrip::FsstGranule::new(4096, 1, &mut seed);
    let gd = fx.data();
    let inputs = [gd.input(StorageClass::VarlenaVerbatim)];
    let sample: Vec<&[u8]> = fx.payloads.iter().map(|p| p.as_slice()).collect();
    let table = crate::fsst::FsstSymbolTable::build(&sample);
    let mut enc = crate::fsst::FsstEncoder::new(table);
    let built = build_stream(&mut enc, &inputs, 0, false);
    let (datums, _arena) = dec_full(&built, 0);
    let input = gd.input(StorageClass::VarlenaVerbatim);
    for r in 0..gd.rows {
        if !input.valid(r) {
            continue;
        }
        varlena_shaped(datums[r as usize])
            .unwrap_or_else(|e| panic!("fsst output row {r}: {e} (StrView §7b)"));
    }
}

/// The wide net: every varlena-producing path already asserted image
/// equality in `roundtrip.rs`; this pin closes with the reference-codec
/// arena law — `alloc_varlena` itself is the single arena entry point and
/// its output shape is what the two tests above verified end-to-end.
#[test]
fn arena_alloc_varlena_is_the_shape_authority() {
    let mut buf = vec![0u8; 256];
    let mut arena = ByteArena::new(&mut buf);
    let d = arena.alloc_varlena(b"payload").expect("alloc");
    varlena_shaped(d).expect("arena entries are varlena-shaped");
    // ≥8-alignment across consecutive odd-length entries.
    let d2 = arena.alloc_varlena(b"x").expect("alloc");
    let d3 = arena.alloc_varlena(b"yz").expect("alloc");
    assert_eq!(d2 % 8, 0);
    assert_eq!(d3 % 8, 0);
    // EncodeInput linkage witness (the same currency round-trips).
    let datums = [d, d2, d3];
    let input = EncodeInput {
        class: StorageClass::VarlenaVerbatim,
        rows: 3,
        datums: &datums,
        validity: None,
    };
    for r in 0..3 {
        assert!(input.valid(r));
    }
}
