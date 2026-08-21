//! The gate-3 differential tooth (M3 exit §2.3 fix, lane dekern): every
//! kernel this lane rewrote is driven OLD-SHAPE vs NEW-KERNEL on random +
//! adversarial payloads.
//!
//! Legs:
//! 1. **coverage pin** — the registry resolves each rewritten key to THIS
//!    crate's hot fn pointers, and the verbatim oracle's pointers are
//!    provably different bodies (the tooth cannot silently test the
//!    oracle against itself);
//! 2. **naive differentials** — per-value re-implementations of the frozen
//!    layouts in the PRE-FIX kernel shape (bounded `read_le` per value)
//!    vs the flat kernels: BYTE_FOR ×{1,2,4,8}, DELTA_FOR, FFOR, and the
//!    DICT_CODES codes face across the full width lattice 0..=32;
//! 3. **verbatim oracle** — hot kernels vs the retained reference witness
//!    bodies (`pgrc2_format::verbatim`) over every class incl. sign
//!    extension, NaN payloads, nulls, short tails, empty strings and
//!    overflow entries; canonical-byte equality plus the §7b ≥8-alignment
//!    pin on arena outputs; `decode_sel ≡ decode_full ∘ select` re-proven
//!    on the hot verbatim set (the reference-era selprop never covered it);
//! 4. **fires-proof** (born-RED discipline) — a seeded payload corruption
//!    must make the differential report divergence, witnessed here.
//!
//! ALP/ALP-RD's differential rides the vendored-oracle suite in
//! `roundtrip.rs` (`alp_roundtrips_bit_exact_with_oracle_differential`) +
//! `selprop.rs`, which drive the same rewritten walk unchanged.

use super::*;
use crate::bytefor::{granule_min_width, ByteForEncoder, FRAME_REF_LEN};
use crate::deltafor::DeltaForEncoder;
use crate::dictcodes::DictCodesEncoder;
use crate::ffor::FforEncoder;
use crate::section::{frame_base, payload_region, read_le};
use crate::verbhot::{VT_VERBATIM_HOT, VT_VERBATIM_VARLENA_HOT};
use alp::bitpack;
use pgrc2_format::abi::CodecVtable;
use pgrc2_format::class::{StorageClass, CLASS_BYVAL, CLASS_VARLENA};
use pgrc2_format::enc::EncodingId;
use pgrc2_format::geom::{FRAME_VALUES, OVERSIZE_THRESHOLD};
use pgrc2_format::part::StreamSectionHdr;
use pgrc2_format::verbatim::{verbatim_reference_vtables, VerbatimEncoder};

const FV: usize = FRAME_VALUES as usize;

// ---------------------------------------------------------------------------
// leg 1: coverage pin
// ---------------------------------------------------------------------------

#[test]
fn registry_serves_the_hot_verbatim_kernels_and_the_oracle_is_distinct() {
    let reg = crate::registry();
    for (i, w) in [1u8, 2, 4, 8].iter().enumerate() {
        let key = KernelKey {
            encoding: EncodingId::Verbatim.as_u16(),
            class: CLASS_BYVAL,
            width: *w,
        };
        let vt = reg.resolve(key).expect("resolves");
        assert!(
            std::ptr::eq(vt, &VT_VERBATIM_HOT[i] as *const CodecVtable),
            "byval w{w} must resolve to the hot vtable"
        );
    }
    let vkey = KernelKey {
        encoding: EncodingId::Verbatim.as_u16(),
        class: CLASS_VARLENA,
        width: 0,
    };
    let vt = reg.resolve(vkey).expect("resolves");
    assert!(
        std::ptr::eq(vt, &VT_VERBATIM_VARLENA_HOT as *const CodecVtable),
        "varlena must resolve to the hot vtable"
    );
    // The oracle is a different body — the differential below is real.
    for reference in verbatim_reference_vtables() {
        let hot = reg.resolve(reference.key).expect("hot key resolves");
        assert_ne!(
            hot.decode_full as usize, reference.decode_full as usize,
            "hot and reference decode_full must be distinct bodies ({:?})",
            reference.key
        );
    }
    // Hot per-width byval kernels are distinct monomorphizations (the S4
    // pin extended to the verbatim set).
    for i in 0..4 {
        for j in i + 1..4 {
            assert_ne!(
                VT_VERBATIM_HOT[i].decode_full as usize, VT_VERBATIM_HOT[j].decode_full as usize,
                "verbatim byval widths {i}/{j} share a decode_full body"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// naive per-value readers (the pre-fix kernel shape; frozen layouts)
// ---------------------------------------------------------------------------

fn open_naive<'a>(built: &'a Built, g: u32, encoding: EncodingId) -> (&'a [u8], u32, u32) {
    let section = built.section();
    let hdr = StreamSectionHdr::decode(section).expect("hdr");
    assert_eq!(hdr.encoding, encoding.as_u16(), "witnessed encoding");
    let payload = payload_region(&hdr, section).expect("payload");
    let fbase = frame_base(&hdr, section, g).expect("fbase");
    (payload, fbase, built.granules[g as usize].0)
}

fn naive_frame_start(built: &Built, fi: u32) -> usize {
    built.frame_table.as_ref().expect("frame table")[fi as usize] as usize
}

fn naive_byte_for(built: &Built, g: u32, w: usize) -> Vec<u64> {
    let (payload, fbase, rows) = open_naive(built, g, EncodingId::ByteFor);
    (0..rows as usize)
        .map(|i| {
            let fs = naive_frame_start(built, fbase + (i / FV) as u32);
            let refw = u64::from_le_bytes(payload[fs..fs + FRAME_REF_LEN].try_into().unwrap());
            let off = fs + FRAME_REF_LEN + (i % FV) * w;
            refw.wrapping_add(read_le(&payload[off..off + w]))
        })
        .collect()
}

fn naive_delta_for(built: &Built, g: u32) -> Vec<u64> {
    let (payload, fbase, rows) = open_naive(built, g, EncodingId::DeltaFor);
    let unzig = |z: u64| -> i64 { ((z >> 1) as i64) ^ -((z & 1) as i64) };
    let mut out = Vec::with_capacity(rows as usize);
    let mut f0 = 0usize;
    let mut f = 0u32;
    while f0 < rows as usize {
        let n = (rows as usize - f0).min(FV);
        let fs = naive_frame_start(built, fbase + f);
        let first = i64::from_le_bytes(payload[fs..fs + 8].try_into().unwrap());
        let w = payload[fs + 8] as usize;
        let base = fs + 16;
        let mut prev = first;
        for i in 0..n {
            let z = read_le(&payload[base + i * w..base + (i + 1) * w]);
            let v = prev.wrapping_add(unzig(z));
            out.push(v as u64);
            prev = v;
        }
        f0 += n;
        f += 1;
    }
    out
}

fn naive_ffor(built: &Built, g: u32) -> Vec<u64> {
    let (payload, fbase, rows) = open_naive(built, g, EncodingId::FforInterleave);
    let mut out = Vec::with_capacity(rows as usize);
    let mut f0 = 0usize;
    let mut f = 0u32;
    while f0 < rows as usize {
        let n = (rows as usize - f0).min(FV);
        let fs = naive_frame_start(built, fbase + f);
        let base = u64::from_le_bytes(payload[fs..fs + 8].try_into().unwrap());
        let bw = u32::from_le_bytes(payload[fs + 8..fs + 12].try_into().unwrap());
        let nwords = bitpack::packed_words(bw);
        let mut words = vec![0u64; 64 * bitpack::LANES];
        for (slot, ch) in words[..nwords]
            .iter_mut()
            .zip(payload[fs + 16..fs + 16 + nwords * 8].chunks_exact(8))
        {
            *slot = u64::from_le_bytes(ch.try_into().unwrap());
        }
        let mut vals = [0u64; 1024];
        bitpack::unpack(&words[..nwords], bw, &mut vals);
        out.extend(vals[..n].iter().map(|&d| base.wrapping_add(d)));
        f0 += n;
        f += 1;
    }
    out
}

fn naive_dict_codes(built: &Built, g: u32) -> Vec<u32> {
    let (payload, fbase, rows) = open_naive(built, g, EncodingId::DictCodes);
    let first = naive_frame_start(built, fbase);
    let bh = &payload[first - 8..first];
    let base = u32::from_le_bytes(bh[..4].try_into().unwrap());
    let width = bh[4] as usize;
    if width == 0 {
        return vec![base; rows as usize];
    }
    let need = (rows as usize * width).div_ceil(8);
    let bits = &payload[first..first + need];
    (0..rows as usize)
        .map(|i| {
            let bitoff = i * width;
            let byte = bitoff / 8;
            let end = (byte + 8).min(bits.len());
            let raw = read_le(&bits[byte..end]) >> (bitoff % 8);
            base + (raw & ((1u64 << width) - 1)) as u32
        })
        .collect()
}

// ---------------------------------------------------------------------------
// leg 2: naive differentials (all rows compared, placeholders included)
// ---------------------------------------------------------------------------

#[test]
fn byte_for_kernel_matches_naive_reader_every_width() {
    let mut seed = 0xD1FF_u64;
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    for (wi, &(w, modu)) in [
        (1u8, 200u64),
        (2, 60_000),
        (4, 3_000_000_000),
        (8, u64::MAX / 2),
    ]
    .iter()
    .enumerate()
    {
        for (shape, last) in [(0u32, 8192u32), (1, 1500), (2, 17), (3, 1)] {
            let gs: Vec<GranuleData> = [8192u32, 8192, last]
                .iter()
                .map(|&rows| {
                    let validity = validity_pattern(shape + wi as u32, rows, &mut seed);
                    let datums = (0..rows)
                        .map(|r| match shape {
                            // Adversarial: min/max delta alternation.
                            2 => 1_000_000 + if r % 2 == 0 { 0 } else { modu },
                            // All-equal (range 0 under a forced width).
                            3 => 1_000_000,
                            _ => 1_000_000 + splitmix(&mut seed) % (modu + 1),
                        })
                        .collect();
                    GranuleData {
                        rows,
                        datums,
                        validity,
                    }
                })
                .collect();
            let inputs: Vec<EncodeInput<'_>> = gs.iter().map(|g| g.input(class)).collect();
            let need = inputs
                .iter()
                .map(|g| granule_min_width(g, true))
                .max()
                .unwrap();
            assert!(need <= w, "corpus must fit the forced width");
            let mut enc = ByteForEncoder::new_bytefor(8, w, true);
            let built = build_stream(&mut enc, &inputs, 0, true);
            for g in 0..gs.len() as u32 {
                let (datums, _a) = dec_full(&built, g);
                assert_eq!(
                    datums,
                    naive_byte_for(&built, g, w as usize),
                    "byte_for w{w} shape {shape} granule {g}"
                );
            }
        }
    }
}

#[test]
fn delta_for_kernel_matches_naive_reader() {
    let mut seed = 0xD1FF_DF_u64;
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    for shape in 0..8u32 {
        let last = [8192u32, 700, 33, 1][shape as usize % 4];
        let gs: Vec<GranuleData> = [8192u32, last]
            .iter()
            .map(|&rows| {
                let mut gd = roundtrip_int_corpus(shape, rows, &mut seed);
                if shape == 6 {
                    // Adversarial zigzag stress: ±huge alternation.
                    for (r, d) in gd.datums.iter_mut().enumerate() {
                        *d = if r % 2 == 0 {
                            i64::MAX as u64 / 3
                        } else {
                            (-(i64::MAX / 3)) as u64
                        };
                    }
                }
                gd
            })
            .collect();
        let inputs: Vec<EncodeInput<'_>> = gs.iter().map(|g| g.input(class)).collect();
        let built = build_stream(&mut DeltaForEncoder::default(), &inputs, 0, true);
        for g in 0..gs.len() as u32 {
            let (datums, _a) = dec_full(&built, g);
            assert_eq!(
                datums,
                naive_delta_for(&built, g),
                "delta_for shape {shape} granule {g}"
            );
        }
    }
}

#[test]
fn ffor_kernel_matches_naive_reader() {
    let mut seed = 0xD1FF_FF_u64;
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    for shape in 0..8u32 {
        let last = [8192u32, 1025, 64, 1][shape as usize % 4];
        let gs: Vec<GranuleData> = [8192u32, last]
            .iter()
            .map(|&rows| roundtrip_int_corpus(shape, rows, &mut seed))
            .collect();
        let signed = shape % 2 == 1;
        let inputs: Vec<EncodeInput<'_>> = gs.iter().map(|g| g.input(class)).collect();
        let built = build_stream(&mut FforEncoder { signed, carry: None }, &inputs, 0, signed);
        for g in 0..gs.len() as u32 {
            let (datums, _a) = dec_full(&built, g);
            assert_eq!(
                datums,
                naive_ffor(&built, g),
                "ffor shape {shape} granule {g}"
            );
        }
    }
}

#[test]
fn dict_codes_kernel_matches_naive_reader_across_the_width_lattice() {
    let mut seed = 0xD1FF_DC_u64;
    for w in [0u32, 1, 2, 3, 5, 7, 8, 10, 13, 16, 17, 24, 29, 31, 32] {
        let span: u64 = if w == 0 { 0 } else { (1u64 << w) - 1 };
        let base: u64 = if w >= 32 { 0 } else { 7 };
        let gs: Vec<GranuleData> = [8192u32, 555]
            .iter()
            .map(|&rows| {
                let validity = validity_pattern(w, rows, &mut seed);
                let datums = (0..rows)
                    .map(|r| {
                        // Pin the exact elected width: hit both extremes.
                        match r {
                            0 => base,
                            1 if rows > 1 => base + span,
                            _ => base + splitmix(&mut seed) % (span + 1),
                        }
                    })
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
        // Codes-face only: no dict sections touched, so no verify pass
        // (verify drives decode_full, which needs the dict).
        let built = build_stream_opts(&mut enc, &inputs, 0, false, false);
        let vt = crate::registry().resolve(built.key()).expect("resolves");
        for g in 0..gs.len() as u32 {
            let ctx = built.ctx(g);
            let mut codes = vec![0u32; ctx.rows as usize];
            let n = (vt.decode_codes)(&ctx, &mut codes).expect("decode_codes");
            assert_eq!(n, ctx.rows);
            assert_eq!(
                codes,
                naive_dict_codes(&built, g),
                "dict codes w{w} granule {g}"
            );
        }
    }
}

/// Gather-face differential (dekern phase 2): the hot zero-copy gather vs a
/// naive per-value materialization through `dict_entry` — every slot
/// compared, placeholders included (the naive arm materializes null slots
/// exactly like the kernel does). Widths beyond 17 need dictionaries larger
/// than a test should build; the unpack math above them is already pinned
/// by the codes-face lattice, and entry resolution is width-independent.
#[test]
fn dict_gather_matches_naive_materialization_across_the_width_lattice() {
    let mut seed = 0xD1FF_6A_u64;
    for w in [0u32, 1, 2, 3, 5, 7, 8, 10, 13, 16, 17] {
        let span: u64 = if w == 0 { 0 } else { (1u64 << w) - 1 };
        let base: u64 = 7;
        let ndv = (base + span + 1) as u32;
        let (index, payload) = super::roundtrip::dict_fixture(ndv);
        let dict = pgrc2_format::dict::DictSections {
            index: &index[..],
            payload: &payload[..],
            entry_count: ndv,
            charlen_form: pgrc2_format::dict::DictCharLenForm::Absolute,
        };
        let gs: Vec<GranuleData> = [8192u32, 555]
            .iter()
            .map(|&rows| {
                let validity = validity_pattern(w, rows, &mut seed);
                let datums = (0..rows)
                    .map(|r| match r {
                        0 => base,
                        1 if rows > 1 => base + span,
                        _ => base + splitmix(&mut seed) % (span + 1),
                    })
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
        let built = build_stream_opts(&mut enc, &inputs, 0, false, false);
        let vt = crate::registry().resolve(built.key()).expect("resolves");
        let pay_lo = payload.as_ptr() as u64;
        let pay_hi = pay_lo + payload.len() as u64;
        for g in 0..gs.len() as u32 {
            let mut ctx = built.ctx(g);
            ctx.dict = Some(dict);
            let mut datums = vec![0u64; ctx.rows as usize];
            let mut arena_buf = vec![0u8; 1 << 20];
            let mut out = DecodeOut {
                datums: &mut datums,
                arena: ByteArena::new(&mut arena_buf),
            };
            let n = (vt.decode_full)(&ctx, &mut out).expect("decode_full");
            assert_eq!(n, ctx.rows);
            let naive = naive_dict_codes(&built, g);
            for (r, (&d, &code)) in datums.iter().zip(naive.iter()).enumerate() {
                let e = pgrc2_format::dict::dict_entry(&dict, code).expect("naive entry");
                assert!(
                    d >= pay_lo && d < pay_hi,
                    "w{w} granule {g} row {r}: datum escaped the payload region"
                );
                // SAFETY: containment proven above; image lives in `payload`.
                let got = unsafe {
                    core::slice::from_raw_parts(d as *const u8, e.image.len())
                };
                assert_eq!(got, e.image, "w{w} granule {g} row {r}: image bytes");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// leg 3: verbatim hot vs the reference oracle
// ---------------------------------------------------------------------------

fn reference_vt(key: KernelKey) -> &'static CodecVtable {
    verbatim_reference_vtables()
        .into_iter()
        .find(|vt| vt.key == key)
        .expect("reference vtable for key")
}

/// Decode granule `g` through an explicit vtable (hot or oracle).
fn dec_full_via(vt: &CodecVtable, built: &Built, g: u32) -> (Vec<u64>, Vec<u8>) {
    let ctx = built.ctx(g);
    let mut datums = vec![0u64; ctx.rows as usize];
    let mut arena_buf = vec![0u8; 1 << 21];
    let n = {
        let mut out = DecodeOut {
            datums: &mut datums,
            arena: ByteArena::new(&mut arena_buf),
        };
        (vt.decode_full)(&ctx, &mut out).expect("decode_full")
    };
    assert_eq!(n, ctx.rows);
    (datums, arena_buf)
}

fn dec_sel_via(vt: &CodecVtable, built: &Built, g: u32, rows: &[u16]) -> (Vec<u64>, Vec<u8>) {
    let ctx = built.ctx(g);
    let mut datums = vec![0u64; rows.len()];
    let mut arena_buf = vec![0u8; 1 << 21];
    let sel = Selection { rows };
    let n = {
        let mut out = DecodeOut {
            datums: &mut datums,
            arena: ByteArena::new(&mut arena_buf),
        };
        (vt.decode_sel)(&ctx, &sel, &mut out).expect("decode_sel")
    };
    assert_eq!(n as usize, rows.len());
    (datums, arena_buf)
}

fn assert_verbatim_diff(built: &Built, granules: usize, class: StorageClass, tag: &str) {
    let hot = crate::registry().resolve(built.key()).expect("hot");
    let oracle = reference_vt(built.key());
    let mut seed = 0x5E1_u64 ^ granules as u64;
    for g in 0..granules as u32 {
        let (h, _ha) = dec_full_via(hot, built, g);
        let (o, _oa) = dec_full_via(oracle, built, g);
        let word_class = !matches!(
            class,
            StorageClass::Fixed { .. } | StorageClass::VarlenaVerbatim
        );
        for r in 0..h.len() {
            if word_class {
                assert_eq!(h[r], o[r], "{tag} granule {g} row {r} (raw word)");
            } else {
                assert!(
                    canon_eq(class, h[r], o[r]),
                    "{tag} granule {g} row {r} (canonical bytes)"
                );
                // §7b: pointer-class outputs are ≥8-aligned.
                assert_eq!(h[r] % 8, 0, "{tag} granule {g} row {r} alignment");
            }
        }
        // decode_sel ≡ decode_full ∘ select on the hot set, and ≡ oracle.
        let rows = built.granules[g as usize].0;
        let sel = random_selection(rows, 3, &mut seed);
        if !sel.is_empty() {
            let (hs, _hsa) = dec_sel_via(hot, built, g, &sel);
            let (os, _osa) = dec_sel_via(oracle, built, g, &sel);
            for (i, &r16) in sel.iter().enumerate() {
                if word_class {
                    assert_eq!(hs[i], h[r16 as usize], "{tag} sel≡full g{g} i{i}");
                    assert_eq!(hs[i], os[i], "{tag} sel oracle g{g} i{i}");
                } else {
                    assert!(
                        canon_eq(class, hs[i], h[r16 as usize]),
                        "{tag} sel≡full g{g} i{i}"
                    );
                    assert!(canon_eq(class, hs[i], os[i]), "{tag} sel oracle g{g} i{i}");
                }
            }
        }
    }
}

#[test]
fn verbatim_word_classes_match_the_reference_oracle() {
    let mut seed = 0x0DDB_u64;
    // BYVAL widths × signedness (sign extension is the differential's
    // sharpest edge: high bit set in the top stored byte).
    for &w in &[1u8, 2, 4, 8] {
        for &signed in &[false, true] {
            let class = StorageClass::ByvalWord { width: w, signed };
            let lim: u64 = if w == 8 {
                u64::MAX
            } else {
                (1u64 << (w * 8)) - 1
            };
            let gs: Vec<GranuleData> = [8192u32, 300]
                .iter()
                .map(|&rows| {
                    let validity = validity_pattern(w as u32 + signed as u32, rows, &mut seed);
                    let datums = (0..rows)
                        .map(|r| {
                            let raw = match r % 4 {
                                0 => lim,                 // all bits set (sign edge)
                                1 => lim >> 1,            // top bit clear
                                2 => 1u64 << (w * 8 - 1), // exactly the sign bit
                                _ => splitmix(&mut seed) & lim,
                            };
                            // The encoder stores the low w bytes; the datum
                            // word convention (§6.7) wants the extended word
                            // for signed classes — feed extended inputs so
                            // encode verify holds.
                            crate::section::extend_word(raw, w, signed)
                        })
                        .collect();
                    GranuleData {
                        rows,
                        datums,
                        validity,
                    }
                })
                .collect();
            let inputs: Vec<EncodeInput<'_>> = gs.iter().map(|g| g.input(class)).collect();
            let built = build_stream(&mut VerbatimEncoder { class }, &inputs, 0, signed);
            assert_verbatim_diff(
                &built,
                gs.len(),
                class,
                &format!("byval w{w} signed={signed}"),
            );
        }
    }
    // F64 with NaN payloads / specials; F32 raw images; BOOL.
    for (class, tag) in [
        (StorageClass::F64, "f64"),
        (StorageClass::F32, "f32"),
        (StorageClass::Bool, "bool"),
    ] {
        let gs: Vec<GranuleData> = [8192u32, 111]
            .iter()
            .map(|&rows| {
                let mut gd = roundtrip_float_corpus(3, rows, &mut seed);
                match class {
                    StorageClass::F32 => {
                        for d in gd.datums.iter_mut() {
                            *d &= 0xFFFF_FFFF; // 4-byte raw images
                        }
                    }
                    StorageClass::Bool => {
                        for d in gd.datums.iter_mut() {
                            *d %= 2;
                        }
                    }
                    _ => {}
                }
                gd
            })
            .collect();
        let inputs: Vec<EncodeInput<'_>> = gs.iter().map(|g| g.input(class)).collect();
        let built = build_stream(&mut VerbatimEncoder { class }, &inputs, 0, false);
        assert_verbatim_diff(&built, gs.len(), class, tag);
    }
}

#[test]
fn verbatim_fixed_matches_the_reference_oracle() {
    let mut seed = 0xF1_u64;
    for len in [5u32, 16] {
        let mut images: Vec<Vec<u8>> = Vec::new();
        let gs: Vec<GranuleData> = [8192u32, 77]
            .iter()
            .map(|&rows| {
                let validity = validity_pattern(len, rows, &mut seed);
                let datums = (0..rows)
                    .map(|_| {
                        let img: Vec<u8> = (0..len).map(|_| splitmix(&mut seed) as u8).collect();
                        images.push(img);
                        images.last().unwrap().as_ptr() as u64
                    })
                    .collect();
                GranuleData {
                    rows,
                    datums,
                    validity,
                }
            })
            .collect();
        let class = StorageClass::Fixed { len };
        let inputs: Vec<EncodeInput<'_>> = gs.iter().map(|g| g.input(class)).collect();
        let built = build_stream(&mut VerbatimEncoder { class }, &inputs, len, false);
        assert_verbatim_diff(&built, gs.len(), class, &format!("fixed{len}"));
    }
}

fn varlena_image(payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + payload.len());
    v.extend_from_slice(&(((payload.len() + 4) as u32) << 2).to_le_bytes());
    v.extend_from_slice(payload);
    v
}

#[test]
fn verbatim_varlena_matches_the_reference_oracle_incl_overflow() {
    let mut seed = 0x7E17_u64;
    let mut images: Vec<Vec<u8>> = Vec::new();
    // Geometry law: only the LAST granule of an extent may be short.
    let gs: Vec<GranuleData> = [8192u32, 8192, 19]
        .iter()
        .map(|&rows| {
            let validity = validity_pattern(rows, rows, &mut seed);
            let datums = (0..rows)
                .map(|r| {
                    let payload: Vec<u8> = match r % 97 {
                        0 => Vec::new(), // empty string
                        1 if rows == 8192 => {
                            // Oversize -> the overflow stream (spec §6.8).
                            vec![0xAB; OVERSIZE_THRESHOLD as usize + 17]
                        }
                        _ => {
                            let n = (splitmix(&mut seed) % 40) as usize;
                            (0..n).map(|_| splitmix(&mut seed) as u8).collect()
                        }
                    };
                    images.push(varlena_image(&payload));
                    images.last().unwrap().as_ptr() as u64
                })
                .collect();
            GranuleData {
                rows,
                datums,
                validity,
            }
        })
        .collect();
    let class = StorageClass::VarlenaVerbatim;
    let inputs: Vec<EncodeInput<'_>> = gs.iter().map(|g| g.input(class)).collect();
    let built = build_stream(&mut VerbatimEncoder { class }, &inputs, 0, false);
    assert!(!built.ovf.is_empty(), "overflow arm must be exercised");
    assert_verbatim_diff(&built, gs.len(), class, "varlena");
}

// ---------------------------------------------------------------------------
// leg 4: fires-proof — the differential detects a real divergence
// ---------------------------------------------------------------------------

#[test]
fn differential_fires_on_seeded_corruption() {
    let mut seed = 0xF12E_u64;
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    let gd = GranuleData {
        rows: 8192,
        datums: (0..8192)
            .map(|_| 1_000_000 + splitmix(&mut seed) % 60_000)
            .collect(),
        validity: None,
    };
    let inputs = vec![gd.input(class)];
    let mut enc = ByteForEncoder::new_bytefor(8, 2, true);
    let mut built = build_stream(&mut enc, &inputs, 0, true);
    let baseline = naive_byte_for(&built, 0, 2);
    // Flip one delta byte in the middle of frame 3's body.
    let hdr = StreamSectionHdr::decode(built.section()).expect("hdr");
    let fs = built.frame_table.as_ref().unwrap()[3] as usize;
    let victim = built.build.section_range.start
        + pgrc2_format::part::STREAM_SECTION_HDR_LEN
        + fs
        + FRAME_REF_LEN
        + 41;
    let _ = hdr;
    built.buf[victim] ^= 0x5A;
    let (mutated, _a) = dec_full(&built, 0);
    assert_ne!(
        mutated, baseline,
        "the differential must detect a corrupted payload byte"
    );
}
