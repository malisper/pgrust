//! Election machinery gates (§5 M3-C: "elections input-decidable with the
//! refusal-to-elect path typed"; the ≥10% gate + incompressible-guard
//! property tests; charter §1 analyze-then-elect discipline).

use super::*;
use pgrc2_format::class::StorageClass;
use pgrc2_format::enc::EncodingId;

use crate::election::{
    elect_bool, elect_float, elect_int, elect_numeric, elect_stream_wrapper, elect_text_dict,
    elect_text_fsst, wins_by_ten_percent, Demotion, DictArm, Election, FsstArm,
};

/// Decimal string with exactly `scale` fraction digits (numeric_in yields
/// dscale == scale). Shared with the numeric round-trip suites.
pub fn decimal_string(mant: i64, scale: i32) -> String {
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

#[test]
fn ten_percent_gate_is_exact() {
    // The gate is LAW: candidate*10 <= baseline*9, boundary inclusive.
    assert!(wins_by_ten_percent(90, 100));
    assert!(!wins_by_ten_percent(91, 100));
    assert!(wins_by_ten_percent(9, 10));
    assert!(!wins_by_ten_percent(10, 10)); // incompressible guard shape
    assert!(!wins_by_ten_percent(11, 10));
    assert!(wins_by_ten_percent(0, 0)); // vacuous
}

#[test]
fn int_election_shapes() {
    let mut seed = 0xE1_u64;
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    // Narrow range → BYTE_FOR width 1 wins.
    let narrow = roundtrip_int_corpus(0, 8192, &mut seed);
    let inputs = [narrow.input(class)];
    match elect_int(&inputs, 8, true, false, false) {
        Election::Elected {
            encoding: EncodingId::ByteFor,
            width: 1,
            ..
        } => {}
        other => panic!("narrow ints must elect BYTE_FOR w1: {other:?}"),
    }
    // Full-u64 noise → incompressible (w8 == baseline) → demote typed.
    let noise = roundtrip_int_corpus(2, 8192, &mut seed);
    let noise = GranuleData {
        validity: None,
        ..noise
    };
    let inputs = [noise.input(class)];
    match elect_int(&inputs, 8, false, false, false) {
        Election::Demoted {
            reason: Demotion::BelowWinGate,
            ..
        } => {}
        other => panic!("noise must demote BelowWinGate: {other:?}"),
    }
    // Constant → CONST.
    let cst = GranuleData {
        rows: 8192,
        datums: vec![77u64; 8192],
        validity: None,
    };
    let inputs = [cst.input(class)];
    match elect_int(&inputs, 8, true, false, false) {
        Election::Elected {
            encoding: EncodingId::Const,
            ..
        } => {}
        other => panic!("constant must elect CONST: {other:?}"),
    }
    // All-null → typed NoValues demotion (verbatim ships).
    let allnull = GranuleData {
        rows: 128,
        datums: vec![0u64; 128],
        validity: Some(vec![0u64; 2]),
    };
    let inputs = [allnull.input(class)];
    match elect_int(&inputs, 8, true, false, false) {
        Election::Demoted {
            reason: Demotion::NoValues,
            ..
        } => {}
        other => panic!("all-null must demote NoValues: {other:?}"),
    }
}

#[test]
fn int_election_is_input_decidable() {
    // Same input twice ⇒ identical election (a pure function; the
    // byte-identical-parts law rides on this).
    let mut seed = 0xE2_u64;
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    for shape in 0..6 {
        let gd = roundtrip_int_corpus(shape, 4096, &mut seed);
        let inputs = [gd.input(class)];
        let a = elect_int(&inputs, 8, true, true, true);
        let b = elect_int(&inputs, 8, true, true, true);
        assert_eq!(a, b, "shape {shape}: election not input-decidable");
    }
}

#[test]
fn ffor_tier_is_fused_gated() {
    // The S4 verdict in machinery form: without the fused posture the FFOR
    // arm is never even offered.
    let mut seed = 0xE3_u64;
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    let gd = roundtrip_int_corpus(0, 8192, &mut seed);
    let inputs = [gd.input(class)];
    for fused in [false, true] {
        if let Election::Elected { encoding, .. } = elect_int(&inputs, 8, true, fused, false) {
            if !fused {
                assert_ne!(
                    encoding,
                    EncodingId::FforInterleave,
                    "FFOR must never win the flat posture"
                );
            }
        }
    }
}

#[test]
fn float_election_shapes() {
    let mut seed = 0xE4_u64;
    // Decimal-like doubles → ALP family clears the gate.
    let dec = roundtrip_float_corpus(0, 8192, &mut seed);
    let dec = GranuleData {
        validity: None,
        ..dec
    };
    let inputs = [dec.input(StorageClass::F64)];
    match elect_float(&inputs, StorageClass::F64) {
        Election::Elected {
            encoding: EncodingId::Alp | EncodingId::AlpRd,
            candidate_bytes,
            baseline_bytes,
            ..
        } => assert!(wins_by_ten_percent(candidate_bytes, baseline_bytes)),
        other => panic!("decimal doubles must elect the ALP family: {other:?}"),
    }
    // Raw-bit noise → demote (incompressible).
    let noise = roundtrip_float_corpus(1, 8192, &mut seed);
    let noise = GranuleData {
        validity: None,
        ..noise
    };
    let inputs = [noise.input(StorageClass::F64)];
    match elect_float(&inputs, StorageClass::F64) {
        Election::Demoted {
            reason: Demotion::BelowWinGate,
            ..
        } => {}
        other => panic!("bit noise must demote: {other:?}"),
    }
    // F32 (SB-5): decimal-scaled float4 MUST elect ALP — the QA corpus
    // `float_family.f4_decimal2` cell (v3 demoted FloatClassUnsupported;
    // that demotion is retired).
    let f32dec = roundtrip_f32_corpus(0, 8192, &mut seed);
    let f32dec = GranuleData {
        validity: None,
        ..f32dec
    };
    let inputs = [f32dec.input(StorageClass::F32)];
    match elect_float(&inputs, StorageClass::F32) {
        Election::Elected {
            encoding: EncodingId::Alp,
            candidate_bytes,
            baseline_bytes,
            ..
        } => {
            assert!(wins_by_ten_percent(candidate_bytes, baseline_bytes));
            assert_eq!(baseline_bytes, 8192 * 4, "f32 baseline is 4 bytes/row");
        }
        other => panic!("decimal f32 must elect ALP: {other:?}"),
    }
    // F32 raw-bit noise → incompressible → demote (never a bit-inexact
    // ALP win; verbatim stays legal for specials-heavy cells).
    let f32noise = roundtrip_f32_corpus(1, 8192, &mut seed);
    let f32noise = GranuleData {
        validity: None,
        ..f32noise
    };
    let inputs = [f32noise.input(StorageClass::F32)];
    match elect_float(&inputs, StorageClass::F32) {
        Election::Demoted {
            reason: Demotion::BelowWinGate,
            ..
        } => {}
        other => panic!("f32 bit noise must demote: {other:?}"),
    }
    // Non-float classes still demote typed (caller contract).
    let ints = GranuleData {
        rows: 128,
        datums: (0..128u64).collect(),
        validity: None,
    };
    let inputs = [ints.input(StorageClass::Bool)];
    match elect_float(&inputs, StorageClass::Bool) {
        Election::Demoted {
            reason: Demotion::FloatClassUnsupported,
            ..
        } => {}
        other => panic!("non-float class must demote FloatClassUnsupported: {other:?}"),
    }
}

#[test]
fn sb3_width_cells_elect_byte_for() {
    // The three QA MUST cells of the SB-3 disposition, as synthetic
    // distributions shaped like the corpus columns; the expected widths
    // are independent arithmetic on the value ranges, not the code's.
    let mut seed = 0xE8_u64;
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    let us: u64 = 1_000_000;
    let cells: [(&str, Box<dyn Fn(&mut u64) -> u64>, u8); 3] = [
        (
            // int_family.i8_bits48_unsorted: 48-bit range → width 6.
            "i8_bits48_unsorted",
            Box::new(|s: &mut u64| splitmix(s) >> 16),
            6,
        ),
        (
            // ts_family.ts_us_unsorted_22bit: 22-bit seconds at µs grain
            // (range 2^22 × 10^6 ≈ 2^42) → width 6.
            "ts_us_unsorted_22bit",
            Box::new(move |s: &mut u64| {
                631_152_000 * us + splitmix(s) % ((1u64 << 22) * us)
            }),
            6,
        ),
        (
            // ts_family.time_us_random: µs-of-day (< 2^37) → width 5.
            "time_us_random",
            Box::new(move |s: &mut u64| splitmix(s) % (86_400 * us)),
            5,
        ),
    ];
    for (name, gen, want_w) in &cells {
        let gs: Vec<GranuleData> = (0..2)
            .map(|_| GranuleData {
                rows: 8192,
                datums: (0..8192).map(|_| gen(&mut seed)).collect(),
                validity: None,
            })
            .collect();
        let inputs: Vec<_> = gs.iter().map(|g| g.input(class)).collect();
        match elect_int(&inputs, 8, true, false, false) {
            Election::Elected {
                encoding: EncodingId::ByteFor,
                width,
                candidate_bytes,
                baseline_bytes,
                ..
            } => {
                assert_eq!(width, *want_w, "{name}: wrong elected width");
                assert!(
                    wins_by_ten_percent(candidate_bytes, baseline_bytes),
                    "{name}: must clear the gate"
                );
            }
            other => panic!("{name} must elect BYTE_FOR (v3 fell to verbatim): {other:?}"),
        }
    }
    // Boundary law: the width ladder switches exactly at the byte edges
    // (one frame; planted min/max — independent of the election path).
    use crate::bytefor::granule_min_width;
    for (range, want) in [
        (0xFF_FFFFu64, 3u8),
        (0x100_0000, 4),
        (0xFF_FFFF_FFFF, 5),
        (0x100_0000_0000, 6),
        (0xFF_FFFF_FFFF_FFFF, 7),
        (0x100_0000_0000_0000, 8),
    ] {
        let gd = GranuleData {
            rows: 2,
            datums: vec![0, range],
            validity: None,
        };
        assert_eq!(
            granule_min_width(&gd.input(class), true),
            want,
            "range {range:#x}"
        );
    }
}

#[test]
fn fsst_election_wins_on_url_and_log_losers() {
    // The SB-4 target class: url/log-shaped near-unique tokens (the QA
    // t_url / t_log cells, where dict breached its cap). The candidate is
    // priced from REAL compression; the win assertion is against
    // independent baseline arithmetic (payload bytes + varlena framing).
    let mut seed = 0xF5E1_u64;
    for shaped in [
        super::roundtrip::url_shaped_payload as fn(&mut u64) -> Vec<u8>,
        super::roundtrip::log_shaped_payload,
    ] {
        let rows = 8192usize;
        let payloads: Vec<Vec<u8>> = (0..rows).map(|_| shaped(&mut seed)).collect();
        let sample: Vec<&[u8]> = payloads.iter().map(|p| p.as_slice()).collect();
        let table = crate::fsst::FsstSymbolTable::build(&sample);
        let code_bytes: usize = payloads.iter().map(|p| table.compressed_len(p)).sum();
        let value_bytes: usize = payloads.iter().map(|p| p.len()).sum();
        // Verbatim varlena baseline (the writer's shape): section header +
        // per-row slot + per-frame slot-end/frame-table entries + 4B-U
        // headers + payload bytes.
        let frames = rows.div_ceil(1024);
        let baseline = 32 + rows * 4 + frames * 8 + value_bytes + rows * 4;
        let arm = FsstArm {
            rows: rows as u64,
            table_bytes: table.serialized_len(),
            code_bytes,
            framing_bytes: rows * 4 + frames * 8 + 32,
            value_bytes,
            baseline_bytes: baseline,
        };
        match elect_text_fsst(arm) {
            Election::Elected {
                encoding: EncodingId::Fsst,
                candidate_bytes,
                ..
            } => {
                // Real compression, not a gate artifact: the code bytes
                // must undercut the raw value bytes outright.
                assert!(
                    code_bytes * 2 < value_bytes,
                    "url/log tokens must compress well: {code_bytes} vs {value_bytes}"
                );
                assert!(wins_by_ten_percent(candidate_bytes, baseline));
            }
            other => panic!("url/log-shaped corpus must elect FSST: {other:?}"),
        }
    }
    // Code-grain incompressible guard: random bytes over the full
    // alphabet must demote BelowWinGate. A 255-single-byte-symbol table
    // prices code_bytes ≈ value_bytes (escapes make it slightly LARGER),
    // so the guard fires on the compression comparison itself — the
    // frame layout's smaller per-value overhead (no varlena headers) can
    // never be the winning margin.
    let rows = 4096usize;
    let payloads: Vec<Vec<u8>> = (0..rows)
        .map(|_| (0..24).map(|_| splitmix(&mut seed) as u8).collect())
        .collect();
    let sample: Vec<&[u8]> = payloads.iter().map(|p| p.as_slice()).collect();
    let table = crate::fsst::FsstSymbolTable::build(&sample);
    let code_bytes: usize = payloads.iter().map(|p| table.compressed_len(p)).sum();
    let value_bytes: usize = payloads.iter().map(|p| p.len()).sum();
    assert!(
        code_bytes >= value_bytes,
        "random bytes cannot genuinely compress: {code_bytes} vs {value_bytes}"
    );
    let frames = rows.div_ceil(1024);
    let baseline = 32 + rows * 4 + frames * 8 + value_bytes + rows * 4;
    let arm = FsstArm {
        rows: rows as u64,
        table_bytes: table.serialized_len(),
        code_bytes,
        framing_bytes: rows * 4 + frames * 8 + 32,
        value_bytes,
        baseline_bytes: baseline,
    };
    match elect_text_fsst(arm) {
        Election::Demoted {
            reason: Demotion::BelowWinGate,
            ..
        } => {}
        other => panic!("random bytes must demote BelowWinGate: {other:?}"),
    }
}

#[test]
fn fsst_election_is_input_decidable() {
    // Same corpus twice ⇒ same table bytes, same priced candidate (the
    // byte-identical-parts law on the SB-4 arm).
    let build = || {
        let mut seed = 0xF5E2_u64;
        let payloads: Vec<Vec<u8>> = (0..2048)
            .map(|_| super::roundtrip::url_shaped_payload(&mut seed))
            .collect();
        let sample: Vec<&[u8]> = payloads.iter().map(|p| p.as_slice()).collect();
        let table = crate::fsst::FsstSymbolTable::build(&sample);
        let mut image = Vec::new();
        table.serialize_into(&mut image);
        let compressed: usize = payloads.iter().map(|p| table.compressed_len(p)).sum();
        (image, compressed)
    };
    let (img_a, comp_a) = build();
    let (img_b, comp_b) = build();
    assert_eq!(img_a, img_b, "table build must be deterministic");
    assert_eq!(comp_a, comp_b, "pricing must be deterministic");
}

#[test]
fn bool_election_shapes() {
    let mut seed = 0xE5_u64;
    let gd = GranuleData {
        rows: 8192,
        datums: (0..8192).map(|_| splitmix(&mut seed) % 2).collect(),
        validity: None,
    };
    let inputs = [gd.input(StorageClass::Bool)];
    match elect_bool(&inputs) {
        Election::Elected {
            encoding: EncodingId::BoolBitmap,
            ..
        } => {}
        other => panic!("bool must elect the bitmap: {other:?}"),
    }
}

#[test]
fn numeric_election_refusals_are_typed() {
    let imgs = |strs: &[&str]| -> Vec<adt_numeric::NumericImage> {
        strs.iter()
            .map(|s| {
                adt_numeric::io::numeric_in(s, -1, None)
                    .expect("parse")
                    .expect("non-soft")
            })
            .collect()
    };
    let build = |images: &[adt_numeric::NumericImage]| -> GranuleData {
        GranuleData {
            rows: images.len() as u32,
            datums: images
                .iter()
                .map(|i| i.as_bytes().as_ptr() as u64)
                .collect(),
            validity: None,
        }
    };
    // Uniform dscale elects.
    let ok = imgs(&["1.50", "2.75", "0.00"]);
    let gd = build(&ok);
    let inputs = [gd.input(StorageClass::VarlenaVerbatim)];
    match elect_numeric(&inputs, usize::MAX / 2).expect("ok") {
        Election::Elected {
            encoding: EncodingId::PackedNumeric,
            aux32: 2,
            ..
        } => {}
        other => panic!("uniform dscale must elect at scale 2: {other:?}"),
    }
    // Mixed dscale refuses typed (the uniform-dscale rule: NEVER rounds).
    let mixed = imgs(&["1.50", "2.5"]);
    let gd = build(&mixed);
    let inputs = [gd.input(StorageClass::VarlenaVerbatim)];
    match elect_numeric(&inputs, usize::MAX / 2).expect("ok") {
        Election::Demoted {
            reason: Demotion::NumericMixedDscale,
            ..
        } => {}
        other => panic!("mixed dscale must demote typed: {other:?}"),
    }
    // NaN refuses typed.
    let nan = imgs(&["NaN", "1"]);
    let gd = build(&nan);
    let inputs = [gd.input(StorageClass::VarlenaVerbatim)];
    match elect_numeric(&inputs, usize::MAX / 2).expect("ok") {
        Election::Demoted {
            reason: Demotion::NumericSpecial,
            ..
        } => {}
        other => panic!("NaN must demote typed: {other:?}"),
    }
    // Overflow refuses typed.
    let big = imgs(&["1", "99999999999999999999"]);
    let gd = build(&big);
    let inputs = [gd.input(StorageClass::VarlenaVerbatim)];
    match elect_numeric(&inputs, usize::MAX / 2).expect("ok") {
        Election::Demoted {
            reason: Demotion::NumericOverflow,
            ..
        } => {}
        other => panic!("overflow must demote typed: {other:?}"),
    }
}

#[test]
fn dict_decision_and_ndv_cap() {
    let arm = |ndv: u64, dict_bytes: usize, codes: usize, baseline: usize| DictArm {
        rows: 8192,
        ndv,
        ndv_cap: 100_000,
        dict_bytes,
        code_stream_bytes: codes,
        baseline_bytes: baseline,
    };
    match elect_text_dict(arm(1000, 20_000, 10_000, 200_000)) {
        Election::Elected {
            encoding: EncodingId::DictCodes,
            ..
        } => {}
        other => panic!("winning dict must elect: {other:?}"),
    }
    match elect_text_dict(arm(1000, 150_000, 60_000, 200_000)) {
        Election::Demoted {
            reason: Demotion::BelowWinGate,
            ..
        } => {}
        other => panic!("losing dict must demote: {other:?}"),
    }
    let mut over = arm(1000, 1, 1, 200_000);
    over.ndv = 200_000;
    match elect_text_dict(over) {
        Election::Demoted {
            reason: Demotion::NdvAboveCap,
            ..
        } => {}
        other => panic!("above-cap NDV must demote typed: {other:?}"),
    }
}

#[test]
fn wrapper_election_engages_only_on_wins() {
    let mut seed = 0xE6_u64;
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    // Highly repetitive stream → the wrapper engages; both arms clear the
    // ≥20% wrapper law here and zstd's exact image is the smaller, so the
    // deterministic ranking elects it (ties would keep LZ4, the first arm).
    let rep = GranuleData {
        rows: 8192,
        datums: (0..8192).map(|r| (r % 4) as u64).collect(),
        validity: None,
    };
    let inputs = [rep.input(class)];
    let mut enc = crate::bytefor::ByteForEncoder::new_bytefor(8, 8, true);
    let built = build_stream(&mut enc, &inputs, 0, true);
    let w =
        elect_stream_wrapper(built.section(), &built.build.granule_payload_ends).expect("elect");
    assert_eq!(w, Some(pgrc2_format::enc::Wrapper::Zstd));
    // The ranking really is by exact size: the elected arm's image is no
    // larger than the other arm's on the same section.
    let z = crate::wrapper::wrapped_len(
        built.section(),
        &built.build.granule_payload_ends,
        pgrc2_format::enc::Wrapper::Zstd,
    )
    .expect("price zstd");
    let l = crate::wrapper::wrapped_len(
        built.section(),
        &built.build.granule_payload_ends,
        pgrc2_format::enc::Wrapper::Lz4,
    )
    .expect("price lz4");
    assert!(z < l, "the election chose Zstd, so its image must be smaller");
    // Noise stream → wrapper refuses (incompressible guard, both arms).
    let noise = GranuleData {
        rows: 8192,
        datums: (0..8192).map(|_| splitmix(&mut seed)).collect(),
        validity: None,
    };
    let inputs = [noise.input(class)];
    let mut enc = crate::bytefor::ByteForEncoder::new_bytefor(8, 8, true);
    let built = build_stream(&mut enc, &inputs, 0, true);
    let w =
        elect_stream_wrapper(built.section(), &built.build.granule_payload_ends).expect("elect");
    assert_eq!(w, None, "incompressible stream must not wrap");
}

#[test]
fn wrapper_gate_is_twenty_percent_exact() {
    // The two gates are DISTINCT laws (O-CMP-4(a)): integer-exact
    // boundaries on both, pinned so neither can drift into the other.
    use crate::election::{wins_by_ten_percent, wins_by_twenty_percent};
    assert!(wins_by_twenty_percent(80, 100), "exactly 20% smaller wins");
    assert!(!wins_by_twenty_percent(81, 100), "19% smaller loses");
    assert!(wins_by_twenty_percent(0, 0), "empty stream: vacuous win");
    // A 15%-smaller candidate passes the encoding gate and FAILS the
    // wrapper gate — the asymmetry is the whole point.
    assert!(wins_by_ten_percent(85, 100));
    assert!(!wins_by_twenty_percent(85, 100));
}

#[test]
fn verify_at_encode_fires_on_a_wrong_election() {
    // The election quadruple's fixed leg has teeth: force a BYTE_FOR width
    // the data cannot fit — the encoder itself must refuse (EncodeContract),
    // proving a mis-elected stream can never emit.
    let mut seed = 0xE7_u64;
    let class = StorageClass::ByvalWord {
        width: 8,
        signed: true,
    };
    let wide = roundtrip_int_corpus(2, 2048, &mut seed); // full-u64 noise
    let wide = GranuleData {
        validity: None,
        ..wide
    };
    let inputs = [wide.input(class)];
    let mut enc = crate::bytefor::ByteForEncoder::new_bytefor(8, 1, true);
    let mut buf = Vec::new();
    let mut ovf = Vec::new();
    let r = crate::election::encode_stream(
        &mut buf,
        &mut ovf,
        &mut enc,
        &inputs,
        false,
        crate::registry(),
        true,
        0,
        true,
    );
    assert!(
        matches!(r, Err(pgrc2_format::FormatError::EncodeContract { .. })),
        "wrong width must refuse at encode: {:?}",
        r.err()
    );
}

// ---------------------------------------------------------------------------
// SEAL-FUSION: the fused int analyzer + carried encoders
// ---------------------------------------------------------------------------

/// The PRE-FUSION `elect_int` body, reconstructed from the still-public
/// per-family helpers — the reference the fused single-pass analyzer must
/// match on every input and posture (outcome equality is the law; the
/// carried facts are additionally proven by byte-identical carried encodes
/// below).
fn reference_elect_int(
    granules: &[pgrc2_format::abi::EncodeInput<'_>],
    byval_width: u8,
    signed: bool,
    fused: bool,
    cold: bool,
) -> Election {
    let baseline: usize = granules
        .iter()
        .map(|g| g.rows as usize * byval_width as usize)
        .sum();
    let any = granules.iter().any(|g| (0..g.rows).any(|r| g.valid(r)));
    if !any {
        return Election::Demoted {
            reason: Demotion::NoValues,
            baseline_bytes: baseline,
        };
    }
    let mut seen: Option<u64> = None;
    let mut constant = true;
    for g in granules {
        for r in 0..g.rows {
            if !g.valid(r) {
                continue;
            }
            let d = g.datums[r as usize];
            match seen {
                None => seen = Some(d),
                Some(s) if s != d => constant = false,
                _ => {}
            }
        }
    }
    if constant {
        return Election::Elected {
            encoding: EncodingId::Const,
            width: byval_width,
            aux32: 0,
            candidate_bytes: 16,
            baseline_bytes: baseline,
        };
    }
    let mut bf_width: u8 = 1;
    for g in granules {
        bf_width = bf_width.max(crate::bytefor::granule_min_width(g, signed));
    }
    let bf_bytes: usize = granules
        .iter()
        .map(|g| crate::bytefor::payload_bytes(g.rows, bf_width))
        .sum();
    let ffor_bytes: Option<usize> = fused.then(|| {
        granules
            .iter()
            .map(|g| crate::ffor::granule_payload_bytes(g, signed))
            .sum()
    });
    let df_bytes: Option<usize> = cold.then(|| {
        granules
            .iter()
            .map(crate::deltafor::granule_payload_bytes)
            .sum()
    });
    let mut best = (EncodingId::ByteFor, bf_width, bf_bytes);
    if let Some(fb) = ffor_bytes {
        if fb < best.2 {
            best = (EncodingId::FforInterleave, 0, fb);
        }
    }
    if let Some(db) = df_bytes {
        if db < best.2 {
            best = (EncodingId::DeltaFor, 0, db);
        }
    }
    if wins_by_ten_percent(best.2, baseline) {
        Election::Elected {
            encoding: best.0,
            width: best.1,
            aux32: 0,
            candidate_bytes: best.2,
            baseline_bytes: baseline,
        }
    } else {
        Election::Demoted {
            reason: Demotion::BelowWinGate,
            baseline_bytes: baseline,
        }
    }
}

#[test]
fn fused_analyzer_matches_reference() {
    use crate::election::elect_int_carry;
    let mut seed = 0xF05E_u64;
    for shape in 0..12u32 {
        // Multi-granule streams incl. a ragged tail granule.
        let g0 = roundtrip_int_corpus(shape, 8192, &mut seed);
        let g1 = roundtrip_int_corpus(shape.wrapping_add(1), 8192, &mut seed);
        let g2 = roundtrip_int_corpus(shape.wrapping_add(2), 3000, &mut seed);
        for &signed in &[true, false] {
            let class = StorageClass::ByvalWord { width: 8, signed };
            let inputs = [g0.input(class), g1.input(class), g2.input(class)];
            for &(fused, cold) in &[(false, false), (true, false), (false, true), (true, true)] {
                let want = reference_elect_int(&inputs, 8, signed, fused, cold);
                let (got, carry) = elect_int_carry(&inputs, 8, signed, fused, cold);
                assert_eq!(got, want, "shape={shape} signed={signed} fused={fused} cold={cold}");
                // One fact per frame of the row-dense stream.
                assert_eq!(
                    carry.frames.len(),
                    8 + 8 + 3, // 8192/1024 ×2 + ceil(3000/1024)
                    "carry frame count"
                );
            }
        }
    }
    // All-null and constant short-circuits.
    let all_null = GranuleData {
        rows: 4096,
        datums: vec![0; 4096],
        validity: Some(vec![0u64; 64]),
    };
    let class = StorageClass::ByvalWord { width: 8, signed: true };
    let inputs = [all_null.input(class)];
    let (got, _) = crate::election::elect_int_carry(&inputs, 8, true, true, true);
    assert_eq!(got, reference_elect_int(&inputs, 8, true, true, true));
    let konst = GranuleData {
        rows: 4096,
        datums: vec![7; 4096],
        validity: None,
    };
    let inputs = [konst.input(class)];
    let (got, _) = crate::election::elect_int_carry(&inputs, 8, true, true, true);
    assert_eq!(got, reference_elect_int(&inputs, 8, true, true, true));
}

/// Carried encoders emit byte-identical sections to the recompute path —
/// the SEAL-FUSION law in miniature (the seal-level tooth is the dirsha
/// gate; this pins it per family at unit grain).
#[test]
fn carried_encoders_emit_identical_bytes() {
    use crate::bytefor::{ByteForEncoder, IntCarryCursor};
    use crate::deltafor::DeltaForEncoder;
    use crate::election::elect_int_carry;
    use crate::ffor::FforEncoder;
    use std::sync::Arc;
    let mut seed = 0xCAFE_u64;
    for shape in 0..12u32 {
        let g0 = roundtrip_int_corpus(shape, 8192, &mut seed);
        let g1 = roundtrip_int_corpus(shape.wrapping_add(3), 2500, &mut seed);
        for &signed in &[true, false] {
            let class = StorageClass::ByvalWord { width: 8, signed };
            let inputs = [g0.input(class), g1.input(class)];
            let (_, carry) = elect_int_carry(&inputs, 8, signed, true, true);
            let facts = Arc::new(carry.frames);
            // BYTE_FOR at the stream width the analyzer elected.
            let mut w: u8 = 1;
            for g in &inputs {
                w = w.max(crate::bytefor::granule_min_width(g, signed));
            }
            let plain = build_stream(
                &mut ByteForEncoder::new_bytefor(8, w, signed),
                &inputs,
                0,
                signed,
            );
            let mut carried_enc = ByteForEncoder::new_bytefor(8, w, signed);
            carried_enc.carry = Some(IntCarryCursor {
                facts: facts.clone(),
                next: 0,
            });
            let carried = build_stream(&mut carried_enc, &inputs, 0, signed);
            assert_eq!(plain.buf, carried.buf, "BYTE_FOR shape={shape} signed={signed}");
            // FFOR.
            let plain = build_stream(&mut FforEncoder { signed, carry: None }, &inputs, 0, signed);
            let carried = build_stream(
                &mut FforEncoder {
                    signed,
                    carry: Some(IntCarryCursor {
                        facts: facts.clone(),
                        next: 0,
                    }),
                },
                &inputs,
                0,
                signed,
            );
            assert_eq!(plain.buf, carried.buf, "FFOR shape={shape} signed={signed}");
            // DELTA_FOR (facts computed under cold=true above).
            let plain = build_stream(&mut DeltaForEncoder::default(), &inputs, 0, signed);
            let carried = build_stream(
                &mut DeltaForEncoder {
                    carry: Some(IntCarryCursor {
                        facts: facts.clone(),
                        next: 0,
                    }),
                },
                &inputs,
                0,
                signed,
            );
            assert_eq!(plain.buf, carried.buf, "DELTA_FOR shape={shape} signed={signed}");
        }
    }
}

/// ALP carried frames == re-encoded frames (f64 + f32), and the FSST
/// carried buffers == recompressed emission.
#[test]
fn carried_alp_and_fsst_emit_identical_bytes() {
    use crate::alpc::{AlpCarryCursor, AlpEncoder, AlpF32Encoder};
    use crate::fsst::{FsstCarry, FsstEncoder, FsstSymbolTable};
    use std::sync::Arc;
    let mut seed = 0xA1FA_u64;
    // f64
    for shape in 0..6u32 {
        let g0 = roundtrip_float_corpus(shape, 8192, &mut seed);
        let g1 = roundtrip_float_corpus(shape.wrapping_add(1), 1800, &mut seed);
        let inputs = [g0.input(StorageClass::F64), g1.input(StorageClass::F64)];
        let frames: Vec<Vec<u8>> = inputs
            .iter()
            .map(|g| {
                let mut e = crate::alpc::encode_granule_carry(g);
                assert_eq!(e.frames.len(), 1);
                std::mem::take(&mut e.frames[0])
            })
            .collect();
        let plain = build_stream(
            &mut AlpEncoder { encoding: EncodingId::Alp, carry: None },
            &inputs,
            0,
            false,
        );
        let carried = build_stream(
            &mut AlpEncoder {
                encoding: EncodingId::Alp,
                carry: Some(AlpCarryCursor {
                    frames: Arc::new(frames),
                    next: 0,
                }),
            },
            &inputs,
            0,
            false,
        );
        assert_eq!(plain.buf, carried.buf, "ALP f64 shape={shape}");
    }
    // f32
    for shape in 0..4u32 {
        let g0 = roundtrip_f32_corpus(shape, 8192, &mut seed);
        let inputs = [g0.input(StorageClass::F32)];
        let frames: Vec<Vec<u8>> = inputs
            .iter()
            .map(|g| {
                let mut e = crate::alpc::encode_granule_f32_carry(g);
                assert_eq!(e.frames.len(), 1);
                std::mem::take(&mut e.frames[0])
            })
            .collect();
        let plain = build_stream(&mut AlpF32Encoder::default(), &inputs, 0, false);
        let carried = build_stream(
            &mut AlpF32Encoder {
                carry: Some(AlpCarryCursor {
                    frames: Arc::new(frames),
                    next: 0,
                }),
            },
            &inputs,
            0,
            false,
        );
        assert_eq!(plain.buf, carried.buf, "ALP f32 shape={shape}");
    }
    // FSST: carried row-dense compression == recompressed emission.
    let fixture = super::roundtrip::FsstGranule::new(6000, 1, &mut seed);
    let gd = fixture.data();
    let inputs = [gd.input(StorageClass::VarlenaVerbatim)];
    let sample: Vec<&[u8]> = fixture.payloads.iter().map(|p| p.as_slice()).collect();
    let table = FsstSymbolTable::build(&sample[..sample.len().min(512)]);
    let mut bytes: Vec<u8> = Vec::new();
    let mut ends: Vec<u32> = Vec::with_capacity(inputs[0].rows as usize + 1);
    ends.push(0);
    for r in 0..inputs[0].rows {
        if inputs[0].valid(r) {
            // SAFETY: test-built EncodeInput obeys the pointer contract.
            let p = unsafe { crate::section::varlena_payload(inputs[0].datums[r as usize]) }
                .expect("valid varlena");
            table.compress_into(p, &mut bytes);
        }
        ends.push(bytes.len() as u32);
    }
    let plain = build_stream(&mut FsstEncoder::new(table.clone()), &inputs, 0, false);
    let carried = build_stream(
        &mut FsstEncoder::new_carried(
            table,
            FsstCarry {
                bytes: Arc::new(bytes),
                ends: Arc::new(ends),
            },
            0,
        ),
        &inputs,
        0,
        false,
    );
    assert_eq!(plain.buf, carried.buf, "FSST carried emission");
}
