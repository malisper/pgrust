//! Constant-lowering fail-closed battery (the M3-E slice clause verbatim:
//! "constant-lowering fail-closed tests (non-inline varlena const header,
//! NaN-containing float range, rescale abstention)") plus the collation
//! gate's lowering face and the datum adapter's no-deref discipline.

use super::{numeric_payload, numeric_payload_nan};
use crate::format::class::{CollationClass, StorageClass};
use crate::format::wire::varlena_header_4b_u;
use crate::key::TypedKey;
use crate::lower::{const_input_from_datum, lower_const, AbstainReason, ConstInput, Lowering};
use crate::profile::{MetaProfile, TypeSemantics};

fn text_c_profile() -> MetaProfile {
    MetaProfile::derive(
        StorageClass::VarlenaVerbatim,
        CollationClass::C,
        TypeSemantics::TextCollated,
    )
    .expect("derives")
}

fn f64_profile() -> MetaProfile {
    MetaProfile::derive(StorageClass::F64, CollationClass::C, TypeSemantics::Float)
        .expect("derives")
}

fn packed_profile(scale: i32) -> MetaProfile {
    MetaProfile::derive(
        StorageClass::VarlenaVerbatim,
        CollationClass::C,
        TypeSemantics::PackedNumeric { scale },
    )
    .expect("derives")
}

fn varlena_image(payload: &[u8]) -> Vec<u8> {
    let mut img = varlena_header_4b_u(payload.len() as u32)
        .to_le_bytes()
        .to_vec();
    img.extend_from_slice(payload);
    img
}

// ---------------------------------------------------------------------------
// fail closed: non-inline varlena const headers
// ---------------------------------------------------------------------------

#[test]
fn non_inline_varlena_headers_abstain() {
    let profile = text_c_profile();
    // 1B short header (low bit set on LE): never dereferenced as 4B-U.
    let short = vec![0x07u8, b'a', b'b', b'c'];
    match lower_const(&profile, ConstInput::VarlenaImage(&short)) {
        Lowering::Abstain(AbstainReason::NonInlineVarlenaHeader) => {}
        other => panic!("short header must abstain, got {other:?}"),
    }
    // 4B-C compressed header (bits 10).
    let mut compressed = varlena_image(b"abcdefgh");
    compressed[0] |= 0b10;
    match lower_const(&profile, ConstInput::VarlenaImage(&compressed)) {
        Lowering::Abstain(AbstainReason::NonInlineVarlenaHeader) => {}
        other => panic!("compressed header must abstain, got {other:?}"),
    }
    // External TOAST pointer marker (0x01).
    let external = vec![0x01u8, 18, 0, 0, 0, 0];
    match lower_const(&profile, ConstInput::VarlenaImage(&external)) {
        Lowering::Abstain(AbstainReason::NonInlineVarlenaHeader) => {}
        other => panic!("external header must abstain, got {other:?}"),
    }
    // Truncated image (header claims more than present).
    let truncated = varlena_header_4b_u(100).to_le_bytes().to_vec();
    match lower_const(&profile, ConstInput::VarlenaImage(&truncated)) {
        Lowering::Abstain(AbstainReason::NonInlineVarlenaHeader) => {}
        other => panic!("truncated image must abstain, got {other:?}"),
    }
    // The plain 4B-U shape lowers.
    let ok = varlena_image(b"abc");
    match lower_const(&profile, ConstInput::VarlenaImage(&ok)) {
        Lowering::Lowered(c) => {
            assert!(
                matches!(c.key, Some(TypedKey::Coarse(_))),
                "C text lowers a coarse key"
            );
            assert_eq!(c.eq_bytes(), Some(&b"abc"[..]));
        }
        other => panic!("4B-U must lower, got {other:?}"),
    }
}

#[test]
fn datum_adapter_refuses_non_inline_without_deref() {
    // A 1-byte buffer whose only byte marks a 1B varlena header: the
    // adapter must refuse WITHOUT reading past it (miri-honest shape:
    // nothing else is allocated).
    let one = [0x05u8];
    // SAFETY: the buffer's first byte is readable — exactly the contract.
    let r = unsafe { const_input_from_datum(StorageClass::VarlenaVerbatim, one.as_ptr() as u64) };
    match r {
        Err(AbstainReason::NonInlineVarlenaHeader) => {}
        other => panic!("adapter must refuse 1B headers, got {other:?}"),
    }
    // A well-formed image adapts and lowers.
    let img = varlena_image(b"hello");
    // SAFETY: img is a live, well-formed 4B-U image.
    let input =
        unsafe { const_input_from_datum(StorageClass::VarlenaVerbatim, img.as_ptr() as u64) }
            .expect("adapts");
    match lower_const(&text_c_profile(), input) {
        Lowering::Lowered(c) => assert_eq!(c.eq_bytes(), Some(&b"hello"[..])),
        other => panic!("must lower, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// fail closed: contains_nan
// ---------------------------------------------------------------------------

#[test]
fn nan_float_constants_abstain() {
    let profile = f64_profile();
    for nan in [
        f64::NAN,
        -f64::NAN,
        f64::from_bits(0x7FF0_0000_0000_0001),
        f64::from_bits(0xFFF8_0000_0000_0042),
    ] {
        match lower_const(&profile, ConstInput::Word(nan.to_bits())) {
            Lowering::Abstain(AbstainReason::FloatNan) => {}
            other => panic!("NaN const must abstain (contains_nan gate), got {other:?}"),
        }
    }
    // A NaN-containing RANGE fails closed because its endpoint lowering
    // abstains — there is no lowered endpoint to build Between from.
    let lo = lower_const(&profile, ConstInput::Word(1.0f64.to_bits()));
    let hi = lower_const(&profile, ConstInput::Word(f64::NAN.to_bits()));
    assert!(lo.lowered().is_some());
    assert_eq!(hi.abstained(), Some(AbstainReason::FloatNan));
    // Non-NaN floats lower exact keys (and no eq bytes — floats are not
    // bloomable).
    match lower_const(&profile, ConstInput::Word((-0.0f64).to_bits())) {
        Lowering::Lowered(c) => {
            assert!(matches!(c.key, Some(TypedKey::Exact(_))));
            assert!(c.eq_bytes().is_none(), "floats are not eq-bloomable");
        }
        other => panic!("must lower, got {other:?}"),
    }
    // f32 gate too.
    let p32 =
        MetaProfile::derive(StorageClass::F32, CollationClass::C, TypeSemantics::Float).unwrap();
    match lower_const(&p32, ConstInput::Word(f32::NAN.to_bits() as u64)) {
        Lowering::Abstain(AbstainReason::FloatNan) => {}
        other => panic!("f32 NaN must abstain, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// fail closed: exact-rescale-or-abstain
// ---------------------------------------------------------------------------

#[test]
fn numeric_rescale_abstention() {
    let profile = packed_profile(2);
    // 1.23 at scale 2 → exact.
    let img = varlena_image(&numeric_payload(123, 2));
    match lower_const(&profile, ConstInput::VarlenaImage(&img)) {
        Lowering::Lowered(c) => {
            assert_eq!(c.key.map(|k| k.raw()), Some(123));
            assert!(c.eq_bytes().is_none(), "numeric is never eq-bloomable");
        }
        other => panic!("exact rescale must lower, got {other:?}"),
    }
    // 1.005 at scale 2 → would round: abstain.
    let img = varlena_image(&numeric_payload(1005, 3));
    match lower_const(&profile, ConstInput::VarlenaImage(&img)) {
        Lowering::Abstain(AbstainReason::NumericUnrepresentable) => {}
        other => panic!("inexact rescale must abstain, got {other:?}"),
    }
    // Overflow at scale → abstain.
    let img = varlena_image(&numeric_payload(i64::MAX as i128, 0));
    match lower_const(&profile, ConstInput::VarlenaImage(&img)) {
        Lowering::Abstain(AbstainReason::NumericUnrepresentable) => {}
        other => panic!("overflow must abstain, got {other:?}"),
    }
    // numeric NaN → typed special abstention.
    let img = varlena_image(&numeric_payload_nan());
    match lower_const(&profile, ConstInput::VarlenaImage(&img)) {
        Lowering::Abstain(AbstainReason::NumericSpecial) => {}
        other => panic!("numeric NaN must abstain, got {other:?}"),
    }
    // Malformed numeric payload → typed abstention, never a guess.
    let img = varlena_image(&[0x12u8]);
    match lower_const(&profile, ConstInput::VarlenaImage(&img)) {
        Lowering::Abstain(AbstainReason::NumericMalformed) => {}
        other => panic!("malformed numeric must abstain, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// the collation gate at lowering + misc typed abstentions
// ---------------------------------------------------------------------------

#[test]
fn collation_gate_shapes_lowering() {
    let img = varlena_image(b"abcdefgh");
    // C: coarse key + eq bytes.
    let c = lower_const(&text_c_profile(), ConstInput::VarlenaImage(&img))
        .lowered()
        .expect("lowers");
    assert!(matches!(c.key, Some(TypedKey::Coarse(_))));
    assert!(c.eq_bytes().is_some());
    // Other deterministic: NO key (no order metadata), eq bytes only.
    let p = MetaProfile::derive(
        StorageClass::VarlenaVerbatim,
        CollationClass::OtherDeterministic,
        TypeSemantics::TextCollated,
    )
    .unwrap();
    let c = lower_const(&p, ConstInput::VarlenaImage(&img))
        .lowered()
        .expect("lowers");
    assert!(c.key.is_none(), "non-C collation must not lower order keys");
    assert!(c.eq_bytes().is_some());
    // Nondeterministic: nothing lowerable at all.
    let p = MetaProfile::derive(
        StorageClass::VarlenaVerbatim,
        CollationClass::Nondeterministic,
        TypeSemantics::TextCollated,
    )
    .unwrap();
    match lower_const(&p, ConstInput::VarlenaImage(&img)) {
        Lowering::Abstain(AbstainReason::NoLowerableSemantics) => {}
        other => panic!("nondeterministic text must abstain, got {other:?}"),
    }
}

#[test]
fn shape_mismatches_abstain_typed() {
    // Word input against a varlena class.
    match lower_const(&text_c_profile(), ConstInput::Word(7)) {
        Lowering::Abstain(AbstainReason::ClassMismatch) => {}
        other => panic!("class mismatch must abstain, got {other:?}"),
    }
    // Wrong fixed length.
    let p = MetaProfile::derive(
        StorageClass::Fixed { len: 16 },
        CollationClass::C,
        TypeSemantics::MemcmpOrdered,
    )
    .unwrap();
    match lower_const(&p, ConstInput::Fixed(&[0u8; 6])) {
        Lowering::Abstain(AbstainReason::FixedLenMismatch) => {}
        other => panic!("fixed-length mismatch must abstain, got {other:?}"),
    }
    // Opaque semantics: nothing lowerable.
    let p = MetaProfile::derive(
        StorageClass::VarlenaVerbatim,
        CollationClass::C,
        TypeSemantics::Opaque,
    )
    .unwrap();
    let img = varlena_image(b"x");
    match lower_const(&p, ConstInput::VarlenaImage(&img)) {
        Lowering::Abstain(AbstainReason::NoLowerableSemantics) => {}
        other => panic!("opaque must abstain, got {other:?}"),
    }
}

#[test]
fn word_constants_lower_with_inline_eq_bytes() {
    // int8: exact key + 8-byte canonical image.
    let p = MetaProfile::derive(
        StorageClass::ByvalWord {
            width: 8,
            signed: true,
        },
        CollationClass::C,
        TypeSemantics::SignedInt,
    )
    .unwrap();
    let c = lower_const(&p, ConstInput::Word(-2i64 as u64))
        .lowered()
        .expect("lowers");
    assert_eq!(c.key.map(|k| k.raw()), Some(-2));
    assert_eq!(c.eq_bytes(), Some(&(-2i64).to_le_bytes()[..]));
    // int2 canonical bytes truncate to the class width.
    let p = MetaProfile::derive(
        StorageClass::ByvalWord {
            width: 2,
            signed: true,
        },
        CollationClass::C,
        TypeSemantics::SignedInt,
    )
    .unwrap();
    let c = lower_const(&p, ConstInput::Word(-2i64 as u64))
        .lowered()
        .expect("lowers");
    assert_eq!(c.eq_bytes(), Some(&(-2i16).to_le_bytes()[..]));
    // bool normalizes its canonical byte.
    let p =
        MetaProfile::derive(StorageClass::Bool, CollationClass::C, TypeSemantics::Bool).unwrap();
    let c = lower_const(&p, ConstInput::Word(0xFF))
        .lowered()
        .expect("lowers");
    assert_eq!(c.eq_bytes(), Some(&[1u8][..]));
    assert_eq!(c.key.map(|k| k.raw()), Some(1));
}

#[test]
fn tampered_from_parts_cannot_reach_a_coarse_eq_allpass() {
    // Even a hand-built LoweredConst with a coarse key cannot produce an
    // Eq AllPass: the evaluator's coarse arm has no such verdict (the
    // type-level law) — checked end-to-end in verdict_tests; here we pin
    // that from_parts preserves the kind tag.
    let k = crate::key::memcmp_var_prefix_key(b"abc");
    let c = crate::lower::LoweredConst::from_parts(Some(TypedKey::Coarse(k)), Some(b"abc"));
    assert!(matches!(c.key, Some(TypedKey::Coarse(_))));
}
