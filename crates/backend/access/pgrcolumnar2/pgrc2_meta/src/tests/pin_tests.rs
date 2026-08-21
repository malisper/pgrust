//! Layout + golden pins: the frozen record shapes this crate emits
//! (cross-checked against the format crate's own pins — a drift on either
//! side fails here too), the hash family's golden vectors (bloom/NDV bytes
//! are part bytes: silent drift is a determinism defect), the transform
//! golden values, and the fingerprint pins.

use crate::format::meta::{
    BloomHdr, NdvRegistersHdr, PsmaEntry, BLOOM_HDR_LEN, NDV_REGISTERS_HDR_LEN, PSMA_BLOCK_LEN,
    PSMA_ENTRIES, PSMA_ENTRY_LEN, STATS_RECORD_LEN,
};
use crate::hash::{meta_hash128, FoldChain};
use crate::key::{f64_order_key, memcmp_fixed_exact_key, memcmp_var_prefix_key, unsigned_flip_key};

#[test]
fn record_shape_pins() {
    // Wire lengths (spec §8; the format crate owns them — re-pinned here
    // because this crate writes the bytes).
    assert_eq!(STATS_RECORD_LEN, 80);
    assert_eq!(PSMA_ENTRIES, 256);
    assert_eq!(PSMA_ENTRY_LEN, 4);
    assert_eq!(PSMA_BLOCK_LEN, 1024);
    assert_eq!(BLOOM_HDR_LEN, 8);
    assert_eq!(NDV_REGISTERS_HDR_LEN, 8);
    // In-memory struct sizes of the wire-adjacent records.
    assert_eq!(core::mem::size_of::<PsmaEntry>(), 4);
    assert_eq!(core::mem::size_of::<BloomHdr>(), 8);
    assert_eq!(core::mem::size_of::<NdvRegistersHdr>(), 8);
    // Crate policy constants (changing one is a bank-recipe version event,
    // never a silent edit).
    assert_eq!(crate::bloom::BLOOM_K_DEFAULT, 4);
    assert_eq!(crate::bloom::BLOOM_BYTES_PER_GRANULE_DEFAULT, 2048);
    // OD-11 (RULED 2026-08-12): floor raised 8 -> 4096 (the v2-measured
    // threshold class); the raise IS the recorded bank-recipe version event.
    assert_eq!(crate::bloom::BLOOM_NDV_FLOOR, 4096);
    assert_eq!(crate::ndv::NDV_PRECISION, 10);
}

#[test]
fn hash_family_golden_vectors() {
    // Golden vectors for the two-lane family (computed independently from
    // the documented chain definition). Bloom + NDV part bytes ride these:
    // a drift here is a part-byte drift.
    assert_eq!(
        meta_hash128(b""),
        (0x22af_6b82_c16f_c5d8, 0xd32b_d8b3_fdcd_e7be)
    );
    assert_eq!(
        meta_hash128(b"pgrc2"),
        (0x8491_3bbc_e706_7257, 0xc828_bbc2_9cb2_3b94)
    );
    assert_eq!(
        meta_hash128(b"hello meta plane"),
        (0x25fb_f673_0c93_65f5, 0x4903_0cd2_b4aa_3441)
    );
    // Length is folded before content: a single zero byte is not the empty
    // string.
    assert_eq!(
        meta_hash128(&[0u8]),
        (0xb69c_9127_5573_441b, 0x86c7_899e_136f_0bae)
    );
}

#[test]
fn fold_chain_golden_vectors() {
    // FoldChain over ("pgrc2pfp" seed): the fingerprint substrate.
    let mut c = FoldChain::new(u64::from_le_bytes(*b"pgrc2pfp"));
    c.word(1);
    c.word(8);
    assert_eq!(c.finish(), 0x15b5_df2e_7d16_b100);
}

#[test]
fn fingerprint_golden_vectors() {
    use crate::format::class::{CollationClass, StorageClass};
    use crate::lower::{lower_const, ConstInput};
    use crate::pcache::predicate_fingerprint;
    use crate::profile::{MetaProfile, TypeSemantics};
    use crate::verdict::ZonePredicate;
    // IsNull at attno 1.
    assert_eq!(
        predicate_fingerprint(1, &ZonePredicate::IsNull),
        0x15b5_df2e_7d16_b100
    );
    // Eq(int8 5) at attno 1.
    let profile = MetaProfile::derive(
        StorageClass::ByvalWord {
            width: 8,
            signed: true,
        },
        CollationClass::C,
        TypeSemantics::SignedInt,
    )
    .unwrap();
    let c = lower_const(&profile, ConstInput::Word(5))
        .lowered()
        .unwrap();
    assert_eq!(
        predicate_fingerprint(1, &ZonePredicate::Eq(c)),
        0x05ad_f076_42a2_4b66
    );
}

#[test]
fn transform_golden_vectors() {
    // Float order keys (hand-derived from the documented twiddle).
    assert_eq!(f64_order_key(0.0).raw(), 0);
    assert_eq!(f64_order_key(-0.0).raw(), 0, "the ±0 class collapses");
    assert_eq!(f64_order_key(1.0).raw(), 0x3FF0_0000_0000_0000);
    assert_eq!(f64_order_key(-1.0).raw(), 0xC00F_FFFF_FFFF_FFFFu64 as i64);
    assert_eq!(f64_order_key(f64::INFINITY).raw(), 0x7FF0_0000_0000_0000);
    assert_eq!(
        f64_order_key(f64::NEG_INFINITY).raw(),
        0x800F_FFFF_FFFF_FFFFu64 as i64
    );
    assert_eq!(f64_order_key(f64::NAN).raw(), i64::MAX);
    assert_eq!(
        f64_order_key(-f64::NAN).raw(),
        i64::MAX,
        "every NaN collapses"
    );
    // Unsigned flip embed.
    assert_eq!(unsigned_flip_key(0).raw(), i64::MIN);
    assert_eq!(unsigned_flip_key(u64::MAX).raw(), i64::MAX);
    assert_eq!(unsigned_flip_key(1 << 63).raw(), 0);
    // Big-endian prefix embeds.
    assert_eq!(memcmp_var_prefix_key(b"").raw(), i64::MIN);
    assert_eq!(memcmp_var_prefix_key(&[0xFF; 8]).raw(), i64::MAX);
    assert_eq!(memcmp_fixed_exact_key(&[0x00]).raw(), i64::MIN);
    assert_eq!(
        memcmp_fixed_exact_key(&[0xFF]).raw(),
        0x7F00_0000_0000_0000,
        "one 0xFF byte in the high position"
    );
}
