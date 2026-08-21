//! The order-embedding transforms: float → sortable i64 and the shared
//! big-endian memcmp embeds (uuid/macaddr fixed images, text/bytea payload
//! prefixes). Extracted verbatim from `pgrc2_meta::key` (ruling 2026-08-08;
//! see the crate provenance header) — the SAME definitions serve build
//! (seal) and probe (constant lowering), and any future consumer (M4
//! abbreviated keys).
//!
//! Keys here are RAW `i64`s. The consumer owns the kind semantics
//! (spec §8.1's coarse-key law):
//!
//! - **Exact** (`k`): order-isomorphic AND equality-faithful w.r.t. the
//!   type's comparison semantics: `a < b ⇔ k(a) < k(b)` and
//!   `a = b ⇔ k(a) = k(b)` (`=` is the TYPE's equality: float ±0.0
//!   collapses, every NaN collapses — exactly PG's `float8_cmp` classes).
//! - **Coarse** (`c`): monotone-with-ties: `a ≤ b ⇒ c(a) ≤ c(b)`.
//!   Contrapositive: `c(a) < c(b) ⇒ a < b` — STRICT key inequalities prove
//!   strict value inequalities; ties prove nothing (prefix collisions).
//!
//! Each transform's doc states its class; coarse keys must never prove an
//! Eq AllPass (`pgrc2_meta::key` enforces this in its types).

// ---------------------------------------------------------------------------
// float order keys (charter §5 "the canonical order-key transform")
// ---------------------------------------------------------------------------

/// The canonical f64 order key. **Exact.** Exactness argument (w.r.t. PG
/// `float8_cmp_internal`: NaN = NaN > everything; -0.0 = +0.0):
///
/// - every NaN bit pattern (either sign) canonicalizes to `i64::MAX`,
///   strictly above `+inf`'s key — NaN's PG equality class collapses to one
///   key, ordered greatest;
/// - `-0.0` canonicalizes to `+0.0` — the ±0 equality class collapses;
/// - all other values use the standard sign-fold bit twiddle
///   (`b ^ ((b >> 63) as u64 >> 1)`), which is order-isomorphic to IEEE
///   order on non-NaN floats and injective.
///
/// Together: `a < b ⇔ k(a) < k(b)` and `a = b ⇔ k(a) = k(b)` under PG float
/// comparison — a true EXACT key. (Constant lowering still abstains on NaN
/// constants: the charter's contains_nan gate is a lowering law, kept
/// independently of this transform's soundness.)
#[inline]
pub fn f64_order_key(v: f64) -> i64 {
    if v.is_nan() {
        return i64::MAX;
    }
    // -0.0 → +0.0 (adding 0.0 canonicalizes the zero sign).
    let v = v + 0.0;
    let b = v.to_bits() as i64;
    b ^ (((b >> 63) as u64) >> 1) as i64
}

/// f32 → exact key via exact (injective, monotone) widening to f64.
#[inline]
pub fn f32_order_key(v: f32) -> i64 {
    f64_order_key(v as f64)
}

/// The f64 key of a datum word carrying IEEE bits in its low 8 bytes
/// (spec §6.7's F64 convention).
#[inline]
pub fn f64_key_from_datum(datum: u64) -> i64 {
    f64_order_key(f64::from_bits(datum))
}

/// The f32 key of a datum word carrying IEEE bits in its low 4 bytes.
#[inline]
pub fn f32_key_from_datum(datum: u64) -> i64 {
    f32_order_key(f32::from_bits(datum as u32))
}

// ---------------------------------------------------------------------------
// memcmp-order transforms (fixed images, prefixes)
// ---------------------------------------------------------------------------

/// The shared big-endian embed: first `min(len, 8)` bytes into the HIGH
/// bytes of a u64 (zero-padded on the right), then the sign flip into i64.
/// Monotone-with-ties w.r.t. memcmp order for any byte strings; injective
/// (hence order-isomorphic) when all inputs have the same length ≤ 8.
#[inline]
pub fn be_prefix_embed(bytes: &[u8]) -> i64 {
    let mut buf = [0u8; 8];
    let n = bytes.len().min(8);
    buf[..n].copy_from_slice(&bytes[..n]);
    (u64::from_be_bytes(buf) ^ (1u64 << 63)) as i64
}

/// Fixed-length memcmp-ordered image, `len ≤ 8` (macaddr, macaddr8): the
/// whole image embeds — injective + order-isomorphic → **Exact**.
#[inline]
pub fn memcmp_fixed_exact_key(image: &[u8]) -> i64 {
    debug_assert!(image.len() <= 8);
    be_prefix_embed(image)
}

/// Fixed-length memcmp-ordered image, `len > 8` (uuid): 8-byte prefix →
/// **Coarse** (distinct values can tie).
#[inline]
pub fn memcmp_fixed_prefix_key(image: &[u8]) -> i64 {
    debug_assert!(image.len() > 8);
    be_prefix_embed(image)
}

/// Varlena payload prefix (bytea always; text only under collation-class C,
/// the consumer profile's gate): 8-byte zero-padded prefix → **Coarse**.
/// Zero-padding is monotone-with-ties for memcmp-with-length order: a
/// string and its extensions may tie, and ties prove nothing — exactly the
/// coarse contract.
#[inline]
pub fn memcmp_var_prefix_key(payload: &[u8]) -> i64 {
    be_prefix_embed(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Transform goldens carried verbatim from `pgrc2_meta`'s pin suite
    /// (raw-i64 form — the values are identical; meta's suite pins the same
    /// values through its typed wrappers).
    #[test]
    fn transform_golden_vectors() {
        // Float order keys (hand-derived from the documented twiddle).
        assert_eq!(f64_order_key(0.0), 0);
        assert_eq!(f64_order_key(-0.0), 0, "the ±0 class collapses");
        assert_eq!(f64_order_key(1.0), 0x3FF0_0000_0000_0000);
        assert_eq!(f64_order_key(-1.0), 0xC00F_FFFF_FFFF_FFFFu64 as i64);
        assert_eq!(f64_order_key(f64::INFINITY), 0x7FF0_0000_0000_0000);
        assert_eq!(
            f64_order_key(f64::NEG_INFINITY),
            0x800F_FFFF_FFFF_FFFFu64 as i64
        );
        assert_eq!(f64_order_key(f64::NAN), i64::MAX);
        assert_eq!(f64_order_key(-f64::NAN), i64::MAX, "every NaN collapses");
        // Big-endian prefix embeds.
        assert_eq!(memcmp_var_prefix_key(b""), i64::MIN);
        assert_eq!(memcmp_var_prefix_key(&[0xFF; 8]), i64::MAX);
        assert_eq!(memcmp_fixed_exact_key(&[0x00]), i64::MIN);
        assert_eq!(
            memcmp_fixed_exact_key(&[0xFF]),
            0x7F00_0000_0000_0000,
            "one 0xFF byte in the high position"
        );
    }
}
