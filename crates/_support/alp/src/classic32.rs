//! ALP classic for f32 (SB-5 / CMP-D): encode f32s as round-trip-verified
//! scaled integers d = fast_round(n * 10^e * 10^-f) in the i32 domain,
//! FFOR-packed per 1024-value vector, with non-conforming values patched
//! back in as (position, bits) pairs — the same laws as the f64 kernels in
//! `classic.rs` (MAX_EXPONENT 10 — 10^10 is the last power of ten exact in
//! f32; i32 encoded domain).
//!
//! **Arithmetic domain (the bit-frozen form of record):** both transforms
//! evaluate in f64 — encode `n * 10^e * 10^-f` with the f64 fast-round
//! magic (2^51 + 2^52, covering the whole i32 domain), decode
//! `(d * 10^f) * 10^-e` in f64 rounded ONCE to f32. Pure-f32 evaluation
//! was measured to leave NO (e,f) pair with a near-zero round-trip
//! failure rate on decimal-scaled data (best ≈ 5%, most ≈ 13–25%: the
//! per-multiply f32 roundings shift results by 0.1–0.4 ulp, straddling
//! rounding boundaries for a matching fraction of inputs), while the f64
//! evaluation sits within 2^-52 of the exact product, so the single f32
//! rounding reproduces `fl32(d * 10^(f-e))` — and `d = n * 10^(e-f)`
//! round-trips decimal-scaled f32 exactly. The round-trip verify (bit
//! equality) remains the sole exactness authority either way.
//!
//! There is no RD arm at this width: a granule no (e,f) captures simply
//! prices above its raw 4-byte images and the granule election stores it
//! [`crate::Scheme::Raw`] (`granule32.rs`), so every bit pattern — NaN
//! payloads, ±0.0, infinities, denormals — still round-trips exactly.

use crate::bitpack;
use crate::constants::{
    EXP_ARR, FACT_ARR, FRAC_ARR, MAGIC_NUMBER, SAMPLES_PER_VECTOR,
    SAMPLING_EARLY_EXIT_THRESHOLD, VECTOR_SIZE,
};

/// The f32 profile's exponent ceiling (reference: 10^10 is the last power
/// of ten exactly representable in f32 — 5^10 < 2^24).
pub const MAX_EXPONENT_F32: u8 = 10;

/// The i32 encoded-domain limits, in the f64 arithmetic domain (i32::MAX
/// is exact in f64).
pub(crate) const ENCODING_UPPER_LIMIT_I32: f64 = 2_147_483_647.0;
pub(crate) const ENCODING_LOWER_LIMIT_I32: f64 = -2_147_483_647.0;

pub(crate) const F32_EXCEPTION_SIZE_BITS: u64 = 32;
pub(crate) const F32_EXCEPTION_POSITION_SIZE_BITS: u64 = 16;

#[inline]
fn fast_round64(x: f64) -> f64 {
    // Two separate fp ops by language rule (never contracted/reassociated).
    // The f64 magic is valid below 2^51, so it covers the whole i32 domain
    // — the f32 scheme has NO fast-round domain gap.
    x + MAGIC_NUMBER - MAGIC_NUMBER
}

/// See `classic.rs::is_impossible_to_encode`: the one range test rejects
/// NaN and both infinities (they fail ordered comparison) and the bit
/// compare is exactly "-0.0" (the only f64 whose bits are the bare sign
/// bit; an f32 -0.0 input scales to an f64 -0.0). Branchless (`|`, not
/// `||`) — it sits inside the verify loop.
#[inline]
fn is_impossible_to_encode_i32(scaled: f64) -> bool {
    !(ENCODING_LOWER_LIMIT_I32..=ENCODING_UPPER_LIMIT_I32).contains(&scaled)
        | (scaled.to_bits() == 1u64 << 63)
}

#[inline]
pub(crate) fn encode_value32(n: f32, exponent: u8, factor: u8) -> i32 {
    let scaled = (n as f64) * EXP_ARR[exponent as usize] * FRAC_ARR[factor as usize];
    // fast_round is computed unconditionally (pure fp ops) and selected
    // against the sentinel: csel, not a branch (the classic.rs shape).
    let rounded = fast_round64(scaled) as i32;
    if is_impossible_to_encode_i32(scaled) { ENCODING_UPPER_LIMIT_I32 as i32 } else { rounded }
}

#[inline]
pub(crate) fn decode_value32(d: i32, exponent: u8, factor: u8) -> f32 {
    // Evaluation order (d * 10^f) * 10^-e in f64, rounded ONCE to f32 —
    // the bit-frozen decode arithmetic the verification contract is
    // defined against (module doc; the codec's hot reader replicates this
    // exactly).
    ((d as f64) * (FACT_ARR[factor as usize] as f64) * FRAC_ARR[exponent as usize]) as f32
}

#[inline]
pub(crate) fn count_bits32(max: i32, min: i32) -> u32 {
    32 - (max.wrapping_sub(min) as u32).leading_zeros()
}

/// Estimated storage bits for one (e,f) over a sample (reference cost
/// model at f32 widths: samples * range-bit-width + exceptions * 48).
pub(crate) fn estimated_size_on_samples32(samples: &[f32], exponent: u8, factor: u8) -> u64 {
    let mut non_exceptions = 0u32;
    let mut exceptions = 0u64;
    let mut max_enc = i32::MIN;
    let mut min_enc = i32::MAX;
    for &v in samples {
        let d = encode_value32(v, exponent, factor);
        let dec = decode_value32(d, exponent, factor);
        if dec.to_bits() == v.to_bits() {
            non_exceptions += 1;
            max_enc = max_enc.max(d);
            min_enc = min_enc.min(d);
        } else {
            exceptions += 1;
        }
    }
    if non_exceptions < 2 {
        return u64::MAX;
    }
    samples.len() as u64 * count_bits32(max_enc, min_enc) as u64
        + exceptions * (F32_EXCEPTION_SIZE_BITS + F32_EXCEPTION_POSITION_SIZE_BITS)
}

/// First-stage selection over sampled vectors: deterministic voting, the
/// f64 shape without the RD switch (no RD arm at this width).
pub(crate) fn find_top_k_combinations32(sampled_vectors: &[Vec<f32>]) -> Vec<(u8, u8)> {
    let mut votes: std::collections::BTreeMap<(u8, u8), u32> = std::collections::BTreeMap::new();
    for samples in sampled_vectors {
        let n = samples.len() as u64;
        let sentinel = n * (F32_EXCEPTION_SIZE_BITS + F32_EXCEPTION_POSITION_SIZE_BITS)
            + n * F32_EXCEPTION_SIZE_BITS;
        let mut best_size = sentinel;
        let (mut best_e, mut best_f) = (0u8, 0u8);
        for e in (0..=MAX_EXPONENT_F32).rev() {
            for f in (0..=e).rev() {
                let size = estimated_size_on_samples32(samples, e, f);
                if size == u64::MAX {
                    continue;
                }
                if size < best_size
                    || (size == best_size && e > best_e)
                    || (size == best_size && e == best_e && f > best_f)
                {
                    best_size = size;
                    best_e = e;
                    best_f = f;
                }
            }
        }
        *votes.entry((best_e, best_f)).or_insert(0) += 1;
    }
    let mut ranked: Vec<((u8, u8), u32)> = votes.into_iter().collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then(b.0 .0.cmp(&a.0 .0)).then(b.0 .1.cmp(&a.0 .1)));
    ranked.truncate(crate::constants::MAX_K_COMBINATIONS);
    ranked.into_iter().map(|(c, _)| c).collect()
}

/// Second-stage per-vector re-ranking (mirror of `classic.rs::choose_ef`).
pub(crate) fn choose_ef32(combinations: &[(u8, u8)], values: &[f32]) -> (u8, u8) {
    debug_assert!(!combinations.is_empty());
    if combinations.len() == 1 {
        return combinations[0];
    }
    let mut buf = [0.0f32; SAMPLES_PER_VECTOR];
    let inc = values.len().div_ceil(SAMPLES_PER_VECTOR).max(1);
    let mut n = 0usize;
    let mut i = 0usize;
    while i < values.len() {
        buf[n] = values[i];
        n += 1;
        i += inc;
    }
    let samples = &buf[..n];
    let mut best = combinations[0];
    let mut best_size = u64::MAX;
    let mut worse = 0usize;
    for &(e, f) in combinations {
        let size = estimated_size_on_samples32(samples, e, f);
        if size >= best_size {
            worse += 1;
            if worse == SAMPLING_EARLY_EXIT_THRESHOLD {
                break;
            }
            continue;
        }
        worse = 0;
        best_size = size;
        best = (e, f);
    }
    best
}

/// One encoded 1024-value f32 vector.
#[derive(Clone, Debug, PartialEq)]
pub struct AlpF32Vector {
    pub len: u16,
    pub exponent: u8,
    pub factor: u8,
    pub bit_width: u8,
    pub for_base: i32,
    /// FFOR deltas (u32 domain, widened) in the FastLanes layout:
    /// bit_width * 16 words.
    pub packed: Vec<u64>,
    pub exc_positions: Vec<u16>,
    /// Original f32 bit images, parallel to exc_positions.
    pub exc_values: Vec<u32>,
}

impl AlpF32Vector {
    /// Exact serialized-form accounting: len(2) + e/f/bw(3) + base(4) +
    /// exception count(2) + packed payload + 6 bytes per exception.
    pub fn size_bytes(&self) -> usize {
        2 + 3 + 4 + 2 + self.packed.len() * 8 + self.exc_positions.len() * 6
    }
}

pub(crate) fn encode_vector32(values: &[f32], exponent: u8, factor: u8) -> AlpF32Vector {
    let n = values.len();
    debug_assert!(0 < n && n <= VECTOR_SIZE);
    let mut encoded = [0i32; VECTOR_SIZE];
    let mut fail = [0u8; VECTOR_SIZE];
    // Cross-iteration-state-free verify loop (see classic.rs); verification
    // is BIT equality, so -0.0 and every NaN payload are exceptions.
    for i in 0..n {
        let d = encode_value32(values[i], exponent, factor);
        encoded[i] = d;
        let dec = decode_value32(d, exponent, factor);
        fail[i] = (dec.to_bits() != values[i].to_bits()) as u8;
    }
    let mut exc_idx = [0u16; VECTOR_SIZE];
    let mut exc_count = 0usize;
    for i in 0..n {
        exc_idx[exc_count] = i as u16;
        exc_count += fail[i] as usize;
    }
    // Exceptions (and short-tail padding) take a valid in-range placeholder
    // so outlier bit patterns don't poison the pack width.
    let mut placeholder = 0i32;
    let mut j = 0usize;
    for i in 0..n {
        if j < exc_count && exc_idx[j] as usize == i {
            j += 1;
            continue;
        }
        placeholder = encoded[i];
        break;
    }
    let mut exc_positions = Vec::with_capacity(exc_count);
    let mut exc_values = Vec::with_capacity(exc_count);
    for &pos in &exc_idx[..exc_count] {
        exc_positions.push(pos);
        exc_values.push(values[pos as usize].to_bits());
        encoded[pos as usize] = placeholder;
    }
    for slot in encoded[n..].iter_mut() {
        *slot = placeholder;
    }
    let mut min = i32::MAX;
    let mut max = i32::MIN;
    for &d in encoded.iter() {
        min = min.min(d);
        max = max.max(d);
    }
    let mut deltas = [0u64; VECTOR_SIZE];
    for (delta, &d) in deltas.iter_mut().zip(encoded.iter()) {
        *delta = d.wrapping_sub(min) as u32 as u64;
    }
    let bit_width = count_bits32(max, min);
    let mut packed = vec![0u64; bitpack::packed_words(bit_width)];
    bitpack::pack(&deltas, bit_width, &mut packed);
    AlpF32Vector {
        len: n as u16,
        exponent,
        factor,
        bit_width: bit_width as u8,
        for_base: min,
        packed,
        exc_positions,
        exc_values,
    }
}

/// Decode one packed lane into all VECTOR_SIZE slots of `buf`: exceptions
/// unpatched, tail slots decoding placeholder garbage the caller truncates
/// (the autovectorizable full-width shape — see classic.rs).
pub(crate) fn decode_packed_into32(
    packed: &[u64],
    bit_width: u32,
    exponent: u8,
    factor: u8,
    for_base: i32,
    buf: &mut [f32; VECTOR_SIZE],
) {
    let mut deltas = [0u64; VECTOR_SIZE];
    bitpack::unpack(packed, bit_width, &mut deltas);
    // f64-evaluated transform, one rounding to f32 (module doc — must
    // stay bit-identical to `decode_value32`).
    let fact = FACT_ARR[factor as usize] as f64;
    let frac = FRAC_ARR[exponent as usize];
    for i in 0..VECTOR_SIZE {
        let d = for_base.wrapping_add(deltas[i] as u32 as i32);
        buf[i] = ((d as f64) * fact * frac) as f32;
    }
}

pub(crate) fn decode_vector32(v: &AlpF32Vector, out: &mut Vec<f32>) {
    let mut buf = [0.0f32; VECTOR_SIZE];
    decode_packed_into32(&v.packed, v.bit_width as u32, v.exponent, v.factor, v.for_base, &mut buf);
    for (&pos, &bits) in v.exc_positions.iter().zip(v.exc_values.iter()) {
        buf[pos as usize] = f32::from_bits(bits);
    }
    out.extend_from_slice(&buf[..v.len as usize]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_value_known() {
        assert_eq!(encode_value32(1.25, 2, 0), 125);
        assert_eq!(decode_value32(125, 2, 0).to_bits(), 1.25f32.to_bits());
        assert_eq!(encode_value32(15.0, 1, 1), 15);
        assert_eq!(decode_value32(15, 1, 1).to_bits(), 15.0f32.to_bits());
    }

    #[test]
    fn fast_round_ties_to_even() {
        assert_eq!(fast_round64(2.5), 2.0);
        assert_eq!(fast_round64(3.5), 4.0);
        assert_eq!(fast_round64(-2.5), -2.0);
        assert_eq!(fast_round64(1.4999), 1.0);
    }

    #[test]
    fn impossible_values_take_sentinel() {
        for v in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY, -0.0f32, 1e30f32] {
            assert_eq!(encode_value32(v, 0, 0), ENCODING_UPPER_LIMIT_I32 as i32);
        }
    }

    #[test]
    fn all_exception_vector_roundtrips() {
        let values = vec![f32::NAN; 100];
        let v = encode_vector32(&values, 3, 1);
        assert_eq!(v.exc_positions.len(), 100);
        assert_eq!(v.bit_width, 0);
        let mut out = Vec::new();
        decode_vector32(&v, &mut out);
        assert_eq!(out.len(), 100);
        for x in out {
            assert_eq!(x.to_bits(), f32::NAN.to_bits());
        }
    }

    #[test]
    fn decimal_scaled_values_have_zero_exceptions() {
        // The module-doc claim with teeth: at (e=2, f=0) — and at the
        // width-equivalent pairs the vote prefers — decimal2-scaled f32
        // round-trips EXACTLY under the f64-evaluated transform (the
        // pure-f32 evaluation measured 13–25% failures on this corpus;
        // this pin is what makes the f4_decimal2 MUST election real).
        fn split(state: &mut u64) -> u64 {
            *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = *state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
        let mut state = 0x5EEDu64;
        for (e, f) in [(2u8, 0u8), (10, 8), (4, 2)] {
            for _ in 0..4000 {
                let k = (split(&mut state) % 100_000) as f32;
                let n = k / 100.0f32;
                let d = encode_value32(n, e, f);
                assert_eq!(
                    decode_value32(d, e, f).to_bits(),
                    n.to_bits(),
                    "k={k} failed at ({e},{f})"
                );
            }
        }
    }
}
