//! ALP classic: encode f64s as round-trip-verified scaled integers
//! d = fast_round(n * 10^e * 10^-f), FFOR-packed per 1024-value vector,
//! with non-conforming values patched back in as (position, bits) pairs.

use crate::bitpack;
use crate::constants::*;

#[inline]
fn fast_round(x: f64) -> f64 {
    // Two separate fp ops by language rule (Rust never contracts or
    // reassociates); folding these would break the trick entirely.
    x + MAGIC_NUMBER - MAGIC_NUMBER
}

/// Scaled values that cannot live in the i64 domain (and -0.0, whose sign
/// dies in integer round-tripping) take a sentinel encoding; the round-trip
/// check then classifies the value as an exception.
///
/// Branchless by construction — this sits inside the verify loop, and a
/// short-circuit guard chain (the reference's shape) compiles to branches
/// that stop the autovectorizer cold. The one range test rejects NaN and
/// both infinities too (they fail ordered comparison), and the bit compare
/// is exactly "scaled == 0.0 && sign negative": -0.0 is the only f64 whose
/// bits are the bare sign bit.
#[inline]
fn is_impossible_to_encode(scaled: f64) -> bool {
    !(ENCODING_LOWER_LIMIT..=ENCODING_UPPER_LIMIT).contains(&scaled)
        | (scaled.to_bits() == 1u64 << 63)
}

#[inline]
pub(crate) fn encode_value(n: f64, exponent: u8, factor: u8) -> i64 {
    let scaled = n * EXP_ARR[exponent as usize] * FRAC_ARR[factor as usize];
    // fast_round is computed unconditionally (pure fp ops, no side
    // effects) and selected against the sentinel: csel, not a branch.
    let rounded = fast_round(scaled) as i64;
    if is_impossible_to_encode(scaled) { ENCODING_UPPER_LIMIT as i64 } else { rounded }
}

#[inline]
pub(crate) fn decode_value(d: i64, exponent: u8, factor: u8) -> f64 {
    // Multiplication order (d * 10^f) * 10^-e is the reference's evaluation
    // order; each product rounds once, and the verification contract is
    // defined against exactly this arithmetic.
    (d as f64) * (FACT_ARR[factor as usize] as f64) * FRAC_ARR[exponent as usize]
}

#[inline]
pub(crate) fn count_bits(max: i64, min: i64) -> u32 {
    64 - (max.wrapping_sub(min) as u64).leading_zeros()
}

/// Estimated storage bits for one (e,f) over a sample, per the reference
/// cost model: samples * range-bit-width + exceptions * 80. Returns
/// u64::MAX when fewer than two samples survive (never a usable candidate).
pub(crate) fn estimated_size_on_samples(samples: &[f64], exponent: u8, factor: u8) -> u64 {
    let mut non_exceptions = 0u32;
    let mut exceptions = 0u64;
    let mut max_enc = i64::MIN;
    let mut min_enc = i64::MAX;
    for &v in samples {
        let d = encode_value(v, exponent, factor);
        let dec = decode_value(d, exponent, factor);
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
    samples.len() as u64 * count_bits(max_enc, min_enc) as u64
        + exceptions * (EXCEPTION_SIZE_BITS + EXCEPTION_POSITION_SIZE_BITS)
}

pub(crate) struct RowgroupCombinations {
    /// Best-first (e,f) candidates, at most MAX_K_COMBINATIONS.
    pub combinations: Vec<(u8, u8)>,
    pub use_rd: bool,
}

/// First-stage selection: each sampled vector votes for its best (e,f);
/// the top-k voted combinations survive to the per-vector second stage.
/// The rowgroup switches to ALP-RD when even the best sampled vector
/// cannot beat the RD size threshold.
pub(crate) fn find_top_k_combinations(sampled_vectors: &[Vec<f64>]) -> RowgroupCombinations {
    // BTreeMap: vote iteration order must be deterministic (same input
    // must always yield the same (e,f) ranking).
    let mut votes: std::collections::BTreeMap<(u8, u8), u32> = std::collections::BTreeMap::new();
    let mut best_overall = u64::MAX;
    for samples in sampled_vectors {
        let n = samples.len() as u64;
        // Reference's worst-case initializer: every sample an exception
        // plus a full exception-width frame.
        let sentinel =
            n * (EXCEPTION_SIZE_BITS + EXCEPTION_POSITION_SIZE_BITS) + n * EXCEPTION_SIZE_BITS;
        let mut best_size = sentinel;
        let (mut best_e, mut best_f) = (0u8, 0u8);
        for e in (0..=MAX_EXPONENT).rev() {
            for f in (0..=e).rev() {
                let size = estimated_size_on_samples(samples, e, f);
                if size == u64::MAX {
                    continue;
                }
                // Ties prefer the larger exponent, then larger factor
                // (reference comparator).
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
        best_overall = best_overall.min(best_size);
    }
    let mut ranked: Vec<((u8, u8), u32)> = votes.into_iter().collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then(b.0 .0.cmp(&a.0 .0)).then(b.0 .1.cmp(&a.0 .1)));
    ranked.truncate(MAX_K_COMBINATIONS);
    RowgroupCombinations {
        combinations: ranked.into_iter().map(|(c, _)| c).collect(),
        use_rd: best_overall >= RD_SIZE_THRESHOLD_LIMIT,
    }
}

/// Second-stage selection: re-rank the rowgroup's k candidates on 32
/// equidistant samples of this vector, early-exiting after two consecutive
/// non-improvements (candidates arrive best-first, so a losing streak
/// rarely recovers).
pub(crate) fn choose_ef(combinations: &[(u8, u8)], values: &[f64]) -> (u8, u8) {
    debug_assert!(!combinations.is_empty());
    if combinations.len() == 1 {
        return combinations[0];
    }
    let mut buf = [0.0f64; SAMPLES_PER_VECTOR];
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
        let size = estimated_size_on_samples(samples, e, f);
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

/// One encoded 1024-value vector (the tail vector of an input may be
/// shorter; `len` is the logical count).
#[derive(Clone, Debug, PartialEq)]
pub struct AlpVector {
    pub len: u16,
    pub exponent: u8,
    pub factor: u8,
    pub bit_width: u8,
    pub for_base: i64,
    /// FFOR deltas in the FastLanes layout: bit_width * 16 words.
    pub packed: Vec<u64>,
    pub exc_positions: Vec<u16>,
    /// Original f64 bit images, parallel to exc_positions.
    pub exc_values: Vec<u64>,
}

impl AlpVector {
    /// Exact serialized-form accounting: len(2) + e/f/bw(3) + base(8) +
    /// exception count(2) + packed payload + 10 bytes per exception.
    pub fn size_bytes(&self) -> usize {
        2 + 3 + 8 + 2 + self.packed.len() * 8 + self.exc_positions.len() * 10
    }
}

pub(crate) fn encode_vector(values: &[f64], exponent: u8, factor: u8) -> AlpVector {
    let n = values.len();
    debug_assert!(0 < n && n <= VECTOR_SIZE);
    let mut encoded = [0i64; VECTOR_SIZE];
    let mut fail = [0u8; VECTOR_SIZE];
    // The verify loop carries no cross-iteration state: it writes the
    // encoded word and a failure predicate byte per slot (the compress
    // store into the exception list would serialize the loop — NEON has
    // no compress; the gather pass below does that scalar). Verification
    // is bit equality — stricter than the reference's f64 ==, so -0.0 and
    // every NaN payload are exceptions.
    for i in 0..n {
        let d = encode_value(values[i], exponent, factor);
        encoded[i] = d;
        let dec = decode_value(d, exponent, factor);
        fail[i] = (dec.to_bits() != values[i].to_bits()) as u8;
    }
    let mut exc_idx = [0u16; VECTOR_SIZE];
    let mut exc_count = 0usize;
    for i in 0..n {
        exc_idx[exc_count] = i as u16;
        exc_count += fail[i] as usize;
    }
    // Exceptions (and the padding of a short tail) are patched with a valid
    // in-range encoding so outlier bit patterns don't poison the pack width.
    let mut placeholder = 0i64;
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
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    for &d in encoded.iter() {
        min = min.min(d);
        max = max.max(d);
    }
    let mut deltas = [0u64; VECTOR_SIZE];
    for (delta, &d) in deltas.iter_mut().zip(encoded.iter()) {
        *delta = d.wrapping_sub(min) as u64;
    }
    let bit_width = count_bits(max, min);
    let mut packed = vec![0u64; bitpack::packed_words(bit_width)];
    bitpack::pack(&deltas, bit_width, &mut packed);
    AlpVector {
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

/// Decode one packed FFOR lane into all VECTOR_SIZE slots of `buf`:
/// exceptions unpatched, slots past the logical length decoding placeholder
/// garbage the caller truncates. Full-width unconditional loop over local
/// arrays — the autovectorizable shape (scvtf + two fmuls per lane on
/// NEON); do not reintroduce per-value branches or Vec::push here (the
/// ClickHouse lesson, see bitpack.rs).
pub(crate) fn decode_packed_into(
    packed: &[u64],
    bit_width: u32,
    exponent: u8,
    factor: u8,
    for_base: i64,
    buf: &mut [f64; VECTOR_SIZE],
) {
    let mut deltas = [0u64; VECTOR_SIZE];
    bitpack::unpack(packed, bit_width, &mut deltas);
    let fact = FACT_ARR[factor as usize] as f64;
    let frac = FRAC_ARR[exponent as usize];
    for i in 0..VECTOR_SIZE {
        let d = for_base.wrapping_add(deltas[i] as i64);
        buf[i] = (d as f64) * fact * frac;
    }
}

pub(crate) fn decode_vector(v: &AlpVector, out: &mut Vec<f64>) {
    let mut buf = [0.0f64; VECTOR_SIZE];
    decode_packed_into(&v.packed, v.bit_width as u32, v.exponent, v.factor, v.for_base, &mut buf);
    for (&pos, &bits) in v.exc_positions.iter().zip(v.exc_values.iter()) {
        buf[pos as usize] = f64::from_bits(bits);
    }
    out.extend_from_slice(&buf[..v.len as usize]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_value_known() {
        // d = 1.23 * 10^2 * 10^-0 = 123; back = 123 * 10^0 * 10^-2.
        assert_eq!(encode_value(1.23, 2, 0), 123);
        assert_eq!(decode_value(123, 2, 0).to_bits(), 1.23f64.to_bits());
        assert_eq!(encode_value(15.0, 1, 1), 15);
        assert_eq!(decode_value(15, 1, 1).to_bits(), 15.0f64.to_bits());
    }

    #[test]
    fn fast_round_ties_to_even() {
        assert_eq!(fast_round(2.5), 2.0);
        assert_eq!(fast_round(3.5), 4.0);
        assert_eq!(fast_round(-2.5), -2.0);
        assert_eq!(fast_round(1.4999), 1.0);
    }

    #[test]
    fn impossible_values_take_sentinel() {
        for v in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.0, 1e300] {
            assert_eq!(encode_value(v, 0, 0), ENCODING_UPPER_LIMIT as i64);
        }
    }

    #[test]
    fn all_exception_vector_roundtrips() {
        let values = vec![f64::NAN; 100];
        let v = encode_vector(&values, 3, 1);
        assert_eq!(v.exc_positions.len(), 100);
        assert_eq!(v.bit_width, 0);
        let mut out = Vec::new();
        decode_vector(&v, &mut out);
        assert_eq!(out.len(), 100);
        for x in out {
            assert_eq!(x.to_bits(), f64::NAN.to_bits());
        }
    }
}
