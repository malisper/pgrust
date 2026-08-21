//! ALP-RD: the fallback for "real doubles" that no (e,f) scaling captures.
//! Each f64 splits at a per-rowgroup cut position p >= 48 into right (low)
//! bits stored packed raw and a left (high) part coded through a <=8-entry
//! dictionary; dictionary misses are stored as (position, left-bits)
//! exceptions. Lossless for every bit pattern by construction — NaN
//! payloads, infinities, denormals and -0.0 included.

use crate::bitpack::{self, LANES};
use crate::constants::*;

#[derive(Clone, Debug, PartialEq)]
pub struct RdDictionary {
    /// Cut position: how many low bits go to the raw-packed substream
    /// (48..=63, i.e. left part always fits u16).
    pub right_bit_width: u8,
    /// Packed code width: max(1, ceil(log2(dict len))), 1..=3.
    pub left_bit_width: u8,
    /// Most-frequent-first left parts; index == packed code.
    pub dict: Vec<u16>,
}

impl RdDictionary {
    /// Serialized size in a granule frame (the form of record, granule.rs):
    /// right/left widths + entry-count byte + the entries.
    pub fn size_bytes(&self) -> usize {
        3 + self.dict.len() * 2
    }
}

fn ceil_log2(n: usize) -> u32 {
    debug_assert!(n >= 1);
    usize::BITS - (n - 1).leading_zeros()
}

/// Build the dictionary for one candidate cut and return the reference cost
/// estimate in bits/value: right + code width + amortized exception cost.
fn build_left_parts_dictionary(samples: &[f64], right_bit_width: u8) -> (RdDictionary, f64) {
    // BTreeMap + (count desc, value asc) sort: dictionary content must be a
    // pure function of the sample (the reference iterates an unordered hash
    // map; we need determinism).
    let mut counts: std::collections::BTreeMap<u16, u32> = std::collections::BTreeMap::new();
    for &v in samples {
        let left = (v.to_bits() >> right_bit_width) as u16;
        *counts.entry(left).or_insert(0) += 1;
    }
    let mut sorted: Vec<(u16, u32)> = counts.into_iter().collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    let dict_len = sorted.len().min(MAX_RD_DICTIONARY_SIZE);
    let exceptions: u64 = sorted[dict_len..].iter().map(|&(_, c)| c as u64).sum();
    let dict: Vec<u16> = sorted[..dict_len].iter().map(|&(v, _)| v).collect();
    let left_bit_width = ceil_log2(dict_len).max(1) as u8;
    let exceptions_bits = exceptions * (RD_EXCEPTION_SIZE_BITS + RD_EXCEPTION_POSITION_SIZE_BITS);
    let estimated = right_bit_width as f64
        + left_bit_width as f64
        + exceptions_bits as f64 / samples.len() as f64;
    (
        RdDictionary { right_bit_width, left_bit_width, dict },
        estimated,
    )
}

/// Try every legal cut (left widths 1..=CUTTING_LIMIT) on the rowgroup
/// sample; keep the cheapest. Ties keep the earlier candidate (larger
/// right width), matching the reference's strict-less update.
pub(crate) fn find_best_dictionary(samples: &[f64]) -> RdDictionary {
    debug_assert!(!samples.is_empty());
    let mut best: Option<(f64, RdDictionary)> = None;
    for left_width in 1..=CUTTING_LIMIT {
        let right_bit_width = (64 - left_width) as u8;
        let (dict, estimated) = build_left_parts_dictionary(samples, right_bit_width);
        if best.as_ref().is_none_or(|(b, _)| estimated < *b) {
            best = Some((estimated, dict));
        }
    }
    best.unwrap().1
}

#[derive(Clone, Debug, PartialEq)]
pub struct RdVector {
    pub len: u16,
    /// right_bit_width * 16 words, FastLanes layout.
    pub packed_right: Vec<u64>,
    /// left_bit_width * 16 words, FastLanes layout.
    pub packed_left: Vec<u64>,
    pub exc_positions: Vec<u16>,
    /// Left parts for dictionary misses, parallel to exc_positions.
    pub exc_left: Vec<u16>,
}

impl RdVector {
    /// len(2) + exception count(2) + both packed substreams + 4 bytes per
    /// exception.
    pub fn size_bytes(&self) -> usize {
        2 + 2 + (self.packed_right.len() + self.packed_left.len()) * 8
            + self.exc_positions.len() * 4
    }
}

pub(crate) fn encode_rd_vector(values: &[f64], d: &RdDictionary) -> RdVector {
    let n = values.len();
    debug_assert!(0 < n && n <= VECTOR_SIZE);
    let r = d.right_bit_width as u32;
    let right_mask = (1u64 << r) - 1;
    // Dictionary padded to its max size with entry 0 so the match scan is
    // a fixed-shape 8-wide compare-select (the early-exit position() scan
    // mispredicts on spread-out left parts and serializes the loop). The
    // descending overwrite keeps the LOWEST matching code, so padding —
    // which duplicates entry 0 — can never shadow a real code or fake a
    // hit that entry 0 would not have produced.
    let mut table = [d.dict[0]; MAX_RD_DICTIONARY_SIZE];
    table[..d.dict.len()].copy_from_slice(&d.dict);
    let mut rights = [0u64; VECTOR_SIZE];
    let mut lefts = [0u64; VECTOR_SIZE];
    let mut miss = [0u8; VECTOR_SIZE];
    for i in 0..n {
        let bits = values[i].to_bits();
        rights[i] = bits & right_mask;
        let left = (bits >> r) as u16;
        let mut code = MAX_RD_DICTIONARY_SIZE;
        for j in (0..MAX_RD_DICTIONARY_SIZE).rev() {
            if table[j] == left {
                code = j;
            }
        }
        miss[i] = (code == MAX_RD_DICTIONARY_SIZE) as u8;
        // Misses store the always-valid code 0; the decode patch
        // overwrites the whole left part.
        lefts[i] = if code == MAX_RD_DICTIONARY_SIZE { 0 } else { code as u64 };
    }
    let mut exc_positions = Vec::new();
    let mut exc_left = Vec::new();
    for i in 0..n {
        if miss[i] != 0 {
            exc_positions.push(i as u16);
            exc_left.push((values[i].to_bits() >> r) as u16);
        }
    }
    // Tail padding stays zero: code 0 always indexes a real dict entry.
    let mut packed_right = vec![0u64; bitpack::packed_words(r)];
    bitpack::pack(&rights, r, &mut packed_right);
    let lbw = d.left_bit_width as u32;
    let mut packed_left = vec![0u64; bitpack::packed_words(lbw)];
    bitpack::pack(&lefts, lbw, &mut packed_left);
    debug_assert_eq!(packed_right.len(), r as usize * LANES);
    RdVector {
        len: n as u16,
        packed_right,
        packed_left,
        exc_positions,
        exc_left,
    }
}

/// Decode both packed lanes into all VECTOR_SIZE slots of `buf`:
/// dictionary-miss exceptions unpatched (those slots decode through the
/// stored placeholder code), slots past the logical length garbage the
/// caller truncates. The dictionary gather is flattened into a pre-shifted
/// 8-slot table indexed by masked code — branch-free and bounds-check-free
/// (the autovectorizable shape; padded slots repeat the last real entry
/// and are only reachable through exception positions, which the caller
/// overwrites).
pub(crate) fn decode_rd_packed_into(
    packed_right: &[u64],
    packed_left: &[u64],
    d: &RdDictionary,
    buf: &mut [f64; VECTOR_SIZE],
) {
    let mut rights = [0u64; VECTOR_SIZE];
    let mut lefts = [0u64; VECTOR_SIZE];
    bitpack::unpack(packed_right, d.right_bit_width as u32, &mut rights);
    bitpack::unpack(packed_left, d.left_bit_width as u32, &mut lefts);
    let r = d.right_bit_width as u32;
    let mut table = [0u64; MAX_RD_DICTIONARY_SIZE];
    for (j, slot) in table.iter_mut().enumerate() {
        *slot = (d.dict[j.min(d.dict.len() - 1)] as u64) << r;
    }
    for i in 0..VECTOR_SIZE {
        buf[i] = f64::from_bits(table[lefts[i] as usize & (MAX_RD_DICTIONARY_SIZE - 1)] | rights[i]);
    }
}

pub(crate) fn decode_rd_vector(v: &RdVector, d: &RdDictionary, out: &mut Vec<f64>) {
    let mut buf = [0.0f64; VECTOR_SIZE];
    decode_rd_packed_into(&v.packed_right, &v.packed_left, d, &mut buf);
    let r = d.right_bit_width as u32;
    let right_mask = (1u64 << r) - 1;
    for (&pos, &left) in v.exc_positions.iter().zip(v.exc_left.iter()) {
        let i = pos as usize;
        // The right (low) bits decoded correctly even on a dictionary miss;
        // recover them from the placeholder-decoded slot, patch the left.
        let right = buf[i].to_bits() & right_mask;
        buf[i] = f64::from_bits(((left as u64) << r) | right);
    }
    out.extend_from_slice(&buf[..v.len as usize]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ceil_log2_values() {
        assert_eq!(ceil_log2(1), 0);
        assert_eq!(ceil_log2(2), 1);
        assert_eq!(ceil_log2(3), 2);
        assert_eq!(ceil_log2(8), 3);
    }

    #[test]
    fn rd_roundtrips_arbitrary_bits() {
        let values: Vec<f64> = (0..1500u64)
            .map(|i| f64::from_bits(i.wrapping_mul(0x9E37_79B9_7F4A_7C15)))
            .collect();
        let dict = find_best_dictionary(&values[..256]);
        assert!(dict.right_bit_width >= 48);
        assert!(dict.dict.len() <= MAX_RD_DICTIONARY_SIZE);
        let mut out = Vec::new();
        for chunk in values.chunks(VECTOR_SIZE) {
            let v = encode_rd_vector(chunk, &dict);
            decode_rd_vector(&v, &dict, &mut out);
        }
        assert_eq!(out.len(), values.len());
        for (a, b) in values.iter().zip(out.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }
}
