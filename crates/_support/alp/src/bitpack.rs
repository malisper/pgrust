//! FastLanes-style bit-packing of one 1024-value vector (VLDB 2023).
//!
//! Layout: the vector is treated as a virtual 1024-bit register of 16 u64
//! lanes; value `i` lives in lane `i % 16`, slot `i / 16`, and packed words
//! are stored lane-interleaved (word `w` of lane `l` at index `w * 16 + l`).
//! Both kernels keep a plain inner loop over the 16 lanes with local
//! accumulator arrays so LLVM autovectorizes them; do not rewrite them with
//! per-value data-dependent branches or shared &mut state.

use crate::constants::VECTOR_SIZE;

pub const LANES: usize = 16;
const ROWS: usize = VECTOR_SIZE / LANES;

/// Number of u64 words `pack` produces for a given bit width.
#[inline]
pub fn packed_words(bit_width: u32) -> usize {
    bit_width as usize * LANES
}

/// Pack the low `bit_width` bits of each value. `out` must hold exactly
/// `packed_words(bit_width)` words.
pub fn pack(input: &[u64; VECTOR_SIZE], bit_width: u32, out: &mut [u64]) {
    debug_assert!(bit_width <= 64);
    debug_assert_eq!(out.len(), packed_words(bit_width));
    if bit_width == 0 {
        return;
    }
    if bit_width == 64 {
        // Full width: the interleaved layout coincides with value order.
        out.copy_from_slice(input);
        return;
    }
    let mask = (1u64 << bit_width) - 1;
    let mut acc = [0u64; LANES];
    let mut filled: u32 = 0;
    let mut out_word = 0usize;
    for row in 0..ROWS {
        let base = row * LANES;
        let rem = 64 - filled;
        if bit_width <= rem {
            for l in 0..LANES {
                acc[l] |= (input[base + l] & mask) << filled;
            }
            filled += bit_width;
            if filled == 64 {
                out[out_word * LANES..(out_word + 1) * LANES].copy_from_slice(&acc);
                acc = [0u64; LANES];
                out_word += 1;
                filled = 0;
            }
        } else {
            // Value straddles the word boundary: low `rem` bits land in the
            // current word (the shift discards the high part), the rest
            // carries into a fresh accumulator.
            for l in 0..LANES {
                acc[l] |= (input[base + l] & mask) << filled;
            }
            out[out_word * LANES..(out_word + 1) * LANES].copy_from_slice(&acc);
            out_word += 1;
            for l in 0..LANES {
                acc[l] = (input[base + l] & mask) >> rem;
            }
            filled = bit_width - rem;
        }
    }
    if filled > 0 {
        out[out_word * LANES..(out_word + 1) * LANES].copy_from_slice(&acc);
    }
}

/// Inverse of [`pack`]. `packed` must hold `packed_words(bit_width)` words.
pub fn unpack(packed: &[u64], bit_width: u32, out: &mut [u64; VECTOR_SIZE]) {
    debug_assert!(bit_width <= 64);
    debug_assert_eq!(packed.len(), packed_words(bit_width));
    if bit_width == 0 {
        out.fill(0);
        return;
    }
    if bit_width == 64 {
        out.copy_from_slice(packed);
        return;
    }
    let mask = (1u64 << bit_width) - 1;
    let mut cur = [0u64; LANES];
    cur.copy_from_slice(&packed[0..LANES]);
    let mut consumed: u32 = 0;
    let mut word = 0usize;
    for row in 0..ROWS {
        if consumed == 64 {
            word += 1;
            cur.copy_from_slice(&packed[word * LANES..(word + 1) * LANES]);
            consumed = 0;
        }
        let base = row * LANES;
        let rem = 64 - consumed;
        if bit_width <= rem {
            for l in 0..LANES {
                out[base + l] = (cur[l] >> consumed) & mask;
            }
            consumed += bit_width;
        } else {
            let mut vals = [0u64; LANES];
            for l in 0..LANES {
                vals[l] = cur[l] >> consumed;
            }
            word += 1;
            cur.copy_from_slice(&packed[word * LANES..(word + 1) * LANES]);
            for l in 0..LANES {
                out[base + l] = (vals[l] | (cur[l] << rem)) & mask;
            }
            consumed = bit_width - rem;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Deterministic splitmix64 so failures reproduce.
    fn splitmix(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    #[test]
    fn roundtrip_every_bit_width() {
        let mut seed = 0x5EED_u64;
        for bw in 0..=64u32 {
            let mask = if bw == 64 { u64::MAX } else { (1u64 << bw) - 1 };
            let mut input = [0u64; VECTOR_SIZE];
            for v in input.iter_mut() {
                *v = splitmix(&mut seed) & mask;
            }
            let mut packed = vec![0u64; packed_words(bw)];
            pack(&input, bw, &mut packed);
            let mut output = [1u64; VECTOR_SIZE];
            unpack(&packed, bw, &mut output);
            assert_eq!(input[..], output[..], "bit_width {bw}");
        }
    }

    #[test]
    fn pack_masks_high_bits() {
        let mut input = [u64::MAX; VECTOR_SIZE];
        input[7] = 0;
        let bw = 5u32;
        let mut packed = vec![0u64; packed_words(bw)];
        pack(&input, bw, &mut packed);
        let mut output = [0u64; VECTOR_SIZE];
        unpack(&packed, bw, &mut output);
        assert_eq!(output[0], 0b11111);
        assert_eq!(output[7], 0);
        assert_eq!(output[1023], 0b11111);
    }
}
