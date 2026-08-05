#[rustfmt::skip]
pub const PG_NUMBER_OF_ONES: [u8; 256] = [
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
];

#[inline(always)]
pub fn pg_popcount32(word: u32) -> i32 {
    word.count_ones() as i32
}

#[inline(always)]
pub fn pg_popcount64(word: u64) -> i32 {
    word.count_ones() as i32
}

/// word must not be 0.
#[inline(always)]
pub fn pg_leftmost_one_pos32(word: u32) -> i32 {
    debug_assert!(word != 0);
    31 - word.leading_zeros() as i32
}

/// word must not be 0.
#[inline(always)]
pub fn pg_leftmost_one_pos64(word: u64) -> i32 {
    debug_assert!(word != 0);
    63 - word.leading_zeros() as i32
}

/// word must not be 0.
#[inline(always)]
pub fn pg_rightmost_one_pos32(word: u32) -> i32 {
    debug_assert!(word != 0);
    word.trailing_zeros() as i32
}

/// word must not be 0.
#[inline(always)]
pub fn pg_rightmost_one_pos64(word: u64) -> i32 {
    debug_assert!(word != 0);
    word.trailing_zeros() as i32
}

/// num must be in 1..=2^31.
#[inline(always)]
pub fn pg_nextpower2_32(num: u32) -> u32 {
    debug_assert!(num > 0 && num <= u32::MAX / 2 + 1);
    if num & (num - 1) == 0 {
        num
    } else {
        1u32 << (pg_leftmost_one_pos32(num) + 1)
    }
}

/// num must be in 1..=2^63.
#[inline(always)]
pub fn pg_nextpower2_64(num: u64) -> u64 {
    debug_assert!(num > 0 && num <= u64::MAX / 2 + 1);
    if num & (num - 1) == 0 {
        num
    } else {
        1u64 << (pg_leftmost_one_pos64(num) + 1)
    }
}

/// num must not be 0.
#[inline(always)]
pub fn pg_prevpower2_32(num: u32) -> u32 {
    1u32 << pg_leftmost_one_pos32(num)
}

/// num must not be 0.
#[inline(always)]
pub fn pg_prevpower2_64(num: u64) -> u64 {
    1u64 << pg_leftmost_one_pos64(num)
}

#[inline(always)]
pub fn pg_ceil_log2_32(num: u32) -> u32 {
    if num < 2 {
        0
    } else {
        pg_leftmost_one_pos32(num - 1) as u32 + 1
    }
}

#[inline(always)]
pub fn pg_ceil_log2_64(num: u64) -> u64 {
    if num < 2 {
        0
    } else {
        pg_leftmost_one_pos64(num - 1) as u64 + 1
    }
}

#[inline(always)]
pub fn pg_rotate_right32(word: u32, n: i32) -> u32 {
    word.rotate_right(n as u32)
}

#[inline(always)]
pub fn pg_rotate_left32(word: u32, n: i32) -> u32 {
    word.rotate_left(n as u32)
}

#[inline]
pub fn pg_popcount(buf: &[u8]) -> u64 {
    if buf.len() < 8 {
        return buf.iter().map(|&b| PG_NUMBER_OF_ONES[b as usize] as u64).sum();
    }
    popcount_optimized(buf)
}

#[inline]
pub fn pg_popcount_masked(buf: &[u8], mask: u8) -> u64 {
    if buf.len() < 8 {
        return buf
            .iter()
            .map(|&b| PG_NUMBER_OF_ONES[(b & mask) as usize] as u64)
            .sum();
    }
    popcount_masked_optimized(buf, mask)
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn popcount_optimized(buf: &[u8]) -> u64 {
    // SAFETY: NEON is baseline on aarch64.
    unsafe { neon::popcount_neon(buf) }
}

#[cfg(target_arch = "aarch64")]
#[inline]
fn popcount_masked_optimized(buf: &[u8], mask: u8) -> u64 {
    // SAFETY: NEON is baseline on aarch64.
    unsafe { neon::popcount_masked_neon(buf, mask) }
}

#[cfg(not(target_arch = "aarch64"))]
#[inline]
fn popcount_optimized(buf: &[u8]) -> u64 {
    words_then_tail(buf, !0u64)
}

#[cfg(not(target_arch = "aarch64"))]
#[inline]
fn popcount_masked_optimized(buf: &[u8], mask: u8) -> u64 {
    words_then_tail(buf, u64::from_ne_bytes([mask; 8]))
}

#[cfg(not(target_arch = "aarch64"))]
#[inline]
fn words_then_tail(buf: &[u8], mask64: u64) -> u64 {
    let mut popcnt = 0u64;
    let mut chunks = buf.chunks_exact(8);
    for w in &mut chunks {
        popcnt += (u64::from_ne_bytes(w.try_into().unwrap()) & mask64).count_ones() as u64;
    }
    let mask = mask64 as u8;
    for &b in chunks.remainder() {
        popcnt += PG_NUMBER_OF_ONES[(b & mask) as usize] as u64;
    }
    popcnt
}

#[cfg(target_arch = "aarch64")]
mod neon {
    use super::PG_NUMBER_OF_ONES;
    use core::arch::aarch64::*;

    // C pg_popcount_aarch64.c NEON shape: 64B blocks on four u64x2
    // accumulators for ILP, one 32B block, 8B words, byte tail. SVE variant
    // not carried — no stable SVE intrinsics; V2's 128-bit SVE matches NEON
    // throughput here.
    #[inline(always)]
    unsafe fn accum_block(acc: uint64x2_t, p: *const u8) -> uint64x2_t {
        // SAFETY: caller guarantees 16 readable bytes at p.
        let vec = unsafe { vld1q_u8(p) };
        vpadalq_u32(acc, vpaddlq_u16(vpaddlq_u8(vcntq_u8(vec))))
    }

    #[inline(always)]
    unsafe fn accum_block_masked(acc: uint64x2_t, p: *const u8, maskv: uint8x16_t) -> uint64x2_t {
        // SAFETY: caller guarantees 16 readable bytes at p.
        let vec = vandq_u8(unsafe { vld1q_u8(p) }, maskv);
        vpadalq_u32(acc, vpaddlq_u16(vpaddlq_u8(vcntq_u8(vec))))
    }

    pub(super) unsafe fn popcount_neon(buf: &[u8]) -> u64 {
        let mut p = buf.as_ptr();
        let mut bytes = buf.len();
        let mut accum1 = vdupq_n_u64(0);
        let mut accum2 = vdupq_n_u64(0);
        let mut accum3 = vdupq_n_u64(0);
        let mut accum4 = vdupq_n_u64(0);
        let mut popcnt = 0u64;
        // SAFETY: every accum_block/read below stays within buf's `bytes`
        // remaining, decremented in lockstep with p.
        unsafe {
            while bytes >= 64 {
                accum1 = accum_block(accum1, p);
                accum2 = accum_block(accum2, p.add(16));
                accum3 = accum_block(accum3, p.add(32));
                accum4 = accum_block(accum4, p.add(48));
                p = p.add(64);
                bytes -= 64;
            }
            if bytes >= 32 {
                accum1 = accum_block(accum1, p);
                accum2 = accum_block(accum2, p.add(16));
                p = p.add(32);
                bytes -= 32;
            }
            popcnt += vaddvq_u64(vaddq_u64(accum1, accum2));
            popcnt += vaddvq_u64(vaddq_u64(accum3, accum4));
            while bytes >= 8 {
                popcnt += (p.cast::<u64>().read_unaligned()).count_ones() as u64;
                p = p.add(8);
                bytes -= 8;
            }
            while bytes > 0 {
                popcnt += PG_NUMBER_OF_ONES[p.read() as usize] as u64;
                p = p.add(1);
                bytes -= 1;
            }
        }
        popcnt
    }

    pub(super) unsafe fn popcount_masked_neon(buf: &[u8], mask: u8) -> u64 {
        let mut p = buf.as_ptr();
        let mut bytes = buf.len();
        let maskv = vdupq_n_u8(mask);
        let mask64 = u64::from_ne_bytes([mask; 8]);
        let mut accum1 = vdupq_n_u64(0);
        let mut accum2 = vdupq_n_u64(0);
        let mut accum3 = vdupq_n_u64(0);
        let mut accum4 = vdupq_n_u64(0);
        let mut popcnt = 0u64;
        // SAFETY: every accum_block_masked/read below stays within buf's
        // `bytes` remaining, decremented in lockstep with p.
        unsafe {
            while bytes >= 64 {
                accum1 = accum_block_masked(accum1, p, maskv);
                accum2 = accum_block_masked(accum2, p.add(16), maskv);
                accum3 = accum_block_masked(accum3, p.add(32), maskv);
                accum4 = accum_block_masked(accum4, p.add(48), maskv);
                p = p.add(64);
                bytes -= 64;
            }
            if bytes >= 32 {
                accum1 = accum_block_masked(accum1, p, maskv);
                accum2 = accum_block_masked(accum2, p.add(16), maskv);
                p = p.add(32);
                bytes -= 32;
            }
            popcnt += vaddvq_u64(vaddq_u64(accum1, accum2));
            popcnt += vaddvq_u64(vaddq_u64(accum3, accum4));
            while bytes >= 8 {
                popcnt += (p.cast::<u64>().read_unaligned() & mask64).count_ones() as u64;
                p = p.add(8);
                bytes -= 8;
            }
            while bytes > 0 {
                popcnt += PG_NUMBER_OF_ONES[(p.read() & mask) as usize] as u64;
                p = p.add(1);
                bytes -= 1;
            }
        }
        popcnt
    }
}

#[cfg(test)]
mod tests;
