// bloomfilter.c: k <= 2 real hashes via hash_bytes_extended, enhanced double
// hashing over a power-of-two bitset; contents are bit-identical to C for the
// same inputs and seed.

use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_error::PgResult;

const MAX_HASH_FUNCS: usize = 10;
const BITS_PER_BYTE: u64 = 8;

pub struct BloomFilter<'mcx> {
    k_hash_funcs: i32,
    seed: u64,
    // m is bitset size, in bits.  Must be a power of two <= 2^32.
    m: u64,
    bitset: PgVec<'mcx, u8>,
}

impl<'mcx> BloomFilter<'mcx> {
    pub fn create_in(
        mcx: Mcx<'mcx>,
        total_elems: i64,
        bloom_work_mem: i32,
        seed: u64,
    ) -> PgResult<BloomFilter<'mcx>> {
        let mut bitset_bytes = u64::min(
            (bloom_work_mem as u64).wrapping_mul(1024),
            total_elems.wrapping_mul(2) as u64,
        );
        bitset_bytes = u64::max(1024 * 1024, bitset_bytes);

        let bloom_power = my_bloom_power(bitset_bytes.wrapping_mul(BITS_PER_BYTE));
        let bitset_bits = 1u64 << bloom_power;
        bitset_bytes = bitset_bits / BITS_PER_BYTE;

        let mut bitset = vec_with_capacity_in::<u8>(mcx, bitset_bytes as usize)?;
        bitset.resize(bitset_bytes as usize, 0);

        Ok(BloomFilter {
            k_hash_funcs: optimal_k(bitset_bits, total_elems),
            seed,
            m: bitset_bits,
            bitset,
        })
    }

    #[inline]
    pub fn add_element(&mut self, elem: &[u8]) {
        let mut hashes = [0u32; MAX_HASH_FUNCS];
        k_hashes(self.k_hash_funcs, self.seed, self.m, elem, &mut hashes);

        for &hash in &hashes[..self.k_hash_funcs as usize] {
            // SAFETY: k_hashes yields hash < m (mod_m) and bitset.len() == m/8.
            unsafe { *self.bitset.get_unchecked_mut((hash >> 3) as usize) |= 1 << (hash & 7) };
        }
    }

    /// True means the element is definitely not in the set; false means it is
    /// probably present.
    #[inline]
    pub fn lacks_element(&self, elem: &[u8]) -> bool {
        let mut hashes = [0u32; MAX_HASH_FUNCS];
        k_hashes(self.k_hash_funcs, self.seed, self.m, elem, &mut hashes);

        for &hash in &hashes[..self.k_hash_funcs as usize] {
            // SAFETY: k_hashes yields hash < m (mod_m) and bitset.len() == m/8.
            if unsafe { *self.bitset.get_unchecked((hash >> 3) as usize) } & (1 << (hash & 7)) == 0
            {
                return true;
            }
        }

        false
    }

    pub fn prop_bits_set(&self) -> f64 {
        let bits_set: u64 = self.bitset.iter().map(|b| u64::from(b.count_ones())).sum();

        bits_set as f64 / self.m as f64
    }

    pub fn bitset(&self) -> &[u8] {
        &self.bitset
    }

    pub fn k_hash_funcs(&self) -> i32 {
        self.k_hash_funcs
    }

    pub fn bitset_bits(&self) -> u64 {
        self.m
    }
}

fn my_bloom_power(mut target_bitset_bits: u64) -> i32 {
    let mut bloom_power: i32 = -1;

    while target_bitset_bits > 0 && bloom_power < 32 {
        bloom_power += 1;
        target_bitset_bits >>= 1;
    }

    bloom_power
}

fn optimal_k(bitset_bits: u64, total_elems: i64) -> i32 {
    // C rint(): round half to even in the default FP environment.
    let k = (core::f64::consts::LN_2 * bitset_bits as f64 / total_elems as f64).round_ties_even()
        as i32;

    i32::max(1, i32::min(k, MAX_HASH_FUNCS as i32))
}

#[inline]
fn k_hashes(k_hash_funcs: i32, seed: u64, m: u64, elem: &[u8], hashes: &mut [u32; MAX_HASH_FUNCS]) {
    let hash = hashfn::hash_bytes_extended(elem, seed);
    let mut x = mod_m(hash as u32, m);
    let mut y = mod_m((hash >> 32) as u32, m);

    hashes[0] = x;
    for i in 1..k_hash_funcs {
        x = mod_m(x.wrapping_add(y), m);
        y = mod_m(y.wrapping_add(i as u32), m);

        hashes[i as usize] = x;
    }
}

#[inline]
fn mod_m(val: u32, m: u64) -> u32 {
    debug_assert!(m <= u32::MAX as u64 + 1);
    debug_assert!((m.wrapping_sub(1)) & m == 0);

    (val as u64 & m.wrapping_sub(1)) as u32
}

#[cfg(test)]
mod tests;
