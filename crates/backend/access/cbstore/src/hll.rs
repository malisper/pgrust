//! Ingest-time per-column NDV sketch (HyperLogLog, p=14: ~0.8% standard
//! error at 16KiB/column — exact tracking would need GB-scale sets at
//! ClickBench text cardinalities, and planner NDV tolerates ~1% error).

use std::hash::Hasher;

const P: u32 = 14;
const M: usize = 1 << P;

pub struct Hll {
    regs: Box<[u8; M]>,
}

impl Default for Hll {
    fn default() -> Self {
        Hll { regs: vec![0u8; M].into_boxed_slice().try_into().unwrap() }
    }
}

impl Hll {
    pub fn add_hash(&mut self, h: u64) {
        let idx = (h >> (64 - P)) as usize;
        // rho over the low 64-P bits; +1 per the HLL definition, saturating
        // when those bits are all zero.
        let rho = ((h << P) | 1u64 << (P - 1)).leading_zeros() as u8 + 1;
        if rho > self.regs[idx] {
            self.regs[idx] = rho;
        }
    }

    pub fn add_i64(&mut self, v: i64) {
        self.add_hash(mix64(v as u64));
    }

    pub fn add_bytes(&mut self, b: &[u8]) {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        h.write(b);
        self.add_hash(h.finish());
    }

    pub fn estimate(&self) -> u64 {
        let m = M as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let mut sum = 0.0f64;
        let mut zeros = 0u32;
        for &r in self.regs.iter() {
            sum += (-(r as f64)).exp2();
            if r == 0 {
                zeros += 1;
            }
        }
        let e = alpha * m * m / sum;
        let est = if e <= 2.5 * m && zeros > 0 { m * (m / zeros as f64).ln() } else { e };
        est.round() as u64
    }
}

// splitmix64 finalizer.
pub(crate) fn mix64(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_exact_band() {
        let mut h = Hll::default();
        for i in 0..1000i64 {
            h.add_i64(i);
        }
        let e = h.estimate() as i64;
        assert!((990..=1010).contains(&e), "est {e}");
    }

    #[test]
    fn large_error_band() {
        let mut h = Hll::default();
        for i in 0..5_000_000i64 {
            h.add_i64(i);
        }
        let e = h.estimate() as f64;
        assert!((e / 5_000_000.0 - 1.0).abs() < 0.03, "est {e}");
    }

    #[test]
    fn text_dedup() {
        let mut h = Hll::default();
        for i in 0..10_000u32 {
            h.add_bytes(format!("key-{}", i % 100).as_bytes());
        }
        let e = h.estimate();
        assert!((95..=105).contains(&e), "est {e}");
    }
}
