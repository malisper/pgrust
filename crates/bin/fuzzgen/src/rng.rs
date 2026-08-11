//! Deterministic PRNG: xoshiro256** seeded via splitmix64. Pure integer
//! arithmetic so streams are identical across platforms; never seeded from
//! OS entropy.

pub struct Rng {
    s: [u64; 4],
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

impl Rng {
    pub fn new(seed: u64) -> Rng {
        let mut sm = seed;
        Rng {
            s: [
                splitmix64(&mut sm),
                splitmix64(&mut sm),
                splitmix64(&mut sm),
                splitmix64(&mut sm),
            ],
        }
    }

    pub fn next_u64(&mut self) -> u64 {
        let result = self.s[1]
            .wrapping_mul(5)
            .rotate_left(7)
            .wrapping_mul(9);
        let t = self.s[1] << 17;
        self.s[2] ^= self.s[0];
        self.s[3] ^= self.s[1];
        self.s[1] ^= self.s[2];
        self.s[0] ^= self.s[3];
        self.s[2] ^= t;
        self.s[3] = self.s[3].rotate_left(45);
        result
    }

    /// Uniform value in [0, bound). `bound` must be nonzero.
    pub fn below(&mut self, bound: u64) -> u64 {
        debug_assert!(bound > 0);
        // Lemire multiply-shift reduction; slight bias is irrelevant here
        // and it stays deterministic without a rejection loop.
        ((u128::from(self.next_u64()) * u128::from(bound)) >> 64) as u64
    }

    pub fn below_usize(&mut self, bound: usize) -> usize {
        self.below(bound as u64) as usize
    }

    /// Uniform value in [lo, hi] inclusive.
    pub fn range_i64(&mut self, lo: i64, hi: i64) -> i64 {
        debug_assert!(lo <= hi);
        let span = (hi as u64).wrapping_sub(lo as u64).wrapping_add(1);
        if span == 0 {
            // Full i64 domain.
            return self.next_u64() as i64;
        }
        lo.wrapping_add(self.below(span) as i64)
    }

    /// True with probability num/den.
    pub fn chance(&mut self, num: u64, den: u64) -> bool {
        self.below(den) < num
    }

    /// Uniform f64 in [0, 1).
    pub fn f64_unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }

    pub fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below_usize(xs.len())]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_stream() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seeds_differ() {
        let mut a = Rng::new(1);
        let mut b = Rng::new(2);
        let av: Vec<u64> = (0..8).map(|_| a.next_u64()).collect();
        let bv: Vec<u64> = (0..8).map(|_| b.next_u64()).collect();
        assert_ne!(av, bv);
    }

    #[test]
    fn below_in_range() {
        let mut r = Rng::new(7);
        for _ in 0..1000 {
            assert!(r.below(10) < 10);
        }
    }
}
