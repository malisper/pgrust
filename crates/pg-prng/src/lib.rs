use std::cell::RefCell;

const FALLBACK_S0: u64 = 0x5851_f42d_4c95_7f2d;
const FALLBACK_S1: u64 = 0x1405_7b7e_f767_814f;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PgPrng {
    s0: u64,
    s1: u64,
}

impl PgPrng {
    /// Rebuild a generator from its raw xoroshiro128** state words (the
    /// `pg_prng_state { s0, s1 }` pair). Exact-state round-trip with [`raw`].
    ///
    /// [`raw`]: PgPrng::raw
    pub const fn from_raw(s0: u64, s1: u64) -> Self {
        Self { s0, s1 }
    }

    /// The raw `(s0, s1)` state words (the C `pg_prng_state` fields), for
    /// carrying the generator state across an owned-value boundary.
    pub const fn raw(self) -> (u64, u64) {
        (self.s0, self.s1)
    }

    pub fn seeded(seed: u64) -> Self {
        let mut state = Self::default();
        state.seed(seed);
        state
    }

    pub fn seeded_from_f64(seed: f64) -> Self {
        let mut state = Self::default();
        state.seed_from_f64(seed);
        state
    }

    pub fn seed(&mut self, mut seed: u64) {
        self.s0 = splitmix64(&mut seed);
        self.s1 = splitmix64(&mut seed);
        self.ensure_seeded();
    }

    pub fn seed_from_f64(&mut self, seed: f64) {
        let seed = (((1_u64 << 52) - 1) as f64 * seed) as i64;
        self.seed(seed as u64);
    }

    pub fn ensure_seeded(&mut self) -> bool {
        if self.s0 == 0 && self.s1 == 0 {
            self.s0 = FALLBACK_S0;
            self.s1 = FALLBACK_S1;
        }

        true
    }

    pub fn next_u64(&mut self) -> u64 {
        self.xoroshiro128ss()
    }

    pub fn u64_range(&mut self, min: u64, max: u64) -> u64 {
        let value = if max > min {
            let range = max.wrapping_sub(min);
            let shift = 63 - leftmost_one_pos64(range);

            loop {
                let value = self.xoroshiro128ss() >> shift;
                if value <= range {
                    break value;
                }
            }
        } else {
            0
        };

        min.wrapping_add(value)
    }

    pub fn next_i64(&mut self) -> i64 {
        self.xoroshiro128ss() as i64
    }

    pub fn next_nonnegative_i64(&mut self) -> i64 {
        (self.xoroshiro128ss() & 0x7fff_ffff_ffff_ffff) as i64
    }

    pub fn i64_range(&mut self, min: i64, max: i64) -> i64 {
        if max > min {
            min.wrapping_add(self.u64_range(0, (max as u64).wrapping_sub(min as u64)) as i64)
        } else {
            min
        }
    }

    pub fn next_u32(&mut self) -> u32 {
        (self.xoroshiro128ss() >> 32) as u32
    }

    pub fn next_i32(&mut self) -> i32 {
        (self.xoroshiro128ss() >> 32) as i32
    }

    pub fn next_nonnegative_i32(&mut self) -> i32 {
        (self.xoroshiro128ss() >> 33) as i32
    }

    pub fn next_f64(&mut self) -> f64 {
        ((self.xoroshiro128ss() >> (64 - 52)) as f64) * 2_f64.powi(-52)
    }

    pub fn normal_f64(&mut self) -> f64 {
        let u1 = 1.0 - self.next_f64();
        let u2 = 1.0 - self.next_f64();
        (-2.0 * u1.ln()).sqrt() * (2.0 * core::f64::consts::PI * u2).sin()
    }

    pub fn next_bool(&mut self) -> bool {
        self.xoroshiro128ss() >> 63 != 0
    }

    fn xoroshiro128ss(&mut self) -> u64 {
        let s0 = self.s0;
        let sx = self.s1 ^ s0;
        let value = s0.wrapping_mul(5).rotate_left(7).wrapping_mul(9);

        self.s0 = s0.rotate_left(24) ^ sx ^ (sx << 16);
        self.s1 = sx.rotate_left(37);

        value
    }
}

thread_local! {
    /// `pg_global_prng_state` (pg_prng.c) lives in backend-private memory —
    /// every backend has its own stream (reseeded per backend in
    /// `InitProcessGlobals`) — so it is thread-local here, never a shared
    /// static.
    static GLOBAL_PRNG: RefCell<PgPrng> = const { RefCell::new(PgPrng::from_raw(0, 0)) };
}

pub fn global_prng<R>(f: impl FnOnce(&mut PgPrng) -> R) -> R {
    GLOBAL_PRNG.with_borrow_mut(f)
}

fn leftmost_one_pos64(word: u64) -> u32 {
    63 - word.leading_zeros()
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_state_is_replaced_with_fallback_seed() {
        let mut state = PgPrng::default();

        assert!(state.ensure_seeded());

        assert_eq!(state.raw(), (FALLBACK_S0, FALLBACK_S1));
    }

    #[test]
    fn seeded_states_produce_same_sequence() {
        let mut left = PgPrng::seeded(42);
        let mut right = PgPrng::seeded(42);

        let left_values = [left.next_u64(), left.next_u64(), left.next_u64()];
        let right_values = [right.next_u64(), right.next_u64(), right.next_u64()];

        assert_eq!(left_values, right_values);
        assert_eq!(
            left_values,
            [
                0x69e8_5b36_3138_1baa,
                0x3bc3_2c54_1d62_6e1d,
                0x3e35_de64_b3b3_78d8,
            ]
        );
    }

    #[test]
    fn range_with_equal_bounds_returns_minimum() {
        let mut state = PgPrng::seeded(1);

        assert_eq!(state.u64_range(9, 9), 9);
        assert_eq!(state.i64_range(-7, -7), -7);
    }

    #[test]
    fn ranges_are_inclusive() {
        let mut state = PgPrng::seeded(99);

        for _ in 0..100 {
            let value = state.u64_range(10, 20);
            assert!((10..=20).contains(&value));
        }

        for _ in 0..100 {
            let value = state.i64_range(-20, -10);
            assert!((-20..=-10).contains(&value));
        }
    }

    #[test]
    fn positive_integer_variants_are_non_negative() {
        let mut state = PgPrng::seeded(123);

        assert!(state.next_nonnegative_i64() >= 0);
        assert!(state.next_nonnegative_i32() >= 0);
    }

    #[test]
    fn double_is_in_unit_interval() {
        let mut state = PgPrng::seeded(456);

        for _ in 0..100 {
            let value = state.next_f64();
            assert!((0.0..1.0).contains(&value));
        }
    }

    #[test]
    fn bool_advances_state() {
        let mut state = PgPrng::seeded(789);
        let before = state;

        let _ = state.next_bool();

        assert_ne!(state, before);
    }

    #[test]
    fn global_state_accessor_mutates_shared_state() {
        global_prng(|state| state.seed(55));
        let before = global_prng(|state| *state);

        let _ = global_prng(PgPrng::next_u64);

        let after = global_prng(|state| *state);
        assert_ne!(after, before);
    }

    #[test]
    fn global_state_is_per_thread() {
        // C's pg_global_prng_state is backend-private memory: seeding it on
        // this thread must not move another thread's stream.
        global_prng(|state| state.seed(1234));
        let here = global_prng(|state| *state);

        std::thread::spawn(move || {
            let there = global_prng(|state| *state);
            assert_ne!(
                there, here,
                "fresh thread saw another thread's global PRNG state"
            );
            assert_eq!(there, PgPrng::from_raw(0, 0));
        })
        .join()
        .unwrap();
    }
}
