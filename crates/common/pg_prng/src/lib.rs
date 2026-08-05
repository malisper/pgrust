use std::cell::Cell;

const FALLBACK_S0: u64 = 0x5851_f42d_4c95_7f2d;
const FALLBACK_S1: u64 = 0x1405_7b7e_f767_814f;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PgPrng {
    s0: u64,
    s1: u64,
}

impl PgPrng {
    pub const fn from_raw(s0: u64, s1: u64) -> Self {
        Self { s0, s1 }
    }

    pub const fn raw(self) -> (u64, u64) {
        (self.s0, self.s1)
    }

    pub fn seeded(seed: u64) -> Self {
        let mut state = Self::default();
        state.seed(seed);
        state
    }

    pub fn seed(&mut self, mut seed: u64) {
        self.s0 = splitmix64(&mut seed);
        self.s1 = splitmix64(&mut seed);
        self.ensure_seeded();
    }

    pub fn fseed(&mut self, fseed: f64) {
        let seed = (((1_u64 << 52) - 1) as f64 * fseed) as i64;
        self.seed(seed as u64);
    }

    // pg_prng_seed_check: an all-zero state is the xoroshiro fixed point.
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

    pub fn u64_range(&mut self, rmin: u64, rmax: u64) -> u64 {
        let val = if rmax > rmin {
            let range = rmax - rmin;
            let rshift = range.leading_zeros();
            loop {
                let val = self.xoroshiro128ss() >> rshift;
                if val <= range {
                    break val;
                }
            }
        } else {
            0
        };
        rmin.wrapping_add(val)
    }

    pub fn next_i64(&mut self) -> i64 {
        self.xoroshiro128ss() as i64
    }

    pub fn next_nonnegative_i64(&mut self) -> i64 {
        (self.xoroshiro128ss() & 0x7fff_ffff_ffff_ffff) as i64
    }

    pub fn i64_range(&mut self, rmin: i64, rmax: i64) -> i64 {
        if rmax > rmin {
            (rmin as u64).wrapping_add(self.u64_range(0, (rmax as u64).wrapping_sub(rmin as u64)))
                as i64
        } else {
            rmin
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
        ((self.xoroshiro128ss() >> (64 - 52)) as f64) * 2f64.powi(-52)
    }

    pub fn normal_f64(&mut self) -> f64 {
        let u1 = 1.0 - self.next_f64();
        let u2 = 1.0 - self.next_f64();
        (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).sin()
    }

    pub fn next_bool(&mut self) -> bool {
        self.xoroshiro128ss() >> 63 != 0
    }

    fn xoroshiro128ss(&mut self) -> u64 {
        let s0 = self.s0;
        let sx = self.s1 ^ s0;
        let val = s0.wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        self.s0 = s0.rotate_left(24) ^ sx ^ (sx << 16);
        self.s1 = sx.rotate_left(37);
        val
    }
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut val = *state;
    val = (val ^ (val >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    val = (val ^ (val >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    val ^ (val >> 31)
}

thread_local! {
    // pg_global_prng_state is backend-private (reseeded per backend in
    // InitProcessGlobals), so per-thread here, never a shared static.
    static GLOBAL_PRNG: Cell<PgPrng> = const { Cell::new(PgPrng::from_raw(0, 0)) };
}

pub fn global_prng<R>(f: impl FnOnce(&mut PgPrng) -> R) -> R {
    GLOBAL_PRNG.with(|cell| {
        let mut state = cell.get();
        let result = f(&mut state);
        cell.set(state);
        result
    })
}

pub fn init_seams() {
    pg_prng_seams::global_prng_uint32::set(|| global_prng(PgPrng::next_u32));
}

#[cfg(test)]
mod tests;
