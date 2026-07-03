use pg_prng::PgPrng;
use types_core::BlockNumber;

pub struct BlockSamplerData {
    pub n_total: BlockNumber,
    pub n_sample: u32,
    pub t: BlockNumber,
    pub m: BlockNumber,
    pub randstate: PgPrng,
}

pub fn block_sampler_init(
    nblocks: BlockNumber,
    samplesize: u32,
    randseed: u32,
) -> (BlockSamplerData, BlockNumber) {
    let bs = BlockSamplerData {
        n_total: nblocks,
        n_sample: samplesize,
        t: 0,
        m: 0,
        randstate: PgPrng::seeded(randseed as u64),
    };
    let nsel = (bs.n_sample as BlockNumber).min(bs.n_total);
    (bs, nsel)
}

impl BlockSamplerData {
    pub fn has_more(&self) -> bool {
        self.t < self.n_total && self.m < self.n_sample
    }

    pub fn next(&mut self) -> BlockNumber {
        let k_remaining = self.n_total - self.t;
        let k_needed = self.n_sample - self.m;
        debug_assert!(self.has_more());

        if k_needed as BlockNumber >= k_remaining {
            self.m += 1;
            let b = self.t;
            self.t += 1;
            return b;
        }

        let mut k_rem = k_remaining;
        let v = sampler_random_fract(&mut self.randstate);
        let mut p = 1.0 - k_needed as f64 / k_rem as f64;
        while v < p {
            self.t += 1;
            k_rem -= 1;
            p *= 1.0 - k_needed as f64 / k_rem as f64;
        }

        self.m += 1;
        let b = self.t;
        self.t += 1;
        b
    }
}

pub struct ReservoirStateData {
    pub w: f64,
    pub randstate: PgPrng,
}

pub fn reservoir_init_selection_state(seed: u64, n: u32) -> ReservoirStateData {
    let mut rs = ReservoirStateData { w: 0.0, randstate: PgPrng::seeded(seed) };
    rs.w = (-sampler_random_fract(&mut rs.randstate).ln() / n as f64).exp();
    rs
}

pub fn reservoir_get_next_s(rs: &mut ReservoirStateData, t: f64, n: u32) -> f64 {
    let n = n as f64;
    let mut s;
    // Vitter's T threshold: Algorithm X below, Algorithm Z above.
    if t <= 22.0 * n {
        let v = sampler_random_fract(&mut rs.randstate);
        s = 0.0;
        let mut t = t + 1.0;
        let mut quot = (t - n) / t;
        while quot > v {
            s += 1.0;
            t += 1.0;
            quot *= (t - n) / t;
        }
    } else {
        let mut w = rs.w;
        let term = t - n + 1.0;
        loop {
            let u = sampler_random_fract(&mut rs.randstate);
            let x = t * (w - 1.0);
            s = x.floor();
            let tmp = (t + 1.0) / term;
            let lhs = ((((u * tmp * tmp) * (term + s)) / (t + x)).ln() / n).exp();
            let rhs = (((t + x) / (term + s)) * term) / t;
            if lhs <= rhs {
                w = rhs / lhs;
                break;
            }
            let mut y = (((u * (t + 1.0)) / term) * (t + s + 1.0)) / (t + x);
            let (mut denom, numer_lim) = if n < s {
                (t, term + s)
            } else {
                (t - n + s, t + 1.0)
            };
            let mut numer = t + s;
            while numer >= numer_lim {
                y *= numer / denom;
                denom -= 1.0;
                numer -= 1.0;
            }
            w = (-sampler_random_fract(&mut rs.randstate).ln() / n).exp();
            if (y.ln() / n).exp() <= (t + x) / t {
                break;
            }
        }
        rs.w = w;
    }
    s
}

pub fn sampler_random_fract(randstate: &mut PgPrng) -> f64 {
    loop {
        let res = randstate.next_f64();
        if res != 0.0 {
            return res;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_sampler_small_table_selects_all() {
        let (mut bs, nsel) = block_sampler_init(3, 100, 42);
        assert_eq!(nsel, 3);
        let mut got = alloc_vec();
        while bs.has_more() {
            got.push(bs.next());
        }
        assert_eq!(got, [0, 1, 2]);
    }

    #[test]
    fn block_sampler_sample_is_sorted_unique_and_sized() {
        let (mut bs, nsel) = block_sampler_init(10_000, 300, 7);
        assert_eq!(nsel, 300);
        let mut got = alloc_vec();
        while bs.has_more() {
            got.push(bs.next());
        }
        assert_eq!(got.len(), 300);
        assert!(got.windows(2).all(|w| w[0] < w[1]));
        assert!(*got.last().unwrap() < 10_000);
    }

    #[test]
    fn reservoir_skip_counts_nonnegative() {
        let mut rs = reservoir_init_selection_state(99, 100);
        let mut t = 100.0f64;
        for _ in 0..5_000 {
            let s = reservoir_get_next_s(&mut rs, t, 100);
            assert!(s >= 0.0 && s.fract() == 0.0);
            t += s + 1.0;
        }
        assert!(t > 5_000.0);
    }

    fn alloc_vec() -> std::vec::Vec<BlockNumber> {
        std::vec::Vec::new()
    }
}
