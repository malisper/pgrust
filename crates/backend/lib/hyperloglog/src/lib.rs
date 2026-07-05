// hyperloglog.c over an inline register file; N = 1 << bwidth. Live widths:
// 10 (abbrev-key abort logic) and 5 (nodeAgg HASHAGG_HLL_BIT_WIDTH).

const POW_2_32: f64 = 4294967296.0;
const NEG_POW_2_32: f64 = -4294967296.0;

pub struct Hll<const N: usize> {
    registers: [u8; N],
}

pub type HyperLogLog = Hll<1024>;
pub type HyperLogLog32 = Hll<32>;

const _: () = assert!(!core::mem::needs_drop::<HyperLogLog>());
const _: () = assert!(!core::mem::needs_drop::<HyperLogLog32>());

// initHyperLogLog's alpha table (m = number of registers).
const fn alpha_mm(m: usize) -> f64 {
    let alpha = match m {
        16 => 0.673,
        32 => 0.697,
        64 => 0.709,
        _ => 0.7213 / (1.0 + 1.079 / m as f64),
    };
    alpha * (m as f64) * (m as f64)
}

impl<const N: usize> Hll<N> {
    const BWIDTH: u8 = {
        assert!(N.is_power_of_two() && N >= 16);
        N.trailing_zeros() as u8
    };

    pub fn new(bwidth: u8) -> Hll<N> {
        assert!(
            bwidth == Self::BWIDTH,
            "initHyperLogLog (hyperloglog.c): bwidth mismatch with the register file"
        );
        Hll { registers: [0; N] }
    }

    #[inline]
    pub fn add(&mut self, hash: u32) {
        let index = (hash >> (32 - Self::BWIDTH)) as usize;
        let count = rho(hash << Self::BWIDTH, 32 - Self::BWIDTH);
        if count > self.registers[index] {
            self.registers[index] = count;
        }
    }

    pub fn estimate(&self) -> f64 {
        let mut sum = 0.0;
        for &r in &self.registers {
            sum += 1.0 / f64::powi(2.0, r as i32);
        }
        let result = alpha_mm(N) / sum;

        if result <= 2.5 * N as f64 {
            let zero_count = self.registers.iter().filter(|&&r| r == 0).count();
            if zero_count != 0 {
                return N as f64 * f64::ln(N as f64 / zero_count as f64);
            }
            result
        } else if result > (1.0 / 30.0) * POW_2_32 {
            NEG_POW_2_32 * f64::ln(1.0 - result / POW_2_32)
        } else {
            result
        }
    }
}

#[inline]
fn rho(x: u32, b: u8) -> u8 {
    if x == 0 {
        return b + 1;
    }
    let j = (x.leading_zeros() + 1) as u8;
    if j > b {
        b + 1
    } else {
        j
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rho_matches_c() {
        assert_eq!(rho(0x8000_0000, 22), 1);
        assert_eq!(rho(0x2000_0000, 22), 3);
        assert_eq!(rho(0, 22), 23);
        assert_eq!(rho(1, 22), 23);
    }

    #[test]
    fn estimate_tracks_cardinality() {
        let mut h = HyperLogLog::new(10);
        assert_eq!(h.estimate(), 0.0);
        let mut x: u64 = 0x9e3779b97f4a7c15;
        for _ in 0..50_000 {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            h.add((x >> 32) as u32);
        }
        let est = h.estimate();
        assert!((40_000.0..60_000.0).contains(&est), "est {est}");
    }

    #[test]
    fn low_cardinality_small_range() {
        let mut h = HyperLogLog::new(10);
        for i in 0..10u32 {
            h.add(i.wrapping_mul(0x9e37_79b9).rotate_left(15));
        }
        let est = h.estimate();
        assert!((5.0..20.0).contains(&est), "est {est}");
    }

    #[test]
    fn bwidth5_estimate_tracks_cardinality() {
        let mut h = HyperLogLog32::new(5);
        let mut x: u64 = 0x9e3779b97f4a7c15;
        for _ in 0..10_000 {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            h.add((x >> 32) as u32);
        }
        let est = h.estimate();
        assert!((6_000.0..16_000.0).contains(&est), "est {est}");
    }
}
