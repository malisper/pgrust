// hyperloglog.c, bwidth-10 arm only (the sole width any C caller passes);
// other widths panic loudly rather than mis-size the inline register file.

const BWIDTH: u8 = 10;
const N_REGISTERS: usize = 1 << BWIDTH;
const POW_2_32: f64 = 4294967296.0;
const NEG_POW_2_32: f64 = -4294967296.0;

pub struct HyperLogLog {
    registers: [u8; N_REGISTERS],
}

const _: () = assert!(!core::mem::needs_drop::<HyperLogLog>());

// alphaMM for m=1024: (0.7213 / (1 + 1.079/m)) * m * m.
const ALPHA_MM: f64 = 0.7213 / (1.0 + 1.079 / N_REGISTERS as f64)
    * (N_REGISTERS as f64)
    * (N_REGISTERS as f64);

impl HyperLogLog {
    pub fn new(bwidth: u8) -> HyperLogLog {
        assert!(
            bwidth == BWIDTH,
            "initHyperLogLog (hyperloglog.c): bwidth {bwidth} arm not ported (only 10)"
        );
        HyperLogLog { registers: [0; N_REGISTERS] }
    }

    #[inline]
    pub fn add(&mut self, hash: u32) {
        let index = (hash >> (32 - BWIDTH)) as usize;
        let count = rho(hash << BWIDTH, 32 - BWIDTH);
        if count > self.registers[index] {
            self.registers[index] = count;
        }
    }

    pub fn estimate(&self) -> f64 {
        let mut sum = 0.0;
        for &r in &self.registers {
            sum += 1.0 / f64::powi(2.0, r as i32);
        }
        let result = ALPHA_MM / sum;

        if result <= 2.5 * N_REGISTERS as f64 {
            let zero_count = self.registers.iter().filter(|&&r| r == 0).count();
            if zero_count != 0 {
                return N_REGISTERS as f64 * f64::ln(N_REGISTERS as f64 / zero_count as f64);
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
}
