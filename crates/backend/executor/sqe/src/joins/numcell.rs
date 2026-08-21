//! [corrnumcell] Exact numeric cells for the grouped build: i128
//! sum/min/max mantissas,
//! the ported select_div_scale/round avg law, and a 384-bit cross-scale
//! comparison where constants stay multiplicands (direction never flips).

use super::ir::JoinCmp;
use std::cmp::Ordering;

const LIMBS: usize = 6;

/// Unsigned 384-bit magnitude; `sat` = overflowed 384 bits.
#[derive(Clone, Copy)]
struct Mag {
    w: [u64; LIMBS],
    sat: bool,
}

impl Mag {
    fn from_u128(v: u128) -> Mag {
        let mut w = [0u64; LIMBS];
        w[0] = v as u64;
        w[1] = (v >> 64) as u64;
        Mag { w, sat: false }
    }

    fn mul_u64(mut self, m: u64) -> Mag {
        let mut carry: u128 = 0;
        for i in 0..LIMBS {
            let t = self.w[i] as u128 * m as u128 + carry;
            self.w[i] = t as u64;
            carry = t >> 64;
        }
        self.sat |= carry != 0;
        self
    }

    fn shl64(mut self) -> Mag {
        self.sat |= self.w[LIMBS - 1] != 0;
        for i in (1..LIMBS).rev() {
            self.w[i] = self.w[i - 1];
        }
        self.w[0] = 0;
        self
    }

    fn add(mut self, o: Mag) -> Mag {
        let mut carry: u128 = 0;
        for i in 0..LIMBS {
            let t = self.w[i] as u128 + o.w[i] as u128 + carry;
            self.w[i] = t as u64;
            carry = t >> 64;
        }
        self.sat |= o.sat || carry != 0;
        self
    }

    fn mul_u128(self, v: u128) -> Mag {
        let hi = (v >> 64) as u64;
        let lo = self.mul_u64(v as u64);
        if hi == 0 {
            return lo;
        }
        lo.add(self.shl64().mul_u64(hi))
    }

    fn mul_pow10(mut self, mut e: u32) -> Mag {
        while e >= 19 {
            self = self.mul_u64(10_000_000_000_000_000_000);
            e -= 19;
        }
        if e > 0 {
            self = self.mul_u64(10u64.pow(e));
        }
        self
    }

    fn cmp_mag(&self, o: &Mag) -> Ordering {
        match (self.sat, o.sat) {
            (true, true) => {
                debug_assert!(false, "two-sided saturation is admission-excluded");
                Ordering::Equal
            }
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => {
                for i in (0..LIMBS).rev() {
                    match self.w[i].cmp(&o.w[i]) {
                        Ordering::Equal => {}
                        ord => return ord,
                    }
                }
                Ordering::Equal
            }
        }
    }
}

fn sign2(a: i128, b: i128) -> i32 {
    if a == 0 || b == 0 {
        0
    } else if (a < 0) == (b < 0) {
        1
    } else {
        -1
    }
}

/// Exact ordering of `a*b*10^ea` vs `c*d*10^ec`.
pub fn cmp_scaled(a: i128, b: i128, ea: u32, c: i128, d: i128, ec: u32) -> Ordering {
    let (sl, sr) = (sign2(a, b), sign2(c, d));
    if sl != sr {
        return sl.cmp(&sr);
    }
    if sl == 0 {
        return Ordering::Equal;
    }
    let ml = Mag::from_u128(a.unsigned_abs()).mul_u128(b.unsigned_abs()).mul_pow10(ea);
    let mr = Mag::from_u128(c.unsigned_abs()).mul_u128(d.unsigned_abs()).mul_pow10(ec);
    let ord = ml.cmp_mag(&mr);
    if sl < 0 { ord.reverse() } else { ord }
}

fn digits10(mut v: u128) -> i32 {
    let mut d = 1;
    while v >= 10 {
        v /= 10;
        d += 1;
    }
    d
}

/// Normalized base-10000 (weight, first digit group) of `mant/10^scale`.
fn wfd(mant: u128, scale: i32) -> (i32, u64) {
    let dd = digits10(mant);
    let p = dd - scale;
    let w = (p - 1).div_euclid(4);
    let shift = scale + 4 * w;
    let fd = if shift >= 0 { mant / 10u128.pow(shift as u32) } else { mant * 10u128.pow((-shift) as u32) };
    (w, fd as u64)
}

/// Ported select_div_scale (16 sig digits, 4-digit groups, clamp [0, 1000]).
fn div_rscale(sum_abs: u128, sum_scale: i32, n: u64) -> i32 {
    let (w1, fd1) = if sum_abs == 0 { (0, 0) } else { wfd(sum_abs, sum_scale) };
    let (w2, fd2) = wfd(n as u128, 0);
    let mut qweight = w1 - w2;
    if fd1 <= fd2 {
        qweight -= 1;
    }
    (16 - qweight * 4).max(sum_scale).max(0).min(1000)
}

/// The ported avg finalize: div_var at select_div_scale, correctly
/// rounded half-away-from-zero -> (avg mantissa, rscale). Precondition
/// |sum| < 2^96, so every intermediate fits u128.
pub fn pg_avg_cell(sum: i128, n: u32, scale: i32) -> (i128, i32) {
    debug_assert!(n > 0 && sum.unsigned_abs() < (1u128 << 96));
    let s = sum.unsigned_abs();
    let r = div_rscale(s, scale, n as u64);
    let nn = n as u128;
    let mut q = s / nn;
    let mut rem = s % nn;
    let mut e = (r - scale) as u32 + 1;
    while e > 0 {
        let step = e.min(18);
        let m = 10u128.pow(step);
        q = q * m + rem * m / nn;
        rem = rem * m % nn;
        e -= step;
    }
    let a = ((q + 5) / 10) as i128;
    (if sum < 0 { -a } else { a }, r)
}

/// `o <op> k * cell` at scales (os, ks, cs) is exactly
/// `o_m*10^(ks+cs) <op> k_m*cell_m*10^os`.
#[allow(clippy::too_many_arguments)]
pub fn num_cell_pass(
    op: JoinCmp,
    o_m: i64,
    o_scale: i32,
    k_m: i64,
    k_scale: i32,
    cell_m: i128,
    cell_scale: i32,
) -> bool {
    let ord = cmp_scaled(
        o_m as i128,
        1,
        (k_scale + cell_scale) as u32,
        k_m as i128,
        cell_m,
        o_scale as u32,
    );
    match op {
        JoinCmp::Lt => ord == Ordering::Less,
        JoinCmp::Le => ord != Ordering::Greater,
        JoinCmp::Gt => ord == Ordering::Greater,
        JoinCmp::Ge => ord != Ordering::Less,
        JoinCmp::Eq => ord == Ordering::Equal,
        JoinCmp::Ne => ord != Ordering::Equal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn avg_rscale_law() {
        assert_eq!(pg_avg_cell(3, 2, 0), (15_000_000_000_000_000, 16));
        assert_eq!(pg_avg_cell(201, 2, 2), (100_500_000_000_000_000_000, 20));
        assert_eq!(pg_avg_cell(2_000_000_000_000_000_000, 2, 2), (100_000_000_000_000_000_000, 4));
        assert_eq!(pg_avg_cell(-3, 2, 0).0, -15_000_000_000_000_000);
        let (a, r) = pg_avg_cell(1, 8, 0);
        assert_eq!(r, 20);
        assert_eq!(a, 12_500_000_000_000_000_000);
    }

    #[test]
    fn wfd_alignment() {
        assert_eq!(wfd(12345, 2), (0, 123));
        assert_eq!(wfd(5, 3), (-1, 50));
        assert_eq!(wfd(10000, 0), (1, 1));
        assert_eq!(wfd(9999, 0), (0, 9999));
    }

    #[test]
    fn cmp_scaled_law() {
        use Ordering::*;
        assert_eq!(cmp_scaled(2498, 1, 3, 2, 12500, 2), Less);
        assert_eq!(cmp_scaled(2500, 1, 3, 2, 12500, 2), Equal);
        assert_eq!(cmp_scaled(2502, 1, 3, 2, 12500, 2), Greater);
        assert_eq!(cmp_scaled(-2500, 1, 3, -2, 12500, 2), Equal);
        assert_eq!(cmp_scaled(0, 1, 30, 0, 12345, 0), Equal);
        assert_eq!(cmp_scaled(i64::MAX as i128, 1, 90, 1, 1, 0), Greater);
        assert_eq!(cmp_scaled(1, 1, 0, i64::MAX as i128, i64::MAX as i128, 30), Less);
    }

    /// `o <op> k*cell` at (os, ks, cs) — the multiplicand law as the
    /// hash_join call sites use it (probe value, probe_scale, k_m,
    /// k_scale, cell mantissa, cell scale). Rows 1-4 pin the boundary
    /// 24.98/25.00/25.02-style around k*cell = 0.2 * 125.00 = 25.00
    /// (cell mantissa 12500 at scale 2 — the same operands as
    /// `cmp_scaled_law`; the original pin carried a 2500 typo that
    /// asserted 24.98 < 5, latent until full units ran on CI-ci).
    /// Rows 5-6 pin the Gt boundary at k*cell = 0.5 * 25.00 = 12.5.
    #[test]
    fn pass_law() {
        assert!(num_cell_pass(JoinCmp::Lt, 2498, 2, 2, 1, 12500, 2));
        assert!(!num_cell_pass(JoinCmp::Lt, 2500, 2, 2, 1, 12500, 2));
        assert!(num_cell_pass(JoinCmp::Le, 2500, 2, 2, 1, 12500, 2));
        assert!(num_cell_pass(JoinCmp::Eq, 2500, 2, 2, 1, 12500, 2));
        assert!(num_cell_pass(JoinCmp::Gt, 13, 0, 5, 1, 2500, 2));
        assert!(!num_cell_pass(JoinCmp::Gt, 12, 0, 5, 1, 2500, 2));
    }
}
