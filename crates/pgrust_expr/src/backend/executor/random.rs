use std::sync::Arc;

use num_bigint::BigInt;
use num_traits::{Signed, Zero};
use parking_lot::Mutex;
use pgrust_nodes::datum::NumericValue;
use rand::RngCore;

const KNUTH_LCG_0: u64 = 0x5851_F42D_4C95_7F2D;
const KNUTH_LCG_1: u64 = 0x1405_7B7E_F767_814F;
const NBASE: u16 = 10_000;
const DEC_DIGITS: i32 = 4;

#[derive(Debug, Clone, Default)]
pub struct PgPrngState {
    s0: u64,
    s1: u64,
    seed_set: bool,
}

impl PgPrngState {
    pub fn shared() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self::default()))
    }

    pub fn fseed(&mut self, seed: f64) {
        let scaled = (((1u64 << 52) - 1) as f64 * seed) as i64;
        self.seed(scaled as u64);
        self.seed_set = true;
    }

    pub fn double(&mut self) -> f64 {
        self.ensure_seeded();
        let value = self.next_u64() >> (64 - 52);
        (value as f64) * 2_f64.powi(-52)
    }

    pub fn double_normal(&mut self) -> f64 {
        let u1 = 1.0 - self.double();
        let u2 = 1.0 - self.double();
        (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).sin()
    }

    pub fn int64_range(&mut self, min: i64, max: i64) -> i64 {
        self.ensure_seeded();
        if max <= min {
            return min;
        }
        let offset = self.uint64_range(0, (max as u64).wrapping_sub(min as u64));
        (min as u64).wrapping_add(offset) as i64
    }

    pub fn numeric_range(&mut self, min: &NumericValue, max: &NumericValue) -> NumericValue {
        self.ensure_seeded();
        random_numeric(self, min, max)
    }

    fn ensure_seeded(&mut self) {
        if self.seed_set {
            return;
        }
        let mut bytes = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut bytes);
        self.s0 = u64::from_le_bytes(bytes[..8].try_into().expect("first seed word"));
        self.s1 = u64::from_le_bytes(bytes[8..].try_into().expect("second seed word"));
        self.seed_check();
        self.seed_set = true;
    }

    fn seed(&mut self, mut seed: u64) {
        self.s0 = splitmix64(&mut seed);
        self.s1 = splitmix64(&mut seed);
        self.seed_check();
    }

    fn seed_check(&mut self) {
        if self.s0 == 0 && self.s1 == 0 {
            self.s0 = KNUTH_LCG_0;
            self.s1 = KNUTH_LCG_1;
        }
    }

    fn next_u64(&mut self) -> u64 {
        let s0 = self.s0;
        let sx = self.s1 ^ s0;
        let value = s0.wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        self.s0 = s0.rotate_left(24) ^ sx ^ sx.wrapping_shl(16);
        self.s1 = sx.rotate_left(37);
        value
    }

    fn uint64_range(&mut self, min: u64, max: u64) -> u64 {
        if max <= min {
            return min;
        }
        let range = max - min;
        let rshift = range.leading_zeros();
        let value = loop {
            let candidate = self.next_u64() >> rshift;
            if candidate <= range {
                break candidate;
            }
        };
        min + value
    }
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

#[derive(Debug, Clone)]
struct NumericVar {
    digits: Vec<u16>,
    weight: i32,
    dscale: u32,
}

fn random_numeric(state: &mut PgPrngState, min: &NumericValue, max: &NumericValue) -> NumericValue {
    let rscale = min.dscale().max(max.dscale());
    let rlen = max.sub(min);
    let rlen_var = numeric_to_var_abs(&rlen).with_dscale(rscale);
    if rlen_var.digits.is_empty() {
        return min.clone().with_dscale(rscale);
    }

    let res_ndigits =
        (rlen_var.weight + 1 + ((rscale as i32 + DEC_DIGITS - 1) / DEC_DIGITS)).max(0) as usize;
    let pow10 = 10_u64
        .pow((((rscale as i32 + DEC_DIGITS - 1) / DEC_DIGITS) * DEC_DIGITS - rscale as i32) as u32);
    let mut rlen64 = u64::from(rlen_var.digits[0]);
    let mut rlen64_ndigits = 1usize;
    while rlen64_ndigits < res_ndigits && rlen64_ndigits < 4 {
        rlen64 *= u64::from(NBASE);
        if rlen64_ndigits < rlen_var.digits.len() {
            rlen64 += u64::from(rlen_var.digits[rlen64_ndigits]);
        }
        rlen64_ndigits += 1;
    }

    loop {
        let mut result = NumericVar {
            digits: vec![0; res_ndigits],
            weight: rlen_var.weight,
            dscale: rscale,
        };
        let first_random = if rlen64_ndigits == res_ndigits && pow10 != 1 {
            state.uint64_range(0, rlen64 / pow10) * pow10
        } else {
            state.uint64_range(0, rlen64)
        };
        let mut rand = first_random;
        for index in (0..rlen64_ndigits).rev() {
            result.digits[index] = (rand % u64::from(NBASE)) as u16;
            rand /= u64::from(NBASE);
        }

        let mut whole_ndigits = res_ndigits;
        if pow10 != 1 {
            whole_ndigits = whole_ndigits.saturating_sub(1);
        }
        let mut index = rlen64_ndigits;
        while index + 3 < whole_ndigits {
            rand = state.uint64_range(0, u64::from(NBASE).pow(4) - 1);
            for _ in 0..4 {
                result.digits[index] = (rand % u64::from(NBASE)) as u16;
                rand /= u64::from(NBASE);
                index += 1;
            }
        }
        while index < whole_ndigits {
            result.digits[index] = state.uint64_range(0, u64::from(NBASE) - 1) as u16;
            index += 1;
        }
        if index < res_ndigits {
            result.digits[index] =
                (state.uint64_range(0, u64::from(NBASE) / pow10 - 1) * pow10) as u16;
        }
        result.strip();
        if var_to_numeric(&result).cmp(&rlen) != std::cmp::Ordering::Greater {
            return var_to_numeric(&result)
                .add(min)
                .round_to_scale(rscale)
                .unwrap_or_else(NumericValue::zero)
                .with_dscale(rscale)
                .normalize();
        }
    }
}

fn numeric_to_var_abs(value: &NumericValue) -> NumericVar {
    let NumericValue::Finite { coeff, scale, .. } = value else {
        return NumericVar {
            digits: Vec::new(),
            weight: 0,
            dscale: 0,
        };
    };
    if coeff.is_zero() {
        return NumericVar {
            digits: Vec::new(),
            weight: 0,
            dscale: 0,
        };
    }
    let mut digits = coeff.abs().to_str_radix(10);
    if *scale as usize >= digits.len() {
        digits = format!(
            "{}{}",
            "0".repeat(*scale as usize + 1 - digits.len()),
            digits
        );
    }
    let split = digits.len() - *scale as usize;
    let mut int_digits = digits[..split].to_string();
    let mut frac_digits = digits[split..].to_string();
    if int_digits.is_empty() {
        int_digits.push('0');
    }
    let int_pad =
        (DEC_DIGITS as usize - int_digits.len() % DEC_DIGITS as usize) % DEC_DIGITS as usize;
    let frac_pad =
        (DEC_DIGITS as usize - frac_digits.len() % DEC_DIGITS as usize) % DEC_DIGITS as usize;
    int_digits = format!("{}{}", "0".repeat(int_pad), int_digits);
    frac_digits.push_str(&"0".repeat(frac_pad));

    let mut groups = Vec::new();
    for chunk in int_digits.as_bytes().chunks(DEC_DIGITS as usize) {
        groups.push(std::str::from_utf8(chunk).unwrap().parse::<u16>().unwrap());
    }
    for chunk in frac_digits.as_bytes().chunks(DEC_DIGITS as usize) {
        groups.push(std::str::from_utf8(chunk).unwrap().parse::<u16>().unwrap());
    }
    let mut var = NumericVar {
        weight: int_digits.len() as i32 / DEC_DIGITS - 1,
        digits: groups,
        dscale: *scale,
    };
    var.strip();
    var
}

fn var_to_numeric(var: &NumericVar) -> NumericValue {
    if var.digits.is_empty() {
        return NumericValue::zero().with_dscale(var.dscale);
    }
    let min_power = var.weight - (var.digits.len() as i32 - 1);
    let mut coeff = BigInt::from(0u8);
    for digit in &var.digits {
        coeff *= u32::from(NBASE);
        coeff += u32::from(*digit);
    }
    if min_power >= 0 {
        coeff *= BigInt::from(u32::from(NBASE)).pow(min_power as u32);
        NumericValue::finite(coeff, 0).with_dscale(var.dscale)
    } else {
        NumericValue::finite(coeff, (-min_power * DEC_DIGITS) as u32).with_dscale(var.dscale)
    }
}

impl NumericVar {
    fn with_dscale(mut self, dscale: u32) -> Self {
        self.dscale = dscale;
        self
    }

    fn strip(&mut self) {
        while self.digits.first().copied() == Some(0) {
            self.digits.remove(0);
            self.weight -= 1;
        }
        while self.digits.last().copied() == Some(0) {
            self.digits.pop();
        }
        if self.digits.is_empty() {
            self.weight = 0;
        }
    }
}
