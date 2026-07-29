//! numeric.c — arbitrary-precision NUMERIC: representation, I/O, add/sub/mul/
//! div/cmp, typmod, int/float conversions, aggregate transition kernels.

pub mod aggregates;
pub mod arith;
pub mod builtins;
pub mod fixed;
pub mod io;
pub mod keypack;
pub mod math;
pub mod ops;
pub mod random;
pub mod series;
pub mod sortsupport;
pub mod var;

#[cfg(test)]
mod tests;

use types_error::{
    PgError, ERRCODE_DIVISION_BY_ZERO, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

pub use aggregates::{
    do_int128_accum, do_int128_discard, do_numeric_accum, do_numeric_discard, numeric_avg,
    numeric_poly_avg, numeric_poly_sum, numeric_sum, Int128AggState, NumericAggState,
    NumericSumAccum,
};
pub use arith::{
    add_var, cmp_var, cmp_var_common, div_var, div_var_int, div_var_int64, mul_var,
    select_div_scale, sub_var,
};
pub use fixed::{
    add_abs_fixed, add_var_fixed, mul_var_fixed, sub_abs_fixed, sub_var_fixed, FixedVar,
};
pub use io::{get_str_from_var, numeric_in, numeric_out_into, numeric_recv, numeric_send};
pub use keypack::{numeric_key_pack, numeric_key_unpack, NumericKeyForm, NUMERIC_KEY_EXP_MAX};
pub use math::{
    div_mod_var, exp_var, gcd_var, ln_var, log_var, mod_var, numeric_exp, numeric_exp_into,
    numeric_fac, numeric_gcd_common, numeric_lcm_common, numeric_ln, numeric_ln_into,
    numeric_log, numeric_mod_common, numeric_out_sci, numeric_power, numeric_power_into,
    numeric_sqrt, numeric_sqrt_into, power_var, sqrt_var, width_bucket_numeric,
};
pub use ops::*;
pub use var::{
    make_result, make_result_into, make_result_opt_error, var_to_uint64, NumericImage, NumericVar,
    VarView,
};

pub type NumericDigit = i16;

pub const NBASE: i32 = 10000;
pub const HALF_NBASE: i32 = 5000;
pub const DEC_DIGITS: i32 = 4;
pub const MUL_GUARD_DIGITS: i32 = 2;
pub const DIV_GUARD_DIGITS: i32 = 4;
pub const NBASE_SQR: i32 = NBASE * NBASE;

pub const NUMERIC_SIGN_MASK: u16 = 0xC000;
pub const NUMERIC_POS: u16 = 0x0000;
pub const NUMERIC_NEG: u16 = 0x4000;
pub const NUMERIC_SHORT: u16 = 0x8000;
pub const NUMERIC_SPECIAL: u16 = 0xC000;

pub const NUMERIC_EXT_SIGN_MASK: u16 = 0xF000;
pub const NUMERIC_NAN: u16 = 0xC000;
pub const NUMERIC_PINF: u16 = 0xD000;
pub const NUMERIC_NINF: u16 = 0xF000;
pub const NUMERIC_INF_SIGN_MASK: u16 = 0x2000;

pub const NUMERIC_SHORT_SIGN_MASK: u16 = 0x2000;
pub const NUMERIC_SHORT_DSCALE_MASK: u16 = 0x1F80;
pub const NUMERIC_SHORT_DSCALE_SHIFT: u16 = 7;
pub const NUMERIC_SHORT_DSCALE_MAX: i32 =
    (NUMERIC_SHORT_DSCALE_MASK >> NUMERIC_SHORT_DSCALE_SHIFT) as i32;
pub const NUMERIC_SHORT_WEIGHT_SIGN_MASK: u16 = 0x0040;
pub const NUMERIC_SHORT_WEIGHT_MASK: u16 = 0x003F;
pub const NUMERIC_SHORT_WEIGHT_MAX: i32 = NUMERIC_SHORT_WEIGHT_MASK as i32;
pub const NUMERIC_SHORT_WEIGHT_MIN: i32 = -(NUMERIC_SHORT_WEIGHT_MASK as i32 + 1);

pub const NUMERIC_DSCALE_MASK: u16 = 0x3FFF;
pub const NUMERIC_DSCALE_MAX: i32 = NUMERIC_DSCALE_MASK as i32;
pub const NUMERIC_WEIGHT_MAX: i32 = i16::MAX as i32;

pub const VARHDRSZ: usize = 4;
pub const NUMERIC_HDRSZ: usize = VARHDRSZ + 2 + 2;
pub const NUMERIC_HDRSZ_SHORT: usize = VARHDRSZ + 2;

pub const NUMERIC_MAX_PRECISION: i32 = 1000;
pub const NUMERIC_MIN_SCALE: i32 = -1000;
pub const NUMERIC_MAX_SCALE: i32 = 1000;
pub const NUMERIC_MAX_DISPLAY_SCALE: i32 = NUMERIC_MAX_PRECISION;
pub const NUMERIC_MIN_DISPLAY_SCALE: i32 = 0;
pub const NUMERIC_MAX_RESULT_SCALE: i32 = NUMERIC_MAX_PRECISION * 2;
pub const NUMERIC_MIN_SIG_DIGITS: i32 = 16;

#[inline]
pub fn numeric_can_be_short(dscale: i32, weight: i32) -> bool {
    dscale <= NUMERIC_SHORT_DSCALE_MAX
        && weight <= NUMERIC_SHORT_WEIGHT_MAX
        && weight >= NUMERIC_SHORT_WEIGHT_MIN
}

/// Borrowed view of a packed numeric: the varlena payload (no varlena
/// header), i.e. the u16 header word, optional i16 weight, then digits.
#[derive(Clone, Copy, Debug)]
pub struct Num<'a> {
    ptr: *const u8,
    len: usize,
    _life: core::marker::PhantomData<&'a [u8]>,
}

impl<'a> Num<'a> {
    #[inline]
    pub fn from_payload(bytes: &'a [u8]) -> Num<'a> {
        assert!(bytes.len() >= 2);
        Num {
            ptr: bytes.as_ptr(),
            len: bytes.len(),
            _life: core::marker::PhantomData,
        }
    }

    #[inline]
    pub fn header(self) -> u16 {
        // SAFETY: from_payload checked len >= 2.
        unsafe { self.ptr.cast::<u16>().read_unaligned() }
    }

    #[inline]
    pub fn is_short(self) -> bool {
        self.header() & NUMERIC_SIGN_MASK == NUMERIC_SHORT
    }

    #[inline]
    pub fn is_special(self) -> bool {
        self.header() & NUMERIC_SIGN_MASK == NUMERIC_SPECIAL
    }

    #[inline]
    pub fn is_nan(self) -> bool {
        self.header() == NUMERIC_NAN
    }

    #[inline]
    pub fn is_pinf(self) -> bool {
        self.header() == NUMERIC_PINF
    }

    #[inline]
    pub fn is_ninf(self) -> bool {
        self.header() == NUMERIC_NINF
    }

    #[inline]
    pub fn is_inf(self) -> bool {
        self.header() & !NUMERIC_INF_SIGN_MASK == NUMERIC_PINF
    }

    #[inline]
    fn header_is_short(self) -> bool {
        self.header() & 0x8000 != 0
    }

    // Payload-relative header size (C's NUMERIC_HEADER_SIZE minus VARHDRSZ).
    #[inline]
    fn header_size(self) -> usize {
        if self.header_is_short() {
            2
        } else {
            4
        }
    }

    #[inline]
    pub fn sign(self) -> u16 {
        let h = self.header();
        if h & NUMERIC_SIGN_MASK == NUMERIC_SHORT {
            if h & NUMERIC_SHORT_SIGN_MASK != 0 {
                NUMERIC_NEG
            } else {
                NUMERIC_POS
            }
        } else if h & NUMERIC_SIGN_MASK == NUMERIC_SPECIAL {
            h & NUMERIC_EXT_SIGN_MASK
        } else {
            h & NUMERIC_SIGN_MASK
        }
    }

    #[inline]
    pub fn dscale(self) -> i32 {
        let h = self.header();
        if self.header_is_short() {
            ((h & NUMERIC_SHORT_DSCALE_MASK) >> NUMERIC_SHORT_DSCALE_SHIFT) as i32
        } else {
            (h & NUMERIC_DSCALE_MASK) as i32
        }
    }

    #[inline]
    pub fn weight(self) -> i32 {
        let h = self.header();
        if self.header_is_short() {
            let w = (h & NUMERIC_SHORT_WEIGHT_MASK) as i32;
            if h & NUMERIC_SHORT_WEIGHT_SIGN_MASK != 0 {
                w | !(NUMERIC_SHORT_WEIGHT_MASK as i32)
            } else {
                w
            }
        } else {
            debug_assert!(self.len >= 4);
            // SAFETY: a long-header numeric payload is at least 4 bytes.
            unsafe { self.ptr.add(2).cast::<i16>().read_unaligned() as i32 }
        }
    }

    #[inline]
    pub fn ndigits(self) -> i32 {
        ((self.len - self.header_size()) / 2) as i32
    }

    #[inline]
    pub fn digits(self) -> &'a [NumericDigit] {
        let off = self.header_size();
        let n = (self.len.saturating_sub(off)) / 2;
        // SAFETY: from_payload's length check + header_size <= 4 keep the
        // range in bounds; alignment guarded below.
        let p = unsafe { self.ptr.add(off) };
        if p as usize % 2 != 0 {
            unaligned_digits_panic();
        }
        // SAFETY: alignment checked above; n i16s lie within the payload.
        unsafe { core::slice::from_raw_parts(p.cast::<NumericDigit>(), n) }
    }

    #[inline]
    pub fn view(self) -> VarView<'a> {
        debug_assert!(!self.is_special());
        VarView {
            ndigits: self.ndigits(),
            weight: self.weight(),
            sign: self.sign(),
            dscale: self.dscale(),
            digits: self.digits(),
        }
    }

    #[inline]
    pub fn as_bytes(self) -> &'a [u8] {
        // SAFETY: constructed from a live &'a [u8] of this length.
        unsafe { core::slice::from_raw_parts(self.ptr, self.len) }
    }
}

#[cold]
#[inline(never)]
fn unaligned_digits_panic() -> ! {
    panic!("adt_numeric: packed numeric image is not 2-byte aligned")
}

#[cold]
#[inline(never)]
pub fn numeric_overflow_error() -> PgError {
    PgError::error("value overflows numeric format")
        .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

#[cold]
#[inline(never)]
pub fn division_by_zero_error() -> PgError {
    PgError::error("division by zero").with_sqlstate(ERRCODE_DIVISION_BY_ZERO)
}

#[cold]
#[inline(never)]
pub fn invalid_numeric_syntax(input: &str) -> PgError {
    PgError::error(format!("invalid input syntax for type numeric: \"{input}\""))
        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}
