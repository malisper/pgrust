//! Family: conversions between [`NumericVar`]`<'mcx>` and the on-disk byte
//! image / `NumericData` struct / native integers / floats.
//!
//! Mirrors numeric.c's `init_var_from_num`/`set_var_from_num`/`make_result`/
//! `make_result_opt_error`/`apply_typmod`/`apply_typmod_special` (disk codec),
//! the `numericvar_to_int32`/`uint64`/`int128` + `int{2,4,8}_to_numeric`
//! family, and the `float{4,8}<->numeric` family.
//!
//! The on-disk value is an owned byte image (`PgVec<'mcx, u8>` — a charged
//! varlena buffer); the read side takes `&[u8]`. Conversions that allocate take
//! an explicit `Mcx<'mcx>` and return [`PgResult`] where the C `ereport`s
//! (overflow / invalid typmod / OOM).

use alloc::string::String;
use core::mem::size_of;

use mcx::{Mcx, PgVec};
use types_datum::VARHDRSZ;
use types_error::{PgError, PgResult, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE};
use types_numeric::var::{NumericSign, NumericVar};
use types_numeric::{
    is_valid_numeric_typmod, numeric_digit_at, numeric_digits, numeric_dscale,
    numeric_header_is_short, numeric_is_special, numeric_ndigits, numeric_sign,
    numeric_typmod_precision, numeric_typmod_scale, numeric_weight, NumericChoice, NumericData,
    NumericDigit, NumericLong, NumericShort, DEC_DIGITS, NBASE, NUMERIC_DSCALE_MASK, NUMERIC_HDRSZ,
    NUMERIC_HDRSZ_SHORT, NUMERIC_SHORT, NUMERIC_SHORT_DSCALE_MAX, NUMERIC_SHORT_DSCALE_SHIFT,
    NUMERIC_SHORT_SIGN_MASK, NUMERIC_SHORT_WEIGHT_MASK, NUMERIC_SHORT_WEIGHT_MAX,
    NUMERIC_SHORT_WEIGHT_MIN, NUMERIC_SHORT_WEIGHT_SIGN_MASK, NUMERIC_SIGN_MASK, NUMERIC_SPECIAL,
};

use crate::{io, kernel_transcendental, kernel_var};

extern crate alloc;

// ---------------------------------------------------------------------------
// Low-level varlena helpers.
// ---------------------------------------------------------------------------

/// The raw 4-byte varlena header word for an uncompressed varlena of byte
/// length `len`, computed the same endian-aware way as `SET_VARSIZE_4B`.
///
/// The two top header bits encode the varlena tag (00 = uncompressed 4-byte).
/// On little-endian those tag bits live in the LOW two bits, so C stores
/// `(uint32) len << 2`. On big-endian (WORDS_BIGENDIAN) the tag bits live in
/// the HIGH two bits, so C stores `len & 0x3FFFFFFF` (no shift).
#[inline]
fn varsize_header(len: usize) -> u32 {
    if cfg!(target_endian = "big") {
        (len as u32) & 0x3FFF_FFFF
    } else {
        (len as u32) << 2
    }
}

/// `value overflows numeric format` (ERRCODE 22003) — the C overflow ereport.
#[inline]
fn value_overflow() -> PgError {
    PgError::error("value overflows numeric format").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

/// OOM-safe zeroed `PgVec<'mcx, u8>` of length `len` for the on-disk varlena
/// image, surfacing OOM as `value overflows numeric format` (project HARD RULE:
/// validated bound + fallible reserve).
#[inline]
fn varlena_buf<'mcx>(mcx: Mcx<'mcx>, len: usize) -> PgResult<PgVec<'mcx, u8>> {
    let mut v = mcx::vec_with_capacity_in::<u8>(mcx, len).map_err(|_| value_overflow())?;
    // Capacity already reserved -> no realloc -> infallible.
    v.resize(len, 0);
    Ok(v)
}

/// `SET_VARSIZE(p, len)`: write the 4-byte varlena length header.
#[inline]
fn set_varsize(bytes: &mut [u8], len: usize) {
    bytes[..VARHDRSZ].copy_from_slice(&varsize_header(len).to_ne_bytes());
}

/// Write `digits` as native-endian `NumericDigit` (i16) pairs into `dst`.
#[inline]
fn write_digits(dst: &mut [u8], digits: &[NumericDigit]) {
    for (i, &d) in digits.iter().enumerate() {
        let off = i * size_of::<NumericDigit>();
        dst[off..off + 2].copy_from_slice(&d.to_ne_bytes());
    }
}

/// `NUMERIC_CAN_BE_SHORT(scale, weight)` (numeric.c:500).
#[inline]
pub(crate) fn numeric_can_be_short(scale: i32, weight: i32) -> bool {
    scale <= NUMERIC_SHORT_DSCALE_MAX as i32
        && (NUMERIC_SHORT_WEIGHT_MIN..=NUMERIC_SHORT_WEIGHT_MAX).contains(&weight)
}

// ---------------------------------------------------------------------------
// Disk codec: NumericVar <-> on-disk byte image.
// ---------------------------------------------------------------------------

/// `init_var_from_num(num, dest)` / `set_var_from_num(num, dest)`: decode an
/// on-disk `numeric` byte image into a fresh `NumericVar` in `mcx`.
///
/// In C, `init_var_from_num` borrows the on-disk digit array (no copy) while
/// `set_var_from_num` copies it. Here the digits must be owned, so the two
/// collapse into a single decoder; mutating the result never affects the
/// original bytes. Special values (NaN/Inf) decode to a special var (no
/// digits). (numeric.c:7539/7570)
pub fn set_var_from_num<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<NumericVar<'mcx>> {
    let sign_word = numeric_sign(num);
    let sign = NumericSign::from_numeric_word(sign_word)
        .ok_or_else(|| PgError::error("on-disk numeric carries an invalid sign word"))?;

    if sign.is_special() {
        return Ok(NumericVar::special(mcx, sign));
    }

    let ndigits = numeric_ndigits(num, num.len());
    let digit_bytes = numeric_digits(num);
    // C's set_var_from_num calls alloc_var(var, ndigits), which reserves one
    // leading spare digit (buf[0]; digits = buf + 1) so round_var can carry out
    // into a new leading digit in place. Reserve that spare (headroom = 1) and
    // write the logical digits starting at index 1.
    let mut digits = crate::alloc_digits(mcx, ndigits + 1)?;
    for i in 0..ndigits {
        digits[i + 1] = numeric_digit_at(digit_bytes, i);
    }

    Ok(NumericVar {
        sign,
        weight: numeric_weight(num),
        dscale: numeric_dscale(num) as i32,
        digits,
        headroom: 1,
    })
}

/// `make_result(var)`: encode a finite/special `NumericVar` into a fresh
/// on-disk byte image (charged varlena buffer); errors on overflow. An
/// interface to [`make_result_opt_error`] without the `have_error` argument.
/// (numeric.c:8010)
pub fn make_result<'mcx>(mcx: Mcx<'mcx>, var: &NumericVar<'_>) -> PgResult<PgVec<'mcx, u8>> {
    // C: make_result(var) == make_result_opt_error(var, NULL); the NULL
    // `have_error` path raises "value overflows numeric format" on int16
    // field overflow (numeric.c:7987).
    make_result_opt_error(mcx, var)?.ok_or_else(|| {
        PgError::error("value overflows numeric format")
            .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
    })
}

/// `make_result_opt_error(var, &have_error)`: build the packed on-disk
/// `Numeric` varlena byte image for `var`. Chooses the SHORT layout iff
/// `NUMERIC_CAN_BE_SHORT(dscale, weight)` after stripping leading/trailing zero
/// digits (else LONG). Special values write the 2-byte special header.
///
/// On int16 field overflow this signals via `Ok(None)` (the C `*have_error =
/// true` path) instead of erroring. (numeric.c:7901)
pub fn make_result_opt_error<'mcx>(
    mcx: Mcx<'mcx>,
    var: &NumericVar<'_>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // Special value: a single 2-byte header (NUMERIC_HDRSZ_SHORT total).
    if var.is_special() {
        // Verify valid special value (C: never write nonzero reserved bits).
        debug_assert!(matches!(
            var.sign,
            NumericSign::NaN | NumericSign::PInf | NumericSign::NInf
        ));
        let sign_word = var.sign.to_numeric_word();
        let len = NUMERIC_HDRSZ_SHORT;
        let mut buf = varlena_buf(mcx, len)?;
        set_varsize(&mut buf, len);
        buf[VARHDRSZ..VARHDRSZ + 2].copy_from_slice(&sign_word.to_ne_bytes());
        return Ok(Some(buf));
    }

    let digits = var.logical_digits();
    let mut start = 0usize;
    let mut n = digits.len();
    let mut weight = var.weight;
    let mut sign = var.sign;

    // Truncate leading zeroes.
    while n > 0 && digits[start] == 0 {
        start += 1;
        weight -= 1;
        n -= 1;
    }
    // Truncate trailing zeroes.
    while n > 0 && digits[start + n - 1] == 0 {
        n -= 1;
    }

    // If zero result, force weight=0 and positive sign.
    if n == 0 {
        weight = 0;
        sign = NumericSign::Pos;
    }

    let dscale = var.dscale;
    let sign_word = sign.to_numeric_word();

    // Build the result.
    if numeric_can_be_short(dscale, weight) {
        let len = NUMERIC_HDRSZ_SHORT + n * size_of::<NumericDigit>();
        let mut buf = varlena_buf(mcx, len)?;
        set_varsize(&mut buf, len);
        let header: u16 = (if sign == NumericSign::Neg {
            NUMERIC_SHORT | NUMERIC_SHORT_SIGN_MASK
        } else {
            NUMERIC_SHORT
        }) | ((dscale as u16) << NUMERIC_SHORT_DSCALE_SHIFT)
            | (if weight < 0 {
                NUMERIC_SHORT_WEIGHT_SIGN_MASK
            } else {
                0
            })
            | ((weight as u16) & NUMERIC_SHORT_WEIGHT_MASK);
        buf[VARHDRSZ..VARHDRSZ + 2].copy_from_slice(&header.to_ne_bytes());
        write_digits(&mut buf[NUMERIC_HDRSZ_SHORT..], &digits[start..start + n]);
        // NUMERIC_CAN_BE_SHORT already range-checked weight & dscale.
        return Ok(Some(buf));
    }

    let len = NUMERIC_HDRSZ + n * size_of::<NumericDigit>();
    let mut buf = varlena_buf(mcx, len)?;
    set_varsize(&mut buf, len);
    let sign_dscale: u16 = sign_word | ((dscale as u16) & NUMERIC_DSCALE_MASK);
    buf[VARHDRSZ..VARHDRSZ + 2].copy_from_slice(&sign_dscale.to_ne_bytes());
    buf[VARHDRSZ + 2..VARHDRSZ + 4].copy_from_slice(&(weight as i16).to_ne_bytes());
    write_digits(&mut buf[NUMERIC_HDRSZ..], &digits[start..start + n]);

    // Check for overflow of the int16 weight / 14-bit dscale fields (C reads
    // them back via NUMERIC_WEIGHT/NUMERIC_DSCALE and compares).
    let stored_weight = numeric_weight(&buf);
    let stored_dscale = numeric_dscale(&buf) as i32;
    if stored_weight != weight || stored_dscale != dscale {
        // have_error path: return None instead of erroring.
        return Ok(None);
    }

    Ok(Some(buf))
}

/// `apply_typmod(var, typmod)`: round/validate `var` in place against `typmod`.
/// Note this is only applied to normal finite values. A default/invalid typmod
/// (`< VARHDRSZ`) is a no-op. (numeric.c:8026)
pub fn apply_typmod(var: &mut NumericVar<'_>, typmod: i32) -> PgResult<()> {
    // Do nothing if we have an invalid typmod.
    if !is_valid_numeric_typmod(typmod) {
        return Ok(());
    }

    let precision = numeric_typmod_precision(typmod);
    let scale = numeric_typmod_scale(typmod);
    let maxdigits = precision - scale;

    // Round to target scale (and set var->dscale).
    kernel_var::round_var(var, scale);

    // But don't allow var->dscale to be negative.
    if var.dscale < 0 {
        var.dscale = 0;
    }

    // Check for overflow - note we can't do this before rounding, because
    // rounding could raise the weight. Also the var's weight could be inflated
    // by leading zeroes (stripped before storage, but perhaps not yet); we must
    // recognize a true zero, whose weight is meaningless.
    let mut ddigits = (var.weight + 1) * DEC_DIGITS;
    if ddigits > maxdigits {
        // Determine true weight; and check for all-zero result.
        for &dig in var.logical_digits().iter() {
            if dig != 0 {
                // Adjust for any high-order decimal zero digits (DEC_DIGITS==4).
                if dig < 10 {
                    ddigits -= 3;
                } else if dig < 100 {
                    ddigits -= 2;
                } else if dig < 1000 {
                    ddigits -= 1;
                }
                if ddigits > maxdigits {
                    return Err(field_overflow(precision, scale, maxdigits));
                }
                break;
            }
            ddigits -= DEC_DIGITS;
        }
    }

    Ok(())
}

/// `apply_typmod_special(num, typmod)`: validate a special value against
/// `typmod`. NaN is allowed regardless of typmod (longstanding behavior); an
/// infinity is rejected if any typmod restriction is present. The value is
/// presented in packed on-disk form (caller error if not special).
/// (numeric.c:8111)
pub fn apply_typmod_special(num: &[u8], typmod: i32) -> PgResult<()> {
    debug_assert!(numeric_is_special(num)); // caller error if not

    // NaN is allowed regardless of the typmod.
    if types_numeric::numeric_is_nan(num) {
        return Ok(());
    }

    // Do nothing if we have a default typmod (-1).
    if !is_valid_numeric_typmod(typmod) {
        return Ok(());
    }

    let precision = numeric_typmod_precision(typmod);
    let scale = numeric_typmod_scale(typmod);

    Err(PgError::error("numeric field overflow")
        .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
        .with_detail(alloc::format!(
            "A field with precision {precision}, scale {scale} cannot hold an infinite value."
        )))
}

/// Build the "numeric field overflow" error with the exact C errdetail text.
fn field_overflow(precision: i32, scale: i32, maxdigits: i32) -> PgError {
    let detail = if maxdigits != 0 {
        alloc::format!(
            "A field with precision {precision}, scale {scale} must round to an absolute value less than 10^{maxdigits}."
        )
    } else {
        alloc::format!(
            "A field with precision {precision}, scale {scale} must round to an absolute value less than 1."
        )
    };
    PgError::error("numeric field overflow")
        .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
        .with_detail(detail)
}

// ---------------------------------------------------------------------------
// Struct codec: NumericData <-> on-disk byte image (bridges the
// NumericData-carrying seams onto the kernels). Validated; never fabricates.
// ---------------------------------------------------------------------------

#[inline]
fn corrupt(detail: &'static str) -> PgError {
    PgError::error(detail)
}

/// `numeric_data_from_bytes`: parse a validated on-disk byte image (including
/// its 4-byte length header) into the structured [`NumericData`], validating
/// the layout. Bytes are never fabricated.
pub fn numeric_data_from_bytes(num: &[u8]) -> PgResult<NumericData> {
    if num.len() < NUMERIC_HDRSZ_SHORT {
        return Err(corrupt("numeric byte image shorter than its minimum header"));
    }
    if u32::from_ne_bytes([num[0], num[1], num[2], num[3]]) != varsize_header(num.len()) {
        return Err(corrupt("numeric varlena length word disagrees with image size"));
    }

    let vl_len_ = i32::from_ne_bytes([num[0], num[1], num[2], num[3]]);
    let header = u16::from_ne_bytes([num[VARHDRSZ], num[VARHDRSZ + 1]]);

    let choice = if numeric_is_special(num) {
        if num.len() != NUMERIC_HDRSZ_SHORT {
            return Err(corrupt("special numeric carries trailing bytes"));
        }
        NumericChoice::NHeader(header)
    } else {
        // Finite value: digit-count must divide evenly.
        let hdr_size = if numeric_header_is_short(num) {
            NUMERIC_HDRSZ_SHORT
        } else {
            NUMERIC_HDRSZ
        };
        if num.len() < hdr_size || (num.len() - hdr_size) % size_of::<NumericDigit>() != 0 {
            return Err(corrupt("numeric digit area has a fractional digit"));
        }
        let ndigits = numeric_ndigits(num, num.len());
        let digit_bytes = numeric_digits(num);
        let mut n_data = alloc::vec::Vec::new();
        n_data
            .try_reserve(ndigits)
            .map_err(|_| corrupt("out of memory parsing numeric byte image"))?;
        for i in 0..ndigits {
            n_data.push(numeric_digit_at(digit_bytes, i));
        }

        if numeric_header_is_short(num) {
            NumericChoice::NShort(NumericShort {
                n_header: header,
                n_data,
            })
        } else {
            NumericChoice::NLong(NumericLong {
                n_sign_dscale: header,
                n_weight: i16::from_ne_bytes([num[VARHDRSZ + 2], num[VARHDRSZ + 3]]),
                n_data,
            })
        }
    };

    Ok(NumericData { vl_len_, choice })
}

/// `numeric_data_to_bytes`: serialize a [`NumericData`] into a fresh charged
/// byte image. Errors (loudly) when the `NumericChoice` variant disagrees with
/// its own header flag bits, or when a nonzero `vl_len_` disagrees with the
/// length recomputed from the digit count — both are corrupt structured values.
pub fn numeric_data_to_bytes<'mcx>(mcx: Mcx<'mcx>, data: &NumericData) -> PgResult<PgVec<'mcx, u8>> {
    let len: usize = match &data.choice {
        NumericChoice::NHeader(_) => NUMERIC_HDRSZ_SHORT,
        NumericChoice::NShort(s) => NUMERIC_HDRSZ_SHORT + s.n_data.len() * size_of::<NumericDigit>(),
        NumericChoice::NLong(l) => NUMERIC_HDRSZ + l.n_data.len() * size_of::<NumericDigit>(),
    };

    // Validate vl_len_ when the structured value carries one (a fresh hand-built
    // struct may leave it 0).
    if data.vl_len_ != 0 && data.vl_len_ as u32 != varsize_header(len) {
        return Err(corrupt("numeric vl_len_ disagrees with its digit count"));
    }

    let mut buf = mcx::vec_with_capacity_in::<u8>(mcx, len)
        .map_err(|_| corrupt("out of memory building numeric byte image"))?;
    buf.extend_from_slice(&varsize_header(len).to_ne_bytes());

    match &data.choice {
        NumericChoice::NHeader(word) => {
            // A bare header word is only legal for a special value (NaN/Inf).
            if (*word & NUMERIC_SIGN_MASK) != NUMERIC_SPECIAL {
                return Err(corrupt("numeric NHeader choice is not a special value"));
            }
            buf.extend_from_slice(&word.to_ne_bytes());
        }
        NumericChoice::NShort(s) => {
            if (s.n_header & NUMERIC_SIGN_MASK) != NUMERIC_SHORT {
                return Err(corrupt("numeric NShort choice lacks the SHORT flag bits"));
            }
            buf.extend_from_slice(&s.n_header.to_ne_bytes());
            for &d in &s.n_data {
                buf.extend_from_slice(&d.to_ne_bytes());
            }
        }
        NumericChoice::NLong(l) => {
            // Long form: the 0x8000 bit must be clear (POS=00 or NEG=01).
            if (l.n_sign_dscale & 0x8000) != 0 {
                return Err(corrupt("numeric NLong choice carries short/special flag bits"));
            }
            buf.extend_from_slice(&l.n_sign_dscale.to_ne_bytes());
            buf.extend_from_slice(&l.n_weight.to_ne_bytes());
            for &d in &l.n_data {
                buf.extend_from_slice(&d.to_ne_bytes());
            }
        }
    }

    debug_assert_eq!(buf.len(), len);
    Ok(buf)
}

// ---------------------------------------------------------------------------
// Integer conversions (numeric.c numericvar_to_int32/uint64/int128 +
// int{2,4,8}_to_numeric + int64_div_fast_to_numeric).
// ---------------------------------------------------------------------------

/// `numericvar_to_int32(var)`: round to nearest integer and convert to `i32`.
/// `Ok(None)` on the C `false` (out of range). (numeric.c:4578)
pub fn numericvar_to_int32(var: &NumericVar<'_>) -> PgResult<Option<i32>> {
    let val = match kernel_transcendental::numericvar_to_int64(var)? {
        Some(v) => v,
        None => return Ok(None),
    };
    if val < i32::MIN as i64 || val > i32::MAX as i64 {
        return Ok(None);
    }
    // Down-convert to int4.
    Ok(Some(val as i32))
}

/// `numericvar_to_uint64(var)`: round to nearest integer and convert to `u64`.
/// `Ok(None)` on the C `false` (overflow, including a negative input).
/// (numeric.c:8269)
pub fn numericvar_to_uint64(var: &NumericVar<'_>) -> PgResult<Option<u64>> {
    // Round to nearest integer. We need a fresh copy because round_var/strip_var
    // mutate in place; set_var_from_var gives us one with carry headroom (in C:
    // init_var + set_var_from_var + round_var(0)). The Mcx is borrowed from the
    // input var's digit buffer allocator.
    let mcx: Mcx = *var.digits.allocator();
    let mut rounded = kernel_var::set_var_from_var(mcx, var)?;
    kernel_var::round_var(&mut rounded, 0);

    // Check for zero input.
    kernel_var::strip_var(&mut rounded);
    let ndigits = rounded.ndigits();
    if ndigits == 0 {
        return Ok(Some(0));
    }

    // Check for negative input.
    if rounded.sign == NumericSign::Neg {
        return Ok(None);
    }

    // For input like 10000000000, we must treat stripped digits as real. So the
    // loop assumes there are weight+1 digits before the decimal point.
    let weight = rounded.weight;
    // Assert(weight >= 0 && ndigits <= weight + 1).
    let digits = rounded.logical_digits();

    let mut val: u64 = digits[0] as u64;
    let mut i = 1i32;
    while i <= weight {
        val = match val.checked_mul(NBASE as u64) {
            Some(v) => v,
            None => return Ok(None),
        };
        if (i as usize) < ndigits {
            val = match val.checked_add(digits[i as usize] as u64) {
                Some(v) => v,
                None => return Ok(None),
            };
        }
        i += 1;
    }

    Ok(Some(val))
}

/// `numericvar_to_int128(var)`: round to nearest integer and convert to `i128`.
/// `Ok(None)` on the C `false` (overflow).
///
/// Reproduces the C overflow test, which accepts `INT128_MIN` (the one nonzero
/// value where `-val == val` on two's complement). (numeric.c:8342)
pub fn numericvar_to_int128(var: &NumericVar<'_>) -> PgResult<Option<i128>> {
    let mcx: Mcx = *var.digits.allocator();
    let mut rounded = kernel_var::set_var_from_var(mcx, var)?;
    kernel_var::round_var(&mut rounded, 0);

    // Check for zero input.
    kernel_var::strip_var(&mut rounded);
    let ndigits = rounded.ndigits();
    if ndigits == 0 {
        return Ok(Some(0));
    }

    let weight = rounded.weight;
    let digits = rounded.logical_digits();
    let neg = rounded.sign == NumericSign::Neg;

    let mut val: i128 = digits[0] as i128;
    let mut i = 1i32;
    while i <= weight {
        let oldval = val;
        val = val.wrapping_mul(NBASE as i128);
        if (i as usize) < ndigits {
            val = val.wrapping_add(digits[i as usize] as i128);
        }

        // The overflow check is a bit tricky because we want to accept
        // INT128_MIN, which will overflow the positive accumulator. We can
        // detect this case easily though because INT128_MIN is the only nonzero
        // value for which -val == val (two's complement).
        if val / NBASE as i128 != oldval {
            // possible overflow?
            if !neg || val.wrapping_neg() != val || val == 0 || oldval < 0 {
                return Ok(None);
            }
        }
        i += 1;
    }

    Ok(Some(if neg { val.wrapping_neg() } else { val }))
}

/// `int64_to_numeric(val)`: build an on-disk byte image from an `i64`.
/// (numeric.c:4402)
pub fn int64_to_numeric<'mcx>(mcx: Mcx<'mcx>, val: i64) -> PgResult<PgVec<'mcx, u8>> {
    let result = kernel_transcendental::int64_to_numericvar(mcx, val)?;
    make_result(mcx, &result)
}

/// `int64_div_fast_to_numeric(val1, log10val2)`: compute `val1 / 10^log10val2`
/// as a fresh on-disk byte image. Much faster than normal numeric division.
/// (numeric.c:4422)
pub fn int64_div_fast_to_numeric<'mcx>(
    mcx: Mcx<'mcx>,
    val1: i64,
    log10val2: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    // result scale
    let rscale = if log10val2 < 0 { 0 } else { log10val2 };

    // how much to decrease the weight by
    let mut w = log10val2 / DEC_DIGITS;
    // how much is left to divide by
    let mut m = log10val2 % DEC_DIGITS;
    if m < 0 {
        m += DEC_DIGITS;
        w -= 1;
    }

    // If there is anything left to divide by (10^m with 0 < m < DEC_DIGITS),
    // multiply the dividend by 10^(DEC_DIGITS - m), and shift the weight by one
    // more.
    let mut result;
    if m > 0 {
        // pow10[] == {1, 10, 100, 1000} for DEC_DIGITS == 4.
        const POW10: [i64; DEC_DIGITS as usize] = [1, 10, 100, 1000];
        let factor: i64 = POW10[(DEC_DIGITS - m) as usize];

        match val1.checked_mul(factor) {
            Some(new_val1) => {
                result = kernel_transcendental::int64_to_numericvar(mcx, new_val1)?;
            }
            None => {
                // do the multiplication using 128-bit integers
                let tmp = val1 as i128 * factor as i128;
                result = int128_to_numericvar(mcx, tmp)?;
            }
        }
        w += 1;
    } else {
        result = kernel_transcendental::int64_to_numericvar(mcx, val1)?;
    }

    result.weight -= w;
    result.dscale = rscale;

    make_result(mcx, &result)
}

/// `int128_to_numericvar(val, var)` (numeric.c:8407): convert a 128-bit integer
/// to a [`NumericVar`]. Reached by [`int64_div_fast_to_numeric`]'s overflow path
/// and by the date/time EXTRACT cores' fast-scaling helper.
pub fn int128_to_numericvar<'mcx>(mcx: Mcx<'mcx>, val: i128) -> PgResult<NumericVar<'mcx>> {
    // int128 can require at most 39 decimal digits; add one for safety.
    let cap = (40 / DEC_DIGITS) as usize;
    let mut buf = crate::alloc_digits(mcx, cap)?;

    let sign;
    let uval: u128 = if val < 0 {
        sign = NumericSign::Neg;
        (val as i128).unsigned_abs()
    } else {
        sign = NumericSign::Pos;
        val as u128
    };

    if val == 0 {
        // ndigits = 0, weight = 0, dscale = 0.
        buf.clear();
        return Ok(NumericVar {
            sign: NumericSign::Pos,
            weight: 0,
            dscale: 0,
            digits: buf,
            headroom: 0,
        });
    }

    // Fill digits from the least-significant end backwards (C: ptr walks down
    // from buf + ndigits). We mirror by writing into `buf` then taking the
    // populated tail as the logical digits via `headroom`.
    let mut uval = uval;
    let mut ndigits = 0usize;
    let mut idx = cap;
    while uval != 0 {
        idx -= 1;
        ndigits += 1;
        let newuval = uval / NBASE as u128;
        buf[idx] = (uval - newuval * NBASE as u128) as NumericDigit;
        uval = newuval;
    }

    Ok(NumericVar {
        sign,
        weight: ndigits as i32 - 1,
        dscale: 0,
        digits: buf,
        headroom: idx,
    })
}

// ---------------------------------------------------------------------------
// Float conversions (numeric.c float4/float8 <-> numeric).
//
// float -> numeric reproduces C exactly: NaN/+-Inf map to the numeric special,
// and a finite value is rendered with C's snprintf("%.*g", DBL_DIG/FLT_DIG)
// then parsed back with set_var_from_str. numeric -> float renders the var to
// decimal (get_str_from_var) and parses it as f64/f32 (C calls float8in/float4in
// on the numeric_out text), mirroring float{4,8}in_internal's ERANGE handling.
// ---------------------------------------------------------------------------

/// `DBL_DIG` from `<float.h>`: decimal digits a `double` represents without
/// loss (15 on IEEE 754).
const DBL_DIG: i32 = 15;

/// `FLT_DIG` from `<float.h>`: decimal digits a `float` (`f32`) represents
/// without loss (6 on IEEE 754).
const FLT_DIG: i32 = 6;

/// Render a finite `f64` exactly as C's `snprintf("%.*g", prec, val)` would:
/// `%g` with `prec` significant digits (treating `prec == 0` as 1),
/// round-half-to-even, trailing zeros stripped (no `#` flag), and `%e` vs `%f`
/// chosen by the decimal exponent of the leading digit (`%e` iff
/// `exp < -4 || exp >= prec`), with the `%e` exponent rendered `e%+02d`.
///
/// Rust's `{:.*e}` produces `prec` significant digits with the same
/// round-half-to-even rounding as the C library, so it is the source of the
/// rounded digit string here.
fn format_g(val: f64, mut prec: i32) -> String {
    debug_assert!(val.is_finite());

    // C: a precision of 0 is treated as 1.
    if prec == 0 {
        prec = 1;
    }

    // %g of zero is "0" (sign preserved as "-0").
    if val == 0.0 {
        return if val.is_sign_negative() {
            String::from("-0")
        } else {
            String::from("0")
        };
    }

    let neg = val < 0.0;
    let a = val.abs();

    // Round to `prec` significant digits in scientific form: {:.*e} gives
    // (prec-1) fractional digits, i.e. exactly `prec` significant digits.
    let sci = alloc::format!("{:.*e}", (prec - 1) as usize, a);
    // `sci` looks like "1.2345e2" or "5e-1".
    let (mant, exp_str) = sci.split_once('e').expect("scientific format has 'e'");
    let exp: i32 = exp_str.parse().expect("exponent is an integer");
    // The significant digits, dot removed (exactly `prec` of them).
    let digits: String = mant.chars().filter(|c| *c != '.').collect();

    let mut out = String::new();
    if neg {
        out.push('-');
    }

    if exp < -4 || exp >= prec {
        // Scientific style: d[.ddd]e(+/-)XX, trailing fractional zeros stripped.
        let mut frac = digits[1..].to_string();
        while frac.ends_with('0') {
            frac.pop();
        }
        out.push(digits.as_bytes()[0] as char);
        if !frac.is_empty() {
            out.push('.');
            out.push_str(&frac);
        }
        out.push('e');
        out.push(if exp < 0 { '-' } else { '+' });
        let mag = exp.unsigned_abs();
        // "%+02d"-style exponent: at least two digits.
        out.push_str(&alloc::format!("{mag:02}"));
    } else if exp >= 0 {
        // Fixed style with the point after (exp + 1) digits.
        let intlen = (exp + 1) as usize;
        if intlen >= digits.len() {
            out.push_str(&digits);
            for _ in 0..(intlen - digits.len()) {
                out.push('0');
            }
        } else {
            let (i, f) = digits.split_at(intlen);
            let mut frac = f.to_string();
            while frac.ends_with('0') {
                frac.pop();
            }
            out.push_str(i);
            if !frac.is_empty() {
                out.push('.');
                out.push_str(&frac);
            }
        }
    } else {
        // Fixed style, exp in -4..=-1: "0.00ddd".
        out.push_str("0.");
        for _ in 0..(-exp - 1) {
            out.push('0');
        }
        let mut frac = digits.clone();
        while frac.ends_with('0') {
            frac.pop();
        }
        out.push_str(&frac);
    }

    out
}

/// `float8_numeric(val)`: build an on-disk byte image from an `f64`. NaN -> NaN
/// special, +-Inf -> +-Inf special; a finite value is rendered with `%.15g` and
/// parsed by `set_var_from_str`. (numeric.c:4711)
pub fn float8_to_numeric<'mcx>(mcx: Mcx<'mcx>, val: f64) -> PgResult<PgVec<'mcx, u8>> {
    if val.is_nan() {
        return make_result(mcx, &NumericVar::special(mcx, NumericSign::NaN));
    }
    if val.is_infinite() {
        let sign = if val < 0.0 {
            NumericSign::NInf
        } else {
            NumericSign::PInf
        };
        return make_result(mcx, &NumericVar::special(mcx, sign));
    }

    let buf = format_g(val, DBL_DIG);
    // No leading/trailing spaces in our own rendering; set_var_from_str never
    // soft-errors on a `%g` string.
    let (result, _endptr) = io::set_var_from_str(mcx, &buf, 0)?;
    make_result(mcx, &result)
}

/// `float4_numeric(val)`: build an on-disk byte image from an `f32`. NaN -> NaN
/// special, +-Inf -> +-Inf special; a finite value is rendered with `%.*g` at
/// `FLT_DIG` (6) precision — NOT widened to `f64` and rendered with `DBL_DIG`,
/// which would print the float's spurious low-order decimal digits — then parsed
/// by `set_var_from_str`. (numeric.c:4822, `float4_numeric`)
pub fn float4_to_numeric<'mcx>(mcx: Mcx<'mcx>, val: f32) -> PgResult<PgVec<'mcx, u8>> {
    if val.is_nan() {
        return make_result(mcx, &NumericVar::special(mcx, NumericSign::NaN));
    }
    if val.is_infinite() {
        let sign = if val < 0.0 {
            NumericSign::NInf
        } else {
            NumericSign::PInf
        };
        return make_result(mcx, &NumericVar::special(mcx, sign));
    }

    // C: snprintf(buf, ..., "%.*g", FLT_DIG, val). format_g rounds the f64
    // widening of `val` to FLT_DIG significant digits, which reproduces the
    // float4's own decimal expansion (the rounding kills the f64 padding bits).
    let buf = format_g(val as f64, FLT_DIG);
    let (result, _endptr) = io::set_var_from_str(mcx, &buf, 0)?;
    make_result(mcx, &result)
}

/// `numeric_float8(num)`: convert an on-disk byte image to `f64`. Specials map
/// to the IEEE `f64` value; a finite value is rendered to decimal and parsed
/// (C calls `float8in` on the `numeric_out` text). On over/underflow raise
/// `ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE` with float8in_internal's message text.
/// (numeric.c:4746)
pub fn numeric_to_float8(num: &[u8]) -> PgResult<f64> {
    if numeric_is_special(num) {
        if types_numeric::numeric_is_pinf(num) {
            return Ok(f64::INFINITY);
        } else if types_numeric::numeric_is_ninf(num) {
            return Ok(f64::NEG_INFINITY);
        } else {
            return Ok(f64::NAN);
        }
    }

    // C: numeric_out -> float8in. Render the var to its canonical decimal text
    // (identical to numeric_out's) then parse as f64. Rendering needs a scratch
    // var with a charged digit buffer, but this conversion only receives the
    // on-disk `&[u8]` (mirroring the C signature that reads a materialized
    // Numeric); allocate the scratch in a short-lived local context. The var
    // (and its PgVec) is dropped before the context, satisfying the reset
    // accounting.
    let scratch = mcx::MemoryContext::new("numeric_float8 scratch");
    let var = decode_finite_for_str(scratch.mcx(), num);
    let s = io::get_str_from_var(&var);
    let result: f64 = s
        .parse()
        .map_err(|_| float_out_of_range(&s, "double precision"))?;
    // float8in_internal: strtod sets ERANGE on overflow (result +-Inf) or
    // genuine underflow (result 0.0 from a nonzero finite input). The source is
    // a finite ordinary numeric; it is zero iff it has no significant digits.
    let source_is_zero = var.ndigits() == 0;
    let err = if result.is_infinite() || (result == 0.0 && !source_is_zero) {
        Some(float_out_of_range(&s, "double precision"))
    } else {
        None
    };
    drop(var);
    drop(scratch);
    match err {
        Some(e) => Err(e),
        None => Ok(result),
    }
}

/// `numeric_float4(num)`: convert an on-disk byte image to `f32`. Specials map
/// to the IEEE `f32` value; finite values are parsed like [`numeric_to_float8`]
/// but mirror float4in_internal's ERANGE handling for `real`. (numeric.c:4840)
pub fn numeric_to_float4(num: &[u8]) -> PgResult<f32> {
    if numeric_is_special(num) {
        if types_numeric::numeric_is_pinf(num) {
            return Ok(f32::INFINITY);
        } else if types_numeric::numeric_is_ninf(num) {
            return Ok(f32::NEG_INFINITY);
        } else {
            return Ok(f32::NAN);
        }
    }

    let scratch = mcx::MemoryContext::new("numeric_float4 scratch");
    let var = decode_finite_for_str(scratch.mcx(), num);
    let s = io::get_str_from_var(&var);
    let result: f32 = s.parse().map_err(|_| float_out_of_range(&s, "real"))?;
    let source_is_zero = var.ndigits() == 0;
    let err = if result.is_infinite() || (result == 0.0 && !source_is_zero) {
        Some(float_out_of_range(&s, "real"))
    } else {
        None
    };
    drop(var);
    drop(scratch);
    match err {
        Some(e) => Err(e),
        None => Ok(result),
    }
}

/// Decode the finite on-disk image into a fresh [`NumericVar`] (digits owned in
/// `mcx`) for rendering with `get_str_from_var`. This mirrors C's
/// `init_var_from_num` (which borrows the on-disk digits) followed by
/// `get_str_from_var`. Caller guarantees `num` is finite (not special).
#[inline]
fn decode_finite_for_str<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> NumericVar<'mcx> {
    let ndigits = numeric_ndigits(num, num.len());
    let digit_bytes = numeric_digits(num);
    let mut digits = PgVec::<NumericDigit>::new_in(mcx);
    digits.reserve_exact(ndigits);
    for i in 0..ndigits {
        digits.push(numeric_digit_at(digit_bytes, i));
    }
    let sign = NumericSign::from_numeric_word(numeric_sign(num)).unwrap_or(NumericSign::Pos);
    NumericVar {
        sign,
        weight: numeric_weight(num),
        dscale: numeric_dscale(num) as i32,
        digits,
        headroom: 0,
    }
}

/// Build the `float{4,8}in_internal` ERANGE error: SQLSTATE 22003 with the exact
/// message `"<num>" is out of range for type <typename>` (float.c:287,489).
#[inline]
fn float_out_of_range(num: &str, typename: &str) -> PgError {
    PgError::error(alloc::format!("\"{num}\" is out of range for type {typename}"))
        .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}
