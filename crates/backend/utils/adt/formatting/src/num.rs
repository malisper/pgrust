//! NUM (number) format-picture engine: roman conversion, locale preparation,
//! `NUM_numpart_from_char` / `NUM_numpart_to_char`, `NUM_eat_non_data_chars`,
//! and the `NUM_processor` driver.
//!
//! Faithful idiomatic port of formatting.c:4875-6282 (PG 18.3).
//!
//! C drives the conversion via raw `char *` pointers into the `inout` and
//! `number` buffers.  We model those buffers as `Vec<u8>` with explicit cursor
//! indices (`inout_p`, `number_p`) so the pointer arithmetic maps directly.
//!
//! ## Seams
//!
//! The genuine cross-subsystem calls are the number/money locale conventions
//! (`PGLC_localeconv`, pg_locale.c) and the multibyte-length helpers
//! (`pg_mbstrlen` / `pg_mblen` over a bounded range, mbutils.c).  Each routes
//! through the centralized `seams::formatting` slots.  The
//! ordinal-suffix helpers (`get_th`), the ASCII case fold (`asc_tolower_z`), and
//! the ASCII upper (`pg_ascii_toupper`) stay in-crate ([`crate::case`]).
//!
//! ## NUM SQL entry-point cores
//!
//! The NUM SQL entry-point cores (`numeric_to_number` / `numeric_to_char` /
//! `int4_to_char` / `int8_to_char` / `float4_to_char` / `float8_to_char`,
//! formatting.c:6289-6937) are ported in [`crate::num_entry`]; they depend on
//! the `backend-utils-adt-numeric` sibling
//! crate (`NumericVar` arithmetic, `numeric_in`/`numeric_out`, `power_var`,
//! `mul_var`, …), reached through seams.  The engine driver `num_processor`
//! (which those entry points feed) is fully ported below.

use ::types_error::{PgError, PgResult};
use ::types_error::{ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_TEXT_REPRESENTATION};
use ::types_cash::CashLconv;
use ::types_core::Oid;

use crate::case::{asc_tolower_z, get_th, pg_ascii_toupper};
use crate::parse::is_c_space;
use crate::tables::*;

/// C: `PGLC_localeconv` via the pg_locale seam (now infallible).
fn pglc_localeconv() -> CashLconv {
    pg_locale_seams::pglc_localeconv::call()
}

/// C: `pg_mbstrlen` via the mbutils seam (`pg_mbstrlen_with_len`). Called on the
/// numeric format pattern (server-encoding-valid SQL text), so the
/// `report_invalid_encoding` path is dead; fall back to the byte length (an
/// upper bound on the char count) rather than escalate.
fn pg_mbstrlen(s: &[u8]) -> i32 {
    mbutils_seams::pg_mbstrlen_with_len::call(s, s.len() as i32)
        .unwrap_or(s.len() as i32)
}

/// C: bounded `pg_mblen` via the mbutils seam (`pg_mblen_range`). C's `pg_mblen`
/// does not validate; the seam only Errs on a slice-overrunning leading char,
/// where the clamped length is the slice length (dead path falls back there).
fn pg_mblen_range(s: &[u8]) -> i32 {
    mbutils_seams::pg_mblen_range::call(s).unwrap_or(s.len() as i32)
}

/// C: `fill_str` (formatting.c:4875) -- fill `max` bytes with `c`, NUL-terminate.
/// Here we return the filled `Vec<u8>` (without the NUL; callers append text).
pub fn fill_str(c: u8, max: usize) -> Vec<u8> {
    vec![c; max]
}

/// C: `int_to_roman` (formatting.c:5080).
pub fn int_to_roman(number: i32) -> Vec<u8> {
    // Out of range -> '###############'.
    if number > 3999 || number < 1 {
        return fill_str(b'#', MAX_ROMAN_LEN);
    }

    let numstr = number.to_string();
    let numstr = numstr.as_bytes();
    let mut len = numstr.len();
    let mut result: Vec<u8> = Vec::with_capacity(MAX_ROMAN_LEN + 1);

    for &ch in numstr.iter() {
        // num = *p - ('0' + 1)
        let mut num = ch as i32 - (b'0' as i32 + 1);
        if num < 0 {
            len -= 1;
            continue; // ignore zeroes
        }
        match len {
            4 => {
                while num >= 0 {
                    result.extend_from_slice(b"M");
                    num -= 1;
                }
            }
            3 => result.extend_from_slice(RM100[num as usize].as_bytes()),
            2 => result.extend_from_slice(RM10[num as usize].as_bytes()),
            1 => result.extend_from_slice(RM1[num as usize].as_bytes()),
            _ => {}
        }
        len -= 1;
    }
    result
}

/// The number processor's working state (C: `NUMProc`, formatting.c:1036).
struct NumProc<'a> {
    is_to_char: bool,
    num: &'a mut NUMDesc,

    sign: i32,
    sign_wrote: bool,
    num_count: i32,
    num_in: bool,
    num_curr: i32,
    out_pre_spaces: i32,

    read_dec: bool,
    read_post: i32,
    read_pre: i32,

    number: Vec<u8>,              // the "number" buffer (NUL-free working copy)
    number_p: usize,              // index into number
    inout: Vec<u8>,               // input (to_number) or output (to_char) buffer
    inout_p: usize,               // index into inout
    last_relevant: Option<usize>, // index into number, or None

    l_negative_sign: Vec<u8>,
    l_positive_sign: Vec<u8>,
    decimal: Vec<u8>,
    l_thousands_sep: Vec<u8>,
    l_currency_symbol: Vec<u8>,
}

impl NumProc<'_> {
    #[inline]
    fn number_at(&self, i: usize) -> u8 {
        if i < self.number.len() {
            self.number[i]
        } else {
            0
        }
    }
    #[inline]
    fn inout_at(&self, i: usize) -> u8 {
        if i < self.inout.len() {
            self.inout[i]
        } else {
            0
        }
    }
}

/// C: `roman_to_int` (formatting.c:5140).  Consumes from `np.inout` at
/// `np.inout_p`; returns the value (>= 1) or -1 on invalid input.
fn roman_to_int(np: &mut NumProc, input_len: usize) -> i32 {
    let mut result = 0i32;
    let mut roman_chars = [0u8; MAX_ROMAN_LEN];
    let mut roman_values = [0i32; MAX_ROMAN_LEN];
    let mut repeat_count = 1;
    let mut v_count = 0;
    let mut l_count = 0;
    let mut d_count = 0;
    let mut subtraction_encountered = false;
    let mut last_subtracted_value = 0;

    let overload = |p: usize| p >= input_len;

    // Skip leading whitespace.
    while !overload(np.inout_p) && is_c_space(np.inout_at(np.inout_p)) {
        np.inout_p += 1;
    }

    // Collect valid roman numerals (at most MAX_ROMAN_LEN).
    let mut len = 0usize;
    while len < MAX_ROMAN_LEN && !overload(np.inout_p) {
        let curr_char = pg_ascii_toupper(np.inout_at(np.inout_p));
        let curr_value = roman_val(curr_char);
        if curr_value == 0 {
            break;
        }
        roman_chars[len] = curr_char;
        roman_values[len] = curr_value;
        np.inout_p += 1;
        len += 1;
    }

    if len == 0 {
        return -1;
    }

    let mut i = 0usize;
    while i < len {
        let curr_char = roman_chars[i];
        let curr_value = roman_values[i];

        if subtraction_encountered && curr_value >= last_subtracted_value {
            return -1;
        }

        if (v_count != 0 && curr_value >= roman_val(b'V'))
            || (l_count != 0 && curr_value >= roman_val(b'L'))
            || (d_count != 0 && curr_value >= roman_val(b'D'))
        {
            return -1;
        }
        match curr_char {
            b'V' => v_count += 1,
            b'L' => l_count += 1,
            b'D' => d_count += 1,
            _ => {}
        }

        if i < len - 1 {
            let next_char = roman_chars[i + 1];
            let next_value = roman_values[i + 1];

            if curr_value < next_value {
                if !is_valid_sub_comb(curr_char, next_char) {
                    return -1;
                }
                if repeat_count > 1 {
                    return -1;
                }
                if (v_count != 0 && next_value >= roman_val(b'V'))
                    || (l_count != 0 && next_value >= roman_val(b'L'))
                    || (d_count != 0 && next_value >= roman_val(b'D'))
                {
                    return -1;
                }
                match next_char {
                    b'V' => v_count += 1,
                    b'L' => l_count += 1,
                    b'D' => d_count += 1,
                    _ => {}
                }
                i += 1; // skip next numeral
                repeat_count = 1;
                subtraction_encountered = true;
                last_subtracted_value = curr_value;
                result += next_value - curr_value;
            } else {
                if curr_char == next_char {
                    repeat_count += 1;
                    if repeat_count > 3 {
                        return -1;
                    }
                } else {
                    repeat_count = 1;
                }
                result += curr_value;
            }
        } else {
            result += curr_value;
        }
        i += 1;
    }

    result
}

/// C: `NUM_prepare_locale` (formatting.c:5290). The locale-convention strings
/// (`CashLconv` fields, all `String`) are read as bytes via `as_bytes()`; the
/// NUM engine keeps them in `Vec<u8>` scratch (mirroring the C `char *`
/// `CurrentMemoryContext` copies).
fn num_prepare_locale(np: &mut NumProc) {
    if np.num.need_locale != 0 {
        let lconv = pglc_localeconv();

        np.l_negative_sign = if !lconv.negative_sign.is_empty() {
            lconv.negative_sign.as_bytes().to_vec()
        } else {
            b"-".to_vec()
        };
        np.l_positive_sign = if !lconv.positive_sign.is_empty() {
            lconv.positive_sign.as_bytes().to_vec()
        } else {
            b"+".to_vec()
        };
        np.decimal = if !lconv.decimal_point.is_empty() {
            lconv.decimal_point.as_bytes().to_vec()
        } else {
            b".".to_vec()
        };
        if !np.num.is_ldecimal() {
            np.decimal = b".".to_vec();
        }

        np.l_thousands_sep = if !lconv.thousands_sep.is_empty() {
            lconv.thousands_sep.as_bytes().to_vec()
        } else if np.decimal.as_slice() != b"," {
            b",".to_vec()
        } else {
            b".".to_vec()
        };

        np.l_currency_symbol = if !lconv.currency_symbol.is_empty() {
            lconv.currency_symbol.as_bytes().to_vec()
        } else {
            b" ".to_vec()
        };
    } else {
        np.l_negative_sign = b"-".to_vec();
        np.l_positive_sign = b"+".to_vec();
        np.decimal = b".".to_vec();
        np.l_thousands_sep = b",".to_vec();
        np.l_currency_symbol = b" ".to_vec();
    }
}

/// C: `get_last_relevant_decnum` (formatting.c:5372).  Returns the index in
/// `num` of the last relevant digit after the decimal point (or the point
/// itself), or `None` if no decimal point.
fn get_last_relevant_decnum(num: &[u8]) -> Option<usize> {
    let dot = num.iter().position(|&c| c == b'.')?;
    let mut result = dot;
    let mut p = dot + 1;
    while p < num.len() && num[p] != 0 {
        if num[p] != b'0' {
            result = p;
        }
        p += 1;
    }
    Some(result)
}

// C: AMOUNT_TEST(s) / OVERLOAD_TEST (formatting.c:1075).
#[inline]
fn overload_test(np: &NumProc, input_len: usize) -> bool {
    np.inout_p >= input_len
}
#[inline]
fn amount_test(np: &NumProc, input_len: usize, s: usize) -> bool {
    np.inout_p <= input_len.saturating_sub(s)
}

/// C: `NUM_numpart_from_char` (formatting.c:5400).
fn num_numpart_from_char(np: &mut NumProc, id: i32, input_len: usize) {
    let mut isread = false;

    if overload_test(np, input_len) {
        return;
    }

    if np.inout_at(np.inout_p) == b' ' {
        np.inout_p += 1;
    }

    if overload_test(np, input_len) {
        return;
    }

    // read sign before number
    if np.number_at(0) == b' ' && (id == NUM_0 || id == NUM_9) && (np.read_pre + np.read_post) == 0
    {
        if np.num.is_lsign() && np.num.lsign == NUM_LSIGN_PRE {
            let xn = np.l_negative_sign.len();
            let xp = np.l_positive_sign.len();
            if xn != 0
                && amount_test(np, input_len, xn)
                && np.inout[np.inout_p..np.inout_p + xn] == np.l_negative_sign[..]
            {
                np.inout_p += xn;
                np.number[0] = b'-';
            } else if xp != 0
                && amount_test(np, input_len, xp)
                && np.inout[np.inout_p..np.inout_p + xp] == np.l_positive_sign[..]
            {
                np.inout_p += xp;
                np.number[0] = b'+';
            }
        } else {
            // simple + - < >
            let c = np.inout_at(np.inout_p);
            if c == b'-' || (np.num.is_bracket() && c == b'<') {
                np.number[0] = b'-';
                np.inout_p += 1;
            } else if c == b'+' {
                np.number[0] = b'+';
                np.inout_p += 1;
            }
        }
    }

    if overload_test(np, input_len) {
        return;
    }

    // read digit or decimal point
    if np.inout_at(np.inout_p).is_ascii_digit() {
        if np.read_dec && np.read_post == np.num.post {
            return;
        }
        let c = np.inout_at(np.inout_p);
        write_number(np, c);
        if np.read_dec {
            np.read_post += 1;
        } else {
            np.read_pre += 1;
        }
        isread = true;
    } else if np.num.is_decimal() && !np.read_dec {
        let x = np.decimal.len();
        if x != 0
            && amount_test(np, input_len, x)
            && np.inout[np.inout_p..np.inout_p + x] == np.decimal[..]
        {
            np.inout_p += x - 1;
            write_number(np, b'.');
            np.read_dec = true;
            isread = true;
        }
    }

    if overload_test(np, input_len) {
        return;
    }

    // Read sign behind "last" number.
    if np.number_at(0) == b' ' && np.read_pre + np.read_post > 0 {
        if np.num.is_lsign()
            && isread
            && (np.inout_p + 1) < input_len
            && !np.inout_at(np.inout_p + 1).is_ascii_digit()
        {
            let tmp = np.inout_p;
            np.inout_p += 1;
            let xn = np.l_negative_sign.len();
            let xp = np.l_positive_sign.len();
            if xn != 0
                && amount_test(np, input_len, xn)
                && np.inout[np.inout_p..np.inout_p + xn] == np.l_negative_sign[..]
            {
                np.inout_p += xn - 1;
                np.number[0] = b'-';
            } else if xp != 0
                && amount_test(np, input_len, xp)
                && np.inout[np.inout_p..np.inout_p + xp] == np.l_positive_sign[..]
            {
                np.inout_p += xp - 1;
                np.number[0] = b'+';
            }
            if np.number_at(0) == b' ' {
                np.inout_p = tmp;
            }
        } else if !isread && !np.num.is_lsign() && (np.num.is_plus() || np.num.is_minus()) {
            let c = np.inout_at(np.inout_p);
            if c == b'-' || c == b'+' {
                np.number[0] = c;
            }
        }
    }
}

/// Write a byte at `number_p`, growing the buffer; advances `number_p`.
fn write_number(np: &mut NumProc, c: u8) {
    if np.number_p >= np.number.len() {
        np.number.resize(np.number_p + 1, 0);
    }
    np.number[np.number_p] = c;
    np.number_p += 1;
}

/// C: `IS_PREDEC_SPACE(_n)` (formatting.c:5605).
fn is_predec_space(np: &NumProc) -> bool {
    !np.num.is_zero() && np.number_p == 0 && np.number_at(0) == b'0' && np.num.post != 0
}

/// Write `bytes` into the inout buffer at `inout_p`, growing as needed; advances
/// `inout_p` by `bytes.len()`.
fn inout_write(np: &mut NumProc, bytes: &[u8]) {
    let end = np.inout_p + bytes.len();
    if end > np.inout.len() {
        np.inout.resize(end, 0);
    }
    np.inout[np.inout_p..end].copy_from_slice(bytes);
    np.inout_p = end;
}

/// Write a single byte at `inout_p`, growing as needed; advances `inout_p`.
fn inout_put(np: &mut NumProc, c: u8) {
    if np.inout_p >= np.inout.len() {
        np.inout.resize(np.inout_p + 1, 0);
    }
    np.inout[np.inout_p] = c;
    np.inout_p += 1;
}

/// C: `NUM_numpart_to_char` (formatting.c:5615).
fn num_numpart_to_char(np: &mut NumProc, id: i32) -> PgResult<()> {
    if np.num.is_roman() {
        return Ok(());
    }

    np.num_in = false;

    // Write sign if a real number will be written.
    if !np.sign_wrote
        && (np.num_curr >= np.out_pre_spaces
            || (np.num.is_zero() && np.num.zero_start == np.num_curr))
        && (!is_predec_space(np)
            || np
                .last_relevant
                .map(|lr| np.number_at(lr) == b'.')
                .unwrap_or(false))
    {
        if np.num.is_lsign() {
            if np.num.lsign == NUM_LSIGN_PRE {
                let s = if np.sign == b'-' as i32 {
                    np.l_negative_sign.clone()
                } else {
                    np.l_positive_sign.clone()
                };
                inout_write(np, &s);
                np.sign_wrote = true;
            }
        } else if np.num.is_bracket() {
            let c = if np.sign == b'+' as i32 { b' ' } else { b'<' };
            inout_put(np, c);
            np.sign_wrote = true;
        } else if np.sign == b'+' as i32 {
            if !np.num.is_fillmode() {
                inout_put(np, b' ');
            }
            np.sign_wrote = true;
        } else if np.sign == b'-' as i32 {
            inout_put(np, b'-');
            np.sign_wrote = true;
        }
    }

    // digits / FM / Zero / Dec. point
    if id == NUM_9 || id == NUM_0 || id == NUM_D || id == NUM_DEC {
        if np.num_curr < np.out_pre_spaces && (np.num.zero_start > np.num_curr || !np.num.is_zero())
        {
            if !np.num.is_fillmode() {
                inout_put(np, b' ');
            }
        } else if np.num.is_zero()
            && np.num_curr < np.out_pre_spaces
            && np.num.zero_start <= np.num_curr
        {
            inout_put(np, b'0');
            np.num_in = true;
        } else {
            // Decimal point?
            if np.number_at(np.number_p) == b'.' {
                let lr_is_dot = np
                    .last_relevant
                    .map(|lr| np.number_at(lr) == b'.')
                    .unwrap_or(false);
                if np.last_relevant.is_none() || !lr_is_dot {
                    let dec = np.decimal.clone();
                    inout_write(np, &dec);
                } else if np.num.is_fillmode() && lr_is_dot {
                    let dec = np.decimal.clone();
                    inout_write(np, &dec);
                }
            } else {
                // Write Digits
                let skip = np.last_relevant.is_some()
                    && np.number_p > np.last_relevant.unwrap()
                    && id != NUM_0;
                if skip {
                    // do nothing (C: empty statement)
                } else if is_predec_space(np) {
                    if !np.num.is_fillmode() {
                        inout_put(np, b' ');
                    } else if np
                        .last_relevant
                        .map(|lr| np.number_at(lr) == b'.')
                        .unwrap_or(false)
                    {
                        inout_put(np, b'0');
                    }
                } else {
                    let c = np.number_at(np.number_p);
                    inout_put(np, c);
                    np.num_in = true;
                }
            }
            // do not exceed string length
            if np.number_at(np.number_p) != 0 {
                np.number_p += 1;
            }
        }

        let mut end = np.num_count
            + (if np.out_pre_spaces != 0 { 1 } else { 0 })
            + (if np.num.is_decimal() { 1 } else { 0 });

        if let Some(lr) = np.last_relevant {
            if lr == np.number_p {
                end = np.num_curr;
            }
        }

        if np.num_curr + 1 == end {
            if np.sign_wrote && np.num.is_bracket() {
                let c = if np.sign == b'+' as i32 { b' ' } else { b'>' };
                inout_put(np, c);
            } else if np.num.is_lsign() && np.num.lsign == NUM_LSIGN_POST {
                let s = if np.sign == b'-' as i32 {
                    np.l_negative_sign.clone()
                } else {
                    np.l_positive_sign.clone()
                };
                inout_write(np, &s);
            }
        }
    }

    np.num_curr += 1;
    Ok(())
}

/// C: `NUM_eat_non_data_chars` (formatting.c:5805).
fn num_eat_non_data_chars(np: &mut NumProc, mut n: i32, input_len: usize) -> PgResult<()> {
    while n > 0 {
        n -= 1;
        if overload_test(np, input_len) {
            break;
        }
        if b"0123456789.,+-".contains(&np.inout_at(np.inout_p)) {
            break;
        }
        np.inout_p += pg_mblen_range(&np.inout[np.inout_p..input_len]) as usize;
    }
    Ok(())
}

/// Result of `NUM_processor`: either the to_char output bytes, or the to_number
/// extracted number string.
pub struct NumProcessed {
    pub out: Vec<u8>,
}

/// C: `NUM_processor` (formatting.c:5820).
#[allow(clippy::too_many_arguments)]
pub fn num_processor(
    nodes: &[FormatNode],
    num: &mut NUMDesc,
    inout: Vec<u8>,
    number: Vec<u8>,
    input_len: usize,
    to_char_out_pre_spaces: i32,
    sign: i32,
    is_to_char: bool,
    _collid: Oid,
) -> PgResult<NumProcessed> {
    let mut np = NumProc {
        is_to_char,
        num,
        sign: 0,
        sign_wrote: false,
        num_count: 0,
        num_in: false,
        num_curr: 0,
        out_pre_spaces: 0,
        read_dec: false,
        read_post: 0,
        read_pre: 0,
        // Charge the genuinely-growable working buffers (C's `number`/`inout`
        // `char *` palloc'd in `CurrentMemoryContext`) to the current context.
        number,
        number_p: 0,
        inout,
        inout_p: 0,
        last_relevant: None,
        l_negative_sign: Vec::new(),
        l_positive_sign: Vec::new(),
        decimal: Vec::new(),
        l_thousands_sep: Vec::new(),
        l_currency_symbol: Vec::new(),
    };

    if np.num.zero_start != 0 {
        np.num.zero_start -= 1;
    }

    if np.num.is_eeee() {
        if !np.is_to_char {
            return Err(
                PgError::error("\"EEEE\" not supported for input".to_string())
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            );
        }
        // return strcpy(inout, number)
        return Ok(NumProcessed {
            out: cstr(&np.number),
        });
    }

    // Sign
    if is_to_char {
        np.sign = sign;
        if np.num.is_plus() || np.num.is_minus() {
            // C: if (IS_PLUS && !IS_MINUS) sign_wrote=false; else sign_wrote=true;
            if np.num.is_plus() && !np.num.is_minus() {
                np.sign_wrote = false; // need sign
            } else {
                np.sign_wrote = true; // needn't sign
            }
        } else {
            if np.sign != b'-' as i32 && np.num.is_fillmode() {
                np.num.flag &= !NUM_F_BRACKET;
            }
            np.sign_wrote = np.sign == b'+' as i32 && np.num.is_fillmode() && !np.num.is_lsign();
            if np.num.lsign == NUM_LSIGN_PRE && np.num.pre == np.num.pre_lsign_num {
                np.num.lsign = NUM_LSIGN_POST;
            }
        }
    } else {
        np.sign = 0; // C: Np->sign = false
    }

    // Count
    np.num_count = np.num.post + np.num.pre - 1;

    if is_to_char {
        np.out_pre_spaces = to_char_out_pre_spaces;

        if np.num.is_fillmode() && np.num.is_decimal() {
            np.last_relevant = get_last_relevant_decnum(&np.number);

            if np.last_relevant.is_some() && np.num.zero_end > np.out_pre_spaces {
                // last_zero_pos = strlen(number) - 1, capped at zero_end - out_pre_spaces
                let nlen = cstrlen(&np.number);
                let last_zero_pos =
                    (nlen as i32 - 1).min(np.num.zero_end - np.out_pre_spaces) as usize;
                if np.last_relevant.unwrap() < last_zero_pos {
                    np.last_relevant = Some(last_zero_pos);
                }
            }
        }

        if !np.sign_wrote && np.out_pre_spaces == 0 {
            np.num_count += 1;
        }
    } else {
        np.out_pre_spaces = 0;
        // number[0] = ' '; number[1] = '\0'  (sign space)
        if np.number.len() < 2 {
            np.number.resize(2, 0);
        }
        np.number[0] = b' ';
        np.number[1] = 0;
    }

    np.num_in = false;
    np.num_curr = 0;

    num_prepare_locale(&mut np);

    // Processor direct cycle
    np.number_p = if np.is_to_char { 0 } else { 1 };

    np.inout_p = 0;
    let mut idx = 0usize;
    while nodes[idx].typ != NODE_TYPE_END {
        let n = &nodes[idx];

        if !np.is_to_char && overload_test(&np, input_len) {
            break;
        }

        if n.typ == NODE_TYPE_ACTION {
            let id = NUM_KEYWORDS[n.key as usize].id;
            let mut do_increment = true;
            match id {
                NUM_9 | NUM_0 | NUM_DEC | NUM_D => {
                    if np.is_to_char {
                        num_numpart_to_char(&mut np, id)?;
                        idx += 1;
                        continue; // for()
                    } else {
                        num_numpart_from_char(&mut np, id, input_len);
                        // break switch -> fall to inout_p++
                    }
                }
                NUM_COMMA => {
                    if np.is_to_char {
                        if !np.num_in {
                            if np.num.is_fillmode() {
                                idx += 1;
                                continue;
                            } else {
                                inout_set(&mut np, b' ');
                            }
                        } else {
                            inout_set(&mut np, b',');
                        }
                    } else {
                        if !np.num_in && np.num.is_fillmode() {
                            idx += 1;
                            continue;
                        }
                        if np.inout_at(np.inout_p) != b',' {
                            idx += 1;
                            continue;
                        }
                    }
                }
                NUM_G => {
                    let pattern = np.l_thousands_sep.clone();
                    let mut pattern_len = pattern.len();
                    if np.is_to_char {
                        if !np.num_in {
                            if np.num.is_fillmode() {
                                idx += 1;
                                continue;
                            } else {
                                pattern_len = pg_mbstrlen(&pattern) as usize;
                                let ip = np.inout_p;
                                ensure_inout(&mut np, ip + pattern_len);
                                for k in 0..pattern_len {
                                    np.inout[ip + k] = b' ';
                                }
                                np.inout_p += pattern_len - 1;
                            }
                        } else {
                            inout_overlay(&mut np, &pattern);
                            np.inout_p += pattern_len - 1;
                        }
                    } else {
                        if !np.num_in && np.num.is_fillmode() {
                            idx += 1;
                            continue;
                        }
                        if amount_test(&np, input_len, pattern_len)
                            && np.inout[np.inout_p..np.inout_p + pattern_len] == pattern[..]
                        {
                            np.inout_p += pattern_len - 1;
                        } else {
                            idx += 1;
                            continue;
                        }
                    }
                }
                NUM_L => {
                    let pattern = np.l_currency_symbol.clone();
                    if np.is_to_char {
                        inout_overlay(&mut np, &pattern);
                        np.inout_p += pattern.len() - 1;
                    } else {
                        let cnt = pg_mbstrlen(&pattern) as i32;
                        num_eat_non_data_chars(&mut np, cnt, input_len)?;
                        idx += 1;
                        continue;
                    }
                }
                NUM_RN | NUM_RN_LOWER => {
                    if np.is_to_char {
                        // C NUM_RN/NUM_rn to_char (formatting.c:6103-6134) writes
                        // ONLY the Roman numeral: strcpy(...) under FM, else
                        // sprintf("%15s", number_p). No sign is ever written
                        // (RN never calls NUM_numpart_to_char, so sign==0); the
                        // result is exactly 15 chars (non-FM) or the bare numeral
                        // (FM). The leading space psql shows in regress .out is a
                        // column left-margin, not part of the value.
                        let number_p: Vec<u8> = if id == NUM_RN_LOWER {
                            asc_tolower_z(&cstr_from(&np.number, np.number_p))
                        } else {
                            cstr_from(&np.number, np.number_p)
                        };
                        if np.num.is_fillmode() {
                            inout_overlay(&mut np, &number_p);
                        } else {
                            // sprintf("%15s", number_p)
                            let padded =
                                crate::printf::fmt_pad_str(15, &String::from_utf8_lossy(&number_p));
                            inout_overlay(&mut np, padded.as_bytes());
                        }
                        let written = cstrlen(&np.inout[np.inout_p..]);
                        np.inout_p += written - 1;
                    } else {
                        let roman_result = roman_to_int(&mut np, input_len);
                        if roman_result < 0 {
                            return Err(PgError::error("invalid Roman numeral".to_string())
                                .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION));
                        }
                        let digits = roman_result.to_string();
                        let numlen = digits.len();
                        let npp = np.number_p;
                        ensure_number(&mut np, npp + numlen);
                        np.number[npp..npp + numlen].copy_from_slice(digits.as_bytes());
                        np.number_p += numlen;
                        np.num.pre = numlen as i32;
                        np.num.post = 0;
                        idx += 1;
                        continue; // roman_to_int ate all the chars
                    }
                }
                NUM_TH_LOWER_ID => {
                    if np.num.is_roman()
                        || np.number_at(0) == b'#'
                        || np.sign == b'-' as i32
                        || np.num.is_decimal()
                    {
                        idx += 1;
                        continue;
                    }
                    if np.is_to_char {
                        let th = get_th(&cstr(&np.number), TH_LOWER)?;
                        inout_overlay(&mut np, th.as_bytes());
                        np.inout_p += 1;
                    } else {
                        num_eat_non_data_chars(&mut np, 2, input_len)?;
                        idx += 1;
                        continue;
                    }
                }
                NUM_TH => {
                    if np.num.is_roman()
                        || np.number_at(0) == b'#'
                        || np.sign == b'-' as i32
                        || np.num.is_decimal()
                    {
                        idx += 1;
                        continue;
                    }
                    if np.is_to_char {
                        let th = get_th(&cstr(&np.number), TH_UPPER)?;
                        inout_overlay(&mut np, th.as_bytes());
                        np.inout_p += 1;
                    } else {
                        num_eat_non_data_chars(&mut np, 2, input_len)?;
                        idx += 1;
                        continue;
                    }
                }
                NUM_MI => {
                    if np.is_to_char {
                        if np.sign == b'-' as i32 {
                            inout_set(&mut np, b'-');
                        } else if np.num.is_fillmode() {
                            idx += 1;
                            continue;
                        } else {
                            inout_set(&mut np, b' ');
                        }
                    } else if np.inout_at(np.inout_p) == b'-' {
                        np.number[0] = b'-';
                    } else {
                        num_eat_non_data_chars(&mut np, 1, input_len)?;
                        idx += 1;
                        continue;
                    }
                }
                NUM_PL => {
                    if np.is_to_char {
                        if np.sign == b'+' as i32 {
                            inout_set(&mut np, b'+');
                        } else if np.num.is_fillmode() {
                            idx += 1;
                            continue;
                        } else {
                            inout_set(&mut np, b' ');
                        }
                    } else if np.inout_at(np.inout_p) == b'+' {
                        np.number[0] = b'+';
                    } else {
                        num_eat_non_data_chars(&mut np, 1, input_len)?;
                        idx += 1;
                        continue;
                    }
                }
                NUM_SG => {
                    if np.is_to_char {
                        let sg = np.sign as u8;
                        inout_set(&mut np, sg);
                    } else {
                        let c = np.inout_at(np.inout_p);
                        if c == b'-' {
                            np.number[0] = b'-';
                        } else if c == b'+' {
                            np.number[0] = b'+';
                        } else {
                            num_eat_non_data_chars(&mut np, 1, input_len)?;
                            idx += 1;
                            continue;
                        }
                    }
                }
                _ => {
                    idx += 1;
                    continue;
                }
            }
            // bottom-of-loop inout_p++
            if do_increment {
                np.inout_p += 1;
            }
            do_increment = true;
            let _ = do_increment;
        } else {
            // Non-pattern character.
            if np.is_to_char {
                let cs = cstr_node(&n.character);
                inout_overlay(&mut np, &cs);
                np.inout_p += cs.len();
            } else {
                np.inout_p += pg_mblen_range(&np.inout[np.inout_p..input_len]) as usize;
            }
            idx += 1;
            continue;
        }

        idx += 1;
    }

    if np.is_to_char {
        // *inout_p = '\0'; return inout
        Ok(NumProcessed {
            out: cstr(&np.inout[..np.inout_p.min(np.inout.len())]),
        })
    } else {
        // Terminate number; trim trailing '.'.
        if np.number_p >= 1 && np.number_at(np.number_p - 1) == b'.' {
            np.number[np.number_p - 1] = 0;
        } else if np.number_p < np.number.len() {
            np.number[np.number_p] = 0;
        } else {
            np.number.push(0);
        }
        np.num.post = np.read_post;
        Ok(NumProcessed {
            out: cstr(&np.number),
        })
    }
}

/// Set a single byte at the current `inout_p` *without* advancing (the C
/// `*Np->inout_p = c` before the shared `inout_p++`).
fn inout_set(np: &mut NumProc, c: u8) {
    if np.inout_p >= np.inout.len() {
        np.inout.resize(np.inout_p + 1, 0);
    }
    np.inout[np.inout_p] = c;
}

/// Overlay `bytes` starting at `inout_p` *without* advancing inout_p (C's
/// `strcpy(Np->inout_p, ...)`).
fn inout_overlay(np: &mut NumProc, bytes: &[u8]) {
    ensure_inout(np, np.inout_p + bytes.len());
    np.inout[np.inout_p..np.inout_p + bytes.len()].copy_from_slice(bytes);
}

fn ensure_inout(np: &mut NumProc, len: usize) {
    if len > np.inout.len() {
        np.inout.resize(len, 0);
    }
}
fn ensure_number(np: &mut NumProc, len: usize) {
    if len > np.number.len() {
        np.number.resize(len, 0);
    }
}

/// `strlen` of a NUL-free-or-NUL-terminated byte buffer.
fn cstrlen(b: &[u8]) -> usize {
    b.iter().position(|&c| c == 0).unwrap_or(b.len())
}

/// Copy a NUL-terminated C string out of a byte buffer (stops at the first NUL).
fn cstr(b: &[u8]) -> Vec<u8> {
    b[..cstrlen(b)].to_vec()
}

/// `cstr` of the buffer starting at `from`.
fn cstr_from(b: &[u8], from: usize) -> Vec<u8> {
    if from >= b.len() {
        return Vec::new();
    }
    cstr(&b[from..])
}

fn cstr_node(buf: &[u8; MAX_MULTIBYTE_CHAR_LEN + 1]) -> Vec<u8> {
    cstr(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int_to_roman_values() {
        assert_eq!(int_to_roman(1), b"I");
        assert_eq!(int_to_roman(4), b"IV");
        assert_eq!(int_to_roman(2024), b"MMXXIV");
        assert_eq!(int_to_roman(3888), b"MMMDCCCLXXXVIII");
        assert_eq!(int_to_roman(0), b"###############");
        assert_eq!(int_to_roman(4000), b"###############");
    }

    #[test]
    fn fill_str_fills() {
        assert_eq!(fill_str(b'#', 3), b"###");
        assert_eq!(fill_str(b' ', 0), b"");
    }

    #[test]
    fn get_last_relevant_decnum_matches_c() {
        // "12.3400" -> last relevant after '.' is the '4' at index 4.
        assert_eq!(get_last_relevant_decnum(b"12.3400"), Some(4));
        // No decimal point -> None.
        assert_eq!(get_last_relevant_decnum(b"1234"), None);
        // "5." -> only the dot is relevant.
        assert_eq!(get_last_relevant_decnum(b"5."), Some(1));
    }
}
