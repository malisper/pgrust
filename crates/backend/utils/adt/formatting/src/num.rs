//! NUM (number) format-picture engine: roman conversion, locale prep,
//! NUM_numpart_from_char / NUM_numpart_to_char, NUM_eat_non_data_chars, and the
//! NUM_processor driver. C drives via `char *` cursors into the `inout`/`number`
//! buffers; modeled here as `Vec<u8>` scratch with explicit cursor indices.

use ::types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_TEXT_REPRESENTATION,
};
use ::types_core::Oid;

use crate::case::{asc_tolower, get_th, pg_ascii_toupper};
use crate::parse::is_c_space;
use crate::tables::*;

fn pg_mbstrlen(s: &[u8]) -> i32 {
    mbutils::pg_mbstrlen_with_len(s)
}

fn pg_mblen_range(s: &[u8]) -> i32 {
    mbutils::pg_mblen_range(s).unwrap_or(s.len() as i32)
}

pub fn fill_str(c: u8, max: usize) -> Vec<u8> {
    vec![c; max]
}

pub fn int_to_roman(number: i32) -> Vec<u8> {
    if number > 3999 || number < 1 {
        return fill_str(b'#', MAX_ROMAN_LEN);
    }

    let numstr = number.to_string();
    let numstr = numstr.as_bytes();
    let mut len = numstr.len();
    let mut result: Vec<u8> = Vec::with_capacity(MAX_ROMAN_LEN + 1);

    for &ch in numstr.iter() {
        let mut num = ch as i32 - (b'0' as i32 + 1);
        if num < 0 {
            len -= 1;
            continue;
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

    number: Vec<u8>,
    number_p: usize,
    inout: Vec<u8>,
    inout_p: usize,
    last_relevant: Option<usize>,

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

    while !overload(np.inout_p) && is_c_space(np.inout_at(np.inout_p)) {
        np.inout_p += 1;
    }

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
                i += 1;
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

// need_locale under a non-C lc_monetary/lc_numeric loud-panics in
// pglc_localeconv; under C locale all conv strings are empty, so both arms
// produce the same defaults ('.'/','/' '/'-'/'+').
fn num_prepare_locale(np: &mut NumProc) {
    if np.num.need_locale != 0 {
        let l = ::pg_locale::pglc_localeconv();

        np.l_negative_sign = if !l.negative_sign.is_empty() {
            l.negative_sign.as_bytes().to_vec()
        } else {
            b"-".to_vec()
        };
        np.l_positive_sign = if !l.positive_sign.is_empty() {
            l.positive_sign.as_bytes().to_vec()
        } else {
            b"+".to_vec()
        };
        np.decimal = b".".to_vec();
        np.l_thousands_sep = b",".to_vec();
        np.l_currency_symbol = if !l.currency_symbol.is_empty() {
            l.currency_symbol.as_bytes().to_vec()
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

#[inline]
fn overload_test(np: &NumProc, input_len: usize) -> bool {
    np.inout_p >= input_len
}
#[inline]
fn amount_test(np: &NumProc, input_len: usize, s: usize) -> bool {
    np.inout_p <= input_len.saturating_sub(s)
}

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

fn write_number(np: &mut NumProc, c: u8) {
    if np.number_p >= np.number.len() {
        np.number.resize(np.number_p + 1, 0);
    }
    np.number[np.number_p] = c;
    np.number_p += 1;
}

fn is_predec_space(np: &NumProc) -> bool {
    !np.num.is_zero() && np.number_p == 0 && np.number_at(0) == b'0' && np.num.post != 0
}

fn inout_write(np: &mut NumProc, bytes: &[u8]) {
    let end = np.inout_p + bytes.len();
    if end > np.inout.len() {
        np.inout.resize(end, 0);
    }
    np.inout[np.inout_p..end].copy_from_slice(bytes);
    np.inout_p = end;
}

fn inout_put(np: &mut NumProc, c: u8) {
    if np.inout_p >= np.inout.len() {
        np.inout.resize(np.inout_p + 1, 0);
    }
    np.inout[np.inout_p] = c;
    np.inout_p += 1;
}

fn num_numpart_to_char(np: &mut NumProc, id: i32) -> PgResult<()> {
    if np.num.is_roman() {
        return Ok(());
    }

    np.num_in = false;

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
                let skip = np.last_relevant.is_some()
                    && np.number_p > np.last_relevant.unwrap()
                    && id != NUM_0;
                if skip {
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

pub struct NumProcessed {
    pub out: Vec<u8>,
}

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
            return Err(PgError::error("\"EEEE\" not supported for input".to_string())
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                .into());
        }
        return Ok(NumProcessed {
            out: cstr(&np.number),
        });
    }

    if is_to_char {
        np.sign = sign;
        if np.num.is_plus() || np.num.is_minus() {
            if np.num.is_plus() && !np.num.is_minus() {
                np.sign_wrote = false;
            } else {
                np.sign_wrote = true;
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
        np.sign = 0;
    }

    np.num_count = np.num.post + np.num.pre - 1;

    if is_to_char {
        np.out_pre_spaces = to_char_out_pre_spaces;

        if np.num.is_fillmode() && np.num.is_decimal() {
            np.last_relevant = get_last_relevant_decnum(&np.number);

            if np.last_relevant.is_some() && np.num.zero_end > np.out_pre_spaces {
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
        if np.number.len() < 2 {
            np.number.resize(2, 0);
        }
        np.number[0] = b' ';
        np.number[1] = 0;
    }

    np.num_in = false;
    np.num_curr = 0;

    num_prepare_locale(&mut np);

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
            match id {
                NUM_9 | NUM_0 | NUM_DEC | NUM_D => {
                    if np.is_to_char {
                        num_numpart_to_char(&mut np, id)?;
                        idx += 1;
                        continue;
                    } else {
                        num_numpart_from_char(&mut np, id, input_len);
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
                        let cnt = pg_mbstrlen(&pattern);
                        num_eat_non_data_chars(&mut np, cnt, input_len)?;
                        idx += 1;
                        continue;
                    }
                }
                NUM_RN | NUM_RN_LOWER => {
                    if np.is_to_char {
                        let number_p: Vec<u8> = if id == NUM_RN_LOWER {
                            asc_tolower(&cstr_from(&np.number, np.number_p))
                        } else {
                            cstr_from(&np.number, np.number_p)
                        };
                        if np.num.is_fillmode() {
                            inout_overlay(&mut np, &number_p);
                        } else {
                            let padded = fmt_pad_str(15, &String::from_utf8_lossy(&number_p));
                            inout_overlay(&mut np, padded.as_bytes());
                        }
                        let written = cstrlen(&np.inout[np.inout_p..]);
                        np.inout_p += written - 1;
                    } else {
                        let roman_result = roman_to_int(&mut np, input_len);
                        if roman_result < 0 {
                            return Err(PgError::error("invalid Roman numeral".to_string())
                                .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
                                .into());
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
                        continue;
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
            np.inout_p += 1;
        } else {
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
        Ok(NumProcessed {
            out: cstr(&np.inout[..np.inout_p.min(np.inout.len())]),
        })
    } else {
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

fn inout_set(np: &mut NumProc, c: u8) {
    if np.inout_p >= np.inout.len() {
        np.inout.resize(np.inout_p + 1, 0);
    }
    np.inout[np.inout_p] = c;
}

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

fn cstrlen(b: &[u8]) -> usize {
    b.iter().position(|&c| c == 0).unwrap_or(b.len())
}

fn cstr(b: &[u8]) -> Vec<u8> {
    b[..cstrlen(b)].to_vec()
}

fn cstr_from(b: &[u8], from: usize) -> Vec<u8> {
    if from >= b.len() {
        return Vec::new();
    }
    cstr(&b[from..])
}

fn cstr_node(buf: &[u8; MAX_MULTIBYTE_CHAR_LEN + 1]) -> Vec<u8> {
    cstr(buf)
}

// sprintf helpers used by the NUM to_char cores (C printf semantics).

pub(crate) fn fmt_pad_str(width: i32, s: &str) -> String {
    let target = width.unsigned_abs() as usize;
    let len = s.chars().count();
    if len >= target {
        return s.to_string();
    }
    let pad = target - len;
    let spaces: String = std::iter::repeat_n(' ', pad).collect();
    if width < 0 {
        format!("{s}{spaces}")
    } else {
        format!("{spaces}{s}")
    }
}

pub(crate) fn fmt_plus_e(prec: usize, val: f64) -> String {
    if val.is_nan() {
        return "NaN".to_string();
    }
    if val.is_infinite() {
        return if val.is_sign_negative() {
            "-Infinity".to_string()
        } else {
            "+Infinity".to_string()
        };
    }
    let neg = val.is_sign_negative();
    let s = format!("{:.*e}", prec, val.abs());
    let s = normalize_exponent(&s);
    if neg {
        format!("-{s}")
    } else {
        format!("+{s}")
    }
}

pub(crate) fn fmt_f(prec: usize, val: f64) -> String {
    if let Some(s) = special_float_text(val) {
        return s;
    }
    format!("{val:.prec$}")
}

pub(crate) fn fmt_f0(val: f64) -> String {
    if let Some(s) = special_float_text(val) {
        return s;
    }
    format!("{val:.0}")
}

fn special_float_text(val: f64) -> Option<String> {
    if val.is_nan() {
        Some("NaN".to_string())
    } else if val.is_infinite() {
        Some(if val.is_sign_negative() {
            "-Infinity".to_string()
        } else {
            "Infinity".to_string()
        })
    } else {
        None
    }
}

fn normalize_exponent(s: &str) -> String {
    if let Some(epos) = s.find(['e', 'E']) {
        let (mantissa, exp) = s.split_at(epos);
        let exp = &exp[1..];
        let (sign, digits) = if let Some(rest) = exp.strip_prefix('-') {
            ('-', rest)
        } else if let Some(rest) = exp.strip_prefix('+') {
            ('+', rest)
        } else {
            ('+', exp)
        };
        let digits = if digits.len() < 2 {
            format!("{digits:0>2}")
        } else {
            digits.to_string()
        };
        format!("{mantissa}e{sign}{digits}")
    } else {
        s.to_string()
    }
}
