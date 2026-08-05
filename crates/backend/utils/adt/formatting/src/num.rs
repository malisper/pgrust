//! NUM (number) format-picture engine: roman conversion, locale prep,
//! NUM_numpart_from_char / NUM_numpart_to_char, and the NUM_processor driver
//! split by direction: to_char writes through a raw cursor into the caller's
//! presized output (C's single palloc0), from_char reads the value slice.

use ::types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_TEXT_REPRESENTATION,
};

use crate::case::{asc_tolower, get_th, pg_ascii_toupper};
use crate::parse::is_c_space;
use crate::tables::*;

fn pg_mbstrlen(s: &[u8]) -> i32 {
    mbutils::pg_mbstrlen_with_len(s).unwrap_or(s.len() as i32)
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

struct NumLocale {
    negative: &'static [u8],
    positive: &'static [u8],
    decimal: &'static [u8],
    thousands: &'static [u8],
    currency: &'static [u8],
}

// NUM_prepare_locale (formatting.c). Under the C locale every conv string is
// empty, so both arms fall through to the same defaults ('.'/','/' '/'-'/'+');
// under a real locale pglc_localeconv now supplies the LC_NUMERIC separators
// too, so D/G follow the locale the way C's do.
fn num_prepare_locale(num: &NUMDesc) -> ::types_error::PgResult<NumLocale> {
    Ok(if num.need_locale != 0 {
        let l = ::pg_locale::pglc_localeconv()?;
        // C's order matters: the thousands fallback is chosen AGAINST the
        // already-resolved decimal point, so a locale that uses ',' for the
        // decimal gets '.' for grouping rather than a separator collision.
        let decimal: &'static [u8] = if !l.decimal_point.is_empty() {
            l.decimal_point.as_bytes()
        } else {
            b"."
        };
        let thousands: &'static [u8] = if !l.thousands_sep.is_empty() {
            l.thousands_sep.as_bytes()
        } else if decimal != b"," {
            b","
        } else {
            b"."
        };
        NumLocale {
            negative: if !l.negative_sign.is_empty() {
                l.negative_sign.as_bytes()
            } else {
                b"-"
            },
            positive: if !l.positive_sign.is_empty() {
                l.positive_sign.as_bytes()
            } else {
                b"+"
            },
            decimal,
            thousands,
            currency: if !l.currency_symbol.is_empty() {
                l.currency_symbol.as_bytes()
            } else {
                b" "
            },
        }
    } else {
        NumLocale {
            negative: b"-",
            positive: b"+",
            decimal: b".",
            thousands: b",",
            currency: b" ",
        }
    })
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

fn cstrlen(b: &[u8]) -> usize {
    b.iter().position(|&c| c == 0).unwrap_or(b.len())
}

fn cstr_slice(b: &[u8]) -> &[u8] {
    &b[..cstrlen(b)]
}

struct NumToChar<'a> {
    num: &'a mut NUMDesc,

    sign: i32,
    sign_wrote: bool,
    num_count: i32,
    num_in: bool,
    num_curr: i32,
    out_pre_spaces: i32,

    number: &'a [u8],
    number_p: usize,
    out: &'a mut [u8],
    out_p: usize,
    nul_at: usize,
    last_relevant: Option<usize>,

    loc: NumLocale,
}

impl NumToChar<'_> {
    #[inline]
    fn number_at(&self, i: usize) -> u8 {
        // number is NUL-terminated (asserted by the driver) and every cursor
        // stops at the NUL, so i stays in bounds — C's raw char* read.
        debug_assert!(i < self.number.len());
        unsafe { *self.number.get_unchecked(i) }
    }

    // SAFETY (all writers): out is sized nodes * NUM_MAX_ITEM_SIZ + 1 and every
    // node writes <= NUM_MAX_ITEM_SIZ bytes (C's NUM_processor buffer contract,
    // NUM_TOCHAR_prepare); debug_asserts re-prove it per write.
    #[inline]
    fn set(&mut self, c: u8) {
        debug_assert!(self.out_p < self.out.len());
        unsafe { *self.out.get_unchecked_mut(self.out_p) = c }
    }

    #[inline]
    fn put(&mut self, c: u8) {
        self.set(c);
        self.out_p += 1;
    }

    #[inline]
    fn overlay(&mut self, bytes: &[u8]) {
        debug_assert!(self.out_p + bytes.len() <= self.out.len());
        unsafe {
            core::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.out.as_mut_ptr().add(self.out_p),
                bytes.len(),
            )
        }
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        self.overlay(bytes);
        self.out_p += bytes.len();
    }

    #[inline]
    fn overlay_spaces(&mut self, n: usize) {
        debug_assert!(self.out_p + n <= self.out.len());
        unsafe { core::ptr::write_bytes(self.out.as_mut_ptr().add(self.out_p), b' ', n) }
    }

    fn is_predec_space(&self) -> bool {
        !self.num.is_zero() && self.number_p == 0 && self.number_at(0) == b'0' && self.num.post != 0
    }

    fn last_relevant_is_dot(&self) -> bool {
        self.last_relevant
            .map(|lr| self.number_at(lr) == b'.')
            .unwrap_or(false)
    }

    // Single call site; inlining removes the dominant prologue/spill cost
    // (20% of the lane) — C's static NUM_numpart_to_char inlines the same way.
    #[inline(always)]
    fn numpart(&mut self, id: i32) {
        if self.num.is_roman() {
            return;
        }

        self.num_in = false;

        if !self.sign_wrote
            && (self.num_curr >= self.out_pre_spaces
                || (self.num.is_zero() && self.num.zero_start == self.num_curr))
            && (!self.is_predec_space() || self.last_relevant_is_dot())
        {
            if self.num.is_lsign() {
                if self.num.lsign == NUM_LSIGN_PRE {
                    let s = if self.sign == b'-' as i32 {
                        self.loc.negative
                    } else {
                        self.loc.positive
                    };
                    self.write(s);
                    self.sign_wrote = true;
                }
            } else if self.num.is_bracket() {
                let c = if self.sign == b'+' as i32 { b' ' } else { b'<' };
                self.put(c);
                self.sign_wrote = true;
            } else if self.sign == b'+' as i32 {
                if !self.num.is_fillmode() {
                    self.put(b' ');
                }
                self.sign_wrote = true;
            } else if self.sign == b'-' as i32 {
                self.put(b'-');
                self.sign_wrote = true;
            }
        }

        if id == NUM_9 || id == NUM_0 || id == NUM_D || id == NUM_DEC {
            if self.num_curr < self.out_pre_spaces
                && (self.num.zero_start > self.num_curr || !self.num.is_zero())
            {
                if !self.num.is_fillmode() {
                    self.put(b' ');
                }
            } else if self.num.is_zero()
                && self.num_curr < self.out_pre_spaces
                && self.num.zero_start <= self.num_curr
            {
                self.put(b'0');
                self.num_in = true;
            } else {
                if self.number_at(self.number_p) == b'.' {
                    let lr_is_dot = self.last_relevant_is_dot();
                    if self.last_relevant.is_none() || !lr_is_dot {
                        let dec = self.loc.decimal;
                        self.write(dec);
                    } else if self.num.is_fillmode() && lr_is_dot {
                        let dec = self.loc.decimal;
                        self.write(dec);
                    }
                } else {
                    let skip = self.last_relevant.is_some()
                        && self.number_p > self.last_relevant.unwrap()
                        && id != NUM_0;
                    if skip {
                    } else if self.is_predec_space() {
                        if !self.num.is_fillmode() {
                            self.put(b' ');
                        } else if self.last_relevant_is_dot() {
                            self.put(b'0');
                        }
                    } else {
                        let c = self.number_at(self.number_p);
                        self.put(c);
                        self.num_in = true;
                        // Sole writer of a NUL (number exhausted before the
                        // picture); records C's strlen cut without the scan.
                        if c == 0 && self.nul_at == usize::MAX {
                            self.nul_at = self.out_p - 1;
                        }
                    }
                }
                if self.number_at(self.number_p) != 0 {
                    self.number_p += 1;
                }
            }

            let mut end = self.num_count
                + (if self.out_pre_spaces != 0 { 1 } else { 0 })
                + (if self.num.is_decimal() { 1 } else { 0 });

            if let Some(lr) = self.last_relevant {
                if lr == self.number_p {
                    end = self.num_curr;
                }
            }

            if self.num_curr + 1 == end {
                if self.sign_wrote && self.num.is_bracket() {
                    let c = if self.sign == b'+' as i32 { b' ' } else { b'>' };
                    self.put(c);
                } else if self.num.is_lsign() && self.num.lsign == NUM_LSIGN_POST {
                    let s = if self.sign == b'-' as i32 {
                        self.loc.negative
                    } else {
                        self.loc.positive
                    };
                    self.write(s);
                }
            }
        }

        self.num_curr += 1;
    }
}

pub fn num_processor_to_char(
    nodes: &[FormatNode],
    num: &mut NUMDesc,
    out: &mut [u8],
    number: &[u8],
    to_char_out_pre_spaces: i32,
    sign: i32,
) -> PgResult<usize> {
    // NUL termination backs number_at's unchecked reads (C's numstr shape).
    assert!(number.last() == Some(&0), "numstr must be NUL-terminated");

    if num.zero_start != 0 {
        num.zero_start -= 1;
    }

    if num.is_eeee() {
        let n = cstrlen(number);
        out[..n].copy_from_slice(&number[..n]);
        return Ok(n);
    }

    let mut np = NumToChar {
        num,
        sign,
        sign_wrote: false,
        num_count: 0,
        num_in: false,
        num_curr: 0,
        out_pre_spaces: to_char_out_pre_spaces,
        number,
        number_p: 0,
        out,
        out_p: 0,
        nul_at: usize::MAX,
        last_relevant: None,
        loc: NumLocale {
            negative: b"",
            positive: b"",
            decimal: b"",
            thousands: b"",
            currency: b"",
        },
    };

    if np.num.is_plus() || np.num.is_minus() {
        np.sign_wrote = !(np.num.is_plus() && !np.num.is_minus());
    } else {
        if np.sign != b'-' as i32 && np.num.is_fillmode() {
            np.num.flag &= !NUM_F_BRACKET;
        }
        np.sign_wrote = np.sign == b'+' as i32 && np.num.is_fillmode() && !np.num.is_lsign();
        if np.num.lsign == NUM_LSIGN_PRE && np.num.pre == np.num.pre_lsign_num {
            np.num.lsign = NUM_LSIGN_POST;
        }
    }

    np.num_count = np.num.post + np.num.pre - 1;

    if np.num.is_fillmode() && np.num.is_decimal() {
        np.last_relevant = get_last_relevant_decnum(np.number);

        if np.last_relevant.is_some() && np.num.zero_end > np.out_pre_spaces {
            let nlen = cstrlen(np.number);
            let last_zero_pos = (nlen as i32 - 1).min(np.num.zero_end - np.out_pre_spaces) as usize;
            if np.last_relevant.unwrap() < last_zero_pos {
                np.last_relevant = Some(last_zero_pos);
            }
        }
    }

    if !np.sign_wrote && np.out_pre_spaces == 0 {
        np.num_count += 1;
    }

    np.loc = num_prepare_locale(np.num)?;

    for n in nodes {
        if n.typ == NODE_TYPE_END {
            break;
        }
        if n.typ != NODE_TYPE_ACTION {
            let cs = cstr_slice(&n.character);
            np.write(cs);
            continue;
        }
        let id = NUM_KEYWORDS[n.key as usize].id;
        match id {
            NUM_9 | NUM_0 | NUM_DEC | NUM_D => {
                np.numpart(id);
                continue;
            }
            NUM_COMMA => {
                if !np.num_in {
                    if np.num.is_fillmode() {
                        continue;
                    } else {
                        np.set(b' ');
                    }
                } else {
                    np.set(b',');
                }
            }
            NUM_G => {
                if !np.num_in {
                    if np.num.is_fillmode() {
                        continue;
                    } else {
                        let pattern_len = pg_mbstrlen(np.loc.thousands) as usize;
                        np.overlay_spaces(pattern_len);
                        np.out_p += pattern_len - 1;
                    }
                } else {
                    let pattern = np.loc.thousands;
                    np.overlay(pattern);
                    np.out_p += pattern.len() - 1;
                }
            }
            NUM_L => {
                let pattern = np.loc.currency;
                np.overlay(pattern);
                np.out_p += pattern.len() - 1;
            }
            NUM_RN | NUM_RN_LOWER => {
                let tail = if np.number_p < np.number.len() {
                    cstr_slice(&np.number[np.number_p..])
                } else {
                    b""
                };
                let lowered;
                let roman: &[u8] = if id == NUM_RN_LOWER {
                    lowered = asc_tolower(tail);
                    &lowered
                } else {
                    tail
                };
                let written = if np.num.is_fillmode() {
                    np.overlay(roman);
                    roman.len()
                } else {
                    // C: sprintf("%15s", ...); roman text is ASCII.
                    let pad = 15usize.saturating_sub(roman.len());
                    np.overlay_spaces(pad);
                    np.out_p += pad;
                    np.overlay(roman);
                    np.out_p -= pad;
                    pad + roman.len()
                };
                np.out_p += written - 1;
            }
            NUM_TH_LOWER_ID => {
                if np.num.is_roman()
                    || np.number_at(0) == b'#'
                    || np.sign == b'-' as i32
                    || np.num.is_decimal()
                {
                    continue;
                }
                let th = get_th(cstr_slice(np.number), TH_LOWER)?;
                np.overlay(th.as_bytes());
                np.out_p += 1;
            }
            NUM_TH => {
                if np.num.is_roman()
                    || np.number_at(0) == b'#'
                    || np.sign == b'-' as i32
                    || np.num.is_decimal()
                {
                    continue;
                }
                let th = get_th(cstr_slice(np.number), TH_UPPER)?;
                np.overlay(th.as_bytes());
                np.out_p += 1;
            }
            NUM_MI => {
                if np.sign == b'-' as i32 {
                    np.set(b'-');
                } else if np.num.is_fillmode() {
                    continue;
                } else {
                    np.set(b' ');
                }
            }
            NUM_PL => {
                if np.sign == b'+' as i32 {
                    np.set(b'+');
                } else if np.num.is_fillmode() {
                    continue;
                } else {
                    np.set(b' ');
                }
            }
            NUM_SG => {
                let sg = np.sign as u8;
                np.set(sg);
                // sign is 0 on the roman path — C's strlen cuts there.
                if sg == 0 && np.nul_at == usize::MAX {
                    np.nul_at = np.out_p;
                }
            }
            _ => continue,
        }
        np.out_p += 1;
    }

    // C's final strlen: nul_at is the only interior NUL ever written.
    Ok(np.nul_at.min(np.out_p).min(np.out.len()))
}

struct NumFromChar<'a> {
    num: &'a mut NUMDesc,

    num_in: bool,
    read_dec: bool,
    read_post: i32,
    read_pre: i32,

    inout: &'a [u8],
    inout_p: usize,
    number: &'a mut Vec<u8>,
    number_p: usize,

    loc: NumLocale,
}

impl NumFromChar<'_> {
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

    #[inline]
    fn overload(&self) -> bool {
        self.inout_p >= self.inout.len()
    }

    #[inline]
    fn amount_test(&self, s: usize) -> bool {
        self.inout_p <= self.inout.len().saturating_sub(s)
    }

    fn write_number(&mut self, c: u8) {
        if self.number_p >= self.number.len() {
            self.number.resize(self.number_p + 1, 0);
        }
        self.number[self.number_p] = c;
        self.number_p += 1;
    }

    fn eat_non_data_chars(&mut self, mut n: i32) {
        while n > 0 {
            n -= 1;
            if self.overload() {
                break;
            }
            if b"0123456789.,+-".contains(&self.inout_at(self.inout_p)) {
                break;
            }
            self.inout_p += pg_mblen_range(&self.inout[self.inout_p..]) as usize;
        }
    }

    fn numpart(&mut self, id: i32) {
        let mut isread = false;

        if self.overload() {
            return;
        }

        if self.inout_at(self.inout_p) == b' ' {
            self.inout_p += 1;
        }

        if self.overload() {
            return;
        }

        if self.number_at(0) == b' '
            && (id == NUM_0 || id == NUM_9)
            && (self.read_pre + self.read_post) == 0
        {
            if self.num.is_lsign() && self.num.lsign == NUM_LSIGN_PRE {
                let xn = self.loc.negative.len();
                let xp = self.loc.positive.len();
                if xn != 0
                    && self.amount_test(xn)
                    && self.inout[self.inout_p..self.inout_p + xn] == self.loc.negative[..]
                {
                    self.inout_p += xn;
                    self.number[0] = b'-';
                } else if xp != 0
                    && self.amount_test(xp)
                    && self.inout[self.inout_p..self.inout_p + xp] == self.loc.positive[..]
                {
                    self.inout_p += xp;
                    self.number[0] = b'+';
                }
            } else {
                let c = self.inout_at(self.inout_p);
                if c == b'-' || (self.num.is_bracket() && c == b'<') {
                    self.number[0] = b'-';
                    self.inout_p += 1;
                } else if c == b'+' {
                    self.number[0] = b'+';
                    self.inout_p += 1;
                }
            }
        }

        if self.overload() {
            return;
        }

        if self.inout_at(self.inout_p).is_ascii_digit() {
            if self.read_dec && self.read_post == self.num.post {
                return;
            }
            let c = self.inout_at(self.inout_p);
            self.write_number(c);
            if self.read_dec {
                self.read_post += 1;
            } else {
                self.read_pre += 1;
            }
            isread = true;
        } else if self.num.is_decimal() && !self.read_dec {
            let x = self.loc.decimal.len();
            if x != 0
                && self.amount_test(x)
                && self.inout[self.inout_p..self.inout_p + x] == self.loc.decimal[..]
            {
                self.inout_p += x - 1;
                self.write_number(b'.');
                self.read_dec = true;
                isread = true;
            }
        }

        if self.overload() {
            return;
        }

        if self.number_at(0) == b' ' && self.read_pre + self.read_post > 0 {
            if self.num.is_lsign()
                && isread
                && (self.inout_p + 1) < self.inout.len()
                && !self.inout_at(self.inout_p + 1).is_ascii_digit()
            {
                let tmp = self.inout_p;
                self.inout_p += 1;
                let xn = self.loc.negative.len();
                let xp = self.loc.positive.len();
                if xn != 0
                    && self.amount_test(xn)
                    && self.inout[self.inout_p..self.inout_p + xn] == self.loc.negative[..]
                {
                    self.inout_p += xn - 1;
                    self.number[0] = b'-';
                } else if xp != 0
                    && self.amount_test(xp)
                    && self.inout[self.inout_p..self.inout_p + xp] == self.loc.positive[..]
                {
                    self.inout_p += xp - 1;
                    self.number[0] = b'+';
                }
                if self.number_at(0) == b' ' {
                    self.inout_p = tmp;
                }
            } else if !isread
                && !self.num.is_lsign()
                && (self.num.is_plus() || self.num.is_minus())
            {
                let c = self.inout_at(self.inout_p);
                if c == b'-' || c == b'+' {
                    self.number[0] = c;
                }
            }
        }
    }

    fn roman_to_int(&mut self) -> i32 {
        let mut result = 0i32;
        let mut roman_chars = [0u8; MAX_ROMAN_LEN];
        let mut roman_values = [0i32; MAX_ROMAN_LEN];
        let mut repeat_count = 1;
        let mut v_count = 0;
        let mut l_count = 0;
        let mut d_count = 0;
        let mut subtraction_encountered = false;
        let mut last_subtracted_value = 0;

        while !self.overload() && is_c_space(self.inout_at(self.inout_p)) {
            self.inout_p += 1;
        }

        let mut len = 0usize;
        while len < MAX_ROMAN_LEN && !self.overload() {
            let curr_char = pg_ascii_toupper(self.inout_at(self.inout_p));
            let curr_value = roman_val(curr_char);
            if curr_value == 0 {
                break;
            }
            roman_chars[len] = curr_char;
            roman_values[len] = curr_value;
            self.inout_p += 1;
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
}

pub fn num_processor_from_char(
    nodes: &[FormatNode],
    num: &mut NUMDesc,
    value: &[u8],
    number: &mut Vec<u8>,
) -> PgResult<usize> {
    if num.zero_start != 0 {
        num.zero_start -= 1;
    }

    if num.is_eeee() {
        return Err(PgError::error("\"EEEE\" not supported for input".to_string())
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
            .into());
    }

    if number.len() < 2 {
        number.resize(2, 0);
    }
    number[0] = b' ';
    number[1] = 0;

    let mut np = NumFromChar {
        num,
        num_in: false,
        read_dec: false,
        read_post: 0,
        read_pre: 0,
        inout: value,
        inout_p: 0,
        number,
        number_p: 1,
        loc: NumLocale {
            negative: b"",
            positive: b"",
            decimal: b"",
            thousands: b"",
            currency: b"",
        },
    };

    np.loc = num_prepare_locale(np.num)?;

    let mut idx = 0usize;
    while nodes[idx].typ != NODE_TYPE_END {
        let n = &nodes[idx];

        if np.overload() {
            break;
        }

        if n.typ == NODE_TYPE_ACTION {
            let id = NUM_KEYWORDS[n.key as usize].id;
            match id {
                NUM_9 | NUM_0 | NUM_DEC | NUM_D => {
                    np.numpart(id);
                }
                NUM_COMMA => {
                    if !np.num_in && np.num.is_fillmode() {
                        idx += 1;
                        continue;
                    }
                    if np.inout_at(np.inout_p) != b',' {
                        idx += 1;
                        continue;
                    }
                }
                NUM_G => {
                    if !np.num_in && np.num.is_fillmode() {
                        idx += 1;
                        continue;
                    }
                    let pattern = np.loc.thousands;
                    let pattern_len = pattern.len();
                    if np.amount_test(pattern_len)
                        && np.inout[np.inout_p..np.inout_p + pattern_len] == pattern[..]
                    {
                        np.inout_p += pattern_len - 1;
                    } else {
                        idx += 1;
                        continue;
                    }
                }
                NUM_L => {
                    let cnt = pg_mbstrlen(np.loc.currency);
                    np.eat_non_data_chars(cnt);
                    idx += 1;
                    continue;
                }
                NUM_RN | NUM_RN_LOWER => {
                    let roman_result = np.roman_to_int();
                    if roman_result < 0 {
                        return Err(PgError::error("invalid Roman numeral".to_string())
                            .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
                            .into());
                    }
                    let digits = roman_result.to_string();
                    let numlen = digits.len();
                    let npp = np.number_p;
                    if npp + numlen > np.number.len() {
                        np.number.resize(npp + numlen, 0);
                    }
                    np.number[npp..npp + numlen].copy_from_slice(digits.as_bytes());
                    np.number_p += numlen;
                    np.num.pre = numlen as i32;
                    np.num.post = 0;
                    idx += 1;
                    continue;
                }
                NUM_TH_LOWER_ID | NUM_TH => {
                    if np.num.is_roman()
                        || np.number_at(0) == b'#'
                        || np.num.is_decimal()
                    {
                        idx += 1;
                        continue;
                    }
                    np.eat_non_data_chars(2);
                    idx += 1;
                    continue;
                }
                NUM_MI => {
                    if np.inout_at(np.inout_p) == b'-' {
                        np.number[0] = b'-';
                    } else {
                        np.eat_non_data_chars(1);
                        idx += 1;
                        continue;
                    }
                }
                NUM_PL => {
                    if np.inout_at(np.inout_p) == b'+' {
                        np.number[0] = b'+';
                    } else {
                        np.eat_non_data_chars(1);
                        idx += 1;
                        continue;
                    }
                }
                NUM_SG => {
                    let c = np.inout_at(np.inout_p);
                    if c == b'-' {
                        np.number[0] = b'-';
                    } else if c == b'+' {
                        np.number[0] = b'+';
                    } else {
                        np.eat_non_data_chars(1);
                        idx += 1;
                        continue;
                    }
                }
                _ => {
                    idx += 1;
                    continue;
                }
            }
            np.inout_p += 1;
        } else {
            np.inout_p += pg_mblen_range(&np.inout[np.inout_p..]) as usize;
            idx += 1;
            continue;
        }

        idx += 1;
    }

    if np.number_p >= 1 && np.number_at(np.number_p - 1) == b'.' {
        np.number[np.number_p - 1] = 0;
    } else if np.number_p < np.number.len() {
        np.number[np.number_p] = 0;
    } else {
        np.number.push(0);
    }
    np.num.post = np.read_post;
    let n = cstrlen(np.number);
    Ok(n)
}

// sprintf helpers used by the NUM/DCH to_char cores (C printf semantics).

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
