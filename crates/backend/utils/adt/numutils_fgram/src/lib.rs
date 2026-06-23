#![allow(non_snake_case)]

use error_fgram::{
    ereport, errsave, PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERROR,
};

pub fn pg_strtoint16(s: &str) -> PgResult<i16> {
    pg_strtoint16_safe(s, None)
}

pub fn pg_strtoint16_safe(s: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<i16> {
    parse_signed(s, "smallint", i16::MIN as i128, i16::MAX as u128, escontext)
        .map(|value| value as i16)
}

pub fn pg_strtoint32(s: &str) -> PgResult<i32> {
    pg_strtoint32_safe(s, None)
}

pub fn pg_strtoint32_safe(s: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<i32> {
    parse_signed(s, "integer", i32::MIN as i128, i32::MAX as u128, escontext)
        .map(|value| value as i32)
}

pub fn pg_strtoint64(s: &str) -> PgResult<i64> {
    pg_strtoint64_safe(s, None)
}

pub fn pg_strtoint64_safe(s: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<i64> {
    parse_signed(s, "bigint", i64::MIN as i128, i64::MAX as u128, escontext)
        .map(|value| value as i64)
}

pub fn uint32in_subr<'a>(
    s: &'a str,
    typname: &str,
    allow_trailing_junk: bool,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<(u32, &'a str)> {
    match parse_unsigned(s, u32::MAX as u128, typname, allow_trailing_junk) {
        Ok(result) => Ok((result.value as u32, result.rest)),
        Err(error) => soft_or_hard(escontext, error).map(|_| (0, "")),
    }
}

pub fn uint64in_subr<'a>(
    s: &'a str,
    typname: &str,
    allow_trailing_junk: bool,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<(u64, &'a str)> {
    match parse_unsigned(s, u64::MAX as u128, typname, allow_trailing_junk) {
        Ok(result) => Ok((result.value as u64, result.rest)),
        Err(error) => soft_or_hard(escontext, error).map(|_| (0, "")),
    }
}

pub fn pg_itoa(i: i16) -> String {
    pg_ltoa(i as i32)
}

pub fn pg_ultoa_n(value: u32) -> String {
    value.to_string()
}

pub fn pg_ltoa(value: i32) -> String {
    value.to_string()
}

pub fn pg_ulltoa_n(value: u64) -> String {
    value.to_string()
}

pub fn pg_lltoa(value: i64) -> String {
    value.to_string()
}

pub fn pg_ultostr_zeropad(value: u32, minwidth: i32) -> String {
    assert!(minwidth > 0);
    let value = value.to_string();
    let minwidth = minwidth as usize;
    if value.len() >= minwidth {
        value
    } else {
        let mut output = String::with_capacity(minwidth);
        output.extend(std::iter::repeat_n('0', minwidth - value.len()));
        output.push_str(&value);
        output
    }
}

pub fn pg_ultostr(value: u32) -> String {
    value.to_string()
}

fn parse_signed(
    s: &str,
    typname: &str,
    min: i128,
    max: u128,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<i128> {
    match parse_signed_inner(s, typname, min, max) {
        Ok(value) => Ok(value),
        Err(error) => soft_or_hard(escontext, error).map(|_| 0),
    }
}

fn parse_signed_inner(s: &str, typname: &str, min: i128, max: u128) -> PgResult<i128> {
    let bytes = s.as_bytes();
    let mut index = 0;

    while bytes.get(index).is_some_and(|byte| is_space(*byte)) {
        index += 1;
    }

    let mut neg = false;
    if bytes.get(index) == Some(&b'-') {
        neg = true;
        index += 1;
    } else if bytes.get(index) == Some(&b'+') {
        index += 1;
    }

    let (base, digit_start) = match (bytes.get(index), bytes.get(index + 1)) {
        (Some(b'0'), Some(b'x' | b'X')) => {
            index += 2;
            (16_u128, index)
        }
        (Some(b'0'), Some(b'o' | b'O')) => {
            index += 2;
            (8_u128, index)
        }
        (Some(b'0'), Some(b'b' | b'B')) => {
            index += 2;
            (2_u128, index)
        }
        _ => (10_u128, index),
    };

    let limit = if neg { min.unsigned_abs() } else { max };
    let mut value = 0_u128;
    let mut saw_digit = false;

    loop {
        let Some(byte) = bytes.get(index).copied() else {
            break;
        };

        if byte == b'_' {
            if !saw_digit {
                return Err(invalid_syntax(s, typname));
            }
            index += 1;
            let Some(next) = bytes.get(index).copied() else {
                return Err(invalid_syntax(s, typname));
            };
            if digit_value(next, base).is_none() {
                return Err(invalid_syntax(s, typname));
            }
            continue;
        }

        let Some(digit) = digit_value(byte, base) else {
            break;
        };
        saw_digit = true;

        if value > (limit - digit) / base {
            return Err(out_of_range(s, typname));
        }
        value = value * base + digit;
        index += 1;
    }

    if !saw_digit || index == digit_start {
        return Err(invalid_syntax(s, typname));
    }

    while bytes.get(index).is_some_and(|byte| is_space(*byte)) {
        index += 1;
    }

    if index != bytes.len() {
        return Err(invalid_syntax(s, typname));
    }

    if neg {
        if value == min.unsigned_abs() {
            Ok(min)
        } else {
            Ok(-(value as i128))
        }
    } else {
        Ok(value as i128)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct UnsignedParse<'a> {
    value: u128,
    rest: &'a str,
}

fn parse_unsigned<'a>(
    s: &'a str,
    max: u128,
    typname: &str,
    allow_trailing_junk: bool,
) -> PgResult<UnsignedParse<'a>> {
    let bytes = s.as_bytes();
    let mut index = 0;

    while bytes.get(index).is_some_and(|byte| is_space(*byte)) {
        index += 1;
    }

    let mut neg = false;
    if bytes.get(index) == Some(&b'-') {
        neg = true;
        index += 1;
    } else if bytes.get(index) == Some(&b'+') {
        index += 1;
    }

    // `uint{32,64}in_subr` use C `strtoul`/`strtou64` with base 0, NOT
    // `pg_strtoint_common`.  base-0 strtoul recognizes a `0x`/`0X` prefix as
    // hexadecimal and a *bare leading zero* as octal (e.g. "010" == 8); it does
    // NOT accept the `0o`/`0b` prefixes or `_` digit separators that the
    // signed `pg_strtoint*` parsers do.  Replicate that exactly.
    let (base, digit_start) = match (bytes.get(index), bytes.get(index + 1)) {
        (Some(b'0'), Some(b'x' | b'X')) => {
            index += 2;
            (16_u128, index)
        }
        // Bare leading zero: octal.  The leading `0` is itself an octal digit,
        // so a lone "0" parses to value 0 (digit_start points at the zero).
        (Some(b'0'), _) => (8_u128, index),
        _ => (10_u128, index),
    };

    let mut value = 0_u128;
    while let Some(byte) = bytes.get(index).copied() {
        let Some(digit) = digit_value(byte, base) else {
            break;
        };
        if value > (u128::MAX - digit) / base {
            return Err(out_of_range(s, typname));
        }
        value = value * base + digit;
        index += 1;
    }

    if index == digit_start {
        return Err(invalid_syntax(s, typname));
    }

    let endptr = index;
    if !allow_trailing_junk {
        while bytes.get(index).is_some_and(|byte| is_space(*byte)) {
            index += 1;
        }
        if index != bytes.len() {
            return Err(invalid_syntax(s, typname));
        }
    }

    if neg {
        let signed_min_abs = (max / 2) + 1;
        if value > signed_min_abs && max == u32::MAX as u128 {
            return Err(out_of_range(s, typname));
        }
        if value > max && max != u32::MAX as u128 {
            return Err(out_of_range(s, typname));
        }
        value = value.wrapping_neg_with_max(max);
    } else if value > max {
        return Err(out_of_range(s, typname));
    }

    Ok(UnsignedParse {
        value,
        rest: &s[endptr..],
    })
}

fn soft_or_hard(context: Option<&mut SoftErrorContext>, error: PgError) -> PgResult<()> {
    errsave(context, error)
}

fn invalid_syntax(input: &str, typname: &str) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INVALID_TEXT_REPRESENTATION)
        .errmsg(format!(
            "invalid input syntax for type {typname}: \"{input}\""
        ))
        .into_error()
}

fn out_of_range(input: &str, typname: &str) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
        .errmsg(format!(
            "value \"{input}\" is out of range for type {typname}"
        ))
        .into_error()
}

fn digit_value(byte: u8, base: u128) -> Option<u128> {
    let value = match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => return None,
    };
    (u128::from(value) < base).then_some(u128::from(value))
}

fn is_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

trait WrappingWithin {
    fn wrapping_neg_with_max(self, max: Self) -> Self;
}

impl WrappingWithin for u128 {
    fn wrapping_neg_with_max(self, max: Self) -> Self {
        if self == 0 {
            0
        } else {
            (max + 1) - self
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use error_fgram::{
        ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
    };

    #[test]
    fn signed_parsers_accept_postgres_bases_and_underscores() {
        assert_eq!(pg_strtoint16("32767").unwrap(), 32767);
        assert_eq!(pg_strtoint16("-32768").unwrap(), -32768);
        assert_eq!(pg_strtoint16("  +0x7fff  ").unwrap(), 32767);
        assert_eq!(pg_strtoint32("0o177777").unwrap(), 65_535);
        assert_eq!(pg_strtoint32("0b1010_0101").unwrap(), 165);
        assert_eq!(
            pg_strtoint64("9_223_372_036_854_775_807").unwrap(),
            i64::MAX
        );
        assert_eq!(
            pg_strtoint64("-9_223_372_036_854_775_808").unwrap(),
            i64::MIN
        );
    }

    #[test]
    fn signed_parsers_reject_invalid_underscore_and_trailing_junk() {
        let error = pg_strtoint32("_1").unwrap_err();
        assert_eq!(error.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
        assert_eq!(
            error.message(),
            "invalid input syntax for type integer: \"_1\""
        );

        let error = pg_strtoint32("1_").unwrap_err();
        assert_eq!(error.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);

        let error = pg_strtoint32("1x").unwrap_err();
        assert_eq!(
            error.message(),
            "invalid input syntax for type integer: \"1x\""
        );
    }

    #[test]
    fn signed_parsers_report_overflow_with_pg_messages() {
        let error = pg_strtoint16("32768").unwrap_err();
        assert_eq!(error.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        assert_eq!(
            error.message(),
            "value \"32768\" is out of range for type smallint"
        );

        let error = pg_strtoint32("-2147483649").unwrap_err();
        assert_eq!(
            error.message(),
            "value \"-2147483649\" is out of range for type integer"
        );
    }

    #[test]
    fn safe_signed_parser_saves_soft_error_and_returns_zero() {
        let mut context = SoftErrorContext::new(true);

        let value = pg_strtoint32_safe("bad", Some(&mut context)).unwrap();

        assert_eq!(value, 0);
        assert!(context.error_occurred());
        assert_eq!(
            context.error().unwrap().message(),
            "invalid input syntax for type integer: \"bad\""
        );
    }

    #[test]
    fn unsigned_parsers_handle_rest_and_trailing_whitespace() {
        assert_eq!(
            uint32in_subr("42 rest", "oid", true, None).unwrap(),
            (42, " rest")
        );
        assert_eq!(
            uint64in_subr("18446744073709551615  ", "xid", false, None).unwrap(),
            (u64::MAX, "  ")
        );
        assert_eq!(
            uint32in_subr("-1", "oid", false, None).unwrap(),
            (u32::MAX, "")
        );
        assert_eq!(
            uint64in_subr("-1", "xid", false, None).unwrap(),
            (u64::MAX, "")
        );
    }

    /// `uint{32,64}in_subr` mirror C `strtoul`/`strtou64` with base 0: a bare
    /// leading `0` is octal and a `0x` prefix is hexadecimal (unlike the signed
    /// `pg_strtoint*` parsers, which require `0o`/`0x`/`0b`).  These mirror the
    /// xid regression test (`src/test/regress/expected/xid.out`):
    /// `'010'::xid == 8`, `'0xffffffff'::xid == 4294967295`, etc.
    #[test]
    fn unsigned_parsers_use_strtoul_base0_octal_and_hex() {
        // Bare leading zero == octal (010 octal == 8 decimal).
        assert_eq!(uint32in_subr("010", "xid", false, None).unwrap(), (8, ""));
        assert_eq!(uint64in_subr("010", "xid8", false, None).unwrap(), (8, ""));
        // A lone "0" is octal zero.
        assert_eq!(uint32in_subr("0", "xid", false, None).unwrap(), (0, ""));
        // 0x / 0X prefix == hexadecimal.
        assert_eq!(
            uint32in_subr("0xffffffff", "xid", false, None).unwrap(),
            (u32::MAX, "")
        );
        assert_eq!(
            uint64in_subr("0xffffffffffffffff", "xid8", false, None).unwrap(),
            (u64::MAX, "")
        );
        // No 0o / 0b prefixes: the leading 0 is octal, so the trailing 'o'/'b'
        // is junk -> invalid syntax.
        assert!(uint32in_subr("0o17", "xid", false, None).is_err());
        assert!(uint32in_subr("0b101", "xid", false, None).is_err());
        // 08 is not a valid octal number ('8' is junk after octal '0').
        assert!(uint32in_subr("08", "xid", false, None).is_err());
    }

    #[test]
    fn unsigned_parsers_reject_invalid_or_out_of_range_input() {
        let error = uint32in_subr("12x", "oid", false, None).unwrap_err();
        assert_eq!(
            error.message(),
            "invalid input syntax for type oid: \"12x\""
        );

        let error = uint32in_subr("4294967296", "oid", false, None).unwrap_err();
        assert_eq!(
            error.message(),
            "value \"4294967296\" is out of range for type oid"
        );

        let error = uint32in_subr("-2147483649", "oid", false, None).unwrap_err();
        assert_eq!(
            error.message(),
            "value \"-2147483649\" is out of range for type oid"
        );
    }

    #[test]
    fn decimal_formatters_match_postgres_text() {
        assert_eq!(pg_itoa(-32768), "-32768");
        assert_eq!(pg_ultoa_n(4_294_967_295), "4294967295");
        assert_eq!(pg_ltoa(i32::MIN), "-2147483648");
        assert_eq!(pg_ulltoa_n(u64::MAX), "18446744073709551615");
        assert_eq!(pg_lltoa(i64::MIN), "-9223372036854775808");
        assert_eq!(pg_ultostr_zeropad(42, 5), "00042");
        assert_eq!(pg_ultostr(42), "42");
    }
}
