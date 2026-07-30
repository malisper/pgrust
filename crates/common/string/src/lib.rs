// MCXT_ALLOC_NO_OOM would return None in C; Rust allocation aborts instead.
pub fn pg_clean_ascii(s: &str, _alloc_flags: i32) -> Option<String> {
    let mut dst = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if !(32..=126).contains(&b) {
            dst.push_str(&format!("\\x{b:02x}"));
        } else {
            dst.push(b as char);
        }
    }
    Some(dst)
}

/// C-locale `isspace()`: HT, LF, VT, FF, CR, SP.
///
/// Rust's `u8::is_ascii_whitespace` / `str::trim_ascii*` are NOT this set --
/// they omit VT (0x0b) -- and `char::is_whitespace` is not it either, because
/// it also accepts non-ASCII space code points.  Anything standing in for a C
/// `isspace()` call must use this.
#[inline]
pub const fn isspace_c_locale(b: u8) -> bool {
    b == b' ' || (b >= 0x09 && b <= 0x0d)
}

/// C: `strtoint(str, &endptr, 10)` (src/common/string.c) plus the
/// `endptr == str || *endptr != '\0' || errno != 0` rejection test that every
/// caller which demands a fully-consumed integer applies.  `None` is exactly
/// "C would have rejected this".
///
/// `s` is a raw text payload; C reaches these call sites through
/// `TextDatumGetCString`, so the C string ends at the first NUL and the
/// `*endptr != '\0'` test is against that NUL.
pub fn strtoint10_strict(s: &[u8]) -> Option<i32> {
    // TextDatumGetCString: the C string stops at the first NUL.
    let s = match s.iter().position(|&b| b == 0) {
        Some(n) => &s[..n],
        None => s,
    };

    let mut i = 0;
    // strtol: skip leading C-locale whitespace.
    while i < s.len() && isspace_c_locale(s[i]) {
        i += 1;
    }
    // strtol: at most one sign, with no whitespace after it.
    let neg = match s.get(i) {
        Some(b'-') => {
            i += 1;
            true
        }
        Some(b'+') => {
            i += 1;
            false
        }
        _ => false,
    };

    let digits_start = i;
    let mut acc: i64 = 0;
    let mut erange = false;
    while i < s.len() && s[i].is_ascii_digit() {
        if !erange {
            acc = acc * 10 + i64::from(s[i] - b'0');
            // Past the int magnitude strtoint reports ERANGE regardless of how
            // many more digits follow, so stop accumulating here.
            if acc > i64::from(i32::MAX) + 1 {
                erange = true;
            }
        }
        i += 1;
    }

    // endptr == str: strtol converted nothing.
    if i == digits_start {
        return None;
    }
    // *endptr != '\0': trailing junk, including trailing whitespace.
    if i != s.len() {
        return None;
    }
    // errno != 0: strtol's ERANGE, or strtoint's `val != (int) val` narrowing.
    if erange {
        return None;
    }
    let v = if neg { -acc } else { acc };
    if v < i64::from(i32::MIN) || v > i64::from(i32::MAX) {
        return None;
    }
    Some(v as i32)
}

pub fn init_seams() {
    string_seams::pg_clean_ascii::set(pg_clean_ascii);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passthrough_clean_ascii() {
        assert_eq!(pg_clean_ascii("psql", 0).unwrap(), "psql");
        assert_eq!(pg_clean_ascii("", 0).unwrap(), "");
    }

    /// Every expectation is the executed output of the real C
    /// `strtoint(s, &endptr, 10)` plus its callers'
    /// `endptr == s || *endptr != '\0' || errno != 0` reject, run under glibc.
    #[test]
    fn strtoint10_strict_matches_c_strtol() {
        // C-locale isspace is {09, 0a, 0b, 0c, 0d, 20}; all six are skipped.
        assert_eq!(strtoint10_strict(b"1"), Some(1));
        assert_eq!(strtoint10_strict(b" 1"), Some(1));
        assert_eq!(strtoint10_strict(b"\t1"), Some(1));
        assert_eq!(strtoint10_strict(b"\n1"), Some(1));
        assert_eq!(strtoint10_strict(b"\x0b1"), Some(1)); // VT: not Rust ws
        assert_eq!(strtoint10_strict(b"\x0c1"), Some(1)); // FF
        assert_eq!(strtoint10_strict(b"\r1"), Some(1));
        assert_eq!(strtoint10_strict(b" \t\n\x0b\x0c\r1"), Some(1));
        // ...and nothing else is.  U+00A0 and U+0085 are not C-locale space.
        assert_eq!(strtoint10_strict(b"\xc2\xa01"), None);
        assert_eq!(strtoint10_strict(b"\xc2\x851"), None);
        assert_eq!(strtoint10_strict(b"\x0e1"), None);
        assert_eq!(strtoint10_strict(b"\x081"), None);

        // One optional sign, immediately before the digits.
        assert_eq!(strtoint10_strict(b"+1"), Some(1));
        assert_eq!(strtoint10_strict(b"-1"), Some(-1));
        assert_eq!(strtoint10_strict(b" +1"), Some(1));
        assert_eq!(strtoint10_strict(b"\x0b-1"), Some(-1));
        assert_eq!(strtoint10_strict(b"-0"), Some(0));
        assert_eq!(strtoint10_strict(b"+ 1"), None);
        assert_eq!(strtoint10_strict(b"+\x0b1"), None);
        assert_eq!(strtoint10_strict(b"++1"), None);
        assert_eq!(strtoint10_strict(b"-+1"), None);
        assert_eq!(strtoint10_strict(b"+"), None);
        assert_eq!(strtoint10_strict(b"-"), None);

        // Decimal only: leading zeros are not octal, and there is no 0x prefix.
        assert_eq!(strtoint10_strict(b"007"), Some(7));
        assert_eq!(strtoint10_strict(b"-007"), Some(-7));
        assert_eq!(strtoint10_strict(b"010"), Some(10));
        assert_eq!(strtoint10_strict(b"0x10"), None);
        assert_eq!(strtoint10_strict(b"0b1"), None);

        // Nothing converted (endptr == s).
        assert_eq!(strtoint10_strict(b""), None);
        assert_eq!(strtoint10_strict(b" "), None);
        assert_eq!(strtoint10_strict(b"\x0b"), None);
        assert_eq!(strtoint10_strict(b"abc"), None);

        // Trailing junk, trailing whitespace included (*endptr != '\0').
        assert_eq!(strtoint10_strict(b"1 "), None);
        assert_eq!(strtoint10_strict(b"1\x0b"), None);
        assert_eq!(strtoint10_strict(b"1\t"), None);
        assert_eq!(strtoint10_strict(b"1a"), None);
        assert_eq!(strtoint10_strict(b"1.5"), None);
        assert_eq!(strtoint10_strict(b"1_000"), None);
        assert_eq!(strtoint10_strict(b"1\xff"), None);

        // strtol ERANGE / strtoint's `val != (int) val` narrowing.
        assert_eq!(strtoint10_strict(b"2147483647"), Some(i32::MAX));
        assert_eq!(strtoint10_strict(b"2147483648"), None);
        assert_eq!(strtoint10_strict(b"-2147483648"), Some(i32::MIN));
        assert_eq!(strtoint10_strict(b"-2147483649"), None);
        assert_eq!(strtoint10_strict(b"9223372036854775808"), None);
        assert_eq!(strtoint10_strict(b"99999999999999999999999"), None);
        assert_eq!(strtoint10_strict(b"-99999999999999999999999"), None);
        // Enough leading zeros to be in range despite the length.
        assert_eq!(strtoint10_strict(b"0000000000000000000005"), Some(5));

        // TextDatumGetCString: the C string ends at the first NUL.
        assert_eq!(strtoint10_strict(b"1\0junk"), Some(1));
        assert_eq!(strtoint10_strict(b"\0"), None);
    }

    #[test]
    fn isspace_c_locale_is_exactly_the_c_set() {
        let want: &[u8] = &[0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x20];
        for b in 0u8..=255 {
            assert_eq!(
                isspace_c_locale(b),
                want.contains(&b),
                "isspace_c_locale(0x{b:02x})"
            );
        }
        // The whole point: Rust's ASCII whitespace omits VT.
        assert!(isspace_c_locale(0x0b));
        assert!(!0x0bu8.is_ascii_whitespace());
    }

    #[test]
    fn hex_escapes_non_printables() {
        assert_eq!(pg_clean_ascii("a\x1fb", 0).unwrap(), "a\\x1fb");
        assert_eq!(pg_clean_ascii("\x7f", 0).unwrap(), "\\x7f");
        assert_eq!(pg_clean_ascii("caf\u{e9}", 0).unwrap(), "caf\\xc3\\xa9");
        assert_eq!(pg_clean_ascii("\t\n", 0).unwrap(), "\\x09\\x0a");
    }
}
