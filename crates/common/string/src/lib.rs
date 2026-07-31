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

/// Result of [`strtoul_base0`]; mirrors what a C caller can observe from
/// `strtoul`/`strtou64`: the return value, the endptr offset, and whether
/// ERANGE was set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StrtoulBase0 {
    /// The C return value.  0 when nothing converted; `u64::MAX` on ERANGE
    /// (glibc saturates to ULONG_MAX regardless of sign); otherwise the
    /// parsed magnitude, negated with wrapping if a `-` sign was present
    /// (`"-1"` -> `u64::MAX`, no ERANGE).
    pub value: u64,
    /// endptr offset: bytes consumed from the start of `s`.  0 == "no
    /// conversion" (C leaves `*endptr == nptr`).
    pub consumed: usize,
    /// glibc set ERANGE: the parsed magnitude exceeded `u64::MAX`.
    pub range_err: bool,
}

/// C: `strtoul(s, endptr, 0)` == `strtou64(s, endptr, 0)` on the 64-bit
/// glibc targets the server runs on (`unsigned long` is `uint64`).
///
/// Base 0 semantics: skip leading C-locale whitespace ([`isspace_c_locale`],
/// VT/FF included), one optional `+`/`-`, then `0x`/`0X` + hex digit ->
/// base 16, else leading `0` -> base 8, else base 10.  Trailing garbage is
/// NOT an error (callers passing NULL endptr simply ignore it: `"123abc"`
/// -> 123).  A minus sign is ACCEPTED and the value wraps modulo 2^64
/// without ERANGE unless the magnitude itself overflows u64.
///
/// glibc errno contract (verified by execution against PostgreSQL 18.4 on
/// Debian glibc): NO errno is set on no-conversion — `"abc"` returns 0 with
/// `consumed == 0` and `range_err == false`; EINVAL only fires for an
/// invalid `base` argument, which 0 is not.  So a C caller's
/// `errno == EINVAL || errno == ERANGE` reject test maps to `range_err`
/// alone, and garbage input "successfully" parses as 0.  Do not "improve"
/// on this — behavioral identity with the C call sites is the contract.
pub fn strtoul_base0(s: &[u8]) -> StrtoulBase0 {
    // C strings end at the first NUL.
    let s = match s.iter().position(|&b| b == 0) {
        Some(n) => &s[..n],
        None => s,
    };

    let mut i = 0;
    while i < s.len() && isspace_c_locale(s[i]) {
        i += 1;
    }
    let mut neg = false;
    match s.get(i) {
        Some(b'-') => {
            neg = true;
            i += 1;
        }
        Some(b'+') => i += 1,
        _ => {}
    }

    // Base detection.  "0x" NOT followed by a hex digit parses as the
    // number 0 with endptr after the "0" (glibc behavior).
    let base: u64 = if s.get(i) == Some(&b'0')
        && matches!(s.get(i + 1), Some(b'x') | Some(b'X'))
        && s.get(i + 2).is_some_and(|b| b.is_ascii_hexdigit())
    {
        i += 2;
        16
    } else if s.get(i) == Some(&b'0') {
        8
    } else {
        10
    };

    let digits_start = i;
    let mut acc: u64 = 0;
    let mut range_err = false;
    while i < s.len() {
        let d = match s[i] {
            b @ b'0'..=b'9' => u64::from(b - b'0'),
            b @ b'a'..=b'f' if base == 16 => u64::from(b - b'a' + 10),
            b @ b'A'..=b'F' if base == 16 => u64::from(b - b'A' + 10),
            _ => break,
        };
        if d >= base {
            break; // '8'/'9' terminate an octal number
        }
        if !range_err {
            match acc.checked_mul(base).and_then(|v| v.checked_add(d)) {
                Some(v) => acc = v,
                None => range_err = true,
            }
        }
        i += 1;
    }

    if i == digits_start {
        // No conversion: value 0, endptr == nptr, errno untouched.
        return StrtoulBase0 { value: 0, consumed: 0, range_err: false };
    }
    let value = if range_err {
        u64::MAX // ULONG_MAX regardless of sign
    } else if neg {
        acc.wrapping_neg()
    } else {
        acc
    };
    StrtoulBase0 { value, consumed: i, range_err }
}

/// C: `SplitGUCList(rawstring, separator, &namelist)` (varlena.c:3829).
///
/// Splits a GUC_LIST_QUOTE-style list. Items are either double-quoted (quote
/// pairs `""` collapse to one literal `"`; embedded separators and whitespace
/// stay in the item; an empty `""` item is legal) or unquoted runs that end at
/// the separator or at whitespace. After an item and its trailing whitespace,
/// the next byte must be the separator or end-of-string — whitespace is NOT
/// itself a separator, so `data wal` is a syntax error, exactly as in C
/// (verified against postgres:18.3: FATAL 'invalid value for parameter
/// "debug_io_direct": "data wal"' / DETAIL 'Invalid list syntax...').
/// Empty unquoted items (`a,,b`, trailing `a,`) are syntax errors. No
/// downcasing, no truncation. `Err(())` is C's `false` return.
///
/// The whitespace set is C's `scanner_isspace` (scansup.c) = space, \t, \n,
/// \r, \v, \f — identical to [`isspace_c_locale`].
pub fn split_guc_list(raw: &str, separator: u8) -> Result<Vec<String>, ()> {
    split_list_common(raw, separator, false)
}

/// C: `SplitDirectoriesString(rawstring, separator, &namelist)`
/// (varlena.c:3708), minus the trailing `canonicalize_path()` C applies to
/// each extracted name — callers apply `pg_path::canonicalize_path` to each
/// returned item to complete the C behavior (pg_string stays dependency-free).
///
/// Differs from [`split_guc_list`] only in the unquoted-item rule: an
/// unquoted name extends to the separator or end of string, so embedded
/// whitespace is allowed; trailing whitespace is excluded from the name.
/// Quoting, quote-pair collapsing, empty-item rejection ('a,,b' and 'a,' are
/// syntax errors) and the empty-input fast path are the same. C truncates
/// each name to MAXPGPATH-1 (1023) bytes; we do too (backing up to a char
/// boundary, since C's mid-UTF-8 cut is unrepresentable in a `String`).
pub fn split_directories_string(raw: &str, separator: u8) -> Result<Vec<String>, ()> {
    // MAXPGPATH (pg_config_manual.h) — keep in sync with pg_path::MAXPGPATH.
    const MAXPGPATH: usize = 1024;
    let mut list = split_list_common(raw, separator, true)?;
    for name in &mut list {
        if name.len() >= MAXPGPATH {
            let mut end = MAXPGPATH - 1;
            while !name.is_char_boundary(end) {
                end -= 1;
            }
            name.truncate(end);
        }
    }
    Ok(list)
}

/// Shared body of SplitGUCList / SplitDirectoriesString: the two C functions
/// are line-for-line identical except for where an unquoted name ends
/// (`whitespace_ends_unquoted`) and the caller-side truncate/canonicalize
/// post-passes.
fn split_list_common(
    raw: &str,
    separator: u8,
    unquoted_may_contain_whitespace: bool,
) -> Result<Vec<String>, ()> {
    let s = raw.as_bytes();
    let mut list = Vec::new();
    let mut p = 0usize;

    while p < s.len() && isspace_c_locale(s[p]) {
        p += 1; // skip leading whitespace
    }
    if p >= s.len() {
        return Ok(list); // allow empty string
    }

    // At the top of the loop, we are at start of a new item.
    loop {
        let item: String;
        if s[p] == b'"' {
            // Quoted name --- collapse quote-quote pairs.
            let mut buf: Vec<u8> = Vec::new();
            p += 1;
            loop {
                let rel = s[p..].iter().position(|&b| b == b'"').ok_or(())?; // mismatched quotes
                buf.extend_from_slice(&s[p..p + rel]);
                p += rel + 1; // past the quote just found
                if p < s.len() && s[p] == b'"' {
                    // Adjacent quotes collapse into one literal quote.
                    buf.push(b'"');
                    p += 1;
                } else {
                    break; // that was the terminating quote
                }
            }
            // Slices were cut at ASCII '"' boundaries of a valid &str, so the
            // bytes are valid UTF-8.
            item = String::from_utf8(buf).expect("ASCII-delimited slices of a str");
        } else if unquoted_may_contain_whitespace {
            // Unquoted name --- extends to separator or end of string;
            // trailing whitespace not included.
            let start = p;
            let mut end = p;
            while p < s.len() && s[p] != separator {
                if !isspace_c_locale(s[p]) {
                    end = p + 1;
                }
                p += 1;
            }
            if start == end {
                return Err(()); // empty unquoted name not allowed
            }
            item = raw[start..end].to_string();
        } else {
            // Unquoted name --- extends to separator or whitespace.
            let start = p;
            while p < s.len() && s[p] != separator && !isspace_c_locale(s[p]) {
                p += 1;
            }
            if start == p {
                return Err(()); // empty unquoted name not allowed
            }
            item = raw[start..p].to_string();
        }

        while p < s.len() && isspace_c_locale(s[p]) {
            p += 1; // skip trailing whitespace
        }

        list.push(item);
        if p >= s.len() {
            return Ok(list);
        }
        if s[p] != separator {
            return Err(()); // invalid syntax
        }
        p += 1;
        while p < s.len() && isspace_c_locale(s[p]) {
            p += 1; // skip leading whitespace for next item
        }
        // We expect another item; if the string ended here (trailing
        // separator) the next loop iteration rejects the empty unquoted name,
        // exactly as C does at the top of its do-loop.
        if p >= s.len() {
            return Err(());
        }
    }
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

    /// Expectations executed against PostgreSQL 18.4 (Debian glibc,
    /// aarch64) via the recovery_target_timeline / recovery_target_xid
    /// check+assign hooks (ALTER SYSTEM acceptance + the parsed value
    /// echoed in "recovery target timeline %u does not exist" /
    /// "starting point-in-time recovery to XID %u"), 2026-07-30.
    #[test]
    fn strtoul_base0_matches_glibc() {
        let ok = |value, consumed| StrtoulBase0 { value, consumed, range_err: false };

        // Plain decimal.
        assert_eq!(strtoul_base0(b"1"), ok(1, 1));
        assert_eq!(strtoul_base0(b"42"), ok(42, 2));
        assert_eq!(strtoul_base0(b"7"), ok(7, 1));
        assert_eq!(strtoul_base0(b"0"), ok(0, 1));

        // Base 0: hex and octal prefixes.
        assert_eq!(strtoul_base0(b"0x10"), ok(16, 4)); // PG parsed timeline 16
        assert_eq!(strtoul_base0(b"0X10"), ok(16, 4));
        assert_eq!(strtoul_base0(b"0xff"), ok(255, 4));
        assert_eq!(strtoul_base0(b"010"), ok(8, 3)); // PG parsed timeline 8
        assert_eq!(strtoul_base0(b"0x"), ok(0, 1)); // just the "0"; 'x' is garbage
        assert_eq!(strtoul_base0(b"0x1G"), ok(1, 3)); // "0x1", trailing G ignored
        assert_eq!(strtoul_base0(b"08"), ok(0, 1)); // '8' ends an octal number

        // Leading C-locale whitespace (VT included) and signs.
        assert_eq!(strtoul_base0(b" 7"), ok(7, 2));
        assert_eq!(strtoul_base0(b"\t7"), ok(7, 2));
        assert_eq!(strtoul_base0(b"\x0b7"), ok(7, 2)); // VT: PG accepts
        assert_eq!(strtoul_base0(b" 0x10"), ok(16, 5));
        assert_eq!(strtoul_base0(b"+5"), ok(5, 2));
        // Minus wraps modulo 2^64, no ERANGE: PG parsed timeline/xid
        // 4294967295 from "-1" (u32 truncation of u64::MAX).
        assert_eq!(strtoul_base0(b"-1"), ok(u64::MAX, 2));
        assert_eq!(strtoul_base0(b"-5"), ok(u64::MAX - 4, 2));
        assert_eq!(strtoul_base0(b"-18446744073709551615"), ok(1, 21));

        // No conversion: value 0, consumed 0, NO error (glibc sets no
        // errno) — PG ACCEPTS these and they parse as 0.
        assert_eq!(strtoul_base0(b"abc"), ok(0, 0));
        assert_eq!(strtoul_base0(b""), ok(0, 0));
        assert_eq!(strtoul_base0(b"++1"), ok(0, 0));
        assert_eq!(strtoul_base0(b"- 1"), ok(0, 0));
        assert_eq!(strtoul_base0(b"latest"), ok(0, 0));

        // Trailing garbage ignored (NULL endptr callers never see it).
        assert_eq!(strtoul_base0(b"123abc"), ok(123, 3)); // PG parsed 123
        assert_eq!(strtoul_base0(b"12 "), ok(12, 2));

        // Boundaries and ERANGE (the ONLY reject the C call sites see).
        assert_eq!(strtoul_base0(b"4294967295"), ok(4294967295, 10));
        assert_eq!(strtoul_base0(b"4294967296"), ok(4294967296, 10)); // u32-truncates to 0 downstream
        assert_eq!(strtoul_base0(b"18446744073709551615"), ok(u64::MAX, 20));
        assert_eq!(
            strtoul_base0(b"18446744073709551616"),
            StrtoulBase0 { value: u64::MAX, consumed: 20, range_err: true }
        );
        assert_eq!(
            strtoul_base0(b"-18446744073709551616"),
            StrtoulBase0 { value: u64::MAX, consumed: 21, range_err: true }
        );
        assert_eq!(
            strtoul_base0(b"99999999999999999999999"),
            StrtoulBase0 { value: u64::MAX, consumed: 23, range_err: true }
        );
        assert_eq!(
            strtoul_base0(b"0xffffffffffffffffff"),
            StrtoulBase0 { value: u64::MAX, consumed: 20, range_err: true }
        );

        // C strings end at the first NUL.
        assert_eq!(strtoul_base0(b"12\034"), ok(12, 2));
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

    /// Expectations verified against PostgreSQL 18.3 (docker postgres:18.3,
    /// 2026-07-31) via postmaster-start with each value:
    ///   debug_io_direct='data wal'  -> FATAL: invalid value for parameter
    ///       "debug_io_direct": "data wal" / DETAIL: Invalid list syntax in
    ///       parameter "debug_io_direct".   (whitespace is NOT a separator)
    ///   debug_io_direct='data,,wal' -> same FATAL/DETAIL (empty item)
    ///   debug_io_direct='"data",wal' -> server started (quoted item OK)
    ///   debug_io_direct='data,wal ' -> server started (trailing ws OK)
    ///   listen_addresses='localhost,,127.0.0.1' -> FATAL: invalid list
    ///       syntax in parameter "listen_addresses"
    ///   listen_addresses='localhost 127.0.0.1' -> same FATAL
    #[test]
    fn split_guc_list_matches_c() {
        let ok = |items: &[&str]| Ok(items.iter().map(|s| s.to_string()).collect::<Vec<_>>());

        // Plain lists, empty input, surrounding whitespace.
        assert_eq!(split_guc_list("data,wal", b','), ok(&["data", "wal"]));
        assert_eq!(split_guc_list("data", b','), ok(&["data"]));
        assert_eq!(split_guc_list("", b','), ok(&[]));
        assert_eq!(split_guc_list("  \t\x0b ", b','), ok(&[])); // all-ws incl VT
        assert_eq!(split_guc_list(" data , wal ", b','), ok(&["data", "wal"]));
        assert_eq!(split_guc_list("data,wal ", b','), ok(&["data", "wal"]));
        // VT (0x0b) is scanner_isspace whitespace — trimmed like any other.
        assert_eq!(split_guc_list("\x0bdata\x0b,\x0bwal", b','), ok(&["data", "wal"]));

        // Whitespace does NOT separate items: 'data wal' is a syntax error.
        assert_eq!(split_guc_list("data wal", b','), Err(()));
        assert_eq!(split_guc_list("data\x0bwal", b','), Err(()));

        // Empty items are syntax errors.
        assert_eq!(split_guc_list("data,,wal", b','), Err(()));
        assert_eq!(split_guc_list(",data", b','), Err(()));
        assert_eq!(split_guc_list("data,", b','), Err(())); // trailing separator
        assert_eq!(split_guc_list(",", b','), Err(()));

        // Quoting: embedded separators/whitespace, doubled quotes, empty item.
        assert_eq!(split_guc_list("\"data\",wal", b','), ok(&["data", "wal"]));
        assert_eq!(split_guc_list("\"a,b\"", b','), ok(&["a,b"]));
        assert_eq!(split_guc_list("\"a b\",c", b','), ok(&["a b", "c"]));
        assert_eq!(split_guc_list("\"a\"\"b\"", b','), ok(&["a\"b"]));
        assert_eq!(split_guc_list("\"\"\"\"", b','), ok(&["\""]));
        assert_eq!(split_guc_list("\"\"", b','), ok(&[""])); // quoted empty OK
        assert_eq!(split_guc_list("\"a\" , \"b\"", b','), ok(&["a", "b"]));
        // Mismatched quotes and junk after a closing quote.
        assert_eq!(split_guc_list("\"a", b','), Err(()));
        assert_eq!(split_guc_list("\"a\"\"", b','), Err(()));
        assert_eq!(split_guc_list("\"a\"b", b','), Err(()));
        // A quote mid-item starts nothing: quotes only matter at item start.
        assert_eq!(split_guc_list("a\"b\",c", b','), ok(&["a\"b\"", "c"]));
    }

    /// SplitDirectoriesString differences verified against PostgreSQL 18.3
    /// (docker postgres:18.3, 2026-07-31):
    ///   shared_preload_libraries='foo,,bar' -> LOG: invalid list syntax in
    ///       parameter "shared_preload_libraries" (server continues, list
    ///       dropped)
    ///   shared_preload_libraries='"a,b"' -> FATAL: could not access file
    ///       "a,b" (quoted comma stays in one item)
    ///   shared_preload_libraries='a""b' -> FATAL: could not access file
    ///       "a""b" (mid-item quotes are literal)
    #[test]
    fn split_directories_string_matches_c() {
        let ok = |items: &[&str]| Ok(items.iter().map(|s| s.to_string()).collect::<Vec<_>>());

        assert_eq!(split_directories_string("a,b", b','), ok(&["a", "b"]));
        assert_eq!(split_directories_string("", b','), ok(&[]));
        assert_eq!(split_directories_string("  ", b','), ok(&[]));

        // Unquoted names may contain embedded whitespace; trailing ws trimmed.
        assert_eq!(split_directories_string("a b", b','), ok(&["a b"]));
        assert_eq!(split_directories_string(" /tmp/x y , /var/z ", b','), ok(&["/tmp/x y", "/var/z"]));
        assert_eq!(split_directories_string("a\x0bb", b','), ok(&["a\x0bb"])); // VT embedded
        assert_eq!(split_directories_string("a b \x0b,c", b','), ok(&["a b", "c"]));

        // Empty items are syntax errors.
        assert_eq!(split_directories_string("foo,,bar", b','), Err(()));
        assert_eq!(split_directories_string(",foo", b','), Err(()));
        assert_eq!(split_directories_string("foo,", b','), Err(()));

        // Quoting.
        assert_eq!(split_directories_string("\"a,b\"", b','), ok(&["a,b"]));
        assert_eq!(split_directories_string("\"a\"\"b\"", b','), ok(&["a\"b"]));
        assert_eq!(split_directories_string("a\"\"b", b','), ok(&["a\"\"b"])); // literal mid-item quotes
        assert_eq!(split_directories_string("\"a", b','), Err(()));
        assert_eq!(split_directories_string("\"a\"b", b','), Err(()));

        // MAXPGPATH-1 byte truncation.
        let long = "x".repeat(2000);
        assert_eq!(
            split_directories_string(&long, b',').unwrap(),
            vec!["x".repeat(1023)]
        );
    }

    #[test]
    fn hex_escapes_non_printables() {
        assert_eq!(pg_clean_ascii("a\x1fb", 0).unwrap(), "a\\x1fb");
        assert_eq!(pg_clean_ascii("\x7f", 0).unwrap(), "\\x7f");
        assert_eq!(pg_clean_ascii("caf\u{e9}", 0).unwrap(), "caf\\xc3\\xa9");
        assert_eq!(pg_clean_ascii("\t\n", 0).unwrap(), "\\x09\\x0a");
    }
}
