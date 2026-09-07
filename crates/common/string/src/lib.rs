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

/// C: `pg_is_ascii(str)` (src/common/string.c:132): true when no byte up to
/// the NUL has the high bit set (`IS_HIGHBIT_SET`).
pub fn pg_is_ascii(s: &[u8]) -> bool {
    !c_str(s).iter().any(|&b| b & 0x80 != 0)
}

/// C: `pg_strip_crlf(str)` (src/common/string.c:153): removes any trailing
/// newline and carriage return characters.  C zero-terminates in place and
/// returns the new length; the returned slice is that string, its `len()`
/// C's return value.
pub fn pg_strip_crlf(s: &[u8]) -> &[u8] {
    let mut s = c_str(s);
    while let [rest @ .., b'\n' | b'\r'] = s {
        s = rest;
    }
    s
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

/// C strings end at the first NUL: every helper here models a `const char *`
/// argument, so a slice is cut there before anything else looks at it.
#[inline]
fn c_str(s: &[u8]) -> &[u8] {
    match s.iter().position(|&b| b == 0) {
        Some(n) => &s[..n],
        None => s,
    }
}

/// C: `pg_str_endswith(str, end)` (src/common/string.c:29): whether `str`
/// has the postfix `end`, both NUL-terminated.
pub fn pg_str_endswith(s: &[u8], end: &[u8]) -> bool {
    let (s, end) = (c_str(s), c_str(end));
    // can't be a postfix if longer; otherwise strcmp the tail
    end.len() <= s.len() && &s[s.len() - end.len()..] == end
}

/// `errno` as a C caller observes it after `strtol`/`strtoint`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StrtoErrno {
    /// The value was out of range (glibc `strtol` saturation, or
    /// `strtoint`'s `val != (int) val` narrowing test).
    Erange,
    /// `base` was not 0 or 2..=36.
    Einval,
}

/// Result of [`strtol`]: what a C caller can observe from
/// `strtol(s, &endptr, base)` — the return value, `endptr - s`, and errno.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Strtol {
    /// The C return value: `LONG_MAX`/`LONG_MIN` on ERANGE, 0 on no
    /// conversion or EINVAL.
    pub value: i64,
    /// endptr offset: bytes consumed from the start of `s`.  0 == "no
    /// conversion" (C leaves `*endptr == nptr`; on EINVAL glibc never writes
    /// `*endptr` at all, which the 0 stands in for).
    pub consumed: usize,
    /// `None` == errno untouched: glibc sets NO errno on no-conversion.
    pub errno: Option<StrtoErrno>,
}

/// C: `strtol(s, &endptr, base)` on the 64-bit glibc targets the server
/// runs on (`long` is `int64`).
///
/// glibc semantics, verified by execution: `base` outside {0, 2..=36} ->
/// EINVAL, 0, endptr untouched.  Otherwise skip leading C-locale whitespace
/// ([`isspace_c_locale`], VT/FF included), take one optional `+`/`-`, then
/// with base 0 or 16 accept a `0x`/`0X` prefix (base 0: `0x` -> 16, leading
/// `0` -> 8, else 10).  A `0x` prefix NOT followed by a hex digit is not an
/// error: the `0` converts and endptr stops on the `x`.  Digits are `0-9`
/// then `a-z`/`A-Z` below the base; the whole digit run is consumed even
/// once the accumulator has overflowed, and overflow saturates at
/// `LONG_MAX`/`LONG_MIN` with ERANGE (`-9223372036854775808` itself is in
/// range).  No digits -> 0, `consumed == 0`, errno untouched.  Trailing
/// garbage is the caller's business.
pub fn strtol(s: &[u8], base: i32) -> Strtol {
    if base < 0 || base == 1 || base > 36 {
        return Strtol { value: 0, consumed: 0, errno: Some(StrtoErrno::Einval) };
    }
    let s = c_str(s);
    let mut base = base as u64;

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

    // Recognize the base prefix.  A "0x" not followed by a hex digit is the
    // number 0 with endptr after the "0" (glibc's noconv special case), which
    // the look-ahead expresses directly.
    if s.get(i) == Some(&b'0') {
        if (base == 0 || base == 16)
            && matches!(s.get(i + 1), Some(b'x') | Some(b'X'))
            && s.get(i + 2).is_some_and(|b| b.is_ascii_hexdigit())
        {
            i += 2;
            base = 16;
        } else if base == 0 {
            base = 8;
        }
    } else if base == 0 {
        base = 10;
    }

    let digits_start = i;
    // Accumulate the magnitude; `overflow` marks a magnitude past u64, which
    // is past LONG_MAX either way.
    let mut acc: u64 = 0;
    let mut overflow = false;
    while i < s.len() {
        let d = match s[i] {
            b @ b'0'..=b'9' => u64::from(b - b'0'),
            b @ b'a'..=b'z' => u64::from(b - b'a' + 10),
            b @ b'A'..=b'Z' => u64::from(b - b'A' + 10),
            _ => break,
        };
        if d >= base {
            break;
        }
        if !overflow {
            match acc.checked_mul(base).and_then(|v| v.checked_add(d)) {
                Some(v) => acc = v,
                None => overflow = true,
            }
        }
        i += 1;
    }

    if i == digits_start {
        // No conversion: value 0, endptr == nptr, errno untouched.
        return Strtol { value: 0, consumed: 0, errno: None };
    }
    // LONG_MIN's magnitude is LONG_MAX + 1.
    let limit = if neg { (i64::MAX as u64) + 1 } else { i64::MAX as u64 };
    if overflow || acc > limit {
        let value = if neg { i64::MIN } else { i64::MAX };
        return Strtol { value, consumed: i, errno: Some(StrtoErrno::Erange) };
    }
    let value = if neg { (acc as i64).wrapping_neg() } else { acc as i64 };
    Strtol { value, consumed: i, errno: None }
}

/// Result of [`strtoint`]: the C return value, `endptr - s`, and errno.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Strtoint {
    /// The C return value: `(int) val` — the long TRUNCATED to int, not
    /// clamped (`4294967297` -> 1 with ERANGE).
    pub value: i32,
    /// endptr offset, exactly as [`Strtol::consumed`].
    pub consumed: usize,
    /// errno: strtol's own, or ERANGE when the long did not fit an int.
    pub errno: Option<StrtoErrno>,
}

/// C: `strtoint(str, &endptr, base)` (src/common/string.c:50) — "just like
/// strtol, but returns int not long": `val = strtol(...); if (val != (int)
/// val) errno = ERANGE; return (int) val;`.
pub fn strtoint(s: &[u8], base: i32) -> Strtoint {
    let r = strtol(s, base);
    let value = r.value as i32;
    let errno = if i64::from(value) != r.value { Some(StrtoErrno::Erange) } else { r.errno };
    Strtoint { value, consumed: r.consumed, errno }
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
    let s = c_str(s);
    let r = strtoint(s, 10);
    // endptr == str: strtol converted nothing.
    // *endptr != '\0': trailing junk, including trailing whitespace.
    // errno != 0: strtol's ERANGE, or strtoint's `val != (int) val` narrowing.
    if r.consumed == 0 || r.consumed != s.len() || r.errno.is_some() {
        return None;
    }
    Some(r.value)
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
    let s = c_str(s);

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

    /// C: `strtol(s, &endptr, base)` under 64-bit glibc.  Every expectation
    /// is the value / `endptr - s` / errno triple the real call produces.
    #[test]
    fn strtol_matches_glibc() {
        let ok = |value: i64, consumed: usize| Strtol { value, consumed, errno: None };
        let erange = |value: i64, consumed: usize| Strtol { value, consumed, errno: Some(StrtoErrno::Erange) };

        // Base 10: C-locale whitespace, one sign, digit run, endptr at the
        // first non-digit (trailing junk is the caller's business).
        assert_eq!(strtol(b"123", 10), ok(123, 3));
        assert_eq!(strtol(b"  \x0b+42xyz", 10), ok(42, 6));
        assert_eq!(strtol(b"\t\n\x0c\r 7", 10), ok(7, 6));
        assert_eq!(strtol(b"-17", 10), ok(-17, 3));
        assert_eq!(strtol(b"12abc", 10), ok(12, 2));
        // C strings end at the first NUL.
        assert_eq!(strtol(b"12\x0034", 10), ok(12, 2));

        // No conversion: 0, endptr == nptr, errno untouched.
        assert_eq!(strtol(b"", 10), ok(0, 0));
        assert_eq!(strtol(b"abc", 10), ok(0, 0));
        assert_eq!(strtol(b"-", 10), ok(0, 0));
        assert_eq!(strtol(b"+", 10), ok(0, 0));
        assert_eq!(strtol(b"- 1", 10), ok(0, 0));
        assert_eq!(strtol(b"   ", 10), ok(0, 0));

        // Base 16 accepts an optional 0x/0X prefix; "0x" with no hex digit
        // after it converts the "0" and leaves endptr on the 'x'.
        assert_eq!(strtol(b"ff", 16), ok(255, 2));
        assert_eq!(strtol(b"FF", 16), ok(255, 2));
        assert_eq!(strtol(b"0XfF", 16), ok(255, 4));
        assert_eq!(strtol(b"-0x1F", 16), ok(-31, 5));
        assert_eq!(strtol(b"0x", 16), ok(0, 1));
        assert_eq!(strtol(b"0xg", 16), ok(0, 1));
        assert_eq!(strtol(b"-0x", 16), ok(0, 2));

        // Base 0: 0x -> 16, leading 0 -> 8, else 10.
        assert_eq!(strtol(b"0x1F", 0), ok(31, 4));
        assert_eq!(strtol(b"0x1g", 0), ok(1, 3));
        assert_eq!(strtol(b"017", 0), ok(15, 3));
        assert_eq!(strtol(b"019", 0), ok(1, 2));
        assert_eq!(strtol(b"0", 0), ok(0, 1));
        assert_eq!(strtol(b"19", 0), ok(19, 2));
        assert_eq!(strtol(b"0x", 0), ok(0, 1));

        // Explicit bases: digits are 0-9 then a-z/A-Z below the base.
        assert_eq!(strtol(b"08", 8), ok(0, 1));
        assert_eq!(strtol(b"12", 2), ok(1, 1));
        assert_eq!(strtol(b"102", 2), ok(2, 2));
        assert_eq!(strtol(b"zz", 36), ok(1295, 2));
        assert_eq!(strtol(b"Zz", 36), ok(1295, 2));
        assert_eq!(strtol(b"z", 35), ok(0, 0));

        // Invalid base: EINVAL, 0, endptr never written.
        let einval = Strtol { value: 0, consumed: 0, errno: Some(StrtoErrno::Einval) };
        assert_eq!(strtol(b"1", 1), einval);
        assert_eq!(strtol(b"1", 37), einval);
        assert_eq!(strtol(b"1", -1), einval);

        // Overflow saturates at LONG_MAX/LONG_MIN with ERANGE; the whole digit
        // run is still consumed.
        assert_eq!(strtol(b"9223372036854775807", 10), ok(i64::MAX, 19));
        assert_eq!(strtol(b"9223372036854775808", 10), erange(i64::MAX, 19));
        assert_eq!(strtol(b"-9223372036854775808", 10), ok(i64::MIN, 20));
        assert_eq!(strtol(b"-9223372036854775809", 10), erange(i64::MIN, 20));
        assert_eq!(strtol(b"99999999999999999999999x", 10), erange(i64::MAX, 23));
        assert_eq!(strtol(b"-ffffffffffffffffffff", 16), erange(i64::MIN, 21));
    }

    /// C: `strtoint(s, &endptr, base)` (src/common/string.c:50) — strtol,
    /// then `if (val != (int) val) errno = ERANGE; return (int) val;`.  The
    /// return value is the TRUNCATED long, not a clamp.
    #[test]
    fn strtoint_matches_c() {
        let ok = |value: i32, consumed: usize| Strtoint { value, consumed, errno: None };
        let erange = |value: i32, consumed: usize| Strtoint { value, consumed, errno: Some(StrtoErrno::Erange) };

        assert_eq!(strtoint(b"2147483647", 10), ok(i32::MAX, 10));
        assert_eq!(strtoint(b"-2147483648", 10), ok(i32::MIN, 11));
        assert_eq!(strtoint(b"2147483648", 10), erange(i32::MIN, 10));
        assert_eq!(strtoint(b"-2147483649", 10), erange(i32::MAX, 11));
        assert_eq!(strtoint(b"4294967297", 10), erange(1, 10));
        // strtol's own ERANGE (LONG_MAX) truncates to -1 and stays ERANGE.
        assert_eq!(strtoint(b"9223372036854775808", 10), erange(-1, 19));

        assert_eq!(strtoint(b"7f", 16), ok(127, 2));
        assert_eq!(strtoint(b"0x7f", 0), ok(127, 4));
        assert_eq!(strtoint(b"12abc", 10), ok(12, 2));
        assert_eq!(strtoint(b" -5", 10), ok(-5, 3));
        assert_eq!(strtoint(b"abc", 10), ok(0, 0));
        assert_eq!(
            strtoint(b"1", 1),
            Strtoint { value: 0, consumed: 0, errno: Some(StrtoErrno::Einval) }
        );
    }

    /// C: `pg_is_ascii(str)` (src/common/string.c:132) — IS_HIGHBIT_SET on
    /// every byte up to the NUL.
    #[test]
    fn pg_is_ascii_matches_c() {
        assert!(pg_is_ascii(b""));
        assert!(pg_is_ascii(b"abc"));
        assert!(pg_is_ascii(b"\x7f\x01 ~"));
        assert!(!pg_is_ascii(b"caf\xc3\xa9"));
        assert!(!pg_is_ascii(b"\x80"));
        // The C string ends at the first NUL.
        assert!(pg_is_ascii(b"ok\x00\xff"));
    }

    /// C: `pg_str_endswith(str, end)` (src/common/string.c:29).
    #[test]
    fn pg_str_endswith_matches_c() {
        assert!(pg_str_endswith(b"foo.txt", b".txt"));
        assert!(!pg_str_endswith(b"txt", b".txt"));
        assert!(pg_str_endswith(b"abc", b"abc"));
        assert!(pg_str_endswith(b"a", b""));
        assert!(pg_str_endswith(b"", b""));
        assert!(!pg_str_endswith(b"", b"a"));
        assert!(!pg_str_endswith(b"abc", b"ABC"));
        // NUL-terminated on both sides.
        assert!(pg_str_endswith(b"ab\x00cd", b"b"));
        assert!(pg_str_endswith(b"abc", b"bc\x00zz"));
    }

    /// C: `pg_strip_crlf(str)` (src/common/string.c:153) — the returned
    /// slice is the string C leaves behind; its length is C's return value.
    #[test]
    fn pg_strip_crlf_matches_c() {
        assert_eq!(pg_strip_crlf(b"abc\r\n"), b"abc");
        assert_eq!(pg_strip_crlf(b"abc\n\r\n"), b"abc");
        assert_eq!(pg_strip_crlf(b"abc\n"), b"abc");
        assert_eq!(pg_strip_crlf(b"abc\r"), b"abc");
        assert_eq!(pg_strip_crlf(b"\r\n"), b"");
        assert_eq!(pg_strip_crlf(b""), b"");
        assert_eq!(pg_strip_crlf(b"a\r\nb"), b"a\r\nb");
        assert_eq!(pg_strip_crlf(b"abc \n"), b"abc ");
        // strlen stops at the first NUL.
        assert_eq!(pg_strip_crlf(b"x\n\x00\n"), b"x");
    }
}
