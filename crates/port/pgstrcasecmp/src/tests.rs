//! Unit tests. The high-bit branch consults the process locale's `<ctype.h>`
//! functions; the test process runs in the default C/POSIX locale, where no
//! high-bit byte classifies as a letter, so high-bit bytes pass through
//! unchanged. Only locale-independent behavior is asserted.

use super::*;

#[test]
fn compares_ascii_case_insensitively() {
    assert_eq!(pg_strcasecmp(b"Postgres", b"postgres"), 0);
    assert!(pg_strcasecmp(b"abc", b"abd") < 0);
    assert!(pg_strcasecmp(b"abe", b"abd") > 0);
    assert_eq!(pg_strcasecmp(b"", b""), 0);
    assert!(pg_strcasecmp(b"a", b"") > 0);
    assert!(pg_strcasecmp(b"", b"a") < 0);
}

#[test]
fn stops_at_nul_like_c_strings() {
    // Everything after the embedded NUL is ignored, exactly like the C walk.
    assert_eq!(pg_strcasecmp(b"abc\0zzz", b"ABC\0yyy"), 0);
    assert_eq!(pg_strcasecmp(b"abc\0", b"abc"), 0);
}

#[test]
fn strncasecmp_limits_comparison() {
    assert_eq!(pg_strncasecmp(b"abcdef", b"ABCxyz", 3), 0);
    assert!(pg_strncasecmp(b"abcdef", b"ABCxyz", 4) < 0);
    // n == 0: nothing is examined.
    assert_eq!(pg_strncasecmp(b"abcdef", b"ABCxyz", 0), 0);
    // Stops at the first NUL even with n to spare.
    assert_eq!(pg_strncasecmp(b"ab\0d", b"AB\0Z", 4), 0);
    // n larger than either input still terminates on the implicit NUL.
    assert_eq!(pg_strncasecmp(b"abc", b"ABC", 100), 0);
}

#[test]
fn ascii_case_helpers_only_change_ascii_letters() {
    assert_eq!(pg_ascii_toupper(b'a'), b'A');
    assert_eq!(pg_ascii_toupper(b'z'), b'Z');
    assert_eq!(pg_ascii_toupper(b'A'), b'A');
    assert_eq!(pg_ascii_toupper(b'0'), b'0');
    assert_eq!(pg_ascii_toupper(0xe1), 0xe1);

    assert_eq!(pg_ascii_tolower(b'A'), b'a');
    assert_eq!(pg_ascii_tolower(b'Z'), b'z');
    assert_eq!(pg_ascii_tolower(b'a'), b'a');
    assert_eq!(pg_ascii_tolower(b'0'), b'0');
    assert_eq!(pg_ascii_tolower(0xc1), 0xc1);
}

#[test]
fn locale_aware_helpers_keep_ascii_fast_path() {
    assert_eq!(pg_toupper(b'a'), b'A');
    assert_eq!(pg_toupper(b'A'), b'A');
    assert_eq!(pg_toupper(b'0'), b'0');
    assert_eq!(pg_tolower(b'A'), b'a');
    assert_eq!(pg_tolower(b'a'), b'a');
    assert_eq!(pg_tolower(b'0'), b'0');
}

#[test]
fn high_bit_bytes_unchanged_in_c_locale() {
    // In the C locale isupper/islower reject high-bit bytes, so the
    // locale-aware helpers leave them alone (matching the C behavior).
    assert_eq!(pg_tolower(0xD7), 0xD7);
    assert_eq!(pg_toupper(0xD7), 0xD7);
    assert!(pg_strcasecmp(&[0xC9], &[0xE9]) != 0);
}
