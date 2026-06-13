//! Unit tests for the pure byte-string helpers (no seams required).

use super::*;

#[test]
fn strtol_basic() {
    assert_eq!(strtol(b"123"), (123, 3, true));
    assert_eq!(strtol(b"  -45x"), (-45, 5, true));
    assert_eq!(strtol(b"+7"), (7, 2, true));
    // no digits
    assert_eq!(strtol(b"abc"), (0, 0, false));
    // overflow clamps to i64::MAX and reports !ok
    let (v, _, ok) = strtol(b"99999999999999999999999");
    assert_eq!(v, i64::MAX);
    assert!(!ok);
}

#[test]
fn atoi_matches_c() {
    assert_eq!(atoi(b"42abc"), 42);
    assert_eq!(atoi(b"x"), 0);
    assert_eq!(atoi(b"-3"), -3);
}

#[test]
fn bcmp_strcmp_semantics() {
    use core::cmp::Ordering::*;
    assert_eq!(bcmp(b"abc", b"abc"), Equal);
    // prefix sorts first (implicit NUL < any byte)
    assert_eq!(bcmp(b"ab", b"abc"), Less);
    assert_eq!(bcmp(b"abd", b"abc"), Greater);
}

#[test]
fn bncmp_bounded() {
    use core::cmp::Ordering::*;
    assert_eq!(bncmp(b"abcX", b"abcY", 3), Equal);
    assert_eq!(bncmp(b"abcX", b"abcY", 4), Less);
}

#[test]
fn strbcmp_backward() {
    use core::cmp::Ordering::*;
    // compare from the ends: "ing" vs "ting" -> "ing" is shorter, sorts first
    assert_eq!(strbcmp(b"ing", b"ting"), Less);
    assert_eq!(strbcmp(b"xing", b"ting"), Greater);
    assert_eq!(strbcmp(b"ing", b"ing"), Equal);
}

#[test]
fn strbncmp_bounded_backward() {
    use core::cmp::Ordering::*;
    // last 3 chars equal
    assert_eq!(strbncmp(b"Xing", b"Ying", 3), Equal);
    // 4 bytes back: g,n,i equal, then 'X'(0x58) < 'Y'(0x59)
    assert_eq!(strbncmp(b"Xing", b"Ying", 4), Less);
}

#[test]
fn bstrchr_bstrstr() {
    assert_eq!(bstrchr(b"hello", b'l'), Some(2));
    assert_eq!(bstrchr(b"hello", b'z'), None);
    assert_eq!(bstrstr(b"hello", b"ll"), Some(2));
    assert_eq!(bstrstr(b"hello", b"xx"), None);
    assert_eq!(bstrstr(b"hello", b""), Some(0));
}

#[test]
fn ctype_helpers() {
    assert!(isspace(b' '));
    assert!(isspace(b'\t'));
    assert!(!isspace(b'a'));
    assert!(isprint(b'a'));
    assert!(!isprint(0x01));
    assert!(isdigit(b'7'));
    assert!(!isdigit(b'x'));
}
