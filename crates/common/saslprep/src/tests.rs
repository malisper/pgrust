//! Unit tests for the `pg_saslprep` port.

use super::*;
use ::mcx::MemoryContext;

/// Run `pg_saslprep` against a fresh scratch context and return the bytes.
fn prep(input: &[u8]) -> Option<alloc::vec::Vec<u8>> {
    let cx = MemoryContext::new("test-saslprep");
    pg_saslprep(cx.mcx(), input).unwrap()
}

#[test]
fn ascii_input_is_returned_unchanged() {
    assert_eq!(prep(b"password").as_deref(), Some(&b"password"[..]));
    assert_eq!(prep(b"test").as_deref(), Some(&b"test"[..]));
    // Empty input is pure ASCII and short-circuits unchanged (the C empty-input
    // rejection only fires after the non-ASCII mapping step).
    assert_eq!(prep(b"").as_deref(), Some(&b""[..]));
    // Control characters are still ASCII here (the pure-ASCII fast path does no
    // prohibited-output check, matching pg_is_ascii short-circuit in C).
    assert_eq!(prep(b"a\x07b").as_deref(), Some(&b"a\x07b"[..]));
    assert_eq!(prep(b"User Name").as_deref(), Some(&b"User Name"[..]));
}

#[test]
fn maps_non_ascii_space_to_ascii_space() {
    // U+00A0 NO-BREAK SPACE -> U+0020 SPACE (table C.1.2).
    assert_eq!(prep("a\u{00a0}b".as_bytes()).as_deref(), Some(&b"a b"[..]));
    // U+3000 IDEOGRAPHIC SPACE -> U+0020.
    assert_eq!(prep("x\u{3000}y".as_bytes()).as_deref(), Some(&b"x y"[..]));
}

#[test]
fn maps_common_nothing_to_empty_and_rejects_empty_result() {
    // U+00AD SOFT HYPHEN maps to nothing (table B.1); the only character, so
    // the post-mapping password is empty and rejected (SASLPREP_PROHIBITED).
    assert_eq!(prep("\u{00ad}".as_bytes()), None);
}

#[test]
fn drops_mapped_to_nothing_within_longer_string() {
    // "a" + SOFT HYPHEN + "b" -> "ab" (the soft hyphen is dropped).
    assert_eq!(prep("a\u{00ad}b".as_bytes()).as_deref(), Some(&b"ab"[..]));
}

#[test]
fn applies_nfkc_normalization() {
    // U+2168 ROMAN NUMERAL NINE normalizes (NFKC) to "IX".
    assert_eq!(prep("\u{2168}".as_bytes()).as_deref(), Some(&b"IX"[..]));
}

#[test]
fn rejects_invalid_utf8() {
    assert_eq!(prep(b"\xff"), None);
    // Truncated multi-byte sequence: lead byte claims 2 bytes but only 1 given.
    assert_eq!(prep(b"\xc3"), None);
}

#[test]
fn rejects_non_ascii_prohibited_output() {
    // U+200E LEFT-TO-RIGHT MARK is in the prohibited-output table.
    assert_eq!(prep("\u{200e}".as_bytes()), None);
}

#[test]
fn enforces_bidirectional_rules() {
    // Contains both an LCat (ASCII 'a') and RandALCat (Hebrew alef U+05D0)
    // -> prohibited.
    assert_eq!(prep("\u{05d0}a\u{05d0}".as_bytes()), None);
    // All-RandALCat with RandALCat first and last is allowed (alef + bet).
    assert_eq!(
        prep("\u{05d0}\u{05d1}".as_bytes()).as_deref(),
        Some("\u{05d0}\u{05d1}".as_bytes())
    );
    // RandALCat followed by a digit: last char is not RandALCat -> prohibited.
    assert_eq!(prep("\u{05d0}1".as_bytes()), None);
}

#[test]
fn is_code_in_table_boundaries() {
    // NON_ASCII_SPACE_RANGES first entry is the singleton 0x00A0.
    assert!(is_code_in_table(0x00a0, NON_ASCII_SPACE_RANGES));
    assert!(!is_code_in_table(0x009f, NON_ASCII_SPACE_RANGES));
    // 0x2000..=0x200B is a range; check both ends and outside.
    assert!(is_code_in_table(0x2000, NON_ASCII_SPACE_RANGES));
    assert!(is_code_in_table(0x200b, NON_ASCII_SPACE_RANGES));
    assert!(!is_code_in_table(0x200c, NON_ASCII_SPACE_RANGES));
    // Empty / out-of-bounds shortcuts.
    assert!(!is_code_in_table(0x0, &[]));
    assert!(!is_code_in_table(0x110000, NON_ASCII_SPACE_RANGES));
}

#[test]
fn utf8_roundtrip_helpers_match_wchar() {
    for &cp in &[0x41u32, 0x00e9, 0x20ac, 0x1f600] {
        let mut buf = [0u8; 4];
        unicode_to_utf8(cp, &mut buf);
        let len = utf8_leading_len(buf[0]);
        assert_eq!(utf8_to_unicode(&buf[..len]), cp);
    }
}
