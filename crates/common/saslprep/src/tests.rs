use super::*;
use mcx::MemoryContext;

fn prep(input: &[u8]) -> Option<Vec<u8>> {
    let cx = MemoryContext::new("test-saslprep");
    pg_saslprep(cx.mcx(), input)
        .unwrap()
        .map(|v| v.iter().copied().collect())
}

#[test]
fn ascii_input_is_returned_unchanged() {
    assert_eq!(prep(b"password").as_deref(), Some(&b"password"[..]));
    assert_eq!(prep(b"test").as_deref(), Some(&b"test"[..]));
    // Empty input is pure ASCII and short-circuits unchanged; the C
    // empty-input rejection only fires after the non-ASCII mapping step.
    assert_eq!(prep(b"").as_deref(), Some(&b""[..]));
    // The pure-ASCII fast path does no prohibited-output check (C pg_is_ascii
    // short-circuit), so ASCII control chars pass through despite RFC 4013.
    assert_eq!(prep(b"a\x07b").as_deref(), Some(&b"a\x07b"[..]));
    assert_eq!(prep(b"User Name").as_deref(), Some(&b"User Name"[..]));
}

#[test]
fn maps_non_ascii_space_to_ascii_space() {
    assert_eq!(prep("a\u{00a0}b".as_bytes()).as_deref(), Some(&b"a b"[..]));
    assert_eq!(prep("x\u{3000}y".as_bytes()).as_deref(), Some(&b"x y"[..]));
}

#[test]
fn maps_common_nothing_to_empty_and_rejects_empty_result() {
    // U+00AD SOFT HYPHEN maps to nothing; as the only character the
    // post-mapping password is empty and rejected (SASLPREP_PROHIBITED).
    assert_eq!(prep("\u{00ad}".as_bytes()), None);
}

#[test]
fn drops_mapped_to_nothing_within_longer_string() {
    assert_eq!(prep("a\u{00ad}b".as_bytes()).as_deref(), Some(&b"ab"[..]));
}

#[test]
fn applies_nfkc_normalization() {
    assert_eq!(prep("\u{2168}".as_bytes()).as_deref(), Some(&b"IX"[..]));
}

#[test]
fn rejects_invalid_utf8() {
    assert_eq!(prep(b"\xff"), None);
    assert_eq!(prep(b"\xc3"), None);
}

#[test]
fn rejects_non_ascii_prohibited_output() {
    assert_eq!(prep("\u{200e}".as_bytes()), None);
    // A non-ASCII byte defeats the ASCII fast path, so the ASCII control char
    // now hits the prohibited-output table.
    assert_eq!(prep("\u{0007}\u{00e9}".as_bytes()), None);
}

#[test]
fn enforces_bidirectional_rules() {
    // LCat ('a') mixed with RandALCat (Hebrew alef U+05D0) -> prohibited.
    assert_eq!(prep("\u{05d0}a\u{05d0}".as_bytes()), None);
    // All-RandALCat with RandALCat first and last is allowed.
    assert_eq!(
        prep("\u{05d0}\u{05d1}".as_bytes()).as_deref(),
        Some("\u{05d0}\u{05d1}".as_bytes())
    );
    // RandALCat followed by a digit: last char is not RandALCat -> prohibited.
    assert_eq!(prep("\u{05d0}1".as_bytes()), None);
}

#[test]
fn rfc_4013_examples() {
    assert_eq!(prep("I\u{00ad}X".as_bytes()).as_deref(), Some(&b"IX"[..]));
    assert_eq!(prep(b"user").as_deref(), Some(&b"user"[..]));
    assert_eq!(prep(b"USER").as_deref(), Some(&b"USER"[..]));
    assert_eq!(prep("\u{00aa}".as_bytes()).as_deref(), Some(&b"a"[..]));
    assert_eq!(prep("\u{2168}".as_bytes()).as_deref(), Some(&b"IX"[..]));
    // RFC 4013 marks U+0007 as prohibited, but C's pure-ASCII fast path
    // returns it unchanged; match C.
    assert_eq!(prep("\u{0007}".as_bytes()).as_deref(), Some(&b"\x07"[..]));
    // Arabic alef followed by "1": bidi violation.
    assert_eq!(prep("\u{0627}\u{0031}".as_bytes()), None);
}

#[test]
fn is_code_in_table_boundaries() {
    assert!(is_code_in_table(0x00a0, NON_ASCII_SPACE_RANGES));
    assert!(!is_code_in_table(0x009f, NON_ASCII_SPACE_RANGES));
    assert!(is_code_in_table(0x2000, NON_ASCII_SPACE_RANGES));
    assert!(is_code_in_table(0x200b, NON_ASCII_SPACE_RANGES));
    assert!(!is_code_in_table(0x200c, NON_ASCII_SPACE_RANGES));
    assert!(!is_code_in_table(0x0, &[]));
    assert!(!is_code_in_table(0x110000, NON_ASCII_SPACE_RANGES));
}

#[test]
fn utf8_roundtrip_helpers_match_wchar() {
    for &cp in &[0x41u32, 0x00e9, 0x20ac, 0x1f600] {
        let mut buf = [0u8; 4];
        unicode_to_utf8(cp, &mut buf);
        let len = pg_utf_mblen(&buf) as usize;
        assert_eq!(wchar::utf8_to_unicode(&buf[..len]), cp);
    }
}
