use super::*;

fn lower(s: &str, full: bool) -> (String, usize) {
    let mut dst = vec![0u8; s.len() * 4 + 1];
    let n = unicode_strlower(&mut dst, s.as_bytes(), full);
    (String::from_utf8(dst[..n].to_vec()).unwrap(), n)
}

fn upper(s: &str, full: bool) -> String {
    let mut dst = vec![0u8; s.len() * 4 + 1];
    let n = unicode_strupper(&mut dst, s.as_bytes(), full);
    String::from_utf8(dst[..n].to_vec()).unwrap()
}

fn fold(s: &str, full: bool) -> String {
    let mut dst = vec![0u8; s.len() * 4 + 1];
    let n = unicode_strfold(&mut dst, s.as_bytes(), full);
    String::from_utf8(dst[..n].to_vec()).unwrap()
}

#[test]
fn simple_codepoint_maps() {
    assert_eq!(unicode_lowercase_simple(0x41), 0x61);
    assert_eq!(unicode_uppercase_simple(0x61), 0x41);
    assert_eq!(unicode_lowercase_simple(0x391), 0x3B1); // Α → α
    assert_eq!(unicode_uppercase_simple(0x3B1), 0x391);
    assert_eq!(unicode_lowercase_simple(0x416), 0x436); // Ж → ж
    assert_eq!(unicode_uppercase_simple(0x436), 0x416);
    assert_eq!(unicode_lowercase_simple(0x130), 0x69); // İ → i (simple)
    assert_eq!(unicode_uppercase_simple(0x131), 0x49); // ı → I (simple)
    assert_eq!(unicode_uppercase_simple(0xDF), 0xDF); // ß self (simple)
    assert_eq!(unicode_casefold_simple(0x3C2), 0x3C3); // ς → σ
    assert_eq!(unicode_lowercase_simple(0x2014), 0x2014); // em dash self
    assert_eq!(unicode_uppercase_simple(0x10428), 0x10400); // Deseret
}

#[test]
fn strlower_simple_and_full() {
    assert_eq!(lower("ΑΒΓ", false).0, "αβγ");
    assert_eq!(upper("αβγ", false), "ΑΒΓ");
    assert_eq!(upper("Жизнь", false), "ЖИЗНЬ");
    // Final sigma is a special (conditioned) mapping: full only.
    assert_eq!(lower("ΟΣ", true).0, "ος");
    assert_eq!(lower("ΟΣ", false).0, "οσ");
    assert_eq!(lower("ΣΤΟ", true).0, "στο");
    // ß expands under full uppercase only.
    assert_eq!(upper("straße", true), "STRASSE");
    assert_eq!(upper("straße", false), "STRAßE");
    // İ lowers to i + COMBINING DOT ABOVE under full, plain i under simple.
    assert_eq!(lower("İ", true).0, "i\u{307}");
    assert_eq!(lower("İ", false).0, "i");
}

#[test]
fn strfold_matches_c_expectations() {
    assert_eq!(fold("ẞß", true), "ssss");
    assert_eq!(fold("ẞß", false), "ßß"); // simple fold: 1E9E → 00DF
    assert_eq!(fold("ΣςΣ", false), "σσσ");
}

#[test]
fn truncation_and_nul_semantics() {
    let src = "ABCDEF".as_bytes();
    let mut dst = [0xAAu8; 4];
    let n = unicode_strlower(&mut dst, src, false);
    assert_eq!(n, 6);
    assert_eq!(&dst, b"abcd"); // truncated, no NUL (no room)
    let mut dst = [0xAAu8; 7];
    let n = unicode_strlower(&mut dst, src, false);
    assert_eq!(n, 6);
    assert_eq!(&dst, b"abcdef\0");
    // Zero-size probe returns the needed length.
    let n = unicode_strlower(&mut [], "İstanbul".as_bytes(), true);
    assert_eq!(n, "i\u{307}stanbul".len());
    // Embedded NUL stops conversion.
    let n = unicode_strlower(&mut dst, b"AB\0CD", false);
    assert_eq!(n, 2);
}

#[test]
fn strtitle_word_boundaries() {
    // Boundary iterator mirroring pg_locale_builtin.c initcap_wbnext
    // (posix isalnum transitions).
    let src = "hello wORLD-φΩσ";
    let bytes = src.as_bytes();
    let mut offset = 0usize;
    let mut init = false;
    let mut prev_alnum = false;
    let mut wbnext = move || {
        while offset < bytes.len() && bytes[offset] != 0 {
            let u = utf8_to_unicode(&bytes[offset..]);
            let curr_alnum = unicode_category::pg_u_isalnum(u, true);
            if !init || curr_alnum != prev_alnum {
                let prev = offset;
                init = true;
                offset += unicode_utf8len(u) as usize;
                prev_alnum = curr_alnum;
                return prev;
            }
            offset += unicode_utf8len(u) as usize;
        }
        bytes.len()
    };
    let mut dst = vec![0u8; src.len() * 4 + 1];
    let n = unicode_strtitle(&mut dst, bytes, false, &mut wbnext);
    assert_eq!(std::str::from_utf8(&dst[..n]).unwrap(), "Hello World-Φωσ");
}
