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

// upstream 66ec24276b18 (18.6): pg_unicode_fast: fix final sigma logic.
#[test]
fn final_sigma_requires_a_preceding_cased_character() {
    assert_eq!(lower("\u{300}Σ", true).0, "\u{300}σ");
    assert_eq!(lower("\u{300}\u{343}Σ", true).0, "\u{300}\u{343}σ");
    assert_eq!(lower("Σ", true).0, "σ");
    assert_eq!(lower("Α\u{300}Σ", true).0, "α\u{300}ς");
    assert_eq!(lower("ΑΣ", true).0, "ας");
    assert_eq!(lower("ΑΣ0", true).0, "ας0");
    assert_eq!(lower("ΑΣ\u{300}", true).0, "ας\u{300}");
    assert_eq!(lower("ΑΣΑ", true).0, "ασα");
    assert_eq!(lower("ΑΣ\u{300}Α", true).0, "ασ\u{300}α");
    assert_eq!(lower("\u{300}ΣΑ", true).0, "\u{300}σα");
    // Regress corpus rows: 0391 0343 03A3 0343 0391 and 0391 0345 03A3 0345 0391.
    assert_eq!(lower("Α\u{343}Σ\u{343}Α", true).0, "α\u{343}σ\u{343}α");
    assert_eq!(lower("Α\u{345}Σ\u{345}Α", true).0, "α\u{345}σ\u{345}α");
    // Only the last of two sigmas is final.
    assert_eq!(lower("ΑΣΣ", true).0, "ασς");
}

// upstream 9021c8f3cabc (18.6): unicode_case.c: defend against truncated UTF8.
#[test]
fn invalid_utf8_stops_conversion() {
    assert_eq!(unicode_strfold(&mut [], b"abc\xCE", false), 3, "case_test.c");
    assert_eq!(unicode_strfold(&mut [], b"abc\xF8xyz", false), 3, "case_test.c");
    let cases: [(&[u8], usize); 6] = [
        (b"abc\xCE", 3),
        (b"a\xE2\x82", 1),
        (b"a\xF0\x9F\x98", 1),
        (b"\xCE\xB1\xCE", 2),
        (b"abc\xF8xyz", 3),
        (b"a\x80b", 1),
    ];
    for (src, want) in cases {
        for full in [false, true] {
            assert_eq!(unicode_strlower(&mut [], src, full), want, "lower {src:?} full={full}");
            assert_eq!(unicode_strupper(&mut [], src, full), want, "upper {src:?} full={full}");
            assert_eq!(unicode_strfold(&mut [], src, full), want, "fold {src:?} full={full}");
        }
    }
    let mut dst = [0xAAu8; 8];
    assert_eq!(unicode_strupper(&mut dst, b"abc\xCE", false), 3);
    assert_eq!(&dst[..5], b"ABC\0\xAA", "nothing past the cut is copied");
    assert_eq!(unicode_strupper(&mut dst, b"abc\xF8xyz", false), 3);
    assert_eq!(&dst[..5], b"ABC\0\xAA", "nothing past the invalid byte is copied");
    let n = unicode_strlower(&mut dst, b"\xCE\x91\xCE\xA3\xCE", true);
    assert_eq!((n, &dst[..n]), (4, &b"\xCE\xB1\xCF\x83"[..]), "sigma before a cut is not final");
}
