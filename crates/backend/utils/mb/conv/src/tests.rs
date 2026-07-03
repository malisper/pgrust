use super::*;
use types_fmgr::{direct_function_call6_coll, PGFunction};

const GROWTH: usize = 4;

fn call(
    f: PGFunction,
    src_enc: pg_enc,
    dest_enc: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<(i32, Vec<u8>)> {
    let mut dest = vec![0xAAu8; src.len() * GROWTH + 1];
    let consumed = direct_function_call6_coll(
        f,
        0,
        Datum::from_i32(src_enc),
        Datum::from_i32(dest_enc),
        Datum::from_usize(src.as_ptr() as usize),
        Datum::from_usize(dest.as_mut_ptr() as usize),
        Datum::from_i32(src.len() as i32),
        Datum::from_bool(no_error),
    )?
    .as_i32();
    let n = dest.iter().position(|&b| b == 0).unwrap();
    dest.truncate(n);
    Ok((consumed, dest))
}

fn ok(
    f: PGFunction,
    src_enc: pg_enc,
    dest_enc: pg_enc,
    src: &[u8],
) -> Vec<u8> {
    let (consumed, out) = call(f, src_enc, dest_enc, src, false).unwrap();
    assert_eq!(consumed as usize, src.len());
    out
}

fn err(
    f: PGFunction,
    src_enc: pg_enc,
    dest_enc: pg_enc,
    src: &[u8],
) -> Box<PgError> {
    call(f, src_enc, dest_enc, src, false).unwrap_err()
}

#[test]
fn latin1_to_utf8_exhaustive_roundtrip() {
    for b in 1u8..=0xff {
        let utf8 = ok(fc_iso8859_1_to_utf8, PG_LATIN1, PG_UTF8, &[b]);
        let expected = char::from_u32(b as u32).unwrap().to_string();
        assert_eq!(utf8, expected.as_bytes(), "byte 0x{b:02x}");
        let back = ok(fc_utf8_to_iso8859_1, PG_UTF8, PG_LATIN1, &utf8);
        assert_eq!(back, [b]);
    }
}

#[test]
fn latin1_mixed_string() {
    let s = "caf\u{e9} na\u{ef}ve \u{c9}L\u{c8}VE".to_string();
    let latin1: Vec<u8> = s.chars().map(|c| c as u32 as u8).collect();
    assert_eq!(
        ok(fc_iso8859_1_to_utf8, PG_LATIN1, PG_UTF8, &latin1),
        s.as_bytes()
    );
    assert_eq!(
        ok(fc_utf8_to_iso8859_1, PG_UTF8, PG_LATIN1, s.as_bytes()),
        latin1
    );
}

#[test]
fn utf8_to_latin1_untranslatable_is_c_exact_22p05() {
    // U+6C34 (CJK) has no LATIN1 equivalent.
    let e = err(fc_utf8_to_iso8859_1, PG_UTF8, PG_LATIN1, "水".as_bytes());
    assert_eq!(e.sqlstate(), types_error::ERRCODE_UNTRANSLATABLE_CHARACTER);
    assert_eq!(
        e.message(),
        "character with byte sequence 0xe6 0xb0 0xb4 in encoding \"UTF8\" has no equivalent in encoding \"LATIN1\""
    );
    // U+20AC euro: 3-byte, l != 2 arm.
    let e = err(fc_utf8_to_iso8859_1, PG_UTF8, PG_LATIN1, "€".as_bytes());
    assert_eq!(e.sqlstate(), types_error::ERRCODE_UNTRANSLATABLE_CHARACTER);
    assert_eq!(
        e.message(),
        "character with byte sequence 0xe2 0x82 0xac in encoding \"UTF8\" has no equivalent in encoding \"LATIN1\""
    );
}

#[test]
fn invalid_utf8_is_c_exact_22021() {
    let e = err(fc_utf8_to_iso8859_1, PG_UTF8, PG_LATIN1, &[b'a', 0xe9, b'x']);
    assert_eq!(
        e.sqlstate(),
        types_error::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE
    );
    assert_eq!(
        e.message(),
        "invalid byte sequence for encoding \"UTF8\": 0xe9 0x78"
    );
    let e = err(fc_utf8_to_win, PG_UTF8, PG_WIN1252, &[0xff]);
    assert_eq!(
        e.message(),
        "invalid byte sequence for encoding \"UTF8\": 0xff"
    );
}

#[test]
fn embedded_nul_reports_invalid() {
    let e = err(fc_iso8859_1_to_utf8, PG_LATIN1, PG_UTF8, &[b'a', 0, b'b']);
    assert_eq!(
        e.sqlstate(),
        types_error::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE
    );
    let (consumed, out) = call(
        fc_iso8859_1_to_utf8,
        PG_LATIN1,
        PG_UTF8,
        &[b'a', 0, b'b'],
        true,
    )
    .unwrap();
    assert_eq!((consumed, out.as_slice()), (1, &b"a"[..]));
}

#[test]
fn no_error_stops_at_untranslatable() {
    let src = "ab水cd".as_bytes();
    let (consumed, out) = call(fc_utf8_to_iso8859_1, PG_UTF8, PG_LATIN1, src, true).unwrap();
    assert_eq!((consumed, out.as_slice()), (2, &b"ab"[..]));
    let (consumed, out) = call(fc_utf8_to_win, PG_UTF8, PG_WIN1252, src, true).unwrap();
    assert_eq!((consumed, out.as_slice()), (2, &b"ab"[..]));
}

// WIN1252 reference: the 27 non-latin1-identity positions (0x80..0x9F) per
// the Unicode.org CP1252 table; remaining high bytes map like LATIN1.
const WIN1252_C1: &[(u8, &str)] = &[
    (0x80, "\u{20AC}"),
    (0x82, "\u{201A}"),
    (0x83, "\u{0192}"),
    (0x84, "\u{201E}"),
    (0x85, "\u{2026}"),
    (0x86, "\u{2020}"),
    (0x87, "\u{2021}"),
    (0x88, "\u{02C6}"),
    (0x89, "\u{2030}"),
    (0x8A, "\u{0160}"),
    (0x8B, "\u{2039}"),
    (0x8C, "\u{0152}"),
    (0x8E, "\u{017D}"),
    (0x91, "\u{2018}"),
    (0x92, "\u{2019}"),
    (0x93, "\u{201C}"),
    (0x94, "\u{201D}"),
    (0x95, "\u{2022}"),
    (0x96, "\u{2013}"),
    (0x97, "\u{2014}"),
    (0x98, "\u{02DC}"),
    (0x99, "\u{2122}"),
    (0x9A, "\u{0161}"),
    (0x9B, "\u{203A}"),
    (0x9C, "\u{0153}"),
    (0x9E, "\u{017E}"),
    (0x9F, "\u{0178}"),
];

#[test]
fn win1252_exhaustive_vs_reference() {
    for b in 1u8..=0xff {
        let expected: Option<String> = if b < 0x80 {
            Some((b as char).to_string())
        } else if (0x80..=0x9f).contains(&b) {
            WIN1252_C1
                .iter()
                .find(|(w, _)| *w == b)
                .map(|(_, u)| u.to_string())
        } else {
            Some(char::from_u32(b as u32).unwrap().to_string())
        };
        match expected {
            Some(u) => {
                assert_eq!(
                    ok(fc_win_to_utf8, PG_WIN1252, PG_UTF8, &[b]),
                    u.as_bytes(),
                    "win1252 0x{b:02x}"
                );
                assert_eq!(
                    ok(fc_utf8_to_win, PG_UTF8, PG_WIN1252, u.as_bytes()),
                    [b],
                    "utf8->win1252 0x{b:02x}"
                );
            }
            None => {
                // 0x81/0x8D/0x8F/0x90/0x9D are unmapped in CP1252.
                let e = err(fc_win_to_utf8, PG_WIN1252, PG_UTF8, &[b]);
                assert_eq!(e.sqlstate(), types_error::ERRCODE_UNTRANSLATABLE_CHARACTER);
                assert_eq!(
                    e.message(),
                    format!(
                        "character with byte sequence 0x{b:02x} in encoding \"WIN1252\" has no equivalent in encoding \"UTF8\""
                    )
                );
            }
        }
    }
}

// LATIN9 (ISO 8859-15) differs from LATIN1 at exactly these 8 positions.
const LATIN9_DELTA: &[(u8, &str)] = &[
    (0xA4, "\u{20AC}"),
    (0xA6, "\u{0160}"),
    (0xA8, "\u{0161}"),
    (0xB4, "\u{017D}"),
    (0xB8, "\u{017E}"),
    (0xBC, "\u{0152}"),
    (0xBD, "\u{0153}"),
    (0xBE, "\u{0178}"),
];

#[test]
fn latin9_exhaustive_vs_reference() {
    for b in 1u8..=0xff {
        let expected: String = if b < 0xa0 {
            char::from_u32(b as u32).unwrap().to_string()
        } else {
            LATIN9_DELTA
                .iter()
                .find(|(w, _)| *w == b)
                .map(|(_, u)| u.to_string())
                .unwrap_or_else(|| char::from_u32(b as u32).unwrap().to_string())
        };
        assert_eq!(
            ok(fc_iso8859_to_utf8, PG_LATIN9, PG_UTF8, &[b]),
            expected.as_bytes(),
            "latin9 0x{b:02x}"
        );
        assert_eq!(
            ok(fc_utf8_to_iso8859, PG_UTF8, PG_LATIN9, expected.as_bytes()),
            [b],
            "utf8->latin9 0x{b:02x}"
        );
    }
}

#[test]
fn latin9_untranslatable_delta_chars() {
    // LATIN1's 0xA4 (currency sign) has no LATIN9 equivalent.
    let e = err(fc_utf8_to_iso8859, PG_UTF8, PG_LATIN9, "\u{A4}".as_bytes());
    assert_eq!(e.sqlstate(), types_error::ERRCODE_UNTRANSLATABLE_CHARACTER);
    assert_eq!(
        e.message(),
        "character with byte sequence 0xc2 0xa4 in encoding \"UTF8\" has no equivalent in encoding \"LATIN9\""
    );
}

#[test]
fn multibyte_output_and_consumed_counts() {
    let (consumed, out) = call(
        fc_win_to_utf8,
        PG_WIN1252,
        PG_UTF8,
        &[0x80, b'1', 0x99],
        false,
    )
    .unwrap();
    assert_eq!(consumed, 3);
    assert_eq!(out, "\u{20AC}1\u{2122}".as_bytes());
}

#[test]
fn check_args_rejects_wrong_encodings() {
    let e = err(fc_iso8859_1_to_utf8, PG_LATIN2, PG_UTF8, b"x");
    assert!(e.message().contains("expected source encoding \"LATIN1\""));
    let e = err(fc_win_to_utf8, PG_WIN1252, PG_LATIN1, b"x");
    assert!(e
        .message()
        .contains("expected destination encoding \"UTF8\""));
}

#[test]
fn non_family_encoding_is_internal_error() {
    let e = err(fc_utf8_to_win, PG_UTF8, PG_LATIN2, b"x");
    assert_eq!(
        e.message(),
        "unexpected encoding ID 9 for WIN character sets"
    );
}

#[test]
#[should_panic(expected = "not ported")]
fn unported_family_member_panics_loudly() {
    let _ = call(fc_utf8_to_win, PG_UTF8, PG_WIN1250, b"x", false);
}

#[test]
fn conv_builtin_lookup() {
    assert_eq!(conv_builtin(4374).unwrap().name, "iso8859_1_to_utf8");
    assert_eq!(conv_builtin(4375).unwrap().name, "utf8_to_iso8859_1");
    assert_eq!(conv_builtin(4358).unwrap().name, "utf8_to_win");
    assert_eq!(conv_builtin(4359).unwrap().name, "win_to_utf8");
    assert_eq!(conv_builtin(4372).unwrap().name, "utf8_to_iso8859");
    assert_eq!(conv_builtin(4373).unwrap().name, "iso8859_to_utf8");
    assert!(conv_builtin(1).is_none());
    for w in CONV_BUILTINS.windows(2) {
        assert!(w[0].foid < w[1].foid);
    }
}
