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
fn conv_builtin_lookup() {
    assert_eq!(conv_builtin(4374).unwrap().name, "iso8859_1_to_utf8");
    assert_eq!(conv_builtin(4375).unwrap().name, "utf8_to_iso8859_1");
    assert_eq!(conv_builtin(4358).unwrap().name, "utf8_to_win");
    assert_eq!(conv_builtin(4359).unwrap().name, "win_to_utf8");
    assert_eq!(conv_builtin(4372).unwrap().name, "utf8_to_iso8859");
    assert_eq!(conv_builtin(4373).unwrap().name, "iso8859_to_utf8");
    assert_eq!(conv_builtin(4302).unwrap().name, "koi8r_to_mic");
    assert_eq!(
        conv_builtin(4387).unwrap().name,
        "shift_jis_2004_to_euc_jis_2004"
    );
    assert_eq!(CONV_BUILTINS.len(), 84);
    assert!(conv_builtin(1).is_none());
    assert!(conv_builtin(4350).is_none());
    for w in CONV_BUILTINS.windows(2) {
        assert!(w[0].foid < w[1].foid);
    }
}

use super::utf8_procs::*;
use wchar::{
    PG_BIG5, PG_EUC_CN, PG_EUC_JIS_2004, PG_EUC_JP, PG_EUC_KR, PG_EUC_TW, PG_GB18030, PG_GBK,
    PG_JOHAB, PG_KOI8R, PG_KOI8U, PG_SHIFT_JIS_2004, PG_SJIS, PG_UHC,
};

/// Single-byte families: every byte the to-utf8 tree maps must roundtrip
/// through utf8 back to itself.
#[test]
fn single_byte_families_roundtrip_all_mapped_bytes() {
    let cases: &[(PGFunction, PGFunction, pg_enc)] = &[
        (fc_win_to_utf8, fc_utf8_to_win, PG_WIN866),
        (fc_win_to_utf8, fc_utf8_to_win, PG_WIN874),
        (fc_win_to_utf8, fc_utf8_to_win, PG_WIN1250),
        (fc_win_to_utf8, fc_utf8_to_win, PG_WIN1251),
        (fc_win_to_utf8, fc_utf8_to_win, PG_WIN1253),
        (fc_win_to_utf8, fc_utf8_to_win, PG_WIN1254),
        (fc_win_to_utf8, fc_utf8_to_win, PG_WIN1255),
        (fc_win_to_utf8, fc_utf8_to_win, PG_WIN1256),
        (fc_win_to_utf8, fc_utf8_to_win, PG_WIN1257),
        (fc_win_to_utf8, fc_utf8_to_win, PG_WIN1258),
        (fc_iso8859_to_utf8, fc_utf8_to_iso8859, PG_LATIN2),
        (fc_iso8859_to_utf8, fc_utf8_to_iso8859, PG_LATIN3),
        (fc_iso8859_to_utf8, fc_utf8_to_iso8859, PG_LATIN4),
        (fc_iso8859_to_utf8, fc_utf8_to_iso8859, PG_LATIN5),
        (fc_iso8859_to_utf8, fc_utf8_to_iso8859, PG_LATIN6),
        (fc_iso8859_to_utf8, fc_utf8_to_iso8859, PG_LATIN7),
        (fc_iso8859_to_utf8, fc_utf8_to_iso8859, PG_LATIN8),
        (fc_iso8859_to_utf8, fc_utf8_to_iso8859, PG_LATIN10),
        (fc_iso8859_to_utf8, fc_utf8_to_iso8859, PG_ISO_8859_5),
        (fc_iso8859_to_utf8, fc_utf8_to_iso8859, PG_ISO_8859_6),
        (fc_iso8859_to_utf8, fc_utf8_to_iso8859, PG_ISO_8859_7),
        (fc_iso8859_to_utf8, fc_utf8_to_iso8859, PG_ISO_8859_8),
        (fc_koi8r_to_utf8, fc_utf8_to_koi8r, PG_KOI8R),
        (fc_koi8u_to_utf8, fc_utf8_to_koi8u, PG_KOI8U),
    ];
    for &(to_utf8, from_utf8, enc) in cases {
        let mut mapped = 0;
        for b in 0x80u8..=0xff {
            let (consumed, utf8) = call(to_utf8, enc, PG_UTF8, &[b], true).unwrap();
            if consumed == 0 {
                continue;
            }
            mapped += 1;
            let back = ok(from_utf8, PG_UTF8, enc, &utf8);
            assert_eq!(back, [b], "enc {enc} byte 0x{b:02x}");
        }
        assert!(mapped > 60, "enc {enc}: only {mapped} high bytes mapped");
    }
}

/// Double-byte radix families: every verifier-accepted 2-byte code the map
/// translates must roundtrip.
#[test]
fn double_byte_families_roundtrip_all_mapped_codes() {
    let cases: &[(PGFunction, PGFunction, pg_enc)] = &[
        (fc_euc_cn_to_utf8, fc_utf8_to_euc_cn, PG_EUC_CN),
        (fc_euc_kr_to_utf8, fc_utf8_to_euc_kr, PG_EUC_KR),
        (fc_big5_to_utf8, fc_utf8_to_big5, PG_BIG5),
        (fc_gbk_to_utf8, fc_utf8_to_gbk, PG_GBK),
        (fc_uhc_to_utf8, fc_utf8_to_uhc, PG_UHC),
        (fc_johab_to_utf8, fc_utf8_to_johab, PG_JOHAB),
        (fc_sjis_to_utf8, fc_utf8_to_sjis, PG_SJIS),
    ];
    for &(to_utf8, from_utf8, enc) in cases {
        let mut mapped = 0usize;
        let mut asymmetric = 0usize;
        for b1 in 0x80u16..=0xff {
            for b2 in 0x00u16..=0xff {
                let src = [b1 as u8, b2 as u8];
                if pg_encoding_verifymbchar(enc, &src) != 2 {
                    continue;
                }
                let (consumed, utf8) = call(to_utf8, enc, PG_UTF8, &src, true).unwrap();
                if consumed != 2 {
                    continue;
                }
                mapped += 1;
                let (back_consumed, back) = call(from_utf8, PG_UTF8, enc, &utf8, true).unwrap();
                if back_consumed as usize == utf8.len() && back == src {
                    continue;
                }
                // C maps are not all bijective (e.g. SJIS/BIG5 dual codes);
                // count rather than fail, and bound the count below.
                asymmetric += 1;
            }
        }
        assert!(mapped > 5000, "enc {enc}: only {mapped} codes mapped");
        assert!(
            asymmetric * 10 <= mapped,
            "enc {enc}: {asymmetric} asymmetric of {mapped}"
        );
    }
}

#[test]
fn known_cjk_vectors() {
    assert_eq!(ok(fc_euc_kr_to_utf8, PG_EUC_KR, PG_UTF8, &[0xb0, 0xa1]), "가".as_bytes());
    assert_eq!(ok(fc_uhc_to_utf8, PG_UHC, PG_UTF8, &[0xb0, 0xa1]), "가".as_bytes());
    assert_eq!(ok(fc_big5_to_utf8, PG_BIG5, PG_UTF8, &[0xa4, 0x40]), "一".as_bytes());
    assert_eq!(ok(fc_gbk_to_utf8, PG_GBK, PG_UTF8, &[0xd6, 0xd0]), "中".as_bytes());
    assert_eq!(ok(fc_euc_cn_to_utf8, PG_EUC_CN, PG_UTF8, &[0xd6, 0xd0]), "中".as_bytes());
    assert_eq!(ok(fc_sjis_to_utf8, PG_SJIS, PG_UTF8, &[0x82, 0xa0]), "あ".as_bytes());
    assert_eq!(ok(fc_euc_jp_to_utf8, PG_EUC_JP, PG_UTF8, &[0xa4, 0xa2]), "あ".as_bytes());
    assert_eq!(
        ok(fc_gb18030_to_utf8, PG_GB18030, PG_UTF8, &[0xd6, 0xd0]),
        "中".as_bytes()
    );
    assert_eq!(ok(fc_utf8_to_gbk, PG_UTF8, PG_GBK, "中".as_bytes()), [0xd6, 0xd0]);
}

#[test]
fn gb18030_algorithmic_ranges() {
    // U+10000 is the first algorithmic 4-byte code: 0x90308130.
    let four = ok(
        fc_utf8_to_gb18030,
        PG_UTF8,
        PG_GB18030,
        "\u{10000}".as_bytes(),
    );
    assert_eq!(four, [0x90, 0x30, 0x81, 0x30]);
    assert_eq!(
        ok(fc_gb18030_to_utf8, PG_GB18030, PG_UTF8, &four),
        "\u{10000}".as_bytes()
    );
    // U+0452 -> 0x8130D330 (range table start).
    let four = ok(fc_utf8_to_gb18030, PG_UTF8, PG_GB18030, "\u{452}".as_bytes());
    assert_eq!(four, [0x81, 0x30, 0xd3, 0x30]);
    assert_eq!(
        ok(fc_gb18030_to_utf8, PG_GB18030, PG_UTF8, &four),
        "\u{452}".as_bytes()
    );
    // U+10FFFF, the top of the linear range.
    let four = ok(
        fc_utf8_to_gb18030,
        PG_UTF8,
        PG_GB18030,
        "\u{10FFFF}".as_bytes(),
    );
    assert_eq!(four, [0xe3, 0x32, 0x9a, 0x35]);
    assert_eq!(
        ok(fc_gb18030_to_utf8, PG_GB18030, PG_UTF8, &four),
        "\u{10FFFF}".as_bytes()
    );
}

#[test]
fn euc_jis_2004_combined_maps() {
    // 0xa4f7 <-> U+304B U+309A (first LUmap/ULmap combined row).
    let utf8 = ok(
        fc_euc_jis_2004_to_utf8,
        PG_EUC_JIS_2004,
        PG_UTF8,
        &[0xa4, 0xf7],
    );
    assert_eq!(utf8, "\u{304B}\u{309A}".as_bytes());
    assert_eq!(
        ok(fc_utf8_to_euc_jis_2004, PG_UTF8, PG_EUC_JIS_2004, &utf8),
        [0xa4, 0xf7]
    );
    // Truncated second char of a potential combined pair errors as invalid.
    let mut src = "\u{304B}".as_bytes().to_vec();
    src.push(0xe3);
    let e = err(fc_utf8_to_euc_jis_2004, PG_UTF8, PG_EUC_JIS_2004, &src);
    assert_eq!(
        e.sqlstate(),
        types_error::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE
    );
}

#[test]
fn shift_jis_2004_combined_roundtrip() {
    let sjis = ok(
        fc_utf8_to_shift_jis_2004,
        PG_UTF8,
        PG_SHIFT_JIS_2004,
        "\u{304B}\u{309A}".as_bytes(),
    );
    assert_eq!(
        ok(fc_shift_jis_2004_to_utf8, PG_SHIFT_JIS_2004, PG_UTF8, &sjis),
        "\u{304B}\u{309A}".as_bytes()
    );
}

#[test]
fn euc_jp_sjis_direct() {
    use super::euc_jp_and_sjis::{fc_euc_jp_to_sjis, fc_sjis_to_euc_jp};
    assert_eq!(
        ok(fc_sjis_to_euc_jp, PG_SJIS, PG_EUC_JP, &[0x82, 0xa0]),
        [0xa4, 0xa2]
    );
    assert_eq!(
        ok(fc_euc_jp_to_sjis, PG_EUC_JP, PG_SJIS, &[0xa4, 0xa2]),
        [0x82, 0xa0]
    );
    // 1-byte kana: SJIS 0xb1 <-> EUC 0x8e 0xb1.
    assert_eq!(ok(fc_sjis_to_euc_jp, PG_SJIS, PG_EUC_JP, &[0xb1]), [0x8e, 0xb1]);
    assert_eq!(ok(fc_euc_jp_to_sjis, PG_EUC_JP, PG_SJIS, &[0x8e, 0xb1]), [0xb1]);
    // Full JIS X0208 roundtrip through SJIS.
    let mut count = 0;
    for c1 in 0xa1u8..=0xf4 {
        for c2 in 0xa1u8..=0xfe {
            let euc = [c1, c2];
            let (consumed, sjis) = call(fc_euc_jp_to_sjis, PG_EUC_JP, PG_SJIS, &euc, true).unwrap();
            if consumed != 2 {
                continue;
            }
            let (bc, back) = call(fc_sjis_to_euc_jp, PG_SJIS, PG_EUC_JP, &sjis, true).unwrap();
            if bc as usize == sjis.len() && back == euc {
                count += 1;
            }
        }
    }
    assert!(count > 7000, "only {count} X0208 codes roundtrip");
}

#[test]
fn euc_jp_sjis_ibm_kanji() {
    use super::euc_jp_and_sjis::{fc_euc_jp_to_sjis, fc_sjis_to_euc_jp};
    // First ibmkanji row: NEC 0xEEEF -> SJIS 0xfa40 -> EUC 0x8f 0xf3 0xf3.
    assert_eq!(
        ok(fc_sjis_to_euc_jp, PG_SJIS, PG_EUC_JP, &[0xfa, 0x40]),
        [0x8f, 0xf3, 0xf3]
    );
    assert_eq!(
        ok(fc_euc_jp_to_sjis, PG_EUC_JP, PG_SJIS, &[0x8f, 0xf3, 0xf3]),
        [0xfa, 0x40]
    );
    assert_eq!(
        ok(fc_sjis_to_euc_jp, PG_SJIS, PG_EUC_JP, &[0xee, 0xef]),
        [0x8f, 0xf3, 0xf3]
    );
}

#[test]
fn mic_roundtrips() {
    use super::cyrillic_and_mic::{fc_koi8r_to_mic, fc_mic_to_koi8r};
    use super::euc_cn_and_mic::{fc_euc_cn_to_mic, fc_mic_to_euc_cn};
    use super::euc_kr_and_mic::{fc_euc_kr_to_mic, fc_mic_to_euc_kr};
    use super::euc_tw_and_big5::{fc_euc_tw_to_mic, fc_mic_to_euc_tw};
    let mic = ok(fc_koi8r_to_mic, PG_KOI8R, PG_MULE_INTERNAL, &[b'a', 0xc1]);
    assert_eq!(mic, [b'a', LC_KOI8_R, 0xc1]);
    assert_eq!(
        ok(fc_mic_to_koi8r, PG_MULE_INTERNAL, PG_KOI8R, &mic),
        [b'a', 0xc1]
    );
    let mic = ok(fc_euc_cn_to_mic, PG_EUC_CN, PG_MULE_INTERNAL, &[0xd6, 0xd0]);
    assert_eq!(mic, [LC_GB2312_80, 0xd6, 0xd0]);
    assert_eq!(
        ok(fc_mic_to_euc_cn, PG_MULE_INTERNAL, PG_EUC_CN, &mic),
        [0xd6, 0xd0]
    );
    let mic = ok(fc_euc_kr_to_mic, PG_EUC_KR, PG_MULE_INTERNAL, &[0xb0, 0xa1]);
    assert_eq!(mic, [LC_KS5601, 0xb0, 0xa1]);
    assert_eq!(
        ok(fc_mic_to_euc_kr, PG_MULE_INTERNAL, PG_EUC_KR, &mic),
        [0xb0, 0xa1]
    );
    let mic = ok(fc_euc_tw_to_mic, PG_EUC_TW, PG_MULE_INTERNAL, &[0xc4, 0xe3]);
    assert_eq!(mic, [LC_CNS11643_1, 0xc4, 0xe3]);
    assert_eq!(
        ok(fc_mic_to_euc_tw, PG_MULE_INTERNAL, PG_EUC_TW, &mic),
        [0xc4, 0xe3]
    );
    // SS2 plane-2 EUC_TW char through MIC.
    let src = [SS2, 0xa2, 0xa1, 0xa1];
    let mic = ok(fc_euc_tw_to_mic, PG_EUC_TW, PG_MULE_INTERNAL, &src);
    assert_eq!(mic, [LC_CNS11643_2, 0xa1, 0xa1]);
    assert_eq!(ok(fc_mic_to_euc_tw, PG_MULE_INTERNAL, PG_EUC_TW, &mic), src);
}

#[test]
fn cyrillic_local2local_roundtrip() {
    use super::cyrillic_and_mic::*;
    let pairs: &[(PGFunction, PGFunction, pg_enc, pg_enc)] = &[
        (fc_koi8r_to_win1251, fc_win1251_to_koi8r, PG_KOI8R, PG_WIN1251),
        (fc_koi8r_to_win866, fc_win866_to_koi8r, PG_KOI8R, PG_WIN866),
        (fc_koi8r_to_iso, fc_iso_to_koi8r, PG_KOI8R, PG_ISO_8859_5),
        (fc_win1251_to_iso, fc_iso_to_win1251, PG_WIN1251, PG_ISO_8859_5),
        (fc_win866_to_iso, fc_iso_to_win866, PG_WIN866, PG_ISO_8859_5),
        (fc_win866_to_win1251, fc_win1251_to_win866, PG_WIN866, PG_WIN1251),
    ];
    for &(fwd, back, src_enc, dst_enc) in pairs {
        let mut mapped = 0;
        for b in 0x80u8..=0xff {
            let (consumed, out) = call(fwd, src_enc, dst_enc, &[b], true).unwrap();
            if consumed != 1 {
                continue;
            }
            let (bc, round) = call(back, dst_enc, src_enc, &out, true).unwrap();
            if bc == 1 && round == [b] {
                mapped += 1;
            }
        }
        assert!(mapped > 60, "{src_enc}<->{dst_enc}: {mapped} roundtrip");
    }
}

#[test]
fn latin2_win1250_roundtrip() {
    use super::latin2_and_win1250::*;
    // 0xa1 in LATIN2 (Aogonek) is 0xa5 in WIN1250.
    assert_eq!(
        ok(fc_latin2_to_win1250, PG_LATIN2, PG_WIN1250, &[0xa1]),
        [0xa5]
    );
    assert_eq!(
        ok(fc_win1250_to_latin2, PG_WIN1250, PG_LATIN2, &[0xa5]),
        [0xa1]
    );
    let mic = ok(fc_win1250_to_mic, PG_WIN1250, PG_MULE_INTERNAL, &[0xa5]);
    assert_eq!(mic, [LC_ISO8859_2, 0xa1]);
    assert_eq!(
        ok(fc_mic_to_win1250, PG_MULE_INTERNAL, PG_WIN1250, &mic),
        [0xa5]
    );
}

#[test]
fn latin_mic_passthrough() {
    use super::latin_and_mic::*;
    let mic = ok(fc_latin1_to_mic, PG_LATIN1, PG_MULE_INTERNAL, &[b'x', 0xe9]);
    assert_eq!(mic, [b'x', LC_ISO8859_1, 0xe9]);
    assert_eq!(
        ok(fc_mic_to_latin1, PG_MULE_INTERNAL, PG_LATIN1, &mic),
        [b'x', 0xe9]
    );
    let e = err(fc_mic_to_latin3, PG_MULE_INTERNAL, PG_LATIN3, &mic);
    assert_eq!(e.sqlstate(), types_error::ERRCODE_UNTRANSLATABLE_CHARACTER);
}

#[test]
fn euc2004_sjis2004_direct() {
    use super::euc2004_sjis2004::*;
    assert_eq!(
        ok(
            fc_euc_jis_2004_to_shift_jis_2004,
            PG_EUC_JIS_2004,
            PG_SHIFT_JIS_2004,
            &[0xa4, 0xa2]
        ),
        [0x82, 0xa0]
    );
    assert_eq!(
        ok(
            fc_shift_jis_2004_to_euc_jis_2004,
            PG_SHIFT_JIS_2004,
            PG_EUC_JIS_2004,
            &[0x82, 0xa0]
        ),
        [0xa4, 0xa2]
    );
    // Exhaustive plane-1 roundtrip EUC -> SJIS -> EUC.
    let mut count = 0;
    for c1 in 0xa1u8..=0xfe {
        for c2 in 0xa1u8..=0xfe {
            let euc = [c1, c2];
            let (consumed, sjis) = call(
                fc_euc_jis_2004_to_shift_jis_2004,
                PG_EUC_JIS_2004,
                PG_SHIFT_JIS_2004,
                &euc,
                true,
            )
            .unwrap();
            if consumed != 2 {
                continue;
            }
            let back = ok(
                fc_shift_jis_2004_to_euc_jis_2004,
                PG_SHIFT_JIS_2004,
                PG_EUC_JIS_2004,
                &sjis,
            );
            assert_eq!(back, euc, "euc_jis_2004 0x{c1:02x}{c2:02x}");
            count += 1;
        }
    }
    assert_eq!(count, 94 * 94);
}

#[test]
fn new_module_error_texts_are_c_exact() {
    let e = err(fc_big5_to_utf8, PG_BIG5, PG_UTF8, &[0xa1]);
    assert_eq!(
        e.message(),
        "invalid byte sequence for encoding \"BIG5\": 0xa1"
    );
    let e = err(fc_utf8_to_gbk, PG_UTF8, PG_GBK, "\u{1F600}".as_bytes());
    assert_eq!(e.sqlstate(), types_error::ERRCODE_UNTRANSLATABLE_CHARACTER);
    let e = err(fc_utf8_to_big5, PG_UTF8, PG_BIG5, "\u{1F600}".as_bytes());
    assert_eq!(
        e.message(),
        "character with byte sequence 0xf0 0x9f 0x98 0x80 in encoding \"UTF8\" has no equivalent in encoding \"BIG5\""
    );
    use super::euc_cn_and_mic::fc_mic_to_euc_cn;
    let e = err(
        fc_mic_to_euc_cn,
        PG_MULE_INTERNAL,
        PG_EUC_CN,
        &[LC_KS5601, 0xb0, 0xa1],
    );
    assert_eq!(
        e.message(),
        "character with byte sequence 0x93 0xb0 0xa1 in encoding \"MULE_INTERNAL\" has no equivalent in encoding \"EUC_CN\""
    );
}
