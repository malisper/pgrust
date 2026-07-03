use super::*;
use mcx::MemoryContext;
use types_core::C_COLLATION_OID;
use types_error::{ERRCODE_INDETERMINATE_COLLATION, ERRCODE_PROGRAM_LIMIT_EXCEEDED};

fn utf8() {
    mbutils::SetDatabaseEncoding(wchar::PG_UTF8).unwrap();
}

#[test]
fn case_functions_c_collation() {
    utf8();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(lower(mcx, b"Hello, World!", C_COLLATION_OID).unwrap().data(), b"hello, world!");
    assert_eq!(upper(mcx, b"Hello, World!", C_COLLATION_OID).unwrap().data(), b"HELLO, WORLD!");
    assert_eq!(
        initcap(mcx, b"hello THE world 3rd time", C_COLLATION_OID).unwrap().data(),
        b"Hello The World 3rd Time"
    );
    assert_eq!(casefold(mcx, b"MiXeD", C_COLLATION_OID).unwrap().data(), b"mixed");
    // ASCII kernels leave multibyte sequences alone under C ctype.
    assert_eq!(
        lower(mcx, "ÄbC".as_bytes(), C_COLLATION_OID).unwrap().data(),
        "Äbc".as_bytes()
    );
    let err = lower(mcx, b"x", 0).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INDETERMINATE_COLLATION);
    assert!(err.message().contains("lower() function"));
}

#[test]
fn pad_functions() {
    utf8();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(lpad(mcx, b"hi", 5, b"xy").unwrap().data(), b"xyxhi");
    assert_eq!(rpad(mcx, b"hi", 5, b"xy").unwrap().data(), b"hixyx");
    assert_eq!(lpad(mcx, b"hello", 3, b"xy").unwrap().data(), b"hel");
    assert_eq!(lpad(mcx, b"hi", -3, b"xy").unwrap().data(), b"");
    assert_eq!(lpad(mcx, b"hi", 5, b"").unwrap().data(), b"hi");
    // Multibyte: char-counted length, pad wraps at a char boundary.
    assert_eq!(
        lpad(mcx, "héllo".as_bytes(), 7, "àb".as_bytes()).unwrap().data(),
        "àbhéllo".as_bytes()
    );
    assert_eq!(
        rpad(mcx, "é".as_bytes(), 3, "ü".as_bytes()).unwrap().data(),
        "éüü".as_bytes()
    );
    let err = lpad(mcx, b"x", i32::MAX, b"y").unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_PROGRAM_LIMIT_EXCEEDED);
    assert_eq!(err.message(), "requested length too large");
}

#[test]
fn trim_functions() {
    utf8();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(btrim(mcx, b"xyxHIxyx", b"xy").unwrap().data(), b"HI");
    assert_eq!(ltrim(mcx, b"xyxHIxyx", b"xy").unwrap().data(), b"HIxyx");
    assert_eq!(rtrim(mcx, b"xyxHIxyx", b"xy").unwrap().data(), b"xyxHI");
    assert_eq!(btrim1(mcx, b"  hi  ").unwrap().data(), b"hi");
    assert_eq!(ltrim1(mcx, b"  hi  ").unwrap().data(), b"hi  ");
    assert_eq!(rtrim1(mcx, b"  hi  ").unwrap().data(), b"  hi");
    assert_eq!(btrim(mcx, b"abc", b"").unwrap().data(), b"abc");
    assert_eq!(btrim(mcx, b"", b"ab").unwrap().data(), b"");
    assert_eq!(btrim(mcx, b"aaaa", b"a").unwrap().data(), b"");
    // Multibyte set members trim whole characters only.
    assert_eq!(
        btrim(mcx, "ééxàéé".as_bytes(), "é".as_bytes()).unwrap().data(),
        "xà".as_bytes()
    );
}

#[test]
fn bytea_trim_functions() {
    utf8();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(byteatrim(mcx, b"\x00abc\x00", b"\x00").unwrap().data(), b"abc");
    assert_eq!(bytealtrim(mcx, b"xxabxx", b"x").unwrap().data(), b"abxx");
    assert_eq!(byteartrim(mcx, b"xxabxx", b"x").unwrap().data(), b"xxab");
    assert_eq!(dobyteatrim(b"abc", b"", true, true), b"abc");
    assert_eq!(dobyteatrim(b"", b"x", true, true), b"");
}

#[test]
fn translate_exact() {
    utf8();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(translate(mcx, b"12345", b"143", b"ax").unwrap().data(), b"a2x5");
    assert_eq!(translate(mcx, b"", b"a", b"b").unwrap().data(), b"");
    assert_eq!(translate(mcx, b"abc", b"", b"").unwrap().data(), b"abc");
    assert_eq!(
        translate(mcx, "héllo".as_bytes(), "é".as_bytes(), b"e").unwrap().data(),
        b"hello"
    );
}

#[test]
fn ascii_and_chr() {
    utf8();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(ascii(b"A").unwrap(), 65);
    assert_eq!(ascii(b"").unwrap(), 0);
    assert_eq!(ascii("é".as_bytes()).unwrap(), 0xE9);
    assert_eq!(ascii("€x".as_bytes()).unwrap(), 0x20AC);
    assert_eq!(chr(mcx, 65).unwrap().data(), b"A");
    assert_eq!(chr(mcx, 0xE9).unwrap().data(), "é".as_bytes());
    assert_eq!(chr(mcx, 0x20AC).unwrap().data(), "€".as_bytes());
    assert_eq!(chr(mcx, 0x10FFFF).unwrap().data().len(), 4);
    assert_eq!(chr(mcx, 0).unwrap_err().message(), "null character not permitted");
    assert_eq!(chr(mcx, -1).unwrap_err().message(), "character number must be positive");
    assert_eq!(
        chr(mcx, 0x110000).unwrap_err().message(),
        "requested character too large for encoding: 1114112"
    );
    assert_eq!(
        chr(mcx, 0xD800).unwrap_err().message(),
        "requested character not valid for encoding: 55296"
    );
}

#[test]
fn repeat_exact() {
    utf8();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(repeat(mcx, b"Pg", 4).unwrap().data(), b"PgPgPgPg");
    assert_eq!(repeat(mcx, b"Pg", 0).unwrap().data(), b"");
    assert_eq!(repeat(mcx, b"Pg", -2).unwrap().data(), b"");
    let err = repeat(mcx, b"Pg", i32::MAX).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_PROGRAM_LIMIT_EXCEEDED);
}

// C: asc_tolower's pnstrdup strnlen-copies, so the NUL and tail are dropped.
#[test]
fn embedded_nul_truncates_case_result() {
    utf8();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(lower(mcx, b"AB\x00CD", C_COLLATION_OID).unwrap().data(), b"ab");
    assert_eq!(upper(mcx, b"ab\x00cd", C_COLLATION_OID).unwrap().data(), b"AB");
    assert_eq!(initcap(mcx, b"ab\x00cd", C_COLLATION_OID).unwrap().data(), b"Ab");
}

#[test]
fn left_right_exact() {
    utf8();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(text_left(mcx, b"hello", 2).unwrap().data(), b"he");
    assert_eq!(text_left(mcx, b"hello", 0).unwrap().data(), b"");
    assert_eq!(text_left(mcx, b"hello", 99).unwrap().data(), b"hello");
    assert_eq!(text_left(mcx, b"hello", -1).unwrap().data(), b"hell");
    assert_eq!(text_left(mcx, b"hello", -99).unwrap().data(), b"");
    assert_eq!(text_left(mcx, b"", 3).unwrap().data(), b"");
    assert_eq!(text_left(mcx, b"hello", i32::MAX).unwrap().data(), b"hello");
    assert_eq!(text_left(mcx, b"hello", i32::MIN).unwrap().data(), b"");
    assert_eq!(text_left(mcx, "日本語".as_bytes(), 2).unwrap().data(), "日本".as_bytes());
    assert_eq!(text_left(mcx, "🐘é".as_bytes(), 1).unwrap().data(), "🐘".as_bytes());
    assert_eq!(text_left(mcx, "日本語".as_bytes(), -1).unwrap().data(), "日本".as_bytes());

    assert_eq!(text_right(mcx, b"hello", 2).unwrap().data(), b"lo");
    assert_eq!(text_right(mcx, b"hello", 0).unwrap().data(), b"");
    assert_eq!(text_right(mcx, b"hello", 99).unwrap().data(), b"hello");
    assert_eq!(text_right(mcx, b"hello", -1).unwrap().data(), b"ello");
    assert_eq!(text_right(mcx, b"hello", -99).unwrap().data(), b"");
    assert_eq!(text_right(mcx, b"", 3).unwrap().data(), b"");
    assert_eq!(text_right(mcx, b"hello", i32::MAX).unwrap().data(), b"hello");
    // C: n = -n wraps at INT32_MIN, stays negative, clips to whole string.
    assert_eq!(text_right(mcx, b"hello", i32::MIN).unwrap().data(), b"hello");
    assert_eq!(text_right(mcx, "日本語".as_bytes(), 2).unwrap().data(), "本語".as_bytes());
    assert_eq!(text_right(mcx, "é🐘".as_bytes(), 1).unwrap().data(), "🐘".as_bytes());
    assert_eq!(text_right(mcx, "日本語".as_bytes(), -1).unwrap().data(), "本語".as_bytes());
}

#[test]
fn reverse_exact() {
    utf8();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(text_reverse(mcx, b"").unwrap().data(), b"");
    assert_eq!(text_reverse(mcx, b"abc").unwrap().data(), b"cba");
    assert_eq!(text_reverse(mcx, "日本語".as_bytes()).unwrap().data(), "語本日".as_bytes());
    assert_eq!(text_reverse(mcx, "a🐘é".as_bytes()).unwrap().data(), "é🐘a".as_bytes());
}

#[test]
fn left_right_reverse_single_byte_encoding() {
    mbutils::SetDatabaseEncoding(wchar::PG_LATIN1).unwrap();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(text_left(mcx, b"ab\xE9d", 3).unwrap().data(), b"ab\xE9");
    assert_eq!(text_right(mcx, b"ab\xE9d", 3).unwrap().data(), b"b\xE9d");
    assert_eq!(text_reverse(mcx, b"ab\xE9d").unwrap().data(), b"d\xE9ba");
}

#[test]
fn chr_ascii_non_utf8_encodings() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    mbutils::SetDatabaseEncoding(wchar::PG_LATIN1).unwrap();
    assert_eq!(chr(mcx, 255).unwrap().data(), b"\xFF");
    assert_eq!(
        chr(mcx, 256).unwrap_err().message(),
        "requested character too large for encoding: 256"
    );
    assert_eq!(ascii(b"\xE9").unwrap(), 0xE9);

    mbutils::SetDatabaseEncoding(wchar::PG_EUC_JP).unwrap();
    assert_eq!(chr(mcx, 127).unwrap().data(), b"\x7F");
    let err = chr(mcx, 128).unwrap_err();
    assert_eq!(err.message(), "requested character too large for encoding: 128");
    assert_eq!(err.sqlstate(), ERRCODE_PROGRAM_LIMIT_EXCEEDED);
    let err = ascii(&[0xA1, 0xA1]).unwrap_err();
    assert_eq!(err.message(), "requested character too large");
    assert_eq!(err.sqlstate(), ERRCODE_PROGRAM_LIMIT_EXCEEDED);
}

#[test]
fn pad_repeat_translate_edges() {
    utf8();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(rpad(mcx, b"hi", -3, b"xy").unwrap().data(), b"");
    assert_eq!(rpad(mcx, b"hello", 3, b"").unwrap().data(), b"hel");
    assert_eq!(lpad(mcx, b"", 3, b"ab").unwrap().data(), b"aba");
    let err = rpad(mcx, b"x", i32::MAX, b"y").unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_PROGRAM_LIMIT_EXCEEDED);
    assert_eq!(err.message(), "requested length too large");
    // Empty pad collapses len to s1len before the overflow guard.
    assert_eq!(lpad(mcx, b"x", i32::MAX, b"").unwrap().data(), b"x");

    assert_eq!(repeat(mcx, b"", 1_000_000).unwrap().data(), b"");
    let err = repeat(mcx, b"Pg", i32::MAX).unwrap_err();
    assert_eq!(err.message(), "requested length too large");

    assert_eq!(translate(mcx, b"abc", b"abc", b"").unwrap().data(), b"");
    assert_eq!(translate(mcx, b"abc", b"", b"xyz").unwrap().data(), b"abc");
    assert_eq!(
        translate(mcx, "a日b".as_bytes(), "日".as_bytes(), "🐘x".as_bytes())
            .unwrap()
            .data(),
        "a🐘b".as_bytes()
    );
    assert_eq!(
        translate(mcx, "aéb".as_bytes(), "xé".as_bytes(), b"y").unwrap().data(),
        b"ab"
    );
}

mod fc_results {
    use datum::{Datum, VarlenaRef};
    use mcx::MemoryContext;
    use types_core::C_COLLATION_OID;
    use types_fmgr::{
        direct_function_call1_coll_in, direct_function_call2_coll_in,
        direct_function_call3_coll_in,
    };

    use crate::builtins::*;

    fn text_image(s: &[u8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(4 + s.len());
        v.extend_from_slice(&datum::varlena::set_varsize_4b(4 + s.len()));
        v.extend_from_slice(s);
        v
    }

    fn text_of(d: Datum) -> &'static [u8] {
        // SAFETY: test results are live 4B-header varlenas kept in the ctx.
        unsafe { VarlenaRef::from_ptr(d.as_usize() as *const u8) }.data()
    }

    #[test]
    fn case_pad_trim_chr() {
        let ctx = MemoryContext::new_bump("t");
        let mixed = text_image(b"AbC");
        let d = direct_function_call1_coll_in(
            fc_lower,
            C_COLLATION_OID,
            ctx.mcx(),
            Datum::from_usize(mixed.as_ptr() as usize),
        )
        .unwrap();
        assert_eq!(text_of(d), b"abc");

        let s = text_image(b"hi");
        let fill = text_image(b"xy");
        let d = direct_function_call3_coll_in(
            fc_lpad,
            0,
            ctx.mcx(),
            Datum::from_usize(s.as_ptr() as usize),
            Datum::from_i32(5),
            Datum::from_usize(fill.as_ptr() as usize),
        )
        .unwrap();
        assert_eq!(text_of(d), b"xyxhi");

        let padded = text_image(b"zzhellozz");
        let set = text_image(b"z");
        let d = direct_function_call2_coll_in(
            fc_btrim,
            0,
            ctx.mcx(),
            Datum::from_usize(padded.as_ptr() as usize),
            Datum::from_usize(set.as_ptr() as usize),
        )
        .unwrap();
        assert_eq!(text_of(d), b"hello");

        let d = direct_function_call1_coll_in(fc_chr, 0, ctx.mcx(), Datum::from_i32(65)).unwrap();
        assert_eq!(text_of(d), b"A");
    }
}

fn install_builtin_cutf8_collation_stub() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        syscache_seams::lookup_pg_collation_locale_row::set(|mcx, _collid| {
            let mut collname = types_tuple::NameData::default();
            collname.namestrcpy("c_utf8_test");
            Ok(Some(syscache_seams::PgCollationLocaleRow {
                collname,
                collnamespace: 11,
                collprovider: pg_locale::COLLPROVIDER_BUILTIN,
                collisdeterministic: true,
                collencoding: -1,
                collcollate: None,
                collctype: None,
                colllocale: Some(mcx::PgString::from_str_in("C.UTF-8", mcx)?),
                collicurules: None,
                collversion: None,
            }))
        });
    });
}

// Verified against live PG 18.3 (builtin C.UTF-8 database, simple mappings).
#[test]
fn case_functions_builtin_cutf8_collation() {
    utf8();
    install_builtin_cutf8_collation_stub();
    let coll = 33333;
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(
        lower(mcx, "ΣΟΦΟΣ Ёлка İ".as_bytes(), coll).unwrap().data(),
        "σοφοσ ёлка i".as_bytes()
    );
    assert_eq!(
        upper(mcx, "straße ﬁ 剣".as_bytes(), coll).unwrap().data(),
        "STRAßE ﬁ 剣".as_bytes()
    );
    assert_eq!(
        initcap(mcx, "über-cool σοφος don't".as_bytes(), coll).unwrap().data(),
        "Über-Cool Σοφος Don'T".as_bytes()
    );
    assert_eq!(
        casefold(mcx, "ΣΟΦΟΣ ẞ".as_bytes(), coll).unwrap().data(),
        "σοφοσ ß".as_bytes()
    );
}
