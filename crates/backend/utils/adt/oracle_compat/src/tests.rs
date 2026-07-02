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

#[test]
fn embedded_nul_stops_case_walk() {
    utf8();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(lower(mcx, b"AB\x00CD", C_COLLATION_OID).unwrap().data(), b"ab\x00CD");
}
