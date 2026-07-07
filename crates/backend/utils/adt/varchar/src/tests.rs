use mcx::MemoryContext;
use types_core::C_COLLATION_OID;
use types_error::SoftErrorContext;

use crate::*;

const C: u32 = C_COLLATION_OID;

fn typmod(n: i32) -> i32 {
    n + VARHDRSZ as i32
}

#[test]
fn bpchar_input_pads_to_typmod() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let v = bpchar_input(mcx, b"abc", typmod(5), None).unwrap().unwrap();
    assert_eq!(v.data(), b"abc  ");
    let v = bpchar_input(mcx, b"", typmod(3), None).unwrap().unwrap();
    assert_eq!(v.data(), b"   ");
    let v = bpchar_input(mcx, b"abc", typmod(3), None).unwrap().unwrap();
    assert_eq!(v.data(), b"abc");
}

#[test]
fn bpchar_input_typmod_minus_one_passthrough() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let v = bpchar_input(mcx, b"abc", -1, None).unwrap().unwrap();
    assert_eq!(v.data(), b"abc");
    let v = bpchar_input(mcx, b"", -1, None).unwrap().unwrap();
    assert_eq!(v.data(), b"");
}

#[test]
fn bpchar_input_truncates_trailing_spaces_only() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let v = bpchar_input(mcx, b"abc   ", typmod(3), None).unwrap().unwrap();
    assert_eq!(v.data(), b"abc");
    let err = bpchar_input(mcx, b"abcd", typmod(3), None).unwrap_err();
    assert_eq!(err.message(), "value too long for type character(3)");
    assert_eq!(err.sqlstate(), types_error::ERRCODE_STRING_DATA_RIGHT_TRUNCATION);
    let err = bpchar_input(mcx, b"abc d", typmod(3), None).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_STRING_DATA_RIGHT_TRUNCATION);
}

#[test]
fn bpchar_input_soft_error() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut esc = SoftErrorContext::new(true);
    let v = bpchar_input(mcx, b"abcd", typmod(3), Some(&mut esc)).unwrap();
    assert!(v.is_none());
    assert!(esc.error_occurred());
    assert_eq!(
        esc.error().unwrap().message(),
        "value too long for type character(3)"
    );
}

#[test]
fn bpchar_input_multibyte_counts_characters() {
    mbutils::SetDatabaseEncoding(wchar::PG_UTF8).unwrap();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    // "é" is 2 bytes, 1 character.
    let v = bpchar_input(mcx, "éé".as_bytes(), typmod(3), None).unwrap().unwrap();
    assert_eq!(v.data(), "éé ".as_bytes());
    let err = bpchar_input(mcx, "ééé".as_bytes(), typmod(2), None).unwrap_err();
    assert_eq!(err.message(), "value too long for type character(2)");
    let v = bpchar_input(mcx, "ééé".as_bytes(), typmod(4), None).unwrap().unwrap();
    assert_eq!(v.data(), "ééé ".as_bytes());
}

#[test]
fn varchar_input_clips_and_errors() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let v = varchar_input(mcx, b"abc", typmod(5), None).unwrap().unwrap();
    assert_eq!(v.data(), b"abc");
    let v = varchar_input(mcx, b"abc   ", typmod(3), None).unwrap().unwrap();
    assert_eq!(v.data(), b"abc");
    let v = varchar_input(mcx, b"abc", -1, None).unwrap().unwrap();
    assert_eq!(v.data(), b"abc");
    let err = varchar_input(mcx, b"abcd", typmod(3), None).unwrap_err();
    assert_eq!(
        err.message(),
        "value too long for type character varying(3)"
    );
    assert_eq!(err.sqlstate(), types_error::ERRCODE_STRING_DATA_RIGHT_TRUNCATION);
}

#[test]
fn varchar_input_multibyte() {
    mbutils::SetDatabaseEncoding(wchar::PG_UTF8).unwrap();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let v = varchar_input(mcx, "ééé".as_bytes(), typmod(3), None).unwrap().unwrap();
    assert_eq!(v.data(), "ééé".as_bytes());
    let err = varchar_input(mcx, "ééé".as_bytes(), typmod(2), None).unwrap_err();
    assert_eq!(
        err.message(),
        "value too long for type character varying(2)"
    );
}

#[test]
fn bpchar_cast_coercion() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert!(bpchar(mcx, b"abc", -1, false).unwrap().is_none());
    assert!(bpchar(mcx, b"abc", typmod(3), false).unwrap().is_none());
    let v = bpchar(mcx, b"abc", typmod(5), false).unwrap().unwrap();
    assert_eq!(v.data(), b"abc  ");
    // Explicit cast silently truncates; implicit errors on non-space tail.
    let v = bpchar(mcx, b"abcd", typmod(3), true).unwrap().unwrap();
    assert_eq!(v.data(), b"abc");
    let err = bpchar(mcx, b"abcd", typmod(3), false).unwrap_err();
    assert_eq!(err.message(), "value too long for type character(3)");
    let v = bpchar(mcx, b"abc ", typmod(3), false).unwrap().unwrap();
    assert_eq!(v.data(), b"abc");
}

#[test]
fn varchar_cast_coercion() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert!(varchar(mcx, b"abc", -1, false).unwrap().is_none());
    assert!(varchar(mcx, b"abc", typmod(5), false).unwrap().is_none());
    let v = varchar(mcx, b"abcd", typmod(3), true).unwrap().unwrap();
    assert_eq!(v.data(), b"abc");
    let err = varchar(mcx, b"abcd", typmod(3), false).unwrap_err();
    assert_eq!(
        err.message(),
        "value too long for type character varying(3)"
    );
}

#[test]
fn eq_cmp_ignore_trailing_blanks() {
    assert!(bpchareq(b"abc   ", b"abc", C).unwrap());
    assert!(bpchareq(b"", b"   ", C).unwrap());
    assert!(!bpchareq(b"abc", b"abd", C).unwrap());
    assert!(bpcharne(b"abc", b"ab", C).unwrap());
    assert!(!bpcharne(b"abc ", b"abc  ", C).unwrap());
    assert_eq!(bpcharcmp(b"abc  ", b"abc", C).unwrap(), 0);
    assert!(bpcharcmp(b"ab", b"abc", C).unwrap() < 0);
    assert!(bpcharlt(b"ab ", b"abc", C).unwrap());
    assert!(bpcharle(b"abc ", b"abc", C).unwrap());
    assert!(bpchargt(b"abd", b"abc  ", C).unwrap());
    assert!(bpcharge(b"abc", b"abc ", C).unwrap());
    // Trailing blanks are insignificant, embedded/leading ones are not.
    assert!(!bpchareq(b" abc", b"abc", C).unwrap());
    assert!(!bpchareq(b"a bc", b"abc", C).unwrap());
}

#[test]
fn pattern_ops_trim_then_memcmp() {
    assert_eq!(btbpchar_pattern_cmp(b"abc  ", b"abc"), 0);
    assert!(bpchar_pattern_lt(b"ab", b"abc"));
    assert!(bpchar_pattern_le(b"abc ", b"abc"));
    assert!(bpchar_pattern_gt(b"abd", b"abc"));
    assert!(bpchar_pattern_ge(b"abc", b"abc  "));
}

#[test]
fn lengths() {
    assert_eq!(bpcharlen(b"abc  ").unwrap(), 3);
    assert_eq!(bpcharlen(b"").unwrap(), 0);
    assert_eq!(bpcharlen(b"     ").unwrap(), 0);
    assert_eq!(bpcharoctetlen(b"abc  "), 5);
    mbutils::SetDatabaseEncoding(wchar::PG_UTF8).unwrap();
    assert_eq!(bpcharlen("éé ".as_bytes()).unwrap(), 2);
}

#[test]
fn hash_ignores_trailing_blanks() {
    let h1 = hashbpchar(b"abc   ", C).unwrap();
    let h2 = hashbpchar(b"abc", C).unwrap();
    assert_eq!(h1, h2);
    let e1 = hashbpcharextended(b"abc ", C, 42).unwrap();
    let e2 = hashbpcharextended(b"abc", C, 42).unwrap();
    assert_eq!(e1, e2);
    let err = hashbpchar(b"abc", 0).unwrap_err();
    assert_eq!(
        err.message(),
        "could not determine which collation to use for string hashing"
    );
}

#[test]
fn char_name_conversions() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let v = char_bpchar(mcx, b'x' as i8).unwrap();
    assert_eq!(v.data(), b"x");
    let n = bpchar_name(b"abc   ");
    assert_eq!(&n[..3], b"abc");
    assert!(n[3..].iter().all(|&b| b == 0));
    let long = [b'a'; 100];
    let n = bpchar_name(&long);
    assert_eq!(&n[..63], &long[..63]);
    assert_eq!(n[63], 0);
    let mut name = [0u8; NAMEDATALEN];
    name[..3].copy_from_slice(b"abc");
    let v = name_bpchar(mcx, &name).unwrap();
    assert_eq!(v.data(), b"abc");
}

#[test]
fn typmod_in_out() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let arr = cstring_array_1d(&[b"5"]);
    assert_eq!(bpchartypmodin(mcx, &arr).unwrap(), typmod(5));
    assert_eq!(varchartypmodin(mcx, &arr).unwrap(), typmod(5));
    let arr = cstring_array_1d(&[b"0"]);
    let err = bpchartypmodin(mcx, &arr).unwrap_err();
    assert_eq!(err.message(), "length for type char must be at least 1");
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_PARAMETER_VALUE);
    let err = varchartypmodin(mcx, &arr).unwrap_err();
    assert_eq!(err.message(), "length for type varchar must be at least 1");
    let arr = cstring_array_1d(&[b"10485761"]);
    let err = bpchartypmodin(mcx, &arr).unwrap_err();
    assert_eq!(err.message(), "length for type char cannot exceed 10485760");
    let arr = cstring_array_1d(&[b"1", b"2"]);
    let err = bpchartypmodin(mcx, &arr).unwrap_err();
    assert_eq!(err.message(), "invalid type modifier");

    let mut buf = [0u8; 16];
    let n = anychar_typmodout(typmod(5), &mut buf);
    assert_eq!(&buf[..n], b"(5)");
    assert_eq!(anychar_typmodout(-1, &mut buf), 0);
}

// Minimal 1-D cstring[] image (4B varlena header + array header + elements).
fn cstring_array_1d(elems: &[&[u8]]) -> Vec<u8> {
    let mut v = vec![0u8; 4];
    v.extend_from_slice(&1i32.to_ne_bytes());
    v.extend_from_slice(&0i32.to_ne_bytes());
    v.extend_from_slice(&(types_core::CSTRINGOID as u32).to_ne_bytes());
    v.extend_from_slice(&(elems.len() as i32).to_ne_bytes());
    v.extend_from_slice(&1i32.to_ne_bytes());
    while v.len() % 8 != 0 {
        v.push(0);
    }
    for e in elems {
        v.extend_from_slice(e);
        v.push(0);
    }
    let total = (v.len() as u32) << 2;
    v[..4].copy_from_slice(&total.to_ne_bytes());
    v
}
