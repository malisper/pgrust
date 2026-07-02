use mcx::MemoryContext;
use wchar::{PG_LATIN1, PG_UTF8};

use crate::{
    downcase_identifier, downcase_truncate_identifier, parser_errposition, scanner_isspace,
    truncate_identifier, NAMEDATALEN,
};

#[test]
fn downcase_ascii() {
    let ctx = MemoryContext::new("t");
    let out = downcase_truncate_identifier(ctx.mcx(), b"FooBar_123", false, PG_UTF8).unwrap();
    assert_eq!(&out[..], b"foobar_123");
}

#[test]
fn downcase_leaves_high_bit_in_multibyte_encoding() {
    let ctx = MemoryContext::new("t");
    let ident = "S\u{00c9}L".as_bytes();
    let out = downcase_truncate_identifier(ctx.mcx(), ident, false, PG_UTF8).unwrap();
    assert_eq!(&out[..], "s\u{00c9}l".as_bytes());
}

#[test]
fn downcase_high_bit_single_byte_c_locale() {
    // C locale: isupper never true for high-bit bytes, so 0xC9 passes through.
    let ctx = MemoryContext::new("t");
    let out = downcase_truncate_identifier(ctx.mcx(), &[b'A', 0xC9, b'Z'], false, PG_LATIN1).unwrap();
    assert_eq!(&out[..], &[b'a', 0xC9, b'z']);
}

#[test]
fn downcase_truncates_at_namedatalen() {
    let ctx = MemoryContext::new("t");
    let long = [b'X'; 100];
    let out = downcase_truncate_identifier(ctx.mcx(), &long, false, PG_UTF8).unwrap();
    assert_eq!(out.len(), NAMEDATALEN - 1);
    assert!(out.iter().all(|&b| b == b'x'));
}

#[test]
fn downcase_no_truncate_flag() {
    let ctx = MemoryContext::new("t");
    let long = [b'y'; 100];
    let out = downcase_identifier(ctx.mcx(), &long, false, false, PG_UTF8).unwrap();
    assert_eq!(out.len(), 100);
}

#[test]
fn truncate_short_ident_untouched() {
    let ctx = MemoryContext::new("t");
    let mut v = mcx::slice_in(ctx.mcx(), b"short".as_slice()).unwrap();
    truncate_identifier(&mut v, false, PG_UTF8).unwrap();
    assert_eq!(&v[..], b"short");
}

#[test]
fn truncate_respects_multibyte_boundary() {
    let ctx = MemoryContext::new("t");
    let mut ident = alloc_ident(&ctx, 62);
    ident.extend_from_slice("\u{00e9}\u{00e9}".as_bytes());
    assert_eq!(ident.len(), 66);
    truncate_identifier(&mut ident, false, PG_UTF8).unwrap();
    // limit 63: 62 ascii + one 2-byte char would hit 64, so clip stays at 62.
    assert_eq!(ident.len(), 62);
}

#[test]
fn truncate_single_byte_encoding_stops_at_nul() {
    let ctx = MemoryContext::new("t");
    let mut ident = alloc_ident(&ctx, 70);
    ident[10] = 0;
    truncate_identifier(&mut ident, false, PG_LATIN1).unwrap();
    assert_eq!(ident.len(), 10);
}

fn alloc_ident<'a>(ctx: &'a MemoryContext, n: usize) -> mcx::PgVec<'a, u8> {
    mcx::vec_from_elem_in(ctx.mcx(), b'a', n)
}

#[test]
fn scanner_isspace_matches_scan_l() {
    for ch in [b' ', b'\t', b'\n', b'\r', 0x0b, 0x0c] {
        assert!(scanner_isspace(ch));
    }
    for ch in [b'a', b'0', 0x00, 0xA0, b'_'] {
        assert!(!scanner_isspace(ch));
    }
}

#[test]
fn errposition_missing_inputs() {
    assert_eq!(parser_errposition(Some(b"select 1"), -1, PG_UTF8), 0);
    assert_eq!(parser_errposition(None, 3, PG_UTF8), 0);
}

#[test]
fn errposition_counts_characters_not_bytes() {
    let src = "s\u{00e9}lect 1".as_bytes();
    assert_eq!(parser_errposition(Some(src), 3, PG_UTF8), 3);
    assert_eq!(parser_errposition(Some(src), 0, PG_UTF8), 1);
    assert_eq!(parser_errposition(Some(src), 3, PG_LATIN1), 4);
}
