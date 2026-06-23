//! Tests cover this crate's own logic. The error-reporting paths
//! (`no_error = false` hitting `report_invalid_encoding` /
//! `report_untranslatable_char`) delegate to the unported `utils/mb/mbutils.c`
//! owner via seams that panic until that owner lands, so they are not exercised
//! here; the `validate_encoding` path is owned locally and is tested.

use super::*;
use ::types_error::ERRCODE_INVALID_PARAMETER_VALUE;
use ::types_wchar::encoding::{PG_EUC_CN, PG_LATIN1};
use ::types_wchar::LC_ISO8859_1;

/// Build a radix tree with the given `chars16` payload and a 1-byte-only root
/// covering `[lower, upper]` (the rest of the bounds are zeroed, as the C build
/// leaves unused byte-count levels at 0).
fn radix1(chars16: Vec<u16>, b1root: u32, b1_lower: u8, b1_upper: u8) -> pg_mb_radix_tree {
    pg_mb_radix_tree {
        chars16,
        chars32: Vec::new(),
        b1root,
        b1_lower,
        b1_upper,
        b2root: 0,
        b2_1_lower: 0,
        b2_1_upper: 0,
        b2_2_lower: 0,
        b2_2_upper: 0,
        b3root: 0,
        b3_1_lower: 0,
        b3_1_upper: 0,
        b3_2_lower: 0,
        b3_2_upper: 0,
        b3_3_lower: 0,
        b3_3_upper: 0,
        b4root: 0,
        b4_1_lower: 0,
        b4_1_upper: 0,
        b4_2_lower: 0,
        b4_2_upper: 0,
        b4_3_lower: 0,
        b4_3_upper: 0,
        b4_4_lower: 0,
        b4_4_upper: 0,
    }
}

#[test]
fn local2local_maps_high_bytes() {
    let mut table = [0; 128];
    table[0] = 0x81;

    let result = local2local(b"a\x80", PG_LATIN1, PG_EUC_CN, &table, false).unwrap();

    assert_eq!(result.bytes, b"a\x81");
    assert_eq!(result.converted, 2);
}

#[test]
fn local2local_stops_on_untranslatable_when_no_error() {
    let table = [0; 128];
    let result = local2local(b"a\x80", PG_LATIN1, PG_EUC_CN, &table, true).unwrap();

    assert_eq!(result.bytes, b"a");
    assert_eq!(result.converted, 1);
}

#[test]
fn latin2mic_prefixes_high_bytes() {
    let result = latin2mic(b"a\xe9", LC_ISO8859_1, PG_LATIN1, false).unwrap();

    assert_eq!(result.bytes, b"a\x81\xe9");
    assert_eq!(result.converted, 2);
}

#[test]
fn mic2latin_removes_matching_prefix() {
    let result = mic2latin(b"a\x81\xe9", LC_ISO8859_1, PG_LATIN1, false).unwrap();

    assert_eq!(result.bytes, b"a\xe9");
    assert_eq!(result.converted, 3);
}

#[test]
fn table_variants_round_trip_high_bytes() {
    let mut table = [0; 128];
    table[0x69] = 0xaa;
    table[0x2a] = 0xe9;

    let to_mic = latin2mic_with_table(b"\xe9", LC_ISO8859_1, PG_LATIN1, &table, false).unwrap();
    assert_eq!(to_mic.bytes, b"\x81\xaa");

    let from_mic =
        mic2latin_with_table(b"\x81\xaa", LC_ISO8859_1, PG_LATIN1, &table, false).unwrap();
    assert_eq!(from_mic.bytes, b"\xe9");
}

#[test]
fn radix_tree_handles_one_byte_lookup() {
    let tree = radix1(vec![0u16, 0x1234], 1, 0x80, 0x80);

    assert_eq!(pg_mb_radix_conv(&tree, b"\x80"), 0x1234);
    assert_eq!(pg_mb_radix_conv(&tree, b"\x81"), 0);
}

#[test]
fn utf_to_local_uses_callback() {
    let result = UtfToLocal(
        "a\u{00a3}".as_bytes(),
        None,
        &[],
        Some(|code| if code == 0x0000_c2a3 { 0xa3 } else { 0 }),
        PG_LATIN1,
        false,
    )
    .unwrap();

    assert_eq!(result.bytes, b"a\xa3");
    assert_eq!(result.converted, 3);
}

#[test]
fn local_to_utf_uses_callback() {
    let result = LocalToUtf(
        b"a\xa3",
        None,
        &[],
        Some(|code| if code == 0xa3 { 0x0000_c2a3 } else { 0 }),
        PG_LATIN1,
        false,
    )
    .unwrap();

    assert_eq!(result.bytes, "a\u{00a3}".as_bytes());
    assert_eq!(result.converted, 2);
}

// With a non-empty cmap and no_error = true, an embedded NUL as the *second*
// character must NOT abandon the already-decoded first char. C (conv.c:585)
// computes l = pg_utf_mblen('\0') == 1, pg_utf8_islegal passes, the l>1
// combined-test is skipped, and the first char falls through to the ordinary
// map/conv_func.
#[test]
fn utf_to_local_translates_first_char_before_embedded_nul() {
    let cmap = [pg_utf_to_local_combined {
        utf1: 0xFFFF_FFFF,
        utf2: 0xFFFF_FFFF,
        code: 0xAB,
    }];

    let result = UtfToLocal(
        "\u{00a3}\0".as_bytes(),
        None,
        &cmap,
        Some(|code| if code == 0x0000_c2a3 { 0xa3 } else { 0 }),
        PG_LATIN1,
        true,
    )
    .unwrap();
    assert_eq!(result.bytes, b"\xa3");
    assert_eq!(result.converted, 2);
}

#[test]
fn validates_encoding_for_utf_helpers() {
    let error = UtfToLocal(b"a", None, &[], None, 99, false).unwrap_err();

    assert_eq!(error.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    assert_eq!(error.message(), "invalid encoding number: 99");
}

// stringinfo_mb.c: appendStringInfoStringQuoted.

fn quoted(s: &[u8], maxlen: i32) -> Vec<u8> {
    let ctx = mcx::MemoryContext::new("conv-string-helpers.test");
    let mut buf = StringInfo::new_in(ctx.mcx());
    appendStringInfoStringQuoted(&mut buf, s, maxlen).unwrap();
    buf.as_bytes().to_vec()
}

#[test]
fn quoted_wraps_plain_string() {
    assert_eq!(quoted(b"abc", -1), b"'abc'");
}

#[test]
fn quoted_doubles_embedded_quotes() {
    assert_eq!(quoted(b"a'b''c", -1), b"'a''b''''c'");
}

#[test]
fn quoted_empty_string() {
    assert_eq!(quoted(b"", -1), b"''");
}

#[test]
fn quoted_leading_and_trailing_quote() {
    assert_eq!(quoted(b"'x'", -1), b"'''x'''");
}
