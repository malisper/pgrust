//! Logic tests for the `varchar`/`bpchar` port. The genuinely-external
//! multibyte and locale owners are reached through seams; this test binary
//! installs deterministic single-byte mocks for them exactly once (a seam may
//! be installed only once per process), modeling a single-byte server encoding
//! with a deterministic C collation, which is sufficient to exercise the
//! length/truncation/blank-pad/compare/hash branch logic.

use super::*;
use std::sync::Once;

use backend_utils_adt_pg_locale_seams as loc;
use backend_utils_mb_mbutils_seams as mb;

static INIT: Once = Once::new();

/// Install single-byte-encoding + deterministic-C-collation mocks for the
/// seams this crate calls. Single-byte: char length == byte length, cliplen ==
/// min(limit, len). Deterministic collation: `pg_strxfrm` is identity (byte
/// order == collation order), so the comparison/hash paths fall to the
/// deterministic branch and `pg_strxfrm` is only the fallback when forced.
fn install_seams() {
    INIT.call_once(|| {
        mb::pg_mbstrlen_with_len::set(|s: &[u8], _limit: i32| Ok(s.len() as i32));
        mb::pg_mbcliplen::set(|_s: &[u8], len: i32, limit: i32| len.min(limit));
        // single-byte: chars == bytes, so char-clip == min(len, limit).
        mb::pg_mbcharcliplen::set(|_s: &[u8], len: i32, limit: i32| Ok(len.min(limit)));
        mb::pg_database_encoding_max_length::set(|| 1);
        // deterministic for every collation except the sentinel 999 (non-det).
        loc::collation_is_deterministic::set(|collid: Oid| Ok(collid != 999));
        // model the C collation (byte comparison) for the ordering tests so
        // `varstr_cmp` takes its memcmp fast path; the non-det sentinel 999 is
        // not C.
        loc::collation_is_c::set(|collid: Oid| Ok(collid != 999));
        // identity transform charged to mcx (single-byte C collation).
        loc::pg_strxfrm::set(|mcx, _collid, src: &[u8]| mcx::slice_in(mcx, src));
    });
}

fn ctx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("varchar-test")
}

// --- pure helpers (no seams) ------------------------------------------------

#[test]
fn bctruelen_strips_trailing_blanks() {
    assert_eq!(bcTruelen(b"abc   "), 3);
    assert_eq!(bcTruelen(b"abc"), 3);
    assert_eq!(bcTruelen(b"   "), 0);
    assert_eq!(bcTruelen(b""), 0);
    assert_eq!(bcTruelen(b" a "), 2);
}

#[test]
fn internal_pattern_compare_order_and_ties() {
    // trailing blanks ignored, then length tiebreak
    assert_eq!(internal_bpchar_pattern_compare(b"abc", b"abc  "), 0);
    assert!(internal_bpchar_pattern_compare(b"abc", b"abd") < 0);
    assert!(internal_bpchar_pattern_compare(b"abd", b"abc") > 0);
    // prefix shorter sorts first after the common bytes match
    assert_eq!(internal_bpchar_pattern_compare(b"ab", b"abc"), -1);
    assert_eq!(internal_bpchar_pattern_compare(b"abc", b"ab"), 1);
    assert!(bpchar_pattern_lt(b"ab", b"abc"));
    assert!(bpchar_pattern_le(b"abc", b"abc  "));
    assert!(bpchar_pattern_ge(b"abc", b"abc  "));
    assert!(bpchar_pattern_gt(b"abc", b"ab"));
    assert_eq!(btbpchar_pattern_cmp(b"x", b"x"), 0);
}

#[test]
fn bpcharoctetlen_is_total_minus_header() {
    assert_eq!(bpcharoctetlen(VARHDRSZ as usize + 7), 7);
    assert_eq!(bpcharoctetlen(VARHDRSZ as usize), 0);
}

#[test]
fn typmodin_validates() {
    // historical: typmod == VARHDRSZ + nchars
    assert_eq!(bpchartypmodin_typmods(&[5]).unwrap(), VARHDRSZ + 5);
    assert_eq!(varchartypmodin_typmods(&[10]).unwrap(), VARHDRSZ + 10);
    // wrong count
    let e = bpchartypmodin_typmods(&[1, 2]).unwrap_err();
    assert_eq!(e.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    // < 1
    assert!(bpchartypmodin_typmods(&[0]).is_err());
    // > MaxAttrSize
    assert!(varchartypmodin_typmods(&[MAX_ATTR_SIZE + 1]).is_err());
}

// --- typmodout --------------------------------------------------------------

#[test]
fn typmodout_formats() {
    let root = ctx();
    let out = bpchartypmodout(root.mcx(), VARHDRSZ + 5).unwrap();
    assert_eq!(out.as_slice(), b"(5)\0");
    // typmod <= VARHDRSZ => empty cstring
    let out = varchartypmodout(root.mcx(), -1).unwrap();
    assert_eq!(out.as_slice(), b"\0");
}

// --- bpchar input / coercion (needs mb seams) -------------------------------

#[test]
fn bpchar_input_blank_pads() {
    install_seams();
    let root = ctx();
    // typmod 4+5 = char(5); "ab" -> "ab   "
    let v = bpcharin(root.mcx(), b"ab", 0, VARHDRSZ + 5, None)
        .unwrap()
        .unwrap();
    assert_eq!(v.as_slice(), b"ab   ");
}

#[test]
fn bpchar_input_truncates_trailing_spaces() {
    install_seams();
    let root = ctx();
    // char(3); "ab  " has 4 chars, extra are spaces -> clip to "ab "
    let v = bpcharin(root.mcx(), b"ab  ", 0, VARHDRSZ + 3, None)
        .unwrap()
        .unwrap();
    assert_eq!(v.as_slice(), b"ab ");
}

#[test]
fn bpchar_input_too_long_hard_error() {
    install_seams();
    let root = ctx();
    let e = bpcharin(root.mcx(), b"abcd", 0, VARHDRSZ + 2, None).unwrap_err();
    assert_eq!(e.sqlstate(), ERRCODE_STRING_DATA_RIGHT_TRUNCATION);
}

#[test]
fn bpchar_input_soft_error() {
    install_seams();
    let root = ctx();
    let mut sc = SoftErrorContext::new(true);
    let r = bpcharin(root.mcx(), b"abcd", 0, VARHDRSZ + 2, Some(&mut sc)).unwrap();
    assert!(r.is_none());
    assert!(sc.error_occurred());
}

#[test]
fn bpchar_input_no_typmod_uses_actual_length() {
    install_seams();
    let root = ctx();
    let v = bpcharin(root.mcx(), b"hello", 0, -1, None).unwrap().unwrap();
    assert_eq!(v.as_slice(), b"hello");
}

#[test]
fn bpchar_coerce_paths() {
    install_seams();
    let root = ctx();
    // matches typmod already -> Source
    matches!(
        bpchar(root.mcx(), b"abcde", VARHDRSZ + 5, false).unwrap(),
        CoerceResult::Source
    );
    // shorter -> blank pad (New)
    match bpchar(root.mcx(), b"ab", VARHDRSZ + 4, false).unwrap() {
        CoerceResult::New(v) => assert_eq!(v.as_slice(), b"ab  "),
        _ => panic!("expected New"),
    }
    // too long, implicit, non-space -> error
    assert!(bpchar(root.mcx(), b"abcd", VARHDRSZ + 2, false).is_err());
    // too long, explicit -> silent truncate
    match bpchar(root.mcx(), b"abcd", VARHDRSZ + 2, true).unwrap() {
        CoerceResult::New(v) => assert_eq!(v.as_slice(), b"ab"),
        _ => panic!("expected New"),
    }
    // invalid typmod -> Source
    matches!(
        bpchar(root.mcx(), b"x", VARHDRSZ - 1, false).unwrap(),
        CoerceResult::Source
    );
}

// --- varchar input / coercion -----------------------------------------------

#[test]
fn varchar_input_no_pad() {
    install_seams();
    let root = ctx();
    let v = varcharin(root.mcx(), b"ab", 0, VARHDRSZ + 5, None)
        .unwrap()
        .unwrap();
    // varchar does NOT blank-pad
    assert_eq!(v.as_slice(), b"ab");
}

#[test]
fn varchar_input_truncates_spaces() {
    install_seams();
    let root = ctx();
    let v = varcharin(root.mcx(), b"ab  ", 0, VARHDRSZ + 2, None)
        .unwrap()
        .unwrap();
    assert_eq!(v.as_slice(), b"ab");
    // non-space overflow -> error
    assert!(varcharin(root.mcx(), b"abcd", 0, VARHDRSZ + 2, None).is_err());
}

#[test]
fn varchar_coerce_paths() {
    install_seams();
    let root = ctx();
    matches!(
        varchar(root.mcx(), b"ab", VARHDRSZ + 5, false).unwrap(),
        CoerceResult::Source
    );
    match varchar(root.mcx(), b"ab  ", VARHDRSZ + 2, false).unwrap() {
        CoerceResult::New(v) => assert_eq!(v.as_slice(), b"ab"),
        _ => panic!("expected New"),
    }
    assert!(varchar(root.mcx(), b"abcd", VARHDRSZ + 2, false).is_err());
    match varchar(root.mcx(), b"abcd", VARHDRSZ + 2, true).unwrap() {
        CoerceResult::New(v) => assert_eq!(v.as_slice(), b"ab"),
        _ => panic!("expected New"),
    };
}

// --- conversions ------------------------------------------------------------

#[test]
fn char_bpchar_one_byte() {
    let root = ctx();
    let v = char_bpchar(root.mcx(), b'Z' as i8).unwrap();
    assert_eq!(v.as_slice(), b"Z");
}

#[test]
fn bpchar_name_strips_blanks_and_zero_pads() {
    install_seams();
    let root = ctx();
    let v = bpchar_name(root.mcx(), b"ab  ").unwrap();
    assert_eq!(v.len(), NAMEDATALEN as usize);
    assert_eq!(&v[..2], b"ab");
    assert!(v[2..].iter().all(|&b| b == 0));
}

#[test]
fn name_bpchar_stops_at_nul() {
    let root = ctx();
    let mut name = vec![0u8; NAMEDATALEN as usize];
    name[..3].copy_from_slice(b"foo");
    let v = name_bpchar(root.mcx(), &name).unwrap();
    assert_eq!(v.as_slice(), b"foo");
}

#[test]
fn out_appends_nul() {
    let root = ctx();
    assert_eq!(bpcharout(root.mcx(), b"hi").unwrap().as_slice(), b"hi\0");
    assert_eq!(varcharout(root.mcx(), b"hi").unwrap().as_slice(), b"hi\0");
}

// --- comparison / hash ------------------------------------------------------

#[test]
fn bpchareq_ignores_trailing_blanks() {
    install_seams();
    assert!(bpchareq(b"abc  ", b"abc", 100).unwrap());
    assert!(!bpchareq(b"abc", b"abd", 100).unwrap());
    assert!(bpcharne(b"abc", b"abd", 100).unwrap());
    assert!(!bpcharne(b"abc ", b"abc", 100).unwrap());
}

#[test]
fn bpchareq_requires_collation() {
    let e = bpchareq(b"a", b"a", InvalidOid).unwrap_err();
    assert_eq!(e.sqlstate(), ERRCODE_INDETERMINATE_COLLATION);
}

#[test]
fn bpchar_ordering() {
    install_seams();
    assert!(bpcharlt(b"abc", b"abd", 100).unwrap());
    assert!(bpcharle(b"abc ", b"abc", 100).unwrap());
    assert!(bpchargt(b"abd", b"abc", 100).unwrap());
    assert!(bpcharge(b"abc", b"abc  ", 100).unwrap());
    assert_eq!(bpcharcmp(b"abc", b"abc ", 100).unwrap(), 0);
    assert!(bpchar_larger(b"abd", b"abc", 100).unwrap());
    assert!(bpchar_smaller(b"abc", b"abd", 100).unwrap());
}

#[test]
fn hashbpchar_deterministic_ignores_blanks() {
    install_seams();
    let root = ctx();
    let h1 = hashbpchar(root.mcx(), b"abc  ", 100).unwrap();
    let h2 = hashbpchar(root.mcx(), b"abc", 100).unwrap();
    assert_eq!(h1, h2);
    let e1 = hashbpcharextended(root.mcx(), b"abc ", 100, 42).unwrap();
    let e2 = hashbpcharextended(root.mcx(), b"abc", 100, 42).unwrap();
    assert_eq!(e1, e2);
}

#[test]
fn hashbpchar_zero_collation_errors() {
    let root = ctx();
    assert_eq!(
        hashbpchar(root.mcx(), b"a", 0).unwrap_err().sqlstate(),
        ERRCODE_INDETERMINATE_COLLATION
    );
}

#[test]
fn hashbpchar_nondeterministic_path() {
    install_seams();
    let root = ctx();
    // collid 999 forces the non-deterministic (pg_strxfrm) branch; identity
    // transform + trailing NUL means trailing-blank-stripped equal keys hash equal.
    let h1 = hashbpchar(root.mcx(), b"abc ", 999).unwrap();
    let h2 = hashbpchar(root.mcx(), b"abc", 999).unwrap();
    assert_eq!(h1, h2);
}
