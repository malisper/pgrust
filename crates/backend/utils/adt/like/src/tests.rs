//! Unit + golden tests for the `LIKE`/`ILIKE` port.
//!
//! The matcher and escape logic reach several seams owned by units that are not
//! ported yet (mbutils' encoding accessors and `pg_mblen_range`, pg_locale's
//! `pg_newlocale_from_collation`/`pg_strncoll`, the recursion/interrupt guards).
//! These tests install loud-no-op / C-locale stubs for those seams once, so the
//! in-crate logic can be exercised under the default single-byte `SQL_ASCII`
//! encoding with the `C` collation — exactly the regression-suite default.
//!
//! The golden cases are transcribed 1:1 from PostgreSQL 18.3
//! `src/test/regress/expected/strings.out`.

use std::sync::Once;

use super::*;
use pg_locale_seams as locale_seam;
use mbutils_seams as mb_seam;
use ::mcx::Mcx;
use ::types_core::Oid;
use locale::{CollProvider, PgLocale, PgLocaleStruct};

static INIT: Once = Once::new();

/// The `C` collation flag core: deterministic, C/POSIX ctype, libc default.
fn c_locale_struct() -> PgLocaleStruct {
    PgLocaleStruct {
        provider: CollProvider::Libc,
        deterministic: true,
        collate_is_c: true,
        ctype_is_c: true,
        is_default: true,
    }
}

/// Install the external seams this crate consumes with single-byte C-locale
/// stubs (owners unported), once per test process.
fn install_seams() {
    INIT.call_once(|| {
        stack_depth_seams::check_stack_depth::set(|| Ok(()));
        postgres_seams::check_for_interrupts::set(|| Ok(()));
        postgres_seams::check_stack_depth::set(|| Ok(()));
        // SQL_ASCII: single-byte, encoding id 0.
        mb_seam::pg_database_encoding_max_length::set(|| 1);
        mb_seam::get_database_encoding::set(|| 0);
        // Single-byte: each byte is its own character.
        mb_seam::pg_mblen_range::set(|_s| 1);
        // C collation resolves without catalog access.
        locale_seam::pg_newlocale_from_collation::set(new_c_locale);
        // Deterministic C locale never reaches pg_strncoll; loud-panic if it does.
        locale_seam::pg_strncoll::set(|_collid, _a, _b| {
            panic!("pg_strncoll: deterministic C locale should not reach the nondeterministic path")
        });
        // C locale (ctype_is_c) folds via pg_ascii_tolower, never char_tolower.
        locale_seam::char_tolower::set(|_c, _collid| {
            panic!("char_tolower: C locale should fold via pg_ascii_tolower")
        });
    });
}

fn new_c_locale(mcx: Mcx<'_>, _collid: Oid) -> PgResult<PgLocale<'_>> {
    ::mcx::alloc_in(mcx, c_locale_struct())
}

fn ctx() -> ::mcx::MemoryContext {
    ::mcx::MemoryContext::new("test")
}

const NULL: Locale<'static> = None;

// --- SB_MatchText (deterministic, NULL locale): the LIKE truth table. ---

#[test]
fn sb_literal_match() {
    install_seams();
    let c = ctx();
    let m = c.mcx();
    assert_eq!(SB_MatchText(b"abc", b"abc", NULL, m).unwrap(), LIKE_TRUE);
    assert_eq!(SB_MatchText(b"abc", b"abd", NULL, m).unwrap(), LIKE_FALSE);
}

#[test]
fn sb_percent_fast_path_matches_everything() {
    install_seams();
    let c = ctx();
    let m = c.mcx();
    assert_eq!(SB_MatchText(b"", b"%", NULL, m).unwrap(), LIKE_TRUE);
    assert_eq!(SB_MatchText(b"anything", b"%", NULL, m).unwrap(), LIKE_TRUE);
}

#[test]
fn sb_underscore_matches_single_char() {
    install_seams();
    let c = ctx();
    let m = c.mcx();
    assert_eq!(SB_MatchText(b"a", b"_", NULL, m).unwrap(), LIKE_TRUE);
    assert_eq!(SB_MatchText(b"", b"_", NULL, m).unwrap(), LIKE_ABORT);
    assert_eq!(SB_MatchText(b"ab", b"a_", NULL, m).unwrap(), LIKE_TRUE);
    assert_eq!(SB_MatchText(b"a", b"a_", NULL, m).unwrap(), LIKE_ABORT);
}

#[test]
fn sb_percent_wildcards() {
    install_seams();
    let c = ctx();
    let m = c.mcx();
    assert_eq!(SB_MatchText(b"abc", b"a%c", NULL, m).unwrap(), LIKE_TRUE);
    assert_eq!(SB_MatchText(b"abc", b"a%d", NULL, m).unwrap(), LIKE_ABORT);
    assert_eq!(SB_MatchText(b"abcde", b"%cd%", NULL, m).unwrap(), LIKE_TRUE);
    assert_eq!(SB_MatchText(b"abc", b"abc%", NULL, m).unwrap(), LIKE_TRUE);
    assert_eq!(SB_MatchText(b"abc", b"%_", NULL, m).unwrap(), LIKE_TRUE);
    assert_eq!(SB_MatchText(b"", b"%_", NULL, m).unwrap(), LIKE_ABORT);
}

#[test]
fn sb_end_of_text_trailing_percent() {
    install_seams();
    let c = ctx();
    let m = c.mcx();
    assert_eq!(SB_MatchText(b"abc", b"abc%%%", NULL, m).unwrap(), LIKE_TRUE);
    assert_eq!(SB_MatchText(b"abc", b"abcd", NULL, m).unwrap(), LIKE_ABORT);
    assert_eq!(SB_MatchText(b"abcd", b"abc", NULL, m).unwrap(), LIKE_FALSE);
}

#[test]
fn sb_escape_literal() {
    install_seams();
    let c = ctx();
    let m = c.mcx();
    assert_eq!(SB_MatchText(b"50%", b"50\\%", NULL, m).unwrap(), LIKE_TRUE);
    assert_eq!(SB_MatchText(b"50x", b"50\\%", NULL, m).unwrap(), LIKE_FALSE);
    assert_eq!(SB_MatchText(b"a\\b", b"a\\\\b", NULL, m).unwrap(), LIKE_TRUE);
}

#[test]
fn sb_escape_at_end_errors() {
    install_seams();
    let c = ctx();
    let m = c.mcx();
    let err = SB_MatchText(b"abc", b"ab\\", NULL, m).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_ESCAPE_SEQUENCE);
    assert_eq!(err.message(), "LIKE pattern must not end with escape character");
}

#[test]
fn sb_escape_at_end_after_percent_errors() {
    install_seams();
    let c = ctx();
    let m = c.mcx();
    let err = SB_MatchText(b"abc", b"%\\", NULL, m).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_ESCAPE_SEQUENCE);
}

// --- do_like_escape (single byte). ---

#[test]
fn do_like_escape_no_escape_doubles_backslashes() {
    install_seams();
    let c = ctx();
    let m = c.mcx();
    let out = do_like_escape(b"a\\b", b"", NextCharMode::SingleByte, m).unwrap();
    assert_eq!(&out[..], b"a\\\\b");
    let out = do_like_escape(b"abc", b"", NextCharMode::SingleByte, m).unwrap();
    assert_eq!(&out[..], b"abc");
}

#[test]
fn do_like_escape_backslash_escape_copies_verbatim() {
    install_seams();
    let c = ctx();
    let m = c.mcx();
    let out = do_like_escape(b"a%b_c", b"\\", NextCharMode::SingleByte, m).unwrap();
    assert_eq!(&out[..], b"a%b_c");
}

#[test]
fn do_like_escape_custom_escape_converted_to_backslash() {
    install_seams();
    let c = ctx();
    let m = c.mcx();
    let out = do_like_escape(b"#%abc", b"#", NextCharMode::SingleByte, m).unwrap();
    assert_eq!(&out[..], b"\\%abc");
    let out = do_like_escape(b"a\\b", b"#", NextCharMode::SingleByte, m).unwrap();
    assert_eq!(&out[..], b"a\\\\b");
    let out = do_like_escape(b"#\\", b"#", NextCharMode::SingleByte, m).unwrap();
    assert_eq!(&out[..], b"\\\\");
}

#[test]
fn do_like_escape_multi_char_escape_errors() {
    install_seams();
    let c = ctx();
    let m = c.mcx();
    let err = do_like_escape(b"abc", b"##", NextCharMode::SingleByte, m).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_ESCAPE_SEQUENCE);
    assert_eq!(err.message(), "invalid escape string");
}

#[test]
fn sb_do_like_escape_builds_payload() {
    install_seams();
    let c = ctx();
    let m = c.mcx();
    let result = SB_do_like_escape(b"a#%", b"#", m).unwrap();
    assert_eq!(&result[..], b"a\\%");
}

#[test]
fn invalid_collation_reports_indeterminate() {
    install_seams();
    let c = ctx();
    let m = c.mcx();
    let err = GenericMatchText(b"a", b"a", 0, m).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INDETERMINATE_COLLATION);
    assert_eq!(err.message(), "could not determine which collation to use for LIKE");

    let err = Generic_Text_IC_like(b"a", b"a", 0, m).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INDETERMINATE_COLLATION);
    assert_eq!(err.message(), "could not determine which collation to use for ILIKE");
}

#[test]
fn name_str_trims_at_nul_like_strlen() {
    assert_eq!(name_str(b"abc\0\0\0\0\0"), b"abc");
    assert_eq!(name_str(b"abc"), b"abc");
    assert_eq!(name_str(b"\0\0"), b"");
    assert_eq!(name_str(b""), b"");
}

#[test]
fn sb_lower_char_c_locale_folds_ascii() {
    let loc = c_locale_struct();
    assert_eq!(SB_lower_char(b'A', &loc, C_COLLATION_OID), b'a');
    assert_eq!(SB_lower_char(b'z', &loc, C_COLLATION_OID), b'z');
}

// ===========================================================================
// Golden tests transcribed 1:1 from PostgreSQL 18.3 strings.out (C collation,
// single-byte SQL_ASCII encoding).
// ===========================================================================

mod golden {
    use super::*;

    fn like(m: Mcx<'_>, str: &[u8], pat: &[u8]) -> bool {
        let yes = textlike(str, pat, C_COLLATION_OID, m).unwrap();
        assert_eq!(textnlike(str, pat, C_COLLATION_OID, m).unwrap(), !yes);
        yes
    }

    fn ilike(m: Mcx<'_>, str: &[u8], pat: &[u8]) -> bool {
        let yes = texticlike(str, pat, C_COLLATION_OID, m).unwrap();
        assert_eq!(texticnlike(str, pat, C_COLLATION_OID, m).unwrap(), !yes);
        yes
    }

    fn namelike_g(m: Mcx<'_>, str: &[u8], pat: &[u8]) -> bool {
        let yes = namelike(str, pat, C_COLLATION_OID, m).unwrap();
        assert_eq!(namenlike(str, pat, C_COLLATION_OID, m).unwrap(), !yes);
        yes
    }

    fn bytealike_g(m: Mcx<'_>, str: &[u8], pat: &[u8]) -> bool {
        let yes = bytealike(str, pat, m).unwrap();
        assert_eq!(byteanlike(str, pat, m).unwrap(), !yes);
        yes
    }

    /// `str LIKE (pat ESCAPE esc)`: normalize with `like_escape`, then match.
    fn like_escape_g(m: Mcx<'_>, str: &[u8], pat: &[u8], esc: &[u8]) -> bool {
        let norm = like_escape(pat, esc, m).unwrap();
        textlike(str, &norm, C_COLLATION_OID, m).unwrap()
    }

    fn bytea_like_escape_g(m: Mcx<'_>, str: &[u8], pat: &[u8], esc: &[u8]) -> bool {
        let norm = like_escape_bytea(pat, esc, m).unwrap();
        bytealike(str, &norm, m).unwrap()
    }

    #[test]
    fn strings_out_like_simplest() {
        install_seams();
        let c = ctx();
        let m = c.mcx();
        assert!(like(m, b"hawkeye", b"h%"));
        assert!(!like(m, b"hawkeye", b"H%"));
        assert!(!like(m, b"hawkeye", b"indio%"));
        assert!(like(m, b"hawkeye", b"h%eye"));
        assert!(like(m, b"indio", b"_ndio"));
        assert!(like(m, b"indio", b"in__o"));
        assert!(!like(m, b"indio", b"in_o"));
        assert!(namelike_g(m, b"abc", b"_b_"));
        assert!(bytealike_g(m, b"abc", b"_b_"));
    }

    #[test]
    fn strings_out_like_unused_escape() {
        install_seams();
        let c = ctx();
        let m = c.mcx();
        assert!(like_escape_g(m, b"hawkeye", b"h%", b"#"));
        assert!(like_escape_g(m, b"indio", b"ind_o", b"$"));
    }

    #[test]
    fn strings_out_like_escape_clause() {
        install_seams();
        let c = ctx();
        let m = c.mcx();
        assert!(like_escape_g(m, b"h%", b"h#%", b"#"));
        assert!(!like_escape_g(m, b"h%wkeye", b"h#%", b"#"));
        assert!(like_escape_g(m, b"h%wkeye", b"h#%%", b"#"));
        assert!(like_escape_g(m, b"h%awkeye", b"h#%a%k%e", b"#"));
        assert!(like_escape_g(m, b"indio", b"_ndio", b"$"));
        assert!(like_escape_g(m, b"i_dio", b"i$_d_o", b"$"));
        assert!(!like_escape_g(m, b"i_dio", b"i$_nd_o", b"$"));
        assert!(like_escape_g(m, b"i_dio", b"i$_d%o", b"$"));
        assert!(bytea_like_escape_g(m, b"a_c", b"a$__", b"$"));
    }

    #[test]
    fn strings_out_like_escape_same_as_pattern_char() {
        install_seams();
        let c = ctx();
        let m = c.mcx();
        assert!(like_escape_g(m, b"maca", b"m%aca", b"%"));
        assert!(like_escape_g(m, b"ma%a", b"m%a%%a", b"%"));
        assert!(like_escape_g(m, b"bear", b"b_ear", b"_"));
        assert!(like_escape_g(m, b"be_r", b"b_e__r", b"_"));
        assert!(!like_escape_g(m, b"be_r", b"__e__r", b"_"));
    }

    #[test]
    fn strings_out_ilike_ascii() {
        install_seams();
        let c = ctx();
        let m = c.mcx();
        // C_COLLATION_OID's locale has ctype_is_c == true, so SB_lower_char folds
        // via the in-crate pg_ascii_tolower (single-byte fold-on-the-fly path).
        assert!(ilike(m, b"hawkeye", b"h%"));
        assert!(ilike(m, b"hawkeye", b"H%"));
        assert!(ilike(m, b"hawkeye", b"H%Eye"));
        assert!(ilike(m, b"Hawkeye", b"h%"));
    }

    #[test]
    fn strings_out_ilike_name_ascii() {
        install_seams();
        let c = ctx();
        let m = c.mcx();
        assert!(nameiclike(b"ABC", b"_b_", C_COLLATION_OID, m).unwrap());
        assert!(!nameicnlike(b"ABC", b"_b_", C_COLLATION_OID, m).unwrap());
    }

    #[test]
    fn strings_out_percent_underscore_combos() {
        install_seams();
        let c = ctx();
        let m = c.mcx();
        assert!(like(m, b"foo", b"_%"));
        assert!(like(m, b"f", b"_%"));
        assert!(!like(m, b"", b"_%"));
        assert!(like(m, b"foo", b"%_"));
        assert!(like(m, b"f", b"%_"));
        assert!(!like(m, b"", b"%_"));
        assert!(like(m, b"foo", b"__%"));
        assert!(like(m, b"foo", b"___%"));
        assert!(!like(m, b"foo", b"____%"));
        assert!(like(m, b"foo", b"%__"));
        assert!(like(m, b"foo", b"%___"));
        assert!(!like(m, b"foo", b"%____"));
        assert!(like(m, b"jack", b"%____%"));
    }
}
