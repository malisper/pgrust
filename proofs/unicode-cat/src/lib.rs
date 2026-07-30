//! Kani C≡Rust equivalence proofs: PostgreSQL unicode_category.c
//! (unicode_category + pg_u_prop_* + pg_u_is* predicates) vs the shipped
//! pgrust `unicode_category` crate.
//!
//! Domain: full symbolic u32 codepoint. The C contract is code <= 0x10ffff
//! (Assert, compiled out in production/NDEBUG — mirrored by a no-op shim),
//! so the full-u32 harnesses prove both the in-contract domain and the
//! out-of-contract behavior (binary search misses -> UNASSIGNED/false on
//! both sides).
//!
//! Table parity: the C side compiles the byte-identical copy of the
//! generated unicode_category_table.h that the Rust build.rs transcribes,
//! so any transcription defect shows up as a counterexample.
//!
//! The private Rust `range_search` is covered by inclusion: every
//! pg_u_prop_* harness exercises it over the full u32 domain against a
//! distinct table.

#[cfg(kani)]
mod proofs {
    use std::os::raw::c_int;

    extern "C" {
        fn c_unicode_category(code: u32) -> c_int;
        fn c_pg_u_prop_alphabetic(code: u32) -> c_int;
        fn c_pg_u_prop_lowercase(code: u32) -> c_int;
        fn c_pg_u_prop_uppercase(code: u32) -> c_int;
        fn c_pg_u_prop_cased(code: u32) -> c_int;
        fn c_pg_u_prop_case_ignorable(code: u32) -> c_int;
        fn c_pg_u_prop_white_space(code: u32) -> c_int;
        fn c_pg_u_prop_hex_digit(code: u32) -> c_int;
        fn c_pg_u_prop_join_control(code: u32) -> c_int;
        fn c_pg_u_isdigit(code: u32, posix: c_int) -> c_int;
        fn c_pg_u_isalpha(code: u32) -> c_int;
        fn c_pg_u_isalnum(code: u32, posix: c_int) -> c_int;
        fn c_pg_u_isupper(code: u32) -> c_int;
        fn c_pg_u_islower(code: u32) -> c_int;
        fn c_pg_u_isblank(code: u32) -> c_int;
        fn c_pg_u_isgraph(code: u32) -> c_int;
        fn c_pg_u_isprint(code: u32) -> c_int;
        fn c_pg_u_ispunct(code: u32, posix: c_int) -> c_int;
        fn c_pg_u_isspace(code: u32) -> c_int;
    }

    // unwind: binary search over the largest table (unicode_categories,
    // 3368 entries) takes at most ceil(log2(3369)) = 12 iterations.
    macro_rules! eq_bool {
        ($name:ident, $cfn:ident, $rfn:path) => {
            #[kani::proof]
            #[kani::unwind(15)]
            fn $name() {
                let code: u32 = kani::any();
                let c = unsafe { $cfn(code) } != 0;
                let r = $rfn(code);
                assert!(c == r, "divergence: C != Rust");
            }
        };
    }

    // Predicates taking the posix-variant flag are split into one harness
    // per concrete flag value (the two-point flag domain is trivially
    // covered): the posix arms avoid the 3368-entry category table and stay
    // fast; the standard arms carry the category-search cost.
    macro_rules! eq_bool_posix {
        ($name_posix:ident, $name_std:ident, $cfn:ident, $rfn:path) => {
            #[kani::proof]
            #[kani::unwind(15)]
            fn $name_posix() {
                let code: u32 = kani::any();
                let c = unsafe { $cfn(code, 1) } != 0;
                let r = $rfn(code, true);
                assert!(c == r, "divergence: C != Rust (posix)");
            }
            #[kani::proof]
            #[kani::unwind(15)]
            fn $name_std() {
                let code: u32 = kani::any();
                let c = unsafe { $cfn(code, 0) } != 0;
                let r = $rfn(code, false);
                assert!(c == r, "divergence: C != Rust (standard)");
            }
        };
    }

    #[kani::proof]
    #[kani::unwind(15)]
    fn eq_unicode_category() {
        let code: u32 = kani::any();
        let c = unsafe { c_unicode_category(code) };
        let r = unicode_category::unicode_category(code) as c_int;
        assert!(c == r, "divergence: C != Rust unicode_category");
    }

    eq_bool!(eq_prop_alphabetic, c_pg_u_prop_alphabetic, unicode_category::pg_u_prop_alphabetic);
    eq_bool!(eq_prop_lowercase, c_pg_u_prop_lowercase, unicode_category::pg_u_prop_lowercase);
    eq_bool!(eq_prop_uppercase, c_pg_u_prop_uppercase, unicode_category::pg_u_prop_uppercase);
    eq_bool!(eq_prop_cased, c_pg_u_prop_cased, unicode_category::pg_u_prop_cased);
    eq_bool!(
        eq_prop_case_ignorable,
        c_pg_u_prop_case_ignorable,
        unicode_category::pg_u_prop_case_ignorable
    );
    eq_bool!(
        eq_prop_white_space,
        c_pg_u_prop_white_space,
        unicode_category::pg_u_prop_white_space
    );
    eq_bool!(eq_prop_hex_digit, c_pg_u_prop_hex_digit, unicode_category::pg_u_prop_hex_digit);
    eq_bool!(
        eq_prop_join_control,
        c_pg_u_prop_join_control,
        unicode_category::pg_u_prop_join_control
    );
    eq_bool_posix!(eq_isdigit_posix, eq_isdigit_std, c_pg_u_isdigit, unicode_category::pg_u_isdigit);
    eq_bool!(eq_isalpha, c_pg_u_isalpha, unicode_category::pg_u_isalpha);
    eq_bool_posix!(eq_isalnum_posix, eq_isalnum_std, c_pg_u_isalnum, unicode_category::pg_u_isalnum);
    eq_bool!(eq_isupper, c_pg_u_isupper, unicode_category::pg_u_isupper);
    eq_bool!(eq_islower, c_pg_u_islower, unicode_category::pg_u_islower);
    eq_bool!(eq_isblank, c_pg_u_isblank, unicode_category::pg_u_isblank);
    eq_bool!(eq_isgraph, c_pg_u_isgraph, unicode_category::pg_u_isgraph);
    eq_bool!(eq_isprint, c_pg_u_isprint, unicode_category::pg_u_isprint);
    eq_bool_posix!(eq_ispunct_posix, eq_ispunct_std, c_pg_u_ispunct, unicode_category::pg_u_ispunct);
    eq_bool!(eq_isspace, c_pg_u_isspace, unicode_category::pg_u_isspace);

    /// NEGATIVE CONTROL — must FAIL (proves the rig is non-vacuous):
    /// Rust lowercase deliberately compared against C uppercase.
    #[kani::proof]
    #[kani::unwind(15)]
    fn control_lower_vs_upper_must_fail() {
        let code: u32 = kani::any();
        let c = unsafe { c_pg_u_prop_uppercase(code) } != 0;
        let r = unicode_category::pg_u_prop_lowercase(code);
        assert!(c == r, "expected failure: lowercase vs uppercase");
    }
}
