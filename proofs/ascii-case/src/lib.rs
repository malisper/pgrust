//! Kani C≡Rust equivalence: pg_ascii_tolower / pg_ascii_toupper.
//! Rust side: shipped crates/port/pgstrcasecmp (path dep). C side: vendored
//! REL_17_STABLE pgstrcasecmp.c via csrc/case_shim.c. Full 2^8 domain.

#[cfg(kani)]
mod proofs {
    extern "C" {
        fn pgc_ascii_tolower_i(ch: i32) -> i32;
        fn pgc_ascii_toupper_i(ch: i32) -> i32;
    }

    #[kani::proof]
    fn ascii_tolower_equiv() {
        let ch: u8 = kani::any();
        let rust = pgstrcasecmp::pg_ascii_tolower(ch);
        let c = unsafe { pgc_ascii_tolower_i(ch as i32) };
        assert_eq!(rust as i32, c);
    }

    #[kani::proof]
    fn ascii_toupper_equiv() {
        let ch: u8 = kani::any();
        let rust = pgstrcasecmp::pg_ascii_toupper(ch);
        let c = unsafe { pgc_ascii_toupper_i(ch as i32) };
        assert_eq!(rust as i32, c);
    }
}
