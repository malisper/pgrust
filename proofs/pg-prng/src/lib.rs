//! Kani C-equivalence harnesses: pgrust `pg_prng` (shipped crate at
//! crates/common/pg_prng) vs vendored PostgreSQL 18.3 C (c_pg_prng.c,
//! compiled via `-Z c-ffi --c-lib`). 100%-coverage campaign, lane p1-laneh.
//!
//! Every harness draws a FULLY SYMBOLIC (s0, s1) state (arms as noted),
//! executes the shipped Rust and the verbatim C, and asserts value AND
//! post-state equality. State crosses the FFI as (s0, s1) scalars —
//! goto-cc rejects cross-language struct-pointer declarations (c_
//! wrappers in c_pg_prng.c are plumbing only).
//!
//! The libm-dependent arms (fseed, next_f64, normal_f64) are NOT proved
//! here — CBMC has no ldexp/log/sin model; their verification of record
//! is the pg_prng_diff differential campaign (bit-exact against the same
//! libm). Routes rows record that split.
//!
//! u64_range / i64_range carry an unbounded rejection loop; the harnesses
//! prove (a) the empty-range arms full-domain and (b) power-of-two-span
//! cells where the C loop provably accepts its first draw (range =
//! 2^k - 1 => val <= range for every draw), pinned with literal spans per
//! the literal-pin law. The general span is fuzz-carried.

#![allow(dead_code)]

#[cfg(kani)]
mod ffi {
    extern "C" {
        pub fn c_prng_seed(seed: u64, o0: *mut u64, o1: *mut u64) -> i32;
        pub fn c_prng_seed_check(s0: u64, s1: u64, o0: *mut u64, o1: *mut u64) -> i32;
        pub fn c_prng_u64(s0: u64, s1: u64, o0: *mut u64, o1: *mut u64) -> u64;
        pub fn c_prng_u64_range(
            s0: u64,
            s1: u64,
            rmin: u64,
            rmax: u64,
            o0: *mut u64,
            o1: *mut u64,
        ) -> u64;
        pub fn c_prng_i64(s0: u64, s1: u64, o0: *mut u64, o1: *mut u64) -> i64;
        pub fn c_prng_i64p(s0: u64, s1: u64, o0: *mut u64, o1: *mut u64) -> i64;
        pub fn c_prng_i64_range(
            s0: u64,
            s1: u64,
            rmin: i64,
            rmax: i64,
            o0: *mut u64,
            o1: *mut u64,
        ) -> i64;
        pub fn c_prng_u32(s0: u64, s1: u64, o0: *mut u64, o1: *mut u64) -> u32;
        pub fn c_prng_i32(s0: u64, s1: u64, o0: *mut u64, o1: *mut u64) -> i32;
        pub fn c_prng_i32p(s0: u64, s1: u64, o0: *mut u64, o1: *mut u64) -> i32;
        pub fn c_prng_bool(s0: u64, s1: u64, o0: *mut u64, o1: *mut u64) -> i32;
    }
}

#[cfg(kani)]
mod harnesses {
    use super::ffi;
    use pg_prng::PgPrng;

    fn sym_state() -> (u64, u64) {
        (kani::any(), kani::any())
    }

    fn assert_state(rust: PgPrng, c0: u64, c1: u64) {
        let (r0, r1) = rust.raw();
        assert_eq!(r0, c0);
        assert_eq!(r1, c1);
    }

    /// value+state dual-exec for every fixed-draw entry point.
    macro_rules! draw_harness {
        ($name:ident, $rust:ident, $cfn:ident, $cast:ty) => {
            #[kani::proof]
            fn $name() {
                let (s0, s1) = sym_state();
                let mut st = PgPrng::from_raw(s0, s1);
                let v = st.$rust();
                let (mut c0, mut c1) = (0u64, 0u64);
                let cv = unsafe { ffi::$cfn(s0, s1, &mut c0, &mut c1) };
                assert_eq!(v as $cast, cv as $cast);
                assert_state(st, c0, c1);
            }
        };
    }

    draw_harness!(eq_next_u64, next_u64, c_prng_u64, u64);
    draw_harness!(eq_next_i64, next_i64, c_prng_i64, i64);
    draw_harness!(eq_next_nonnegative_i64, next_nonnegative_i64, c_prng_i64p, i64);
    draw_harness!(eq_next_u32, next_u32, c_prng_u32, u32);
    draw_harness!(eq_next_i32, next_i32, c_prng_i32, i32);
    draw_harness!(eq_next_nonnegative_i32, next_nonnegative_i32, c_prng_i32p, i32);
    draw_harness!(eq_next_bool, next_bool, c_prng_bool, i32);

    #[kani::proof]
    fn eq_seed() {
        let seed: u64 = kani::any();
        let st = PgPrng::seeded(seed);
        let (mut c0, mut c1) = (0u64, 0u64);
        let _ = unsafe { ffi::c_prng_seed(seed, &mut c0, &mut c1) };
        assert_state(st, c0, c1);
    }

    #[kani::proof]
    fn eq_seed_check() {
        let (s0, s1) = sym_state();
        let mut st = PgPrng::from_raw(s0, s1);
        let r = st.ensure_seeded();
        let (mut c0, mut c1) = (0u64, 0u64);
        let cr = unsafe { ffi::c_prng_seed_check(s0, s1, &mut c0, &mut c1) };
        assert_eq!(r as i32, cr);
        assert_state(st, c0, c1);
    }

    #[kani::proof]
    fn eq_nonnegative_signs() {
        let (s0, s1) = sym_state();
        let mut st = PgPrng::from_raw(s0, s1);
        assert!(st.next_nonnegative_i64() >= 0);
        let mut st2 = PgPrng::from_raw(s0, s1);
        assert!(st2.next_nonnegative_i32() >= 0);
    }

    // ---- u64_range / i64_range: empty-range arms (loop never entered;
    // unwind(2) bounds the never-taken rejection loop for the checker) ----

    #[kani::proof]
    #[kani::unwind(2)]
    fn eq_u64_range_empty() {
        let (s0, s1) = sym_state();
        let mut st = PgPrng::from_raw(s0, s1);
        let rmin: u64 = kani::any();
        let rmax: u64 = kani::any();
        kani::assume(rmax <= rmin); // C: else-arm, val = 0, no loop
        let v = st.u64_range(rmin, rmax);
        let (mut c0, mut c1) = (0u64, 0u64);
        let cv = unsafe { ffi::c_prng_u64_range(s0, s1, rmin, rmax, &mut c0, &mut c1) };
        assert_eq!(v, cv);
        assert_eq!(v, rmin);
        assert_state(st, c0, c1);
    }

    #[kani::proof]
    #[kani::unwind(2)]
    fn eq_i64_range_empty() {
        let (s0, s1) = sym_state();
        let mut st = PgPrng::from_raw(s0, s1);
        let rmin: i64 = kani::any();
        let rmax: i64 = kani::any();
        kani::assume(rmax <= rmin);
        let v = st.i64_range(rmin, rmax);
        let (mut c0, mut c1) = (0u64, 0u64);
        let cv = unsafe { ffi::c_prng_i64_range(s0, s1, rmin, rmax, &mut c0, &mut c1) };
        assert_eq!(v, cv);
        assert_eq!(v, rmin);
        assert_state(st, c0, c1);
    }

    // ---- power-of-two-span cells: first draw always accepted, so the C
    // do-while provably runs exactly once (range = 2^k - 1 => the rshifted
    // draw is <= range). Literal spans per the literal-pin law; symbolic
    // state and symbolic rmin. ----

    macro_rules! pow2_range_cell {
        ($name:ident, $span_minus_1:literal) => {
            #[kani::proof]
            #[kani::unwind(3)]
            fn $name() {
                let (s0, s1) = sym_state();
                let mut st = PgPrng::from_raw(s0, s1);
                let rmin: u64 = kani::any();
                kani::assume(rmin <= u64::MAX - $span_minus_1);
                let rmax = rmin + $span_minus_1;
                let v = st.u64_range(rmin, rmax);
                let (mut c0, mut c1) = (0u64, 0u64);
                let cv =
                    unsafe { ffi::c_prng_u64_range(s0, s1, rmin, rmax, &mut c0, &mut c1) };
                assert_eq!(v, cv);
                assert!(v >= rmin && v <= rmax);
                assert_state(st, c0, c1);
            }
        };
    }

    pow2_range_cell!(eq_u64_range_span1, 1u64);
    pow2_range_cell!(eq_u64_range_span256, 255u64);
    pow2_range_cell!(eq_u64_range_span2p32, 4294967295u64);
    pow2_range_cell!(eq_u64_range_full, 18446744073709551615u64);

    #[kani::proof]
    #[kani::unwind(3)]
    fn eq_i64_range_pow2_cell() {
        // i64 arm over a 2^8 span: exercises the uval>PG_INT64_MAX fold-back.
        let (s0, s1) = sym_state();
        let mut st = PgPrng::from_raw(s0, s1);
        let rmin: i64 = kani::any();
        kani::assume(rmin <= i64::MAX - 255);
        let rmax = rmin + 255;
        let v = st.i64_range(rmin, rmax);
        let (mut c0, mut c1) = (0u64, 0u64);
        let cv = unsafe { ffi::c_prng_i64_range(s0, s1, rmin, rmax, &mut c0, &mut c1) };
        assert_eq!(v, cv);
        assert!(v >= rmin && v <= rmax);
        assert_state(st, c0, c1);
    }

    /// Must-fail negative control (family non-vacuity): a deliberately
    /// wrong value claim on the INTENDED assert.
    #[kani::proof]
    fn control_next_u64_wrong_value() {
        let (s0, s1) = sym_state();
        let mut st = PgPrng::from_raw(s0, s1);
        let v = st.next_u64();
        let (mut c0, mut c1) = (0u64, 0u64);
        let cv = unsafe { ffi::c_prng_u64(s0, s1, &mut c0, &mut c1) };
        assert!(v == cv.wrapping_add(1), "INTENDED-FAIL: off-by-one claim");
    }
}
