//! Kani C-equivalence harnesses: pgrust integer-output family (shipped
//! crate at crates/backend/utils/adt/numutils) vs vendored PostgreSQL C
//! (c/pg_intout.c, compiled via `-Z c-ffi --c-lib`).
//!
//! Functions under proof: pg_ultoa_n (int4out unsigned core),
//! pg_ltoa (int4out), pg_ulltoa_n (int8out unsigned core),
//! pg_lltoa (int8out), pg_itoa (int2out).
//!
//! decimal_length32/64 (lib.rs:479/:487) are PRIVATE in the shipped crate,
//! so they have no standalone harness; their equivalence to C's
//! decimalLength32/64 is subsumed by the length half of the byte-equality
//! assertions below (a wrong length shifts every digit position and the
//! returned len).
//!
//! Output comparison: C pg_ltoa/pg_lltoa/pg_itoa NUL-terminate; the Rust
//! ports deliberately do not (documented at lib.rs:620). Byte-equality is
//! therefore over the returned length and bytes[..len]; the trailing NUL
//! is C-only plumbing, not a parity surface.
//!
//! WATCH-LIST experiment note: these bodies chain constant divisions
//! (/10000, /100, /10, /100000000). Per TRIAGE.md dividers are wall-class
//! risk; whether *small-constant* divider chains are fast is exactly what
//! these harnesses measure. Timeout data is a deliverable.
//!
//! Unwind accounting (all loops in each harness):
//!   - C pg_leftmost_one_pos32 shift loop: <=4 iterations (64-bit: <=8)
//!   - C/Rust pg_ultoa_n `while value >= 10000`: <=2 iterations
//!   - C/Rust pg_ulltoa_n `while value >= 100000000`: <=2 iterations
//!   - harness byte-compare loop: <= output length
//! unwind(13) covers everything 32-bit; 64-bit SYMBOLIC harnesses use
//! unwind(9) (shift loop 8 + len<=7 in the <1e7 band) — unwind(22) there
//! blew the formula up ~10x (21 dead copies of the 64-bit /1e8 loop) and
//! walled a <1e4-band proof that solves in ~10s at unwind(9). 64-bit
//! SPOT harnesses (concrete values, constant-propagated) use unwind(22)
//! to cover 20-digit outputs.

#![allow(dead_code)]

#[cfg(kani)]
mod ffi {
    // int returns throughout (C returns int; no void/Unit shim needed).
    extern "C" {
        pub fn pg_ultoa_n(value: u32, a: *mut u8) -> i32;
        pub fn pg_ltoa(value: i32, a: *mut u8) -> i32;
        pub fn pg_ulltoa_n(value: u64, a: *mut u8) -> i32;
        pub fn pg_lltoa(value: i64, a: *mut u8) -> i32;
        pub fn pg_itoa(i: i16, a: *mut u8) -> i32;
    }
}

#[cfg(kani)]
mod harnesses {
    use crate::ffi;

    /// Compare returned lengths and bytes[..len]. Trailing NUL (C-only)
    /// excluded by construction.
    fn check_eq(clen: i32, cbuf: &[u8], rlen: usize, rbuf: &[u8]) {
        assert_eq!(clen as usize, rlen);
        let mut i = 0;
        while i < rlen {
            assert_eq!(cbuf[i], rbuf[i]);
            i += 1;
        }
    }

    // ---- int2out core: full-domain i16 ----
    // Full-domain single harness PROVED at 14.8s (2026-07-28) — over the
    // 10s standing budget, so it stands as a two-way split + coverage.

    fn itoa_case(x: i16) {
        let mut cbuf = [0u8; 8];
        let mut rbuf = [0u8; 8];
        let clen = unsafe { ffi::pg_itoa(x, cbuf.as_mut_ptr()) };
        let rlen = numutils::pg_itoa(x, &mut rbuf);
        check_eq(clen, &cbuf, rlen, &rbuf);
    }

    #[kani::proof]
    #[kani::unwind(13)]
    fn eq_itoa_i16_small() {
        let x: i16 = kani::any();
        kani::assume(x > -10_000 && x < 10_000);
        itoa_case(x);
    }

    #[kani::proof]
    #[kani::unwind(13)]
    fn eq_itoa_i16_big() {
        let x: i16 = kani::any();
        kani::assume(x <= -10_000 || x >= 10_000);
        itoa_case(x);
    }

    /// MANDATORY union-coverage for the i16 split.
    #[kani::proof]
    fn cover_itoa_i16_split() {
        let x: i16 = kani::any();
        assert!((x > -10_000 && x < 10_000) || (x <= -10_000 || x >= 10_000));
    }

    // ---- int4out cores ----
    // WALL DATA (2026-07-28, kissat, 30s hard timeout):
    //   eq over full u32 domain          -> WALL (>30s)
    //   eq over [1e4, 1e8) (1 loop iter) -> WALL
    //   eq over [1e8, u32::MAX]          -> WALL
    //   per-decimal-length: d5 4.1s, d6 6.6s, d7 13.8s GREEN;
    //   d8 [1e7,1e8) WALL; d8a [1e7,5e7) WALL; d8b [5e7,1e8) 28.2s GREEN
    //   (once — boundary-flaky, not standing).
    // Cost ~doubles per decimal digit of range width; the reliable
    // symbolic ceiling is ~1e7-wide ranges. Symbolic proved domain:
    // |v| < 1e7. Beyond that: concrete spot proofs (see eq_*_spots).

    fn ltoa_case(x: i32) {
        let mut cbuf = [0u8; 13];
        let mut rbuf = [0u8; 13];
        let clen = unsafe { ffi::pg_ltoa(x, cbuf.as_mut_ptr()) };
        let rlen = numutils::pg_ltoa(x, &mut rbuf);
        check_eq(clen, &cbuf, rlen, &rbuf);
    }

    /// pg_ltoa sign/wrap logic over the symbolically-provable magnitude
    /// band (the unsigned digit core is covered by the pg_ultoa_n split).
    #[kani::proof]
    #[kani::unwind(13)]
    fn eq_ltoa_i32_abs_lt1e6() {
        let x: i32 = kani::any();
        kani::assume(x > -1_000_000 && x < 1_000_000);
        ltoa_case(x);
    }

    /// Concrete spot proofs across the wall region (d8-d10, extremes,
    /// sign boundaries). Constants propagate; solver cost is trivial.
    #[kani::proof]
    #[kani::unwind(13)]
    fn eq_ltoa_i32_spots() {
        const SPOTS: [i32; 10] = [
            i32::MIN,
            -2_000_000_000,
            -123_456_789,
            -10_000_000,
            -1,
            0,
            10_000_000,
            123_456_789,
            999_999_999,
            i32::MAX,
        ];
        let mut k = 0;
        while k < SPOTS.len() {
            ltoa_case(SPOTS[k]);
            k += 1;
        }
    }

    #[kani::proof]
    #[kani::unwind(13)]
    fn eq_ultoa_n_u32_spots() {
        const SPOTS: [u32; 8] = [
            10_000_000,
            49_999_999,
            50_000_000,
            99_999_999,
            100_000_000,
            999_999_999,
            1_000_000_000,
            u32::MAX,
        ];
        let mut k = 0;
        while k < SPOTS.len() {
            ultoa_case(SPOTS[k]);
            k += 1;
        }
    }

    // ---- int4out unsigned-core case-split (escalation ladder step 3) ----
    // Standing harnesses cover [0, 1e7) symbolically; the coverage
    // harness below proves exactly that union. d8+ ranges are WALL
    // (probes retained below, not standing).


    fn ultoa_case(x: u32) {
        let mut cbuf = [0u8; 12];
        let mut rbuf = [0u8; 12];
        let clen = unsafe { ffi::pg_ultoa_n(x, cbuf.as_mut_ptr()) };
        let rlen = numutils::pg_ultoa_n(x, &mut rbuf);
        check_eq(clen, &cbuf, rlen, &rbuf);
    }

    #[kani::proof]
    #[kani::unwind(13)]
    fn eq_ultoa_n_u32_r1_lt1e4() {
        let x: u32 = kani::any();
        kani::assume(x < 10_000);
        ultoa_case(x);
    }

    // Coarse ranges r2 = [1e4,1e8) and r3 = [1e8,u32::MAX] WALL at 30s
    // (measured 2026-07-28): one symbolic /10000 loop iteration over a
    // 1e8-wide operand already exceeds the budget. Finer split: one
    // harness per decimal length (each fixes olength and the branch
    // structure; division operands stay narrow). d7 proved whole at
    // 13.8s — over the 10s standing budget — so it stands as halves.

    #[kani::proof]
    #[kani::unwind(13)]
    fn eq_ultoa_n_u32_d5() {
        let x: u32 = kani::any();
        kani::assume(x >= 10_000 && x < 100_000);
        ultoa_case(x);
    }

    #[kani::proof]
    #[kani::unwind(13)]
    fn eq_ultoa_n_u32_d6() {
        let x: u32 = kani::any();
        kani::assume(x >= 100_000 && x < 1_000_000);
        ultoa_case(x);
    }

    #[kani::proof]
    #[kani::unwind(13)]
    fn eq_ultoa_n_u32_d7a() {
        let x: u32 = kani::any();
        kani::assume(x >= 1_000_000 && x < 5_000_000);
        ultoa_case(x);
    }

    #[kani::proof]
    #[kani::unwind(13)]
    fn eq_ultoa_n_u32_d7b() {
        let x: u32 = kani::any();
        kani::assume(x >= 5_000_000 && x < 10_000_000);
        ultoa_case(x);
    }

    /// MANDATORY union-coverage: the standing case-split ranges
    /// (r1_lt1e4 + d5 + d6 + d7a + d7b) cover exactly the CLAIMED
    /// symbolic domain [0, 1e7). Predicates mirror the harness assumes
    /// VERBATIM — keep in sync or the gate silently weakens.
    #[kani::proof]
    fn cover_ultoa_n_u32_split() {
        let x: u32 = kani::any();
        kani::assume(x < 10_000_000); // the claimed symbolic domain
        assert!(
            (x < 10_000)
                || (x >= 10_000 && x < 100_000)
                || (x >= 100_000 && x < 1_000_000)
                || (x >= 1_000_000 && x < 5_000_000)
                || (x >= 5_000_000 && x < 10_000_000)
        );
    }

    // ---- WALL PROBES (not standing; expected to exceed the 30s hard
    // timeout). Measured 2026-07-28, kissat: d8 [1e7,1e8) WALL;
    // d8a [1e7,5e7) WALL; d8b [5e7,1e8) GREEN once at 28.2s
    // (boundary-flaky). Kept for re-measurement when solver stacks
    // improve. NOT in run-all.sh. ----

    #[kani::proof]
    #[kani::unwind(13)]
    fn wall_probe_ultoa_n_u32_d8a() {
        let x: u32 = kani::any();
        kani::assume(x >= 10_000_000 && x < 50_000_000);
        ultoa_case(x);
    }

    #[kani::proof]
    #[kani::unwind(13)]
    fn wall_probe_ultoa_n_u32_d8b() {
        let x: u32 = kani::any();
        kani::assume(x >= 50_000_000 && x < 100_000_000);
        ultoa_case(x);
    }

    // ---- int8out cores ----
    // Full-domain u64/i64 is a fortiori WALL (strict superset of the u32
    // wall region, plus a symbolic /100000000). Standing coverage mirrors
    // the u32 shape: symbolic [0, 1e7) split by decimal length (these go
    // through pg_ulltoa_n's own 64-bit body — separate code from
    // pg_ultoa_n — value2 tail path; the `while value >= 100000000` loop
    // body is symbolically out of reach and covered by concrete spots).

    fn ulltoa_case(x: u64) {
        let mut cbuf = [0u8; 21];
        let mut rbuf = [0u8; 21];
        let clen = unsafe { ffi::pg_ulltoa_n(x, cbuf.as_mut_ptr()) };
        let rlen = numutils::pg_ulltoa_n(x, &mut rbuf);
        check_eq(clen, &cbuf, rlen, &rbuf);
    }

    fn lltoa_case(x: i64) {
        let mut cbuf = [0u8; 22];
        let mut rbuf = [0u8; 22];
        let clen = unsafe { ffi::pg_lltoa(x, cbuf.as_mut_ptr()) };
        let rlen = numutils::pg_lltoa(x, &mut rbuf);
        check_eq(clen, &cbuf, rlen, &rbuf);
    }

    #[kani::proof]
    #[kani::unwind(9)]
    fn eq_ulltoa_n_u64_r1_lt1e4() {
        let x: u64 = kani::any();
        kani::assume(x < 10_000);
        ulltoa_case(x);
    }

    #[kani::proof]
    #[kani::unwind(9)]
    fn eq_ulltoa_n_u64_d5() {
        let x: u64 = kani::any();
        kani::assume(x >= 10_000 && x < 100_000);
        ulltoa_case(x);
    }

    #[kani::proof]
    #[kani::unwind(9)]
    fn eq_ulltoa_n_u64_d6() {
        let x: u64 = kani::any();
        kani::assume(x >= 100_000 && x < 1_000_000);
        ulltoa_case(x);
    }

    #[kani::proof]
    #[kani::unwind(9)]
    fn eq_ulltoa_n_u64_d7a() {
        let x: u64 = kani::any();
        kani::assume(x >= 1_000_000 && x < 5_000_000);
        ulltoa_case(x);
    }

    #[kani::proof]
    #[kani::unwind(9)]
    fn eq_ulltoa_n_u64_d7b() {
        let x: u64 = kani::any();
        kani::assume(x >= 5_000_000 && x < 10_000_000);
        ulltoa_case(x);
    }

    /// MANDATORY union-coverage for the u64 split: claimed symbolic
    /// domain [0, 1e7). Predicates mirror the harness assumes VERBATIM.
    #[kani::proof]
    fn cover_ulltoa_n_u64_split() {
        let x: u64 = kani::any();
        kani::assume(x < 10_000_000); // the claimed symbolic domain
        assert!(
            (x < 10_000)
                || (x >= 10_000 && x < 100_000)
                || (x >= 100_000 && x < 1_000_000)
                || (x >= 1_000_000 && x < 5_000_000)
                || (x >= 5_000_000 && x < 10_000_000)
        );
    }

    /// pg_lltoa sign/wrap logic over the symbolically-provable band.
    /// (Single |x|<1e6 harness proved at 24.6s — too close to the 30s
    /// ceiling to stand; split by sign, coverage below.)
    #[kani::proof]
    #[kani::unwind(9)]
    fn eq_lltoa_i64_neg_1e6() {
        let x: i64 = kani::any();
        kani::assume(x > -1_000_000 && x < 0);
        lltoa_case(x);
    }

    #[kani::proof]
    #[kani::unwind(9)]
    fn eq_lltoa_i64_pos_1e6() {
        let x: i64 = kani::any();
        kani::assume(x >= 0 && x < 1_000_000);
        lltoa_case(x);
    }

    /// MANDATORY union-coverage for the i64 sign split over the claimed
    /// band (-1e6, 1e6).
    #[kani::proof]
    fn cover_lltoa_i64_split() {
        let x: i64 = kani::any();
        kani::assume(x > -1_000_000 && x < 1_000_000);
        assert!((x > -1_000_000 && x < 0) || (x >= 0 && x < 1_000_000));
    }

    /// Concrete spot proofs across the u64 wall region, including full
    /// exercise of the `while value >= 100000000` two-iteration loop and
    /// the 19/20-digit extremes.
    #[kani::proof]
    #[kani::unwind(22)]
    fn eq_ulltoa_n_u64_spots() {
        const SPOTS: [u64; 10] = [
            10_000_000,
            99_999_999,
            100_000_000, // loop entry
            999_999_999,
            9_999_999_999,
            123_456_789_012,
            9_007_199_254_740_993,
            9_999_999_999_999_999_999, // 19 digits, 2 loop iterations
            10_000_000_000_000_000_000, // 20 digits
            u64::MAX,
        ];
        let mut k = 0;
        while k < SPOTS.len() {
            ulltoa_case(SPOTS[k]);
            k += 1;
        }
    }

    #[kani::proof]
    #[kani::unwind(22)]
    fn eq_lltoa_i64_spots() {
        const SPOTS: [i64; 8] = [
            i64::MIN,
            -9_223_372_036_854_775_807,
            -1_000_000_000_000,
            -10_000_000,
            -1,
            10_000_000,
            1_000_000_000_000_000_000,
            i64::MAX,
        ];
        let mut k = 0;
        while k < SPOTS.len() {
            lltoa_case(SPOTS[k]);
            k += 1;
        }
    }

    // ---- negative control: rig non-vacuity ----
    // Deliberately compares C output for x against Rust output for x+1;
    // MUST fail with a decodable counterexample. A passing control means
    // the gate is broken.

    #[kani::proof]
    #[kani::unwind(13)]
    fn control_intout_mismatch() {
        let x: u32 = kani::any();
        kani::assume(x < 100);
        let mut cbuf = [0u8; 12];
        let mut rbuf = [0u8; 12];
        let clen = unsafe { ffi::pg_ultoa_n(x, cbuf.as_mut_ptr()) };
        let rlen = numutils::pg_ultoa_n(x + 1, &mut rbuf);
        check_eq(clen, &cbuf, rlen, &rbuf);
    }
}
