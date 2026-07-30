//! Kani C≡Rust equivalence: the macaddr8 family (mac8.c).
//! Rust side: shipped crates/backend/utils/adt/mac8. C side: vendored
//! csrc/mac8_shim.c (postgres master mac8.c, fetched 2026-07-28).
//!
//! Coverage:
//!  - macaddr8_out: fully symbolic 8 bytes -> 23-char text, byte equality.
//!  - macaddr8_cmp + lt/le/eq/ge/gt/ne: fully symbolic 8-byte pairs.
//!    C's cmp returns exact {-1,0,1} (explicit constants, not memcmp), so
//!    the harness asserts exact value equality — no sign-equivalence
//!    canonicalizer is needed (contrast proofs/uuid, where C memcmp only
//!    fixes the sign).
//!  - macaddr8_in: fully symbolic NUL-terminated input, symbolic length
//!    <= 25 (covers every accepted form: bare hex, and ':'/'-'/'.'
//!    spacers after any pair — longest valid input without surrounding
//!    whitespace is 23 chars — plus slack for leading/trailing space
//!    paths). Verdict parity + parsed-bytes parity on accept. The length
//!    domain is range-partitioned into 4 band harnesses (a single len<=25
//!    harness walls the solver) + a mandatory union-coverage harness;
//!    concrete per-format witness harnesses prove each canonical notation
//!    is actually accepted by both sides (non-vacuous accept domain).
//!  - macaddr8_set7bit / macaddrtomacaddr8 (oids 4125/4123): fully
//!    symbolic 8-/6-byte inputs, byte equality (REL_18_STABLE C).
//!  - macaddr8tomacaddr (oid 4124): fully symbolic 8 bytes; verdict +
//!    value + sqlstate parity via the cash-pattern PgError::error stub
//!    (`-Z stubbing`); message/hint text out of scope.
//!  - negative control: cmp argument swap, must FAIL (rig non-vacuity).
//!
//! Solver notes (measured 2026-07-28): everything except the symbolic-in
//! bands is sub-second on kissat. The in bands need the DEFAULT solver —
//! external kissat multi-passes and walls >30s on them — and cost 9.7s /
//! 15.9s / 20.6s / 26.0s. That exceeds the 10s standing target for the
//! upper bands but fits the 30s hard cap; the escalation ladder is
//! exhausted: cost is unwind-DEPTH-bound, not width-bound (a width-1
//! len=25-only probe still took 25.3s), so no further case-split helps.

#[cfg(kani)]
mod proofs {
    use adt_mac::MacAddr;
    use adt_mac8::{
        macaddr8_cmp_internal, macaddr8_eq, macaddr8_ge, macaddr8_gt, macaddr8_in_internal,
        macaddr8_le, macaddr8_lt, macaddr8_ne, macaddr8_out_into, macaddr8_set7bit,
        macaddr8_and, macaddr8_not, macaddr8_or, macaddr8_trunc, macaddr8tomacaddr,
        macaddrtomacaddr8, MacAddr8, MACADDR8_OUT_LEN,
    };
    use types_error::{PgError, ERRCODE_INTERNAL_ERROR, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERROR};

    extern "C" {
        fn pgc_macaddr8_in(str_: *const u8, out: *mut u8) -> i32;
        fn pgc_macaddr8_out(b: *const u8, result: *mut u8) -> i32;
        fn pgc_macaddr8_cmp(b1: *const u8, b2: *const u8) -> i32;
        fn pgc_macaddr8_lt(b1: *const u8, b2: *const u8) -> i32;
        fn pgc_macaddr8_le(b1: *const u8, b2: *const u8) -> i32;
        fn pgc_macaddr8_eq(b1: *const u8, b2: *const u8) -> i32;
        fn pgc_macaddr8_ge(b1: *const u8, b2: *const u8) -> i32;
        fn pgc_macaddr8_gt(b1: *const u8, b2: *const u8) -> i32;
        fn pgc_macaddr8_ne(b1: *const u8, b2: *const u8) -> i32;
        fn pgc_macaddr8_set7bit(bin: *const u8, bout: *mut u8) -> i32;
        fn pgc_macaddrtomacaddr8(b6: *const u8, bout: *mut u8) -> i32;
        fn pgc_macaddr8tomacaddr(b8: *const u8, bout: *mut u8) -> i32;
        fn pgc_macaddr8_not(bin: *const u8, bout: *mut u8) -> i32;
        fn pgc_macaddr8_and(b1: *const u8, b2: *const u8, bout: *mut u8) -> i32;
        fn pgc_macaddr8_or(b1: *const u8, b2: *const u8, bout: *mut u8) -> i32;
        fn pgc_macaddr8_trunc(bin: *const u8, bout: *mut u8) -> i32;
    }

    /// Stub for `PgError::error` (same as proofs/cash): field-identical to
    /// the shipped `new_impl(ERROR, ..)` result except `message` (text left
    /// out of the proof) and `location` (`Location::caller()` is
    /// Kani-unsupported; shipped code fills `Some(..)`, the stub leaves
    /// `None` — the field is not asserted on). `sqlstate` starts at the same
    /// `default_sqlstate_for_level(ERROR)` value so the shipped
    /// `.with_sqlstate(..)` in `out_of_range_err` stays load-bearing.
    fn stub_pg_error_error(_message: impl Into<String>) -> PgError {
        PgError {
            level: ERROR,
            sqlstate: ERRCODE_INTERNAL_ERROR,
            message: String::new(),
            message_raw: None,
            detail: None,
            detail_log: None,
            hint: None,
            context: None,
            backtrace: None,
            message_id: None,
            domain: None,
            context_domain: None,
            hide_statement: false,
            hide_context: false,
            location: None,
            saved_errno: None,
            cursor_position: None,
            internal_position: None,
            internal_query: None,
            schema_name: None,
            table_name: None,
            column_name: None,
            datatype_name: None,
            constraint_name: None,
            plpgsql_context_attached: false,
        }
    }

    // ---------- macaddr8_out ----------

    #[kani::proof]
    fn eq_macaddr8_out() {
        let b: [u8; 8] = kani::any();

        let mut rbuf = [0u8; MACADDR8_OUT_LEN];
        let rlen = macaddr8_out_into(&MacAddr8::from_bytes(b), &mut rbuf);

        let mut cbuf = [0u8; 24];
        let clen = unsafe { pgc_macaddr8_out(b.as_ptr(), cbuf.as_mut_ptr()) };

        assert_eq!(rlen as i32, clen);
        for i in 0..24 {
            assert_eq!(rbuf[i], cbuf[i]);
        }
    }

    // ---------- macaddr8_cmp + boolean operators ----------

    #[kani::proof]
    fn eq_macaddr8_cmp() {
        let b1: [u8; 8] = kani::any();
        let b2: [u8; 8] = kani::any();

        let rust = macaddr8_cmp_internal(&MacAddr8::from_bytes(b1), &MacAddr8::from_bytes(b2));
        let c = unsafe { pgc_macaddr8_cmp(b1.as_ptr(), b2.as_ptr()) };
        assert_eq!(rust, c);
    }

    macro_rules! op_harness {
        ($name:ident, $rust_op:ident, $c_op:ident) => {
            #[kani::proof]
            fn $name() {
                let b1: [u8; 8] = kani::any();
                let b2: [u8; 8] = kani::any();

                let rust = $rust_op(&MacAddr8::from_bytes(b1), &MacAddr8::from_bytes(b2));
                let c = unsafe { $c_op(b1.as_ptr(), b2.as_ptr()) };
                assert_eq!(rust as i32, c);
            }
        };
    }

    op_harness!(eq_macaddr8_lt, macaddr8_lt, pgc_macaddr8_lt);
    op_harness!(eq_macaddr8_le, macaddr8_le, pgc_macaddr8_le);
    op_harness!(eq_macaddr8_eq, macaddr8_eq, pgc_macaddr8_eq);
    op_harness!(eq_macaddr8_ge, macaddr8_ge, pgc_macaddr8_ge);
    op_harness!(eq_macaddr8_gt, macaddr8_gt, pgc_macaddr8_gt);
    op_harness!(eq_macaddr8_ne, macaddr8_ne, pgc_macaddr8_ne);

    // ---------- macaddr8_in ----------

    /// Shared body: run both parsers on the same NUL-terminated byte string
    /// and assert verdict + parsed-bytes parity.
    fn check_in_parity(buf: &[u8], len: usize) {
        let mut cout = [0u8; 8];
        let c_ok = unsafe { pgc_macaddr8_in(buf.as_ptr(), cout.as_mut_ptr()) };

        let rust = macaddr8_in_internal(&buf[..len]);

        assert!(c_ok == rust.is_some() as i32);
        if let Some(addr) = rust {
            let rbytes = addr.to_bytes();
            for i in 0..8 {
                assert!(rbytes[i] == cout[i]);
            }
        }
    }

    /// Full-symbolic input over a symbolic length band [lo, hi], buffer
    /// capped at hi+1 bytes (bytes past `len` are forced to 0, so a
    /// hi-capped buffer denotes exactly the same input set as a larger
    /// one). NUL terminates the C string exactly at `len`; interior bytes
    /// are constrained non-NUL so the C string and the Rust slice denote
    /// the same input (SQL cstrings cannot contain NUL).
    ///
    /// The single len<=25 harness walled (>30s SAT after a 3.7s decision
    /// procedure), so the domain is range-partitioned into bands with
    /// per-band buffer caps (escalation ladder step 3);
    /// `sym_in_band_coverage` is the mandatory union-coverage harness
    /// proving the bands tile 0..=25. len 25 is exhaustive-complete for
    /// the accept domain: every accepted input has <= 23 significant
    /// chars, and the surrounding-whitespace paths are structurally
    /// length-insensitive (also witnessed at 25 by the band harnesses).
    macro_rules! sym_in_band_harness {
        ($name:ident, $lo:literal, $hi:literal, $uw:literal) => {
            #[kani::proof]
            #[kani::unwind($uw)]
            fn $name() {
                const CAP: usize = $hi;
                let mut buf: [u8; CAP + 1] = kani::any();
                let len: usize = kani::any();
                kani::assume(len >= $lo && len <= $hi);
                for i in 0..CAP + 1 {
                    if i < len {
                        kani::assume(buf[i] != 0);
                    } else {
                        buf[i] = 0;
                    }
                }

                check_in_parity(&buf, len);
            }
        };
    }

    // unwind = hi + 2 exactly (max per-byte loop iterations + 1); slack
    // copies of the loops are catastrophic for the SAT formula (TRIAGE).
    sym_in_band_harness!(eq_macaddr8_in_sym_len_0_12, 0, 12, 14);
    sym_in_band_harness!(eq_macaddr8_in_sym_len_13_18, 13, 18, 20);
    sym_in_band_harness!(eq_macaddr8_in_sym_len_19_22, 19, 22, 24);
    sym_in_band_harness!(eq_macaddr8_in_sym_len_23_25, 23, 25, 27);

    /// Union coverage: the bands above tile the whole domain 0..=25.
    #[kani::proof]
    fn sym_in_band_coverage() {
        let len: usize = kani::any();
        kani::assume(len <= 25);
        assert!(
            (0..=12).contains(&len)
                || (13..=18).contains(&len)
                || (19..=22).contains(&len)
                || (23..=25).contains(&len)
        );
    }

    /// Concrete-format witnesses: each canonical notation must be accepted
    /// by BOTH sides (guards the symbolic harness against vacuous-accept
    /// domains; the symbolic harness already proves parity). One harness
    /// per format (case-split: a single looped harness walled symex).
    fn check_in_accepts(case: &[u8]) {
        let len = case.len() - 1; // strip the explicit NUL
        let mut cout = [0u8; 8];
        let c_ok = unsafe { pgc_macaddr8_in(case.as_ptr(), cout.as_mut_ptr()) };
        let rust = macaddr8_in_internal(&case[..len]);
        assert!(c_ok == 1);
        assert!(rust.is_some());
        let rbytes = rust.unwrap().to_bytes();
        for i in 0..8 {
            assert!(rbytes[i] == cout[i]);
        }
    }

    macro_rules! witness_harness {
        ($name:ident, $input:expr) => {
            #[kani::proof]
            #[kani::unwind(27)]
            fn $name() {
                check_in_accepts($input);
            }
        };
    }

    // colon-separated EUI-64
    witness_harness!(wit_in_colon64, b"08:00:2b:01:02:03:04:05\0");
    // dash-separated EUI-64
    witness_harness!(wit_in_dash64, b"08-00-2b-01-02-03-04-05\0");
    // dot-separated (spacer optional per pair)
    witness_harness!(wit_in_dot64, b"0800.2b01.0203.0405\0");
    // bare hex EUI-64
    witness_harness!(wit_in_bare64, b"08002b0102030405\0");
    // colon-separated EUI-48 -> FF/FE fill
    witness_harness!(wit_in_colon48, b"08:00:2b:01:02:03\0");
    // bare hex EUI-48
    witness_harness!(wit_in_bare48, b"08002b010203\0");
    // leading/trailing whitespace
    witness_harness!(wit_in_ws64, b" 08002b0102030405 \0");

    // ---------- bitwise ops + trunc (oids 4120, 4121, 4122, 4112) ----------

    #[kani::proof]
    fn eq_macaddr8_not() {
        let b: [u8; 8] = kani::any();

        let rust = macaddr8_not(&MacAddr8::from_bytes(b)).to_bytes();
        let mut cout = [0u8; 8];
        unsafe { pgc_macaddr8_not(b.as_ptr(), cout.as_mut_ptr()) };
        assert_eq!(rust, cout);
    }

    #[kani::proof]
    fn eq_macaddr8_and() {
        let b1: [u8; 8] = kani::any();
        let b2: [u8; 8] = kani::any();

        let rust = macaddr8_and(&MacAddr8::from_bytes(b1), &MacAddr8::from_bytes(b2)).to_bytes();
        let mut cout = [0u8; 8];
        unsafe { pgc_macaddr8_and(b1.as_ptr(), b2.as_ptr(), cout.as_mut_ptr()) };
        assert_eq!(rust, cout);
    }

    #[kani::proof]
    fn eq_macaddr8_or() {
        let b1: [u8; 8] = kani::any();
        let b2: [u8; 8] = kani::any();

        let rust = macaddr8_or(&MacAddr8::from_bytes(b1), &MacAddr8::from_bytes(b2)).to_bytes();
        let mut cout = [0u8; 8];
        unsafe { pgc_macaddr8_or(b1.as_ptr(), b2.as_ptr(), cout.as_mut_ptr()) };
        assert_eq!(rust, cout);
    }

    #[kani::proof]
    fn eq_macaddr8_trunc() {
        let b: [u8; 8] = kani::any();

        let rust = macaddr8_trunc(&MacAddr8::from_bytes(b)).to_bytes();
        let mut cout = [0u8; 8];
        unsafe { pgc_macaddr8_trunc(b.as_ptr(), cout.as_mut_ptr()) };
        assert_eq!(rust, cout);
    }

    // ---------- set7bit + conversion operators (oids 4125, 4123, 4124) ----------

    #[kani::proof]
    fn eq_macaddr8_set7bit() {
        let b: [u8; 8] = kani::any();

        let rust = macaddr8_set7bit(&MacAddr8::from_bytes(b)).to_bytes();
        let mut cout = [0u8; 8];
        unsafe { pgc_macaddr8_set7bit(b.as_ptr(), cout.as_mut_ptr()) };
        assert_eq!(rust, cout);
    }

    #[kani::proof]
    fn eq_macaddrtomacaddr8() {
        let b: [u8; 6] = kani::any();

        let rust = macaddrtomacaddr8(&MacAddr::from_bytes(b)).to_bytes();
        let mut cout = [0u8; 8];
        unsafe { pgc_macaddrtomacaddr8(b.as_ptr(), cout.as_mut_ptr()) };
        assert_eq!(rust, cout);
    }

    /// macaddr8tomacaddr: full symbolic 8-byte domain, verdict parity
    /// (Rust Err ⇔ C ereport path), value parity on Ok, and on Err the
    /// shipped sqlstate (22003 ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, applied
    /// by the real `with_sqlstate` call) + level against C's errcode. The
    /// error MESSAGE plumbing is stubbed (`PgError::error` → field-identical
    /// constructor, see stub_pg_error_error): value/verdict/sqlstate are in
    /// the theorem, message/hint TEXT is not. The shipped `with_hint` call
    /// still executes (hint: Some(..) from a &'static str) — it did not wall
    /// symex, so it is NOT stubbed.
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stub_pg_error_error)]
    fn eq_macaddr8tomacaddr() {
        let b: [u8; 8] = kani::any();

        let mut cout = [0u8; 6];
        let cerr = unsafe { pgc_macaddr8tomacaddr(b.as_ptr(), cout.as_mut_ptr()) };

        match macaddr8tomacaddr(&MacAddr8::from_bytes(b)) {
            Ok(addr) => {
                // C succeeded too, with the identical 6 bytes.
                assert!(cerr == 0);
                let rbytes = addr.to_bytes();
                for i in 0..6 {
                    assert!(rbytes[i] == cout[i]);
                }
            }
            Err(e) => {
                // C raised ereport(ERROR, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE):
                // verdict + sqlstate parity (sqlstate set by the SHIPPED
                // with_sqlstate call, not the stub).
                assert!(cerr == 1);
                assert!(e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
                assert!(e.level == ERROR);
            }
        }
    }

    // ---------- negative control ----------

    /// Deliberately broken assertion: cmp must be symmetric under argument
    /// swap — false whenever b1 != b2. MUST FAIL with a counterexample;
    /// proves the rig is non-vacuous. Run with the DEFAULT solver.
    #[kani::proof]
    fn control_macaddr8_cmp_swap_must_fail() {
        let b1: [u8; 8] = kani::any();
        let b2: [u8; 8] = kani::any();

        let c = unsafe { pgc_macaddr8_cmp(b1.as_ptr(), b2.as_ptr()) };
        let rust = macaddr8_cmp_internal(&MacAddr8::from_bytes(b2), &MacAddr8::from_bytes(b1));
        assert!(c == rust); // wrong on purpose: args swapped on the Rust side
    }
}
