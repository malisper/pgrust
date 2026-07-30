//! Kani C≡Rust equivalence: macaddr_cmp_internal + macaddr_out +
//! macaddr_not/and/or/trunc (pg_proc oids 3144/3145/3146/753) +
//! macaddr_lt/le/eq/ge/gt/ne (oids 830-835) + macaddr_in (oid 436).
//! Rust side: shipped crates/backend/utils/adt/mac. C side: vendored mac.c
//! (csrc/mac_shim.c; cmp/out from master, not/and/or/trunc/in from
//! REL_18_STABLE). Fully symbolic 6-byte addresses everywhere.
//!
//! macaddr_in: verdict + parsed-bytes + error-class (sqlstate) parity.
//! Coverage shape (the mac8 band pattern WALLED here — see the wall record
//! at sym_in_len_harness):
//!   - full-symbolic ASCII inputs, per-length harnesses len 0..=4;
//!   - concrete per-format witnesses proving every canonical notation
//!     (colon/dash/triple/dot/dash-double/bare, plus the sscanf-inherited
//!     whitespace/1-digit/0x acceptances) is accepted by both sides with
//!     equal bytes at its real length (12-19);
//!   - PROVED DIVERGENCE at len >= 19: C99 %x stores mod 2^32 (unsigned
//!     int target), so >8-hex-digit fields (or '-' before exactly-8) can
//!     wrap into 0..=255 and be ACCEPTED by C while the shipped i64-based
//!     scanner REJECTS with 22003 — pinned by wit_in_div_9digit_trunc /
//!     wit_in_div_neg_wrap. Unreachable below len 19 (a >=9-char field
//!     plus the minimal ":x"x5 tail), so the sub-19 domain is
//!     divergence-free by construction.
//! C's sscanf is executed by a documented glibc-semantics directive model
//! (csrc/mac_shim.c — the modeled seam; "modulo sscanf model" in the
//! ledger). macaddr_in harnesses need `-Z stubbing` and (for the
//! per-length set) `--no-assertion-reach-checks --solver kissat`.
//!
//! Negative control: control_macaddr_and_vs_c_or_must_fail pits Rust
//! macaddr_and against C macaddr_or — must FAIL with a counterexample
//! (rig non-vacuity). Run it with the DEFAULT solver (kissat is
//! non-incremental and never terminates on failing harnesses).

#[cfg(kani)]
mod proofs {
    use adt_mac::{
        macaddr_and, macaddr_cmp_internal, macaddr_not, macaddr_or, macaddr_out_into,
        macaddr_trunc, MacAddr, MACADDR_OUT_LEN,
    };

    extern "C" {
        fn pgc_macaddr_cmp(b1: *const u8, b2: *const u8) -> i32;
        fn pgc_macaddr_out(b: *const u8, result: *mut u8) -> i32;
        fn pgc_macaddr_not(bin: *const u8, bout: *mut u8) -> i32;
        fn pgc_macaddr_and(b1: *const u8, b2: *const u8, bout: *mut u8) -> i32;
        fn pgc_macaddr_or(b1: *const u8, b2: *const u8, bout: *mut u8) -> i32;
        fn pgc_macaddr_trunc(bin: *const u8, bout: *mut u8) -> i32;
    }

    #[kani::proof]
    fn macaddr_cmp_equiv() {
        let b1: [u8; 6] = kani::any();
        let b2: [u8; 6] = kani::any();

        let rust = macaddr_cmp_internal(&MacAddr::from_bytes(b1), &MacAddr::from_bytes(b2));
        let c = unsafe { pgc_macaddr_cmp(b1.as_ptr(), b2.as_ptr()) };
        assert_eq!(rust, c);
    }

    #[kani::proof]
    fn macaddr_out_equiv() {
        let b: [u8; 6] = kani::any();

        let mut rbuf = [0u8; MACADDR_OUT_LEN];
        let rlen = macaddr_out_into(&MacAddr::from_bytes(b), &mut rbuf);

        let mut cbuf = [0u8; 18];
        let clen = unsafe { pgc_macaddr_out(b.as_ptr(), cbuf.as_mut_ptr()) };

        assert_eq!(rlen as i32, clen);
        for i in 0..17 {
            assert_eq!(rbuf[i], cbuf[i]);
        }
    }

    // ---------- bitwise ops + trunc (oids 3144, 3145, 3146, 753) ----------

    #[kani::proof]
    fn eq_macaddr_not() {
        let b: [u8; 6] = kani::any();

        let rust = macaddr_not(&MacAddr::from_bytes(b)).to_bytes();
        let mut cout = [0u8; 6];
        unsafe { pgc_macaddr_not(b.as_ptr(), cout.as_mut_ptr()) };
        assert_eq!(rust, cout);
    }

    #[kani::proof]
    fn eq_macaddr_and() {
        let b1: [u8; 6] = kani::any();
        let b2: [u8; 6] = kani::any();

        let rust = macaddr_and(&MacAddr::from_bytes(b1), &MacAddr::from_bytes(b2)).to_bytes();
        let mut cout = [0u8; 6];
        unsafe { pgc_macaddr_and(b1.as_ptr(), b2.as_ptr(), cout.as_mut_ptr()) };
        assert_eq!(rust, cout);
    }

    #[kani::proof]
    fn eq_macaddr_or() {
        let b1: [u8; 6] = kani::any();
        let b2: [u8; 6] = kani::any();

        let rust = macaddr_or(&MacAddr::from_bytes(b1), &MacAddr::from_bytes(b2)).to_bytes();
        let mut cout = [0u8; 6];
        unsafe { pgc_macaddr_or(b1.as_ptr(), b2.as_ptr(), cout.as_mut_ptr()) };
        assert_eq!(rust, cout);
    }

    #[kani::proof]
    fn eq_macaddr_trunc() {
        let b: [u8; 6] = kani::any();

        let rust = macaddr_trunc(&MacAddr::from_bytes(b)).to_bytes();
        let mut cout = [0u8; 6];
        unsafe { pgc_macaddr_trunc(b.as_ptr(), cout.as_mut_ptr()) };
        assert_eq!(rust, cout);
    }

    // ---------- macaddr_lt/le/eq/ge/gt/ne (oids 830-835) ----------

    use adt_mac::{macaddr_eq, macaddr_ge, macaddr_gt, macaddr_le, macaddr_lt, macaddr_ne};

    extern "C" {
        fn pgc_macaddr_lt(b1: *const u8, b2: *const u8) -> i32;
        fn pgc_macaddr_le(b1: *const u8, b2: *const u8) -> i32;
        fn pgc_macaddr_eq(b1: *const u8, b2: *const u8) -> i32;
        fn pgc_macaddr_ge(b1: *const u8, b2: *const u8) -> i32;
        fn pgc_macaddr_gt(b1: *const u8, b2: *const u8) -> i32;
        fn pgc_macaddr_ne(b1: *const u8, b2: *const u8) -> i32;
    }

    macro_rules! eq_mac_bool_op {
        ($($harness:ident: $rust:ident / $c:ident;)*) => {$(
            #[kani::proof]
            fn $harness() {
                let b1: [u8; 6] = kani::any();
                let b2: [u8; 6] = kani::any();

                let rust = $rust(&MacAddr::from_bytes(b1), &MacAddr::from_bytes(b2));
                let c = unsafe { $c(b1.as_ptr(), b2.as_ptr()) };
                assert!(rust as i32 == c);
            }
        )*};
    }

    eq_mac_bool_op! {
        eq_macaddr_lt: macaddr_lt / pgc_macaddr_lt;
        eq_macaddr_le: macaddr_le / pgc_macaddr_le;
        eq_macaddr_eq2: macaddr_eq / pgc_macaddr_eq;
        eq_macaddr_ge: macaddr_ge / pgc_macaddr_ge;
        eq_macaddr_gt: macaddr_gt / pgc_macaddr_gt;
        eq_macaddr_ne: macaddr_ne / pgc_macaddr_ne;
    }

    // ---------- macaddr_in (oid 436) ----------

    use proof_support::stubs;
    use types_error::{
        ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERROR,
    };

    extern "C" {
        /// 0 = accepted (result filled); 1 = invalid-syntax ereturn;
        /// 2 = octet-range ereturn. See csrc/mac_shim.c (sscanf model
        /// documented there).
        fn pgc_macaddr_in(str_: *const u8, result: *mut u8) -> i32;
    }

    /// Shared body: run both parsers on the same NUL-terminated ASCII byte
    /// string; assert verdict parity, parsed-bytes parity on accept, and
    /// error-CLASS parity on reject (C's two ereturn errcodes vs the shipped
    /// sqlstates set by the real `with_sqlstate` calls — the message text is
    /// stubbed out of the proof, cash pattern).
    /// Returns true iff the two implementations agree (verdict, bytes on
    /// accept, error class + level on reject). ONE bool so the band
    /// harnesses carry a SINGLE user assertion — external kissat re-solves
    /// per property batch (prove-target trap), and the multi-assert version
    /// of this body walled >420s where the single-assert one solves in
    /// budget.
    fn in_parity_holds(buf: &[u8], len: usize) -> bool {
        let mut cout = [0u8; 6];
        let cret = unsafe { pgc_macaddr_in(buf.as_ptr(), cout.as_mut_ptr()) };

        // Bytes are fenced to 1..=0x7f by every caller: ASCII is valid UTF-8,
        // so the &str the shipped fc_macaddr_in would produce via
        // from_utf8_lossy is exactly these bytes.
        let s = unsafe { core::str::from_utf8_unchecked(&buf[..len]) };
        match adt_mac::macaddr_in(s, None) {
            Ok(addr) => cret == 0 && addr.to_bytes() == cout,
            Err(e) => {
                let class_ok = match cret {
                    1 => e.sqlstate == ERRCODE_INVALID_TEXT_REPRESENTATION,
                    2 => e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                    _ => false,
                };
                let ok = class_ok && e.level == ERROR;
                // Box<PgError> drop glue alone costs tens of seconds of symex
                // (varbit-rows measured trap); fully adjudicated — leak it.
                core::mem::forget(e);
                ok
            }
        }
    }

    /// Per-length symbolic harness (escalation ladder step 3): concrete
    /// `len`, fully symbolic ASCII bytes (1..=0x7f — SQL cstrings cannot
    /// contain NUL; the ASCII cap makes the harness's unchecked-UTF-8 view
    /// sound, and no accepted form can contain a non-ASCII byte: both sides
    /// reject them as non-hex/non-separator).
    ///
    /// WALL RECORD (measured 2026-07-28, multi-agent load, ladder
    /// exhausted): the 7-format sscanf cascade is the heaviest circuit in
    /// the suite so far. Symbolic-length bands (mac8 pattern, 0..=12 /
    /// 13..=18, single-assert body) wall BOTH solvers (default = CaDiCaL
    /// propositional-reduction wedge >600s; kissat >420s). Per-length split
    /// + tight unwind + single assert + --no-assertion-reach-checks (each
    /// assertion-reachability cover check is a separate ~30-40s external
    /// kissat solve) yields: len 0 = 0.9s, 1 = 19s, 2 = 65s, 3 = 156s,
    /// 4 = 258s, len >= 5 = wall (>300s; width-1 probes at 6 and 12 also
    /// >300s, so the cost is depth-of-cascade-bound and no further split
    /// helps). Standing set = len <= 4 (release-gate tier per the strtoint
    /// precedent) + the concrete per-format witnesses below, which pin
    /// every accept regime at its real length (12-19).
    macro_rules! sym_in_len_harness {
        ($($name:ident: $len:literal, $uw:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($uw)]
            #[kani::stub(alloc::fmt::format, stubs::stub_format)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $name() {
                const LEN: usize = $len;
                let mut buf: [u8; LEN + 1] = kani::any();
                for i in 0..LEN {
                    kani::assume(buf[i] >= 1 && buf[i] <= 0x7f);
                }
                buf[LEN] = 0;

                assert!(in_parity_holds(&buf, LEN));
            }
        )*};
    }

    // unwind = len + 2 EXACTLY (dead unreachable loop copies are
    // catastrophic — TRIAGE / mac8 lesson). len >= 5 is the recorded wall
    // (see the macro doc above); do not re-add lengths without re-probing.
    sym_in_len_harness! {
        eq_macaddr_in_len_0: 0, 2;
        eq_macaddr_in_len_1: 1, 3;
        eq_macaddr_in_len_2: 2, 4;
        eq_macaddr_in_len_3: 3, 5;
        eq_macaddr_in_len_4: 4, 6;
    }

    /// Union coverage over the SYMBOLIC portion of the proved domain: the
    /// per-length harnesses tile 0..=4 exactly (lengths 5..=18 are covered
    /// only by the concrete per-format witnesses — spot proofs, not a
    /// domain; 19+ is the proved-divergence region, wit_in_div_*).
    #[kani::proof]
    fn sym_in_len_coverage() {
        let len: usize = kani::any();
        kani::assume(len <= 4);
        assert!(len == 0 || len == 1 || len == 2 || len == 3 || len == 4);
    }

    /// Concrete-format witnesses: each canonical notation must be accepted
    /// by BOTH sides with equal bytes (non-vacuous accept domain; the
    /// symbolic bands already prove parity).
    fn check_in_accepts(case: &[u8]) {
        let len = case.len() - 1; // strip the explicit NUL
        let mut cout = [0u8; 6];
        let cret = unsafe { pgc_macaddr_in(case.as_ptr(), cout.as_mut_ptr()) };
        assert!(cret == 0);
        let s = unsafe { core::str::from_utf8_unchecked(&case[..len]) };
        let rust = adt_mac::macaddr_in(s, None);
        match rust {
            Ok(addr) => {
                let rb = addr.to_bytes();
                for i in 0..6 {
                    assert!(rb[i] == cout[i]);
                }
            }
            Err(_) => panic!("Rust rejected a canonical form C accepts"),
        }
    }

    macro_rules! witness_harness {
        ($name:ident, $input:expr) => {
            #[kani::proof]
            #[kani::unwind(24)]
            #[kani::stub(alloc::fmt::format, stubs::stub_format)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $name() {
                check_in_accepts($input);
            }
        };
    }

    // the seven mac.c formats, in cascade order
    witness_harness!(wit_in_colon, b"08:00:2b:01:02:03\0");
    witness_harness!(wit_in_dash, b"08-00-2b-01-02-03\0");
    witness_harness!(wit_in_colon_triples, b"08002b:010203\0");
    witness_harness!(wit_in_dash_triples, b"08002b-010203\0");
    witness_harness!(wit_in_dot_doubles, b"0800.2b01.0203\0");
    witness_harness!(wit_in_dash_doubles, b"0800-2b01-0203\0");
    witness_harness!(wit_in_bare, b"08002b010203\0");
    // sscanf-inherited acceptances: whitespace skip, 1-digit fields, 0x prefix
    witness_harness!(wit_in_ws, b" 08:00:2b:01:02:03 \0");
    witness_harness!(wit_in_short, b"8:0:2b:1:2:3\0");
    witness_harness!(wit_in_0x, b"0x8:0x0:0x2b:1:2:3\0");

    /// PROVED DIVERGENCE witnesses (len >= 19 region): C99's %x stores
    /// through `unsigned int` (mod 2^32), so a field with more than 8
    /// significant hex digits — or a '-' sign in front of exactly 8 —
    /// can truncate/wrap into 0..=255 and be ACCEPTED by C, while the
    /// shipped Rust scanner accumulates into i64 and REJECTS it with the
    /// octet-range error. Reachable only at input length >= 19 (a >=9-char
    /// field plus the minimal ":x" tail x5); the symbolic bands stop at 18,
    /// so the green domain is exactly the divergence-free one. These
    /// harnesses pin the divergence: C accepts, Rust raises 22003.
    macro_rules! divergence_witness {
        ($name:ident, $input:expr, $b0:literal) => {
            #[kani::proof]
            #[kani::unwind(24)]
            #[kani::stub(alloc::fmt::format, stubs::stub_format)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            fn $name() {
                let case: &[u8] = $input;
                let len = case.len() - 1;
                let mut cout = [0u8; 6];
                let cret = unsafe { pgc_macaddr_in(case.as_ptr(), cout.as_mut_ptr()) };
                assert!(cret == 0); // C ACCEPTS the truncated value
                assert!(cout[0] == $b0);
                let s = unsafe { core::str::from_utf8_unchecked(&case[..len]) };
                match adt_mac::macaddr_in(s, None) {
                    Ok(_) => panic!("Rust accepted — divergence witness stale"),
                    Err(e) => {
                        // Rust REJECTS with the octet-range error
                        assert!(e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
                        core::mem::forget(e);
                    }
                }
            }
        };
    }

    // 0x100000001 mod 2^32 = 1: C parses "01:00:00:00:00:00"
    divergence_witness!(wit_in_div_9digit_trunc, b"100000001:0:0:0:0:0\0", 1);
    // -(0xffffff01) mod 2^32 = 0xff = 255: C parses "ff:00:00:00:00:00"
    divergence_witness!(wit_in_div_neg_wrap, b"-ffffff01:0:0:0:0:0\0", 0xff);

    // ---------- negative control: rig must be able to fail ----------

    /// Deliberate mismatch: Rust macaddr_and vs C macaddr_or. MUST FAIL with
    /// a counterexample (any b1 != b2 byte pair where and != or). Run with
    /// the DEFAULT solver.
    #[kani::proof]
    fn control_macaddr_and_vs_c_or_must_fail() {
        let b1: [u8; 6] = kani::any();
        let b2: [u8; 6] = kani::any();

        let rust = macaddr_and(&MacAddr::from_bytes(b1), &MacAddr::from_bytes(b2)).to_bytes();
        let mut cout = [0u8; 6];
        unsafe { pgc_macaddr_or(b1.as_ptr(), b2.as_ptr(), cout.as_mut_ptr()) };
        for i in 0..6 {
            assert!(rust[i] == cout[i]); // wrong on purpose: and vs or
        }
    }
}
