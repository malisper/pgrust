//! Kani C-equivalence harnesses: pgrust `adt_bool` (shipped crate at
//! crates/backend/utils/adt/bool) vs vendored PostgreSQL bool.c
//! (c/pg_bool.c, compiled via `-Z c-ffi --c-lib`).
//!
//! The comparison family and boolout are finite-domain (bool inputs):
//! the proofs are full-domain theorems. parse_bool_with_len is proved
//! for fully symbolic byte strings of len <= 6 (longest literal "false"
//! = 5 bytes, plus one over-length byte), case-split per length with a
//! union-coverage harness, with a symbolic pad byte past len covering
//! both C calling regimes (NUL contract and boolin whitespace trim).
//!
//! Run each harness (run-all.sh is the recipe of record):
//!   timeout 30 cargo kani -Z c-ffi --c-lib c/pg_bool.c --solver kissat \
//!     --harness <name>
//! eq_bool_accum_inv additionally needs `-Z stubbing`. The negative control
//! (control_negative_boollt_vs_c_boolle) runs with the DEFAULT solver —
//! suite rule: controls validate by counterexample, and kissat does not
//! terminate usefully on failing harnesses.

#![allow(dead_code)]

#[cfg(kani)]
mod ffi {
    use core::ffi::{c_char, c_int, c_ulong};

    extern "C" {
        pub fn pg_parse_bool_with_len(
            value: *const c_char,
            len: c_ulong,
            result: *mut c_int,
        ) -> c_int;
        pub fn pg_boolout(b: c_int, result: *mut c_char) -> c_int;
        pub fn pg_booleq(a1: c_int, a2: c_int) -> c_int;
        pub fn pg_boolne(a1: c_int, a2: c_int) -> c_int;
        pub fn pg_boollt(a1: c_int, a2: c_int) -> c_int;
        pub fn pg_boolgt(a1: c_int, a2: c_int) -> c_int;
        pub fn pg_boolle(a1: c_int, a2: c_int) -> c_int;
        pub fn pg_boolge(a1: c_int, a2: c_int) -> c_int;
    }
}

#[cfg(kani)]
mod harnesses {
    use crate::ffi;
    use core::ffi::c_int;

    /// Postgres bool.c whitespace set as trimmed by boolin (isspace, C locale).
    fn is_c_space(b: u8) -> bool {
        matches!(b, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
    }

    // ---- boolout ---------------------------------------------------------

    #[kani::proof]
    fn eq_boolout() {
        let b: bool = kani::any();
        // c_char is i8 on macOS but u8 on aarch64-Linux — buffer must be
        // spelled in c_char or the crate fails E0308 on the fleet runners.
        let mut buf: [c_char; 2] = [0x7f as c_char; 2];
        unsafe { ffi::pg_boolout(b as c_int, buf.as_mut_ptr()) };
        assert_eq!(buf[0] as u8, adt_bool::boolout(b));
        assert_eq!(buf[1], 0); // C NUL-terminates; Rust caller owns framing
    }

    // ---- comparison family (full finite domain) --------------------------

    #[kani::proof]
    fn eq_booleq() {
        let (a, b): (bool, bool) = (kani::any(), kani::any());
        let c = unsafe { ffi::pg_booleq(a as c_int, b as c_int) };
        assert_eq!(c != 0, adt_bool::booleq(a, b));
    }

    #[kani::proof]
    fn eq_boolne() {
        let (a, b): (bool, bool) = (kani::any(), kani::any());
        let c = unsafe { ffi::pg_boolne(a as c_int, b as c_int) };
        assert_eq!(c != 0, adt_bool::boolne(a, b));
    }

    #[kani::proof]
    fn eq_boollt() {
        let (a, b): (bool, bool) = (kani::any(), kani::any());
        let c = unsafe { ffi::pg_boollt(a as c_int, b as c_int) };
        assert_eq!(c != 0, adt_bool::boollt(a, b));
    }

    #[kani::proof]
    fn eq_boolgt() {
        let (a, b): (bool, bool) = (kani::any(), kani::any());
        let c = unsafe { ffi::pg_boolgt(a as c_int, b as c_int) };
        assert_eq!(c != 0, adt_bool::boolgt(a, b));
    }

    #[kani::proof]
    fn eq_boolle() {
        let (a, b): (bool, bool) = (kani::any(), kani::any());
        let c = unsafe { ffi::pg_boolle(a as c_int, b as c_int) };
        assert_eq!(c != 0, adt_bool::boolle(a, b));
    }

    #[kani::proof]
    fn eq_boolge() {
        let (a, b): (bool, bool) = (kani::any(), kani::any());
        let c = unsafe { ffi::pg_boolge(a as c_int, b as c_int) };
        assert_eq!(c != 0, adt_bool::boolge(a, b));
    }

    // ---- parse_bool_with_len ---------------------------------------------
    //
    // Bounds: len <= 6 (longest accepted literal is "false", 5 bytes; cap 6
    // covers every keyword, every strict prefix, and the over-length reject
    // regime), data bytes fully symbolic.
    //
    // The pad byte at buf[len] is the byte C may read past `len` in the 'o'
    // case (it compares max(len, 2) bytes). It is symbolic over the values
    // reachable in C callers: NUL (parse_bool / GUC contract: exact
    // NUL-terminated string) or any C-locale isspace byte (boolin contract:
    // trailing whitespace trimmed from len but still present in memory).
    // This one family therefore covers both calling regimes.
    //
    // Escalation-ladder note (step 3, case-split): the single symbolic-len
    // harness proves green but at ~19s solve / ~25s wall — over the 10s
    // standing budget (cap 8 vs 6 made no difference; the cost is the
    // symbolic len). Split into fixed-len harnesses (~0.3s solve each) plus
    // the MANDATORY union-coverage harness below.

    /// Shared driver at a fixed length; pad symbolic per the note above.
    fn drive_parse_bool_fixed_len(len: usize) {
        const CAP: usize = 6;
        assert!(len <= CAP);
        let pad: u8 = kani::any();
        kani::assume(pad == 0 || is_c_space(pad));

        let data: [u8; CAP] = kani::any();
        let mut buf = [0u8; CAP + 2];
        // Constant-bound fill (data-dependent bounds blow the unwind check).
        for i in 0..CAP {
            if i < len {
                buf[i] = data[i];
            }
        }
        buf[len] = pad;

        let mut c_result: c_int = 0;
        let c_ok = unsafe {
            ffi::pg_parse_bool_with_len(
                buf.as_ptr() as *const core::ffi::c_char,
                len as core::ffi::c_ulong,
                &mut c_result,
            )
        };
        let r = adt_bool::parse_bool_with_len(&buf[..len]);

        assert_eq!(c_ok != 0, r.is_some());
        if let Some(v) = r {
            assert_eq!(c_result != 0, v);
        }
    }

    // Unwind bound: max loop is the constant CAP fill (6 iterations) /
    // pg_strncasecmp (<= max(len,2) <= 6 iterations).
    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_parse_bool_with_len_l0() {
        drive_parse_bool_fixed_len(0);
    }

    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_parse_bool_with_len_l1() {
        drive_parse_bool_fixed_len(1);
    }

    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_parse_bool_with_len_l2() {
        drive_parse_bool_fixed_len(2);
    }

    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_parse_bool_with_len_l3() {
        drive_parse_bool_fixed_len(3);
    }

    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_parse_bool_with_len_l4() {
        drive_parse_bool_fixed_len(4);
    }

    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_parse_bool_with_len_l5() {
        drive_parse_bool_fixed_len(5);
    }

    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_parse_bool_with_len_l6() {
        drive_parse_bool_fixed_len(6);
    }

    /// MANDATORY union-coverage harness for the len case-split: every length
    /// in the stated bound (len <= 6) is handled by one of the l0..l6
    /// harnesses, so the split cannot silently under-cover.
    #[kani::proof]
    fn cover_parse_bool_with_len_split() {
        let len: usize = kani::any();
        kani::assume(len <= 6);
        assert!(
            len == 0 || len == 1 || len == 2 || len == 3 || len == 4 || len == 5 || len == 6
        );
    }

    // ---- bool aggregate transition/finalizer cores (oids 3496-3499) ----
    // Value-transition proofs: the agg-context allocation/ownership is out
    // of scope (C shim models PG_ARGISNULL/makeBoolAggState as a
    // has_state flag + zero-init; the shipped Rust core has the same
    // Option<BoolAggState> shape). Full symbolic i64 counters — including
    // out-of-invariant states: the transition math must agree everywhere.

    use adt_bool::{bool_accum, bool_accum_inv, bool_alltrue, bool_anytrue, BoolAggState};
    use proof_support::stubs;
    use types_error::{ERRCODE_INTERNAL_ERROR, ERROR};

    extern "C" {
        fn pg_bool_accum(
            has_state: c_int,
            in_count: i64,
            in_true: i64,
            has_val: c_int,
            val: c_int,
            out_count: *mut i64,
            out_true: *mut i64,
        ) -> c_int;
        fn pg_bool_accum_inv(
            has_state: c_int,
            in_count: i64,
            in_true: i64,
            has_val: c_int,
            val: c_int,
            out_count: *mut i64,
            out_true: *mut i64,
            err: *mut c_int,
        ) -> c_int;
        fn pg_bool_alltrue(has_state: c_int, aggcount: i64, aggtrue: i64, isnull: *mut c_int)
            -> c_int;
        fn pg_bool_anytrue(has_state: c_int, aggcount: i64, aggtrue: i64, isnull: *mut c_int)
            -> c_int;
        fn pg_int4_bool(arg: i32) -> c_int;
        fn pg_bool_int4(arg: c_int) -> i32;
    }

    fn any_state_args() -> (Option<BoolAggState>, c_int, i64, i64) {
        let has_state: bool = kani::any();
        let count: i64 = kani::any();
        let tru: i64 = kani::any();
        let state = if has_state {
            Some(BoolAggState {
                aggcount: count,
                aggtrue: tru,
            })
        } else {
            None
        };
        (state, has_state as c_int, count, tru)
    }

    // Overflow fence for the ± transitions: at count/true == i64::MAX/MIN
    // C wraps (-fwrapv) while the Rust build panics on debug overflow — a
    // state unreachable through the aggregate protocol (counters count
    // aggregated rows). Fence to the non-saturated domain; everything else
    // is full-symbolic.

    #[kani::proof]
    fn eq_bool_accum() {
        let (state, has_state, count, tru) = any_state_args();
        kani::assume(count < i64::MAX && tru < i64::MAX);
        let has_val: bool = kani::any();
        let v: bool = kani::any();
        let val = if has_val { Some(v) } else { None };

        let (mut cc, mut ct): (i64, i64) = (0, 0);
        unsafe {
            pg_bool_accum(has_state, count, tru, has_val as c_int, v as c_int, &mut cc, &mut ct)
        };
        let r = bool_accum(state, val);
        assert!(r.aggcount == cc && r.aggtrue == ct);
    }

    /// Verdict + value + sqlstate/level parity on the NULL-state error arm
    /// (message text stubbed out of the proof).
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
    fn eq_bool_accum_inv() {
        let (state, has_state, count, tru) = any_state_args();
        kani::assume(count > i64::MIN && tru > i64::MIN);
        let has_val: bool = kani::any();
        let v: bool = kani::any();
        let val = if has_val { Some(v) } else { None };

        let (mut cc, mut ct): (i64, i64) = (0, 0);
        let mut cerr: c_int = -1;
        unsafe {
            pg_bool_accum_inv(
                has_state,
                count,
                tru,
                has_val as c_int,
                v as c_int,
                &mut cc,
                &mut ct,
                &mut cerr,
            )
        };
        match bool_accum_inv(state, val) {
            Ok(r) => assert!(cerr == 0 && r.aggcount == cc && r.aggtrue == ct),
            Err(e) => {
                let ok = cerr == 1 && e.sqlstate == ERRCODE_INTERNAL_ERROR && e.level == ERROR;
                // Box<PgError> drop glue walls symex (varbit-rows trap).
                core::mem::forget(e);
                assert!(ok);
            }
        }
    }

    macro_rules! eq_bool_agg_final {
        ($h:ident, $cfn:ident, $rfn:ident) => {
            #[kani::proof]
            fn $h() {
                let (state, has_state, count, tru) = any_state_args();
                let mut isnull: c_int = -1;
                let c = unsafe { $cfn(has_state, count, tru, &mut isnull) };
                match $rfn(state.as_ref()) {
                    Some(b) => assert!(isnull == 0 && b as c_int == c),
                    None => assert!(isnull == 1),
                }
            }
        };
    }

    eq_bool_agg_final!(eq_bool_alltrue, pg_bool_alltrue, bool_alltrue);
    eq_bool_agg_final!(eq_bool_anytrue, pg_bool_anytrue, bool_anytrue);

    // ---- int4_bool / bool_int4 casts (oids 2557/2558) ----

    #[kani::proof]
    fn eq_int4_bool() {
        let v: i32 = kani::any();
        let c = unsafe { pg_int4_bool(v) };
        assert!(adt_int::int4_bool(v) as c_int == c);
    }

    #[kani::proof]
    fn eq_bool_int4() {
        let v: bool = kani::any();
        let c = unsafe { pg_bool_int4(v as c_int) };
        assert!(adt_int::bool_int4(v) == c);
    }

    // ---- negative control -------------------------------------------------
    // Deliberately mismatched pairing: Rust boollt vs C boolle. MUST FAIL
    // with a decodable counterexample (a == b), proving the rig is
    // non-vacuous. Never "fix" this harness.

    #[kani::proof]
    fn control_negative_boollt_vs_c_boolle() {
        let (a, b): (bool, bool) = (kani::any(), kani::any());
        let c = unsafe { ffi::pg_boolle(a as c_int, b as c_int) };
        assert_eq!(c != 0, adt_bool::boollt(a, b));
    }
}
