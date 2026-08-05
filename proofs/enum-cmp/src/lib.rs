//! enum-cmp proof family — C≡Rust equivalence for the enum comparison
//! surface (7 pg_proc rows): enum_lt (3510) / enum_gt (3511) / enum_le
//! (3512) / enum_ge (3513) / enum_cmp (3514) / enum_smaller (3524) /
//! enum_larger (3525).
//!
//! C side: c/pg_enum_cmp.c — verbatim REL_18_STABLE enum.c bodies; see its
//! header for provenance and the full shim manifest.
//!
//! Rust side (SHIPPED code, never copied): adt_enum::builtins::{fc_enum_lt,
//! fc_enum_le, fc_enum_ge, fc_enum_gt, fc_enum_cmp, fc_enum_smaller,
//! fc_enum_larger} — WRAPPER-LEVEL: a real LocalFcinfo<2> frame plus a real
//! FmgrInfo, so datum unwrap -> enum_cmp_internal -> Datum pack AND the
//! fn_extra memo protocol are inside the theorem.
//!
//! Seam model (state-seam pattern; both sides read the SAME C statics):
//!  - compare_values_of_enum (typcache.c, catalog sort-order engine) is a
//!    seam. Both sides answer from one shared symbolic i32 oracle
//!    (pg_enum_oracle_cmp), so the theorem quantifies over EVERY possible
//!    sort-order verdict; only the typcache internals leave the proof.
//!    The seam call's (typeoid, arg1, arg2) inputs are recorded on both
//!    sides and asserted equal (trace parity — a keyless oracle would
//!    otherwise mask a wrong-typeoid divergence).
//!  - ENUMOID syscache lookup (odd-OID cold arm) is a seam: shared
//!    symbolic (found, enumtypid) oracle; miss arm stays in-theorem as
//!    verdict + sqlstate parity (22P03).
//!  - fn_extra memo: C caches a TypeCacheEntry* keyed by the enum type OID;
//!    shipped Rust caches the type OID itself. The model carries the OID on
//!    both sides; memo-WRITE parity is asserted (cold arm must install the
//!    oracle's typeoid, fast paths must not touch it).
//!
//! Soundness notes for the ledger:
//!  - value-space only on the Err arm: message text/location out of proof
//!    (PgError::new stubbed field-identically, std format stubbed);
//!    sqlstate (22P03) + level (ERROR), set by SHIPPED with_sqlstate code,
//!    stay in-theorem.
//!  - flinfo-less (None) call surface (tuplesort shim, documented shipped
//!    divergence note at adt_enum/src/lib.rs:83) is NOT covered here: C's
//!    fmgr surface always passes flinfo; these rows are the fmgr surface.
//!  - skew controls (MUST FAIL, default solver): cmp-oracle skew and
//!    lookup-typeoid skew prove both seam models are load-bearing.

use std::os::raw::c_int;

use types_core::Oid;

extern "C" {
    // shared oracle statics (defined in c/pg_enum_cmp.c)
    pub static mut pg_enum_oracle_lookup_typeoid: Oid;
    pub static mut pg_enum_oracle_lookup_found: c_int;
    pub static mut pg_enum_oracle_cmp: c_int;
    // C-side trace recording
    pub static mut pg_enum_trace_cmp_called: c_int;
    pub static mut pg_enum_trace_cmp_typeoid: Oid;
    pub static mut pg_enum_trace_cmp_arg1: Oid;
    pub static mut pg_enum_trace_cmp_arg2: Oid;
    pub static mut pg_enum_memo_written_flag: c_int;
    pub static mut pg_enum_memo_written: Oid;

    pub fn pg_enum_trace_reset() -> c_int;

    pub fn pg_enum_lt(a: Oid, b: Oid, has_memo: c_int, memo: Oid, out: *mut c_int, err: *mut c_int) -> c_int;
    pub fn pg_enum_le(a: Oid, b: Oid, has_memo: c_int, memo: Oid, out: *mut c_int, err: *mut c_int) -> c_int;
    pub fn pg_enum_ge(a: Oid, b: Oid, has_memo: c_int, memo: Oid, out: *mut c_int, err: *mut c_int) -> c_int;
    pub fn pg_enum_gt(a: Oid, b: Oid, has_memo: c_int, memo: Oid, out: *mut c_int, err: *mut c_int) -> c_int;
    pub fn pg_enum_smaller(a: Oid, b: Oid, has_memo: c_int, memo: Oid, out: *mut Oid, err: *mut c_int) -> c_int;
    pub fn pg_enum_larger(a: Oid, b: Oid, has_memo: c_int, memo: Oid, out: *mut Oid, err: *mut c_int) -> c_int;
    pub fn pg_enum_cmp(a: Oid, b: Oid, has_memo: c_int, memo: Oid, out: *mut c_int, err: *mut c_int) -> c_int;
}

#[cfg(kani)]
mod proofs {
    use super::*;
    use core::sync::atomic::{AtomicBool, AtomicU32, Ordering::Relaxed};

    use datum::Datum;
    use proof_support::fcinfo::{fci, FcFn};
    use proof_support::stubs;
    use syscache_seams::PgEnumShape;
    use types_error::{PgError, ERRCODE_INVALID_BINARY_REPRESENTATION, ERROR};
    use types_fmgr::FmgrInfo;

    // ---- Rust-side seam stubs: answer from the SHARED C statics, record
    //      the call inputs for trace parity. ----

    static R_CMP_CALLED: AtomicBool = AtomicBool::new(false);
    static R_CMP_TYPEOID: AtomicU32 = AtomicU32::new(0);
    static R_CMP_A1: AtomicU32 = AtomicU32::new(0);
    static R_CMP_A2: AtomicU32 = AtomicU32::new(0);

    fn stub_compare_values_of_enum(type_id: Oid, arg1: Oid, arg2: Oid) -> types_error::PgResult<i32> {
        R_CMP_CALLED.store(true, Relaxed);
        R_CMP_TYPEOID.store(type_id, Relaxed);
        R_CMP_A1.store(arg1, Relaxed);
        R_CMP_A2.store(arg2, Relaxed);
        // SAFETY: single-threaded harness; static written before the call.
        Ok(unsafe { pg_enum_oracle_cmp })
    }

    fn stub_lookup_pg_enum_by_oid(enum_oid: Oid) -> types_error::PgResult<Option<PgEnumShape>> {
        // SAFETY: single-threaded harness reads of the shared oracle.
        if unsafe { pg_enum_oracle_lookup_found } == 0 {
            return Ok(None);
        }
        Ok(Some(PgEnumShape {
            oid: enum_oid,
            enumtypid: unsafe { pg_enum_oracle_lookup_typeoid },
            enumlabel: types_tuple::NameData::default(),
            xmin: 0,
            xmin_committed: true,
        }))
    }

    fn reset_traces() {
        R_CMP_CALLED.store(false, Relaxed);
        R_CMP_TYPEOID.store(0, Relaxed);
        R_CMP_A1.store(0, Relaxed);
        R_CMP_A2.store(0, Relaxed);
        unsafe { pg_enum_trace_reset() };
    }

    /// Arm the oracles with one symbolic assignment (both sides read these).
    fn arm_oracles(found: bool, typeoid: Oid, cmp: i32) {
        // SAFETY: single-threaded harness; C reads them after this write.
        unsafe {
            pg_enum_oracle_lookup_found = found as c_int;
            pg_enum_oracle_lookup_typeoid = typeoid;
            pg_enum_oracle_cmp = cmp;
        }
    }

    /// Invoke a shipped fc_* wrapper with a real FmgrInfo carrying the
    /// symbolic memo plane; returns (result, memo_after).
    fn drive(
        fc: FcFn<Box<PgError>>,
        a: Oid,
        b: Oid,
        has_memo: bool,
        memo_typeoid: Oid,
    ) -> (Result<Datum, Box<PgError>>, Option<Oid>) {
        let mut fl = FmgrInfo::unresolved();
        if has_memo {
            fl.set_fn_extra(memo_typeoid);
        }
        let mut f = fci([Datum::from_oid(a), Datum::from_oid(b)]);
        let r = fc(Some(&mut fl), &mut f);
        let memo_after = fl.fn_extra_ref::<Oid>().copied();
        (r, memo_after)
    }

    /// Shared post-call parity asserts: trace + memo protocol.
    fn assert_seam_parity(has_memo: bool, memo_typeoid: Oid, memo_after: Option<Oid>) {
        unsafe {
            // cmp-seam call/input parity
            assert!(R_CMP_CALLED.load(Relaxed) == (pg_enum_trace_cmp_called != 0));
            if pg_enum_trace_cmp_called != 0 {
                assert!(R_CMP_TYPEOID.load(Relaxed) == pg_enum_trace_cmp_typeoid);
                assert!(R_CMP_A1.load(Relaxed) == pg_enum_trace_cmp_arg1);
                assert!(R_CMP_A2.load(Relaxed) == pg_enum_trace_cmp_arg2);
            }
            // memo-write parity: C wrote fn_extra iff Rust did (beyond the
            // preinstalled memo), and with the same type OID.
            if pg_enum_memo_written_flag != 0 {
                assert!(memo_after == Some(pg_enum_memo_written));
            } else if has_memo {
                assert!(memo_after == Some(memo_typeoid));
            } else {
                assert!(memo_after.is_none());
            }
        }
    }

    macro_rules! eq_enum_op {
        ($($h:ident: $fc:path, $cfn:ident, $check:expr;)*) => {$(
            /// Full plane: symbolic (a, b, memo presence, memoized type OID,
            /// lookup oracle, cmp oracle). Covers equal/even-even fast paths,
            /// memoized + cold seam arms, and the 22P03 miss arm.
            #[kani::proof]
            #[kani::stub(typcache_seams::compare_values_of_enum::call, stub_compare_values_of_enum)]
            #[kani::stub(syscache_seams::lookup_pg_enum_by_oid::call, stub_lookup_pg_enum_by_oid)]
            #[kani::stub(types_error::PgError::new, stubs::stub_pg_error_new)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let a: Oid = kani::any();
                let b: Oid = kani::any();
                let has_memo: bool = kani::any();
                let memo_typeoid: Oid = kani::any();
                let found: bool = kani::any();
                let lookup_typeoid: Oid = kani::any();
                let cmp: i32 = kani::any();
                arm_oracles(found, lookup_typeoid, cmp);
                reset_traces();

                let mut c_out: c_int = 0;
                let mut c_err: c_int = 0;
                unsafe { $cfn(a, b, has_memo as c_int, memo_typeoid, &mut c_out, &mut c_err) };

                let (r, memo_after) = drive($fc, a, b, has_memo, memo_typeoid);
                match r {
                    Ok(d) => {
                        assert!(c_err == 0);
                        let check: fn(Datum, c_int) -> bool = $check;
                        assert!(check(d, c_out));
                        kani::cover!(a == b);
                        kani::cover!((a & 1) == 0 && (b & 1) == 0 && a != b);
                        kani::cover!(has_memo && ((a & 1) != 0 || (b & 1) != 0) && a != b);
                        kani::cover!(!has_memo && ((a & 1) != 0 || (b & 1) != 0) && a != b);
                    }
                    Err(e) => {
                        assert!(c_err == 1);
                        assert!(e.level == ERROR);
                        assert!(e.sqlstate == ERRCODE_INVALID_BINARY_REPRESENTATION);
                        kani::cover!(true); // Err arm reachable
                        core::mem::forget(e);
                    }
                }
                assert_seam_parity(has_memo, memo_typeoid, memo_after);
            }
        )*};
    }

    eq_enum_op! {
        eq_enum_lt:  adt_enum::builtins::fc_enum_lt,  pg_enum_lt,  |d, c| d.as_bool() == (c != 0);
        eq_enum_le:  adt_enum::builtins::fc_enum_le,  pg_enum_le,  |d, c| d.as_bool() == (c != 0);
        eq_enum_ge:  adt_enum::builtins::fc_enum_ge,  pg_enum_ge,  |d, c| d.as_bool() == (c != 0);
        eq_enum_gt:  adt_enum::builtins::fc_enum_gt,  pg_enum_gt,  |d, c| d.as_bool() == (c != 0);
        eq_enum_cmp: adt_enum::builtins::fc_enum_cmp, pg_enum_cmp, |d, c| d.as_i32() == c;
    }

    macro_rules! eq_enum_minmax {
        ($($h:ident: $fc:path, $cfn:ident;)*) => {$(
            /// WINNER-IDENTITY theorem (divergence-#9 shape): asserts WHICH
            /// argument won, C's own choice as the oracle — including the
            /// tie plane (cmp oracle = 0 on distinct odd OIDs).
            #[kani::proof]
            #[kani::stub(typcache_seams::compare_values_of_enum::call, stub_compare_values_of_enum)]
            #[kani::stub(syscache_seams::lookup_pg_enum_by_oid::call, stub_lookup_pg_enum_by_oid)]
            #[kani::stub(types_error::PgError::new, stubs::stub_pg_error_new)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            fn $h() {
                let a: Oid = kani::any();
                let b: Oid = kani::any();
                let has_memo: bool = kani::any();
                let memo_typeoid: Oid = kani::any();
                let found: bool = kani::any();
                let lookup_typeoid: Oid = kani::any();
                let cmp: i32 = kani::any();
                arm_oracles(found, lookup_typeoid, cmp);
                reset_traces();

                let mut c_out: Oid = 0;
                let mut c_err: c_int = 0;
                unsafe { $cfn(a, b, has_memo as c_int, memo_typeoid, &mut c_out, &mut c_err) };

                let (r, memo_after) = drive($fc, a, b, has_memo, memo_typeoid);
                match r {
                    Ok(d) => {
                        assert!(c_err == 0);
                        // winner identity: the returned OID is C's winner
                        assert!(d.as_oid() == c_out);
                        // tie plane reachable (cmp=0 on distinct odd oids)
                        kani::cover!(cmp == 0 && a != b && ((a & 1) != 0 || (b & 1) != 0) && c_err == 0);
                    }
                    Err(e) => {
                        assert!(c_err == 1);
                        assert!(e.level == ERROR);
                        assert!(e.sqlstate == ERRCODE_INVALID_BINARY_REPRESENTATION);
                        core::mem::forget(e);
                    }
                }
                assert_seam_parity(has_memo, memo_typeoid, memo_after);
            }
        )*};
    }

    eq_enum_minmax! {
        eq_enum_smaller: adt_enum::builtins::fc_enum_smaller, pg_enum_smaller;
        eq_enum_larger:  adt_enum::builtins::fc_enum_larger,  pg_enum_larger;
    }

    /// MUST FAIL (cmp-seam model is load-bearing): C answers from a skewed
    /// oracle value. DEFAULT solver (expected-fail rule).
    #[kani::proof]
    #[kani::stub(typcache_seams::compare_values_of_enum::call, stub_compare_values_of_enum)]
    #[kani::stub(syscache_seams::lookup_pg_enum_by_oid::call, stub_lookup_pg_enum_by_oid)]
    #[kani::stub(types_error::PgError::new, stubs::stub_pg_error_new)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn control_enum_cmp_oracle_skew() {
        // Concrete odd OIDs pin the seam arm; only the oracle is symbolic.
        let (a, b) = (17u32, 19u32);
        let cmp: i32 = kani::any();
        kani::assume(cmp >= -1 && cmp <= 1);
        arm_oracles(true, 100_001, cmp);
        reset_traces();

        let mut c_out: c_int = 0;
        let mut c_err: c_int = 0;
        unsafe { pg_enum_cmp(a, b, 1, 100_001, &mut c_out, &mut c_err) };
        // skew: Rust answers from cmp+1
        unsafe { pg_enum_oracle_cmp = cmp + 1 };
        let (r, _) = drive(adt_enum::builtins::fc_enum_cmp, a, b, true, 100_001);
        match r {
            Ok(d) => assert!(d.as_i32() == c_out), // expected failure
            Err(e) => {
                core::mem::forget(e);
                panic!("memoized cmp cannot error");
            }
        }
    }

    /// MUST FAIL (lookup-typeoid seam feeds the cmp seam): C resolves the
    /// cold-arm typeoid from a skewed lookup oracle; trace parity must
    /// catch the wrong-typeoid propagation. DEFAULT solver.
    #[kani::proof]
    #[kani::stub(typcache_seams::compare_values_of_enum::call, stub_compare_values_of_enum)]
    #[kani::stub(syscache_seams::lookup_pg_enum_by_oid::call, stub_lookup_pg_enum_by_oid)]
    #[kani::stub(types_error::PgError::new, stubs::stub_pg_error_new)]
    #[kani::stub(std::fmt::format, stubs::stub_format)]
    fn control_enum_lookup_typeoid_skew() {
        let (a, b) = (17u32, 19u32);
        let typeoid: Oid = kani::any();
        kani::assume(typeoid < Oid::MAX);
        arm_oracles(true, typeoid, 1);
        reset_traces();

        let mut c_out: c_int = 0;
        let mut c_err: c_int = 0;
        unsafe { pg_enum_cmp(a, b, 0, 0, &mut c_out, &mut c_err) };
        // skew the lookup answer for the Rust side
        unsafe { pg_enum_oracle_lookup_typeoid = typeoid + 1 };
        let (r, memo_after) = drive(adt_enum::builtins::fc_enum_cmp, a, b, false, 0);
        let _ = r.map_err(core::mem::forget);
        assert_seam_parity(false, 0, memo_after); // expected failure (trace typeoid)
    }
}
