//! adt-misc proof family (wave-2, 3 pg_proc rows): pg_num_nulls (438) /
//! pg_num_nonnulls (440) / any_value_transfn (6292).
//!
//! C side: c/pg_adt_misc.c — verbatim REL_18_STABLE misc.c bodies; see its
//! header for provenance and the shim manifest.
//!
//! Rust side (SHIPPED code): adt_misc::{fc_pg_num_nulls, fc_pg_num_nonnulls,
//! fc_any_value_transfn} — WRAPPER-LEVEL over real LocalFcinfo<N> frames
//! with SYMBOLIC per-arg null flags, so the argisnull walk and the Datum
//! pack are inside the theorem.
//!
//! Plane fences (documented, deliberate):
//!  - num_nulls/num_nonnulls: SEPARATE-ARGUMENTS surface only (flinfo=None
//!    => get_fn_expr_variadic false on both sides; the C VARIADIC-array arm
//!    is trap-fenced 99). The variadic array arm (detoast + nullbitmap) is
//!    a distinct future surface, recorded in the ledger rows.
//!  - any_value_transfn: non-null-arg plane (the SQL surface is a strict
//!    transfn — fmgr skips null inputs before the C body runs; shipped
//!    Rust's isnull passthrough is fmgr-surface behavior outside the C
//!    body). Value passthrough incl. arg1-ignored is in-theorem.
//!
//! Frames at N=0/1/4 literal arities (LocalFcinfo is const-generic; the
//! count loop's circuit is identical per-N, N=4 carries the symbolic-flag
//! walk). One MUST-FAIL cross-wiring control (nulls vs C nonnulls).

use std::os::raw::c_int;

extern "C" {
    pub fn pg_num_nulls(
        variadic: c_int, nargs: c_int, argnull: *const c_int, out: *mut i32, isnull: *mut c_int,
    ) -> c_int;
    pub fn pg_num_nonnulls(
        variadic: c_int, nargs: c_int, argnull: *const c_int, out: *mut i32, isnull: *mut c_int,
    ) -> c_int;
    pub fn pg_any_value_transfn(arg0: u64, out: *mut u64) -> c_int;
}

#[cfg(kani)]
mod proofs {
    use super::*;

    use datum::{Datum, NullableDatum};
    use proof_support::fcinfo::FcFn;
    use types_error::PgError;
    use types_fmgr::LocalFcinfo;

    type CFn = unsafe extern "C" fn(c_int, c_int, *const c_int, *mut i32, *mut c_int) -> c_int;

    /// One N-arity cell: symbolic per-arg null flags + symbolic datums.
    fn drive_counts<const N: usize>(fc: FcFn<Box<PgError>>, cfn: CFn) {
        let mut flags = [0 as c_int; N];
        let mut f = LocalFcinfo::<N>::new(0);
        for i in 0..N {
            let isnull: bool = kani::any();
            flags[i] = isnull as c_int;
            f.args[i] = if isnull {
                NullableDatum::null()
            } else {
                NullableDatum::value(Datum::from_u64(kani::any()))
            };
        }
        let mut c_out: i32 = -1;
        let mut c_isnull: c_int = 0;
        let trap = unsafe {
            cfn(0, N as c_int, flags.as_ptr(), &mut c_out, &mut c_isnull)
        };
        assert!(trap != 99, "variadic plane violation");
        match fc(None, &mut f) {
            Ok(d) => {
                assert!(f.isnull as c_int == c_isnull);
                if !f.isnull {
                    assert!(d.as_i32() == c_out);
                }
            }
            Err(e) => {
                core::mem::forget(e);
                panic!("separate-arguments arm cannot error");
            }
        }
    }

    #[kani::proof]
    fn eq_pg_num_nulls_n4() {
        drive_counts::<4>(adt_misc::fc_pg_num_nulls, pg_num_nulls);
    }

    #[kani::proof]
    fn eq_pg_num_nulls_n1() {
        drive_counts::<1>(adt_misc::fc_pg_num_nulls, pg_num_nulls);
    }

    #[kani::proof]
    fn eq_pg_num_nulls_n0() {
        drive_counts::<0>(adt_misc::fc_pg_num_nulls, pg_num_nulls);
    }

    #[kani::proof]
    fn eq_pg_num_nonnulls_n4() {
        drive_counts::<4>(adt_misc::fc_pg_num_nonnulls, pg_num_nonnulls);
    }

    #[kani::proof]
    fn eq_pg_num_nonnulls_n1() {
        drive_counts::<1>(adt_misc::fc_pg_num_nonnulls, pg_num_nonnulls);
    }

    #[kani::proof]
    fn eq_pg_num_nonnulls_n0() {
        drive_counts::<0>(adt_misc::fc_pg_num_nonnulls, pg_num_nonnulls);
    }

    /// MUST FAIL (rig non-vacuity): Rust num_nulls wired against C
    /// num_NONnulls at N=4. DEFAULT solver (expected-fail rule).
    #[kani::proof]
    fn control_num_nulls_vs_c_nonnulls() {
        drive_counts::<4>(adt_misc::fc_pg_num_nulls, pg_num_nonnulls);
    }

    /// 6292 any_value_transfn: value passthrough on the non-null-arg plane
    /// (strict-transfn surface; see module doc), arg1 symbolic + ignored.
    #[kani::proof]
    fn eq_any_value_transfn() {
        let v0: u64 = kani::any();
        let v1: u64 = kani::any();
        let mut f = LocalFcinfo::<2>::new(0);
        f.args[0] = NullableDatum::value(Datum::from_u64(v0));
        let arg1_null: bool = kani::any();
        f.args[1] = if arg1_null {
            NullableDatum::null()
        } else {
            NullableDatum::value(Datum::from_u64(v1))
        };
        let mut c_out: u64 = 0;
        unsafe { pg_any_value_transfn(v0, &mut c_out) };
        match adt_misc::fc_any_value_transfn(None, &mut f) {
            Ok(d) => {
                assert!(!f.isnull);
                assert!(d.as_u64() == c_out);
            }
            Err(e) => {
                core::mem::forget(e);
                panic!("passthrough cannot error");
            }
        }
    }
}
