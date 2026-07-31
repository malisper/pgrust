//! Kani C-equivalence harnesses: pgrust `arrayutils` (shipped crate at
//! crates/backend/utils/adt/arrayutils) vs vendored PostgreSQL 18.3 C
//! (c_arrayutils.c, compiled via `-Z c-ffi --c-lib`). 100%-coverage
//! campaign, lane p1-laneh.
//!
//! Fences (n <= 6 = MAXDIM everywhere — the shipped constant; C reads
//! exactly n elements). The "validated caller" functions
//! (array_get_offset, mda_*) additionally fence input magnitudes exactly
//! where C signed-overflow UB would begin — the assumes mirror the C
//! header comments ("caller has already range-checked", "overflow is
//! impossible"), so the proof domain is the C contract domain.
//! array_get_n_items and array_check_bounds run FULL-RANGE i32 inputs
//! (their C bodies are overflow-checked by construction).
//!
//! Error plane: the C shim records errcode class 1 (= 54000
//! ERRCODE_PROGRAM_LIMIT_EXCEEDED, the only code arrayutils.c raises) in
//! a proof-side channel; harnesses assert Rust Err/sqlstate agreement.

#![allow(dead_code)]

// All entries cross the FFI through c_ wrappers (c_arrayutils.c): goto-cc
// rejects cross-language declarations naming struct Node, and the wrappers
// pin escontext = NULL (hard-error shape) + reset the errcode channel.
#[cfg(kani)]
mod ffi {
    extern "C" {
        pub fn c_array_get_offset(
            n: i32,
            dim: *const i32,
            lb: *const i32,
            indx: *const i32,
        ) -> i32;
        pub fn c_array_get_n_items(ndim: i32, dims: *const i32) -> i32;
        pub fn c_array_check_bounds(ndim: i32, dims: *const i32, lb: *const i32) -> i32;
        pub fn c_mda_get_range(n: i32, span: *mut i32, st: *const i32, endp: *const i32) -> i32;
        pub fn c_mda_get_prod(n: i32, range: *const i32, prod: *mut i32) -> i32;
        pub fn c_mda_get_offset_values(
            n: i32,
            dist: *mut i32,
            prod: *const i32,
            span: *const i32,
        ) -> i32;
        pub fn c_mda_next_tuple(n: i32, curr: *mut i32, span: *const i32) -> i32;
        pub fn c_errcode_read() -> i32;
    }
}

#[cfg(kani)]
mod harnesses {
    use super::ffi;
    use arrayutils::{
        array_check_bounds, array_get_n_items, array_get_offset, mda_get_offset_values,
        mda_get_prod, mda_get_range, mda_next_tuple,
    };
    use types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED;

    const MAXDIM: usize = 6;

    /// Typed per-element staging (CBMC byte-pun law: never memcpy symbolic
    /// multi-byte values).
    fn sym_arr() -> [i32; MAXDIM] {
        [
            kani::any(),
            kani::any(),
            kani::any(),
            kani::any(),
            kani::any(),
            kani::any(),
        ]
    }

    fn sym_n() -> i32 {
        let n: i32 = kani::any();
        kani::assume((0..=MAXDIM as i32).contains(&n));
        n
    }

    #[kani::proof]
    #[kani::unwind(8)]
    fn eq_array_get_n_items() {
        // FULL-RANGE dims; ndim includes the <=0 arms.
        let ndim: i32 = kani::any();
        kani::assume(ndim <= MAXDIM as i32); // Rust deviation fence: ndim <= len
        let dims = sym_arr();
        let cv = unsafe { ffi::c_array_get_n_items(ndim, dims.as_ptr()) };
        let cerr = unsafe { ffi::c_errcode_read() };
        match array_get_n_items(ndim, &dims) {
            Ok(v) => {
                assert_eq!(v, cv);
                assert_eq!(cerr, 0);
            }
            Err(e) => {
                assert_eq!(cv, -1);
                assert_eq!(cerr, 1);
                assert!(e.sqlstate() == ERRCODE_PROGRAM_LIMIT_EXCEEDED);
            }
        }
    }

    /// Rust-only corrupt-header arm: claimed ndim wider than the slice
    /// ALWAYS errors (C counterpart reads out of bounds - UB, no oracle).
    #[kani::proof]
    #[kani::unwind(8)]
    fn ndim_wider_than_slice_always_errors() {
        let dims = sym_arr();
        let len: usize = kani::any();
        kani::assume(len < MAXDIM);
        let ndim: i32 = kani::any();
        kani::assume(ndim > len as i32 && ndim <= MAXDIM as i32);
        let e = array_get_n_items(ndim, &dims[..len]).unwrap_err();
        assert!(e.sqlstate() == ERRCODE_PROGRAM_LIMIT_EXCEEDED);
    }

    #[kani::proof]
    #[kani::unwind(8)]
    fn eq_array_check_bounds() {
        // FULL-RANGE dims/lb.
        let n = sym_n();
        let dims = sym_arr();
        let lb = sym_arr();
        let cv = unsafe { ffi::c_array_check_bounds(n, dims.as_ptr(), lb.as_ptr()) };
        let cerr = unsafe { ffi::c_errcode_read() };
        match array_check_bounds(n, &dims[..n as usize], &lb[..n as usize]) {
            Ok(()) => {
                assert_eq!(cv, 1);
                assert_eq!(cerr, 0);
            }
            Err(e) => {
                assert_eq!(cv, 0);
                assert_eq!(cerr, 1);
                assert!(e.sqlstate() == ERRCODE_PROGRAM_LIMIT_EXCEEDED);
            }
        }
    }

    #[kani::proof]
    #[kani::unwind(8)]
    fn eq_array_get_offset() {
        // C contract domain: dims positive-small, subscript deltas small
        // (the exact fuzz fold: dim in [1,16], lb/indx in [-64,63]).
        let n = sym_n();
        let dim = sym_arr();
        let lb = sym_arr();
        let indx = sym_arr();
        for i in 0..MAXDIM {
            kani::assume(dim[i] >= 1 && dim[i] <= 16);
            kani::assume(lb[i] >= -64 && lb[i] <= 63);
            kani::assume(indx[i] >= -64 && indx[i] <= 63);
        }
        let v = array_get_offset(n, &dim, &lb, &indx);
        let cv = unsafe { ffi::c_array_get_offset(n, dim.as_ptr(), lb.as_ptr(), indx.as_ptr()) };
        assert_eq!(v, cv);
    }

    #[kani::proof]
    #[kani::unwind(8)]
    fn eq_mda_get_range() {
        // i16-folded endpoints (no i32 overflow in endp-st+1).
        let n = sym_n();
        let mut st = sym_arr();
        let mut endp = sym_arr();
        for i in 0..MAXDIM {
            kani::assume(st[i] >= i16::MIN as i32 && st[i] <= i16::MAX as i32);
            kani::assume(endp[i] >= i16::MIN as i32 && endp[i] <= i16::MAX as i32);
        }
        let _ = &mut st;
        let _ = &mut endp;
        let mut rspan = [0i32; MAXDIM];
        let mut cspan = [0i32; MAXDIM];
        mda_get_range(n, &mut rspan, &st, &endp);
        let _ = unsafe { ffi::c_mda_get_range(n, cspan.as_mut_ptr(), st.as_ptr(), endp.as_ptr()) };
        assert_eq!(rspan, cspan);
    }

    #[kani::proof]
    #[kani::unwind(8)]
    fn eq_mda_get_prod() {
        let n: i32 = kani::any();
        kani::assume((1..=MAXDIM as i32).contains(&n)); // C writes prod[n-1]
        let range = sym_arr();
        for i in 0..MAXDIM {
            kani::assume(range[i] >= 1 && range[i] <= 16);
        }
        let mut rprod = [0i32; MAXDIM];
        let mut cprod = [0i32; MAXDIM];
        mda_get_prod(n, &range, &mut rprod);
        let _ = unsafe { ffi::c_mda_get_prod(n, range.as_ptr(), cprod.as_mut_ptr()) };
        assert_eq!(rprod, cprod);
    }

    #[kani::proof]
    #[kani::unwind(8)]
    fn eq_mda_get_offset_values() {
        let n: i32 = kani::any();
        kani::assume((1..=MAXDIM as i32).contains(&n));
        let prod = sym_arr();
        let span = sym_arr();
        for i in 0..MAXDIM {
            kani::assume(prod[i] >= 1 && prod[i] <= 16);
            kani::assume(span[i] >= 1 && span[i] <= 16);
        }
        let mut rdist = [0i32; MAXDIM];
        let mut cdist = [0i32; MAXDIM];
        mda_get_offset_values(n, &mut rdist, &prod, &span);
        let _ = unsafe {
            ffi::c_mda_get_offset_values(n, cdist.as_mut_ptr(), prod.as_ptr(), span.as_ptr())
        };
        assert_eq!(rdist, cdist);
    }

    #[kani::proof]
    #[kani::unwind(8)]
    fn eq_mda_next_tuple() {
        // C precondition: span >= 1, curr a valid tuple under span.
        let n = sym_n(); // n = 0 probes the n<=0 early return
        let span = sym_arr();
        let curr0 = sym_arr();
        for i in 0..MAXDIM {
            kani::assume(span[i] >= 1 && span[i] <= 16);
            kani::assume(curr0[i] >= 0 && curr0[i] < span[i]);
        }
        let mut rcurr = curr0;
        let mut ccurr = curr0;
        let rv = mda_next_tuple(n, &mut rcurr, &span);
        let cv = unsafe { ffi::c_mda_next_tuple(n, ccurr.as_mut_ptr(), span.as_ptr()) };
        assert_eq!(rv, cv);
        assert_eq!(rcurr, ccurr);
    }

    /// Must-fail negative control (family non-vacuity), on the INTENDED
    /// assert: a wrong-value claim against the C offset.
    #[kani::proof]
    #[kani::unwind(8)]
    fn control_get_offset_wrong_value() {
        let dim = [3i32, 4, 5, 2, 3, 2];
        let lb = [1i32; 6];
        let indx: [i32; 6] = [
            kani::any(),
            kani::any(),
            kani::any(),
            kani::any(),
            kani::any(),
            kani::any(),
        ];
        for i in 0..6 {
            kani::assume(indx[i] >= lb[i] && indx[i] < lb[i] + dim[i]);
        }
        let cv = unsafe { ffi::c_array_get_offset(6, dim.as_ptr(), lb.as_ptr(), indx.as_ptr()) };
        assert!(
            array_get_offset(6, &dim, &lb, &indx) == cv + 1,
            "INTENDED-FAIL: off-by-one claim"
        );
    }
}
