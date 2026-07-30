//! Kani C≡Rust equivalence: array HEADER-READ builtins (no element access,
//! no typcache) — oids 747 array_dims, 748 array_ndims, 2091 array_lower,
//! 2092 array_upper, 2176 array_length, 3179 array_cardinality.
//!
//! Rust side (shipped code, path-dep — never copied): the fc_* WRAPPERS
//! themselves — arrayfuncs::ops::{fc_array_ndims, fc_array_lower,
//! fc_array_upper, fc_array_cardinality, fc_array_dims} and
//! arrayfuncs::builtins::fc_array_length — called through a real
//! LocalFcinfo frame (proof_support::fcinfo), so the datum unwrap, the
//! detoast call, read_dims_lbounds, the null-verdict conditionals and the
//! Datum pack are ALL inside the theorem.
//!
//! C side: proofs/arrayfuncs-hdr/c/pg_arrayhdr.c (REL_18_STABLE
//! arrayfuncs.c/arrayutils.c, provenance + shims documented there).
//!
//! Harness plane and fences (each recorded in the ledger row):
//!  - FLAT PRE-DETOASTED IMAGES: the datum arg points at a plain 4B-header
//!    flat array image (CAP = 64 bytes = 16B header + 6 dims + 6 lbounds;
//!    elements are never read by any function under proof, so none are
//!    materialized; trailing unused lanes are LITERAL zero — dead-symbolic-
//!    bytes trap). The detoast seam is installed as an identity byte-copy
//!    (sound for plain images, which real detoast returns unchanged;
//!    toasted/compressed inputs are out of proof).
//!  - ndim FENCE 0..=MAXDIM on every row except array_ndims (full-i32
//!    ndim): shipped read_dims_lbounds loops `0..ndim as usize` BEFORE the
//!    wrapper's sanity check, so ndim < 0 or ndim > 6 (corruption-plane
//!    images, unreachable from any constructed array) panics in Rust where
//!    C returns NULL/garbage — recorded as a corruption-plane divergence
//!    CANDIDATE (witness: tests/corruption_plane.rs), not provable and not
//!    ruled here. array_ndims reads only the ndim field and IS proved over
//!    the full i32 ndim plane including both sanity-check arms.
//!  - array_upper/array_dims ub FENCE: dims[i] + lb[i] - 1 stays in i32
//!    (both the sum and sum-1) — the in-contract plane; C wraps under
//!    -fwrapv, Rust release wraps identically, but the wrap plane is
//!    unreachable for real arrays (ArrayCheckBounds enforces it at
//!    construction) and Kani's overflow check would flag the Rust arm.
//!  - Allocator: proof_support mcx-stubs recipe ("modulo static-buffer
//!    allocator model"); PgError message text out of proof (value-space
//!    only, sqlstate/level parity asserted on the Err arm).
//!  - array_dims non-null arm renders text through core::fmt (shipped
//!    dims_text write!) — std fmt machinery walls symex, so the SYMBOLIC
//!    array_dims harness fences to the null-verdict plane (literal ndim=0
//!    image) and the value plane stands on tests/native_diff.rs
//!    (tested(differential) against the same vendored C).
//!
//! Run (one at a time, RSS-watchdogged; kissat for expected-green):
//!   timeout 450 cargo kani -Z c-ffi -Z stubbing --c-lib c/pg_arrayhdr.c \
//!     --harness <h> --exact --solver kissat

use std::os::raw::c_int;

extern "C" {
    fn pg_array_ndims(v: *const u8, isnull: *mut c_int) -> i32;
    fn pg_array_lower(v: *const u8, reqdim: i32, isnull: *mut c_int) -> i32;
    fn pg_array_upper(v: *const u8, reqdim: i32, isnull: *mut c_int) -> i32;
    fn pg_array_length(v: *const u8, reqdim: i32, isnull: *mut c_int) -> i32;
    fn pg_array_cardinality(v: *const u8, err: *mut c_int) -> i32;
    fn pg_array_dims(v: *const u8, isnull: *mut c_int, out: *mut u8) -> i32;
}

pub const MAXDIM: usize = 6;
/// 16B header + MAXDIM dims + MAXDIM lbounds; elements never read.
pub const CAP: usize = 16 + 4 * MAXDIM + 4 * MAXDIM;

/// Build a flat 4B-header array image: literal varlena size (CAP), symbolic
/// ndim, dataoffset 0 (no null bitmap), literal elemtype 23, then the
/// PACKED on-disk layout — dims[0..ndim] immediately after the header and
/// lbs[0..ndim] immediately after the dims (ARR_LBOUND = base + 16 + 4*ndim,
/// ndim-dependent!). Unused trailing bytes stay literal zero.
///
/// HISTORY: v1 wrote lbs at the FIXED offset 16+4*MAXDIM; both sides read
/// 16+4*ndim so parity still held over consistent bytes, but for ndim < 6
/// the lbounds actually read were literal zeros — the fences bound values
/// nothing read (eq_array_upper's "fence-excluded" overflow was exactly
/// dims[0]=i32::MIN with read-lb 0), and the proven lbs plane was narrower
/// than the ledger would claim. Packed layout restores the intended plane.
pub fn mk_image(ndim: i32, dims: &[i32; MAXDIM], lbs: &[i32; MAXDIM]) -> [u8; CAP] {
    let mut img = [0u8; CAP];
    img[0..4].copy_from_slice(&datum::varlena::set_varsize_4b(CAP));
    img[4..8].copy_from_slice(&ndim.to_ne_bytes());
    // dataoffset = 0 (no nulls); elemtype = 23 (int4, literal; never read
    // by the functions under proof)
    img[12..16].copy_from_slice(&23u32.to_ne_bytes());
    if ndim > 0 && ndim <= MAXDIM as i32 {
        let n = ndim as usize;
        for i in 0..n {
            let d = 16 + 4 * i;
            img[d..d + 4].copy_from_slice(&dims[i].to_ne_bytes());
            let l = 16 + 4 * n + 4 * i;
            img[l..l + 4].copy_from_slice(&lbs[i].to_ne_bytes());
        }
    }
    img
}

#[cfg(kani)]
mod proofs {
    use super::*;
    use proof_support::{mcx_stubs, stubs};
    use types_error::{ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR};

    /// Identity detoast for plain (non-toasted) images: byte-copy into a
    /// PgVec, exactly what real detoast yields for a plain 4B-header
    /// varlena. Sound only on the flat pre-detoasted plane (module doc).
    fn proof_detoast<'m>(
        mcx: mcx::Mcx<'m>,
        image: &[u8],
    ) -> types_error::PgResult<mcx::PgVec<'m, u8>> {
        let mut v = mcx::vec_with_capacity_in(mcx, image.len())?;
        mcx::vec_append_bytes(&mut v, image)?;
        Ok(v)
    }

    /// One shipped-wrapper call: arm the frame, run, return
    /// (rust_isnull, rust_value_datum). Err arm is statically dead for the
    /// header readers (allocation is stubbed infallible) except
    /// cardinality, which adjudicates it in its own harness.
    fn call_wrapper(
        fc: fn(
            Option<&mut types_fmgr::FmgrInfo>,
            &mut types_fmgr::FunctionCallInfoBaseData,
        ) -> types_error::PgResult<datum::Datum>,
        args: &[datum::Datum],
        ctx: &mcx::MemoryContext,
    ) -> types_error::PgResult<(bool, datum::Datum)> {
        // real 2-arg frame (1-arg callers just ignore slot 1)
        let mut f = proof_support::fcinfo::fci([
            args[0],
            if args.len() > 1 { args[1] } else { datum::Datum::from_i32(0) },
        ]);
        // SAFETY(harness): ctx outlives the call; forgotten at harness end.
        unsafe { f.set_result_mcx(ctx.mcx()) };
        let r = fc(None, &mut f)?;
        Ok((f.isnull, r))
    }

    fn install_detoast() {
        detoast_seams::detoast_attr::set(proof_detoast);
    }

    // unwind: read_dims_lbounds <= 6 iterations + mk_image 6-lane loop +
    // slack for mcx registry walks; the 64-byte detoast copy is a
    // copy_nonoverlapping (CBMC memcpy builtin at concrete length).
    macro_rules! recipe {
        ($(#[$m:meta])* fn $name:ident() $body:block) => {
            #[kani::proof]
            #[kani::unwind(10)]
            #[kani::stub(mcx::Mcx::allocate, mcx_stubs::stub_mcx_allocate)]
            #[kani::stub(mcx::Mcx::grow, mcx_stubs::stub_mcx_grow)]
            #[kani::stub(mcx::Mcx::deallocate, mcx_stubs::stub_mcx_deallocate)]
            #[kani::stub(std::env::var, stubs::stub_env_var_zero)]
            #[kani::stub(std::sync::OnceLock::get_or_init, stubs::stub_once_lock_get_or_init)]
            #[kani::stub(types_error::PgError::error, stubs::stub_pg_error_error)]
            #[kani::stub(std::fmt::format, stubs::stub_format)]
            $(#[$m])*
            fn $name() $body
        };
    }

    /// Symbolic in-contract array header: ndim 0..=6, symbolic dims/lbs in
    /// the live lanes, literal zeros beyond (dead-symbolic-bytes trap).
    fn sym_header() -> (i32, [i32; MAXDIM], [i32; MAXDIM]) {
        let ndim: i32 = kani::any();
        kani::assume(ndim >= 0 && ndim <= MAXDIM as i32);
        let mut dims = [0i32; MAXDIM];
        let mut lbs = [0i32; MAXDIM];
        for i in 0..MAXDIM {
            if (i as i32) < ndim {
                dims[i] = kani::any();
                lbs[i] = kani::any();
            }
        }
        (ndim, dims, lbs)
    }

    recipe! {
        /// oid 748 array_ndims — FULL-i32 ndim plane (only the ndim field
        /// is read before the sanity check), both sanity-check arms.
        fn eq_array_ndims() {
            install_detoast();
            let ndim: i32 = kani::any();
            let img = mk_image(ndim, &[0; MAXDIM], &[0; MAXDIM]);
            let mut c_null: c_int = 0;
            let c = unsafe { pg_array_ndims(img.as_ptr(), &mut c_null) };
            let ctx = mcx::MemoryContext::new_bump("kani-arrhdr");
            let d = datum::Datum::from_usize(img.as_ptr() as usize);
            match call_wrapper(arrayfuncs::ops::fc_array_ndims, &[d], &ctx) {
                Ok((rnull, rv)) => {
                    assert!(rnull == (c_null == 1));
                    if !rnull {
                        assert!(rv.as_i32() == c);
                    }
                }
                Err(e) => {
                    core::mem::forget(e);
                    panic!("fc_array_ndims errored");
                }
            }
            kani::cover!(c_null == 1);
            kani::cover!(c_null == 0);
            core::mem::forget(ctx);
        }
    }

    recipe! {
        /// oid 2091 array_lower — ndim fence 0..=6, full-i32 reqdim,
        /// symbolic lbounds.
        fn eq_array_lower() {
            install_detoast();
            let (ndim, dims, lbs) = sym_header();
            let reqdim: i32 = kani::any();
            let img = mk_image(ndim, &dims, &lbs);
            let mut c_null: c_int = 0;
            let c = unsafe { pg_array_lower(img.as_ptr(), reqdim, &mut c_null) };
            let ctx = mcx::MemoryContext::new_bump("kani-arrhdr");
            let d = datum::Datum::from_usize(img.as_ptr() as usize);
            match call_wrapper(
                arrayfuncs::ops::fc_array_lower,
                &[d, datum::Datum::from_i32(reqdim)],
                &ctx,
            ) {
                Ok((rnull, rv)) => {
                    assert!(rnull == (c_null == 1));
                    if !rnull {
                        assert!(rv.as_i32() == c);
                    }
                }
                Err(e) => {
                    core::mem::forget(e);
                    panic!("fc_array_lower errored");
                }
            }
            kani::cover!(c_null == 1);
            kani::cover!(c_null == 0);
            core::mem::forget(ctx);
        }
    }

    recipe! {
        /// oid 2092 array_upper — ndim fence 0..=6, full-i32 reqdim,
        /// ub-overflow fence (module doc).
        fn eq_array_upper() {
            install_detoast();
            let (ndim, dims, lbs) = sym_header();
            for i in 0..MAXDIM {
                let s = dims[i] as i64 + lbs[i] as i64;
                kani::assume(s >= i32::MIN as i64 && s <= i32::MAX as i64);
                kani::assume(s - 1 >= i32::MIN as i64);
            }
            let reqdim: i32 = kani::any();
            let img = mk_image(ndim, &dims, &lbs);
            let mut c_null: c_int = 0;
            let c = unsafe { pg_array_upper(img.as_ptr(), reqdim, &mut c_null) };
            let ctx = mcx::MemoryContext::new_bump("kani-arrhdr");
            let d = datum::Datum::from_usize(img.as_ptr() as usize);
            match call_wrapper(
                arrayfuncs::ops::fc_array_upper,
                &[d, datum::Datum::from_i32(reqdim)],
                &ctx,
            ) {
                Ok((rnull, rv)) => {
                    assert!(rnull == (c_null == 1));
                    if !rnull {
                        assert!(rv.as_i32() == c);
                    }
                }
                Err(e) => {
                    core::mem::forget(e);
                    panic!("fc_array_upper errored");
                }
            }
            kani::cover!(c_null == 1);
            kani::cover!(c_null == 0);
            core::mem::forget(ctx);
        }
    }

    recipe! {
        /// oid 2176 array_length — ndim fence 0..=6, full-i32 reqdim.
        fn eq_array_length() {
            install_detoast();
            let (ndim, dims, lbs) = sym_header();
            let reqdim: i32 = kani::any();
            let img = mk_image(ndim, &dims, &lbs);
            let mut c_null: c_int = 0;
            let c = unsafe { pg_array_length(img.as_ptr(), reqdim, &mut c_null) };
            let ctx = mcx::MemoryContext::new_bump("kani-arrhdr");
            let d = datum::Datum::from_usize(img.as_ptr() as usize);
            match call_wrapper(
                arrayfuncs::builtins::fc_array_length,
                &[d, datum::Datum::from_i32(reqdim)],
                &ctx,
            ) {
                Ok((rnull, rv)) => {
                    assert!(rnull == (c_null == 1));
                    if !rnull {
                        assert!(rv.as_i32() == c);
                    }
                }
                Err(e) => {
                    core::mem::forget(e);
                    panic!("fc_array_length errored");
                }
            }
            kani::cover!(c_null == 1);
            kani::cover!(c_null == 0);
            core::mem::forget(ctx);
        }
    }

    recipe! {
        /// oid 3179 array_cardinality — ndim fence 0..=6, FULL-i32 dims
        /// (negative dims + overflow reach the 54000 error arm on both
        /// sides); Ok-value + Err verdict/sqlstate/level parity.
        fn eq_array_cardinality() {
            install_detoast();
            let (ndim, dims, lbs) = sym_header();
            let img = mk_image(ndim, &dims, &lbs);
            let mut c_err: c_int = 0;
            let c = unsafe { pg_array_cardinality(img.as_ptr(), &mut c_err) };
            let ctx = mcx::MemoryContext::new_bump("kani-arrhdr");
            let d = datum::Datum::from_usize(img.as_ptr() as usize);
            match call_wrapper(arrayfuncs::ops::fc_array_cardinality, &[d], &ctx) {
                Ok((rnull, rv)) => {
                    assert!(c_err == 0);
                    assert!(!rnull);
                    assert!(rv.as_i32() == c);
                }
                Err(e) => {
                    assert!(c_err == 1);
                    assert!(e.sqlstate == ERRCODE_PROGRAM_LIMIT_EXCEEDED);
                    assert!(e.level == ERROR);
                    core::mem::forget(e);
                }
            }
            kani::cover!(c_err == 0);
            kani::cover!(c_err == 1);
            core::mem::forget(ctx);
        }
    }

    recipe! {
        /// oid 747 array_dims — NULL-VERDICT plane only (LITERAL ndim=0
        /// image: the non-null arm renders text via core::fmt, which walls
        /// symex — module doc; value plane covered by tests/native_diff.rs).
        fn eq_array_dims_nullplane() {
            install_detoast();
            let img = mk_image(0, &[0; MAXDIM], &[0; MAXDIM]);
            let mut c_null: c_int = 0;
            let mut c_out = [0u8; 6 * 33 + 1];
            let c = unsafe { pg_array_dims(img.as_ptr(), &mut c_null, c_out.as_mut_ptr()) };
            assert!(c_null == 1 && c == 0);
            let ctx = mcx::MemoryContext::new_bump("kani-arrhdr");
            let d = datum::Datum::from_usize(img.as_ptr() as usize);
            match call_wrapper(arrayfuncs::ops::fc_array_dims, &[d], &ctx) {
                Ok((rnull, _rv)) => assert!(rnull),
                Err(e) => {
                    core::mem::forget(e);
                    panic!("fc_array_dims errored");
                }
            }
            core::mem::forget(ctx);
        }
    }

    recipe! {
        /// NEGATIVE CONTROL (family gate non-vacuity): C sees reqdim,
        /// shipped Rust sees reqdim+1 — MUST FAIL with a value
        /// counterexample. Run with the DEFAULT solver (kissat never
        /// terminates on failing harnesses).
        fn control_array_lower_reqdim_skew_must_fail() {
            install_detoast();
            let (ndim, dims, lbs) = sym_header();
            kani::assume(ndim >= 2); // both reqdim and reqdim+1 in range
            let reqdim: i32 = 1;
            let img = mk_image(ndim, &dims, &lbs);
            let mut c_null: c_int = 0;
            let c = unsafe { pg_array_lower(img.as_ptr(), reqdim, &mut c_null) };
            let ctx = mcx::MemoryContext::new_bump("kani-arrhdr");
            let d = datum::Datum::from_usize(img.as_ptr() as usize);
            match call_wrapper(
                arrayfuncs::ops::fc_array_lower,
                &[d, datum::Datum::from_i32(reqdim + 1)],
                &ctx,
            ) {
                Ok((rnull, rv)) => {
                    assert!(!rnull && c_null == 0);
                    assert!(rv.as_i32() == c, "skew control: values must diverge");
                }
                Err(e) => {
                    core::mem::forget(e);
                    panic!("fc_array_lower errored");
                }
            }
            core::mem::forget(ctx);
        }
    }
}

