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
//!  - ndim: NO FENCE — the full i32 plane, as the union of the in-range
//!    literal cells (ndim in {0,1,2,6}, symbolic dims/lbounds) and the
//!    *_ndim_corrupt cells (symbolic ndim with `ndim as u32 > MAXDIM`, i.e.
//!    both ndim < 0 and ndim > MAXDIM, over a full symbolic 6-dim body).
//!    HISTORY: this WAS fenced to 0..=MAXDIM, because shipped
//!    read_dims_lbounds looped `0..ndim as usize` BEFORE the wrapper's
//!    sanity check and panicked (dims[6] on a [i32; 6]; a ~2^64 range for
//!    negative ndim) where C returns NULL. That was a real divergence, not
//!    a solver limit, and it was FIXED (arrayfuncs: sanity-check ndim
//!    BEFORE the dims read) — so the fence came off and the plane is
//!    proved. tests/corruption_plane.rs is the standing native regression
//!    witness that pgrust does not panic there.
//!    array_cardinality is the asymmetric member (C has no sanity check):
//!    non-positive ndim is dual-executed against C's VALUE 0, while above
//!    MAXDIM C reads dim words past the datum, so there is no C answer —
//!    that cell is a RUST-ONLY defined-error theorem, flagged as such.
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

/// CORRUPTION-PLANE image: all MAXDIM dim words and all MAXDIM lbound words
/// are materialized at the offsets a 6-dimensional array would use, then the
/// header ndim field is stamped with `ndim` VERBATIM — so the header can claim
/// any i32 dimension count while the body carries a full 6-dim payload. This
/// is the shape a corrupt page or a crafted binary-format value has; no array
/// pgrust or C can construct reaches it (ArrayCheckBounds / array_recv cap
/// ndim at MAXDIM).
///
/// Distinct from mk_image, which only fills lanes for a valid ndim: the
/// corruption-plane harnesses need real (symbolic) bytes sitting in the dims
/// area so the theorem is "both sides ignore them and null out", not "both
/// sides read the same zeros".
pub fn mk_corrupt_image(ndim: i32, dims: &[i32; MAXDIM], lbs: &[i32; MAXDIM]) -> [u8; CAP] {
    let mut img = [0u8; CAP];
    img[0..4].copy_from_slice(&datum::varlena::set_varsize_4b(CAP));
    img[12..16].copy_from_slice(&23u32.to_ne_bytes());
    let mut i = 0;
    while i < MAXDIM {
        let d = 16 + 4 * i;
        img[d..d + 4].copy_from_slice(&dims[i].to_ne_bytes());
        let l = 16 + 4 * MAXDIM + 4 * i;
        img[l..l + 4].copy_from_slice(&lbs[i].to_ne_bytes());
        i += 1;
    }
    // stamped LAST so it is unconditionally the raw claimed count
    img[4..8].copy_from_slice(&ndim.to_ne_bytes());
    img
}

/// Header-only image for array_ndims (which reads nothing past the ndim
/// field before its sanity check): no dims/lbs writes at all.
pub fn mk_hdr_image(ndim: i32) -> [u8; CAP] {
    let mut img = [0u8; CAP];
    img[0..4].copy_from_slice(&datum::varlena::set_varsize_4b(CAP));
    img[4..8].copy_from_slice(&ndim.to_ne_bytes());
    img[12..16].copy_from_slice(&23u32.to_ne_bytes());
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
    /// (rust_isnull, rust_value_datum).
    fn call_wrapper(
        fc: fn(
            Option<&mut types_fmgr::FmgrInfo>,
            &mut types_fmgr::FunctionCallInfoBaseData,
        ) -> types_error::PgResult<datum::Datum>,
        a0: datum::Datum,
        a1: datum::Datum,
        ctx: &mcx::MemoryContext,
    ) -> types_error::PgResult<(bool, datum::Datum)> {
        let mut f = proof_support::fcinfo::fci([a0, a1]);
        // SAFETY(harness): ctx outlives the call; forgotten at harness end.
        unsafe { f.set_result_mcx(ctx.mcx()) };
        let r = fc(None, &mut f)?;
        Ok((f.isnull, r))
    }

    fn install_detoast() {
        detoast_seams::detoast_attr::set(proof_detoast);
    }

    /// Symbolic dims/lbs for the first `n` (LITERAL) lanes, literal zeros
    /// beyond — dead-symbolic-bytes trap + literals-prune law: each cell
    /// harness pins ndim so every image offset and loop bound folds.
    fn sym_lanes(n: usize) -> ([i32; MAXDIM], [i32; MAXDIM]) {
        let mut dims = [0i32; MAXDIM];
        let mut lbs = [0i32; MAXDIM];
        let mut i = 0;
        while i < n {
            dims[i] = kani::any();
            lbs[i] = kani::any();
            i += 1;
        }
        (dims, lbs)
    }

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

    recipe! {
        /// oid 748 array_ndims — FULL-i32 ndim plane (only the ndim field
        /// is read before the sanity check), both sanity-check arms.
        fn eq_array_ndims() {
            install_detoast();
            let ndim: i32 = kani::any();
            let img = mk_hdr_image(ndim);
            let mut c_null: c_int = 0;
            let c = unsafe { pg_array_ndims(img.as_ptr(), &mut c_null) };
            let ctx = mcx::MemoryContext::new_bump("kani-arrhdr");
            let d = datum::Datum::from_usize(img.as_ptr() as usize);
            match call_wrapper(arrayfuncs::ops::fc_array_ndims, d, datum::Datum::from_i32(0), &ctx) {
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

    /// ndim-literal cell: array_lower — full-i32 reqdim + symbolic lbounds.
    macro_rules! lower_cell {
        ($name:ident, $n:literal) => {
            recipe! {
                fn $name() {
                    install_detoast();
                    let (dims, lbs) = sym_lanes($n);
                    let reqdim: i32 = kani::any();
                    let img = mk_image($n, &dims, &lbs);
                    let mut c_null: c_int = 0;
                    let c = unsafe { pg_array_lower(img.as_ptr(), reqdim, &mut c_null) };
                    let ctx = mcx::MemoryContext::new_bump("kani-arrhdr");
                    let d = datum::Datum::from_usize(img.as_ptr() as usize);
                    match call_wrapper(
                        arrayfuncs::ops::fc_array_lower,
                        d,
                        datum::Datum::from_i32(reqdim),
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
                    if $n > 0 {
                        kani::cover!(c_null == 0);
                    }
                    core::mem::forget(ctx);
                }
            }
        };
    }
    lower_cell!(eq_array_lower_n0, 0);
    lower_cell!(eq_array_lower_n1, 1);
    lower_cell!(eq_array_lower_n2, 2);
    lower_cell!(eq_array_lower_n6, 6);

    /// ndim-literal cell: array_length — full-i32 reqdim + symbolic dims.
    macro_rules! length_cell {
        ($name:ident, $n:literal) => {
            recipe! {
                fn $name() {
                    install_detoast();
                    let (dims, lbs) = sym_lanes($n);
                    let reqdim: i32 = kani::any();
                    let img = mk_image($n, &dims, &lbs);
                    let mut c_null: c_int = 0;
                    let c = unsafe { pg_array_length(img.as_ptr(), reqdim, &mut c_null) };
                    let ctx = mcx::MemoryContext::new_bump("kani-arrhdr");
                    let d = datum::Datum::from_usize(img.as_ptr() as usize);
                    match call_wrapper(
                        arrayfuncs::builtins::fc_array_length,
                        d,
                        datum::Datum::from_i32(reqdim),
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
                    if $n > 0 {
                        kani::cover!(c_null == 0);
                    }
                    core::mem::forget(ctx);
                }
            }
        };
    }
    length_cell!(eq_array_length_n0, 0);
    length_cell!(eq_array_length_n1, 1);
    length_cell!(eq_array_length_n2, 2);
    length_cell!(eq_array_length_n6, 6);

    /// ndim-literal cell: array_upper — full-i32 reqdim; per-lane ub fence
    /// (dims+lb and dims+lb-1 stay in i32 — module doc).
    macro_rules! upper_cell {
        ($name:ident, $n:literal) => {
            recipe! {
                fn $name() {
                    install_detoast();
                    let (dims, lbs) = sym_lanes($n);
                    let mut i = 0;
                    while i < $n {
                        let s = dims[i] as i64 + lbs[i] as i64;
                        kani::assume(s >= i32::MIN as i64 && s <= i32::MAX as i64);
                        kani::assume(s - 1 >= i32::MIN as i64);
                        i += 1;
                    }
                    let reqdim: i32 = kani::any();
                    let img = mk_image($n, &dims, &lbs);
                    let mut c_null: c_int = 0;
                    let c = unsafe { pg_array_upper(img.as_ptr(), reqdim, &mut c_null) };
                    let ctx = mcx::MemoryContext::new_bump("kani-arrhdr");
                    let d = datum::Datum::from_usize(img.as_ptr() as usize);
                    match call_wrapper(
                        arrayfuncs::ops::fc_array_upper,
                        d,
                        datum::Datum::from_i32(reqdim),
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
                    if $n > 0 {
                        kani::cover!(c_null == 0);
                    }
                    core::mem::forget(ctx);
                }
            }
        };
    }
    upper_cell!(eq_array_upper_n0, 0);
    upper_cell!(eq_array_upper_n1, 1);
    upper_cell!(eq_array_upper_n2, 2);
    upper_cell!(eq_array_upper_n6, 6);

    /// ndim-literal cell: array_cardinality — FULL-i32 dims (negative dims
    /// + i32 overflow + MaxArraySize all reach the 54000 error arm on both
    /// sides); Ok-value + Err verdict/sqlstate/level parity.
    macro_rules! card_cell {
        ($name:ident, $n:literal) => {
            recipe! {
                fn $name() {
                    install_detoast();
                    let (dims, lbs) = sym_lanes($n);
                    let img = mk_image($n, &dims, &lbs);
                    let mut c_err: c_int = 0;
                    let c = unsafe { pg_array_cardinality(img.as_ptr(), &mut c_err) };
                    let ctx = mcx::MemoryContext::new_bump("kani-arrhdr");
                    let d = datum::Datum::from_usize(img.as_ptr() as usize);
                    match call_wrapper(
                        arrayfuncs::ops::fc_array_cardinality,
                        d,
                        datum::Datum::from_i32(0),
                        &ctx,
                    ) {
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
                    if $n > 0 {
                        kani::cover!(c_err == 1);
                    }
                    core::mem::forget(ctx);
                }
            }
        };
    }
    card_cell!(eq_array_cardinality_n0, 0);
    card_cell!(eq_array_cardinality_n1, 1);
    card_cell!(eq_array_cardinality_n2, 2);
    card_cell!(eq_array_cardinality_n6, 6);

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
            match call_wrapper(arrayfuncs::ops::fc_array_dims, d, datum::Datum::from_i32(0), &ctx) {
                Ok((rnull, _rv)) => assert!(rnull),
                Err(e) => {
                    core::mem::forget(e);
                    panic!("fc_array_dims errored");
                }
            }
            core::mem::forget(ctx);
        }
    }

    // ---- CORRUPTION PLANE (ndim outside 0..=MAXDIM) ----
    //
    // The fence these replace was a DIVERGENCE, not a solver limit: shipped
    // read_dims_lbounds looped `0..ndim as usize` on the raw header field
    // before any wrapper's sanity check, so ndim=7 indexed dims[6] on a
    // [i32; 6] and ndim<0 made a ~2^64 range — both panics, where C returns
    // NULL. fix/array-hdr-corruption-plane moved the ndim check ahead of the
    // fill, so the plane is now provable and the fence is gone: `ndim as u32 >
    // MAXDIM` (one compare covering ndim < 0 and ndim > MAXDIM) with a FULL
    // 6-dim symbolic body underneath, union'd with the in-range literal cells
    // above = the full i32 ndim plane.
    //
    // C's check is `AARR_NDIM(v) <= 0 || AARR_NDIM(v) > MAXDIM`, so ndim == 0
    // nulls too but is IN range for the fill; it stays covered by the n0
    // cells, which read a valid (empty) body.
    macro_rules! corrupt_ndim_cell {
        ($name:ident, $c:ident, $rust:path) => {
            recipe! {
                fn $name() {
                    install_detoast();
                    let ndim: i32 = kani::any();
                    kani::assume(ndim as u32 > MAXDIM as u32);
                    let mut dims = [0i32; MAXDIM];
                    let mut lbs = [0i32; MAXDIM];
                    let mut i = 0;
                    while i < MAXDIM {
                        dims[i] = kani::any();
                        lbs[i] = kani::any();
                        i += 1;
                    }
                    let reqdim: i32 = kani::any();
                    let img = mk_corrupt_image(ndim, &dims, &lbs);
                    let mut c_null: c_int = 0;
                    let c = unsafe { $c(img.as_ptr(), reqdim, &mut c_null) };
                    let ctx = mcx::MemoryContext::new_bump("kani-arrhdr");
                    let d = datum::Datum::from_usize(img.as_ptr() as usize);
                    match call_wrapper($rust, d, datum::Datum::from_i32(reqdim), &ctx) {
                        Ok((rnull, rv)) => {
                            assert!(rnull == (c_null == 1));
                            if !rnull {
                                assert!(rv.as_i32() == c);
                            }
                        }
                        Err(e) => {
                            core::mem::forget(e);
                            panic!("header reader errored on the corruption plane");
                        }
                    }
                    // The whole point: C's sanity check fires, so NULL, and the
                    // shipped wrapper agrees instead of panicking.
                    assert!(c_null == 1);
                    core::mem::forget(ctx);
                }
            }
        };
    }
    corrupt_ndim_cell!(
        eq_array_lower_ndim_corrupt,
        pg_array_lower,
        arrayfuncs::ops::fc_array_lower
    );
    corrupt_ndim_cell!(
        eq_array_upper_ndim_corrupt,
        pg_array_upper,
        arrayfuncs::ops::fc_array_upper
    );
    corrupt_ndim_cell!(
        eq_array_length_ndim_corrupt,
        pg_array_length,
        arrayfuncs::builtins::fc_array_length
    );

    recipe! {
        /// oid 748 array_ndims over the corruption plane WITH a full symbolic
        /// 6-dim body: the pre-existing full-i32 cell reads a header-only
        /// image (dims/lbs literal zero), so this is the widening that puts
        /// symbolic bytes in the area the old panic indexed into.
        fn eq_array_ndims_ndim_corrupt() {
            install_detoast();
            let ndim: i32 = kani::any();
            kani::assume(ndim as u32 > MAXDIM as u32);
            let mut dims = [0i32; MAXDIM];
            let mut lbs = [0i32; MAXDIM];
            let mut i = 0;
            while i < MAXDIM {
                dims[i] = kani::any();
                lbs[i] = kani::any();
                i += 1;
            }
            let img = mk_corrupt_image(ndim, &dims, &lbs);
            let mut c_null: c_int = 0;
            let c = unsafe { pg_array_ndims(img.as_ptr(), &mut c_null) };
            let ctx = mcx::MemoryContext::new_bump("kani-arrhdr");
            let d = datum::Datum::from_usize(img.as_ptr() as usize);
            match call_wrapper(arrayfuncs::ops::fc_array_ndims, d, datum::Datum::from_i32(0), &ctx) {
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
            assert!(c_null == 1);
            core::mem::forget(ctx);
        }
    }

    recipe! {
        /// oid 747 array_dims over the corruption plane. Cheap despite the
        /// core::fmt wall on the value plane: the sanity check nulls out
        /// before dims_text runs, so no fmt machinery is reachable here.
        fn eq_array_dims_ndim_corrupt() {
            install_detoast();
            let ndim: i32 = kani::any();
            kani::assume(ndim as u32 > MAXDIM as u32);
            let mut dims = [0i32; MAXDIM];
            let mut lbs = [0i32; MAXDIM];
            let mut i = 0;
            while i < MAXDIM {
                dims[i] = kani::any();
                lbs[i] = kani::any();
                i += 1;
            }
            let img = mk_corrupt_image(ndim, &dims, &lbs);
            let mut c_null: c_int = 0;
            let mut c_out = [0u8; MAXDIM * 33 + 1];
            let c = unsafe { pg_array_dims(img.as_ptr(), &mut c_null, c_out.as_mut_ptr()) };
            assert!(c_null == 1 && c == 0);
            let ctx = mcx::MemoryContext::new_bump("kani-arrhdr");
            let d = datum::Datum::from_usize(img.as_ptr() as usize);
            match call_wrapper(arrayfuncs::ops::fc_array_dims, d, datum::Datum::from_i32(0), &ctx) {
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
        /// oid 3179 array_cardinality, NON-POSITIVE ndim. The asymmetric
        /// member: it has NO sanity check, so C's ArrayGetNItems takes its own
        /// `ndim <= 0 -> return 0` arm and yields the VALUE 0, not a NULL —
        /// dual-executed here over full-i32 negative ndim with a symbolic
        /// 6-dim body. (ndim == 0 is also covered by eq_array_cardinality_n0.)
        fn eq_array_cardinality_ndim_nonpos() {
            install_detoast();
            let ndim: i32 = kani::any();
            kani::assume(ndim <= 0);
            let mut dims = [0i32; MAXDIM];
            let mut lbs = [0i32; MAXDIM];
            let mut i = 0;
            while i < MAXDIM {
                dims[i] = kani::any();
                lbs[i] = kani::any();
                i += 1;
            }
            let img = mk_corrupt_image(ndim, &dims, &lbs);
            let mut c_err: c_int = 0;
            let c = unsafe { pg_array_cardinality(img.as_ptr(), &mut c_err) };
            assert!(c_err == 0 && c == 0);
            let ctx = mcx::MemoryContext::new_bump("kani-arrhdr");
            let d = datum::Datum::from_usize(img.as_ptr() as usize);
            match call_wrapper(
                arrayfuncs::ops::fc_array_cardinality,
                d,
                datum::Datum::from_i32(0),
                &ctx,
            ) {
                Ok((rnull, rv)) => {
                    assert!(!rnull);
                    assert!(rv.as_i32() == c);
                }
                Err(e) => {
                    core::mem::forget(e);
                    panic!("fc_array_cardinality errored on non-positive ndim");
                }
            }
            core::mem::forget(ctx);
        }
    }

    recipe! {
        /// oid 3179 array_cardinality, ndim ABOVE MAXDIM — RUST-ONLY theorem,
        /// deliberately NOT dual-executed. C hands ArrayGetNItems a bare
        /// `const int *dims` and reads `ndim` words, so above MAXDIM it reads
        /// past the dims area and past the datum: there is no C answer to
        /// match (the executed oracle returned a garbage product for a 7-dim
        /// body and raised the array-size error for ndim=1000, purely
        /// byte-dependent). pgrust takes a DEFINED error instead. Claim: a
        /// catchable PgError with C's own dimension-count sqlstate, never a
        /// panic and never a bogus Ok.
        fn rust_array_cardinality_ndim_over_maxdim_errors() {
            install_detoast();
            let ndim: i32 = kani::any();
            kani::assume(ndim > MAXDIM as i32);
            let mut dims = [0i32; MAXDIM];
            let mut lbs = [0i32; MAXDIM];
            let mut i = 0;
            while i < MAXDIM {
                dims[i] = kani::any();
                lbs[i] = kani::any();
                i += 1;
            }
            let img = mk_corrupt_image(ndim, &dims, &lbs);
            let ctx = mcx::MemoryContext::new_bump("kani-arrhdr");
            let d = datum::Datum::from_usize(img.as_ptr() as usize);
            match call_wrapper(
                arrayfuncs::ops::fc_array_cardinality,
                d,
                datum::Datum::from_i32(0),
                &ctx,
            ) {
                Ok((_rnull, _rv)) => panic!("over-MAXDIM ndim must not yield a value"),
                Err(e) => {
                    assert!(e.sqlstate == ERRCODE_PROGRAM_LIMIT_EXCEEDED);
                    assert!(e.level == ERROR);
                    core::mem::forget(e);
                }
            }
            core::mem::forget(ctx);
        }
    }

    recipe! {
        /// NEGATIVE CONTROL (family gate non-vacuity): C sees reqdim=1,
        /// shipped Rust sees reqdim=2 — MUST FAIL with a value
        /// counterexample. Run with the DEFAULT solver (kissat never
        /// terminates on failing harnesses).
        fn control_array_lower_reqdim_skew_must_fail() {
            install_detoast();
            let (dims, lbs) = sym_lanes(2);
            let img = mk_image(2, &dims, &lbs);
            let mut c_null: c_int = 0;
            let c = unsafe { pg_array_lower(img.as_ptr(), 1, &mut c_null) };
            let ctx = mcx::MemoryContext::new_bump("kani-arrhdr");
            let d = datum::Datum::from_usize(img.as_ptr() as usize);
            match call_wrapper(
                arrayfuncs::ops::fc_array_lower,
                d,
                datum::Datum::from_i32(2),
                &ctx,
            ) {
                Ok((rnull, rv)) => {
                    // NOTE: an earlier control also asserted
                    // `!rnull && c_null == 0` here; kani attributed the
                    // control's (required) failure to THAT check, which a
                    // 1M-iteration native replay (tests/control_replay.rs)
                    // shows can never fail on this plane — check-attribution
                    // artifact. The control now carries exactly ONE failable
                    // check so the gate can only fire for the right reason;
                    // the null-verdict claim is eq_array_lower_n2's theorem.
                    let _ = rnull;
                    assert!(rv.as_i32() == c); // skew: values must diverge
                }
                Err(e) => {
                    // No panic here: the control must carry EXACTLY one
                    // failable property (the value assert). Err-arm
                    // unreachability is attested by the PROVED eq_array_lower
                    // cells, whose Err-arm panics verified unreachable.
                    core::mem::forget(e);
                }
            }
            core::mem::forget(ctx);
        }
    }
}
