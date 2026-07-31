//! arrayutils_diff: differential fuzz driver — shipped Rust `arrayutils` vs
//! vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_arrayutils_io.c). Crate under test:
//! crates/backend/utils/adt/arrayutils.
//!
//! Comparison planes: value (exact ints / output arrays element-for-element)
//! + error-verdict + errcode class (1 = ERRCODE_PROGRAM_LIMIT_EXCEEDED, the
//! only code this file raises; Rust side asserted 54000 on the same arms).
//!
//! Input layout: [selector][n][payload]; selector % 7 picks the arm, n % 7
//! gives the dimension count 0..=6 (MAXDIM). Payload supplies little-endian
//! i32 fields, zero-extended when short.
//!   0 array_get_n_items(_safe)  ndim = n (also probes ndim<0 and the
//!     Rust-only ndim>len arm), dims = FULL-RANGE i32s -> value/verdict/code
//!   1 array_check_bounds(_safe) dims, lb = FULL-RANGE i32s -> verdict/code
//!   2 array_get_offset          dim/lb/indx CONSTRAINED (see carves)
//!   3 mda_get_range             st/endp CONSTRAINED -> span image
//!   4 mda_get_prod              range CONSTRAINED -> prod image
//!   5 mda_get_offset_values     prod/span CONSTRAINED -> dist image
//!   6 mda_next_tuple            span>=1, curr in [0,span) -> ret + curr image
//!
//! DOMAIN CARVES (C caller-contract / UB fences — each fences C signed-
//! overflow UB, never pgrust behavior; the C comments state the contracts:
//! "caller has already range-checked", "overflow is impossible"):
//!   - array_get_offset: dim in [1,16], lb/indx in [-64,63] => offset and
//!     scale stay far inside i32 (16^6 = 2^24).
//!   - mda_get_range: st/endp folded to i16 => no i32 overflow in endp-st+1.
//!   - mda_get_prod: range in [1,16] => products <= 2^24.
//!   - mda_get_offset_values: prod/span in [1,16] => all terms tiny.
//!   - mda_next_tuple: span in [1,16], curr folded into [0, span) — C's
//!     documented precondition (curr is a valid tuple under span); span 0
//!     would be modulo-by-zero in C (UB) and a panic in Rust.
//!   array_get_n_items and array_check_bounds run FULL-RANGE: their C bodies
//!   are overflow-checked by construction (int64 widening / builtin add
//!   overflow), so every i32 input is well-defined on both sides.
//!
//! DELIBERATE DEVIATION UNDER TEST (documented in the crate): for
//! array_get_n_items, ndim > dims.len() makes C read past the caller's
//! buffer (UB on a corrupt header); Rust raises the ndims error instead.
//! The driver gives C a MAXDIM-sized buffer always, and separately asserts
//! the RUST-ONLY error arm (no C call) when claimed ndim > provided len.
//!
//! FC-WRAPPER PLANE: not applicable — the crate has no builtins.rs / fc_*
//! surface (support routines, not SQL-callable).
//!
//! SKIPPED: C ArrayGetIntegerTypmods — array-Datum/catalog machinery
//! (deconstruct_array, palloc, pg_strtoint32), not ported by this crate;
//! out of the pure phase-1 scope.

#![allow(dead_code)]

use arrayutils::{
    array_check_bounds, array_check_bounds_safe, array_get_n_items, array_get_n_items_safe,
    array_get_offset, mda_get_offset_values, mda_get_prod, mda_get_range, mda_next_tuple,
};
use types_error::{SoftErrorContext, ERRCODE_PROGRAM_LIMIT_EXCEEDED};

extern "C" {
    // Shared TLS errcode accessor (defined in csrc/pg_float_io.c).
    fn pg_diff_errcode_get() -> i32;

    fn pg_diff_array_get_offset(n: i32, dim: *const i32, lb: *const i32, indx: *const i32)
        -> i32;
    fn pg_diff_array_get_n_items(ndim: i32, dims: *const i32) -> i32;
    fn pg_diff_array_check_bounds(ndim: i32, dims: *const i32, lb: *const i32) -> i32;
    fn pg_diff_mda_get_range(n: i32, span: *mut i32, st: *const i32, endp: *const i32);
    fn pg_diff_mda_get_prod(n: i32, range: *const i32, prod: *mut i32);
    fn pg_diff_mda_get_offset_values(
        n: i32,
        dist: *mut i32,
        prod: *const i32,
        span: *const i32,
    );
    fn pg_diff_mda_next_tuple(n: i32, curr: *mut i32, span: *const i32) -> i32;
}

const MAXDIM: usize = 6;

/// Little-endian i32 field reader, zero-extended past payload end.
fn i32_at(payload: &[u8], idx: usize) -> i32 {
    let mut b = [0u8; 4];
    let off = idx * 4;
    for (i, slot) in b.iter_mut().enumerate() {
        if let Some(&v) = payload.get(off + i) {
            *slot = v;
        }
    }
    i32::from_le_bytes(b)
}

fn arr(payload: &[u8], base: usize) -> [i32; MAXDIM] {
    core::array::from_fn(|i| i32_at(payload, base + i))
}

/// Fold to [1, 16] (positive small dims — the C "validated" domain).
fn small_pos(v: i32) -> i32 {
    (v & 0xf) + 1
}

/// Fold to [-64, 63].
fn small(v: i32) -> i32 {
    (v & 0x7f) - 64
}

pub fn arrayutils_diff(data: &[u8]) {
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    let Some((&nb, payload)) = rest.split_first() else {
        return;
    };
    let n = (nb % 7) as i32; // 0..=6
    let nu = n as usize;

    match sel % 7 {
        0 => {
            // array_get_n_items: full-range dims; ndim also probes <=0.
            let dims = arr(payload, 0);
            let ndim = if nb >= 224 { -((nb % 7) as i32) } else { n };
            let cv = unsafe { pg_diff_array_get_n_items(ndim, dims.as_ptr()) };
            let cerr = unsafe { pg_diff_errcode_get() };
            let mut esc = SoftErrorContext::new(true);
            let rv = array_get_n_items_safe(ndim, &dims, Some(&mut esc)).unwrap();
            assert_eq!(rv, cv, "array_get_n_items value/sentinel");
            assert_eq!(esc.error_occurred(), cerr != 0, "array_get_n_items verdict");
            // hard-error shape agrees with the soft shape
            match array_get_n_items(ndim, &dims) {
                Ok(v) => {
                    assert!(!esc.error_occurred());
                    assert_eq!(v, cv);
                }
                Err(e) => {
                    assert!(esc.error_occurred());
                    assert_eq!(cerr, 1, "oracle errcode class");
                    assert_eq!(e.sqlstate(), ERRCODE_PROGRAM_LIMIT_EXCEEDED);
                }
            }
            // RUST-ONLY deviation arm: claimed ndim wider than the slice
            // (no C call — C would read out of bounds; see module header).
            if n >= 1 {
                let short = &dims[..nu - 1];
                let e = array_get_n_items(n, short).unwrap_err();
                assert_eq!(e.sqlstate(), ERRCODE_PROGRAM_LIMIT_EXCEEDED);
                // soft path: -1 sentinel exactly (mutants-audit survivor fix)
                let mut esc2 = SoftErrorContext::new(true);
                assert_eq!(array_get_n_items_safe(n, short, Some(&mut esc2)).unwrap(), -1);
                assert!(esc2.error_occurred());
            }
        }
        1 => {
            // array_check_bounds: full-range dims/lb.
            let dims = arr(payload, 0);
            let lb = arr(payload, MAXDIM);
            let cv = unsafe { pg_diff_array_check_bounds(n, dims.as_ptr(), lb.as_ptr()) };
            let cerr = unsafe { pg_diff_errcode_get() };
            let mut esc = SoftErrorContext::new(true);
            let rv =
                array_check_bounds_safe(n, &dims[..nu], &lb[..nu], Some(&mut esc)).unwrap();
            assert_eq!(rv as i32, cv, "array_check_bounds verdict");
            assert_eq!(esc.error_occurred(), cerr != 0);
            match array_check_bounds(n, &dims[..nu], &lb[..nu]) {
                Ok(()) => assert_eq!(cv, 1),
                Err(e) => {
                    assert_eq!(cv, 0);
                    assert_eq!(cerr, 1, "oracle errcode class");
                    assert_eq!(e.sqlstate(), ERRCODE_PROGRAM_LIMIT_EXCEEDED);
                }
            }
        }
        2 => {
            // array_get_offset: constrained to the validated domain.
            let dim: [i32; MAXDIM] = core::array::from_fn(|i| small_pos(i32_at(payload, i)));
            let lb: [i32; MAXDIM] =
                core::array::from_fn(|i| small(i32_at(payload, MAXDIM + i)));
            let indx: [i32; MAXDIM] =
                core::array::from_fn(|i| small(i32_at(payload, 2 * MAXDIM + i)));
            let rv = array_get_offset(n, &dim, &lb, &indx);
            let cv = unsafe {
                pg_diff_array_get_offset(n, dim.as_ptr(), lb.as_ptr(), indx.as_ptr())
            };
            assert_eq!(rv, cv, "array_get_offset value");
        }
        3 => {
            // mda_get_range: i16-folded endpoints.
            let st: [i32; MAXDIM] =
                core::array::from_fn(|i| i32_at(payload, i) as i16 as i32);
            let endp: [i32; MAXDIM] =
                core::array::from_fn(|i| i32_at(payload, MAXDIM + i) as i16 as i32);
            let mut rspan = [0i32; MAXDIM];
            let mut cspan = [0i32; MAXDIM];
            mda_get_range(n, &mut rspan, &st, &endp);
            unsafe {
                pg_diff_mda_get_range(n, cspan.as_mut_ptr(), st.as_ptr(), endp.as_ptr())
            };
            assert_eq!(rspan, cspan, "mda_get_range span image");
        }
        4 => {
            // mda_get_prod: small positive ranges; C writes prod[n-1] so n>=1.
            if n == 0 {
                return;
            }
            let range: [i32; MAXDIM] =
                core::array::from_fn(|i| small_pos(i32_at(payload, i)));
            let mut rprod = [0i32; MAXDIM];
            let mut cprod = [0i32; MAXDIM];
            mda_get_prod(n, &range, &mut rprod);
            unsafe { pg_diff_mda_get_prod(n, range.as_ptr(), cprod.as_mut_ptr()) };
            assert_eq!(rprod, cprod, "mda_get_prod image");
        }
        5 => {
            // mda_get_offset_values: small positive prod/span; n>=1.
            if n == 0 {
                return;
            }
            let prod: [i32; MAXDIM] =
                core::array::from_fn(|i| small_pos(i32_at(payload, i)));
            let span: [i32; MAXDIM] =
                core::array::from_fn(|i| small_pos(i32_at(payload, MAXDIM + i)));
            let mut rdist = [0i32; MAXDIM];
            let mut cdist = [0i32; MAXDIM];
            mda_get_offset_values(n, &mut rdist, &prod, &span);
            unsafe {
                pg_diff_mda_get_offset_values(
                    n,
                    cdist.as_mut_ptr(),
                    prod.as_ptr(),
                    span.as_ptr(),
                )
            };
            assert_eq!(rdist, cdist, "mda_get_offset_values image");
        }
        _ => {
            // mda_next_tuple: span>=1, curr folded into [0, span). n=0 probes
            // the n<=0 early-return on both sides.
            let span: [i32; MAXDIM] =
                core::array::from_fn(|i| small_pos(i32_at(payload, i)));
            let mut rcurr: [i32; MAXDIM] =
                core::array::from_fn(|i| i32_at(payload, MAXDIM + i).rem_euclid(span[i]));
            let mut ccurr = rcurr;
            let rv = mda_next_tuple(n, &mut rcurr, &span);
            let cv =
                unsafe { pg_diff_mda_next_tuple(n, ccurr.as_mut_ptr(), span.as_ptr()) };
            assert_eq!(rv, cv, "mda_next_tuple return");
            assert_eq!(rcurr, ccurr, "mda_next_tuple curr image");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fixed sweep: every arm executes against the C oracle every test run.
    #[test]
    fn arm_sweep() {
        let payloads: [&[u8]; 4] = [
            &[],
            &[0xff; 96],
            &[0x01; 96],
            &[0x80, 0x00, 0x00, 0x80, 0x7f, 0xff, 0xff, 0x7f, 0x10, 0x20, 0x30, 0x40],
        ];
        for sel in 0u8..7 {
            for nb in [0u8, 1, 3, 6, 13, 200, 250] {
                for p in payloads {
                    let mut data = vec![sel, nb];
                    data.extend_from_slice(p);
                    arrayutils_diff(&data);
                }
            }
        }
    }

    /// Single-field witness pairs (skill obligation): array_get_offset merges
    /// (dim, lb, indx) per-dimension into one scalar — each element of each
    /// array must independently steer the offset, small deltas, both orders.
    #[test]
    fn single_field_witness_pairs_offset() {
        let dim = [3i32, 4, 5, 2, 3, 2];
        let lb = [1i32, 1, 1, 1, 1, 1];
        let indx = [2i32, 3, 4, 1, 2, 1];
        let base = array_get_offset(6, &dim, &lb, &indx);
        for i in 0..6 {
            for delta in [-1i32, 1] {
                // indx element: must move the offset
                let mut ix = indx;
                ix[i] += delta;
                let v = array_get_offset(6, &dim, &lb, &ix);
                assert_ne!(v, base, "indx[{i}] delta {delta} not witnessed");
                // lb element: exact mirror of indx
                let mut l = lb;
                l[i] += delta;
                let w = array_get_offset(6, &dim, &l, &indx);
                assert_eq!(w - base, base - v, "lb[{i}] should mirror indx[{i}]");
            }
            // dim element steers scale for all lower positions (i > 0)
            if i > 0 {
                let mut d = dim;
                d[i] += 1;
                assert_ne!(
                    array_get_offset(6, &d, &lb, &indx),
                    base,
                    "dim[{i}] delta not witnessed"
                );
            }
        }
        // and variants agree with C via the differential arm
        for i in 0..6usize {
            let mut payload = Vec::new();
            for v in dim {
                payload.extend_from_slice(&(v - 1).to_le_bytes()); // small_pos folds &0xf +1
            }
            for v in lb {
                payload.extend_from_slice(&(v + 64).to_le_bytes()); // small folds &0x7f -64
            }
            let mut ix = indx;
            ix[i] += 1;
            for v in ix {
                payload.extend_from_slice(&(v + 64).to_le_bytes());
            }
            let mut data = vec![2u8, 6u8];
            data.extend_from_slice(&payload);
            arrayutils_diff(&data);
        }
    }

    /// The Rust-only corrupt-header arm: claimed ndim > provided dims.
    #[test]
    fn ndim_wider_than_slice_errors() {
        let e = array_get_n_items(3, &[2, 2]).unwrap_err();
        assert_eq!(e.sqlstate(), ERRCODE_PROGRAM_LIMIT_EXCEEDED);
    }

    /// Replay every checked-in seed. Corpus is COMMITTED.
    #[test]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/arrayutils_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/arrayutils_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() && p.file_name().is_some_and(|f| f != ".gitkeep") {
                arrayutils_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }
}
