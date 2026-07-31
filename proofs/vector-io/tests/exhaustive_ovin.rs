//! Native EXHAUSTIVE-DIFF for the oidvector in/out cluster (2026-07-30
//! measure-sweep triage). Enumerates the ENTIRE eq_ovin_len0..len4 domain
//! (all NUL-free ASCII strings of length 0..=4, bytes 1..=127, in the CAP=8
//! rig) through the shipped fc_oidvectorin against the natively compiled C
//! shim, plus the eq_ovout_spots concrete vectors and the
//! cover_ovin_both_arms reachability facts. Census-grade
//! (tested(differential)); never recorded as proved.
//!
//! Run: cargo test --release --test exhaustive_ovin -- --nocapture

// The package lib is empty under non-kani cfg; force-link it so the build
// script's rustc-link-lib for the native C archive reaches this test.
#[allow(unused_extern_crates)]
extern crate proof_vector_io;

use datum::{Datum, NullableDatum};
use std::os::raw::c_int;
use types_error::{ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERROR};
use types_fmgr::LocalFcinfo;

extern "C" {
    fn pg_oidvectorin(s: *const u8, values: *mut u32, cap: c_int, n_out: *mut c_int) -> c_int;
    fn pg_oidvectorout(values: *const u32, dim1: i32, rp: *mut u8) -> c_int;
}

const OIDOID: u32 = 26;

fn check_img_header(img: &[u8], n: i32) -> Result<(), String> {
    let total = 24 + 4 * n as usize;
    let vl = ::datum::varlena::set_varsize_4b(total);
    if img[0..4] != vl {
        return Err(format!("varlena header mismatch: {:?} vs {:?}", &img[0..4], vl));
    }
    let f = |o: usize| i32::from_ne_bytes([img[o], img[o + 1], img[o + 2], img[o + 3]]);
    if f(4) != 1 || f(8) != 0 || f(12) as u32 != OIDOID || f(16) != n || f(20) != 0 {
        return Err(format!(
            "header fields mismatch: ndim={} dataoffset={} elemtype={} dim1={} lbound1={} (want 1/0/{}/{}/0)",
            f(4), f(8), f(12), f(16), f(20), OIDOID, n
        ));
    }
    Ok(())
}

/// One in-direction comparison; mirrors proofs::ovin_check exactly.
fn ovin_diff(buf: &[u8; 8], ctx: &mcx::MemoryContext) -> Result<(), String> {
    let mut cvals = [0u32; 4];
    let mut cn: c_int = 0;
    let cst = unsafe { pg_oidvectorin(buf.as_ptr(), cvals.as_mut_ptr(), 4, &mut cn) };
    if cst == 99 {
        return Err("repalloc arm reached under len<=7".into());
    }

    let mut f = LocalFcinfo::<1>::new(0);
    // SAFETY: ctx outlives the call.
    unsafe { f.set_result_mcx(ctx.mcx()) };
    f.args[0] = NullableDatum::value(Datum::from_usize(buf.as_ptr() as usize));
    match adt_scalar::builtins::fc_oidvectorin(None, &mut f) {
        Ok(d) => {
            if cst != 0 {
                return Err(format!("Rust Ok but C status {}", cst));
            }
            let n = cn;
            let img = unsafe {
                core::slice::from_raw_parts(d.as_usize() as *const u8, 24 + 4 * n as usize)
            };
            check_img_header(img, n)?;
            for i in 0..n as usize {
                let o = 24 + 4 * i;
                let v = u32::from_ne_bytes([img[o], img[o + 1], img[o + 2], img[o + 3]]);
                if v != cvals[i] {
                    return Err(format!("element {} mismatch: rust {} vs c {}", i, v, cvals[i]));
                }
            }
            Ok(())
        }
        Err(e) => {
            let ok = (cst == 1 && e.sqlstate == ERRCODE_INVALID_TEXT_REPRESENTATION)
                || (cst == 2 && e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
            if !ok {
                return Err(format!(
                    "error-class mismatch: C status {} vs Rust sqlstate {:?}",
                    cst, e.sqlstate
                ));
            }
            if e.level != ERROR {
                return Err(format!("level mismatch: {:?}", e.level));
            }
            Ok(())
        }
    }
}

#[test]
fn exhaustive_ovin_len0_to_len4() {
    let mut count: u64 = 0;
    let mut failures: Vec<String> = Vec::new();
    // Epoch-recycled bump context so leaked result images don't accumulate.
    let mut ctx = mcx::MemoryContext::new_bump("native-ovin");
    let mut since_recycle: u32 = 0;

    let mut run = |buf: &[u8; 8], ctx: &mut mcx::MemoryContext, since: &mut u32| -> Option<String> {
        *since += 1;
        if *since >= 1 << 14 {
            *ctx = mcx::MemoryContext::new_bump("native-ovin");
            *since = 0;
        }
        ovin_diff(buf, ctx).err().map(|e| format!("input {:?}: {}", buf, e))
    };

    // len 0
    let buf = [0u8; 8];
    if let Some(e) = run(&buf, &mut ctx, &mut since_recycle) {
        failures.push(e);
    }
    count += 1;

    // len 1..=4, bytes 1..=127
    for len in 1..=4usize {
        let mut idx = [1u8; 4];
        loop {
            let mut buf = [0u8; 8];
            buf[..len].copy_from_slice(&idx[..len]);
            if let Some(e) = run(&buf, &mut ctx, &mut since_recycle) {
                if failures.len() < 20 {
                    failures.push(e);
                }
            }
            count += 1;
            // increment odometer over positions 0..len, digits 1..=127
            let mut p = 0;
            loop {
                if p == len {
                    break;
                }
                if idx[p] < 127 {
                    idx[p] += 1;
                    break;
                }
                idx[p] = 1;
                p += 1;
            }
            if p == len {
                break;
            }
        }
        eprintln!("len {} done, cumulative {} cases, {} failures", len, count, failures.len());
    }

    assert!(
        failures.is_empty(),
        "{} divergences in {} cases; first: {}",
        failures.len(),
        count,
        failures[0]
    );
    eprintln!("EXHAUSTIVE ovin len0..=4: {} cases, 0 divergences", count);
}

/// eq_ovout_spots concrete vectors, natively.
#[test]
fn native_ovout_spots() {
    for values in [
        [0u32, u32::MAX, 1_000_000_000, 0],
        [4_294_967_294, 10_000, 99_999, 0],
    ] {
        let dim1 = 3i32;
        let mut cbuf = [0u8; 64];
        let clen = unsafe { pg_oidvectorout(values.as_ptr(), dim1, cbuf.as_mut_ptr()) };

        #[repr(C)]
        struct OidVec4 {
            hdr: array::oidvector,
            values: [u32; 4],
        }
        let total = 24 + 4 * dim1 as usize;
        let vl = ::datum::varlena::set_varsize_4b(total);
        let img = OidVec4 {
            hdr: array::oidvector {
                vl_len_: i32::from_ne_bytes(vl),
                ndim: 1,
                dataoffset: 0,
                elemtype: OIDOID,
                dim1,
                lbound1: 0,
            },
            values,
        };
        let ctx = mcx::MemoryContext::new_bump("native-ovout");
        let mut f = LocalFcinfo::<1>::new(0);
        // SAFETY: ctx outlives the call.
        unsafe { f.set_result_mcx(ctx.mcx()) };
        f.args[0] = NullableDatum::value(Datum::from_usize(&img as *const OidVec4 as usize));
        let d = adt_scalar::builtins::fc_oidvectorout(None, &mut f)
            .expect("oidvectorout errored on a valid image");
        let out = unsafe {
            core::slice::from_raw_parts(d.as_usize() as *const u8, clen as usize + 1)
        };
        assert_eq!(
            out,
            &cbuf[..clen as usize + 1],
            "ovout mismatch for {:?}: rust {:?} vs c {:?}",
            values,
            String::from_utf8_lossy(&out[..clen as usize]),
            String::from_utf8_lossy(&cbuf[..clen as usize])
        );
    }
    eprintln!("native ovout spots: both vectors identical to C");
}

/// cover_ovin_both_arms reachability facts, natively.
#[test]
fn native_cover_both_arms() {
    let ctx = mcx::MemoryContext::new_bump("native-cover");
    // Ok arm
    let ok_buf = *b"11\0\0\0\0\0\0";
    assert!(ovin_diff(&ok_buf, &ctx).is_ok());
    let mut f = LocalFcinfo::<1>::new(0);
    // SAFETY: ctx outlives the call.
    unsafe { f.set_result_mcx(ctx.mcx()) };
    f.args[0] = NullableDatum::value(Datum::from_usize(ok_buf.as_ptr() as usize));
    assert!(adt_scalar::builtins::fc_oidvectorin(None, &mut f).is_ok());
    // 22P02 arm
    let err_buf = *b",1\0\0\0\0\0\0";
    let mut f2 = LocalFcinfo::<1>::new(0);
    // SAFETY: ctx outlives the call.
    unsafe { f2.set_result_mcx(ctx.mcx()) };
    f2.args[0] = NullableDatum::value(Datum::from_usize(err_buf.as_ptr() as usize));
    match adt_scalar::builtins::fc_oidvectorin(None, &mut f2) {
        Err(e) => assert_eq!(e.sqlstate, ERRCODE_INVALID_TEXT_REPRESENTATION),
        Ok(_) => panic!("expected 22P02"),
    }
    eprintln!("native cover: both arms reachable");
}
