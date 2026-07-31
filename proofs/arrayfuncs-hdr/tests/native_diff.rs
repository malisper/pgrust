//! Native differential for the array_dims VALUE plane (the text image the
//! symbolic harness cannot cover — core::fmt walls symex; see src/lib.rs)
//! plus a cross-check sweep of the scalar rows. Census-grade
//! (tested(differential)), never recorded as proved.
//!
//! Rust side here is the shipped dims_text-backed wrapper output via
//! arrayfuncs::foundation::read_dims_lbounds + ops::dims_text being private;
//! we go through the public wrapper with a real bump context and an
//! installed identity detoast (same plane as the harnesses).

use proof_arrayfuncs_hdr::{mk_image, CAP, MAXDIM};
use std::os::raw::c_int;

extern "C" {
    fn pg_array_dims(v: *const u8, isnull: *mut c_int, out: *mut u8) -> i32;
    fn pg_array_upper(v: *const u8, reqdim: i32, isnull: *mut c_int) -> i32;
    fn pg_array_cardinality(v: *const u8, err: *mut c_int) -> i32;
}

fn install_detoast_once() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        detoast_seams::detoast_attr::set(|mcx, image| {
            let mut v = mcx::vec_with_capacity_in(mcx, image.len())?;
            mcx::vec_append_bytes(&mut v, image)?;
            Ok(v)
        });
    });
}

fn call2(
    fc: fn(
        Option<&mut types_fmgr::FmgrInfo>,
        &mut types_fmgr::FunctionCallInfoBaseData,
    ) -> types_error::PgResult<datum::Datum>,
    a: datum::Datum,
    b: datum::Datum,
    ctx: &mcx::MemoryContext,
) -> types_error::PgResult<(bool, datum::Datum)> {
    let mut f = types_fmgr::LocalFcinfo::<2>::new(0);
    f.args[0] = datum::NullableDatum::value(a);
    f.args[1] = datum::NullableDatum::value(b);
    unsafe { f.set_result_mcx(ctx.mcx()) };
    let r = fc(None, &mut f)?;
    Ok((f.isnull, r))
}

/// xorshift PRNG — deterministic, no deps.
fn xs(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

#[test]
fn native_diff_array_dims_and_scalars() {
    install_detoast_once();
    let ctx = mcx::MemoryContext::new_bump("native-diff");
    let mut st = 0x9e3779b97f4a7c15u64;
    let mut checked = 0u64;
    for iter in 0..2_000_000u64 {
        let ndim = (xs(&mut st) % 8) as i32 - 1; // -1..=6: incl 0 and edge 6
        if ndim < 0 {
            continue; // corruption plane: witnessed separately
        }
        let mut dims = [0i32; MAXDIM];
        let mut lbs = [0i32; MAXDIM];
        for i in 0..ndim as usize {
            // mixed magnitudes incl negatives; keep ub in-contract
            dims[i] = (xs(&mut st) as i32).unsigned_abs() as i32 % 1_000_000;
            lbs[i] = (xs(&mut st) as i32) % 1_000_000;
        }
        let img = mk_image(ndim, &dims, &lbs);
        let d = datum::Datum::from_usize(img.as_ptr() as usize);

        // array_dims image
        let mut c_null: c_int = 0;
        let mut c_out = [0u8; MAXDIM * 33 + 1];
        let c_len =
            unsafe { pg_array_dims(img.as_ptr(), &mut c_null, c_out.as_mut_ptr()) } as usize;
        let (rnull, rv) = call2(
            arrayfuncs::ops::fc_array_dims,
            d,
            datum::Datum::from_i32(0),
            &ctx,
        )
        .expect("fc_array_dims");
        assert_eq!(rnull, c_null == 1, "dims null verdict iter={iter}");
        if !rnull {
            // rv is a text varlena datum: [4B header][bytes]
            let p = rv.as_usize() as *const u8;
            let total = unsafe { u32::from_ne_bytes(*(p as *const [u8; 4])) >> 2 } as usize;
            let body = unsafe { std::slice::from_raw_parts(p.add(4), total - 4) };
            assert_eq!(body, &c_out[..c_len], "dims image iter={iter}");
        }

        // array_upper value
        let reqdim = (xs(&mut st) % 8) as i32; // 0..=7: null + value arms
        let mut cu_null: c_int = 0;
        let cu = unsafe { pg_array_upper(img.as_ptr(), reqdim, &mut cu_null) };
        let (runull, ruv) = call2(
            arrayfuncs::ops::fc_array_upper,
            d,
            datum::Datum::from_i32(reqdim),
            &ctx,
        )
        .expect("fc_array_upper");
        assert_eq!(runull, cu_null == 1, "upper null verdict iter={iter}");
        if !runull {
            assert_eq!(ruv.as_i32(), cu, "upper value iter={iter}");
        }

        // cardinality
        let mut c_err: c_int = 0;
        let cc = unsafe { pg_array_cardinality(img.as_ptr(), &mut c_err) };
        match call2(
            arrayfuncs::ops::fc_array_cardinality,
            d,
            datum::Datum::from_i32(0),
            &ctx,
        ) {
            Ok((_n, rcv)) => {
                assert_eq!(c_err, 0, "cardinality err verdict iter={iter}");
                assert_eq!(rcv.as_i32(), cc, "cardinality value iter={iter}");
            }
            Err(_) => assert_eq!(c_err, 1, "cardinality err verdict iter={iter}"),
        }
        checked += 1;
    }
    assert!(checked > 1_000_000);
}
