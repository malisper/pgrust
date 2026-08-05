//! Native replay of the kani skew control: verify the FIRST assert
//! (!rnull && c_null == 0) can never fail concretely on this plane, and
//! the value-divergence assert does fail (i.e. the control fails for the
//! RIGHT reason).
use proof_arrayfuncs_hdr::{mk_image, MAXDIM};
use std::os::raw::c_int;

extern "C" {
    fn pg_array_lower(v: *const u8, reqdim: i32, isnull: *mut c_int) -> i32;
}

fn xs(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

#[test]
fn control_first_assert_never_fails_value_assert_does() {
    detoast_seams::detoast_attr::set(|mcx, image| {
        let mut v = mcx::vec_with_capacity_in(mcx, image.len())?;
        mcx::vec_append_bytes(&mut v, image)?;
        Ok(v)
    });
    let ctx = mcx::MemoryContext::new_bump("ctl");
    let mut st = 7u64;
    let mut diverged = 0u64;
    for _ in 0..1_000_000 {
        let mut dims = [0i32; MAXDIM];
        let mut lbs = [0i32; MAXDIM];
        for i in 0..2 {
            dims[i] = xs(&mut st) as i32;
            lbs[i] = xs(&mut st) as i32;
        }
        let img = mk_image(2, &dims, &lbs);
        let mut c_null: c_int = 0;
        let c = unsafe { pg_array_lower(img.as_ptr(), 1, &mut c_null) };
        let mut f = types_fmgr::LocalFcinfo::<2>::new(0);
        f.args[0] =
            datum::NullableDatum::value(datum::Datum::from_usize(img.as_ptr() as usize));
        f.args[1] = datum::NullableDatum::value(datum::Datum::from_i32(2));
        unsafe { f.set_result_mcx(ctx.mcx()) };
        let rv = arrayfuncs::ops::fc_array_lower(None, &mut f).expect("lower");
        // the control's first assert
        assert!(!f.isnull && c_null == 0, "first assert must hold concretely");
        if rv.as_i32() != c {
            diverged += 1;
        }
    }
    assert!(diverged > 900_000, "value assert should diverge nearly always");
}
