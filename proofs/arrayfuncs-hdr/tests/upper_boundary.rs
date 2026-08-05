//! Native replay for the Kani eq_array_upper FAILED (fence-excluded
//! overflow at ops.rs:738): sweep the fence boundary grid with
//! overflow-checks enabled (see Cargo profile below / RUSTFLAGS at
//! invocation). A panic here would confirm the counterexample; none has
//! reproduced — recorded as artifact-suspect per the
//! FAILED-with-no-decodable-playback law.
use proof_arrayfuncs_hdr::{mk_image, MAXDIM};
use std::os::raw::c_int;

extern "C" {
    fn pg_array_upper(v: *const u8, reqdim: i32, isnull: *mut c_int) -> i32;
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

#[test]
fn upper_fence_boundary_grid() {
    install_detoast_once();
    let ctx = mcx::MemoryContext::new_bump("upper-bound");
    let grid: &[i32] = &[
        i32::MIN, i32::MIN + 1, i32::MIN + 2, -2, -1, 0, 1, 2,
        i32::MAX - 2, i32::MAX - 1, i32::MAX,
    ];
    let mut checked = 0u64;
    for &d0 in grid {
        for &l0 in grid {
            let s = d0 as i64 + l0 as i64;
            // the harness fence
            if s < i32::MIN as i64 || s > i32::MAX as i64 || s - 1 < i32::MIN as i64 {
                continue;
            }
            let mut dims = [0i32; MAXDIM];
            let mut lbs = [0i32; MAXDIM];
            dims[0] = d0;
            lbs[0] = l0;
            let img = mk_image(1, &dims, &lbs);
            let mut c_null: c_int = 0;
            let c = unsafe { pg_array_upper(img.as_ptr(), 1, &mut c_null) };
            let mut f = types_fmgr::LocalFcinfo::<2>::new(0);
            f.args[0] = datum::NullableDatum::value(datum::Datum::from_usize(img.as_ptr() as usize));
            f.args[1] = datum::NullableDatum::value(datum::Datum::from_i32(1));
            unsafe { f.set_result_mcx(ctx.mcx()) };
            let rv = arrayfuncs::ops::fc_array_upper(None, &mut f).expect("upper");
            assert!(!f.isnull && c_null == 0);
            assert_eq!(rv.as_i32(), c, "d0={d0} l0={l0}");
            checked += 1;
        }
    }
    assert!(checked > 50, "grid too small: {checked}");
}
