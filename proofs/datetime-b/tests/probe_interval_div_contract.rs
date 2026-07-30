//! One-off probe: decode the native-diff interval_avg mean-arm mismatches
//! (fp-contraction suspect). Not part of the standing suite verdict.
use proof_datetime_b::*;
use adt_datetime::consts::Interval;
use adt_timestamp::interval::IntervalAggState;
use datum::{Datum, NullableDatum};
use std::os::raw::c_int;
use types_fmgr::LocalFcinfo;

#[test]
fn probe_mean_arm_mismatch() {
    let ctx = mcx::MemoryContext::new("probe");
    let mcx = ctx.mcx();
    let cases = [
        (83i64, 1754227992627510275i64, 488638659i32, -511530385i32),
        (26, 1034678173914253036, 23201, 89056888),
        (240, 2300052485921199597, -495105715, -413027200),
    ];
    for (n, t, d, m) in cases {
        let sum = Interval { time: t, day: d, month: m };
        let r_state = IntervalAggState { N: n, pInfcount: 0, nInfcount: 0, sumX: sum };
        let c_state = CIntervalAggState { n, sum, p_infcount: 0, n_infcount: 0 };
        let mut c_res = Interval::default();
        let (mut c_isnull, mut c_err): (c_int, c_int) = (0, 0);
        unsafe { pg_interval_avg(&c_state, &mut c_res, &mut c_isnull, &mut c_err) };
        let mut f = LocalFcinfo::<1>::new(0);
        f.args[0] = NullableDatum::value(Datum::from_usize(&r_state as *const _ as usize));
        unsafe { f.set_result_mcx(mcx) };
        let r = adt_timestamp::builtins::fc_interval_avg(None, &mut f);
        match r {
            Ok(dd) => {
                let p = dd.as_usize() as *const u8;
                let rt = unsafe { (p as *const i64).read_unaligned() };
                let rd = unsafe { (p.add(8) as *const i32).read_unaligned() };
                let rm = unsafe { (p.add(12) as *const i32).read_unaligned() };
                println!(
                    "N={n} sum=({t},{d},{m})\n  C   =({},{},{}) cerr={c_err}\n  Rust=({rt},{rd},{rm})",
                    c_res.time, c_res.day, c_res.month
                );
            }
            Err(_) => println!("N={n}: Rust err, cerr={c_err}"),
        }
    }
}
