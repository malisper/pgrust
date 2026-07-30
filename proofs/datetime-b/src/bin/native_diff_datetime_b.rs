//! Native differential for the datetime-b band-immune / float-wall faces
//! (wave-6 native_diff_dividers precedent — census-grade, weaker than proof;
//! rows record tested(differential), never proved):
//!   - time_scale / timetz_scale: the loop-free /10^k divider face, all
//!     typmods x boundary grid + mass random sweep
//!   - make_date / make_timestamp: the loop-free date2j /100 and /4 face
//!     over dense year sweeps + random
//!   - make_time / make_timestamp: fully-random f64 seconds (the 53-bit
//!     constant multiply the Kani grid harnesses cannot carry)
//!   - interval_avg mean arm (interval_div, 53-bit float divide) +
//!     interval_sum over random states
//!   - interval_avg_serialize round-trip vs the vendored C pair
//!   - timetz_part tz arms over random zones (full Rust unit decode)
//!
//! SAME shipped fc_* wrappers as the Kani harnesses vs the SAME vendored
//! REL_18_STABLE C (linked natively via build.rs cc).
//!
//! Run: cargo run --release --bin native_diff_datetime_b

// Pull in the package lib: the vendored C archive is bundled into its rlib
// (build.rs cc link directive), so the extern "C" symbols resolve from there
// (wave-6 lesson: without this `use`, the extern symbols go unresolved).
use proof_datetime_b as _;

use proof_datetime_b::{
    pg_interval_avg, pg_interval_avg_deserialize, pg_interval_avg_serialize, pg_interval_sum,
    pg_make_date, pg_make_time, pg_make_timestamp, pg_time_scale, pg_timetz_part_units_float,
    pg_timetz_scale, CIntervalAggState, C_DTK_TZ, C_DTK_TZ_HOUR, C_DTK_TZ_MINUTE, UNITS_TIMEZONE,
    UNITS_TIMEZONE_HOUR, UNITS_TIMEZONE_MINUTE,
};

use adt_datetime::consts::Interval;
use adt_timestamp::interval::IntervalAggState;
use datum::{Datum, NullableDatum};
use std::os::raw::c_int;
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData, LocalFcinfo};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    fn i64(&mut self) -> i64 {
        self.next() as i64
    }
    fn i32(&mut self) -> i32 {
        self.next() as i32
    }
    fn f64_bits(&mut self) -> f64 {
        f64::from_bits(self.next())
    }
}

type FcFn = fn(
    Option<&mut FmgrInfo>,
    &mut FunctionCallInfoBaseData,
) -> Result<Datum, Box<types_error::PgError>>;

fn call_n<'m, const N: usize>(
    mcx: mcx::Mcx<'m>,
    fc: FcFn,
    args: [Datum; N],
) -> (Result<Datum, ()>, bool) {
    let mut f = LocalFcinfo::<N>::new(0);
    for (slot, d) in f.args.iter_mut().zip(args) {
        *slot = NullableDatum::value(d);
    }
    // SAFETY: the context outlives the call.
    unsafe { f.set_result_mcx(mcx) };
    let r = fc(None, &mut f).map_err(|_| ());
    (r, f.isnull)
}

fn timetz_img(time: i64, zone: i32) -> [u8; 12] {
    let mut img = [0u8; 12];
    img[..8].copy_from_slice(&time.to_ne_bytes());
    img[8..].copy_from_slice(&zone.to_ne_bytes());
    img
}

fn read_timetz(d: Datum) -> (i64, i32) {
    let p = d.as_usize() as *const u8;
    unsafe { ((p as *const i64).read_unaligned(), (p.add(8) as *const i32).read_unaligned()) }
}

fn read_iv(d: Datum) -> (i64, i32, i32) {
    let p = d.as_usize() as *const u8;
    unsafe {
        (
            (p as *const i64).read_unaligned(),
            (p.add(8) as *const i32).read_unaligned(),
            (p.add(12) as *const i32).read_unaligned(),
        )
    }
}

fn main() {
    let ctx = mcx::MemoryContext::new("native-diff-datetime-b");
    let mcx = ctx.mcx();
    let mut bad = 0u64;
    let mut n = 0u64;
    let report = |name: &str, ok: bool, inputs: String, bad: &mut u64| {
        if !ok {
            *bad += 1;
            if *bad <= 20 {
                println!("MISMATCH {name}: {inputs}");
            }
        }
    };

    // ---- time_scale / timetz_scale: full typmod face ----
    let mut rng = Rng(0x9E3779B97F4A7C15);
    let scale_case = |time: i64, typmod: i32, bad: &mut u64, n: &mut u64| {
        let mut c_out = 0i64;
        unsafe { pg_time_scale(time, typmod, &mut c_out) };
        let (r, _) = call_n(mcx, adt_date::builtins::fc_time_scale as FcFn, [
            Datum::from_i64(time),
            Datum::from_i32(typmod),
        ]);
        let ok = matches!(r, Ok(d) if d.as_i64() == c_out);
        *n += 1;
        report("time_scale", ok, format!("time={time} typmod={typmod}"), bad);

        let zone = 3600i32;
        let (mut rt, mut rz) = (0i64, 0i32);
        unsafe { pg_timetz_scale(time, zone, typmod, &mut rt, &mut rz) };
        let img = timetz_img(time, zone);
        let (r, _) = call_n(mcx, adt_date::builtins::fc_timetz_scale as FcFn, [
            Datum::from_usize(img.as_ptr() as usize),
            Datum::from_i32(typmod),
        ]);
        let ok = matches!(r, Ok(d) if read_timetz(d) == (rt, rz));
        *n += 1;
        report("timetz_scale", ok, format!("time={time} typmod={typmod}"), bad);
    };
    let grid: Vec<i64> = {
        let mut g = vec![0i64, 1, -1, 86_399_999_999, 43_200_000_000, -86_399_999_999];
        for p in 0..=6u32 {
            let scale = 10i64.pow(6 - p);
            let off = scale / 2;
            for d in [-1, 0, 1] {
                g.push(off + d);
                g.push(-(off + d));
                g.push(scale * 12345 + off + d);
            }
        }
        g
    };
    for &t in &grid {
        for tm in -3..=8 {
            scale_case(t, tm, &mut bad, &mut n);
        }
    }
    for _ in 0..1_000_000 {
        // contract-domain concentration: valid times, all typmods
        let t = (rng.next() % 86_400_000_001) as i64;
        scale_case(t, (rng.next() % 9) as i32 - 1, &mut bad, &mut n);
    }
    for _ in 0..200_000 {
        // wide sweep (stays clear of the i64::MIN fwrapv-vs-panic plane,
        // which is out of contract — see the p6 harness fence)
        let t = rng.i64() / 2;
        scale_case(t, rng.i32() % 16, &mut bad, &mut n);
    }

    // ---- make_date: dense year sweep + random ----
    let md_case = |y: i32, m: i32, d: i32, bad: &mut u64, n: &mut u64| {
        let mut c_out = 0i32;
        let mut c_err: c_int = 0;
        unsafe { pg_make_date(y, m, d, &mut c_out, &mut c_err) };
        let (r, _) = call_n(mcx, adt_date::builtins::fc_make_date as FcFn, [
            Datum::from_i32(y),
            Datum::from_i32(m),
            Datum::from_i32(d),
        ]);
        let ok = match r {
            Ok(dd) => c_err == 0 && dd.as_i32() == c_out,
            Err(()) => c_err == 1,
        };
        *n += 1;
        report("make_date", ok, format!("y={y} m={m} d={d} cerr={c_err}"), bad);
    };
    for y in -6000..6000 {
        md_case(y, 2, 29, &mut bad, &mut n);
        md_case(y, 12, 31, &mut bad, &mut n);
    }
    for _ in 0..1_000_000 {
        let y = (rng.i32() % 6_000_000).wrapping_add(rng.i32() % 3);
        md_case(y, rng.i32() % 20, rng.i32() % 40, &mut bad, &mut n);
    }
    for _ in 0..200_000 {
        md_case(rng.i32(), rng.i32(), rng.i32(), &mut bad, &mut n);
    }

    // ---- make_time: random f64 seconds (53-bit face) ----
    let mt_case = |h: i32, m: i32, s: f64, bad: &mut u64, n: &mut u64| {
        let mut c_out = 0i64;
        let mut c_err: c_int = 0;
        unsafe { pg_make_time(h, m, s, &mut c_out, &mut c_err) };
        let (r, _) = call_n(mcx, adt_date::builtins::fc_make_time as FcFn, [
            Datum::from_i32(h),
            Datum::from_i32(m),
            Datum::from_f64(s),
        ]);
        let ok = match r {
            Ok(d) => c_err == 0 && d.as_i64() == c_out,
            Err(()) => c_err == 1,
        };
        *n += 1;
        report("make_time", ok, format!("h={h} m={m} s={s:?} cerr={c_err}"), bad);
    };
    for _ in 0..1_000_000 {
        let s = (rng.next() % 70_000_000) as f64 / 1e6;
        mt_case(rng.i32() % 30, rng.i32() % 70, s, &mut bad, &mut n);
    }
    for _ in 0..200_000 {
        mt_case(rng.i32() % 30, rng.i32() % 70, rng.f64_bits(), &mut bad, &mut n);
    }

    // ---- make_timestamp: year sweep x random time-of-day ----
    let mts_case = |y: i32, mo: i32, d: i32, h: i32, mi: i32, s: f64,
                        bad: &mut u64, n: &mut u64| {
        let mut c_out = 0i64;
        let mut c_err: c_int = 0;
        unsafe { pg_make_timestamp(y, mo, d, h, mi, s, &mut c_out, &mut c_err) };
        let (r, _) = call_n(mcx, adt_timestamp::builtins::fc_make_timestamp as FcFn, [
            Datum::from_i32(y),
            Datum::from_i32(mo),
            Datum::from_i32(d),
            Datum::from_i32(h),
            Datum::from_i32(mi),
            Datum::from_f64(s),
        ]);
        let ok = match r {
            Ok(dd) => c_err == 0 && dd.as_i64() == c_out,
            Err(()) => c_err == 1,
        };
        *n += 1;
        report(
            "make_timestamp",
            ok,
            format!("y={y} mo={mo} d={d} h={h} mi={mi} s={s:?} cerr={c_err}"),
            bad,
        );
    };
    for _ in 0..1_000_000 {
        let y = rng.i32() % 300_000;
        let s = (rng.next() % 70_000_000) as f64 / 1e6;
        mts_case(y, rng.i32() % 15, rng.i32() % 35, rng.i32() % 26, rng.i32() % 62, s,
                 &mut bad, &mut n);
    }

    // ---- interval_avg (mean arm incl interval_div) + interval_sum ----
    let agg_case = |state_r: IntervalAggState, bad: &mut u64, n: &mut u64| {
        let state_c = CIntervalAggState {
            n: state_r.N,
            sum: state_r.sumX,
            p_infcount: state_r.pInfcount,
            n_infcount: state_r.nInfcount,
        };
        for (fc, pg, name) in [
            (
                adt_timestamp::builtins::fc_interval_avg as FcFn,
                pg_interval_avg
                    as unsafe extern "C" fn(
                        *const CIntervalAggState,
                        *mut Interval,
                        *mut c_int,
                        *mut c_int,
                    ) -> c_int,
                "interval_avg",
            ),
            (adt_timestamp::builtins::fc_interval_sum as FcFn, pg_interval_sum, "interval_sum"),
        ] {
            let mut c_res = Interval::default();
            let (mut c_isnull, mut c_err): (c_int, c_int) = (0, 0);
            unsafe { pg(&state_c, &mut c_res, &mut c_isnull, &mut c_err) };
            let (r, isnull) = call_n(mcx, fc, [
                Datum::from_usize(&state_r as *const IntervalAggState as usize),
            ]);
            let ok = match r {
                Ok(d) => {
                    c_err == 0
                        && isnull == (c_isnull == 1)
                        && (isnull || read_iv(d) == (c_res.time, c_res.day, c_res.month))
                }
                Err(()) => c_err != 0,
            };
            *n += 1;
            report(
                name,
                ok,
                format!(
                    "N={} sum=({},{},{}) p={} ni={} cerr={c_err}",
                    state_r.N,
                    state_r.sumX.time,
                    state_r.sumX.day,
                    state_r.sumX.month,
                    state_r.pInfcount,
                    state_r.nInfcount
                ),
                bad,
            );
        }
    };
    for _ in 0..1_000_000 {
        let nn = (rng.next() % 1000) as i64;
        agg_case(
            IntervalAggState {
                N: nn,
                pInfcount: (rng.next() % 3) as i64,
                nInfcount: (rng.next() % 3) as i64,
                sumX: Interval {
                    time: rng.i64() / 4,
                    day: rng.i32() / 4,
                    month: rng.i32() / 4,
                },
            },
            &mut bad,
            &mut n,
        );
    }
    for _ in 0..200_000 {
        agg_case(
            IntervalAggState {
                N: rng.i64(),
                pInfcount: rng.i64(),
                nInfcount: rng.i64(),
                sumX: Interval { time: rng.i64(), day: rng.i32(), month: rng.i32() },
            },
            &mut bad,
            &mut n,
        );
    }

    // ---- serialize round-trip vs vendored C pair ----
    for _ in 0..500_000 {
        let state_r = IntervalAggState {
            N: rng.i64(),
            pInfcount: rng.i64(),
            nInfcount: rng.i64(),
            sumX: Interval { time: rng.i64(), day: rng.i32(), month: rng.i32() },
        };
        let state_c = CIntervalAggState {
            n: state_r.N,
            sum: state_r.sumX,
            p_infcount: state_r.pInfcount,
            n_infcount: state_r.nInfcount,
        };
        let mut c_img = [0u8; 40];
        unsafe { pg_interval_avg_serialize(&state_c, c_img.as_mut_ptr()) };
        let (r, _) = call_n(mcx, adt_timestamp::builtins::fc_interval_avg_serialize as FcFn, [
            Datum::from_usize(&state_r as *const IntervalAggState as usize),
        ]);
        let ok = match r {
            Ok(d) => {
                let p = d.as_usize() as *const u8;
                let img = unsafe { core::slice::from_raw_parts(p, 44) };
                img[4..] == c_img
            }
            Err(()) => false,
        };
        n += 1;
        report("interval_avg_serialize", ok, format!("N={}", state_r.N), &mut bad);

        // C deserialize of the C image must reproduce the state
        let mut back = CIntervalAggState {
            n: 0,
            sum: Interval::default(),
            p_infcount: 0,
            n_infcount: 0,
        };
        let mut c_err: c_int = 0;
        unsafe { pg_interval_avg_deserialize(c_img.as_ptr(), 40, &mut back, &mut c_err) };
        let ok = c_err == 0
            && back.n == state_c.n
            && back.sum == state_c.sum
            && back.p_infcount == state_c.p_infcount
            && back.n_infcount == state_c.n_infcount;
        n += 1;
        report("interval_avg_deserialize(C rt)", ok, format!("N={}", state_r.N), &mut bad);
    }

    // ---- timetz_part tz arms ----
    let tzp_case = |time: i64, zone: i32, bad: &mut u64, n: &mut u64| {
        for (units, val, name) in [
            (&UNITS_TIMEZONE[..], C_DTK_TZ, "timetz_part(timezone)"),
            (&UNITS_TIMEZONE_HOUR[..], C_DTK_TZ_HOUR, "timetz_part(timezone_hour)"),
            (&UNITS_TIMEZONE_MINUTE[..], C_DTK_TZ_MINUTE, "timetz_part(timezone_minute)"),
        ] {
            let mut c_out = 0f64;
            let mut c_err: c_int = 0;
            unsafe { pg_timetz_part_units_float(time, zone, val, &mut c_out, &mut c_err) };
            let timg = timetz_img(time, zone);
            let (r, _) = call_n(mcx, adt_date::builtins::fc_timetz_part as FcFn, [
                Datum::from_usize(units.as_ptr() as usize),
                Datum::from_usize(timg.as_ptr() as usize),
            ]);
            let ok = match r {
                Ok(d) => c_err == 0 && d.as_f64().to_bits() == c_out.to_bits(),
                Err(()) => c_err == 1,
            };
            *n += 1;
            report(name, ok, format!("time={time} zone={zone} cerr={c_err}"), bad);
        }
    };
    for _ in 0..500_000 {
        let time = (rng.next() % 86_400_000_001) as i64;
        tzp_case(time, rng.i32(), &mut bad, &mut n);
    }

    println!("native_diff_datetime_b: {n} checks, {bad} mismatches");
    if bad > 0 {
        std::process::exit(1);
    }
}
