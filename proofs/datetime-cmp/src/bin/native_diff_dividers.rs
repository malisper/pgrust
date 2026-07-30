//! Native differential for the wave-6 /USECS_PER_DAY divider rows whose full
//! symbolic domains are a measured SAT wall (see proofs/TRIAGE.md):
//! time/timetz ± interval, interval_justify_hours/interval, timestamp_mi.
//!
//! SAME shipped fc_* wrappers as the Kani harnesses (real LocalFcinfo frame,
//! real memory context for the by-ref results) vs the SAME vendored
//! REL_18_STABLE C (linked natively via build.rs). Boundary grid cross +
//! mass random sweep. Census-grade, weaker than proof: rows record
//! tested(differential), not proved (json-escape precedent).
//!
//! Run: cargo run --release --bin native_diff_dividers

// Pull in the package lib: the vendored C archive is bundled into its rlib
// (build.rs cc link directive), so the extern "C" symbols resolve from there.
use proof_datetime_cmp as _;

use datum::{Datum, NullableDatum};
use std::os::raw::c_int;
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData, LocalFcinfo};

extern "C" {
    fn pg_time_pl_interval(time: i64, st: i64, sd: i32, sm: i32, result: *mut i64) -> c_int;
    fn pg_time_mi_interval(time: i64, st: i64, sd: i32, sm: i32, result: *mut i64) -> c_int;
    fn pg_timetz_pl_interval(
        tt: i64,
        tz: i32,
        st: i64,
        sd: i32,
        sm: i32,
        rt: *mut i64,
        rz: *mut i32,
    ) -> c_int;
    fn pg_timetz_mi_interval(
        tt: i64,
        tz: i32,
        st: i64,
        sd: i32,
        sm: i32,
        rt: *mut i64,
        rz: *mut i32,
    ) -> c_int;
    fn pg_interval_justify_hours(
        t: i64,
        d: i32,
        m: i32,
        rt: *mut i64,
        rd: *mut i32,
        rm: *mut i32,
    ) -> c_int;
    fn pg_interval_justify_interval(
        t: i64,
        d: i32,
        m: i32,
        rt: *mut i64,
        rd: *mut i32,
        rm: *mut i32,
    ) -> c_int;
    fn pg_timestamp_mi(dt1: i64, dt2: i64, rt: *mut i64, rd: *mut i32, rm: *mut i32) -> c_int;
    // wave-7 band-immune dividers: izone (zone.time / USECS_PER_SEC) and
    // typmod scale (/10^k rounding)
    fn pg_timestamp_izone(zt: i64, zd: i32, zm: i32, ts: i64, r: *mut i64) -> c_int;
    fn pg_timestamptz_izone(zt: i64, zd: i32, zm: i32, ts: i64, r: *mut i64) -> c_int;
    fn pg_timestamp_scale(ts: i64, typmod: i32, r: *mut i64) -> c_int;
}

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
}

type FcFn = fn(
    Option<&mut FmgrInfo>,
    &mut FunctionCallInfoBaseData,
) -> Result<Datum, Box<types_error::PgError>>;

fn iv_img(time: i64, day: i32, month: i32) -> [u8; 16] {
    let mut img = [0u8; 16];
    img[..8].copy_from_slice(&time.to_ne_bytes());
    img[8..12].copy_from_slice(&day.to_ne_bytes());
    img[12..].copy_from_slice(&month.to_ne_bytes());
    img
}

fn timetz_img(time: i64, zone: i32) -> [u8; 12] {
    let mut img = [0u8; 12];
    img[..8].copy_from_slice(&time.to_ne_bytes());
    img[8..].copy_from_slice(&zone.to_ne_bytes());
    img
}

fn call2<'m>(mcx: mcx::Mcx<'m>, fc: FcFn, a: Datum, b: Datum) -> Result<Datum, ()> {
    let mut f = LocalFcinfo::<2>::new(0);
    f.args[0] = NullableDatum::value(a);
    f.args[1] = NullableDatum::value(b);
    // SAFETY: the context outlives the call.
    unsafe { f.set_result_mcx(mcx) };
    fc(None, &mut f).map_err(|_| ())
}

fn call1<'m>(mcx: mcx::Mcx<'m>, fc: FcFn, a: Datum) -> Result<Datum, ()> {
    let mut f = LocalFcinfo::<1>::new(0);
    f.args[0] = NullableDatum::value(a);
    // SAFETY: the context outlives the call.
    unsafe { f.set_result_mcx(mcx) };
    fc(None, &mut f).map_err(|_| ())
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

fn read_timetz(d: Datum) -> (i64, i32) {
    let p = d.as_usize() as *const u8;
    unsafe { ((p as *const i64).read_unaligned(), (p.add(8) as *const i32).read_unaligned()) }
}

const USECS_PER_DAY: i64 = 86_400_000_000;

/// interesting i64 time values (both time-of-day and span.time roles)
fn t_grid() -> Vec<i64> {
    vec![
        0,
        1,
        -1,
        USECS_PER_DAY - 1,
        USECS_PER_DAY,
        USECS_PER_DAY + 1,
        -USECS_PER_DAY,
        -(USECS_PER_DAY + 1),
        43_200_000_000,
        7_777_777_777,
        i64::MAX,
        i64::MAX - 1,
        i64::MIN,
        i64::MIN + 1,
        i64::MAX / 2,
        i64::MIN / 2,
    ]
}

fn d_grid() -> Vec<i32> {
    vec![0, 1, -1, 29, 30, 31, -30, i32::MAX, i32::MIN, i32::MAX - 1, i32::MIN + 1]
}

fn main() {
    let ctx = mcx::MemoryContext::new("native-diff");
    let mcx = ctx.mcx();
    let mut bad = 0u64;
    let mut n = 0u64;
    let mut report = |name: &str, ok: bool, inputs: String| {
        if !ok {
            bad += 1;
            if bad <= 20 {
                println!("MISMATCH {name}: {inputs}");
            }
        }
    };

    // ---- time ± interval ----
    let mut rng = Rng(0x9E3779B97F4A7C15);
    let mut time_case = |time: i64, st: i64, sd: i32, sm: i32| {
        for (fc, pg, name) in [
            (
                adt_date::builtins::fc_time_pl_interval as FcFn,
                pg_time_pl_interval as unsafe extern "C" fn(i64, i64, i32, i32, *mut i64) -> c_int,
                "time_pl_interval",
            ),
            (adt_date::builtins::fc_time_mi_interval as FcFn, pg_time_mi_interval, "time_mi_interval"),
        ] {
            let mut cval = 0i64;
            let cerr = unsafe { pg(time, st, sd, sm, &mut cval) };
            let img = iv_img(st, sd, sm);
            let r = call2(mcx, fc, Datum::from_i64(time), Datum::from_usize(img.as_ptr() as usize));
            let ok = match r {
                Ok(d) => cerr == 0 && d.as_i64() == cval,
                Err(()) => cerr == 1,
            };
            n += 1;
            report(name, ok, format!("time={time} span=({st},{sd},{sm}) cerr={cerr}"));
        }
    };
    for &time in &t_grid() {
        for &st in &t_grid() {
            time_case(time, st, 0, 0);
            time_case(time, st, i32::MAX, i32::MAX);
            time_case(time, st, i32::MIN, i32::MIN);
        }
    }
    for _ in 0..2_000_000 {
        let (time, st) = (rng.i64(), rng.i64());
        time_case(time, st, 0, 0);
    }
    // contract-domain concentration: time in [0, USECS_PER_DAY)
    for _ in 0..2_000_000 {
        let time = (rng.next() % USECS_PER_DAY as u64) as i64;
        let st = rng.i64() % (10 * USECS_PER_DAY);
        time_case(time, st, 0, 0);
    }

    // ---- timetz ± interval ----
    let mut tz_case = |tt: i64, tz: i32, st: i64, sd: i32, sm: i32| {
        for (fc, pg, name) in [
            (
                adt_date::builtins::fc_timetz_pl_interval as FcFn,
                pg_timetz_pl_interval
                    as unsafe extern "C" fn(i64, i32, i64, i32, i32, *mut i64, *mut i32) -> c_int,
                "timetz_pl_interval",
            ),
            (adt_date::builtins::fc_timetz_mi_interval as FcFn, pg_timetz_mi_interval, "timetz_mi_interval"),
        ] {
            let (mut rt, mut rz) = (0i64, 0i32);
            let cerr = unsafe { pg(tt, tz, st, sd, sm, &mut rt, &mut rz) };
            let targ = timetz_img(tt, tz);
            let sarg = iv_img(st, sd, sm);
            let r = call2(
                mcx,
                fc,
                Datum::from_usize(targ.as_ptr() as usize),
                Datum::from_usize(sarg.as_ptr() as usize),
            );
            let ok = match r {
                Ok(d) => {
                    let (t, z) = read_timetz(d);
                    cerr == 0 && t == rt && z == rz
                }
                Err(()) => cerr == 1,
            };
            n += 1;
            report(name, ok, format!("tt={tt} tz={tz} span=({st},{sd},{sm}) cerr={cerr}"));
        }
    };
    for &tt in &t_grid() {
        for &st in &t_grid() {
            tz_case(tt, 3600, st, 0, 0);
        }
    }
    for _ in 0..1_000_000 {
        tz_case(rng.i64(), rng.i32(), rng.i64(), 0, 0);
    }

    // ---- justify_hours / justify_interval ----
    let mut j_case = |t: i64, d: i32, m: i32| {
        for (fc, pg, name) in [
            (
                adt_date::builtins::fc_interval_justify_hours as FcFn,
                pg_interval_justify_hours
                    as unsafe extern "C" fn(i64, i32, i32, *mut i64, *mut i32, *mut i32) -> c_int,
                "interval_justify_hours",
            ),
            (
                adt_date::builtins::fc_interval_justify_interval as FcFn,
                pg_interval_justify_interval,
                "interval_justify_interval",
            ),
        ] {
            let (mut rt, mut rd, mut rm) = (0i64, 0i32, 0i32);
            let cerr = unsafe { pg(t, d, m, &mut rt, &mut rd, &mut rm) };
            let img = iv_img(t, d, m);
            let r = call1(mcx, fc, Datum::from_usize(img.as_ptr() as usize));
            let ok = match r {
                Ok(dd) => {
                    let (ot, od, om) = read_iv(dd);
                    cerr == 0 && ot == rt && od == rd && om == rm
                }
                Err(()) => cerr == 1,
            };
            n += 1;
            report(name, ok, format!("iv=({t},{d},{m}) cerr={cerr}"));
        }
    };
    for &t in &t_grid() {
        for &d in &d_grid() {
            for &m in &d_grid() {
                j_case(t, d, m);
            }
        }
    }
    for _ in 0..1_000_000 {
        j_case(rng.i64(), rng.i32(), rng.i32());
    }

    // ---- timestamp_mi ----
    let mut ts_case = |dt1: i64, dt2: i64| {
        let (mut rt, mut rd, mut rm) = (0i64, 0i32, 0i32);
        let cerr = unsafe { pg_timestamp_mi(dt1, dt2, &mut rt, &mut rd, &mut rm) };
        let r = call2(
            mcx,
            adt_date::builtins::fc_timestamp_mi as FcFn,
            Datum::from_i64(dt1),
            Datum::from_i64(dt2),
        );
        let ok = match r {
            Ok(d) => {
                let (ot, od, om) = read_iv(d);
                cerr == 0 && ot == rt && od == rd && om == rm
            }
            Err(()) => cerr == 1,
        };
        n += 1;
        report("timestamp_mi", ok, format!("dt1={dt1} dt2={dt2} cerr={cerr}"));
    };
    for &a in &t_grid() {
        for &b in &t_grid() {
            ts_case(a, b);
        }
    }
    for _ in 0..2_000_000 {
        ts_case(rng.i64(), rng.i64());
    }

    // ---- wave-7: timestamp[tz]_izone (symbolic zone.time divider) ----
    let mut iz_case = |zt: i64, ts: i64| {
        for (fc, pg, name) in [
            (
                adt_timestamp::builtins::fc_timestamp_izone as FcFn,
                pg_timestamp_izone
                    as unsafe extern "C" fn(i64, i32, i32, i64, *mut i64) -> c_int,
                "timestamp_izone",
            ),
            (adt_timestamp::builtins::fc_timestamptz_izone as FcFn, pg_timestamptz_izone, "timestamptz_izone"),
        ] {
            let mut cval = 0i64;
            let cerr = unsafe { pg(zt, 0, 0, ts, &mut cval) };
            let img = iv_img(zt, 0, 0);
            let r = call2(mcx, fc, Datum::from_usize(img.as_ptr() as usize), Datum::from_i64(ts));
            let ok = match r {
                Ok(d) => cerr == 0 && d.as_i64() == cval,
                Err(()) => cerr == 1 || cerr == 2,
            };
            n += 1;
            report(name, ok, format!("zt={zt} ts={ts} cerr={cerr}"));
        }
    };
    for &zt in &t_grid() {
        for &ts in &t_grid() {
            if zt == i64::MIN {
                continue; // infinite-interval sentinel needs day/month MIN too; not a value cell
            }
            iz_case(zt, ts);
        }
    }
    for _ in 0..2_000_000 {
        iz_case(rng.i64(), rng.i64());
    }

    // ---- wave-7: timestamp_scale (/10^k rounding, all typmods) ----
    let mut sc_case = |ts: i64, typmod: i32| {
        let mut cval = 0i64;
        let cerr = unsafe { pg_timestamp_scale(ts, typmod, &mut cval) };
        let r = call2(
            mcx,
            adt_timestamp::builtins::fc_timestamp_scale as FcFn,
            Datum::from_i64(ts),
            Datum::from_i32(typmod),
        );
        let ok = match r {
            Ok(d) => cerr == 0 && d.as_i64() == cval,
            Err(()) => cerr == 2,
        };
        n += 1;
        report("timestamp_scale", ok, format!("ts={ts} typmod={typmod} cerr={cerr}"));
    };
    for typmod in -2..=7 {
        for &ts in &t_grid() {
            sc_case(ts, typmod);
        }
        // rounding boundaries per scale
        for k in 0..=6u32 {
            let scale = 10i64.pow(6 - k);
            let off = scale / 2;
            for base in [0i64, scale, -scale, 1_755_555_555_000_000] {
                for d in [-1, 0, 1] {
                    sc_case(base + off + d, typmod);
                    sc_case(-(base + off) + d, typmod);
                }
            }
        }
    }
    for _ in 0..2_000_000 {
        sc_case(rng.i64(), (rng.next() % 7) as i32);
    }

    println!("native_diff_dividers: {n} checks, {bad} mismatches");
    if bad > 0 {
        std::process::exit(1);
    }
}
