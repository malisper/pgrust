//! Native differential tier for the lane-D adt_date remainder rows
//! (tested(differential) — census-grade, weaker than proof; wave-6 bin
//! pattern). Covers exactly what the Kani harnesses fence or wall:
//!   - 1419 interval_time: the band-immune `% USECS_PER_DAY` face
//!     (Kani full screen walls; spots + err plane proved).
//!   - 2478 interval_recv / 2479 interval_send: memory-walled >6GiB local
//!     cap (date/time/timetz siblings proved) + random-typmod recv face.
//!   - wrap-parity regions excluded by Kani overflow checks (C -fwrapv ==
//!     Rust RELEASE wrap): time_mi_time out-of-contract, datetime rows'
//!     below-lower-bound dates, in_range_timetz's top offset sliver.
//!   - 2038 timetz_izone full-zone face (Kani: literal cells + err planes).
//!   - 1200 interval_scale random typmod x interval sweep (Kani: typmod<0
//!     plane + concrete spot grid).
//!   - 2910/2912 typmodout full sweep; 2909/2911 typmod_check sweep.
//! MUST run --release (wrap parity is a release-semantics claim).

use proof_datetime_b::*;
use std::os::raw::c_int;

use adt_datetime::consts::Interval;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        // splitmix64
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
    fn i64(&mut self) -> i64 {
        self.next() as i64
    }
    fn i32(&mut self) -> i32 {
        self.next() as i32
    }
    fn range_i64(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next() % ((hi - lo) as u64 + 1)) as i64
    }
}

fn interesting_i64(r: &mut Rng) -> i64 {
    const EDGE: &[i64] = &[
        0, 1, -1, 86_399_999_999, 86_400_000_000, 86_400_000_001,
        -86_400_000_000, -86_400_000_001, i64::MIN, i64::MIN + 1, i64::MAX,
        i64::MAX - 1, 1_000_000, -1_000_000,
    ];
    match r.next() % 3 {
        0 => EDGE[(r.next() % EDGE.len() as u64) as usize],
        1 => r.i64(),
        _ => r.range_i64(-200_000_000_000, 200_000_000_000),
    }
}

fn main() {
    let mut r = Rng(0xADD8_2026_0729);
    let mut checks: u64 = 0;
    let mut fails: u64 = 0;
    let mut report = |name: &str, ok: bool, detail: String| {
        if !ok {
            println!("MISMATCH {name}: {detail}");
        }
    };

    // ---- 1419 interval_time (% face, full i64 incl wrap-free err lattice)
    for _ in 0..2_000_000 {
        let t = interesting_i64(&mut r);
        let (d, m) = (r.i32(), r.i32());
        let span = Interval { time: t, day: d, month: m };
        let mut c_out: i64 = 0;
        let mut c_err: c_int = 0;
        unsafe { pg_adr_interval_time(t, d, m, &mut c_out, &mut c_err) };
        let rr = adt_date::interval_time(&span);
        checks += 1;
        match rr {
            Ok(v) => {
                let ok = c_err == 0 && v == c_out;
                if !ok { fails += 1; }
                report("interval_time", ok, format!("t={t} d={d} m={m} rust={v} c={c_out} cerr={c_err}"));
            }
            Err(_) => {
                let ok = c_err == 1;
                if !ok { fails += 1; }
                report("interval_time", ok, format!("t={t} d={d} m={m} rust=Err cerr={c_err}"));
            }
        }
    }

    // ---- 1690 time_mi_time full-domain wrap parity ----
    for _ in 0..2_000_000 {
        let (t1, t2) = (interesting_i64(&mut r), interesting_i64(&mut r));
        let mut c_res = Interval { time: 0, day: 0, month: 0 };
        unsafe { pg_adr_time_mi_time(t1, t2, &mut c_res) };
        let rr = adt_date::time_mi_time(t1, t2);
        checks += 1;
        let ok = rr.time == c_res.time && rr.day == c_res.day && rr.month == c_res.month;
        if !ok { fails += 1; }
        report("time_mi_time", ok, format!("t1={t1} t2={t2} rust={} c={}", rr.time, c_res.time));
    }

    // ---- date->timestamp family incl below-lower-bound wrap region ----
    for _ in 0..2_000_000 {
        let date: i32 = match r.next() % 3 {
            0 => r.i32(),
            1 => (r.next() % 4_000_000) as i32 - 2_600_000,
            _ => [i32::MIN, i32::MAX, -2_451_545, -2_451_546, 106_751_993, 106_751_994][(r.next() % 6) as usize],
        };
        let time = r.range_i64(0, 86_400_000_000);
        let zone = (r.next() % 115_200) as i32 - 57_600;
        // date2timestamp
        let mut c_out: i64 = 0;
        let mut c_err: c_int = 0;
        unsafe { pg_adr_date_timestamp(date, &mut c_out, &mut c_err) };
        let rr = adt_date::date2timestamp(date);
        checks += 1;
        let ok = match &rr {
            Ok(v) => c_err == 0 && *v == c_out,
            Err(_) => c_err == 1,
        };
        if !ok { fails += 1; }
        report("date2timestamp", ok, format!("date={date} rust={rr:?} c={c_out}/{c_err}"));
        // datetime_timestamp
        let mut c_out2: i64 = 0;
        let mut c_err2: c_int = 0;
        unsafe { pg_adr_datetime_timestamp(date, time, &mut c_out2, &mut c_err2) };
        let rr2 = adt_date::datetime_timestamp(date, time);
        checks += 1;
        let ok2 = match &rr2 {
            Ok(v) => c_err2 == 0 && *v == c_out2,
            Err(_) => c_err2 == 1,
        };
        if !ok2 { fails += 1; }
        report("datetime_timestamp", ok2, format!("date={date} time={time} rust={rr2:?} c={c_out2}/{c_err2}"));
        // datetimetz_timestamptz
        let tt = adt_date::TimeTzADT { time, zone };
        let mut c_out3: i64 = 0;
        let mut c_err3: c_int = 0;
        unsafe { pg_adr_datetimetz_timestamptz(date, time, zone, &mut c_out3, &mut c_err3) };
        let rr3 = adt_date::datetimetz_timestamptz(date, &tt);
        checks += 1;
        let ok3 = match &rr3 {
            Ok(v) => c_err3 == 0 && *v == c_out3,
            Err(_) => c_err3 == 1,
        };
        if !ok3 { fails += 1; }
        report("datetimetz", ok3, format!("date={date} time={time} zone={zone} rust={rr3:?} c={c_out3}/{c_err3}"));
    }

    // ---- 2038 timetz_izone full-zone face ----
    for _ in 0..2_000_000 {
        let zt = match r.next() % 2 {
            0 => r.i64(),
            _ => r.range_i64(-100_000_000_000, 100_000_000_000),
        };
        let (zd, zm) = if r.next() % 4 == 0 { (r.i32(), r.i32()) } else { (0, 0) };
        let t = r.range_i64(0, 86_399_999_999);
        let z = (r.next() % 115_200) as i32 - 57_600;
        let zone = Interval { time: zt, day: zd, month: zm };
        let tt = adt_date::TimeTzADT { time: t, zone: z };
        let (mut c_t, mut c_z): (i64, i32) = (0, 0);
        let mut c_err: c_int = 0;
        unsafe { pg_adr_timetz_izone(zt, zd, zm, t, z, &mut c_t, &mut c_z, &mut c_err) };
        let rr = adt_date::timetz_izone(&zone, &tt);
        checks += 1;
        let ok = match &rr {
            Ok(v) => c_err == 0 && v.time == c_t && v.zone == c_z,
            Err(_) => c_err == 2,
        };
        if !ok { fails += 1; }
        report("timetz_izone", ok, format!("zt={zt} zd={zd} zm={zm} t={t} z={z} rust={rr:?} c=({c_t},{c_z})/{c_err}"));
    }

    // ---- 1200 interval_scale random typmod x interval ----
    const MASKS: &[i32] = &[
        0x7FFF, 1 << 1, 1 << 2, 1 << 3, 1 << 10, 1 << 11, 1 << 12,
        (1 << 2) | (1 << 1), (1 << 3) | (1 << 10), (1 << 3) | (1 << 10) | (1 << 11),
        (1 << 3) | (1 << 10) | (1 << 11) | (1 << 12), (1 << 10) | (1 << 11),
        (1 << 10) | (1 << 11) | (1 << 12), (1 << 11) | (1 << 12),
        (1 << 2) | (1 << 3), // unrecognized combo
    ];
    for _ in 0..2_000_000 {
        let range = MASKS[(r.next() % MASKS.len() as u64) as usize];
        let prec = match r.next() % 4 {
            0 => 0xFFFF,
            1 => (r.next() % 7) as i32,
            2 => 7 + (r.next() % 100) as i32,
            _ => -1i32 & 0xFFFF,
        };
        let typmod = if r.next() % 8 == 0 { -1 } else { (range << 16) | (prec & 0xFFFF) };
        let iv = Interval { time: interesting_i64(&mut r), day: r.i32(), month: r.i32() };
        let mut c_res = Interval { time: 0, day: 0, month: 0 };
        let mut c_err: c_int = 0;
        unsafe { pg_adr_interval_scale(iv.time, iv.day, iv.month, typmod, &mut c_res, &mut c_err) };
        let rr = adt_timestamp::interval::interval_scale(&iv, typmod);
        checks += 1;
        let ok = match &rr {
            Ok(v) => c_err == 0 && v.time == c_res.time && v.day == c_res.day && v.month == c_res.month,
            Err(_) => c_err != 0,
        };
        if !ok { fails += 1; }
        report("interval_scale", ok, format!("typmod={typmod:#x} iv=({},{},{}) rust={rr:?} c=({},{},{})/{c_err}", iv.time, iv.day, iv.month, c_res.time, c_res.day, c_res.month));
    }

    // ---- typmodout full sweep + typmod_check sweep ----
    let mut sweep = |istz: bool| {
        for typmod in -70_000i32..70_000 {
            let mut c_buf = [0u8; 64];
            let c_len = unsafe { pg_adr_anytime_typmodout(istz as c_int, typmod, c_buf.as_mut_ptr()) };
            let mut r_buf = [0u8; 64];
            let suffix: &[u8] = if istz { b" with time zone" } else { b" without time zone" };
            let len = adt_timestamp::builtins::typmod_paren_suffix_out(typmod, suffix, &mut r_buf);
            checks += 1;
            let ok = len as c_int == c_len && r_buf[..len] == c_buf[..len];
            if !ok { fails += 1; println!("MISMATCH typmodout typmod={typmod} istz={istz}"); }
        }
    };
    sweep(false);
    sweep(true);
    for typmod in -70_000i32..70_000 {
        let mut c_out: i32 = 0;
        let mut c_err: c_int = 0;
        unsafe { pg_adr_anytime_typmod_check(0, typmod, &mut c_out, &mut c_err) };
        let rr = adt_date::anytime_typmod_check(false, typmod);
        checks += 1;
        let ok = match &rr {
            Ok(v) => c_err == 0 && *v == c_out,
            Err(_) => c_err == 2,
        };
        if !ok { fails += 1; println!("MISMATCH typmod_check typmod={typmod} rust={rr:?} c={c_out}/{c_err}"); }
    }

    // ---- in_range_timetz top-sliver + full sweep (wrapper-level) ----
    {
        use datum::{Datum, NullableDatum};
        use types_fmgr::LocalFcinfo;
        for _ in 0..1_000_000 {
            let vt = r.range_i64(0, 86_400_000_000);
            let bt = r.range_i64(0, 86_400_000_000);
            let vz = (r.next() % 115_200) as i32 - 57_600;
            let bz = (r.next() % 115_200) as i32 - 57_600;
            let ot = match r.next() % 3 {
                0 => r.range_i64(i64::MAX - 150_000_000_000, i64::MAX), // fenced sliver
                1 => r.i64(),
                _ => r.range_i64(0, 200_000_000_000),
            };
            let (od, om) = (r.i32(), r.i32());
            let (sub, less) = (r.next() % 2 == 0, r.next() % 2 == 0);
            let mut cres: c_int = -1;
            let mut cerr: c_int = 0;
            unsafe {
                pg_adr_in_range_timetz_interval(
                    vt, vz, bt, bz, ot, od, om, sub as c_int, less as c_int, &mut cres, &mut cerr,
                )
            };
            let mut vimg = [0u8; 12];
            vimg[..8].copy_from_slice(&vt.to_ne_bytes());
            vimg[8..].copy_from_slice(&vz.to_ne_bytes());
            let mut bimg = [0u8; 12];
            bimg[..8].copy_from_slice(&bt.to_ne_bytes());
            bimg[8..].copy_from_slice(&bz.to_ne_bytes());
            let mut oimg = [0u8; 16];
            oimg[..8].copy_from_slice(&ot.to_ne_bytes());
            oimg[8..12].copy_from_slice(&od.to_ne_bytes());
            oimg[12..].copy_from_slice(&om.to_ne_bytes());
            let mut f = LocalFcinfo::<5>::new(0);
            f.args[0] = NullableDatum { value: Datum::from_usize(vimg.as_ptr() as usize), isnull: false };
            f.args[1] = NullableDatum { value: Datum::from_usize(bimg.as_ptr() as usize), isnull: false };
            f.args[2] = NullableDatum { value: Datum::from_usize(oimg.as_ptr() as usize), isnull: false };
            f.args[3] = NullableDatum { value: Datum::from_bool(sub), isnull: false };
            f.args[4] = NullableDatum { value: Datum::from_bool(less), isnull: false };
            let rr = adt_date::builtins::fc_in_range_timetz_interval(None, &mut f);
            checks += 1;
            let ok = match &rr {
                Ok(d) => cerr == 0 && d.as_bool() as c_int == cres,
                Err(_) => cerr == 3,
            };
            if !ok {
                fails += 1;
                println!("MISMATCH in_range_timetz vt={vt} vz={vz} bt={bt} bz={bz} ot={ot} sub={sub} less={less} c={cres}/{cerr} rust={rr:?}");
            }
        }
    }

    // ---- interval/date/time/timetz recv+send roundtrips ----
    {
        let mut ctx = mcx::MemoryContext::new_bump("native-adr");
        for _ in 0..1_000_000 {
            // random payloads
            let mut p16 = [0u8; 16];
            for b in p16.iter_mut() { *b = r.next() as u8; }
            // bias time field toward valid range half the time
            if r.next() % 2 == 0 {
                let t = r.range_i64(0, 86_400_000_000);
                p16[..8].copy_from_slice(&t.to_be_bytes());
            }
            let typmod = match r.next() % 3 {
                0 => -1,
                1 => ((MASKS[(r.next() % MASKS.len() as u64) as usize]) << 16) | (r.next() % 7) as i32,
                _ => r.i32(),
            };

            // interval_recv (the memory-walled row) with random typmod
            {
                let mut c_res = Interval { time: 0, day: 0, month: 0 };
                let mut c_err: c_int = 0;
                unsafe { pg_adr_interval_recv(p16.as_ptr(), 16, typmod, &mut c_res, &mut c_err) };
                let mut si = stringinfo::StringInfo::with_capacity_in(ctx.mcx(), 32).unwrap();
                si.append_bytes(&p16).unwrap();
                let rr = adt_timestamp::interval::interval_recv(&mut si, typmod);
                checks += 1;
                let ok = match &rr {
                    Ok(v) => c_err == 0 && v.time == c_res.time && v.day == c_res.day && v.month == c_res.month,
                    Err(_) => c_err != 0,
                };
                if !ok {
                    fails += 1;
                    println!("MISMATCH interval_recv payload={p16:?} typmod={typmod:#x} rust={rr:?} c=({},{},{})/{c_err}", c_res.time, c_res.day, c_res.month);
                }
            }
            // date_recv / time_recv / timetz_recv
            {
                let mut c_out: i32 = 0;
                let mut c_err: c_int = 0;
                unsafe { pg_adr_date_recv(p16.as_ptr(), 4, &mut c_out, &mut c_err) };
                let mut si = stringinfo::StringInfo::with_capacity_in(ctx.mcx(), 32).unwrap();
                si.append_bytes(&p16[..4]).unwrap();
                let rr = adt_date::date_recv(&mut si);
                checks += 1;
                let ok = match &rr {
                    Ok(v) => c_err == 0 && *v == c_out,
                    Err(_) => c_err == 1,
                };
                if !ok { fails += 1; println!("MISMATCH date_recv {p16:?} rust={rr:?} c={c_out}/{c_err}"); }
            }
            {
                let mut c_t: i64 = 0;
                let mut c_z: i32 = 0;
                let mut c_err: c_int = 0;
                unsafe { pg_adr_timetz_recv(p16.as_ptr(), 12, -1, &mut c_t, &mut c_z, &mut c_err) };
                let mut si = stringinfo::StringInfo::with_capacity_in(ctx.mcx(), 32).unwrap();
                si.append_bytes(&p16[..12]).unwrap();
                let rr = adt_date::timetz_recv(&mut si, -1);
                checks += 1;
                let ok = match &rr {
                    Ok(v) => c_err == 0 && v.time == c_t && v.zone == c_z,
                    Err(_) => c_err == 1 || c_err == 5,
                };
                if !ok { fails += 1; println!("MISMATCH timetz_recv {p16:?} rust={rr:?} c=({c_t},{c_z})/{c_err}"); }
            }

            // interval_send (the other memory-walled row)
            {
                let (t, d, m) = (interesting_i64(&mut r), r.i32(), r.i32());
                let iv = Interval { time: t, day: d, month: m };
                let mut cbuf = [0u8; 20];
                let clen = unsafe { pg_adr_interval_send(t, d, m, cbuf.as_mut_ptr()) };
                let b = adt_timestamp::interval::interval_send(ctx.mcx(), &iv).unwrap();
                let img = b.as_bytes();
                checks += 1;
                let ok = clen == 20 && img.len() == 20 && img == &cbuf[..];
                if !ok { fails += 1; println!("MISMATCH interval_send iv=({t},{d},{m}) c={cbuf:?} rust={img:?}"); }
            }
            ctx.reset();
        }
    }

    println!("native_diff_adt_date_rem: {checks} checks, {fails} mismatches");
    if fails > 0 {
        std::process::exit(1);
    }
}
