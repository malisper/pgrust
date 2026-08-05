//! Native differential sweep for the five hlp:: divider ladders whose kissat
//! "walls" were retracted under the concealed-FAILED law.
//!
//! `hlp::eq_date2j_full` came back SATISFIABLE (a property VIOLATION) in 1.9ms
//! under cadical where kissat had reported a flat 602s timeout, but CBMC then
//! hangs building the trace, so the counterexample never printed. This binary
//! finds it the cheap way: the same shipped functions against the same
//! vendored REL_18_STABLE C (linked natively via build.rs cc), swept over the
//! danger set the ladders quantify over.
//!
//! Census-grade evidence, weaker than proof — rows record tested(differential).
//!
//! Run: cargo run --release --bin native_diff_hlp_walls

use proof_datetime_b as _;

use adt_datetime::calendar::{date2j, isoweek2j, j2date};
use adt_datetime::decode::dt2time;
use std::os::raw::c_int;

extern "C" {
    fn pg_hlp_date2j(y: c_int, m: c_int, d: c_int) -> c_int;
    fn pg_hlp_j2date(jd: c_int, year: *mut c_int, month: *mut c_int, day: *mut c_int) -> c_int;
    fn pg_hlp_dt2time(
        jd: i64,
        hour: *mut c_int,
        min: *mut c_int,
        sec: *mut c_int,
        fsec: *mut i32,
    ) -> c_int;
    fn pg_hlp_isoweek2j(year: c_int, week: c_int) -> c_int;
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        // splitmix64
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn i32(&mut self) -> i32 {
        self.next() as i32
    }
    fn i64(&mut self) -> i64 {
        self.next() as i64
    }
}

/// Values that break constant-divider chains: sentinels, the exact divisor
/// multiples, and the wrap thresholds the -fwrapv notes name.
const EDGE_I32: &[i32] = &[
    0,
    1,
    -1,
    2,
    -2,
    3,
    4,
    -4,
    99,
    100,
    -100,
    101,
    255,
    256,
    -256,
    257,
    365,
    1461,
    -1461,
    4800,
    4799,
    -4800,
    32167,
    -32167,
    146097,
    274_000,
    274_100,
    274_200, // 7834*month i32 overflow threshold ~274151
    -274_000,
    -274_200,
    i32::MAX,
    i32::MIN,
    i32::MAX - 1,
    i32::MIN + 1,
    i32::MAX / 2,
    i32::MIN / 2,
];

fn main() {
    let mut fails = 0usize;
    let mut checked = 0usize;
    let mut first: Option<String> = None;

    // ---- date2j: full 3-arg edge cross product + random mass sweep ----
    for &y in EDGE_I32 {
        for &m in EDGE_I32 {
            for &d in EDGE_I32 {
                checked += 1;
                let r = date2j(y, m, d);
                let c = unsafe { pg_hlp_date2j(y, m, d) };
                if r != c {
                    fails += 1;
                    if first.is_none() {
                        first = Some(format!(
                            "date2j({y}, {m}, {d}): rust={r} c={c}  (delta {})",
                            r.wrapping_sub(c)
                        ));
                    }
                }
            }
        }
    }
    println!("date2j edge grid: {checked} cells, {fails} mismatches");
    if let Some(ref f) = first {
        println!("  FIRST: {f}");
    }

    let mut rng = Rng(0x1234_5678_9ABC_DEF0);
    let (mut c2, mut f2) = (0usize, 0usize);
    let mut first2: Option<String> = None;
    for _ in 0..3_000_000 {
        let (y, m, d) = (rng.i32(), rng.i32(), rng.i32());
        c2 += 1;
        let r = date2j(y, m, d);
        let c = unsafe { pg_hlp_date2j(y, m, d) };
        if r != c {
            f2 += 1;
            if first2.is_none() {
                first2 = Some(format!("date2j({y}, {m}, {d}): rust={r} c={c}"));
            }
        }
    }
    println!("date2j random: {c2} cells, {f2} mismatches");
    if let Some(ref f) = first2 {
        println!("  FIRST: {f}");
    }

    // ---- j2date: full edge set + random ----
    let (mut c3, mut f3) = (0usize, 0usize);
    let mut first3: Option<String> = None;
    let mut probe_j2date = |jd: i32, c3: &mut usize, f3: &mut usize, first3: &mut Option<String>| {
        *c3 += 1;
        let (mut ry, mut rm, mut rd) = (0i32, 0i32, 0i32);
        j2date(jd, &mut ry, &mut rm, &mut rd);
        let (mut cy, mut cm, mut cd) = (0i32, 0i32, 0i32);
        unsafe { pg_hlp_j2date(jd, &mut cy, &mut cm, &mut cd) };
        if (ry, rm, rd) != (cy, cm, cd) {
            *f3 += 1;
            if first3.is_none() {
                *first3 = Some(format!(
                    "j2date({jd}): rust=({ry},{rm},{rd}) c=({cy},{cm},{cd})"
                ));
            }
        }
    };
    for &jd in EDGE_I32 {
        probe_j2date(jd, &mut c3, &mut f3, &mut first3);
    }
    for _ in 0..3_000_000 {
        let jd = rng.i32();
        probe_j2date(jd, &mut c3, &mut f3, &mut first3);
    }
    println!("j2date edge+random: {c3} cells, {f3} mismatches");
    if let Some(ref f) = first3 {
        println!("  FIRST: {f}");
    }

    // ---- dt2time: i64 divider chain ----
    const EDGE_I64: &[i64] = &[
        0,
        1,
        -1,
        999_999,
        1_000_000,
        -1_000_000,
        59_999_999,
        60_000_000,
        -60_000_000,
        3_599_999_999,
        3_600_000_000,
        -3_600_000_000,
        86_399_999_999,
        86_400_000_000,
        -86_400_000_000,
        i64::MAX,
        i64::MIN,
        i64::MAX - 1,
        i64::MIN + 1,
    ];
    let (mut c4, mut f4) = (0usize, 0usize);
    let mut first4: Option<String> = None;
    let mut probe_dt2time =
        |jd: i64, c4: &mut usize, f4: &mut usize, first4: &mut Option<String>| {
            *c4 += 1;
            let (mut rh, mut rmin, mut rs, mut rf) = (0i32, 0i32, 0i32, 0i32);
            dt2time(jd, &mut rh, &mut rmin, &mut rs, &mut rf);
            let (mut ch, mut cmin, mut cs, mut cf) = (0i32, 0i32, 0i32, 0i32);
            unsafe { pg_hlp_dt2time(jd, &mut ch, &mut cmin, &mut cs, &mut cf) };
            if (rh, rmin, rs, rf) != (ch, cmin, cs, cf) {
                *f4 += 1;
                if first4.is_none() {
                    *first4 = Some(format!(
                        "dt2time({jd}): rust=({rh},{rmin},{rs},{rf}) c=({ch},{cmin},{cs},{cf})"
                    ));
                }
            }
        };
    for &jd in EDGE_I64 {
        probe_dt2time(jd, &mut c4, &mut f4, &mut first4);
    }
    for _ in 0..3_000_000 {
        let jd = rng.i64();
        probe_dt2time(jd, &mut c4, &mut f4, &mut first4);
    }
    println!("dt2time edge+random: {c4} cells, {f4} mismatches");
    if let Some(ref f) = first4 {
        println!("  FIRST: {f}");
    }

    // ---- isoweek2j: the two retracted band ladders ----
    let (mut c5, mut f5) = (0usize, 0usize);
    let mut first5: Option<String> = None;
    for &y in EDGE_I32 {
        for &w in EDGE_I32 {
            c5 += 1;
            let r = isoweek2j(y, w);
            let c = unsafe { pg_hlp_isoweek2j(y, w) };
            if r != c {
                f5 += 1;
                if first5.is_none() {
                    first5 = Some(format!("isoweek2j({y}, {w}): rust={r} c={c}"));
                }
            }
        }
    }
    // the AD band the ad_band ladder masks (1 + raw & 0x007F_FFFF) x full week
    for _ in 0..2_000_000 {
        let year = 1 + ((rng.next() as u32) & 0x007F_FFFF) as i32;
        let week = rng.i32();
        c5 += 1;
        let r = isoweek2j(year, week);
        let c = unsafe { pg_hlp_isoweek2j(year, week) };
        if r != c {
            f5 += 1;
            if first5.is_none() {
                first5 = Some(format!("isoweek2j AD({year}, {week}): rust={r} c={c}"));
            }
        }
    }
    // the BC/zero band the bc_band ladder masks
    for _ in 0..2_000_000 {
        let year = -(((rng.next() as u32) & 0x007F_FFFF) as i32);
        let week = rng.i32();
        c5 += 1;
        let r = isoweek2j(year, week);
        let c = unsafe { pg_hlp_isoweek2j(year, week) };
        if r != c {
            f5 += 1;
            if first5.is_none() {
                first5 = Some(format!("isoweek2j BC({year}, {week}): rust={r} c={c}"));
            }
        }
    }
    println!("isoweek2j edge+AD+BC bands: {c5} cells, {f5} mismatches");
    if let Some(ref f) = first5 {
        println!("  FIRST: {f}");
    }

    let total = fails + f2 + f3 + f4 + f5;
    println!("\nTOTAL mismatches: {total}");
    if total != 0 {
        std::process::exit(1);
    }
}
