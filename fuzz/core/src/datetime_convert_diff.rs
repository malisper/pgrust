//! datetime_convert_diff: differential fuzz driver — shipped Rust `adt_date`
//! timestamp<->date/time/timetz conversions and time/timetz +- interval
//! arithmetic vs vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha
//! 62d6c7d3df) C (csrc/pg_datetime_io_io.c).
//!
//! Crate under test: crates/backend/utils/adt/adt_date. Every compared entry
//! point is defined THERE; the shared kernel `timestamp2tm` lives in
//! adt_timestamp (unclaimed by this lane) and rides along as oracle
//! infrastructure, the same way `interval2itm` does for `interval_engine_diff`.
//!
//! Why a separate target rather than more arms on `datetime_engine_diff` (whose
//! header makes exactly this argument for itself): that target dispatches on
//! `sel % 4` and its banked corpus (7.9k entries, one 10M-exec CI cluster campaign,
//! 0 divergences) is keyed to that modulus. Widening it remaps every banked
//! seed to a different arm, discarding the measured coverage the bank
//! represents — and the abbrev arms below must install a zone-abbreviation
//! table, which would perturb the pinned environment its other arms were
//! cleared under. A new target keeps every existing bank and floor-clean
//! verdict intact; either way the marginal CI cluster cost is one 10M campaign.
//!
//! PINNED ENVIRONMENT (mirrors csrc/pg_datetime_io_io.c — environment, never
//! computation; identical to datetime_io_diff's, which see):
//!   - session timezone = GMT; tz database = {GMT} only (PGRUST_TZDIR points at
//!     a nonexistent directory). `timestamp2tm`'s tzp!=NULL branch crosses the
//!     localtime-library boundary: C answers through the GMT `pg_localtime`
//!     shim, Rust through its REAL pgtz GMT zone, and that equivalence is part
//!     of what these arms fuzz.
//!   - current date/time pinned to 2026-06-15 12:30:45.123456 GMT.
//!   - DateStyle/DateOrder pinned ISO/YMD: no arm here emits or parses text, so
//!     style is not a live variable (unlike the io and engine targets).
//!
//! Comparison planes: the returned scalar (date i32 / time i64 / timestamptz
//! i64 / timetz (i64, i32) staged FIELD BY FIELD), the SQL-NULL flag (three of
//! these entry points `PG_RETURN_NULL()` on non-finite input), the
//! error-verdict, and the errcode class. Message text is out of scope.
//!
//! DOMAIN FENCES (each matches a C-side contract or C-side undefined
//! behavior — nothing real PostgreSQL can produce is fenced out):
//!   - `fold_ts`: timestamps folded into `IS_VALID_TIMESTAMP` or a not-finite
//!     sentinel. Out-of-range timestamp datums are unreachable through SQL (the
//!     on-disk invariant every timestamp_in enforces) and drive `timestamp2tm`
//!     into `date > INT_MAX` / j2date on a negative Julian day.
//!   - `fold_date`: dates folded into `IS_VALID_DATE` or a not-finite sentinel,
//!     the same fence `datetime_io_diff` arm 1 (date_out) already documents.
//!     Raw i32 dates overflow `dateVal * USECS_PER_DAY` (i64) in
//!     `date2timestamptz_opt_overflow`, where C relies on -fwrapv.
//!   - `fold_time` / `fold_zone`: time folded into 0..=USECS_PER_DAY and
//!     |zone| < 16h, the TimeADT/TimeTzADT on-disk invariants.
//! The Interval payload is deliberately NOT fenced: the +- interval entry
//! points must accept every i64 usec / i32 day / i32 month bit pattern, which
//! is exactly where this crate's -fwrapv defect family lives.
//!
//! Input layout: [selector][payload]; selector % 5 picks the arm:
//!   0 timestamp_date (2029) + timestamp_time (1316)        — [ts i64]
//!   1 timestamptz_date (1178) + timestamptz_time (2019)
//!       + timestamptz_timetz (1388)                        — [ts i64]
//!   2 date_timestamptz (1174)                              — [date i32]
//!   3 interval_time (1419) + time_pl_interval (1747)
//!       + time_mi_interval (1748)                          — [time i64][span]
//!   4 timetz_pl_interval (1749) + timetz_mi_interval (1750)
//!                                            — [time i64][zone i32][span]

use adt_date::{
    date2timestamptz, interval_time, time_mi_interval, time_pl_interval, timestamp_date,
    timestamp_time, timestamptz_date, timestamptz_time, timestamptz_timetz, timetz_mi_interval,
    timetz_pl_interval, DateADT, TimeADT, TimeTzADT,
};
use adt_datetime::Interval;
use types_error::PgError;

extern "C" {
    fn pg_diff_timestamp_date(ts: i64, out: *mut i32) -> i32;
    fn pg_diff_timestamptz_date(ts: i64, out: *mut i32) -> i32;
    fn pg_diff_timestamp_time(ts: i64, out: *mut i64) -> i32;
    fn pg_diff_timestamptz_time(ts: i64, out: *mut i64) -> i32;
    fn pg_diff_timestamptz_timetz(ts: i64, out_time: *mut i64, out_zone: *mut i32) -> i32;
    fn pg_diff_date_timestamptz(date: i32, out: *mut i64) -> i32;
    fn pg_diff_interval_time(time: i64, day: i32, month: i32, out: *mut i64) -> i32;
    fn pg_diff_time_pm_interval(
        sub: i32,
        time: i64,
        sp_time: i64,
        sp_day: i32,
        sp_month: i32,
        out: *mut i64,
    ) -> i32;
    fn pg_diff_timetz_pm_interval(
        sub: i32,
        time: i64,
        zone: i32,
        sp_time: i64,
        sp_day: i32,
        sp_month: i32,
        out_time: *mut i64,
        out_zone: *mut i32,
    ) -> i32;
}

/// The C entries' "returned SQL NULL" code (csrc/pg_datetime_io_io.c
/// `PG_DT_NULLED`); anything else nonzero is an errcode class.
const NULLED: i32 = 1;

const USECS_PER_DAY: i64 = 86_400_000_000;

fn i32_at(b: &[u8], o: usize) -> i32 {
    i32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}

fn i64_at(b: &[u8], o: usize) -> i64 {
    i64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}

/// Timestamp datum fence (see header): the two not-finite sentinels stay in the
/// domain (they select the early-return arms), everything else is folded into
/// `IS_VALID_TIMESTAMP`.
fn fold_ts(raw: i64) -> i64 {
    const MIN_TIMESTAMP: i64 = -211_813_488_000_000_000;
    const END_TIMESTAMP: i64 = 9_223_371_331_200_000_000;
    match raw {
        i64::MIN => i64::MIN, // -infinity
        i64::MAX => i64::MAX, // +infinity
        v => {
            // width computed unsigned: the range straddles zero and is wider
            // than i64 can hold as a difference
            let span = (END_TIMESTAMP as i128 - MIN_TIMESTAMP as i128) as u128;
            (MIN_TIMESTAMP as i128 + ((v as u64 as u128) % span) as i128) as i64
        }
    }
}

/// Date datum fence (see header), matching `datetime_io_diff` arm 1.
fn fold_date(raw: i32) -> DateADT {
    const MIN_DATE: i32 = -2_451_545; // DATETIME_MIN_JULIAN - POSTGRES_EPOCH_JDATE
    const END_DATE: i32 = 2_147_483_494 - 2_451_545 + 1; // DATE_END_JULIAN - epoch
    match raw {
        i32::MIN => i32::MIN, // -infinity
        i32::MAX => i32::MAX, // +infinity
        v => {
            let span = (END_DATE as i64 - MIN_DATE as i64) as u64;
            (MIN_DATE as i64 + ((v as u32 as u64) % span) as i64) as i32
        }
    }
}

/// TimeADT on-disk invariant: 0 <= time <= USECS_PER_DAY.
fn fold_time(raw: i64) -> TimeADT {
    (raw as u64 % (USECS_PER_DAY as u64 + 1)) as i64
}

/// TimeTzADT on-disk invariant: |zone| < 16 hours (in seconds, west-positive).
fn fold_zone(raw: i32) -> i32 {
    const LIM: i32 = 16 * 60 * 60;
    raw.rem_euclid(2 * LIM) - LIM
}

/// C oracle errcode classes (csrc/pg_datetime_io_io.c header). Every entry
/// point here reports only 22008.
fn rust_err_class(e: &PgError) -> i32 {
    use types_error::*;
    if e.sqlstate == ERRCODE_INVALID_DATETIME_FORMAT {
        1
    } else if e.sqlstate == ERRCODE_DATETIME_FIELD_OVERFLOW
        || e.sqlstate == ERRCODE_DATETIME_VALUE_OUT_OF_RANGE
    {
        2
    } else if e.sqlstate == ERRCODE_FEATURE_NOT_SUPPORTED {
        3
    } else {
        9
    }
}

/// Fold a `PgResult<Option<T>>` (the PG_RETURN_NULL-capable entry points) into
/// the C entries' (code, value) convention.
fn class_opt<T>(r: &Result<Option<T>, Box<PgError>>) -> i32 {
    match r {
        Ok(None) => NULLED,
        Ok(Some(_)) => 0,
        Err(e) => rust_err_class(e),
    }
}

fn class<T>(r: &Result<T, Box<PgError>>) -> i32 {
    match r {
        Ok(_) => 0,
        Err(e) => rust_err_class(e),
    }
}

pub fn datetime_convert_diff(data: &[u8]) {
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    super::datetime_io_diff::init_env_for_siblings();
    match sel % 5 {
        0 => timestamp_to_date_time(payload),
        1 => timestamptz_to_date_time_timetz(payload),
        2 => date_to_timestamptz(payload),
        3 => time_interval_arith(payload),
        _ => timetz_interval_arith(payload),
    }
}

/// Arm 0: timestamp_date / timestamp_time (no zone: `timestamp2tm(tzp=NULL)`).
fn timestamp_to_date_time(payload: &[u8]) {
    if payload.len() < 8 {
        return;
    }
    let ts = fold_ts(i64_at(payload, 0));

    let mut cdate = 0i32;
    let cdrc = unsafe { pg_diff_timestamp_date(ts, &mut cdate) };
    let rd = timestamp_date(ts);
    let rdc = class(&rd);
    assert!(
        cdrc == rdc,
        "timestamp_date VERDICT DIVERGENCE ts={ts}: C={cdrc} Rust={rdc} ({:?})",
        rd.as_ref().err().map(|e| e.sqlstate)
    );
    if cdrc == 0 {
        let rv = *rd.as_ref().unwrap();
        assert!(
            cdate == rv,
            "timestamp_date VALUE DIVERGENCE ts={ts}: C={cdate} Rust={rv}"
        );
    }

    let mut ctime = 0i64;
    let ctrc = unsafe { pg_diff_timestamp_time(ts, &mut ctime) };
    let rt = timestamp_time(ts);
    let rtc = class_opt(&rt);
    assert!(
        ctrc == rtc,
        "timestamp_time VERDICT DIVERGENCE ts={ts}: C={ctrc} Rust={rtc} ({:?})",
        rt.as_ref().err().map(|e| e.sqlstate)
    );
    if ctrc == 0 {
        let rv = rt.as_ref().unwrap().unwrap();
        assert!(
            ctime == rv,
            "timestamp_time VALUE DIVERGENCE ts={ts}: C={ctime} Rust={rv}"
        );
    }
}

/// Arm 1: timestamptz_date / timestamptz_time / timestamptz_timetz — the
/// `timestamp2tm(tzp=&tz)` face, which crosses the GMT localtime seam.
fn timestamptz_to_date_time_timetz(payload: &[u8]) {
    if payload.len() < 8 {
        return;
    }
    let ts = fold_ts(i64_at(payload, 0));

    let mut cdate = 0i32;
    let cdrc = unsafe { pg_diff_timestamptz_date(ts, &mut cdate) };
    let rd = timestamptz_date(ts);
    let rdc = class(&rd);
    assert!(
        cdrc == rdc,
        "timestamptz_date VERDICT DIVERGENCE ts={ts}: C={cdrc} Rust={rdc} ({:?})",
        rd.as_ref().err().map(|e| e.sqlstate)
    );
    if cdrc == 0 {
        let rv = *rd.as_ref().unwrap();
        assert!(
            cdate == rv,
            "timestamptz_date VALUE DIVERGENCE ts={ts}: C={cdate} Rust={rv}"
        );
    }

    let mut ctime = 0i64;
    let ctrc = unsafe { pg_diff_timestamptz_time(ts, &mut ctime) };
    let rt = timestamptz_time(ts);
    let rtc = class_opt(&rt);
    assert!(
        ctrc == rtc,
        "timestamptz_time VERDICT DIVERGENCE ts={ts}: C={ctrc} Rust={rtc} ({:?})",
        rt.as_ref().err().map(|e| e.sqlstate)
    );
    if ctrc == 0 {
        let rv = rt.as_ref().unwrap().unwrap();
        assert!(
            ctime == rv,
            "timestamptz_time VALUE DIVERGENCE ts={ts}: C={ctime} Rust={rv}"
        );
    }

    let mut cttime = 0i64;
    let mut ctzone = 0i32;
    let cttrc = unsafe { pg_diff_timestamptz_timetz(ts, &mut cttime, &mut ctzone) };
    let rtt = timestamptz_timetz(ts);
    let rttc = class_opt(&rtt);
    assert!(
        cttrc == rttc,
        "timestamptz_timetz VERDICT DIVERGENCE ts={ts}: C={cttrc} Rust={rttc} ({:?})",
        rtt.as_ref().err().map(|e| e.sqlstate)
    );
    if cttrc == 0 {
        let rv = rtt.as_ref().unwrap().as_ref().unwrap();
        assert!(
            cttime == rv.time && ctzone == rv.zone,
            "timestamptz_timetz VALUE DIVERGENCE ts={ts}: \
             C=(time {cttime} zone {ctzone}) Rust=(time {} zone {})",
            rv.time,
            rv.zone
        );
    }
}

/// Arm 2: date_timestamptz — `DetermineTimeZoneOffset` over the pinned GMT
/// zone, then the `dateVal * USECS_PER_DAY + tz * USECS_PER_SEC` datum build.
fn date_to_timestamptz(payload: &[u8]) {
    if payload.len() < 4 {
        return;
    }
    let date = fold_date(i32_at(payload, 0));

    let mut cts = 0i64;
    let crc = unsafe { pg_diff_date_timestamptz(date, &mut cts) };
    let r = date2timestamptz(date);
    let rc = class(&r);
    assert!(
        crc == rc,
        "date_timestamptz VERDICT DIVERGENCE date={date}: C={crc} Rust={rc} ({:?})",
        r.as_ref().err().map(|e| e.sqlstate)
    );
    if crc == 0 {
        let rv = *r.as_ref().unwrap();
        assert!(
            cts == rv,
            "date_timestamptz VALUE DIVERGENCE date={date}: C={cts} Rust={rv}"
        );
    }
}

/// Arm 3: interval_time / time_pl_interval / time_mi_interval. The Interval is
/// staged field by field and deliberately UNFENCED (see header).
fn time_interval_arith(payload: &[u8]) {
    if payload.len() < 24 {
        return;
    }
    let time = fold_time(i64_at(payload, 0));
    let sp_time = i64_at(payload, 8);
    let sp_day = i32_at(payload, 16);
    let sp_month = i32_at(payload, 20);
    let span = Interval { time: sp_time, day: sp_day, month: sp_month };

    let mut cout = 0i64;
    let crc = unsafe { pg_diff_interval_time(sp_time, sp_day, sp_month, &mut cout) };
    let r = interval_time(&span);
    let rc = class(&r);
    assert!(
        crc == rc,
        "interval_time VERDICT DIVERGENCE span=(t {sp_time} d {sp_day} m {sp_month}): \
         C={crc} Rust={rc} ({:?})",
        r.as_ref().err().map(|e| e.sqlstate)
    );
    if crc == 0 {
        let rv = *r.as_ref().unwrap();
        assert!(
            cout == rv,
            "interval_time VALUE DIVERGENCE span=(t {sp_time} d {sp_day} m {sp_month}): \
             C={cout} Rust={rv}"
        );
    }

    for sub in [0i32, 1i32] {
        let mut c = 0i64;
        let crc = unsafe { pg_diff_time_pm_interval(sub, time, sp_time, sp_day, sp_month, &mut c) };
        let r = if sub == 0 {
            time_pl_interval(time, &span)
        } else {
            time_mi_interval(time, &span)
        };
        let rc = class(&r);
        let nm = if sub == 0 { "time_pl_interval" } else { "time_mi_interval" };
        assert!(
            crc == rc,
            "{nm} VERDICT DIVERGENCE time={time} span=(t {sp_time} d {sp_day} m {sp_month}): \
             C={crc} Rust={rc} ({:?})",
            r.as_ref().err().map(|e| e.sqlstate)
        );
        if crc == 0 {
            let rv = *r.as_ref().unwrap();
            assert!(
                c == rv,
                "{nm} VALUE DIVERGENCE time={time} \
                 span=(t {sp_time} d {sp_day} m {sp_month}): C={c} Rust={rv}"
            );
        }
    }
}

/// Arm 4: timetz_pl_interval / timetz_mi_interval (zone passthrough plane).
fn timetz_interval_arith(payload: &[u8]) {
    if payload.len() < 28 {
        return;
    }
    let time = fold_time(i64_at(payload, 0));
    let zone = fold_zone(i32_at(payload, 8));
    let sp_time = i64_at(payload, 12);
    let sp_day = i32_at(payload, 20);
    let sp_month = i32_at(payload, 24);
    let span = Interval { time: sp_time, day: sp_day, month: sp_month };
    let arg = TimeTzADT { time, zone };

    for sub in [0i32, 1i32] {
        let mut ct = 0i64;
        let mut cz = 0i32;
        let crc = unsafe {
            pg_diff_timetz_pm_interval(
                sub, time, zone, sp_time, sp_day, sp_month, &mut ct, &mut cz,
            )
        };
        let r = if sub == 0 {
            timetz_pl_interval(&arg, &span)
        } else {
            timetz_mi_interval(&arg, &span)
        };
        let rc = class(&r);
        let nm = if sub == 0 { "timetz_pl_interval" } else { "timetz_mi_interval" };
        assert!(
            crc == rc,
            "{nm} VERDICT DIVERGENCE timetz=(t {time} z {zone}) \
             span=(t {sp_time} d {sp_day} m {sp_month}): C={crc} Rust={rc} ({:?})",
            r.as_ref().err().map(|e| e.sqlstate)
        );
        if crc == 0 {
            let rv = r.as_ref().unwrap();
            assert!(
                ct == rv.time && cz == rv.zone,
                "{nm} VALUE DIVERGENCE timetz=(t {time} z {zone}) \
                 span=(t {sp_time} d {sp_day} m {sp_month}): \
                 C=(t {ct} z {cz}) Rust=(t {} z {})",
                rv.time,
                rv.zone
            );
        }
    }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/datetime_convert_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/datetime_convert_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                datetime_convert_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 500, "expected >=500 seeds, found {n}");
    }

    fn ts_arm(arm: u8, ts: i64) -> Vec<u8> {
        let mut v = vec![arm];
        v.extend_from_slice(&ts.to_le_bytes());
        v
    }

    fn iv_arm(time: i64, sp_time: i64, sp_day: i32, sp_month: i32) -> Vec<u8> {
        let mut v = vec![3u8];
        v.extend_from_slice(&time.to_le_bytes());
        v.extend_from_slice(&sp_time.to_le_bytes());
        v.extend_from_slice(&sp_day.to_le_bytes());
        v.extend_from_slice(&sp_month.to_le_bytes());
        v
    }

    fn tz_arm(time: i64, zone: i32, sp_time: i64, sp_day: i32, sp_month: i32) -> Vec<u8> {
        let mut v = vec![4u8];
        v.extend_from_slice(&time.to_le_bytes());
        v.extend_from_slice(&zone.to_le_bytes());
        v.extend_from_slice(&sp_time.to_le_bytes());
        v.extend_from_slice(&sp_day.to_le_bytes());
        v.extend_from_slice(&sp_month.to_le_bytes());
        v
    }

    const TS_GRID: [i64; 10] = [
        0,
        1,
        -1,
        i64::MIN,
        i64::MAX,
        -211_813_488_000_000_000,
        9_223_371_331_200_000_000 - 1,
        86_400_000_000,
        -86_400_000_000,
        -946_684_800_000_000,
    ];

    #[test]
    fn arms_smoke_timestamp_conversions() {
        for arm in [0u8, 1u8] {
            for ts in TS_GRID {
                datetime_convert_diff(&ts_arm(arm, ts));
            }
            // and a sweep so the fold covers the whole valid range
            for k in 0..2000i64 {
                datetime_convert_diff(&ts_arm(arm, k.wrapping_mul(1_000_000_007)));
            }
        }
    }

    #[test]
    fn arms_smoke_date_timestamptz() {
        for d in [
            0i32,
            1,
            -1,
            i32::MIN,
            i32::MAX,
            -2_451_545,
            2_147_483_494 - 2_451_545,
            10_957,
        ] {
            let mut v = vec![2u8];
            v.extend_from_slice(&d.to_le_bytes());
            datetime_convert_diff(&v);
        }
        for k in 0..2000i32 {
            let mut v = vec![2u8];
            v.extend_from_slice(&k.wrapping_mul(1_000_003).to_le_bytes());
            datetime_convert_diff(&v);
        }
    }

    #[test]
    fn arms_smoke_interval_arith() {
        const IVS: [(i64, i32, i32); 8] = [
            (0, 0, 0),
            (1, 0, 0),
            (-1, 0, 0),
            (86_400_000_000, 0, 0),
            (-86_400_000_000, 0, 0),
            (i64::MAX, 0, 0),
            (i64::MIN, 0, 0),
            (i64::MIN, i32::MIN, i32::MIN),
        ];
        for t in [0i64, 1, 86_400_000_000, 43_200_000_000] {
            for (it, idy, imo) in IVS {
                datetime_convert_diff(&iv_arm(t, it, idy, imo));
                for z in [0i32, 3600, -3600, 57_599, -57_599] {
                    datetime_convert_diff(&tz_arm(t, z, it, idy, imo));
                }
            }
        }
    }

    /// The arms must actually REACH their compared entry points. A driver whose
    /// payload-length guard is wrong returns early on every input and would
    /// otherwise pass every test above vacuously (the campaign's dead-arm
    /// class). Each case here is one whose C verdict is known non-trivially.
    #[test]
    fn arms_are_not_vacuous() {
        super::super::datetime_io_diff::init_env_for_siblings();

        // timestamp_time on +infinity returns SQL NULL, on 0 a real time.
        let mut out = 0i64;
        assert_eq!(unsafe { pg_diff_timestamp_time(i64::MAX, &mut out) }, NULLED);
        assert_eq!(unsafe { pg_diff_timestamp_time(0, &mut out) }, 0);
        assert_eq!(out, 0, "postgres epoch is midnight");

        // timestamp_date on the postgres epoch is date 0.
        let mut d = 0i32;
        assert_eq!(unsafe { pg_diff_timestamp_date(0, &mut d) }, 0);
        assert_eq!(d, 0);

        // the tz face resolves through the GMT localtime seam: same answer.
        assert_eq!(unsafe { pg_diff_timestamptz_date(0, &mut d) }, 0);
        assert_eq!(d, 0);
        let mut tt = 0i64;
        let mut tzn = 0i32;
        assert_eq!(unsafe { pg_diff_timestamptz_timetz(0, &mut tt, &mut tzn) }, 0);
        assert_eq!((tt, tzn), (0, 0), "GMT: midnight at offset 0");

        // date_timestamptz(0) == 0 usec; the overflow arm ereports 22008.
        let mut ts = 0i64;
        assert_eq!(unsafe { pg_diff_date_timestamptz(0, &mut ts) }, 0);
        assert_eq!(ts, 0);
        assert_eq!(
            unsafe { pg_diff_date_timestamptz(2_147_483_494 - 2_451_545, &mut ts) },
            2,
            "DATE_END_JULIAN overflow arm is 22008"
        );

        // interval_time wraps negatives into the day; the not-finite sentinel
        // ereports.
        assert_eq!(unsafe { pg_diff_interval_time(-1, 0, 0, &mut out) }, 0);
        assert_eq!(out, 86_400_000_000 - 1, "'-1 usec' -> 23:59:59.999999");
        assert_eq!(
            unsafe { pg_diff_interval_time(i64::MIN, i32::MIN, i32::MIN, &mut out) },
            2,
            "INTERVAL_NOT_FINITE arm is 22008"
        );

        // time +- interval folds back into the day on both sides.
        assert_eq!(
            unsafe { pg_diff_time_pm_interval(0, 0, 86_400_000_000, 0, 0, &mut out) },
            0
        );
        assert_eq!(out, 0, "+1 day on midnight is midnight");
        assert_eq!(unsafe { pg_diff_time_pm_interval(1, 0, 1, 0, 0, &mut out) }, 0);
        assert_eq!(out, 86_400_000_000 - 1, "midnight minus 1 usec wraps");

        // timetz passes its zone through untouched.
        assert_eq!(
            unsafe { pg_diff_timetz_pm_interval(0, 0, 3600, 1, 0, 0, &mut tt, &mut tzn) },
            0
        );
        assert_eq!((tt, tzn), (1, 3600), "zone is passthrough");
    }
}
