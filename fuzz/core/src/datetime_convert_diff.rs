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
//! `sel % 4` and its banked corpus (7.9k entries, one 10M-exec fleet campaign,
//! 0 divergences) is keyed to that modulus. Widening it remaps every banked
//! seed to a different arm, discarding the measured coverage the bank
//! represents — and the abbrev arms below must install a zone-abbreviation
//! table, which would perturb the pinned environment its other arms were
//! cleared under. A new target keeps every existing bank and floor-clean
//! verdict intact; either way the marginal fleet cost is one 10M campaign.
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
//!   5 DecodeTimezoneAbbrev       — [token bytes]   (pinned abbrev table)
//!   6 DecodeTimezoneAbbrevPrefix — [string bytes]  (pinned abbrev table)
//!
//! PINNED ABBREVIATION TABLE (arms 5-6 only): the io target never installs a
//! `zoneabbrevtbl`, which is why decode.rs 250-313 reads zero hits across all
//! three floor-clean targets. These two arms install one — through
//! PostgreSQL's OWN `ConvertTimeZoneAbbrevs` + `InstallTimeZoneAbbrevs` on the
//! C side (vendored verbatim, so neither side hand-rolls the
//! TimeZoneAbbrevTable layout or the DYNTZ value-is-a-byte-offset encoding) and
//! the shipped Rust equivalents on ours, from the SAME entry list. The list
//! spans positive/negative/zero fixed offsets, a DTZ (is_dst) entry, an abbrev
//! of exactly TOKMAXLEN so the full-width NUL-terminated token path is
//! witnessed, and ONE DYNTZ
//! entry whose zone is "GMT" — which keeps the DYNTZ branch
//! (FetchDynamicTimeZone -> pg_tzset) INSIDE the compared domain, GMT being the
//! one name the pinned tz database admits. A DYNTZ entry naming a tzdata zone
//! would leave the domain through the pg_tzset carve, so none is included.
//! The pg_tz pointer itself is not comparable across implementations, so its
//! plane is "did it resolve to a zone" (have_tz), alongside ftype and offset.

use adt_date::{
    date2timestamptz, interval_time, time_mi_interval, time_pl_interval, timestamp_date,
    timestamp_time, timestamptz_date, timestamptz_time, timestamptz_timetz, timetz_mi_interval,
    timetz_pl_interval, DateADT, TimeADT, TimeTzADT,
};
use adt_datetime::tz::{ConvertTimeZoneAbbrevs, InstallTimeZoneAbbrevs, PgTz, TzEntry};
use adt_datetime::{DateTimeErrorExtra, DecodeTimezoneAbbrev, DecodeTimezoneAbbrevPrefix, Interval};
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
    fn pg_diff_decode_timezone_abbrev(
        tok: *const u8,
        toklen: i32,
        ftype: *mut i32,
        offset: *mut i32,
        have_tz: *mut i32,
    ) -> i32;
    fn pg_diff_decode_timezone_abbrev_prefix(
        s: *const u8,
        len: i32,
        offset: *mut i32,
        have_tz: *mut i32,
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
pub(crate) fn fold_ts(raw: i64) -> i64 {
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
pub(crate) fn fold_date(raw: i32) -> DateADT {
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
pub(crate) fn fold_time(raw: i64) -> TimeADT {
    (raw as u64 % (USECS_PER_DAY as u64 + 1)) as i64
}

/// TimeTzADT on-disk invariant: |zone| < 16 hours (in seconds, west-positive).
pub(crate) fn fold_zone(raw: i32) -> i32 {
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
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    super::datetime_io_diff::init_env_for_siblings();
    match sel % 7 {
        0 => timestamp_to_date_time(payload),
        1 => timestamptz_to_date_time_timetz(payload),
        2 => date_to_timestamptz(payload),
        3 => time_interval_arith(payload),
        4 => timetz_interval_arith(payload),
        5 => decode_tz_abbrev(payload),
        _ => decode_tz_abbrev_prefix(payload),
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

/// The pinned abbreviation entries (see header), byte-identical to the C
/// oracle's `pg_dt_pinned_abbrevs`. Sorted by strcmp, as CheckDateTokenTable
/// requires.
///
/// No abbrev exceeds TOKMAXLEN, because `ConvertTimeZoneAbbrevs` has that as a
/// PRECONDITION its real caller enforces: tzparser.c:59 rejects a longer
/// abbreviation ("time zone abbreviation %s is too long") before the table
/// builder ever sees it. An over-long entry produced a C-vs-Rust length
/// mismatch here (C missed the lookup where pgrust matched) purely because it
/// is out of contract on both sides — not a product defect.
fn pinned_abbrev_entries() -> Vec<TzEntry<'static>> {
    vec![
        TzEntry { abbrev: b"aaa", zone: None, offset: -43200, is_dst: false },
        TzEntry { abbrev: b"bbb", zone: None, offset: 0, is_dst: true },
        TzEntry { abbrev: b"ccc", zone: None, offset: 3600, is_dst: false },
        TzEntry { abbrev: b"dddddddddd", zone: None, offset: 50400, is_dst: true },
        TzEntry { abbrev: b"eee", zone: None, offset: -1, is_dst: false },
        TzEntry { abbrev: b"gmtdyn", zone: Some(b"GMT"), offset: 0, is_dst: false },
        TzEntry { abbrev: b"zzz", zone: None, offset: 57599, is_dst: false },
    ]
}

/// Install the pinned table once per thread (C: `pg_dt_install_pinned_abbrevs`).
/// The table must outlive the installing exec, exactly as the real GUC extra
/// does, so it is leaked on both sides rather than arena-allocated.
fn install_pinned_abbrevs() {
    std::thread_local! {
        static DONE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
    DONE.with(|d| {
        if !d.get() {
            let entries = pinned_abbrev_entries();
            let tbl = ConvertTimeZoneAbbrevs(&entries);
            InstallTimeZoneAbbrevs(tbl);
            d.set(true);
        }
    });
}

/// Tokens are downcased and capped at TOKMAXLEN by both sides (the C entry
/// aborts above it as a driver contract, mirroring DecodeTimezoneAbbrev's own
/// callers, which only ever hand it a datetkn-sized token).
const TOKMAXLEN: usize = 10;

/// Arm 5: DecodeTimezoneAbbrev over the pinned table.
fn decode_tz_abbrev(payload: &[u8]) {
    if payload.is_empty() {
        return;
    }
    install_pinned_abbrevs();

    // PLUMBING FENCE (not a behavior carve): the C entry hands the vendored
    // code a NUL-TERMINATED lowtoken, so an embedded NUL truncates its view
    // while a Rust slice would keep the padded tail — the two sides would be
    // given different logical tokens. Truncate at the first NUL so they agree.
    // Nothing observable is fenced out: DecodeTimezoneAbbrev's only callers
    // pass ParseDateTime's `field[i]`, which are exact-length slices of token
    // runs (decode.rs:583) and are NUL-free by construction, so a NUL-padded
    // token is unreachable through SQL.
    let stop = payload.iter().position(|&c| c == 0).unwrap_or(payload.len());
    let n = stop.min(TOKMAXLEN);
    if n == 0 {
        return;
    }
    let low: Vec<u8> = payload[..n].iter().map(|c| c.to_ascii_lowercase()).collect();

    let (mut cft, mut coff, mut chave) = (0i32, 0i32, 0i32);
    let crc = unsafe {
        pg_diff_decode_timezone_abbrev(low.as_ptr(), n as i32, &mut cft, &mut coff, &mut chave)
    };

    let (mut rft, mut roff) = (0i32, 0i32);
    let mut rtz: Option<&'static PgTz> = None;
    let mut extra = DateTimeErrorExtra::default();
    let rrc = DecodeTimezoneAbbrev(0, &low, &mut rft, &mut roff, &mut rtz, &mut extra);
    let rhave = i32::from(rtz.is_some());

    assert!(
        crc == rrc,
        "DecodeTimezoneAbbrev DTERR DIVERGENCE tok={:?}: C={crc} Rust={rrc}",
        String::from_utf8_lossy(&low)
    );
    // planes are only defined when the call succeeded
    if crc == 0 {
        assert!(
            cft == rft && coff == roff && chave == rhave,
            "DecodeTimezoneAbbrev PLANE DIVERGENCE tok={:?}: \
             C=(ftype {cft} offset {coff} have_tz {chave}) \
             Rust=(ftype {rft} offset {roff} have_tz {rhave})",
            String::from_utf8_lossy(&low)
        );
    }
}

/// Arm 6: DecodeTimezoneAbbrevPrefix (longest-prefix match) over the same table.
fn decode_tz_abbrev_prefix(payload: &[u8]) {
    if payload.is_empty() || payload.len() > 63 {
        return;
    }
    install_pinned_abbrevs();

    // C reads a NUL-terminated buffer, so an embedded NUL would truncate its
    // view while Rust's slice keeps the tail: strip them so both sides see the
    // same string (a plumbing fence, not a behavior carve).
    let buf: Vec<u8> = payload.iter().copied().filter(|&c| c != 0).collect();
    if buf.is_empty() {
        return;
    }

    let (mut coff, mut chave) = (0i32, 0i32);
    let crc = unsafe {
        pg_diff_decode_timezone_abbrev_prefix(
            buf.as_ptr(),
            buf.len() as i32,
            &mut coff,
            &mut chave,
        )
    };

    let mut roff = 0i32;
    let mut rtz: Option<&'static PgTz> = None;
    let rrc = DecodeTimezoneAbbrevPrefix(&buf, &mut roff, &mut rtz);
    let rhave = i32::from(rtz.is_some());

    assert!(
        crc == rrc,
        "DecodeTimezoneAbbrevPrefix LENGTH DIVERGENCE str={:?}: C={crc} Rust={rrc}",
        String::from_utf8_lossy(&buf)
    );
    if crc > 0 {
        assert!(
            coff == roff && chave == rhave,
            "DecodeTimezoneAbbrevPrefix PLANE DIVERGENCE str={:?}: \
             C=(offset {coff} have_tz {chave}) Rust=(offset {roff} have_tz {rhave})",
            String::from_utf8_lossy(&buf)
        );
    }
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_corpus_replays_clean() {
        let _serial = crate::c_oracle_serial();
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
        let _serial = crate::c_oracle_serial();
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
        let _serial = crate::c_oracle_serial();
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
        let _serial = crate::c_oracle_serial();
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
        let _serial = crate::c_oracle_serial();
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

    fn abbrev_arm(tok: &[u8]) -> Vec<u8> {
        let mut v = vec![5u8];
        v.extend_from_slice(tok);
        v
    }

    fn prefix_arm(s: &[u8]) -> Vec<u8> {
        let mut v = vec![6u8];
        v.extend_from_slice(s);
        v
    }

    #[test]
    fn arms_smoke_tz_abbrev() {
        let _serial = crate::c_oracle_serial();
        for tok in [
            &b"aaa"[..],
            b"bbb",
            b"ccc",
            b"dddddddddd",
            b"eee",
            b"gmtdyn",
            b"zzz",
            b"AAA",
            b"GmtDyn",
            b"aa",
            b"aaaa",
            b"gmt",
            b"gmtdy",
            b"zz",
            b"a",
            b"\xff\xfe",
        ] {
            datetime_convert_diff(&abbrev_arm(tok));
            datetime_convert_diff(&prefix_arm(tok));
        }
        for tail in [&b"+05"[..], b"-1", b"x", b"0", b" rest", b"aaa", b""] {
            for tok in [&b"aaa"[..], b"gmtdyn", b"dddddddddd", b"zzz"] {
                let mut v = tok.to_vec();
                v.extend_from_slice(tail);
                datetime_convert_diff(&prefix_arm(&v));
            }
        }
        // byte sweep so the non-alphabetic and case paths are all hit
        for b in 0u8..=255 {
            datetime_convert_diff(&abbrev_arm(&[b, b'a', b'a']));
            datetime_convert_diff(&prefix_arm(&[b, b'a', b'a', b'a']));
        }
    }

    /// The pinned abbrev table must actually be INSTALLED and consulted: with
    /// zoneabbrevtbl NULL (the io target's environment) every lookup misses and
    /// both arms would agree vacuously on UNKNOWN_FIELD forever — which is the
    /// state that left decode.rs 250-313 at zero hits in the first place.
    #[test]
    fn tz_abbrev_arms_are_not_vacuous() {
        let _serial = crate::c_oracle_serial();
        super::super::datetime_io_diff::init_env_for_siblings();
        install_pinned_abbrevs();

        // a fixed-offset TZ entry resolves with its offset and no zone
        let (mut ft, mut off, mut have) = (0i32, 0i32, 0i32);
        let rc = unsafe {
            pg_diff_decode_timezone_abbrev(b"ccc".as_ptr(), 3, &mut ft, &mut off, &mut have)
        };
        assert_eq!(rc, 0, "pinned abbrev 'ccc' must resolve");
        assert_eq!((off, have), (3600, 0), "fixed offset, no dynamic zone");

        // the DYNTZ entry resolves THROUGH pg_tzset to a real zone
        let rc = unsafe {
            pg_diff_decode_timezone_abbrev(b"gmtdyn".as_ptr(), 6, &mut ft, &mut off, &mut have)
        };
        assert_eq!(rc, 0, "pinned DYNTZ abbrev must resolve");
        assert_eq!(have, 1, "DYNTZ resolves to a pg_tz (GMT)");

        // a near-miss must MISS (so the table is not matching everything)
        let rc = unsafe {
            pg_diff_decode_timezone_abbrev(b"aab".as_ptr(), 3, &mut ft, &mut off, &mut have)
        };
        assert_eq!(rc, 0);
        assert_eq!(have, 0, "'aab' is not in the pinned table");

        // the prefix matcher returns the matched LENGTH, not a boolean
        let (mut poff, mut phave) = (0i32, 0i32);
        let n = unsafe {
            pg_diff_decode_timezone_abbrev_prefix(
                b"cccx".as_ptr(),
                4,
                &mut poff,
                &mut phave,
            )
        };
        assert_eq!(n, 3, "longest prefix match of 'cccx' is 'ccc'");
        assert_eq!(poff, 3600);
    }
}
